// Package compaction merges SSTables to bound read and space amplification.
//
// It provides three pieces the DB composes:
//
//   - MergingIterator: a k-way merge of sorted iterators using a min-heap,
//     O(log k) per entry. Used by compaction and by range scans.
//   - Filter: the drop rules applied while merging (keep only the newest
//     version of each user key; drop a tombstone only at the bottom).
//   - Pick: the size-tiered policy that chooses which tables to merge.
//
// # Size-tiered vs leveled
//
// Size-tiered compaction (Cassandra's default, RocksDB "universal") waits
// until several tables of similar size exist and merges them into one
// bigger table. Each byte is rewritten about log_T(N) times, so write
// amplification is low; the cost is read amplification (a point read may
// check one table per tier, mitigated by bloom filters) and temporary space
// amplification (a merge needs room for inputs and output at once).
//
// Leveled compaction (LevelDB, RocksDB default) keeps each level a single
// sorted run ~10x larger than the one above, so reads touch one table per
// level and space overhead is ~10%, but each byte is rewritten ~10 times per
// level. Size-tiered suits write-heavy workloads, which is what this engine
// targets.
package compaction

import (
	"bytes"
	"container/heap"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

// Iterator is the interface shared by memtable and SSTable iterators.
type Iterator interface {
	SeekToFirst()
	Seek(key base.InternalKey)
	Next()
	Valid() bool
	Key() base.InternalKey
	Value() []byte
	Err() error
	Close() error
}

// MergingIterator yields the union of its children in internal-key order.
// Children must not contain identical internal keys (sequence numbers are
// unique, so they never do); if they did, the lower-indexed child wins.
type MergingIterator struct {
	children []Iterator
	h        iterHeap
	err      error
}

// NewMergingIterator merges children. Order children newest-first so ties
// (impossible in practice) resolve to the newest source.
func NewMergingIterator(children ...Iterator) *MergingIterator {
	return &MergingIterator{children: children}
}

type heapItem struct {
	it  Iterator
	idx int
}

type iterHeap []heapItem

func (h iterHeap) Len() int { return len(h) }
func (h iterHeap) Less(i, j int) bool {
	if c := base.Compare(h[i].it.Key(), h[j].it.Key()); c != 0 {
		return c < 0
	}
	return h[i].idx < h[j].idx
}
func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *iterHeap) Push(x any)   { *h = append(*h, x.(heapItem)) }
func (h *iterHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func (m *MergingIterator) rebuild() {
	m.h = m.h[:0]
	for i, c := range m.children {
		if c.Valid() {
			m.h = append(m.h, heapItem{c, i})
		} else if err := c.Err(); err != nil && m.err == nil {
			m.err = err
		}
	}
	heap.Init(&m.h)
}

// SeekToFirst positions every child at its start.
func (m *MergingIterator) SeekToFirst() {
	for _, c := range m.children {
		c.SeekToFirst()
	}
	m.rebuild()
}

// Seek positions every child at the first entry >= key.
func (m *MergingIterator) Seek(key base.InternalKey) {
	for _, c := range m.children {
		c.Seek(key)
	}
	m.rebuild()
}

// Valid reports whether there is a current entry and no error occurred.
func (m *MergingIterator) Valid() bool { return m.err == nil && len(m.h) > 0 }

// Key returns the smallest current key.
func (m *MergingIterator) Key() base.InternalKey { return m.h[0].it.Key() }

// Value returns the value for Key.
func (m *MergingIterator) Value() []byte { return m.h[0].it.Value() }

// Next advances past the current entry.
func (m *MergingIterator) Next() {
	top := m.h[0].it
	top.Next()
	if top.Valid() {
		heap.Fix(&m.h, 0)
		return
	}
	if err := top.Err(); err != nil && m.err == nil {
		m.err = err
	}
	heap.Pop(&m.h)
}

// Err returns the first child error.
func (m *MergingIterator) Err() error { return m.err }

// Close closes every child.
func (m *MergingIterator) Close() error {
	for _, c := range m.children {
		if err := c.Close(); err != nil && m.err == nil {
			m.err = err
		}
	}
	return m.err
}

// Stats counts what a compaction kept and dropped.
type Stats struct {
	InputEntries      int64
	OutputEntries     int64
	DroppedVersions   int64 // older versions shadowed by a newer one
	DroppedTombstones int64 // tombstones removed because nothing older exists
}

// Filter streams it (already positioned, e.g. via SeekToFirst) and calls
// emit for each entry that must survive the merge.
//
// horizon is the oldest snapshot any reader of the output can use: every
// reader that will see the merged table reads at a sequence number >=
// horizon. (The DB passes its visible sequence number at the start of the
// merge; readers that started earlier keep using the input tables, which
// they hold references to.)
//
// Drop rules, per user key, with versions arriving newest-first:
//
//  1. Versions newer than horizon are all kept: a reader between two of
//     them needs the older one.
//  2. The newest version at or below horizon is kept; it is what a reader
//     at horizon sees.
//  3. Anything older than that is shadowed for every possible reader and
//     is dropped.
//  4. The version kept by rule 2 is dropped too if it is a tombstone and
//     bottom is true (the merge includes the oldest table in the
//     database). If older tables exist below the merge, they may hold a
//     value for the key, and dropping the tombstone would resurrect it.
func Filter(it Iterator, bottom bool, horizon base.SeqNum, emit func(key base.InternalKey, value []byte) error) (Stats, error) {
	var (
		st      Stats
		lastKey []byte
		haveKey bool
		covered bool // a version <= horizon was already seen for lastKey
	)
	for ; it.Valid(); it.Next() {
		k := it.Key()
		st.InputEntries++
		if !haveKey || !bytes.Equal(k.UserKey, lastKey) {
			lastKey = append(lastKey[:0], k.UserKey...)
			haveKey, covered = true, false
		}
		if covered {
			st.DroppedVersions++ // rule 3
			continue
		}
		if k.Seq <= horizon {
			covered = true
			if k.Kind == base.KindDelete && bottom {
				st.DroppedTombstones++ // rule 4
				continue
			}
		}
		if err := emit(k, it.Value()); err != nil {
			return st, err
		}
		st.OutputEntries++
	}
	return st, it.Err()
}

// PickOptions tunes the size-tiered policy.
type PickOptions struct {
	MinMerge  int     // fewest tables to merge at once (default 4)
	MaxMerge  int     // most tables to merge at once (default 16)
	Bucket    float64 // tables within [avg/Bucket, avg*Bucket] are "similar" (default 2)
	MaxTables int     // above this many tables, merge regardless of size (default 12)
}

func (o *PickOptions) defaults() {
	if o.MinMerge < 2 {
		o.MinMerge = 4
	}
	if o.MaxMerge < o.MinMerge {
		o.MaxMerge = max(16, o.MinMerge)
	}
	if o.Bucket <= 1 {
		o.Bucket = 2
	}
	if o.MaxTables <= 0 {
		o.MaxTables = 12
	}
}

// Pick chooses tables to merge. sizes lists table sizes newest-first. It
// returns a half-open range [start, end) of adjacent tables, or ok=false.
//
// Only adjacent tables are merged. Tables cover disjoint sequence ranges in
// age order; merging a contiguous run keeps that true, so "newest-first" is
// still well defined afterwards and the read path stays correct.
//
// The policy finds the longest run of similar-sized adjacent tables
// (a tier). If none has MinMerge tables but there are more than MaxTables
// tables in total, it merges the adjacent MinMerge tables with the smallest
// combined size, which bounds read amplification.
func Pick(sizes []int64, opts PickOptions) (start, end int, ok bool) {
	opts.defaults()
	n := len(sizes)
	bestS, bestE := 0, 0
	for s := 0; s < n; s++ {
		sum := float64(sizes[s])
		e := s + 1
		for e < n && e-s < opts.MaxMerge {
			avg := sum / float64(e-s)
			sz := float64(max(sizes[e], 1))
			if sz > avg*opts.Bucket || sz*opts.Bucket < avg {
				break
			}
			sum += float64(sizes[e])
			e++
		}
		if e-s > bestE-bestS {
			bestS, bestE = s, e
		}
	}
	if bestE-bestS >= opts.MinMerge {
		return bestS, bestE, true
	}
	if n > opts.MaxTables {
		w := opts.MinMerge
		best, bestSum := 0, int64(-1)
		for s := 0; s+w <= n; s++ {
			var sum int64
			for _, sz := range sizes[s : s+w] {
				sum += sz
			}
			if bestSum < 0 || sum < bestSum {
				best, bestSum = s, sum
			}
		}
		return best, best + w, true
	}
	return 0, 0, false
}
