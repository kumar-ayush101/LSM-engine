package memtable

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"sync"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

// ErrDuplicate is returned when an entry with the same user key and sequence
// number is added twice. Sequence numbers are unique per write, so this
// indicates a bug in the caller.
var ErrDuplicate = errors.New("memtable: duplicate internal key")

// LookupResult is the outcome of a point lookup.
type LookupResult int

const (
	// NotFound means the memtable has no version of the key visible at the
	// snapshot. The caller must keep searching older data (immutable
	// memtables, SSTables).
	NotFound LookupResult = iota
	// Found means the newest visible version is a value.
	Found
	// Deleted means the newest visible version is a tombstone. The caller
	// must stop searching: older data may still hold a value for this key,
	// but it has been deleted.
	Deleted
)

// nodeOverhead approximates the per-entry memory cost beyond key and value
// bytes (node struct, slice headers, trailer). It only needs to be roughly
// right: it decides when the memtable is "full" and should be flushed.
const nodeOverhead = 64

// Memtable is the in-memory, sorted write buffer of the LSM tree.
//
// Concurrency: a single sync.RWMutex guards the skip list. Writes take the
// exclusive lock; Get and iterator steps take the shared lock, so readers
// run in parallel with each other. A lock-free skip list would let readers
// run concurrently with writers too, but it is much harder to get right,
// and in this engine writes are already serialized by the DB (to assign
// sequence numbers in order), so the extra win is small.
type Memtable struct {
	mu   sync.RWMutex
	list *skiplist
	size int64 // approximate bytes used; guarded by mu
}

// New returns an empty memtable.
func New() *Memtable {
	return &Memtable{list: newSkiplist(rand.Uint64(), rand.Uint64())}
}

// Put records key=value at sequence number seq.
func (m *Memtable) Put(key, value []byte, seq base.SeqNum) error {
	return m.add(key, value, seq, base.KindSet)
}

// Delete records a tombstone for key at sequence number seq.
//
// A delete cannot simply remove the key: older versions may live in
// immutable memtables or SSTables on disk. The tombstone shadows them until
// compaction can prove no older version remains.
func (m *Memtable) Delete(key []byte, seq base.SeqNum) error {
	return m.add(key, nil, seq, base.KindDelete)
}

func (m *Memtable) add(key, value []byte, seq base.SeqNum, kind base.Kind) error {
	if seq > base.MaxSeqNum {
		return errors.New("memtable: sequence number exceeds MaxSeqNum")
	}
	// Copy before taking the lock: the caller may reuse its buffers, and
	// copying outside the critical section keeps it short.
	k := append([]byte(nil), key...)
	var v []byte
	if kind == base.KindSet {
		v = append([]byte{}, value...) // non-nil even for an empty value
	}
	ik := base.MakeInternalKey(k, seq, kind)

	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.list.insert(ik, v) {
		return ErrDuplicate
	}
	m.size += int64(ik.Size() + len(v) + nodeOverhead)
	return nil
}

// Get returns the newest version of key with seq <= snapshot.
// The returned value is a copy and is only meaningful when the result is Found.
func (m *Memtable) Get(key []byte, snapshot base.SeqNum) ([]byte, LookupResult) {
	search := base.MakeSearchKey(key, snapshot)

	m.mu.RLock()
	n := m.list.findGreaterOrEqual(search, nil)
	m.mu.RUnlock()

	// n's key and value are immutable once linked, so reading them after
	// releasing the lock is safe.
	if n == nil || !bytes.Equal(n.key.UserKey, key) {
		return nil, NotFound
	}
	if n.key.Kind == base.KindDelete {
		return nil, Deleted
	}
	return append([]byte{}, n.value...), Found
}

// ApproximateSize returns an estimate of the memory used, in bytes.
func (m *Memtable) ApproximateSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Len returns the number of entries (all versions, including tombstones).
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.length
}

// Empty reports whether the memtable has no entries.
func (m *Memtable) Empty() bool {
	return m.Len() == 0
}
