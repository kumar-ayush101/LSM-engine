package compaction

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

// sliceIter is an in-memory Iterator over sorted entries.
type sliceIter struct {
	keys   []base.InternalKey
	vals   []string
	i      int
	err    error
	failAt int // inject an error when reaching this index (-1 = never)
	closed bool
}

type kv struct {
	k base.InternalKey
	v string
}

func newSlice(entries []kv) *sliceIter {
	s := &sliceIter{failAt: -1}
	sorted := append([]kv(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return base.Compare(sorted[i].k, sorted[j].k) < 0 })
	for _, e := range sorted {
		s.keys = append(s.keys, e.k)
		s.vals = append(s.vals, e.v)
	}
	return s
}

func (s *sliceIter) check() {
	if s.failAt >= 0 && s.i >= s.failAt {
		s.err = errors.New("injected")
	}
}
func (s *sliceIter) SeekToFirst() { s.i = 0; s.check() }
func (s *sliceIter) Seek(k base.InternalKey) {
	s.i = sort.Search(len(s.keys), func(i int) bool { return base.Compare(s.keys[i], k) >= 0 })
	s.check()
}
func (s *sliceIter) Next()                 { s.i++; s.check() }
func (s *sliceIter) Valid() bool           { return s.err == nil && s.i < len(s.keys) }
func (s *sliceIter) Key() base.InternalKey { return s.keys[s.i] }
func (s *sliceIter) Value() []byte         { return []byte(s.vals[s.i]) }
func (s *sliceIter) Err() error            { return s.err }
func (s *sliceIter) Close() error          { s.closed = true; return s.err }

func ik(k string, seq base.SeqNum, kind base.Kind) base.InternalKey {
	return base.MakeInternalKey([]byte(k), seq, kind)
}

func TestMergingIteratorOrder(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	var children []Iterator
	var all []base.InternalKey
	seq := base.SeqNum(1)
	for c := 0; c < 7; c++ {
		var m []kv
		for i := 0; i < 300; i++ {
			k := ik(fmt.Sprintf("k%03d", r.IntN(200)), seq, base.KindSet)
			seq++
			m = append(m, kv{k, fmt.Sprint(k.Seq)})
			all = append(all, k)
		}
		children = append(children, newSlice(m))
	}
	sort.Slice(all, func(i, j int) bool { return base.Compare(all[i], all[j]) < 0 })
	mi := NewMergingIterator(children...)
	i := 0
	for mi.SeekToFirst(); mi.Valid(); mi.Next() {
		if base.Compare(mi.Key(), all[i]) != 0 || string(mi.Value()) != fmt.Sprint(all[i].Seq) {
			t.Fatalf("pos %d: got %v want %v", i, mi.Key(), all[i])
		}
		i++
	}
	if i != len(all) {
		t.Fatalf("merged %d of %d", i, len(all))
	}
	// Seek lands on the first key >= target across all children.
	target := base.MakeSearchKey([]byte("k100"), base.MaxSeqNum)
	mi.Seek(target)
	j := sort.Search(len(all), func(i int) bool { return base.Compare(all[i], target) >= 0 })
	if !mi.Valid() || base.Compare(mi.Key(), all[j]) != 0 {
		t.Fatalf("seek: got %v want %v", mi.Key(), all[j])
	}
	mi.Close()
	for _, c := range children {
		if !c.(*sliceIter).closed {
			t.Fatal("child not closed")
		}
	}
}

func TestMergingIteratorPropagatesErrors(t *testing.T) {
	a := newSlice([]kv{{ik("a", 1, base.KindSet), ""}, {ik("c", 2, base.KindSet), ""}})
	b := newSlice([]kv{{ik("b", 3, base.KindSet), ""}, {ik("d", 4, base.KindSet), ""}})
	b.failAt = 1
	mi := NewMergingIterator(a, b)
	n := 0
	for mi.SeekToFirst(); mi.Valid(); mi.Next() {
		n++
	}
	if mi.Err() == nil {
		t.Fatalf("error not propagated after %d entries", n)
	}
}

func TestFilterDropRules(t *testing.T) {
	entries := []kv{
		{ik("a", 9, base.KindSet), "a9"},
		{ik("a", 5, base.KindSet), "a5"},  // shadowed
		{ik("b", 8, base.KindDelete), ""}, // tombstone over...
		{ik("b", 2, base.KindSet), "b2"},  // ...an older value
		{ik("c", 7, base.KindDelete), ""}, // lone tombstone
		{ik("d", 1, base.KindSet), "d1"},
	}
	run := func(bottom bool) ([]string, Stats) {
		var out []string
		st, err := Filter(func() Iterator { s := newSlice(entries); s.SeekToFirst(); return s }(), bottom,
			func(k base.InternalKey, v []byte) error {
				out = append(out, fmt.Sprintf("%s#%d%s=%s", k.UserKey, k.Seq, map[base.Kind]string{base.KindSet: "", base.KindDelete: "D"}[k.Kind], v))
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		return out, st
	}

	// Not bottom: older tables may hold "b" or "c", so tombstones stay.
	out, st := run(false)
	if fmt.Sprint(out) != "[a#9=a9 b#8D= c#7D= d#1=d1]" {
		t.Fatalf("not bottom: %v", out)
	}
	if st.DroppedVersions != 2 || st.DroppedTombstones != 0 || st.InputEntries != 6 || st.OutputEntries != 4 {
		t.Fatalf("not bottom stats: %+v", st)
	}
	// Bottom: nothing older exists, so tombstones and what they shadow vanish.
	out, st = run(true)
	if fmt.Sprint(out) != "[a#9=a9 d#1=d1]" {
		t.Fatalf("bottom: %v", out)
	}
	if st.DroppedVersions != 2 || st.DroppedTombstones != 2 || st.OutputEntries != 2 {
		t.Fatalf("bottom stats: %+v", st)
	}
}

func TestPick(t *testing.T) {
	cases := []struct {
		name       string
		sizes      []int64
		start, end int
		ok         bool
	}{
		{"too few", []int64{10, 10, 10}, 0, 0, false},
		{"one tier", []int64{10, 11, 9, 12}, 0, 4, true},
		{"newest tier among big tables", []int64{10, 12, 9, 11, 1000, 1100}, 0, 4, true},
		{"tier behind a big table", []int64{5000, 10, 12, 9, 11, 10}, 1, 6, true},
		{"mixed sizes, no tier", []int64{1, 100, 10000, 1000000}, 0, 0, false},
		{"too many tables forces smallest window", []int64{1, 100, 10000, 1, 100, 10000, 1, 100, 10000, 1, 100, 1, 1}, 9, 13, true},
	}
	for _, c := range cases {
		s, e, ok := Pick(c.sizes, PickOptions{})
		if ok != c.ok || (ok && (s != c.start || e != c.end)) {
			t.Errorf("%s: got [%d,%d) %v, want [%d,%d) %v", c.name, s, e, ok, c.start, c.end, c.ok)
		}
	}
	// MaxMerge caps the run length.
	sizes := make([]int64, 40)
	for i := range sizes {
		sizes[i] = 10
	}
	if s, e, ok := Pick(sizes, PickOptions{MaxMerge: 8}); !ok || e-s != 8 {
		t.Fatalf("MaxMerge: [%d,%d) %v", s, e, ok)
	}
}
