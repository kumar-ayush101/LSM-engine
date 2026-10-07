package memtable

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

func ikey(k string, seq base.SeqNum, kind base.Kind) base.InternalKey {
	return base.MakeInternalKey([]byte(k), seq, kind)
}

// checkInvariants verifies the structural properties of a skip list:
//   - every level is strictly sorted,
//   - every node on level i>0 also appears on level i-1 (towers are contiguous),
//   - level 0 contains exactly s.length nodes,
//   - no level above s.height is populated.
func checkInvariants(t *testing.T, s *skiplist) {
	t.Helper()
	for level := 0; level < maxHeight; level++ {
		if level >= s.height && s.head.next[level] != nil {
			t.Fatalf("level %d populated above height %d", level, s.height)
		}
		count := 0
		for x := s.head.next[level]; x != nil; x = x.next[level] {
			if len(x.next) <= level {
				t.Fatalf("node %v linked at level %d but has height %d", x.key, level, len(x.next))
			}
			if nx := x.next[level]; nx != nil && base.Compare(x.key, nx.key) >= 0 {
				t.Fatalf("level %d not strictly sorted: %v then %v", level, x.key, nx.key)
			}
			count++
		}
		if level == 0 && count != s.length {
			t.Fatalf("level 0 has %d nodes, length is %d", count, s.length)
		}
	}
	// Each node reachable at level i must be reachable at level i-1.
	for level := 1; level < s.height; level++ {
		lower := map[*node]bool{}
		for x := s.head.next[level-1]; x != nil; x = x.next[level-1] {
			lower[x] = true
		}
		for x := s.head.next[level]; x != nil; x = x.next[level] {
			if !lower[x] {
				t.Fatalf("node %v on level %d missing from level %d", x.key, level, level-1)
			}
		}
	}
}

func TestSkiplistEmpty(t *testing.T) {
	s := newSkiplist(1, 2)
	if s.first() != nil {
		t.Fatal("empty list should have no first node")
	}
	if n := s.findGreaterOrEqual(ikey("a", 1, base.KindSet), nil); n != nil {
		t.Fatalf("empty list returned %v", n.key)
	}
	checkInvariants(t, s)
}

func TestSkiplistInsertSortedAndInvariants(t *testing.T) {
	s := newSkiplist(1, 2)
	r := rand.New(rand.NewPCG(3, 4))
	var want []base.InternalKey
	for i := 0; i < 5000; i++ {
		k := ikey(fmt.Sprintf("key-%05d", r.IntN(1000)), base.SeqNum(i+1), base.KindSet)
		if !s.insert(k, nil) {
			t.Fatalf("unexpected duplicate %v", k)
		}
		want = append(want, k)
	}
	checkInvariants(t, s)

	sort.Slice(want, func(i, j int) bool { return base.Compare(want[i], want[j]) < 0 })
	i := 0
	for x := s.first(); x != nil; x = x.next[0] {
		if base.Compare(x.key, want[i]) != 0 {
			t.Fatalf("position %d: got %v, want %v", i, x.key, want[i])
		}
		i++
	}
	if i != len(want) {
		t.Fatalf("iterated %d nodes, want %d", i, len(want))
	}
}

func TestSkiplistRejectsDuplicateInternalKey(t *testing.T) {
	s := newSkiplist(1, 2)
	k := ikey("a", 7, base.KindSet)
	if !s.insert(k, []byte("v1")) {
		t.Fatal("first insert failed")
	}
	if s.insert(k, []byte("v2")) {
		t.Fatal("duplicate insert should be rejected")
	}
	if s.length != 1 || string(s.first().value) != "v1" {
		t.Fatal("duplicate insert modified the list")
	}
	// Same user key with a different seq is a distinct version.
	if !s.insert(ikey("a", 8, base.KindSet), nil) {
		t.Fatal("different seq should be accepted")
	}
	checkInvariants(t, s)
}

func TestSkiplistFindGreaterOrEqual(t *testing.T) {
	s := newSkiplist(1, 2)
	for _, k := range []base.InternalKey{
		ikey("b", 5, base.KindSet),
		ikey("b", 3, base.KindDelete),
		ikey("d", 1, base.KindSet),
	} {
		s.insert(k, nil)
	}
	cases := []struct {
		seek base.InternalKey
		want *base.InternalKey
	}{
		{ikey("a", 1, base.KindSet), &base.InternalKey{UserKey: []byte("b"), Seq: 5, Kind: base.KindSet}},
		{base.MakeSearchKey([]byte("b"), 4), &base.InternalKey{UserKey: []byte("b"), Seq: 3, Kind: base.KindDelete}},
		{base.MakeSearchKey([]byte("b"), 2), &base.InternalKey{UserKey: []byte("d"), Seq: 1, Kind: base.KindSet}},
		{ikey("e", 1, base.KindSet), nil},
	}
	for _, c := range cases {
		n := s.findGreaterOrEqual(c.seek, nil)
		switch {
		case c.want == nil && n != nil:
			t.Errorf("seek %v: got %v, want nil", c.seek, n.key)
		case c.want != nil && n == nil:
			t.Errorf("seek %v: got nil, want %v", c.seek, *c.want)
		case c.want != nil && base.Compare(n.key, *c.want) != 0:
			t.Errorf("seek %v: got %v, want %v", c.seek, n.key, *c.want)
		}
	}
}

func TestRandomHeightDistribution(t *testing.T) {
	s := newSkiplist(42, 42)
	const trials = 200000
	counts := make([]int, maxHeight+1)
	for i := 0; i < trials; i++ {
		h := s.randomHeight()
		if h < 1 || h > maxHeight {
			t.Fatalf("height %d out of range", h)
		}
		counts[h]++
	}
	// P(height == 1) = 1 - 1/branching = 0.75. Allow a generous margin.
	frac1 := float64(counts[1]) / trials
	if frac1 < 0.73 || frac1 > 0.77 {
		t.Fatalf("P(h=1) = %.3f, want ~0.75", frac1)
	}
	// P(height >= 2) should be ~4x P(height >= 3).
	ge2, ge3 := 0, 0
	for h := 2; h <= maxHeight; h++ {
		ge2 += counts[h]
		if h >= 3 {
			ge3 += counts[h]
		}
	}
	ratio := float64(ge2) / float64(ge3)
	if ratio < 3.5 || ratio > 4.5 {
		t.Fatalf("P(h>=2)/P(h>=3) = %.2f, want ~4", ratio)
	}
}
