package memtable

import (
	"math/rand/v2"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

const (
	// maxHeight caps the number of levels. With branching factor 4, a list of
	// height 12 stays O(log n) for up to ~4^12 = 16M entries, which is far
	// more than one memtable holds before it is flushed.
	maxHeight = 12

	// branching is 1/p: each node is promoted to the next level with
	// probability 1/branching. p = 1/4 (as in LevelDB) gives on average
	// 1/(1-p) = 1.33 pointers per node, versus 2 for p = 1/2, at the cost of
	// slightly more comparisons per level.
	branching = 4
)

// node is one entry in the skip list. key and value never change after the
// node is linked into the list, and nodes are never removed, so a reader
// that obtained a node pointer under the lock can read key/value afterwards
// without holding it.
type node struct {
	key   base.InternalKey
	value []byte
	next  []*node // next[i] is the successor at level i; len(next) is the node's height
}

// skiplist is an ordered set of internal keys. It is NOT safe for concurrent
// use: the Memtable wrapper serializes access with an RWMutex.
type skiplist struct {
	head   *node // sentinel; its key is never compared
	height int   // current height of the tallest node, 1..maxHeight
	length int
	rng    *rand.Rand
}

func newSkiplist(seed1, seed2 uint64) *skiplist {
	return &skiplist{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		rng:    rand.New(rand.NewPCG(seed1, seed2)),
	}
}

// randomHeight returns a height in [1, maxHeight] with
// P(height >= h) = (1/branching)^(h-1).
func (s *skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && s.rng.IntN(branching) == 0 {
		h++
	}
	return h
}

// findGreaterOrEqual returns the first node whose key is >= key, or nil if
// there is none. If prev is non-nil, prev[i] is set to the last node at
// level i whose key is < key (the node that would precede key at level i).
func (s *skiplist) findGreaterOrEqual(key base.InternalKey, prev *[maxHeight]*node) *node {
	x := s.head
	level := s.height - 1
	for {
		next := x.next[level]
		if next != nil && base.Compare(next.key, key) < 0 {
			// Keep moving right on this level.
			x = next
			continue
		}
		if prev != nil {
			prev[level] = x
		}
		if level == 0 {
			return next
		}
		// Drop down a level.
		level--
	}
}

// insert adds key/value. It returns false, leaving the list unchanged, if an
// entry with an identical internal key (same user key, seq and kind) already
// exists. The caller must not modify key.UserKey or value afterwards.
func (s *skiplist) insert(key base.InternalKey, value []byte) bool {
	var prev [maxHeight]*node
	if n := s.findGreaterOrEqual(key, &prev); n != nil && base.Compare(n.key, key) == 0 {
		return false
	}

	h := s.randomHeight()
	if h > s.height {
		// Levels above the old height have only the head as predecessor.
		for i := s.height; i < h; i++ {
			prev[i] = s.head
		}
		s.height = h
	}

	n := &node{key: key, value: value, next: make([]*node, h)}
	for i := 0; i < h; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}
	s.length++
	return true
}

// first returns the smallest node, or nil if the list is empty.
func (s *skiplist) first() *node {
	return s.head.next[0]
}
