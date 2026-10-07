package memtable

import "github.com/kumar-ayush101/LSM-engine/internal/base"

// Iterator walks every entry of a memtable (all versions, including
// tombstones) in internal-key order: user key ascending, newest first.
// The flush path uses it to write the memtable out as a sorted SSTable.
//
// The read lock is held only while following a next pointer, not for the
// iterator's lifetime, so a long-lived iterator never blocks writers.
// This is safe because nodes are never removed or modified after being
// linked. Entries inserted concurrently may or may not be observed.
//
// An Iterator is not safe for concurrent use by multiple goroutines.
type Iterator struct {
	m *Memtable
	n *node
}

// NewIterator returns an unpositioned iterator. Call SeekToFirst or Seek
// before using it.
func (m *Memtable) NewIterator() *Iterator {
	return &Iterator{m: m}
}

// SeekToFirst positions the iterator at the smallest entry.
func (it *Iterator) SeekToFirst() {
	it.m.mu.RLock()
	it.n = it.m.list.first()
	it.m.mu.RUnlock()
}

// Seek positions the iterator at the first entry whose internal key is >=
// key. To start at the newest version of a user key, pass
// base.MakeSearchKey(userKey, base.MaxSeqNum).
func (it *Iterator) Seek(key base.InternalKey) {
	it.m.mu.RLock()
	it.n = it.m.list.findGreaterOrEqual(key, nil)
	it.m.mu.RUnlock()
}

// Valid reports whether the iterator is positioned at an entry.
func (it *Iterator) Valid() bool {
	return it.n != nil
}

// Next advances to the next entry. Valid must be true.
func (it *Iterator) Next() {
	it.m.mu.RLock()
	it.n = it.n.next[0]
	it.m.mu.RUnlock()
}

// Key returns the current internal key. Valid must be true. The returned
// UserKey must not be modified.
func (it *Iterator) Key() base.InternalKey {
	return it.n.key
}

// Value returns the current value (nil for tombstones). Valid must be true.
// The returned slice must not be modified.
func (it *Iterator) Value() []byte {
	return it.n.value
}
