package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"

	"github.com/kumar-ayush101/LSM-engine/bloom"
	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

type indexEntry struct {
	lastKey base.InternalKey
	handle  blockHandle
}

// Reader reads a table. The index and bloom filter are loaded into memory
// when the table is opened; data blocks are read from disk on demand
// (with pread, so concurrent readers do not share a file position).
//
// A Reader is safe for concurrent use. It is reference counted: the DB
// holds one reference, and every iterator or in-flight Get holds another,
// so a table deleted by compaction stays readable until its last user is
// done.
type Reader struct {
	f       *os.File
	path    string
	size    int64
	entries uint64
	index   []indexEntry
	filter  bloom.Filter

	refs     atomic.Int32
	onDelete func() // called when the last ref is dropped after MarkObsolete
	obsolete atomic.Bool

	// Counters for read-amplification stats.
	BlockReads  atomic.Int64
	FilterSkips atomic.Int64
}

// Open opens the table at path and verifies its footer, index and filter.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := newReader(f, path)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}

func newReader(f *os.File, path string) (*Reader, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size < footerLen {
		return nil, ErrCorrupt
	}
	var foot [footerLen]byte
	if _, err := f.ReadAt(foot[:], size-footerLen); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint64(foot[40:]) != magic {
		return nil, ErrCorrupt
	}
	fh := blockHandle{binary.LittleEndian.Uint64(foot[0:]), binary.LittleEndian.Uint64(foot[8:])}
	ih := blockHandle{binary.LittleEndian.Uint64(foot[16:]), binary.LittleEndian.Uint64(foot[24:])}
	r := &Reader{f: f, path: path, size: size, entries: binary.LittleEndian.Uint64(foot[32:])}
	r.refs.Store(1)

	filter, err := r.readBlock(fh)
	if err != nil {
		return nil, err
	}
	r.filter = filter
	idx, err := r.readBlock(ih)
	if err != nil {
		return nil, err
	}
	for len(idx) > 0 {
		klen, n := binary.Uvarint(idx)
		if n <= 0 || klen > uint64(len(idx)-n) {
			return nil, ErrCorrupt
		}
		key, err := base.DecodeInternalKey(append([]byte(nil), idx[n:n+int(klen)]...))
		if err != nil {
			return nil, ErrCorrupt
		}
		idx = idx[n+int(klen):]
		off, n1 := binary.Uvarint(idx)
		if n1 <= 0 {
			return nil, ErrCorrupt
		}
		idx = idx[n1:]
		ln, n2 := binary.Uvarint(idx)
		if n2 <= 0 {
			return nil, ErrCorrupt
		}
		idx = idx[n2:]
		r.index = append(r.index, indexEntry{lastKey: key, handle: blockHandle{off, ln}})
	}
	if len(r.index) == 0 {
		return nil, ErrCorrupt
	}
	return r, nil
}

func (r *Reader) readBlock(h blockHandle) ([]byte, error) {
	if h.length > uint64(r.size) || h.offset > uint64(r.size)-h.length-crcLen {
		return nil, ErrCorrupt
	}
	buf := make([]byte, h.length+crcLen)
	if _, err := r.f.ReadAt(buf, int64(h.offset)); err != nil && err != io.EOF {
		return nil, err
	}
	if !checkCRC(buf[:h.length], buf[h.length:]) {
		return nil, ErrCorrupt
	}
	return buf[:h.length], nil
}

// Path returns the file path.
func (r *Reader) Path() string { return r.path }

// Size returns the file size in bytes.
func (r *Reader) Size() int64 { return r.size }

// Entries returns the number of entries (all versions and tombstones).
func (r *Reader) Entries() uint64 { return r.entries }

// MayContain consults the bloom filter for userKey.
func (r *Reader) MayContain(userKey []byte) bool { return r.filter.MayContain(userKey) }

// Get returns the newest version of userKey with seq <= snapshot.
// found is false when the table has no visible version.
func (r *Reader) Get(userKey []byte, snapshot base.SeqNum) (value []byte, kind base.Kind, found bool, err error) {
	if !r.filter.MayContain(userKey) {
		r.FilterSkips.Add(1)
		return nil, 0, false, nil
	}
	it := r.NewIterator()
	defer it.Close()
	it.Seek(base.MakeSearchKey(userKey, snapshot))
	if err := it.Err(); err != nil {
		return nil, 0, false, err
	}
	if !it.Valid() || !bytes.Equal(it.Key().UserKey, userKey) {
		return nil, 0, false, nil
	}
	k := it.Key()
	return append([]byte(nil), it.Value()...), k.Kind, true, nil
}

// Ref adds a reference. Every Ref must be paired with Unref.
func (r *Reader) Ref() { r.refs.Add(1) }

// Unref drops a reference; the last one closes the file, and deletes it if
// MarkObsolete was called.
func (r *Reader) Unref() {
	if r.refs.Add(-1) == 0 {
		r.f.Close()
		if r.obsolete.Load() {
			os.Remove(r.path)
			if r.onDelete != nil {
				r.onDelete()
			}
		}
	}
}

// MarkObsolete arranges for the file to be deleted once the last reference
// is dropped. onDelete (optional) runs after deletion.
func (r *Reader) MarkObsolete(onDelete func()) {
	r.onDelete = onDelete
	r.obsolete.Store(true)
}

// Iterator walks a table in internal-key order. It holds a reference on
// the Reader until Close.
type Iterator struct {
	r      *Reader
	bi     int // current block index
	block  []byte
	pos    int // offset of the current entry within block
	next   int // offset of the following entry
	key    base.InternalKey
	value  []byte
	valid  bool
	err    error
	closed bool
}

// NewIterator returns an unpositioned iterator.
func (r *Reader) NewIterator() *Iterator {
	r.Ref()
	return &Iterator{r: r, bi: -1}
}

func (it *Iterator) loadBlock(i int) bool {
	it.valid = false
	if i >= len(it.r.index) {
		it.bi = len(it.r.index)
		return false
	}
	b, err := it.r.readBlock(it.r.index[i].handle)
	if err != nil {
		it.err = err
		return false
	}
	it.r.BlockReads.Add(1)
	it.bi, it.block, it.next = i, b, 0
	return true
}

// parse decodes the entry at it.next.
func (it *Iterator) parse() bool {
	if it.next >= len(it.block) {
		it.valid = false
		return false
	}
	b := it.block[it.next:]
	klen, n1 := binary.Uvarint(b)
	if n1 <= 0 {
		it.err, it.valid = ErrCorrupt, false
		return false
	}
	vlen, n2 := binary.Uvarint(b[n1:])
	if n2 <= 0 || klen+vlen > uint64(len(b)-n1-n2) {
		it.err, it.valid = ErrCorrupt, false
		return false
	}
	start := n1 + n2
	key, err := base.DecodeInternalKey(b[start : start+int(klen)])
	if err != nil {
		it.err, it.valid = ErrCorrupt, false
		return false
	}
	it.pos = it.next
	it.key = key
	it.value = b[start+int(klen) : start+int(klen)+int(vlen)]
	it.next += start + int(klen) + int(vlen)
	it.valid = true
	return true
}

// SeekToFirst positions at the first entry.
func (it *Iterator) SeekToFirst() {
	if it.loadBlock(0) {
		it.parse()
	}
}

// Seek positions at the first entry >= key.
func (it *Iterator) Seek(key base.InternalKey) {
	idx := it.r.index
	i := sort.Search(len(idx), func(i int) bool { return base.Compare(idx[i].lastKey, key) >= 0 })
	if !it.loadBlock(i) {
		return
	}
	for it.parse() {
		if base.Compare(it.key, key) >= 0 {
			return
		}
	}
	// Unreachable for a well-formed table (the block's last key is >= key);
	// fall through to the next block defensively.
	if it.err == nil {
		it.nextBlock()
	}
}

func (it *Iterator) nextBlock() {
	for it.loadBlock(it.bi + 1) {
		if it.parse() {
			return
		}
	}
}

// Next advances to the next entry.
func (it *Iterator) Next() {
	if !it.valid {
		return
	}
	if !it.parse() && it.err == nil {
		it.nextBlock()
	}
}

// Valid reports whether the iterator is positioned at an entry.
func (it *Iterator) Valid() bool { return it.valid && it.err == nil }

// Key returns the current key; it aliases internal buffers until the next move.
func (it *Iterator) Key() base.InternalKey { return it.key }

// Value returns the current value; it aliases internal buffers.
func (it *Iterator) Value() []byte { return it.value }

// Err returns the first error encountered (e.g. a checksum failure).
func (it *Iterator) Err() error { return it.err }

// Close releases the iterator's reference on the table.
func (it *Iterator) Close() error {
	if !it.closed {
		it.closed = true
		it.valid = false
		it.r.Unref()
	}
	return it.err
}
