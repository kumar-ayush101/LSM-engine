package db

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
	"github.com/kumar-ayush101/LSM-engine/memtable"
)

var (
	// ErrNotFound is returned by Get when the key does not exist or was deleted.
	ErrNotFound = errors.New("db: key not found")
	// ErrClosed is returned by operations on a closed DB.
	ErrClosed = errors.New("db: closed")
)

// Options configures a DB. Fields are added as components land
// (WAL sync policy, memtable size, compaction strategy, ...).
type Options struct{}

// DB is an LSM-tree key-value store.
//
// Current stage (week 1): writes go to an in-memory skip-list memtable only.
// Data is not yet durable; the WAL and SSTables are added in later stages.
type DB struct {
	dir  string
	opts Options

	// writeMu serializes writers. Holding it while assigning the sequence
	// number AND inserting into the memtable guarantees that sequence
	// numbers enter the memtable in increasing order, which is what makes
	// publishing visibleSeq safe (see below).
	writeMu sync.Mutex
	nextSeq base.SeqNum // next sequence number to assign; guarded by writeMu

	// visibleSeq is the highest sequence number whose write is fully
	// applied. Readers use it as their snapshot, so they never observe a
	// partially applied write. It is stored only after the memtable insert.
	visibleSeq atomic.Uint64

	mem    *memtable.Memtable
	closed atomic.Bool
}

// Open opens (creating if needed) the database in dir.
func Open(dir string, opts *Options) (*DB, error) {
	if dir == "" {
		return nil, errors.New("db: empty directory path")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	d := &DB{
		dir:     dir,
		nextSeq: 1, // seq 0 is reserved so "snapshot 0" sees nothing
		mem:     memtable.New(),
	}
	if opts != nil {
		d.opts = *opts
	}
	return d, nil
}

// Put sets key to value. The DB copies both; the caller may reuse them.
func (d *DB) Put(key, value []byte) error {
	return d.write(key, value, base.KindSet)
}

// Delete removes key. Deleting a missing key is not an error.
func (d *DB) Delete(key []byte) error {
	return d.write(key, nil, base.KindDelete)
}

func (d *DB) write(key, value []byte, kind base.Kind) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	if d.closed.Load() {
		return ErrClosed
	}
	if d.nextSeq > base.MaxSeqNum {
		return errors.New("db: sequence numbers exhausted")
	}
	seq := d.nextSeq

	var err error
	if kind == base.KindSet {
		err = d.mem.Put(key, value, seq)
	} else {
		err = d.mem.Delete(key, seq)
	}
	if err != nil {
		return err
	}
	d.nextSeq++
	// Publish only after the write is in the memtable.
	d.visibleSeq.Store(uint64(seq))
	return nil
}

// Get returns the current value of key, or ErrNotFound.
func (d *DB) Get(key []byte) ([]byte, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	snapshot := base.SeqNum(d.visibleSeq.Load())
	v, res := d.mem.Get(key, snapshot)
	switch res {
	case memtable.Found:
		return v, nil
	case memtable.Deleted:
		// A tombstone ends the search. Once SSTables exist this matters:
		// we must not fall through to older on-disk versions.
		return nil, ErrNotFound
	default:
		// Later stages: search immutable memtables, then SSTables newest-first.
		return nil, ErrNotFound
	}
}

// Close closes the DB. Further operations return ErrClosed. Close is
// idempotent.
func (d *DB) Close() error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	d.closed.Store(true)
	return nil
}
