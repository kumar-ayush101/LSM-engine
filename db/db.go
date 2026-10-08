package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
	"github.com/kumar-ayush101/LSM-engine/memtable"
	"github.com/kumar-ayush101/LSM-engine/wal"
)

var (
	// ErrNotFound is returned by Get when the key does not exist or was deleted.
	ErrNotFound = errors.New("db: key not found")
	// ErrClosed is returned by operations on a closed DB.
	ErrClosed = errors.New("db: closed")
	// ErrMemtableFull is returned when a write would exceed
	// Options.MaxMemtableBytes. Until SSTable flush exists (W3), all data
	// lives in the memtable, so this cap is what bounds memory use.
	ErrMemtableFull = errors.New("db: memtable full")
	// ErrTooLarge is returned for keys or values that cannot fit in one WAL
	// record.
	ErrTooLarge = errors.New("db: key or value too large")
)

// SyncPolicy controls when the WAL is fsynced, trading write latency
// against how much acknowledged data a power loss or OS crash can destroy.
//
// Every policy writes each record to the OS before acknowledging it, so a
// process crash (including kill -9) loses nothing under any policy. The
// policies differ only for machine-level failures.
type SyncPolicy int

const (
	// SyncGroup (default) fsyncs before acknowledging, but concurrent
	// writers share one fsync (group commit). Same durability as
	// SyncAlways; much higher throughput under concurrency.
	SyncGroup SyncPolicy = iota
	// SyncAlways fsyncs every write while holding the write lock, one at a
	// time. The simple, slow baseline that group commit improves on.
	SyncAlways
	// SyncPeriodic acknowledges writes immediately and fsyncs in the
	// background every Options.SyncInterval. A power loss can lose up to
	// one interval of acknowledged writes.
	SyncPeriodic
)

func (p SyncPolicy) String() string {
	switch p {
	case SyncGroup:
		return "group"
	case SyncAlways:
		return "always"
	case SyncPeriodic:
		return "periodic"
	default:
		return fmt.Sprintf("SyncPolicy(%d)", int(p))
	}
}

// ParseSyncPolicy converts "group", "always" or "periodic" to a SyncPolicy.
func ParseSyncPolicy(s string) (SyncPolicy, error) {
	switch s {
	case "group", "":
		return SyncGroup, nil
	case "always":
		return SyncAlways, nil
	case "periodic":
		return SyncPeriodic, nil
	}
	return 0, fmt.Errorf("db: unknown sync policy %q (want group, always or periodic)", s)
}

// Options configures a DB. The zero value is valid.
type Options struct {
	Sync SyncPolicy
	// SyncInterval is the fsync period for SyncPeriodic. Default 100ms.
	SyncInterval time.Duration
	// MaxMemtableBytes caps memtable memory. Default 256 MiB.
	MaxMemtableBytes int64
}

const (
	defaultSyncInterval     = 100 * time.Millisecond
	defaultMaxMemtableBytes = 256 << 20
	walFileName             = "wal.log"
)

// DB is an LSM-tree key-value store.
//
// Current stage: writes go to a write-ahead log and then an in-memory
// skip-list memtable. Open replays the log, so data survives restarts and
// crashes. SSTables (W3) will let the memtable be flushed to disk.
type DB struct {
	dir  string
	opts Options
	lock *dirLock

	// writeMu serializes writers. Holding it while appending to the WAL
	// and inserting into the memtable guarantees that sequence numbers
	// appear in increasing order in both.
	writeMu sync.Mutex
	nextSeq base.SeqNum // guarded by writeMu
	log     *wal.Writer
	buf     []byte // record encoding scratch; guarded by writeMu

	// visibleSeq is the highest sequence number readers may see. It only
	// advances once a write is in the memtable AND as durable as the sync
	// policy promises, so a reader never observes a write that could still
	// disappear in a crash the policy protects against.
	visibleSeq atomic.Uint64

	mem    *memtable.Memtable
	closed atomic.Bool

	stopSync chan struct{}
	syncDone chan struct{}
}

// Open opens (creating if needed) the database in dir and replays its WAL.
func Open(dir string, opts *Options) (*DB, error) {
	if dir == "" {
		return nil, errors.New("db: empty directory path")
	}
	o := Options{}
	if opts != nil {
		o = *opts
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = defaultSyncInterval
	}
	if o.MaxMemtableBytes <= 0 {
		o.MaxMemtableBytes = defaultMaxMemtableBytes
	}
	if o.Sync < SyncGroup || o.Sync > SyncPeriodic {
		return nil, fmt.Errorf("db: invalid sync policy %d", o.Sync)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	d := &DB{dir: dir, opts: o, lock: lock, mem: memtable.New()}
	if err := d.recover(); err != nil {
		lock.release()
		return nil, err
	}
	if o.Sync == SyncPeriodic {
		d.stopSync = make(chan struct{})
		d.syncDone = make(chan struct{})
		go d.periodicSync()
	}
	return d, nil
}

// recover replays the WAL into the memtable and opens it for appending.
func (d *DB) recover() error {
	path := filepath.Join(d.dir, walFileName)
	var last base.SeqNum
	n := 0
	_, err := wal.Recover(path, func(p []byte) error {
		seq, kind, key, value, err := decodeRecord(p)
		if err == nil && seq <= last {
			err = fmt.Errorf("sequence number %d after %d", seq, last)
		}
		if err == nil {
			if kind == base.KindSet {
				err = d.mem.Put(key, value, seq) // memtable copies key/value
			} else {
				err = d.mem.Delete(key, seq)
			}
		}
		if err != nil {
			return recordError{index: n, err: err}
		}
		last = seq
		n++
		return nil
	})
	if err != nil {
		return fmt.Errorf("db: replaying WAL: %w", err)
	}
	w, err := wal.OpenWriter(path)
	if err != nil {
		return err
	}
	d.log = w
	d.nextSeq = last + 1 // seq 0 is reserved so "snapshot 0" sees nothing
	d.visibleSeq.Store(uint64(last))
	return nil
}

// Put sets key to value. The DB copies both; the caller may reuse them.
func (d *DB) Put(key, value []byte) error {
	return d.write(key, value, base.KindSet)
}

// Delete removes key. Deleting a missing key is not an error.
func (d *DB) Delete(key []byte) error {
	return d.write(key, nil, base.KindDelete)
}

// recordOverhead bounds encodeRecord's framing (seq + kind + uvarint).
const recordOverhead = 8 + 1 + 10

func (d *DB) write(key, value []byte, kind base.Kind) error {
	if len(key)+len(value)+recordOverhead > wal.MaxRecordSize {
		return ErrTooLarge
	}

	d.writeMu.Lock()
	if d.closed.Load() {
		d.writeMu.Unlock()
		return ErrClosed
	}
	if d.mem.ApproximateSize() >= d.opts.MaxMemtableBytes {
		d.writeMu.Unlock()
		return ErrMemtableFull
	}
	seq := d.nextSeq

	// 1. Log first. If the process dies after this, replay restores it.
	d.buf = encodeRecord(d.buf[:0], seq, kind, key, value)
	off, err := d.log.Append(d.buf)
	if err == nil && d.opts.Sync == SyncAlways {
		err = d.log.SyncTo(off)
	}
	if err != nil {
		d.writeMu.Unlock()
		return err
	}

	// 2. Apply to the memtable. Readers cannot see it yet: their snapshot
	// is visibleSeq, which is still < seq.
	if kind == base.KindSet {
		err = d.mem.Put(key, value, seq)
	} else {
		err = d.mem.Delete(key, seq)
	}
	if err != nil {
		// Unreachable unless there is a bug: seq is fresh and in range.
		d.writeMu.Unlock()
		return err
	}
	d.nextSeq++
	d.writeMu.Unlock()

	// 3. Group commit: fsync outside the write lock, so other writers can
	// append meanwhile and share the next fsync.
	if d.opts.Sync == SyncGroup {
		if err := d.log.SyncTo(off); err != nil {
			return err
		}
	}

	// 4. Publish. The log is sequential, so a sync up to off also covers
	// every earlier write; making seq visible therefore never exposes a
	// non-durable write. Writers can finish out of order, so only advance.
	d.publish(seq)
	return nil
}

func (d *DB) publish(seq base.SeqNum) {
	for {
		cur := d.visibleSeq.Load()
		if uint64(seq) <= cur || d.visibleSeq.CompareAndSwap(cur, uint64(seq)) {
			return
		}
	}
}

func (d *DB) periodicSync() {
	defer close(d.syncDone)
	t := time.NewTicker(d.opts.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-d.stopSync:
			return
		case <-t.C:
			// A failure is sticky inside the WAL writer, so every later
			// write returns it; nothing more to do here.
			_ = d.log.Sync()
		}
	}
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

// Stats is a point-in-time summary of the DB.
type Stats struct {
	Entries       int    // memtable entries, all versions incl. tombstones
	MemtableBytes int64  // approximate memtable memory
	MaxMemtable   int64  // Options.MaxMemtableBytes
	WALBytes      int64  // WAL file size
	LastSeq       uint64 // highest visible sequence number
	SyncPolicy    string
}

// Stats returns current statistics.
func (d *DB) Stats() Stats {
	return Stats{
		Entries:       d.mem.Len(),
		MemtableBytes: d.mem.ApproximateSize(),
		MaxMemtable:   d.opts.MaxMemtableBytes,
		WALBytes:      d.log.Size(),
		LastSeq:       d.visibleSeq.Load(),
		SyncPolicy:    d.opts.Sync.String(),
	}
}

// Close syncs the WAL, releases the directory lock, and makes further
// operations return ErrClosed. Close is idempotent.
func (d *DB) Close() error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if d.closed.Swap(true) {
		return nil
	}
	if d.stopSync != nil {
		close(d.stopSync)
		<-d.syncDone
	}
	err := d.log.Close()
	if lerr := d.lock.release(); err == nil {
		err = lerr
	}
	return err
}
