package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kumar-ayush101/LSM-engine/compaction"
	"github.com/kumar-ayush101/LSM-engine/internal/base"
	"github.com/kumar-ayush101/LSM-engine/manifest"
	"github.com/kumar-ayush101/LSM-engine/memtable"
	"github.com/kumar-ayush101/LSM-engine/sstable"
	"github.com/kumar-ayush101/LSM-engine/wal"
)

var (
	// ErrNotFound is returned by Get when the key does not exist or was deleted.
	ErrNotFound = errors.New("db: key not found")
	// ErrClosed is returned by operations on a closed DB.
	ErrClosed = errors.New("db: closed")
	// ErrMemtableFull is no longer returned: since memtables are flushed to
	// SSTables, a writer that outpaces the flush waits instead. It is kept
	// so existing callers that match on it still compile.
	ErrMemtableFull = errors.New("db: memtable full")
	// ErrTooLarge is returned for keys or values that cannot fit in one WAL
	// record.
	ErrTooLarge = errors.New("db: key or value too large")
	// ErrBackground wraps a failed flush or compaction. It is sticky: the DB
	// stops accepting writes, and a restart recovers from the WAL and
	// MANIFEST.
	ErrBackground = errors.New("db: background flush or compaction failed")
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
	// MemtableSize is the flush threshold: when the active memtable reaches
	// it, it becomes immutable and is written to an SSTable in the
	// background. Default 4 MiB.
	MemtableSize int64
	// MaxMemtableBytes bounds memtable memory. At most two memtables exist
	// (active + one being flushed), so the effective flush threshold is
	// min(MemtableSize, MaxMemtableBytes/2). Default 256 MiB.
	MaxMemtableBytes int64
	// BlockSize is the SSTable data block size. Default 4 KiB.
	BlockSize int
	// BloomFPRate is the bloom filter false-positive target. Default 1%.
	BloomFPRate float64
	// Compaction tunes the size-tiered policy (zero value = defaults).
	Compaction compaction.PickOptions
	// DisableAutoCompaction turns off background compaction (Compact still
	// works). Used by benchmarks to measure its effect.
	DisableAutoCompaction bool
}

const (
	defaultSyncInterval     = 100 * time.Millisecond
	defaultMemtableSize     = 4 << 20
	defaultMaxMemtableBytes = 256 << 20
	// legacyWALName is the single log file used before WAL rotation (W2).
	// It is replayed once, flushed to an SSTable, and deleted.
	legacyWALName = "wal.log"
)

func logName(num uint64) string   { return fmt.Sprintf("%06d.log", num) }
func tableName(num uint64) string { return fmt.Sprintf("%06d.sst", num) }

// tableHandle is one live SSTable.
type tableHandle struct {
	meta     manifest.FileMeta
	r        *sstable.Reader
	smallest []byte // user key range, for skipping tables on Get
	largest  []byte
}

// DB is an LSM-tree key-value store.
//
// Write path: WAL append -> active memtable. When the memtable reaches the
// flush threshold it is frozen (immutable), a new WAL file is started, and
// a background goroutine writes the frozen memtable to an SSTable, records
// it in the MANIFEST, and deletes the old WAL. A second goroutine merges
// SSTables with size-tiered compaction.
//
// Read path: active memtable -> immutable memtable -> SSTables newest
// first, stopping at the first version visible at the read's snapshot. A
// tombstone stops the search.
type DB struct {
	dir       string
	opts      Options
	threshold int64
	lock      *dirLock

	// writeMu serializes writers: sequence assignment, WAL append and
	// memtable insert happen in one critical section, so sequence numbers
	// are in increasing order in both.
	writeMu sync.Mutex
	nextSeq base.SeqNum // guarded by writeMu
	buf     []byte      // guarded by writeMu

	// visibleSeq is the highest sequence number readers may see. It only
	// advances once a write is in the memtable AND as durable as the sync
	// policy promises.
	visibleSeq atomic.Uint64
	closed     atomic.Bool

	// mu guards the read state below. Fields that writers also change
	// (mem, log) are modified only while holding both writeMu and mu.
	mu         sync.RWMutex
	cond       *sync.Cond // on &mu; signalled when imm is flushed or bgErr is set
	mem        *memtable.Memtable
	imm        *memtable.Memtable // being flushed; nil if none
	immLogNext uint64             // MANIFEST LogNumber to record once imm is flushed
	log        *wal.Writer
	logNum     uint64
	tables     []*tableHandle // newest first
	nextFile   uint64
	bgErr      error
	closing    bool

	manifestMu sync.Mutex // serializes MANIFEST edits
	man        *manifest.Manifest
	compactMu  sync.Mutex // one compaction at a time

	flushCh     chan struct{}
	compactCh   chan struct{}
	flushDone   sync.WaitGroup
	compactDone sync.WaitGroup
	stopSync    chan struct{}
	syncDone    chan struct{}

	// Counters (process lifetime).
	userBytes    atomic.Int64 // key + value bytes accepted by Put/Delete
	walBytes     atomic.Int64 // bytes appended to WAL files
	flushBytes   atomic.Int64 // bytes written by memtable flushes
	compactBytes atomic.Int64 // bytes written by compactions
	flushes      atomic.Int64
	compactions  atomic.Int64
	stalls       atomic.Int64
	droppedVers  atomic.Int64
	droppedTombs atomic.Int64
	gets         atomic.Int64
	tableProbes  atomic.Int64 // SSTables consulted by Get (read amplification)
	retiredReads atomic.Int64 // block reads/filter skips of tables already compacted away
	retiredSkips atomic.Int64
}

// Open opens (creating if needed) the database in dir and recovers it from
// the MANIFEST and WAL.
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
	if o.MemtableSize <= 0 {
		o.MemtableSize = defaultMemtableSize
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
	d := &DB{
		dir: dir, opts: o, lock: lock,
		threshold: max(1, min(o.MemtableSize, o.MaxMemtableBytes/2)),
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
	}
	d.cond = sync.NewCond(&d.mu)
	if err := d.recover(); err != nil {
		d.closeFiles()
		lock.release()
		return nil, err
	}
	d.flushDone.Add(1)
	go d.flushLoop()
	d.compactDone.Add(1)
	go d.compactLoop()
	if o.Sync == SyncPeriodic {
		d.stopSync = make(chan struct{})
		d.syncDone = make(chan struct{})
		go d.periodicSync()
	}
	d.signal(d.compactCh) // tables from a previous run may need merging
	return d, nil
}

// recover rebuilds the DB state:
//
//  1. Replay the MANIFEST to learn the live SSTables and which WAL files
//     are already flushed (those numbered below LogNumber).
//  2. Delete garbage: tables not in the MANIFEST (a crash between writing
//     a table and committing it) and WALs already flushed (a crash between
//     the commit and the deletion).
//  3. Replay the remaining WALs, oldest first, into a memtable, and flush
//     it to an SSTable so recovery always starts from an empty WAL.
func (d *DB) recover() error {
	man, st, err := manifest.Open(d.dir)
	if err != nil {
		return err
	}
	d.man = man
	d.nextFile = max(st.NextFile, 1)
	last := st.LastSeq
	live := map[uint64]bool{}
	for _, fm := range st.NewestFirst() {
		th, err := d.openTable(fm)
		if err != nil {
			return fmt.Errorf("db: opening live table %d: %w", fm.Num, err)
		}
		d.tables = append(d.tables, th)
		live[fm.Num] = true
		last = max(last, fm.MaxSeq)
	}

	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return err
	}
	var logs []string
	var logNums []uint64
	hasLegacy := false
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(d.dir, name)
		switch {
		case name == legacyWALName:
			hasLegacy = true
		case strings.HasSuffix(name, ".sst.tmp"):
			os.Remove(path)
		case strings.HasSuffix(name, ".log"), strings.HasSuffix(name, ".sst"):
			num, err := strconv.ParseUint(name[:len(name)-4], 10, 64)
			if err != nil {
				continue // not ours
			}
			d.nextFile = max(d.nextFile, num+1)
			if strings.HasSuffix(name, ".sst") {
				if !live[num] {
					os.Remove(path)
				}
			} else if num < st.LogNumber {
				os.Remove(path)
			} else {
				logNums = append(logNums, num)
			}
		}
	}
	sort.Slice(logNums, func(i, j int) bool { return logNums[i] < logNums[j] })
	if hasLegacy {
		logs = append(logs, filepath.Join(d.dir, legacyWALName))
	}
	for _, n := range logNums {
		logs = append(logs, filepath.Join(d.dir, logName(n)))
	}

	mem := memtable.New()
	for _, path := range logs {
		n := 0
		_, err := wal.Recover(path, func(p []byte) error {
			seq, kind, key, value, err := decodeRecord(p)
			if err == nil && seq <= last {
				err = fmt.Errorf("sequence number %d after %d", seq, last)
			}
			if err == nil {
				if kind == base.KindSet {
					err = mem.Put(key, value, seq) // memtable copies key/value
				} else {
					err = mem.Delete(key, seq)
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
			return fmt.Errorf("db: replaying %s: %w", filepath.Base(path), err)
		}
	}

	// Commit the recovered state: the replayed writes become a table, and a
	// fresh WAL starts empty.
	edit := manifest.Edit{SetLastSeq: true, LastSeq: last}
	var flushed *tableHandle
	if !mem.Empty() {
		it := mem.NewIterator()
		it.SeekToFirst()
		fm, err := d.writeTable(it, nil)
		if err != nil {
			return fmt.Errorf("db: flushing recovered WAL: %w", err)
		}
		d.flushBytes.Add(fm.Size)
		edit.Added = append(edit.Added, fm)
		if flushed, err = d.openTable(fm); err != nil {
			return err
		}
	}
	d.logNum = d.nextFile
	d.nextFile++
	edit.SetLogNumber, edit.LogNumber = true, d.logNum
	edit.SetNextFile, edit.NextFile = true, d.nextFile
	if err := d.man.Apply(edit); err != nil {
		if flushed != nil {
			flushed.r.Unref()
		}
		return err
	}
	if flushed != nil {
		d.tables = append([]*tableHandle{flushed}, d.tables...)
	}
	for _, path := range logs {
		os.Remove(path)
	}
	if d.log, err = wal.OpenWriter(filepath.Join(d.dir, logName(d.logNum))); err != nil {
		return err
	}
	d.mem = memtable.New()
	d.nextSeq = last + 1 // seq 0 is reserved so "snapshot 0" sees nothing
	d.visibleSeq.Store(uint64(last))
	return nil
}

func (d *DB) openTable(fm manifest.FileMeta) (*tableHandle, error) {
	r, err := sstable.Open(filepath.Join(d.dir, tableName(fm.Num)))
	if err != nil {
		return nil, err
	}
	s, err1 := base.DecodeInternalKey(fm.Smallest)
	l, err2 := base.DecodeInternalKey(fm.Largest)
	if err1 != nil || err2 != nil {
		r.Unref()
		return nil, base.ErrCorruptKey
	}
	return &tableHandle{meta: fm, r: r, smallest: s.UserKey, largest: l.UserKey}, nil
}

func (d *DB) allocFile() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := d.nextFile
	d.nextFile++
	return n
}

// writeTable writes the entries of it (already positioned) to a new table.
// If filter is non-nil it decides what survives (compaction); otherwise
// every entry is written (flush). It returns the table's MANIFEST entry, or
// a zero FileMeta with Num == 0 if filter dropped everything.
func (d *DB) writeTable(it compaction.Iterator, filter func(compaction.Iterator, func(base.InternalKey, []byte) error) error) (manifest.FileMeta, error) {
	num := d.allocFile()
	w, err := sstable.Create(filepath.Join(d.dir, tableName(num)), sstable.WriterOptions{
		BlockSize: d.opts.BlockSize, BloomFPRate: d.opts.BloomFPRate,
	})
	if err != nil {
		return manifest.FileMeta{}, err
	}
	emitted := false
	emit := func(k base.InternalKey, v []byte) error {
		emitted = true
		return w.Add(k, v)
	}
	if filter != nil {
		err = filter(it, emit)
	} else {
		for ; it.Valid() && err == nil; it.Next() {
			err = emit(it.Key(), it.Value())
		}
		if err == nil {
			err = it.Err()
		}
	}
	if err != nil {
		w.Abort()
		return manifest.FileMeta{}, err
	}
	if !emitted {
		w.Abort()
		return manifest.FileMeta{}, nil
	}
	meta, err := w.Finish()
	if err != nil {
		return manifest.FileMeta{}, err
	}
	return manifest.FileMeta{
		Num: num, Size: meta.Size, Entries: meta.Entries,
		MinSeq: meta.MinSeq, MaxSeq: meta.MaxSeq,
		Smallest: meta.Smallest.Encode(nil), Largest: meta.Largest.Encode(nil),
	}, nil
}

// ---------------------------------------------------------------- writes

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
	if err := d.makeRoom(); err != nil {
		d.writeMu.Unlock()
		return err
	}
	seq := d.nextSeq
	log, mem := d.log, d.mem

	// 1. Log first. If the process dies after this, replay restores it.
	d.buf = encodeRecord(d.buf[:0], seq, kind, key, value)
	off, err := log.Append(d.buf)
	if err == nil && d.opts.Sync == SyncAlways {
		err = log.SyncTo(off)
	}
	if err != nil {
		d.writeMu.Unlock()
		return err
	}

	// 2. Apply to the memtable. Readers cannot see it yet: their snapshot
	// is visibleSeq, which is still < seq.
	if kind == base.KindSet {
		err = mem.Put(key, value, seq)
	} else {
		err = mem.Delete(key, seq)
	}
	if err != nil {
		d.writeMu.Unlock()
		return err // unreachable unless there is a bug: seq is fresh
	}
	d.nextSeq++
	d.userBytes.Add(int64(len(key) + len(value)))
	d.walBytes.Add(int64(len(d.buf) + 8))
	d.writeMu.Unlock()

	// 3. Group commit: fsync outside the write lock, so other writers can
	// append meanwhile and share the next fsync. If the log was rotated in
	// the meantime, rotation already closed (and so synced) it, and SyncTo
	// returns at once.
	if d.opts.Sync == SyncGroup {
		if err := log.SyncTo(off); err != nil {
			return err
		}
	}

	// 4. Publish. The log is sequential, so a sync up to off also covers
	// every earlier write in it, and earlier logs were synced when they
	// were rotated out.
	d.publish(seq)
	return nil
}

// makeRoom ensures the active memtable has room, rotating it if it is
// full. If the previous memtable is still being flushed, the writer waits
// (a write stall) instead of letting memory grow without bound.
// writeMu must be held.
func (d *DB) makeRoom() error {
	for d.mem.ApproximateSize() >= d.threshold {
		d.mu.Lock()
		if d.imm != nil {
			d.stalls.Add(1)
		}
		for d.imm != nil && d.bgErr == nil && !d.closing {
			d.cond.Wait()
		}
		err, closing := d.bgErr, d.closing
		d.mu.Unlock()
		if err != nil {
			return err
		}
		if closing {
			return ErrClosed
		}
		if err := d.rotate(); err != nil {
			return err
		}
	}
	return nil
}

// rotate freezes the active memtable and starts a new WAL file.
// writeMu must be held and imm must be nil.
func (d *DB) rotate() error {
	num := d.allocFile()
	w, err := wal.OpenWriter(filepath.Join(d.dir, logName(num)))
	if err != nil {
		return err
	}
	// Close syncs the old log, so everything in the frozen memtable is
	// durable before it can be flushed and the log deleted.
	if err := d.log.Close(); err != nil {
		w.Close()
		os.Remove(filepath.Join(d.dir, logName(num)))
		return err
	}
	d.mu.Lock()
	d.imm, d.immLogNext = d.mem, num
	d.mem = memtable.New()
	d.log, d.logNum = w, num
	d.mu.Unlock()
	d.signal(d.flushCh)
	return nil
}

func (d *DB) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
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
			d.mu.RLock()
			log := d.log
			d.mu.RUnlock()
			// A failure is sticky inside the WAL writer, so every later
			// write returns it; nothing more to do here.
			_ = log.Sync()
		}
	}
}

func (d *DB) setBgErr(err error) {
	d.mu.Lock()
	if d.bgErr == nil {
		d.bgErr = fmt.Errorf("%w: %w", ErrBackground, err)
	}
	d.cond.Broadcast()
	d.mu.Unlock()
}

// ---------------------------------------------------------------- flush

func (d *DB) flushLoop() {
	defer d.flushDone.Done()
	for range d.flushCh {
		if err := d.flushImm(); err != nil {
			d.setBgErr(err)
		}
	}
}

// flushImm writes the immutable memtable to an SSTable and commits it.
//
// Ordering makes every crash point safe:
//  1. table written and fsynced under a temp name, then renamed
//     (crash: an unreferenced table, deleted as garbage on recovery)
//  2. MANIFEST edit fsynced: table added, LogNumber advanced
//     (the commit point; crash before it: the old WAL is replayed)
//  3. old WAL deleted (crash before it: deleted as garbage on recovery)
func (d *DB) flushImm() error {
	d.mu.RLock()
	imm, logNext := d.imm, d.immLogNext
	d.mu.RUnlock()
	if imm == nil {
		return nil
	}
	it := imm.NewIterator()
	it.SeekToFirst()
	fm, err := d.writeTable(it, nil)
	if err != nil {
		return err
	}
	d.mu.RLock()
	nextFile := d.nextFile
	d.mu.RUnlock()
	d.manifestMu.Lock()
	err = d.man.Apply(manifest.Edit{
		Added:        []manifest.FileMeta{fm},
		SetLogNumber: true, LogNumber: logNext,
		SetNextFile: true, NextFile: nextFile,
		SetLastSeq: true, LastSeq: fm.MaxSeq,
	})
	d.manifestMu.Unlock()
	if err != nil {
		os.Remove(filepath.Join(d.dir, tableName(fm.Num)))
		return err
	}
	th, err := d.openTable(fm)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.tables = append([]*tableHandle{th}, d.tables...)
	d.imm = nil
	d.cond.Broadcast()
	d.mu.Unlock()

	d.flushes.Add(1)
	d.flushBytes.Add(fm.Size)
	d.removeLogsBelow(logNext)
	if !d.opts.DisableAutoCompaction {
		d.signal(d.compactCh)
	}
	return nil
}

func (d *DB) removeLogsBelow(num uint64) {
	entries, _ := os.ReadDir(d.dir)
	for _, e := range entries {
		name := e.Name()
		if name == legacyWALName {
			os.Remove(filepath.Join(d.dir, name))
			continue
		}
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		if n, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 10, 64); err == nil && n < num {
			os.Remove(filepath.Join(d.dir, name))
		}
	}
}

// Flush writes the active memtable to an SSTable and waits for it to be
// committed. Writes continue to be accepted meanwhile.
func (d *DB) Flush() error {
	d.writeMu.Lock()
	if d.closed.Load() {
		d.writeMu.Unlock()
		return ErrClosed
	}
	if !d.mem.Empty() {
		d.mu.Lock()
		for d.imm != nil && d.bgErr == nil {
			d.cond.Wait()
		}
		err := d.bgErr
		d.mu.Unlock()
		if err == nil {
			err = d.rotate()
		}
		if err != nil {
			d.writeMu.Unlock()
			return err
		}
	}
	d.writeMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	for d.imm != nil && d.bgErr == nil {
		d.cond.Wait()
	}
	return d.bgErr
}

// ---------------------------------------------------------------- compaction

func (d *DB) compactLoop() {
	defer d.compactDone.Done()
	for range d.compactCh {
		if d.opts.DisableAutoCompaction {
			continue
		}
		for {
			did, err := d.maybeCompact()
			if err != nil {
				d.setBgErr(err)
				break
			}
			if !did {
				break
			}
		}
	}
}

func (d *DB) liveTables() ([]*tableHandle, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return append([]*tableHandle(nil), d.tables...), d.closing || d.bgErr != nil
}

func (d *DB) maybeCompact() (bool, error) {
	d.compactMu.Lock()
	defer d.compactMu.Unlock()
	tables, stop := d.liveTables()
	if stop {
		return false, nil
	}
	sizes := make([]int64, len(tables))
	for i, t := range tables {
		sizes[i] = t.meta.Size
	}
	s, e, ok := compaction.Pick(sizes, d.opts.Compaction)
	if !ok {
		return false, nil
	}
	return true, d.compact(tables, s, e)
}

// Compact merges every SSTable into one, dropping shadowed versions and
// (since the merge includes the oldest table) tombstones.
func (d *DB) Compact() error {
	if d.closed.Load() {
		return ErrClosed
	}
	d.compactMu.Lock()
	defer d.compactMu.Unlock()
	tables, _ := d.liveTables()
	if len(tables) == 0 {
		return nil
	}
	return d.compact(tables, 0, len(tables))
}

// compact merges tables[s:e] (adjacent, newest-first) into one table.
// compactMu must be held.
func (d *DB) compact(tables []*tableHandle, s, e int) error {
	inputs := tables[s:e]
	bottom := e == len(tables) // nothing older exists below the merge
	horizon := base.SeqNum(d.visibleSeq.Load())

	children := make([]compaction.Iterator, len(inputs))
	for i, t := range inputs {
		children[i] = t.r.NewIterator()
	}
	mi := compaction.NewMergingIterator(children...)
	mi.SeekToFirst()
	var st compaction.Stats
	fm, err := d.writeTable(mi, func(it compaction.Iterator, emit func(base.InternalKey, []byte) error) error {
		var ferr error
		st, ferr = compaction.Filter(it, bottom, horizon, emit)
		return ferr
	})
	if cerr := mi.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}

	edit := manifest.Edit{}
	for _, t := range inputs {
		edit.Deleted = append(edit.Deleted, t.meta.Num)
	}
	var out *tableHandle
	if fm.Num != 0 {
		edit.Added = []manifest.FileMeta{fm}
	}
	d.mu.RLock()
	edit.SetNextFile, edit.NextFile = true, d.nextFile
	d.mu.RUnlock()
	d.manifestMu.Lock()
	err = d.man.Apply(edit)
	d.manifestMu.Unlock()
	if err != nil {
		if fm.Num != 0 {
			os.Remove(filepath.Join(d.dir, tableName(fm.Num)))
		}
		return err
	}
	if fm.Num != 0 {
		if out, err = d.openTable(fm); err != nil {
			return err
		}
	}

	// Install: flushes may have prepended tables meanwhile, but only
	// compaction removes tables, so the inputs are still contiguous.
	d.mu.Lock()
	idx := -1
	for i, t := range d.tables {
		if t == inputs[0] {
			idx = i
			break
		}
	}
	if idx < 0 || idx+len(inputs) > len(d.tables) {
		d.mu.Unlock()
		return errors.New("db: compaction inputs vanished")
	}
	next := append([]*tableHandle(nil), d.tables[:idx]...)
	if out != nil {
		next = append(next, out)
	}
	next = append(next, d.tables[idx+len(inputs):]...)
	d.tables = next
	d.mu.Unlock()

	// Readers that started before the install still hold references to the
	// inputs; each file is deleted when its last reference is dropped.
	for _, t := range inputs {
		d.retiredReads.Add(t.r.BlockReads.Load())
		d.retiredSkips.Add(t.r.FilterSkips.Load())
		t.r.MarkObsolete(nil)
		t.r.Unref()
	}
	d.compactions.Add(1)
	d.compactBytes.Add(fm.Size)
	d.droppedVers.Add(st.DroppedVersions)
	d.droppedTombs.Add(st.DroppedTombstones)
	return nil
}

// ---------------------------------------------------------------- reads

// readState is a consistent view for one read: everything with a sequence
// number <= snap is in exactly the memtables and tables listed.
type readState struct {
	snap   base.SeqNum
	mem    *memtable.Memtable
	imm    *memtable.Memtable
	tables []*tableHandle
}

// acquire takes a view. The snapshot is read under the same lock as the
// structure, so a compaction installed before the view has a horizon <=
// snap (it captured visibleSeq earlier, and visibleSeq only grows).
func (d *DB) acquire() *readState {
	d.mu.RLock()
	rs := &readState{
		snap:   base.SeqNum(d.visibleSeq.Load()),
		mem:    d.mem,
		imm:    d.imm,
		tables: append([]*tableHandle(nil), d.tables...),
	}
	for _, t := range rs.tables {
		t.r.Ref()
	}
	d.mu.RUnlock()
	return rs
}

func (rs *readState) release() {
	for _, t := range rs.tables {
		t.r.Unref()
	}
}

// Get returns the current value of key, or ErrNotFound.
func (d *DB) Get(key []byte) ([]byte, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	rs := d.acquire()
	defer rs.release()
	d.gets.Add(1)

	for _, m := range []*memtable.Memtable{rs.mem, rs.imm} {
		if m == nil {
			continue
		}
		v, res := m.Get(key, rs.snap)
		switch res {
		case memtable.Found:
			return v, nil
		case memtable.Deleted:
			// A tombstone ends the search: older tables may still hold a
			// value for this key, but it has been deleted.
			return nil, ErrNotFound
		}
	}
	for _, t := range rs.tables {
		if t.meta.MinSeq > rs.snap || bytes.Compare(key, t.smallest) < 0 || bytes.Compare(key, t.largest) > 0 {
			continue
		}
		d.tableProbes.Add(1)
		v, kind, found, err := t.r.Get(key, rs.snap)
		if err != nil {
			return nil, err
		}
		if found {
			if kind == base.KindDelete {
				return nil, ErrNotFound
			}
			return v, nil
		}
	}
	return nil, ErrNotFound
}

// KV is one key/value pair returned by Scan.
type KV struct {
	Key   []byte
	Value []byte
}

// Scan returns live key/value pairs with start <= key < end in key order,
// at most limit of them (limit <= 0 means no limit). A nil start means the
// first key; a nil end means no upper bound. The result is a consistent
// snapshot: concurrent writes are either fully visible or not at all.
func (d *DB) Scan(start, end []byte, limit int) ([]KV, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	rs := d.acquire()
	defer rs.release()

	var children []compaction.Iterator
	children = append(children, rs.mem.NewIterator())
	if rs.imm != nil {
		children = append(children, rs.imm.NewIterator())
	}
	for _, t := range rs.tables {
		children = append(children, t.r.NewIterator())
	}
	mi := compaction.NewMergingIterator(children...)
	defer mi.Close()

	var (
		out     []KV
		lastKey []byte
		haveKey bool
	)
	for mi.Seek(base.MakeSearchKey(start, base.MaxSeqNum)); mi.Valid(); mi.Next() {
		k := mi.Key()
		if end != nil && bytes.Compare(k.UserKey, end) >= 0 {
			break
		}
		if k.Seq > rs.snap {
			continue // written after this scan's snapshot
		}
		if haveKey && bytes.Equal(k.UserKey, lastKey) {
			continue // older version of a key already decided
		}
		lastKey = append(lastKey[:0], k.UserKey...)
		haveKey = true
		if k.Kind == base.KindSet {
			out = append(out, KV{
				Key:   append([]byte(nil), k.UserKey...),
				Value: append([]byte{}, mi.Value()...),
			})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, mi.Err()
}

// ---------------------------------------------------------------- stats

// Stats is a point-in-time summary of the DB.
type Stats struct {
	Entries       int    // memtable entries (active + immutable), all versions incl. tombstones
	MemtableBytes int64  // approximate memory of the active memtable
	MaxMemtable   int64  // flush threshold for the active memtable
	WALBytes      int64  // current WAL file size
	LastSeq       uint64 // highest visible sequence number
	SyncPolicy    string

	Tables        int    // live SSTables
	TableBytes    int64  // total SSTable size on disk
	TableEntries  uint64 // entries across SSTables
	ManifestBytes int64

	Flushes         int64
	Compactions     int64
	WriteStalls     int64
	DroppedVersions int64 // shadowed versions removed by compaction
	DroppedTombs    int64 // tombstones removed by compaction

	UserBytesWritten       int64   // key + value bytes accepted
	WALBytesWritten        int64   // bytes appended to the WAL
	FlushBytesWritten      int64   // bytes written by flushes
	CompactionBytesWritten int64   // bytes written by compactions
	WriteAmplification     float64 // (WAL + flush + compaction bytes) / user bytes
	SpaceAmplification     float64 // SSTable bytes / live user bytes (0 if unknown)

	Gets             int64
	TableProbes      int64 // SSTables consulted by Get after range checks
	BlockReads       int64 // data blocks read from disk
	BloomFilterSkips int64 // SSTable lookups avoided by bloom filters
}

// Stats returns current statistics.
func (d *DB) Stats() Stats {
	d.mu.RLock()
	st := Stats{
		Entries:       d.mem.Len(),
		MemtableBytes: d.mem.ApproximateSize(),
		MaxMemtable:   d.threshold,
		WALBytes:      d.log.Size(),
		LastSeq:       d.visibleSeq.Load(),
		SyncPolicy:    d.opts.Sync.String(),
		Tables:        len(d.tables),
	}
	if d.imm != nil {
		st.Entries += d.imm.Len()
	}
	st.BlockReads = d.retiredReads.Load()
	st.BloomFilterSkips = d.retiredSkips.Load()
	for _, t := range d.tables {
		st.TableBytes += t.meta.Size
		st.TableEntries += t.meta.Entries
		st.BlockReads += t.r.BlockReads.Load()
		st.BloomFilterSkips += t.r.FilterSkips.Load()
	}
	d.mu.RUnlock()
	if !d.closed.Load() {
		d.manifestMu.Lock()
		if d.man != nil {
			st.ManifestBytes = d.man.Size()
		}
		d.manifestMu.Unlock()
	}
	st.Flushes = d.flushes.Load()
	st.Compactions = d.compactions.Load()
	st.WriteStalls = d.stalls.Load()
	st.DroppedVersions = d.droppedVers.Load()
	st.DroppedTombs = d.droppedTombs.Load()
	st.UserBytesWritten = d.userBytes.Load()
	st.WALBytesWritten = d.walBytes.Load()
	st.FlushBytesWritten = d.flushBytes.Load()
	st.CompactionBytesWritten = d.compactBytes.Load()
	if st.UserBytesWritten > 0 {
		st.WriteAmplification = float64(st.WALBytesWritten+st.FlushBytesWritten+st.CompactionBytesWritten) / float64(st.UserBytesWritten)
	}
	st.Gets = d.gets.Load()
	st.TableProbes = d.tableProbes.Load()
	return st
}

// ---------------------------------------------------------------- close

// Close stops background work, syncs the WAL, releases the directory lock,
// and makes further operations return ErrClosed. The active memtable is not
// flushed; its writes are in the WAL and are recovered on the next Open.
// Close is idempotent.
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
	d.mu.Lock()
	d.closing = true
	d.cond.Broadcast()
	d.mu.Unlock()
	// The flusher may signal the compactor, so stop it first.
	close(d.flushCh)
	d.flushDone.Wait()
	close(d.compactCh)
	d.compactDone.Wait()

	err := d.closeFiles()
	if lerr := d.lock.release(); err == nil {
		err = lerr
	}
	return err
}

func (d *DB) closeFiles() error {
	var err error
	if d.log != nil {
		err = d.log.Close()
	}
	d.manifestMu.Lock()
	if d.man != nil {
		if merr := d.man.Close(); err == nil {
			err = merr
		}
	}
	d.manifestMu.Unlock()
	d.mu.Lock()
	for _, t := range d.tables {
		t.r.Unref()
	}
	d.tables = nil
	d.mu.Unlock()
	return err
}
