package wal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var (
	// ErrClosed is returned by operations on a closed Writer.
	ErrClosed = errors.New("wal: closed")
	// ErrFailed wraps the first write or fsync error. After it, the Writer
	// rejects all further appends and syncs.
	//
	// The error is sticky on purpose. If fsync fails, the kernel may already
	// have dropped the dirty pages and cleared the error, so a retry can
	// "succeed" while the data is gone (the 2018 PostgreSQL "fsyncgate"
	// issue). The only safe reaction is to stop accepting writes; on restart,
	// recovery reads back what actually reached the disk.
	ErrFailed = errors.New("wal: log failed")
)

// file is the subset of *os.File the Writer uses; tests inject failures.
type file interface {
	io.Writer
	Sync() error
	Close() error
}

// Writer appends records to a log file.
//
// Append writes each record straight to the OS (no user-space buffer), so a
// process crash (kill -9) never loses an appended record. Durability against
// power loss or an OS crash additionally requires SyncTo.
//
// SyncTo implements group commit: concurrent callers queue on syncMu, and a
// single fsync covers every record appended before it started. Callers whose
// offset is already covered return without another fsync.
type Writer struct {
	syncMu sync.Mutex // held for the whole fsync; serializes syncs

	mu      sync.Mutex // guards the fields below
	f       file
	buf     []byte
	written int64 // bytes appended
	synced  int64 // bytes known durable
	err     error // sticky; wraps ErrFailed
	closed  bool
}

// OpenWriter opens the log at path for appending, creating it if needed.
// Run Recover first so the file ends on a record boundary.
//
// It fsyncs the existing contents before returning: a previous process may
// have crashed with records still in the OS page cache, and recovery just
// treated them as committed.
func OpenWriter(path string) (*Writer, error) {
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, fs.ErrNotExist)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err == nil {
		err = f.Sync()
	}
	if err == nil && created {
		// Make the new directory entry durable too.
		err = syncDir(filepath.Dir(path))
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return newWriter(f, st.Size()), nil
}

func newWriter(f file, size int64) *Writer {
	return &Writer{f: f, written: size, synced: size}
}

// Append writes one record and returns the log offset just past it. Pass
// that offset to SyncTo to make the record durable.
func (w *Writer) Append(payload []byte) (int64, error) {
	if len(payload) > MaxRecordSize {
		return 0, ErrRecordTooLarge
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, ErrClosed
	}
	w.buf = appendRecord(w.buf[:0], payload)
	n, err := w.f.Write(w.buf)
	w.written += int64(n)
	if err != nil {
		// A partial record may now be on disk. Recovery will truncate it,
		// but this Writer can no longer append valid records after it.
		w.err = fmt.Errorf("%w: write: %w", ErrFailed, err)
		return 0, w.err
	}
	if cap(w.buf) > 1<<20 {
		w.buf = nil // do not pin a large buffer after one big record
	}
	return w.written, nil
}

// SyncTo makes the log durable at least up to offset off.
func (w *Writer) SyncTo(off int64) error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()

	w.mu.Lock()
	if w.synced >= off {
		// Another caller's fsync already covered us: that is the group commit.
		w.mu.Unlock()
		return nil
	}
	if w.err != nil {
		w.mu.Unlock()
		return w.err
	}
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	target := w.written
	w.mu.Unlock()

	// fsync without holding mu, so appends continue during the slow part.
	err := w.f.Sync()

	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		if w.err == nil {
			w.err = fmt.Errorf("%w: fsync: %w", ErrFailed, err)
		}
		return w.err
	}
	if target > w.synced {
		w.synced = target
	}
	return nil
}

// Sync makes everything appended so far durable.
func (w *Writer) Sync() error {
	w.mu.Lock()
	off := w.written
	w.mu.Unlock()
	return w.SyncTo(off)
}

// Size returns the number of bytes in the log.
func (w *Writer) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written
}

// Close syncs and closes the log. It is idempotent.
func (w *Writer) Close() error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true

	err := w.err
	if err == nil {
		if serr := w.f.Sync(); serr != nil {
			w.err = fmt.Errorf("%w: fsync: %w", ErrFailed, serr)
			err = w.err
		} else {
			w.synced = w.written
		}
	}
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// syncDir fsyncs a directory so a newly created file's entry survives a
// crash. On Windows, NTFS journals directory metadata and Go cannot open a
// directory for syncing, so it is a no-op there.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
