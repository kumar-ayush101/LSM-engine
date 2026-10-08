package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrLocked is returned by Open when another process (or another DB in this
// process) already has the directory open. Two writers appending to the same
// WAL would interleave records and corrupt it.
var ErrLocked = errors.New("db: directory is locked by another process")

const lockFileName = "LOCK"

// dirLock is an OS-level advisory lock on dir/LOCK. The OS releases it when
// the process exits, including on kill -9, so a crash never leaves a stale
// lock behind (unlike a create-exclusive "lock file" scheme).
type dirLock struct {
	f *os.File
}

func lockDir(dir string) (*dirLock, error) {
	f, err := os.OpenFile(filepath.Join(dir, lockFileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := lockFile(f); err != nil {
		f.Close()
		if errors.Is(err, errWouldBlock) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
		}
		return nil, fmt.Errorf("db: locking %s: %w", dir, err)
	}
	return &dirLock{f: f}, nil
}

func (l *dirLock) release() error {
	// Closing the file releases the lock on every platform.
	return l.f.Close()
}

var errWouldBlock = errors.New("lock held")
