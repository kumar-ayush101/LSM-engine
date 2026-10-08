//go:build windows

package db

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

var procLockFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")

const (
	lockfileFailImmediately = 0x00000001
	lockfileExclusiveLock   = 0x00000002
	errorLockViolation      = syscall.Errno(33)
)

func lockFile(f *os.File) error {
	var ol syscall.Overlapped
	r, _, err := procLockFileEx.Call(
		f.Fd(),
		lockfileExclusiveLock|lockfileFailImmediately,
		0,    // reserved
		1, 0, // lock 1 byte
		uintptr(unsafe.Pointer(&ol)),
	)
	if r != 0 {
		return nil
	}
	if errors.Is(err, errorLockViolation) {
		return errWouldBlock
	}
	return err
}
