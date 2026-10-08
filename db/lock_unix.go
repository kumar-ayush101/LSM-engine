//go:build unix

package db

import (
	"errors"
	"os"
	"syscall"
)

func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return errWouldBlock
	}
	return err
}
