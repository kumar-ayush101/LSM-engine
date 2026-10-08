//go:build windows

package bench

import (
	"syscall"
	"time"
	"unsafe"
)

// On Windows, Go's time.Now advances in steps of ~0.5 ms (measured 312 µs
// on the development machine), far too coarse to time a single operation.
// QueryPerformanceCounter has sub-microsecond resolution.

var (
	kernel32 = syscall.NewLazyDLL("kernel32.dll")
	qpc      = kernel32.NewProc("QueryPerformanceCounter")
	qpf      = kernel32.NewProc("QueryPerformanceFrequency")
	qpcFreq  = func() int64 {
		var f int64
		qpf.Call(uintptr(unsafe.Pointer(&f)))
		return f
	}()
)

func ticks() int64 {
	var c int64
	qpc.Call(uintptr(unsafe.Pointer(&c)))
	return c
}

func ticksToDuration(t int64) time.Duration {
	return time.Duration(float64(t) * float64(time.Second) / float64(qpcFreq))
}
