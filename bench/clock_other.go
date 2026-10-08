//go:build !windows

package bench

import "time"

var epoch = time.Now()

// ticks returns a monotonic timestamp in nanoseconds.
func ticks() int64 { return int64(time.Since(epoch)) }

func ticksToDuration(t int64) time.Duration { return time.Duration(t) }
