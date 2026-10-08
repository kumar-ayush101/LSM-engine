// Command lsmbench runs the benchmark harness and prints a Markdown table.
//
//	go run ./cmd/lsmbench                       # default suite
//	go run ./cmd/lsmbench -keys 100000 -writes 500000 -sync group -c 16
//
// All numbers are measured on the machine it runs on.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/kumar-ayush101/LSM-engine/bench"
	"github.com/kumar-ayush101/LSM-engine/db"
)

func main() {
	var (
		keys    = flag.Int("keys", 0, "distinct keys (0 = run the default suite)")
		writes  = flag.Int("writes", 200000, "Put operations")
		reads   = flag.Int("reads", 200000, "Get operations")
		valSize = flag.Int("value", 100, "value size in bytes")
		conc    = flag.Int("c", 8, "concurrent goroutines")
		syncStr = flag.String("sync", "group", "group, always or periodic")
		memMB   = flag.Int("memtable-mb", 4, "memtable flush threshold in MiB")
		miss    = flag.Float64("miss", 0.5, "fraction of reads for absent keys")
		dirBase = flag.String("dir", "", "parent directory for the temporary database (default: OS temp)")
	)
	flag.Parse()
	pol, err := db.ParseSyncPolicy(*syncStr)
	if err != nil {
		fatal(err)
	}

	var suite []bench.Config
	if *keys > 0 {
		suite = []bench.Config{{Keys: *keys, Writes: *writes, Reads: *reads, ValueSize: *valSize,
			Concurrency: *conc, Sync: pol, MemtableMB: *memMB, MissRatio: *miss}}
	} else {
		base := bench.Config{Keys: 100000, Writes: 500000, Reads: 200000, ValueSize: 100,
			Concurrency: 8, MemtableMB: 4, MissRatio: 0.5}
		for _, s := range []db.SyncPolicy{db.SyncPeriodic, db.SyncGroup} {
			c := base
			c.Sync = s
			suite = append(suite, c)
		}
		c := base
		c.Sync, c.Writes, c.Concurrency = db.SyncAlways, 20000, 8
		suite = append(suite, c)
		c = base
		c.Sync, c.Writes, c.Concurrency = db.SyncAlways, 5000, 1
		suite = append(suite, c)
	}

	fmt.Fprintf(os.Stderr, "lsmbench: %s/%s, %d CPUs, Go %s\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	var results []bench.Result
	for i, cfg := range suite {
		dir, cleanup, err := bench.TempDir(*dirBase)
		if err != nil {
			fatal(err)
		}
		cfg.Dir, cfg.Seed = dir, uint64(i+1)
		t0 := time.Now()
		r, err := bench.Run(cfg)
		cleanup()
		if err != nil {
			fatal(err)
		}
		fmt.Fprintf(os.Stderr, "  run %d/%d done in %s\n", i+1, len(suite), time.Since(t0).Round(time.Millisecond))
		results = append(results, r)
	}
	fmt.Print(bench.Markdown(results))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "lsmbench:", err)
	os.Exit(1)
}
