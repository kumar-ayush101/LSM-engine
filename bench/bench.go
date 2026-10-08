// Package bench is the benchmark harness: it drives a db.DB with a
// configurable workload and reports throughput, latency percentiles, and
// write / read / space amplification taken from the engine's own counters.
//
// Every number it reports is measured on the machine it runs on; nothing is
// estimated or hard-coded. cmd/lsmbench wraps it as a command-line tool.
package bench

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kumar-ayush101/LSM-engine/db"
)

// Config describes one benchmark run.
type Config struct {
	Dir         string        // database directory (created; must be empty)
	Keys        int           // distinct keys in the key space
	ValueSize   int           // bytes per value
	Writes      int           // total Put operations
	Reads       int           // total Get operations (after the write phase)
	Concurrency int           // goroutines per phase
	Sync        db.SyncPolicy // WAL fsync policy
	MemtableMB  int           // flush threshold (default 4)
	Seed        uint64
	// MissRatio is the fraction of reads for keys that were never written,
	// which exercises the bloom filters. 0..1.
	MissRatio float64
	// NoCompaction disables background compaction.
	NoCompaction bool
}

// Phase summarizes one timed phase.
type Phase struct {
	Name     string
	Ops      int
	Duration time.Duration
	P50, P99 time.Duration
	Max      time.Duration
}

// OpsPerSec is the phase throughput.
func (p Phase) OpsPerSec() float64 {
	if p.Duration <= 0 {
		return 0
	}
	return float64(p.Ops) / p.Duration.Seconds()
}

// Result is the outcome of Run.
type Result struct {
	Config Config
	Write  Phase
	Read   Phase
	Stats  db.Stats // engine counters after the read phase

	LiveBytes          int64   // key+value bytes of the final live data set
	SpaceAmplification float64 // bytes on disk / LiveBytes, after final compaction
	DiskBytesBefore    int64   // SSTable + WAL bytes before the final Compact
	ReadAmplification  float64 // SSTables probed per Get
	FoundRatio         float64 // fraction of reads that found a value
}

func key(i int) []byte { return []byte(fmt.Sprintf("user%012d", i)) }

// Run executes the workload.
func Run(cfg Config) (Result, error) {
	if cfg.Keys <= 0 || cfg.ValueSize < 0 || cfg.Writes <= 0 {
		return Result{}, fmt.Errorf("bench: keys, writes must be > 0")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MemtableMB <= 0 {
		cfg.MemtableMB = 4
	}
	d, err := db.Open(cfg.Dir, &db.Options{
		Sync:                  cfg.Sync,
		MemtableSize:          int64(cfg.MemtableMB) << 20,
		DisableAutoCompaction: cfg.NoCompaction,
	})
	if err != nil {
		return Result{}, err
	}
	defer d.Close()

	res := Result{Config: cfg}
	var lastVal sync.Map // key index -> value length of its last write (for LiveBytes)

	// ---- write phase: uniform random keys, so keys are overwritten and
	// compaction has shadowed versions to drop.
	res.Write, err = runPhase("write", cfg.Writes, cfg.Concurrency, func(g, i int, rng *rand.Rand, buf []byte) error {
		k := rng.IntN(cfg.Keys)
		for j := range buf {
			buf[j] = byte('a' + rng.IntN(26))
		}
		if err := d.Put(key(k), buf); err != nil {
			return err
		}
		lastVal.Store(k, len(buf))
		return nil
	}, cfg.Seed, cfg.ValueSize)
	if err != nil {
		return res, err
	}
	// Let background flush/compaction settle so reads see a steady state.
	if err := d.Flush(); err != nil {
		return res, err
	}

	// ---- read phase
	var found atomic.Int64
	if cfg.Reads > 0 {
		res.Read, err = runPhase("read", cfg.Reads, cfg.Concurrency, func(g, i int, rng *rand.Rand, _ []byte) error {
			var k []byte
			if rng.Float64() < cfg.MissRatio {
				k = []byte(fmt.Sprintf("miss%012d", rng.IntN(cfg.Keys)))
			} else {
				k = key(rng.IntN(cfg.Keys))
			}
			_, err := d.Get(k)
			if err == nil {
				found.Add(1)
				return nil
			}
			if err == db.ErrNotFound {
				return nil
			}
			return err
		}, cfg.Seed+1, 0)
		if err != nil {
			return res, err
		}
		res.FoundRatio = float64(found.Load()) / float64(cfg.Reads)
	}
	res.Stats = d.Stats()
	if res.Stats.Gets > 0 {
		res.ReadAmplification = float64(res.Stats.TableProbes) / float64(res.Stats.Gets)
	}

	// ---- space amplification: disk bytes vs the live data set.
	lastVal.Range(func(k, v any) bool {
		res.LiveBytes += int64(len(key(k.(int))) + v.(int))
		return true
	})
	res.DiskBytesBefore = res.Stats.TableBytes + res.Stats.WALBytes
	if err := d.Compact(); err != nil {
		return res, err
	}
	after := d.Stats()
	if res.LiveBytes > 0 {
		res.SpaceAmplification = float64(after.TableBytes+after.WALBytes) / float64(res.LiveBytes)
	}
	return res, nil
}

// runPhase runs n ops across conc goroutines and records every latency.
func runPhase(name string, n, conc int, op func(g, i int, rng *rand.Rand, buf []byte) error, seed uint64, bufSize int) (Phase, error) {
	lat := make([]time.Duration, n)
	var next atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	start := time.Now()
	for g := 0; g < conc; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(g)))
			buf := make([]byte, bufSize)
			for {
				i := int(next.Add(1) - 1)
				if i >= n || firstErr.Load() != nil {
					return
				}
				t0 := ticks()
				if err := op(g, i, rng, buf); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				lat[i] = ticksToDuration(ticks() - t0)
			}
		}(g)
	}
	wg.Wait()
	p := Phase{Name: name, Ops: n, Duration: time.Since(start)}
	if err, _ := firstErr.Load().(error); err != nil {
		return p, err
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p.P50 = lat[n/2]
	p.P99 = lat[min(n-1, n*99/100)]
	p.Max = lat[n-1]
	return p, nil
}

// TempDir creates an empty directory for a run under base (or the OS temp
// dir), returning it and a cleanup function.
func TempDir(base string) (string, func(), error) {
	dir, err := os.MkdirTemp(base, "lsmbench-")
	if err != nil {
		return "", nil, err
	}
	return filepath.Join(dir, "db"), func() { os.RemoveAll(dir) }, nil
}

// Markdown renders results as a table row block.
func Markdown(rs []Result) string {
	var b strings.Builder
	b.WriteString("| Workload | Sync | Writes/s | Write p50 | Write p99 | Reads/s | Read p50 | Read p99 | Write amp | Read amp | Space amp | Tables | Compactions | Bloom skips |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range rs {
		c := r.Config
		fmt.Fprintf(&b, "| %d keys, %d B values, %d writers, %.0f%% misses | %s | %.0f | %s | %s | %.0f | %s | %s | %.2fx | %.2f | %.2fx | %d | %d | %d |\n",
			c.Keys, c.ValueSize, c.Concurrency, c.MissRatio*100, c.Sync,
			r.Write.OpsPerSec(), us(r.Write.P50), us(r.Write.P99),
			r.Read.OpsPerSec(), us(r.Read.P50), us(r.Read.P99),
			r.Stats.WriteAmplification, r.ReadAmplification, r.SpaceAmplification,
			r.Stats.Tables, r.Stats.Compactions, r.Stats.BloomFilterSkips)
	}
	return b.String()
}

func us(d time.Duration) string {
	if d >= time.Millisecond {
		return fmt.Sprintf("%.2f ms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.1f µs", float64(d)/float64(time.Microsecond))
}
