package bench

import (
	"testing"

	"github.com/kumar-ayush101/LSM-engine/db"
)

// TestRunSmall checks the harness end to end on a tiny workload: counters
// are consistent and the amplification figures are in plausible ranges.
func TestRunSmall(t *testing.T) {
	dir, cleanup, err := TempDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	r, err := Run(Config{
		Dir: dir, Keys: 2000, ValueSize: 100, Writes: 20000, Reads: 5000,
		Concurrency: 4, Sync: db.SyncPeriodic, MemtableMB: 1, MissRatio: 0.5, Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + Markdown([]Result{r}))
	if r.Write.Ops != 20000 || r.Read.Ops != 5000 || r.Write.P99 < r.Write.P50 {
		t.Fatalf("phases: %+v %+v", r.Write, r.Read)
	}
	if r.Stats.Flushes == 0 {
		t.Fatal("expected flushes")
	}
	if r.Stats.WriteAmplification < 1 {
		t.Fatalf("write amp %.2f < 1", r.Stats.WriteAmplification)
	}
	// After a full compaction, only live keys remain: space amp is near 1
	// (block framing, index and bloom filter add a little).
	if r.SpaceAmplification < 0.8 || r.SpaceAmplification > 2 {
		t.Fatalf("space amp %.2f", r.SpaceAmplification)
	}
	// Every written key is found; half the reads are for absent keys.
	if r.FoundRatio < 0.4 || r.FoundRatio > 0.6 {
		t.Fatalf("found ratio %.2f", r.FoundRatio)
	}
	if r.Stats.BloomFilterSkips == 0 {
		t.Fatal("bloom filters never skipped a table")
	}
}
