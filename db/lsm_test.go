package db

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/compaction"
)

// smallOpts forces frequent flushes and compactions.
func smallOpts() *Options {
	return &Options{
		MemtableSize: 8 << 10,
		BlockSize:    512,
		Compaction:   compaction.PickOptions{MinMerge: 3, MaxTables: 6},
	}
}

func files(t *testing.T, dir, pattern string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, pattern))
	sort.Strings(m)
	return m
}

func TestFlushMovesDataToSSTable(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, nil)
	for i := 0; i < 100; i++ {
		d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprint(i)))
	}
	d.Delete([]byte("k050"))
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	st := d.Stats()
	if st.Tables != 1 || st.Entries != 0 || st.TableEntries != 101 || st.Flushes != 1 {
		t.Fatalf("after flush: %+v", st)
	}
	// Reads now come from the SSTable, and the tombstone in it still wins.
	expect(t, d, "k007", "7")
	expect(t, d, "k050", "")
	if len(files(t, dir, "*.sst")) != 1 || len(files(t, dir, "*.log")) != 1 {
		t.Fatalf("files: %v %v", files(t, dir, "*.sst"), files(t, dir, "*.log"))
	}
	d.Close()
	d = mustOpen(t, dir, nil)
	defer d.Close()
	expect(t, d, "k099", "99")
	expect(t, d, "k050", "")
}

// TestTombstoneShadowsOlderTable: a delete in the memtable or a newer table
// must hide a value in an older table.
func TestTombstoneShadowsOlderTable(t *testing.T) {
	d := mustOpen(t, t.TempDir(), &Options{DisableAutoCompaction: true})
	defer d.Close()
	d.Put([]byte("k"), []byte("old"))
	d.Flush()
	d.Delete([]byte("k"))
	expect(t, d, "k", "") // tombstone in memtable, value in table
	d.Flush()
	expect(t, d, "k", "") // tombstone in newer table
	d.Put([]byte("k"), []byte("new"))
	expect(t, d, "k", "new")
	if st := d.Stats(); st.Tables != 2 {
		t.Fatalf("tables = %d", st.Tables)
	}
}

func TestCompactDropsShadowedAndTombstones(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, &Options{DisableAutoCompaction: true})
	for round := 0; round < 4; round++ {
		for i := 0; i < 50; i++ {
			d.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("r%d", round)))
		}
		d.Flush()
	}
	for i := 0; i < 10; i++ {
		d.Delete([]byte(fmt.Sprintf("k%02d", i)))
	}
	d.Flush()
	if st := d.Stats(); st.Tables != 5 || st.TableEntries != 210 {
		t.Fatalf("before compaction: %+v", st)
	}
	if err := d.Compact(); err != nil {
		t.Fatal(err)
	}
	st := d.Stats()
	// 40 live keys remain; 150 old versions and 10 tombstones (plus the 10
	// values they shadowed) are gone.
	if st.Tables != 1 || st.TableEntries != 40 || st.DroppedTombs != 10 || st.DroppedVersions != 160 {
		t.Fatalf("after compaction: %+v", st)
	}
	if n := len(files(t, dir, "*.sst")); n != 1 {
		t.Fatalf("%d table files on disk, want 1 (inputs must be deleted)", n)
	}
	expect(t, d, "k05", "")
	expect(t, d, "k25", "r3")
	d.Close()
	d = mustOpen(t, dir, nil)
	defer d.Close()
	expect(t, d, "k05", "")
	expect(t, d, "k49", "r3")
}

// TestPartialCompactionKeepsTombstones: merging only newer tables must not
// drop a tombstone that hides a value in an older, unmerged table.
func TestPartialCompactionKeepsTombstones(t *testing.T) {
	d := mustOpen(t, t.TempDir(), &Options{DisableAutoCompaction: true})
	defer d.Close()
	d.Put([]byte("k"), []byte("ancient"))
	d.Flush() // oldest table holds the value
	for i := 0; i < 3; i++ {
		d.Put([]byte(fmt.Sprintf("x%d", i)), []byte("x"))
		if i == 1 {
			d.Delete([]byte("k"))
		}
		d.Flush()
	}
	d.compactMu.Lock()
	tables, _ := d.liveTables()
	err := d.compact(tables, 0, 3) // newest three; not bottom
	d.compactMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if st := d.Stats(); st.Tables != 2 || st.DroppedTombs != 0 {
		t.Fatalf("stats %+v", st)
	}
	expect(t, d, "k", "")
}

func TestScan(t *testing.T) {
	d := mustOpen(t, t.TempDir(), smallOpts())
	defer d.Close()
	model := map[string]string{}
	r := rand.New(rand.NewPCG(9, 9))
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("key%04d", r.IntN(500))
		if r.IntN(4) == 0 {
			d.Delete([]byte(k))
			delete(model, k)
		} else {
			v := fmt.Sprint(i)
			d.Put([]byte(k), []byte(v))
			model[k] = v
		}
	}
	var keys []string
	for k := range model {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	got, err := d.Scan(nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) {
		t.Fatalf("full scan: %d pairs, want %d", len(got), len(keys))
	}
	for i, kv := range got {
		if string(kv.Key) != keys[i] || string(kv.Value) != model[keys[i]] {
			t.Fatalf("pos %d: %s=%s want %s=%s", i, kv.Key, kv.Value, keys[i], model[keys[i]])
		}
	}
	// Bounded range with limit.
	got, _ = d.Scan([]byte("key0100"), []byte("key0200"), 5)
	var want []string
	for _, k := range keys {
		if k >= "key0100" && k < "key0200" && len(want) < 5 {
			want = append(want, k)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("range scan: %d, want %d", len(got), len(want))
	}
	for i := range got {
		if string(got[i].Key) != want[i] {
			t.Fatalf("range pos %d: %s want %s", i, got[i].Key, want[i])
		}
	}
	if st := d.Stats(); st.Flushes == 0 {
		t.Fatal("scan test should span SSTables")
	}
}

// TestRandomizedWithFlushAndCompaction runs a long random workload with
// tiny memtables (constant flushing and compacting), checks every key
// against a model while it runs and after reopening.
func TestRandomizedWithFlushAndCompaction(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, smallOpts())
	model := map[string]string{}
	r := rand.New(rand.NewPCG(11, 12))
	check := func(d *DB) {
		t.Helper()
		for i := 0; i < 400; i++ {
			k := fmt.Sprintf("k%03d", i)
			expect(t, d, k, model[k])
		}
	}
	for round := 0; round < 4; round++ {
		for i := 0; i < 5000; i++ {
			k := fmt.Sprintf("k%03d", r.IntN(400))
			if r.IntN(5) == 0 {
				if err := d.Delete([]byte(k)); err != nil {
					t.Fatal(err)
				}
				delete(model, k)
			} else {
				v := strings.Repeat(string(rune('a'+round)), 1+r.IntN(40))
				if err := d.Put([]byte(k), []byte(v)); err != nil {
					t.Fatal(err)
				}
				model[k] = v
			}
		}
		check(d)
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		d = mustOpen(t, dir, smallOpts())
		check(d)
	}
	st := d.Stats()
	t.Logf("flushes %d, compactions %d, tables %d, write amp %.2f", st.Flushes, st.Compactions, st.Tables, st.WriteAmplification)
	if st.Tables > 8 {
		t.Fatalf("compaction is not keeping up: %d tables", st.Tables)
	}
	d.Close()
	// No garbage left: only live tables, one WAL, MANIFEST and LOCK.
	d = mustOpen(t, dir, nil)
	defer d.Close()
	if got, want := len(files(t, dir, "*.sst")), d.Stats().Tables; got != want {
		t.Fatalf("%d .sst files, %d live", got, want)
	}
	if n := len(files(t, dir, "*.log")); n != 1 {
		t.Fatalf("%d WAL files, want 1", n)
	}
}

// TestConcurrentReadsDuringCompaction: readers and scanners run while
// writers force flushes and compactions. Every read must see a value that
// was written for that key (values encode the key), and no read may fail.
func TestConcurrentReadsDuringCompaction(t *testing.T) {
	d := mustOpen(t, t.TempDir(), smallOpts())
	defer d.Close()
	const keys = 300
	for i := 0; i < keys; i++ {
		d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("k%03d-0", i)))
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 4000; i++ {
				k := fmt.Sprintf("k%03d", (i*7+w)%keys)
				if err := d.Put([]byte(k), []byte(fmt.Sprintf("%s-%d", k, i))); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("k%03d", i%keys)
				v, err := d.Get([]byte(k))
				if err != nil || !bytes.HasPrefix(v, []byte(k+"-")) {
					errs <- fmt.Errorf("Get(%s) = %q, %v", k, v, err)
					return
				}
				if i%50 == 0 {
					kvs, err := d.Scan(nil, nil, 0)
					if err != nil || len(kvs) != keys {
						errs <- fmt.Errorf("scan: %d pairs, %v", len(kvs), err)
						return
					}
				}
			}
		}()
	}
	go func() {
		// Stop readers once writers are done.
		for d.Stats().LastSeq < keys+3*4000 {
			select {
			case <-stop:
				return
			default:
			}
		}
		close(stop)
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if st := d.Stats(); st.Compactions == 0 {
		t.Fatalf("expected compactions during the test: %+v", st)
	}
}

// TestRecoveryDeletesGarbage: an unreferenced table (crash after writing a
// table, before committing it) and a stale temp file are removed on Open
// and never read.
func TestRecoveryDeletesGarbage(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, nil)
	d.Put([]byte("k"), []byte("v"))
	d.Flush()
	d.Close()
	// A bogus table that the MANIFEST does not mention, holding a key that
	// must not appear, plus a leftover temp file.
	live := files(t, dir, "*.sst")[0]
	data, _ := os.ReadFile(live)
	os.WriteFile(filepath.Join(dir, "000999.sst"), data, 0o644)
	os.WriteFile(filepath.Join(dir, "001000.sst.tmp"), []byte("junk"), 0o644)

	d = mustOpen(t, dir, nil)
	defer d.Close()
	if _, err := os.Stat(filepath.Join(dir, "000999.sst")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("orphan table not deleted")
	}
	if _, err := os.Stat(filepath.Join(dir, "001000.sst.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temp file not deleted")
	}
	if d.Stats().Tables != 1 {
		t.Fatalf("tables = %d", d.Stats().Tables)
	}
	expect(t, d, "k", "v")
}

// TestUpgradeFromSingleWAL: a directory written by the W2 engine (one
// wal.log, no MANIFEST) is recovered and migrated.
func TestUpgradeFromSingleWAL(t *testing.T) {
	dir := t.TempDir()
	var buf []byte
	var rec []byte
	for i, kv := range [][2]string{{"a", "1"}, {"b", "2"}} {
		rec = encodeRecord(rec[:0], seqOf(i+1), 1, []byte(kv[0]), []byte(kv[1]))
		buf = appendFramed(buf, rec)
	}
	os.WriteFile(filepath.Join(dir, legacyWALName), buf, 0o644)
	d := mustOpen(t, dir, nil)
	expect(t, d, "a", "1")
	expect(t, d, "b", "2")
	if _, err := os.Stat(filepath.Join(dir, legacyWALName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("legacy wal.log not removed after migration")
	}
	d.Put([]byte("c"), []byte("3"))
	if d.Stats().LastSeq != 3 {
		t.Fatalf("LastSeq = %d", d.Stats().LastSeq)
	}
	d.Close()
	d = mustOpen(t, dir, nil)
	defer d.Close()
	expect(t, d, "a", "1")
	expect(t, d, "c", "3")
}
