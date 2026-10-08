package sstable

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

type entry struct {
	key   base.InternalKey
	value string
}

func sorted(entries []entry) []entry {
	sort.Slice(entries, func(i, j int) bool { return base.Compare(entries[i].key, entries[j].key) < 0 })
	return entries
}

func build(t *testing.T, entries []entry, opts WriterOptions) (*Reader, Meta) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.sst")
	w, err := Create(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := w.Add(e.key, []byte(e.value)); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temp file left behind")
	}
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Unref)
	return r, meta
}

func genEntries(n int) []entry {
	r := rand.New(rand.NewPCG(1, 2))
	var out []entry
	seq := base.SeqNum(1)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%05d", i)
		for v := 0; v <= r.IntN(3); v++ {
			kind := base.KindSet
			val := fmt.Sprintf("val-%d-%d", i, seq)
			if r.IntN(5) == 0 {
				kind, val = base.KindDelete, ""
			}
			out = append(out, entry{base.MakeInternalKey([]byte(k), seq, kind), val})
			seq++
		}
	}
	return sorted(out)
}

func TestRoundTripIteration(t *testing.T) {
	entries := genEntries(3000)
	r, meta := build(t, entries, WriterOptions{BlockSize: 512})
	if meta.Entries != uint64(len(entries)) || r.Entries() != meta.Entries {
		t.Fatalf("entries: meta %d reader %d want %d", meta.Entries, r.Entries(), len(entries))
	}
	if len(r.index) < 10 {
		t.Fatalf("expected many blocks, got %d", len(r.index))
	}
	if base.Compare(meta.Smallest, entries[0].key) != 0 || base.Compare(meta.Largest, entries[len(entries)-1].key) != 0 {
		t.Fatalf("bounds %v..%v", meta.Smallest, meta.Largest)
	}
	it := r.NewIterator()
	defer it.Close()
	i := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if base.Compare(it.Key(), entries[i].key) != 0 || string(it.Value()) != entries[i].value {
			t.Fatalf("entry %d: got %v=%q want %v=%q", i, it.Key(), it.Value(), entries[i].key, entries[i].value)
		}
		i++
	}
	if it.Err() != nil || i != len(entries) {
		t.Fatalf("iterated %d of %d, err %v", i, len(entries), it.Err())
	}
}

func TestSeekAndGet(t *testing.T) {
	entries := genEntries(2000)
	r, _ := build(t, entries, WriterOptions{BlockSize: 256})
	// Every entry is reachable by Seek to its exact key.
	it := r.NewIterator()
	defer it.Close()
	for _, e := range entries {
		it.Seek(e.key)
		if !it.Valid() || base.Compare(it.Key(), e.key) != 0 {
			t.Fatalf("Seek(%v) landed on %v", e.key, it.Key())
		}
	}
	// Get honours snapshots: model the newest version <= snapshot.
	rng := rand.New(rand.NewPCG(3, 4))
	for i := 0; i < 3000; i++ {
		uk := fmt.Sprintf("key-%05d", rng.IntN(2100))
		snap := base.SeqNum(rng.IntN(6000))
		var want *entry
		for j := range entries {
			e := &entries[j]
			if string(e.key.UserKey) == uk && e.key.Seq <= snap {
				want = e // entries for a key are newest-first; first match wins
				break
			}
		}
		v, kind, found, err := r.Get([]byte(uk), snap)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case want == nil && found:
			t.Fatalf("Get(%s@%d) found %q, want nothing", uk, snap, v)
		case want != nil && (!found || kind != want.key.Kind || string(v) != want.value):
			t.Fatalf("Get(%s@%d) = %q,%v,%v want %v=%q", uk, snap, v, kind, found, want.key, want.value)
		}
	}
	// Seek past the end.
	it.Seek(base.MakeSearchKey([]byte("zzz"), base.MaxSeqNum))
	if it.Valid() {
		t.Fatal("seek past end should be invalid")
	}
}

func TestBloomSkipsAbsentKeys(t *testing.T) {
	r, _ := build(t, genEntries(5000), WriterOptions{})
	before := r.BlockReads.Load()
	for i := 0; i < 10000; i++ {
		if _, _, found, _ := r.Get([]byte(fmt.Sprintf("absent-%d", i)), base.MaxSeqNum); found {
			t.Fatal("found absent key")
		}
	}
	reads := r.BlockReads.Load() - before
	t.Logf("10000 absent lookups: %d block reads, %d filter skips", reads, r.FilterSkips.Load())
	if reads > 300 { // ~1% false positives
		t.Fatalf("bloom filter not effective: %d block reads", reads)
	}
}

func TestOutOfOrderRejected(t *testing.T) {
	w, err := Create(filepath.Join(t.TempDir(), "x.sst"), WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	w.Add(base.MakeInternalKey([]byte("b"), 1, base.KindSet), nil)
	if err := w.Add(base.MakeInternalKey([]byte("a"), 2, base.KindSet), nil); err == nil {
		t.Fatal("out-of-order key accepted")
	}
	if _, err := w.Finish(); err == nil {
		t.Fatal("Finish after error should fail")
	}
}

func TestEmptyTableRejected(t *testing.T) {
	dir := t.TempDir()
	w, _ := Create(filepath.Join(dir, "x.sst"), WriterOptions{})
	if _, err := w.Finish(); err == nil {
		t.Fatal("empty table should be rejected")
	}
	if files, _ := os.ReadDir(dir); len(files) != 0 {
		t.Fatalf("files left behind: %v", files)
	}
}

func TestCorruptionDetected(t *testing.T) {
	entries := genEntries(500)
	r, _ := build(t, entries, WriterOptions{BlockSize: 256})
	path := r.Path()
	orig, _ := os.ReadFile(path)

	// Flip a byte in the first data block: opening succeeds (index and filter
	// are intact) but reading that block fails its checksum.
	bad := append([]byte(nil), orig...)
	bad[10] ^= 0xff
	p2 := filepath.Join(t.TempDir(), "bad.sst")
	os.WriteFile(p2, bad, 0o644)
	r2, err := Open(p2)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	it := r2.NewIterator()
	it.SeekToFirst()
	if !errors.Is(it.Err(), ErrCorrupt) {
		t.Fatalf("data corruption not detected: %v", it.Err())
	}
	it.Close()
	r2.Unref()

	// Damaged footer or truncated file fails Open.
	for name, data := range map[string][]byte{
		"magic":     append(append([]byte(nil), orig[:len(orig)-1]...), orig[len(orig)-1]^1),
		"truncated": orig[:len(orig)-20],
		"tiny":      orig[:10],
	} {
		p := filepath.Join(t.TempDir(), name+".sst")
		os.WriteFile(p, data, 0o644)
		if r, err := Open(p); err == nil {
			r.Unref()
			t.Fatalf("%s: Open succeeded", name)
		}
	}
}

func TestRefCountingDefersDelete(t *testing.T) {
	r, _ := build(t, genEntries(100), WriterOptions{})
	r.Ref() // extra ref to balance the Cleanup Unref
	it := r.NewIterator()
	it.SeekToFirst()
	deleted := false
	r.MarkObsolete(func() { deleted = true })
	r.Unref() // the "DB" reference
	if deleted {
		t.Fatal("file deleted while an iterator holds a ref")
	}
	for ; it.Valid(); it.Next() {
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	r.Unref() // test cleanup's reference, taken early
	it.Close()
	if !deleted {
		t.Fatal("file not deleted after last ref")
	}
	if _, err := os.Stat(r.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("file still exists")
	}
	r.Ref() // let t.Cleanup's Unref be harmless: refs goes 1 -> 0 again on a closed file
}

func TestConcurrentReaders(t *testing.T) {
	entries := genEntries(2000)
	r, _ := build(t, entries, WriterOptions{BlockSize: 512})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g; i < len(entries); i += 8 {
				e := entries[i]
				it := r.NewIterator()
				it.Seek(e.key)
				if !it.Valid() || base.Compare(it.Key(), e.key) != 0 {
					t.Errorf("seek %v", e.key)
				}
				it.Close()
			}
		}(g)
	}
	wg.Wait()
}
