package db

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
	"github.com/kumar-ayush101/LSM-engine/wal"
)

var allPolicies = []SyncPolicy{SyncGroup, SyncAlways, SyncPeriodic}

func mustOpen(t *testing.T, dir string, opts *Options) *DB {
	t.Helper()
	d, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func expect(t *testing.T, d *DB, key, want string) {
	t.Helper()
	v, err := d.Get([]byte(key))
	if want == "" {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%q) = %q, %v; want ErrNotFound", key, v, err)
		}
		return
	}
	if err != nil || string(v) != want {
		t.Fatalf("Get(%q) = %q, %v; want %q", key, v, err, want)
	}
}

func TestRecordRoundTrip(t *testing.T) {
	cases := []struct {
		seq   base.SeqNum
		kind  base.Kind
		key   []byte
		value []byte
	}{
		{1, base.KindSet, []byte("k"), []byte("v")},
		{2, base.KindSet, nil, nil},
		{3, base.KindDelete, []byte("gone"), nil},
		{base.MaxSeqNum, base.KindSet, bytes.Repeat([]byte("x"), 300), []byte{0, 1, 2}},
	}
	for _, c := range cases {
		p := encodeRecord(nil, c.seq, c.kind, c.key, c.value)
		seq, kind, key, value, err := decodeRecord(p)
		if err != nil || seq != c.seq || kind != c.kind || !bytes.Equal(key, c.key) || !bytes.Equal(value, c.value) {
			t.Fatalf("round trip %+v: got %d %v %q %q %v", c, seq, kind, key, value, err)
		}
	}
}

func TestDecodeRecordRejectsGarbage(t *testing.T) {
	good := encodeRecord(nil, 5, base.KindSet, []byte("key"), []byte("val"))
	bad := [][]byte{
		nil,
		good[:8],
		append(encodeRecord(nil, 5, base.KindDelete, []byte("k"), nil), 'x'), // tombstone with value
		encodeRecord(nil, 0, base.KindSet, []byte("k"), nil),                 // seq 0 is reserved
	}
	kindBad := append([]byte(nil), good...)
	kindBad[8] = 9
	bad = append(bad, kindBad)
	lenBad := append([]byte(nil), good[:9]...)
	lenBad = append(lenBad, 100) // key length beyond payload
	bad = append(bad, lenBad)
	for i, p := range bad {
		if _, _, _, _, err := decodeRecord(p); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	for _, pol := range allPolicies {
		t.Run(pol.String(), func(t *testing.T) {
			dir := t.TempDir()
			d := mustOpen(t, dir, &Options{Sync: pol})
			d.Put([]byte("a"), []byte("1"))
			d.Put([]byte("b"), []byte("2"))
			d.Put([]byte("a"), []byte("3"))
			d.Delete([]byte("b"))
			d.Put([]byte("empty"), nil)
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}

			d = mustOpen(t, dir, &Options{Sync: pol})
			defer d.Close()
			expect(t, d, "a", "3")
			expect(t, d, "b", "")
			if v, err := d.Get([]byte("empty")); err != nil || len(v) != 0 {
				t.Fatalf("empty value: %q, %v", v, err)
			}
			// Sequence numbers continue after the replayed ones.
			if got := d.Stats().LastSeq; got != 5 {
				t.Fatalf("LastSeq after reopen = %d, want 5", got)
			}
			d.Put([]byte("c"), []byte("4"))
			if got := d.Stats().LastSeq; got != 6 {
				t.Fatalf("LastSeq after write = %d, want 6", got)
			}
		})
	}
}

// TestReopenWithoutClose simulates a process crash: the DB is never closed,
// so only what reached the OS is in the WAL. Every acknowledged write must
// still be there.
func TestReopenWithoutClose(t *testing.T) {
	for _, pol := range allPolicies {
		t.Run(pol.String(), func(t *testing.T) {
			dir := t.TempDir()
			d := mustOpen(t, dir, &Options{Sync: pol})
			for i := 0; i < 100; i++ {
				if err := d.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i*i))); err != nil {
					t.Fatal(err)
				}
			}
			// Abandon d without Close; release only the lock (the OS would
			// do this when the process dies) and the file handle.
			d.lock.release()
			d.log.Close()

			d2 := mustOpen(t, dir, &Options{Sync: pol})
			defer d2.Close()
			for i := 0; i < 100; i++ {
				expect(t, d2, fmt.Sprint(i), fmt.Sprint(i*i))
			}
		})
	}
}

func TestTornWALTailIsDiscardedOnOpen(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, nil)
	d.Put([]byte("kept"), []byte("yes"))
	d.Close()

	// Append half a record, as if the process died mid-write.
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x12, 0x34, 0x56, 0x78, 0x40, 0, 0, 0, 'p', 'a'})
	f.Close()

	d = mustOpen(t, dir, nil)
	defer d.Close()
	expect(t, d, "kept", "yes")
	d.Put([]byte("after"), []byte("ok"))
	d.Close()

	d = mustOpen(t, dir, nil)
	defer d.Close()
	expect(t, d, "kept", "yes")
	expect(t, d, "after", "ok")
}

func TestCorruptWALFailsOpen(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, nil)
	for i := 0; i < 10; i++ {
		d.Put([]byte(fmt.Sprint(i)), []byte("value"))
	}
	d.Close()

	path := filepath.Join(dir, walFileName)
	b, _ := os.ReadFile(path)
	b[20] ^= 0xff // inside the first record, with valid records after it
	os.WriteFile(path, b, 0o644)

	if _, err := Open(dir, nil); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("Open = %v, want wal.ErrCorrupt", err)
	}
}

func TestDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, nil)
	if _, err := Open(dir, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	d.Close()
	d2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	d2.Close()
}

func TestMemtableFull(t *testing.T) {
	d := mustOpen(t, t.TempDir(), &Options{MaxMemtableBytes: 4096})
	defer d.Close()
	var err error
	for i := 0; i < 1000 && err == nil; i++ {
		err = d.Put([]byte(fmt.Sprint(i)), make([]byte, 100))
	}
	if !errors.Is(err, ErrMemtableFull) {
		t.Fatalf("got %v, want ErrMemtableFull", err)
	}
	// Existing data is still readable.
	expect(t, d, "0", string(make([]byte, 100)))
}

func TestTooLarge(t *testing.T) {
	d := mustOpen(t, t.TempDir(), nil)
	defer d.Close()
	if err := d.Put([]byte("k"), make([]byte, wal.MaxRecordSize)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
}

func TestParseSyncPolicy(t *testing.T) {
	for _, p := range allPolicies {
		got, err := ParseSyncPolicy(p.String())
		if err != nil || got != p {
			t.Fatalf("ParseSyncPolicy(%q) = %v, %v", p.String(), got, err)
		}
	}
	if _, err := ParseSyncPolicy("sometimes"); err == nil {
		t.Fatal("expected error")
	}
	if _, err := Open(t.TempDir(), &Options{Sync: 99}); err == nil {
		t.Fatal("Open should reject an invalid policy")
	}
}

// TestRandomizedReopen interleaves random writes with reopens and checks
// the DB against a map after each reopen.
func TestRandomizedReopen(t *testing.T) {
	dir := t.TempDir()
	r := rand.New(rand.NewPCG(1, 2))
	model := map[string]string{}
	opts := &Options{Sync: SyncPeriodic}
	d := mustOpen(t, dir, opts)
	for round := 0; round < 5; round++ {
		for i := 0; i < 400; i++ {
			k := fmt.Sprintf("k%02d", r.IntN(60))
			if r.IntN(3) == 0 {
				d.Delete([]byte(k))
				delete(model, k)
			} else {
				v := fmt.Sprintf("v%d-%d", round, i)
				d.Put([]byte(k), []byte(v))
				model[k] = v
			}
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		d = mustOpen(t, dir, opts)
		for i := 0; i < 60; i++ {
			k := fmt.Sprintf("k%02d", i)
			expect(t, d, k, model[k])
		}
	}
	d.Close()
}

func TestConcurrentGroupCommitPersists(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, &Options{Sync: SyncGroup})
	const writers, each = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				k := []byte(fmt.Sprintf("w%d-%d", w, i))
				if err := d.Put(k, k); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	d.Close()

	d = mustOpen(t, dir, nil)
	defer d.Close()
	for w := 0; w < writers; w++ {
		for i := 0; i < each; i++ {
			k := fmt.Sprintf("w%d-%d", w, i)
			expect(t, d, k, k)
		}
	}
}
