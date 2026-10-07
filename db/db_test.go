package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func openTest(t *testing.T) *DB {
	t.Helper()
	d, err := Open(filepath.Join(t.TempDir(), "db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestOpenRejectsEmptyDir(t *testing.T) {
	if _, err := Open("", nil); err == nil {
		t.Fatal("expected error for empty dir")
	}
}

func TestPutGetDelete(t *testing.T) {
	d := openTest(t)

	if _, err := d.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on empty DB: %v, want ErrNotFound", err)
	}
	if err := d.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if v, err := d.Get([]byte("k")); err != nil || string(v) != "v1" {
		t.Fatalf("Get = %q, %v; want v1", v, err)
	}
	if err := d.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if v, _ := d.Get([]byte("k")); string(v) != "v2" {
		t.Fatalf("overwrite: got %q, want v2", v)
	}
	if err := d.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete: %v, want ErrNotFound", err)
	}
	// Re-insert after delete.
	d.Put([]byte("k"), []byte("v3"))
	if v, _ := d.Get([]byte("k")); string(v) != "v3" {
		t.Fatalf("after re-put: got %q, want v3", v)
	}
}

func TestDeleteMissingKeyIsNotAnError(t *testing.T) {
	d := openTest(t)
	if err := d.Delete([]byte("nope")); err != nil {
		t.Fatal(err)
	}
}

func TestSequenceNumbersAreMonotonic(t *testing.T) {
	d := openTest(t)
	for i := 1; i <= 10; i++ {
		d.Put([]byte("k"), []byte("v"))
		if got := d.visibleSeq.Load(); got != uint64(i) {
			t.Fatalf("after %d writes visibleSeq = %d", i, got)
		}
	}
	d.Delete([]byte("k"))
	if got := d.visibleSeq.Load(); got != 11 {
		t.Fatalf("delete should consume a seq: visibleSeq = %d, want 11", got)
	}
}

func TestClose(t *testing.T) {
	d := openTest(t)
	d.Put([]byte("k"), []byte("v"))
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := d.Put([]byte("k"), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close: %v", err)
	}
	if err := d.Delete([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete after Close: %v", err)
	}
	if _, err := d.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close: %v", err)
	}
}

// TestConcurrentWritersAndReaders: run with -race.
func TestConcurrentWritersAndReaders(t *testing.T) {
	d := openTest(t)
	const writers, perWriter = 8, 500

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := []byte(fmt.Sprintf("w%d-%d", w, i))
				if err := d.Put(k, k); err != nil {
					t.Error(err)
					return
				}
				// A reader must see its own completed write.
				if v, err := d.Get(k); err != nil || string(v) != string(k) {
					t.Errorf("read-your-write %s: %q, %v", k, v, err)
					return
				}
				if i%3 == 0 {
					if err := d.Delete(k); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	if got, want := d.visibleSeq.Load(), uint64(writers*perWriter+writers*((perWriter+2)/3)); got != want {
		t.Fatalf("visibleSeq = %d, want %d", got, want)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			k := []byte(fmt.Sprintf("w%d-%d", w, i))
			v, err := d.Get(k)
			if i%3 == 0 {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("%s should be deleted, got %q, %v", k, v, err)
				}
			} else if err != nil || string(v) != string(k) {
				t.Fatalf("%s: got %q, %v", k, v, err)
			}
		}
	}
}
