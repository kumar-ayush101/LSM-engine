package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func payloads(n int) [][]byte {
	var out [][]byte
	for i := 0; i < n; i++ {
		// Varying sizes, including empty.
		out = append(out, bytes.Repeat([]byte{byte('a' + i%26)}, i*7%50))
	}
	return out
}

func encodeAll(ps [][]byte) ([]byte, []int64) {
	var buf []byte
	var ends []int64
	for _, p := range ps {
		buf = appendRecord(buf, p)
		ends = append(ends, int64(len(buf)))
	}
	return buf, ends
}

func replayBytes(t *testing.T, b []byte) ([][]byte, int64, error) {
	t.Helper()
	var got [][]byte
	_, valid, err := replay(bytes.NewReader(b), int64(len(b)), func(p []byte) error {
		got = append(got, append([]byte(nil), p...))
		return nil
	})
	return got, valid, err
}

func equalPrefix(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range got {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestWriteRecoverRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	want := payloads(100)
	for _, p := range want {
		if _, err := w.Append(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got [][]byte
	res, err := Recover(path, func(p []byte) error {
		got = append(got, append([]byte(nil), p...))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	equalPrefix(t, got, want)
	if res.Records != 100 || res.TruncatedBytes != 0 {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestRecoverMissingFileIsEmpty(t *testing.T) {
	res, err := Recover(filepath.Join(t.TempDir(), "none.wal"), func([]byte) error {
		t.Fatal("callback should not run")
		return nil
	})
	if err != nil || res.Records != 0 {
		t.Fatalf("got %+v, %v", res, err)
	}
}

// TestTornTailAtEveryOffset simulates a crash after every possible number
// of bytes reached disk: recovery must return exactly the complete records.
func TestTornTailAtEveryOffset(t *testing.T) {
	ps := payloads(12)
	full, ends := encodeAll(ps)
	for cut := 0; cut <= len(full); cut++ {
		complete := 0
		for complete < len(ends) && ends[complete] <= int64(cut) {
			complete++
		}
		got, valid, err := replayBytes(t, full[:cut])
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		equalPrefix(t, got, ps[:complete])
		wantValid := int64(0)
		if complete > 0 {
			wantValid = ends[complete-1]
		}
		if valid != wantValid {
			t.Fatalf("cut %d: valid = %d, want %d", cut, valid, wantValid)
		}
	}
}

func TestDamagedLastRecordIsTruncated(t *testing.T) {
	ps := payloads(5)
	full, ends := encodeAll(ps)
	full[len(full)-1] ^= 0xff
	got, valid, err := replayBytes(t, full)
	if err != nil {
		t.Fatal(err)
	}
	equalPrefix(t, got, ps[:4])
	if valid != ends[3] {
		t.Fatalf("valid = %d, want %d", valid, ends[3])
	}
}

func TestZeroFilledTailIsTruncated(t *testing.T) {
	ps := payloads(5)
	full, ends := encodeAll(ps)
	full = append(full, make([]byte, 100<<10)...)
	got, valid, err := replayBytes(t, full)
	if err != nil {
		t.Fatal(err)
	}
	equalPrefix(t, got, ps)
	if valid != ends[4] {
		t.Fatalf("valid = %d, want %d", valid, ends[4])
	}
}

func TestMidLogCorruptionIsAnError(t *testing.T) {
	ps := payloads(6)
	for _, mutate := range []struct {
		name string
		fn   func(b []byte, ends []int64)
	}{
		{"flipped payload byte", func(b []byte, ends []int64) { b[ends[1]+headerLen] ^= 1 }},
		{"flipped crc byte", func(b []byte, ends []int64) { b[ends[1]] ^= 1 }},
		{"huge length", func(b []byte, ends []int64) { copy(b[ends[1]+4:], []byte{0xff, 0xff, 0xff, 0x7f}) }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			full, ends := encodeAll(ps)
			mutate.fn(full, ends)
			got, valid, err := replayBytes(t, full)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
			equalPrefix(t, got, ps[:2])
			if valid != ends[1] {
				t.Fatalf("valid = %d, want %d", valid, ends[1])
			}
		})
	}
}

func TestRecoverTruncatesFileAndAppendContinues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	full, ends := encodeAll(payloads(3))
	torn := append(append([]byte(nil), full...), appendRecord(nil, []byte("lost"))[:6]...)
	if err := os.WriteFile(path, torn, 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := Recover(path, func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if res.Records != 3 || res.ValidBytes != ends[2] || res.TruncatedBytes != 6 {
		t.Fatalf("unexpected %+v", res)
	}
	if st, _ := os.Stat(path); st.Size() != ends[2] {
		t.Fatalf("file size %d, want %d", st.Size(), ends[2])
	}

	w, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append([]byte("new")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	var last []byte
	res, err = Recover(path, func(p []byte) error { last = append([]byte(nil), p...); return nil })
	if err != nil || res.Records != 4 || string(last) != "new" {
		t.Fatalf("after append: %+v, %v, last=%q", res, err, last)
	}
}

func TestRecoverCallbackErrorStops(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	full, _ := encodeAll(payloads(5))
	os.WriteFile(path, full, 0o644)
	boom := errors.New("boom")
	calls := 0
	_, err := Recover(path, func([]byte) error {
		calls++
		if calls == 2 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) || calls != 2 {
		t.Fatalf("err = %v, calls = %d", err, calls)
	}
}

func TestRecordTooLarge(t *testing.T) {
	w := newWriter(&fakeFile{}, 0)
	if _, err := w.Append(make([]byte, MaxRecordSize+1)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("got %v", err)
	}
	// Not sticky: a too-large record is rejected before touching the file.
	if _, err := w.Append([]byte("ok")); err != nil {
		t.Fatal(err)
	}
}

// fakeFile records writes and lets tests inject failures and slow fsyncs.
type fakeFile struct {
	mu        sync.Mutex
	data      []byte
	writeErr  error
	syncErr   error
	syncDelay time.Duration
	syncs     atomic.Int64
}

func (f *fakeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeErr != nil {
		n := len(p) / 2 // simulate a partial write
		f.data = append(f.data, p[:n]...)
		return n, f.writeErr
	}
	f.data = append(f.data, p...)
	return len(p), nil
}

func (f *fakeFile) Sync() error {
	time.Sleep(f.syncDelay)
	f.syncs.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncErr
}

func (f *fakeFile) Close() error { return nil }

func TestSyncErrorIsSticky(t *testing.T) {
	ff := &fakeFile{}
	w := newWriter(ff, 0)
	off1, _ := w.Append([]byte("a"))
	if err := w.SyncTo(off1); err != nil {
		t.Fatal(err)
	}

	ff.syncErr = errors.New("EIO")
	off2, _ := w.Append([]byte("b"))
	if err := w.SyncTo(off2); !errors.Is(err, ErrFailed) {
		t.Fatalf("SyncTo: %v, want ErrFailed", err)
	}

	// Even if the disk "recovers", the writer stays failed.
	ff.syncErr = nil
	if _, err := w.Append([]byte("c")); !errors.Is(err, ErrFailed) {
		t.Fatalf("Append after failure: %v", err)
	}
	if err := w.SyncTo(off2); !errors.Is(err, ErrFailed) {
		t.Fatalf("SyncTo after failure: %v", err)
	}
	// Data that was durable before the failure is still reported as durable.
	if err := w.SyncTo(off1); err != nil {
		t.Fatalf("SyncTo(already synced) = %v", err)
	}
	if err := w.Close(); !errors.Is(err, ErrFailed) {
		t.Fatalf("Close = %v", err)
	}
}

func TestWriteErrorIsSticky(t *testing.T) {
	ff := &fakeFile{writeErr: errors.New("ENOSPC")}
	w := newWriter(ff, 0)
	if _, err := w.Append([]byte("hello")); !errors.Is(err, ErrFailed) {
		t.Fatalf("got %v", err)
	}
	ff.writeErr = nil
	if _, err := w.Append([]byte("again")); !errors.Is(err, ErrFailed) {
		t.Fatalf("second append: %v", err)
	}
}

func TestClosedWriter(t *testing.T) {
	w := newWriter(&fakeFile{}, 0)
	off, _ := w.Append([]byte("a"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := w.Append([]byte("b")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after close: %v", err)
	}
	// Close synced everything, so a waiter for an earlier offset succeeds.
	if err := w.SyncTo(off); err != nil {
		t.Fatalf("SyncTo(off) after close: %v", err)
	}
}

// TestGroupCommitCoalescesSyncs: with slow fsyncs and many concurrent
// writers, one fsync must cover several records. Run with -race.
func TestGroupCommitCoalescesSyncs(t *testing.T) {
	ff := &fakeFile{syncDelay: 2 * time.Millisecond}
	w := newWriter(ff, 0)
	const writers, each = 8, 25

	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				off, err := w.Append([]byte(fmt.Sprintf("g%d-%d", g, i)))
				if err != nil {
					t.Error(err)
					return
				}
				if err := w.SyncTo(off); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	calls := int64(writers * each)
	syncs := ff.syncs.Load()
	t.Logf("%d SyncTo calls, %d fsyncs", calls, syncs)
	if syncs >= calls {
		t.Fatalf("no coalescing: %d fsyncs for %d SyncTo calls", syncs, calls)
	}
	got, _, err := replayBytes(t, ff.data)
	if err != nil || len(got) != writers*each {
		t.Fatalf("replay: %d records, %v", len(got), err)
	}
}
