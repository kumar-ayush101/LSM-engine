package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

func fm(num uint64, minSeq, maxSeq base.SeqNum) FileMeta {
	return FileMeta{
		Num: num, Size: int64(num * 100), Entries: num * 10, MinSeq: minSeq, MaxSeq: maxSeq,
		Smallest: base.MakeInternalKey([]byte("a"), maxSeq, base.KindSet).Encode(nil),
		Largest:  base.MakeInternalKey([]byte("z"), minSeq, base.KindSet).Encode(nil),
	}
}

func TestEditRoundTrip(t *testing.T) {
	e := Edit{
		SetLogNumber: true, LogNumber: 7,
		SetNextFile: true, NextFile: 12,
		SetLastSeq: true, LastSeq: 999,
		Added:   []FileMeta{fm(3, 1, 10), fm(4, 11, 20)},
		Deleted: []uint64{1, 2},
	}
	got, err := decodeEdit(e.encode())
	if err != nil || !reflect.DeepEqual(got, e) {
		t.Fatalf("round trip: %+v, %v", got, err)
	}
	if _, err := decodeEdit([]byte{99}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unknown tag: %v", err)
	}
	if _, err := decodeEdit(e.encode()[:5]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("truncated: %v", err)
	}
}

func TestPersistAndReplay(t *testing.T) {
	dir := t.TempDir()
	m, st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Files) != 0 || st.NextFile != 1 {
		t.Fatalf("fresh state %+v", st)
	}
	steps := []Edit{
		{Added: []FileMeta{fm(1, 1, 10)}, SetLogNumber: true, LogNumber: 2, SetLastSeq: true, LastSeq: 10},
		{Added: []FileMeta{fm(3, 11, 20)}, SetLogNumber: true, LogNumber: 4, SetLastSeq: true, LastSeq: 20},
		// compaction: 1 and 3 merged into 5
		{Added: []FileMeta{fm(5, 1, 20)}, Deleted: []uint64{1, 3}, SetNextFile: true, NextFile: 6},
	}
	for _, e := range steps {
		if err := m.Apply(e); err != nil {
			t.Fatal(err)
		}
	}
	want := m.State()
	m.Close()

	for round := 0; round < 2; round++ { // reopen twice: replay, then replay the snapshot
		m2, got, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d: got %+v\nwant %+v", round, got, want)
		}
		if len(got.Files) != 1 || got.Files[5].MinSeq != 1 || got.LogNumber != 4 || got.NextFile != 6 || got.LastSeq != 20 {
			t.Fatalf("unexpected state %+v", got)
		}
		m2.Close()
	}
}

func TestDeleteUnknownRejected(t *testing.T) {
	m, _, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Apply(Edit{Deleted: []uint64{42}}); err == nil {
		t.Fatal("deleting an unknown table must fail")
	}
	if len(m.State().Files) != 0 {
		t.Fatal("failed edit changed state")
	}
}

func TestTornEditIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	m, _, _ := Open(dir)
	m.Apply(Edit{Added: []FileMeta{fm(1, 1, 5)}})
	m.Close()
	// Simulate a crash in the middle of appending the next edit.
	f, _ := os.OpenFile(filepath.Join(dir, FileName), os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x40, 0, 0, 0, 4})
	f.Close()
	m, st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if len(st.Files) != 1 {
		t.Fatalf("want the one committed table, got %+v", st.Files)
	}
}

func TestNewestFirstOrdering(t *testing.T) {
	st := State{}
	st.Apply(Edit{Added: []FileMeta{fm(1, 1, 10), fm(2, 21, 30), fm(3, 11, 20)}})
	var order []uint64
	for _, f := range st.NewestFirst() {
		order = append(order, f.Num)
	}
	if !reflect.DeepEqual(order, []uint64{2, 3, 1}) {
		t.Fatalf("order %v", order)
	}
}
