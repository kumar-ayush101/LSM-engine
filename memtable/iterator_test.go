package memtable

import (
	"fmt"
	"sync"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

func collect(it *Iterator) []string {
	var out []string
	for ; it.Valid(); it.Next() {
		out = append(out, fmt.Sprintf("%s#%d,%s=%s", it.Key().UserKey, it.Key().Seq, it.Key().Kind, it.Value()))
	}
	return out
}

func TestIteratorEmpty(t *testing.T) {
	it := New().NewIterator()
	it.SeekToFirst()
	if it.Valid() {
		t.Fatal("iterator over empty memtable should be invalid")
	}
}

func TestIteratorOrderIncludesAllVersionsAndTombstones(t *testing.T) {
	m := New()
	m.Put([]byte("b"), []byte("b1"), 1)
	m.Put([]byte("a"), []byte("a2"), 2)
	m.Delete([]byte("b"), 3)
	m.Put([]byte("c"), []byte("c4"), 4)
	m.Put([]byte("a"), []byte("a5"), 5)

	it := m.NewIterator()
	it.SeekToFirst()
	got := collect(it)
	want := []string{
		"a#5,SET=a5",
		"a#2,SET=a2",
		"b#3,DEL=",
		"b#1,SET=b1",
		"c#4,SET=c4",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got  %v\nwant %v", got, want)
	}
}

func TestIteratorSeek(t *testing.T) {
	m := New()
	m.Put([]byte("a"), []byte("1"), 1)
	m.Put([]byte("c"), []byte("2"), 2)
	m.Put([]byte("c"), []byte("3"), 3)
	m.Put([]byte("e"), []byte("4"), 4)

	it := m.NewIterator()

	it.Seek(base.MakeSearchKey([]byte("b"), base.MaxSeqNum))
	if !it.Valid() || string(it.Key().UserKey) != "c" || it.Key().Seq != 3 {
		t.Fatalf("Seek(b) landed on %v", it.Key())
	}
	// Seek to c at snapshot 2 skips the newer c#3.
	it.Seek(base.MakeSearchKey([]byte("c"), 2))
	if !it.Valid() || it.Key().Seq != 2 {
		t.Fatalf("Seek(c@2) landed on %v", it.Key())
	}
	it.Seek(base.MakeSearchKey([]byte("f"), base.MaxSeqNum))
	if it.Valid() {
		t.Fatalf("Seek past end should be invalid, got %v", it.Key())
	}
}

// TestIteratorConcurrentWithWrites checks that iterating while writers are
// inserting is race-free and always yields a sorted sequence.
func TestIteratorConcurrentWithWrites(t *testing.T) {
	m := New()
	for i := 0; i < 500; i++ {
		m.Put([]byte(fmt.Sprintf("k%04d", i*2)), nil, base.SeqNum(i+1))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			m.Put([]byte(fmt.Sprintf("k%04d", i*2+1)), nil, base.SeqNum(1000+i))
		}
	}()

	for round := 0; round < 20; round++ {
		it := m.NewIterator()
		it.SeekToFirst()
		var prev *base.InternalKey
		n := 0
		for ; it.Valid(); it.Next() {
			k := it.Key()
			if prev != nil && base.Compare(*prev, k) >= 0 {
				t.Fatalf("iterator out of order: %v then %v", *prev, k)
			}
			prev = &k
			n++
		}
		if n < 500 {
			t.Fatalf("iterator saw %d entries, want at least the 500 pre-existing ones", n)
		}
	}
	wg.Wait()
}
