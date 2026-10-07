package memtable

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

func mustGet(t *testing.T, m *Memtable, key string, snap base.SeqNum, wantRes LookupResult, wantVal string) {
	t.Helper()
	v, res := m.Get([]byte(key), snap)
	if res != wantRes {
		t.Fatalf("Get(%q, %d): result %v, want %v", key, snap, res, wantRes)
	}
	if res == Found && string(v) != wantVal {
		t.Fatalf("Get(%q, %d) = %q, want %q", key, snap, v, wantVal)
	}
}

func TestPutGet(t *testing.T) {
	m := New()
	if !m.Empty() {
		t.Fatal("new memtable should be empty")
	}
	if err := m.Put([]byte("a"), []byte("1"), 1); err != nil {
		t.Fatal(err)
	}
	if err := m.Put([]byte("b"), []byte("2"), 2); err != nil {
		t.Fatal(err)
	}
	mustGet(t, m, "a", base.MaxSeqNum, Found, "1")
	mustGet(t, m, "b", base.MaxSeqNum, Found, "2")
	mustGet(t, m, "c", base.MaxSeqNum, NotFound, "")
	mustGet(t, m, "", base.MaxSeqNum, NotFound, "")
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
}

func TestNewestVersionWins(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v1"), 1)
	m.Put([]byte("k"), []byte("v2"), 5)
	m.Put([]byte("k"), []byte("v3"), 9)
	mustGet(t, m, "k", base.MaxSeqNum, Found, "v3")
	// All versions are kept; the memtable never overwrites in place.
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3", m.Len())
	}
}

func TestSnapshotReads(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v1"), 10)
	m.Put([]byte("k"), []byte("v2"), 20)
	m.Delete([]byte("k"), 30)
	m.Put([]byte("k"), []byte("v4"), 40)

	mustGet(t, m, "k", 9, NotFound, "")
	mustGet(t, m, "k", 10, Found, "v1")
	mustGet(t, m, "k", 19, Found, "v1")
	mustGet(t, m, "k", 20, Found, "v2")
	mustGet(t, m, "k", 30, Deleted, "")
	mustGet(t, m, "k", 39, Deleted, "")
	mustGet(t, m, "k", 40, Found, "v4")
}

func TestTombstoneShadowsAndIsDistinctFromNotFound(t *testing.T) {
	m := New()
	m.Delete([]byte("gone"), 1) // tombstone with no older version in this memtable
	mustGet(t, m, "gone", base.MaxSeqNum, Deleted, "")
	mustGet(t, m, "never", base.MaxSeqNum, NotFound, "")

	m.Put([]byte("x"), []byte("v"), 2)
	m.Delete([]byte("x"), 3)
	mustGet(t, m, "x", base.MaxSeqNum, Deleted, "")
	mustGet(t, m, "x", 2, Found, "v")
}

func TestPrefixKeysAreDistinct(t *testing.T) {
	m := New()
	m.Put([]byte("ab"), []byte("long"), 1)
	mustGet(t, m, "a", base.MaxSeqNum, NotFound, "")
	m.Put([]byte("a"), []byte("short"), 2)
	mustGet(t, m, "a", base.MaxSeqNum, Found, "short")
	mustGet(t, m, "ab", base.MaxSeqNum, Found, "long")
}

func TestEmptyValueIsFoundNotDeleted(t *testing.T) {
	m := New()
	m.Put([]byte("k"), nil, 1)
	v, res := m.Get([]byte("k"), base.MaxSeqNum)
	if res != Found || v == nil || len(v) != 0 {
		t.Fatalf("got (%v, %v), want (empty non-nil, Found)", v, res)
	}
}

func TestDuplicateSeqRejected(t *testing.T) {
	m := New()
	if err := m.Put([]byte("k"), []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	if err := m.Put([]byte("k"), []byte("other"), 1); err != ErrDuplicate {
		t.Fatalf("got %v, want ErrDuplicate", err)
	}
	mustGet(t, m, "k", base.MaxSeqNum, Found, "v")
}

func TestSeqOutOfRangeRejected(t *testing.T) {
	m := New()
	if err := m.Put([]byte("k"), nil, base.MaxSeqNum+1); err == nil {
		t.Fatal("expected error for seq > MaxSeqNum")
	}
}

func TestInputsAndOutputsAreCopied(t *testing.T) {
	m := New()
	key, val := []byte("key"), []byte("val")
	m.Put(key, val, 1)
	key[0], val[0] = 'X', 'X' // caller reuses its buffers
	mustGet(t, m, "key", base.MaxSeqNum, Found, "val")

	got, _ := m.Get([]byte("key"), base.MaxSeqNum)
	got[0] = 'Y' // caller mutates the returned value
	mustGet(t, m, "key", base.MaxSeqNum, Found, "val")
}

func TestApproximateSizeGrows(t *testing.T) {
	m := New()
	if m.ApproximateSize() != 0 {
		t.Fatal("empty memtable should have size 0")
	}
	m.Put([]byte("k"), make([]byte, 1000), 1)
	s1 := m.ApproximateSize()
	if s1 < 1000 {
		t.Fatalf("size %d should include the 1000-byte value", s1)
	}
	m.Delete([]byte("k"), 2)
	if m.ApproximateSize() <= s1 {
		t.Fatal("tombstones must count toward size")
	}
}

// TestRandomizedAgainstModel compares the memtable to a simple reference
// model over many random operations, checking reads at random snapshots.
func TestRandomizedAgainstModel(t *testing.T) {
	type version struct {
		seq     base.SeqNum
		deleted bool
		value   string
	}
	r := rand.New(rand.NewPCG(7, 7))
	m := New()
	model := map[string][]version{} // versions in increasing seq order

	const ops = 20000
	for seq := base.SeqNum(1); seq <= ops; seq++ {
		k := fmt.Sprintf("k%03d", r.IntN(200))
		if r.IntN(4) == 0 {
			m.Delete([]byte(k), seq)
			model[k] = append(model[k], version{seq: seq, deleted: true})
		} else {
			v := fmt.Sprintf("v%d", seq)
			m.Put([]byte(k), []byte(v), seq)
			model[k] = append(model[k], version{seq: seq, value: v})
		}
	}

	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("k%03d", r.IntN(220)) // includes keys never written
		snap := base.SeqNum(r.IntN(ops + 1))
		wantRes, wantVal := NotFound, ""
		vs := model[k]
		for j := len(vs) - 1; j >= 0; j-- {
			if vs[j].seq <= snap {
				if vs[j].deleted {
					wantRes = Deleted
				} else {
					wantRes, wantVal = Found, vs[j].value
				}
				break
			}
		}
		mustGet(t, m, k, snap, wantRes, wantVal)
	}
}

// TestConcurrentReadersAndWriters is primarily a data-race check; run with -race.
func TestConcurrentReadersAndWriters(t *testing.T) {
	m := New()
	const writers, perWriter, readers = 4, 2000, 4

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// Unique seq per write across all writers.
				seq := base.SeqNum(w*perWriter + i + 1)
				key := []byte(fmt.Sprintf("w%d-%d", w, i))
				if err := m.Put(key, key, seq); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	stop := make(chan struct{})
	var rwg sync.WaitGroup
	for r := 0; r < readers; r++ {
		rwg.Add(1)
		go func(r int) {
			defer rwg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := []byte(fmt.Sprintf("w%d-%d", r%writers, i%perWriter))
				if v, res := m.Get(key, base.MaxSeqNum); res == Found && string(v) != string(key) {
					t.Errorf("Get(%s) = %s", key, v)
					return
				}
				_ = m.ApproximateSize()
			}
		}(r)
	}
	wg.Wait()
	close(stop)
	rwg.Wait()

	if m.Len() != writers*perWriter {
		t.Fatalf("Len = %d, want %d", m.Len(), writers*perWriter)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			key := fmt.Sprintf("w%d-%d", w, i)
			mustGet(t, m, key, base.MaxSeqNum, Found, key)
		}
	}
	checkInvariants(t, m.list)
}
