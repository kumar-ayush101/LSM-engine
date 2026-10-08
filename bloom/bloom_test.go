package bloom

import (
	"fmt"
	"math"
	"testing"
)

func TestSizingFormulas(t *testing.T) {
	// p = 1%: m/n = -ln(0.01)/ln2^2 ~= 9.585, k = round(9.585 * ln2) = 7.
	if bpk := BitsPerKey(0.01); math.Abs(bpk-9.585) > 0.01 {
		t.Fatalf("BitsPerKey(0.01) = %.3f", bpk)
	}
	if k := NumHashes(BitsPerKey(0.01)); k != 7 {
		t.Fatalf("NumHashes = %d, want 7", k)
	}
	if k := NumHashes(BitsPerKey(0.001)); k != 10 {
		t.Fatalf("NumHashes(0.1%%) = %d, want 10", k)
	}
}

func TestNoFalseNegatives(t *testing.T) {
	b := NewBuilder(0.01)
	for i := 0; i < 10000; i++ {
		b.Add([]byte(fmt.Sprintf("key-%d", i)))
	}
	f := b.Finish()
	for i := 0; i < 10000; i++ {
		if !f.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			t.Fatalf("false negative for key-%d", i)
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	for _, p := range []float64{0.01, 0.05} {
		b := NewBuilder(p)
		const n = 20000
		for i := 0; i < n; i++ {
			b.Add([]byte(fmt.Sprintf("present-%d", i)))
		}
		f := b.Finish()
		fp := 0
		const probes = 100000
		for i := 0; i < probes; i++ {
			if f.MayContain([]byte(fmt.Sprintf("absent-%d", i))) {
				fp++
			}
		}
		rate := float64(fp) / probes
		t.Logf("target %.2f%%: measured %.3f%% (%d bytes for %d keys)", p*100, rate*100, len(f), n)
		if rate > p*1.5 {
			t.Fatalf("false-positive rate %.4f exceeds 1.5x target %.4f", rate, p)
		}
	}
}

func TestConsecutiveDuplicatesAreFree(t *testing.T) {
	b := NewBuilder(0.01)
	for i := 0; i < 5; i++ {
		b.Add([]byte("same"))
	}
	b.Add([]byte("other"))
	if b.Len() != 2 {
		t.Fatalf("Len = %d, want 2", b.Len())
	}
}

func TestEmptyAndMalformedFilters(t *testing.T) {
	f := NewBuilder(0.01).Finish() // no keys
	if f.MayContain([]byte("x")) {
		t.Fatal("empty filter should reject every key")
	}
	for _, bad := range []Filter{nil, {}, {0xff}, {0, 0, 0, 99}} {
		if !bad.MayContain([]byte("x")) {
			t.Fatalf("malformed filter %v must answer maybe", bad)
		}
	}
}
