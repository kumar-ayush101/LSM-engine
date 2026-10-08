// Package bloom implements Bloom filters used to skip SSTables that cannot
// contain a key.
//
// # Sizing
//
// For n keys and a target false-positive rate p, the optimal number of bits
// and hash functions are
//
//	m = -n ln p / (ln 2)^2      (about 9.6 bits per key for p = 1%)
//	k = (m / n) ln 2            (about 7 hash functions for p = 1%)
//
// A lookup for a key that was added always returns true (no false
// negatives). A lookup for an absent key returns true with probability
// about p.
//
// # Hashing
//
// Instead of k independent hash functions, the filter derives k bit
// positions from one 64-bit hash using double hashing (Kirsch and
// Mitzenmacher): g_i(x) = h1(x) + i*h2(x) mod m. This has the same
// asymptotic false-positive rate and costs one hash per key.
//
// # Encoding
//
// A filter is the bit array followed by one byte holding k.
package bloom

import (
	"hash/fnv"
	"math"
)

// DefaultFPRate is the false-positive rate used when none is given.
const DefaultFPRate = 0.01

// BitsPerKey returns -ln(p) / (ln 2)^2, the optimal bits per key for
// false-positive rate p.
func BitsPerKey(p float64) float64 {
	return -math.Log(p) / (math.Ln2 * math.Ln2)
}

// NumHashes returns round((m/n) ln 2), clamped to [1, 30].
func NumHashes(bitsPerKey float64) int {
	k := int(math.Round(bitsPerKey * math.Ln2))
	return min(30, max(1, k))
}

// Hash is the 64-bit hash the filter uses: FNV-1a followed by a
// splitmix64-style finalizer, which spreads FNV's weak high bits.
func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Builder accumulates key hashes and produces a Filter.
type Builder struct {
	fpRate float64
	hashes []uint64
}

// NewBuilder returns a builder targeting false-positive rate fpRate
// (DefaultFPRate if fpRate is not in (0, 1)).
func NewBuilder(fpRate float64) *Builder {
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = DefaultFPRate
	}
	return &Builder{fpRate: fpRate}
}

// Add adds key. Adding the same key twice in a row is free: an SSTable adds
// one user key per version, and consecutive versions share the key.
func (b *Builder) Add(key []byte) {
	h := Hash(key)
	if n := len(b.hashes); n > 0 && b.hashes[n-1] == h {
		return
	}
	b.hashes = append(b.hashes, h)
}

// Len returns the number of distinct (consecutive) keys added.
func (b *Builder) Len() int { return len(b.hashes) }

// Finish builds the filter.
func (b *Builder) Finish() Filter {
	n := len(b.hashes)
	bpk := BitsPerKey(b.fpRate)
	k := NumHashes(bpk)
	bits := int(math.Ceil(float64(n) * bpk))
	if bits < 64 {
		bits = 64 // tiny filters have a high false-positive rate otherwise
	}
	nbytes := (bits + 7) / 8
	bits = nbytes * 8

	f := make(Filter, nbytes+1)
	for _, h := range b.hashes {
		h1, h2 := uint32(h), uint32(h>>32)|1
		for i := 0; i < k; i++ {
			pos := (uint64(h1) + uint64(i)*uint64(h2)) % uint64(bits)
			f[pos/8] |= 1 << (pos % 8)
		}
	}
	f[nbytes] = byte(k)
	return f
}

// Filter is an encoded Bloom filter.
type Filter []byte

// MayContain reports whether key may have been added. False means the key
// was definitely not added. A malformed or empty filter returns true, so a
// damaged filter costs performance, never correctness.
func (f Filter) MayContain(key []byte) bool {
	if len(f) < 2 {
		return true
	}
	k := int(f[len(f)-1])
	if k < 1 || k > 30 {
		return true
	}
	bits := uint64(len(f)-1) * 8
	h := Hash(key)
	h1, h2 := uint32(h), uint32(h>>32)|1
	for i := 0; i < k; i++ {
		pos := (uint64(h1) + uint64(i)*uint64(h2)) % bits
		if f[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}
