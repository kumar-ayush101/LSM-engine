// Package base defines the core key types shared by every layer of the engine
// (memtable, WAL, SSTables, compaction).
//
// Every write is stored as an internal key: the user key plus a sequence
// number and a kind (set or delete). Multiple versions of the same user key
// can coexist; ordering them newest-first lets readers pick the latest version
// visible at a given snapshot, and lets compaction decide which versions are
// obsolete.
package base

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// SeqNum is a monotonically increasing number assigned to every write.
// A higher sequence number means a newer write.
type SeqNum uint64

// MaxSeqNum is the largest valid sequence number. The top 8 bits of the
// 64-bit trailer are reserved for the Kind, so sequence numbers use 56 bits.
const MaxSeqNum SeqNum = 1<<56 - 1

// Kind says whether an internal key carries a value or is a tombstone.
type Kind uint8

const (
	// KindDelete marks a tombstone: the key was deleted at this sequence number.
	KindDelete Kind = 0
	// KindSet marks a regular key/value write.
	KindSet Kind = 1
)

// KindMax is the largest Kind. Used to build seek keys that sort before every
// real entry with the same user key and sequence number.
const KindMax = KindSet

func (k Kind) String() string {
	switch k {
	case KindDelete:
		return "DEL"
	case KindSet:
		return "SET"
	default:
		return fmt.Sprintf("KIND(%d)", uint8(k))
	}
}

// TrailerLen is the encoded size of the (seq, kind) trailer.
const TrailerLen = 8

// InternalKey is a user key tagged with the sequence number and kind of the
// write that produced it.
type InternalKey struct {
	UserKey []byte
	Seq     SeqNum
	Kind    Kind
}

// MakeInternalKey builds an InternalKey. It does not copy userKey.
func MakeInternalKey(userKey []byte, seq SeqNum, kind Kind) InternalKey {
	return InternalKey{UserKey: userKey, Seq: seq, Kind: kind}
}

// MakeSearchKey returns the key that sorts first among all versions of
// userKey that are visible at snapshot (Seq <= snapshot). Seeking to it lands
// on the newest visible version.
func MakeSearchKey(userKey []byte, snapshot SeqNum) InternalKey {
	return InternalKey{UserKey: userKey, Seq: snapshot, Kind: KindMax}
}

// Trailer packs seq and kind into one uint64: seq<<8 | kind.
func (k InternalKey) Trailer() uint64 {
	return uint64(k.Seq)<<8 | uint64(k.Kind)
}

// Size is the encoded size of the key in bytes.
func (k InternalKey) Size() int {
	return len(k.UserKey) + TrailerLen
}

// Encode appends the encoded key (user key followed by an 8-byte
// little-endian trailer) to dst and returns the extended slice.
func (k InternalKey) Encode(dst []byte) []byte {
	dst = append(dst, k.UserKey...)
	return binary.LittleEndian.AppendUint64(dst, k.Trailer())
}

// ErrCorruptKey is returned when an encoded internal key is malformed.
var ErrCorruptKey = errors.New("base: corrupt internal key")

// DecodeInternalKey parses a key produced by Encode. The returned UserKey
// aliases buf.
func DecodeInternalKey(buf []byte) (InternalKey, error) {
	if len(buf) < TrailerLen {
		return InternalKey{}, ErrCorruptKey
	}
	n := len(buf) - TrailerLen
	t := binary.LittleEndian.Uint64(buf[n:])
	kind := Kind(t & 0xff)
	if kind > KindMax {
		return InternalKey{}, ErrCorruptKey
	}
	return InternalKey{UserKey: buf[:n:n], Seq: SeqNum(t >> 8), Kind: kind}, nil
}

// Compare orders internal keys by user key ascending, then by sequence
// number descending (newest first), then by kind descending.
//
// Newest-first means a forward scan meets the latest version of a key before
// any older one, so a point lookup can stop at the first match.
func Compare(a, b InternalKey) int {
	if c := bytes.Compare(a.UserKey, b.UserKey); c != 0 {
		return c
	}
	// Comparing trailers handles seq and kind together, both descending.
	at, bt := a.Trailer(), b.Trailer()
	switch {
	case at > bt:
		return -1
	case at < bt:
		return 1
	default:
		return 0
	}
}

func (k InternalKey) String() string {
	return fmt.Sprintf("%q#%d,%s", k.UserKey, k.Seq, k.Kind)
}
