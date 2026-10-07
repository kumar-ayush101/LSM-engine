package base

import (
	"bytes"
	"sort"
	"testing"
)

func TestCompareOrdering(t *testing.T) {
	// Expected order: user key asc, then seq desc, then kind desc.
	want := []InternalKey{
		MakeInternalKey([]byte("a"), 9, KindSet),
		MakeInternalKey([]byte("a"), 9, KindDelete),
		MakeInternalKey([]byte("a"), 3, KindSet),
		MakeInternalKey([]byte("a"), 1, KindDelete),
		MakeInternalKey([]byte("ab"), 100, KindSet),
		MakeInternalKey([]byte("b"), 2, KindSet),
	}
	got := make([]InternalKey, len(want))
	// Reverse to force sorting to do work.
	for i := range want {
		got[len(want)-1-i] = want[i]
	}
	sort.Slice(got, func(i, j int) bool { return Compare(got[i], got[j]) < 0 })
	for i := range want {
		if Compare(got[i], want[i]) != 0 {
			t.Fatalf("position %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

func TestCompareEmptyUserKey(t *testing.T) {
	empty := MakeInternalKey(nil, 5, KindSet)
	a := MakeInternalKey([]byte("a"), 1, KindSet)
	if Compare(empty, a) >= 0 {
		t.Fatalf("empty user key should sort before %q", a.UserKey)
	}
	if Compare(empty, MakeInternalKey([]byte{}, 5, KindSet)) != 0 {
		t.Fatal("nil and empty user keys should compare equal")
	}
}

func TestSearchKeySortsBeforeVisibleVersions(t *testing.T) {
	k := []byte("k")
	search := MakeSearchKey(k, 5)
	// Entries newer than the snapshot sort before the search key...
	if Compare(MakeInternalKey(k, 6, KindDelete), search) >= 0 {
		t.Fatal("seq 6 should sort before search key at snapshot 5")
	}
	// ...and entries visible at the snapshot sort at or after it.
	for _, kind := range []Kind{KindSet, KindDelete} {
		if Compare(search, MakeInternalKey(k, 5, kind)) > 0 {
			t.Fatalf("search key should sort at or before seq 5 %v", kind)
		}
	}
	if Compare(search, MakeInternalKey(k, 4, KindSet)) >= 0 {
		t.Fatal("search key should sort before seq 4")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []InternalKey{
		MakeInternalKey(nil, 0, KindDelete),
		MakeInternalKey([]byte("hello"), 42, KindSet),
		MakeInternalKey([]byte{0, 0xff, 0}, MaxSeqNum, KindSet),
	}
	for _, k := range cases {
		buf := k.Encode(nil)
		if len(buf) != k.Size() {
			t.Fatalf("%v: encoded %d bytes, Size() = %d", k, len(buf), k.Size())
		}
		got, err := DecodeInternalKey(buf)
		if err != nil {
			t.Fatalf("%v: decode: %v", k, err)
		}
		if !bytes.Equal(got.UserKey, k.UserKey) || got.Seq != k.Seq || got.Kind != k.Kind {
			t.Fatalf("round trip: got %v, want %v", got, k)
		}
	}
}

func TestDecodeRejectsCorruptKeys(t *testing.T) {
	if _, err := DecodeInternalKey([]byte{1, 2, 3}); err != ErrCorruptKey {
		t.Fatalf("short buffer: got %v, want ErrCorruptKey", err)
	}
	bad := MakeInternalKey([]byte("x"), 1, KindSet).Encode(nil)
	bad[len(bad)-8] = 0x7f // invalid kind byte (low byte of little-endian trailer)
	if _, err := DecodeInternalKey(bad); err != ErrCorruptKey {
		t.Fatalf("bad kind: got %v, want ErrCorruptKey", err)
	}
}

func TestDecodedUserKeyCannotGrowIntoTrailer(t *testing.T) {
	buf := MakeInternalKey([]byte("ab"), 7, KindSet).Encode(nil)
	k, err := DecodeInternalKey(buf)
	if err != nil {
		t.Fatal(err)
	}
	// Appending to the decoded user key must not overwrite the trailer bytes.
	_ = append(k.UserKey, 'Z')
	again, err := DecodeInternalKey(buf)
	if err != nil || again.Seq != 7 || again.Kind != KindSet {
		t.Fatalf("trailer was clobbered: %v, %v", again, err)
	}
}
