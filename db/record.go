package db

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

// A WAL record payload for one write:
//
//	+-----------+----------+------------------+-----+----------------+
//	| seq (8 B) | kind (1) | key len (uvarint)| key | value (rest)   |
//	+-----------+----------+------------------+-----+----------------+
//
// The value has no length field: it is whatever follows the key. Tombstones
// have no value bytes.

var errBadRecord = errors.New("db: malformed WAL record")

func encodeRecord(dst []byte, seq base.SeqNum, kind base.Kind, key, value []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, uint64(seq))
	dst = append(dst, byte(kind))
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = append(dst, key...)
	return append(dst, value...)
}

// decodeRecord parses a payload. key and value alias p.
func decodeRecord(p []byte) (seq base.SeqNum, kind base.Kind, key, value []byte, err error) {
	if len(p) < 9 {
		return 0, 0, nil, nil, errBadRecord
	}
	seq = base.SeqNum(binary.LittleEndian.Uint64(p))
	kind = base.Kind(p[8])
	if kind > base.KindMax || seq == 0 || seq > base.MaxSeqNum {
		return 0, 0, nil, nil, errBadRecord
	}
	klen, n := binary.Uvarint(p[9:])
	if n <= 0 || klen > uint64(len(p)-9-n) {
		return 0, 0, nil, nil, errBadRecord
	}
	rest := p[9+n:]
	key, value = rest[:klen], rest[klen:]
	if kind == base.KindDelete && len(value) != 0 {
		return 0, 0, nil, nil, errBadRecord
	}
	return seq, kind, key, value, nil
}

func (e recordError) Error() string {
	return fmt.Sprintf("db: WAL record %d: %v", e.index, e.err)
}

func (e recordError) Unwrap() error { return e.err }

type recordError struct {
	index int
	err   error
}
