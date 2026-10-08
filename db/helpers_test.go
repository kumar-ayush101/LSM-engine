package db

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

// appendFramed frames payload exactly like wal.Writer (crc | len | payload).
// Used by tests that build WAL files by hand.
func appendFramed(dst, payload []byte) []byte {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(payload)))
	c := crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), hdr[4:])
	c = crc32.Update(c, crc32.MakeTable(crc32.Castagnoli), payload)
	binary.LittleEndian.PutUint32(hdr[:4], c)
	return append(append(dst, hdr[:]...), payload...)
}

func seqOf(i int) base.SeqNum { return base.SeqNum(i) }
