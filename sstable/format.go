// Package sstable implements the immutable, sorted on-disk table format.
//
// # File layout
//
//	+---------------------------+
//	| data block 0              |  entries in internal-key order
//	| data block 1              |
//	| ...                       |
//	+---------------------------+
//	| filter block              |  bloom filter over user keys
//	+---------------------------+
//	| index block               |  one entry per data block
//	+---------------------------+
//	| footer (48 bytes)         |
//	+---------------------------+
//
// Every block (data, filter, index) is followed by a 4-byte CRC-32C of its
// contents, verified on read.
//
// Data block entry:
//
//	key len (uvarint) | value len (uvarint) | internal key | value
//
// The internal key is the user key plus the 8-byte (seq<<8 | kind) trailer.
//
// Index entry, one per data block (a sparse index):
//
//	key len (uvarint) | last internal key in the block |
//	block offset (uvarint) | block length (uvarint)
//
// Because the index stores each block's last key, a lookup binary-searches
// for the first block whose last key is >= the search key; that is the only
// block that can contain it. One index entry per ~4 KiB block keeps the
// index small enough to hold in memory for every open table.
//
// Footer:
//
//	filter offset | filter len | index offset | index len (4 x uint64 LE) |
//	entry count (uint64 LE) | magic (uint64 LE)
package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	footerLen = 48
	magic     = 0x4c534d5353543031 // "LSMSST01"
	crcLen    = 4

	// DefaultBlockSize is the target uncompressed size of a data block.
	DefaultBlockSize = 4 << 10
)

// ErrCorrupt is returned when a table fails a checksum or structural check.
var ErrCorrupt = errors.New("sstable: corrupt table")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func appendCRC(dst, block []byte) []byte {
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(block, castagnoli))
}

func checkCRC(block []byte, sum []byte) bool {
	return binary.LittleEndian.Uint32(sum) == crc32.Checksum(block, castagnoli)
}

type blockHandle struct {
	offset, length uint64 // length excludes the trailing CRC
}
