// Package wal implements the write-ahead log used for crash recovery of the memtable.
//
// Every write is appended to the log before it is applied to the memtable.
// After a crash, replaying the log rebuilds the memtable, so no acknowledged
// write is lost.
//
// # Record format
//
//	+-----------+-----------+-------------------+
//	| crc (4 B) | len (4 B) | payload (len B)   |
//	+-----------+-----------+-------------------+
//
// Both integers are little-endian. crc is CRC-32C (Castagnoli) over the len
// field and the payload, so a corrupted length is detected too. An all-zero
// header never validates, which matters because some filesystems leave a
// zero-filled tail after a crash.
//
// # Recovery rules
//
// A crash can leave the final record partially written (a "torn write").
// Recovery keeps every valid record and truncates the log after the last one
// when the damage is confined to the tail:
//
//   - the header or payload of the last record is cut off by end of file, or
//   - the last record fails its checksum, or
//   - everything from the bad record to end of file is zero bytes.
//
// A bad record followed by non-zero data cannot be explained by a crash
// during append; it is real corruption, and recovery fails with ErrCorrupt
// instead of silently discarding acknowledged writes.
package wal
