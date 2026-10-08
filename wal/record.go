package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
)

const (
	headerLen = 8

	// MaxRecordSize bounds a single payload. It also protects recovery from
	// allocating huge buffers when a length field is corrupt.
	MaxRecordSize = 32 << 20
)

var (
	// ErrCorrupt is returned by Recover when a damaged record is followed by
	// more data, i.e. the damage is not a torn final write.
	ErrCorrupt = errors.New("wal: corrupt record")
	// ErrRecordTooLarge is returned by Append for payloads above MaxRecordSize.
	ErrRecordTooLarge = errors.New("wal: record too large")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// checksum covers the 4-byte length field and the payload.
func checksum(lenField, payload []byte) uint32 {
	c := crc32.Update(0, castagnoli, lenField)
	return crc32.Update(c, castagnoli, payload)
}

// appendRecord appends the framed record for payload to dst.
func appendRecord(dst, payload []byte) []byte {
	var hdr [headerLen]byte
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[:4], checksum(hdr[4:], payload))
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

// RecoveryResult describes what Recover found.
type RecoveryResult struct {
	Records        int   // valid records replayed
	ValidBytes     int64 // log size after recovery
	TruncatedBytes int64 // bytes of torn tail removed
}

// Recover replays every valid record in the log at path, calling fn with
// each payload in order, then truncates a torn tail (see package docs) and
// syncs the file. A missing file is an empty log.
//
// The payload slice passed to fn is reused; fn must copy what it keeps.
// If fn returns an error, recovery stops and returns it.
func Recover(path string, fn func(payload []byte) error) (RecoveryResult, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return RecoveryResult{}, nil
	}
	if err != nil {
		return RecoveryResult{}, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return RecoveryResult{}, err
	}
	records, valid, err := replay(f, st.Size(), fn)
	res := RecoveryResult{Records: records, ValidBytes: valid}
	if err != nil {
		return res, err
	}
	if valid < st.Size() {
		if err := f.Truncate(valid); err != nil {
			return res, fmt.Errorf("wal: truncating torn tail: %w", err)
		}
		if err := f.Sync(); err != nil {
			return res, fmt.Errorf("wal: syncing after truncation: %w", err)
		}
		res.TruncatedBytes = st.Size() - valid
	}
	return res, nil
}

// replay reads records from r (size bytes long). It returns the number of
// valid records and the offset just past the last one.
func replay(r io.Reader, size int64, fn func([]byte) error) (int, int64, error) {
	br := bufio.NewReaderSize(r, 64<<10)
	var (
		hdr     [headerLen]byte
		buf     []byte
		off     int64
		records int
	)
	for off < size {
		if size-off < headerLen {
			return records, off, nil // torn header
		}
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return records, off, err
		}
		n := int64(binary.LittleEndian.Uint32(hdr[4:]))
		want := binary.LittleEndian.Uint32(hdr[:4])
		end := off + headerLen + n

		if n <= MaxRecordSize {
			if end > size {
				return records, off, nil // torn payload
			}
			if int64(cap(buf)) < n {
				buf = make([]byte, n)
			}
			buf = buf[:n]
			if _, err := io.ReadFull(br, buf); err != nil {
				return records, off, err
			}
			if checksum(hdr[4:], buf) == want {
				if err := fn(buf); err != nil {
					return records, off, err
				}
				records++
				off = end
				continue
			}
			if end == size {
				return records, off, nil // last record damaged
			}
		}

		// A bad record with data after it. Only a zero-filled tail is
		// consistent with a crash; anything else is corruption.
		if isZero(hdr[:]) {
			zero, err := restIsZero(br)
			if err != nil {
				return records, off, err
			}
			if zero {
				return records, off, nil
			}
		}
		return records, off, fmt.Errorf("%w at offset %d", ErrCorrupt, off)
	}
	return records, off, nil
}

func isZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func restIsZero(r io.Reader) (bool, error) {
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if !isZero(buf[:n]) {
			return false, nil
		}
		if err == io.EOF {
			return true, nil
		}
		if err != nil {
			return false, err
		}
	}
}
