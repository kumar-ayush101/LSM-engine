package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// HardState is the part of a node's state that MUST survive a crash. Raft's
// correctness depends on a node never forgetting the term it has reached or
// the vote it has already granted; otherwise it could vote twice in one term
// and elect two leaders.
type HardState struct {
	CurrentTerm Term
	VotedFor    string // node ID voted for in CurrentTerm, "" if none
}

// Storage persists a node's hard state and replicated log durably. The core
// consensus module calls it; everything it writes must be on stable storage
// (fsynced) before the call returns, because Raft acknowledges actions (a
// vote, a stored entry) to peers only after they are durable.
//
// The log is 1-indexed. FirstIndex is 1 and LastIndex is 0 for an empty log.
type Storage interface {
	// LoadHardState returns the persisted term and vote (zero value if none).
	LoadHardState() (HardState, error)
	// SaveHardState persists hs durably.
	SaveHardState(hs HardState) error

	// FirstIndex and LastIndex bound the log. For a log with no entries,
	// LastIndex returns 0.
	FirstIndex() Index
	LastIndex() Index
	// Term returns the term of the entry at i, or 0 for i == 0.
	Term(i Index) (Term, bool)
	// Entries returns entries in [lo, hi] inclusive.
	Entries(lo, hi Index) ([]Entry, error)

	// Append stores entries durably. They must be contiguous and start at
	// LastIndex()+1 after any truncation the caller has already performed.
	Append(entries []Entry) error
	// TruncateSuffix removes every entry with index >= from. Used when a
	// follower's log conflicts with the leader's and must be overwritten.
	TruncateSuffix(from Index) error

	// Close releases file handles.
	Close() error
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func checksum(lenField, payload []byte) uint32 {
	c := crc32.Update(0, castagnoli, lenField)
	return crc32.Update(c, castagnoli, payload)
}

const recordHeaderLen = 8

// ErrCorrupt is returned when the log file contains damage that cannot be
// explained by a torn final write.
var ErrCorrupt = errors.New("raft: corrupt log record")

// record kinds in the log file.
const (
	recHardState = 1
	recEntry     = 2
	// recTruncate marks that every entry with index >= From has been removed
	// (a follower pruning a conflicting suffix). Its entry records remain
	// physically in the file, so this barrier tells replay to drop them.
	recTruncate = 3
)

// FileStorage persists Raft state in a single append-only file per node,
// framed exactly like the engine's WAL and MANIFEST: each record is
//
//	crc (4 B) | len (4 B) | payload (len B)
//
// where the CRC-32C covers the length and payload so a corrupt length is
// caught too. The payload's first byte is the record kind.
//
// HardState records and Entry records are interleaved in one log. On open we
// replay the file: the last HardState wins, and entries are applied in
// order, with a later entry at an index replacing an earlier one (that is how
// TruncateSuffix+Append after a conflict is represented on disk). A torn
// final record is truncated, matching the WAL's recovery contract.
type FileStorage struct {
	mu   sync.Mutex
	path string
	f    *os.File
	w    *bufio.Writer

	hard    HardState
	entries []Entry // in-memory copy, entries[0] has Index == first
	first   Index
}

// OpenFileStorage opens or creates the Raft state file at path.
func OpenFileStorage(path string) (*FileStorage, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{path: path, first: 1}
	if err := s.replay(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	s.w = bufio.NewWriter(f)
	return s, nil
}

// replay reads the existing file into memory, truncating a torn tail.
func (s *FileStorage) replay() error {
	f, err := os.OpenFile(s.path, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	br := bufio.NewReaderSize(f, 64<<10)
	var (
		hdr [recordHeaderLen]byte
		buf []byte
		off int64
	)
	for off < size {
		if size-off < recordHeaderLen {
			break // torn header
		}
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			break
		}
		n := int64(binary.LittleEndian.Uint32(hdr[4:]))
		want := binary.LittleEndian.Uint32(hdr[:4])
		end := off + recordHeaderLen + n
		if n < 1 || n > MaxCommandSize+64 || end > size {
			break // torn / implausible payload
		}
		if int64(cap(buf)) < n {
			buf = make([]byte, n)
		}
		buf = buf[:n]
		if _, err := io.ReadFull(br, buf); err != nil {
			break
		}
		if checksum(hdr[4:], buf) != want {
			if end == size {
				break // last record damaged: torn write
			}
			return fmt.Errorf("%w at offset %d", ErrCorrupt, off)
		}
		if err := s.applyRecord(buf); err != nil {
			return err
		}
		off = end
	}
	if off < size {
		if err := f.Truncate(off); err != nil {
			return fmt.Errorf("raft: truncating torn tail: %w", err)
		}
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// applyRecord updates in-memory state from one decoded payload.
func (s *FileStorage) applyRecord(p []byte) error {
	switch p[0] {
	case recHardState:
		if len(p) < 9 {
			return fmt.Errorf("%w: short hardstate", ErrCorrupt)
		}
		s.hard.CurrentTerm = Term(binary.LittleEndian.Uint64(p[1:9]))
		s.hard.VotedFor = string(p[9:])
	case recEntry:
		e, err := decodeEntry(p[1:])
		if err != nil {
			return err
		}
		s.applyEntry(e)
	case recTruncate:
		if len(p) < 9 {
			return fmt.Errorf("%w: short truncate", ErrCorrupt)
		}
		from := Index(binary.LittleEndian.Uint64(p[1:9]))
		s.truncateInMemory(from)
	default:
		return fmt.Errorf("%w: unknown record kind %d", ErrCorrupt, p[0])
	}
	return nil
}

// applyEntry inserts e into the in-memory log, replacing anything at or after
// its index (a conflict overwrite) and dropping gaps defensively.
func (s *FileStorage) applyEntry(e Entry) {
	if len(s.entries) == 0 {
		s.first = e.Index
		s.entries = []Entry{e}
		return
	}
	last := s.entries[len(s.entries)-1].Index
	switch {
	case e.Index == last+1:
		s.entries = append(s.entries, e)
	case e.Index >= s.first && e.Index <= last:
		// Overwrite this index and discard everything after it.
		s.entries = s.entries[:e.Index-s.first]
		s.entries = append(s.entries, e)
	default:
		// Non-contiguous (should not happen with a correct caller); reset.
		s.first = e.Index
		s.entries = []Entry{e}
	}
}

func encodeEntry(e Entry) []byte {
	buf := make([]byte, 1+16+len(e.Command))
	buf[0] = recEntry
	binary.LittleEndian.PutUint64(buf[1:], uint64(e.Term))
	binary.LittleEndian.PutUint64(buf[9:], uint64(e.Index))
	copy(buf[17:], e.Command)
	return buf
}

func decodeEntry(p []byte) (Entry, error) {
	if len(p) < 16 {
		return Entry{}, fmt.Errorf("%w: short entry", ErrCorrupt)
	}
	e := Entry{
		Term:  Term(binary.LittleEndian.Uint64(p[0:8])),
		Index: Index(binary.LittleEndian.Uint64(p[8:16])),
	}
	if cmd := p[16:]; len(cmd) > 0 {
		e.Command = append([]byte(nil), cmd...)
	}
	return e, nil
}

// writeRecord frames and appends payload, flushes and fsyncs.
func (s *FileStorage) writeRecord(payload []byte) error {
	var hdr [recordHeaderLen]byte
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[:4], checksum(hdr[4:], payload))
	if _, err := s.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(payload); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *FileStorage) LoadHardState() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hard, nil
}

func (s *FileStorage) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if hs == s.hard {
		return nil
	}
	payload := make([]byte, 9+len(hs.VotedFor))
	payload[0] = recHardState
	binary.LittleEndian.PutUint64(payload[1:], uint64(hs.CurrentTerm))
	copy(payload[9:], hs.VotedFor)
	if err := s.writeRecord(payload); err != nil {
		return err
	}
	s.hard = hs
	return nil
}

func (s *FileStorage) FirstIndex() Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return 1
	}
	return s.first
}

func (s *FileStorage) LastIndex() Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastLocked()
}

func (s *FileStorage) lastLocked() Index {
	if len(s.entries) == 0 {
		return 0
	}
	return s.entries[len(s.entries)-1].Index
}

func (s *FileStorage) Term(i Index) (Term, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i == 0 {
		return 0, true
	}
	if i < s.first || i > s.lastLocked() {
		return 0, false
	}
	return s.entries[i-s.first].Term, true
}

func (s *FileStorage) Entries(lo, hi Index) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	last := s.lastLocked()
	if lo < s.first || hi > last || lo > hi {
		return nil, fmt.Errorf("raft: entries [%d,%d] out of range [%d,%d]", lo, hi, s.first, last)
	}
	out := make([]Entry, hi-lo+1)
	copy(out, s.entries[lo-s.first:hi-s.first+1])
	return out, nil
}

func (s *FileStorage) Append(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		if err := s.writeRecord(encodeEntry(e)); err != nil {
			return err
		}
		s.applyEntry(e)
	}
	return nil
}

func (s *FileStorage) TruncateSuffix(from Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 || from > s.lastLocked() {
		return nil
	}
	s.truncateInMemory(from)
	// A dedicated truncate record keeps replay honest: without it, replaying
	// the file would re-add the dropped tail (its entry records are still
	// physically present). Overwrites via Append after a conflict are handled
	// by applyEntry, but a bare truncation (follower prunes without an
	// immediate Append) needs this barrier.
	return s.writeTruncateRecord(from)
}

// truncateInMemory drops every in-memory entry with index >= from.
func (s *FileStorage) truncateInMemory(from Index) {
	if len(s.entries) == 0 || from > s.lastLocked() {
		return
	}
	if from <= s.first {
		s.entries = s.entries[:0]
	} else {
		s.entries = s.entries[:from-s.first]
	}
}

func (s *FileStorage) writeTruncateRecord(from Index) error {
	payload := make([]byte, 9)
	payload[0] = recTruncate
	binary.LittleEndian.PutUint64(payload[1:], uint64(from))
	return s.writeRecord(payload)
}

func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.w.Flush()
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	s.f = nil
	return err
}
