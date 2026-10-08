// Package manifest records which SSTables are live, so recovery after a
// flush or compaction knows exactly which files make up the database.
//
// Without a manifest, recovery would have to guess from the directory
// listing. That is wrong after a crash mid-compaction: the inputs and the
// output both exist on disk, and reading both would resurrect deleted keys
// or double-count data. With a manifest, a table is live if and only if
// the manifest says so; anything else in the directory is garbage.
//
// # Format
//
// MANIFEST is a log of version edits, framed exactly like the WAL (CRC-32C,
// length, payload), so a torn final edit is detected and discarded. An
// edit is acknowledged only after it is fsynced; a flush or compaction
// "commits" at that moment.
//
// Each edit is a sequence of tagged fields:
//
//	1 LogNumber   uvarint     WALs numbered below this are fully flushed
//	2 NextFile    uvarint     next file number to allocate
//	3 LastSeq     uvarint     highest sequence number stored in a table
//	4 AddFile     num, size, entries, minSeq, maxSeq (uvarints),
//	              smallest, largest (uvarint length + encoded internal key)
//	5 DeleteFile  num
//
// On Open the log is replayed and then rewritten as a single snapshot edit
// (written to MANIFEST.tmp, fsynced, renamed over MANIFEST), so the file
// never grows without bound.
package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/kumar-ayush101/LSM-engine/internal/base"
	"github.com/kumar-ayush101/LSM-engine/sstable"
	"github.com/kumar-ayush101/LSM-engine/wal"
)

// FileName is the manifest's name inside the database directory.
const FileName = "MANIFEST"

// ErrCorrupt is returned for an edit that does not decode.
var ErrCorrupt = errors.New("manifest: corrupt edit")

// FileMeta describes one live table.
type FileMeta struct {
	Num      uint64
	Size     int64
	Entries  uint64
	MinSeq   base.SeqNum
	MaxSeq   base.SeqNum
	Smallest []byte // encoded internal key
	Largest  []byte // encoded internal key
}

// Edit is one atomic change to the set of live tables.
type Edit struct {
	SetLogNumber bool
	LogNumber    uint64
	SetNextFile  bool
	NextFile     uint64
	SetLastSeq   bool
	LastSeq      base.SeqNum
	Added        []FileMeta
	Deleted      []uint64
}

// State is the result of applying every edit in order.
type State struct {
	LogNumber uint64
	NextFile  uint64
	LastSeq   base.SeqNum
	Files     map[uint64]FileMeta
}

// NewestFirst returns the live tables ordered by MaxSeq descending, which
// is newest-first because tables always cover disjoint sequence ranges.
func (s *State) NewestFirst() []FileMeta {
	out := make([]FileMeta, 0, len(s.Files))
	for _, f := range s.Files {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MaxSeq > out[j].MaxSeq })
	return out
}

// Apply applies e to s.
func (s *State) Apply(e Edit) error {
	if s.Files == nil {
		s.Files = map[uint64]FileMeta{}
	}
	for _, n := range e.Deleted {
		if _, ok := s.Files[n]; !ok {
			return fmt.Errorf("manifest: delete of unknown table %d", n)
		}
		delete(s.Files, n)
	}
	for _, f := range e.Added {
		s.Files[f.Num] = f
		s.NextFile = max(s.NextFile, f.Num+1)
	}
	if e.SetLogNumber {
		s.LogNumber = e.LogNumber
	}
	if e.SetNextFile {
		s.NextFile = max(s.NextFile, e.NextFile)
	}
	if e.SetLastSeq {
		s.LastSeq = max(s.LastSeq, e.LastSeq)
	}
	return nil
}

const (
	tagLogNumber = 1
	tagNextFile  = 2
	tagLastSeq   = 3
	tagAddFile   = 4
	tagDelete    = 5
)

func (e Edit) encode() []byte {
	var b []byte
	u := func(v uint64) { b = binary.AppendUvarint(b, v) }
	bs := func(p []byte) { u(uint64(len(p))); b = append(b, p...) }
	if e.SetLogNumber {
		u(tagLogNumber)
		u(e.LogNumber)
	}
	if e.SetNextFile {
		u(tagNextFile)
		u(e.NextFile)
	}
	if e.SetLastSeq {
		u(tagLastSeq)
		u(uint64(e.LastSeq))
	}
	for _, f := range e.Added {
		u(tagAddFile)
		u(f.Num)
		u(uint64(f.Size))
		u(f.Entries)
		u(uint64(f.MinSeq))
		u(uint64(f.MaxSeq))
		bs(f.Smallest)
		bs(f.Largest)
	}
	for _, n := range e.Deleted {
		u(tagDelete)
		u(n)
	}
	return b
}

func decodeEdit(p []byte) (Edit, error) {
	var e Edit
	u := func() (uint64, bool) {
		v, n := binary.Uvarint(p)
		if n <= 0 {
			return 0, false
		}
		p = p[n:]
		return v, true
	}
	bs := func() ([]byte, bool) {
		n, ok := u()
		if !ok || n > uint64(len(p)) {
			return nil, false
		}
		out := append([]byte(nil), p[:n]...)
		p = p[n:]
		return out, true
	}
	for len(p) > 0 {
		tag, ok := u()
		if !ok {
			return e, ErrCorrupt
		}
		switch tag {
		case tagLogNumber:
			e.SetLogNumber = true
			e.LogNumber, ok = u()
		case tagNextFile:
			e.SetNextFile = true
			e.NextFile, ok = u()
		case tagLastSeq:
			var v uint64
			v, ok = u()
			e.SetLastSeq, e.LastSeq = true, base.SeqNum(v)
		case tagAddFile:
			var f FileMeta
			var size, minS, maxS uint64
			vals := []*uint64{&f.Num, &size, &f.Entries, &minS, &maxS}
			for _, ptr := range vals {
				if *ptr, ok = u(); !ok {
					return e, ErrCorrupt
				}
			}
			f.Size, f.MinSeq, f.MaxSeq = int64(size), base.SeqNum(minS), base.SeqNum(maxS)
			if f.Smallest, ok = bs(); !ok {
				return e, ErrCorrupt
			}
			f.Largest, ok = bs()
			e.Added = append(e.Added, f)
		case tagDelete:
			var n uint64
			n, ok = u()
			e.Deleted = append(e.Deleted, n)
		default:
			return e, ErrCorrupt
		}
		if !ok {
			return e, ErrCorrupt
		}
	}
	return e, nil
}

// Manifest is an open manifest log. It is not safe for concurrent use; the
// DB serializes edits.
type Manifest struct {
	dir   string
	w     *wal.Writer
	state State
}

// Open replays dir/MANIFEST (a missing file is an empty state), rewrites it
// as a single snapshot, and opens it for appending.
func Open(dir string) (*Manifest, State, error) {
	path := filepath.Join(dir, FileName)
	st := State{Files: map[uint64]FileMeta{}, NextFile: 1}
	_, err := wal.Recover(path, func(p []byte) error {
		e, err := decodeEdit(p)
		if err != nil {
			return err
		}
		return st.Apply(e)
	})
	if err != nil {
		return nil, State{}, fmt.Errorf("manifest: replay: %w", err)
	}
	m := &Manifest{dir: dir, state: st}
	if err := m.rewrite(); err != nil {
		return nil, State{}, err
	}
	return m, m.State(), nil
}

// rewrite writes the current state as one edit to a temp file and renames
// it over MANIFEST. The rename is atomic, so a crash leaves either the old
// log or the new snapshot, both complete.
func (m *Manifest) rewrite() error {
	path := filepath.Join(m.dir, FileName)
	tmp := path + ".tmp"
	os.Remove(tmp)
	snap := Edit{
		SetLogNumber: true, LogNumber: m.state.LogNumber,
		SetNextFile: true, NextFile: m.state.NextFile,
		SetLastSeq: true, LastSeq: m.state.LastSeq,
	}
	for _, f := range m.state.NewestFirst() {
		snap.Added = append(snap.Added, f)
	}
	w, err := wal.OpenWriter(tmp)
	if err != nil {
		return err
	}
	if _, err := w.Append(snap.encode()); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	if m.w != nil {
		m.w.Close()
		m.w = nil
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if err := sstable.SyncDir(m.dir); err != nil {
		return err
	}
	m.w, err = wal.OpenWriter(path)
	return err
}

// Apply durably records e and applies it to the in-memory state. When it
// returns nil, the edit survives a crash.
func (m *Manifest) Apply(e Edit) error {
	next := m.copyState()
	if err := next.Apply(e); err != nil {
		return err
	}
	off, err := m.w.Append(e.encode())
	if err == nil {
		err = m.w.SyncTo(off)
	}
	if err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	m.state = next
	return nil
}

func (m *Manifest) copyState() State {
	s := m.state
	s.Files = make(map[uint64]FileMeta, len(m.state.Files))
	for k, v := range m.state.Files {
		s.Files[k] = v
	}
	return s
}

// State returns a copy of the current state.
func (m *Manifest) State() State { return m.copyState() }

// Size returns the manifest file size in bytes.
func (m *Manifest) Size() int64 { return m.w.Size() }

// Close syncs and closes the manifest.
func (m *Manifest) Close() error {
	if m.w == nil {
		return nil
	}
	err := m.w.Close()
	m.w = nil
	return err
}
