package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/kumar-ayush101/LSM-engine/bloom"
	"github.com/kumar-ayush101/LSM-engine/internal/base"
)

// WriterOptions configures a Writer.
type WriterOptions struct {
	BlockSize   int     // default DefaultBlockSize
	BloomFPRate float64 // default bloom.DefaultFPRate
}

// Meta summarizes a finished table.
type Meta struct {
	Size     int64
	Entries  uint64
	Smallest base.InternalKey // copies, safe to keep
	Largest  base.InternalKey
	MinSeq   base.SeqNum
	MaxSeq   base.SeqNum
}

// Writer builds a table from entries added in strictly increasing
// internal-key order. The file is written to path+".tmp", fsynced, and
// renamed into place by Finish, so a crash never leaves a half-written table
// under the final name.
type Writer struct {
	path, tmp string
	f         *os.File
	w         *bufio.Writer
	opts      WriterOptions

	offset   uint64
	block    []byte
	lastKey  []byte // encoded internal key of the last entry added
	index    []byte
	filter   *bloom.Builder
	meta     Meta
	haveLast bool
	err      error
}

// Create starts a new table at path.
func Create(path string, opts WriterOptions) (*Writer, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{
		path: path, tmp: tmp, f: f,
		w:      bufio.NewWriterSize(f, 64<<10),
		opts:   opts,
		filter: bloom.NewBuilder(opts.BloomFPRate),
	}, nil
}

// Add appends an entry. Keys must be strictly increasing.
func (w *Writer) Add(key base.InternalKey, value []byte) error {
	if w.err != nil {
		return w.err
	}
	enc := key.Encode(nil)
	if w.haveLast {
		prev, _ := base.DecodeInternalKey(w.lastKey)
		if base.Compare(prev, key) >= 0 {
			w.err = fmt.Errorf("sstable: keys out of order: %v then %v", prev, key)
			return w.err
		}
	} else {
		w.meta.Smallest = copyKey(key)
		w.meta.MinSeq, w.meta.MaxSeq = key.Seq, key.Seq
	}
	w.meta.MinSeq = min(w.meta.MinSeq, key.Seq)
	w.meta.MaxSeq = max(w.meta.MaxSeq, key.Seq)

	w.block = binary.AppendUvarint(w.block, uint64(len(enc)))
	w.block = binary.AppendUvarint(w.block, uint64(len(value)))
	w.block = append(w.block, enc...)
	w.block = append(w.block, value...)
	w.lastKey = append(w.lastKey[:0], enc...)
	w.haveLast = true
	w.filter.Add(key.UserKey)
	w.meta.Entries++

	if len(w.block) >= w.opts.BlockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *Writer) writeBlock(b []byte) (blockHandle, error) {
	h := blockHandle{offset: w.offset, length: uint64(len(b))}
	if _, err := w.w.Write(b); err != nil {
		return h, err
	}
	var crc [crcLen]byte
	if _, err := w.w.Write(appendCRC(crc[:0], b)); err != nil {
		return h, err
	}
	w.offset += uint64(len(b)) + crcLen
	return h, nil
}

func (w *Writer) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}
	h, err := w.writeBlock(w.block)
	if err != nil {
		w.err = err
		return err
	}
	w.index = binary.AppendUvarint(w.index, uint64(len(w.lastKey)))
	w.index = append(w.index, w.lastKey...)
	w.index = binary.AppendUvarint(w.index, h.offset)
	w.index = binary.AppendUvarint(w.index, h.length)
	w.block = w.block[:0]
	return nil
}

// Finish writes the filter, index and footer, fsyncs, and renames the file
// into place. The Writer cannot be used afterwards.
func (w *Writer) Finish() (Meta, error) {
	if w.err != nil {
		w.Abort()
		return Meta{}, w.err
	}
	if !w.haveLast {
		w.Abort()
		return Meta{}, errors.New("sstable: empty table")
	}
	last, _ := base.DecodeInternalKey(w.lastKey)
	w.meta.Largest = copyKey(last)

	err := w.flushBlock()
	var fh, ih blockHandle
	if err == nil {
		fh, err = w.writeBlock(w.filter.Finish())
	}
	if err == nil {
		ih, err = w.writeBlock(w.index)
	}
	if err == nil {
		var foot [footerLen]byte
		binary.LittleEndian.PutUint64(foot[0:], fh.offset)
		binary.LittleEndian.PutUint64(foot[8:], fh.length)
		binary.LittleEndian.PutUint64(foot[16:], ih.offset)
		binary.LittleEndian.PutUint64(foot[24:], ih.length)
		binary.LittleEndian.PutUint64(foot[32:], w.meta.Entries)
		binary.LittleEndian.PutUint64(foot[40:], magic)
		_, err = w.w.Write(foot[:])
		w.offset += footerLen
	}
	if err == nil {
		err = w.w.Flush()
	}
	if err == nil {
		err = w.f.Sync()
	}
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(w.tmp, w.path)
	}
	if err == nil {
		err = SyncDir(filepath.Dir(w.path))
	}
	if err != nil {
		os.Remove(w.tmp)
		return Meta{}, err
	}
	w.meta.Size = int64(w.offset)
	w.err = errors.New("sstable: writer finished")
	return w.meta, nil
}

// Abort discards the partially written table.
func (w *Writer) Abort() {
	w.f.Close()
	os.Remove(w.tmp)
	if w.err == nil {
		w.err = errors.New("sstable: writer aborted")
	}
}

func copyKey(k base.InternalKey) base.InternalKey {
	return base.InternalKey{UserKey: append([]byte(nil), k.UserKey...), Seq: k.Seq, Kind: k.Kind}
}

// SyncDir fsyncs a directory so renames and new files in it are durable.
// It is a no-op on Windows, where Go cannot sync a directory handle and NTFS
// journals metadata.
func SyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
