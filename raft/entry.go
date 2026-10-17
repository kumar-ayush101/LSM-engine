package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Term is a Raft term: a logical clock that increases every time an election
// is started. At most one leader is elected per term.
type Term uint64

// Index is a 1-based position in the replicated log. Index 0 means "before
// the first entry" and is used as a sentinel (e.g. prevLogIndex of the very
// first AppendEntries).
type Index uint64

// Entry is one record in the replicated log: a client command tagged with
// the term in which the leader created it. The term is what makes the
// consistency check in AppendEntries work.
type Entry struct {
	Term    Term
	Index   Index
	Command []byte // an encoded Command; empty for a no-op entry
}

// CmdKind is the operation a Command performs on the state machine.
type CmdKind uint8

const (
	// CmdNoop is a leader's first entry in a new term. Committing an entry
	// from the current term lets the leader safely advance commitIndex over
	// entries inherited from previous terms (Raft §5.4.2).
	CmdNoop CmdKind = iota
	// CmdPut sets Key to Value.
	CmdPut
	// CmdDelete removes Key (writes a tombstone in the LSM engine).
	CmdDelete
)

// Command is a decoded state-machine operation carried by an Entry.
type Command struct {
	Kind  CmdKind
	Key   []byte
	Value []byte
}

// ErrBadCommand is returned when a command cannot be decoded.
var ErrBadCommand = errors.New("raft: malformed command")

// MaxCommandSize bounds an encoded command so a corrupt length field cannot
// make recovery allocate without limit. It must comfortably exceed the
// server's key and value limits.
const MaxCommandSize = 64 << 20

// Encode serializes a command:
//
//	kind (1 B) | key len (uvarint) | key | value
//
// The value is the remaining bytes, so its length is implicit.
func (c Command) Encode() []byte {
	buf := make([]byte, 1+binary.MaxVarintLen64+len(c.Key)+len(c.Value))
	buf[0] = byte(c.Kind)
	n := 1 + binary.PutUvarint(buf[1:], uint64(len(c.Key)))
	n += copy(buf[n:], c.Key)
	n += copy(buf[n:], c.Value)
	return buf[:n]
}

// DecodeCommand parses bytes produced by Encode.
func DecodeCommand(b []byte) (Command, error) {
	if len(b) == 0 {
		return Command{}, fmt.Errorf("%w: empty", ErrBadCommand)
	}
	kind := CmdKind(b[0])
	if kind > CmdDelete {
		return Command{}, fmt.Errorf("%w: unknown kind %d", ErrBadCommand, kind)
	}
	rest := b[1:]
	klen, n := binary.Uvarint(rest)
	if n <= 0 {
		return Command{}, fmt.Errorf("%w: bad key length", ErrBadCommand)
	}
	rest = rest[n:]
	if uint64(len(rest)) < klen {
		return Command{}, fmt.Errorf("%w: key length %d exceeds payload", ErrBadCommand, klen)
	}
	key := rest[:klen]
	value := rest[klen:]
	// Copy so the caller owns the memory independent of b.
	cmd := Command{Kind: kind}
	if klen > 0 {
		cmd.Key = append([]byte(nil), key...)
	}
	if len(value) > 0 {
		cmd.Value = append([]byte(nil), value...)
	}
	return cmd, nil
}

// NewPutCommand and NewDeleteCommand are convenience constructors used by the
// server when proposing client writes.
func NewPutCommand(key, value []byte) []byte {
	return Command{Kind: CmdPut, Key: key, Value: value}.Encode()
}

func NewDeleteCommand(key []byte) []byte {
	return Command{Kind: CmdDelete, Key: key}.Encode()
}
