// Package cluster turns the single-node LSM engine (db.DB) into a replicated,
// highly-available key-value store by driving it from a raft.Node.
//
// # How a write flows
//
//	client PUT -> leader server -> raft.Propose(command)
//	           -> leader appends to its log, replicates to followers
//	           -> once a majority has stored it, it is committed
//	           -> every node's apply loop calls db.Put / db.Delete
//	           -> leader acknowledges 204 to the client
//
// A follower that receives a write returns the leader's address so the client
// (or a proxy) can retry there. Reads are served locally from each node's db;
// they are "read-your-writes" on the leader and eventually consistent on
// followers, which is the right trade-off for a KV cache/store and keeps the
// read path as fast as the single-node engine.
//
// The db is the replicated state machine: applying the same ordered log of
// Put/Delete commands on every node makes every node's db converge to the
// same contents.
package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"time"

	"github.com/kumar-ayush101/LSM-engine/db"
	"github.com/kumar-ayush101/LSM-engine/raft"
)

// ErrNotLeader is returned by Put/Delete on a non-leader node. Callers should
// redirect to Leader().
var ErrNotLeader = errors.New("cluster: not leader")

// Config configures a cluster Node.
type Config struct {
	// ID is this node's unique identifier.
	ID string
	// Peers maps every OTHER node's ID to its raft peer base URL
	// (e.g. "http://10.0.0.2:9001"). Must not include ID.
	Peers map[string]string
	// DataDir holds the db data and the raft state file.
	DataDir string
	// ClusterSecret authenticates peer-to-peer raft RPCs.
	ClusterSecret string

	// DBOptions is passed through to db.Open (nil for defaults).
	DBOptions *db.Options

	// Raft timing (zero = sensible defaults).
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration

	Logger *slog.Logger
}

// Node is a single member of the replicated cluster: an LSM db plus the raft
// node that replicates writes to it.
type Node struct {
	cfg   Config
	log   *slog.Logger
	db    *db.DB
	raft  *raft.Node
	trans *raft.HTTPTransport
	store *raft.FileStorage
}

// dbApplier applies committed raft commands to the db. It satisfies
// raft.Applier. Apply is called from raft's single apply goroutine in log
// order, so no extra locking is needed.
type dbApplier struct{ d *db.DB }

func (a dbApplier) Apply(cmd raft.Command) error {
	switch cmd.Kind {
	case raft.CmdPut:
		return a.d.Put(cmd.Key, cmd.Value)
	case raft.CmdDelete:
		return a.d.Delete(cmd.Key)
	case raft.CmdNoop:
		return nil
	default:
		return fmt.Errorf("cluster: unknown command kind %d", cmd.Kind)
	}
}

// Open starts a cluster node: it opens the db, recovers the raft state and
// begins participating in elections and replication.
func Open(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ID == "" {
		return nil, errors.New("cluster: empty node ID")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("cluster: empty data dir")
	}
	if len(cfg.ClusterSecret) < 16 {
		return nil, errors.New("cluster: ClusterSecret must be at least 16 bytes")
	}

	d, err := db.Open(filepath.Join(cfg.DataDir, "db"), cfg.DBOptions)
	if err != nil {
		return nil, fmt.Errorf("cluster: open db: %w", err)
	}

	store, err := raft.OpenFileStorage(filepath.Join(cfg.DataDir, "raft", "state.log"))
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("cluster: open raft storage: %w", err)
	}

	trans := raft.NewHTTPTransport(cfg.ClusterSecret, cfg.RPCTimeout)

	rn, err := raft.NewNode(raft.Config{
		ID:                 cfg.ID,
		Peers:              cfg.Peers,
		Storage:            store,
		Transport:          trans,
		Applier:            dbApplier{d},
		HeartbeatInterval:  cfg.HeartbeatInterval,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		Logger:             cfg.Logger,
	})
	if err != nil {
		store.Close()
		d.Close()
		return nil, fmt.Errorf("cluster: new raft node: %w", err)
	}

	n := &Node{cfg: cfg, log: cfg.Logger, db: d, raft: rn, trans: trans, store: store}
	rn.Start()
	return n, nil
}

// Close stops raft and closes the db and raft storage.
func (n *Node) Close() error {
	n.raft.Stop()
	err := n.store.Close()
	if derr := n.db.Close(); err == nil {
		err = derr
	}
	return err
}

// Put replicates a key/value write through raft. It returns ErrNotLeader if
// this node is not the leader; the caller should redirect to Leader().
func (n *Node) Put(key, value []byte) error {
	return n.propose(raft.NewPutCommand(key, value))
}

// Delete replicates a delete through raft.
func (n *Node) Delete(key []byte) error {
	return n.propose(raft.NewDeleteCommand(key))
}

func (n *Node) propose(cmd []byte) error {
	err := n.raft.ProposeAndWait(cmd)
	if errors.Is(err, raft.ErrNotLeader) {
		return ErrNotLeader
	}
	return err
}

// Get reads a key from the local db. On the leader this is read-your-writes;
// on a follower it is eventually consistent (as fresh as the last applied
// entry).
func (n *Node) Get(key []byte) ([]byte, error) { return n.db.Get(key) }

// DB exposes the underlying engine for reads, scans and stats.
func (n *Node) DB() *db.DB { return n.db }

// IsLeader reports whether this node is the current raft leader.
func (n *Node) IsLeader() bool { return n.raft.IsLeader() }

// Leader returns the current leader's node ID and raft peer address (empty
// address if this node is the leader or the leader is unknown).
func (n *Node) Leader() (id, addr string) { return n.raft.LeaderHint() }

// Status returns the raft status for monitoring.
func (n *Node) Status() raft.Status { return n.raft.Status() }

// RaftHandler returns the HTTP handler for peer-to-peer raft RPCs. Mount it on
// the INTERNAL peer listener, never the public client API.
func (n *Node) RaftHandler() http.Handler { return n.trans.Handler(n.raft) }
