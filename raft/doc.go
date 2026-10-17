// Package raft implements the Raft consensus algorithm from scratch, using
// only the Go standard library (no etcd/raft, hashicorp/raft or any other
// consensus library), to replicate the LSM key-value engine across a small
// cluster of nodes.
//
// # What Raft gives us
//
// A single-node LSM engine is durable but not available: if the node or its
// disk dies, the data is gone and writes stop. Raft keeps an identical,
// ordered log of write commands on every node. One node is the leader; it is
// the only node that accepts writes. A write is acknowledged to the client
// only after it is stored on a majority of nodes, so the cluster tolerates
// the failure of a minority (one node in a three-node cluster) with no data
// loss and only a brief pause while a new leader is elected.
//
// # The three sub-problems (Raft paper, Ongaro & Ousterhout 2014)
//
//   - Leader election. Nodes start as followers. If a follower hears nothing
//     from a leader within a randomized election timeout it becomes a
//     candidate, increments the term, votes for itself and asks the others
//     for votes (RequestVote). A candidate that collects a majority becomes
//     leader and sends heartbeats to suppress further elections.
//   - Log replication. The leader appends each client command to its log and
//     sends it to the followers (AppendEntries). Once a majority has stored
//     an entry the leader marks it committed and applies it to the state
//     machine (the LSM db); followers apply it when they learn it is
//     committed.
//   - Safety. A candidate can only win if its log is at least as up to date
//     as the voter's (the election restriction), and a leader only counts an
//     entry from its own term toward commitment. Together these guarantee
//     that a committed entry is never lost or overwritten.
//
// # Layout of this package
//
//	entry.go       log entries and the Put/Delete command encoding
//	storage.go     crash-safe persistence of term, vote and the log
//	raft.go        the consensus state machine (follower/candidate/leader)
//	transport.go   the RPC interface and message types
//	transport_http.go  an HTTP implementation of the transport
//	applier.go     applies committed commands to a db.DB
//
// The core (raft.go) does no I/O of its own: it talks to a Storage for
// persistence, a Transport for RPCs, and an Applier for the state machine,
// all interfaces, so it can be driven deterministically in tests with
// in-memory fakes.
package raft
