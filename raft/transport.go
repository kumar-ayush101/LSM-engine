package raft

// This file defines the two Raft RPCs and the Transport interface the core
// uses to send them. Keeping the messages and the interface separate from any
// particular wire format lets the core be tested with an in-memory transport
// and run in production over HTTP (transport_http.go).

// RequestVoteArgs is sent by a candidate to gather votes (Raft §5.2).
type RequestVoteArgs struct {
	Term         Term   `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex Index  `json:"last_log_index"`
	LastLogTerm  Term   `json:"last_log_term"`
}

// RequestVoteReply is the response to RequestVote.
type RequestVoteReply struct {
	Term        Term `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesArgs is sent by the leader to replicate log entries and as a
// heartbeat when Entries is empty (Raft §5.3).
type AppendEntriesArgs struct {
	Term         Term    `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex Index   `json:"prev_log_index"`
	PrevLogTerm  Term    `json:"prev_log_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit Index   `json:"leader_commit"`
}

// AppendEntriesReply is the response to AppendEntries.
//
// ConflictIndex and ConflictTerm let the leader back up nextIndex by more
// than one per round trip when a follower's log diverges (an optimization
// described in the Raft paper §5.3 and the author's dissertation).
type AppendEntriesReply struct {
	Term    Term `json:"term"`
	Success bool `json:"success"`

	ConflictIndex Index `json:"conflict_index,omitempty"`
	ConflictTerm  Term  `json:"conflict_term,omitempty"`
}

// Transport sends RPCs to peers identified by node ID. Implementations must
// be safe for concurrent use. A call that cannot reach the peer returns an
// error; the core treats that as "no reply" and retries later, so a transport
// should apply its own timeout rather than block forever.
type Transport interface {
	SendRequestVote(peerID string, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error)
}
