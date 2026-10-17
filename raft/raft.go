package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Role is a node's current role in the cluster.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("Role(%d)", int(r))
	}
}

var (
	// ErrNotLeader is returned by Propose on a node that is not the leader.
	// The caller should redirect the client to LeaderHint.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrStopped is returned once the node is shutting down.
	ErrStopped = errors.New("raft: node stopped")
)

// Config configures a Node. All fields except the optional ones are required.
type Config struct {
	// ID is this node's unique identifier within the cluster.
	ID string
	// Peers maps every OTHER node's ID to an opaque address the Transport
	// understands. It must not include ID.
	Peers map[string]string

	Storage   Storage
	Transport Transport
	Applier   Applier

	// HeartbeatInterval is how often a leader sends AppendEntries. Default
	// 50ms. ElectionTimeout should be several multiples of it.
	HeartbeatInterval time.Duration
	// ElectionTimeoutMin/Max bound the randomized election timeout. A
	// follower that hears nothing for a random duration in this range starts
	// an election. Randomization keeps nodes from repeatedly splitting the
	// vote. Defaults: 150ms..300ms.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// Logger is optional; defaults to slog.Default().
	Logger *slog.Logger
}

func (c *Config) withDefaults() {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Node is one member of a Raft cluster. It owns a single goroutine (run) that
// is the only writer of the volatile role/term state; public methods and RPC
// handlers synchronize with it through mu. All durable state goes through the
// Storage before any action depending on it is externally visible.
type Node struct {
	cfg Config
	log *slog.Logger
	id  string
	// peers is the full set of voting members including self, used for
	// majority math.
	peerIDs []string

	mu sync.Mutex

	// Persistent state (mirrored durably via storage).
	currentTerm Term
	votedFor    string

	// Volatile state on all servers.
	role        Role
	commitIndex Index
	lastApplied Index
	leaderID    string

	// Volatile state on leaders, reset on election. Indexed by peer ID.
	nextIndex  map[string]Index
	matchIndex map[string]Index

	storage Storage
	trans   Transport
	applier Applier

	// Timers and signalling.
	electionDeadline time.Time
	applyCh          chan struct{}
	stopCh           chan struct{}
	stopped          bool
	wg               sync.WaitGroup

	// observers for tests/metrics
	onApply func(Entry)

	// appliedCond broadcasts whenever lastApplied advances, so Propose
	// callers waiting for their entry to be applied can wake up.
	appliedCond *sync.Cond
}

// NewNode creates a node and recovers its persistent state. Call Start to
// begin participating in the cluster.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: empty node ID")
	}
	if _, ok := cfg.Peers[cfg.ID]; ok {
		return nil, errors.New("raft: Peers must not include this node's own ID")
	}
	if cfg.Storage == nil || cfg.Transport == nil || cfg.Applier == nil {
		return nil, errors.New("raft: Storage, Transport and Applier are required")
	}
	cfg.withDefaults()

	n := &Node{
		cfg:        cfg,
		log:        cfg.Logger.With("node", cfg.ID),
		id:         cfg.ID,
		role:       Follower,
		storage:    cfg.Storage,
		trans:      cfg.Transport,
		applier:    cfg.Applier,
		nextIndex:  map[string]Index{},
		matchIndex: map[string]Index{},
		applyCh:    make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}
	n.peerIDs = append(n.peerIDs, cfg.ID)
	for id := range cfg.Peers {
		n.peerIDs = append(n.peerIDs, id)
	}
	n.appliedCond = sync.NewCond(&n.mu)

	hs, err := n.storage.LoadHardState()
	if err != nil {
		return nil, fmt.Errorf("raft: loading hard state: %w", err)
	}
	n.currentTerm = hs.CurrentTerm
	n.votedFor = hs.VotedFor
	return n, nil
}

// Start launches the node's background goroutines.
func (n *Node) Start() {
	n.resetElectionTimer()
	n.wg.Add(2)
	go n.run()
	go n.applyLoop()
}

// Stop halts the node and waits for its goroutines to exit.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.appliedCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// ---- public inspection --------------------------------------------------

// Status is a snapshot of the node's state for monitoring.
type Status struct {
	ID           string `json:"id"`
	Role         string `json:"role"`
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leader_id"`
	CommitIndex  uint64 `json:"commit_index"`
	LastApplied  uint64 `json:"last_applied"`
	LastLogIndex uint64 `json:"last_log_index"`
	Peers        int    `json:"peers"`
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:           n.id,
		Role:         n.role.String(),
		Term:         uint64(n.currentTerm),
		LeaderID:     n.leaderID,
		CommitIndex:  uint64(n.commitIndex),
		LastApplied:  uint64(n.lastApplied),
		LastLogIndex: uint64(n.storage.LastIndex()),
		Peers:        len(n.peerIDs),
	}
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// LeaderHint returns the Transport address of the current leader if known,
// along with its node ID. Used by the server to redirect writes.
func (n *Node) LeaderHint() (id, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	id = n.leaderID
	if id == n.id {
		return id, ""
	}
	return id, n.cfg.Peers[id]
}

// ---- proposing commands -------------------------------------------------

// Propose appends a command to the log if this node is the leader and returns
// the index it was placed at. The command is NOT yet committed; the caller
// waits for it to be applied (see WaitApplied) before acknowledging to a
// client.
func (n *Node) Propose(command []byte) (Index, Term, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, 0, ErrStopped
	}
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	idx := n.storage.LastIndex() + 1
	e := Entry{Term: n.currentTerm, Index: idx, Command: command}
	if err := n.storage.Append([]Entry{e}); err != nil {
		return 0, 0, err
	}
	n.matchIndex[n.id] = idx
	// Kick replication immediately rather than waiting for the next heartbeat.
	n.broadcastAppendLocked()
	return idx, n.currentTerm, nil
}

// ProposeAndWait proposes a command and blocks until it is committed and
// applied, or returns an error (ErrNotLeader, ErrStopped). It is the method
// the server uses to turn a client write into a replicated, acknowledged
// write.
func (n *Node) ProposeAndWait(command []byte) error {
	idx, term, err := n.Propose(command)
	if err != nil {
		return err
	}
	return n.WaitApplied(idx, term)
}

// majority returns the smallest number of nodes forming a majority.
func (n *Node) majority() int { return len(n.peerIDs)/2 + 1 }

// WaitApplied blocks until the entry at idx (proposed in term) has been
// applied to the state machine, or returns an error if this node stops being
// the leader for that term (so the entry may never commit here) or shuts
// down. A successful return means the command is committed cluster-wide and
// applied locally — safe to acknowledge to the client.
func (n *Node) WaitApplied(idx Index, term Term) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		if n.lastApplied >= idx {
			// Confirm the applied entry is the one we proposed (same term),
			// not a different entry that overwrote this index after a
			// leadership change.
			if t, ok := n.storage.Term(idx); ok && t == term {
				return nil
			}
			return ErrNotLeader
		}
		if n.stopped {
			return ErrStopped
		}
		// If we are no longer leader in the proposing term, the entry may be
		// overwritten; fail fast so the client can retry on the new leader.
		if n.currentTerm != term || n.role != Leader {
			// It might still commit if a later leader keeps it, but we cannot
			// guarantee it here; surface a retryable error.
			if t, ok := n.storage.Term(idx); ok && t == term && n.lastApplied >= idx {
				return nil
			}
			if n.currentTerm != term {
				return ErrNotLeader
			}
		}
		n.appliedCond.Wait()
	}
}

// ---- the main loop ------------------------------------------------------

func (n *Node) run() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.tickInterval())
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

// tickInterval is small enough to drive both heartbeats and election
// timeouts with acceptable resolution.
func (n *Node) tickInterval() time.Duration {
	return n.cfg.HeartbeatInterval / 2
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch n.role {
	case Leader:
		// Leaders send heartbeats every tick; replication also piggybacks.
		n.broadcastAppendLocked()
	case Follower, Candidate:
		if time.Now().After(n.electionDeadline) {
			n.startElectionLocked()
		}
	}
}

func (n *Node) resetElectionTimer() {
	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()
}

func (n *Node) resetElectionTimerLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	d := n.cfg.ElectionTimeoutMin + time.Duration(rand.Int64N(int64(span)+1))
	n.electionDeadline = time.Now().Add(d)
}

// ---- elections ----------------------------------------------------------

func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.persistHardStateLocked()
	n.resetElectionTimerLocked()

	term := n.currentTerm
	lastIdx := n.storage.LastIndex()
	lastTerm, _ := n.storage.Term(lastIdx)
	n.log.Info("starting election", "term", term, "last_index", lastIdx, "last_term", lastTerm)

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	votes := 1 // vote for self
	if votes >= n.majority() {
		n.becomeLeaderLocked() // single-node cluster
		return
	}
	for _, pid := range n.peerIDs {
		if pid == n.id {
			continue
		}
		go n.solicitVote(pid, n.cfg.Peers[pid], args, &votes)
	}
}

// solicitVote sends one RequestVote and tallies the result. It runs in its
// own goroutine and locks only to update shared state.
func (n *Node) solicitVote(peerID, peerAddr string, args RequestVoteArgs, votes *int) {
	reply, err := n.trans.SendRequestVote(peerAddr, args)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	// Stale: we have moved on since sending this request.
	if n.role != Candidate || n.currentTerm != args.Term {
		return
	}
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	if reply.VoteGranted {
		*votes++
		if *votes >= n.majority() {
			n.becomeLeaderLocked()
		}
	}
}

func (n *Node) becomeLeaderLocked() {
	if n.role == Leader {
		return
	}
	n.role = Leader
	n.leaderID = n.id
	last := n.storage.LastIndex()
	for _, pid := range n.peerIDs {
		n.nextIndex[pid] = last + 1
		n.matchIndex[pid] = 0
	}
	n.matchIndex[n.id] = last
	n.log.Info("became leader", "term", n.currentTerm, "last_index", last)

	// Commit an empty no-op from the new term so entries inherited from
	// earlier terms can be committed via the current-term rule (§5.4.2).
	noop := Entry{Term: n.currentTerm, Index: last + 1, Command: Command{Kind: CmdNoop}.Encode()}
	if err := n.storage.Append([]Entry{noop}); err != nil {
		n.log.Error("appending no-op failed", "err", err)
		return
	}
	n.matchIndex[n.id] = noop.Index
	n.broadcastAppendLocked()
}

// stepDownLocked reverts to follower on seeing a higher term.
func (n *Node) stepDownLocked(term Term) {
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.persistHardStateLocked()
	n.resetElectionTimerLocked()
	n.appliedCond.Broadcast()
}

func (n *Node) persistHardStateLocked() {
	if err := n.storage.SaveHardState(HardState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor}); err != nil {
		// A failure to persist is fatal: the node could violate safety by
		// forgetting its term or vote. Stop and let a restart recover.
		n.log.Error("persisting hard state failed; stopping node", "err", err)
		n.stopLocked()
	}
}

func (n *Node) stopLocked() {
	if n.stopped {
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.appliedCond.Broadcast()
}

// ---- replication (leader side) ------------------------------------------

func (n *Node) broadcastAppendLocked() {
	if n.role != Leader {
		return
	}
	for _, pid := range n.peerIDs {
		if pid == n.id {
			continue
		}
		n.sendAppendLocked(pid)
	}
}

func (n *Node) sendAppendLocked(peerID string) {
	next := n.nextIndex[peerID]
	if next < 1 {
		next = 1
	}
	prevIndex := next - 1
	prevTerm, ok := n.storage.Term(prevIndex)
	if !ok {
		prevTerm = 0
	}
	var entries []Entry
	last := n.storage.LastIndex()
	if next <= last {
		es, err := n.storage.Entries(next, last)
		if err == nil {
			entries = es
		}
	}
	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	go n.replicateTo(peerID, n.cfg.Peers[peerID], args, term)
}

func (n *Node) replicateTo(peerID, peerAddr string, args AppendEntriesArgs, term Term) {
	reply, err := n.trans.SendAppendEntries(peerAddr, args)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.currentTerm != term {
		return // stale
	}
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	if reply.Success {
		newMatch := args.PrevLogIndex + Index(len(args.Entries))
		if newMatch > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatch
			n.nextIndex[peerID] = newMatch + 1
		}
		n.maybeAdvanceCommitLocked()
		return
	}
	// Rejected: back up nextIndex using the conflict hint when present.
	if reply.ConflictTerm != 0 {
		// Find the last entry we have in ConflictTerm.
		idx := n.lastIndexForTermLocked(reply.ConflictTerm)
		if idx > 0 {
			n.nextIndex[peerID] = idx + 1
		} else {
			n.nextIndex[peerID] = reply.ConflictIndex
		}
	} else if reply.ConflictIndex > 0 {
		n.nextIndex[peerID] = reply.ConflictIndex
	} else if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
	if n.nextIndex[peerID] < 1 {
		n.nextIndex[peerID] = 1
	}
	// Retry immediately with the backed-up index.
	n.sendAppendLocked(peerID)
}

// lastIndexForTermLocked returns the highest index whose entry has term t, or
// 0 if the leader has no entry in that term.
func (n *Node) lastIndexForTermLocked(t Term) Index {
	last := n.storage.LastIndex()
	first := n.storage.FirstIndex()
	for i := last; i >= first; i-- {
		if et, ok := n.storage.Term(i); ok && et == t {
			return i
		}
		if i == first {
			break
		}
	}
	return 0
}

// maybeAdvanceCommitLocked advances commitIndex to the highest index stored on
// a majority, but only for entries from the current term (§5.4.2).
func (n *Node) maybeAdvanceCommitLocked() {
	last := n.storage.LastIndex()
	for idx := last; idx > n.commitIndex; idx-- {
		termAt, ok := n.storage.Term(idx)
		if !ok || termAt != n.currentTerm {
			continue
		}
		count := 0
		for _, pid := range n.peerIDs {
			if n.matchIndex[pid] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.signalApply()
			return
		}
	}
}

// ---- RPC handlers (receiver side) ---------------------------------------

// HandleRequestVote processes a RequestVote RPC and returns the reply.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := RequestVoteReply{Term: n.currentTerm}

	if n.stopped {
		return reply
	}
	if args.Term < n.currentTerm {
		return reply // stale candidate
	}
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}
	reply.Term = n.currentTerm

	// Grant at most one vote per term, and only to a candidate whose log is
	// at least as up to date as ours (§5.4.1).
	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	if canVote && n.candidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		n.votedFor = args.CandidateID
		n.persistHardStateLocked()
		n.resetElectionTimerLocked()
		reply.VoteGranted = true
	}
	return reply
}

func (n *Node) candidateUpToDateLocked(lastIdx Index, lastTerm Term) bool {
	myLast := n.storage.LastIndex()
	myTerm, _ := n.storage.Term(myLast)
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= myLast
}

// HandleAppendEntries processes an AppendEntries RPC and returns the reply.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := AppendEntriesReply{Term: n.currentTerm}

	if n.stopped {
		return reply
	}
	if args.Term < n.currentTerm {
		return reply // stale leader
	}
	// Valid leader for this term (or newer): become/stay follower.
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}
	n.role = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()
	reply.Term = n.currentTerm

	// Log consistency check: our entry at PrevLogIndex must match PrevLogTerm.
	if args.PrevLogIndex > 0 {
		myLast := n.storage.LastIndex()
		if args.PrevLogIndex > myLast {
			// We are missing entries; tell the leader where our log ends.
			reply.ConflictIndex = myLast + 1
			reply.ConflictTerm = 0
			return reply
		}
		myTerm, _ := n.storage.Term(args.PrevLogIndex)
		if myTerm != args.PrevLogTerm {
			// Conflict: report the first index of the conflicting term so the
			// leader can skip the whole term in one round trip.
			reply.ConflictTerm = myTerm
			reply.ConflictIndex = n.firstIndexForTermLocked(myTerm)
			return reply
		}
	}

	// Append any new entries, overwriting conflicts.
	if err := n.appendFollowerEntriesLocked(args.PrevLogIndex, args.Entries); err != nil {
		n.log.Error("follower append failed", "err", err)
		return reply // Success stays false
	}
	reply.Success = true

	// Advance commit index; apply up to the leader's commit or our last entry.
	if args.LeaderCommit > n.commitIndex {
		last := n.storage.LastIndex()
		n.commitIndex = min(args.LeaderCommit, last)
		n.signalApply()
	}
	return reply
}

func (n *Node) firstIndexForTermLocked(t Term) Index {
	first := n.storage.FirstIndex()
	last := n.storage.LastIndex()
	for i := first; i <= last; i++ {
		if et, ok := n.storage.Term(i); ok && et == t {
			return i
		}
	}
	return first
}

// appendFollowerEntriesLocked writes the leader's entries, truncating only
// where they actually conflict (so a repeated AppendEntries is idempotent and
// never drops committed entries).
func (n *Node) appendFollowerEntriesLocked(prevIndex Index, entries []Entry) error {
	for i, e := range entries {
		idx := prevIndex + Index(i) + 1
		if existing, ok := n.storage.Term(idx); ok {
			if existing == e.Term {
				continue // already have this entry
			}
			// Conflict: delete this and everything after, then append the rest.
			if err := n.storage.TruncateSuffix(idx); err != nil {
				return err
			}
		}
		if err := n.storage.Append([]Entry{e}); err != nil {
			return err
		}
	}
	return nil
}

// ---- apply loop ---------------------------------------------------------

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
			n.applyReady()
		}
	}
}

func (n *Node) applyReady() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		idx := n.lastApplied + 1
		es, err := n.storage.Entries(idx, idx)
		if err != nil || len(es) == 0 {
			n.mu.Unlock()
			return
		}
		e := es[0]
		n.mu.Unlock()

		cmd, err := DecodeCommand(e.Command)
		if err != nil {
			n.log.Error("decoding committed command failed; stopping", "index", idx, "err", err)
			n.mu.Lock()
			n.stopLocked()
			n.mu.Unlock()
			return
		}
		if cmd.Kind != CmdNoop {
			if err := n.applier.Apply(cmd); err != nil {
				n.log.Error("applying committed command failed; stopping", "index", idx, "err", err)
				n.mu.Lock()
				n.stopLocked()
				n.mu.Unlock()
				return
			}
		}
		n.mu.Lock()
		n.lastApplied = idx
		obs := n.onApply
		n.appliedCond.Broadcast()
		n.mu.Unlock()
		if obs != nil {
			obs(e)
		}
	}
}
