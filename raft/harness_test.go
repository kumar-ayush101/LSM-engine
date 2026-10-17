package raft

import (
	"sync"
	"testing"
	"time"
)

// memTransport routes RPCs between in-process nodes. It supports partitioning
// a node (dropping all its traffic) to simulate crashes and network splits.
type memTransport struct {
	mu    sync.Mutex
	nodes map[string]*Node
	down  map[string]bool // partitioned node IDs
}

func newMemTransport() *memTransport {
	return &memTransport{nodes: map[string]*Node{}, down: map[string]bool{}}
}

func (t *memTransport) register(n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[n.id] = n
}

func (t *memTransport) setDown(id string, down bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.down[id] = down
}

// peerTransport is the Transport handed to a single node; it tags the sender
// so partitioning blocks traffic in both directions.
type peerTransport struct {
	t    *memTransport
	self string
}

func (p *peerTransport) reachable(peer string) (*Node, bool) {
	p.t.mu.Lock()
	defer p.t.mu.Unlock()
	if p.t.down[p.self] || p.t.down[peer] {
		return nil, false
	}
	n, ok := p.t.nodes[peer]
	return n, ok
}

func (p *peerTransport) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	n, ok := p.reachable(peer)
	if !ok {
		return RequestVoteReply{}, ErrStopped
	}
	return n.HandleRequestVote(args), nil
}

func (p *peerTransport) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	n, ok := p.reachable(peer)
	if !ok {
		return AppendEntriesReply{}, ErrStopped
	}
	return n.HandleAppendEntries(args), nil
}

// stateMachine is a trivial applier that records applied commands as a map,
// used to check that replicas converge.
type stateMachine struct {
	mu   sync.Mutex
	data map[string]string
	ops  int
}

func newStateMachine() *stateMachine {
	return &stateMachine{data: map[string]string{}}
}

func (s *stateMachine) Apply(cmd Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops++
	switch cmd.Kind {
	case CmdPut:
		s.data[string(cmd.Key)] = string(cmd.Value)
	case CmdDelete:
		delete(s.data, string(cmd.Key))
	}
	return nil
}

func (s *stateMachine) get(k string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[k]
	return v, ok
}

func (s *stateMachine) snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// cluster is a set of in-process nodes wired through a memTransport.
type cluster struct {
	t     *testing.T
	trans *memTransport
	nodes map[string]*Node
	sms   map[string]*stateMachine
	ids   []string
}

func newCluster(t *testing.T, ids ...string) *cluster {
	t.Helper()
	c := &cluster{
		t:     t,
		trans: newMemTransport(),
		nodes: map[string]*Node{},
		sms:   map[string]*stateMachine{},
		ids:   ids,
	}
	for _, id := range ids {
		peers := map[string]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = other
			}
		}
		sm := newStateMachine()
		n, err := NewNode(Config{
			ID:                 id,
			Peers:              peers,
			Storage:            newMemStorage(),
			Transport:          &peerTransport{t: c.trans, self: id},
			Applier:            sm,
			HeartbeatInterval:  10 * time.Millisecond,
			ElectionTimeoutMin: 60 * time.Millisecond,
			ElectionTimeoutMax: 120 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}
		c.nodes[id] = n
		c.sms[id] = sm
		c.trans.register(n)
	}
	return c
}

func (c *cluster) start() {
	for _, n := range c.nodes {
		n.Start()
	}
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

// waitLeader waits until exactly one leader exists and returns it.
func (c *cluster) waitLeader(timeout time.Duration) *Node {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, id := range c.ids {
			if c.trans.down[id] {
				continue
			}
			if c.nodes[id].IsLeader() {
				leaders = append(leaders, c.nodes[id])
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no single leader elected within %s", timeout)
	return nil
}

// memStorage is an in-memory Storage for tests (no fsync).
type memStorage struct {
	mu      sync.Mutex
	hard    HardState
	entries []Entry
	first   Index
}

func newMemStorage() *memStorage { return &memStorage{first: 1} }

func (s *memStorage) LoadHardState() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hard, nil
}

func (s *memStorage) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hard = hs
	return nil
}

func (s *memStorage) FirstIndex() Index { return 1 }

func (s *memStorage) LastIndex() Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastLocked()
}

func (s *memStorage) lastLocked() Index {
	if len(s.entries) == 0 {
		return 0
	}
	return s.entries[len(s.entries)-1].Index
}

func (s *memStorage) Term(i Index) (Term, bool) {
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

func (s *memStorage) Entries(lo, hi Index) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Entry, hi-lo+1)
	copy(out, s.entries[lo-s.first:hi-s.first+1])
	return out, nil
}

func (s *memStorage) Append(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		if len(s.entries) == 0 {
			s.first = e.Index
		}
		s.entries = append(s.entries, e)
	}
	return nil
}

func (s *memStorage) TruncateSuffix(from Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 || from > s.lastLocked() {
		return nil
	}
	if from <= s.first {
		s.entries = s.entries[:0]
	} else {
		s.entries = s.entries[:from-s.first]
	}
	return nil
}

func (s *memStorage) Close() error { return nil }
