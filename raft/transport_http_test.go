package raft

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestHTTPClusterReplicates spins up a real 3-node cluster where peers talk
// over HTTP (httptest servers on loopback), proving the HTTP transport and
// the core work together end to end.
func TestHTTPClusterReplicates(t *testing.T) {
	ids := []string{"a", "b", "c"}
	secret := "cluster-secret-token"

	type member struct {
		node *Node
		sm   *stateMachine
		srv  *httptest.Server
	}
	members := map[string]*member{}
	addrs := map[string]string{}

	// First pass: create httptest servers so we know each peer's URL. We use
	// a mux we can populate after the node exists.
	muxes := map[string]*http.ServeMux{}
	for _, id := range ids {
		mux := http.NewServeMux()
		muxes[id] = mux
		srv := httptest.NewServer(mux)
		addrs[id] = srv.URL
		members[id] = &member{srv: srv}
	}
	t.Cleanup(func() {
		for _, m := range members {
			m.srv.Close()
		}
	})

	// Second pass: build nodes now that addresses are known.
	for _, id := range ids {
		peers := map[string]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		sm := newStateMachine()
		trans := NewHTTPTransport(secret, 100*time.Millisecond)
		n, err := NewNode(Config{
			ID:                 id,
			Peers:              peers,
			Storage:            newMemStorage(),
			Transport:          trans,
			Applier:            sm,
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 300 * time.Millisecond,
			ElectionTimeoutMax: 600 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		members[id].node = n
		members[id].sm = sm
		// Mount the RPC handler onto this node's server.
		h := trans.Handler(n)
		muxes[id].Handle("/raft/", h)
	}
	for _, id := range ids {
		members[id].node.Start()
	}
	t.Cleanup(func() {
		for _, m := range members {
			m.node.Stop()
		}
	})

	// Wait for a leader.
	var leader *Node
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && leader == nil {
		for _, id := range ids {
			if members[id].node.IsLeader() {
				leader = members[id].node
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected over HTTP transport")
	}
	t.Logf("leader=%s", leader.id)

	// Write through the leader; all nodes converge.
	for i := 0; i < 20; i++ {
		cmd := NewPutCommand([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
		if err := leader.ProposeAndWait(cmd); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	converged := func() bool {
		for _, id := range ids {
			if len(members[id].sm.snapshot()) != 20 {
				return false
			}
		}
		return true
	}
	d2 := time.Now().Add(3 * time.Second)
	for time.Now().Before(d2) && !converged() {
		time.Sleep(20 * time.Millisecond)
	}
	if !converged() {
		for _, id := range ids {
			t.Logf("node %s applied %d keys", id, len(members[id].sm.snapshot()))
		}
		t.Fatal("nodes did not converge over HTTP transport")
	}

	// Spot-check values on a follower.
	for _, id := range ids {
		if id == leader.id {
			continue
		}
		if v, ok := members[id].sm.get("k05"); !ok || v != "v05" {
			t.Fatalf("follower %s k05 = %q,%v", id, v, ok)
		}
	}
}

// TestHTTPTransportRejectsBadSecret verifies the cluster secret gates RPCs.
func TestHTTPTransportRejectsBadSecret(t *testing.T) {
	sm := newStateMachine()
	n, err := NewNode(Config{
		ID:        "a",
		Peers:     map[string]string{"b": "http://127.0.0.1:1"},
		Storage:   newMemStorage(),
		Transport: NewHTTPTransport("right-secret", 200*time.Millisecond),
		Applier:   sm,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := NewHTTPTransport("right-secret", 0).Handler(n)
	srv := httptest.NewServer(server)
	defer srv.Close()

	// A client with the wrong secret must be rejected.
	wrong := NewHTTPTransport("wrong-secret", 200*time.Millisecond)
	if _, err := wrong.SendRequestVote(srv.URL, RequestVoteArgs{Term: 1, CandidateID: "x"}); err == nil {
		t.Fatal("expected rejection with wrong cluster secret")
	}
	// The right secret works.
	right := NewHTTPTransport("right-secret", 200*time.Millisecond)
	if _, err := right.SendRequestVote(srv.URL, RequestVoteArgs{Term: 1, CandidateID: "x"}); err != nil {
		t.Fatalf("right secret rejected: %v", err)
	}
}
