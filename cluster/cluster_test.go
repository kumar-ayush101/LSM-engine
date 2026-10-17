package cluster

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kumar-ayush101/LSM-engine/db"
)

// testCluster spins up n db-backed raft nodes talking over httptest servers.
type testCluster struct {
	t     *testing.T
	ids   []string
	nodes map[string]*Node
	srvs  map[string]*httptest.Server
}

func newTestCluster(t *testing.T, ids ...string) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, ids: ids, nodes: map[string]*Node{}, srvs: map[string]*httptest.Server{}}

	muxes := map[string]*http.ServeMux{}
	addrs := map[string]string{}
	for _, id := range ids {
		mux := http.NewServeMux()
		muxes[id] = mux
		srv := httptest.NewServer(mux)
		tc.srvs[id] = srv
		addrs[id] = srv.URL
	}

	for _, id := range ids {
		peers := map[string]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		n, err := Open(Config{
			ID:                 id,
			Peers:              peers,
			DataDir:            filepath.Join(t.TempDir(), id),
			ClusterSecret:      "test-cluster-secret-abcdef",
			DBOptions:          &db.Options{Sync: db.SyncPeriodic},
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 300 * time.Millisecond,
			ElectionTimeoutMax: 600 * time.Millisecond,
			RPCTimeout:         150 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Open %s: %v", id, err)
		}
		tc.nodes[id] = n
		muxes[id].Handle("/raft/", n.RaftHandler())
	}
	t.Cleanup(func() {
		// Stop incoming RPCs first (close servers), then stop nodes, so a
		// peer never calls into a node whose storage is already closed.
		for _, s := range tc.srvs {
			s.Close()
		}
		for _, n := range tc.nodes {
			n.Close()
		}
	})
	return tc
}

func (tc *testCluster) waitLeader(timeout time.Duration) *Node {
	tc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, id := range tc.ids {
			if tc.nodes[id].IsLeader() {
				leaders = append(leaders, tc.nodes[id])
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.t.Fatalf("no single leader within %s", timeout)
	return nil
}

func TestClusterReplicatesWritesToDB(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	leader := tc.waitLeader(3 * time.Second)

	// Write 30 keys through the leader.
	for i := 0; i < 30; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		v := []byte(fmt.Sprintf("val%02d", i))
		if err := leader.Put(k, v); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Delete one.
	if err := leader.Delete([]byte("key05")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Every node's db must converge: 29 keys present, key05 gone.
	waitFn(t, 3*time.Second, func() bool {
		for _, id := range tc.ids {
			d := tc.nodes[id].DB()
			if _, err := d.Get([]byte("key29")); err != nil {
				return false
			}
			if _, err := d.Get([]byte("key05")); !errors.Is(err, db.ErrNotFound) {
				return false
			}
		}
		return true
	})

	for _, id := range tc.ids {
		d := tc.nodes[id].DB()
		v, err := d.Get([]byte("key10"))
		if err != nil || string(v) != "val10" {
			t.Fatalf("node %s key10 = %q, %v", id, v, err)
		}
	}
}

func TestClusterFollowerRejectsWrite(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	leader := tc.waitLeader(3 * time.Second)
	for _, id := range tc.ids {
		n := tc.nodes[id]
		if n.IsLeader() {
			continue
		}
		if err := n.Put([]byte("k"), []byte("v")); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("follower %s Put = %v, want ErrNotLeader", id, err)
		}
		// The follower should be able to name the leader.
		lid, _ := n.Leader()
		if lid != leader.cfg.ID {
			t.Logf("follower %s leader hint = %q (leader is %q)", id, lid, leader.cfg.ID)
		}
	}
}

func TestClusterSurvivesLeaderLoss(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	leader := tc.waitLeader(3 * time.Second)

	for i := 0; i < 5; i++ {
		if err := leader.Put([]byte(fmt.Sprintf("a%d", i)), []byte("1")); err != nil {
			t.Fatal(err)
		}
	}

	// Close the leader (simulates a crash). A new leader must emerge among
	// the remaining two and keep accepting writes.
	oldLeaderID := leader.cfg.ID
	if err := leader.Close(); err != nil {
		t.Logf("closing old leader: %v", err)
	}
	delete(tc.nodes, oldLeaderID)
	tc.srvs[oldLeaderID].Close()

	var newLeader *Node
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && newLeader == nil {
		for id, n := range tc.nodes {
			_ = id
			if n.IsLeader() {
				newLeader = n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader after old leader crashed")
	}
	t.Logf("new leader after failover: %s", newLeader.cfg.ID)

	for i := 0; i < 5; i++ {
		if err := newLeader.Put([]byte(fmt.Sprintf("b%d", i)), []byte("2")); err != nil {
			t.Fatalf("post-failover put: %v", err)
		}
	}
	// Data written before the crash is still readable on the new leader.
	if v, err := newLeader.Get([]byte("a3")); err != nil || string(v) != "1" {
		t.Fatalf("pre-crash key a3 = %q, %v", v, err)
	}
}

func waitFn(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
