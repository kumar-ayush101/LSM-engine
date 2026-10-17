package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestSingleNodeElectsItself(t *testing.T) {
	c := newCluster(t, "a")
	c.start()
	defer c.stop()
	leader := c.waitLeader(2 * time.Second)
	if leader.id != "a" {
		t.Fatalf("expected a to be leader, got %s", leader.id)
	}
}

func TestThreeNodeElectsOneLeader(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	c.start()
	defer c.stop()
	leader := c.waitLeader(2 * time.Second)
	t.Logf("leader=%s term=%d", leader.id, leader.Status().Term)
}

func TestReplicationAppliesOnAllNodes(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	c.start()
	defer c.stop()
	leader := c.waitLeader(2 * time.Second)

	for i := 0; i < 10; i++ {
		cmd := NewPutCommand([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		if err := leader.ProposeAndWait(cmd); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	// Every node's state machine must converge to the same 10 keys.
	waitConverged(t, c, 10, 2*time.Second)
	for _, id := range c.ids {
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("k%d", i)
			v, ok := c.sms[id].get(k)
			if !ok || v != fmt.Sprintf("v%d", i) {
				t.Fatalf("node %s key %s = %q,%v", id, k, v, ok)
			}
		}
	}
}

func TestDeleteReplicates(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	c.start()
	defer c.stop()
	leader := c.waitLeader(2 * time.Second)
	must(t, leader.ProposeAndWait(NewPutCommand([]byte("x"), []byte("1"))))
	must(t, leader.ProposeAndWait(NewDeleteCommand([]byte("x"))))
	waitConverged(t, c, 0, 2*time.Second)
	for _, id := range c.ids {
		if _, ok := c.sms[id].get("x"); ok {
			t.Fatalf("node %s still has x after delete", id)
		}
	}
}

func TestLeaderFailoverContinues(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	c.start()
	defer c.stop()
	leader := c.waitLeader(2 * time.Second)

	// Write some data under the first leader.
	for i := 0; i < 5; i++ {
		must(t, leader.ProposeAndWait(NewPutCommand([]byte(fmt.Sprintf("pre%d", i)), []byte("1"))))
	}

	// Partition the leader off; the remaining two must elect a new leader.
	c.trans.setDown(leader.id, true)
	t.Logf("partitioned old leader %s", leader.id)

	// Wait for a new leader among the survivors.
	var newLeader *Node
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			if id == leader.id {
				continue
			}
			if c.nodes[id].IsLeader() {
				newLeader = c.nodes[id]
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader after partitioning the old one")
	}
	t.Logf("new leader=%s", newLeader.id)

	// New leader accepts writes; the two survivors converge.
	for i := 0; i < 5; i++ {
		if err := newLeader.ProposeAndWait(NewPutCommand([]byte(fmt.Sprintf("post%d", i)), []byte("2"))); err != nil {
			t.Fatalf("post-failover propose: %v", err)
		}
	}

	// Heal the partition; the old leader must step down and catch up.
	c.trans.setDown(leader.id, false)
	waitFn(t, 3*time.Second, func() bool {
		pre, post := 0, 0
		data := c.sms[leader.id].snapshot()
		for k := range data {
			switch {
			case len(k) >= 3 && k[:3] == "pre":
				pre++
			case len(k) >= 4 && k[:4] == "post":
				post++
			}
		}
		return pre == 5 && post == 5
	})
	// The reconnected old leader must no longer think it is leader.
	if c.nodes[leader.id].IsLeader() && newLeader.IsLeader() {
		t.Fatal("two leaders after healing")
	}
}

func TestProposeOnFollowerRejected(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	c.start()
	defer c.stop()
	leader := c.waitLeader(2 * time.Second)
	for _, id := range c.ids {
		if id == leader.id {
			continue
		}
		if _, _, err := c.nodes[id].Propose(NewPutCommand([]byte("k"), []byte("v"))); err != ErrNotLeader {
			t.Fatalf("follower %s Propose = %v, want ErrNotLeader", id, err)
		}
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	// A node restarts and recovers its term/vote and log from FileStorage.
	dir := t.TempDir()
	st, err := OpenFileStorage(dir + "/raft.state")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveHardState(HardState{CurrentTerm: 7, VotedFor: "b"}); err != nil {
		t.Fatal(err)
	}
	must(t, st.Append([]Entry{
		{Term: 7, Index: 1, Command: NewPutCommand([]byte("k"), []byte("v"))},
		{Term: 7, Index: 2, Command: NewDeleteCommand([]byte("k"))},
	}))
	st.Close()

	st2, err := OpenFileStorage(dir + "/raft.state")
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	hs, _ := st2.LoadHardState()
	if hs.CurrentTerm != 7 || hs.VotedFor != "b" {
		t.Fatalf("recovered hard state = %+v", hs)
	}
	if st2.LastIndex() != 2 {
		t.Fatalf("recovered last index = %d, want 2", st2.LastIndex())
	}
	es, err := st2.Entries(1, 2)
	if err != nil || len(es) != 2 {
		t.Fatalf("entries: %v %v", es, err)
	}
	cmd, err := DecodeCommand(es[0].Command)
	if err != nil || cmd.Kind != CmdPut || string(cmd.Key) != "k" || string(cmd.Value) != "v" {
		t.Fatalf("entry 1 decode: %+v %v", cmd, err)
	}
}

func TestFileStorageTruncateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/raft.state"
	st, err := OpenFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	must(t, st.Append([]Entry{
		{Term: 1, Index: 1, Command: NewPutCommand([]byte("a"), []byte("1"))},
		{Term: 1, Index: 2, Command: NewPutCommand([]byte("b"), []byte("2"))},
		{Term: 1, Index: 3, Command: NewPutCommand([]byte("c"), []byte("3"))},
	}))
	// A conflict overwrites index 2 onward with a different-term entry.
	must(t, st.TruncateSuffix(2))
	must(t, st.Append([]Entry{{Term: 2, Index: 2, Command: NewPutCommand([]byte("b"), []byte("x"))}}))
	st.Close()

	st2, err := OpenFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if st2.LastIndex() != 2 {
		t.Fatalf("after truncate+append+restart last index = %d, want 2", st2.LastIndex())
	}
	term, _ := st2.Term(2)
	if term != 2 {
		t.Fatalf("entry 2 term = %d, want 2 (the overwrite)", term)
	}
}

// ---- helpers ----

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func waitConverged(t *testing.T, c *cluster, wantKeys int, timeout time.Duration) {
	t.Helper()
	waitFn(t, timeout, func() bool {
		for _, id := range c.ids {
			if len(c.sms[id].snapshot()) != wantKeys {
				return false
			}
		}
		return true
	})
}

func waitFn(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
