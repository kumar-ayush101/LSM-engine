package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kumar-ayush101/LSM-engine/cluster"
	"github.com/kumar-ayush101/LSM-engine/server"
)

// clusterConfig holds the multi-node settings parsed from the environment.
// When nodeID is empty the server runs single-node (no replication).
type clusterConfig struct {
	nodeID   string            // this node's ID (LSM_NODE_ID)
	peers    map[string]string // other node ID -> raft peer URL (LSM_PEERS)
	raftAddr string            // internal listen address for raft RPCs (LSM_RAFT_ADDR)
	secret   string            // shared cluster secret (LSM_CLUSTER_SECRET)
	enabled  bool
}

// parseClusterConfig reads the cluster-related environment variables.
//
//	LSM_NODE_ID         this node's ID, e.g. "n1". Enables cluster mode.
//	LSM_RAFT_ADDR       internal listen address for peer RPCs, e.g. ":9001".
//	LSM_PEERS           comma-separated id=url pairs for the OTHER nodes,
//	                    e.g. "n2=http://10.0.0.2:9001,n3=http://10.0.0.3:9001".
//	LSM_CLUSTER_SECRET  shared secret authenticating peer RPCs (min 16 bytes).
func parseClusterConfig(getenv func(string) string) (clusterConfig, error) {
	id := strings.TrimSpace(getenv("LSM_NODE_ID"))
	if id == "" {
		return clusterConfig{enabled: false}, nil
	}
	cc := clusterConfig{nodeID: id, enabled: true, peers: map[string]string{}}
	cc.raftAddr = strings.TrimSpace(getenv("LSM_RAFT_ADDR"))
	if cc.raftAddr == "" {
		return cc, errors.New("LSM_RAFT_ADDR is required in cluster mode (e.g. :9001)")
	}
	cc.secret = getenv("LSM_CLUSTER_SECRET")
	if len(cc.secret) < 16 {
		return cc, errors.New("LSM_CLUSTER_SECRET must be at least 16 bytes in cluster mode")
	}
	peersRaw := strings.TrimSpace(getenv("LSM_PEERS"))
	if peersRaw == "" {
		return cc, errors.New("LSM_PEERS is required in cluster mode (id=url,id=url)")
	}
	for _, pair := range strings.Split(peersRaw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		pid, url, ok := strings.Cut(pair, "=")
		pid, url = strings.TrimSpace(pid), strings.TrimSpace(url)
		if !ok || pid == "" || url == "" {
			return cc, fmt.Errorf("LSM_PEERS entry %q is not id=url", pair)
		}
		if pid == id {
			return cc, fmt.Errorf("LSM_PEERS must not include this node's own ID %q", id)
		}
		cc.peers[pid] = url
	}
	if len(cc.peers) == 0 {
		return cc, errors.New("LSM_PEERS listed no peers")
	}
	return cc, nil
}

// replicator adapts *cluster.Node to server.Replicator: it translates the
// cluster's not-leader error to the server's sentinel and exposes Status as
// an opaque value.
type replicator struct{ n *cluster.Node }

func (r replicator) Put(key, value []byte) error { return translate(r.n.Put(key, value)) }
func (r replicator) Delete(key []byte) error     { return translate(r.n.Delete(key)) }
func (r replicator) IsLeader() bool              { return r.n.IsLeader() }
func (r replicator) Leader() (string, string)    { return r.n.Leader() }

func (r replicator) Status() server.ClusterStatus {
	s := r.n.Status()
	return server.ClusterStatus{
		ID:          s.ID,
		Role:        s.Role,
		Term:        s.Term,
		LeaderID:    s.LeaderID,
		CommitIndex: s.CommitIndex,
		LastApplied: s.LastApplied,
		LastLog:     s.LastLogIndex,
		Peers:       s.Peers,
	}
}

func translate(err error) error {
	if errors.Is(err, cluster.ErrNotLeader) {
		// Wrap the server sentinel so the handler maps it to a 421 redirect.
		return fmt.Errorf("%w", server.ErrNotLeader)
	}
	return err
}

// clusterTiming returns the raft timing used by the binary. The election
// timeout is comfortably larger than a typical cross-node RPC round trip so a
// healthy cluster does not re-elect under normal latency.
func clusterTiming() (heartbeat, electMin, electMax, rpcTimeout time.Duration) {
	return 100 * time.Millisecond, 600 * time.Millisecond, 1200 * time.Millisecond, 300 * time.Millisecond
}
