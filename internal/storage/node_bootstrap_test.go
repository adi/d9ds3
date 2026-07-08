package storage

import (
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitLeader(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, id := n.shards[0].raft.LeaderWithID(); id != "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected within 5s")
}

// TestBootstrapRegistersDNSNotIP proves the two Raft-identity invariants:
//  1. bootstrap registers self by the --raft-advertise string (here a hostname),
//     never the transport's resolved IP — so a pod-IP change can't orphan the node.
//  2. --bootstrap/--join are first-start-only: a restart on the same state dirs
//     recovers from the persisted log (fresh=false, no join) and keeps membership.
func TestBootstrapRegistersDNSNotIP(t *testing.T) {
	raftAddr := freePort(t) // 127.0.0.1:NNNNN
	_, port, _ := net.SplitHostPort(raftAddr)
	advertise := net.JoinHostPort("localhost", port) // resolves to 127.0.0.1, differs textually
	data, state := t.TempDir(), t.TempDir()

	cfg := Config{
		NodeID: "n0", DataDir: data, StateDir: state,
		RaftBind: raftAddr, RaftAdvertise: advertise,
		HTTPBind: freePort(t), Bootstrap: true, Shards: 1,
	}

	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if !n.shards[0].fresh {
		t.Fatal("first start should be fresh")
	}
	waitLeader(t, n)

	assertSelfAddr := func(when string, node *Node) {
		fut := node.shards[0].raft.GetConfiguration()
		if err := fut.Error(); err != nil {
			t.Fatalf("%s: GetConfiguration: %v", when, err)
		}
		servers := fut.Configuration().Servers
		if len(servers) != 1 {
			t.Fatalf("%s: want 1 server, got %d", when, len(servers))
		}
		if got := servers[0].Address; got != raft.ServerAddress(advertise) {
			t.Fatalf("%s: registered address %q, want the advertise name %q (not the resolved IP)", when, got, advertise)
		}
	}
	assertSelfAddr("after bootstrap", n)
	n.Shutdown()

	// Restart on the SAME dirs, still passing --bootstrap and a --join addr.
	// Both must be ignored because persisted state exists.
	cfg.JoinAddr = "127.0.0.1:1" // would fail loudly if a join were attempted
	n2, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("restart NewNode: %v", err)
	}
	defer n2.Shutdown()
	if n2.shards[0].fresh {
		t.Fatal("restart must NOT be fresh (state persisted)")
	}
	if n2.needsJoin() {
		t.Fatal("restart must NOT need a join")
	}
	waitLeader(t, n2)
	assertSelfAddr("after restart", n2) // membership recovered, address intact
}
