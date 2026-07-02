package storage

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/types"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Config configures a storage node.
type Config struct {
	NodeID  string // unique id, also the Raft ServerID
	DataDir string // object data root (vstore/keys/buckets/mpu/iam/staging) — the backup/rsync surface
	// RaftDir holds node-local Raft consensus state (log/stable BoltDB + snapshots).
	// It is deliberately SEPARATE from DataDir: it must never be copied between nodes
	// or restored independently. If empty, defaults to "<DataDir>-raft" (a sibling,
	// never nested inside DataDir).
	RaftDir       string
	RaftBind      string   // base host:port for Raft transports (shard i uses port+i)
	RaftAdvertise string   // advertise base addr (defaults to RaftBind)
	HTTPBind      string   // host:port for the data-plane HTTP server
	Bootstrap     bool     // bootstrap fresh single-node groups
	JoinAddr      string   // data-plane HTTP addr of a node to join through
	Peers         []string // data-plane HTTP addrs of peers, for blob-pull fallback
	Shards        int      // number of Raft groups (buckets hash to a shard); default 1

	// Snapshot tuning (0 = Raft defaults). Lowering these forces frequent
	// snapshotting + log truncation (used by tests and small deployments).
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
	TrailingLogs      uint64

	// StagingTTL bounds how long an unclaimed fan-out payload lives in staging
	// before the sweeper reclaims it (0 = DefaultStagingTTL).
	StagingTTL time.Duration
}

// shard is one Raft group. All nodes are members of all shards; a bucket is owned
// by exactly one shard (by hash), so writes to different shards commit through
// different leaders in parallel.
type shard struct {
	id          int
	raft        *raft.Raft
	fsm         *fsm
	transport   *raft.NetworkTransport
	advertise   raft.ServerAddress
	logStore    *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
}

// Node is one storage replica: a member of every Raft shard whose FSMs apply the
// log to a shared local posix backend, plus a data-plane HTTP server.
type Node struct {
	cfg     Config
	backend *posixBackend
	shards  []*shard
	http    *http.Server
	gcStop  chan struct{}
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.RaftAdvertise == "" {
		cfg.RaftAdvertise = cfg.RaftBind
	}
	if cfg.Shards <= 0 {
		cfg.Shards = 1
	}
	if cfg.RaftDir == "" {
		// Sibling of DataDir, never nested inside it, so backing up / rsync-ing the
		// object data can't touch Raft consensus state.
		cfg.RaftDir = filepath.Clean(cfg.DataDir) + "-raft"
	}
	backend, err := newPosixBackend(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, backend: backend, gcStop: make(chan struct{})}
	for i := 0; i < cfg.Shards; i++ {
		sh, err := n.startShard(i)
		if err != nil {
			return nil, err
		}
		n.shards = append(n.shards, sh)
	}
	n.http = &http.Server{Addr: cfg.HTTPBind, Handler: n.routes()}
	return n, nil
}

func (n *Node) numShards() int { return n.cfg.Shards }

// shardOf maps a bucket to its owning shard. Empty bucket (cluster-wide ops such
// as IAM) maps to the root shard 0.
func (n *Node) shardOf(bucket string) int {
	if bucket == "" || n.cfg.Shards == 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(bucket))
	return int(h.Sum32() % uint32(n.cfg.Shards))
}

func shardAddr(base string, shardIdx int) (string, error) {
	host, portStr, err := net.SplitHostPort(base)
	if err != nil {
		return "", err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(port+shardIdx)), nil
}

func (n *Node) startShard(i int) (*shard, error) {
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(n.cfg.NodeID)
	if n.cfg.SnapshotThreshold > 0 {
		rc.SnapshotThreshold = n.cfg.SnapshotThreshold
	}
	if n.cfg.SnapshotInterval > 0 {
		rc.SnapshotInterval = n.cfg.SnapshotInterval
	}
	if n.cfg.TrailingLogs > 0 {
		rc.TrailingLogs = n.cfg.TrailingLogs
	}

	bindAddr, err := shardAddr(n.cfg.RaftBind, i)
	if err != nil {
		return nil, fmt.Errorf("shard %d bind addr: %w", i, err)
	}
	advAddr, err := shardAddr(n.cfg.RaftAdvertise, i)
	if err != nil {
		return nil, fmt.Errorf("shard %d advertise addr: %w", i, err)
	}
	advertise, err := net.ResolveTCPAddr("tcp", advAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve advertise: %w", err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("shard %d transport: %w", i, err)
	}

	dir := filepath.Join(n.cfg.RaftDir, fmt.Sprintf("shard-%d", i))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dir, "log.bolt"))
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dir, "stable.bolt"))
	if err != nil {
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(dir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	f := newFSM(n.backend)
	f.fetchBlob = n.pullBlob
	r, err := raft.NewRaft(rc, f, logStore, stableStore, snaps, transport)
	if err != nil {
		return nil, fmt.Errorf("shard %d raft: %w", i, err)
	}
	if n.cfg.Bootstrap {
		r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{ID: rc.LocalID, Address: transport.LocalAddr()}}})
	}
	return &shard{
		id: i, raft: r, fsm: f, transport: transport, advertise: transport.LocalAddr(),
		logStore: logStore, stableStore: stableStore,
	}, nil
}

// Start serves the data-plane HTTP API and, if configured, joins a cluster.
func (n *Node) Start() error {
	if n.cfg.JoinAddr != "" {
		if err := n.join(); err != nil {
			return fmt.Errorf("join cluster: %w", err)
		}
	}
	go n.runStagingGC()
	return n.http.ListenAndServe()
}

// runStagingGC periodically reclaims orphaned fan-out payloads.
func (n *Node) runStagingGC() {
	ttl := n.cfg.StagingTTL
	if ttl <= 0 {
		ttl = DefaultStagingTTL
	}
	interval := ttl / 2
	if interval < time.Second {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-n.gcStop:
			return
		case <-t.C:
			n.backend.sweepStaging(ttl, time.Now())
		}
	}
}

// Shutdown stops the data-plane server, the sweeper, and every Raft shard.
func (n *Node) Shutdown() error {
	select {
	case <-n.gcStop:
	default:
		close(n.gcStop)
	}
	if n.http != nil {
		n.http.Close()
	}
	for _, sh := range n.shards {
		sh.raft.Shutdown().Error()
		// Close the BoltDB stores so their file locks release — otherwise a
		// restart on the same data dir would block on the bbolt lock.
		if sh.logStore != nil {
			sh.logStore.Close()
		}
		if sh.stableStore != nil {
			sh.stableStore.Close()
		}
	}
	return nil
}

// join asks the bootstrap node to add this node as a voter in every shard,
// retrying until it leads all shards (a freshly bootstrapped node may still be
// winning per-shard elections).
func (n *Node) join() error {
	body, _ := json.Marshal(map[string]any{
		"id":         n.cfg.NodeID,
		"raft_addr":  n.cfg.RaftAdvertise,
		"num_shards": n.cfg.Shards,
	})
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		resp, err := http.Post("http://"+n.cfg.JoinAddr+"/v1/join", "application/json", bytesReader(body))
		if err != nil {
			last = err
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		msg, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		last = fmt.Errorf("join rejected (%d): %s", resp.StatusCode, msg)
		time.Sleep(300 * time.Millisecond)
	}
	return last
}

// ---- command submission ----

// submitTo appends a raw command to the given shard. Valid only on that shard's
// leader (else raft.ErrNotLeader). Returns the committed index.
func (n *Node) submitTo(shardIdx int, raw []byte) (uint64, error) {
	if shardIdx < 0 || shardIdx >= len(n.shards) {
		return 0, fmt.Errorf("shard %d out of range", shardIdx)
	}
	f := n.shards[shardIdx].raft.Apply(raw, 10*time.Second)
	if err := f.Error(); err != nil {
		return 0, err
	}
	if resp := f.Response(); resp != nil {
		if e, ok := resp.(error); ok && e != nil {
			return 0, e
		}
	}
	return f.Index(), nil
}

// ready reports whether this node is a functioning member: every shard it belongs
// to currently sees a leader (so it is connected to a quorum, not un-joined or
// stuck mid-election). Peer discovery is unaffected — the headless Service uses
// publishNotReadyAddresses, and the gateway addresses pods by DNS directly.
func (n *Node) ready() bool {
	for _, sh := range n.shards {
		if addr, _ := sh.raft.LeaderWithID(); addr == "" {
			return false
		}
	}
	return len(n.shards) > 0
}

func (n *Node) status() types.Status {
	st := types.Status{NodeID: n.cfg.NodeID, NumShards: n.cfg.Shards}
	for _, sh := range n.shards {
		_, leaderID := sh.raft.LeaderWithID()
		leads := leaderID == raft.ServerID(n.cfg.NodeID)
		st.Shards = append(st.Shards, types.ShardStatus{
			Shard: sh.id, IsLeader: leads, AppliedIndex: sh.fsm.AppliedIndex(),
		})
		if sh.id == 0 {
			st.IsLeader = leads
		}
	}
	return st
}

// rebalance spreads shard leadership across the cluster: for each shard this node
// currently leads, transfer leadership to the member at index (shard % members).
// This turns co-located leaders into distributed ones so writes to different
// buckets commit through different nodes.
func (n *Node) rebalance() {
	for _, sh := range n.shards {
		if _, leaderID := sh.raft.LeaderWithID(); leaderID != raft.ServerID(n.cfg.NodeID) {
			continue
		}
		cfgFuture := sh.raft.GetConfiguration()
		if cfgFuture.Error() != nil {
			continue
		}
		servers := cfgFuture.Configuration().Servers
		if len(servers) < 2 {
			continue
		}
		target := servers[sh.id%len(servers)]
		if target.ID == raft.ServerID(n.cfg.NodeID) {
			continue
		}
		sh.raft.LeadershipTransferToServer(target.ID, target.Address)
	}
}

// pullBlob recovers payload this node missed during fan-out by fetching it from a
// peer. Called by the FSM when Apply reports ErrBlobMissing.
func (n *Node) pullBlob(c *command.Command) error {
	switch c.Op {
	case command.OpCompleteMultipart:
		for _, p := range c.Parts {
			dst := n.backend.mpPartPath(c.UploadID, p.PartNumber)
			if _, err := os.Stat(dst); err == nil {
				continue
			}
			if err := n.pullFromPeers(dst, fmt.Sprintf("/v1/mppart/%s/%d", c.UploadID, p.PartNumber)); err != nil {
				return err
			}
		}
		return nil
	default: // OpPutObject: pull the committed object bytes for this version.
		dst := n.backend.stagingPath(c.BlobToken)
		version := ""
		if bm, err := n.backend.readBucketMeta(c.Bucket); err == nil && bm.VersioningEnabled() {
			version = c.VersionID
		}
		return n.pullFromPeers(dst, fmt.Sprintf("/v1/object?bucket=%s&key=%s&version=%s",
			urlq(c.Bucket), urlq(c.Key), urlq(version)))
	}
}

func (n *Node) pullFromPeers(dst, pathAndQuery string) error {
	for _, peer := range n.cfg.Peers {
		resp, err := http.Get("http://" + peer + pathAndQuery)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}
		if err := os.MkdirAll(pathDir(dst), 0o755); err != nil {
			resp.Body.Close()
			return err
		}
		out, err := os.Create(dst)
		if err != nil {
			resp.Body.Close()
			return err
		}
		_, err = io.Copy(out, resp.Body)
		out.Close()
		resp.Body.Close()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("could not pull %s from any peer", pathAndQuery)
}
