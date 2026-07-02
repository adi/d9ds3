package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/adi/d9ds3/internal/cluster"
	"github.com/adi/d9ds3/internal/gateway"
	"github.com/adi/d9ds3/internal/s3api"
	"github.com/adi/d9ds3/internal/s3event"
	"github.com/adi/d9ds3/internal/storage"
	awstypes "github.com/adi/d9ds3/internal/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	rootAK = "admin"
	rootSK = "supersecretkey"
)

// hopts tunes cluster construction (snapshotting knobs; zero = Raft defaults).
type hopts struct {
	snapThreshold uint64
	snapInterval  time.Duration
	trailingLogs  uint64
	shards        int
	stagingTTL    time.Duration
}

type harness struct {
	t       *testing.T
	nodes   []*storage.Node
	addrs   []string         // data-plane HTTP addrs, index-aligned with nodes
	configs []storage.Config // index-aligned node configs, for restart
	opts    hopts

	ts     *httptest.Server
	client *s3.Client
	gw     *gateway.Gateway
	url    string
}

func startHarness(t *testing.T, numNodes int) *harness {
	return startHarnessOpts(t, numNodes, hopts{})
}

// startHarnessOpts brings up an n-node storage cluster + an in-process gateway and
// returns an AWS-SDK S3 client pointed at it.
func startHarnessOpts(t *testing.T, numNodes int, opts hopts) *harness {
	t.Helper()
	h := &harness{t: t, opts: opts}

	for i := 0; i < numNodes; i++ {
		h.makeNode(i == 0)
		if i == 0 {
			waitLeader(t, h.addrs[0])
		}
	}
	if numNodes > 1 {
		waitApplied(t, h.addrs)
	}

	gw, err := gateway.New(cluster.NewClient(h.addrs), t.TempDir(), s3event.New(""))
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	waitRoot(t, gw)
	h.gw = gw

	ts := httptest.NewServer(s3api.New(gw, "us-east-1").Handler())
	h.ts, h.url = ts, ts.URL
	h.client = s3.NewFromConfig(aws.Config{
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider(rootAK, rootSK, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
		o.UsePathStyle = true
	})

	t.Cleanup(func() {
		ts.Close()
		for _, n := range h.nodes {
			n.Shutdown()
		}
	})
	return h
}

// makeNode creates and starts one storage node, appending it to the harness. The
// first node bootstraps; later nodes join through node 0.
func (h *harness) makeNode(bootstrap bool) string {
	h.t.Helper()
	idx := len(h.nodes)
	shards := h.opts.shards
	if shards <= 0 {
		shards = 1
	}
	httpAddr := fmt.Sprintf("127.0.0.1:%d", freePort(h.t))
	cfg := storage.Config{
		NodeID:            fmt.Sprintf("n%d", idx+1),
		DataDir:           h.t.TempDir(),
		RaftBind:          fmt.Sprintf("127.0.0.1:%d", freePortBlock(h.t, shards)),
		HTTPBind:          httpAddr,
		Bootstrap:         bootstrap,
		Shards:            h.opts.shards,
		SnapshotThreshold: h.opts.snapThreshold,
		SnapshotInterval:  h.opts.snapInterval,
		TrailingLogs:      h.opts.trailingLogs,
		StagingTTL:        h.opts.stagingTTL,
	}
	if !bootstrap {
		cfg.JoinAddr = h.addrs[0]
	}
	// Every node lists the others as blob-pull peers.
	cfg.Peers = append([]string{}, h.addrs...)
	n, err := storage.NewNode(cfg)
	if err != nil {
		h.t.Fatalf("NewNode %d: %v", idx, err)
	}
	h.nodes = append(h.nodes, n)
	h.addrs = append(h.addrs, httpAddr)
	h.configs = append(h.configs, cfg)
	go n.Start()
	return httpAddr
}

// addNode joins a brand-new node to a running cluster and returns its HTTP addr.
func (h *harness) addNode() string {
	return h.makeNode(false)
}

// stopNode shuts a node down (simulating a crash), keeping its data dir + ports.
func (h *harness) stopNode(idx int) {
	h.nodes[idx].Shutdown()
}

// restartNode brings a previously stopped node back up on its original data dir
// and ports. It rejoins the cluster from its persisted Raft state (no bootstrap,
// no re-join) and catches up via log replay / snapshot install.
func (h *harness) restartNode(idx int) {
	cfg := h.configs[idx]
	cfg.Bootstrap = false
	cfg.JoinAddr = ""
	n, err := storage.NewNode(cfg)
	if err != nil {
		h.t.Fatalf("restart node %d: %v", idx, err)
	}
	h.nodes[idx] = n
	go n.Start()
}

func (h *harness) ctx() context.Context { return context.Background() }

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// freePortBlock finds a base port whose next `count` ports are all bindable, so a
// node's per-shard Raft transports (base+i) don't collide.
func freePortBlock(t *testing.T, count int) int {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		base := freePort(t)
		var ls []net.Listener
		ok := true
		for i := 0; i < count; i++ {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
			if err != nil {
				ok = false
				break
			}
			ls = append(ls, l)
		}
		for _, l := range ls {
			l.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatalf("could not find a free port block of %d", count)
	return 0
}

// rebalance asks the bootstrap node to spread shard leadership across the cluster.
func (h *harness) rebalance() {
	http.Post("http://"+h.addrs[0]+"/v1/rebalance", "application/json", nil)
}

func othersExcept(addrs []string, i int) []string {
	var out []string
	for j, a := range addrs {
		if j != i {
			out = append(out, a)
		}
	}
	return out
}

func waitLeader(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/v1/status")
		if err == nil {
			var s awstypes.Status
			decodeJSON(resp, &s)
			if s.IsLeader {
				return
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatal("no leader elected in time")
}

func waitApplied(t *testing.T, addrs []string) {
	t.Helper()
	// Give followers a moment to be added as voters and catch up.
	time.Sleep(2 * time.Second)
	_ = addrs
}

func waitRoot(t *testing.T, gw *gateway.Gateway) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := gw.PutAccount(gateway.Ctx{Account: rootAK},
			awstypes.Account{AccessKeyID: rootAK, SecretKey: rootSK, Role: "admin"}); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("could not bootstrap root account")
}

func decodeJSON(resp *http.Response, v any) {
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(v)
}
