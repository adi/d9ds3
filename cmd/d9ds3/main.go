// Command d9ds3 runs either a storage node (Raft peer + local backend + data
// plane) or a stateless S3 gateway.
//
//	d9ds3 storage --id n1 --data ./data1 --raft 127.0.0.1:9001 --http 127.0.0.1:8001 --bootstrap
//	d9ds3 gateway --s3 :8080 --nodes 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 \
//	              --root-access-key admin --root-secret secret
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/adi/d9ds3/internal/cluster"
	"github.com/adi/d9ds3/internal/gateway"
	"github.com/adi/d9ds3/internal/s3api"
	"github.com/adi/d9ds3/internal/s3event"
	"github.com/adi/d9ds3/internal/storage"
	"github.com/adi/d9ds3/internal/types"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "storage":
		runStorage(os.Args[2:])
	case "gateway":
		runGateway(os.Args[2:])
	case "standalone":
		runStandalone(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: d9ds3 <standalone|storage|gateway> [flags]")
	os.Exit(2)
}

func runStorage(args []string) {
	fs := flag.NewFlagSet("storage", flag.ExitOnError)
	id := fs.String("id", "", "unique node id (Raft ServerID)")
	data := fs.String("data", "", "object data directory (the backup/rsync surface)")
	raftDir := fs.String("raft-dir", "", "Raft consensus state dir — node-local, keep on its own volume (default: <data>-raft)")
	raftBind := fs.String("raft", "127.0.0.1:9001", "Raft transport bind addr")
	raftAdv := fs.String("raft-advertise", "", "Raft advertise addr (defaults to --raft)")
	httpBind := fs.String("http", "127.0.0.1:8001", "data-plane HTTP bind addr")
	bootstrap := fs.Bool("bootstrap", false, "bootstrap a fresh single-node cluster")
	join := fs.String("join", "", "data-plane HTTP addr of a node to join through")
	peers := fs.String("peers", "", "comma-separated peer HTTP addrs (blob-pull fallback)")
	shards := fs.Int("shards", 1, "number of Raft shards (buckets hash to a shard); base raft port + shard index")
	stagingTTL := fs.Duration("staging-ttl", 0, "how long unclaimed fan-out payloads live in staging before GC (0 = 15m default)")
	_ = fs.Parse(args)

	if *id == "" || *data == "" {
		log.Fatal("storage: --id and --data are required")
	}
	node, err := storage.NewNode(storage.Config{
		NodeID: *id, DataDir: *data, RaftDir: *raftDir, RaftBind: *raftBind, RaftAdvertise: *raftAdv,
		HTTPBind: *httpBind, Bootstrap: *bootstrap, JoinAddr: *join, Peers: splitCSV(*peers),
		Shards: *shards, StagingTTL: *stagingTTL,
	})
	if err != nil {
		log.Fatalf("storage: %v", err)
	}
	log.Printf("storage node %s: raft=%s http=%s bootstrap=%v", *id, *raftBind, *httpBind, *bootstrap)
	if err := node.Start(); err != nil {
		log.Fatalf("storage: %v", err)
	}
}

func runGateway(args []string) {
	fs := flag.NewFlagSet("gateway", flag.ExitOnError)
	s3addr := fs.String("s3", ":8080", "S3 API listen addr")
	nodes := fs.String("nodes", "", "comma-separated storage-node HTTP addrs")
	stage := fs.String("stage", "", "scratch dir for buffering uploads (default: os temp)")
	region := fs.String("region", "us-east-1", "S3 signing region")
	events := fs.String("events", "", "event-notification target (http(s)://... or file:///...)")
	rootAK := fs.String("root-access-key", "", "bootstrap root (admin) access key")
	rootSK := fs.String("root-secret", "", "bootstrap root (admin) secret key")
	_ = fs.Parse(args)

	nodeList := splitCSV(*nodes)
	if len(nodeList) == 0 {
		log.Fatal("gateway: --nodes is required")
	}
	stageDir := *stage
	if stageDir == "" {
		stageDir = os.TempDir() + "/d9ds3-gateway-stage"
	}
	cl := cluster.NewClient(nodeList)
	notifier := s3event.New(*events)
	gw, err := gateway.New(cl, stageDir, notifier)
	if err != nil {
		log.Fatalf("gateway: %v", err)
	}
	if *rootAK != "" && *rootSK != "" {
		go bootstrapRoot(gw, *rootAK, *rootSK)
	}
	srv := s3api.New(gw, *region)
	// Readiness reflects account-ready: don't accept traffic until the root account
	// exists (which also implies the cluster leader is reachable).
	if *rootAK != "" {
		ak := *rootAK
		srv.SetReadyFunc(func() bool { return gw.AccountExists(ak) })
	}
	log.Printf("gateway: s3=%s region=%s nodes=%v", *s3addr, *region, nodeList)
	if err := srv.Listen(*s3addr); err != nil {
		log.Fatalf("gateway: %v", err)
	}
}

// runStandalone runs a single-node storage cluster and the gateway in one process
// — the simplest way to get an S3 endpoint on one machine/pod (no clustering).
func runStandalone(args []string) {
	fs := flag.NewFlagSet("standalone", flag.ExitOnError)
	s3addr := fs.String("s3", ":8080", "S3 API listen addr")
	data := fs.String("data", "", "object data directory")
	raftDir := fs.String("raft-dir", "", "Raft state dir (default: <data>-raft)")
	region := fs.String("region", "us-east-1", "S3 signing region")
	events := fs.String("events", "", "event-notification target (http(s)://... or file:///...)")
	rootAK := fs.String("root-access-key", "", "bootstrap root (admin) access key")
	rootSK := fs.String("root-secret", "", "bootstrap root (admin) secret key")
	stage := fs.String("stage", "", "scratch dir for buffering uploads (default: os temp)")
	_ = fs.Parse(args)

	if *data == "" {
		log.Fatal("standalone: --data is required")
	}
	// Internal, loopback-only storage node (single-node bootstrap, one shard).
	node, err := storage.NewNode(storage.Config{
		NodeID: "standalone", DataDir: *data, RaftDir: *raftDir,
		RaftBind: "127.0.0.1:9001", HTTPBind: "127.0.0.1:8001",
		Bootstrap: true, Shards: 1,
	})
	if err != nil {
		log.Fatalf("standalone: %v", err)
	}
	go func() {
		if err := node.Start(); err != nil {
			log.Fatalf("standalone storage: %v", err)
		}
	}()

	stageDir := *stage
	if stageDir == "" {
		stageDir = os.TempDir() + "/d9ds3-standalone-stage"
	}
	cl := cluster.NewClient([]string{"127.0.0.1:8001"})
	gw, err := gateway.New(cl, stageDir, s3event.New(*events))
	if err != nil {
		log.Fatalf("standalone: %v", err)
	}
	if *rootAK != "" && *rootSK != "" {
		go bootstrapRoot(gw, *rootAK, *rootSK)
	}
	srv := s3api.New(gw, *region)
	if *rootAK != "" {
		ak := *rootAK
		srv.SetReadyFunc(func() bool { return gw.AccountExists(ak) })
	}
	log.Printf("standalone: s3=%s region=%s data=%s", *s3addr, *region, *data)
	if err := srv.Listen(*s3addr); err != nil {
		log.Fatalf("standalone: %v", err)
	}
}

// bootstrapRoot ensures the root admin account exists, retrying until the Raft
// leader is reachable. Idempotent (PutAccount overwrites).
func bootstrapRoot(gw *gateway.Gateway, ak, sk string) {
	acct := types.Account{AccessKeyID: ak, SecretKey: sk, Role: "admin"}
	for i := 0; i < 60; i++ {
		if _, err := gw.PutAccount(gateway.Ctx{Account: ak}, acct); err == nil {
			log.Printf("gateway: root admin account %q ready", ak)
			return
		}
		time.Sleep(time.Second)
	}
	log.Printf("gateway: WARNING could not bootstrap root account after retries")
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
