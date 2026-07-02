package e2e

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	awstypes "github.com/adi/d9ds3/internal/types"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// getStatus fetches a node's status; a down/unreachable node yields a zero Status.
func getStatus(t *testing.T, addr string) awstypes.Status {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/v1/status")
	if err != nil {
		return awstypes.Status{}
	}
	var s awstypes.Status
	decodeJSON(resp, &s)
	return s
}

func waitAllShardsLeader(t *testing.T, addrs []string, numShards int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		led := map[int]bool{}
		reachable := 0
		for _, a := range addrs {
			st := getStatus(t, a)
			if st.NumShards == numShards {
				reachable++
			}
			for _, sh := range st.Shards {
				if sh.IsLeader {
					led[sh.Shard] = true
				}
			}
		}
		if reachable == len(addrs) && len(led) == numShards {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("cluster not fully up (all nodes reachable + all shards led) in time")
}

// TestSharding runs a 4-shard, 3-node cluster: writes to many buckets (which hash
// across shards) succeed and replicate, and after rebalancing, shard leadership is
// distributed across multiple nodes (parallel write paths).
func TestSharding(t *testing.T) {
	const numShards = 4
	h := startHarnessOpts(t, 3, hopts{shards: numShards})
	waitAllShardsLeader(t, h.addrs, numShards)

	// Create buckets and objects; buckets hash across shards.
	buckets := []string{}
	for i := 0; i < 12; i++ {
		b := fmt.Sprintf("shard-bucket-%02d", i)
		buckets = append(buckets, b)
		mkBucket(t, h.client, b)
		putText(t, h.client, b, "obj.txt", fmt.Sprintf("data-%02d", i))
	}
	// Read every object back through the gateway.
	for i, b := range buckets {
		if got := getText(t, h.client, b, "obj.txt"); got != fmt.Sprintf("data-%02d", i) {
			t.Fatalf("bucket %s read: %q", b, got)
		}
	}

	// Every object must replicate to all 3 nodes' local backends.
	deadline := time.Now().Add(10 * time.Second)
	for {
		allHave := true
		for i, b := range buckets {
			for _, addr := range h.addrs {
				code, body := dataPlaneGet(addr, b, "obj.txt")
				if code != 200 || body != fmt.Sprintf("data-%02d", i) {
					allHave = false
				}
			}
		}
		if allHave {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("objects did not replicate to all nodes across shards")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Rebalance and verify shard leadership is spread across >1 node.
	h.rebalance()
	var leaderNodes map[string]bool
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		leaderNodes = map[string]bool{}
		total := 0
		for _, a := range h.addrs {
			st := getStatus(t, a)
			for _, sh := range st.Shards {
				if sh.IsLeader {
					leaderNodes[st.NodeID] = true
					total++
				}
			}
		}
		if total == numShards && len(leaderNodes) >= 2 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if len(leaderNodes) < 2 {
		t.Fatalf("shard leadership not distributed after rebalance: leaders on %v", leaderNodes)
	}
	t.Logf("shard leaders distributed across %d nodes", len(leaderNodes))

	// Writes still work after rebalance (leaders moved).
	nb := "shard-after-rebalance"
	mkBucket(t, h.client, nb)
	putText(t, h.client, nb, "k", "post-rebalance")
	if got := getText(t, h.client, nb, "k"); got != "post-rebalance" {
		t.Fatalf("post-rebalance write/read: %q", got)
	}
}

// TestShardingFailover kills a node in a sharded cluster and verifies writes to a
// bucket whose shard it may have led still succeed via re-election.
func TestShardingFailover(t *testing.T) {
	const numShards = 3
	h := startHarnessOpts(t, 3, hopts{shards: numShards})
	waitAllShardsLeader(t, h.addrs, numShards)
	h.rebalance()
	time.Sleep(1 * time.Second)

	for i := 0; i < 6; i++ {
		mkBucket(t, h.client, fmt.Sprintf("fo-%d", i))
	}
	// Kill one node; quorum (2/3) remains in every shard.
	h.nodes[1].Shutdown()

	// Writes to buckets across all shards must still succeed after re-election.
	deadline := time.Now().Add(25 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = nil
		for i := 0; i < 6; i++ {
			_, err := h.client.PutObject(h.ctx(), &s3.PutObjectInput{
				Bucket: aws.String(fmt.Sprintf("fo-%d", i)), Key: aws.String("k"), Body: strReader("v"),
			})
			if err != nil {
				lastErr = err
				break
			}
		}
		if lastErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("writes did not recover after node failure: %v", lastErr)
	}
	for i := 0; i < 6; i++ {
		if got := getText(t, h.client, fmt.Sprintf("fo-%d", i), "k"); got != "v" {
			t.Fatalf("read after failover fo-%d: %q", i, got)
		}
	}
}
