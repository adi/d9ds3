package e2e

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// dataPlaneGet reads an object directly from a specific storage node's local
// backend (bypassing the gateway), to prove the log replicated it there.
func dataPlaneGet(addr, bucket, key string) (int, string) {
	resp, err := http.Get("http://" + addr + "/v1/object?bucket=" + bucket + "&key=" + key)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestReplication proves that one write through the gateway lands on every
// replica's local backend via the Raft log, and that a delete propagates too.
func TestReplication(t *testing.T) {
	h := startHarness(t, 3)
	ctx := context.Background()

	mkBucket(t, h.client, "repl")
	putText(t, h.client, "repl", "obj.txt", "replicated via raft")

	// Every replica must hold the object locally (allow brief async apply lag).
	deadline := time.Now().Add(10 * time.Second)
	for {
		allHave := true
		for _, addr := range h.addrs {
			code, body := dataPlaneGet(addr, "repl", "obj.txt")
			if code != 200 || body != "replicated via raft" {
				allHave = false
			}
		}
		if allHave {
			break
		}
		if time.Now().After(deadline) {
			for _, addr := range h.addrs {
				code, body := dataPlaneGet(addr, "repl", "obj.txt")
				t.Errorf("replica %s: code=%d body=%q", addr, code, body)
			}
			t.Fatal("object did not replicate to all nodes")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Delete through the gateway and confirm it propagates to every replica.
	if _, err := h.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("repl"), Key: aws.String("obj.txt")}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		allGone := true
		for _, addr := range h.addrs {
			if code, _ := dataPlaneGet(addr, "repl", "obj.txt"); code != 404 {
				allGone = false
			}
		}
		if allGone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delete did not propagate to all nodes")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestLeaderFailover kills the leader and verifies the cluster keeps serving
// writes and reads through the surviving quorum.
func TestLeaderFailover(t *testing.T) {
	h := startHarness(t, 3)
	ctx := context.Background()

	mkBucket(t, h.client, "fail")
	putText(t, h.client, "fail", "before.txt", "before")

	// Kill the current leader (node 0 bootstrapped and won the first election).
	h.nodes[0].Shutdown()

	// A new write must eventually succeed once a new leader is elected. The
	// gateway re-resolves the leader; retry across the election window.
	var wrote bool
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_, err := h.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("fail"), Key: aws.String("after.txt"), Body: strReader("after"),
		})
		if err == nil {
			wrote = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !wrote {
		t.Fatal("no write succeeded after leader failover")
	}
	if got := getText(t, h.client, "fail", "after.txt"); got != "after" {
		t.Fatalf("post-failover read: %q", got)
	}
	// Data written before the failure is still readable.
	if got := getText(t, h.client, "fail", "before.txt"); got != "before" {
		t.Fatalf("pre-failover data lost: %q", got)
	}
}
