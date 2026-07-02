package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDownNodeRecovery kills a follower, writes many objects while it is down (so
// it misses the fan-out entirely), then brings it back and verifies it recovers
// every object — via Raft log replay plus the peer-pull fallback for the payloads.
func TestDownNodeRecovery(t *testing.T) {
	h := startHarnessOpts(t, 3, hopts{})

	mkBucket(t, h.client, "recov")

	// Kill a follower (node 0 bootstrapped and leads shard 0).
	h.stopNode(2)
	time.Sleep(500 * time.Millisecond)

	// Write while node 2 is down: fan-out reaches nodes 0 and 1 (quorum), commits.
	const n = 15
	for i := 0; i < n; i++ {
		putText(t, h.client, "recov", fmt.Sprintf("obj-%02d", i), fmt.Sprintf("val-%02d", i))
	}
	// Data is readable from the surviving quorum.
	if got := getText(t, h.client, "recov", "obj-00"); got != "val-00" {
		t.Fatalf("read during outage: %q", got)
	}

	// Bring node 2 back; it must catch up and recover every payload.
	h.restartNode(2)
	joiner := h.addrs[2]

	deadline := time.Now().Add(30 * time.Second)
	for {
		missing := 0
		for i := 0; i < n; i++ {
			code, body := dataPlaneGet(joiner, "recov", fmt.Sprintf("obj-%02d", i))
			if code != 200 || body != fmt.Sprintf("val-%02d", i) {
				missing++
			}
		}
		if missing == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovered node still missing %d/%d objects", missing, n)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// After recovery, staging on every node should be drained (payloads were
	// renamed into vstore on apply, or pulled-then-renamed on the recovered node).
	for i := 0; i < 3; i++ {
		assertStagingEmpty(t, h.configs[i].DataDir)
	}
}

// TestStagingGC verifies the sweeper reclaims an orphaned fan-out payload (one
// whose operation never committed).
func TestStagingGC(t *testing.T) {
	h := startHarnessOpts(t, 1, hopts{stagingTTL: 2 * time.Second})
	stagingDir := filepath.Join(h.configs[0].DataDir, ".d9", "staging")

	// Simulate an orphan: a staged payload that never got a committed command.
	orphan := filepath.Join(stagingDir, "orphan-token")
	if err := os.WriteFile(orphan, []byte("dangling"), 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	// Backdate it so it is already older than the TTL.
	old := time.Now().Add(-time.Minute)
	os.Chtimes(orphan, old, old)

	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(orphan); os.IsNotExist(err) {
			return // swept
		}
		if time.Now().After(deadline) {
			t.Fatal("sweeper did not reclaim the orphaned staging payload")
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func assertStagingEmpty(t *testing.T, dataDir string) {
	t.Helper()
	for _, d := range []string{"staging", "mpstaging"} {
		entries, err := os.ReadDir(filepath.Join(dataDir, ".d9", d))
		if err != nil {
			continue
		}
		if len(entries) != 0 {
			names := []string{}
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Fatalf("%s/%s not drained: %v", dataDir, d, names)
		}
	}
}
