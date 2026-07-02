package e2e

import (
	"fmt"
	"testing"
	"time"
)

// TestSnapshotJoin forces the leader to snapshot and truncate its log, then joins
// a brand-new node. The joiner cannot replay the (truncated) early log, so it must
// reconstruct state from the installed snapshot — proving FSM Snapshot/Restore.
func TestSnapshotJoin(t *testing.T) {
	h := startHarnessOpts(t, 1, hopts{
		snapThreshold: 5,
		snapInterval:  300 * time.Millisecond,
		trailingLogs:  2,
	})
	mkBucket(t, h.client, "snap")

	const n = 40
	for i := 0; i < n; i++ {
		putText(t, h.client, "snap", fmt.Sprintf("obj-%03d", i), fmt.Sprintf("value-%03d", i))
	}
	// Let the leader take at least one snapshot and truncate its log.
	time.Sleep(2 * time.Second)

	// Join a fresh, empty node.
	joiner := h.addNode()

	// The joiner must end up with the EARLIEST object, which lives only in the
	// snapshot (its log entry was truncated away).
	deadline := time.Now().Add(20 * time.Second)
	for {
		code, body := dataPlaneGet(joiner, "snap", "obj-000")
		if code == 200 && body == "value-000" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("joiner never received snapshot: obj-000 code=%d body=%q", code, body)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Spot-check a few more across the range on the joiner.
	for _, i := range []int{7, 19, 39} {
		key := fmt.Sprintf("obj-%03d", i)
		code, body := dataPlaneGet(joiner, "snap", key)
		if code != 200 || body != fmt.Sprintf("value-%03d", i) {
			t.Fatalf("joiner missing %s: code=%d body=%q", key, code, body)
		}
	}
}
