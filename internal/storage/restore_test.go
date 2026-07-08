package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/types"
)

func mkBackend(t *testing.T) *posixBackend {
	t.Helper()
	b, err := newPosixBackend(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("newPosixBackend: %v", err)
	}
	return b
}

func nowMeta() map[string]string {
	return map[string]string{command.MetaMTime: time.Now().UTC().Format(time.RFC3339Nano)}
}

func createBucket(t *testing.T, b *posixBackend, bucket string) {
	t.Helper()
	if err := b.applyCreateBucket(&command.Command{Op: command.OpCreateBucket, Bucket: bucket, Meta: nowMeta()}); err != nil {
		t.Fatalf("createBucket %s: %v", bucket, err)
	}
}

// putReplicated writes an object through the normal (log-applied) path.
func putReplicated(t *testing.T, b *posixBackend, bucket, key, content, token string) {
	t.Helper()
	if err := os.WriteFile(b.stagingPath(token), []byte(content), 0o644); err != nil {
		t.Fatalf("stage: %v", err)
	}
	c := &command.Command{
		Op: command.OpPutObject, Bucket: bucket, Key: key, BlobToken: token,
		VersionID: "ver-" + token, Size: int64(len(content)), ETag: `"e"`, Meta: nowMeta(),
	}
	if err := b.applyPutObject(c); err != nil {
		t.Fatalf("putReplicated %s/%s: %v", bucket, key, err)
	}
}

// TestApplyPutWithoutBucketMeta simulates a committed PUT arriving on a replica
// that has no metadata (and no directory) for the bucket — e.g. a follower
// materializing the write, or an inherited/asymmetric disk. The apply must not
// reject (the gateway already validated the bucket); it must create and serve it,
// so all replicas converge.
func TestApplyPutWithoutBucketMeta(t *testing.T) {
	b := mkBackend(t)
	// NOTE: no createBucket — the bucket has no sidecar and no directory here.
	if err := os.WriteFile(b.stagingPath("tok"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("stage: %v", err)
	}
	c := &command.Command{
		Op: command.OpPutObject, Bucket: "inherited", Key: "dir/obj.txt", BlobToken: "tok",
		VersionID: "v1", Size: 7, ETag: `"e"`, Meta: nowMeta(),
	}
	if err := b.applyPutObject(c); err != nil {
		t.Fatalf("apply put must tolerate a missing bucket, got: %v", err)
	}
	// The object is now a real file and is served through the normal read path.
	if _, err := os.Stat(filepath.Join(b.root, "inherited", "dir", "obj.txt")); err != nil {
		t.Fatalf("object file not written: %v", err)
	}
	res, err := b.GetObject("inherited", "dir/obj.txt", types.GetOptions{})
	if err != nil {
		t.Fatalf("GetObject after implicit-bucket put: %v", err)
	}
	res.Body.Close()
}

// TestRestorePreservesPrefilledData reproduces the snapshot-install data-loss bug:
// a node holding a prefilled file receives a snapshot that does NOT contain it.
// The prefilled file must survive; a replicated object deleted upstream must go.
func TestRestorePreservesPrefilledData(t *testing.T) {
	// Node under test: has a prefilled file + two replicated objects.
	b := mkBackend(t)
	createBucket(t, b, "buk")

	prefill := filepath.Join(b.root, "buk", "operator", "important.dat")
	os.MkdirAll(filepath.Dir(prefill), 0o755)
	if err := os.WriteFile(prefill, []byte("OPERATOR DATA - do not delete"), 0o644); err != nil {
		t.Fatalf("prefill: %v", err)
	}
	putReplicated(t, b, "buk", "keep.txt", "old-keep", "tok-keep")
	putReplicated(t, b, "buk", "gone.txt", "will-be-deleted-upstream", "tok-gone")

	// Leader snapshot: same bucket, keep.txt with NEW content, no gone.txt, no prefill.
	leader := mkBackend(t)
	createBucket(t, leader, "buk")
	putReplicated(t, leader, "buk", "keep.txt", "new-keep", "tok-keep2")
	var snap bytes.Buffer
	if err := leader.snapshotTo(&snap); err != nil {
		t.Fatalf("snapshotTo: %v", err)
	}

	// Install the leader snapshot onto our node.
	if err := b.restoreFrom(bytes.NewReader(snap.Bytes())); err != nil {
		t.Fatalf("restoreFrom: %v", err)
	}

	// 1. Prefilled operator file MUST survive an InstallSnapshot.
	if got, err := os.ReadFile(prefill); err != nil || string(got) != "OPERATOR DATA - do not delete" {
		t.Fatalf("prefilled file lost/altered by restore: err=%v got=%q", err, got)
	}
	// 2. Replicated object present in the snapshot is reconciled to the new content.
	keep := filepath.Join(b.root, "buk", "keep.txt")
	if got, err := os.ReadFile(keep); err != nil || string(got) != "new-keep" {
		t.Fatalf("replicated key not reconciled: err=%v got=%q", err, got)
	}
	// 3. Replicated object deleted upstream (absent from snapshot) is removed.
	if _, err := os.Stat(filepath.Join(b.root, "buk", "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("replicated-then-deleted key should be gone, stat err=%v", err)
	}
	// 4. It's still readable through the normal API (prefill synthesized).
	if _, err := b.GetObject("buk", "operator/important.dat", types.GetOptions{}); err != nil {
		t.Fatalf("prefilled object not served after restore: %v", err)
	}
}
