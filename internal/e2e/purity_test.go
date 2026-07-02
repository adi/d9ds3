package e2e

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/xattr"
)

// TestDataVolumePurity verifies that after real S3 operations the --data volume
// contains ONLY the browsable object files — no sidecars, no internal directories —
// and that object metadata rides in xattrs on the files.
func TestDataVolumePurity(t *testing.T) {
	h := startHarness(t, 1)
	ctx := context.Background()
	dataDir := h.configs[0].DataDir

	mkBucket(t, h.client, "pure")
	// Put an object with content-type, user metadata, and tags.
	if _, err := h.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("pure"), Key: aws.String("a/b/c.json"),
		Body: bytes.NewReader([]byte(`{"hi":1}`)), ContentType: aws.String("application/json"),
		Metadata: map[string]string{"team": "storage"}, Tagging: aws.String("env=prod"),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	// Set an ACL too (more metadata that must NOT become a file).
	if _, err := h.client.PutObjectAcl(ctx, &s3.PutObjectAclInput{
		Bucket: aws.String("pure"), Key: aws.String("a/b/c.json"), ACL: s3types.ObjectCannedACLPrivate,
	}); err != nil {
		t.Fatalf("PutObjectAcl: %v", err)
	}

	// 1. The data volume contains ONLY object files (no *.json sidecars, no dot-dirs).
	objFile := filepath.Join(dataDir, "pure", "a", "b", "c.json")
	var regularFiles []string
	filepath.WalkDir(dataDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		base := d.Name()
		if strings.HasPrefix(base, ".") && base != filepath.Base(dataDir) {
			t.Fatalf("data volume polluted with dot-entry: %s", p)
		}
		if !d.IsDir() {
			regularFiles = append(regularFiles, p)
		}
		return nil
	})
	if len(regularFiles) != 1 || regularFiles[0] != objFile {
		t.Fatalf("data volume should hold only the object file, got: %v", regularFiles)
	}

	// 2. The object bytes are exactly what we put.
	if b, _ := os.ReadFile(objFile); string(b) != `{"hi":1}` {
		t.Fatalf("object bytes on disk: %q", b)
	}

	// 3. Metadata lives in xattrs on the file (versitygw-style).
	ct, err := xattr.LGet(objFile, "user.d9ds3.content-type")
	if err != nil || string(ct) != "application/json" {
		t.Fatalf("content-type xattr: err=%v val=%q", err, ct)
	}
	if um, err := xattr.LGet(objFile, "user.d9ds3.usermeta"); err != nil || !strings.Contains(string(um), "storage") {
		t.Fatalf("user-metadata xattr: err=%v val=%q", err, um)
	}
	if _, err := xattr.LGet(objFile, "user.d9ds3.tags"); err != nil {
		t.Fatalf("tags xattr missing: %v", err)
	}

	// 4. Internal bookkeeping is in the SEPARATE state dir, not --data.
	if h.configs[0].StateDir == dataDir {
		t.Fatal("state dir must be separate from data dir")
	}
	if _, err := os.Stat(filepath.Join(h.configs[0].StateDir, "buckets", "pure.json")); err != nil {
		t.Fatalf("bucket config should live in the state dir: %v", err)
	}
}
