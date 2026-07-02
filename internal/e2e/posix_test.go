package e2e

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestPosixMapping proves the versitygw-style 1:1 mapping: an object PUT through
// S3 is a plain, browsable file at <data>/<bucket>/<key> with the exact bytes.
func TestPosixMapping(t *testing.T) {
	h := startHarness(t, 1)
	ctx := context.Background()
	dataDir := h.configs[0].DataDir

	mkBucket(t, h.client, "posix")
	body := []byte("browsable on disk")
	if _, err := h.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("posix"), Key: aws.String("dir/sub/hello.txt"), Body: bytes.NewReader(body),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// The object is a real file at the natural path.
	onDisk := filepath.Join(dataDir, "posix", "dir", "sub", "hello.txt")
	got, err := os.ReadFile(onDisk)
	if err != nil {
		t.Fatalf("object not browsable at %s: %v", onDisk, err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("on-disk bytes mismatch: %q", got)
	}

	// Deleting through S3 removes the file from the tree.
	if _, err := h.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("posix"), Key: aws.String("dir/sub/hello.txt")}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := os.Stat(onDisk); !os.IsNotExist(err) {
		t.Fatalf("file should be gone after delete, stat err=%v", err)
	}
}

// TestPrefilledDisk proves the reverse direction: a disk seeded with plain folders
// and files (no S3 ever involved) is served as S3 buckets/objects, with metadata
// synthesized from the files.
func TestPrefilledDisk(t *testing.T) {
	h := startHarness(t, 1)
	ctx := context.Background()
	dataDir := h.configs[0].DataDir

	// Seed the disk directly: a "bucket" folder with a nested file.
	seed := filepath.Join(dataDir, "seededbucket", "photos", "2024")
	if err := os.MkdirAll(seed, 0o755); err != nil {
		t.Fatalf("seed mkdir: %v", err)
	}
	content := []byte("hello from a pre-existing file")
	if err := os.WriteFile(filepath.Join(seed, "note.txt"), content, 0o644); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	// The folder shows up as a bucket.
	lb, err := h.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if !hasBucket(lb.Buckets, "seededbucket") {
		t.Fatalf("prefilled folder not listed as a bucket: %+v", lb.Buckets)
	}

	// The file shows up as an object.
	lo, err := h.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("seededbucket")})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	found := false
	for _, o := range lo.Contents {
		if aws.ToString(o.Key) == "photos/2024/note.txt" {
			found = true
			if aws.ToInt64(o.Size) != int64(len(content)) {
				t.Fatalf("synthesized size wrong: %d", aws.ToInt64(o.Size))
			}
		}
	}
	if !found {
		t.Fatalf("prefilled file not listed as an object: %+v", lo.Contents)
	}

	// And it's readable via S3 with a synthesized ETag/content-type.
	out, err := h.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("seededbucket"), Key: aws.String("photos/2024/note.txt")})
	if err != nil {
		t.Fatalf("GetObject prefilled: %v", err)
	}
	gotBytes, _ := io.ReadAll(out.Body)
	if !bytes.Equal(gotBytes, content) {
		t.Fatalf("prefilled GET body mismatch: %q", gotBytes)
	}
	if aws.ToString(out.ContentType) != "text/plain" {
		t.Fatalf("synthesized content-type: %q", aws.ToString(out.ContentType))
	}
	if aws.ToString(out.ETag) == "" {
		t.Fatal("expected a synthesized ETag")
	}
}
