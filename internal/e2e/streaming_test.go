package e2e

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestStreamingChunkedUpload drives a PutObject with the SDK's DEFAULT checksum
// behavior (which uses aws-chunked streaming with a trailing checksum), verifying
// the gateway decodes and validates the signed chunked payload end-to-end.
func TestStreamingChunkedUpload(t *testing.T) {
	h := startHarness(t, 1)
	ctx := context.Background()

	// A client that does NOT disable checksums → SDK sends aws-chunked + trailer.
	c := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(rootAK, rootSK, ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(h.url)
		o.UsePathStyle = true
	})

	mkBucket(t, c, "stream")
	payload := bytes.Repeat([]byte("streaming-chunked-"), 4096) // ~72 KiB across chunks
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("stream"), Key: aws.String("s.bin"), Body: bytes.NewReader(payload),
	}); err != nil {
		t.Fatalf("streaming PutObject: %v", err)
	}
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("stream"), Key: aws.String("s.bin")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, _ := io.ReadAll(out.Body)
	if !bytes.Equal(got, payload) {
		t.Fatalf("streaming round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}
