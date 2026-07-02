package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestDelimitedPagination checks that delimiter roll-up and pagination compose:
// every object and common prefix appears exactly once across pages.
func TestDelimitedPagination(t *testing.T) {
	h := startHarness(t, 1)
	ctx := context.Background()
	c := h.client
	mkBucket(t, c, "dpage")
	for _, k := range []string{"a", "d1/x", "d1/y", "d2/x", "d2/y", "m", "z"} {
		putText(t, c, "dpage", k, "v")
	}
	// Expected with delimiter "/": objects {a, m, z}, common prefixes {d1/, d2/}.
	gotObjs := map[string]int{}
	gotPfx := map[string]int{}
	var token *string
	pages := 0
	for {
		out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String("dpage"), Delimiter: aws.String("/"),
			MaxKeys: aws.Int32(2), ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("ListObjectsV2: %v", err)
		}
		for _, o := range out.Contents {
			gotObjs[aws.ToString(o.Key)]++
		}
		for _, p := range out.CommonPrefixes {
			gotPfx[aws.ToString(p.Prefix)]++
		}
		pages++
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
		if pages > 20 {
			t.Fatal("pagination did not terminate")
		}
	}
	wantObjs := []string{"a", "m", "z"}
	wantPfx := []string{"d1/", "d2/"}
	if len(gotObjs) != 3 || len(gotPfx) != 2 {
		t.Fatalf("objs=%v prefixes=%v", gotObjs, gotPfx)
	}
	for _, k := range wantObjs {
		if gotObjs[k] != 1 {
			t.Fatalf("object %q count=%d (want 1)", k, gotObjs[k])
		}
	}
	for _, p := range wantPfx {
		if gotPfx[p] != 1 {
			t.Fatalf("prefix %q count=%d (want 1)", p, gotPfx[p])
		}
	}
	if pages < 3 {
		t.Fatalf("expected multiple pages, got %d", pages)
	}
}

// TestVersionPagination checks paged ListObjectVersions returns every version once.
func TestVersionPagination(t *testing.T) {
	h := startHarness(t, 1)
	ctx := context.Background()
	c := h.client
	mkBucket(t, c, "vpage")
	if _, err := c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String("vpage"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("PutBucketVersioning: %v", err)
	}
	for i := 0; i < 3; i++ {
		putText(t, c, "vpage", "v", "x")
	}
	for i := 0; i < 2; i++ {
		putText(t, c, "vpage", "w", "y")
	}
	seen := map[string]int{} // key|versionId
	var keyMarker, verMarker *string
	pages := 0
	for {
		out, err := c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String("vpage"), MaxKeys: aws.Int32(2),
			KeyMarker: keyMarker, VersionIdMarker: verMarker,
		})
		if err != nil {
			t.Fatalf("ListObjectVersions: %v", err)
		}
		for _, v := range out.Versions {
			seen[aws.ToString(v.Key)+"|"+aws.ToString(v.VersionId)]++
		}
		pages++
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		keyMarker, verMarker = out.NextKeyMarker, out.NextVersionIdMarker
		if pages > 20 {
			t.Fatal("version pagination did not terminate")
		}
	}
	if len(seen) != 5 {
		t.Fatalf("expected 5 distinct versions across pages, got %d: %v", len(seen), seen)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("version %q returned %d times", k, n)
		}
	}
}
