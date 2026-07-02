package e2e

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestS3Select drives SelectObjectContent through the AWS SDK end-to-end.
func TestS3Select(t *testing.T) {
	h := startHarness(t, 1)
	ctx := context.Background()
	c := h.client
	mkBucket(t, c, "sel")

	csv := "name,age,city\nalice,34,NYC\nbob,29,LA\ncarol,41,SF\n"
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("sel"), Key: aws.String("people.csv"), Body: bytes.NewReader([]byte(csv)),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	out, err := c.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
		Bucket:         aws.String("sel"),
		Key:            aws.String("people.csv"),
		Expression:     aws.String("SELECT s.name FROM S3Object s WHERE CAST(s.age AS INT) > 30"),
		ExpressionType: s3types.ExpressionTypeSql,
		InputSerialization: &s3types.InputSerialization{
			CSV: &s3types.CSVInput{FileHeaderInfo: s3types.FileHeaderInfoUse},
		},
		OutputSerialization: &s3types.OutputSerialization{
			CSV: &s3types.CSVOutput{},
		},
	})
	if err != nil {
		t.Fatalf("SelectObjectContent: %v", err)
	}
	defer out.GetStream().Close()

	var got bytes.Buffer
	for ev := range out.GetStream().Events() {
		if rec, ok := ev.(*s3types.SelectObjectContentEventStreamMemberRecords); ok {
			got.Write(rec.Value.Payload)
		}
	}
	if err := out.GetStream().Err(); err != nil {
		t.Fatalf("event stream err: %v", err)
	}
	result := got.String()
	if !strings.Contains(result, "alice") || !strings.Contains(result, "carol") {
		t.Fatalf("expected alice+carol (age>30), got: %q", result)
	}
	if strings.Contains(result, "bob") {
		t.Fatalf("bob (age 29) should be filtered out, got: %q", result)
	}
}
