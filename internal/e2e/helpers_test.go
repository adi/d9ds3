package e2e

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/adi/d9ds3/internal/gateway"
	"github.com/adi/d9ds3/internal/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func mkBucket(t *testing.T, c *s3.Client, name string) {
	t.Helper()
	if _, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("CreateBucket %s: %v", name, err)
	}
}

func putText(t *testing.T, c *s3.Client, bucket, key, body string) {
	t.Helper()
	if _, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(body)),
	}); err != nil {
		t.Fatalf("PutObject %s/%s: %v", bucket, key, err)
	}
}

func getText(t *testing.T, c *s3.Client, bucket, key string) string {
	t.Helper()
	out, err := c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject %s/%s: %v", bucket, key, err)
	}
	b, _ := io.ReadAll(out.Body)
	return string(b)
}

// getTextC puts then gets, used to smoke-test a fresh client's credentials.
func getTextC(t *testing.T, c *s3.Client, bucket, key, body string) string {
	t.Helper()
	putText(t, c, bucket, key, body)
	return getText(t, c, bucket, key)
}

func hasBucket(bs []s3types.Bucket, name string) bool {
	for _, b := range bs {
		if aws.ToString(b.Name) == name {
			return true
		}
	}
	return false
}

func strReader(s string) *strings.Reader { return strings.NewReader(s) }

func gwCtx() gateway.Ctx { return gateway.Ctx{Account: rootAK} }

func account(ak, sk, role string) types.Account {
	return types.Account{AccessKeyID: ak, SecretKey: sk, Role: role}
}

func awsCfg(ak, sk string) aws.Config {
	return aws.Config{
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider(ak, sk, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
}
