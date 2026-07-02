package auth

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	testRegion  = "us-east-1"
	testService = "s3"
	testAKID    = "AKIAIOSFODNN7EXAMPLE"
	testSecret  = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testAccount = "123456789012"
)

// testLookup knows exactly one access key.
func testLookup(accessKeyID string) (string, string, bool) {
	if accessKeyID == testAKID {
		return testSecret, testAccount, true
	}
	return "", "", false
}

func newVerifier() *Verifier {
	return &Verifier{Region: testRegion, Service: testService, Lookup: testLookup}
}

func testCreds() aws.Credentials {
	return aws.Credentials{AccessKeyID: testAKID, SecretAccessKey: testSecret}
}

// disableDoubleEncoding matches how the aws-sdk-go-v2 S3 client signs.
func disableDoubleEncoding(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }

func mustAuthError(t *testing.T, err error) *AuthError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an *AuthError, got nil")
	}
	var ae *AuthError
	if !errors.As(err, &ae) {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
	return ae
}

func TestVerifyHeaderSigning(t *testing.T) {
	body := []byte("hello world, this is the object payload")
	payloadHash := hexSHA256(body)

	req, err := http.NewRequest(http.MethodPut, "http://s3.example.com/my-bucket/path/to/object.txt", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("Content-Type", "text/plain")

	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), testCreds(), req, payloadHash, testService, testRegion, time.Now(), disableDoubleEncoding); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}

	res, err := newVerifier().Verify(req)
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}
	if res.Identity == nil || res.Identity.Anonymous {
		t.Fatalf("expected authenticated identity, got %+v", res.Identity)
	}
	if res.Identity.AccessKeyID != testAKID || res.Identity.Account != testAccount {
		t.Fatalf("unexpected identity: %+v", res.Identity)
	}
	if res.Body != nil {
		t.Fatalf("expected nil Body for non-streaming request")
	}
}

func TestVerifyHeaderSigningGET(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://s3.example.com/my-bucket?prefix=foo&max-keys=10", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", emptyStringSHA256)

	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), testCreds(), req, emptyStringSHA256, testService, testRegion, time.Now(), disableDoubleEncoding); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}

	res, err := newVerifier().Verify(req)
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}
	if res.Identity.AccessKeyID != testAKID {
		t.Fatalf("unexpected identity: %+v", res.Identity)
	}
}

func TestVerifyTamperedSignature(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://s3.example.com/my-bucket/obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", emptyStringSHA256)

	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), testCreds(), req, emptyStringSHA256, testService, testRegion, time.Now(), disableDoubleEncoding); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}

	// Flip the last hex digit of the signature.
	auth := req.Header.Get("Authorization")
	last := auth[len(auth)-1]
	repl := byte('0')
	if last == '0' {
		repl = '1'
	}
	req.Header.Set("Authorization", auth[:len(auth)-1]+string(repl))

	_, err = newVerifier().Verify(req)
	ae := mustAuthError(t, err)
	if ae.Code != "SignatureDoesNotMatch" {
		t.Fatalf("expected SignatureDoesNotMatch, got %q", ae.Code)
	}
}

func TestVerifyTamperedHeader(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://s3.example.com/my-bucket/obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", emptyStringSHA256)
	req.Header.Set("X-Amz-Meta-Foo", "original")

	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), testCreds(), req, emptyStringSHA256, testService, testRegion, time.Now(), disableDoubleEncoding); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}

	// Tamper a signed header's value after signing.
	if !strings.Contains(req.Header.Get("Authorization"), "x-amz-meta-foo") {
		t.Fatalf("expected x-amz-meta-foo to be signed; Authorization=%q", req.Header.Get("Authorization"))
	}
	req.Header.Set("X-Amz-Meta-Foo", "tampered")

	_, err = newVerifier().Verify(req)
	ae := mustAuthError(t, err)
	if ae.Code != "SignatureDoesNotMatch" {
		t.Fatalf("expected SignatureDoesNotMatch, got %q", ae.Code)
	}
}

func TestVerifyUnknownAccessKey(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://s3.example.com/my-bucket/obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", emptyStringSHA256)

	unknownCreds := aws.Credentials{AccessKeyID: "AKIAUNKNOWNKEYEXAMPLE", SecretAccessKey: "someothersecret"}
	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), unknownCreds, req, emptyStringSHA256, testService, testRegion, time.Now(), disableDoubleEncoding); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}

	_, err = newVerifier().Verify(req)
	ae := mustAuthError(t, err)
	if ae.Code != "InvalidAccessKeyId" {
		t.Fatalf("expected InvalidAccessKeyId, got %q", ae.Code)
	}
}

func TestVerifyAnonymous(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://s3.example.com/public-bucket/obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := newVerifier().Verify(req)
	if err != nil {
		t.Fatalf("expected nil error for anonymous request, got %v", err)
	}
	if res.Identity == nil || !res.Identity.Anonymous {
		t.Fatalf("expected anonymous identity, got %+v", res.Identity)
	}
}

func TestVerifyPresigned(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://s3.example.com/my-bucket/path/to/object.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	q := req.URL.Query()
	q.Set("X-Amz-Expires", "900")
	req.URL.RawQuery = q.Encode()

	signer := v4.NewSigner()
	signedURI, _, err := signer.PresignHTTP(context.Background(), testCreds(), req, unsignedPayload, testService, testRegion, time.Now(), disableDoubleEncoding)
	if err != nil {
		t.Fatalf("PresignHTTP: %v", err)
	}

	got, err := http.NewRequest(http.MethodGet, signedURI, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := newVerifier().Verify(got)
	if err != nil {
		t.Fatalf("Verify presigned returned error: %v", err)
	}
	if res.Identity.AccessKeyID != testAKID || res.Identity.Account != testAccount {
		t.Fatalf("unexpected identity: %+v", res.Identity)
	}
}

func TestVerifyPresignedExpired(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://s3.example.com/my-bucket/object.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	q := req.URL.Query()
	q.Set("X-Amz-Expires", "1")
	req.URL.RawQuery = q.Encode()

	// Sign as of an hour ago so the 1-second window is long past.
	signer := v4.NewSigner()
	signedURI, _, err := signer.PresignHTTP(context.Background(), testCreds(), req, unsignedPayload, testService, testRegion, time.Now().Add(-time.Hour), disableDoubleEncoding)
	if err != nil {
		t.Fatalf("PresignHTTP: %v", err)
	}

	got, err := http.NewRequest(http.MethodGet, signedURI, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newVerifier().Verify(got)
	ae := mustAuthError(t, err)
	if ae.Code != "AccessDenied" {
		t.Fatalf("expected AccessDenied for expired presign, got %q", ae.Code)
	}
}

func TestVerifyPresignedTamperedSignature(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://s3.example.com/my-bucket/object.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	q := req.URL.Query()
	q.Set("X-Amz-Expires", "900")
	req.URL.RawQuery = q.Encode()

	signer := v4.NewSigner()
	signedURI, _, err := signer.PresignHTTP(context.Background(), testCreds(), req, unsignedPayload, testService, testRegion, time.Now(), disableDoubleEncoding)
	if err != nil {
		t.Fatalf("PresignHTTP: %v", err)
	}

	got, err := http.NewRequest(http.MethodGet, signedURI, nil)
	if err != nil {
		t.Fatal(err)
	}
	gq := got.URL.Query()
	sig := gq.Get("X-Amz-Signature")
	repl := byte('0')
	if sig[len(sig)-1] == '0' {
		repl = '1'
	}
	gq.Set("X-Amz-Signature", sig[:len(sig)-1]+string(repl))
	got.URL.RawQuery = gq.Encode()

	_, err = newVerifier().Verify(got)
	ae := mustAuthError(t, err)
	if ae.Code != "SignatureDoesNotMatch" {
		t.Fatalf("expected SignatureDoesNotMatch, got %q", ae.Code)
	}
}

// chunkSignature computes an aws-chunked per-chunk signature the same way the
// implementation does, so the test can hand-build a valid streaming body.
func chunkSignature(signingKey []byte, scope, amzDate, prevSig string, data []byte) string {
	sts := strings.Join([]string{
		chunkStringToSignPrefix,
		amzDate,
		scope,
		prevSig,
		emptyStringSHA256,
		hexSHA256(data),
	}, "\n")
	return hex.EncodeToString(hmacSHA256(signingKey, []byte(sts)))
}

// buildStreamingRequest signs a streaming request, then builds a matching
// aws-chunked body for the given chunks. It returns the request (with Body set)
// and the seed signature. If corruptChunk >= 0, that chunk's signature is
// corrupted.
func buildStreamingRequest(t *testing.T, chunks [][]byte, corruptChunk int) *http.Request {
	t.Helper()

	signingTime := time.Now().UTC()
	amzDate := signingTime.Format(amzDateFormat)
	shortDate := signingTime.Format(shortDateFormat)
	scope := strings.Join([]string{shortDate, testRegion, testService, "aws4_request"}, "/")

	req, err := http.NewRequest(http.MethodPut, "http://s3.example.com/my-bucket/streamed-object", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", streamingPayload)

	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), testCreds(), req, streamingPayload, testService, testRegion, signingTime, disableDoubleEncoding); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}

	// Extract the seed (header) signature from the Authorization header.
	_, _, seedSig, err := parseAuthorizationHeader(req.Header.Get("Authorization"))
	if err != nil {
		t.Fatalf("parseAuthorizationHeader: %v", err)
	}

	signingKey := deriveSigningKey(testSecret, shortDate, testRegion, testService)

	var body bytes.Buffer
	prev := seedSig
	writeChunk := func(idx int, data []byte) {
		sig := chunkSignature(signingKey, scope, amzDate, prev, data)
		prev = sig
		if idx == corruptChunk {
			// Corrupt the signature that goes on the wire (but keep the chain
			// state honest for subsequent chunks).
			b := []byte(sig)
			if b[len(b)-1] == '0' {
				b[len(b)-1] = '1'
			} else {
				b[len(b)-1] = '0'
			}
			sig = string(b)
		}
		fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(data), sig)
		body.Write(data)
		body.WriteString("\r\n")
	}

	for i, c := range chunks {
		writeChunk(i, c)
	}
	// Terminating zero-length chunk.
	writeChunk(len(chunks), []byte{})

	req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))
	return req
}

func TestVerifyStreaming(t *testing.T) {
	chunk1 := []byte("the quick brown fox ")
	chunk2 := []byte("jumps over the lazy dog")
	want := append(append([]byte{}, chunk1...), chunk2...)

	req := buildStreamingRequest(t, [][]byte{chunk1, chunk2}, -1)

	res, err := newVerifier().Verify(req)
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}
	if res.Body == nil {
		t.Fatalf("expected a decoding Body for streaming request")
	}
	got, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("reading decoded body: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("decoded body mismatch:\n got=%q\nwant=%q", got, want)
	}
}

func TestVerifyStreamingCorruptChunk(t *testing.T) {
	chunk1 := []byte("the quick brown fox ")
	chunk2 := []byte("jumps over the lazy dog")

	// Corrupt the second chunk's signature.
	req := buildStreamingRequest(t, [][]byte{chunk1, chunk2}, 1)

	res, err := newVerifier().Verify(req)
	if err != nil {
		t.Fatalf("Verify returned error (seed signature should still be valid): %v", err)
	}
	if res.Body == nil {
		t.Fatalf("expected a decoding Body")
	}
	_, err = io.ReadAll(res.Body)
	if err == nil {
		t.Fatalf("expected an error reading a body with a corrupt chunk signature")
	}
	if !strings.Contains(err.Error(), "chunk signature") {
		t.Fatalf("expected chunk signature error, got: %v", err)
	}
}
