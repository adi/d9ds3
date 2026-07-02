// Package auth implements AWS Signature Version 4 (SigV4) verification for an
// S3-compatible gateway. It supports the three signing schemes S3 clients use:
//
//  1. Authorization-header signing ("AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...")
//  2. Presigned query signing (X-Amz-Algorithm / X-Amz-Credential / X-Amz-Signature / ...)
//  3. Streaming chunked payload (x-amz-content-sha256 == STREAMING-AWS4-HMAC-SHA256-PAYLOAD),
//     for which Verify returns a decoding Body that validates each chunk signature.
//
// The implementation mirrors the canonicalization performed by
// github.com/aws/aws-sdk-go-v2/aws/signer/v4 for the S3 service (which disables
// double URI-path escaping), so requests signed by that SDK verify cleanly.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	algorithm               = "AWS4-HMAC-SHA256"
	unsignedPayload         = "UNSIGNED-PAYLOAD"
	streamingPayload        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingPayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	chunkStringToSignPrefix = "AWS4-HMAC-SHA256-PAYLOAD"

	// emptyStringSHA256 is the hex encoded sha256 of the empty string.
	emptyStringSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	amzDateFormat   = "20060102T150405Z"
	shortDateFormat = "20060102"

	maxClockSkew = 15 * time.Minute
)

// timeNow is overridable in tests; production uses time.Now.
var timeNow = time.Now

// SecretLookup resolves an access key id to its secret key and account id.
// ok=false means the key is unknown.
type SecretLookup func(accessKeyID string) (secretKey string, accountID string, ok bool)

// Identity is the caller identity resolved from a request's signature.
type Identity struct {
	AccessKeyID string
	Account     string
	Anonymous   bool
}

// Verifier authenticates SigV4-signed *http.Request values.
type Verifier struct {
	Region  string // e.g. "us-east-1"
	Service string // "s3"
	Lookup  SecretLookup
}

// AuthError carries an S3 error code so the caller can render the appropriate
// XML error response.
type AuthError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func newAuthError(code, message string, status int) *AuthError {
	return &AuthError{Code: code, Message: message, HTTPStatus: status}
}

func errSignatureMismatch() *AuthError {
	return newAuthError("SignatureDoesNotMatch",
		"The request signature we calculated does not match the signature you provided. Check your key and signing method.",
		http.StatusForbidden)
}

func errInvalidAccessKey(id string) *AuthError {
	return newAuthError("InvalidAccessKeyId",
		"The AWS Access Key Id you provided does not exist in our records: "+id,
		http.StatusForbidden)
}

func errAccessDenied(msg string) *AuthError {
	return newAuthError("AccessDenied", msg, http.StatusForbidden)
}

func errMalformedAuthHeader(msg string) *AuthError {
	return newAuthError("AuthorizationHeaderMalformed", msg, http.StatusBadRequest)
}

func errMalformedQuery(msg string) *AuthError {
	return newAuthError("AuthorizationQueryParametersError", msg, http.StatusBadRequest)
}

// VerifyResult is returned by Verify on success.
type VerifyResult struct {
	Identity *Identity
	// Body, if non-nil, MUST be used by the caller in place of r.Body. It is set
	// when the request uses STREAMING-AWS4-HMAC-SHA256-PAYLOAD (aws-chunked);
	// reading it yields the decoded object payload and validates each chunk's
	// signature, returning an error on mismatch.
	Body io.Reader
}

// credentialScope holds the parsed pieces of a SigV4 credential scope string:
// "<accessKeyID>/<date>/<region>/<service>/aws4_request".
type credentialScope struct {
	accessKeyID string
	date        string
	region      string
	service     string
	scope       string // "<date>/<region>/<service>/aws4_request"
}

func parseCredential(cred string) (credentialScope, error) {
	parts := strings.Split(cred, "/")
	if len(parts) != 5 || parts[4] != "aws4_request" {
		return credentialScope{}, fmt.Errorf("malformed credential %q", cred)
	}
	for _, p := range parts {
		if p == "" {
			return credentialScope{}, fmt.Errorf("malformed credential %q", cred)
		}
	}
	return credentialScope{
		accessKeyID: parts[0],
		date:        parts[1],
		region:      parts[2],
		service:     parts[3],
		scope:       strings.Join(parts[1:], "/"),
	}, nil
}

// Verify authenticates an *http.Request signed with SigV4. See the package doc
// and the type comments for the supported schemes. A request carrying no
// signing information is treated as anonymous (Identity{Anonymous:true}, nil
// error); the caller is responsible for enforcing bucket/object policy.
func (v *Verifier) Verify(r *http.Request) (*VerifyResult, error) {
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return v.verifyPresigned(r)
	}
	if strings.HasPrefix(r.Header.Get("Authorization"), algorithm) {
		return v.verifyHeader(r)
	}
	// No signing information at all -> anonymous.
	return &VerifyResult{Identity: &Identity{Anonymous: true}}, nil
}

// verifyHeader handles Authorization-header (and streaming chunked) signing.
func (v *Verifier) verifyHeader(r *http.Request) (*VerifyResult, error) {
	cred, signedHeaders, providedSig, err := parseAuthorizationHeader(r.Header.Get("Authorization"))
	if err != nil {
		return nil, errMalformedAuthHeader(err.Error())
	}

	secret, account, ok := v.Lookup(cred.accessKeyID)
	if !ok {
		return nil, errInvalidAccessKey(cred.accessKeyID)
	}

	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}
	signingTime, err := time.Parse(amzDateFormat, amzDate)
	if err != nil {
		return nil, errMalformedAuthHeader("invalid X-Amz-Date: " + amzDate)
	}
	if skew := absDuration(timeNow().Sub(signingTime)); skew > maxClockSkew {
		return nil, errAccessDenied("Request date is too skewed from server time")
	}

	hashedPayload := r.Header.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		// Header signing without an explicit content hash: treat as the empty
		// payload hash used by clients that omit the header.
		hashedPayload = emptyStringSHA256
	}

	signingKey := deriveSigningKey(secret, cred.date, cred.region, cred.service)
	expectedSig, err := computeSignature(r, signedHeaders, hashedPayload, amzDate, cred.scope, signingKey)
	if err != nil {
		return nil, errMalformedAuthHeader(err.Error())
	}
	if !constantTimeEqualHex(expectedSig, providedSig) {
		return nil, errSignatureMismatch()
	}

	res := &VerifyResult{Identity: &Identity{AccessKeyID: cred.accessKeyID, Account: account}}

	// Streaming chunked payload: expose a decoding, per-chunk-validating body.
	if hashedPayload == streamingPayload || hashedPayload == streamingPayloadTrailer {
		res.Body = newChunkedReader(r.Body, signingKey, cred.scope, amzDate, providedSig)
	}

	return res, nil
}

// verifyPresigned handles presigned-URL (query string) signing.
func (v *Verifier) verifyPresigned(r *http.Request) (*VerifyResult, error) {
	q := r.URL.Query()

	if got := q.Get("X-Amz-Algorithm"); got != algorithm {
		return nil, errMalformedQuery("unsupported X-Amz-Algorithm: " + got)
	}
	providedSig := q.Get("X-Amz-Signature")
	credStr := q.Get("X-Amz-Credential")
	signedHeadersStr := q.Get("X-Amz-SignedHeaders")
	amzDate := q.Get("X-Amz-Date")
	if providedSig == "" || credStr == "" || signedHeadersStr == "" || amzDate == "" {
		return nil, errMalformedQuery("missing required query signing parameters")
	}

	cred, err := parseCredential(credStr)
	if err != nil {
		return nil, errMalformedQuery(err.Error())
	}

	signingTime, err := time.Parse(amzDateFormat, amzDate)
	if err != nil {
		return nil, errMalformedQuery("invalid X-Amz-Date: " + amzDate)
	}

	// Enforce the presign expiry window (X-Amz-Expires seconds from X-Amz-Date).
	expiresStr := q.Get("X-Amz-Expires")
	if expiresStr == "" {
		return nil, errMalformedQuery("missing X-Amz-Expires")
	}
	expiresSec, err := strconv.Atoi(expiresStr)
	if err != nil || expiresSec <= 0 {
		return nil, errMalformedQuery("invalid X-Amz-Expires: " + expiresStr)
	}
	now := timeNow()
	if now.Before(signingTime.Add(-maxClockSkew)) {
		return nil, errAccessDenied("Request is not yet valid")
	}
	if now.After(signingTime.Add(time.Duration(expiresSec) * time.Second)) {
		return nil, errAccessDenied("Request has expired")
	}

	secret, account, ok := v.Lookup(cred.accessKeyID)
	if !ok {
		return nil, errInvalidAccessKey(cred.accessKeyID)
	}

	signedHeaders := splitSignedHeaders(signedHeadersStr)

	// Canonical query string excludes X-Amz-Signature (it is appended after
	// the canonical request is built).
	canonQuery := q
	canonQuery.Del("X-Amz-Signature")

	signingKey := deriveSigningKey(secret, cred.date, cred.region, cred.service)
	expectedSig, err := computeSignatureWithQuery(r, signedHeaders, unsignedPayload, amzDate, cred.scope, signingKey, canonicalQuery(canonQuery))
	if err != nil {
		return nil, errMalformedQuery(err.Error())
	}
	if !constantTimeEqualHex(expectedSig, providedSig) {
		return nil, errSignatureMismatch()
	}

	return &VerifyResult{Identity: &Identity{AccessKeyID: cred.accessKeyID, Account: account}}, nil
}

// parseAuthorizationHeader parses "AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...".
func parseAuthorizationHeader(h string) (cred credentialScope, signedHeaders []string, signature string, err error) {
	rest := strings.TrimSpace(strings.TrimPrefix(h, algorithm))
	var credStr, signedHeadersStr string
	for _, part := range strings.Split(rest, ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "Credential="):
			credStr = strings.TrimPrefix(part, "Credential=")
		case strings.HasPrefix(part, "SignedHeaders="):
			signedHeadersStr = strings.TrimPrefix(part, "SignedHeaders=")
		case strings.HasPrefix(part, "Signature="):
			signature = strings.TrimPrefix(part, "Signature=")
		}
	}
	if credStr == "" || signedHeadersStr == "" || signature == "" {
		return credentialScope{}, nil, "", fmt.Errorf("missing Credential/SignedHeaders/Signature")
	}
	cred, err = parseCredential(credStr)
	if err != nil {
		return credentialScope{}, nil, "", err
	}
	return cred, splitSignedHeaders(signedHeadersStr), signature, nil
}

func splitSignedHeaders(s string) []string {
	parts := strings.Split(s, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.ToLower(strings.TrimSpace(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// computeSignature builds the canonical request from the request's own query
// string and returns the hex signature.
func computeSignature(r *http.Request, signedHeaders []string, hashedPayload, amzDate, scope string, signingKey []byte) (string, error) {
	return computeSignatureWithQuery(r, signedHeaders, hashedPayload, amzDate, scope, signingKey, canonicalQuery(r.URL.Query()))
}

func computeSignatureWithQuery(r *http.Request, signedHeaders []string, hashedPayload, amzDate, scope string, signingKey []byte, canonQuery string) (string, error) {
	canonHeaders, err := canonicalHeaders(r, signedHeaders)
	if err != nil {
		return "", err
	}
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r.URL),
		canonQuery,
		canonHeaders,
		strings.Join(signedHeaders, ";"),
		hashedPayload,
	}, "\n")

	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		scope,
		hexSHA256([]byte(canonicalRequest)),
	}, "\n")

	return hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign))), nil
}

// canonicalURI returns the S3-style canonical URI path. S3 signing disables
// double URI-path escaping, so the canonical path is exactly the request's
// (already-encoded) escaped path, with an empty path normalized to "/".
func canonicalURI(u *url.URL) string {
	p := u.EscapedPath()
	if p == "" {
		return "/"
	}
	return p
}

// canonicalQuery encodes query parameters sorted by key, matching the SDK which
// uses url.Values.Encode() and then replaces "+" with "%20".
func canonicalQuery(v url.Values) string {
	return strings.ReplaceAll(v.Encode(), "+", "%20")
}

// canonicalHeaders builds the canonical headers block (one "name:value\n" line
// per signed header, in the order given) plus enforces the SDK's whitespace
// normalization.
func canonicalHeaders(r *http.Request, signedHeaders []string) (string, error) {
	host := sanitizedHost(r)
	var b strings.Builder
	for _, name := range signedHeaders {
		b.WriteString(name)
		b.WriteByte(':')
		switch name {
		case "host":
			b.WriteString(stripExcessSpaces(host))
		case "content-length":
			v := r.Header.Get("Content-Length")
			if v == "" && r.ContentLength >= 0 {
				v = strconv.FormatInt(r.ContentLength, 10)
			}
			b.WriteString(strings.TrimSpace(stripExcessSpaces(v)))
		default:
			values := r.Header.Values(textproto.CanonicalMIMEHeaderKey(name))
			for i, v := range values {
				if i > 0 {
					b.WriteByte(',')
				}
				b.WriteString(strings.TrimSpace(stripExcessSpaces(v)))
			}
		}
		b.WriteByte('\n')
	}
	return b.String(), nil
}

// sanitizedHost mirrors the SDK's host resolution: prefer r.Host, fall back to
// r.URL.Host, and strip a default port (80/443) for the request scheme.
func sanitizedHost(r *http.Request) string {
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	if port := portOnly(host); port != "" && isDefaultPort(r.URL.Scheme, port) {
		host = stripPort(host)
	}
	return host
}

func stripPort(hostport string) string {
	colon := strings.IndexByte(hostport, ':')
	if colon == -1 {
		return hostport
	}
	if i := strings.IndexByte(hostport, ']'); i != -1 {
		return strings.TrimPrefix(hostport[:i], "[")
	}
	return hostport[:colon]
}

func portOnly(hostport string) string {
	colon := strings.IndexByte(hostport, ':')
	if colon == -1 {
		return ""
	}
	if i := strings.Index(hostport, "]:"); i != -1 {
		return hostport[i+len("]:"):]
	}
	if strings.Contains(hostport, "]") {
		return ""
	}
	return hostport[colon+len(":"):]
}

func isDefaultPort(scheme, port string) bool {
	if port == "" {
		return true
	}
	s := strings.ToLower(scheme)
	return (s == "http" && port == "80") || (s == "https" && port == "443")
}

const doubleSpace = "  "

// stripExcessSpaces collapses runs of internal spaces to a single space,
// matching the SDK's normalization of signed header values.
func stripExcessSpaces(str string) string {
	var j, k, l, m, spaces int
	for j = len(str) - 1; j >= 0 && str[j] == ' '; j-- {
	}
	for k = 0; k < j && str[k] == ' '; k++ {
	}
	str = str[k : j+1]

	j = strings.Index(str, doubleSpace)
	if j < 0 {
		return str
	}

	buf := []byte(str)
	for k, m, l = j, j, len(buf); k < l; k++ {
		if buf[k] == ' ' {
			if spaces == 0 {
				buf[m] = buf[k]
				m++
			}
			spaces++
		} else {
			spaces = 0
			buf[m] = buf[k]
			m++
		}
	}
	return string(buf[:m])
}

// deriveSigningKey computes the SigV4 signing key HMAC chain.
func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func constantTimeEqualHex(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
