package s3api

import (
	"errors"
	"net"
	"net/http"
	"strings"

	"github.com/adi/d9ds3/internal/auth"
	"github.com/adi/d9ds3/internal/authz"
	"github.com/adi/d9ds3/internal/s3err"
)

// queryValues is url.Values with convenience accessors for sub-resource routing.
type queryValues map[string][]string

func (q queryValues) has(k string) bool { _, ok := q[k]; return ok }
func (q queryValues) get(k string) string {
	if v, ok := q[k]; ok && len(v) > 0 {
		return v[0]
	}
	return ""
}

// authenticate verifies SigV4 (or detects anonymous) and builds the request ctx.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (*reqCtx, error) {
	res, err := s.verifier.Verify(r)
	if err != nil {
		var ae *auth.AuthError
		if errors.As(err, &ae) {
			return nil, s3err.APIError{Code: ae.Code, Message: ae.Message, HTTPStatus: ae.HTTPStatus}
		}
		return nil, s3err.ErrAccessDenied
	}
	rc := &reqCtx{w: w, r: r, body: r.Body}
	if res.Body != nil {
		rc.body = res.Body
	}
	rc.gctx.SourceIP = clientIP(r)
	if res.Identity != nil && !res.Identity.Anonymous {
		rc.isAuth = true
		rc.account = res.Identity.Account
		rc.gctx.Account = res.Identity.Account
		if a, err := s.gw.GetAccount(res.Identity.AccessKeyID); err == nil && a != nil {
			rc.role = a.Role
		}
	}
	return rc, nil
}

// S3 action names for policy evaluation.
const (
	actListBucket   = "s3:ListBucket"
	actGetObject    = "s3:GetObject"
	actPutObject    = "s3:PutObject"
	actDeleteObject = "s3:DeleteObject"
	actGetAcl       = "s3:GetObjectAcl"
	actPutAcl       = "s3:PutObjectAcl"
	actGetBucketAcl = "s3:GetBucketAcl"
	actPutBucketAcl = "s3:PutBucketAcl"
	actCreateBucket = "s3:CreateBucket"
	actDeleteBucket = "s3:DeleteBucket"
	actGetTagging   = "s3:GetObjectTagging"
	actPutTagging   = "s3:PutObjectTagging"
	actBucketPolicy = "s3:PutBucketPolicy"
	actGetBucketPol = "s3:GetBucketPolicy"
)

// authorize enforces bucket policy + ACL for the action on bucket/key.
// Admin role and bucket owner bypass all checks. Nonexistent buckets are allowed
// through so the handler can return the proper NoSuchBucket / create the bucket.
func (s *Server) authorize(rc *reqCtx, action string) error {
	if rc.role == "admin" {
		return nil
	}
	bm, err := s.gw.GetBucketMeta(rc.bucket)
	if err != nil {
		// Bucket doesn't exist yet: creating requires authentication; otherwise
		// defer to the handler (it will surface NoSuchBucket).
		if action == actCreateBucket && !rc.isAuth {
			return s3err.ErrAccessDenied
		}
		return nil
	}
	if rc.isAuth && bm.Owner == rc.account {
		return nil
	}
	if len(bm.Policy) > 0 {
		switch authz.EvaluatePolicy(bm.Policy, authz.Request{
			Principal: rc.account, IsAuthenticated: rc.isAuth,
			Action: action, Bucket: rc.bucket, Key: rc.key, SourceIP: rc.gctx.SourceIP,
		}) {
		case authz.DecisionDeny:
			return s3err.ErrAccessDenied
		case authz.DecisionAllow:
			return nil
		}
	}
	// ACL fallback.
	acl := bm.ACL
	if isObjectAction(action) && rc.key != "" {
		if om, err := s.gw.HeadObject(rc.bucket, rc.key, ""); err == nil && om.ACL != nil {
			acl = om.ACL
		}
	}
	if acl != nil && authz.CheckACL(acl, rc.account, rc.isAuth, permForAction(action)) {
		return nil
	}
	return s3err.ErrAccessDenied
}

func isObjectAction(action string) bool {
	switch action {
	case actGetObject, actPutObject, actDeleteObject, actGetAcl, actPutAcl, actGetTagging, actPutTagging:
		return true
	}
	return false
}

func permForAction(action string) string {
	switch action {
	case actGetObject, actListBucket, actGetTagging:
		return "READ"
	case actPutObject, actDeleteObject, actPutTagging:
		return "WRITE"
	case actGetAcl, actGetBucketAcl:
		return "READ_ACP"
	case actPutAcl, actPutBucketAcl:
		return "WRITE_ACP"
	default:
		return "FULL_CONTROL"
	}
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
