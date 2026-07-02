// Package s3api terminates the S3 HTTP protocol (path-style) on net/http and maps
// each request onto the gateway. It performs SigV4 authentication, access control
// (bucket policy + ACL), sub-resource routing by query parameter, and S3 XML
// rendering. It is the analogue of versitygw's s3api layer.
package s3api

import (
	"io"
	"net/http"
	"strings"

	"github.com/adi/d9ds3/internal/auth"
	"github.com/adi/d9ds3/internal/gateway"
	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/webui"
)

// Server is the S3 front-end.
type Server struct {
	gw       *gateway.Gateway
	verifier *auth.Verifier
	region   string
	webui    http.Handler
	ready    func() bool // readiness gate for /readyz (default: cluster has a leader)
}

// New builds the S3 server over a gateway for the given signing region.
func New(gw *gateway.Gateway, region string) *Server {
	s := &Server{gw: gw, region: region, webui: webui.Handler(gw), ready: gw.HasLeader}
	s.verifier = &auth.Verifier{
		Region:  region,
		Service: "s3",
		Lookup: func(ak string) (secret, account string, ok bool) {
			a, err := gw.GetAccount(ak)
			if err != nil || a == nil {
				return "", "", false
			}
			return a.SecretKey, a.AccessKeyID, true
		},
	}
	return s
}

// Listen serves the S3 API on addr.
func (s *Server) Listen(addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler()}
	return srv.ListenAndServe()
}

// Handler returns the HTTP handler (useful for tests via httptest).
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serve)
}

// SetReadyFunc overrides the /readyz gate. Used to make readiness reflect that the
// bootstrap (root) account is live, not just that the process is up.
func (s *Server) SetReadyFunc(fn func() bool) {
	if fn != nil {
		s.ready = fn
	}
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if rec := recover(); rec != nil {
			writeErr(w, r, s3err.ErrInternal)
		}
	}()

	// Health/readiness endpoints (no auth). Liveness = process up; readiness =
	// ready to serve authenticated traffic (cluster leader present and, when
	// configured, the bootstrap account is live).
	switch r.URL.Path {
	case "/healthz":
		w.WriteHeader(http.StatusOK)
		return
	case "/readyz":
		if s.ready != nil && s.ready() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}

	// The web console and its Basic-auth API are served before SigV4 auth.
	if r.URL.Path == "/console" || strings.HasPrefix(r.URL.Path, "/console/") {
		s.webui.ServeHTTP(w, r)
		return
	}

	// CORS preflight is answered before authentication.
	if r.Method == http.MethodOptions {
		s.handleCORSPreflight(w, r)
		return
	}

	rc, err := s.authenticate(w, r)
	if err != nil {
		writeErr(w, r, err)
		return
	}

	bucket, key := splitPath(r.URL.Path)
	rc.bucket, rc.key = bucket, key
	rc.q = queryValues(r.URL.Query())

	switch {
	case bucket == "":
		s.dispatchService(rc)
	case key == "":
		s.dispatchBucket(rc)
	default:
		s.dispatchObject(rc)
	}
}

// reqCtx is the per-request state shared by handlers.
type reqCtx struct {
	w       http.ResponseWriter
	r       *http.Request
	bucket  string
	key     string
	q       queryValues
	account string
	isAuth  bool
	role    string
	body    io.Reader // effective request body (chunk-decoded when signed-streaming)
	gctx    gateway.Ctx
}

// dispatchService handles service-level requests (no bucket).
func (s *Server) dispatchService(rc *reqCtx) {
	switch rc.r.Method {
	case http.MethodGet:
		if _, ok := rc.q["admin"]; ok {
			s.handleAdmin(rc)
			return
		}
		s.handleListBuckets(rc)
	case http.MethodPost, http.MethodPut, http.MethodDelete:
		if _, ok := rc.q["admin"]; ok {
			s.handleAdmin(rc)
			return
		}
		writeErr(rc.w, rc.r, s3err.ErrMethodNotAllowed)
	default:
		writeErr(rc.w, rc.r, s3err.ErrMethodNotAllowed)
	}
}

// dispatchBucket handles bucket-level requests, keyed by method + sub-resource.
func (s *Server) dispatchBucket(rc *reqCtx) {
	q := rc.q
	switch rc.r.Method {
	case http.MethodGet, http.MethodHead:
		switch {
		case q.has("acl"):
			s.handleGetBucketACL(rc)
		case q.has("policy"):
			s.handleGetBucketPolicy(rc)
		case q.has("cors"):
			s.handleGetBucketCORS(rc)
		case q.has("tagging"):
			s.handleGetBucketTagging(rc)
		case q.has("versioning"):
			s.handleGetBucketVersioning(rc)
		case q.has("location"):
			s.handleGetBucketLocation(rc)
		case q.has("object-lock"):
			s.handleGetObjectLockConfig(rc)
		case q.has("ownershipControls"):
			s.handleGetOwnership(rc)
		case q.has("uploads"):
			s.handleListMultipartUploads(rc)
		case q.has("versions"):
			s.handleListObjectVersions(rc)
		case rc.r.Method == http.MethodHead:
			s.handleHeadBucket(rc)
		default:
			s.handleListObjects(rc)
		}
	case http.MethodPut:
		switch {
		case q.has("acl"):
			s.handlePutBucketACL(rc)
		case q.has("policy"):
			s.handlePutBucketPolicy(rc)
		case q.has("cors"):
			s.handlePutBucketCORS(rc)
		case q.has("tagging"):
			s.handlePutBucketTagging(rc)
		case q.has("versioning"):
			s.handlePutBucketVersioning(rc)
		case q.has("object-lock"):
			s.handlePutObjectLockConfig(rc)
		case q.has("ownershipControls"):
			s.handlePutOwnership(rc)
		default:
			s.handleCreateBucket(rc)
		}
	case http.MethodPost:
		if q.has("delete") {
			s.handleDeleteObjects(rc)
			return
		}
		writeErr(rc.w, rc.r, s3err.ErrMethodNotAllowed)
	case http.MethodDelete:
		switch {
		case q.has("policy"):
			s.handleDeleteBucketPolicy(rc)
		case q.has("cors"):
			s.handleDeleteBucketCORS(rc)
		case q.has("tagging"):
			s.handleDeleteBucketTagging(rc)
		case q.has("ownershipControls"):
			s.handleDeleteOwnership(rc)
		default:
			s.handleDeleteBucket(rc)
		}
	default:
		writeErr(rc.w, rc.r, s3err.ErrMethodNotAllowed)
	}
}

// dispatchObject handles object-level requests.
func (s *Server) dispatchObject(rc *reqCtx) {
	q := rc.q
	switch rc.r.Method {
	case http.MethodGet:
		switch {
		case q.has("acl"):
			s.handleGetObjectACL(rc)
		case q.has("tagging"):
			s.handleGetObjectTagging(rc)
		case q.has("retention"):
			s.handleGetObjectRetention(rc)
		case q.has("legal-hold"):
			s.handleGetObjectLegalHold(rc)
		case q.has("attributes"):
			s.handleGetObjectAttributes(rc)
		case q.has("uploadId"):
			s.handleListParts(rc)
		default:
			s.handleGetObject(rc)
		}
	case http.MethodHead:
		s.handleHeadObject(rc)
	case http.MethodPut:
		switch {
		case q.has("acl"):
			s.handlePutObjectACL(rc)
		case q.has("tagging"):
			s.handlePutObjectTagging(rc)
		case q.has("retention"):
			s.handlePutObjectRetention(rc)
		case q.has("legal-hold"):
			s.handlePutObjectLegalHold(rc)
		case q.has("uploadId") && q.get("partNumber") != "":
			s.handleUploadPart(rc)
		case rc.r.Header.Get("X-Amz-Copy-Source") != "":
			s.handleCopyObject(rc)
		default:
			s.handlePutObject(rc)
		}
	case http.MethodPost:
		switch {
		case q.has("select"):
			s.handleSelectObject(rc)
		case q.has("uploads"):
			s.handleCreateMultipartUpload(rc)
		case q.has("uploadId"):
			s.handleCompleteMultipartUpload(rc)
		default:
			writeErr(rc.w, rc.r, s3err.ErrMethodNotAllowed)
		}
	case http.MethodDelete:
		switch {
		case q.has("tagging"):
			s.handleDeleteObjectTagging(rc)
		case q.has("uploadId"):
			s.handleAbortMultipartUpload(rc)
		default:
			s.handleDeleteObject(rc)
		}
	default:
		writeErr(rc.w, rc.r, s3err.ErrMethodNotAllowed)
	}
}

// splitPath parses a path-style URL into bucket and key.
func splitPath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", ""
	}
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}
