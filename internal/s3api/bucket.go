package s3api

import (
	"encoding/xml"
	"io"
	"net/http"

	"github.com/adi/d9ds3/internal/authz"
	"github.com/adi/d9ds3/internal/gateway"
	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

const maxConfigBody = 20 << 20 // 20 MiB cap for XML/JSON config bodies

func readBody(rc *reqCtx) ([]byte, error) {
	return io.ReadAll(io.LimitReader(rc.body, maxConfigBody))
}

// ---- service ----

func (s *Server) handleListBuckets(rc *reqCtx) {
	if !rc.isAuth {
		writeErr(rc.w, rc.r, s3err.ErrAccessDenied)
		return
	}
	bs, err := s.gw.ListBuckets()
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if rc.role != "admin" {
		owned := bs[:0]
		for _, b := range bs {
			if b.Owner == rc.account {
				owned = append(owned, b)
			}
		}
		bs = owned
	}
	writeXML(rc.w, http.StatusOK, renderListBuckets(rc.account, bs))
}

// ---- bucket lifecycle ----

func (s *Server) handleCreateBucket(rc *reqCtx) {
	if err := s.authorize(rc, actCreateBucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	in := gateway.CreateBucketInput{
		Bucket:     rc.bucket,
		Ownership:  rc.r.Header.Get("X-Amz-Object-Ownership"),
		ObjectLock: rc.r.Header.Get("X-Amz-Bucket-Object-Lock-Enabled") == "true",
	}
	if canned := rc.r.Header.Get("X-Amz-Acl"); canned != "" {
		in.ACL = authz.CannedACL(canned, types.Owner{ID: rc.account, DisplayName: rc.account}, types.Owner{ID: rc.account})
	}
	if _, err := s.gw.CreateBucket(rc.gctx, in); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.Header().Set("Location", "/"+rc.bucket)
	rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteBucket(rc *reqCtx) {
	if err := s.authorize(rc, actDeleteBucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if _, err := s.gw.DeleteBucket(rc.gctx, rc.bucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeNoContent(rc.w)
}

func (s *Server) handleHeadBucket(rc *reqCtx) {
	if err := s.authorize(rc, actListBucket); err != nil {
		rc.w.WriteHeader(s3err.From(err).HTTPStatus)
		return
	}
	if _, err := s.gw.GetBucketMeta(rc.bucket); err != nil {
		rc.w.WriteHeader(s3err.From(err).HTTPStatus)
		return
	}
	rc.w.Header().Set("x-amz-bucket-region", s.region)
	rc.w.WriteHeader(http.StatusOK)
}

// ---- listings ----

func (s *Server) handleListObjects(rc *reqCtx) {
	if err := s.authorize(rc, actListBucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	q := rc.q
	in := types.ListInput{
		Bucket: rc.bucket, Prefix: q.get("prefix"), Delimiter: q.get("delimiter"),
		MaxKeys: atoiOr(q.get("max-keys"), 1000),
		Marker:  q.get("marker"), StartAfter: q.get("start-after"),
		ContinuationToken: q.get("continuation-token"),
	}
	res, err := s.gw.ListObjectsV2(in)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	v2 := q.get("list-type") == "2"
	writeXML(rc.w, http.StatusOK, renderListObjects(in, res, v2))
}

func (s *Server) handleListObjectVersions(rc *reqCtx) {
	if err := s.authorize(rc, actListBucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	q := rc.q
	in := types.ListVersionsInput{
		Bucket: rc.bucket, Prefix: q.get("prefix"), Delimiter: q.get("delimiter"),
		MaxKeys:   atoiOr(q.get("max-keys"), 1000),
		KeyMarker: q.get("key-marker"), VersionIDMarker: q.get("version-id-marker"),
	}
	res, err := s.gw.ListObjectVersions(in)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeXML(rc.w, http.StatusOK, renderListVersions(in, res))
}

func (s *Server) handleListMultipartUploads(rc *reqCtx) {
	if err := s.authorize(rc, actListBucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	ups, err := s.gw.ListMultipartUploads(rc.bucket, rc.q.get("prefix"))
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	out := &xListMPU{XMLNS: s3ns, Bucket: rc.bucket}
	for _, u := range ups {
		out.Uploads = append(out.Uploads, xMPUEntry{Key: u.Key, UploadId: u.UploadID, Initiated: fmtTime(u.Initiated)})
	}
	writeXML(rc.w, http.StatusOK, out)
}

// ---- bucket ACL ----

func (s *Server) handleGetBucketACL(rc *reqCtx) {
	if err := s.authorize(rc, actGetBucketAcl); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	bm, err := s.gw.GetBucketMeta(rc.bucket)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	acl := bm.ACL
	if acl == nil {
		acl = authz.CannedACL("private", types.Owner{ID: bm.Owner, DisplayName: bm.Owner}, types.Owner{ID: bm.Owner})
	}
	writeXML(rc.w, http.StatusOK, renderACL(acl))
}

func (s *Server) handlePutBucketACL(rc *reqCtx) {
	if err := s.authorize(rc, actPutBucketAcl); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	acl, err := s.resolveACLFromRequest(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if _, err := s.gw.PutBucketACL(rc.gctx, rc.bucket, acl); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

// ---- bucket policy ----

func (s *Server) handleGetBucketPolicy(rc *reqCtx) {
	if err := s.authorize(rc, actGetBucketPol); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	bm, err := s.gw.GetBucketMeta(rc.bucket)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if len(bm.Policy) == 0 {
		writeErr(rc.w, rc.r, s3err.ErrNoSuchBucketPolicy)
		return
	}
	rc.w.Header().Set("Content-Type", "application/json")
	rc.w.WriteHeader(http.StatusOK)
	rc.w.Write(bm.Policy)
}

func (s *Server) handlePutBucketPolicy(rc *reqCtx) {
	if err := s.authorize(rc, actBucketPolicy); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if _, err := s.gw.PutBucketPolicy(rc.gctx, rc.bucket, body); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeNoContent(rc.w)
}

func (s *Server) handleDeleteBucketPolicy(rc *reqCtx) {
	if err := s.authorize(rc, actBucketPolicy); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if _, err := s.gw.DeleteBucketPolicy(rc.gctx, rc.bucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeNoContent(rc.w)
}

// ---- bucket CORS ----

func (s *Server) handleGetBucketCORS(rc *reqCtx) {
	bm, err := s.gw.GetBucketMeta(rc.bucket)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if len(bm.CORS) == 0 {
		writeErr(rc.w, rc.r, s3err.ErrNoSuchCORS)
		return
	}
	writeXML(rc.w, http.StatusOK, renderCORS(bm.CORS))
}

func (s *Server) handlePutBucketCORS(rc *reqCtx) {
	if err := s.authorize(rc, actPutBucketAcl); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rules, err := parseCORS(body)
	if err != nil {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}
	if _, err := s.gw.PutBucketCors(rc.gctx, rc.bucket, rules); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteBucketCORS(rc *reqCtx) {
	if _, err := s.gw.DeleteBucketCors(rc.gctx, rc.bucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeNoContent(rc.w)
}

// ---- bucket tagging ----

func (s *Server) handleGetBucketTagging(rc *reqCtx) {
	bm, err := s.gw.GetBucketMeta(rc.bucket)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if len(bm.Tags) == 0 {
		writeErr(rc.w, rc.r, s3err.ErrNoSuchTagSet)
		return
	}
	writeXML(rc.w, http.StatusOK, renderTagging(bm.Tags))
}

func (s *Server) handlePutBucketTagging(rc *reqCtx) {
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	tags, err := parseTagging(body)
	if err != nil {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}
	if _, err := s.gw.PutBucketTagging(rc.gctx, rc.bucket, tags); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteBucketTagging(rc *reqCtx) {
	if _, err := s.gw.DeleteBucketTagging(rc.gctx, rc.bucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeNoContent(rc.w)
}

// ---- versioning ----

func (s *Server) handleGetBucketVersioning(rc *reqCtx) {
	bm, err := s.gw.GetBucketMeta(rc.bucket)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeXML(rc.w, http.StatusOK, &xVersioning{XMLNS: s3ns, Status: bm.Versioning})
}

func (s *Server) handlePutBucketVersioning(rc *reqCtx) {
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	var x xVersioning
	if err := xml.Unmarshal(body, &x); err != nil {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}
	if _, err := s.gw.PutBucketVersioning(rc.gctx, rc.bucket, x.Status); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

// ---- location ----

func (s *Server) handleGetBucketLocation(rc *reqCtx) {
	if _, err := s.gw.GetBucketMeta(rc.bucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	loc := s.region
	if loc == "us-east-1" {
		loc = "" // us-east-1 is represented by an empty LocationConstraint
	}
	writeXML(rc.w, http.StatusOK, &xLocation{XMLNS: s3ns, Location: loc})
}

// ---- object lock config ----

func (s *Server) handleGetObjectLockConfig(rc *reqCtx) {
	bm, err := s.gw.GetBucketMeta(rc.bucket)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if bm.ObjectLock == nil || !bm.ObjectLock.Enabled {
		writeErr(rc.w, rc.r, s3err.ErrNoSuchObjectLock)
		return
	}
	out := &xObjectLockCfg{XMLNS: s3ns, ObjectLockEnabled: "Enabled"}
	if bm.ObjectLock.DefaultMode != "" {
		out.Rule = &xLockRule{DefaultRetention: xDefaultRetention{
			Mode: bm.ObjectLock.DefaultMode, Days: bm.ObjectLock.DefaultDays, Years: bm.ObjectLock.DefaultYrs,
		}}
	}
	writeXML(rc.w, http.StatusOK, out)
}

func (s *Server) handlePutObjectLockConfig(rc *reqCtx) {
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	var x xObjectLockCfg
	if err := xml.Unmarshal(body, &x); err != nil {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}
	cfg := &types.ObjectLockConfig{Enabled: x.ObjectLockEnabled == "Enabled"}
	if x.Rule != nil {
		cfg.DefaultMode = x.Rule.DefaultRetention.Mode
		cfg.DefaultDays = x.Rule.DefaultRetention.Days
		cfg.DefaultYrs = x.Rule.DefaultRetention.Years
	}
	if _, err := s.gw.PutObjectLockConfig(rc.gctx, rc.bucket, cfg); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

// ---- ownership controls ----

func (s *Server) handleGetOwnership(rc *reqCtx) {
	bm, err := s.gw.GetBucketMeta(rc.bucket)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	own := bm.Ownership
	if own == "" {
		own = "BucketOwnerEnforced"
	}
	writeXML(rc.w, http.StatusOK, &xOwnership{XMLNS: s3ns, Rules: []xOwnershipRule{{ObjectOwnership: own}}})
}

func (s *Server) handlePutOwnership(rc *reqCtx) {
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	var x xOwnership
	if err := xml.Unmarshal(body, &x); err != nil || len(x.Rules) == 0 {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}
	if _, err := s.gw.PutBucketOwnership(rc.gctx, rc.bucket, x.Rules[0].ObjectOwnership); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteOwnership(rc *reqCtx) {
	if _, err := s.gw.DeleteBucketOwnership(rc.gctx, rc.bucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeNoContent(rc.w)
}

// resolveACLFromRequest builds an ACL from either a canned-ACL header or an XML body.
func (s *Server) resolveACLFromRequest(rc *reqCtx) (*types.ACL, error) {
	owner := types.Owner{ID: rc.account, DisplayName: rc.account}
	if canned := rc.r.Header.Get("X-Amz-Acl"); canned != "" {
		return authz.CannedACL(canned, owner, owner), nil
	}
	body, err := readBody(rc)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return authz.CannedACL("private", owner, owner), nil
	}
	acl, err := parseACL(body)
	if err != nil {
		return nil, s3err.ErrMalformedXML
	}
	return acl, nil
}
