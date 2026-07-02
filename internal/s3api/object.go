package s3api

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adi/d9ds3/internal/authz"
	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/gateway"
	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

// ---- GET / HEAD ----

func (s *Server) handleGetObject(rc *reqCtx) {
	if err := s.authorize(rc, actGetObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	opt := types.GetOptions{VersionID: rc.q.get("versionId")}
	if br := parseRangeHeader(rc.r.Header.Get("Range")); br != nil {
		opt.Range = br
	}
	res, err := s.gw.GetObject(rc.bucket, rc.key, opt)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	defer res.Body.Close()

	if code := evalConditional(rc.r.Header, res.Info.ETag, res.Info.LastModified); code != 0 {
		rc.w.WriteHeader(code)
		return
	}
	setObjectResponseHeaders(rc.w, &res.Info)
	applyResponseOverrides(rc.w, rc.q)
	if res.IsRange {
		rc.w.Header().Set("Content-Range", res.ContentRange)
		rc.w.Header().Set("Content-Length", strconv.FormatInt(res.PartialLength, 10))
		rc.w.WriteHeader(http.StatusPartialContent)
	} else {
		rc.w.Header().Set("Content-Length", strconv.FormatInt(res.Info.Size, 10))
		rc.w.WriteHeader(http.StatusOK)
	}
	io.Copy(rc.w, res.Body)
}

func (s *Server) handleHeadObject(rc *reqCtx) {
	if err := s.authorize(rc, actGetObject); err != nil {
		rc.w.WriteHeader(s3err.From(err).HTTPStatus)
		return
	}
	oi, err := s.gw.HeadObject(rc.bucket, rc.key, rc.q.get("versionId"))
	if err != nil {
		rc.w.WriteHeader(s3err.From(err).HTTPStatus)
		return
	}
	if code := evalConditional(rc.r.Header, oi.ETag, oi.LastModified); code != 0 {
		rc.w.WriteHeader(code)
		return
	}
	setObjectResponseHeaders(rc.w, oi)
	rc.w.Header().Set("Content-Length", strconv.FormatInt(oi.Size, 10))
	rc.w.WriteHeader(http.StatusOK)
}

// ---- PUT ----

func (s *Server) handlePutObject(rc *reqCtx) {
	if err := s.authorize(rc, actPutObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	in := s.putInputFromHeaders(rc)
	res, err := s.gw.PutObject(rc.gctx, in, rc.body)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.Header().Set("ETag", res.ETag)
	if res.VersionID != "" && res.VersionID != "null" {
		rc.w.Header().Set("x-amz-version-id", res.VersionID)
	}
	rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) putInputFromHeaders(rc *reqCtx) gateway.PutObjectInput {
	h := rc.r.Header
	in := gateway.PutObjectInput{
		Bucket: rc.bucket, Key: rc.key,
		ContentType:        h.Get("Content-Type"),
		ContentEncoding:    h.Get("Content-Encoding"),
		ContentLanguage:    h.Get("Content-Language"),
		ContentDisposition: h.Get("Content-Disposition"),
		CacheControl:       h.Get("Cache-Control"),
		Expires:            h.Get("Expires"),
		StorageClass:       h.Get("X-Amz-Storage-Class"),
		ContentMD5:         h.Get("Content-Md5"),
		UserMeta:           userMetaFromHeaders(h),
		LegalHold:          h.Get("X-Amz-Object-Lock-Legal-Hold"),
	}
	if tagging := h.Get("X-Amz-Tagging"); tagging != "" {
		in.Tags = parseTagQuery(tagging)
	}
	if canned := h.Get("X-Amz-Acl"); canned != "" {
		owner := types.Owner{ID: rc.account, DisplayName: rc.account}
		in.ACL = authz.CannedACL(canned, owner, owner)
	}
	if mode := h.Get("X-Amz-Object-Lock-Mode"); mode != "" {
		in.RetentionMode = mode
		in.RetainUntil = parseAmzTime(h.Get("X-Amz-Object-Lock-Retain-Until-Date"))
	}
	return in
}

// ---- Copy ----

func (s *Server) handleCopyObject(rc *reqCtx) {
	if err := s.authorize(rc, actPutObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	srcBucket, srcKey, srcVer, ok := parseCopySource(rc.r.Header.Get("X-Amz-Copy-Source"))
	if !ok {
		writeErr(rc.w, rc.r, s3err.ErrInvalidArgument)
		return
	}
	in := gateway.CopyObjectInput{
		SrcBucket: srcBucket, SrcKey: srcKey, SrcVersionID: srcVer,
		DstBucket: rc.bucket, DstKey: rc.key,
		MetadataDirective: rc.r.Header.Get("X-Amz-Metadata-Directive"),
		TaggingDirective:  rc.r.Header.Get("X-Amz-Tagging-Directive"),
		ContentType:       rc.r.Header.Get("Content-Type"),
		UserMeta:          userMetaFromHeaders(rc.r.Header),
	}
	res, err := s.gw.CopyObject(rc.gctx, in)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if res.VersionID != "" && res.VersionID != "null" {
		rc.w.Header().Set("x-amz-version-id", res.VersionID)
	}
	writeXML(rc.w, http.StatusOK, &xCopyResult{XMLNS: s3ns, ETag: res.ETag, LastModified: fmtTime(res.MTime)})
}

// ---- Delete ----

func (s *Server) handleDeleteObject(rc *reqCtx) {
	if err := s.authorize(rc, actDeleteObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	res, err := s.gw.DeleteObject(rc.gctx, rc.bucket, rc.key, rc.q.get("versionId"))
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if res.VersionID != "" && res.VersionID != "null" {
		rc.w.Header().Set("x-amz-version-id", res.VersionID)
	}
	if res.DeleteMarker {
		rc.w.Header().Set("x-amz-delete-marker", "true")
	}
	writeNoContent(rc.w)
}

func (s *Server) handleDeleteObjects(rc *reqCtx) {
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	var req xDeleteReq
	if err := xml.Unmarshal(body, &req); err != nil {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}
	out := &xDeleteResult{XMLNS: s3ns}
	for _, o := range req.Objects {
		rc.key = o.Key
		if err := s.authorize(rc, actDeleteObject); err != nil {
			out.Errors = append(out.Errors, xDelError{Key: o.Key, Code: "AccessDenied", Message: "Access Denied"})
			continue
		}
		res, err := s.gw.DeleteObject(rc.gctx, rc.bucket, o.Key, o.VersionId)
		if err != nil {
			ae := s3err.From(err)
			out.Errors = append(out.Errors, xDelError{Key: o.Key, Code: ae.Code, Message: ae.Message})
			continue
		}
		if !req.Quiet {
			d := xDeleted{Key: o.Key, VersionId: o.VersionId}
			if res.DeleteMarker {
				d.DeleteMarker = true
				d.DeleteMarkerVersionId = res.VersionID
			}
			out.Deleted = append(out.Deleted, d)
		}
	}
	writeXML(rc.w, http.StatusOK, out)
}

// ---- object ACL ----

func (s *Server) handleGetObjectACL(rc *reqCtx) {
	if err := s.authorize(rc, actGetAcl); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	oi, err := s.gw.HeadObject(rc.bucket, rc.key, rc.q.get("versionId"))
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	acl := oi.ACL
	if acl == nil {
		bm, _ := s.gw.GetBucketMeta(rc.bucket)
		owner := types.Owner{ID: rc.account}
		if bm != nil {
			owner = types.Owner{ID: bm.Owner, DisplayName: bm.Owner}
		}
		acl = authz.CannedACL("private", owner, owner)
	}
	writeXML(rc.w, http.StatusOK, renderACL(acl))
}

func (s *Server) handlePutObjectACL(rc *reqCtx) {
	if err := s.authorize(rc, actPutAcl); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	acl, err := s.resolveACLFromRequest(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if _, err := s.gw.PutObjectACL(rc.gctx, rc.bucket, rc.key, rc.q.get("versionId"), acl); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

// ---- object tagging ----

func (s *Server) handleGetObjectTagging(rc *reqCtx) {
	if err := s.authorize(rc, actGetTagging); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	oi, err := s.gw.HeadObject(rc.bucket, rc.key, rc.q.get("versionId"))
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeXML(rc.w, http.StatusOK, renderTagging(oi.Tags))
}

func (s *Server) handlePutObjectTagging(rc *reqCtx) {
	if err := s.authorize(rc, actPutTagging); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
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
	if _, err := s.gw.PutObjectTagging(rc.gctx, rc.bucket, rc.key, rc.q.get("versionId"), tags); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteObjectTagging(rc *reqCtx) {
	if err := s.authorize(rc, actPutTagging); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if _, err := s.gw.DeleteObjectTagging(rc.gctx, rc.bucket, rc.key, rc.q.get("versionId")); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeNoContent(rc.w)
}

// ---- retention / legal hold ----

func (s *Server) handleGetObjectRetention(rc *reqCtx) {
	oi, err := s.gw.HeadObject(rc.bucket, rc.key, rc.q.get("versionId"))
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if oi.Retention == nil {
		writeErr(rc.w, rc.r, s3err.ErrNoSuchObjectLock)
		return
	}
	writeXML(rc.w, http.StatusOK, &xRetention{
		XMLNS: s3ns, Mode: oi.Retention.Mode, RetainUntilDate: oi.Retention.RetainUntil.UTC().Format(time.RFC3339),
	})
}

func (s *Server) handlePutObjectRetention(rc *reqCtx) {
	if err := s.authorize(rc, actPutObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	var x xRetention
	if err := xml.Unmarshal(body, &x); err != nil {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}
	r := &types.Retention{Mode: x.Mode, RetainUntil: parseAmzTime(x.RetainUntilDate)}
	if _, err := s.gw.PutObjectRetention(rc.gctx, rc.bucket, rc.key, rc.q.get("versionId"), r); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetObjectLegalHold(rc *reqCtx) {
	oi, err := s.gw.HeadObject(rc.bucket, rc.key, rc.q.get("versionId"))
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	status := "OFF"
	if oi.LegalHold != nil {
		status = oi.LegalHold.Status
	}
	writeXML(rc.w, http.StatusOK, &xLegalHold{XMLNS: s3ns, Status: status})
}

func (s *Server) handlePutObjectLegalHold(rc *reqCtx) {
	if err := s.authorize(rc, actPutObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	var x xLegalHold
	if err := xml.Unmarshal(body, &x); err != nil {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}
	if _, err := s.gw.PutObjectLegalHold(rc.gctx, rc.bucket, rc.key, rc.q.get("versionId"), x.Status); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetObjectAttributes(rc *reqCtx) {
	if err := s.authorize(rc, actGetObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	oi, err := s.gw.HeadObject(rc.bucket, rc.key, rc.q.get("versionId"))
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeXML(rc.w, http.StatusOK, &xObjectAttributes{
		XMLNS: s3ns, ETag: strings.Trim(oi.ETag, `"`), ObjectSize: oi.Size, StorageClass: storageClassOr(oi.StorageClass),
	})
}

// ---- multipart ----

func (s *Server) handleCreateMultipartUpload(rc *reqCtx) {
	if err := s.authorize(rc, actPutObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	in := s.putInputFromHeaders(rc)
	uploadID, err := s.gw.CreateMultipartUpload(rc.gctx, in)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeXML(rc.w, http.StatusOK, &xInitiateMPU{XMLNS: s3ns, Bucket: rc.bucket, Key: rc.key, UploadId: uploadID})
}

func (s *Server) handleUploadPart(rc *reqCtx) {
	if err := s.authorize(rc, actPutObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	partNum, err := strconv.Atoi(rc.q.get("partNumber"))
	if err != nil || partNum < 1 {
		writeErr(rc.w, rc.r, s3err.ErrInvalidArgument)
		return
	}
	uploadID := rc.q.get("uploadId")

	if src := rc.r.Header.Get("X-Amz-Copy-Source"); src != "" {
		sb, sk, sv, ok := parseCopySource(src)
		if !ok {
			writeErr(rc.w, rc.r, s3err.ErrInvalidArgument)
			return
		}
		var rng *types.ByteRange
		if r := parseRangeHeader(strings.Replace(rc.r.Header.Get("X-Amz-Copy-Source-Range"), "bytes=", "bytes=", 1)); r != nil {
			rng = r
		}
		etag, mtime, err := s.gw.UploadPartCopy(rc.gctx, rc.bucket, rc.key, uploadID, partNum,
			gateway.CopyObjectInput{SrcBucket: sb, SrcKey: sk, SrcVersionID: sv}, rng)
		if err != nil {
			writeErr(rc.w, rc.r, err)
			return
		}
		writeXML(rc.w, http.StatusOK, &xCopyResult{XMLNS: s3ns, ETag: etag, LastModified: fmtTime(mtime)})
		return
	}

	etag, err := s.gw.UploadPart(rc.gctx, rc.bucket, rc.key, uploadID, partNum, rc.body)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	rc.w.Header().Set("ETag", etag)
	rc.w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCompleteMultipartUpload(rc *reqCtx) {
	if err := s.authorize(rc, actPutObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	body, err := readBody(rc)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	var req xCompleteReq
	if err := xml.Unmarshal(body, &req); err != nil {
		writeErr(rc.w, rc.r, s3err.ErrMalformedXML)
		return
	}
	parts := make([]command.CompletedPart, 0, len(req.Parts))
	for _, p := range req.Parts {
		parts = append(parts, command.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag})
	}
	res, err := s.gw.CompleteMultipartUpload(rc.gctx, rc.bucket, rc.key, rc.q.get("uploadId"), parts)
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if res.VersionID != "" && res.VersionID != "null" {
		rc.w.Header().Set("x-amz-version-id", res.VersionID)
	}
	writeXML(rc.w, http.StatusOK, &xCompleteMPU{
		XMLNS: s3ns, Bucket: rc.bucket, Key: rc.key, ETag: res.ETag,
		Location: "/" + rc.bucket + "/" + rc.key,
	})
}

func (s *Server) handleAbortMultipartUpload(rc *reqCtx) {
	if err := s.authorize(rc, actPutObject); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	if _, err := s.gw.AbortMultipartUpload(rc.gctx, rc.bucket, rc.key, rc.q.get("uploadId")); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	writeNoContent(rc.w)
}

func (s *Server) handleListParts(rc *reqCtx) {
	if err := s.authorize(rc, actListBucket); err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	mp, err := s.gw.GetMultipartUpload(rc.bucket, rc.q.get("uploadId"))
	if err != nil {
		writeErr(rc.w, rc.r, err)
		return
	}
	out := &xListParts{
		XMLNS: s3ns, Bucket: rc.bucket, Key: rc.key, UploadId: mp.UploadID,
		StorageClass: storageClassOr(mp.StorageClass),
	}
	for _, p := range mp.Parts {
		out.Parts = append(out.Parts, xPart{
			PartNumber: p.PartNumber, ETag: p.ETag, Size: p.Size, LastModified: fmtTime(p.LastModified),
		})
	}
	writeXML(rc.w, http.StatusOK, out)
}

// ---- helpers ----

func userMetaFromHeaders(h http.Header) map[string]string {
	out := map[string]string{}
	for k, v := range h {
		if len(k) > 11 && strings.EqualFold(k[:11], "X-Amz-Meta-") && len(v) > 0 {
			out[strings.ToLower(k[11:])] = v[0]
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseTagQuery(s string) map[string]string {
	vals, err := url.ParseQuery(s)
	if err != nil {
		return nil
	}
	out := map[string]string{}
	for k, v := range vals {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

// parseCopySource parses "[/]bucket/key[?versionId=..]" (URL-encoded).
func parseCopySource(src string) (bucket, key, versionID string, ok bool) {
	if q := strings.IndexByte(src, '?'); q >= 0 {
		if vals, err := url.ParseQuery(src[q+1:]); err == nil {
			versionID = vals.Get("versionId")
		}
		src = src[:q]
	}
	src = strings.TrimPrefix(src, "/")
	if dec, err := url.QueryUnescape(src); err == nil {
		src = dec
	}
	i := strings.IndexByte(src, '/')
	if i < 0 {
		return "", "", "", false
	}
	return src[:i], src[i+1:], versionID, true
}

func parseRangeHeader(h string) *types.ByteRange {
	if !strings.HasPrefix(h, "bytes=") {
		return nil
	}
	spec := strings.TrimPrefix(h, "bytes=")
	if strings.Contains(spec, ",") {
		return nil
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return nil
	}
	startS, endS := spec[:dash], spec[dash+1:]
	if startS == "" {
		n, err := strconv.ParseInt(endS, 10, 64)
		if err != nil {
			return nil
		}
		return &types.ByteRange{Start: -n, End: -1}
	}
	start, err := strconv.ParseInt(startS, 10, 64)
	if err != nil {
		return nil
	}
	br := &types.ByteRange{Start: start, End: -1}
	if endS != "" {
		if e, err := strconv.ParseInt(endS, 10, 64); err == nil {
			br.End = e
		}
	}
	return br
}

// evalConditional returns a non-zero HTTP status if a precondition short-circuits
// the response (304 Not Modified or 412 Precondition Failed), else 0.
func evalConditional(h http.Header, etag string, lastMod time.Time) int {
	etagMatch := func(want string) bool {
		want = strings.TrimSpace(want)
		return want == "*" || strings.Contains(want, etag) || strings.Contains(want, strings.Trim(etag, `"`))
	}
	if inm := h.Get("If-None-Match"); inm != "" {
		if etagMatch(inm) {
			return http.StatusNotModified
		}
	} else if ims := h.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !lastMod.After(t) {
			return http.StatusNotModified
		}
	}
	if im := h.Get("If-Match"); im != "" && !etagMatch(im) {
		return http.StatusPreconditionFailed
	}
	if ius := h.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && lastMod.After(t) {
			return http.StatusPreconditionFailed
		}
	}
	return 0
}

func applyResponseOverrides(w http.ResponseWriter, q queryValues) {
	set := func(param, header string) {
		if v := q.get(param); v != "" {
			w.Header().Set(header, v)
		}
	}
	set("response-content-type", "Content-Type")
	set("response-content-language", "Content-Language")
	set("response-expires", "Expires")
	set("response-cache-control", "Cache-Control")
	set("response-content-disposition", "Content-Disposition")
	set("response-content-encoding", "Content-Encoding")
}

func atoiOr(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}
