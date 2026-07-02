// Package gateway implements the write (Mutator) surface of the S3 gateway: it
// fans object/part payloads out to the storage replicas, submits commands to the
// Raft leader, and returns once the log commits. Reads are delegated to the
// cluster client. Authentication and access control live in the s3api layer; the
// gateway records the authenticated identity carried on each call.
package gateway

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/adi/d9ds3/internal/cluster"
	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/s3event"
	"github.com/adi/d9ds3/internal/types"
	"github.com/google/uuid"
)

// Gateway is the stateless write/read facade over the storage cluster.
type Gateway struct {
	cl       *cluster.Client
	events   s3event.Notifier
	stageDir string
}

// New builds a gateway. stageDir buffers upload bodies before fan-out; events
// (may be nil) receives notifications on mutations.
func New(cl *cluster.Client, stageDir string, events s3event.Notifier) (*Gateway, error) {
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return nil, err
	}
	if events == nil {
		events = s3event.New("")
	}
	return &Gateway{cl: cl, events: events, stageDir: stageDir}, nil
}

// Client exposes the underlying cluster client (used by the s3api layer for
// account lookups during authentication).
func (g *Gateway) Client() *cluster.Client { return g.cl }

// Ctx carries per-request context for a mutation.
type Ctx struct {
	Account  string // authenticated account id (owner / IssuedBy)
	SourceIP string
}

var nowFn = time.Now

func nowStamp() string { return nowFn().UTC().Format(time.RFC3339Nano) }
func newID() string    { return uuid.NewString() }

// submit encodes and appends a command, returning the commit index.
func (g *Gateway) submit(c *command.Command) (uint64, error) {
	if c.Meta == nil {
		c.Meta = map[string]string{}
	}
	if _, ok := c.Meta[command.MetaMTime]; !ok {
		c.Meta[command.MetaMTime] = nowStamp()
	}
	raw, err := command.Encode(c)
	if err != nil {
		return 0, err
	}
	return g.cl.Submit(c.Bucket, raw)
}

// bucketVersioning reports whether new writes should create distinct versions.
func (g *Gateway) bucketVersioning(bucket string) (bool, *types.BucketMeta, error) {
	bm, err := g.cl.GetBucketMeta(bucket)
	if err != nil {
		return false, nil, err
	}
	return bm.VersioningEnabled(), bm, nil
}

// visibleVersion returns the version id the client should see for a write.
func visibleVersion(versioningEnabled bool, candidate string) string {
	if versioningEnabled {
		return candidate
	}
	return "null"
}

// ---- buckets ----

// CreateBucketInput parameterizes CreateBucket.
type CreateBucketInput struct {
	Bucket     string
	Ownership  string
	ObjectLock bool
	ACL        *types.ACL
}

func (g *Gateway) CreateBucket(ctx Ctx, in CreateBucketInput) (uint64, error) {
	c := &command.Command{Op: command.OpCreateBucket, Bucket: in.Bucket, IssuedBy: ctx.Account, Meta: map[string]string{}}
	if in.Ownership != "" {
		c.Meta["ownership"] = in.Ownership
	}
	if in.ObjectLock {
		c.Meta["object-lock"] = "true"
	}
	if in.ACL != nil {
		c.Config, _ = json.Marshal(in.ACL)
	}
	return g.submit(c)
}

func (g *Gateway) DeleteBucket(ctx Ctx, bucket string) (uint64, error) {
	return g.submit(&command.Command{Op: command.OpDeleteBucket, Bucket: bucket, IssuedBy: ctx.Account})
}

// ---- PutObject ----

// PutObjectInput parameterizes PutObject.
type PutObjectInput struct {
	Bucket, Key        string
	ContentType        string
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
	Expires            string
	StorageClass       string
	UserMeta           map[string]string
	Tags               map[string]string
	ACL                *types.ACL
	RetentionMode      string
	RetainUntil        time.Time
	LegalHold          string
	ContentMD5         string // optional base64 md5 to validate
}

// PutResult is returned after a successful write.
type PutResult struct {
	ETag      string
	VersionID string
	Index     uint64
}

func (g *Gateway) PutObject(ctx Ctx, in PutObjectInput, body io.Reader) (*PutResult, error) {
	verEnabled, _, err := g.bucketVersioning(in.Bucket)
	if err != nil {
		return nil, err
	}
	token := newID()
	tmpPath := filepath.Join(g.stageDir, token)
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpPath)

	h := md5.New()
	size, err := io.Copy(tmp, io.TeeReader(body, h))
	tmp.Close()
	if err != nil {
		return nil, err
	}
	sum := h.Sum(nil)
	if in.ContentMD5 != "" {
		if err := checkContentMD5(in.ContentMD5, sum); err != nil {
			return nil, err
		}
	}
	etag := `"` + hex.EncodeToString(sum) + `"`

	if err := g.cl.FanOut(token, tmpPath); err != nil {
		return nil, err
	}

	versionID := newID()
	c := &command.Command{
		Op: command.OpPutObject, Bucket: in.Bucket, Key: in.Key,
		VersionID: versionID, BlobToken: token, Size: size, ETag: etag,
		StorageCls: in.StorageClass, IssuedBy: ctx.Account,
		Meta: g.contentMeta(in),
	}
	if in.ACL != nil {
		c.Config, _ = json.Marshal(in.ACL)
	}
	idx, err := g.submit(c)
	if err != nil {
		return nil, err
	}
	vv := visibleVersion(verEnabled, versionID)
	g.fire(s3event.Event{EventName: "s3:ObjectCreated:Put", Bucket: in.Bucket, Key: in.Key, VersionID: vv, ETag: etag, Size: size, Requester: ctx.Account, SourceIP: ctx.SourceIP})
	return &PutResult{ETag: etag, VersionID: vv, Index: idx}, nil
}

func (g *Gateway) contentMeta(in PutObjectInput) map[string]string {
	m := map[string]string{command.MetaMTime: nowStamp()}
	set := func(k, v string) {
		if v != "" {
			m[k] = v
		}
	}
	set(command.MetaContentType, in.ContentType)
	set(command.MetaContentEncoding, in.ContentEncoding)
	set(command.MetaContentLang, in.ContentLanguage)
	set(command.MetaContentDisp, in.ContentDisposition)
	set(command.MetaCacheControl, in.CacheControl)
	set(command.MetaExpires, in.Expires)
	for k, v := range in.UserMeta {
		m["x-amz-meta-"+k] = v
	}
	if len(in.Tags) > 0 {
		if b, err := json.Marshal(in.Tags); err == nil {
			m[command.MetaTags] = string(b)
		}
	}
	if in.RetentionMode != "" && !in.RetainUntil.IsZero() {
		m[command.MetaRetentionMode] = in.RetentionMode
		m[command.MetaRetainUntil] = in.RetainUntil.UTC().Format(time.RFC3339Nano)
	}
	if in.LegalHold != "" {
		m[command.MetaLegalHold] = in.LegalHold
	}
	return m
}

// ---- DeleteObject ----

// DeleteResult reports the outcome of a delete.
type DeleteResult struct {
	VersionID    string
	DeleteMarker bool
	Index        uint64
}

func (g *Gateway) DeleteObject(ctx Ctx, bucket, key, versionID string) (*DeleteResult, error) {
	verEnabled, _, err := g.bucketVersioning(bucket)
	if err != nil {
		return nil, err
	}
	c := &command.Command{Op: command.OpDeleteObject, Bucket: bucket, Key: key, IssuedBy: ctx.Account, Meta: map[string]string{}}
	res := &DeleteResult{}
	if versionID != "" {
		c.VersionID = versionID
		c.Meta["explicit-version"] = "true"
		res.VersionID = versionID
	} else if verEnabled {
		c.VersionID = newID()
		res.VersionID = c.VersionID
		res.DeleteMarker = true
	}
	idx, err := g.submit(c)
	if err != nil {
		return nil, err
	}
	res.Index = idx
	name := "s3:ObjectRemoved:Delete"
	if res.DeleteMarker {
		name = "s3:ObjectRemoved:DeleteMarkerCreated"
	}
	g.fire(s3event.Event{EventName: name, Bucket: bucket, Key: key, VersionID: res.VersionID, Requester: ctx.Account, SourceIP: ctx.SourceIP})
	return res, nil
}

// ---- CopyObject ----

// CopyResult is returned after a server-side copy.
type CopyResult struct {
	ETag      string
	MTime     time.Time
	VersionID string
	Index     uint64
}

// CopyObjectInput parameterizes CopyObject.
type CopyObjectInput struct {
	SrcBucket, SrcKey, SrcVersionID string
	DstBucket, DstKey               string
	MetadataDirective               string // COPY | REPLACE
	TaggingDirective                string // COPY | REPLACE
	ContentType                     string
	UserMeta                        map[string]string
	ACL                             *types.ACL
}

func (g *Gateway) CopyObject(ctx Ctx, in CopyObjectInput) (*CopyResult, error) {
	verEnabled, _, err := g.bucketVersioning(in.DstBucket)
	if err != nil {
		return nil, err
	}
	src, err := g.cl.HeadObject(in.SrcBucket, in.SrcKey, in.SrcVersionID)
	if err != nil {
		return nil, err
	}
	mtime := nowFn().UTC()
	versionID := newID()
	c := &command.Command{
		Op: command.OpCopyObject, Bucket: in.DstBucket, Key: in.DstKey, VersionID: versionID,
		BlobToken: newID(), ETag: src.ETag, Size: src.Size, IssuedBy: ctx.Account,
		Source: &command.ObjectRef{Bucket: in.SrcBucket, Key: in.SrcKey, VersionID: in.SrcVersionID},
		Meta:   map[string]string{command.MetaMTime: mtime.Format(time.RFC3339Nano)},
	}
	if in.MetadataDirective == "REPLACE" {
		c.Meta["metadata-directive"] = "REPLACE"
		if in.ContentType != "" {
			c.Meta[command.MetaContentType] = in.ContentType
		}
		for k, v := range in.UserMeta {
			c.Meta["x-amz-meta-"+k] = v
		}
	}
	if in.TaggingDirective == "REPLACE" {
		c.Meta["tagging-directive"] = "REPLACE"
	}
	if in.ACL != nil {
		c.Config, _ = json.Marshal(in.ACL)
	}
	idx, err := g.submit(c)
	if err != nil {
		return nil, err
	}
	vv := visibleVersion(verEnabled, versionID)
	g.fire(s3event.Event{EventName: "s3:ObjectCreated:Copy", Bucket: in.DstBucket, Key: in.DstKey, VersionID: vv, ETag: src.ETag, Size: src.Size, Requester: ctx.Account, SourceIP: ctx.SourceIP})
	return &CopyResult{ETag: src.ETag, MTime: mtime, VersionID: vv, Index: idx}, nil
}

// ---- reads (delegated) ----

func (g *Gateway) ListBuckets() ([]types.BucketMeta, error) { return g.cl.ListBuckets() }
func (g *Gateway) GetBucketMeta(bucket string) (*types.BucketMeta, error) {
	return g.cl.GetBucketMeta(bucket)
}
func (g *Gateway) HeadObject(bucket, key, version string) (*types.ObjectMeta, error) {
	return g.cl.HeadObject(bucket, key, version)
}
func (g *Gateway) GetObject(bucket, key string, opt types.GetOptions) (*types.GetResult, error) {
	return g.cl.GetObject(bucket, key, opt)
}
func (g *Gateway) ListObjectsV2(in types.ListInput) (*types.ListResult, error) {
	return g.cl.ListObjectsV2(in)
}
func (g *Gateway) ListObjectVersions(in types.ListVersionsInput) (*types.ListVersionsResult, error) {
	return g.cl.ListObjectVersions(in)
}

func (g *Gateway) fire(ev s3event.Event) {
	ev.Time = nowFn().UTC()
	g.events.Notify(ev)
}
