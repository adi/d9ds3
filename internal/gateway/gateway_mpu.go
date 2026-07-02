package gateway

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/s3event"
	"github.com/adi/d9ds3/internal/types"
)

// CreateMultipartUpload initiates an upload and returns its id.
func (g *Gateway) CreateMultipartUpload(ctx Ctx, in PutObjectInput) (string, error) {
	uploadID := newID()
	c := &command.Command{
		Op: command.OpCreateMultipart, Bucket: in.Bucket, Key: in.Key, UploadID: uploadID,
		StorageCls: in.StorageClass, IssuedBy: ctx.Account, Meta: g.contentMeta(in),
	}
	if in.ACL != nil {
		c.Config, _ = json.Marshal(in.ACL)
	}
	if _, err := g.submit(c); err != nil {
		return "", err
	}
	return uploadID, nil
}

// UploadPart stages and records one part; returns its ETag.
func (g *Gateway) UploadPart(ctx Ctx, bucket, key, uploadID string, partNumber int, body io.Reader) (string, error) {
	token := newID()
	tmpPath := filepath.Join(g.stageDir, token)
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpPath)
	h := md5.New()
	size, err := io.Copy(tmp, io.TeeReader(body, h))
	tmp.Close()
	if err != nil {
		return "", err
	}
	etag := `"` + hex.EncodeToString(h.Sum(nil)) + `"`
	if err := g.cl.FanOutPart(uploadID, partNumber, tmpPath); err != nil {
		return "", err
	}
	c := &command.Command{
		Op: command.OpUploadPart, Bucket: bucket, Key: key, UploadID: uploadID,
		PartNumber: partNumber, ETag: etag, Size: size, IssuedBy: ctx.Account,
	}
	if _, err := g.submit(c); err != nil {
		return "", err
	}
	return etag, nil
}

// UploadPartCopy copies a byte range of a source object into a part.
func (g *Gateway) UploadPartCopy(ctx Ctx, bucket, key, uploadID string, partNumber int, src CopyObjectInput, rng *types.ByteRange) (string, time.Time, error) {
	res, err := g.cl.GetObject(src.SrcBucket, src.SrcKey, types.GetOptions{VersionID: src.SrcVersionID, Range: rng})
	if err != nil {
		return "", time.Time{}, err
	}
	defer res.Body.Close()
	etag, err := g.UploadPart(ctx, bucket, key, uploadID, partNumber, res.Body)
	return etag, nowFn().UTC(), err
}

// CompleteResult is returned after completing a multipart upload.
type CompleteResult struct {
	ETag      string
	VersionID string
	Index     uint64
}

// CompleteMultipartUpload assembles the parts into an object.
func (g *Gateway) CompleteMultipartUpload(ctx Ctx, bucket, key, uploadID string, parts []command.CompletedPart) (*CompleteResult, error) {
	verEnabled, _, err := g.bucketVersioning(bucket)
	if err != nil {
		return nil, err
	}
	versionID := newID()
	c := &command.Command{
		Op: command.OpCompleteMultipart, Bucket: bucket, Key: key, UploadID: uploadID,
		VersionID: versionID, BlobToken: newID(), Parts: parts, IssuedBy: ctx.Account,
	}
	idx, err := g.submit(c)
	if err != nil {
		return nil, err
	}
	etag := multipartETag(parts)
	vv := visibleVersion(verEnabled, versionID)
	g.fire(s3event.Event{EventName: "s3:ObjectCreated:CompleteMultipartUpload", Bucket: bucket, Key: key, VersionID: vv, ETag: etag, Requester: ctx.Account, SourceIP: ctx.SourceIP})
	return &CompleteResult{ETag: etag, VersionID: vv, Index: idx}, nil
}

// AbortMultipartUpload discards an in-progress upload.
func (g *Gateway) AbortMultipartUpload(ctx Ctx, bucket, key, uploadID string) (uint64, error) {
	return g.submit(&command.Command{
		Op: command.OpAbortMultipart, Bucket: bucket, Key: key, UploadID: uploadID, IssuedBy: ctx.Account,
	})
}

// GetMultipartUpload / ListMultipartUploads delegate to the cluster.
func (g *Gateway) GetMultipartUpload(bucket, uploadID string) (*types.MultipartUpload, error) {
	return g.cl.GetMultipartUpload(bucket, uploadID)
}
func (g *Gateway) ListMultipartUploads(bucket, prefix string) ([]types.MultipartUpload, error) {
	return g.cl.ListMultipartUploads(bucket, prefix)
}
