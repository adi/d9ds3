package storage

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

// MinPartSize is the S3 minimum size for every multipart part except the last.
const MinPartSize = 5 * 1024 * 1024

func (b *posixBackend) readMPU(bucket, uploadID string) (*types.MultipartUpload, error) {
	var mp types.MultipartUpload
	if err := readJSON(b.mpuMetaPath(bucket, uploadID), &mp); err != nil {
		return nil, s3err.ErrNoSuchUpload
	}
	return &mp, nil
}

func (b *posixBackend) applyCreateMultipart(c *command.Command) error {
	if _, err := b.readBucketMeta(c.Bucket); err != nil {
		return err
	}
	mp := types.MultipartUpload{
		Bucket:       c.Bucket,
		Key:          c.Key,
		UploadID:     c.UploadID,
		Initiated:    mtime(c),
		Owner:        c.IssuedBy,
		StorageClass: c.StorageCls,
		ContentType:  c.Meta[command.MetaContentType],
		UserMeta:     userMetaFrom(c.Meta),
	}
	if len(c.Config) > 0 {
		var acl types.ACL
		if json.Unmarshal(c.Config, &acl) == nil {
			mp.ACL = &acl
		}
	}
	return writeJSON(b.mpuMetaPath(c.Bucket, c.UploadID), mp)
}

func (b *posixBackend) applyUploadPart(c *command.Command) error {
	if _, err := os.Stat(b.mpPartPath(c.UploadID, c.PartNumber)); err != nil {
		return ErrBlobMissing
	}
	mp, err := b.readMPU(c.Bucket, c.UploadID)
	if err != nil {
		return err
	}
	part := types.Part{PartNumber: c.PartNumber, ETag: c.ETag, Size: c.Size, LastModified: mtime(c)}
	replaced := false
	for i := range mp.Parts {
		if mp.Parts[i].PartNumber == c.PartNumber {
			mp.Parts[i] = part
			replaced = true
			break
		}
	}
	if !replaced {
		mp.Parts = append(mp.Parts, part)
	}
	sort.Slice(mp.Parts, func(i, j int) bool { return mp.Parts[i].PartNumber < mp.Parts[j].PartNumber })
	return writeJSON(b.mpuMetaPath(c.Bucket, c.UploadID), *mp)
}

func (b *posixBackend) applyCompleteMultipart(c *command.Command) error {
	bm, err := b.readBucketMeta(c.Bucket)
	if err != nil {
		return err
	}
	mp, err := b.readMPU(c.Bucket, c.UploadID)
	if err != nil {
		return err
	}

	// Resolve the requested parts against what was uploaded, in ascending order.
	byNum := map[int]types.Part{}
	for _, p := range mp.Parts {
		byNum[p.PartNumber] = p
	}
	ordered := make([]types.Part, 0, len(c.Parts))
	prev := 0
	for _, want := range c.Parts {
		if want.PartNumber <= prev {
			return s3err.ErrInvalidPartOrder
		}
		prev = want.PartNumber
		got, ok := byNum[want.PartNumber]
		if !ok || !etagEqual(got.ETag, want.ETag) {
			return s3err.ErrInvalidPart
		}
		ordered = append(ordered, got)
	}
	if len(ordered) == 0 {
		return s3err.ErrInvalidPart
	}
	for i, p := range ordered {
		if i < len(ordered)-1 && p.Size < MinPartSize {
			return s3err.ErrEntityTooSmall
		}
	}

	// Ensure every part payload is present locally (pull-fallback handled by FSM).
	for _, p := range ordered {
		if _, err := os.Stat(b.mpPartPath(c.UploadID, p.PartNumber)); err != nil {
			return ErrBlobMissing
		}
	}

	// Assemble the parts into a staging temp, computing the multipart ETag.
	tmp := b.stagingPath(c.BlobToken)
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	digest := md5.New()
	var total int64
	for _, p := range ordered {
		in, err := os.Open(b.mpPartPath(c.UploadID, p.PartNumber))
		if err != nil {
			out.Close()
			os.Remove(tmp)
			return err
		}
		n, err := io.Copy(out, in)
		in.Close()
		if err != nil {
			out.Close()
			os.Remove(tmp)
			return err
		}
		total += n
		if raw, derr := hex.DecodeString(strings.Trim(p.ETag, `"`)); derr == nil {
			digest.Write(raw)
		}
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	etag := `"` + hex.EncodeToString(digest.Sum(nil)) + "-" + strconv.Itoa(len(ordered)) + `"`

	// Install the assembled object as the new current version (browsable key file).
	km := b.loadKeyMetaOrNew(c.Bucket, c.Key)
	if err := b.displaceCurrent(km, bm.VersioningEnabled()); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := b.putCurrentBytes(c.Bucket, c.Key, tmp); err != nil {
		return err
	}
	vid := "null"
	if bm.VersioningEnabled() {
		vid = c.VersionID
	}
	nv := types.ObjectMeta{
		Bucket: c.Bucket, Key: c.Key, VersionID: vid, BlobID: "",
		ETag: etag, Size: total, ContentType: mp.ContentType, UserMeta: mp.UserMeta,
		StorageClass: mp.StorageClass, LastModified: mtime(c), ACL: mp.ACL,
	}
	applyLockDefaults(bm, &nv, c)
	prependLatest(km, nv)
	if err := b.writeKeyMeta(km); err != nil {
		return err
	}
	b.cleanupUpload(c.Bucket, c.UploadID)
	return nil
}

func (b *posixBackend) applyAbortMultipart(c *command.Command) error {
	if _, err := b.readMPU(c.Bucket, c.UploadID); err != nil {
		return nil // already gone: idempotent
	}
	b.cleanupUpload(c.Bucket, c.UploadID)
	return nil
}

func (b *posixBackend) cleanupUpload(bucket, uploadID string) {
	// Remove every staged part for this upload (glob covers parts not in the list).
	matches, _ := filepath.Glob(filepath.Join(b.root, "mpstaging", uploadID+"__*"))
	for _, m := range matches {
		os.Remove(m)
	}
	os.Remove(b.mpuMetaPath(bucket, uploadID))
}

// ---- multipart reads ----

func (b *posixBackend) GetMultipartUpload(bucket, uploadID string) (*types.MultipartUpload, error) {
	return b.readMPU(bucket, uploadID)
}

func (b *posixBackend) ListMultipartUploads(bucket, prefix string) ([]types.MultipartUpload, error) {
	if _, err := b.readBucketMeta(bucket); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(b.root, "mpu", bucket))
	if err != nil {
		return nil, nil
	}
	var out []types.MultipartUpload
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var mp types.MultipartUpload
		if readJSON(filepath.Join(b.root, "mpu", bucket, e.Name()), &mp) == nil {
			if prefix == "" || strings.HasPrefix(mp.Key, prefix) {
				out = append(out, mp)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].UploadID < out[j].UploadID
	})
	return out, nil
}

func etagEqual(a, b string) bool {
	return strings.Trim(a, `"`) == strings.Trim(b, `"`)
}
