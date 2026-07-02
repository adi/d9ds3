package storage

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

// ---- KeyMeta helpers ----

func (b *posixBackend) readKeyMeta(bucket, key string) (*types.KeyMeta, error) {
	p, err := b.keyMetaPath(bucket, key)
	if err != nil {
		return nil, err
	}
	var km types.KeyMeta
	if err := readJSON(p, &km); err != nil {
		if os.IsNotExist(err) {
			return nil, s3err.ErrNoSuchKey
		}
		return nil, err
	}
	return &km, nil
}

func (b *posixBackend) writeKeyMeta(km *types.KeyMeta) error {
	p, err := b.keyMetaPath(km.Bucket, km.Key)
	if err != nil {
		return err
	}
	return writeJSON(p, *km)
}

func (b *posixBackend) deleteKeyMeta(bucket, key string) error {
	p, err := b.keyMetaPath(bucket, key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (b *posixBackend) removeBlob(bucket, blobID string) {
	if blobID != "" {
		os.Remove(b.vblobPath(bucket, blobID))
	}
}

// insertVersion prepends a new version to a key's history, honoring versioning.
// When versioning is not Enabled the new version replaces the "null" version.
func (b *posixBackend) insertVersion(km *types.KeyMeta, nv types.ObjectMeta, versioningEnabled bool) {
	if !versioningEnabled {
		nv.VersionID = "null"
		kept := km.Versions[:0]
		for _, v := range km.Versions {
			if v.VersionID == "null" {
				b.removeBlob(km.Bucket, v.BlobID)
				continue
			}
			kept = append(kept, v)
		}
		km.Versions = kept
	}
	for i := range km.Versions {
		km.Versions[i].IsLatest = false
	}
	nv.IsLatest = true
	km.Versions = append([]types.ObjectMeta{nv}, km.Versions...)
}

// ---- PutObject ----

func (b *posixBackend) applyPutObject(c *command.Command) error {
	bm, err := b.readBucketMeta(c.Bucket)
	if err != nil {
		return err
	}
	staged := b.stagingPath(c.BlobToken)
	if _, err := os.Stat(staged); err != nil {
		return ErrBlobMissing
	}
	if err := os.MkdirAll(filepath.Join(b.root, "vstore", c.Bucket), 0o755); err != nil {
		return err
	}
	if err := os.Rename(staged, b.vblobPath(c.Bucket, c.BlobToken)); err != nil {
		return err
	}

	nv := metaFromCommand(c, c.Size, c.ETag, c.VersionID)
	applyLockDefaults(bm, &nv, c)
	applyPutExtras(&nv, c)

	km, err := b.readKeyMeta(c.Bucket, c.Key)
	if err != nil {
		km = &types.KeyMeta{Bucket: c.Bucket, Key: c.Key}
	}
	b.insertVersion(km, nv, bm.VersioningEnabled())
	return b.writeKeyMeta(km)
}

// applyPutExtras attaches ACL (Config), tags, retention, and legal-hold supplied
// on a PutObject/Copy/Complete command to the new version.
func applyPutExtras(nv *types.ObjectMeta, c *command.Command) {
	if len(c.Config) > 0 {
		var acl types.ACL
		if json.Unmarshal(c.Config, &acl) == nil {
			nv.ACL = &acl
		}
	}
	if s := c.Meta[command.MetaTags]; s != "" {
		var tags map[string]string
		if json.Unmarshal([]byte(s), &tags) == nil {
			nv.Tags = tags
		}
	}
	if mode := c.Meta[command.MetaRetentionMode]; mode != "" {
		if t, err := time.Parse(time.RFC3339Nano, c.Meta[command.MetaRetainUntil]); err == nil {
			nv.Retention = &types.Retention{Mode: mode, RetainUntil: t}
		}
	}
	if lh := c.Meta[command.MetaLegalHold]; lh != "" {
		nv.LegalHold = &types.LegalHold{Status: lh}
	}
}

// ---- DeleteObject ----

func (b *posixBackend) applyDeleteObject(c *command.Command) error {
	bm, err := b.readBucketMeta(c.Bucket)
	if err != nil {
		return err
	}
	km, err := b.readKeyMeta(c.Bucket, c.Key)
	if err != nil {
		// Deleting a nonexistent key: create a delete marker if versioning is on,
		// otherwise it is a no-op success.
		if s3err.From(err).Code == s3err.ErrNoSuchKey.Code && bm.VersioningEnabled() && c.VersionID != "" {
			km = &types.KeyMeta{Bucket: c.Bucket, Key: c.Key}
		} else {
			return nil
		}
	}

	// Delete a specific version.
	if c.VersionID != "" && c.Meta["explicit-version"] == "true" {
		kept := km.Versions[:0]
		removed := false
		for _, v := range km.Versions {
			if v.VersionID == c.VersionID {
				b.removeBlob(c.Bucket, v.BlobID)
				removed = true
				continue
			}
			kept = append(kept, v)
		}
		km.Versions = kept
		if removed && len(km.Versions) > 0 {
			km.Versions[0].IsLatest = true
		}
		if len(km.Versions) == 0 {
			return b.deleteKeyMeta(c.Bucket, c.Key)
		}
		return b.writeKeyMeta(km)
	}

	// Delete latest: create a delete marker (versioned) or drop the null version.
	if bm.VersioningEnabled() {
		marker := types.ObjectMeta{
			Bucket: c.Bucket, Key: c.Key, VersionID: c.VersionID,
			DeleteMarker: true, LastModified: mtime(c),
		}
		b.insertVersion(km, marker, true)
		return b.writeKeyMeta(km)
	}
	// Unversioned / suspended: remove the null version.
	kept := km.Versions[:0]
	for _, v := range km.Versions {
		if v.VersionID == "null" {
			b.removeBlob(c.Bucket, v.BlobID)
			continue
		}
		kept = append(kept, v)
	}
	km.Versions = kept
	if bm.Versioning == "Suspended" {
		// Suspended delete creates a null delete marker.
		marker := types.ObjectMeta{Bucket: c.Bucket, Key: c.Key, VersionID: "null", DeleteMarker: true, LastModified: mtime(c)}
		b.insertVersion(km, marker, false)
		return b.writeKeyMeta(km)
	}
	if len(km.Versions) == 0 {
		return b.deleteKeyMeta(c.Bucket, c.Key)
	}
	km.Versions[0].IsLatest = true
	return b.writeKeyMeta(km)
}

// ---- CopyObject ----

func (b *posixBackend) applyCopyObject(c *command.Command) error {
	if c.Source == nil {
		return s3err.ErrInvalidKey
	}
	bm, err := b.readBucketMeta(c.Bucket)
	if err != nil {
		return err
	}
	src, srcMeta, err := b.resolveVersion(c.Source.Bucket, c.Source.Key, c.Source.VersionID)
	if err != nil {
		return err
	}
	if src.DeleteMarker {
		return s3err.ErrNoSuchKey
	}

	// Copy the source blob into a new version blob.
	if err := os.MkdirAll(filepath.Join(b.root, "vstore", c.Bucket), 0o755); err != nil {
		return err
	}
	if err := copyFile(b.vblobPath(c.Source.Bucket, src.BlobID), b.vblobPath(c.Bucket, c.BlobToken)); err != nil {
		return err
	}
	_ = srcMeta

	nv := src // start from source metadata
	nv.Bucket, nv.Key, nv.VersionID = c.Bucket, c.Key, c.VersionID
	nv.BlobID = c.BlobToken
	nv.LastModified = mtime(c)
	nv.IsLatest = false
	nv.ACL = nil
	if c.Meta["metadata-directive"] == "REPLACE" {
		replacement := metaFromCommand(c, src.Size, src.ETag, c.VersionID)
		replacement.BlobID = c.BlobToken
		replacement.StorageClass = nv.StorageClass
		nv = replacement
	}
	if c.Meta["tagging-directive"] == "REPLACE" {
		nv.Tags = nil // caller sets tags via a follow-up PutObjectTagging if needed
	}
	applyLockDefaults(bm, &nv, c)

	km, err := b.readKeyMeta(c.Bucket, c.Key)
	if err != nil {
		km = &types.KeyMeta{Bucket: c.Bucket, Key: c.Key}
	}
	b.insertVersion(km, nv, bm.VersioningEnabled())
	return b.writeKeyMeta(km)
}

// ---- reads ----

// resolveVersion returns the requested version (or latest) plus its KeyMeta.
func (b *posixBackend) resolveVersion(bucket, key, versionID string) (types.ObjectMeta, *types.KeyMeta, error) {
	if _, err := b.readBucketMeta(bucket); err != nil {
		return types.ObjectMeta{}, nil, err
	}
	km, err := b.readKeyMeta(bucket, key)
	if err != nil {
		return types.ObjectMeta{}, nil, err
	}
	if versionID != "" {
		for _, v := range km.Versions {
			if v.VersionID == versionID {
				return v, km, nil
			}
		}
		return types.ObjectMeta{}, nil, s3err.ErrNoSuchVersion
	}
	latest := km.Latest()
	if latest == nil {
		return types.ObjectMeta{}, nil, s3err.ErrNoSuchKey
	}
	return *latest, km, nil
}

func (b *posixBackend) HeadObject(bucket, key, versionID string) (*types.ObjectMeta, error) {
	v, _, err := b.resolveVersion(bucket, key, versionID)
	if err != nil {
		return nil, err
	}
	if v.DeleteMarker {
		if versionID == "" {
			return nil, s3err.ErrNoSuchKey
		}
		return nil, s3err.ErrMethodNotAllowed
	}
	return &v, nil
}

func (b *posixBackend) GetObject(bucket, key string, opt types.GetOptions) (*types.GetResult, error) {
	v, _, err := b.resolveVersion(bucket, key, opt.VersionID)
	if err != nil {
		return nil, err
	}
	if v.DeleteMarker {
		if opt.VersionID == "" {
			return nil, s3err.ErrNoSuchKey
		}
		return nil, s3err.ErrMethodNotAllowed
	}
	f, err := os.Open(b.vblobPath(bucket, v.BlobID))
	if err != nil {
		return nil, s3err.ErrNoSuchKey
	}
	res := &types.GetResult{Info: v, Body: f, PartialLength: v.Size}
	if opt.Range != nil {
		start, end := opt.Range.Start, opt.Range.End
		if start < 0 { // suffix range: the last (-start) bytes
			n := -start
			if n > v.Size {
				n = v.Size
			}
			start = v.Size - n
			end = v.Size - 1
		} else if end < 0 || end >= v.Size {
			end = v.Size - 1
		}
		if start < 0 || start >= v.Size || start > end {
			f.Close()
			return nil, s3err.ErrInvalidRange
		}
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
		res.Body = limitedReadCloser(f, end-start+1)
		res.PartialLength = end - start + 1
		res.IsRange = true
		res.ContentRange = "bytes " + itoa(start) + "-" + itoa(end) + "/" + itoa(v.Size)
	}
	return res, nil
}

// ---- listings ----

func (b *posixBackend) walkKeys(bucket string) ([]string, error) {
	root := filepath.Join(b.root, "keys", bucket)
	var keys []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".d9k") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		keys = append(keys, filepath.ToSlash(strings.TrimSuffix(rel, ".d9k")))
		return nil
	})
	sort.Strings(keys)
	return keys, err
}

func (b *posixBackend) ListObjectsV2(in types.ListInput) (*types.ListResult, error) {
	if _, err := b.readBucketMeta(in.Bucket); err != nil {
		return nil, err
	}
	keys, err := b.walkKeys(in.Bucket)
	if err != nil {
		return nil, err
	}
	max := clampMax(in.MaxKeys)
	res := &types.ListResult{}
	seen := map[string]bool{}
	after := in.ContinuationToken
	if after == "" {
		after = in.StartAfter
	}
	if in.Marker != "" {
		after = in.Marker
	}
	count := 0
	lastEmitted := "" // display token of the last emitted result; the continuation cursor
	truncate := func() *types.ListResult {
		res.IsTruncated, res.NextToken, res.NextMarker = true, lastEmitted, lastEmitted
		return res
	}
	for _, key := range keys {
		if in.Prefix != "" && !strings.HasPrefix(key, in.Prefix) {
			continue
		}
		// The "display token" is the common-prefix string this key rolls into
		// under the delimiter, else the key itself. Ordering, dedup, and
		// continuation all use the display token so delimiter + pagination compose.
		display, isPrefix := displayToken(key, in.Prefix, in.Delimiter)
		if after != "" && display <= after {
			continue
		}
		if isPrefix {
			if seen[display] {
				continue
			}
			if count >= max {
				return truncate(), nil
			}
			seen[display] = true
			res.CommonPrefixes = append(res.CommonPrefixes, display)
			lastEmitted = display
			count++
			continue
		}
		km, err := b.readKeyMeta(in.Bucket, key)
		if err != nil {
			continue
		}
		latest := km.Latest()
		if latest == nil || latest.DeleteMarker {
			continue
		}
		if count >= max {
			return truncate(), nil
		}
		res.Objects = append(res.Objects, *latest)
		lastEmitted = key
		count++
	}
	return res, nil
}

func (b *posixBackend) ListObjectVersions(in types.ListVersionsInput) (*types.ListVersionsResult, error) {
	if _, err := b.readBucketMeta(in.Bucket); err != nil {
		return nil, err
	}
	keys, err := b.walkKeys(in.Bucket)
	if err != nil {
		return nil, err
	}
	max := clampMax(in.MaxKeys)
	res := &types.ListVersionsResult{}
	seen := map[string]bool{}
	count := 0
	// When a VersionIDMarker is present the marker key is re-scanned and versions
	// are emitted starting AT that version id (inclusive resume); otherwise the
	// marker key is skipped entirely.
	resumeInclusive := in.KeyMarker != "" && in.VersionIDMarker != ""
	for _, key := range keys {
		if in.Prefix != "" && !strings.HasPrefix(key, in.Prefix) {
			continue
		}
		display, isPrefix := displayToken(key, in.Prefix, in.Delimiter)
		if in.KeyMarker != "" {
			if display < in.KeyMarker {
				continue
			}
			if display == in.KeyMarker && !(resumeInclusive && !isPrefix && key == in.KeyMarker) {
				continue // marker already fully returned
			}
		}
		if isPrefix {
			if seen[display] {
				continue
			}
			if count >= max {
				res.IsTruncated, res.NextKeyMarker = true, display
				return res, nil
			}
			seen[display] = true
			res.CommonPrefixes = append(res.CommonPrefixes, display)
			count++
			continue
		}
		km, err := b.readKeyMeta(in.Bucket, key)
		if err != nil {
			continue
		}
		reached := !(resumeInclusive && key == in.KeyMarker)
		for _, v := range km.Versions {
			if !reached {
				if v.VersionID == in.VersionIDMarker {
					reached = true // emit from this version onward (inclusive)
				} else {
					continue
				}
			}
			if count >= max {
				res.IsTruncated, res.NextKeyMarker, res.NextVersionIDMarker = true, key, v.VersionID
				return res, nil
			}
			if v.DeleteMarker {
				res.DeleteMarkers = append(res.DeleteMarkers, v)
			} else {
				res.Versions = append(res.Versions, v)
			}
			count++
		}
	}
	return res, nil
}

// ---- command → ObjectMeta ----

func metaFromCommand(c *command.Command, size int64, etag, versionID string) types.ObjectMeta {
	m := types.ObjectMeta{
		Bucket:             c.Bucket,
		Key:                c.Key,
		VersionID:          versionID,
		BlobID:             c.BlobToken,
		ETag:               etag,
		Size:               size,
		ContentType:        c.Meta[command.MetaContentType],
		ContentEncoding:    c.Meta[command.MetaContentEncoding],
		ContentLanguage:    c.Meta[command.MetaContentLang],
		ContentDisposition: c.Meta[command.MetaContentDisp],
		CacheControl:       c.Meta[command.MetaCacheControl],
		Expires:            c.Meta[command.MetaExpires],
		StorageClass:       c.StorageCls,
		LastModified:       mtime(c),
		UserMeta:           userMetaFrom(c.Meta),
	}
	return m
}

// applyLockDefaults stamps bucket default object-lock retention onto a new version.
func applyLockDefaults(bm *types.BucketMeta, nv *types.ObjectMeta, c *command.Command) {
	if bm.ObjectLock == nil || !bm.ObjectLock.Enabled || bm.ObjectLock.DefaultMode == "" {
		return
	}
	if nv.Retention != nil {
		return
	}
	until := mtime(c)
	if bm.ObjectLock.DefaultDays > 0 {
		until = until.AddDate(0, 0, bm.ObjectLock.DefaultDays)
	}
	if bm.ObjectLock.DefaultYrs > 0 {
		until = until.AddDate(bm.ObjectLock.DefaultYrs, 0, 0)
	}
	nv.Retention = &types.Retention{Mode: bm.ObjectLock.DefaultMode, RetainUntil: until}
}

func userMetaFrom(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		if strings.HasPrefix(k, "x-amz-meta-") {
			out[strings.TrimPrefix(k, "x-amz-meta-")] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func mtime(c *command.Command) time.Time {
	if s := c.Meta[command.MetaMTime]; s != "" {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func clampMax(n int) int {
	if n <= 0 || n > 1000 {
		return 1000
	}
	return n
}

// displayToken returns the listing position for a key: the common-prefix string
// it rolls into under the delimiter (isPrefix=true), or the key itself.
func displayToken(key, prefix, delimiter string) (string, bool) {
	if delimiter == "" {
		return key, false
	}
	rest := key[len(prefix):]
	if idx := strings.Index(rest, delimiter); idx >= 0 {
		return prefix + rest[:idx+len(delimiter)], true
	}
	return key, false
}
