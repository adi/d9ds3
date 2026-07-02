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

// loadKeyMeta returns a key's history, synthesizing it from a prefilled object
// file when there is no sidecar (so a disk seeded with plain files just works).
func (b *posixBackend) loadKeyMeta(bucket, key string) (*types.KeyMeta, error) {
	km, err := b.readKeyMeta(bucket, key)
	if err == nil {
		return km, nil
	}
	if err == s3err.ErrNoSuchKey {
		if syn := b.synthesizeKeyMeta(bucket, key); syn != nil {
			return syn, nil
		}
	}
	return nil, err
}

// loadKeyMetaOrNew is loadKeyMeta but returns a fresh empty history if absent.
// It clears the Synthesized flag: callers are about to apply a replicated write,
// so the key becomes first-class replicated state (and thus reconciled by restore).
func (b *posixBackend) loadKeyMetaOrNew(bucket, key string) *types.KeyMeta {
	if km, err := b.loadKeyMeta(bucket, key); err == nil {
		km.Synthesized = false
		return km
	}
	return &types.KeyMeta{Bucket: bucket, Key: key}
}

// synthesizeKeyMeta builds metadata for a plain object file that has no sidecar
// (a prefilled file). Returns nil if no such file exists. The result is cached as
// a sidecar (best-effort) so subsequent reads skip re-hashing.
func (b *posixBackend) synthesizeKeyMeta(bucket, key string) *types.KeyMeta {
	op, err := b.objectPath(bucket, key)
	if err != nil {
		return nil
	}
	fi, err := os.Stat(op)
	if err != nil || fi.IsDir() {
		return nil
	}
	km := &types.KeyMeta{Bucket: bucket, Key: key, Synthesized: true, Versions: []types.ObjectMeta{{
		Bucket: bucket, Key: key, VersionID: "null", BlobID: "",
		Size: fi.Size(), ETag: md5File(op), ContentType: guessContentType(key),
		LastModified: fi.ModTime().UTC(), IsLatest: true,
	}}}
	b.writeKeyMeta(km) // best-effort cache; ignored on read-only fs
	return km
}

// physPath returns the on-disk path of a version's bytes: the browsable key file
// for the current version (BlobID==""), else the archived version blob.
func (b *posixBackend) physPath(bucket string, v types.ObjectMeta) (string, error) {
	if v.BlobID == "" {
		return b.objectPath(bucket, v.Key)
	}
	return b.vblobPath(bucket, v.BlobID), nil
}

// prependLatest inserts nv as the new current (latest) version.
func prependLatest(km *types.KeyMeta, nv types.ObjectMeta) {
	for i := range km.Versions {
		km.Versions[i].IsLatest = false
	}
	nv.IsLatest = true
	km.Versions = append([]types.ObjectMeta{nv}, km.Versions...)
}

// displaceCurrent makes room at the key path for a new current version: it either
// archives the existing current file (preserving it as a non-current version) or,
// for an unversioned "null" overwrite, drops it (its bytes will be overwritten).
func (b *posixBackend) displaceCurrent(km *types.KeyMeta, versioningEnabled bool) error {
	cur := km.Latest()
	if cur == nil || cur.DeleteMarker || cur.BlobID != "" {
		return nil // nothing currently at the key path
	}
	if versioningEnabled || cur.VersionID != "null" {
		if err := b.archiveCurrent(km.Bucket, km.Key, cur.VersionID); err != nil {
			return err
		}
		cur.BlobID = cur.VersionID // now lives in the version store
		return nil
	}
	km.Versions = km.Versions[1:] // discard the null version being overwritten
	return nil
}

// finalizeKey persists (or removes) a key's history; the object file itself is
// managed by the caller.
func (b *posixBackend) finalizeKey(km *types.KeyMeta) error {
	if len(km.Versions) == 0 {
		b.removeCurrent(km.Bucket, km.Key)
		return b.deleteKeyMeta(km.Bucket, km.Key)
	}
	return b.writeKeyMeta(km)
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
	km := b.loadKeyMetaOrNew(c.Bucket, c.Key)
	if err := b.displaceCurrent(km, bm.VersioningEnabled()); err != nil {
		return err
	}
	if err := b.putCurrentBytes(c.Bucket, c.Key, staged); err != nil {
		return err
	}
	vid := "null"
	if bm.VersioningEnabled() {
		vid = c.VersionID
	}
	nv := metaFromCommand(c, c.Size, c.ETag, vid)
	nv.BlobID = "" // current version lives as the browsable key file
	applyLockDefaults(bm, &nv, c)
	applyPutExtras(&nv, c)
	prependLatest(km, nv)
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
	km, err := b.loadKeyMeta(c.Bucket, c.Key)
	if err != nil {
		// Deleting a nonexistent key: create a delete marker if versioning is on,
		// otherwise it is a no-op success.
		if err == s3err.ErrNoSuchKey && bm.VersioningEnabled() && c.VersionID != "" {
			km = &types.KeyMeta{Bucket: c.Bucket, Key: c.Key}
		} else {
			return nil
		}
	}
	km.Synthesized = false // an explicit S3 delete is a replicated change

	// Delete a specific version.
	if c.VersionID != "" && c.Meta["explicit-version"] == "true" {
		kept := km.Versions[:0]
		removedCurrent := false
		for _, v := range km.Versions {
			if v.VersionID == c.VersionID {
				if v.BlobID == "" && !v.DeleteMarker {
					removedCurrent = true // its bytes are the key-path file
				} else {
					b.removeArchivedBlob(c.Bucket, v.BlobID)
				}
				continue
			}
			kept = append(kept, v)
		}
		km.Versions = kept
		if removedCurrent {
			b.removeCurrent(c.Bucket, c.Key)
			b.promoteNewLatest(km) // pull the next version up to the key path
		} else if len(km.Versions) > 0 {
			km.Versions[0].IsLatest = true
		}
		return b.finalizeKey(km)
	}

	// Delete latest.
	if bm.VersioningEnabled() {
		// Archive the current object (if any) and add a delete marker as latest.
		if err := b.displaceCurrent(km, true); err != nil {
			return err
		}
		prependLatest(km, types.ObjectMeta{
			Bucket: c.Bucket, Key: c.Key, VersionID: c.VersionID,
			DeleteMarker: true, LastModified: mtime(c),
		})
		return b.finalizeKey(km)
	}
	// Unversioned / suspended: drop the current null version.
	if cur := km.Latest(); cur != nil && !cur.DeleteMarker {
		if cur.BlobID == "" {
			b.removeCurrent(c.Bucket, c.Key)
		} else {
			b.removeArchivedBlob(c.Bucket, cur.BlobID)
		}
		if cur.VersionID == "null" || cur.BlobID == "" {
			km.Versions = km.Versions[1:] // remove the null current from history
		}
	}
	if bm.Versioning == "Suspended" {
		prependLatest(km, types.ObjectMeta{Bucket: c.Bucket, Key: c.Key, VersionID: "null", DeleteMarker: true, LastModified: mtime(c)})
	} else if len(km.Versions) > 0 {
		km.Versions[0].IsLatest = true
	}
	return b.finalizeKey(km)
}

// promoteNewLatest ensures the new latest version's bytes are at the key path,
// pulling an archived version up when the previous current was removed.
func (b *posixBackend) promoteNewLatest(km *types.KeyMeta) {
	if len(km.Versions) == 0 {
		return
	}
	nl := &km.Versions[0]
	nl.IsLatest = true
	if !nl.DeleteMarker && nl.BlobID != "" {
		if b.promoteArchived(km.Bucket, km.Key, nl.BlobID) == nil {
			nl.BlobID = "" // now the browsable key file
		}
	}
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
	src, _, err := b.resolveVersion(c.Source.Bucket, c.Source.Key, c.Source.VersionID)
	if err != nil {
		return err
	}
	if src.DeleteMarker {
		return s3err.ErrNoSuchKey
	}
	srcPath, err := b.physPath(c.Source.Bucket, src)
	if err != nil {
		return err
	}

	km := b.loadKeyMetaOrNew(c.Bucket, c.Key)
	if err := b.displaceCurrent(km, bm.VersioningEnabled()); err != nil {
		return err
	}
	// Write the copied bytes as the new current object file.
	dst, err := b.objectPath(c.Bucket, c.Key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := copyFile(srcPath, dst); err != nil {
		return err
	}

	vid := "null"
	if bm.VersioningEnabled() {
		vid = c.VersionID
	}
	nv := src // inherit source metadata
	nv.Bucket, nv.Key, nv.VersionID, nv.BlobID = c.Bucket, c.Key, vid, ""
	nv.LastModified = mtime(c)
	nv.IsLatest = false
	nv.ACL = nil
	if c.Meta["metadata-directive"] == "REPLACE" {
		repl := metaFromCommand(c, src.Size, src.ETag, vid)
		repl.BlobID = ""
		repl.StorageClass = nv.StorageClass
		nv = repl
	}
	if c.Meta["tagging-directive"] == "REPLACE" {
		nv.Tags = nil
	}
	applyLockDefaults(bm, &nv, c)
	prependLatest(km, nv)
	return b.writeKeyMeta(km)
}

// ---- reads ----

// resolveVersion returns the requested version (or latest) plus its KeyMeta.
func (b *posixBackend) resolveVersion(bucket, key, versionID string) (types.ObjectMeta, *types.KeyMeta, error) {
	if _, err := b.readBucketMeta(bucket); err != nil {
		return types.ObjectMeta{}, nil, err
	}
	km, err := b.loadKeyMeta(bucket, key)
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
	pp, err := b.physPath(bucket, v)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(pp)
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

// walkObjectTree lists keys with a live current object by walking the browsable
// 1:1 tree at <root>/<bucket> (delete-marker-current keys have no file here, so
// they are naturally excluded from V2 listings).
func (b *posixBackend) walkObjectTree(bucket string) ([]string, error) {
	root := filepath.Join(b.root, bucket)
	var keys []string
	filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	sort.Strings(keys)
	return keys, nil
}

// walkAllKeys lists every key with history (including delete-marker-current keys)
// by walking the metadata tree; used for version listings.
func (b *posixBackend) walkAllKeys(bucket string) ([]string, error) {
	root := b.idir("objmeta", bucket)
	var keys []string
	filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		keys = append(keys, filepath.ToSlash(strings.TrimSuffix(rel, ".json")))
		return nil
	})
	sort.Strings(keys)
	return keys, nil
}

func (b *posixBackend) ListObjectsV2(in types.ListInput) (*types.ListResult, error) {
	if _, err := b.readBucketMeta(in.Bucket); err != nil {
		return nil, err
	}
	keys, err := b.walkObjectTree(in.Bucket)
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
		km, err := b.loadKeyMeta(in.Bucket, key)
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
	// Union of keys with metadata sidecars and prefilled files without one.
	metaKeys, err := b.walkAllKeys(in.Bucket)
	if err != nil {
		return nil, err
	}
	fileKeys, _ := b.walkObjectTree(in.Bucket)
	keys := mergeSortedUnique(metaKeys, fileKeys)
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
		km, err := b.loadKeyMeta(in.Bucket, key)
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

// mergeSortedUnique merges two sorted string slices, deduplicating.
func mergeSortedUnique(a, b []string) []string {
	set := make(map[string]struct{}, len(a)+len(b))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		set[s] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
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
