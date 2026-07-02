package storage

import (
	"encoding/json"
	"os"

	"github.com/adi/d9ds3/internal/types"
)

// Short xattr names for object metadata (stored under the xattrPrefix namespace).
const (
	xaVersionID    = "version-id"
	xaETag         = "etag"
	xaContentType  = "content-type"
	xaContentEnc   = "content-encoding"
	xaContentLang  = "content-language"
	xaContentDisp  = "content-disposition"
	xaCacheControl = "cache-control"
	xaExpires      = "expires"
	xaStorageClass = "storage-class"
	xaUserMeta     = "usermeta"
	xaTags         = "tags"
	xaACL          = "acl"
	xaRetention    = "retention"
	xaLegalHold    = "legal-hold"
)

// writeCurrentXattrs persists a current version's metadata onto its object file.
// The file's mtime is set to the version's LastModified so a plain `ls`/`stat`
// shows the S3 last-modified time.
func writeCurrentXattrs(path string, v *types.ObjectMeta) error {
	if err := setXattr(path, "managed", []byte("1")); err != nil {
		return err
	}
	setStr := func(name, val string) { setOrRemove(path, name, []byte(val), val != "") }
	setStr(xaVersionID, v.VersionID)
	setStr(xaETag, v.ETag)
	setStr(xaContentType, v.ContentType)
	setStr(xaContentEnc, v.ContentEncoding)
	setStr(xaContentLang, v.ContentLanguage)
	setStr(xaContentDisp, v.ContentDisposition)
	setStr(xaCacheControl, v.CacheControl)
	setStr(xaExpires, v.Expires)
	setStr(xaStorageClass, v.StorageClass)
	setJSON(path, xaUserMeta, v.UserMeta, len(v.UserMeta) > 0)
	setJSON(path, xaTags, v.Tags, len(v.Tags) > 0)
	setJSON(path, xaACL, v.ACL, v.ACL != nil)
	setJSON(path, xaRetention, v.Retention, v.Retention != nil)
	if v.LegalHold != nil {
		setStr(xaLegalHold, v.LegalHold.Status)
	} else {
		removeXattr(path, xaLegalHold)
	}
	if !v.LastModified.IsZero() {
		os.Chtimes(path, v.LastModified, v.LastModified)
	}
	return nil
}

// readCurrentMeta builds the current version's metadata for an object file. A file
// with our marker xattr is "managed" (written via S3); a plain file is prefilled,
// so its metadata is synthesized (returns synthesized=true).
func readCurrentMeta(path, bucket, key string) (*types.ObjectMeta, bool, error) {
	fi, err := os.Stat(path)
	if err != nil || fi.IsDir() {
		return nil, false, os.ErrNotExist
	}
	if !isManaged(path) {
		return &types.ObjectMeta{
			Bucket: bucket, Key: key, VersionID: "null", Size: fi.Size(),
			ETag: md5File(path), ContentType: guessContentType(key),
			LastModified: fi.ModTime().UTC(), IsLatest: true,
		}, true, nil
	}
	v := &types.ObjectMeta{
		Bucket: bucket, Key: key, Size: fi.Size(), LastModified: fi.ModTime().UTC(), IsLatest: true,
		VersionID:          sx(path, xaVersionID, "null"),
		ETag:               sx(path, xaETag, ""),
		ContentType:        sx(path, xaContentType, ""),
		ContentEncoding:    sx(path, xaContentEnc, ""),
		ContentLanguage:    sx(path, xaContentLang, ""),
		ContentDisposition: sx(path, xaContentDisp, ""),
		CacheControl:       sx(path, xaCacheControl, ""),
		Expires:            sx(path, xaExpires, ""),
		StorageClass:       sx(path, xaStorageClass, ""),
	}
	jx(path, xaUserMeta, &v.UserMeta)
	jx(path, xaTags, &v.Tags)
	if b, ok := getXattr(path, xaACL); ok {
		var a types.ACL
		if json.Unmarshal(b, &a) == nil {
			v.ACL = &a
		}
	}
	if b, ok := getXattr(path, xaRetention); ok {
		var r types.Retention
		if json.Unmarshal(b, &r) == nil {
			v.Retention = &r
		}
	}
	if s := sx(path, xaLegalHold, ""); s != "" {
		v.LegalHold = &types.LegalHold{Status: s}
	}
	return v, false, nil
}

// ---- small helpers ----

func sx(path, name, def string) string {
	if b, ok := getXattr(path, name); ok {
		return string(b)
	}
	return def
}

func jx(path, name string, out any) {
	if b, ok := getXattr(path, name); ok {
		json.Unmarshal(b, out)
	}
}

func setOrRemove(path, name string, val []byte, present bool) {
	if present {
		setXattr(path, name, val)
	} else {
		removeXattr(path, name)
	}
}

func setJSON(path, name string, v any, present bool) {
	if !present {
		removeXattr(path, name)
		return
	}
	if b, err := json.Marshal(v); err == nil {
		setXattr(path, name, b)
	}
}
