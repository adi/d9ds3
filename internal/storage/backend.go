package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

// ErrBlobMissing means a Command references a staged payload that never arrived on
// this node (fan-out missed it). The FSM catches this and pulls the payload from a
// peer before retrying Apply.
var ErrBlobMissing = errors.New("staged blob missing")

// posixBackend is the local storage engine (the LocalBackend). It keeps a 1:1
// POSIX mapping like versitygw — an object "bucket/dir/key" is a real, browsable
// file at "<data>/bucket/dir/key", carrying its S3 metadata in extended
// attributes. The --data volume contains NOTHING but the object tree; all internal
// bookkeeping lives in a separate state root. On-disk layout:
//
//	<data>/<bucket>/<key>               object bytes + metadata in xattrs (user.d9ds3.*)
//	<state>/versions/<bucket>/<blobid>.blob   NON-current version payloads
//	<state>/history/<bucket>/<key>.json       version history (only for versioned keys)
//	<state>/buckets/<bucket>.json             bucket configuration
//	<state>/mpu/<bucket>/<uploadid>.json      in-progress multipart uploads
//	<state>/staging|mpstaging/...             pre-commit fan-out buffers
//	<state>/iam/accounts.json                 replicated IAM accounts
type posixBackend struct {
	root  string // data root: the browsable object tree
	state string // state root: all internal bookkeeping (kept out of --data)
	iam   *iamStore
	mu    sync.Mutex // serializes Apply against snapshot Persist/Restore
}

func newPosixBackend(dataRoot, stateRoot string) (*posixBackend, error) {
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return nil, err
	}
	for _, d := range []string{"staging", "mpstaging", "versions", "history", "buckets", "mpu", "iam"} {
		if err := os.MkdirAll(filepath.Join(stateRoot, d), 0o755); err != nil {
			return nil, err
		}
	}
	b := &posixBackend{root: dataRoot, state: stateRoot}
	b.iam = newIAMStore(filepath.Join(stateRoot, "iam", "accounts.json"))
	if err := b.iam.load(); err != nil {
		return nil, err
	}
	return b, nil
}

// ---- path helpers ----

func (b *posixBackend) idir(parts ...string) string {
	return filepath.Join(append([]string{b.state}, parts...)...)
}
func (b *posixBackend) stagingPath(token string) string { return b.idir("staging", token) }
func (b *posixBackend) mpPartPath(uploadID string, part int) string {
	return b.idir("mpstaging", fmt.Sprintf("%s__%05d", uploadID, part))
}
func (b *posixBackend) vblobPath(bucket, blobID string) string {
	return b.idir("versions", bucket, blobID+".blob")
}
func (b *posixBackend) bucketMetaPath(bucket string) string {
	return b.idir("buckets", bucket+".json")
}
func (b *posixBackend) mpuMetaPath(bucket, uploadID string) string {
	return b.idir("mpu", bucket, uploadID+".json")
}

// historyPath maps bucket/key to its version-history file, rejecting path traversal.
func (b *posixBackend) historyPath(bucket, key string) (string, error) {
	base := b.idir("history", bucket)
	full := filepath.Join(base, filepath.FromSlash(key)+".json")
	if !strings.HasPrefix(full, base+string(os.PathSeparator)) {
		return "", s3err.ErrInvalidKey
	}
	return full, nil
}

// objectPath maps bucket/key to the browsable object file, rejecting traversal.
func (b *posixBackend) objectPath(bucket, key string) (string, error) {
	base := filepath.Join(b.root, bucket)
	full := filepath.Join(base, filepath.FromSlash(key))
	if full == base || !strings.HasPrefix(full, base+string(os.PathSeparator)) {
		return "", s3err.ErrInvalidKey
	}
	return full, nil
}

// ---- physical object-file operations ----
//
// The CURRENT version of a key is stored as a plain file at <data>/<bucket>/<key>
// (BlobID==""), so the data root is a browsable, prefill-able 1:1 tree. Only
// NON-current versions are moved into the state root's versions store.

// putCurrentBytes installs src (a staged temp file) as the current object file.
func (b *posixBackend) putCurrentBytes(bucket, key, src string) error {
	op, err := b.objectPath(bucket, key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(op), 0o755); err != nil {
		return err
	}
	return moveFile(src, op) // atomic overwrite
}

// archiveCurrent moves the current object file into the version store under vid.
func (b *posixBackend) archiveCurrent(bucket, key, vid string) error {
	op, err := b.objectPath(bucket, key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(b.idir("versions", bucket), 0o755); err != nil {
		return err
	}
	return moveFile(op, b.vblobPath(bucket, vid))
}

// promoteArchived moves an archived version back to the current object file.
func (b *posixBackend) promoteArchived(bucket, key, vid string) error {
	op, err := b.objectPath(bucket, key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(op), 0o755); err != nil {
		return err
	}
	return moveFile(b.vblobPath(bucket, vid), op)
}

// removeCurrent deletes the current object file and prunes emptied parent dirs.
func (b *posixBackend) removeCurrent(bucket, key string) {
	op, err := b.objectPath(bucket, key)
	if err != nil {
		return
	}
	os.Remove(op)
	pruneEmptyDirs(filepath.Dir(op), filepath.Join(b.root, bucket))
}

// removeArchivedBlob deletes a non-current version's blob (BlobID=="" is the
// current file and is handled by removeCurrent, so it is ignored here).
func (b *posixBackend) removeArchivedBlob(bucket, blobID string) {
	if blobID != "" {
		os.Remove(b.vblobPath(bucket, blobID))
	}
}

// pruneEmptyDirs removes now-empty parent dirs up to (but not including) stop.
func pruneEmptyDirs(dir, stop string) {
	for dir != stop && strings.HasPrefix(dir, stop+string(os.PathSeparator)) {
		if os.Remove(dir) != nil { // stops at the first non-empty dir
			return
		}
		dir = filepath.Dir(dir)
	}
}

// ---- Apply: execute one committed Command deterministically ----

func (b *posixBackend) Apply(c *command.Command) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch c.Op {
	case command.OpCreateBucket:
		return b.applyCreateBucket(c)
	case command.OpDeleteBucket:
		return b.applyDeleteBucket(c)
	case command.OpPutObject:
		return b.applyPutObject(c)
	case command.OpDeleteObject:
		return b.applyDeleteObject(c)
	case command.OpCopyObject:
		return b.applyCopyObject(c)
	case command.OpCreateMultipart:
		return b.applyCreateMultipart(c)
	case command.OpUploadPart:
		return b.applyUploadPart(c)
	case command.OpCompleteMultipart:
		return b.applyCompleteMultipart(c)
	case command.OpAbortMultipart:
		return b.applyAbortMultipart(c)
	case command.OpPutObjectAcl:
		return b.applyObjectMetaMutation(c, mutObjectACL)
	case command.OpPutObjectTagging:
		return b.applyObjectMetaMutation(c, mutObjectTags)
	case command.OpDeleteObjectTag:
		return b.applyObjectMetaMutation(c, mutObjectDelTags)
	case command.OpPutObjectRetention:
		return b.applyObjectMetaMutation(c, mutObjectRetention)
	case command.OpPutObjectLegalHold:
		return b.applyObjectMetaMutation(c, mutObjectLegalHold)
	case command.OpPutBucketAcl, command.OpPutBucketPolicy, command.OpDeleteBucketPolicy,
		command.OpPutBucketCors, command.OpDeleteBucketCors, command.OpPutBucketTagging,
		command.OpDeleteBucketTag, command.OpPutBucketVersioning, command.OpPutBucketOwnership,
		command.OpDeleteBucketOwner, command.OpPutObjectLockConfig:
		return b.applyBucketConfig(c)
	case command.OpPutAccount:
		return b.iam.put(c.Config)
	case command.OpDeleteAccount:
		return b.iam.delete(c.Key)
	default:
		return fmt.Errorf("unsupported op %q", c.Op)
	}
}

// ---- buckets ----

func (b *posixBackend) applyCreateBucket(c *command.Command) error {
	if c.Bucket == "" || strings.ContainsAny(c.Bucket, "/\\") {
		return s3err.ErrInvalidBucketName
	}
	if _, err := os.Stat(b.bucketMetaPath(c.Bucket)); err == nil {
		return s3err.ErrBucketAlreadyOwn
	}
	meta := types.BucketMeta{
		Name:      c.Bucket,
		Owner:     c.IssuedBy,
		CreatedAt: mtime(c),
		Ownership: "BucketOwnerEnforced",
	}
	if s := c.Meta["ownership"]; s != "" {
		meta.Ownership = s
	}
	if c.Meta["object-lock"] == "true" {
		meta.ObjectLock = &types.ObjectLockConfig{Enabled: true}
		meta.Versioning = "Enabled" // object lock requires versioning
	}
	if len(c.Config) > 0 { // initial ACL, if provided
		var acl types.ACL
		if json.Unmarshal(c.Config, &acl) == nil {
			meta.ACL = &acl
		}
	}
	for _, d := range []string{
		filepath.Join(b.root, c.Bucket), // browsable object tree
		b.idir("versions", c.Bucket),
		b.idir("history", c.Bucket),
		b.idir("mpu", c.Bucket),
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	return writeJSON(b.bucketMetaPath(c.Bucket), meta)
}

func (b *posixBackend) applyDeleteBucket(c *command.Command) error {
	// A bucket is deletable only when it holds no objects — neither replicated keys
	// (metadata present, including delete markers) nor prefilled files on disk.
	if files, _ := b.walkObjectTree(c.Bucket); len(files) > 0 || !dirEmpty(b.idir("history", c.Bucket)) {
		return s3err.ErrBucketNotEmpty
	}
	os.RemoveAll(filepath.Join(b.root, c.Bucket))
	os.RemoveAll(b.idir("versions", c.Bucket))
	os.RemoveAll(b.idir("history", c.Bucket))
	os.RemoveAll(b.idir("mpu", c.Bucket))
	if err := os.Remove(b.bucketMetaPath(c.Bucket)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// applyBucketConfig mutates one field of a bucket's config.
func (b *posixBackend) applyBucketConfig(c *command.Command) error {
	meta, err := b.readBucketMeta(c.Bucket)
	if err != nil {
		return err
	}
	switch c.Op {
	case command.OpPutBucketAcl:
		var acl types.ACL
		if err := json.Unmarshal(c.Config, &acl); err != nil {
			return err
		}
		meta.ACL = &acl
	case command.OpPutBucketPolicy:
		meta.Policy = append([]byte(nil), c.Config...)
	case command.OpDeleteBucketPolicy:
		meta.Policy = nil
	case command.OpPutBucketCors:
		var cors []types.CORSRule
		if err := json.Unmarshal(c.Config, &cors); err != nil {
			return err
		}
		meta.CORS = cors
	case command.OpDeleteBucketCors:
		meta.CORS = nil
	case command.OpPutBucketTagging:
		var tags map[string]string
		if err := json.Unmarshal(c.Config, &tags); err != nil {
			return err
		}
		meta.Tags = tags
	case command.OpDeleteBucketTag:
		meta.Tags = nil
	case command.OpPutBucketVersioning:
		meta.Versioning = string(c.Config)
	case command.OpPutBucketOwnership:
		meta.Ownership = string(c.Config)
	case command.OpDeleteBucketOwner:
		meta.Ownership = ""
	case command.OpPutObjectLockConfig:
		var lc types.ObjectLockConfig
		if err := json.Unmarshal(c.Config, &lc); err != nil {
			return err
		}
		meta.ObjectLock = &lc
	}
	return writeJSON(b.bucketMetaPath(c.Bucket), *meta)
}

// ---- bucket reads ----

func (b *posixBackend) readBucketMeta(bucket string) (*types.BucketMeta, error) {
	var m types.BucketMeta
	if err := readJSON(b.bucketMetaPath(bucket), &m); err == nil {
		return &m, nil
	}
	// Prefill support: a plain top-level directory is a bucket with default config.
	if bucket != "" && !strings.ContainsAny(bucket, "/\\") {
		if fi, err := os.Stat(filepath.Join(b.root, bucket)); err == nil && fi.IsDir() {
			return &types.BucketMeta{Name: bucket, CreatedAt: fi.ModTime().UTC(), Ownership: "BucketOwnerEnforced"}, nil
		}
	}
	return nil, s3err.ErrNoSuchBucket
}

// bucketMetaForWrite returns a bucket's config, or a default (versioning off) when
// none exists locally. An already-committed write MUST apply deterministically on
// every replica — even one that lacks the bucket locally (e.g. asymmetric prefill,
// or a follower materializing the write before it has seen the bucket) — so object
// writes never reject on a missing bucket. The gateway is the layer that rejects a
// PUT to a truly nonexistent bucket, before the command is ever logged.
func (b *posixBackend) bucketMetaForWrite(bucket string) *types.BucketMeta {
	if bm, err := b.readBucketMeta(bucket); err == nil {
		return bm
	}
	return &types.BucketMeta{Name: bucket, Ownership: "BucketOwnerEnforced"}
}

// HeadBucket / GetBucketMeta return the full bucket config.
func (b *posixBackend) GetBucketMeta(bucket string) (*types.BucketMeta, error) {
	return b.readBucketMeta(bucket)
}

func (b *posixBackend) ListBuckets() ([]types.BucketMeta, error) {
	seen := map[string]bool{}
	var out []types.BucketMeta
	// Configured buckets (with a metadata sidecar).
	if entries, err := os.ReadDir(b.idir("buckets")); err == nil {
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			var m types.BucketMeta
			if readJSON(b.idir("buckets", e.Name()), &m) == nil {
				seen[m.Name] = true
				out = append(out, m)
			}
		}
	}
	// Prefilled buckets: plain top-level directories with no sidecar.
	if entries, err := os.ReadDir(b.root); err == nil {
		for _, e := range entries {
			if !e.IsDir() || seen[e.Name()] {
				continue
			}
			created := time.Time{}
			if fi, err := e.Info(); err == nil {
				created = fi.ModTime().UTC()
			}
			out = append(out, types.BucketMeta{Name: e.Name(), CreatedAt: created, Ownership: "BucketOwnerEnforced"})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ---- shared json helpers ----

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func dirEmpty(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return true
	}
	return len(entries) == 0
}
