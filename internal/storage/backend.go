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

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

// ErrBlobMissing means a Command references a staged payload that never arrived on
// this node (fan-out missed it). The FSM catches this and pulls the payload from a
// peer before retrying Apply.
var ErrBlobMissing = errors.New("staged blob missing")

// posixBackend is the local storage engine (the LocalBackend). On-disk layout:
//
//	staging/<token>                object payloads fanned out, awaiting commit
//	mpstaging/<uploadid>__<part>   multipart part payloads, awaiting complete
//	vstore/<bucket>/<blobid>.blob  immutable version payloads (flat per bucket)
//	keys/<bucket>/<key>.d9k        KeyMeta: the version history for a key
//	buckets/<bucket>.json          BucketMeta: existence marker + all bucket config
//	mpu/<bucket>/<uploadid>.json   in-progress MultipartUpload metadata
//	iam/accounts.json              replicated IAM accounts
type posixBackend struct {
	root string
	iam  *iamStore
	mu   sync.Mutex // serializes Apply against snapshot Persist/Restore
}

func newPosixBackend(root string) (*posixBackend, error) {
	for _, d := range []string{"staging", "mpstaging", "vstore", "keys", "buckets", "mpu", "iam"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	b := &posixBackend{root: root}
	b.iam = newIAMStore(filepath.Join(root, "iam", "accounts.json"))
	if err := b.iam.load(); err != nil {
		return nil, err
	}
	return b, nil
}

// ---- path helpers ----

func (b *posixBackend) stagingPath(token string) string {
	return filepath.Join(b.root, "staging", token)
}
func (b *posixBackend) mpPartPath(uploadID string, part int) string {
	return filepath.Join(b.root, "mpstaging", fmt.Sprintf("%s__%05d", uploadID, part))
}
func (b *posixBackend) vblobPath(bucket, blobID string) string {
	return filepath.Join(b.root, "vstore", bucket, blobID+".blob")
}
func (b *posixBackend) bucketMetaPath(bucket string) string {
	return filepath.Join(b.root, "buckets", bucket+".json")
}
func (b *posixBackend) mpuMetaPath(bucket, uploadID string) string {
	return filepath.Join(b.root, "mpu", bucket, uploadID+".json")
}

// keyMetaPath maps bucket/key to the KeyMeta file, rejecting path traversal.
func (b *posixBackend) keyMetaPath(bucket, key string) (string, error) {
	base := filepath.Join(b.root, "keys", bucket)
	full := filepath.Join(base, filepath.FromSlash(key)+".d9k")
	if !strings.HasPrefix(full, base+string(os.PathSeparator)) {
		return "", s3err.ErrInvalidKey
	}
	return full, nil
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
	if err := os.MkdirAll(filepath.Join(b.root, "vstore", c.Bucket), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(b.root, "keys", c.Bucket), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(b.root, "mpu", c.Bucket), 0o755); err != nil {
		return err
	}
	return writeJSON(b.bucketMetaPath(c.Bucket), meta)
}

func (b *posixBackend) applyDeleteBucket(c *command.Command) error {
	if !dirEmpty(filepath.Join(b.root, "keys", c.Bucket)) {
		return s3err.ErrBucketNotEmpty
	}
	os.RemoveAll(filepath.Join(b.root, "vstore", c.Bucket))
	os.RemoveAll(filepath.Join(b.root, "keys", c.Bucket))
	os.RemoveAll(filepath.Join(b.root, "mpu", c.Bucket))
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
	if err := readJSON(b.bucketMetaPath(bucket), &m); err != nil {
		return nil, s3err.ErrNoSuchBucket
	}
	return &m, nil
}

// HeadBucket / GetBucketMeta return the full bucket config.
func (b *posixBackend) GetBucketMeta(bucket string) (*types.BucketMeta, error) {
	return b.readBucketMeta(bucket)
}

func (b *posixBackend) ListBuckets() ([]types.BucketMeta, error) {
	entries, err := os.ReadDir(filepath.Join(b.root, "buckets"))
	if err != nil {
		return nil, err
	}
	var out []types.BucketMeta
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var m types.BucketMeta
		if readJSON(filepath.Join(b.root, "buckets", e.Name()), &m) == nil {
			out = append(out, m)
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
