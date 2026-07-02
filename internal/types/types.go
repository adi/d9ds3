// Package types holds the data-transfer objects shared by the gateway, storage,
// and cluster packages. Keeping them dependency-free avoids import cycles.
package types

import (
	"encoding/json"
	"io"
	"time"
)

// ---- access control ----

// Owner is a canonical S3 owner (account).
type Owner struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name,omitempty"`
}

// Grantee is the subject of a grant.
type Grantee struct {
	Type        string `json:"type"` // CanonicalUser | Group | AmazonCustomerByEmail
	ID          string `json:"id,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	URI         string `json:"uri,omitempty"`
	Email       string `json:"email,omitempty"`
}

// Grant pairs a grantee with a permission (FULL_CONTROL/READ/WRITE/READ_ACP/WRITE_ACP).
type Grant struct {
	Grantee    Grantee `json:"grantee"`
	Permission string  `json:"permission"`
}

// ACL is an access-control policy for a bucket or object.
type ACL struct {
	Owner  Owner   `json:"owner"`
	Grants []Grant `json:"grants"`
}

// Canonical S3 group URIs.
const (
	GroupAllUsers           = "http://acs.amazonaws.com/groups/global/AllUsers"
	GroupAuthenticatedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
)

// ---- object lock ----

// Retention is an object-lock retention setting on a version.
type Retention struct {
	Mode        string    `json:"mode"` // GOVERNANCE | COMPLIANCE
	RetainUntil time.Time `json:"retain_until"`
}

// LegalHold is an object-lock legal-hold flag on a version.
type LegalHold struct {
	Status string `json:"status"` // ON | OFF
}

// ObjectLockConfig is a bucket's default object-lock configuration.
type ObjectLockConfig struct {
	Enabled     bool   `json:"enabled"`
	DefaultMode string `json:"default_mode,omitempty"`
	DefaultDays int    `json:"default_days,omitempty"`
	DefaultYrs  int    `json:"default_years,omitempty"`
}

// ---- objects & versions ----

// ObjectMeta is the metadata for a single object version (no bytes).
type ObjectMeta struct {
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	VersionID          string            `json:"version_id"`
	BlobID             string            `json:"blob_id,omitempty"` // physical payload filename in vstore (distinct from VersionID)
	ETag               string            `json:"etag"`
	Size               int64             `json:"size"`
	ContentType        string            `json:"content_type,omitempty"`
	ContentEncoding    string            `json:"content_encoding,omitempty"`
	ContentLanguage    string            `json:"content_language,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	CacheControl       string            `json:"cache_control,omitempty"`
	Expires            string            `json:"expires,omitempty"`
	UserMeta           map[string]string `json:"user_meta,omitempty"`
	StorageClass       string            `json:"storage_class,omitempty"`
	LastModified       time.Time         `json:"last_modified"`
	IsLatest           bool              `json:"is_latest"`
	DeleteMarker       bool              `json:"delete_marker,omitempty"`
	ACL                *ACL              `json:"acl,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	Retention          *Retention        `json:"retention,omitempty"`
	LegalHold          *LegalHold        `json:"legal_hold,omitempty"`
}

// KeyMeta is the version history for one key. Versions are ordered newest-first;
// Versions[0] is the current (latest) version.
type KeyMeta struct {
	Bucket   string       `json:"bucket"`
	Key      string       `json:"key"`
	Versions []ObjectMeta `json:"versions"`
	// Synthesized is true when this metadata was derived from a prefilled object
	// file rather than written by a replicated S3 operation. Such keys are never
	// removed by snapshot restore — only an explicit S3 delete removes them.
	Synthesized bool `json:"synthesized,omitempty"`
}

// Latest returns the current version, or nil if the key has no versions.
func (k *KeyMeta) Latest() *ObjectMeta {
	if len(k.Versions) == 0 {
		return nil
	}
	return &k.Versions[0]
}

// ---- buckets ----

// CORSRule is one cross-origin rule.
type CORSRule struct {
	ID             string   `json:"id,omitempty"`
	AllowedOrigins []string `json:"allowed_origins"`
	AllowedMethods []string `json:"allowed_methods"`
	AllowedHeaders []string `json:"allowed_headers,omitempty"`
	ExposeHeaders  []string `json:"expose_headers,omitempty"`
	MaxAgeSeconds  int      `json:"max_age_seconds,omitempty"`
}

// BucketMeta holds a bucket's existence marker and all of its configuration.
type BucketMeta struct {
	Name       string            `json:"name"`
	Owner      string            `json:"owner,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	Versioning string            `json:"versioning,omitempty"` // "" | Enabled | Suspended
	Ownership  string            `json:"ownership,omitempty"`  // ObjectWriter | BucketOwnerPreferred | BucketOwnerEnforced
	ACL        *ACL              `json:"acl,omitempty"`
	Policy     json.RawMessage   `json:"policy,omitempty"`
	CORS       []CORSRule        `json:"cors,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	ObjectLock *ObjectLockConfig `json:"object_lock,omitempty"`
}

// VersioningEnabled reports whether new writes should create distinct versions.
func (b *BucketMeta) VersioningEnabled() bool { return b.Versioning == "Enabled" }

// ---- multipart ----

// Part is one uploaded multipart part.
type Part struct {
	PartNumber   int       `json:"part_number"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
}

// MultipartUpload is an in-progress multipart upload.
type MultipartUpload struct {
	Bucket       string            `json:"bucket"`
	Key          string            `json:"key"`
	UploadID     string            `json:"upload_id"`
	Initiated    time.Time         `json:"initiated"`
	Owner        string            `json:"owner,omitempty"`
	StorageClass string            `json:"storage_class,omitempty"`
	ContentType  string            `json:"content_type,omitempty"`
	UserMeta     map[string]string `json:"user_meta,omitempty"`
	ACL          *ACL              `json:"acl,omitempty"`
	Parts        []Part            `json:"parts,omitempty"`
}

// ---- IAM ----

// Account is an IAM credential/identity, replicated via the log so every node
// authenticates identically.
type Account struct {
	AccessKeyID string `json:"access_key_id"`
	SecretKey   string `json:"secret_key"`
	Role        string `json:"role"` // admin | user
	UserID      string `json:"user_id,omitempty"`
}

// ---- reads ----

// ByteRange is an inclusive byte range; End < 0 means "to end of object".
type ByteRange struct {
	Start int64
	End   int64
}

// GetOptions parameterizes a read.
type GetOptions struct {
	VersionID string
	Range     *ByteRange
}

// GetResult is a streaming object read. Callers must Close Body.
type GetResult struct {
	Info          ObjectMeta
	Body          io.ReadCloser
	ContentRange  string // set when a Range was satisfied
	PartialLength int64  // bytes in Body when ranged (else full size)
	IsRange       bool
}

// ListInput parameters ListObjects(V1/V2).
type ListInput struct {
	Bucket            string
	Prefix            string
	Delimiter         string
	MaxKeys           int
	ContinuationToken string // v2
	StartAfter        string // v2
	Marker            string // v1
}

// ListResult is one page of an object listing.
type ListResult struct {
	Objects        []ObjectMeta `json:"objects"`
	CommonPrefixes []string     `json:"common_prefixes"`
	IsTruncated    bool         `json:"is_truncated"`
	NextToken      string       `json:"next_token,omitempty"`
	NextMarker     string       `json:"next_marker,omitempty"`
}

// ListVersionsInput parameters ListObjectVersions.
type ListVersionsInput struct {
	Bucket          string
	Prefix          string
	Delimiter       string
	MaxKeys         int
	KeyMarker       string
	VersionIDMarker string
}

// ListVersionsResult is one page of a versions listing.
type ListVersionsResult struct {
	Versions            []ObjectMeta `json:"versions"`
	DeleteMarkers       []ObjectMeta `json:"delete_markers"`
	CommonPrefixes      []string     `json:"common_prefixes"`
	IsTruncated         bool         `json:"is_truncated"`
	NextKeyMarker       string       `json:"next_key_marker,omitempty"`
	NextVersionIDMarker string       `json:"next_version_id_marker,omitempty"`
}

// ShardStatus reports this node's role in one Raft shard.
type ShardStatus struct {
	Shard        int    `json:"shard"`
	IsLeader     bool   `json:"is_leader"`
	AppliedIndex uint64 `json:"applied_index"`
}

// Status reports a storage node's health and per-shard Raft position.
type Status struct {
	NodeID    string        `json:"node_id"`
	NumShards int           `json:"num_shards"`
	Shards    []ShardStatus `json:"shards"`
	// IsLeader reflects leadership of the root shard (shard 0), where cluster-wide
	// state such as IAM lives; used for readiness checks.
	IsLeader bool `json:"is_leader"`
}

// LeadsShard reports whether this node is the leader of the given shard.
func (s *Status) LeadsShard(shard int) bool {
	for _, sh := range s.Shards {
		if sh.Shard == shard {
			return sh.IsLeader
		}
	}
	return false
}

// AppliedForShard returns the applied index for the given shard.
func (s *Status) AppliedForShard(shard int) uint64 {
	for _, sh := range s.Shards {
		if sh.Shard == shard {
			return sh.AppliedIndex
		}
	}
	return 0
}
