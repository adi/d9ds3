// Package command defines the replicated-log record. Every mutating S3 operation
// is encoded as exactly one Command, appended to the Raft log, and deterministically
// executed by each storage node's FSM. Object/part bytes are NEVER inlined here —
// a BlobToken points at payload that was fanned out to the storage nodes' staging
// areas out-of-band. This keeps the log small and the data plane fast.
package command

import (
	"encoding/json"
	"fmt"
)

// SchemaVersion is stamped into every Command so old records stay replayable as
// the schema evolves.
const SchemaVersion = 1

// Op identifies which mutation a Command represents. One Op per mutating S3
// method; the FSM switches on it. Read methods never produce a Command.
type Op string

const (
	// Buckets
	OpCreateBucket Op = "create_bucket"
	OpDeleteBucket Op = "delete_bucket"

	// Objects
	OpPutObject    Op = "put_object"
	OpDeleteObject Op = "delete_object"
	OpCopyObject   Op = "copy_object"

	// Multipart
	OpCreateMultipart   Op = "create_multipart"
	OpUploadPart        Op = "upload_part"
	OpCompleteMultipart Op = "complete_multipart"
	OpAbortMultipart    Op = "abort_multipart"

	// Object metadata
	OpPutObjectAcl       Op = "put_object_acl"
	OpPutObjectTagging   Op = "put_object_tagging"
	OpDeleteObjectTag    Op = "delete_object_tagging"
	OpPutObjectRetention Op = "put_object_retention"
	OpPutObjectLegalHold Op = "put_object_legal_hold"

	// Bucket configuration (Config holds the raw serialized config)
	OpPutBucketAcl        Op = "put_bucket_acl"
	OpPutBucketPolicy     Op = "put_bucket_policy"
	OpDeleteBucketPolicy  Op = "delete_bucket_policy"
	OpPutBucketCors       Op = "put_bucket_cors"
	OpDeleteBucketCors    Op = "delete_bucket_cors"
	OpPutBucketTagging    Op = "put_bucket_tagging"
	OpDeleteBucketTag     Op = "delete_bucket_tagging"
	OpPutBucketVersioning Op = "put_bucket_versioning"
	OpPutBucketOwnership  Op = "put_bucket_ownership"
	OpDeleteBucketOwner   Op = "delete_bucket_ownership"
	OpPutObjectLockConfig Op = "put_object_lock_config"

	// IAM (account management, replicated so every node authenticates identically)
	OpPutAccount    Op = "put_account"
	OpDeleteAccount Op = "delete_account"
)

// ObjectRef names a bucket/key(/version) tuple (used as the source of a copy).
type ObjectRef struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id,omitempty"`
}

// CompletedPart identifies one part in a CompleteMultipartUpload request.
type CompletedPart struct {
	PartNumber int    `json:"part"`
	ETag       string `json:"etag"`
}

// Reserved keys inside Command.Meta. These carry values that must be identical
// on every replica, so they are computed once on the gateway and replayed from
// the log (keeping FSM.Apply free of wall-clock/randomness).
const (
	MetaContentType     = "content-type"
	MetaContentEncoding = "content-encoding"
	MetaContentLang     = "content-language"
	MetaContentDisp     = "content-disposition"
	MetaCacheControl    = "cache-control"
	MetaExpires         = "expires"
	MetaMTime           = "d9:mtime"      // RFC3339Nano modification time, set by the gateway
	MetaTags            = "d9:tags"       // JSON map of object tags set at put time
	MetaRetentionMode   = "d9:ret-mode"   // GOVERNANCE|COMPLIANCE set at put time
	MetaRetainUntil     = "d9:ret-until"  // RFC3339Nano retain-until set at put time
	MetaLegalHold       = "d9:legal-hold" // ON|OFF set at put time
)

// Command is a single entry in the replicated log.
//
// Determinism contract: FSM.Apply must be a pure function of (Command, staged
// blob, current local state). Anything non-deterministic — timestamps, etags,
// version ids, generated ids — is computed on the gateway and carried here, so
// every replica computes the identical result.
type Command struct {
	Version int `json:"v"`
	Op      Op  `json:"op"`

	Bucket    string `json:"bucket,omitempty"`
	Key       string `json:"key,omitempty"`
	VersionID string `json:"version_id,omitempty"`

	BlobToken  string `json:"blob,omitempty"` // pointer to staged payload; empty for bodiless ops
	Size       int64  `json:"size,omitempty"`
	ETag       string `json:"etag,omitempty"`
	StorageCls string `json:"storage_class,omitempty"`

	// Multipart
	UploadID   string          `json:"upload_id,omitempty"`
	PartNumber int             `json:"part_number,omitempty"`
	Parts      []CompletedPart `json:"parts,omitempty"`

	Meta   map[string]string `json:"meta,omitempty"` // content-type, mtime, x-amz-meta-*
	Source *ObjectRef        `json:"src,omitempty"`  // for copy / upload-part-copy

	// Config carries the raw serialized configuration for Put*Config /
	// Put*Acl / Put*Tagging / retention / legal-hold / policy ops.
	Config []byte `json:"config,omitempty"`

	IssuedBy string `json:"by,omitempty"`    // authenticated account (audit + ownership)
	Nonce    string `json:"nonce,omitempty"` // idempotency / dedup key
}

// Encode serializes a Command for the Raft log.
func Encode(c *Command) ([]byte, error) {
	if c.Version == 0 {
		c.Version = SchemaVersion
	}
	return json.Marshal(c)
}

// Decode parses a Command from the Raft log.
func Decode(b []byte) (*Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("command decode: %w", err)
	}
	if c.Version > SchemaVersion {
		return nil, fmt.Errorf("command schema v%d newer than supported v%d", c.Version, SchemaVersion)
	}
	return &c, nil
}
