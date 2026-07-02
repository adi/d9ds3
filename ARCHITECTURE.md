# d9ds3 — a log-decoupled, horizontally-scalable S3 gateway

`d9ds3` is a re-imagining of [versitygw](https://github.com/versity/versitygw).
versitygw is a stateless S3 gateway that translates each S3 request directly into
**one** storage backend (posix, scoutfs, azure, s3-proxy). It scales horizontally
only because it is stateless — every gateway instance ultimately talks to the same
shared backend, which becomes the scaling ceiling.

`d9ds3` keeps versitygw's idea of a `Backend` translation layer but **splits it
along the read/write axis** and inserts a **replicated command log** between the
S3 frontend and N independent storage replicas. Every replica owns a *full copy*
of the data and applies the same ordered stream of mutations, so replicas can be
added to scale reads and durability without a shared backend.

```
                                         ┌────────────► storage-A ──► local posix FS
                                         │  (raft peer, applies FSM)
   ┌──────────┐    mutate cmd   ┌────────┴────────┐
   │  client  │ ──────────────► │   RAFT LOG      │──► storage-B ──► local posix FS
   │ (aws sdk)│                 │ (storage nodes  │
   └────┬─────┘                 │  form the       │──► storage-C ──► local posix FS
        │      payload bytes    │  cluster)       │        ▲
        │  (direct fan-out) ───►└─────────────────┘        │
        │                                                   │
   ┌────▼─────┐    read (Get/Head/List)                     │
   │ gateway  │ ───────────────────────────────────────────┘
   │ (stateless, S3 API, scales horizontally)   routed to a caught-up replica
   └──────────┘
```

---

## Two tiers

### 1. Gateway tier (stateless, S3 protocol)
Mirrors versitygw's `s3api` + `auth` layers. Responsibilities:
- Terminate the S3 HTTP(S) protocol (Fiber), parse/authenticate SigV4 requests.
- **Reads** (`GetObject`, `HeadObject`, `ListObjects*`, `HeadBucket`, `ListBuckets`,
  all the `Get*`/`List*` methods): forwarded to **any storage replica** that is
  caught up enough to satisfy the request.
- **Mutations** (`PutObject`, `DeleteObject(s)`, `CopyObject`, `CreateBucket`,
  `DeleteBucket`, multipart, and every `Put*`/tagging/acl/policy/retention method):
  1. If the request carries a body (PutObject, UploadPart), stream the bytes
     **directly to every storage replica** (data-plane fan-out) under a one-shot
     *blob token*.
  2. Submit a **command** describing the mutation to the Raft leader. The command
     references the blob token instead of embedding the bytes.
  3. Ack the client once Raft **commits** the command (durable on a quorum of
     replicas' logs). Return the commit index as a consistency token.

Gateways hold no durable state → add as many as you need behind a load balancer.

### 2. Storage tier (Raft cluster, stateful)
Each storage node is:
- A **Raft peer**. The set of storage nodes *is* the Raft cluster; the Raft log is
  the replication log. There is no external broker.
- A **finite state machine (FSM)**. `FSM.Apply(command)` deterministically executes
  the mutation against the node's **local posix backend** (create bucket dir, install
  object, delete, write metadata sidecar, etc.). Because every node applies the same
  log in the same order, all nodes converge (deterministic last-writer-wins).
- A **data-plane server** (HTTP) that:
  - receives fanned-out payloads into a **staging** area keyed by blob token,
  - serves **reads** (`GET object`, `HEAD`, `LIST`) from the local backend,
  - serves **blob pulls** so a lagging/recovering node can fetch a payload it missed
    from a peer (fallback for when fan-out didn't reach it),
  - reports **status** (Raft role, leader address, `applied_index`).

---

## Why these four choices fit together

| Choice | Consequence in this design |
|---|---|
| **Embedded Raft** | The log *is* Raft; storage nodes replicate it among themselves. No Kafka/NATS to run. Leader election, quorum durability, snapshots, and membership come for free. |
| **Direct payload fan-out** | Object bytes never enter the Raft log (which would bloat it and throttle the data plane). Bytes go gateway→replicas out-of-band; the log carries only a small pointer. |
| **Eventual / async** | The client is acked at Raft **commit** (quorum has the *command* durably). Followers **apply** to their local FS asynchronously but in log order. Reads that need read-your-writes are routed to a replica whose `applied_index ≥ commit_index` of the write (or to the leader). |
| **Go** | Reuse aws-sdk-go-v2 types, Fiber, and the versitygw S3 surface/idioms directly. |

### The write path in full (PutObject)
```
client ─PUT object(body)─► gateway
  gateway: auth, compute etag, mint blobToken
  gateway ─stream body─► storage-A /v1/blob/{token}   ┐  (fan-out, parallel;
  gateway ─stream body─► storage-B /v1/blob/{token}   ├── requires ≥quorum ok,
  gateway ─stream body─► storage-C /v1/blob/{token}   ┘   best-effort to rest)
  gateway ─submit PutObject{bucket,key,meta,etag,blobToken}─► raft leader
  raft: replicate cmd to quorum, COMMIT
  leader FSM.Apply: mv staging/{token} → buckets/bucket/key ; write meta sidecar
  gateway ◄─ commit index ── returns 200 + ETag + x-d9-index: <idx>
  followers: FSM.Apply the same cmd (async); if staging/{token} missing → pull from a peer
```
A mutation **without** a body (Delete, CreateBucket, tagging…) skips step-1 fan-out
entirely; it is purely a Raft command.

### The read path
```
client ─GET object─► gateway ─► pick replica (leader, or any with fresh applied_index)
                                 replica serves bytes from local posix backend
```

---

## Command log schema

A command is a versioned, self-describing record (see `internal/command`). The FSM
switches on `Op`. Bytes are **never** inlined — `BlobToken` points at staged payload.

```go
type Op string
const (
    OpCreateBucket Op = "create_bucket"
    OpDeleteBucket Op = "delete_bucket"
    OpPutObject    Op = "put_object"
    OpDeleteObject Op = "delete_object"
    OpCopyObject   Op = "copy_object"
    // ... one per mutating S3 method (multipart, acl, tagging, policy, ...)
)

type Command struct {
    Version   int               // schema version, for forward-compat replay
    Op        Op                // which mutation
    Bucket    string
    Key       string
    BlobToken string            // staged-payload pointer (empty for bodiless ops)
    Size      int64
    ETag      string
    Meta      map[string]string // content-type, user metadata, acl, tags, ...
    Source    ObjectRef         // for copy
    IssuedBy  string            // authenticated account, for audit
    Nonce     string            // idempotency / dedup key
}
```

Determinism rules for replay-safety: no wall-clock or randomness inside `FSM.Apply`
— any timestamp/etag/version-id is computed on the gateway and carried in the command.

---

## Splitting versitygw's `Backend` interface

versitygw has one ~60-method `Backend`. We split it into three:

```go
// Reader — served locally by any storage replica (no log involvement).
type Reader interface {
    GetObject(ctx, GetObjectInput) (*GetObjectOutput, error)
    HeadObject(...) ; ListObjectsV2(...) ; ListObjects(...) ;
    HeadBucket(...) ; ListBuckets(...) ; GetObjectTagging(...) ; /* all Get*/List* */
}

// Mutator — the gateway-side surface; each method builds a Command,
// fans out any payload, submits to the log, waits for commit.
type Mutator interface {
    CreateBucket(...) ; DeleteBucket(...) ;
    PutObject(...) ; DeleteObject(...) ; CopyObject(...) ;
    CreateMultipartUpload(...) ; UploadPart(...) ; CompleteMultipartUpload(...) ; /* all Put*/Delete* */
}

// LocalBackend — the deterministic executor inside FSM.Apply on each storage node.
// This is where the posix (later scoutfs/azure) implementation lives.
type LocalBackend interface {
    Reader
    Apply(cmd Command, blob BlobSource) error // installs the mutation locally
}
```

`Reader` is implemented by the storage node (local FS) and *proxied* by the gateway
(HTTP to a replica). `Mutator` is implemented only by the gateway. `LocalBackend` is
the swappable storage engine (v1: `posix`).

---

## Consistency, ordering & conflicts
- **Total order** per Raft: a single replicated log → a global order of all mutations
  → deterministic last-writer-wins, identical on every replica. (Throughput of a
  single Raft group is the tradeoff; see *Scaling* for sharding by bucket.)
- **Durability of ack**: quorum of Raft logs. Surviving minority failure loses nothing.
- **Read-your-writes**: gateway returns `x-d9-index`; a follow-up read can pin to a
  replica with `applied_index ≥` that value, or hit the leader.
- **Idempotent apply**: FSM tracks the last applied Raft index; `Nonce` guards against
  duplicate submission on gateway retry.

## On-disk layout — a 1:1 POSIX mapping (like versitygw)
An object `bucket/dir/key` is a **plain, browsable file** at `<data>/bucket/dir/key`,
carrying its S3 metadata (etag, content-type, user-metadata, tags, ACL, retention,
legal-hold, version-id) in **extended attributes** (`user.d9ds3.*`) — versitygw-style.
The `--data` volume contains **nothing but the object tree**: no sidecars, no hidden
dirs. You can `ls`/`cat`/`rsync` it, and a disk **pre-seeded** with folders-of-files
is served as-is (bucket = top-level directory, object = file). A plain file with no
xattrs (a naive prefill, or an `rsync` that dropped xattrs) still works — metadata is
synthesized from the file (size from `stat`, ETag from its MD5, content-type from the
extension); xattrs are an enrichment, never a requirement.

**Prefilled data is never destroyed by the system.** Files written through S3 carry
a managed marker xattr; snapshot install (Raft `InstallSnapshot`) reconciles only
those *replicated* files, while prefilled plain files (no managed xattr) are always
preserved. The only thing that removes operator-provided data is an explicit S3
delete. (`DeleteBucket` likewise refuses a bucket that still holds prefilled files.)
Note: prefill is node-local; for cluster-wide consistency seed the same tree on each
node, or prefill one node and let snapshots carry it to the rest.

Three independent roots (each may be its own volume):
- **`--data`** — ONLY the browsable object tree (files + xattrs). This is the
  backup/rsync surface; nothing internal ever lands here.
- **`--state-dir`** — internal bookkeeping kept out of `--data`: `versions/`
  (non-current version payloads), `history/` (version history, only for versioned
  keys), `buckets/` (bucket config), `mpu/` (in-flight multipart), `staging/` +
  `mpstaging/` (pre-commit fan-out buffers), `iam/`. Durable, node-local. Defaults to
  `<data>-state`.
- **`--raft-dir`** — node-local consensus state: `shard-<i>/{log.bolt, stable.bolt,
  snapshots/}`. Machine-specific; **never** copy between nodes or restore
  independently. Defaults to `<data>-raft`.

Moves between `--data` and `--state-dir` fall back to copy+remove when they're on
different volumes (cross-device), so you can put them on separate PVCs.

## Failure & recovery
- **Storage node crash**: rejoins, Raft ships it the log tail (or a snapshot +
  tail); FSM re-applies to converge. Missing staged blobs are pulled from a peer.
- **Leader loss**: Raft elects a new leader; gateway re-resolves the leader on the
  next submit (`ErrNotLeader` → follow redirect).
- **Gateway crash**: stateless; in-flight un-acked writes are simply retried by client.
- **Snapshotting** (implemented): each shard's FSM snapshots the full local dataset
  (tar of buckets/keys/vstore/mpu/iam); the log compacts behind it, and a brand-new
  node bootstraps from an installed snapshot (Raft InstallSnapshot) instead of
  replaying the whole log. `Restore` replaces local state from the snapshot.

## Scaling
- **Reads**: every node is a member of every shard, so any node holds all data;
  bucket-scoped reads are routed to the bucket's shard leader for read-after-write.
- **Gateways**: stateless; add freely.
- **Writes** (implemented): buckets hash across **N independent Raft shards**
  (`--shards N`). All nodes join all shards; shard leadership is spread across the
  cluster (`/v1/rebalance`), so writes to different buckets commit through different
  leaders in parallel. The gateway resolves the per-bucket shard leader on submit.
  Cluster-wide state (IAM accounts) lives on the root shard 0.

---

## Package layout (mirrors versitygw where it makes sense)
```
cmd/d9ds3/          CLI dispatcher: `storage` and `gateway` subcommands
internal/
  s3api/            S3 protocol (net/http): routing, SigV4 auth, access control, XML, S3 Select
  s3err/            S3 error codes → XML
  command/          Command schema (the replicated-log record) + codec
  gateway/          Mutator impl: fan-out, submit-to-shard-leader, events, read routing
  storage/          Multi-shard Raft node, FSM, posix backend, snapshot, data-plane HTTP
  cluster/          Per-shard leader resolution, fan-out, read proxy, IAM lookup
  auth/             SigV4 (header/presigned/streaming-chunked) verification
  authz/            Bucket-policy evaluation + ACL checks + canned ACLs
  s3event/          Event notifications (webhook/file)
  s3select/         S3 Select SQL engine + event-stream encoding
  webui/            Embedded web console + Basic-auth JSON API
  types/            Shared domain model
```

## Status
Full S3 surface (buckets, objects, versioning, multipart, ACL/policy/CORS/tagging/
object-lock, batch delete, range/conditional, S3 Select), SigV4 + IAM + access
control, event notifications, a web console, bucket-sharded multi-Raft write
scaling, and full-dataset snapshot/restore — all covered by an AWS-SDK integration
suite (see `internal/e2e`). The `internal/` packages below map the S3 layer to the
replicated log and the sharded storage tier.
