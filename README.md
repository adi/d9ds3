# d9ds3

A log-decoupled, horizontally-scalable, **S3-compatible** object gateway — a
re-imagining of [versitygw](https://github.com/versity/versitygw).

versitygw translates each S3 request directly into **one** shared storage backend.
`d9ds3` splits that backend along the **read/write axis** and inserts a **replicated
command log** (embedded Raft) between the S3 frontend and **N independent storage
replicas**. Every mutating S3 op becomes a log entry that all storage nodes apply in
the same order, so they stay in sync; reads are served locally by any replica.

Module path: `github.com/adi/d9ds3`. See [`ARCHITECTURE.md`](./ARCHITECTURE.md).

```
  S3 client ──► gateway ──┬── mutate ──► [ Raft log ] ──► every storage replica (applies → local FS)
   (SigV4, stateless)     └── read ────────────────────► any caught-up replica
```

## What works (driven by the AWS SDK v2 in the test suite)

**Auth & IAM** — SigV4 header signing, presigned URLs, **aws-chunked streaming**
uploads (signed + trailer), anonymous access; replicated IAM accounts with a
root/admin bootstrap and an admin API for user CRUD.

**Buckets** — create/delete/head/list; versioning config; policy; CORS (+ preflight);
tagging; ownership controls; object-lock config; location.

**Objects** — put/get/head/delete/copy; **multipart** (create/upload/upload-part-copy/
complete/abort/list-parts/list-uploads); **versioning** (version ids, delete markers,
`ListObjectVersions`, versioned get/delete); batch `DeleteObjects`; **range** & **conditional**
GET (`If-Match`/`If-None-Match`/`If-*-Since`); Content-MD5 validation; object ACL,
tagging, retention, legal-hold; `GetObjectAttributes`; response-header overrides.

**Access control** — bucket-policy evaluation (allow/deny, wildcards, principals,
IP conditions) + ACL (canned + explicit grants, AllUsers/AuthenticatedUsers groups),
owner/admin bypass.

**Distribution** — the storage nodes form the Raft cluster; a single write through
the gateway replicates to **every** node's local backend and survives leader loss
(quorum). Reads route to a caught-up replica.

**Events** — S3-style event notifications (webhook / file sink) on mutations.

**S3 Select** — `SelectObjectContent` over CSV & JSON (SELECT/WHERE/LIMIT/CAST/LIKE,
`COUNT(*)`), returned as an AWS event stream the SDK decodes natively.

**Web console** — a static browser UI at `/console` (login, bucket/object browser,
upload/download, IAM admin) backed by a Basic-auth JSON API.

**Write scaling** — buckets hash across **N independent Raft shards** (`--shards`);
all nodes are members of all shards, and shard leadership is distributed across the
cluster (`POST /v1/rebalance`), so writes to different buckets commit through
different leaders in parallel.

**Durability** — real FSM **snapshot/restore** of the full dataset: logs compact and
a brand-new node bootstraps from an installed snapshot rather than replaying the
whole log.

**Node recovery** — a node that was down misses the payload fan-out; on restart it
replays the Raft log (or installs a snapshot) and, for each object whose payload it
never received, pulls the committed bytes from a peer before applying — so it never
silently skips an object. Unclaimed fan-out payloads (ops that never committed) are
reclaimed by a **staging GC** (`--staging-ttl`).

> Reserved gateway paths (they shadow like-named buckets on the S3 endpoint):
> `/console` (web UI), `/healthz` (liveness), `/readyz` (readiness — 200 only once
> the bootstrap account is live, so pods accept traffic without the brief
> `InvalidAccessKeyId` startup window).

## Build & test
```sh
go build -o bin/d9ds3 ./cmd/d9ds3
go test ./...           # unit (auth/authz/s3event) + full AWS-SDK integration + replication/failover
```

## Run a 3-node cluster + gateway
```sh
bin/d9ds3 storage --id n1 --data ./data/n1 --raft 127.0.0.1:9001 --http 127.0.0.1:8001 \
  --bootstrap --peers 127.0.0.1:8002,127.0.0.1:8003
bin/d9ds3 storage --id n2 --data ./data/n2 --raft 127.0.0.1:9002 --http 127.0.0.1:8002 \
  --join 127.0.0.1:8001 --peers 127.0.0.1:8001,127.0.0.1:8003
bin/d9ds3 storage --id n3 --data ./data/n3 --raft 127.0.0.1:9003 --http 127.0.0.1:8003 \
  --join 127.0.0.1:8001 --peers 127.0.0.1:8001,127.0.0.1:8002

bin/d9ds3 gateway --s3 :8080 --nodes 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 \
  --region us-east-1 --root-access-key admin --root-secret supersecretkey \
  --events http://localhost:9000/hook
```

## Use it (aws-cli)
```sh
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=supersecretkey AWS_REGION=us-east-1
alias s3='aws --endpoint-url http://localhost:8080'
s3 s3 mb s3://demo
s3 s3 cp ./file s3://demo/file
s3 s3api put-bucket-versioning --bucket demo --versioning-configuration Status=Enabled
s3 s3 cp s3://demo/file -
```

Manage IAM (admin creds required):
```sh
curl -X POST 'http://localhost:8080/?admin&action=create-account' \
  --aws-sigv4 'aws:amz:us-east-1:s3' --user 'admin:supersecretkey' \
  -d '{"access_key_id":"alice","secret_key":"alicesecret","role":"user"}'
```

## Layout
```
cmd/d9ds3/          CLI: `storage` and `gateway` subcommands
internal/s3api/     S3 protocol (net/http): routing, SigV4 auth, access control, XML
internal/gateway/   Mutator: fan-out, submit-to-leader, event firing, read delegation
internal/cluster/   Gateway→storage client: leader resolution, fan-out, read proxy, IAM
internal/storage/   Raft node, FSM, posix backend (objects/versions/multipart/config), data plane
internal/auth/      SigV4 (header/presigned/streaming-chunked) verification
internal/authz/     Bucket-policy evaluation + ACL checks + canned ACLs
internal/s3event/   Event notifications (webhook/file)
internal/command/   The replicated-log record schema + codec
internal/types/     Shared domain model
internal/s3err/     S3 error codes → XML
internal/e2e/       AWS-SDK-v2 integration suite + replication/failover tests
```

## Sharded (write-scaling) deployment
Give every node the same `--shards N` and a base raft port with room for N groups
(shard `i` binds `raftPort+i`):
```sh
bin/d9ds3 storage --id n1 --data ./data/n1 --raft 127.0.0.1:9001 --http 127.0.0.1:8001 --bootstrap --shards 4
bin/d9ds3 storage --id n2 --data ./data/n2 --raft 127.0.0.1:9101 --http 127.0.0.1:8002 --join 127.0.0.1:8001 --shards 4
bin/d9ds3 storage --id n3 --data ./data/n3 --raft 127.0.0.1:9201 --http 127.0.0.1:8003 --join 127.0.0.1:8001 --shards 4
# spread shard leadership across nodes:
curl -X POST http://127.0.0.1:8001/v1/rebalance
```

## Web console
Open `http://<gateway>/console/`, log in with an access key + secret, and browse.

## Testing
`go test ./...` runs unit tests (auth/authz/s3event/s3select) and the AWS-SDK
integration suite: full S3 surface, delimiter+pagination, versioning pagination,
multipart, S3 Select, streaming-chunked upload, presigned URLs, public policy, web
console, 3-node replication + leader failover, snapshot-join, and 4-shard write
distribution + sharded failover.
```
