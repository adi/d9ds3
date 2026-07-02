# Kubernetes deployment

Two examples:

- **`single-node.yaml`** — one `d9ds3 standalone` pod (storage + gateway in one
  process). Durable local storage, no replication/failover. Dev / CI / edge / small.
- **`cluster.yaml`** — a 3-node storage `StatefulSet` (Raft) behind a headless
  Service for peer discovery, plus a stateless gateway `Deployment`. Quorum
  durability, survives a node loss.

Two volumes per node by default:
- **`--data`** — ONLY the browsable object tree; object metadata is stored in
  **extended attributes** on the files. It's the portable backup/rsync surface
  (`rsync -X`/`tar --xattrs` to carry metadata; a plain copy still works, metadata
  is re-synthesized). The `--data` filesystem must support user xattrs — ext4/xfs
  (and most CSI block volumes) do.
- **`--state-dir`** — all node-local internal state kept out of `--data`:
  versions/history/config/iam/staging, plus Raft consensus in a `raft/` subdir.

Raft's `raft/` consensus state must never be copied between nodes or restored
independently. For lower write latency under heavy load, add a dedicated fast PVC and
pass **`--raft-dir=/raft`** to move the Raft log off the shared state volume.

## Container image

The manifests pull **`ghcr.io/adi/d9ds3`** from GitHub Container Registry. Images are
published automatically by the `release` GitHub Action on every `v*` tag
(`.github/workflows/release.yml`), multi-arch for `linux/amd64` + `linux/arm64`.

Available tags: `latest`, plus `X.Y.Z` / `X.Y` / `vX.Y.Z` per release (e.g. `0.1.0`).
The manifests default to `:latest`; **pin to a version** (e.g. `ghcr.io/adi/d9ds3:0.1.0`)
for reproducible deploys.

Two one-time notes:
- The GHCR package may be **private** — either make it public (GitHub → the `d9ds3`
  package → Package settings → Change visibility → Public) or add an
  `imagePullSecret` referencing a GHCR token to the pod specs.
- To build/push manually instead of via CI: `docker build -t ghcr.io/adi/d9ds3:dev . && docker push ghcr.io/adi/d9ds3:dev`.

## Deploy
```sh
kubectl apply -f deploy/k8s/single-node.yaml      # or cluster.yaml
```
Change the `d9ds3-root` Secret before deploying. S3 is served at the Service; the
web console is at `http://<service>/console`.

Scaling notes for `cluster.yaml`:
- With `SHARDS > 1`, expose Raft ports `9001..9001+SHARDS-1` and run
  `POST /v1/rebalance` on `d9ds3-storage-0` once to spread shard leadership.
- Growing `replicas` adds nodes that join and receive a snapshot automatically;
  update the gateway's `--nodes` list to include the new pods.
