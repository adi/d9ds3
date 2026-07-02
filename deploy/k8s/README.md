# Kubernetes deployment

Two examples:

- **`single-node.yaml`** — one `d9ds3 standalone` pod (storage + gateway in one
  process). Durable local storage, no replication/failover. Dev / CI / edge / small.
- **`cluster.yaml`** — a 3-node storage `StatefulSet` (Raft) behind a headless
  Service for peer discovery, plus a stateless gateway `Deployment`. Quorum
  durability, survives a node loss.

Both keep **object data** (`--data`) and **Raft consensus state** (`--raft-dir`) on
separate volumes: `--data` is the portable backup/rsync surface; `--raft-dir` is
node-local and must never be copied between nodes or restored independently.

## Build & push the image
```sh
docker build -t ghcr.io/adi/d9ds3:latest .
docker push ghcr.io/adi/d9ds3:latest
```

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
