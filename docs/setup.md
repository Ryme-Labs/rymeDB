# Setup rymeDB

Choose one of the three deployment paths below. Docker is the quickest way to
try rymeDB, Kubernetes is for a cluster deployment, and running from source is
best for development. All examples use the default PostgreSQL, RESP, and HTTP
ports: `5433`, `6380`, and `8080`.

Use a unique API key outside local testing. For production settings, TLS,
backups, and operational guidance, see [configuration](configuration.md),
[security](security.md), and [production operations](production.md).

## Docker

Install Docker with BuildKit, then pull the published image:

```bash
docker pull ghcr.io/ryme-labs/rymedb:latest
```

Create a persistent volume and start a single node:

```bash
docker volume create rymedb-data
docker run --name rymedb --rm \
  -e RYME_API_KEY=local-dev-key \
  -p 5433:5433 -p 6380:6380 -p 8080:8080 \
  -v rymedb-data:/var/lib/rymedb \
  ghcr.io/ryme-labs/rymedb:latest \
  --pg-listen 0.0.0.0:5433 --resp-listen 0.0.0.0:6380 --http-listen 0.0.0.0:8080
```

The explicit listener addresses are needed because Docker port forwarding
cannot reach a process bound only to the container's loopback interface. The
volume stores database state; do not mount it read-write into multiple
containers.

Check readiness and connect over PostgreSQL and RESP:

```bash
curl -fsS -H 'Authorization: Bearer local-dev-key' http://127.0.0.1:8080/ready
psql 'postgresql://postgres@127.0.0.1:5433/postgres' -c 'SELECT 1;'
redis-cli -p 6380 PING
```

To build the image from a checkout instead, run
`docker buildx build --load -f deploy/docker/Dockerfile.server -t rymedb:local .`
and use `rymedb:local` in the `docker run` command.

## Kubernetes

This path uses the Helm chart, which configures a three-node Raft cluster by
default. Install `kubectl`, Helm, and a cluster with a persistent-volume
storage class. The chart defaults to a storage class named `nvme`; override it
if your cluster uses another name.

Create a namespace and API-key Secret before installing the chart. The prompt
below avoids putting the key itself in your shell history:

```bash
kubectl create namespace rymedb
read -rsp 'API key: ' RYME_API_KEY; echo
kubectl -n rymedb create secret generic rymedb-api-key \
  --from-literal=api-key="$RYME_API_KEY"
unset RYME_API_KEY
```

Install the chart, setting the image tag and your cluster's storage class:

```bash
helm upgrade --install rymedb deploy/k8s/helm/rymeDB \
  --namespace rymedb \
  --set image.repository=ghcr.io/ryme-labs/rymedb \
  --set image.tag=0.1.0 \
  --set persistence.storageClass=YOUR_STORAGE_CLASS
```

Wait for the pods and inspect cluster status:

```bash
kubectl -n rymedb rollout status statefulset/rymedb
kubectl -n rymedb get pods -o wide
kubectl -n rymedb port-forward service/rymedb 8080:8080 5433:5433 6380:6380
```

In another terminal, verify the forwarded endpoints:

```bash
curl -fsS -H 'Authorization: Bearer replace-with-a-long-random-key' http://127.0.0.1:8080/ready
psql 'postgresql://postgres@127.0.0.1:5433/postgres' -c 'SELECT 1;'
redis-cli -p 6380 PING
```

The chart's Service is internal to the cluster; use an intentional ingress or
load balancer for remote access, and configure TLS before exposing client
traffic. A single-node StatefulSet manifest is also available at
[`deploy/k8s/base/statefulset.yaml`](https://github.com/Ryme-Labs/rymeDB/blob/main/deploy/k8s/base/statefulset.yaml).

## Run from source

Install Rust using the repository's pinned toolchain. For PostgreSQL and RESP
smoke tests, also install `psql` and `redis-cli` (or `valkey-cli`). Clone the
repository and verify the Rust toolchain:

```bash
git clone https://github.com/Ryme-Labs/rymeDB.git
cd rymeDB
rustup show active-toolchain
```

Start a local server with loopback-only listeners and a local data directory:

```bash
export RYME_API_KEY=local-dev-key
cargo run --locked -p ryme-server -- --http-listen 127.0.0.1:8080 --pg-listen 127.0.0.1:5433 --resp-listen 127.0.0.1:6380 --data-dir ./data
```

Cargo's `--` separates Cargo options from server options. The server accepts
listener flags directly; there is no `serve --listen` subcommand. Run
`cargo run --locked -p ryme-server -- --help` to see the available options.

Verify readiness and try SQL, HTTP key/value, and RESP:

```bash
curl -fsS -H 'Authorization: Bearer local-dev-key' http://127.0.0.1:8080/ready
psql 'postgresql://postgres@127.0.0.1:5433/postgres' -c 'SELECT 1;'

curl -fsS -X PUT \
  -H 'Authorization: Bearer local-dev-key' \
  -H 'Content-Type: application/json' \
  --data '{"value":"hello"}' \
  http://127.0.0.1:8080/v1/kv/cache/greeting

redis-cli -p 6380 SET greeting hello
redis-cli -p 6380 GET greeting
```

Stop the foreground server with Ctrl-C. Its data is in `./data`; preserve or
back it up before resetting a development database. Set `RUST_LOG=info` or
`RUST_LOG=debug` when you need more diagnostic output.

The [npm/TypeScript](sdk-js.md) and [Rust](sdk-rust.md) guides show how to
connect with the supported SDKs. For issues, see
[troubleshooting](troubleshooting.md); contributors can continue with the
[development setup](development.md).
