# Setup from A to Z

This page gets a developer from an empty checkout or Docker host to a tested
database connection. For production hardening, continue with [configuration](configuration.md),
[security](security.md), and [production operations](production.md).

## 1. Prerequisites

| Path | Requirement |
| --- | --- |
| Run the server | Rust 1.89 or the pinned toolchain in `rust-toolchain.toml` |
| JavaScript SDK/CLI | Node.js 18+, pnpm, and TypeScript for local builds |
| Java SDK | JDK 17+ and Maven |
| Rust SDK | Rust and Cargo |
| PostgreSQL compatibility | `psql` |
| RESP compatibility | `redis-cli` or `valkey-cli` |
| Container path | Docker with BuildKit |

Clone the repository and verify the toolchain:

```bash
git clone https://github.com/Ryme-Labs/rymeDB.git
cd rymeDB
rustup show active-toolchain
```

## 2. Run from source

```bash
export RYME_API_KEY=local-dev-key
cargo run --locked -p ryme-server -- --http-listen 127.0.0.1:8080 --pg-listen 127.0.0.1:5433 --resp-listen 127.0.0.1:6380 --data-dir ./data
```

The `--` is Cargo's separator. `ryme-server` accepts listener flags directly;
there is no `serve --listen` subcommand. Use `cargo run --locked -p
ryme-server -- --help` to see every flag.

## 3. Health check

```bash
curl -fsS -H 'Authorization: Bearer local-dev-key' http://127.0.0.1:8080/health
curl -fsS -H 'Authorization: Bearer local-dev-key' http://127.0.0.1:8080/ready
curl -fsS -H 'Authorization: Bearer local-dev-key' http://127.0.0.1:8080/metrics
```

`/health` is a liveness check. `/ready` reports node, commit, leader, and
cluster state. `/metrics` returns JSON; `/metrics/prometheus` returns scrape
text.

## 4. First data

### SQL

```bash
psql 'postgresql://postgres@127.0.0.1:5433/postgres' <<'SQL'
CREATE TABLE messages (
  id TEXT PRIMARY KEY,
  room TEXT NOT NULL,
  body TEXT NOT NULL
);
INSERT INTO messages VALUES ('m-1', 'lobby', 'hello');
SELECT * FROM messages;
SQL
```

### HTTP key/value

```bash
curl -fsS -X PUT \
  -H 'Authorization: Bearer local-dev-key' \
  -H 'Content-Type: application/json' \
  --data '{"value":"hello"}' \
  http://127.0.0.1:8080/v1/kv/cache/greeting

curl -fsS -H 'Authorization: Bearer local-dev-key' \
  http://127.0.0.1:8080/v1/kv/cache/greeting
```

### RESP

```bash
redis-cli -p 6380 SET greeting hello
redis-cli -p 6380 GET greeting
```

## 5. Run with Docker

Pull the published image:

```bash
docker pull ghcr.io/ryme-labs/rymedb:latest
```

Run a persistent single node. The container must override the loopback
defaults because Docker port forwarding reaches the container network:

```bash
docker volume create rymeDB-data
docker run --name rymeDB --rm \
  -e RYME_API_KEY=local-dev-key \
  -p 5433:5433 -p 6380:6380 -p 8080:8080 \
  -v rymeDB-data:/var/lib/rymedb \
  ghcr.io/ryme-labs/rymedb:latest \
  --pg-listen 0.0.0.0:5433 --resp-listen 0.0.0.0:6380 --http-listen 0.0.0.0:8080
```

The image exposes PostgreSQL on 5433, RESP on 6380, and HTTP on 8080. The
volume contains WAL, schema, authentication, branches, topics, and snapshots.
Do not mount a production volume read-write into multiple containers.

Build the image locally:

```bash
docker buildx build --load -f deploy/docker/Dockerfile.server -t rymeDB:local .
```

## 6. Install a client

| Client | Local checkout | Published package |
| --- | --- | --- |
| npm/TypeScript | `pnpm --dir sdks/js build` | `@rymelabs/rymedb` |
| Rust | `cargo test --manifest-path sdks/rust/Cargo.toml --locked` | `rymedb-client` |
| Java | `mvn -f sdks/java/pom.xml verify` | `com.rymelabs:rymedb-client` |

The SDK pages contain complete connection examples. Raw HTTP, WebSocket,
PostgreSQL, and RESP clients are first-class supported paths too.

## 7. Stop, reset, and inspect

Stop the foreground server with Ctrl-C. To reset a local development database,
stop all clients and move the explicit data directory to a backup location;
never remove an unknown path recursively. For Docker, stop the container and
inspect the volume with `docker volume inspect rymeDB-data`.

Inspect logs with the process' stderr or your service manager. Set
`RUST_LOG=info`/`RUST_LOG=debug` when the binary is run with tracing support.
Use `/v1/observe/slow`, `/v1/traces`, and `/metrics` before changing code.

## 8. Run the verification suite

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
```

CI additionally builds the Pages site, validates OpenAPI, checks the three
SDK directories, and exercises PostgreSQL compatibility with `psql`.
