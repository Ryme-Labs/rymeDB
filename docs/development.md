# Development guide

The core is Rust. TypeScript is used for the operator CLI and npm SDK; Java
and Rust SDKs are thin protocol clients. Keep gateway and platform features
outside the storage/transaction hot path when possible.

## Repository map

| Path | Responsibility |
| --- | --- |
| `crates/ryme-storage` | WAL, segments, cache, snapshots |
| `crates/ryme-txn` | MVCC transactions and conflict validation |
| `crates/ryme-sql` | SQL parsing/execution and schema metadata |
| `crates/ryme-raft` | Membership, quorum log, range replication |
| `crates/ryme-wire-pg` | PostgreSQL wire protocol |
| `crates/ryme-wire-resp` | RESP protocol and commands |
| `crates/ryme-realtime` | CDC, presence, broadcast, durable topics |
| `crates/ryme-server` | listeners, auth, HTTP/WebSocket routes |
| `apps/cli` | `rymedb-cli` operator CLI |
| `sdks/` | Java, npm/TypeScript, and Rust clients only |
| `schemas/openapi` | REST contract and validator |
| `docs/` | Docusaurus documentation source |

## Local setup

```bash
git clone https://github.com/Ryme-Labs/rymeDB.git
cd rymeDB
rustup show active-toolchain
pnpm install --frozen-lockfile
```

Protobuf is required by the gRPC build. On Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install --yes protobuf-compiler postgresql-client
```

## Required checks

```bash
cargo fmt --package ryme-sql --package ryme-wire-resp --package ryme-wire-pg --package ryme-raft -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
pnpm --dir sdks/js build
pnpm --dir sdks/js test
mvn -f sdks/java/pom.xml verify
cargo test --manifest-path sdks/rust/Cargo.toml --locked
python3 schemas/openapi/validate.py
pnpm docs:dev
pnpm docs:build
```

The CI workflows run on `ubuntu-24.04`, install `protoc` and `psql`, test the
protocols, build Pages, and publish only the Java, npm/JS, and Rust SDKs.

## Adding a feature

1. Write the protocol/API behavior and failure cases first.
2. Add unit and integration tests at the narrowest crate boundary.
3. Ensure all write paths publish exactly once to the committed CDC path when
   the feature is data-bearing.
4. Update OpenAPI, SDK methods, CLI coverage, and docs together.
5. Add a benchmark when the feature affects latency, throughput, memory, or
   connections.
6. Run the complete verification suite and use a descriptive commit authored
   as `deyoyk <karthikw7755@gmail.com>` for repository work.

## Benchmark development

Use `ryme-bench` with JSON output and preserve raw results under `bench/results`.
State the machine, data shape, durability, QoS tier, protocol, concurrency,
and error count. Do not tune a benchmark by weakening correctness semantics.

## Documentation development

Docs use Docusaurus. Run `pnpm docs:dev` for a local preview and
`pnpm docs:build` to validate static output. Keep paragraphs short, put options
in tables, keep commands on one line unless shell continuation is intentional,
and link to the generated OpenAPI reference rather than duplicating schemas.
