# Production operations

Target: low resource, high reliability, high availability.

## Resource profile

Defaults are edge-small: 64 MiB cache, 30 s expiry sweep, 5 min archive
interval, 10k connection admission cap. Raise `cache_bytes` to 256 MiB–2 GiB
only on performance nodes. Value cap 4 MiB and key cap 1 KiB are enforced
centrally in `Gateway::put_with_ttl` (`MAX_VALUE_BYTES`, `MAX_KEY_BYTES`,
empty table/key rejected), so REST, native, and gRPC share one shape
policy to protect replication and cache efficiency.

## Health and autoscaling

- `GET /health` liveness, `GET /ready` readiness with leader and commit.
- `GET /metrics` JSON with p50/p90/p95/p99, `GET /metrics/prometheus` text.
- `GET /v1/traces?limit=` newest-first request spans (trace/span ids, name,
  timing, table attributes) from the in-memory collector fed by the
  kv/sql/copy handlers and every wire gateway (RESP, PG, native, gRPC).
  Append `&name=<op>` and/or `&table=<name>` to narrow the feed.
  gRPC callers may send W3C `traceparent` metadata to link server spans
  into the incoming trace instead of starting a new root. Set
  `otel.endpoint` (plus optional `otel.service`,
  default `rymedb`, and `otel.interval_secs`, default 30) to also forward
  drained spans as OTLP/HTTP+JSON to `{endpoint}/v1/traces`; empty batches
  send nothing and failed exports are dropped with a warning, never retried
  into an unbounded queue.
- `GET /v1/observe/slow?limit=` newest-first recent operations over 5ms with  kind, fingerprint, table, micros and unix timestamp. Only normalized
  fingerprints are stored, never raw statements; the ring holds 256 entries.
  Append `&table=<name>` to narrow the feed to one table (filter-then-take,
  newest-first).
  The shared latency window, histogram and slow-log are also fed by the RESP
  (per command plus `EXEC` batches) and PostgreSQL wire (per statement)
  gateways, so `/metrics` and the slow feed cover all protocols.
- `GET /v1/metering` idempotent usage aggregates for billing downstream.
- `GET /v1/autoscale?cpu_pct=&disk_used_pct=&...` returns pressure and action:
  `add_gateways_or_compute`, `split_hot_range`, `add_follower`,
  `add_realtime_partition`, `add_io_capacity_or_throttle_writes`, `hold`.
  Scale on QPS, p99 queueing, Raft lag, sockets, compaction debt, not CPU alone.

## Reliability

- WAL with CRC segments, quorum Raft, tiered durability
  (`strict`, `regional-fast`, `local-durable`, `memory`).
- PITR checkpoints, immutable snapshots, local/S3 archive with retention.
- Branch metadata is copy-on-write; shared segments are GC-safe.
- Table-sharded transactions commit atomically: single-shard writes take
  the fast path, multi-shard writes run 2PC (validate every participating
  shard including read-only ones, fsync a coordinator decision record,
  then apply per shard). Coordinator recovery replays decisions on open
  with duplicate-aware apply, so a crash between shard applies still
  converges. Table moves serialize against commits.
- Chaos coverage: multi-node E2E, SIGKILL recovery, admission control,
  joint-consensus membership, DNS peers.

## Security

- Bearer API keys plus HMAC JWT, constant-time comparison, separate issuers.
- Password credentials (salted stretched SHA-256), TOTP OTP with skew window,
  WebAuthn passkeys (P-256 registration, challenge issuance with 5-minute
  expiry and one-shot consumption, assertion verification covering rpId
  hash, user presence, origin allow-list, sign-count monotonicity, and
  ECDSA; configure `passkey_rp_id` plus `passkey_origins`, attestation
  verification is out of scope in v1), per-table JSON column masking on
  every read path (mask rules are operator-managed: setting them needs
  `Owner`/`Admin`, so application keys cannot silently unmask PII).
- RBAC roles with RLS predicates compiled before execution.
- TLS at ingress, mTLS service-to-service, envelope encryption for snapshots
  (`ryme-crypto` key ring with rotation; volume encryption remains the outer layer).
- QoS tiers (`shared`, `dedicated_shard`, `dedicated_cluster`) with token-bucket
  admission on reads, writes, egress, realtime and connections (`GET /v1/qos`).
  Tier changes (`POST /v1/qos/tier`) and shard moves (`POST /v1/shards/move`)
  are scoped to the caller's own tenant; cross-tenant targets get `403`, so
  one tenant cannot retune or relocate another's capacity. On REST, reads
  draw from the read bucket (including `GET`, scan, GraphQL, SQL reads and
  TTL), deletes and SQL writes draw from the write bucket, and every
  materialized response draws its pk/value bytes from the egress bucket
  (`429` when exhausted).
  Enforced on every gateway: REST, native, RESP (per-command, classified by
  staged writes so reads and `MULTI`/`EXEC` batches are metered precisely) and
  PostgreSQL wire (per statement via `Statement::is_write`, throttle surfaces
  as `53400`). RESP and native gateways additionally charge response bytes
  to the egress bucket, so bulk reads cannot outrun the tier. The PG wire
  gateway charges full simple-query and extended-execute responses the
  same way (denial keeps protocol sync via error plus ready-for-query),
  and gRPC charges exact encoded reply bytes per RPC
  (`resource_exhausted` on exhaustion). Denied RESP
  commands answer `-ERR ... quota`; denied writes
  never reach commit. Each admitted command also records a read/write unit in
  metering, so wire-protocol traffic is billed like REST. The native gateway
  meters and times every op too, and its `sql` op classifies statements so
  writes draw from the write bucket instead of slipping through as reads. Storage-byte
  accounting reconciles from actual engine bytes on the sweep cadence
  (`Backend::reconcile_qos`), so deletes and overwrites release quota instead
  of accumulating forever.

## Native protocol

`ryme-native/1` framed JSON over TCP (u32 length prefix, 4 MiB cap):
`ping`, `get`, `put` (with `ttl_secs`), `delete`, `scan`, `sql`.
Enable with `native_listen` / `--native-listen`; QoS and masking enforced.

## gRPC

`crates/ryme-wire-grpc/proto/ryme.proto` (`rymedb.v1.Ryme`): `Health`,
`KvGet`/`KvPut`/`KvDelete`, `Scan`, `Sql` over tonic, values as bytes.
Enable with `grpc_listen` / `--grpc-listen` (plaintext loopback/dev only
in v1, like PG/RESP). `grpc_tls_listen` / `--grpc-tls-listen` serves the
same API over TLS using the node cert/key with standard client
verification (`tls_gateways_e2e`-style coverage in `ryme-wire-grpc`
`tls.rs`); Bearer API-key auth, per-RPC QoS,
metering and latency/histogram/slow observability mirror the native
gateway, including SQL write classification. Per-connection concurrency is
capped by `max_connections` (`serve_with_incoming_limited` /
`serve_tls_limited`; standalone `serve` defaults to 1024). CDC publishes through the
shared `Gateway`, so realtime subscribers see gRPC writes.

## TLS and regions

- Native gateway TLS: set `native_tls_listen` plus `tls_cert_pem`/`tls_key_pem`
  (rustls, ring provider, no client auth in v1). Generate with
  `openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=rymedb"`.
  HTTP stays plaintext behind a terminating reverse proxy, or set
  `https_listen` (`--https-listen`) to serve the full HTTP API including
  WebSockets over TLS via a rustls+hyper accept loop with admission caps
  (`https_e2e` proves handshake + routed request). PostgreSQL wire answers
  `SSLRequest` with `S` and upgrades in place when a cert/key is configured
  (`N` + plaintext otherwise). RESP has no in-band upgrade, so set
  `resp_tls_listen` (`--resp-tls-listen`) for a dedicated TLS RESP port
  (`tls_gateways_e2e` proves both). Mutual TLS: set `tls_client_ca_pem`
  (`--tls-client-ca-pem`) to require service clients to present a
  CA-chained certificate (bare clients get `CertificateRequired`).
- Raft mesh TLS: set `raft_tls` (`--raft-tls`, requires the same cert/key
  plus client CA) to encrypt and mutually authenticate every inter-node
  RPC — votes, appends, snapshots — with hostname-verified server certs.
  `mesh_tls_elect_and_replicate` proves election plus replication over the
  mesh. Nodes share one mesh identity in v1; per-node identities are future
  work. PG/RESP plaintext listeners stay available for loopback/dev only.
- PostgreSQL wire: simple protocol accepts multi-statement strings (split
  quote-aware, `--` comments stripped), session `SET`/`SHOW`/`RESET`,
  and SQL-level `PREPARE name AS stmt` / `EXECUTE name [(params)]` /
  `DEALLOCATE name|ALL` backed by the per-connection statement registry;
  errors carry the protocol-mandated terminator. `psql_smoke` drives all
  of it through real `psql`, including the extended Parse/Bind flow.
  Extended-protocol portals cache their parsed AST (keyed by bound text, so
  re-prepares can never serve a stale plan); repeated `Execute` on one
  portal skips bind+parse entirely.
- Regions: each node reports `GET /v1/regions` (`region`, `read_only`,
  leadership, commit). Followers run `--read_only`: reads serve locally with
  bounded staleness, every mutation path (KV/REST/SQL/COPY/PG-wire/RESP/native/
  gRPC/branches/backups/topics/index/auth-writes) is rejected — `503` over
  REST, `25006` over the PG wire, `READONLY` over RESP. Strong multi-region
  writes are WAN-RTT bound; do not advertise sub-ms for them. See
  `docs/performance.md`.

## Runbook

Leader loss: elect follower, reroute, replace node, verify quorum and p99.
Disk pressure: throttle writes, add I/O, prune archives, verify margin.
Realtime backlog: repartition fanout, scale gateway, verify event lag.
Tenant overload: enforce quotas, isolate shard group, verify neighbor SLOs.
