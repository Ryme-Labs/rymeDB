# Configuration reference

`ryme-server` loads JSON into `ryme_config::Config`
(`crates/ryme-config/src/lib.rs`, `Config::from_file`) or starts from
`Config::default()`. CLI flags (`crates/ryme-server/src/main.rs`) override the
file for listeners, raft, advertise address, and data dir. `Config::validate`
runs on every boot.

## Top-level fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `node_id` | string | `"ryme-0"` | Must be non-empty. Reported by `/ready` and `/metrics`. |
| `pg_listen` | socket addr | `127.0.0.1:5433` | PostgreSQL wire listener. |
| `resp_listen` | socket addr | `127.0.0.1:6380` | RESP listener. |
| `native_listen` | socket addr / null | null | Native framed-JSON listener (`ryme-native/1`); unset disables it. Flag: `--native-listen`. |
| `native_tls_listen` | socket addr / null | null | TLS native listener; requires `tls_cert_pem` + `tls_key_pem`. Flag: `--native-tls-listen`. |
| `tls_cert_pem` | path / null | null | PEM certificate chain for the TLS native listener. Flag: `--tls-cert-pem`. |
| `tls_key_pem` | path / null | null | PEM private key (PKCS8/PKCS1/SEC1) for the TLS native listener. Flag: `--tls-key-pem`. |
| `tls_client_ca_pem` | path / null | null | PEM CA bundle requiring mutual TLS clients to present a cert chain. Requires cert+key. Flag: `--tls-client-ca-pem`. |
| `https_listen` | socket addr / null | null | TLS HTTP listener serving the full REST/WS API via rustls+hyper. Requires cert+key. Flag: `--https-listen`. |
| `resp_tls_listen` | socket addr / null | null | Dedicated TLS RESP listener (same cert/key). Flag: `--resp-tls-listen`. |
| `region` | string | `"local-1"` | Placement region reported by `GET /v1/regions`. Flag: `--region`. |
| `read_only` | bool | false | Follower mode: serve reads, reject mutations with 503. Flag: `--read-only`. |
| `raft_tls` | bool | false | Mutual-TLS Raft mesh: every inter-node RPC handshakes with CA-chained certs. Requires `raft_listen`, `tls_cert_pem`, `tls_key_pem` and `tls_client_ca_pem`. Flag: `--raft-tls`. |
| `http_listen` | socket addr | `127.0.0.1:8080` | REST listener. |
| `data_dir` | path | `/var/lib/rymedb` | WAL, snapshots, Raft state, main SQL schema metadata, and tenant-scoped branch schemas live here. |
| `durability` | enum | `"local-durable"` | One of `strict`, `regional-fast`, `local-durable`, `memory` (kebab-case). The three durable tiers share one implementation in v1: fsync per commit plus Raft quorum where clustered. Only `memory` relaxes durability (no fsync); never select it for data you cannot lose. Pinned by `only_memory_relaxes_durability`. |
| `storage_mode` | enum | `"hot"` | `hot` keeps the working set materialized in memory; `standard` starts from immutable NVMe segments, uses the bounded segment cache for point reads, and evicts committed rows from the resident engine after durable publication. |
| `cache_bytes` | u64 | `67108864` (64 MiB) | Bounds the decoded immutable-segment read cache. Must be >= 1 MiB. Raise to 256 MiB–2 GiB on performance nodes. |
| `max_connections` | u32 | `10000` | Must be non-zero. Excess PG/RESP connections get `53300` / `-ERR overloaded`; excess HTTP/HTTPS TCP connections are closed immediately; gRPC per-connection concurrency is capped at the same value. |
| `http_max_body_bytes` | const | `2097152` (2 MiB) | Explicit `DefaultBodyLimit` on the HTTP router; oversize JSON bodies get `413`. Not yet operator-tunable in v1. |
| `archive` | object | `{}` | See below. |
| `archive_replica` | object / null | null | Second archive target (same shape as `archive`) for cross-region copies via `POST /v1/backups/copy`. |
| `cluster` | object | `{}` | See below. Unset `raft_listen` means single-node. |
| `sweep_interval_secs` | u64 | `30` | TTL sweeper period. `0` disables sweeping. |
| `autosplit_writes` | u64 | `0` | Per-range counted-write threshold for automatic splits. `0` disables accounting and the background loop. The loop never splits while 256+ ranges exist (merge or raise via API). Flag: `--autosplit-writes`. |
| `autosplit_interval_secs` | u64 | `30` | Background auto-split period. `0` disables the loop. Flag: `--autosplit-interval-secs`. |
| `otel` | object | `{}` | OTLP trace export: `endpoint` (e.g. `http://collector:4318`), `service` (default `"rymedb"`), `interval_secs` (default `30`). Unset `endpoint` disables export. Config file only. |
| `passkey_rp_id` | string | `""` | Relying-party id WebAuthn assertions are verified against (SHA-256 compared to the authenticator-data rpId hash). Empty disables passkey login. Config file only. |
| `passkey_origins` | string[] | `[]` | Exact-match allow-list for the client-data `origin` during assertion verification. Config file only. |
| `rls_tables` | object | `{}` | Tenant-column policies as `{ "table": "tenant_column" }`. Gateway reads, scans, and writes only allow JSON rows whose configured tenant column matches the authenticated tenant. Config file only. |
| `shards` | usize | `1` | Range 1–256. Cannot exceed 1 when `raft_listen` is set. |
| `index_partitions` | usize | `4` | Range 1–64. Hash partitions for vector/text index spaces; writes route by id, reads fan out and merge. |
| `replicated_tables` | string[] | `[]` | Non-empty requires `raft_listen` (selects the Hybrid backend). |

Realtime WebSocket sessions also consume the authenticated tenant's QoS
`max_connections` quota. The session is released automatically when the
socket closes, while the node-level `max_connections` limit still protects
the HTTP listener itself. Each delivered realtime event also consumes one
`realtime_msg_per_sec` token and its serialized bytes consume the egress
bucket; heartbeat and ping/pong control frames are not counted.

## `cluster`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `node_index` | usize | `0` | This node's Raft id; must be unique in the set. |
| `raft_listen` | socket addr / null | null | When set, the node boots `serve_cluster`. |
| `peers` | `{id, addr}[]` | `[]` | Other members. `addr` is a hostname or IP string, resolved at dial time. |
| `learner` | bool | `false` | Joiners boot as non-campaigning learners until admitted. |
| `advertise_addr` | string / null | null | Address peers dial for this node. Falls back to `raft_listen` unless that is a wildcard; always set it when listening on `0.0.0.0`. |

## `archive`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `local_dir` | path / null | null | Local archive target. |
| `s3_endpoint` | string / null | null | S3-compatible endpoint. Requires bucket + region. |
| `s3_bucket` | string / null | null | Bucket name. |
| `s3_region` | string / null | null | Region name. |
| `s3_path_style` | bool | `false` | Path-style addressing for S3 compatibles. |
| `interval_secs` | u64 | `300` | Background archive period. `0` disables the loop. |
| `verify_interval_secs` | u64 | `0` | Scheduled restore-drill period against the latest backup. `0` disables. Set `604800` in production; last report at `GET /v1/backups/drill`. Flag: `--verify-interval-secs`. |
| `keep` | usize | `7` | Retained archives. |
| `snapshot_keep` | usize | `8` | Retained local snapshots per shard. Older snapshots are pruned by the sweep loop; WAL segments fully below the oldest retained snapshot are then truncated. |

## Auth

The server seeds one API key from `RYME_API_KEY`, defaulting to
`ryme-dev-key` when unset. Send it as `Authorization: Bearer <key>` or the
`x-api-key` header (streams also accept `?api_key=`). JWT verification applies
when configured; otherwise an unknown Bearer token is `401`. Mutating
endpoints additionally require a write-capable principal (`403` without it).

Set `RYME_PG_PASSWORD` to enable PostgreSQL wire authentication. Clients use
the normal PostgreSQL `user` and `password` startup fields; the configured
password selects the default tenant, while an API key or JWT supplied as the
password selects its authenticated tenant. When unset, the embedded wire
gateway retains trust-mode behavior for local development.

Set `RYME_RESP_PASSWORD` to enable RESP `AUTH` and the same tenant selection
behavior. Without it, RESP remains in trust mode for local development and
returns the normal compatibility error for `AUTH`.

Both mechanisms use the protocol’s password exchange; use the TLS listeners
for non-loopback deployments.

For OIDC/JWT verification, set `RYME_JWT_SECRET` for HS256 or set
`RYME_JWT_JWKS_FILE` to a JSON JWKS file containing RSA signing keys for
RS256. The verifier selects a matching `kid` from the token, which permits key
rotation; `RYME_JWT_JWK_KID` can restrict the mounted set to one key. Set
`RYME_JWT_ISSUER` and `RYME_JWT_AUDIENCE` together to enforce those claims.
Use `RYME_JWT_JWKS_URL` instead when the provider exposes a reachable JWKS
endpoint; the file setting takes precedence and the URL is fetched with a
10-second timeout during startup.
The `/v1/auth/oidc/token` exchange can use the same mounted file, or an
explicit `RYME_OIDC_JWKS_FILE`/`RYME_OIDC_JWKS_URL` and optional
`RYME_OIDC_JWK_KID`; its OIDC issuer and audience are taken from
`RYME_OIDC_ISSUER` and `RYME_OIDC_AUDIENCE`.

Application-auth state is stored atomically in `data_dir/auth.json`. It contains
password hashes, OTP/passkey metadata, API-key digests, and refresh-token
digests; raw API keys, passwords, refresh tokens, and WebAuthn challenges are
not written to that file. Keep the data directory private and back it up with
the same controls as the database WAL.

SQL table and index metadata is stored atomically in `data_dir/schema.json`.
The server rebuilds secondary-index entries from committed rows during startup;
the file contains definitions only, not application data.
