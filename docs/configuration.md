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
| `http_max_body_bytes` | u64 | `2097152` (2 MiB) | Explicit `DefaultBodyLimit` on HTTP and HTTPS routers; oversize JSON bodies get `413`. Must be between 1 byte and 64 MiB. |
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

Authentication is selected by the credential presented by the client. Keep
the data directory private and use TLS for non-loopback deployments.

### HTTP, REST, and WebSocket

| Setting or credential | Default | Behavior |
| --- | --- | --- |
| `RYME_API_KEY` | `ryme-dev-key` | Seeds the local API key. Send it as `Authorization: Bearer <key>` or `x-api-key`; streams also accept `?api_key=`. |
| Unknown bearer token | — | Returns `401` when JWT verification is not configured or the token is invalid. |
| Mutating request | — | Requires a write-capable principal; otherwise returns `403`. |

### PostgreSQL and RESP

| Environment variable | Protocol | Behavior when set | Behavior when unset |
| --- | --- | --- | --- |
| `RYME_PG_PASSWORD` | PostgreSQL wire | Enables password authentication. The configured password selects the default tenant; an API key or JWT used as the password selects its tenant. | Trust mode for local development. |
| `RYME_RESP_PASSWORD` | RESP | Enables `AUTH` with the same tenant selection rules. | Trust mode; `AUTH` returns the compatibility error. |

Both protocols use their normal password exchange. Configure the TLS listener
before exposing either protocol outside the local host.

### JWT and OIDC

| Setting | Purpose |
| --- | --- |
| `RYME_JWT_SECRET` | Verify HS256 tokens. |
| `RYME_JWT_JWKS_FILE` | Verify RS256 tokens from a mounted JSON JWKS file. The file takes precedence over the URL. |
| `RYME_JWT_JWKS_URL` | Fetch a reachable JWKS endpoint during startup with a 10-second timeout. |
| `RYME_JWT_JWK_KID` | Restrict verification to one mounted key id. |
| `RYME_JWT_ISSUER` + `RYME_JWT_AUDIENCE` | Enforce the matching token claims when both are set. |
| `RYME_OIDC_JWKS_FILE` / `RYME_OIDC_JWKS_URL` | Override the JWKS source used by `/v1/auth/oidc/token`. |
| `RYME_OIDC_JWK_KID`, `RYME_OIDC_ISSUER`, `RYME_OIDC_AUDIENCE` | Restrict the OIDC key and validate its issuer and audience. |

JWT principals accept rymeDB claims (`tenant`, `roles`) and common Supabase
claims (`tenant_id`, `role`, and `app_metadata.roles`). Role names are
case-insensitive: `anon` is read-only, `authenticated` is read/write, and
`service_role` is the owner role. Expiration, not-before, and future issued-at
checks use the configured clock-skew leeway.

### On-disk auth state

`data_dir/auth.json` is written atomically and stores password hashes,
OTP/passkey metadata, API-key digests, and refresh-token digests. Raw secrets,
refresh tokens, and WebAuthn challenges are never written there. SQL table and
index definitions are stored separately in `data_dir/schema.json`; secondary
indexes are rebuilt from committed rows at startup.
