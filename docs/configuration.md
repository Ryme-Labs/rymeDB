# Configuration

`ryme-server` starts with `Config::default()` or reads a JSON file passed with
`--config`. CLI flags override the file. Unknown JSON fields are rejected so a
misspelled production setting cannot silently do nothing.

## Minimal file

```json
{
  "node_id": "node-0",
  "pg_listen": "0.0.0.0:5433",
  "resp_listen": "0.0.0.0:6380",
  "http_listen": "0.0.0.0:8080",
  "data_dir": "/var/lib/rymedb",
  "durability": "local-durable",
  "cache_bytes": 268435456,
  "max_connections": 10000
}
```

```bash
ryme-server --config /etc/rymedb/node.json
```

## Core fields

| Field | Default | Meaning |
| --- | --- | --- |
| `node_id` | `ryme-0` | Non-empty node name. |
| `pg_listen` | `127.0.0.1:5433` | PostgreSQL wire listener. |
| `resp_listen` | `127.0.0.1:6380` | RESP listener. |
| `http_listen` | `127.0.0.1:8080` | REST, GraphQL, WebSocket, and dashboard listener. |
| `data_dir` | `/var/lib/rymedb` | WAL, schema, auth, audit, branch, topic, snapshot, and placement state. |
| `durability` | `local-durable` | `strict`, `regional-fast`, `local-durable`, or `memory`. Only `memory` skips fsync. |
| `storage_mode` | `hot` | `hot` keeps the working set materialized; `standard` reads immutable segments through a bounded cache. |
| `cache_bytes` | `67108864` | Segment cache size. Minimum 1 MiB. |
| `max_connections` | `10000` | Node-wide connection limit for protocol gateways. |
| `http_max_body_bytes` | `2097152` | HTTP per-request limit; maximum 64 MiB. HTTP and HTTPS share a 256 MiB configured active-body budget. |
| `region` | `local-1` | Region label returned by placement APIs. |
| `read_only` | `false` | Reject mutations while serving reads. |
| `sweep_interval_secs` | `30` | TTL, MVCC, and retention sweep period; `0` disables it. |
| `shards` | `1` | Local shard count, from 1 to 256. Cluster mode currently requires 1. |
| `index_partitions` | `4` | Vector/text index partitions, from 1 to 64. |
| `replicated_tables` | `[]` | Tables using the clustered backend; requires `cluster.raft_listen`. |
| `autosplit_writes` | `0` | Range write threshold; `0` keeps autosplitting off. |
| `autosplit_interval_secs` | `30` | Range autosplit scan period; `0` disables it. |

Search indexes have fixed estimated-memory admission budgets shared across all
index spaces in one server process: 128 MiB for full-text structures and 256
MiB for vector/HNSW structures. Per-tenant ceilings are one quarter of those
budgets (32 MiB full-text, 64 MiB vector), so a single tenant cannot consume
the node-wide search-index allowance. The estimates include index metadata,
IDs, and retained HNSW tombstones. Full-text replacement/deletion releases its
previous reservation; vector removal releases ID-map accounting immediately,
while tombstoned graph-node accounting remains until compaction. Indexing is
rejected with an overload error when either budget is exhausted. These are
index-structure budgets, not caps on total process memory, and are not
currently configurable.

The native TCP/TLS gateway limits request bodies to 4 MiB plus 1 KiB, uses
16 KiB per-connection read buffers, and shares a 256 MiB budget across read
buffers and pending framed input.
Connections are closed when the process-wide input budget is exhausted; this
budget is not currently configurable.

The gRPC TCP/TLS gateway accepts protobuf requests up to 4 MiB plus 64 KiB,
allows up to 32 concurrent streams per connection, and caps active requests at
64 per listener. Additional streams wait for capacity.

## Listener flags

The equivalent CLI flags are `--pg-listen`, `--resp-listen`,
`--http-listen`, `--native-listen`, `--grpc-listen`, `--https-listen`,
`--resp-tls-listen`, `--native-tls-listen`, `--raft-listen`,
`--advertise-addr`, `--data-dir`, `--region`, and `--read-only`.

Optional TLS listeners require `tls_cert_pem` and `tls_key_pem`. Add
`tls_client_ca_pem` to require client certificates. `raft_tls` additionally
requires a Raft listener, server certificate/key, and client CA.
Non-loopback Raft listeners always require `raft_tls: true`; loopback-only
development Raft listeners may remain plaintext.

HTTP authentication requests are limited to 120 attempts per client address
and 5 attempts per credential/client pair in a 60-second window.
When HTTP/HTTPS is behind a reverse proxy,
set `RYME_TRUSTED_PROXY_CIDRS` to the comma-separated CIDRs of proxies that
connect directly to RymeDB (for example, `10.20.0.0/16,2001:db8:20::/48`).
Only those peers may supply `X-Forwarded-For`; the server walks the forwarded
chain from the trusted side and ignores untrusted header values. Chains over
32 hops fall back to the direct peer address for rate limiting. Configure the
proxy to append the actual client address. Leave this variable unset when
clients connect directly. Invalid CIDRs prevent startup.

:::danger[Bind deliberately]

Defaults are loopback-only. If you bind to `0.0.0.0`, enable TLS and
authentication, restrict firewall access, and set `cluster.advertise_addr`
explicitly for a cluster. Do not expose trust-mode development listeners.

Startup fails closed for non-loopback listeners unless the matching credentials
are configured: `RYME_API_KEY` for HTTP/native/gRPC APIs,
`RYME_PG_PASSWORD` for PostgreSQL, and `RYME_RESP_PASSWORD` for RESP. There is
no fixed fallback owner key. Credentials protecting non-loopback listeners
must contain at least 16 bytes; loopback-only development listeners retain
their local trust/development behavior.
:::

`RYME_PUBLIC_API_KEY` is an optional, intentionally public HTTP credential for
browser applications. It maps only to the read-only SQL `anon` role and does
not replace `RYME_API_KEY` for operator access or satisfy the external-listener
requirement. Its effective access is controlled by SQL grants and RLS policies;
see [authentication and RLS](auth.md#browser-applications). The Helm chart
accepts this public value through `publicApiKey`; keep the operator credential
in the chart's `apiKeySecret`; PostgreSQL and RESP passwords come from the
`protocolPasswordsSecret`.

## Cluster

```json
{
  "raft_tls": true,
  "tls_cert_pem": "/etc/rymedb/tls/tls.crt",
  "tls_key_pem": "/etc/rymedb/tls/tls.key",
  "tls_client_ca_pem": "/etc/rymedb/tls/ca.crt",
  "cluster": {
    "node_index": 0,
    "raft_listen": "10.0.0.10:9080",
    "advertise_addr": "10.0.0.10:9080",
    "peers": [
      { "id": 1, "addr": "10.0.0.11:9080" },
      { "id": 2, "addr": "10.0.0.12:9080" }
    ],
    "learner": false
  }
}
```

`node_index` must be unique. A learner catches up without campaigning. Cluster
membership changes are made through the leader; see [cluster operations](cluster-operations.md).

## Archive and observability

```json
{
  "archive": {
    "local_dir": "/backup/rymedb",
    "interval_secs": 300,
    "verify_interval_secs": 604800,
    "keep": 7,
    "snapshot_keep": 8
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service": "rymedb",
    "interval_secs": 30
  }
}
```

For S3-compatible storage, set `s3_endpoint`, `s3_bucket`, `s3_region`, and
optionally `s3_path_style`. HTTPS is required by default; set
`s3_allow_insecure_http` only for an intentionally isolated local/private
development endpoint. Configure `archive_replica` for a second target.
The OTLP endpoint is optional; `/metrics`, `/metrics/prometheus`,
`/v1/observe/slow`, and `/v1/traces` remain available locally without an OTLP
collector. These platform-wide operational routes require a configured
superuser credential.

## Tenant policies

`rls_tables` maps a table to the JSON field containing its tenant id:

```json
{ "rls_tables": { "messages": "tenant_id", "jobs": "account_id" } }
```

Table and field names must be nonempty, NUL-free, and at most 256 bytes.
Invalid mappings prevent the server from starting.

Requests authenticated as a tenant can only read or mutate matching rows.
For SQL-native policy metadata and migration behavior, read [auth and tenants](auth.md).

## Validation checklist

Before starting a node:

```bash
ryme-server --config /etc/rymedb/node.json --help
RYME_API_KEY=... ryme-server --config /etc/rymedb/node.json
curl -fsS -H "Authorization: Bearer $RYME_API_KEY" http://127.0.0.1:8080/ready
```

Common validation failures are a zero connection limit, an invalid shard
count, incomplete TLS paths, S3 endpoint without bucket/region, or
`replicated_tables` without `raft_listen`.
