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
| `data_dir` | `/var/lib/rymedb` | WAL, schema, auth, branch, topic, snapshot, and placement state. |
| `durability` | `local-durable` | `strict`, `regional-fast`, `local-durable`, or `memory`. Only `memory` skips fsync. |
| `storage_mode` | `hot` | `hot` keeps the working set materialized; `standard` reads immutable segments through a bounded cache. |
| `cache_bytes` | `67108864` | Segment cache size. Minimum 1 MiB. |
| `max_connections` | `10000` | Node-wide connection limit for protocol gateways. |
| `http_max_body_bytes` | `2097152` | HTTP request limit; maximum 64 MiB. |
| `region` | `local-1` | Region label returned by placement APIs. |
| `read_only` | `false` | Reject mutations while serving reads. |
| `sweep_interval_secs` | `30` | TTL, MVCC, and retention sweep period; `0` disables it. |
| `shards` | `1` | Local shard count, from 1 to 256. Cluster mode currently requires 1. |
| `index_partitions` | `4` | Vector/text index partitions, from 1 to 64. |
| `replicated_tables` | `[]` | Tables using the clustered backend; requires `cluster.raft_listen`. |
| `autosplit_writes` | `0` | Range write threshold; `0` keeps autosplitting off. |
| `autosplit_interval_secs` | `30` | Range autosplit scan period; `0` disables it. |

## Listener flags

The equivalent CLI flags are `--pg-listen`, `--resp-listen`,
`--http-listen`, `--native-listen`, `--grpc-listen`, `--https-listen`,
`--resp-tls-listen`, `--native-tls-listen`, `--raft-listen`,
`--advertise-addr`, `--data-dir`, `--region`, and `--read-only`.

Optional TLS listeners require `tls_cert_pem` and `tls_key_pem`. Add
`tls_client_ca_pem` to require client certificates. `raft_tls` additionally
requires a Raft listener, server certificate/key, and client CA.

:::danger[Bind deliberately]

Defaults are loopback-only. If you bind to `0.0.0.0`, enable TLS and
authentication, restrict firewall access, and set `cluster.advertise_addr`
explicitly for a cluster. Do not expose trust-mode development listeners.
:::

## Cluster

```json
{
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
optionally `s3_path_style`. Configure `archive_replica` for a second target.
The OTLP endpoint is optional; `/metrics`, `/metrics/prometheus`,
`/v1/observe/slow`, and `/v1/traces` remain available locally without an OTLP
collector. These operational routes still require an authenticated owner or
admin principal.

## Tenant policies

`rls_tables` maps a table to the JSON field containing its tenant id:

```json
{ "rls_tables": { "messages": "tenant_id", "jobs": "account_id" } }
```

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
