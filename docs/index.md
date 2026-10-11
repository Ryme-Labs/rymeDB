# rymeDB documentation

rymeDB is a Rust-first distributed database and backend platform for realtime
chat, AI agents, gateways, games, and collaborative applications.

The project combines a low-latency key/value path with relational SQL,
MVCC transactions, branch-local schemas, durable realtime streams,
PostgreSQL-compatible access, REST, WebSockets, authentication, and
horizontal range placement.

<img class="docs-banner" src="images/rymedb-banner.png" alt="rymeDB">

## Start quickly

Run a local server from the repository:

```sh
cargo run --locked -p ryme-server -- serve --listen 127.0.0.1:8080
```

Then check readiness:

```sh
curl http://127.0.0.1:8080/ready
```

Use the [configuration guide](configuration.md) for durable storage, TLS,
authentication, tenants, QoS, and gateway settings.

## Choose a path

=== "Application developer"

    Start with the [npm SDK](sdk-js.md), [Rust SDK](sdk-rust.md), or
    [Java SDK](sdk-java.md). The [streaming guide](streaming.md) explains
    CDC, reactive queries, presence, broadcast, and durable topics.

=== "Database operator"

    Read [production operations](production.md), then [cluster operations](cluster-operations.md)
    and [backup and restore](backup-restore.md).

=== "PostgreSQL or Supabase user"

    Review [migrations](migration.md), [isolation and tenancy](isolation.md),
    and the [REST API reference](api.md). PostgreSQL drivers can connect
    through the wire gateway.

## Product surfaces

| Surface | Use it for |
| --- | --- |
| PostgreSQL wire | Existing drivers, ORMs, prepared statements, SQL migrations |
| REST and GraphQL | Browser/server CRUD and generated API workflows |
| WebSocket realtime | CDC, reactive queries, presence, broadcast, and durable topics |
| RESP | Redis/Valkey-compatible cache and stream workloads |
| Native and gRPC | High-throughput internal services and typed clients |
| Branches and PITR | Preview environments, safe migrations, recovery, and testing |

The repository's [README](https://github.com/Ryme-Labs/rymeDB) contains the
project overview, while this site is the maintained operator and developer
reference.
