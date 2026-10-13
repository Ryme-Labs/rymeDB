---
title: How rymeDB differs
sidebar_label: How it differs
---

# How rymeDB differs

rymeDB combines relational SQL, key/value access, and realtime delivery behind
one transactional storage layer. Whether that combination is useful depends
on your workload; it is not a drop-in replacement for every database.

| If you need… | rymeDB provides | Keep in mind |
| --- | --- | --- |
| Relational queries and transactions | SQL through PostgreSQL wire compatibility, with MVCC and WAL | Check the [PostgreSQL compatibility guide](postgres.md) for unsupported features and differences. |
| Redis-style access | RESP commands and key/value operations | RESP compatibility does not mean every Redis module or command is implemented. |
| Realtime updates | Change streams, WebSockets, presence, and broadcast alongside database operations | Realtime fanout has workload-dependent latency; measure with your channel count and subscriber distribution. |
| A single operational surface | SQL, RESP, REST, and WebSocket gateways share database state | Gateways do not make the system equivalent to a specialized cache, warehouse, or message broker. |
| Replicated operation | Raft-based membership and range placement | Replication adds quorum latency and requires careful backup and failure procedures. |

Choose PostgreSQL when its ecosystem, extensions, and mature relational feature
set are the priority. Choose Redis/Valkey for cache-specific capabilities or
when its established deployment model fits better. Choose a dedicated event
platform for large-scale independent event retention and consumption. Evaluate
rymeDB against equivalent durability, consistency, and acknowledgement
requirements; see [measured benchmarks](performance.md) and
[migration guidance](migration.md).
