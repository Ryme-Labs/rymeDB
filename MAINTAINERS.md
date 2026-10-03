# Maintainers

rymeDB is maintained by Rymelabs with community module owners.

| Area | Owner |
| --- | --- |
| Storage engine, transactions, WAL (`ryme-storage`, `ryme-txn`, `ryme-wal`) | @rymelabs |
| Replication, sharding, routing (`ryme-raft`, `ryme-shard`, `ryme-router`) | @rymelabs |
| Gateways (`ryme-wire-pg`, `ryme-wire-resp`, `ryme-wire-native`, `ryme-server`) | @rymelabs |
| SQL (`ryme-sql`) | @rymelabs |
| Auth, realtime, control plane (`ryme-auth`, `ryme-realtime`, `ryme-control`) | @rymelabs |
| Index, migrate, metering, QoS, observe, backup, archive, branch, crypto (`ryme-index`, `ryme-migrate`, `ryme-metering`, `ryme-qos`, `ryme-observe`, `ryme-backup`, `ryme-archive`, `ryme-branch`, `ryme-crypto`) | @rymelabs |
| SDKs (`sdks/*`) | @rymelabs |
| Dashboard and CLI (`apps/*`) | @rymelabs |
| Benchmarks (`bench/*`, `crates/ryme-bench`) | @rymelabs |
| Deploy (`deploy/*`) and CI (`.github/workflows/*`) | @rymelabs |

Module ownership is granted to sustained contributors per area. Security
reports go through `SECURITY.md`, never through module owners directly.
