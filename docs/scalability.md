---
title: Scalability and capacity planning
sidebar_label: Scalability
---

# Scalability and capacity planning

Scale rymeDB from measured workload limits. A larger node can improve
single-node capacity; a Raft cluster adds availability and range placement,
but writes that require quorum also pay network and replication costs. It is
not safe to assume that adding nodes linearly increases throughput.

## Measure before scaling

Record operation mix, value size, keyspace, active connections, concurrency,
storage latency, durability mode, and (in a cluster) quorum and follower lag.
Use the same protocol and acknowledgement semantics as the application.
Track p50/p95/p99 latency, successful operations, errors, CPU, memory, disk
growth, and replication lag together. Start with the reproducible commands and
limitations in [performance and benchmark evidence](performance.md).

## Single node

- Keep the data directory on persistent storage with predictable write latency;
  do not share one writable data directory between server processes.
- Size memory for the working set and leave headroom for indexes, query
  materialization, connections, and the operating system.
- Use bounded connection pools and application backpressure. More concurrent
  requests can increase queueing and tail latency rather than throughput.
- Watch disk capacity and backup duration as data grows; verify restores, not
  just backup creation.

## Cluster

Use an odd-sized voting group, commonly three nodes, across independent failure
domains. A majority is required to make progress. Add capacity by observing
range loads and moving/splitting ranges with the supported control plane;
never rebalance by copying live database files. Keep Raft traffic on a reliable
low-latency network and configure TLS/mTLS when the network is not trusted.

Follow the [cluster operations](cluster-operations.md) runbook for membership,
range placement, and quorum failures. A cluster does not replace tested backups:
replication can faithfully replicate accidental deletion or corruption.
Range movement copies the current version history to the new owner and keeps
the previous shard's copy so a later move cannot apply a stale migration
tombstone over newer data. This currently trades disk space for safe repeated
rebalancing; plan capacity for duplicate shard history. After restart, persisted
range state records the range bounds and owners. On restart, the shard set loads
that last completed topology so it can reconcile control-plane placement and
ignore stale copies when rebuilding ownership.

## Realtime fanout

The cost of a publish depends on payload size, subscribers per channel,
connection count, and whether traffic concentrates on a hot channel. Test both
hot-topic and distributed-topic patterns. Bound client queues, reconnect
bursts, and subscription counts, and monitor delivery latency as well as
throughput. The benchmark page documents current results and known latency
limits; validate those limits against your own topology before production use.
