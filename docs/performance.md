# Performance and benchmark evidence

These are measured local results, not promises. Each number depends on the
machine, payload, durability, protocol, concurrency, QoS tier, and data shape.
Raw JSON artifacts live under `bench/results/`.

## Reproduce

Build a release server and benchmark binary, then use a dedicated QoS tier:

```bash
cargo build --locked --release -p ryme-server -p ryme-bench
rymedb-cli qos tier dedicated_cluster dedicated_cluster
ryme-bench resp --addr 127.0.0.1:6380 --workload mixed --ops 40000 --concurrency 8 --pipeline 1 --json
```

For PostgreSQL-wire workloads:

```bash
ryme-bench sql --addr 127.0.0.1:5433 --user postgres --database postgres --workload mixed --ops 10000 --concurrency 8 --json
```

A valid run has zero errors and `successful_ops == requested_ops`. Record CPU,
RAM, storage, server commit, durability, payload, keyspace, concurrency, and
whether the endpoint is loopback or remote.

## Same-machine results

The October 2, 2026 loopback run used 12 CPUs, 15 GiB RAM, 128-byte values,
8 workers, and a warm keyspace:

| Workload | rymeDB durable | p50 | p99 | Same-run Valkey |
| --- | ---: | ---: | ---: | ---: |
| GET | 102,451 qps | 66 µs | 231 µs | Comparable read path; see raw matrix |
| SET | 67,244 qps | 109 µs | 320 µs | Comparable write path; see raw matrix |
| Mixed GET/SET | 87,477 qps | 78 µs | 270 µs | 127,475 qps, 59/124 µs in the captured mixed run |
| Transaction block | 35,390 blocks/s | 195 µs | 761 µs | 51,995 blocks/s, 143/399 µs |

The compound transaction is intentionally stronger work: serializable OCC and
durable WAL versus a non-persistent Valkey run. It was about 1.5x behind at
p50 and 1.9x at p99 in this capture. The simple read path was close, not
universally faster.

## Replication and realtime

The three-node loopback Raft test measured roughly 2.3 ms median leader
quorum acknowledgement and 3.5 ms median follower visibility, with a maximum
around 4.3 ms over 20 writes.

The 100-connection, 10,000-publish realtime stress run delivered 1,000,000 of
1,000,000 expected messages with zero receive errors. It measured approximately
266k fanout deliveries/s and 1.29k publish requests/s, but publish-to-client
latency was about 5.3 s p50 and 8.6 s p99 under that burst. Reliability was
fixed; the low-latency/10k-publish target is not yet met for that workload.

The current release build also includes a cached broadcast envelope so one
JSON payload is encoded once and reused across subscribers. A smaller
20-connection loopback run on the same 12-vCPU machine delivered all 20,000
messages with zero publish or receive errors at 13,089 publish requests/s and
2.81 million fanout deliveries/s. Publish-to-client latency was 38.9 ms p50,
69.6 ms p95, and 77.6 ms p99. This is a separate workload and should not be
treated as a direct replacement for the 100-connection burst result.

Run it yourself:

```bash
ryme-bench realtime --addr 127.0.0.1:8080 --api-key local-dev-key \
  --connections 100 --connection-concurrency 256 --messages 10000 \
  --payload-bytes 128 --json
```

The cached-envelope run used 20 connections, 64 connection concurrency, 1,000
messages, and 32 concurrent publishers:

```bash
ryme-bench realtime --addr 127.0.0.1:8080 --api-key local-dev-key \
  --connections 20 --connection-concurrency 64 --messages 1000 \
  --payload-bytes 128 --publish-concurrency 32 --json
```

## How to read the numbers

- Compare the same protocol and acknowledgement semantics.
- Separate server execution, network, fsync, quorum, and client scheduling.
- Report percentile histograms, not only average or peak throughput.
- Never compare an in-process probe with a networked database claim.
- Re-run on clean, identical hardware before publishing a headline.

There is no defensible blanket “10000x faster than PostgreSQL” or “100x faster
than Valkey” claim. The goal is workload-specific, reproducible improvement
while retaining transactions, durability, realtime, and compatibility.
