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

To isolate the in-memory transaction core from networking and WAL durability:

```bash
ryme-bench local --ops 200000 --concurrency 8 --value-bytes 128
```

This reports parallel commit throughput, failed commits, and a separate
parallel point-read probe. By default it writes distinct keys. Set `--keyspace`
below `--ops` to measure steady-state overwrites, for example `--keyspace 1000`;
the benchmark prepopulates that keyspace before timing. Failed commits are
included in the output because concurrent writes to a hot keyspace can conflict
under optimistic concurrency control. Use a single worker when measuring
overwrite cost without concurrent conflicts. This is a core microbenchmark, not
an end-to-end database comparison.

To measure the sharded Hot-mode WAL write path independently of network protocols:

```bash
ryme-bench sharded-local --ops 20000 --concurrency 4 --shards 4 --value-bytes 128
```

This distributes writes across tables placed on each shard, reports throughput
and sampled p50/p95/p99 operation latency, and removes its temporary data
directory after the run. Each commit writes WAL and a delta segment. Delta
segments no longer rewrite the `latest` pointer; snapshot writes update that
pointer, and segment discovery still finds the newest delta. The default
benchmark does not fsync each write; add `--sync-each-write` to include WAL,
segment, and directory sync costs. Compare shard counts and worker counts on
the same machine, and retain JSON output with hardware and durability details.
This benchmark is in-process and must not be compared directly with networked
database results.

Pass `--standard-mode` to benchmark the segment-backed Standard path with the
same workload and acknowledgement policy. Output identifies the selected
storage mode. Compare Hot and Standard runs sequentially on the same idle
machine; they have different memory-residency semantics, so the result is a
mode comparison rather than a like-for-like database claim. By default, each
transaction writes one key; set `--batch-size 64` to test larger commits. In
that case `--ops` counts transactions, while `qps` counts individual writes
and `transaction_qps` reports committed transactions per second.

A local three-pair batch comparison on the AMD Ryzen 5 4500 used one shard,
one worker, 500 transactions × 64 distinct 128-byte writes, and no per-write
fsync. All runs completed with zero failures. After removing the redundant
segment-engine clone, median throughput was 129.8k writes/s in Hot mode and
151.0k writes/s in Standard mode; sampled transaction p50 was 482 µs and
435 µs respectively. This is evidence for this specific mode/workload
comparison, not an end-to-end database claim. The benchmark samples only eight
transactions per run, so its tail percentiles are too sparse to interpret.

To isolate the delta-segment builder's owned-entry path from the borrowed API:

```bash
cargo run --locked --release -p ryme-bench -- segment-build \
  --entries 1024 --value-bytes 128 --iterations 30
```

This compares building from a borrowed slice (which clones its entries) with
building from an already-owned vector. Vector creation for the owned case is
outside the timed interval because transaction code already owns that vector.
The benchmark reports p50/p95 build time and a median ratio. It measures only
segment construction—not durable writes, transaction throughput, or end-to-end
latency—and should be repeated on the same otherwise-idle machine before
interpreting small differences.

On the 12-logical-CPU AMD Ryzen 5 4500 listed by the command, three release
runs at 1,024 entries × 128 value bytes × 40 iterations measured borrowed-path
median build times of 0.870–0.872 ms and owned-path medians of 0.762–0.766 ms.
The median of the three per-run ratios was about 1.14×. Compared with the prior
recorded run of this same microbenchmark, before moving the locally built
engine into the segment, borrowed-path median time fell from 1.314 ms to
0.872 ms and owned-path time from 1.215 ms to 0.765 ms. This is narrowly scoped
segment-construction evidence; it does not establish a transaction-throughput
or end-to-end latency gain. Rerun on the target workload.

## Delta-segment pointer experiment

A paired release microbenchmark used the same 12-logical-CPU AMD Ryzen 5 4500,
8 shards, 4 workers, 5,000 distinct 128-byte writes, and per-write sync. Two
runs each measured 7,677 and 7,610 ops/s before, then 8,924 and 8,801 ops/s
after avoiding a delta's redundant `latest` pointer rewrite. All runs had zero
failed writes. The [raw capture](../bench/results/segment-latest-pointer.json)
includes sampled latencies; p99 varied sharply between runs and is not a stable
result.

This small local sample is not an SLO or a comparison with another database.
Repeat on clean, identical machines before making performance claims.

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

By default, all subscribers and publishes use the single `bench` channel (a
hot-topic test). Set `--channel-count 10` to spread both connections and
publishes round-robin over `bench-0` through `bench-9`, approximating a uniform
multi-topic workload. Delivery percentage is calculated from the subscribers
and successful publishes on each channel, so channels with no subscribers do
not inflate the expected-delivery count.

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
