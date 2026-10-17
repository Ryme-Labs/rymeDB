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

Measure RESP `MGET` end to end with a local or clustered server:

```bash
ryme-bench resp --addr 127.0.0.1:6380 --workload mget --mget-keys 256 \
  --ops 10000 --concurrency 8 --pipeline 1 --json
```

Here `ops` and `pipeline` count commands; each command reads `mget-keys` values,
and reported latency covers writing the request and reading its full reply.
Prepopulate those keys on the target server to measure hits. In cluster mode,
this includes owner routing and local/remote value handling.

A localhost smoke benchmark on an AMD Ryzen 5 4500 used Standard storage,
memory durability, 16 seeded 16-byte values, and one client issuing 2,000
16-key MGET commands per capture. Three captures measured p50 at 37–49 µs,
p99 at 62–72 µs, and 20.3k–25.9k commands/s, with zero protocol errors. The
segment cache warmed during the run. This includes client encoding and reply
parsing, but is not a cold-disk or production deployment result; see the
[raw captures](../bench/results/resp-mget-standard-local.json).

An eight-client Standard-mode capture on the same host used a dedicated-cluster
QoS tier, 16 seeded 16-byte values, and 16,000 16-key MGET commands per capture.
Two sequential runs measured 63.6k–64.0k commands/s, p50 at 111–112 µs, and
p99 at 308–328 µs, with zero errors. This remains a localhost, memory-durability
smoke benchmark, not a production or cross-database comparison; see the
[raw captures](../bench/results/resp-mget-standard-8clients.json).

The Standard-mode batch-read path resolves all segment handles under one cache
lock when the active segments are resident, instead of reacquiring the cache
lock for each segment. With 80 active segments, eight clients, and 16-key MGETs,
two local captures measured 41.8k–45.3k commands/s and p99 at 456–501 µs, with
zero errors. Treat these as a local scaling baseline, not a cross-database
comparison; see the [raw captures](../bench/results/resp-mget-standard-80segments-8clients.json).

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

To benchmark the SQL executor in-process without PostgreSQL-wire or network
overhead:

```bash
cargo run --locked --release -j 1 -p ryme-bench -- sql-local \
  --rows 1000 --ops 1000 --json
```

The command seeds a bounded in-memory table outside the timed interval, warms
the query, then measures a filtered projection with three predicates. It checks
the result row count on every operation and reports p50/p95/p99 latency and
throughput. This isolates executor/storage work for one fixed query shape; it
does not measure SQL parsing, concurrent clients, WAL durability, network
overhead, or establish a cross-database performance claim.

To compare tracked single-key reads with read-only and batched reads in both
storage modes:

```bash
cargo run --locked -j 1 -p ryme-bench -- txn-read-batch \
  --records 10000 --candidates 1024 --iterations 100 --batch-size 256 --json
cargo run --locked -j 1 -p ryme-bench -- txn-read-batch \
  --records 10000 --candidates 1024 --iterations 100 --batch-size 256 \
  --standard-mode --json
```

Standard mode seeds a temporary segment-backed database, warms its cache, and
removes the temporary directory after the run. This measures cached point-read
overhead, not cold disk latency or a full networked deployment. Compare modes
on the same idle machine; the output includes the storage mode and read
percentiles. `read_only_batched_read` models the transaction-core access path
used by RESP `MGET`: one read-only transaction with repeated `get_many` calls.
Its speedup is reported against `read_only_single_read`, so neither side pays
conflict-observation tracking.

`--candidates` may exceed `--records`; the benchmark then cycles through the
seeded keys, measuring repeated-key batches. JSON reports the number of
distinct keys per iteration. For example, use `--records 64 --candidates 1024`
to measure 16 reads per distinct key while keeping batch size fixed.

On an AMD Ryzen 5 4500, two release captures with 64 records, 1,024 reads per
iteration, and batches of 256 measured read-only batched p50 at 157–161 µs vs
184–185 µs for individual reads in Hot mode (1.15–1.17×). Standard mode
measured 323–326 µs vs 536 µs (1.65–1.66×). This is a warmed, repeated-key
core workload, not end-to-end RESP or cold-disk latency; see the
[raw captures](../bench/results/txn-read-batch-repeated.json).

A segment-batch duplicate-elimination prototype was not retained: its unique-key
read-only p50 regressed from 490 µs to 588 µs with hashing and 529 µs with
sorting. Repeated-key batches benefited, but the general path must not penalize
the unique-key workload without a cheaper exact duplicate check.

Standard segment point reads previously Bloom-checked, binary-searched the
segment key vector, then searched the same key in the segment engine. Removing
the redundant vector search reduced read-only batched p50 from 490 µs to
296–298 µs for 1,024 unique reads, and from 323–326 µs to 221–222 µs for 64
keys repeated 16 times. Single-read p50 also fell from 705 µs to 494–497 µs
for the unique workload. The unique baseline is one capture; after values are
two captures. False-positive Bloom results still go through the authoritative
engine lookup. See [raw before/after captures](../bench/results/segment-bloom-lookup.json).

On an AMD Ryzen 5 4500, two sequential release captures with 10,000 records,
1,024 reads/iteration, 100 iterations, and batches of 256 showed read-only
batch p50 of 588–592 µs versus 812–817 µs for individual read-only gets in
Standard mode (1.38× in both captures). Hot mode measured 275–277 µs versus
291–295 µs (1.05–1.07×). The Hot-mode p99 varied substantially, so treat these
as preliminary in-process results, not end-to-end RESP latency claims. See the
[read-only batch captures](../bench/results/txn-read-batch-readonly.json).

Two sequential release captures on an AMD Ryzen 5 4500 (10,000 records,
1,024 reads/iteration, 100 iterations, 256-key batches) measured Standard-mode
batch p50 at 1.403–1.410 ms versus 1.645–1.646 ms for individual reads (1.17×
in both captures). Hot-mode results varied between a small batch win and a
small loss, so this change does not establish a reliable Hot-mode speedup.
These are single-host microbenchmarks, not cross-database claims; see the
[raw captures](../bench/results/txn-read-batch-standard.json).

To measure the in-memory vector index's build, exact-search, and HNSW-search
paths with the same generated corpus:

```bash
cargo run --locked --release -j 1 -p ryme-bench -- vector-index \
  --vectors 2000 --dimensions 64 --queries 200 --partitions 4 --top-k 10 --ann-ef 64 --json
```

The command reports dataset generation separately from index-build time, then
measures exact and adaptive ANN API search latency/QPS and recall@k. Exact and
ANN call order alternates per query to reduce systematic cache-order bias. The
ANN API uses exact search when a partition's vector count is no larger than
`min(effective_ef × 2 × M, 2048)`; otherwise it searches the HNSW graph. `M` is
16 below 64 dimensions and 32 at 64 dimensions or above. The deterministic
SplitMix-derived corpus avoids the duplicate vectors produced by simple
periodic generators. The vector payload is capped at 16 MiB to avoid
unexpectedly large allocations; index metadata and graph edges require
additional memory. Use identical corpus size, dimensions, partitions, top-k,
`ann-ef`, and build profile when comparing revisions. This isolates the Rust
index implementation; it does not measure HTTP overhead, persistence,
replication, or establish a cross-database performance claim. Query percentiles
are descriptive only with the reported sample count (200 is too small for
stable p99 conclusions).

On an AMD Ryzen 5 4500, release build, 64 dimensions, four partitions, top-k 10,
`ann-ef=64`, and 100 deterministic queries, the adaptive ANN API measured p50
at 300.5 µs with exact recall at 4,096 vectors; partitions below the
`effective_ef × HNSW_M` work threshold use exact search. At 8,192 vectors it
used HNSW and measured 632.6 µs p50, 0.978 mean recall@10; exact search measured
907.1 µs p50. The [raw captures](../bench/results/vector-index-crossover.json)
record full results. These are local in-memory crossover observations, not
stable tail-latency or end-to-end database claims. Repeat several times on the
target machine before using them for capacity planning.

Exact vector search now borrows each indexed ID during scoring and copies only
the selected top-k IDs. In a same-machine single-run comparison against
`910d98d`, 4,096-vector exact-search p50 moved from 302.2 µs to 217.0–217.1 µs
across two post-change runs; 8,192-vector exact-search p50 moved from 907.1 µs
to 547.8 µs. The adaptive 8,192-vector HNSW p50 measured 625.8 µs after the
change versus 632.6 µs before it, with recall@10 unchanged at 0.978. These
captures are not a statistically controlled benchmark; see the
[borrowed-ID comparison](../bench/results/vector-topk-borrowed-ids.json) and
repeat paired runs on the target workload.

Search width is an explicit latency/quality control. On the same machine, with
20,480 vectors (64 dimensions, four partitions), top-k 10, and 300 deterministic
queries per run, three release runs at `ef=64` measured median ANN p50 878 µs,
1,135 QPS, and 0.910 recall@10. At `ef=128`, median p50 was 1.527 ms,
648 QPS, and recall@10 was 0.981. Both settings built the index in about
2.7 seconds; exact-search p50 was about 3.38 ms. The API, CLI, and JS SDK keep
`ef=64` as their default to favor speed; set `ef=128` when this measured
recall/latency tradeoff better fits the workload. These are local index-only
measurements; see the [raw profile](../bench/results/vector-hnsw-ef-profile.json)
and benchmark your own corpus before choosing a production value.

The graph degree is also dimension-aware. A follow-up release profile on the
same Ryzen 5 4500 used 3,000 vectors, top-k 10, one partition, and 100 queries.
At 32 dimensions, `M=16` measured 57.3 µs ANN p50, 99.2 µs exact p50, 0.983
recall@10, and 140 ms build time. At 384 dimensions, the original `M=16`
measured 445 µs ANN p50 and 0.645 recall@10; `M=32` measured 709 µs, 0.891
recall@10, and 1.30 s build time, while exact p50 was 1.07 ms. For a larger
64-dimensional corpus (20,480 vectors, four partitions), `M=32` measured
1.46 ms ANN p50, 0.988 recall@10, and 4.21 s build time. These one-run
measurements show the tradeoff that motivated using a wider graph only for
embedding dimensions: better recall while ANN remained faster than exact in
these samples, at the cost of index-build time and memory. They are not stable
tail-latency or cross-database claims; see the [dimension profile](../bench/results/vector-hnsw-dimensions.json)
and repeat on your own corpus.

To measure full-text search when a common term matches every document:

```bash
cargo run --locked --release -j 1 -p ryme-bench -- text-index \
  --documents 10000 --queries 500 --partitions 4 --top-k 10 --json
```

The benchmark returns p50/p95/p99 and QPS for a warmed, in-process index. On
the same host and corpus, bounded top-k selection followed by direct single-term
posting traversal reduced median query p50 from 1.716 ms to 98.8 µs and raised
median QPS from 566 to 9,944 across three runs. The first step avoids sorting
and copying every match; the single-term path also avoids building a score map.
Multi-term queries retain the accumulator needed to combine term scores. See
the [bounded top-k runs](../bench/results/text-search-bounded-topk.json) and
[single-term fast-path runs](../bench/results/text-search-single-term-fast-path.json).
These measurements exclude network, persistence, and replication.

To profile multi-term accumulation, rerun with `--query-terms 2`; both common
terms then match every document. Reserving the score map from the capped sum of
posting lengths reduced median p50 from 1.279 ms to 696 µs and raised QPS from
745 to 1,270 in three runs. The estimate is capped at the table's document
count to bound memory. See the [raw profile](../bench/results/text-search-multiterm-capacity.json).

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
With multiple shards, rows within a batch are assigned round-robin across
shard-placed tables, so larger batches exercise cross-shard coordination. Use
`--shards 1` to isolate the single-shard batch path; benchmark table-local
batches separately when that matches the application workload.

Standard-mode scans merge the sorted key ranges of their delta segments and
resolve each key from newest to oldest at the requested snapshot. They stop
after collecting the requested number of visible rows instead of materializing
the whole table first; scan work still depends on how many segment files must
be consulted and how many tombstones or expired rows are encountered.

To measure cached Standard-mode scan pages over a snapshot plus one-row delta
segments:

```bash
cargo run --locked --release -p ryme-bench -- segment-scan \
  --rows 10000 --deltas 100 --limit 100 --iterations 30
```

Add `--compact` to compact the generated segment chain before warming the cache
and timing scans. Compare paired runs with and without this flag to isolate the
effect of compaction; compaction setup is outside the measured interval.

Setup and one cache-warming scan are outside the timed samples. The output
includes p50/p95 scan latency and throughput; vary one parameter at a time and
compare sequential runs on the same otherwise-idle machine.

To measure Standard-mode startup's maximum-commit discovery against the former
full-segment decode path:

```bash
cargo run --locked --release -j 1 -p ryme-bench -- segment-startup \
  --rows 10000 --superseded-deltas 500 --deltas 500 --iterations 10
```

To compare retention's former full-row decode scan with checksum-verified
metadata pruning:

```bash
cargo run --locked --release -j 1 -p ryme-bench -- segment-prune \
  --rows 10000 --deltas 100 --iterations 10
```

This creates one snapshot and a live delta tail, then alternates the previous
decode-and-classify scan with `SegmentStore::prune`. No files are eligible for
deletion in this setup, so the measurement isolates retention's segment scan
and validation cost rather than filesystem cleanup. Both paths validate the
segment checksum; the baseline also parses and rebuilds row indexes. The
optimized path uses fixed-buffer checksum verification without rebuilding row
indexes. Results use a warm filesystem cache and should be repeated on the
target storage and workload.

The command creates an initial snapshot, a superseded delta run, a newer
snapshot, and a live delta tail. It alternates the two measurements to reduce
filesystem-cache ordering bias, and reports p50/p95 plus their p50 ratio. The
full-decode path rebuilds every segment's engine, bloom filter, and sparse
index; the optimized path skips files superseded by the latest snapshot and
reads metadata while checksum-verifying the remaining segments with a
fixed-size buffer. This isolates commit-timestamp discovery, not total database
startup or cold-storage latency.

On an AMD Ryzen 5 4500 (12 logical CPUs), a release run with 10,000 snapshot
rows, 500 superseded deltas, 500 live deltas, and 10 iterations measured p50
at 35.1 ms for full decode versus 4.45 ms for the optimized path (7.88×).
Files were locally cached; this is a startup-work microbenchmark, not a full
server restart measurement.

On the same AMD Ryzen 5 4500, the segment-prune scan with a 10,000-row
snapshot, 100 delta segments, and 10 iterations measured 14.87 ms p50 for the
full-decode scan versus 1.84 ms for metadata pruning (8.09×). This measures
only the warm-cache retention scan with no segment deletion; it is not an
end-to-end database latency result.

Two sequential A/B pairs on an AMD Ryzen 5 4500 (12 logical CPUs), comparing
the pre-merge implementation at `0202d42` with the k-way merge, measured p50
latency of 7.49–7.57 ms versus 51.6–51.9 µs. The median p50 was about 145×
lower; p95 was 7.59–7.81 ms versus 61.7–63.6 µs. This is a warm-cache,
synthetic page workload (10,000 snapshot rows, 100 one-row deltas, limit 100),
not a general database or end-to-end performance claim.

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
It also reports p50/p95 decode time for a pre-encoded segment; encoding is not
included in the decode timing. The benchmark measures construction and decode
in isolation—not durable writes, transaction throughput, or end-to-end
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

The same `segment-build` command also reports `lookup_mean_ns` for
deterministic pseudo-random point hits against one already-built segment. This
includes the segment Bloom/key checks, engine lookup, and returned-value clone;
compare only identical entry counts, payloads, build profiles, and hardware.

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

An earlier 100-connection, 10,000-publish realtime stress capture delivered
1,000,000 of 1,000,000 expected messages with zero receive errors. It measured
approximately 266k fanout deliveries/s and 1.29k publish requests/s, but
publish-to-client latency was about 5.3 s p50 and 8.6 s p99 under that burst.
That historical result predates the current cached-envelope and batched-writer
path; it is not directly comparable to the newer captures below.

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

After batching WebSocket writes (commit `efb5101`), two sequential runs with
that same workload on the AMD Ryzen 5 4500 loopback setup delivered all 20,000
expected messages. Publish-to-client p50 was 0.572–0.669 ms and p95 was
1.408–1.792 ms, while p99 remained 109.7–111.5 ms (maximum 137–139 ms).
Completed fanout throughput was 136.7k–140.1k deliveries/s. The p99 tail is
still poor. These runs are a validation capture, not a controlled before/after
comparison; the 100-connection/10,000-publish workload above remains the target
that needs a repeat run. Raw outputs and exact invocation are in
[`bench/results/realtime-writer-batch-20x1000.json`](../bench/results/realtime-writer-batch-20x1000.json).

The exact 100-connection/10,000-publish workload above was then run twice on
the same machine against an isolated release server. Both runs delivered all
1,000,000 expected messages with no publish or receive errors. Publish-to-client
p50 was 2.310–2.365 ms, p95 was 4.207–4.285 ms, and p99 was 5.418–5.572 ms;
completed fanout throughput was 1.098–1.112 million deliveries/s. However,
about 0.14% of samples exceeded 100 ms (maximum 157–162 ms), so the extreme tail
still needs work. This is a repeated current-build validation, not a controlled
before/after experiment. Raw captures are in
[`bench/results/realtime-writer-batch-100x10000.json`](../bench/results/realtime-writer-batch-100x10000.json).

## How to read the numbers

- Compare the same protocol and acknowledgement semantics.
- Separate server execution, network, fsync, quorum, and client scheduling.
- Report percentile histograms, not only average or peak throughput.
- Never compare an in-process probe with a networked database claim.
- Re-run on clean, identical hardware before publishing a headline.

There is no defensible blanket “10000x faster than PostgreSQL” or “100x faster
than Valkey” claim. The goal is workload-specific, reproducible improvement
while retaining transactions, durability, realtime, and compatibility.
