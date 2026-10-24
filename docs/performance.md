# Performance and benchmark evidence

These are measured local results, not promises. Each number depends on the
machine, payload, durability, protocol, concurrency, QoS tier, and data shape.
Raw JSON artifacts live under `bench/results/`.

Benchmark JSON includes OS, architecture, and logical CPU count. It reads the
CPU model and host memory from procfs on Linux and the processor identifier on
Windows. Set `RYME_BENCH_CPU_MODEL` and `RYME_BENCH_HOST_MEMORY_BYTES` to provide
more precise host metadata or fill fields the current platform does not expose.

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

With `--pipeline 1`, latency percentiles sample individual command round trips.
With a larger pipeline, each latency sample is the time to write and receive a
whole pipeline batch; it is labeled `pipeline_batch` and is not divided into
synthetic per-command latency. `qps` and `successful_ops` still count successful
commands, while `latency_samples` counts the samples used for the percentiles.
Transaction workload percentiles are labeled `transaction`.

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

An exploratory cache-window experiment compared two baseline and three candidate release captures of an eight-client localhost workload with 80 SET commits over 16 keys, then 16-key MGET commands. It appeared to improve throughput, but it did not exercise the cache-window implementation: RESP MGET dispatch sends storage reads in batches of four, while that implementation requires at least 16 keys in one storage call. The result is not evidence for the cache-window change and that change has been removed. The in-process 80-segment benchmark also showed a serial deep-chain regression at both tested window sizes. See the [raw exploratory captures](../bench/results/resp-mget-standard-segment-cache-prefetch-windows-20261024.json).
The earlier eager-cache measurements in
[this capture](../bench/results/resp-mget-standard-80segments-8clients.json)
predate lazy segment loading and are historical.

RESP array replies now calculate the exact output size once and append bulk
lengths directly, avoiding a temporary formatted string for each item. Three
16-key MGET captures on the same Windows Ryzen 5 4500 setup measured median
throughput of 41.8k commands/s before and 42.2k after, with p50 at 175/173 microseconds
and p99 at 403/397 microseconds. These differences are within run-to-run variation and
do not establish a speedup. The change also makes MGET's 16 MiB limit include
RESP framing. See the [raw captures](../bench/results/resp-mget-array-encode-20261024.json).

I also tested sending each 16-key local MGET as one storage batch, which would
activate the segment layer's large-batch Bloom-hash reuse. Against the existing
four-key MGET batches, six candidate captures were highly variable and had
median p50/p99 of 178.5/438 microseconds versus 173/397 microseconds. This
localhost workload did not show a reliable benefit, so the four-key batch size
remains. See the [raw batch-size captures](../bench/results/resp-mget-batch-size-20261024.json).

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

Use `--batch-size 64` to stage 64 writes per transaction. In batch mode,
`--ops` counts transactions, `qps` counts individual writes per second, and
`transaction_qps` reports transactions per second. Batches are bounded by the
benchmark to 20,000 writes and 8 MiB of values per transaction. Five alternating
release-run pairs on a Windows Ryzen 5 4500 with one worker and 64 writes per
transaction measured median throughput at 329.2k writes/s before and 327.9k
with a candidate that skipped the redundant whole-write-set validation pass.
The difference is below run-to-run variation, so the transaction change was
discarded. See the [raw captures](../bench/results/txn-local-write-validation-batch-20261024.json).

To benchmark the SQL executor in-process without PostgreSQL-wire or network
overhead:

```bash
cargo run --locked --release -j 1 -p ryme-bench -- sql-local \
  --rows 1000 --ops 1000 --limit 4000 --json
```

The command seeds a bounded in-memory table outside the timed interval, warms
the query, then measures a filtered projection with three predicates. It checks
the result row count on every operation and reports p50/p95/p99 latency and
throughput. This isolates executor/storage work for one fixed query shape; it
does not measure SQL parsing, concurrent clients, WAL durability, network
overhead, or establish a cross-database performance claim.

To reproduce the indexed small-limit case and its large-result control:

```bash
cargo run --locked --release -j 1 -p ryme-bench -- sql-local \
  --rows 20000 --ops 300 --limit 10 --concurrency 1 --json
cargo run --locked --release -j 1 -p ryme-bench -- sql-local \
  --rows 20000 --ops 300 --limit 4000 --concurrency 1 --json
cargo run --locked --release -j 1 -p ryme-bench -- sql-local \
  --rows 20000 --ops 300 --limit 1 --seq-min 19990 --concurrency 1 --json
```

Indexed SQL scans now start with `min(limit, 256)` candidate reads and grow the
batch up to 256 when residual predicates or RLS reject rows. On the same host,
five alternating release pairs over 20,000 seeded rows and 834 indexed
candidates measured median p50 549 Âµs before and 131 Âµs after for `LIMIT 10`
(76.2% lower). The 834-row control measured 2.81 ms for both versions. A SQL
test covers refill through early residual-predicate and RLS rejects. These are
in-process results, not PostgreSQL-wire or network
latency; see the [raw captures](../bench/results/sql-indexed-limit-adaptive-batches-windows-20261024.json).

The `--seq-min` option sets a residual predicate threshold to reproduce
selective-match workloads. An estimated-selectivity batch-growth rule was
compared with bounded doubling on the same host for five alternating release
pairs, using 20,000 rows, 834 indexed candidates, and one result row. The
estimated rule had median p50 3.1% higher, p95 5.0% higher, p99 9.0% higher,
and throughput 2.2% lower. An all-match `LIMIT 10` control was effectively
flat. The estimated rule was discarded; bounded doubling remains in use. See
the [raw captures](../bench/results/sql-indexed-limit-growth-selectivity-windows-20261024.json).

The SQL indexed-row fetch chunk remains 256. A 512-row candidate halved fetch
calls for the measured indexed query, but alternating release runs with 20,000
rows/834 results showed median p50 only 0.7% lower and median throughput 0.3%
higher. With 50,000 rows/2,084 results, median p50 was 0.2% lower and
throughput 0.8% higher. These differences do not establish a reliable gain, so
the 256-row chunk remains. See the [raw captures](../bench/results/sql-indexed-read-batch-size-20261024.json).

Use `--composite-key-projection` to benchmark a composite-primary-key table
while projecting only a non-key column. SQL projection decodes the composite
key only when a selected column needs one of its components. Five alternating
release-run pairs on a Windows Ryzen 5 4500, each returning 2,084 rows, measured
median p50 at 8.18 ms before and 8.00 ms after (2.2% lower); candidate p50 was
lower in each pair. Median throughput was 116 versus 120 queries/s. Tail
percentiles varied across pairs, so no p95/p99 improvement is claimed. This is
one in-process workload and does not establish end-to-end latency gains. See
the [paired captures](../bench/results/sql-composite-key-projection-20261024.json).

The `--limit` option accepts values from 0 through 4,000. A sequential
before/after capture for the same 1,000-row indexed query with `LIMIT 0` on an
AMD Ryzen 5 4500 (12 logical CPUs, Windows 11) measured median p50 falling from
73.6 µs to 1.7 µs and median throughput rising from 13.3k to 542.8k queries/s.
The executor now returns before index lookup or row reads when the limit is
zero. A `LIMIT 4000` control query returned 42 rows and had median p50 of
106.4 µs before and 108.3 µs after; this difference is within run-to-run noise
and is not a measured regression or gain. The zero-limit result is an
in-process microbenchmark, not an end-to-end result; see the [raw captures](../bench/results/sql-local-limit-zero-20261024.json).

Before selective JSON projection, three sequential release captures on an AMD
Ryzen 5 4500 with 1,000 rows,
10,000 operations, one worker, and 42 result rows measured p50 at 84.9–86.7 µs
and throughput at 11.3k–11.6k queries/s. p99 ranged from 103.6–136.6 µs, so
tail latency was less stable than the median. These are local in-process
baselines, not end-to-end database results; see the
[raw captures](../bench/results/sql-local-indexed-filtered-20261017.json).

SQL projection now deserializes only selected JSON roots instead of building a
complete `serde_json::Value` for each result row. Sequential before/after
release captures on the same Ryzen 5 4500 measured median-of-capture p50 falling
from 86.6 to 82.8 µs for 42 results (4.4%), and from 1.012 to 0.982 ms for 417
results (3.0%). Tail percentiles varied, and this is a single-host in-process
result, not a general database speed claim; see the
[before/after captures](../bench/results/sql-json-projection-20261017.json).

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
percentiles, and labels Hot as in-memory-only and Standard as running with WAL
and segment sync disabled. `read_only_batched_read` models the transaction-core
access path used by RESP `MGET`: one read-only transaction with repeated
`get_many` calls.
Its speedup is reported against `read_only_single_read`, so neither side pays
conflict-observation tracking.

`--candidates` may exceed `--records`; the benchmark then cycles through the
seeded keys, measuring repeated-key batches. JSON reports the number of
distinct keys per iteration. For example, use `--records 64 --candidates 1024`
to measure 16 reads per distinct key while keeping batch size fixed.

The same benchmark also reports a shuffled unique-key control. On the Windows
Ryzen 5 4500 host above, two runs with 1,024 distinct keys in 256-key batches
measured shuffled read-only p50 at 415–418 µs in Hot mode and about 565 µs in
Standard mode. Ordered batches measured 117–119 µs and 373–389 µs respectively.
This identifies an optimization target, not a before/after claim; see the
[raw captures](../bench/results/txn-read-batch-unique-windows-20261024.json).

For same-table shuffled batches, duplicate detection now hashes only primary-key
bytes after the batch scope check. On the same Windows Ryzen 5 4500 host, three
Hot-mode captures of 1,024 unique keys measured median p50 at 390.7 µs, down
from 415.4 µs across three baseline captures. For 64 records repeated across
1,024 reads, three post-change captures measured median p50 at 168.6 µs, down
from 212.2 µs in the single baseline capture. These are in-process results;
the repeated-key comparison has one baseline capture, and per-capture p99 is
noisy. See the [raw before/after captures](../bench/results/txn-read-batch-primary-key-hash-windows-20261024.json).

Two follow-up approaches to shuffled unique-key batches were measured and
discarded. Sorting keys before the ordered map walk raised Hot-mode p50 from a
409.7 µs baseline median to 612.7 µs across two captures. Direct MVCC-version
lookup in the individual-read fallback left Hot-mode p50 effectively flat and
raised Standard-mode median p50 by 3.4% across three alternating pairs. The
existing lookup path remains in place. See the [raw captures](../bench/results/txn-read-shuffled-lookup-experiments-windows-20261024.json).

A fast path for a key with exactly one MVCC version was also measured on 10,000
records and 1,024 read keys. Across three alternating pairs, Hot-mode median
p50 changed by less than 1%, while Standard-mode median p50 regressed 5.6%.
The branch was discarded; the general snapshot-aware version lookup remains.
See the [raw captures](../bench/results/txn-read-single-version-fast-path-windows-20261024.json).

On an AMD Ryzen 5 4500, two release captures with 64 records, 1,024 reads per
iteration, and batches of 256 measured read-only batched p50 at 157–161 µs vs
184–185 µs for individual reads in Hot mode (1.15–1.17×). Standard mode
measured 323–326 µs vs 536 µs (1.65–1.66×). This is a warmed, repeated-key
core workload, not end-to-end RESP or cold-disk latency; see the
[raw captures](../bench/results/txn-read-batch-repeated.json).

A Windows capture on an AMD Ryzen 5 4500 with 12 logical CPUs and 16 GiB RAM,
using the same 64-record repeated-key workload and 100 iterations, measured
read-only batch p50/p99 at 190/267 µs in Hot mode and 205/289 µs in Standard
mode. These are one capture per mode with sync disabled, so compare them only
with equivalent local runs; see the [raw capture](../bench/results/txn-read-batch-windows-20261024.json).

Hot-mode unsorted batches with duplicate keys now sort once, read each distinct
key through the ordered MVCC range path, and fan values back to the original
positions. In a fresh run with 64 records, 1,024 reads, 100 iterations, and
256-key batches, read-only batched p50 moved from 300 µs in one pre-change
capture to 210 µs in both post-change captures (~30% lower); p95 moved from
307 µs to 220–255 µs. This is a local in-process comparison with only one
baseline capture, so treat it as directional, not a general speed claim. The
optimization is for Hot-mode `Engine::read_many`; Standard segment reads follow
a separate code path. See the [before/after captures](../bench/results/txn-read-batch-hot-duplicates-20261017.json).

I tested replacing Hot-mode's duplicate-free `HashSet` check and subsequent
dedup `HashMap` pass with one map pass. Three alternating release-build pairs
on this host measured median shuffled read-only p50 1.9% higher for repeated
keys and 4.3% higher for unique keys, so the candidate was discarded. The
existing set check remains useful for batches without duplicates; see the
[raw experiment](../bench/results/hot-readmany-single-pass-dedup-windows-20261024.json).

I also tested capping the Hot-mode duplicate map's initial reservation at 256
entries. With a single 1,024-key batch, five alternating pairs measured
repeated-key median p50 4.6% lower, but three near-unique pairs (1,023 distinct
keys) regressed median p50 by 6.5% as the map grew. Since the workload shape is
unknown when reserving, this candidate was discarded; the Hot path retains its
full-batch reservation. See the [raw captures](../bench/results/hot-readmany-dedup-reserve-cap-windows-20261024.json).

Standard segment batch reads now bypass duplicate detection for strictly
ascending and descending batches. For other batches of at least 16 keys, they
resolve each distinct key once and fan its value back to duplicate positions.
On the Ryzen 5 4500, two release captures with 64 records, 1,024 reads per
iteration, 100 iterations, and batches of 256 measured read-only p50 at
177.1–177.3 µs, versus 223.2–223.6 µs before this change (about 21% lower).
Descending repeated-key batches measured 172.2–172.9 µs. This is a local,
in-process result, not end-to-end RESP latency. The batch benchmark also reports
a deterministic shuffled-key control: the current 1,024-distinct-key p50 was
about 518 µs, but there is no matching pre-change shuffled capture, so that
number does not establish a before/after effect. Use the reproduction command
above with `--records 64` for repeated keys and `--records 1024` for unique keys.

For unsorted Standard segment batches, duplicate detection now hashes only the
primary-key bytes when all keys share a tenant/database/table scope; mixed-scope
batches continue to compare full record keys. Three alternating release-build
pairs on the same Windows Ryzen 5 4500 host measured median read-only p50 18.6%
lower for 64 keys repeated across 1,024 reads, and 10.3% lower for 1,024 shuffled
unique keys. Unique-key p99 was noisy and 4.4% higher by median, so this result
does not establish a tail-latency improvement. This is a warmed in-process
Standard-mode benchmark with WAL and segment sync disabled, not networked or
cold-disk latency; see the [raw captures](../bench/results/segment-readmany-primary-key-hash-windows-20261024.json).

The same deduplication map now reserves for the incoming batch size to avoid
rehashing while registering unique keys. Three alternating release-build pairs
on this host showed median shuffled read-only p50 8.8% lower for 64 keys
repeated across 1,024 reads and 8.1% lower for 1,024 unique keys, with every
paired p50 favoring the reserved map. One unique-key candidate capture had a
large p95/p99 outlier, so no tail improvement is claimed. This is a warmed
in-process result for batches of 256; larger batches and concurrent workloads
need separate measurements. See the [raw captures](../bench/results/segment-readmany-map-reserve-windows-20261024.json).

Because segment batches can be larger than SQL's usual 256-key chunks, the
dedup map's initial reservation is capped at 256 entries and grows normally for
larger unique batches. In three alternating 1,024-key Standard-mode pairs, the
cap measured median shuffled read-only p50 5.7% lower for repeated keys and
0.6% higher for unique keys (effectively flat); tail percentiles were noisy.
This bounds speculative initial allocation for duplicate-heavy batches, not
the final map size, and is not a general speedup claim. See the
[raw captures](../bench/results/segment-readmany-map-reserve-cap-windows-20261024.json).

I also tested capping the `unique_indices` vector's initial capacity for
1,024-key batches. The repeated-key p50 was effectively unchanged, while all
three shuffled unique-key captures regressed p50 (median 11.2% higher), so that
change was discarded and the vector keeps full batch capacity. See the
[raw experiment](../bench/results/segment-readmany-unique-indices-cap-windows-20261024.json).

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

Serializable reads record conflict keys in a `BTreeSet`. For sorted, unique
batches whose keys do not overlap the transaction's existing read set, tracking
uses the set insertion result instead of a separate membership lookup. Other
batch shapes and single-key reads keep duplicate-safe membership checks. If a
resource limit rejects a newly inserted key, it is removed so the transaction
remains internally consistent. Two sequential Hot-mode release captures
(10,000 records, 1,024 unique reads, 256-key batches, 128-byte values) measured
tracked batched-read p50 at 538–539 µs, down from 861–863 µs (about 38%), and
staged-write batched-read p50 at 568–569 µs, down from 865–867 µs (about 34%).
Read-only batched p50 stayed around 82 µs. This is an in-process transaction
benchmark, not end-to-end database latency; see the [read-set captures](../bench/results/txn-read-set-insert.json).

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
At 32 dimensions, `M=16` measured 57.5 µs ANN p50, 100.2 µs exact p50, 0.983
recall@10, and 140 ms build time. At 384 dimensions, the original `M=16`
measured 445 µs ANN p50 and 0.645 recall@10; `M=32` measured 709 µs, 0.891
recall@10, and 1.30 s build time, while exact p50 was 1.07 ms. For a larger
64-dimensional corpus (20,480 vectors, four partitions), two runs with `M=32`
measured 1.46 ms ANN p50, 0.988 recall@10, and 4.21–4.34 s build time. At
384 dimensions, `ef=128` measured 983 µs p50 and 0.971 recall@10; `ef=256`
reached exact recall but was slower than exact search. These local
measurements show the tradeoff that motivated using a wider graph only for
embedding dimensions: better recall while ANN remained faster than exact in
these samples, at the cost of index-build time and memory. They are not stable
tail-latency or cross-database claims; see the [dimension profile](../bench/results/vector-hnsw-dimensions.json)
and repeat on your own corpus.

HNSW neighbor pruning now borrows each adjacency list while scoring candidates
instead of cloning it. On Windows with an AMD Ryzen 5 4500, a three-run release
comparison using 5,000 vectors (64 dimensions, one partition, 500 queries,
top-k 10, `ef=64`) reduced median index build time from 851 ms to 689 ms (19%).
Mean recall@10 remained 0.979. The build-time ranges were 843–1,067 ms before
and 687–741 ms after; these index-only runs were not interleaved. See the
[raw captures](../bench/results/vector-hnsw-neighbor-pruning-windows-20261024.json)
and repeat on your own corpus.

HNSW graph construction and traversal now accumulate `f32` products in `f32`;
exact vector search and normalization retain `f64` accumulation. On the same
Windows Ryzen 5 4500, three alternating 5,000-vector, 64-dimensional release
captures reduced median build time from 630 ms to 547 ms (13.1%) and ANN p50
from 166 µs to 142 µs (14.6%), with mean recall@10 unchanged at 0.979. A
3,000-vector, 384-dimensional profile measured 1.318 s to 1.271 s median build
time and 985 µs to 955 µs ANN p50, with mean recall@10 unchanged at 0.971.
The precision test verifies absolute score error below 1e-4 for sampled
normalized vectors at 64, 384, and 4,096 dimensions. These are local in-memory
index measurements; see the [raw captures](../bench/results/vector-hnsw-f32-accumulation-windows-20261024.json).

The HNSW dot product now uses eight independent `f32` accumulators over groups
of eight elements, exposing instruction-level parallelism while retaining a
scalar tail for dimensions not divisible by eight. Across three alternating
release pairs, median index build time fell 36.4% for 5,000 64D vectors and
59.4% for 3,000 384D vectors. ANN p50 fell 32.0% and 50.5%, respectively;
mean recall@10 was unchanged at 0.979 and 0.971. The 64D p99 was noisy and
regressed by median, while 384D p95/p99 improved. Exact-search controls were
approximately flat. The existing dot-score error check and all 38 index-crate
tests passed. These are local index benchmarks, not end-to-end results; see the
[paired captures](../bench/results/vector-hnsw-dot-unrolled-windows-20261024.json).

An eight-accumulator candidate was compared with the four-accumulator version
in ten alternating release pairs on the same Windows host. For 3,000 384D
vectors, median index build time fell from 556.6 ms to 495.3 ms (11.0%), and
the candidate built faster in all ten pairs. ANN p50 was nearly flat (487.7 to
480.2 µs, -1.6%); p95 regressed 5.3% (681.9 to 717.8 µs), while p99 improved
3.1%. Recall@10 stayed at 0.971. A 5,000-vector 64D control was effectively
flat for build time (-0.3%) and p50 (-1.8%), with recall unchanged at 0.979.
All 38 index-crate tests passed, including score-bound checks at tail and large
dimensions. This is a local build-time result, not a general query-latency
gain; see the [raw captures](../bench/results/vector-hnsw-dot-eight-accumulators-windows-20261024.json).

Spelling out the eight accumulator updates instead of indexing them in a loop
did not produce a reliable gain. Across eight alternating release pairs on
the same host, 384D build median changed by -2.0% (six pairs faster), with ANN
p50/p95 effectively flat and p99 4.4% higher. The 64D build median was 0.6%
higher and p95 6.7% higher. Recall was unchanged in both workloads. The
explicit statements were discarded; see the [raw captures](../bench/results/vector-hnsw-explicit-eight-lane-unroll-windows-20261024.json).

An HNSW build experiment moved the selected-neighbor vector into the new node
instead of cloning it before linking reverse edges. Eight alternating release
pairs showed median build time effectively flat: 396.2 to 399.0 ms for 5,000
64D vectors and 561.2 to 551.0 ms for 3,000 384D vectors. The small differences
were not consistent across dimensions, and recall was unchanged, so the
existing ordering remains. See the [raw captures](../bench/results/vector-hnsw-neighbor-move-experiment-windows-20261024.json).

Exact vector search now uses four independent `f64` accumulators for its dot
product as well. Across three alternating release pairs, exact-search median
p50 fell 50.3% for 5,000 64D vectors and 49.6% for 3,000 384D vectors; median
p95/p99 also fell in both workloads. The f64 score error stayed below 1e-12
at dimensions 63–65, 383–385, and 4095–4096, including scalar-tail cases, and
the ANN recall controls were unchanged.
All 38 index-crate tests passed. These are local in-memory results; see the
[paired captures](../bench/results/vector-exact-dot-unrolled-windows-20261024.json).

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

An experiment replaced the temporary document-frequency hash map with a vector
aligned to query terms, avoiding cloned term strings. On a Windows Ryzen 5 4500,
10,000-document, four-partition, two-common-term benchmark, median p50 was
644.5 microseconds before and 650.7 microseconds after, with no measured latency improvement. The
change was not retained; allocation effects for long queries remain unmeasured.
See the [raw captures](../bench/results/text-search-document-frequency-vector-20261024.json).

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

An experiment to precompute and reserve the exact WAL write-payload size did
not show a benefit. On a Windows Ryzen 5 4500, a single-shard Hot-mode run with
one worker, 2,000 transactions, eight 128-byte writes per transaction, and no
per-write fsync measured median throughput of 6,044 writes/s before and 5,112
writes/s with the candidate. Median sampled p50 was 1,269 microseconds before
and 1,409 microseconds with the candidate. These captures were sequential,
not interleaved, and varied substantially; the candidate was discarded rather
than treated as a performance improvement. The [raw captures](../bench/results/wal-payload-reserve-experiment-windows-20261024.json)
record the run counts and benchmark limits.

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

For exactly one delta over a snapshot, a direct two-cursor merge avoids the
binary heap used by the general multi-segment scan. Eight alternating release
pairs on this Windows host (10,000 snapshot rows, 100 returned rows) reduced
median scan p50 from 31.6 to 28.6 µs (9.5%) and p95 from 43.9 to 37.6 µs
(14.5%); seven of eight candidate p50 captures were faster. A 100-delta control
uses the unchanged heap path; its p50 was flat and p95 noisy. The storage
tests cover overlapping keys, tombstones, expiry, read timestamps, and cursor
pagination in the two-segment path. These are local warm-cache scans, not
end-to-end SQL latency; see the [paired captures](../bench/results/segment-scan-two-way-merge-windows-20261024.json).

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

The current release build also includes cached broadcast envelopes so JSON
payloads and Supabase-compatible protocol frames are encoded once and their
immutable bytes are shared across subscribers rather than copied for every
delivery. A smaller
20-connection loopback run on the same 12-vCPU machine delivered all 20,000
messages with zero publish or receive errors at 13,089 publish requests/s and
2.81 million fanout deliveries/s. Publish-to-client latency was 38.9 ms p50,
69.6 ms p95, and 77.6 ms p99. This is a separate workload and should not be
treated as a direct replacement for the 100-connection burst result.

Run it yourself:

```bash
ryme-bench realtime --addr 127.0.0.1:8080 \
  --api-key local-dev-anon --publish-api-key local-dev-writer \
  --connections 100 --connection-concurrency 256 --messages 10000 \
  --payload-bytes 128 --json
```

WebSocket subscribers authenticate with `--api-key`, which must be an `anon`
key when passed in the WebSocket query string. It defaults to
`RYME_PUBLIC_API_KEY` (then `RYME_API_KEY` for compatibility). HTTP publishers
authenticate with `--publish-api-key`, which needs permission to publish. The
publisher key defaults to `--api-key` for servers that allow that key to
publish. To keep a publisher secret out of command-line arguments, set
`RYME_BENCH_PUBLISH_API_KEY`; it takes precedence over the default.

Realtime quotas count fanout deliveries as well as publishes. Set the
benchmark tenant's QoS tier high enough for the selected connection count and
publish rate; for a local server using the `default` tenant, an operator can
run `rymedb-cli qos tier default dedicated_cluster` before this workload.

By default, all subscribers and publishes use the single `bench` channel (a
hot-topic test). Set `--channel-count 10` to spread both connections and
publishes round-robin over `bench-0` through `bench-9`, approximating a uniform
multi-topic workload. Delivery percentage is calculated from the subscribers
and successful publishes on each channel, so channels with no subscribers do
not inflate the expected-delivery count.

The cached-envelope run used 20 connections, 64 connection concurrency, 1,000
messages, and 32 concurrent publishers:

```bash
ryme-bench realtime --addr 127.0.0.1:8080 \
  --api-key local-dev-anon --publish-api-key local-dev-writer \
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

A separate Windows 11 release run on the AMD Ryzen 5 4500 with the same
100-connection, 10,000-publish workload delivered all 1,000,000 expected
messages. It measured 491k–495k fanout deliveries/s, p50 6.756–6.859 ms,
p95 159.9–176.8 ms, and p99 322.1–332.7 ms. The long tail remains poor, and
these Windows results are not directly comparable to the Linux capture. Raw
results are in
[`bench/results/realtime-shared-bytes-100x10000-windows.json`](../bench/results/realtime-shared-bytes-100x10000-windows.json).

On that same Windows host and workload, writer batch limits of 32 and 128 were
compared with the current limit of 64. The 64-frame batch had median p95/p99 of
110/275 ms; 32 had 136/314 ms and 128 had 128/310 ms. Throughput changed by
about 1% for both alternatives, without a tail-latency improvement. The current
64-frame setting is retained. All captures delivered 1,000,000 messages with
zero errors; see the [batch-size captures](../bench/results/realtime-writer-batch-size-windows-20261024.json).

## How to read the numbers

- Compare the same protocol and acknowledgement semantics.
- Separate server execution, network, fsync, quorum, and client scheduling.
- Report percentile histograms, not only average or peak throughput.
- Never compare an in-process probe with a networked database claim.
- Re-run on clean, identical hardware before publishing a headline.

There is no defensible blanket “10000x faster than PostgreSQL” or “100x faster
than Valkey” claim. The goal is workload-specific, reproducible improvement
while retaining transactions, durability, realtime, and compatibility.

For 16-key read-only batches across 80 Standard-mode delta segments, three
sequential release captures on an AMD Ryzen 5 4500 measured p50 at 36.9–37.2 µs
after reusing each key's Bloom hashes across segment checks, down from 71.3 µs
in all three baseline captures (48% lower by median capture p50). After p95 was
37.6–43.2 µs, compared with 75.2–89.6 µs before. The benchmark seeds 80 records
as 80 delta segments, uses 128-byte values, 100 iterations, and disables WAL and
segment sync; it measures the in-process transaction path with a warmed cache,
not network latency or durable write performance. Exact segment engine lookups
still confirm Bloom positives. See the [raw captures](../bench/results/txn-read-batch-segment-bloom-windows-20261024.json).
