# Performance report

All numbers below are measured, not projected. Raw histograms live in
`bench/results/`. Do not quote a headline ratio without the workload,
topology, durability mode, payload and concurrency beside it.

## This machine

12 CPUs, 15 GiB RAM, x86_64, loopback TCP, 128-byte values, 10k keyspace,
8 client workers, 100k ops, warm keyspace, `ryme-bench resp --pipeline 1`.

Benchmark precondition: set the measured tenant to `dedicated_cluster`
(`POST /v1/qos/tier`) first. Every gateway enforces token-bucket QoS, so at
the default `shared` tier writes throttle to ~1k qps and the numbers below
are unreachable. Re-verified 2026-10-02 after QoS landed on the wire
gateways (`bench/results/bench-rerun-{get,set,mixed,tx}-p1.json`, Valkey tx
in `bench-rerun-valkey-tx-p1.json`): get 102451 qps (p50 66us, p99 231us),
set 67244 qps (p50 109us, p99 320us), mixed 87477 qps (p50 78us, p99 270us),
tx 35390 blocks/s (p50 195us, p99 761us) vs Valkey tx 51995 blocks/s
(p50 143us, p99 399us) — all at or better than the tables below, i.e. no
regression from the CDC/QoS/tracing additions, and the honest Valkey gap
(~1.5x p50, ~1.9x p99) reproduces. One tx run on a server with 400k prior
writes measured 7.3k blocks/s; repeat runs on identical state measured
~35k, so that single point reads as shared-hardware noise — re-run before
quoting either way.

## Fresh run (2026-10-02, durable local WAL, single node)

| workload | qps | p50 | p90 | p95 | p99 | p99.9 | max | errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| get | 106586 | 64us | 123us | 153us | 247us | 560us | 1054us | 0 |
| set | 54139 | 133us | 241us | 289us | 429us | 793us | 1881us | 0 |
| mixed | 81804 | 81us | 162us | 201us | 355us | 1066us | 2054us | 0 |

Source files: `bench/results/bench-get-p1.json`, `bench-set-p1.json`,
`bench-mixed-p1.json`. Historical same-box Valkey comparisons live in
`bench/results/matrix.json` (`valkey-*` vs `rymedb-*` rows, same client).

## Compound transactions (2026-10-02, loopback TCP, release binaries)

Workload `tx` (`ryme-bench resp --workload tx`): one `MULTI` block of
`SET` 128-byte value + `INCRBY` counter + `GET`, committed as a single
transaction; 20k blocks, 8 workers, per-worker keys (no cross-worker
contention), 0 errors everywhere. Same client, same workload, both
servers loopback.

| system | durability | qps (blocks/s) | p50 | p99 |
| --- | --- | ---: | ---: | ---: |
| rymeDB | durable local WAL (fsync per commit) | 32739 | 207us | 782us |
| rymeDB | memory (no fsync) | 36810 | 188us | 645us |
| Valkey | no persistence | 53495 | 142us | 343us |

Source files: `bench/results/rymedb-tx-p1.json`,
`rymedb-tx-mem-p1.json`, `valkey-tx-p1.json`.

Reading this honestly: Valkey leads by ~1.4x at p50 and ~2.3x at
p99 on this workload, so the "2-10x better than Valkey" goal is still
**not met** — but the gap closed from ~14x after fixing two real
issues this round: commit-path lock contention (the commit lock in
`begin()` plus a global live-transaction registry; throughput rose
~10x, 3.4k to 32k blocks/s) and a benchmark-methodology flaw (reused
keys grew unbounded version chains; keys are now spread over the
keyspace). Our rows also fsync every commit (durable) and validate
serializable OCC per transaction, which Valkey does not do; fsync
itself accounts for only ~10% (durable vs memory rows). Remaining
leads under investigation: engine-lock granularity is the prime
suspect. Single-run numbers on shared hardware; re-run before quoting.

## Mixed simple ops (2026-10-02, same setup, 40k ops, 8 workers)

`mixed` alternates `SET` 128-byte values and `GET`s over a 100k
keyspace. Here reads parallelize under the engine read-write lock:

| system | qps | p50 | p99 |
| --- | ---: | ---: | ---: |
| rymeDB durable | 91724 | 73us | 278us |
| Valkey, no persistence | 127475 | 59us | 124us |

Simple-command p50 is within 1.2x; the remaining gap concentrates in
write-heavy transactional paths (per-command transaction machinery,
WAL append, OCC validation) rather than the data-plane reads.

## Replicated transactions (3-node Raft, loopback, durable WAL)

`cluster_replication_latency` elects a leader, then times 20 single-key
writes: leader acknowledgement (quorum commit) and visibility on a
follower poll (1ms poll):

| metric | median | max over 20 |
| --- | ---: | ---: |
| leader ack (quorum) | ~2.3ms | — |
| follower-visible | ~3.5ms | ~4.3ms |

Stable across runs within ~10%. The test pins liveness (every write
visible < 2s) and a 100ms median bound; the measured ~3.5ms sits at
the edge of "low-single-ms" — honest status: close, with fsync-per-commit
on three nodes as the known cost.

In-process engine probe (no socket, not comparable to networked results):
200k sync-commit writes at ~670k qps, 10k point reads at ~1us mean.

## Realtime fanout benchmark

The WebSocket path can be measured against a running server with:

```text
ryme-bench realtime --addr 127.0.0.1:3000 --api-key ryme-dev-key \
  --connections 100 --connection-concurrency 256 --messages 10000 \
  --payload-bytes 128 --json
```

The command opens all subscribers before publishing, sends authenticated
`POST /v1/broadcast` requests with bounded concurrency, and waits for every
subscriber to receive every successful publish. Connections are established
with bounded concurrency (`--connection-concurrency`) so high-connection runs
do not serialize setup. It reports publisher qps,
fanout deliveries per second, sampled publisher-to-client latency percentiles,
delivery percentage, and errors. The benchmark carries a monotonic timestamp
from the publisher process in each payload, so the latency fields are valid
for same-process or same-host comparisons; cross-host runs require synchronized
clocks or should treat those fields as advisory. A run is only valid when
`publish_errors=0`, `receive_errors=0`, and `delivery_percent=100`;
the initial intra-region realtime target is
`publish_to_client_p50_us <= 10000` and `publish_to_client_p99_us <= 50000`.
record the CPU, memory, payload, connection count, publish concurrency, and
server QoS tier beside the result. Broadcast channels use a dedicated
32-shard registry and sequence allocation path, separate from CDC/query,
presence, and durable-topic state. This is an end-to-end workload gate, not a
claim about all realtime workloads or connection capacity.

## What we claim

- Same-AZ loopback reads at p50 under 0.1 ms and p99 under 0.5 ms for hot
  128-byte keys against a single durable node.
- Runtime `/metrics` and Prometheus output expose p50, p90, p95, p99, p99.9,
  and max tail measurements for the request histogram.
- Durable single-row writes acknowledged at p50 ~0.13 ms, p99 ~0.43 ms.
- Metadata branch creation is O(1) in data size (manifest + WAL pointer).
  Pinned by `extras_branch_metadata_latency`: with 1000 rows loaded, the
  median of 5 HTTP branch creates and one promote each complete under
  250 ms on loopback.

## What we do not claim

- No blanket "100x faster than Valkey" headline. Valkey command execution is
  already sub-microsecond; over the network, transport dominates both systems.
  Compare `matrix.json` rows workload-by-workload instead.
- No cross-machine, cross-region or PostgreSQL comparison is made here; those
  suites require the multi-node harness and are tracked as future work.
- Pipelined (`--pipeline 16`) latency rows are batch means, not per-op latency.
