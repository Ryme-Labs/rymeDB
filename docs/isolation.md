# Transaction isolation

rymeDB defaults to `SERIALIZABLE` and offers `SNAPSHOT` (snapshot
isolation) as a per-session opt-out over the PostgreSQL wire protocol.
Every statement runs as a single optimistic-concurrency transaction:
reads execute against a stable snapshot timestamp and the write set is
validated at commit.

## Levels

| Level | Aborts on | Use when |
| --- | --- | --- |
| `SERIALIZABLE` (default) | Write-write, read-write and phantom (range) conflicts | Correctness first: counters, read-modify-write, multi-key invariants |
| `SNAPSHOT` | Write-write conflicts only | Contended read-heavy workloads that tolerate stale reads |

Serializable validation covers point reads and full-table scans. A scan
records the scanned table, so any concurrent commit touching that table
aborts the scanner with a `phantom conflict`. This is conservative: two
transactions touching disjoint keys of the same table can conflict. Keep
hot counters on isolated keys or separate tables when that matters.

`SNAPSHOT` never aborts on stale reads but still rejects concurrent
writes to the same key, so no update is silently lost.

## PostgreSQL wire

```sql
SHOW transaction_isolation;
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

The setting is per connection and applies to simple and extended
protocol queries. `RESET transaction_isolation` and `RESET ALL` restore
the `SERIALIZABLE` default. No other level is accepted; there are no
multi-statement transaction blocks, so each statement commits on its own.

## Conflict errors

Conflicts surface as SQLSTATE `40001` with one of `write-write
conflict`, `read-write conflict` or `phantom conflict`. Retry the
statement on `40001`.

The level split is pinned by Jepsen-style proof tests in `ryme-txn`:
classic write-skew commits twice under `SNAPSHOT` but aborts once under
`SERIALIZABLE`, and 400 concurrent balance transfers across 8 threads
preserve the total under optimistic retry.
