# Isolation and retry behavior

Every SQL statement runs through MVCC. An explicit transaction keeps one
snapshot and one write set across statements; autocommit statements commit
individually.

## Levels

| Level | Detects | Use |
| --- | --- | --- |
| `SERIALIZABLE` (default) | Write/write, read/write, and conservative table-scan phantom conflicts | Counters, transfers, and invariants |
| `SNAPSHOT` | Write/write conflicts | Read-heavy workloads |

```sql
SHOW transaction_isolation;
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
RESET transaction_isolation;
```

PostgreSQL `READ COMMITTED` and `REPEATABLE READ` requests map to the engine's
snapshot semantics. The wire gateway reports the selected session setting.

## Retry loop

Serializable conflicts return SQLSTATE `40001`:

```text
begin
  execute all reads and writes
  commit
on 40001
  rollback
  sleep with bounded jitter
  retry the complete transaction
```

Do not retry only the last statement: the snapshot and all earlier reads are
part of the transaction's validation. Cap retries and return a useful error
when contention remains high.

Transactions restored or forwarded below the retained MVCC history are rejected
before reads or commits, with a `transaction snapshot too old` conflict. Start
a new transaction to obtain a snapshot within the current retention window.

Full-table scans record a conservative table-level read dependency. Separate
hot counters or use point reads when independent keys must commit in parallel.

## Session controls

`statement_timeout` accepts milliseconds or `ms`, `s`, `min`, and `h` units.
Timeouts and client cancellation return SQLSTATE `57014`. `SET LOCAL` values
are restored by commit, rollback, and savepoint rollback.

## Debugging conflicts

1. Log the transaction id, SQLSTATE, table/key, and retry count.
2. Check `/v1/observe/slow` and `/v1/traces` for long transactions.
3. Reduce transaction scope and avoid holding a snapshot while doing network
   work.
4. Split unrelated tables or point operations when a scan dependency is the
   source of phantom conflicts.
