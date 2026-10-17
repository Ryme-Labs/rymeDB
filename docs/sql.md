# SQL and transactions

The SQL engine is a Rust executor behind the HTTP SQL endpoint and the
PostgreSQL wire gateway. It supports a relational schema path and a
key/value-compatible path, both using MVCC transactions and the same committed
change stream.

## Common SQL surface

| Category | Implemented examples |
| --- | --- |
| DDL | `CREATE/ALTER/DROP TABLE`, columns, defaults, identity/serial, primary/unique/foreign keys, checks, indexes |
| DML | `INSERT`, multi-row insert, `UPDATE`, `DELETE`, `UPSERT`, `RETURNING`, `COPY` |
| Queries | projections, aliases, predicates, `IN`, `BETWEEN`, `LIKE`, null-safe comparisons, `ORDER`, `LIMIT`, `OFFSET`, `DISTINCT` |
| Relational | joins, `GROUP BY`, aggregates, CTEs, `INSERT ... SELECT`, `UPDATE ... FROM`, `DELETE ... USING` |
| Schema objects | views, materialized views, sequences, functions/triggers metadata, roles and grants metadata, RLS policies |
| PostgreSQL values | JSON paths, arrays, UUID/time helpers, SQL `NULL` behavior |

This is a compatibility-oriented subset, not a promise to run arbitrary
PostgreSQL extensions or PL/pgSQL unchanged. Check [migration](migration.md)
before cutover.

## Query resource bounds

Each SQL statement is limited to 1 MiB, 65,536 tokens, and 64 recursive parse
levels. Larger or more deeply nested statements are rejected before execution.
Split generated SQL into smaller statements when it exceeds these limits; use
the bulk-copy endpoints for large data loads.

## HTTP SQL

```bash
curl -fsS -X POST \
  -H 'Authorization: Bearer local-dev-key' \
  -H 'Content-Type: application/json' \
  --data '{"sql":"SELECT * FROM messages WHERE room = $1","params":["lobby"]}' \
  http://127.0.0.1:8080/v1/sql
```

The CLI shortens this to:

```bash
rymedb-cli sql 'SELECT count(*) FROM messages'
rymedb-cli sql explain "SELECT * FROM messages WHERE id = 'm-1'"
```

`POST /v1/sql/copy` handles bulk rows. PostgreSQL `COPY FROM STDIN` and
`COPY TO STDOUT` are available through the wire protocol.

## Transactions

```sql
BEGIN;
UPDATE accounts SET balance = balance - 10 WHERE id = 'a';
UPDATE accounts SET balance = balance + 10 WHERE id = 'b';
COMMIT;
```

Outside an explicit block, each statement is its own transaction. Explicit
transactions support `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE`,
`COMMIT`, and `ROLLBACK`. An error leaves the transaction aborted until it is
rolled back, matching PostgreSQL's normal protocol behavior.

The default isolation level is serializable optimistic concurrency control.
Conflicts return SQLSTATE `40001`; retry the whole transaction with bounded
backoff. `SNAPSHOT` is available for workloads that tolerate weaker read
conflict detection. See [isolation](isolation.md).

## Schema and migrations

Schema metadata is persisted under `data_dir/schema.json` and rebuilt on
startup. Use the migration ledger rather than relying on an application-side
“already ran” flag:

```bash
rymedb-cli migrate apply 2026-01-create-messages \
  'CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT NOT NULL)' \
  --author deyoyk
rymedb-cli migrate ledger
```

The ledger is tenant- and branch-scoped and hash chained. Duplicate ids are
rejected before a second execution. See [migration and compatibility](migration.md).

## PostgreSQL driver setup

```bash
psql 'postgresql://postgres@127.0.0.1:5433/postgres' \
  -c 'SHOW transaction_isolation; SELECT version();'
```

Common startup/session probes, prepared statements, `statement_timeout`,
`SET LOCAL`, `LISTEN/NOTIFY`, cancellation, and PostgreSQL COPY are covered by
the protocol tests. Use the wire gateway for ORMs, but keep a compatibility
test suite for the ORM features your application relies on.
