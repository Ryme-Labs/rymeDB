# PostgreSQL compatibility

Connect a normal PostgreSQL client to the PostgreSQL wire listener. The
default is `127.0.0.1:5433`; the database name and user are accepted for
driver compatibility while tenant/auth behavior is controlled by the server.

Authenticated application and tenant owner/admin sessions cannot use the
startup `user` value to become `postgres`. Global schema, role, and privilege
statements require a platform superuser; data statements remain subject to
the authenticated tenant and SQL/RLS policy.

```bash
psql 'postgresql://postgres@127.0.0.1:5433/postgres'
PGPASSWORD="$RYME_PG_PASSWORD" psql -h db.example.com -p 5433 -U app -d app
```

## Covered protocol behavior

- Startup and authentication, `BackendKeyData`, and cancel requests.
- Startup parameter lists require PostgreSQL's final zero-byte terminator and
  reject bytes following it.
- Startup, TLS negotiation, and authentication must complete within 15 seconds;
  the deadline ends before the authenticated session begins.
- Simple and extended query flows (`Parse`, `Bind`, `Describe`, `Execute`,
  `Sync`) with prepared portal invalidation.
- Explicit transactions, `SAVEPOINT`, `ROLLBACK TO`, `RELEASE`, and aborted
  transaction state.
- `COPY FROM STDIN` and `COPY TO STDOUT`, including nulls and escaping.
- `COPY FROM STDIN` buffers at most 64 MiB per connection and shares a 256 MiB
  process-wide buffer budget across active COPY sessions.
- `LISTEN`, `UNLISTEN`, and post-commit `NOTIFY` notifications.
- Common driver probes such as `version()`, `current_setting`, `SHOW ALL`,
  `SET NAMES`, `SET TIME ZONE`, and `application_name`.
- `statement_timeout`, SQLSTATE errors, RLS/catalog introspection, views,
  sequences, roles, grants, and persisted migration metadata.
- Ordered SQL scans sort the full result up to the query materialization bound
  of 100,000 rows and 64 MiB; larger ordered results fail with an overload error
  instead of returning a partial, incorrectly ordered result.
- `LIMIT` and `OFFSET` each accept values up to 10,000; primary-key-ordered
  scans size the read window from their combined values so offset pages retain
  all requested rows.

## Compatibility boundary

rymeDB is not PostgreSQL's executor, extension ABI, or storage format. Review
unsupported or partially supported application features before migration:

| Area | Guidance |
| --- | --- |
| Extensions | Do not assume arbitrary extensions, FDWs, PostGIS, or custom types exist. |
| PL/pgSQL | Common function/trigger metadata and simple timestamp triggers are supported; arbitrary procedural code needs review. |
| Query planner | `EXPLAIN` is available, but plans and indexes are not PostgreSQL plans. |
| Isolation | `READ COMMITTED`/`REPEATABLE READ` map to snapshot semantics; serializable validation is engine-specific. |
| Large COPY jobs | Chunk requests and verify constraints; HTTP COPY is capped at 10,000 rows per request. |
| LISTEN/NOTIFY | Notifications are ephemeral and process-scoped; use durable realtime topics for replay. |

Run your ORM's generated SQL against a disposable rymeDB data directory and
keep the compatibility test in CI. The repository's PostgreSQL tests are a
reference, not a guarantee for every driver.

## Migration helpers

Use PostgreSQL `COPY` for data, then apply schema through the migration ledger:

```bash
pg_dump --schema-only source_db > schema.sql
rymedb-cli migrate apply 001-schema "$(cat schema.sql)" --author migration-bot
```

For a full plan, see [migration](migration.md).
