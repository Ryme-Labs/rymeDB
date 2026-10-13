# Migration from other databases

Migrate in stages: inventory, export, transform, load, verify, dual-run,
cut over, and keep the source untouched until rollback is no longer needed.
Never import an internal PostgreSQL, Redis, Neon, or Supabase storage file;
move data through SQL, logical dumps, REST, or RESP.

## Compatibility matrix

| Source | First tool | Main review points |
| --- | --- | --- |
| PostgreSQL | `pg_dump`, `COPY`, wire protocol | Extensions, PL/pgSQL, custom types, planner assumptions |
| Supabase | SQL dump + policy inspection | Auth provider, RLS expressions, storage/functions, generated REST queries |
| Neon | Logical SQL/data export + branch JSON | Branch metadata is mapped, not copied from internal pages/WAL |
| Redis/Valkey | RESP scan/export | TTLs, data structures, stream/group semantics, durability expectations |
| Other SQL | CSV/JSON plus schema translation | Types, constraints, indexes, transaction semantics |

## PostgreSQL or Supabase

### 1. Inventory

List tables, indexes, extensions, functions, triggers, policies, roles,
sequences, generated columns, custom types, and background jobs. Mark each as
`direct`, `rewrite`, or `unsupported` before moving data.

### 2. Export schema and data

```bash
pg_dump --schema-only --no-owner source_db > schema.sql
pg_dump --data-only --format=plain source_db > data.sql
```

Prefer `COPY` for large tables. The wire gateway supports `COPY FROM STDIN`
and `COPY TO STDOUT`; the HTTP copy endpoint accepts bounded batches. Preserve
nulls, encodings, identity values, and primary keys.

### 3. Apply reviewed schema

```bash
rymedb-cli migrate apply 001-schema "$(cat schema.sql)" --author migration-bot
rymedb-cli migrate ledger
```

Run schema migrations with a platform superuser credential. The main SQL
catalog is shared across tenant namespaces, so tenant owner/admin credentials
are intentionally rejected for DDL, role, and privilege statements until
tenant-scoped catalogs exist.

Split a dump into compatible migration units when one large statement makes
review or rollback difficult. Duplicate migration ids are rejected by the
branch-scoped hash chain.

### 4. Load and verify

```bash
psql 'postgresql://postgres@127.0.0.1:5433/postgres' \
  -c "COPY messages FROM STDIN"
```

Compare row counts, primary-key sets, null counts, checksums of canonicalized
rows, sequence positions, and application-level invariants. Then run the
application's generated SQL and realtime tests.

### Supabase-specific review

- Map Supabase JWT issuer/audience and `tenant_id`/role claims.
- Translate simple `auth.uid() = user_id` RLS policies using JWT `sub` and
  test default-deny behavior. Keep tenant isolation policies separate.
- Review storage buckets, Edge Functions, scheduled jobs, and extensions;
  they are not automatically moved by a database dump.
- Replace Supabase Realtime subscriptions with `/v1/stream`,
  `/v1/query-stream`, or the Supabase-compatible channel endpoint.

The analyzer is useful for a first pass:

```bash
rymedb-cli migrate supabase supabase-dump.sql
```

It reports tables, policies, and detected tenant columns. Treat its output as
a plan requiring review.

## Neon branches

```bash
rymedb-cli migrate neon branches.json
```

The mapper converts branch names, parent branches, and LSN high/low values to
rymeDB branch plans. Create a branch and load logical data; internal page
formats and WAL are not portable. A branch is copy-on-write and selected with
`X-Ryme-Branch` or `branch=` on WebSockets.

## Redis or Valkey

Map by data type rather than copying serialized internals:

| Source type | rymeDB path |
| --- | --- |
| String/session | RESP `SET`/`GET` or `/v1/kv` |
| Hash/list/set/zset | RESP commands; validate JSON encoding and ordering behavior |
| Stream | `XADD`/`XREAD`/consumer groups or durable topics when application replay is required |
| Pub/sub | Broadcast for ephemeral fanout; durable topics for recovery |
| TTL | `EX`, `PX`, and key TTL endpoints |
| Lua | Review sandboxed `EVAL` behavior and rewrite filesystem/network assumptions |

Use a cursor-based `SCAN`, dual-write, and compare key counts and TTL samples.
Do not treat RESP compatibility as proof that every Redis module is present.

## Cutover plan

1. Freeze schema changes and record the source migration position.
2. Bulk load a consistent snapshot.
3. Replay changes or dual-write while measuring lag.
4. Run read-only and authorization verification.
5. Pause writes, drain lag, and switch clients.
6. Keep the source read-only through the rollback window.
7. Record the cutover migration and backup checkpoint.

For large transfers, use `ryme-migrate` helpers to parse COPY/INSERT input,
chunk rows, validate data, and gate cutover on bounded CDC lag. Rehearse the
rollback with a branch or disposable node first.
