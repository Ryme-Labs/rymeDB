# Migration and compatibility

## Ledger

`POST /v1/migrate/apply` executes one SQL statement and records it in the
control-plane hash-chained ledger (`{id, sql, author, checksum,
parent_checksum, schema_version}`); duplicate ids get `409` before anything
runs twice. Migration applications are serialized per server process, so
concurrent requests cannot execute the same id before the ledger is updated.
`GET /v1/migrate/ledger` returns entries in apply order
with the schema version and chain validity. Branch metadata is persisted under
`data_dir/branches.json`, while the migration ledger and backup checkpoints are
persisted under `data_dir/control.json`; snapshots and archives cover data,
not the migration ledger.

## PostgreSQL

The wire gateway advertises `BackendKeyData` and honors PostgreSQL
`CancelRequest` packets, returning SQLSTATE `57014` for a canceled statement
at the next execution boundary.

1. Export schema, then bulk load with PostgreSQL `COPY table FROM STDIN`
   through the wire gateway or `POST /v1/sql/copy` in 500-row transactions
   (max 10000 rows per request). The wire path accepts tab-separated text,
   decodes standard COPY escapes, and returns `COPY n` after the final
   `CopyDone` message.
2. Parse `COPY ... FROM stdin` tab-separated dumps with `ryme-migrate`
   `parse_copy_text`, or `INSERT INTO ... VALUES` lines with
   `parse_insert_line`.
3. Validate with `validate_rows`, chunk with `chunk_rows`, gate cutover with
   `plan_cutover(snapshot_rows, cdc_lag_ms)` (ready when lag <= 1000 ms).
4. Verify types, nulls, sequences, constraints, prepared statements,
   timezones, JSON, arrays, `RETURNING`, `ON CONFLICT`, session variables.

## Supabase

REST: `GET /rest/v1/:table?select=&key=eq.<id>&order=key.desc&limit=&offset=`,
`POST /rest/v1/:table` with `{key, value}` or `{rows: [...]}`,
`DELETE /rest/v1/:table?key=eq.<id>`.

GraphQL: `POST /graphql` with `{ table(key: "k") }` or `{ table(limit: 100) }`.

Realtime: `GET /v1/stream?table=` for CDC, `GET /v1/query-stream?table=`
for snapshot plus updates. The npm/TypeScript SDK exposes
`subscribeTable`/`subscribeQuery`; Java and Rust clients can use the same
HTTP/WebSocket endpoints directly.

Auth accepts Supabase-style JWT claims (`sub`, `tenant`, `roles`, `exp`)
where feasible; use separate issuers for app users versus operators.

RLS-filtered scans continue through storage pages before applying user-facing
limits, so leading rows from other tenants do not starve authorized results.

Dump analysis: `POST /v1/migrate/supabase` with `{dump}` returns tables and
`CREATE POLICY` entries with detected tenant columns (`auth.uid() = <col>`
maps to `allow_table` RLS). CLI: `ryme migrate supabase dump.sql`.
`WITH CHECK`-only policies yield empty expressions and need manual review.

## Neon

Map branch JSON (`[{name, parent, lsn}]`) with `POST /v1/migrate/neon` into
`{id, parent, base_commit_ts}` plans (LSN `high/low` hex to u64), then create
branches and cut over. CLI: `ryme migrate neon branches.json`. Migrate data
at the logical level; internal page formats are never imported. A branch created
with `base_commit_ts` set to zero pins the current commit automatically.

Branch reads use MVCC snapshots without copying rows. Send
`X-Ryme-Branch: <id>` to `/v1/sql`, `/v1/kv`, `/rest/v1`, or `/graphql` to
read and write the selected tenant's branch through a copy-on-write overlay.
Parent rows are visible at the branch base commit, branch mutations are stored
under the branch namespace, and branch deletes mask (but do not remove) parent
rows. Branch table and index definitions are stored under the data directory
per tenant/branch, so branch DDL survives request/executor recreation without
changing `main`. `main` keeps the live view.
Resetting a branch advances its storage epoch, so prior branch-local writes are
discarded from the new view; deleting a branch removes its selectable metadata.
Parents with live child branches must be deleted from the leaves upward so
manifest references remain safe for segment garbage collection.
`GET /v1/branches/<id>/diff?against=<branch>` keeps the manifest comparison
fields and adds `changes`: effective row differences with the table name,
base64url primary key, and base64url values on each side (`null` means the row
is absent). This includes changes inherited from the branch snapshot, not only
rows written directly in the branch overlay.

## SQL dialect

Projections, predicates, ordering, grouping, and RETURNING accept common
relation-qualified references such as source.id and source.payload.
CREATE INDEX CONCURRENTLY and DROP INDEX CONCURRENTLY are accepted for
compatibility; index publication remains atomic.
UPDATE target SET ... FROM source WHERE target.id = source.id supports
qualified join columns, source predicates, source-backed assignments, and
RETURNING.
DELETE FROM target USING source WHERE target.id = source.id supports qualified
join columns, source predicates, and RETURNING.

`CREATE TABLE` (including `IF NOT EXISTS`, column types, single-column and composite
`PRIMARY KEY` constraints, single-column and composite `UNIQUE` constraints,
`CHECK`, `FOREIGN KEY ... REFERENCES`, `NOT NULL`, and `DEFAULT` values) plus single-column `ALTER TABLE ... ADD COLUMN`,
`ALTER TABLE ... ADD PRIMARY KEY` and `ADD CONSTRAINT` for `UNIQUE`, `CHECK`, and `FOREIGN KEY ... REFERENCES`,
plus `ALTER TABLE ... DROP CONSTRAINT [IF EXISTS]` for named constraints,
`DROP COLUMN`, `RENAME COLUMN`, and `ALTER COLUMN` default/nullability/type changes
(including `ALTER COLUMN ... TYPE` for supported scalar, boolean, array, and JSON values)
(including `IF [NOT] EXISTS` where supported), plus
foreign-key actions `ON DELETE`/`ON UPDATE RESTRICT`, `CASCADE`, `SET NULL`,
and `SET DEFAULT`,
`DROP TABLE [IF EXISTS]`, `TRUNCATE TABLE`, and `DROP INDEX [IF EXISTS]`,
including `TRUNCATE ... RESTART IDENTITY`, `CONTINUE IDENTITY`, and
foreign-key `CASCADE`/`RESTRICT`,
with PostgreSQL `information_schema.tables` and
`information_schema.columns` introspection, custom key/value `INSERT`, and PostgreSQL-style
`INSERT INTO table (...) VALUES (...)`; `ON CONFLICT (columns) DO UPDATE` uses
the matching primary or unique constraint and supports `EXCLUDED.column`
assignments and a conditional `WHERE` predicate, while targeted or untargeted
`ON CONFLICT DO NOTHING` skips conflicting rows,
and `UPDATE`/conflict assignments support atomic numeric and text expressions
such as `count = count + 1`, `count + EXCLUDED.count`, concatenation,
`COALESCE`, `GREATEST`, and `LEAST`,
and `INSERT INTO table DEFAULT VALUES` materializes identity/serial and column
defaults with normal constraints and `RETURNING`,
`INSERT INTO target (...) SELECT ... FROM source [WHERE ...]` copies projected
and filtered rows atomically,
simple one or more `WITH name AS (SELECT * FROM source [WHERE ...])` CTEs can
feed outer selects and `INSERT ... SELECT` statements,
prepared PostgreSQL parameters are bound outside SQL literals/comments with
multi-digit placeholder support and scalar type preservation,
extended-protocol `Describe` returns prepared parameter metadata for drivers,
`ALTER TABLE ... ALTER COLUMN ... TYPE ... USING column::type` is accepted for
standard self-column casts,
declared array columns (`text[]`, `integer[]`, and similar) accept PostgreSQL
`ARRAY[...]` and `'{...}'` literals and remain structured arrays in JSON rows,
JSONB projections support PostgreSQL `->` and `->>` operators, including
chained object and array paths in selected columns,
`serial`/`bigserial`/`smallserial` and `GENERATED ... AS IDENTITY` columns
generate integer primary keys when omitted and recover their next value from
durable rows after executor restart,
`CREATE INDEX` and `CREATE UNIQUE INDEX` on the key/value compatibility fields or
multiple declared columns,
with `pg_catalog.pg_indexes` and common PostgreSQL system-catalog introspection
(`pg_namespace`, `pg_class`, `pg_type`, `pg_attribute`, `pg_constraint`, and `pg_index`),
and indexed equality lookup,
`SELECT * FROM t KEY 'k'`, `SELECT * FROM t LIMIT n`, `UPDATE`, `DELETE`,
predicate mutations such as `UPDATE t SET status = 'ready' WHERE id = '...'`
and `DELETE FROM t WHERE status = 'expired'`, with `RETURNING` rows for bulk
mutations,
named projections such as `SELECT payload, count FROM t WHERE id = '...'`,
`COPY t FROM stdin` (bulk path), `SELECT DISTINCT`, and `EXPLAIN <sql>` (planned access path),
`POST /v1/sql/explain` for plan without execution. Scalar builtins:
`gen_random_uuid()` (v4) and `now()` (unix seconds) evaluate in KEY/VALUE
positions; quoted literals are never evaluated.
Filtered scans: `SELECT * FROM t WHERE key = 'a' [AND key IN ('a', 'b')]
[AND key NOT IN ('c', 'd')] [AND score BETWEEN 10 AND 20]
[AND score NOT BETWEEN 30 AND 40] [AND key = 'a' OR key = 'b']
[AND state IS DISTINCT FROM NULL] [AND value NOT ILIKE 'hello%']
[AND value CONTAINS 'x'
AND value != 'y'] [ORDER BY key|value|column|json_path ASC|DESC] [LIMIT n] [OFFSET n]`.
Predicates evaluate on stored bytes as text; plain scans stream the ordered
range while filtered/ordered/paged scans page through storage until the
requested window is satisfied, with a 10k result cap unless an equality
predicate can use a maintained secondary index.
Aggregates: `SELECT COUNT(*) | COUNT(field) | SUM(field) | AVG(field) | MIN(field) | MAX(field)
FROM t [WHERE ...]` compute over all visible filtered rows; numerics parse
as f64 with non-numeric values skipped (`SUM` over none yields `0`,
`AVG`/`MIN`/`MAX` over none yield `null`). Aggregate names without parentheses
are treated as ordinary projected columns; named schema columns and JSON paths
are resolved from structured rows, and `COUNT(column)` excludes SQL `NULL`.
Grouped queries also support `HAVING` predicates over group columns and
aggregate aliases such as `COUNT(*) > 1`.
Key-equality joins: `SELECT * FROM a [LEFT|RIGHT|FULL] [OUTER] JOIN b ON KEY = KEY
[WHERE ...] [ORDER BY ...] [LIMIT n] [OFFSET n]` hash-joins on primary-key
bytes and returns rows shaped `{"left": ..., "right": ...}` (lossy UTF-8),
using JSON `null` for the unmatched side of an outer join. Other `ON` shapes
are rejected, never misexecuted.
Grouping: `SELECT <key|value|column|agg>, ... FROM t [WHERE ...] GROUP BY <key|value|column>
[ORDER BY ...] [LIMIT n] [OFFSET n]` groups exact key/value bytes and
structured column values (including JSON paths), and returns one row per group.
Bare select items must name the group field; aggregates reuse the scalar rules
above, and null values share one group.
Set reads: top-level `SELECT ... UNION [ALL] SELECT ...` combines compatible
table projections under one transaction snapshot. `UNION` removes duplicate
rows while `UNION ALL` preserves them; both branches must return the same
number of projected columns.

## Observability

`GET /metrics` JSON, `GET /metrics/prometheus` text, `EXPLAIN` plans,
slow-query warn above 5 ms, per-fingerprint work via `ryme-observe`
histograms, OTel-style trace spans (`TraceSpan`/`TraceCollector`) and
`query_fingerprint` normalization for grouping slow queries.

## Realtime primitives

- CDC: `GET /v1/stream?table=` (committed changes only).
- Broadcast: `POST /v1/broadcast` ephemeral per-channel fanout.
- Presence: `POST /v1/presence/join|leave`, `GET /v1/presence/:channel`
  with TTL expiry.
- Durable topics: `POST /v1/topics/append`, `GET /v1/topics/read` with
  per-partition cursors and bounded retention for resumable consumers.

## Vector and full-text search
- Vectors: `POST /v1/vector/upsert`, `POST /v1/vector/search` (exact,
  `top_k`, cosine over length-normalized values), `DELETE /v1/vector/:table/:id`.
  Exact scope: dim <= 4096, 100k vectors/space, in-memory per node,
  tenant-isolated namespaces.
- ANN: `POST /v1/vector/ann-search` (`top_k`, `ef`, default 64) over an HNSW
  graph (M=16, beam ef-build 64) maintained alongside exact storage; deletes
  are tombstoned. Spaces are hash-partitioned (`index_partitions`, default 4):
  writes route by id, searches fan out per partition and merge top-k, so ANN
  scales with partitions the way range shards scale tables. Recall is
  workload-dependent; the `ann_recall_against_exact` test pins >= 80% top-10
  overlap on synthetic 300x16 data. SDKs: `vectorAnnSearch` everywhere; CLI:
  `ryme vector ann-search <table> <v,...> [--top-k N] [--ef N]`.
- Full text: `POST /v1/text/index`, `POST /v1/text/search` (TF-IDF ranked),
  `DELETE /v1/text/:table/:id`. Lowercase alphanumeric tokenizer,
  100k docs/space. The npm/TypeScript, Java, and Rust SDKs expose the vector
  and text endpoints;
  CLI: `ryme vector ...`, `ryme text ...`.

## Application auth

Password register/verify, TOTP setup/verify, WebAuthn challenge, column
masking, and OIDC: `POST /v1/auth/oidc/login` builds the provider
authorization URL from `RYME_OIDC_*` env; `POST /v1/auth/oidc/token`
verifies an HS256 ID token (`iss`/`aud`/`exp`) and returns a principal
receipt. RS256/JWKS discovery is future work; configure one HMAC provider
per node. `POST /v1/auth/token` returns the existing API key plus an opaque
30-day refresh token. Send `{"grant_type":"refresh_token",
"refresh_token":"..."}` to rotate it; each refresh token is single-use,
stored only as a digest, and replaying a rotated token returns `401`. User
password hashes, OTP enrollment, passkey credentials, API-key digests, and
active refresh sessions are atomically persisted in `data_dir/auth.json`;
WebAuthn challenges remain intentionally ephemeral.
