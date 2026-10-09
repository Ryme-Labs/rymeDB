# Migration and compatibility

## Ledger

`POST /v1/migrate/apply` executes one SQL statement and records it in the
control-plane hash-chained ledger (`{id, sql, author, checksum,
parent_checksum, schema_version}`); duplicate ids get `409` before anything
runs twice, and `GET /v1/migrate/ledger` returns entries in apply order
with the schema version and chain validity. The ledger is process-local
like the rest of the control plane (branches, placement); snapshots and
archives cover data, not control metadata.

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
for snapshot plus updates. SDK helpers: JS `subscribeTable`/`subscribeQuery`,
Python `subscribe_table`/`subscribe_query`, Go `SubscribeTable`/`SubscribeQuery`.

Auth accepts Supabase-style JWT claims (`sub`, `tenant`, `roles`, `exp`)
where feasible; use separate issuers for app users versus operators.

Dump analysis: `POST /v1/migrate/supabase` with `{dump}` returns tables and
`CREATE POLICY` entries with detected tenant columns (`auth.uid() = <col>`
maps to `allow_table` RLS). CLI: `ryme migrate supabase dump.sql`.
`WITH CHECK`-only policies yield empty expressions and need manual review.

## Neon

Map branch JSON (`[{name, parent, lsn}]`) with `POST /v1/migrate/neon` into
`{id, parent, base_commit_ts}` plans (LSN `high/low` hex to u64), then create
branches and cut over. CLI: `ryme migrate neon branches.json`. Migrate data
at the logical level; internal page formats are never imported.

## SQL dialect

`CREATE TABLE` (including `IF NOT EXISTS`, column types, `PRIMARY KEY`, and
`NOT NULL` metadata), with PostgreSQL `information_schema.tables` and
`information_schema.columns` introspection, custom key/value `INSERT`, and PostgreSQL-style
`INSERT INTO table (...) VALUES (...)`; `ON CONFLICT` maps to upsert,
`SELECT * FROM t KEY 'k'`, `SELECT * FROM t LIMIT n`, `UPDATE`, `DELETE`,
`COPY t FROM stdin` (bulk path), `EXPLAIN <sql>` (planned access path),
`POST /v1/sql/explain` for plan without execution. Scalar builtins:
`gen_random_uuid()` (v4) and `now()` (unix seconds) evaluate in KEY/VALUE
positions; quoted literals are never evaluated.
Filtered scans: `SELECT * FROM t WHERE key = 'a' [AND value CONTAINS 'x'
AND value != 'y'] [ORDER BY key|value ASC|DESC] [LIMIT n] [OFFSET n]`.
Predicates evaluate on stored bytes as text; plain scans stream the ordered
range while filtered/ordered/paged scans evaluate the head 10k rows in key
order — correct within that stated bound, not a full-table engine.
Aggregates: `SELECT COUNT(*) | COUNT(field) | SUM | AVG | MIN | MAX (field)
FROM t [WHERE ...]` compute over the head 10k filtered rows; numerics parse
as f64 with non-numeric values skipped (`SUM` over none yields `0`,
`AVG`/`MIN`/`MAX` over none yield `null`). No `GROUP BY` in v1.
Key-equality joins: `SELECT * FROM a JOIN b ON KEY = KEY [WHERE ...]
[ORDER BY ...] [LIMIT n] [OFFSET n]` hash-joins on primary-key bytes and
returns rows shaped `{"left": ..., "right": ...}` (lossy UTF-8). Only
inner key-equality joins; other `ON` shapes are rejected, never
misexecuted.
Grouping: `SELECT <field|agg>, ... FROM t [WHERE ...] GROUP BY <field>
[ORDER BY ...] [LIMIT n] [OFFSET n]` groups exact key/value bytes and
returns one `{"label": result}` row per group. Bare select items must name
the group field; aggregates reuse the scalar rules above.

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
  100k docs/space. SDKs: JS/Python/Go/Rust `vectorUpsert`/`vectorSearch`/
  `textIndex`/`textSearch` (+ Swift/Kotlin/Dart/C# equivalents);
  CLI: `ryme vector ...`, `ryme text ...`.

## Application auth

Password register/verify, TOTP setup/verify, WebAuthn challenge, column
masking, and OIDC: `POST /v1/auth/oidc/login` builds the provider
authorization URL from `RYME_OIDC_*` env; `POST /v1/auth/oidc/token`
verifies an HS256 ID token (`iss`/`aud`/`exp`) and returns a principal
receipt. RS256/JWKS discovery is future work; configure one HMAC provider
per node.
