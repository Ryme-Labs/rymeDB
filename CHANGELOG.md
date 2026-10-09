# Changelog

All notable changes to rymeDB are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/); versions follow
[SemVer](https://semver.org/) once 1.0.0 ships.

## [0.1.0] — In development

### Added

- Java, npm, and Rust SDKs now expose matching JSON REST insert/upsert,
  filtered update, and filtered delete helpers.

- REST writes now accept ordinary JSON objects for Supabase-style inserts and
  filtered PATCH requests merge partial fields into existing JSON rows.
- REST inserts also accept top-level JSON arrays used by Supabase bulk
  inserts, alongside the existing `{rows: [...]}` compatibility shape.
- REST deletes now accept arbitrary PostgREST filters such as `id=eq.<id>`
  and remove every matching row across storage pages.
- PostgREST REST filters now support compound `or=(...)` and `and=(...)`
  groups with nested filter expressions.
- REST inserts support `on_conflict=<field>` to update an existing JSON row
  when a logical conflict field matches.
- REST ordering now supports multiple fields and PostgREST
  `nullsfirst`/`nullslast` modifiers.
- REST honors PostgREST `Prefer: return=minimal` mutations and emits exact
  `Content-Range` totals for `Prefer: count=exact` reads.
- PostgreSQL session compatibility now exposes common driver defaults for
  `server_version_num`, `default_transaction_isolation`, authorization, and
  identifier limits through `SHOW` and `current_setting`.
- PostgreSQL session commands now accept `READ COMMITTED`, `REPEATABLE READ`,
  and `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL`.

- PostgREST filtering now advances through ordered storage pages, so matching
  rows beyond the first page are not lost; non-key ordering scans the complete
  filtered result before sorting.

- Cluster CDC streams now use deterministic commit-derived cursors, so a
  reconnect through another live gateway resumes from the same sequence
  watermark instead of using that gateway's local realtime counter.

- PostgreSQL wire `COPY FROM STDIN` now accepts declared SQL column lists and
  typed text fields, including `\\N` nulls and omitted-column defaults, while
  retaining the key/value bulk-ingest path for schemaless tables.
- PostgreSQL wire transactions now support named `SAVEPOINT`, `RELEASE
  SAVEPOINT`, and `ROLLBACK TO SAVEPOINT` controls, restoring staged MVCC
  writes, read dependencies, and realtime change metadata at rollback.
- PostgreSQL `ALTER DEFAULT PRIVILEGES` declarations now persist table and
  sequence default ACLs and apply matching grants to subsequently created
  tables, views, and sequences, including schema snapshot restoration.
- PostgreSQL `pg_catalog.pg_default_acl` now exposes persisted default
  privilege owners, namespaces, object types, and ACL strings for migration
  and ORM introspection.
- PostgreSQL `CREATE ROLE`/`DROP ROLE` and `GRANT`/`REVOKE` declarations now
  persist role and table/schema/sequence privilege metadata through schema
  snapshots. `pg_roles` and `information_schema.table_privileges` expose the
  stored metadata for migration and ORM introspection.
- PostgreSQL sequences now support persisted `CREATE SEQUENCE`, `ALTER
  SEQUENCE` restart/increment options, `DROP SEQUENCE`, and `nextval`/`currval`/
  `setval` calls, with sequence state exposed through `pg_catalog.pg_sequences`
  and schema snapshots.
- PostgreSQL `CREATE VIEW`, `CREATE OR REPLACE VIEW`, and `DROP VIEW` now
  persist readable view definitions, execute common table/projection queries,
  restore through schema snapshots, and appear as `VIEW` relations in
  `information_schema.tables` and `pg_catalog.pg_class`.
- PostgreSQL `pg_catalog.pg_proc` and `pg_catalog.pg_trigger` now expose
  persisted function and trigger metadata for ORM and migration introspection.
- Common PostgreSQL `CREATE OR REPLACE FUNCTION` and `CREATE TRIGGER` schema
  declarations are now persisted with snapshots. `BEFORE` row triggers that
  assign `NEW.<column>` from `now()`, `CURRENT_TIMESTAMP`, or
  `clock_timestamp()` update inserted/updated rows and `RETURNING` values.
- PostgreSQL wire batching and Supabase dump analysis now preserve
  dollar-quoted function/procedure bodies, including semicolons inside `$$`
  blocks and tagged dollar quotes.
- PostgreSQL `pg_catalog.pg_policies` now exposes persisted policy names,
  commands, roles, and `USING`/`WITH CHECK` expressions for Supabase and ORM
  introspection.
- PostgreSQL `DROP POLICY [IF EXISTS] ... ON ...` now removes the named RLS
  policy and persists the remaining policy state across schema snapshots.
- Enabled RLS tables now default-deny reads and writes when no applicable
  policy command exists, including `FOR SELECT` policies that must not grant
  insert/update/delete access implicitly.
- Realtime CDC replay, live updates, and query snapshots now use the same
  persisted SQL RLS state as PostgreSQL reads, so migration-created policies
  protect WebSocket data as well as direct queries.
- Common Supabase `CREATE POLICY ... FOR ALL` declarations using
  `auth.uid() = tenant_column` now install tenant-scoped read/write checks in
  the SQL executor and persist them with schema snapshots. `ALTER TABLE ...
  ENABLE/DISABLE/FORCE ROW LEVEL SECURITY` controls activation, and
  command-specific policies keep separate read and write enforcement.
- PostgreSQL migration setup now accepts `CREATE SCHEMA IF NOT EXISTS` and
  `CREATE EXTENSION IF NOT EXISTS ... [WITH SCHEMA ...]` declarations.
- PostgreSQL aggregates now return protocol-level `NULL` for empty
  `SUM`, `AVG`, `MIN`, and `MAX` results, and scalar aggregate descriptions
  preserve their function labels.
- PostgreSQL table projections and `RETURNING` now preserve nullable
  schema/JSON fields as protocol-level `NULL` cells instead of empty strings.
- PostgreSQL column projections now preserve explicit `AS` aliases in core
  results and wire-level row descriptions.
- Scalar PostgreSQL reads without `FROM` now run through the shared SQL
  executor, including literals, built-in session values, casts, and bound
  placeholders across simple and extended wire requests, with protocol-level
  null handling for `SELECT NULL`.
- Top-level PostgreSQL `UNION`, `INTERSECT`, and `EXCEPT` queries now combine
  compatible table projections under one transaction snapshot with distinct
  and `ALL` multiset handling.
- PostgreSQL key-equality joins now support `LEFT`, `RIGHT`, and `FULL OUTER`
  semantics with explicit `NULL` values for unmatched rows.
- Grouped PostgreSQL queries now support `HAVING` predicates over group columns
  and aggregate aliases such as `COUNT(*)`.
- PostgreSQL extended-protocol `Describe` now returns parameter metadata for
  prepared statements and correctly distinguishes statement and portal names.
- The realtime benchmark opens subscriber WebSockets with bounded concurrency,
  making high-connection capacity runs practical without unbounded client fanout.
- PostgreSQL `ALTER COLUMN TYPE ... USING column::type` casts are accepted
  for safe self-column migration syntax.
- The realtime benchmark now reports sampled publisher-to-client fanout
  latency (`p50`, `p95`, `p99`, and `max`) alongside delivery and throughput.
- PostgreSQL prepared parameters now bind only real placeholders outside SQL
  literals and comments, handle multi-digit references, preserve scalar
  `NULL`/boolean/numeric values, and escape text safely.
- Basic PostgreSQL CTEs now rewrite one or more simple `WITH name AS (SELECT *
  FROM table [WHERE ...])` sources for outer selects, aggregates, grouping,
  and `INSERT ... SELECT` statements.
- PostgreSQL `ALTER TABLE ... DROP CONSTRAINT [IF EXISTS]` now removes named
  unique, check, foreign-key, and single-column primary-key constraints and
  persists their metadata across schema snapshots.
- PostgreSQL TRUNCATE options now support `RESTART IDENTITY`,
  `CONTINUE IDENTITY`, and foreign-key `CASCADE`/`RESTRICT` behavior.
- PostgreSQL DELETE ... USING now joins target and source rows by qualified
  columns, supports source-filtered deletes and RETURNING, and preserves
  transaction, foreign-key, RLS, and CDC handling.
- PostgreSQL UPDATE ... FROM now joins source rows by qualified columns,
  supports source-filtered updates and source-backed assignments, preserves
  transaction/constraint/RLS checks, and returns affected rows.
- PostgreSQL CREATE INDEX CONCURRENTLY and DROP INDEX CONCURRENTLY syntax is
  accepted for migration compatibility while using the engine's atomic index
  builder.
- SQL projections, predicates, ordering, grouping, and RETURNING now accept
  relation-qualified column references such as source.id.
- SQL table and secondary-index metadata now persists atomically in
  `data_dir/schema.json`, is shared by HTTP and PostgreSQL gateways, restored
  with rebuilt index entries, isolated for branch executors, and included in
  archive backups. Migration applications are serialized per server process
  so concurrent requests cannot execute duplicate or interleaved migrations.
- Branch schema snapshots now persist tenant-safely under `branch-schemas`,
  preserving branch-only tables and indexes across executor recreation while
  keeping branch DDL out of `main`; child branches inherit their parent
  schema snapshot, and archives preserve the nested branch metadata paths.
- Branch metadata persistence now fsyncs the replacement file, rebuilds
  manifest reference counts during recovery, rejects missing manifests or
  parents, and prevents deleting a branch that still has children.
- Live query streams retain the latest published commit per topic, so a
  broadcast-ring lag recovery resnapshot advances the client watermark rather
  than replaying from the connection's original commit.
- Raft recovery now fsyncs metadata replacements, fails closed on malformed
  metadata or log/apply payloads, and uses idempotent state-machine replay so
  leader-local commits are not applied twice.
- Backup manifests now validate their identity, paths, checksums, and duplicate
  entries; restores verify stored lengths and publish through an atomic staged
  directory, preventing partial destinations after a failed restore.
- Scheduled backup drills now materialize and clean up a temporary restore
  directory, exercising the real checksum, decryption, and atomic publish path
  instead of only reading and verifying archive objects.
- Realtime broadcast, change-stream replay, lag recovery, query updates, and
  snapshots now charge both the tenant message-rate and egress-byte buckets;
  heartbeat and ping/pong control frames remain exempt.
- SQL WHERE clauses now accept standard equality and range operators (`=`, `<>`,
  `!=`, `>`, `>=`, `<`, and `<=`) with numeric-aware comparisons, schema-column
  filters, JSON projection predicates, and `IS NULL`/`IS NOT NULL` checks.
- Filtered scans now paginate through storage pages until they collect the
  requested matches, preventing late matches from being hidden behind a full
  first page on large unindexed tables.
- SQL predicates now support `LIKE` and `ILIKE` wildcard matching with `%` and
  `_` patterns.
- Named and JSON-path indexes now preserve their indexed column metadata and
  enforce unique constraints across inserts, updates, and transactions.
- `CREATE TABLE` now recognizes column-level and table-level `PRIMARY KEY`
  constraints, including composite keys encoded as stable length-delimited
  storage keys.
- `ALTER TABLE ... ADD PRIMARY KEY` now validates existing rows, rekeys stored
  records for single and composite keys, and rebuilds secondary indexes.
- `ALTER TABLE ... ALTER COLUMN ... TYPE` now converts existing scalar, boolean,
  array, and JSON values, validates constraints, and rebuilds affected indexes.
- SQL updates can now relocate single and composite primary keys, refresh
  secondary indexes, and apply foreign-key `ON UPDATE` actions atomically.
- Duplicate `CREATE TABLE` statements now fail, while `CREATE TABLE IF NOT
  EXISTS` remains idempotent without replacing the existing schema.
- Duplicate `CREATE INDEX` statements now fail unless `IF NOT EXISTS` is used;
  `DROP INDEX [IF EXISTS]` removes durable index metadata and entries.
- Inline and single-column table-level `UNIQUE` constraints now create durable
  named indexes and reject duplicate inserts and updates.
- Composite `UNIQUE` constraints and composite `CREATE [UNIQUE] INDEX`
  definitions now encode column tuples, enforce uniqueness across writes and
  transactions, persist with schema snapshots, and track column renames.
- Column-level and table-level `CHECK` constraints now persist with schema
  snapshots and reject false rows across SQL writes, transactions, and bulk
  upserts while preserving PostgreSQL's NULL-as-unknown behavior.
- Column-level and table-level `FOREIGN KEY ... REFERENCES` constraints now
  persist with schema snapshots, validate inserts and updates, and protect
  referenced rows from deletes across transactional write paths.
- Foreign keys now support PostgreSQL-compatible `ON DELETE RESTRICT`,
  `ON DELETE CASCADE`, `ON DELETE SET NULL`, and `ON DELETE SET DEFAULT`
  behavior across point, filtered, and transactional deletes.
- Foreign keys now enforce `ON UPDATE RESTRICT`, `ON UPDATE CASCADE`,
  `ON UPDATE SET NULL`, and `ON UPDATE SET DEFAULT` when referenced unique
  keys change, including PostgreSQL constraint metadata for both actions.
- `ALTER TABLE ... ADD CONSTRAINT` now adds and validates `UNIQUE`, `CHECK`,
  and `FOREIGN KEY ... REFERENCES` constraints against existing and future rows.
- `ALTER TABLE ... ADD COLUMN` now backfills existing JSON rows from defaults,
  preserves nullability/identity metadata, and persists the updated schema.
- `ALTER TABLE ... DROP COLUMN` now removes the field from durable JSON rows,
  cleans dependent named indexes, and persists the updated schema.
- `ALTER TABLE ... RENAME COLUMN` now migrates stored JSON fields and keeps
  schema-column indexes and uniqueness checks aligned with the new name.
- `ALTER TABLE ... ADD/DROP COLUMN IF [NOT] EXISTS` now supports idempotent
  migration scripts.
- `ALTER TABLE ... ALTER COLUMN` now supports setting or dropping defaults and
  toggling `NOT NULL`, validating existing rows before tightening nullability.
- `DROP TABLE [IF EXISTS]` now removes durable rows, schema metadata, indexes,
  and table-local sequences.
- `TRUNCATE TABLE` now clears durable rows while preserving schema and indexes.
- SQL aggregate parsing now requires function-call parentheses, so columns named
  `count`, `sum`, `avg`, `min`, or `max` remain valid projections.
- `SELECT DISTINCT` now deduplicates projected rows before applying
  `LIMIT`/`OFFSET`.
- SQL predicates now support `IN (...)` and `NOT IN (...)` operand lists.
- SQL predicates now support inclusive `BETWEEN` and `NOT BETWEEN` ranges.
- SQL filters now support `OR` with normal SQL `AND` precedence.
- SQL predicates now support null-safe `IS DISTINCT FROM` comparisons.
- SQL predicates now support `NOT LIKE` and `NOT ILIKE` patterns.
- Aggregates, grouping, and key joins now page through all visible rows instead
  of truncating results at the first 10,000-row storage page.
- Scalar and grouped aggregates now resolve declared schema columns and JSON
  paths, excluding SQL `NULL` values from `COUNT(column)`.
- `GROUP BY` now accepts declared columns and JSON paths, preserving `NULL` as
  a shared group and projecting the grouped column in the result.
- `ORDER BY` now sorts schema columns and JSON paths in projections, scans, and
  grouped results alongside the existing key/value fields.
- Standard predicate mutations now support updating and deleting every visible
  row matching a `WHERE` clause in one transaction, including multi-row
  `RETURNING` results.
- PostgreSQL `INSERT ... ON CONFLICT DO NOTHING` now skips existing primary or
  unique conflicts without overwriting rows, including multi-row inserts and
  `RETURNING`.
- PostgreSQL conflict-target upserts now match declared primary or unique
  columns, apply `EXCLUDED.column` assignments, support targeted `DO NOTHING`,
  and return the updated row for single- and multi-row inserts.
- Conflict-target updates now support a `WHERE` predicate, including the
  common optimistic-version form that compares an existing column with an
  `EXCLUDED` value.
- SQL `UPDATE` and conflict-target assignments now support atomic numeric and
  text expressions, including `count = count + 1`, `count + EXCLUDED.count`,
  concatenation, `COALESCE`, `GREATEST`, and `LEAST`.
- PostgreSQL `INSERT INTO table DEFAULT VALUES` now materializes identity and
  column defaults through the normal constraint, transaction, and `RETURNING`
  paths.
- PostgreSQL `INSERT INTO target (...) SELECT ... FROM source` now copies
  projected, filtered rows atomically and supports `RETURNING` and the existing
  conflict-handling paths.
- Branch overlay pagination now reads parent pages from the requested cursor
  and merges only branch-local changes, avoiding a full-table scan on every
  `scan_after` request.
- The in-process Raft cluster now rejects malformed committed payloads and
  propagates replay failures instead of silently advancing its applied index.
- SQL schema rows now materialize declared array columns from `ARRAY[...]` and
  PostgreSQL brace literals as structured JSON arrays instead of opaque text.
- SQL JSONB projections now support `->` and `->>` object/array paths,
  including chained expressions.
- SQL `serial` and identity columns now generate omitted integer IDs and
  reinitialize their next value from durable rows after executor restart.
- Runtime latency metrics now expose p99.9 alongside p50/p90/p95/p99 and max
  in JSON and Prometheus formats.
- RLS-protected scans now paginate through unauthorized rows before applying
  query limits, so tenants receive the requested visible rows even when other
  tenants occupy earlier key ranges.
- Durable commits now append checksummed immutable `.sst` delta segments, while
  snapshots produce full sorted bases with sparse indexes and Bloom filters;
  startup merges the segment set when snapshots are unavailable, retention
  prunes safely around the newest base, compacts at 64 segments, and archive
  jobs include the artifacts.
- `cache_bytes` now bounds a decoded immutable-segment LRU for point reads;
  cache occupancy, hits, and misses are observable and entries are invalidated
  when segments are rewritten, compacted, or pruned.
- Added `storage_mode = "standard"`: durable managers can recover without
  materializing the full segment history, resolve point reads and scans from
  immutable segments, and evict committed rows from the resident engine.
- PITR restore now publishes the restored immutable base and truncates future
  snapshots and WAL records, so the restored state survives process restart.
- SDK distribution is limited to the Java, npm/TypeScript, and Rust clients;
  the release workflow packages and publishes those three artifacts and the
  Pages workflow builds their documentation.
- Transactional MVCC core with OCC, CRC segmented WAL and four durability
  modes (`strict`, `regional-fast`, `local-durable`, `memory`).
- Raft replication with joint-consensus membership, range sharding,
  three-tier QoS token-bucket admission and read-only follower mode.
- Gateways: PostgreSQL wire (simple + extended protocol, SSLRequest
  upgrade), RESP (strings, counters, hashes, lists, sets, expiry),
  native framed-JSON binary protocol, gRPC (`rymedb.v1.Ryme`: `Health`,
  `KvGet`/`KvPut`/`KvDelete`, `Scan`, `Sql` via tonic, `grpc_listen`),
  REST + Supabase-compatible surface,
  GraphQL, WebSocket realtime, HTTPS and dedicated TLS listeners, mTLS
  client-cert verification on the native listener.
- SQL KEY/VALUE dialect: CRUD, UPSERT, COPY bulk ingest, prepared
  statements, `gen_random_uuid()` / `now()` builtins, WHERE / ORDER BY /
  OFFSET scans, EXPLAIN with access paths.
- Secondary indexes: partitioned exact vector search, HNSW approximate
  search with recall tests, TF-IDF full-text search.
- Copy-on-write branching: create, list, reset, diff, promote/swap,
  delete with segment garbage accounting.
- Backups: PITR checkpoints, snapshots, local/S3 archive with envelope
  encryption and drillable `verify`.
- Retention: automatic MVCC version collection guarded by live-transaction
  snapshots, per-shard snapshot pruning (`archive.snapshot_keep`, default
  8), and WAL segment truncation below the oldest retained snapshot.
- Isolation: serializable-by-default optimistic concurrency with read-write
  and phantom validation, plus a per-connection `SNAPSHOT` opt-out via
  `SET TRANSACTION ISOLATION LEVEL` on the PostgreSQL wire protocol.
- RESP transactions: atomic `MULTI`/`EXEC`/`DISCARD` over a single
  serializable transaction with `EXECABORT` on queued unknown commands.
- RESP sorted sets: `ZADD` (`NX`/`XX`/`GT`/`LT`), `ZSCORE`, `ZRANK`,
  `ZREVRANK`, `ZRANGE` (`REV`/`WITHSCORES`), `ZREM`, `ZCARD`, `ZCOUNT`,
  `ZINCRBY` with score ordering and lexicographic member tiebreak.
- RESP iteration: cursor-based `SCAN` with `MATCH` globs and `COUNT`
  over MVCC snapshots, backed by `scan_after` range reads on every
  transaction backend.
- RESP streams: `XADD` (auto/explicit/sequence IDs, `MAXLEN`/`MINID`),
  `XRANGE`/`XREVRANGE` with `COUNT`, `XLEN`, `XTRIM`, blocking and
  non-blocking `XREAD`, `XDEL`, consumer groups (`XGROUP`,
  `XREADGROUP` with `BLOCK`, `XACK`, summary `XPENDING`, `XAUTOCLAIM`
  with delivery timestamps), and `XINFO` stream/group/consumer
  introspection.
- RESP blocking pops: `BLPOP`/`BRPOP`/`BLMOVE` woken by a commit bus,
  so waiters observe writes from every gateway and protocol.
- Committed CDC on every write path: SQL executor (`INSERT`/`UPSERT`/
  `UPDATE`/`DELETE`/bulk, covering PG wire, GraphQL and native SQL), RESP
  single/`MULTI`-`EXEC`/blocking/consumer-group commits, KV gateway TTL
  touches, and TTL-sweeper deletes; `INSERT` vs `UPDATE` from pre-write
  reads with blind multi-writes pre-reading their keys, and zero clone
  overhead for tables without subscribers.
- Placement ranges: key-range map with epoch-fenced `split`/`merge`
  (`ryme_router::Router::merge`, `ControlPlane::split_range`/`merge_ranges`),
  live at `GET /v1/ranges` (list + `?key=` lookup), `POST /v1/ranges/split`
  and `POST /v1/ranges/merge`; stale epochs get `409` with the current
  epoch. OpenAPI grows to 69 paths.
- Test and startup hardening: e2e/cluster suites bind-and-hold their ports
  instead of re-binding freed ones, and every server listener bind
  (`ryme_server::bind_retry`, used by `main`, internal TLS/native tasks and
  `serve_https`) retries `AddrInUse` briefly, removing the intermittent
  port-race failures under parallel load.
- SDK parity for placement ranges: `ranges`/`range_split`/`range_merge` in
  Python, JS, and Go SDKs with stub-based tests asserting method, path, and
  body, plus `ryme ranges <list|route|split|merge>` CLI commands verified
  against a live server including the stale-epoch 409 path.
- Automatic range splitting: per-range REST write-load counters
  (`Router::note_write`, shared atomics, serde-invisible, zeroed on
  split/merge) fed by KV put/delete, SQL writes and COPY row counts;
  `ControlPlane::auto_split_once` splits every range at or above the
  threshold at the key-space midpoint under the same epoch fencing as
  manual splits (children `{id}-a-{epoch}`/`{id}-b-{epoch}`, unsplittable
  or contended ranges skipped). Served at `POST /v1/ranges/autosplit`
  (`?min_writes=` override, `{"split": [...]}`), plus a config-gated
  background task (`autosplit_writes`, default 0 = fully off including
  accounting; `autosplit_interval_secs`, default 30). Merges stay manual.
  All four wire gateways feed the counters through a shared
  `RangeLoadHook`: RESP notes committed staged keys on the single-command,
  `EXEC` and blocking paths; PG notes successful write statements by
  table; native notes put/delete by key and SQL writes by table; gRPC
  notes `KvPut`/`KvDelete` by key and SQL writes by table — each behind
  `with_range_hook`, off by default with zero overhead.   The trigger is
  covered in Python/JS/Go/Rust SDKs (`range_autosplit`), thin Swift/Kotlin/
  Dart/C# clients, `ryme ranges autosplit [min-writes]`, and a "Split hot"
  button on the dashboard observe tab (wiring test extended). Covered by router unit
  tests, control midpoint/candidate tests, per-gateway hook tests, and
  live e2es where REST PUTs and RESP SETs/INCRs together push `range-0`
  over the threshold into two epoch-1 children, plus a loop e2e that
  writes with a 1s interval and polls placement until the background
  task splits without any trigger call. A zero threshold (default-off
  config, or explicit `?min_writes=0`) splits nothing, so the trigger
  endpoint can never surprise-split a quiet cluster; pinned by e2e.
  OpenAPI grows
  to 72 paths.
- Visible range write load: `GET /v1/ranges/loads` serves the per-range
  `{id, epoch, writes}` counters behind auto-split (any credential;
  zeroed on split/merge), covered in Python/JS/Go/Rust SDKs
  (`range_loads`), thin Swift/Kotlin/Dart/C# clients,
  `ryme ranges loads`, and a "Loads" dashboard button. OpenAPI grows
  to 73 paths.
- Dashboard overview shows range write loads next to placement ranges
  (`loads: count=` with per-range `writes=`), stub-tested including the
  error path.
- Auth-before-parse on range split/merge: the handlers now read the raw
  body and authenticate first, so unauthenticated callers always get
  `401` (previously axum's JSON extractor returned `422` before auth
  ran) and malformed bodies from authenticated callers get `400`, matching
  the documented OpenAPI responses. Covered by a live e2e asserting
  `401` on all six ranges routes without credentials and `400` on
  malformed split/merge bodies with credentials.
- Branch detail now requires authentication: `GET /v1/branches/:id`
  returned branch manifests without credentials while the list endpoint
  required them (and the spec documents `401`/`403`). One-line fix plus
  live e2e asserting `401` unauthenticated and `200` authenticated.
- Uniform malformed-body handling: every JSON endpoint now parses through
  one `json_body` helper (`400 {"error":"body"}`), matching the
  21 spec paths that document `400` (nothing documents axum's default
  `422`, which is what all 32 handlers returned before). The shared
  extractor keeps handler bodies untouched and preserves each handler's
  auth order. Covered by a live e2e posting truncated JSON to 11
  endpoints (SQL, COPY, EXPLAIN, GraphQL, branches, QoS tier, REST
  insert, vector, topics, broadcast, Supabase import).
- Autosplit CLI flags and config docs: `--autosplit-writes` and
  `--autosplit-interval-secs` (verified in `--help`), plus
  `configuration.md` rows for both fields and the previously
  undocumented `otel` object.
- Strict config files: all five `ryme-config` structs now use
  `deny_unknown_fields`, so a typo like `"autosplit_write"` fails fast
  at startup instead of silently running with defaults. Covered by a
  unit test asserting top-level and nested typos are rejected naming the
  field, and that a serialized default config round-trips.
- Slow-log table filter: `GET /v1/observe/slow?table=<name>` narrows the
  newest-first feed to one table (filter-then-take via
  `SlowLog::recent_filtered`), with the `table` query param in OpenAPI
  and support in all eight SDKs, `ryme observe slow [--table T]`, and a
  table-filter input on the dashboard observe tab. Covered by an observe
  unit test (ordering, limit, missing table) and a live e2e asserting the
  filtered shape.
- Trace span filters: `GET /v1/traces` accepts `?name=` (exact operation
  name) and `?table=` (span table attribute) via
  `TraceCollector::recent_filtered`, with params in OpenAPI and support
  in all eight SDKs, `ryme traces [--name OP] [--table T]`, and a
  table-filter input on the dashboard observe tab. Covered by an observe
  unit test (name, table, combined, limits) and a live e2e asserting
  filtered/non-empty plus missing-filter/empty results.
- Tenant-isolated realtime: presence rooms, broadcast channels, and
  durable partitions were keyed by bare name, so any tenant could read
  another tenant's rooms, channels, and partitions. All eight entry
  points now scope keys as `{tenant}/{name}` from the caller's
  principal, with per-tenant durable cursors. Covered by a realtime
  unit test and a live two-tenant e2e (JWTs minted in-test) asserting
  cross-tenant invisibility plus independent cursor zero-starts.
- Full-retention stream replay: resume previously capped replay at 1000
  records per connect even when the history ring retained more, silently
  truncating large gaps. `replay` now clamps to the ring bound (100000)
  and the server passes the live ring capacity, so retention is the
  single honest bound; docs updated to say so.
- Dashboard broadcast card: the web console could post to topics but had
  no broadcast surface. New card with channel + payload-JSON inputs (Post),
  Watch/Stop live subscription, and its own output pane, covered by the
  wiring test (buttons, paths, inputs).
- Dashboard presence card: presence was the last realtime surface without
  console coverage. New card with channel/member inputs (Join/List) and
  its own output pane, covered by the wiring test.
- Presence expiry is now enforced everywhere, not just on list: joining
  prunes the room's dead members first (so the returned member count is
  exact), and the TTL sweep loop calls a new `Realtime::prune_presence`
  that drops expired members across all rooms including abandoned ones.
  Covered by a realtime unit test and a live e2e (two 1s-TTL joins,
  2.5s sleep, third join reports exactly one member).
- Flake-hardened cluster membership e2e: the add/remove/transfer/replace
  calls pinned the leader discovered minutes earlier, so a leadership
  change mid-test surfaced as `503 not leader` (correct server behavior).
  A `send_to_leader` helper now rediscovers the leader and retries past
  503s within the existing 20s deadline style.
- Negative membership coverage: removing an unknown member (`404`),
  replacing with an empty set (`400`), adding a duplicate id (`400`),
  and removing the leader itself (`400 transfer first`) are now pinned
  by a live three-node e2e.
- Closed a password timing oracle: unknown users (or users without a
  password) failed fast while real users burned 210K hashes, letting
  attackers enumerate valid ids by response time. `verify_password` now
  always runs the full hash against a fixed dummy entry for misses, so
  all rejections cost the same; covered by a unit test flooring both
  miss paths at full hash cost.
- API key issuance: password/OTP/OIDC verification returned receipts
  only — no endpoint could mint a usable credential, so password auth
  was a dead end. New `POST /v1/auth/token` verifies id/password (plus
  the TOTP code when the account enrolled one) and issues a `ryme_`-prefixed
  key carrying the account tenant and roles. Covered by a live e2e
  (register → token → key works on a protected route; wrong password
  `401`; OTP enrollment enforced; correct TOTP mints), OpenAPI schema,
  `auth_token` clients in Python/JS/Go/Rust SDKs with stub tests, and
  `ryme auth token <id> <password> [--code C]`.
- Key revocation: minted keys could never be revoked. New
  `DELETE /v1/auth/keys` (key in the JSON body so it never hits access
  logs; write-capable principal; `404` for unknown keys), covered by a
  unit test and a live e2e (mint → use → revoke → `401`), OpenAPI
  schema, `auth_revoke` clients in Python/JS/Go/Rust SDKs with stub
  tests, `authRevoke` in the thin Swift/Kotlin/Dart/C# clients, and
  `ryme auth revoke <key>`.
- OIDC login now issues keys too: `POST /v1/auth/oidc/token` returned a
  receipt only, leaving OIDC a second dead end. It now mints a
  `ryme_`-prefixed key for the verified identity (tenant/roles from
  claims) via the same shared issuer, with the OpenAPI response updated
  to `201`. Covered by extending the OIDC e2e with an in-test HS256
  token whose key then works on a protected route.
- Real WebAuthn passkeys: challenge issuance existed with no way to
  enroll or verify (the registry stored bare ids, no keys). New
  `POST /v1/auth/passkey/register` (admin-enrolled P-256 credentials,
  on-curve validated) and `POST /v1/auth/passkey/verify` checking
  rpId hash, user presence, origin allow-list
  (`passkey_rp_id`/`passkey_origins` config), 5-minute one-shot
  challenges, sign-count monotonicity, and ECDSA over
  authenticatorData plus the client-data hash — then minting an API key
  for the bound password user. No attestation verification in v1,
  documented as such. Covered by registry unit tests (valid, replay,
  stale, wrong origin/rp/counter) and a live e2e running the full
  ceremony with an in-test keypair, plus OpenAPI paths and schemas.
- Passkey client parity: `passkey_register`/`passkey_verify` clients in
  Python/JS/Go/Rust SDKs (stub-tested) and thin Swift/Kotlin/Dart/C#
  clients, plus `ryme auth passkey-register` and
  `ryme auth passkey-verify`. Browser ceremony itself stays in
  authenticatorland; the dashboard console intentionally gains no
  passkey buttons (raw blob pasting is worse UX, not better).
- Token parity follow-up: `authToken` in the thin Swift/Kotlin/Dart/C#
  clients (each file's optional-param idiom) and a Token button on the
  dashboard auth card, covered by the console wiring test.
- Wired migration ledger: `ryme-migrate::Ledger` (hash-chained entries,
  duplicate and chain verification) existed with no server surface.
  New `POST /v1/migrate/apply` (executes one SQL statement, then records
  `{id, sql, author}`; duplicate ids get `409` pre-execution) and
  `GET /v1/migrate/ledger` (entries, version, validity), backed by a
  `migrations` field on `ControlPlane` with the same process-local
  durability as branches and placement. Covered by a live e2e (apply
  executes the row into place, ledger versions, dup `409`, bad SQL
  `400`), OpenAPI paths and schemas, `migrate_apply`/`migrate_ledger`
  clients in all eight SDKs with stub tests, and
  `ryme migrate apply <id> <sql>` / `ryme migrate ledger`.
- Migration apply semantics hardened: read-only statements are rejected
  with `400` (a migration that writes nothing is meaningless), and SQL
  execution failures now surface their real error instead of a generic
  `500`, covered by e2e.
- Password hardening: no maximum length meant a 2MB password burned
  ~2MB × 210K hashes per request, and both register and verify hashed
  synchronously on tokio workers (a few concurrent guesses could stall
  the executor). Passwords are now capped at 256 bytes (`400` beyond,
  enforced identically for all ids so the timing fix holds), and both
  handlers run hashing on `spawn_blocking` threads. Covered by unit
  asserts plus live e2e (`400` on oversized register/verify bodies,
  normal register/verify still `201`/`200`).
- Broadcast subscribe endpoint: `POST /v1/broadcast` was fire-and-forget
  with no read path anywhere (`broadcast_subscribe` had only unit-test
  callers). New `GET /v1/broadcast/:channel` WebSocket endpoint
  (tenant-scoped, header or `?api_key=` auth, same lag-skip semantics as
  the change feed), covered by a live e2e and OpenAPI. Subscribe parity
  in Python (`subscribe_broadcast`), JS (`subscribeBroadcast` +
  `BroadcastRecord`), Go (`SubscribeBroadcast` + `BroadcastMessage`),
  and `ryme stream broadcast <channel> [--count N]` (plus a `?`/`&`
  api_key separator fix shared with all stream commands), verified live
  against a real server.
- Resumable change streams: `Realtime` keeps a bounded per-topic history
  ring beside the broadcast channel with `replay(tenant, db, table,
  since_commit_ts)` (oldest-first, paginated), and
  `GET /v1/stream?table=&from=<commit>` replays retained changes after
  the watermark then continues live, suppressing redelivery by sequence
  number. Without `from` the socket stays quiet until the next commit
  (unchanged legacy behavior); `commit_ts` is a strictly increasing
  server counter so watermarks are exact. Covered by a realtime unit
  test and a live e2e (live r2, disconnect, missed r3 replayed on resume
  from C2, live r4 continues), plus `from` support in the Python/JS/Go
  subscribe helpers. The docs spell out the one honest limit: RESP only
  publishes while subscribed, so RESP-only writes made with zero
  subscribers are absent from replay.
- CLI spec cross-check: `apps/cli/src/cli.test.ts` asserts every hand-written
  CLI path (string and template literals) exists in `schemas/openapi/rest.yaml`
  and pins recent observability/placement endpoints; wired as
  `pnpm --dir apps/cli test` mirroring the dashboard suite.
- Queryable slow-log: `ryme_observe::SlowLog` ring (256 entries, newest-first)
  fed by the `kv_put`/`sql`/`sql_copy` slow paths with kind, fingerprint,
  table, micros and timestamp — fingerprints only, never raw statements —
  served at `GET /v1/observe/slow?limit=` (authenticated) with
  `slow_log` clients in Python/JS/Go/Rust SDKs and `ryme observe slow`.
  Verified live: a 2000-row COPY lands as `COPY bulk` at 30ms. OpenAPI
  grows to 70 paths.
- Live request tracing: `TraceCollector`/`TraceSpan` graduate from unit-tested
  shelfware to production — `SharedState.traces` fed by the `rest_list`,
  `kv_get`, `kv_put`, `sql_exec` and `sql_copy` handlers with table
  attributes, served at authenticated `GET /v1/traces?limit=`. OpenAPI grows
  to 71 paths.
- OTLP span export: `ryme_observe::otlp_resource_spans` encodes drained spans
  as OTLP/HTTP+JSON with 32/16-hex ids, string nanos and service resource;
  `ryme_server::export_traces_once` POSTs batches to `{otel.endpoint}/v1/traces`
  (empty batches send nothing, failures drop with a warning), driven by a
  background task gated on `otel.endpoint`/`otel.interval_secs` (defaults:
  off, service `rymedb`, 30s). Covered by encoder goldens, drain tests and a
  live export test against a stub receiver.
- Traces parity: `traces(limit?)` clients in Python/JS/Go/Rust SDKs,
  `ryme traces [--limit N]` CLI (verified live: empty log, then a `kv_put`
  span with table attributes after a PUT), and dashboard overview coverage
  with graceful degradation.
- Benchmark re-validation after the wire-gateway QoS turn: set/get/mixed/tx
  re-measured at `dedicated_cluster` tier (required precondition now that
  every gateway enforces QoS — at `shared`, writes throttle to ~1k qps),
  all at or better than the published tables, so no regression from the
  CDC/QoS/tracing additions. Valkey tx gap re-checked at ~1.5x p50 / ~1.9x
  p99, matching the documented honest status.
- QoS and metering on the wire gateways: RESP and PostgreSQL gateways take
  the shared `QosRegistry`/`MeterRegistry` (opt-in, standalone use stays
  unlimited) and admit every command — RESP classified by staged writes,
  PG by `Statement::is_write` across simple, extended and `EXECUTE` paths —
  with throttle surfacing as `-ERR ... quota` (RESP) and `53400` (PG).
- Gateway observability on the wire protocols: RESP and PostgreSQL gateways
  share the node latency window, histogram and slow-log (opt-in
  `with_observe`, standalone use unaffected). RESP records per command and
  per `EXEC` batch; PG records per statement with normalized fingerprints.
  Covered by live observe tests on both gateways.
- Native gateway metering, timing and a QoS classification fix: every op
  records read/write units plus latency/histogram/slow observations, and the
  `sql` op now admits writes from the write bucket (previously every SQL
  statement drew from the read bucket). Covered by a live native test.
- QoS storage accounting that can't leak: `QosRegistry::set_stored_bytes`
  plus `Backend::stored_bytes_by_tenant` (all four backends) reconciled from
  real engine bytes on the sweep loop, so the admitted-write accumulator is
  periodically reset to actual retained bytes instead of growing
  monotonically toward the cap. Covered by a registry unit test and a
  `qos_bytes` backend test.
- Dashboard overview covers placement ranges and the slow-log: range count
  with id/epoch/leader lines and newest slow entries with kind/fingerprint/
  micros, each degrading to a `warn:` line when its endpoint fails.
- Dashboard web console observes placement and performance: new `observe`
  tab with range list/route-by-key, recent slow-log and recent trace spans
  (all `GET`s against documented OpenAPI paths), served live at
  `/dashboard` and covered by a button-wiring test.
- Rust SDK parity for backups, shards, and ranges: `checkpoint`,
  `snapshot`, `latest_checkpoint`, `pitr`, `restore`, `archive`,
  `archives`, `backup_copy`, `shard_layout`, `shard_move`, `ranges`,
  `range_split`, `range_merge` (plus a bodiless-`POST` helper), covered by
  two stub-server tests asserting method, path, and body.
- Thin SDK parity for backups, shards, ranges, slow-log and traces: the
  same fifteen-method surface (`backup_copy`, `checkpoint`, `snapshot`,
  `latest_checkpoint`, `pitr`, `restore`, `archive`, `archives`,
  `shard_layout`, `shard_move`, `ranges`, `range_split`, `range_merge`,
  `slow_log`, `traces`) added to Swift, Kotlin, Dart and C# clients
  following each SDK's conventions; verified by review plus a mechanical
  path/body-key cross-check against the OpenAPI schemas (no toolchains
  available in this environment for those four languages).
- Cross-region archive copies: `archive_replica` target plus
  `POST /v1/backups/copy` (`ryme backup copy`) with SHA-256 verification
  on source reads and replica read-backs.
- PostgreSQL wire: quote-aware multi-statement simple queries, SQL-level
  `PREPARE`/`EXECUTE`/`DEALLOCATE`, and the missing ErrorResponse
  terminator that broke real clients on every error; `psql_smoke`
  verifies the gateway through real `psql`.
- Prepared-statement AST cache on the PG extended protocol: bound portals
  keep their parsed `Statement` keyed by bound text, so repeated `Execute`
  skips bind+parse with zero behavior change (stale plans impossible by
  construction); covered by raw-TCP repeat-execution, error-stability, and
  invalidation tests (re-`Bind`, portal `Close`, statement `Close`, and
  re-`PREPARE` under the same name serving the new plan), and `psql_smoke`
  still green.
- SQL-level `EXECUTE` uses the same bound-text-keyed AST cache (with
  eviction on `PREPARE` overwrite, `DEALLOCATE`, protocol `Parse`/`Close`),
  proven by a raw-TCP test covering repeat execution, parameter keying,
  re-`PREPARE` invalidation and post-`DEALLOCATE` 26000 errors.
- Sharded transactions: multi-shard 2PC with validate-all-participants,
  fsynced coordinator decisions, idempotent crash recovery, and
  coordinator log retention; table moves now serialize against commits.
- RESP HyperLogLog: `PFADD`/`PFCOUNT`/`PFMERGE` with p=14 registers and
  small-range correction.
- RESP geospatial: `GEOADD`/`GEODIST`/`GEOPOS`/`GEOHASH`/`GEOSEARCH`
  plus legacy `GEORADIUS`/`GEORADIUSBYMEMBER`, all verified digit for
  digit against Redis (earth radius, integer scores, geohash strings,
  `STORE` round-tripping).
- RESP parity harness: `redis_parity.rs` diffs ~100 commands against a
  live Redis; fixes from it include nil-array shapes for empty reads
  and blocking timeouts, upfront `XDEL` ID validation, quantized
  coordinates at `GEOADD` time, `%.4f` display distances in the query
  unit, index-order default sorting, and exact Redis value-error
  strings. `valkey_cli_smoke` covers the gateway through real
  `valkey-cli`.
- RESP connection management: `HELLO 2` negotiates RESP2 with a server map,
  `HELLO 3` is honestly refused with `NOPROTO`, plus per-connection
  `CLIENT SETNAME`/`GETNAME`, server-wide `CLIENT ID`, accepted
  `CLIENT SETINFO`, `ECHO`, and passwordless-exact `AUTH` errors — all
  QoS/metered like other commands. Real `redis-py` 8 connects and drives
  `ping`/`set`/`get`/client commands (with `protocol=2`); previously it
  died on the `CLIENT SETINFO` handshake with "unknown command".
- Auth: API keys, HMAC JWT with issuer/audience separation, passwords,
  TOTP, passkeys, OIDC (HS256), RBAC/RLS, column masking.
- Realtime: committed CDC, broadcast, presence with TTL, durable topics
  with cursors; query-stream snapshots.
- Platform: metering with idempotent ingest, billing summary, micro-USD
  invoices, autoscale advice, migration ledger, Supabase/Neon analyzers,
  OTel-style traces, query fingerprints, Prometheus exposition.
- SDKs: JavaScript/TypeScript, Python, Go, Rust, Swift, Kotlin, Dart, C#.
- Operator surfaces: `ryme` CLI, web admin console at `GET /dashboard`,
  Helm chart, plain K8s manifests, branch-per-PR preview CI.
- Correctness: Jepsen-style transfer invariant, chaos/restart/SIGKILL
  suites, fault-injection membership tests.
- Benchmarks: `ryme-bench` with p50/p90/p95/p99/p99.9 histograms and an
  honest performance report in `docs/performance.md`.
- gRPC gateway: new `ryme-wire-grpc` crate (`proto/ryme.proto`,
  `rymedb.v1.Ryme` with `Health`, `KvGet`, `KvPut`, `KvDelete`, `Scan`,
  `Sql`) built on tonic with Bearer API-key auth, per-RPC QoS/metering and
  latency/histogram/slow observability mirroring the native gateway, wired
  behind `grpc_listen` / `--grpc-listen` (plaintext loopback/dev only in
  v1, `grpc_tls_listen` serves the same API over TLS from the node cert/key);
  verified by tonic roundtrip tests plus a real Python grpcio client
  against the live server (health/put/get/scan/sql + auth rejection), and a
  TLS roundtrip with standard CA verification plus a live grpcio-over-TLS
  check through the server binary.
- Wire-gateway spans in traces: RESP, PG, native and gRPC gateways now push
  spans into the shared collector (previously REST-only), so `/v1/traces`
  and OTLP export cover all protocols; gRPC honors W3C `traceparent`
  metadata for parent linkage via `TraceSpan::linked` (malformed headers
  rejected, verified by unit tests and a live linkage test).
- Native gateway bind-and-hold in tests and server: new
  `ryme_server::serve_with_native` accepts a pre-bound native `TcpListener`
  so the extras auth/presence/topics e2e no longer races on a freed port;
  the native roundtrip helper retries connects and responses to a deadline,
  removing the intermittent `early eof` failure under parallel load.
- Optional gateway bind-and-hold: new `ryme_server::OptionalListeners`
  plus `serve_with_optional` carry pre-bound `TcpListener`s for native,
  RESP-TLS, native-TLS, gRPC, gRPC-TLS and HTTPS, with
  `serve_https_with_listener` for the HTTPS path; the RESP-TLS e2e now
  holds its port and retries the TLS handshake to a deadline instead of
  racing on a freed port.
- Chaos e2e bind-and-hold: `chaos_e2e` binds PG/RESP/HTTP listeners before
  spawning the server and passes them into `serve`, removing the last
  in-process `free_port` race for core gateways.
- Binary-spawn port allocation: `crash` and `cluster_bin` e2e hold all
  chosen listeners simultaneously before releasing them to the child
  `ryme-server` processes, guaranteeing distinct ports within each test
  and narrowing the cross-test steal window.
- Gateway task handles: `spawn_gateways` now returns named `GatewayTasks`
  instead of an order-dependent `Vec`, so `serve_state` joins PG/RESP/HTTP
  plus archive by name; adding or reordering background gateways can no
  longer misroute shutdown joins.
- gRPC admission caps: new `serve_with_incoming_limited` and
  `serve_tls_limited` cap per-connection concurrency by `max_connections`
  (standalone defaults stay 1024); `ryme-server` now passes its
  `max_connections` into both gRPC listeners like PG/RESP/native/HTTPS.
- HTTP admission caps: new `serve_http_with_listener` serves the plaintext
  REST API through the same semaphore-gated hyper accept loop as HTTPS,
  so excess TCP connections are closed immediately instead of growing
  without bound; covered by `http_admit` e2e. Both HTTP and HTTPS loops
  now use `serve_connection_with_upgrades`, fixing WebSocket streams
  (`/v1/stream`, `/v1/query-stream`, broadcast) over the capped
  listeners.
- Explicit HTTP body cap: the router now sets `DefaultBodyLimit::max`
  (2 MiB), so the manual hyper accept loop enforces the same oversize
  rejection as `axum::serve`; covered by `http_body_limit_rejects_oversize`
  asserting `413` on a 3 MiB SQL body.
- Bounded scan pagination: `GET /rest/v1/:table` clamps `offset` to 10k
  with saturating row accounting, and `GET /v1/scan/:table` clamps
  `limit` to 1..1000 like the REST surface; covered by
  `e2e_scan_pagination_bounds` asserting huge offsets return `[]` and
  huge scan limits stay capped.
- Bounded query-stream snapshots: `GET /v1/query-stream` clamps `limit`
  to 1..1000 (was 10000) and the realtime backstop matches, cutting
  worst-case per-subscriber snapshot and per-write refresh scans 10x;
  covered by `live_query_snapshot_limit_capped` seeding 1200 rows and
  asserting a 999999-limit snapshot returns exactly 1000.
- Bounded presence rooms: at most 1000 members per room with re-join of
  an existing member always succeeding and overflow rejected as `429`
  overload; TTL clamped to 1s..24h so `u64::MAX` can no longer pin
  immortal members; covered by `presence_room_cap_and_ttl_clamp`.
- Idle realtime scope reclamation: new `Realtime::prune_idle` drops
  receiverless change-feed senders with empty history, receiverless
  broadcast channels, and receiverless query topics on the sweep loop;
  scopes with replayable history are kept; covered by
  `prune_idle_drops_subscriberless_empty_scopes`.
- Auto-split range ceiling: the background loop stops at 256 ranges
  (`AUTO_SPLIT_MAX_RANGES`); manual split/merge are unchanged, so
  operators keep the scale path while a hot keyspace cannot fragment
  metadata exponentially; covered by `auto_split_stops_at_max_ranges`.
- Central KV shape policy: `Gateway::put_with_ttl` enforces non-empty
  table, 1..1024-byte keys, and 4 MiB values (`MAX_KEY_BYTES`,
  `MAX_VALUE_BYTES`) for every Gateway caller at once, and the REST
  handler shares the same constants; covered by gateway
  `put_rejects_shape_violations` / `put_accepts_boundary_shapes`.
- gRPC error-code consistency: `status_of` maps `InvalidArgument` to
  `InvalidArgument` and `NotFound` to `NotFound` instead of `Internal`,
  matching the inline pre-checks; write-shape violations keep the
  established `ok:false` reply contract, pinned by
  `grpc_shape_errors_surface`.
- Branch id validation: `create_root`/`create_child` reject empty,
  over-128-byte, or `/`/`..`-containing ids as `InvalidArgument`,
  mirroring the backup-id sanitization precedent; covered by
  `child_rejects_bad_ids`.
- Read-only follower enforcement on every gateway: the PG wire executor
  now inherits `read_only` (previously writes over PG were accepted on
  followers) and the RESP gateway gains `with_read_only` with `READONLY`
  rejection at all five commit funnels (single, EXEC, blocking pops,
  moves, consumer groups); covered by RESP `read_only_rejects_writes_allows_reads`
  and follower e2e PG/RESP write denials.
- Read-only SQL on native/gRPC: per-request executors built in
  `op_sql`/gRPC-`sql` now inherit the gateway flag via
  `Gateway::is_read_only` (previously SQL writes over both protocols
  bypassed follower protection); covered by
  `grpc_read_only_rejects_sql_writes` and follower e2e native
  put/SQL denials.
- Dedicated read-only error: new `RymeError::ReadOnly` (was `Unavailable`)
  surfaces as `503` over REST, `25006` over the PG wire, and
  `unavailable` over gRPC; covered by `error_codes_map` and a live
  follower e2e asserting the `25006` frame.
- Backup metadata now requires authentication: `GET /v1/backups/latest`
  and `GET /v1/backups/pitr` returned checkpoint manifests without
  credentials while sibling backup routes required them; covered by e2e
  asserting `401` unauthenticated alongside the existing `200`
  authenticated path.
- Tenant-scoped control operations: `POST /v1/qos/tier` and
  `POST /v1/shards/move` now reject cross-tenant targets with `403`,
  closing a noisy-neighbor vector where any writer could retune or
  relocate another tenant's capacity; covered by chaos and sharded e2e
  cross-tenant denials.
- Operator-only cluster membership: add/remove/transfer/replace now
  require `Owner`/`Admin` (`can_admin`) instead of any writer, so
  application keys cannot eject nodes or join a member that would receive
  replicated data; covered by `membership_rejects_non_admin_mutations`
  and documented in `docs/cluster-operations.md`.
- Operator-only topology and capacity: QoS tier changes now require
  `Owner`/`Admin` of the caller's own tenant, and shard moves plus
  range split/merge/autosplit triggers require `Owner`/`Admin`, so
  application keys cannot grab capacity or reshape placement;
  covered by chaos, sharded, and ranges e2e non-admin denials.
- Operator-only restore and role grants: `POST /v1/backups/restore`
  requires `Owner`/`Admin` (previously any writer could rewind the whole
  node), and `POST /v1/auth/register` rejects `owner`/`admin` grants from
  non-admin callers (previously any writer could mint owners);
  covered by `extras_auth_privilege_boundaries`.
- Account-takeover fix: `register_password` returned `Ok` while
  overwriting an existing id's password and tenant, so any writer could
  seize any account; re-registration is now `409 Conflict` with the
  stored credential untouched, and non-admin callers can only register
  into their own tenant; covered by `reregister_never_takes_over` and
  extended privilege-boundary e2e, with the `409` documented in
  `schemas/openapi/rest.yaml`.
- Key-revocation ownership: `DELETE /v1/auth/keys` accepted any key from
  any writer, so a compromised app key could revoke operator keys and
  lock out administration; revocation now requires admin rights or key
  ownership (self-service preserved) via `ApiKeyStore::owner_of`;
  covered by extended privilege-boundary e2e.
- Passkey-enrollment ownership: `POST /v1/auth/passkey/register`
  accepted any user from any writer, so an attacker could bind their own
  key to a victim id and mint that victim's API keys; enrollment now
  requires admin rights or a matching caller id, and duplicate
  credential ids are `409` instead of silent overwrite; covered by
  registry unit asserts and extended privilege-boundary e2e.
- OTP-enrollment ownership: `POST /v1/auth/otp/setup` generated and
  returned a fresh secret for any user id to any writer, hijacking the
  victim's second factor (and locking them out); now admin-or-self like
  passkeys; covered in the privilege-boundary e2e.
- Strict role names on register: unknown `roles` entries now fail with
  `400` instead of silently degrading to the default, so typos like
  `"owenr"` cannot grant unintended access levels; covered in the
  privilege-boundary e2e.
- Complete REST admission: reads (`GET`, scan, GraphQL, SQL reads, TTL),
  deletes, and SQL writes now draw from the read/write buckets, and every
  materialized response charges its bytes to the egress bucket (`429` on
  exhaustion); previously large-read exfiltration loops ran unthrottled
  at shared tier. Covered by `e2e_read_egress_throttled` and the
  `admit_egress` bucket unit test.
- Wire-protocol response egress: RESP charges reply bytes at the socket
  loop and native charges framed-response bytes in dispatch, so bulk
  reads on both binary protocols draw down the tier egress bucket;
  covered by `qos_throttles_response_bytes` and
  `native_egress_throttles_responses`. The PG wire gateway charges
  simple-query and extended-execute responses the same way without
  breaking protocol sync; covered by `pg_qos_throttles_response_bytes`.
  gRPC charges exact encoded reply bytes per RPC; covered by
  `grpc_egress_throttles_responses`.
- Bounded auth identifiers: user ids capped at 256 bytes
  (`MAX_USER_LEN`) on register, passkey enroll, and challenge issue,
  and the challenge map prunes expired entries plus caps at 4096 pending
  (`429` beyond); covered by registry unit tests and a live `400` on an
  oversize challenge user.
- Durable-topic shape policy: `POST /v1/topics/append` enforces the
  shared 1 KiB key / 4 MiB value caps instead of relying solely on the
  2 MiB body limit; covered by a `400` on a 2 KiB message key.
- Realtime naming bounds: channel/partition/member names capped at
  256 bytes and presence state at 4 KiB, keeping the 1000-member room
  cap meaningful; covered by live `400`s.
- Spec/docs catch-up for enforced caps: query-stream clamp and
  `/v1/scan` limit corrected to 1000 in `docs/streaming.md` and
  `schemas/openapi/rest.yaml`.
- Manual verify requires write auth: `GET /v1/backups/verify` re-reads
  and decrypts the whole archive but accepted any authenticated
  principal; read-only callers now get `403`, matching the cost of the
  operation; covered by `e2e_verify_requires_write`.
- First live PITR coverage: `e2e_pitr_restore_roundtrip` writes a row,
  snapshots, checkpoints, writes a second row, restores to the checkpoint
  commit, and asserts the first row survives while the second is gone.
- Bounded checkpoint log: `BackupLog::record` keeps the newest 128
  markers (`BACKUP_LOG_KEEP`) instead of growing without bound; covered
  by a `ryme-backup` unit test and noted in `docs/backup-restore.md`,
  which also now states the backup-metadata auth requirement correctly.
- Operator-only archive egress: `POST /v1/backups/archive` and
  `POST /v1/backups/copy` require `Owner`/`Admin` instead of any writer,
  closing a cost-exhaustion vector (full data reads plus object-store
  egress on demand); covered by non-admin denials in the backup e2e.
- Operator-only masking rules: `POST /v1/auth/mask` requires
  `Owner`/`Admin` instead of any writer, so application keys cannot
  silently unmask PII columns; covered by a privilege-boundary denial
  with the operator path still green in the presence e2e.
- Scheduled restore drills: `archive.verify_interval_secs` (default off)
  re-verifies the latest backup on a cadence with results at
  `GET /v1/backups/drill` (`404` until the first drill); covered by
  `e2e_scheduled_drill_verifies_latest_backup` and documented in
  `docs/backup-restore.md`. The failure path is covered too:
  `e2e_drill_reports_corruption` flips a byte in an archived segment and
  asserts the drill records the backup id with a non-empty error. Surfaced as `backupDrill`/`backup_drill`/
  `BackupDrill`/`backup_drill` in JS/Python/Go/Rust SDKs (stub-tested),
  thin Swift/Kotlin/Dart/C# clients, `ryme backup drill`,
  `--verify-interval-secs`, the dashboard overview (with a `never run`
  state until the first drill), and a web-console Backups card button.
- Honest durability tiers: `Durability::is_durable` centralizes the
  fsync mapping (only `memory` relaxes it) with a pinning unit test, and
  the configuration docs now state that the three durable tiers share
  one full-durability implementation in v1 instead of implying tuned
  trade-offs that do not exist yet.
