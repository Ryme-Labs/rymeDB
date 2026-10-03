# Streaming (live changes and live queries)

rymeDB exposes two WebSocket endpoints for realtime data. Both upgrade from
`GET` and authenticate like the rest of the API: `Authorization: Bearer`
header, `x-api-key` header, or `?api_key=` query param (handy because
browser `WebSocket` clients cannot set headers).

## `GET /v1/stream?table=<name>` — change feed

Pushes one JSON text frame per committed change to the table. Auth requires
any valid credential; no read-permission check is performed. Each frame is a
`ChangeRecord` (`crates/ryme-realtime/src/lib.rs`):
```json
{
  "tenant": "default", "database": "default", "branch": "main",
  "table": "docs", "op": "INSERT",
  "pk": [107, 49], "after": [111, 110, 101],
  "commit_ts": 2, "sequence": 1
}
```

- `op` is `INSERT`, `UPDATE`, or `DELETE` (uppercase).
- `pk` and `after` are **byte arrays** (JSON number arrays), not strings:
  `[107, 49]` decodes to `"k1"`. `after` is `null` on deletes.
- The server skips (does not redeliver) messages when a slow consumer lags
  behind the broadcast channel, so a `/v1/stream` consumer **can miss
  changes under lag**. If you need at-least-complete state, use
  `/v1/query-stream` below.
- Resume with `?from=<commit>`: the socket first replays retained changes
  with `commit_ts` after the watermark (oldest-first, up to the full ring
  retention), then
  continues live, suppressing any live redelivery by sequence number — so
  a reconnecting client sees each change exactly once. Without `from` the
  socket stays quiet until the next commit (legacy behavior). `commit_ts`
  is a server-wide strictly increasing counter, so watermarks are exact.
  Retention is the per-topic ring (server `Realtime::new` capacity);
  changes committed while disconnected beyond retention cannot be
  replayed. REST/SQL/native/gRPC writes always publish; RESP only
  publishes while the table has subscribers (a throughput optimization),
  so RESP-only writes made with zero subscribers are absent from replay.
- Send a Close frame (or just disconnect) to stop; the server breaks the
  forwarding loop on close.

## `GET /v1/query-stream?table=<name>[&limit=<n>]` — live query

Requires a read-capable principal (`403` otherwise). `limit` defaults to 100
and is clamped to 1–1000. The first frame is always a full snapshot of the
table at the current commit:

```json
{"type": "snapshot", "commit": 7, "rows": [{"pk": "k1", "value": "one"}]}
```

Here `pk`/`value` are UTF-8 strings. Later frames are updates carrying only
commits newer than the snapshot:

```json
{"type": "update", "commit": 8, "rows": [{"pk": "k2", "value": "two"}], "truncated": false}
```

If the consumer lags, the server re-sends a fresh full snapshot instead of
dropping data, so a query-stream consumer resynchronizes automatically —
unlike the raw change feed.

## Tenant isolation

Presence rooms, broadcast channels, and durable partitions are scoped to
the caller's tenant (`{tenant}/{name}` internally): two tenants can use
the same channel or partition name without seeing each other, and durable
cursors restart at zero per tenant. Table change feeds were already scoped
by tenant/database/table.

Presence members carry a TTL (default 60s, clamped to 1s..24h). Expiry is enforced on every
read and join, and the TTL sweep loop prunes dead members across all rooms
(including rooms nobody lists anymore) while dropping emptied rooms, so
abandoned presence state cannot accumulate. Each room holds at most 1000
members; joins beyond the cap get `429`, and re-joining an existing member
always succeeds.

Channel, partition, and member names are capped at 256 bytes and presence
state payloads at 4 KiB (`400` beyond), so rooms stay small enough for the
1000-member cap to mean something.

Idle realtime scopes are reclaimed on the sweep cadence: change-feed and
broadcast senders with no receivers and no retained history, and query
topics with no receivers, are dropped. Scopes holding replayable history
are kept, so resuming consumers never lose retained changes.

## Event sources

Every committed write publishes exactly one change event per written key,
after the commit succeeds, so no path can silently bypass the feed:
- REST/Native KV gateway (`put`, `delete`, TTL touch)
- SQL executor (`INSERT`, `UPSERT`, `UPDATE`, `DELETE`, `COPY`/bulk), which
  also covers the PostgreSQL wire gateway, GraphQL, and native SQL
- RESP gateway: single commands, `MULTI`/`EXEC` batches, blocking pops and
  moves, and consumer-group deliveries (all under table `_kv`)
- TTL sweeper: one `DELETE` per expired key

`INSERT` vs `UPDATE` is decided from the transaction's pre-write read, so
read-modify-write commands and SQL statements report the precise operation.
Blind multi-writes (`MSET`, `UPSERT`, bulk ingest) pre-read their keys for
the same reason. Commits with no subscribers for a table skip cloning
entirely, so unwatched tables pay no CDC overhead. Replay, recovery, and
follower-apply paths never publish, so each committed change is emitted once
by the committing node.

## Operator CLI

```sh
ryme stream watch docs --count 2
ryme stream query docs --limit 100 --count 2
ryme stream broadcast lobby --count 1
```

`watch` tails `/v1/stream`; `query` tails `/v1/query-stream`; `broadcast`
tails `/v1/broadcast/<channel>`. `--count N`
exits after N frames (useful for scripting); without it, the command runs
until the socket closes or you send SIGINT.

## SDKs

- **Python** (`sdks/python/src/rymedb/__init__.py`):
  `client.subscribe_table(table)` / `client.subscribe_query(table, limit)`
  / `client.subscribe_broadcast(channel)` return an iterable `Subscription`
  of parsed dicts; call `.close()` when done.
- **JS** (`sdks/js/src/index.ts`): `subscribeTable(base, table, onMessage,
  {apiKey?})`
  and `subscribeQuery(base, table, onMessage, {apiKey?, limit?})`, plus
  `subscribeBroadcast(base, channel, onMessage, {apiKey?})`, all returning
  `{ready, close}`. `onMessage` receives parsed `ChangeRecord` /
  `QueryMessage` / `BroadcastRecord` objects.
- **Go** (`sdks/go/subscribe.go`): `client.SubscribeTable(table)` /
  `client.SubscribeQuery(table, limit)` / `client.SubscribeBroadcast(channel)`
  return a `*Subscription`; decode each frame with `sub.Next(&record)` /
  `sub.Next(&msg)` into `ChangeRecord` / `QueryMessage` / `BroadcastMessage`,
  then `sub.Close()`.

## `GET /v1/broadcast/<channel>` — broadcast subscribe

`POST /v1/broadcast` was previously fire-and-forget with no read path.
The channel now has a WebSocket subscribe endpoint mirroring the change
feed: authenticate like the other streams (header or `?api_key=`), then
receive one `BroadcastMsg` JSON frame (`channel`, `from`, `payload`,
`commit_ts`, `sequence`) per post to your tenant's channel. No history is
replayed — broadcast stays ephemeral — and slow consumers skip lagged
frames exactly like `/v1/stream`.

All three pass the key as `?api_key=` and default the base-URL scheme
(`http`→`ws`, `https`→`wss`).
