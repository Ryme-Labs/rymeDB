# Realtime, CDC, presence, and durable topics

All realtime endpoints are WebSockets. Authenticate with a bearer header,
`x-api-key`, or `api_key` query parameter. Browser clients normally use the
query parameter; server clients should prefer headers.

## Choose a stream

| Endpoint | Semantics | Recovery |
| --- | --- | --- |
| `/v1/stream?table=messages` | One committed change record per write | `from` or `from_sequence` replay while retained |
| `/v1/query-stream?table=messages` | Snapshot followed by current query updates | Sends a fresh snapshot if a consumer lags |
| `/v1/broadcast/{channel}` | Ephemeral fanout | No history; use topics for replay |
| `/v1/presence/{channel}/stream` | Snapshot plus join/leave state | Fresh state snapshot on lag |
| `/v1/topics/{partition}/stream` | Ordered persisted messages | Cursor replay; `cursor_expired` when retention is gone |
| `/realtime/v1/websocket` | Supabase/Phoenix channel protocol | Protocol-level join and presence behavior |

## Change feed

```text
wss://db.example.com/v1/stream?table=messages&api_key=...&from_sequence=42
```

Frames contain tenant, database, branch, table, operation, primary key,
before/after bytes, commit timestamp, transaction id, and sequence:

```json
{
  "tenant": "default",
  "database": "default",
  "branch": "main",
  "table": "messages",
  "op": "INSERT",
  "pk": [109, 45, 49],
  "before": null,
  "after": [104, 105],
  "commit_ts": 7,
  "tx_id": 7,
  "sequence": 42
}
```

`pk`, `before`, and `after` are byte arrays. The stream replays retained
changes oldest-first and suppresses already delivered sequence numbers. If a
cursor has expired, the server sends an error and closes instead of silently
returning an incomplete history; resnapshot and reconnect.

## Reactive queries

```text
wss://db.example.com/v1/query-stream?table=messages&limit=100&select=id,body&order=id.desc&status=eq.ready
```

The first frame is a `snapshot`; subsequent frames are `update` messages.
Filters, projections, order, views, and branch selection are reevaluated on
committed changes. Lagging consumers receive a current snapshot.

## Broadcast and presence

Publish an ephemeral event:

```bash
curl -fsS -X POST \
  -H 'Authorization: Bearer local-dev-key' \
  -H 'Content-Type: application/json' \
  --data '{"channel":"lobby","payload":{"event":"typing","user":"ada"}}' \
  http://127.0.0.1:8080/v1/broadcast
```

Broadcast is tenant-scoped and intentionally not WAL-backed. Presence rooms
start with a state snapshot, then emit joins/leaves. Members have a TTL,
rooms cap at 1,000 members, and abandoned state is swept.

## Durable topics

```bash
rymedb-cli topics append chat-room m-1 '{"text":"hello"}'
rymedb-cli topics read chat-room --from 0
rymedb-cli stream broadcast lobby --count 1
```

`POST /v1/topics/append` atomically persists a partition message and cursor.
The topic stream replays from `from=<cursor>` and then follows live appends.
Use it for chat delivery, agent jobs, or game events that must survive a
disconnect. Retention is bounded; handle `cursor_expired` by taking a new
application snapshot.

## Supabase Realtime

`/realtime/v1/websocket?apikey=<key>&vsn=1.0.0` accepts Phoenix-style
`phx_join`, `phx_leave`, heartbeat, broadcast, presence, and
`postgres_changes` configuration. RLS is checked before database changes are
exposed. The npm/TypeScript and Rust SDKs wrap this protocol.

## Delivery rules

- CDC is emitted once after a committed write; SQL, REST, native, gRPC, and
  RESP writes share the history.
- Change history and outgoing queues are bounded.
- Heartbeats detect dead connections; stalled sends are disconnected after a
  bounded timeout.
- Broadcast is best-effort and ephemeral; query streams resnapshot; durable
  topics replay within retention.
- Tenant, branch, and QoS limits apply to sockets and delivered bytes.
