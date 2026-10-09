# @rymelabs/rymedb — JavaScript SDK

Typed client for rymeDB's REST API plus WebSocket subscriptions. Zero
runtime dependencies (Node 18+; `WebSocket` global used for subscriptions).

## Install

```sh
npm install /path/to/rymeDB/sdks/js
```

```js
import { RymeHttpClient, subscribeTable } from "@rymelabs/rymedb";
```

## Quickstart

```js
const c = new RymeHttpClient({ base: "http://127.0.0.1:8080", apiKey: "test-key" });

await c.kvPut("docs", "hello", '{"a":1}');
await c.kvGet("docs", "hello"); // '{"a":1}' — raw value text, never parsed
await c.sql("SELECT * FROM docs KEY 'hello'");
await c.ready(); // { ready, node, commit, leader, cluster }

const sub = subscribeTable("http://127.0.0.1:8080", "docs", (record) => {
  console.log(record.op, record.table); // INSERT docs
}, { apiKey: "test-key" });
await sub.ready;
sub.close();
```

Notes:

- The API key falls back to `$RYME_API_KEY` when omitted. Mutating endpoints
  need a write-capable principal; failures throw `RymeError` carrying
  `status` and `body`.
- `kvGet` returns the raw value bytes as text. Parse JSON yourself when your
  values are JSON.
- `subscribeQuery(base, table, onMessage, { limit?, branch?, apiKey? })` tails
  `/v1/query-stream`: first a `snapshot` message, then `update` messages.
- `subscribeTable(base, table, onMessage, { apiKey?, branch?, from? })` tails
  `/v1/stream`; subscriptions reconnect by default and resume from the latest
  received `sequence`. Set `reconnect: false` for one-shot behavior or tune
  the bounded retry delay with `reconnectDelayMs`.
- `subscribeBroadcast(base, channel, onMessage, { apiKey? })` tails
  `/v1/broadcast/<channel>` for ephemeral channel messages.
- `slowLog(limit?, table?)` and `traces(limit?, name?, table?)` narrow the
  observability feeds; `rangeAutosplit` and `rangeLoads` cover placement
  load.
- `RymeClient` (in the same package) is the legacy RESP client (`GET`/`SET`
  over TCP); prefer `RymeHttpClient` for new code.

Method coverage mirrors the server: kv get/put/delete/ttl, sql, scan,
branches, checkpoint/latest/pitr/snapshot/restore/archive/archives, shards
layout/move, cluster members/add/remove/transfer/replace, ready/metrics, and
both streams. Tests: `pnpm build && pnpm test`.

Supabase-style REST CRUD is available through `restInsert`, `restUpsert`,
`restUpdate`, `restDelete`, and `restDeleteWhere`; insert bodies may be JSON
objects or arrays, and update/delete methods accept PostgREST query strings.
