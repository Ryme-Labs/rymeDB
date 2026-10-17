# npm / JavaScript SDK

Package: `@rymelabs/rymedb`. It is a typed, zero-runtime-dependency HTTP and
WebSocket client for Node.js 18+ and browser-compatible runtimes with a
WebSocket implementation.

## Install

```bash
npm install @rymelabs/rymedb
```

For the repository version:

```bash
pnpm --dir sdks/js install
pnpm --dir sdks/js build
pnpm --dir sdks/js test
```

## HTTP quickstart

```ts
import { RymeHttpClient } from "@rymelabs/rymedb";

const db = new RymeHttpClient({
  base: "http://127.0.0.1:8080",
  apiKey: process.env.RYME_API_KEY,
});

await db.kvPut("cache", "greeting", "hello");
console.log(await db.kvGet("cache", "greeting"));
console.log(await db.sql("SELECT * FROM messages WHERE room = $1", ["lobby"]));
console.log(await db.ready());
```

`kvGet` returns raw text; parse JSON explicitly when your application stores
JSON. Errors expose HTTP status and response body.

In a browser app, pass the public project key explicitly, for example with a
framework-exposed variable such as `NEXT_PUBLIC_RYME_PUBLIC_API_KEY`:

```ts
const db = new RymeHttpClient({
  base: process.env.NEXT_PUBLIC_RYME_URL!,
  apiKey: process.env.NEXT_PUBLIC_RYME_PUBLIC_API_KEY!,
});
```

That value is intentionally visible to every visitor and is limited to the
read-only SQL `anon` role. Use a user access token for authenticated operations,
and enforce row access with SQL grants and RLS; never put `RYME_API_KEY` in
browser code. See [authentication and RLS](auth.md#browser-applications).

## Realtime

```ts
import { subscribeTable, subscribeQuery } from "@rymelabs/rymedb";

const changes = subscribeTable(
  "http://127.0.0.1:8080",
  "messages",
  (change) => console.log(change.op, change.table),
  { apiKey: process.env.RYME_API_KEY, reconnect: true },
);
await changes.ready;

const query = subscribeQuery(
  "http://127.0.0.1:8080",
  "messages",
  (message) => console.log(message.type, message.rows),
  { apiKey: process.env.RYME_API_KEY, limit: 100, select: "id,body" },
);
await query.ready;
```

Available helpers include `subscribeTable`, `subscribeTableResumable`,
`subscribeQuery`, `subscribeBroadcast`, `subscribePresence`,
`subscribeDurableTopic`, and `subscribeSupabaseChannel`. Resumable table
subscriptions keep the last sequence and reconnect within bounded retry
delays. `close()` stops a subscription.

## CRUD, branches, and migrations

The client also exposes `restInsert`, `restUpsert`, `restUpdate`,
`restDeleteWhere`, `scan`, branch/backup/cluster/metrics helpers, and
`migrateApply`/`migrateLedger`. Pass `branch: "preview"` to migration helpers
or use the branch option on data methods.

The legacy `RymeClient` RESP client is retained for compatibility; use
`RymeHttpClient` for new application code when HTTP/WebSocket is appropriate.
