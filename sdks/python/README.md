# rymedb — Python SDK

Typed client for rymeDB's REST API plus WebSocket subscriptions. Requires
`httpx` and `websocket-client` (declared in `pyproject.toml`).

## Install

```sh
pip install ./sdks/python
```

```python
from rymedb import RymeHttpClient
```

## Quickstart

```python
c = RymeHttpClient("http://127.0.0.1:8080", api_key="test-key")

c.kv_put("docs", "hello", '{"a":1}')
c.kv_get("docs", "hello")  # '{"a":1}' — raw value text, never parsed
c.sql("SELECT * FROM docs KEY 'hello'")
c.ready()  # {"ready": True, "node": "ryme-0", ...}

sub = c.subscribe_query("docs", limit=10)
for msg in sub:  # first message is always the snapshot
    print(msg["type"], len(msg["rows"]))
    break
sub.close()
```

Notes:

- The API key falls back to `$RYME_API_KEY` when omitted. Mutating endpoints
  need a write-capable principal; failures raise `RymeError` carrying
  `status` and `body`.
- `kv_get` returns the raw value bytes as text. Parse JSON yourself when your
  values are JSON.
- `subscribe_query(table, limit=None)` tails `/v1/query-stream`: first a
  `snapshot` message, then `update` messages.
- `subscribe_table(table, timeout=30.0, from_commit=None)` tails
  `/v1/stream`; pass the last seen `commit_ts` as `from_commit` to replay
  missed changes, then continue live.
- `subscribe_broadcast(channel, timeout=30.0)` tails
  `/v1/broadcast/<channel>` for ephemeral channel messages.
- `slow_log(limit=None, table=None)` and `traces(limit=None, name=None,
  table=None)` narrow the observability feeds; `range_autosplit` and
  `range_loads` cover placement load.
- `RymeClient` (same package) is the legacy RESP client over TCP; prefer
  `RymeHttpClient` for new code.

Method coverage mirrors the server: kv get/put/delete/ttl, sql, scan,
branches, checkpoint/latest/pitr/snapshot/restore/archive/archives, shards
layout/move, cluster members/add/remove/transfer/replace, ready/metrics, and
both streams. Tests: `pytest sdks/python/tests`.
