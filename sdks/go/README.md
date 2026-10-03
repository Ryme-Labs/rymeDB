# rymedb-go — Go SDK

Typed client for rymeDB's REST API plus WebSocket subscriptions. Requires
`golang.org/x/net` (pinned in `go.mod`, compatible with go 1.22).

## Install

This module is consumed from this repo until published:

```sh
go mod edit -replace rymelabs/rymedb-go=/path/to/rymeDB/sdks/go
go get rymelabs/rymedb-go@v0.0.0
```

```go
import rymedb "rymelabs/rymedb-go"
```

## Quickstart

```go
c := rymedb.NewHttpClient("http://127.0.0.1:8080", "test-key")

c.KvPut("docs", "hello", `{"a":1}`, nil)
v, _ := c.KvGet("docs", "hello") // '{"a":1}' — raw value text, never parsed
ready, _ := c.Ready()            // ready.Ready, ready.Node, ready.Leader

sub, _ := c.SubscribeQuery("docs", nil)
var snap rymedb.QueryMessage     // first message is always the snapshot
sub.Next(&snap)
sub.Close()
```

Notes:

- The API key falls back to `$RYME_API_KEY` when empty. Mutating endpoints
  need a write-capable principal; failures return `*HttpError` carrying
  `Status` and `Body`.
- `KvGet` returns the raw value bytes as text. Parse JSON yourself when your
  values are JSON.
- `SubscribeTable(table)` tails `/v1/stream`; decode frames with
  `sub.Next(&record)` into `ChangeRecord`. `SubscribeTableFrom(table, &from)`
  replays changes after a `commit_ts` watermark, then continues live.
- `SubscribeBroadcast(channel)` tails `/v1/broadcast/<channel>`; decode
  frames with `sub.Next(&msg)` into `BroadcastMessage`.
- `SlowLog(&limit, &table)` and `Traces(&limit, &name, &table)` narrow the
  observability feeds; `RangeAutosplit` and `RangeLoads` cover placement
  load.
- `Client` (same package) is the legacy RESP client over TCP; prefer
  `HttpClient` for new code.

Method coverage mirrors the server: kv get/put/delete/ttl, sql, scan,
branches, checkpoint/latest/pitr/snapshot/restore/archive/archives, shards
layout/move, cluster members/add/remove/transfer/replace, ready/metrics, and
both streams. Tests: `go test ./...`.
