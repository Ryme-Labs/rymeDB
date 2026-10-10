# RESP compatibility

rymeDB speaks RESP2/RESP3 inline and multibulk framing on `resp_listen`
(plaintext) and `resp_tls_listen` (dedicated TLS port). Hashes, lists and
sets are stored as JSON documents under the key; collection commands
require UTF-8 payloads and answer `wrong type` otherwise. Single-key
read-modify-write commands (counters, `APPEND`, `GETDEL`) commit in one
transaction. Coverage table (`crates/ryme-wire-resp/tests/commands.rs`
pins every row):

| Group | Commands |
| --- | --- |
| Connection | `PING`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB` |
| Scripting | `EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH [SYNC|ASYNC]` (sandboxed Lua 5.4) |
| Transactions | `MULTI`, `EXEC`, `DISCARD` (atomic, see below) |
| Strings | `GET`, `SET` (`EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`GET`), `SETNX`, `GETSET`, `GETEX`, `GETDEL`, `MGET`, `MSET`, `MSETNX`, `APPEND`, `STRLEN` |
| Counters | `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT` (clean float formatting, saturating integers) |
| Keys | `DEL`, `EXISTS`, `TYPE` (`string`/`none` in v1), `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` |
| Hashes | `HSET`, `HGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`, `HKEYS`, `HVALS` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE` (negative indexes) |
| Blocking lists | `BLPOP`, `BRPOP`, `BLMOVE` (commit-wakeup, timeout or `0` for indefinite) |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER` |
| Sorted sets | `ZADD` (`NX`/`XX`/`GT`/`LT`), `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE` (index ranges, `REV`, `WITHSCORES`), `ZREM`, `ZCARD`, `ZCOUNT`, `ZINCRBY` |
| Iteration | `SCAN` (cursor, `MATCH`, `COUNT`), `KEYS` (full glob scan) |
| Streams | `XADD` (auto/explicit IDs, `MAXLEN`/`MINID`), `XRANGE`, `XREVRANGE` (`COUNT`), `XLEN`, `XTRIM`, `XREAD` (`BLOCK`, `COUNT`), `XDEL`, `XGROUP` (`CREATE`/`DESTROY`/`SETID`), `XREADGROUP` (`BLOCK`, `COUNT`), `XACK`, `XPENDING` (summary), `XAUTOCLAIM`, `XINFO` (`STREAM`/`GROUPS`/`CONSUMERS`/`HELP`) |
| HyperLogLog | `PFADD`, `PFCOUNT`, `PFMERGE` (p=14, ~0.81% std error) |
| Geospatial | `GEOADD`, `GEODIST` (`m`/`km`/`mi`/`ft`), `GEOPOS`, `GEOHASH`, `GEOSEARCH` (`FROMMEMBER`/`FROMLONLAT`, `BYRADIUS`/`BYBOX`, `ASC`/`DESC`, `COUNT`, `WITHDIST`/`WITHCOORD`), `GEORADIUS`, `GEORADIUSBYMEMBER` (`WITHDIST`/`WITHHASH`/`WITHCOORD`, `STORE`/`STOREDIST`) |

When the gateway has realtime enabled, RESP Pub/Sub is bridged through a
tenant/database-scoped realtime topic, so subscribers connected through
different gateways in the same process receive `PUBLISH` messages. In a
Raft-backed deployment, the leader also forwards the sequence-stamped event
to peer gateways through the realtime fanout path. Without a realtime layer,
the gateway retains its low-overhead process-local Pub/Sub path.

Sorted sets order by score with lexicographic member tiebreak. Scores
accept `inf`/`-inf`; `nan` is rejected. `ZRANGE` supports index ranges
only (no `BYSCORE`/`BYLEX`).

Streams store ordered `ms-seq` entries as JSON documents. `XADD` accepts
`*`, `ms-*` and explicit IDs strictly greater than the last entry
(`0-0` is rejected); `MAXLEN`/`MINID` trim exactly (`~` is accepted and
ignored). `XREAD` supports `BLOCK` in milliseconds (`0` waits
indefinitely) with `$` meaning new entries only; waiters wake on the
commit bus with fresh transactions per check, so no snapshot is pinned
while blocked. Consumer groups (`XGROUP`, `XREADGROUP`, `XACK`) are not
covered. Each command commits in one transaction, so queueing stream
writes inside `MULTI` keeps them atomic.

Consumer groups live in the stream document, so group cursors and the
pending list commit atomically with the entries. `XREADGROUP` with `>`
advances the group cursor and tracks redeliveries; explicit IDs read
history without moving the cursor. `XPENDING` answers the summary form
only. `XAUTOCLAIM` reassigns entries idle past the threshold with
cursor pagination; pending entries carry delivery timestamps, and
pre-timestamp PELs parse with zero timestamps. Bare-array stream
documents written before groups existed keep reading; every write
upgrades them. `XINFO` reports lengths, IDs, per-group lag and
pending, and per-consumer idle/inactive; `radix-tree-keys` reports the
entry count and `radix-tree-nodes` is 0 (no radix index in this
engine). Missing keys answer `no such key`.

Legacy `GEORADIUS`/`GEORADIUSBYMEMBER` share the search core with
`WITHDIST`/`WITHHASH`/`WITHCOORD`, `COUNT`, ordering and `STORE`
(geohash scores, stays geo-readable) / `STOREDIST` (meters). Stored
geohash scores decode back to cell centers, so `STORE` destinations
answer `GEOPOS` and accept `GEOADD`, matching Redis. Unflagged searches
return index (score) order; `ASC`/`DESC` sort by distance, with ties
reversed under `DESC`, all verified against Redis.

## Wire compatibility

`crates/ryme-wire-resp/tests/redis_parity.rs` diffs this server
against a live Redis command-for-command (run
`redis-server --port 7779` then
`RYME_REDIS_ADDR=127.0.0.1:7779 cargo test -p ryme-wire-resp --test
redis_parity`). Floats compare at 6 decimals, unordered collections as
sets, HyperLogLog within 10%, `SCAN` by full-iteration key sets; error
replies compare by presence only, except value errors, which match
Redis strings exactly (`value is not an integer or out of range`,
`value is not a valid float`, `min or max is not a float`,
`invalid longitude,latitude pair`). Nil shapes follow Redis:
bulk nil for values, nil array for `XREAD`/`XREADGROUP`/blocking
timeouts and missing `GEOPOS` members. `XDEL` validates every ID
before deleting anything. `valkey_cli_smoke` additionally drives the
gateway through real `valkey-cli` over piped stdin (all types,
`MULTI`/`EXEC`, `SCAN`, error paths). Integer scores
(`WITHHASH`, `STOREDIST`, `ZRANGE ... WITHSCORES` on a `STORE` key)
are the 52-bit cells; `GEOHASH` strings are the standard 50-bit
base32 form with the constant `0` pad, both verified digit-for-digit
against Redis.

## Transactions

`MULTI` queues commands per connection (`+QUEUED` each) and `EXEC`
applies them in a single serializable transaction, returning one array
with a reply per command. A conflicting concurrent write aborts the
whole `EXEC` with an error and nothing is applied; retry the block.
`DISCARD` drops the queue. An unknown command marks the queue dirty, so
`EXEC` answers `-EXECABORT` and applies nothing. Argument errors (wrong
arity, `value is not an integer or out of range`) surface as error elements in the `EXEC` array
while the remaining commands still apply, matching Redis. Nested
`MULTI` and bare `EXEC`/`DISCARD` are rejected.

Lua scripts run atomically inside the command transaction and receive the
standard `KEYS` and `ARGV` tables plus `redis.call` and `redis.pcall`.
`redis.pcall` returns command failures as `{err = "..."}` tables so scripts
can inspect and handle them. The runtime loads only
safe table/string/math/UTF-8 libraries, disables filesystem/process helpers,
limits scripts to 1 MiB, 8 MiB of Lua memory, and 100,000 VM instructions.
script debugging and arbitrary Redis module APIs are not exposed yet. RESP
pub/sub is in-memory and best-effort; durable replay and
query subscriptions remain available through
the WebSocket realtime API. `KEYS` is supported for compatibility, but it
scans the complete keyspace synchronously; use `SCAN` for production traffic.
These return `unknown command` rather than a wrong answer.

## Connection management

`HELLO 2` negotiates RESP2 with the server map (`server`, `version`, `proto`,
`id`, `mode`, `role`, `modules`); bare `HELLO` behaves the same. `HELLO 3`
negotiates RESP3 and returns the same fields as a map. RESP3 keeps the command
engine shared with RESP2, translates null replies to the RESP3 null type, and
uses push frames for Pub/Sub deliveries and subscription acknowledgements.
`CLIENT SETNAME`/`GETNAME` are per-connection state, `CLIENT ID` is a
server-wide counter, `CLIENT SETINFO` is accepted, `ECHO` round-trips, and
`AUTH` answers `Client sent AUTH, but no password is set` exactly like a
passwordless Redis. Verified live against real `redis-py`
(`ping`/`set`/`get`/`client_id`/`client_setname`/`client_getname`/`echo`).
