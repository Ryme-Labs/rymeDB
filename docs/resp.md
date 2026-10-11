# RESP2/RESP3 and Valkey compatibility

The RESP gateway listens on `127.0.0.1:6380` by default and supports RESP2
and RESP3 framing. Existing `redis-cli`, `valkey-cli`, and many RESP clients
can connect without an SDK.

```bash
redis-cli -h 127.0.0.1 -p 6380 PING
redis-cli -p 6380 SET session:1 active EX 60
redis-cli -p 6380 GET session:1
```

Enable `RYME_RESP_PASSWORD` for password authentication. Use the dedicated
`resp_tls_listen` listener with TLS rather than exposing plaintext RESP.

## Implemented command families

| Family | Commands and behavior |
| --- | --- |
| Strings/keys | `GET`, `SET`, `SETNX`, `GETSET`, `GETEX`, `GETDEL`, `MGET`, `MSET`, `APPEND`, `STRLEN`, `DEL`, `EXISTS`, `TYPE` |
| Expiry | `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST` |
| Counters | `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT` |
| Hashes | `HSET`, `HGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`, `HKEYS`, `HVALS` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `BLPOP`, `BRPOP`, `BLMOVE` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER` |
| Sorted sets | `ZADD`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREM`, `ZCARD`, `ZCOUNT`, `ZINCRBY` |
| Streams | `XADD`, `XRANGE`, `XREVRANGE`, `XLEN`, `XTRIM`, `XREAD`, `XDEL`, `XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XAUTOCLAIM`, `XINFO` |
| Scripting | Sandboxed Lua 5.4: `EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`, `PCALL` |
| Transactions | `MULTI`, `EXEC`, `DISCARD`; staged writes commit atomically |
| Pub/sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB` |
| Other | `SCAN`, `KEYS`, HyperLogLog, geospatial commands |

The exact compatibility suite is in `crates/ryme-wire-resp/tests/`. Unsupported
option combinations should be treated as errors, not silently assumed to have
Redis semantics.

## Transaction notes

Commands inside `MULTI` execute against one transaction. Blocking list and
stream reads wake on commits and take a fresh transaction for each check.
Sorted sets use score ordering with lexicographic member tie-breaks. Stream
consumer groups and pending entries are persisted with the stream document.

RESP writes publish to the same tenant-scoped CDC path as SQL and REST writes;
the realtime page explains how to consume them.
