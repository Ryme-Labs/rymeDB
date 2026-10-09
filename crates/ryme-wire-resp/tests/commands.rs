use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn command(addr: std::net::SocketAddr, parts: &[&str]) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut frame = format!("*{}\r\n", parts.len());
    for part in parts {
        frame.push_str(&format!("${}\r\n{part}\r\n", part.len()));
    }
    socket.write_all(frame.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 4096];
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
            if raw.ends_with(b"\r\n") {
                break;
            }
        }
    })
    .await
    .unwrap();
    String::from_utf8(raw).unwrap().trim().to_string()
}

async fn read_reply(socket: &mut tokio::net::TcpStream) -> String {
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 4096];
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(read > 0, "connection closed before reply");
            raw.extend_from_slice(&chunk[..read]);
            if raw.ends_with(b"\r\n") {
                break;
            }
        }
    })
    .await
    .unwrap();
    String::from_utf8(raw).unwrap()
}

async fn serve() -> std::net::SocketAddr {
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    addr
}

#[tokio::test]
async fn pubsub_delivers_messages_to_subscribed_connections() {
    let addr = serve().await;
    let mut subscriber = tokio::net::TcpStream::connect(addr).await.unwrap();
    subscriber.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nchat\r\n").await.unwrap();
    assert_eq!(read_reply(&mut subscriber).await, "*3\r\n$9\r\nsubscribe\r\n$4\r\nchat\r\n:1\r\n");

    assert_eq!(command(addr, &["PUBLISH", "chat", "hello"]).await, ":1");
    assert_eq!(
        read_reply(&mut subscriber).await,
        "*3\r\n$7\r\nmessage\r\n$4\r\nchat\r\n$5\r\nhello\r\n"
    );

    subscriber.write_all(b"*1\r\n$11\r\nUNSUBSCRIBE\r\n").await.unwrap();
    assert_eq!(
        read_reply(&mut subscriber).await,
        "*3\r\n$11\r\nunsubscribe\r\n$4\r\nchat\r\n:0\r\n"
    );

    subscriber.write_all(b"*2\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nroom:*\r\n").await.unwrap();
    assert_eq!(
        read_reply(&mut subscriber).await,
        "*3\r\n$10\r\npsubscribe\r\n$6\r\nroom:*\r\n:1\r\n"
    );
    assert_eq!(command(addr, &["PUBLISH", "room:1", "hello"]).await, ":1");
    assert_eq!(
        read_reply(&mut subscriber).await,
        "*4\r\n$8\r\npmessage\r\n$6\r\nroom:*\r\n$6\r\nroom:1\r\n$5\r\nhello\r\n"
    );
    assert_eq!(command(addr, &["PUBSUB", "NUMPAT"]).await, ":1");
    subscriber.write_all(b"*1\r\n$12\r\nPUNSUBSCRIBE\r\n").await.unwrap();
    assert_eq!(
        read_reply(&mut subscriber).await,
        "*3\r\n$12\r\npunsubscribe\r\n$6\r\nroom:*\r\n:0\r\n"
    );
}

#[tokio::test]
async fn counters_roundtrip() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "n"]).await, ":0");
    assert_eq!(command(addr, &["INCR", "n"]).await, ":1");
    assert_eq!(command(addr, &["INCRBY", "n", "41"]).await, ":42");
    assert_eq!(command(addr, &["DECR", "n"]).await, ":41");
    assert_eq!(command(addr, &["DECRBY", "n", "40"]).await, ":1");
    assert_eq!(command(addr, &["GET", "n"]).await, "$1\r\n1");
    assert_eq!(command(addr, &["SET", "s", "abc"]).await, "+OK");
    assert!(command(addr, &["INCR", "s"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["INCRBY", "n"]).await.split_whitespace().next().unwrap(), "-ERR");
}

#[tokio::test]
async fn set_get_returns_the_previous_value_atomically() {
    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "cache:key", "v1", "GET"]).await, "$-1");
    assert_eq!(command(addr, &["SET", "cache:key", "v2", "GET"]).await, "$2\r\nv1");
    assert_eq!(command(addr, &["SET", "missing", "v", "XX", "GET"]).await, "$-1");
    assert_eq!(command(addr, &["GET", "missing"]).await, "$-1");
}

#[tokio::test]
async fn float_counter_formats_cleanly() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "f"]).await, ":0");
    assert_eq!(command(addr, &["INCRBYFLOAT", "f", "0.5"]).await, "$3\r\n0.5");
    assert_eq!(command(addr, &["INCRBYFLOAT", "f", "1.5"]).await, "$1\r\n2");
    assert!(command(addr, &["INCRBYFLOAT", "f", "xx"]).await.starts_with("-ERR"));
}

#[tokio::test]
async fn multi_key_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["MSET", "a", "1", "b", "2"]).await, "+OK");
    assert_eq!(
        command(addr, &["MGET", "a", "b", "ghost"]).await,
        "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1"
    );
    assert_eq!(command(addr, &["MSET", "odd"]).await.split_whitespace().next().unwrap(), "-ERR");
    assert_eq!(command(addr, &["EXISTS", "a", "b", "ghost"]).await, ":2");
    assert_eq!(command(addr, &["TYPE", "a"]).await, "+string");
    assert_eq!(command(addr, &["TYPE", "ghost"]).await, "+none");
    assert_eq!(command(addr, &["STRLEN", "a"]).await, ":1");
    assert_eq!(command(addr, &["STRLEN", "ghost"]).await, ":0");
    assert_eq!(command(addr, &["APPEND", "a", "23"]).await, ":3");
    assert_eq!(command(addr, &["GET", "a"]).await, "$3\r\n123");
}

#[tokio::test]
async fn hash_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "h"]).await, ":0");
    assert_eq!(command(addr, &["HSET", "h", "f1", "v1", "f2", "v2"]).await, ":2");
    assert_eq!(command(addr, &["HSET", "h", "f1", "v1b"]).await, ":0");
    assert_eq!(command(addr, &["HGET", "h", "f1"]).await, "$3\r\nv1b");
    assert_eq!(command(addr, &["HGET", "h", "ghost"]).await, "$-1");
    assert_eq!(command(addr, &["HEXISTS", "h", "f2"]).await, ":1");
    assert_eq!(command(addr, &["HEXISTS", "h", "ghost"]).await, ":0");
    assert_eq!(command(addr, &["HLEN", "h"]).await, ":2");
    assert_eq!(
        command(addr, &["HGETALL", "h"]).await,
        "*4\r\n$2\r\nf1\r\n$3\r\nv1b\r\n$2\r\nf2\r\n$2\r\nv2"
    );
    assert_eq!(command(addr, &["HDEL", "h", "f1", "ghost"]).await, ":1");
    assert_eq!(command(addr, &["HLEN", "h"]).await, ":1");
    assert_eq!(command(addr, &["SET", "plain", "x"]).await, "+OK");
    assert!(command(addr, &["HGET", "plain", "f"]).await.starts_with("-ERR"));
}

#[tokio::test]
async fn list_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "l"]).await, ":0");
    assert_eq!(command(addr, &["RPUSH", "l", "a", "b", "c"]).await, ":3");
    assert_eq!(command(addr, &["LPUSH", "l", "z"]).await, ":4");
    assert_eq!(command(addr, &["LLEN", "l"]).await, ":4");
    assert_eq!(
        command(addr, &["LRANGE", "l", "0", "-1"]).await,
        "*4\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc"
    );
    assert_eq!(command(addr, &["LRANGE", "l", "1", "2"]).await, "*2\r\n$1\r\na\r\n$1\r\nb");
    assert_eq!(command(addr, &["LRANGE", "l", "9", "9"]).await, "*0");
    assert_eq!(command(addr, &["LPOP", "l"]).await, "$1\r\nz");
    assert_eq!(command(addr, &["RPOP", "l"]).await, "$1\r\nc");
    assert_eq!(command(addr, &["LLEN", "l"]).await, ":2");
    assert_eq!(command(addr, &["LPOP", "ghost"]).await, "$-1");
}

#[tokio::test]
async fn zset_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "z"]).await, ":0");
    assert_eq!(command(addr, &["ZADD", "z", "1", "a", "2", "b", "1.5", "c"]).await, ":3");
    assert_eq!(command(addr, &["ZADD", "z", "5", "a"]).await, ":0");
    assert_eq!(command(addr, &["ZSCORE", "z", "a"]).await, "$1\r\n5");
    assert_eq!(command(addr, &["ZSCORE", "z", "ghost"]).await, "$-1");
    assert_eq!(command(addr, &["ZCARD", "z"]).await, ":3");
    assert_eq!(command(addr, &["ZRANK", "z", "a"]).await, ":2");
    assert_eq!(command(addr, &["ZREVRANK", "z", "a"]).await, ":0");
    assert_eq!(command(addr, &["ZRANK", "z", "ghost"]).await, "$-1");
    assert_eq!(
        command(addr, &["ZRANGE", "z", "0", "-1"]).await,
        "*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na"
    );
    assert_eq!(
        command(addr, &["ZRANGE", "z", "0", "1", "WITHSCORES"]).await,
        "*4\r\n$1\r\nc\r\n$3\r\n1.5\r\n$1\r\nb\r\n$1\r\n2"
    );
    assert_eq!(
        command(addr, &["ZRANGE", "z", "0", "-1", "REV"]).await,
        "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc"
    );
    assert_eq!(command(addr, &["ZCOUNT", "z", "1.5", "5"]).await, ":3");
    assert_eq!(command(addr, &["ZCOUNT", "z", "(1.5", "5"]).await, ":2");
    assert_eq!(command(addr, &["ZCOUNT", "z", "-inf", "+inf"]).await, ":3");
    assert_eq!(command(addr, &["ZINCRBY", "z", "1", "c"]).await, "$3\r\n2.5");
    assert_eq!(command(addr, &["ZADD", "z", "NX", "9", "c", "0", "new"]).await, ":1");
    assert_eq!(command(addr, &["ZSCORE", "z", "c"]).await, "$3\r\n2.5");
    assert_eq!(command(addr, &["ZADD", "z", "XX", "9", "c", "0", "ghost"]).await, ":0");
    assert_eq!(command(addr, &["ZREM", "z", "a", "ghost"]).await, ":1");
    assert_eq!(command(addr, &["ZCARD", "z"]).await, ":3");
    assert!(command(addr, &["ZADD", "z", "nan", "x"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["SET", "plain", "x"]).await, "+OK");
    assert!(command(addr, &["ZSCORE", "plain", "f"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["ZRANGE", "ghost", "0", "-1"]).await, "*0");
    assert_eq!(command(addr, &["ZCARD", "ghost"]).await, ":0");
}

#[tokio::test]
async fn set_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "s"]).await, ":0");
    assert_eq!(command(addr, &["SADD", "s", "b", "a", "a"]).await, ":2");
    assert_eq!(command(addr, &["SCARD", "s"]).await, ":2");
    assert_eq!(command(addr, &["SISMEMBER", "s", "a"]).await, ":1");
    assert_eq!(command(addr, &["SISMEMBER", "s", "ghost"]).await, ":0");
    assert_eq!(command(addr, &["SMEMBERS", "s"]).await, "*2\r\n$1\r\na\r\n$1\r\nb");
    assert_eq!(command(addr, &["SREM", "s", "a", "ghost"]).await, ":1");
    assert_eq!(command(addr, &["SCARD", "s"]).await, ":1");
}

#[tokio::test]
async fn scan_iterates_with_cursor() {
    let addr = serve().await;
    assert_eq!(command(addr, &["MSET", "sc:a", "1", "sc:b", "2", "sc:c", "3"]).await, "+OK");
    let mut seen = std::collections::BTreeSet::new();
    let mut cursor = String::from("0");
    loop {
        let reply = command(addr, &["SCAN", &cursor, "COUNT", "2"]).await;
        let mut lines = reply.split("\r\n");
        assert_eq!(lines.next(), Some("*2"));
        let cursor_len: usize = lines.next().unwrap_or("$0")[1..].parse().unwrap_or(0);
        let next_cursor = lines.next().unwrap_or("0").to_string();
        assert_eq!(next_cursor.len(), cursor_len);
        let count: usize = lines.next().unwrap_or("*0")[1..].parse().unwrap_or(0);
        for _ in 0..count {
            let len: usize = lines.next().unwrap_or("$0")[1..].parse().unwrap_or(0);
            seen.insert(lines.next().unwrap_or("").to_string());
            assert!(len > 0);
        }
        cursor = next_cursor;
        if cursor == "0" {
            break;
        }
    }
    assert!(seen.contains("sc:a") && seen.contains("sc:b") && seen.contains("sc:c"));
    assert_eq!(seen.len(), 3);
}

#[tokio::test]
async fn scan_match_filters() {
    let addr = serve().await;
    assert_eq!(command(addr, &["MSET", "mx:1", "1", "mx:2", "2", "other", "3"]).await, "+OK");
    let reply = command(addr, &["SCAN", "0", "MATCH", "mx:*"]).await;
    assert!(reply.contains("mx:1") && reply.contains("mx:2"), "match page: {reply}");
    assert!(!reply.contains("other"), "match page: {reply}");
    let reply = command(addr, &["SCAN", "0", "MATCH", "mx:?", "COUNT", "10"]).await;
    assert!(reply.contains("mx:1") && reply.contains("mx:2"), "glob page: {reply}");
    assert!(command(addr, &["SCAN", "zz"]).await.starts_with("-ERR"));
    assert!(command(addr, &["SCAN", "0", "TYPE", "string"]).await.starts_with("-ERR"));
}

#[tokio::test]
async fn keys_matches_the_full_keyspace() {
    let addr = serve().await;
    assert_eq!(command(addr, &["MSET", "chat:1", "a", "chat:2", "b", "other", "c"]).await, "+OK");
    let reply = command(addr, &["KEYS", "chat:?"]).await;
    assert_eq!(reply, "*2\r\n$6\r\nchat:1\r\n$6\r\nchat:2");
    assert_eq!(
        command(addr, &["KEYS", "*"]).await,
        "*3\r\n$6\r\nchat:1\r\n$6\r\nchat:2\r\n$5\r\nother"
    );
    assert!(command(addr, &["KEYS"]).await.starts_with("-ERR"));
}

#[tokio::test]
async fn stream_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["XADD", "st", "100-1", "f", "a"]).await, "$5\r\n100-1");
    assert_eq!(command(addr, &["XADD", "st", "100-2", "f", "b"]).await, "$5\r\n100-2");
    assert_eq!(command(addr, &["XLEN", "st"]).await, ":2");
    assert_eq!(
        command(addr, &["XRANGE", "st", "-", "+"]).await,
        "*2\r\n*2\r\n$5\r\n100-1\r\n*2\r\n$1\r\nf\r\n$1\r\na\r\n*2\r\n$5\r\n100-2\r\n*2\r\n$1\r\nf\r\n$1\r\nb"
    );
    assert_eq!(
        command(addr, &["XREVRANGE", "st", "+", "-", "COUNT", "1"]).await,
        "*1\r\n*2\r\n$5\r\n100-2\r\n*2\r\n$1\r\nf\r\n$1\r\nb"
    );
    assert_eq!(
        command(addr, &["XREAD", "STREAMS", "st", "100-1"]).await,
        "*1\r\n*2\r\n$2\r\nst\r\n*1\r\n*2\r\n$5\r\n100-2\r\n*2\r\n$1\r\nf\r\n$1\r\nb"
    );
    assert_eq!(command(addr, &["XREAD", "STREAMS", "st", "$"]).await, "*-1");
    assert!(command(addr, &["XADD", "st", "100-1", "f", "c"]).await.starts_with("-ERR"));
    assert!(command(addr, &["XADD", "st", "0-0", "f", "c"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["XDEL", "st", "100-1"]).await, ":1");
    assert!(command(addr, &["XDEL", "st", "bogus-id"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["XLEN", "st"]).await, ":1");
    assert_eq!(
        command(addr, &["XADD", "st", "MAXLEN", "~", "1", "200-1", "f", "c"]).await,
        "$5\r\n200-1"
    );
    assert_eq!(command(addr, &["XLEN", "st"]).await, ":1");
    assert_eq!(command(addr, &["XRANGE", "ghost", "-", "+"]).await, "*0");
    assert_eq!(command(addr, &["XLEN", "ghost"]).await, ":0");
    assert_eq!(command(addr, &["SET", "leg", "[[\"500-1\",[[\"f\",\"v\"]]]]"]).await, "+OK");
    assert_eq!(command(addr, &["XLEN", "leg"]).await, ":1");
    assert_eq!(
        command(addr, &["XRANGE", "leg", "-", "+"]).await,
        "*1\r\n*2\r\n$5\r\n500-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv"
    );
    assert!(command(addr, &["XREAD", "BLOCK", "xx", "STREAMS", "st", "0-0"])
        .await
        .starts_with("-ERR"));
    assert_eq!(command(addr, &["SET", "plain", "x"]).await, "+OK");
    assert!(command(addr, &["XLEN", "plain"]).await.starts_with("-ERR"));
}

#[tokio::test]
async fn xinfo_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "xi"]).await, ":0");
    assert_eq!(command(addr, &["XADD", "xi", "1-1", "f", "v"]).await, "$3\r\n1-1");
    assert_eq!(command(addr, &["XADD", "xi", "2-2", "f", "w"]).await, "$3\r\n2-2");
    assert_eq!(command(addr, &["XGROUP", "CREATE", "xi", "gg", "0-0"]).await, "+OK");
    assert!(command(addr, &["XREADGROUP", "GROUP", "gg", "cc", "STREAMS", "xi", ">"])
        .await
        .starts_with("*1"));
    assert_eq!(
        command(addr, &["XINFO", "STREAM", "xi"]).await,
        "*20\r\n$6\r\nlength\r\n:2\r\n$15\r\nradix-tree-keys\r\n:2\r\n$16\r\nradix-tree-nodes\r\n:0\r\n$17\r\nlast-generated-id\r\n$3\r\n2-2\r\n$20\r\nmax-deleted-entry-id\r\n$3\r\n0-0\r\n$13\r\nentries-added\r\n:2\r\n$23\r\nrecorded-first-entry-id\r\n$3\r\n1-1\r\n$6\r\ngroups\r\n:1\r\n$11\r\nfirst-entry\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n$10\r\nlast-entry\r\n*1\r\n*2\r\n$3\r\n2-2\r\n*2\r\n$1\r\nf\r\n$1\r\nw"
    );
    assert_eq!(
        command(addr, &["XINFO", "GROUPS", "xi"]).await,
        "*1\r\n*12\r\n$4\r\nname\r\n$2\r\ngg\r\n$9\r\nconsumers\r\n:1\r\n$7\r\npending\r\n:2\r\n$17\r\nlast-delivered-id\r\n$3\r\n2-2\r\n$12\r\nentries-read\r\n:2\r\n$3\r\nlag\r\n:0"
    );
    let consumers = command(addr, &["XINFO", "CONSUMERS", "xi", "gg"]).await;
    assert!(
        consumers.starts_with(
            "*1\r\n*8\r\n$4\r\nname\r\n$2\r\ncc\r\n$7\r\npending\r\n:2\r\n$4\r\nidle\r\n:"
        ),
        "consumers: {consumers}"
    );
    let full = command(addr, &["XINFO", "STREAM", "xi", "FULL"]).await;
    assert!(full.starts_with("*18\r\n$6\r\nlength\r\n:2"), "full: {full}");
    assert!(full.contains("$7\r\nentries\r\n*2"), "full: {full}");
    assert!(full.contains("$12\r\nentries-read\r\n:2"), "full: {full}");
    assert!(full.contains("$9\r\npel-count\r\n:2"), "full: {full}");
    let partial = command(addr, &["XINFO", "STREAM", "xi", "FULL", "COUNT", "1"]).await;
    assert!(partial.contains("$7\r\nentries\r\n*1"), "partial: {partial}");
    assert!(command(addr, &["XINFO", "STREAM", "ghost"]).await.starts_with("-ERR"));
    assert!(command(addr, &["XINFO", "GROUPS", "ghost"]).await.starts_with("-ERR"));
    assert!(command(addr, &["XINFO", "CONSUMERS", "ghost", "gg"]).await.starts_with("-ERR"));
    assert!(command(addr, &["XINFO", "BOGUS"]).await.starts_with("-ERR"));
    assert!(command(addr, &["XINFO", "HELP"]).await.contains("CONSUMERS"));
}

#[tokio::test]
async fn hyperloglog_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "h1", "h2", "hm"]).await, ":0");
    assert_eq!(command(addr, &["PFCOUNT", "h1"]).await, ":0");
    let members: Vec<String> = (0..100).map(|n| format!("m{n}")).collect();
    let mut parts: Vec<&str> = vec!["PFADD", "h1"];
    for member in &members {
        parts.push(member);
    }
    assert_eq!(command(addr, &parts).await, ":1");
    assert_eq!(command(addr, &parts).await, ":0");
    let count: u64 = command(addr, &["PFCOUNT", "h1"]).await[1..].parse().unwrap();
    assert!((98..=102).contains(&count), "count: {count}");
    assert_eq!(command(addr, &["PFADD", "h2", "a", "b", "c"]).await, ":1");
    assert_eq!(command(addr, &["PFMERGE", "hm", "h1", "h2"]).await, "+OK");
    let merged: u64 = command(addr, &["PFCOUNT", "hm"]).await[1..].parse().unwrap();
    assert!(merged >= count && merged <= count + 3, "merged: {merged}");
    let union: u64 = command(addr, &["PFCOUNT", "h1", "h2"]).await[1..].parse().unwrap();
    assert_eq!(union, merged);
    assert_eq!(command(addr, &["SET", "plain", "x"]).await, "+OK");
    assert!(command(addr, &["PFADD", "plain", "x"]).await.starts_with("-ERR"));
    assert!(command(addr, &["PFCOUNT", "plain"]).await.starts_with("-ERR"));
}

#[tokio::test]
async fn hyperloglog_accuracy() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "big"]).await, ":0");
    for chunk in 0..10 {
        let held: Vec<String> =
            (0..1000).map(|n| format!("user-{}-{n}", chunk * 1000 + n)).collect();
        let mut parts: Vec<&str> = vec!["PFADD", "big"];
        for member in &held {
            parts.push(member);
        }
        assert!(command(addr, &parts).await.starts_with(':'));
    }
    let count: u64 = command(addr, &["PFCOUNT", "big"]).await[1..].parse().unwrap();
    let error = (count as f64 - 10_000.0).abs() / 10_000.0;
    assert!(error < 0.05, "count: {count}");
}

#[tokio::test]
async fn geo_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "geo"]).await, ":0");
    assert_eq!(
        command(
            addr,
            &[
                "GEOADD",
                "geo",
                "13.361389",
                "38.115556",
                "Palermo",
                "15.087269",
                "37.502669",
                "Catania"
            ]
        )
        .await,
        ":2"
    );
    assert_eq!(command(addr, &["GEOADD", "geo", "13.361389", "38.115556", "Palermo"]).await, ":0");
    let bulk = |reply: String| reply.split("\r\n").nth(1).unwrap_or("").to_string();
    let dist: f64 =
        bulk(command(addr, &["GEODIST", "geo", "Palermo", "Catania"]).await).parse().unwrap();
    assert!((dist - 166_274.0).abs() < 5.0, "dist: {dist}");
    let km: f64 =
        bulk(command(addr, &["GEODIST", "geo", "Palermo", "Catania", "km"]).await).parse().unwrap();
    assert!((km - 166.274).abs() < 0.005, "km: {km}");
    assert_eq!(command(addr, &["GEODIST", "geo", "Palermo", "ghost"]).await, "$-1");
    let pos = command(addr, &["GEOPOS", "geo", "Palermo", "ghost"]).await;
    let mut parts = pos.split("\r\n");
    assert_eq!(parts.next(), Some("*2"));
    assert_eq!(parts.next(), Some("*2"));
    let lon: f64 = parts.nth(1).unwrap_or("0").parse().unwrap();
    let lat: f64 = parts.nth(1).unwrap_or("0").parse().unwrap();
    assert!((lon - 13.361389).abs() < 1e-5, "lon: {lon}");
    assert!((lat - 38.115556).abs() < 1e-5, "lat: {lat}");
    assert_eq!(parts.next(), Some("*-1"));
    assert_eq!(
        command(addr, &["GEOSEARCH", "geo", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km"])
            .await,
        "*2\r\n$7\r\nPalermo\r\n$7\r\nCatania"
    );
    assert_eq!(
        command(addr, &["GEOSEARCH", "geo", "FROMMEMBER", "Palermo", "BYRADIUS", "100", "km"])
            .await,
        "*1\r\n$7\r\nPalermo"
    );
    assert_eq!(
        command(
            addr,
            &["GEOSEARCH", "geo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "DESC"]
        )
        .await,
        "*2\r\n$7\r\nPalermo\r\n$7\r\nCatania"
    );
    assert_eq!(
        command(
            addr,
            &["GEOSEARCH", "geo", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "COUNT", "1"]
        )
        .await,
        "*1\r\n$7\r\nPalermo"
    );
    assert!(command(
        addr,
        &["GEOSEARCH", "geo", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "WITHDIST"]
    )
    .await
    .contains("166.2742"));
    assert!(command(addr, &["GEOADD", "geo", "200", "38", "bad"]).await.starts_with("-ERR"));
    assert!(command(addr, &["GEOSEARCH", "geo", "FROMMEMBER", "ghost", "BYRADIUS", "200", "km"])
        .await
        .starts_with("-ERR"));
    assert_eq!(command(addr, &["SET", "plain", "x"]).await, "+OK");
    assert!(command(addr, &["GEODIST", "plain", "a", "b"]).await.starts_with("-ERR"));
}

#[tokio::test]
async fn georadius_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "gr", "grdst"]).await, ":0");
    assert_eq!(
        command(
            addr,
            &[
                "GEOADD",
                "gr",
                "13.361389",
                "38.115556",
                "Palermo",
                "15.087269",
                "37.502669",
                "Catania"
            ]
        )
        .await,
        ":2"
    );
    assert_eq!(
        command(addr, &["GEORADIUS", "gr", "15", "37", "200", "km"]).await,
        "*2\r\n$7\r\nPalermo\r\n$7\r\nCatania"
    );
    assert_eq!(
        command(addr, &["GEORADIUS", "gr", "15", "37", "200", "km", "ASC"]).await,
        "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo"
    );
    assert_eq!(
        command(addr, &["GEORADIUS", "gr", "15", "37", "200", "km", "WITHHASH"]).await,
        "*2\r\n*2\r\n$7\r\nPalermo\r\n:3479099956230698\r\n*2\r\n$7\r\nCatania\r\n:3479447370796909"
    );
    assert_eq!(
        command(addr, &["GEORADIUSBYMEMBER", "gr", "Palermo", "200", "km"]).await,
        "*2\r\n$7\r\nPalermo\r\n$7\r\nCatania"
    );
    assert_eq!(
        command(addr, &["GEORADIUS", "gr", "15", "37", "200", "km", "STOREDIST", "grdst"]).await,
        ":2"
    );
    assert_eq!(
        command(addr, &["ZRANGE", "grdst", "0", "-1", "WITHSCORES"]).await,
        "*4\r\n$7\r\nCatania\r\n$23\r\n56441.25787015815876657\r\n$7\r\nPalermo\r\n$24\r\n190442.42984775736113079"
    );
    assert_eq!(
        command(addr, &["GEORADIUS", "gr", "15", "37", "200", "km", "STORE", "grdst"]).await,
        ":2"
    );
    assert_eq!(command(addr, &["ZCARD", "grdst"]).await, ":2");
    assert_eq!(
        command(addr, &["GEOHASH", "gr", "Palermo", "Catania", "ghost"]).await,
        "*3\r\n$11\r\nsqc8b49rny0\r\n$11\r\nsqdtr74hyu0\r\n$-1"
    );
    let stored = command(addr, &["GEOPOS", "grdst", "Palermo"]).await;
    let mut parts = stored.split("\r\n");
    assert_eq!(parts.next(), Some("*1"));
    assert_eq!(parts.next(), Some("*2"));
    let lon: f64 = parts.nth(1).unwrap_or("0").parse().unwrap();
    let lat: f64 = parts.nth(1).unwrap_or("0").parse().unwrap();
    assert!((lon - 13.361389).abs() < 1e-5, "lon: {lon}");
    assert!((lat - 38.115556).abs() < 1e-5, "lat: {lat}");
}
