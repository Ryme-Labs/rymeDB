use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn command(addr: std::net::SocketAddr, parts: &[&str]) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut frame = format!("*{}\r\n", parts.len());
    for part in parts {
        frame.push_str(&format!("${}\r\n{part}\r\n", part.len()));
    }
    socket.write_all(frame.as_bytes()).await.unwrap();
    read_reply(&mut socket).await.trim().to_string()
}

async fn command_on(socket: &mut tokio::net::TcpStream, parts: &[&str]) -> String {
    let mut frame = format!("*{}\r\n", parts.len());
    for part in parts {
        frame.push_str(&format!("${}\r\n{part}\r\n", part.len()));
    }
    socket.write_all(frame.as_bytes()).await.unwrap();
    read_reply(socket).await.trim().to_string()
}

async fn read_reply(socket: &mut tokio::net::TcpStream) -> String {
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        read_resp_frame(socket, &mut raw).await;
    })
    .await
    .unwrap();
    String::from_utf8(raw).unwrap()
}

async fn read_resp_line(socket: &mut tokio::net::TcpStream, raw: &mut Vec<u8>) -> Vec<u8> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        socket.read_exact(&mut byte).await.unwrap();
        raw.push(byte[0]);
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            line.truncate(line.len() - 2);
            return line;
        }
    }
}

async fn read_resp_frame(socket: &mut tokio::net::TcpStream, raw: &mut Vec<u8>) {
    let mut pending = vec![1usize];
    while let Some(remaining) = pending.last_mut() {
        if *remaining == 0 {
            pending.pop();
            continue;
        }
        *remaining -= 1;
        let mut prefix = [0u8; 1];
        socket.read_exact(&mut prefix).await.unwrap();
        raw.push(prefix[0]);
        let line = read_resp_line(socket, raw).await;
        let children = match prefix[0] {
            b'$' | b'!' | b'=' => {
                let len = std::str::from_utf8(&line).unwrap().parse::<isize>().unwrap();
                if len >= 0 {
                    let mut body = vec![0; len as usize + 2];
                    socket.read_exact(&mut body).await.unwrap();
                    raw.extend_from_slice(&body);
                }
                0
            }
            b'*' | b'~' | b'>' | b'%' | b'|' => {
                let count = std::str::from_utf8(&line).unwrap().parse::<isize>().unwrap().max(0);
                let count = usize::try_from(count).unwrap_or(0);
                if matches!(prefix[0], b'%' | b'|') {
                    count.saturating_mul(2)
                } else {
                    count
                }
            }
            _ => 0,
        };
        if children > 0 {
            pending.push(children);
        }
    }
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
async fn ping_in_pubsub_mode_uses_the_pubsub_reply_shape() {
    let addr = serve().await;
    let mut subscriber = tokio::net::TcpStream::connect(addr).await.unwrap();
    subscriber.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nchat\r\n").await.unwrap();
    assert_eq!(
        read_reply(&mut subscriber).await,
        "*3\r\n$9\r\nsubscribe\r\n$4\r\nchat\r\n:1\r\n"
    );

    assert_eq!(command_on(&mut subscriber, &["PING"]).await, "*2\r\n$4\r\npong\r\n$0");
    assert_eq!(
        command_on(&mut subscriber, &["PING", "heartbeat"]).await,
        "*2\r\n$4\r\npong\r\n$9\r\nheartbeat"
    );
    assert!(command_on(&mut subscriber, &["PING", "one", "extra"]).await.contains("wrong args"));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn concurrent_set_overwrites_do_not_expose_serialization_conflicts() {
    const CLIENTS: usize = 12;
    const WRITES_PER_CLIENT: usize = 40;

    let addr = serve().await;
    let start = std::sync::Arc::new(tokio::sync::Barrier::new(CLIENTS));
    let mut clients = Vec::with_capacity(CLIENTS);
    for client in 0..CLIENTS {
        let start = start.clone();
        clients.push(tokio::spawn(async move {
            let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
            start.wait().await;
            let mut failures = Vec::new();
            for write in 0..WRITES_PER_CLIENT {
                let value = format!("{client}-{write}");
                let reply = command_on(&mut socket, &["SET", "hot-key", &value]).await;
                if reply != "+OK" {
                    failures.push(reply);
                }
            }
            failures
        }));
    }

    let mut failures = Vec::new();
    for client in clients {
        failures.extend(client.await.unwrap());
    }
    assert!(failures.is_empty(), "SET exposed internal transaction conflicts: {failures:?}");
    let value = command(addr, &["GET", "hot-key"]).await;
    assert!(value.starts_with('$'), "hot key missing after successful SETs: {value}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn concurrent_multi_increments_retry_conflicts_without_losing_updates() {
    const CLIENTS: usize = 12;
    const TRANSACTIONS_PER_CLIENT: usize = 20;

    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "multi-count", "0"]).await, "+OK");
    let start = std::sync::Arc::new(tokio::sync::Barrier::new(CLIENTS));
    let mut clients = Vec::with_capacity(CLIENTS);
    for _ in 0..CLIENTS {
        let start = start.clone();
        clients.push(tokio::spawn(async move {
            let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
            start.wait().await;
            let mut failures = Vec::new();
            for _ in 0..TRANSACTIONS_PER_CLIENT {
                if command_on(&mut socket, &["MULTI"]).await != "+OK" {
                    failures.push(String::from("MULTI failed"));
                    continue;
                }
                if command_on(&mut socket, &["INCR", "multi-count"]).await != "+QUEUED" {
                    failures.push(String::from("INCR was not queued"));
                    continue;
                }
                let reply = command_on(&mut socket, &["EXEC"]).await;
                if !reply.starts_with("*1\r\n:") {
                    failures.push(reply);
                }
            }
            failures
        }));
    }

    let mut failures = Vec::new();
    for client in clients {
        failures.extend(client.await.unwrap());
    }
    assert!(failures.is_empty(), "EXEC exposed transaction conflicts: {failures:?}");
    assert_eq!(
        command(addr, &["GET", "multi-count"]).await,
        format!(
            "${}\r\n{}",
            (CLIENTS * TRANSACTIONS_PER_CLIENT).to_string().len(),
            CLIENTS * TRANSACTIONS_PER_CLIENT
        )
    );
}

#[tokio::test]
async fn owner_reader_serves_remote_gets() {
    let manager = ryme_txn::TxnManager::new();
    let mut seed = manager.begin();
    manager.put(
        &mut seed,
        ryme_storage::RecordKey::new("t", "d", "_kv", b"local"),
        b"local-value".to_vec(),
    );
    manager.commit(seed).unwrap();
    let gateway =
        ryme_wire_resp::RespGateway::with_manager(String::from("t"), String::from("d"), manager)
            .with_remote_reader(ryme_wire_resp::RemoteReader::new(|key| async move {
                assert_eq!(key.table, "_kv");
                if key.pk.as_slice() == b"local" {
                    return Ok(ryme_wire_resp::RemoteRead::Local);
                }
                let value = match key.pk.as_slice() {
                    b"remote" => Some(b"owner-value".to_vec()),
                    b"other" => Some(b"other-value".to_vec()),
                    b"missing" => None,
                    _ => None,
                };
                Ok(ryme_wire_resp::RemoteRead::Value {
                    value,
                    expires_at: Some(ryme_txn::now_unix() + 60),
                })
            }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    assert_eq!(command(addr, &["GET", "remote"]).await, "$11\r\nowner-value");
    assert_eq!(
        command(addr, &["MGET", "remote", "other", "missing"]).await,
        "*3\r\n$11\r\nowner-value\r\n$11\r\nother-value\r\n$-1"
    );
    assert_eq!(
        command(addr, &["MGET", "remote", "local", "missing"]).await,
        "*3\r\n$11\r\nowner-value\r\n$11\r\nlocal-value\r\n$-1"
    );
    assert_eq!(command(addr, &["EXISTS", "remote", "missing"]).await, ":1");
    assert_eq!(command(addr, &["EXISTS", "remote", "local", "local"]).await, ":3");
    assert_eq!(
        command(addr, &["MGET", "local", "missing"]).await,
        "*2\r\n$11\r\nlocal-value\r\n$-1"
    );
    assert_eq!(command(addr, &["TYPE", "remote"]).await, "+string");
    let ttl: i64 = command(addr, &["TTL", "remote"]).await[1..].parse().unwrap();
    assert!((1..=60).contains(&ttl));
    let pttl: i64 = command(addr, &["PTTL", "remote"]).await[1..].parse().unwrap();
    assert!((1_000..=60_000).contains(&pttl));
}

#[tokio::test]
async fn owner_reader_batches_local_reads_in_standard_storage_mode() {
    let dir = std::env::temp_dir().join(format!(
        "ryme-resp-owner-standard-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    ));
    let manager = ryme_txn::DurableManager::open_with_mode(
        &dir,
        1024 * 1024,
        ryme_txn::SyncPolicy::Never,
        1024 * 1024,
        ryme_storage::StorageMode::Standard,
    )
    .unwrap();
    let mut seed = manager.begin();
    manager.put(
        &mut seed,
        ryme_storage::RecordKey::new("t", "d", "_kv", b"local"),
        b"local-value".to_vec(),
    );
    manager.commit(seed).unwrap();
    let gateway =
        ryme_wire_resp::RespGateway::with_backend(String::from("t"), String::from("d"), manager)
            .with_remote_reader(ryme_wire_resp::RemoteReader::new(|key| async move {
                if key.pk.as_slice() == b"local" {
                    return Ok(ryme_wire_resp::RemoteRead::Local);
                }
                Ok(ryme_wire_resp::RemoteRead::Value {
                    value: (key.pk.as_slice() == b"remote").then(|| b"owner-value".to_vec()),
                    expires_at: None,
                })
            }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });

    assert_eq!(
        command(addr, &["MGET", "remote", "local", "missing"]).await,
        "*3\r\n$11\r\nowner-value\r\n$11\r\nlocal-value\r\n$-1"
    );
    assert_eq!(command(addr, &["EXISTS", "remote", "local", "local"]).await, ":3");
    assert_eq!(
        command(addr, &["MGET", "local", "missing"]).await,
        "*2\r\n$11\r\nlocal-value\r\n$-1"
    );
    assert_eq!(command(addr, &["TTL", "remote"]).await, ":-1");
    assert_eq!(command(addr, &["PTTL", "remote"]).await, ":-1");
    assert_eq!(command(addr, &["EXPIRETIME", "remote"]).await, ":-1");
    assert_eq!(command(addr, &["PEXPIRETIME", "remote"]).await, ":-1");
    assert_eq!(command(addr, &["EXPIRETIME", "missing"]).await, ":-2");
    assert_eq!(command(addr, &["PEXPIRETIME", "missing"]).await, ":-2");

    server.abort();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
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
async fn float_counter_rejects_non_finite_results_without_changing_value() {
    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "f", "1e308"]).await, "+OK");
    assert!(command(addr, &["INCRBYFLOAT", "f", "1e308"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["GET", "f"]).await, "$5\r\n1e308");

    assert_eq!(command(addr, &["SET", "f", "inf"]).await, "+OK");
    assert!(command(addr, &["INCRBYFLOAT", "f", "1"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["GET", "f"]).await, "$3\r\ninf");
}

#[tokio::test]
async fn integer_counters_reject_overflow_without_changing_value() {
    let addr = serve().await;
    let max = i64::MAX.to_string();
    let min = i64::MIN.to_string();

    assert_eq!(command(addr, &["SET", "n", &max]).await, "+OK");
    assert!(command(addr, &["INCR", "n"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["GET", "n"]).await, format!("${}\r\n{max}", max.len()));

    assert_eq!(command(addr, &["SET", "n", &min]).await, "+OK");
    assert!(command(addr, &["DECR", "n"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["GET", "n"]).await, format!("${}\r\n{min}", min.len()));
    assert!(command(addr, &["DECRBY", "n", &min]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["GET", "n"]).await, format!("${}\r\n{min}", min.len()));
}

#[tokio::test]
async fn single_argument_commands_reject_extra_arguments() {
    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "key", "value"]).await, "+OK");
    assert_eq!(command(addr, &["EXPIRE", "key", "60"]).await, ":1");

    for args in [
        vec!["ECHO", "message", "extra"],
        vec!["GET", "key", "extra"],
        vec!["GETDEL", "key", "extra"],
        vec!["STRLEN", "key", "extra"],
        vec!["TTL", "key", "extra"],
        vec!["PTTL", "key", "extra"],
        vec!["TYPE", "key", "extra"],
        vec!["PERSIST", "key", "extra"],
    ] {
        assert!(command(addr, &args).await.starts_with("-ERR"), "accepted {args:?}");
    }

    assert_eq!(command(addr, &["GET", "key"]).await, "$5\r\nvalue");
    let ttl = command(addr, &["TTL", "key"]).await;
    assert!(
        ttl.strip_prefix(':')
            .and_then(|value| value.parse::<i64>().ok())
            .is_some_and(|ttl| ttl > 0)
    );
}

#[tokio::test]
async fn multi_key_ops() {
    let addr = serve().await;
    assert_eq!(command(addr, &["MSET", "a", "1", "b", "2"]).await, "+OK");
    assert_eq!(
        command(addr, &["MGET", "a", "b", "ghost"]).await,
        "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1"
    );
    assert_eq!(
        command(addr, &["MGET", "a", "a", "ghost"]).await,
        "*3\r\n$1\r\n1\r\n$1\r\n1\r\n$-1"
    );
    assert_eq!(command(addr, &["MSET", "odd"]).await.split_whitespace().next().unwrap(), "-ERR");
    assert_eq!(command(addr, &["EXISTS", "a", "b", "ghost"]).await, ":2");
    assert_eq!(command(addr, &["EXISTS", "a", "a", "ghost"]).await, ":2");
    assert_eq!(command(addr, &["TYPE", "a"]).await, "+string");
    assert_eq!(command(addr, &["TYPE", "ghost"]).await, "+none");
    assert_eq!(command(addr, &["STRLEN", "a"]).await, ":1");
    assert_eq!(command(addr, &["STRLEN", "ghost"]).await, ":0");
    assert_eq!(command(addr, &["APPEND", "a", "23"]).await, ":3");
    assert_eq!(command(addr, &["GET", "a"]).await, "$3\r\n123");
}

#[tokio::test]
async fn string_conditionals_and_getex_are_atomic() {
    let addr = serve().await;
    assert_eq!(command(addr, &["DEL", "lock", "a", "b", "session"]).await, ":0");
    assert_eq!(command(addr, &["SETNX", "lock", "owner-1"]).await, ":1");
    assert_eq!(command(addr, &["SETNX", "lock", "owner-2"]).await, ":0");
    assert_eq!(command(addr, &["GETSET", "lock", "owner-3"]).await, "$7\r\nowner-1");
    assert_eq!(command(addr, &["GET", "lock"]).await, "$7\r\nowner-3");

    assert_eq!(command(addr, &["MSETNX", "a", "1", "b", "2"]).await, ":1");
    assert_eq!(command(addr, &["MSETNX", "b", "new", "c", "3"]).await, ":0");
    assert_eq!(command(addr, &["GET", "c"]).await, "$-1");

    assert_eq!(command(addr, &["SET", "session", "token", "EX", "30"]).await, "+OK");
    assert_eq!(command(addr, &["GETEX", "session", "EX", "5"]).await, "$5\r\ntoken");
    let ttl: i64 = command(addr, &["TTL", "session"]).await[1..].parse().unwrap();
    assert!((1..=5).contains(&ttl));
    assert_eq!(command(addr, &["GETEX", "session", "PERSIST"]).await, "$5\r\ntoken");
    assert_eq!(command(addr, &["TTL", "session"]).await, ":-1");
    assert_eq!(command(addr, &["GETEX", "missing", "EX", "5"]).await, "$-1");
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
    assert_eq!(command(addr, &["HDEL", "h", "f2"]).await, ":1");
    assert_eq!(command(addr, &["HLEN", "h"]).await, ":0");
    assert_eq!(command(addr, &["TYPE", "h"]).await, "+none");
    assert_eq!(command(addr, &["HDEL", "missing", "f"]).await, ":0");
    assert_eq!(command(addr, &["TYPE", "missing"]).await, "+none");
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
    assert_eq!(command(addr, &["LPOP", "l"]).await, "$1\r\na");
    assert_eq!(command(addr, &["RPOP", "l"]).await, "$1\r\nb");
    assert_eq!(command(addr, &["LLEN", "l"]).await, ":0");
    assert_eq!(command(addr, &["TYPE", "l"]).await, "+none");
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
    for args in [
        ["ZADD", "z", "NX", "GT", "9", "c"],
        ["ZADD", "z", "NX", "LT", "0", "new"],
        ["ZADD", "z", "GT", "LT", "0", "a"],
    ] {
        assert!(command(addr, &args).await.starts_with("-ERR"), "accepted {args:?}");
    }
    assert_eq!(command(addr, &["ZCARD", "z"]).await, ":4");
    assert_eq!(command(addr, &["ZSCORE", "z", "a"]).await, "$1\r\n5");
    assert_eq!(command(addr, &["ZREM", "z", "a", "ghost"]).await, ":1");
    assert_eq!(command(addr, &["ZCARD", "z"]).await, ":3");
    assert_eq!(command(addr, &["ZREM", "z", "b", "c", "new"]).await, ":3");
    assert_eq!(command(addr, &["ZCARD", "z"]).await, ":0");
    assert_eq!(command(addr, &["TYPE", "z"]).await, "+none");
    assert_eq!(command(addr, &["ZREM", "missing", "member"]).await, ":0");
    assert_eq!(command(addr, &["TYPE", "missing"]).await, "+none");
    assert!(command(addr, &["ZADD", "z", "nan", "x"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["SET", "plain", "x"]).await, "+OK");
    assert!(command(addr, &["ZSCORE", "plain", "f"]).await.starts_with("-ERR"));
    assert_eq!(command(addr, &["ZRANGE", "ghost", "0", "-1"]).await, "*0");
    assert_eq!(command(addr, &["ZCARD", "ghost"]).await, ":0");
}

#[tokio::test]
async fn sorted_sets_preserve_infinite_scores() {
    let addr = serve().await;
    assert_eq!(command(addr, &["ZADD", "z", "+inf", "high"]).await, ":1");
    assert_eq!(command(addr, &["ZSCORE", "z", "high"]).await, "$3\r\ninf");
    assert_eq!(command(addr, &["ZADD", "z", "-inf", "low"]).await, ":1");
    assert_eq!(
        command(addr, &["ZRANGE", "z", "0", "-1"]).await,
        "*2\r\n$3\r\nlow\r\n$4\r\nhigh"
    );
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
    assert_eq!(command(addr, &["SREM", "s", "b"]).await, ":1");
    assert_eq!(command(addr, &["SCARD", "s"]).await, ":0");
    assert_eq!(command(addr, &["TYPE", "s"]).await, "+none");
    assert_eq!(command(addr, &["SREM", "missing", "member"]).await, ":0");
    assert_eq!(command(addr, &["TYPE", "missing"]).await, "+none");
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
    assert_eq!(command(addr, &["XRANGE", "st", "-", "+", "COUNT", "0"]).await, "*-1");
    assert_eq!(command(addr, &["XRANGE", "st", "-", "+", "COUNT", "-1"]).await, "*-1");
    assert_eq!(
        command(addr, &["XREVRANGE", "st", "+", "-", "COUNT", "0"]).await,
        "*-1"
    );
    assert_eq!(command(addr, &["XRANGE", "missing", "-", "+", "COUNT", "0"]).await, "*0");
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
async fn stream_reads_treat_zero_count_as_unlimited() {
    let addr = serve().await;
    assert_eq!(
        command(addr, &["XADD", "read-zero", "1-1", "f", "a"]).await,
        "$3\r\n1-1"
    );
    assert_eq!(
        command(addr, &["XADD", "read-zero", "2-2", "f", "b"]).await,
        "$3\r\n2-2"
    );

    let xread = command(addr, &["XREAD", "COUNT", "0", "STREAMS", "read-zero", "0-0"]).await;
    assert!(
        xread.contains("$3\r\n1-1") && xread.contains("$3\r\n2-2"),
        "XREAD: {xread}"
    );
    let latest = command(addr, &["XREAD", "COUNT", "1", "STREAMS", "read-zero", "+"]).await;
    assert!(latest.contains("$3\r\n2-2") && !latest.contains("$3\r\n1-1"), "XREAD +: {latest}");
    let negative_xread =
        command(addr, &["XREAD", "COUNT", "-1", "STREAMS", "read-zero", "0-0"]).await;
    assert!(
        negative_xread.contains("$3\r\n1-1") && negative_xread.contains("$3\r\n2-2"),
        "XREAD: {negative_xread}"
    );
    assert_eq!(command(addr, &["XREAD", "STREAMS", "missing", "+"]).await, "*-1");

    assert_eq!(
        command(addr, &["XGROUP", "CREATE", "read-zero", "group-zero", "0-0"]).await,
        "+OK"
    );
    let xreadgroup = command(
        addr,
        &[
            "XREADGROUP",
            "GROUP",
            "group-zero",
            "consumer-zero",
            "COUNT",
            "0",
            "STREAMS",
            "read-zero",
            ">",
        ],
    )
    .await;
    assert!(
        xreadgroup.contains("$3\r\n1-1") && xreadgroup.contains("$3\r\n2-2"),
        "XREADGROUP: {xreadgroup}"
    );
    assert_eq!(
        command(addr, &["XGROUP", "CREATE", "read-zero", "group-negative", "0-0"]).await,
        "+OK"
    );
    let negative_xreadgroup = command(
        addr,
        &[
            "XREADGROUP",
            "GROUP",
            "group-negative",
            "consumer-negative",
            "COUNT",
            "-1",
            "STREAMS",
            "read-zero",
            ">",
        ],
    )
    .await;
    assert!(
        negative_xreadgroup.contains("$3\r\n1-1")
            && negative_xreadgroup.contains("$3\r\n2-2"),
        "XREADGROUP: {negative_xreadgroup}"
    );
}

#[tokio::test]
async fn xreadgroup_history_is_scoped_to_pending_consumer() {
    let addr = serve().await;
    assert_eq!(command(addr, &["XADD", "history", "1-1", "f", "a"]).await, "$3\r\n1-1");
    assert_eq!(command(addr, &["XADD", "history", "2-2", "f", "b"]).await, "$3\r\n2-2");
    assert_eq!(command(addr, &["XGROUP", "CREATE", "history", "workers", "0-0"]).await, "+OK");
    assert!(command(addr, &["XREADGROUP", "GROUP", "workers", "alice", "STREAMS", "history", ">"])
        .await
        .contains("$3\r\n2-2"));

    assert_eq!(
        command(addr, &["XREADGROUP", "GROUP", "workers", "bob", "STREAMS", "history", "0-0"])
            .await,
        "*-1"
    );
    let alice_history = command(
        addr,
        &["XREADGROUP", "GROUP", "workers", "alice", "STREAMS", "history", "1-1"],
    )
    .await;
    assert!(alice_history.contains("$3\r\n2-2"), "reply: {alice_history}");
    assert!(!alice_history.contains("$3\r\n1-1"), "reply: {alice_history}");

    let pending = command(addr, &["XPENDING", "history", "workers"]).await;
    assert!(pending.contains("$5\r\nalice"), "pending: {pending}");
    assert!(!pending.contains("$3\r\nbob"), "pending: {pending}");
    let groups = command(addr, &["XINFO", "GROUPS", "history"]).await;
    assert!(groups.contains("$12\r\nentries-read\r\n:2"), "groups: {groups}");
}

#[tokio::test]
async fn blocking_xreadgroup_persists_consumer_before_timeout() {
    let addr = serve().await;
    assert_eq!(command(addr, &["XADD", "blocked-group", "1-1", "f", "a"]).await, "$3\r\n1-1");
    assert_eq!(
        command(addr, &["XGROUP", "CREATE", "blocked-group", "workers", "1-1"]).await,
        "+OK"
    );

    let reply = command(
        addr,
        &[
            "XREADGROUP",
            "GROUP",
            "workers",
            "idle-consumer",
            "BLOCK",
            "25",
            "STREAMS",
            "blocked-group",
            ">",
        ],
    )
    .await;
    assert_eq!(reply, "*-1");
    let consumers = command(addr, &["XINFO", "CONSUMERS", "blocked-group", "workers"]).await;
    assert!(consumers.contains("idle-consumer"), "consumers: {consumers}");
    assert!(consumers.contains("pending\r\n:0"), "consumers: {consumers}");
}

#[tokio::test]
async fn stream_last_generated_id_survives_entry_deletion() {
    let addr = serve().await;
    assert_eq!(
        command(addr, &["XADD", "deleted-stream", "9-1", "f", "old"]).await,
        "$3\r\n9-1"
    );
    assert_eq!(command(addr, &["XDEL", "deleted-stream", "9-1"]).await, ":1");

    let info = command(addr, &["XINFO", "STREAM", "deleted-stream"]).await;
    assert!(
        info.contains("$17\r\nlast-generated-id\r\n$3\r\n9-1"),
        "stream info: {info}"
    );
    assert!(command(addr, &["XADD", "deleted-stream", "9-1", "f", "duplicate"])
        .await
        .starts_with("-ERR"));

    assert_eq!(
        command(addr, &["XGROUP", "CREATE", "deleted-stream", "workers", "$"]).await,
        "+OK"
    );
    let groups = command(addr, &["XINFO", "GROUPS", "deleted-stream"]).await;
    assert!(
        groups.contains("$17\r\nlast-delivered-id\r\n$3\r\n9-1"),
        "group info: {groups}"
    );
    assert_eq!(
        command(addr, &["XGROUP", "CREATE", "deleted-stream", "reset-workers", "0-0"]).await,
        "+OK"
    );
    assert_eq!(
        command(addr, &["XGROUP", "SETID", "deleted-stream", "reset-workers", "$"]).await,
        "+OK"
    );
    let groups = command(addr, &["XINFO", "GROUPS", "deleted-stream"]).await;
    assert_eq!(groups.matches("$17\r\nlast-delivered-id\r\n$3\r\n9-1").count(), 2);
    assert_eq!(
        command(addr, &["XADD", "deleted-stream", "9-2", "f", "new"]).await,
        "$3\r\n9-2"
    );
    let delivered = command(
        addr,
        &["XREADGROUP", "GROUP", "workers", "consumer", "STREAMS", "deleted-stream", ">"],
    )
    .await;
    assert!(delivered.contains("$3\r\n9-2"), "delivery: {delivered}");

    assert_eq!(
        command(addr, &["XGROUP", "CREATE", "empty-stream", "empty-group", "$", "MKSTREAM"]).await,
        "+OK"
    );
    let empty_group = command(addr, &["XINFO", "GROUPS", "empty-stream"]).await;
    assert!(
        empty_group.contains("$17\r\nlast-delivered-id\r\n$3\r\n0-0"),
        "empty group: {empty_group}"
    );
}

#[tokio::test]
async fn xautoclaim_removes_deleted_entries_from_pending_list() {
    let addr = serve().await;
    assert_eq!(command(addr, &["XADD", "claim-orphan", "1-1", "f", "a"]).await, "$3\r\n1-1");
    assert_eq!(command(addr, &["XGROUP", "CREATE", "claim-orphan", "workers", "0-0"]).await, "+OK");
    assert!(command(
        addr,
        &["XREADGROUP", "GROUP", "workers", "first", "STREAMS", "claim-orphan", ">"]
    )
    .await
    .contains("$3\r\n1-1"));
    assert_eq!(command(addr, &["XDEL", "claim-orphan", "1-1"]).await, ":1");
    assert!(command(addr, &["XPENDING", "claim-orphan", "workers"]).await.contains("*4\r\n:1"));

    let claimed = command(
        addr,
        &["XAUTOCLAIM", "claim-orphan", "workers", "second", "0", "0-0", "COUNT", "10"],
    )
    .await;
    assert!(claimed.contains("*1\r\n$3\r\n1-1"), "deleted IDs: {claimed}");
    assert!(command(addr, &["XPENDING", "claim-orphan", "workers"]).await.contains("*4\r\n:0"));

    let claimed_again = command(
        addr,
        &["XAUTOCLAIM", "claim-orphan", "workers", "second", "0", "0-0", "COUNT", "10"],
    )
    .await;
    assert!(claimed_again.ends_with("*0"), "second response: {claimed_again}");
}

#[tokio::test]
async fn xautoclaim_justid_returns_only_claimed_ids() {
    let addr = serve().await;
    assert_eq!(command(addr, &["XADD", "claim-justid", "1-1", "f", "a"]).await, "$3\r\n1-1");
    assert_eq!(command(addr, &["XGROUP", "CREATE", "claim-justid", "workers", "0-0"]).await, "+OK");
    assert!(command(
        addr,
        &["XREADGROUP", "GROUP", "workers", "first", "STREAMS", "claim-justid", ">"]
    )
    .await
    .contains("$3\r\n1-1"));

    let claimed = command(
        addr,
        &["XAUTOCLAIM", "claim-justid", "workers", "second", "-1", "0-0", "JUSTID"],
    )
    .await;
    assert_eq!(claimed, "*3\r\n$3\r\n0-0\r\n*1\r\n$3\r\n1-1\r\n*0");
    assert!(command(addr, &["XPENDING", "claim-justid", "workers"]).await.contains("*4\r\n:1"));
    assert!(command(
        addr,
        &["XAUTOCLAIM", "claim-justid", "workers", "third", "0", "0-0", "COUNT", "0"]
    )
    .await
    .starts_with("-ERR"));
}

#[tokio::test]
async fn xautoclaim_cursor_includes_the_next_pending_id() {
    let addr = serve().await;
    assert_eq!(command(addr, &["XADD", "claim-cursor", "1-1", "f", "a"]).await, "$3\r\n1-1");
    assert_eq!(command(addr, &["XADD", "claim-cursor", "2-2", "f", "b"]).await, "$3\r\n2-2");
    assert_eq!(command(addr, &["XGROUP", "CREATE", "claim-cursor", "workers", "0-0"]).await, "+OK");
    assert!(command(
        addr,
        &["XREADGROUP", "GROUP", "workers", "first", "STREAMS", "claim-cursor", ">"]
    )
    .await
    .contains("$3\r\n2-2"));

    let first = command(
        addr,
        &["XAUTOCLAIM", "claim-cursor", "workers", "second", "0", "0-0", "COUNT", "1"],
    )
    .await;
    assert!(first.contains("$3\r\n2-2"), "next cursor: {first}");
    assert!(first.contains("$3\r\n1-1"), "first claimed ID: {first}");

    let second = command(
        addr,
        &["XAUTOCLAIM", "claim-cursor", "workers", "third", "0", "2-2", "COUNT", "1"],
    )
    .await;
    assert!(second.contains("$3\r\n2-2"), "second claimed ID: {second}");
    assert!(second.contains("$3\r\n0-0"), "terminal cursor: {second}");
}

#[tokio::test]
async fn xautoclaim_accepts_exclusive_and_incomplete_start_ids() {
    let addr = serve().await;
    for id in ["1-1", "1-2", "2-0"] {
        assert_eq!(command(addr, &["XADD", "claim-exclusive", id, "f", id]).await, format!("${}\r\n{}", id.len(), id));
    }
    assert_eq!(command(addr, &["XGROUP", "CREATE", "claim-exclusive", "workers", "0-0"]).await, "+OK");
    assert!(command(
        addr,
        &["XREADGROUP", "GROUP", "workers", "first", "STREAMS", "claim-exclusive", ">"]
    )
    .await
    .contains("$3\r\n2-0"));

    let exclusive = command(
        addr,
        &["XAUTOCLAIM", "claim-exclusive", "workers", "second", "0", "(1-1", "COUNT", "1"],
    )
    .await;
    assert!(exclusive.contains("$3\r\n1-2"), "exclusive start: {exclusive}");
    assert!(!exclusive.contains("$3\r\n1-1"), "exclusive start returned boundary: {exclusive}");

    let incomplete = command(
        addr,
        &["XAUTOCLAIM", "claim-exclusive", "workers", "third", "0", "2", "COUNT", "1"],
    )
    .await;
    assert!(incomplete.contains("$3\r\n2-0"), "incomplete start: {incomplete}");
}

#[tokio::test]
async fn xautoclaim_bounds_scans_of_entries_below_idle_threshold() {
    let addr = serve().await;
    for n in 1..=11 {
        let id = format!("{n}-0");
        assert_eq!(
            command(addr, &["XADD", "claim-idle", &id, "f", "v"]).await,
            format!("${}\r\n{}", id.len(), id)
        );
    }
    assert_eq!(command(addr, &["XGROUP", "CREATE", "claim-idle", "workers", "0-0"]).await, "+OK");
    assert!(command(
        addr,
        &["XREADGROUP", "GROUP", "workers", "first", "STREAMS", "claim-idle", ">"]
    )
    .await
    .contains("$4\r\n11-0"));

    let scanned = command(
        addr,
        &["XAUTOCLAIM", "claim-idle", "workers", "second", "99999999", "0-0", "COUNT", "1"],
    )
    .await;
    assert!(scanned.starts_with("*3\r\n$4\r\n11-0\r\n*0\r\n*0"), "bounded scan: {scanned}");
    let consumers = command(addr, &["XINFO", "CONSUMERS", "claim-idle", "workers"]).await;
    assert!(consumers.contains("second"), "consumer should be registered: {consumers}");
    assert!(consumers.contains("pending\r\n:0"), "consumer has no pending entries: {consumers}");
}

#[tokio::test]
async fn blocking_xread_pins_dollar_to_initial_stream_position() {
    let addr = serve().await;
    assert_eq!(
        command(addr, &["XADD", "blocking-dollar", "1-1", "f", "old"]).await,
        "$3\r\n1-1"
    );

    let reader = tokio::spawn(async move {
        command(addr, &["XREAD", "BLOCK", "1000", "STREAMS", "blocking-dollar", "$"]).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert_eq!(
        command(addr, &["XADD", "blocking-dollar", "2-2", "f", "new"]).await,
        "$3\r\n2-2"
    );

    let reply = tokio::time::timeout(std::time::Duration::from_secs(2), reader)
        .await
        .expect("blocking XREAD should wake after XADD")
        .expect("reader task should complete");
    assert!(reply.contains("$3\r\n2-2"), "reply: {reply}");
    assert!(!reply.contains("$3\r\n1-1"), "reply included the old entry: {reply}");
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
    assert!(command(addr, &["XREADGROUP", "GROUP", "gg", "cc", "STREAMS", "xi", "$"])
        .await
        .starts_with("-ERR"));
    assert!(command(addr, &["XREADGROUP", "GROUP", "gg", "cc", "STREAMS", "xi", "+"])
        .await
        .starts_with("-ERR"));
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
    for n in 3..=12 {
        let id = format!("{n}-0");
        assert_eq!(
            command(addr, &["XADD", "xi", &id, "f", "v"]).await,
            format!("${}\r\n{}", id.len(), id)
        );
    }
    let default_count = command(addr, &["XINFO", "STREAM", "xi", "FULL"]).await;
    assert!(default_count.contains("$7\r\nentries\r\n*10"), "default count: {default_count}");
    let all_count = command(addr, &["XINFO", "STREAM", "xi", "FULL", "COUNT", "0"]).await;
    assert!(all_count.contains("$7\r\nentries\r\n*12"), "zero count: {all_count}");
    let negative_count = command(addr, &["XINFO", "STREAM", "xi", "FULL", "COUNT", "-1"]).await;
    assert!(negative_count.contains("$7\r\nentries\r\n*10"), "negative count: {negative_count}");
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
    assert!(command(addr, &["PFMERGE", "hm"]).await.starts_with("-ERR"));
    let after_invalid_merge: u64 = command(addr, &["PFCOUNT", "hm"]).await[1..].parse().unwrap();
    assert_eq!(after_invalid_merge, merged, "invalid PFMERGE must not mutate destination");
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
