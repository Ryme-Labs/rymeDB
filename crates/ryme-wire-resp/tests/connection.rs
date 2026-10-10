use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_line(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        socket.read_exact(&mut byte).await.unwrap();
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            break;
        }
    }
    line
}

async fn read_frame(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut pending: Vec<i64> = Vec::new();
    pending.push(1);
    while let Some(top) = pending.pop() {
        if top == 0 {
            continue;
        }
        let line = read_line(socket).await;
        out.extend_from_slice(&line);
        let head = String::from_utf8_lossy(&line).into_owned();
        let prefix = head.chars().next().unwrap_or(' ');
        match prefix {
            '$' => {
                let len: i64 = head[1..].trim().parse().unwrap_or(-1);
                if len >= 0 {
                    let mut body = vec![0u8; len as usize + 2];
                    socket.read_exact(&mut body).await.unwrap();
                    out.extend_from_slice(&body);
                }
                pending.push(top - 1);
            }
            '*' | '>' => {
                let count: i64 = head[1..].trim().parse().unwrap_or(0);
                pending.push(top - 1);
                pending.push(count.max(0));
            }
            '%' => {
                let count: i64 = head[1..].trim().parse().unwrap_or(0);
                pending.push(top - 1);
                pending.push(count.saturating_mul(2).max(0));
            }
            '_' => pending.push(top - 1),
            _ => pending.push(top - 1),
        }
    }
    out
}

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
async fn hello_negotiates_resp2() {
    let addr = serve().await;
    let reply = command(addr, &["HELLO", "2"]).await;
    assert!(reply.contains("rymedb"), "{reply}");
    assert!(reply.contains("proto"), "{reply}");
    let reply = command(addr, &["HELLO"]).await;
    assert!(reply.contains("rymedb"), "{reply}");
}

#[tokio::test]
async fn hello_negotiates_resp3_and_auth() {
    let addr = serve().await;
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n").await.unwrap();
    let reply =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await.unwrap();
    let reply = String::from_utf8_lossy(&reply);
    assert!(reply.starts_with("%7\r\n"), "{reply}");
    assert!(reply.contains("+proto\r\n:3\r\n"), "{reply}");
    socket.write_all(b"*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n").await.unwrap();
    let null_reply =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&null_reply), "_\r\n");
    let reply = command(addr, &["HELLO", "2", "AUTH", "u", "p"]).await;
    assert!(reply.contains("no password is set"), "{reply}");
    let reply = command(addr, &["AUTH", "secret"]).await;
    assert!(reply.contains("no password is set"), "{reply}");
}

#[tokio::test]
async fn resp3_pubsub_uses_push_frames() {
    let addr = serve().await;
    let mut subscriber = tokio::net::TcpStream::connect(addr).await.unwrap();
    subscriber.write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n").await.unwrap();
    let _ =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut subscriber)).await.unwrap();
    subscriber.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nchat\r\n").await.unwrap();
    let subscribed =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut subscriber)).await.unwrap();
    assert!(String::from_utf8_lossy(&subscribed).starts_with(">3\r\n"));

    assert_eq!(command(addr, &["PUBLISH", "chat", "hello"]).await, ":1");
    let message =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut subscriber)).await.unwrap();
    assert!(String::from_utf8_lossy(&message).starts_with(">3\r\n"));
}

#[tokio::test]
async fn client_name_roundtrip_and_id() {
    let addr = serve().await;
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let frame = |parts: &[&str]| {
        let mut out = format!("*{}\r\n", parts.len());
        for part in parts {
            out.push_str(&format!("${}\r\n{part}\r\n", part.len()));
        }
        out
    };
    let script =
        format!("{}{}", frame(&["CLIENT", "GETNAME"]), frame(&["CLIENT", "SETNAME", "worker-1"]));
    socket.write_all(script.as_bytes()).await.unwrap();
    let first =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&first).into_owned(), "$-1\r\n");
    let second =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&second).into_owned(), "+OK\r\n");
    let script = format!("{}{}", frame(&["CLIENT", "GETNAME"]), frame(&["CLIENT", "ID"]));
    socket.write_all(script.as_bytes()).await.unwrap();
    let third =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&third).into_owned(), "$8\r\nworker-1\r\n");
    let fourth =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await.unwrap();
    let id = String::from_utf8_lossy(&fourth).into_owned();
    assert!(id.starts_with(':'));
    let script = frame(&["CLIENT", "ID"]);
    socket.write_all(script.as_bytes()).await.unwrap();
    let fifth =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&fifth).into_owned(), id);
}

#[tokio::test]
async fn client_setinfo_accepted_like_drivers() {
    let addr = serve().await;
    assert_eq!(command(addr, &["CLIENT", "SETINFO", "LIB-NAME", "redis-py"]).await, "+OK");
    assert_eq!(command(addr, &["CLIENT", "SETINFO", "LIB-NAME", "x", "LIB-VER", "1"]).await, "+OK");
    assert!(command(addr, &["CLIENT", "FROBNICATE"]).await.contains("unknown subcommand"));
    assert!(command(addr, &["CLIENT"]).await.contains("wrong args"));
}

#[tokio::test]
async fn pipelined_reply_flushes_before_a_blocking_command() {
    let addr = serve().await;
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let frame = |parts: &[&str]| {
        let mut out = format!("*{}\r\n", parts.len());
        for part in parts {
            out.push_str(&format!("${}\r\n{part}\r\n", part.len()));
        }
        out
    };
    let pipeline = format!("{}{}", frame(&["PING"]), frame(&["BLPOP", "empty", "1"]));
    socket.write_all(pipeline.as_bytes()).await.unwrap();
    let first =
        tokio::time::timeout(Duration::from_millis(250), read_frame(&mut socket)).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&first), "+PONG\r\n");
    let second =
        tokio::time::timeout(Duration::from_secs(2), read_frame(&mut socket)).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&second), "*-1\r\n");
}

#[tokio::test]
async fn echo_roundtrip() {
    let addr = serve().await;
    assert_eq!(command(addr, &["ECHO", "hello"]).await, "$5\r\nhello");
    assert!(command(addr, &["ECHO"]).await.contains("wrong args"));
}

#[tokio::test]
async fn hello_full_shape() {
    let addr = serve().await;
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n").await.unwrap();
    let raw = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    for part in ["server", "rymedb", "proto", "mode", "standalone", "role", "master", "modules"] {
        assert!(text.contains(part), "{text}");
    }
}

#[tokio::test]
async fn auth_routes_commands_to_the_authenticated_tenant() {
    let manager = ryme_txn::TxnManager::new();
    let gateway = ryme_wire_resp::RespGateway::with_manager(
        String::from("default"),
        String::from("d"),
        manager.clone(),
    )
    .with_authenticator(|user, password| {
        if user == "alice" && password == "secret" {
            Ok(String::from("alpha"))
        } else {
            Err(ryme_error::RymeError::Unauthorized)
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });

    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let frame = |parts: &[&str]| {
        let mut out = format!("*{}\r\n", parts.len());
        for part in parts {
            out.push_str(&format!("${}\r\n{part}\r\n", part.len()));
        }
        out
    };
    socket.write_all(frame(&["GET", "key"]).as_bytes()).await.unwrap();
    let reply = read_frame(&mut socket).await;
    assert!(String::from_utf8_lossy(&reply).contains("NOAUTH"));
    socket.write_all(frame(&["AUTH", "alice", "secret"]).as_bytes()).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&read_frame(&mut socket).await), "+OK\r\n");
    socket.write_all(frame(&["SET", "key", "value"]).as_bytes()).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&read_frame(&mut socket).await), "+OK\r\n");
    socket.write_all(frame(&["GET", "key"]).as_bytes()).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&read_frame(&mut socket).await), "$5\r\nvalue\r\n");

    let mut txn = manager.begin();
    assert_eq!(manager.scan(&mut txn, "alpha", "d", "_kv", 10).unwrap().len(), 1);
    assert!(manager.scan(&mut txn, "default", "d", "_kv", 10).unwrap().is_empty());
}

#[tokio::test]
async fn auth_rejects_invalid_passwords() {
    let gateway = ryme_wire_resp::RespGateway::new(String::from("default"), String::from("d"))
        .with_authenticator(|_, password| {
            (password == "secret")
                .then(|| String::from("default"))
                .ok_or(ryme_error::RymeError::Unauthorized)
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let reply = command(addr, &["AUTH", "wrong"]).await;
    assert!(reply.contains("WRONGPASS"), "{reply}");
}
