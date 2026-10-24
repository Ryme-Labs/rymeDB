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
async fn set_ex_immediate_expiry() {
    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "k", "v", "EX", "0"]).await, "+OK");
    assert_eq!(command(addr, &["GET", "k"]).await, "$-1");
    assert_eq!(command(addr, &["TTL", "k"]).await, ":-2");
    assert_eq!(command(addr, &["PTTL", "k"]).await, ":-2");
}

#[tokio::test]
async fn ttl_lifecycle() {
    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "k", "v"]).await, "+OK");
    assert_eq!(command(addr, &["TTL", "k"]).await, ":-1");
    assert_eq!(command(addr, &["PERSIST", "k"]).await, ":0");
    assert_eq!(command(addr, &["EXPIRE", "k", "100"]).await, ":1");
    let ttl: i64 = command(addr, &["TTL", "k"]).await[1..].parse().unwrap();
    assert!((1..=100).contains(&ttl));
    let pttl: i64 = command(addr, &["PTTL", "k"]).await[1..].parse().unwrap();
    assert!(pttl > 0);
    assert_eq!(command(addr, &["PERSIST", "k"]).await, ":1");
    assert_eq!(command(addr, &["TTL", "k"]).await, ":-1");
    assert_eq!(command(addr, &["GET", "k"]).await, "$1\r\nv");
}

#[tokio::test]
async fn millisecond_expiry_keeps_subsecond_precision() {
    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "short", "v", "PX", "350"]).await, "+OK");
    let pttl: i64 = command(addr, &["PTTL", "short"]).await[1..].parse().unwrap();
    assert!((1..=350).contains(&pttl), "expected a positive subsecond TTL, got {pttl}");
    assert_eq!(command(addr, &["TTL", "short"]).await, ":0");

    assert_eq!(command(addr, &["SET", "reset", "v"]).await, "+OK");
    assert_eq!(command(addr, &["PEXPIRE", "reset", "300"]).await, ":1");
    let pttl: i64 = command(addr, &["PTTL", "reset"]).await[1..].parse().unwrap();
    assert!((1..=300).contains(&pttl), "expected a positive subsecond TTL, got {pttl}");

    assert_eq!(command(addr, &["SET", "getex", "v"]).await, "+OK");
    assert_eq!(command(addr, &["GETEX", "getex", "PX", "300"]).await, "$1\r\nv");
    let pttl: i64 = command(addr, &["PTTL", "getex"]).await[1..].parse().unwrap();
    assert!((1..=300).contains(&pttl), "expected a positive subsecond TTL, got {pttl}");

    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(command(addr, &["GET", "short"]).await, "$-1");
    assert_eq!(command(addr, &["GET", "reset"]).await, "$-1");
    assert_eq!(command(addr, &["GET", "getex"]).await, "$-1");
}

#[tokio::test]
async fn absolute_expiry_commands_handle_units_and_conditional_flags() {
    let addr = serve().await;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    let expires_at_secs = now.as_secs() + 10;
    assert_eq!(command(addr, &["SET", "seconds", "v"]).await, "+OK");
    assert_eq!(command(addr, &["EXPIREAT", "seconds", &expires_at_secs.to_string()]).await, ":1");
    assert_eq!(command(addr, &["EXPIRETIME", "seconds"]).await, format!(":{expires_at_secs}"));
    assert_eq!(
        command(addr, &["PEXPIRETIME", "seconds"]).await,
        format!(":{}", expires_at_secs * 1000)
    );
    let ttl: i64 = command(addr, &["TTL", "seconds"]).await[1..].parse().unwrap();
    assert!((1..=10).contains(&ttl));

    let expires_at_millis = now.as_millis() + 350;
    assert_eq!(command(addr, &["SET", "millis", "v"]).await, "+OK");
    assert_eq!(command(addr, &["PEXPIREAT", "millis", &expires_at_millis.to_string()]).await, ":1");
    assert_eq!(command(addr, &["PEXPIRETIME", "millis"]).await, format!(":{expires_at_millis}"));
    assert_eq!(
        command(addr, &["EXPIRETIME", "millis"]).await,
        format!(":{}", expires_at_millis / 1000)
    );
    let pttl: i64 = command(addr, &["PTTL", "millis"]).await[1..].parse().unwrap();
    assert!((1..=350).contains(&pttl));

    assert_eq!(command(addr, &["SET", "conditional", "v", "EX", "10"]).await, "+OK");
    assert_eq!(
        command(addr, &["EXPIREAT", "conditional", &now.as_secs().to_string(), "NX"]).await,
        ":0"
    );
    assert_eq!(command(addr, &["GET", "conditional"]).await, "$1\r\nv");
    assert_eq!(
        command(addr, &["EXPIREAT", "conditional", &now.as_secs().to_string(), "XX"]).await,
        ":1"
    );
    assert_eq!(command(addr, &["GET", "conditional"]).await, "$-1");

    assert_eq!(command(addr, &["SET", "past", "v"]).await, "+OK");
    assert_eq!(command(addr, &["EXPIREAT", "past", "0", "NX"]).await, ":1");
    assert_eq!(command(addr, &["GET", "past"]).await, "$-1");
}

#[tokio::test]
async fn expire_time_commands_distinguish_persistent_and_missing_keys() {
    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "persistent", "v"]).await, "+OK");
    assert_eq!(command(addr, &["EXPIRETIME", "persistent"]).await, ":-1");
    assert_eq!(command(addr, &["PEXPIRETIME", "persistent"]).await, ":-1");
    assert_eq!(command(addr, &["EXPIRETIME", "missing"]).await, ":-2");
    assert_eq!(command(addr, &["PEXPIRETIME", "missing"]).await, ":-2");
    assert_eq!(command(addr, &["SET", "expired", "v", "PX", "0"]).await, "+OK");
    assert_eq!(command(addr, &["EXPIRETIME", "expired"]).await, ":-2");
    assert_eq!(command(addr, &["PEXPIRETIME", "expired"]).await, ":-2");
    assert_eq!(command(addr, &["SET", "deleted", "v"]).await, "+OK");
    assert_eq!(command(addr, &["DEL", "deleted"]).await, ":1");
    assert_eq!(command(addr, &["TTL", "deleted"]).await, ":-2");
    assert_eq!(command(addr, &["PTTL", "deleted"]).await, ":-2");
    assert_eq!(command(addr, &["EXPIRETIME", "deleted"]).await, ":-2");
    assert_eq!(command(addr, &["PEXPIRETIME", "deleted"]).await, ":-2");
}

#[tokio::test]
async fn expire_modes_and_nx_xx() {
    let addr = serve().await;
    assert_eq!(command(addr, &["EXPIRE", "missing", "10"]).await, ":0");
    assert_eq!(command(addr, &["SET", "flags", "v"]).await, "+OK");
    assert_eq!(command(addr, &["EXPIRE", "flags", "10", "NX", "XX"]).await, "-ERR syntax");
    assert_eq!(command(addr, &["EXPIRE", "flags", "10", "NX", "NX"]).await, "-ERR syntax");
    assert_eq!(command(addr, &["TTL", "flags"]).await, ":-1");
    assert_eq!(command(addr, &["SET", "k", "v", "NX"]).await, "+OK");
    assert_eq!(command(addr, &["SET", "k", "v2", "NX"]).await, "$-1");
    assert_eq!(command(addr, &["SET", "k", "v2", "XX"]).await, "+OK");
    assert_eq!(command(addr, &["SET", "absent", "v", "XX"]).await, "$-1");
    assert_eq!(command(addr, &["SET", "px", "v", "PX", "5000"]).await, "+OK");
    let ttl: i64 = command(addr, &["TTL", "px"]).await[1..].parse().unwrap();
    assert!((1..=5).contains(&ttl));
    assert_eq!(command(addr, &["EXPIRE", "k", "10", "NX"]).await, ":1");
    assert_eq!(command(addr, &["EXPIRE", "k", "20", "NX"]).await, ":0");
    assert_eq!(command(addr, &["EXPIRE", "k", "20", "XX"]).await, ":1");
    assert_eq!(command(addr, &["EXPIRE", "k", "5", "GT"]).await, ":0");
    assert_eq!(command(addr, &["EXPIRE", "k", "50", "GT"]).await, ":1");
    assert_eq!(command(addr, &["EXPIRE", "k", "60", "LT"]).await, ":0");
    assert_eq!(command(addr, &["EXPIRE", "k", "5", "LT"]).await, ":1");
    assert_eq!(command(addr, &["PEXPIRE", "k", "60000"]).await, ":1");
    assert_eq!(command(addr, &["GETDEL", "k"]).await, "$2\r\nv2");
    assert_eq!(command(addr, &["GET", "k"]).await, "$-1");
    assert_eq!(command(addr, &["GETDEL", "k"]).await, "$-1");
}
