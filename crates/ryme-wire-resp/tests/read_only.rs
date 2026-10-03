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

async fn serve_read_only() -> std::net::SocketAddr {
    let gateway =
        ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d")).with_read_only(true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    addr
}

#[tokio::test]
async fn read_only_rejects_writes_allows_reads() {
    let addr = serve_read_only().await;
    assert_eq!(command(addr, &["PING"]).await, "+PONG");
    let denied = command(addr, &["SET", "k", "v"]).await;
    assert!(denied.contains("READONLY"), "{denied}");
    assert_eq!(command(addr, &["GET", "k"]).await, "$-1");
    let denied = command(addr, &["INCR", "counter"]).await;
    assert!(denied.contains("READONLY"), "{denied}");
}
