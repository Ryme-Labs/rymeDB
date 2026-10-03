use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_response(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 4096];
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match socket.read(&mut chunk).await {
                Ok(0) => break,
                Ok(read) => {
                    raw.extend_from_slice(&chunk[..read]);
                    if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
    .await;
    raw
}

#[tokio::test]
async fn http_connection_cap_sheds_excess() {
    let app = axum::Router::new().route(
        "/hold",
        axum::routing::get(|| async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            "ok"
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = ryme_server::serve_http_with_listener(listener, app, 1).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut first = tokio::net::TcpStream::connect(addr).await.unwrap();
    first.write_all(b"GET /hold HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut second = tokio::net::TcpStream::connect(addr).await.unwrap();
    second.write_all(b"GET /hold HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n").await.unwrap();
    let body = read_response(&mut second).await;
    assert!(body.is_empty(), "{body:?}");
    let body = read_response(&mut first).await;
    assert!(String::from_utf8_lossy(&body).contains("200"), "{body:?}");
}

#[tokio::test]
async fn http_body_limit_rejects_oversize() {
    std::env::set_var("RYME_API_KEY", "ryme-admit-key");
    let root = std::env::temp_dir().join(format!(
        "ryme-admit-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = http_listener.local_addr().unwrap();
    let config = ryme_config::Config {
        node_id: String::from("admit"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..ryme_config::Config::default()
    };
    tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let big = "x".repeat(3 * 1024 * 1024);
    let body = format!("{{\"sql\":\"{big}\"}}");
    let mut socket = tokio::net::TcpStream::connect(http).await.unwrap();
    let head = format!(
        "POST /v1/sql HTTP/1.1\r\nhost: x\r\nauthorization: Bearer ryme-admit-key\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    socket.write_all(body.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 8192];
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match socket.read(&mut chunk).await {
                Ok(0) => break,
                Ok(read) => raw.extend_from_slice(&chunk[..read]),
                Err(_) => break,
            }
        }
    })
    .await;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse::<u16>()
        .unwrap_or(0);
    assert_eq!(status, 413, "{text}");
    let _ = std::fs::remove_dir_all(&root);
}
