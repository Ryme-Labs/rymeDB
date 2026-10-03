use std::time::Duration;

const KEY: &str = "ryme-shard-e2e-key-1c5e8b3d7a02";

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

fn test_config(dir: &std::path::Path) -> ryme_config::Config {
    let mut base = ryme_config::Config::default();
    base.archive.interval_secs = 0;
    base.sweep_interval_secs = 0;
    base.shards = 4;
    ryme_config::Config { node_id: String::from("shard-e2e"), data_dir: dir.to_path_buf(), ..base }
}

async fn http_request(addr: std::net::SocketAddr, head: &str, body: &[u8]) -> (u16, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\nauthorization: Bearer {KEY}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 8192];
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
        }
    })
    .await
    .unwrap();
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
    let body = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|index| raw[index + 4..].to_vec())
        .unwrap_or_default();
    (status, body)
}

async fn bearer_status(addr: std::net::SocketAddr, token: &str, head: &str, body: &[u8]) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\nauthorization: Bearer {token}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 4096];
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
        }
    })
    .await
    .unwrap();
    String::from_utf8_lossy(&raw)
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse::<u16>()
        .unwrap_or(0)
}

#[tokio::test]
async fn sharded_write_move_read() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-shard-e2e-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let mut config = test_config(&root);
    config.pg_listen = pg;
    config.resp_listen = resp;
    config.http_listen = http;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    for (table, pk, value) in
        [("users", "u1", "ada"), ("users", "u2", "grace"), ("orders", "o1", "book")]
    {
        let (status, _) =
            http_request(http, &format!("PUT /v1/kv/{table}/{pk}"), value.as_bytes()).await;
        assert_eq!(status, 200);
    }
    let (status, body) = http_request(http, "GET /v1/shards", b"").await;
    assert_eq!(status, 200);
    let layout = String::from_utf8_lossy(&body).into_owned();
    assert!(layout.contains("\"mode\":\"sharded\""));
    assert!(layout.contains("users"));
    let (status, body) = http_request(http, "POST /v1/shards/move", b"{\"table\":\"users\"}").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let (status, _) =
        http_request(http, "POST /v1/shards/move", b"{\"table\":\"users\",\"tenant\":\"evil\"}")
            .await;
    assert_eq!(status, 403);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/register",
        b"{\"id\":\"app\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 201);
    let (status, body) = http_request(
        http,
        "POST /v1/auth/token",
        b"{\"id\":\"app\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 201);
    let app_key = serde_json::from_slice::<serde_json::Value>(&body)
        .unwrap()
        .get("key")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    let status =
        bearer_status(http, &app_key, "POST /v1/shards/move", b"{\"table\":\"users\"}").await;
    assert_eq!(status, 403);
    let (status, body) = http_request(http, "GET /v1/kv/users/u1", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body, b"ada");
    let (status, body) = http_request(http, "GET /v1/kv/users/u2", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body, b"grace");
    let (status, _) = http_request(http, "PUT /v1/kv/users/u3", b"hopper").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/kv/users/u3", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body, b"hopper");
    let (status, body) = http_request(http, "GET /v1/kv/orders/o1", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body, b"book");
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
