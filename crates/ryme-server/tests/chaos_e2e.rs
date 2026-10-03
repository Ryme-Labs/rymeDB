use ryme_config::Config;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: &str = "ryme-chaos-key-4d8f2c9a6e31";

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

async fn http_request(addr: std::net::SocketAddr, head: &str, body: &[u8]) -> (u16, Vec<u8>) {
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

async fn spawn_server(
    config: Config,
    pg_listener: tokio::net::TcpListener,
    resp_listener: tokio::net::TcpListener,
    http_listener: tokio::net::TcpListener,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    })
}

fn config_for(
    root: &std::path::Path,
    pg: std::net::SocketAddr,
    resp: std::net::SocketAddr,
    http: std::net::SocketAddr,
) -> Config {
    Config {
        node_id: String::from("chaos"),
        data_dir: root.to_path_buf(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    }
}

async fn bearer_request(addr: std::net::SocketAddr, token: &str, head: &str, body: &[u8]) -> u16 {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\nauthorization: Bearer {token}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
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

async fn app_key_for(addr: std::net::SocketAddr, id: &str) -> String {
    let body = format!("{{\"id\":\"{id}\",\"password\":\"correct-horse\"}}");
    let (status, _) = http_request(addr, "POST /v1/auth/register", body.as_bytes()).await;
    assert_eq!(status, 201);
    let (status, raw) = http_request(addr, "POST /v1/auth/token", body.as_bytes()).await;
    assert_eq!(status, 201);
    let parsed: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    parsed.get("key").and_then(|v| v.as_str()).unwrap().to_string()
}

#[tokio::test]
async fn chaos_restart_preserves_commits() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-chaos-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let server =
        spawn_server(config_for(&root, pg, resp, http), pg_listener, resp_listener, http_listener)
            .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    for index in 0..50 {
        let body = format!(
            "{{\"table\":\"docs\",\"rows\":[{{\"key\":\"k{index}\",\"value\":\"v{index}\"}}]}}"
        );
        let (status, _) = http_request(http, "POST /v1/sql/copy", body.as_bytes()).await;
        assert_eq!(status, 200);
    }
    let (status, _) = http_request(http, "POST /v1/backups/checkpoint", b"{}").await;
    assert_eq!(status, 200);
    server.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let server =
        spawn_server(config_for(&root, pg, resp, http), pg_listener, resp_listener, http_listener)
            .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, body) = http_request(http, "GET /rest/v1/docs?limit=100", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("k0"), "{text}");
    assert!(text.contains("k49"), "{text}");
    let (status, _) = http_request(http, "POST /v1/backups/checkpoint", b"{}").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/backups/latest", b"").await;
    assert_eq!(status, 200);
    assert!(!body.is_empty());
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn chaos_qos_shields_tenant() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-chaos-qos-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let server =
        spawn_server(config_for(&root, pg, resp, http), pg_listener, resp_listener, http_listener)
            .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) =
        http_request(http, "POST /v1/qos/tier", b"{\"tenant\":\"default\",\"tier\":\"shared\"}")
            .await;
    assert_eq!(status, 200);
    let (status, _) =
        http_request(http, "POST /v1/qos/tier", b"{\"tenant\":\"evil\",\"tier\":\"shared\"}").await;
    assert_eq!(status, 403);
    let app_key = app_key_for(http, "app").await;
    let status = bearer_request(
        http,
        &app_key,
        "POST /v1/qos/tier",
        b"{\"tenant\":\"default\",\"tier\":\"shared\"}",
    )
    .await;
    assert_eq!(status, 403);
    let (status, body) = http_request(http, "GET /v1/qos", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("default"), "{text}");
    let (status, _) = http_request(http, "PUT /v1/kv/docs/victim", b"{\"ok\":true}").await;
    assert_eq!(status, 200);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}
