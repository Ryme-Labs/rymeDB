use ryme_config::Config;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: &str = "ryme-ranges-key-9c4e1a2b7d30";

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

fn test_config(
    dir: &std::path::Path,
    pg: std::net::SocketAddr,
    resp: std::net::SocketAddr,
    http: std::net::SocketAddr,
) -> Config {
    let mut base = Config::default();
    base.archive.local_dir = Some(dir.join("archive"));
    base.archive.interval_secs = 0;
    Config {
        node_id: String::from("ranges"),
        data_dir: dir.to_path_buf(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        archive: base.archive,
        ..Config::default()
    }
}

async fn http_request(
    addr: std::net::SocketAddr,
    head: &str,
    body: &[u8],
) -> (u16, serde_json::Value) {
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
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    (status, json)
}

async fn http_request_no_auth(
    addr: std::net::SocketAddr,
    head: &str,
    body: &[u8],
) -> (u16, serde_json::Value) {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
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
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    (status, json)
}

async fn bearer_request(
    addr: std::net::SocketAddr,
    token: &str,
    head: &str,
    body: &[u8],
) -> (u16, serde_json::Value) {
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
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    (status, json)
}

#[tokio::test]
async fn ranges_reject_non_admin_topology() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-ranges-rbac-{}-{}",
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
    let config = test_config(&root, pg, resp, http);
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
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
    let app_key = body.get("key").and_then(|v| v.as_str()).unwrap().to_string();
    let split = serde_json::json!({
        "id": "range-0",
        "mid": "m",
        "left_id": "range-a",
        "right_id": "range-b",
        "expected_epoch": 0,
    });
    let (status, _) =
        bearer_request(http, &app_key, "POST /v1/ranges/split", split.to_string().as_bytes()).await;
    assert_eq!(status, 403);
    let merge = serde_json::json!({
        "left_id": "range-a",
        "right_id": "range-b",
        "merged_id": "range-c",
        "expected_left_epoch": 0,
        "expected_right_epoch": 0,
    });
    let (status, _) =
        bearer_request(http, &app_key, "POST /v1/ranges/merge", merge.to_string().as_bytes()).await;
    assert_eq!(status, 403);
    let (status, _) = bearer_request(http, &app_key, "POST /v1/ranges/autosplit", b"").await;
    assert_eq!(status, 403);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn ranges_split_merge_lifecycle() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-ranges-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let config = test_config(&root, pg, resp, http);
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, body) = http_request(http, "GET /v1/ranges", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(body[0]["id"], "range-0");
    assert_eq!(body[0]["epoch"], 0);
    let split = serde_json::json!({
        "id": "range-0",
        "mid": "m",
        "left_id": "range-a",
        "right_id": "range-b",
        "expected_epoch": 0,
    });
    let (status, body) =
        http_request(http, "POST /v1/ranges/split", split.to_string().as_bytes()).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(2));
    assert_eq!(body[0]["epoch"], 1);
    let (status, body) = http_request(http, "GET /v1/ranges?key=a", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], "range-a");
    let (status, body) = http_request(http, "GET /v1/ranges?key=z", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], "range-b");
    let stale_split = serde_json::json!({
        "id": "range-a",
        "mid": "f",
        "left_id": "range-a1",
        "right_id": "range-a2",
        "expected_epoch": 0,
    });
    let (status, body) =
        http_request(http, "POST /v1/ranges/split", stale_split.to_string().as_bytes()).await;
    assert_eq!(status, 409);
    assert!(body["error"].as_str().unwrap_or("").contains("stale epoch"));
    let stale_merge = serde_json::json!({
        "left_id": "range-a",
        "right_id": "range-b",
        "merged_id": "range-c",
        "expected_left_epoch": 0,
        "expected_right_epoch": 1,
    });
    let (status, _) =
        http_request(http, "POST /v1/ranges/merge", stale_merge.to_string().as_bytes()).await;
    assert_eq!(status, 409);
    let merge = serde_json::json!({
        "left_id": "range-a",
        "right_id": "range-b",
        "merged_id": "range-c",
        "expected_left_epoch": 1,
        "expected_right_epoch": 1,
    });
    let (status, body) =
        http_request(http, "POST /v1/ranges/merge", merge.to_string().as_bytes()).await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], "range-c");
    assert_eq!(body["epoch"], 2);
    let (status, body) = http_request(http, "GET /v1/ranges", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let _ = std::fs::remove_dir_all(&root);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[tokio::test]
async fn ranges_autosplit_fires_on_write_load() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-autosplit-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let mut config = test_config(&root, pg, resp, http);
    config.autosplit_writes = 3;
    config.autosplit_interval_secs = 3600;
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, body) = http_request(http, "POST /v1/ranges/autosplit", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body["split"].as_array().map(Vec::len), Some(0));
    for index in 1..=3 {
        let (status, _) =
            http_request(http, &format!("PUT /v1/kv/t/k{index}"), format!("v{index}").as_bytes())
                .await;
        assert_eq!(status, 200);
    }
    let (status, body) = http_request(http, "POST /v1/ranges/autosplit", b"").await;
    assert_eq!(status, 200);
    let split = body["split"].as_array().cloned().unwrap_or_default();
    assert_eq!(split.len(), 2);
    assert_eq!(split[0], "range-0-a-0");
    assert_eq!(split[1], "range-0-b-0");
    let (status, body) = http_request(http, "GET /v1/ranges", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(2));
    assert!(body.as_array().unwrap().iter().all(|range| range["epoch"] == 1));
    let (status, body) = http_request(http, "POST /v1/ranges/autosplit", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body["split"].as_array().map(Vec::len), Some(0));
    let _ = std::fs::remove_dir_all(&root);
}

async fn resp_command(addr: std::net::SocketAddr, parts: &[&str]) -> String {
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

#[tokio::test]
async fn ranges_autosplit_counts_wire_gateway_writes() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-autosplit-wire-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let mut config = test_config(&root, pg, resp, http);
    config.autosplit_writes = 4;
    config.autosplit_interval_secs = 3600;
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(http, "PUT /v1/kv/t/w1", b"v1").await;
    assert_eq!(status, 200);
    assert_eq!(resp_command(resp, &["SET", "w2", "v2"]).await, "+OK");
    assert_eq!(resp_command(resp, &["SET", "w3", "v3"]).await, "+OK");
    assert_eq!(resp_command(resp, &["INCR", "counter"]).await, ":1");
    let (status, body) = http_request(http, "GET /v1/ranges/loads", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(body[0]["id"], "range-0");
    assert_eq!(body[0]["epoch"], 0);
    assert_eq!(body[0]["writes"], 4);
    let (status, body) = http_request(http, "POST /v1/ranges/autosplit", b"").await;
    assert_eq!(status, 200);
    let split = body["split"].as_array().cloned().unwrap_or_default();
    assert_eq!(split.len(), 2);
    assert_eq!(split[0], "range-0-a-0");
    assert_eq!(split[1], "range-0-b-0");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn ranges_autosplit_zero_threshold_splits_nothing() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-autosplit-zero-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let config = test_config(&root, pg, resp, http);
    assert_eq!(config.autosplit_writes, 0);
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, body) = http_request(http, "POST /v1/ranges/autosplit", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body["split"].as_array().map(Vec::len), Some(0));
    let (status, body) = http_request(http, "POST /v1/ranges/autosplit?min_writes=0", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body["split"].as_array().map(Vec::len), Some(0));
    let (status, body) = http_request(http, "GET /v1/ranges", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let _ = std::fs::remove_dir_all(&root);
}
#[tokio::test]
async fn ranges_autosplit_background_loop_splits_without_trigger() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-autosplit-loop-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let mut config = test_config(&root, pg, resp, http);
    config.autosplit_writes = 2;
    config.autosplit_interval_secs = 1;
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    for index in 1..=2 {
        let (status, _) =
            http_request(http, &format!("PUT /v1/kv/t/b{index}"), format!("v{index}").as_bytes())
                .await;
        assert_eq!(status, 200);
    }
    let mut ranges = serde_json::Value::Null;
    for _ in 0..30 {
        let (status, body) = http_request(http, "GET /v1/ranges", b"").await;
        assert_eq!(status, 200);
        if body.as_array().map(Vec::len) == Some(2) {
            ranges = body;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(ranges.as_array().map(Vec::len), Some(2));
    assert!(ranges.as_array().unwrap().iter().all(|range| range["epoch"] == 1));
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn ranges_endpoints_reject_unauthenticated_callers() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-ranges-auth-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let config = test_config(&root, pg, resp, http);
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    for (head, body) in [
        ("GET /v1/ranges", [].as_slice()),
        ("GET /v1/ranges/loads", [].as_slice()),
        ("GET /v1/ranges?key=a", [].as_slice()),
        ("POST /v1/ranges/autosplit", [].as_slice()),
        ("POST /v1/ranges/split", b"{}".as_slice()),
        ("POST /v1/ranges/merge", b"{}".as_slice()),
    ] {
        let (status, _) = http_request_no_auth(http, head, body).await;
        assert_eq!(status, 401, "{head}");
    }
    for head in ["POST /v1/ranges/split", "POST /v1/ranges/merge"] {
        let (status, _) = http_request(http, head, b"{}".as_slice()).await;
        assert_eq!(status, 400, "{head}");
    }
    let _ = std::fs::remove_dir_all(&root);
}
