use ryme_config::Config;
use ryme_txn::{DurableManager, SyncPolicy};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: &str = "ryme-e2e-key-7f3a9c1e5b24";

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
        node_id: String::from("e2e"),
        data_dir: dir.to_path_buf(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        archive: base.archive,
        ..Config::default()
    }
}

#[tokio::test]
async fn malformed_json_bodies_return_400() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-e2e-400-{}-{}", std::process::id(), now_ms()));
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
    for head in [
        "POST /v1/sql",
        "POST /v1/sql/copy",
        "POST /v1/sql/explain",
        "POST /graphql",
        "POST /v1/branches",
        "POST /v1/qos/tier",
        "POST /rest/v1/docs",
        "POST /v1/vector/upsert",
        "POST /v1/topics/append",
        "POST /v1/broadcast",
        "POST /v1/migrate/supabase",
    ] {
        let (status, _) = http_request(http, head, b"{oops").await;
        assert_eq!(status, 400, "{head}");
    }
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
    String::from_utf8(raw).unwrap()
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

async fn http_status_no_auth(addr: std::net::SocketAddr, head: &str) -> u16 {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\ncontent-type: application/json\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
    );
    socket.write_all(request.as_bytes()).await.unwrap();
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

async fn bearer_status(addr: std::net::SocketAddr, token: &str, head: &str, body: &[u8]) -> u16 {
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
async fn e2e_verify_requires_write() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-e2e-verify-{}-{}", std::process::id(), now_ms()));
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
    let body = "{\"id\":\"ro\",\"password\":\"correct-horse\",\"roles\":[\"readonly\"]}";
    let (status, _) = http_request(http, "POST /v1/auth/register", body.as_bytes()).await;
    assert_eq!(status, 201);
    let (status, raw) = http_request(http, "POST /v1/auth/token", body.as_bytes()).await;
    assert_eq!(status, 201);
    let ro_key = serde_json::from_slice::<serde_json::Value>(&raw)
        .unwrap()
        .get("key")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    let status = bearer_status(http, &ro_key, "GET /v1/backups/verify?backup_id=x", b"").await;
    assert_eq!(status, 403);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn e2e_scan_pagination_bounds() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-e2e-bounds-{}-{}", std::process::id(), now_ms()));
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
    let seed = "{\"table\":\"bound\",\"rows\":[{\"key\":\"a\",\"value\":\"1\"},{\"key\":\"b\",\"value\":\"2\"},{\"key\":\"c\",\"value\":\"3\"},{\"key\":\"d\",\"value\":\"4\"},{\"key\":\"e\",\"value\":\"5\"}]}";
    let (status, _) = http_request(http, "POST /v1/sql/copy", seed.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, body) =
        http_request(http, "GET /rest/v1/bound?limit=100&offset=99999999", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body, b"[]");
    let (status, body) = http_request(http, "GET /v1/scan/bound?limit=999999", b"").await;
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["rows"].as_array().map(|rows| rows.len()), Some(5));
    let (status, body) = http_request(http, "GET /v1/scan/bound?limit=0", b"").await;
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(parsed["rows"].as_array().map(|rows| rows.len()).unwrap_or(0) >= 1);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn e2e_resp_rest_and_recovery() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!("ryme-e2e-{}-{}", std::process::id(), now_ms()));
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
    let set_reply = resp_command(resp, &["SET", "hello", "world"]).await;
    assert!(set_reply.starts_with("+OK"), "{set_reply}");
    let get_reply = resp_command(resp, &["GET", "hello"]).await;
    assert!(get_reply.contains("world"), "{get_reply}");
    let (status, _) = http_request(http, "PUT /v1/kv/docs/doc1", b"{\"a\":1}").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/kv/docs/doc1", b"").await;
    assert_eq!(status, 200);
    assert!(body.windows(7).any(|w| w == b"{\"a\":1}"));
    let (status, _) = http_request(http, "PUT /v1/kv/docs/gone?ttl=0", b"x").await;
    assert_eq!(status, 200);
    let (status, _) = http_request(http, "GET /v1/kv/docs/gone", b"").await;
    assert_eq!(status, 404);
    let (status, _) = http_request(http, "PUT /v1/kv/docs/live?ttl=100", b"y").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/kv/docs/live/ttl", b"").await;
    assert_eq!(status, 200);
    let ttl: i64 = String::from_utf8_lossy(&body)
        .replace(|c: char| !c.is_ascii_digit() && c != '-', "")
        .parse()
        .unwrap();
    assert!((1..=100).contains(&ttl), "{ttl}");
    let (status, _) = http_request(http, "GET /v1/kv/docs/doc1/ttl", b"").await;
    assert_eq!(status, 200);
    let (status, body) =
        http_request(http, "POST /v1/sql", b"{\"sql\":\"SELECT * FROM docs KEY 'doc1'\"}").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("doc1"));
    let (status, _) = http_request(
        http,
        "POST /v1/branches",
        b"{\"id\":\"preview-1\",\"parent\":\"main\",\"base_commit_ts\":1}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = http_request(http, "POST /v1/backups/checkpoint", b"{}").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/backups/latest", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("ckpt-"));
    assert_eq!(http_status_no_auth(http, "GET /v1/backups/latest").await, 401);
    assert_eq!(http_status_no_auth(http, "GET /v1/backups/pitr?target=1").await, 401);
    let (status, _) = http_request(http, "GET /metrics", b"").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /dashboard", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("rymeDB Admin"), "{text}");
    assert!(text.contains("/v1/sql"), "{text}");
    let (status, body) =
        http_request(http, "POST /v1/backups/archive", b"{\"backup_id\":\"e2e-1\"}").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert!(String::from_utf8_lossy(&body).contains("e2e-1"));
    let (status, body) = http_request(http, "GET /v1/backups/archives", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("e2e-1"));
    let (status, body) = http_request(http, "GET /v1/backups/verify?backup_id=e2e-1", b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("verified"), "{text}");
    assert!(!text.contains("\"verified\":0"), "{text}");
    assert!(!text.contains("\"encrypted\":0"), "{text}");
    assert!(text.contains("e2e-1"), "{text}");
    let (status, _) = http_request(http, "GET /v1/backups/verify?backup_id=ghost", b"").await;
    assert_eq!(status, 404);
    let (status, body) = http_request(http, "POST /v1/snapshots", b"").await;
    assert_eq!(status, 200);
    let snapshot_commit = snapshot_commit(&body);
    let (status, _) = http_request(http, "PUT /v1/kv/docs/after-snap", b"late").await;
    assert_eq!(status, 200);
    let (status, body) =
        http_request(http, &format!("POST /v1/backups/restore?target={snapshot_commit}"), b"")
            .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let (status, _) = http_request(http, "GET /v1/kv/_kv/hello", b"").await;
    assert_eq!(status, 200);
    let (status, _) = http_request(http, "GET /v1/kv/docs/after-snap", b"").await;
    assert_eq!(status, 404);
    server.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let wal_dir = root.join("wal");
    let reopened = DurableManager::open(&wal_dir, 64 * 1024 * 1024, SyncPolicy::Always).unwrap();
    let mut txn = reopened.begin();
    let key = ryme_storage::RecordKey::new("default", "default", "_kv", b"hello");
    let value = reopened.get(&mut txn, &key).unwrap();
    assert_eq!(value, Some(b"world".to_vec()));
    let _ = std::fs::remove_dir_all(&root);
}

fn snapshot_commit(body: &[u8]) -> u64 {
    let text = String::from_utf8_lossy(body).into_owned();
    let marker = "\"commit\":";
    let start = text.find(marker).unwrap() + marker.len();
    text[start..].split(|c: char| !c.is_ascii_digit()).next().unwrap().parse().unwrap()
}

fn checkpoint_commit(body: &[u8]) -> u64 {
    let text = String::from_utf8_lossy(body).into_owned();
    let marker = "\"commit_ts\":";
    let start = text.find(marker).unwrap() + marker.len();
    text[start..].split(|c: char| !c.is_ascii_digit()).next().unwrap().parse().unwrap()
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[tokio::test]
async fn e2e_pitr_restore_roundtrip() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-e2e-pitr-{}-{}", std::process::id(), now_ms()));
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
    let (status, _) = http_request(http, "PUT /v1/kv/docs/before", b"v-before").await;
    assert_eq!(status, 200);
    let (status, _) = http_request(http, "POST /v1/snapshots", b"").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "POST /v1/backups/checkpoint", b"{}").await;
    assert_eq!(status, 200);
    let target = checkpoint_commit(&body);
    server.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let pg_listener = bind_listener().await;
    let resp_listener = bind_listener().await;
    let http_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp = resp_listener.local_addr().unwrap();
    let http = http_listener.local_addr().unwrap();
    let config = test_config(&root, pg, resp, http);
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(http, "PUT /v1/kv/docs/after", b"v-after").await;
    assert_eq!(status, 200);
    let (status, body) =
        http_request(http, &format!("POST /v1/backups/restore?target={target}"), b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let (status, body) = http_request(http, "GET /v1/kv/docs/before", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body, b"v-before");
    let (status, body) = http_request(http, "GET /v1/kv/docs/after", b"").await;
    assert_eq!(status, 404, "{}", String::from_utf8_lossy(&body));
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn e2e_scheduled_drill_verifies_latest_backup() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-e2e-drill-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let mut config = test_config(&root, pg, resp, http);
    config.archive.verify_interval_secs = 1;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(http, "GET /v1/backups/drill", b"").await;
    assert_eq!(status, 404);
    assert_eq!(http_status_no_auth(http, "GET /v1/backups/drill").await, 401);
    let (status, _) = http_request(http, "PUT /v1/kv/docs/drill", b"{\"n\":1}").await;
    assert_eq!(status, 200);
    let (status, _) = http_request(http, "POST /v1/backups/checkpoint", b"{}").await;
    assert_eq!(status, 200);
    let (status, body) =
        http_request(http, "POST /v1/backups/archive", b"{\"backup_id\":\"drill-1\"}").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (status, body) = http_request(http, "GET /v1/backups/drill", b"").await;
        if status == 200 {
            let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(parsed.get("backup_id").and_then(|v| v.as_str()), Some("drill-1"));
            assert!(parsed.get("error").map(|v| v.is_null()).unwrap_or(false), "{parsed}");
            assert!(
                parsed.get("verified_files").and_then(|v| v.as_u64()).unwrap_or(0) > 0,
                "{parsed}"
            );
            break;
        }
        assert_eq!(status, 404);
        assert!(tokio::time::Instant::now() < deadline, "no drill completed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

fn corrupt_newest_archive_file(root: &std::path::Path) {
    let mut stack = vec![root.join("archive")];
    let mut best: Option<(u64, std::path::PathBuf)> = None;
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.file_name().and_then(|n| n.to_str()) == Some("manifest.json") {
                continue;
            }
            let size = path.metadata().map(|m| m.len()).unwrap_or(0);
            if best.as_ref().map(|(s, _)| size > *s).unwrap_or(true) {
                best = Some((size, path));
            }
        }
    }
    let (_, path) = best.expect("archived segment file");
    let mut bytes = std::fs::read(&path).unwrap();
    assert!(!bytes.is_empty());
    bytes[0] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
}

#[tokio::test]
async fn e2e_drill_reports_corruption() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-e2e-corrupt-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let mut config = test_config(&root, pg, resp, http);
    config.archive.verify_interval_secs = 1;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(http, "PUT /v1/kv/docs/corrupt", b"{\"n\":1}").await;
    assert_eq!(status, 200);
    let (status, _) = http_request(http, "POST /v1/backups/checkpoint", b"{}").await;
    assert_eq!(status, 200);
    let (status, body) =
        http_request(http, "POST /v1/backups/archive", b"{\"backup_id\":\"bad-1\"}").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    corrupt_newest_archive_file(&root);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (status, body) = http_request(http, "GET /v1/backups/drill", b"").await;
        if status == 200 {
            let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(parsed.get("backup_id").and_then(|v| v.as_str()), Some("bad-1"));
            assert!(
                parsed
                    .get("error")
                    .and_then(|v| v.as_str())
                    .map(|e| !e.is_empty())
                    .unwrap_or(false),
                "{parsed}"
            );
            break;
        }
        assert_eq!(status, 404);
        assert!(tokio::time::Instant::now() < deadline, "no drill completed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn e2e_read_egress_throttled() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-e2e-egress-{}-{}", std::process::id(), now_ms()));
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
    let big = "v".repeat(1024 * 1024);
    let (status, _) = http_request(http, "PUT /v1/kv/docs/big", big.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, _) = http_request(http, "GET /v1/kv/docs/big", b"").await;
    assert_eq!(status, 200);
    let mut denied = false;
    for _ in 0..7 {
        let (status, _) = http_request(http, "GET /v1/kv/docs/big", b"").await;
        if status == 429 {
            denied = true;
            break;
        }
        assert_eq!(status, 200);
    }
    assert!(denied, "shared egress bucket never throttled 1MiB reads");
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn e2e_backup_copy_to_replica() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-e2e-copy-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let mut config = test_config(&root, pg, resp, http);
    config.archive_replica = Some(ryme_config::ArchiveConfig {
        local_dir: Some(root.join("archive-replica")),
        ..Default::default()
    });
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(http, "PUT /v1/kv/docs/copy-me", b"{\"n\":1}").await;
    assert_eq!(status, 200);
    let (status, _) = http_request(http, "POST /v1/backups/checkpoint", b"{}").await;
    assert_eq!(status, 200);
    let (status, body) =
        http_request(http, "POST /v1/backups/archive", b"{\"backup_id\":\"copy-1\"}").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let (status, body) =
        http_request(http, "POST /v1/backups/copy", b"{\"backup_id\":\"copy-1\"}").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert!(String::from_utf8_lossy(&body).contains("copy-1"));
    let (status, _) =
        http_request(http, "POST /v1/backups/copy", b"{\"backup_id\":\"ghost\"}").await;
    assert_eq!(status, 404);
    let app_key = app_key_for(http, "app").await;
    let status = bearer_status(http, &app_key, "POST /v1/backups/archive", b"{}").await;
    assert_eq!(status, 403);
    let status =
        bearer_status(http, &app_key, "POST /v1/backups/copy", b"{\"backup_id\":\"copy-1\"}").await;
    assert_eq!(status, 403);
    let replica_files: Vec<_> = walkdir_files(&root.join("archive-replica"));
    assert!(replica_files.iter().any(|f| f.ends_with("manifest.json")), "{replica_files:?}");
    assert!(replica_files.len() >= 2, "{replica_files:?}");
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

fn walkdir_files(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Some(name) = path.to_str() {
                out.push(name.to_string());
            }
        }
    }
    out.sort();
    out
}
