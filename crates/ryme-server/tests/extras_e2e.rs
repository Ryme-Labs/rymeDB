use ryme_config::Config;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: &str = "ryme-extras-key-7b3e9d1a5f42";

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

async fn native_roundtrip(addr: std::net::SocketAddr) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let attempt = async {
            let mut socket = tokio::net::TcpStream::connect(addr).await?;
            let put = serde_json::json!({
                "key": KEY,
                "op": "put",
                "table": "docs",
                "pk": "n1",
                "value": "v1",
            });
            let raw = serde_json::to_vec(&put).unwrap();
            let mut frame = (raw.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&raw);
            socket.write_all(&frame).await?;
            let mut header = [0u8; 4];
            socket.read_exact(&mut header).await?;
            let len = u32::from_be_bytes(header) as usize;
            let mut body = vec![0u8; len];
            socket.read_exact(&mut body).await?;
            let response: serde_json::Value = serde_json::from_slice(&body)?;
            assert_eq!(response.get("ok"), Some(&serde_json::Value::Bool(true)));
            let get = serde_json::json!({
                "key": KEY,
                "op": "get",
                "table": "docs",
                "pk": "n1",
            });
            let raw = serde_json::to_vec(&get).unwrap();
            let mut frame = (raw.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&raw);
            socket.write_all(&frame).await?;
            socket.read_exact(&mut header).await?;
            let len = u32::from_be_bytes(header) as usize;
            let mut body = vec![0u8; len];
            socket.read_exact(&mut body).await?;
            let response: serde_json::Value = serde_json::from_slice(&body)?;
            assert_eq!(response.get("value"), Some(&serde_json::Value::String(String::from("v1"))));
            Ok::<(), std::io::Error>(())
        }
        .await;
        match attempt {
            Ok(()) => return,
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("native roundtrip failed: {e}"),
        }
    }
}

#[tokio::test]
async fn extras_auth_presence_topics_mask() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-{}-{}",
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
    let native_listener = bind_listener().await;
    let native = native_listener.local_addr().unwrap();
    let config = Config {
        node_id: String::from("extras"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        native_listen: Some(native),
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve_with_native(
            config,
            pg_listener,
            resp_listener,
            http_listener,
            native_listener,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (status, _) = http_request(
        http,
        "POST /v1/auth/register",
        b"{\"id\":\"ada\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 201);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/verify",
        b"{\"id\":\"ada\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) =
        http_request(http, "POST /v1/auth/verify", b"{\"id\":\"ada\",\"password\":\"wrong-pass\"}")
            .await;
    assert_eq!(status, 401);
    let huge = "p".repeat(300);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/verify",
        format!("{{\"id\":\"ada\",\"password\":\"{huge}\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/register",
        format!("{{\"id\":\"big\",\"password\":\"{huge}\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) =
        http_request(http, "PUT /v1/kv/users/u1", b"{\"name\":\"ada\",\"ssn\":\"123\"}").await;
    assert_eq!(status, 200);
    let (status, _) =
        http_request(http, "POST /v1/auth/mask", b"{\"table\":\"users\",\"fields\":[\"ssn\"]}")
            .await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/kv/users/u1", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("***"), "{text}");
    assert!(!text.contains("123"), "{text}");
    let (status, _) = http_request(
        http,
        "POST /v1/presence/join",
        b"{\"channel\":\"room:1\",\"member\":\"ada\",\"ttl_secs\":60}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/presence/room:1", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("ada"));
    let (status, _) = http_request(
        http,
        "POST /v1/broadcast",
        b"{\"channel\":\"room:1\",\"payload\":{\"hello\":true}}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = http_request(
        http,
        "POST /v1/topics/append",
        b"{\"partition\":\"orders\",\"key\":\"k1\",\"value\":\"v1\"}",
    )
    .await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("cursor"));
    let (status, body) =
        http_request(http, "GET /v1/topics/read?partition=orders&from=0&limit=10", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("k1"));
    let big_key = "k".repeat(2048);
    let (status, _) = http_request(
        http,
        "POST /v1/topics/append",
        format!("{{\"partition\":\"orders\",\"key\":\"{big_key}\",\"value\":\"v\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 400);
    let big_name = "n".repeat(300);
    let (status, _) = http_request(
        http,
        "POST /v1/presence/join",
        format!("{{\"channel\":\"{big_name}\",\"member\":\"ada\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = http_request(
        http,
        "POST /v1/presence/join",
        format!("{{\"channel\":\"room:1\",\"member\":\"{big_name}\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 400);
    let big_state = "s".repeat(5000);
    let (status, _) = http_request(
        http,
        "POST /v1/presence/join",
        format!("{{\"channel\":\"room:1\",\"member\":\"ada\",\"state\":\"{big_state}\"}}")
            .as_bytes(),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = http_request(
        http,
        "POST /v1/broadcast",
        format!("{{\"channel\":\"{big_name}\",\"payload\":true}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = http_request(
        http,
        "POST /v1/topics/append",
        format!("{{\"partition\":\"{big_name}\",\"key\":\"k\",\"value\":\"v\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) =
        http_request(http, "POST /v1/auth/passkey/challenge", b"{\"user\":\"ada\"}").await;
    assert_eq!(status, 200);
    native_roundtrip(native).await;
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_branch_lifecycle_and_billing() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-branch-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-branch"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let restart_config = config.clone();
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(
        http,
        "POST /v1/branches",
        b"{\"id\":\"preview-9\",\"parent\":\"main\",\"base_commit_ts\":3}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/branches", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("preview-9"), "{text}");
    let (status, body) = http_request(http, "GET /v1/branches/preview-9", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("preview-9"));
    assert_eq!(http_status_no_auth(http, "GET /v1/branches/preview-9").await, 401);
    assert_eq!(http_status_no_auth(http, "GET /v1/branches").await, 401);
    let (status, body) =
        http_request(http, "GET /v1/branches/preview-9/diff?against=main", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("only_left"));
    let (status, body) =
        http_request(http, "POST /v1/branches/preview-9/reset", b"{\"base_commit_ts\":9}").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains('9'));
    let (status, body) = http_request(http, "POST /v1/branches/preview-9/promote", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("main"));
    let (status, _) = http_request(http, "PUT /v1/kv/docs/b1", b"x").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/billing/summary", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("default"));
    server.abort();

    let pg_listener = bind_listener().await;
    let resp_listener = bind_listener().await;
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(restart_config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, body) = http_request(http, "GET /v1/branches/preview-9", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("preview-9"));
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_branch_metadata_latency() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-branch-lat-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-branch-lat"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    for index in 0..1000 {
        let (status, _) = http_request(
            http,
            &format!("PUT /v1/kv/docs/seed-{index}"),
            format!("{{\"n\":{index}}}").as_bytes(),
        )
        .await;
        assert_eq!(status, 200);
    }
    let (status, _) = http_request(
        http,
        "POST /v1/branches",
        b"{\"id\":\"lat-warm\",\"parent\":\"main\",\"base_commit_ts\":1}",
    )
    .await;
    assert_eq!(status, 200);
    let mut creates = Vec::new();
    for index in 0..5 {
        let id = format!("lat-{index}");
        let start = std::time::Instant::now();
        let (status, _) = http_request(
            http,
            "POST /v1/branches",
            format!("{{\"id\":\"{id}\",\"parent\":\"main\",\"base_commit_ts\":1}}").as_bytes(),
        )
        .await;
        creates.push(start.elapsed());
        assert_eq!(status, 200);
    }
    creates.sort();
    let median_create = creates[creates.len() / 2];
    assert!(median_create < Duration::from_millis(250), "branch create median: {median_create:?}");
    let start = std::time::Instant::now();
    let (status, _) = http_request(http, "POST /v1/branches/lat-0/promote", b"").await;
    let promote = start.elapsed();
    assert_eq!(status, 200);
    assert!(promote < Duration::from_millis(250), "branch promote: {promote:?}");
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_vector_text_search() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-index-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-index"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(
        http,
        "POST /v1/vector/upsert",
        b"{\"table\":\"emb\",\"id\":\"a\",\"vector\":[1.0,0.0]}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = http_request(
        http,
        "POST /v1/vector/upsert",
        b"{\"table\":\"emb\",\"id\":\"b\",\"vector\":[0.0,1.0]}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = http_request(
        http,
        "POST /v1/vector/search",
        b"{\"table\":\"emb\",\"vector\":[0.9,0.1],\"top_k\":2}",
    )
    .await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.find("\"a\"").unwrap() < text.find("\"b\"").unwrap(), "{text}");
    let (status, _) = http_request(
        http,
        "POST /v1/vector/upsert",
        b"{\"table\":\"emb\",\"id\":\"c\",\"vector\":[1.0]}",
    )
    .await;
    assert_eq!(status, 400);
    let (status, body) = http_request(http, "DELETE /v1/vector/emb/b", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("true"));
    let (status, body) = http_request(http, "GET /v1/index/stats", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("\"partitions\":4"), "{text}");
    assert!(text.contains("emb"), "{text}");
    let (status, _) = http_request(
        http,
        "POST /v1/text/index",
        b"{\"table\":\"docs\",\"id\":\"d1\",\"text\":\"the quick brown fox\"}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = http_request(
        http,
        "POST /v1/text/index",
        b"{\"table\":\"docs\",\"id\":\"d2\",\"text\":\"lorem ipsum dolor\"}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = http_request(
        http,
        "POST /v1/text/search",
        b"{\"table\":\"docs\",\"query\":\"quick fox\",\"top_k\":10}",
    )
    .await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("d1"), "{text}");
    assert!(!text.contains("d2"), "{text}");
    let (status, body) = http_request(http, "DELETE /v1/text/docs/d1", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("true"));
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_read_only_follower_and_invoice() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-follower-{}-{}",
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
    let native_listener = bind_listener().await;
    let native = native_listener.local_addr().unwrap();
    let mut config = Config {
        node_id: String::from("follower-1"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        native_listen: Some(native),
        ..Config::default()
    };
    config.region = String::from("eu-west-1");
    config.read_only = true;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve_with_native(
            config,
            pg_listener,
            resp_listener,
            http_listener,
            native_listener,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, body) = http_request(http, "GET /v1/regions", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("eu-west-1"), "{text}");
    assert!(text.contains("true"), "{text}");
    let (status, _) = http_request(http, "PUT /v1/kv/docs/blocked", b"x").await;
    assert_eq!(status, 503);
    let (status, _) =
        http_request(http, "POST /v1/sql", b"{\"sql\":\"INSERT INTO docs KEY 'a' VALUE 'b'\"}")
            .await;
    assert_eq!(status, 503);
    let (status, _) =
        http_request(http, "POST /v1/sql", b"{\"sql\":\"SELECT * FROM docs KEY 'a'\"}").await;
    assert_eq!(status, 200);
    let (status, _) = http_request(
        http,
        "POST /v1/vector/upsert",
        b"{\"table\":\"emb\",\"id\":\"a\",\"vector\":[1.0]}",
    )
    .await;
    assert_eq!(status, 503);
    let (status, _) = http_request(http, "GET /v1/billing/invoice", b"").await;
    assert_eq!(status, 200);
    let denied = resp_command(resp, &["SET", "blocked", "1"]).await;
    assert!(denied.contains("READONLY"), "{denied}");
    assert_eq!(resp_command(resp, &["PING"]).await, "+PONG");
    let pg_denied = pg_simple_error(pg, "INSERT INTO docs KEY 'a' VALUE 'b'").await;
    assert!(pg_denied.contains("read-only"), "{pg_denied}");
    assert!(pg_denied.contains("25006"), "{pg_denied}");
    let denied = native_op(
        native,
        serde_json::json!({"key": KEY, "op": "put", "table": "docs", "pk": "n1", "value": "v1"}),
    )
    .await;
    assert_eq!(denied.get("ok"), Some(&serde_json::Value::Bool(false)));
    assert!(
        denied.get("error").and_then(|v| v.as_str()).unwrap_or("").contains("read-only"),
        "{denied}"
    );
    let denied = native_op(
        native,
        serde_json::json!({"key": KEY, "op": "sql", "sql": "INSERT INTO docs KEY 'a' VALUE 'b'"}),
    )
    .await;
    assert_eq!(denied.get("ok"), Some(&serde_json::Value::Bool(false)));
    assert!(
        denied.get("error").and_then(|v| v.as_str()).unwrap_or("").contains("read-only"),
        "{denied}"
    );
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

async fn native_op(addr: std::net::SocketAddr, body: serde_json::Value) -> serde_json::Value {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let raw = serde_json::to_vec(&body).unwrap();
    let mut frame = (raw.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&raw);
    socket.write_all(&frame).await.unwrap();
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await.unwrap();
    let len = u32::from_be_bytes(header) as usize;
    let mut response = vec![0u8; len];
    socket.read_exact(&mut response).await.unwrap();
    serde_json::from_slice(&response).unwrap()
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

async fn pg_simple_error(addr: std::net::SocketAddr, sql: &str) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut startup = b"user\0u\0\0".to_vec();
    let mut frame = ((startup.len() + 8) as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&196608u32.to_be_bytes());
    frame.extend_from_slice(&startup);
    startup = frame;
    socket.write_all(&startup).await.unwrap();
    let mut out = String::new();
    loop {
        let mut tag = [0u8; 1];
        socket.read_exact(&mut tag).await.unwrap();
        let mut length_buffer = [0u8; 4];
        socket.read_exact(&mut length_buffer).await.unwrap();
        let length = u32::from_be_bytes(length_buffer) as usize;
        let mut payload = vec![0u8; length - 4];
        socket.read_exact(&mut payload).await.unwrap();
        if tag[0] == b'Z' {
            break;
        }
    }
    let query = sql.as_bytes();
    let mut message = vec![b'Q'];
    message.extend_from_slice(&((query.len() + 4) as u32).to_be_bytes());
    message.extend_from_slice(query);
    socket.write_all(&message).await.unwrap();
    loop {
        let mut tag = [0u8; 1];
        socket.read_exact(&mut tag).await.unwrap();
        let mut length_buffer = [0u8; 4];
        socket.read_exact(&mut length_buffer).await.unwrap();
        let length = u32::from_be_bytes(length_buffer) as usize;
        let mut payload = vec![0u8; length - 4];
        socket.read_exact(&mut payload).await.unwrap();
        if tag[0] == b'E' {
            out.push_str(&String::from_utf8_lossy(&payload));
            return out;
        }
        if tag[0] == b'Z' {
            return out;
        }
    }
}

#[tokio::test]
async fn extras_oidc_flow() {
    std::env::set_var("RYME_API_KEY", KEY);
    std::env::set_var("RYME_OIDC_ISSUER", "https://auth.rymedb.test");
    std::env::set_var("RYME_OIDC_AUDIENCE", "rymedb-app");
    std::env::set_var("RYME_OIDC_SECRET", "oidc-e2e-secret");
    std::env::set_var("RYME_OIDC_AUTH_ENDPOINT", "https://auth.rymedb.test/authorize");
    std::env::set_var("RYME_OIDC_CLIENT_ID", "rymedb-app");
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-oidc-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-oidc"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, body) = http_request(
        http,
        "POST /v1/auth/oidc/login",
        b"{\"redirect_uri\":\"https://app.test/callback\",\"state\":\"s1\"}",
    )
    .await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("response_type=code"), "{text}");
    assert!(text.contains("state=s1"), "{text}");
    let (status, _) =
        http_request(http, "POST /v1/auth/oidc/token", b"{\"id_token\":\"bad.token.here\"}").await;
    assert_eq!(status, 401);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let header = ryme_auth::base64_url_encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = format!(
        "{{\"sub\":\"oidc-user\",\"tenant\":\"sso\",\"roles\":[\"readwrite\"],\"iss\":\"https://auth.rymedb.test\",\"aud\":\"rymedb-app\",\"exp\":{}}}",
        now + 3600
    );
    let payload_b64 = ryme_auth::base64_url_encode(payload.as_bytes());
    let input = format!("{header}.{payload_b64}");
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"oidc-e2e-secret").unwrap();
    mac.update(input.as_bytes());
    use hmac::Mac;
    let id_token =
        format!("{input}.{}", ryme_auth::base64_url_encode(&mac.finalize().into_bytes()));
    let (status, body) = http_request(
        http,
        "POST /v1/auth/oidc/token",
        format!("{{\"id_token\":\"{id_token}\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed.get("tenant").and_then(|v| v.as_str()), Some("sso"));
    let key = parsed.get("key").and_then(|v| v.as_str()).unwrap().to_string();
    let (status, _) = bearer_request(http, &key, "GET /v1/ranges", b"").await;
    assert_eq!(status, 200);
    server.abort();
    std::env::remove_var("RYME_OIDC_ISSUER");
    std::env::remove_var("RYME_OIDC_AUDIENCE");
    std::env::remove_var("RYME_OIDC_SECRET");
    std::env::remove_var("RYME_OIDC_AUTH_ENDPOINT");
    std::env::remove_var("RYME_OIDC_CLIENT_ID");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_migrate_analyze_and_scalars() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-migrate-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-migrate"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let dump = "CREATE TABLE public.messages (id uuid); CREATE POLICY \"own\" ON public.messages FOR SELECT USING (auth.uid() = user_id);";
    let body = serde_json::json!({ "dump": dump }).to_string();
    let (status, response) = http_request(http, "POST /v1/migrate/supabase", body.as_bytes()).await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&response).into_owned();
    assert!(text.contains("messages"), "{text}");
    assert!(text.contains("user_id"), "{text}");
    let branches = r#"[{"name":"preview-1","parent":"main","lsn":"0/16B4C50"}]"#;
    let body = serde_json::json!({ "branches": branches }).to_string();
    let (status, response) = http_request(http, "POST /v1/migrate/neon", body.as_bytes()).await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&response).contains("preview-1"));
    let (status, _) = http_request(http, "POST /v1/migrate/neon", b"{\"branches\":\"nope\"}").await;
    assert_eq!(status, 400);
    let (status, body) = http_request(
        http,
        "POST /v1/sql",
        b"{\"sql\":\"INSERT INTO docs KEY gen_random_uuid() VALUE 'v'\"}",
    )
    .await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("ok"));
    let (status, body) = http_request(http, "GET /v1/regions", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("local-1"));
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_migrate_apply_executes_and_records() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-ledger-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-ledger"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let apply =
        serde_json::json!({"id": "m1", "sql": "INSERT INTO docs KEY 'k1' VALUE 'v1'"}).to_string();
    let (status, applied) = http_request(http, "POST /v1/migrate/apply", apply.as_bytes()).await;
    assert_eq!(status, 201);
    let applied = String::from_utf8_lossy(&applied).into_owned();
    assert!(applied.contains("\"migration_id\":\"m1\""), "{applied}");
    assert!(applied.contains("\"result_schema_version\":1"), "{applied}");
    let (status, row) = http_request(http, "GET /v1/kv/docs/k1", b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&row));
    let (status, body) = http_request(http, "GET /v1/migrate/ledger", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("\"version\":1"), "{text}");
    assert!(text.contains("\"valid\":true"), "{text}");
    assert!(text.contains("\"migration_id\":\"m1\""), "{text}");
    let (status, _) = http_request(http, "POST /v1/migrate/apply", apply.as_bytes()).await;
    assert_eq!(status, 409);
    let (status, _) =
        http_request(http, "POST /v1/migrate/apply", b"{\"id\":\"m2\",\"sql\":\"NOPE\"}").await;
    assert_eq!(status, 400);
    let (status, _) = http_request(
        http,
        "POST /v1/migrate/apply",
        b"{\"id\":\"m3\",\"sql\":\"SELECT * FROM docs KEY 'k1'\"}",
    )
    .await;
    assert_eq!(status, 400);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_select_where_order_offset() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-select-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-select"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let seed = "{\"table\":\"fruit\",\"rows\":[{\"key\":\"a\",\"value\":\"apple\"},{\"key\":\"b\",\"value\":\"banana\"},{\"key\":\"c\",\"value\":\"apricot\"}]}";
    let (status, _) = http_request(http, "POST /v1/sql/copy", seed.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, body) = http_request(
        http,
        "POST /v1/sql",
        b"{\"sql\":\"SELECT * FROM fruit WHERE value CONTAINS 'ap'\"}",
    )
    .await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains('a') && text.contains('c'), "{text}");
    assert!(!text.contains('b'), "{text}");
    let (status, body) = http_request(
        http,
        "POST /v1/sql",
        b"{\"sql\":\"SELECT * FROM fruit ORDER BY key DESC LIMIT 1 OFFSET 1\"}",
    )
    .await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains('b'), "{text}");
    assert!(!text.contains('c'), "{text}");
    let (status, body) = http_request(
        http,
        "POST /v1/sql/explain",
        b"{\"sql\":\"SELECT * FROM fruit WHERE key != 'a'\"}",
    )
    .await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("filters 1"));
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_select_aggregates() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-agg-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-agg"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let seed = "{\"table\":\"nums\",\"rows\":[{\"key\":\"a\",\"value\":\"10\"},{\"key\":\"b\",\"value\":\"20\"},{\"key\":\"c\",\"value\":\"oops\"}]}";
    let (status, _) = http_request(http, "POST /v1/sql/copy", seed.as_bytes()).await;
    assert_eq!(status, 200);
    for (sql, label, value) in [
        ("SELECT COUNT(*) FROM nums", "count", "3"),
        ("SELECT SUM(value) FROM nums", "sum", "30"),
        ("SELECT AVG(value) FROM nums WHERE key != 'c'", "avg", "15"),
        ("SELECT MAX(value) FROM nums", "max", "oops"),
    ] {
        let body = serde_json::json!({ "sql": sql }).to_string();
        let (status, response) = http_request(http, "POST /v1/sql", body.as_bytes()).await;
        assert_eq!(status, 200, "{sql}");
        let text = String::from_utf8_lossy(&response).into_owned();
        assert!(text.contains(label), "{sql} {text}");
        assert!(text.contains(value), "{sql} {text}");
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_select_join() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-join-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-join"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let seed = "{\"table\":\"users\",\"rows\":[{\"key\":\"a\",\"value\":\"ada\"},{\"key\":\"b\",\"value\":\"grace\"}]}";
    let (status, _) = http_request(http, "POST /v1/sql/copy", seed.as_bytes()).await;
    assert_eq!(status, 200);
    let seed = "{\"table\":\"orders\",\"rows\":[{\"key\":\"a\",\"value\":\"o1\"},{\"key\":\"q\",\"value\":\"stray\"}]}";
    let (status, _) = http_request(http, "POST /v1/sql/copy", seed.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, body) = http_request(
        http,
        "POST /v1/sql",
        b"{\"sql\":\"SELECT * FROM users JOIN orders ON KEY = KEY\"}",
    )
    .await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains('a'), "{text}");
    assert!(text.contains("ada"), "{text}");
    assert!(text.contains("o1"), "{text}");
    assert!(!text.contains("grace"), "{text}");
    assert!(!text.contains("stray"), "{text}");
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_select_group_by() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-group-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-group"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let seed = "{\"table\":\"tags\",\"rows\":[{\"key\":\"a\",\"value\":\"red\"},{\"key\":\"b\",\"value\":\"blue\"},{\"key\":\"c\",\"value\":\"red\"}]}";
    let (status, _) = http_request(http, "POST /v1/sql/copy", seed.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, body) = http_request(
        http,
        "POST /v1/sql",
        b"{\"sql\":\"SELECT value, COUNT(*) FROM tags GROUP BY value\"}",
    )
    .await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("red"), "{text}");
    assert!(text.contains('2'), "{text}");
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_auth_privilege_boundaries() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-priv-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-priv"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
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
    let app_key = serde_json::from_slice::<serde_json::Value>(&body)
        .unwrap()
        .get("key")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    let (status, _) = bearer_request(
        http,
        &app_key,
        "POST /v1/auth/register",
        b"{\"id\":\"sneaky\",\"password\":\"correct-horse\",\"roles\":[\"owner\"]}",
    )
    .await;
    assert_eq!(status, 403);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/register",
        b"{\"id\":\"boss\",\"password\":\"correct-horse\",\"roles\":[\"owner\"]}",
    )
    .await;
    assert_eq!(status, 201);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/register",
        b"{\"id\":\"typo\",\"password\":\"correct-horse\",\"roles\":[\"owenr\"]}",
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = bearer_request(
        http,
        &app_key,
        "POST /v1/auth/register",
        b"{\"id\":\"peer\",\"password\":\"correct-horse\",\"roles\":[\"readwrite\"]}",
    )
    .await;
    assert_eq!(status, 201);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/register",
        b"{\"id\":\"app\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 409);
    let (status, _) = bearer_request(
        http,
        &app_key,
        "POST /v1/auth/register",
        b"{\"id\":\"far\",\"password\":\"correct-horse\",\"tenant\":\"evil\"}",
    )
    .await;
    assert_eq!(status, 403);
    let (status, _) = bearer_request(
        http,
        &app_key,
        "POST /v1/auth/mask",
        b"{\"table\":\"users\",\"fields\":[]}",
    )
    .await;
    assert_eq!(status, 403);
    let (status, _) =
        bearer_request(http, &app_key, "POST /v1/backups/restore?target=1", b"").await;
    assert_eq!(status, 403);
    let (status, _) = bearer_request(
        http,
        &app_key,
        "DELETE /v1/auth/keys",
        format!("{{\"key\":\"{KEY}\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 403);
    let secret = p256::ecdsa::SigningKey::from_slice(&[4u8; 32]).unwrap();
    let public = secret.verifying_key().to_sec1_point(false);
    let public_b64 = ryme_auth::base64_url_encode(public.as_bytes());
    let (status, _) = bearer_request(
        http,
        &app_key,
        "POST /v1/auth/passkey/register",
        format!(
            "{{\"user\":\"boss\",\"credential_id\":\"cred-evil\",\"public_key\":\"{public_b64}\"}}"
        )
        .as_bytes(),
    )
    .await;
    assert_eq!(status, 403);
    let (status, _) = bearer_request(
        http,
        &app_key,
        "POST /v1/auth/passkey/register",
        format!(
            "{{\"user\":\"app\",\"credential_id\":\"cred-app\",\"public_key\":\"{public_b64}\"}}"
        )
        .as_bytes(),
    )
    .await;
    assert_eq!(status, 201);
    let (status, _) = bearer_request(
        http,
        &app_key,
        "POST /v1/auth/passkey/register",
        format!(
            "{{\"user\":\"app\",\"credential_id\":\"cred-app\",\"public_key\":\"{public_b64}\"}}"
        )
        .as_bytes(),
    )
    .await;
    assert_eq!(status, 409);
    let big_user = "u".repeat(300);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/passkey/challenge",
        format!("{{\"user\":\"{big_user}\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) =
        bearer_request(http, &app_key, "POST /v1/auth/otp/setup", b"{\"id\":\"boss\"}").await;
    assert_eq!(status, 403);
    let (status, _) =
        bearer_request(http, &app_key, "POST /v1/auth/otp/setup", b"{\"id\":\"app\"}").await;
    assert_eq!(status, 200);
    let (status, _) = bearer_request(
        http,
        &app_key,
        "DELETE /v1/auth/keys",
        format!("{{\"key\":\"{app_key}\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = bearer_request(http, &app_key, "GET /v1/ranges", b"").await;
    assert_eq!(status, 401);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_slow_log_endpoint() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-slow-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-slow"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, body) = http_request(http, "GET /v1/observe/slow", b"").await;
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed.get("entries"), Some(&serde_json::Value::Array(Vec::new())));
    let (status, _) = http_request(http, "GET /v1/observe/slow?limit=5", b"").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/observe/slow?table=docs", b"").await;
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed.get("entries"), Some(&serde_json::Value::Array(Vec::new())));
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_traces_record_requests() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-traces-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-traces"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(http, "PUT /v1/kv/docs/traced", b"{\"n\":1}").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /v1/traces?limit=5", b"").await;
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let spans = parsed.get("spans").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    assert!(!spans.is_empty());
    let put = spans.iter().find(|span| span.get("name").and_then(|v| v.as_str()) == Some("kv_put"));
    assert!(put.is_some());
    let put = put.unwrap();
    assert!(put
        .get("attributes")
        .and_then(|v| v.as_array())
        .map(|attrs| attrs.iter().any(|attr| attr.get(0).and_then(|v| v.as_str()) == Some("table")
            && attr.get(1).and_then(|v| v.as_str()) == Some("docs")))
        .unwrap_or(false));
    let (status, body) = http_request(http, "GET /v1/traces?name=kv_put&table=docs", b"").await;
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let spans = parsed.get("spans").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    assert!(!spans.is_empty());
    assert!(spans.iter().all(|span| span.get("name").and_then(|v| v.as_str()) == Some("kv_put")));
    for head in ["GET /v1/traces?name=missing", "GET /v1/traces?table=missing"] {
        let (status, body) = http_request(http, head, b"").await;
        assert_eq!(status, 200, "{head}");
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.get("spans"), Some(&serde_json::Value::Array(Vec::new())), "{head}");
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn extras_auth_token_issues_usable_keys() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-token-{}-{}",
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
    let config = Config {
        node_id: String::from("extras-token"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        ..Config::default()
    };
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(
        http,
        "POST /v1/auth/register",
        b"{\"id\":\"tok\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 201);
    let (status, body) = http_request(
        http,
        "POST /v1/auth/token",
        b"{\"id\":\"tok\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 201);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let key = parsed.get("key").and_then(|v| v.as_str()).unwrap().to_string();
    assert!(key.starts_with("ryme_"));
    let (status, body) = bearer_request(http, &key, "GET /v1/ranges", b"").await;
    assert_eq!(status, 200);
    assert!(body.is_array());
    let (status, _) =
        http_request(http, "POST /v1/auth/token", b"{\"id\":\"tok\",\"password\":\"wrong-pass\"}")
            .await;
    assert_eq!(status, 401);
    let (status, body) = http_request(http, "POST /v1/auth/otp/setup", b"{\"id\":\"tok\"}").await;
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let secret = parsed.get("secret").and_then(|v| v.as_str()).unwrap().to_string();
    let (status, _) = http_request(
        http,
        "POST /v1/auth/token",
        b"{\"id\":\"tok\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 401);
    let raw = ryme_auth::base64_url_decode(&secret).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let code = ryme_auth::totp_code(&raw, now);
    let (status, body) = http_request(
        http,
        "POST /v1/auth/token",
        format!("{{\"id\":\"tok\",\"password\":\"correct-horse\",\"code\":\"{code}\"}}").as_bytes(),
    )
    .await;
    assert_eq!(status, 201);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(parsed.get("key").and_then(|v| v.as_str()).is_some());
    let revoke = format!("{{\"key\":\"{key}\"}}", key = key);
    let (status, _) = http_request(http, "DELETE /v1/auth/keys", revoke.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, _) = bearer_request(http, &key, "GET /v1/ranges", b"").await;
    assert_eq!(status, 401);
    let (status, _) =
        http_request(http, "DELETE /v1/auth/keys", b"{\"key\":\"ryme_missing\"}").await;
    assert_eq!(status, 404);
    let _ = std::fs::remove_dir_all(&root);
}

fn webauthn_assertion(
    secret: &p256::ecdsa::SigningKey,
    rp_id: &str,
    origin: &str,
    challenge_b64: &str,
    counter: u32,
) -> (String, String, String) {
    use p256::ecdsa::signature::Signer;
    use sha2::{Digest, Sha256};
    let client_data = format!(
        "{{\"type\":\"webauthn.get\",\"challenge\":\"{challenge_b64}\",\"origin\":\"{origin}\"}}"
    );
    let mut hasher = Sha256::new();
    hasher.update(rp_id.as_bytes());
    let mut auth_data = hasher.finalize().to_vec();
    auth_data.push(0x01);
    auth_data.extend_from_slice(&counter.to_be_bytes());
    let mut hasher = Sha256::new();
    hasher.update(client_data.as_bytes());
    let mut signed = auth_data.clone();
    signed.extend_from_slice(&hasher.finalize());
    let signature: p256::ecdsa::Signature = secret.sign(&signed);
    (
        ryme_auth::base64_url_encode(&auth_data),
        ryme_auth::base64_url_encode(client_data.as_bytes()),
        ryme_auth::base64_url_encode(signature.to_der().as_bytes()),
    )
}

#[tokio::test]
async fn extras_passkey_full_ceremony_issues_keys() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-extras-passkey-{}-{}",
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
    let mut base = ryme_config::Config::default();
    base.archive.interval_secs = 0;
    let config = ryme_config::Config {
        node_id: String::from("extras-passkey"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        archive: base.archive,
        passkey_rp_id: String::from("auth.test"),
        passkey_origins: vec![String::from("https://auth.test")],
        ..ryme_config::Config::default()
    };
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let secret = p256::ecdsa::SigningKey::from_slice(&[9u8; 32]).unwrap();
    let public = secret.verifying_key().to_sec1_point(false);
    let public_b64 = ryme_auth::base64_url_encode(public.as_bytes());
    let (status, _) = http_request(
        http,
        "POST /v1/auth/register",
        b"{\"id\":\"pkada\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 201);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/passkey/register",
        format!(
            "{{\"user\":\"pkada\",\"credential_id\":\"cred-9\",\"public_key\":\"{public_b64}\"}}"
        )
        .as_bytes(),
    )
    .await;
    assert_eq!(status, 201);
    let (status, body) =
        http_request(http, "POST /v1/auth/passkey/challenge", b"{\"user\":\"pkada\"}").await;
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let challenge = parsed.get("challenge").and_then(|v| v.as_str()).unwrap().to_string();
    let (auth_data, client_data, signature) =
        webauthn_assertion(&secret, "auth.test", "https://auth.test", &challenge, 1);
    let (status, body) = http_request(
        http,
        "POST /v1/auth/passkey/verify",
        format!("{{\"user\":\"pkada\",\"credential_id\":\"cred-9\",\"authenticator_data\":\"{auth_data}\",\"client_data_json\":\"{client_data}\",\"signature\":\"{signature}\"}}")
            .as_bytes(),
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let key = parsed.get("key").and_then(|v| v.as_str()).unwrap().to_string();
    assert!(key.starts_with("ryme_"));
    let (status, _) = bearer_request(http, &key, "GET /v1/ranges", b"").await;
    assert_eq!(status, 200);
    let (status, _) = http_request(
        http,
        "POST /v1/auth/passkey/verify",
        format!("{{\"user\":\"pkada\",\"credential_id\":\"cred-9\",\"authenticator_data\":\"{auth_data}\",\"client_data_json\":\"{client_data}\",\"signature\":\"{signature}\"}}")
            .as_bytes(),
    )
    .await;
    assert_eq!(status, 401);
    let _ = std::fs::remove_dir_all(&root);
}
