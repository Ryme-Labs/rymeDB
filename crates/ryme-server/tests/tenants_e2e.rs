use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: &str = "ryme-tenants-key-5f2c8a1d9e40";
const JWT_SECRET: &str = "ryme-tenants-jwt-secret-9d4c";

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

fn mint_jwt(tenant: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let header = ryme_auth::base64_url_encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let payload = format!(
        "{{\"sub\":\"tester\",\"tenant\":\"{tenant}\",\"roles\":[\"Owner\"],\"exp\":{}}}",
        now + 3600
    );
    let payload_b64 = ryme_auth::base64_url_encode(payload.as_bytes());
    let input = format!("{header}.{payload_b64}");
    let mut mac = Hmac::<Sha256>::new_from_slice(JWT_SECRET.as_bytes()).unwrap();
    mac.update(input.as_bytes());
    let sig = ryme_auth::base64_url_encode(&mac.finalize().into_bytes());
    format!("{input}.{sig}")
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

async fn bearer_branch_request(
    addr: std::net::SocketAddr,
    token: &str,
    branch: &str,
    head: &str,
    body: &[u8],
) -> (u16, serde_json::Value) {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\nauthorization: Bearer {token}\r\nx-ryme-branch: {branch}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
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
    let json = serde_json::from_slice(&body).unwrap_or_default();
    (status, json)
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[tokio::test]
async fn tenants_share_no_presence_or_partitions() {
    std::env::set_var("RYME_API_KEY", KEY);
    std::env::set_var("RYME_JWT_SECRET", JWT_SECRET);
    let root =
        std::env::temp_dir().join(format!("ryme-tenants-{}-{}", std::process::id(), now_ms()));
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
        node_id: String::from("tenants"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        archive: base.archive,
        ..ryme_config::Config::default()
    };
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let alpha = mint_jwt("alpha");
    let beta = mint_jwt("beta");
    let join = serde_json::json!({"channel": "room", "member": "alice"}).to_string();
    let (status, _) = bearer_request(http, &alpha, "POST /v1/presence/join", join.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, body) = bearer_request(http, &alpha, "GET /v1/presence/room", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let (status, body) = bearer_request(http, &beta, "GET /v1/presence/room", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(0));

    let copy = serde_json::json!({
        "table": "sql_docs",
        "rows": [{"key": "shared", "value": "alpha-row"}]
    })
    .to_string();
    let (status, _) = bearer_request(http, &alpha, "POST /v1/sql/copy", copy.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, body) = bearer_request(
        http,
        &alpha,
        "POST /v1/sql",
        br#"{"sql":"SELECT * FROM sql_docs KEY 'shared'"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body.get("pk").and_then(|v| v.as_str()), Some("shared"));
    assert_eq!(body.get("value").and_then(|v| v.as_str()), Some("alpha-row"));
    let (status, body) = bearer_request(
        http,
        &beta,
        "POST /v1/sql",
        br#"{"sql":"SELECT * FROM sql_docs KEY 'shared'"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body.get("pk"), None);

    let (status, _) = bearer_request(http, KEY, "PUT /v1/kv/docs/default", b"default").await;
    assert_eq!(status, 200);
    let (status, _) = bearer_request(http, &alpha, "PUT /v1/kv/docs/alpha", br#""alpha""#).await;
    assert_eq!(status, 200);
    let (status, body) = bearer_request(http, &alpha, "GET /v1/kv/docs/alpha", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body, serde_json::Value::String(String::from("alpha")));
    let (status, _) = bearer_request(http, &beta, "GET /v1/kv/docs/alpha", b"").await;
    assert_eq!(status, 404);
    let (status, _body) = bearer_request(http, &beta, "GET /v1/kv/docs/default", b"").await;
    assert_eq!(status, 404);

    let branch = serde_json::json!({
        "id": "preview",
        "parent": "main",
        "base_commit_ts": 1
    })
    .to_string();
    let (status, _) = bearer_request(http, &alpha, "POST /v1/branches", branch.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, _) = bearer_request(http, &beta, "GET /v1/branches/preview", b"").await;
    assert_eq!(status, 404);
    let (status, body) = bearer_request(http, &alpha, "GET /v1/branches/preview", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.get("tenant").and_then(|value| value.as_str()), Some("alpha"));
    let (status, body) = bearer_branch_request(
        http,
        &alpha,
        "preview",
        "POST /v1/sql",
        br#"{"sql":"SELECT * FROM sql_docs KEY 'shared'"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body.get("pk"), None);
    let (status, _) =
        bearer_branch_request(http, &alpha, "preview", "GET /v1/kv/docs/alpha", b"").await;
    assert_eq!(status, 404);
    let (status, _) = bearer_branch_request(
        http,
        &alpha,
        "preview",
        "PUT /v1/kv/docs/branch-write",
        br#""branch-write""#,
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) =
        bearer_branch_request(http, &alpha, "preview", "GET /v1/kv/docs/branch-write", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body, serde_json::Value::String(String::from("branch-write")));
    let (status, _) = bearer_request(http, &alpha, "GET /v1/kv/docs/branch-write", b"").await;
    assert_eq!(status, 404);
    let (status, _) = bearer_request(http, &beta, "POST /v1/branches", branch.as_bytes()).await;
    assert_eq!(status, 200);
    let (status, _) = bearer_request(http, KEY, "GET /v1/branches/preview", b"").await;
    assert_eq!(status, 404);

    let append = serde_json::json!({"partition": "orders", "key": "k1", "value": "v1"}).to_string();
    let (status, body) =
        bearer_request(http, &alpha, "POST /v1/topics/append", append.as_bytes()).await;
    assert_eq!(status, 200);
    assert_eq!(body.get("cursor").and_then(|v| v.as_u64()), Some(0));
    let (status, body) =
        bearer_request(http, &alpha, "GET /v1/topics/read?partition=orders&from=0", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let (status, body) =
        bearer_request(http, &beta, "GET /v1/topics/read?partition=orders&from=0", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(0));
    let (status, body) =
        bearer_request(http, &beta, "POST /v1/topics/append", append.as_bytes()).await;
    assert_eq!(status, 200);
    assert_eq!(body.get("cursor").and_then(|v| v.as_u64()), Some(0));
    let (status, body) =
        bearer_request(http, &alpha, "GET /v1/topics/read?partition=orders&from=0", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    std::env::remove_var("RYME_JWT_SECRET");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn presence_counts_drop_expired_members_on_join() {
    std::env::set_var("RYME_API_KEY", KEY);
    std::env::set_var("RYME_JWT_SECRET", JWT_SECRET);
    let root =
        std::env::temp_dir().join(format!("ryme-presence-ttl-{}-{}", std::process::id(), now_ms()));
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
        node_id: String::from("presence-ttl"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        archive: base.archive,
        ..ryme_config::Config::default()
    };
    let _server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let alpha = mint_jwt("alpha");
    for member in ["a", "b"] {
        let join =
            serde_json::json!({"channel": "room", "member": member, "ttl_secs": 1}).to_string();
        let (status, _) =
            bearer_request(http, &alpha, "POST /v1/presence/join", join.as_bytes()).await;
        assert_eq!(status, 200);
    }
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let join = serde_json::json!({"channel": "room", "member": "c", "ttl_secs": 60}).to_string();
    let (status, body) =
        bearer_request(http, &alpha, "POST /v1/presence/join", join.as_bytes()).await;
    assert_eq!(status, 200);
    assert_eq!(body.get("members").and_then(|v| v.as_u64()), Some(1), "{body}");
    std::env::remove_var("RYME_JWT_SECRET");
    let _ = std::fs::remove_dir_all(&root);
}
