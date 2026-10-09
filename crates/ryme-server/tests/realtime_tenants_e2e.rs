use futures_util::StreamExt;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

const API_KEY: &str = "ryme-realtime-tenants-key-6e2a";
const JWT_SECRET: &str = "ryme-realtime-tenants-secret-7b3f";

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

fn mint_jwt(tenant: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let header = ryme_auth::base64_url_encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let payload = format!(
        "{{\"sub\":\"realtime-test\",\"tenant\":\"{tenant}\",\"roles\":[\"Owner\"],\"exp\":{}}}",
        now + 3600
    );
    let payload = ryme_auth::base64_url_encode(payload.as_bytes());
    let input = format!("{header}.{payload}");
    let mut mac = Hmac::<Sha256>::new_from_slice(JWT_SECRET.as_bytes()).unwrap();
    mac.update(input.as_bytes());
    let signature = ryme_auth::base64_url_encode(&mac.finalize().into_bytes());
    format!("{input}.{signature}")
}

async fn http_request(
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
    let text = String::from_utf8_lossy(&raw);
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
        .position(|window| window == b"\r\n\r\n")
        .map(|index| raw[index + 4..].to_vec())
        .unwrap_or_default();
    let body = serde_json::from_slice(&body).unwrap_or_default();
    (status, body)
}

async fn next_json(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> serde_json::Value {
    let message = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match message {
        tokio_tungstenite::tungstenite::Message::Text(text) => serde_json::from_str(&text).unwrap(),
        other => panic!("unexpected WebSocket message: {other:?}"),
    }
}

fn websocket_request(url: &str, token: &str) -> tokio_tungstenite::tungstenite::http::Request<()> {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("authorization", HeaderValue::from_str(&format!("Bearer {token}")).unwrap());
    request
}

fn row_keys(message: &serde_json::Value) -> Vec<&str> {
    message["rows"].as_array().into_iter().flatten().filter_map(|row| row["pk"].as_str()).collect()
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[tokio::test]
async fn realtime_streams_use_authenticated_tenant() {
    std::env::set_var("RYME_API_KEY", API_KEY);
    std::env::set_var("RYME_JWT_SECRET", JWT_SECRET);
    let root = std::env::temp_dir().join(format!(
        "ryme-realtime-tenants-{}-{}",
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
    let mut config = ryme_config::Config::default();
    config.node_id = String::from("realtime-tenants");
    config.data_dir = root.clone();
    config.archive.interval_secs = 0;
    config.pg_listen = pg;
    config.resp_listen = resp;
    config.http_listen = http;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let alpha = mint_jwt("alpha");
    let (status, _) = http_request(http, API_KEY, "PUT /v1/kv/docs/default-row", b"default").await;
    assert_eq!(status, 200);
    let alpha_copy = serde_json::json!({
        "table": "docs",
        "rows": [{"key": "alpha-row", "value": "alpha"}]
    })
    .to_string();
    let (status, _) = http_request(http, &alpha, "POST /v1/sql/copy", alpha_copy.as_bytes()).await;
    assert_eq!(status, 200);

    let query_url = format!("ws://{http}/v1/query-stream?table=docs&limit=100");
    let (mut query, _) =
        tokio_tungstenite::connect_async(websocket_request(&query_url, &alpha)).await.unwrap();
    let snapshot = next_json(&mut query).await;
    assert_eq!(snapshot["type"], "snapshot");
    let keys = row_keys(&snapshot);
    assert!(keys.contains(&"alpha-row"), "{snapshot}");
    assert!(!keys.contains(&"default-row"), "{snapshot}");
    query.close(None).await.unwrap();

    let stream_url = format!("ws://{http}/v1/stream?table=docs");
    let (mut stream, _) =
        tokio_tungstenite::connect_async(websocket_request(&stream_url, &alpha)).await.unwrap();
    let (status, _) =
        http_request(http, API_KEY, "PUT /v1/kv/docs/default-live", b"default-live").await;
    assert_eq!(status, 200);
    let no_cross_tenant_event = tokio::time::timeout(Duration::from_millis(500), stream.next());
    assert!(no_cross_tenant_event.await.is_err());

    let alpha_live_copy = serde_json::json!({
        "table": "docs",
        "rows": [{"key": "alpha-live", "value": "alpha-live"}]
    })
    .to_string();
    let (status, _) =
        http_request(http, &alpha, "POST /v1/sql/copy", alpha_live_copy.as_bytes()).await;
    assert_eq!(status, 200);
    let event = next_json(&mut stream).await;
    let event_pk: Vec<u8> = serde_json::from_value(event["pk"].clone()).unwrap();
    assert_eq!(event_pk, b"alpha-live");
    stream.close(None).await.unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&root);
    std::env::remove_var("RYME_JWT_SECRET");
}
