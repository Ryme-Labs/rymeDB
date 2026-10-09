use ryme_config::Config;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: &str = "ryme-compat-key-9c4e2a7b1d56";

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

async fn http_request(addr: std::net::SocketAddr, head: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let (status, body, _) = http_request_headers(addr, head, None, body).await;
    (status, body)
}

async fn http_request_headers(
    addr: std::net::SocketAddr,
    head: &str,
    prefer: Option<&str>,
    body: &[u8],
) -> (u16, Vec<u8>, String) {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let prefer_header = prefer.map(|value| format!("prefer: {value}\r\n")).unwrap_or_default();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\norigin: http://localhost:3000\r\nauthorization: Bearer {KEY}\r\n{prefer_header}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
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
    let header_end = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(raw.len());
    let headers = String::from_utf8_lossy(&raw[..header_end]).into_owned();
    let body = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|index| raw[index + 4..].to_vec())
        .unwrap_or_default();
    (status, body, headers)
}

#[tokio::test]
async fn compat_rest_graphql_copy_explain() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-compat-{}-{}",
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
        node_id: String::from("compat"),
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
    let (status, body, headers) =
        http_request_headers(http, "OPTIONS /rest/v1/people", None, b"").await;
    assert_eq!(status, 204);
    assert!(body.is_empty());
    let headers = headers.to_ascii_lowercase();
    assert!(headers.contains("access-control-allow-origin: http://localhost:3000"), "{headers}");
    assert!(headers.contains("access-control-allow-methods:"), "{headers}");
    assert!(headers.contains("access-control-allow-headers:"), "{headers}");
    let (status, body, headers) =
        http_request_headers(http, "OPTIONS /v1/presence/join", None, b"").await;
    assert_eq!(status, 204);
    assert!(body.is_empty());
    assert!(headers
        .to_ascii_lowercase()
        .contains("access-control-allow-origin: http://localhost:3000"));
    let (status, _) = http_request(
        http,
        "POST /v1/sql/copy",
        b"{\"table\":\"docs\",\"rows\":[{\"key\":\"k1\",\"value\":\"v1\"},{\"key\":\"k2\",\"value\":\"v2\"}]}",
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http, "GET /rest/v1/docs?limit=10", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("k1"), "{text}");
    let (status, body) = http_request(http, "GET /rest/v1/docs?key=eq.k2", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("k2"), "{text}");
    assert!(!text.contains("k1"), "{text}");
    let (status, _) = http_request(
        http,
        "POST /v1/sql/copy",
        br#"{"table":"profiles","rows":[{"key":"p1","value":"{\"status\":\"ready\",\"score\":12,\"owner\":\"a\"}"},{"key":"p2","value":"{\"status\":\"queued\",\"score\":4,\"owner\":\"b\"}"}]}"#,
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = http_request(
        http,
        "GET /rest/v1/profiles?select=key,status&status=eq.ready&score=gte.10",
        b"",
    )
    .await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains(r#""key":"p1""#), "{text}");
    assert!(text.contains(r#""status":"ready""#), "{text}");
    assert!(!text.contains("owner"), "{text}");
    assert!(!text.contains("p2"), "{text}");
    let rows = (0..257)
        .map(|index| {
            let value = serde_json::json!({
                "status": if index == 256 { "ready" } else { "queued" },
            });
            serde_json::json!({
                "key": format!("row-{index:03}"),
                "value": serde_json::to_string(&value).unwrap(),
            })
        })
        .collect::<Vec<_>>();
    let body = serde_json::to_vec(&serde_json::json!({
        "table": "long_profiles",
        "rows": rows,
    }))
    .unwrap();
    let (status, _) = http_request(http, "POST /v1/sql/copy", &body).await;
    assert_eq!(status, 200);
    let (status, body) =
        http_request(http, "GET /rest/v1/long_profiles?status=eq.ready&limit=1", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(text.contains("row-256"), "filtered match beyond first page: {text}");
    let (status, _) =
        http_request(http, "POST /rest/v1/docs", b"{\"key\":\"k3\",\"value\":\"v3\"}").await;
    assert_eq!(status, 201);
    let (status, body) =
        http_request(http, "POST /rest/v1/people", br#"{"id":"p1","name":"Ada","status":"ready"}"#)
            .await;
    assert_eq!(status, 201);
    assert!(String::from_utf8_lossy(&body).contains(r#""name":"Ada""#));
    let (status, body) = http_request(
        http,
        "POST /rest/v1/people",
        br#"[{"id":"p2","name":"Grace"},{"id":"p3","name":"Linus"}]"#,
    )
    .await;
    assert_eq!(status, 201);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains(r#""id":"p2""#), "{text}");
    assert!(text.contains(r#""id":"p3""#), "{text}");
    let (status, body, _) = http_request_headers(
        http,
        "POST /rest/v1/people",
        Some("return=minimal"),
        br#"{"id":"p6","name":"Minimal"}"#,
    )
    .await;
    assert_eq!(status, 201);
    assert!(body.is_empty());
    let (status, _) = http_request(
        http,
        "POST /rest/v1/people",
        br#"{"id":"p4","email":"ada@example.com","name":"Ada"}"#,
    )
    .await;
    assert_eq!(status, 201);
    let (status, _) = http_request(
        http,
        "POST /rest/v1/people?on_conflict=email",
        br#"{"id":"p5","email":"ada@example.com","name":"Updated Ada"}"#,
    )
    .await;
    assert_eq!(status, 201);
    let (status, body) =
        http_request(http, "GET /rest/v1/people?email=eq.ada%40example.com&select=name", b"").await;
    assert_eq!(status, 200);
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].get("name").and_then(serde_json::Value::as_str), Some("Updated Ada"));
    let (status, body, headers) =
        http_request_headers(http, "GET /rest/v1/people?limit=1", Some("count=exact"), b"").await;
    assert_eq!(status, 200);
    assert_eq!(serde_json::from_slice::<Vec<serde_json::Value>>(&body).unwrap().len(), 1);
    assert!(headers.to_ascii_lowercase().contains("content-range: 0-0/5"), "{headers}");
    let (status, body) =
        http_request(http, "PATCH /rest/v1/people?id=eq.p1", br#"{"status":"away"}"#).await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains(r#""status":"away""#));
    let (status, body) =
        http_request(http, "GET /rest/v1/people?select=key,name,status&id=eq.p1", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains(r#""name":"Ada""#), "{text}");
    assert!(text.contains(r#""status":"away""#), "{text}");
    let (status, body) = http_request(http, "DELETE /rest/v1/people?id=eq.p2", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains(r#""deleted":1"#));
    let (status, body) = http_request(http, "GET /rest/v1/people?id=eq.p2", b"").await;
    assert_eq!(status, 200);
    assert_eq!(String::from_utf8_lossy(&body), "[]");
    let (status, body) =
        http_request(http, "GET /rest/v1/people?or=(id.eq.p1,id.eq.p3)&select=id,name", b"").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains(r#""id":"p1""#), "{text}");
    assert!(text.contains(r#""id":"p3""#), "{text}");
    assert!(!text.contains(r#""id":"p2""#), "{text}");
    let (status, body) =
        http_request(http, "POST /graphql", b"{\"query\":\"{ docs(key: \\\"k1\\\") }\"}").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("k1"));
    let (status, body) =
        http_request(http, "POST /v1/sql/explain", b"{\"sql\":\"SELECT * FROM docs KEY 'k1'\"}")
            .await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("point_lookup"));
    let (status, body) = http_request(http, "GET /v1/metering", b"").await;
    assert_eq!(status, 200);
    assert!(
        String::from_utf8_lossy(&body).contains("read_unit")
            || String::from_utf8_lossy(&body).contains("write_unit")
    );
    let (status, body) = http_request(http, "GET /v1/autoscale?cpu_pct=95", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("add_gateways_or_compute"));
    let (status, body) = http_request(http, "GET /metrics/prometheus", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("rymedb_rest_microseconds"));
    let (status, body) = http_request(http, "GET /metrics", b"").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("p99_micros"));
    let (status, _) = http_request(http, "DELETE /rest/v1/docs?key=eq.k3", b"").await;
    assert_eq!(status, 200);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}
