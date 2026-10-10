use futures_util::{SinkExt, StreamExt};
use std::time::Duration;

const KEY: &str = "ryme-query-e2e-key-4b8d2f0a9c13";

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

fn test_config(dir: &std::path::Path) -> ryme_config::Config {
    let mut base = ryme_config::Config::default();
    base.archive.interval_secs = 0;
    base.sweep_interval_secs = 0;
    ryme_config::Config { node_id: String::from("query-e2e"), data_dir: dir.to_path_buf(), ..base }
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

async fn http_request_branch(
    addr: std::net::SocketAddr,
    branch: &str,
    head: &str,
    body: &[u8],
) -> (u16, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\nauthorization: Bearer {KEY}\r\nx-ryme-branch: {branch}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
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

async fn next_text(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> serde_json::Value {
    let message = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match message {
        tokio_tungstenite::tungstenite::Message::Text(text) => serde_json::from_str(&text).unwrap(),
        other => panic!("unexpected message: {other:?}"),
    }
}

fn row_pks(message: &serde_json::Value) -> Vec<String> {
    message["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|row| row["pk"].as_str().map(String::from))
        .collect()
}

#[tokio::test]
async fn live_query_snapshot_update_reconnect() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!("ryme-query-{}-{}", std::process::id(), now_ms()));
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
    let (status, _) = http_request(http, "PUT /v1/kv/docs/k1", b"one").await;
    assert_eq!(status, 200);
    let url = format!("ws://{http}/v1/query-stream?table=docs&limit=100&api_key={KEY}");
    let (mut stream, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let snapshot = next_text(&mut stream).await;
    assert_eq!(snapshot["type"], "snapshot");
    assert!(row_pks(&snapshot).contains(&String::from("k1")));
    let snapshot_commit = snapshot["commit"].as_u64().unwrap();
    let (status, _) = http_request(http, "PUT /v1/kv/docs/k2", b"two").await;
    assert_eq!(status, 200);
    let update = next_text(&mut stream).await;
    assert_eq!(update["type"], "update");
    assert!(update["commit"].as_u64().unwrap() > snapshot_commit);
    let pks = row_pks(&update);
    assert!(pks.contains(&String::from("k1")));
    assert!(pks.contains(&String::from("k2")));
    stream.close(None).await.unwrap();
    let url = format!("ws://{http}/v1/query-stream?table=docs&limit=100&api_key={KEY}");
    let (mut second, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let snapshot = next_text(&mut second).await;
    assert_eq!(snapshot["type"], "snapshot");
    let pks = row_pks(&snapshot);
    assert!(pks.contains(&String::from("k1")));
    assert!(pks.contains(&String::from("k2")));
    let (status, _) = http_request(http, "DELETE /v1/kv/docs/k1", b"").await;
    assert_eq!(status, 200);
    let update = next_text(&mut second).await;
    assert_eq!(update["type"], "update");
    let pks = row_pks(&update);
    assert!(!pks.contains(&String::from("k1")));
    assert!(pks.contains(&String::from("k2")));
    second.close(None).await.unwrap();
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn branch_realtime_streams_are_isolated_and_read_branch_snapshots() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-branch-stream-{}-{}",
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
    let mut config = test_config(&root);
    config.pg_listen = pg;
    config.resp_listen = resp;
    config.http_listen = http;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (status, _) = http_request(http, "PUT /v1/kv/docs/base", b"main-value").await;
    assert_eq!(status, 200);
    let branch_body = br#"{"id":"preview","parent":"main","base_commit_ts":0}"#;
    let (status, _) = http_request(http, "POST /v1/branches", branch_body).await;
    assert_eq!(status, 200);

    let stream_url = format!("ws://{http}/v1/stream?table=docs&branch=preview&api_key={KEY}");
    let (mut stream, _) = tokio_tungstenite::connect_async(stream_url).await.unwrap();
    let (status, _) =
        http_request_branch(http, "preview", "PUT /v1/kv/docs/branch", b"branch-value").await;
    assert_eq!(status, 200);
    let event = next_text(&mut stream).await;
    assert_eq!(event["branch"], "preview");
    assert_eq!(change_pk(&event), "branch");

    let (status, _) = http_request(http, "PUT /v1/kv/docs/main-only", b"main-value").await;
    assert_eq!(status, 200);
    assert!(tokio::time::timeout(Duration::from_millis(500), stream.next()).await.is_err());
    stream.close(None).await.unwrap();

    let query_url = format!("ws://{http}/v1/query-stream?table=docs&branch=preview&api_key={KEY}");
    let (mut query, _) = tokio_tungstenite::connect_async(query_url).await.unwrap();
    let snapshot = next_text(&mut query).await;
    assert_eq!(snapshot["type"], "snapshot");
    let pks = row_pks(&snapshot);
    assert!(pks.contains(&String::from("base")), "{snapshot}");
    assert!(pks.contains(&String::from("branch")), "{snapshot}");
    assert!(!pks.contains(&String::from("main-only")), "{snapshot}");
    query.close(None).await.unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

async fn put_commit(http: std::net::SocketAddr, table: &str, key: &str, value: &str) -> u64 {
    let (status, body) =
        http_request(http, &format!("PUT /v1/kv/{table}/{key}"), value.as_bytes()).await;
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    parsed.get("commit").and_then(|v| v.as_u64()).unwrap()
}

fn change_pk(message: &serde_json::Value) -> String {
    let bytes: Vec<u8> =
        serde_json::from_value(message.get("pk").cloned().unwrap_or_default()).unwrap_or_default();
    String::from_utf8(bytes).unwrap_or_default()
}

#[tokio::test]
async fn live_broadcast_stream_receives_posts() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!("ryme-cast-{}-{}", std::process::id(), now_ms()));
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
    let url = format!("ws://{http}/v1/broadcast/lobby?api_key={KEY}");
    let (mut stream, response) = tokio_tungstenite::connect_async(url).await.unwrap();
    assert_eq!(response.status(), 101);
    let (status, _) = http_request(
        http,
        "POST /v1/broadcast",
        b"{\"channel\":\"lobby\",\"payload\":{\"hello\":true}}",
    )
    .await;
    assert_eq!(status, 200);
    let message = next_text(&mut stream).await;
    assert_eq!(message.get("channel").and_then(|v| v.as_str()), Some("lobby"));
    assert_eq!(
        message.get("payload").and_then(|v| v.get("hello")),
        Some(&serde_json::Value::Bool(true))
    );
    assert!(message.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > 0);
    stream.close(None).await.unwrap();
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn supabase_realtime_protocol_joins_heartbeats_and_broadcasts() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-supabase-realtime-{}-{}",
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
    let mut config = test_config(&root);
    config.pg_listen = pg;
    config.resp_listen = resp;
    config.http_listen = http;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let url = format!("ws://{http}/realtime/v1/websocket?apikey={KEY}&vsn=1.0.0");
    let (mut stream, response) = tokio_tungstenite::connect_async(url).await.unwrap();
    assert_eq!(response.status(), 101);
    stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({
                "topic": "realtime:room",
                "event": "phx_join",
                "payload": { "config": { "broadcast": { "ack": true, "self": true } } },
                "ref": "1",
                "join_ref": "1"
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let joined = next_text(&mut stream).await;
    assert_eq!(joined["event"], "phx_reply");
    assert_eq!(joined["payload"]["status"], "ok");

    stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({
                "topic": "phoenix",
                "event": "heartbeat",
                "payload": {},
                "ref": "2"
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let heartbeat = next_text(&mut stream).await;
    assert_eq!(heartbeat["topic"], "phoenix");
    assert_eq!(heartbeat["event"], "phx_reply");

    stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({
                "topic": "realtime:database-changes",
                "event": "phx_join",
                "payload": { "config": { "postgres_changes": [{
                    "event": "INSERT",
                    "schema": "public",
                    "table": "messages",
                    "filter": "value=eq.hello"
                }] } },
                "ref": "changes-1",
                "join_ref": "changes-1"
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let changes_joined = next_text(&mut stream).await;
    assert_eq!(changes_joined["event"], "phx_reply");
    assert_eq!(changes_joined["payload"]["status"], "ok");
    assert_eq!(changes_joined["payload"]["response"]["postgres_changes"][0]["table"], "messages");
    let changes_system = next_text(&mut stream).await;
    assert_eq!(changes_system["event"], "system");
    assert_eq!(changes_system["payload"]["extension"], "postgres_changes");

    let (status, _) = http_request(http, "PUT /v1/kv/messages/m1", b"hello").await;
    assert_eq!(status, 200);
    let change = next_text(&mut stream).await;
    assert_eq!(change["event"], "postgres_changes");
    assert_eq!(change["payload"]["data"]["type"], "INSERT");
    assert_eq!(change["payload"]["data"]["table"], "messages");
    assert_eq!(change["payload"]["data"]["record"]["value"], "hello");
    assert_eq!(
        change["payload"]["ids"][0],
        changes_joined["payload"]["response"]["postgres_changes"][0]["id"]
    );

    stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({
                "topic": "realtime:room",
                "event": "broadcast",
                "payload": { "event": "chat", "payload": { "text": "hello" } },
                "ref": "3",
                "join_ref": "1"
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let first = next_text(&mut stream).await;
    let second = next_text(&mut stream).await;
    let broadcast = if first["event"] == "broadcast" { first.clone() } else { second.clone() };
    let ack = if first["event"] == "phx_reply" { first } else { second };
    assert_eq!(broadcast["event"], "broadcast");
    assert_eq!(broadcast["payload"]["event"], "chat");
    assert_eq!(broadcast["payload"]["payload"]["text"], "hello");
    assert_eq!(ack["event"], "phx_reply");
    assert_eq!(ack["payload"]["status"], "ok");

    stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({
                "topic": "realtime:presence-room",
                "event": "phx_join",
                "payload": { "config": { "presence": { "enabled": true, "key": "ada" } } },
                "ref": "presence-1",
                "join_ref": "presence-1"
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let presence_joined = next_text(&mut stream).await;
    assert_eq!(presence_joined["event"], "phx_reply");
    assert_eq!(presence_joined["payload"]["status"], "ok");
    let presence_state = next_text(&mut stream).await;
    assert_eq!(presence_state["event"], "presence_state");
    assert!(presence_state["payload"].as_object().unwrap().is_empty());

    stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({
                "topic": "realtime:presence-room",
                "event": "presence",
                "payload": {
                    "type": "presence",
                    "event": "track",
                    "payload": { "status": "online", "room": "lobby" }
                },
                "ref": "presence-2",
                "join_ref": "presence-1"
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let track_ack = next_text(&mut stream).await;
    assert_eq!(track_ack["event"], "phx_reply");
    assert_eq!(track_ack["payload"]["status"], "ok");
    let presence_diff = next_text(&mut stream).await;
    assert_eq!(presence_diff["event"], "presence_diff");
    assert_eq!(presence_diff["payload"]["joins"]["ada"]["metas"][0]["status"], "online");

    stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({
                "topic": "realtime:presence-room",
                "event": "presence",
                "payload": { "type": "presence", "event": "untrack", "payload": {} },
                "ref": "presence-3",
                "join_ref": "presence-1"
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let untrack_ack = next_text(&mut stream).await;
    assert_eq!(untrack_ack["event"], "phx_reply");
    assert_eq!(untrack_ack["payload"]["status"], "ok");
    let leave_diff = next_text(&mut stream).await;
    assert_eq!(leave_diff["event"], "presence_diff");
    assert!(leave_diff["payload"]["leaves"]["ada"].is_object());

    stream.close(None).await.unwrap();
    let v2_url = format!("ws://{http}/realtime/v1/websocket?apikey={KEY}&vsn=2.0.0");
    let (mut v2, response) = tokio_tungstenite::connect_async(v2_url).await.unwrap();
    assert_eq!(response.status(), 101);
    v2.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::json!([
            "7",
            "8",
            "realtime:array-room",
            "phx_join",
            { "config": { "broadcast": { "ack": false } } }
        ])
        .to_string(),
    ))
    .await
    .unwrap();
    let v2_joined = next_text(&mut v2).await;
    assert_eq!(v2_joined[3], "phx_reply");
    assert_eq!(v2_joined[4]["status"], "ok");
    v2.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::json!([null, "9", "phoenix", "heartbeat", {}]).to_string(),
    ))
    .await
    .unwrap();
    let v2_heartbeat = next_text(&mut v2).await;
    assert_eq!(v2_heartbeat[3], "phx_reply");
    v2.close(None).await.unwrap();
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn live_presence_stream_snapshots_and_follows_members() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-presence-stream-{}-{}",
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
    let mut config = test_config(&root);
    config.pg_listen = pg;
    config.resp_listen = resp;
    config.http_listen = http;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let url = format!("ws://{http}/v1/presence/room/stream?api_key={KEY}");
    let (mut stream, response) = tokio_tungstenite::connect_async(url).await.unwrap();
    assert_eq!(response.status(), 101);
    let snapshot = next_text(&mut stream).await;
    assert_eq!(snapshot["type"], "presence_state");
    assert_eq!(snapshot["members"].as_array().map(Vec::len), Some(0));

    let (status, _) = http_request(
        http,
        "POST /v1/presence/join",
        br#"{"channel":"room","member":"ada","state":{"typing":true},"ttl_secs":60}"#,
    )
    .await;
    assert_eq!(status, 200);
    let joined = next_text(&mut stream).await;
    assert_eq!(joined["type"], "join");
    assert_eq!(joined["member"], "ada");
    assert_eq!(joined["state"]["typing"], true);

    let (status, _) =
        http_request(http, "POST /v1/presence/leave", br#"{"channel":"room","member":"ada"}"#)
            .await;
    assert_eq!(status, 200);
    let left = next_text(&mut stream).await;
    assert_eq!(left["type"], "leave");
    assert_eq!(left["member"], "ada");
    stream.close(None).await.unwrap();
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn live_durable_topic_stream_replays_and_follows() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-topic-stream-{}-{}", std::process::id(), now_ms()));
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
    for (key, value) in [("k1", "one"), ("k2", "two")] {
        let body = format!(
            "{{\"partition\":\"chat\",\"key\":\"{key}\",\"value\":\"{value}\",\"retention\":16}}"
        );
        let (status, _) = http_request(http, "POST /v1/topics/append", body.as_bytes()).await;
        assert_eq!(status, 200);
    }
    let url = format!("ws://{http}/v1/topics/chat/stream?from=0&api_key={KEY}");
    let (mut stream, response) = tokio_tungstenite::connect_async(url).await.unwrap();
    assert_eq!(response.status(), 101);
    let first = next_text(&mut stream).await;
    let second = next_text(&mut stream).await;
    assert_eq!(first.get("cursor").and_then(|value| value.as_u64()), Some(0));
    assert_eq!(second.get("cursor").and_then(|value| value.as_u64()), Some(1));
    assert_eq!(second.get("value").and_then(|value| value.as_str()), Some("two"));
    let body = br#"{"partition":"chat","key":"k3","value":"three","retention":16}"#;
    let (status, _) = http_request(http, "POST /v1/topics/append", body).await;
    assert_eq!(status, 200);
    let live = next_text(&mut stream).await;
    assert_eq!(live.get("cursor").and_then(|value| value.as_u64()), Some(2));
    assert_eq!(live.get("value").and_then(|value| value.as_str()), Some("three"));
    stream.close(None).await.unwrap();
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn live_cdc_stream_resumes_from_commit() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-resume-{}-{}", std::process::id(), now_ms()));
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
    let _ = put_commit(http, "docs", "r1", "one").await;
    let url = format!("ws://{http}/v1/stream?table=docs&api_key={KEY}");
    let (mut stream, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let _ = put_commit(http, "docs", "r2", "two").await;
    let second = next_text(&mut stream).await;
    let commit2 = second.get("commit_ts").and_then(|v| v.as_u64()).unwrap();
    assert_eq!(change_pk(&second), "r2", "{second}");
    stream.close(None).await.unwrap();
    let commit3 = put_commit(http, "docs", "r3", "three").await;
    assert!(commit3 > commit2);
    let url = format!("ws://{http}/v1/stream?table=docs&api_key={KEY}&from={commit2}");
    let (mut resumed, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let replayed = next_text(&mut resumed).await;
    assert_eq!(change_pk(&replayed), "r3", "{replayed}");
    assert_eq!(replayed.get("commit_ts").and_then(|v| v.as_u64()), Some(commit3));
    let _ = put_commit(http, "docs", "r4", "four").await;
    let live = next_text(&mut resumed).await;
    assert_eq!(change_pk(&live), "r4", "{live}");
    let sequence4 = live.get("sequence").and_then(|v| v.as_u64()).unwrap();
    resumed.close(None).await.unwrap();
    let _ = put_commit(http, "docs", "r5", "five").await;
    let url = format!("ws://{http}/v1/stream?table=docs&api_key={KEY}&from_sequence={sequence4}");
    let (mut exact, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let replayed = next_text(&mut exact).await;
    assert_eq!(change_pk(&replayed), "r5", "{replayed}");
    exact.close(None).await.unwrap();
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn live_query_snapshot_limit_capped() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!("ryme-qlim-{}-{}", std::process::id(), now_ms()));
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
    let mut rows = String::from("{\"table\":\"big\",\"rows\":[");
    for index in 0..1200 {
        if index > 0 {
            rows.push(',');
        }
        rows.push_str(&format!("{{\"key\":\"q-{index:04}\",\"value\":\"v\"}}"));
    }
    rows.push_str("]}");
    let (status, _) = http_request(http, "POST /v1/sql/copy", rows.as_bytes()).await;
    assert_eq!(status, 200);
    let url = format!("ws://{http}/v1/query-stream?table=big&limit=999999&api_key={KEY}");
    let (mut stream, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let snapshot = next_text(&mut stream).await;
    assert_eq!(snapshot["type"], "snapshot");
    assert_eq!(row_pks(&snapshot).len(), 1000, "snapshot capped at 1000");
    stream.close(None).await.unwrap();
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn live_stream_sheds_on_egress_quota() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!("ryme-shed-{}-{}", std::process::id(), now_ms()));
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
    let url = format!("ws://{http}/v1/stream?table=docs&api_key={KEY}");
    let (mut stream, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let big = "v".repeat(512 * 1024);
    let mut received = 0usize;
    let start = tokio::time::Instant::now();
    for index in 0..4 {
        let _ = http_request(http, &format!("PUT /v1/kv/docs/shed-{index}"), big.as_bytes()).await;
        match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(_)))) => {
                received += 1;
            }
            _ => break,
        }
    }
    assert!((1..4).contains(&received), "shed {received} of 4 bulk messages");
    assert!(start.elapsed() < Duration::from_secs(14), "stream never shed");
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}
