use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn stub_once() -> (std::net::SocketAddr, tokio::sync::oneshot::Receiver<Vec<u8>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (done, waiting) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let Ok((mut socket, _)) = listener.accept().await else { return };
        let mut raw = Vec::new();
        let mut chunk = vec![0u8; 8192];
        let length = loop {
            let read = socket.read(&mut chunk).await.unwrap_or(0);
            if read == 0 {
                break 0;
            }
            raw.extend_from_slice(&chunk[..read]);
            if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n").map(|index| index + 4)
            {
                let head = String::from_utf8_lossy(&raw[..end]).into_owned();
                let mut found = 0;
                for line in head.lines().skip(1) {
                    if line.is_empty() {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.trim().eq_ignore_ascii_case("content-length") {
                            found = value.trim().parse().unwrap_or(0);
                        }
                    }
                }
                break found;
            }
        };
        let mut body = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|index| raw[index + 4..].to_vec())
            .unwrap_or_default();
        while body.len() < length {
            let read = socket.read(&mut chunk).await.unwrap_or(0);
            if read == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..read]);
        }
        let head = String::from_utf8_lossy(&raw).into_owned();
        let request_line = head.lines().next().unwrap_or("").to_string();
        let mut seen = format!("{request_line}\n").into_bytes();
        seen.extend_from_slice(&body);
        let _ = socket
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}")
            .await;
        let _ = done.send(seen);
    });
    (addr, waiting)
}

#[tokio::test]
async fn otlp_export_posts_resource_spans() {
    let (addr, waiting) = stub_once().await;
    let traces = Arc::new(Mutex::new(ryme_observe::TraceCollector::new(16)));
    {
        let mut collector = traces.lock().unwrap();
        let mut first = ryme_observe::TraceSpan::root(String::from("kv_get"), 1_700_000_000);
        first.attr(String::from("table"), String::from("docs"));
        first.finish(120);
        collector.push(first);
        collector.push(ryme_observe::TraceSpan::root(String::from("kv_put"), 1_700_000_001));
    }
    let client = reqwest::Client::new();
    let endpoint = format!("http://{addr}");
    let exported =
        ryme_server::export_traces_once(&traces, &client, &endpoint, "rymedb-test").await.unwrap();
    assert_eq!(exported, 2);
    assert_eq!(traces.lock().unwrap().len(), 0);
    let seen =
        tokio::time::timeout(std::time::Duration::from_secs(5), waiting).await.unwrap().unwrap();
    let text = String::from_utf8_lossy(&seen).into_owned();
    assert!(text.starts_with("POST /v1/traces "), "{text}");
    assert!(text.contains("\"service.name\""), "{text}");
    assert!(text.contains("rymedb-test"), "{text}");
    assert!(text.contains("\"kv_get\""), "{text}");
    assert!(text.contains("\"table\""), "{text}");
}

#[tokio::test]
async fn otlp_export_empty_is_noop() {
    let traces = Arc::new(Mutex::new(ryme_observe::TraceCollector::new(16)));
    let client = reqwest::Client::new();
    let exported =
        ryme_server::export_traces_once(&traces, &client, "http://127.0.0.1:1", "x").await.unwrap();
    assert_eq!(exported, 0);
}
