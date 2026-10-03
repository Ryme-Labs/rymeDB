use ryme_config::{Config, RaftPeer};
use std::time::Duration;

const KEY: &str = "ryme-hybrid-e2e-key-7f2a9c4d1b58";

async fn bind_node() -> (
    tokio::net::TcpListener,
    tokio::net::TcpListener,
    tokio::net::TcpListener,
    tokio::net::TcpListener,
) {
    (
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
    )
}

fn node_config(
    dir: &std::path::Path,
    index: usize,
    pg: std::net::SocketAddr,
    resp: std::net::SocketAddr,
    http: std::net::SocketAddr,
    raft: std::net::SocketAddr,
    rafts: &[std::net::SocketAddr],
) -> Config {
    let mut base = Config::default();
    base.archive.interval_secs = 0;
    base.sweep_interval_secs = 0;
    Config {
        node_id: format!("hybrid-{index}"),
        data_dir: dir.join(format!("n{index}")),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        archive: base.archive,
        replicated_tables: vec![String::from("syscfg")],
        cluster: ryme_config::ClusterConfig {
            node_index: index,
            raft_listen: Some(raft),
            learner: false,
            advertise_addr: None,
            peers: rafts
                .iter()
                .copied()
                .enumerate()
                .filter(|(i, _)| *i != index)
                .map(|(i, addr)| RaftPeer { id: i, addr: addr.to_string() })
                .collect(),
        },
        ..Config::default()
    }
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

async fn is_leader(http: std::net::SocketAddr) -> bool {
    let Ok((status, body)) =
        tokio::time::timeout(Duration::from_secs(2), http_request(http, "GET /ready", b"")).await
    else {
        return false;
    };
    status == 200 && String::from_utf8_lossy(&body).contains("\"leader\":true")
}

async fn wait_leader(https: &[std::net::SocketAddr], skip: Option<usize>) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        for (index, http) in https.iter().enumerate() {
            if Some(index) == skip {
                continue;
            }
            if is_leader(*http).await {
                return index;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_value(https: &[std::net::SocketAddr], table: &str, key: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut done = true;
        for http in https {
            let (status, body) =
                http_request(*http, &format!("GET /v1/kv/{table}/{key}"), b"").await;
            if status != 200 || body != value.as_bytes() {
                done = false;
            }
        }
        if done {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "no convergence for {key}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn hybrid_tiers_replicate_and_isolate() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-hybrid-e2e-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut bound = Vec::new();
    for _ in 0..3 {
        bound.push(bind_node().await);
    }
    let mut pg = Vec::new();
    let mut resp = Vec::new();
    let mut http = Vec::new();
    let mut raft = Vec::new();
    for node in &bound {
        pg.push(node.0.local_addr().unwrap());
        resp.push(node.1.local_addr().unwrap());
        http.push(node.2.local_addr().unwrap());
        raft.push(node.3.local_addr().unwrap());
    }
    let mut handles = Vec::new();
    for (index, (pg_listener, resp_listener, http_listener, raft_listener)) in
        bound.drain(..).enumerate()
    {
        let config =
            node_config(&root, index, pg[index], resp[index], http[index], raft[index], &raft);
        handles.push(
            ryme_server::serve_cluster(
                config,
                pg_listener,
                resp_listener,
                http_listener,
                raft_listener,
            )
            .await
            .unwrap(),
        );
    }
    let leader = wait_leader(&http, None).await;
    let (status, _) = http_request(http[leader], "PUT /v1/kv/syscfg/flag", b"on").await;
    assert_eq!(status, 200);
    wait_value(&http, "syscfg", "flag", "on").await;
    let (status, _) = http_request(http[leader], "PUT /v1/kv/cache/slot", b"tmp").await;
    assert_eq!(status, 200);
    let (status, body) = http_request(http[leader], "GET /v1/kv/cache/slot", b"").await;
    assert_eq!(status, 200);
    assert_eq!(body, b"tmp");
    for (index, addr) in http.iter().enumerate() {
        if index == leader {
            continue;
        }
        let (status, _) = http_request(*addr, "GET /v1/kv/cache/slot", b"").await;
        assert_eq!(status, 404);
    }
    let (status, body) = http_request(http[leader], "GET /v1/shards", b"").await;
    assert_eq!(status, 200);
    let layout = String::from_utf8_lossy(&body).into_owned();
    assert!(layout.contains("\"mode\":\"hybrid\""));
    assert!(layout.contains("\"tier\":\"replicated\""));
    assert!(layout.contains("\"tier\":\"local\""));
    handles.remove(leader).shutdown();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let second = wait_leader(&http, Some(leader)).await;
    assert_ne!(leader, second);
    let (status, _) = http_request(http[second], "PUT /v1/kv/syscfg/flag2", b"on2").await;
    assert_eq!(status, 200);
    let live: Vec<std::net::SocketAddr> =
        http.iter().enumerate().filter(|(i, _)| *i != leader).map(|(_, a)| *a).collect();
    wait_value(&live, "syscfg", "flag2", "on2").await;
    for handle in handles {
        handle.shutdown();
    }
    let _ = std::fs::remove_dir_all(&root);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
