use futures_util::StreamExt;
use ryme_config::{Config, RaftPeer};
use std::time::Duration;

const KEY: &str = "ryme-cluster-e2e-key-9d4c2b7a1f60";

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

async fn bind_addr(addr: std::net::SocketAddr) -> tokio::net::TcpListener {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => return listener,
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("bind {addr}: {e}"),
        }
    }
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
    Config {
        node_id: format!("e2e-{index}"),
        data_dir: dir.join(format!("n{index}")),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        archive: base.archive,
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

async fn wait_value(https: &[std::net::SocketAddr], key: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut done = true;
        for http in https {
            let (status, body) = http_request(*http, &format!("GET /v1/kv/docs/{key}"), b"").await;
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

async fn wait_ranges(https: &[std::net::SocketAddr]) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut converged = true;
        for http in https {
            let (status, body) = http_request(*http, "GET /v1/ranges", b"").await;
            let text = String::from_utf8_lossy(&body);
            if status != 200 || !text.contains("range-left") || !text.contains("range-right") {
                converged = false;
                break;
            }
        }
        if converged {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "range metadata did not converge");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn cluster_write_failover_restart() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-cluster-e2e-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut bound = Vec::new();
    for _ in 0..3 {
        bound.push(bind_node().await);
    }
    let addr_of = |index: usize| -> (
        std::net::SocketAddr,
        std::net::SocketAddr,
        std::net::SocketAddr,
        std::net::SocketAddr,
    ) {
        (
            bound[index].0.local_addr().unwrap(),
            bound[index].1.local_addr().unwrap(),
            bound[index].2.local_addr().unwrap(),
            bound[index].3.local_addr().unwrap(),
        )
    };
    let mut pg = Vec::new();
    let mut resp = Vec::new();
    let mut http = Vec::new();
    let mut raft = Vec::new();
    for index in 0..3 {
        let (a, b, c, d) = addr_of(index);
        pg.push(a);
        resp.push(b);
        http.push(c);
        raft.push(d);
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
    let first = wait_leader(&http, None).await;
    let (status, _) = http_request(http[first], "PUT /v1/kv/docs/dkey", b"{\"n\":1}").await;
    assert_eq!(status, 200);
    wait_value(&http, "dkey", "{\"n\":1}").await;
    handles.remove(first).shutdown();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let second = wait_leader(&http, Some(first)).await;
    assert_ne!(first, second);
    let (status, _) = http_request(http[second], "PUT /v1/kv/docs/dkey2", b"{\"n\":2}").await;
    assert_eq!(status, 200);
    let live: Vec<std::net::SocketAddr> =
        http.iter().enumerate().filter(|(i, _)| *i != first).map(|(_, a)| *a).collect();
    wait_value(&live, "dkey2", "{\"n\":2}").await;
    let config = node_config(&root, first, pg[first], resp[first], http[first], raft[first], &raft);
    let pg_listener = bind_addr(pg[first]).await;
    let resp_listener = bind_addr(resp[first]).await;
    let http_listener = bind_addr(http[first]).await;
    let raft_listener = bind_addr(raft[first]).await;
    let revived = ryme_server::serve_cluster(
        config,
        pg_listener,
        resp_listener,
        http_listener,
        raft_listener,
    )
    .await
    .unwrap();
    wait_value(&http, "dkey2", "{\"n\":2}").await;
    revived.shutdown();
    for handle in handles {
        handle.shutdown();
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn cluster_replication_latency() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-cluster-lat-{}-{}", std::process::id(), now_ms()));
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
    let follower = (0..3).find(|index| *index != leader).unwrap_or(1);
    let mut ack_micros = Vec::new();
    let mut visible_micros = Vec::new();
    for round in 0..20 {
        let key = format!("lat-{round}");
        let body = format!("{{\"n\":{round}}}");
        let start = std::time::Instant::now();
        let (status, _) =
            http_request(http[leader], &format!("PUT /v1/kv/docs/{key}"), body.as_bytes()).await;
        assert_eq!(status, 200);
        ack_micros.push(start.elapsed().as_micros() as u64);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (status, seen) =
                http_request(http[follower], &format!("GET /v1/kv/docs/{key}"), b"").await;
            if status == 200 && seen == body.as_bytes() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "no replication for {key}");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        visible_micros.push(start.elapsed().as_micros() as u64);
    }
    ack_micros.sort_unstable();
    visible_micros.sort_unstable();
    let ack_median = ack_micros[ack_micros.len() / 2];
    let visible_median = visible_micros[visible_micros.len() / 2];
    let visible_max = visible_micros[visible_micros.len() - 1];
    eprintln!("leader_ack_median_us={ack_median} replica_visible_median_us={visible_median} replica_visible_max_us={visible_max}");
    assert!(visible_max < 2_000_000, "replication stalled: {visible_max}us");
    assert!(visible_median < 100_000, "replica median too slow: {visible_median}us");
    for handle in handles {
        handle.shutdown();
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn cluster_range_split_replicates_metadata() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-cluster-ranges-{}-{}",
        std::process::id(),
        now_ms()
    ));
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
    let body = br#"{"id":"range-0","mid":"t","left_id":"range-left","right_id":"range-right","expected_epoch":0}"#;
    let (status, _) = http_request(http[leader], "POST /v1/ranges/split", body).await;
    assert_eq!(status, 200);
    wait_ranges(&http).await;
    for handle in handles {
        handle.shutdown();
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn cluster_broadcast_reaches_every_gateway() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-cluster-broadcast-{}-{}",
        std::process::id(),
        now_ms()
    ));
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
    let mut sockets = Vec::new();
    for address in &http {
        let url = format!("ws://{address}/v1/broadcast/cluster-chat?api_key={KEY}");
        let (socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        sockets.push(socket);
    }
    let body = br#"{"channel":"cluster-chat","from":"cluster-test","payload":{"message":"hello"}}"#;
    let (status, response) = http_request(http[leader], "POST /v1/broadcast", body).await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&response).contains("sequence"));
    for mut socket in sockets {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match message {
            tokio_tungstenite::tungstenite::Message::Text(text) => {
                assert!(text.contains("hello"), "unexpected broadcast: {text}");
            }
            other => panic!("unexpected websocket message: {other:?}"),
        }
    }
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
