use ryme_config::{Config, RaftPeer};
use std::time::Duration;

const KEY: &str = "ryme-membership-e2e-key-4b9c1d7e2a63";

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

struct NodePorts {
    pg: std::net::SocketAddr,
    resp: std::net::SocketAddr,
    http: std::net::SocketAddr,
    raft: std::net::SocketAddr,
}

fn node_config(
    dir: &std::path::Path,
    index: usize,
    count: usize,
    ports: &NodePorts,
    rafts: &[std::net::SocketAddr],
    learner: bool,
) -> Config {
    let mut base = Config::default();
    base.archive.interval_secs = 0;
    Config {
        node_id: format!("mem-{index}"),
        data_dir: dir.join(format!("n{index}")),
        pg_listen: ports.pg,
        resp_listen: ports.resp,
        http_listen: ports.http,
        archive: base.archive,
        cluster: ryme_config::ClusterConfig {
            node_index: index,
            raft_listen: Some(ports.raft),
            learner,
            advertise_addr: Some(ports.raft.to_string()),
            peers: rafts
                .iter()
                .copied()
                .enumerate()
                .filter(|(i, _)| *i != index && *i < count)
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
    tokio::time::timeout(Duration::from_secs(15), async {
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

async fn wait_leader(https: &[std::net::SocketAddr]) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        for (index, http) in https.iter().enumerate() {
            if is_leader(*http).await {
                return index;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn send_to_leader(https: &[std::net::SocketAddr], head: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        for http in https {
            if !is_leader(*http).await {
                continue;
            }
            let (status, payload) = http_request(*http, head, body).await;
            if status != 503 {
                return (status, payload);
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader accepted {head}");
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

async fn bearer_request(
    addr: std::net::SocketAddr,
    token: &str,
    head: &str,
    body: &[u8],
) -> (u16, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\nauthorization: Bearer {token}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 8192];
    tokio::time::timeout(Duration::from_secs(15), async {
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

#[tokio::test]
async fn membership_rejects_non_admin_mutations() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-membership-rbac-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let bound = bind_node().await;
    let ports = NodePorts {
        pg: bound.0.local_addr().unwrap(),
        resp: bound.1.local_addr().unwrap(),
        http: bound.2.local_addr().unwrap(),
        raft: bound.3.local_addr().unwrap(),
    };
    let rafts = vec![ports.raft];
    let config = node_config(&root, 0, 1, &ports, &rafts, false);
    let handle =
        ryme_server::serve_cluster(config, bound.0, bound.1, bound.2, bound.3).await.unwrap();
    wait_leader(&[ports.http]).await;
    let (status, _) = http_request(
        ports.http,
        "POST /v1/auth/register",
        b"{\"id\":\"app\",\"password\":\"correct-horse\"}",
    )
    .await;
    assert_eq!(status, 201);
    let (status, body) = http_request(
        ports.http,
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
        ports.http,
        &app_key,
        "POST /v1/cluster/members",
        b"{\"id\":9,\"addr\":\"127.0.0.1:9999\"}",
    )
    .await;
    assert_eq!(status, 403);
    let (status, _) =
        bearer_request(ports.http, &app_key, "POST /v1/cluster/transfer", b"{\"target\":0}").await;
    assert_eq!(status, 403);
    let (status, _) =
        bearer_request(ports.http, &app_key, "POST /v1/cluster/replace", b"{\"members\":[]}").await;
    assert_eq!(status, 403);
    let (status, _) =
        bearer_request(ports.http, &app_key, "DELETE /v1/cluster/members/9", b"").await;
    assert_eq!(status, 403);
    let (status, _) = http_request(ports.http, "GET /v1/cluster/members", b"").await;
    assert_eq!(status, 200);
    handle.shutdown();
    let _ = std::fs::remove_dir_all(&root);
}

async fn member_addrs(http: std::net::SocketAddr) -> Option<Vec<String>> {
    let (status, body) = http_request(http, "GET /v1/cluster/members", b"").await;
    if status != 200 {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&body).ok()?;
    value
        .get("members")?
        .as_array()?
        .iter()
        .map(|member| member.get("addr")?.as_str().map(String::from))
        .collect()
}

async fn wait_addrs(http: std::net::SocketAddr, count: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(addrs) = member_addrs(http).await {
            if addrs.len() == count && addrs.iter().all(|addr| !addr.is_empty()) {
                return;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no membership convergence");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn membership_add_remove_via_http() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-membership-e2e-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let mut bound = Vec::new();
    for _ in 0..4 {
        bound.push(bind_node().await);
    }
    let mut ports = Vec::new();
    for node in &bound {
        ports.push(NodePorts {
            pg: node.0.local_addr().unwrap(),
            resp: node.1.local_addr().unwrap(),
            http: node.2.local_addr().unwrap(),
            raft: node.3.local_addr().unwrap(),
        });
    }
    let https: Vec<std::net::SocketAddr> = ports.iter().map(|p| p.http).collect();
    let rafts: Vec<std::net::SocketAddr> = ports.iter().map(|p| p.raft).collect();
    let mut listeners: Vec<_> = std::mem::take(&mut bound);
    let mut handles = Vec::new();
    for (index, node_ports) in ports.iter().take(3).enumerate() {
        let config = node_config(&root, index, 3, node_ports, &rafts, false);
        let (pg_listener, resp_listener, http_listener, raft_listener) = listeners.remove(0);
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
    let base: Vec<std::net::SocketAddr> = https[..3].to_vec();
    let first = wait_leader(&base).await;
    wait_addrs(https[first], 3).await;
    let config = node_config(&root, 3, 4, &ports[3], &rafts, true);
    let (pg_listener, resp_listener, http_listener, raft_listener) = listeners.remove(0);
    let joining = ryme_server::serve_cluster(
        config,
        pg_listener,
        resp_listener,
        http_listener,
        raft_listener,
    )
    .await
    .unwrap();
    let add = format!("{{\"id\":3,\"addr\":\"{}\"}}", rafts[3]);
    let (status, _) = send_to_leader(&base, "POST /v1/cluster/members", add.as_bytes()).await;
    assert_eq!(status, 200);
    wait_addrs(https[first], 4).await;
    let (status, _) = http_request(https[first], "PUT /v1/kv/docs/mkey", b"{\"n\":7}").await;
    assert_eq!(status, 200);
    wait_value(&https, "mkey", "{\"n\":7}").await;
    let (status, _) = send_to_leader(&base, "DELETE /v1/cluster/members/3", b"").await;
    assert_eq!(status, 200);
    wait_addrs(https[first], 3).await;
    joining.shutdown();
    for handle in handles {
        handle.shutdown();
    }
    let _ = std::fs::remove_dir_all(&root);
}

async fn wait_leader_moved(https: &[std::net::SocketAddr], prev: usize) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        for (index, http) in https.iter().enumerate() {
            if index != prev && is_leader(*http).await {
                return index;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no transfer");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn membership_transfer_replace_via_http() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root =
        std::env::temp_dir().join(format!("ryme-transfer-e2e-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut bound = Vec::new();
    for _ in 0..3 {
        bound.push(bind_node().await);
    }
    let mut ports = Vec::new();
    for node in &bound {
        ports.push(NodePorts {
            pg: node.0.local_addr().unwrap(),
            resp: node.1.local_addr().unwrap(),
            http: node.2.local_addr().unwrap(),
            raft: node.3.local_addr().unwrap(),
        });
    }
    let https: Vec<std::net::SocketAddr> = ports.iter().map(|p| p.http).collect();
    let rafts: Vec<std::net::SocketAddr> = ports.iter().map(|p| p.raft).collect();
    let mut handles = Vec::new();
    for (index, node_ports) in ports.iter().enumerate() {
        let config = node_config(&root, index, 3, node_ports, &rafts, false);
        let (pg_listener, resp_listener, http_listener, raft_listener) = bound.remove(0);
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
    let first = wait_leader(&https).await;
    wait_addrs(https[first], 3).await;
    let target = (first + 1) % 3;
    let body = format!("{{\"target\":{target}}}");
    let (status, _) = send_to_leader(&https, "POST /v1/cluster/transfer", body.as_bytes()).await;
    assert_eq!(status, 200);
    let second = wait_leader_moved(&https, first).await;
    let keep: Vec<usize> = (0..3).filter(|index| *index != first).collect();
    let members = keep
        .iter()
        .map(|index| format!("{{\"id\":{index},\"addr\":\"{}\"}}", rafts[*index]))
        .collect::<Vec<_>>()
        .join(",");
    let body = format!("{{\"members\":[{members}]}}");
    let (status, _) = send_to_leader(&https, "POST /v1/cluster/replace", body.as_bytes()).await;
    assert_eq!(status, 200);
    wait_addrs(https[second], 2).await;
    let kept: Vec<std::net::SocketAddr> = keep.iter().map(|index| https[*index]).collect();
    let (status, _) = http_request(https[second], "PUT /v1/kv/docs/tkey", b"{\"n\":9}").await;
    assert_eq!(status, 200);
    wait_value(&kept, "tkey", "{\"n\":9}").await;
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

#[tokio::test]
async fn membership_rejects_invalid_mutations() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-membership-neg-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let mut bound = Vec::new();
    for _ in 0..3 {
        bound.push(bind_node().await);
    }
    let mut ports = Vec::new();
    for node in &bound {
        ports.push(NodePorts {
            pg: node.0.local_addr().unwrap(),
            resp: node.1.local_addr().unwrap(),
            http: node.2.local_addr().unwrap(),
            raft: node.3.local_addr().unwrap(),
        });
    }
    let https: Vec<std::net::SocketAddr> = ports.iter().map(|p| p.http).collect();
    let rafts: Vec<std::net::SocketAddr> = ports.iter().map(|p| p.raft).collect();
    let mut handles = Vec::new();
    for (index, node_ports) in ports.iter().enumerate() {
        let config = node_config(&root, index, 3, node_ports, &rafts, false);
        let (pg_listener, resp_listener, http_listener, raft_listener) = bound.remove(0);
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
    let base: Vec<std::net::SocketAddr> = https.clone();
    wait_leader(&base).await;
    let (status, _) = send_to_leader(&base, "DELETE /v1/cluster/members/99", b"").await;
    assert_eq!(status, 404);
    let (status, _) = send_to_leader(&base, "POST /v1/cluster/replace", b"{\"members\":[]}").await;
    assert_eq!(status, 400);
    let dup = format!("{{\"id\":0,\"addr\":\"{}\"}}", rafts[0]);
    let (status, _) = send_to_leader(&base, "POST /v1/cluster/members", dup.as_bytes()).await;
    assert_eq!(status, 400);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let leader = wait_leader(&base).await;
        let head = format!("DELETE /v1/cluster/members/{leader}");
        let (status, _) = http_request(https[leader], &head, b"").await;
        if status == 503 {
            assert!(tokio::time::Instant::now() < deadline, "self-removal never reached a leader");
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        assert_eq!(status, 400, "{head}");
        break;
    }
    for handle in handles {
        handle.shutdown();
    }
    let _ = std::fs::remove_dir_all(&root);
}
