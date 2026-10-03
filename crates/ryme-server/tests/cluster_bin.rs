use ryme_config::{Config, RaftPeer};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: &str = "ryme-cluster-bin-key-7e2a4c91b5d3";

fn alloc_addrs(
) -> Vec<(std::net::SocketAddr, std::net::SocketAddr, std::net::SocketAddr, std::net::SocketAddr)> {
    let mut held = Vec::new();
    for _ in 0..3 {
        held.push((
            std::net::TcpListener::bind("127.0.0.1:0").unwrap(),
            std::net::TcpListener::bind("127.0.0.1:0").unwrap(),
            std::net::TcpListener::bind("127.0.0.1:0").unwrap(),
            std::net::TcpListener::bind("127.0.0.1:0").unwrap(),
        ));
    }
    let addrs = held
        .iter()
        .map(|(a, b, c, d)| {
            (
                a.local_addr().unwrap(),
                b.local_addr().unwrap(),
                c.local_addr().unwrap(),
                d.local_addr().unwrap(),
            )
        })
        .collect();
    drop(held);
    addrs
}

struct Child {
    child: std::process::Child,
}

impl Child {
    fn spawn(config_path: &std::path::Path) -> Self {
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_ryme-server"))
            .arg("--config")
            .arg(config_path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        Self { child }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn http_request(
    addr: std::net::SocketAddr,
    head: &str,
    body: &[u8],
) -> Option<(u16, Vec<u8>)> {
    let mut socket = tokio::net::TcpStream::connect(addr).await.ok()?;
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\nauthorization: Bearer {KEY}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(request.as_bytes()).await.ok()?;
    socket.write_all(body).await.ok()?;
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 8192];
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let read = socket.read(&mut chunk).await.unwrap_or(0);
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
        }
    })
    .await
    .ok()?;
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
    Some((status, body))
}

async fn is_leader(http: std::net::SocketAddr) -> bool {
    let Ok(Some((status, body))) =
        tokio::time::timeout(Duration::from_secs(2), http_request(http, "GET /ready", b"")).await
    else {
        return false;
    };
    status == 200 && String::from_utf8_lossy(&body).contains("\"leader\":true")
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
        node_id: format!("bin-{index}"),
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

#[tokio::test]
async fn cluster_boots_from_binary_config() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-cluster-bin-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let addrs = alloc_addrs();
    let rafts: Vec<std::net::SocketAddr> = addrs.iter().map(|a| a.3).collect();
    let mut children = Vec::new();
    for (index, (pg, resp, http, raft)) in addrs.iter().enumerate() {
        let config = node_config(&root, index, *pg, *resp, *http, *raft, &rafts);
        let path = root.join(format!("n{index}.json"));
        std::fs::write(&path, serde_json::to_string(&config).unwrap()).unwrap();
        children.push(Child::spawn(&path));
    }
    let https: Vec<std::net::SocketAddr> = addrs.iter().map(|a| a.2).collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let leader = loop {
        let mut found = None;
        for (index, http) in https.iter().enumerate() {
            if is_leader(*http).await {
                found = Some(index);
                break;
            }
        }
        if let Some(index) = found {
            break index;
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let (status, _) = http_request(https[leader], "PUT /v1/kv/docs/binkey", b"{\"n\":3}")
        .await
        .expect("leader reachable");
    assert_eq!(status, 200);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut done = true;
        for http in &https {
            match http_request(*http, "GET /v1/kv/docs/binkey", b"").await {
                Some((status, body)) if status == 200 && body == b"{\"n\":3}" => {}
                _ => done = false,
            }
        }
        if done {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "no convergence");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    drop(children);
    let _ = std::fs::remove_dir_all(&root);
}
