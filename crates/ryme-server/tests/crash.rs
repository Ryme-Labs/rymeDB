use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn alloc_ports() -> (std::net::SocketAddr, std::net::SocketAddr, std::net::SocketAddr) {
    let a = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let b = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let c = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addrs = (a.local_addr().unwrap(), b.local_addr().unwrap(), c.local_addr().unwrap());
    drop((a, b, c));
    addrs
}

struct Server {
    child: std::process::Child,
    resp: std::net::SocketAddr,
    http: std::net::SocketAddr,
}

impl Server {
    fn start(dir: &std::path::Path) -> Self {
        let (pg, resp, http) = alloc_ports();
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_ryme-server"))
            .arg("--pg-listen")
            .arg(pg.to_string())
            .arg("--resp-listen")
            .arg(resp.to_string())
            .arg("--http-listen")
            .arg(http.to_string())
            .arg("--data-dir")
            .arg(dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        Self { child, resp, http }
    }

    async fn wait_healthy(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(mut socket) = tokio::net::TcpStream::connect(self.http).await {
                let request = b"GET /health HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n";
                if socket.write_all(request).await.is_ok() {
                    let mut raw = Vec::new();
                    let mut chunk = vec![0u8; 1024];
                    if tokio::time::timeout(Duration::from_secs(2), async {
                        loop {
                            let read = socket.read(&mut chunk).await.unwrap_or(0);
                            if read == 0 {
                                break;
                            }
                            raw.extend_from_slice(&chunk[..read]);
                        }
                    })
                    .await
                    .is_ok()
                        && String::from_utf8_lossy(&raw).contains("200")
                    {
                        return;
                    }
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "server never healthy");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn resp(&self, parts: &[&str]) -> String {
        let mut socket = tokio::net::TcpStream::connect(self.resp).await.unwrap();
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
        String::from_utf8(raw).unwrap()
    }

    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

#[tokio::test]
async fn sigkill_then_recover() {
    let root = std::env::temp_dir().join(format!("ryme-crash-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut server = Server::start(&root);
    server.wait_healthy().await;
    for index in 0..500 {
        let key = format!("k-{index:04}");
        let value = format!("v-{index:04}");
        let reply = server.resp(&["SET", &key, &value]).await;
        assert!(reply.starts_with("+OK"), "{reply}");
    }
    let reply = server.resp(&["GET", "k-0042"]).await;
    assert!(reply.contains("v-0042"), "{reply}");
    server.kill();
    let mut server = Server::start(&root);
    server.wait_healthy().await;
    for index in [0, 42, 123, 499] {
        let key = format!("k-{index:04}");
        let reply = server.resp(&["GET", &key]).await;
        assert!(reply.contains(&format!("v-{index:04}")), "{reply}");
    }
    let reply = server.resp(&["SET", "post", "crash"]).await;
    assert!(reply.starts_with("+OK"), "{reply}");
    server.kill();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn sigkill_mid_load_then_healthy() {
    let root =
        std::env::temp_dir().join(format!("ryme-crash-load-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut server = Server::start(&root);
    server.wait_healthy().await;
    let reply = server.resp(&["SET", "anchor", "here"]).await;
    assert!(reply.starts_with("+OK"), "{reply}");
    let addr = server.resp;
    let writers: Vec<_> = (0..4)
        .map(|worker| {
            tokio::spawn(async move {
                for index in 0..500 {
                    let key = format!("w{worker}-{index:04}");
                    let frame = format!(
                        "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$8\r\nvalue-{index:04}\r\n",
                        key.len()
                    );
                    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
                    if socket.write_all(frame.as_bytes()).await.is_err() {
                        return;
                    }
                    let mut chunk = vec![0u8; 64];
                    let _ = socket.read(&mut chunk).await;
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(400)).await;
    server.kill();
    for writer in writers {
        let _ = writer.await;
    }
    let mut server = Server::start(&root);
    server.wait_healthy().await;
    let reply = server.resp(&["GET", "anchor"]).await;
    assert!(reply.contains("here"), "{reply}");
    let reply = server.resp(&["SET", "after", "restart"]).await;
    assert!(reply.starts_with("+OK"), "{reply}");
    let reply = server.resp(&["GET", "after"]).await;
    assert!(reply.contains("restart"), "{reply}");
    server.kill();
    let _ = std::fs::remove_dir_all(&root);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
