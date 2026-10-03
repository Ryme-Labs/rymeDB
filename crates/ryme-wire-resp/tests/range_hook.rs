use ryme_router::RangeLoadHook;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Seen = Arc<Mutex<Vec<(Vec<u8>, u64)>>>;

async fn command(addr: std::net::SocketAddr, parts: &[&str]) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
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
    String::from_utf8(raw).unwrap().trim().to_string()
}

async fn serve(hook: RangeLoadHook) -> std::net::SocketAddr {
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"))
        .with_range_hook(hook);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    addr
}

fn recorder() -> (RangeLoadHook, Seen) {
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let moved = seen.clone();
    let hook = RangeLoadHook::armed(Arc::new(move |key: &[u8], count: u64| {
        moved.lock().unwrap().push((key.to_vec(), count));
    }));
    (hook, seen)
}

fn routing(table: &str, pk: &str) -> Vec<u8> {
    let mut out = table.as_bytes().to_vec();
    out.push(0);
    out.extend_from_slice(pk.as_bytes());
    out
}

#[tokio::test]
async fn range_hook_records_committed_resp_writes() {
    let (hook, seen) = recorder();
    let addr = serve(hook).await;
    assert_eq!(command(addr, &["SET", "k", "v"]).await, "+OK");
    assert_eq!(command(addr, &["GET", "k"]).await, "$1\r\nv");
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0], (routing("_kv", "k"), 1));
    }
    assert_eq!(command(addr, &["DEL", "k"]).await, ":1");
    assert_eq!(command(addr, &["DEL", "missing"]).await, ":0");
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[1], (routing("_kv", "k"), 1));
    }
}

#[tokio::test]
async fn range_hook_records_exec_batch_once() {
    let (hook, seen) = recorder();
    let addr = serve(hook).await;
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    for parts in [
        ["MULTI"].as_slice(),
        ["SET", "a", "1"].as_slice(),
        ["SET", "b", "2"].as_slice(),
        ["EXEC"].as_slice(),
    ] {
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
    }
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(seen.contains(&(routing("_kv", "a"), 1)));
    assert!(seen.contains(&(routing("_kv", "b"), 1)));
}
