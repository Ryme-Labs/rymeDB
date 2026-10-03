use ryme_router::RangeLoadHook;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_frame(socket: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let mut tag = [0u8; 1];
    socket.read_exact(&mut tag).await.unwrap();
    let mut len = [0u8; 4];
    socket.read_exact(&mut len).await.unwrap();
    let len = u32::from_be_bytes(len) as usize;
    let mut body = vec![0u8; len - 4];
    socket.read_exact(&mut body).await.unwrap();
    (tag[0], body)
}

fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

async fn startup(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut hello = Vec::new();
    hello.extend_from_slice(&16u32.to_be_bytes());
    hello.extend_from_slice(&196608u32.to_be_bytes());
    hello.extend_from_slice(b"user\0u\0\0");
    socket.write_all(&hello).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (tag, _) = read_frame(&mut socket).await;
            if tag == b'Z' {
                break;
            }
        }
    })
    .await
    .unwrap();
    socket
}

async fn simple(socket: &mut tokio::net::TcpStream, sql: &str) -> Vec<u8> {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    socket.write_all(&frame(b'Q', &payload)).await.unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (tag, body) = read_frame(socket).await;
            out.extend_from_slice(&body);
            if tag == b'Z' {
                break;
            }
        }
    })
    .await
    .unwrap();
    out
}

#[tokio::test]
async fn range_hook_records_pg_writes_by_table() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let moved = seen.clone();
    let hook = RangeLoadHook::armed(Arc::new(move |key: &[u8], count: u64| {
        moved.lock().unwrap().push((key.to_vec(), count));
    }));
    let gateway =
        ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d")).with_range_hook(hook);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let mut socket = startup(addr).await;
    let inserted = simple(&mut socket, "INSERT INTO docs KEY 'k1' VALUE 'v1'").await;
    assert!(String::from_utf8_lossy(&inserted).contains("OK"), "{inserted:?}");
    let _ = simple(&mut socket, "SELECT * FROM docs KEY 'k1'").await;
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0], (b"docs".to_vec(), 1));
}
