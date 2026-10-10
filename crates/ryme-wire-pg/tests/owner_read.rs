use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

async fn read_frame(socket: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let tag =
        tokio::time::timeout(Duration::from_secs(5), socket.read_u8()).await.unwrap().unwrap();
    let length = socket.read_u32().await.unwrap() as usize;
    let mut body = vec![0u8; length - 4];
    socket.read_exact(&mut body).await.unwrap();
    (tag, body)
}

async fn startup(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut startup = Vec::new();
    startup.extend_from_slice(&16u32.to_be_bytes());
    startup.extend_from_slice(&196608u32.to_be_bytes());
    startup.extend_from_slice(b"user\0u\0\0");
    socket.write_all(&startup).await.unwrap();
    loop {
        let (tag, _) = read_frame(&mut socket).await;
        if tag == b'Z' {
            return socket;
        }
    }
}

#[tokio::test]
async fn owner_reader_serves_autocommit_primary_key_selects() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"))
        .with_remote_reader(ryme_wire_pg::RemoteReader::new(|key| async move {
            assert_eq!(key.table, "docs");
            assert_eq!(key.pk, b"remote".to_vec());
            Ok(ryme_wire_pg::RemoteRead::Value {
                value: Some(b"owner-value".to_vec()),
                expires_at: None,
            })
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let mut socket = startup(addr).await;
    socket.write_all(&frame(b'Q', b"SELECT * FROM docs KEY 'remote'\0")).await.unwrap();
    let mut found = false;
    loop {
        let (tag, body) = read_frame(&mut socket).await;
        if tag == b'D' && String::from_utf8_lossy(&body).contains("owner-value") {
            found = true;
        }
        if tag == b'Z' {
            break;
        }
    }
    assert!(found);
}
