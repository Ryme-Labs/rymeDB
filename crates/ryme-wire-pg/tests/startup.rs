use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_frame(socket: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let tag = socket.read_u8().await.unwrap();
    let length = socket.read_u32().await.unwrap() as usize;
    let mut body = vec![0u8; length - 4];
    socket.read_exact(&mut body).await.unwrap();
    (tag, body)
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

#[tokio::test]
async fn cancel_request_is_consumed_without_a_startup_greeting() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });

    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut packet = Vec::with_capacity(16);
    packet.extend_from_slice(&16u32.to_be_bytes());
    packet.extend_from_slice(&80877102u32.to_be_bytes());
    packet.extend_from_slice(&123u32.to_be_bytes());
    packet.extend_from_slice(&456u32.to_be_bytes());
    socket.write_all(&packet).await.unwrap();

    let mut response = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(1), socket.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read, 0, "CancelRequest must not receive a startup response");
}

#[tokio::test]
async fn cancel_request_signals_advertised_backend() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });

    let mut session = tokio::net::TcpStream::connect(addr).await.unwrap();
    session.write_all(&16u32.to_be_bytes()).await.unwrap();
    session.write_all(&196608u32.to_be_bytes()).await.unwrap();
    session.write_all(b"user\0u\0\0").await.unwrap();
    let (pid, secret) = loop {
        let (tag, body) = read_frame(&mut session).await;
        if tag == b'K' {
            assert_eq!(body.len(), 8);
            break (
                u32::from_be_bytes(body[0..4].try_into().unwrap()),
                u32::from_be_bytes(body[4..8].try_into().unwrap()),
            );
        }
        assert_ne!(tag, b'Z', "backend key data must precede ReadyForQuery");
    };
    while read_frame(&mut session).await.0 != b'Z' {}

    let mut cancel = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut packet = Vec::with_capacity(16);
    packet.extend_from_slice(&16u32.to_be_bytes());
    packet.extend_from_slice(&80877102u32.to_be_bytes());
    packet.extend_from_slice(&pid.to_be_bytes());
    packet.extend_from_slice(&secret.to_be_bytes());
    cancel.write_all(&packet).await.unwrap();
    cancel.shutdown().await.unwrap();
    let mut eof = [0u8; 1];
    assert_eq!(cancel.read(&mut eof).await.unwrap(), 0);

    session.write_all(&frame(b'Q', b"SELECT 1\0")).await.unwrap();
    let mut saw_cancel = false;
    loop {
        let (tag, body) = read_frame(&mut session).await;
        if tag == b'E' && String::from_utf8_lossy(&body).contains("57014") {
            saw_cancel = true;
        }
        if tag == b'Z' {
            break;
        }
    }
    assert!(saw_cancel);
}
