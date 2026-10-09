use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_frame(socket: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let tag =
        tokio::time::timeout(Duration::from_secs(5), socket.read_u8()).await.unwrap().unwrap();
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

async fn simple(socket: &mut tokio::net::TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    socket.write_all(&frame(b'Q', format!("{sql}\0").as_bytes())).await.unwrap();
    let mut frames = Vec::new();
    loop {
        let frame = read_frame(socket).await;
        let ready = frame.0 == b'Z';
        frames.push(frame);
        if ready {
            return frames;
        }
    }
}

fn notification_body(body: &[u8]) -> (u32, String, String) {
    let pid = u32::from_be_bytes(body[..4].try_into().unwrap());
    let channel_end = body[4..].iter().position(|byte| *byte == 0).unwrap() + 4;
    let payload_start = channel_end + 1;
    let payload_end =
        body[payload_start..].iter().position(|byte| *byte == 0).unwrap() + payload_start;
    (
        pid,
        String::from_utf8(body[4..channel_end].to_vec()).unwrap(),
        String::from_utf8(body[payload_start..payload_end].to_vec()).unwrap(),
    )
}

#[tokio::test]
async fn listen_notify_delivers_async_notification_response() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("tenant"), String::from("database"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });

    let mut listener_socket = startup(addr).await;
    let mut sender_socket = startup(addr).await;

    let listen_rollback = simple(&mut listener_socket, "BEGIN; LISTEN Chat; ROLLBACK").await;
    assert!(listen_rollback
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("ROLLBACK") }));

    let before_commit = simple(&mut sender_socket, "NOTIFY chat, 'before commit'").await;
    assert!(before_commit
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("NOTIFY") }));

    let rolled_back =
        simple(&mut sender_socket, "BEGIN; NOTIFY chat, 'rolled back'; ROLLBACK").await;
    assert!(rolled_back
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("ROLLBACK") }));
    assert!(tokio::time::timeout(Duration::from_millis(100), read_frame(&mut listener_socket))
        .await
        .is_err());

    let listen_frames = simple(&mut listener_socket, "BEGIN; LISTEN Chat; COMMIT").await;
    assert!(listen_frames
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("LISTEN") }));

    let notify_frames = simple(
        &mut sender_socket,
        "BEGIN; NOTIFY chat, 'hello, world'; SAVEPOINT keep; NOTIFY chat, 'discarded'; ROLLBACK TO SAVEPOINT keep; COMMIT",
    )
    .await;
    assert!(notify_frames
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("NOTIFY") }));

    let (tag, body) = read_frame(&mut listener_socket).await;
    assert_eq!(tag, b'A');
    let (pid, channel, payload) = notification_body(&body);
    assert!(pid > 0);
    assert_eq!(channel, "chat");
    assert_eq!(payload, "hello, world");
    assert!(tokio::time::timeout(Duration::from_millis(100), read_frame(&mut listener_socket))
        .await
        .is_err());

    let unlisten_rollback = simple(&mut listener_socket, "BEGIN; UNLISTEN chat; ROLLBACK").await;
    assert!(unlisten_rollback
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("ROLLBACK") }));
    let still_listening = simple(&mut sender_socket, "NOTIFY chat, 'still listening'").await;
    assert!(still_listening
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("NOTIFY") }));
    let (tag, body) = read_frame(&mut listener_socket).await;
    assert_eq!(tag, b'A');
    let (_, channel, payload) = notification_body(&body);
    assert_eq!(channel, "chat");
    assert_eq!(payload, "still listening");

    let unlisten_frames = simple(&mut listener_socket, "BEGIN; UNLISTEN chat; COMMIT").await;
    assert!(unlisten_frames
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("UNLISTEN") }));
    let after_unlisten = simple(&mut sender_socket, "NOTIFY chat, 'not delivered'").await;
    assert!(after_unlisten
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("NOTIFY") }));
    assert!(tokio::time::timeout(Duration::from_millis(100), read_frame(&mut listener_socket))
        .await
        .is_err());
}
