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

#[tokio::test]
async fn simple_protocol_transactions_commit_rollback_and_abort() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });

    let mut socket = startup(addr).await;
    let committed = simple(
        &mut socket,
        "BEGIN; INSERT INTO docs (id, value) VALUES ('tx', 'inside'); SELECT * FROM docs KEY 'tx'; COMMIT;",
    )
    .await;
    assert!(committed.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("inside")));
    assert!(committed
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("COMMIT") }));

    let rolled_back = simple(
        &mut socket,
        "BEGIN; INSERT INTO docs (id, value) VALUES ('rolled', 'gone'); ROLLBACK; SELECT * FROM docs KEY 'rolled';",
    )
    .await;
    assert!(rolled_back
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("ROLLBACK") }));
    assert!(!rolled_back.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("gone")));

    let _ = simple(&mut socket, "INSERT INTO docs KEY 'duplicate' VALUE 'old'").await;
    let error = simple(&mut socket, "BEGIN; INSERT INTO docs KEY 'duplicate' VALUE 'new'").await;
    assert!(error
        .iter()
        .any(|(tag, body)| { *tag == b'E' && String::from_utf8_lossy(body).contains("40001") }));
    let aborted = simple(&mut socket, "SELECT * FROM docs KEY 'tx'").await;
    assert!(aborted
        .iter()
        .any(|(tag, body)| { *tag == b'E' && String::from_utf8_lossy(body).contains("25P02") }));
    let rolled_back = simple(&mut socket, "ROLLBACK").await;
    assert!(rolled_back
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("ROLLBACK") }));
}
