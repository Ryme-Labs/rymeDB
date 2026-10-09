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

async fn read_until_ready(socket: &mut tokio::net::TcpStream) -> Vec<(u8, Vec<u8>)> {
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

async fn simple(socket: &mut tokio::net::TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    socket.write_all(&frame(b'Q', format!("{sql}\0").as_bytes())).await.unwrap();
    read_until_ready(socket).await
}

#[tokio::test]
async fn copy_text_protocol_ingests_rows_and_completes() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });

    let mut socket = startup(addr).await;
    socket.write_all(&frame(b'Q', b"COPY docs FROM STDIN\0")).await.unwrap();
    let (tag, body) = read_frame(&mut socket).await;
    assert_eq!(tag, b'G');
    assert_eq!(body, [0, 0, 2, 0, 0, 0, 0]);

    socket.write_all(&frame(b'd', b"k1\tv1\nk2\tv\\t2\n")).await.unwrap();
    socket.write_all(&frame(b'c', &[])).await.unwrap();
    let frames = read_until_ready(&mut socket).await;
    assert!(frames
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("COPY 2") }));

    socket.write_all(&frame(b'Q', b"SELECT * FROM docs KEY 'k2'\0")).await.unwrap();
    let frames = read_until_ready(&mut socket).await;
    assert!(frames
        .iter()
        .any(|(tag, body)| { *tag == b'D' && String::from_utf8_lossy(body).contains("v\t2") }));
}

#[tokio::test]
async fn copy_text_protocol_ingests_declared_sql_columns_and_nulls() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });

    let mut socket = startup(addr).await;
    let created = simple(
        &mut socket,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, body TEXT, active BOOLEAN DEFAULT true)",
    )
    .await;
    assert!(!created.iter().any(|(tag, _)| *tag == b'E'));

    socket.write_all(&frame(b'Q', b"COPY events (id, body) FROM STDIN\0")).await.unwrap();
    let (tag, body) = read_frame(&mut socket).await;
    assert_eq!(tag, b'G');
    assert_eq!(body, [0, 0, 2, 0, 0, 0, 0]);

    socket.write_all(&frame(b'd', b"1\thello\n2\t\\N\n")).await.unwrap();
    socket.write_all(&frame(b'c', &[])).await.unwrap();
    let frames = read_until_ready(&mut socket).await;
    assert!(frames
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("COPY 2") }));

    let rows = simple(&mut socket, "SELECT id, body, active FROM events").await;
    assert!(rows
        .iter()
        .any(|(tag, body)| { *tag == b'D' && String::from_utf8_lossy(body).contains("hello") }));
    let defaulted = simple(&mut socket, "SELECT active FROM events WHERE id = 1").await;
    assert!(defaulted
        .iter()
        .any(|(tag, body)| { *tag == b'D' && String::from_utf8_lossy(body).contains("true") }));
    let null_body = simple(&mut socket, "SELECT body FROM events WHERE id = 2").await;
    assert!(null_body.iter().any(|(tag, body)| {
        *tag == b'D' && body.windows(4).any(|part| part == (-1i32).to_be_bytes())
    }));
}

#[tokio::test]
async fn copy_inside_transaction_stages_until_commit_and_rolls_back() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });

    let mut socket = startup(addr).await;
    let _ = simple(&mut socket, "BEGIN").await;
    socket.write_all(&frame(b'Q', b"COPY tx_docs FROM STDIN\0")).await.unwrap();
    let (tag, _) = read_frame(&mut socket).await;
    assert_eq!(tag, b'G');
    socket.write_all(&frame(b'd', b"k1\tv1\nk2\tv2\n")).await.unwrap();
    socket.write_all(&frame(b'c', &[])).await.unwrap();
    let copied = read_until_ready(&mut socket).await;
    assert!(copied
        .iter()
        .any(|(tag, body)| { *tag == b'C' && String::from_utf8_lossy(body).contains("COPY 2") }));

    let staged = simple(&mut socket, "SELECT * FROM tx_docs KEY 'k1'").await;
    assert!(staged
        .iter()
        .any(|(tag, body)| { *tag == b'D' && String::from_utf8_lossy(body).contains("v1") }));
    let _ = simple(&mut socket, "ROLLBACK").await;
    let rolled_back = simple(&mut socket, "SELECT * FROM tx_docs KEY 'k1'").await;
    assert!(!rolled_back.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("v1")));

    let _ = simple(&mut socket, "BEGIN").await;
    socket.write_all(&frame(b'Q', b"COPY tx_docs FROM STDIN\0")).await.unwrap();
    let (tag, _) = read_frame(&mut socket).await;
    assert_eq!(tag, b'G');
    socket.write_all(&frame(b'd', b"k1\tcommitted\n")).await.unwrap();
    socket.write_all(&frame(b'c', &[])).await.unwrap();
    let _ = read_until_ready(&mut socket).await;
    let _ = simple(&mut socket, "COMMIT").await;
    let committed = simple(&mut socket, "SELECT * FROM tx_docs KEY 'k1'").await;
    assert!(committed.iter().any(|(tag, body)| {
        *tag == b'D' && String::from_utf8_lossy(body).contains("committed")
    }));
}
