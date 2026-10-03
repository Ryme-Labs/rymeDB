use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

#[tokio::test]
async fn cdc_emits_crud_ops() {
    let realtime = ryme_realtime::Realtime::new(64);
    let mut rx = realtime.subscribe("t", "d", "_kv");
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"))
        .with_realtime(realtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    assert_eq!(command(addr, &["SET", "a", "1"]).await, "+OK");
    assert_eq!(command(addr, &["SET", "a", "2"]).await, "+OK");
    assert_eq!(command(addr, &["MSET", "b", "3", "c", "4"]).await, "+OK");
    assert_eq!(command(addr, &["INCR", "n"]).await, ":1");
    assert_eq!(command(addr, &["DEL", "a"]).await, ":1");
    let mut events = Vec::new();
    for _ in 0..6 {
        let record =
            tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        events.push((record.op, record.pk.clone(), record.after.clone()));
    }
    assert_eq!(
        events,
        vec![
            (ryme_realtime::Operation::Insert, b"a".to_vec(), Some(b"1".to_vec())),
            (ryme_realtime::Operation::Update, b"a".to_vec(), Some(b"2".to_vec())),
            (ryme_realtime::Operation::Insert, b"b".to_vec(), Some(b"3".to_vec())),
            (ryme_realtime::Operation::Insert, b"c".to_vec(), Some(b"4".to_vec())),
            (ryme_realtime::Operation::Insert, b"n".to_vec(), Some(b"1".to_vec())),
            (ryme_realtime::Operation::Delete, b"a".to_vec(), None),
        ]
    );
}

async fn pipeline(socket: &mut tokio::net::TcpStream, parts: &[&str]) {
    let mut frame = format!("*{}\r\n", parts.len());
    for part in parts {
        frame.push_str(&format!("${}\r\n{part}\r\n", part.len()));
    }
    socket.write_all(frame.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 4096];
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
}

#[tokio::test]
async fn cdc_emits_exec_batch() {
    let realtime = ryme_realtime::Realtime::new(64);
    let mut rx = realtime.subscribe("t", "d", "_kv");
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"))
        .with_realtime(realtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    pipeline(&mut socket, &["MULTI"]).await;
    pipeline(&mut socket, &["SET", "x", "1"]).await;
    pipeline(&mut socket, &["SET", "y", "2"]).await;
    pipeline(&mut socket, &["EXEC"]).await;
    for want in [b"x".to_vec(), b"y".to_vec()] {
        let record =
            tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        assert_eq!(record.op, ryme_realtime::Operation::Insert);
        assert_eq!(record.pk, want);
    }
}
