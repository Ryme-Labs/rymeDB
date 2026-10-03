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

fn parse_msg(name: &str, query: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(name.as_bytes());
    payload.push(0);
    payload.extend_from_slice(query.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&0i16.to_be_bytes());
    frame(b'P', &payload)
}

fn bind_msg(portal: &str, statement: &str, params: &[&str]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(portal.as_bytes());
    payload.push(0);
    payload.extend_from_slice(statement.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&0i16.to_be_bytes());
    payload.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for param in params {
        payload.extend_from_slice(&(param.len() as i32).to_be_bytes());
        payload.extend_from_slice(param.as_bytes());
    }
    frame(b'B', &payload)
}

fn execute_msg(portal: &str) -> Vec<u8> {
    let mut payload = portal.as_bytes().to_vec();
    payload.push(0);
    frame(b'E', &payload)
}

fn sync_msg() -> Vec<u8> {
    frame(b'S', &[])
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

async fn roundtrip(socket: &mut tokio::net::TcpStream, message: Vec<u8>) -> Vec<(u8, Vec<u8>)> {
    socket.write_all(&message).await.unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (tag, body) = read_frame(socket).await;
            out.push((tag, body));
            if tag == b'1' || tag == b'2' || tag == b'Z' {
                break;
            }
        }
    })
    .await
    .unwrap();
    out
}

async fn execute(socket: &mut tokio::net::TcpStream, portal: &str) -> Vec<(u8, Vec<u8>)> {
    socket.write_all(&execute_msg(portal)).await.unwrap();
    socket.write_all(&sync_msg()).await.unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (tag, body) = read_frame(socket).await;
            out.push((tag, body));
            if tag == b'Z' {
                break;
            }
        }
    })
    .await
    .unwrap();
    out
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
async fn prepared_portal_reexecutes_identically() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let mut socket = startup(addr).await;
    let inserted = simple(&mut socket, "INSERT INTO docs KEY 'k1' VALUE 'v1'").await;
    assert!(String::from_utf8_lossy(&inserted).contains("OK"), "{inserted:?}");
    let replied = roundtrip(&mut socket, parse_msg("s1", "SELECT * FROM docs KEY 'k1'")).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'1'));
    let replied = roundtrip(&mut socket, bind_msg("p1", "s1", &[])).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'2'));
    let first = execute(&mut socket, "p1").await;
    assert!(!first.is_empty());
    let second = execute(&mut socket, "p1").await;
    assert_eq!(first, second);
    assert!(first.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("v1")));
}

#[tokio::test]
async fn prepared_portal_errors_are_stable() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let mut socket = startup(addr).await;
    let replied = execute(&mut socket, "missing").await;
    assert!(replied.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("portal")));
    let replied = roundtrip(&mut socket, parse_msg("bad", "FROBNICATE docs")).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'1'));
    let replied = roundtrip(&mut socket, bind_msg("pbad", "bad", &[])).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'2'));
    let first = execute(&mut socket, "pbad").await;
    let second = execute(&mut socket, "pbad").await;
    assert_eq!(first, second);
    assert!(first.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("42601")));
}

fn close_msg(kind: u8, name: &str) -> Vec<u8> {
    let mut payload = vec![kind];
    payload.extend_from_slice(name.as_bytes());
    payload.push(0);
    frame(b'C', &payload)
}

async fn close(socket: &mut tokio::net::TcpStream, kind: u8, name: &str) -> Vec<(u8, Vec<u8>)> {
    socket.write_all(&close_msg(kind, name)).await.unwrap();
    socket.write_all(&sync_msg()).await.unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (tag, body) = read_frame(socket).await;
            out.push((tag, body));
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
async fn prepared_portal_invalidation() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let mut socket = startup(addr).await;
    let _ = simple(&mut socket, "INSERT INTO docs KEY 'k1' VALUE 'v1'").await;
    let _ = simple(&mut socket, "INSERT INTO docs KEY 'k2' VALUE 'v2'").await;
    let replied = roundtrip(&mut socket, parse_msg("s1", "SELECT * FROM docs KEY 'k1'")).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'1'));
    let replied = roundtrip(&mut socket, bind_msg("p1", "s1", &[])).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'2'));
    let first = execute(&mut socket, "p1").await;
    assert!(first.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("v1")));
    let replied = roundtrip(&mut socket, bind_msg("p1", "s1", &[])).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'2'));
    let rebound = execute(&mut socket, "p1").await;
    assert_eq!(first, rebound);
    let closed = close(&mut socket, b' ', "p1").await;
    assert!(closed.iter().any(|(tag, _)| *tag == b'3'));
    let gone = execute(&mut socket, "p1").await;
    assert!(gone.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("portal")));
    let replied = roundtrip(&mut socket, bind_msg("p1", "s1", &[])).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'2'));
    let revived = execute(&mut socket, "p1").await;
    assert_eq!(first, revived);
    let closed = close(&mut socket, b'S', "s1").await;
    assert!(closed.iter().any(|(tag, _)| *tag == b'3'));
    let gone = execute(&mut socket, "p1").await;
    assert!(gone.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("statement")));
    let replied = roundtrip(&mut socket, parse_msg("s1", "SELECT * FROM docs KEY 'k2'")).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'1'));
    let replied = roundtrip(&mut socket, bind_msg("p1", "s1", &[])).await;
    assert_eq!(replied.first().map(|(tag, _)| *tag), Some(b'2'));
    let moved = execute(&mut socket, "p1").await;
    assert!(moved.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("v2")));
    assert!(!moved.iter().any(|(_, body)| String::from_utf8_lossy(body).contains("v1")));
}

#[tokio::test]
async fn prepared_sql_execute_caches() {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let mut socket = startup(addr).await;
    let _ = simple(&mut socket, "INSERT INTO docs KEY 'k1' VALUE 'v1'").await;
    let _ = simple(&mut socket, "INSERT INTO docs KEY 'k2' VALUE 'v2'").await;
    let prepared = simple(&mut socket, "PREPARE foo AS SELECT * FROM docs KEY $1").await;
    assert!(String::from_utf8_lossy(&prepared).contains("PREPARE"), "{prepared:?}");
    let first = simple(&mut socket, "EXECUTE foo ('k1')").await;
    assert!(String::from_utf8_lossy(&first).contains("v1"), "{first:?}");
    let second = simple(&mut socket, "EXECUTE foo ('k1')").await;
    assert_eq!(first, second);
    let moved = simple(&mut socket, "EXECUTE foo ('k2')").await;
    assert!(String::from_utf8_lossy(&moved).contains("v2"), "{moved:?}");
    assert!(!String::from_utf8_lossy(&moved).contains("v1"));
    let _ = simple(&mut socket, "PREPARE foo AS SELECT * FROM docs KEY 'k2'").await;
    let fixed = simple(&mut socket, "EXECUTE foo").await;
    assert!(String::from_utf8_lossy(&fixed).contains("v2"), "{fixed:?}");
    let _ = simple(&mut socket, "DEALLOCATE foo").await;
    let gone = simple(&mut socket, "EXECUTE foo").await;
    assert!(String::from_utf8_lossy(&gone).contains("26000"), "{gone:?}");
}
