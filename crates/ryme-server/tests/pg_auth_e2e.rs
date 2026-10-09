use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn startup_packet(user: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&196608u32.to_be_bytes());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.extend_from_slice(b"\0database\0postgres\0\0");
    let mut packet = Vec::with_capacity(body.len() + 4);
    packet.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    packet.extend_from_slice(&body);
    packet
}

fn password_packet(password: &str) -> Vec<u8> {
    let payload = format!("{password}\0");
    let mut packet = Vec::with_capacity(payload.len() + 5);
    packet.push(b'p');
    packet.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    packet.extend_from_slice(payload.as_bytes());
    packet
}

fn query_packet(query: &str) -> Vec<u8> {
    let payload = format!("{query}\0");
    let mut packet = Vec::with_capacity(payload.len() + 5);
    packet.push(b'Q');
    packet.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    packet.extend_from_slice(payload.as_bytes());
    packet
}

async fn read_frame(socket: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let tag = socket.read_u8().await.unwrap();
    let length = socket.read_u32().await.unwrap() as usize;
    let mut payload = vec![0u8; length - 4];
    socket.read_exact(&mut payload).await.unwrap();
    (tag, payload)
}

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

#[tokio::test]
async fn server_pg_password_authentication_gate() {
    const PASSWORD: &str = "pg-auth-e2e-secret";
    std::env::set_var("RYME_PG_PASSWORD", PASSWORD);
    let root = std::env::temp_dir().join(format!(
        "ryme-pg-auth-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let http_listener = bind_listener().await;
    let mut config = ryme_config::Config::default();
    config.node_id = String::from("pg-auth");
    config.data_dir = root.clone();
    config.archive.interval_secs = 0;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut rejected = tokio::net::TcpStream::connect(pg).await.unwrap();
    rejected.write_all(&startup_packet("alice")).await.unwrap();
    let (tag, body) = read_frame(&mut rejected).await;
    assert_eq!(tag, b'R');
    assert_eq!(u32::from_be_bytes(body[..4].try_into().unwrap()), 3);
    rejected.write_all(&password_packet("wrong")).await.unwrap();
    let (tag, body) = read_frame(&mut rejected).await;
    assert_eq!(tag, b'E');
    assert!(String::from_utf8_lossy(&body).contains("28P01"));

    let mut socket = tokio::net::TcpStream::connect(pg).await.unwrap();
    socket.write_all(&startup_packet("alice")).await.unwrap();
    let (tag, body) = read_frame(&mut socket).await;
    assert_eq!(tag, b'R');
    assert_eq!(u32::from_be_bytes(body[..4].try_into().unwrap()), 3);
    socket.write_all(&password_packet(PASSWORD)).await.unwrap();
    let mut auth_ok = false;
    loop {
        let (tag, body) = read_frame(&mut socket).await;
        if tag == b'R' {
            assert_eq!(u32::from_be_bytes(body[..4].try_into().unwrap()), 0);
            auth_ok = true;
        }
        if tag == b'Z' {
            break;
        }
    }
    assert!(auth_ok);
    socket.write_all(&query_packet("SELECT 1")).await.unwrap();
    let mut saw_error = false;
    loop {
        let (tag, _) = read_frame(&mut socket).await;
        if tag == b'E' {
            saw_error = true;
        }
        if tag == b'Z' {
            break;
        }
    }
    assert!(!saw_error);

    server.abort();
    std::env::remove_var("RYME_PG_PASSWORD");
    let _ = std::fs::remove_dir_all(&root);
}
