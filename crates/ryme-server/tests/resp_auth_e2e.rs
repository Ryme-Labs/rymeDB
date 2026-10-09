use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn command(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len());
    for part in parts {
        out.push_str(&format!("${}\r\n{part}\r\n", part.len()));
    }
    out.into_bytes()
}

async fn read_response(socket: &mut tokio::net::TcpStream) -> String {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 256];
    loop {
        let read = socket.read(&mut buffer).await.unwrap();
        assert!(read > 0, "RESP connection closed before a response");
        raw.extend_from_slice(&buffer[..read]);
        if raw.ends_with(b"\r\n") {
            return String::from_utf8(raw).unwrap();
        }
    }
}

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

#[tokio::test]
async fn server_resp_authentication_gate() {
    const PASSWORD: &str = "resp-auth-e2e-secret";
    std::env::set_var("RYME_RESP_PASSWORD", PASSWORD);
    let root = std::env::temp_dir().join(format!(
        "ryme-resp-auth-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let mut config = ryme_config::Config::default();
    config.node_id = String::from("resp-auth");
    config.data_dir = root.clone();
    config.archive.interval_secs = 0;
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut socket = tokio::net::TcpStream::connect(resp).await.unwrap();
    socket.write_all(&command(&["GET", "key"])).await.unwrap();
    assert!(read_response(&mut socket).await.contains("NOAUTH"));
    socket.write_all(&command(&["AUTH", PASSWORD])).await.unwrap();
    assert_eq!(read_response(&mut socket).await, "+OK\r\n");
    socket.write_all(&command(&["SET", "key", "value"])).await.unwrap();
    assert_eq!(read_response(&mut socket).await, "+OK\r\n");
    socket.write_all(&command(&["GET", "key"])).await.unwrap();
    assert!(read_response(&mut socket).await.contains("value"));

    server.abort();
    std::env::remove_var("RYME_RESP_PASSWORD");
    let _ = std::fs::remove_dir_all(&root);
}
