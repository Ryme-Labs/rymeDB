use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn excess_connections_rejected() {
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve_limited(listener, 2).await;
    });
    let mut first = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut second = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut third = tokio::net::TcpStream::connect(addr).await.unwrap();
    for socket in [&mut first, &mut second] {
        socket.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut reply = vec![0u8; 7];
        tokio::time::timeout(Duration::from_secs(5), socket.read_exact(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&reply, b"+PONG\r\n");
    }
    let mut reply = vec![0u8; 1024];
    let read = tokio::time::timeout(Duration::from_secs(5), third.read(&mut reply))
        .await
        .unwrap()
        .unwrap();
    let text = String::from_utf8_lossy(&reply[..read]).into_owned();
    assert!(text.starts_with("-ERR overloaded") || read == 0, "unexpected: {text:?}");
    drop(first);
    drop(second);
    drop(third);
}
