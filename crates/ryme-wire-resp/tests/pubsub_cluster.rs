use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn start_gateway(
    gateway: ryme_wire_resp::RespGateway,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    (address, task)
}

#[tokio::test]
async fn pubsub_reaches_a_subscriber_on_another_gateway() {
    let realtime = ryme_realtime::Realtime::new(64);
    let (subscriber_addr, subscriber_task) = start_gateway(
        ryme_wire_resp::RespGateway::new(String::from("tenant"), String::from("database"))
            .with_realtime(realtime.clone()),
    )
    .await;
    let (publisher_addr, publisher_task) = start_gateway(
        ryme_wire_resp::RespGateway::new(String::from("tenant"), String::from("database"))
            .with_realtime(realtime),
    )
    .await;

    let mut subscriber = tokio::net::TcpStream::connect(subscriber_addr).await.unwrap();
    subscriber.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nroom\r\n").await.unwrap();
    let mut subscription_ack = [0u8; 33];
    subscriber.read_exact(&mut subscription_ack).await.unwrap();
    assert_eq!(&subscription_ack, b"*3\r\n$9\r\nsubscribe\r\n$4\r\nroom\r\n:1\r\n");

    let mut publisher = tokio::net::TcpStream::connect(publisher_addr).await.unwrap();
    publisher.write_all(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nroom\r\n$2\r\nhi\r\n").await.unwrap();
    let mut publish_reply = [0u8; 4];
    publisher.read_exact(&mut publish_reply).await.unwrap();
    assert_eq!(&publish_reply, b":0\r\n");

    let mut message = [0u8; 35];
    tokio::time::timeout(Duration::from_secs(2), subscriber.read_exact(&mut message))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&message, b"*3\r\n$7\r\nmessage\r\n$4\r\nroom\r\n$2\r\nhi\r\n");

    subscriber_task.abort();
    publisher_task.abort();
}
