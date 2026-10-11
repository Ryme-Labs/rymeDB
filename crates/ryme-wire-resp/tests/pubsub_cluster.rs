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

#[tokio::test]
async fn pubsub_isolated_between_authenticated_tenants() {
    let (address, task) = start_gateway(
        ryme_wire_resp::RespGateway::new(String::from("default"), String::from("database"))
            .with_realtime(ryme_realtime::Realtime::new(64))
            .with_authenticator(|_, password| {
                let tenant = match password {
                    "alpha-secret" => "alpha",
                    "beta-secret" => "beta",
                    _ => return Err(ryme_error::RymeError::Unauthorized),
                };
                Ok(ryme_auth::Principal {
                    id: String::from("user"),
                    tenant: String::from(tenant),
                    roles: [ryme_auth::Role::Owner].into_iter().collect(),
                })
            }),
    )
    .await;

    let mut alpha_subscriber = tokio::net::TcpStream::connect(address).await.unwrap();
    alpha_subscriber
        .write_all(
            b"*2\r\n$4\r\nAUTH\r\n$12\r\nalpha-secret\r\n*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nroom\r\n",
        )
        .await
        .unwrap();
    let mut auth_reply = [0u8; 5];
    alpha_subscriber.read_exact(&mut auth_reply).await.unwrap();
    assert_eq!(&auth_reply, b"+OK\r\n");
    let mut subscription_reply = [0u8; 33];
    alpha_subscriber.read_exact(&mut subscription_reply).await.unwrap();
    assert_eq!(&subscription_reply, b"*3\r\n$9\r\nsubscribe\r\n$4\r\nroom\r\n:1\r\n");

    let mut beta_publisher = tokio::net::TcpStream::connect(address).await.unwrap();
    beta_publisher.write_all(
        b"*2\r\n$4\r\nAUTH\r\n$11\r\nbeta-secret\r\n*3\r\n$7\r\nPUBLISH\r\n$4\r\nroom\r\n$2\r\nno\r\n",
    ).await.unwrap();
    let mut beta_reply = [0u8; 5];
    beta_publisher.read_exact(&mut beta_reply).await.unwrap();
    assert_eq!(&beta_reply, b"+OK\r\n");
    let mut publish_reply = [0u8; 4];
    beta_publisher.read_exact(&mut publish_reply).await.unwrap();
    assert_eq!(&publish_reply, b":0\r\n");
    let mut cross_tenant_message = [0u8; 64];
    assert!(tokio::time::timeout(
        Duration::from_millis(250),
        alpha_subscriber.read(&mut cross_tenant_message),
    )
    .await
    .is_err());

    let mut alpha_publisher = tokio::net::TcpStream::connect(address).await.unwrap();
    alpha_publisher.write_all(
        b"*2\r\n$4\r\nAUTH\r\n$12\r\nalpha-secret\r\n*3\r\n$7\r\nPUBLISH\r\n$4\r\nroom\r\n$2\r\nyes\r\n",
    ).await.unwrap();
    alpha_publisher.read_exact(&mut auth_reply).await.unwrap();
    assert_eq!(&auth_reply, b"+OK\r\n");
    alpha_publisher.read_exact(&mut publish_reply).await.unwrap();
    assert_eq!(&publish_reply, b":1\r\n");
    let mut alpha_message = [0u8; 36];
    alpha_subscriber.read_exact(&mut alpha_message).await.unwrap();
    assert_eq!(&alpha_message, b"*3\r\n$7\r\nmessage\r\n$4\r\nroom\r\n$3\r\nyes\r\n");

    task.abort();
}
