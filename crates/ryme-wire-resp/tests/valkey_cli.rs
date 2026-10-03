async fn serve() -> std::net::SocketAddr {
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    addr
}

fn cli_available() -> bool {
    std::process::Command::new("valkey-cli")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test]
async fn valkey_cli_smoke() {
    if !cli_available() {
        return;
    }
    let addr = serve().await;
    let script = [
        "PING",
        "SET k v",
        "GET k",
        "INCR n",
        "HSET h f1 v1 f2 v2",
        "HGETALL h",
        "RPUSH l a b",
        "LRANGE l 0 -1",
        "SADD s x y",
        "SMEMBERS s",
        "ZADD z 1 a 2 b",
        "ZRANGE z 0 -1 WITHSCORES",
        "XADD st 9-1 f v",
        "XREAD STREAMS st 0-0",
        "GEOADD g 13.361389 38.115556 Palermo",
        "GEODIST g Palermo Palermo",
        "SCAN 0 MATCH sk:*",
        "MSET sk:a 1 sk:b 2",
        "SCAN 0 MATCH sk:*",
        "MULTI",
        "SET m 1",
        "EXEC",
        "NOSUCHCMD",
        "GET ghost",
    ]
    .join("\n")
        + "\n";
    let mut child = tokio::process::Command::new("valkey-cli")
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(addr.port().to_string())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(script.as_bytes()).await.unwrap();
    }
    let output = child.wait_with_output().await.unwrap();
    assert!(output.status.success(), "{output:?}");
    let text = String::from_utf8(output.stdout).unwrap();
    let expected = [
        "PONG",
        "OK",
        "v",
        "1",
        "2",
        "f1",
        "v1",
        "f2",
        "v2",
        "2",
        "a",
        "b",
        "2",
        "x",
        "y",
        "2",
        "a",
        "1",
        "b",
        "2",
        "9-1",
        "st",
        "9-1",
        "f",
        "v",
        "1",
        "0.0000",
        "0",
        "",
        "OK",
        "0",
        "sk:a",
        "sk:b",
        "OK",
        "QUEUED",
        "OK",
        "ERR unknown command",
        "",
        "",
    ]
    .join("\n")
        + "\n";
    assert_eq!(text, expected, "\n{text}");
}
