use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn serve() -> std::net::SocketAddr {
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    address
}

fn frame(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
    }
    out
}

async fn command(addr: std::net::SocketAddr, parts: &[&str]) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(&frame(parts)).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 4096];
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(read > 0, "connection closed before reply");
            raw.extend_from_slice(&chunk[..read]);
            if raw.ends_with(b"\r\n") {
                break;
            }
        }
    })
    .await
    .unwrap();
    String::from_utf8(raw).unwrap()
}

#[tokio::test]
async fn eval_runs_atomic_commands_with_keys_and_arguments() {
    let addr = serve().await;
    assert_eq!(
        command(
            addr,
            &["EVAL", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "counter", "41",],
        )
        .await,
        "$2\r\nOK\r\n"
    );
    assert_eq!(command(addr, &["GET", "counter"]).await, "$2\r\n41\r\n");
    assert_eq!(
        command(
            addr,
            &["EVAL", "return redis.call('INCRBY', KEYS[1], ARGV[1])", "1", "counter", "1"]
        )
        .await,
        ":42\r\n"
    );
}

#[tokio::test]
async fn eval_returns_arrays_and_script_cache_supports_evalsha() {
    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "a", "one"]).await, "+OK\r\n");
    assert_eq!(command(addr, &["SET", "b", "two"]).await, "+OK\r\n");
    let script = "return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}";
    let loaded = command(addr, &["SCRIPT", "LOAD", script]).await;
    let sha = loaded.split("\r\n").nth(1).unwrap_or("").to_string();
    assert_eq!(sha.len(), 40);
    assert_eq!(command(addr, &["SCRIPT", "EXISTS", &sha]).await, "*1\r\n:1\r\n");
    assert_eq!(
        command(addr, &["EVALSHA", &sha, "2", "a", "b"]).await,
        "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n"
    );
    assert_eq!(command(addr, &["SCRIPT", "FLUSH"]).await, "+OK\r\n");
    assert!(command(addr, &["EVALSHA", &sha, "0"]).await.starts_with("-NOSCRIPT "));
}

#[tokio::test]
async fn pcall_returns_command_errors_to_lua_and_preserves_error_replies() {
    let addr = serve().await;
    assert_eq!(command(addr, &["SET", "counter", "not-an-integer"]).await, "+OK\r\n");
    assert_eq!(
        command(addr, &["EVAL", "return redis.pcall('INCR', KEYS[1])", "1", "counter",],).await,
        "-ERR value is not an integer or out of range\r\n"
    );
    assert_eq!(
        command(
            addr,
            &[
                "EVAL",
                "local reply = redis.pcall('INCR', KEYS[1]); return reply.err and 'handled' or reply",
                "1",
                "counter",
            ],
        )
        .await,
        "$7\r\nhandled\r\n"
    );
}

#[tokio::test]
async fn script_flush_accepts_async_mode() {
    let addr = serve().await;
    let script = "return 'cached'";
    let loaded = command(addr, &["SCRIPT", "LOAD", script]).await;
    let sha = loaded.split("\r\n").nth(1).unwrap_or("").to_string();
    assert_eq!(command(addr, &["SCRIPT", "FLUSH", "ASYNC"]).await, "+OK\r\n");
    assert!(command(addr, &["EVALSHA", &sha, "0"]).await.starts_with("-NOSCRIPT "));
}

#[tokio::test]
async fn eval_has_no_filesystem_or_process_libraries() {
    let addr = serve().await;
    let reply = command(addr, &["EVAL", "return os.execute('echo unsafe')", "0"]).await;
    assert!(reply.starts_with("-ERR Error running script:"), "unexpected reply: {reply}");
}

#[tokio::test]
async fn eval_stops_scripts_that_run_too_long() {
    let addr = serve().await;
    let reply = command(addr, &["EVAL", "while true do end", "0"]).await;
    assert!(reply.contains("instruction limit"), "unexpected reply: {reply}");
}
