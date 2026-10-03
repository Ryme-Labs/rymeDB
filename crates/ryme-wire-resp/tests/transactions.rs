use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Session {
    socket: tokio::net::TcpStream,
}

impl Session {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let socket = tokio::net::TcpStream::connect(addr).await.unwrap();
        Self { socket }
    }

    async fn command(&mut self, parts: &[&str]) -> String {
        let mut frame = format!("*{}\r\n", parts.len());
        for part in parts {
            frame.push_str(&format!("${}\r\n{part}\r\n", part.len()));
        }
        self.socket.write_all(frame.as_bytes()).await.unwrap();
        let mut raw = Vec::new();
        let mut chunk = vec![0u8; 4096];
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let read = self.socket.read(&mut chunk).await.unwrap();
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
}

async fn serve() -> std::net::SocketAddr {
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    addr
}

#[tokio::test]
async fn multi_exec_applies_atomically() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "tx:a", "tx:b"]).await, ":0");
    assert_eq!(session.command(&["MULTI"]).await, "+OK");
    assert_eq!(session.command(&["SET", "tx:a", "1"]).await, "+QUEUED");
    assert_eq!(session.command(&["SET", "tx:b", "2"]).await, "+QUEUED");
    assert_eq!(session.command(&["EXEC"]).await, "*2\r\n+OK\r\n+OK");
    assert_eq!(session.command(&["MGET", "tx:a", "tx:b"]).await, "*2\r\n$1\r\n1\r\n$1\r\n2");
}

#[tokio::test]
async fn multi_exec_reads_own_writes() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "tx:c"]).await, ":0");
    assert_eq!(session.command(&["MULTI"]).await, "+OK");
    assert_eq!(session.command(&["INCRBY", "tx:c", "5"]).await, "+QUEUED");
    assert_eq!(session.command(&["GET", "tx:c"]).await, "+QUEUED");
    assert_eq!(session.command(&["EXEC"]).await, "*2\r\n:5\r\n$1\r\n5");
}

#[tokio::test]
async fn discard_drops_queue() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "tx:d"]).await, ":0");
    assert_eq!(session.command(&["MULTI"]).await, "+OK");
    assert_eq!(session.command(&["SET", "tx:d", "1"]).await, "+QUEUED");
    assert_eq!(session.command(&["DISCARD"]).await, "+OK");
    assert_eq!(session.command(&["GET", "tx:d"]).await, "$-1");
}

#[tokio::test]
async fn control_errors() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert!(session.command(&["EXEC"]).await.starts_with("-ERR"));
    assert!(session.command(&["DISCARD"]).await.starts_with("-ERR"));
    assert_eq!(session.command(&["MULTI"]).await, "+OK");
    assert!(session.command(&["MULTI"]).await.starts_with("-ERR"));
    assert_eq!(session.command(&["DISCARD"]).await, "+OK");
    assert_eq!(session.command(&["MULTI"]).await, "+OK");
    assert_eq!(session.command(&["EXEC"]).await, "*0");
}

#[tokio::test]
async fn unknown_command_aborts_exec() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "tx:e"]).await, ":0");
    assert_eq!(session.command(&["MULTI"]).await, "+OK");
    assert_eq!(session.command(&["SET", "tx:e", "1"]).await, "+QUEUED");
    assert!(session.command(&["NOSUCHCOMMAND"]).await.starts_with("-ERR"));
    assert!(session.command(&["EXEC"]).await.starts_with("-EXECABORT"));
    assert_eq!(session.command(&["GET", "tx:e"]).await, "$-1");
}

#[tokio::test]
async fn element_error_keeps_other_writes() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "tx:f", "tx:g"]).await, ":0");
    assert_eq!(session.command(&["SET", "tx:f", "not-a-number"]).await, "+OK");
    assert_eq!(session.command(&["MULTI"]).await, "+OK");
    assert_eq!(session.command(&["INCR", "tx:f"]).await, "+QUEUED");
    assert_eq!(session.command(&["SET", "tx:g", "ok"]).await, "+QUEUED");
    assert_eq!(
        session.command(&["EXEC"]).await,
        "*2\r\n-ERR value is not an integer or out of range\r\n+OK"
    );
    assert_eq!(session.command(&["GET", "tx:g"]).await, "$2\r\nok");
}

#[tokio::test]
async fn blpop_immediate_and_order() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "bl:a", "bl:b"]).await, ":0");
    assert_eq!(session.command(&["RPUSH", "bl:a", "one", "two"]).await, ":2");
    assert_eq!(session.command(&["BLPOP", "bl:a", "1"]).await, "*2\r\n$4\r\nbl:a\r\n$3\r\none");
    assert_eq!(session.command(&["BRPOP", "bl:a", "1"]).await, "*2\r\n$4\r\nbl:a\r\n$3\r\ntwo");
    assert_eq!(session.command(&["RPUSH", "bl:b", "bee"]).await, ":1");
    assert_eq!(
        session.command(&["BLPOP", "bl:a", "bl:b", "1"]).await,
        "*2\r\n$4\r\nbl:b\r\n$3\r\nbee"
    );
}

#[tokio::test]
async fn blpop_wakes_on_push() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "bl:w"]).await, ":0");
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let mut pusher = Session::connect(addr).await;
        assert_eq!(pusher.command(&["RPUSH", "bl:w", "late"]).await, ":1");
    });
    let start = std::time::Instant::now();
    assert_eq!(session.command(&["BLPOP", "bl:w", "5"]).await, "*2\r\n$4\r\nbl:w\r\n$4\r\nlate");
    assert!(start.elapsed() < std::time::Duration::from_secs(4));
}

#[tokio::test]
async fn blpop_timeout_returns_nil() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "bl:t"]).await, ":0");
    let start = std::time::Instant::now();
    assert_eq!(session.command(&["BLPOP", "bl:t", "0.2"]).await, "*-1");
    assert!(start.elapsed() >= std::time::Duration::from_millis(200));
    assert!(session.command(&["BLPOP", "bl:t", "xx"]).await.starts_with("-ERR"));
    assert_eq!(session.command(&["SET", "plain", "x"]).await, "+OK");
    assert!(session.command(&["BLPOP", "plain", "1"]).await.starts_with("-ERR"));
}

#[tokio::test]
async fn xread_block_waits_for_entries() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "bl:st"]).await, ":0");
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let mut adder = Session::connect(addr).await;
        assert_eq!(adder.command(&["XADD", "bl:st", "300-1", "f", "v"]).await, "$5\r\n300-1");
    });
    let start = std::time::Instant::now();
    assert_eq!(
        session.command(&["XREAD", "BLOCK", "5000", "STREAMS", "bl:st", "0-0"]).await,
        "*1\r\n*2\r\n$5\r\nbl:st\r\n*1\r\n*2\r\n$5\r\n300-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv"
    );
    assert!(start.elapsed() < std::time::Duration::from_secs(4));
    let start = std::time::Instant::now();
    assert_eq!(session.command(&["XREAD", "BLOCK", "200", "STREAMS", "bl:st", "$"]).await, "*-1");
    assert!(start.elapsed() >= std::time::Duration::from_millis(200));
}

#[tokio::test]
async fn consumer_group_flow() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "cg:s"]).await, ":0");
    assert!(session.command(&["XGROUP", "CREATE", "cg:s", "g1", "0-0"]).await.starts_with("-ERR"));
    assert_eq!(session.command(&["XADD", "cg:s", "400-1", "f", "a"]).await, "$5\r\n400-1");
    assert_eq!(session.command(&["XADD", "cg:s", "400-2", "f", "b"]).await, "$5\r\n400-2");
    assert_eq!(session.command(&["XGROUP", "CREATE", "cg:s", "g1", "0-0"]).await, "+OK");
    assert!(session.command(&["XGROUP", "CREATE", "cg:s", "g1", "0-0"]).await.starts_with("-ERR"));
    assert_eq!(
        session.command(&["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "cg:s", ">"]).await,
        "*1\r\n*2\r\n$4\r\ncg:s\r\n*2\r\n*2\r\n$5\r\n400-1\r\n*2\r\n$1\r\nf\r\n$1\r\na\r\n*2\r\n$5\r\n400-2\r\n*2\r\n$1\r\nf\r\n$1\r\nb"
    );
    assert_eq!(
        session.command(&["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "cg:s", ">"]).await,
        "*-1"
    );
    assert_eq!(
        session.command(&["XPENDING", "cg:s", "g1"]).await,
        "*4\r\n:2\r\n$5\r\n400-1\r\n$5\r\n400-2\r\n*1\r\n*2\r\n$2\r\nc1\r\n$1\r\n2"
    );
    assert_eq!(session.command(&["XACK", "cg:s", "g1", "400-1", "ghost"]).await, ":1");
    assert_eq!(
        session.command(&["XPENDING", "cg:s", "g1"]).await,
        "*4\r\n:1\r\n$5\r\n400-2\r\n$5\r\n400-2\r\n*1\r\n*2\r\n$2\r\nc1\r\n$1\r\n1"
    );
    assert_eq!(
        session
            .command(&["XREADGROUP", "GROUP", "g1", "c2", "COUNT", "1", "STREAMS", "cg:s", ">"])
            .await,
        "*-1"
    );
    assert_eq!(session.command(&["XGROUP", "SETID", "cg:s", "g1", "400-1"]).await, "+OK");
    assert_eq!(
        session.command(&["XREADGROUP", "GROUP", "g1", "c2", "STREAMS", "cg:s", ">"]).await,
        "*1\r\n*2\r\n$4\r\ncg:s\r\n*1\r\n*2\r\n$5\r\n400-2\r\n*2\r\n$1\r\nf\r\n$1\r\nb"
    );
    assert_eq!(session.command(&["XGROUP", "DESTROY", "cg:s", "g1"]).await, ":1");
    assert!(session.command(&["XACK", "cg:s", "g1", "400-2"]).await.starts_with("-ERR"));
    assert!(session
        .command(&["XREADGROUP", "GROUP", "ghost", "c1", "STREAMS", "cg:s", ">"])
        .await
        .starts_with("-ERR"));
}

#[tokio::test]
async fn autoclaim_moves_idle_entries() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "ac:s"]).await, ":0");
    assert_eq!(session.command(&["XADD", "ac:s", "500-1", "f", "a"]).await, "$5\r\n500-1");
    assert_eq!(session.command(&["XADD", "ac:s", "500-2", "f", "b"]).await, "$5\r\n500-2");
    assert_eq!(session.command(&["XGROUP", "CREATE", "ac:s", "g", "0-0"]).await, "+OK");
    assert_eq!(
        session.command(&["XREADGROUP", "GROUP", "g", "slow", "STREAMS", "ac:s", ">"]).await,
        "*1\r\n*2\r\n$4\r\nac:s\r\n*2\r\n*2\r\n$5\r\n500-1\r\n*2\r\n$1\r\nf\r\n$1\r\na\r\n*2\r\n$5\r\n500-2\r\n*2\r\n$1\r\nf\r\n$1\r\nb"
    );
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    assert_eq!(
        session.command(&["XAUTOCLAIM", "ac:s", "g", "fast", "100", "0-0"]).await,
        "*3\r\n$3\r\n0-0\r\n*2\r\n*2\r\n$5\r\n500-1\r\n*2\r\n$1\r\nf\r\n$1\r\na\r\n*2\r\n$5\r\n500-2\r\n*2\r\n$1\r\nf\r\n$1\r\nb\r\n*0"
    );
    assert_eq!(
        session.command(&["XPENDING", "ac:s", "g"]).await,
        "*4\r\n:2\r\n$5\r\n500-1\r\n$5\r\n500-2\r\n*1\r\n*2\r\n$4\r\nfast\r\n$1\r\n2"
    );
    assert_eq!(
        session.command(&["XAUTOCLAIM", "ac:s", "g", "fast", "60000", "0-0", "COUNT", "1"]).await,
        "*3\r\n$3\r\n0-0\r\n*0\r\n*0"
    );
    assert_eq!(
        session.command(&["XAUTOCLAIM", "ac:s", "g", "fast", "0", "0-0", "COUNT", "1"]).await,
        "*3\r\n$5\r\n500-2\r\n*1\r\n*2\r\n$5\r\n500-1\r\n*2\r\n$1\r\nf\r\n$1\r\na\r\n*0"
    );
    assert_eq!(
        session.command(&["XAUTOCLAIM", "ac:s", "g", "fast", "0", "500-1", "COUNT", "1"]).await,
        "*3\r\n$3\r\n0-0\r\n*1\r\n*2\r\n$5\r\n500-2\r\n*2\r\n$1\r\nf\r\n$1\r\nb\r\n*0"
    );
    assert!(session
        .command(&["XAUTOCLAIM", "ac:s", "ghost", "fast", "0", "0-0"])
        .await
        .starts_with("-ERR"));
}

#[tokio::test]
async fn blmove_moves_atomically() {
    let addr = serve().await;
    let mut session = Session::connect(addr).await;
    assert_eq!(session.command(&["DEL", "bm:s", "bm:d"]).await, ":0");
    assert_eq!(session.command(&["RPUSH", "bm:s", "a", "b"]).await, ":2");
    assert_eq!(session.command(&["BLMOVE", "bm:s", "bm:d", "LEFT", "RIGHT", "1"]).await, "$1\r\na");
    assert_eq!(session.command(&["LRANGE", "bm:s", "0", "-1"]).await, "*1\r\n$1\r\nb");
    assert_eq!(session.command(&["LRANGE", "bm:d", "0", "-1"]).await, "*1\r\n$1\r\na");
    assert_eq!(
        session.command(&["BLMOVE", "bm:empty", "bm:d", "LEFT", "RIGHT", "0.2"]).await,
        "*-1"
    );
    assert!(session
        .command(&["BLMOVE", "bm:s", "bm:d", "UP", "RIGHT", "1"])
        .await
        .starts_with("-ERR"));
}
