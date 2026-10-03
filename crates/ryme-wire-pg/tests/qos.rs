use std::sync::{Arc, Mutex};
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

async fn query(addr: std::net::SocketAddr, sql: &str) -> Vec<u8> {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut startup = Vec::new();
    startup.extend_from_slice(&16u32.to_be_bytes());
    startup.extend_from_slice(&196608u32.to_be_bytes());
    startup.extend_from_slice(b"user\0u\0\0");
    socket.write_all(&startup).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (tag, _) = read_frame(&mut socket).await;
            if tag == b'Z' {
                break;
            }
        }
        let mut frame = vec![b'Q'];
        let payload = format!("{sql}\0");
        frame.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        frame.extend_from_slice(payload.as_bytes());
        socket.write_all(&frame).await.unwrap();
        let mut out = Vec::new();
        loop {
            let (tag, body) = read_frame(&mut socket).await;
            out.extend_from_slice(&body);
            if tag == b'Z' {
                break;
            }
        }
        out
    })
    .await
    .unwrap()
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[tokio::test]
async fn pg_qos_throttles_writes() {
    let qos = Arc::new(Mutex::new(ryme_qos::QosRegistry::new()));
    let metering = Arc::new(Mutex::new(ryme_metering::MeterRegistry::new()));
    {
        let mut registry = qos.lock().unwrap();
        registry.set_quota(
            "t",
            ryme_qos::Quota { write_qps: 1, ..ryme_qos::Quota::for_tier(ryme_qos::Tier::Shared) },
            now_nanos(),
        );
    }
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"))
        .with_qos(qos.clone())
        .with_metering(metering.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let first = query(addr, "INSERT INTO docs KEY 'k1' VALUE 'v1'").await;
    assert!(!String::from_utf8_lossy(&first).contains("quota"), "{first:?}");
    let denied = query(addr, "INSERT INTO docs KEY 'k2' VALUE 'v2'").await;
    assert!(String::from_utf8_lossy(&denied).contains("quota"), "{denied:?}");
    let registry = metering.lock().unwrap();
    let writes = registry.total_for("t", "d", ryme_metering::Metric::WriteUnit);
    assert_eq!(writes.quantity, 1);
}

#[tokio::test]
async fn pg_qos_throttles_response_bytes() {
    let qos = Arc::new(Mutex::new(ryme_qos::QosRegistry::new()));
    {
        let mut registry = qos.lock().unwrap();
        registry.set_quota(
            "t",
            ryme_qos::Quota {
                egress_bytes_per_sec: 1024,
                ..ryme_qos::Quota::for_tier(ryme_qos::Tier::Shared)
            },
            now_nanos(),
        );
    }
    let gateway =
        ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d")).with_qos(qos.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let big = "v".repeat(200);
    let inserted = query(addr, &format!("INSERT INTO docs KEY 'big' VALUE '{big}'")).await;
    assert!(!String::from_utf8_lossy(&inserted).contains("quota"), "{inserted:?}");
    let first = query(addr, "SELECT * FROM docs KEY 'big'").await;
    let text = String::from_utf8_lossy(&first).into_owned();
    assert!(text.contains(&big), "{text}");
    for _ in 0..4 {
        let denied = query(addr, "SELECT * FROM docs KEY 'big'").await;
        let text = String::from_utf8_lossy(&denied).into_owned();
        if text.contains("egress") {
            return;
        }
    }
    panic!("shared egress bucket never throttled 200B PG reads");
}

#[tokio::test]
async fn pg_observe_records_statements() {
    let latency = ryme_observe::LatencyWindow::new();
    let histogram = ryme_observe::Histogram::new(128);
    let slow_log = ryme_observe::SlowLog::new(16);
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d")).with_observe(
        latency.clone(),
        histogram.clone(),
        slow_log.clone(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    let _ = query(addr, "INSERT INTO docs KEY 'k1' VALUE 'v1'").await;
    let _ = query(addr, "SELECT * FROM docs KEY 'k1'").await;
    assert_eq!(latency.snapshot().count, 2);
    assert_eq!(histogram.snapshot().count, 2);
    assert!(slow_log.is_empty());
}
