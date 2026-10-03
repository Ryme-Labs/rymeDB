use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn command(addr: std::net::SocketAddr, parts: &[&str]) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut frame = format!("*{}\r\n", parts.len());
    for part in parts {
        frame.push_str(&format!("${}\r\n{part}\r\n", part.len()));
    }
    socket.write_all(frame.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 4096];
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let read = socket.read(&mut chunk).await.unwrap();
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

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[tokio::test]
async fn qos_throttles_writes_not_reads() {
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
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"))
        .with_qos(qos.clone())
        .with_metering(metering.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    assert_eq!(command(addr, &["SET", "a", "1"]).await, "+OK");
    assert_eq!(command(addr, &["GET", "a"]).await, "$1\r\n1");
    let denied = command(addr, &["SET", "b", "2"]).await;
    assert!(denied.contains("quota"), "{denied}");
    assert_eq!(command(addr, &["GET", "missing"]).await, "$-1");
    let registry = metering.lock().unwrap();
    let writes = registry.total_for("t", "d", ryme_metering::Metric::WriteUnit);
    let reads = registry.total_for("t", "d", ryme_metering::Metric::ReadUnit);
    assert_eq!(writes.quantity, 1);
    assert!(reads.quantity >= 2);
}

#[tokio::test]
async fn qos_absent_means_unlimited() {
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    for index in 0..10 {
        assert_eq!(command(addr, &["SET", &format!("k{index}"), "v"]).await, "+OK");
    }
}

#[tokio::test]
async fn observe_records_commands() {
    let latency = ryme_observe::LatencyWindow::new();
    let histogram = ryme_observe::Histogram::new(128);
    let slow_log = ryme_observe::SlowLog::new(16);
    let gateway = ryme_wire_resp::RespGateway::new(String::from("t"), String::from("d"))
        .with_observe(latency.clone(), histogram.clone(), slow_log.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    assert_eq!(command(addr, &["SET", "a", "1"]).await, "+OK");
    assert_eq!(command(addr, &["GET", "a"]).await, "$1\r\n1");
    assert_eq!(latency.snapshot().count, 2);
    assert_eq!(histogram.snapshot().count, 2);
    assert!(slow_log.is_empty());
}
