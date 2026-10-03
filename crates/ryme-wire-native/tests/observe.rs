use ryme_auth::{ApiKeyStore, PolicyEngine, Principal, Role};
use ryme_gateway::Gateway;
use ryme_realtime::Realtime;
use ryme_wire_native::NativeGateway;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn call(addr: std::net::SocketAddr, body: serde_json::Value) -> serde_json::Value {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let raw = serde_json::to_vec(&body).unwrap();
    let mut frame = (raw.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&raw);
    socket.write_all(&frame).await.unwrap();
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await.unwrap();
    let len = u32::from_be_bytes(header) as usize;
    let mut response = vec![0u8; len];
    socket.read_exact(&mut response).await.unwrap();
    serde_json::from_slice(&response).unwrap()
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[tokio::test]
async fn native_observe_meter_and_classify_sql() {
    let gateway = Gateway::new(
        String::from("t"),
        String::from("d"),
        String::from("main"),
        PolicyEngine::new(),
        Realtime::new(16),
    );
    let mut roles = HashSet::new();
    roles.insert(Role::Owner);
    let keys = ApiKeyStore::new();
    keys.insert(
        String::from("k"),
        Principal { id: String::from("u"), tenant: String::from("t"), roles },
    );
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
    let latency = ryme_observe::LatencyWindow::new();
    let histogram = ryme_observe::Histogram::new(128);
    let slow_log = ryme_observe::SlowLog::new(16);
    let native = NativeGateway::with_qos(gateway, keys, qos.clone())
        .with_metering(metering.clone())
        .with_observe(latency.clone(), histogram.clone(), slow_log.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = native.serve(listener).await;
    });
    let put =
        serde_json::json!({"key": "k", "op": "put", "table": "docs", "pk": "a", "value": "1"});
    assert_eq!(call(addr, put).await.get("ok"), Some(&serde_json::Value::Bool(true)));
    let get = serde_json::json!({"key": "k", "op": "get", "table": "docs", "pk": "a"});
    assert_eq!(call(addr, get).await.get("ok"), Some(&serde_json::Value::Bool(true)));
    let insert =
        serde_json::json!({"key": "k", "op": "sql", "sql": "INSERT INTO docs KEY 'b' VALUE '2'"});
    let denied = call(addr, insert).await;
    assert!(denied.get("error").and_then(|v| v.as_str()).unwrap_or("").contains("quota"));
    assert_eq!(latency.snapshot().count, 3);
    assert_eq!(histogram.snapshot().count, 3);
    assert!(slow_log.is_empty());
    let registry = metering.lock().unwrap();
    assert_eq!(registry.total_for("t", "d", ryme_metering::Metric::WriteUnit).quantity, 1);
    assert_eq!(registry.total_for("t", "d", ryme_metering::Metric::ReadUnit).quantity, 1);
}
