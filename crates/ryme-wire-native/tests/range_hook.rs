use ryme_auth::{ApiKeyStore, PolicyEngine, Principal, Role};
use ryme_gateway::Gateway;
use ryme_realtime::Realtime;
use ryme_router::RangeLoadHook;
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

#[tokio::test]
async fn range_hook_records_native_put_and_delete() {
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
    let seen = Arc::new(Mutex::new(Vec::new()));
    let moved = seen.clone();
    let hook = RangeLoadHook::armed(Arc::new(move |key: &[u8], count: u64| {
        moved.lock().unwrap().push((key.to_vec(), count));
    }));
    let native = NativeGateway::with_gateway(gateway, keys).with_range_hook(hook);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = native.serve(listener).await;
    });
    let put =
        serde_json::json!({"key": "k", "op": "put", "table": "docs", "pk": "a", "value": "1"});
    let reply = call(addr, put).await;
    assert_eq!(reply["ok"], true);
    let delete = serde_json::json!({"key": "k", "op": "delete", "table": "docs", "pk": "a"});
    let reply = call(addr, delete).await;
    assert_eq!(reply["ok"], true);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    let mut expected = b"docs\0a".to_vec();
    assert_eq!(seen[0], (expected.clone(), 1));
    expected = b"docs\0a".to_vec();
    assert_eq!(seen[1], (expected, 1));
}
