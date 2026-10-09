use ryme_auth::{ApiKeyStore, PolicyEngine, Principal, Role};
use ryme_gateway::Gateway;
use ryme_realtime::Realtime;
use ryme_wire_native::{encode_frame, NativeGateway};
use std::collections::HashSet;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn test_gateway() -> NativeGateway {
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
    NativeGateway::with_gateway(gateway, keys)
}

async fn read_response(socket: &mut tokio::net::TcpStream) -> serde_json::Value {
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await.unwrap();
    let length = u32::from_be_bytes(header) as usize;
    let mut body = vec![0u8; length];
    socket.read_exact(&mut body).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn pipelined_frames_keep_response_order() {
    let native = test_gateway();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = native.serve(listener).await;
    });

    let request = serde_json::json!({"key": "k", "op": "ping"});
    let frame = encode_frame(&serde_json::to_vec(&request).unwrap()).unwrap();
    let mut pipeline = frame.clone();
    pipeline.extend_from_slice(&frame);
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(&pipeline).await.unwrap();

    assert_eq!(read_response(&mut socket).await["ok"], true);
    assert_eq!(read_response(&mut socket).await["ok"], true);
}
