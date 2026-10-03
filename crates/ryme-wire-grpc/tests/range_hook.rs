use ryme_auth::{ApiKeyStore, PolicyEngine, Principal, Role};
use ryme_gateway::Gateway;
use ryme_realtime::Realtime;
use ryme_router::RangeLoadHook;
use ryme_wire_grpc::{proto, GrpcGateway};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tonic::Request;

fn authed<T>(message: T, key: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert("authorization", format!("Bearer {key}").parse().unwrap());
    request
}

#[tokio::test]
async fn range_hook_records_grpc_kv_writes() {
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
    let grpc = GrpcGateway::with_gateway(gateway, keys).with_range_hook(hook);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = grpc.serve_with_incoming(listener).await;
    });
    let mut client =
        proto::ryme_client::RymeClient::connect(format!("http://{addr}")).await.unwrap();
    let reply = client
        .kv_put(authed(
            proto::KvPutRequest {
                table: String::from("docs"),
                pk: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl_secs: 0,
            },
            "k",
        ))
        .await
        .unwrap();
    assert!(reply.into_inner().ok);
    let reply = client
        .kv_delete(authed(
            proto::KvDeleteRequest { table: String::from("docs"), pk: b"a".to_vec() },
            "k",
        ))
        .await
        .unwrap();
    assert!(reply.into_inner().ok);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], (b"docs\0a".to_vec(), 1));
    assert_eq!(seen[1], (b"docs\0a".to_vec(), 1));
}
