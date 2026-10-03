use ryme_auth::{ApiKeyStore, PolicyEngine, Principal, Role};
use ryme_gateway::Gateway;
use ryme_realtime::Realtime;
use ryme_wire_grpc::{proto, GrpcGateway};
use std::collections::HashSet;
use tonic::Request;

fn authed<T>(message: T, key: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert("authorization", format!("Bearer {key}").parse().unwrap());
    request
}

async fn serve() -> (std::net::SocketAddr, GrpcGateway) {
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
    let grpc = GrpcGateway::with_gateway(gateway, keys);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = grpc.clone();
    tokio::spawn(async move {
        let _ = service.serve_with_incoming(listener).await;
    });
    (addr, grpc)
}

async fn connect(
    addr: std::net::SocketAddr,
) -> proto::ryme_client::RymeClient<tonic::transport::Channel> {
    proto::ryme_client::RymeClient::connect(format!("http://{addr}")).await.unwrap()
}

#[tokio::test]
async fn grpc_health_and_kv_roundtrip() {
    let (addr, _) = serve().await;
    let mut client = connect(addr).await;
    let reply = client.health(authed(proto::HealthRequest {}, "k")).await.unwrap();
    assert!(reply.into_inner().ok);
    let put = client
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
        .unwrap()
        .into_inner();
    assert!(put.ok, "{}", put.error);
    let get = client
        .kv_get(authed(proto::KvGetRequest { table: String::from("docs"), pk: b"a".to_vec() }, "k"))
        .await
        .unwrap()
        .into_inner();
    assert!(get.found);
    assert_eq!(get.value, b"1");
    let missing = client
        .kv_get(authed(
            proto::KvGetRequest { table: String::from("docs"), pk: b"no".to_vec() },
            "k",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!missing.found);
    let scan = client
        .scan(authed(proto::ScanRequest { table: String::from("docs"), limit: 10 }, "k"))
        .await
        .unwrap()
        .into_inner();
    assert!(scan.ok);
    assert_eq!(scan.rows.len(), 1);
    let delete = client
        .kv_delete(authed(
            proto::KvDeleteRequest { table: String::from("docs"), pk: b"a".to_vec() },
            "k",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(delete.ok, "{}", delete.error);
    let gone = client
        .kv_get(authed(proto::KvGetRequest { table: String::from("docs"), pk: b"a".to_vec() }, "k"))
        .await
        .unwrap()
        .into_inner();
    assert!(!gone.found);
}

#[tokio::test]
async fn grpc_sql_and_auth() {
    let (addr, _) = serve().await;
    let mut client = connect(addr).await;
    let denied =
        client.kv_get(proto::KvGetRequest { table: String::from("docs"), pk: b"a".to_vec() }).await;
    assert!(denied.is_err());
    let reply = client
        .sql(authed(
            proto::SqlRequest { sql: String::from("INSERT INTO docs KEY 's1' VALUE 'v1'") },
            "k",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(reply.ok, "{}", reply.error);
    let reply = client
        .sql(authed(proto::SqlRequest { sql: String::from("SELECT * FROM docs KEY 's1'") }, "k"))
        .await
        .unwrap()
        .into_inner();
    assert!(reply.ok, "{}", reply.error);
    assert_eq!(reply.rows.len(), 1);
    assert_eq!(reply.rows[0].value, b"v1");
}

#[tokio::test]
async fn grpc_limited_roundtrip() {
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
    let grpc = GrpcGateway::with_gateway(gateway, keys);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = grpc.serve_with_incoming_limited(listener, 8).await;
    });
    let mut client = connect(addr).await;
    let reply = client.health(authed(proto::HealthRequest {}, "k")).await.unwrap();
    assert!(reply.into_inner().ok);
    let put = client
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
        .unwrap()
        .into_inner();
    assert!(put.ok, "{}", put.error);
}

#[tokio::test]
async fn grpc_shape_errors_surface() {
    let (addr, _) = serve().await;
    let mut client = connect(addr).await;
    let big_pk = vec![b'k'; ryme_gateway::MAX_KEY_BYTES + 1];
    let reply = client
        .kv_put(authed(
            proto::KvPutRequest {
                table: String::from("docs"),
                pk: big_pk,
                value: b"1".to_vec(),
                ttl_secs: 0,
            },
            "k",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!reply.ok);
    assert!(reply.error.contains("key"), "{}", reply.error);
    let err = client
        .kv_put(authed(
            proto::KvPutRequest {
                table: String::new(),
                pk: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl_secs: 0,
            },
            "k",
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");
}

#[tokio::test]
async fn grpc_read_only_rejects_sql_writes() {
    use ryme_wire_grpc::proto::ryme_server::Ryme;
    let mut inner = Gateway::new(
        String::from("t"),
        String::from("d"),
        String::from("main"),
        PolicyEngine::new(),
        Realtime::new(16),
    );
    inner.set_read_only(true);
    let mut roles = HashSet::new();
    roles.insert(Role::Owner);
    let keys = ApiKeyStore::new();
    keys.insert(
        String::from("k"),
        Principal { id: String::from("u"), tenant: String::from("t"), roles },
    );
    let grpc = GrpcGateway::with_gateway(inner, keys);
    let reply = grpc
        .sql(authed(
            proto::SqlRequest { sql: String::from("INSERT INTO docs KEY 'a' VALUE 'b'") },
            "k",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!reply.ok, "{reply:?}");
    assert!(reply.error.contains("read-only"), "{}", reply.error);
    let reply = grpc
        .sql(authed(proto::SqlRequest { sql: String::from("SELECT * FROM docs KEY 'a'") }, "k"))
        .await
        .unwrap()
        .into_inner();
    assert!(reply.ok, "{}", reply.error);
}

fn traced<T>(message: T, traceparent: &str) -> Request<T> {
    let mut request = authed(message, "k");
    request.metadata_mut().insert("traceparent", traceparent.parse().unwrap());
    request
}

#[tokio::test]
async fn grpc_traceparent_links_spans() {
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
    let traces = std::sync::Arc::new(std::sync::Mutex::new(ryme_observe::TraceCollector::new(16)));
    let grpc = GrpcGateway::with_gateway(gateway, keys).with_traces(traces.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = grpc.serve_with_incoming(listener).await;
    });
    let mut client = connect(addr).await;
    let header = "00-4bf92f3577b34da6a3ce929d0e0e4730-00f067aa0ba902b7-01";
    let reply = client
        .kv_put(traced(
            proto::KvPutRequest {
                table: String::from("docs"),
                pk: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl_secs: 0,
            },
            header,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(reply.ok, "{}", reply.error);
    let _ = client
        .kv_get(authed(proto::KvGetRequest { table: String::from("docs"), pk: b"a".to_vec() }, "k"))
        .await
        .unwrap();
    let spans = traces.lock().unwrap().recent(10);
    assert_eq!(spans.len(), 2);
    let linked =
        spans.iter().find(|span| span.trace_id == "4bf92f3577b34da6a3ce929d0e0e4730").unwrap();
    assert_eq!(linked.parent, Some(String::from("00f067aa0ba902b7")));
    let root = spans.iter().find(|span| span.trace_id != linked.trace_id).unwrap();
    assert_eq!(root.parent, None);
}
