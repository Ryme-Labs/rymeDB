use ryme_auth::{ApiKeyStore, PolicyEngine, Principal, Role};
use ryme_gateway::Gateway;
use ryme_realtime::Realtime;
use ryme_wire_grpc::{proto, GrpcGateway};
use std::collections::HashSet;
use tonic::Request;

fn fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn authed<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert("authorization", "Bearer k".parse().unwrap());
    request
}

async fn tls_channel(
    addr: std::net::SocketAddr,
) -> proto::ryme_client::RymeClient<tonic::transport::Channel> {
    let ca = std::fs::read(fixture("grpc-localhost-ca-cert.pem")).unwrap();
    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca))
        .domain_name("localhost");
    let channel = tonic::transport::Channel::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect()
        .await
        .unwrap();
    proto::ryme_client::RymeClient::new(channel)
}

#[tokio::test]
async fn grpc_tls_handshake_and_roundtrip() {
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
    let acceptor = ryme_tls::TlsAcceptor::from_pem_files(
        &fixture("grpc-localhost-cert.pem"),
        &fixture("grpc-localhost-key.pem"),
    )
    .unwrap();
    let grpc = GrpcGateway::with_gateway(gateway, keys);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = grpc.serve_tls(listener, acceptor).await;
    });
    let mut client = tls_channel(addr).await;
    let reply = client.health(authed(proto::HealthRequest {})).await.unwrap().into_inner();
    assert!(reply.ok);
    let put = client
        .kv_put(authed(proto::KvPutRequest {
            table: String::from("docs"),
            pk: b"tls".to_vec(),
            value: b"yes".to_vec(),
            ttl_secs: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(put.ok, "{}", put.error);
    let get = client
        .kv_get(authed(proto::KvGetRequest { table: String::from("docs"), pk: b"tls".to_vec() }))
        .await
        .unwrap()
        .into_inner();
    assert!(get.found);
    assert_eq!(get.value, b"yes");
}
