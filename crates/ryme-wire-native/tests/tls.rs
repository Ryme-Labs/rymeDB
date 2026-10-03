use ryme_auth::{ApiKeyStore, PolicyEngine, Principal, Role};
use ryme_gateway::Gateway;
use ryme_realtime::Realtime;
use ryme_wire_native::{NativeGateway, TlsAcceptor};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[tokio::test]
async fn tls_ping_roundtrip() {
    let acceptor = TlsAcceptor::from_pem_files(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/ryme-test-cert.pem")
            .as_path(),
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/ryme-test-key.pem")
            .as_path(),
    )
    .unwrap();
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
    let native = NativeGateway::with_gateway(gateway, keys);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = native.serve_tls(listener, 16, acceptor).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(server_name, stream).await.unwrap();
    let body = serde_json::json!({ "key": "k", "op": "ping" });
    let raw = serde_json::to_vec(&body).unwrap();
    let mut frame = (raw.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&raw);
    tls.write_all(&frame).await.unwrap();
    let mut header = [0u8; 4];
    tls.read_exact(&mut header).await.unwrap();
    let len = u32::from_be_bytes(header) as usize;
    let mut response = vec![0u8; len];
    tls.read_exact(&mut response).await.unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(parsed.get("ok"), Some(&serde_json::Value::Bool(true)));
}

fn fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn mutual_acceptor() -> TlsAcceptor {
    TlsAcceptor::from_pem_files_mutual(
        fixture("ryme-test-cert.pem").as_path(),
        fixture("ryme-test-key.pem").as_path(),
        fixture("ryme-test-ca-cert.pem").as_path(),
    )
    .unwrap()
}

fn mtls_connector() -> tokio_rustls::TlsConnector {
    use rustls_pki_types::pem::PemObject;
    let cert_raw = std::fs::read(fixture("ryme-test-client-cert.pem")).unwrap();
    let key_raw = std::fs::read(fixture("ryme-test-client-key.pem")).unwrap();
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls::pki_types::CertificateDer::pem_slice_iter(&cert_raw)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let key = rustls::pki_types::PrivateKeyDer::pem_slice_iter(&key_raw).next().unwrap().unwrap();
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_client_auth_cert(certs, key)
        .unwrap();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

async fn tls_ping(
    tls: &mut tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
) -> serde_json::Value {
    let body = serde_json::json!({ "key": "k", "op": "ping" });
    let raw = serde_json::to_vec(&body).unwrap();
    let mut frame = (raw.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&raw);
    tls.write_all(&frame).await.unwrap();
    let mut header = [0u8; 4];
    tls.read_exact(&mut header).await.unwrap();
    let len = u32::from_be_bytes(header) as usize;
    let mut response = vec![0u8; len];
    tls.read_exact(&mut response).await.unwrap();
    serde_json::from_slice(&response).unwrap()
}

fn test_service() -> NativeGateway {
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

#[tokio::test]
async fn mutual_tls_accepts_client_cert() {
    let native = test_service();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = native.serve_tls(listener, 16, mutual_acceptor()).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let connector = mtls_connector();
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(server_name, stream).await.unwrap();
    let parsed = tls_ping(&mut tls).await;
    assert_eq!(parsed.get("ok"), Some(&serde_json::Value::Bool(true)));
}

#[tokio::test]
async fn mutual_tls_rejects_bare_client() {
    let native = test_service();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = native.serve_tls(listener, 16, mutual_acceptor()).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let outcome = connector.connect(server_name, stream).await;
    if let Ok(mut tls) = outcome {
        let body = serde_json::json!({ "key": "k", "op": "ping" });
        let raw = serde_json::to_vec(&body).unwrap();
        let mut frame = (raw.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&raw);
        let mut failed = tls.write_all(&frame).await.is_err();
        if !failed {
            let mut header = [0u8; 4];
            failed = tls.read_exact(&mut header).await.is_err();
        }
        assert!(failed, "bare client must not complete an authenticated ping");
    }
}
