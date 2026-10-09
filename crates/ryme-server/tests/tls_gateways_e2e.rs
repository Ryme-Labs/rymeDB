use ryme_config::Config;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: &str = "ryme-tls-gw-key-8d2f41c9a6e7";

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
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

fn fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

async fn bind_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

fn tls_connector() -> tokio_rustls::TlsConnector {
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

async fn http_request(addr: std::net::SocketAddr, head: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{head} HTTP/1.1\r\nhost: 127.0.0.1\r\nauthorization: Bearer {KEY}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 8192];
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
        }
    })
    .await
    .unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse::<u16>()
        .unwrap_or(0);
    let body = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|index| raw[index + 4..].to_vec())
        .unwrap_or_default();
    (status, body)
}

async fn read_pg_frame<S>(socket: &mut S) -> (u8, Vec<u8>)
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut tag = [0u8; 1];
    tokio::time::timeout(Duration::from_secs(5), socket.read_exact(&mut tag))
        .await
        .unwrap()
        .unwrap();
    let mut length_buffer = [0u8; 4];
    socket.read_exact(&mut length_buffer).await.unwrap();
    let length = u32::from_be_bytes(length_buffer) as usize;
    let mut payload = vec![0u8; length - 4];
    socket.read_exact(&mut payload).await.unwrap();
    (tag[0], payload)
}

#[tokio::test]
async fn pg_ssl_upgrade_roundtrip() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-tls-pg-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let config = Config {
        node_id: String::from("tls-pg"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        tls_cert_pem: Some(fixture("ryme-test-cert.pem")),
        tls_key_pem: Some(fixture("ryme-test-key.pem")),
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let _ = ryme_server::serve(config, pg_listener, resp_listener, http_listener).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, _) = http_request(http, "PUT /v1/kv/docs/tls1", b"{\"ok\":true}").await;
    assert_eq!(status, 200);
    let mut socket = tokio::net::TcpStream::connect(pg).await.unwrap();
    let mut ssl_request = 8u32.to_be_bytes().to_vec();
    ssl_request.extend_from_slice(&80877103u32.to_be_bytes());
    socket.write_all(&ssl_request).await.unwrap();
    let mut answer = [0u8; 1];
    socket.read_exact(&mut answer).await.unwrap();
    assert_eq!(answer, [b'S']);
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = tls_connector().connect(server_name, socket).await.unwrap();
    let mut startup = b"user\0u\0\0".to_vec();
    let mut frame = ((startup.len() + 8) as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&196608u32.to_be_bytes());
    frame.extend_from_slice(&startup);
    startup = frame;
    tls.write_all(&startup).await.unwrap();
    assert_eq!(read_pg_frame(&mut tls).await.0, b'R');
    loop {
        let (tag, _) = read_pg_frame(&mut tls).await;
        if tag == b'Z' {
            break;
        }
        assert!(tag == b'S' || tag == b'K');
    }
    let query = b"SELECT * FROM docs KEY 'tls1'";
    let mut q = vec![b'Q'];
    q.extend_from_slice(&((query.len() + 4) as u32).to_be_bytes());
    q.extend_from_slice(query);
    tls.write_all(&q).await.unwrap();
    let mut saw_data = false;
    for _ in 0..8 {
        let (tag, _) = read_pg_frame(&mut tls).await;
        if tag == b'D' {
            saw_data = true;
        }
        if tag == b'Z' {
            break;
        }
    }
    assert!(saw_data);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn resp_tls_ping_roundtrip() {
    std::env::set_var("RYME_API_KEY", KEY);
    let root = std::env::temp_dir().join(format!(
        "ryme-tls-resp-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pg_listener = bind_listener().await;
    let pg = pg_listener.local_addr().unwrap();
    let resp_listener = bind_listener().await;
    let resp = resp_listener.local_addr().unwrap();
    let http_listener = bind_listener().await;
    let http = http_listener.local_addr().unwrap();
    let resp_tls_listener = bind_listener().await;
    let resp_tls = resp_tls_listener.local_addr().unwrap();
    let config = Config {
        node_id: String::from("tls-resp"),
        data_dir: root.clone(),
        pg_listen: pg,
        resp_listen: resp,
        http_listen: http,
        resp_tls_listen: Some(resp_tls),
        tls_cert_pem: Some(fixture("ryme-test-cert.pem")),
        tls_key_pem: Some(fixture("ryme-test-key.pem")),
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        let extra = ryme_server::OptionalListeners {
            resp_tls: Some(resp_tls_listener),
            ..Default::default()
        };
        let _ = ryme_server::serve_with_optional(
            config,
            pg_listener,
            resp_listener,
            http_listener,
            extra,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mut tls = {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let attempt = tokio::net::TcpStream::connect(resp_tls).await;
            match attempt {
                Ok(stream) => {
                    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
                    match tls_connector().connect(server_name, stream).await {
                        Ok(tls) => break tls,
                        Err(_) if std::time::Instant::now() < deadline => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        Err(e) => panic!("tls connect failed: {e}"),
                    }
                }
                Err(_) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => panic!("tcp connect failed: {e}"),
            }
        }
    };
    tls.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 1024];
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let read = tls.read(&mut chunk).await.unwrap();
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
    assert!(raw.starts_with(b"+PONG"), "{raw:?}");
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}
