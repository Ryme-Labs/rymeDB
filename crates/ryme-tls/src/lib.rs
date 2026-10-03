use ryme_error::{Result, RymeError};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TlsAcceptor {
    config: Arc<rustls::ServerConfig>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

impl TlsAcceptor {
    pub fn from_pem_files(cert_path: &std::path::Path, key_path: &std::path::Path) -> Result<Self> {
        let cert_raw = std::fs::read(cert_path)?;
        let key_raw = std::fs::read(key_path)?;
        Self::from_pem_bytes(&cert_raw, &key_raw)
    }

    pub fn from_pem_files_mutual(
        cert_path: &std::path::Path,
        key_path: &std::path::Path,
        ca_path: &std::path::Path,
    ) -> Result<Self> {
        let cert_raw = std::fs::read(cert_path)?;
        let key_raw = std::fs::read(key_path)?;
        let ca_raw = std::fs::read(ca_path)?;
        Self::from_pem_bytes_mutual(&cert_raw, &key_raw, &ca_raw)
    }

    pub fn from_pem_bytes(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self> {
        let certs = read_certs(cert_pem, "tls cert")?;
        let key = tls_private_key(key_pem)?;
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
        Ok(Self {
            config: Arc::new(config),
            cert_pem: cert_pem.to_vec(),
            key_pem: key_pem.to_vec(),
        })
    }

    pub fn from_pem_bytes_mutual(cert_pem: &[u8], key_pem: &[u8], ca_pem: &[u8]) -> Result<Self> {
        let certs = read_certs(cert_pem, "tls cert")?;
        let key = tls_private_key(key_pem)?;
        let cas = read_certs(ca_pem, "tls client ca")?;
        let mut roots = rustls::RootCertStore::empty();
        for raw in cas {
            roots.add(raw).map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
        let config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
        Ok(Self {
            config: Arc::new(config),
            cert_pem: cert_pem.to_vec(),
            key_pem: key_pem.to_vec(),
        })
    }

    pub fn identity(&self) -> (Vec<u8>, Vec<u8>) {
        (self.cert_pem.clone(), self.key_pem.clone())
    }

    pub fn acceptor(&self) -> tokio_rustls::TlsAcceptor {
        tokio_rustls::TlsAcceptor::from(self.config.clone())
    }
}

fn read_certs(pem: &[u8], what: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use rustls_pki_types::pem::PemObject;
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls::pki_types::CertificateDer::pem_slice_iter(pem)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
    if certs.is_empty() {
        return Err(RymeError::InvalidArgument(String::from(what)));
    }
    Ok(certs)
}

fn tls_private_key(key_pem: &[u8]) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    use rustls_pki_types::pem::PemObject;
    rustls::pki_types::PrivateKeyDer::pem_slice_iter(key_pem)
        .next()
        .unwrap_or(Err(rustls_pki_types::pem::Error::NoItemsFound))
        .map_err(|e| RymeError::InvalidArgument(e.to_string()))
}

#[derive(Debug, Clone)]
pub struct MeshConnector {
    config: Arc<rustls::ClientConfig>,
}

impl MeshConnector {
    pub fn from_pem_bytes(
        ca_pem: &[u8],
        client_cert_pem: Option<&[u8]>,
        client_key_pem: Option<&[u8]>,
    ) -> Result<Self> {
        use rustls_pki_types::pem::PemObject;
        let cas: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls::pki_types::CertificateDer::pem_slice_iter(ca_pem)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
        if cas.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("tls client ca")));
        }
        let mut roots = rustls::RootCertStore::empty();
        for raw in cas {
            roots.add(raw).map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
        }
        let verifier = rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
        let builder =
            rustls::ClientConfig::builder().dangerous().with_custom_certificate_verifier(verifier);
        let config = match (client_cert_pem, client_key_pem) {
            (Some(cert_pem), Some(key_pem)) => {
                let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
                    rustls::pki_types::CertificateDer::pem_slice_iter(cert_pem)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
                if certs.is_empty() {
                    return Err(RymeError::InvalidArgument(String::from("tls cert")));
                }
                let key = tls_private_key(key_pem)?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| RymeError::InvalidArgument(e.to_string()))?
            }
            (None, None) => builder.with_no_client_auth(),
            _ => return Err(RymeError::InvalidArgument(String::from("tls cert/key"))),
        };
        Ok(Self { config: Arc::new(config) })
    }

    pub fn from_pem_files(
        ca_path: &std::path::Path,
        client_cert_path: Option<&std::path::Path>,
        client_key_path: Option<&std::path::Path>,
    ) -> Result<Self> {
        let ca_raw = std::fs::read(ca_path)?;
        let cert_raw = match client_cert_path {
            Some(path) => Some(std::fs::read(path)?),
            None => None,
        };
        let key_raw = match client_key_path {
            Some(path) => Some(std::fs::read(path)?),
            None => None,
        };
        Self::from_pem_bytes(&ca_raw, cert_raw.as_deref(), key_raw.as_deref())
    }

    pub fn connector(&self) -> tokio_rustls::TlsConnector {
        tokio_rustls::TlsConnector::from(self.config.clone())
    }

    pub async fn connect(
        &self,
        addr: &str,
        server_name: &str,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let stream =
            tokio::net::TcpStream::connect(addr).await.map_err(|e| RymeError::Io(e.to_string()))?;
        let _ = stream.set_nodelay(true);
        let name = rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|_| RymeError::InvalidArgument(String::from("server name")))?;
        self.connector().connect(name, stream).await.map_err(|e| RymeError::Io(e.to_string()))
    }
}

pub fn server_name_for(addr: &str) -> String {
    if let Some(rest) = addr.strip_prefix('[') {
        return rest.split(']').next().unwrap_or("localhost").to_string();
    }
    addr.rsplit(':').nth(1).unwrap_or(addr).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_name_parsing() {
        assert_eq!(server_name_for("127.0.0.1:9000"), "127.0.0.1");
        assert_eq!(server_name_for("node-0:9000"), "node-0");
        assert_eq!(server_name_for("[::1]:9000"), "::1");
    }

    #[test]
    fn acceptor_rejects_empty() {
        assert!(TlsAcceptor::from_pem_bytes(b"junk", b"junk").is_err());
        assert!(MeshConnector::from_pem_bytes(b"junk", None, None).is_err());
    }
}
