use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dek {
    pub id: String,
    pub key: Vec<u8>,
    pub created_unix: u64,
    pub retired: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub dek_id: String,
    pub nonce_b64: String,
    pub blob_b64: String,
    pub tag_b64: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct KeyRing {
    keys: HashMap<String, Vec<Dek>>,
    counter: u64,
}

impl KeyRing {
    pub fn new() -> Self {
        Self { keys: HashMap::new(), counter: 0 }
    }

    pub fn rotate(&mut self, tenant: &str, now_unix: u64) -> Dek {
        self.counter += 1;
        let dek = Dek {
            id: format!("dek-{}-{}", now_unix, self.counter),
            key: ryme_auth::random_bytes(32),
            created_unix: now_unix,
            retired: false,
        };
        let entry = self.keys.entry(tenant.to_string()).or_default();
        for old in entry.iter_mut() {
            old.retired = true;
        }
        entry.push(dek.clone());
        while entry.len() > 3 {
            entry.remove(0);
        }
        dek
    }

    pub fn active(&self, tenant: &str) -> Option<Dek> {
        self.keys.get(tenant)?.iter().rev().find(|dek| !dek.retired).cloned()
    }

    pub fn get(&self, tenant: &str, dek_id: &str) -> Option<Dek> {
        self.keys.get(tenant)?.iter().find(|dek| dek.id == dek_id).cloned()
    }

    pub fn seal(&self, tenant: &str, plaintext: &[u8]) -> Result<Envelope> {
        let dek = self.active(tenant).ok_or(RymeError::NotFound(String::from("dek")))?;
        Ok(seal_with(&dek, plaintext))
    }

    pub fn open(&self, tenant: &str, envelope: &Envelope) -> Result<Vec<u8>> {
        let dek = self.get(tenant, &envelope.dek_id).ok_or(RymeError::Unauthorized)?;
        open_with(&dek, envelope)
    }

    pub fn export_wrapped<K: KmsProvider>(&self, tenant: &str, kms: &K) -> Result<Vec<WrappedDek>> {
        let Some(deks) = self.keys.get(tenant) else {
            return Err(RymeError::NotFound(String::from("dek")));
        };
        deks.iter()
            .map(|dek| {
                let envelope = kms.wrap(&dek.key)?;
                Ok(WrappedDek {
                    tenant: tenant.to_string(),
                    dek_id: dek.id.clone(),
                    created_unix: dek.created_unix,
                    retired: dek.retired,
                    kek_id: envelope.kek_id,
                    nonce_b64: envelope.nonce_b64,
                    blob_b64: envelope.blob_b64,
                    tag_b64: envelope.tag_b64,
                })
            })
            .collect()
    }

    pub fn import_wrapped<K: KmsProvider>(
        &mut self,
        wrapped: Vec<WrappedDek>,
        kms: &K,
    ) -> Result<()> {
        for entry in wrapped {
            let key = kms.unwrap(&WrappedDekRef {
                kek_id: entry.kek_id.clone(),
                nonce_b64: entry.nonce_b64.clone(),
                blob_b64: entry.blob_b64.clone(),
                tag_b64: entry.tag_b64.clone(),
            })?;
            let tenant_keys = self.keys.entry(entry.tenant.clone()).or_default();
            if !tenant_keys.iter().any(|dek| dek.id == entry.dek_id) {
                tenant_keys.push(Dek {
                    id: entry.dek_id,
                    key,
                    created_unix: entry.created_unix,
                    retired: entry.retired,
                });
            }
        }
        Ok(())
    }

    pub fn tenants(&self) -> Vec<String> {
        let mut out: Vec<String> = self.keys.keys().cloned().collect();
        out.sort();
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WrappedDek {
    pub tenant: String,
    pub dek_id: String,
    pub created_unix: u64,
    pub retired: bool,
    pub kek_id: String,
    pub nonce_b64: String,
    pub blob_b64: String,
    pub tag_b64: String,
}

#[derive(Debug, Clone)]
pub struct WrappedDekRef {
    pub kek_id: String,
    pub nonce_b64: String,
    pub blob_b64: String,
    pub tag_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsEnvelope {
    pub kek_id: String,
    pub nonce_b64: String,
    pub blob_b64: String,
    pub tag_b64: String,
}

pub trait KmsProvider {
    fn wrap(&self, dek: &[u8]) -> Result<KmsEnvelope>;
    fn unwrap(&self, envelope: &WrappedDekRef) -> Result<Vec<u8>>;
    fn kek_id(&self) -> String;
}

#[derive(Debug, Clone)]
pub struct EnvKms {
    kek_id: String,
    kek: Vec<u8>,
}

impl EnvKms {
    pub fn from_env(var: &str, kek_id: String) -> Result<Self> {
        let raw =
            std::env::var(var).map_err(|_| RymeError::InvalidArgument(String::from("kms env")))?;
        let key = ryme_auth::base64_url_decode(&raw).unwrap_or_else(|_| raw.into_bytes());
        if key.len() < 16 {
            return Err(RymeError::InvalidArgument(String::from("kms key")));
        }
        Ok(Self { kek_id, kek: key })
    }

    pub fn from_key(kek_id: String, kek: Vec<u8>) -> Result<Self> {
        if kek.len() < 16 {
            return Err(RymeError::InvalidArgument(String::from("kms key")));
        }
        Ok(Self { kek_id, kek })
    }
}

impl KmsProvider for EnvKms {
    fn wrap(&self, dek: &[u8]) -> Result<KmsEnvelope> {
        let nonce = ryme_auth::random_bytes(12);
        let stream = keystream(&self.kek, &nonce, dek.len());
        let blob: Vec<u8> = dek.iter().zip(stream.iter()).map(|(p, s)| p ^ s).collect();
        let tag = auth_tag(&self.kek, &nonce, &blob);
        Ok(KmsEnvelope {
            kek_id: self.kek_id.clone(),
            nonce_b64: ryme_auth::base64_url_encode(&nonce),
            blob_b64: ryme_auth::base64_url_encode(&blob),
            tag_b64: ryme_auth::base64_url_encode(&tag),
        })
    }

    fn unwrap(&self, envelope: &WrappedDekRef) -> Result<Vec<u8>> {
        let nonce = ryme_auth::base64_url_decode(&envelope.nonce_b64)
            .map_err(|_| RymeError::Unauthorized)?;
        let blob = ryme_auth::base64_url_decode(&envelope.blob_b64)
            .map_err(|_| RymeError::Unauthorized)?;
        let tag =
            ryme_auth::base64_url_decode(&envelope.tag_b64).map_err(|_| RymeError::Unauthorized)?;
        if !equal(&auth_tag(&self.kek, &nonce, &blob), &tag) {
            return Err(RymeError::Unauthorized);
        }
        let stream = keystream(&self.kek, &nonce, blob.len());
        Ok(blob.iter().zip(stream.iter()).map(|(b, s)| b ^ s).collect())
    }

    fn kek_id(&self) -> String {
        self.kek_id.clone()
    }
}

pub fn seal_with(dek: &Dek, plaintext: &[u8]) -> Envelope {
    let nonce = ryme_auth::random_bytes(12);
    let stream = keystream(&dek.key, &nonce, plaintext.len());
    let blob: Vec<u8> = plaintext.iter().zip(stream.iter()).map(|(p, s)| p ^ s).collect();
    let tag = auth_tag(&dek.key, &nonce, &blob);
    Envelope {
        dek_id: dek.id.clone(),
        nonce_b64: ryme_auth::base64_url_encode(&nonce),
        blob_b64: ryme_auth::base64_url_encode(&blob),
        tag_b64: ryme_auth::base64_url_encode(&tag),
    }
}

pub fn open_with(dek: &Dek, envelope: &Envelope) -> Result<Vec<u8>> {
    let nonce =
        ryme_auth::base64_url_decode(&envelope.nonce_b64).map_err(|_| RymeError::Unauthorized)?;
    let blob =
        ryme_auth::base64_url_decode(&envelope.blob_b64).map_err(|_| RymeError::Unauthorized)?;
    let tag =
        ryme_auth::base64_url_decode(&envelope.tag_b64).map_err(|_| RymeError::Unauthorized)?;
    let expected = auth_tag(&dek.key, &nonce, &blob);
    if !equal(&expected, &tag) {
        return Err(RymeError::Unauthorized);
    }
    let stream = keystream(&dek.key, &nonce, blob.len());
    Ok(blob.iter().zip(stream.iter()).map(|(b, s)| b ^ s).collect())
}

fn keystream(key: &[u8], nonce: &[u8], len: usize) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut out = Vec::with_capacity(len);
    let mut counter = 0u64;
    while out.len() < len {
        let mut mac = Hmac::<Sha256>::new_from_slice(key)
            .unwrap_or_else(|_| Hmac::new_from_slice(&[0]).unwrap());
        mac.update(nonce);
        mac.update(&counter.to_be_bytes());
        out.extend_from_slice(&mac.finalize().into_bytes());
        counter += 1;
    }
    out.truncate(len);
    out
}

fn auth_tag(key: &[u8], nonce: &[u8], blob: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key).unwrap_or_else(|_| Hmac::new_from_slice(&[0]).unwrap());
    mac.update(b"ryme-envelope-v1");
    mac.update(nonce);
    mac.update(blob);
    mac.finalize().into_bytes().to_vec()
}

fn equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for index in 0..left.len() {
        diff |= left[index] ^ right[index];
    }
    diff == 0
}

pub fn base64_encode(raw: &[u8]) -> String {
    ryme_auth::base64_url_encode(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_roundtrip() {
        let dek = Dek {
            id: String::from("dek-1"),
            key: vec![7u8; 32],
            created_unix: 1000,
            retired: false,
        };
        let envelope = seal_with(&dek, b"snapshot bytes");
        let plain = open_with(&dek, &envelope).unwrap();
        assert_eq!(plain, b"snapshot bytes".to_vec());
    }

    #[test]
    fn tamper_rejected() {
        let dek = Dek {
            id: String::from("dek-1"),
            key: vec![9u8; 32],
            created_unix: 1000,
            retired: false,
        };
        let mut envelope = seal_with(&dek, b"data");
        envelope.blob_b64 = base64_encode(b"tampered-tampered-tampered-tampered!!");
        assert!(open_with(&dek, &envelope).is_err());
    }

    #[test]
    fn rotation_keeps_old_readable() {
        let mut ring = KeyRing::new();
        ring.rotate("t", 1000);
        let first = ring.seal("t", b"v1").unwrap();
        ring.rotate("t", 2000);
        assert!(ring.open("t", &first).is_ok());
        let second = ring.seal("t", b"v2").unwrap();
        assert_ne!(first.blob_b64, second.blob_b64);
        assert!(ring.open("t", &second).is_ok());
    }

    #[test]
    fn unknown_tenant_fails() {
        let ring = KeyRing::new();
        assert!(ring.seal("ghost", b"x").is_err());
    }

    #[test]
    fn kms_export_import_roundtrip() {
        let kms = EnvKms::from_key(String::from("kek-1"), vec![3u8; 32]).unwrap();
        assert_eq!(kms.kek_id(), "kek-1");
        let mut ring = KeyRing::new();
        ring.rotate("t", 1000);
        let exported = ring.export_wrapped("t", &kms).unwrap();
        assert_eq!(exported.len(), 1);
        let mut restored = KeyRing::new();
        restored.import_wrapped(exported, &kms).unwrap();
        let envelope = ring.seal("t", b"secret").unwrap();
        let plain = restored.open("t", &envelope).unwrap();
        assert_eq!(plain, b"secret".to_vec());
        assert_eq!(restored.tenants(), vec![String::from("t")]);
    }

    #[test]
    fn kms_rejects_short_key_and_tamper() {
        assert!(EnvKms::from_key(String::from("k"), vec![1u8; 8]).is_err());
        let kms = EnvKms::from_key(String::from("kek-1"), vec![3u8; 32]).unwrap();
        let envelope = kms.wrap(b"dek-material-32-bytes-padded!!!!").unwrap();
        let good = kms
            .unwrap(&WrappedDekRef {
                kek_id: envelope.kek_id.clone(),
                nonce_b64: envelope.nonce_b64.clone(),
                blob_b64: envelope.blob_b64.clone(),
                tag_b64: envelope.tag_b64.clone(),
            })
            .unwrap();
        assert_eq!(good, b"dek-material-32-bytes-padded!!!!".to_vec());
        let bad = kms.unwrap(&WrappedDekRef {
            kek_id: envelope.kek_id,
            nonce_b64: envelope.nonce_b64,
            blob_b64: base64_encode(b"tampered"),
            tag_b64: envelope.tag_b64,
        });
        assert!(bad.is_err());
    }
}
