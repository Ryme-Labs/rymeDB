use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Owner,
    Admin,
    Developer,
    ReadWrite,
    ReadOnly,
    RealtimePublisher,
    RealtimeSubscriber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    pub id: String,
    pub tenant: String,
    pub roles: HashSet<Role>,
}

impl Principal {
    pub fn can_write(&self) -> bool {
        self.roles.contains(&Role::Owner)
            || self.roles.contains(&Role::Admin)
            || self.roles.contains(&Role::Developer)
            || self.roles.contains(&Role::ReadWrite)
    }

    pub fn can_admin(&self) -> bool {
        self.roles.contains(&Role::Owner) || self.roles.contains(&Role::Admin)
    }

    pub fn can_read(&self) -> bool {
        self.can_write()
            || self.roles.contains(&Role::ReadOnly)
            || self.roles.contains(&Role::RealtimeSubscriber)
    }

    pub fn can_publish(&self) -> bool {
        self.can_write() || self.roles.contains(&Role::RealtimePublisher)
    }
}

#[derive(Debug, Default)]
pub struct PolicyEngine {
    table_policies: HashMap<String, String>,
    masked_fields: std::sync::RwLock<HashMap<String, Vec<String>>>,
}

impl PolicyEngine {
    pub fn new() -> Self {
        Self {
            table_policies: HashMap::new(),
            masked_fields: std::sync::RwLock::new(HashMap::new()),
        }
    }

    pub fn allow_table(&mut self, table: String, tenant_column: String) {
        self.table_policies.insert(table, tenant_column);
    }

    pub fn has_table_policy(&self, table: &str) -> bool {
        self.table_policies.contains_key(table)
    }

    pub fn mask_fields(&self, table: String, fields: Vec<String>) {
        if let Ok(mut guard) = self.masked_fields.write() {
            guard.insert(table, fields);
        }
    }

    pub fn masked_value(&self, table: &str, value: &[u8]) -> Vec<u8> {
        let fields = match self.masked_fields.read() {
            Ok(guard) => guard.get(table).cloned(),
            Err(_) => None,
        };
        let Some(fields) = fields else {
            return value.to_vec();
        };
        let Ok(mut parsed) = serde_json::from_slice::<serde_json::Value>(value) else {
            return value.to_vec();
        };
        let Some(obj) = parsed.as_object_mut() else {
            return value.to_vec();
        };
        for field in fields {
            if obj.contains_key(field.as_str()) {
                obj.insert(field.clone(), serde_json::Value::String(String::from("***")));
            }
        }
        serde_json::to_vec(&parsed).unwrap_or_else(|_| value.to_vec())
    }

    pub fn predicate(&self, principal: &Principal, table: &str) -> Result<String> {
        if !principal.can_read() {
            return Err(RymeError::Forbidden);
        }
        match self.table_policies.get(table) {
            Some(column) => {
                let tenant = principal.tenant.replace('\'', "''");
                Ok(format!("{column} = '{tenant}'"))
            }
            None => Ok(String::from("true")),
        }
    }

    pub fn row_allowed(&self, principal: &Principal, table: &str, value: &[u8]) -> Result<bool> {
        self.predicate(principal, table)?;
        let Some(column) = self.table_policies.get(table) else {
            return Ok(true);
        };
        let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(value) else {
            return Ok(false);
        };
        Ok(object
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .and_then(|(_, value)| value.as_str())
            .is_some_and(|tenant| tenant == principal.tenant))
    }

    pub fn check_write_row(&self, principal: &Principal, table: &str, value: &[u8]) -> Result<()> {
        if !principal.can_write() {
            return Err(RymeError::Forbidden);
        }
        if self.row_allowed(principal, table, value)? {
            Ok(())
        } else {
            Err(RymeError::Forbidden)
        }
    }

    pub fn check_write(&self, principal: &Principal, _table: &str) -> Result<()> {
        if principal.can_write() {
            Ok(())
        } else {
            Err(RymeError::Forbidden)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApiKeyStore {
    inner: Arc<RwLock<HashMap<String, Principal>>>,
}

impl ApiKeyStore {
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn insert(&self, api_key: String, principal: Principal) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(Self::digest(&api_key), principal);
        }
    }

    pub fn remove(&self, api_key: &str) -> bool {
        self.inner
            .write()
            .map(|mut guard| guard.remove(&Self::digest(api_key)).is_some())
            .unwrap_or(false)
    }

    pub fn owner_of(&self, presented: &str) -> Option<Principal> {
        let guard = self.inner.read().ok()?;
        guard.get(&Self::digest(presented)).cloned()
    }

    pub fn authenticate(&self, presented: &str) -> Result<Principal> {
        let guard =
            self.inner.read().map_err(|_| RymeError::Internal(String::from("auth lock")))?;
        guard.get(&Self::digest(presented)).cloned().ok_or(RymeError::Unauthorized)
    }

    pub fn snapshot(&self) -> Result<HashMap<String, Principal>> {
        self.inner
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| RymeError::Internal(String::from("auth lock")))
    }

    pub fn from_snapshot(snapshot: HashMap<String, Principal>) -> Self {
        Self { inner: Arc::new(RwLock::new(snapshot)) }
    }

    fn digest(api_key: &str) -> String {
        use sha2::{Digest, Sha256};
        base64_url_encode(&Sha256::digest(api_key.as_bytes()))
    }
}

impl Default for ApiKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for index in 0..left.len() {
        diff |= left[index] ^ right[index];
    }
    diff == 0
}

#[derive(Debug, Clone)]
pub struct JwtVerifier {
    secret: Vec<u8>,
    leeway_secs: u64,
    issuer: Option<String>,
    audience: Option<String>,
}

impl JwtVerifier {
    pub fn new(secret: Vec<u8>) -> Self {
        Self { secret, leeway_secs: 60, issuer: None, audience: None }
    }

    pub fn with_issuer(secret: Vec<u8>, issuer: String, audience: String) -> Self {
        Self { secret, leeway_secs: 60, issuer: Some(issuer), audience: Some(audience) }
    }

    pub fn principal_from_token(&self, token: &str, now_secs: u64) -> Result<Principal> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let mut parts = token.split('.');
        let header_b64 = parts.next().ok_or(RymeError::Unauthorized)?;
        let payload_b64 = parts.next().ok_or(RymeError::Unauthorized)?;
        let sig_b64 = parts.next().ok_or(RymeError::Unauthorized)?;
        if parts.next().is_some() {
            return Err(RymeError::Unauthorized);
        }
        let signing_input = format!("{header_b64}.{payload_b64}");
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.secret).map_err(|_| RymeError::Unauthorized)?;
        mac.update(signing_input.as_bytes());
        let expected = mac.finalize().into_bytes();
        let presented = base64_url_decode(sig_b64).map_err(|_| RymeError::Unauthorized)?;
        if !constant_time_equal(&expected, &presented) {
            return Err(RymeError::Unauthorized);
        }
        let payload_raw = base64_url_decode(payload_b64).map_err(|_| RymeError::Unauthorized)?;
        let claims: serde_json::Value =
            serde_json::from_slice(&payload_raw).map_err(|_| RymeError::Unauthorized)?;
        let exp = claims.get("exp").and_then(|v| v.as_u64()).unwrap_or(0);
        if exp + self.leeway_secs < now_secs {
            return Err(RymeError::Unauthorized);
        }
        if let Some(issuer) = self.issuer.as_ref() {
            let got = claims.get("iss").and_then(|v| v.as_str()).unwrap_or("");
            if !constant_time_equal(got.as_bytes(), issuer.as_bytes()) {
                return Err(RymeError::Unauthorized);
            }
        }
        if let Some(audience) = self.audience.as_ref() {
            let valid = match claims.get("aud") {
                Some(serde_json::Value::String(single)) => {
                    constant_time_equal(single.as_bytes(), audience.as_bytes())
                }
                Some(serde_json::Value::Array(list)) => list.iter().any(|entry| {
                    entry.as_str().is_some_and(|name| {
                        constant_time_equal(name.as_bytes(), audience.as_bytes())
                    })
                }),
                _ => audience.is_empty(),
            };
            if !valid {
                return Err(RymeError::Unauthorized);
            }
        }
        let id = claims.get("sub").and_then(|v| v.as_str()).unwrap_or("jwt").to_string();
        let tenant = claims.get("tenant").and_then(|v| v.as_str()).unwrap_or("default").to_string();
        let mut roles = HashSet::new();
        if let Some(list) = claims.get("roles").and_then(|v| v.as_array()) {
            for entry in list {
                if let Some(name) = entry.as_str() {
                    if let Some(role) = parse_role(name) {
                        roles.insert(role);
                    }
                }
            }
        }
        if roles.is_empty() {
            roles.insert(Role::ReadWrite);
        }
        Ok(Principal { id, tenant, roles })
    }
}

fn parse_role(name: &str) -> Option<Role> {
    match name {
        "owner" => Some(Role::Owner),
        "admin" => Some(Role::Admin),
        "developer" => Some(Role::Developer),
        "readwrite" => Some(Role::ReadWrite),
        "readonly" => Some(Role::ReadOnly),
        "realtime_publisher" => Some(Role::RealtimePublisher),
        "realtime_subscriber" => Some(Role::RealtimeSubscriber),
        _ => None,
    }
}

pub fn base64_url_decode(input: &str) -> std::result::Result<Vec<u8>, String> {
    use base64::Engine;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    engine.decode(input).map_err(|e| e.to_string())
}

pub fn base64_url_encode(raw: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

pub fn random_bytes(count: usize) -> Vec<u8> {
    let mut out = vec![0u8; count];
    let mut filled = 0usize;
    if let Ok(mut file) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let mut remaining = &mut out[..];
        while !remaining.is_empty() {
            match file.read(remaining) {
                Ok(0) => break,
                Ok(read) => {
                    filled += read;
                    remaining = &mut out[filled..];
                }
                Err(_) => break,
            }
        }
    }
    if filled < count {
        let mut state = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9e3779b97f4a7c15);
        if state == 0 {
            state = 0x9e3779b97f4a7c15;
        }
        let mut index = filled;
        while index < count {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            for byte in state.to_le_bytes() {
                if index >= count {
                    break;
                }
                out[index] = out[index].wrapping_add(byte);
                index += 1;
            }
        }
    }
    out
}

pub const REFRESH_TOKEN_TTL_SECS: u64 = 30 * 24 * 60 * 60;
const MAX_REFRESH_SESSIONS: usize = 100_000;

#[derive(Debug, Clone)]
pub struct RefreshTokenStore {
    inner: Arc<RwLock<HashMap<String, RefreshTokenRecord>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshTokenRecord {
    pub principal: Principal,
    pub expires_at: u64,
}

impl RefreshTokenStore {
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn issue(&self, principal: Principal, now_secs: u64) -> Result<String> {
        let raw = base64_url_encode(&random_bytes(32));
        let digest = Self::digest(&raw);
        let mut sessions =
            self.inner.write().map_err(|_| RymeError::Internal(String::from("auth lock")))?;
        sessions.retain(|_, session| session.expires_at > now_secs);
        if sessions.len() >= MAX_REFRESH_SESSIONS {
            return Err(RymeError::Overload(String::from("refresh sessions")));
        }
        sessions.insert(
            digest,
            RefreshTokenRecord {
                principal,
                expires_at: now_secs.saturating_add(REFRESH_TOKEN_TTL_SECS),
            },
        );
        Ok(raw)
    }

    pub fn rotate(&self, presented: &str, now_secs: u64) -> Result<(Principal, String)> {
        if presented.is_empty() {
            return Err(RymeError::Unauthorized);
        }
        let digest = Self::digest(presented);
        let mut sessions =
            self.inner.write().map_err(|_| RymeError::Internal(String::from("auth lock")))?;
        let session = sessions.remove(&digest).ok_or(RymeError::Unauthorized)?;
        if session.expires_at <= now_secs {
            return Err(RymeError::Unauthorized);
        }
        let raw = base64_url_encode(&random_bytes(32));
        let next_digest = Self::digest(&raw);
        sessions.insert(
            next_digest,
            RefreshTokenRecord {
                principal: session.principal.clone(),
                expires_at: now_secs.saturating_add(REFRESH_TOKEN_TTL_SECS),
            },
        );
        Ok((session.principal, raw))
    }

    pub fn revoke(&self, presented: &str) -> bool {
        self.inner
            .write()
            .map(|mut sessions| sessions.remove(&Self::digest(presented)).is_some())
            .unwrap_or(false)
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|sessions| sessions.len()).unwrap_or(0)
    }

    pub fn snapshot(&self) -> Result<HashMap<String, RefreshTokenRecord>> {
        self.inner
            .read()
            .map(|sessions| sessions.clone())
            .map_err(|_| RymeError::Internal(String::from("auth lock")))
    }

    pub fn from_snapshot(snapshot: HashMap<String, RefreshTokenRecord>) -> Self {
        Self { inner: Arc::new(RwLock::new(snapshot)) }
    }

    fn digest(token: &str) -> String {
        use sha2::{Digest, Sha256};
        base64_url_encode(&Sha256::digest(token.as_bytes()))
    }
}

impl Default for RefreshTokenStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordHash {
    pub salt_b64: String,
    pub hash_b64: String,
    pub rounds: u32,
}

pub const PASSWORD_ROUNDS: u32 = 210_000;
pub const MAX_PASSWORD_LEN: usize = 256;
pub const MAX_USER_LEN: usize = 256;
pub const MAX_CHALLENGES: usize = 4096;

pub fn hash_password(password: &str) -> PasswordHash {
    let salt = random_bytes(16);
    hash_password_with_salt(password, &salt, PASSWORD_ROUNDS)
}

pub fn hash_password_with_salt(password: &str, salt: &[u8], rounds: u32) -> PasswordHash {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(salt);
    digest.update(password.as_bytes());
    let mut out = digest.finalize().to_vec();
    for _ in 1..rounds.max(1) {
        let mut next = Sha256::new();
        next.update(&out);
        next.update(salt);
        out = next.finalize().to_vec();
    }
    PasswordHash { salt_b64: base64_url_encode(salt), hash_b64: base64_url_encode(&out), rounds }
}

pub fn verify_password(password: &str, stored: &PasswordHash) -> bool {
    let Ok(salt) = base64_url_decode(&stored.salt_b64) else {
        return false;
    };
    let candidate = hash_password_with_salt(password, &salt, stored.rounds);
    constant_time_equal(candidate.hash_b64.as_bytes(), stored.hash_b64.as_bytes())
}

pub fn totp_code(secret: &[u8], unix_secs: u64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let step = unix_secs / 30;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret)
        .unwrap_or_else(|_| Hmac::new_from_slice(&[0]).unwrap());
    mac.update(&step.to_be_bytes());
    let digest = mac.finalize().into_bytes();
    let offset = (digest[digest.len() - 1] & 0x0f) as usize;
    let code = ((digest[offset] as u32 & 0x7f) << 24)
        | ((digest[offset + 1] as u32) << 16)
        | ((digest[offset + 2] as u32) << 8)
        | (digest[offset + 3] as u32);
    format!("{:06}", code % 1_000_000)
}

pub fn verify_totp(secret: &[u8], code: &str, unix_secs: u64) -> bool {
    for delta in [0u64, 30, 60] {
        let base = unix_secs.saturating_sub(delta);
        if constant_time_equal(totp_code(secret, base).as_bytes(), code.as_bytes()) {
            return true;
        }
        if constant_time_equal(totp_code(secret, base + delta).as_bytes(), code.as_bytes()) {
            return true;
        }
    }
    false
}

#[derive(Debug, Default)]
pub struct PasskeyRegistry {
    challenges: HashMap<String, (Vec<u8>, u64)>,
    credentials: HashMap<String, PasskeyCredential>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasskeyCredential {
    pub user: String,
    pub public_key: Vec<u8>,
    pub sign_count: u32,
}

pub const PASSKEY_CHALLENGE_TTL_SECS: u64 = 300;

#[derive(Debug, Clone)]
pub struct PasskeyAssertion<'a> {
    pub user: &'a str,
    pub credential_id: &'a str,
    pub authenticator_data: &'a [u8],
    pub client_data_json: &'a [u8],
    pub signature_der: &'a [u8],
    pub rp_id: &'a str,
    pub origins: &'a [String],
    pub now_secs: u64,
}
impl PasskeyRegistry {
    pub fn new() -> Self {
        Self { challenges: HashMap::new(), credentials: HashMap::new() }
    }

    pub fn challenge(&mut self, user: &str) -> Result<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.challenge_at(user, now)
    }

    pub fn challenge_at(&mut self, user: &str, now_secs: u64) -> Result<String> {
        if user.is_empty() || user.len() > MAX_USER_LEN {
            return Err(RymeError::InvalidArgument(String::from("user")));
        }
        self.challenges.retain(|_, (_, issued_at)| {
            issued_at.saturating_add(PASSKEY_CHALLENGE_TTL_SECS) >= now_secs
        });
        if self.challenges.len() >= MAX_CHALLENGES && !self.challenges.contains_key(user) {
            return Err(RymeError::Overload(String::from("challenges")));
        }
        let raw = random_bytes(32);
        let encoded = base64_url_encode(&raw);
        self.challenges.insert(user.to_string(), (raw, now_secs));
        Ok(encoded)
    }

    pub fn register(&mut self, user: &str, credential_id: String, public_key: &[u8]) -> Result<()> {
        use p256::elliptic_curve::sec1::FromSec1Point;
        if user.is_empty() || user.len() > MAX_USER_LEN {
            return Err(RymeError::InvalidArgument(String::from("user")));
        }
        if credential_id.is_empty() || credential_id.len() > 256 {
            return Err(RymeError::InvalidArgument(String::from("credential")));
        }
        if public_key.len() > 128 {
            return Err(RymeError::InvalidArgument(String::from("public key")));
        }
        if self.credentials.contains_key(&credential_id) {
            return Err(RymeError::Conflict(String::from("credential")));
        }
        p256::AffinePoint::from_sec1_bytes(public_key)
            .map_err(|_| RymeError::InvalidArgument(String::from("public key")))?;
        self.credentials.insert(
            credential_id,
            PasskeyCredential {
                user: user.to_string(),
                public_key: public_key.to_vec(),
                sign_count: 0,
            },
        );
        Ok(())
    }

    pub fn owner_of(&self, credential_id: &str) -> Option<String> {
        self.credentials.get(credential_id).map(|credential| credential.user.clone())
    }

    pub fn snapshot(&self) -> HashMap<String, PasskeyCredential> {
        self.credentials.clone()
    }

    pub fn from_snapshot(credentials: HashMap<String, PasskeyCredential>) -> Self {
        Self { challenges: HashMap::new(), credentials }
    }

    pub fn verify_assertion(&mut self, assertion: &PasskeyAssertion<'_>) -> Result<()> {
        use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
        use sha2::{Digest, Sha256};
        let PasskeyAssertion {
            user,
            credential_id,
            authenticator_data,
            client_data_json,
            signature_der,
            rp_id,
            origins,
            now_secs,
        } = assertion;
        let user: &str = user;
        let credential_id: &str = credential_id;
        let rp_id: &str = rp_id;
        let now_secs: u64 = *now_secs;
        let (expected_challenge, issued_at) =
            self.challenges.get(user).ok_or(RymeError::Unauthorized)?;
        if issued_at.saturating_add(PASSKEY_CHALLENGE_TTL_SECS) < now_secs {
            self.challenges.remove(user);
            return Err(RymeError::Unauthorized);
        }
        let credential = self.credentials.get(credential_id).ok_or(RymeError::Unauthorized)?;
        if credential.user != user {
            return Err(RymeError::Unauthorized);
        }
        if authenticator_data.len() < 37 {
            return Err(RymeError::InvalidArgument(String::from("authenticator data")));
        }
        let mut hasher = Sha256::new();
        hasher.update(rp_id.as_bytes());
        if authenticator_data[..32] != hasher.finalize()[..] {
            return Err(RymeError::Unauthorized);
        }
        if authenticator_data[32] & 0x01 == 0 {
            return Err(RymeError::Unauthorized);
        }
        let presented_count = u32::from_be_bytes([
            authenticator_data[33],
            authenticator_data[34],
            authenticator_data[35],
            authenticator_data[36],
        ]);
        if presented_count != 0
            && credential.sign_count != 0
            && presented_count <= credential.sign_count
        {
            return Err(RymeError::Unauthorized);
        }
        let client_data: serde_json::Value =
            serde_json::from_slice(client_data_json).map_err(|_| RymeError::Unauthorized)?;
        if client_data.get("type").and_then(|v| v.as_str()) != Some("webauthn.get") {
            return Err(RymeError::Unauthorized);
        }
        let origin = client_data.get("origin").and_then(|v| v.as_str()).unwrap_or("");
        if !origins.iter().any(|allowed| allowed == origin) {
            return Err(RymeError::Unauthorized);
        }
        let presented = client_data.get("challenge").and_then(|v| v.as_str()).unwrap_or("");
        let want = base64_url_encode(expected_challenge.as_slice());
        if !constant_time_equal(presented.as_bytes(), want.as_bytes()) {
            return Err(RymeError::Unauthorized);
        }
        let verifying = VerifyingKey::from_sec1_bytes(&credential.public_key)
            .map_err(|_| RymeError::Unauthorized)?;
        let mut hasher = Sha256::new();
        hasher.update(client_data_json);
        let mut signed = authenticator_data.to_vec();
        signed.extend_from_slice(&hasher.finalize());
        let signature = Signature::from_der(signature_der).map_err(|_| RymeError::Unauthorized)?;
        verifying.verify(&signed, &signature).map_err(|_| RymeError::Unauthorized)?;
        self.challenges.remove(user);
        if let Some(credential) = self.credentials.get_mut(credential_id) {
            credential.sign_count = credential.sign_count.max(presented_count);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OidcConfig {
    pub issuer: String,
    pub audience: String,
    pub client_secret: Vec<u8>,
    pub auth_endpoint: String,
    pub client_id: String,
}

impl OidcConfig {
    pub fn auth_url(&self, redirect_uri: &str, state: &str) -> Result<String> {
        if self.auth_endpoint.is_empty() || redirect_uri.is_empty() || state.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("oidc login")));
        }
        Ok(format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&scope=openid%20email%20profile&state={}",
            self.auth_endpoint, self.client_id, redirect_uri, state
        ))
    }

    pub fn verify_id_token(&self, id_token: &str, now_secs: u64) -> Result<Principal> {
        let verifier = JwtVerifier::with_issuer(
            self.client_secret.clone(),
            self.issuer.clone(),
            self.audience.clone(),
        );
        verifier.principal_from_token(id_token, now_secs)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredUser {
    pub tenant: String,
    pub roles: HashSet<Role>,
    pub password: Option<PasswordHash>,
    pub otp_secret: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
pub struct CredentialStore {
    users: HashMap<String, StoredUser>,
}

impl CredentialStore {
    pub fn new() -> Self {
        Self { users: HashMap::new() }
    }

    pub fn register_password(
        &mut self,
        id: String,
        tenant: String,
        password: &str,
        roles: HashSet<Role>,
    ) -> Result<()> {
        if id.is_empty()
            || id.len() > MAX_USER_LEN
            || password.len() < 8
            || password.len() > MAX_PASSWORD_LEN
        {
            return Err(RymeError::InvalidArgument(String::from("credentials")));
        }
        if self.users.contains_key(&id) {
            return Err(RymeError::Conflict(String::from("user")));
        }
        self.users.insert(
            id,
            StoredUser { tenant, roles, password: Some(hash_password(password)), otp_secret: None },
        );
        Ok(())
    }

    pub fn verify_password(&self, id: &str, password: &str) -> Result<Principal> {
        if password.len() > MAX_PASSWORD_LEN {
            return Err(RymeError::InvalidArgument(String::from("credentials")));
        }
        let stored = self.users.get(id).and_then(|user| user.password.as_ref());
        let dummy;
        let reference: &PasswordHash = match stored {
            Some(hash) => hash,
            None => {
                dummy = PasswordHash {
                    salt_b64: base64_url_encode(&[0u8; 16]),
                    hash_b64: base64_url_encode(&[0u8; 32]),
                    rounds: PASSWORD_ROUNDS,
                };
                &dummy
            }
        };
        if !verify_password(password, reference) {
            return Err(RymeError::Unauthorized);
        }
        let user = self.users.get(id).ok_or(RymeError::Unauthorized)?;
        Ok(Principal { id: id.to_string(), tenant: user.tenant.clone(), roles: user.roles.clone() })
    }

    pub fn set_otp_secret(&mut self, id: &str, secret: Vec<u8>) -> Result<()> {
        if secret.len() < 16 {
            return Err(RymeError::InvalidArgument(String::from("otp secret")));
        }
        let user = self.users.get_mut(id).ok_or(RymeError::NotFound(String::from("user")))?;
        user.otp_secret = Some(secret);
        Ok(())
    }

    pub fn otp_enrolled(&self, id: &str) -> bool {
        self.users.get(id).and_then(|user| user.otp_secret.as_ref()).is_some()
    }

    pub fn principal(&self, id: &str) -> Result<Principal> {
        let user = self.users.get(id).ok_or(RymeError::Unauthorized)?;
        Ok(Principal { id: id.to_string(), tenant: user.tenant.clone(), roles: user.roles.clone() })
    }

    pub fn verify_otp(&self, id: &str, code: &str, now_secs: u64) -> Result<Principal> {
        let user = self.users.get(id).ok_or(RymeError::Unauthorized)?;
        let secret = user.otp_secret.as_ref().ok_or(RymeError::Unauthorized)?;
        if verify_totp(secret, code, now_secs) {
            Ok(Principal {
                id: id.to_string(),
                tenant: user.tenant.clone(),
                roles: user.roles.clone(),
            })
        } else {
            Err(RymeError::Unauthorized)
        }
    }

    pub fn len(&self) -> usize {
        self.users.len()
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    pub fn snapshot(&self) -> HashMap<String, StoredUser> {
        self.users.clone()
    }

    pub fn from_snapshot(users: HashMap<String, StoredUser>) -> Self {
        Self { users }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_roundtrip() {
        let store = ApiKeyStore::new();
        let mut roles = HashSet::new();
        roles.insert(Role::ReadWrite);
        store.insert(
            String::from("secret-key"),
            Principal { id: String::from("u"), tenant: String::from("t"), roles },
        );
        assert!(store.authenticate("secret-key").is_ok());
        assert!(store.authenticate("wrong").is_err());
        assert_eq!(store.owner_of("secret-key").map(|p| p.id), Some(String::from("u")));
        assert!(store.owner_of("wrong").is_none());
        assert!(!store.remove("missing"));
        assert!(store.remove("secret-key"));
        assert!(store.authenticate("secret-key").is_err());
        assert!(store.owner_of("secret-key").is_none());
    }

    #[test]
    fn refresh_tokens_rotate_once_and_expire() {
        let store = RefreshTokenStore::new();
        let mut roles = HashSet::new();
        roles.insert(Role::ReadWrite);
        let principal = Principal { id: String::from("u"), tenant: String::from("t"), roles };
        let first = store.issue(principal.clone(), 100).unwrap();
        let (rotated_principal, second) = store.rotate(&first, 101).unwrap();
        assert_eq!(rotated_principal.id, principal.id);
        assert_ne!(first, second);
        assert!(store.rotate(&first, 101).is_err());
        assert!(store.rotate(&second, 101 + REFRESH_TOKEN_TTL_SECS).is_err());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn jwt_reject_bad_signature() {
        let verifier = JwtVerifier::new(b"test-secret".to_vec());
        assert!(verifier.principal_from_token("a.b.c", 1000).is_err());
    }

    #[test]
    fn jwt_enforces_issuer() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let payload = serde_json::json!({
            "sub": "ada",
            "tenant": "t",
            "roles": ["readonly"],
            "exp": 2000000000u64,
            "iss": "https://auth.rymedb.test",
            "aud": "rymedb-app",
        });
        let payload_raw = serde_json::to_vec(&payload).unwrap();
        let header_b64 = base64_url_encode(br#"{"alg":"HS256"}"#);
        let payload_b64 = base64_url_encode(&payload_raw);
        let input = format!("{header_b64}.{payload_b64}");
        let mut mac = Hmac::<Sha256>::new_from_slice(b"issuer-secret").unwrap();
        mac.update(input.as_bytes());
        let sig_b64 = base64_url_encode(&mac.finalize().into_bytes());
        let token = format!("{input}.{sig_b64}");
        let strict = JwtVerifier::with_issuer(
            b"issuer-secret".to_vec(),
            String::from("https://auth.rymedb.test"),
            String::from("rymedb-app"),
        );
        let principal = strict.principal_from_token(&token, 1000).unwrap();
        assert_eq!(principal.id, "ada");
        assert!(principal.roles.contains(&Role::ReadOnly));
        let wrong_issuer = JwtVerifier::with_issuer(
            b"issuer-secret".to_vec(),
            String::from("https://other.test"),
            String::from("rymedb-app"),
        );
        assert!(wrong_issuer.principal_from_token(&token, 1000).is_err());
        let open = JwtVerifier::new(b"issuer-secret".to_vec());
        assert!(open.principal_from_token(&token, 1000).is_ok());
    }

    fn oidc_test_config() -> OidcConfig {
        OidcConfig {
            issuer: String::from("https://auth.rymedb.test"),
            audience: String::from("rymedb-app"),
            client_secret: b"issuer-secret".to_vec(),
            auth_endpoint: String::from("https://auth.rymedb.test/authorize"),
            client_id: String::from("rymedb-app"),
        }
    }

    fn oidc_test_token() -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let payload = serde_json::json!({
            "sub": "oidc-user",
            "tenant": "t",
            "roles": ["readonly"],
            "exp": 2000000000u64,
            "iss": "https://auth.rymedb.test",
            "aud": "rymedb-app",
        });
        let payload_raw = serde_json::to_vec(&payload).unwrap();
        let header_b64 = base64_url_encode(br#"{"alg":"HS256"}"#);
        let payload_b64 = base64_url_encode(&payload_raw);
        let input = format!("{header_b64}.{payload_b64}");
        let mut mac = Hmac::<Sha256>::new_from_slice(b"issuer-secret").unwrap();
        mac.update(input.as_bytes());
        let sig_b64 = base64_url_encode(&mac.finalize().into_bytes());
        format!("{input}.{sig_b64}")
    }

    #[test]
    fn oidc_verifies_id_token() {
        let config = oidc_test_config();
        let principal = config.verify_id_token(&oidc_test_token(), 1000).unwrap();
        assert_eq!(principal.id, "oidc-user");
        assert!(principal.roles.contains(&Role::ReadOnly));
        assert!(config.verify_id_token("bad.token.here", 1000).is_err());
    }

    #[test]
    fn oidc_builds_login_url() {
        let config = oidc_test_config();
        let url = config.auth_url("https://app.test/callback", "xyz").unwrap();
        assert!(url.contains("response_type=code"));
        assert!(url.contains("state=xyz"));
        assert!(config.auth_url("", "xyz").is_err());
    }

    #[test]
    fn password_roundtrip() {
        let stored = hash_password_with_salt("hunter2", b"fixed-salt-123456", 2);
        assert!(verify_password("hunter2", &stored));
        assert!(!verify_password("wrong", &stored));
    }

    #[test]
    fn totp_window() {
        let secret = b"totp-secret";
        let code = totp_code(secret, 1_700_000_000);
        assert!(verify_totp(secret, &code, 1_700_000_000));
        assert!(!verify_totp(secret, "000000", 1_700_000_000));
    }

    #[test]
    fn mask_redacts_fields() {
        let engine = PolicyEngine::new();
        engine.mask_fields(String::from("users"), vec![String::from("ssn")]);
        let masked = engine.masked_value("users", b"{\"name\":\"ada\",\"ssn\":\"123\"}");
        let text = String::from_utf8(masked).unwrap();
        assert!(text.contains("***"));
        assert!(!text.contains("123"));
        let plain = engine.masked_value("other", b"{\"ssn\":\"123\"}");
        assert_eq!(plain, b"{\"ssn\":\"123\"}".to_vec());
    }

    #[test]
    fn tenant_policy_matches_rows_before_returning_them() {
        let mut engine = PolicyEngine::new();
        engine.allow_table(String::from("messages"), String::from("tenant_id"));
        let principal = Principal {
            id: String::from("ada"),
            tenant: String::from("tenant-a"),
            roles: [Role::ReadWrite].into_iter().collect(),
        };
        assert!(engine
            .row_allowed(&principal, "messages", br#"{"tenant_id":"tenant-a","body":"hi"}"#)
            .unwrap());
        assert!(!engine
            .row_allowed(&principal, "messages", br#"{"tenant_id":"tenant-b","body":"secret"}"#)
            .unwrap());
        assert!(!engine.row_allowed(&principal, "messages", br#"{"body":"missing"}"#).unwrap());
        assert!(engine
            .check_write_row(&principal, "messages", br#"{"tenant_id":"tenant-a"}"#)
            .is_ok());
        assert!(matches!(
            engine.check_write_row(&principal, "messages", br#"{"tenant_id":"tenant-b"}"#),
            Err(RymeError::Forbidden)
        ));
        assert_eq!(
            engine
                .predicate(
                    &Principal { tenant: String::from("a'b"), ..principal.clone() },
                    "messages"
                )
                .unwrap(),
            "tenant_id = 'a''b'"
        );
    }

    #[test]
    fn passkey_challenge_registers() {
        let mut registry = PasskeyRegistry::new();
        let challenge = registry.challenge("ada").unwrap();
        assert!(!challenge.is_empty());
        assert!(registry.register("ada", String::from("cred-1"), b"nope").is_err());
        let secret = p256::ecdsa::SigningKey::from_slice(&[7u8; 32]).unwrap();
        let public = secret.verifying_key().to_sec1_point(false).as_bytes().to_vec();
        registry.register("ada", String::from("cred-1"), &public).unwrap();
        assert_eq!(registry.owner_of("cred-1"), Some(String::from("ada")));
        assert_eq!(registry.owner_of("missing"), None);
        assert!(registry.register("mallory", String::from("cred-1"), &public).is_err());
        assert_eq!(registry.owner_of("cred-1"), Some(String::from("ada")));
        assert!(registry
            .register(&"u".repeat(MAX_USER_LEN + 1), String::from("cred-2"), &public)
            .is_err());
    }

    #[test]
    fn challenge_bounds_users_and_map() {
        let mut registry = PasskeyRegistry::new();
        assert!(registry.challenge("").is_err());
        assert!(registry.challenge(&"u".repeat(MAX_USER_LEN + 1)).is_err());
        assert!(registry.challenge(&"u".repeat(MAX_USER_LEN)).is_ok());
        for index in 0..MAX_CHALLENGES - 1 {
            registry.challenge_at(&format!("user-{index}"), 1000).unwrap();
        }
        assert!(registry.challenge_at("one-more", 1000).is_err());
        assert!(registry.challenge_at("user-0", 1000).is_ok());
        assert!(registry.challenge_at("fresh", 1000 + PASSKEY_CHALLENGE_TTL_SECS + 1).is_ok());
    }

    #[test]
    fn passkey_assertion_verifies_and_consumes() {
        use p256::ecdsa::signature::Signer;
        use sha2::{Digest, Sha256};
        let secret = p256::ecdsa::SigningKey::from_slice(&[7u8; 32]).unwrap();
        let public = secret.verifying_key().to_sec1_point(false).as_bytes().to_vec();
        let rp = "auth.test";
        let origins = vec![String::from("https://auth.test")];
        let mut registry = PasskeyRegistry::new();
        registry.register("ada", String::from("cred-1"), &public).unwrap();
        let challenge = registry.challenge_at("ada", 1000).unwrap();
        let client_data = format!(
            "{{\"type\":\"webauthn.get\",\"challenge\":\"{challenge}\",\"origin\":\"https://auth.test\"}}"
        );
        let mut hasher = Sha256::new();
        hasher.update(rp.as_bytes());
        let mut auth_data = hasher.finalize().to_vec();
        auth_data.push(0x01);
        auth_data.extend_from_slice(&1u32.to_be_bytes());
        let mut hasher = Sha256::new();
        hasher.update(client_data.as_bytes());
        let mut signed = auth_data.clone();
        signed.extend_from_slice(&hasher.finalize());
        let signature: p256::ecdsa::Signature = secret.sign(&signed);
        let signature = signature.to_der().as_bytes().to_vec();
        let attempt = |registry: &mut PasskeyRegistry, auth: &[u8], client: &[u8], sig: &[u8]| {
            registry.verify_assertion(&PasskeyAssertion {
                user: "ada",
                credential_id: "cred-1",
                authenticator_data: auth,
                client_data_json: client,
                signature_der: sig,
                rp_id: rp,
                origins: &origins,
                now_secs: 1100,
            })
        };
        assert!(attempt(&mut registry, &auth_data, client_data.as_bytes(), &signature).is_ok());
        assert!(attempt(&mut registry, &auth_data, client_data.as_bytes(), &signature).is_err());
        let fresh = registry.challenge_at("ada", 1200).unwrap();
        let tampered = client_data.replace(&fresh, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        assert!(attempt(&mut registry, &auth_data, tampered.as_bytes(), &signature).is_err());
        let stale = registry.challenge_at("ada", 0).unwrap();
        let stale_client = format!(
            "{{\"type\":\"webauthn.get\",\"challenge\":\"{stale}\",\"origin\":\"https://auth.test\"}}"
        );
        let mut hasher = Sha256::new();
        hasher.update(stale_client.as_bytes());
        let mut stale_signed = auth_data.clone();
        stale_signed.extend_from_slice(&hasher.finalize());
        let stale_sig: p256::ecdsa::Signature = secret.sign(&stale_signed);
        let stale_sig = stale_sig.to_der().as_bytes().to_vec();
        assert!(attempt(&mut registry, &auth_data, stale_client.as_bytes(), &stale_sig).is_err());
        let fresh = registry.challenge_at("ada", 1300).unwrap();
        let bad_origin = format!(
            "{{\"type\":\"webauthn.get\",\"challenge\":\"{fresh}\",\"origin\":\"https://evil.test\"}}"
        );
        assert!(attempt(&mut registry, &auth_data, bad_origin.as_bytes(), &signature).is_err());
        assert!(registry
            .verify_assertion(&PasskeyAssertion {
                user: "ada",
                credential_id: "cred-1",
                authenticator_data: &auth_data,
                client_data_json: client_data.as_bytes(),
                signature_der: &signature,
                rp_id: "other.test",
                origins: &origins,
                now_secs: 1400,
            })
            .is_err());
        let replay_challenge = registry.challenge_at("ada", 1500).unwrap();
        let replay_client = format!(
            "{{\"type\":\"webauthn.get\",\"challenge\":\"{replay_challenge}\",\"origin\":\"https://auth.test\"}}"
        );
        let mut hasher = Sha256::new();
        hasher.update(replay_client.as_bytes());
        let mut replay_signed = auth_data.clone();
        replay_signed.extend_from_slice(&hasher.finalize());
        let replay_sig: p256::ecdsa::Signature = secret.sign(&replay_signed);
        let replay_sig = replay_sig.to_der().as_bytes().to_vec();
        assert!(attempt(&mut registry, &auth_data, replay_client.as_bytes(), &replay_sig).is_err());
    }

    #[test]
    fn credential_store_password_otp() {
        let mut store = CredentialStore::new();
        let mut roles = HashSet::new();
        roles.insert(Role::ReadWrite);
        store
            .register_password(String::from("ada"), String::from("t"), "correct-horse", roles)
            .unwrap();
        assert!(store.verify_password("ada", "correct-horse").is_ok());
        assert!(store.verify_password("ada", "wrong-pass").is_err());
        let huge = "p".repeat(MAX_PASSWORD_LEN + 1);
        assert!(store.verify_password("ada", &huge).is_err());
        assert!(store
            .register_password(String::from("big"), String::from("t"), &huge, HashSet::new())
            .is_err());
        assert!(store
            .register_password(String::from("x"), String::from("t"), "short", HashSet::new())
            .is_err());
        store.set_otp_secret("ada", b"0123456789abcdef".to_vec()).unwrap();
        let code = totp_code(b"0123456789abcdef", 1_700_000_000);
        assert!(store.verify_otp("ada", &code, 1_700_000_000).is_ok());
        assert!(store.verify_otp("ada", "000000", 1_700_000_000).is_err());
        assert!(store.otp_enrolled("ada"));
        assert!(!store.otp_enrolled("ghost"));
    }

    #[test]
    fn reregister_never_takes_over() {
        let mut store = CredentialStore::new();
        let mut roles = HashSet::new();
        roles.insert(Role::ReadWrite);
        store
            .register_password(String::from("ada"), String::from("t"), "correct-horse", roles)
            .unwrap();
        assert!(store
            .register_password(
                String::from("ada"),
                String::from("evil"),
                "attacker-password",
                HashSet::new()
            )
            .is_err());
        assert!(store.verify_password("ada", "correct-horse").is_ok());
        assert!(store.verify_password("ada", "attacker-password").is_err());
        assert_eq!(store.principal("ada").unwrap().tenant, "t");
        assert!(store
            .register_password(
                "u".repeat(MAX_USER_LEN + 1),
                String::from("t"),
                "correct-horse",
                HashSet::new()
            )
            .is_err());
    }

    #[test]
    fn unknown_users_pay_full_hash_cost() {
        let mut store = CredentialStore::new();
        let mut roles = HashSet::new();
        roles.insert(Role::ReadWrite);
        store
            .register_password(String::from("ada"), String::from("t"), "correct-horse", roles)
            .unwrap();
        let start = std::time::Instant::now();
        assert!(store.verify_password("ghost", "correct-horse").is_err());
        assert!(start.elapsed() >= std::time::Duration::from_millis(5));
        let start = std::time::Instant::now();
        assert!(store.verify_password("ada", "wrong-pass").is_err());
        assert!(start.elapsed() >= std::time::Duration::from_millis(5));
        assert!(store.verify_password("ada", "correct-horse").is_ok());
    }
}
