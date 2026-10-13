use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Superuser,
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
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.id.len() > MAX_PRINCIPAL_ID_LEN {
            return Err(RymeError::InvalidArgument(String::from("principal id")));
        }
        validate_tenant(&self.tenant)
    }

    pub fn can_platform_admin(&self) -> bool {
        self.roles.contains(&Role::Superuser)
    }

    pub fn can_write(&self) -> bool {
        self.can_platform_admin()
            || self.roles.contains(&Role::Owner)
            || self.roles.contains(&Role::Admin)
            || self.roles.contains(&Role::Developer)
            || self.roles.contains(&Role::ReadWrite)
    }

    pub fn can_admin(&self) -> bool {
        self.can_platform_admin()
            || self.roles.contains(&Role::Owner)
            || self.roles.contains(&Role::Admin)
    }

    pub fn sql_role(&self) -> &'static str {
        if self.can_admin() {
            "postgres"
        } else if self.can_write() {
            "authenticated"
        } else if self.roles.contains(&Role::ReadOnly) {
            "anon"
        } else {
            "realtime_subscriber"
        }
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

pub const MAX_MASKED_TABLES: usize = 10_000;
pub const MAX_MASKED_FIELDS: usize = 128;
pub const MAX_MASKED_FIELD_BYTES: usize = 256;
pub const MAX_POLICY_TABLE_BYTES: usize = 256;

fn tenant_field_matches(
    object: &serde_json::Map<String, serde_json::Value>,
    column: &str,
    tenant: &str,
) -> bool {
    let mut values =
        object.iter().filter(|(name, _)| name.eq_ignore_ascii_case(column)).map(|(_, value)| value);
    let Some(value) = values.next() else { return false };
    values.next().is_none() && value.as_str().is_some_and(|value| value == tenant)
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

    pub fn mask_fields(&self, table: String, mut fields: Vec<String>) -> Result<()> {
        if table.is_empty() || table.len() > MAX_POLICY_TABLE_BYTES || table.contains('\0') {
            return Err(RymeError::InvalidArgument(String::from("table")));
        }
        if fields.len() > MAX_MASKED_FIELDS
            || fields.iter().any(|field| {
                field.is_empty() || field.len() > MAX_MASKED_FIELD_BYTES || field.contains('\0')
            })
        {
            return Err(RymeError::InvalidArgument(String::from("fields")));
        }
        fields.sort_unstable();
        fields.dedup();
        let mut guard = self
            .masked_fields
            .write()
            .map_err(|_| RymeError::Internal(String::from("masked fields lock")))?;
        if fields.is_empty() {
            guard.remove(&table);
            return Ok(());
        }
        if !guard.contains_key(&table) && guard.len() >= MAX_MASKED_TABLES {
            return Err(RymeError::Overload(String::from("masked tables")));
        }
        guard.insert(table, fields);
        Ok(())
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
        Ok(tenant_field_matches(&object, column, &principal.tenant))
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
    inner: Arc<RwLock<HashMap<String, ApiKeyRecord>>>,
}

#[derive(Debug, Clone)]
struct ApiKeyRecord {
    principal: Principal,
    expires_at: Option<u64>,
}

const MAX_API_KEYS: usize = 100_000;

impl ApiKeyStore {
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn insert(&self, api_key: String, principal: Principal) -> Result<()> {
        self.insert_record(api_key, principal, None, current_unix_secs())
    }

    pub fn insert_with_expiry(
        &self,
        api_key: String,
        principal: Principal,
        expires_at: u64,
        now_secs: u64,
    ) -> Result<()> {
        self.insert_record(api_key, principal, Some(expires_at), now_secs)
    }

    fn insert_record(
        &self,
        api_key: String,
        principal: Principal,
        expires_at: Option<u64>,
        now_secs: u64,
    ) -> Result<()> {
        principal.validate()?;
        if expires_at.is_some_and(|expires_at| expires_at <= now_secs) {
            return Err(RymeError::InvalidArgument(String::from("api key expiration")));
        }
        let digest = Self::digest(&api_key);
        let mut guard =
            self.inner.write().map_err(|_| RymeError::Internal(String::from("auth lock")))?;
        guard.retain(|_, record| record.expires_at.is_none_or(|expiry| expiry > now_secs));
        if guard.len() >= MAX_API_KEYS && !guard.contains_key(&digest) {
            return Err(RymeError::Overload(String::from("api keys")));
        }
        guard.insert(digest, ApiKeyRecord { principal, expires_at });
        Ok(())
    }

    pub fn remove(&self, api_key: &str) -> bool {
        self.inner
            .write()
            .map(|mut guard| guard.remove(&Self::digest(api_key)).is_some())
            .unwrap_or(false)
    }

    pub fn owner_of(&self, presented: &str) -> Option<Principal> {
        self.owner_of_at(presented, current_unix_secs())
    }

    pub fn owner_of_at(&self, presented: &str, now_secs: u64) -> Option<Principal> {
        let guard = self.inner.read().ok()?;
        let record = guard.get(&Self::digest(presented))?;
        if record.expires_at.is_some_and(|expires_at| expires_at <= now_secs) {
            return None;
        }
        Some(record.principal.clone())
    }

    pub fn authenticate(&self, presented: &str) -> Result<Principal> {
        self.authenticate_at(presented, current_unix_secs())
    }

    pub fn authenticate_at(&self, presented: &str, now_secs: u64) -> Result<Principal> {
        let guard =
            self.inner.read().map_err(|_| RymeError::Internal(String::from("auth lock")))?;
        let record = guard.get(&Self::digest(presented)).ok_or(RymeError::Unauthorized)?;
        if record.expires_at.is_some_and(|expires_at| expires_at <= now_secs) {
            return Err(RymeError::Unauthorized);
        }
        Ok(record.principal.clone())
    }

    pub fn snapshot(&self) -> Result<HashMap<String, Principal>> {
        self.inner
            .read()
            .map(|guard| {
                guard
                    .iter()
                    .map(|(digest, record)| (digest.clone(), record.principal.clone()))
                    .collect()
            })
            .map_err(|_| RymeError::Internal(String::from("auth lock")))
    }

    pub fn from_snapshot(snapshot: HashMap<String, Principal>) -> Result<Self> {
        Self::from_snapshot_with_expirations(snapshot, HashMap::new())
    }

    pub fn expiration_snapshot(&self) -> Result<HashMap<String, u64>> {
        self.inner
            .read()
            .map(|guard| {
                guard
                    .iter()
                    .filter_map(|(digest, record)| {
                        record.expires_at.map(|expires_at| (digest.clone(), expires_at))
                    })
                    .collect()
            })
            .map_err(|_| RymeError::Internal(String::from("auth lock")))
    }

    pub fn from_snapshot_with_expirations(
        snapshot: HashMap<String, Principal>,
        expirations: HashMap<String, u64>,
    ) -> Result<Self> {
        if snapshot.len() > MAX_API_KEYS {
            return Err(RymeError::Corrupt(String::from("api key snapshot")));
        }
        if snapshot.values().any(|principal| principal.validate().is_err()) {
            return Err(RymeError::Corrupt(String::from("api key principal")));
        }
        if expirations.len() > snapshot.len()
            || expirations.keys().any(|key| !snapshot.contains_key(key))
        {
            return Err(RymeError::Corrupt(String::from("api key expiration snapshot")));
        }
        let records = snapshot
            .into_iter()
            .map(|(digest, principal)| {
                let expires_at = expirations.get(&digest).copied();
                (digest, ApiKeyRecord { principal, expires_at })
            })
            .collect();
        Ok(Self { inner: Arc::new(RwLock::new(records)) })
    }

    fn digest(api_key: &str) -> String {
        use sha2::{Digest, Sha256};
        base64_url_encode(&Sha256::digest(api_key.as_bytes()))
    }
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
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
pub struct RsaJwk {
    pub kid: Option<String>,
    pub modulus: Vec<u8>,
    pub exponent: Vec<u8>,
}

#[derive(Debug, Clone)]
enum JwtKey {
    Hmac(Vec<u8>),
    RsaJwks(Vec<RsaJwk>),
}

#[derive(Debug, Clone)]
pub struct JwtVerifier {
    key: JwtKey,
    leeway_secs: u64,
    issuer: Option<String>,
    audience: Option<String>,
}

const MAX_JWT_TOKEN_LEN: usize = 16 * 1024;
const MAX_PRINCIPAL_ID_LEN: usize = 256;
const MAX_TENANT_LEN: usize = 256;

fn validate_tenant(tenant: &str) -> Result<()> {
    if tenant.is_empty() || tenant.len() > MAX_TENANT_LEN {
        return Err(RymeError::InvalidArgument(String::from("tenant")));
    }
    Ok(())
}

impl JwtVerifier {
    pub fn new(secret: Vec<u8>) -> Self {
        Self { key: JwtKey::Hmac(secret), leeway_secs: 60, issuer: None, audience: None }
    }

    pub fn with_issuer(secret: Vec<u8>, issuer: String, audience: String) -> Self {
        Self::new(secret).with_claims(Some(issuer), Some(audience))
    }

    pub fn with_rsa_jwk(modulus: Vec<u8>, exponent: Vec<u8>) -> Self {
        Self::with_rsa_jwks(vec![RsaJwk { kid: None, modulus, exponent }])
    }

    pub fn with_rsa_jwks(keys: Vec<RsaJwk>) -> Self {
        Self { key: JwtKey::RsaJwks(keys), leeway_secs: 60, issuer: None, audience: None }
    }

    pub fn with_rsa_jwk_issuer(
        modulus: Vec<u8>,
        exponent: Vec<u8>,
        issuer: String,
        audience: String,
    ) -> Self {
        Self::with_rsa_jwk(modulus, exponent).with_claims(Some(issuer), Some(audience))
    }

    pub fn with_rsa_jwks_issuer(keys: Vec<RsaJwk>, issuer: String, audience: String) -> Self {
        Self::with_rsa_jwks(keys).with_claims(Some(issuer), Some(audience))
    }

    fn with_claims(mut self, issuer: Option<String>, audience: Option<String>) -> Self {
        self.issuer = issuer;
        self.audience = audience;
        self
    }

    pub fn principal_from_token(&self, token: &str, now_secs: u64) -> Result<Principal> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        if token.len() > MAX_JWT_TOKEN_LEN {
            return Err(RymeError::Unauthorized);
        }
        let mut parts = token.split('.');
        let header_b64 = parts.next().ok_or(RymeError::Unauthorized)?;
        let payload_b64 = parts.next().ok_or(RymeError::Unauthorized)?;
        let sig_b64 = parts.next().ok_or(RymeError::Unauthorized)?;
        if parts.next().is_some() {
            return Err(RymeError::Unauthorized);
        }
        let signing_input = format!("{header_b64}.{payload_b64}");
        let header_raw = base64_url_decode(header_b64).map_err(|_| RymeError::Unauthorized)?;
        let header: serde_json::Value =
            serde_json::from_slice(&header_raw).map_err(|_| RymeError::Unauthorized)?;
        let algorithm = header.get("alg").and_then(|value| value.as_str()).unwrap_or("");
        let presented = base64_url_decode(sig_b64).map_err(|_| RymeError::Unauthorized)?;
        match &self.key {
            JwtKey::Hmac(secret) => {
                if algorithm != "HS256" {
                    return Err(RymeError::Unauthorized);
                }
                let mut mac =
                    Hmac::<Sha256>::new_from_slice(secret).map_err(|_| RymeError::Unauthorized)?;
                mac.update(signing_input.as_bytes());
                let expected = mac.finalize().into_bytes();
                if !constant_time_equal(&expected, &presented) {
                    return Err(RymeError::Unauthorized);
                }
            }
            JwtKey::RsaJwks(keys) => {
                if algorithm != "RS256" {
                    return Err(RymeError::Unauthorized);
                }
                let token_kid = header.get("kid").and_then(|value| value.as_str());
                let key = if let Some(kid) = token_kid {
                    keys.iter().find(|key| key.kid.as_deref() == Some(kid))
                } else if keys.len() == 1 {
                    keys.first()
                } else {
                    let mut unlabelled = keys.iter().filter(|key| key.kid.is_none());
                    let key = unlabelled.next();
                    if unlabelled.next().is_some() {
                        None
                    } else {
                        key
                    }
                }
                .ok_or(RymeError::Unauthorized)?;
                let public_key =
                    ring::signature::RsaPublicKeyComponents { n: &key.modulus, e: &key.exponent };
                public_key
                    .verify(
                        &ring::signature::RSA_PKCS1_2048_8192_SHA256,
                        signing_input.as_bytes(),
                        &presented,
                    )
                    .map_err(|_| RymeError::Unauthorized)?;
            }
        }
        let payload_raw = base64_url_decode(payload_b64).map_err(|_| RymeError::Unauthorized)?;
        let claims: serde_json::Value =
            serde_json::from_slice(&payload_raw).map_err(|_| RymeError::Unauthorized)?;
        let exp =
            claims.get("exp").and_then(|value| value.as_u64()).ok_or(RymeError::Unauthorized)?;
        if exp.saturating_add(self.leeway_secs) < now_secs {
            return Err(RymeError::Unauthorized);
        }
        for claim in ["nbf", "iat"] {
            if let Some(value) = claims.get(claim) {
                let value = value.as_u64().ok_or(RymeError::Unauthorized)?;
                if value > now_secs.saturating_add(self.leeway_secs) {
                    return Err(RymeError::Unauthorized);
                }
            }
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
        let id = claims
            .get("sub")
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .filter(|value| value.len() <= MAX_PRINCIPAL_ID_LEN)
            .ok_or(RymeError::Unauthorized)?
            .to_string();
        let tenant = claims
            .get("tenant")
            .or_else(|| claims.get("tenant_id"))
            .or_else(|| claims.get("app_metadata").and_then(|v| v.get("tenant")))
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        if tenant.is_empty() || tenant.len() > MAX_TENANT_LEN {
            return Err(RymeError::Unauthorized);
        }
        let roles = claim_roles(&claims);
        Ok(Principal { id, tenant: tenant.to_string(), roles })
    }
}

fn claim_roles(claims: &serde_json::Value) -> HashSet<Role> {
    let mut roles = HashSet::new();
    let mut add = |value: &serde_json::Value| match value {
        serde_json::Value::Array(list) => {
            for entry in list {
                if let Some(name) = entry.as_str() {
                    if let Some(role) = parse_role(name) {
                        roles.insert(role);
                    }
                }
            }
        }
        serde_json::Value::String(name) => {
            if let Some(role) = parse_role(name) {
                roles.insert(role);
            }
        }
        _ => {}
    };
    if let Some(value) = claims.get("roles") {
        add(value);
    }
    if let Some(value) = claims.get("role") {
        add(value);
    }
    if let Some(value) = claims.get("app_metadata").and_then(|metadata| metadata.get("roles")) {
        add(value);
    }
    roles
}

fn parse_role(name: &str) -> Option<Role> {
    match name.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "service_role" | "superuser" => Some(Role::Superuser),
        "owner" => Some(Role::Owner),
        "admin" => Some(Role::Admin),
        "developer" => Some(Role::Developer),
        "readwrite" | "read_write" | "authenticated" => Some(Role::ReadWrite),
        "readonly" | "read_only" | "anon" => Some(Role::ReadOnly),
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
    use ring::rand::{SecureRandom, SystemRandom};

    let mut out = vec![0u8; count];
    SystemRandom::new().fill(&mut out).expect("OS secure random source unavailable");
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
        principal.validate()?;
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
        self.rotate_with(presented, now_secs, |principal| Ok(principal.clone()))
    }

    pub fn rotate_with<T, F>(&self, presented: &str, now_secs: u64, issue: F) -> Result<(T, String)>
    where
        F: FnOnce(&Principal) -> Result<T>,
    {
        if presented.is_empty() {
            return Err(RymeError::Unauthorized);
        }
        let digest = Self::digest(presented);
        let mut sessions =
            self.inner.write().map_err(|_| RymeError::Internal(String::from("auth lock")))?;
        let session = sessions.get(&digest).cloned().ok_or(RymeError::Unauthorized)?;
        if session.expires_at <= now_secs {
            sessions.remove(&digest);
            return Err(RymeError::Unauthorized);
        }
        let issued = issue(&session.principal)?;
        let raw = base64_url_encode(&random_bytes(32));
        let next_digest = Self::digest(&raw);
        sessions.remove(&digest);
        sessions.insert(
            next_digest,
            RefreshTokenRecord {
                principal: session.principal.clone(),
                expires_at: now_secs.saturating_add(REFRESH_TOKEN_TTL_SECS),
            },
        );
        Ok((issued, raw))
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn snapshot(&self) -> Result<HashMap<String, RefreshTokenRecord>> {
        self.inner
            .read()
            .map(|sessions| sessions.clone())
            .map_err(|_| RymeError::Internal(String::from("auth lock")))
    }

    pub fn from_snapshot(snapshot: HashMap<String, RefreshTokenRecord>) -> Result<Self> {
        if snapshot.len() > MAX_REFRESH_SESSIONS {
            return Err(RymeError::Corrupt(String::from("refresh token snapshot")));
        }
        if snapshot.values().any(|record| record.principal.validate().is_err()) {
            return Err(RymeError::Corrupt(String::from("refresh token principal")));
        }
        Ok(Self { inner: Arc::new(RwLock::new(snapshot)) })
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

/// Legacy SHA-256 work factor retained for serialized-hash compatibility.
pub const PASSWORD_ROUNDS: u32 = 210_000;
const ARGON2ID_MARKER: u32 = u32::MAX;
const ARGON2ID_MEMORY_KIB: u32 = 19 * 1024;
const ARGON2ID_TIME_COST: u32 = 2;
const ARGON2ID_PARALLELISM: u32 = 1;
const PASSWORD_HASH_LEN: usize = 32;
const MAX_LEGACY_PASSWORD_ROUNDS: u32 = 1_000_000;
pub const MAX_PASSWORD_LEN: usize = 256;
pub const MAX_USER_LEN: usize = 256;
pub const MAX_CHALLENGES: usize = 4096;
const MAX_USERS: usize = 100_000;

pub fn hash_password(password: &str) -> PasswordHash {
    let salt = random_bytes(16);
    let mut hash = [0u8; PASSWORD_HASH_LEN];
    argon2id()
        .hash_password_into(password.as_bytes(), &salt, &mut hash)
        .expect("fixed Argon2id password parameters must be valid");
    PasswordHash {
        salt_b64: base64_url_encode(&salt),
        hash_b64: base64_url_encode(&hash),
        rounds: ARGON2ID_MARKER,
    }
}

/// Hash a password using the legacy SHA-256 format.
///
/// New credentials must use [`hash_password`]. This remains available so
/// callers can create fixtures and retain compatibility with stored hashes
/// written before Argon2id was introduced.
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
    let candidate = if stored.rounds == ARGON2ID_MARKER {
        let mut hash = [0u8; PASSWORD_HASH_LEN];
        if argon2id().hash_password_into(password.as_bytes(), &salt, &mut hash).is_err() {
            return false;
        }
        base64_url_encode(&hash)
    } else {
        if stored.rounds == 0 || stored.rounds > MAX_LEGACY_PASSWORD_ROUNDS {
            return false;
        }
        hash_password_with_salt(password, &salt, stored.rounds).hash_b64
    };
    constant_time_equal(candidate.as_bytes(), stored.hash_b64.as_bytes())
}

fn argon2id() -> argon2::Argon2<'static> {
    let params = argon2::Params::new(
        ARGON2ID_MEMORY_KIB,
        ARGON2ID_TIME_COST,
        ARGON2ID_PARALLELISM,
        Some(PASSWORD_HASH_LEN),
    )
    .expect("fixed Argon2id password parameters must be valid");
    argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params)
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
    matching_totp_step(secret, code, unix_secs).is_some()
}

fn matching_totp_step(secret: &[u8], code: &str, unix_secs: u64) -> Option<u64> {
    let current_step = unix_secs / 30;
    let mut matched_step = 0u64;
    let mut found = 0u64;
    for step in [current_step, current_step.saturating_sub(1), current_step.saturating_sub(2)] {
        let matches = u64::from(constant_time_equal(
            totp_code(secret, step * 30).as_bytes(),
            code.as_bytes(),
        ));
        let take = matches & (found ^ 1);
        let mask = 0u64.wrapping_sub(take);
        matched_step = (matched_step & !mask) | (step & mask);
        found |= matches;
    }
    (found == 1).then_some(matched_step)
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
const MAX_PASSKEY_CREDENTIALS: usize = 100_000;

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
        if self.credentials.len() >= MAX_PASSKEY_CREDENTIALS {
            return Err(RymeError::Overload(String::from("passkey credentials")));
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

    pub fn from_snapshot(credentials: HashMap<String, PasskeyCredential>) -> Result<Self> {
        if credentials.len() > MAX_PASSKEY_CREDENTIALS {
            return Err(RymeError::Corrupt(String::from("passkey snapshot")));
        }
        Ok(Self { challenges: HashMap::new(), credentials })
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
    pub redirect_uris: Vec<String>,
}

impl OidcConfig {
    pub fn auth_url(&self, redirect_uri: &str, state: &str) -> Result<String> {
        if self.auth_endpoint.is_empty()
            || self.client_id.is_empty()
            || redirect_uri.is_empty()
            || state.is_empty()
        {
            return Err(RymeError::InvalidArgument(String::from("oidc login")));
        }
        if !self.redirect_uris.iter().any(|allowed| allowed == redirect_uri) {
            return Err(RymeError::InvalidArgument(String::from("oidc redirect_uri")));
        }
        let separator = if self.auth_endpoint.contains('?') { '&' } else { '?' };
        Ok(format!(
            "{}{separator}response_type=code&client_id={}&redirect_uri={}&scope=openid%20email%20profile&state={}",
            self.auth_endpoint,
            encode_query_component(&self.client_id),
            encode_query_component(redirect_uri),
            encode_query_component(state),
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

fn encode_query_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(char::from_digit((byte >> 4) as u32, 16).unwrap().to_ascii_uppercase());
            encoded.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap().to_ascii_uppercase());
        }
    }
    encoded
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredUser {
    pub tenant: String,
    pub roles: HashSet<Role>,
    pub password: Option<PasswordHash>,
    pub otp_secret: Option<Vec<u8>>,
    #[serde(default)]
    pub otp_last_used_step: Option<u64>,
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
        validate_tenant(&tenant)?;
        if self.users.contains_key(&id) {
            return Err(RymeError::Conflict(String::from("user")));
        }
        if self.users.len() >= MAX_USERS {
            return Err(RymeError::Overload(String::from("users")));
        }
        self.users.insert(
            id,
            StoredUser {
                tenant,
                roles,
                password: Some(hash_password(password)),
                otp_secret: None,
                otp_last_used_step: None,
            },
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
                    rounds: ARGON2ID_MARKER,
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
        user.otp_last_used_step = None;
        Ok(())
    }

    pub fn otp_enrolled(&self, id: &str) -> bool {
        self.users.get(id).and_then(|user| user.otp_secret.as_ref()).is_some()
    }

    pub fn principal(&self, id: &str) -> Result<Principal> {
        let user = self.users.get(id).ok_or(RymeError::Unauthorized)?;
        Ok(Principal { id: id.to_string(), tenant: user.tenant.clone(), roles: user.roles.clone() })
    }

    pub fn tenant_of(&self, id: &str) -> Option<&str> {
        self.users.get(id).map(|user| user.tenant.as_str())
    }

    pub fn verify_otp(&mut self, id: &str, code: &str, now_secs: u64) -> Result<Principal> {
        let user = self.users.get_mut(id).ok_or(RymeError::Unauthorized)?;
        let secret = user.otp_secret.as_ref().ok_or(RymeError::Unauthorized)?;
        let step = matching_totp_step(secret, code, now_secs).ok_or(RymeError::Unauthorized)?;
        if user.otp_last_used_step.is_some_and(|last_used| step <= last_used) {
            return Err(RymeError::Unauthorized);
        }
        user.otp_last_used_step = Some(step);
        Ok(Principal {
            id: id.to_string(),
            tenant: user.tenant.clone(),
            roles: user.roles.clone(),
        })
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

    pub fn from_snapshot(users: HashMap<String, StoredUser>) -> Result<Self> {
        if users.len() > MAX_USERS {
            return Err(RymeError::Corrupt(String::from("user snapshot")));
        }
        if users.keys().any(|id| id.is_empty() || id.len() > MAX_USER_LEN)
            || users.values().any(|user| validate_tenant(&user.tenant).is_err())
        {
            return Err(RymeError::Corrupt(String::from("user snapshot")));
        }
        Ok(Self { users })
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
        store
            .insert(
                String::from("secret-key"),
                Principal { id: String::from("u"), tenant: String::from("t"), roles },
            )
            .unwrap();
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
    fn expiring_api_keys_reject_expired_tokens_and_survive_snapshots() {
        let store = ApiKeyStore::new();
        let mut roles = HashSet::new();
        roles.insert(Role::ReadWrite);
        let principal = Principal { id: String::from("u"), tenant: String::from("t"), roles };
        store.insert_with_expiry(String::from("short-key"), principal, 200, 100).unwrap();

        assert!(store.authenticate_at("short-key", 199).is_ok());
        assert!(store.authenticate_at("short-key", 200).is_err());
        assert!(store.owner_of_at("short-key", 200).is_none());

        let restored = ApiKeyStore::from_snapshot_with_expirations(
            store.snapshot().unwrap(),
            store.expiration_snapshot().unwrap(),
        )
        .unwrap();
        assert!(restored.authenticate_at("short-key", 199).is_ok());
        assert!(restored.authenticate_at("short-key", 200).is_err());
    }

    #[test]
    fn auth_stores_reject_invalid_principal_tenants() {
        let principal =
            Principal { id: String::from("u"), tenant: String::new(), roles: HashSet::new() };
        assert!(principal.validate().is_err());
        assert!(ApiKeyStore::new().insert(String::from("key"), principal.clone()).is_err());
        assert!(RefreshTokenStore::new().issue(principal, 100).is_err());

        let mut users = HashMap::new();
        users.insert(
            String::from("u"),
            StoredUser {
                tenant: String::new(),
                roles: HashSet::new(),
                password: None,
                otp_secret: None,
                otp_last_used_step: None,
            },
        );
        assert!(matches!(
            CredentialStore::from_snapshot(users),
            Err(RymeError::Corrupt(message)) if message == "user snapshot"
        ));
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
        assert!(store
            .rotate_with::<(), _>(&second, 101, |_| {
                Err(RymeError::Overload(String::from("test")))
            })
            .is_err());
        let (_, third) = store.rotate(&second, 101).unwrap();
        assert!(store.rotate(&third, 101 + REFRESH_TOKEN_TTL_SECS).is_err());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn jwt_reject_bad_signature() {
        let verifier = JwtVerifier::new(b"test-secret".to_vec());
        assert!(verifier.principal_from_token("a.b.c", 1000).is_err());
    }

    #[test]
    fn jwt_requires_numeric_expiration_and_temporal_claims() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let token = |payload: serde_json::Value| {
            let header = base64_url_encode(br#"{"alg":"HS256"}"#);
            let payload = base64_url_encode(serde_json::to_string(&payload).unwrap().as_bytes());
            let input = format!("{header}.{payload}");
            let mut mac = Hmac::<Sha256>::new_from_slice(b"temporal-secret").unwrap();
            mac.update(input.as_bytes());
            format!("{input}.{}", base64_url_encode(&mac.finalize().into_bytes()))
        };
        let verifier = JwtVerifier::new(b"temporal-secret".to_vec());

        assert!(verifier
            .principal_from_token(&token(serde_json::json!({ "sub": "ada" })), 0)
            .is_err());
        assert!(verifier
            .principal_from_token(&token(serde_json::json!({ "sub": "ada", "exp": "never" })), 0,)
            .is_err());
        assert!(verifier
            .principal_from_token(
                &token(serde_json::json!({
                    "sub": "ada",
                    "exp": 2000000000u64,
                    "nbf": "soon",
                })),
                1000,
            )
            .is_err());
        assert!(verifier
            .principal_from_token(
                &token(serde_json::json!({
                    "sub": "ada",
                    "exp": 2000000000u64,
                    "iat": false,
                })),
                1000,
            )
            .is_err());
    }

    #[test]
    fn jwt_requires_non_empty_subject() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let token = |payload: serde_json::Value| {
            let header = base64_url_encode(br#"{"alg":"HS256"}"#);
            let payload = base64_url_encode(serde_json::to_string(&payload).unwrap().as_bytes());
            let input = format!("{header}.{payload}");
            let mut mac = Hmac::<Sha256>::new_from_slice(b"subject-secret").unwrap();
            mac.update(input.as_bytes());
            format!("{input}.{}", base64_url_encode(&mac.finalize().into_bytes()))
        };
        let verifier = JwtVerifier::new(b"subject-secret".to_vec());

        assert!(verifier
            .principal_from_token(&token(serde_json::json!({ "exp": 2000000000u64 })), 1000)
            .is_err());
        assert!(verifier
            .principal_from_token(
                &token(serde_json::json!({ "sub": "  ", "exp": 2000000000u64 })),
                1000,
            )
            .is_err());
    }

    #[test]
    fn jwt_bounds_token_and_principal_sizes() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let token = |payload: serde_json::Value| {
            let header = base64_url_encode(br#"{"alg":"HS256"}"#);
            let payload = base64_url_encode(serde_json::to_string(&payload).unwrap().as_bytes());
            let input = format!("{header}.{payload}");
            let mut mac = Hmac::<Sha256>::new_from_slice(b"size-secret").unwrap();
            mac.update(input.as_bytes());
            format!("{input}.{}", base64_url_encode(&mac.finalize().into_bytes()))
        };
        let verifier = JwtVerifier::new(b"size-secret".to_vec());

        assert!(verifier.principal_from_token(&"x".repeat(MAX_JWT_TOKEN_LEN + 1), 1000).is_err());
        assert!(verifier
            .principal_from_token(
                &token(serde_json::json!({
                    "sub": "u".repeat(MAX_PRINCIPAL_ID_LEN + 1),
                    "exp": 2000000000u64,
                })),
                1000,
            )
            .is_err());
        assert!(verifier
            .principal_from_token(
                &token(serde_json::json!({
                    "sub": "u",
                    "tenant": "t".repeat(MAX_TENANT_LEN + 1),
                    "exp": 2000000000u64,
                })),
                1000,
            )
            .is_err());
        assert!(verifier
            .principal_from_token(
                &token(serde_json::json!({ "sub": "u", "tenant": "", "exp": 2000000000u64 })),
                1000,
            )
            .is_err());
    }

    #[test]
    fn jwt_enforces_not_before_and_issued_at() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let token = |claim: &str, value: u64| {
            let header = base64_url_encode(br#"{"alg":"HS256"}"#);
            let payload = base64_url_encode(
                serde_json::to_string(&serde_json::json!({
                    "sub": "ada",
                    "exp": 2000000000u64,
                    claim: value,
                }))
                .unwrap()
                .as_bytes(),
            );
            let input = format!("{header}.{payload}");
            let mut mac = Hmac::<Sha256>::new_from_slice(b"temporal-secret").unwrap();
            mac.update(input.as_bytes());
            format!("{input}.{}", base64_url_encode(&mac.finalize().into_bytes()))
        };
        let verifier = JwtVerifier::new(b"temporal-secret".to_vec());
        assert!(verifier.principal_from_token(&token("nbf", 1061), 1000).is_err());
        assert!(verifier.principal_from_token(&token("nbf", 1060), 1000).is_ok());
        assert!(verifier.principal_from_token(&token("iat", 1061), 1000).is_err());
    }

    #[test]
    fn jwt_rejects_algorithm_confusion() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let payload = base64_url_encode(
            serde_json::to_string(&serde_json::json!({ "sub": "ada", "exp": 2000000000u64 }))
                .unwrap()
                .as_bytes(),
        );
        let header = base64_url_encode(br#"{"alg":"RS256"}"#);
        let input = format!("{header}.{payload}");
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(input.as_bytes());
        let token = format!("{input}.{}", base64_url_encode(&mac.finalize().into_bytes()));
        assert!(JwtVerifier::new(b"secret".to_vec()).principal_from_token(&token, 1000).is_err());
    }

    #[test]
    fn jwt_without_a_recognized_role_has_no_write_capability() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let header = base64_url_encode(br#"{"alg":"HS256"}"#);
        let payload = base64_url_encode(
            serde_json::to_string(&serde_json::json!({
                "sub": "unassigned",
                "tenant": "t",
                "exp": 2000000000u64,
            }))
            .unwrap()
            .as_bytes(),
        );
        let input = format!("{header}.{payload}");
        let mut mac = Hmac::<Sha256>::new_from_slice(b"role-secret").unwrap();
        mac.update(input.as_bytes());
        let token = format!("{input}.{}", base64_url_encode(&mac.finalize().into_bytes()));
        let principal = JwtVerifier::new(b"role-secret".to_vec())
            .principal_from_token(&token, 1000)
            .unwrap();
        assert!(!principal.can_read());
        assert!(!principal.can_write());
    }

    #[test]
    fn jwt_verifies_rs256_jwk() {
        use base64::Engine;
        const PRIVATE_KEY: &str = "MIIEpAIBAAKCAQEAksesa1z6EENheQ7vbs6t9HdN1sQmR1ATKmR7+h8QCxDRLjCiNmK40dHlQLpnnMEYOwE4A12eqt8zTY7zV4arusU9/zXoBdJNBppyyee8KydKjSJzBN79sic1qeP0TyP73uyFmXfsmQA6JDEXhTBPuCvFExedDZhf4NVzqN3aILpeKz+Uha0wuGRWi1tIvwLlVB/uGJFywB1eBimv2qrC8+LNiQc6b5hLbQUh//rJ8+boBxXcIGKOHU/e68pc66xKVoU+y0uqkw3v13Usvs/JY8y9y8X/Vqg1PjYG3/wqo346dKOnwzB8NKEQYax//Ez+dnF9ziT7PsLoiAFrUXo5lwIDAQABAoIBADMYaz8cqLhMnHvl5RevvwKhHWmr8p9UkrdpL3dMpOsVOZxYtb808p2n8aRsv3DkDylLLVKOnpNhC5zuLGvFp7Zmv0Q+YbzVDpdD4L3Ee86R6PPkkCGb9rizyYyQnBWIsz9RGtjn1GcWmybKKCPnQ3kbU4ln6PY4mvfLlm2sfShy8R+E/w9bBQ2q5LDPAMRexEkCwoQQ87ukbHDAh28hWZSv4m5zOBAyJXlOpXzCnV5XMiUxogYvDoFMipgTM/9dUnpRx3sU326pkMeURGy2ZcD0dL6s0dPKLLPqVedKfj5kfxdA3Dl26jOq9NLLyL/36cZCyTtRAYi+HfClM9OLsqECgYEAxv3Nk+2Q/5kHvWFwbCn0u1Fbgv3uuDT/2vlpW1Yax/ytnrA3whMg+RFhKArMLKf5NgeAuC7Pz7y03HuPTgbtPH+HIEHeYCEUUBZz+kFDbPvMNXYQAjuBeinh1y7B2F1yZ93XiRDw7GDGROmWFWVRtw4guzUNCFKptlHzfkTnCb8CgYEAvNSjumP5wr5G0z93LRFNLMYSRrc401AoTkZOGLC4XlzJsajgof+TJ2jQFtNFQeDOBhAlxIsRG95FXuTdrLlq8R4cD0QwMmonaaetjfRZUmG9AcH6TC+kIha9sJr7pSSwQ3oYF9kEEiTQTwdri5rySE2gDgDRyNh82uEeo+as1ikCgYBs7AY4X8SLtBdBeXGRM1ILqpfd1uNxn0khcn5SCYq1R0E96JVMvS3CR9oXkfl6xCMsiOTTbIRB4LhbK6Mggbgf8mpjpMp1cgXtZKztIgYnRJjd9pcCsdHIrd457BUdOuFhq1PMaiKGMN62X+nQIlqdSbVu47YThTPyYV9YEZ0pfwKBgQC7n5TneLsMMz/wWpkx4IUMPeqe9SKzZGwICndCtSUcLZoCusgobudqOwy4AUhJgvNytsywE3X2eM340fMo0jteRQqgaKJzFWL/6inaaLXv00mX24O1+4hcjrgAKJ5topy5DzGtUGZpGMcaH0dOubCPAwJLp6jKpcC5QwHM3UFSsQKBgQCBDeN3Ht8rq/gzca1R4U3QJmFCl9Ba9AJLH6PgeBTkBD+ZeZPFJjnahCmcunMUqvkaqffgUi+/YRIasgGtASHoZff3wx0wmEjB2L729ZH3OyADhzBiqB26DXtLfXbmPE1fRoUTvG1PgNonAFXsxJTd6fHl5tgcA2o77nFrlr7i/w==";
        let private_key = base64::engine::general_purpose::STANDARD.decode(PRIVATE_KEY).unwrap();
        let key_pair = ring::signature::RsaKeyPair::from_der(&private_key).unwrap();
        let header = base64_url_encode(br#"{"alg":"RS256","kid":"test"}"#);
        let payload = base64_url_encode(
            serde_json::to_string(&serde_json::json!({
                "sub": "ada",
                "tenant": "t",
                "roles": ["readonly"],
                "exp": 2000000000u64,
            }))
            .unwrap()
            .as_bytes(),
        );
        let input = format!("{header}.{payload}");
        let mut signature = vec![0; key_pair.public().modulus_len()];
        key_pair
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                input.as_bytes(),
                &mut signature,
            )
            .unwrap();
        let token = format!("{input}.{}", base64_url_encode(&signature));
        let modulus = base64::engine::general_purpose::STANDARD
            .decode("ksesa1z6EENheQ7vbs6t9HdN1sQmR1ATKmR7+h8QCxDRLjCiNmK40dHlQLpnnMEYOwE4A12eqt8zTY7zV4arusU9/zXoBdJNBppyyee8KydKjSJzBN79sic1qeP0TyP73uyFmXfsmQA6JDEXhTBPuCvFExedDZhf4NVzqN3aILpeKz+Uha0wuGRWi1tIvwLlVB/uGJFywB1eBimv2qrC8+LNiQc6b5hLbQUh//rJ8+boBxXcIGKOHU/e68pc66xKVoU+y0uqkw3v13Usvs/JY8y9y8X/Vqg1PjYG3/wqo346dKOnwzB8NKEQYax//Ez+dnF9ziT7PsLoiAFrUXo5lw==")
            .unwrap();
        let exponent = base64_url_decode("AQAB").unwrap();
        let verifier = JwtVerifier::with_rsa_jwks(vec![
            RsaJwk {
                kid: Some(String::from("old")),
                modulus: modulus.clone(),
                exponent: exponent.clone(),
            },
            RsaJwk { kid: Some(String::from("test")), modulus, exponent },
        ]);
        let principal = verifier.principal_from_token(&token, 1000).unwrap();
        assert_eq!(principal.id, "ada");
        assert!(principal.roles.contains(&Role::ReadOnly));
        let unknown_kid = base64_url_encode(br#"{"alg":"RS256","kid":"unknown"}"#);
        let unknown_input = format!("{unknown_kid}.{payload}");
        let mut unknown_signature = vec![0; key_pair.public().modulus_len()];
        key_pair
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                unknown_input.as_bytes(),
                &mut unknown_signature,
            )
            .unwrap();
        let unknown_token = format!("{unknown_input}.{}", base64_url_encode(&unknown_signature));
        assert!(verifier.principal_from_token(&unknown_token, 1000).is_err());
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

    #[test]
    fn jwt_accepts_supabase_style_role_claims() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let payload = serde_json::json!({
            "sub": "ada",
            "tenant_id": "project-a",
            "role": "authenticated",
            "app_metadata": { "roles": ["realtime_subscriber"] },
            "exp": 2000000000u64,
        });
        let header_b64 = base64_url_encode(br#"{"alg":"HS256"}"#);
        let payload_b64 = base64_url_encode(serde_json::to_string(&payload).unwrap().as_bytes());
        let input = format!("{header_b64}.{payload_b64}");
        let mut mac = Hmac::<Sha256>::new_from_slice(b"supabase-secret").unwrap();
        mac.update(input.as_bytes());
        let token = format!("{input}.{}", base64_url_encode(&mac.finalize().into_bytes()));

        let principal = JwtVerifier::new(b"supabase-secret".to_vec())
            .principal_from_token(&token, 1000)
            .unwrap();
        assert_eq!(principal.tenant, "project-a");
        assert!(principal.roles.contains(&Role::ReadWrite));
        assert!(principal.roles.contains(&Role::RealtimeSubscriber));
    }

    fn oidc_test_config() -> OidcConfig {
        OidcConfig {
            issuer: String::from("https://auth.rymedb.test"),
            audience: String::from("rymedb-app"),
            client_secret: b"issuer-secret".to_vec(),
            auth_endpoint: String::from("https://auth.rymedb.test/authorize"),
            client_id: String::from("rymedb-app"),
            redirect_uris: vec![
                String::from("https://app.test/callback"),
                String::from("https://app.test/callback?next=/home&state=evil"),
            ],
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
    fn oidc_encodes_untrusted_query_values() {
        let config = oidc_test_config();
        let url = config
            .auth_url("https://app.test/callback?next=/home&state=evil", "x y&z")
            .unwrap();
        assert!(url.contains(
            "redirect_uri=https%3A%2F%2Fapp.test%2Fcallback%3Fnext%3D%2Fhome%26state%3Devil"
        ));
        assert!(url.ends_with("&state=x%20y%26z"));
        assert!(!url.contains("&state=evil"));
        assert!(config.auth_url("https://evil.test/callback", "xyz").is_err());
    }

    #[test]
    fn password_roundtrip() {
        let stored = hash_password_with_salt("hunter2", b"fixed-salt-123456", 2);
        assert!(verify_password("hunter2", &stored));
        assert!(!verify_password("wrong", &stored));
    }

    #[test]
    fn new_passwords_use_argon2id_and_legacy_work_factor_is_bounded() {
        let stored = hash_password("hunter2");
        assert_eq!(stored.rounds, ARGON2ID_MARKER);
        assert!(verify_password("hunter2", &stored));
        assert!(!verify_password("wrong", &stored));

        let malformed = PasswordHash { rounds: u32::MAX - 1, ..stored };
        assert!(!verify_password("hunter2", &malformed));
    }

    #[test]
    fn totp_window() {
        let secret = b"totp-secret";
        let now = 1_700_000_000;
        assert!(verify_totp(secret, &totp_code(secret, now), now));
        assert!(verify_totp(secret, &totp_code(secret, now - 30), now));
        assert!(verify_totp(secret, &totp_code(secret, now - 60), now));
        assert!(!verify_totp(secret, &totp_code(secret, now - 90), now));
        assert!(!verify_totp(secret, "000000", now));
    }

    #[test]
    fn mask_redacts_fields() {
        let engine = PolicyEngine::new();
        engine.mask_fields(String::from("users"), vec![String::from("ssn")]).unwrap();
        let masked = engine.masked_value("users", b"{\"name\":\"ada\",\"ssn\":\"123\"}");
        let text = String::from_utf8(masked).unwrap();
        assert!(text.contains("***"));
        assert!(!text.contains("123"));
        let plain = engine.masked_value("other", b"{\"ssn\":\"123\"}");
        assert_eq!(plain, b"{\"ssn\":\"123\"}".to_vec());
    }

    #[test]
    fn mask_configuration_is_bounded_and_clearable() {
        let engine = PolicyEngine::new();
        assert!(engine.mask_fields(String::new(), Vec::new()).is_err());
        assert!(engine
            .mask_fields(String::from("users"), vec!["f".repeat(MAX_MASKED_FIELD_BYTES + 1)])
            .is_err());
        assert!(engine
            .mask_fields(
                String::from("users"),
                (0..=MAX_MASKED_FIELDS).map(|index| format!("field-{index}")).collect(),
            )
            .is_err());
        engine.mask_fields(String::from("users"), vec![String::from("ssn")]).unwrap();
        engine.mask_fields(String::from("users"), Vec::new()).unwrap();
        assert_eq!(engine.masked_value("users", br#"{"ssn":"123"}"#), br#"{"ssn":"123"}"#);
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
        assert!(!engine
            .row_allowed(
                &principal,
                "messages",
                br#"{"tenant_id":"tenant-a","Tenant_id":"tenant-b"}"#,
            )
            .unwrap());
        assert!(engine
            .check_write_row(&principal, "messages", br#"{"tenant_id":"tenant-a"}"#)
            .is_ok());
        assert!(matches!(
            engine.check_write_row(&principal, "messages", br#"{"tenant_id":"tenant-b"}"#),
            Err(RymeError::Forbidden)
        ));
        assert!(matches!(
            engine.check_write_row(
                &principal,
                "messages",
                br#"{"tenant_id":"tenant-a","Tenant_id":"tenant-b"}"#,
            ),
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
    fn credential_registration_requires_bounded_tenant() {
        let mut store = CredentialStore::new();
        assert!(store
            .register_password(String::from("ada"), String::new(), "correct-horse", HashSet::new())
            .is_err());
        assert!(store
            .register_password(
                String::from("ada"),
                "t".repeat(MAX_TENANT_LEN + 1),
                "correct-horse",
                HashSet::new(),
            )
            .is_err());
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
