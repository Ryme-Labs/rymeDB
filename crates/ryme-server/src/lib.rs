use axum::extract::{Path, Query, State, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Json;
use futures_util::{SinkExt, StreamExt};
use ryme_archive::{Archiver, BackupManifest};
use ryme_auth::{
    ApiKeyStore, CredentialStore, JwtVerifier, OidcConfig, PasskeyRegistry, PolicyEngine,
    Principal, RefreshTokenRecord, RefreshTokenStore, Role, RsaJwk,
};
use ryme_backup::Checkpoint;
use ryme_config::{Config, OtelConfig};
use ryme_control::ControlPlane;
use ryme_crypto::{EnvKms, KeyRing, KmsProvider, WrappedDek};
use ryme_gateway::Gateway;
use ryme_index::PartitionedIndex;
use ryme_metering::{MeterRegistry, Metric, UsageEvent};
use ryme_observe::{Histogram, LatencyWindow, SlowEntry, SlowLog, TraceCollector, TraceSpan};
use ryme_qos::{QosRegistry, Tier};
use ryme_raft::net::{ClusterBackend, Node, RangeOwner};
use ryme_realtime::Realtime;
use ryme_router::Range;
use ryme_shard::{HybridBackend, ShardSet, TableRef};
use ryme_sql::{bind, parse, Executor, QueryResult, Statement};
use ryme_txn::{DurableManager, SyncPolicy, TxnBackend, TxnManager};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

pub async fn bind_retry(addr: std::net::SocketAddr) -> ryme_error::Result<TcpListener> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match TcpListener::bind(addr).await {
            Ok(listener) => return Ok(listener),
            Err(e)
                if e.kind() == std::io::ErrorKind::AddrInUse
                    && std::time::Instant::now() < deadline =>
            {
                tracing::warn!("bind {addr} in use; retrying: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(e) => return Err(ryme_error::RymeError::Io(e.to_string())),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Backend {
    Single(DurableManager),
    Cluster(ClusterBackend),
    Sharded(ShardSet),
    Hybrid(HybridBackend),
}

impl ryme_txn::TxnBackend for Backend {
    fn begin(&self) -> ryme_txn::Transaction {
        match self {
            Self::Single(manager) => manager.begin(),
            Self::Cluster(backend) => backend.begin(),
            Self::Sharded(shards) => shards.begin(),
            Self::Hybrid(hybrid) => hybrid.begin(),
        }
    }

    fn get(
        &self,
        txn: &mut ryme_txn::Transaction,
        key: &ryme_storage::RecordKey,
    ) -> ryme_error::Result<Option<Vec<u8>>> {
        match self {
            Self::Single(manager) => manager.get(txn, key),
            Self::Cluster(backend) => backend.get(txn, key),
            Self::Sharded(shards) => shards.get(txn, key),
            Self::Hybrid(hybrid) => hybrid.get(txn, key),
        }
    }

    fn put(&self, txn: &mut ryme_txn::Transaction, key: ryme_storage::RecordKey, value: Vec<u8>) {
        match self {
            Self::Single(manager) => manager.put(txn, key, value),
            Self::Cluster(backend) => backend.put(txn, key, value),
            Self::Sharded(shards) => shards.put(txn, key, value),
            Self::Hybrid(hybrid) => hybrid.put(txn, key, value),
        }
    }

    fn put_with_ttl(
        &self,
        txn: &mut ryme_txn::Transaction,
        key: ryme_storage::RecordKey,
        value: Vec<u8>,
        expires_at: u64,
    ) {
        match self {
            Self::Single(manager) => manager.put_with_ttl(txn, key, value, expires_at),
            Self::Cluster(backend) => backend.put_with_ttl(txn, key, value, expires_at),
            Self::Sharded(shards) => shards.put_with_ttl(txn, key, value, expires_at),
            Self::Hybrid(hybrid) => hybrid.put_with_ttl(txn, key, value, expires_at),
        }
    }

    fn delete(&self, txn: &mut ryme_txn::Transaction, key: ryme_storage::RecordKey) {
        match self {
            Self::Single(manager) => manager.delete(txn, key),
            Self::Cluster(backend) => backend.delete(txn, key),
            Self::Sharded(shards) => shards.delete(txn, key),
            Self::Hybrid(hybrid) => hybrid.delete(txn, key),
        }
    }

    fn expires_at(&self, key: &ryme_storage::RecordKey) -> ryme_error::Result<Option<u64>> {
        match self {
            Self::Single(manager) => manager.expires_at(key),
            Self::Cluster(backend) => backend.expires_at(key),
            Self::Sharded(shards) => shards.expires_at(key),
            Self::Hybrid(hybrid) => hybrid.expires_at(key),
        }
    }

    fn commit(
        &self,
        txn: ryme_txn::Transaction,
    ) -> impl std::future::Future<Output = ryme_error::Result<u64>> + Send {
        let backend = self.clone();
        async move {
            match backend {
                Self::Single(manager) => DurableManager::commit(&manager, txn),
                Self::Cluster(cluster) => cluster.commit(txn).await,
                Self::Sharded(shards) => shards.commit(txn).await,
                Self::Hybrid(hybrid) => hybrid.commit(txn).await,
            }
        }
    }

    fn scan(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> ryme_error::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::Single(manager) => manager.scan(txn, tenant, database, table, limit),
            Self::Cluster(backend) => backend.scan(txn, tenant, database, table, limit),
            Self::Sharded(shards) => shards.scan(txn, tenant, database, table, limit),
            Self::Hybrid(hybrid) => hybrid.scan(txn, tenant, database, table, limit),
        }
    }

    fn scan_after(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> ryme_error::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::Single(manager) => {
                manager.scan_after(txn, tenant, database, table, start_after, limit)
            }
            Self::Cluster(backend) => {
                backend.scan_after(txn, tenant, database, table, start_after, limit)
            }
            Self::Sharded(shards) => {
                shards.scan_after(txn, tenant, database, table, start_after, limit)
            }
            Self::Hybrid(hybrid) => {
                hybrid.scan_after(txn, tenant, database, table, start_after, limit)
            }
        }
    }
}

impl Backend {
    pub fn is_cluster(&self) -> bool {
        matches!(self, Self::Cluster(_))
    }

    pub async fn is_leader(&self) -> bool {
        match self {
            Self::Single(_) => true,
            Self::Cluster(backend) => backend.node().is_leader().await,
            Self::Sharded(_) => true,
            Self::Hybrid(hybrid) => hybrid.raft().node().is_leader().await,
        }
    }

    pub fn latest_commit(&self) -> u64 {
        match self {
            Self::Single(manager) => manager.inner().latest_commit(),
            Self::Cluster(backend) => backend.node().manager().latest_commit(),
            Self::Sharded(shards) => shards.latest_commit(),
            Self::Hybrid(hybrid) => {
                hybrid.raft().node().manager().latest_commit().max(hybrid.local().latest_commit())
            }
        }
    }

    pub fn gc_oldest(&self) -> ryme_error::Result<u64> {
        match self {
            Self::Single(manager) => manager.gc_oldest(),
            Self::Cluster(backend) => backend.gc_oldest(),
            Self::Sharded(shards) => shards.gc_oldest(),
            Self::Hybrid(hybrid) => hybrid.gc_oldest(),
        }
    }

    pub fn retention_sweep(&self, snapshot_keep: usize) -> ryme_error::Result<(usize, usize)> {
        match self {
            Self::Single(manager) => manager.retention_sweep(snapshot_keep),
            Self::Cluster(_) => Ok((0, 0)),
            Self::Sharded(shards) => shards.retention_sweep(snapshot_keep),
            Self::Hybrid(hybrid) => hybrid.retention_sweep(snapshot_keep),
        }
    }

    fn spaces(&self) -> ryme_error::Result<Vec<(String, String, String)>> {
        match self {
            Self::Single(manager) => manager.spaces(),
            Self::Cluster(backend) => backend.node().manager().spaces(),
            Self::Sharded(shards) => Ok(shards.spaces()),
            Self::Hybrid(hybrid) => {
                let mut out = hybrid.local().spaces();
                out.extend(hybrid.raft().node().manager().spaces().unwrap_or_default());
                out.sort();
                out.dedup();
                Ok(out)
            }
        }
    }

    pub fn stored_bytes_by_tenant(&self) -> HashMap<String, u64> {
        let mut out: HashMap<String, u64> = HashMap::new();
        let mut add = |tenant: String, bytes: u64| {
            let total = out.get(&tenant).copied().unwrap_or(0).saturating_add(bytes);
            out.insert(tenant, total);
        };
        match self {
            Self::Single(manager) => {
                for (tenant, database, table) in manager.spaces().unwrap_or_default() {
                    let bytes = manager.table_bytes(&tenant, &database, &table).unwrap_or(0);
                    add(tenant, bytes);
                }
            }
            Self::Cluster(backend) => {
                let manager = backend.node().manager();
                for (tenant, database, table) in manager.spaces().unwrap_or_default() {
                    let bytes = manager.table_bytes(&tenant, &database, &table).unwrap_or(0);
                    add(tenant, bytes);
                }
            }
            Self::Sharded(shards) => {
                for (table, _, bytes) in shards.tables() {
                    add(table.tenant, bytes);
                }
            }
            Self::Hybrid(hybrid) => {
                for (table, _, bytes) in hybrid.tables() {
                    add(table.tenant, bytes);
                }
            }
        }
        out
    }

    pub fn reconcile_qos(&self, qos: &Arc<Mutex<QosRegistry>>) {
        let usage = self.stored_bytes_by_tenant();
        if let Ok(mut registry) = qos.lock() {
            for (tenant, bytes) in usage {
                registry.set_stored_bytes(&tenant, bytes);
            }
        }
    }

    pub async fn sweep_table(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
        cdc: Option<&Realtime>,
    ) -> ryme_error::Result<usize> {
        use ryme_txn::TxnBackend;
        match self {
            Self::Single(manager) => {
                let expired = manager.expired_keys(tenant, database, table, limit)?;
                if expired.is_empty() {
                    return Ok(0);
                }
                let count = expired.len();
                let mut txn = manager.begin();
                for pk in &expired {
                    manager.delete(
                        &mut txn,
                        ryme_storage::RecordKey::new(tenant, database, table, pk),
                    );
                }
                let commit_ts = <DurableManager as TxnBackend>::commit(manager, txn).await?;
                Self::emit_swept(cdc, tenant, database, table, &expired, commit_ts);
                Ok(count)
            }
            Self::Cluster(backend) => {
                let expired =
                    backend.node().manager().expired_keys(tenant, database, table, limit)?;
                if expired.is_empty() {
                    return Ok(0);
                }
                let count = expired.len();
                let mut txn = backend.begin();
                for pk in &expired {
                    backend.delete(
                        &mut txn,
                        ryme_storage::RecordKey::new(tenant, database, table, pk),
                    );
                }
                let commit_ts = backend.commit(txn).await?;
                Self::emit_swept(cdc, tenant, database, table, &expired, commit_ts);
                Ok(count)
            }
            Self::Sharded(shards) => {
                let shard = shards.route_table(tenant, database, table);
                let manager = shards
                    .shard_manager(shard)
                    .ok_or_else(|| ryme_error::RymeError::Unavailable(String::from("shard")))?;
                let expired = manager.expired_keys(tenant, database, table, limit)?;
                if expired.is_empty() {
                    return Ok(0);
                }
                let count = expired.len();
                let mut txn = manager.begin();
                for pk in &expired {
                    manager.delete(
                        &mut txn,
                        ryme_storage::RecordKey::new(tenant, database, table, pk),
                    );
                }
                let commit_ts = <DurableManager as TxnBackend>::commit(&manager, txn).await?;
                Self::emit_swept(cdc, tenant, database, table, &expired, commit_ts);
                Ok(count)
            }
            Self::Hybrid(hybrid) => {
                let table_ref = TableRef::new(tenant, database, table);
                match hybrid.tier(&table_ref) {
                    ryme_shard::Tier::Replicated => {
                        let backend = hybrid.raft();
                        let expired = backend
                            .node()
                            .manager()
                            .expired_keys(tenant, database, table, limit)?;
                        if expired.is_empty() {
                            return Ok(0);
                        }
                        let count = expired.len();
                        let mut txn = backend.begin();
                        for pk in &expired {
                            backend.delete(
                                &mut txn,
                                ryme_storage::RecordKey::new(tenant, database, table, pk),
                            );
                        }
                        let commit_ts = backend.commit(txn).await?;
                        Self::emit_swept(cdc, tenant, database, table, &expired, commit_ts);
                        Ok(count)
                    }
                    ryme_shard::Tier::Local => {
                        let local = hybrid.local();
                        let shard = local.route_table(tenant, database, table);
                        let manager = local.shard_manager(shard).ok_or_else(|| {
                            ryme_error::RymeError::Unavailable(String::from("shard"))
                        })?;
                        let expired = manager.expired_keys(tenant, database, table, limit)?;
                        if expired.is_empty() {
                            return Ok(0);
                        }
                        let count = expired.len();
                        let mut txn = manager.begin();
                        for pk in &expired {
                            manager.delete(
                                &mut txn,
                                ryme_storage::RecordKey::new(tenant, database, table, pk),
                            );
                        }
                        let commit_ts =
                            <DurableManager as TxnBackend>::commit(&manager, txn).await?;
                        Self::emit_swept(cdc, tenant, database, table, &expired, commit_ts);
                        Ok(count)
                    }
                }
            }
        }
    }

    pub async fn sweep_once(&self, cdc: Option<&Realtime>) -> usize {
        let spaces = self.spaces().unwrap_or_default();
        let mut removed = 0;
        for (tenant, database, table) in spaces {
            match self.sweep_table(&tenant, &database, &table, 1000, cdc).await {
                Ok(count) => removed += count,
                Err(_) => continue,
            }
        }
        removed
    }

    fn emit_swept(
        cdc: Option<&Realtime>,
        tenant: &str,
        database: &str,
        table: &str,
        pks: &[Vec<u8>],
        commit_ts: u64,
    ) {
        let Some(realtime) = cdc else { return };
        for pk in pks {
            let _ = realtime.publish(ryme_realtime::NewChange {
                tenant: tenant.to_string(),
                database: database.to_string(),
                branch: String::from("main"),
                table: table.to_string(),
                op: ryme_realtime::Operation::Delete,
                pk: pk.clone(),
                before: None,
                after: None,
                commit_ts,
                tx_id: commit_ts,
            });
        }
    }
}

type BranchStorage = ryme_branch::BranchBackend<Backend>;

#[derive(Debug, Clone)]
pub struct SharedState {
    backend: Backend,
    durable: DurableManager,
    gateway: Gateway<BranchStorage>,
    executor: Executor<BranchStorage>,
    rls_tables: HashMap<String, String>,
    realtime: Realtime,
    durable_persist_lock: Arc<tokio::sync::Mutex<()>>,
    migration_lock: Arc<tokio::sync::Mutex<()>>,
    keys: ApiKeyStore,
    jwt: Option<JwtVerifier>,
    oidc: Option<OidcConfig>,
    oidc_jwt: Option<JwtVerifier>,
    control: Arc<Mutex<ControlPlane>>,
    latency: LatencyWindow,
    histogram: Histogram,
    slow_log: SlowLog,
    traces: Arc<Mutex<TraceCollector>>,
    metering: Arc<Mutex<MeterRegistry>>,
    qos: Arc<Mutex<QosRegistry>>,
    dek_ring: Arc<Mutex<KeyRing>>,
    indexes: Arc<Mutex<PartitionedIndex>>,
    credentials: Arc<Mutex<CredentialStore>>,
    refresh_tokens: RefreshTokenStore,
    passkeys: Arc<Mutex<PasskeyRegistry>>,
    native_listen: Option<std::net::SocketAddr>,
    grpc_tls_listen: Option<std::net::SocketAddr>,
    grpc_listen: Option<std::net::SocketAddr>,
    native_tls_listen: Option<std::net::SocketAddr>,
    https_listen: Option<std::net::SocketAddr>,
    resp_tls_listen: Option<std::net::SocketAddr>,
    tls: Option<ryme_wire_native::TlsAcceptor>,
    region: String,
    read_only: bool,
    passkey_rp_id: String,
    passkey_origins: Vec<String>,
    autosplit_writes: u64,
    archive: Option<ArchiveTarget>,
    archive_keep: usize,
    archive_replica: Option<ArchiveTarget>,
    snapshot_keep: usize,
    last_drill: Arc<Mutex<Option<DrillReport>>>,
    wal_dir: std::path::PathBuf,
    data_dir: std::path::PathBuf,
    started_unix: u64,
    node_id: String,
    tenant: String,
    database: String,
    branch_path: std::path::PathBuf,
    auth_path: std::path::PathBuf,
    control_path: std::path::PathBuf,
    durable_path: std::path::PathBuf,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct AuthSnapshot {
    #[serde(default)]
    api_keys: HashMap<String, Principal>,
    #[serde(default)]
    users: HashMap<String, ryme_auth::StoredUser>,
    #[serde(default)]
    refresh_tokens: HashMap<String, RefreshTokenRecord>,
    #[serde(default)]
    passkeys: HashMap<String, ryme_auth::PasskeyCredential>,
}

fn load_auth_snapshot(path: &std::path::Path) -> ryme_error::Result<AuthSnapshot> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(AuthSnapshot::default())
        }
        Err(error) => return Err(error.into()),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)?.permissions().mode();
        if mode & 0o077 != 0 {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("auth snapshot: {error}")))
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ControlSnapshot {
    #[serde(default)]
    backups: ryme_backup::BackupLog,
    #[serde(default)]
    migrations: ryme_migrate::Ledger,
    #[serde(default)]
    ranges: Vec<Range>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct DurableSnapshot {
    #[serde(default)]
    topics: Vec<ryme_realtime::DurableTopicSnapshot>,
}

fn load_durable_snapshot(
    path: &std::path::Path,
) -> ryme_error::Result<Vec<ryme_realtime::DurableTopicSnapshot>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)?.permissions().mode();
        if mode & 0o077 != 0 {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
    }
    let snapshot: DurableSnapshot = serde_json::from_slice(&bytes)
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("durable snapshot: {error}")))?;
    Ok(snapshot.topics)
}

fn load_schema_snapshot(path: &std::path::Path) -> ryme_error::Result<ryme_sql::SchemaSnapshot> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ryme_sql::SchemaSnapshot::default())
        }
        Err(error) => return Err(error.into()),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)?.permissions().mode();
        if mode & 0o077 != 0 {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("schema snapshot: {error}")))
}

fn load_control_snapshot(path: &std::path::Path) -> ryme_error::Result<ControlSnapshot> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ControlSnapshot::default())
        }
        Err(error) => return Err(error.into()),
    };
    let snapshot: ControlSnapshot = serde_json::from_slice(&bytes)
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("control snapshot: {error}")))?;
    snapshot
        .migrations
        .verify()
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("control ledger: {error}")))?;
    Ok(ControlSnapshot {
        backups: ryme_backup::BackupLog::from_checkpoints(snapshot.backups.checkpoints().to_vec()),
        migrations: snapshot.migrations,
        ranges: snapshot.ranges,
    })
}

#[derive(Debug, Clone)]
pub enum ArchiveTarget {
    Local(ryme_archive::local::LocalStore),
    S3(ryme_archive::s3::S3Store),
}

impl ArchiveTarget {
    async fn archive_files(
        &self,
        backup_id: &str,
        commit_ts: u64,
        files: Vec<(String, Vec<u8>)>,
    ) -> ryme_error::Result<BackupManifest> {
        match self {
            Self::Local(store) => {
                Archiver::new(store.clone()).archive_files(backup_id, commit_ts, files).await
            }
            Self::S3(store) => {
                Archiver::new(store.clone()).archive_files(backup_id, commit_ts, files).await
            }
        }
    }

    async fn archive_files_encrypted<F>(
        &self,
        backup_id: &str,
        commit_ts: u64,
        files: Vec<(String, Vec<u8>)>,
        seal: &F,
    ) -> ryme_error::Result<BackupManifest>
    where
        F: Fn(&[u8]) -> ryme_error::Result<(Vec<u8>, ryme_archive::FileEncryption)> + Send + Sync,
    {
        match self {
            Self::Local(store) => {
                Archiver::new(store.clone())
                    .archive_files_encrypted(backup_id, commit_ts, files, seal)
                    .await
            }
            Self::S3(store) => {
                Archiver::new(store.clone())
                    .archive_files_encrypted(backup_id, commit_ts, files, seal)
                    .await
            }
        }
    }

    async fn verify_backup<F>(&self, manifest_key: &str, open: &F) -> ryme_error::Result<u64>
    where
        F: Fn(&Option<ryme_archive::FileEncryption>, &[u8]) -> ryme_error::Result<Vec<u8>>
            + Send
            + Sync,
    {
        match self {
            Self::Local(store) => {
                Archiver::new(store.clone()).verify_backup(manifest_key, open).await
            }
            Self::S3(store) => Archiver::new(store.clone()).verify_backup(manifest_key, open).await,
        }
    }

    async fn restore_backup<F>(
        &self,
        manifest_key: &str,
        dest_dir: &std::path::Path,
        open: &F,
    ) -> ryme_error::Result<u64>
    where
        F: Fn(&Option<ryme_archive::FileEncryption>, &[u8]) -> ryme_error::Result<Vec<u8>>
            + Send
            + Sync,
    {
        match self {
            Self::Local(store) => {
                Archiver::new(store.clone()).restore_backup_with(manifest_key, dest_dir, open).await
            }
            Self::S3(store) => {
                Archiver::new(store.clone()).restore_backup_with(manifest_key, dest_dir, open).await
            }
        }
    }

    async fn list_backups(&self) -> ryme_error::Result<Vec<BackupManifest>> {
        match self {
            Self::Local(store) => Archiver::new(store.clone()).list_backups().await,
            Self::S3(store) => Archiver::new(store.clone()).list_backups().await,
        }
    }

    async fn copy_from(&self, source: &Self, manifest: &BackupManifest) -> ryme_error::Result<u64> {
        match (source, self) {
            (Self::Local(from), Self::Local(to)) => {
                ryme_archive::copy_backup(from, to, manifest).await
            }
            (Self::Local(from), Self::S3(to)) => {
                ryme_archive::copy_backup(from, to, manifest).await
            }
            (Self::S3(from), Self::Local(to)) => {
                ryme_archive::copy_backup(from, to, manifest).await
            }
            (Self::S3(from), Self::S3(to)) => ryme_archive::copy_backup(from, to, manifest).await,
        }
    }

    async fn prune(&self, keep: usize) -> ryme_error::Result<Vec<String>> {
        match self {
            Self::Local(store) => Archiver::new(store.clone()).prune(keep).await,
            Self::S3(store) => Archiver::new(store.clone()).prune(keep).await,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct StreamQuery {
    pub table: String,
    pub api_key: Option<String>,
    pub from: Option<u64>,
    pub from_sequence: Option<u64>,
    pub branch: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BroadcastStreamQuery {
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PresenceStreamQuery {
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SupabaseRealtimeQuery {
    pub apikey: Option<String>,
    pub api_key: Option<String>,
    pub vsn: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DurableStreamQuery {
    pub from: Option<u64>,
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct QueryStreamQuery {
    pub table: String,
    pub limit: Option<usize>,
    pub api_key: Option<String>,
    pub branch: Option<String>,
    pub select: Option<String>,
    pub order: Option<String>,
    #[serde(flatten)]
    pub filters: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
struct ReactiveQuerySpec {
    filters: Vec<(String, String)>,
    select: Option<String>,
    order: Option<String>,
}

impl ReactiveQuerySpec {
    fn from_query(query: &QueryStreamQuery) -> Self {
        let mut filters = query
            .filters
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        filters.sort_by(|left, right| left.0.cmp(&right.0));
        Self { filters, select: query.select.clone(), order: query.order.clone() }
    }

    fn is_reactive(&self) -> bool {
        !self.filters.is_empty() || self.select.is_some() || self.order.is_some()
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ScanQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RestListQuery {
    pub select: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct CopyRequest {
    pub table: String,
    pub rows: Vec<CopyRow>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct CopyRow {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ExplainRequest {
    pub sql: String,
    pub params: Option<Vec<String>>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct GraphqlRequest {
    pub query: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RestWriteBody {
    pub key: Option<String>,
    pub value: Option<serde_json::Value>,
    pub rows: Option<Vec<CopyRow>>,
    #[serde(flatten)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AutoscaleQuery {
    pub cpu_pct: Option<u8>,
    pub active_connections: Option<u64>,
    pub connection_limit: Option<u64>,
    pub shard_qps: Option<u64>,
    pub shard_qps_limit: Option<u64>,
    pub disk_used_pct: Option<u8>,
    pub follower_lag_ms: Option<u64>,
    pub realtime_sockets: Option<u64>,
    pub realtime_lag_ms: Option<u64>,
    pub compaction_debt_mb: Option<u64>,
    pub p99_queue_ms: Option<u64>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SqlRequest {
    pub sql: String,
    pub params: Option<Vec<String>>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BranchRequest {
    pub id: String,
    pub parent: String,
    pub base_commit_ts: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct CheckpointRequest {
    pub manifest_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PitrQuery {
    pub target: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ArchiveRequest {
    pub backup_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MoveShardRequest {
    pub tenant: Option<String>,
    pub database: Option<String>,
    pub table: String,
    pub target: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SplitRangeRequest {
    pub id: String,
    pub mid: String,
    pub left_id: String,
    pub right_id: String,
    pub expected_epoch: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MergeRangesRequest {
    pub left_id: String,
    pub right_id: String,
    pub merged_id: String,
    pub expected_left_epoch: u64,
    pub expected_right_epoch: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct VerifyRangeRequest {
    pub id: String,
    pub target: usize,
    pub expected_epoch: u64,
    pub max_rows: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TransferRangeRequest {
    pub id: String,
    pub target: usize,
    pub expected_epoch: u64,
    pub max_rows: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RangeLookup {
    pub key: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ClusterMemberRequest {
    pub id: usize,
    pub addr: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ClusterTransferRequest {
    pub target: usize,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ClusterReplaceRequest {
    pub members: Vec<ClusterMemberRequest>,
}

impl SharedState {
    fn branch_schema_path(&self, tenant: &str, branch: &str) -> std::path::PathBuf {
        let tenant = ryme_auth::base64_url_encode(tenant.as_bytes());
        let branch = ryme_auth::base64_url_encode(branch.as_bytes());
        self.data_dir.join("branch-schemas").join(tenant).join(format!("{branch}.json"))
    }

    fn persist_auth(&self) -> ryme_error::Result<()> {
        let snapshot = AuthSnapshot {
            api_keys: self.keys.snapshot()?,
            users: self
                .credentials
                .lock()
                .map_err(|_| ryme_error::RymeError::Internal(String::from("auth lock")))?
                .snapshot(),
            refresh_tokens: self.refresh_tokens.snapshot()?,
            passkeys: self
                .passkeys
                .lock()
                .map_err(|_| ryme_error::RymeError::Internal(String::from("auth lock")))?
                .snapshot(),
        };
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|error| ryme_error::RymeError::Internal(format!("auth snapshot: {error}")))?;
        if let Some(parent) = self.auth_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = self.auth_path.with_extension("tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_data()?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::rename(temporary, &self.auth_path)?;
        Ok(())
    }

    fn persist_control(&self) -> ryme_error::Result<()> {
        let control = self
            .control
            .lock()
            .map_err(|_| ryme_error::RymeError::Internal(String::from("lock")))?;
        self.persist_control_locked(&control)
    }

    fn persist_control_locked(&self, control: &ControlPlane) -> ryme_error::Result<()> {
        let snapshot = ControlSnapshot {
            backups: control.backups.clone(),
            migrations: control.migrations.clone(),
            ranges: control.ranges(),
        };
        let bytes = serde_json::to_vec(&snapshot).map_err(|error| {
            ryme_error::RymeError::Internal(format!("control snapshot: {error}"))
        })?;
        if let Some(parent) = self.control_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = self.control_path.with_extension("tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_data()?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::rename(temporary, &self.control_path)?;
        Ok(())
    }

    fn persist_durable_topics(&self) -> ryme_error::Result<()> {
        let snapshot = DurableSnapshot { topics: self.realtime.durable_snapshot()? };
        let bytes = serde_json::to_vec(&snapshot).map_err(|error| {
            ryme_error::RymeError::Internal(format!("durable snapshot: {error}"))
        })?;
        if let Some(parent) = self.durable_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = self.durable_path.with_extension("tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_data()?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::rename(temporary, &self.durable_path)?;
        Ok(())
    }

    pub fn build(config: &Config) -> ryme_error::Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        let wal_dir = config.data_dir.join("wal");
        let policy = match config.durability.is_durable() {
            true => SyncPolicy::Always,
            false => SyncPolicy::Never,
        };
        let durable = DurableManager::open_with_mode(
            &wal_dir,
            64 * 1024 * 1024,
            policy,
            config.cache_bytes,
            config.storage_mode,
        )?;
        let open_node = |dir: std::path::PathBuf| -> ryme_error::Result<std::sync::Arc<Node>> {
            let peers: Vec<String> = config.cluster.peers.iter().map(|p| p.addr.clone()).collect();
            let peer_ids: Vec<usize> = config.cluster.peers.iter().map(|p| p.id).collect();
            let self_addr = config.cluster.advertise_addr.clone().or_else(|| {
                config.cluster.raft_listen.and_then(|listen| {
                    if listen.ip().is_unspecified() {
                        None
                    } else {
                        Some(listen.to_string())
                    }
                })
            });
            Node::open_with_addr(
                config.cluster.node_index,
                peers,
                peer_ids,
                &dir,
                config.cluster.learner,
                self_addr,
            )
        };
        let backend = match config.cluster.raft_listen {
            Some(_) if config.replicated_tables.is_empty() => {
                let node = open_node(config.data_dir.join("raft"))?;
                Self::apply_mesh_tls(&node, config)?;
                Backend::Cluster(ClusterBackend::new(node))
            }
            Some(_) => {
                let node = open_node(config.data_dir.join("raft"))?;
                Self::apply_mesh_tls(&node, config)?;
                let replicated = config
                    .replicated_tables
                    .iter()
                    .map(|table| TableRef::new("default", "default", table))
                    .collect();
                let local = ryme_shard::ShardSet::open_with_mode(
                    &config.data_dir.join("local"),
                    1,
                    policy,
                    config.cache_bytes,
                    config.storage_mode,
                )?;
                Backend::Hybrid(HybridBackend::new(ClusterBackend::new(node), local, replicated))
            }
            None if config.shards > 1 => Backend::Sharded(ryme_shard::ShardSet::open_with_mode(
                &config.data_dir,
                config.shards,
                policy,
                config.cache_bytes,
                config.storage_mode,
            )?),
            None => Backend::Single(durable.clone()),
        };
        let realtime = Realtime::new(4096).with_stable_cdc();
        let durable_path = config.data_dir.join("topics.json");
        let durable_snapshot = load_durable_snapshot(&durable_path)?;
        realtime.restore_durable_snapshot(durable_snapshot)?;
        let mut policies = PolicyEngine::new();
        for (table, tenant_column) in &config.rls_tables {
            policies.allow_table(table.clone(), tenant_column.clone());
        }
        let tenant = String::from("default");
        let database = String::from("default");
        let branch = String::from("main");
        let storage = ryme_branch::BranchBackend::passthrough(backend.clone(), database.clone());
        let mut gateway = Gateway::with_backend(
            tenant.clone(),
            database.clone(),
            branch.clone(),
            policies,
            realtime.clone(),
            storage.clone(),
        );
        gateway.set_read_only(config.read_only);
        let schema_path = config.data_dir.join("schema.json");
        let schema_snapshot = load_schema_snapshot(&schema_path)?;
        let mut executor = Executor::with_backend(tenant.clone(), database.clone(), storage)
            .with_realtime(realtime.clone());
        executor.restore_schema_snapshot(schema_snapshot)?;
        executor.set_schema_path(schema_path);
        executor.set_rls_tables(config.rls_tables.clone());
        executor.set_read_only(config.read_only);
        let branch_path = config.data_dir.join("branches.json");
        let auth_path = config.data_dir.join("auth.json");
        let control_path = config.data_dir.join("control.json");
        let auth_snapshot = load_auth_snapshot(&auth_path)?;
        let control_snapshot = load_control_snapshot(&control_path)?;
        let has_persisted_ranges = !control_snapshot.ranges.is_empty();
        let mut control = ControlPlane::new();
        control.backups = control_snapshot.backups;
        control.migrations = control_snapshot.migrations;
        control.branches = ryme_branch::BranchManager::load(&branch_path)?;
        if control_snapshot.ranges.is_empty() {
            control.add_range(Range::new(
                String::from("range-0"),
                Vec::new(),
                Vec::new(),
                config.node_id.clone(),
                0,
            ));
        } else {
            control.restore_ranges(control_snapshot.ranges)?;
        }
        // Keep the synthetic control-plane range visible for the range API, but
        // do not activate data-plane range routing until an operator has split
        // or autosplit it. Fresh sharded deployments must still support
        // whole-table placement through /v1/shards/move.
        let placements = if !has_persisted_ranges {
            Vec::new()
        } else {
            control
                .ranges()
                .into_iter()
                .map(|range| ryme_shard::RangePlacement {
                    id: range.id,
                    start: range.start,
                    end: range.end,
                    shard: 0,
                })
                .collect()
        };
        match &backend {
            Backend::Sharded(shards) => shards.set_range_topology(placements)?,
            Backend::Hybrid(hybrid) => hybrid.local().set_range_topology(placements)?,
            _ => {}
        }
        if control.branches.list_for(&tenant).is_empty() {
            control.branches.create_root_for(
                &tenant,
                branch.clone(),
                ryme_branch::Manifest {
                    id: String::from("genesis"),
                    segments: Vec::new(),
                    wal_start: 0,
                },
            )?;
            control.branches.persist(&branch_path)?;
        }
        let keys = ApiKeyStore::from_snapshot(auth_snapshot.api_keys);
        let api_key =
            std::env::var("RYME_API_KEY").unwrap_or_else(|_| String::from("ryme-dev-key"));
        let mut roles = HashSet::new();
        roles.insert(Role::Owner);
        keys.insert(api_key, Principal { id: String::from("dev"), tenant: tenant.clone(), roles });
        let jwt = load_jwt_verifier()?;
        let archive = archive_target(&config.archive)?;
        let archive_replica = match config.archive_replica.clone() {
            Some(replica) => archive_target(&replica)?,
            None => None,
        };
        let dek_ring = load_or_create_ring(&config.data_dir, &tenant)?;
        let oidc_issuer = std::env::var("RYME_OIDC_ISSUER").ok();
        let oidc_audience = std::env::var("RYME_OIDC_AUDIENCE").ok();
        let oidc_secret = std::env::var("RYME_OIDC_SECRET").ok();
        let oidc_jwks_path = std::env::var_os("RYME_OIDC_JWKS_FILE")
            .or_else(|| std::env::var_os("RYME_JWT_JWKS_FILE"));
        let oidc_jwks_url = if oidc_jwks_path.is_none() {
            std::env::var("RYME_OIDC_JWKS_URL")
                .ok()
                .or_else(|| std::env::var("RYME_JWT_JWKS_URL").ok())
        } else {
            None
        };
        let oidc_enabled =
            oidc_secret.is_some() || oidc_jwks_path.is_some() || oidc_jwks_url.is_some();
        let oidc = match (oidc_issuer, oidc_audience) {
            (Some(issuer), Some(audience)) if oidc_enabled => Some(OidcConfig {
                issuer,
                audience,
                client_secret: oidc_secret.unwrap_or_default().into_bytes(),
                auth_endpoint: std::env::var("RYME_OIDC_AUTH_ENDPOINT").unwrap_or_default(),
                client_id: std::env::var("RYME_OIDC_CLIENT_ID").unwrap_or_default(),
            }),
            (None, None) if !oidc_enabled => None,
            _ if oidc_enabled => {
                return Err(ryme_error::RymeError::InvalidArgument(String::from(
                    "oidc issuer and audience are required",
                )))
            }
            _ => None,
        };
        let oidc_kid = std::env::var("RYME_OIDC_JWK_KID")
            .ok()
            .or_else(|| std::env::var("RYME_JWT_JWK_KID").ok());
        let oidc_jwt = match (&oidc, oidc_jwks_path, oidc_jwks_url) {
            (Some(config), Some(path), _) => Some(load_jwks_verifier(
                std::path::Path::new(&path),
                oidc_kid,
                Some(config.issuer.clone()),
                Some(config.audience.clone()),
            )?),
            (Some(config), None, Some(url)) => Some(load_jwks_verifier_url(
                &url,
                oidc_kid,
                Some(config.issuer.clone()),
                Some(config.audience.clone()),
            )?),
            (None, Some(_), _) | (None, None, Some(_)) => {
                return Err(ryme_error::RymeError::InvalidArgument(String::from(
                    "oidc issuer and audience are required with a jwks file",
                )))
            }
            _ => None,
        };
        Ok(Self {
            backend,
            durable,
            gateway,
            executor,
            rls_tables: config.rls_tables.clone(),
            realtime,
            durable_persist_lock: Arc::new(tokio::sync::Mutex::new(())),
            migration_lock: Arc::new(tokio::sync::Mutex::new(())),
            keys,
            jwt,
            oidc,
            oidc_jwt,
            control: Arc::new(Mutex::new(control)),
            latency: LatencyWindow::new(),
            histogram: Histogram::new(1024),
            slow_log: SlowLog::new(256),
            traces: Arc::new(Mutex::new(TraceCollector::new(256))),
            metering: Arc::new(Mutex::new(MeterRegistry::new())),
            qos: Arc::new(Mutex::new(QosRegistry::new())),
            dek_ring,
            indexes: Arc::new(Mutex::new(PartitionedIndex::new(config.index_partitions))),
            credentials: Arc::new(Mutex::new(CredentialStore::from_snapshot(auth_snapshot.users))),
            refresh_tokens: RefreshTokenStore::from_snapshot(auth_snapshot.refresh_tokens),
            passkeys: Arc::new(Mutex::new(PasskeyRegistry::from_snapshot(auth_snapshot.passkeys))),
            native_listen: config.native_listen,
            grpc_tls_listen: config.grpc_tls_listen,
            grpc_listen: config.grpc_listen,
            native_tls_listen: config.native_tls_listen,
            https_listen: config.https_listen,
            resp_tls_listen: config.resp_tls_listen,
            tls: match (&config.tls_cert_pem, &config.tls_key_pem, &config.tls_client_ca_pem) {
                (Some(cert), Some(key), Some(ca)) => {
                    Some(ryme_wire_native::TlsAcceptor::from_pem_files_mutual(cert, key, ca)?)
                }
                (Some(cert), Some(key), None) => {
                    Some(ryme_wire_native::TlsAcceptor::from_pem_files(cert, key)?)
                }
                (None, None, None) => None,
                _ => {
                    return Err(ryme_error::RymeError::InvalidArgument(String::from(
                        "tls cert/key",
                    )))
                }
            },
            region: config.region.clone(),
            read_only: config.read_only,
            passkey_rp_id: config.passkey_rp_id.clone(),
            passkey_origins: config.passkey_origins.clone(),
            autosplit_writes: config.autosplit_writes,
            archive,
            archive_keep: config.archive.keep,
            archive_replica,
            snapshot_keep: config.archive.snapshot_keep,
            last_drill: Arc::new(Mutex::new(None)),
            wal_dir,
            data_dir: config.data_dir.clone(),
            started_unix: now_secs(),
            node_id: config.node_id.clone(),
            tenant,
            database,
            branch_path,
            auth_path,
            control_path,
            durable_path,
        })
    }

    fn apply_mesh_tls(
        node: &std::sync::Arc<ryme_raft::net::Node>,
        config: &Config,
    ) -> ryme_error::Result<()> {
        if !config.raft_tls {
            return Ok(());
        }
        let (Some(cert), Some(key)) = (config.tls_cert_pem.as_ref(), config.tls_key_pem.as_ref())
        else {
            return Err(ryme_error::RymeError::InvalidArgument(String::from("tls cert/key")));
        };
        let acceptor = match config.tls_client_ca_pem.as_ref() {
            Some(ca) => ryme_tls::TlsAcceptor::from_pem_files_mutual(cert, key, ca)?,
            None => ryme_tls::TlsAcceptor::from_pem_files(cert, key)?,
        };
        let connector = match config.tls_client_ca_pem.as_ref() {
            Some(ca) => ryme_tls::MeshConnector::from_pem_files(ca, Some(cert), Some(key))?,
            None => {
                return Err(ryme_error::RymeError::InvalidArgument(String::from("tls client ca")));
            }
        };
        node.set_mesh_tls(ryme_raft::net::MeshTransport { acceptor, connector });
        Ok(())
    }

    pub fn durable(&self) -> &DurableManager {
        &self.durable
    }

    pub fn realtime(&self) -> &Realtime {
        &self.realtime
    }

    pub fn control(&self) -> &Arc<Mutex<ControlPlane>> {
        &self.control
    }

    pub fn raft_node(&self) -> Option<std::sync::Arc<Node>> {
        match &self.backend {
            Backend::Cluster(backend) => Some(backend.node().clone()),
            Backend::Hybrid(hybrid) => Some(hybrid.raft().node().clone()),
            Backend::Single(_) | Backend::Sharded(_) => None,
        }
    }

    async fn archive_now(&self, backup_id: Option<String>) -> ryme_error::Result<BackupManifest> {
        let target = self
            .archive
            .clone()
            .ok_or_else(|| ryme_error::RymeError::InvalidArgument(String::from("archive")))?;
        let snapshots = snapshot_backend_files(self).await?;
        let mut files = Vec::new();
        let mut commit = 0u64;
        for (snapshot_commit, snapshot_path) in snapshots {
            commit = commit.max(snapshot_commit);
            let snapshot_bytes = std::fs::read(&snapshot_path)?;
            let snapshot_name = snapshot_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| ryme_error::RymeError::Internal(String::from("snapshot name")))?
                .to_string();
            files.push((snapshot_name, snapshot_bytes));
        }
        files.extend(archive_metadata_files(&self.data_dir)?);
        let log_dirs: Vec<(String, std::path::PathBuf)> = match &self.backend {
            Backend::Single(_) => vec![(String::from("wal-"), self.wal_dir.clone())],
            Backend::Cluster(_) => vec![(String::from("raft-"), self.data_dir.join("raft"))],
            Backend::Sharded(shards) => (0..shards.shard_count())
                .filter_map(|index| {
                    shards.shard_wal_dir(index).map(|dir| (format!("shard-{index}-"), dir))
                })
                .collect(),
            Backend::Hybrid(hybrid) => {
                let mut dirs = vec![(String::from("raft-"), self.data_dir.join("raft"))];
                dirs.extend((0..hybrid.local().shard_count()).filter_map(|index| {
                    hybrid.local().shard_wal_dir(index).map(|dir| (format!("local-{index}-"), dir))
                }));
                dirs
            }
        };
        for (prefix, log_dir) in log_dirs {
            for path in wal_segment_files(&log_dir)? {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| ryme_error::RymeError::Internal(String::from("segment name")))?
                    .to_string();
                files.push((format!("{prefix}{name}"), std::fs::read(&path)?));
            }
            for path in immutable_segment_files(&log_dir)? {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| ryme_error::RymeError::Internal(String::from("segment name")))?
                    .to_string();
                files.push((format!("{prefix}sst-{name}"), std::fs::read(&path)?));
            }
        }
        let id = backup_id.unwrap_or_else(|| format!("auto-{commit}"));
        let manifest = match self.active_dek() {
            Some(dek) => {
                target
                    .archive_files_encrypted(&id, commit, files, &|bytes| {
                        let envelope = ryme_crypto::seal_with(&dek, bytes);
                        Ok((
                            ryme_auth::base64_url_decode(&envelope.blob_b64).map_err(|_| {
                                ryme_error::RymeError::Internal(String::from("envelope"))
                            })?,
                            ryme_archive::FileEncryption {
                                dek_id: envelope.dek_id.clone(),
                                nonce_b64: envelope.nonce_b64.clone(),
                                tag_b64: envelope.tag_b64.clone(),
                            },
                        ))
                    })
                    .await?
            }
            None => target.archive_files(&id, commit, files).await?,
        };
        let _ = target.prune(self.archive_keep).await;
        Ok(manifest)
    }

    fn active_dek(&self) -> Option<ryme_crypto::Dek> {
        self.dek_ring.lock().ok()?.active(&self.tenant).or_else(|| {
            let mut ring = self.dek_ring.lock().ok()?;
            let dek = ring.rotate(&self.tenant.clone(), now_secs());
            let path = self.data_dir.join("keyring.json");
            let _ = persist_ring(&ring, &path);
            Some(dek)
        })
    }

    fn principal(&self, headers: &HeaderMap) -> ryme_error::Result<Principal> {
        if let Some(value) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
            return self.keys.authenticate(value);
        }
        if let Some(value) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
            if let Some(token) = value.strip_prefix("Bearer ") {
                if let Ok(principal) = self.keys.authenticate(token) {
                    return Ok(principal);
                }
                if let Some(verifier) = self.jwt.as_ref() {
                    return verifier.principal_from_token(token, now_secs());
                }
                return Err(ryme_error::RymeError::Unauthorized);
            }
        }
        Err(ryme_error::RymeError::Unauthorized)
    }

    fn principal_with_query(
        &self,
        headers: &HeaderMap,
        api_key: Option<&str>,
    ) -> ryme_error::Result<Principal> {
        if let Some(key) = api_key {
            if let Ok(principal) = self.keys.authenticate(key) {
                return Ok(principal);
            }
        }
        self.principal(headers)
    }
}

pub fn router(state: SharedState) -> axum::Router {
    axum::Router::new()
        .route("/health", get(health))
        .route("/dashboard", get(dashboard))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/metrics/prometheus", get(prometheus))
        .route("/v1/observe/slow", get(slow_log))
        .route("/v1/traces", get(traces))
        .route("/v1/kv/:table/:key", get(kv_get).put(kv_put).delete(kv_delete))
        .route("/v1/kv/:table/:key/ttl", get(kv_ttl))
        .route("/v1/sql", post(sql_exec))
        .route("/v1/sql/copy", post(sql_copy))
        .route("/v1/sql/explain", post(sql_explain))
        .route("/v1/scan/:table", get(scan))
        .route(
            "/rest/v1/:table",
            get(rest_list)
                .post(rest_insert)
                .patch(rest_upsert)
                .delete(rest_delete)
                .options(cors_options),
        )
        .route("/graphql", post(graphql_exec).options(cors_options))
        .route("/v1/metering", get(metering_snapshot))
        .route("/v1/billing/summary", get(billing_summary))
        .route("/v1/billing/invoice", get(billing_invoice))
        .route("/v1/migrate/supabase", post(migrate_supabase))
        .route("/v1/migrate/neon", post(migrate_neon))
        .route("/v1/migrate/apply", post(migrate_apply))
        .route("/v1/migrate/ledger", get(migrate_ledger))
        .route("/v1/autoscale", get(autoscale_advice))
        .route("/v1/qos", get(qos_snapshot))
        .route("/v1/qos/tier", post(qos_set_tier))
        .route("/v1/auth/register", post(auth_register))
        .route("/v1/auth/verify", post(auth_verify))
        .route("/v1/auth/token", post(auth_token))
        .route("/v1/auth/keys", delete(auth_revoke))
        .route("/v1/auth/otp/setup", post(otp_setup))
        .route("/v1/auth/otp/verify", post(otp_verify))
        .route("/v1/auth/passkey/challenge", post(passkey_challenge))
        .route("/v1/auth/passkey/register", post(passkey_register))
        .route("/v1/auth/passkey/verify", post(passkey_verify))
        .route("/v1/auth/oidc/login", post(oidc_login))
        .route("/v1/auth/oidc/token", post(oidc_token))
        .route("/v1/auth/mask", post(mask_set))
        .route("/v1/presence/join", post(presence_join))
        .route("/v1/presence/leave", post(presence_leave))
        .route("/v1/presence/:channel/stream", get(presence_stream))
        .route("/v1/presence/:channel", get(presence_list))
        .route("/v1/broadcast", post(broadcast_post))
        .route("/v1/broadcast/:channel", get(broadcast_stream))
        .route("/realtime/v1/websocket", get(supabase_realtime_stream))
        .route("/v1/topics/append", post(durable_append))
        .route("/v1/topics/read", get(durable_read))
        .route("/v1/topics/:partition/stream", get(durable_stream))
        .route("/v1/vector/upsert", post(vector_upsert))
        .route("/v1/vector/search", post(vector_search))
        .route("/v1/vector/ann-search", post(vector_ann_search))
        .route("/v1/vector/:table/:id", delete(vector_delete))
        .route("/v1/text/index", post(text_index))
        .route("/v1/text/search", post(text_search))
        .route("/v1/text/:table/:id", delete(text_delete))
        .route("/v1/index/stats", get(index_stats))
        .route("/v1/branches", post(branch_create).get(branch_list))
        .route("/v1/branches/:id", get(branch_get).delete(branch_delete))
        .route("/v1/branches/:id/reset", post(branch_reset))
        .route("/v1/branches/:id/promote", post(branch_promote))
        .route("/v1/branches/:id/diff", get(branch_diff))
        .route("/v1/backups/checkpoint", post(checkpoint_create))
        .route("/v1/backups/latest", get(checkpoint_latest))
        .route("/v1/backups/pitr", get(checkpoint_pitr))
        .route("/v1/snapshots", post(snapshot_create))
        .route("/v1/backups/restore", post(backup_restore))
        .route("/v1/backups/archive", post(backup_archive))
        .route("/v1/backups/archives", get(backup_archives))
        .route("/v1/backups/copy", post(backup_copy))
        .route("/v1/backups/verify", get(backup_verify))
        .route("/v1/backups/drill", get(drill_status))
        .route("/v1/shards", get(shard_layout))
        .route("/v1/shards/move", post(shard_move))
        .route("/v1/ranges", get(range_list))
        .route("/v1/ranges/loads", get(range_loads))
        .route("/v1/ranges/split", post(range_split))
        .route("/v1/ranges/merge", post(range_merge))
        .route("/v1/ranges/autosplit", post(range_autosplit))
        .route("/v1/ranges/verify", post(range_verify))
        .route("/v1/ranges/transfer", post(range_transfer))
        .route("/v1/cluster/members", get(cluster_members).post(cluster_add_member))
        .route("/v1/cluster/members/:id", delete(cluster_remove_member))
        .route("/v1/cluster/transfer", post(cluster_transfer))
        .route("/v1/cluster/replace", post(cluster_replace))
        .route("/v1/regions", get(regions))
        .route("/v1/stream", get(stream))
        .route("/v1/query-stream", get(query_stream))
        .layer(axum::extract::DefaultBodyLimit::max(2 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(state.clone(), read_only_guard))
        .layer(axum::middleware::from_fn(cors_headers))
        .with_state(state)
}

async fn cors_options(headers: HeaderMap) -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    apply_cors_headers(&mut response, headers.get("origin"));
    response
}

async fn cors_headers(request: axum::http::Request<axum::body::Body>, next: Next) -> Response {
    let origin = request.headers().get("origin").cloned();
    if request.method() == axum::http::Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        apply_cors_headers(&mut response, origin.as_ref());
        return response;
    }
    let mut response = next.run(request).await;
    apply_cors_headers(&mut response, origin.as_ref());
    response
}

fn apply_cors_headers(response: &mut Response, origin: Option<&HeaderValue>) {
    response.headers_mut().insert(
        "access-control-allow-origin",
        origin.cloned().unwrap_or_else(|| HeaderValue::from_static("*")),
    );
    response.headers_mut().insert("vary", HeaderValue::from_static("Origin"));
    response.headers_mut().insert(
        "access-control-allow-methods",
        HeaderValue::from_static("GET, POST, PUT, PATCH, DELETE, OPTIONS"),
    );
    response.headers_mut().insert(
        "access-control-allow-headers",
        HeaderValue::from_static(
            "authorization, content-type, apikey, x-api-key, x-client-info, prefer, x-ryme-branch",
        ),
    );
    response.headers_mut().insert(
        "access-control-expose-headers",
        HeaderValue::from_static("content-range, range-unit, content-type"),
    );
    response.headers_mut().insert("access-control-max-age", HeaderValue::from_static("600"));
}

async fn read_only_guard(
    State(state): State<SharedState>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    if state.read_only && is_mutating(request.method(), request.uri().path()) {
        return error_response(ryme_error::RymeError::ReadOnly(String::from("read-only follower")));
    }
    next.run(request).await
}

fn is_mutating(method: &axum::http::Method, path: &str) -> bool {
    if method == axum::http::Method::GET
        || method == axum::http::Method::HEAD
        || method == axum::http::Method::OPTIONS
    {
        return false;
    }
    !matches!(
        path,
        "/v1/sql"
            | "/v1/sql/explain"
            | "/graphql"
            | "/v1/auth/verify"
            | "/v1/auth/otp/verify"
            | "/v1/auth/token"
    )
}

async fn regions(State(state): State<SharedState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "region": state.region,
        "read_only": state.read_only,
        "node": state.node_id,
        "commit": state.backend.latest_commit(),
        "leader": state.backend.is_leader().await,
    }))
}

#[derive(Default)]
pub struct OptionalListeners {
    pub native: Option<TcpListener>,
    pub resp_tls: Option<TcpListener>,
    pub native_tls: Option<TcpListener>,
    pub grpc: Option<TcpListener>,
    pub grpc_tls: Option<TcpListener>,
    pub https: Option<TcpListener>,
}

impl OptionalListeners {
    pub fn with_native(listener: TcpListener) -> Self {
        Self { native: Some(listener), ..Self::default() }
    }
}

pub async fn serve(
    config: Config,
    pg_listener: TcpListener,
    resp_listener: TcpListener,
    http_listener: TcpListener,
) -> ryme_error::Result<()> {
    let state = SharedState::build(&config)?;
    serve_state(
        state,
        config.archive.interval_secs,
        config.archive.verify_interval_secs,
        config.sweep_interval_secs,
        config.max_connections as usize,
        config.otel.clone(),
        config.autosplit_writes,
        config.autosplit_interval_secs,
        pg_listener,
        resp_listener,
        http_listener,
        OptionalListeners::default(),
    )
    .await
}

pub async fn serve_with_native(
    config: Config,
    pg_listener: TcpListener,
    resp_listener: TcpListener,
    http_listener: TcpListener,
    native_listener: TcpListener,
) -> ryme_error::Result<()> {
    serve_with_optional(
        config,
        pg_listener,
        resp_listener,
        http_listener,
        OptionalListeners::with_native(native_listener),
    )
    .await
}

pub async fn serve_with_optional(
    config: Config,
    pg_listener: TcpListener,
    resp_listener: TcpListener,
    http_listener: TcpListener,
    extra: OptionalListeners,
) -> ryme_error::Result<()> {
    let state = SharedState::build(&config)?;
    serve_state(
        state,
        config.archive.interval_secs,
        config.archive.verify_interval_secs,
        config.sweep_interval_secs,
        config.max_connections as usize,
        config.otel.clone(),
        config.autosplit_writes,
        config.autosplit_interval_secs,
        pg_listener,
        resp_listener,
        http_listener,
        extra,
    )
    .await
}

pub struct ClusterHandle {
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
    pub node: std::sync::Arc<Node>,
}

impl ClusterHandle {
    pub fn shutdown(self) {
        let node = self.node.clone();
        node.shutdown(self.tasks);
    }
}

pub async fn serve_cluster(
    config: Config,
    pg_listener: TcpListener,
    resp_listener: TcpListener,
    http_listener: TcpListener,
    raft_listener: TcpListener,
) -> ryme_error::Result<ClusterHandle> {
    let state = SharedState::build(&config)?;
    let node = match state.backend.clone() {
        Backend::Cluster(backend) => backend.node().clone(),
        Backend::Hybrid(hybrid) => hybrid.raft().node().clone(),
        Backend::Single(_) | Backend::Sharded(_) => {
            return Err(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
        }
    };
    install_cluster_range_replication(&state, &node)?;
    install_cluster_data_replication(&state, &node)?;
    install_cluster_realtime_replication(&state, &node)?;
    install_cluster_presence_replication(&state, &node)?;
    install_cluster_topic_replication(&state, &node)?;
    node.replay_topics().await?;
    let mut tasks = node.spawn(raft_listener);
    tasks.extend(
        spawn_gateways(
            state,
            config.archive.interval_secs,
            config.archive.verify_interval_secs,
            config.sweep_interval_secs,
            config.max_connections as usize,
            config.otel.clone(),
            config.autosplit_writes,
            config.autosplit_interval_secs,
            pg_listener,
            resp_listener,
            http_listener,
            OptionalListeners::default(),
        )
        .into_vec(),
    );
    Ok(ClusterHandle { tasks, node })
}

pub struct GatewayTasks {
    pub pg: tokio::task::JoinHandle<()>,
    pub resp: tokio::task::JoinHandle<()>,
    pub resp_tls: tokio::task::JoinHandle<()>,
    pub native: tokio::task::JoinHandle<()>,
    pub native_tls: tokio::task::JoinHandle<()>,
    pub http: tokio::task::JoinHandle<()>,
    pub https: tokio::task::JoinHandle<()>,
    pub archive: tokio::task::JoinHandle<()>,
    pub drill: tokio::task::JoinHandle<()>,
    pub sweep: tokio::task::JoinHandle<()>,
    pub otel: tokio::task::JoinHandle<()>,
    pub grpc: tokio::task::JoinHandle<()>,
    pub grpc_tls: tokio::task::JoinHandle<()>,
    pub autosplit: tokio::task::JoinHandle<()>,
}

impl GatewayTasks {
    pub fn into_vec(self) -> Vec<tokio::task::JoinHandle<()>> {
        vec![
            self.pg,
            self.resp,
            self.resp_tls,
            self.native,
            self.native_tls,
            self.http,
            self.https,
            self.archive,
            self.drill,
            self.sweep,
            self.otel,
            self.grpc,
            self.grpc_tls,
            self.autosplit,
        ]
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_gateways(
    state: SharedState,
    archive_interval: u64,
    verify_interval: u64,
    sweep_interval: u64,
    max_connections: usize,
    otel: OtelConfig,
    autosplit_writes: u64,
    autosplit_interval: u64,
    pg_listener: TcpListener,
    resp_listener: TcpListener,
    http_listener: TcpListener,
    extra: OptionalListeners,
) -> GatewayTasks {
    let mut pg_executor = state.executor.clone().with_tenant(state.tenant.clone());
    pg_executor.set_rls_tables(state.rls_tables.clone());
    pg_executor.set_read_only(state.read_only);
    let range_hook: ryme_router::RangeLoadHook = if autosplit_writes > 0 {
        let control = state.control.clone();
        ryme_router::RangeLoadHook::armed(Arc::new(move |key: &[u8], count: u64| {
            if let Ok(control) = control.lock() {
                let _ = control.router.note_write(key, count);
            }
        }))
    } else {
        ryme_router::RangeLoadHook::default()
    };
    let mut pg = match state.tls.clone() {
        Some(acceptor) => ryme_wire_pg::PgGateway::with_backend_tls(pg_executor, acceptor),
        None => ryme_wire_pg::PgGateway::with_backend_executor(pg_executor),
    };
    if let Ok(expected_password) = std::env::var("RYME_PG_PASSWORD") {
        let keys = state.keys.clone();
        let jwt = state.jwt.clone();
        let default_tenant = state.tenant.clone();
        pg = pg.with_authenticator(move |_user, password| {
            wire_tenant(&expected_password, &keys, jwt.as_ref(), &default_tenant, password)
        });
    }
    let pg = pg
        .with_qos(state.qos.clone())
        .with_metering(state.metering.clone())
        .with_observe(state.latency.clone(), state.histogram.clone(), state.slow_log.clone())
        .with_range_hook(range_hook.clone());
    let mut resp = ryme_wire_resp::RespGateway::with_backend(
        state.tenant.clone(),
        state.database.clone(),
        state.backend.clone(),
    );
    let resp_node = state.raft_node();
    if let Some(authenticator) = resp_authenticator(&state) {
        resp = resp.with_authenticator(move |user, password| authenticator(user, password));
    }
    let resp = resp
        .with_realtime(state.realtime.clone())
        .with_realtime_replicator(move |payload| {
            if let Some(node) = resp_node.clone() {
                tokio::spawn(async move {
                    let _ = node.fanout_realtime_peers(payload).await;
                });
            }
        })
        .with_qos(state.qos.clone())
        .with_metering(state.metering.clone())
        .with_observe(state.latency.clone(), state.histogram.clone(), state.slow_log.clone())
        .with_range_hook(range_hook.clone())
        .with_read_only(state.read_only);
    let app = router(state.clone());
    let OptionalListeners {
        native: prebound_native,
        resp_tls: prebound_resp_tls,
        native_tls: prebound_native_tls,
        grpc: prebound_grpc,
        grpc_tls: prebound_grpc_tls,
        https: prebound_https,
    } = extra;
    let archive_state = state.clone();
    let archive_task = tokio::spawn(async move {
        if archive_state.archive.is_none() || archive_interval == 0 {
            futures_util::future::pending::<()>().await;
            return;
        }
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(archive_interval)).await;
            match archive_state.archive_now(None).await {
                Ok(manifest) => {
                    tracing::info!(commit = manifest.commit_ts, "archived backup");
                }
                Err(e) => {
                    tracing::warn!("background archive failed: {e}");
                }
            }
        }
    });
    let drill_state = state.clone();
    let drill_task = tokio::spawn(async move {
        if drill_state.archive.is_none() || verify_interval == 0 {
            futures_util::future::pending::<()>().await;
            return;
        }
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(verify_interval)).await;
            let report = drill_once(&drill_state).await;
            if let Ok(mut guard) = drill_state.last_drill.lock() {
                *guard = Some(report);
            }
        }
    });
    let pg_task = tokio::spawn(async move {
        let _ = pg.serve_limited(pg_listener, max_connections).await;
    });
    let resp_task = tokio::spawn(async move {
        let _ = resp.serve_limited(resp_listener, max_connections).await;
    });
    let resp_tls_state = state.clone();
    let resp_tls_hook = range_hook.clone();
    let prebound_resp_tls = std::sync::Arc::new(tokio::sync::Mutex::new(prebound_resp_tls));
    let resp_tls_task = tokio::spawn(async move {
        let listener = match prebound_resp_tls.lock().await.take() {
            Some(listener) => listener,
            None => {
                let (Some(addr), Some(_)) =
                    (resp_tls_state.resp_tls_listen, resp_tls_state.tls.clone())
                else {
                    futures_util::future::pending::<()>().await;
                    return;
                };
                match bind_retry(addr).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        tracing::warn!("resp tls listener bind failed: {e}");
                        futures_util::future::pending::<()>().await;
                        return;
                    }
                }
            }
        };
        let acceptor = match resp_tls_state.tls.clone() {
            Some(acceptor) => acceptor,
            None => {
                futures_util::future::pending::<()>().await;
                return;
            }
        };
        let mut gateway = ryme_wire_resp::RespGateway::with_backend(
            resp_tls_state.tenant.clone(),
            resp_tls_state.database.clone(),
            resp_tls_state.backend.clone(),
        );
        if let Some(authenticator) = resp_authenticator(&resp_tls_state) {
            gateway =
                gateway.with_authenticator(move |user, password| authenticator(user, password));
        }
        let resp_tls_node = resp_tls_state.raft_node();
        let gateway = gateway
            .with_realtime(resp_tls_state.realtime.clone())
            .with_realtime_replicator(move |payload| {
                if let Some(node) = resp_tls_node.clone() {
                    tokio::spawn(async move {
                        let _ = node.fanout_realtime_peers(payload).await;
                    });
                }
            })
            .with_qos(resp_tls_state.qos.clone())
            .with_metering(resp_tls_state.metering.clone())
            .with_observe(
                resp_tls_state.latency.clone(),
                resp_tls_state.histogram.clone(),
                resp_tls_state.slow_log.clone(),
            )
            .with_range_hook(resp_tls_hook)
            .with_read_only(resp_tls_state.read_only);
        let _ = gateway.serve_tls(listener, max_connections, acceptor).await;
    });
    let native_state = state.clone();
    let native_hook = range_hook.clone();
    let prebound_native = std::sync::Arc::new(tokio::sync::Mutex::new(prebound_native));
    let native_task = tokio::spawn(async move {
        let listener = match prebound_native.lock().await.take() {
            Some(listener) => listener,
            None => {
                let Some(addr) = native_state.native_listen else {
                    futures_util::future::pending::<()>().await;
                    return;
                };
                match bind_retry(addr).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        tracing::warn!("native listener bind failed: {e}");
                        futures_util::future::pending::<()>().await;
                        return;
                    }
                }
            }
        };
        let native = ryme_wire_native::NativeGateway::with_qos(
            native_state.gateway.clone(),
            native_state.keys.clone(),
            native_state.qos.clone(),
        )
        .with_metering(native_state.metering.clone())
        .with_observe(
            native_state.latency.clone(),
            native_state.histogram.clone(),
            native_state.slow_log.clone(),
        )
        .with_range_hook(native_hook);
        let _ = native.serve_limited(listener, max_connections).await;
    });
    let tls_state = state.clone();
    let tls_hook = range_hook.clone();
    let prebound_native_tls = std::sync::Arc::new(tokio::sync::Mutex::new(prebound_native_tls));
    let tls_task = tokio::spawn(async move {
        let listener = match prebound_native_tls.lock().await.take() {
            Some(listener) => listener,
            None => {
                let (Some(addr), Some(_)) = (tls_state.native_tls_listen, tls_state.tls.clone())
                else {
                    futures_util::future::pending::<()>().await;
                    return;
                };
                match bind_retry(addr).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        tracing::warn!("native tls listener bind failed: {e}");
                        futures_util::future::pending::<()>().await;
                        return;
                    }
                }
            }
        };
        let acceptor = match tls_state.tls.clone() {
            Some(acceptor) => acceptor,
            None => {
                futures_util::future::pending::<()>().await;
                return;
            }
        };
        let native = ryme_wire_native::NativeGateway::with_qos(
            tls_state.gateway.clone(),
            tls_state.keys.clone(),
            tls_state.qos.clone(),
        )
        .with_metering(tls_state.metering.clone())
        .with_observe(
            tls_state.latency.clone(),
            tls_state.histogram.clone(),
            tls_state.slow_log.clone(),
        )
        .with_range_hook(tls_hook);
        let _ = native.serve_tls(listener, max_connections, acceptor).await;
    });
    let grpc_tls_state = state.clone();
    let grpc_tls_hook = range_hook.clone();
    let prebound_grpc_tls = std::sync::Arc::new(tokio::sync::Mutex::new(prebound_grpc_tls));
    let grpc_tls_task = tokio::spawn(async move {
        let listener = match prebound_grpc_tls.lock().await.take() {
            Some(listener) => listener,
            None => {
                let (Some(addr), Some(_)) =
                    (grpc_tls_state.grpc_tls_listen, grpc_tls_state.tls.clone())
                else {
                    futures_util::future::pending::<()>().await;
                    return;
                };
                match bind_retry(addr).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        tracing::warn!("grpc tls listener bind failed: {e}");
                        futures_util::future::pending::<()>().await;
                        return;
                    }
                }
            }
        };
        let acceptor = match grpc_tls_state.tls.clone() {
            Some(acceptor) => acceptor,
            None => {
                futures_util::future::pending::<()>().await;
                return;
            }
        };
        let gateway = ryme_wire_grpc::GrpcGateway::with_qos(
            grpc_tls_state.gateway.clone(),
            grpc_tls_state.keys.clone(),
            grpc_tls_state.qos.clone(),
        )
        .with_metering(grpc_tls_state.metering.clone())
        .with_observe(
            grpc_tls_state.latency.clone(),
            grpc_tls_state.histogram.clone(),
            grpc_tls_state.slow_log.clone(),
        )
        .with_traces(grpc_tls_state.traces.clone())
        .with_range_hook(grpc_tls_hook);
        let _ = gateway.serve_tls_limited(listener, acceptor, max_connections).await;
    });
    let sweep_state = state.clone();
    let sweep_task = tokio::spawn(async move {
        if sweep_interval == 0 {
            futures_util::future::pending::<()>().await;
            return;
        }
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(sweep_interval)).await;
            let removed = sweep_state.backend.sweep_once(Some(&sweep_state.realtime)).await;
            if removed > 0 {
                tracing::info!(removed = removed, "swept expired keys");
            }
            let stale = sweep_state.realtime.prune_presence(ryme_txn::now_unix());
            if stale > 0 {
                tracing::info!(removed = stale, "swept expired presence members");
            }
            let idle = sweep_state.realtime.prune_idle();
            if idle > 0 {
                tracing::info!(removed = idle, "pruned idle realtime scopes");
            }
            match sweep_state.backend.gc_oldest() {
                Ok(horizon) => tracing::debug!(horizon = horizon, "collected mvcc versions"),
                Err(e) => tracing::warn!(error = %e, "mvcc gc failed"),
            }
            match sweep_state.backend.retention_sweep(sweep_state.snapshot_keep) {
                Ok((snaps, segs)) if snaps + segs > 0 => {
                    tracing::info!(snapshots = snaps, wal_segments = segs, "pruned retention")
                }
                Err(e) => tracing::warn!(error = %e, "retention sweep failed"),
                _ => {}
            }
            sweep_state.backend.reconcile_qos(&sweep_state.qos);
        }
    });
    let otel_state = state.clone();
    let otel_task = tokio::spawn(async move {
        let (Some(endpoint), service, interval) =
            (otel.endpoint, otel.service, otel.interval_secs.max(1))
        else {
            futures_util::future::pending::<()>().await;
            return;
        };
        let client = reqwest::Client::new();
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
            match export_traces_once(&otel_state.traces, &client, &endpoint, &service).await {
                Ok(0) => {}
                Ok(count) => tracing::info!(spans = count, "exported otlp spans"),
                Err(e) => tracing::warn!("otlp export failed: {e}"),
            }
        }
    });
    let http_task = tokio::spawn(async move {
        if let Err(e) = serve_http_with_listener(http_listener, app, max_connections).await {
            tracing::warn!("http listener failed: {e}");
        }
    });
    let https_state = state.clone();
    let prebound_https = std::sync::Arc::new(tokio::sync::Mutex::new(prebound_https));
    let https_task = tokio::spawn(async move {
        let prebound = prebound_https.lock().await.take();
        let acceptor = match https_state.tls.clone() {
            Some(acceptor) => acceptor,
            None => {
                futures_util::future::pending::<()>().await;
                return;
            }
        };
        if let Some(listener) = prebound {
            if let Err(e) =
                serve_https_with_listener(listener, https_state, max_connections, acceptor).await
            {
                tracing::warn!("https listener failed: {e}");
            }
            return;
        }
        let (Some(addr), _) = (https_state.https_listen, https_state.tls.clone()) else {
            futures_util::future::pending::<()>().await;
            return;
        };
        if let Err(e) = serve_https(addr, https_state, max_connections, acceptor).await {
            tracing::warn!("https listener failed: {e}");
        }
    });
    let grpc_state = state.clone();
    let grpc_hook = range_hook.clone();
    let prebound_grpc = std::sync::Arc::new(tokio::sync::Mutex::new(prebound_grpc));
    let grpc_task = tokio::spawn(async move {
        let listener = match prebound_grpc.lock().await.take() {
            Some(listener) => listener,
            None => {
                let Some(addr) = grpc_state.grpc_listen else {
                    futures_util::future::pending::<()>().await;
                    return;
                };
                match bind_retry(addr).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        tracing::warn!("grpc listener bind failed: {e}");
                        futures_util::future::pending::<()>().await;
                        return;
                    }
                }
            }
        };
        let gateway = ryme_wire_grpc::GrpcGateway::with_qos(
            grpc_state.gateway.clone(),
            grpc_state.keys.clone(),
            grpc_state.qos.clone(),
        )
        .with_metering(grpc_state.metering.clone())
        .with_observe(
            grpc_state.latency.clone(),
            grpc_state.histogram.clone(),
            grpc_state.slow_log.clone(),
        )
        .with_traces(grpc_state.traces.clone())
        .with_range_hook(grpc_hook);
        let _ = gateway.serve_with_incoming_limited(listener, max_connections).await;
    });
    let autosplit_state = state.clone();
    let autosplit_task = tokio::spawn(async move {
        if autosplit_writes == 0 || autosplit_interval == 0 {
            futures_util::future::pending::<()>().await;
            return;
        }
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(autosplit_interval)).await;
            let result = match autosplit_state.control.lock() {
                Ok(mut control) => {
                    let previous = control.ranges();
                    match control.auto_split_once(autosplit_writes) {
                        Ok(created) => Ok((created, previous, control.ranges())),
                        Err(error) => Err(error),
                    }
                }
                Err(_) => Err(ryme_error::RymeError::Internal(String::from("control lock"))),
            };
            match result {
                Ok((created, previous, updated)) if !created.is_empty() => {
                    if let Err(error) = sync_range_topology(&autosplit_state, &updated) {
                        if let Ok(mut control) = autosplit_state.control.lock() {
                            let _ = control.restore_ranges(previous);
                        }
                        tracing::error!(%error, "failed to apply auto-split topology");
                    } else if let Err(error) = autosplit_state.persist_control() {
                        tracing::error!(%error, "failed to persist auto-split topology");
                    } else {
                        tracing::info!(ranges = ?created, "auto-split hot ranges");
                    }
                }
                Ok(_) => {}
                Err(error) => tracing::error!(%error, "auto-split failed"),
            }
        }
    });
    GatewayTasks {
        pg: pg_task,
        resp: resp_task,
        resp_tls: resp_tls_task,
        native: native_task,
        native_tls: tls_task,
        http: http_task,
        https: https_task,
        archive: archive_task,
        drill: drill_task,
        sweep: sweep_task,
        otel: otel_task,
        grpc: grpc_task,
        grpc_tls: grpc_tls_task,
        autosplit: autosplit_task,
    }
}

pub async fn serve_http_with_listener(
    listener: TcpListener,
    app: axum::Router,
    max_connections: usize,
) -> ryme_error::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as ConnBuilder;
    let limit = Arc::new(tokio::sync::Semaphore::new(max_connections.max(1)));
    loop {
        let (socket, _) =
            listener.accept().await.map_err(|e| ryme_error::RymeError::Io(e.to_string()))?;
        let Ok(permit) = limit.clone().try_acquire_owned() else {
            continue;
        };
        let _ = socket.set_nodelay(true).map_err(|e| ryme_error::RymeError::Io(e.to_string()));
        let app = app.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let io = TokioIo::new(socket);
            let service = hyper_util::service::TowerToHyperService::new(app);
            let _ = ConnBuilder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(io, service)
                .await;
        });
    }
}

pub async fn serve_https(
    addr: std::net::SocketAddr,
    state: SharedState,
    max_connections: usize,
    acceptor: ryme_wire_native::TlsAcceptor,
) -> ryme_error::Result<()> {
    let listener = bind_retry(addr).await?;
    serve_https_with_listener(listener, state, max_connections, acceptor).await
}

pub async fn serve_https_with_listener(
    listener: TcpListener,
    state: SharedState,
    max_connections: usize,
    acceptor: ryme_wire_native::TlsAcceptor,
) -> ryme_error::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as ConnBuilder;
    let limit = Arc::new(tokio::sync::Semaphore::new(max_connections.max(1)));
    let acceptor = acceptor.acceptor();
    loop {
        let (socket, _) =
            listener.accept().await.map_err(|e| ryme_error::RymeError::Io(e.to_string()))?;
        let Ok(permit) = limit.clone().try_acquire_owned() else {
            continue;
        };
        let _ = socket.set_nodelay(true).map_err(|e| ryme_error::RymeError::Io(e.to_string()));
        let service = state.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let Ok(tls) = acceptor.accept(socket).await else {
                return;
            };
            let app = router(service);
            let io = TokioIo::new(tls);
            let service = hyper_util::service::TowerToHyperService::new(app);
            let _ = ConnBuilder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(io, service)
                .await;
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_state(
    state: SharedState,
    archive_interval: u64,
    verify_interval: u64,
    sweep_interval: u64,
    max_connections: usize,
    otel: OtelConfig,
    autosplit_writes: u64,
    autosplit_interval: u64,
    pg_listener: TcpListener,
    resp_listener: TcpListener,
    http_listener: TcpListener,
    extra: OptionalListeners,
) -> ryme_error::Result<()> {
    let tasks = spawn_gateways(
        state,
        archive_interval,
        verify_interval,
        sweep_interval,
        max_connections,
        otel,
        autosplit_writes,
        autosplit_interval,
        pg_listener,
        resp_listener,
        http_listener,
        extra,
    );
    let GatewayTasks {
        pg: pg_task,
        resp: resp_task,
        http: http_task,
        archive: archive_task,
        resp_tls: _resp_tls_task,
        native: _native_task,
        native_tls: _native_tls_task,
        https: _https_task,
        sweep: _sweep_task,
        otel: _otel_task,
        grpc: _grpc_task,
        grpc_tls: _grpc_tls_task,
        autosplit: _autosplit_task,
        drill: _drill_task,
    } = tasks;
    let (pg_result, resp_result, http_result, _) =
        tokio::join!(pg_task, resp_task, http_task, archive_task);
    pg_result.map_err(|e| ryme_error::RymeError::Internal(e.to_string()))?;
    resp_result.map_err(|e| ryme_error::RymeError::Internal(e.to_string()))?;
    http_result.map_err(|e| ryme_error::RymeError::Internal(e.to_string()))?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn dashboard() -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../../../apps/dashboard/web/index.html"),
    )
        .into_response()
}

async fn ready(State(state): State<SharedState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "ready": true,
        "node": state.node_id,
        "commit": state.backend.latest_commit(),
        "leader": state.backend.is_leader().await,
        "cluster": state.backend.is_cluster(),
    }))
}

async fn metrics(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let snapshot = state.latency.snapshot();
    let histogram = state.histogram.snapshot();
    Json(serde_json::json!({
        "node": state.node_id,
        "uptime_secs": now_secs().saturating_sub(state.started_unix),
        "commit": state.backend.latest_commit(),
        "rest_count": snapshot.count,
        "rest_mean_micros": snapshot.mean_micros,
        "rest_max_micros": snapshot.max_micros,
        "p50_micros": histogram.p50_micros,
        "p90_micros": histogram.p90_micros,
        "p95_micros": histogram.p95_micros,
        "p99_micros": histogram.p99_micros,
        "p999_micros": histogram.p999_micros,
    }))
}

async fn prometheus(State(state): State<SharedState>) -> Response {
    let histogram = state.histogram.snapshot();
    let body = format!(
        "# HELP rymedb_uptime_seconds node uptime\n# TYPE rymedb_uptime_seconds counter\nrymedb_uptime_seconds {} \n# HELP rymedb_commit_index latest commit\n# TYPE rymedb_commit_index gauge\nrymedb_commit_index {} \n# HELP rymedb_rest_microseconds rest latency\n# TYPE rymedb_rest_microseconds summary\nrymedb_rest_microseconds{{quantile=\"0.5\"}} {}\nrymedb_rest_microseconds{{quantile=\"0.9\"}} {}\nrymedb_rest_microseconds{{quantile=\"0.95\"}} {}\nrymedb_rest_microseconds{{quantile=\"0.99\"}} {}\nrymedb_rest_microseconds{{quantile=\"0.999\"}} {}\nrymedb_rest_microseconds_count {}\nrymedb_rest_microseconds_max {} \n",
        now_secs().saturating_sub(state.started_unix),
        state.backend.latest_commit(),
        histogram.p50_micros,
        histogram.p90_micros,
        histogram.p95_micros,
        histogram.p99_micros,
        histogram.p999_micros,
        histogram.count,
        histogram.max_micros
    );
    (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
        .into_response()
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SlowQuery {
    pub limit: Option<usize>,
    pub table: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TraceQuery {
    pub limit: Option<usize>,
    pub name: Option<String>,
    pub table: Option<String>,
}

async fn slow_log(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<SlowQuery>,
) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "entries": state
                .slow_log
                .recent_filtered(query.limit.unwrap_or(100), query.table.as_deref())
        })),
    )
        .into_response()
}

async fn traces(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<TraceQuery>,
) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let spans = match state.traces.lock() {
        Ok(traces) => traces.recent_filtered(
            query.limit.unwrap_or(100),
            query.name.as_deref(),
            query.table.as_deref(),
        ),
        Err(_) => Vec::new(),
    };
    (StatusCode::OK, Json(serde_json::json!({ "spans": spans }))).into_response()
}

fn record_span(state: &SharedState, name: &str, attrs: &[(&str, &str)], micros: u64) {
    let mut span = TraceSpan::root(name.to_string(), now_secs());
    for (key, value) in attrs {
        span.attr(key.to_string(), value.to_string());
    }
    span.finish(micros);
    if let Ok(mut traces) = state.traces.lock() {
        traces.push(span);
    }
}

pub async fn export_traces_once(
    traces: &Arc<Mutex<TraceCollector>>,
    client: &reqwest::Client,
    endpoint: &str,
    service: &str,
) -> ryme_error::Result<usize> {
    let spans = match traces.lock() {
        Ok(mut collector) => collector.drain(500),
        Err(_) => return Err(ryme_error::RymeError::Internal(String::from("traces lock"))),
    };
    if spans.is_empty() {
        return Ok(0);
    }
    let payload = ryme_observe::otlp_resource_spans(service, &spans);
    let url = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
    let response = client
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| ryme_error::RymeError::Io(e.to_string()))?;
    if !response.status().is_success() {
        return Err(ryme_error::RymeError::Io(format!("otlp status {}", response.status())));
    }
    Ok(spans.len())
}

fn record_meter(state: &SharedState, metric: Metric, quantity: u64) {
    let event = UsageEvent::new(
        state.tenant.clone(),
        state.database.clone(),
        metric,
        quantity,
        format!("{}-{}", state.node_id, now_secs_nanos()),
    );
    if let Ok(mut registry) = state.metering.lock() {
        registry.ingest(event);
    }
}

fn now_secs_nanos() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
}

fn qos_now_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

fn admit_read(state: &SharedState, tenant: &str) -> ryme_error::Result<()> {
    match state.qos.lock() {
        Ok(mut qos) => qos.admit_read(tenant, qos_now_nanos()),
        Err(_) => Err(ryme_error::RymeError::Internal(String::from("qos lock"))),
    }
}

fn admit_write(state: &SharedState, tenant: &str, bytes: u64) -> ryme_error::Result<()> {
    match state.qos.lock() {
        Ok(mut qos) => qos.admit_write(tenant, bytes, qos_now_nanos()),
        Err(_) => Err(ryme_error::RymeError::Internal(String::from("qos lock"))),
    }
}
fn admit_egress(state: &SharedState, tenant: &str, bytes: u64) -> ryme_error::Result<()> {
    match state.qos.lock() {
        Ok(mut qos) => qos.admit_egress(tenant, bytes, qos_now_nanos()),
        Err(_) => Err(ryme_error::RymeError::Internal(String::from("qos lock"))),
    }
}

fn admit_realtime(state: &SharedState, tenant: &str, messages: u64) -> ryme_error::Result<()> {
    match state.qos.lock() {
        Ok(mut qos) => qos.admit_realtime(tenant, messages, qos_now_nanos()),
        Err(_) => Err(ryme_error::RymeError::Internal(String::from("qos lock"))),
    }
}

struct RealtimeConnectionGuard {
    qos: Arc<Mutex<QosRegistry>>,
    tenant: String,
}

impl Drop for RealtimeConnectionGuard {
    fn drop(&mut self) {
        if let Ok(mut qos) = self.qos.lock() {
            qos.connection_close(&self.tenant);
        }
    }
}

fn open_realtime_connection(
    state: &SharedState,
    tenant: &str,
) -> ryme_error::Result<RealtimeConnectionGuard> {
    let qos = state.qos.clone();
    qos.lock()
        .map_err(|_| ryme_error::RymeError::Internal(String::from("qos lock")))?
        .connection_open(tenant, qos_now_nanos())?;
    Ok(RealtimeConnectionGuard { qos, tenant: tenant.to_string() })
}

fn branch_snapshot(
    state: &SharedState,
    headers: &HeaderMap,
    tenant: &str,
) -> ryme_error::Result<Option<(String, u64, u64)>> {
    branch_snapshot_selected(state, headers, tenant, None)
}

fn branch_snapshot_selected(
    state: &SharedState,
    headers: &HeaderMap,
    tenant: &str,
    requested: Option<&str>,
) -> ryme_error::Result<Option<(String, u64, u64)>> {
    let header_branch = headers
        .get("x-ryme-branch")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty());
    if let (Some(header_branch), Some(requested)) =
        (header_branch, requested.filter(|v| !v.is_empty()))
    {
        if header_branch != requested {
            return Err(ryme_error::RymeError::InvalidArgument(String::from(
                "branch selection mismatch",
            )));
        }
    }
    let branch = requested.filter(|value| !value.is_empty()).or(header_branch).unwrap_or("main");
    if branch == "main" {
        return Ok(None);
    }
    let selected = state
        .control
        .lock()
        .map_err(|_| ryme_error::RymeError::Internal(String::from("lock")))?
        .branches
        .get_for(tenant, branch)?;
    if selected.base_commit_ts == 0 {
        return Err(ryme_error::RymeError::InvalidArgument(String::from(
            "branch has no data snapshot",
        )));
    }
    Ok(Some((selected.id, selected.base_commit_ts, selected.storage_epoch)))
}

fn validate_branch_selection(
    state: &SharedState,
    headers: &HeaderMap,
    tenant: &str,
) -> ryme_error::Result<()> {
    let _ = branch_snapshot(state, headers, tenant)?;
    Ok(())
}

fn branch_gateway(
    state: &SharedState,
    headers: &HeaderMap,
    tenant: &str,
) -> ryme_error::Result<Gateway<BranchStorage>> {
    let gateway = state.gateway.clone();
    match branch_snapshot(state, headers, tenant)? {
        Some((branch, base_commit_ts, storage_epoch)) => {
            let manager = ryme_branch::BranchBackend::new(
                state.backend.clone(),
                state.database.clone(),
                branch.clone(),
                base_commit_ts,
                storage_epoch,
            );
            Ok(gateway.with_backend_manager(manager).with_branch(branch))
        }
        None => Ok(gateway),
    }
}

fn branch_executor(
    state: &SharedState,
    headers: &HeaderMap,
    tenant: &str,
) -> ryme_error::Result<Executor<BranchStorage>> {
    let executor = state.executor.clone().with_tenant(tenant.to_string());
    match branch_snapshot(state, headers, tenant)? {
        Some((branch, base_commit_ts, storage_epoch)) => {
            let manager = ryme_branch::BranchBackend::new(
                state.backend.clone(),
                state.database.clone(),
                branch.clone(),
                base_commit_ts,
                storage_epoch,
            );
            let schema_path = state.branch_schema_path(tenant, &branch);
            let mut branch_executor = executor.with_isolated_schema(manager).with_branch(branch);
            if schema_path.exists() {
                let snapshot = load_schema_snapshot(&schema_path)?;
                branch_executor.restore_schema_snapshot(snapshot)?;
            }
            branch_executor.set_schema_path(schema_path);
            Ok(branch_executor)
        }
        None => Ok(executor),
    }
}

fn range_routing_key(table: &str, key: &[u8]) -> Vec<u8> {
    let mut routing = Vec::with_capacity(table.len() + key.len() + 1);
    routing.extend_from_slice(table.as_bytes());
    routing.push(0);
    routing.extend_from_slice(key);
    routing
}

fn sql_range_keys(statement: &Statement) -> Option<Vec<Vec<u8>>> {
    match statement {
        Statement::Insert { pk, .. }
        | Statement::Upsert { pk, .. }
        | Statement::InsertIgnore { pk, .. }
        | Statement::InsertConflict { pk, .. }
        | Statement::Update { pk, .. }
        | Statement::UpdateRow { pk, .. }
        | Statement::Delete { pk, .. } => Some(vec![pk.clone()]),
        Statement::CopyFrom { rows, .. } => Some(rows.iter().map(|(pk, _)| pk.clone()).collect()),
        Statement::Returning { statement, .. } => sql_range_keys(statement),
        _ => None,
    }
}

fn note_range_write(state: &SharedState, routing_key: &[u8], count: u64) {
    if state.autosplit_writes == 0 {
        return;
    }
    if let Ok(control) = state.control.lock() {
        let _ = control.router.note_write(routing_key, count);
    }
}

fn rest_row_to_json(pk: &[u8], value: &[u8]) -> serde_json::Value {
    match serde_json::from_slice::<serde_json::Value>(value) {
        Ok(parsed) if parsed.is_object() => {
            let mut obj = parsed.as_object().cloned().unwrap_or_default();
            obj.insert(
                String::from("key"),
                serde_json::Value::String(String::from_utf8_lossy(pk).to_string()),
            );
            serde_json::Value::Object(obj)
        }
        Ok(parsed) => serde_json::json!({
            "key": String::from_utf8_lossy(pk),
            "value": parsed,
        }),
        Err(_) => serde_json::json!({
            "key": String::from_utf8_lossy(pk),
            "value": String::from_utf8_lossy(value),
        }),
    }
}

fn rest_list_filtered(rows: Vec<(Vec<u8>, Vec<u8>)>, raw: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    filter_rows_by_query(rows, raw)
}

fn rest_matching_keys(
    gateway: &Gateway<BranchStorage>,
    principal: &Principal,
    table: &str,
    raw: &str,
) -> ryme_error::Result<Vec<Vec<u8>>> {
    let mut cursor = None;
    let mut keys = Vec::new();
    loop {
        let (page, next) = gateway.scan_page(principal, table, cursor.as_deref(), 256)?;
        keys.extend(rest_list_filtered(page, raw).into_iter().map(|(key, _)| key));
        let Some(next_cursor) = next else { break };
        cursor = Some(next_cursor);
    }
    Ok(keys)
}

fn rest_conflict_key(
    gateway: &Gateway<BranchStorage>,
    principal: &Principal,
    table: &str,
    field: &str,
    expected: &serde_json::Value,
) -> ryme_error::Result<Option<Vec<u8>>> {
    let mut cursor = None;
    loop {
        let (page, next) = gateway.scan_page(principal, table, cursor.as_deref(), 256)?;
        for (key, value) in page {
            let row = rest_row_to_json(&key, &value);
            if row.get(field) == Some(expected) {
                return Ok(Some(key));
            }
        }
        let Some(next_cursor) = next else { return Ok(None) };
        cursor = Some(next_cursor);
    }
}

async fn rest_list(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(table): Path<String>,
    Query(query): Query<RestListQuery>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> Response {
    let start = SystemTime::now();
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = admit_read(&state, &principal.tenant) {
        return error_response(e);
    }
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let offset = query.offset.unwrap_or(0).min(10_000);
    let gateway = match branch_gateway(&state, &headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    let raw = raw.as_deref().unwrap_or("");
    let count_exact = rest_prefer(&headers, "count=exact");
    let target = limit.saturating_add(offset);
    let full_order_scan = rest_order_requires_full_scan(query.order.as_deref());
    let page_limit = if full_order_scan { 256 } else { target.clamp(1, 1000) };
    let mut cursor = None;
    let mut filtered = Vec::new();
    loop {
        let (page, next) =
            match gateway.scan_page(&principal, &table, cursor.as_deref(), page_limit) {
                Ok(page) => page,
                Err(e) => return error_response(e),
            };
        filtered.extend(rest_list_filtered(page, raw));
        if (!full_order_scan && !count_exact && filtered.len() >= target) || next.is_none() {
            break;
        }
        cursor = next;
    }
    let total = filtered.len();
    let ordered = order_rows(filtered, query.order.as_deref());
    let paged: Vec<(Vec<u8>, Vec<u8>)> = ordered.into_iter().skip(offset).take(limit).collect();
    let egress: u64 = paged.iter().map(|(pk, value)| (pk.len() + value.len()) as u64).sum();
    if let Err(e) = admit_egress(&state, &principal.tenant, egress) {
        return error_response(e);
    }
    let items: Vec<serde_json::Value> = paged
        .iter()
        .map(|(pk, value)| {
            rest_project_row(
                rest_row_to_json(pk, &gateway.masked(&table, value.clone())),
                query.select.as_deref(),
            )
        })
        .collect();
    let range = if items.is_empty() {
        format!("*/{total}")
    } else {
        format!("{}-{}/{}", offset, offset + items.len() - 1, total)
    };
    record_meter(&state, Metric::ReadUnit, items.len() as u64);
    let micros = elapsed_micros(start);
    state.latency.observe_micros(micros);
    state.histogram.record(micros);
    record_span(&state, "rest_list", &[("table", table.as_str())], micros);
    let mut response = (StatusCode::OK, Json(items)).into_response();
    if count_exact {
        if let Ok(value) = HeaderValue::from_str(&range) {
            response.headers_mut().insert("content-range", value);
        }
        response.headers_mut().insert("range-unit", HeaderValue::from_static("items"));
    }
    response
}

fn filter_rows_by_query(rows: Vec<(Vec<u8>, Vec<u8>)>, raw: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let filters: Vec<(String, String)> = raw
        .split('&')
        .filter_map(|pair| {
            let mut split = pair.splitn(2, '=');
            let name = url_decode(split.next().unwrap_or(""));
            let value = url_decode(split.next().unwrap_or(""));
            if name.is_empty() || matches!(name.as_str(), "select" | "limit" | "offset" | "order") {
                None
            } else {
                Some((name, value))
            }
        })
        .collect();
    filter_rows_by_params(rows, &filters)
}

fn filter_rows_by_params(
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    filters: &[(String, String)],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    if filters.is_empty() {
        return rows;
    }
    rows.into_iter()
        .filter(|(pk, value)| {
            let row = rest_row_to_json(pk, value);
            filters.iter().all(|(name, expression)| rest_filter_matches(&row, name, expression))
        })
        .collect()
}

fn rest_filter_matches(row: &serde_json::Value, field: &str, expression: &str) -> bool {
    if field.eq_ignore_ascii_case("or") || field.eq_ignore_ascii_case("and") {
        let mut matches = rest_compound_filters(expression).into_iter().filter_map(|filter| {
            let (field, expression) = filter.split_once('.')?;
            Some(rest_filter_matches(row, field, expression))
        });
        return if field.eq_ignore_ascii_case("or") {
            matches.any(|matched| matched)
        } else {
            matches.all(|matched| matched)
        };
    }
    let Some(actual) = row.get(field) else { return false };
    let (operator, expected) = expression.split_once('.').unwrap_or(("eq", expression));
    if actual.is_null() && !operator.eq_ignore_ascii_case("is") {
        return false;
    }
    if operator.eq_ignore_ascii_case("not") {
        let Some((nested_operator, nested_expected)) = expected.split_once('.') else {
            return false;
        };
        return !rest_filter_matches(row, field, &format!("{nested_operator}.{nested_expected}"));
    }
    let actual_text = rest_scalar_text(actual);
    let expected = expected.trim();
    match operator.to_ascii_lowercase().as_str() {
        "eq" => actual_text.as_deref() == Some(expected),
        "neq" => actual_text.as_deref() != Some(expected),
        "gt" => rest_compare(actual_text.as_deref(), Some(expected)) == Ordering::Greater,
        "gte" => matches!(
            rest_compare(actual_text.as_deref(), Some(expected)),
            Ordering::Greater | Ordering::Equal
        ),
        "lt" => rest_compare(actual_text.as_deref(), Some(expected)) == Ordering::Less,
        "lte" => matches!(
            rest_compare(actual_text.as_deref(), Some(expected)),
            Ordering::Less | Ordering::Equal
        ),
        "like" | "ilike" => rest_like(
            actual_text.as_deref().unwrap_or_default(),
            expected,
            operator.eq_ignore_ascii_case("ilike"),
        ),
        "in" => expected
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .map(|values| values.split(',').any(|value| actual_text.as_deref() == Some(value)))
            .unwrap_or(false),
        "is" => match expected.to_ascii_lowercase().as_str() {
            "null" => actual.is_null(),
            "true" => actual.as_bool() == Some(true),
            "false" => actual.as_bool() == Some(false),
            _ => false,
        },
        _ => false,
    }
}

fn rest_compound_filters(expression: &str) -> Vec<&str> {
    let value = expression
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(expression);
    let mut filters = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    for (index, character) in value.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                let filter = value[start..index].trim();
                if !filter.is_empty() {
                    filters.push(filter);
                }
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    let filter = value[start..].trim();
    if !filter.is_empty() {
        filters.push(filter);
    }
    filters
}

fn rest_scalar_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        _ => Some(value.to_string()),
    }
}

fn rest_compare(left: Option<&str>, right: Option<&str>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => match (left.parse::<f64>(), right.parse::<f64>()) {
            (Ok(left), Ok(right)) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
            _ => left.cmp(right),
        },
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
    }
}

fn rest_like(value: &str, pattern: &str, insensitive: bool) -> bool {
    let value = if insensitive { value.to_ascii_lowercase() } else { value.to_string() };
    let pattern = if insensitive { pattern.to_ascii_lowercase() } else { pattern.to_string() };
    let value = value.chars().collect::<Vec<_>>();
    let pattern = pattern
        .chars()
        .map(|character| match character {
            '%' => '*',
            '_' => '?',
            character => character,
        })
        .collect::<Vec<_>>();
    let mut matched = vec![false; value.len() + 1];
    matched[0] = true;
    for character in pattern {
        let mut next = vec![false; value.len() + 1];
        if character == '*' {
            next[0] = matched[0];
            for index in 1..=value.len() {
                next[index] = matched[index] || next[index - 1];
            }
        } else {
            for index in 1..=value.len() {
                next[index] =
                    matched[index - 1] && (character == '?' || character == value[index - 1]);
            }
        }
        matched = next;
    }
    matched[value.len()]
}

fn rest_project_row(row: serde_json::Value, select: Option<&str>) -> serde_json::Value {
    let Some(select) = select.map(str::trim).filter(|select| !select.is_empty() && *select != "*")
    else {
        return row;
    };
    let Some(object) = row.as_object() else { return row };
    let mut projected = serde_json::Map::new();
    for item in select.split(',').map(str::trim).filter(|item| !item.is_empty()) {
        let (alias, field) = item.split_once(':').unwrap_or((item, item));
        if let Some(value) = object.get(field.trim()) {
            projected.insert(alias.trim().to_string(), value.clone());
        }
    }
    serde_json::Value::Object(projected)
}

#[cfg(test)]
mod rest_compatibility_tests {
    use super::*;

    #[test]
    fn postgrest_filters_and_projection_are_applied() {
        let rows = vec![
            (b"one".to_vec(), br#"{"status":"ready","score":12,"owner":"a"}"#.to_vec()),
            (b"two".to_vec(), br#"{"status":"queued","score":4,"owner":"b"}"#.to_vec()),
        ];
        let filtered = filter_rows_by_query(rows, "status=eq.ready&score=gte.10");
        assert_eq!(filtered.len(), 1);
        let row =
            rest_project_row(rest_row_to_json(&filtered[0].0, &filtered[0].1), Some("key,status"));
        assert_eq!(row, serde_json::json!({"key":"one","status":"ready"}));
        let ordered = order_rows(
            vec![
                (b"one".to_vec(), br#"{"score":12}"#.to_vec()),
                (b"two".to_vec(), br#"{"score":4}"#.to_vec()),
            ],
            Some("score.desc"),
        );
        assert_eq!(ordered[0].0, b"one");
        let ordered = order_rows(
            vec![
                (b"one".to_vec(), br#"{"group":"a","score":4}"#.to_vec()),
                (b"two".to_vec(), br#"{"group":"a","score":9}"#.to_vec()),
                (b"three".to_vec(), br#"{"group":"b","score":1}"#.to_vec()),
            ],
            Some("group.asc,score.desc"),
        );
        assert_eq!(
            ordered.into_iter().map(|row| row.0).collect::<Vec<_>>(),
            vec![b"two".to_vec(), b"one".to_vec(), b"three".to_vec()]
        );
        let ordered = order_rows(
            vec![
                (b"null".to_vec(), br#"{"score":null}"#.to_vec()),
                (b"value".to_vec(), br#"{"score":2}"#.to_vec()),
            ],
            Some("score.asc"),
        );
        assert_eq!(ordered[0].0, b"value");
        let ordered = order_rows(
            vec![
                (b"null".to_vec(), br#"{"score":null}"#.to_vec()),
                (b"value".to_vec(), br#"{"score":2}"#.to_vec()),
            ],
            Some("score.desc.nullslast"),
        );
        assert_eq!(ordered[0].0, b"value");
    }

    #[test]
    fn postgrest_like_and_null_filters_have_expected_semantics() {
        let rows = vec![
            (b"one".to_vec(), br#"{"name":"Alice","deleted":null}"#.to_vec()),
            (b"two".to_vec(), br#"{"name":"Bob","deleted":false}"#.to_vec()),
        ];
        assert_eq!(filter_rows_by_query(rows.clone(), "name=ilike.*ali*&deleted=is.null").len(), 1);
        assert_eq!(filter_rows_by_query(rows.clone(), "name=like.Ali%&deleted=is.null").len(), 1);
        assert_eq!(filter_rows_by_query(rows, "name=not.ilike.*ali*").len(), 1);
    }

    #[test]
    fn postgrest_compound_filters_have_expected_semantics() {
        let rows = vec![
            (b"one".to_vec(), br#"{"status":"ready","score":12}"#.to_vec()),
            (b"two".to_vec(), br#"{"status":"queued","score":4}"#.to_vec()),
            (b"three".to_vec(), br#"{"status":"failed","score":2}"#.to_vec()),
        ];
        assert_eq!(
            filter_rows_by_query(rows.clone(), "or=(status.eq.ready,status.eq.queued)").len(),
            2
        );
        assert_eq!(filter_rows_by_query(rows, "and=(status.neq.failed,score.gte.4)").len(), 2);
    }

    #[test]
    fn postgrest_prefer_tokens_are_case_insensitive_and_composable() {
        let mut headers = HeaderMap::new();
        headers.insert("prefer", HeaderValue::from_static("return=minimal, count=exact"));
        assert!(rest_prefer(&headers, "return=minimal"));
        assert!(rest_prefer(&headers, "count=exact"));
        assert!(!rest_prefer(&headers, "return=representation"));
    }
}

fn url_decode(input: &str) -> String {
    let mut out = String::new();
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex_val(bytes[index + 1]), hex_val(bytes[index + 2]))
            {
                out.push((high * 16 + low) as char);
                index += 3;
                continue;
            }
        }
        if bytes[index] == b'+' {
            out.push(' ');
        } else {
            out.push(bytes[index] as char);
        }
        index += 1;
    }
    out
}

fn hex_val(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn order_rows(rows: Vec<(Vec<u8>, Vec<u8>)>, order: Option<&str>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = rows;
    let specs = rest_order_specs(order);
    rows.sort_by(|left, right| {
        let left_row = rest_row_to_json(&left.0, &left.1);
        let right_row = rest_row_to_json(&right.0, &right.1);
        for (field, descending, nulls_first) in &specs {
            let left_value = left_row.get(*field);
            let right_value = right_row.get(*field);
            let left_null = left_value.map_or(true, serde_json::Value::is_null);
            let right_null = right_value.map_or(true, serde_json::Value::is_null);
            if left_null != right_null {
                let nulls_first = nulls_first.unwrap_or(*descending);
                return if left_null == nulls_first { Ordering::Less } else { Ordering::Greater };
            }
            if !left_null {
                let ordering = rest_compare(
                    left_value.and_then(rest_scalar_text).as_deref(),
                    right_value.and_then(rest_scalar_text).as_deref(),
                );
                if ordering != Ordering::Equal {
                    return if *descending { ordering.reverse() } else { ordering };
                }
            }
        }
        left.0.cmp(&right.0)
    });
    rows
}

fn rest_order_requires_full_scan(order: Option<&str>) -> bool {
    rest_order_specs(order).iter().any(|(field, descending, _)| *field != "key" || *descending)
}

fn rest_order_specs(order: Option<&str>) -> Vec<(&str, bool, Option<bool>)> {
    let Some(order) = order.filter(|value| !value.trim().is_empty()) else {
        return vec![("key", false, None)];
    };
    let specs = order
        .split(',')
        .filter_map(|spec| {
            let mut parts = spec.split('.').map(str::trim).filter(|part| !part.is_empty());
            let field = parts.next()?;
            let descending =
                parts.next().is_some_and(|direction| direction.eq_ignore_ascii_case("desc"));
            let nulls_first = parts.next().and_then(|nulls| {
                if nulls.eq_ignore_ascii_case("nullsfirst") {
                    Some(true)
                } else if nulls.eq_ignore_ascii_case("nullslast") {
                    Some(false)
                } else {
                    None
                }
            });
            Some((field, descending, nulls_first))
        })
        .collect::<Vec<_>>();
    if specs.is_empty() {
        vec![("key", false, None)]
    } else {
        specs
    }
}

async fn rest_insert(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(table): Path<String>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
    body: axum::body::Bytes,
) -> Response {
    let bodies = match rest_write_bodies(&body) {
        Ok(bodies) => bodies,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = validate_branch_selection(&state, &headers, &principal.tenant) {
        return error_response(e);
    }
    let rows = bodies
        .iter()
        .flat_map(|body| rest_body_rows(body).into_iter().map(move |row| (body, row)))
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("rows")));
    }
    let gateway = match branch_gateway(&state, &headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    let conflict_field =
        rest_query_value(&raw.unwrap_or_default(), "on_conflict").and_then(|field| {
            field
                .split(',')
                .next()
                .map(str::trim)
                .filter(|field| !field.is_empty())
                .map(str::to_string)
        });
    let mut inserted = Vec::new();
    for (body, (mut key, value)) in rows {
        if let Some(field) = conflict_field.as_deref() {
            if let Some(expected) = body.fields.get(field) {
                if let Some(existing) =
                    match rest_conflict_key(&gateway, &principal, &table, field, expected) {
                        Ok(existing) => existing,
                        Err(e) => return error_response(e),
                    }
                {
                    key = existing;
                }
            }
        }
        if let Err(e) = admit_write(&state, &principal.tenant, (key.len() + value.len()) as u64) {
            return error_response(e);
        }
        match gateway.put(&principal, &table, key.clone(), value.clone()).await {
            Ok(commit) => {
                record_meter(&state, Metric::WriteUnit, 1);
                note_range_write(&state, &range_routing_key(&table, &key), 1);
                inserted.push(rest_row_to_json(&key, &value));
                let _ = commit;
            }
            Err(e) => return error_response(e),
        }
    }
    if rest_prefer(&headers, "return=minimal") {
        StatusCode::CREATED.into_response()
    } else {
        (StatusCode::CREATED, Json(inserted)).into_response()
    }
}

async fn rest_upsert(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(table): Path<String>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
    body: axum::body::Bytes,
) -> Response {
    let parsed = match rest_write_bodies(&body) {
        Ok(body) => body,
        Err(e) => return error_response(e),
    };
    let raw = raw.unwrap_or_default();
    if body_is_json_array(&body) || parsed.len() != 1 || parsed[0].fields.is_empty() {
        return rest_insert(
            State(state),
            headers,
            Path(table),
            axum::extract::RawQuery(Some(raw)),
            body,
        )
        .await;
    }
    let parsed = parsed.into_iter().next().expect("single REST object");

    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = validate_branch_selection(&state, &headers, &principal.tenant) {
        return error_response(e);
    }
    let gateway = match branch_gateway(&state, &headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    let keys = match rest_matching_keys(&gateway, &principal, &table, &raw) {
        Ok(keys) => keys,
        Err(e) => return error_response(e),
    };
    let mut updated = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(current) = (match gateway.get(&principal, &table, &key) {
            Ok(current) => current,
            Err(e) => return error_response(e),
        }) else {
            continue;
        };
        let Some(mut object) = serde_json::from_slice::<serde_json::Value>(&current)
            .ok()
            .and_then(|value| value.as_object().cloned())
        else {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from(
                "PATCH requires JSON object rows",
            )));
        };
        object.extend(parsed.fields.clone());
        let value = match serde_json::to_vec(&serde_json::Value::Object(object)) {
            Ok(value) => value,
            Err(e) => return error_response(ryme_error::RymeError::InvalidArgument(e.to_string())),
        };
        if let Err(e) = admit_write(&state, &principal.tenant, (key.len() + value.len()) as u64) {
            return error_response(e);
        }
        match gateway.put(&principal, &table, key.clone(), value.clone()).await {
            Ok(commit) => {
                record_meter(&state, Metric::WriteUnit, 1);
                note_range_write(&state, &range_routing_key(&table, &key), 1);
                updated.push(rest_row_to_json(&key, &value));
                let _ = commit;
            }
            Err(e) => return error_response(e),
        }
    }
    if rest_prefer(&headers, "return=minimal") {
        StatusCode::OK.into_response()
    } else {
        (StatusCode::OK, Json(updated)).into_response()
    }
}

async fn rest_delete(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(table): Path<String>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = validate_branch_selection(&state, &headers, &principal.tenant) {
        return error_response(e);
    }
    let raw = raw.unwrap_or_default();
    if !rest_query_has_filter(&raw) {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from(
            "filter required",
        )));
    }
    let gateway = match branch_gateway(&state, &headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    let keys = match rest_matching_keys(&gateway, &principal, &table, &raw) {
        Ok(keys) => keys,
        Err(e) => return error_response(e),
    };
    let mut deleted = 0u64;
    let mut last_commit = 0u64;
    for key in keys {
        if let Err(e) = admit_write(&state, &principal.tenant, key.len() as u64) {
            return error_response(e);
        }
        match gateway.delete(&principal, &table, key.clone()).await {
            Ok(commit) => {
                record_meter(&state, Metric::WriteUnit, 1);
                note_range_write(&state, &range_routing_key(&table, &key), 1);
                deleted += 1;
                last_commit = commit;
            }
            Err(e) => return error_response(e),
        }
    }
    if rest_prefer(&headers, "return=minimal") {
        StatusCode::OK.into_response()
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "commit": last_commit,
                "deleted": deleted,
            })),
        )
            .into_response()
    }
}

fn rest_prefer(headers: &HeaderMap, wanted: &str) -> bool {
    headers
        .get("prefer")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(',')
        .any(|preference| preference.trim().eq_ignore_ascii_case(wanted))
}

fn rest_query_has_filter(raw: &str) -> bool {
    raw.split('&').any(|pair| {
        let name = url_decode(pair.split_once('=').map(|(name, _)| name).unwrap_or(pair));
        !name.is_empty() && !matches!(name.as_str(), "select" | "limit" | "offset" | "order")
    })
}

fn rest_query_value(raw: &str, wanted: &str) -> Option<String> {
    raw.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (url_decode(name) == wanted).then(|| url_decode(value))
    })
}

fn rest_body_rows(body: &RestWriteBody) -> Vec<(Vec<u8>, Vec<u8>)> {
    if let Some(rows) = body.rows.clone() {
        return rows
            .into_iter()
            .map(|row| (row.key.into_bytes(), row.value.into_bytes()))
            .collect();
    }
    match (body.key.clone(), body.value.clone()) {
        (Some(key), Some(value)) => {
            let bytes = match value {
                serde_json::Value::String(text) => text.into_bytes(),
                other => other.to_string().into_bytes(),
            };
            vec![(key.into_bytes(), bytes)]
        }
        _ if !body.fields.is_empty() => {
            let key = body
                .fields
                .get("key")
                .or_else(|| body.fields.get("id"))
                .and_then(serde_json::Value::as_str);
            let Some(key) = key else { return Vec::new() };
            let value = serde_json::to_vec(&serde_json::Value::Object(body.fields.clone()))
                .unwrap_or_default();
            vec![(key.as_bytes().to_vec(), value)]
        }
        _ => Vec::new(),
    }
}

fn rest_write_bodies(body: &[u8]) -> Result<Vec<RestWriteBody>, ryme_error::RymeError> {
    let value: serde_json::Value = json_body(body)?;
    match value {
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(|value| {
                serde_json::from_value(value)
                    .map_err(|_| ryme_error::RymeError::InvalidArgument(String::from("body")))
            })
            .collect(),
        serde_json::Value::Object(_) => Ok(vec![serde_json::from_value(value)
            .map_err(|_| ryme_error::RymeError::InvalidArgument(String::from("body")))?]),
        _ => Err(ryme_error::RymeError::InvalidArgument(String::from("body"))),
    }
}

fn body_is_json_array(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body).map(|value| value.is_array()).unwrap_or(false)
}

async fn graphql_exec(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let body: GraphqlRequest = match json_body(&body) {
        Ok(body) => body,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = admit_read(&state, &principal.tenant) {
        return error_response(e);
    }
    let gateway = match branch_gateway(&state, &headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    match execute_graphql(&state, &gateway, &principal, &body.query).await {
        Ok(value) => (StatusCode::OK, Json(serde_json::json!({ "data": value }))).into_response(),
        Err(e) => error_response(e),
    }
}

async fn execute_graphql(
    state: &SharedState,
    gateway: &Gateway<BranchStorage>,
    principal: &Principal,
    query: &str,
) -> ryme_error::Result<serde_json::Value> {
    let table = parse_graphql_table(query).ok_or_else(|| {
        ryme_error::RymeError::InvalidArgument(String::from("table(key:) required"))
    })?;
    if let Some(key) = parse_graphql_key(query) {
        match gateway.get(principal, &table, key.as_bytes())? {
            Some(value) => {
                admit_egress(state, &principal.tenant, value.len() as u64)?;
                let masked = gateway.masked(&table, value);
                Ok(serde_json::json!({ table: rest_row_to_json(key.as_bytes(), &masked) }))
            }
            None => Ok(serde_json::json!({ table: serde_json::Value::Null })),
        }
    } else {
        let limit = parse_graphql_limit(query).unwrap_or(100).min(1000);
        let rows = gateway.scan(principal, &table, limit)?;
        let egress: u64 = rows.iter().map(|(pk, value)| (pk.len() + value.len()) as u64).sum();
        admit_egress(state, &principal.tenant, egress)?;
        let items: Vec<serde_json::Value> = rows
            .iter()
            .map(|(pk, value)| rest_row_to_json(pk, &gateway.masked(&table, value.clone())))
            .collect();
        Ok(serde_json::json!({ table: items }))
    }
}

fn parse_graphql_table(query: &str) -> Option<String> {
    let start = query.find('{')?;
    let rest = query[start + 1..].trim_start();
    let mut name = String::new();
    for ch in rest.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            name.push(ch);
        } else {
            break;
        }
    }
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn parse_graphql_key(query: &str) -> Option<String> {
    let marker = "key:";
    let pos = query.find(marker)?;
    let rest = query[pos + marker.len()..].trim_start();
    let quote = rest.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let end = rest[1..].find(quote)?;
    Some(rest[1..1 + end].to_string())
}

fn parse_graphql_limit(query: &str) -> Option<usize> {
    let marker = "limit:";
    let pos = query.find(marker)?;
    let rest = query[pos + marker.len()..].trim_start();
    let mut digits = String::new();
    for ch in rest.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else {
            break;
        }
    }
    digits.parse::<usize>().ok()
}

async fn sql_copy(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: CopyRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let start = SystemTime::now();
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = validate_branch_selection(&state, &headers, &principal.tenant) {
        return error_response(e);
    }
    if request.table.is_empty() || request.rows.len() > 10000 {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("rows")));
    }
    let bytes: u64 = request.rows.iter().map(|row| (row.key.len() + row.value.len()) as u64).sum();
    if let Err(e) = admit_write(&state, &principal.tenant, bytes) {
        return error_response(e);
    }
    let rows: Vec<(Vec<u8>, Vec<u8>)> = request
        .rows
        .into_iter()
        .map(|row| (row.key.into_bytes(), row.value.into_bytes()))
        .collect();
    let executor = match branch_executor(&state, &headers, &principal.tenant) {
        Ok(executor) => executor,
        Err(e) => return error_response(e),
    };
    match executor.bulk_upsert(request.table.clone(), rows.clone()).await {
        Ok(count) => {
            record_meter(&state, Metric::WriteUnit, count as u64);
            for (key, _) in rows.iter().take(count) {
                note_range_write(&state, &range_routing_key(&request.table, key), 1);
            }
            let micros = elapsed_micros(start);
            state.latency.observe_micros(micros);
            state.histogram.record(micros);
            record_span(&state, "sql_copy", &[("table", request.table.as_str())], micros);
            if micros > ryme_observe::SLOW_THRESHOLD_MICROS {
                tracing::warn!(table = %request.table, micros = micros, "slow sql_copy");
                state.slow_log.record(SlowEntry {
                    kind: String::from("sql"),
                    fingerprint: format!("COPY {}", request.table),
                    table: request.table.clone(),
                    micros,
                    at_unix: now_secs(),
                });
            }
            (StatusCode::OK, Json(serde_json::json!({ "ok": true, "rows": count }))).into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn sql_explain(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: ExplainRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let sql = match request.params {
        Some(params) => bind(&request.sql, &params),
        None => request.sql,
    };
    let executor = match branch_executor(&state, &headers, &principal.tenant) {
        Ok(executor) => executor,
        Err(e) => return error_response(e),
    };
    match executor.explain(&sql) {
        Ok(plan) => (StatusCode::OK, Json(serde_json::json!({ "plan": plan }))).into_response(),
        Err(e) => error_response(e),
    }
}

async fn metering_snapshot(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let snapshot = match state.metering.lock() {
        Ok(registry) => registry.snapshot(),
        Err(_) => {
            return error_response(ryme_error::RymeError::Internal(String::from("lock")));
        }
    };
    let items: Vec<serde_json::Value> = snapshot
        .into_iter()
        .map(|(key, total)| {
            serde_json::json!({
                "tenant": key.tenant,
                "database": key.database,
                "metric": key.metric,
                "quantity": total.quantity,
                "events": total.events,
            })
        })
        .collect();
    (StatusCode::OK, Json(items)).into_response()
}

async fn billing_summary(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let bills = match state.metering.lock() {
        Ok(registry) => registry.billing_summary(),
        Err(_) => {
            return error_response(ryme_error::RymeError::Internal(String::from("lock")));
        }
    };
    (StatusCode::OK, Json(bills)).into_response()
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct InvoiceQuery {
    pub tenant: Option<String>,
}

async fn billing_invoice(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<InvoiceQuery>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    let tenant = query.tenant.unwrap_or_else(|| principal.tenant.clone());
    if tenant != principal.tenant
        && !principal.roles.contains(&Role::Owner)
        && !principal.roles.contains(&Role::Admin)
    {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let invoice = match state.metering.lock() {
        Ok(registry) => registry.invoice(&tenant, &ryme_metering::PriceTable::default()),
        Err(_) => {
            return error_response(ryme_error::RymeError::Internal(String::from("lock")));
        }
    };
    (StatusCode::OK, Json(invoice)).into_response()
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SupabaseAnalyzeRequest {
    pub dump: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MigrateApplyRequest {
    pub id: String,
    pub sql: String,
    pub author: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct NeonAnalyzeRequest {
    pub branches: String,
}

async fn migrate_supabase(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: SupabaseAnalyzeRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if request.dump.len() > 4 * 1024 * 1024 {
        return error_response(ryme_error::RymeError::Overload(String::from("dump")));
    }
    (StatusCode::OK, Json(ryme_migrate::parse_supabase_dump(&request.dump))).into_response()
}

async fn migrate_neon(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: NeonAnalyzeRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    match ryme_migrate::parse_neon_branches(&request.branches) {
        Ok(plans) => (StatusCode::OK, Json(plans)).into_response(),
        Err(e) => error_response(e),
    }
}

async fn apply_migration_locked(
    state: &SharedState,
    tenant: String,
    request: MigrateApplyRequest,
    statement: ryme_sql::Statement,
    author: String,
) -> ryme_error::Result<ryme_migrate::LedgerEntry> {
    let _migration_guard = state.migration_lock.lock().await;
    let duplicate = match state.control.lock() {
        Ok(control) => {
            control.migrations.entries().iter().any(|entry| entry.migration_id == request.id)
        }
        Err(_) => return Err(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    if duplicate {
        return Err(ryme_error::RymeError::Conflict(String::from("migration")));
    }
    let executor = state.executor.clone().with_tenant(tenant);
    executor.execute(statement).await?;
    let mut control =
        state.control.lock().map_err(|_| ryme_error::RymeError::Internal(String::from("lock")))?;
    let applied = control.migrations.apply(request.id, &request.sql, author, now_secs())?;
    drop(control);
    state.persist_control()?;
    Ok(applied)
}

async fn migrate_apply(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: MigrateApplyRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let statement = match ryme_sql::parse(&request.sql) {
        Ok(statement) => statement,
        Err(e) => return error_response(e),
    };
    if !statement.is_write() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from(
            "read-only migration",
        )));
    }
    let author = request.author.clone().unwrap_or_else(|| principal.id.clone());
    match apply_migration_locked(&state, principal.tenant, request, statement, author).await {
        Ok(entry) => (StatusCode::CREATED, Json(entry)).into_response(),
        Err(e) => error_response(e),
    }
}

async fn migrate_ledger(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "entries": control.migrations.entries(),
            "version": control.migrations.schema_version(),
            "valid": control.migrations.verify().is_ok(),
        })),
    )
        .into_response()
}

async fn autoscale_advice(
    State(state): State<SharedState>,
    Query(query): Query<AutoscaleQuery>,
) -> Response {
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    let input = ryme_control::AutoscaleInput {
        cpu_pct: query.cpu_pct.unwrap_or(20),
        active_connections: query.active_connections.unwrap_or(10),
        connection_limit: query.connection_limit.unwrap_or(10000),
        shard_qps: query.shard_qps.unwrap_or(100),
        shard_qps_limit: query.shard_qps_limit.unwrap_or(200000),
        disk_used_pct: query.disk_used_pct.unwrap_or(30),
        follower_lag_ms: query.follower_lag_ms.unwrap_or(5),
        realtime_sockets: query.realtime_sockets.unwrap_or(100),
        realtime_lag_ms: query.realtime_lag_ms.unwrap_or(5),
        compaction_debt_mb: query.compaction_debt_mb.unwrap_or(8),
        p99_queue_ms: query.p99_queue_ms.unwrap_or(1),
    };
    let advice = control.advise(&input);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "pressure": advice.pressure,
            "action": advice.action,
            "urgent": advice.urgent,
        })),
    )
        .into_response()
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct QosTierRequest {
    pub tenant: String,
    pub tier: Tier,
}

async fn qos_snapshot(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let snapshot = match state.qos.lock() {
        Ok(qos) => qos.snapshot(),
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    (StatusCode::OK, Json(snapshot)).into_response()
}

async fn qos_set_tier(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: QosTierRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if request.tenant.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("tenant")));
    }
    if !principal.can_admin() || request.tenant != principal.tenant {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    match state.qos.lock() {
        Ok(mut qos) => {
            qos.set_tier(&request.tenant, request.tier, qos_now_nanos());
            (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
        }
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AuthRegisterRequest {
    pub id: String,
    pub tenant: Option<String>,
    pub password: String,
    pub roles: Option<Vec<String>>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AuthVerifyRequest {
    pub id: String,
    pub password: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AuthTokenRequest {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub grant_type: Option<String>,
    #[serde(default)]
    pub refresh_token: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AuthRevokeRequest {
    pub key: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct OtpSetupRequest {
    pub id: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct OtpVerifyRequest {
    pub id: String,
    pub code: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PasskeyChallengeRequest {
    pub user: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PasskeyRegisterRequest {
    pub user: String,
    pub credential_id: String,
    pub public_key: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PasskeyVerifyRequest {
    pub user: String,
    pub credential_id: String,
    pub authenticator_data: String,
    pub client_data_json: String,
    pub signature: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MaskRequest {
    pub table: String,
    pub fields: Vec<String>,
}

fn parse_roles(
    names: Option<Vec<String>>,
) -> Result<std::collections::HashSet<Role>, ryme_error::RymeError> {
    let mut roles = std::collections::HashSet::new();
    for name in names.unwrap_or_default() {
        match name.as_str() {
            "owner" => {
                roles.insert(Role::Owner);
            }
            "admin" => {
                roles.insert(Role::Admin);
            }
            "developer" => {
                roles.insert(Role::Developer);
            }
            "readwrite" => {
                roles.insert(Role::ReadWrite);
            }
            "readonly" => {
                roles.insert(Role::ReadOnly);
            }
            "realtime_publisher" => {
                roles.insert(Role::RealtimePublisher);
            }
            "realtime_subscriber" => {
                roles.insert(Role::RealtimeSubscriber);
            }
            _ => {
                return Err(ryme_error::RymeError::InvalidArgument(String::from("role")));
            }
        }
    }
    if roles.is_empty() {
        roles.insert(Role::ReadWrite);
    }
    Ok(roles)
}

async fn auth_register(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: AuthRegisterRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let tenant = request.tenant.unwrap_or_else(|| principal.tenant.clone());
    if tenant != principal.tenant && !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let roles = match parse_roles(request.roles) {
        Ok(roles) => roles,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() && (roles.contains(&Role::Owner) || roles.contains(&Role::Admin)) {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let credentials = state.credentials.clone();
    let registered = tokio::task::spawn_blocking(move || {
        let mut store = match credentials.lock() {
            Ok(store) => store,
            Err(_) => return Err(ryme_error::RymeError::Internal(String::from("lock"))),
        };
        store.register_password(request.id, tenant, &request.password, roles)
    })
    .await;
    match registered {
        Ok(Ok(())) => match state.persist_auth() {
            Ok(()) => {
                (StatusCode::CREATED, Json(serde_json::json!({ "ok": true }))).into_response()
            }
            Err(e) => error_response(e),
        },
        Ok(Err(e)) => error_response(e),
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("task"))),
    }
}

async fn auth_verify(State(state): State<SharedState>, body: axum::body::Bytes) -> Response {
    let request: AuthVerifyRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let credentials = state.credentials.clone();
    let verified = tokio::task::spawn_blocking(move || match credentials.lock() {
        Ok(store) => store.verify_password(&request.id, &request.password),
        Err(_) => Err(ryme_error::RymeError::Internal(String::from("lock"))),
    })
    .await;
    match verified {
        Ok(Ok(principal)) => {
            (StatusCode::OK, Json(serde_json::json!({ "ok": true, "tenant": principal.tenant })))
                .into_response()
        }
        Ok(Err(e)) => error_response(e),
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("task"))),
    }
}

fn issue_api_key(
    keys: &ryme_auth::ApiKeyStore,
    principal: &ryme_auth::Principal,
) -> (String, String) {
    let key = format!("ryme_{}", ryme_auth::base64_url_encode(&ryme_auth::random_bytes(32)));
    keys.insert(key.clone(), principal.clone());
    (key, principal.tenant.clone())
}

async fn auth_token(State(state): State<SharedState>, body: axum::body::Bytes) -> Response {
    let request: AuthTokenRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let credentials = state.credentials.clone();
    let keys = state.keys.clone();
    let refresh_tokens = state.refresh_tokens.clone();
    let now = ryme_txn::now_unix();
    let issued = tokio::task::spawn_blocking(move || {
        if request.refresh_token.is_some() || request.grant_type.as_deref() == Some("refresh_token")
        {
            if request.grant_type.as_deref().is_some_and(|grant| grant != "refresh_token") {
                return Err(ryme_error::RymeError::InvalidArgument(String::from("grant_type")));
            }
            let presented =
                request.refresh_token.as_deref().ok_or(ryme_error::RymeError::Unauthorized)?;
            let (principal, refresh_token) = refresh_tokens.rotate(presented, now)?;
            let (key, tenant) = issue_api_key(&keys, &principal);
            return Ok((key, tenant, refresh_token));
        }
        if request.grant_type.as_deref().is_some_and(|grant| grant != "password") {
            return Err(ryme_error::RymeError::InvalidArgument(String::from("grant_type")));
        }
        let principal = match credentials.lock() {
            Ok(store) => match store.verify_password(&request.id, &request.password) {
                Ok(principal) => principal,
                Err(e) => return Err(e),
            },
            Err(_) => return Err(ryme_error::RymeError::Internal(String::from("lock"))),
        };
        if let Ok(store) = credentials.lock() {
            if store.otp_enrolled(&request.id) {
                let code = request.code.as_deref().unwrap_or("");
                if store.verify_otp(&request.id, code, now).is_err() {
                    return Err(ryme_error::RymeError::Unauthorized);
                }
            }
        }
        let refresh_token = refresh_tokens.issue(principal.clone(), now)?;
        let (key, tenant) = issue_api_key(&keys, &principal);
        Ok((key, tenant, refresh_token))
    })
    .await;
    match issued {
        Ok(Ok((key, tenant, refresh_token))) => match state.persist_auth() {
            Ok(()) => (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "key": key,
                    "tenant": tenant,
                    "refresh_token": refresh_token,
                    "refresh_token_expires_in": ryme_auth::REFRESH_TOKEN_TTL_SECS,
                })),
            )
                .into_response(),
            Err(e) => error_response(e),
        },
        Ok(Err(e)) => error_response(e),
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("task"))),
    }
}

async fn auth_revoke(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: AuthRevokeRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if request.key.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("key")));
    }
    let Some(owner) = state.keys.owner_of(&request.key) else {
        return error_response(ryme_error::RymeError::NotFound(String::from("key")));
    };
    if !principal.can_admin() && owner.id != principal.id {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if state.keys.remove(&request.key) {
        match state.persist_auth() {
            Ok(()) => {
                (StatusCode::OK, Json(serde_json::json!({ "revoked": true }))).into_response()
            }
            Err(e) => error_response(e),
        }
    } else {
        error_response(ryme_error::RymeError::NotFound(String::from("key")))
    }
}

async fn otp_setup(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: OtpSetupRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if !principal.can_admin() && principal.id != request.id {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let secret = ryme_auth::random_bytes(20);
    let encoded = ryme_auth::base64_url_encode(&secret);
    let updated = match state.credentials.lock() {
        Ok(mut store) => store.set_otp_secret(&request.id, secret),
        Err(_) => Err(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match updated {
        Ok(()) => match state.persist_auth() {
            Ok(()) => {
                (StatusCode::OK, Json(serde_json::json!({ "secret": encoded }))).into_response()
            }
            Err(e) => error_response(e),
        },
        Err(e) => error_response(e),
    }
}

async fn otp_verify(State(state): State<SharedState>, body: axum::body::Bytes) -> Response {
    let request: OtpVerifyRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let now = ryme_txn::now_unix();
    let verified = match state.credentials.lock() {
        Ok(store) => store.verify_otp(&request.id, &request.code, now),
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match verified {
        Ok(principal) => {
            (StatusCode::OK, Json(serde_json::json!({ "ok": true, "tenant": principal.tenant })))
                .into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn passkey_challenge(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: PasskeyChallengeRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    if request.user.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("user")));
    }
    match state.passkeys.lock() {
        Ok(mut registry) => match registry.challenge(&request.user) {
            Ok(challenge) => (StatusCode::OK, Json(serde_json::json!({ "challenge": challenge })))
                .into_response(),
            Err(e) => error_response(e),
        },
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

async fn passkey_register(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: PasskeyRegisterRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if !principal.can_admin() && principal.id != request.user {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let public_key = match ryme_auth::base64_url_decode(&request.public_key) {
        Ok(key) => key,
        Err(_) => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from(
                "public key",
            )))
        }
    };
    let registered = match state.passkeys.lock() {
        Ok(mut registry) => registry.register(&request.user, request.credential_id, &public_key),
        Err(_) => Err(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match registered {
        Ok(()) => match state.persist_auth() {
            Ok(()) => {
                (StatusCode::CREATED, Json(serde_json::json!({ "ok": true }))).into_response()
            }
            Err(e) => error_response(e),
        },
        Err(e) => error_response(e),
    }
}

async fn passkey_verify(State(state): State<SharedState>, body: axum::body::Bytes) -> Response {
    let request: PasskeyVerifyRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    if state.passkey_rp_id.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from(
            "passkey rp_id",
        )));
    }
    let authenticator_data = match ryme_auth::base64_url_decode(&request.authenticator_data) {
        Ok(raw) => raw,
        Err(_) => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from(
                "authenticator data",
            )))
        }
    };
    let client_data_json = match ryme_auth::base64_url_decode(&request.client_data_json) {
        Ok(raw) => raw,
        Err(_) => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from(
                "client data",
            )))
        }
    };
    let signature = match ryme_auth::base64_url_decode(&request.signature) {
        Ok(raw) => raw,
        Err(_) => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from(
                "signature",
            )))
        }
    };
    let rp_id = state.passkey_rp_id.clone();
    let origins = state.passkey_origins.clone();
    let keys = state.keys.clone();
    let mut registry = match state.passkeys.lock() {
        Ok(registry) => registry,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    if registry
        .verify_assertion(&ryme_auth::PasskeyAssertion {
            user: &request.user,
            credential_id: &request.credential_id,
            authenticator_data: &authenticator_data,
            client_data_json: &client_data_json,
            signature_der: &signature,
            rp_id: &rp_id,
            origins: &origins,
            now_secs: ryme_txn::now_unix(),
        })
        .is_err()
    {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    drop(registry);
    let principal = match state.credentials.lock() {
        Ok(store) => store.principal(&request.user),
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    let principal = match principal {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    let (key, tenant) = issue_api_key(&keys, &principal);
    match state.persist_auth() {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::json!({ "key": key, "tenant": tenant })))
            .into_response(),
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct OidcLoginRequest {
    pub redirect_uri: String,
    pub state: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct OidcTokenRequest {
    pub id_token: String,
}

async fn oidc_login(State(state): State<SharedState>, body: axum::body::Bytes) -> Response {
    let request: OidcLoginRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let Some(config) = state.oidc.clone() else {
        return error_response(ryme_error::RymeError::Unavailable(String::from("oidc")));
    };
    let state_value =
        request.state.unwrap_or_else(|| ryme_auth::base64_url_encode(&ryme_auth::random_bytes(16)));
    match config.auth_url(&request.redirect_uri, &state_value) {
        Ok(auth_url) => (
            StatusCode::OK,
            Json(serde_json::json!({ "auth_url": auth_url, "state": state_value })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

async fn oidc_token(State(state): State<SharedState>, body: axum::body::Bytes) -> Response {
    let request: OidcTokenRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let Some(config) = state.oidc.clone() else {
        return error_response(ryme_error::RymeError::Unavailable(String::from("oidc")));
    };
    let now = ryme_txn::now_unix();
    let verified = match state.oidc_jwt.as_ref() {
        Some(verifier) => verifier.principal_from_token(&request.id_token, now),
        None => config.verify_id_token(&request.id_token, now),
    };
    match verified {
        Ok(principal) => {
            let (key, tenant) = issue_api_key(&state.keys, &principal);
            match state.persist_auth() {
                Ok(()) => {
                    (StatusCode::CREATED, Json(serde_json::json!({ "key": key, "tenant": tenant })))
                        .into_response()
                }
                Err(e) => error_response(e),
            }
        }
        Err(e) => error_response(e),
    }
}

async fn mask_set(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: MaskRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if request.table.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("table")));
    }
    state.gateway.set_mask(request.table, request.fields);
    (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PresenceJoinRequest {
    pub channel: String,
    pub member: String,
    pub state: Option<serde_json::Value>,
    pub ttl_secs: Option<u64>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PresenceLeaveRequest {
    pub channel: String,
    pub member: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BroadcastRequest {
    pub channel: String,
    pub from: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ClusterBroadcast {
    tenant: String,
    channel: String,
    from: String,
    payload: serde_json::Value,
    commit_ts: u64,
    sequence: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum ClusterPresence {
    Join {
        tenant: String,
        channel: String,
        member: String,
        state: serde_json::Value,
        expires_unix: u64,
        now_unix: u64,
    },
    Leave {
        tenant: String,
        channel: String,
        member: String,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ClusterTopicAppend {
    tenant: String,
    partition: String,
    cursor: u64,
    key: Vec<u8>,
    value: Vec<u8>,
    commit_ts: u64,
    retention: usize,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DurableAppendRequest {
    pub partition: String,
    pub key: String,
    pub value: String,
    pub retention: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DurableReadQuery {
    pub partition: String,
    pub from: Option<u64>,
    pub limit: Option<usize>,
}

async fn presence_join(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: PresenceJoinRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_publish() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if request.channel.is_empty() || request.member.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("presence")));
    }
    if request.channel.len() > 256 || request.member.len() > 256 {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("presence")));
    }
    if request
        .state
        .as_ref()
        .map(|state| serde_json::to_string(state).map(|text| text.len()).unwrap_or(0))
        .unwrap_or(0)
        > 4096
    {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("presence")));
    }
    if let Err(e) = admit_realtime(&state, &principal.tenant, 1) {
        return error_response(e);
    }
    let now = ryme_txn::now_unix();
    let member = request.member;
    let state_value = request.state.unwrap_or(serde_json::Value::Null);
    let ttl = request.ttl_secs.unwrap_or(60).clamp(1, ryme_realtime::PRESENCE_MAX_TTL_SECS);
    if let Some(node) = state.raft_node() {
        if !node.is_leader().await {
            return error_response(ryme_error::RymeError::Unavailable(String::from("not leader")));
        }
        let event = ClusterPresence::Join {
            tenant: principal.tenant.clone(),
            channel: request.channel.clone(),
            member,
            state: state_value,
            expires_unix: now.saturating_add(ttl),
            now_unix: now,
        };
        let payload = match serde_json::to_vec(&event) {
            Ok(payload) => payload,
            Err(error) => {
                return error_response(ryme_error::RymeError::Internal(error.to_string()))
            }
        };
        return match node.fanout_presence(payload).await {
            Ok(()) => {
                let count =
                    state.realtime.presence_list(&principal.tenant, &request.channel, now).len();
                (StatusCode::OK, Json(serde_json::json!({ "members": count }))).into_response()
            }
            Err(error) => error_response(error),
        };
    }
    match state.realtime.presence_join(
        &principal.tenant,
        &request.channel,
        member,
        state_value,
        ttl,
        now,
    ) {
        Ok(count) => {
            (StatusCode::OK, Json(serde_json::json!({ "members": count }))).into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn presence_leave(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: PresenceLeaveRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_publish() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if request.channel.is_empty() || request.member.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("presence")));
    }
    if request.channel.len() > 256 || request.member.len() > 256 {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("presence")));
    }
    if let Some(node) = state.raft_node() {
        if !node.is_leader().await {
            return error_response(ryme_error::RymeError::Unavailable(String::from("not leader")));
        }
        let now = ryme_txn::now_unix();
        let removed = state
            .realtime
            .presence_list(&principal.tenant, &request.channel, now)
            .iter()
            .any(|member| member.member == request.member);
        let event = ClusterPresence::Leave {
            tenant: principal.tenant.clone(),
            channel: request.channel.clone(),
            member: request.member.clone(),
        };
        let payload = match serde_json::to_vec(&event) {
            Ok(payload) => payload,
            Err(error) => {
                return error_response(ryme_error::RymeError::Internal(error.to_string()))
            }
        };
        return match node.fanout_presence(payload).await {
            Ok(()) => {
                (StatusCode::OK, Json(serde_json::json!({ "removed": removed }))).into_response()
            }
            Err(error) => error_response(error),
        };
    }
    match state.realtime.presence_leave(&principal.tenant, &request.channel, &request.member) {
        Ok(removed) => {
            (StatusCode::OK, Json(serde_json::json!({ "removed": removed }))).into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn presence_list(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(channel): Path<String>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let now = ryme_txn::now_unix();
    let members = state.realtime.presence_list(&principal.tenant, &channel, now);
    (StatusCode::OK, Json(members)).into_response()
}

async fn presence_stream(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(channel): Path<String>,
    Query(query): Query<PresenceStreamQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let principal = match state.principal_with_query(&headers, query.api_key.as_deref()) {
        Ok(principal) => principal,
        Err(error) => return error_response(error),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if channel.is_empty() || channel.len() > 256 {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("presence")));
    }
    if let Err(error) = admit_realtime(&state, &principal.tenant, 1) {
        return error_response(error);
    }
    let connection = match open_realtime_connection(&state, &principal.tenant) {
        Ok(connection) => connection,
        Err(error) => return error_response(error),
    };
    let realtime = state.realtime.clone();
    let tenant = principal.tenant.clone();
    let qos = state.qos.clone();
    upgrade.on_upgrade(move |socket| async move {
        let _connection = connection;
        forward_presence(socket, realtime, qos, &tenant, &channel).await;
    })
}

async fn supabase_realtime_stream(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<SupabaseRealtimeQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let api_key = query.apikey.as_deref().or(query.api_key.as_deref());
    let principal = match state.principal_with_query(&headers, api_key) {
        Ok(principal) => principal,
        Err(error) => return error_response(error),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let array_protocol = match query.vsn.as_deref() {
        None | Some("1.0.0") => false,
        Some("2.0.0") => true,
        Some(_) => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from("vsn")))
        }
    };
    if let Err(error) = admit_realtime(&state, &principal.tenant, 1) {
        return error_response(error);
    }
    let connection = match open_realtime_connection(&state, &principal.tenant) {
        Ok(connection) => connection,
        Err(error) => return error_response(error),
    };
    let realtime = state.realtime.clone();
    let qos = state.qos.clone();
    let tenant = principal.tenant.clone();
    let database = state.database.clone();
    let sender = principal.id.clone();
    let can_publish = principal.can_publish();
    upgrade.on_upgrade(move |socket| async move {
        let _connection = connection;
        forward_supabase_realtime(
            socket,
            state,
            realtime,
            qos,
            tenant,
            database,
            sender,
            can_publish,
            array_protocol,
        )
        .await;
    })
}

async fn publish_broadcast(
    state: &SharedState,
    tenant: &str,
    from: String,
    channel: String,
    payload: serde_json::Value,
) -> ryme_error::Result<u64> {
    admit_realtime(state, tenant, 1)?;
    let commit = state.backend.latest_commit();
    if let Some(node) = state.raft_node() {
        if !node.is_leader().await {
            return Err(ryme_error::RymeError::Unavailable(String::from("not leader")));
        }
        let sequence = state.realtime.reserve_sequence();
        let event = ClusterBroadcast {
            tenant: tenant.to_string(),
            channel,
            from,
            payload,
            commit_ts: commit,
            sequence,
        };
        let encoded = serde_json::to_vec(&event)
            .map_err(|error| ryme_error::RymeError::Internal(error.to_string()))?;
        node.fanout_realtime(encoded).await?;
        return Ok(sequence);
    }
    state.realtime.broadcast(tenant, &channel, from, payload, commit)
}

async fn broadcast_post(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: BroadcastRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_publish() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if request.channel.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("channel")));
    }
    if request.channel.len() > 256 {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("channel")));
    }
    match publish_broadcast(
        &state,
        &principal.tenant,
        request.from.unwrap_or_else(|| principal.id.clone()),
        request.channel,
        request.payload,
    )
    .await
    {
        Ok(sequence) => {
            (StatusCode::OK, Json(serde_json::json!({ "sequence": sequence }))).into_response()
        }
        Err(e) => error_response(e),
    }
}

#[derive(Debug)]
struct SupabaseFrame {
    join_ref: Option<String>,
    reference: Option<String>,
    topic: String,
    event: String,
    payload: serde_json::Value,
}

static NEXT_SUPABASE_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_SUPABASE_PRESENCE_KEY: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
struct SupabaseChangeSubscription {
    id: u64,
    event: String,
    schema: String,
    table: Option<String>,
    filter: Option<String>,
    select: Option<Vec<String>>,
}

#[derive(Debug)]
enum SupabaseRealtimeEvent {
    Broadcast(String, ryme_realtime::BroadcastMsg),
    Change(String, SupabaseChangeSubscription, ryme_realtime::ChangeRecord),
    Presence(String, ryme_realtime::PresenceEvent),
}

#[derive(Debug)]
struct SupabaseChannelState {
    broadcast_task: tokio::task::JoinHandle<()>,
    postgres_tasks: Vec<tokio::task::JoinHandle<()>>,
    presence_task: Option<tokio::task::JoinHandle<()>>,
    presence_key: Option<String>,
    presence_seen_sequence: u64,
    ack: bool,
    include_self: bool,
}

fn parse_supabase_frame(value: serde_json::Value) -> Option<(SupabaseFrame, bool)> {
    if let serde_json::Value::Array(values) = value {
        if values.len() != 5 {
            return None;
        }
        let text =
            |index: usize| values.get(index).and_then(serde_json::Value::as_str).map(String::from);
        return Some((
            SupabaseFrame {
                join_ref: text(0),
                reference: text(1),
                topic: text(2)?,
                event: text(3)?,
                payload: values.get(4).cloned().unwrap_or(serde_json::Value::Null),
            },
            true,
        ));
    }
    let serde_json::Value::Object(object) = value else { return None };
    Some((
        SupabaseFrame {
            join_ref: object.get("join_ref").and_then(serde_json::Value::as_str).map(String::from),
            reference: object.get("ref").and_then(serde_json::Value::as_str).map(String::from),
            topic: object.get("topic").and_then(serde_json::Value::as_str)?.to_string(),
            event: object.get("event").and_then(serde_json::Value::as_str)?.to_string(),
            payload: object.get("payload").cloned().unwrap_or(serde_json::Value::Null),
        },
        false,
    ))
}

fn encode_supabase_frame(frame: &SupabaseFrame, array_protocol: bool) -> String {
    if array_protocol {
        serde_json::json!([
            frame.join_ref,
            frame.reference,
            frame.topic,
            frame.event,
            frame.payload,
        ])
        .to_string()
    } else {
        serde_json::json!({
            "join_ref": frame.join_ref,
            "ref": frame.reference,
            "topic": frame.topic,
            "event": frame.event,
            "payload": frame.payload,
        })
        .to_string()
    }
}

fn supabase_reply(
    frame: &SupabaseFrame,
    status: &str,
    response: serde_json::Value,
    array_protocol: bool,
) -> String {
    encode_supabase_frame(
        &SupabaseFrame {
            join_ref: frame.join_ref.clone(),
            reference: frame.reference.clone(),
            topic: frame.topic.clone(),
            event: String::from("phx_reply"),
            payload: serde_json::json!({ "status": status, "response": response }),
        },
        array_protocol,
    )
}

fn supabase_subscription_config(
    payload: &serde_json::Value,
) -> Result<Vec<SupabaseChangeSubscription>, String> {
    let Some(value) = payload.pointer("/config/postgres_changes") else {
        return Ok(Vec::new());
    };
    let Some(entries) = value.as_array() else {
        return Err(String::from("postgres_changes must be an array"));
    };
    let mut subscriptions = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(object) = entry.as_object() else {
            return Err(String::from("postgres_changes entry must be an object"));
        };
        let event = object
            .get("event")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("*")
            .to_ascii_uppercase();
        if !matches!(event.as_str(), "*" | "INSERT" | "UPDATE" | "DELETE") {
            return Err(String::from("postgres_changes event"));
        }
        let schema = object
            .get("schema")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("public")
            .to_string();
        if !schema.eq_ignore_ascii_case("public") {
            return Err(String::from("only public schema is supported"));
        }
        let table = object
            .get("table")
            .and_then(serde_json::Value::as_str)
            .filter(|table| !table.is_empty())
            .map(String::from);
        let filter = object.get("filter").and_then(serde_json::Value::as_str).map(String::from);
        let select = object.get("select").map(|value| {
            value
                .as_array()
                .map(|values| {
                    values
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(String::from)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        });
        let id = NEXT_SUPABASE_SUBSCRIPTION_ID.fetch_add(1, AtomicOrdering::Relaxed);
        subscriptions.push(SupabaseChangeSubscription { id, event, schema, table, filter, select });
    }
    Ok(subscriptions)
}

fn supabase_presence_config(
    payload: &serde_json::Value,
) -> Result<Option<String>, String> {
    let Some(value) = payload.pointer("/config/presence") else {
        return Ok(None);
    };
    let Some(object) = value.as_object() else {
        return Err(String::from("presence must be an object"));
    };
    if object.get("enabled").and_then(serde_json::Value::as_bool) == Some(false) {
        return Ok(None);
    }
    let key = object
        .get("key")
        .and_then(serde_json::Value::as_str)
        .filter(|key| !key.is_empty())
        .map(String::from)
        .unwrap_or_else(|| format!("ryme-{}", NEXT_SUPABASE_PRESENCE_KEY.fetch_add(1, AtomicOrdering::Relaxed)));
    if key.len() > 256 {
        return Err(String::from("presence key"));
    }
    Ok(Some(key))
}

fn supabase_presence_meta(
    member: &ryme_realtime::PresenceMember,
    phx_ref: String,
) -> serde_json::Value {
    let mut meta = match member.state.clone() {
        serde_json::Value::Object(object) => object,
        state => {
            let mut object = serde_json::Map::new();
            object.insert(String::from("state"), state);
            object
        }
    };
    meta.insert(String::from("phx_ref"), serde_json::Value::String(phx_ref));
    serde_json::Value::Object(meta)
}

fn supabase_presence_state(
    members: &[ryme_realtime::PresenceMember],
) -> serde_json::Value {
    let mut state = serde_json::Map::new();
    for member in members {
        state.insert(
            member.member.clone(),
            serde_json::json!({
                "metas": [supabase_presence_meta(member, format!("ryme-{}", member.expires_unix))]
            }),
        );
    }
    serde_json::Value::Object(state)
}

fn supabase_presence_diff(event: &ryme_realtime::PresenceEvent) -> serde_json::Value {
    let mut joins = serde_json::Map::new();
    let mut leaves = serde_json::Map::new();
    let member = ryme_realtime::PresenceMember {
        member: event.member.clone(),
        state: event.state.clone(),
        expires_unix: event.expires_unix,
    };
    let target = if event.kind == "leave" { &mut leaves } else { &mut joins };
    target.insert(
        event.member.clone(),
        serde_json::json!({
            "metas": [supabase_presence_meta(&member, format!("ryme-{}", event.sequence))]
        }),
    );
    serde_json::json!({ "joins": joins, "leaves": leaves })
}

fn supabase_json_value(record: &ryme_realtime::ChangeRecord, after: bool) -> serde_json::Value {
    let raw = if after { record.after.as_deref() } else { record.before.as_deref() };
    let Some(raw) = raw else { return serde_json::Value::Object(serde_json::Map::new()) };
    if let Ok(value) = serde_json::from_slice(raw) {
        return value;
    }
    serde_json::json!({
        "id": String::from_utf8_lossy(&record.pk),
        "value": String::from_utf8_lossy(raw),
    })
}

fn supabase_column_value(
    record: &ryme_realtime::ChangeRecord,
    value: &serde_json::Value,
    column: &str,
) -> serde_json::Value {
    if column.eq_ignore_ascii_case("id")
        || column.eq_ignore_ascii_case("key")
        || column.eq_ignore_ascii_case("pk")
    {
        return serde_json::Value::String(String::from_utf8_lossy(&record.pk).into_owned());
    }
    if column.eq_ignore_ascii_case("value") || column.eq_ignore_ascii_case("data") {
        if let Some(value) = value.as_object().and_then(|object| object.get(column)).cloned() {
            return value;
        }
    }
    value
        .as_object()
        .and_then(|object| {
            object
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(column))
                .map(|(_, value)| value.clone())
        })
        .unwrap_or(serde_json::Value::Null)
}

fn supabase_filter_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Null => String::from("null"),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        _ => value.to_string(),
    }
}

fn supabase_filter_matches(
    filter: Option<&str>,
    record: &ryme_realtime::ChangeRecord,
    value: &serde_json::Value,
) -> bool {
    let Some(filter) = filter else { return true };
    rest_compound_filters(filter).into_iter().all(|condition| {
        let Some((column, expression)) = condition.split_once('=') else { return false };
        let (negated, expression) = expression
            .strip_prefix("not.")
            .map_or((false, expression), |expression| (true, expression));
        let Some((operator, expected)) = expression.split_once('.') else { return false };
        let actual = supabase_column_value(record, value, column.trim());
        let actual_text = supabase_filter_text(&actual);
        let expected = expected.trim();
        let matches = match operator.to_ascii_lowercase().as_str() {
            "eq" => actual_text == expected,
            "neq" => actual_text != expected,
            "is" => match expected.to_ascii_lowercase().as_str() {
                "null" => actual.is_null(),
                "true" => actual == serde_json::Value::Bool(true),
                "false" => actual == serde_json::Value::Bool(false),
                _ => false,
            },
            "in" => expected
                .strip_prefix('(')
                .and_then(|value| value.strip_suffix(')'))
                .map(|values| values.split(',').any(|value| value.trim() == actual_text))
                .unwrap_or(false),
            "like" | "ilike" => {
                rest_like(&actual_text, expected, operator.eq_ignore_ascii_case("ilike"))
            }
            "match" | "imatch" => {
                rest_like(&actual_text, expected, operator.eq_ignore_ascii_case("imatch"))
            }
            "gt" | "gte" | "lt" | "lte" => {
                let actual_number = actual_text.parse::<f64>().ok();
                let expected_number = expected.parse::<f64>().ok();
                match (actual_number, expected_number, operator.to_ascii_lowercase().as_str()) {
                    (Some(actual), Some(expected), "gt") => actual > expected,
                    (Some(actual), Some(expected), "gte") => actual >= expected,
                    (Some(actual), Some(expected), "lt") => actual < expected,
                    (Some(actual), Some(expected), "lte") => actual <= expected,
                    _ => false,
                }
            }
            "isdistinct" => actual_text != expected,
            _ => false,
        };
        if negated {
            !matches
        } else {
            matches
        }
    })
}

fn supabase_project_value(
    value: serde_json::Value,
    select: Option<&[String]>,
) -> serde_json::Value {
    let Some(select) = select else { return value };
    let serde_json::Value::Object(object) = value else { return value };
    let mut projected = serde_json::Map::new();
    for column in select {
        if let Some((name, value)) =
            object.iter().find(|(name, _)| name.eq_ignore_ascii_case(column))
        {
            projected.insert(name.clone(), value.clone());
        }
    }
    serde_json::Value::Object(projected)
}

fn supabase_column_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(number) if number.is_i64() || number.is_u64() => "int8",
        serde_json::Value::Number(_) => "float8",
        serde_json::Value::Array(_) => "jsonb",
        serde_json::Value::Object(_) => "jsonb",
        serde_json::Value::Null | serde_json::Value::String(_) => "text",
    }
}

fn supabase_columns(value: &serde_json::Value) -> Vec<serde_json::Value> {
    value
        .as_object()
        .map(|object| {
            object
                .iter()
                .map(|(name, value)| serde_json::json!({ "name": name, "type": supabase_column_type(value) }))
                .collect()
        })
        .unwrap_or_default()
}

fn supabase_change_payload(
    subscription: &SupabaseChangeSubscription,
    record: &ryme_realtime::ChangeRecord,
) -> serde_json::Value {
    let before =
        supabase_project_value(supabase_json_value(record, false), subscription.select.as_deref());
    let after =
        supabase_project_value(supabase_json_value(record, true), subscription.select.as_deref());
    let record_value = if matches!(record.op, ryme_realtime::Operation::Delete) {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        after.clone()
    };
    let old_record = if matches!(record.op, ryme_realtime::Operation::Insert) {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        before.clone()
    };
    let event = match record.op {
        ryme_realtime::Operation::Insert => "INSERT",
        ryme_realtime::Operation::Update => "UPDATE",
        ryme_realtime::Operation::Delete => "DELETE",
    };
    serde_json::json!({
        "ids": [subscription.id],
        "data": {
            "schema": subscription.schema,
            "table": record.table,
            "commit_timestamp": record.commit_ts.to_string(),
            "type": event,
            "columns": supabase_columns(&record_value),
            "record": record_value,
            "old_record": old_record,
            "errors": serde_json::Value::Null,
        }
    })
}

fn supabase_change_matches(
    subscription: &SupabaseChangeSubscription,
    record: &ryme_realtime::ChangeRecord,
) -> bool {
    let event = match record.op {
        ryme_realtime::Operation::Insert => "INSERT",
        ryme_realtime::Operation::Update => "UPDATE",
        ryme_realtime::Operation::Delete => "DELETE",
    };
    if subscription.event != "*" && subscription.event != event {
        return false;
    }
    if subscription.table.as_deref().is_some_and(|table| table != record.table) {
        return false;
    }
    let value = supabase_json_value(record, !matches!(record.op, ryme_realtime::Operation::Delete));
    supabase_filter_matches(subscription.filter.as_deref(), record, &value)
}

fn spawn_supabase_broadcast_forwarder(
    realtime: &Realtime,
    tenant: &str,
    topic: &str,
    events: &tokio::sync::mpsc::Sender<SupabaseRealtimeEvent>,
) -> tokio::task::JoinHandle<()> {
    let channel = topic.strip_prefix("realtime:").unwrap_or(topic).to_string();
    let mut receiver = realtime.broadcast_subscribe(tenant, &channel);
    let topic = topic.to_string();
    let events = events.clone();
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(record) => {
                    if events
                        .try_send(SupabaseRealtimeEvent::Broadcast(topic.clone(), record))
                        .is_err()
                    {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
                | Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

fn spawn_supabase_presence_forwarder(
    realtime: &Realtime,
    tenant: &str,
    topic: &str,
    events: &tokio::sync::mpsc::Sender<SupabaseRealtimeEvent>,
) -> tokio::task::JoinHandle<()> {
    let channel = topic.strip_prefix("realtime:").unwrap_or(topic).to_string();
    let mut receiver = realtime.presence_subscribe(tenant, &channel);
    let topic = topic.to_string();
    let events = events.clone();
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    if events
                        .try_send(SupabaseRealtimeEvent::Presence(topic.clone(), event))
                        .is_err()
                    {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
                | Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

async fn supabase_presence_join(
    state: &SharedState,
    tenant: &str,
    channel: &str,
    member: String,
    presence_state: serde_json::Value,
) -> ryme_error::Result<usize> {
    let now = ryme_txn::now_unix();
    if let Some(node) = state.raft_node() {
        if !node.is_leader().await {
            return Err(ryme_error::RymeError::Unavailable(String::from("not leader")));
        }
        let event = ClusterPresence::Join {
            tenant: tenant.to_string(),
            channel: channel.to_string(),
            member,
            state: presence_state,
            expires_unix: now.saturating_add(ryme_realtime::PRESENCE_MAX_TTL_SECS),
            now_unix: now,
        };
        let payload = serde_json::to_vec(&event)
            .map_err(|error| ryme_error::RymeError::Internal(error.to_string()))?;
        node.fanout_presence(payload).await?;
        Ok(state.realtime.presence_list(tenant, channel, now).len())
    } else {
        state.realtime.presence_join(
            tenant,
            channel,
            member,
            presence_state,
            ryme_realtime::PRESENCE_MAX_TTL_SECS,
            now,
        )
    }
}

async fn supabase_presence_leave(
    state: &SharedState,
    tenant: &str,
    channel: &str,
    member: &str,
) -> ryme_error::Result<bool> {
    if let Some(node) = state.raft_node() {
        if !node.is_leader().await {
            return Err(ryme_error::RymeError::Unavailable(String::from("not leader")));
        }
        let event = ClusterPresence::Leave {
            tenant: tenant.to_string(),
            channel: channel.to_string(),
            member: member.to_string(),
        };
        let payload = serde_json::to_vec(&event)
            .map_err(|error| ryme_error::RymeError::Internal(error.to_string()))?;
        let existed = state
            .realtime
            .presence_list(tenant, channel, ryme_txn::now_unix())
            .iter()
            .any(|current| current.member == member);
        node.fanout_presence(payload).await?;
        Ok(existed)
    } else {
        state.realtime.presence_leave(tenant, channel, member)
    }
}

fn spawn_supabase_change_forwarder(
    realtime: &Realtime,
    tenant: &str,
    database: &str,
    branch: &str,
    topic: &str,
    subscription: SupabaseChangeSubscription,
    events: &tokio::sync::mpsc::Sender<SupabaseRealtimeEvent>,
) -> tokio::task::JoinHandle<()> {
    let mut receiver = match subscription.table.as_deref() {
        Some(table) => realtime.subscribe_branch(tenant, database, branch, table),
        None => realtime.subscribe_all_changes(),
    };
    let tenant = tenant.to_string();
    let database = database.to_string();
    let branch = branch.to_string();
    let topic = topic.to_string();
    let events = events.clone();
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(record)
                    if record.tenant == tenant
                        && record.database == database
                        && record.branch == branch
                        && supabase_change_matches(&subscription, &record) =>
                {
                    if events
                        .try_send(SupabaseRealtimeEvent::Change(
                            topic.clone(),
                            subscription.clone(),
                            record,
                        ))
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
                | Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

async fn forward_supabase_realtime(
    socket: axum::extract::ws::WebSocket,
    state: SharedState,
    realtime: Realtime,
    qos: Arc<Mutex<QosRegistry>>,
    tenant: String,
    database: String,
    sender: String,
    can_publish: bool,
    array_protocol: bool,
) {
    let (sink, mut incoming) = socket.split();
    let outgoing = start_realtime_writer(sink);
    let (events, mut event_queue) =
        tokio::sync::mpsc::channel::<SupabaseRealtimeEvent>(REALTIME_OUTGOING_QUEUE_CAPACITY);
    let mut channels: HashMap<String, SupabaseChannelState> = HashMap::new();
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Ping(Vec::new())) {
                    break;
                }
            }
            event = event_queue.recv(), if !channels.is_empty() => {
                let Some(event) = event else { break };
                let (_, text) = match event {
                    SupabaseRealtimeEvent::Broadcast(topic, record) => {
                        let Some(channel) = channels.get(&topic) else { continue };
                        if !channel.include_self && record.from == sender {
                            continue;
                        }
                        let (event_name, payload) = match record.payload {
                            serde_json::Value::Object(mut payload) => {
                                let event_name = payload.remove("event")
                                    .and_then(|value| value.as_str().map(String::from))
                                    .unwrap_or_else(|| String::from("message"));
                                let payload = payload.remove("payload").unwrap_or(serde_json::Value::Object(payload));
                                (event_name, payload)
                            }
                            payload => (String::from("message"), payload),
                        };
                        let text = encode_supabase_frame(
                            &SupabaseFrame {
                                join_ref: None,
                                reference: None,
                                topic: topic.clone(),
                                event: String::from("broadcast"),
                                payload: serde_json::json!({
                                    "event": event_name,
                                    "payload": payload,
                                    "type": "broadcast",
                                }),
                            },
                            array_protocol,
                        );
                        (topic, text)
                    }
                    SupabaseRealtimeEvent::Change(topic, subscription, record) => {
                        let Some(channel) = channels.get(&topic) else { continue };
                        if channel.postgres_tasks.is_empty() {
                            continue;
                        }
                        let text = encode_supabase_frame(
                            &SupabaseFrame {
                                join_ref: None,
                                reference: None,
                                topic: topic.clone(),
                                event: String::from("postgres_changes"),
                                payload: supabase_change_payload(&subscription, &record),
                            },
                            array_protocol,
                        );
                        (topic, text)
                    }
                    SupabaseRealtimeEvent::Presence(topic, event) => {
                        let Some(channel) = channels.get_mut(&topic) else { continue };
                        if channel.presence_task.is_none()
                            || event.sequence <= channel.presence_seen_sequence
                        {
                            continue;
                        }
                        channel.presence_seen_sequence = event.sequence;
                        let text = encode_supabase_frame(
                            &SupabaseFrame {
                                join_ref: None,
                                reference: None,
                                topic: topic.clone(),
                                event: String::from("presence_diff"),
                                payload: supabase_presence_diff(&event),
                            },
                            array_protocol,
                        );
                        (topic, text)
                    }
                };
                if !stream_realtime_event(&qos, &tenant, text.len() as u64)
                    || !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(text)) {
                    break;
                }
            }
            next = incoming.next() => {
                let Some(Ok(message)) = next else { break };
                let axum::extract::ws::Message::Text(text) = message else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                    continue;
                };
                let Some((frame, frame_array)) = parse_supabase_frame(value) else {
                    continue;
                };
                let array_protocol = frame_array || array_protocol;
                match frame.event.as_str() {
                    "heartbeat" if frame.topic == "phoenix" => {
                        let reply = supabase_reply(&frame, "ok", serde_json::json!({}), array_protocol);
                        if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(reply)) {
                            break;
                        }
                    }
                    "phx_join" => {
                        if !frame.topic.starts_with("realtime:") || frame.topic.len() > 256 {
                            let reply = supabase_reply(&frame, "error", serde_json::json!({"reason":"invalid topic"}), array_protocol);
                            if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(reply)) { break; }
                            continue;
                        }
                        if let Some(channel) = channels.remove(&frame.topic) {
                            channel.broadcast_task.abort();
                            for task in channel.postgres_tasks {
                                task.abort();
                            }
                            if let Some(presence_key) = channel.presence_key {
                                let _ = supabase_presence_leave(
                                    &state,
                                    &tenant,
                                    frame.topic.strip_prefix("realtime:").unwrap_or(&frame.topic),
                                    &presence_key,
                                )
                                .await;
                            }
                            if let Some(task) = channel.presence_task {
                                task.abort();
                            }
                        }
                        let ack = frame.payload.pointer("/config/broadcast/ack")
                            .and_then(serde_json::Value::as_bool).unwrap_or(false);
                        let include_self = frame.payload.pointer("/config/broadcast/self")
                            .and_then(serde_json::Value::as_bool).unwrap_or(false);
                        let task = spawn_supabase_broadcast_forwarder(&realtime, &tenant, &frame.topic, &events);
                        let subscriptions = match supabase_subscription_config(&frame.payload) {
                            Ok(subscriptions) => subscriptions,
                            Err(reason) => {
                                task.abort();
                                let reply = supabase_reply(
                                    &frame,
                                    "error",
                                    serde_json::json!({ "reason": reason }),
                                    array_protocol,
                                );
                                if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(reply)) { break; }
                                continue;
                            }
                        };
                        let presence_key = match supabase_presence_config(&frame.payload) {
                            Ok(key) => key,
                            Err(reason) => {
                                task.abort();
                                let reply = supabase_reply(
                                    &frame,
                                    "error",
                                    serde_json::json!({ "reason": reason }),
                                    array_protocol,
                                );
                                if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(reply)) { break; }
                                continue;
                            }
                        };
                        let presence_task = presence_key.as_ref().map(|_| {
                            spawn_supabase_presence_forwarder(&realtime, &tenant, &frame.topic, &events)
                        });
                        let postgres_tasks = subscriptions
                            .iter()
                            .cloned()
                            .map(|subscription| {
                                spawn_supabase_change_forwarder(
                                    &realtime,
                                    &tenant,
                                    &database,
                                    "main",
                                    &frame.topic,
                                    subscription,
                                    &events,
                                )
                            })
                            .collect();
                        channels.insert(frame.topic.clone(), SupabaseChannelState {
                            broadcast_task: task,
                            postgres_tasks,
                            presence_task,
                            presence_key: presence_key.clone(),
                            presence_seen_sequence: 0,
                            ack,
                            include_self,
                        });
                        let response = serde_json::json!({
                            "postgres_changes": subscriptions.iter().map(|subscription| serde_json::json!({
                                "id": subscription.id,
                                "event": subscription.event,
                                "schema": subscription.schema,
                                "table": subscription.table,
                            })).collect::<Vec<_>>()
                        });
                        let reply = supabase_reply(&frame, "ok", response, array_protocol);
                        if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(reply)) { break; }
                        if presence_key.is_some() {
                            let channel_name = frame
                                .topic
                                .strip_prefix("realtime:")
                                .unwrap_or(&frame.topic);
                            let (members, sequence) =
                                realtime.presence_snapshot(&tenant, channel_name, ryme_txn::now_unix());
                            if let Some(channel) = channels.get_mut(&frame.topic) {
                                channel.presence_seen_sequence = sequence;
                            }
                            let presence_state = encode_supabase_frame(
                                &SupabaseFrame {
                                    join_ref: None,
                                    reference: None,
                                    topic: frame.topic.clone(),
                                    event: String::from("presence_state"),
                                    payload: supabase_presence_state(&members),
                                },
                                array_protocol,
                            );
                            if !queue_realtime_message(
                                &outgoing,
                                axum::extract::ws::Message::Text(presence_state),
                            ) {
                                break;
                            }
                        }
                        if !subscriptions.is_empty() {
                            let system = encode_supabase_frame(
                                &SupabaseFrame {
                                    join_ref: None,
                                    reference: None,
                                    topic: frame.topic.clone(),
                                    event: String::from("system"),
                                    payload: serde_json::json!({
                                        "message": "Subscribed to PostgreSQL",
                                        "status": "ok",
                                        "extension": "postgres_changes",
                                        "channel": "main",
                                    }),
                                },
                                array_protocol,
                            );
                            if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(system)) { break; }
                        }
                    }
                    "phx_leave" => {
                        if let Some(channel) = channels.remove(&frame.topic) {
                            channel.broadcast_task.abort();
                            for task in channel.postgres_tasks {
                                task.abort();
                            }
                            if let Some(task) = channel.presence_task {
                                task.abort();
                            }
                            if let Some(presence_key) = channel.presence_key {
                                let _ = supabase_presence_leave(
                                    &state,
                                    &tenant,
                                    frame.topic.strip_prefix("realtime:").unwrap_or(&frame.topic),
                                    &presence_key,
                                )
                                .await;
                            }
                        }
                        let reply = supabase_reply(&frame, "ok", serde_json::json!({}), array_protocol);
                        if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(reply)) { break; }
                    }
                    "broadcast" => {
                        if !can_publish || !channels.contains_key(&frame.topic) {
                            let reply = supabase_reply(&frame, "error", serde_json::json!({"reason":"not joined or not allowed"}), array_protocol);
                            if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(reply)) { break; }
                            continue;
                        }
                        let channel = frame.topic.strip_prefix("realtime:").unwrap_or(&frame.topic);
                        let (event_name, payload) = match frame.payload.clone() {
                            serde_json::Value::Object(mut payload) => {
                                let event_name = payload.remove("event")
                                    .and_then(|value| value.as_str().map(String::from))
                                    .unwrap_or_else(|| String::from("message"));
                                let payload = payload.remove("payload").unwrap_or(serde_json::Value::Object(payload));
                                (event_name, payload)
                            }
                            payload => (String::from("message"), payload),
                        };
                        let published = publish_broadcast(
                            &state,
                            &tenant,
                            sender.clone(),
                            channel.to_string(),
                            serde_json::json!({ "event": event_name, "payload": payload }),
                        ).await;
                        if channels.get(&frame.topic).is_some_and(|channel| channel.ack) {
                            let (status, response) = match published {
                                Ok(sequence) => ("ok", serde_json::json!({ "sequence": sequence })),
                                Err(error) => ("error", serde_json::json!({ "reason": error.to_string() })),
                            };
                            let reply = supabase_reply(&frame, status, response, array_protocol);
                            if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(reply)) { break; }
                        }
                    }
                    "presence" => {
                        let presence_key = channels
                            .get(&frame.topic)
                            .and_then(|channel| channel.presence_key.clone());
                        let presence_event = frame
                            .payload
                            .get("event")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("");
                        let channel_name = frame
                            .topic
                            .strip_prefix("realtime:")
                            .unwrap_or(&frame.topic);
                        let result = if !can_publish {
                            Err(ryme_error::RymeError::Forbidden)
                        } else if presence_key.is_none() {
                            Err(ryme_error::RymeError::InvalidArgument(String::from(
                                "presence is not enabled",
                            )))
                        } else {
                            match presence_event {
                                "track" => {
                                    let state_value = frame
                                        .payload
                                        .get("payload")
                                        .cloned()
                                        .unwrap_or(serde_json::Value::Null);
                                    let size = serde_json::to_vec(&state_value)
                                        .map(|value| value.len())
                                        .unwrap_or(usize::MAX);
                                    if size > 4096 {
                                        Err(ryme_error::RymeError::InvalidArgument(String::from(
                                            "presence payload",
                                        )))
                                    } else {
                                        supabase_presence_join(
                                            &state,
                                            &tenant,
                                            channel_name,
                                            presence_key.unwrap_or_default(),
                                            state_value,
                                        )
                                        .await
                                        .map(|_| ())
                                    }
                                }
                                "untrack" => supabase_presence_leave(
                                    &state,
                                    &tenant,
                                    channel_name,
                                    &presence_key.unwrap_or_default(),
                                )
                                .await
                                .map(|_| ()),
                                _ => Err(ryme_error::RymeError::InvalidArgument(String::from(
                                    "presence event",
                                ))),
                            }
                        };
                        let (status, response) = match result {
                            Ok(()) => ("ok", serde_json::json!({})),
                            Err(error) => (
                                "error",
                                serde_json::json!({ "reason": error.to_string() }),
                            ),
                        };
                        let reply = supabase_reply(&frame, status, response, array_protocol);
                        if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(reply)) { break; }
                    }
                    _ => {}
                }
            }
        }
    }
    for (topic, channel) in channels {
        channel.broadcast_task.abort();
        for task in channel.postgres_tasks {
            task.abort();
        }
        if let Some(task) = channel.presence_task {
            task.abort();
        }
        if let Some(presence_key) = channel.presence_key {
            let channel_name = topic.strip_prefix("realtime:").unwrap_or(&topic);
            let _ = supabase_presence_leave(&state, &tenant, channel_name, &presence_key).await;
        }
    }
}

async fn broadcast_stream(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(channel): Path<String>,
    Query(query): Query<BroadcastStreamQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let principal = match state.principal_with_query(&headers, query.api_key.as_deref()) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if channel.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("channel")));
    }
    if let Err(e) = admit_realtime(&state, &principal.tenant, 1) {
        return error_response(e);
    }
    let connection = match open_realtime_connection(&state, &principal.tenant) {
        Ok(connection) => connection,
        Err(e) => return error_response(e),
    };
    let realtime = state.realtime.clone();
    let tenant = principal.tenant.clone();
    let qos = state.qos.clone();
    upgrade.on_upgrade(move |socket| async move {
        let _connection = connection;
        forward_broadcast(socket, realtime, qos, &tenant, &channel).await;
    })
}

async fn forward_broadcast(
    socket: axum::extract::ws::WebSocket,
    realtime: Realtime,
    qos: Arc<Mutex<QosRegistry>>,
    tenant: &str,
    channel: &str,
) {
    let mut receiver = realtime.broadcast_subscribe(tenant, channel);
    let (sender, mut incoming) = socket.split();
    let outgoing = start_realtime_writer(sender);
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !queue_realtime_message(
                    &outgoing,
                    axum::extract::ws::Message::Ping(Vec::new()),
                ) {
                    break;
                }
            }
            message = receiver.recv() => {
                match message {
                    Ok(record) => {
                        let text = serde_json::to_string(&record).unwrap_or_else(|_| String::from("{}"));
                        if !stream_realtime_event(&qos, tenant, text.len() as u64) {
                            break;
                        }
                        if !queue_realtime_message(
                            &outgoing,
                            axum::extract::ws::Message::Text(text),
                        ) {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => break,
                    Err(_) => break,
                }
            }
            next = incoming.next() => {
                match next {
                    Some(Ok(axum::extract::ws::Message::Close(_))) | None => break,
                    Some(Ok(axum::extract::ws::Message::Ping(payload))) => {
                        if !queue_realtime_message(
                            &outgoing,
                            axum::extract::ws::Message::Pong(payload),
                        ) {
                            break;
                        }
                    }
                    _ => continue,
                }
            }
        }
    }
}

fn queue_presence_snapshot(
    outgoing: &RealtimeOutgoing,
    realtime: &Realtime,
    qos: &Arc<Mutex<QosRegistry>>,
    tenant: &str,
    channel: &str,
) -> Option<u64> {
    let (members, sequence) = realtime.presence_snapshot(tenant, channel, ryme_txn::now_unix());
    let text = serde_json::json!({
        "type": "presence_state",
        "channel": channel,
        "members": members,
        "sequence": sequence,
    })
    .to_string();
    if stream_realtime_event(qos, tenant, text.len() as u64)
        && queue_realtime_message(outgoing, axum::extract::ws::Message::Text(text))
    {
        Some(sequence)
    } else {
        None
    }
}

async fn forward_presence(
    socket: axum::extract::ws::WebSocket,
    realtime: Realtime,
    qos: Arc<Mutex<QosRegistry>>,
    tenant: &str,
    channel: &str,
) {
    let mut receiver = realtime.presence_subscribe(tenant, channel);
    let (sender, mut incoming) = socket.split();
    let outgoing = start_realtime_writer(sender);
    let Some(mut seen_sequence) =
        queue_presence_snapshot(&outgoing, &realtime, &qos, tenant, channel)
    else {
        return;
    };
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !queue_realtime_message(
                    &outgoing,
                    axum::extract::ws::Message::Ping(Vec::new()),
                ) {
                    break;
                }
            }
            message = receiver.recv() => {
                match message {
                    Ok(event) => {
                        if event.sequence <= seen_sequence {
                            continue;
                        }
                        seen_sequence = event.sequence;
                        let text = serde_json::to_string(&event)
                            .unwrap_or_else(|_| String::from("{}"));
                        if !stream_realtime_event(&qos, tenant, text.len() as u64)
                            || !queue_realtime_message(
                                &outgoing,
                                axum::extract::ws::Message::Text(text),
                            )
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let Some(sequence) = queue_presence_snapshot(
                            &outgoing,
                            &realtime,
                            &qos,
                            tenant,
                            channel,
                        ) else {
                            break;
                        };
                        seen_sequence = sequence;
                    }
                    Err(_) => break,
                }
            }
            next = incoming.next() => {
                match next {
                    Some(Ok(axum::extract::ws::Message::Close(_))) | None => break,
                    Some(Ok(axum::extract::ws::Message::Ping(payload))) => {
                        if !queue_realtime_message(
                            &outgoing,
                            axum::extract::ws::Message::Pong(payload),
                        ) {
                            break;
                        }
                    }
                    _ => continue,
                }
            }
        }
    }
}

async fn durable_stream(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(partition): Path<String>,
    Query(query): Query<DurableStreamQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let principal = match state.principal_with_query(&headers, query.api_key.as_deref()) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if partition.is_empty() || partition.contains('\0') || partition.len() > 256 {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("partition")));
    }
    if let Err(e) = admit_realtime(&state, &principal.tenant, 1) {
        return error_response(e);
    }
    let connection = match open_realtime_connection(&state, &principal.tenant) {
        Ok(connection) => connection,
        Err(e) => return error_response(e),
    };
    let realtime = state.realtime.clone();
    let tenant = principal.tenant.clone();
    let qos = state.qos.clone();
    let from = query.from.unwrap_or(0);
    upgrade.on_upgrade(move |socket| async move {
        let _connection = connection;
        forward_durable_topic(socket, realtime, qos, &tenant, &partition, from).await;
    })
}

fn durable_message_text(message: &ryme_realtime::DurableMsg) -> String {
    serde_json::to_string(&serde_json::json!({
        "partition": message.partition,
        "cursor": message.cursor,
        "key": String::from_utf8_lossy(&message.key),
        "value": String::from_utf8_lossy(&message.value),
        "commit_ts": message.commit_ts,
    }))
    .unwrap_or_else(|_| String::from("{}"))
}

fn queue_durable_message(
    outgoing: &RealtimeOutgoing,
    qos: &Arc<Mutex<QosRegistry>>,
    tenant: &str,
    message: &ryme_realtime::DurableMsg,
) -> bool {
    let text = durable_message_text(message);
    stream_realtime_event(qos, tenant, text.len() as u64)
        && queue_realtime_message(outgoing, axum::extract::ws::Message::Text(text))
}

async fn forward_durable_topic(
    socket: axum::extract::ws::WebSocket,
    realtime: Realtime,
    qos: Arc<Mutex<QosRegistry>>,
    tenant: &str,
    partition: &str,
    from: u64,
) {
    let mut receiver = realtime.durable_subscribe(tenant, partition);
    let replayed = realtime.durable_read(tenant, partition, from, 1000);
    let (sender, mut incoming) = socket.split();
    let outgoing = start_realtime_writer(sender);
    let mut next_cursor = from;
    for message in &replayed {
        next_cursor = next_cursor.max(message.cursor.saturating_add(1));
        if !queue_durable_message(&outgoing, &qos, tenant, message) {
            return;
        }
    }
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !queue_realtime_message(
                    &outgoing,
                    axum::extract::ws::Message::Ping(Vec::new()),
                ) {
                    break;
                }
            }
            message = receiver.recv() => {
                match message {
                    Ok(message) => {
                        if message.cursor < next_cursor {
                            continue;
                        }
                        next_cursor = message.cursor.saturating_add(1);
                        if !queue_durable_message(&outgoing, &qos, tenant, &message) {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let recovered = realtime.durable_read(tenant, partition, next_cursor, 1000);
                        if recovered.first().map(|message| message.cursor > next_cursor).unwrap_or(false) {
                            break;
                        }
                        for message in recovered {
                            if message.cursor < next_cursor {
                                continue;
                            }
                            next_cursor = message.cursor.saturating_add(1);
                            if !queue_durable_message(&outgoing, &qos, tenant, &message) {
                                return;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            next = incoming.next() => {
                match next {
                    Some(Ok(axum::extract::ws::Message::Close(_))) | None => break,
                    Some(Ok(axum::extract::ws::Message::Ping(payload))) => {
                        if !queue_realtime_message(
                            &outgoing,
                            axum::extract::ws::Message::Pong(payload),
                        ) {
                            break;
                        }
                    }
                    _ => continue,
                }
            }
        }
    }
}

async fn durable_append(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: DurableAppendRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if request.partition.is_empty() || request.partition.contains('\0') {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("partition")));
    }
    if request.partition.len() > 256 {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("partition")));
    }
    if request.key.is_empty() || request.key.len() > ryme_gateway::MAX_KEY_BYTES {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("key")));
    }
    if request.value.len() > ryme_gateway::MAX_VALUE_BYTES {
        return error_response(ryme_error::RymeError::Overload(String::from("value")));
    }
    let bytes = (request.key.len() + request.value.len()) as u64;
    if let Err(e) = admit_write(&state, &principal.tenant, bytes) {
        return error_response(e);
    }
    let _durable_persist_guard = state.durable_persist_lock.lock().await;
    let commit = state.backend.latest_commit();
    if let Some(node) = state.raft_node() {
        if !node.is_leader().await {
            return error_response(ryme_error::RymeError::Unavailable(String::from("not leader")));
        }
        let cursor = state.realtime.durable_cursor(&principal.tenant, &request.partition);
        let event = ClusterTopicAppend {
            tenant: principal.tenant.clone(),
            partition: request.partition.clone(),
            cursor,
            key: request.key.into_bytes(),
            value: request.value.into_bytes(),
            commit_ts: commit,
            retention: request.retention.unwrap_or(1024),
        };
        let payload = match serde_json::to_vec(&event) {
            Ok(payload) => payload,
            Err(error) => {
                return error_response(ryme_error::RymeError::Internal(error.to_string()))
            }
        };
        return match node.propose_topic(payload).await {
            Ok(_) => {
                (StatusCode::OK, Json(serde_json::json!({ "cursor": cursor }))).into_response()
            }
            Err(error) => error_response(error),
        };
    }
    match state.realtime.durable_append(
        &principal.tenant,
        &request.partition,
        request.key.into_bytes(),
        request.value.into_bytes(),
        commit,
        request.retention.unwrap_or(1024),
    ) {
        Ok(cursor) => match state.persist_durable_topics() {
            Ok(()) => {
                (StatusCode::OK, Json(serde_json::json!({ "cursor": cursor }))).into_response()
            }
            Err(e) => error_response(e),
        },
        Err(e) => error_response(e),
    }
}

async fn durable_read(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<DurableReadQuery>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let messages = state.realtime.durable_read(
        &principal.tenant,
        &query.partition,
        query.from.unwrap_or(0),
        query.limit.unwrap_or(100),
    );
    let egress: u64 = messages.iter().map(|msg| (msg.key.len() + msg.value.len()) as u64).sum();
    if let Err(e) = admit_egress(&state, &principal.tenant, egress) {
        return error_response(e);
    }
    let items: Vec<serde_json::Value> = messages
        .into_iter()
        .map(|msg| {
            serde_json::json!({
                "partition": msg.partition,
                "cursor": msg.cursor,
                "key": String::from_utf8_lossy(&msg.key),
                "value": String::from_utf8_lossy(&msg.value),
                "commit_ts": msg.commit_ts,
            })
        })
        .collect();
    (StatusCode::OK, Json(items)).into_response()
}

fn index_space(state: &SharedState, table: &str) -> String {
    format!("{}/{}/{}", state.tenant, state.database, table)
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct VectorUpsertRequest {
    pub table: String,
    pub id: String,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct VectorSearchRequest {
    pub table: String,
    pub vector: Vec<f32>,
    pub top_k: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TextIndexRequest {
    pub table: String,
    pub id: String,
    pub text: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TextSearchRequest {
    pub table: String,
    pub query: String,
    pub top_k: Option<usize>,
}

async fn vector_upsert(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: VectorUpsertRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if request.table.is_empty() || request.id.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from(
            "vector target",
        )));
    }
    if let Err(e) = admit_write(&state, &principal.tenant, (request.vector.len() * 4) as u64) {
        return error_response(e);
    }
    let space = index_space(&state, &request.table);
    match state.indexes.lock() {
        Ok(mut indexes) => match indexes.vector_upsert(&space, request.id, request.vector) {
            Ok(()) => {
                record_meter(&state, Metric::WriteUnit, 1);
                (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
            }
            Err(e) => error_response(e),
        },
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

async fn vector_search(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: VectorSearchRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = admit_read(&state, &principal.tenant) {
        return error_response(e);
    }
    let space = index_space(&state, &request.table);
    match state.indexes.lock() {
        Ok(indexes) => {
            match indexes.vector_search(&space, &request.vector, request.top_k.unwrap_or(10)) {
                Ok(hits) => (StatusCode::OK, Json(hits)).into_response(),
                Err(e) => error_response(e),
            }
        }
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AnnSearchRequest {
    pub table: String,
    pub vector: Vec<f32>,
    pub top_k: Option<usize>,
    pub ef: Option<usize>,
}

async fn vector_ann_search(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: AnnSearchRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = admit_read(&state, &principal.tenant) {
        return error_response(e);
    }
    let space = index_space(&state, &request.table);
    match state.indexes.lock() {
        Ok(indexes) => {
            match indexes.ann_search(
                &space,
                &request.vector,
                request.top_k.unwrap_or(10),
                request.ef.unwrap_or(64),
            ) {
                Ok(hits) => (StatusCode::OK, Json(hits)).into_response(),
                Err(e) => error_response(e),
            }
        }
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

async fn vector_delete(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path((table, id)): Path<(String, String)>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let space = index_space(&state, &table);
    match state.indexes.lock() {
        Ok(mut indexes) => {
            let removed = indexes.vector_remove(&space, &id);
            (StatusCode::OK, Json(serde_json::json!({ "removed": removed }))).into_response()
        }
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

async fn text_index(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: TextIndexRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if request.table.is_empty() || request.id.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("text target")));
    }
    if let Err(e) = admit_write(&state, &principal.tenant, request.text.len() as u64) {
        return error_response(e);
    }
    let space = index_space(&state, &request.table);
    match state.indexes.lock() {
        Ok(mut indexes) => match indexes.text_index(&space, request.id, &request.text) {
            Ok(terms) => {
                record_meter(&state, Metric::WriteUnit, 1);
                (StatusCode::OK, Json(serde_json::json!({ "terms": terms }))).into_response()
            }
            Err(e) => error_response(e),
        },
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

async fn text_search(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: TextSearchRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = admit_read(&state, &principal.tenant) {
        return error_response(e);
    }
    let space = index_space(&state, &request.table);
    match state.indexes.lock() {
        Ok(indexes) => {
            let hits = indexes.text_search(&space, &request.query, request.top_k.unwrap_or(10));
            (StatusCode::OK, Json(hits)).into_response()
        }
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

async fn text_delete(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path((table, id)): Path<(String, String)>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let space = index_space(&state, &table);
    match state.indexes.lock() {
        Ok(mut indexes) => {
            let removed = indexes.text_remove(&space, &id);
            (StatusCode::OK, Json(serde_json::json!({ "removed": removed }))).into_response()
        }
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

async fn index_stats(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    match state.indexes.lock() {
        Ok(indexes) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "partitions": indexes.len(),
                "spaces": indexes.spaces(),
            })),
        )
            .into_response(),
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    }
}

async fn kv_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path((table, key)): Path<(String, String)>,
) -> Response {
    let start = SystemTime::now();
    let result = inner_kv_get(&state, &headers, &table, key.as_bytes()).await;
    let micros = elapsed_micros(start);
    state.latency.observe_micros(micros);
    state.histogram.record(micros);
    record_meter(&state, Metric::ReadUnit, 1);
    record_span(&state, "kv_get", &[("table", table.as_str())], micros);
    result
}

async fn inner_kv_get(
    state: &SharedState,
    headers: &HeaderMap,
    table: &str,
    key: &[u8],
) -> Response {
    let principal = match state.principal(headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if let Err(e) = admit_read(state, &principal.tenant) {
        return error_response(e);
    }
    let gateway = match branch_gateway(state, headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    if branch_snapshot(state, headers, &principal.tenant).ok().flatten().is_none() {
        if let Some(node) = state.raft_node() {
            let routing = range_routing_key(table, key);
            if let Some((owner, _epoch)) = node.range_owner(&routing) {
                if owner != node.node_id() {
                    let record = ryme_storage::RecordKey::new(
                        &principal.tenant,
                        &state.database,
                        table,
                        key,
                    );
                    let (value, _expires_at) = match node.fetch_range_value(owner, record, 0).await {
                        Ok(result) => result,
                        Err(error) => return error_response(error),
                    };
                    return match value {
                        Some(value) => match gateway.visible_value(&principal, table, &value) {
                            Ok(true) => {
                                if let Err(e) = admit_egress(state, &principal.tenant, value.len() as u64) {
                                    error_response(e)
                                } else {
                                    let masked = gateway.masked(table, value);
                                    (StatusCode::OK, masked).into_response()
                                }
                            }
                            Ok(false) => {
                                error_response(ryme_error::RymeError::NotFound(String::from("row")))
                            }
                            Err(error) => error_response(error),
                        },
                        None => error_response(ryme_error::RymeError::NotFound(String::from("row"))),
                    };
                }
            }
        }
    }
    match gateway.get(&principal, table, key) {
        Ok(Some(value)) => {
            if let Err(e) = admit_egress(state, &principal.tenant, value.len() as u64) {
                return error_response(e);
            }
            let masked = gateway.masked(table, value);
            (StatusCode::OK, masked).into_response()
        }
        Ok(None) => error_response(ryme_error::RymeError::NotFound(String::from("row"))),
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PutQuery {
    pub ttl: Option<u64>,
}

async fn kv_put(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path((table, key)): Path<(String, String)>,
    Query(query): Query<PutQuery>,
    body: axum::body::Bytes,
) -> Response {
    let start = SystemTime::now();
    let key_bytes = key.into_bytes();
    let result =
        inner_kv_put(&state, &headers, &table, key_bytes.clone(), body.to_vec(), query.ttl).await;
    if result.status() == StatusCode::OK {
        note_range_write(&state, &range_routing_key(&table, &key_bytes), 1);
    }
    let micros = elapsed_micros(start);
    state.latency.observe_micros(micros);
    state.histogram.record(micros);
    if micros > ryme_observe::SLOW_THRESHOLD_MICROS {
        tracing::warn!(table = %table, micros = micros, "slow kv_put");
        state.slow_log.record(SlowEntry {
            kind: String::from("kv_put"),
            fingerprint: format!("PUT {table}"),
            table: table.clone(),
            micros,
            at_unix: now_secs(),
        });
    }
    record_meter(&state, Metric::WriteUnit, 1);
    record_span(&state, "kv_put", &[("table", table.as_str())], micros);
    result
}

async fn inner_kv_put(
    state: &SharedState,
    headers: &HeaderMap,
    table: &str,
    key: Vec<u8>,
    value: Vec<u8>,
    ttl: Option<u64>,
) -> Response {
    if key.is_empty() || key.len() > ryme_gateway::MAX_KEY_BYTES {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("key")));
    }
    if value.len() > ryme_gateway::MAX_VALUE_BYTES {
        return error_response(ryme_error::RymeError::Overload(String::from("value")));
    }
    let expires_at = match ttl {
        Some(secs) if secs > 315_360_000 => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from("ttl")))
        }
        Some(secs) => Some(ryme_txn::now_unix().saturating_add(secs)),
        None => None,
    };
    let principal = match state.principal(headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if let Err(e) = validate_branch_selection(state, headers, &principal.tenant) {
        return error_response(e);
    }
    if let Err(e) = admit_write(state, &principal.tenant, value.len() as u64) {
        return error_response(e);
    }
    let gateway = match branch_gateway(state, headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    match gateway.put_with_ttl(&principal, table, key, value, expires_at).await {
        Ok(commit) => {
            (StatusCode::OK, Json(serde_json::json!({ "commit": commit }))).into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn kv_ttl(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path((table, key)): Path<(String, String)>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if let Err(e) = admit_read(&state, &principal.tenant) {
        return error_response(e);
    }
    let gateway = match branch_gateway(&state, &headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    match gateway.ttl_of(&principal, &table, key.as_bytes()) {
        Ok(ryme_gateway::Ttl::Missing) => {
            error_response(ryme_error::RymeError::NotFound(String::from("row")))
        }
        Ok(ryme_gateway::Ttl::Persistent) => {
            (StatusCode::OK, Json(serde_json::json!({ "ttl": -1 }))).into_response()
        }
        Ok(ryme_gateway::Ttl::Seconds(secs)) => {
            (StatusCode::OK, Json(serde_json::json!({ "ttl": secs }))).into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn kv_delete(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path((table, key)): Path<(String, String)>,
) -> Response {
    let start = SystemTime::now();
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if let Err(e) = validate_branch_selection(&state, &headers, &principal.tenant) {
        return error_response(e);
    }
    let key_bytes = key.into_bytes();
    if let Err(e) = admit_write(&state, &principal.tenant, key_bytes.len() as u64) {
        return error_response(e);
    }
    let gateway = match branch_gateway(&state, &headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    let result = match gateway.delete(&principal, &table, key_bytes.clone()).await {
        Ok(commit) => {
            (StatusCode::OK, Json(serde_json::json!({ "commit": commit }))).into_response()
        }
        Err(e) => error_response(e),
    };
    if result.status() == StatusCode::OK {
        note_range_write(&state, &range_routing_key(&table, &key_bytes), 1);
    }
    let micros = elapsed_micros(start);
    state.latency.observe_micros(micros);
    state.histogram.record(micros);
    record_meter(&state, Metric::WriteUnit, 1);
    result
}

async fn sql_exec(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: SqlRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let start = SystemTime::now();
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let sql = match request.params {
        Some(params) => bind(&request.sql, &params),
        None => request.sql,
    };
    let statement = match parse(&sql) {
        Ok(statement) => statement,
        Err(e) => return error_response(e),
    };
    if statement.is_write() && !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if statement.is_write() {
        if let Err(e) = admit_write(&state, &principal.tenant, sql.len() as u64) {
            return error_response(e);
        }
    } else if let Err(e) = admit_read(&state, &principal.tenant) {
        return error_response(e);
    }
    let table_name = statement.table().to_string();
    let write_statement = statement.is_write();
    let range_keys = if write_statement { sql_range_keys(&statement) } else { None };
    let executor = match branch_executor(&state, &headers, &principal.tenant) {
        Ok(executor) => executor,
        Err(e) => return error_response(e),
    };
    let result = match executor.execute(statement).await {
        Ok(QueryResult::Ok) => {
            (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
        }
        Ok(QueryResult::Row { pk, value }) => {
            if let Err(e) = admit_egress(&state, &principal.tenant, (pk.len() + value.len()) as u64)
            {
                return error_response(e);
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "pk": String::from_utf8_lossy(&pk),
                    "value": String::from_utf8_lossy(&state.gateway.masked(&table_name, value)),
                })),
            )
                .into_response()
        }
        Ok(QueryResult::Scalar { label, value }) => {
            if let Err(e) =
                admit_egress(&state, &principal.tenant, (label.len() + value.len()) as u64)
            {
                return error_response(e);
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "scalar": label,
                    "value": String::from_utf8_lossy(&value),
                })),
            )
                .into_response()
        }
        Ok(QueryResult::Rows { rows }) => {
            let egress: u64 = rows.iter().map(|(pk, value)| (pk.len() + value.len()) as u64).sum();
            if let Err(e) = admit_egress(&state, &principal.tenant, egress) {
                return error_response(e);
            }
            let items: Vec<serde_json::Value> = rows
                .into_iter()
                .map(|(pk, value)| {
                    serde_json::json!({
                        "pk": String::from_utf8_lossy(&pk),
                        "value": String::from_utf8_lossy(&state.gateway.masked(&table_name, value)),
                    })
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({ "rows": items }))).into_response()
        }
        Ok(QueryResult::Table { columns, rows }) => {
            let egress: u64 = columns.iter().map(String::len).sum::<usize>() as u64
                + rows.iter().flatten().map(Vec::len).sum::<usize>() as u64;
            if let Err(e) = admit_egress(&state, &principal.tenant, egress) {
                return error_response(e);
            }
            let items: Vec<serde_json::Value> = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!(row
                        .into_iter()
                        .map(|value| {
                            String::from_utf8_lossy(&state.gateway.masked(&table_name, value))
                                .to_string()
                        })
                        .collect::<Vec<_>>())
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({ "columns": columns, "rows": items })))
                .into_response()
        }
        Ok(QueryResult::Returning { columns, rows }) => {
            let egress: u64 = columns.iter().map(String::len).sum::<usize>() as u64
                + rows.iter().flatten().map(Vec::len).sum::<usize>() as u64;
            if let Err(e) = admit_egress(&state, &principal.tenant, egress) {
                return error_response(e);
            }
            let items: Vec<serde_json::Value> = rows
                .into_iter()
                .map(|row| {
                    let values: Vec<String> = row
                        .into_iter()
                        .map(|value| {
                            String::from_utf8_lossy(&state.gateway.masked(&table_name, value))
                                .to_string()
                        })
                        .collect();
                    serde_json::json!(values)
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({ "columns": columns, "rows": items })))
                .into_response()
        }
        Err(e) => error_response(e),
    };
    if write_statement && result.status() == StatusCode::OK {
        if let Some(keys) = range_keys.as_deref() {
            for key in keys {
                note_range_write(&state, &range_routing_key(&table_name, key), 1);
            }
        } else {
            note_range_write(&state, table_name.as_bytes(), 1);
        }
    }
    let micros = elapsed_micros(start);
    state.latency.observe_micros(micros);
    state.histogram.record(micros);
    if micros > ryme_observe::SLOW_THRESHOLD_MICROS {
        tracing::warn!(
            micros = micros,
            fingerprint = ryme_observe::query_fingerprint(&sql).as_str(),
            "slow sql"
        );
        state.slow_log.record(SlowEntry {
            kind: String::from("sql"),
            fingerprint: ryme_observe::query_fingerprint(&sql),
            table: table_name.clone(),
            micros,
            at_unix: now_secs(),
        });
    }
    record_meter(&state, Metric::ReadUnit, 1);
    record_span(&state, "sql_exec", &[("table", table_name.as_str())], micros);
    result
}

async fn scan(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(table): Path<String>,
    Query(query): Query<ScanQuery>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    if let Err(e) = admit_read(&state, &principal.tenant) {
        return error_response(e);
    }
    let gateway = match branch_gateway(&state, &headers, &principal.tenant) {
        Ok(gateway) => gateway,
        Err(e) => return error_response(e),
    };
    match gateway.scan(&principal, &table, limit) {
        Ok(rows) => {
            let egress: u64 = rows.iter().map(|(pk, value)| (pk.len() + value.len()) as u64).sum();
            if let Err(e) = admit_egress(&state, &principal.tenant, egress) {
                return error_response(e);
            }
            let items: Vec<serde_json::Value> = rows
                .into_iter()
                .map(|(pk, value)| {
                    let masked = gateway.masked(&table, value);
                    serde_json::json!({
                        "pk": String::from_utf8_lossy(&pk),
                        "value": String::from_utf8_lossy(&masked),
                    })
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({ "rows": items }))).into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn branch_create(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: BranchRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    let mut branches = control.branches.clone();
    if branches.get_for(&principal.tenant, &request.parent).is_err() && request.parent == "main" {
        if let Err(e) = branches.create_root_for(
            &principal.tenant,
            String::from("main"),
            ryme_branch::Manifest {
                id: format!("genesis-{}", principal.tenant),
                segments: Vec::new(),
                wal_start: 0,
            },
        ) {
            return error_response(e);
        }
    }
    let base_commit_ts = if request.base_commit_ts == 0 {
        state.backend.latest_commit()
    } else {
        request.base_commit_ts
    };
    let parent_schema = if request.parent == "main" {
        state.executor.schema_snapshot()
    } else {
        let parent_path = state.branch_schema_path(&principal.tenant, &request.parent);
        if parent_path.exists() {
            match load_schema_snapshot(&parent_path) {
                Ok(snapshot) => snapshot,
                Err(e) => return error_response(e),
            }
        } else {
            state.executor.schema_snapshot()
        }
    };
    let branch_id = request.id.clone();
    match branches.create_child_for(
        &principal.tenant,
        branch_id.clone(),
        &request.parent,
        base_commit_ts,
    ) {
        Ok(()) => {
            let schema_path = state.branch_schema_path(&principal.tenant, &branch_id);
            if let Err(e) = ryme_sql::persist_schema_snapshot(&schema_path, &parent_schema) {
                return error_response(e);
            }
            match branches.persist(&state.branch_path) {
                Ok(()) => {
                    control.branches = branches;
                    (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
                }
                Err(e) => error_response(e),
            }
        }
        Err(e) => error_response(e),
    }
}

async fn branch_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.branches.get_for(&principal.tenant, &id) {
        Ok(branch) => (StatusCode::OK, Json(branch)).into_response(),
        Err(e) => error_response(e),
    }
}

async fn branch_delete(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    let mut branches = control.branches.clone();
    match branches.delete_for(&principal.tenant, &id) {
        Ok(garbage) => match branches.persist(&state.branch_path) {
            Ok(()) => {
                control.branches = branches;
                let _ = std::fs::remove_file(state.branch_schema_path(&principal.tenant, &id));
                (StatusCode::OK, Json(serde_json::json!({ "garbage": garbage }))).into_response()
            }
            Err(e) => error_response(e),
        },
        Err(e) => error_response(e),
    }
}

async fn branch_list(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    (StatusCode::OK, Json(control.branches.list_for(&principal.tenant))).into_response()
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BranchResetRequest {
    pub base_commit_ts: u64,
}

async fn branch_reset(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let request: BranchResetRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    let mut branches = control.branches.clone();
    let base_commit_ts = if request.base_commit_ts == 0 {
        state.backend.latest_commit()
    } else {
        request.base_commit_ts
    };
    match branches.reset_for(&principal.tenant, &id, base_commit_ts) {
        Ok(branch) => match branches.persist(&state.branch_path) {
            Ok(()) => {
                control.branches = branches;
                if let Err(e) = ryme_sql::persist_schema_snapshot(
                    &state.branch_schema_path(&principal.tenant, &id),
                    &state.executor.schema_snapshot(),
                ) {
                    return error_response(e);
                }
                (StatusCode::OK, Json(branch)).into_response()
            }
            Err(e) => error_response(e),
        },
        Err(e) => error_response(e),
    }
}

async fn promote_branch_data(
    state: &SharedState,
    tenant: &str,
    id: &str,
) -> ryme_error::Result<u64> {
    let child = {
        let control = state
            .control
            .lock()
            .map_err(|_| ryme_error::RymeError::Internal(String::from("lock")))?;
        control.branches.get_for(tenant, id)?
    };
    if child.parent_id.as_deref() != Some("main") {
        return Err(ryme_error::RymeError::InvalidArgument(String::from(
            "only main branch promotion is supported",
        )));
    }
    let branch = ryme_branch::BranchBackend::new(
        state.backend.clone(),
        state.database.clone(),
        child.id.clone(),
        child.base_commit_ts,
        child.storage_epoch,
    );
    let local_database = branch
        .local_database()
        .ok_or_else(|| ryme_error::RymeError::Internal(String::from("branch storage")))?;
    let mut tables = HashSet::new();
    for (row_tenant, database, table) in state.backend.spaces()? {
        if row_tenant == tenant && database == local_database {
            tables.insert(table);
        }
    }
    let mut changes = Vec::new();
    for table in tables {
        for (pk, value) in branch.changes(tenant, &table)? {
            changes.push((table.clone(), pk, value));
        }
    }

    let mut base_txn = state.backend.begin();
    base_txn.restamp(child.base_commit_ts);
    let mut current_txn = state.backend.begin();
    for (table, pk, _value) in &changes {
        let key = ryme_storage::RecordKey::new(tenant, &state.database, table, pk);
        let base = state.backend.get(&mut base_txn, &key)?;
        let current = state.backend.get(&mut current_txn, &key)?;
        if base != current {
            return Err(ryme_error::RymeError::Conflict(format!(
                "branch conflict on {table}/{}",
                String::from_utf8_lossy(pk)
            )));
        }
    }

    let mut write_txn = state.backend.begin();
    for (table, pk, value) in changes {
        let key = ryme_storage::RecordKey::new(tenant, &state.database, &table, &pk);
        match value {
            Some(value) => state.backend.put(&mut write_txn, key, value),
            None => state.backend.delete(&mut write_txn, key),
        }
    }
    state.backend.commit(write_txn).await
}

async fn branch_promote(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = promote_branch_data(&state, &principal.tenant, &id).await {
        return error_response(e);
    }
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    let mut branches = control.branches.clone();
    match branches.promote_for(&principal.tenant, &id) {
        Ok(branch) => match branches.persist(&state.branch_path) {
            Ok(()) => {
                control.branches = branches;
                (StatusCode::OK, Json(branch)).into_response()
            }
            Err(e) => error_response(e),
        },
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BranchDiffQuery {
    pub against: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct BranchRowDiff {
    table: String,
    pk: String,
    left: Option<String>,
    right: Option<String>,
}

fn scan_branch_diff_rows<B: TxnBackend>(
    backend: &B,
    tenant: &str,
    database: &str,
    table: &str,
) -> ryme_error::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    const PAGE: usize = 1024;
    let mut rows = BTreeMap::new();
    let mut txn = backend.begin();
    let mut start_after = None;
    loop {
        let page = match start_after.as_deref() {
            Some(start_after) => {
                backend.scan_after(&mut txn, tenant, database, table, start_after, PAGE)?
            }
            None => backend.scan(&mut txn, tenant, database, table, PAGE)?,
        };
        let count = page.len();
        if let Some((last, _)) = page.last() {
            start_after = Some(last.clone());
        }
        rows.extend(page);
        if count < PAGE {
            break;
        }
    }
    Ok(rows)
}

fn branch_diff_data(
    state: &SharedState,
    tenant: &str,
    left: &ryme_branch::Branch,
    right: &ryme_branch::Branch,
) -> ryme_error::Result<Vec<BranchRowDiff>> {
    let left_backend = if left.id == "main" {
        BranchStorage::passthrough(state.backend.clone(), state.database.clone())
    } else {
        BranchStorage::new(
            state.backend.clone(),
            state.database.clone(),
            left.id.clone(),
            left.base_commit_ts,
            left.storage_epoch,
        )
    };
    let right_backend = if right.id == "main" {
        BranchStorage::passthrough(state.backend.clone(), state.database.clone())
    } else {
        BranchStorage::new(
            state.backend.clone(),
            state.database.clone(),
            right.id.clone(),
            right.base_commit_ts,
            right.storage_epoch,
        )
    };
    let local_left = left_backend.local_database();
    let local_right = right_backend.local_database();
    let mut tables = BTreeSet::new();
    for (row_tenant, database, table) in state.backend.spaces()? {
        if row_tenant != tenant {
            continue;
        }
        if database == state.database
            || local_left.is_some_and(|local| database == local)
            || local_right.is_some_and(|local| database == local)
        {
            tables.insert(table);
        }
    }

    let mut changes = Vec::new();
    for table in tables {
        let left_rows = scan_branch_diff_rows(&left_backend, tenant, &state.database, &table)?;
        let right_rows = scan_branch_diff_rows(&right_backend, tenant, &state.database, &table)?;
        let keys: BTreeSet<Vec<u8>> = left_rows.keys().chain(right_rows.keys()).cloned().collect();
        for pk in keys {
            let left_value = left_rows.get(&pk);
            let right_value = right_rows.get(&pk);
            if left_value == right_value {
                continue;
            }
            changes.push(BranchRowDiff {
                table: table.clone(),
                pk: ryme_auth::base64_url_encode(&pk),
                left: left_value.map(|value| ryme_auth::base64_url_encode(value)),
                right: right_value.map(|value| ryme_auth::base64_url_encode(value)),
            });
        }
    }
    Ok(changes)
}

async fn branch_diff(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<BranchDiffQuery>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    let branches = match state.control.lock() {
        Ok(guard) => guard.branches.clone(),
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    let diff = branches
        .get_for(&principal.tenant, &id)
        .and_then(|left| {
            branches.get_for(&principal.tenant, &query.against).map(|right| (left, right))
        })
        .and_then(|(left, right)| {
            branches
                .diff_for(&principal.tenant, &id, &query.against)
                .map(|manifest_diff| (left, right, manifest_diff))
        });
    match diff {
        Ok((left_branch, right_branch, (only_left, only_right))) => {
            let changes =
                match branch_diff_data(&state, &principal.tenant, &left_branch, &right_branch) {
                    Ok(changes) => changes,
                    Err(e) => return error_response(e),
                };
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "only_left": only_left,
                    "only_right": only_right,
                    "changes": changes,
                })),
            )
                .into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn checkpoint_create(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: CheckpointRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let commit = state.backend.latest_commit();
    let checkpoint = Checkpoint {
        id: format!("ckpt-{commit}"),
        commit_ts: commit,
        manifest_id: request.manifest_id.unwrap_or_else(|| String::from("genesis")),
        created_unix: now_secs(),
    };
    let recorded = match state.control.lock() {
        Ok(mut control) => {
            control.backups.record(checkpoint.clone());
            Ok(())
        }
        Err(_) => Err(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match recorded.and_then(|()| state.persist_control()) {
        Ok(()) => (StatusCode::OK, Json(checkpoint)).into_response(),
        Err(e) => error_response(e),
    }
}

async fn checkpoint_latest(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.backups.latest() {
        Some(checkpoint) => (StatusCode::OK, Json(checkpoint)).into_response(),
        None => error_response(ryme_error::RymeError::NotFound(String::from("checkpoint"))),
    }
}

async fn checkpoint_pitr(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<PitrQuery>,
) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.backups.select_pitr(query.target) {
        Ok(checkpoint) => (StatusCode::OK, Json(checkpoint)).into_response(),
        Err(e) => error_response(e),
    }
}

async fn snapshot_create(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    match snapshot_backend_files(&state).await {
        Ok(files) => {
            let commit = files.iter().map(|(commit, _)| *commit).max().unwrap_or(0);
            let names: Vec<&str> = files
                .iter()
                .map(|(_, path)| path.file_name().and_then(|n| n.to_str()).unwrap_or(""))
                .collect();
            (StatusCode::OK, Json(serde_json::json!({ "commit": commit, "files": names })))
                .into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn backup_restore(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<PitrQuery>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if state.backend.is_cluster() {
        return error_response(ryme_error::RymeError::Unavailable(String::from(
            "restore unavailable in cluster mode",
        )));
    }
    let restored = match &state.backend {
        Backend::Single(manager) => manager.restore_to(query.target).map(|replayed| vec![replayed]),
        Backend::Cluster(_) | Backend::Hybrid(_) => Err(ryme_error::RymeError::Unavailable(
            String::from("restore unavailable in cluster mode"),
        )),
        Backend::Sharded(shards) => {
            let mut replayed = Vec::new();
            for index in 0..shards.shard_count() {
                let Some(manager) = shards.shard_manager(index) else {
                    return error_response(ryme_error::RymeError::Internal(String::from("shard")));
                };
                match manager.restore_to(query.target) {
                    Ok(count) => replayed.push(count),
                    Err(e) => return error_response(e),
                }
            }
            Ok(replayed)
        }
    };
    match restored {
        Ok(replayed) => (
            StatusCode::OK,
            Json(serde_json::json!({ "target": query.target, "replayed": replayed })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

async fn backup_archive(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: ArchiveRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    match state.archive_now(request.backup_id).await {
        Ok(manifest) => {
            let checkpoint = Checkpoint {
                id: manifest.backup_id.clone(),
                commit_ts: manifest.commit_ts,
                manifest_id: manifest.manifest_key(),
                created_unix: now_secs(),
            };
            let recorded = match state.control.lock() {
                Ok(mut guard) => {
                    guard.backups.record(checkpoint);
                    Ok(())
                }
                Err(_) => Err(ryme_error::RymeError::Internal(String::from("lock"))),
            };
            match recorded.and_then(|()| state.persist_control()) {
                Ok(()) => (StatusCode::OK, Json(manifest)).into_response(),
                Err(e) => error_response(e),
            }
        }
        Err(e) => error_response(e),
    }
}

async fn backup_archives(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let Some(target) = state.archive.clone() else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("archive")));
    };
    match target.list_backups().await {
        Ok(manifests) => (StatusCode::OK, Json(manifests)).into_response(),
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct VerifyQuery {
    pub backup_id: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DrillReport {
    pub backup_id: String,
    pub verified_files: u64,
    pub at_unix: u64,
    pub error: Option<String>,
}

fn open_archive_bytes(
    state: &SharedState,
    encryption: &Option<ryme_archive::FileEncryption>,
    bytes: &[u8],
) -> ryme_error::Result<Vec<u8>> {
    let Some(envelope) = encryption else {
        return Ok(bytes.to_vec());
    };
    let guard =
        state.dek_ring.lock().map_err(|_| ryme_error::RymeError::Internal(String::from("lock")))?;
    let dek =
        guard.get(&state.tenant, &envelope.dek_id).ok_or(ryme_error::RymeError::Unauthorized)?;
    ryme_crypto::open_with(
        &dek,
        &ryme_crypto::Envelope {
            dek_id: envelope.dek_id.clone(),
            nonce_b64: envelope.nonce_b64.clone(),
            blob_b64: ryme_auth::base64_url_encode(bytes),
            tag_b64: envelope.tag_b64.clone(),
        },
    )
}

async fn drill_once(state: &SharedState) -> DrillReport {
    let target = match state.archive.clone() {
        Some(target) => target,
        None => {
            return DrillReport {
                backup_id: String::new(),
                verified_files: 0,
                at_unix: now_secs(),
                error: Some(String::from("archive")),
            }
        }
    };
    let manifest = match target.list_backups().await {
        Ok(mut manifests) => {
            manifests.sort_by_key(|m| m.commit_ts);
            match manifests.pop() {
                Some(manifest) => manifest,
                None => {
                    return DrillReport {
                        backup_id: String::new(),
                        verified_files: 0,
                        at_unix: now_secs(),
                        error: Some(String::from("backup")),
                    }
                }
            }
        }
        Err(e) => {
            return DrillReport {
                backup_id: String::new(),
                verified_files: 0,
                at_unix: now_secs(),
                error: Some(e.to_string()),
            }
        }
    };
    let backup_id = manifest.backup_id.clone();
    let parent = state.data_dir.parent().unwrap_or_else(|| std::path::Path::new("."));
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let restore_dir = parent.join(format!(".rymedb-restore-drill-{}-{nanos}", std::process::id()));
    let result = target
        .restore_backup(&manifest.manifest_key(), &restore_dir, &|encryption, bytes| {
            open_archive_bytes(state, encryption, bytes)
        })
        .await;
    let cleanup = std::fs::remove_dir_all(&restore_dir);
    match (result, cleanup) {
        (Ok(commit), Ok(())) => {
            let verified = manifest.files.len() as u64;
            tracing::info!(backup = %backup_id, commit, files = verified, "restore drill ok");
            DrillReport { backup_id, verified_files: verified, at_unix: now_secs(), error: None }
        }
        (Ok(_), Err(e)) => {
            tracing::warn!(backup = %backup_id, error = %e, "restore drill cleanup failed");
            DrillReport {
                backup_id,
                verified_files: 0,
                at_unix: now_secs(),
                error: Some(format!("cleanup: {e}")),
            }
        }
        (Err(e), _) => {
            tracing::warn!(backup = %backup_id, error = %e, "restore drill failed");
            DrillReport {
                backup_id,
                verified_files: 0,
                at_unix: now_secs(),
                error: Some(e.to_string()),
            }
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BackupCopyRequest {
    pub backup_id: String,
}

async fn backup_copy(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: BackupCopyRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let Some(primary) = state.archive.clone() else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("archive")));
    };
    let Some(replica) = state.archive_replica.clone() else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from(
            "archive_replica",
        )));
    };
    let manifests = match primary.list_backups().await {
        Ok(manifests) => manifests,
        Err(e) => return error_response(e),
    };
    let Some(manifest) = manifests.into_iter().find(|m| m.backup_id == request.backup_id) else {
        return error_response(ryme_error::RymeError::NotFound(String::from("backup")));
    };
    match replica.copy_from(&primary, &manifest).await {
        Ok(_) => (StatusCode::OK, Json(manifest)).into_response(),
        Err(e) => error_response(e),
    }
}

async fn backup_verify(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<VerifyQuery>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let Some(target) = state.archive.clone() else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("archive")));
    };
    let manifests = match target.list_backups().await {
        Ok(manifests) => manifests,
        Err(e) => return error_response(e),
    };
    let Some(manifest) = manifests.into_iter().find(|m| m.backup_id == query.backup_id) else {
        return error_response(ryme_error::RymeError::NotFound(String::from("backup")));
    };
    match target
        .verify_backup(&manifest.manifest_key(), &|encryption, bytes| {
            open_archive_bytes(&state.clone(), encryption, bytes)
        })
        .await
    {
        Ok(verified) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "backup_id": manifest.backup_id,
                "commit": manifest.commit_ts,
                "verified": verified,
                "encrypted": manifest.files.iter().filter(|f| f.encryption.is_some()).count(),
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

async fn drill_status(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let report = state.last_drill.lock().ok().and_then(|guard| guard.clone());
    match report {
        Some(report) => (StatusCode::OK, Json(report)).into_response(),
        None => error_response(ryme_error::RymeError::NotFound(String::from("drill"))),
    }
}

async fn shard_layout(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    match &state.backend {
        Backend::Sharded(shards) => {
            let layout: Vec<serde_json::Value> = shards
                .tables()
                .into_iter()
                .map(|(table, shard, bytes)| {
                    serde_json::json!({
                        "tenant": table.tenant,
                        "database": table.database,
                        "table": table.table,
                        "shard": shard,
                        "bytes": bytes,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "mode": "sharded",
                    "shards": shards.shard_count(),
                    "placement": layout,
                })),
            )
                .into_response()
        }
        Backend::Hybrid(hybrid) => {
            let layout: Vec<serde_json::Value> = hybrid
                .tables()
                .into_iter()
                .map(|(table, tier, _)| {
                    serde_json::json!({
                        "tenant": table.tenant,
                        "database": table.database,
                        "table": table.table,
                        "tier": format!("{tier:?}").to_lowercase(),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "mode": "hybrid",
                    "placement": layout,
                })),
            )
                .into_response()
        }
        _ => (StatusCode::OK, Json(serde_json::json!({ "mode": "single", "shards": 1 })))
            .into_response(),
    }
}

async fn shard_move(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: MoveShardRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let tenant = request.tenant.unwrap_or_else(|| state.tenant.clone());
    if !principal.can_admin() || tenant != principal.tenant {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let database = request.database.unwrap_or_else(|| state.database.clone());
    let table = TableRef::new(&tenant, &database, &request.table);
    let moved = match &state.backend {
        Backend::Sharded(shards) => {
            shards.move_table(&tenant, &database, &request.table, request.target)
        }
        Backend::Hybrid(hybrid) => match hybrid.tier(&table) {
            ryme_shard::Tier::Local => {
                hybrid.local().move_table(&tenant, &database, &request.table, request.target)
            }
            ryme_shard::Tier::Replicated => {
                return error_response(ryme_error::RymeError::InvalidArgument(String::from("tier")))
            }
        },
        _ => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from("sharding")))
        }
    };
    match moved {
        Ok(report) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "table": report.table.table,
                "from": report.from,
                "to": report.to,
                "bytes": report.bytes,
                "commit": report.max_commit_ts,
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

async fn range_list(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(lookup): Query<RangeLookup>,
) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match lookup.key {
        Some(key) => match control.route(key.as_bytes()) {
            Ok(range) => (StatusCode::OK, Json(range)).into_response(),
            Err(e) => error_response(e),
        },
        None => (StatusCode::OK, Json(control.ranges())).into_response(),
    }
}

fn install_cluster_range_replication(
    state: &SharedState,
    node: &std::sync::Arc<Node>,
) -> ryme_error::Result<()> {
    let ranges = state
        .control
        .lock()
        .map_err(|_| ryme_error::RymeError::Internal(String::from("lock")))?
        .ranges();
    node.set_range_owners(range_owners(&ranges));
    let state = state.clone();
    let hook: ryme_raft::net::MetadataHook =
        Arc::new(move |payload| apply_replicated_ranges(&state, payload));
    node.set_metadata_hook(hook)
}

fn range_owners(ranges: &[Range]) -> Vec<RangeOwner> {
    ranges
        .iter()
        .filter_map(|range| {
            let owner = range.leader.strip_prefix("raft-")?.parse().ok()?;
            Some(RangeOwner {
                start: range.start.clone(),
                end: range.end.clone(),
                owner,
                epoch: range.epoch,
            })
        })
        .collect()
}

fn install_cluster_data_replication(
    state: &SharedState,
    node: &std::sync::Arc<Node>,
) -> ryme_error::Result<()> {
    let state = state.clone();
    let hook: ryme_raft::net::MetadataHook =
        Arc::new(move |payload| apply_replicated_data(&state, payload));
    node.set_data_hook(hook)
}

fn install_cluster_realtime_replication(
    state: &SharedState,
    node: &std::sync::Arc<Node>,
) -> ryme_error::Result<()> {
    let state = state.clone();
    let hook: ryme_raft::net::MetadataHook =
        Arc::new(move |payload| apply_replicated_broadcast(&state, payload));
    node.set_realtime_hook(hook)
}

fn install_cluster_presence_replication(
    state: &SharedState,
    node: &std::sync::Arc<Node>,
) -> ryme_error::Result<()> {
    let state = state.clone();
    let hook: ryme_raft::net::MetadataHook =
        Arc::new(move |payload| apply_replicated_presence(&state, payload));
    node.set_presence_hook(hook)
}

fn apply_replicated_data(state: &SharedState, payload: &[u8]) -> ryme_error::Result<()> {
    let (commit_ts, encoded) = ryme_raft::decode_applied(payload)?;
    let writes = ryme_txn::decode_writes(&encoded)?;
    let mut query_tables = BTreeSet::new();
    for (key, write) in writes {
        let mut before_txn = state.backend.begin();
        before_txn.restamp(commit_ts.saturating_sub(1));
        let before = state.backend.get(&mut before_txn, &key)?;
        let op = match write.value {
            None => ryme_realtime::Operation::Delete,
            Some(_) if before.is_some() => ryme_realtime::Operation::Update,
            Some(_) => ryme_realtime::Operation::Insert,
        };
        let after = write.value;
        state.realtime.publish(ryme_realtime::NewChange {
            tenant: key.tenant.clone(),
            database: key.database.clone(),
            branch: String::from("main"),
            table: key.table.clone(),
            op,
            pk: key.pk.clone(),
            before,
            after,
            commit_ts,
            tx_id: commit_ts,
        })?;
        query_tables.insert((key.tenant, key.database, key.table));
    }
    for (tenant, database, table) in query_tables {
        let Some(limit) = state.realtime.query_limit_branch(&tenant, &database, "main", &table)
        else {
            continue;
        };
        let mut txn = state.backend.begin();
        txn.restamp(commit_ts);
        let rows = state.backend.scan(&mut txn, &tenant, &database, &table, limit)?;
        let _ = state
            .realtime
            .publish_query_branch(&tenant, &database, "main", &table, commit_ts, rows, limit);
    }
    Ok(())
}

fn install_cluster_topic_replication(
    state: &SharedState,
    node: &std::sync::Arc<Node>,
) -> ryme_error::Result<()> {
    let state = state.clone();
    let hook: ryme_raft::net::MetadataHook =
        Arc::new(move |payload| apply_replicated_topic(&state, payload));
    node.set_topic_hook(hook)
}

fn apply_replicated_broadcast(state: &SharedState, payload: &[u8]) -> ryme_error::Result<()> {
    let event: ClusterBroadcast = serde_json::from_slice(payload)
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("realtime event: {error}")))?;
    state
        .realtime
        .broadcast_with_sequence(
            &event.tenant,
            &event.channel,
            event.from,
            event.payload,
            event.commit_ts,
            event.sequence,
        )
        .map(|_| ())
}

fn apply_replicated_presence(state: &SharedState, payload: &[u8]) -> ryme_error::Result<()> {
    let event: ClusterPresence = serde_json::from_slice(payload)
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("presence event: {error}")))?;
    match event {
        ClusterPresence::Join {
            tenant,
            channel,
            member,
            state: member_state,
            expires_unix,
            now_unix,
        } => state
            .realtime
            .presence_join_at(&tenant, &channel, member, member_state, expires_unix, now_unix)
            .map(|_| ()),
        ClusterPresence::Leave { tenant, channel, member } => {
            state.realtime.presence_leave(&tenant, &channel, &member).map(|_| ())
        }
    }
}

fn apply_replicated_topic(state: &SharedState, payload: &[u8]) -> ryme_error::Result<()> {
    let event: ClusterTopicAppend = serde_json::from_slice(payload)
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("topic event: {error}")))?;
    state
        .realtime
        .durable_append_at(
            &event.tenant,
            &event.partition,
            event.cursor,
            event.key,
            event.value,
            event.commit_ts,
            event.retention,
        )
        .and_then(|_| state.persist_durable_topics())
}

fn apply_replicated_ranges(state: &SharedState, payload: &[u8]) -> ryme_error::Result<()> {
    let ranges: Vec<Range> = serde_json::from_slice(payload)
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("range metadata: {error}")))?;
    let mut control =
        state.control.lock().map_err(|_| ryme_error::RymeError::Internal(String::from("lock")))?;
    let previous = control.ranges();
    control.restore_ranges(ranges.clone())?;
    if let Err(error) = sync_range_topology(state, &ranges) {
        let _ = control.restore_ranges(previous.clone());
        let _ = sync_range_topology(state, &previous);
        return Err(error);
    }
    if let Err(error) = state.persist_control_locked(&control) {
        let _ = control.restore_ranges(previous.clone());
        let _ = sync_range_topology(state, &previous);
        return Err(error);
    }
    if let Some(node) = state.raft_node() {
        node.set_range_owners(range_owners(&ranges));
    }
    Ok(())
}

async fn commit_cluster_ranges(
    state: &SharedState,
    node: &std::sync::Arc<Node>,
    previous: Vec<Range>,
    updated: Vec<Range>,
) -> ryme_error::Result<()> {
    let payload = serde_json::to_vec(&updated)
        .map_err(|error| ryme_error::RymeError::Internal(format!("range metadata: {error}")))?;
    if let Err(error) = node.propose_metadata(payload).await {
        let mut control = state
            .control
            .lock()
            .map_err(|_| ryme_error::RymeError::Internal(String::from("lock")))?;
        let _ = control.restore_ranges(previous.clone());
        let _ = sync_range_topology(state, &previous);
        return Err(error);
    }
    Ok(())
}

async fn range_loads(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    (StatusCode::OK, Json(control.range_loads())).into_response()
}

async fn range_split(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let request: SplitRangeRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from("body")))
        }
    };
    let raft = state.raft_node();
    if let Some(node) = raft.as_ref() {
        if !node.is_leader().await {
            return error_response(ryme_error::RymeError::Unavailable(String::from("not leader")));
        }
    }
    let prepared = {
        let mut control = match state.control.lock() {
            Ok(guard) => guard,
            Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
        };
        let previous = control.ranges();
        match control.split_range(
            &request.id,
            request.mid.into_bytes(),
            request.left_id.clone(),
            request.right_id.clone(),
            request.expected_epoch,
        ) {
            Ok(()) => {
                let updated = control.ranges();
                if let Err(error) = sync_range_topology(&state, &updated) {
                    let _ = control.restore_ranges(previous);
                    return error_response(error);
                }
                Ok((previous, updated))
            }
            Err(error) => Err(error),
        }
    };
    let (previous, updated) = match prepared {
        Ok(value) => value,
        Err(error) => return error_response(error),
    };
    let commit_result = if let Some(node) = raft {
        commit_cluster_ranges(&state, &node, previous.clone(), updated).await
    } else {
        state.persist_control()
    };
    if let Err(error) = commit_result {
        if let Ok(mut control) = state.control.lock() {
            let _ = control.restore_ranges(previous.clone());
        }
        let _ = sync_range_topology(&state, &previous);
        return error_response(error);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    let left = control.router_get(&request.left_id);
    let right = control.router_get(&request.right_id);
    match (left, right) {
        (Ok(left), Ok(right)) => (StatusCode::OK, Json(vec![left, right])).into_response(),
        (Err(e), _) | (_, Err(e)) => error_response(e),
    }
}

async fn range_merge(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let request: MergeRangesRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from("body")))
        }
    };
    let raft = state.raft_node();
    if let Some(node) = raft.as_ref() {
        if !node.is_leader().await {
            return error_response(ryme_error::RymeError::Unavailable(String::from("not leader")));
        }
    }
    let prepared = {
        let mut control = match state.control.lock() {
            Ok(guard) => guard,
            Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
        };
        let previous = control.ranges();
        match control.merge_ranges(
            &request.left_id,
            &request.right_id,
            request.merged_id.clone(),
            request.expected_left_epoch,
            request.expected_right_epoch,
        ) {
            Ok(()) => {
                let updated = control.ranges();
                if let Err(error) = sync_range_topology(&state, &updated) {
                    let _ = control.restore_ranges(previous);
                    return error_response(error);
                }
                Ok((previous, updated))
            }
            Err(error) => Err(error),
        }
    };
    let (previous, updated) = match prepared {
        Ok(value) => value,
        Err(error) => return error_response(error),
    };
    let commit_result = if let Some(node) = raft {
        commit_cluster_ranges(&state, &node, previous.clone(), updated).await
    } else {
        state.persist_control()
    };
    if let Err(error) = commit_result {
        if let Ok(mut control) = state.control.lock() {
            let _ = control.restore_ranges(previous.clone());
        }
        let _ = sync_range_topology(&state, &previous);
        return error_response(error);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.router_get(&request.merged_id) {
        Ok(merged) => (StatusCode::OK, Json(merged)).into_response(),
        Err(e) => error_response(e),
    }
}

fn sync_range_topology(state: &SharedState, ranges: &[Range]) -> ryme_error::Result<()> {
    let placements = ranges
        .iter()
        .cloned()
        .map(|range| ryme_shard::RangePlacement {
            id: range.id,
            start: range.start,
            end: range.end,
            shard: 0,
        })
        .collect();
    match &state.backend {
        Backend::Sharded(shards) => shards.set_range_topology(placements),
        Backend::Hybrid(hybrid) => hybrid.local().set_range_topology(placements),
        _ => Ok(()),
    }
}

fn require_raft(state: &SharedState) -> Option<std::sync::Arc<Node>> {
    state.raft_node()
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AutosplitQuery {
    pub min_writes: Option<u64>,
}

async fn range_autosplit(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<AutosplitQuery>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let raft = state.raft_node();
    if let Some(node) = raft.as_ref() {
        if !node.is_leader().await {
            return error_response(ryme_error::RymeError::Unavailable(String::from("not leader")));
        }
    }
    let threshold = match query.min_writes {
        Some(min) if min > 0 => min,
        None if state.autosplit_writes > 0 => state.autosplit_writes,
        _ => {
            return (StatusCode::OK, Json(serde_json::json!({ "split": [] }))).into_response();
        }
    };
    let prepared = {
        let mut control = match state.control.lock() {
            Ok(guard) => guard,
            Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
        };
        let previous = control.ranges();
        match control.auto_split_once(threshold) {
            Ok(created) if created.is_empty() => Ok((previous, created, None)),
            Ok(created) => {
                let updated = control.ranges();
                if let Err(error) = sync_range_topology(&state, &updated) {
                    let _ = control.restore_ranges(previous);
                    return error_response(error);
                }
                Ok((previous, created, Some(updated)))
            }
            Err(error) => Err(error),
        }
    };
    let (previous, created, updated) = match prepared {
        Ok(value) => value,
        Err(error) => return error_response(error),
    };
    if let Some(updated) = updated {
        let commit_result = if let Some(node) = raft {
            commit_cluster_ranges(&state, &node, previous.clone(), updated).await
        } else {
            state.persist_control()
        };
        if let Err(error) = commit_result {
            if let Ok(mut control) = state.control.lock() {
                let _ = control.restore_ranges(previous.clone());
            }
            let _ = sync_range_topology(&state, &previous);
            return error_response(error);
        }
    }
    (StatusCode::OK, Json(serde_json::json!({ "split": created }))).into_response()
}

async fn range_verify(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let request: VerifyRangeRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from("body")))
        }
    };
    if !state.backend.is_cluster() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
    }
    let Some(node) = state.raft_node() else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
    };
    if !node.is_leader().await {
        return error_response(ryme_error::RymeError::Unavailable(String::from("not leader")));
    }
    let range = {
        let control = match state.control.lock() {
            Ok(guard) => guard,
            Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
        };
        let range = match control.router_get(&request.id) {
            Ok(range) => range,
            Err(error) => return error_response(error),
        };
        if range.epoch != request.expected_epoch {
            return error_response(ryme_error::RymeError::Conflict(format!(
                "range epoch {}",
                range.epoch
            )));
        }
        range
    };
    let members = node.current_config().await;
    if !members.iter().any(|member| member.id == request.target) {
        return error_response(ryme_error::RymeError::NotFound(String::from("peer")));
    }
    let max_rows = request.max_rows.unwrap_or(100_000).clamp(1, 100_000);
    let read_ts = node.manager().latest_commit();
    let source = match node.snapshot_range(&range.start, &range.end, read_ts, max_rows) {
        Ok(snapshot) => snapshot,
        Err(error) => return error_response(error),
    };
    let target = if request.target == node.node_id() {
        source.clone()
    } else {
        match node
            .fetch_range_snapshot(
                request.target,
                range.start.clone(),
                range.end.clone(),
                read_ts,
                max_rows,
            )
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => return error_response(error),
        }
    };
    {
        let control = match state.control.lock() {
            Ok(guard) => guard,
            Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
        };
        match control.router_get(&request.id) {
            Ok(current) if current.epoch == request.expected_epoch => {}
            Ok(current) => {
                return error_response(ryme_error::RymeError::Conflict(format!(
                    "range epoch {}",
                    current.epoch
                )))
            }
            Err(error) => return error_response(error),
        }
    }
    let source_bytes: u64 =
        source.rows.iter().map(|row| row.value.len() as u64 + row.pk.len() as u64).sum();
    let target_bytes: u64 =
        target.rows.iter().map(|row| row.value.len() as u64 + row.pk.len() as u64).sum();
    let matching = !source.truncated
        && !target.truncated
        && target.applied_commit >= source.snapshot_ts
        && source.rows == target.rows;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "range": range.id,
            "epoch": range.epoch,
            "source": node.node_id(),
            "target": request.target,
            "snapshot_ts": source.snapshot_ts,
            "source_commit": source.applied_commit,
            "target_commit": target.applied_commit,
            "source_rows": source.rows.len(),
            "target_rows": target.rows.len(),
            "source_bytes": source_bytes,
            "target_bytes": target_bytes,
            "source_truncated": source.truncated,
            "target_truncated": target.truncated,
            "matching": matching,
            "ready_for_transfer": matching,
        })),
    )
        .into_response()
}

async fn range_transfer(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let request: TransferRangeRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from("body")))
        }
    };
    if !state.backend.is_cluster() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
    }
    let Some(node) = state.raft_node() else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
    };
    if !node.is_leader().await {
        return error_response(ryme_error::RymeError::Unavailable(String::from("not leader")));
    }
    if request.target == node.node_id() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("target")));
    }
    let range = {
        let control = match state.control.lock() {
            Ok(guard) => guard,
            Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
        };
        let range = match control.router_get(&request.id) {
            Ok(range) => range,
            Err(error) => return error_response(error),
        };
        if range.epoch != request.expected_epoch {
            return error_response(ryme_error::RymeError::Conflict(format!(
                "range epoch {}",
                range.epoch
            )));
        }
        range
    };
    let members = node.current_config().await;
    if !members.iter().any(|member| member.id == request.target) {
        return error_response(ryme_error::RymeError::NotFound(String::from("peer")));
    }
    let max_rows = request.max_rows.unwrap_or(100_000).clamp(1, 100_000);
    let read_ts = node.manager().latest_commit();
    let snapshot = match node.snapshot_range(&range.start, &range.end, read_ts, max_rows) {
        Ok(snapshot) if !snapshot.truncated => snapshot,
        Ok(_) => {
            return error_response(ryme_error::RymeError::Overload(String::from("range snapshot")))
        }
        Err(error) => return error_response(error),
    };
    let rows = if let Err(error) =
        node.install_range_snapshot_on(request.target, snapshot.clone()).await
    {
        return error_response(error);
    } else {
        snapshot.rows.len()
    };
    let verified = match node
        .fetch_range_snapshot(
            request.target,
            range.start.clone(),
            range.end.clone(),
            snapshot.snapshot_ts,
            max_rows,
        )
        .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => return error_response(error),
    };
    if verified.truncated
        || verified.applied_commit < snapshot.snapshot_ts
        || verified.rows != snapshot.rows
    {
        return error_response(ryme_error::RymeError::Conflict(String::from(
            "range transfer verification",
        )));
    }
    let previous = {
        let mut control = match state.control.lock() {
            Ok(guard) => guard,
            Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
        };
        let previous = control.ranges();
        let leader = format!("raft-{}", request.target);
        let updated_range = match control.move_range(&request.id, leader, request.expected_epoch) {
            Ok(range) => range,
            Err(error) => return error_response(error),
        };
        let updated = control.ranges();
        if let Err(error) = sync_range_topology(&state, &updated) {
            let _ = control.restore_ranges(previous.clone());
            return error_response(error);
        }
        (previous, updated, updated_range)
    };
    if let Err(error) =
        commit_cluster_ranges(&state, &node, previous.0.clone(), previous.1.clone()).await
    {
        if let Ok(mut control) = state.control.lock() {
            let _ = control.restore_ranges(previous.0.clone());
        }
        let _ = sync_range_topology(&state, &previous.0);
        return error_response(error);
    }
    let moved = previous.2;
    let bytes: u64 =
        snapshot.rows.iter().map(|row| row.pk.len() as u64 + row.value.len() as u64).sum();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "range": moved.id,
            "epoch": moved.epoch,
            "leader": moved.leader,
            "target": request.target,
            "rows": rows,
            "bytes": bytes,
            "snapshot_ts": snapshot.snapshot_ts,
            "verified": true,
        })),
    )
        .into_response()
}

async fn cluster_members(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let Some(node) = require_raft(&state) else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
    };
    let current = node.current_config().await;
    let joint = node.joint_config().await.unwrap_or_default();
    let members: Vec<serde_json::Value> = current
        .iter()
        .map(|member| {
            serde_json::json!({
                "id": member.id,
                "addr": member.addr,
                "self": member.id == node.node_id(),
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "term": node.term().await,
            "leader": node.is_leader().await,
            "commit": node.commit_index().await,
            "members": members,
            "joint": joint.iter().map(|member| member.id).collect::<Vec<_>>(),
        })),
    )
        .into_response()
}

async fn cluster_add_member(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: ClusterMemberRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let Some(node) = require_raft(&state) else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
    };
    if request.addr.trim().is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("addr")));
    }
    match node.add_member(request.id, request.addr).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(e) => error_response(e),
    }
}

async fn cluster_remove_member(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<usize>,
) -> Response {
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let Some(node) = require_raft(&state) else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
    };
    match node.remove_member(id).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(e) => error_response(e),
    }
}

async fn cluster_transfer(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: ClusterTransferRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let Some(node) = require_raft(&state) else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
    };
    match node.transfer(request.target).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(e) => error_response(e),
    }
}

async fn cluster_replace(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: ClusterReplaceRequest = match json_body(&body) {
        Ok(request) => request,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_admin() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let Some(node) = require_raft(&state) else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("cluster")));
    };
    let mut members = Vec::new();
    for member in request.members {
        if member.addr.trim().is_empty() {
            return error_response(ryme_error::RymeError::InvalidArgument(String::from("addr")));
        }
        members.push(ryme_raft::net::Member { id: member.id, addr: member.addr });
    }
    match node.replace_members(members).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(e) => error_response(e),
    }
}

async fn stream(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let principal = match state.principal_with_query(&headers, query.api_key.as_deref()) {
        Ok(principal) => principal,
        Err(_) => return error_response(ryme_error::RymeError::Unauthorized),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = admit_realtime(&state, &principal.tenant, 1) {
        return error_response(e);
    }
    let connection = match open_realtime_connection(&state, &principal.tenant) {
        Ok(connection) => connection,
        Err(e) => return error_response(e),
    };
    let (branch, _, _) = match branch_snapshot_selected(
        &state,
        &headers,
        &principal.tenant,
        query.branch.as_deref(),
    ) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => (String::from("main"), state.backend.latest_commit(), 0),
        Err(e) => return error_response(e),
    };
    let realtime = state.realtime.clone();
    let tenant = principal.tenant.clone();
    let database = state.database.clone();
    let table = query.table.clone();
    let from = query.from.unwrap_or(u64::MAX);
    let from_sequence = query.from_sequence;
    let qos = state.qos.clone();
    let rls_executor =
        state.executor.clone().with_tenant(tenant.clone()).with_branch(branch.clone());
    upgrade.on_upgrade(move |socket| async move {
        let _connection = connection;
        forward_changes(
            socket,
            realtime,
            qos,
            rls_executor,
            &tenant,
            &database,
            &branch,
            &table,
            from,
            from_sequence,
        )
        .await;
    })
}

fn stream_realtime_event(qos: &Arc<Mutex<QosRegistry>>, tenant: &str, bytes: u64) -> bool {
    match qos.lock() {
        Ok(mut registry) => {
            let now = qos_now_nanos();
            registry.admit_realtime(tenant, 1, now).is_ok()
                && registry.admit_egress(tenant, bytes, now).is_ok()
        }
        Err(_) => false,
    }
}

const REALTIME_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const REALTIME_OUTGOING_QUEUE_CAPACITY: usize = 256;

type RealtimeSink =
    futures_util::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message>;

type RealtimeOutgoing = tokio::sync::mpsc::Sender<axum::extract::ws::Message>;

fn start_realtime_writer(sender: RealtimeSink) -> RealtimeOutgoing {
    let (outgoing, mut queue) = tokio::sync::mpsc::channel(REALTIME_OUTGOING_QUEUE_CAPACITY);
    tokio::spawn(async move {
        let mut sender = sender;
        while let Some(message) = queue.recv().await {
            if !send_realtime_message_with_sink(&mut sender, message).await {
                break;
            }
        }
    });
    outgoing
}

fn queue_realtime_message(
    outgoing: &RealtimeOutgoing,
    message: axum::extract::ws::Message,
) -> bool {
    outgoing.try_send(message).is_ok()
}

async fn send_realtime_message_with_sink(
    sender: &mut RealtimeSink,
    message: axum::extract::ws::Message,
) -> bool {
    tokio::time::timeout(REALTIME_SEND_TIMEOUT, sender.send(message))
        .await
        .map(|result| result.is_ok())
        .unwrap_or(false)
}

async fn forward_changes(
    socket: axum::extract::ws::WebSocket,
    realtime: Realtime,
    qos: Arc<Mutex<QosRegistry>>,
    rls_executor: Executor<BranchStorage>,
    tenant: &str,
    database: &str,
    branch: &str,
    table: &str,
    from: u64,
    from_sequence: Option<u64>,
) {
    let mut receiver = realtime.subscribe_branch(tenant, database, branch, table);
    let replayed = match from_sequence {
        Some(sequence) => realtime.replay_after_sequence_branch(
            tenant,
            database,
            branch,
            table,
            sequence,
            realtime.history_capacity(),
        ),
        None => realtime.replay_branch(
            tenant,
            database,
            branch,
            table,
            from,
            realtime.history_capacity(),
        ),
    };
    let mut seen_sequence = from_sequence.unwrap_or(0);
    let (sender, mut incoming) = socket.split();
    let outgoing = start_realtime_writer(sender);
    for record in &replayed {
        seen_sequence = seen_sequence.max(record.sequence);
        if !realtime_change_allowed_by_executor(&rls_executor, tenant, branch, record) {
            continue;
        }
        let text = serde_json::to_string(record).unwrap_or_else(|_| String::from("{}"));
        if !stream_realtime_event(&qos, tenant, text.len() as u64) {
            return;
        }
        if !queue_realtime_message(&outgoing, axum::extract::ws::Message::Text(text)) {
            return;
        }
    }
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !queue_realtime_message(
                    &outgoing,
                    axum::extract::ws::Message::Ping(Vec::new()),
                ) {
                    break;
                }
            }
            message = receiver.recv() => {
                match message {
                    Ok(record) => {
                        if record.sequence <= seen_sequence {
                            continue;
                        }
                        seen_sequence = seen_sequence.max(record.sequence);
                        if !realtime_change_allowed_by_executor(
                            &rls_executor,
                            tenant,
                            branch,
                            &record,
                        ) {
                            continue;
                        }
                        let text = serde_json::to_string(&record).unwrap_or_else(|_| String::from("{}"));
                        if !stream_realtime_event(&qos, tenant, text.len() as u64) {
                            break;
                        }
                        if !queue_realtime_message(
                            &outgoing,
                            axum::extract::ws::Message::Text(text),
                        ) {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        let recovered = realtime.replay_after_sequence_branch(
                            tenant,
                            database,
                            branch,
                            table,
                            seen_sequence,
                            realtime.history_capacity(),
                        );
                        // The topic ring is bounded. If it no longer contains
                        // every skipped topic event, close instead of silently
                        // delivering a permanently incomplete change stream.
                        if recovered.len() < skipped as usize {
                            break;
                        }
                        for record in recovered {
                            if record.sequence <= seen_sequence {
                                continue;
                            }
                            seen_sequence = record.sequence;
                            if !realtime_change_allowed_by_executor(
                                &rls_executor,
                                tenant,
                                branch,
                                &record,
                            ) {
                                continue;
                            }
                            let text = serde_json::to_string(&record)
                                .unwrap_or_else(|_| String::from("{}"));
                            if !stream_realtime_event(&qos, tenant, text.len() as u64) {
                                return;
                            }
                            if !queue_realtime_message(
                                &outgoing,
                                axum::extract::ws::Message::Text(text),
                            ) {
                                return;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            next = incoming.next() => {
                match next {
                    Some(Ok(axum::extract::ws::Message::Close(_))) | None => break,
                    Some(Ok(axum::extract::ws::Message::Ping(payload))) => {
                        if !queue_realtime_message(
                            &outgoing,
                            axum::extract::ws::Message::Pong(payload),
                        ) {
                            break;
                        }
                    }
                    _ => continue,
                }
            }
        }
    }
}

async fn query_stream(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<QueryStreamQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let principal = match state.principal_with_query(&headers, query.api_key.as_deref()) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_read() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    if let Err(e) = admit_realtime(&state, &principal.tenant, 1) {
        return error_response(e);
    }
    let connection = match open_realtime_connection(&state, &principal.tenant) {
        Ok(connection) => connection,
        Err(e) => return error_response(e),
    };
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let (branch, branch_commit, storage_epoch) = match branch_snapshot_selected(
        &state,
        &headers,
        &principal.tenant,
        query.branch.as_deref(),
    ) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => (String::from("main"), state.backend.latest_commit(), 0),
        Err(e) => return error_response(e),
    };
    let backend: BranchStorage = if branch == "main" {
        ryme_branch::BranchBackend::passthrough(state.backend.clone(), state.database.clone())
    } else {
        ryme_branch::BranchBackend::new(
            state.backend.clone(),
            state.database.clone(),
            branch.clone(),
            branch_commit,
            storage_epoch,
        )
    };
    let realtime = state.realtime.clone();
    let tenant = principal.tenant.clone();
    let database = state.database.clone();
    let table = query.table.clone();
    let query_spec = ReactiveQuerySpec::from_query(&query);
    let qos = state.qos.clone();
    let rls_executor =
        state.executor.clone().with_tenant(tenant.clone()).with_branch(branch.clone());
    upgrade.on_upgrade(move |socket| async move {
        let _connection = connection;
        forward_query(
            socket,
            backend,
            realtime,
            qos,
            rls_executor,
            &tenant,
            &database,
            &table,
            &branch,
            limit,
            branch_commit,
            query_spec,
        )
        .await;
    })
}

#[allow(clippy::too_many_arguments)]
async fn forward_query<B: TxnBackend + Send + Sync + 'static>(
    socket: axum::extract::ws::WebSocket,
    backend: B,
    realtime: Realtime,
    qos: Arc<Mutex<QosRegistry>>,
    rls_executor: Executor<BranchStorage>,
    tenant: &str,
    database: &str,
    table: &str,
    branch: &str,
    limit: usize,
    initial_commit: u64,
    query_spec: ReactiveQuerySpec,
) {
    if query_spec.is_reactive() {
        forward_reactive_query(
            socket,
            backend,
            realtime,
            qos,
            rls_executor,
            tenant,
            database,
            table,
            branch,
            limit,
            initial_commit,
            query_spec,
        )
        .await;
        return;
    }
    let mut receiver = realtime.query_subscribe_branch(tenant, database, branch, table, limit);
    let (sender, mut incoming) = socket.split();
    let outgoing = start_realtime_writer(sender);
    let current_commit =
        initial_commit.max(realtime.query_latest_commit_branch(tenant, database, branch, table));
    let (mut snapshot_commit, snapshot_queued) = send_query_snapshot(
        &outgoing,
        &backend,
        &qos,
        &rls_executor,
        tenant,
        database,
        table,
        branch,
        limit,
        current_commit,
    );
    if !snapshot_queued {
        return;
    }
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !queue_realtime_message(
                    &outgoing,
                    axum::extract::ws::Message::Ping(Vec::new()),
                ) {
                    break;
                }
            }
            message = receiver.recv() => {
                match message {
                    Ok(update) => {
                        if update.commit_ts <= snapshot_commit {
                            continue;
                        }
                        snapshot_commit = update.commit_ts;
                        let text = serde_json::to_string(&serde_json::json!({
                            "type": "update",
                            "commit": update.commit_ts,
                            "branch": branch,
                            "rows": update.rows.iter().filter(|row| {
                                rls_executor.row_allowed_by_rls(table, &row.value)
                            }).map(|row| serde_json::json!({
                                "pk": String::from_utf8_lossy(&row.pk),
                                "value": String::from_utf8_lossy(&row.value),
                            })).collect::<Vec<_>>(),
                            "truncated": update.truncated,
                        }))
                        .unwrap_or_else(|_| String::from("{}"));
                        if !stream_realtime_event(&qos, tenant, text.len() as u64) {
                            break;
                        }
                        if !queue_realtime_message(
                            &outgoing,
                            axum::extract::ws::Message::Text(text),
                        ) {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let current_commit = initial_commit.max(realtime.query_latest_commit_branch(
                            tenant, database, branch, table,
                        ));
                        let (commit, queued) = send_query_snapshot(
                            &outgoing,
                            &backend,
                            &qos,
                            &rls_executor,
                            tenant,
                            database,
                            table,
                            branch,
                            limit,
                            current_commit,
                        );
                        if !queued {
                            break;
                        }
                        snapshot_commit = commit;
                    }
                    Err(_) => break,
                }
            }
            next = incoming.next() => {
                match next {
                    Some(Ok(axum::extract::ws::Message::Close(_))) | None => break,
                    Some(Ok(axum::extract::ws::Message::Ping(payload))) => {
                        if !queue_realtime_message(
                            &outgoing,
                            axum::extract::ws::Message::Pong(payload),
                        ) {
                            break;
                        }
                    }
                    _ => continue,
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn forward_reactive_query<B: TxnBackend + Send + Sync + 'static>(
    socket: axum::extract::ws::WebSocket,
    backend: B,
    realtime: Realtime,
    qos: Arc<Mutex<QosRegistry>>,
    rls_executor: Executor<BranchStorage>,
    tenant: &str,
    database: &str,
    table: &str,
    branch: &str,
    limit: usize,
    initial_commit: u64,
    spec: ReactiveQuerySpec,
) {
    let mut receiver = realtime.subscribe_branch(tenant, database, branch, table);
    let (sender, mut incoming) = socket.split();
    let outgoing = start_realtime_writer(sender);
    let (mut snapshot_commit, snapshot_queued) = send_reactive_query_snapshot(
        &outgoing,
        &backend,
        &qos,
        &rls_executor,
        tenant,
        database,
        table,
        branch,
        limit,
        initial_commit,
        &spec,
        "snapshot",
    );
    if !snapshot_queued {
        return;
    }
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !queue_realtime_message(
                    &outgoing,
                    axum::extract::ws::Message::Ping(Vec::new()),
                ) {
                    break;
                }
            }
            message = receiver.recv() => {
                match message {
                    Ok(change) => {
                        if change.commit_ts <= snapshot_commit {
                            continue;
                        }
                        let (commit, queued) = send_reactive_query_snapshot(
                            &outgoing,
                            &backend,
                            &qos,
                            &rls_executor,
                            tenant,
                            database,
                            table,
                            branch,
                            limit,
                            change.commit_ts,
                            &spec,
                            "update",
                        );
                        if !queued {
                            break;
                        }
                        snapshot_commit = commit;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let current_commit = initial_commit.max(
                            realtime.latest_change_commit_branch(tenant, database, branch, table),
                        );
                        let (commit, queued) = send_reactive_query_snapshot(
                            &outgoing,
                            &backend,
                            &qos,
                            &rls_executor,
                            tenant,
                            database,
                            table,
                            branch,
                            limit,
                            current_commit,
                            &spec,
                            "snapshot",
                        );
                        if !queued {
                            break;
                        }
                        snapshot_commit = commit;
                    }
                    Err(_) => break,
                }
            }
            next = incoming.next() => {
                match next {
                    Some(Ok(axum::extract::ws::Message::Close(_))) | None => break,
                    Some(Ok(axum::extract::ws::Message::Ping(payload))) => {
                        if !queue_realtime_message(
                            &outgoing,
                            axum::extract::ws::Message::Pong(payload),
                        ) {
                            break;
                        }
                    }
                    _ => continue,
                }
            }
        }
    }
}

#[cfg(test)]
fn rls_row_allowed(
    rls_tables: &HashMap<String, String>,
    tenant: &str,
    table: &str,
    value: &[u8],
) -> bool {
    let Some(column) = rls_tables.get(table) else { return true };
    let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(value) else {
        return false;
    };
    object
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(column))
        .and_then(|(_, value)| value.as_str())
        .is_some_and(|row_tenant| row_tenant == tenant)
}

#[cfg(test)]
fn realtime_change_allowed(
    rls_tables: &HashMap<String, String>,
    tenant: &str,
    branch: &str,
    record: &ryme_realtime::ChangeRecord,
) -> bool {
    record.tenant == tenant
        && record.branch == branch
        && record
            .after
            .as_deref()
            .or(record.before.as_deref())
            .is_none_or(|value| rls_row_allowed(rls_tables, tenant, &record.table, value))
}

fn realtime_change_allowed_by_executor<B: TxnBackend>(
    executor: &Executor<B>,
    tenant: &str,
    branch: &str,
    record: &ryme_realtime::ChangeRecord,
) -> bool {
    record.tenant == tenant
        && record.branch == branch
        && record
            .after
            .as_deref()
            .or(record.before.as_deref())
            .is_none_or(|value| executor.row_allowed_by_rls(&record.table, value))
}

#[cfg(test)]
fn scan_realtime_rows<B: TxnBackend>(
    backend: &B,
    rls_tables: &HashMap<String, String>,
    tenant: &str,
    database: &str,
    table: &str,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    if limit == 0 {
        return Vec::new();
    }
    let mut txn = backend.begin();
    let mut visible = Vec::with_capacity(limit);
    let mut start_after = None;
    loop {
        let page = match start_after.as_deref() {
            Some(start_after) => backend
                .scan_after(&mut txn, tenant, database, table, start_after, limit)
                .unwrap_or_default(),
            None => backend.scan(&mut txn, tenant, database, table, limit).unwrap_or_default(),
        };
        if page.is_empty() {
            break;
        }
        let page_len = page.len();
        start_after = page.last().map(|(pk, _)| pk.clone());
        visible.extend(
            page.into_iter().filter(|(_, value)| rls_row_allowed(rls_tables, tenant, table, value)),
        );
        if visible.len() >= limit {
            visible.truncate(limit);
            break;
        }
        if page_len < limit {
            break;
        }
    }
    visible
}

fn scan_realtime_rows_with_executor<B: TxnBackend>(
    backend: &B,
    rls_executor: &Executor<BranchStorage>,
    tenant: &str,
    database: &str,
    table: &str,
    limit: usize,
    read_ts: Option<u64>,
    filters: &[(String, String)],
    order: Option<&str>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    if limit == 0 {
        return Vec::new();
    }
    let mut txn = backend.begin();
    if let Some(read_ts) = read_ts {
        txn.restamp(read_ts);
    }
    let mut visible = Vec::with_capacity(limit);
    let mut start_after = None;
    let full_order_scan = rest_order_requires_full_scan(order);
    let page_limit = if full_order_scan { 256 } else { limit };
    loop {
        let page = match start_after.as_deref() {
            Some(start_after) => backend
                .scan_after(&mut txn, tenant, database, table, start_after, page_limit)
                .unwrap_or_default(),
            None => backend.scan(&mut txn, tenant, database, table, page_limit).unwrap_or_default(),
        };
        if page.is_empty() {
            break;
        }
        let page_len = page.len();
        start_after = page.last().map(|(pk, _)| pk.clone());
        let page = page
            .into_iter()
            .filter(|(_, value)| rls_executor.row_allowed_by_rls(table, value))
            .collect::<Vec<_>>();
        visible.extend(filter_rows_by_params(page, filters));
        if !full_order_scan && visible.len() >= limit {
            break;
        }
        if page_len < page_limit {
            break;
        }
    }
    let mut visible = order_rows(visible, order);
    visible.truncate(limit);
    visible
}

fn send_query_snapshot<B: TxnBackend>(
    outgoing: &RealtimeOutgoing,
    backend: &B,
    qos: &Arc<Mutex<QosRegistry>>,
    rls_executor: &Executor<BranchStorage>,
    tenant: &str,
    database: &str,
    table: &str,
    branch: &str,
    limit: usize,
    commit: u64,
) -> (u64, bool) {
    let rows = scan_realtime_rows_with_executor(
        backend,
        rls_executor,
        tenant,
        database,
        table,
        limit,
        None,
        &[],
        None,
    );
    let snapshot = serde_json::json!({
        "type": "snapshot",
        "commit": commit,
        "branch": branch,
        "rows": rows
            .into_iter()
            .map(|(pk, value)| serde_json::json!({
                "pk": String::from_utf8_lossy(&pk),
                "value": String::from_utf8_lossy(&value),
            }))
            .collect::<Vec<_>>(),
    });
    let text = snapshot.to_string();
    if !stream_realtime_event(qos, tenant, text.len() as u64) {
        return (commit, false);
    }
    (commit, queue_realtime_message(outgoing, axum::extract::ws::Message::Text(text)))
}

fn send_reactive_query_snapshot<B: TxnBackend>(
    outgoing: &RealtimeOutgoing,
    backend: &B,
    qos: &Arc<Mutex<QosRegistry>>,
    rls_executor: &Executor<BranchStorage>,
    tenant: &str,
    database: &str,
    table: &str,
    branch: &str,
    limit: usize,
    commit: u64,
    spec: &ReactiveQuerySpec,
    kind: &str,
) -> (u64, bool) {
    let rows = scan_realtime_rows_with_executor(
        backend,
        rls_executor,
        tenant,
        database,
        table,
        limit,
        Some(commit),
        &spec.filters,
        spec.order.as_deref(),
    );
    let payload_rows = rows
        .into_iter()
        .map(|(pk, value)| {
            let mut row = rest_project_row(rest_row_to_json(&pk, &value), spec.select.as_deref());
            if let Some(object) = row.as_object_mut() {
                object.insert(
                    String::from("pk"),
                    serde_json::Value::String(String::from_utf8_lossy(&pk).to_string()),
                );
                object.insert(
                    String::from("value"),
                    serde_json::Value::String(String::from_utf8_lossy(&value).to_string()),
                );
            }
            row
        })
        .collect::<Vec<_>>();
    let text = serde_json::json!({
        "type": kind,
        "commit": commit,
        "branch": branch,
        "rows": payload_rows,
        "truncated": false,
    })
    .to_string();
    if !stream_realtime_event(qos, tenant, text.len() as u64) {
        return (commit, false);
    }
    (commit, queue_realtime_message(outgoing, axum::extract::ws::Message::Text(text)))
}

fn json_body<T>(body: &[u8]) -> Result<T, ryme_error::RymeError>
where
    T: serde::de::DeserializeOwned,
{
    match serde_json::from_slice(body) {
        Ok(value) => Ok(value),
        Err(_) => Err(ryme_error::RymeError::InvalidArgument(String::from("body"))),
    }
}

fn error_response(error: ryme_error::RymeError) -> Response {
    let (status, message) = match &error {
        ryme_error::RymeError::InvalidArgument(detail) => (StatusCode::BAD_REQUEST, detail.clone()),
        ryme_error::RymeError::NotFound(detail) => (StatusCode::NOT_FOUND, detail.clone()),
        ryme_error::RymeError::Conflict(detail) => (StatusCode::CONFLICT, detail.clone()),
        ryme_error::RymeError::Unauthorized => {
            (StatusCode::UNAUTHORIZED, String::from("unauthorized"))
        }
        ryme_error::RymeError::Forbidden => (StatusCode::FORBIDDEN, String::from("forbidden")),
        ryme_error::RymeError::Overload(detail) => (StatusCode::TOO_MANY_REQUESTS, detail.clone()),
        ryme_error::RymeError::Timeout => (StatusCode::GATEWAY_TIMEOUT, String::from("timeout")),
        ryme_error::RymeError::Unavailable(detail) => {
            (StatusCode::SERVICE_UNAVAILABLE, detail.clone())
        }
        ryme_error::RymeError::ReadOnly(detail) => {
            (StatusCode::SERVICE_UNAVAILABLE, detail.clone())
        }
        ryme_error::RymeError::Corrupt(detail) => {
            (StatusCode::INTERNAL_SERVER_ERROR, detail.clone())
        }
        ryme_error::RymeError::Io(detail) => (StatusCode::INTERNAL_SERVER_ERROR, detail.clone()),
        ryme_error::RymeError::Internal(detail) => {
            (StatusCode::INTERNAL_SERVER_ERROR, detail.clone())
        }
    };
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn load_jwt_verifier() -> ryme_error::Result<Option<JwtVerifier>> {
    let issuer = std::env::var("RYME_JWT_ISSUER").ok();
    let audience = std::env::var("RYME_JWT_AUDIENCE").ok();
    if let Ok(secret) = std::env::var("RYME_JWT_SECRET") {
        return Ok(Some(match (issuer, audience) {
            (Some(issuer), Some(audience)) => {
                JwtVerifier::with_issuer(secret.into_bytes(), issuer, audience)
            }
            _ => JwtVerifier::new(secret.into_bytes()),
        }));
    }

    let Some(path) = std::env::var_os("RYME_JWT_JWKS_FILE") else {
        if let Ok(url) = std::env::var("RYME_JWT_JWKS_URL") {
            let verifier = load_jwks_verifier_url(
                &url,
                std::env::var("RYME_JWT_JWK_KID").ok(),
                issuer,
                audience,
            )?;
            return Ok(Some(verifier));
        }
        return Ok(None);
    };
    let verifier = load_jwks_verifier(
        std::path::Path::new(&path),
        std::env::var("RYME_JWT_JWK_KID").ok(),
        issuer,
        audience,
    )?;
    Ok(Some(verifier))
}

fn load_jwks_verifier_url(
    url: &str,
    requested_kid: Option<String>,
    issuer: Option<String>,
    audience: Option<String>,
) -> ryme_error::Result<JwtVerifier> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|error| {
            ryme_error::RymeError::InvalidArgument(format!("jwt jwks client: {error}"))
        })?;
    let response =
        client.get(url).send().and_then(|response| response.error_for_status()).map_err(
            |error| ryme_error::RymeError::Unavailable(format!("jwt jwks url: {error}")),
        )?;
    let raw = response
        .bytes()
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("jwt jwks url body: {error}")))?;
    load_jwks_verifier_bytes(&raw, requested_kid, issuer, audience)
}

fn load_jwks_verifier(
    path: &std::path::Path,
    requested_kid: Option<String>,
    issuer: Option<String>,
    audience: Option<String>,
) -> ryme_error::Result<JwtVerifier> {
    let raw = std::fs::read(path).map_err(|error| {
        ryme_error::RymeError::InvalidArgument(format!("jwt jwks file {}: {error}", path.display()))
    })?;
    load_jwks_verifier_bytes(&raw, requested_kid, issuer, audience)
}

fn load_jwks_verifier_bytes(
    raw: &[u8],
    requested_kid: Option<String>,
    issuer: Option<String>,
    audience: Option<String>,
) -> ryme_error::Result<JwtVerifier> {
    let document: serde_json::Value = serde_json::from_slice(&raw)
        .map_err(|error| ryme_error::RymeError::Corrupt(format!("jwt jwks: {error}")))?;
    let keys = document
        .get("keys")
        .and_then(|value| value.as_array())
        .ok_or_else(|| ryme_error::RymeError::Corrupt(String::from("jwt jwks keys")))?;
    let selected_keys = keys
        .iter()
        .filter(|key| key.get("kty").and_then(|value| value.as_str()) == Some("RSA"))
        .filter(|key| {
            key.get("alg")
                .and_then(|value| value.as_str())
                .map(|algorithm| algorithm == "RS256")
                .unwrap_or(true)
        })
        .filter(|key| {
            requested_kid
                .as_deref()
                .map(|kid| key.get("kid").and_then(|value| value.as_str()) == Some(kid))
                .unwrap_or(true)
        })
        .map(|key| {
            let kid = key.get("kid").and_then(|value| value.as_str()).map(String::from);
            let modulus = key
                .get("n")
                .and_then(|value| value.as_str())
                .ok_or_else(|| ryme_error::RymeError::Corrupt(String::from("jwt jwks modulus")))
                .and_then(|value| {
                    ryme_auth::base64_url_decode(value).map_err(|_| {
                        ryme_error::RymeError::Corrupt(String::from("jwt jwks modulus"))
                    })
                })?;
            let exponent = key
                .get("e")
                .and_then(|value| value.as_str())
                .ok_or_else(|| ryme_error::RymeError::Corrupt(String::from("jwt jwks exponent")))
                .and_then(|value| {
                    ryme_auth::base64_url_decode(value).map_err(|_| {
                        ryme_error::RymeError::Corrupt(String::from("jwt jwks exponent"))
                    })
                })?;
            Ok(RsaJwk { kid, modulus, exponent })
        })
        .collect::<ryme_error::Result<Vec<_>>>()?;
    if selected_keys.is_empty() {
        return Err(ryme_error::RymeError::Corrupt(String::from("jwt jwks rsa key")));
    }
    let verifier = match (issuer, audience) {
        (Some(issuer), Some(audience)) => {
            JwtVerifier::with_rsa_jwks_issuer(selected_keys, issuer, audience)
        }
        (None, None) => JwtVerifier::with_rsa_jwks(selected_keys),
        _ => {
            return Err(ryme_error::RymeError::InvalidArgument(String::from(
                "jwt issuer and audience must be configured together",
            )))
        }
    };
    Ok(verifier)
}

fn wire_tenant(
    expected_password: &str,
    keys: &ApiKeyStore,
    jwt: Option<&JwtVerifier>,
    default_tenant: &str,
    password: &str,
) -> ryme_error::Result<String> {
    let expected = expected_password.as_bytes();
    let presented = password.as_bytes();
    let password_matches = expected.len() == presented.len()
        && expected
            .iter()
            .zip(presented)
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0;
    if password_matches {
        return Ok(default_tenant.to_string());
    }
    if let Ok(principal) = keys.authenticate(password) {
        return Ok(principal.tenant);
    }
    if let Some(verifier) = jwt {
        if let Ok(principal) = verifier.principal_from_token(password, now_secs()) {
            return Ok(principal.tenant);
        }
    }
    Err(ryme_error::RymeError::Unauthorized)
}

fn resp_authenticator(state: &SharedState) -> Option<ryme_wire_resp::RespAuthenticator> {
    let expected_password = std::env::var("RYME_RESP_PASSWORD").ok()?;
    let keys = state.keys.clone();
    let jwt = state.jwt.clone();
    let default_tenant = state.tenant.clone();
    Some(Arc::new(move |_user, password| {
        wire_tenant(&expected_password, &keys, jwt.as_ref(), &default_tenant, password)
    }))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WrappedRing {
    kek_id: String,
    deks: Vec<WrappedDek>,
}

fn kms_from_env() -> Option<EnvKms> {
    std::env::var("RYME_KMS_KEY")
        .ok()
        .and_then(|key| EnvKms::from_key(String::from("env-1"), key.into_bytes()).ok())
}

fn load_or_create_ring(
    data_dir: &std::path::Path,
    tenant: &str,
) -> ryme_error::Result<Arc<Mutex<KeyRing>>> {
    let path = data_dir.join("keyring.json");
    if let Ok(raw) = std::fs::read(&path) {
        if let Ok(wrapped) = serde_json::from_slice::<WrappedRing>(&raw) {
            if let Some(kms) = kms_from_env() {
                if kms.kek_id() == wrapped.kek_id {
                    let mut ring = KeyRing::new();
                    if ring.import_wrapped(wrapped.deks, &kms).is_ok()
                        && ring.active(tenant).is_some()
                    {
                        return Ok(Arc::new(Mutex::new(ring)));
                    }
                }
            }
        }
        if let Ok(ring) = serde_json::from_slice::<KeyRing>(&raw) {
            if ring.active(tenant).is_some() {
                return Ok(Arc::new(Mutex::new(ring)));
            }
        }
    }
    let mut ring = KeyRing::new();
    ring.rotate(tenant, now_secs());
    persist_ring(&ring, &path)?;
    Ok(Arc::new(Mutex::new(ring)))
}

fn persist_ring(ring: &KeyRing, path: &std::path::Path) -> ryme_error::Result<()> {
    let raw = match kms_from_env() {
        Some(kms) => {
            let mut deks = Vec::new();
            for tenant in ring.tenants() {
                deks.extend(ring.export_wrapped(&tenant, &kms).unwrap_or_default());
            }
            serde_json::to_vec(&WrappedRing { kek_id: kms.kek_id(), deks })
                .map_err(|e| ryme_error::RymeError::Internal(e.to_string()))?
        }
        None => {
            tracing::warn!(
                "persisting DEK ring without KMS wrap; volume encryption is the outer layer"
            );
            serde_json::to_vec(ring).map_err(|e| ryme_error::RymeError::Internal(e.to_string()))?
        }
    };
    std::fs::write(path, &raw)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn elapsed_micros(start: SystemTime) -> u64 {
    start.elapsed().map(|d| d.as_micros() as u64).unwrap_or(0)
}

fn wal_segment_files(dir: &std::path::Path) -> ryme_error::Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(ryme_error::RymeError::from(e)),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with("seg-") && name.ends_with(".wal") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn immutable_segment_files(dir: &std::path::Path) -> ryme_error::Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    let segments = dir.join("segments");
    let entries = match std::fs::read_dir(&segments) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = path.file_name().and_then(|name| name.to_str()).unwrap_or("");
        if name.starts_with("segment-") && name.ends_with(".sst") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn archive_metadata_files(
    data_dir: &std::path::Path,
) -> ryme_error::Result<Vec<(String, Vec<u8>)>> {
    let mut files = Vec::new();
    for name in [
        "schema.json",
        "branches.json",
        "control.json",
        "topics.json",
        "auth.json",
        "placements.json",
    ] {
        let path = data_dir.join(name);
        match std::fs::read(&path) {
            Ok(bytes) => files.push((String::from(name), bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }

    let root = data_dir.join("branch-schemas");
    let mut pending = vec![root.clone()];
    while let Some(directory) = pending.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            if file_type.is_dir() {
                pending.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let relative = path
                .strip_prefix(data_dir)
                .map_err(|_| ryme_error::RymeError::Internal(String::from("archive path")))?;
            let name = relative.to_string_lossy().replace('\\', "/");
            files.push((name, std::fs::read(path)?));
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(files)
}

async fn snapshot_backend_files(
    state: &SharedState,
) -> ryme_error::Result<Vec<(u64, std::path::PathBuf)>> {
    match &state.backend {
        Backend::Single(manager) => Ok(vec![manager.write_snapshot()?]),
        Backend::Cluster(backend) => {
            Ok(vec![snapshot_manager(backend.node().manager(), &state.data_dir.join("snapshots"))?])
        }
        Backend::Sharded(shards) => {
            let mut out = Vec::new();
            for index in 0..shards.shard_count() {
                let Some(manager) = shards.shard_manager(index) else {
                    continue;
                };
                out.push(manager.write_snapshot()?);
            }
            if out.is_empty() {
                return Err(ryme_error::RymeError::Internal(String::from("snapshot")));
            }
            Ok(out)
        }
        Backend::Hybrid(hybrid) => {
            let mut out = vec![snapshot_manager(
                hybrid.raft().node().manager(),
                &state.data_dir.join("snapshots"),
            )?];
            let local = hybrid.local();
            for index in 0..local.shard_count() {
                let Some(manager) = local.shard_manager(index) else {
                    continue;
                };
                out.push(manager.write_snapshot()?);
            }
            Ok(out)
        }
    }
}

fn snapshot_manager(
    manager: &TxnManager,
    dir: &std::path::Path,
) -> ryme_error::Result<(u64, std::path::PathBuf)> {
    std::fs::create_dir_all(dir)?;
    let raw = manager.encode_snapshot()?;
    let snapshot = ryme_storage::Engine::decode_snapshot(&raw)?;
    let max = snapshot.max_commit_ts();
    let path = dir.join(format!("snap-{max:020}.rsnap"));
    let tmp = dir.join("snap.tmp");
    std::fs::write(&tmp, &raw)?;
    std::fs::rename(&tmp, &path)?;
    std::fs::write(dir.join("latest"), path.file_name().and_then(|n| n.to_str()).unwrap_or(""))?;
    Ok((max, path))
}

fn archive_target(
    config: &ryme_config::ArchiveConfig,
) -> ryme_error::Result<Option<ArchiveTarget>> {
    if let Some(dir) = config.local_dir.clone() {
        return Ok(Some(ArchiveTarget::Local(ryme_archive::local::LocalStore::new(dir))));
    }
    match (config.s3_endpoint.clone(), config.s3_bucket.clone(), config.s3_region.clone()) {
        (Some(endpoint), Some(bucket), Some(region)) => {
            let access_key = std::env::var("RYME_S3_ACCESS_KEY").unwrap_or_default();
            let secret_key = std::env::var("RYME_S3_SECRET_KEY").unwrap_or_default();
            Ok(Some(ArchiveTarget::S3(ryme_archive::s3::S3Store::new(
                ryme_archive::s3::S3Config {
                    endpoint,
                    bucket,
                    region,
                    access_key,
                    secret_key,
                    path_style: config.s3_path_style,
                },
            )?)))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod realtime_policy_tests {
    use super::*;

    #[tokio::test]
    async fn realtime_outgoing_queue_rejects_slow_consumers() {
        let (outgoing, mut queued) = tokio::sync::mpsc::channel(2);

        assert!(queue_realtime_message(&outgoing, axum::extract::ws::Message::Ping(vec![])));
        assert!(queue_realtime_message(&outgoing, axum::extract::ws::Message::Ping(vec![])));
        assert!(!queue_realtime_message(&outgoing, axum::extract::ws::Message::Ping(vec![])));

        assert!(matches!(queued.recv().await, Some(axum::extract::ws::Message::Ping(_))));
    }

    #[test]
    fn archive_metadata_includes_branch_schema_tree() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-archive-metadata-{}-{}",
            std::process::id(),
            now_secs()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("branch-schemas/tenant/preview")).unwrap();
        std::fs::write(dir.join("schema.json"), b"main-schema").unwrap();
        std::fs::write(dir.join("branches.json"), b"branches").unwrap();
        std::fs::write(dir.join("placements.json"), b"placements").unwrap();
        std::fs::write(dir.join("branch-schemas/tenant/preview/schema.json"), b"branch-schema")
            .unwrap();

        let files = archive_metadata_files(&dir).unwrap();
        let names: Vec<_> = files.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "branch-schemas/tenant/preview/schema.json",
                "branches.json",
                "placements.json",
                "schema.json",
            ]
        );
        assert_eq!(files[0].1, b"branch-schema".to_vec());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn jwks_url_loader_fetches_and_selects_requested_key() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let body =
            br#"{"keys":[{"kty":"RSA","kid":"url-key","alg":"RS256","n":"AQAB","e":"AQAB"}]}"#;
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request);
            write!(
                socket,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                String::from_utf8_lossy(body)
            )
            .unwrap();
        });
        let verifier = load_jwks_verifier_url(
            &format!("http://{address}/jwks.json"),
            Some(String::from("url-key")),
            Some(String::from("issuer")),
            Some(String::from("audience")),
        )
        .unwrap();
        assert!(verifier.principal_from_token("invalid.token.value", 0).is_err());
        server.join().unwrap();
    }

    #[test]
    fn realtime_snapshot_filters_rls_rows_across_pages() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-realtime-policy-{}-{}",
            std::process::id(),
            now_secs()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Never).unwrap();
        let backend = Backend::Single(manager.clone());
        let mut txn = manager.begin();
        manager.put(
            &mut txn,
            ryme_storage::RecordKey::new("tenant-a", "default", "messages", b"a-hidden"),
            br#"{"tenant_id":"tenant-b","body":"secret"}"#.to_vec(),
        );
        manager.put(
            &mut txn,
            ryme_storage::RecordKey::new("tenant-a", "default", "messages", b"b-visible"),
            br#"{"tenant_id":"tenant-a","body":"hello"}"#.to_vec(),
        );
        manager.commit(txn).unwrap();
        let policies = HashMap::from([(String::from("messages"), String::from("tenant_id"))]);

        let rows = scan_realtime_rows(&backend, &policies, "tenant-a", "default", "messages", 1);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, b"b-visible");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn realtime_delete_filter_checks_before_image() {
        let policies = HashMap::from([(String::from("messages"), String::from("tenant_id"))]);
        let record = ryme_realtime::ChangeRecord {
            tenant: String::from("tenant-a"),
            database: String::from("default"),
            branch: String::from("main"),
            table: String::from("messages"),
            op: ryme_realtime::Operation::Delete,
            pk: b"hidden".to_vec(),
            before: Some(br#"{"tenant_id":"tenant-b"}"#.to_vec()),
            after: None,
            commit_ts: 2,
            tx_id: 2,
            sequence: 1,
        };
        assert!(!realtime_change_allowed(&policies, "tenant-a", "main", &record));
    }

    #[tokio::test]
    async fn realtime_filters_rows_using_sql_policy_state() {
        let executor = ryme_sql::Executor::new(String::from("tenant-a"), String::from("default"));
        executor
            .execute(
                ryme_sql::parse("CREATE TABLE messages (id TEXT PRIMARY KEY, tenant_id TEXT)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                ryme_sql::parse(
                    "CREATE POLICY own_messages ON messages FOR ALL USING (auth.uid() = tenant_id) WITH CHECK (auth.uid() = tenant_id)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(ryme_sql::parse("ALTER TABLE messages ENABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        let hidden = ryme_realtime::ChangeRecord {
            tenant: String::from("tenant-a"),
            database: String::from("default"),
            branch: String::from("main"),
            table: String::from("messages"),
            op: ryme_realtime::Operation::Insert,
            pk: b"hidden".to_vec(),
            before: None,
            after: Some(br#"{"tenant_id":"tenant-b"}"#.to_vec()),
            commit_ts: 1,
            tx_id: 1,
            sequence: 1,
        };
        let visible = ryme_realtime::ChangeRecord {
            after: Some(br#"{"tenant_id":"tenant-a"}"#.to_vec()),
            ..hidden.clone()
        };
        assert!(!realtime_change_allowed_by_executor(&executor, "tenant-a", "main", &hidden));
        assert!(realtime_change_allowed_by_executor(&executor, "tenant-a", "main", &visible));
    }

    #[tokio::test]
    async fn sql_schema_snapshot_survives_state_restart() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-schema-restart-{}-{}",
            std::process::id(),
            now_secs()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut config = ryme_config::Config::default();
        config.node_id = String::from("schema-restart");
        config.data_dir = dir.clone();

        let state = SharedState::build(&config).unwrap();
        state
            .executor
            .execute(
                ryme_sql::parse("CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT)").unwrap(),
            )
            .await
            .unwrap();
        state
            .executor
            .execute(ryme_sql::parse("CREATE INDEX messages_body_idx ON messages (body)").unwrap())
            .await
            .unwrap();
        drop(state);

        let restarted = SharedState::build(&config).unwrap();
        assert_eq!(restarted.executor.catalog_tables(), vec![String::from("messages")]);
        assert_eq!(restarted.executor.catalog_indexes("messages").len(), 1);
        let mode = std::fs::metadata(dir.join("schema.json")).unwrap().permissions();
        #[cfg(unix)]
        assert_eq!(std::os::unix::fs::PermissionsExt::mode(&mode) & 0o077, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn branch_schema_survives_executor_recreation_without_leaking_to_main() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-branch-schema-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut config = ryme_config::Config::default();
        config.node_id = String::from("branch-schema");
        config.data_dir = dir.clone();
        let state = SharedState::build(&config).unwrap();
        state
            .executor
            .execute(
                ryme_sql::parse("CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT)").unwrap(),
            )
            .await
            .unwrap();
        state
            .executor
            .execute(ryme_sql::parse("INSERT INTO messages KEY 'm1' VALUE 'hello'").unwrap())
            .await
            .unwrap();

        {
            let mut control = state.control.lock().unwrap();
            let mut branches = control.branches.clone();
            branches
                .create_child_for(
                    "default",
                    String::from("preview"),
                    "main",
                    state.backend.latest_commit(),
                )
                .unwrap();
            branches.persist(&state.branch_path).unwrap();
            control.branches = branches;
        }
        ryme_sql::persist_schema_snapshot(
            &state.branch_schema_path("default", "preview"),
            &state.executor.schema_snapshot(),
        )
        .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("x-ryme-branch", axum::http::HeaderValue::from_static("preview"));
        let branch = branch_executor(&state, &headers, "default").unwrap();
        branch
            .execute(ryme_sql::parse("CREATE TABLE branch_only (id TEXT PRIMARY KEY)").unwrap())
            .await
            .unwrap();
        drop(branch);

        let recreated = branch_executor(&state, &headers, "default").unwrap();
        assert!(recreated.catalog_tables().contains(&String::from("branch_only")));
        assert!(!state.executor.catalog_tables().contains(&String::from("branch_only")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn migration_lock_serializes_duplicate_execution() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-migration-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut config = ryme_config::Config::default();
        config.node_id = String::from("migration-lock");
        config.data_dir = dir.clone();
        let state = SharedState::build(&config).unwrap();
        let mut receiver = state.realtime.subscribe_branch("default", "default", "main", "docs");
        let request = MigrateApplyRequest {
            id: String::from("race-1"),
            sql: String::from("UPSERT INTO docs KEY 'k1' VALUE 'v1'"),
            author: Some(String::from("test")),
        };
        let statement = ryme_sql::parse(&request.sql).unwrap();
        let left = apply_migration_locked(
            &state,
            String::from("default"),
            request.clone(),
            statement.clone(),
            String::from("test"),
        );
        let right = apply_migration_locked(
            &state,
            String::from("default"),
            request,
            statement,
            String::from("test"),
        );
        let (left, right) = tokio::join!(left, right);
        let results = [left, right];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(ryme_error::RymeError::Conflict(_))))
                .count(),
            1
        );
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.pk, b"k1");
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
            .await
            .is_err());
        assert_eq!(state.control.lock().unwrap().migrations.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn range_topology_survives_control_snapshot() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-range-snapshot-{}-{}",
            std::process::id(),
            now_secs()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut config = Config::default();
        config.data_dir = dir.clone();
        config.archive.interval_secs = 0;

        let state = SharedState::build(&config).unwrap();
        {
            let mut control = state.control.lock().unwrap();
            control
                .split_range(
                    "range-0",
                    b"m".to_vec(),
                    String::from("left"),
                    String::from("right"),
                    0,
                )
                .unwrap();
            state.persist_control_locked(&control).unwrap();
        }
        drop(state);

        let reopened = SharedState::build(&config).unwrap();
        let control = reopened.control.lock().unwrap();
        assert_eq!(control.ranges().len(), 2);
        assert_eq!(control.route(b"a").unwrap().id, "left");
        assert_eq!(control.route(b"z").unwrap().id, "right");
        drop(control);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
