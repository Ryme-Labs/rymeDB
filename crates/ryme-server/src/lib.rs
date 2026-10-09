use axum::extract::{Path, Query, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Json;
use futures_util::{SinkExt, StreamExt};
use ryme_archive::{Archiver, BackupManifest};
use ryme_auth::{
    ApiKeyStore, CredentialStore, JwtVerifier, OidcConfig, PasskeyRegistry, PolicyEngine,
    Principal, Role,
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
use ryme_raft::net::{ClusterBackend, Node};
use ryme_realtime::Realtime;
use ryme_router::Range;
use ryme_shard::{HybridBackend, ShardSet, TableRef};
use ryme_sql::{bind, parse, Executor, QueryResult};
use ryme_txn::{DurableManager, SyncPolicy, TxnManager};
use std::collections::{HashMap, HashSet};
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
            Self::Single(manager) => manager.inner().spaces(),
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
                for (tenant, database, table) in manager.inner().spaces().unwrap_or_default() {
                    let bytes =
                        manager.inner().table_bytes(&tenant, &database, &table).unwrap_or(0);
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
                let expired = manager.inner().expired_keys(tenant, database, table, limit)?;
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
                let expired = manager.inner().expired_keys(tenant, database, table, limit)?;
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
                        let expired =
                            manager.inner().expired_keys(tenant, database, table, limit)?;
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
                after: None,
                commit_ts,
            });
        }
    }
}

#[derive(Debug, Clone)]
pub struct SharedState {
    backend: Backend,
    durable: DurableManager,
    gateway: Gateway<Backend>,
    executor: Executor<Backend>,
    rls_tables: HashMap<String, String>,
    realtime: Realtime,
    keys: ApiKeyStore,
    jwt: Option<JwtVerifier>,
    oidc: Option<OidcConfig>,
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
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BroadcastStreamQuery {
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct QueryStreamQuery {
    pub table: String,
    pub limit: Option<usize>,
    pub api_key: Option<String>,
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
    pub fn build(config: &Config) -> ryme_error::Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        let wal_dir = config.data_dir.join("wal");
        let policy = match config.durability.is_durable() {
            true => SyncPolicy::Always,
            false => SyncPolicy::Never,
        };
        let durable = DurableManager::open(&wal_dir, 64 * 1024 * 1024, policy)?;
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
                let local = ryme_shard::ShardSet::open(&config.data_dir.join("local"), 1, policy)?;
                Backend::Hybrid(HybridBackend::new(ClusterBackend::new(node), local, replicated))
            }
            None if config.shards > 1 => Backend::Sharded(ryme_shard::ShardSet::open(
                &config.data_dir,
                config.shards,
                policy,
            )?),
            None => Backend::Single(durable.clone()),
        };
        let realtime = Realtime::new(4096);
        let mut policies = PolicyEngine::new();
        for (table, tenant_column) in &config.rls_tables {
            policies.allow_table(table.clone(), tenant_column.clone());
        }
        let tenant = String::from("default");
        let database = String::from("default");
        let branch = String::from("main");
        let mut gateway = Gateway::with_backend(
            tenant.clone(),
            database.clone(),
            branch.clone(),
            policies,
            realtime.clone(),
            backend.clone(),
        );
        gateway.set_read_only(config.read_only);
        let mut executor =
            Executor::with_backend(tenant.clone(), database.clone(), backend.clone())
                .with_realtime(realtime.clone());
        executor.set_rls_tables(config.rls_tables.clone());
        executor.set_read_only(config.read_only);
        let mut control = ControlPlane::new();
        control.add_range(Range::new(
            String::from("range-0"),
            Vec::new(),
            Vec::new(),
            config.node_id.clone(),
            0,
        ));
        let _ = control.branches.create_root(
            branch.clone(),
            ryme_branch::Manifest {
                id: String::from("genesis"),
                segments: Vec::new(),
                wal_start: 0,
            },
        );
        let keys = ApiKeyStore::new();
        let api_key =
            std::env::var("RYME_API_KEY").unwrap_or_else(|_| String::from("ryme-dev-key"));
        let mut roles = HashSet::new();
        roles.insert(Role::Owner);
        keys.insert(api_key, Principal { id: String::from("dev"), tenant: tenant.clone(), roles });
        let jwt = std::env::var("RYME_JWT_SECRET").ok().map(|s| {
            match (std::env::var("RYME_JWT_ISSUER").ok(), std::env::var("RYME_JWT_AUDIENCE").ok()) {
                (Some(issuer), Some(audience)) => {
                    JwtVerifier::with_issuer(s.into_bytes(), issuer, audience)
                }
                _ => JwtVerifier::new(s.into_bytes()),
            }
        });
        let archive = archive_target(&config.archive)?;
        let archive_replica = match config.archive_replica.clone() {
            Some(replica) => archive_target(&replica)?,
            None => None,
        };
        let dek_ring = load_or_create_ring(&config.data_dir, &tenant)?;
        let oidc = match (
            std::env::var("RYME_OIDC_ISSUER").ok(),
            std::env::var("RYME_OIDC_AUDIENCE").ok(),
            std::env::var("RYME_OIDC_SECRET").ok(),
        ) {
            (Some(issuer), Some(audience), Some(secret)) => Some(OidcConfig {
                issuer,
                audience,
                client_secret: secret.into_bytes(),
                auth_endpoint: std::env::var("RYME_OIDC_AUTH_ENDPOINT").unwrap_or_default(),
                client_id: std::env::var("RYME_OIDC_CLIENT_ID").unwrap_or_default(),
            }),
            _ => None,
        };
        Ok(Self {
            backend,
            durable,
            gateway,
            executor,
            rls_tables: config.rls_tables.clone(),
            realtime,
            keys,
            jwt,
            oidc,
            control: Arc::new(Mutex::new(control)),
            latency: LatencyWindow::new(),
            histogram: Histogram::new(1024),
            slow_log: SlowLog::new(256),
            traces: Arc::new(Mutex::new(TraceCollector::new(256))),
            metering: Arc::new(Mutex::new(MeterRegistry::new())),
            qos: Arc::new(Mutex::new(QosRegistry::new())),
            dek_ring,
            indexes: Arc::new(Mutex::new(PartitionedIndex::new(config.index_partitions))),
            credentials: Arc::new(Mutex::new(CredentialStore::new())),
            passkeys: Arc::new(Mutex::new(PasskeyRegistry::new())),
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
            get(rest_list).post(rest_insert).patch(rest_upsert).delete(rest_delete),
        )
        .route("/graphql", post(graphql_exec))
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
        .route("/v1/presence/:channel", get(presence_list))
        .route("/v1/broadcast", post(broadcast_post))
        .route("/v1/broadcast/:channel", get(broadcast_stream))
        .route("/v1/topics/append", post(durable_append))
        .route("/v1/topics/read", get(durable_read))
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
        .route("/v1/cluster/members", get(cluster_members).post(cluster_add_member))
        .route("/v1/cluster/members/:id", delete(cluster_remove_member))
        .route("/v1/cluster/transfer", post(cluster_transfer))
        .route("/v1/cluster/replace", post(cluster_replace))
        .route("/v1/regions", get(regions))
        .route("/v1/stream", get(stream))
        .route("/v1/query-stream", get(query_stream))
        .layer(axum::extract::DefaultBodyLimit::max(2 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(state.clone(), read_only_guard))
        .with_state(state)
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
    if method == axum::http::Method::GET || method == axum::http::Method::HEAD {
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
    let mut pg_executor =
        Executor::with_backend(state.tenant.clone(), state.database.clone(), state.backend.clone())
            .with_realtime(state.realtime.clone());
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
    let pg = match state.tls.clone() {
        Some(acceptor) => ryme_wire_pg::PgGateway::with_backend_tls(pg_executor, acceptor),
        None => ryme_wire_pg::PgGateway::with_backend_executor(pg_executor),
    }
    .with_qos(state.qos.clone())
    .with_metering(state.metering.clone())
    .with_observe(state.latency.clone(), state.histogram.clone(), state.slow_log.clone())
    .with_range_hook(range_hook.clone());
    let resp = ryme_wire_resp::RespGateway::with_backend(
        state.tenant.clone(),
        state.database.clone(),
        state.backend.clone(),
    )
    .with_realtime(state.realtime.clone())
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
        let gateway = ryme_wire_resp::RespGateway::with_backend(
            resp_tls_state.tenant.clone(),
            resp_tls_state.database.clone(),
            resp_tls_state.backend.clone(),
        )
        .with_realtime(resp_tls_state.realtime.clone())
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
            let created = match autosplit_state.control.lock() {
                Ok(mut control) => control.auto_split_once(autosplit_writes).unwrap_or_default(),
                Err(_) => Vec::new(),
            };
            if !created.is_empty() {
                tracing::info!(ranges = ?created, "auto-split hot ranges");
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
    }))
}

async fn prometheus(State(state): State<SharedState>) -> Response {
    let histogram = state.histogram.snapshot();
    let body = format!(
        "# HELP rymedb_uptime_seconds node uptime\n# TYPE rymedb_uptime_seconds counter\nrymedb_uptime_seconds {} \n# HELP rymedb_commit_index latest commit\n# TYPE rymedb_commit_index gauge\nrymedb_commit_index {} \n# HELP rymedb_rest_microseconds rest latency\n# TYPE rymedb_rest_microseconds summary\nrymedb_rest_microseconds{{quantile=\"0.5\"}} {}\nrymedb_rest_microseconds{{quantile=\"0.9\"}} {}\nrymedb_rest_microseconds{{quantile=\"0.95\"}} {}\nrymedb_rest_microseconds{{quantile=\"0.99\"}} {}\nrymedb_rest_microseconds_count {}\nrymedb_rest_microseconds_max {} \n",
        now_secs().saturating_sub(state.started_unix),
        state.backend.latest_commit(),
        histogram.p50_micros,
        histogram.p90_micros,
        histogram.p95_micros,
        histogram.p99_micros,
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

fn range_routing_key(table: &str, key: &[u8]) -> Vec<u8> {
    let mut routing = Vec::with_capacity(table.len() + key.len() + 1);
    routing.extend_from_slice(table.as_bytes());
    routing.push(0);
    routing.extend_from_slice(key);
    routing
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
    let rows = match state.gateway.scan(&principal, &table, limit.saturating_add(offset)) {
        Ok(rows) => rows,
        Err(e) => return error_response(e),
    };
    let filtered = rest_list_filtered(rows, raw.as_deref().unwrap_or(""));
    let ordered = order_rows(filtered, query.order.as_deref());
    let paged: Vec<(Vec<u8>, Vec<u8>)> = ordered.into_iter().skip(offset).take(limit).collect();
    let egress: u64 = paged.iter().map(|(pk, value)| (pk.len() + value.len()) as u64).sum();
    if let Err(e) = admit_egress(&state, &principal.tenant, egress) {
        return error_response(e);
    }
    let items: Vec<serde_json::Value> = paged
        .iter()
        .map(|(pk, value)| rest_row_to_json(pk, &state.gateway.masked(&table, value.clone())))
        .collect();
    record_meter(&state, Metric::ReadUnit, items.len() as u64);
    let micros = elapsed_micros(start);
    state.latency.observe_micros(micros);
    state.histogram.record(micros);
    record_span(&state, "rest_list", &[("table", table.as_str())], micros);
    (StatusCode::OK, Json(items)).into_response()
}

fn filter_rows_by_query(rows: Vec<(Vec<u8>, Vec<u8>)>, raw: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut key_eq: Option<String> = None;
    for pair in raw.split('&') {
        let mut split = pair.splitn(2, '=');
        let name = split.next().unwrap_or("");
        let value = split.next().unwrap_or("");
        if name == "key" {
            if let Some(rest) = value.strip_prefix("eq.") {
                key_eq = Some(url_decode(rest));
            }
        }
        if name.is_empty()
            || name == "select"
            || name == "limit"
            || name == "offset"
            || name == "order"
        {
            continue;
        }
    }
    match key_eq {
        Some(want) => {
            rows.into_iter().filter(|(pk, _)| String::from_utf8_lossy(pk) == want).collect()
        }
        None => rows,
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
    match order {
        Some(spec) if spec.starts_with("key.desc") => {
            rows.sort_by(|a, b| b.0.cmp(&a.0));
        }
        _ => {
            rows.sort_by(|a, b| a.0.cmp(&b.0));
        }
    }
    rows
}

async fn rest_insert(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(table): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let body: RestWriteBody = match json_body(&body) {
        Ok(body) => body,
        Err(e) => return error_response(e),
    };
    let principal = match state.principal(&headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if !principal.can_write() {
        return error_response(ryme_error::RymeError::Forbidden);
    }
    let rows = rest_body_rows(&body);
    if rows.is_empty() {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from("rows")));
    }
    let mut inserted = Vec::new();
    for (key, value) in rows {
        if let Err(e) = admit_write(&state, &principal.tenant, (key.len() + value.len()) as u64) {
            return error_response(e);
        }
        match state.gateway.put(&principal, &table, key.clone(), value.clone()).await {
            Ok(commit) => {
                record_meter(&state, Metric::WriteUnit, 1);
                inserted.push(rest_row_to_json(&key, &value));
                let _ = commit;
            }
            Err(e) => return error_response(e),
        }
    }
    (StatusCode::CREATED, Json(inserted)).into_response()
}

async fn rest_upsert(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(table): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    rest_insert(State(state), headers, Path(table), body).await
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
    let raw = raw.unwrap_or_default();
    let mut target: Option<String> = None;
    for pair in raw.split('&') {
        let mut split = pair.splitn(2, '=');
        if split.next().unwrap_or("") == "key" {
            if let Some(rest) = split.next().unwrap_or("").strip_prefix("eq.") {
                target = Some(url_decode(rest));
            }
        }
    }
    let Some(key) = target else {
        return error_response(ryme_error::RymeError::InvalidArgument(String::from(
            "key=eq.<id> required",
        )));
    };
    if let Err(e) = admit_write(&state, &principal.tenant, key.len() as u64) {
        return error_response(e);
    }
    match state.gateway.delete(&principal, &table, key.into_bytes()).await {
        Ok(commit) => {
            record_meter(&state, Metric::WriteUnit, 1);
            (StatusCode::OK, Json(serde_json::json!({ "commit": commit }))).into_response()
        }
        Err(e) => error_response(e),
    }
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
        _ => Vec::new(),
    }
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
    match execute_graphql(&state, &principal, &body.query).await {
        Ok(value) => (StatusCode::OK, Json(serde_json::json!({ "data": value }))).into_response(),
        Err(e) => error_response(e),
    }
}

async fn execute_graphql(
    state: &SharedState,
    principal: &Principal,
    query: &str,
) -> ryme_error::Result<serde_json::Value> {
    let table = parse_graphql_table(query).ok_or_else(|| {
        ryme_error::RymeError::InvalidArgument(String::from("table(key:) required"))
    })?;
    if let Some(key) = parse_graphql_key(query) {
        match state.gateway.get(principal, &table, key.as_bytes())? {
            Some(value) => {
                admit_egress(state, &principal.tenant, value.len() as u64)?;
                let masked = state.gateway.masked(&table, value);
                Ok(serde_json::json!({ table: rest_row_to_json(key.as_bytes(), &masked) }))
            }
            None => Ok(serde_json::json!({ table: serde_json::Value::Null })),
        }
    } else {
        let limit = parse_graphql_limit(query).unwrap_or(100).min(1000);
        let rows = state.gateway.scan(principal, &table, limit)?;
        let egress: u64 = rows.iter().map(|(pk, value)| (pk.len() + value.len()) as u64).sum();
        admit_egress(state, &principal.tenant, egress)?;
        let items: Vec<serde_json::Value> = rows
            .iter()
            .map(|(pk, value)| rest_row_to_json(pk, &state.gateway.masked(&table, value.clone())))
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
    match state.executor.bulk_upsert(request.table.clone(), rows.clone()).await {
        Ok(count) => {
            record_meter(&state, Metric::WriteUnit, count as u64);
            note_range_write(&state, request.table.as_bytes(), count as u64);
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
    match state.executor.explain(&sql) {
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
    let author = request.author.unwrap_or_else(|| principal.id.clone());
    let duplicate = match state.control.lock() {
        Ok(control) => {
            control.migrations.entries().iter().any(|entry| entry.migration_id == request.id)
        }
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    if duplicate {
        return error_response(ryme_error::RymeError::Conflict(String::from("migration")));
    }
    if let Err(e) = state.executor.execute(statement).await {
        return error_response(e);
    }
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.migrations.apply(request.id, &request.sql, author, now_secs()) {
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
    pub id: String,
    pub password: String,
    pub code: Option<String>,
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
        Ok(Ok(())) => {
            (StatusCode::CREATED, Json(serde_json::json!({ "ok": true }))).into_response()
        }
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
    let now = ryme_txn::now_unix();
    let issued = tokio::task::spawn_blocking(move || {
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
        Ok(issue_api_key(&keys, &principal))
    })
    .await;
    match issued {
        Ok(Ok((key, tenant))) => {
            (StatusCode::CREATED, Json(serde_json::json!({ "key": key, "tenant": tenant })))
                .into_response()
        }
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
        (StatusCode::OK, Json(serde_json::json!({ "revoked": true }))).into_response()
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
    match state.credentials.lock() {
        Ok(mut store) => match store.set_otp_secret(&request.id, secret) {
            Ok(()) => {
                (StatusCode::OK, Json(serde_json::json!({ "secret": encoded }))).into_response()
            }
            Err(e) => error_response(e),
        },
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
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
    match state.passkeys.lock() {
        Ok(mut registry) => {
            match registry.register(&request.user, request.credential_id, &public_key) {
                Ok(()) => {
                    (StatusCode::CREATED, Json(serde_json::json!({ "ok": true }))).into_response()
                }
                Err(e) => error_response(e),
            }
        }
        Err(_) => error_response(ryme_error::RymeError::Internal(String::from("lock"))),
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
    (StatusCode::CREATED, Json(serde_json::json!({ "key": key, "tenant": tenant }))).into_response()
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
    match config.verify_id_token(&request.id_token, now) {
        Ok(principal) => {
            let (key, tenant) = issue_api_key(&state.keys, &principal);
            (StatusCode::CREATED, Json(serde_json::json!({ "key": key, "tenant": tenant })))
                .into_response()
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
    match state.realtime.presence_join(
        &principal.tenant,
        &request.channel,
        request.member,
        request.state.unwrap_or(serde_json::Value::Null),
        request.ttl_secs.unwrap_or(60),
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
    if let Err(e) = admit_realtime(&state, &principal.tenant, 1) {
        return error_response(e);
    }
    let commit = state.backend.latest_commit();
    match state.realtime.broadcast(
        &principal.tenant,
        &request.channel,
        request.from.unwrap_or_else(|| principal.id.clone()),
        request.payload,
        commit,
    ) {
        Ok(sequence) => {
            (StatusCode::OK, Json(serde_json::json!({ "sequence": sequence }))).into_response()
        }
        Err(e) => error_response(e),
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
    let realtime = state.realtime.clone();
    let tenant = principal.tenant.clone();
    let qos = state.qos.clone();
    upgrade.on_upgrade(move |socket| async move {
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
    let (mut sender, mut incoming) = socket.split();
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !send_realtime_message(
                    &mut sender,
                    axum::extract::ws::Message::Ping(Vec::new()),
                )
                .await {
                    break;
                }
            }
            message = receiver.recv() => {
                match message {
                    Ok(record) => {
                        let text = serde_json::to_string(&record).unwrap_or_else(|_| String::from("{}"));
                        if !stream_egress(&qos, tenant, text.len() as u64) {
                            break;
                        }
                        if !send_realtime_message(
                            &mut sender,
                            axum::extract::ws::Message::Text(text),
                        )
                        .await {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
            next = incoming.next() => {
                match next {
                    Some(Ok(axum::extract::ws::Message::Close(_))) | None => break,
                    Some(Ok(axum::extract::ws::Message::Ping(payload))) => {
                        if !send_realtime_message(
                            &mut sender,
                            axum::extract::ws::Message::Pong(payload),
                        )
                        .await {
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
    if request.partition.is_empty() {
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
    let commit = state.backend.latest_commit();
    match state.realtime.durable_append(
        &principal.tenant,
        &request.partition,
        request.key.into_bytes(),
        request.value.into_bytes(),
        commit,
        request.retention.unwrap_or(1024),
    ) {
        Ok(cursor) => {
            (StatusCode::OK, Json(serde_json::json!({ "cursor": cursor }))).into_response()
        }
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
    let result = inner_kv_get(&state, &headers, &table, key.as_bytes());
    let micros = elapsed_micros(start);
    state.latency.observe_micros(micros);
    state.histogram.record(micros);
    record_meter(&state, Metric::ReadUnit, 1);
    record_span(&state, "kv_get", &[("table", table.as_str())], micros);
    result
}

fn inner_kv_get(state: &SharedState, headers: &HeaderMap, table: &str, key: &[u8]) -> Response {
    let principal = match state.principal(headers) {
        Ok(principal) => principal,
        Err(e) => return error_response(e),
    };
    if let Err(e) = admit_read(state, &principal.tenant) {
        return error_response(e);
    }
    match state.gateway.get(&principal, table, key) {
        Ok(Some(value)) => {
            if let Err(e) = admit_egress(state, &principal.tenant, value.len() as u64) {
                return error_response(e);
            }
            let masked = state.gateway.masked(table, value);
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
    if let Err(e) = admit_write(state, &principal.tenant, value.len() as u64) {
        return error_response(e);
    }
    match state.gateway.put_with_ttl(&principal, table, key, value, expires_at).await {
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
    match state.gateway.ttl_of(&principal, &table, key.as_bytes()) {
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
    let key_bytes = key.into_bytes();
    if let Err(e) = admit_write(&state, &principal.tenant, key_bytes.len() as u64) {
        return error_response(e);
    }
    let result = match state.gateway.delete(&principal, &table, key_bytes.clone()).await {
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
    let result = match state.executor.execute(statement).await {
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
        note_range_write(&state, table_name.as_bytes(), 1);
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
    match state.gateway.scan(&principal, &table, limit) {
        Ok(rows) => {
            let egress: u64 = rows.iter().map(|(pk, value)| (pk.len() + value.len()) as u64).sum();
            if let Err(e) = admit_egress(&state, &principal.tenant, egress) {
                return error_response(e);
            }
            let items: Vec<serde_json::Value> = rows
                .into_iter()
                .map(|(pk, value)| {
                    let masked = state.gateway.masked(&table, value);
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
    match control.create_branch(request.id, &request.parent, request.base_commit_ts) {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(e) => error_response(e),
    }
}

async fn branch_get(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.branches.get(&id) {
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
    match control.delete_branch(&id) {
        Ok(garbage) => {
            (StatusCode::OK, Json(serde_json::json!({ "garbage": garbage }))).into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn branch_list(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    (StatusCode::OK, Json(control.list_branches())).into_response()
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
    match control.reset_branch(&id, request.base_commit_ts) {
        Ok(branch) => (StatusCode::OK, Json(branch)).into_response(),
        Err(e) => error_response(e),
    }
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
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.promote_branch(&id) {
        Ok(branch) => (StatusCode::OK, Json(branch)).into_response(),
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BranchDiffQuery {
    pub against: String,
}

async fn branch_diff(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<BranchDiffQuery>,
) -> Response {
    if state.principal(&headers).is_err() {
        return error_response(ryme_error::RymeError::Unauthorized);
    }
    let control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.diff_branches(&id, &query.against) {
        Ok((only_left, only_right)) => (
            StatusCode::OK,
            Json(serde_json::json!({ "only_left": only_left, "only_right": only_right })),
        )
            .into_response(),
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
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    control.backups.record(checkpoint.clone());
    (StatusCode::OK, Json(checkpoint)).into_response()
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
            match state.control.lock() {
                Ok(mut guard) => guard.backups.record(checkpoint),
                Err(_) => {
                    return error_response(ryme_error::RymeError::Internal(String::from("lock")))
                }
            }
            (StatusCode::OK, Json(manifest)).into_response()
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
    match target
        .verify_backup(&manifest.manifest_key(), &|encryption, bytes| {
            open_archive_bytes(state, encryption, bytes)
        })
        .await
    {
        Ok(verified) => {
            tracing::info!(backup = %backup_id, files = verified, "restore drill ok");
            DrillReport { backup_id, verified_files: verified, at_unix: now_secs(), error: None }
        }
        Err(e) => {
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
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.split_range(
        &request.id,
        request.mid.into_bytes(),
        request.left_id.clone(),
        request.right_id.clone(),
        request.expected_epoch,
    ) {
        Ok(()) => {
            let left = control.router_get(&request.left_id);
            let right = control.router_get(&request.right_id);
            match (left, right) {
                (Ok(left), Ok(right)) => (StatusCode::OK, Json(vec![left, right])).into_response(),
                (Err(e), _) | (_, Err(e)) => error_response(e),
            }
        }
        Err(e) => error_response(e),
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
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    match control.merge_ranges(
        &request.left_id,
        &request.right_id,
        request.merged_id.clone(),
        request.expected_left_epoch,
        request.expected_right_epoch,
    ) {
        Ok(()) => match control.router_get(&request.merged_id) {
            Ok(merged) => (StatusCode::OK, Json(merged)).into_response(),
            Err(e) => error_response(e),
        },
        Err(e) => error_response(e),
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
    let mut control = match state.control.lock() {
        Ok(guard) => guard,
        Err(_) => return error_response(ryme_error::RymeError::Internal(String::from("lock"))),
    };
    let threshold = match query.min_writes {
        Some(min) if min > 0 => min,
        None if state.autosplit_writes > 0 => state.autosplit_writes,
        _ => {
            return (StatusCode::OK, Json(serde_json::json!({ "split": [] }))).into_response();
        }
    };
    match control.auto_split_once(threshold) {
        Ok(created) => {
            (StatusCode::OK, Json(serde_json::json!({ "split": created }))).into_response()
        }
        Err(e) => error_response(e),
    }
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
    let realtime = state.realtime.clone();
    let tenant = state.tenant.clone();
    let database = state.database.clone();
    let table = query.table.clone();
    let from = query.from.unwrap_or(u64::MAX);
    let from_sequence = query.from_sequence;
    let qos = state.qos.clone();
    upgrade.on_upgrade(move |socket| async move {
        forward_changes(socket, realtime, qos, &tenant, &database, &table, from, from_sequence)
            .await;
    })
}

fn stream_egress(qos: &Arc<Mutex<QosRegistry>>, tenant: &str, bytes: u64) -> bool {
    match qos.lock() {
        Ok(mut registry) => registry.admit_egress(tenant, bytes, qos_now_nanos()).is_ok(),
        Err(_) => false,
    }
}

const REALTIME_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

async fn send_realtime_message(
    sender: &mut futures_util::stream::SplitSink<
        axum::extract::ws::WebSocket,
        axum::extract::ws::Message,
    >,
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
    tenant: &str,
    database: &str,
    table: &str,
    from: u64,
    from_sequence: Option<u64>,
) {
    let mut receiver = realtime.subscribe(tenant, database, table);
    let replayed = match from_sequence {
        Some(sequence) => realtime.replay_after_sequence(
            tenant,
            database,
            table,
            sequence,
            realtime.history_capacity(),
        ),
        None => realtime.replay(tenant, database, table, from, realtime.history_capacity()),
    };
    let mut seen_sequence = from_sequence.unwrap_or(0);
    let (mut sender, mut incoming) = socket.split();
    for record in &replayed {
        seen_sequence = seen_sequence.max(record.sequence);
        let text = serde_json::to_string(record).unwrap_or_else(|_| String::from("{}"));
        if !stream_egress(&qos, tenant, text.len() as u64) {
            return;
        }
        if !send_realtime_message(&mut sender, axum::extract::ws::Message::Text(text)).await {
            return;
        }
    }
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !send_realtime_message(
                    &mut sender,
                    axum::extract::ws::Message::Ping(Vec::new()),
                )
                .await {
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
                        let text = serde_json::to_string(&record).unwrap_or_else(|_| String::from("{}"));
                        if !stream_egress(&qos, tenant, text.len() as u64) {
                            break;
                        }
                        if !send_realtime_message(
                            &mut sender,
                            axum::extract::ws::Message::Text(text),
                        )
                        .await {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        let recovered = realtime.replay_after_sequence(
                            tenant,
                            database,
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
                            let text = serde_json::to_string(&record)
                                .unwrap_or_else(|_| String::from("{}"));
                            if !stream_egress(&qos, tenant, text.len() as u64) {
                                return;
                            }
                            if !send_realtime_message(
                                &mut sender,
                                axum::extract::ws::Message::Text(text),
                            )
                            .await {
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
                        if !send_realtime_message(
                            &mut sender,
                            axum::extract::ws::Message::Pong(payload),
                        )
                        .await {
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
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let backend = state.backend.clone();
    let realtime = state.realtime.clone();
    let tenant = state.tenant.clone();
    let database = state.database.clone();
    let table = query.table.clone();
    let qos = state.qos.clone();
    upgrade.on_upgrade(move |socket| async move {
        forward_query(socket, backend, realtime, qos, &tenant, &database, &table, limit).await;
    })
}

#[allow(clippy::too_many_arguments)]
async fn forward_query(
    socket: axum::extract::ws::WebSocket,
    backend: Backend,
    realtime: Realtime,
    qos: Arc<Mutex<QosRegistry>>,
    tenant: &str,
    database: &str,
    table: &str,
    limit: usize,
) {
    let mut receiver = realtime.query_subscribe(tenant, database, table, limit);
    let (mut sender, mut incoming) = socket.split();
    let mut snapshot_commit =
        send_query_snapshot(&mut sender, &backend, &qos, tenant, database, table, limit).await;
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !send_realtime_message(
                    &mut sender,
                    axum::extract::ws::Message::Ping(Vec::new()),
                )
                .await {
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
                            "rows": update.rows.iter().map(|row| serde_json::json!({
                                "pk": String::from_utf8_lossy(&row.pk),
                                "value": String::from_utf8_lossy(&row.value),
                            })).collect::<Vec<_>>(),
                            "truncated": update.truncated,
                        }))
                        .unwrap_or_else(|_| String::from("{}"));
                        if !stream_egress(&qos, tenant, text.len() as u64) {
                            break;
                        }
                        if !send_realtime_message(
                            &mut sender,
                            axum::extract::ws::Message::Text(text),
                        )
                        .await {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        snapshot_commit = send_query_snapshot(
                            &mut sender,
                            &backend,
                            &qos,
                            tenant,
                            database,
                            table,
                            limit,
                        )
                        .await;
                    }
                    Err(_) => break,
                }
            }
            next = incoming.next() => {
                match next {
                    Some(Ok(axum::extract::ws::Message::Close(_))) | None => break,
                    Some(Ok(axum::extract::ws::Message::Ping(payload))) => {
                        if !send_realtime_message(
                            &mut sender,
                            axum::extract::ws::Message::Pong(payload),
                        )
                        .await {
                            break;
                        }
                    }
                    _ => continue,
                }
            }
        }
    }
}

async fn send_query_snapshot(
    sender: &mut futures_util::stream::SplitSink<
        axum::extract::ws::WebSocket,
        axum::extract::ws::Message,
    >,
    backend: &Backend,
    qos: &Arc<Mutex<QosRegistry>>,
    tenant: &str,
    database: &str,
    table: &str,
    limit: usize,
) -> u64 {
    use ryme_txn::TxnBackend;
    let mut txn = backend.begin();
    let rows = backend.scan(&mut txn, tenant, database, table, limit).unwrap_or_default();
    let commit = backend.latest_commit();
    let snapshot = serde_json::json!({
        "type": "snapshot",
        "commit": commit,
        "rows": rows
            .into_iter()
            .map(|(pk, value)| serde_json::json!({
                "pk": String::from_utf8_lossy(&pk),
                "value": String::from_utf8_lossy(&value),
            }))
            .collect::<Vec<_>>(),
    });
    let text = snapshot.to_string();
    if !stream_egress(qos, tenant, text.len() as u64) {
        return commit;
    }
    let _ = send_realtime_message(sender, axum::extract::ws::Message::Text(text)).await;
    commit
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
