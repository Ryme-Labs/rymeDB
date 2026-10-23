use crate::{decode_apply, encode_applied, ApplyPayload, LogEntry, Role};
use ryme_error::{Result, RymeError};
use ryme_storage::{Engine, RecordKey};
use ryme_txn::{decode_writes, encode_writes, now_unix, TxnManager};
use ryme_wal::{Wal, WalRecord};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};

// The heartbeat loop runs every 60 ms. Keep several heartbeat intervals in
// the election window so TLS handshakes, disk syncs, and executor contention
// cannot turn a healthy leader into a needless election under load.
const ELECTION_TIMEOUT_MIN_MS: u64 = 500;
const ELECTION_TIMEOUT_JITTER_MS: u64 = 400;
const DEFAULT_MAX_CONNECTIONS: usize = 1024;
const MAX_RAFT_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_RPC_FRAME_BYTES: usize = 16 * 1024 * 1024;
const APPEND_RPC_BASE_ESTIMATE_BYTES: usize = 256;
const APPEND_RPC_ENTRY_OVERHEAD_ESTIMATE_BYTES: usize = 96;
const RPC_READ_FRAME_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const RPC_WRITE_FRAME_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const RPC_FRAME_BUDGET_UNIT_BYTES: usize = 16 * 1024;
const RPC_FRAME_DECODE_WEIGHT: usize = 4;
const RPC_FRAME_WRITE_WEIGHT: usize = 1;
const RPC_READ_FRAME_BUDGET_PERMITS: usize =
    RPC_READ_FRAME_BUDGET_BYTES / RPC_FRAME_BUDGET_UNIT_BYTES;
const RPC_WRITE_FRAME_BUDGET_PERMITS: usize =
    RPC_WRITE_FRAME_BUDGET_BYTES / RPC_FRAME_BUDGET_UNIT_BYTES;
static NEXT_METADATA_TEMP_ID: AtomicU64 = AtomicU64::new(0);
static RPC_READ_FRAME_BUDGET: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(RPC_READ_FRAME_BUDGET_PERMITS));
static RPC_WRITE_FRAME_BUDGET: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(RPC_WRITE_FRAME_BUDGET_PERMITS));

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<()> {
    let parent =
        path.parent().ok_or_else(|| RymeError::InvalidArgument(String::from("metadata path")))?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn persist_metadata_file(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;

    let (temporary, mut file) = loop {
        let id = NEXT_METADATA_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let temporary = path.with_extension(format!("tmp-{}-{id}", std::process::id()));
        match std::fs::OpenOptions::new().write(true).create_new(true).open(&temporary) {
            Ok(file) => break (temporary, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    };
    let result = file.write_all(bytes).and_then(|()| file.sync_data());
    drop(file);
    if let Err(error) = result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    if let Err(error) = std::fs::rename(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    sync_parent_directory(path)
}

type RangeReadValues = Vec<(Option<Vec<u8>>, Option<u64>)>;

struct RpcFrameBuffer<'a> {
    bytes: Vec<u8>,
    exceeded_limit: bool,
    exhausted_budget: bool,
    budget: &'a Semaphore,
    permits: Vec<tokio::sync::SemaphorePermit<'a>>,
    reserved_units: usize,
}

struct BudgetedRpc<'a> {
    rpc: Rpc,
    _permit: tokio::sync::SemaphorePermit<'a>,
}

impl<'a> RpcFrameBuffer<'a> {
    fn new(budget: &'a Semaphore) -> Self {
        Self {
            bytes: Vec::new(),
            exceeded_limit: false,
            exhausted_budget: false,
            budget,
            permits: Vec::new(),
            reserved_units: 0,
        }
    }
}

impl std::io::Write for RpcFrameBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(required) = self.bytes.len().checked_add(bytes.len()) else {
            self.exceeded_limit = true;
            return Err(std::io::Error::other("rpc frame limit"));
        };
        if required > MAX_RPC_FRAME_BYTES {
            self.exceeded_limit = true;
            return Err(std::io::Error::other("rpc frame limit"));
        }
        if required > self.bytes.capacity() {
            let doubled = self.bytes.capacity().saturating_mul(2).max(8 * 1024);
            let capacity = required.max(doubled).min(MAX_RPC_FRAME_BYTES);
            let reserved_units = capacity.div_ceil(RPC_FRAME_BUDGET_UNIT_BYTES);
            let additional_units = reserved_units.saturating_sub(self.reserved_units);
            let permit = if additional_units == 0 {
                None
            } else {
                match self.budget.try_acquire_many(
                    additional_units.saturating_mul(RPC_FRAME_WRITE_WEIGHT) as u32,
                ) {
                    Ok(permit) => Some(permit),
                    Err(_) => {
                        self.exhausted_budget = true;
                        return Err(std::io::Error::other("rpc frame budget"));
                    }
                }
            };
            self.bytes
                .try_reserve_exact(capacity.saturating_sub(self.bytes.len()))
                .map_err(std::io::Error::other)?;
            if let Some(permit) = permit {
                self.permits.push(permit);
            }
            self.reserved_units = reserved_units;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Rpc {
    VoteRequest {
        term: u64,
        candidate: usize,
        last_term: u64,
        last_index: u64,
    },
    VoteResponse {
        term: u64,
        granted: bool,
    },
    PreVoteRequest {
        term: u64,
        candidate: usize,
        last_term: u64,
        last_index: u64,
    },
    PreVoteResponse {
        term: u64,
        granted: bool,
    },
    TransferRequest {
        term: u64,
        from: usize,
    },
    TransferResponse {
        term: u64,
        accepted: bool,
    },
    Realtime {
        payload: Vec<u8>,
    },
    RealtimeResponse {
        ok: bool,
    },
    Presence {
        payload: Vec<u8>,
    },
    PresenceResponse {
        ok: bool,
    },
    RangeSnapshotRequest {
        start: Vec<u8>,
        end: Vec<u8>,
        #[serde(default)]
        routing_version: u8,
        read_ts: u64,
        max_rows: u32,
        #[serde(default)]
        tenant: Option<String>,
        #[serde(default)]
        database: Option<String>,
    },
    RangeSnapshotResponse {
        snapshot: RangeSnapshot,
    },
    RangeInstallRequest {
        snapshot: RangeSnapshot,
    },
    RangeInstallResponse {
        rows: u64,
    },
    RangePurgeRequest {
        group_id: String,
        expected_epoch: u64,
        snapshot: RangeSnapshot,
    },
    RangePurgeResponse {
        rows: u64,
    },
    RangeReadRequest {
        key: RecordKey,
        read_ts: u64,
    },
    RangeReadResponse {
        value: Option<Vec<u8>>,
        expires_at: Option<u64>,
        #[serde(default)]
        read_ts: u64,
    },
    RangeReadBatchRequest {
        keys: Vec<RecordKey>,
        read_ts: u64,
    },
    RangeReadBatchResponse {
        values: Vec<(Option<Vec<u8>>, Option<u64>)>,
    },
    ProposeWrite {
        payload: Vec<u8>,
        route_generation: Option<u64>,
    },
    ProposeWriteResponse {
        commit_ts: Option<u64>,
        error: Option<RpcError>,
    },
    ProposeTransaction {
        payload: Vec<u8>,
    },
    ProposeTransactionResponse {
        commit_ts: Option<u64>,
        error: Option<RpcError>,
    },
    PrepareTransaction {
        transaction_id: u128,
        commit_ts: u64,
        payload: Vec<u8>,
    },
    PrepareTransactionResponse {
        prepared: bool,
        error: Option<RpcError>,
    },
    ResolveTransaction {
        transaction_id: u128,
        commit_ts: u64,
        committed: bool,
    },
    ResolveTransactionResponse {
        resolved: bool,
        error: Option<RpcError>,
    },
    RangePrepareTransaction {
        group_id: String,
        transaction_id: u128,
        commit_ts: u64,
        payload: Vec<u8>,
    },
    RangePrepareTransactionResponse {
        prepared: bool,
        error: Option<RpcError>,
    },
    RangeResolveTransaction {
        group_id: String,
        transaction_id: u128,
        commit_ts: u64,
        committed: bool,
    },
    RangeResolveTransactionResponse {
        resolved: bool,
        error: Option<RpcError>,
    },
    RangeDecisionTransaction {
        group_id: String,
        transaction_id: u128,
        commit_ts: u64,
        committed: bool,
        participants: Vec<ResolutionParticipant>,
    },
    RangeDecisionTransactionResponse {
        recorded: bool,
        error: Option<RpcError>,
    },
    RangeCoordinateTransaction {
        group_id: String,
        transaction_id: u128,
        commit_ts: u64,
        participants: Vec<ResolutionParticipant>,
    },
    RangeCoordinateTransactionResponse {
        recorded: bool,
        error: Option<RpcError>,
    },
    RangeVoteRequest {
        group_id: String,
        term: u64,
        candidate: usize,
        last_term: u64,
        last_index: u64,
    },
    RangeVoteResponse {
        term: u64,
        granted: bool,
    },
    RangeCommit {
        group_id: String,
        commit_ts: u64,
        writes: Vec<u8>,
    },
    RangeCommitResponse {
        committed: bool,
        error: Option<RpcError>,
    },
    RangeProposeWrite {
        group_id: String,
        payload: Vec<u8>,
        #[serde(default)]
        route_generation: Option<u64>,
    },
    RangeProposeWriteResponse {
        commit_ts: Option<u64>,
        error: Option<RpcError>,
    },
    RangeAppend {
        group_id: String,
        index: u64,
        term: u64,
        leader: usize,
        prev_index: u64,
        prev_term: u64,
        leader_commit: u64,
        commit_ts: u64,
        writes: Vec<u8>,
        #[serde(default)]
        publish_data: bool,
        #[serde(default)]
        kind: RangeEntryKind,
    },
    RangeAppendResponse {
        term: u64,
        appended: bool,
        last_index: u64,
        #[serde(default)]
        conflict_index: Option<u64>,
        error: Option<RpcError>,
    },
    AppendRequest {
        term: u64,
        leader: usize,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendResponse {
        term: u64,
        ok: bool,
        match_index: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RpcError {
    kind: String,
    message: Option<String>,
}

impl From<RymeError> for RpcError {
    fn from(error: RymeError) -> Self {
        let (kind, message) = match error {
            RymeError::InvalidArgument(message) => ("invalid_argument", Some(message)),
            RymeError::NotFound(message) => ("not_found", Some(message)),
            RymeError::Conflict(message) => ("conflict", Some(message)),
            RymeError::Unauthorized => ("unauthorized", None),
            RymeError::Forbidden => ("forbidden", None),
            RymeError::Overload(message) => ("overload", Some(message)),
            RymeError::Io(message) => ("io", Some(message)),
            RymeError::Corrupt(message) => ("corrupt", Some(message)),
            RymeError::Unavailable(message) => ("unavailable", Some(message)),
            RymeError::ReadOnly(message) => ("read_only", Some(message)),
            RymeError::Timeout => ("timeout", None),
            RymeError::Internal(message) => ("internal", Some(message)),
        };
        Self { kind: kind.to_string(), message }
    }
}

impl From<RpcError> for RymeError {
    fn from(error: RpcError) -> Self {
        let message = error.message.unwrap_or_default();
        match error.kind.as_str() {
            "invalid_argument" => Self::InvalidArgument(message),
            "not_found" => Self::NotFound(message),
            "conflict" => Self::Conflict(message),
            "unauthorized" => Self::Unauthorized,
            "forbidden" => Self::Forbidden,
            "overload" => Self::Overload(message),
            "io" => Self::Io(message),
            "corrupt" => Self::Corrupt(message),
            "unavailable" => Self::Unavailable(message),
            "read_only" => Self::ReadOnly(message),
            "timeout" => Self::Timeout,
            "internal" => Self::Internal(message),
            _ => Self::Internal(format!("remote rpc error: {}", error.kind)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeSnapshotRow {
    pub tenant: String,
    pub database: String,
    pub table: String,
    pub pk: Vec<u8>,
    pub value: Vec<u8>,
    pub expires_at: u64,
    pub versions: Vec<RangeSnapshotVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeSnapshotVersion {
    pub commit_ts: u64,
    pub value: Option<Vec<u8>>,
    pub expires_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeSnapshot {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    #[serde(default)]
    pub routing_version: u8,
    pub snapshot_ts: u64,
    pub applied_commit: u64,
    pub rows: Vec<RangeSnapshotRow>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeOwner {
    #[serde(default)]
    pub id: String,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    #[serde(default)]
    pub routing_version: u8,
    pub owner: usize,
    #[serde(default)]
    pub replicas: Vec<usize>,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum IntentRecord {
    Prepare { transaction_id: u128, commit_ts: u64, state: ryme_txn::TransactionState },
    Resolve { transaction_id: u128, commit_ts: u64, committed: bool },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RangeEntryKind {
    #[default]
    Data,
    Prepare {
        transaction_id: u128,
        state: Vec<u8>,
    },
    Resolve {
        transaction_id: u128,
        committed: bool,
    },
    Decision {
        transaction_id: u128,
        committed: bool,
        participants: Vec<ResolutionParticipant>,
    },
    Coordinate {
        transaction_id: u128,
        participants: Vec<ResolutionParticipant>,
    },
}

#[derive(Debug, Clone)]
struct PreparedIntent {
    commit_ts: u64,
    state: ryme_txn::TransactionState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ResolutionRecord {
    Pending {
        transaction_id: u128,
        commit_ts: u64,
        participant: usize,
        committed: bool,
        #[serde(default)]
        group_id: String,
    },
    Batch {
        transaction_id: u128,
        commit_ts: u64,
        committed: bool,
        participants: Vec<ResolutionParticipant>,
    },
    Complete {
        transaction_id: u128,
        commit_ts: u64,
        participant: usize,
        #[serde(default)]
        group_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResolutionParticipant {
    participant: usize,
    group_id: String,
}

#[derive(Debug, Clone)]
struct PendingResolution {
    transaction_id: u128,
    commit_ts: u64,
    participant: usize,
    committed: bool,
    group_id: String,
}

#[derive(Debug, Clone)]
struct PendingRangeDecision {
    group_id: String,
    transaction_id: u128,
    commit_ts: u64,
    committed: bool,
    participants: Vec<ResolutionParticipant>,
}

#[derive(Debug, Clone)]
struct PendingRangeCoordination {
    group_id: String,
    transaction_id: u128,
    commit_ts: u64,
    participants: Vec<ResolutionParticipant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RangeLogRecord {
    group_id: String,
    index: u64,
    term: u64,
    commit_ts: u64,
    writes: Vec<u8>,
    #[serde(default)]
    publish_data: bool,
    #[serde(default)]
    kind: RangeEntryKind,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct RangeGroupState {
    term: u64,
    voted_for: Option<usize>,
    leader: Option<usize>,
    commit_index: u64,
    applied_index: u64,
    last_contact_ms: u64,
    last_heartbeat_ms: u64,
}

fn derived_range_id(start: &[u8], end: &[u8], epoch: u64) -> String {
    let mut hash = 0xcbf29ce484222325u64 ^ epoch;
    for byte in start.iter().chain([0xff].iter()).chain(end.iter()) {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("range-{hash:016x}")
}

fn read_bounded_metadata(path: &std::path::Path, label: &str) -> Result<Option<Vec<u8>>> {
    use std::io::Read;

    let path_metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !path_metadata.file_type().is_file() {
        return Err(RymeError::Corrupt(format!("{label} file type")));
    }
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(RymeError::Corrupt(format!("{label} file type")));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if path_metadata.dev() != metadata.dev() || path_metadata.ino() != metadata.ino() {
            return Err(RymeError::Corrupt(format!("{label} file identity")));
        }
    }
    let file_len = metadata.len();
    if file_len > MAX_RAFT_METADATA_BYTES as u64 {
        return Err(RymeError::Overload(label.to_string()));
    }
    let capacity = usize::try_from(file_len).map_err(|_| RymeError::Overload(label.to_string()))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take((MAX_RAFT_METADATA_BYTES as u64).saturating_add(1)).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_RAFT_METADATA_BYTES {
        return Err(RymeError::Overload(label.to_string()));
    }
    Ok(Some(bytes))
}

fn snapshot_live_value(row: &RangeSnapshotRow, snapshot_ts: u64) -> Option<Vec<u8>> {
    let mut value = None;
    for version in &row.versions {
        if version.commit_ts > snapshot_ts {
            continue;
        }
        value = if version.value.is_some()
            && (version.expires_at == 0 || version.expires_at > now_unix())
        {
            version.value.clone()
        } else {
            None
        };
    }
    value
}

async fn write_frame<S>(socket: &mut S, rpc: &Rpc) -> Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    let mut frame = RpcFrameBuffer::new(&RPC_WRITE_FRAME_BUDGET);
    if let Err(error) = serde_json::to_writer(&mut frame, rpc) {
        if frame.exceeded_limit {
            return Err(RymeError::Overload(String::from("rpc frame")));
        }
        if frame.exhausted_budget {
            return Err(RymeError::Overload(String::from("rpc frame budget")));
        }
        return Err(RymeError::Internal(error.to_string()));
    }
    let body = frame.bytes;
    if body.is_empty() {
        return Err(RymeError::Overload(String::from("rpc frame")));
    }
    socket
        .write_all(&(body.len() as u32).to_be_bytes())
        .await
        .map_err(|e| RymeError::Io(e.to_string()))?;
    socket.write_all(&body).await.map_err(|e| RymeError::Io(e.to_string()))?;
    Ok(())
}

async fn read_frame<S>(socket: &mut S) -> Result<Rpc>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let frame = read_frame_with_budget(socket, &RPC_READ_FRAME_BUDGET).await?;
    Ok(frame.rpc)
}

async fn read_frame_with_budget<'a, S>(
    socket: &mut S,
    budget: &'a Semaphore,
) -> Result<BudgetedRpc<'a>>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await.map_err(|e| RymeError::Io(e.to_string()))?;
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 || length > MAX_RPC_FRAME_BYTES {
        return Err(RymeError::InvalidArgument(String::from("rpc frame")));
    }
    let permits =
        length.div_ceil(RPC_FRAME_BUDGET_UNIT_BYTES).saturating_mul(RPC_FRAME_DECODE_WEIGHT);
    let permit = budget
        .acquire_many(permits as u32)
        .await
        .map_err(|_| RymeError::Unavailable(String::from("rpc frame budget")))?;
    let mut body = vec![0u8; length];
    socket.read_exact(&mut body).await.map_err(|e| RymeError::Io(e.to_string()))?;
    let rpc = serde_json::from_slice(&body)
        .map_err(|_| RymeError::Corrupt(String::from("rpc decode")))?;
    Ok(BudgetedRpc { rpc, _permit: permit })
}

#[derive(Debug)]
pub struct PeerPool {
    addr: String,
    conn: Mutex<Option<MeshStream>>,
    dials: std::sync::atomic::AtomicU64,
    isolated: std::sync::atomic::AtomicBool,
    tls_required: std::sync::atomic::AtomicBool,
    tls: std::sync::Mutex<Option<ryme_tls::MeshConnector>>,
}

enum MeshStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl std::fmt::Debug for MeshStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => formatter.write_str("Plain"),
            Self::Tls(_) => formatter.write_str("Tls"),
        }
    }
}

impl PeerPool {
    pub fn new(addr: String) -> Arc<Self> {
        Arc::new(Self {
            addr,
            conn: Mutex::new(None),
            dials: std::sync::atomic::AtomicU64::new(0),
            isolated: std::sync::atomic::AtomicBool::new(false),
            tls_required: std::sync::atomic::AtomicBool::new(false),
            tls: std::sync::Mutex::new(None),
        })
    }

    pub fn set_connector(&self, connector: Option<ryme_tls::MeshConnector>) {
        if let Err(error) = self.try_set_connector(connector) {
            tracing::error!(%error, "failed to install raft peer TLS connector");
        }
    }

    pub fn try_set_connector(&self, connector: Option<ryme_tls::MeshConnector>) -> Result<()> {
        if connector.is_some() {
            self.tls_required.store(true, Ordering::SeqCst);
        }
        let mut guard = self
            .tls
            .lock()
            .map_err(|_| RymeError::Internal(String::from("raft peer TLS config lock")))?;
        *guard = connector;
        drop(guard);
        self.purge();
        Ok(())
    }

    fn require_tls(&self) {
        self.tls_required.store(true, Ordering::SeqCst);
    }

    pub fn set_isolated(&self, isolated: bool) {
        self.isolated.store(isolated, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn dials(&self) -> u64 {
        self.dials.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn purge(&self) {
        if let Ok(mut guard) = self.conn.try_lock() {
            *guard = None;
        }
    }

    async fn dial(&self) -> Result<MeshStream> {
        let connector = self
            .tls
            .lock()
            .map_err(|_| RymeError::Internal(String::from("raft peer TLS config lock")))?
            .clone();
        if let Some(connector) = connector {
            let name = ryme_tls::server_name_for(&self.addr);
            let stream = tokio::time::timeout(
                Duration::from_millis(800),
                connector.connect(&self.addr, &name),
            )
            .await
            .map_err(|_| RymeError::Timeout)??;
            return Ok(MeshStream::Tls(Box::new(stream)));
        }
        if self.tls_required.load(Ordering::SeqCst) {
            return Err(RymeError::Unavailable(String::from(
                "raft peer TLS connector unavailable",
            )));
        }
        let socket = tokio::time::timeout(
            Duration::from_millis(800),
            TcpStream::connect(self.addr.as_str()),
        )
        .await
        .map_err(|_| RymeError::Timeout)?
        .map_err(|e| RymeError::Io(e.to_string()))?;
        let _ = socket.set_nodelay(true);
        Ok(MeshStream::Plain(socket))
    }

    pub(crate) async fn roundtrip(&self, rpc: &Rpc) -> Result<Rpc> {
        if self.isolated.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(RymeError::Unavailable(String::from("isolated")));
        }
        let mut guard = self.conn.lock().await;
        if self.tls_required.load(Ordering::SeqCst)
            && matches!(guard.as_ref(), Some(MeshStream::Plain(_)))
        {
            *guard = None;
        }
        if guard.is_none() {
            let stream = self.dial().await?;
            self.dials.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            *guard = Some(stream);
        }
        let socket = guard.as_mut().ok_or_else(|| RymeError::Internal(String::from("pool")))?;
        let result = match socket {
            MeshStream::Plain(stream) => roundtrip_stream(stream, rpc).await,
            MeshStream::Tls(stream) => roundtrip_stream(stream, rpc).await,
        };
        match result {
            Ok(response) => Ok(response),
            Err(e) => {
                *guard = None;
                Err(e)
            }
        }
    }
}

async fn roundtrip_stream<S>(socket: &mut S, rpc: &Rpc) -> Result<Rpc>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(Duration::from_millis(1500), write_frame(socket, rpc))
        .await
        .map_err(|_| RymeError::Timeout)??;
    tokio::time::timeout(Duration::from_millis(1500), read_frame(socket))
        .await
        .map_err(|_| RymeError::Timeout)?
}

#[derive(Debug, Clone)]
pub struct MeshTransport {
    pub acceptor: ryme_tls::TlsAcceptor,
    pub connector: ryme_tls::MeshConnector,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Member {
    pub id: usize,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfChange {
    pub old: Vec<Member>,
    pub new: Option<Vec<Member>>,
}

impl ConfChange {
    pub fn single(members: Vec<Member>) -> Self {
        Self { old: members, new: None }
    }

    pub fn joint(old: Vec<Member>, new: Vec<Member>) -> Self {
        Self { old, new: Some(new) }
    }

    pub fn effective(&self) -> &Vec<Member> {
        self.new.as_ref().unwrap_or(&self.old)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Meta {
    term: u64,
    voted_for: Option<usize>,
    commit_index: u64,
    #[serde(default)]
    members: Vec<Member>,
    #[serde(default)]
    joint: Option<Vec<Member>>,
}

fn normalize_members(members: Vec<Member>) -> Vec<Member> {
    let mut members = members;
    members.sort_by_key(|member| member.id);
    members.dedup_by_key(|member| member.id);
    members
}

fn majority(total: usize) -> usize {
    total / 2 + 1
}

fn committed_in(acks: &std::collections::BTreeMap<usize, u64>, members: &[Member]) -> u64 {
    if members.is_empty() {
        return u64::MAX;
    }
    let mut matched: Vec<u64> =
        members.iter().map(|member| acks.get(&member.id).copied().unwrap_or(0)).collect();
    matched.sort_unstable();
    matched[matched.len() - majority(members.len())]
}

fn votes_win(voters: &[usize], members: &[Member]) -> bool {
    if members.is_empty() {
        return true;
    }
    let have = voters.iter().filter(|id| members.iter().any(|member| member.id == **id)).count();
    have >= majority(members.len())
}

fn encode_log_frame(index: u64, term: u64, apply: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(24 + apply.len());
    out.extend_from_slice(&index.to_be_bytes());
    out.extend_from_slice(&term.to_be_bytes());
    out.extend_from_slice(&(apply.len() as u64).to_be_bytes());
    out.extend_from_slice(apply);
    out
}

fn decode_log_frame(input: &[u8]) -> Result<(u64, u64, Vec<u8>)> {
    if input.len() < 24 {
        return Err(RymeError::Corrupt(String::from("log frame")));
    }
    let index = u64::from_be_bytes(
        input[0..8].try_into().map_err(|_| RymeError::Corrupt(String::from("log frame")))?,
    );
    let term = u64::from_be_bytes(
        input[8..16].try_into().map_err(|_| RymeError::Corrupt(String::from("log frame")))?,
    );
    let length = u64::from_be_bytes(
        input[16..24].try_into().map_err(|_| RymeError::Corrupt(String::from("log frame")))?,
    );
    let available = input.len().saturating_sub(24) as u64;
    if length != available {
        return Err(RymeError::Corrupt(String::from("log frame")));
    }
    Ok((index, term, input[24..].to_vec()))
}

fn append_entry_frame_estimate(entry: &LogEntry) -> Result<usize> {
    let mut payload_bytes = 2usize;
    for (index, byte) in entry.payload.iter().enumerate() {
        let digits = if *byte < 10 {
            1
        } else if *byte < 100 {
            2
        } else {
            3
        };
        payload_bytes = payload_bytes
            .checked_add(digits + usize::from(index > 0))
            .ok_or_else(|| RymeError::Overload(String::from("raft append entry")))?;
    }
    APPEND_RPC_ENTRY_OVERHEAD_ESTIMATE_BYTES
        .checked_add(payload_bytes)
        .ok_or_else(|| RymeError::Overload(String::from("raft append entry")))
}

fn append_batch_count(entries: &[LogEntry], next_index: u64) -> Result<usize> {
    let mut estimated_bytes = APPEND_RPC_BASE_ESTIMATE_BYTES;
    let mut count = 0usize;
    for entry in entries.iter().filter(|entry| entry.index >= next_index) {
        let Some(required_bytes) = estimated_bytes.checked_add(append_entry_frame_estimate(entry)?)
        else {
            return Err(RymeError::Overload(String::from("raft append batch")));
        };
        if required_bytes > MAX_RPC_FRAME_BYTES {
            if count == 0 {
                return Err(RymeError::Overload(String::from("raft append entry")));
            }
            break;
        }
        estimated_bytes = required_bytes;
        count += 1;
    }
    Ok(count)
}

fn validate_append_batch(entries: &[LogEntry]) -> Result<()> {
    if append_batch_count(entries, 0)? == entries.len() {
        Ok(())
    } else {
        Err(RymeError::Overload(String::from("raft append batch")))
    }
}

fn validate_apply_payload(input: &[u8]) -> Result<()> {
    match decode_apply(input)? {
        ApplyPayload::Data { writes, .. } => {
            decode_writes(&writes)?;
        }
        ApplyPayload::Conf { .. } => {}
        ApplyPayload::Metadata { payload } => {
            if payload.is_empty() {
                return Err(RymeError::Corrupt(String::from("metadata")));
            }
        }
        ApplyPayload::Topic { payload } => {
            if payload.is_empty() {
                return Err(RymeError::Corrupt(String::from("topic")));
            }
        }
        ApplyPayload::Resolution { payload } => {
            let _: ResolutionRecord = serde_json::from_slice(&payload)
                .map_err(|_| RymeError::Corrupt(String::from("resolution")))?;
        }
        ApplyPayload::Restore { .. } => {}
    }
    Ok(())
}

#[derive(Debug)]
struct Inner {
    term: u64,
    role: Role,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,
    commit_index: u64,
    applied: u64,
    current: Vec<Member>,
    joint: Option<Vec<Member>>,
    learner: bool,
    acks: std::collections::BTreeMap<usize, u64>,
    next_index: std::collections::BTreeMap<usize, u64>,
    wal: ryme_wal::Wal,
    meta_path: PathBuf,
    reset_epoch: u64,
    observed_epoch: u64,
    campaign_now: bool,
    leader_id: Option<usize>,
}

impl Inner {
    fn is_member(&self, id: usize) -> bool {
        self.current.iter().any(|member| member.id == id)
            || self
                .joint
                .as_ref()
                .map(|joint| joint.iter().any(|member| member.id == id))
                .unwrap_or(false)
    }

    fn advance_commit(&mut self) -> Result<()> {
        let mut target = committed_in(&self.acks, &self.current);
        if let Some(joint) = self.joint.as_ref() {
            target = target.min(committed_in(&self.acks, joint));
        }
        if target <= self.commit_index {
            return Ok(());
        }
        let Some(candidate) = self
            .log
            .iter()
            .rev()
            .find(|entry| {
                entry.index > self.commit_index && entry.index <= target && entry.term == self.term
            })
            .map(|entry| entry.index)
        else {
            return Ok(());
        };
        let previous_commit = self.commit_index;
        self.commit_index = candidate;
        if let Err(error) = self.persist_meta() {
            self.commit_index = previous_commit;
            return Err(error);
        }
        Ok(())
    }

    fn persist_meta(&self) -> Result<()> {
        let meta = Meta {
            term: self.term,
            voted_for: self.voted_for,
            commit_index: self.commit_index,
            members: self.current.clone(),
            joint: self.joint.clone(),
        };
        let raw = serde_json::to_vec(&meta).map_err(|e| RymeError::Internal(e.to_string()))?;
        if raw.len() > MAX_RAFT_METADATA_BYTES {
            return Err(RymeError::Overload(String::from("raft metadata")));
        }
        persist_metadata_file(&self.meta_path, &raw)
    }

    fn persist_entries(&mut self, entries: &[LogEntry]) -> Result<()> {
        for entry in entries {
            let frame = encode_log_frame(entry.index, entry.term, &entry.payload);
            self.wal.append(entry.term, &frame)?;
        }
        self.wal.sync()?;
        Ok(())
    }

    fn replace_log_suffix(&mut self, keep_through: u64, entries: &[LogEntry]) -> Result<()> {
        if self.commit_index > keep_through || self.applied > keep_through {
            return Err(RymeError::Conflict(String::from("committed log suffix")));
        }
        let dir = self
            .meta_path
            .parent()
            .ok_or_else(|| RymeError::Internal(String::from("raft wal path")))?;
        let records = Wal::read_all(dir)?;
        let mut replacement = Vec::with_capacity(records.len() + entries.len());
        for record in records {
            let (index, _, _) = decode_log_frame(&record.payload)?;
            if index <= keep_through {
                replacement.push(record);
            }
        }
        let mut next_lsn = match replacement.last() {
            Some(record) => record
                .lsn
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("raft wal lsn")))?,
            None => 0,
        };
        for entry in entries {
            let lsn = next_lsn;
            next_lsn = next_lsn
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("raft wal lsn")))?;
            replacement.push(WalRecord {
                lsn,
                commit_ts: entry.term,
                payload: encode_log_frame(entry.index, entry.term, &entry.payload),
            });
        }
        self.wal.replace_records(&replacement)
    }

    fn last_position(&self) -> (u64, u64) {
        self.log.last().map(|e| (e.term, e.index)).unwrap_or((0, 0))
    }

    fn last_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }
}

pub type MetadataHook = Arc<dyn Fn(&[u8]) -> Result<()> + Send + Sync>;

#[derive(Default)]
struct MetadataHookSlot(std::sync::Mutex<Option<MetadataHook>>);

impl std::fmt::Debug for MetadataHookSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MetadataHookSlot(..)")
    }
}

#[derive(Debug)]
pub struct Node {
    id: usize,
    pools: std::sync::Mutex<std::collections::HashMap<usize, Arc<PeerPool>>>,
    conn_tasks: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    inner: Arc<Mutex<Inner>>,
    manager: TxnManager,
    write_lock: Arc<Mutex<()>>,
    rng: Arc<Mutex<u64>>,
    isolated: Arc<std::sync::atomic::AtomicBool>,
    mesh_tls: std::sync::Mutex<Option<Arc<MeshTransport>>>,
    metadata: std::sync::Mutex<Option<Vec<u8>>>,
    metadata_hook: MetadataHookSlot,
    data_hook: MetadataHookSlot,
    realtime_hook: MetadataHookSlot,
    presence_hook: MetadataHookSlot,
    topic_hook: MetadataHookSlot,
    range_owners: std::sync::RwLock<Vec<RangeOwner>>,
    range_leaders: std::sync::RwLock<BTreeMap<String, usize>>,
    range_generation: AtomicU64,
    txn_commit_lock: Arc<Mutex<()>>,
    prepared: std::sync::Mutex<BTreeMap<u128, PreparedIntent>>,
    intent_wal: std::sync::Mutex<Wal>,
    pending_resolutions: std::sync::Mutex<BTreeMap<(String, u128, u64, usize), PendingResolution>>,
    resolution_wal: std::sync::Mutex<Wal>,
    range_prepared: std::sync::Mutex<BTreeMap<(String, u128), PreparedIntent>>,
    range_decisions: std::sync::Mutex<BTreeMap<(String, u128, u64), PendingRangeDecision>>,
    range_recovery_coordinations:
        std::sync::Mutex<BTreeMap<(String, u128, u64), PendingRangeCoordination>>,
    range_logs: std::sync::Mutex<BTreeMap<String, u64>>,
    range_entries: std::sync::Mutex<BTreeMap<String, BTreeMap<u64, RangeLogRecord>>>,
    range_groups: std::sync::Mutex<BTreeMap<String, RangeGroupState>>,
    range_meta_persist_lock: std::sync::Mutex<()>,
    range_meta_path: PathBuf,
    range_wal_path: PathBuf,
    range_wal: std::sync::Mutex<Wal>,
}

impl Node {
    pub fn open(
        id: usize,
        peers: Vec<String>,
        peer_ids: Vec<usize>,
        dir: &std::path::Path,
    ) -> Result<Arc<Self>> {
        Self::open_inner(id, peers, peer_ids, dir, false, None)
    }

    pub fn open_learner(
        id: usize,
        peers: Vec<String>,
        peer_ids: Vec<usize>,
        dir: &std::path::Path,
    ) -> Result<Arc<Self>> {
        Self::open_inner(id, peers, peer_ids, dir, true, None)
    }

    pub fn open_with_addr(
        id: usize,
        peers: Vec<String>,
        peer_ids: Vec<usize>,
        dir: &std::path::Path,
        learner: bool,
        self_addr: Option<String>,
    ) -> Result<Arc<Self>> {
        Self::open_inner(id, peers, peer_ids, dir, learner, self_addr)
    }

    fn open_inner(
        id: usize,
        peers: Vec<String>,
        peer_ids: Vec<usize>,
        dir: &std::path::Path,
        learner: bool,
        self_addr: Option<String>,
    ) -> Result<Arc<Self>> {
        std::fs::create_dir_all(dir)?;
        let meta_path = dir.join("raft-meta.json");
        let meta = match read_bounded_metadata(&meta_path, "raft metadata")? {
            Some(raw) => serde_json::from_slice::<Meta>(&raw)
                .map_err(|error| RymeError::Corrupt(format!("raft metadata: {error}")))?,
            None => {
                Meta { term: 0, voted_for: None, commit_index: 0, members: Vec::new(), joint: None }
            }
        };
        let meta_fresh = meta.members.is_empty() && meta.joint.is_none();
        let mut members: Vec<Member> =
            peer_ids.into_iter().zip(peers).map(|(id, addr)| Member { id, addr }).collect();
        if meta.members.is_empty() {
            members.push(Member { id, addr: String::new() });
        } else {
            members = meta.members.clone();
        }
        if let Some(addr) = self_addr {
            if !addr.is_empty() {
                if let Some(self_member) = members.iter_mut().find(|member| member.id == id) {
                    self_member.addr = addr;
                }
            }
        }
        members.sort_by_key(|member| member.id);
        members.dedup_by_key(|member| member.id);
        let mut wal = ryme_wal::Wal::open(dir, 64 * 1024 * 1024)?;
        let _ = &mut wal;
        let records = ryme_wal::Wal::read_all(dir)?;
        let intent_dir = dir.join("txn-intents");
        let intent_wal = Wal::open(&intent_dir, 16 * 1024 * 1024)?;
        let mut prepared = BTreeMap::new();
        for record in Wal::read_all(&intent_dir)? {
            let intent: IntentRecord = serde_json::from_slice(&record.payload)
                .map_err(|error| RymeError::Corrupt(format!("transaction intent: {error}")))?;
            match intent {
                IntentRecord::Prepare { transaction_id, commit_ts, state } => {
                    prepared.insert(transaction_id, PreparedIntent { commit_ts, state });
                }
                IntentRecord::Resolve { transaction_id, .. } => {
                    prepared.remove(&transaction_id);
                }
            }
        }
        let resolution_dir = dir.join("txn-resolutions");
        let resolution_wal = Wal::open(&resolution_dir, 16 * 1024 * 1024)?;
        let mut pending_resolutions = BTreeMap::new();
        for record in Wal::read_all(&resolution_dir)? {
            let resolution: ResolutionRecord = serde_json::from_slice(&record.payload)
                .map_err(|error| RymeError::Corrupt(format!("transaction resolution: {error}")))?;
            match resolution {
                ResolutionRecord::Pending {
                    transaction_id,
                    commit_ts,
                    participant,
                    committed,
                    group_id,
                } => {
                    pending_resolutions.insert(
                        (group_id.clone(), transaction_id, commit_ts, participant),
                        PendingResolution {
                            transaction_id,
                            commit_ts,
                            participant,
                            committed,
                            group_id,
                        },
                    );
                }
                ResolutionRecord::Batch { transaction_id, commit_ts, committed, participants } => {
                    for participant in participants {
                        pending_resolutions.insert(
                            (
                                participant.group_id.clone(),
                                transaction_id,
                                commit_ts,
                                participant.participant,
                            ),
                            PendingResolution {
                                transaction_id,
                                commit_ts,
                                participant: participant.participant,
                                committed,
                                group_id: participant.group_id,
                            },
                        );
                    }
                }
                ResolutionRecord::Complete { transaction_id, commit_ts, participant, group_id } => {
                    pending_resolutions.remove(&(group_id, transaction_id, commit_ts, participant));
                }
            }
        }
        let range_dir = dir.join("range-wal");
        let range_wal = Wal::open(&range_dir, 64 * 1024 * 1024)?;
        let mut range_records = Vec::new();
        for record in Wal::read_all(&range_dir)? {
            let range_record: RangeLogRecord = serde_json::from_slice(&record.payload)
                .map_err(|error| RymeError::Corrupt(format!("range log: {error}")))?;
            if range_record.group_id.is_empty()
                || range_record.index == 0
                || range_record.term == 0
                || range_record.commit_ts != record.commit_ts
            {
                return Err(RymeError::Corrupt(String::from("range log metadata")));
            }
            range_records.push(range_record);
        }
        let range_meta_path = dir.join("range-meta.json");
        let mut range_groups: BTreeMap<String, RangeGroupState> =
            match read_bounded_metadata(&range_meta_path, "range metadata")? {
                Some(raw) => serde_json::from_slice(&raw)
                    .map_err(|error| RymeError::Corrupt(format!("range metadata: {error}")))?,
                None => BTreeMap::new(),
            };
        let manager = TxnManager::new();
        let mut log_by_index = BTreeMap::<u64, LogEntry>::new();
        for record in records {
            let (index, term, apply) = decode_log_frame(&record.payload)?;
            if index == 0 || term == 0 {
                return Err(RymeError::Corrupt(String::from("raft log metadata")));
            }
            validate_apply_payload(&apply)?;
            if let Some(existing) = log_by_index.get(&index) {
                if existing.term != term || existing.payload != apply {
                    return Err(RymeError::Corrupt(String::from("conflicting raft log index")));
                }
                continue;
            }
            log_by_index.insert(index, LogEntry { index, term, payload: apply });
        }
        let mut expected_index = 1u64;
        for index in log_by_index.keys() {
            if *index != expected_index {
                return Err(RymeError::Corrupt(String::from("raft log gap")));
            }
            expected_index = expected_index
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("raft log index overflow")))?;
        }
        if meta.commit_index > log_by_index.keys().next_back().copied().unwrap_or(0) {
            return Err(RymeError::Corrupt(String::from("raft commit index")));
        }
        let log: Vec<LogEntry> = log_by_index.into_values().collect();
        let mut applied = 0u64;
        let mut replayed_commit_ts = BTreeSet::new();
        let mut replayed_data = Vec::new();
        let mut replayed: Option<crate::ConfChange> = None;
        let mut replayed_joint: Option<Vec<Member>> = None;
        let mut metadata = None;
        let mut replayed_resolutions = Vec::new();
        for entry in log.iter().filter(|e| e.index <= meta.commit_index) {
            match decode_apply(&entry.payload)? {
                ApplyPayload::Data { commit_ts, writes } => {
                    let writes = decode_writes(&writes)?;
                    manager.replay_at(commit_ts, &writes)?;
                    replayed_data.push((commit_ts, writes));
                    replayed_commit_ts.insert(commit_ts);
                }
                ApplyPayload::Conf { change } => {
                    replayed = Some(change);
                }
                ApplyPayload::Metadata { payload } => {
                    metadata = Some(payload);
                }
                ApplyPayload::Topic { .. } => {}
                ApplyPayload::Resolution { payload } => {
                    let resolution: ResolutionRecord = serde_json::from_slice(&payload)
                        .map_err(|error| RymeError::Corrupt(format!("resolution: {error}")))?;
                    match resolution {
                        ResolutionRecord::Pending {
                            transaction_id,
                            commit_ts,
                            participant,
                            committed,
                            group_id,
                        } => replayed_resolutions.push(PendingResolution {
                            transaction_id,
                            commit_ts,
                            participant,
                            committed,
                            group_id,
                        }),
                        ResolutionRecord::Batch {
                            transaction_id,
                            commit_ts,
                            committed,
                            participants,
                        } => {
                            replayed_resolutions.extend(participants.into_iter().map(
                                |participant| PendingResolution {
                                    transaction_id,
                                    commit_ts,
                                    participant: participant.participant,
                                    committed,
                                    group_id: participant.group_id,
                                },
                            ));
                        }
                        ResolutionRecord::Complete { .. } => {}
                    }
                }
                ApplyPayload::Restore { target } => {
                    let replay = replayed_data
                        .iter()
                        .filter(|(commit_ts, _)| *commit_ts <= target)
                        .cloned()
                        .collect::<Vec<_>>();
                    manager.restore_with_replay(Engine::default(), &replay)?;
                }
            }
            applied = entry.index;
        }
        let mut range_logs = BTreeMap::new();
        let mut range_entries: BTreeMap<String, BTreeMap<u64, RangeLogRecord>> = BTreeMap::new();
        let mut range_prepared = BTreeMap::new();
        let mut range_decisions = BTreeMap::new();
        let mut range_recovery_coordinations = BTreeMap::new();
        for range_record in range_records {
            let group_id = range_record.group_id.clone();
            let group_entries = range_entries.entry(group_id.clone()).or_default();
            if group_entries.insert(range_record.index, range_record.clone()).is_some() {
                return Err(RymeError::Corrupt(String::from("duplicate range log index")));
            }
            let last = range_logs.entry(group_id.clone()).or_insert(0);
            *last = (*last).max(range_record.index);
            let state = range_groups.entry(group_id).or_default();
            state.term = state.term.max(range_record.term).max(1);
        }
        for (group_id, state) in &range_groups {
            let entries = range_entries.get(group_id);
            let last_index =
                entries.and_then(|entries| entries.keys().next_back().copied()).unwrap_or(0);
            if state.applied_index > state.commit_index || state.commit_index > last_index {
                return Err(RymeError::Corrupt(String::from("range commit index")));
            }
            if let Some(entries) = entries {
                let mut expected_index = 1u64;
                for index in entries.keys() {
                    if *index != expected_index {
                        return Err(RymeError::Corrupt(String::from("range log gap")));
                    }
                    expected_index = expected_index.checked_add(1).ok_or_else(|| {
                        RymeError::Corrupt(String::from("range log index overflow"))
                    })?;
                }
            }
        }
        for (group_id, entries) in &range_entries {
            let state = range_groups.entry(group_id.clone()).or_default();
            if state.commit_index > entries.keys().next_back().copied().unwrap_or(0) {
                return Err(RymeError::Corrupt(String::from("range commit index")));
            }
            for index in 1..=state.commit_index {
                let record = entries
                    .get(&index)
                    .ok_or_else(|| RymeError::Corrupt(String::from("range log gap")))?;
                match &record.kind {
                    RangeEntryKind::Data => {
                        let writes = decode_writes(&record.writes)?;
                        manager.replay_at(record.commit_ts, &writes)?;
                    }
                    RangeEntryKind::Prepare { transaction_id, state } => {
                        let state: ryme_txn::TransactionState = serde_json::from_slice(state)
                            .map_err(|error| {
                                RymeError::Corrupt(format!("range transaction prepare: {error}"))
                            })?;
                        if !replayed_commit_ts.contains(&record.commit_ts) {
                            range_prepared.insert(
                                (group_id.clone(), *transaction_id),
                                PreparedIntent { commit_ts: record.commit_ts, state },
                            );
                        }
                    }
                    RangeEntryKind::Resolve { transaction_id, committed } => {
                        if *committed {
                            if let Some(existing) =
                                range_prepared.remove(&(group_id.clone(), *transaction_id))
                            {
                                let writes =
                                    existing.state.writes.into_iter().collect::<BTreeMap<_, _>>();
                                if !writes.is_empty() {
                                    manager.replay_at(record.commit_ts, &writes)?;
                                }
                            }
                        } else {
                            range_prepared.remove(&(group_id.clone(), *transaction_id));
                        }
                    }
                    RangeEntryKind::Decision { transaction_id, committed, participants } => {
                        range_recovery_coordinations.remove(&(
                            group_id.clone(),
                            *transaction_id,
                            record.commit_ts,
                        ));
                        range_decisions.insert(
                            (group_id.clone(), *transaction_id, record.commit_ts),
                            PendingRangeDecision {
                                group_id: group_id.clone(),
                                transaction_id: *transaction_id,
                                commit_ts: record.commit_ts,
                                committed: *committed,
                                participants: participants.clone(),
                            },
                        );
                    }
                    RangeEntryKind::Coordinate { transaction_id, participants } => {
                        range_recovery_coordinations.insert(
                            (group_id.clone(), *transaction_id, record.commit_ts),
                            PendingRangeCoordination {
                                group_id: group_id.clone(),
                                transaction_id: *transaction_id,
                                commit_ts: record.commit_ts,
                                participants: participants.clone(),
                            },
                        );
                    }
                }
            }
            state.applied_index = state.commit_index;
        }
        if let Some(change) = replayed {
            members = normalize_members(change.old.clone());
            replayed_joint = change.new.clone().map(normalize_members);
        }
        let joint = replayed_joint.or(meta.joint.clone());
        let learner = learner && meta_fresh;
        let mut pools = std::collections::HashMap::new();
        for member in members.iter().filter(|member| member.id != id) {
            if !member.addr.is_empty() {
                pools.insert(member.id, PeerPool::new(member.addr.clone()));
            }
        }
        let node = Arc::new(Self {
            id,
            pools: std::sync::Mutex::new(pools),
            conn_tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
            inner: Arc::new(Mutex::new(Inner {
                term: meta.term,
                role: Role::Follower,
                voted_for: meta.voted_for,
                current: members,
                joint,
                learner,
                log,
                commit_index: meta.commit_index,
                applied,
                acks: std::collections::BTreeMap::new(),
                next_index: std::collections::BTreeMap::new(),
                wal,
                meta_path,
                reset_epoch: 0,
                observed_epoch: 0,
                campaign_now: false,
                leader_id: None,
            })),
            manager,
            write_lock: Arc::new(Mutex::new(())),
            isolated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            mesh_tls: std::sync::Mutex::new(None),
            rng: Arc::new(Mutex::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(1)
                    .wrapping_add((id as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15)),
            )),
            metadata: std::sync::Mutex::new(metadata),
            metadata_hook: MetadataHookSlot::default(),
            data_hook: MetadataHookSlot::default(),
            realtime_hook: MetadataHookSlot::default(),
            presence_hook: MetadataHookSlot::default(),
            topic_hook: MetadataHookSlot::default(),
            range_owners: std::sync::RwLock::new(Vec::new()),
            range_leaders: std::sync::RwLock::new(BTreeMap::new()),
            range_generation: AtomicU64::new(0),
            txn_commit_lock: Arc::new(Mutex::new(())),
            prepared: std::sync::Mutex::new(prepared),
            intent_wal: std::sync::Mutex::new(intent_wal),
            pending_resolutions: std::sync::Mutex::new(pending_resolutions),
            resolution_wal: std::sync::Mutex::new(resolution_wal),
            range_prepared: std::sync::Mutex::new(range_prepared),
            range_decisions: std::sync::Mutex::new(range_decisions),
            range_recovery_coordinations: std::sync::Mutex::new(range_recovery_coordinations),
            range_logs: std::sync::Mutex::new(range_logs),
            range_entries: std::sync::Mutex::new(range_entries),
            range_groups: std::sync::Mutex::new(range_groups),
            range_meta_persist_lock: std::sync::Mutex::new(()),
            range_meta_path,
            range_wal_path: range_dir,
            range_wal: std::sync::Mutex::new(range_wal),
        });
        for commit_ts in replayed_commit_ts {
            node.clear_prepared_for_commit(commit_ts)?;
        }
        for resolution in replayed_resolutions {
            node.queue_resolution_for_group(
                &resolution.group_id,
                resolution.transaction_id,
                resolution.commit_ts,
                resolution.participant,
                resolution.committed,
            )?;
        }
        Ok(node)
    }

    async fn next_election_timeout(&self) -> Duration {
        let mut rng = self.rng.lock().await;
        *rng ^= *rng << 13;
        *rng ^= *rng >> 7;
        *rng ^= *rng << 17;
        Duration::from_millis(ELECTION_TIMEOUT_MIN_MS + (*rng % ELECTION_TIMEOUT_JITTER_MS))
    }

    pub fn spawn(self: &Arc<Self>, listener: TcpListener) -> Vec<tokio::task::JoinHandle<()>> {
        self.spawn_limited(listener, DEFAULT_MAX_CONNECTIONS)
    }

    pub fn spawn_limited(
        self: &Arc<Self>,
        listener: TcpListener,
        max_connections: usize,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let accept = self.clone();
        let connections = self.conn_tasks.clone();
        let limit = Arc::new(tokio::sync::Semaphore::new(max_connections.max(1)));
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let Ok(permit) = limit.clone().try_acquire_owned() else {
                    continue;
                };
                let _ = socket.set_nodelay(true);
                let node = accept.clone();
                let task = tokio::spawn(async move {
                    let _permit = permit;
                    let transport = match node.mesh_tls.lock() {
                        Ok(guard) => guard.clone(),
                        Err(_) => {
                            tracing::error!(
                                node_id = node.id,
                                "raft mesh TLS config lock is poisoned"
                            );
                            return;
                        }
                    };
                    if let Some(transport) = transport {
                        let Ok(tls) = transport.acceptor.acceptor().accept(socket).await else {
                            return;
                        };
                        let _ = node.serve_connection(tls).await;
                        return;
                    }
                    let _ = node.serve_connection(socket).await;
                });
                let mut connections = connections.lock().unwrap_or_else(|e| e.into_inner());
                connections.retain(|task| !task.is_finished());
                connections.push(task);
            }
        });
        let main = self.clone();
        let main_task = tokio::spawn(async move {
            main.main_loop().await;
        });
        vec![accept_task, main_task]
    }

    pub fn shutdown(&self, tasks: Vec<tokio::task::JoinHandle<()>>) {
        for task in tasks {
            task.abort();
        }
        let mut connections = self.conn_tasks.lock().unwrap_or_else(|e| e.into_inner());
        for task in connections.drain(..) {
            task.abort();
        }
    }

    pub async fn run(self: &Arc<Self>, listener: TcpListener) -> Result<()> {
        let mut tasks = self.spawn(listener);
        if let Some(task) = tasks.pop() {
            let _ = task.await;
        }
        Ok(())
    }

    fn pools_snapshot(&self) -> Vec<(usize, Arc<PeerPool>)> {
        let pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        pools.iter().map(|(id, pool)| (*id, pool.clone())).collect()
    }

    async fn member_pools(&self) -> Vec<(usize, Arc<PeerPool>)> {
        let inner = self.inner.lock().await;
        let mut ids: Vec<usize> = inner.current.iter().map(|m| m.id).collect();
        if let Some(joint) = inner.joint.as_ref() {
            ids.extend(joint.iter().map(|m| m.id));
        }
        drop(inner);
        self.pools_snapshot()
            .into_iter()
            .filter(|(id, _)| *id != self.id && ids.contains(id))
            .collect()
    }

    fn reconcile_pools(&self, members: &[Member]) {
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        pools.retain(|id, _| *id == self.id || members.iter().any(|m| m.id == *id));
        for member in members {
            if member.id == self.id || member.addr.is_empty() {
                continue;
            }
            let pool = pools.entry(member.id).or_insert_with(|| PeerPool::new(member.addr.clone()));
            self.adopt_pool(pool);
        }
    }

    fn ensure_pools(&self, members: &[Member]) {
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        for member in members {
            if member.id == self.id || member.addr.is_empty() {
                continue;
            }
            let pool = pools.entry(member.id).or_insert_with(|| PeerPool::new(member.addr.clone()));
            self.adopt_pool(pool);
        }
    }

    fn adopt_pool(&self, pool: &Arc<PeerPool>) {
        let transport = match self.mesh_tls.lock() {
            Ok(guard) => guard.clone(),
            Err(_) => {
                pool.require_tls();
                tracing::error!(node_id = self.id, "raft mesh TLS config lock is poisoned");
                return;
            }
        };
        if let Some(transport) = transport {
            if let Err(error) = pool.try_set_connector(Some(transport.connector.clone())) {
                tracing::error!(node_id = self.id, %error, "failed to install raft peer TLS connector");
            }
        }
    }

    pub fn set_mesh_tls(&self, transport: MeshTransport) {
        if let Err(error) = self.try_set_mesh_tls(transport) {
            tracing::error!(node_id = self.id, %error, "failed to install raft mesh TLS transport");
        }
    }

    pub fn try_set_mesh_tls(&self, transport: MeshTransport) -> Result<()> {
        let transport = Arc::new(transport);
        let pools = self.pools_snapshot();
        for (_, pool) in &pools {
            pool.require_tls();
        }
        {
            let mut guard = self
                .mesh_tls
                .lock()
                .map_err(|_| RymeError::Internal(String::from("raft mesh TLS config lock")))?;
            *guard = Some(transport.clone());
        }
        for (_, pool) in pools {
            pool.try_set_connector(Some(transport.connector.clone()))?;
        }
        Ok(())
    }

    pub fn mesh_enabled(&self) -> bool {
        match self.try_mesh_enabled() {
            Ok(enabled) => enabled,
            Err(error) => {
                tracing::error!(node_id = self.id, %error, "failed to read raft mesh TLS state");
                true
            }
        }
    }

    pub fn try_mesh_enabled(&self) -> Result<bool> {
        self.mesh_tls
            .lock()
            .map(|guard| guard.is_some())
            .map_err(|_| RymeError::Internal(String::from("raft mesh TLS config lock")))
    }

    fn range_timeout_ms(&self, group_id: &str) -> u64 {
        let mut hash = 0xcbf29ce484222325u64 ^ self.id as u64;
        for byte in group_id.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        350 + hash % 250
    }

    async fn range_tick(self: &Arc<Self>) {
        if self.isolated.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let now = now_ms();
        let Ok(configured_ranges) = self.try_range_owners() else {
            tracing::error!(node_id = self.id, "range ownership metadata unavailable");
            return;
        };
        for configured in configured_ranges {
            let range = match self.try_with_current_range_leader(configured) {
                Ok(range) => range,
                Err(error) => {
                    tracing::error!(node_id = self.id, %error, "range leader metadata unavailable");
                    return;
                }
            };
            if !range.replicas.contains(&self.id) {
                continue;
            }
            let state = match self.try_ensure_range_group(&range.id, range.owner) {
                Ok(state) => state,
                Err(error) => {
                    tracing::error!(group_id = %range.id, %error, "range metadata unavailable");
                    continue;
                }
            };
            if state.leader == Some(self.id) {
                if now.saturating_sub(state.last_heartbeat_ms) >= 100 {
                    if let Ok(mut groups) = self.range_groups.lock() {
                        if let Some(current) = groups.get_mut(&range.id) {
                            current.last_heartbeat_ms = now;
                            current.last_contact_ms = now;
                        }
                    }
                    if let Err(error) = self.persist_range_meta() {
                        tracing::error!(
                            group_id = %range.id,
                            %error,
                            "range heartbeat metadata persistence failed"
                        );
                        continue;
                    }
                    match self.range_heartbeat(&range.id).await {
                        Ok(()) => {
                            if let Ok(mut leaders) = self.range_leaders.write() {
                                leaders.insert(range.id.clone(), self.id);
                            }
                        }
                        Err(error) => {
                            tracing::warn!(group_id = %range.id, %error, "range heartbeat quorum unavailable");
                        }
                    }
                }
                continue;
            }
            if state.last_contact_ms == 0 {
                if let Ok(mut groups) = self.range_groups.lock() {
                    if let Some(current) = groups.get_mut(&range.id) {
                        current.last_contact_ms = now;
                    }
                }
                if let Err(error) = self.persist_range_meta() {
                    tracing::error!(
                        group_id = %range.id,
                        %error,
                        "range contact metadata persistence failed"
                    );
                    continue;
                }
                continue;
            }
            if now.saturating_sub(state.last_contact_ms) >= self.range_timeout_ms(&range.id) {
                if let Err(error) = self.campaign_range(&range).await {
                    tracing::warn!(group_id = %range.id, %error, "range election failed");
                }
            }
        }
    }

    async fn range_heartbeat(self: &Arc<Self>, group_id: &str) -> Result<()> {
        let _commit = self.txn_commit_lock.lock().await;
        self.ensure_not_isolated()?;
        let mut range = self
            .try_range_owners()?
            .into_iter()
            .find(|range| range.id == group_id)
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        let state = self.try_ensure_range_group(group_id, self.id)?;
        if state.leader != Some(self.id) {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        range.owner = self.id;
        let mut replicas = range.replicas;
        if replicas.is_empty() {
            replicas.push(self.id);
        }
        if !replicas.contains(&self.id) {
            return Err(RymeError::Unavailable(String::from("range replica")));
        }
        replicas.sort_unstable();
        replicas.dedup();
        let (last_index, last_term) = self.try_range_last_position(group_id)?;
        let mut acknowledgements = 1usize;
        for replica in replicas.iter().copied().filter(|replica| *replica != self.id) {
            if self
                .sync_range_replica(
                    group_id,
                    replica,
                    state.term.max(1),
                    last_index,
                    last_term,
                    state.commit_index,
                )
                .await
                .is_ok()
            {
                acknowledgements += 1;
            }
        }
        self.ensure_not_isolated()?;
        if acknowledgements < majority(replicas.len()) {
            return Err(RymeError::Unavailable(String::from("range quorum")));
        }
        let current = self
            .range_groups
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?
            .get(group_id)
            .cloned()
            .ok_or_else(|| RymeError::Unavailable(String::from("range metadata unavailable")))?;
        if current.term != state.term || current.leader != Some(self.id) {
            return Err(RymeError::Unavailable(String::from("stale range leader")));
        }
        Ok(())
    }

    async fn sync_range_replica(
        self: &Arc<Self>,
        group_id: &str,
        replica: usize,
        term: u64,
        leader_last: u64,
        leader_last_term: u64,
        leader_commit: u64,
    ) -> Result<()> {
        let pool = self
            .pool_for(replica)
            .ok_or_else(|| RymeError::Unavailable(String::from("range replica unavailable")))?;
        let heartbeat = pool
            .roundtrip(&Rpc::RangeAppend {
                group_id: group_id.to_string(),
                index: 0,
                term,
                leader: self.id,
                prev_index: leader_last,
                prev_term: leader_last_term,
                leader_commit,
                commit_ts: 0,
                writes: Vec::new(),
                publish_data: false,
                kind: RangeEntryKind::Data,
            })
            .await?;
        let mut next_index = match heartbeat {
            Rpc::RangeAppendResponse {
                term: peer_term,
                appended,
                last_index,
                conflict_index,
                ..
            } => {
                if peer_term > term {
                    self.observe_range_term(group_id, peer_term)?;
                    return Err(RymeError::Unavailable(String::from("stale range leader")));
                }
                if appended {
                    if last_index < leader_last {
                        return Err(RymeError::Unavailable(String::from(
                            "range heartbeat acknowledgement index",
                        )));
                    }
                    return Ok(());
                }
                conflict_index
                    .unwrap_or_else(|| last_index.min(leader_last).saturating_add(1))
                    .clamp(1, leader_last.saturating_add(1).max(1))
            }
            _ => return Err(RymeError::Corrupt(String::from("range append rpc"))),
        };
        let mut same_index_retries = 0u8;
        while next_index <= leader_last {
            let index = next_index;
            let record = self
                .try_range_entry(group_id, index)?
                .ok_or_else(|| RymeError::Unavailable(String::from("range log gap")))?;
            let prev_index = index - 1;
            let prev_term = if prev_index == 0 {
                0
            } else {
                self.try_range_entry(group_id, prev_index)?
                    .map(|entry| entry.term)
                    .ok_or_else(|| RymeError::Unavailable(String::from("range log gap")))?
            };
            let response = pool
                .roundtrip(&Rpc::RangeAppend {
                    group_id: group_id.to_string(),
                    index,
                    term,
                    leader: self.id,
                    prev_index,
                    prev_term,
                    leader_commit: 0,
                    commit_ts: record.commit_ts,
                    writes: record.writes,
                    publish_data: record.publish_data,
                    kind: record.kind,
                })
                .await?;
            match response {
                Rpc::RangeAppendResponse {
                    term: peer_term,
                    appended: true,
                    last_index,
                    error: None,
                    ..
                } => {
                    if peer_term > term {
                        self.observe_range_term(group_id, peer_term)?;
                        return Err(RymeError::Unavailable(String::from("stale range leader")));
                    }
                    if last_index < index {
                        return Err(RymeError::Unavailable(String::from(
                            "range append acknowledgement index",
                        )));
                    }
                    same_index_retries = 0;
                    if index == leader_last {
                        break;
                    }
                    next_index = index.checked_add(1).ok_or_else(|| {
                        RymeError::Corrupt(String::from("range log index overflow"))
                    })?;
                }
                Rpc::RangeAppendResponse {
                    term: peer_term,
                    appended: false,
                    last_index,
                    conflict_index,
                    ..
                } => {
                    if peer_term > term {
                        self.observe_range_term(group_id, peer_term)?;
                        return Err(RymeError::Unavailable(String::from("stale range leader")));
                    }
                    let retry_index = conflict_index
                        .unwrap_or_else(|| last_index.min(leader_last).saturating_add(1))
                        .clamp(1, leader_last.saturating_add(1).max(1));
                    if retry_index == next_index {
                        same_index_retries = same_index_retries.saturating_add(1);
                        if same_index_retries > 1 {
                            return Err(RymeError::Unavailable(String::from(
                                "range replica conflict",
                            )));
                        }
                    } else {
                        same_index_retries = 0;
                    }
                    next_index = retry_index;
                }
                _ => return Err(RymeError::Corrupt(String::from("range append rpc"))),
            }
        }
        let committed = pool
            .roundtrip(&Rpc::RangeAppend {
                group_id: group_id.to_string(),
                index: 0,
                term,
                leader: self.id,
                prev_index: leader_last,
                prev_term: leader_last_term,
                leader_commit,
                commit_ts: 0,
                writes: Vec::new(),
                publish_data: false,
                kind: RangeEntryKind::Data,
            })
            .await?;
        match committed {
            Rpc::RangeAppendResponse { term: peer_term, appended: true, last_index, .. }
                if peer_term <= term && last_index >= leader_last =>
            {
                Ok(())
            }
            Rpc::RangeAppendResponse { term: peer_term, .. } if peer_term > term => {
                self.observe_range_term(group_id, peer_term)?;
                Err(RymeError::Unavailable(String::from("stale range leader")))
            }
            _ => Err(RymeError::Unavailable(String::from("range commit gap"))),
        }
    }

    fn observe_range_term(&self, group_id: &str, term: u64) -> Result<()> {
        {
            let mut groups = self
                .range_groups
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
            let state = groups.entry(group_id.to_string()).or_default();
            if term <= state.term {
                return Ok(());
            }
            state.term = term;
            state.voted_for = None;
            state.leader = None;
            state.last_contact_ms = now_ms();
        }
        let leader_update = self
            .range_leaders
            .write()
            .map(|mut leaders| leaders.remove(group_id))
            .map_err(|_| RymeError::Internal(String::from("range leader metadata lock")));
        let persist = self.persist_range_meta();
        leader_update?;
        persist
    }

    async fn campaign_range(self: &Arc<Self>, range: &RangeOwner) -> Result<()> {
        self.ensure_not_isolated()?;
        let (last_index, last_term) = self.try_range_last_position(&range.id)?;
        let (term, last_term, last_index) = {
            let mut groups = self
                .range_groups
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
            let state = groups.entry(range.id.clone()).or_default();
            if state.leader == Some(self.id) {
                return Ok(());
            }
            state.term = state
                .term
                .max(1)
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("range term overflow")))?;
            state.voted_for = Some(self.id);
            state.leader = None;
            state.last_contact_ms = now_ms();
            (state.term, last_term, last_index)
        };
        self.persist_range_meta()?;
        let mut votes = vec![self.id];
        let mut tasks = Vec::new();
        for replica in range.replicas.iter().copied().filter(|replica| *replica != self.id) {
            let Some(pool) = self.pool_for(replica) else { continue };
            let request = Rpc::RangeVoteRequest {
                group_id: range.id.clone(),
                term,
                candidate: self.id,
                last_term,
                last_index,
            };
            tasks.push(tokio::spawn(async move {
                pool.roundtrip(&request).await.map(|response| (response, replica))
            }));
        }
        for task in tasks {
            let Ok(Ok((response, replica))) = task.await else { continue };
            if let Rpc::RangeVoteResponse { term: peer_term, granted } = response {
                if peer_term > term {
                    self.observe_range_term(&range.id, peer_term)?;
                    return Ok(());
                }
                if granted && !votes.contains(&replica) {
                    votes.push(replica);
                }
            }
        }
        if votes.len() < majority(range.replicas.len()) {
            return Ok(());
        }
        self.ensure_not_isolated()?;
        let won = {
            let mut groups = self
                .range_groups
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
            let state = groups.entry(range.id.clone()).or_default();
            if state.term == term && state.voted_for == Some(self.id) && state.leader.is_none() {
                state.leader = Some(self.id);
                state.last_contact_ms = now_ms();
                state.last_heartbeat_ms = 0;
                true
            } else {
                false
            }
        };
        if !won {
            return Ok(());
        }
        if let Err(error) = self.persist_range_meta() {
            let mut groups = self
                .range_groups
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
            if let Some(state) = groups.get_mut(&range.id) {
                if state.term == term && state.leader == Some(self.id) {
                    state.leader = None;
                    state.last_heartbeat_ms = 0;
                }
            }
            return Err(error);
        }
        self.range_heartbeat(&range.id).await?;
        self.range_leaders
            .write()
            .map_err(|_| RymeError::Internal(String::from("range leader metadata lock")))?
            .insert(range.id.clone(), self.id);
        self.recover_range_coordinations_for_group(&range.id).await?;
        Ok(())
    }

    async fn recover_range_coordinations_for_group(self: &Arc<Self>, group_id: &str) -> Result<()> {
        let Some(range) = self.try_range_owner_by_id(group_id)? else {
            return Ok(());
        };
        if range.owner != self.id {
            return Ok(());
        }
        let commit_index = self
            .range_groups
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?
            .get(group_id)
            .map(|state| state.commit_index)
            .unwrap_or(0);
        let entries = self
            .range_entries
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range entries lock")))?
            .get(group_id)
            .cloned()
            .unwrap_or_default();
        let mut pending = BTreeMap::new();
        for record in entries.values().filter(|record| record.index <= commit_index) {
            match &record.kind {
                RangeEntryKind::Coordinate { transaction_id, participants } => {
                    pending.insert((*transaction_id, record.commit_ts), participants.clone());
                }
                RangeEntryKind::Decision { transaction_id, .. } => {
                    pending.remove(&(*transaction_id, record.commit_ts));
                }
                _ => {}
            }
        }
        for ((transaction_id, commit_ts), participants) in pending {
            if let Err(error) = self
                .record_range_decision(group_id, transaction_id, commit_ts, false, participants)
                .await
            {
                tracing::error!(
                    group_id,
                    transaction_id,
                    commit_ts,
                    %error,
                    "range leader recovery abort failed"
                );
            }
        }
        Ok(())
    }

    fn handle_range_vote(
        &self,
        group_id: &str,
        term: u64,
        candidate: usize,
        last_term: u64,
        last_index: u64,
    ) -> Result<(u64, bool)> {
        if term == 0 {
            return Ok((self.try_range_term(group_id)?, false));
        }
        let Some(range) = self.try_range_owners()?.into_iter().find(|range| range.id == group_id)
        else {
            return Ok((self.try_range_term(group_id)?, false));
        };
        if !range.replicas.contains(&self.id) || !range.replicas.contains(&candidate) {
            return Ok((self.try_range_term(group_id)?, false));
        }
        let (local_last_index, local_last_term) = self.try_range_last_position(group_id)?;
        let (response_term, granted, changed) = {
            let mut groups = self
                .range_groups
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
            let state = groups.entry(group_id.to_string()).or_default();
            if term < state.term {
                return Ok((state.term.max(1), false));
            }
            let mut changed = false;
            if term > state.term {
                state.term = term;
                state.voted_for = None;
                state.leader = None;
                changed = true;
            }
            let current_vote = state.voted_for;
            let up_to_date = last_term > local_last_term
                || (last_term == local_last_term && last_index >= local_last_index);
            let granted = up_to_date && (current_vote.is_none() || current_vote == Some(candidate));
            if granted {
                changed |= state.voted_for != Some(candidate) || state.leader.is_some();
                state.voted_for = Some(candidate);
                state.leader = None;
                state.last_contact_ms = now_ms();
            }
            (state.term.max(1), granted, changed)
        };
        if changed || granted {
            self.persist_range_meta()?;
        }
        Ok((response_term, granted))
    }

    async fn main_loop(self: Arc<Self>) {
        loop {
            if self.isolated.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            self.range_tick().await;
            self.retry_pending_resolutions().await;
            if self.take_campaign().await {
                if let Err(error) = self.campaign(true).await {
                    tracing::warn!(node = self.id, %error, "raft election campaign failed");
                }
                continue;
            }
            let (is_leader, can_campaign) = {
                let inner = self.inner.lock().await;
                let member = inner.is_member(self.id);
                (inner.role == Role::Leader, member)
            };
            if !can_campaign && !is_leader {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            if is_leader {
                self.replicate_all().await;
                tokio::time::sleep(Duration::from_millis(60)).await;
            } else {
                let timeout = self.next_election_timeout().await;
                if tokio::time::timeout(timeout, self.wait_for_reset()).await.is_err() {
                    if let Err(error) = self.campaign(false).await {
                        tracing::warn!(node = self.id, %error, "raft election campaign failed");
                    }
                }
            }
        }
    }

    async fn take_campaign(&self) -> bool {
        let mut inner = self.inner.lock().await;
        if inner.campaign_now {
            inner.campaign_now = false;
            return true;
        }
        false
    }

    async fn wait_for_reset(&self) {
        let term = self.inner.lock().await.term;
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut inner = self.inner.lock().await;
            if inner.term != term || inner.role == Role::Leader || inner.campaign_now {
                return;
            }
            if inner.reset_flag() {
                return;
            }
        }
    }

    async fn campaign(self: &Arc<Self>, skip_prevote: bool) -> Result<()> {
        self.ensure_not_isolated()?;
        if self.inner.lock().await.learner {
            return Ok(());
        }
        if !skip_prevote && !self.prevote().await? {
            return Ok(());
        }
        self.ensure_not_isolated()?;
        let (term, last_term, last_index) = {
            let mut inner = self.inner.lock().await;
            if inner.role == Role::Leader {
                return Ok(());
            }
            inner.term = inner
                .term
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("raft term overflow")))?;
            inner.role = Role::Candidate;
            inner.voted_for = Some(self.id);
            inner.leader_id = None;
            inner.persist_meta()?;
            let (last_term, last_index) = inner.last_position();
            (inner.term, last_term, last_index)
        };
        let mut votes = vec![self.id];
        let mut tasks = Vec::new();
        for (peer, pool) in self.member_pools().await {
            let request = Rpc::VoteRequest { term, candidate: self.id, last_term, last_index };
            tasks.push(tokio::spawn(async move {
                pool.roundtrip(&request).await.map(|response| (peer, response))
            }));
        }
        for task in tasks {
            if let Ok(Ok((peer, Rpc::VoteResponse { term: peer_term, granted }))) = task.await {
                let mut inner = self.inner.lock().await;
                if peer_term > inner.term {
                    inner.term = peer_term;
                    inner.role = Role::Follower;
                    inner.voted_for = None;
                    inner.leader_id = None;
                    inner.persist_meta()?;
                    return Ok(());
                }
                if granted && inner.term == term && !votes.contains(&peer) {
                    votes.push(peer);
                }
            }
        }
        let mut inner = self.inner.lock().await;
        if inner.term != term || inner.role != Role::Candidate {
            return Ok(());
        }
        self.ensure_not_isolated()?;
        let win_current = votes_win(&votes, &inner.current);
        let win_joint = inner.joint.as_ref().map(|joint| votes_win(&votes, joint)).unwrap_or(true);
        if win_current && win_joint {
            let last = inner.last_index();
            let next_index = last
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("raft log index overflow")))?;
            inner.role = Role::Leader;
            inner.leader_id = Some(self.id);
            inner.next_index.clear();
            inner.acks.clear();
            let mut ids: Vec<usize> = inner.current.iter().map(|member| member.id).collect();
            if let Some(joint) = inner.joint.as_ref() {
                ids.extend(joint.iter().map(|member| member.id));
            }
            ids.sort_unstable();
            ids.dedup();
            for id in ids {
                inner.next_index.insert(id, next_index);
                inner.acks.insert(id, 0);
            }
            if let Some(slot) = inner.acks.get_mut(&self.id) {
                *slot = last;
            }
        } else {
            inner.role = Role::Follower;
            inner.leader_id = None;
        }
        Ok(())
    }

    async fn prevote(&self) -> Result<bool> {
        let (prospective, last_term, last_index) = {
            let inner = self.inner.lock().await;
            if inner.role == Role::Leader {
                return Ok(false);
            }
            let (last_term, last_index) = inner.last_position();
            (
                inner
                    .term
                    .checked_add(1)
                    .ok_or_else(|| RymeError::Corrupt(String::from("raft term overflow")))?,
                last_term,
                last_index,
            )
        };
        let mut votes = vec![self.id];
        let mut tasks = Vec::new();
        for (peer, pool) in self.member_pools().await {
            let request = Rpc::PreVoteRequest {
                term: prospective,
                candidate: self.id,
                last_term,
                last_index,
            };
            tasks.push(tokio::spawn(async move {
                pool.roundtrip(&request).await.map(|response| (peer, response))
            }));
        }
        for task in tasks {
            match task.await {
                Ok(Ok((peer, Rpc::PreVoteResponse { term: peer_term, granted }))) => {
                    if peer_term > prospective {
                        let mut inner = self.inner.lock().await;
                        if peer_term > inner.term {
                            inner.term = peer_term;
                            inner.role = Role::Follower;
                            inner.voted_for = None;
                            inner.leader_id = None;
                            inner.persist_meta()?;
                        }
                        return Ok(false);
                    }
                    if granted && !votes.contains(&peer) {
                        votes.push(peer);
                    }
                }
                _ => continue,
            }
        }
        let inner = self.inner.lock().await;
        Ok(votes_win(&votes, &inner.current)
            && inner.joint.as_ref().map(|joint| votes_win(&votes, joint)).unwrap_or(true))
    }

    pub async fn transfer(self: &Arc<Self>, target: usize) -> Result<()> {
        {
            let inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return Err(RymeError::Unavailable(String::from("not leader")));
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let (caught_up, term) = {
                let inner = self.inner.lock().await;
                let caught_up = inner
                    .next_index
                    .get(&target)
                    .copied()
                    .map(|next| next > inner.last_index())
                    .unwrap_or(false);
                (caught_up, inner.term)
            };
            if caught_up {
                let pool = self
                    .pool_for(target)
                    .ok_or_else(|| RymeError::Unavailable(String::from("peer")))?;
                match pool.roundtrip(&Rpc::TransferRequest { term, from: self.id }).await {
                    Ok(Rpc::TransferResponse { accepted, .. }) if accepted => return Ok(()),
                    Ok(_) => return Err(RymeError::Unavailable(String::from("refused"))),
                    Err(e) => return Err(e),
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RymeError::Timeout);
            }
            self.replicate_once().await?;
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    fn pool_for(&self, target: usize) -> Option<Arc<PeerPool>> {
        self.pools.lock().unwrap_or_else(|e| e.into_inner()).get(&target).cloned()
    }

    pub fn set_isolated(&self, isolated: bool) {
        self.isolated.store(isolated, std::sync::atomic::Ordering::SeqCst);
        for (_, pool) in self.pools_snapshot() {
            pool.set_isolated(isolated);
            pool.purge();
        }
    }

    fn ensure_not_isolated(&self) -> Result<()> {
        if self.isolated.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(RymeError::Unavailable(String::from("isolated")));
        }
        Ok(())
    }

    pub fn pool(&self, target: usize) -> Option<Arc<PeerPool>> {
        self.pool_for(target)
    }

    async fn replicate_all(self: &Arc<Self>) {
        if let Err(error) = self.replicate_once().await {
            tracing::error!(node = self.id, %error, "raft replication metadata update failed");
            self.set_isolated(true);
            return;
        }
        if let Err(error) = self.apply_ready().await {
            tracing::error!(node = self.id, %error, "raft state machine apply failed");
            self.set_isolated(true);
        }
    }

    async fn replicate_once(self: &Arc<Self>) -> Result<()> {
        self.ensure_not_isolated()?;
        let peers: Vec<(usize, Arc<PeerPool>)> = self.member_pools().await;
        let (term, leader_commit, snapshot) = {
            let inner = self.inner.lock().await;
            (inner.term, inner.commit_index, Arc::new(inner.log.clone()))
        };
        let mut tasks = Vec::new();
        for (peer, pool) in peers {
            let node = self.clone();
            let snapshot = snapshot.clone();
            tasks.push(tokio::spawn(async move {
                node.replicate_to(peer, pool, term, leader_commit, snapshot).await
            }));
        }
        for task in tasks {
            let result = task
                .await
                .map_err(|error| RymeError::Internal(format!("raft replication task: {error}")))?;
            if let Err(error) = result {
                self.set_isolated(true);
                return Err(error);
            }
        }
        Ok(())
    }

    async fn replicate_to(
        &self,
        peer: usize,
        pool: Arc<PeerPool>,
        term: u64,
        leader_commit: u64,
        snapshot: Arc<Vec<LogEntry>>,
    ) -> Result<()> {
        let next = {
            let inner = self.inner.lock().await;
            inner.next_index.get(&peer).copied().unwrap_or(1)
        };
        let prev = snapshot.iter().rev().find(|e| e.index < next);
        let (prev_index, prev_term) = prev.map(|e| (e.index, e.term)).unwrap_or((0, 0));
        let batch_count = append_batch_count(&snapshot, next)?;
        let entries: Vec<LogEntry> =
            snapshot.iter().filter(|e| e.index >= next).take(batch_count).cloned().collect();
        let sent_last = entries.last().map(|entry| entry.index).unwrap_or(prev_index);
        let request = Rpc::AppendRequest {
            term,
            leader: self.id,
            prev_index,
            prev_term,
            entries,
            leader_commit,
        };
        match pool.roundtrip(&request).await {
            Ok(Rpc::AppendResponse { term: peer_term, ok, match_index }) => {
                let mut inner = self.inner.lock().await;
                if peer_term > inner.term {
                    inner.term = peer_term;
                    inner.role = Role::Follower;
                    inner.voted_for = None;
                    inner.leader_id = None;
                    inner.persist_meta()?;
                    return Ok(());
                }
                if inner.term != term || inner.role != Role::Leader {
                    return Ok(());
                }
                if ok {
                    if match_index < prev_index {
                        return Ok(());
                    }
                    let matched = match_index.min(sent_last);
                    let next = inner.next_index.entry(peer).or_insert(1);
                    *next = matched.saturating_add(1).max(1);
                    let ack = inner.acks.entry(peer).or_insert(0);
                    *ack = matched.max(*ack);
                    inner.advance_commit()?;
                } else {
                    let next = inner.next_index.entry(peer).or_insert(1);
                    let suggested = match_index
                        .min(snapshot.last().map(|entry| entry.index).unwrap_or(0))
                        .saturating_add(1)
                        .max(1);
                    *next =
                        if suggested < *next { suggested } else { next.saturating_sub(1).max(1) };
                }
            }
            Ok(_) => {}
            Err(_) => {}
        }
        Ok(())
    }

    async fn apply_ready(self: &Arc<Self>) -> Result<()> {
        let entries = {
            let inner = self.inner.lock().await;
            inner
                .log
                .iter()
                .filter(|e| e.index > inner.applied && e.index <= inner.commit_index)
                .cloned()
                .collect::<Vec<_>>()
        };
        for entry in entries {
            match decode_apply(&entry.payload)? {
                ApplyPayload::Data { commit_ts, writes } => {
                    let encoded_writes = writes.clone();
                    let writes = decode_writes(&writes)?;
                    let range_owners = self.try_range_owners()?;
                    if range_owners.is_empty() {
                        self.manager.replay_at(commit_ts, &writes)?;
                    } else {
                        self.manager.replay_at_filtered_fallible(commit_ts, &writes, |key| {
                            Ok(self
                                .try_range_owner_key_in(&range_owners, key)?
                                .map(|owner| owner == self.id)
                                .unwrap_or(true))
                        })?;
                    }
                    if self.inner.lock().await.role != Role::Leader {
                        self.apply_data_hook(crate::encode_applied(commit_ts, &encoded_writes))?;
                    }
                    self.clear_prepared_for_commit(commit_ts)?;
                }
                ApplyPayload::Conf { change } => {
                    self.apply_conf(change).await?;
                }
                ApplyPayload::Metadata { payload } => {
                    self.apply_metadata(payload)?;
                }
                ApplyPayload::Topic { payload } => {
                    self.apply_topic(payload)?;
                }
                ApplyPayload::Resolution { payload } => {
                    self.apply_resolution(payload)?;
                }
                ApplyPayload::Restore { target } => {
                    self.restore_to(target).await?;
                }
            }
            self.inner.lock().await.applied = entry.index;
        }
        Ok(())
    }

    fn apply_metadata(&self, payload: Vec<u8>) -> Result<()> {
        let hook = {
            let mut metadata = self
                .metadata
                .lock()
                .map_err(|_| RymeError::Internal(String::from("metadata lock")))?;
            *metadata = Some(payload.clone());
            self.metadata_hook
                .0
                .lock()
                .map_err(|_| RymeError::Internal(String::from("metadata hook lock")))?
                .clone()
        };
        if let Some(hook) = hook {
            hook(&payload)?;
        }
        Ok(())
    }

    fn apply_data_hook(&self, payload: Vec<u8>) -> Result<()> {
        let hook = self
            .data_hook
            .0
            .lock()
            .map_err(|_| RymeError::Internal(String::from("data hook lock")))?
            .clone();
        if let Some(hook) = hook {
            hook(&payload)?;
        }
        Ok(())
    }

    fn apply_realtime(&self, payload: Vec<u8>) -> Result<()> {
        let hook = self
            .realtime_hook
            .0
            .lock()
            .map_err(|_| RymeError::Internal(String::from("realtime hook lock")))?
            .clone();
        if let Some(hook) = hook {
            hook(&payload)?;
        }
        Ok(())
    }

    fn apply_topic(&self, payload: Vec<u8>) -> Result<()> {
        let hook = self
            .topic_hook
            .0
            .lock()
            .map_err(|_| RymeError::Internal(String::from("topic hook lock")))?
            .clone();
        if let Some(hook) = hook {
            hook(&payload)?;
        }
        Ok(())
    }

    fn apply_resolution(&self, payload: Vec<u8>) -> Result<()> {
        let resolution: ResolutionRecord = serde_json::from_slice(&payload)
            .map_err(|error| RymeError::Corrupt(format!("resolution: {error}")))?;
        match resolution {
            ResolutionRecord::Pending {
                transaction_id,
                commit_ts,
                participant,
                committed,
                group_id,
            } => self.queue_resolution_for_group(
                &group_id,
                transaction_id,
                commit_ts,
                participant,
                committed,
            ),
            ResolutionRecord::Batch { transaction_id, commit_ts, committed, participants } => {
                for participant in participants {
                    self.queue_resolution_for_group(
                        &participant.group_id,
                        transaction_id,
                        commit_ts,
                        participant.participant,
                        committed,
                    )?;
                }
                Ok(())
            }
            ResolutionRecord::Complete { .. } => Ok(()),
        }
    }

    fn apply_presence(&self, payload: Vec<u8>) -> Result<()> {
        let hook = self
            .presence_hook
            .0
            .lock()
            .map_err(|_| RymeError::Internal(String::from("presence hook lock")))?
            .clone();
        if let Some(hook) = hook {
            hook(&payload)?;
        }
        Ok(())
    }

    async fn apply_conf(&self, change: crate::ConfChange) -> Result<()> {
        let members = {
            let mut inner = self.inner.lock().await;
            inner.current = normalize_members(change.old.clone());
            inner.joint = change.new.clone().map(normalize_members);
            let admitted = inner
                .current
                .iter()
                .chain(inner.joint.iter().flatten())
                .any(|member| member.id == self.id && !member.addr.is_empty());
            if admitted {
                inner.learner = false;
            }
            if !inner.is_member(self.id) {
                inner.role = Role::Follower;
                inner.campaign_now = false;
            }
            inner.persist_meta()?;
            let mut members = inner.current.clone();
            if let Some(joint) = inner.joint.as_ref() {
                members.extend(joint.iter().cloned());
            }
            members
        };
        self.reconcile_pools(&members);
        Ok(())
    }

    async fn serve_connection<S>(self: &Arc<Self>, mut socket: S) -> Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        if self.isolated.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        loop {
            if self.isolated.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }
            let request = match tokio::time::timeout(
                Duration::from_secs(5),
                read_frame_with_budget(&mut socket, &RPC_READ_FRAME_BUDGET),
            )
            .await
            {
                Ok(Ok(request)) => request,
                _ => return Ok(()),
            };
            let BudgetedRpc { rpc, _permit } = request;
            let response = self.handle_rpc(rpc).await?;
            drop(_permit);
            if tokio::time::timeout(Duration::from_secs(5), write_frame(&mut socket, &response))
                .await
                .is_err()
            {
                return Ok(());
            }
        }
    }

    async fn handle_rpc(self: &Arc<Self>, rpc: Rpc) -> Result<Rpc> {
        match rpc {
            Rpc::VoteRequest { term, candidate, last_term, last_index } => {
                let mut inner = self.inner.lock().await;
                if term == 0 || term < inner.term {
                    return Ok(Rpc::VoteResponse { term: inner.term, granted: false });
                }
                if !inner.is_member(candidate) {
                    return Ok(Rpc::VoteResponse { term: inner.term, granted: false });
                }
                let term_changed = term > inner.term;
                if term_changed {
                    inner.term = term;
                    inner.role = Role::Follower;
                    inner.voted_for = None;
                    inner.leader_id = None;
                }
                let already = inner.voted_for.is_some() && inner.voted_for != Some(candidate);
                if already {
                    if term_changed {
                        inner.persist_meta()?;
                    }
                    let term = inner.term;
                    return Ok(Rpc::VoteResponse { term, granted: false });
                }
                let (last_t, last_i) = inner.last_position();
                if last_term < last_t || (last_term == last_t && last_index < last_i) {
                    if term_changed {
                        inner.persist_meta()?;
                    }
                    let term = inner.term;
                    return Ok(Rpc::VoteResponse { term, granted: false });
                }
                inner.voted_for = Some(candidate);
                inner.bump_reset();
                inner.persist_meta()?;
                Ok(Rpc::VoteResponse { term: inner.term, granted: true })
            }
            Rpc::PreVoteRequest { term, candidate, last_term, last_index } => {
                let inner = self.inner.lock().await;
                if term <= inner.term {
                    return Ok(Rpc::PreVoteResponse { term: inner.term, granted: false });
                }
                if !inner.is_member(candidate) {
                    return Ok(Rpc::PreVoteResponse { term: inner.term, granted: false });
                }
                let (last_t, last_i) = inner.last_position();
                if last_term < last_t || (last_term == last_t && last_index < last_i) {
                    return Ok(Rpc::PreVoteResponse { term: inner.term, granted: false });
                }
                Ok(Rpc::PreVoteResponse { term: inner.term, granted: true })
            }
            Rpc::TransferRequest { term, from } => {
                let mut inner = self.inner.lock().await;
                if term < inner.term
                    || term == 0
                    || !inner.is_member(from)
                    || inner.leader_id != Some(from)
                {
                    return Ok(Rpc::TransferResponse { term: inner.term, accepted: false });
                }
                if term > inner.term {
                    inner.term = term;
                    inner.voted_for = None;
                    inner.persist_meta()?;
                }
                inner.role = Role::Follower;
                inner.leader_id = None;
                inner.campaign_now = true;
                Ok(Rpc::TransferResponse { term: inner.term, accepted: true })
            }
            Rpc::Realtime { payload } => {
                if payload.is_empty() {
                    return Err(RymeError::InvalidArgument(String::from("realtime")));
                }
                self.apply_realtime(payload)?;
                Ok(Rpc::RealtimeResponse { ok: true })
            }
            Rpc::Presence { payload } => {
                if payload.is_empty() {
                    return Err(RymeError::InvalidArgument(String::from("presence")));
                }
                self.apply_presence(payload)?;
                Ok(Rpc::PresenceResponse { ok: true })
            }
            Rpc::ProposeWrite { payload, route_generation } => {
                let writes = decode_writes(&payload)?;
                if writes.is_empty() {
                    return Err(RymeError::InvalidArgument(String::from("writes")));
                }
                match self.forward_write_to_leader(writes, route_generation).await {
                    Ok(commit_ts) => {
                        Ok(Rpc::ProposeWriteResponse { commit_ts: Some(commit_ts), error: None })
                    }
                    Err(error) => {
                        Ok(Rpc::ProposeWriteResponse { commit_ts: None, error: Some(error.into()) })
                    }
                }
            }
            Rpc::ProposeTransaction { payload } => {
                let state: ryme_txn::TransactionState = serde_json::from_slice(&payload)
                    .map_err(|error| RymeError::Corrupt(format!("transaction: {error}")))?;
                let txn = self.manager.from_state(state)?;
                match self.forward_transaction_to_leader(txn).await {
                    Ok(commit_ts) => Ok(Rpc::ProposeTransactionResponse {
                        commit_ts: Some(commit_ts),
                        error: None,
                    }),
                    Err(error) => Ok(Rpc::ProposeTransactionResponse {
                        commit_ts: None,
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::PrepareTransaction { transaction_id, commit_ts, payload } => {
                let _commit = self.txn_commit_lock.lock().await;
                let state: ryme_txn::TransactionState = serde_json::from_slice(&payload)
                    .map_err(|error| RymeError::Corrupt(format!("transaction prepare: {error}")))?;
                let txn = self.manager.from_state(state.clone())?;
                match self.prepare_intent(transaction_id, commit_ts, state, &txn) {
                    Ok(()) => Ok(Rpc::PrepareTransactionResponse { prepared: true, error: None }),
                    Err(error) => Ok(Rpc::PrepareTransactionResponse {
                        prepared: false,
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::ResolveTransaction { transaction_id, commit_ts, committed } => {
                let _commit = self.txn_commit_lock.lock().await;
                match self.resolve_intent(transaction_id, commit_ts, committed) {
                    Ok(()) => Ok(Rpc::ResolveTransactionResponse { resolved: true, error: None }),
                    Err(error) => Ok(Rpc::ResolveTransactionResponse {
                        resolved: false,
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::RangePrepareTransaction { group_id, transaction_id, commit_ts, payload } => {
                let _commit = self.txn_commit_lock.lock().await;
                let state: ryme_txn::TransactionState =
                    serde_json::from_slice(&payload).map_err(|error| {
                        RymeError::Corrupt(format!("range transaction prepare: {error}"))
                    })?;
                match self.prepare_range_group(&group_id, transaction_id, commit_ts, state).await {
                    Ok(()) => {
                        Ok(Rpc::RangePrepareTransactionResponse { prepared: true, error: None })
                    }
                    Err(error) => Ok(Rpc::RangePrepareTransactionResponse {
                        prepared: false,
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::RangeResolveTransaction { group_id, transaction_id, commit_ts, committed } => {
                let _commit = self.txn_commit_lock.lock().await;
                match self
                    .resolve_range_group(&group_id, transaction_id, commit_ts, committed)
                    .await
                {
                    Ok(()) => {
                        Ok(Rpc::RangeResolveTransactionResponse { resolved: true, error: None })
                    }
                    Err(error) => Ok(Rpc::RangeResolveTransactionResponse {
                        resolved: false,
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::RangeDecisionTransaction {
                group_id,
                transaction_id,
                commit_ts,
                committed,
                participants,
            } => {
                let _commit = self.txn_commit_lock.lock().await;
                match self
                    .record_range_decision(
                        &group_id,
                        transaction_id,
                        commit_ts,
                        committed,
                        participants,
                    )
                    .await
                {
                    Ok(()) => {
                        Ok(Rpc::RangeDecisionTransactionResponse { recorded: true, error: None })
                    }
                    Err(error) => Ok(Rpc::RangeDecisionTransactionResponse {
                        recorded: false,
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::RangeCoordinateTransaction {
                group_id,
                transaction_id,
                commit_ts,
                participants,
            } => {
                let _commit = self.txn_commit_lock.lock().await;
                match self
                    .record_range_coordination(&group_id, transaction_id, commit_ts, participants)
                    .await
                {
                    Ok(()) => {
                        Ok(Rpc::RangeCoordinateTransactionResponse { recorded: true, error: None })
                    }
                    Err(error) => Ok(Rpc::RangeCoordinateTransactionResponse {
                        recorded: false,
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::RangeVoteRequest { group_id, term, candidate, last_term, last_index } => {
                let (term, granted) =
                    self.handle_range_vote(&group_id, term, candidate, last_term, last_index)?;
                Ok(Rpc::RangeVoteResponse { term, granted })
            }
            Rpc::RangeCommit { group_id, commit_ts, writes } => {
                let _commit = self.txn_commit_lock.lock().await;
                match self.commit_range_group(&group_id, commit_ts, &writes).await {
                    Ok(()) => Ok(Rpc::RangeCommitResponse { committed: true, error: None }),
                    Err(error) => {
                        Ok(Rpc::RangeCommitResponse { committed: false, error: Some(error.into()) })
                    }
                }
            }
            Rpc::RangeProposeWrite { group_id, payload, route_generation } => {
                match self.commit_range_write(&group_id, &payload, route_generation).await {
                    Ok(commit_ts) => Ok(Rpc::RangeProposeWriteResponse {
                        commit_ts: Some(commit_ts),
                        error: None,
                    }),
                    Err(error) => Ok(Rpc::RangeProposeWriteResponse {
                        commit_ts: None,
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::RangeAppend {
                group_id,
                index,
                term,
                leader,
                prev_index,
                prev_term,
                leader_commit,
                commit_ts,
                writes,
                publish_data,
                kind,
            } => {
                let result = self.append_range_follower_marked(
                    &group_id,
                    term,
                    leader,
                    prev_index,
                    prev_term,
                    index,
                    commit_ts,
                    &writes,
                    leader_commit,
                    publish_data,
                    kind.clone(),
                );
                match result {
                    Ok(last_index) => Ok(Rpc::RangeAppendResponse {
                        term: self.try_range_term(&group_id)?,
                        appended: true,
                        last_index,
                        conflict_index: None,
                        error: None,
                    }),
                    Err(error) => Ok(Rpc::RangeAppendResponse {
                        term: self.try_range_term(&group_id)?,
                        appended: false,
                        last_index: self.try_range_last_index(&group_id)?,
                        conflict_index: self.range_conflict_index(
                            &group_id,
                            prev_index,
                            prev_term,
                            index,
                            term,
                            commit_ts,
                            &writes,
                            publish_data,
                            &kind,
                        )?,
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::RangeSnapshotRequest {
                start,
                end,
                routing_version,
                read_ts,
                max_rows,
                tenant,
                database,
            } => {
                let snapshot = self.snapshot_range_for_version(
                    routing_version,
                    &start,
                    &end,
                    read_ts,
                    max_rows as usize,
                    tenant.as_deref(),
                    database.as_deref(),
                )?;
                Ok(Rpc::RangeSnapshotResponse { snapshot })
            }
            Rpc::RangeInstallRequest { snapshot } => {
                let _commit = self.txn_commit_lock.lock().await;
                let rows = self.install_range_snapshot(&snapshot)?;
                Ok(Rpc::RangeInstallResponse { rows })
            }
            Rpc::RangePurgeRequest { group_id, expected_epoch, snapshot } => {
                let rows =
                    self.purge_range_after_transfer(&group_id, expected_epoch, &snapshot).await?;
                Ok(Rpc::RangePurgeResponse { rows })
            }
            Rpc::RangeReadRequest { key, read_ts } => {
                let (value, expires_at) = self.read_range_value(&key, read_ts)?;
                let effective_read_ts =
                    if read_ts == 0 { self.manager.latest_commit() } else { read_ts };
                Ok(Rpc::RangeReadResponse { value, expires_at, read_ts: effective_read_ts })
            }
            Rpc::RangeReadBatchRequest { keys, read_ts } => {
                let values = self.read_range_values(&keys, read_ts)?;
                Ok(Rpc::RangeReadBatchResponse { values })
            }
            Rpc::AppendRequest { term, leader, prev_index, prev_term, entries, leader_commit } => {
                let (result, applied_now, log_storage_failed) = {
                    let mut inner = self.inner.lock().await;
                    if term < inner.term || term == 0 || !inner.is_member(leader) {
                        let term = inner.term;
                        (
                            Rpc::AppendResponse {
                                term,
                                ok: false,
                                match_index: inner.last_index(),
                            },
                            false,
                            false,
                        )
                    } else {
                        let term_changed = term > inner.term;
                        if term_changed {
                            inner.term = term;
                            inner.voted_for = None;
                            inner.leader_id = None;
                        }
                        inner.role = Role::Follower;
                        inner.leader_id = Some(leader);
                        inner.bump_reset();
                        let outcome = append_to_log(
                            &mut inner,
                            term,
                            prev_index,
                            prev_term,
                            entries,
                            leader_commit,
                        );
                        if term_changed || outcome.is_ok() {
                            inner.persist_meta()?;
                        }
                        let log_storage_failed =
                            outcome.as_ref().is_err_and(is_wal_storage_failure);
                        match outcome {
                            Ok(matched) => (
                                Rpc::AppendResponse {
                                    term: inner.term,
                                    ok: true,
                                    match_index: matched,
                                },
                                true,
                                false,
                            ),
                            Err(_) => {
                                let term = inner.term;
                                (
                                    Rpc::AppendResponse {
                                        term,
                                        ok: false,
                                        match_index: inner.last_index(),
                                    },
                                    false,
                                    log_storage_failed,
                                )
                            }
                        }
                    }
                };
                if log_storage_failed {
                    self.set_isolated(true);
                }
                if applied_now {
                    self.apply_ready().await?;
                }
                Ok(result)
            }
            Rpc::VoteResponse { .. }
            | Rpc::AppendResponse { .. }
            | Rpc::PreVoteResponse { .. }
            | Rpc::TransferResponse { .. }
            | Rpc::RealtimeResponse { .. }
            | Rpc::PresenceResponse { .. }
            | Rpc::RangeSnapshotResponse { .. }
            | Rpc::RangeInstallResponse { .. }
            | Rpc::RangePurgeResponse { .. }
            | Rpc::RangeReadResponse { .. }
            | Rpc::RangeReadBatchResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::ProposeWriteResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::ProposeTransactionResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::PrepareTransactionResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::ResolveTransactionResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::RangePrepareTransactionResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::RangeResolveTransactionResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::RangeDecisionTransactionResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::RangeCoordinateTransactionResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::RangeVoteResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::RangeCommitResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::RangeProposeWriteResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
            Rpc::RangeAppendResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
        }
    }

    pub async fn propose_write(
        self: &Arc<Self>,
        writes: BTreeMap<RecordKey, ryme_txn::WriteOp>,
    ) -> Result<u64> {
        self.propose_write_with_generation(writes, None).await
    }

    async fn propose_write_with_generation(
        self: &Arc<Self>,
        writes: BTreeMap<RecordKey, ryme_txn::WriteOp>,
        route_generation: Option<u64>,
    ) -> Result<u64> {
        let mut txn = self.manager.begin();
        if let Some(generation) = route_generation {
            txn.set_route_generation(generation);
        }
        for (key, op) in writes {
            match op.value {
                Some(bytes) => self.manager.put_with_ttl(&mut txn, key, bytes, op.expires_at),
                None => self.manager.delete(&mut txn, key),
            }
        }
        self.commit_txn(txn).await
    }

    async fn known_leader(&self) -> Option<usize> {
        let inner = self.inner.lock().await;
        if inner.role == Role::Leader {
            Some(self.id)
        } else {
            inner.leader_id
        }
    }

    async fn forward_write_to_leader(
        self: &Arc<Self>,
        writes: BTreeMap<RecordKey, ryme_txn::WriteOp>,
        route_generation: Option<u64>,
    ) -> Result<u64> {
        let Some(leader) = self.known_leader().await else {
            return Err(RymeError::Unavailable(String::from("leader unknown")));
        };
        if leader == self.id {
            return self.propose_write_with_generation(writes, route_generation).await;
        }
        let pool = self
            .pool_for(leader)
            .ok_or_else(|| RymeError::Unavailable(String::from("leader unavailable")))?;
        let payload = encode_writes(&writes)?;
        match pool.roundtrip(&Rpc::ProposeWrite { payload, route_generation }).await? {
            Rpc::ProposeWriteResponse { commit_ts: Some(commit_ts), error: None } => Ok(commit_ts),
            Rpc::ProposeWriteResponse { error: Some(error), .. } => Err(error.into()),
            _ => Err(RymeError::Corrupt(String::from("write forward rpc"))),
        }
    }

    async fn forward_transaction_to_leader(
        self: &Arc<Self>,
        txn: ryme_txn::Transaction,
    ) -> Result<u64> {
        let Some(leader) = self.known_leader().await else {
            return Err(RymeError::Unavailable(String::from("leader unknown")));
        };
        if leader == self.id {
            return self.commit_txn(txn).await;
        }
        let pool = self
            .pool_for(leader)
            .ok_or_else(|| RymeError::Unavailable(String::from("leader unavailable")))?;
        let payload = serde_json::to_vec(&txn.state())
            .map_err(|error| RymeError::Internal(format!("transaction: {error}")))?;
        match pool.roundtrip(&Rpc::ProposeTransaction { payload }).await? {
            Rpc::ProposeTransactionResponse { commit_ts: Some(commit_ts), error: None } => {
                Ok(commit_ts)
            }
            Rpc::ProposeTransactionResponse { error: Some(error), .. } => Err(error.into()),
            Rpc::ProposeTransactionResponse { .. } => {
                Err(RymeError::Corrupt(String::from("transaction forward response")))
            }
            _ => Err(RymeError::Corrupt(String::from("transaction forward rpc"))),
        }
    }

    pub async fn propose_write_on(
        self: &Arc<Self>,
        owner: usize,
        writes: BTreeMap<RecordKey, ryme_txn::WriteOp>,
        route_generation: Option<u64>,
    ) -> Result<u64> {
        if writes.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("writes")));
        }
        if let Some((group_id, range_owner)) = self.range_group_for_writes(&writes)? {
            return self
                .propose_range_write(range_owner, &group_id, writes, route_generation)
                .await;
        }
        if self.is_leader().await {
            return self.propose_write_with_generation(writes, route_generation).await;
        }
        let _ = owner;
        self.forward_write_to_leader(writes, route_generation).await
    }

    pub async fn propose_write_to_leader(
        self: &Arc<Self>,
        writes: BTreeMap<RecordKey, ryme_txn::WriteOp>,
        route_generation: Option<u64>,
    ) -> Result<u64> {
        if writes.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("writes")));
        }
        self.forward_write_to_leader(writes, route_generation).await
    }

    pub async fn commit_transaction_to_leader(
        self: &Arc<Self>,
        txn: ryme_txn::Transaction,
    ) -> Result<u64> {
        self.forward_transaction_to_leader(txn).await
    }

    fn range_group_for_writes(
        &self,
        writes: &BTreeMap<RecordKey, ryme_txn::WriteOp>,
    ) -> Result<Option<(String, usize)>> {
        let owners = self.try_range_owners()?;
        if owners.is_empty() {
            return Ok(None);
        }
        let mut group = None;
        for key in writes.keys() {
            let Some(range) = self.try_range_for_record_in(&owners, key)? else {
                return Ok(None);
            };
            match &group {
                None => group = Some((range.id, range.owner)),
                Some((group_id, owner)) if *group_id == range.id && *owner == range.owner => {}
                Some(_) => return Ok(None),
            }
        }
        Ok(group)
    }

    async fn propose_range_write(
        self: &Arc<Self>,
        owner: usize,
        group_id: &str,
        writes: BTreeMap<RecordKey, ryme_txn::WriteOp>,
        route_generation: Option<u64>,
    ) -> Result<u64> {
        let payload = encode_writes(&writes)?;
        if owner == self.id {
            return self.commit_range_write(group_id, &payload, route_generation).await;
        }
        let pool = self
            .pool_for(owner)
            .ok_or_else(|| RymeError::Unavailable(String::from("range leader unavailable")))?;
        match pool
            .roundtrip(&Rpc::RangeProposeWrite {
                group_id: group_id.to_string(),
                payload,
                route_generation,
            })
            .await?
        {
            Rpc::RangeProposeWriteResponse { commit_ts: Some(commit_ts), error: None } => {
                Ok(commit_ts)
            }
            Rpc::RangeProposeWriteResponse { error: Some(error), .. } => Err(error.into()),
            _ => Err(RymeError::Corrupt(String::from("range write rpc"))),
        }
    }

    pub async fn propose_metadata(self: &Arc<Self>, payload: Vec<u8>) -> Result<u64> {
        let encoded = crate::encode_metadata(&payload)?;
        let index = self.propose_frame(encoded).await?;
        self.wait_applied(index).await?;
        Ok(index)
    }

    pub async fn propose_restore(self: &Arc<Self>, target: u64) -> Result<u64> {
        if target > self.manager.latest_commit() {
            return Err(RymeError::InvalidArgument(String::from("restore target")));
        }
        let index = self.propose_frame(crate::encode_restore(target)).await?;
        self.wait_applied(index).await?;
        Ok(index)
    }

    async fn restore_to(self: &Arc<Self>, target: u64) -> Result<u64> {
        let range_owners = self.try_range_owners()?;
        if !range_owners.is_empty() {
            return self.restore_active_ranges_to(target, range_owners).await;
        }
        let entries = {
            let inner = self.inner.lock().await;
            inner.log.clone()
        };
        let mut replay = Vec::new();
        for entry in entries {
            let ApplyPayload::Data { commit_ts, writes } = decode_apply(&entry.payload)? else {
                continue;
            };
            if commit_ts <= target {
                replay.push((commit_ts, decode_writes(&writes)?));
            }
        }
        self.manager.restore_with_replay(Engine::default(), &replay)
    }

    async fn restore_active_ranges_to(
        self: &Arc<Self>,
        target: u64,
        ranges: Vec<RangeOwner>,
    ) -> Result<u64> {
        let mut restored = 0u64;
        for range in ranges {
            if !range.replicas.contains(&self.id) && range.owner != self.id {
                continue;
            }
            let entries = self
                .range_entries
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range entries")))?
                .get(&range.id)
                .cloned()
                .unwrap_or_default();
            let mut replacement = Engine::default();
            let mut keep_through = 0u64;
            let mut prepared = BTreeMap::<u128, (u64, ryme_txn::TransactionState)>::new();
            for (index, record) in &entries {
                if record.commit_ts > target {
                    continue;
                }
                keep_through = *index;
                match &record.kind {
                    RangeEntryKind::Data => {
                        for (key, op) in decode_writes(&record.writes)? {
                            replacement.apply_with_expiry(
                                key,
                                record.commit_ts,
                                op.value,
                                op.expires_at,
                            )?;
                        }
                    }
                    RangeEntryKind::Prepare { transaction_id, state } => {
                        let state: ryme_txn::TransactionState = serde_json::from_slice(state)
                            .map_err(|error| {
                                RymeError::Corrupt(format!("range transaction prepare: {error}"))
                            })?;
                        prepared.insert(*transaction_id, (record.commit_ts, state));
                    }
                    RangeEntryKind::Resolve { transaction_id, committed } => {
                        if let Some((_, state)) = prepared.remove(transaction_id) {
                            if *committed {
                                for (key, op) in state.writes {
                                    replacement.apply_with_expiry(
                                        key,
                                        record.commit_ts,
                                        op.value,
                                        op.expires_at,
                                    )?;
                                }
                            }
                        }
                    }
                    RangeEntryKind::Decision { .. } | RangeEntryKind::Coordinate { .. } => {}
                }
            }
            self.manager.replace_routing_range_for_version(
                range.routing_version,
                &range.start,
                &range.end,
                replacement,
            )?;
            if let Ok(mut pending) = self.range_prepared.lock() {
                pending.retain(|(group_id, _), _| group_id != &range.id);
            }
            if let Ok(mut pending) = self.range_decisions.lock() {
                pending.retain(|(group_id, _, commit_ts), _| {
                    group_id != &range.id || *commit_ts <= target
                });
            }
            if let Ok(mut pending) = self.range_recovery_coordinations.lock() {
                pending.retain(|(group_id, _, commit_ts), _| {
                    group_id != &range.id || *commit_ts <= target
                });
            }
            {
                let mut groups = self
                    .range_groups
                    .lock()
                    .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
                let state = groups.entry(range.id.clone()).or_default();
                state.commit_index = state.commit_index.min(keep_through);
                state.applied_index = state.applied_index.min(keep_through);
            }
            self.truncate_range_suffix(&range.id, keep_through)?;
            if !prepared.is_empty()
                && self
                    .try_range_owner_by_id(&range.id)?
                    .is_some_and(|current| current.owner == self.id)
            {
                for (transaction_id, (commit_ts, _)) in prepared {
                    self.commit_range_record(
                        &range.id,
                        commit_ts,
                        RangeEntryKind::Resolve { transaction_id, committed: false },
                    )
                    .await?;
                }
            }
            restored = restored.max(target);
        }
        self.persist_range_meta()?;
        self.manager.advance_to(target);
        Ok(restored)
    }

    pub async fn propose_topic(self: &Arc<Self>, payload: Vec<u8>) -> Result<u64> {
        let encoded = crate::encode_topic(&payload)?;
        let index = self.propose_frame(encoded).await?;
        self.wait_applied(index).await?;
        Ok(index)
    }

    pub async fn fanout_realtime(self: &Arc<Self>, payload: Vec<u8>) -> Result<()> {
        if payload.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("realtime")));
        }
        if !self.is_leader().await {
            return Err(RymeError::Unavailable(String::from("not leader")));
        }
        self.apply_realtime(payload.clone())?;
        self.fanout_realtime_peers(payload).await
    }

    pub async fn fanout_realtime_peers(self: &Arc<Self>, payload: Vec<u8>) -> Result<()> {
        if payload.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("realtime")));
        }
        if !self.is_leader().await {
            return Err(RymeError::Unavailable(String::from("not leader")));
        }
        let peers = self.member_pools().await;
        for (_, pool) in peers {
            let payload = payload.clone();
            tokio::spawn(async move {
                let _ = pool.roundtrip(&Rpc::Realtime { payload }).await;
            });
        }
        Ok(())
    }

    pub async fn fanout_presence(self: &Arc<Self>, payload: Vec<u8>) -> Result<()> {
        if payload.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("presence")));
        }
        if !self.is_leader().await {
            return Err(RymeError::Unavailable(String::from("not leader")));
        }
        self.apply_presence(payload.clone())?;
        let peers = self.member_pools().await;
        for (_, pool) in peers {
            let payload = payload.clone();
            tokio::spawn(async move {
                let _ = pool.roundtrip(&Rpc::Presence { payload }).await;
            });
        }
        Ok(())
    }

    fn intent_id(&self, txn: &ryme_txn::Transaction) -> u128 {
        ((self.id as u128) << 64) | txn.id as u128
    }

    fn append_intent_record(&self, record: &IntentRecord, commit_ts: u64) -> Result<()> {
        let payload = serde_json::to_vec(record)
            .map_err(|error| RymeError::Internal(format!("transaction intent: {error}")))?;
        let mut wal = self
            .intent_wal
            .lock()
            .map_err(|_| RymeError::Internal(String::from("transaction intent wal")))?;
        if let Err(error) = wal.append(commit_ts, &payload).and_then(|_| wal.sync()) {
            let isolate = is_wal_storage_failure(&error);
            drop(wal);
            if isolate {
                self.set_isolated(true);
            }
            return Err(error);
        }
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn queue_resolution(
        &self,
        transaction_id: u128,
        commit_ts: u64,
        participant: usize,
        committed: bool,
    ) -> Result<()> {
        self.queue_resolution_for_group("", transaction_id, commit_ts, participant, committed)
    }

    fn queue_resolution_for_group(
        &self,
        group_id: &str,
        transaction_id: u128,
        commit_ts: u64,
        participant: usize,
        committed: bool,
    ) -> Result<()> {
        let key = (group_id.to_string(), transaction_id, commit_ts, participant);
        let mut pending = self
            .pending_resolutions
            .lock()
            .map_err(|_| RymeError::Internal(String::from("transaction resolutions")))?;
        if pending.contains_key(&key) {
            return Ok(());
        }
        let record = ResolutionRecord::Pending {
            transaction_id,
            commit_ts,
            participant,
            committed,
            group_id: group_id.to_string(),
        };
        let payload = serde_json::to_vec(&record)
            .map_err(|error| RymeError::Internal(format!("transaction resolution: {error}")))?;
        let mut wal = self
            .resolution_wal
            .lock()
            .map_err(|_| RymeError::Internal(String::from("transaction resolution wal")))?;
        if let Err(error) = wal.append(commit_ts, &payload).and_then(|_| wal.sync()) {
            let isolate = is_wal_storage_failure(&error);
            drop(wal);
            drop(pending);
            if isolate {
                self.set_isolated(true);
            }
            return Err(error);
        }
        pending.insert(
            key,
            PendingResolution {
                transaction_id,
                commit_ts,
                participant,
                committed,
                group_id: group_id.to_string(),
            },
        );
        Ok(())
    }

    fn complete_resolution(&self, resolution: &PendingResolution) -> Result<()> {
        let key = (
            resolution.group_id.clone(),
            resolution.transaction_id,
            resolution.commit_ts,
            resolution.participant,
        );
        let mut pending = self
            .pending_resolutions
            .lock()
            .map_err(|_| RymeError::Internal(String::from("transaction resolutions")))?;
        if !pending.contains_key(&key) {
            return Ok(());
        }
        let record = ResolutionRecord::Complete {
            transaction_id: resolution.transaction_id,
            commit_ts: resolution.commit_ts,
            participant: resolution.participant,
            group_id: resolution.group_id.clone(),
        };
        let payload = serde_json::to_vec(&record)
            .map_err(|error| RymeError::Internal(format!("transaction resolution: {error}")))?;
        let mut wal = self
            .resolution_wal
            .lock()
            .map_err(|_| RymeError::Internal(String::from("transaction resolution wal")))?;
        if let Err(error) = wal.append(resolution.commit_ts, &payload).and_then(|_| wal.sync()) {
            let isolate = is_wal_storage_failure(&error);
            drop(wal);
            drop(pending);
            if isolate {
                self.set_isolated(true);
            }
            return Err(error);
        }
        pending.remove(&key);
        Ok(())
    }

    fn try_pending_resolutions_snapshot(&self) -> Result<Vec<PendingResolution>> {
        self.pending_resolutions
            .lock()
            .map(|pending| pending.values().cloned().collect())
            .map_err(|_| RymeError::Internal(String::from("transaction resolutions lock")))
    }

    #[cfg(test)]
    fn pending_resolutions_snapshot(&self) -> Vec<PendingResolution> {
        self.try_pending_resolutions_snapshot().expect("transaction resolutions lock")
    }

    fn prepare_intent(
        &self,
        transaction_id: u128,
        commit_ts: u64,
        state: ryme_txn::TransactionState,
        txn: &ryme_txn::Transaction,
    ) -> Result<()> {
        if let Some(expected) = state.route_generation {
            if self.range_generation() != expected {
                return Err(RymeError::Conflict(String::from("range topology changed")));
            }
        }
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| RymeError::Internal(String::from("prepared transactions")))?;
        if let Some(existing) = prepared.get(&transaction_id) {
            if existing.commit_ts == commit_ts && existing.state == state {
                return Ok(());
            }
            return Err(RymeError::Conflict(String::from("transaction already prepared")));
        }
        for existing in prepared.values() {
            let other = self.manager.from_state(existing.state.clone())?;
            if transactions_conflict(txn, &other) {
                return Err(RymeError::Conflict(String::from("prepared transaction conflict")));
            }
        }
        self.manager.validate_at(txn, commit_ts)?;
        self.append_intent_record(
            &IntentRecord::Prepare { transaction_id, commit_ts, state: state.clone() },
            commit_ts,
        )?;
        prepared.insert(transaction_id, PreparedIntent { commit_ts, state });
        Ok(())
    }

    fn reject_prepared_conflict(&self, txn: &ryme_txn::Transaction) -> Result<()> {
        {
            let prepared = self
                .prepared
                .lock()
                .map_err(|_| RymeError::Internal(String::from("prepared transactions")))?;
            for existing in prepared.values() {
                let other = self.manager.from_state(existing.state.clone())?;
                if transactions_conflict(txn, &other) {
                    return Err(RymeError::Conflict(String::from("prepared transaction conflict")));
                }
            }
        }
        let range_prepared = self
            .range_prepared
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range prepared transactions")))?;
        for existing in range_prepared.values() {
            let other = self.manager.from_state(existing.state.clone())?;
            if transactions_conflict(txn, &other) {
                return Err(RymeError::Conflict(String::from("prepared transaction conflict")));
            }
        }
        Ok(())
    }

    fn range_prepare_conflict(
        &self,
        group_id: &str,
        transaction_id: u128,
        commit_ts: u64,
        txn: &ryme_txn::Transaction,
        state: &ryme_txn::TransactionState,
    ) -> Result<()> {
        let prepared = self
            .range_prepared
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range prepared transactions")))?;
        if let Some(existing) = prepared.get(&(group_id.to_string(), transaction_id)) {
            if existing.commit_ts == commit_ts && existing.state == *state {
                return Ok(());
            }
            return Err(RymeError::Conflict(String::from("transaction already prepared")));
        }
        for ((existing_group, _), existing) in prepared.iter() {
            if existing_group != group_id {
                continue;
            }
            let other = self.manager.from_state(existing.state.clone())?;
            if transactions_conflict(txn, &other) {
                return Err(RymeError::Conflict(String::from("prepared transaction conflict")));
            }
        }
        Ok(())
    }

    async fn prepare_range_group(
        self: &Arc<Self>,
        group_id: &str,
        transaction_id: u128,
        commit_ts: u64,
        state: ryme_txn::TransactionState,
    ) -> Result<()> {
        let range = self
            .try_range_owner_by_id(group_id)?
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.owner != self.id {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        if self.try_range_resolution(group_id, transaction_id, commit_ts)? == Some(false) {
            return Err(RymeError::Conflict(String::from("transaction already aborted")));
        }
        let txn = self.manager.from_state(state.clone())?;
        if let Some(expected) = state.route_generation {
            if self.range_generation() != expected {
                return Err(RymeError::Conflict(String::from("range topology changed")));
            }
        }
        self.manager.validate_at(&txn, commit_ts)?;
        self.range_prepare_conflict(group_id, transaction_id, commit_ts, &txn, &state)?;
        let mut local_state = state.clone();
        let range_owners = self.try_range_owners()?;
        let mut retained_writes = Vec::with_capacity(local_state.writes.len());
        for (key, write) in std::mem::take(&mut local_state.writes) {
            if self
                .try_range_for_record_in(&range_owners, &key)?
                .map(|range| range.id == group_id)
                .unwrap_or(true)
            {
                retained_writes.push((key, write));
            }
        }
        local_state.writes = retained_writes;
        let encoded = serde_json::to_vec(&local_state)
            .map_err(|error| RymeError::Internal(format!("range transaction prepare: {error}")))?;
        self.commit_range_record(
            group_id,
            commit_ts,
            RangeEntryKind::Prepare { transaction_id, state: encoded },
        )
        .await
    }

    async fn resolve_range_group(
        self: &Arc<Self>,
        group_id: &str,
        transaction_id: u128,
        commit_ts: u64,
        committed: bool,
    ) -> Result<()> {
        let range = self
            .try_range_owner_by_id(group_id)?
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.owner != self.id {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        if let Some(previous) = self.try_range_resolution(group_id, transaction_id, commit_ts)? {
            if previous == committed {
                return Ok(());
            }
            return Err(RymeError::Conflict(String::from("transaction resolution changed")));
        }
        let existing = self
            .range_prepared
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range prepared transactions")))?
            .get(&(group_id.to_string(), transaction_id))
            .cloned();
        let Some(existing) = existing else {
            if committed {
                return Ok(());
            }
            return self
                .commit_range_record(
                    group_id,
                    commit_ts,
                    RangeEntryKind::Resolve { transaction_id, committed },
                )
                .await;
        };
        if existing.commit_ts != commit_ts {
            return Err(RymeError::Conflict(String::from("transaction timestamp changed")));
        }
        self.commit_range_record(
            group_id,
            commit_ts,
            RangeEntryKind::Resolve { transaction_id, committed },
        )
        .await
    }

    async fn record_range_decision(
        self: &Arc<Self>,
        group_id: &str,
        transaction_id: u128,
        commit_ts: u64,
        committed: bool,
        participants: Vec<ResolutionParticipant>,
    ) -> Result<()> {
        let key = (group_id.to_string(), transaction_id, commit_ts);
        if let Some(existing) = self
            .range_decisions
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range decisions")))?
            .get(&key)
            .cloned()
        {
            if existing.committed == committed && existing.participants == participants {
                return Ok(());
            }
            return Err(RymeError::Conflict(String::from("transaction decision changed")));
        }
        let range = self
            .try_range_owner_by_id(group_id)?
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.owner != self.id {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        self.commit_range_record(
            group_id,
            commit_ts,
            RangeEntryKind::Decision { transaction_id, committed, participants },
        )
        .await
    }

    async fn record_range_coordination(
        self: &Arc<Self>,
        group_id: &str,
        transaction_id: u128,
        commit_ts: u64,
        participants: Vec<ResolutionParticipant>,
    ) -> Result<()> {
        let range = self
            .try_range_owner_by_id(group_id)?
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.owner != self.id {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        self.commit_range_record(
            group_id,
            commit_ts,
            RangeEntryKind::Coordinate { transaction_id, participants },
        )
        .await
    }

    async fn record_range_decision_on(
        self: &Arc<Self>,
        group_id: &str,
        owner: usize,
        transaction_id: u128,
        commit_ts: u64,
        committed: bool,
        participants: Vec<ResolutionParticipant>,
    ) -> Result<()> {
        for attempt in 0..3 {
            let owner = self.try_range_owner_by_id(group_id)?.map_or(owner, |range| range.owner);
            let result = if owner == self.id {
                self.record_range_decision(
                    group_id,
                    transaction_id,
                    commit_ts,
                    committed,
                    participants.clone(),
                )
                .await
            } else {
                match self.pool_for(owner) {
                    Some(pool) => match pool
                        .roundtrip(&Rpc::RangeDecisionTransaction {
                            group_id: group_id.to_string(),
                            transaction_id,
                            commit_ts,
                            committed,
                            participants: participants.clone(),
                        })
                        .await
                    {
                        Ok(Rpc::RangeDecisionTransactionResponse {
                            recorded: true,
                            error: None,
                        }) => Ok(()),
                        Ok(Rpc::RangeDecisionTransactionResponse {
                            error: Some(error), ..
                        }) => Err(error.into()),
                        Ok(_) => Err(RymeError::Corrupt(String::from("range decision rpc"))),
                        Err(error) => Err(error),
                    },
                    None => {
                        Err(RymeError::Unavailable(String::from("range participant unavailable")))
                    }
                }
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) if attempt < 2 && matches!(&error, RymeError::Unavailable(_)) => {
                    tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(RymeError::Unavailable(String::from("range participant unavailable")))
    }

    async fn record_range_coordination_on(
        self: &Arc<Self>,
        group_id: &str,
        owner: usize,
        transaction_id: u128,
        commit_ts: u64,
        participants: Vec<ResolutionParticipant>,
    ) -> Result<()> {
        for attempt in 0..3 {
            let owner = self.try_range_owner_by_id(group_id)?.map_or(owner, |range| range.owner);
            let result = if owner == self.id {
                self.record_range_coordination(
                    group_id,
                    transaction_id,
                    commit_ts,
                    participants.clone(),
                )
                .await
            } else {
                match self.pool_for(owner) {
                    Some(pool) => match pool
                        .roundtrip(&Rpc::RangeCoordinateTransaction {
                            group_id: group_id.to_string(),
                            transaction_id,
                            commit_ts,
                            participants: participants.clone(),
                        })
                        .await
                    {
                        Ok(Rpc::RangeCoordinateTransactionResponse {
                            recorded: true,
                            error: None,
                        }) => Ok(()),
                        Ok(Rpc::RangeCoordinateTransactionResponse {
                            error: Some(error), ..
                        }) => Err(error.into()),
                        Ok(_) => Err(RymeError::Corrupt(String::from("range coordination rpc"))),
                        Err(error) => Err(error),
                    },
                    None => {
                        Err(RymeError::Unavailable(String::from("range participant unavailable")))
                    }
                }
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) if attempt < 2 && matches!(&error, RymeError::Unavailable(_)) => {
                    tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(RymeError::Unavailable(String::from("range participant unavailable")))
    }

    fn resolve_intent(&self, transaction_id: u128, commit_ts: u64, committed: bool) -> Result<()> {
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| RymeError::Internal(String::from("prepared transactions")))?;
        let Some(existing) = prepared.get(&transaction_id) else {
            return Ok(());
        };
        if existing.commit_ts != commit_ts {
            return Err(RymeError::Conflict(String::from("transaction timestamp changed")));
        }
        self.append_intent_record(
            &IntentRecord::Resolve { transaction_id, commit_ts, committed },
            commit_ts,
        )?;
        prepared.remove(&transaction_id);
        Ok(())
    }

    fn clear_prepared_for_commit(&self, commit_ts: u64) -> Result<()> {
        let ids: Vec<u128> = self
            .prepared
            .lock()
            .map_err(|_| RymeError::Internal(String::from("prepared transactions")))?
            .iter()
            .filter_map(|(id, intent)| (intent.commit_ts == commit_ts).then_some(*id))
            .collect();
        for id in ids {
            self.resolve_intent(id, commit_ts, true)?;
        }
        Ok(())
    }

    /// Returns the configured key ranges or an error if range metadata is unavailable.
    pub fn try_range_owners(&self) -> Result<Vec<RangeOwner>> {
        self.range_owners
            .read()
            .map(|owners| owners.clone())
            .map_err(|_| RymeError::Internal(String::from("range ownership metadata poisoned")))
    }

    fn try_range_for_record_in(
        &self,
        owners: &[RangeOwner],
        key: &RecordKey,
    ) -> Result<Option<RangeOwner>> {
        let Some(version) = owners.first().map(|range| range.routing_version) else {
            return Ok(None);
        };
        let routing = ryme_storage::record_routing_key(version, key)?;
        owners
            .iter()
            .find(|range| {
                routing.as_slice() >= range.start.as_slice()
                    && (range.end.is_empty() || routing.as_slice() < range.end.as_slice())
            })
            .cloned()
            .map(|range| self.try_with_current_range_leader(range))
            .transpose()
    }

    fn try_range_owner_key_in(
        &self,
        owners: &[RangeOwner],
        key: &RecordKey,
    ) -> Result<Option<usize>> {
        Ok(self.try_range_for_record_in(owners, key)?.map(|range| range.owner))
    }

    fn try_with_current_range_leader(&self, mut range: RangeOwner) -> Result<RangeOwner> {
        let leaders = self
            .range_leaders
            .read()
            .map_err(|_| RymeError::Internal(String::from("range leader metadata lock")))?;
        if let Some(leader) = leaders.get(&range.id).copied() {
            range.owner = leader;
        }
        Ok(range)
    }

    fn try_range_last_index(&self, group_id: &str) -> Result<u64> {
        let logs = self
            .range_logs
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range log lock")))?;
        Ok(logs.get(group_id).copied().unwrap_or(0))
    }

    fn try_range_last_term(&self, group_id: &str) -> Result<u64> {
        self.try_range_last_position(group_id).map(|(_, term)| term)
    }

    fn try_range_last_position(&self, group_id: &str) -> Result<(u64, u64)> {
        let logs = self
            .range_logs
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range log lock")))?;
        let last = logs.get(group_id).copied().unwrap_or(0);
        if last == 0 {
            return Ok((0, 0));
        }
        let entries = self
            .range_entries
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range entries lock")))?;
        let term = entries
            .get(group_id)
            .and_then(|group| group.get(&last))
            .map(|record| record.term)
            .ok_or_else(|| RymeError::Corrupt(String::from("range log gap")))?;
        Ok((last, term))
    }

    fn try_range_entry(&self, group_id: &str, index: u64) -> Result<Option<RangeLogRecord>> {
        let entries = self
            .range_entries
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range entries lock")))?;
        Ok(entries.get(group_id).and_then(|group| group.get(&index).cloned()))
    }

    fn try_range_resolution(
        &self,
        group_id: &str,
        transaction_id: u128,
        commit_ts: u64,
    ) -> Result<Option<bool>> {
        let entries = self
            .range_entries
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range entries lock")))?;
        Ok(entries.get(group_id).into_iter().flat_map(|group| group.values()).find_map(|record| {
            if record.commit_ts != commit_ts {
                return None;
            }
            match &record.kind {
                RangeEntryKind::Resolve { transaction_id: id, committed }
                    if *id == transaction_id =>
                {
                    Some(*committed)
                }
                _ => None,
            }
        }))
    }

    #[cfg(test)]
    fn range_last_index(&self, group_id: &str) -> u64 {
        self.try_range_last_index(group_id).unwrap()
    }

    #[cfg(test)]
    fn range_last_term(&self, group_id: &str) -> u64 {
        self.try_range_last_term(group_id).unwrap()
    }

    #[cfg(test)]
    fn range_entry(&self, group_id: &str, index: u64) -> Option<RangeLogRecord> {
        self.try_range_entry(group_id, index).unwrap()
    }

    #[cfg(test)]
    fn range_resolution(
        &self,
        group_id: &str,
        transaction_id: u128,
        commit_ts: u64,
    ) -> Option<bool> {
        self.try_range_resolution(group_id, transaction_id, commit_ts).unwrap()
    }

    fn try_ensure_range_group(&self, group_id: &str, leader: usize) -> Result<RangeGroupState> {
        let mut groups = self
            .range_groups
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
        let state = groups.entry(group_id.to_string()).or_default();
        if state.term == 0 {
            state.term = 1;
        }
        if state.leader.is_none() {
            state.leader = Some(leader);
        }
        Ok(state.clone())
    }

    fn try_range_term(&self, group_id: &str) -> Result<u64> {
        let groups = self
            .range_groups
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
        Ok(groups.get(group_id).map(|state| state.term).unwrap_or(1).max(1))
    }

    fn persist_range_meta(&self) -> Result<()> {
        let _persist = self
            .range_meta_persist_lock
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata persist lock")))?;
        let states = self
            .range_groups
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?
            .clone();
        let raw = serde_json::to_vec(&states)
            .map_err(|error| RymeError::Internal(format!("range metadata: {error}")))?;
        if raw.len() > MAX_RAFT_METADATA_BYTES {
            return Err(RymeError::Overload(String::from("range metadata")));
        }
        persist_metadata_file(&self.range_meta_path, &raw)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_range_entry(
        &self,
        group_id: &str,
        index: u64,
        term: u64,
        commit_ts: u64,
        encoded_writes: &[u8],
        publish_data: bool,
        kind: RangeEntryKind,
    ) -> Result<u64> {
        self.ensure_not_isolated()?;
        decode_writes(encoded_writes)?;
        if let RangeEntryKind::Prepare { state, .. } = &kind {
            let _: ryme_txn::TransactionState = serde_json::from_slice(state).map_err(|error| {
                RymeError::Corrupt(format!("range transaction prepare: {error}"))
            })?;
        }
        let mut logs =
            self.range_logs.lock().map_err(|_| RymeError::Internal(String::from("range log")))?;
        let last = logs.get(group_id).copied().unwrap_or(0);
        if index <= last {
            return Ok(last);
        }
        let next_index = last
            .checked_add(1)
            .ok_or_else(|| RymeError::Corrupt(String::from("range log index overflow")))?;
        if index != next_index {
            return Err(RymeError::Unavailable(String::from("range log gap")));
        }
        let record = serde_json::to_vec(&RangeLogRecord {
            group_id: group_id.to_string(),
            index,
            term,
            commit_ts,
            writes: encoded_writes.to_vec(),
            publish_data,
            kind: kind.clone(),
        })
        .map_err(|error| RymeError::Internal(format!("range log: {error}")))?;
        let mut wal =
            self.range_wal.lock().map_err(|_| RymeError::Internal(String::from("range wal")))?;
        let mut entries = self
            .range_entries
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range entries")))?;
        if let Err(error) = wal.append(commit_ts, &record).and_then(|_| wal.sync()) {
            let isolate = is_wal_storage_failure(&error);
            drop(entries);
            drop(wal);
            drop(logs);
            if isolate {
                self.set_isolated(true);
            }
            return Err(error);
        }
        entries.entry(group_id.to_string()).or_default().insert(
            index,
            RangeLogRecord {
                group_id: group_id.to_string(),
                index,
                term,
                commit_ts,
                writes: encoded_writes.to_vec(),
                publish_data,
                kind,
            },
        );
        logs.insert(group_id.to_string(), index);
        Ok(index)
    }

    fn truncate_range_suffix(&self, group_id: &str, keep_through: u64) -> Result<()> {
        let state = self
            .range_groups
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?
            .get(group_id)
            .cloned()
            .unwrap_or_default();
        if state.commit_index > keep_through || state.applied_index > keep_through {
            return Err(RymeError::Conflict(String::from("committed range suffix")));
        }

        let mut logs =
            self.range_logs.lock().map_err(|_| RymeError::Internal(String::from("range log")))?;
        let mut wal =
            self.range_wal.lock().map_err(|_| RymeError::Internal(String::from("range wal")))?;
        let mut entries = self
            .range_entries
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range entries")))?;
        let replacement = (|| -> Result<(usize, Vec<WalRecord>)> {
            let records = Wal::read_all(&self.range_wal_path)?;
            let mut kept = Vec::with_capacity(records.len());
            for record in &records {
                let range_record: RangeLogRecord = serde_json::from_slice(&record.payload)
                    .map_err(|error| RymeError::Corrupt(format!("range log: {error}")))?;
                if range_record.group_id != group_id || range_record.index <= keep_through {
                    kept.push(record.clone());
                }
            }
            Ok((records.len(), kept))
        })();
        let (record_count, kept) = match replacement {
            Ok(replacement) => replacement,
            Err(error) => {
                drop(entries);
                drop(wal);
                drop(logs);
                self.set_isolated(true);
                return Err(error);
            }
        };
        if kept.len() != record_count {
            if let Err(error) = wal.replace_records(&kept) {
                drop(entries);
                drop(wal);
                drop(logs);
                self.set_isolated(true);
                return Err(error);
            }
        }
        if let Some(group) = entries.get_mut(group_id) {
            group.retain(|index, _| *index <= keep_through);
            if group.is_empty() {
                entries.remove(group_id);
            }
        }
        if keep_through == 0 {
            logs.remove(group_id);
        } else {
            logs.insert(group_id.to_string(), keep_through);
        }
        Ok(())
    }

    fn apply_range_committed(&self, group_id: &str, commit_index: u64) -> Result<()> {
        let result = self.apply_range_committed_inner(group_id, commit_index);
        if result.is_err() {
            self.set_isolated(true);
        }
        result
    }

    fn apply_range_committed_inner(&self, group_id: &str, commit_index: u64) -> Result<()> {
        let (start, end) = {
            let mut groups = self
                .range_groups
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
            let state = groups.entry(group_id.to_string()).or_default();
            state.commit_index = state.commit_index.max(commit_index);
            if state.commit_index <= state.applied_index {
                return Ok(());
            }
            (state.applied_index + 1, state.commit_index)
        };
        let records: Vec<RangeLogRecord> = {
            let entries = self
                .range_entries
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range entries")))?;
            let group = entries
                .get(group_id)
                .ok_or_else(|| RymeError::Unavailable(String::from("range log gap")))?;
            (start..=end)
                .map(|index| {
                    group
                        .get(&index)
                        .cloned()
                        .ok_or_else(|| RymeError::Unavailable(String::from("range log gap")))
                })
                .collect::<Result<Vec<_>>>()?
        };
        for record in records {
            match record.kind {
                RangeEntryKind::Data => {
                    let writes = decode_writes(&record.writes)?;
                    self.manager.replay_at(record.commit_ts, &writes)?;
                    if record.publish_data {
                        self.apply_data_hook(crate::encode_applied(
                            record.commit_ts,
                            &record.writes,
                        ))?;
                    }
                }
                RangeEntryKind::Prepare { transaction_id, state } => {
                    let state: ryme_txn::TransactionState = serde_json::from_slice(&state)
                        .map_err(|error| {
                            RymeError::Corrupt(format!("range transaction prepare: {error}"))
                        })?;
                    self.range_prepared
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("range prepared")))?
                        .insert(
                            (group_id.to_string(), transaction_id),
                            PreparedIntent { commit_ts: record.commit_ts, state },
                        );
                }
                RangeEntryKind::Resolve { transaction_id, committed } => {
                    let existing = self
                        .range_prepared
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("range prepared")))?
                        .remove(&(group_id.to_string(), transaction_id));
                    if committed {
                        if let Some(existing) = existing {
                            let writes =
                                existing.state.writes.into_iter().collect::<BTreeMap<_, _>>();
                            if !writes.is_empty() {
                                self.manager.replay_at(record.commit_ts, &writes)?;
                                let encoded_writes = encode_writes(&writes)?;
                                let is_owner = self
                                    .try_range_owner_by_id(group_id)?
                                    .is_some_and(|range| range.owner == self.id);
                                if is_owner {
                                    self.apply_data_hook(crate::encode_applied(
                                        record.commit_ts,
                                        &encoded_writes,
                                    ))?;
                                }
                            }
                        }
                    }
                }
                RangeEntryKind::Decision { transaction_id, committed, participants } => {
                    self.range_recovery_coordinations
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("range recoveries")))?
                        .remove(&(group_id.to_string(), transaction_id, record.commit_ts));
                    self.range_decisions
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("range decisions")))?
                        .insert(
                            (group_id.to_string(), transaction_id, record.commit_ts),
                            PendingRangeDecision {
                                group_id: group_id.to_string(),
                                transaction_id,
                                commit_ts: record.commit_ts,
                                committed,
                                participants,
                            },
                        );
                }
                RangeEntryKind::Coordinate { .. } => {}
            }
        }
        {
            let mut groups = self
                .range_groups
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
            if let Some(state) = groups.get_mut(group_id) {
                state.applied_index = state.applied_index.max(end);
            }
        }
        self.persist_range_meta()
    }

    #[allow(clippy::too_many_arguments)]
    fn range_conflict_index(
        &self,
        group_id: &str,
        prev_index: u64,
        prev_term: u64,
        index: u64,
        term: u64,
        commit_ts: u64,
        encoded_writes: &[u8],
        publish_data: bool,
        kind: &RangeEntryKind,
    ) -> Result<Option<u64>> {
        let last = self.try_range_last_index(group_id)?;
        if prev_index > last {
            return Ok(Some(last.saturating_add(1)));
        }
        if prev_index > 0 {
            let previous = self
                .try_range_entry(group_id, prev_index)?
                .ok_or_else(|| RymeError::Corrupt(String::from("range log gap")))?;
            if previous.term != prev_term {
                return Ok(Some(prev_index));
            }
        }
        if index > 0 && index <= last {
            let existing = self
                .try_range_entry(group_id, index)?
                .ok_or_else(|| RymeError::Corrupt(String::from("range log gap")))?;
            if existing.term != term
                || existing.commit_ts != commit_ts
                || existing.writes != encoded_writes
                || existing.publish_data != publish_data
                || existing.kind != *kind
            {
                return Ok(Some(index));
            }
        }
        if index > last.saturating_add(1) {
            Ok(Some(last.saturating_add(1)))
        } else {
            Ok(None)
        }
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn append_range_follower(
        &self,
        group_id: &str,
        term: u64,
        leader: usize,
        prev_index: u64,
        prev_term: u64,
        index: u64,
        commit_ts: u64,
        encoded_writes: &[u8],
        leader_commit: u64,
    ) -> Result<u64> {
        self.append_range_follower_marked(
            group_id,
            term,
            leader,
            prev_index,
            prev_term,
            index,
            commit_ts,
            encoded_writes,
            leader_commit,
            false,
            RangeEntryKind::Data,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn append_range_follower_marked(
        &self,
        group_id: &str,
        term: u64,
        leader: usize,
        prev_index: u64,
        prev_term: u64,
        index: u64,
        commit_ts: u64,
        encoded_writes: &[u8],
        leader_commit: u64,
        publish_data: bool,
        kind: RangeEntryKind,
    ) -> Result<u64> {
        if term == 0 {
            return Err(RymeError::InvalidArgument(String::from("range term")));
        }
        let Some(range) = self.try_range_owner_by_id(group_id)? else {
            return Err(RymeError::NotFound(String::from("range group")));
        };
        if !range.replicas.contains(&self.id) || !range.replicas.contains(&leader) {
            return Err(RymeError::Unavailable(String::from("range replica")));
        }
        if prev_index == 0 && prev_term != 0 {
            return Err(RymeError::InvalidArgument(String::from("range log prefix")));
        }
        let mut state = self.try_ensure_range_group(group_id, leader)?;
        if term < state.term {
            return Err(RymeError::Unavailable(String::from("stale range term")));
        }
        if term == state.term && state.leader.is_some() && state.leader != Some(leader) {
            return Err(RymeError::Unavailable(String::from("range leader conflict")));
        }
        if term > state.term || state.leader != Some(leader) {
            state.term = term;
            state.voted_for = None;
            state.leader = Some(leader);
            self.range_groups
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?
                .insert(group_id.to_string(), state.clone());
            self.persist_range_meta()?;
        }
        let mut groups = self
            .range_groups
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
        let current = groups
            .get_mut(group_id)
            .ok_or_else(|| RymeError::Unavailable(String::from("range metadata unavailable")))?;
        current.last_contact_ms = now_ms();
        drop(groups);
        self.range_leaders
            .write()
            .map_err(|_| RymeError::Internal(String::from("range leader metadata lock")))?
            .insert(group_id.to_string(), leader);
        let mut last = self.try_range_last_index(group_id)?;
        if prev_index > last {
            return Err(RymeError::Unavailable(String::from("range log gap")));
        }
        if prev_index != 0 {
            let previous = self
                .try_range_entry(group_id, prev_index)?
                .ok_or_else(|| RymeError::Unavailable(String::from("range log gap")))?;
            if previous.term != prev_term {
                return Err(RymeError::Unavailable(String::from("range log conflict")));
            }
        }
        if index == 0 {
            if last > prev_index {
                self.truncate_range_suffix(group_id, prev_index)?;
                last = prev_index;
            }
        } else {
            if index <= last {
                let existing = self
                    .try_range_entry(group_id, index)?
                    .ok_or_else(|| RymeError::Unavailable(String::from("range log gap")))?;
                if existing.term == term
                    && existing.commit_ts == commit_ts
                    && existing.writes == encoded_writes
                    && existing.publish_data == publish_data
                    && existing.kind == kind
                {
                    if leader_commit > 0 {
                        self.apply_range_committed(group_id, leader_commit.min(last))?;
                    }
                    return Ok(last);
                }
                if existing.term != term
                    || existing.commit_ts != commit_ts
                    || existing.writes != encoded_writes
                    || existing.publish_data != publish_data
                    || existing.kind != kind
                {
                    self.truncate_range_suffix(group_id, index - 1)?;
                    last = index - 1;
                }
            }
            let next_index = last
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("range log index overflow")))?;
            if index != next_index {
                return Err(RymeError::Unavailable(String::from("range log gap")));
            }
            last = self.append_range_entry(
                group_id,
                index,
                term,
                commit_ts,
                encoded_writes,
                publish_data,
                kind,
            )?;
        }
        if leader_commit > 0 {
            self.apply_range_committed(group_id, leader_commit.min(last))?;
        }
        Ok(last)
    }

    fn try_range_owner_by_id(&self, group_id: &str) -> Result<Option<RangeOwner>> {
        let Some(range) = self.try_range_owners()?.into_iter().find(|range| range.id == group_id)
        else {
            return Ok(None);
        };
        self.try_with_current_range_leader(range).map(Some)
    }

    async fn replicate_range_record(
        self: &Arc<Self>,
        group_id: &str,
        commit_ts: u64,
        encoded_writes: &[u8],
        publish_data: bool,
        kind: RangeEntryKind,
    ) -> Result<(u64, u64, Vec<usize>)> {
        let range = self
            .try_range_owner_by_id(group_id)?
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.owner != self.id {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        let mut replicas = range.replicas;
        if replicas.is_empty() {
            replicas.push(range.owner);
        }
        if !replicas.contains(&self.id) {
            return Err(RymeError::Unavailable(String::from("range replica")));
        }
        replicas.sort_unstable();
        replicas.dedup();
        let index = self
            .try_range_last_index(group_id)?
            .checked_add(1)
            .ok_or_else(|| RymeError::Corrupt(String::from("range log index overflow")))?;
        let term = self.try_ensure_range_group(group_id, self.id)?.term.max(1);
        self.append_range_entry(
            group_id,
            index,
            term,
            commit_ts,
            encoded_writes,
            publish_data,
            kind,
        )?;
        let mut acknowledgements = 1usize;
        let mut acknowledged = vec![self.id];
        for replica in replicas.iter().copied().filter(|replica| *replica != self.id) {
            if self.sync_range_replica(group_id, replica, term, index, term, 0).await.is_ok() {
                acknowledgements += 1;
                acknowledged.push(replica);
            }
        }
        if acknowledgements < replicas.len() / 2 + 1 {
            self.abort_range_entry(group_id, index, &replicas).await?;
            return Err(RymeError::Unavailable(String::from("range quorum")));
        }
        Ok((index, term, acknowledged))
    }

    async fn replicate_range_entry(
        self: &Arc<Self>,
        group_id: &str,
        commit_ts: u64,
        encoded_writes: &[u8],
        publish_data: bool,
    ) -> Result<(u64, u64, Vec<usize>)> {
        self.replicate_range_record(
            group_id,
            commit_ts,
            encoded_writes,
            publish_data,
            RangeEntryKind::Data,
        )
        .await
    }

    async fn commit_range_entry(
        self: &Arc<Self>,
        group_id: &str,
        index: u64,
        term: u64,
        acknowledged: &[usize],
    ) -> Result<()> {
        {
            let mut groups = self
                .range_groups
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?;
            let state = groups.entry(group_id.to_string()).or_default();
            state.term = state.term.max(term).max(1);
            state.leader = Some(self.id);
            state.commit_index = state.commit_index.max(index);
        }
        self.apply_range_committed(group_id, index)?;
        for replica in acknowledged.iter().copied().filter(|replica| *replica != self.id) {
            let _ = self.sync_range_replica(group_id, replica, term, index, term, index).await;
        }
        Ok(())
    }

    async fn abort_range_entry(
        self: &Arc<Self>,
        group_id: &str,
        index: u64,
        replicas: &[usize],
    ) -> Result<()> {
        let keep_through = index.saturating_sub(1);
        self.truncate_range_suffix(group_id, keep_through)?;
        let last_term = if keep_through == 0 { 0 } else { self.try_range_last_term(group_id)? };
        let commit_index = self
            .range_groups
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?
            .get(group_id)
            .map(|state| state.commit_index)
            .unwrap_or(0);
        for replica in replicas.iter().copied().filter(|replica| *replica != self.id) {
            let _ = self
                .sync_range_replica(
                    group_id,
                    replica,
                    self.try_range_term(group_id)?,
                    keep_through,
                    last_term,
                    commit_index,
                )
                .await;
        }
        Ok(())
    }

    async fn commit_range_record(
        self: &Arc<Self>,
        group_id: &str,
        commit_ts: u64,
        kind: RangeEntryKind,
    ) -> Result<()> {
        let empty_writes = encode_writes(&BTreeMap::new())?;
        let (index, term, acknowledged) =
            self.replicate_range_record(group_id, commit_ts, &empty_writes, false, kind).await?;
        self.commit_range_entry(group_id, index, term, &acknowledged).await
    }

    async fn replicate_range_group(
        self: &Arc<Self>,
        group_id: &str,
        commit_ts: u64,
        encoded_writes: &[u8],
    ) -> Result<()> {
        let (index, term, acknowledged) =
            self.replicate_range_entry(group_id, commit_ts, encoded_writes, false).await?;
        self.commit_range_entry(group_id, index, term, &acknowledged).await
    }

    async fn commit_range_group(
        self: &Arc<Self>,
        group_id: &str,
        commit_ts: u64,
        encoded_writes: &[u8],
    ) -> Result<()> {
        self.replicate_range_group(group_id, commit_ts, encoded_writes).await
    }

    async fn commit_range_write(
        self: &Arc<Self>,
        group_id: &str,
        encoded_writes: &[u8],
        route_generation: Option<u64>,
    ) -> Result<u64> {
        if let Some(expected) = route_generation {
            if self.range_generation() != expected {
                return Err(RymeError::Conflict(String::from("range topology changed")));
            }
        }
        let range = self
            .try_range_owner_by_id(group_id)?
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.owner != self.id {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        let writes = decode_writes(encoded_writes)?;
        if writes.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("writes")));
        }
        let _commit = self.txn_commit_lock.lock().await;
        let current = self
            .try_range_owner_by_id(group_id)?
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if current.owner != self.id || current.epoch != range.epoch {
            return Err(RymeError::Conflict(String::from("range topology changed")));
        }
        let mut txn = self.manager.begin();
        if let Some(generation) = route_generation {
            txn.set_route_generation(generation);
        }
        for (key, op) in &writes {
            match &op.value {
                Some(value) => {
                    self.manager.put_with_ttl(&mut txn, key.clone(), value.clone(), op.expires_at)
                }
                None => self.manager.delete(&mut txn, key.clone()),
            }
        }
        self.reject_prepared_conflict(&txn)?;
        let commit_ts = self.manager.reserve(&txn)?;
        let (index, term, acknowledged) =
            match self.replicate_range_entry(group_id, commit_ts, encoded_writes, true).await {
                Ok(result) => result,
                Err(error) => return Err(error),
            };
        if let Err(error) = self.manager.commit_filtered_at(&txn, commit_ts, |_| true) {
            self.abort_range_entry(group_id, index, &range.replicas).await?;
            return Err(error);
        }
        self.commit_range_entry(group_id, index, term, &acknowledged).await?;
        Ok(commit_ts)
    }

    async fn append_range_on(
        self: &Arc<Self>,
        owner: usize,
        group_id: &str,
        commit_ts: u64,
        encoded_writes: &[u8],
    ) -> Result<()> {
        if owner == self.id {
            return self.commit_range_group(group_id, commit_ts, encoded_writes).await;
        }
        let pool = self
            .pool_for(owner)
            .ok_or_else(|| RymeError::Unavailable(String::from("range leader unavailable")))?;
        match pool
            .roundtrip(&Rpc::RangeCommit {
                group_id: group_id.to_string(),
                commit_ts,
                writes: encoded_writes.to_vec(),
            })
            .await?
        {
            Rpc::RangeCommitResponse { committed: true, error: None } => Ok(()),
            Rpc::RangeCommitResponse { error: Some(error), .. } => Err(error.into()),
            _ => Err(RymeError::Corrupt(String::from("range commit rpc"))),
        }
    }

    async fn publish_range_writes(
        self: &Arc<Self>,
        commit_ts: u64,
        writes: &BTreeMap<RecordKey, ryme_txn::WriteOp>,
    ) -> Result<()> {
        let range_owners = self.try_range_owners()?;
        if range_owners.is_empty() {
            return Ok(());
        }
        let mut groups: BTreeMap<String, (usize, BTreeMap<RecordKey, ryme_txn::WriteOp>)> =
            BTreeMap::new();
        for (key, op) in writes {
            let Some(range) = self.try_range_for_record_in(&range_owners, key)? else { continue };
            groups
                .entry(range.id)
                .or_insert_with(|| (range.owner, BTreeMap::new()))
                .1
                .insert(key.clone(), op.clone());
        }
        for (group_id, (owner, writes)) in groups {
            let encoded = encode_writes(&writes)?;
            self.append_range_on(owner, &group_id, commit_ts, &encoded).await?;
        }
        Ok(())
    }

    fn try_transaction_participant_ranges(
        &self,
        txn: &ryme_txn::Transaction,
    ) -> Result<BTreeMap<String, RangeOwner>> {
        let ranges = self.try_range_owners()?;
        if ranges.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut participants = BTreeMap::new();
        let routing_version = ranges
            .first()
            .map(|range| range.routing_version)
            .unwrap_or(ryme_storage::LEGACY_ROUTING_VERSION);
        for key in txn.writes().keys().chain(txn.read_keys().iter()) {
            if let Some(range) = self.try_range_for_record_in(&ranges, key)? {
                participants.insert(range.id.clone(), range);
            }
        }
        for (tenant, database, table) in
            txn.scanned_tables().iter().chain(txn.constraint_scanned_tables().iter())
        {
            let (prefix, end) =
                ryme_storage::table_routing_bounds(routing_version, tenant, database, table)?;
            for range in &ranges {
                let starts_before_end = end.is_empty() || range.start.as_slice() < end.as_slice();
                let ends_after_start =
                    range.end.is_empty() || range.end.as_slice() > prefix.as_slice();
                if starts_before_end && ends_after_start {
                    let range = self.try_with_current_range_leader(range.clone())?;
                    participants.insert(range.id.clone(), range);
                }
            }
        }
        Ok(participants)
    }

    fn transaction_participant_ranges(
        &self,
        txn: &ryme_txn::Transaction,
    ) -> BTreeMap<String, RangeOwner> {
        self.try_transaction_participant_ranges(txn).unwrap_or_default()
    }

    pub fn transaction_participants(&self, txn: &ryme_txn::Transaction) -> BTreeSet<usize> {
        self.transaction_participant_ranges(txn).values().map(|range| range.owner).collect()
    }

    async fn prepare_on(
        self: &Arc<Self>,
        group_id: &str,
        owner: usize,
        transaction_id: u128,
        commit_ts: u64,
        state: ryme_txn::TransactionState,
    ) -> Result<()> {
        let payload = serde_json::to_vec(&state)
            .map_err(|error| RymeError::Internal(format!("transaction prepare: {error}")))?;
        for attempt in 0..3 {
            let owner = self.try_range_owner_by_id(group_id)?.map_or(owner, |range| range.owner);
            let result = if owner == self.id {
                self.prepare_range_group(group_id, transaction_id, commit_ts, state.clone()).await
            } else {
                match self.pool_for(owner) {
                    Some(pool) => match pool
                        .roundtrip(&Rpc::RangePrepareTransaction {
                            group_id: group_id.to_string(),
                            transaction_id,
                            commit_ts,
                            payload: payload.clone(),
                        })
                        .await
                    {
                        Ok(Rpc::RangePrepareTransactionResponse {
                            prepared: true,
                            error: None,
                        }) => Ok(()),
                        Ok(Rpc::RangePrepareTransactionResponse { error: Some(error), .. }) => {
                            Err(error.into())
                        }
                        Ok(_) => Err(RymeError::Corrupt(String::from("transaction prepare rpc"))),
                        Err(error) => Err(error),
                    },
                    None => {
                        Err(RymeError::Unavailable(String::from("range participant unavailable")))
                    }
                }
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) if attempt < 2 && matches!(&error, RymeError::Unavailable(_)) => {
                    tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(RymeError::Unavailable(String::from("range participant unavailable")))
    }

    async fn resolve_on(
        self: &Arc<Self>,
        group_id: &str,
        owner: usize,
        transaction_id: u128,
        commit_ts: u64,
        committed: bool,
    ) -> Result<()> {
        for attempt in 0..3 {
            let owner = self.try_range_owner_by_id(group_id)?.map_or(owner, |range| range.owner);
            let result = if owner == self.id {
                if group_id.is_empty() {
                    self.resolve_intent(transaction_id, commit_ts, committed)
                } else {
                    self.resolve_range_group(group_id, transaction_id, commit_ts, committed).await
                }
            } else {
                match self.pool_for(owner) {
                    Some(pool) => {
                        let response = if group_id.is_empty() {
                            pool.roundtrip(&Rpc::ResolveTransaction {
                                transaction_id,
                                commit_ts,
                                committed,
                            })
                            .await
                        } else {
                            pool.roundtrip(&Rpc::RangeResolveTransaction {
                                group_id: group_id.to_string(),
                                transaction_id,
                                commit_ts,
                                committed,
                            })
                            .await
                        };
                        match response {
                            Ok(Rpc::ResolveTransactionResponse { resolved: true, error: None })
                            | Ok(Rpc::RangeResolveTransactionResponse {
                                resolved: true,
                                error: None,
                            }) => Ok(()),
                            Ok(Rpc::ResolveTransactionResponse { error: Some(error), .. })
                            | Ok(Rpc::RangeResolveTransactionResponse {
                                error: Some(error), ..
                            }) => Err(error.into()),
                            Ok(_) => {
                                Err(RymeError::Corrupt(String::from("transaction resolve rpc")))
                            }
                            Err(error) => Err(error),
                        }
                    }
                    None => {
                        Err(RymeError::Unavailable(String::from("range participant unavailable")))
                    }
                }
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) if attempt < 2 && matches!(&error, RymeError::Unavailable(_)) => {
                    tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(RymeError::Unavailable(String::from("range participant unavailable")))
    }

    async fn retry_pending_resolutions(self: &Arc<Self>) {
        match self.try_pending_resolutions_snapshot() {
            Ok(resolutions) => {
                for resolution in resolutions {
                    if self
                        .resolve_on(
                            &resolution.group_id,
                            resolution.participant,
                            resolution.transaction_id,
                            resolution.commit_ts,
                            resolution.committed,
                        )
                        .await
                        .is_ok()
                    {
                        if let Err(error) = self.complete_resolution(&resolution) {
                            tracing::error!(
                                transaction_id = resolution.transaction_id,
                                commit_ts = resolution.commit_ts,
                                participant = resolution.participant,
                                %error,
                                "transaction resolution checkpoint failed"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                tracing::error!(%error, "pending transaction resolution state unavailable")
            }
        }
        self.recover_pending_range_coordinations().await;
        self.retry_pending_range_decisions().await;
    }

    fn try_pending_range_coordinations_snapshot(&self) -> Result<Vec<PendingRangeCoordination>> {
        self.range_recovery_coordinations
            .lock()
            .map(|coordinations| coordinations.values().cloned().collect())
            .map_err(|_| RymeError::Internal(String::from("range recovery coordination lock")))
    }

    async fn recover_pending_range_coordinations(self: &Arc<Self>) {
        let coordinations = match self.try_pending_range_coordinations_snapshot() {
            Ok(coordinations) => coordinations,
            Err(error) => {
                tracing::error!(%error, "pending range coordination state unavailable");
                return;
            }
        };
        for coordination in coordinations {
            let range = match self.try_range_owner_by_id(&coordination.group_id) {
                Ok(Some(range)) => range,
                Ok(None) => continue,
                Err(error) => {
                    tracing::error!(group_id = %coordination.group_id, %error, "range owner lookup failed");
                    continue;
                }
            };
            if range.owner != self.id {
                continue;
            }
            if let Err(error) = self
                .record_range_decision(
                    &coordination.group_id,
                    coordination.transaction_id,
                    coordination.commit_ts,
                    false,
                    coordination.participants.clone(),
                )
                .await
            {
                tracing::error!(
                    transaction_id = coordination.transaction_id,
                    commit_ts = coordination.commit_ts,
                    %error,
                    "range transaction recovery abort failed"
                );
            }
        }
    }

    fn try_pending_range_decisions_snapshot(&self) -> Result<Vec<PendingRangeDecision>> {
        self.range_decisions
            .lock()
            .map(|decisions| decisions.values().cloned().collect())
            .map_err(|_| RymeError::Internal(String::from("range decisions lock")))
    }

    async fn retry_pending_range_decisions(self: &Arc<Self>) {
        let decisions = match self.try_pending_range_decisions_snapshot() {
            Ok(decisions) => decisions,
            Err(error) => {
                tracing::error!(%error, "pending range decision state unavailable");
                return;
            }
        };
        for decision in decisions {
            let range = match self.try_range_owner_by_id(&decision.group_id) {
                Ok(Some(range)) => range,
                Ok(None) => continue,
                Err(error) => {
                    tracing::error!(group_id = %decision.group_id, %error, "range owner lookup failed");
                    continue;
                }
            };
            if range.owner != self.id {
                continue;
            }
            let mut complete = true;
            for participant in &decision.participants {
                if self
                    .resolve_on(
                        &participant.group_id,
                        participant.participant,
                        decision.transaction_id,
                        decision.commit_ts,
                        decision.committed,
                    )
                    .await
                    .is_err()
                {
                    complete = false;
                    let _ = self.queue_resolution_for_group(
                        &participant.group_id,
                        decision.transaction_id,
                        decision.commit_ts,
                        participant.participant,
                        decision.committed,
                    );
                }
            }
            if complete {
                match self.range_decisions.lock() {
                    Ok(mut decisions) => {
                        decisions.remove(&(
                            decision.group_id.clone(),
                            decision.transaction_id,
                            decision.commit_ts,
                        ));
                    }
                    Err(_) => {
                        tracing::error!(
                            group_id = %decision.group_id,
                            transaction_id = decision.transaction_id,
                            commit_ts = decision.commit_ts,
                            "range decision completion lock poisoned"
                        );
                    }
                }
            }
        }
    }

    fn queue_range_resolutions(
        &self,
        transaction_id: u128,
        commit_ts: u64,
        participants: &BTreeMap<String, RangeOwner>,
        committed: bool,
    ) -> Result<()> {
        for (group_id, range) in participants {
            let participant =
                self.try_range_owner_by_id(group_id)?.map_or(range.owner, |current| current.owner);
            self.queue_resolution_for_group(
                group_id,
                transaction_id,
                commit_ts,
                participant,
                committed,
            )?;
        }
        Ok(())
    }

    async fn commit_global_txn(self: &Arc<Self>, txn: ryme_txn::Transaction) -> Result<u64> {
        let writes = txn.writes().clone();
        let result = self
            .manager
            .commit_with(txn, |commit_ts, encoded| async move {
                self.replicate_frame(commit_ts, encoded).await.map(|_| ())
            })
            .await;
        if let Ok(commit_ts) = result {
            if let Err(error) = self.publish_range_writes(commit_ts, &writes).await {
                tracing::error!(commit_ts, %error, "range replica publish failed after global commit");
            }
        }
        result
    }

    async fn commit_distributed_txn(self: &Arc<Self>, txn: ryme_txn::Transaction) -> Result<u64> {
        if txn.writes().is_empty() {
            return Ok(txn.read_ts);
        }
        let participants = self.try_transaction_participant_ranges(&txn)?;
        if participants.len() < 2 {
            return self.commit_global_txn(txn).await;
        }
        let transaction_id = self.intent_id(&txn);
        let commit_ts = self.manager.reserve(&txn)?;
        let state = txn.state();
        let participant_ids = participants.clone();
        let decision_participants = participants
            .iter()
            .map(|(group_id, range)| {
                Ok(ResolutionParticipant {
                    participant: self
                        .try_range_owner_by_id(group_id)?
                        .map_or(range.owner, |current| current.owner),
                    group_id: group_id.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (coordinator_group, coordinator_owner) = decision_participants
            .first()
            .map(|participant| (participant.group_id.clone(), participant.participant))
            .ok_or_else(|| {
                RymeError::Internal(String::from("distributed transaction coordinator"))
            })?;
        self.record_range_coordination_on(
            &coordinator_group,
            coordinator_owner,
            transaction_id,
            commit_ts,
            decision_participants.clone(),
        )
        .await?;
        for (group_id, range) in &participants {
            match self
                .prepare_on(group_id, range.owner, transaction_id, commit_ts, state.clone())
                .await
            {
                Ok(()) => {}
                Err(error) => {
                    if let Err(queue_error) = self.queue_range_resolutions(
                        transaction_id,
                        commit_ts,
                        &participant_ids,
                        false,
                    ) {
                        tracing::error!(transaction_id, commit_ts, %queue_error, "transaction abort resolution queue failed");
                    }
                    if let Err(decision_error) = self
                        .record_range_decision_on(
                            &coordinator_group,
                            coordinator_owner,
                            transaction_id,
                            commit_ts,
                            false,
                            decision_participants.clone(),
                        )
                        .await
                    {
                        tracing::error!(
                            transaction_id,
                            commit_ts,
                            %decision_error,
                            "transaction abort decision recovery left pending"
                        );
                    }
                    self.retry_pending_resolutions().await;
                    return Err(error);
                }
            }
        }
        if let Err(error) = self
            .record_range_decision_on(
                &coordinator_group,
                coordinator_owner,
                transaction_id,
                commit_ts,
                true,
                decision_participants.clone(),
            )
            .await
        {
            tracing::error!(
                transaction_id,
                commit_ts,
                %error,
                "transaction decision is unresolved; coordinator recovery will choose abort"
            );
            return Err(error);
        }
        if let Err(error) =
            self.queue_range_resolutions(transaction_id, commit_ts, &participant_ids, true)
        {
            tracing::error!(transaction_id, commit_ts, %error, "transaction resolution queue failed");
        }
        self.retry_pending_resolutions().await;
        Ok(commit_ts)
    }

    pub async fn commit_txn(self: &Arc<Self>, txn: ryme_txn::Transaction) -> Result<u64> {
        self.ensure_not_isolated()?;
        if let Some(expected) = txn.route_generation() {
            let current = self.range_generation();
            if current != expected {
                return Err(RymeError::Conflict(String::from("range topology changed")));
            }
        }
        let _commit = self.txn_commit_lock.lock().await;
        self.reject_prepared_conflict(&txn)?;
        if self.try_transaction_participant_ranges(&txn)?.len() > 1 {
            return self.commit_distributed_txn(txn).await;
        }
        self.commit_global_txn(txn).await
    }

    async fn replicate_frame(self: &Arc<Self>, commit_ts: u64, encoded: Vec<u8>) -> Result<()> {
        let payload = encode_applied(commit_ts, &encoded);
        self.propose_frame(payload).await.map(|_| ())
    }

    async fn propose_frame(self: &Arc<Self>, payload: Vec<u8>) -> Result<u64> {
        self.ensure_not_isolated()?;
        let _write = self.write_lock.lock().await;
        self.ensure_not_isolated()?;
        {
            let inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return Err(RymeError::Unavailable(String::from("not leader")));
            }
        }
        let index = {
            let mut inner = self.inner.lock().await;
            let index = inner
                .last_index()
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("raft log index overflow")))?;
            let entry = LogEntry { index, term: inner.term, payload };
            validate_append_batch(std::slice::from_ref(&entry))?;
            if let Err(error) = inner.persist_entries(std::slice::from_ref(&entry)) {
                let isolate = is_wal_storage_failure(&error);
                drop(inner);
                if isolate {
                    self.set_isolated(true);
                }
                return Err(error);
            }
            inner.log.push(entry);
            inner.acks.insert(self.id, index);
            index
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            self.replicate_once().await?;
            let (role, committed, last) = {
                let mut inner = self.inner.lock().await;
                if let Err(error) = inner.advance_commit() {
                    drop(inner);
                    self.set_isolated(true);
                    return Err(error);
                }
                (inner.role, inner.commit_index, inner.last_index())
            };
            if committed >= last {
                break;
            }
            if role != Role::Leader {
                return Err(RymeError::Unavailable(String::from("stepped down")));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RymeError::Unavailable(String::from("no quorum")));
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        Ok(index)
    }

    async fn wait_applied(&self, index: u64) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if self.inner.lock().await.applied >= index {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RymeError::Timeout);
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    pub async fn add_member(self: &Arc<Self>, id: usize, addr: String) -> Result<()> {
        {
            let inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return Err(RymeError::Unavailable(String::from("not leader")));
            }
            if inner.is_member(id) {
                return Err(RymeError::InvalidArgument(String::from("member")));
            }
        }
        self.catch_up(id, addr.clone()).await?;
        let members = {
            let inner = self.inner.lock().await;
            let mut members: Vec<Member> = inner
                .current
                .iter()
                .map(|member| Member { id: member.id, addr: member.addr.clone() })
                .collect();
            members.push(Member { id, addr });
            members
        };
        self.replace_members(members).await
    }

    pub async fn propose_joint(self: &Arc<Self>, target: Vec<Member>) -> Result<()> {
        {
            let inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return Err(RymeError::Unavailable(String::from("not leader")));
            }
        }
        let target = normalize_members(target);
        if target.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("member")));
        }
        self.ensure_pools(&target);
        let current = {
            let inner = self.inner.lock().await;
            inner.current.clone()
        };
        let payload = crate::encode_conf_change(&crate::ConfChange::joint(current, target))?;
        let index = self.propose_frame(payload).await?;
        self.wait_applied(index).await
    }

    pub async fn finalize(self: &Arc<Self>) -> Result<()> {
        self.finalize_joint().await
    }

    pub async fn joint_config(&self) -> Option<Vec<Member>> {
        self.inner.lock().await.joint.clone()
    }

    pub async fn current_config(&self) -> Vec<Member> {
        self.inner.lock().await.current.clone()
    }

    pub async fn replace_members(self: &Arc<Self>, target: Vec<Member>) -> Result<()> {
        {
            let inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return Err(RymeError::Unavailable(String::from("not leader")));
            }
        }
        let target = normalize_members(target);
        if target.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("member")));
        }
        if !target.iter().any(|member| member.id == self.id) {
            return Err(RymeError::InvalidArgument(String::from("transfer first")));
        }
        self.ensure_pools(&target);
        self.finalize_joint().await?;
        let current = {
            let inner = self.inner.lock().await;
            inner.current.clone()
        };
        if current == target {
            return Ok(());
        }
        let joint_payload =
            crate::encode_conf_change(&crate::ConfChange::joint(current, target.clone()))?;
        let joint_index = self.propose_frame(joint_payload).await?;
        self.wait_applied(joint_index).await?;
        self.finalize_joint().await
    }

    async fn finalize_joint(self: &Arc<Self>) -> Result<()> {
        let pending = {
            let inner = self.inner.lock().await;
            inner.joint.clone()
        };
        let Some(target) = pending else {
            return Ok(());
        };
        let payload = crate::encode_conf_change(&crate::ConfChange::single(target))?;
        let index = self.propose_frame(payload).await?;
        self.wait_applied(index).await
    }

    async fn catch_up(&self, _id: usize, addr: String) -> Result<()> {
        let pool = PeerPool::new(addr);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut next = 1u64;
        loop {
            let (term, commit, snapshot) = {
                let inner = self.inner.lock().await;
                if inner.role != Role::Leader {
                    return Err(RymeError::Unavailable(String::from("not leader")));
                }
                (inner.term, inner.commit_index, Arc::new(inner.log.clone()))
            };
            let prev = snapshot.iter().rev().find(|e| e.index < next);
            let (prev_index, prev_term) = prev.map(|e| (e.index, e.term)).unwrap_or((0, 0));
            let last = snapshot.last().map(|e| e.index).unwrap_or(0);
            if next > last {
                return Ok(());
            }
            let batch_count = append_batch_count(&snapshot, next)?;
            let entries: Vec<LogEntry> =
                snapshot.iter().filter(|e| e.index >= next).take(batch_count).cloned().collect();
            let sent_last = entries.last().map(|entry| entry.index).unwrap_or(prev_index);
            let request = Rpc::AppendRequest {
                term,
                leader: self.id,
                prev_index,
                prev_term,
                entries,
                leader_commit: commit,
            };
            match pool.roundtrip(&request).await {
                Ok(Rpc::AppendResponse { term: peer_term, ok, match_index }) => {
                    {
                        let mut inner = self.inner.lock().await;
                        if peer_term > inner.term {
                            inner.term = peer_term;
                            inner.role = Role::Follower;
                            inner.voted_for = None;
                            inner.leader_id = None;
                            inner.persist_meta()?;
                            return Err(RymeError::Unavailable(String::from("stepped down")));
                        }
                        if inner.term != term || inner.role != Role::Leader {
                            return Err(RymeError::Unavailable(String::from("not leader")));
                        }
                    }
                    if ok {
                        if match_index < sent_last {
                            return Err(RymeError::Unavailable(String::from(
                                "catch-up acknowledgement index",
                            )));
                        }
                        if sent_last >= last {
                            return Ok(());
                        }
                        next = sent_last.checked_add(1).ok_or_else(|| {
                            RymeError::Corrupt(String::from("raft log index overflow"))
                        })?;
                    } else {
                        let suggested = match_index.min(last).saturating_add(1).max(1);
                        next = if suggested < next {
                            suggested
                        } else {
                            next.saturating_sub(1).max(1)
                        };
                    }
                }
                Ok(_) => return Err(RymeError::Corrupt(String::from("rpc"))),
                Err(_) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(RymeError::Timeout);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RymeError::Timeout);
            }
        }
    }

    pub async fn remove_member(self: &Arc<Self>, id: usize) -> Result<()> {
        if id == self.id {
            return Err(RymeError::InvalidArgument(String::from("transfer first")));
        }
        let members = {
            let inner = self.inner.lock().await;
            if !inner.is_member(id) {
                return Err(RymeError::NotFound(String::from("member")));
            }
            inner
                .current
                .iter()
                .filter(|member| member.id != id)
                .map(|member| Member { id: member.id, addr: member.addr.clone() })
                .collect::<Vec<_>>()
        };
        if members.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("member")));
        }
        self.replace_members(members).await
    }

    pub async fn is_leader(&self) -> bool {
        self.inner.lock().await.role == Role::Leader
    }

    pub fn metadata_snapshot(&self) -> Option<Vec<u8>> {
        self.metadata.lock().ok().and_then(|metadata| metadata.clone())
    }

    pub fn set_metadata_hook(&self, hook: MetadataHook) -> Result<()> {
        {
            let mut registered = self
                .metadata_hook
                .0
                .lock()
                .map_err(|_| RymeError::Internal(String::from("metadata hook lock")))?;
            *registered = Some(hook.clone());
        }
        let current = self
            .metadata
            .lock()
            .map_err(|_| RymeError::Internal(String::from("metadata lock")))?
            .clone();
        if let Some(payload) = current {
            hook(&payload)?;
        }
        Ok(())
    }

    pub fn set_data_hook(&self, hook: MetadataHook) -> Result<()> {
        let mut registered = self
            .data_hook
            .0
            .lock()
            .map_err(|_| RymeError::Internal(String::from("data hook lock")))?;
        *registered = Some(hook);
        Ok(())
    }

    pub fn set_realtime_hook(&self, hook: MetadataHook) -> Result<()> {
        let mut registered = self
            .realtime_hook
            .0
            .lock()
            .map_err(|_| RymeError::Internal(String::from("realtime hook lock")))?;
        *registered = Some(hook);
        Ok(())
    }

    pub fn set_presence_hook(&self, hook: MetadataHook) -> Result<()> {
        let mut registered = self
            .presence_hook
            .0
            .lock()
            .map_err(|_| RymeError::Internal(String::from("presence hook lock")))?;
        *registered = Some(hook);
        Ok(())
    }

    pub fn set_topic_hook(&self, hook: MetadataHook) -> Result<()> {
        let mut registered = self
            .topic_hook
            .0
            .lock()
            .map_err(|_| RymeError::Internal(String::from("topic hook lock")))?;
        *registered = Some(hook);
        Ok(())
    }

    pub async fn replay_topics(&self) -> Result<()> {
        let payloads = {
            let inner = self.inner.lock().await;
            inner
                .log
                .iter()
                .filter(|entry| entry.index <= inner.commit_index)
                .filter_map(|entry| match decode_apply(&entry.payload) {
                    Ok(ApplyPayload::Topic { payload }) => Some(payload),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        for payload in payloads {
            self.apply_topic(payload)?;
        }
        Ok(())
    }

    pub async fn term(&self) -> u64 {
        self.inner.lock().await.term
    }

    pub async fn commit_index(&self) -> u64 {
        self.inner.lock().await.commit_index
    }

    pub async fn read_latest(&self, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        let mut txn = self.manager.begin();
        self.manager.get(&mut txn, key)
    }

    pub fn snapshot_range(
        &self,
        start: &[u8],
        end: &[u8],
        read_ts: u64,
        max_rows: usize,
    ) -> Result<RangeSnapshot> {
        self.snapshot_range_for_version(
            ryme_storage::LEGACY_ROUTING_VERSION,
            start,
            end,
            read_ts,
            max_rows,
            None,
            None,
        )
    }

    pub fn snapshot_range_for(
        &self,
        start: &[u8],
        end: &[u8],
        read_ts: u64,
        max_rows: usize,
        tenant_filter: Option<&str>,
        database_filter: Option<&str>,
    ) -> Result<RangeSnapshot> {
        self.snapshot_range_for_version(
            ryme_storage::LEGACY_ROUTING_VERSION,
            start,
            end,
            read_ts,
            max_rows,
            tenant_filter,
            database_filter,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn snapshot_range_for_version(
        &self,
        routing_version: u8,
        start: &[u8],
        end: &[u8],
        read_ts: u64,
        max_rows: usize,
        tenant_filter: Option<&str>,
        database_filter: Option<&str>,
    ) -> Result<RangeSnapshot> {
        ryme_storage::validate_routing_version(routing_version)?;
        if start >= end && !end.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("range")));
        }
        let max_rows = max_rows.clamp(1, 100_000);
        let snapshot_ts = if read_ts == 0 { self.manager.latest_commit() } else { read_ts };
        let mut rows = Vec::new();
        let mut truncated = false;
        for (tenant, database, table) in self.manager.spaces()? {
            if tenant_filter.is_some_and(|wanted| wanted != tenant)
                || database_filter.is_some_and(|wanted| wanted != database)
            {
                continue;
            }
            let remaining = max_rows.saturating_sub(rows.len());
            for (pk, table_versions) in self.manager.export_table_routing_range(
                routing_version,
                &tenant,
                &database,
                &table,
                start,
                end,
                snapshot_ts,
                remaining.saturating_add(1),
            )? {
                let versions: Vec<RangeSnapshotVersion> = table_versions
                    .into_iter()
                    .map(|version| RangeSnapshotVersion {
                        commit_ts: version.commit_ts,
                        value: version.value,
                        expires_at: version.expires_at,
                    })
                    .collect();
                if versions.is_empty() {
                    continue;
                }
                let mut value = Vec::new();
                let mut expires_at = 0;
                for version in &versions {
                    if version.value.is_some()
                        && (version.expires_at == 0 || version.expires_at > now_unix())
                    {
                        value = version.value.clone().unwrap_or_default();
                        expires_at = version.expires_at;
                    } else {
                        value.clear();
                        expires_at = 0;
                    }
                }
                rows.push(RangeSnapshotRow {
                    tenant: tenant.clone(),
                    database: database.clone(),
                    table: table.clone(),
                    pk,
                    value,
                    expires_at,
                    versions,
                });
                if rows.len() > max_rows {
                    truncated = true;
                    rows.truncate(max_rows);
                    break;
                }
            }
            if truncated {
                break;
            }
        }
        rows.sort_by(|left, right| {
            left.table
                .as_bytes()
                .cmp(right.table.as_bytes())
                .then_with(|| left.pk.cmp(&right.pk))
                .then_with(|| left.tenant.cmp(&right.tenant))
                .then_with(|| left.database.cmp(&right.database))
        });
        Ok(RangeSnapshot {
            start: start.to_vec(),
            end: end.to_vec(),
            routing_version,
            snapshot_ts,
            applied_commit: self.manager.latest_commit(),
            rows,
            truncated,
        })
    }

    pub async fn scan_range_page(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: Option<&[u8]>,
        read_ts: u64,
        limit: usize,
    ) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        let ranges = self.try_range_owners()?;
        let routing_version = ranges
            .first()
            .map(|range| range.routing_version)
            .unwrap_or(ryme_storage::LEGACY_ROUTING_VERSION);
        let (table_start, table_end) =
            ryme_storage::table_routing_bounds(routing_version, tenant, database, table)?;
        let mut spans = Vec::new();
        for range in ranges {
            let range = self.try_with_current_range_leader(range)?;
            let lower = if range.start.as_slice() > table_start.as_slice() {
                range.start
            } else {
                table_start.clone()
            };
            let upper = if range.end.is_empty() {
                table_end.clone()
            } else if range.end.as_slice() < table_end.as_slice() {
                range.end
            } else {
                table_end.clone()
            };
            if lower < upper {
                spans.push((range.owner, lower, upper));
            }
        }
        if spans.is_empty() {
            return Ok(None);
        }

        let max_rows = limit.min(100_000);
        let mut rows = BTreeMap::new();
        for (owner, lower, upper) in spans {
            let snapshot = if owner == self.id {
                self.snapshot_range_for_version(
                    routing_version,
                    &lower,
                    &upper,
                    read_ts,
                    max_rows,
                    Some(tenant),
                    Some(database),
                )?
            } else {
                self.fetch_range_snapshot_for_version(
                    owner,
                    routing_version,
                    lower,
                    upper,
                    read_ts,
                    max_rows,
                    tenant.to_string(),
                    database.to_string(),
                )
                .await?
            };
            for row in snapshot.rows {
                if row.table != table
                    || row.tenant != tenant
                    || row.database != database
                    || start_after.is_some_and(|after| row.pk.as_slice() <= after)
                {
                    continue;
                }
                if let Some(value) = snapshot_live_value(&row, snapshot.snapshot_ts) {
                    rows.insert(row.pk, value);
                }
            }
        }
        Ok(Some(rows.into_iter().take(limit).collect()))
    }

    pub async fn fetch_range_snapshot(
        &self,
        peer: usize,
        start: Vec<u8>,
        end: Vec<u8>,
        read_ts: u64,
        max_rows: usize,
    ) -> Result<RangeSnapshot> {
        self.fetch_range_snapshot_with_version(
            peer,
            ryme_storage::LEGACY_ROUTING_VERSION,
            start,
            end,
            read_ts,
            max_rows,
        )
        .await
    }

    pub async fn fetch_range_snapshot_with_version(
        &self,
        peer: usize,
        routing_version: u8,
        start: Vec<u8>,
        end: Vec<u8>,
        read_ts: u64,
        max_rows: usize,
    ) -> Result<RangeSnapshot> {
        let pool = self.pool_for(peer).ok_or_else(|| RymeError::NotFound(String::from("peer")))?;
        let max_rows = u32::try_from(max_rows.min(100_000))
            .map_err(|_| RymeError::InvalidArgument(String::from("max_rows")))?;
        match pool
            .roundtrip(&Rpc::RangeSnapshotRequest {
                start,
                end,
                routing_version,
                read_ts,
                max_rows,
                tenant: None,
                database: None,
            })
            .await?
        {
            Rpc::RangeSnapshotResponse { snapshot } => Ok(snapshot),
            _ => Err(RymeError::Corrupt(String::from("range snapshot rpc"))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_range_snapshot_for(
        &self,
        peer: usize,
        start: Vec<u8>,
        end: Vec<u8>,
        read_ts: u64,
        max_rows: usize,
        tenant: String,
        database: String,
    ) -> Result<RangeSnapshot> {
        self.fetch_range_snapshot_for_version(
            peer,
            ryme_storage::LEGACY_ROUTING_VERSION,
            start,
            end,
            read_ts,
            max_rows,
            tenant,
            database,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_range_snapshot_for_version(
        &self,
        peer: usize,
        routing_version: u8,
        start: Vec<u8>,
        end: Vec<u8>,
        read_ts: u64,
        max_rows: usize,
        tenant: String,
        database: String,
    ) -> Result<RangeSnapshot> {
        let pool = self.pool_for(peer).ok_or_else(|| RymeError::NotFound(String::from("peer")))?;
        let max_rows = u32::try_from(max_rows.min(100_000))
            .map_err(|_| RymeError::InvalidArgument(String::from("max_rows")))?;
        match pool
            .roundtrip(&Rpc::RangeSnapshotRequest {
                start,
                end,
                routing_version,
                read_ts,
                max_rows,
                tenant: Some(tenant),
                database: Some(database),
            })
            .await?
        {
            Rpc::RangeSnapshotResponse { snapshot } => Ok(snapshot),
            _ => Err(RymeError::Corrupt(String::from("range snapshot rpc"))),
        }
    }

    pub fn install_range_snapshot(&self, snapshot: &RangeSnapshot) -> Result<u64> {
        if snapshot.truncated {
            return Err(RymeError::Overload(String::from("range snapshot")));
        }
        if !snapshot.end.is_empty() && snapshot.start >= snapshot.end {
            return Err(RymeError::InvalidArgument(String::from("range")));
        }
        if snapshot.rows.len() > 100_000 {
            return Err(RymeError::Overload(String::from("range snapshot")));
        }
        ryme_storage::validate_routing_version(snapshot.routing_version)?;
        if self.manager.latest_commit() > snapshot.snapshot_ts {
            return Err(RymeError::Unavailable(String::from("target ahead")));
        }
        let mut replacement = Engine::new();
        let mut seen = BTreeSet::new();
        for row in &snapshot.rows {
            let key = RecordKey::new(&row.tenant, &row.database, &row.table, &row.pk);
            let routing_key = ryme_storage::record_routing_key(snapshot.routing_version, &key)?;
            if routing_key.as_slice() < snapshot.start.as_slice()
                || (!snapshot.end.is_empty() && routing_key.as_slice() >= snapshot.end.as_slice())
            {
                return Err(RymeError::Corrupt(String::from("range snapshot scope")));
            }
            if !seen.insert(key.clone()) || row.versions.is_empty() {
                return Err(RymeError::Corrupt(String::from("range snapshot row")));
            }
            let mut previous_commit_ts = 0;
            for version in &row.versions {
                if version.commit_ts <= previous_commit_ts
                    || version.commit_ts > snapshot.snapshot_ts
                {
                    return Err(RymeError::Corrupt(String::from("range snapshot version order")));
                }
                previous_commit_ts = version.commit_ts;
                replacement
                    .apply_with_expiry(
                        key.clone(),
                        version.commit_ts,
                        version.value.clone(),
                        version.expires_at,
                    )
                    .map_err(|error| {
                        RymeError::Corrupt(format!("range snapshot version: {error}"))
                    })?;
            }
        }
        let existing = self.snapshot_range_for_version(
            snapshot.routing_version,
            &snapshot.start,
            &snapshot.end,
            snapshot.snapshot_ts,
            100_000,
            None,
            None,
        )?;
        if existing.truncated {
            return Err(RymeError::Overload(String::from("target range")));
        }
        self.manager.install_routing_range_snapshot(
            snapshot.routing_version,
            &snapshot.start,
            &snapshot.end,
            snapshot.snapshot_ts,
            replacement,
        )?;
        Ok(snapshot.rows.len() as u64)
    }

    pub async fn purge_range_after_transfer(
        &self,
        group_id: &str,
        expected_epoch: u64,
        snapshot: &RangeSnapshot,
    ) -> Result<u64> {
        if snapshot.truncated {
            return Err(RymeError::Overload(String::from("range snapshot")));
        }
        let _commit = self.txn_commit_lock.lock().await;
        let range = self
            .try_range_owner_by_id(group_id)?
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.epoch != expected_epoch || range.owner == self.id {
            return Err(RymeError::Unavailable(String::from("range ownership update pending")));
        }
        let current = self.snapshot_range_for_version(
            snapshot.routing_version,
            &snapshot.start,
            &snapshot.end,
            self.manager.latest_commit(),
            100_000,
            None,
            None,
        )?;
        if current.truncated || current.rows != snapshot.rows {
            return Err(RymeError::Conflict(String::from("range changed during transfer")));
        }
        let keys: Vec<RecordKey> = current
            .rows
            .iter()
            .map(|row| RecordKey::new(&row.tenant, &row.database, &row.table, &row.pk))
            .collect();
        if keys.is_empty() {
            return Ok(0);
        }
        Ok(self.manager.purge_keys(&keys)? as u64)
    }

    pub async fn install_range_snapshot_on(
        &self,
        peer: usize,
        snapshot: RangeSnapshot,
    ) -> Result<u64> {
        let pool = self.pool_for(peer).ok_or_else(|| RymeError::NotFound(String::from("peer")))?;
        match pool.roundtrip(&Rpc::RangeInstallRequest { snapshot }).await? {
            Rpc::RangeInstallResponse { rows } => Ok(rows),
            _ => Err(RymeError::Corrupt(String::from("range install rpc"))),
        }
    }

    pub async fn purge_range_after_transfer_on(
        &self,
        peer: usize,
        group_id: String,
        expected_epoch: u64,
        snapshot: RangeSnapshot,
    ) -> Result<u64> {
        let pool = self.pool_for(peer).ok_or_else(|| RymeError::NotFound(String::from("peer")))?;
        match pool.roundtrip(&Rpc::RangePurgeRequest { group_id, expected_epoch, snapshot }).await?
        {
            Rpc::RangePurgeResponse { rows } => Ok(rows),
            _ => Err(RymeError::Corrupt(String::from("range purge rpc"))),
        }
    }

    pub fn set_range_owners(&self, owners: Vec<RangeOwner>) {
        if let Err(error) = self.try_set_range_owners(owners) {
            tracing::error!(%error, "failed to install range ownership metadata");
        }
    }

    /// Installs range ownership metadata and reports validation, persistence,
    /// or synchronization failures to the caller.
    pub fn try_set_range_owners(&self, mut owners: Vec<RangeOwner>) -> Result<()> {
        if let Some(first) = owners.first() {
            if ryme_storage::validate_routing_version(first.routing_version).is_err()
                || owners.iter().any(|range| range.routing_version != first.routing_version)
            {
                return Err(RymeError::InvalidArgument(String::from("range routing version")));
            }
        }
        for range in &mut owners {
            if range.id.is_empty() {
                range.id = derived_range_id(&range.start, &range.end, range.epoch);
            }
            if range.replicas.is_empty() {
                range.replicas.push(range.owner);
            }
            if !range.replicas.contains(&range.owner) {
                range.replicas.push(range.owner);
            }
            range.replicas.sort_unstable();
            range.replicas.dedup();
            self.try_ensure_range_group(&range.id, range.owner)?;
        }
        self.persist_range_meta()?;
        owners.sort_by(|left, right| left.start.cmp(&right.start));
        let mut current = self
            .range_owners
            .write()
            .map_err(|_| RymeError::Internal(String::from("range ownership metadata poisoned")))?;
        let mut leaders = self
            .range_leaders
            .write()
            .map_err(|_| RymeError::Internal(String::from("range leader metadata lock")))?;
        if *current == owners {
            return Ok(());
        }
        *current = owners.clone();
        leaders.clear();
        for range in owners {
            leaders.insert(range.id, range.owner);
        }
        self.range_generation.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn range_generation(&self) -> u64 {
        self.range_generation.load(Ordering::SeqCst)
    }

    pub fn range_owners(&self) -> Vec<RangeOwner> {
        self.range_owners.read().map(|owners| owners.clone()).unwrap_or_default()
    }

    pub fn range_owner(&self, routing_key: &[u8]) -> Option<(usize, u64)> {
        let owners = self.range_owners.read().ok()?;
        let leaders = self.range_leaders.read().ok()?;
        owners
            .iter()
            .find(|range| {
                routing_key >= range.start.as_slice()
                    && (range.end.is_empty() || routing_key < range.end.as_slice())
            })
            .map(|range| {
                let owner = leaders.get(&range.id).copied().unwrap_or(range.owner);
                (owner, range.epoch)
            })
    }

    pub fn range_owner_key(&self, key: &RecordKey) -> Option<usize> {
        self.try_range_owner_key(key).ok().flatten()
    }

    /// Resolves a record key to its range owner, propagating range metadata errors.
    pub fn try_range_owner_key(&self, key: &RecordKey) -> Result<Option<usize>> {
        let owners = self.try_range_owners()?;
        self.try_range_owner_key_in(&owners, key)
    }

    pub fn owns_range_key(&self, key: &RecordKey) -> bool {
        self.try_range_owner_key(key)
            .map(|owner| owner.map(|owner| owner == self.id).unwrap_or(true))
            .unwrap_or(false)
    }

    pub fn read_range_value(
        &self,
        key: &RecordKey,
        read_ts: u64,
    ) -> Result<(Option<Vec<u8>>, Option<u64>)> {
        let mut txn = self.manager.begin();
        if read_ts != 0 {
            txn.restamp(read_ts);
        }
        let value = self.manager.get(&mut txn, key)?;
        let expires_at = if value.is_some() {
            if read_ts == 0 {
                self.manager.expires_at(key)?
            } else {
                self.manager.expires_at_at(key, read_ts)?
            }
        } else {
            None
        };
        Ok((value, expires_at))
    }

    pub async fn fetch_range_value(
        &self,
        peer: usize,
        key: RecordKey,
        read_ts: u64,
    ) -> Result<(Option<Vec<u8>>, Option<u64>)> {
        let (value, expires_at, _) = self.fetch_range_value_with_ts(peer, key, read_ts).await?;
        Ok((value, expires_at))
    }

    pub async fn fetch_range_value_with_ts(
        &self,
        peer: usize,
        key: RecordKey,
        read_ts: u64,
    ) -> Result<(Option<Vec<u8>>, Option<u64>, u64)> {
        let pool = self.pool_for(peer).ok_or_else(|| RymeError::NotFound(String::from("peer")))?;
        match pool.roundtrip(&Rpc::RangeReadRequest { key, read_ts }).await? {
            Rpc::RangeReadResponse { value, expires_at, read_ts } => {
                Ok((value, expires_at, read_ts))
            }
            _ => Err(RymeError::Corrupt(String::from("range read rpc"))),
        }
    }

    pub fn read_range_values(&self, keys: &[RecordKey], read_ts: u64) -> Result<RangeReadValues> {
        if keys.len() > 1_024 {
            return Err(RymeError::Overload(String::from("range read batch")));
        }
        keys.iter().map(|key| self.read_range_value(key, read_ts)).collect()
    }

    pub async fn fetch_range_values(
        &self,
        peer: usize,
        keys: Vec<RecordKey>,
        read_ts: u64,
    ) -> Result<RangeReadValues> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > 1_024 {
            return Err(RymeError::Overload(String::from("range read batch")));
        }
        let pool = self.pool_for(peer).ok_or_else(|| RymeError::NotFound(String::from("peer")))?;
        match pool.roundtrip(&Rpc::RangeReadBatchRequest { keys, read_ts }).await? {
            Rpc::RangeReadBatchResponse { values } => Ok(values),
            _ => Err(RymeError::Corrupt(String::from("range read batch rpc"))),
        }
    }

    pub fn manager(&self) -> &TxnManager {
        &self.manager
    }

    pub fn gc_oldest(&self) -> Result<u64> {
        self.manager.gc_oldest()
    }

    pub fn node_id(&self) -> usize {
        self.id
    }
}

fn merge_scan_page(
    txn: &ryme_txn::Transaction,
    tenant: &str,
    database: &str,
    table: &str,
    start_after: Option<&[u8]>,
    limit: usize,
    page: Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows: BTreeMap<Vec<u8>, Vec<u8>> = page.into_iter().collect();
    for (key, write) in txn.writes() {
        if key.tenant != tenant
            || key.database != database
            || key.table != table
            || start_after.is_some_and(|after| key.pk.as_slice() <= after)
        {
            continue;
        }
        match write.value.as_ref() {
            Some(value) if write.expires_at == 0 || write.expires_at > now_unix() => {
                rows.insert(key.pk.clone(), value.clone());
            }
            _ => {
                rows.remove(&key.pk);
            }
        }
    }
    rows.into_iter().take(limit).collect()
}

fn key_matches_scan(key: &RecordKey, scans: &BTreeSet<(String, String, String)>) -> bool {
    scans.iter().any(|(tenant, database, table)| {
        key.tenant == *tenant && key.database == *database && key.table == *table
    })
}

fn transactions_conflict(left: &ryme_txn::Transaction, right: &ryme_txn::Transaction) -> bool {
    for key in left.writes().keys() {
        if right.writes().contains_key(key)
            || right.read_keys().contains(key)
            || (right.isolation() == ryme_txn::Isolation::Serializable
                && key_matches_scan(key, right.scanned_tables()))
            || key_matches_scan(key, right.constraint_scanned_tables())
        {
            return true;
        }
    }
    for key in right.writes().keys() {
        if left.read_keys().contains(key)
            || (left.isolation() == ryme_txn::Isolation::Serializable
                && key_matches_scan(key, left.scanned_tables()))
            || key_matches_scan(key, left.constraint_scanned_tables())
        {
            return true;
        }
    }
    false
}

#[derive(Debug, Clone)]
pub struct ClusterBackend {
    node: Arc<Node>,
}

impl ClusterBackend {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }

    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub fn gc_oldest(&self) -> Result<u64> {
        self.node.manager().gc_oldest()
    }
}

impl ryme_txn::TxnBackend for ClusterBackend {
    fn begin(&self) -> ryme_txn::Transaction {
        let mut txn = self.node.manager().begin();
        txn.set_route_generation(self.node.range_generation());
        txn
    }

    fn get(&self, txn: &mut ryme_txn::Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        self.node.manager().get(txn, key)
    }

    fn get_async<'a>(
        &'a self,
        txn: &'a mut ryme_txn::Transaction,
        key: &'a RecordKey,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<Vec<u8>>>> + Send + 'a>> {
        let node = self.node.clone();
        Box::pin(async move {
            let Some(owner) = node.try_range_owner_key(key)? else {
                return node.manager().get(txn, key);
            };
            if owner == node.node_id() {
                return node.manager().get(txn, key);
            }
            if txn.writes().contains_key(key) {
                return node.manager().get(txn, key);
            }
            // The routing replica may observe the global commit after the client
            // replica does. Ask the range owner for its current durable view so a
            // successful write is immediately visible through a routed read.
            let establish_snapshot = txn.read_keys().is_empty()
                && txn.writes().is_empty()
                && txn.scanned_tables().is_empty()
                && txn.constraint_scanned_tables().is_empty();
            let snapshot_floor = txn.read_ts;
            let read_ts = if establish_snapshot { 0 } else { txn.read_ts };
            let (mut value, _, observed_read_ts) =
                node.fetch_range_value_with_ts(owner, key.clone(), read_ts).await?;
            if establish_snapshot {
                // A stale range owner must not move the transaction's snapshot backward.
                let snapshot_ts = snapshot_floor.max(observed_read_ts);
                if observed_read_ts < snapshot_ts {
                    value =
                        node.fetch_range_value_with_ts(owner, key.clone(), snapshot_ts).await?.0;
                }
                txn.restamp(snapshot_ts);
            }
            txn.record_read(key.clone(), value.is_some())?;
            Ok(value)
        })
    }

    fn scan_async<'a>(
        &'a self,
        txn: &'a mut ryme_txn::Transaction,
        tenant: &'a str,
        database: &'a str,
        table: &'a str,
        limit: usize,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>>> + Send + 'a>>
    {
        let node = self.node.clone();
        Box::pin(async move {
            let page =
                node.scan_range_page(tenant, database, table, None, txn.read_ts, limit).await?;
            let Some(page) = page else {
                return node.manager().scan(txn, tenant, database, table, limit);
            };
            txn.record_scan(tenant, database, table)?;
            Ok(merge_scan_page(txn, tenant, database, table, None, limit, page))
        })
    }

    fn scan_after_async<'a>(
        &'a self,
        txn: &'a mut ryme_txn::Transaction,
        tenant: &'a str,
        database: &'a str,
        table: &'a str,
        start_after: &'a [u8],
        limit: usize,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>>> + Send + 'a>>
    {
        let node = self.node.clone();
        Box::pin(async move {
            let page = node
                .scan_range_page(tenant, database, table, Some(start_after), txn.read_ts, limit)
                .await?;
            let Some(page) = page else {
                return node.manager().scan_after(txn, tenant, database, table, start_after, limit);
            };
            txn.record_scan(tenant, database, table)?;
            Ok(merge_scan_page(txn, tenant, database, table, Some(start_after), limit, page))
        })
    }

    fn put(&self, txn: &mut ryme_txn::Transaction, key: RecordKey, value: Vec<u8>) {
        self.node.manager().put(txn, key, value);
    }

    fn put_with_ttl(
        &self,
        txn: &mut ryme_txn::Transaction,
        key: RecordKey,
        value: Vec<u8>,
        expires_at: u64,
    ) {
        self.node.manager().put_with_ttl(txn, key, value, expires_at);
    }

    fn delete(&self, txn: &mut ryme_txn::Transaction, key: RecordKey) {
        self.node.manager().delete(txn, key);
    }

    fn expires_at(&self, key: &RecordKey) -> Result<Option<u64>> {
        self.node.manager().expires_at(key)
    }

    fn expires_at_at(&self, key: &RecordKey, read_ts: u64) -> Result<Option<u64>> {
        self.node.manager().expires_at_at(key, read_ts)
    }

    fn commit(
        &self,
        txn: ryme_txn::Transaction,
    ) -> impl std::future::Future<Output = Result<u64>> + Send {
        let node = self.node.clone();
        let route_generation = txn.route_generation();
        async move {
            txn.ensure_healthy()?;
            let distributed = node.try_transaction_participant_ranges(&txn)?.len() > 1;
            let routed = if !distributed
                && txn.read_keys().is_empty()
                && txn.scanned_tables().is_empty()
                && txn.constraint_scanned_tables().is_empty()
                && !txn.writes().is_empty()
            {
                let owners = node.try_range_owners()?;
                if owners.is_empty() {
                    None
                } else {
                    let mut owner = None;
                    let mut same_owner = true;
                    for key in txn.writes().keys() {
                        let Some(next_owner) = node.try_range_owner_key_in(&owners, key)? else {
                            same_owner = false;
                            break;
                        };
                        match owner {
                            None => owner = Some(next_owner),
                            Some(current) if current == next_owner => {}
                            Some(_) => {
                                same_owner = false;
                                break;
                            }
                        }
                    }
                    Some((same_owner.then_some(owner).flatten(), txn.writes().clone()))
                }
            } else {
                None
            };
            let result = match routed {
                Some((Some(owner), writes)) => {
                    node.propose_write_on(owner, writes, route_generation).await
                }
                Some((None, writes)) => {
                    node.propose_write_to_leader(writes, route_generation).await
                }
                None => node.commit_transaction_to_leader(txn).await,
            };
            if let Ok(commit_ts) = result {
                // Keep transactions opened on a follower from lagging behind a
                // commit it just forwarded to the global leader. Range reads
                // still fetch their current value from the range owner.
                node.manager().advance_to(commit_ts);
            }
            result
        }
    }

    fn scan(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.node.manager().scan(txn, tenant, database, table, limit)
    }

    fn scan_after(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.node.manager().scan_after(txn, tenant, database, table, start_after, limit)
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn is_wal_storage_failure(error: &RymeError) -> bool {
    match error {
        RymeError::Io(_) | RymeError::Corrupt(_) => true,
        RymeError::Unavailable(message) => message.starts_with("wal is in a failed state"),
        _ => false,
    }
}

fn append_to_log(
    inner: &mut tokio::sync::MutexGuard<'_, Inner>,
    leader_term: u64,
    prev_index: u64,
    prev_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
) -> Result<u64> {
    if leader_term == 0 {
        return Err(RymeError::InvalidArgument(String::from("raft append term")));
    }
    validate_append_batch(&entries)?;
    if prev_index == 0 && prev_term != 0 {
        return Err(RymeError::Unavailable(String::from("invalid log prefix")));
    }
    if prev_index > 0 {
        let Ok(position) = inner.log.binary_search_by_key(&prev_index, |entry| entry.index) else {
            return Err(RymeError::Unavailable(String::from("missing prefix")));
        };
        let existing = &inner.log[position];
        if existing.term != prev_term {
            return Err(RymeError::Unavailable(String::from("conflict")));
        }
    }
    let mut expected = prev_index;
    let mut conflict = None;
    for entry in &entries {
        expected = expected
            .checked_add(1)
            .ok_or_else(|| RymeError::Unavailable(String::from("log index overflow")))?;
        if entry.index != expected {
            return Err(RymeError::Unavailable(String::from("log gap")));
        }
        if entry.term == 0 || entry.term > leader_term {
            return Err(RymeError::InvalidArgument(String::from("raft append term")));
        }
        validate_apply_payload(&entry.payload)
            .map_err(|_| RymeError::InvalidArgument(String::from("raft append payload")))?;
        if let Ok(position) =
            inner.log.binary_search_by_key(&entry.index, |existing| existing.index)
        {
            let existing = &inner.log[position];
            if existing.term == entry.term {
                if existing.payload != entry.payload {
                    return Err(RymeError::InvalidArgument(String::from(
                        "conflicting raft entry for term and index",
                    )));
                }
            } else if conflict.is_none() {
                conflict = Some(entry.index);
            }
        }
    }
    if let Some(conflict) = conflict {
        let keep_through = conflict - 1;
        let replacement: Vec<LogEntry> =
            entries.into_iter().filter(|entry| entry.index >= conflict).collect();
        inner.replace_log_suffix(keep_through, &replacement)?;
        inner.log.retain(|entry| entry.index <= keep_through);
        inner.log.extend(replacement);
    } else {
        let last = inner.last_index();
        let new_entries: Vec<LogEntry> =
            entries.into_iter().filter(|entry| entry.index > last).collect();
        if !new_entries.is_empty() {
            inner.persist_entries(&new_entries)?;
            inner.log.extend(new_entries);
        }
    }
    let last = inner.last_index();
    if leader_commit > inner.commit_index {
        inner.commit_index = leader_commit.min(last);
    }
    Ok(inner.last_index())
}

trait ResetFlag {
    fn bump_reset(&mut self);
    fn reset_flag(&mut self) -> bool;
}

impl ResetFlag for Inner {
    fn bump_reset(&mut self) {
        self.reset_epoch = self.reset_epoch.wrapping_add(1);
    }

    fn reset_flag(&mut self) -> bool {
        let current = self.reset_epoch;
        if current != self.observed_epoch {
            self.observed_epoch = current;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_frame_writer_stops_at_the_serialized_size_limit() {
        let rpc = Rpc::Realtime { payload: vec![u8::MAX; 5 * 1024 * 1024] };
        let budget = Semaphore::new(RPC_WRITE_FRAME_BUDGET_PERMITS);
        let mut frame = RpcFrameBuffer::new(&budget);
        assert!(serde_json::to_writer(&mut frame, &rpc).is_err());
        assert!(frame.exceeded_limit);
        assert!(frame.bytes.len() <= MAX_RPC_FRAME_BYTES);
    }

    #[test]
    fn rpc_frame_writer_reserves_aggregate_budget_before_growth() {
        let rpc = Rpc::VoteRequest { term: 1, candidate: 0, last_term: 0, last_index: 0 };
        let budget = Semaphore::new(0);
        let mut frame = RpcFrameBuffer::new(&budget);
        assert!(serde_json::to_writer(&mut frame, &rpc).is_err());
        assert!(frame.exhausted_budget);
        assert!(frame.bytes.is_empty());
    }

    #[test]
    fn rpc_frame_writer_growth_reserves_only_new_budget_units() {
        let budget = Semaphore::new(RPC_FRAME_WRITE_WEIGHT);
        let mut frame = RpcFrameBuffer::new(&budget);
        std::io::Write::write_all(&mut frame, &vec![0; 8 * 1024]).unwrap();
        std::io::Write::write_all(&mut frame, b"x").unwrap();

        assert_eq!(frame.bytes.len(), 8 * 1024 + 1);
        assert!(!frame.exhausted_budget);
        assert_eq!(budget.available_permits(), 0);
    }

    #[tokio::test]
    async fn rpc_frame_reader_waits_for_decode_budget_before_allocating_body() {
        let rpc = Rpc::VoteRequest { term: 1, candidate: 0, last_term: 0, last_index: 0 };
        let body = serde_json::to_vec(&rpc).unwrap();
        let mut frame = Vec::with_capacity(4 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        let (mut sender, mut receiver) = tokio::io::duplex(frame.len());
        sender.write_all(&frame).await.unwrap();

        let exhausted_budget = Semaphore::new(0);
        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            read_frame_with_budget(&mut receiver, &exhausted_budget)
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn rpc_frame_reader_holds_budget_until_decoded_request_is_dropped() {
        let rpc = Rpc::Realtime { payload: vec![42; 64 * 1024] };
        let body = serde_json::to_vec(&rpc).unwrap();
        let mut frame = Vec::with_capacity(4 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        let (mut sender, mut receiver) = tokio::io::duplex(frame.len());
        sender.write_all(&frame).await.unwrap();

        let permits = body.len().div_ceil(RPC_FRAME_BUDGET_UNIT_BYTES) * RPC_FRAME_DECODE_WEIGHT;
        let budget = Semaphore::new(permits);
        let request = read_frame_with_budget(&mut receiver, &budget).await.unwrap();
        assert_eq!(budget.available_permits(), 0);
        assert!(matches!(&request.rpc, Rpc::Realtime { payload } if payload.len() == 64 * 1024));

        drop(request);
        assert_eq!(budget.available_permits(), permits);
    }

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ryme-raft-{label}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ))
    }

    #[test]
    fn constraint_scans_enlist_all_table_ranges_and_conflict_with_inserts() {
        let dir = temp_dir("constraint-scan-ranges");
        let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let (table_start, table_end) =
            ryme_storage::table_routing_bounds(0, "t", "d", "docs").unwrap();
        let mut split = table_start.clone();
        split.push(0x80);
        node.set_range_owners(vec![
            RangeOwner {
                id: String::from("range-a"),
                start: table_start.clone(),
                end: split.clone(),
                routing_version: 0,
                owner: 0,
                replicas: vec![0],
                epoch: 1,
            },
            RangeOwner {
                id: String::from("range-b"),
                start: split,
                end: table_end,
                routing_version: 0,
                owner: 1,
                replicas: vec![1],
                epoch: 1,
            },
        ]);

        let mut constraint_txn = node.manager.begin();
        constraint_txn.record_constraint_scan("t", "d", "docs").unwrap();
        node.manager.put(
            &mut constraint_txn,
            RecordKey::new("t", "d", "docs", b"row-a"),
            b"unique-value".to_vec(),
        );
        assert_eq!(node.transaction_participant_ranges(&constraint_txn).len(), 2);

        let manager = TxnManager::new();
        let mut concurrent_insert = manager.begin();
        manager.put(
            &mut concurrent_insert,
            RecordKey::new("t", "d", "docs", b"row-b"),
            b"same-unique-value".to_vec(),
        );
        assert!(transactions_conflict(&constraint_txn, &concurrent_insert));
        assert_eq!(
            constraint_txn.state().constraint_scanned,
            vec![(String::from("t"), String::from("d"), String::from("docs"))]
        );

        drop(node);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn open_rejects_corrupt_metadata() {
        let dir = temp_dir("meta");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("raft-meta.json"), b"not-json").unwrap();

        let result = Node::open(0, Vec::new(), Vec::new(), &dir);
        assert!(
            matches!(result, Err(RymeError::Corrupt(message)) if message.starts_with("raft metadata:"))
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn open_rejects_oversized_metadata_files() {
        for (label, file) in [("raft", "raft-meta.json"), ("range", "range-meta.json")] {
            let dir = temp_dir(label);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(file), vec![b'x'; MAX_RAFT_METADATA_BYTES + 1]).unwrap();

            let result = Node::open(0, Vec::new(), Vec::new(), &dir);
            assert!(
                matches!(result, Err(RymeError::Overload(message)) if message == format!("{label} metadata"))
            );
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn open_rejects_corrupt_log_payload() {
        let dir = temp_dir("log");
        let mut wal = ryme_wal::Wal::open(&dir, 1024 * 1024).unwrap();
        wal.append(1, &encode_log_frame(1, 1, b"invalid apply payload")).unwrap();
        wal.sync().unwrap();

        let result = Node::open(0, Vec::new(), Vec::new(), &dir);
        assert!(matches!(result, Err(RymeError::Corrupt(_))));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn conflicting_append_rewrites_suffix_and_survives_restart() {
        let dir = temp_dir("append-conflict");
        let first =
            LogEntry { index: 1, term: 1, payload: crate::encode_metadata(b"first").unwrap() };
        let old_suffix =
            LogEntry { index: 2, term: 1, payload: crate::encode_metadata(b"old").unwrap() };
        let replacement =
            LogEntry { index: 2, term: 2, payload: crate::encode_metadata(b"new").unwrap() };
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            let mut inner = node.inner.try_lock().unwrap();
            inner.persist_entries(&[first.clone(), old_suffix.clone()]).unwrap();
            inner.log.extend([first.clone(), old_suffix]);
            inner.commit_index = 1;
            inner.term = 2;
            inner.persist_meta().unwrap();
            append_to_log(&mut inner, 2, 1, 1, vec![replacement.clone()], 1).unwrap();
            assert_eq!(inner.log[1].payload, replacement.payload);
        }
        let records = ryme_wal::Wal::read_all(&dir).unwrap();
        let decoded: Vec<LogEntry> = records
            .iter()
            .map(|record| {
                let (index, term, payload) = decode_log_frame(&record.payload).unwrap();
                LogEntry { index, term, payload }
            })
            .collect();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[1].payload, replacement.payload);

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let inner = reopened.inner.try_lock().unwrap();
        assert_eq!(inner.log.len(), 2);
        assert_eq!(inner.log[1].payload, replacement.payload);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn prepared_intents_survive_restart_until_resolution() {
        let dir = temp_dir("intent");
        let key = RecordKey::new("t", "d", "s", b"pending");
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            let mut txn = node.manager.begin();
            node.manager.put(&mut txn, key.clone(), b"value".to_vec());
            let state = txn.state();
            node.prepare_intent(77, 2, state.clone(), &txn).unwrap();
            assert_eq!(node.prepared.lock().unwrap().len(), 1);
            let mut probe = node.manager.begin();
            assert_eq!(node.manager.get(&mut probe, &key).unwrap(), None);
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        assert_eq!(reopened.prepared.lock().unwrap().len(), 1);
        reopened.resolve_intent(77, 2, false).unwrap();
        assert_eq!(reopened.prepared.lock().unwrap().len(), 0);
        let mut probe = reopened.manager.begin();
        assert_eq!(reopened.manager.get(&mut probe, &key).unwrap(), None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn range_prepared_decisions_survive_restart_until_range_resolution() {
        let dir = temp_dir("range-intent");
        let key = RecordKey::new("t", "d", "docs", b"pending-range");
        let configure = |node: &Node| {
            node.set_range_owners(vec![RangeOwner {
                id: String::from("range-a"),
                start: Vec::new(),
                end: Vec::new(),
                routing_version: 0,
                owner: 0,
                replicas: vec![0],
                epoch: 1,
            }]);
        };
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            configure(&node);
            let mut txn = node.manager.begin();
            node.manager.put(&mut txn, key.clone(), b"value".to_vec());
            node.prepare_range_group("range-a", 99, 7, txn.state()).await.unwrap();
            assert_eq!(node.range_prepared.lock().unwrap().len(), 1);
            let mut conflicting = node.manager.begin();
            node.manager.put(&mut conflicting, key.clone(), b"other".to_vec());
            assert!(matches!(
                node.commit_txn(conflicting).await,
                Err(RymeError::Conflict(message)) if message == "prepared transaction conflict"
            ));
            let mut probe = node.manager.begin();
            assert_eq!(node.manager.get(&mut probe, &key).unwrap(), None);
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        assert_eq!(reopened.range_prepared.lock().unwrap().len(), 1);
        configure(&reopened);
        reopened.resolve_range_group("range-a", 99, 7, false).await.unwrap();
        assert!(reopened.range_prepared.lock().unwrap().is_empty());
        drop(reopened);

        let reopened_again = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        assert!(reopened_again.range_prepared.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn distributed_range_decision_commits_each_participant_without_global_data() {
        let dir = temp_dir("range-distributed-commit");
        let configure = |node: &Node| {
            node.set_range_owners(vec![
                RangeOwner {
                    id: String::from("range-a"),
                    start: Vec::new(),
                    end: b"m\0".to_vec(),
                    routing_version: 0,
                    owner: 0,
                    replicas: vec![0],
                    epoch: 1,
                },
                RangeOwner {
                    id: String::from("range-b"),
                    start: b"m\0".to_vec(),
                    end: Vec::new(),
                    routing_version: 0,
                    owner: 0,
                    replicas: vec![0],
                    epoch: 1,
                },
            ]);
        };
        let left = RecordKey::new("t", "d", "a", b"left");
        let right = RecordKey::new("t", "d", "z", b"right");
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            configure(&node);
            let mut txn = node.manager.begin();
            node.manager.put(&mut txn, left.clone(), b"one".to_vec());
            node.manager.put(&mut txn, right.clone(), b"two".to_vec());
            let commit_ts = node.commit_txn(txn).await.unwrap();
            assert!(commit_ts > 0);
            let mut probe = node.manager.begin();
            assert_eq!(node.manager.get(&mut probe, &left).unwrap(), Some(b"one".to_vec()));
            assert_eq!(node.manager.get(&mut probe, &right).unwrap(), Some(b"two".to_vec()));
            assert!(node.range_prepared.lock().unwrap().is_empty());
            assert!(node.range_decisions.lock().unwrap().is_empty());
            assert!(node.inner.lock().await.log.is_empty());
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        configure(&reopened);
        reopened.retry_pending_range_decisions().await;
        let mut probe = reopened.manager.begin();
        assert_eq!(reopened.manager.get(&mut probe, &left).unwrap(), Some(b"one".to_vec()));
        assert_eq!(reopened.manager.get(&mut probe, &right).unwrap(), Some(b"two".to_vec()));
        assert!(reopened.range_decisions.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn coordinator_recovery_aborts_prepared_ranges_without_a_decision() {
        let dir = temp_dir("range-coordinator-recovery");
        let configure = |node: &Node| {
            node.set_range_owners(vec![
                RangeOwner {
                    id: String::from("range-a"),
                    start: Vec::new(),
                    end: b"m\0".to_vec(),
                    routing_version: 0,
                    owner: 0,
                    replicas: vec![0],
                    epoch: 1,
                },
                RangeOwner {
                    id: String::from("range-b"),
                    start: b"m\0".to_vec(),
                    end: Vec::new(),
                    routing_version: 0,
                    owner: 0,
                    replicas: vec![0],
                    epoch: 1,
                },
            ]);
        };
        let left = RecordKey::new("t", "d", "a", b"left");
        let right = RecordKey::new("t", "d", "z", b"right");
        let participants = vec![
            ResolutionParticipant { participant: 0, group_id: String::from("range-a") },
            ResolutionParticipant { participant: 0, group_id: String::from("range-b") },
        ];
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            configure(&node);
            node.record_range_coordination("range-a", 313, 7, participants.clone()).await.unwrap();
            let mut txn = node.manager.begin();
            node.manager.put(&mut txn, left.clone(), b"one".to_vec());
            node.manager.put(&mut txn, right.clone(), b"two".to_vec());
            node.prepare_range_group("range-a", 313, 7, txn.state()).await.unwrap();
            node.prepare_range_group("range-b", 313, 7, txn.state()).await.unwrap();
            assert_eq!(node.range_prepared.lock().unwrap().len(), 2);
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        configure(&reopened);
        assert_eq!(reopened.range_recovery_coordinations.lock().unwrap().len(), 1);
        reopened.retry_pending_resolutions().await;
        assert!(reopened.range_prepared.lock().unwrap().is_empty());
        assert!(reopened.range_recovery_coordinations.lock().unwrap().is_empty());
        assert!(reopened.range_decisions.lock().unwrap().is_empty());
        let mut probe = reopened.manager.begin();
        assert_eq!(reopened.manager.get(&mut probe, &left).unwrap(), None);
        assert_eq!(reopened.manager.get(&mut probe, &right).unwrap(), None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn promoted_range_leader_aborts_unresolved_coordinator_marker() {
        let dir = temp_dir("range-leader-recovery");
        let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        node.set_range_owners(vec![RangeOwner {
            id: String::from("range-a"),
            start: Vec::new(),
            end: Vec::new(),
            routing_version: 0,
            owner: 0,
            replicas: vec![0],
            epoch: 1,
        }]);
        let participants =
            vec![ResolutionParticipant { participant: 0, group_id: String::from("range-a") }];
        node.record_range_coordination("range-a", 414, 8, participants.clone()).await.unwrap();
        node.recover_range_coordinations_for_group("range-a").await.unwrap();
        node.retry_pending_range_decisions().await;
        assert!(node.range_decisions.lock().unwrap().is_empty());
        assert!(node.range_recovery_coordinations.lock().unwrap().is_empty());
        let txn = node.manager.begin();
        assert!(matches!(
            node.prepare_range_group("range-a", 414, 8, txn.state()).await,
            Err(RymeError::Conflict(message)) if message == "transaction already aborted"
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn resolution_queue_survives_restart_until_acknowledged() {
        let dir = temp_dir("resolution-queue");
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            node.queue_resolution(77, 2, 3, false).unwrap();
            assert_eq!(node.pending_resolutions_snapshot().len(), 1);
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let pending = reopened.pending_resolutions_snapshot();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].participant, 3);
        assert!(!pending[0].committed);
        reopened.complete_resolution(&pending[0]).unwrap();
        assert!(reopened.pending_resolutions_snapshot().is_empty());

        let reopened_again = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        assert!(reopened_again.pending_resolutions_snapshot().is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn committed_resolution_record_replays_after_restart() {
        let dir = temp_dir("committed-resolution");
        let payload = serde_json::to_vec(&ResolutionRecord::Batch {
            transaction_id: 91,
            commit_ts: 12,
            committed: true,
            participants: vec![
                ResolutionParticipant { participant: 3, group_id: String::from("range-a") },
                ResolutionParticipant { participant: 4, group_id: String::from("range-b") },
            ],
        })
        .unwrap();
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            let entry = LogEntry {
                index: 1,
                term: 1,
                payload: crate::encode_resolution(&payload).unwrap(),
            };
            let mut inner = node.inner.try_lock().unwrap();
            inner.persist_entries(std::slice::from_ref(&entry)).unwrap();
            inner.log.push(entry);
            inner.term = 1;
            inner.commit_index = 1;
            inner.persist_meta().unwrap();
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let pending = reopened.pending_resolutions_snapshot();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[1].transaction_id, 91);
        assert_eq!(pending[1].participant, 4);
        assert_eq!(pending[1].group_id, "range-b");
        assert!(pending[1].committed);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn prepared_intent_rejects_conflicting_commit() {
        let dir = temp_dir("intent-conflict");
        let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let key = RecordKey::new("t", "d", "s", b"reserved");
        let mut prepared = node.manager.begin();
        node.manager.put(&mut prepared, key.clone(), b"prepared".to_vec());
        node.prepare_intent(91, 2, prepared.state(), &prepared).unwrap();

        let mut conflicting = node.manager.begin();
        node.manager.put(&mut conflicting, key, b"other".to_vec());
        assert!(matches!(
            node.commit_txn(conflicting).await,
            Err(RymeError::Conflict(message)) if message == "prepared transaction conflict"
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn committed_data_log_clears_unresolved_intent_on_restart() {
        let dir = temp_dir("intent-commit");
        let key = RecordKey::new("t", "d", "s", b"committed");
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            let mut txn = node.manager.begin();
            node.manager.put(&mut txn, key.clone(), b"value".to_vec());
            node.prepare_intent(88, 2, txn.state(), &txn).unwrap();
        }
        let mut writes = BTreeMap::new();
        writes.insert(key.clone(), ryme_txn::WriteOp::put(b"value".to_vec()));
        let mut wal = Wal::open(&dir, 64 * 1024 * 1024).unwrap();
        let apply = encode_applied(2, &encode_writes(&writes).unwrap());
        wal.append(2, &encode_log_frame(1, 1, &apply)).unwrap();
        wal.sync().unwrap();
        let meta =
            Meta { term: 1, voted_for: None, commit_index: 1, members: Vec::new(), joint: None };
        std::fs::write(dir.join("raft-meta.json"), serde_json::to_vec(&meta).unwrap()).unwrap();

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        assert_eq!(reopened.prepared.lock().unwrap().len(), 0);
        let mut probe = reopened.manager.begin();
        assert_eq!(reopened.manager.get(&mut probe, &key).unwrap(), Some(b"value".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn range_log_records_replay_after_restart() {
        let dir = temp_dir("range-log");
        let key = RecordKey::new("t", "d", "s", b"replica");
        let mut writes = BTreeMap::new();
        writes.insert(key.clone(), ryme_txn::WriteOp::put(b"value".to_vec()));
        let encoded = encode_writes(&writes).unwrap();
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            assert_eq!(
                node.append_range_follower("range-a", 3, 0, 0, 0, 1, 7, &encoded, 0).unwrap(),
                1
            );
            assert_eq!(node.range_last_index("range-a"), 1);
            let mut pending = node.manager.begin();
            assert_eq!(node.manager.get(&mut pending, &key).unwrap(), None);
            assert!(matches!(
                node.append_range_follower("range-a", 2, 0, 0, 0, 0, 0, &[], 0),
                Err(RymeError::Unavailable(message)) if message == "stale range term"
            ));
            assert_eq!(node.append_range_follower("range-a", 3, 0, 1, 3, 0, 0, &[], 1).unwrap(), 1);
            let mut committed = node.manager.begin();
            assert_eq!(node.manager.get(&mut committed, &key).unwrap(), Some(b"value".to_vec()));
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        assert_eq!(reopened.range_last_index("range-a"), 1);
        let mut probe = reopened.manager.begin();
        assert_eq!(reopened.manager.get(&mut probe, &key).unwrap(), Some(b"value".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn range_restore_trims_future_state_and_wal() {
        let dir = temp_dir("range-restore");
        let key = RecordKey::new("t", "d", "s", b"restore");
        let range = RangeOwner {
            id: String::from("range-a"),
            start: Vec::new(),
            end: Vec::new(),
            routing_version: 0,
            owner: 0,
            replicas: vec![0],
            epoch: 1,
        };
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            node.set_range_owners(vec![range.clone()]);
            for (index, commit_ts, value) in
                [(1, 5, b"before".as_slice()), (2, 10, b"after".as_slice())]
            {
                let mut writes = BTreeMap::new();
                writes.insert(key.clone(), ryme_txn::WriteOp::put(value.to_vec()));
                let encoded = encode_writes(&writes).unwrap();
                node.append_range_follower(
                    "range-a",
                    1,
                    0,
                    index - 1,
                    if index == 1 { 0 } else { 1 },
                    index,
                    commit_ts,
                    &encoded,
                    0,
                )
                .unwrap();
                node.append_range_follower("range-a", 1, 0, index, 1, 0, 0, &[], index).unwrap();
            }
            let mut current = node.manager.begin();
            assert_eq!(node.manager.get(&mut current, &key).unwrap(), Some(b"after".to_vec()));
            node.restore_to(5).await.unwrap();
            let mut restored = node.manager.begin();
            assert_eq!(node.manager.get(&mut restored, &key).unwrap(), Some(b"before".to_vec()));
            assert_eq!(node.range_last_index("range-a"), 1);
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        reopened.set_range_owners(vec![range]);
        let mut restored = reopened.manager.begin();
        assert_eq!(reopened.manager.get(&mut restored, &key).unwrap(), Some(b"before".to_vec()));
        assert_eq!(reopened.range_last_index("range-a"), 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn range_restore_aborts_prepared_intents_before_restart() {
        let dir = temp_dir("range-restore-prepared");
        let key = RecordKey::new("t", "d", "s", b"prepared-restore");
        let range = RangeOwner {
            id: String::from("range-a"),
            start: Vec::new(),
            end: Vec::new(),
            routing_version: 0,
            owner: 0,
            replicas: vec![0],
            epoch: 1,
        };
        let transaction_id = 991u128;
        let transaction_state = {
            let manager = TxnManager::new();
            let mut txn = manager.begin();
            manager.put(&mut txn, key.clone(), b"pending".to_vec());
            txn.state()
        };
        let encoded_state = serde_json::to_vec(&transaction_state).unwrap();
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            node.set_range_owners(vec![range.clone()]);
            node.append_range_follower_marked(
                "range-a",
                1,
                0,
                0,
                0,
                1,
                5,
                &encode_writes(&BTreeMap::new()).unwrap(),
                0,
                false,
                RangeEntryKind::Prepare { transaction_id, state: encoded_state },
            )
            .unwrap();
            node.append_range_follower_marked(
                "range-a",
                1,
                0,
                1,
                1,
                0,
                0,
                &[],
                1,
                false,
                RangeEntryKind::Data,
            )
            .unwrap();
            assert_eq!(node.range_prepared.lock().unwrap().len(), 1);
            node.restore_to(5).await.unwrap();
            assert_eq!(node.range_resolution("range-a", transaction_id, 5), Some(false));
            assert!(node.range_prepared.lock().unwrap().is_empty());
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        reopened.set_range_owners(vec![range]);
        assert!(reopened.range_prepared.lock().unwrap().is_empty());
        assert!(matches!(
            reopened
                .prepare_range_group("range-a", transaction_id, 5, transaction_state)
                .await,
            Err(RymeError::Conflict(message)) if message == "transaction already aborted"
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn uncommitted_range_wal_is_not_replayed_after_restart() {
        let dir = temp_dir("range-uncommitted");
        let key = RecordKey::new("t", "d", "s", b"pending-range");
        let mut writes = BTreeMap::new();
        writes.insert(key.clone(), ryme_txn::WriteOp::put(b"pending".to_vec()));
        let encoded = encode_writes(&writes).unwrap();
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            node.append_range_follower("range-a", 4, 0, 0, 0, 1, 9, &encoded, 0).unwrap();
        }

        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let mut pending = reopened.manager.begin();
        assert_eq!(reopened.manager.get(&mut pending, &key).unwrap(), None);
        reopened.append_range_follower("range-a", 4, 0, 1, 4, 0, 0, &[], 1).unwrap();
        let mut committed = reopened.manager.begin();
        assert_eq!(reopened.manager.get(&mut committed, &key).unwrap(), Some(b"pending".to_vec()));
        drop(reopened);

        let reopened_again = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let mut probe = reopened_again.manager.begin();
        assert_eq!(
            reopened_again.manager.get(&mut probe, &key).unwrap(),
            Some(b"pending".to_vec())
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn range_snapshot_honors_transaction_read_timestamp() {
        let dir = temp_dir("range-snapshot-ts");
        let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let key = RecordKey::new("t", "d", "docs", b"row");
        let mut first = BTreeMap::new();
        first.insert(key.clone(), ryme_txn::WriteOp::put(b"old".to_vec()));
        node.manager.replay_at(10, &first).unwrap();
        let mut second = BTreeMap::new();
        second.insert(key, ryme_txn::WriteOp::put(b"new".to_vec()));
        node.manager.replay_at(20, &second).unwrap();

        let snapshot =
            node.snapshot_range_for(b"docs\0", b"docs\0\xff", 10, 100, None, None).unwrap();
        assert_eq!(snapshot.snapshot_ts, 10);
        assert_eq!(snapshot.rows.len(), 1);
        assert_eq!(snapshot.rows[0].versions[0].value, Some(b"old".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rejected_range_snapshot_preserves_existing_rows_and_rejects_scope_escape() {
        let dir = temp_dir("range-snapshot-reject");
        let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let existing = RecordKey::new("t", "d", "docs", b"existing");
        let mut seed = BTreeMap::new();
        seed.insert(existing.clone(), ryme_txn::WriteOp::put(b"keep".to_vec()));
        node.manager.replay_at(10, &seed).unwrap();

        let snapshot = RangeSnapshot {
            start: b"docs\0".to_vec(),
            end: b"docs\0\xff".to_vec(),
            routing_version: ryme_storage::LEGACY_ROUTING_VERSION,
            snapshot_ts: 20,
            applied_commit: 20,
            rows: vec![RangeSnapshotRow {
                tenant: String::from("t"),
                database: String::from("d"),
                table: String::from("docs"),
                pk: b"incoming".to_vec(),
                value: b"new".to_vec(),
                expires_at: 0,
                versions: vec![
                    RangeSnapshotVersion {
                        commit_ts: 20,
                        value: Some(b"new".to_vec()),
                        expires_at: 0,
                    },
                    RangeSnapshotVersion {
                        commit_ts: 15,
                        value: Some(b"older".to_vec()),
                        expires_at: 0,
                    },
                ],
            }],
            truncated: false,
        };
        assert!(matches!(node.install_range_snapshot(&snapshot), Err(RymeError::Corrupt(_))));

        let mut probe = node.manager.begin();
        assert_eq!(node.manager.get(&mut probe, &existing).unwrap(), Some(b"keep".to_vec()));
        assert_eq!(
            node.manager.get(&mut probe, &RecordKey::new("t", "d", "docs", b"incoming")).unwrap(),
            None
        );

        let mut escaped = snapshot;
        escaped.rows[0].table = String::from("other");
        escaped.rows[0].versions = vec![RangeSnapshotVersion {
            commit_ts: 20,
            value: Some(b"escaped".to_vec()),
            expires_at: 0,
        }];
        assert!(matches!(node.install_range_snapshot(&escaped), Err(RymeError::Corrupt(_))));
        let mut probe = node.manager.begin();
        assert_eq!(node.manager.get(&mut probe, &existing).unwrap(), Some(b"keep".to_vec()));
        assert_eq!(
            node.manager.get(&mut probe, &RecordKey::new("t", "d", "other", b"incoming")).unwrap(),
            None
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn range_point_read_uses_historical_version_ttl() {
        let dir = temp_dir("range-read-ttl-ts");
        let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let key = RecordKey::new("t", "d", "docs", b"row");
        let old_expires_at = ryme_txn::now_unix() + 60;
        let new_expires_at = old_expires_at + 60;
        let mut old = BTreeMap::new();
        old.insert(key.clone(), ryme_txn::WriteOp::put_ttl(b"old".to_vec(), old_expires_at));
        node.manager.replay_at(10, &old).unwrap();
        let mut new = BTreeMap::new();
        new.insert(key.clone(), ryme_txn::WriteOp::put_ttl(b"new".to_vec(), new_expires_at));
        node.manager.replay_at(20, &new).unwrap();

        assert_eq!(
            node.read_range_value(&key, 10).unwrap(),
            (Some(b"old".to_vec()), Some(old_expires_at))
        );
        assert_eq!(
            node.read_range_value(&key, 0).unwrap(),
            (Some(b"new".to_vec()), Some(new_expires_at))
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn divergent_range_suffix_is_replaced_and_persisted() {
        let dir = temp_dir("range-conflict");
        let first_key = RecordKey::new("t", "d", "s", b"first");
        let old_key = RecordKey::new("t", "d", "s", b"old");
        let new_key = RecordKey::new("t", "d", "s", b"new");
        let mut first = BTreeMap::new();
        first.insert(first_key, ryme_txn::WriteOp::put(b"one".to_vec()));
        let mut old = BTreeMap::new();
        old.insert(old_key, ryme_txn::WriteOp::put(b"old".to_vec()));
        let mut new = BTreeMap::new();
        new.insert(new_key.clone(), ryme_txn::WriteOp::put(b"new".to_vec()));
        let first = encode_writes(&first).unwrap();
        let old = encode_writes(&old).unwrap();
        let new = encode_writes(&new).unwrap();
        {
            let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
            node.append_range_follower("range-a", 1, 0, 0, 0, 1, 7, &first, 0).unwrap();
            node.append_range_follower("range-a", 1, 0, 1, 1, 2, 8, &old, 0).unwrap();
            node.append_range_follower("range-a", 2, 1, 1, 1, 2, 9, &new, 0).unwrap();
            assert_eq!(node.range_last_term("range-a"), 2);
            assert_eq!(node.range_entry("range-a", 2).unwrap().writes, new);
            assert_eq!(Wal::read_all(&node.range_wal_path).unwrap().len(), 2);

            node.append_range_follower("range-a", 2, 1, 2, 2, 0, 0, &[], 2).unwrap();
            let mut probe = node.manager.begin();
            assert_eq!(node.manager.get(&mut probe, &new_key).unwrap(), Some(b"new".to_vec()));

            let result = node.append_range_follower("range-a", 3, 2, 1, 1, 2, 10, &old, 0);
            assert!(
                matches!(result, Err(RymeError::Conflict(message)) if message == "committed range suffix")
            );
        }
        let reopened = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        assert_eq!(reopened.range_last_term("range-a"), 2);
        assert_eq!(Wal::read_all(&reopened.range_wal_path).unwrap().len(), 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn transferred_range_purges_only_an_unchanged_source() {
        let dir = temp_dir("range-transfer-purge");
        let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        let key = RecordKey::new("t", "d", "docs", b"row");
        let mut writes = BTreeMap::new();
        writes.insert(key.clone(), ryme_txn::WriteOp::put(b"before".to_vec()));
        node.manager.replay_at(10, &writes).unwrap();
        let snapshot = node.snapshot_range(b"docs\0", b"docs\0\xff", 0, 100).unwrap();

        node.set_range_owners(vec![RangeOwner {
            id: String::from("range-a"),
            start: b"docs\0".to_vec(),
            end: b"docs\0\xff".to_vec(),
            routing_version: 0,
            owner: 1,
            replicas: vec![1],
            epoch: 2,
        }]);
        assert_eq!(node.purge_range_after_transfer("range-a", 2, &snapshot).await.unwrap(), 1);
        let mut probe = node.manager.begin();
        assert_eq!(node.manager.get(&mut probe, &key).unwrap(), None);

        let mut changed = BTreeMap::new();
        changed.insert(key.clone(), ryme_txn::WriteOp::put(b"after".to_vec()));
        node.manager.replay_at(20, &changed).unwrap();
        let changed_snapshot = node.snapshot_range(b"docs\0", b"docs\0\xff", 0, 100).unwrap();
        node.manager.replay_at(30, &changed).unwrap();
        assert!(matches!(
            node.purge_range_after_transfer("range-a", 2, &changed_snapshot).await,
            Err(RymeError::Conflict(message)) if message == "range changed during transfer"
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn restore_rebuilds_a_whole_dataset_from_the_committed_log() {
        let dir = temp_dir("raft-restore");
        let key = RecordKey::new("t", "d", "docs", b"row");
        let mut first = BTreeMap::new();
        first.insert(key.clone(), ryme_txn::WriteOp::put(b"before".to_vec()));
        let mut second = BTreeMap::new();
        second.insert(key.clone(), ryme_txn::WriteOp::put(b"after".to_vec()));
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                payload: encode_applied(10, &encode_writes(&first).unwrap()),
            },
            LogEntry {
                index: 2,
                term: 1,
                payload: encode_applied(20, &encode_writes(&second).unwrap()),
            },
            LogEntry { index: 3, term: 1, payload: crate::encode_restore(10) },
        ];
        let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        {
            let mut inner = node.inner.lock().await;
            inner.persist_entries(&entries).unwrap();
            inner.log.extend(entries);
            inner.commit_index = 3;
            inner.term = 1;
        }
        node.apply_ready().await.unwrap();

        let mut probe = node.manager.begin();
        assert_eq!(node.manager.get(&mut probe, &key).unwrap(), Some(b"before".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn resolution_queue_uses_the_current_range_leader() {
        let dir = temp_dir("resolution-current-owner");
        let node = Node::open(0, Vec::new(), Vec::new(), &dir).unwrap();
        node.set_range_owners(vec![RangeOwner {
            id: String::from("range-a"),
            start: Vec::new(),
            end: Vec::new(),
            routing_version: 0,
            owner: 2,
            replicas: vec![1, 2],
            epoch: 4,
        }]);
        let mut participants = BTreeMap::new();
        participants.insert(
            String::from("range-a"),
            RangeOwner {
                id: String::from("range-a"),
                start: Vec::new(),
                end: Vec::new(),
                routing_version: 0,
                owner: 1,
                replicas: vec![1, 2],
                epoch: 4,
            },
        );
        node.queue_range_resolutions(55, 9, &participants, true).unwrap();
        let pending = node.pending_resolutions_snapshot();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].participant, 2);
        let _ = std::fs::remove_dir_all(dir);
    }
}
