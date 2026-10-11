use crate::{decode_apply, encode_applied, ApplyPayload, LogEntry, Role};
use ryme_error::{Result, RymeError};
use ryme_storage::{RecordKey, TableVersion};
use ryme_txn::{decode_writes, encode_writes, now_unix, TxnManager};
use ryme_wal::Wal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RangeEntryKind {
    Data,
    Prepare { transaction_id: u128, state: Vec<u8> },
    Resolve { transaction_id: u128, committed: bool },
}

impl Default for RangeEntryKind {
    fn default() -> Self {
        Self::Data
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResolutionParticipant {
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

fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut next = prefix.to_vec();
    for index in (0..next.len()).rev() {
        if next[index] < u8::MAX {
            next[index] += 1;
            next.truncate(index + 1);
            return next;
        }
    }
    Vec::new()
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
    let body = serde_json::to_vec(rpc).map_err(|e| RymeError::Internal(e.to_string()))?;
    if body.len() > 16 * 1024 * 1024 {
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
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await.map_err(|e| RymeError::Io(e.to_string()))?;
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 || length > 16 * 1024 * 1024 {
        return Err(RymeError::InvalidArgument(String::from("rpc frame")));
    }
    let mut body = vec![0u8; length];
    socket.read_exact(&mut body).await.map_err(|e| RymeError::Io(e.to_string()))?;
    serde_json::from_slice(&body).map_err(|_| RymeError::Corrupt(String::from("rpc decode")))
}

#[derive(Debug)]
pub struct PeerPool {
    addr: String,
    conn: Mutex<Option<MeshStream>>,
    dials: std::sync::atomic::AtomicU64,
    isolated: std::sync::atomic::AtomicBool,
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
            tls: std::sync::Mutex::new(None),
        })
    }

    pub fn set_connector(&self, connector: Option<ryme_tls::MeshConnector>) {
        if let Ok(mut guard) = self.tls.lock() {
            *guard = connector;
        }
        self.purge();
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
        let connector = self.tls.lock().ok().and_then(|guard| guard.clone());
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
    ) as usize;
    if input.len() != 24 + length {
        return Err(RymeError::Corrupt(String::from("log frame")));
    }
    Ok((index, term, input[24..].to_vec()))
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
    leader_contact_ms: u64,
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

    fn advance_commit(&mut self) {
        let mut target = committed_in(&self.acks, &self.current);
        if let Some(joint) = self.joint.as_ref() {
            target = target.min(committed_in(&self.acks, joint));
        }
        if target <= self.commit_index {
            return;
        }
        let mut candidate = target;
        while candidate > self.commit_index {
            if self.log.iter().any(|e| e.index == candidate && e.term == self.term) {
                self.commit_index = candidate;
                let _ = self.persist_meta();
                return;
            }
            candidate -= 1;
        }
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
        let tmp = self.meta_path.with_extension("tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&raw)?;
            file.sync_data()?;
        }
        std::fs::rename(&tmp, &self.meta_path)?;
        Ok(())
    }

    fn persist_entries(&mut self, entries: &[LogEntry]) -> Result<()> {
        for entry in entries {
            let frame = encode_log_frame(entry.index, entry.term, &entry.payload);
            self.wal.append(entry.term, &frame)?;
        }
        self.wal.sync()?;
        Ok(())
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
    range_logs: std::sync::Mutex<BTreeMap<String, u64>>,
    range_entries: std::sync::Mutex<BTreeMap<String, BTreeMap<u64, RangeLogRecord>>>,
    range_groups: std::sync::Mutex<BTreeMap<String, RangeGroupState>>,
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
        let meta = match std::fs::read(&meta_path) {
            Ok(raw) => serde_json::from_slice::<Meta>(&raw)
                .map_err(|error| RymeError::Corrupt(format!("raft metadata: {error}")))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Meta { term: 0, voted_for: None, commit_index: 0, members: Vec::new(), joint: None }
            }
            Err(e) => return Err(RymeError::from(e)),
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
                IntentRecord::Resolve { transaction_id, committed, .. } => {
                    if committed {
                        prepared.remove(&transaction_id);
                    } else {
                        prepared.remove(&transaction_id);
                    }
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
            range_records.push(range_record);
        }
        let range_meta_path = dir.join("range-meta.json");
        let mut range_groups: BTreeMap<String, RangeGroupState> =
            match std::fs::read(&range_meta_path) {
                Ok(raw) => serde_json::from_slice(&raw)
                    .map_err(|error| RymeError::Corrupt(format!("range metadata: {error}")))?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
                Err(error) => return Err(RymeError::from(error)),
            };
        let manager = TxnManager::new();
        let mut log: Vec<LogEntry> = Vec::new();
        for record in records {
            let (index, term, apply) = decode_log_frame(&record.payload)?;
            if log.iter().any(|e: &LogEntry| e.index == index) {
                continue;
            }
            validate_apply_payload(&apply)?;
            log.push(LogEntry { index, term, payload: apply });
        }
        log.sort_by_key(|e| e.index);
        let mut applied = 0u64;
        let mut replayed_commit_ts = BTreeSet::new();
        let mut replayed: Option<crate::ConfChange> = None;
        let mut replayed_joint: Option<Vec<Member>> = None;
        let mut metadata = None;
        let mut replayed_resolutions = Vec::new();
        for entry in log.iter().filter(|e| e.index <= meta.commit_index) {
            match decode_apply(&entry.payload)? {
                ApplyPayload::Data { commit_ts, writes } => {
                    let writes = decode_writes(&writes)?;
                    manager.replay_at(commit_ts, &writes)?;
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
            }
            applied = entry.index;
        }
        let mut range_logs = BTreeMap::new();
        let mut range_entries: BTreeMap<String, BTreeMap<u64, RangeLogRecord>> = BTreeMap::new();
        let mut range_prepared = BTreeMap::new();
        for range_record in range_records {
            let group_id = range_record.group_id.clone();
            range_entries
                .entry(group_id.clone())
                .or_default()
                .insert(range_record.index, range_record.clone());
            let last = range_logs.entry(group_id.clone()).or_insert(0);
            *last = (*last).max(range_record.index);
            let state = range_groups.entry(group_id).or_default();
            state.term = state.term.max(range_record.term).max(1);
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
                    RangeEntryKind::Resolve { transaction_id, .. } => {
                        range_prepared.remove(&(group_id.clone(), *transaction_id));
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
                leader_contact_ms: 0,
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
            range_logs: std::sync::Mutex::new(range_logs),
            range_entries: std::sync::Mutex::new(range_entries),
            range_groups: std::sync::Mutex::new(range_groups),
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
        Duration::from_millis(180 + (*rng % 140))
    }

    pub fn spawn(self: &Arc<Self>, listener: TcpListener) -> Vec<tokio::task::JoinHandle<()>> {
        let accept = self.clone();
        let connections = self.conn_tasks.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let _ = socket.set_nodelay(true);
                let node = accept.clone();
                let task = tokio::spawn(async move {
                    if let Some(transport) =
                        node.mesh_tls.lock().ok().and_then(|guard| guard.clone())
                    {
                        let Ok(tls) = transport.acceptor.acceptor().accept(socket).await else {
                            return;
                        };
                        let _ = node.serve_connection(tls).await;
                        return;
                    }
                    let _ = node.serve_connection(socket).await;
                });
                connections.lock().unwrap_or_else(|e| e.into_inner()).push(task);
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
        let transport = self.mesh_tls.lock().ok().and_then(|guard| guard.clone());
        if let Some(transport) = transport {
            pool.set_connector(Some(transport.connector.clone()));
        }
    }

    pub fn set_mesh_tls(&self, transport: MeshTransport) {
        let transport = Arc::new(transport);
        if let Ok(mut guard) = self.mesh_tls.lock() {
            *guard = Some(transport.clone());
        }
        for (_, pool) in self.pools_snapshot() {
            pool.set_connector(Some(transport.connector.clone()));
        }
    }

    pub fn mesh_enabled(&self) -> bool {
        self.mesh_tls.lock().ok().and_then(|guard| guard.clone()).is_some()
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
        let now = now_ms();
        for configured in self.range_owners() {
            let range = self.with_current_range_leader(configured);
            if !range.replicas.contains(&self.id) {
                continue;
            }
            let state = self.ensure_range_group(&range.id, range.owner);
            if state.leader == Some(self.id) {
                if now.saturating_sub(state.last_heartbeat_ms) >= 100 {
                    if let Ok(mut groups) = self.range_groups.lock() {
                        if let Some(current) = groups.get_mut(&range.id) {
                            current.last_heartbeat_ms = now;
                            current.last_contact_ms = now;
                        }
                    }
                    let _ = self.persist_range_meta();
                    let _ = self.range_heartbeat(&range.id).await;
                }
                continue;
            }
            if state.last_contact_ms == 0 {
                if let Ok(mut groups) = self.range_groups.lock() {
                    if let Some(current) = groups.get_mut(&range.id) {
                        current.last_contact_ms = now;
                    }
                }
                let _ = self.persist_range_meta();
                continue;
            }
            if now.saturating_sub(state.last_contact_ms) >= self.range_timeout_ms(&range.id) {
                self.campaign_range(&range).await;
            }
        }
    }

    async fn range_heartbeat(self: &Arc<Self>, group_id: &str) -> Result<()> {
        let range = self
            .range_owner_by_id(group_id)
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.owner != self.id {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        let state = self.ensure_range_group(group_id, self.id);
        let last_index = self.range_last_index(group_id);
        let last_term = if last_index == 0 { 0 } else { self.range_last_term(group_id) };
        for replica in range.replicas.iter().copied().filter(|replica| *replica != self.id) {
            let _ = self
                .sync_range_replica(
                    group_id,
                    replica,
                    state.term.max(1),
                    last_index,
                    last_term,
                    state.commit_index,
                )
                .await;
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
                    self.observe_range_term(group_id, peer_term);
                    return Err(RymeError::Unavailable(String::from("stale range leader")));
                }
                if appended {
                    return Ok(());
                }
                conflict_index
                    .unwrap_or_else(|| last_index.min(leader_last).saturating_add(1))
                    .max(1)
            }
            _ => return Err(RymeError::Corrupt(String::from("range append rpc"))),
        };
        let mut same_index_retries = 0u8;
        while next_index <= leader_last {
            let index = next_index;
            let record = self
                .range_entry(group_id, index)
                .ok_or_else(|| RymeError::Unavailable(String::from("range log gap")))?;
            let prev_index = index - 1;
            let prev_term = if prev_index == 0 {
                0
            } else {
                self.range_entry(group_id, prev_index)
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
                        self.observe_range_term(group_id, peer_term);
                        return Err(RymeError::Unavailable(String::from("stale range leader")));
                    }
                    next_index = last_index + 1;
                    same_index_retries = 0;
                }
                Rpc::RangeAppendResponse {
                    term: peer_term,
                    appended: false,
                    last_index,
                    conflict_index,
                    ..
                } => {
                    if peer_term > term {
                        self.observe_range_term(group_id, peer_term);
                        return Err(RymeError::Unavailable(String::from("stale range leader")));
                    }
                    let retry_index = conflict_index
                        .unwrap_or_else(|| last_index.min(leader_last).saturating_add(1))
                        .max(1);
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
            Rpc::RangeAppendResponse { term: peer_term, appended: true, .. }
                if peer_term <= term =>
            {
                Ok(())
            }
            Rpc::RangeAppendResponse { term: peer_term, .. } if peer_term > term => {
                self.observe_range_term(group_id, peer_term);
                Err(RymeError::Unavailable(String::from("stale range leader")))
            }
            _ => Err(RymeError::Unavailable(String::from("range commit gap"))),
        }
    }

    fn observe_range_term(&self, group_id: &str, term: u64) {
        let mut changed = false;
        if let Ok(mut groups) = self.range_groups.lock() {
            let state = groups.entry(group_id.to_string()).or_default();
            if term > state.term {
                state.term = term;
                state.voted_for = None;
                state.leader = None;
                state.last_contact_ms = now_ms();
                changed = true;
            }
        }
        if changed {
            if let Ok(mut leaders) = self.range_leaders.write() {
                leaders.remove(group_id);
            }
            let _ = self.persist_range_meta();
        }
    }

    async fn campaign_range(self: &Arc<Self>, range: &RangeOwner) {
        let (term, last_term, last_index) = {
            let mut groups = self.range_groups.lock().unwrap_or_else(|error| error.into_inner());
            let state = groups.entry(range.id.clone()).or_default();
            if state.leader == Some(self.id) {
                return;
            }
            state.term = state.term.max(1).saturating_add(1);
            state.voted_for = Some(self.id);
            state.leader = None;
            state.last_contact_ms = now_ms();
            (state.term, self.range_last_term(&range.id), self.range_last_index(&range.id))
        };
        let _ = self.persist_range_meta();
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
            match response {
                Rpc::RangeVoteResponse { term: peer_term, granted } => {
                    if peer_term > term {
                        self.observe_range_term(&range.id, peer_term);
                        return;
                    }
                    if granted && !votes.contains(&replica) {
                        votes.push(replica);
                    }
                }
                _ => {}
            }
        }
        if votes.len() < majority(range.replicas.len()) {
            return;
        }
        let won = if let Ok(mut groups) = self.range_groups.lock() {
            let state = groups.entry(range.id.clone()).or_default();
            if state.term == term && state.voted_for == Some(self.id) && state.leader.is_none() {
                state.leader = Some(self.id);
                state.last_contact_ms = now_ms();
                state.last_heartbeat_ms = 0;
                true
            } else {
                false
            }
        } else {
            false
        };
        if !won {
            return;
        }
        if let Ok(mut leaders) = self.range_leaders.write() {
            leaders.insert(range.id.clone(), self.id);
        }
        let _ = self.persist_range_meta();
        let _ = self.range_heartbeat(&range.id).await;
    }

    fn handle_range_vote(
        &self,
        group_id: &str,
        term: u64,
        candidate: usize,
        last_term: u64,
        last_index: u64,
    ) -> (u64, bool) {
        let Some(range) = self.range_owners().into_iter().find(|range| range.id == group_id) else {
            return (self.range_term(group_id), false);
        };
        if !range.replicas.contains(&self.id) || !range.replicas.contains(&candidate) {
            return (self.range_term(group_id), false);
        }
        let mut groups = self.range_groups.lock().unwrap_or_else(|error| error.into_inner());
        let state = groups.entry(group_id.to_string()).or_default();
        if term < state.term {
            return (state.term.max(1), false);
        }
        if term > state.term {
            state.term = term;
            state.voted_for = None;
            state.leader = None;
        }
        let local_last_term = self.range_last_term(group_id);
        let local_last_index = self.range_last_index(group_id);
        let current_vote = state.voted_for;
        let up_to_date = last_term > local_last_term
            || (last_term == local_last_term && last_index >= local_last_index);
        let granted = up_to_date && (current_vote.is_none() || current_vote == Some(candidate));
        if granted {
            state.voted_for = Some(candidate);
            state.leader = None;
            state.last_contact_ms = now_ms();
        }
        let response_term = state.term.max(1);
        drop(groups);
        if granted {
            let _ = self.persist_range_meta();
        }
        (response_term, granted)
    }

    async fn main_loop(self: Arc<Self>) {
        loop {
            self.range_tick().await;
            self.retry_pending_resolutions().await;
            if self.take_campaign().await {
                self.campaign(true).await;
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
                    self.campaign(false).await;
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

    async fn campaign(self: &Arc<Self>, skip_prevote: bool) {
        if self.inner.lock().await.learner {
            return;
        }
        if !skip_prevote && !self.prevote().await {
            return;
        }
        let (term, last_term, last_index) = {
            let mut inner = self.inner.lock().await;
            if inner.role == Role::Leader {
                return;
            }
            inner.term += 1;
            inner.role = Role::Candidate;
            inner.voted_for = Some(self.id);
            inner.leader_id = None;
            let _ = inner.persist_meta();
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
                    let _ = inner.persist_meta();
                    return;
                }
                if granted && inner.term == term && !votes.contains(&peer) {
                    votes.push(peer);
                }
            }
        }
        let mut inner = self.inner.lock().await;
        if inner.term != term || inner.role != Role::Candidate {
            return;
        }
        let win_current = votes_win(&votes, &inner.current);
        let win_joint = inner.joint.as_ref().map(|joint| votes_win(&votes, joint)).unwrap_or(true);
        if win_current && win_joint {
            inner.role = Role::Leader;
            inner.leader_id = Some(self.id);
            let last = inner.last_index();
            inner.next_index.clear();
            inner.acks.clear();
            let mut ids: Vec<usize> = inner.current.iter().map(|member| member.id).collect();
            if let Some(joint) = inner.joint.as_ref() {
                ids.extend(joint.iter().map(|member| member.id));
            }
            ids.sort_unstable();
            ids.dedup();
            for id in ids {
                inner.next_index.insert(id, last + 1);
                inner.acks.insert(id, 0);
            }
            if let Some(slot) = inner.acks.get_mut(&self.id) {
                *slot = last;
            }
        } else {
            inner.role = Role::Follower;
            inner.leader_id = None;
        }
    }

    async fn prevote(&self) -> bool {
        let (prospective, last_term, last_index) = {
            let inner = self.inner.lock().await;
            if inner.role == Role::Leader {
                return false;
            }
            let (last_term, last_index) = inner.last_position();
            (inner.term + 1, last_term, last_index)
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
                            let _ = inner.persist_meta();
                        }
                        return false;
                    }
                    if granted && !votes.contains(&peer) {
                        votes.push(peer);
                    }
                }
                _ => continue,
            }
        }
        let inner = self.inner.lock().await;
        votes_win(&votes, &inner.current)
            && inner.joint.as_ref().map(|joint| votes_win(&votes, joint)).unwrap_or(true)
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
            self.replicate_once().await;
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

    pub fn pool(&self, target: usize) -> Option<Arc<PeerPool>> {
        self.pool_for(target)
    }

    async fn replicate_all(self: &Arc<Self>) {
        self.replicate_once().await;
        if let Err(error) = self.apply_ready().await {
            tracing::error!(node = self.id, %error, "raft state machine apply failed");
            self.set_isolated(true);
        }
    }

    async fn replicate_once(self: &Arc<Self>) {
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
            let _ = task.await;
        }
    }

    async fn replicate_to(
        &self,
        peer: usize,
        pool: Arc<PeerPool>,
        term: u64,
        leader_commit: u64,
        snapshot: Arc<Vec<LogEntry>>,
    ) {
        let next = {
            let inner = self.inner.lock().await;
            inner.next_index.get(&peer).copied().unwrap_or(1)
        };
        let prev = snapshot.iter().rev().find(|e| e.index < next);
        let (prev_index, prev_term) = prev.map(|e| (e.index, e.term)).unwrap_or((0, 0));
        let entries: Vec<LogEntry> = snapshot.iter().filter(|e| e.index >= next).cloned().collect();
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
                    let _ = inner.persist_meta();
                    return;
                }
                if inner.term != term || inner.role != Role::Leader {
                    return;
                }
                if ok {
                    let next = inner.next_index.entry(peer).or_insert(1);
                    *next = match_index + 1;
                    let ack = inner.acks.entry(peer).or_insert(0);
                    *ack = match_index.max(*ack);
                    inner.advance_commit();
                } else {
                    let next = inner.next_index.entry(peer).or_insert(1);
                    *next = match_index.saturating_add(1).max(1);
                }
            }
            Ok(_) => {}
            Err(_) => {}
        }
    }

    async fn apply_ready(&self) -> Result<()> {
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
                    if self.range_owners().is_empty() {
                        self.manager.replay_at(commit_ts, &writes)?;
                    } else {
                        self.manager.replay_at_filtered(commit_ts, &writes, |key| {
                            self.owns_range_key(key)
                        })?;
                    }
                    if self.inner.lock().await.role != Role::Leader {
                        self.apply_data_hook(crate::encode_applied(commit_ts, &encoded_writes))?;
                    }
                    self.clear_prepared_for_commit(commit_ts)?;
                }
                ApplyPayload::Conf { change } => {
                    self.apply_conf(change).await;
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

    async fn apply_conf(&self, change: crate::ConfChange) {
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
            let _ = inner.persist_meta();
            let mut members = inner.current.clone();
            if let Some(joint) = inner.joint.as_ref() {
                members.extend(joint.iter().cloned());
            }
            members
        };
        self.reconcile_pools(&members);
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
            let rpc =
                match tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await {
                    Ok(Ok(rpc)) => rpc,
                    _ => return Ok(()),
                };
            let response = self.handle_rpc(rpc).await?;
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
                if term < inner.term {
                    return Ok(Rpc::VoteResponse { term: inner.term, granted: false });
                }
                if !inner.is_member(candidate) && recent_leader(inner.leader_contact_ms) {
                    return Ok(Rpc::VoteResponse { term: inner.term, granted: false });
                }
                if term > inner.term {
                    inner.term = term;
                    inner.role = Role::Follower;
                    inner.voted_for = None;
                }
                let already = inner.voted_for.is_some() && inner.voted_for != Some(candidate);
                if already {
                    let term = inner.term;
                    return Ok(Rpc::VoteResponse { term, granted: false });
                }
                let (last_t, last_i) = inner.last_position();
                if last_term < last_t || (last_term == last_t && last_index < last_i) {
                    let term = inner.term;
                    return Ok(Rpc::VoteResponse { term, granted: false });
                }
                inner.voted_for = Some(candidate);
                inner.bump_reset();
                let _ = inner.persist_meta();
                Ok(Rpc::VoteResponse { term: inner.term, granted: true })
            }
            Rpc::PreVoteRequest { term, candidate, last_term, last_index } => {
                let inner = self.inner.lock().await;
                if term <= inner.term {
                    return Ok(Rpc::PreVoteResponse { term: inner.term, granted: false });
                }
                if !inner.is_member(candidate) && recent_leader(inner.leader_contact_ms) {
                    return Ok(Rpc::PreVoteResponse { term: inner.term, granted: false });
                }
                let (last_t, last_i) = inner.last_position();
                if last_term < last_t || (last_term == last_t && last_index < last_i) {
                    return Ok(Rpc::PreVoteResponse { term: inner.term, granted: false });
                }
                Ok(Rpc::PreVoteResponse { term: inner.term, granted: true })
            }
            Rpc::TransferRequest { term, from: _ } => {
                let mut inner = self.inner.lock().await;
                if term < inner.term {
                    return Ok(Rpc::TransferResponse { term: inner.term, accepted: false });
                }
                if term > inner.term {
                    inner.term = term;
                    inner.voted_for = None;
                    let _ = inner.persist_meta();
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
                let txn = self.manager.from_state(state);
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
                let txn = self.manager.from_state(state.clone());
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
            Rpc::RangeVoteRequest { group_id, term, candidate, last_term, last_index } => {
                let (term, granted) =
                    self.handle_range_vote(&group_id, term, candidate, last_term, last_index);
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
                        term: self.range_term(&group_id),
                        appended: true,
                        last_index,
                        conflict_index: None,
                        error: None,
                    }),
                    Err(error) => Ok(Rpc::RangeAppendResponse {
                        term: self.range_term(&group_id),
                        appended: false,
                        last_index: self.range_last_index(&group_id),
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
                        ),
                        error: Some(error.into()),
                    }),
                }
            }
            Rpc::RangeSnapshotRequest { start, end, read_ts, max_rows, tenant, database } => {
                let snapshot = self.snapshot_range_for(
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
                let (result, applied_now) = {
                    let mut inner = self.inner.lock().await;
                    if term < inner.term {
                        let term = inner.term;
                        (
                            Rpc::AppendResponse {
                                term,
                                ok: false,
                                match_index: inner.last_index(),
                            },
                            false,
                        )
                    } else {
                        if term > inner.term {
                            inner.term = term;
                            inner.voted_for = None;
                        }
                        inner.role = Role::Follower;
                        inner.leader_id = Some(leader);
                        inner.bump_reset();
                        inner.leader_contact_ms = now_ms();
                        let outcome = append_to_log(
                            &mut inner,
                            prev_index,
                            prev_term,
                            entries,
                            leader_commit,
                        );
                        match outcome {
                            Ok(matched) => {
                                let _ = inner.persist_meta();
                                (
                                    Rpc::AppendResponse {
                                        term: inner.term,
                                        ok: true,
                                        match_index: matched,
                                    },
                                    true,
                                )
                            }
                            Err(_) => {
                                let term = inner.term;
                                (
                                    Rpc::AppendResponse {
                                        term,
                                        ok: false,
                                        match_index: inner.last_index(),
                                    },
                                    false,
                                )
                            }
                        }
                    }
                };
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
        if let Some((group_id, range_owner)) = self.range_group_for_writes(&writes) {
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
    ) -> Option<(String, usize)> {
        let mut group = None;
        for key in writes.keys() {
            let range = self.range_for_record(key)?;
            match &group {
                None => group = Some((range.id, range.owner)),
                Some((group_id, owner)) if *group_id == range.id && *owner == range.owner => {}
                Some(_) => return None,
            }
        }
        group
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

    async fn propose_resolutions(
        self: &Arc<Self>,
        transaction_id: u128,
        commit_ts: u64,
        participants: &BTreeMap<String, RangeOwner>,
        committed: bool,
    ) -> Result<u64> {
        let participants = participants
            .iter()
            .map(|(group_id, range)| ResolutionParticipant {
                participant: range.owner,
                group_id: group_id.clone(),
            })
            .collect();
        let payload = serde_json::to_vec(&ResolutionRecord::Batch {
            transaction_id,
            commit_ts,
            committed,
            participants,
        })
        .map_err(|error| RymeError::Internal(format!("resolution: {error}")))?;
        let index = self.propose_frame(crate::encode_resolution(&payload)?).await?;
        self.wait_applied(index).await?;
        Ok(index)
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
        wal.append(commit_ts, &payload)?;
        wal.sync()
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
        wal.append(commit_ts, &payload)?;
        wal.sync()?;
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
        wal.append(resolution.commit_ts, &payload)?;
        wal.sync()?;
        pending.remove(&key);
        Ok(())
    }

    fn pending_resolutions_snapshot(&self) -> Vec<PendingResolution> {
        self.pending_resolutions
            .lock()
            .map(|pending| pending.values().cloned().collect())
            .unwrap_or_default()
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
            let other = self.manager.from_state(existing.state.clone());
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
                let other = self.manager.from_state(existing.state.clone());
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
            let other = self.manager.from_state(existing.state.clone());
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
            let other = self.manager.from_state(existing.state.clone());
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
            .range_owner_by_id(group_id)
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.owner != self.id {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        let txn = self.manager.from_state(state.clone());
        if let Some(expected) = state.route_generation {
            if self.range_generation() != expected {
                return Err(RymeError::Conflict(String::from("range topology changed")));
            }
        }
        self.manager.validate_at(&txn, commit_ts)?;
        self.range_prepare_conflict(group_id, transaction_id, commit_ts, &txn, &state)?;
        let encoded = serde_json::to_vec(&state)
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
            .range_owner_by_id(group_id)
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.owner != self.id {
            return Err(RymeError::Unavailable(String::from("not range leader")));
        }
        let existing = self
            .range_prepared
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range prepared transactions")))?
            .get(&(group_id.to_string(), transaction_id))
            .cloned();
        let Some(existing) = existing else {
            return Ok(());
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

    fn range_for_record(&self, key: &RecordKey) -> Option<RangeOwner> {
        let mut routing = Vec::with_capacity(key.table.len() + key.pk.len() + 1);
        routing.extend_from_slice(key.table.as_bytes());
        routing.push(0);
        routing.extend_from_slice(&key.pk);
        self.range_owners()
            .into_iter()
            .find(|range| {
                routing.as_slice() >= range.start.as_slice()
                    && (range.end.is_empty() || routing.as_slice() < range.end.as_slice())
            })
            .map(|range| self.with_current_range_leader(range))
    }

    fn with_current_range_leader(&self, mut range: RangeOwner) -> RangeOwner {
        if let Some(leader) =
            self.range_leaders.read().ok().and_then(|leaders| leaders.get(&range.id).copied())
        {
            range.owner = leader;
        }
        range
    }

    fn range_last_index(&self, group_id: &str) -> u64 {
        self.range_logs.lock().ok().and_then(|logs| logs.get(group_id).copied()).unwrap_or(0)
    }

    fn range_last_term(&self, group_id: &str) -> u64 {
        let last = self.range_last_index(group_id);
        self.range_entries
            .lock()
            .ok()
            .and_then(|groups| groups.get(group_id)?.get(&last).map(|record| record.term))
            .unwrap_or(0)
    }

    fn range_entry(&self, group_id: &str, index: u64) -> Option<RangeLogRecord> {
        self.range_entries.lock().ok().and_then(|groups| groups.get(group_id)?.get(&index).cloned())
    }

    fn ensure_range_group(&self, group_id: &str, leader: usize) -> RangeGroupState {
        let mut groups = self.range_groups.lock().unwrap_or_else(|error| error.into_inner());
        let state = groups.entry(group_id.to_string()).or_default();
        if state.term == 0 {
            state.term = 1;
        }
        if state.leader.is_none() {
            state.leader = Some(leader);
        }
        state.clone()
    }

    fn range_term(&self, group_id: &str) -> u64 {
        self.range_groups
            .lock()
            .ok()
            .and_then(|groups| groups.get(group_id).map(|state| state.term))
            .unwrap_or(1)
            .max(1)
    }

    fn persist_range_meta(&self) -> Result<()> {
        let states = self
            .range_groups
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range metadata lock")))?
            .clone();
        let raw = serde_json::to_vec(&states)
            .map_err(|error| RymeError::Internal(format!("range metadata: {error}")))?;
        let temporary = self.range_meta_path.with_extension("json.tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(&raw)?;
            file.sync_data()?;
        }
        std::fs::rename(temporary, &self.range_meta_path)?;
        Ok(())
    }

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
        if index != last + 1 {
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
        wal.append(commit_ts, &record)?;
        wal.sync()?;
        self.range_entries
            .lock()
            .map_err(|_| RymeError::Internal(String::from("range entries")))?
            .entry(group_id.to_string())
            .or_default()
            .insert(
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

        let records = Wal::read_all(&self.range_wal_path)?;
        let mut kept = Vec::with_capacity(records.len());
        for record in &records {
            let range_record: RangeLogRecord = serde_json::from_slice(&record.payload)
                .map_err(|error| RymeError::Corrupt(format!("range log: {error}")))?;
            if range_record.group_id != group_id || range_record.index <= keep_through {
                kept.push(record.clone());
            }
        }
        if kept.len() != records.len() {
            self.range_wal
                .lock()
                .map_err(|_| RymeError::Internal(String::from("range wal")))?
                .replace_records(&kept)?;
        }
        if let Ok(mut entries) = self.range_entries.lock() {
            if let Some(group) = entries.get_mut(group_id) {
                group.retain(|index, _| *index <= keep_through);
                if group.is_empty() {
                    entries.remove(group_id);
                }
            }
        }
        if let Ok(mut logs) = self.range_logs.lock() {
            if keep_through == 0 {
                logs.remove(group_id);
            } else {
                logs.insert(group_id.to_string(), keep_through);
            }
        }
        Ok(())
    }

    fn apply_range_committed(&self, group_id: &str, commit_index: u64) -> Result<()> {
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
                RangeEntryKind::Resolve { transaction_id, .. } => {
                    self.range_prepared
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("range prepared")))?
                        .remove(&(group_id.to_string(), transaction_id));
                }
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
    ) -> Option<u64> {
        let last = self.range_last_index(group_id);
        if prev_index > last {
            return Some(last + 1);
        }
        if prev_index > 0
            && self.range_entry(group_id, prev_index).map(|entry| entry.term) != Some(prev_term)
        {
            return Some(prev_index);
        }
        if index > 0 && index <= last {
            if let Some(existing) = self.range_entry(group_id, index) {
                if existing.term != term
                    || existing.commit_ts != commit_ts
                    || existing.writes != encoded_writes
                    || existing.publish_data != publish_data
                    || existing.kind != *kind
                {
                    return Some(index);
                }
            }
        }
        if index > last + 1 {
            Some(last + 1)
        } else {
            None
        }
    }

    #[cfg(test)]
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
        let mut state = self.ensure_range_group(group_id, leader);
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
        if let Ok(mut groups) = self.range_groups.lock() {
            if let Some(current) = groups.get_mut(group_id) {
                current.last_contact_ms = now_ms();
            }
        }
        if let Ok(mut leaders) = self.range_leaders.write() {
            leaders.insert(group_id.to_string(), leader);
        }
        let mut last = self.range_last_index(group_id);
        if prev_index > last {
            return Err(RymeError::Unavailable(String::from("range log gap")));
        }
        if prev_index != 0 {
            let previous = self
                .range_entry(group_id, prev_index)
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
                    .range_entry(group_id, index)
                    .ok_or_else(|| RymeError::Unavailable(String::from("range log gap")))?;
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
            if index != last + 1 {
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

    fn range_owner_by_id(&self, group_id: &str) -> Option<RangeOwner> {
        self.range_owners()
            .into_iter()
            .find(|range| range.id == group_id)
            .map(|range| self.with_current_range_leader(range))
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
            .range_owner_by_id(group_id)
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
        let index = self.range_last_index(group_id) + 1;
        let term = self.ensure_range_group(group_id, self.id).term.max(1);
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
            let _ = self.abort_range_entry(group_id, index, &replicas).await;
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
        self.persist_range_meta()?;
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
        let last_term = if keep_through == 0 { 0 } else { self.range_last_term(group_id) };
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
                    self.range_term(group_id),
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
            .range_owner_by_id(group_id)
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
            .range_owner_by_id(group_id)
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
            let _ = self.abort_range_entry(group_id, index, &range.replicas).await;
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
        if self.range_owners().is_empty() {
            return Ok(());
        }
        let mut groups: BTreeMap<String, (usize, BTreeMap<RecordKey, ryme_txn::WriteOp>)> =
            BTreeMap::new();
        for (key, op) in writes {
            let Some(range) = self.range_for_record(key) else { continue };
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

    fn transaction_participant_ranges(
        &self,
        txn: &ryme_txn::Transaction,
    ) -> BTreeMap<String, RangeOwner> {
        let ranges = self.range_owners();
        if ranges.is_empty() {
            return BTreeMap::new();
        }
        let mut participants = BTreeMap::new();
        for key in txn.writes().keys().chain(txn.read_keys().iter()) {
            if let Some(range) = self.range_for_record(key) {
                participants.insert(range.id.clone(), range);
            }
        }
        for (tenant, database, table) in txn.scanned_tables() {
            let mut prefix = Vec::with_capacity(table.len() + 1);
            prefix.extend_from_slice(table.as_bytes());
            prefix.push(0);
            let end = next_prefix(&prefix);
            for range in &ranges {
                let starts_before_end = end.is_empty() || range.start.as_slice() < end.as_slice();
                let ends_after_start =
                    range.end.is_empty() || range.end.as_slice() > prefix.as_slice();
                if starts_before_end && ends_after_start {
                    let _ = (tenant, database);
                    let range = self.with_current_range_leader(range.clone());
                    participants.insert(range.id.clone(), range);
                }
            }
        }
        participants
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
        if owner == self.id {
            return self.prepare_range_group(group_id, transaction_id, commit_ts, state).await;
        }
        let pool = self
            .pool_for(owner)
            .ok_or_else(|| RymeError::Unavailable(String::from("range participant unavailable")))?;
        let payload = serde_json::to_vec(&state)
            .map_err(|error| RymeError::Internal(format!("transaction prepare: {error}")))?;
        match pool
            .roundtrip(&Rpc::RangePrepareTransaction {
                group_id: group_id.to_string(),
                transaction_id,
                commit_ts,
                payload,
            })
            .await?
        {
            Rpc::RangePrepareTransactionResponse { prepared: true, error: None } => Ok(()),
            Rpc::RangePrepareTransactionResponse { error: Some(error), .. } => Err(error.into()),
            _ => Err(RymeError::Corrupt(String::from("transaction prepare rpc"))),
        }
    }

    async fn resolve_on(
        self: &Arc<Self>,
        group_id: &str,
        owner: usize,
        transaction_id: u128,
        commit_ts: u64,
        committed: bool,
    ) -> Result<()> {
        if owner == self.id {
            if group_id.is_empty() {
                return self.resolve_intent(transaction_id, commit_ts, committed);
            }
            return self.resolve_range_group(group_id, transaction_id, commit_ts, committed).await;
        }
        let pool = self
            .pool_for(owner)
            .ok_or_else(|| RymeError::Unavailable(String::from("range participant unavailable")))?;
        let response = if group_id.is_empty() {
            pool.roundtrip(&Rpc::ResolveTransaction { transaction_id, commit_ts, committed })
                .await?
        } else {
            pool.roundtrip(&Rpc::RangeResolveTransaction {
                group_id: group_id.to_string(),
                transaction_id,
                commit_ts,
                committed,
            })
            .await?
        };
        match response {
            Rpc::ResolveTransactionResponse { resolved: true, error: None }
            | Rpc::RangeResolveTransactionResponse { resolved: true, error: None } => Ok(()),
            Rpc::ResolveTransactionResponse { error: Some(error), .. }
            | Rpc::RangeResolveTransactionResponse { error: Some(error), .. } => Err(error.into()),
            _ => Err(RymeError::Corrupt(String::from("transaction resolve rpc"))),
        }
    }

    async fn retry_pending_resolutions(self: &Arc<Self>) {
        for resolution in self.pending_resolutions_snapshot() {
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

    fn queue_range_resolutions(
        &self,
        transaction_id: u128,
        commit_ts: u64,
        participants: &BTreeMap<String, RangeOwner>,
        committed: bool,
    ) -> Result<()> {
        for (group_id, range) in participants {
            self.queue_resolution_for_group(
                group_id,
                transaction_id,
                commit_ts,
                range.owner,
                committed,
            )?;
        }
        Ok(())
    }

    async fn replicate_range_resolutions(
        self: &Arc<Self>,
        transaction_id: u128,
        commit_ts: u64,
        participants: &BTreeMap<String, RangeOwner>,
        committed: bool,
    ) -> Result<()> {
        self.propose_resolutions(transaction_id, commit_ts, participants, committed)
            .await
            .map(|_| ())
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

    async fn commit_txn_at(
        self: &Arc<Self>,
        txn: ryme_txn::Transaction,
        commit_ts: u64,
    ) -> Result<u64> {
        let writes = txn.writes().clone();
        let result = self
            .manager
            .commit_at_with(txn, commit_ts, |commit_ts, encoded| async move {
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
        let participants = self.transaction_participant_ranges(&txn);
        if participants.len() < 2 {
            return self.commit_global_txn(txn).await;
        }
        let transaction_id = self.intent_id(&txn);
        let commit_ts = self.manager.reserve(&txn)?;
        let state = txn.state();
        let participant_ids = participants.clone();
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
                    if let Err(resolution_error) = self
                        .replicate_range_resolutions(
                            transaction_id,
                            commit_ts,
                            &participant_ids,
                            false,
                        )
                        .await
                    {
                        tracing::error!(
                            transaction_id,
                            commit_ts,
                            %resolution_error,
                            "transaction abort resolution replication failed"
                        );
                    }
                    self.retry_pending_resolutions().await;
                    return Err(error);
                }
            }
        }
        match self.commit_txn_at(txn, commit_ts).await {
            Ok(commit_ts) => {
                if let Err(error) =
                    self.queue_range_resolutions(transaction_id, commit_ts, &participant_ids, true)
                {
                    tracing::error!(transaction_id, commit_ts, %error, "transaction resolution queue failed");
                }
                if let Err(error) = self
                    .replicate_range_resolutions(transaction_id, commit_ts, &participant_ids, true)
                    .await
                {
                    tracing::error!(transaction_id, commit_ts, %error, "transaction resolution replication failed");
                }
                self.retry_pending_resolutions().await;
                Ok(commit_ts)
            }
            Err(error) => {
                if let Err(queue_error) =
                    self.queue_range_resolutions(transaction_id, commit_ts, &participant_ids, false)
                {
                    tracing::error!(transaction_id, commit_ts, %queue_error, "transaction abort resolution queue failed");
                }
                if let Err(resolution_error) = self
                    .replicate_range_resolutions(transaction_id, commit_ts, &participant_ids, false)
                    .await
                {
                    tracing::error!(
                        transaction_id,
                        commit_ts,
                        %resolution_error,
                        "transaction abort resolution replication failed"
                    );
                }
                self.retry_pending_resolutions().await;
                Err(error)
            }
        }
    }

    pub async fn commit_txn(self: &Arc<Self>, txn: ryme_txn::Transaction) -> Result<u64> {
        if let Some(expected) = txn.route_generation() {
            let current = self.range_generation();
            if current != expected {
                return Err(RymeError::Conflict(String::from("range topology changed")));
            }
        }
        let _commit = self.txn_commit_lock.lock().await;
        self.reject_prepared_conflict(&txn)?;
        if self.transaction_participant_ranges(&txn).len() > 1 {
            return self.commit_distributed_txn(txn).await;
        }
        self.commit_global_txn(txn).await
    }

    async fn replicate_frame(self: &Arc<Self>, commit_ts: u64, encoded: Vec<u8>) -> Result<()> {
        let payload = encode_applied(commit_ts, &encoded);
        self.propose_frame(payload).await.map(|_| ())
    }

    async fn propose_frame(self: &Arc<Self>, payload: Vec<u8>) -> Result<u64> {
        let _write = self.write_lock.lock().await;
        {
            let inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return Err(RymeError::Unavailable(String::from("not leader")));
            }
        }
        let index = {
            let mut inner = self.inner.lock().await;
            let index = inner.last_index() + 1;
            let entry = LogEntry { index, term: inner.term, payload };
            inner.persist_entries(std::slice::from_ref(&entry))?;
            inner.log.push(entry);
            inner.acks.insert(self.id, index);
            index
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            self.replicate_once().await;
            let (role, committed, last) = {
                let mut inner = self.inner.lock().await;
                inner.advance_commit();
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
            let entries: Vec<LogEntry> =
                snapshot.iter().filter(|e| e.index >= next).cloned().collect();
            let last = snapshot.last().map(|e| e.index).unwrap_or(0);
            if next > last {
                return Ok(());
            }
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
                            let _ = inner.persist_meta();
                            return Err(RymeError::Unavailable(String::from("stepped down")));
                        }
                        if inner.term != term || inner.role != Role::Leader {
                            return Err(RymeError::Unavailable(String::from("not leader")));
                        }
                    }
                    if ok && match_index >= last {
                        return Ok(());
                    }
                    next = match_index.saturating_add(1).max(1);
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
        self.snapshot_range_for(start, end, read_ts, max_rows, None, None)
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
            for (pk, table_versions) in self.manager.export_table(&tenant, &database, &table)? {
                let mut routing = Vec::with_capacity(table.len() + pk.len() + 1);
                routing.extend_from_slice(table.as_bytes());
                routing.push(0);
                routing.extend_from_slice(&pk);
                if routing.as_slice() < start || (!end.is_empty() && routing.as_slice() >= end) {
                    continue;
                }
                let versions: Vec<RangeSnapshotVersion> = table_versions
                    .into_iter()
                    .filter(|version| version.commit_ts <= snapshot_ts)
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
        let table_start = {
            let mut prefix = table.as_bytes().to_vec();
            prefix.push(0);
            prefix
        };
        let table_end = next_prefix(&table_start);
        let ranges = self.range_owners();
        let mut spans = Vec::new();
        for range in ranges {
            let range = self.with_current_range_leader(range);
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
                self.snapshot_range_for(
                    &lower,
                    &upper,
                    read_ts,
                    max_rows,
                    Some(tenant),
                    Some(database),
                )?
            } else {
                self.fetch_range_snapshot_for(
                    owner,
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
        let pool = self.pool_for(peer).ok_or_else(|| RymeError::NotFound(String::from("peer")))?;
        let max_rows = u32::try_from(max_rows.min(100_000))
            .map_err(|_| RymeError::InvalidArgument(String::from("max_rows")))?;
        match pool
            .roundtrip(&Rpc::RangeSnapshotRequest {
                start,
                end,
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
        let pool = self.pool_for(peer).ok_or_else(|| RymeError::NotFound(String::from("peer")))?;
        let max_rows = u32::try_from(max_rows.min(100_000))
            .map_err(|_| RymeError::InvalidArgument(String::from("max_rows")))?;
        match pool
            .roundtrip(&Rpc::RangeSnapshotRequest {
                start,
                end,
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
        if self.manager.latest_commit() > snapshot.snapshot_ts {
            return Err(RymeError::Unavailable(String::from("target ahead")));
        }
        let existing =
            self.snapshot_range(&snapshot.start, &snapshot.end, snapshot.snapshot_ts, 100_000)?;
        if existing.truncated {
            return Err(RymeError::Overload(String::from("target range")));
        }
        let wanted: std::collections::HashSet<(String, String, String, Vec<u8>)> = snapshot
            .rows
            .iter()
            .map(|row| {
                (row.tenant.clone(), row.database.clone(), row.table.clone(), row.pk.clone())
            })
            .collect();
        let purge: Vec<RecordKey> = existing
            .rows
            .iter()
            .filter(|row| {
                !wanted.contains(&(
                    row.tenant.clone(),
                    row.database.clone(),
                    row.table.clone(),
                    row.pk.clone(),
                ))
            })
            .map(|row| RecordKey::new(&row.tenant, &row.database, &row.table, &row.pk))
            .collect();
        if !purge.is_empty() {
            self.manager.purge_keys(&purge)?;
        }
        let mut tables: BTreeMap<(String, String, String), Vec<(Vec<u8>, Vec<TableVersion>)>> =
            BTreeMap::new();
        for row in &snapshot.rows {
            tables
                .entry((row.tenant.clone(), row.database.clone(), row.table.clone()))
                .or_default()
                .push((
                    row.pk.clone(),
                    row.versions
                        .iter()
                        .map(|version| TableVersion {
                            commit_ts: version.commit_ts,
                            value: version.value.clone(),
                            expires_at: version.expires_at,
                        })
                        .collect(),
                ));
        }
        for ((tenant, database, table), rows) in tables {
            self.manager.import_table(&tenant, &database, &table, rows)?;
        }
        self.manager.advance_to(snapshot.snapshot_ts.saturating_sub(1));
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
            .range_owner_by_id(group_id)
            .ok_or_else(|| RymeError::NotFound(String::from("range group")))?;
        if range.epoch != expected_epoch || range.owner == self.id {
            return Err(RymeError::Unavailable(String::from("range ownership update pending")));
        }
        let current = self.snapshot_range(
            &snapshot.start,
            &snapshot.end,
            self.manager.latest_commit(),
            100_000,
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

    pub fn set_range_owners(&self, mut owners: Vec<RangeOwner>) {
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
            self.ensure_range_group(&range.id, range.owner);
        }
        let _ = self.persist_range_meta();
        owners.sort_by(|left, right| left.start.cmp(&right.start));
        if let Ok(mut current) = self.range_owners.write() {
            if *current == owners {
                return;
            }
            *current = owners;
            self.range_generation.fetch_add(1, Ordering::SeqCst);
        }
        if let Ok(owners) = self.range_owners.read() {
            if let Ok(mut leaders) = self.range_leaders.write() {
                leaders.clear();
                for range in owners.iter() {
                    leaders.insert(range.id.clone(), range.owner);
                }
            }
        }
    }

    pub fn range_generation(&self) -> u64 {
        self.range_generation.load(Ordering::SeqCst)
    }

    pub fn range_owners(&self) -> Vec<RangeOwner> {
        self.range_owners.read().map(|owners| owners.clone()).unwrap_or_default()
    }

    pub fn range_owner(&self, routing_key: &[u8]) -> Option<(usize, u64)> {
        let owners = self.range_owners.read().ok()?;
        owners
            .iter()
            .find(|range| {
                routing_key >= range.start.as_slice()
                    && (range.end.is_empty() || routing_key < range.end.as_slice())
            })
            .map(|range| {
                let owner = self
                    .range_leaders
                    .read()
                    .ok()
                    .and_then(|leaders| leaders.get(&range.id).copied())
                    .unwrap_or(range.owner);
                (owner, range.epoch)
            })
    }

    pub fn range_owner_key(&self, key: &RecordKey) -> Option<usize> {
        let mut routing = Vec::with_capacity(key.table.len() + key.pk.len() + 1);
        routing.extend_from_slice(key.table.as_bytes());
        routing.push(0);
        routing.extend_from_slice(&key.pk);
        self.range_owner(&routing).map(|(owner, _)| owner)
    }

    pub fn owns_range_key(&self, key: &RecordKey) -> bool {
        self.range_owner_key(key).map(|owner| owner == self.id).unwrap_or(true)
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
        let expires_at = value.as_ref().and_then(|_| self.manager.expires_at(key).ok().flatten());
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

    pub fn read_range_values(
        &self,
        keys: &[RecordKey],
        read_ts: u64,
    ) -> Result<Vec<(Option<Vec<u8>>, Option<u64>)>> {
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
    ) -> Result<Vec<(Option<Vec<u8>>, Option<u64>)>> {
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
        {
            return true;
        }
    }
    for key in right.writes().keys() {
        if left.read_keys().contains(key)
            || (left.isolation() == ryme_txn::Isolation::Serializable
                && key_matches_scan(key, left.scanned_tables()))
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
            let Some(owner) = node.range_owner_key(key) else {
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
                && txn.scanned_tables().is_empty();
            let read_ts = if establish_snapshot { 0 } else { txn.read_ts };
            let (value, _, observed_read_ts) =
                node.fetch_range_value_with_ts(owner, key.clone(), read_ts).await?;
            if establish_snapshot && observed_read_ts != 0 {
                txn.restamp(observed_read_ts);
            }
            txn.record_read(key.clone(), value.is_some());
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
            txn.record_scan(tenant, database, table);
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
            txn.record_scan(tenant, database, table);
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

    fn commit(
        &self,
        txn: ryme_txn::Transaction,
    ) -> impl std::future::Future<Output = Result<u64>> + Send {
        let node = self.node.clone();
        let route_generation = txn.route_generation();
        let distributed = node.transaction_participant_ranges(&txn).len() > 1;
        let routed = if !distributed
            && txn.read_keys().is_empty()
            && txn.scanned_tables().is_empty()
            && !txn.writes().is_empty()
        {
            let owners = node.range_owners();
            if owners.is_empty() {
                None
            } else {
                let owner = txn
                    .writes()
                    .keys()
                    .map(|key| node.range_owner_key(key))
                    .try_fold(None, |current, next| match (current, next) {
                        (None, Some(owner)) => Ok(Some(owner)),
                        (Some(existing), Some(owner)) if existing == owner => Ok(current),
                        _ => Err(()),
                    })
                    .ok()
                    .flatten()
                    .map_or_else(
                        || Some((None, txn.writes().clone())),
                        |owner| Some((Some(owner), txn.writes().clone())),
                    );
                owner
            }
        } else {
            None
        };
        async move {
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

fn recent_leader(contact_ms: u64) -> bool {
    contact_ms != 0 && now_ms().saturating_sub(contact_ms) < 600
}

fn append_to_log(
    inner: &mut tokio::sync::MutexGuard<'_, Inner>,
    prev_index: u64,
    prev_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
) -> Result<u64> {
    if prev_index > 0 {
        let Some(existing) = inner.log.iter().find(|e| e.index == prev_index) else {
            return Err(RymeError::Unavailable(String::from("missing prefix")));
        };
        if existing.term != prev_term {
            inner.log.retain(|e| e.index < prev_index);
            return Err(RymeError::Unavailable(String::from("conflict")));
        }
        inner.log.retain(|e| e.index <= prev_index);
    }
    let mut fresh: Vec<LogEntry> = Vec::new();
    for entry in entries {
        if inner.log.iter().any(|e| e.index == entry.index) {
            continue;
        }
        fresh.push(entry);
    }
    if !fresh.is_empty() {
        inner.persist_entries(&fresh)?;
        inner.log.extend(fresh);
        inner.log.sort_by_key(|e| e.index);
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

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ryme-raft-{label}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ))
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
}
