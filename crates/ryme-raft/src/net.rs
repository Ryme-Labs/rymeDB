use crate::{decode_apply, encode_applied, ApplyPayload, LogEntry, Role};
use ryme_error::{Result, RymeError};
use ryme_storage::{RecordKey, TableVersion};
use ryme_txn::{decode_writes, now_unix, TxnManager};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use std::path::PathBuf;
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
    RangeReadRequest {
        key: RecordKey,
        read_ts: u64,
    },
    RangeReadResponse {
        value: Option<Vec<u8>>,
        expires_at: Option<u64>,
    },
    RangeReadBatchRequest {
        keys: Vec<RecordKey>,
        read_ts: u64,
    },
    RangeReadBatchResponse {
        values: Vec<(Option<Vec<u8>>, Option<u64>)>,
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
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub owner: usize,
    pub epoch: u64,
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
        let mut replayed: Option<crate::ConfChange> = None;
        let mut replayed_joint: Option<Vec<Member>> = None;
        let mut metadata = None;
        for entry in log.iter().filter(|e| e.index <= meta.commit_index) {
            match decode_apply(&entry.payload)? {
                ApplyPayload::Data { commit_ts, writes } => {
                    let writes = decode_writes(&writes)?;
                    manager.replay_at(commit_ts, &writes)?;
                }
                ApplyPayload::Conf { change } => {
                    replayed = Some(change);
                }
                ApplyPayload::Metadata { payload } => {
                    metadata = Some(payload);
                }
                ApplyPayload::Topic { .. } => {}
            }
            applied = entry.index;
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
        });
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

    async fn main_loop(self: Arc<Self>) {
        loop {
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
                    self.manager.replay_at(commit_ts, &writes)?;
                    if self.inner.lock().await.role != Role::Leader {
                        self.apply_data_hook(crate::encode_applied(commit_ts, &encoded_writes))?;
                    }
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

    async fn serve_connection<S>(&self, mut socket: S) -> Result<()>
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

    async fn handle_rpc(&self, rpc: Rpc) -> Result<Rpc> {
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
            Rpc::RangeSnapshotRequest { start, end, read_ts, max_rows } => {
                let snapshot = self.snapshot_range(&start, &end, read_ts, max_rows as usize)?;
                Ok(Rpc::RangeSnapshotResponse { snapshot })
            }
            Rpc::RangeInstallRequest { snapshot } => {
                let rows = self.install_range_snapshot(&snapshot)?;
                Ok(Rpc::RangeInstallResponse { rows })
            }
            Rpc::RangeReadRequest { key, read_ts } => {
                let (value, expires_at) = self.read_range_value(&key, read_ts)?;
                Ok(Rpc::RangeReadResponse { value, expires_at })
            }
            Rpc::RangeReadBatchRequest { keys, read_ts } => {
                let values = self.read_range_values(&keys, read_ts)?;
                Ok(Rpc::RangeReadBatchResponse { values })
            }
            Rpc::AppendRequest {
                term,
                leader: _,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => {
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
            | Rpc::RangeReadResponse { .. }
            | Rpc::RangeReadBatchResponse { .. } => {
                Err(RymeError::InvalidArgument(String::from("rpc direction")))
            }
        }
    }

    pub async fn propose_write(
        self: &Arc<Self>,
        writes: BTreeMap<RecordKey, ryme_txn::WriteOp>,
    ) -> Result<u64> {
        let mut txn = self.manager.begin();
        for (key, op) in writes {
            match op.value {
                Some(bytes) => self.manager.put_with_ttl(&mut txn, key, bytes, op.expires_at),
                None => self.manager.delete(&mut txn, key),
            }
        }
        self.commit_txn(txn).await
    }

    pub async fn propose_metadata(self: &Arc<Self>, payload: Vec<u8>) -> Result<u64> {
        let encoded = crate::encode_metadata(&payload)?;
        let index = self.propose_frame(encoded).await?;
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

    pub async fn commit_txn(self: &Arc<Self>, txn: ryme_txn::Transaction) -> Result<u64> {
        self.manager
            .commit_with(txn, |commit_ts, encoded| async move {
                self.replicate_frame(commit_ts, encoded).await.map(|_| ())
            })
            .await
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
        if start >= end && !end.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("range")));
        }
        let max_rows = max_rows.clamp(1, 100_000);
        let snapshot_ts = if read_ts == 0 { self.manager.latest_commit() } else { read_ts };
        let mut rows = Vec::new();
        let mut truncated = false;
        for (tenant, database, table) in self.manager.spaces()? {
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
        match pool.roundtrip(&Rpc::RangeSnapshotRequest { start, end, read_ts, max_rows }).await? {
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

    pub fn set_range_owners(&self, mut owners: Vec<RangeOwner>) {
        owners.sort_by(|left, right| left.start.cmp(&right.start));
        if let Ok(mut current) = self.range_owners.write() {
            *current = owners;
        }
    }

    pub fn range_owner(&self, routing_key: &[u8]) -> Option<(usize, u64)> {
        let owners = self.range_owners.read().ok()?;
        owners
            .iter()
            .find(|range| {
                routing_key >= range.start.as_slice()
                    && (range.end.is_empty() || routing_key < range.end.as_slice())
            })
            .map(|range| (range.owner, range.epoch))
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
        let pool = self.pool_for(peer).ok_or_else(|| RymeError::NotFound(String::from("peer")))?;
        match pool.roundtrip(&Rpc::RangeReadRequest { key, read_ts }).await? {
            Rpc::RangeReadResponse { value, expires_at } => Ok((value, expires_at)),
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
        self.node.manager().begin()
    }

    fn get(&self, txn: &mut ryme_txn::Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        self.node.manager().get(txn, key)
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
        async move { node.commit_txn(txn).await }
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
}
