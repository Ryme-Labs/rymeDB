use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::broadcast;

type EncodedFrameSlot = tokio::sync::OnceCell<(String, Arc<str>)>;
type EncodedFrameCache = Arc<[EncodedFrameSlot; 2]>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub tenant: String,
    pub database: String,
    pub branch: String,
    pub table: String,
    pub op: Operation,
    pub pk: Vec<u8>,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub commit_ts: u64,
    pub tx_id: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRow {
    pub pk: Vec<u8>,
    pub value: Vec<u8>,
}

pub const PRESENCE_MAX_MEMBERS: usize = 1000;
pub const PRESENCE_MAX_TTL_SECS: u64 = 86400;
pub const PRESENCE_MAX_MEMBER_BYTES: usize = 256;
pub const PRESENCE_MAX_STATE_BYTES: usize = 64 * 1024;
pub const PRESENCE_MAX_MEMBERS_TOTAL: usize = 100_000;
pub const PRESENCE_MAX_STATE_BYTES_TOTAL: usize = 256 * 1024 * 1024;
const BROADCAST_SHARDS: usize = 32;
const TABLE_TOPIC_SHARDS: usize = 32;
const PRESENCE_SHARDS: usize = 32;
const STABLE_CDC_SEQUENCE_STRIDE: u64 = 1_000_000;
const BROADCAST_BUFFER_CAPACITY: usize = 16_384;
const MAX_BROADCAST_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_CHANGE_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_TABLE_HISTORY_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_CHANNEL_BYTES: usize = 256;
pub const DURABLE_PARTITION_MAX_BYTES: usize = 256;
pub const DURABLE_KEY_MAX_BYTES: usize = 1024;
pub const DURABLE_VALUE_MAX_BYTES: usize = 4 * 1024 * 1024;
pub const DURABLE_READ_MAX_MESSAGES: usize = 1000;
const MAX_DURABLE_TOPICS: usize = 100_000;
const MAX_DURABLE_TOPICS_PER_TENANT: usize = 10_000;
const MAX_TABLE_TOPICS: usize = 100_000;
const MAX_TABLE_TOPICS_PER_TENANT: usize = 10_000;
const MAX_BROADCAST_TOPICS: usize = 100_000;
const MAX_BROADCAST_TOPICS_PER_TENANT: usize = 10_000;
const MAX_PRESENCE_TOPICS: usize = 100_000;
const MAX_PRESENCE_TOPICS_PER_TENANT: usize = 10_000;
const MAX_PRESENCE_ROOMS: usize = 100_000;
const MAX_PRESENCE_ROOMS_PER_TENANT: usize = 10_000;

type BroadcastTopicShard = Mutex<HashMap<String, Arc<BroadcastTopic>>>;
type PresenceTopicShard = Mutex<HashMap<String, broadcast::Sender<PresenceEvent>>>;
type PresenceStateShard = Mutex<HashMap<String, HashMap<String, PresenceMember>>>;
type DurableTopicShard = Mutex<HashMap<String, broadcast::Sender<DurableMsg>>>;

#[derive(Debug, Default)]
struct TopicCounts {
    total: usize,
    per_tenant: HashMap<String, usize>,
}

#[derive(Debug, Default)]
struct TopicRegistry {
    table: TopicCounts,
    broadcast: TopicCounts,
    presence_topics: TopicCounts,
    presence_rooms: TopicCounts,
}

#[derive(Debug, Clone, Copy)]
enum TopicKind {
    Table,
    Broadcast,
    PresenceTopic,
    PresenceRoom,
}

impl TopicRegistry {
    fn admit(&mut self, kind: TopicKind, tenant: &str) -> Result<()> {
        let (counts, global_limit, tenant_limit) = match kind {
            TopicKind::Table => (&mut self.table, MAX_TABLE_TOPICS, MAX_TABLE_TOPICS_PER_TENANT),
            TopicKind::Broadcast => {
                (&mut self.broadcast, MAX_BROADCAST_TOPICS, MAX_BROADCAST_TOPICS_PER_TENANT)
            }
            TopicKind::PresenceTopic => {
                (&mut self.presence_topics, MAX_PRESENCE_TOPICS, MAX_PRESENCE_TOPICS_PER_TENANT)
            }
            TopicKind::PresenceRoom => {
                (&mut self.presence_rooms, MAX_PRESENCE_ROOMS, MAX_PRESENCE_ROOMS_PER_TENANT)
            }
        };
        let tenant_count = counts.per_tenant.get(tenant).copied().unwrap_or(0);
        if counts.total >= global_limit || tenant_count >= tenant_limit {
            return Err(RymeError::Overload(String::from("realtime topic limit")));
        }
        counts.total += 1;
        *counts.per_tenant.entry(tenant.to_string()).or_default() += 1;
        Ok(())
    }

    fn release(&mut self, kind: TopicKind, tenant: &str) {
        let counts = match kind {
            TopicKind::Table => &mut self.table,
            TopicKind::Broadcast => &mut self.broadcast,
            TopicKind::PresenceTopic => &mut self.presence_topics,
            TopicKind::PresenceRoom => &mut self.presence_rooms,
        };
        counts.total = counts.total.saturating_sub(1);
        if let Some(count) = counts.per_tenant.get_mut(tenant) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.per_tenant.remove(tenant);
            }
        }
    }
}

#[derive(Debug)]
struct BroadcastTopic {
    sender: broadcast::Sender<Arc<BroadcastMsg>>,
    publish_lock: Mutex<()>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryUpdate {
    pub tenant: String,
    pub database: String,
    pub branch: String,
    pub table: String,
    pub commit_ts: u64,
    pub sequence: u64,
    pub rows: Vec<QueryRow>,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub struct Realtime {
    inner: Arc<Mutex<RealtimeInner>>,
    topic_registry: Arc<Mutex<TopicRegistry>>,
    changes: broadcast::Sender<ChangeRecord>,
    table_topics: Arc<Vec<Mutex<TableTopicShard>>>,
    broadcast_topics: Arc<Vec<BroadcastTopicShard>>,
    presence_topics: Arc<Vec<PresenceTopicShard>>,
    presence_state: Arc<Vec<PresenceStateShard>>,
    durable_topics: Arc<Vec<DurableTopicShard>>,
    broadcast_capacity: usize,
    capacity: usize,
    sequence: Arc<AtomicU64>,
    presence_sequence: Arc<AtomicU64>,
    presence_member_count: Arc<AtomicUsize>,
    presence_state_bytes: Arc<AtomicUsize>,
    history_bytes: Arc<AtomicUsize>,
    stable_cdc: bool,
    cdc_sequences: Arc<Mutex<HashMap<String, (u64, u64)>>>,
}

#[derive(Debug)]
struct QueryTopic {
    sender: broadcast::Sender<QueryUpdate>,
    max_limit: usize,
    latest_commit: u64,
}

#[derive(Debug)]
struct RealtimeInner {
    durable: HashMap<String, DurableTopic>,
    durable_topics_by_tenant: HashMap<String, usize>,
}

#[derive(Debug, Default)]
struct TableTopicShard {
    topics: HashMap<String, broadcast::Sender<ChangeRecord>>,
    history: HashMap<String, std::collections::VecDeque<ChangeRecord>>,
    queries: HashMap<String, QueryTopic>,
    tenants: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceMember {
    pub member: String,
    pub state: serde_json::Value,
    pub expires_unix: u64,
}

fn encoded_presence_state_bytes(state: &serde_json::Value) -> usize {
    serde_json::to_vec(state).map(|bytes| bytes.len()).unwrap_or(usize::MAX)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceEvent {
    #[serde(rename = "type")]
    pub kind: String,
    pub channel: String,
    pub member: String,
    pub state: serde_json::Value,
    pub expires_unix: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMsg {
    pub channel: String,
    pub from: String,
    pub payload: serde_json::Value,
    pub commit_ts: u64,
    pub sequence: u64,
    #[serde(skip)]
    json_cache: Arc<OnceLock<String>>,
    #[serde(skip)]
    encoded_frames: EncodedFrameCache,
    #[serde(skip)]
    fanout_quota_pre_admitted: bool,
}

impl BroadcastMsg {
    /// Serialize the stable broadcast envelope once and reuse it for every
    /// websocket subscriber. Each websocket still receives its own owned
    /// frame, but subscriber fanout no longer repeats JSON traversal and
    /// encoding for the same payload.
    pub fn json_text(&self) -> String {
        self.json_cache
            .get_or_init(|| serde_json::to_string(self).unwrap_or_else(|_| String::from("{}")))
            .clone()
    }

    pub fn json_len(&self) -> usize {
        self.json_cache
            .get_or_init(|| serde_json::to_string(self).unwrap_or_else(|_| String::from("{}")))
            .len()
    }

    fn checked_json_len(&self) -> Result<usize> {
        let bytes = self.json_len();
        if bytes > MAX_BROADCAST_MESSAGE_BYTES {
            Err(RymeError::Overload(String::from("broadcast payload")))
        } else {
            Ok(bytes)
        }
    }

    pub fn fanout_quota_pre_admitted(&self) -> bool {
        self.fanout_quota_pre_admitted
    }

    /// Reuse a protocol-specific frame for subscribers receiving the same
    /// broadcast. The cache is capped at two variants to keep per-message
    /// memory bounded (normally one topic in each of two wire formats).
    pub async fn cached_text_frame(
        &self,
        topic: &str,
        protocol: u8,
        encode: impl FnOnce() -> String,
    ) -> Arc<str> {
        let Some(cache) = self.encoded_frames.get(usize::from(protocol)) else {
            return Arc::from(encode());
        };
        if let Some((cached_topic, frame)) = cache.get() {
            return if cached_topic == topic { Arc::clone(frame) } else { Arc::from(encode()) };
        }

        let mut encode = Some(encode);
        let (cached_topic, frame) = cache
            .get_or_init(|| async {
                let frame: Arc<str> =
                    Arc::from(encode.take().expect("frame cache initializer runs once")());
                (topic.to_owned(), frame)
            })
            .await;
        if cached_topic == topic {
            Arc::clone(frame)
        } else {
            Arc::from(encode.take().expect("frame cache initialized for another topic")())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableMsg {
    pub partition: String,
    pub cursor: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub commit_ts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableTopicSnapshot {
    pub tenant: String,
    pub partition: String,
    pub messages: Vec<DurableMsg>,
    pub next_cursor: u64,
    pub retention: usize,
}

pub fn validate_durable_partition(partition: &str) -> Result<()> {
    if partition.is_empty()
        || partition.len() > DURABLE_PARTITION_MAX_BYTES
        || partition.contains('\0')
    {
        return Err(RymeError::InvalidArgument(String::from("partition")));
    }
    Ok(())
}

pub fn validate_channel(channel: &str) -> Result<()> {
    if channel.is_empty() || channel.len() > MAX_CHANNEL_BYTES {
        return Err(RymeError::InvalidArgument(String::from("channel")));
    }
    Ok(())
}

#[derive(Debug)]
struct DurableTopic {
    messages: VecDeque<DurableMsg>,
    next_cursor: u64,
    retention: usize,
}

#[derive(Debug, Clone)]
pub struct NewChange {
    pub tenant: String,
    pub database: String,
    pub branch: String,
    pub table: String,
    pub op: Operation,
    pub pk: Vec<u8>,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub commit_ts: u64,
    pub tx_id: u64,
}

fn change_record_bytes(record: &ChangeRecord) -> usize {
    change_record_bytes_parts(
        &record.tenant,
        &record.database,
        &record.branch,
        &record.table,
        &record.pk,
        record.before.as_deref(),
        record.after.as_deref(),
    )
}

fn change_record_bytes_parts(
    tenant: &str,
    database: &str,
    branch: &str,
    table: &str,
    pk: &[u8],
    before: Option<&[u8]>,
    after: Option<&[u8]>,
) -> usize {
    96usize
        .saturating_add(tenant.len())
        .saturating_add(database.len())
        .saturating_add(branch.len())
        .saturating_add(table.len())
        .saturating_add(pk.len())
        .saturating_add(before.map_or(0, |value| value.len()))
        .saturating_add(after.map_or(0, |value| value.len()))
}

impl Realtime {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.clamp(16, 100000);
        let (changes, _) = broadcast::channel(capacity);
        Self {
            inner: Arc::new(Mutex::new(RealtimeInner {
                durable: HashMap::new(),
                durable_topics_by_tenant: HashMap::new(),
            })),
            topic_registry: Arc::new(Mutex::new(TopicRegistry::default())),
            changes,
            table_topics: Arc::new(
                (0..TABLE_TOPIC_SHARDS).map(|_| Mutex::new(TableTopicShard::default())).collect(),
            ),
            broadcast_topics: Arc::new(
                (0..BROADCAST_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            ),
            presence_topics: Arc::new(
                (0..PRESENCE_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            ),
            presence_state: Arc::new(
                (0..PRESENCE_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            ),
            durable_topics: Arc::new(
                (0..BROADCAST_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            ),
            broadcast_capacity: capacity.max(BROADCAST_BUFFER_CAPACITY),
            capacity,
            sequence: Arc::new(AtomicU64::new(0)),
            presence_sequence: Arc::new(AtomicU64::new(0)),
            presence_member_count: Arc::new(AtomicUsize::new(0)),
            presence_state_bytes: Arc::new(AtomicUsize::new(0)),
            history_bytes: Arc::new(AtomicUsize::new(0)),
            stable_cdc: false,
            cdc_sequences: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Use commit-derived CDC cursors so every gateway that applies the same
    /// committed write set exposes the same resume watermark.
    pub fn with_stable_cdc(mut self) -> Self {
        self.stable_cdc = true;
        self
    }

    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn next_presence_sequence(&self) -> u64 {
        self.presence_sequence.fetch_add(1, Ordering::Relaxed) + 1
    }

    // `try_update` is newer than the workspace MSRV (1.89), so keep using the
    // compatible alias while allowing its deprecation on newer stable toolchains.
    #[allow(deprecated)]
    fn reserve_presence_capacity(&self, members: usize, state_bytes: usize) -> bool {
        let members_reserved = self.presence_member_count.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| {
                current.checked_add(members).filter(|next| *next <= PRESENCE_MAX_MEMBERS_TOTAL)
            },
        );
        if members_reserved.is_err() {
            return false;
        }
        let bytes_reserved = self.presence_state_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| {
                current
                    .checked_add(state_bytes)
                    .filter(|next| *next <= PRESENCE_MAX_STATE_BYTES_TOTAL)
            },
        );
        if bytes_reserved.is_err() {
            self.presence_member_count.fetch_sub(members, Ordering::AcqRel);
            return false;
        }
        true
    }

    fn release_presence_capacity(&self, members: usize, state_bytes: usize) {
        self.presence_member_count.fetch_sub(members, Ordering::AcqRel);
        self.presence_state_bytes.fetch_sub(state_bytes, Ordering::AcqRel);
    }

    pub fn reserve_sequence(&self) -> u64 {
        self.next_sequence()
    }

    fn broadcast_shard(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.broadcast_topics.len()
    }

    fn table_topic_shard(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.table_topics.len()
    }

    fn durable_topic_shard(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.durable_topics.len()
    }

    fn presence_shard(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.presence_topics.len()
    }

    fn admit_topic(&self, kind: TopicKind, tenant: &str) -> Result<()> {
        self.topic_registry
            .lock()
            .map_err(|_| RymeError::Internal(String::from("realtime topic registry lock")))?
            .admit(kind, tenant)
    }

    fn release_topic(&self, kind: TopicKind, tenant: &str) {
        if let Ok(mut registry) = self.topic_registry.lock() {
            registry.release(kind, tenant);
        }
    }

    fn ensure_table_topic(
        &self,
        topics: &mut TableTopicShard,
        key: &str,
        tenant: &str,
    ) -> Result<()> {
        if topics.topics.contains_key(key)
            || topics.history.contains_key(key)
            || topics.queries.contains_key(key)
        {
            return Ok(());
        }
        self.admit_topic(TopicKind::Table, tenant)?;
        topics.tenants.insert(key.to_owned(), tenant.to_owned());
        Ok(())
    }

    fn release_table_topic_if_empty(&self, topics: &mut TableTopicShard, key: &str) {
        if topics.topics.contains_key(key)
            || topics.history.contains_key(key)
            || topics.queries.contains_key(key)
        {
            return;
        }
        if let Some(tenant) = topics.tenants.remove(key) {
            self.release_topic(TopicKind::Table, &tenant);
            if let Ok(mut sequences) = self.cdc_sequences.lock() {
                sequences.remove(key);
            }
        }
    }

    pub fn publish(&self, event: NewChange) -> Result<u64> {
        let record_bytes = change_record_bytes_parts(
            &event.tenant,
            &event.database,
            &event.branch,
            &event.table,
            &event.pk,
            event.before.as_deref(),
            event.after.as_deref(),
        );
        if record_bytes > MAX_CHANGE_RECORD_BYTES {
            return Err(RymeError::Overload(String::from("realtime change")));
        }
        let key = branch_topic_key(&event.tenant, &event.database, &event.branch, &event.table);
        let shard = self.table_topic_shard(&key);
        let mut topics = self
            .table_topics
            .get(shard)
            .ok_or_else(|| RymeError::Internal(String::from("realtime topic shard")))?
            .lock()
            .map_err(|_| RymeError::Internal(String::from("realtime topic lock")))?;
        self.ensure_table_topic(&mut topics, &key, &event.tenant)?;
        let sequence = self.next_change_sequence(&key, event.commit_ts);
        let record = ChangeRecord {
            tenant: event.tenant,
            database: event.database,
            branch: event.branch,
            table: event.table,
            op: event.op,
            pk: event.pk,
            before: event.before,
            after: event.after,
            commit_ts: event.commit_ts,
            tx_id: event.tx_id,
            sequence,
        };
        if let Some(sender) = topics.topics.get(&key) {
            if sender.receiver_count() > 0 {
                let _ = sender.send(record.clone());
            }
        }
        if self.changes.receiver_count() > 0 {
            let _ = self.changes.send(record.clone());
        }
        self.retain_history(&mut topics, key, record, record_bytes);
        Ok(sequence)
    }

    // `try_update` is newer than the workspace MSRV (1.89), so keep using the
    // compatible alias while allowing its deprecation on newer stable toolchains.
    #[allow(deprecated)]
    fn retain_history(
        &self,
        topics: &mut TableTopicShard,
        key: String,
        record: ChangeRecord,
        record_bytes: usize,
    ) {
        let remove_empty = {
            let log = topics.history.entry(key.clone()).or_default();
            while log.len() >= self.capacity {
                if let Some(evicted) = log.pop_front() {
                    self.release_history_bytes(change_record_bytes(&evicted));
                }
            }
            while self.history_bytes.load(Ordering::Acquire).saturating_add(record_bytes)
                > MAX_TABLE_HISTORY_BYTES
            {
                let Some(evicted) = log.pop_front() else { break };
                self.release_history_bytes(change_record_bytes(&evicted));
            }
            let reserved =
                self.history_bytes.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(record_bytes)
                        .filter(|next| *next <= MAX_TABLE_HISTORY_BYTES)
                });
            if reserved.is_ok() {
                log.push_back(record);
                false
            } else {
                log.is_empty()
            }
        };
        if remove_empty {
            topics.history.remove(&key);
            self.release_table_topic_if_empty(topics, &key);
        }
    }

    // `try_update` is newer than the workspace MSRV (1.89), so keep using the
    // compatible alias while allowing its deprecation on newer stable toolchains.
    #[allow(deprecated)]
    fn release_history_bytes(&self, bytes: usize) {
        let _ = self.history_bytes.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(bytes))
        });
    }

    fn next_change_sequence(&self, key: &str, commit_ts: u64) -> u64 {
        if !self.stable_cdc {
            return self.reserve_sequence();
        }
        let Ok(mut sequences) = self.cdc_sequences.lock() else {
            return self.reserve_sequence();
        };
        let ordinal = match sequences.get(key).copied() {
            Some((last_commit, ordinal)) if last_commit == commit_ts => ordinal.saturating_add(1),
            _ => 0,
        };
        sequences.insert(key.to_string(), (commit_ts, ordinal));
        commit_ts
            .saturating_mul(STABLE_CDC_SEQUENCE_STRIDE)
            .saturating_add(ordinal.saturating_add(1))
    }

    pub fn has_subscribers(&self, tenant: &str, database: &str, table: &str) -> bool {
        self.has_subscribers_branch(tenant, database, "main", table)
    }

    pub fn has_subscribers_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> bool {
        if self.changes.receiver_count() > 0 {
            return true;
        }
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        self.table_topics
            .get(shard)
            .and_then(|topics| topics.lock().ok())
            .and_then(|topics| topics.topics.get(&key).map(|sender| sender.receiver_count() > 0))
            .unwrap_or(false)
    }

    pub fn subscribe(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
    ) -> broadcast::Receiver<ChangeRecord> {
        self.subscribe_branch(tenant, database, "main", table)
    }

    pub fn subscribe_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> broadcast::Receiver<ChangeRecord> {
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        let Some(topics) = self.table_topics.get(shard) else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        let Ok(mut topics) = topics.lock() else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        if self.ensure_table_topic(&mut topics, &key, tenant).is_err() {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        }
        topics.topics.entry(key).or_insert_with(|| broadcast::channel(self.capacity).0).subscribe()
    }

    /// Subscribe to every committed change. Consumers should filter by
    /// tenant, database, branch, table, and policy before exposing records.
    pub fn subscribe_all_changes(&self) -> broadcast::Receiver<ChangeRecord> {
        self.changes.subscribe()
    }

    pub fn replay(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        since_commit_ts: u64,
        limit: usize,
    ) -> Vec<ChangeRecord> {
        self.replay_branch(tenant, database, "main", table, since_commit_ts, limit)
    }

    pub fn replay_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
        since_commit_ts: u64,
        limit: usize,
    ) -> Vec<ChangeRecord> {
        let limit = limit.clamp(1, 100000);
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        let Some(topics) = self.table_topics.get(shard).and_then(|topics| topics.lock().ok())
        else {
            return Vec::new();
        };
        let Some(log) = topics.history.get(&key) else { return Vec::new() };
        log.iter()
            .filter(|record| record.commit_ts > since_commit_ts)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Replay retained changes after an exact per-topic sequence watermark.
    /// Unlike commit timestamps, sequences distinguish multiple writes in the
    /// same transaction and are therefore safe for reconnecting consumers.
    pub fn replay_after_sequence(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Vec<ChangeRecord> {
        self.replay_after_sequence_branch(tenant, database, "main", table, after_sequence, limit)
    }

    pub fn replay_after_sequence_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Vec<ChangeRecord> {
        let limit = limit.clamp(1, 100000);
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        let Some(topics) = self.table_topics.get(shard).and_then(|topics| topics.lock().ok())
        else {
            return Vec::new();
        };
        let Some(log) = topics.history.get(&key) else { return Vec::new() };
        log.iter().filter(|record| record.sequence > after_sequence).take(limit).cloned().collect()
    }

    /// Return the oldest and newest retained sequence for a table topic.
    /// Consumers can use this to distinguish an empty replay from a cursor
    /// that has fallen out of the bounded history ring.
    pub fn change_sequence_bounds(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> Option<(u64, u64)> {
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        self.table_topics.get(shard).and_then(|topics| topics.lock().ok()).and_then(|topics| {
            let history = topics.history.get(&key)?;
            Some((history.front()?.sequence, history.back()?.sequence))
        })
    }

    /// Return the oldest and newest retained commit timestamp for a table
    /// topic. This is useful for validating the legacy commit watermark.
    pub fn change_commit_bounds(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> Option<(u64, u64)> {
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        self.table_topics.get(shard).and_then(|topics| topics.lock().ok()).and_then(|topics| {
            let history = topics.history.get(&key)?;
            Some((history.front()?.commit_ts, history.back()?.commit_ts))
        })
    }

    pub fn latest_change_commit_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> u64 {
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        self.table_topics
            .get(shard)
            .and_then(|topics| topics.lock().ok())
            .and_then(|topics| {
                topics
                    .history
                    .get(&key)
                    .and_then(|history| history.back())
                    .map(|record| record.commit_ts)
            })
            .unwrap_or(0)
    }

    pub fn history_capacity(&self) -> usize {
        self.capacity
    }

    pub fn query_limit(&self, tenant: &str, database: &str, table: &str) -> Option<usize> {
        self.query_limit_branch(tenant, database, "main", table)
    }

    pub fn query_limit_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> Option<usize> {
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        self.table_topics.get(shard)?.lock().ok()?.queries.get(&key).map(|topic| topic.max_limit)
    }

    pub fn query_subscribe(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> broadcast::Receiver<QueryUpdate> {
        self.query_subscribe_branch(tenant, database, "main", table, limit)
    }

    pub fn query_subscribe_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
        limit: usize,
    ) -> broadcast::Receiver<QueryUpdate> {
        let key = branch_topic_key(tenant, database, branch, table);
        let limit = limit.clamp(1, 1000);
        let shard = self.table_topic_shard(&key);
        let Some(topic_shard) = self.table_topics.get(shard) else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        let Ok(mut topic_shard) = topic_shard.lock() else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        if self.ensure_table_topic(&mut topic_shard, &key, tenant).is_err() {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        }
        let topic = topic_shard.queries.entry(key).or_insert_with(|| QueryTopic {
            sender: broadcast::channel(self.capacity).0,
            max_limit: limit,
            latest_commit: 0,
        });
        topic.max_limit = topic.max_limit.max(limit);
        topic.sender.subscribe()
    }

    pub fn query_latest_commit(&self, tenant: &str, database: &str, table: &str) -> u64 {
        self.query_latest_commit_branch(tenant, database, "main", table)
    }

    pub fn query_latest_commit_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> u64 {
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        self.table_topics
            .get(shard)
            .and_then(|topics| topics.lock().ok())
            .and_then(|topics| topics.queries.get(&key).map(|topic| topic.latest_commit))
            .unwrap_or(0)
    }

    pub fn publish_query(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        commit_ts: u64,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        limit: usize,
    ) -> Option<u64> {
        self.publish_query_branch(tenant, database, "main", table, commit_ts, rows, limit)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_query_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
        commit_ts: u64,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        limit: usize,
    ) -> Option<u64> {
        let key = branch_topic_key(tenant, database, branch, table);
        let shard = self.table_topic_shard(&key);
        let mut topics = self.table_topics.get(shard)?.lock().ok()?;
        if !topics.queries.contains_key(&key) {
            return None;
        }
        let sequence = self.next_sequence();
        let topic = topics.queries.get_mut(&key)?;
        topic.latest_commit = topic.latest_commit.max(commit_ts);
        let truncated = rows.len() >= limit;
        let rows = rows.into_iter().take(limit).map(|(pk, value)| QueryRow { pk, value }).collect();
        let _ = topic.sender.send(QueryUpdate {
            tenant: tenant.to_string(),
            database: database.to_string(),
            branch: branch.to_string(),
            table: table.to_string(),
            commit_ts,
            sequence,
            rows,
            truncated,
        });
        Some(sequence)
    }

    pub fn presence_join(
        &self,
        tenant: &str,
        channel: &str,
        member: String,
        state: serde_json::Value,
        ttl_secs: u64,
        now_unix: u64,
    ) -> Result<usize> {
        let ttl = ttl_secs.clamp(1, PRESENCE_MAX_TTL_SECS);
        self.presence_join_at(
            tenant,
            channel,
            member,
            state,
            now_unix.saturating_add(ttl),
            now_unix,
        )
    }

    pub fn presence_join_at(
        &self,
        tenant: &str,
        channel: &str,
        member: String,
        state: serde_json::Value,
        expires_unix: u64,
        now_unix: u64,
    ) -> Result<usize> {
        validate_channel(channel)?;
        if member.is_empty() || member.len() > PRESENCE_MAX_MEMBER_BYTES {
            return Err(RymeError::InvalidArgument(String::from("presence member")));
        }
        let state_bytes = serde_json::to_vec(&state)
            .map_err(|_| RymeError::InvalidArgument(String::from("presence state")))?;
        if state_bytes.len() > PRESENCE_MAX_STATE_BYTES {
            return Err(RymeError::Overload(String::from("presence state")));
        }
        let key = scope_key(tenant, channel);
        let shard = self.presence_shard(&key);
        let mut presence = self
            .presence_state
            .get(shard)
            .ok_or_else(|| RymeError::Internal(String::from("presence state shard")))?
            .lock()
            .map_err(|_| RymeError::Internal(String::from("presence state lock")))?;
        let new_room = !presence.contains_key(&key);
        if new_room {
            self.admit_topic(TopicKind::PresenceRoom, tenant)?;
        }
        let members = presence.entry(key).or_insert_with(HashMap::new);
        let expired_members: Vec<(String, usize)> = members
            .iter()
            .filter(|(_, member)| member.expires_unix <= now_unix)
            .map(|(member, value)| (member.clone(), encoded_presence_state_bytes(&value.state)))
            .collect();
        let expired_state_bytes = expired_members.iter().map(|(_, bytes)| *bytes).sum();
        members.retain(|_, member| member.expires_unix > now_unix);
        if !expired_members.is_empty() {
            self.release_presence_capacity(expired_members.len(), expired_state_bytes);
        }
        let existing_state_bytes =
            members.get(&member).map(|value| encoded_presence_state_bytes(&value.state));
        let is_new_member = existing_state_bytes.is_none();
        if is_new_member && members.len() >= PRESENCE_MAX_MEMBERS {
            if new_room {
                presence.remove(&scope_key(tenant, channel));
            }
            drop(presence);
            if new_room {
                self.release_topic(TopicKind::PresenceRoom, tenant);
            }
            return Err(RymeError::Overload(String::from("presence room full")));
        }
        let new_state_bytes = state_bytes.len();
        let capacity_reserved = if let Some(old_state_bytes) = existing_state_bytes {
            if new_state_bytes > old_state_bytes {
                self.reserve_presence_capacity(0, new_state_bytes - old_state_bytes)
            } else {
                true
            }
        } else {
            self.reserve_presence_capacity(1, new_state_bytes)
        };
        if !capacity_reserved {
            if new_room {
                presence.remove(&scope_key(tenant, channel));
            }
            drop(presence);
            if new_room {
                self.release_topic(TopicKind::PresenceRoom, tenant);
            }
            return Err(RymeError::Overload(String::from("presence capacity")));
        }
        if let Some(old_state_bytes) = existing_state_bytes {
            if old_state_bytes > new_state_bytes {
                self.release_presence_capacity(0, old_state_bytes - new_state_bytes);
            }
        }
        let count = members.len() + usize::from(is_new_member);
        let expired: Vec<(String, u64)> = expired_members
            .into_iter()
            .map(|(member, _)| (member, self.next_presence_sequence()))
            .collect();
        let sequence = self.next_presence_sequence();
        members.insert(
            member.clone(),
            PresenceMember { member: member.clone(), state: state.clone(), expires_unix },
        );
        drop(presence);
        for (expired_member, sequence) in expired {
            self.publish_presence(
                tenant,
                PresenceEvent {
                    kind: String::from("leave"),
                    channel: channel.to_string(),
                    member: expired_member,
                    state: serde_json::Value::Null,
                    expires_unix: 0,
                    sequence,
                },
            )?;
        }
        self.publish_presence(
            tenant,
            PresenceEvent {
                kind: String::from("join"),
                channel: channel.to_string(),
                member,
                state,
                expires_unix,
                sequence,
            },
        )?;
        Ok(count)
    }

    pub fn prune_presence(&self, now_unix: u64) -> usize {
        let mut expired_events = Vec::new();
        let mut expired_rooms = Vec::new();
        let mut removed = 0;
        let mut removed_state_bytes = 0usize;
        for shard in self.presence_state.iter() {
            let Ok(mut presence) = shard.lock() else { continue };
            presence.retain(|key, members| {
                let before = members.len();
                let expired: Vec<(String, usize)> = members
                    .iter()
                    .filter(|(_, member)| member.expires_unix <= now_unix)
                    .map(|(member, value)| {
                        (member.clone(), encoded_presence_state_bytes(&value.state))
                    })
                    .collect();
                members.retain(|_, member| member.expires_unix > now_unix);
                removed_state_bytes += expired.iter().map(|(_, bytes)| *bytes).sum::<usize>();
                if let Some((tenant, channel)) = split_scope_key(key) {
                    for (member, _) in expired {
                        expired_events.push((
                            tenant.to_string(),
                            channel.to_string(),
                            member,
                            self.next_presence_sequence(),
                        ));
                    }
                }
                removed += before - members.len();
                if members.is_empty() {
                    expired_rooms.push(key.clone());
                    false
                } else {
                    true
                }
            });
        }
        if removed > 0 {
            self.release_presence_capacity(removed, removed_state_bytes);
        }
        for key in expired_rooms {
            if let Some((tenant, _)) = split_scope_key(&key) {
                self.release_topic(TopicKind::PresenceRoom, tenant);
            }
        }
        for (tenant, channel, member, sequence) in expired_events {
            let _ = self.publish_presence(
                &tenant,
                PresenceEvent {
                    kind: String::from("leave"),
                    channel,
                    member,
                    state: serde_json::Value::Null,
                    expires_unix: 0,
                    sequence,
                },
            );
        }
        removed
    }

    pub fn prune_idle(&self) -> usize {
        let mut removed = 0;
        for shard in self.table_topics.iter() {
            let Ok(mut topics) = shard.lock() else { continue };
            let keys: HashSet<String> = topics
                .topics
                .keys()
                .chain(topics.history.keys())
                .chain(topics.queries.keys())
                .cloned()
                .collect();
            for key in keys {
                let change_active =
                    topics.topics.get(&key).is_some_and(|sender| sender.receiver_count() > 0);
                let query_active =
                    topics.queries.get(&key).is_some_and(|topic| topic.sender.receiver_count() > 0);
                if !change_active && !query_active {
                    if topics.topics.remove(&key).is_some() {
                        removed += 1;
                    }
                    if let Some(history) = topics.history.remove(&key) {
                        for record in &history {
                            self.release_history_bytes(change_record_bytes(record));
                        }
                        removed += 1;
                    }
                    if topics.queries.remove(&key).is_some() {
                        removed += 1;
                    }
                    self.release_table_topic_if_empty(&mut topics, &key);
                } else if !change_active
                    && topics.history.get(&key).map(|log| log.is_empty()).unwrap_or(true)
                    && topics.topics.remove(&key).is_some()
                {
                    removed += 1;
                    self.release_table_topic_if_empty(&mut topics, &key);
                }
            }
        }
        for shard in self.broadcast_topics.iter() {
            if let Ok(mut topics) = shard.lock() {
                let idle_broadcast: Vec<String> = topics
                    .iter()
                    .filter(|(_, topic)| {
                        topic.sender.receiver_count() == 0 && Arc::strong_count(topic) == 1
                    })
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in idle_broadcast {
                    if let Some((tenant, _)) = split_scope_key(&key) {
                        self.release_topic(TopicKind::Broadcast, tenant);
                    }
                    topics.remove(&key);
                    removed += 1;
                }
            }
        }
        for shard in self.presence_topics.iter() {
            if let Ok(mut topics) = shard.lock() {
                let idle_topics: Vec<String> = topics
                    .iter()
                    .filter(|(_, sender)| sender.receiver_count() == 0)
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in idle_topics {
                    if let Some((tenant, _)) = split_scope_key(&key) {
                        self.release_topic(TopicKind::PresenceTopic, tenant);
                    }
                    topics.remove(&key);
                    removed += 1;
                }
            }
        }
        for shard in self.durable_topics.iter() {
            let idle_topics = if let Ok(mut topics) = shard.lock() {
                let idle_topics: Vec<String> = topics
                    .iter()
                    .filter(|(_, sender)| sender.receiver_count() == 0)
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in &idle_topics {
                    topics.remove(key);
                    removed += 1;
                }
                idle_topics
            } else {
                Vec::new()
            };
            if let Ok(mut inner) = self.inner.lock() {
                for key in idle_topics {
                    let empty =
                        inner.durable.get(&key).is_some_and(|topic| topic.messages.is_empty());
                    if empty {
                        if let Some((tenant, _)) = split_scope_key(&key) {
                            inner.durable.remove(&key);
                            let remove_tenant = inner
                                .durable_topics_by_tenant
                                .get_mut(tenant)
                                .is_some_and(|count| {
                                    *count = count.saturating_sub(1);
                                    *count == 0
                                });
                            if remove_tenant {
                                inner.durable_topics_by_tenant.remove(tenant);
                            }
                        }
                    }
                }
            }
        }
        removed
    }

    pub fn presence_leave(&self, tenant: &str, channel: &str, member: &str) -> Result<bool> {
        validate_channel(channel)?;
        if member.is_empty() || member.len() > PRESENCE_MAX_MEMBER_BYTES {
            return Err(RymeError::InvalidArgument(String::from("presence member")));
        }
        let key = scope_key(tenant, channel);
        let shard = self.presence_shard(&key);
        let mut presence = self
            .presence_state
            .get(shard)
            .ok_or_else(|| RymeError::Internal(String::from("presence state shard")))?
            .lock()
            .map_err(|_| RymeError::Internal(String::from("presence state lock")))?;
        let removed_state_bytes = presence
            .get(&key)
            .and_then(|members| members.get(member))
            .map(|value| encoded_presence_state_bytes(&value.state));
        let removed =
            presence.get_mut(&key).map(|members| members.remove(member).is_some()).unwrap_or(false);
        let empty = presence.get(&key).is_some_and(HashMap::is_empty);
        if empty {
            presence.remove(&key);
        }
        let sequence = removed.then(|| self.next_presence_sequence());
        drop(presence);
        if removed {
            self.release_presence_capacity(1, removed_state_bytes.unwrap_or(0));
        }
        if empty {
            self.release_topic(TopicKind::PresenceRoom, tenant);
        }
        if removed {
            self.publish_presence(
                tenant,
                PresenceEvent {
                    kind: String::from("leave"),
                    channel: channel.to_string(),
                    member: member.to_string(),
                    state: serde_json::Value::Null,
                    expires_unix: 0,
                    sequence: sequence.unwrap_or(0),
                },
            )?;
        }
        Ok(removed)
    }

    pub fn presence_list(&self, tenant: &str, channel: &str, now_unix: u64) -> Vec<PresenceMember> {
        self.presence_snapshot(tenant, channel, now_unix).0
    }

    pub fn presence_snapshot(
        &self,
        tenant: &str,
        channel: &str,
        now_unix: u64,
    ) -> (Vec<PresenceMember>, u64) {
        if validate_channel(channel).is_err() {
            return (Vec::new(), self.presence_sequence.load(Ordering::Acquire));
        }
        let mut out = Vec::new();
        let mut expired: Vec<(String, usize)> = Vec::new();
        let sequence;
        let key = scope_key(tenant, channel);
        let shard = self.presence_shard(&key);
        let mut emptied = false;
        if let Some(state_shard) = self.presence_state.get(shard) {
            if let Ok(mut presence) = state_shard.lock() {
                if let Some(members) = presence.get_mut(&key) {
                    expired = members
                        .iter()
                        .filter(|(_, member)| member.expires_unix <= now_unix)
                        .map(|(member, value)| {
                            (member.clone(), encoded_presence_state_bytes(&value.state))
                        })
                        .collect();
                    members.retain(|_, member| member.expires_unix > now_unix);
                    let mut names: Vec<String> = members.keys().cloned().collect();
                    names.sort();
                    for name in names {
                        if let Some(member) = members.get(&name) {
                            out.push(PresenceMember {
                                member: name,
                                state: member.state.clone(),
                                expires_unix: member.expires_unix,
                            });
                        }
                    }
                    if members.is_empty() {
                        presence.remove(&key);
                        emptied = true;
                    }
                }
                let expired_count = expired.len();
                let expired_state_bytes = expired.iter().map(|(_, bytes)| *bytes).sum();
                let expired = expired
                    .into_iter()
                    .map(|(member, _)| (member, self.next_presence_sequence()))
                    .collect::<Vec<_>>();
                sequence = self.presence_sequence.load(Ordering::Acquire);
                drop(presence);
                if expired_count > 0 {
                    self.release_presence_capacity(expired_count, expired_state_bytes);
                }
                if emptied {
                    self.release_topic(TopicKind::PresenceRoom, tenant);
                }
                for (member, sequence) in expired {
                    let _ = self.publish_presence(
                        tenant,
                        PresenceEvent {
                            kind: String::from("leave"),
                            channel: channel.to_string(),
                            member,
                            state: serde_json::Value::Null,
                            expires_unix: 0,
                            sequence,
                        },
                    );
                }
            } else {
                sequence = self.presence_sequence.load(Ordering::Acquire);
            }
        } else {
            sequence = self.presence_sequence.load(Ordering::Acquire);
        }
        (out, sequence)
    }

    pub fn presence_subscribe(
        &self,
        tenant: &str,
        channel: &str,
    ) -> broadcast::Receiver<PresenceEvent> {
        if validate_channel(channel).is_err() {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        }
        let key = scope_key(tenant, channel);
        let shard = self.presence_shard(&key);
        let Ok(mut topics) = self.presence_topics[shard].lock() else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        if !topics.contains_key(&key) && self.admit_topic(TopicKind::PresenceTopic, tenant).is_err()
        {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        }
        topics
            .entry(key)
            .or_insert_with(|| broadcast::channel(self.broadcast_capacity).0)
            .subscribe()
    }

    fn publish_presence(&self, tenant: &str, event: PresenceEvent) -> Result<()> {
        validate_channel(&event.channel)?;
        let key = scope_key(tenant, &event.channel);
        let shard = self.presence_shard(&key);
        let topics = self
            .presence_topics
            .get(shard)
            .ok_or_else(|| RymeError::Internal(String::from("presence shard")))?
            .lock()
            .map_err(|_| RymeError::Internal(String::from("presence lock")))?;
        if let Some(sender) = topics.get(&key) {
            let _ = sender.send(event);
        }
        Ok(())
    }

    pub fn broadcast(
        &self,
        tenant: &str,
        channel: &str,
        from: String,
        payload: serde_json::Value,
        commit_ts: u64,
    ) -> Result<u64> {
        validate_channel(channel)?;
        let key = scope_key(tenant, channel);
        let shard = self.broadcast_shard(&key);
        let Some(topic) = self.existing_broadcast_topic(shard, &key)? else {
            return Ok(self.next_sequence());
        };
        let _publish = topic
            .publish_lock
            .lock()
            .map_err(|_| RymeError::Internal(String::from("broadcast topic lock")))?;
        let sequence = self.next_sequence();
        let message = BroadcastMsg {
            channel: channel.to_string(),
            from,
            payload,
            commit_ts,
            sequence,
            json_cache: Arc::new(OnceLock::new()),
            encoded_frames: Arc::new(std::array::from_fn(|_| tokio::sync::OnceCell::new())),
            fanout_quota_pre_admitted: false,
        };
        message.checked_json_len()?;
        let _ = topic.sender.send(Arc::new(message));
        Ok(sequence)
    }

    pub fn broadcast_with_admission(
        &self,
        tenant: &str,
        channel: &str,
        from: String,
        payload: serde_json::Value,
        commit_ts: u64,
        admit: impl FnOnce(usize, u64) -> Result<()>,
    ) -> Result<u64> {
        validate_channel(channel)?;
        let key = scope_key(tenant, channel);
        let shard = self.broadcast_shard(&key);
        let Some(topic) = self.existing_broadcast_topic(shard, &key)? else {
            let sequence = self.next_sequence();
            let message = BroadcastMsg {
                channel: channel.to_string(),
                from,
                payload,
                commit_ts,
                sequence,
                json_cache: Arc::new(OnceLock::new()),
                encoded_frames: Arc::new(std::array::from_fn(|_| tokio::sync::OnceCell::new())),
                fanout_quota_pre_admitted: true,
            };
            admit(0, message.checked_json_len()? as u64)?;
            return Ok(sequence);
        };
        let _publish = topic
            .publish_lock
            .lock()
            .map_err(|_| RymeError::Internal(String::from("broadcast topic lock")))?;
        let sequence = self.next_sequence();
        let message = BroadcastMsg {
            channel: channel.to_string(),
            from,
            payload,
            commit_ts,
            sequence,
            json_cache: Arc::new(OnceLock::new()),
            encoded_frames: Arc::new(std::array::from_fn(|_| tokio::sync::OnceCell::new())),
            fanout_quota_pre_admitted: true,
        };
        let receiver_count = topic.sender.receiver_count();
        admit(receiver_count, message.checked_json_len()? as u64)?;
        let _ = topic.sender.send(Arc::new(message));
        Ok(sequence)
    }

    pub fn broadcast_with_sequence(
        &self,
        tenant: &str,
        channel: &str,
        from: String,
        payload: serde_json::Value,
        commit_ts: u64,
        sequence: u64,
    ) -> Result<u64> {
        validate_channel(channel)?;
        let key = scope_key(tenant, channel);
        let shard = self.broadcast_shard(&key);
        let Some(topic) = self.existing_broadcast_topic(shard, &key)? else {
            self.sequence.fetch_max(sequence, Ordering::Relaxed);
            return Ok(sequence);
        };
        let _publish = topic
            .publish_lock
            .lock()
            .map_err(|_| RymeError::Internal(String::from("broadcast topic lock")))?;
        self.sequence.fetch_max(sequence, Ordering::Relaxed);
        let message = BroadcastMsg {
            channel: channel.to_string(),
            from,
            payload,
            commit_ts,
            sequence,
            json_cache: Arc::new(OnceLock::new()),
            encoded_frames: Arc::new(std::array::from_fn(|_| tokio::sync::OnceCell::new())),
            fanout_quota_pre_admitted: false,
        };
        message.checked_json_len()?;
        let _ = topic.sender.send(Arc::new(message));
        Ok(sequence)
    }

    pub fn broadcast_subscribe(
        &self,
        tenant: &str,
        channel: &str,
    ) -> broadcast::Receiver<Arc<BroadcastMsg>> {
        if validate_channel(channel).is_err() {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        }
        let key = scope_key(tenant, channel);
        let shard = self.broadcast_shard(&key);
        let Ok(topic) = self.broadcast_topic(shard, key) else {
            let (_, receiver) = broadcast::channel::<Arc<BroadcastMsg>>(16);
            return receiver;
        };
        topic.sender.subscribe()
    }

    fn existing_broadcast_topic(
        &self,
        shard: usize,
        key: &str,
    ) -> Result<Option<Arc<BroadcastTopic>>> {
        let topics = self
            .broadcast_topics
            .get(shard)
            .ok_or_else(|| RymeError::Internal(String::from("broadcast shard")))?
            .lock()
            .map_err(|_| RymeError::Internal(String::from("broadcast lock")))?;
        Ok(topics.get(key).cloned())
    }

    fn broadcast_topic(&self, shard: usize, key: String) -> Result<Arc<BroadcastTopic>> {
        let mut topics = self
            .broadcast_topics
            .get(shard)
            .ok_or_else(|| RymeError::Internal(String::from("broadcast shard")))?
            .lock()
            .map_err(|_| RymeError::Internal(String::from("broadcast lock")))?;
        if !topics.contains_key(&key) {
            let (tenant, _) = split_scope_key(&key)
                .ok_or_else(|| RymeError::InvalidArgument(String::from("broadcast scope")))?;
            self.admit_topic(TopicKind::Broadcast, tenant)?;
        }
        Ok(topics
            .entry(key)
            .or_insert_with(|| {
                Arc::new(BroadcastTopic {
                    sender: broadcast::channel(self.broadcast_capacity).0,
                    publish_lock: Mutex::new(()),
                })
            })
            .clone())
    }

    pub fn durable_append(
        &self,
        tenant: &str,
        partition: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        commit_ts: u64,
        retention: usize,
    ) -> Result<u64> {
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        let cursor = inner
            .durable
            .get(&durable_key(tenant, partition))
            .map(|topic| topic.next_cursor)
            .unwrap_or(0);
        Self::durable_append_locked(
            self, &mut inner, tenant, partition, cursor, key, value, commit_ts, retention,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn durable_append_at(
        &self,
        tenant: &str,
        partition: &str,
        cursor: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        commit_ts: u64,
        retention: usize,
    ) -> Result<u64> {
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        Self::durable_append_locked(
            self, &mut inner, tenant, partition, cursor, key, value, commit_ts, retention,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn durable_append_locked(
        &self,
        inner: &mut RealtimeInner,
        tenant: &str,
        partition: &str,
        cursor: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        commit_ts: u64,
        retention: usize,
    ) -> Result<u64> {
        if tenant.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("tenant")));
        }
        validate_durable_partition(partition)?;
        if key.is_empty() || key.len() > DURABLE_KEY_MAX_BYTES {
            return Err(RymeError::InvalidArgument(String::from("key")));
        }
        if value.len() > DURABLE_VALUE_MAX_BYTES {
            return Err(RymeError::Overload(String::from("value")));
        }
        let topic_key = durable_key(tenant, partition);
        let is_new_topic = !inner.durable.contains_key(&topic_key);
        if is_new_topic {
            let tenant_topics = inner.durable_topics_by_tenant.get(tenant).copied().unwrap_or(0);
            if inner.durable.len() >= MAX_DURABLE_TOPICS
                || tenant_topics >= MAX_DURABLE_TOPICS_PER_TENANT
            {
                return Err(RymeError::Overload(String::from("durable topics")));
            }
        }
        let topic = inner.durable.entry(topic_key.clone()).or_insert_with(|| DurableTopic {
            messages: VecDeque::new(),
            next_cursor: 0,
            retention: retention.clamp(16, 100000),
        });
        if is_new_topic {
            *inner.durable_topics_by_tenant.entry(tenant.to_string()).or_default() += 1;
        }
        if cursor < topic.next_cursor {
            if let Some(existing) = topic.messages.iter().find(|message| message.cursor == cursor) {
                if existing.partition != partition
                    || existing.key != key
                    || existing.value != value
                    || existing.commit_ts != commit_ts
                {
                    return Err(RymeError::Corrupt(String::from(
                        "conflicting durable topic cursor",
                    )));
                }
            }
            return Ok(cursor);
        }
        if cursor > topic.next_cursor {
            return Err(RymeError::Corrupt(String::from("durable topic cursor")));
        }
        if cursor == u64::MAX {
            return Err(RymeError::Overload(String::from("durable topic cursor")));
        }
        topic.retention = retention.clamp(16, 100000);
        topic.next_cursor = topic.next_cursor.saturating_add(1);
        let message =
            DurableMsg { partition: partition.to_string(), cursor, key, value, commit_ts };
        topic.messages.push_back(message.clone());
        while topic.messages.len() > topic.retention {
            topic.messages.pop_front();
        }
        let shard = self.durable_topic_shard(&topic_key);
        if let Some(topics) = self.durable_topics.get(shard) {
            if let Ok(topics) = topics.lock() {
                if let Some(sender) = topics.get(&topic_key) {
                    let _ = sender.send(message);
                }
            }
        }
        Ok(cursor)
    }

    pub fn durable_subscribe(
        &self,
        tenant: &str,
        partition: &str,
    ) -> broadcast::Receiver<DurableMsg> {
        if tenant.is_empty() || validate_durable_partition(partition).is_err() {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        }
        let key = durable_key(tenant, partition);
        if let Ok(mut inner) = self.inner.lock() {
            if !inner.durable.contains_key(&key) {
                let tenant_topics =
                    inner.durable_topics_by_tenant.get(tenant).copied().unwrap_or(0);
                if inner.durable.len() >= MAX_DURABLE_TOPICS
                    || tenant_topics >= MAX_DURABLE_TOPICS_PER_TENANT
                {
                    let (_, receiver) = broadcast::channel(16);
                    return receiver;
                }
                inner.durable.insert(
                    key.clone(),
                    DurableTopic { messages: VecDeque::new(), next_cursor: 0, retention: 16 },
                );
                *inner.durable_topics_by_tenant.entry(tenant.to_string()).or_default() += 1;
            }
        } else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        }
        let shard = self.durable_topic_shard(&key);
        let Some(topics) = self.durable_topics.get(shard) else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        let Ok(mut topics) = topics.lock() else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        topics.entry(key).or_insert_with(|| broadcast::channel(self.capacity).0).subscribe()
    }

    pub fn durable_read(
        &self,
        tenant: &str,
        partition: &str,
        from_cursor: u64,
        limit: usize,
    ) -> Vec<DurableMsg> {
        if tenant.is_empty() || validate_durable_partition(partition).is_err() {
            return Vec::new();
        }
        let limit = limit.clamp(1, DURABLE_READ_MAX_MESSAGES);
        let Ok(inner) = self.inner.lock() else {
            return Vec::new();
        };
        let Some(topic) = inner.durable.get(&durable_key(tenant, partition)) else {
            return Vec::new();
        };
        topic.messages.iter().filter(|msg| msg.cursor >= from_cursor).take(limit).cloned().collect()
    }

    pub fn durable_cursor(&self, tenant: &str, partition: &str) -> u64 {
        if tenant.is_empty() || validate_durable_partition(partition).is_err() {
            return 0;
        }
        self.inner
            .lock()
            .ok()
            .and_then(|inner| {
                inner.durable.get(&durable_key(tenant, partition)).map(|t| t.next_cursor)
            })
            .unwrap_or(0)
    }

    /// Return the oldest and newest retained cursor for a durable partition.
    pub fn durable_cursor_bounds(&self, tenant: &str, partition: &str) -> Option<(u64, u64)> {
        if tenant.is_empty() || validate_durable_partition(partition).is_err() {
            return None;
        }
        self.inner.lock().ok().and_then(|inner| {
            inner.durable.get(&durable_key(tenant, partition)).and_then(|topic| {
                Some((topic.messages.front()?.cursor, topic.messages.back()?.cursor))
            })
        })
    }

    pub fn durable_snapshot(&self) -> Result<Vec<DurableTopicSnapshot>> {
        let inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        let mut snapshots = Vec::with_capacity(inner.durable.len());
        for (scope, topic) in &inner.durable {
            let Some((tenant, partition)) = split_scope_key(scope) else {
                return Err(RymeError::Corrupt(String::from("durable topic scope")));
            };
            snapshots.push(DurableTopicSnapshot {
                tenant: tenant.to_string(),
                partition: partition.to_string(),
                messages: topic.messages.iter().cloned().collect(),
                next_cursor: topic.next_cursor,
                retention: topic.retention,
            });
        }
        snapshots.sort_by(|left, right| {
            left.tenant.cmp(&right.tenant).then(left.partition.cmp(&right.partition))
        });
        Ok(snapshots)
    }

    pub fn restore_durable_snapshot(&self, snapshots: Vec<DurableTopicSnapshot>) -> Result<()> {
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        let mut pending = Vec::with_capacity(snapshots.len());
        let mut seen = HashSet::with_capacity(snapshots.len());
        let mut pending_topics_by_tenant = HashMap::<String, usize>::new();
        for snapshot in snapshots {
            if snapshot.tenant.is_empty() {
                return Err(RymeError::Corrupt(String::from("durable topic identity")));
            }
            validate_durable_partition(&snapshot.partition)
                .map_err(|_| RymeError::Corrupt(String::from("durable topic identity")))?;
            if snapshot.messages.iter().any(|message| message.partition != snapshot.partition) {
                return Err(RymeError::Corrupt(String::from("durable topic partition")));
            }
            if snapshot.messages.iter().any(|message| {
                message.key.is_empty()
                    || message.key.len() > DURABLE_KEY_MAX_BYTES
                    || message.value.len() > DURABLE_VALUE_MAX_BYTES
            }) {
                return Err(RymeError::Corrupt(String::from("durable topic message")));
            }
            let retention = snapshot.retention.clamp(16, 100000);
            let mut messages = snapshot.messages;
            if messages.windows(2).any(|pair| pair[0].cursor >= pair[1].cursor) {
                return Err(RymeError::Corrupt(String::from("durable topic cursor order")));
            }
            if messages.len() > retention {
                messages = messages.split_off(messages.len() - retention);
            }
            let required_next = messages
                .last()
                .map(|message| {
                    message
                        .cursor
                        .checked_add(1)
                        .ok_or_else(|| RymeError::Corrupt(String::from("durable topic cursor")))
                })
                .transpose()?
                .unwrap_or(0);
            if snapshot.next_cursor < required_next {
                return Err(RymeError::Corrupt(String::from("durable topic cursor")));
            }
            let next_cursor = snapshot.next_cursor;
            let key = durable_key(&snapshot.tenant, &snapshot.partition);
            if inner.durable.contains_key(&key) || !seen.insert(key.clone()) {
                return Err(RymeError::Corrupt(String::from("duplicate durable topic")));
            }
            let tenant_topics = inner
                .durable_topics_by_tenant
                .get(&snapshot.tenant)
                .copied()
                .unwrap_or(0)
                .saturating_add(
                    pending_topics_by_tenant.get(&snapshot.tenant).copied().unwrap_or(0),
                );
            if inner.durable.len().saturating_add(pending.len()) > MAX_DURABLE_TOPICS
                || tenant_topics > MAX_DURABLE_TOPICS_PER_TENANT
            {
                return Err(RymeError::Corrupt(String::from("durable topic limit")));
            }
            *pending_topics_by_tenant.entry(snapshot.tenant.clone()).or_default() += 1;
            pending.push((
                key,
                DurableTopic { messages: messages.into_iter().collect(), next_cursor, retention },
            ));
        }
        for (key, topic) in pending {
            let tenant = split_scope_key(&key)
                .map(|(tenant, _)| tenant.to_string())
                .ok_or_else(|| RymeError::Corrupt(String::from("durable topic scope")))?;
            inner.durable.insert(key, topic);
            *inner.durable_topics_by_tenant.entry(tenant).or_default() += 1;
        }
        Ok(())
    }
}

impl Default for Realtime {
    fn default() -> Self {
        Self::new(4096)
    }
}

fn branch_topic_key(tenant: &str, database: &str, branch: &str, table: &str) -> String {
    format!(
        "{}:{}{}:{}{}:{}{}:{}",
        tenant.len(),
        tenant,
        database.len(),
        database,
        branch.len(),
        branch,
        table.len(),
        table
    )
}

fn scope_key(tenant: &str, name: &str) -> String {
    format!("{}:{tenant}{name}", tenant.len())
}

fn split_scope_key(key: &str) -> Option<(&str, &str)> {
    let (tenant_len, scope) = key.split_once(':')?;
    let tenant_len = tenant_len.parse::<usize>().ok()?;
    Some((scope.get(..tenant_len)?, scope.get(tenant_len..)?))
}

fn durable_key(tenant: &str, partition: &str) -> String {
    scope_key(tenant, partition)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_returns_changes_after_commit_oldest_first() {
        let realtime = Realtime::new(64);
        for commit in [10u64, 20, 30] {
            realtime
                .publish(NewChange {
                    tenant: String::from("t"),
                    database: String::from("d"),
                    branch: String::from("main"),
                    table: String::from("docs"),
                    op: crate::Operation::Insert,
                    pk: commit.to_be_bytes().to_vec(),
                    before: None,
                    after: None,
                    commit_ts: commit,
                    tx_id: commit,
                })
                .unwrap();
        }
        assert!(realtime.replay("t", "d", "missing", 0, 10).is_empty());
        let all = realtime.replay("t", "d", "docs", 0, 10);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].commit_ts, 10);
        assert_eq!(all[2].commit_ts, 30);
        assert!(all.windows(2).all(|pair| pair[0].sequence < pair[1].sequence));
        let tail = realtime.replay("t", "d", "docs", 10, 10);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].commit_ts, 20);
        assert!(realtime.replay("t", "d", "docs", 30, 10).is_empty());
        assert_eq!(realtime.replay("t", "d", "docs", 0, 2).len(), 2);
        assert_eq!(realtime.replay("t", "d", "docs", 0, 5000).len(), 3);
        let by_sequence = realtime.replay_after_sequence("t", "d", "docs", all[0].sequence, 10);
        assert_eq!(by_sequence.len(), 2);
        assert_eq!(by_sequence[0].commit_ts, 20);
        assert_eq!(realtime.change_sequence_bounds("t", "d", "main", "docs"), Some((1, 3)));
        assert_eq!(realtime.change_commit_bounds("t", "d", "main", "docs"), Some((10, 30)));
    }

    #[test]
    fn oversized_change_is_rejected_before_topic_admission() {
        let realtime = Realtime::new(64);
        let result = realtime.publish(NewChange {
            tenant: String::from("t"),
            database: String::from("d"),
            branch: String::from("main"),
            table: String::from("docs"),
            op: Operation::Insert,
            pk: b"key".to_vec(),
            before: None,
            after: Some(vec![0; MAX_CHANGE_RECORD_BYTES + 1]),
            commit_ts: 1,
            tx_id: 1,
        });
        assert!(
            matches!(result, Err(RymeError::Overload(message)) if message == "realtime change")
        );
        assert_eq!(realtime.history_bytes.load(Ordering::Acquire), 0);
        assert!(realtime.table_topics.iter().all(|shard| shard.lock().unwrap().history.is_empty()));
    }

    #[test]
    fn concurrent_publish_keeps_topic_sequence_order() {
        let realtime = Realtime::new(1024);
        let workers: Vec<_> = (0..8)
            .map(|worker| {
                let realtime = realtime.clone();
                std::thread::spawn(move || {
                    for index in 0..64u64 {
                        realtime
                            .publish(NewChange {
                                tenant: String::from("t"),
                                database: String::from("d"),
                                branch: String::from("main"),
                                table: String::from("docs"),
                                op: Operation::Insert,
                                pk: format!("{worker}-{index}").into_bytes(),
                                before: None,
                                after: None,
                                commit_ts: index + 1,
                                tx_id: index + 1,
                            })
                            .unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let records = realtime.replay("t", "d", "docs", 0, 1024);
        assert_eq!(records.len(), 512);
        assert!(records.windows(2).all(|pair| pair[0].sequence < pair[1].sequence));
    }

    #[test]
    fn stable_cdc_sequences_match_across_gateways() {
        let left = Realtime::new(64).with_stable_cdc();
        let right = Realtime::new(64).with_stable_cdc();
        let event = || NewChange {
            tenant: String::from("t"),
            database: String::from("d"),
            branch: String::from("main"),
            table: String::from("docs"),
            op: Operation::Insert,
            pk: b"one".to_vec(),
            before: None,
            after: Some(b"value".to_vec()),
            commit_ts: 42,
            tx_id: 42,
        };
        let mut left_events = left.subscribe("t", "d", "docs");
        let mut right_events = right.subscribe("t", "d", "docs");
        let first = left.publish(event()).unwrap();
        let second = right.publish(event()).unwrap();
        assert_eq!(first, second);
        assert_eq!(first, 42_000_001);

        let next = NewChange { pk: b"two".to_vec(), commit_ts: 42, tx_id: 42, ..event() };
        left.publish(next.clone()).unwrap();
        right.publish(next).unwrap();
        assert_eq!(left_events.try_recv().unwrap().sequence, 42_000_001);
        assert_eq!(right_events.try_recv().unwrap().sequence, 42_000_001);
        assert_eq!(left_events.try_recv().unwrap().sequence, 42_000_002);
        assert_eq!(right_events.try_recv().unwrap().sequence, 42_000_002);
    }

    #[test]
    fn query_refresh_flow() {
        let realtime = Realtime::new(64);
        assert_eq!(realtime.query_limit("t", "d", "docs"), None);
        assert_eq!(realtime.query_latest_commit("t", "d", "docs"), 0);
        assert!(realtime.publish_query("t", "d", "docs", 5, Vec::new(), 100).is_none());
        let mut first = realtime.query_subscribe("t", "d", "docs", 10);
        assert_eq!(realtime.query_limit("t", "d", "docs"), Some(10));
        let mut second = realtime.query_subscribe("t", "d", "docs", 50);
        assert_eq!(realtime.query_limit("t", "d", "docs"), Some(50));
        let rows = vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec())];
        let sequence = realtime.publish_query("t", "d", "docs", 7, rows, 50).unwrap();
        assert!(sequence > 0);
        assert_eq!(realtime.query_latest_commit("t", "d", "docs"), 7);
        realtime.publish_query("t", "d", "docs", 6, Vec::new(), 50).unwrap();
        assert_eq!(realtime.query_latest_commit("t", "d", "docs"), 7);
        for receiver in [&mut first, &mut second] {
            let update = receiver.try_recv().unwrap();
            assert_eq!(update.commit_ts, 7);
            assert_eq!(update.rows.len(), 2);
            assert!(!update.truncated);
        }
    }

    #[test]
    fn query_truncation_flag() {
        let realtime = Realtime::new(64);
        let mut receiver = realtime.query_subscribe("t", "d", "docs", 2);
        let rows = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        realtime.publish_query("t", "d", "docs", 9, rows, 2).unwrap();
        let update = receiver.try_recv().unwrap();
        assert_eq!(update.rows.len(), 2);
        assert!(update.truncated);
    }

    #[test]
    fn change_and_query_topics_are_scoped_by_branch() {
        let realtime = Realtime::new(64);
        let mut main_changes = realtime.subscribe_branch("t", "d", "main", "docs");
        let mut preview_changes = realtime.subscribe_branch("t", "d", "preview", "docs");
        realtime
            .publish(NewChange {
                tenant: String::from("t"),
                database: String::from("d"),
                branch: String::from("main"),
                table: String::from("docs"),
                op: Operation::Insert,
                pk: b"main".to_vec(),
                before: None,
                after: Some(b"one".to_vec()),
                commit_ts: 1,
                tx_id: 1,
            })
            .unwrap();
        realtime
            .publish(NewChange {
                tenant: String::from("t"),
                database: String::from("d"),
                branch: String::from("preview"),
                table: String::from("docs"),
                op: Operation::Insert,
                pk: b"preview".to_vec(),
                before: None,
                after: Some(b"two".to_vec()),
                commit_ts: 2,
                tx_id: 2,
            })
            .unwrap();
        assert_eq!(main_changes.try_recv().unwrap().branch, "main");
        assert!(main_changes.try_recv().is_err());
        assert_eq!(preview_changes.try_recv().unwrap().branch, "preview");
        assert!(preview_changes.try_recv().is_err());

        let mut main_queries = realtime.query_subscribe_branch("t", "d", "main", "docs", 10);
        let mut preview_queries = realtime.query_subscribe_branch("t", "d", "preview", "docs", 10);
        realtime
            .publish_query_branch(
                "t",
                "d",
                "preview",
                "docs",
                3,
                vec![(b"preview".to_vec(), b"two".to_vec())],
                10,
            )
            .unwrap();
        assert_eq!(preview_queries.try_recv().unwrap().rows.len(), 1);
        assert!(main_queries.try_recv().is_err());
    }

    #[test]
    fn presence_join_leave_expire() {
        let realtime = Realtime::new(64);
        realtime
            .presence_join(
                "t",
                "room:1",
                String::from("ada"),
                serde_json::json!({"x": 1}),
                60,
                1000,
            )
            .unwrap();
        realtime
            .presence_join(
                "t",
                "room:1",
                String::from("grace"),
                serde_json::json!({"x": 2}),
                60,
                1000,
            )
            .unwrap();
        let members = realtime.presence_list("t", "room:1", 1001);
        assert_eq!(members.len(), 2);
        assert!(realtime.presence_leave("t", "room:1", "ada").unwrap());
        assert_eq!(realtime.presence_list("t", "room:1", 1001).len(), 1);
        assert_eq!(realtime.presence_list("t", "room:1", 2000).len(), 0);
    }

    #[test]
    fn unrelated_presence_rooms_use_independent_state_shards() {
        let realtime = Realtime::new(64);
        let first_channel = "room-0";
        let first_key = scope_key("t", first_channel);
        let first_shard = realtime.presence_shard(&first_key);
        let (second_channel, second_key, second_shard) = (1..)
            .map(|index| {
                let channel = format!("room-{index}");
                let key = scope_key("t", &channel);
                let shard = realtime.presence_shard(&key);
                (channel, key, shard)
            })
            .find(|(_, _, shard)| *shard != first_shard)
            .expect("room keys should distribute across presence shards");

        realtime
            .presence_join("t", first_channel, String::from("a"), serde_json::Value::Null, 60, 1000)
            .unwrap();
        realtime
            .presence_join(
                "t",
                &second_channel,
                String::from("b"),
                serde_json::Value::Null,
                60,
                1000,
            )
            .unwrap();

        assert!(realtime.presence_state[first_shard].lock().unwrap().contains_key(&first_key));
        assert!(realtime.presence_state[second_shard].lock().unwrap().contains_key(&second_key));
    }

    #[test]
    fn presence_scope_keys_do_not_collide_on_embedded_separators() {
        let realtime = Realtime::new(64);
        realtime
            .presence_join("a\0b", "c", String::from("first"), serde_json::Value::Null, 60, 1000)
            .unwrap();
        realtime
            .presence_join("a", "b\0c", String::from("second"), serde_json::Value::Null, 60, 1000)
            .unwrap();

        let first = realtime.presence_list("a\0b", "c", 1001);
        let second = realtime.presence_list("a", "b\0c", 1001);
        assert_eq!(
            first.iter().map(|member| member.member.as_str()).collect::<Vec<_>>(),
            ["first"]
        );
        assert_eq!(
            second.iter().map(|member| member.member.as_str()).collect::<Vec<_>>(),
            ["second"]
        );
    }

    #[test]
    fn presence_room_cap_and_ttl_clamp() {
        let realtime = Realtime::new(64);
        for index in 0..PRESENCE_MAX_MEMBERS {
            realtime
                .presence_join("t", "full", format!("m-{index}"), serde_json::json!(null), 60, 1000)
                .unwrap();
        }
        assert!(realtime
            .presence_join("t", "full", String::from("extra"), serde_json::json!(null), 60, 1000)
            .is_err());
        realtime
            .presence_join("t", "full", String::from("m-0"), serde_json::json!(null), 60, 1000)
            .unwrap();
        realtime
            .presence_join("t", "far", String::from("a"), serde_json::json!(null), u64::MAX, 1000)
            .unwrap();
        assert!(realtime.presence_list("t", "far", 1000 + PRESENCE_MAX_TTL_SECS + 1).is_empty());
    }

    #[test]
    fn presence_global_capacity_is_bounded_and_released() {
        let realtime = Realtime::new(64);
        assert!(realtime
            .reserve_presence_capacity(PRESENCE_MAX_MEMBERS_TOTAL, PRESENCE_MAX_STATE_BYTES_TOTAL));
        assert!(matches!(
            realtime.presence_join(
                "t",
                "full",
                String::from("member"),
                serde_json::Value::Null,
                60,
                1000,
            ),
            Err(RymeError::Overload(message)) if message == "presence capacity"
        ));
        realtime
            .release_presence_capacity(PRESENCE_MAX_MEMBERS_TOTAL, PRESENCE_MAX_STATE_BYTES_TOTAL);
        assert!(realtime
            .presence_join("t", "full", String::from("member"), serde_json::Value::Null, 60, 1000)
            .is_ok());
    }

    #[test]
    fn presence_rejects_oversized_member_and_state() {
        let realtime = Realtime::new(64);
        assert!(matches!(
            realtime.presence_join(
                "t",
                "room",
                "m".repeat(PRESENCE_MAX_MEMBER_BYTES + 1),
                serde_json::Value::Null,
                60,
                1000,
            ),
            Err(RymeError::InvalidArgument(message)) if message == "presence member"
        ));
        assert!(matches!(
            realtime.presence_join(
                "t",
                "room",
                String::from("member"),
                serde_json::Value::String("x".repeat(PRESENCE_MAX_STATE_BYTES)),
                60,
                1000,
            ),
            Err(RymeError::Overload(message)) if message == "presence state"
        ));
        assert!(realtime.presence_list("t", "room", 1000).is_empty());
    }

    #[test]
    fn realtime_channels_are_bounded_before_scope_admission() {
        let realtime = Realtime::new(64);
        let channel = "x".repeat(MAX_CHANNEL_BYTES + 1);
        assert!(matches!(
            realtime.presence_join(
                "t",
                &channel,
                String::from("member"),
                serde_json::Value::Null,
                60,
                1000,
            ),
            Err(RymeError::InvalidArgument(message)) if message == "channel"
        ));
        assert!(matches!(
            realtime.broadcast(
                "t",
                &channel,
                String::from("sender"),
                serde_json::Value::Null,
                1,
            ),
            Err(RymeError::InvalidArgument(message)) if message == "channel"
        ));
        assert!(realtime.presence_list("t", &channel, 1000).is_empty());
    }

    #[test]
    fn presence_subscribers_receive_join_leave_and_expiry_events() {
        let realtime = Realtime::new(64);
        let mut events = realtime.presence_subscribe("org/team", "room/main");
        realtime
            .presence_join(
                "org/team",
                "room/main",
                String::from("ada"),
                serde_json::json!({"typing": true}),
                10,
                1000,
            )
            .unwrap();
        let joined = events.try_recv().unwrap();
        assert_eq!(joined.kind, "join");
        assert_eq!(joined.member, "ada");
        assert_eq!(joined.state["typing"], true);
        assert_eq!(joined.expires_unix, 1010);

        assert_eq!(realtime.prune_presence(1011), 1);
        let expired = events.try_recv().unwrap();
        assert_eq!(expired.kind, "leave");
        assert_eq!(expired.member, "ada");
        assert_eq!(expired.state, serde_json::Value::Null);
    }

    #[test]
    fn prune_idle_drops_subscriberless_empty_scopes() {
        let realtime = Realtime::new(64);
        let idle_change = realtime.subscribe("t", "d", "ghost");
        drop(idle_change);
        let idle_cast = realtime.broadcast_subscribe("t", "nowhere");
        drop(idle_cast);
        let idle_query = realtime.query_subscribe("t", "d", "ghost", 10);
        drop(idle_query);
        assert_eq!(realtime.prune_idle(), 3);
        assert_eq!(realtime.prune_idle(), 0);
        realtime
            .presence_join("t", "room", String::from("a"), serde_json::json!(null), 60, 1000)
            .unwrap();
        let live = realtime.subscribe("t", "d", "kept");
        realtime
            .broadcast("t", "kept-cast", String::from("a"), serde_json::json!(true), 1)
            .unwrap();
        let kept_cast = realtime.broadcast_subscribe("t", "kept-cast");
        assert_eq!(realtime.prune_idle(), 0);
        drop(live);
        drop(kept_cast);
    }

    #[test]
    fn broadcast_payload_is_bounded_at_the_fanout_boundary() {
        let realtime = Realtime::new(64);
        let mut receiver = realtime.broadcast_subscribe("t", "room");
        let result = realtime.broadcast(
            "t",
            "room",
            String::from("sender"),
            serde_json::Value::String("x".repeat(MAX_BROADCAST_MESSAGE_BYTES + 1)),
            1,
        );
        assert!(matches!(
            result,
            Err(RymeError::Overload(message)) if message == "broadcast payload"
        ));
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn prune_idle_reclaims_history_only_table_topics() {
        let realtime = Realtime::new(64);
        realtime
            .publish(NewChange {
                tenant: String::from("t"),
                database: String::from("d"),
                branch: String::from("main"),
                table: String::from("history-only"),
                op: Operation::Insert,
                pk: b"one".to_vec(),
                before: None,
                after: Some(b"value".to_vec()),
                commit_ts: 1,
                tx_id: 1,
            })
            .unwrap();
        assert_eq!(realtime.replay("t", "d", "history-only", 0, 10).len(), 1);
        assert!(realtime.history_bytes.load(Ordering::Acquire) > 0);
        assert_eq!(realtime.prune_idle(), 1);
        assert_eq!(realtime.history_bytes.load(Ordering::Acquire), 0);
        assert!(realtime.replay("t", "d", "history-only", 0, 10).is_empty());
        assert_eq!(realtime.prune_idle(), 0);
    }

    #[test]
    fn realtime_topic_registry_rejects_global_and_tenant_overflow() {
        let realtime = Realtime::new(64);
        {
            let mut registry = realtime.topic_registry.lock().unwrap();
            registry.table.total = MAX_TABLE_TOPICS;
        }
        let change = NewChange {
            tenant: String::from("t"),
            database: String::from("d"),
            branch: String::from("main"),
            table: String::from("docs"),
            op: Operation::Insert,
            pk: b"one".to_vec(),
            before: None,
            after: None,
            commit_ts: 1,
            tx_id: 1,
        };
        assert!(matches!(realtime.publish(change.clone()), Err(RymeError::Overload(_))));

        {
            let mut registry = realtime.topic_registry.lock().unwrap();
            registry.table.total = 0;
            registry.table.per_tenant.insert(String::from("t"), MAX_TABLE_TOPICS_PER_TENANT);
        }
        assert!(matches!(realtime.publish(change), Err(RymeError::Overload(_))));
    }

    #[test]
    fn branch_topic_keys_keep_tenants_isolated_when_names_contain_separators() {
        let realtime = Realtime::new(64);
        let mut first = realtime.subscribe_branch("a/b", "c", "main", "docs");
        let mut second = realtime.subscribe_branch("a", "b/c", "main", "docs");
        for (tenant, database, pk) in [("a/b", "c", &b"first"[..]), ("a", "b/c", &b"second"[..])] {
            realtime
                .publish(NewChange {
                    tenant: tenant.to_string(),
                    database: database.to_string(),
                    branch: String::from("main"),
                    table: String::from("docs"),
                    op: Operation::Insert,
                    pk: pk.to_vec(),
                    before: None,
                    after: None,
                    commit_ts: 1,
                    tx_id: 1,
                })
                .unwrap();
        }
        assert_eq!(first.try_recv().unwrap().pk, b"first");
        assert!(first.try_recv().is_err());
        assert_eq!(second.try_recv().unwrap().pk, b"second");
        assert!(second.try_recv().is_err());
    }

    #[test]
    fn broadcast_fanout() {
        let realtime = Realtime::new(64);
        let mut first = realtime.broadcast_subscribe("t", "lobby");
        let mut second = realtime.broadcast_subscribe("t", "lobby");
        realtime
            .broadcast("t", "lobby", String::from("ada"), serde_json::json!({"hello": true}), 3)
            .unwrap();
        for receiver in [&mut first, &mut second] {
            let msg = receiver.try_recv().unwrap();
            assert_eq!(msg.channel, "lobby");
            assert_eq!(msg.commit_ts, 3);
        }
    }

    #[test]
    fn broadcast_without_subscribers_does_not_create_ephemeral_topic() {
        let realtime = Realtime::new(64);
        realtime
            .broadcast("t", "unobserved", String::from("ada"), serde_json::json!({"x": 1}), 1)
            .unwrap();
        assert!(realtime.broadcast_topics.iter().all(|shard| shard.lock().unwrap().is_empty()));

        let mut receiver = realtime.broadcast_subscribe("t", "unobserved");
        realtime
            .broadcast("t", "unobserved", String::from("ada"), serde_json::json!({"x": 2}), 2)
            .unwrap();
        assert_eq!(receiver.try_recv().unwrap().commit_ts, 2);
    }

    #[test]
    fn broadcast_fanout_shares_one_immutable_event() {
        let realtime = Realtime::new(64);
        let mut first = realtime.broadcast_subscribe("t", "lobby");
        let mut second = realtime.broadcast_subscribe("t", "lobby");
        realtime
            .broadcast(
                "t",
                "lobby",
                String::from("ada"),
                serde_json::json!({"large": ["payload", 1, 2, 3]}),
                3,
            )
            .unwrap();

        let first = first.try_recv().unwrap();
        let second = second.try_recv().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn broadcast_reuses_bounded_protocol_frames() {
        let realtime = Realtime::new(64);
        let mut receiver = realtime.broadcast_subscribe("t", "lobby");
        realtime
            .broadcast("t", "lobby", String::from("ada"), serde_json::json!({"x": 1}), 3)
            .unwrap();
        let message = receiver.try_recv().unwrap();
        let encodes = std::sync::atomic::AtomicUsize::new(0);

        let first = message
            .cached_text_frame("room:lobby", 0, || {
                encodes.fetch_add(1, Ordering::Relaxed);
                String::from("encoded")
            })
            .await;
        let second = message
            .cached_text_frame("room:lobby", 0, || {
                encodes.fetch_add(1, Ordering::Relaxed);
                String::from("unexpected re-encode")
            })
            .await;
        let array_frame = message
            .cached_text_frame("room:lobby", 1, || {
                encodes.fetch_add(1, Ordering::Relaxed);
                String::from("array encoded")
            })
            .await;
        let repeated_array_frame = message
            .cached_text_frame("room:lobby", 1, || {
                encodes.fetch_add(1, Ordering::Relaxed);
                String::from("unexpected array re-encode")
            })
            .await;
        let other_topic_frame = message
            .cached_text_frame("other:lobby", 0, || {
                encodes.fetch_add(1, Ordering::Relaxed);
                String::from("other topic")
            })
            .await;
        let repeated_other_topic = message
            .cached_text_frame("other:lobby", 0, || {
                encodes.fetch_add(1, Ordering::Relaxed);
                String::from("other topic again")
            })
            .await;

        assert_eq!(first.as_ref(), "encoded");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(array_frame.as_ref(), "array encoded");
        assert!(Arc::ptr_eq(&array_frame, &repeated_array_frame));
        assert_eq!(other_topic_frame.as_ref(), "other topic");
        assert_eq!(repeated_other_topic.as_ref(), "other topic again");
        assert_eq!(encodes.load(Ordering::Relaxed), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn broadcast_fanout_encodes_a_cold_frame_once() {
        const SUBSCRIBERS: usize = 32;

        let realtime = Realtime::new(64);
        let mut receiver = realtime.broadcast_subscribe("t", "lobby");
        realtime
            .broadcast("t", "lobby", String::from("ada"), serde_json::json!({"x": 1}), 3)
            .unwrap();
        let message = receiver.try_recv().unwrap();
        let start = Arc::new(tokio::sync::Barrier::new(SUBSCRIBERS));
        let encodes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut tasks = Vec::with_capacity(SUBSCRIBERS);

        for _ in 0..SUBSCRIBERS {
            let message = Arc::clone(&message);
            let start = Arc::clone(&start);
            let encodes = Arc::clone(&encodes);
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                message
                    .cached_text_frame("room:lobby", 0, || {
                        encodes.fetch_add(1, Ordering::Relaxed);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        String::from("encoded")
                    })
                    .await
            }));
        }

        let mut frames = Vec::with_capacity(SUBSCRIBERS);
        for task in tasks {
            frames.push(task.await.unwrap());
        }
        assert_eq!(encodes.load(Ordering::Relaxed), 1);
        assert!(frames.iter().all(|frame| Arc::ptr_eq(frame, &frames[0])));
    }

    #[test]
    fn broadcast_with_admission_charges_actual_fanout_before_sending() {
        let realtime = Realtime::new(64);
        let mut receiver = realtime.broadcast_subscribe("t", "lobby");
        let mut admitted = None;
        realtime
            .broadcast_with_admission(
                "t",
                "lobby",
                String::from("ada"),
                serde_json::json!({"hello": true}),
                3,
                |subscribers, bytes| {
                    admitted = Some((subscribers, bytes));
                    Ok(())
                },
            )
            .unwrap();
        let message = receiver.try_recv().unwrap();
        assert_eq!(admitted, Some((1, message.json_len() as u64)));

        let error = realtime.broadcast_with_admission(
            "t",
            "lobby",
            String::from("ada"),
            serde_json::Value::Null,
            4,
            |_, _| Err(ryme_error::RymeError::Overload(String::from("test quota"))),
        );
        assert!(error.is_err());
        assert!(matches!(receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
    }

    #[test]
    fn broadcast_admission_does_not_block_other_topics_in_the_same_shard() {
        let realtime = Realtime::new(64);
        let blocked_channel = "blocked";
        let blocked_key = scope_key("t", blocked_channel);
        let shard = realtime.broadcast_shard(&blocked_key);
        let other_channel = (0..10_000)
            .map(|index| format!("other-{index}"))
            .find(|channel| realtime.broadcast_shard(&scope_key("t", channel)) == shard)
            .expect("find a channel that hashes to the same shard");

        let entered_admission = Arc::new(std::sync::Barrier::new(2));
        let release_admission = Arc::new(std::sync::Barrier::new(2));
        let publisher_realtime = realtime.clone();
        let publisher_entered = entered_admission.clone();
        let publisher_release = release_admission.clone();
        let publisher = std::thread::spawn(move || {
            publisher_realtime.broadcast_with_admission(
                "t",
                blocked_channel,
                String::from("publisher"),
                serde_json::Value::Null,
                1,
                |_, _| {
                    publisher_entered.wait();
                    publisher_release.wait();
                    Ok(())
                },
            )
        });
        entered_admission.wait();

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let other_realtime = realtime.clone();
        let other_publisher = std::thread::spawn(move || {
            let result = other_realtime.broadcast(
                "t",
                &other_channel,
                String::from("publisher"),
                serde_json::Value::Null,
                1,
            );
            result_tx.send(result).unwrap();
        });
        let other_result = result_rx.recv_timeout(std::time::Duration::from_secs(1));

        release_admission.wait();
        publisher.join().unwrap().unwrap();
        other_publisher.join().unwrap();
        assert!(other_result.expect("unrelated topic publish was blocked").is_ok());
    }

    #[test]
    fn broadcast_sequences_are_unique_under_concurrent_publishers() {
        let realtime = Realtime::new(512);
        let mut receiver = realtime.broadcast_subscribe("t", "lobby");
        let mut workers = Vec::new();
        for worker in 0..4u64 {
            let realtime = realtime.clone();
            workers.push(std::thread::spawn(move || {
                (0..100u64)
                    .map(|message| {
                        realtime
                            .broadcast(
                                "t",
                                "lobby",
                                format!("{worker}-{message}"),
                                serde_json::Value::Null,
                                message,
                            )
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            }));
        }
        let mut sequences =
            workers.into_iter().flat_map(|worker| worker.join().unwrap()).collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=400).collect::<Vec<_>>());
        for expected in 1..=400 {
            assert_eq!(receiver.try_recv().unwrap().sequence, expected);
        }
    }

    #[test]
    fn durable_cursor_resume() {
        let realtime = Realtime::new(64);
        let mut live = realtime.durable_subscribe("t", "orders");
        let first =
            realtime.durable_append("t", "orders", b"k1".to_vec(), b"v1".to_vec(), 5, 16).unwrap();
        let second =
            realtime.durable_append("t", "orders", b"k2".to_vec(), b"v2".to_vec(), 6, 16).unwrap();
        assert_eq!(second, first + 1);
        assert_eq!(realtime.durable_cursor("t", "orders"), second + 1);
        let tail = realtime.durable_read("t", "orders", second, 10);
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].key, b"k2".to_vec());
        let all = realtime.durable_read("t", "orders", 0, 10);
        assert_eq!(all.len(), 2);
        assert_eq!(live.try_recv().unwrap().cursor, first);
        assert_eq!(live.try_recv().unwrap().cursor, second);
        assert_eq!(realtime.durable_cursor_bounds("t", "orders"), Some((first, second)));
    }

    #[test]
    fn durable_subscriptions_bound_and_reclaim_empty_topics() {
        let realtime = Realtime::new(64);
        let live = realtime.durable_subscribe("tenant", "orders");
        assert_eq!(realtime.durable_snapshot().unwrap().len(), 1);
        drop(live);
        assert_eq!(realtime.prune_idle(), 1);
        assert!(realtime.durable_snapshot().unwrap().is_empty());

        realtime
            .inner
            .lock()
            .unwrap()
            .durable_topics_by_tenant
            .insert(String::from("tenant"), MAX_DURABLE_TOPICS_PER_TENANT);
        let mut rejected = realtime.durable_subscribe("tenant", "rejected");
        assert!(matches!(rejected.try_recv(), Err(broadcast::error::TryRecvError::Closed)));
        assert!(realtime
            .durable_snapshot()
            .unwrap()
            .iter()
            .all(|topic| topic.partition != "rejected"));
    }

    #[test]
    fn durable_topics_round_trip_through_snapshot() {
        let realtime = Realtime::new(64);
        realtime.durable_append("tenant", "orders", b"k".to_vec(), b"v".to_vec(), 5, 16).unwrap();
        let snapshots = realtime.durable_snapshot().unwrap();
        let restored = Realtime::new(64);
        restored.restore_durable_snapshot(snapshots).unwrap();
        assert_eq!(restored.durable_cursor("tenant", "orders"), 1);
        let messages = restored.durable_read("tenant", "orders", 0, 10);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key, b"k");
    }

    #[test]
    fn durable_topic_scope_codec_handles_delimiters_in_tenant() {
        let realtime = Realtime::new(64);
        realtime
            .durable_append("tenant\0with-delimiter", "orders", b"k".to_vec(), b"v".to_vec(), 5, 16)
            .unwrap();
        let snapshots = realtime.durable_snapshot().unwrap();
        assert_eq!(snapshots[0].tenant, "tenant\0with-delimiter");
        assert_eq!(snapshots[0].partition, "orders");

        let restored = Realtime::new(64);
        restored.restore_durable_snapshot(snapshots).unwrap();
        assert_eq!(restored.durable_read("tenant\0with-delimiter", "orders", 0, 10).len(), 1);
    }

    #[test]
    fn durable_topic_rejects_invalid_partitions_and_conflicting_replays() {
        assert!(validate_durable_partition("").is_err());
        assert!(validate_durable_partition("bad\0partition").is_err());
        assert!(validate_durable_partition(&"x".repeat(DURABLE_PARTITION_MAX_BYTES + 1)).is_err());

        let realtime = Realtime::new(64);
        realtime.durable_append("tenant", "orders", b"k".to_vec(), b"v".to_vec(), 5, 16).unwrap();
        assert!(matches!(
            realtime.durable_append_at(
                "tenant",
                "orders",
                0,
                b"different".to_vec(),
                b"v".to_vec(),
                5,
                16,
            ),
            Err(RymeError::Corrupt(_))
        ));
        assert!(realtime.durable_read("tenant", "bad\0partition", 0, 10).is_empty());
    }

    #[test]
    fn durable_topics_bound_message_shapes_at_the_storage_boundary() {
        let realtime = Realtime::new(64);
        assert!(matches!(
            realtime.durable_append("tenant", "orders", Vec::new(), b"v".to_vec(), 5, 16),
            Err(RymeError::InvalidArgument(_))
        ));
        assert!(matches!(
            realtime.durable_append(
                "tenant",
                "orders",
                b"k".to_vec(),
                vec![b'v'; DURABLE_VALUE_MAX_BYTES + 1],
                5,
                16,
            ),
            Err(RymeError::Overload(_))
        ));

        let snapshot = DurableTopicSnapshot {
            tenant: String::from("tenant"),
            partition: String::from("orders"),
            messages: vec![DurableMsg {
                partition: String::from("orders"),
                cursor: 0,
                key: b"k".to_vec(),
                value: vec![b'v'; DURABLE_VALUE_MAX_BYTES + 1],
                commit_ts: 1,
            }],
            next_cursor: 1,
            retention: 16,
        };
        assert!(matches!(
            realtime.restore_durable_snapshot(vec![snapshot]),
            Err(RymeError::Corrupt(_))
        ));
    }

    #[test]
    fn durable_snapshot_rejects_invalid_cursor_order() {
        let realtime = Realtime::new(64);
        let snapshot = DurableTopicSnapshot {
            tenant: String::from("tenant"),
            partition: String::from("orders"),
            messages: vec![
                DurableMsg {
                    partition: String::from("orders"),
                    cursor: 1,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    commit_ts: 1,
                },
                DurableMsg {
                    partition: String::from("orders"),
                    cursor: 1,
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec(),
                    commit_ts: 2,
                },
            ],
            next_cursor: 2,
            retention: 16,
        };
        assert!(matches!(
            realtime.restore_durable_snapshot(vec![snapshot]),
            Err(RymeError::Corrupt(_))
        ));
    }

    #[test]
    fn durable_snapshot_restore_is_atomic_on_late_validation_failure() {
        let realtime = Realtime::new(64);
        let valid = DurableTopicSnapshot {
            tenant: String::from("tenant"),
            partition: String::from("orders"),
            messages: Vec::new(),
            next_cursor: 0,
            retention: 16,
        };
        let invalid = DurableTopicSnapshot {
            tenant: String::from("tenant"),
            partition: String::new(),
            messages: Vec::new(),
            next_cursor: 0,
            retention: 16,
        };

        assert!(matches!(
            realtime.restore_durable_snapshot(vec![valid, invalid]),
            Err(RymeError::Corrupt(_))
        ));
        assert_eq!(realtime.durable_cursor("tenant", "orders"), 0);
        assert!(realtime.durable_read("tenant", "orders", 0, 10).is_empty());
    }

    #[test]
    fn durable_snapshot_restore_trims_retention_in_linear_time() {
        let messages = (0..32u64)
            .map(|cursor| DurableMsg {
                partition: String::from("orders"),
                cursor,
                key: cursor.to_be_bytes().to_vec(),
                value: vec![cursor as u8],
                commit_ts: cursor,
            })
            .collect();
        let realtime = Realtime::new(64);
        realtime
            .restore_durable_snapshot(vec![DurableTopicSnapshot {
                tenant: String::from("tenant"),
                partition: String::from("orders"),
                messages,
                next_cursor: 32,
                retention: 16,
            }])
            .unwrap();

        let retained = realtime.durable_read("tenant", "orders", 0, 100);
        assert_eq!(retained.len(), 16);
        assert_eq!(retained.first().map(|message| message.cursor), Some(16));
        assert_eq!(realtime.durable_cursor("tenant", "orders"), 32);
    }

    #[test]
    fn retained_cursor_bounds_expose_expired_replays() {
        let realtime = Realtime::new(16);
        for cursor in 0..20u64 {
            realtime
                .publish(NewChange {
                    tenant: String::from("t"),
                    database: String::from("d"),
                    branch: String::from("main"),
                    table: String::from("docs"),
                    op: Operation::Insert,
                    pk: cursor.to_be_bytes().to_vec(),
                    before: None,
                    after: None,
                    commit_ts: cursor + 1,
                    tx_id: cursor + 1,
                })
                .unwrap();
            realtime
                .durable_append(
                    "t",
                    "orders",
                    cursor.to_be_bytes().to_vec(),
                    b"value".to_vec(),
                    cursor + 1,
                    16,
                )
                .unwrap();
        }
        assert_eq!(realtime.change_sequence_bounds("t", "d", "main", "docs"), Some((5, 20)));
        assert_eq!(realtime.durable_cursor_bounds("t", "orders"), Some((4, 19)));
        assert_eq!(realtime.replay_after_sequence("t", "d", "docs", 0, 100)[0].sequence, 5);
        assert_eq!(realtime.durable_read("t", "orders", 0, 100)[0].cursor, 4);
    }

    #[test]
    fn tenants_share_no_rooms_channels_or_partitions() {
        let realtime = Realtime::new(64);
        realtime
            .presence_join("t", "room", String::from("ada"), serde_json::json!(null), 60, 1000)
            .unwrap();
        assert_eq!(realtime.presence_list("t", "room", 1001).len(), 1);
        assert!(realtime.presence_list("u", "room", 1001).is_empty());
        assert!(!realtime.presence_leave("u", "room", "ada").unwrap());
        assert_eq!(realtime.presence_list("t", "room", 1001).len(), 1);
        let mut subscriber = realtime.broadcast_subscribe("u", "lobby");
        realtime.broadcast("t", "lobby", String::from("ada"), serde_json::json!(true), 3).unwrap();
        assert!(subscriber.try_recv().is_err());
        realtime.durable_append("t", "orders", b"k".to_vec(), b"v".to_vec(), 5, 16).unwrap();
        assert_eq!(realtime.durable_cursor("u", "orders"), 0);
        assert!(realtime.durable_read("u", "orders", 0, 10).is_empty());
        assert_eq!(realtime.durable_read("t", "orders", 0, 10).len(), 1);
    }

    #[test]
    fn prune_presence_drops_expired_and_empty_rooms() {
        let realtime = Realtime::new(64);
        realtime
            .presence_join("t", "gone", String::from("a"), serde_json::json!(null), 10, 1000)
            .unwrap();
        realtime
            .presence_join("t", "gone", String::from("b"), serde_json::json!(null), 10, 1000)
            .unwrap();
        realtime
            .presence_join("t", "stays", String::from("c"), serde_json::json!(null), 5000, 1000)
            .unwrap();
        assert_eq!(realtime.prune_presence(1005), 0);
        assert_eq!(realtime.prune_presence(2000), 2);
        assert!(realtime.presence_list("t", "gone", 2000).is_empty());
        assert_eq!(realtime.presence_list("t", "stays", 2000).len(), 1);
        let count = realtime
            .presence_join("t", "stays", String::from("d"), serde_json::json!(null), 60, 3000)
            .unwrap();
        assert_eq!(count, 2);
        let revived = realtime
            .presence_join("t", "gone", String::from("e"), serde_json::json!(null), 60, 3000)
            .unwrap();
        assert_eq!(revived, 1);
    }
}
