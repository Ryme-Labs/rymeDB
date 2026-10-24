use mlua::{HookTriggers, Lua, LuaOptions, MultiValue, StdLib, Value as LuaValue, VmState};
use ryme_auth::Principal;
use ryme_error::{Result, RymeError};
pub use ryme_gateway::{RemoteRead, RemoteReader};
use ryme_metering::{MeterRegistry, Metric, UsageEvent};
use ryme_observe::{
    Histogram, LatencyWindow, SlowEntry, SlowLog, TraceCollector, TraceSpan, SLOW_THRESHOLD_MICROS,
};
use ryme_qos::QosRegistry;
use ryme_realtime::{NewChange, Operation, Realtime};
use ryme_router::RangeLoadHook;
use ryme_storage::RecordKey;
use ryme_txn::{Transaction, TxnBackend, TxnManager};
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, LazyLock, Mutex,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore};

const KV_TABLE: &str = "_kv";
const RESP_MAX_FRAME_BYTES: usize = 1024 * 1024;
const RESP_READ_CHUNK_BYTES: usize = 16 * 1024;
const RESP_INPUT_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const RESP_INPUT_BUDGET_UNIT_BYTES: usize = 16 * 1024;
const RESP_MULTI_MAX_QUEUE_BYTES: usize = 16 * 1024 * 1024;
const RESP_MULTI_MAX_QUEUE_COMMANDS: usize = 10_000;
const RESP_MULTI_QUEUE_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const RESP_PUBSUB_QUEUE_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const RESP_PUBSUB_EGRESS_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const RESP_MAX_COMMAND_ARGS: usize = 1024;
const RESP_MAX_COLLECTION_BYTES: usize = 16 * 1024 * 1024;
const RESP_MAX_EXEC_REPLY_BYTES: usize = RESP_MAX_COLLECTION_BYTES;
const RESP_SCAN_PAGE_ITEMS: usize = 4;
const RESP_MAX_SCAN_RESULT_ITEMS: usize = 10_000;
const RESP_MAX_SCAN_RESULT_KEY_BYTES: usize = RESP_MAX_COLLECTION_BYTES;
const RESP_MGET_BATCH_ITEMS: usize = 4;
const RESP_LUA_MAX_NESTING: usize = 64;
const RESP_LUA_MAX_ARRAY_ITEMS: usize = 65_536;
const RESP_LUA_MAX_REPLY_BYTES: usize = RESP_MAX_COLLECTION_BYTES;
const RESP_SCRIPT_MAX_BYTES: usize = 1024 * 1024;
const RESP_SCRIPT_CACHE_MAX_ENTRIES: usize = 256;
const RESP_SCRIPT_CACHE_MAX_BYTES: usize = 16 * 1024 * 1024;
const RESP_MAX_PUBSUB_SUBSCRIPTIONS: usize = 1024;
const RESP_MAX_PUBSUB_NAME_BYTES: usize = 4 * 1024;
const RESP_MAX_GLOBAL_PUBSUB_SUBSCRIPTIONS: usize = 8192;
const RESP_MAX_GLOBAL_PUBSUB_PATTERNS: usize = 1024;
const RESP_MAX_CLUSTER_BRIDGES: usize = 10_000;
const MAX_COMMAND_CONFLICT_RETRIES: usize = 64;
pub const DEFAULT_MAX_CONNECTIONS: usize = 10_000;
static RESP_INPUT_BUDGET: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    Arc::new(Semaphore::new(RESP_INPUT_BUDGET_BYTES / RESP_INPUT_BUDGET_UNIT_BYTES))
});
static RESP_MULTI_QUEUE_BUDGET: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    Arc::new(Semaphore::new(RESP_MULTI_QUEUE_BUDGET_BYTES / RESP_INPUT_BUDGET_UNIT_BYTES))
});
static RESP_PUBSUB_QUEUE_BUDGET: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    Arc::new(Semaphore::new(RESP_PUBSUB_QUEUE_BUDGET_BYTES / RESP_INPUT_BUDGET_UNIT_BYTES))
});
static RESP_PUBSUB_EGRESS_BUDGET: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    Arc::new(Semaphore::new(RESP_PUBSUB_EGRESS_BUDGET_BYTES / RESP_INPUT_BUDGET_UNIT_BYTES))
});
type CdcPending = Vec<(RecordKey, Operation, Option<Vec<u8>>, Option<Vec<u8>>)>;
type PubSubKey = (String, Vec<u8>);
type PubSubCounts = Arc<Mutex<std::collections::HashMap<PubSubKey, usize>>>;
type ScriptCache = Arc<Mutex<std::collections::HashMap<(String, String), Vec<u8>>>>;

fn check_collection_bytes(bytes: &[u8]) -> std::result::Result<(), String> {
    if bytes.len() > RESP_MAX_COLLECTION_BYTES {
        Err(String::from("collection exceeds the 16 MiB limit"))
    } else {
        Ok(())
    }
}

fn next_scan_result_key_bytes(
    item_count: usize,
    current_bytes: usize,
    key_bytes: usize,
) -> std::result::Result<usize, String> {
    if item_count >= RESP_MAX_SCAN_RESULT_ITEMS {
        return Err(String::from("scan result exceeds the 10,000 key limit"));
    }
    current_bytes
        .checked_add(key_bytes)
        .filter(|bytes| *bytes <= RESP_MAX_SCAN_RESULT_KEY_BYTES)
        .ok_or_else(|| String::from("scan result exceeds the 16 MiB key limit"))
}

fn next_resp_collection_bytes(
    current_bytes: usize,
    item_bytes: usize,
) -> std::result::Result<usize, String> {
    current_bytes
        .checked_add(item_bytes)
        .filter(|bytes| *bytes <= RESP_MAX_COLLECTION_BYTES)
        .ok_or_else(|| String::from("response exceeds the 16 MiB limit"))
}

pub type RespAuthenticator = Arc<dyn Fn(&str, &str) -> Result<Principal> + Send + Sync>;

#[derive(Clone)]
pub struct RespGateway<B = TxnManager> {
    manager: B,
    tenant: String,
    database: String,
    branch: String,
    authenticator: Option<RespAuthenticator>,
    realtime: Option<Realtime>,
    realtime_replicator: Option<Arc<dyn Fn(Vec<u8>) + Send + Sync>>,
    qos: Option<Arc<Mutex<QosRegistry>>>,
    metering: Option<Arc<Mutex<MeterRegistry>>>,
    latency: Option<LatencyWindow>,
    histogram: Option<Histogram>,
    slow_log: Option<SlowLog>,
    traces: Option<Arc<Mutex<TraceCollector>>>,
    range_hook: RangeLoadHook,
    pubsub: PubSubBus,
    scripts: ScriptCache,
    next_client_id: Arc<AtomicU64>,
    read_only: bool,
    read_allowed: bool,
    write_allowed: bool,
    publish_allowed: bool,
    remote_reader: Option<RemoteReader>,
}

impl<B: std::fmt::Debug> std::fmt::Debug for RespGateway<B> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RespGateway")
            .field("manager", &self.manager)
            .field("tenant", &self.tenant)
            .field("database", &self.database)
            .field("branch", &self.branch)
            .field("authenticator_configured", &self.authenticator.is_some())
            .field("read_only", &self.read_only)
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
struct ClientState {
    name: Option<Vec<u8>>,
    id: u64,
    authenticated: bool,
    tenant: String,
    resp3: bool,
    subscriptions: std::collections::BTreeSet<Vec<u8>>,
    patterns: std::collections::BTreeSet<Vec<u8>>,
    pubsub: Option<PubSubBus>,
}

#[derive(Debug)]
struct PubSubMessage {
    tenant: String,
    channel: Vec<u8>,
    payload: Vec<u8>,
    _queue_permit: OwnedSemaphorePermit,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ClusterPubSubMessage {
    channel: Vec<u8>,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ClusterPubSubBroadcast {
    tenant: String,
    channel: String,
    from: String,
    payload: serde_json::Value,
    commit_ts: u64,
    sequence: u64,
}

#[derive(Debug, Clone)]
struct PubSubBus {
    sender: broadcast::Sender<Arc<PubSubMessage>>,
    counts: PubSubCounts,
    pattern_counts: PubSubCounts,
    total_subscriptions: Arc<AtomicUsize>,
    cluster_started: Arc<Mutex<std::collections::HashSet<String>>>,
}

fn new_pubsub() -> PubSubBus {
    PubSubBus {
        sender: broadcast::channel(4096).0,
        counts: Arc::new(Mutex::new(std::collections::HashMap::new())),
        pattern_counts: Arc::new(Mutex::new(std::collections::HashMap::new())),
        total_subscriptions: Arc::new(AtomicUsize::new(0)),
        cluster_started: Arc::new(Mutex::new(std::collections::HashSet::new())),
    }
}

fn reserve_pubsub_message(
    tenant: String,
    channel: Vec<u8>,
    payload: Vec<u8>,
) -> Option<Arc<PubSubMessage>> {
    let bytes = std::mem::size_of::<PubSubMessage>()
        .checked_add(tenant.len())?
        .checked_add(channel.len())?
        .checked_add(payload.len())?;
    if bytes > RESP_PUBSUB_QUEUE_BUDGET_BYTES {
        return None;
    }
    let units = bytes.div_ceil(RESP_INPUT_BUDGET_UNIT_BYTES).max(1);
    let permit =
        RESP_PUBSUB_QUEUE_BUDGET.clone().try_acquire_many_owned(u32::try_from(units).ok()?).ok()?;
    Some(Arc::new(PubSubMessage { tenant, channel, payload, _queue_permit: permit }))
}

fn reserve_pubsub_egress(
    message: &PubSubMessage,
    extra_bytes: usize,
) -> Option<OwnedSemaphorePermit> {
    let bytes = message
        .channel
        .len()
        .checked_add(message.payload.len())?
        .checked_add(extra_bytes)?
        .checked_add(128)?;
    if bytes > RESP_PUBSUB_EGRESS_BUDGET_BYTES {
        return None;
    }
    let units = bytes.div_ceil(RESP_INPUT_BUDGET_UNIT_BYTES).max(1);
    RESP_PUBSUB_EGRESS_BUDGET.clone().try_acquire_many_owned(u32::try_from(units).ok()?).ok()
}

impl PubSubBus {
    fn subscribe(&self) -> broadcast::Receiver<Arc<PubSubMessage>> {
        self.sender.subscribe()
    }

    // `try_update` is newer than the workspace MSRV (1.89), so keep using the
    // compatible alias while allowing its deprecation on newer stable toolchains.
    #[allow(deprecated)]
    fn reserve_subscription(&self) -> bool {
        self.total_subscriptions
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current < RESP_MAX_GLOBAL_PUBSUB_SUBSCRIPTIONS).then_some(current + 1)
            })
            .is_ok()
    }

    fn add(&self, tenant: &str, channel: &[u8]) -> bool {
        if let Ok(mut counts) = self.counts.lock() {
            let key = (tenant.to_string(), channel.to_vec());
            if let Some(count) = counts.get_mut(&key) {
                if !self.reserve_subscription() {
                    return false;
                }
                *count += 1;
                return true;
            }
            if !self.reserve_subscription() {
                return false;
            }
            counts.insert(key, 1);
            true
        } else {
            false
        }
    }

    fn remove(&self, tenant: &str, channel: &[u8]) {
        if let Ok(mut counts) = self.counts.lock() {
            let key = (tenant.to_string(), channel.to_vec());
            if let Some(count) = counts.get_mut(&key) {
                *count = count.saturating_sub(1);
                self.total_subscriptions.fetch_sub(1, Ordering::Relaxed);
                if *count == 0 {
                    counts.remove(&key);
                }
            }
        }
    }

    fn add_pattern(&self, tenant: &str, pattern: &[u8]) -> bool {
        if let Ok(mut counts) = self.pattern_counts.lock() {
            let key = (tenant.to_string(), pattern.to_vec());
            if let Some(count) = counts.get_mut(&key) {
                if !self.reserve_subscription() {
                    return false;
                }
                *count += 1;
                return true;
            }
            if counts.len() >= RESP_MAX_GLOBAL_PUBSUB_PATTERNS || !self.reserve_subscription() {
                return false;
            }
            counts.insert(key, 1);
            true
        } else {
            false
        }
    }

    fn remove_pattern(&self, tenant: &str, pattern: &[u8]) {
        if let Ok(mut counts) = self.pattern_counts.lock() {
            let key = (tenant.to_string(), pattern.to_vec());
            if let Some(count) = counts.get_mut(&key) {
                *count = count.saturating_sub(1);
                self.total_subscriptions.fetch_sub(1, Ordering::Relaxed);
                if *count == 0 {
                    counts.remove(&key);
                }
            }
        }
    }

    fn publish(&self, tenant: &str, channel: Vec<u8>, payload: Vec<u8>) -> Option<usize> {
        let delivered = self.delivered_count(tenant, &channel);
        let message = reserve_pubsub_message(tenant.to_string(), channel, payload)?;
        let _ = self.sender.send(message);
        Some(delivered)
    }

    fn delivered_count(&self, tenant: &str, channel: &[u8]) -> usize {
        let exact = self
            .counts
            .lock()
            .ok()
            .and_then(|counts| counts.get(&(tenant.to_string(), channel.to_vec())).copied())
            .unwrap_or(0);
        let patterned = self
            .pattern_counts
            .lock()
            .ok()
            .map(|counts| {
                counts
                    .iter()
                    .filter(|((pattern_tenant, pattern), _)| {
                        pattern_tenant == tenant && glob_match(pattern, channel)
                    })
                    .map(|(_, count)| *count)
                    .sum::<usize>()
            })
            .unwrap_or(0);
        exact.saturating_add(patterned)
    }

    fn start_cluster_bridge(&self, realtime: Realtime, tenant: String, database: String) {
        if !self.reserve_cluster_bridge(&tenant) {
            return;
        }
        let topic = resp_pubsub_topic(&database);
        let mut receiver = realtime.broadcast_subscribe(&tenant, &topic);
        let sender = self.sender.clone();
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(message) => {
                        let Ok(message) =
                            serde_json::from_value::<ClusterPubSubMessage>(message.payload.clone())
                        else {
                            continue;
                        };
                        let Some(message) = reserve_pubsub_message(
                            tenant.clone(),
                            message.channel,
                            message.payload,
                        ) else {
                            continue;
                        };
                        let _ = sender.send(message);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    fn reserve_cluster_bridge(&self, tenant: &str) -> bool {
        let Ok(mut started) = self.cluster_started.lock() else {
            return false;
        };
        if started.contains(tenant) || started.len() >= RESP_MAX_CLUSTER_BRIDGES {
            return false;
        }
        started.insert(tenant.to_string())
    }
}

fn resp_pubsub_topic(database: &str) -> String {
    format!("__ryme_resp_pubsub__:{database}")
}

impl Drop for ClientState {
    fn drop(&mut self) {
        if let Some(pubsub) = &self.pubsub {
            for channel in &self.subscriptions {
                pubsub.remove(&self.tenant, channel);
            }
            for pattern in &self.patterns {
                pubsub.remove_pattern(&self.tenant, pattern);
            }
        }
    }
}

impl RespGateway<TxnManager> {
    pub fn new(tenant: String, database: String) -> Self {
        Self {
            manager: TxnManager::new(),
            tenant,
            database,
            branch: String::from("main"),
            authenticator: None,
            realtime: None,
            realtime_replicator: None,
            qos: None,
            metering: None,
            latency: None,
            histogram: None,
            slow_log: None,
            traces: None,
            range_hook: RangeLoadHook::default(),
            pubsub: new_pubsub(),
            scripts: Arc::new(Mutex::new(std::collections::HashMap::new())),
            next_client_id: Arc::new(AtomicU64::new(1)),
            read_only: false,
            read_allowed: true,
            write_allowed: true,
            publish_allowed: true,
            remote_reader: None,
        }
    }

    pub fn with_manager(tenant: String, database: String, manager: TxnManager) -> Self {
        Self {
            manager,
            tenant,
            database,
            branch: String::from("main"),
            authenticator: None,
            realtime: None,
            realtime_replicator: None,
            qos: None,
            metering: None,
            latency: None,
            histogram: None,
            slow_log: None,
            traces: None,
            range_hook: RangeLoadHook::default(),
            pubsub: new_pubsub(),
            scripts: Arc::new(Mutex::new(std::collections::HashMap::new())),
            next_client_id: Arc::new(AtomicU64::new(1)),
            read_only: false,
            read_allowed: true,
            write_allowed: true,
            publish_allowed: true,
            remote_reader: None,
        }
    }
}

impl<B> RespGateway<B>
where
    B: TxnBackend,
{
    pub fn with_backend(tenant: String, database: String, manager: B) -> Self {
        Self {
            manager,
            tenant,
            database,
            branch: String::from("main"),
            authenticator: None,
            realtime: None,
            realtime_replicator: None,
            qos: None,
            metering: None,
            latency: None,
            histogram: None,
            slow_log: None,
            traces: None,
            range_hook: RangeLoadHook::default(),
            pubsub: new_pubsub(),
            scripts: Arc::new(Mutex::new(std::collections::HashMap::new())),
            next_client_id: Arc::new(AtomicU64::new(1)),
            read_only: false,
            read_allowed: true,
            write_allowed: true,
            publish_allowed: true,
            remote_reader: None,
        }
    }

    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    pub fn with_remote_reader(mut self, reader: RemoteReader) -> Self {
        self.remote_reader = Some(reader);
        self
    }

    pub fn with_authenticator<F>(mut self, authenticator: F) -> Self
    where
        F: Fn(&str, &str) -> Result<Principal> + Send + Sync + 'static,
    {
        self.authenticator = Some(Arc::new(authenticator));
        self
    }

    pub fn with_realtime(mut self, realtime: Realtime) -> Self {
        self.realtime = Some(realtime);
        self
    }

    pub fn with_realtime_replicator<F>(mut self, replicator: F) -> Self
    where
        F: Fn(Vec<u8>) + Send + Sync + 'static,
    {
        self.realtime_replicator = Some(Arc::new(replicator));
        self
    }

    fn start_realtime_pubsub(&self) {
        if let Some(realtime) = self.realtime.clone() {
            self.pubsub.start_cluster_bridge(realtime, self.tenant.clone(), self.database.clone());
        }
    }

    pub fn with_branch(mut self, branch: String) -> Self {
        self.branch = branch;
        self
    }

    pub fn with_qos(mut self, qos: Arc<Mutex<QosRegistry>>) -> Self {
        self.qos = Some(qos);
        self
    }

    pub fn with_metering(mut self, metering: Arc<Mutex<MeterRegistry>>) -> Self {
        self.metering = Some(metering);
        self
    }

    pub fn with_observe(
        mut self,
        latency: LatencyWindow,
        histogram: Histogram,
        slow_log: SlowLog,
    ) -> Self {
        self.latency = Some(latency);
        self.histogram = Some(histogram);
        self.slow_log = Some(slow_log);
        self
    }

    pub fn with_traces(mut self, traces: Arc<Mutex<TraceCollector>>) -> Self {
        self.traces = Some(traces);
        self
    }

    pub fn with_range_hook(mut self, hook: RangeLoadHook) -> Self {
        self.range_hook = hook;
        self
    }

    fn note_range_write(&self, key: &RecordKey) {
        self.note_range_key(&key.tenant, &key.table, &key.pk);
    }

    fn note_range_key(&self, tenant: &str, table: &str, pk: &[u8]) {
        if !self.range_hook.is_armed() {
            return;
        }
        self.range_hook.note(tenant, &self.database, table, pk, 1);
    }

    fn record_timing(&self, fingerprint: &str, micros: u64) {
        if let Some(latency) = self.latency.as_ref() {
            latency.observe_micros(micros);
        }
        if let Some(histogram) = self.histogram.as_ref() {
            histogram.record(micros);
        }
        if micros > SLOW_THRESHOLD_MICROS {
            if let Some(slow_log) = self.slow_log.as_ref() {
                slow_log.record(SlowEntry {
                    kind: String::from("resp"),
                    fingerprint: fingerprint.to_string(),
                    table: String::from(KV_TABLE),
                    micros,
                    at_unix: slow_now_secs(),
                });
            }
        }
        if let Some(traces) = self.traces.as_ref() {
            let mut span = TraceSpan::root(String::from("resp"), slow_now_secs());
            span.attr(String::from("command"), fingerprint.to_string());
            span.attr(String::from("table"), String::from(KV_TABLE));
            span.finish(micros);
            if let Ok(mut collector) = traces.lock() {
                collector.push(span);
            }
        }
    }

    fn admit(&self, write: bool, bytes: u64) -> Option<Vec<u8>> {
        let qos = self.qos.as_ref()?;
        let now = qos_now_nanos();
        let admitted = match qos.lock() {
            Ok(mut registry) if write => registry.admit_write(&self.tenant, bytes, now),
            Ok(mut registry) => registry.admit_read(&self.tenant, now),
            Err(_) => Err(RymeError::Internal(String::from("qos lock"))),
        };
        admitted.err().map(|e| encode_error(e.to_string()))
    }

    fn readonly_deny(&self, write: bool) -> Option<Vec<u8>> {
        if write && !self.write_allowed {
            return Some(encode_error(String::from("NOPERM permission denied")));
        }
        if write && self.read_only {
            return Some(encode_error(String::from(
                "READONLY You can't write against a read only replica",
            )));
        }
        None
    }

    fn admit_response(&self, bytes: u64) -> Option<Vec<u8>> {
        let qos = self.qos.as_ref()?;
        let now = qos_now_nanos();
        let denied = match qos.lock() {
            Ok(mut registry) => registry.admit_egress(&self.tenant, bytes, now).err(),
            Err(_) => Some(RymeError::Internal(String::from("qos lock"))),
        };
        denied.map(|e| encode_error(e.to_string()))
    }

    fn observe(&self, write: bool) {
        let Some(metering) = self.metering.as_ref() else { return };
        let metric = if write { Metric::WriteUnit } else { Metric::ReadUnit };
        let event =
            UsageEvent::new(self.tenant.clone(), self.database.clone(), metric, 1, String::new());
        if let Ok(mut registry) = metering.lock() {
            registry.ingest(event);
        }
    }

    fn fresh_client(&self) -> ClientState {
        ClientState {
            name: None,
            id: self.next_client_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            authenticated: self.authenticator.is_none(),
            tenant: self.tenant.clone(),
            resp3: false,
            subscriptions: std::collections::BTreeSet::new(),
            patterns: std::collections::BTreeSet::new(),
            pubsub: Some(self.pubsub.clone()),
        }
    }

    fn authenticate(&mut self, args: &[Vec<u8>], client: &mut ClientState) -> Vec<u8> {
        let Some(authenticator) = self.authenticator.clone() else {
            return encode_error(String::from("Client sent AUTH, but no password is set"));
        };
        let (user, password) = match args {
            [password] => ("default", password.as_slice()),
            [user, password] => match std::str::from_utf8(user) {
                Ok(user) => (user, password.as_slice()),
                Err(_) => return encode_error(String::from("WRONGPASS invalid username")),
            },
            _ => return encode_error(String::from("wrong args")),
        };
        let password = match std::str::from_utf8(password) {
            Ok(password) => password,
            Err(_) => return encode_error(String::from("WRONGPASS invalid password")),
        };
        match authenticator(user, password) {
            Ok(principal) if !principal.tenant.is_empty() => {
                let read_allowed = principal.can_read();
                let write_allowed = principal.can_write();
                let publish_allowed = principal.can_publish();
                let tenant = principal.tenant;
                for channel in &client.subscriptions {
                    self.pubsub.remove(&client.tenant, channel);
                }
                for pattern in &client.patterns {
                    self.pubsub.remove_pattern(&client.tenant, pattern);
                }
                client.subscriptions.clear();
                client.patterns.clear();
                self.tenant = tenant;
                client.tenant = self.tenant.clone();
                self.read_allowed = read_allowed;
                self.write_allowed = write_allowed;
                self.publish_allowed = publish_allowed;
                client.authenticated = true;
                self.start_realtime_pubsub();
                encode_simple("OK")
            }
            Ok(_) | Err(_) => {
                encode_error(String::from("WRONGPASS invalid username-password pair"))
            }
        }
    }

    fn hello(&mut self, command: &RespCommand, client: &mut ClientState) -> Vec<u8> {
        let mut index = 0;
        let mut protocol = 2;
        if let Some(version) = command.args.first() {
            if version.as_slice() != b"2" && version.as_slice() != b"3" {
                return encode_error(String::from("NOPROTO unsupported protocol version"));
            }
            protocol = if version.as_slice() == b"3" { 3 } else { 2 };
            index = 1;
        }
        while index < command.args.len() {
            match command.args[index].to_ascii_uppercase().as_slice() {
                b"AUTH" => {
                    if index + 2 >= command.args.len() {
                        return encode_error(String::from("wrong args"));
                    }
                    let reply = self.authenticate(&command.args[index + 1..index + 3], client);
                    if reply != encode_simple("OK") {
                        return reply;
                    }
                    index += 3;
                }
                b"SETNAME" => {
                    let Some(name) = command.args.get(index + 1) else {
                        return encode_error(String::from("wrong args"));
                    };
                    client.name = Some(name.clone());
                    index += 2;
                }
                _ => return encode_error(String::from("syntax error")),
            }
        }
        if !client.authenticated {
            return encode_error(String::from("NOAUTH Authentication required"));
        }
        client.resp3 = protocol == 3;
        Self::encode_hello(client.id, client.resp3)
    }

    fn encode_hello(id: u64, resp3: bool) -> Vec<u8> {
        if resp3 {
            let mut out = b"%7\r\n".to_vec();
            out.extend_from_slice(b"+server\r\n$6\r\nrymedb\r\n");
            out.extend_from_slice(
                format!(
                    "+version\r\n${}\r\n{}\r\n",
                    env!("CARGO_PKG_VERSION").len(),
                    env!("CARGO_PKG_VERSION")
                )
                .as_bytes(),
            );
            out.extend_from_slice(b"+proto\r\n:3\r\n");
            out.extend_from_slice(format!("+id\r\n:{id}\r\n").as_bytes());
            out.extend_from_slice(b"+mode\r\n$10\r\nstandalone\r\n");
            out.extend_from_slice(b"+role\r\n$6\r\nmaster\r\n");
            out.extend_from_slice(b"+modules\r\n*0\r\n");
            return out;
        }
        let mut out = b"*14\r\n".to_vec();
        for part in [
            "server",
            "rymedb",
            "version",
            env!("CARGO_PKG_VERSION"),
            "proto",
            "2",
            "mode",
            "standalone",
            "role",
            "master",
        ] {
            out.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
        }
        out.extend_from_slice(format!("$2\r\nid\r\n:{id}\r\n").as_bytes());
        out.extend_from_slice(b"$7\r\nmodules\r\n*0\r\n");
        out
    }

    fn client_cmd(&self, command: &RespCommand, client: &mut ClientState) -> Vec<u8> {
        let Some(sub) = command.args.first() else {
            return encode_error(String::from("wrong args"));
        };
        match sub.to_ascii_uppercase().as_slice() {
            b"SETNAME" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                client.name = Some(command.args[1].clone());
                encode_simple("OK")
            }
            b"GETNAME" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match client.name.as_ref() {
                    Some(name) => encode_bulk(name),
                    None => encode_null(),
                }
            }
            b"ID" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                encode_integer(client.id as i64)
            }
            b"SETINFO" => {
                if command.args.len() < 3 || command.args.len().is_multiple_of(2) {
                    return encode_error(String::from("wrong args"));
                }
                encode_simple("OK")
            }
            b"HELP" => encode_raw_array(vec![
                encode_bulk(b"CLIENT <subcommand> [<arg> [value] ...]"),
                encode_bulk(b"SETNAME <name>"),
                encode_bulk(b"GETNAME"),
                encode_bulk(b"ID"),
                encode_bulk(b"SETINFO <section> <value>"),
                encode_bulk(b"HELP"),
            ]),
            _ => encode_error(format!(
                "unknown subcommand '{}'. Try CLIENT HELP.",
                String::from_utf8_lossy(sub)
            )),
        }
    }

    fn subscribe(&self, channels: &[Vec<u8>], client: &mut ClientState) -> Vec<u8> {
        if channels.is_empty() {
            return encode_error(String::from("wrong args"));
        }
        if let Some(error) =
            validate_pubsub_subscriptions(&client.subscriptions, client.patterns.len(), channels)
        {
            return encode_error(error.to_string());
        }
        let mut replies = Vec::with_capacity(channels.len());
        let mut added: Vec<Vec<u8>> = Vec::new();
        for channel in channels {
            if !client.subscriptions.contains(channel) {
                if !self.pubsub.add(&client.tenant, channel) {
                    for added_channel in added {
                        client.subscriptions.remove(&added_channel);
                        self.pubsub.remove(&client.tenant, &added_channel);
                    }
                    return encode_error(String::from("global pubsub subscription limit exceeded"));
                }
                client.subscriptions.insert(channel.clone());
                added.push(channel.clone());
            }
            replies.push(encode_raw_array(vec![
                encode_bulk(b"subscribe"),
                encode_bulk(channel),
                encode_integer(subscription_count(client) as i64),
            ]));
        }
        replies.into_iter().flatten().collect()
    }

    fn unsubscribe(&self, channels: &[Vec<u8>], client: &mut ClientState) -> Vec<u8> {
        let channels: Vec<Vec<u8>> = if channels.is_empty() {
            client.subscriptions.iter().cloned().collect()
        } else {
            channels.to_vec()
        };
        if channels.is_empty() {
            return encode_raw_array(vec![
                encode_bulk(b"unsubscribe"),
                encode_null(),
                encode_integer(0),
            ]);
        }
        let mut replies = Vec::with_capacity(channels.len());
        for channel in channels {
            if client.subscriptions.remove(&channel) {
                self.pubsub.remove(&client.tenant, &channel);
            }
            replies.push(encode_raw_array(vec![
                encode_bulk(b"unsubscribe"),
                encode_bulk(&channel),
                encode_integer(subscription_count(client) as i64),
            ]));
        }
        replies.into_iter().flatten().collect()
    }

    fn psubscribe(&self, patterns: &[Vec<u8>], client: &mut ClientState) -> Vec<u8> {
        if patterns.is_empty() {
            return encode_error(String::from("wrong args"));
        }
        if let Some(error) =
            validate_pubsub_subscriptions(&client.patterns, client.subscriptions.len(), patterns)
        {
            return encode_error(error.to_string());
        }
        let mut replies = Vec::with_capacity(patterns.len());
        let mut added: Vec<Vec<u8>> = Vec::new();
        for pattern in patterns {
            if !client.patterns.contains(pattern) {
                if !self.pubsub.add_pattern(&client.tenant, pattern) {
                    for added_pattern in added {
                        client.patterns.remove(&added_pattern);
                        self.pubsub.remove_pattern(&client.tenant, &added_pattern);
                    }
                    return encode_error(String::from("global pubsub subscription limit exceeded"));
                }
                client.patterns.insert(pattern.clone());
                added.push(pattern.clone());
            }
            replies.push(encode_raw_array(vec![
                encode_bulk(b"psubscribe"),
                encode_bulk(pattern),
                encode_integer(subscription_count(client) as i64),
            ]));
        }
        replies.into_iter().flatten().collect()
    }

    fn punsubscribe(&self, patterns: &[Vec<u8>], client: &mut ClientState) -> Vec<u8> {
        let patterns: Vec<Vec<u8>> = if patterns.is_empty() {
            client.patterns.iter().cloned().collect()
        } else {
            patterns.to_vec()
        };
        if patterns.is_empty() {
            return encode_raw_array(vec![
                encode_bulk(b"punsubscribe"),
                encode_null(),
                encode_integer(subscription_count(client) as i64),
            ]);
        }
        let mut replies = Vec::with_capacity(patterns.len());
        for pattern in patterns {
            if client.patterns.remove(&pattern) {
                self.pubsub.remove_pattern(&client.tenant, &pattern);
            }
            replies.push(encode_raw_array(vec![
                encode_bulk(b"punsubscribe"),
                encode_bulk(&pattern),
                encode_integer(subscription_count(client) as i64),
            ]));
        }
        replies.into_iter().flatten().collect()
    }

    fn pubsub_command(&self, args: &[Vec<u8>]) -> Vec<u8> {
        let Some(subcommand) = args.first() else {
            return encode_error(String::from("wrong args"));
        };
        match subcommand.to_ascii_uppercase().as_slice() {
            b"NUMSUB" => {
                let Ok(counts) = self.pubsub.counts.lock() else {
                    return encode_error(String::from("pubsub lock"));
                };
                let mut reply = Vec::with_capacity((args.len().saturating_sub(1)) * 2);
                for channel in &args[1..] {
                    reply.push(encode_bulk(channel));
                    reply.push(encode_integer(
                        counts.get(&(self.tenant.clone(), channel.clone())).copied().unwrap_or(0)
                            as i64,
                    ));
                }
                encode_raw_array(reply)
            }
            b"NUMPAT" if args.len() == 1 => {
                let count = self
                    .pubsub
                    .pattern_counts
                    .lock()
                    .ok()
                    .map(|counts| {
                        counts
                            .iter()
                            .filter(|((tenant, _), _)| tenant == &self.tenant)
                            .map(|(_, count)| *count)
                            .sum::<usize>()
                    })
                    .unwrap_or(0);
                encode_integer(count as i64)
            }
            b"CHANNELS" if args.len() <= 2 => {
                let Ok(counts) = self.pubsub.counts.lock() else {
                    return encode_error(String::from("pubsub lock"));
                };
                let pattern = args.get(1).map(Vec::as_slice).unwrap_or(b"*");
                encode_raw_array(
                    counts
                        .iter()
                        .filter(|((tenant, channel), count)| {
                            tenant == &self.tenant && **count > 0 && glob_match(pattern, channel)
                        })
                        .map(|((_, channel), _)| encode_bulk(channel))
                        .collect(),
                )
            }
            _ => encode_error(String::from("syntax error")),
        }
    }

    fn script_command(&self, args: &[Vec<u8>]) -> Vec<u8> {
        let Some(subcommand) = args.first() else {
            return encode_error(String::from("wrong args"));
        };
        match subcommand.to_ascii_uppercase().as_slice() {
            b"LOAD" if args.len() == 2 => {
                let script = args[1].clone();
                if script.len() > RESP_SCRIPT_MAX_BYTES {
                    return encode_error(String::from("script exceeds the 1 MiB limit"));
                }
                let sha = script_sha1(&script);
                if let Ok(mut scripts) = self.scripts.lock() {
                    let key = (self.tenant.clone(), sha.clone());
                    if !script_cache_has_capacity(&scripts, &key, script.len()) {
                        return encode_error(String::from("script cache limit"));
                    }
                    scripts.insert(key, script);
                } else {
                    return encode_error(String::from("script cache lock"));
                }
                encode_bulk(sha.as_bytes())
            }
            b"EXISTS" if args.len() > 1 => {
                let Ok(scripts) = self.scripts.lock() else {
                    return encode_error(String::from("script cache lock"));
                };
                encode_raw_array(
                    args[1..]
                        .iter()
                        .map(|sha| {
                            let exists = std::str::from_utf8(sha).ok().is_some_and(|sha| {
                                scripts.contains_key(&(self.tenant.clone(), sha.to_string()))
                            });
                            encode_integer(i64::from(exists))
                        })
                        .collect(),
                )
            }
            b"FLUSH"
                if args.len() == 1
                    || (args.len() == 2
                        && (args[1].eq_ignore_ascii_case(b"SYNC")
                            || args[1].eq_ignore_ascii_case(b"ASYNC"))) =>
            {
                if let Ok(mut scripts) = self.scripts.lock() {
                    scripts.retain(|(tenant, _), _| tenant != &self.tenant);
                    encode_simple("OK")
                } else {
                    encode_error(String::from("script cache lock"))
                }
            }
            _ => encode_error(String::from("syntax error")),
        }
    }

    fn eval_command(&self, txn: &mut Transaction, command: &str, args: &[Vec<u8>]) -> Vec<u8> {
        if args.len() < 2 {
            return encode_error(String::from("wrong args"));
        }
        let script = if command == "EVAL" {
            args[0].clone()
        } else {
            let Ok(sha) = std::str::from_utf8(&args[0]) else {
                return encode_noscript();
            };
            let Ok(scripts) = self.scripts.lock() else {
                return encode_error(String::from("script cache lock"));
            };
            let Some(script) = scripts.get(&(self.tenant.clone(), sha.to_string())) else {
                return encode_noscript();
            };
            script.clone()
        };
        let key_count = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
        {
            Some(count) if count <= args.len().saturating_sub(2) => count,
            _ => return encode_error(String::from("value is not an integer or out of range")),
        };
        if script.len() > RESP_SCRIPT_MAX_BYTES {
            return encode_error(String::from("script exceeds the 1 MiB limit"));
        }
        let keys = args[2..2 + key_count].to_vec();
        let argv = args[2 + key_count..].to_vec();
        if command == "EVAL" {
            let sha = script_sha1(&script);
            if let Ok(mut scripts) = self.scripts.lock() {
                let key = (self.tenant.clone(), sha);
                if script_cache_has_capacity(&scripts, &key, script.len()) {
                    scripts.insert(key, script.clone());
                }
            }
        }
        match self.run_lua_script(txn, &script, &keys, &argv) {
            Ok(reply) => reply,
            Err(error) => encode_error(format!("Error running script: {error}")),
        }
    }

    fn run_lua_script(
        &self,
        txn: &mut Transaction,
        script: &[u8],
        keys: &[Vec<u8>],
        argv: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        let script =
            std::str::from_utf8(script).map_err(|_| String::from("script is not UTF-8"))?;
        let lua = Lua::new_with(
            StdLib::COROUTINE | StdLib::TABLE | StdLib::STRING | StdLib::UTF8 | StdLib::MATH,
            LuaOptions::default(),
        )
        .map_err(|error| error.to_string())?;
        lua.set_memory_limit(8 * 1024 * 1024).map_err(|error| error.to_string())?;
        let instruction_counter = Arc::new(AtomicUsize::new(0));
        lua.set_hook(HookTriggers::new().every_nth_instruction(1_000), move |_, _| {
            if instruction_counter.fetch_add(1_000, Ordering::Relaxed) >= 100_000 {
                return Err(mlua::Error::RuntimeError(String::from(
                    "script exceeded the instruction limit",
                )));
            }
            Ok(VmState::Continue)
        });
        let transaction = std::rc::Rc::new(std::cell::RefCell::new(txn));
        let result = lua.scope(|scope| {
            let call_transaction = std::rc::Rc::clone(&transaction);
            let call = scope.create_function_mut(move |lua, values: MultiValue| {
                let mut values = values.into_iter();
                let command =
                    values.next().map(lua_value_bytes).transpose()?.ok_or_else(|| {
                        mlua::Error::RuntimeError(String::from("missing command"))
                    })?;
                let name = std::str::from_utf8(&command)
                    .map_err(|_| mlua::Error::RuntimeError(String::from("command is not UTF-8")))?
                    .to_ascii_uppercase();
                let args = values.map(lua_value_bytes).collect::<mlua::Result<Vec<_>>>()?;
                let command = RespCommand { name, args };
                self.authorize_script_command(&command).map_err(mlua::Error::RuntimeError)?;
                let reply = self.dispatch_in(&mut call_transaction.borrow_mut(), command);
                parse_lua_resp(&reply).map_err(mlua::Error::RuntimeError)?.into_lua(lua)
            })?;
            let pcall_transaction = std::rc::Rc::clone(&transaction);
            let pcall = scope.create_function_mut(move |lua, values: MultiValue| {
                let mut values = values.into_iter();
                let command =
                    values.next().map(lua_value_bytes).transpose()?.ok_or_else(|| {
                        mlua::Error::RuntimeError(String::from("missing command"))
                    })?;
                let name = std::str::from_utf8(&command)
                    .map_err(|_| mlua::Error::RuntimeError(String::from("command is not UTF-8")))?
                    .to_ascii_uppercase();
                let args = values.map(lua_value_bytes).collect::<mlua::Result<Vec<_>>>()?;
                let command = RespCommand { name, args };
                self.authorize_script_command(&command).map_err(mlua::Error::RuntimeError)?;
                let reply = self.dispatch_in(&mut pcall_transaction.borrow_mut(), command);
                parse_lua_resp(&reply).map_err(mlua::Error::RuntimeError)?.into_lua_pcall(lua)
            })?;
            let redis = lua.create_table()?;
            redis.set("call", call)?;
            redis.set("pcall", pcall)?;
            lua.globals().set("redis", redis)?;
            lua.globals().set("KEYS", lua_bytes_table(&lua, keys)?)?;
            lua.globals().set("ARGV", lua_bytes_table(&lua, argv)?)?;
            for name in ["collectgarbage", "dofile", "loadfile", "require"] {
                lua.globals().set(name, LuaValue::Nil)?;
            }
            lua.load(script).set_name("rymedb-eval").eval::<LuaValue>()
        });
        let value = result.map_err(|error| error.to_string())?;
        lua_value_resp(&value)
    }

    fn authorize_script_command(&self, command: &RespCommand) -> std::result::Result<(), String> {
        if matches!(
            command.name.as_str(),
            "EVAL" | "EVALSHA" | "SCRIPT" | "SUBSCRIBE" | "PSUBSCRIBE"
        ) {
            return Err(String::from("command is not allowed in scripts"));
        }
        if command.name == "PUBLISH" {
            if !self.publish_allowed {
                return Err(String::from("NOPERM permission denied"));
            }
        } else if !self.write_allowed && !is_read_only_command(command) {
            return Err(String::from("NOPERM permission denied"));
        }
        Ok(())
    }

    fn cdc_pending(&self, txn: &Transaction) -> Result<CdcPending> {
        let Some(_realtime) = self.realtime.as_ref() else { return Ok(Vec::new()) };
        let mut out = Vec::new();
        for (key, staged) in txn.writes() {
            let mut before_txn = self.manager.begin();
            before_txn.restamp(txn.read_ts);
            let before = self.manager.get(&mut before_txn, key)?;
            let op = match &staged.value {
                None => Operation::Delete,
                Some(_) => match txn.observed_existed(key) {
                    Some(true) => Operation::Update,
                    Some(false) => Operation::Insert,
                    None if before.is_some() => Operation::Update,
                    None => Operation::Insert,
                },
            };
            out.push((key.clone(), op, before, staged.value.clone()));
        }
        Ok(out)
    }

    fn cdc_emit(&self, pending: CdcPending, commit_ts: u64) {
        let Some(realtime) = self.realtime.as_ref() else { return };
        for (key, op, before, after) in pending {
            let _ = realtime.publish(NewChange {
                tenant: key.tenant.clone(),
                database: key.database.clone(),
                branch: self.branch.clone(),
                table: key.table.clone(),
                op,
                pk: key.pk.clone(),
                before,
                after,
                commit_ts,
                tx_id: commit_ts,
            });
        }
    }

    pub async fn serve(&self, listener: TcpListener) -> Result<()> {
        self.serve_limited(listener, DEFAULT_MAX_CONNECTIONS).await
    }

    pub async fn serve_limited(&self, listener: TcpListener, max_connections: usize) -> Result<()> {
        self.start_realtime_pubsub();
        let limit = Arc::new(tokio::sync::Semaphore::new(max_connections.max(1)));
        loop {
            let (socket, _) = listener.accept().await.map_err(|e| RymeError::Io(e.to_string()))?;
            let Ok(permit) = limit.clone().try_acquire_owned() else {
                let mut socket = socket;
                let _ = socket.write_all(b"-ERR overloaded\r\n").await;
                continue;
            };
            socket.set_nodelay(true).map_err(|e| RymeError::Io(e.to_string()))?;
            let service = self.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let _ = service.handle(socket).await;
            });
        }
    }

    pub async fn serve_tls(
        &self,
        listener: TcpListener,
        max_connections: usize,
        acceptor: ryme_wire_native::TlsAcceptor,
    ) -> Result<()>
    where
        B: Send + Sync + 'static,
    {
        self.start_realtime_pubsub();
        let limit = Arc::new(tokio::sync::Semaphore::new(max_connections.max(1)));
        let acceptor = acceptor.acceptor();
        loop {
            let (socket, _) = listener.accept().await.map_err(|e| RymeError::Io(e.to_string()))?;
            let Ok(permit) = limit.clone().try_acquire_owned() else {
                continue;
            };
            socket.set_nodelay(true).map_err(|e| RymeError::Io(e.to_string()))?;
            let service = self.clone();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let Ok(Ok(tls)) = tokio::time::timeout(
                    ryme_wire_native::TLS_HANDSHAKE_TIMEOUT,
                    acceptor.accept(socket),
                )
                .await
                else {
                    return;
                };
                let _ = service.handle(tls).await;
            });
        }
    }

    async fn handle<S>(&self, socket: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        // Keep parsing and writing decoupled for a small bounded batch.  This is
        // especially important for pipelined clients: one write per command
        // turns a single read into a syscall storm, while an unbounded reply
        // buffer would let a client consume arbitrary memory.
        const WRITE_BATCH_BYTES: usize = 64 * 1024;
        let mut input_permits: Vec<OwnedSemaphorePermit> = vec![RESP_INPUT_BUDGET
            .clone()
            .try_acquire_many_owned(1)
            .map_err(|_| RymeError::Overload(String::from("input memory budget")))?];
        let (mut reader, mut writer) = tokio::io::split(socket);
        let mut service = self.clone();
        let mut buffer = vec![0u8; RESP_READ_CHUNK_BYTES];
        let mut pending: Vec<u8> = Vec::new();
        let mut pending_reserved_units = 0usize;
        let mut multi: Option<Multi> = None;
        let mut client = service.fresh_client();
        let mut pubsub = service.pubsub.subscribe();
        loop {
            let read = tokio::select! {
                read = reader.read(&mut buffer) => {
                    Some(read.map_err(|e| RymeError::Io(e.to_string()))?)
                }
                message = pubsub.recv(),
                    if !client.subscriptions.is_empty() || !client.patterns.is_empty() =>
                {
                    if let Ok(message) = message {
                        if message.tenant == client.tenant
                            && client.subscriptions.contains(&message.channel)
                        {
                            if let Some(_permit) = reserve_pubsub_egress(&message, 0) {
                                let reply = encode_raw_array(vec![
                                    encode_bulk(b"message"),
                                    encode_bulk(&message.channel),
                                    encode_bulk(&message.payload),
                                ]);
                                let reply = if client.resp3 {
                                    resp3_push(&reply).unwrap_or(reply)
                                } else {
                                    reply
                                };
                                writer.write_all(&reply).await.map_err(|e| RymeError::Io(e.to_string()))?;
                            }
                        }
                        for pattern in &client.patterns {
                            if message.tenant == client.tenant
                                && glob_match(pattern, &message.channel)
                            {
                                if let Some(_permit) = reserve_pubsub_egress(&message, pattern.len()) {
                                    let reply = encode_raw_array(vec![
                                        encode_bulk(b"pmessage"),
                                        encode_bulk(pattern),
                                        encode_bulk(&message.channel),
                                        encode_bulk(&message.payload),
                                    ]);
                                    let reply = if client.resp3 {
                                        resp3_push(&reply).unwrap_or(reply)
                                    } else {
                                        reply
                                    };
                                    writer.write_all(&reply).await.map_err(|e| RymeError::Io(e.to_string()))?;
                                }
                            }
                        }
                    }
                    None
                }
            };
            let Some(read) = read else { continue };
            if read == 0 {
                return Ok(());
            }
            let next_len = pending
                .len()
                .checked_add(read)
                .ok_or_else(|| RymeError::Overload(String::from("request")))?;
            if next_len > RESP_MAX_FRAME_BYTES + RESP_READ_CHUNK_BYTES {
                return Err(RymeError::Overload(String::from("request")));
            }
            let required_units = next_len.div_ceil(RESP_INPUT_BUDGET_UNIT_BYTES);
            let additional_units = required_units.saturating_sub(pending_reserved_units);
            if additional_units > 0 {
                let permit = RESP_INPUT_BUDGET
                    .clone()
                    .try_acquire_many_owned(additional_units as u32)
                    .map_err(|_| RymeError::Overload(String::from("input memory budget")))?;
                input_permits.push(permit);
            }
            pending
                .try_reserve_exact(read)
                .map_err(|_| RymeError::Overload(String::from("request memory")))?;
            pending_reserved_units = required_units;
            pending.extend_from_slice(&buffer[..read]);
            let mut consumed = 0usize;
            let mut replies = Vec::with_capacity(WRITE_BATCH_BYTES);
            while let Some((command, command_bytes)) = decode_command(&pending[consumed..])? {
                consumed += command_bytes;
                if !replies.is_empty() && Self::may_block(&command, &multi) {
                    writer.write_all(&replies).await.map_err(|e| RymeError::Io(e.to_string()))?;
                    replies.clear();
                }
                let is_pubsub_command = matches!(
                    command.name.as_str(),
                    "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE"
                );
                let reply = service.dispatch_conn(command, &mut multi, &mut client).await;
                let reply = if client.resp3 {
                    if is_pubsub_command {
                        resp3_pubsub_replies(&reply).unwrap_or(reply)
                    } else {
                        resp3_replies(&reply).unwrap_or(reply)
                    }
                } else {
                    reply
                };
                let reply = match service.admit_response(reply.len() as u64) {
                    Some(denied) => denied,
                    None => reply,
                };
                replies.extend_from_slice(&reply);
                if replies.len() >= WRITE_BATCH_BYTES {
                    writer.write_all(&replies).await.map_err(|e| RymeError::Io(e.to_string()))?;
                    replies.clear();
                }
            }
            // Compact once after the whole parse pass.  Draining per command
            // makes large pipelines repeatedly memmove the unread suffix.
            if consumed > 0 {
                pending.drain(..consumed);
            }
            if !replies.is_empty() {
                writer.write_all(&replies).await.map_err(|e| RymeError::Io(e.to_string()))?;
            }
            if pending.len() > RESP_MAX_FRAME_BYTES {
                return Err(RymeError::Overload(String::from("request")));
            }
        }
    }

    fn may_block(command: &RespCommand, multi: &Option<Multi>) -> bool {
        if multi.is_some() {
            return false;
        }
        match command.name.as_str() {
            "BLPOP" | "BRPOP" | "BLMOVE" => true,
            "XREAD" => Self::xread_waits(&command.args),
            "XREADGROUP" => Self::xread_waits(&command.args),
            _ => false,
        }
    }

    async fn dispatch_conn(
        &mut self,
        command: RespCommand,
        multi: &mut Option<Multi>,
        client: &mut ClientState,
    ) -> Vec<u8> {
        if !client.authenticated && !matches!(command.name.as_str(), "AUTH" | "HELLO" | "PING") {
            return encode_error(String::from("NOAUTH Authentication required"));
        }
        if client.authenticated
            && !self.read_allowed
            && !matches!(command.name.as_str(), "AUTH" | "HELLO" | "PING" | "PUBLISH")
        {
            return encode_error(String::from("NOPERM permission denied"));
        }
        if command.name == "PUBLISH" && !self.publish_allowed {
            return encode_error(String::from("NOPERM permission denied"));
        }
        match command.name.as_str() {
            "HELLO" => {
                let start = std::time::Instant::now();
                let bytes = Self::command_bytes(&command);
                if let Some(denied) = self.admit(false, bytes) {
                    self.record_timing("HELLO", start.elapsed().as_micros() as u64);
                    return denied;
                }
                let reply = self.hello(&command, client);
                self.observe(false);
                self.record_timing("HELLO", start.elapsed().as_micros() as u64);
                reply
            }
            "AUTH" => self.authenticate(&command.args, client),
            "CLIENT" => {
                let start = std::time::Instant::now();
                let bytes = Self::command_bytes(&command);
                if let Some(denied) = self.admit(false, bytes) {
                    self.record_timing("CLIENT", start.elapsed().as_micros() as u64);
                    return denied;
                }
                let reply = self.client_cmd(&command, client);
                self.observe(false);
                self.record_timing("CLIENT", start.elapsed().as_micros() as u64);
                reply
            }
            "SUBSCRIBE" => self.subscribe(&command.args, client),
            "UNSUBSCRIBE" => self.unsubscribe(&command.args, client),
            "PSUBSCRIBE" => self.psubscribe(&command.args, client),
            "PUNSUBSCRIBE" => self.punsubscribe(&command.args, client),
            "PUBSUB" => self.pubsub_command(&command.args),
            "MULTI" => {
                if multi.is_some() {
                    return encode_error(String::from("MULTI calls can not be nested"));
                }
                *multi = Some(Multi::default());
                encode_simple("OK")
            }
            "EXEC" => {
                let start = std::time::Instant::now();
                let Some(state) = multi.take() else {
                    return encode_error(String::from("EXEC without MULTI"));
                };
                if state.dirty {
                    return encode_exec_abort();
                }
                let mut bytes = 0u64;
                for queued in &state.queue {
                    bytes = bytes.saturating_add(Self::command_bytes(queued));
                }
                let retry_conflicts = state.queue.iter().all(retries_conflict_safely);
                let mut retries = 0;
                let mut admitted = false;
                loop {
                    let mut txn = self.manager.begin();
                    if state.queue.iter().all(is_read_only_command) {
                        if let Err(error) = txn.set_read_only() {
                            self.record_timing("EXEC", start.elapsed().as_micros() as u64);
                            return encode_error(error.to_string());
                        }
                    }
                    let mut replies = Vec::with_capacity(state.queue.len());
                    let mut reply_bytes = 0usize;
                    for queued in &state.queue {
                        let reply = self.dispatch_in(&mut txn, queued.clone());
                        let Some(next_reply_bytes) = reply_bytes
                            .checked_add(reply.len())
                            .filter(|bytes| *bytes <= RESP_MAX_EXEC_REPLY_BYTES)
                        else {
                            self.record_timing("EXEC", start.elapsed().as_micros() as u64);
                            return encode_error(String::from(
                                "EXEC reply exceeds the 16 MiB limit; transaction aborted",
                            ));
                        };
                        reply_bytes = next_reply_bytes;
                        replies.push(reply);
                    }
                    if let Err(error) = txn.ensure_healthy() {
                        self.record_timing("EXEC", start.elapsed().as_micros() as u64);
                        return encode_error(error.to_string());
                    }
                    let write = !txn.writes().is_empty();
                    if !admitted {
                        if let Some(denied) = self.readonly_deny(write) {
                            self.record_timing("EXEC", start.elapsed().as_micros() as u64);
                            return denied;
                        }
                        if let Some(denied) = self.admit(write, bytes) {
                            self.record_timing("EXEC", start.elapsed().as_micros() as u64);
                            return denied;
                        }
                        admitted = true;
                    }
                    let pending = match self.cdc_pending(&txn) {
                        Ok(pending) => pending,
                        Err(error) => return encode_error(error.to_string()),
                    };
                    let range_keys: Vec<RecordKey> = if self.range_hook.is_armed() {
                        txn.writes().keys().cloned().collect()
                    } else {
                        Vec::new()
                    };
                    match self.manager.commit(txn).await {
                        Ok(commit_ts) => {
                            for key in &range_keys {
                                self.note_range_write(key);
                            }
                            self.cdc_emit(pending, commit_ts);
                            self.observe(write);
                            self.record_timing("EXEC", start.elapsed().as_micros() as u64);
                            return encode_raw_array(replies);
                        }
                        Err(RymeError::Conflict(_))
                            if retry_conflicts && retries < MAX_COMMAND_CONFLICT_RETRIES =>
                        {
                            retries += 1;
                            tokio::task::yield_now().await;
                        }
                        Err(error) => {
                            let reply = encode_error(error.to_string());
                            self.record_timing("EXEC", start.elapsed().as_micros() as u64);
                            return reply;
                        }
                    }
                }
            }
            "DISCARD" => {
                if multi.take().is_none() {
                    return encode_error(String::from("DISCARD without MULTI"));
                }
                encode_simple("OK")
            }
            _ => {
                if let Some(state) = multi.as_mut() {
                    if !is_known(&command.name) {
                        state.dirty = true;
                        return encode_error(String::from("unknown command"));
                    }
                    if state.queue.len() >= RESP_MULTI_MAX_QUEUE_COMMANDS {
                        state.dirty = true;
                        return encode_error(String::from("MULTI command limit exceeded"));
                    }
                    let Some(command_bytes) = Self::multi_command_memory_bytes(&command) else {
                        state.dirty = true;
                        return encode_error(String::from("MULTI queue memory limit exceeded"));
                    };
                    let Some(next_bytes) = state
                        .queue_bytes
                        .checked_add(command_bytes)
                        .filter(|bytes| *bytes <= RESP_MULTI_MAX_QUEUE_BYTES)
                    else {
                        state.dirty = true;
                        return encode_error(String::from("MULTI queue memory limit exceeded"));
                    };
                    let required_units = next_bytes.div_ceil(RESP_INPUT_BUDGET_UNIT_BYTES);
                    let additional_units = required_units.saturating_sub(state.reserved_units);
                    if additional_units > 0 && state.queue_permits.try_reserve(1).is_err() {
                        state.dirty = true;
                        return encode_error(String::from("MULTI queue memory limit exceeded"));
                    }
                    if state.queue.try_reserve(1).is_err() {
                        state.dirty = true;
                        return encode_error(String::from("MULTI queue memory limit exceeded"));
                    }
                    let permit = if additional_units > 0 {
                        match RESP_MULTI_QUEUE_BUDGET
                            .clone()
                            .try_acquire_many_owned(additional_units as u32)
                        {
                            Ok(permit) => Some(permit),
                            Err(_) => {
                                state.dirty = true;
                                return encode_error(String::from(
                                    "MULTI queue memory limit exceeded",
                                ));
                            }
                        }
                    } else {
                        None
                    };
                    if let Some(permit) = permit {
                        state.queue_permits.push(permit);
                    }
                    state.queue_bytes = next_bytes;
                    state.reserved_units = required_units;
                    state.queue.push(command);
                    return encode_simple("QUEUED");
                }
                match command.name.as_str() {
                    "BLPOP" => self.blocking_pop(command.args, true).await,
                    "BRPOP" => self.blocking_pop(command.args, false).await,
                    "BLMOVE" => self.blocking_move(command.args).await,
                    "XREAD" if Self::xread_waits(&command.args) => {
                        self.xread_blocking(command.args).await
                    }
                    "XREADGROUP" if Self::xread_waits(&command.args) => {
                        self.xreadgroup_blocking(command.args).await
                    }
                    _ => self.dispatch(command).await,
                }
            }
        }
    }

    async fn wait_for_commit(
        waiter: &mut tokio::sync::watch::Receiver<u64>,
        remaining: Option<std::time::Duration>,
    ) {
        match remaining {
            Some(limit) => {
                let _ = tokio::time::timeout(limit, waiter.changed()).await;
            }
            None => {
                let _ = waiter.changed().await;
            }
        }
    }

    fn blocking_timeout(
        args: &[Vec<u8>],
    ) -> std::result::Result<Option<std::time::Instant>, String> {
        let raw = args.last().ok_or_else(|| String::from("wrong args"))?;
        let text = std::str::from_utf8(raw)
            .map_err(|_| String::from("timeout is not a float or out of range"))?;
        let secs = text
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite() && *v >= 0.0)
            .ok_or_else(|| String::from("timeout is not a float or out of range"))?;
        if secs == 0.0 {
            return Ok(None);
        }
        std::time::Instant::now()
            .checked_add(std::time::Duration::from_secs_f64(secs))
            .map(Some)
            .ok_or_else(|| String::from("timeout is not a float or out of range"))
    }

    async fn blocking_pop(&self, args: Vec<Vec<u8>>, front: bool) -> Vec<u8> {
        if args.len() < 2 {
            return encode_error(String::from("wrong args"));
        }
        let deadline = match Self::blocking_timeout(&args) {
            Ok(deadline) => deadline,
            Err(e) => return encode_error(e),
        };
        let keys = &args[..args.len() - 1];
        let mut waiter = ryme_txn::subscribe_commits();
        loop {
            let mut txn = self.manager.begin();
            let mut hit: Option<(Vec<u8>, String)> = None;
            let mut failed: Option<Vec<u8>> = None;
            for key in keys {
                match self.read_list(&mut txn, key) {
                    Ok(Some(list)) if !list.is_empty() => {
                        let mut rest = list;
                        let value =
                            if front { rest.remove(0) } else { rest.pop().unwrap_or_default() };
                        match self.write_list(&mut txn, key, &rest) {
                            Ok(()) => {
                                hit = Some((key.clone(), value));
                                break;
                            }
                            Err(e) => {
                                failed = Some(encode_error(e));
                                break;
                            }
                        }
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        failed = Some(encode_error(e));
                        break;
                    }
                }
            }
            if let Some(reply) = failed {
                return reply;
            }
            if let Some((key, value)) = hit {
                let bytes = (key.len() + value.len()) as u64;
                if let Some(denied) = self.readonly_deny(true) {
                    return denied;
                }
                if let Some(denied) = self.admit(true, bytes) {
                    return denied;
                }
                let pending = match self.cdc_pending(&txn) {
                    Ok(pending) => pending,
                    Err(error) => return encode_error(error.to_string()),
                };
                match self.manager.commit(txn).await {
                    Ok(commit_ts) => {
                        self.note_range_key(&self.tenant, KV_TABLE, &key);
                        self.cdc_emit(pending, commit_ts);
                        self.observe(true);
                        let mut out = format!("*2\r\n${}\r\n", key.len()).into_bytes();
                        out.extend_from_slice(&key);
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(
                            format!("${}\r\n{value}\r\n", value.len()).as_bytes(),
                        );
                        return out;
                    }
                    Err(e) => return encode_error(e.to_string()),
                }
            }
            drop(txn);
            if deadline.is_some_and(|end| std::time::Instant::now() >= end) {
                return encode_nil_array();
            }
            let remaining =
                deadline.map(|end| end.saturating_duration_since(std::time::Instant::now()));
            Self::wait_for_commit(&mut waiter, remaining).await;
        }
    }

    async fn blocking_move(&self, args: Vec<Vec<u8>>) -> Vec<u8> {
        if args.len() != 5 {
            return encode_error(String::from("wrong args"));
        }
        let from_front = match args[2].as_slice() {
            b"LEFT" => true,
            b"RIGHT" => false,
            _ => return encode_error(String::from("syntax")),
        };
        let to_front = match args[3].as_slice() {
            b"LEFT" => true,
            b"RIGHT" => false,
            _ => return encode_error(String::from("syntax")),
        };
        let deadline = match Self::blocking_timeout(&[args[4].clone()]) {
            Ok(deadline) => deadline,
            Err(e) => return encode_error(e),
        };
        let mut waiter = ryme_txn::subscribe_commits();
        loop {
            let mut txn = self.manager.begin();
            match self.read_list(&mut txn, &args[0]) {
                Ok(Some(list)) if !list.is_empty() => {
                    let mut source = list;
                    let value = if from_front {
                        source.remove(0)
                    } else {
                        source.pop().unwrap_or_default()
                    };
                    let mut target = match self.read_list(&mut txn, &args[1]) {
                        Ok(target) => target.unwrap_or_default(),
                        Err(error) => return encode_error(error),
                    };
                    if to_front {
                        target.insert(0, value.clone());
                    } else {
                        target.push(value.clone());
                    }
                    let stored = match self.write_list(&mut txn, &args[0], &source) {
                        Ok(()) => self.write_list(&mut txn, &args[1], &target),
                        Err(e) => Err(e),
                    };
                    match stored {
                        Ok(()) => {
                            let bytes = (args[0].len() + args[1].len() + value.len()) as u64;
                            if let Some(denied) = self.readonly_deny(true) {
                                return denied;
                            }
                            if let Some(denied) = self.admit(true, bytes) {
                                return denied;
                            }
                            let pending = match self.cdc_pending(&txn) {
                                Ok(pending) => pending,
                                Err(error) => return encode_error(error.to_string()),
                            };
                            match self.manager.commit(txn).await {
                                Ok(commit_ts) => {
                                    self.note_range_key(&self.tenant, KV_TABLE, &args[0]);
                                    self.note_range_key(&self.tenant, KV_TABLE, &args[1]);
                                    self.cdc_emit(pending, commit_ts);
                                    self.observe(true);
                                    return encode_bulk(value.as_bytes());
                                }
                                Err(e) => return encode_error(e.to_string()),
                            }
                        }
                        Err(e) => return encode_error(e),
                    }
                }
                Ok(_) => {}
                Err(e) => return encode_error(e),
            }
            drop(txn);
            if deadline.is_some_and(|end| std::time::Instant::now() >= end) {
                return encode_nil_array();
            }
            let remaining =
                deadline.map(|end| end.saturating_duration_since(std::time::Instant::now()));
            Self::wait_for_commit(&mut waiter, remaining).await;
        }
    }

    async fn dispatch(&self, command: RespCommand) -> Vec<u8> {
        let start = std::time::Instant::now();
        let fingerprint = command.name.clone();
        let bytes = Self::command_bytes(&command);
        if let Some(reply) = self.remote_read_reply(&command).await {
            if let Some(denied) = self.admit(false, bytes) {
                self.record_timing(&fingerprint, start.elapsed().as_micros() as u64);
                return denied;
            }
            self.observe(false);
            self.record_timing(&fingerprint, start.elapsed().as_micros() as u64);
            return reply;
        }
        let retry_conflicts = retries_conflict_safely(&command);
        let mut retries = 0;
        let mut admitted = false;
        loop {
            let mut txn = self.manager.begin();
            if let Err(error) = set_command_read_only(&mut txn, &command) {
                self.record_timing(&fingerprint, start.elapsed().as_micros() as u64);
                return encode_error(error.to_string());
            }
            let reply = self.dispatch_in(&mut txn, command.clone());
            if let Err(error) = txn.ensure_healthy() {
                self.record_timing(&fingerprint, start.elapsed().as_micros() as u64);
                return encode_error(error.to_string());
            }
            let write = !txn.writes().is_empty();
            if !admitted {
                if let Some(denied) = self.readonly_deny(write) {
                    self.record_timing(&fingerprint, start.elapsed().as_micros() as u64);
                    return denied;
                }
                if let Some(denied) = self.admit(write, bytes) {
                    self.record_timing(&fingerprint, start.elapsed().as_micros() as u64);
                    return denied;
                }
                admitted = true;
            }
            let pending = match self.cdc_pending(&txn) {
                Ok(pending) => pending,
                Err(error) => return encode_error(error.to_string()),
            };
            let range_keys: Vec<RecordKey> = if self.range_hook.is_armed() {
                txn.writes().keys().cloned().collect()
            } else {
                Vec::new()
            };
            match self.manager.commit(txn).await {
                Ok(commit_ts) => {
                    for key in &range_keys {
                        self.note_range_write(key);
                    }
                    self.cdc_emit(pending, commit_ts);
                    self.observe(write);
                    self.record_timing(&fingerprint, start.elapsed().as_micros() as u64);
                    return reply;
                }
                Err(RymeError::Conflict(_))
                    if retry_conflicts && retries < MAX_COMMAND_CONFLICT_RETRIES =>
                {
                    retries += 1;
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    let reply = encode_error(error.to_string());
                    self.record_timing(&fingerprint, start.elapsed().as_micros() as u64);
                    return reply;
                }
            }
        }
    }

    async fn remote_read_reply(&self, command: &RespCommand) -> Option<Vec<u8>> {
        let reader = self.remote_reader.as_ref()?;
        match command.name.as_str() {
            "GET" if command.args.len() == 1 => {
                let key = RecordKey::new(&self.tenant, &self.database, KV_TABLE, &command.args[0]);
                match reader.read(key).await {
                    Ok(RemoteRead::Local) => None,
                    Ok(RemoteRead::Value { value, .. }) => {
                        Some(value.map_or_else(encode_null, |value| encode_bulk(&value)))
                    }
                    Err(error) => Some(encode_error(error.to_string())),
                }
            }
            "MGET" if !command.args.is_empty() => {
                let results = match reader
                    .read_many(
                        command
                            .args
                            .iter()
                            .map(|key| RecordKey::new(&self.tenant, &self.database, KV_TABLE, key))
                            .collect(),
                    )
                    .await
                {
                    Ok(results) if results.len() == command.args.len() => results,
                    Ok(_) => return Some(encode_error(String::from("owner read count"))),
                    Err(error) => return Some(encode_error(error.to_string())),
                };
                if !results.iter().any(|result| matches!(result, RemoteRead::Value { .. })) {
                    return None;
                }
                let mut values = vec![None; results.len()];
                let mut local_indexes = Vec::new();
                let mut local_keys = Vec::new();
                for (index, (key, result)) in command.args.iter().zip(results).enumerate() {
                    match result {
                        RemoteRead::Local => {
                            local_indexes.push(index);
                            local_keys.push(RecordKey::new(
                                &self.tenant,
                                &self.database,
                                KV_TABLE,
                                key,
                            ));
                        }
                        RemoteRead::Value { value, .. } => {
                            values[index] = value;
                        }
                    }
                }
                if !local_keys.is_empty() {
                    let mut local = self.manager.begin();
                    if let Err(error) = local.set_read_only() {
                        return Some(encode_error(error.to_string()));
                    }
                    let local_values = match self.manager.get_many(&mut local, &local_keys) {
                        Ok(values) if values.len() == local_indexes.len() => values,
                        Ok(_) => return Some(encode_error(String::from("local read count"))),
                        Err(error) => return Some(encode_error(error.to_string())),
                    };
                    for (index, value) in local_indexes.into_iter().zip(local_values) {
                        values[index] = value;
                    }
                }
                Some(encode_array(values))
            }
            "EXISTS" if !command.args.is_empty() => {
                let results = match reader
                    .read_many(
                        command
                            .args
                            .iter()
                            .map(|key| RecordKey::new(&self.tenant, &self.database, KV_TABLE, key))
                            .collect(),
                    )
                    .await
                {
                    Ok(results) if results.len() == command.args.len() => results,
                    Ok(_) => return Some(encode_error(String::from("owner read count"))),
                    Err(error) => return Some(encode_error(error.to_string())),
                };
                if !results.iter().any(|result| matches!(result, RemoteRead::Value { .. })) {
                    return None;
                }
                let mut found = 0i64;
                let mut local_keys = Vec::new();
                for (key, result) in command.args.iter().zip(results) {
                    match result {
                        RemoteRead::Local => local_keys.push(RecordKey::new(
                            &self.tenant,
                            &self.database,
                            KV_TABLE,
                            key,
                        )),
                        RemoteRead::Value { value, .. } => {
                            if value.is_some() {
                                found += 1;
                            }
                        }
                    }
                }
                if !local_keys.is_empty() {
                    let mut local = self.manager.begin();
                    if let Err(error) = local.set_read_only() {
                        return Some(encode_error(error.to_string()));
                    }
                    match self.manager.get_many(&mut local, &local_keys) {
                        Ok(values) if values.len() == local_keys.len() => {
                            found += values.iter().filter(|value| value.is_some()).count() as i64;
                        }
                        Ok(_) => return Some(encode_error(String::from("local read count"))),
                        Err(error) => return Some(encode_error(error.to_string())),
                    }
                }
                Some(encode_integer(found))
            }
            "TYPE" if command.args.len() == 1 => {
                let key = RecordKey::new(&self.tenant, &self.database, KV_TABLE, &command.args[0]);
                match reader.read(key).await {
                    Ok(RemoteRead::Local) => None,
                    Ok(RemoteRead::Value { value, .. }) => {
                        Some(encode_simple(if value.is_some() { "string" } else { "none" }))
                    }
                    Err(error) => Some(encode_error(error.to_string())),
                }
            }
            "TTL" | "PTTL" if command.args.len() == 1 => {
                let key = RecordKey::new(&self.tenant, &self.database, KV_TABLE, &command.args[0]);
                match reader.read(key).await {
                    Ok(RemoteRead::Local) => None,
                    Ok(RemoteRead::Value { value, expires_at }) => Some(encode_integer(
                        remote_ttl(value.is_some(), expires_at, command.name == "PTTL"),
                    )),
                    Err(error) => Some(encode_error(error.to_string())),
                }
            }
            "EXPIRETIME" | "PEXPIRETIME" if command.args.len() == 1 => {
                let key = RecordKey::new(&self.tenant, &self.database, KV_TABLE, &command.args[0]);
                match reader.read(key).await {
                    Ok(RemoteRead::Local) => None,
                    Ok(RemoteRead::Value { value, expires_at }) => Some(encode_integer(
                        remote_expire_time(
                            value.is_some(),
                            expires_at,
                            command.name == "PEXPIRETIME",
                        ),
                    )),
                    Err(error) => Some(encode_error(error.to_string())),
                }
            }
            _ => None,
        }
    }

    fn publish_pubsub(&self, channel: Vec<u8>, payload: Vec<u8>) -> Vec<u8> {
        let Some(realtime) = self.realtime.clone() else {
            return match self.pubsub.publish(&self.tenant, channel, payload) {
                Some(delivered) => encode_integer(delivered as i64),
                None => encode_error(String::from("pubsub memory budget exceeded")),
            };
        };
        let delivered = self.pubsub.delivered_count(&self.tenant, &channel);
        self.start_realtime_pubsub();
        let message = match serde_json::to_value(ClusterPubSubMessage { channel, payload }) {
            Ok(message) => message,
            Err(error) => return encode_error(error.to_string()),
        };
        match realtime.broadcast(
            &self.tenant,
            &resp_pubsub_topic(&self.database),
            String::new(),
            message.clone(),
            0,
        ) {
            Ok(sequence) => {
                if let Some(replicator) = &self.realtime_replicator {
                    let envelope = ClusterPubSubBroadcast {
                        tenant: self.tenant.clone(),
                        channel: resp_pubsub_topic(&self.database),
                        from: String::new(),
                        payload: message,
                        commit_ts: 0,
                        sequence,
                    };
                    if let Ok(payload) = serde_json::to_vec(&envelope) {
                        replicator(payload);
                    }
                }
                encode_integer(delivered as i64)
            }
            Err(error) => encode_error(error.to_string()),
        }
    }

    fn dispatch_in(&self, txn: &mut Transaction, command: RespCommand) -> Vec<u8> {
        match command.name.as_str() {
            "PING" => encode_simple("PONG"),
            "EVAL" | "EVALSHA" => self.eval_command(txn, &command.name, &command.args),
            "SCRIPT" => self.script_command(&command.args),
            "PUBLISH" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                self.publish_pubsub(command.args[0].clone(), command.args[1].clone())
            }
            "ECHO" => match command.args.first() {
                Some(message) => encode_bulk(message),
                None => encode_error(String::from("wrong args")),
            },
            "AUTH" => encode_error(String::from("Client sent AUTH, but no password is set")),
            "GET" => match command.args.first() {
                Some(key) => {
                    let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
                    match self.manager.get(txn, &record) {
                        Ok(Some(value)) => encode_bulk(&value),
                        Ok(None) => encode_null(),
                        Err(e) => encode_error(e.to_string()),
                    }
                }
                None => encode_error(String::from("wrong args")),
            },
            "SET" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                let options = match parse_set_options(&command.args[2..]) {
                    Ok(options) => options,
                    Err(e) => return encode_error(e),
                };
                let record =
                    RecordKey::new(&self.tenant, &self.database, KV_TABLE, &command.args[0]);
                if !options.only_missing && !options.only_present && !options.return_previous {
                    match options.expires_at {
                        Some(ts) => {
                            self.manager.put_with_ttl(txn, record, command.args[1].clone(), ts)
                        }
                        None => self.manager.put(txn, record, command.args[1].clone()),
                    }
                    return encode_simple("OK");
                }
                let previous = match self.manager.get(txn, &record) {
                    Ok(value) => value,
                    Err(e) => return encode_error(e.to_string()),
                };
                let exists = previous.is_some();
                if options.only_missing && exists {
                    return encode_null();
                }
                if options.only_present && !exists {
                    return encode_null();
                }
                match options.expires_at {
                    Some(ts) => self.manager.put_with_ttl(txn, record, command.args[1].clone(), ts),
                    None => self.manager.put(txn, record, command.args[1].clone()),
                }
                if options.return_previous {
                    previous.map_or_else(encode_null, |value| encode_bulk(&value))
                } else {
                    encode_simple("OK")
                }
            }
            "SETNX" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                let record =
                    RecordKey::new(&self.tenant, &self.database, KV_TABLE, &command.args[0]);
                match self.manager.get(txn, &record) {
                    Ok(Some(_)) => encode_integer(0),
                    Ok(None) => {
                        self.manager.put(txn, record, command.args[1].clone());
                        encode_integer(1)
                    }
                    Err(error) => encode_error(error.to_string()),
                }
            }
            "GETSET" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                let record =
                    RecordKey::new(&self.tenant, &self.database, KV_TABLE, &command.args[0]);
                match self.manager.get(txn, &record) {
                    Ok(previous) => {
                        self.manager.put(txn, record, command.args[1].clone());
                        previous.map_or_else(encode_null, |value| encode_bulk(&value))
                    }
                    Err(error) => encode_error(error.to_string()),
                }
            }
            "GETEX" => self.getex(txn, &command.args),
            "GETDEL" => match command.args.first() {
                Some(key) => {
                    let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
                    match self.manager.get(txn, &record) {
                        Ok(Some(value)) => {
                            self.manager.delete(txn, record);
                            encode_bulk(&value)
                        }
                        Ok(None) => encode_null(),
                        Err(e) => encode_error(e.to_string()),
                    }
                }
                None => encode_error(String::from("wrong args")),
            },
            "EXPIRE" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.expire_key(txn, &command.args[0], &command.args[1], &command.args[2..]) {
                    Ok(applied) => encode_integer(i64::from(applied)),
                    Err(e) => encode_error(e),
                }
            }
            "PEXPIRE" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.expire_key_ms(
                    txn,
                    &command.args[0],
                    &command.args[1],
                    &command.args[2..],
                ) {
                    Ok(applied) => encode_integer(i64::from(applied)),
                    Err(e) => encode_error(e),
                }
            }
            "EXPIREAT" | "PEXPIREAT" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.expire_key_at(
                    txn,
                    &command.args[0],
                    &command.args[1],
                    command.name == "PEXPIREAT",
                    &command.args[2..],
                ) {
                    Ok(applied) => encode_integer(i64::from(applied)),
                    Err(error) => encode_error(error),
                }
            }
            "TTL" => match command.args.first() {
                Some(key) => match self.ttl_secs(txn, key) {
                    Ok(ttl) => encode_integer(ttl),
                    Err(error) => encode_error(error.to_string()),
                },
                None => encode_error(String::from("wrong args")),
            },
            "PTTL" => match command.args.first() {
                Some(key) => match self.ttl_millis(txn, key) {
                    Ok(ttl) => encode_integer(ttl),
                    Err(error) => encode_error(error.to_string()),
                },
                None => encode_error(String::from("wrong args")),
            },
            "EXPIRETIME" | "PEXPIRETIME" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.expire_time(
                    txn,
                    &command.args[0],
                    command.name == "PEXPIRETIME",
                ) {
                    Ok(timestamp) => encode_integer(timestamp),
                    Err(error) => encode_error(error.to_string()),
                }
            }
            "PERSIST" => match command.args.first() {
                Some(key) => {
                    let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
                    let staged_expiry = txn.writes().get(&record).is_some_and(|write| {
                        write.value.is_some()
                            && write.expires_at != 0
                            && !ryme_storage::expiry_is_expired(
                                write.expires_at,
                                ryme_txn::now_unix_millis(),
                            )
                    });
                    if staged_expiry {
                        match self.manager.get(txn, &record) {
                            Ok(Some(value)) => {
                                self.manager.put(txn, record, value);
                                encode_integer(1)
                            }
                            Ok(None) => encode_integer(0),
                            Err(error) => encode_error(error.to_string()),
                        }
                    } else if txn.writes().contains_key(&record) {
                        encode_integer(0)
                    } else {
                        match self.manager.expires_at_at(&record, txn.read_ts) {
                            Ok(Some(ts)) if ts != 0 => match self.manager.get(txn, &record) {
                                Ok(Some(value)) => {
                                    self.manager.put(txn, record, value);
                                    encode_integer(1)
                                }
                                Ok(None) => encode_integer(0),
                                Err(error) => encode_error(error.to_string()),
                            },
                            Ok(_) => encode_integer(0),
                            Err(error) => encode_error(error.to_string()),
                        }
                    }
                }
                None => encode_error(String::from("wrong args")),
            },
            "DEL" => {
                if command.args.is_empty() {
                    return encode_error(String::from("wrong args"));
                }
                let mut seen = HashSet::with_capacity(command.args.len());
                let keys: Vec<_> =
                    command.args.into_iter().filter(|key| seen.insert(key.clone())).collect();
                let records: Vec<_> = keys
                    .iter()
                    .map(|key| RecordKey::new(&self.tenant, &self.database, KV_TABLE, key))
                    .collect();
                let mut exists = Vec::with_capacity(records.len());
                for batch in records.chunks(RESP_MGET_BATCH_ITEMS) {
                    let values = match self.manager.get_many(txn, batch) {
                        Ok(values) if values.len() == batch.len() => values,
                        Ok(_) => return encode_error(String::from("read count")),
                        Err(error) => return encode_error(error.to_string()),
                    };
                    exists.extend(values.into_iter().map(|value| value.is_some()));
                }
                let mut removed = 0;
                for (record, exists) in records.into_iter().zip(exists) {
                    if exists {
                        self.manager.delete(txn, record);
                        removed += 1;
                    }
                }
                encode_integer(removed)
            }
            "EXISTS" => {
                if command.args.is_empty() {
                    return encode_error(String::from("wrong args"));
                }
                let records = command
                    .args
                    .iter()
                    .map(|key| RecordKey::new(&self.tenant, &self.database, KV_TABLE, key))
                    .collect::<Vec<_>>();
                let mut found = 0usize;
                for batch in records.chunks(RESP_MGET_BATCH_ITEMS) {
                    match self.manager.get_many(txn, batch) {
                        Ok(values) if values.len() == batch.len() => {
                            found += values.iter().filter(|value| value.is_some()).count();
                        }
                        Ok(_) => return encode_error(String::from("read count")),
                        Err(error) => return encode_error(error.to_string()),
                    }
                }
                encode_integer(found as i64)
            }
            "TYPE" => match command.args.first() {
                Some(key) => {
                    let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
                    match self.manager.get(txn, &record) {
                        Ok(Some(_)) => encode_simple("string"),
                        Ok(None) => encode_simple("none"),
                        Err(error) => encode_error(error.to_string()),
                    }
                }
                None => encode_error(String::from("wrong args")),
            },
            "APPEND" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                let record =
                    RecordKey::new(&self.tenant, &self.database, KV_TABLE, &command.args[0]);
                let mut current = match self.manager.get(txn, &record) {
                    Ok(value) => value.unwrap_or_default(),
                    Err(error) => return encode_error(error.to_string()),
                };
                current.extend_from_slice(&command.args[1]);
                let length = current.len();
                self.manager.put(txn, record, current);
                encode_integer(length as i64)
            }
            "STRLEN" => match command.args.first() {
                Some(key) => {
                    let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
                    match self.manager.get(txn, &record) {
                        Ok(Some(value)) => encode_integer(value.len() as i64),
                        Ok(None) => encode_integer(0),
                        Err(error) => encode_error(error.to_string()),
                    }
                }
                None => encode_error(String::from("wrong args")),
            },
            "INCR" => self.incr_by(txn, &command.args, 1),
            "DECR" => self.incr_by(txn, &command.args, -1),
            "INCRBY" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                let delta: i64 = match int_arg(&command.args[1]) {
                    Ok(delta) => delta,
                    Err(e) => return encode_error(e),
                };
                self.incr_by(txn, &command.args[..1], delta)
            }
            "DECRBY" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                let delta: i64 = match int_arg(&command.args[1]) {
                    Ok(delta) => delta,
                    Err(e) => return encode_error(e),
                };
                self.incr_by(txn, &command.args[..1], -delta)
            }
            "INCRBYFLOAT" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                let delta: f64 = match float_arg(&command.args[1]) {
                    Ok(delta) => delta,
                    Err(e) => return encode_error(e),
                };
                self.incr_float(txn, &command.args[0], delta)
            }
            "MGET" => {
                if command.args.is_empty() {
                    return encode_error(String::from("wrong args"));
                }
                let records = command
                    .args
                    .iter()
                    .map(|key| RecordKey::new(&self.tenant, &self.database, KV_TABLE, key))
                    .collect::<Vec<_>>();
                let mut items = Vec::with_capacity(records.len());
                let mut response_bytes = 0usize;
                for batch in records.chunks(RESP_MGET_BATCH_ITEMS) {
                    let values = match self.manager.get_many(txn, batch) {
                        Ok(values) if values.len() == batch.len() => values,
                        Ok(_) => return encode_error(String::from("MGET result size mismatch")),
                        Err(error) => return encode_error(error.to_string()),
                    };
                    for value in values {
                        match value {
                            Some(value) => {
                                response_bytes =
                                    match next_resp_collection_bytes(response_bytes, value.len()) {
                                        Ok(bytes) => bytes,
                                        Err(error) => return encode_error(error),
                                    };
                                items.push(Some(value));
                            }
                            None => items.push(None),
                        }
                    }
                }
                encode_array(items)
            }
            "MSET" => {
                if command.args.is_empty() || !command.args.len().is_multiple_of(2) {
                    return encode_error(String::from("wrong args"));
                }
                let writes: Vec<_> = command
                    .args
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| RecordKey::new(&self.tenant, &self.database, KV_TABLE, &pair[0]))
                    .collect();
                for (record, pair) in writes.into_iter().zip(command.args.as_chunks::<2>().0) {
                    self.manager.put(txn, record, pair[1].clone());
                }
                encode_simple("OK")
            }
            "MSETNX" => {
                if command.args.is_empty() || !command.args.len().is_multiple_of(2) {
                    return encode_error(String::from("wrong args"));
                }
                let mut records = Vec::with_capacity(command.args.len() / 2);
                for pair in command.args.as_chunks::<2>().0 {
                    let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, &pair[0]);
                    match self.manager.get(txn, &record) {
                        Ok(Some(_)) => return encode_integer(0),
                        Ok(None) => records.push((record, pair[1].clone())),
                        Err(error) => return encode_error(error.to_string()),
                    }
                }
                for (record, value) in records {
                    self.manager.put(txn, record, value);
                }
                encode_integer(1)
            }
            "HSET" => {
                if command.args.len() < 3 || command.args.len().is_multiple_of(2) {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_map(txn, &command.args[0]) {
                    Ok(map) => {
                        let mut map = map.unwrap_or_default();
                        let mut added = 0;
                        for pair in command.args[1..].as_chunks::<2>().0 {
                            let field = match std::str::from_utf8(&pair[0]) {
                                Ok(field) => field.to_string(),
                                Err(_) => return encode_error(String::from("wrong type")),
                            };
                            let value = match std::str::from_utf8(&pair[1]) {
                                Ok(value) => value.to_string(),
                                Err(_) => return encode_error(String::from("wrong type")),
                            };
                            if map.insert(field, value).is_none() {
                                added += 1;
                            }
                        }
                        match self.write_map(txn, &command.args[0], &map) {
                            Ok(()) => encode_integer(added),
                            Err(e) => encode_error(e),
                        }
                    }
                    Err(e) => encode_error(e),
                }
            }
            "HGET" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_map(txn, &command.args[0]) {
                    Ok(map) => match map
                        .unwrap_or_default()
                        .get(&String::from_utf8_lossy(&command.args[1]).into_owned())
                    {
                        Some(value) => encode_bulk(value.as_bytes()),
                        None => encode_null(),
                    },
                    Err(e) => encode_error(e),
                }
            }
            "HDEL" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_map(txn, &command.args[0]) {
                    Ok(map) => {
                        let mut map = map.unwrap_or_default();
                        let mut removed = 0;
                        for field in &command.args[1..] {
                            if map.remove(String::from_utf8_lossy(field).as_ref()).is_some() {
                                removed += 1;
                            }
                        }
                        match self.write_map(txn, &command.args[0], &map) {
                            Ok(()) => encode_integer(removed),
                            Err(e) => encode_error(e),
                        }
                    }
                    Err(e) => encode_error(e),
                }
            }
            "HEXISTS" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_map(txn, &command.args[0]) {
                    Ok(map) => encode_integer(
                        map.unwrap_or_default()
                            .contains_key(String::from_utf8_lossy(&command.args[1]).as_ref())
                            as i64,
                    ),
                    Err(e) => encode_error(e),
                }
            }
            "HLEN" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_map(txn, &command.args[0]) {
                    Ok(map) => encode_integer(map.unwrap_or_default().len() as i64),
                    Err(e) => encode_error(e),
                }
            }
            "HGETALL" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_map(txn, &command.args[0]) {
                    Ok(map) => {
                        let mut pairs: Vec<(String, String)> =
                            map.unwrap_or_default().into_iter().collect();
                        pairs.sort_by(|a, b| a.0.cmp(&b.0));
                        let mut items = Vec::new();
                        for (field, value) in pairs {
                            items.push(Some(field.into_bytes()));
                            items.push(Some(value.into_bytes()));
                        }
                        encode_array(items)
                    }
                    Err(e) => encode_error(e),
                }
            }
            "HKEYS" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_map(txn, &command.args[0]) {
                    Ok(map) => {
                        let mut fields: Vec<String> =
                            map.unwrap_or_default().keys().cloned().collect();
                        fields.sort();
                        encode_array(fields.into_iter().map(|f| Some(f.into_bytes())).collect())
                    }
                    Err(e) => encode_error(e),
                }
            }
            "HVALS" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_map(txn, &command.args[0]) {
                    Ok(map) => {
                        let mut pairs: Vec<(String, String)> =
                            map.unwrap_or_default().into_iter().collect();
                        pairs.sort_by(|a, b| a.0.cmp(&b.0));
                        encode_array(pairs.into_iter().map(|(_, v)| Some(v.into_bytes())).collect())
                    }
                    Err(e) => encode_error(e),
                }
            }
            "LPUSH" => self.push_list(txn, &command.args, true),
            "RPUSH" => self.push_list(txn, &command.args, false),
            "LPOP" => self.pop_list(txn, &command.args, true),
            "RPOP" => self.pop_list(txn, &command.args, false),
            "LLEN" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_list(txn, &command.args[0]) {
                    Ok(list) => encode_integer(list.unwrap_or_default().len() as i64),
                    Err(e) => encode_error(e),
                }
            }
            "LRANGE" => {
                if command.args.len() != 3 {
                    return encode_error(String::from("wrong args"));
                }
                let (start, stop) = match (int_arg(&command.args[1]), int_arg(&command.args[2])) {
                    (Ok(start), Ok(stop)) => (start, stop),
                    (Err(e), _) | (_, Err(e)) => return encode_error(e),
                };
                match self.read_list(txn, &command.args[0]) {
                    Ok(list) => {
                        let list = list.unwrap_or_default();
                        encode_array(slice_list(&list, start, stop))
                    }
                    Err(e) => encode_error(e),
                }
            }
            "SADD" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_set(txn, &command.args[0]) {
                    Ok(set) => {
                        let mut set = set.unwrap_or_default();
                        let mut added = 0;
                        for member in &command.args[1..] {
                            let text = match std::str::from_utf8(member) {
                                Ok(text) => text.to_string(),
                                Err(_) => return encode_error(String::from("wrong type")),
                            };
                            if set.insert(text) {
                                added += 1;
                            }
                        }
                        match self.write_set(txn, &command.args[0], &set) {
                            Ok(()) => encode_integer(added),
                            Err(e) => encode_error(e),
                        }
                    }
                    Err(e) => encode_error(e),
                }
            }
            "SREM" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_set(txn, &command.args[0]) {
                    Ok(set) => {
                        let mut set = set.unwrap_or_default();
                        let mut removed = 0;
                        for member in &command.args[1..] {
                            if set.remove(String::from_utf8_lossy(member).as_ref()) {
                                removed += 1;
                            }
                        }
                        match self.write_set(txn, &command.args[0], &set) {
                            Ok(()) => encode_integer(removed),
                            Err(e) => encode_error(e),
                        }
                    }
                    Err(e) => encode_error(e),
                }
            }
            "SMEMBERS" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_set(txn, &command.args[0]) {
                    Ok(set) => {
                        let mut members: Vec<String> =
                            set.unwrap_or_default().into_iter().collect();
                        members.sort();
                        encode_array(members.into_iter().map(|m| Some(m.into_bytes())).collect())
                    }
                    Err(e) => encode_error(e),
                }
            }
            "SCARD" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_set(txn, &command.args[0]) {
                    Ok(set) => encode_integer(set.unwrap_or_default().len() as i64),
                    Err(e) => encode_error(e),
                }
            }
            "SISMEMBER" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_set(txn, &command.args[0]) {
                    Ok(set) => encode_integer(
                        set.unwrap_or_default()
                            .contains(String::from_utf8_lossy(&command.args[1]).as_ref())
                            as i64,
                    ),
                    Err(e) => encode_error(e),
                }
            }
            "ZADD" => match self.zadd(txn, &command.args) {
                Ok(added) => encode_integer(added),
                Err(e) => encode_error(e),
            },
            "ZSCORE" => {
                if command.args.len() != 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_zset(txn, &command.args[0]) {
                    Ok(set) => match set
                        .unwrap_or_default()
                        .get(String::from_utf8_lossy(&command.args[1]).as_ref())
                    {
                        Some(score) => encode_bulk(format_float(*score).as_bytes()),
                        None => encode_null(),
                    },
                    Err(e) => encode_error(e),
                }
            }
            "ZRANK" => match self.zrank(txn, &command.args, false) {
                Ok(rank) => rank.map(encode_integer).unwrap_or_else(encode_null),
                Err(e) => encode_error(e),
            },
            "ZREVRANK" => match self.zrank(txn, &command.args, true) {
                Ok(rank) => rank.map(encode_integer).unwrap_or_else(encode_null),
                Err(e) => encode_error(e),
            },
            "ZRANGE" => match self.zrange(txn, &command.args) {
                Ok(items) => encode_array(items),
                Err(e) => encode_error(e),
            },
            "ZREM" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_zset(txn, &command.args[0]) {
                    Ok(set) => {
                        let mut set = set.unwrap_or_default();
                        let mut removed = 0;
                        for member in &command.args[1..] {
                            if set.remove(String::from_utf8_lossy(member).as_ref()).is_some() {
                                removed += 1;
                            }
                        }
                        match self.write_zset(txn, &command.args[0], &set) {
                            Ok(()) => encode_integer(removed),
                            Err(e) => encode_error(e),
                        }
                    }
                    Err(e) => encode_error(e),
                }
            }
            "ZCARD" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_zset(txn, &command.args[0]) {
                    Ok(set) => encode_integer(set.unwrap_or_default().len() as i64),
                    Err(e) => encode_error(e),
                }
            }
            "ZCOUNT" => {
                if command.args.len() != 3 {
                    return encode_error(String::from("wrong args"));
                }
                match self.zcount(txn, &command.args) {
                    Ok(count) => encode_integer(count),
                    Err(e) => encode_error(e),
                }
            }
            "ZINCRBY" => {
                if command.args.len() != 3 {
                    return encode_error(String::from("wrong args"));
                }
                match self.zincrby(txn, &command.args) {
                    Ok(score) => encode_bulk(format_float(score).as_bytes()),
                    Err(e) => encode_error(e),
                }
            }
            "SCAN" => match self.scan_cursor(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "KEYS" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                let pattern = &command.args[0];
                let mut after = Vec::new();
                let mut keys = Vec::new();
                let mut key_bytes = 0usize;
                loop {
                    let rows = match self.manager.scan_after(
                        txn,
                        &self.tenant,
                        &self.database,
                        KV_TABLE,
                        &after,
                        RESP_SCAN_PAGE_ITEMS,
                    ) {
                        Ok(rows) => rows,
                        Err(e) => return encode_error(e.to_string()),
                    };
                    if rows.is_empty() {
                        break;
                    }
                    let short_page = rows.len() < RESP_SCAN_PAGE_ITEMS;
                    for (key, _) in rows {
                        after = key.clone();
                        if glob_match(pattern, &key) {
                            key_bytes = match next_scan_result_key_bytes(
                                keys.len(),
                                key_bytes,
                                key.len(),
                            ) {
                                Ok(bytes) => bytes,
                                Err(error) => return encode_error(error),
                            };
                            if keys.try_reserve(1).is_err() {
                                return encode_error(String::from("scan result allocation failed"));
                            }
                            keys.push(Some(key));
                        }
                    }
                    if short_page {
                        break;
                    }
                }
                encode_array(keys)
            }
            "XADD" => match self.xadd(txn, &command.args) {
                Ok(id) => encode_bulk(id.as_bytes()),
                Err(e) => encode_error(e),
            },
            "XRANGE" => match self.xrange(txn, &command.args, false) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "XREVRANGE" => match self.xrange(txn, &command.args, true) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "XLEN" => {
                if command.args.len() != 1 {
                    return encode_error(String::from("wrong args"));
                }
                match self.read_stream(txn, &command.args[0]) {
                    Ok(stream) => encode_integer(stream.unwrap_or_default().entries.len() as i64),
                    Err(e) => encode_error(e),
                }
            }
            "XTRIM" => match self.xtrim(txn, &command.args) {
                Ok(removed) => encode_integer(removed),
                Err(e) => encode_error(e),
            },
            "XREAD" => match self.xread(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "XDEL" => {
                if command.args.len() < 2 {
                    return encode_error(String::from("wrong args"));
                }
                for id in &command.args[1..] {
                    let text = String::from_utf8_lossy(id).into_owned();
                    if Self::parse_stream_id(&text).is_err() {
                        return encode_error(String::from(
                            "Invalid stream ID specified as stream command argument",
                        ));
                    }
                }
                match self.read_stream(txn, &command.args[0]) {
                    Ok(stream) => {
                        let mut data = stream.unwrap_or_default();
                        let mut removed = 0;
                        for id in &command.args[1..] {
                            let text = String::from_utf8_lossy(id).into_owned();
                            if data.entries.iter().any(|(eid, _)| eid == &text) {
                                data.entries.retain(|(eid, _)| eid != &text);
                                Self::note_deleted(&mut data, &text);
                                removed += 1;
                            }
                        }
                        match self.write_stream(txn, &command.args[0], &data) {
                            Ok(()) => encode_integer(removed),
                            Err(e) => encode_error(e),
                        }
                    }
                    Err(e) => encode_error(e),
                }
            }
            "XGROUP" => match self.xgroup(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "XREADGROUP" => match self.xreadgroup(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "XACK" => match self.xack(txn, &command.args) {
                Ok(count) => encode_integer(count),
                Err(e) => encode_error(e),
            },
            "XPENDING" => match self.xpending(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "XAUTOCLAIM" => match self.xautoclaim(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "XINFO" => match self.xinfo(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "PFADD" => {
                if command.args.is_empty() {
                    return encode_error(String::from("wrong args"));
                }
                match self.pfadd(txn, &command.args) {
                    Ok(changed) => encode_integer(i64::from(changed)),
                    Err(e) => encode_error(e),
                }
            }
            "PFCOUNT" => {
                if command.args.is_empty() {
                    return encode_error(String::from("wrong args"));
                }
                match self.pfcount(txn, &command.args) {
                    Ok(count) => encode_integer(count as i64),
                    Err(e) => encode_error(e),
                }
            }
            "PFMERGE" => {
                if command.args.is_empty() {
                    return encode_error(String::from("wrong args"));
                }
                match self.pfmerge(txn, &command.args) {
                    Ok(()) => encode_simple("OK"),
                    Err(e) => encode_error(e),
                }
            }
            "GEOADD" => match self.geoadd(txn, &command.args) {
                Ok(added) => encode_integer(added),
                Err(e) => encode_error(e),
            },
            "GEODIST" => match self.geodist(txn, &command.args) {
                Ok(distance) => {
                    distance.map(|d| encode_bulk(d.as_bytes())).unwrap_or_else(encode_null)
                }
                Err(e) => encode_error(e),
            },
            "GEOPOS" => match self.geopos(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "GEOSEARCH" => match self.geosearch(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "GEORADIUS" => match self.georadius(txn, &command.args, false) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "GEORADIUSBYMEMBER" => match self.georadius(txn, &command.args, true) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            "GEOHASH" => match self.geohash(txn, &command.args) {
                Ok(reply) => reply,
                Err(e) => encode_error(e),
            },
            _ => encode_error(String::from("unknown command")),
        }
    }

    fn incr_by(&self, txn: &mut Transaction, args: &[Vec<u8>], delta: i64) -> Vec<u8> {
        if args.len() != 1 {
            return encode_error(String::from("wrong args"));
        }
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, &args[0]);
        let current = match self.manager.get(txn, &record) {
            Ok(value) => value.unwrap_or_default(),
            Err(error) => return encode_error(error.to_string()),
        };
        let base: i64 = if current.is_empty() {
            0
        } else {
            match std::str::from_utf8(&current).ok().and_then(|s| s.parse().ok()) {
                Some(base) => base,
                None => {
                    return encode_error(String::from("value is not an integer or out of range"))
                }
            }
        };
        let next = base.saturating_add(delta);
        self.manager.put(txn, record, next.to_string().into_bytes());
        encode_integer(next)
    }

    fn incr_float(&self, txn: &mut Transaction, key: &[u8], delta: f64) -> Vec<u8> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let current = match self.manager.get(txn, &record) {
            Ok(value) => value.unwrap_or_default(),
            Err(error) => return encode_error(error.to_string()),
        };
        let base: f64 = if current.is_empty() {
            0.0
        } else {
            match std::str::from_utf8(&current).ok().and_then(|s| s.parse().ok()) {
                Some(base) => base,
                None => return encode_error(String::from("value is not a valid float")),
            }
        };
        let next = base + delta;
        self.manager.put(txn, record, format_float(next).into_bytes());
        encode_bulk(format_float(next).as_bytes())
    }

    fn read_map(
        &self,
        txn: &mut Transaction,
        key: &[u8],
    ) -> std::result::Result<Option<std::collections::HashMap<String, String>>, String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        match self.manager.get(txn, &record).map_err(|error| error.to_string())? {
            Some(value) if value.is_empty() => Ok(Some(std::collections::HashMap::new())),
            Some(value) => {
                check_collection_bytes(&value)?;
                serde_json::from_slice(&value).map(Some).map_err(|_| String::from("wrong type"))
            }
            None => Ok(None),
        }
    }

    fn write_map(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        map: &std::collections::HashMap<String, String>,
    ) -> std::result::Result<(), String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let raw = serde_json::to_vec(map).map_err(|_| String::from("wrong type"))?;
        check_collection_bytes(&raw)?;
        self.manager.put(txn, record, raw);
        Ok(())
    }

    fn read_list(
        &self,
        txn: &mut Transaction,
        key: &[u8],
    ) -> std::result::Result<Option<Vec<String>>, String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        match self.manager.get(txn, &record).map_err(|error| error.to_string())? {
            Some(value) if value.is_empty() => Ok(Some(Vec::new())),
            Some(value) => {
                check_collection_bytes(&value)?;
                serde_json::from_slice(&value).map(Some).map_err(|_| String::from("wrong type"))
            }
            None => Ok(None),
        }
    }

    fn write_list(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        list: &[String],
    ) -> std::result::Result<(), String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let raw = serde_json::to_vec(list).map_err(|_| String::from("wrong type"))?;
        check_collection_bytes(&raw)?;
        self.manager.put(txn, record, raw);
        Ok(())
    }

    fn push_list(&self, txn: &mut Transaction, args: &[Vec<u8>], front: bool) -> Vec<u8> {
        if args.len() < 2 {
            return encode_error(String::from("wrong args"));
        }
        let mut list = match self.read_list(txn, &args[0]) {
            Ok(list) => list.unwrap_or_default(),
            Err(e) => return encode_error(e),
        };
        for member in &args[1..] {
            let text = match std::str::from_utf8(member) {
                Ok(text) => text.to_string(),
                Err(_) => return encode_error(String::from("wrong type")),
            };
            if front {
                list.insert(0, text);
            } else {
                list.push(text);
            }
        }
        let length = list.len();
        match self.write_list(txn, &args[0], &list) {
            Ok(()) => encode_integer(length as i64),
            Err(e) => encode_error(e),
        }
    }

    fn pop_list(&self, txn: &mut Transaction, args: &[Vec<u8>], front: bool) -> Vec<u8> {
        if args.len() != 1 {
            return encode_error(String::from("wrong args"));
        }
        let mut list = match self.read_list(txn, &args[0]) {
            Ok(list) => list.unwrap_or_default(),
            Err(e) => return encode_error(e),
        };
        if list.is_empty() {
            return encode_null();
        }
        let value = if front { list.remove(0) } else { list.pop().unwrap_or_default() };
        match self.write_list(txn, &args[0], &list) {
            Ok(()) => encode_bulk(value.as_bytes()),
            Err(e) => encode_error(e),
        }
    }

    fn read_set(
        &self,
        txn: &mut Transaction,
        key: &[u8],
    ) -> std::result::Result<Option<std::collections::HashSet<String>>, String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        match self.manager.get(txn, &record).map_err(|error| error.to_string())? {
            Some(value) if value.is_empty() => Ok(Some(std::collections::HashSet::new())),
            Some(value) => {
                check_collection_bytes(&value)?;
                let list: Vec<String> =
                    serde_json::from_slice(&value).map_err(|_| String::from("wrong type"))?;
                Ok(Some(list.into_iter().collect()))
            }
            None => Ok(None),
        }
    }

    fn write_set(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        set: &std::collections::HashSet<String>,
    ) -> std::result::Result<(), String> {
        let mut list: Vec<String> = set.iter().cloned().collect();
        list.sort();
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let raw = serde_json::to_vec(&list).map_err(|_| String::from("wrong type"))?;
        check_collection_bytes(&raw)?;
        self.manager.put(txn, record, raw);
        Ok(())
    }

    fn read_zset(
        &self,
        txn: &mut Transaction,
        key: &[u8],
    ) -> std::result::Result<Option<std::collections::HashMap<String, f64>>, String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        match self.manager.get(txn, &record).map_err(|error| error.to_string())? {
            Some(value) if value.is_empty() => Ok(Some(std::collections::HashMap::new())),
            Some(value) => {
                check_collection_bytes(&value)?;
                serde_json::from_slice(&value).map(Some).map_err(|_| String::from("wrong type"))
            }
            None => Ok(None),
        }
    }

    fn write_zset(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        set: &std::collections::HashMap<String, f64>,
    ) -> std::result::Result<(), String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let raw = serde_json::to_vec(set).map_err(|_| String::from("wrong type"))?;
        check_collection_bytes(&raw)?;
        self.manager.put(txn, record, raw);
        Ok(())
    }

    fn sorted_entries(set: &std::collections::HashMap<String, f64>) -> Vec<(&String, f64)> {
        let mut entries: Vec<(&String, f64)> = set.iter().map(|(m, s)| (m, *s)).collect();
        entries.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(b.0)));
        entries
    }

    fn parse_score(raw: &[u8]) -> std::result::Result<f64, String> {
        let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax error"))?;
        match text.to_ascii_lowercase().as_str() {
            "inf" | "+inf" => Ok(f64::INFINITY),
            "-inf" => Ok(f64::NEG_INFINITY),
            _ => match text.parse::<f64>() {
                Ok(value) if value.is_finite() => Ok(value),
                Ok(_) => Err(String::from("value is not a valid float")),
                Err(_) => Err(String::from("syntax error")),
            },
        }
    }

    fn zadd(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<i64, String> {
        if args.len() < 3 {
            return Err(String::from("wrong args"));
        }
        let mut only_add = false;
        let mut only_update = false;
        let mut greater = false;
        let mut less = false;
        let mut index = 1;
        while index < args.len() {
            let flag = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
            match flag.to_ascii_uppercase().as_str() {
                "NX" => {
                    only_add = true;
                    index += 1;
                }
                "XX" => {
                    only_update = true;
                    index += 1;
                }
                "GT" => {
                    greater = true;
                    index += 1;
                }
                "LT" => {
                    less = true;
                    index += 1;
                }
                _ => break,
            }
        }
        if only_add && only_update {
            return Err(String::from("syntax"));
        }
        let pairs = &args[index..];
        if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
            return Err(String::from("syntax"));
        }
        let mut set = self.read_zset(txn, &args[0])?.unwrap_or_default();
        let mut added = 0;
        for pair in pairs.as_chunks::<2>().0 {
            let score = Self::parse_score(&pair[0])?;
            let member =
                std::str::from_utf8(&pair[1]).map_err(|_| String::from("wrong type"))?.to_string();
            match set.get(&member).copied() {
                None if only_update => {}
                None => {
                    set.insert(member, score);
                    added += 1;
                }
                Some(current) => {
                    let update =
                        !(only_add || (greater && score <= current) || (less && score >= current));
                    if update {
                        set.insert(member, score);
                    }
                }
            }
        }
        self.write_zset(txn, &args[0], &set)?;
        Ok(added)
    }

    fn zrank(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
        reverse: bool,
    ) -> std::result::Result<Option<i64>, String> {
        if args.len() != 2 {
            return Err(String::from("wrong args"));
        }
        let set = self.read_zset(txn, &args[0])?.unwrap_or_default();
        let member = String::from_utf8_lossy(&args[1]).into_owned();
        let mut entries = Self::sorted_entries(&set);
        if reverse {
            entries.reverse();
        }
        Ok(entries.iter().position(|(m, _)| *m == &member).map(|rank| rank as i64))
    }

    fn zrange(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<Option<Vec<u8>>>, String> {
        if args.len() < 3 {
            return Err(String::from("wrong args"));
        }
        let (start, stop) = match (int_arg(&args[1]), int_arg(&args[2])) {
            (Ok(start), Ok(stop)) => (start, stop),
            (Err(e), _) | (_, Err(e)) => return Err(e),
        };
        let mut reverse = false;
        let mut with_scores = false;
        for flag in &args[3..] {
            let name = std::str::from_utf8(flag).map_err(|_| String::from("syntax"))?;
            match name.to_ascii_uppercase().as_str() {
                "REV" => reverse = true,
                "WITHSCORES" => with_scores = true,
                _ => return Err(String::from("syntax")),
            }
        }
        let set = self.read_zset(txn, &args[0])?.unwrap_or_default();
        let mut entries = Self::sorted_entries(&set);
        if reverse {
            entries.reverse();
        }
        let members: Vec<String> = entries.iter().map(|(m, _)| (*m).clone()).collect();
        let mut items = Vec::new();
        for member in slice_list(&members, start, stop).into_iter().flatten() {
            if with_scores {
                let score =
                    set.get(String::from_utf8_lossy(&member).as_ref()).copied().unwrap_or(0.0);
                items.push(Some(member));
                items.push(Some(format_float(score).into_bytes()));
            } else {
                items.push(Some(member));
            }
        }
        Ok(items)
    }

    fn zcount(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<i64, String> {
        let (min, min_exclusive) = Self::parse_zbound(&args[1])?;
        let (max, max_exclusive) = Self::parse_zbound(&args[2])?;
        let set = self.read_zset(txn, &args[0])?.unwrap_or_default();
        let mut count = 0;
        for score in set.values() {
            let above = if min_exclusive { *score > min } else { *score >= min };
            let below = if max_exclusive { *score < max } else { *score <= max };
            if above && below {
                count += 1;
            }
        }
        Ok(count)
    }

    fn parse_zbound(raw: &[u8]) -> std::result::Result<(f64, bool), String> {
        let bound_error = || String::from("min or max is not a float");
        let text = std::str::from_utf8(raw).map_err(|_| bound_error())?;
        let lowered = text.to_ascii_lowercase();
        if lowered == "-inf" {
            return Ok((f64::NEG_INFINITY, false));
        }
        if lowered == "+inf" || lowered == "inf" {
            return Ok((f64::INFINITY, false));
        }
        if let Some(rest) = text.strip_prefix('(') {
            let value = Self::parse_score(rest.as_bytes()).map_err(|_| bound_error())?;
            return Ok((value, true));
        }
        Ok((Self::parse_score(raw).map_err(|_| bound_error())?, false))
    }

    fn zincrby(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<f64, String> {
        let delta = float_arg(&args[1])?;
        let member =
            std::str::from_utf8(&args[2]).map_err(|_| String::from("wrong type"))?.to_string();
        let mut set = self.read_zset(txn, &args[0])?.unwrap_or_default();
        let next = set.get(&member).copied().unwrap_or(0.0) + delta;
        if !next.is_finite() {
            return Err(String::from("increment would overflow"));
        }
        set.insert(member, next);
        self.write_zset(txn, &args[0], &set)?;
        Ok(next)
    }

    fn read_stream(
        &self,
        txn: &mut Transaction,
        key: &[u8],
    ) -> std::result::Result<Option<StreamData>, String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        match self.manager.get(txn, &record).map_err(|error| error.to_string())? {
            Some(value) if value.is_empty() => Ok(Some(StreamData::default())),
            Some(value) => {
                check_collection_bytes(&value)?;
                let doc: serde_json::Value =
                    serde_json::from_slice(&value).map_err(|_| String::from("wrong type"))?;
                if doc.is_array() {
                    Ok(Some(StreamData {
                        entries: Self::parse_stream_entries(&doc)?,
                        groups: std::collections::HashMap::new(),
                        added: 0,
                        max_deleted: String::from("0-0"),
                    }))
                } else if doc.is_object() {
                    let entries = doc
                        .get("entries")
                        .map(Self::parse_stream_entries)
                        .transpose()?
                        .unwrap_or_default();
                    let added = doc.get("added").and_then(|v| v.as_u64()).unwrap_or(0);
                    let max_deleted = doc
                        .get("max_deleted")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| String::from("0-0"));
                    Ok(Some(StreamData {
                        entries,
                        groups: Self::parse_stream_groups(&doc)?,
                        added,
                        max_deleted,
                    }))
                } else {
                    Err(String::from("wrong type"))
                }
            }
            None => Ok(None),
        }
    }

    fn write_stream(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        data: &StreamData,
    ) -> std::result::Result<(), String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let mut groups = serde_json::Map::with_capacity(data.groups.len());
        for (name, group) in data.groups.iter() {
            let mut consumers = serde_json::Map::with_capacity(group.consumers.len());
            for (consumer, state) in group.consumers.iter() {
                consumers.insert(
                    consumer.clone(),
                    serde_json::json!({"delivered_ms": state.delivered_ms, "seen_ms": state.seen_ms}),
                );
            }
            groups.insert(
                name.clone(),
                serde_json::json!({
                    "last": group.last,
                    "pending": group.pending,
                    "read": group.read,
                    "consumers": consumers,
                }),
            );
        }
        let raw = serde_json::to_vec(&serde_json::json!({
            "entries": data.entries,
            "groups": groups,
            "added": data.added,
            "max_deleted": data.max_deleted,
        }))
        .map_err(|_| String::from("wrong type"))?;
        check_collection_bytes(&raw)?;
        self.manager.put(txn, record, raw);
        Ok(())
    }

    fn parse_stream_entries(
        value: &serde_json::Value,
    ) -> std::result::Result<StreamEntries, String> {
        let items = value.as_array().ok_or_else(|| String::from("wrong type"))?;
        let mut entries = Vec::with_capacity(items.len());
        for item in items {
            let pair = item.as_array().ok_or_else(|| String::from("wrong type"))?;
            let id =
                pair.first().and_then(|v| v.as_str()).ok_or_else(|| String::from("wrong type"))?;
            Self::parse_stream_id(id).map_err(|_| String::from("wrong type"))?;
            let fields =
                pair.get(1).and_then(|v| v.as_array()).ok_or_else(|| String::from("wrong type"))?;
            let mut record = Vec::with_capacity(fields.len());
            for field in fields {
                let pair = field.as_array().ok_or_else(|| String::from("wrong type"))?;
                let name = pair
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| String::from("wrong type"))?;
                let data = pair
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| String::from("wrong type"))?;
                record.push((name.to_string(), data.to_string()));
            }
            entries.push((id.to_string(), record));
        }
        Ok(entries)
    }

    fn parse_stream_groups(
        value: &serde_json::Value,
    ) -> std::result::Result<std::collections::HashMap<String, StreamGroup>, String> {
        let mut groups = std::collections::HashMap::new();
        let Some(raw) = value.get("groups") else {
            return Ok(groups);
        };
        let map = raw.as_object().ok_or_else(|| String::from("wrong type"))?;
        for (name, group) in map {
            let last = group
                .get("last")
                .and_then(|v| v.as_str())
                .ok_or_else(|| String::from("wrong type"))?;
            Self::parse_stream_id(last).map_err(|_| String::from("wrong type"))?;
            let pending_raw = group
                .get("pending")
                .and_then(|v| v.as_array())
                .ok_or_else(|| String::from("wrong type"))?;
            let mut pending = Vec::with_capacity(pending_raw.len());
            for item in pending_raw {
                let entry = item.as_array().ok_or_else(|| String::from("wrong type"))?;
                let id = entry
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| String::from("wrong type"))?;
                Self::parse_stream_id(id).map_err(|_| String::from("wrong type"))?;
                let consumer = entry
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| String::from("wrong type"))?;
                let deliveries = entry
                    .get(2)
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| String::from("wrong type"))?;
                let delivered_at = entry.get(3).and_then(|v| v.as_u64()).unwrap_or(0);
                pending.push((id.to_string(), consumer.to_string(), deliveries, delivered_at));
            }
            let read = group.get("read").and_then(|v| v.as_u64()).unwrap_or(0);
            let mut consumers = std::collections::HashMap::new();
            if let Some(raw) = group.get("consumers").and_then(|v| v.as_object()) {
                for (name, state) in raw {
                    let delivered_ms = state.get("delivered_ms").and_then(|v| v.as_u64());
                    let seen_ms = state.get("seen_ms").and_then(|v| v.as_u64());
                    consumers.insert(name.clone(), ConsumerState { delivered_ms, seen_ms });
                }
            }
            groups.insert(
                name.clone(),
                StreamGroup { last: last.to_string(), pending, read, consumers },
            );
        }
        Ok(groups)
    }

    fn parse_stream_id(text: &str) -> std::result::Result<(u64, u64), String> {
        let (ms, seq) = text.split_once('-').ok_or_else(|| String::from("syntax"))?;
        let ms = ms.parse::<u64>().map_err(|_| String::from("syntax"))?;
        let seq = seq.parse::<u64>().map_err(|_| String::from("syntax"))?;
        Ok((ms, seq))
    }

    fn stream_now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn command_bytes(command: &RespCommand) -> u64 {
        command.args.iter().map(|arg| arg.len() as u64).sum()
    }

    fn multi_command_memory_bytes(command: &RespCommand) -> Option<usize> {
        let command_and_args = command
            .args
            .len()
            .checked_mul(std::mem::size_of::<Vec<u8>>())?
            .checked_add(std::mem::size_of::<RespCommand>() * 2)?
            .checked_add(command.name.len())?;
        command.args.iter().try_fold(command_and_args, |total, arg| total.checked_add(arg.len()))
    }

    fn stream_last(entries: &[(String, Vec<(String, String)>)]) -> (u64, u64) {
        entries.last().and_then(|(id, _)| Self::parse_stream_id(id).ok()).unwrap_or((0, 0))
    }

    fn assign_stream_id(
        entries: &[(String, Vec<(String, String)>)],
        raw: &[u8],
    ) -> std::result::Result<String, String> {
        let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
        let last = Self::stream_last(entries);
        if text == "*" {
            let now = Self::stream_now_ms();
            let next = if now > last.0 {
                (now, 0)
            } else {
                (last.0, last.1.checked_add(1).ok_or_else(|| String::from("overflow"))?)
            };
            if next <= (0, 0) {
                return Err(String::from("overflow"));
            }
            return Ok(format!("{}-{}", next.0, next.1));
        }
        if let Some(ms_text) = text.strip_suffix("-*") {
            let ms = ms_text.parse::<u64>().map_err(|_| String::from("syntax"))?;
            let next = if ms > last.0 {
                (ms, 0)
            } else if ms == last.0 {
                (ms, last.1.checked_add(1).ok_or_else(|| String::from("overflow"))?)
            } else {
                return Err(String::from("id smaller than last"));
            };
            return Ok(format!("{}-{}", next.0, next.1));
        }
        let next = Self::parse_stream_id(text)?;
        if next <= (0, 0) || next <= last {
            return Err(String::from("id smaller than last"));
        }
        Ok(text.to_string())
    }

    fn trim_stream(
        data: &mut StreamData,
        max_len: Option<usize>,
        min_id: Option<(u64, u64)>,
    ) -> usize {
        let before = data.entries.len();
        if let Some(max) = max_len {
            if data.entries.len() > max {
                let drained: Vec<(String, StreamFields)> =
                    data.entries.drain(0..data.entries.len() - max).collect();
                for (id, _) in drained {
                    Self::note_deleted(data, &id);
                }
            }
        }
        if let Some(floor) = min_id {
            let mut removed = Vec::new();
            data.entries.retain(|(id, _)| {
                let keep = Self::parse_stream_id(id).is_ok_and(|v| v >= floor);
                if !keep {
                    removed.push(id.clone());
                }
                keep
            });
            for id in removed {
                Self::note_deleted(data, &id);
            }
        }
        before - data.entries.len()
    }

    fn note_deleted(data: &mut StreamData, id: &str) {
        let current = Self::parse_stream_id(&data.max_deleted).unwrap_or((0, 0));
        if let Ok(next) = Self::parse_stream_id(id) {
            if next > current {
                data.max_deleted = id.to_string();
            }
        }
    }

    fn parse_trim(args: &[Vec<u8>], index: &mut usize) -> std::result::Result<TrimSpec, String> {
        let mut max_len: Option<usize> = None;
        let mut min_id: Option<(u64, u64)> = None;
        while *index < args.len() {
            let name = std::str::from_utf8(&args[*index]).map_err(|_| String::from("syntax"))?;
            match name.to_ascii_uppercase().as_str() {
                "MAXLEN" => {
                    *index += 1;
                    let mut raw = args.get(*index).ok_or_else(|| String::from("syntax"))?;
                    if raw.eq_ignore_ascii_case(b"~") || raw == b"=" {
                        *index += 1;
                        raw = args.get(*index).ok_or_else(|| String::from("syntax"))?;
                    }
                    let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
                    max_len = Some(text.parse::<usize>().map_err(|_| String::from("syntax"))?);
                    *index += 1;
                }
                "MINID" => {
                    *index += 1;
                    let mut raw = args.get(*index).ok_or_else(|| String::from("syntax"))?;
                    if raw.eq_ignore_ascii_case(b"~") || raw == b"=" {
                        *index += 1;
                        raw = args.get(*index).ok_or_else(|| String::from("syntax"))?;
                    }
                    let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
                    min_id = Some(Self::parse_stream_id(text)?);
                    *index += 1;
                }
                _ => break,
            }
        }
        Ok((max_len, min_id))
    }

    fn xadd(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<String, String> {
        if args.len() < 2 {
            return Err(String::from("wrong args"));
        }
        let mut index = 1;
        let (max_len, min_id) = Self::parse_trim(args, &mut index)?;
        let id_raw = args.get(index).ok_or_else(|| String::from("wrong args"))?;
        index += 1;
        let fields = args.get(index..).ok_or_else(|| String::from("wrong args"))?;
        if fields.is_empty() || !fields.len().is_multiple_of(2) {
            return Err(String::from("wrong args"));
        }
        let mut data = self.read_stream(txn, &args[0])?.unwrap_or_default();
        let id = Self::assign_stream_id(&data.entries, id_raw)?;
        let mut record = Vec::with_capacity(fields.len() / 2);
        for pair in fields.as_chunks::<2>().0 {
            let field =
                std::str::from_utf8(&pair[0]).map_err(|_| String::from("wrong type"))?.to_string();
            let value =
                std::str::from_utf8(&pair[1]).map_err(|_| String::from("wrong type"))?.to_string();
            record.push((field, value));
        }
        data.entries.push((id.clone(), record));
        data.added += 1;
        Self::trim_stream(&mut data, max_len, min_id);
        self.write_stream(txn, &args[0], &data)?;
        Ok(id)
    }

    fn encode_stream_entries(entries: &[&(String, Vec<(String, String)>)]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", entries.len()).into_bytes();
        for (id, fields) in entries {
            out.extend_from_slice(
                format!("*2\r\n${}\r\n{id}\r\n*{}\r\n", id.len(), fields.len() * 2).as_bytes(),
            );
            for (field, value) in fields.iter() {
                out.extend_from_slice(format!("${}\r\n{field}\r\n", field.len()).as_bytes());
                out.extend_from_slice(format!("${}\r\n{value}\r\n", value.len()).as_bytes());
            }
        }
        out
    }

    fn xrange(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
        reverse: bool,
    ) -> std::result::Result<Vec<u8>, String> {
        if args.len() < 3 {
            return Err(String::from("wrong args"));
        }
        let parse_bound = |raw: &[u8]| -> std::result::Result<(Option<(u64, u64)>, bool), String> {
            let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
            if text == "-" || text == "+" {
                return Ok((None, false));
            }
            let (exclusive, id) = match text.strip_prefix('(') {
                Some(rest) => (true, rest),
                None => (false, text),
            };
            Ok((Some(Self::parse_stream_id(id)?), exclusive))
        };
        let (low, low_exclusive) =
            if reverse { parse_bound(&args[2])? } else { parse_bound(&args[1])? };
        let (high, high_exclusive) =
            if reverse { parse_bound(&args[1])? } else { parse_bound(&args[2])? };
        let mut count: Option<usize> = None;
        let mut index = 3;
        while index < args.len() {
            let name = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
            if !name.eq_ignore_ascii_case("COUNT") {
                return Err(String::from("syntax"));
            }
            index += 1;
            let raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
            let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
            count = Some(text.parse::<usize>().map_err(|_| String::from("syntax"))?.min(10_000));
            index += 1;
        }
        let data = self.read_stream(txn, &args[0])?.unwrap_or_default();
        let mut picked: Vec<&(String, Vec<(String, String)>)> = Vec::new();
        let mut ordered: Vec<&(String, Vec<(String, String)>)> = data.entries.iter().collect();
        if reverse {
            ordered.reverse();
        }
        for entry in ordered {
            let id = Self::parse_stream_id(&entry.0).map_err(|_| String::from("corrupt"))?;
            let past_low = match low {
                None => true,
                Some(bound) => {
                    if low_exclusive {
                        id > bound
                    } else {
                        id >= bound
                    }
                }
            };
            if !past_low {
                continue;
            }
            let before_high = match high {
                None => true,
                Some(bound) => {
                    if high_exclusive {
                        id < bound
                    } else {
                        id <= bound
                    }
                }
            };
            if !before_high {
                if reverse {
                    break;
                }
                continue;
            }
            picked.push(entry);
            if count.is_some_and(|n| picked.len() >= n) {
                break;
            }
        }
        Ok(Self::encode_stream_entries(&picked))
    }

    fn xtrim(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<i64, String> {
        if args.len() < 2 {
            return Err(String::from("wrong args"));
        }
        let mut index = 1;
        let (max_len, min_id) = Self::parse_trim(args, &mut index)?;
        if index != args.len() || (max_len.is_none() && min_id.is_none()) {
            return Err(String::from("syntax"));
        }
        let mut data = self.read_stream(txn, &args[0])?.unwrap_or_default();
        let removed = Self::trim_stream(&mut data, max_len, min_id);
        self.write_stream(txn, &args[0], &data)?;
        Ok(removed as i64)
    }

    fn xread_options(
        args: &[Vec<u8>],
    ) -> std::result::Result<(Option<usize>, Option<u64>, usize), String> {
        let mut count: Option<usize> = None;
        let mut block: Option<u64> = None;
        let mut index = 0;
        while index < args.len() {
            let name = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
            if name.eq_ignore_ascii_case("BLOCK") {
                index += 1;
                let raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
                let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
                block = Some(
                    text.parse::<u64>()
                        .map_err(|_| String::from("timeout is not an integer or out of range"))?,
                );
                index += 1;
                continue;
            }
            if name.eq_ignore_ascii_case("COUNT") {
                index += 1;
                let raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
                let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
                count =
                    Some(text.parse::<usize>().map_err(|_| String::from("syntax"))?.min(10_000));
                index += 1;
                continue;
            }
            break;
        }
        let streams = args.get(index).ok_or_else(|| String::from("syntax"))?;
        if !streams.eq_ignore_ascii_case(b"STREAMS") {
            return Err(String::from("syntax"));
        }
        Ok((count, block, index + 1))
    }

    fn xread_select(
        &self,
        txn: &mut Transaction,
        count: Option<usize>,
        rest: &[Vec<u8>],
    ) -> std::result::Result<Vec<Vec<u8>>, String> {
        if rest.is_empty() || !rest.len().is_multiple_of(2) {
            return Err(String::from("syntax"));
        }
        let half = rest.len() / 2;
        let mut out = Vec::new();
        for pair in 0..half {
            let key = &rest[pair];
            let last =
                std::str::from_utf8(&rest[half + pair]).map_err(|_| String::from("syntax"))?;
            if last == "$" {
                continue;
            }
            let bound = Self::parse_stream_id(last)?;
            let data = self.read_stream(txn, key)?.unwrap_or_default();
            let mut picked: Vec<&(String, Vec<(String, String)>)> = Vec::new();
            for entry in data.entries.iter() {
                let id = Self::parse_stream_id(&entry.0).map_err(|_| String::from("corrupt"))?;
                if id > bound {
                    picked.push(entry);
                    if count.is_some_and(|n| picked.len() >= n) {
                        break;
                    }
                }
            }
            if !picked.is_empty() {
                let name = String::from_utf8_lossy(key).into_owned();
                let mut head = format!("*2\r\n${}\r\n{name}\r\n", name.len()).into_bytes();
                head.extend_from_slice(&Self::encode_stream_entries(&picked));
                out.push(head);
            }
        }
        Ok(out)
    }

    fn encode_xread(out: Vec<Vec<u8>>) -> Vec<u8> {
        if out.is_empty() {
            return encode_nil_array();
        }
        let mut reply = format!("*{}\r\n", out.len()).into_bytes();
        for group in out {
            reply.extend_from_slice(&group);
        }
        reply
    }

    fn xread(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        let (count, block, keys_at) = Self::xread_options(args)?;
        if block.is_some() {
            return Err(String::from("blocking reads unsupported"));
        }
        let rest = args.get(keys_at..).ok_or_else(|| String::from("syntax"))?;
        Ok(Self::encode_xread(self.xread_select(txn, count, rest)?))
    }

    fn xread_waits(args: &[Vec<u8>]) -> bool {
        let mut index = 0;
        while index < args.len() {
            let Ok(name) = std::str::from_utf8(&args[index]) else {
                return false;
            };
            if name.eq_ignore_ascii_case("STREAMS") {
                return false;
            }
            if name.eq_ignore_ascii_case("BLOCK") {
                return true;
            }
            index += 1;
        }
        false
    }

    async fn xread_blocking(&self, args: Vec<Vec<u8>>) -> Vec<u8> {
        let (count, block, keys_at) = match Self::xread_options(&args) {
            Ok(parsed) => parsed,
            Err(e) => return encode_error(e),
        };
        let Some(block_ms) = block else {
            return encode_error(String::from("syntax"));
        };
        let deadline = if block_ms == 0 {
            None
        } else {
            Some(std::time::Instant::now() + std::time::Duration::from_millis(block_ms))
        };
        let mut waiter = ryme_txn::subscribe_commits();
        loop {
            let mut txn = self.manager.begin();
            let rest = args.get(keys_at..).unwrap_or(&[]);
            match self.xread_select(&mut txn, count, rest) {
                Err(e) => return encode_error(e),
                Ok(groups) if !groups.is_empty() => return Self::encode_xread(groups),
                Ok(_) => {}
            }
            drop(txn);
            if deadline.is_some_and(|end| std::time::Instant::now() >= end) {
                return encode_nil_array();
            }
            let remaining =
                deadline.map(|end| end.saturating_duration_since(std::time::Instant::now()));
            Self::wait_for_commit(&mut waiter, remaining).await;
        }
    }

    fn scan_cursor(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        if args.is_empty() {
            return Err(String::from("wrong args"));
        }
        let cursor = std::str::from_utf8(&args[0]).map_err(|_| String::from("syntax"))?;
        let mut after = if cursor == "0" {
            Vec::new()
        } else {
            hex_decode(cursor).ok_or_else(|| String::from("syntax"))?
        };
        let mut pattern: Option<String> = None;
        let mut count = 10i64;
        let mut index = 1;
        while index < args.len() {
            let name = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
            match name.to_ascii_uppercase().as_str() {
                "MATCH" => {
                    index += 1;
                    let pat = args.get(index).ok_or_else(|| String::from("syntax"))?;
                    pattern = Some(
                        std::str::from_utf8(pat).map_err(|_| String::from("syntax"))?.to_string(),
                    );
                    index += 1;
                }
                "COUNT" => {
                    index += 1;
                    let raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
                    let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
                    count = text.parse::<i64>().map_err(|_| String::from("syntax"))?;
                    index += 1;
                }
                _ => return Err(String::from("syntax")),
            }
        }
        let want = count.clamp(1, RESP_MAX_SCAN_RESULT_ITEMS as i64) as usize;
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut key_bytes = 0usize;
        let mut exhausted = false;
        let mut drained = true;
        while keys.len() < want && !exhausted {
            let rows = self
                .manager
                .scan_after(
                    txn,
                    &self.tenant,
                    &self.database,
                    KV_TABLE,
                    &after,
                    RESP_SCAN_PAGE_ITEMS,
                )
                .map_err(|e| e.to_string())?;
            if rows.len() < RESP_SCAN_PAGE_ITEMS {
                exhausted = true;
            }
            if rows.is_empty() {
                break;
            }
            for (key, _) in rows {
                after = key.clone();
                let matched = pattern.as_ref().is_none_or(|pat| {
                    glob_match(pat.as_bytes(), String::from_utf8_lossy(&key).as_bytes())
                });
                if matched {
                    key_bytes = next_scan_result_key_bytes(keys.len(), key_bytes, key.len())?;
                    keys.try_reserve(1)
                        .map_err(|_| String::from("scan result allocation failed"))?;
                    keys.push(key);
                    if keys.len() >= want {
                        drained = false;
                        break;
                    }
                }
            }
        }
        let cursor = if exhausted && drained { String::from("0") } else { hex_encode(&after) };
        let mut out = format!("*2\r\n${}\r\n{cursor}\r\n", cursor.len()).into_bytes();
        out.extend_from_slice(&encode_array(keys.into_iter().map(Some).collect()));
        Ok(out)
    }

    fn read_hll(
        &self,
        txn: &mut Transaction,
        key: &[u8],
    ) -> std::result::Result<Option<Vec<u8>>, String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        match self.manager.get(txn, &record).map_err(|error| error.to_string())? {
            Some(value) if value.is_empty() => Ok(Some(vec![0; HLL_REGISTERS])),
            Some(value) => {
                check_collection_bytes(&value)?;
                let registers: Vec<u8> =
                    serde_json::from_slice(&value).map_err(|_| String::from("wrong type"))?;
                if registers.len() != HLL_REGISTERS {
                    return Err(String::from("wrong type"));
                }
                Ok(Some(registers))
            }
            None => Ok(None),
        }
    }

    fn write_hll(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        registers: &[u8],
    ) -> std::result::Result<(), String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let raw = serde_json::to_vec(registers).map_err(|_| String::from("wrong type"))?;
        check_collection_bytes(&raw)?;
        self.manager.put(txn, record, raw);
        Ok(())
    }

    fn pfadd(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<bool, String> {
        let mut registers = self.read_hll(txn, &args[0])?.unwrap_or(vec![0; HLL_REGISTERS]);
        let mut changed = false;
        for member in &args[1..] {
            let (index, rho) = hll_position(member);
            if registers[index] < rho {
                registers[index] = rho;
                changed = true;
            }
        }
        self.write_hll(txn, &args[0], &registers)?;
        Ok(changed)
    }

    fn pfcount(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<u64, String> {
        let mut merged = vec![0; HLL_REGISTERS];
        for key in args {
            if let Some(registers) = self.read_hll(txn, key)? {
                for (slot, value) in merged.iter_mut().zip(registers.iter()) {
                    if *slot < *value {
                        *slot = *value;
                    }
                }
            }
        }
        Ok(hll_estimate(&merged))
    }

    fn pfmerge(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<(), String> {
        let mut merged = vec![0; HLL_REGISTERS];
        for key in &args[1..] {
            if let Some(registers) = self.read_hll(txn, key)? {
                for (slot, value) in merged.iter_mut().zip(registers.iter()) {
                    if *slot < *value {
                        *slot = *value;
                    }
                }
            }
        }
        self.write_hll(txn, &args[0], &merged)?;
        Ok(())
    }

    fn read_geo(
        &self,
        txn: &mut Transaction,
        key: &[u8],
    ) -> std::result::Result<Option<std::collections::HashMap<String, GeoPoint>>, String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        match self.manager.get(txn, &record).map_err(|error| error.to_string())? {
            Some(value) if value.is_empty() => Ok(Some(std::collections::HashMap::new())),
            Some(value) => {
                check_collection_bytes(&value)?;
                let doc: serde_json::Value =
                    serde_json::from_slice(&value).map_err(|_| String::from("wrong type"))?;
                if let Ok(points) = Self::parse_geo_points(&doc) {
                    return Ok(Some(points));
                }
                let scores: std::collections::HashMap<String, f64> =
                    serde_json::from_value(doc).map_err(|_| String::from("wrong type"))?;
                Ok(Some(
                    scores
                        .into_iter()
                        .map(|(member, score)| (member, geo_decode_score(score as u64)))
                        .collect(),
                ))
            }
            None => Ok(None),
        }
    }

    fn parse_geo_points(
        value: &serde_json::Value,
    ) -> std::result::Result<std::collections::HashMap<String, GeoPoint>, String> {
        let map = value.as_object().ok_or_else(|| String::from("wrong type"))?;
        let mut points = std::collections::HashMap::with_capacity(map.len());
        for (member, point) in map {
            let pair = point.as_array().ok_or_else(|| String::from("wrong type"))?;
            let lon =
                pair.first().and_then(|v| v.as_f64()).ok_or_else(|| String::from("wrong type"))?;
            let lat =
                pair.get(1).and_then(|v| v.as_f64()).ok_or_else(|| String::from("wrong type"))?;
            points.insert(member.clone(), (lon, lat));
        }
        Ok(points)
    }

    fn write_geo(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        points: &std::collections::HashMap<String, GeoPoint>,
    ) -> std::result::Result<(), String> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let raw = serde_json::to_vec(points).map_err(|_| String::from("wrong type"))?;
        check_collection_bytes(&raw)?;
        self.manager.put(txn, record, raw);
        Ok(())
    }

    fn geoadd(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<i64, String> {
        if args.len() < 4 || !(args.len() - 1).is_multiple_of(3) {
            return Err(String::from("wrong args"));
        }
        let mut points = self.read_geo(txn, &args[0])?.unwrap_or_default();
        let mut added = 0;
        for triple in args[1..].as_chunks::<3>().0 {
            let lon = geo_parse_coordinate(&triple[0])?;
            let lat = geo_parse_coordinate(&triple[1])?;
            geo_validate(lon, lat)?;
            let member = std::str::from_utf8(&triple[2])
                .map_err(|_| String::from("wrong type"))?
                .to_string();
            let stored = geo_decode_score(geo_hash_score(lon, lat));
            if points.insert(member, stored).is_none() {
                added += 1;
            }
        }
        self.write_geo(txn, &args[0], &points)?;
        Ok(added)
    }

    fn geodist(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Option<String>, String> {
        if args.len() < 3 || args.len() > 4 {
            return Err(String::from("wrong args"));
        }
        let factor = match args.get(3) {
            Some(unit) => {
                let name = std::str::from_utf8(unit).map_err(|_| String::from("syntax"))?;
                geo_unit_factor(name).ok_or_else(|| String::from("syntax"))?
            }
            None => 1.0,
        };
        let points = self.read_geo(txn, &args[0])?.unwrap_or_default();
        let first = String::from_utf8_lossy(&args[1]).into_owned();
        let second = String::from_utf8_lossy(&args[2]).into_owned();
        match (points.get(&first), points.get(&second)) {
            (Some(a), Some(b)) => {
                Ok(Some(geo_format_distance(geo_haversine_meters(*a, *b), factor)))
            }
            _ => Ok(None),
        }
    }

    fn geopos(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        if args.len() < 2 {
            return Err(String::from("wrong args"));
        }
        let points = self.read_geo(txn, &args[0])?.unwrap_or_default();
        let mut out = format!("*{}\r\n", args.len() - 1).into_bytes();
        for member in &args[1..] {
            let name = String::from_utf8_lossy(member).into_owned();
            match points.get(&name) {
                Some((lon, lat)) => {
                    let lon_text = format_float(*lon);
                    let lat_text = format_float(*lat);
                    out.extend_from_slice(
                        format!(
                            "*2\r\n${}\r\n{lon_text}\r\n${}\r\n{lat_text}\r\n",
                            lon_text.len(),
                            lat_text.len()
                        )
                        .as_bytes(),
                    );
                }
                None => out.extend_from_slice(b"*-1\r\n"),
            }
        }
        Ok(out)
    }

    fn geosearch(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        if args.len() < 4 {
            return Err(String::from("wrong args"));
        }
        let points = self.read_geo(txn, &args[0])?.unwrap_or_default();
        let mut index = 1;
        let origin = |args: &[Vec<u8>],
                      index: &mut usize|
         -> std::result::Result<GeoPoint, String> {
            let kind = std::str::from_utf8(args.get(*index).ok_or_else(|| String::from("syntax"))?)
                .map_err(|_| String::from("syntax"))?;
            if kind.eq_ignore_ascii_case("FROMMEMBER") {
                *index += 1;
                let member = args.get(*index).ok_or_else(|| String::from("syntax"))?;
                *index += 1;
                let name = String::from_utf8_lossy(member).into_owned();
                return points.get(&name).copied().ok_or_else(|| String::from("not found"));
            }
            if kind.eq_ignore_ascii_case("FROMLONLAT") {
                *index += 1;
                let lon = args.get(*index).ok_or_else(|| String::from("syntax"))?;
                *index += 1;
                let lat = args.get(*index).ok_or_else(|| String::from("syntax"))?;
                *index += 1;
                let lon = geo_parse_coordinate(lon)?;
                let lat = geo_parse_coordinate(lat)?;
                geo_validate(lon, lat)?;
                return Ok((lon, lat));
            }
            Err(String::from("syntax"))
        };
        let center = origin(args, &mut index)?;
        let shape = std::str::from_utf8(args.get(index).ok_or_else(|| String::from("syntax"))?)
            .map_err(|_| String::from("syntax"))?;
        index += 1;
        let (shape, unit) = if shape.eq_ignore_ascii_case("BYRADIUS") {
            let distance = args.get(index).ok_or_else(|| String::from("syntax"))?;
            index += 1;
            let unit = args.get(index).ok_or_else(|| String::from("syntax"))?;
            index += 1;
            let name = std::str::from_utf8(unit).map_err(|_| String::from("syntax"))?;
            let factor = geo_unit_factor(name).ok_or_else(|| String::from("syntax"))?;
            let text = std::str::from_utf8(distance).map_err(|_| String::from("syntax"))?;
            let value = text
                .parse::<f64>()
                .ok()
                .filter(|v| *v >= 0.0)
                .ok_or_else(|| String::from("syntax"))?;
            (GeoShape::Radius(value * factor), factor)
        } else if shape.eq_ignore_ascii_case("BYBOX") {
            let width = args.get(index).ok_or_else(|| String::from("syntax"))?;
            index += 1;
            let height = args.get(index).ok_or_else(|| String::from("syntax"))?;
            index += 1;
            let unit = args.get(index).ok_or_else(|| String::from("syntax"))?;
            index += 1;
            let name = std::str::from_utf8(unit).map_err(|_| String::from("syntax"))?;
            let factor = geo_unit_factor(name).ok_or_else(|| String::from("syntax"))?;
            let width = std::str::from_utf8(width)
                .map_err(|_| String::from("syntax"))?
                .parse::<f64>()
                .ok()
                .filter(|v| *v >= 0.0)
                .ok_or_else(|| String::from("syntax"))?;
            let height = std::str::from_utf8(height)
                .map_err(|_| String::from("syntax"))?
                .parse::<f64>()
                .ok()
                .filter(|v| *v >= 0.0)
                .ok_or_else(|| String::from("syntax"))?;
            (GeoShape::Box(width * factor, height * factor), factor)
        } else {
            return Err(String::from("syntax"));
        };
        let mut sort = GeoSort::Score;
        let mut count: Option<usize> = None;
        let mut with_dist = false;
        let mut with_coord = false;
        while index < args.len() {
            let name = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
            match name.to_ascii_uppercase().as_str() {
                "ASC" => {
                    sort = GeoSort::DistanceAsc;
                    index += 1;
                }
                "DESC" => {
                    sort = GeoSort::DistanceDesc;
                    index += 1;
                }
                "COUNT" => {
                    index += 1;
                    let raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
                    let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
                    count = Some(
                        text.parse::<usize>().map_err(|_| String::from("syntax"))?.min(10_000),
                    );
                    index += 1;
                }
                "WITHDIST" => {
                    with_dist = true;
                    index += 1;
                }
                "WITHCOORD" => {
                    with_coord = true;
                    index += 1;
                }
                _ => return Err(String::from("syntax")),
            }
        }
        let found = Self::geo_search_run(center, &shape, &points, sort, count);
        Ok(Self::encode_geo_results(&points, found, with_dist, with_coord, false, unit))
    }

    fn geo_search_run<'a>(
        center: GeoPoint,
        shape: &GeoShape,
        points: &'a std::collections::HashMap<String, GeoPoint>,
        sort: GeoSort,
        count: Option<usize>,
    ) -> Vec<(&'a String, f64)> {
        let mut found: Vec<(&String, f64)> = Vec::new();
        for (member, point) in points.iter() {
            let inside = match shape {
                GeoShape::Radius(radius) => geo_haversine_meters(center, *point) <= *radius,
                GeoShape::Box(width, height) => {
                    let to_radians = std::f64::consts::PI / 180.0;
                    let dx = (point.0 - center.0) * to_radians * center.1.cos() * EARTH_RADIUS_M;
                    let dy = (point.1 - center.1) * to_radians * EARTH_RADIUS_M;
                    dx.abs() * 2.0 <= *width && dy.abs() * 2.0 <= *height
                }
            };
            if inside {
                found.push((member, geo_haversine_meters(center, *point)));
            }
        }
        match sort {
            GeoSort::Score => found.sort_by(|a, b| {
                let left = points.get(a.0).map(|p| geo_hash_score(p.0, p.1)).unwrap_or(0);
                let right = points.get(b.0).map(|p| geo_hash_score(p.0, p.1)).unwrap_or(0);
                left.cmp(&right).then_with(|| a.0.cmp(b.0))
            }),
            GeoSort::DistanceAsc => {
                found.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(b.0)));
            }
            GeoSort::DistanceDesc => {
                found.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| b.0.cmp(a.0)));
            }
        }
        if let Some(n) = count {
            found.truncate(n);
        }
        found
    }
    fn encode_geo_results(
        points: &std::collections::HashMap<String, GeoPoint>,
        found: Vec<(&String, f64)>,
        with_dist: bool,
        with_coord: bool,
        with_hash: bool,
        unit: f64,
    ) -> Vec<u8> {
        let mut out = format!("*{}\r\n", found.len()).into_bytes();
        for (member, distance) in found {
            if !with_dist && !with_coord && !with_hash {
                out.extend_from_slice(format!("${}\r\n{member}\r\n", member.len()).as_bytes());
                continue;
            }
            let mut parts = 1;
            if with_dist {
                parts += 1;
            }
            if with_hash {
                parts += 1;
            }
            if with_coord {
                parts += 1;
            }
            out.extend_from_slice(
                format!("*{parts}\r\n${}\r\n{member}\r\n", member.len()).as_bytes(),
            );
            if with_dist {
                let text = geo_format_distance(distance, unit);
                out.extend_from_slice(format!("${}\r\n{text}\r\n", text.len()).as_bytes());
            }
            if with_hash {
                let point = points.get(member).copied().unwrap_or((0.0, 0.0));
                let hash = geo_hash_score(point.0, point.1).to_string();
                out.extend_from_slice(format!(":{hash}\r\n").as_bytes());
            }
            if with_coord {
                let point = points.get(member).copied().unwrap_or((0.0, 0.0));
                let lon_text = format_float(point.0);
                let lat_text = format_float(point.1);
                out.extend_from_slice(
                    format!(
                        "*2\r\n${}\r\n{lon_text}\r\n${}\r\n{lat_text}\r\n",
                        lon_text.len(),
                        lat_text.len()
                    )
                    .as_bytes(),
                );
            }
        }
        out
    }

    fn nogroup(key: &[u8], group: &[u8]) -> String {
        format!(
            "NOGROUP No such key '{}' or consumer group '{}' in XGROUP subcommand",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(group)
        )
    }

    fn xgroup(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        if args.len() < 2 {
            return Err(String::from("wrong args"));
        }
        let name = std::str::from_utf8(&args[0]).map_err(|_| String::from("syntax"))?;
        match name.to_ascii_uppercase().as_str() {
            "CREATE" => {
                if args.len() < 4 || args.len() > 5 {
                    return Err(String::from("wrong args"));
                }
                let mkstream =
                    args.get(4).is_some_and(|flag| flag.eq_ignore_ascii_case(b"MKSTREAM"));
                if args.len() == 5 && !mkstream {
                    return Err(String::from("syntax"));
                }
                let mut data = match self.read_stream(txn, &args[1])? {
                    Some(data) => data,
                    None if mkstream => StreamData::default(),
                    None => return Err(String::from("no such key")),
                };
                let group_name = String::from_utf8_lossy(&args[2]).into_owned();
                if data.groups.contains_key(&group_name) {
                    return Err(String::from("BUSYGROUP Consumer Group name already exists"));
                }
                let id_text = std::str::from_utf8(&args[3]).map_err(|_| String::from("syntax"))?;
                let last = if id_text == "$" {
                    data.entries
                        .last()
                        .map(|(id, _)| id.clone())
                        .ok_or_else(|| String::from("invalid ID"))?
                } else {
                    let id = Self::parse_stream_id(id_text)?;
                    if id == (0, 0) {
                        String::from("0-0")
                    } else {
                        id_text.to_string()
                    }
                };
                data.groups.insert(
                    group_name,
                    StreamGroup {
                        last,
                        pending: Vec::new(),
                        read: 0,
                        consumers: std::collections::HashMap::new(),
                    },
                );
                self.write_stream(txn, &args[1], &data)?;
                Ok(encode_simple("OK"))
            }
            "DESTROY" => {
                if args.len() != 3 {
                    return Err(String::from("wrong args"));
                }
                let mut data = self.read_stream(txn, &args[1])?.unwrap_or_default();
                let group_name = String::from_utf8_lossy(&args[2]).into_owned();
                if data.groups.remove(&group_name).is_none() {
                    return Err(Self::nogroup(&args[1], &args[2]));
                }
                self.write_stream(txn, &args[1], &data)?;
                Ok(encode_integer(1))
            }
            "SETID" => {
                if args.len() != 4 {
                    return Err(String::from("wrong args"));
                }
                let mut data = self.read_stream(txn, &args[1])?.unwrap_or_default();
                let group_name = String::from_utf8_lossy(&args[2]).into_owned();
                let Some(group) = data.groups.get_mut(&group_name) else {
                    return Err(Self::nogroup(&args[1], &args[2]));
                };
                let id_text = std::str::from_utf8(&args[3]).map_err(|_| String::from("syntax"))?;
                group.last = if id_text == "$" {
                    data.entries.last().map(|(id, _)| id.clone()).unwrap_or(String::from("0-0"))
                } else {
                    Self::parse_stream_id(id_text)?;
                    id_text.to_string()
                };
                self.write_stream(txn, &args[1], &data)?;
                Ok(encode_simple("OK"))
            }
            _ => Err(String::from("syntax")),
        }
    }

    fn parse_xreadgroup(
        args: &[Vec<u8>],
    ) -> std::result::Result<(Option<usize>, Option<u64>, usize), String> {
        if args.len() < 5 {
            return Err(String::from("wrong args"));
        }
        if !args[0].eq_ignore_ascii_case(b"GROUP") {
            return Err(String::from("syntax"));
        }
        let (count, block, index) = Self::xread_options(&args[3..])?;
        let index = index + 3;
        let rest = args.get(index..).ok_or_else(|| String::from("syntax"))?;
        if rest.is_empty() || !rest.len().is_multiple_of(2) {
            return Err(String::from("syntax"));
        }
        Ok((count, block, index))
    }

    fn xreadgroup_deliver(
        &self,
        txn: &mut Transaction,
        count: Option<usize>,
        group_name: &[u8],
        consumer: &[u8],
        rest: &[Vec<u8>],
    ) -> std::result::Result<Vec<Vec<u8>>, String> {
        if rest.is_empty() || !rest.len().is_multiple_of(2) {
            return Err(String::from("syntax"));
        }
        let half = rest.len() / 2;
        let group_text = String::from_utf8_lossy(group_name).into_owned();
        let consumer_text = String::from_utf8_lossy(consumer).into_owned();
        let mut out = Vec::new();
        for pair in 0..half {
            let key = &rest[pair];
            let last =
                std::str::from_utf8(&rest[half + pair]).map_err(|_| String::from("syntax"))?;
            let mut data = self.read_stream(txn, key)?.unwrap_or_default();
            let Some(group) = data.groups.get_mut(&group_text) else {
                return Err(Self::nogroup(key, group_name));
            };
            let now = Self::stream_now_ms();
            let seen = group.consumers.entry(consumer_text.clone()).or_default();
            let mut changed = false;
            if seen.seen_ms != Some(now) {
                seen.seen_ms = Some(now);
                changed = true;
            }
            let bound = if last == ">" {
                Some(Self::parse_stream_id(&group.last).map_err(|_| String::from("corrupt"))?)
            } else if last == "$" {
                None
            } else {
                Some(Self::parse_stream_id(last)?)
            };
            let mut picked: Vec<&(String, StreamFields)> = Vec::new();
            for entry in data.entries.iter() {
                let id = Self::parse_stream_id(&entry.0).map_err(|_| String::from("corrupt"))?;
                let fresh = match bound {
                    Some(floor) => id > floor,
                    None => false,
                };
                if fresh {
                    picked.push(entry);
                    if count.is_some_and(|n| picked.len() >= n) {
                        break;
                    }
                }
            }
            if picked.is_empty() {
                if changed {
                    self.write_stream(txn, key, &data)?;
                }
                continue;
            }
            let max_id = picked.last().map(|(id, _)| id.clone()).unwrap_or_default();
            let delivered_at = Self::stream_now_ms();
            group.read += picked.len() as u64;
            group.consumers.entry(consumer_text.clone()).or_default().delivered_ms =
                Some(delivered_at);
            for (id, _) in picked.iter() {
                match group
                    .pending
                    .iter_mut()
                    .find(|(pending, _, _, _)| pending.as_str() == id.as_str())
                {
                    Some((_, _, deliveries, seen)) => {
                        *deliveries += 1;
                        *seen = delivered_at;
                    }
                    None => {
                        group.pending.push(((*id).clone(), consumer_text.clone(), 1, delivered_at))
                    }
                }
            }
            if last == ">" {
                group.last = max_id;
            }
            self.write_stream(txn, key, &data)?;
            let mut head = format!("*2\r\n${}\r\n", key.len()).into_bytes();
            head.extend_from_slice(key);
            head.extend_from_slice(b"\r\n");
            head.extend_from_slice(&Self::encode_stream_entries(&picked));
            out.push(head);
        }
        Ok(out)
    }

    fn xreadgroup(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        let (count, block, keys_at) = Self::parse_xreadgroup(args)?;
        if block.is_some() {
            return Err(String::from("BLOCK not allowed here"));
        }
        let rest = args.get(keys_at..).ok_or_else(|| String::from("syntax"))?;
        Ok(Self::encode_xread(self.xreadgroup_deliver(txn, count, &args[1], &args[2], rest)?))
    }

    async fn xreadgroup_blocking(&self, args: Vec<Vec<u8>>) -> Vec<u8> {
        let (count, block, keys_at) = match Self::parse_xreadgroup(&args) {
            Ok(parsed) => parsed,
            Err(e) => return encode_error(e),
        };
        let Some(block_ms) = block else {
            return encode_error(String::from("syntax"));
        };
        let deadline = if block_ms == 0 {
            None
        } else {
            Some(std::time::Instant::now() + std::time::Duration::from_millis(block_ms))
        };
        let mut waiter = ryme_txn::subscribe_commits();
        loop {
            let mut txn = self.manager.begin();
            let rest = args.get(keys_at..).unwrap_or(&[]);
            match self.xreadgroup_deliver(&mut txn, count, &args[1], &args[2], rest) {
                Err(e) => return encode_error(e),
                Ok(groups) if !groups.is_empty() => {
                    let bytes: u64 = args.iter().map(|arg| arg.len() as u64).sum();
                    if let Some(denied) = self.readonly_deny(true) {
                        return denied;
                    }
                    if let Some(denied) = self.admit(true, bytes) {
                        return denied;
                    }
                    let pending = match self.cdc_pending(&txn) {
                        Ok(pending) => pending,
                        Err(error) => return encode_error(error.to_string()),
                    };
                    match self.manager.commit(txn).await {
                        Ok(commit_ts) => {
                            self.cdc_emit(pending, commit_ts);
                            self.observe(true);
                            return Self::encode_xread(groups);
                        }
                        Err(e) => return encode_error(e.to_string()),
                    }
                }
                Ok(_) => {}
            }
            drop(txn);
            if deadline.is_some_and(|end| std::time::Instant::now() >= end) {
                return encode_nil_array();
            }
            let remaining =
                deadline.map(|end| end.saturating_duration_since(std::time::Instant::now()));
            Self::wait_for_commit(&mut waiter, remaining).await;
        }
    }

    fn xack(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> std::result::Result<i64, String> {
        if args.len() < 3 {
            return Err(String::from("wrong args"));
        }
        let mut data = self.read_stream(txn, &args[0])?.unwrap_or_default();
        let group_name = String::from_utf8_lossy(&args[1]).into_owned();
        let Some(group) = data.groups.get_mut(&group_name) else {
            return Err(Self::nogroup(&args[0], &args[1]));
        };
        let mut acked = 0;
        for id in &args[2..] {
            let text = String::from_utf8_lossy(id).into_owned();
            let before = group.pending.len();
            group.pending.retain(|(pending, _, _, _)| pending != &text);
            if group.pending.len() < before {
                acked += 1;
            }
        }
        self.write_stream(txn, &args[0], &data)?;
        Ok(acked)
    }

    fn xpending(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        if args.len() != 2 {
            return Err(String::from("wrong args"));
        }
        let data = self.read_stream(txn, &args[0])?.unwrap_or_default();
        let group_name = String::from_utf8_lossy(&args[1]).into_owned();
        let Some(group) = data.groups.get(&group_name) else {
            return Err(Self::nogroup(&args[0], &args[1]));
        };
        let mut ordered: Vec<(u64, u64)> = Vec::with_capacity(group.pending.len());
        for (id, _, _, _) in group.pending.iter() {
            ordered.push(Self::parse_stream_id(id).map_err(|_| String::from("corrupt"))?);
        }
        ordered.sort_unstable();
        let mut out = format!("*4\r\n:{}\r\n", ordered.len()).into_bytes();
        match (ordered.first(), ordered.last()) {
            (Some(first), Some(last)) => {
                let low = format!("{}-{}", first.0, first.1);
                let high = format!("{}-{}", last.0, last.1);
                out.extend_from_slice(format!("${}\r\n{low}\r\n", low.len()).as_bytes());
                out.extend_from_slice(format!("${}\r\n{high}\r\n", high.len()).as_bytes());
            }
            _ => out.extend_from_slice(b"$-1\r\n$-1\r\n"),
        }
        let mut per_consumer: std::collections::BTreeMap<&str, usize> =
            std::collections::BTreeMap::new();
        for (_, consumer, _, _) in group.pending.iter() {
            *per_consumer.entry(consumer.as_str()).or_default() += 1;
        }
        out.extend_from_slice(format!("*{}\r\n", per_consumer.len()).as_bytes());
        for (consumer, total) in per_consumer {
            let total_text = total.to_string();
            out.extend_from_slice(
                format!(
                    "*2\r\n${}\r\n{consumer}\r\n${}\r\n{total_text}\r\n",
                    consumer.len(),
                    total_text.len()
                )
                .as_bytes(),
            );
        }
        Ok(out)
    }

    fn xautoclaim(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        if args.len() < 5 {
            return Err(String::from("wrong args"));
        }
        let idle_text = std::str::from_utf8(&args[3]).map_err(|_| String::from("syntax"))?;
        let min_idle = idle_text.parse::<u64>().map_err(|_| String::from("syntax"))?;
        let start_text = std::str::from_utf8(&args[4]).map_err(|_| String::from("syntax"))?;
        let start = Self::parse_stream_id(start_text)?;
        let mut count = 100usize;
        let mut index = 5;
        while index < args.len() {
            let name = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
            if !name.eq_ignore_ascii_case("COUNT") {
                return Err(String::from("syntax"));
            }
            index += 1;
            let raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
            let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
            count = text.parse::<usize>().map_err(|_| String::from("syntax"))?.min(10_000);
            index += 1;
        }
        let mut data = self.read_stream(txn, &args[0])?.unwrap_or_default();
        let group_name = String::from_utf8_lossy(&args[1]).into_owned();
        let consumer = String::from_utf8_lossy(&args[2]).into_owned();
        let Some(group) = data.groups.get_mut(&group_name) else {
            return Err(Self::nogroup(&args[0], &args[1]));
        };
        let now = Self::stream_now_ms();
        let mut order: Vec<usize> = (0..group.pending.len()).collect();
        order.sort_by_key(|slot| Self::parse_stream_id(&group.pending[*slot].0).unwrap_or((0, 0)));
        let mut claimed: Vec<&(String, StreamFields)> = Vec::new();
        let mut orphans: Vec<Vec<u8>> = Vec::new();
        let mut cursor = String::from("0-0");
        let mut remaining = count;
        for slot in order {
            if remaining == 0 {
                cursor = group.pending[slot].0.clone();
                break;
            }
            let idle = now.saturating_sub(group.pending[slot].3);
            let id = Self::parse_stream_id(&group.pending[slot].0)
                .map_err(|_| String::from("corrupt"))?;
            if id <= start || idle < min_idle {
                continue;
            }
            group.pending[slot].1.clone_from(&consumer);
            group.pending[slot].2 += 1;
            group.pending[slot].3 = now;
            group.consumers.entry(consumer.clone()).or_default().delivered_ms = Some(now);
            let claimed_id = group.pending[slot].0.clone();
            match data.entries.iter().find(|(eid, _)| eid == &claimed_id) {
                Some(entry) => claimed.push(entry),
                None => {
                    orphans.push(format!("${}\r\n{claimed_id}\r\n", claimed_id.len()).into_bytes());
                }
            }
            remaining -= 1;
        }
        self.write_stream(txn, &args[0], &data)?;
        let mut out = format!("*3\r\n${}\r\n{cursor}\r\n", cursor.len()).into_bytes();
        out.extend_from_slice(&Self::encode_stream_entries(&claimed));
        out.extend_from_slice(format!("*{}\r\n", orphans.len()).as_bytes());
        for orphan in orphans {
            out.extend_from_slice(&orphan);
        }
        Ok(out)
    }

    fn georadius(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
        by_member: bool,
    ) -> std::result::Result<Vec<u8>, String> {
        if args.len() < 4 {
            return Err(String::from("wrong args"));
        }
        let points = self.read_geo(txn, &args[0])?.unwrap_or_default();
        let (center, mut index) = if by_member {
            let member = std::str::from_utf8(&args[1]).map_err(|_| String::from("syntax"))?;
            let center = points.get(member).copied().ok_or_else(|| String::from("not found"))?;
            (center, 2)
        } else {
            if args.len() < 5 {
                return Err(String::from("wrong args"));
            }
            let lon = geo_parse_coordinate(&args[1])?;
            let lat = geo_parse_coordinate(&args[2])?;
            geo_validate(lon, lat)?;
            ((lon, lat), 3)
        };
        let radius_raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
        index += 1;
        let unit_raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
        index += 1;
        let unit = std::str::from_utf8(unit_raw).map_err(|_| String::from("syntax"))?;
        let factor = geo_unit_factor(unit).ok_or_else(|| String::from("syntax"))?;
        let radius_text = std::str::from_utf8(radius_raw).map_err(|_| String::from("syntax"))?;
        let radius = radius_text
            .parse::<f64>()
            .ok()
            .filter(|v| *v >= 0.0)
            .ok_or_else(|| String::from("syntax"))?;
        let mut sort = GeoSort::Score;
        let mut count: Option<usize> = None;
        let mut with_dist = false;
        let mut with_hash = false;
        let mut with_coord = false;
        let mut store: Option<(&[u8], bool)> = None;
        while index < args.len() {
            let name = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
            match name.to_ascii_uppercase().as_str() {
                "ASC" => {
                    sort = GeoSort::DistanceAsc;
                    index += 1;
                }
                "DESC" => {
                    sort = GeoSort::DistanceDesc;
                    index += 1;
                }
                "COUNT" => {
                    index += 1;
                    let raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
                    let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
                    count = Some(
                        text.parse::<usize>().map_err(|_| String::from("syntax"))?.min(10_000),
                    );
                    index += 1;
                }
                "WITHDIST" => {
                    with_dist = true;
                    index += 1;
                }
                "WITHHASH" => {
                    with_hash = true;
                    index += 1;
                }
                "WITHCOORD" => {
                    with_coord = true;
                    index += 1;
                }
                "STORE" | "STOREDIST" => {
                    let is_dist = name.eq_ignore_ascii_case("STOREDIST");
                    index += 1;
                    let dest = args.get(index).ok_or_else(|| String::from("syntax"))?;
                    index += 1;
                    if store.is_some() {
                        return Err(String::from("syntax"));
                    }
                    store = Some((dest, is_dist));
                }
                _ => return Err(String::from("syntax")),
            }
        }
        let found =
            Self::geo_search_run(center, &GeoShape::Radius(radius * factor), &points, sort, count);
        if let Some((dest, is_dist)) = store {
            let mut scores: std::collections::HashMap<String, f64> =
                std::collections::HashMap::with_capacity(found.len());
            for (member, distance) in found.iter() {
                let score = if is_dist {
                    *distance
                } else {
                    let point = points.get(*member).copied().unwrap_or((0.0, 0.0));
                    geo_hash_score(point.0, point.1) as f64
                };
                scores.insert((*member).clone(), score);
            }
            let total = scores.len() as i64;
            self.write_zset(txn, dest, &scores)?;
            return Ok(encode_integer(total));
        }
        Ok(Self::encode_geo_results(&points, found, with_dist, with_coord, with_hash, factor))
    }

    fn geohash(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        if args.len() < 2 {
            return Err(String::from("wrong args"));
        }
        let points = self.read_geo(txn, &args[0])?.unwrap_or_default();
        let mut out = format!("*{}\r\n", args.len() - 1).into_bytes();
        for member in &args[1..] {
            let name = String::from_utf8_lossy(member).into_owned();
            match points.get(&name) {
                Some(point) => {
                    let hash = geo_base32(point.0, point.1);
                    out.extend_from_slice(format!("${}\r\n{hash}\r\n", hash.len()).as_bytes());
                }
                None => out.extend_from_slice(b"$-1\r\n"),
            }
        }
        Ok(out)
    }

    fn xinfo(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        if args.is_empty() {
            return Err(String::from("wrong args"));
        }
        let name = std::str::from_utf8(&args[0]).map_err(|_| String::from("syntax"))?;
        match name.to_ascii_uppercase().as_str() {
            "HELP" => Ok(Self::encode_bulks(&[
                "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "CONSUMERS <key> <groupname>",
                "    Show consumers of <groupname>.",
                "GROUPS <key>",
                "    Show the stream consumer groups.",
                "STREAM <key> [FULL [COUNT <count>]]",
                "    Show information about the stream.",
                "HELP",
                "    Print this help.",
            ])),
            "GROUPS" => {
                if args.len() != 2 {
                    return Err(String::from("wrong args"));
                }
                let data =
                    self.read_stream(txn, &args[1])?.ok_or_else(|| String::from("no such key"))?;
                let mut names: Vec<&String> = data.groups.keys().collect();
                names.sort();
                let mut out = format!("*{}\r\n", names.len()).into_bytes();
                for name in names {
                    let group = &data.groups[name];
                    out.extend_from_slice(&Self::encode_group(name, group, &data.entries));
                }
                Ok(out)
            }
            "CONSUMERS" => {
                if args.len() != 3 {
                    return Err(String::from("wrong args"));
                }
                let data =
                    self.read_stream(txn, &args[1])?.ok_or_else(|| String::from("no such key"))?;
                let group_name = String::from_utf8_lossy(&args[2]).into_owned();
                let Some(group) = data.groups.get(&group_name) else {
                    return Err(Self::nogroup(&args[1], &args[2]));
                };
                let now = Self::stream_now_ms();
                let mut names: Vec<&String> = group.consumers.keys().collect();
                names.sort();
                let mut out = format!("*{}\r\n", names.len()).into_bytes();
                for name in names {
                    let state = &group.consumers[name];
                    let pending =
                        group.pending.iter().filter(|(_, c, _, _)| c == name).count() as i64;
                    let idle =
                        state.delivered_ms.map(|seen| now.saturating_sub(seen) as i64).unwrap_or(0);
                    let inactive = state
                        .delivered_ms
                        .map(|seen| now.saturating_sub(seen) as i64)
                        .unwrap_or(-1);
                    out.extend_from_slice(
                        format!(
                            "*8\r\n$4\r\nname\r\n${}\r\n{name}\r\n$7\r\npending\r\n:{pending}\r\n$4\r\nidle\r\n:{idle}\r\n$8\r\ninactive\r\n:{inactive}\r\n",
                            name.len()
                        )
                        .as_bytes(),
                    );
                }
                Ok(out)
            }
            "STREAM" => self.xinfo_stream(txn, args),
            _ => Err(String::from("syntax")),
        }
    }

    fn encode_full_group(name: &str, group: &StreamGroup, entries: &StreamEntries) -> Vec<u8> {
        let mut pending: Vec<&(String, String, u64, u64)> = group.pending.iter().collect();
        pending.sort_by(|a, b| {
            Self::parse_stream_id(&a.0)
                .unwrap_or((0, 0))
                .cmp(&Self::parse_stream_id(&b.0).unwrap_or((0, 0)))
        });
        let lag = match Self::parse_stream_id(&group.last) {
            Ok(floor) => entries
                .iter()
                .filter(|(id, _)| Self::parse_stream_id(id).is_ok_and(|v| v > floor))
                .count() as i64,
            Err(_) => entries.len() as i64,
        };
        let mut out = format!(
            "*14\r\n$4\r\nname\r\n${}\r\n{name}\r\n$17\r\nlast-delivered-id\r\n${}\r\n{}\r\n$12\r\nentries-read\r\n:{}\r\n$3\r\nlag\r\n:{lag}\r\n$9\r\npel-count\r\n:{}\r\n$7\r\npending\r\n",
            name.len(),
            group.last.len(),
            group.last,
            group.read,
            pending.len(),
        )
        .into_bytes();
        out.extend_from_slice(format!("*{}\r\n", pending.len()).as_bytes());
        for (id, consumer, deliveries, seen) in pending {
            out.extend_from_slice(
                format!(
                    "*4\r\n${}\r\n{id}\r\n${}\r\n{consumer}\r\n:{seen}\r\n:{deliveries}\r\n",
                    id.len(),
                    consumer.len(),
                )
                .as_bytes(),
            );
        }
        let mut consumers: Vec<&String> = group.consumers.keys().collect();
        consumers.sort();
        out.extend_from_slice(b"$9\r\nconsumers\r\n");
        out.extend_from_slice(format!("*{}\r\n", consumers.len()).as_bytes());
        for consumer in consumers {
            let state = &group.consumers[consumer];
            let seen = state.seen_ms.unwrap_or(0);
            let active = state.delivered_ms.map(|v| v as i64).unwrap_or(-1);
            let pel = group.pending.iter().filter(|(_, c, _, _)| c == consumer).count();
            out.extend_from_slice(
                format!(
                    "*10\r\n$4\r\nname\r\n${}\r\n{consumer}\r\n$9\r\nseen-time\r\n:{seen}\r\n$11\r\nactive-time\r\n:{active}\r\n$9\r\npel-count\r\n:{pel}\r\n$7\r\npending\r\n",
                    consumer.len()
                )
                .as_bytes(),
            );
            out.extend_from_slice(format!("*{}\r\n", pel).as_bytes());
            for (id, _, seen, deliveries) in
                group.pending.iter().filter(|(_, c, _, _)| c == consumer)
            {
                out.extend_from_slice(
                    format!("*3\r\n${}\r\n{id}\r\n:{seen}\r\n:{deliveries}\r\n", id.len())
                        .as_bytes(),
                );
            }
        }
        out
    }

    fn encode_group(name: &str, group: &StreamGroup, entries: &StreamEntries) -> Vec<u8> {
        let lag = match Self::parse_stream_id(&group.last) {
            Ok(floor) => entries
                .iter()
                .filter(|(id, _)| Self::parse_stream_id(id).is_ok_and(|v| v > floor))
                .count() as i64,
            Err(_) => entries.len() as i64,
        };
        format!(
            "*12\r\n$4\r\nname\r\n${}\r\n{name}\r\n$9\r\nconsumers\r\n:{}\r\n$7\r\npending\r\n:{}\r\n$17\r\nlast-delivered-id\r\n${}\r\n{}\r\n$12\r\nentries-read\r\n:{}\r\n$3\r\nlag\r\n:{lag}\r\n",
            name.len(),
            group.consumers.len(),
            group.pending.len(),
            group.last.len(),
            group.last,
            group.read,
        )
        .into_bytes()
    }

    fn encode_bulks(items: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", items.len()).into_bytes();
        for item in items {
            out.extend_from_slice(format!("${}\r\n{item}\r\n", item.len()).as_bytes());
        }
        out
    }

    fn xinfo_stream(
        &self,
        txn: &mut Transaction,
        args: &[Vec<u8>],
    ) -> std::result::Result<Vec<u8>, String> {
        if args.len() < 2 {
            return Err(String::from("wrong args"));
        }
        let mut full = false;
        let mut count: Option<usize> = None;
        let mut index = 2;
        if index < args.len()
            && std::str::from_utf8(&args[index])
                .map_err(|_| String::from("syntax"))?
                .eq_ignore_ascii_case("FULL")
        {
            full = true;
            index += 1;
            if index < args.len() {
                let name = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
                if !name.eq_ignore_ascii_case("COUNT") {
                    return Err(String::from("syntax"));
                }
                index += 1;
                let raw = args.get(index).ok_or_else(|| String::from("syntax"))?;
                let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax"))?;
                count =
                    Some(text.parse::<usize>().map_err(|_| String::from("syntax"))?.min(10_000));
                index += 1;
            }
        }
        if index != args.len() {
            return Err(String::from("syntax"));
        }
        let data = self.read_stream(txn, &args[1])?.ok_or_else(|| String::from("no such key"))?;
        let last_id = data.entries.last().map(|(id, _)| id.clone()).unwrap_or(String::from("0-0"));
        let first_id =
            data.entries.first().map(|(id, _)| id.clone()).unwrap_or(String::from("0-0"));
        let mut names: Vec<&String> = data.groups.keys().collect();
        names.sort();
        let mut out = format!(
            "*{}\r\n$6\r\nlength\r\n:{}\r\n$15\r\nradix-tree-keys\r\n:{}\r\n$16\r\nradix-tree-nodes\r\n:0\r\n$17\r\nlast-generated-id\r\n${}\r\n{last_id}\r\n$20\r\nmax-deleted-entry-id\r\n${}\r\n{}\r\n$13\r\nentries-added\r\n:{}\r\n$23\r\nrecorded-first-entry-id\r\n${}\r\n{first_id}\r\n",
            if full { 18 } else { 20 },
            data.entries.len(),
            data.entries.len(),
            last_id.len(),
            data.max_deleted.len(),
            data.max_deleted,
            data.added,
            first_id.len(),
        )
        .into_bytes();
        if full {
            let mut shown: Vec<&(String, StreamFields)> = data.entries.iter().collect();
            if let Some(n) = count {
                shown.truncate(n);
            }
            out.extend_from_slice(b"$7\r\nentries\r\n");
            out.extend_from_slice(&Self::encode_stream_entries(&shown));
            out.extend_from_slice(b"$6\r\ngroups\r\n");
            out.extend_from_slice(format!("*{}\r\n", names.len()).as_bytes());
            for name in names {
                out.extend_from_slice(&Self::encode_full_group(
                    name,
                    &data.groups[name],
                    &data.entries,
                ));
            }
        } else {
            out.extend_from_slice(b"$6\r\ngroups\r\n");
            out.extend_from_slice(format!(":{}\r\n", names.len()).as_bytes());
            out.extend_from_slice(b"$11\r\nfirst-entry\r\n");
            match data.entries.first() {
                Some(entry) => {
                    let single = [entry];
                    out.extend_from_slice(&Self::encode_stream_entries(&single));
                }
                None => out.extend_from_slice(b"*-1\r\n"),
            }
            out.extend_from_slice(b"$10\r\nlast-entry\r\n");
            match data.entries.last() {
                Some(entry) => {
                    let single = [entry];
                    out.extend_from_slice(&Self::encode_stream_entries(&single));
                }
                None => out.extend_from_slice(b"*-1\r\n"),
            }
        }
        Ok(out)
    }

    fn ttl_secs(&self, txn: &Transaction, key: &[u8]) -> Result<i64> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        if let Some(write) = txn.writes().get(&record) {
            let now = ryme_txn::now_unix_millis();
            if write.value.is_none() || ryme_storage::expiry_is_expired(write.expires_at, now) {
                return Ok(-2);
            }
            if write.expires_at == 0 {
                return Ok(-1);
            }
            let remaining =
                ryme_storage::expiry_remaining_millis(write.expires_at, now).unwrap_or(0);
            return Ok((remaining / 1000) as i64);
        }
        match self.manager.expires_at_at(&record, txn.read_ts)? {
            Some(0) => Ok(-1),
            Some(ts) => {
                let now = ryme_txn::now_unix_millis();
                let remaining = ryme_storage::expiry_remaining_millis(ts, now).unwrap_or(0);
                if remaining == 0 { Ok(-2) } else { Ok((remaining / 1000) as i64) }
            }
            None => Ok(-2),
        }
    }

    fn ttl_millis(&self, txn: &Transaction, key: &[u8]) -> Result<i64> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        if let Some(write) = txn.writes().get(&record) {
            let now = ryme_txn::now_unix_millis();
            if write.value.is_none() || ryme_storage::expiry_is_expired(write.expires_at, now) {
                return Ok(-2);
            }
            if write.expires_at == 0 {
                return Ok(-1);
            }
            let remaining =
                ryme_storage::expiry_remaining_millis(write.expires_at, now).unwrap_or(0);
            return Ok(remaining.min(i64::MAX as u64) as i64);
        }
        match self.manager.expires_at_at(&record, txn.read_ts)? {
            Some(0) => Ok(-1),
            Some(ts) => {
                let remaining =
                    ryme_storage::expiry_remaining_millis(ts, ryme_txn::now_unix_millis())
                        .unwrap_or(0);
                if remaining == 0 { Ok(-2) } else { Ok(remaining.min(i64::MAX as u64) as i64) }
            }
            None => Ok(-2),
        }
    }

    fn expire_time(&self, txn: &mut Transaction, key: &[u8], millis: bool) -> Result<i64> {
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let now = ryme_txn::now_unix_millis();
        let expires_at = if let Some(write) = txn.writes().get(&record) {
            if write.value.is_none() || ryme_storage::expiry_is_expired(write.expires_at, now) {
                return Ok(-2);
            }
            write.expires_at
        } else {
            if self.manager.get(txn, &record)?.is_none() {
                return Ok(-2);
            }
            self.manager.expires_at_at(&record, txn.read_ts)?.unwrap_or(0)
        };
        if expires_at == 0 {
            return Ok(-1);
        }
        if ryme_storage::expiry_is_expired(expires_at, now) {
            return Ok(-2);
        }
        let timestamp_millis = ryme_storage::expiry_to_millis(expires_at);
        let timestamp = if millis { timestamp_millis } else { timestamp_millis / 1000 };
        Ok(timestamp.min(i64::MAX as u64) as i64)
    }

    fn getex(&self, txn: &mut Transaction, args: &[Vec<u8>]) -> Vec<u8> {
        if args.is_empty() {
            return encode_error(String::from("wrong args"));
        }
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, &args[0]);
        let option = match parse_getex_option(&args[1..]) {
            Ok(option) => option,
            Err(error) => return encode_error(error),
        };
        let value = match self.manager.get(txn, &record) {
            Ok(value) => value,
            Err(error) => return encode_error(error.to_string()),
        };
        let Some(value) = value else {
            return encode_null();
        };
        match option {
            GetExOption::Keep => {}
            GetExOption::Persist => self.manager.put_with_ttl(txn, record, value.clone(), 0),
            GetExOption::Expires(expires_at) => {
                self.manager.put_with_ttl(txn, record, value.clone(), expires_at)
            }
        }
        encode_bulk(&value)
    }

    fn expire_key(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        secs_raw: &[u8],
        flags: &[Vec<u8>],
    ) -> std::result::Result<bool, String> {
        let secs: i64 = std::str::from_utf8(secs_raw)
            .map_err(|_| String::from("value is not an integer or out of range"))?
            .parse()
            .map_err(|_| String::from("value is not an integer or out of range"))?;
        let expires_at = ryme_storage::expiry_at_millis(
            ryme_txn::now_unix_millis()
                .saturating_add((secs.max(0) as u64).saturating_mul(1000)),
        );
        self.apply_expire(txn, key, expires_at, flags)
    }

    fn expire_key_ms(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        millis_raw: &[u8],
        flags: &[Vec<u8>],
    ) -> std::result::Result<bool, String> {
        let millis: i64 = std::str::from_utf8(millis_raw)
            .map_err(|_| String::from("value is not an integer or out of range"))?
            .parse()
            .map_err(|_| String::from("value is not an integer or out of range"))?;
        let expires_at = ryme_storage::expiry_at_millis(
            ryme_txn::now_unix_millis().saturating_add(millis.max(0) as u64),
        );
        self.apply_expire(txn, key, expires_at, flags)
    }

    fn expire_key_at(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        timestamp_raw: &[u8],
        millis: bool,
        flags: &[Vec<u8>],
    ) -> std::result::Result<bool, String> {
        let timestamp: i64 = std::str::from_utf8(timestamp_raw)
            .map_err(|_| String::from("value is not an integer or out of range"))?
            .parse()
            .map_err(|_| String::from("value is not an integer or out of range"))?;
        let expires_at = if millis {
            ryme_storage::expiry_at_millis(timestamp.max(0) as u64)
        } else if timestamp <= 0 {
            ryme_storage::expiry_at_millis(0)
        } else {
            timestamp as u64
        };
        self.apply_expire(txn, key, expires_at, flags)
    }

    fn apply_expire(
        &self,
        txn: &mut Transaction,
        key: &[u8],
        expires_at: u64,
        flags: &[Vec<u8>],
    ) -> std::result::Result<bool, String> {
        let mut mode = ExpireMode::Always;
        for flag in flags {
            if mode != ExpireMode::Always {
                return Err(String::from("syntax"));
            }
            let name = std::str::from_utf8(flag).map_err(|_| String::from("syntax"))?;
            match name.to_ascii_uppercase().as_str() {
                "NX" => mode = ExpireMode::OnlyMissing,
                "XX" => mode = ExpireMode::OnlyPresent,
                "GT" => mode = ExpireMode::Greater,
                "LT" => mode = ExpireMode::Less,
                _ => return Err(String::from("syntax")),
            }
        }
        let record = RecordKey::new(&self.tenant, &self.database, KV_TABLE, key);
        let now = ryme_txn::now_unix_millis();
        let (value, expiry) = if let Some(write) = txn.writes().get(&record) {
            let Some(value) = write.value.as_ref() else {
                return Ok(false);
            };
            if ryme_storage::expiry_is_expired(write.expires_at, now) {
                return Ok(false);
            }
            (value.clone(), write.expires_at)
        } else {
            let Some(value) = self.manager.get(txn, &record).map_err(|e| e.to_string())? else {
                return Ok(false);
            };
            let Some(expiry) =
                self.manager.expires_at_at(&record, txn.read_ts).map_err(|e| e.to_string())?
            else {
                return Ok(false);
            };
            (value, expiry)
        };
        let allowed = match mode {
            ExpireMode::Always => true,
            ExpireMode::OnlyMissing => expiry == 0,
            ExpireMode::OnlyPresent => expiry != 0,
            ExpireMode::Greater => {
                expiry != 0
                    && ryme_storage::expiry_to_millis(expires_at)
                        > ryme_storage::expiry_to_millis(expiry)
            }
            ExpireMode::Less => {
                expiry == 0
                    || ryme_storage::expiry_to_millis(expires_at)
                        < ryme_storage::expiry_to_millis(expiry)
            }
        };
        if !allowed {
            return Ok(false);
        }
        if ryme_storage::expiry_is_expired(expires_at, now) {
            self.manager.delete(txn, record);
        } else {
            self.manager.put_with_ttl(txn, record, value, expires_at);
        }
        Ok(true)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpireMode {
    Always,
    OnlyMissing,
    OnlyPresent,
    Greater,
    Less,
}

enum GetExOption {
    Keep,
    Persist,
    Expires(u64),
}

fn parse_getex_option(args: &[Vec<u8>]) -> std::result::Result<GetExOption, String> {
    if args.is_empty() {
        return Ok(GetExOption::Keep);
    }
    if args.len() == 1 && args[0].eq_ignore_ascii_case(b"PERSIST") {
        return Ok(GetExOption::Persist);
    }
    if args.len() != 2 {
        return Err(String::from("syntax"));
    }
    let mode = std::str::from_utf8(&args[0]).map_err(|_| String::from("syntax"))?;
    let amount = std::str::from_utf8(&args[1])
        .map_err(|_| String::from("value is not an integer or out of range"))?
        .parse::<i64>()
        .map_err(|_| String::from("value is not an integer or out of range"))?;
    if amount < 0 {
        return Err(String::from("invalid expire time in 'getex' command"));
    }
    let now_millis = ryme_txn::now_unix_millis();
    match mode.to_ascii_uppercase().as_str() {
        "EX" if amount > 0 => Ok(GetExOption::Expires(ryme_storage::expiry_at_millis(
            now_millis.saturating_add((amount as u64).saturating_mul(1000)),
        ))),
        "PX" if amount > 0 => Ok(GetExOption::Expires(ryme_storage::expiry_at_millis(
            now_millis.saturating_add(amount as u64),
        ))),
        "EXAT" => Ok(GetExOption::Expires(amount as u64)),
        "PXAT" => Ok(GetExOption::Expires(ryme_storage::expiry_at_millis(amount as u64))),
        "EX" | "PX" => Err(String::from("invalid expire time in 'getex' command")),
        _ => Err(String::from("syntax")),
    }
}

struct SetOptions {
    expires_at: Option<u64>,
    only_missing: bool,
    only_present: bool,
    return_previous: bool,
}

fn parse_set_options(args: &[Vec<u8>]) -> std::result::Result<SetOptions, String> {
    let mut options = SetOptions {
        expires_at: None,
        only_missing: false,
        only_present: false,
        return_previous: false,
    };
    let mut index = 0;
    while index < args.len() {
        let name = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
        match name.to_ascii_uppercase().as_str() {
            "NX" => {
                options.only_missing = true;
                index += 1;
            }
            "XX" => {
                options.only_present = true;
                index += 1;
            }
            "GET" => {
                options.return_previous = true;
                index += 1;
            }
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if options.expires_at.is_some() {
                    return Err(String::from("syntax"));
                }
                index += 1;
                if index >= args.len() {
                    return Err(String::from("syntax"));
                }
                let raw = std::str::from_utf8(&args[index]).map_err(|_| String::from("syntax"))?;
                let amount: i64 = raw
                    .parse()
                    .map_err(|_| String::from("value is not an integer or out of range"))?;
                if amount < 0 {
                    return Err(String::from("invalid expire time in 'set' command"));
                }
                let now_millis = ryme_txn::now_unix_millis();
                options.expires_at = Some(match name.to_ascii_uppercase().as_str() {
                    "EX" => ryme_storage::expiry_at_millis(
                        now_millis.saturating_add((amount as u64).saturating_mul(1000)),
                    ),
                    "PX" => ryme_storage::expiry_at_millis(
                        now_millis.saturating_add(amount as u64),
                    ),
                    "EXAT" => amount as u64,
                    _ => ryme_storage::expiry_at_millis(amount as u64),
                });
                index += 1;
            }
            _ => return Err(String::from("syntax")),
        }
    }
    if options.only_missing && options.only_present {
        return Err(String::from("syntax"));
    }
    Ok(options)
}

#[derive(Debug, Clone)]
struct RespCommand {
    name: String,
    args: Vec<Vec<u8>>,
}

fn retries_conflict_safely(command: &RespCommand) -> bool {
    !matches!(command.name.as_str(), "EVAL" | "EVALSHA" | "PUBLISH")
}

fn set_command_read_only(txn: &mut Transaction, command: &RespCommand) -> ryme_error::Result<()> {
    if is_read_only_command(command) {
        txn.set_read_only()?;
    }
    Ok(())
}

// Keep this an allowlist: read-only transactions reject writes, so an
// ambiguous command must retain normal transaction tracking.
fn is_read_only_command(command: &RespCommand) -> bool {
    matches!(
        command.name.as_str(),
        "GET"
            | "TTL"
            | "PTTL"
            | "EXPIRETIME"
            | "PEXPIRETIME"
            | "EXISTS"
            | "TYPE"
            | "STRLEN"
            | "MGET"
            | "HGET"
            | "HEXISTS"
            | "HLEN"
            | "HGETALL"
            | "HKEYS"
            | "HVALS"
            | "LLEN"
            | "LRANGE"
            | "SMEMBERS"
            | "SCARD"
            | "SISMEMBER"
            | "ZSCORE"
            | "ZRANK"
            | "ZREVRANK"
            | "ZRANGE"
            | "ZCARD"
            | "ZCOUNT"
            | "SCAN"
            | "KEYS"
            | "XRANGE"
            | "XREVRANGE"
            | "XLEN"
            | "XREAD"
            | "XPENDING"
            | "XINFO"
            | "PFCOUNT"
            | "GEODIST"
            | "GEOPOS"
            | "GEOSEARCH"
            | "GEOHASH"
    )
}

#[derive(Debug)]
enum LuaRespValue {
    Null,
    Integer(i64),
    Bulk(Vec<u8>),
    Simple(Vec<u8>),
    Array(Vec<LuaRespValue>),
    Error(String),
}

impl LuaRespValue {
    fn into_lua(self, lua: &Lua) -> mlua::Result<LuaValue> {
        match self {
            Self::Null => Ok(LuaValue::Nil),
            Self::Integer(value) => Ok(LuaValue::Integer(value)),
            Self::Bulk(value) | Self::Simple(value) => {
                Ok(LuaValue::String(lua.create_string(value)?))
            }
            Self::Array(values) => {
                let table = lua.create_table()?;
                for (index, value) in values.into_iter().enumerate() {
                    table.set(index + 1, value.into_lua(lua)?)?;
                }
                Ok(LuaValue::Table(table))
            }
            Self::Error(error) => Err(mlua::Error::RuntimeError(error)),
        }
    }

    fn into_lua_pcall(self, lua: &Lua) -> mlua::Result<LuaValue> {
        match self {
            Self::Error(error) => {
                let table = lua.create_table()?;
                table.set("err", error)?;
                Ok(LuaValue::Table(table))
            }
            value => value.into_lua(lua),
        }
    }
}

fn lua_value_bytes(value: LuaValue) -> mlua::Result<Vec<u8>> {
    match value {
        LuaValue::String(value) => Ok(value.as_bytes().to_vec()),
        LuaValue::Integer(value) => Ok(value.to_string().into_bytes()),
        LuaValue::Number(value) if value.is_finite() => Ok(value.to_string().into_bytes()),
        LuaValue::Boolean(value) => Ok(if value { b"1".to_vec() } else { b"0".to_vec() }),
        _ => Err(mlua::Error::RuntimeError(String::from("command argument is not a string"))),
    }
}

fn lua_bytes_table(lua: &Lua, values: &[Vec<u8>]) -> mlua::Result<mlua::Table> {
    let table = lua.create_table()?;
    for (index, value) in values.iter().enumerate() {
        table.set(index + 1, lua.create_string(value)?)?;
    }
    Ok(table)
}

fn lua_value_resp(value: &LuaValue) -> std::result::Result<Vec<u8>, String> {
    let encoded = lua_value_resp_at(value, 0)?;
    if encoded.len() > RESP_LUA_MAX_REPLY_BYTES {
        Err(String::from("script reply exceeds the 16 MiB limit"))
    } else {
        Ok(encoded)
    }
}

fn lua_value_resp_at(value: &LuaValue, depth: usize) -> std::result::Result<Vec<u8>, String> {
    if depth > RESP_LUA_MAX_NESTING {
        return Err(String::from("script reply nesting limit exceeded"));
    }
    match value {
        LuaValue::Nil | LuaValue::Boolean(false) => Ok(encode_null()),
        LuaValue::Boolean(true) => Ok(encode_integer(1)),
        LuaValue::Integer(value) => Ok(encode_integer(*value)),
        LuaValue::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok(encode_integer(*value as i64))
        }
        LuaValue::Number(value) if value.is_finite() => {
            Ok(encode_bulk(value.to_string().as_bytes()))
        }
        LuaValue::String(value) => Ok(encode_bulk(value.as_bytes().as_ref())),
        LuaValue::Table(table) => {
            if let Ok(LuaValue::String(error)) = table.get::<LuaValue>("err") {
                let encoded = encode_raw_error(&error.as_bytes());
                return if encoded.len() <= RESP_LUA_MAX_REPLY_BYTES {
                    Ok(encoded)
                } else {
                    Err(String::from("script reply exceeds the 16 MiB limit"))
                };
            }
            let mut values = Vec::new();
            let mut values_bytes = 0usize;
            for value in table.sequence_values::<LuaValue>() {
                if values.len() >= RESP_LUA_MAX_ARRAY_ITEMS {
                    return Err(String::from("script reply array item limit exceeded"));
                }
                let encoded =
                    lua_value_resp_at(&value.map_err(|error| error.to_string())?, depth + 1)?;
                values_bytes = values_bytes
                    .checked_add(encoded.len())
                    .ok_or_else(|| String::from("script reply exceeds the 16 MiB limit"))?;
                if values_bytes > RESP_LUA_MAX_REPLY_BYTES {
                    return Err(String::from("script reply exceeds the 16 MiB limit"));
                }
                values.push(encoded);
            }
            let header = format!("*{}\r\n", values.len());
            if values_bytes.saturating_add(header.len()) > RESP_LUA_MAX_REPLY_BYTES {
                return Err(String::from("script reply exceeds the 16 MiB limit"));
            }
            let encoded = encode_raw_array(values);
            if encoded.len() > RESP_LUA_MAX_REPLY_BYTES {
                Err(String::from("script reply exceeds the 16 MiB limit"))
            } else {
                Ok(encoded)
            }
        }
        _ => Err(String::from("script returned an unsupported value")),
    }
}

fn parse_lua_resp(input: &[u8]) -> std::result::Result<LuaRespValue, String> {
    if input.len() > RESP_LUA_MAX_REPLY_BYTES {
        return Err(String::from("reply exceeds the 16 MiB limit"));
    }
    fn parse(
        input: &[u8],
        offset: &mut usize,
        depth: usize,
    ) -> std::result::Result<LuaRespValue, String> {
        if depth > RESP_LUA_MAX_NESTING {
            return Err(String::from("reply nesting limit exceeded"));
        }
        let kind = *input.get(*offset).ok_or_else(|| String::from("short reply"))?;
        *offset += 1;
        let line_end = find_crlf(&input[*offset..])
            .map(|end| end + *offset)
            .ok_or_else(|| String::from("short reply"))?;
        let line = &input[*offset..line_end];
        *offset = line_end + 2;
        match kind {
            b'+' => Ok(LuaRespValue::Simple(line.to_vec())),
            b'-' => Ok(LuaRespValue::Error(String::from_utf8_lossy(line).into_owned())),
            b':' => Ok(LuaRespValue::Integer(
                std::str::from_utf8(line)
                    .map_err(|_| String::from("invalid integer"))?
                    .parse()
                    .map_err(|_| String::from("invalid integer"))?,
            )),
            b'$' => {
                let length: i64 = std::str::from_utf8(line)
                    .map_err(|_| String::from("invalid bulk length"))?
                    .parse()
                    .map_err(|_| String::from("invalid bulk length"))?;
                if length < 0 {
                    return Ok(LuaRespValue::Null);
                }
                let length = usize::try_from(length).map_err(|_| String::from("bulk too large"))?;
                let end =
                    (*offset).checked_add(length).ok_or_else(|| String::from("bulk too large"))?;
                let frame_end = end.checked_add(2).ok_or_else(|| String::from("bulk too large"))?;
                if frame_end > input.len() || input.get(end..frame_end) != Some(b"\r\n") {
                    return Err(String::from("short bulk reply"));
                }
                let value = input[*offset..end].to_vec();
                *offset = frame_end;
                Ok(LuaRespValue::Bulk(value))
            }
            b'*' => {
                let length: i64 = std::str::from_utf8(line)
                    .map_err(|_| String::from("invalid array length"))?
                    .parse()
                    .map_err(|_| String::from("invalid array length"))?;
                if length < 0 {
                    return Ok(LuaRespValue::Null);
                }
                let length = usize::try_from(length)
                    .map_err(|_| String::from("array length is too large"))?;
                if length > RESP_LUA_MAX_ARRAY_ITEMS {
                    return Err(String::from("array length exceeds limit"));
                }
                if length > input.len().saturating_sub(*offset) / 3 {
                    return Err(String::from("array length exceeds reply"));
                }
                let mut values = Vec::with_capacity(length);
                for _ in 0..length {
                    values.push(parse(input, offset, depth + 1)?);
                }
                Ok(LuaRespValue::Array(values))
            }
            _ => Err(String::from("unsupported reply type")),
        }
    }

    let mut offset = 0;
    let value = parse(input, &mut offset, 0)?;
    if offset != input.len() {
        return Err(String::from("trailing reply data"));
    }
    Ok(value)
}

fn script_sha1(script: &[u8]) -> String {
    use sha1::{Digest, Sha1};
    let mut digest = Sha1::new();
    digest.update(script);
    format!("{:x}", digest.finalize())
}

fn script_cache_has_capacity(
    scripts: &std::collections::HashMap<(String, String), Vec<u8>>,
    key: &(String, String),
    script_bytes: usize,
) -> bool {
    if script_bytes > RESP_SCRIPT_MAX_BYTES {
        return false;
    }
    let existing_bytes = scripts.get(key).map(Vec::len).unwrap_or(0);
    let total_bytes =
        scripts.values().fold(0usize, |total, script| total.saturating_add(script.len()));
    let entry_count = if scripts.contains_key(key) { scripts.len() } else { scripts.len() + 1 };
    entry_count <= RESP_SCRIPT_CACHE_MAX_ENTRIES
        && total_bytes.saturating_sub(existing_bytes).saturating_add(script_bytes)
            <= RESP_SCRIPT_CACHE_MAX_BYTES
}

#[derive(Debug, Default)]
struct Multi {
    queue: Vec<RespCommand>,
    queue_bytes: usize,
    reserved_units: usize,
    queue_permits: Vec<OwnedSemaphorePermit>,
    dirty: bool,
}

type StreamFields = Vec<(String, String)>;
type StreamEntries = Vec<(String, StreamFields)>;
type TrimSpec = (Option<usize>, Option<(u64, u64)>);

#[derive(Debug)]
struct StreamData {
    entries: StreamEntries,
    groups: std::collections::HashMap<String, StreamGroup>,
    added: u64,
    max_deleted: String,
}

impl Default for StreamData {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            groups: std::collections::HashMap::new(),
            added: 0,
            max_deleted: String::from("0-0"),
        }
    }
}

#[derive(Debug, Default)]
struct StreamGroup {
    last: String,
    pending: Vec<(String, String, u64, u64)>,
    read: u64,
    consumers: std::collections::HashMap<String, ConsumerState>,
}

#[derive(Debug, Default)]
struct ConsumerState {
    delivered_ms: Option<u64>,
    seen_ms: Option<u64>,
}

type GeoPoint = (f64, f64);

enum GeoShape {
    Radius(f64),
    Box(f64, f64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeoSort {
    Score,
    DistanceAsc,
    DistanceDesc,
}

const EARTH_RADIUS_M: f64 = 6_372_797.560856;

fn geo_haversine_meters(from: GeoPoint, to: GeoPoint) -> f64 {
    let to_radians = std::f64::consts::PI / 180.0;
    let lon_from = from.0 * to_radians;
    let lat_from = from.1 * to_radians;
    let lon_to = to.0 * to_radians;
    let lat_to = to.1 * to_radians;
    let delta_lat = (lat_to - lat_from) / 2.0;
    let delta_lon = (lon_to - lon_from) / 2.0;
    let chord = delta_lat.sin().powi(2) + lat_from.cos() * lat_to.cos() * delta_lon.sin().powi(2);
    2.0 * EARTH_RADIUS_M * chord.sqrt().asin()
}

fn geo_format_distance(meters: f64, factor: f64) -> String {
    format!("{:.4}", meters / factor)
}

fn geo_unit_factor(unit: &str) -> Option<f64> {
    match unit.to_ascii_lowercase().as_str() {
        "m" => Some(1.0),
        "km" => Some(1_000.0),
        "mi" => Some(1_609.344),
        "ft" => Some(0.3048),
        _ => None,
    }
}

fn geo_parse_coordinate(raw: &[u8]) -> std::result::Result<f64, String> {
    let text = std::str::from_utf8(raw).map_err(|_| String::from("syntax error"))?;
    text.parse::<f64>().ok().filter(|v| v.is_finite()).ok_or_else(|| String::from("syntax error"))
}

fn geo_hash_score(lon: f64, lat: f64) -> u64 {
    let mut lon_range = (-180.0f64, 180.0f64);
    let mut lat_range = (-85.05112878f64, 85.05112878f64);
    let mut bits = 0u64;
    for _ in 0..26 {
        bits <<= 1;
        let mid = (lon_range.0 + lon_range.1) / 2.0;
        if lon >= mid {
            bits |= 1;
            lon_range.0 = mid;
        } else {
            lon_range.1 = mid;
        }
        bits <<= 1;
        let mid = (lat_range.0 + lat_range.1) / 2.0;
        if lat >= mid {
            bits |= 1;
            lat_range.0 = mid;
        } else {
            lat_range.1 = mid;
        }
    }
    bits
}

fn geo_base32(lon: f64, lat: f64) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let mut lon_range = (-180.0f64, 180.0f64);
    let mut lat_range = (-90.0f64, 90.0f64);
    let mut bits = 0u64;
    for _ in 0..25 {
        bits <<= 1;
        let mid = (lon_range.0 + lon_range.1) / 2.0;
        if lon >= mid {
            bits |= 1;
            lon_range.0 = mid;
        } else {
            lon_range.1 = mid;
        }
        bits <<= 1;
        let mid = (lat_range.0 + lat_range.1) / 2.0;
        if lat >= mid {
            bits |= 1;
            lat_range.0 = mid;
        } else {
            lat_range.1 = mid;
        }
    }
    let mut out = String::with_capacity(11);
    for step in 0..10 {
        out.push(ALPHABET[((bits >> (45 - step * 5)) & 31) as usize] as char);
    }
    out.push('0');
    out
}

fn geo_decode_score(score: u64) -> GeoPoint {
    let mut lon_range = (-180.0f64, 180.0f64);
    let mut lat_range = (-85.05112878f64, 85.05112878f64);
    for step in 0..26 {
        let lon_bit = (score >> (51 - step * 2)) & 1 == 1;
        let lat_bit = (score >> (50 - step * 2)) & 1 == 1;
        let mid = (lon_range.0 + lon_range.1) / 2.0;
        if lon_bit {
            lon_range.0 = mid;
        } else {
            lon_range.1 = mid;
        }
        let mid = (lat_range.0 + lat_range.1) / 2.0;
        if lat_bit {
            lat_range.0 = mid;
        } else {
            lat_range.1 = mid;
        }
    }
    ((lon_range.0 + lon_range.1) / 2.0, (lat_range.0 + lat_range.1) / 2.0)
}

fn geo_validate(lon: f64, lat: f64) -> std::result::Result<(), String> {
    if !(-180.0..=180.0).contains(&lon) || !(-85.05112878..=85.05112878).contains(&lat) {
        return Err(format!("invalid longitude,latitude pair {lon:.6},{lat:.6}"));
    }
    Ok(())
}

const HLL_PRECISION: u32 = 14;
const HLL_REGISTERS: usize = 1 << HLL_PRECISION;

fn hll_hash(member: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in member {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58476d1ce4e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d049bb133111eb);
    hash ^= hash >> 31;
    hash
}

fn hll_position(member: &[u8]) -> (usize, u8) {
    let hash = hll_hash(member);
    let index = (hash >> (64 - HLL_PRECISION)) as usize;
    let rest = hash & (u64::MAX >> HLL_PRECISION);
    let rho = rest.leading_zeros() - HLL_PRECISION + 1;
    (index, rho.min(64 - HLL_PRECISION + 1) as u8)
}

fn hll_estimate(registers: &[u8]) -> u64 {
    let size = registers.len() as f64;
    let mut harmonic = 0.0;
    let mut zeros = 0u64;
    for register in registers {
        harmonic += 2.0f64.powi(-i32::from(*register));
        if *register == 0 {
            zeros += 1;
        }
    }
    let alpha = 0.7213 / (1.0 + 1.079 / size);
    let estimate = alpha * size * size / harmonic;
    if estimate <= 2.5 * size && zeros > 0 {
        (size * (size / zeros as f64).ln()) as u64
    } else {
        estimate as u64
    }
}

fn is_known(name: &str) -> bool {
    matches!(
        name,
        "PING"
            | "ECHO"
            | "PUBLISH"
            | "EVAL"
            | "EVALSHA"
            | "SCRIPT"
            | "AUTH"
            | "GET"
            | "SET"
            | "SETNX"
            | "GETSET"
            | "GETEX"
            | "GETDEL"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "TTL"
            | "PTTL"
            | "EXPIRETIME"
            | "PEXPIRETIME"
            | "PERSIST"
            | "DEL"
            | "EXISTS"
            | "TYPE"
            | "APPEND"
            | "STRLEN"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "INCRBYFLOAT"
            | "MGET"
            | "MSET"
            | "MSETNX"
            | "HSET"
            | "HGET"
            | "HDEL"
            | "HEXISTS"
            | "HLEN"
            | "HGETALL"
            | "HKEYS"
            | "HVALS"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "LLEN"
            | "LRANGE"
            | "SADD"
            | "SREM"
            | "SMEMBERS"
            | "SCARD"
            | "SISMEMBER"
            | "ZADD"
            | "ZSCORE"
            | "ZRANK"
            | "ZREVRANK"
            | "ZRANGE"
            | "ZREM"
            | "ZCARD"
            | "ZCOUNT"
            | "ZINCRBY"
            | "SCAN"
            | "KEYS"
            | "XADD"
            | "XRANGE"
            | "XREVRANGE"
            | "XLEN"
            | "XTRIM"
            | "XREAD"
            | "XDEL"
            | "PFADD"
            | "PFCOUNT"
            | "PFMERGE"
            | "GEOADD"
            | "GEODIST"
            | "GEOPOS"
            | "GEOSEARCH"
            | "GEORADIUS"
            | "GEORADIUSBYMEMBER"
            | "GEOHASH"
            | "BLPOP"
            | "BRPOP"
            | "BLMOVE"
            | "XGROUP"
            | "XREADGROUP"
            | "XACK"
            | "XPENDING"
            | "XAUTOCLAIM"
            | "XINFO"
    )
}

fn subscription_count(client: &ClientState) -> usize {
    client.subscriptions.len().saturating_add(client.patterns.len())
}

fn validate_pubsub_subscriptions(
    current: &std::collections::BTreeSet<Vec<u8>>,
    other_count: usize,
    requested: &[Vec<u8>],
) -> Option<&'static str> {
    let mut additions = std::collections::BTreeSet::new();
    for name in requested {
        if name.len() > RESP_MAX_PUBSUB_NAME_BYTES {
            return Some("pubsub channel or pattern is too long");
        }
        if !current.contains(name) {
            additions.insert(name.clone());
        }
    }
    if current.len().saturating_add(other_count).saturating_add(additions.len())
        > RESP_MAX_PUBSUB_SUBSCRIPTIONS
    {
        Some("pubsub subscription limit exceeded")
    } else {
        None
    }
}

fn decode_command(input: &[u8]) -> Result<Option<(RespCommand, usize)>> {
    if input.is_empty() {
        return Ok(None);
    }
    if input[0] == b'*' {
        decode_array(input)
    } else {
        decode_inline(input)
    }
}

fn decode_inline(input: &[u8]) -> Result<Option<(RespCommand, usize)>> {
    let Some(end) = find_crlf(input) else {
        return Ok(None);
    };
    let frame_end =
        end.checked_add(2).ok_or_else(|| RymeError::Overload(String::from("request")))?;
    if frame_end > RESP_MAX_FRAME_BYTES {
        return Err(RymeError::Overload(String::from("request")));
    }
    let line = std::str::from_utf8(&input[..end])
        .map_err(|_| RymeError::InvalidArgument(String::from("resp utf8")))?;
    let mut tokens = line.split_whitespace();
    let Some(name) = tokens.next() else {
        return Err(RymeError::InvalidArgument(String::from("resp")));
    };
    let args: Vec<Vec<u8>> =
        tokens.take(RESP_MAX_COMMAND_ARGS + 1).map(|part| part.as_bytes().to_vec()).collect();
    if args.len() > RESP_MAX_COMMAND_ARGS {
        return Err(RymeError::InvalidArgument(String::from("resp arity")));
    }
    Ok(Some((RespCommand { name: name.to_ascii_uppercase(), args }, end + 2)))
}

fn decode_array(input: &[u8]) -> Result<Option<(RespCommand, usize)>> {
    let Some(first_end) = find_crlf(input) else {
        return Ok(None);
    };
    let count: usize = String::from_utf8_lossy(&input[1..first_end])
        .parse()
        .map_err(|_| RymeError::InvalidArgument(String::from("resp")))?;
    if count == 0 || count > RESP_MAX_COMMAND_ARGS + 1 {
        return Err(RymeError::InvalidArgument(String::from("resp arity")));
    }
    let mut offset =
        first_end.checked_add(2).ok_or_else(|| RymeError::Overload(String::from("request")))?;
    if offset > RESP_MAX_FRAME_BYTES {
        return Err(RymeError::Overload(String::from("request")));
    }
    let mut parts: Vec<Vec<u8>> = Vec::new();
    for _ in 0..count {
        if offset >= input.len() {
            return Ok(None);
        }
        if input[offset] != b'$' {
            return Err(RymeError::InvalidArgument(String::from("resp")));
        }
        let Some(len_end) = find_crlf(&input[offset..]).map(|v| v + offset) else {
            return Ok(None);
        };
        let length: usize = String::from_utf8_lossy(&input[offset + 1..len_end])
            .parse()
            .map_err(|_| RymeError::InvalidArgument(String::from("resp")))?;
        let value_start =
            len_end.checked_add(2).ok_or_else(|| RymeError::Overload(String::from("request")))?;
        let frame_end = value_start
            .checked_add(length)
            .and_then(|end| end.checked_add(2))
            .ok_or_else(|| RymeError::Overload(String::from("request")))?;
        if frame_end > RESP_MAX_FRAME_BYTES {
            return Err(RymeError::Overload(String::from("request")));
        }
        if input.len() < frame_end {
            return Ok(None);
        }
        if &input[value_start + length..frame_end] != b"\r\n" {
            return Err(RymeError::InvalidArgument(String::from("resp")));
        }
        parts.push(input[value_start..value_start + length].to_vec());
        offset = frame_end;
    }
    if parts.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("resp")));
    }
    Ok(Some((
        RespCommand {
            name: std::str::from_utf8(&parts[0])
                .map_err(|_| RymeError::InvalidArgument(String::from("resp command name")))?
                .to_ascii_uppercase(),
            args: parts[1..].to_vec(),
        },
        offset,
    )))
}

fn find_crlf(input: &[u8]) -> Option<usize> {
    input.windows(2).position(|w| w == b"\r\n")
}

fn encode_simple(value: &str) -> Vec<u8> {
    format!("+{value}\r\n").into_bytes()
}

fn encode_error(message: String) -> Vec<u8> {
    format!("-ERR {message}\r\n").into_bytes()
}

fn encode_raw_error(message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.len() + 3);
    out.extend_from_slice(b"-");
    out.extend_from_slice(message);
    out.extend_from_slice(b"\r\n");
    out
}

fn encode_noscript() -> Vec<u8> {
    b"-NOSCRIPT No matching script. Please use EVAL.\r\n".to_vec()
}

fn encode_integer(value: i64) -> Vec<u8> {
    format!(":{value}\r\n").into_bytes()
}

fn encode_bulk(value: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", value.len()).into_bytes();
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
    out
}

fn encode_null() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

fn encode_nil_array() -> Vec<u8> {
    b"*-1\r\n".to_vec()
}

fn encode_exec_abort() -> Vec<u8> {
    b"-EXECABORT Transaction discarded because of previous errors.\r\n".to_vec()
}

fn encode_raw_array(replies: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out = format!("*{}\r\n", replies.len()).into_bytes();
    for reply in replies {
        out.extend_from_slice(&reply);
    }
    out
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit(u32::from(*byte >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(*byte & 0x0f), 16).unwrap_or('0'));
    }
    out
}

fn qos_now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn slow_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    let raw = text.as_bytes();
    let mut index = 0;
    while index < raw.len() {
        let high = (raw[index] as char).to_digit(16)?;
        let low = (raw[index + 1] as char).to_digit(16)?;
        out.push((high * 16 + low) as u8);
        index += 2;
    }
    Some(out)
}

fn glob_match(pattern: &[u8], value: &[u8]) -> bool {
    let (mut px, mut vx) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while vx < value.len() || px < pattern.len() {
        if px < pattern.len() {
            match pattern[px] {
                b'*' => {
                    star = Some(px);
                    mark = vx;
                    px += 1;
                    continue;
                }
                b'?' => {
                    if vx < value.len() {
                        px += 1;
                        vx += 1;
                        continue;
                    }
                }
                b'\\' if px + 1 < pattern.len() => {
                    if vx < value.len() && pattern[px + 1] == value[vx] {
                        px += 2;
                        vx += 1;
                        continue;
                    }
                }
                b'[' => {
                    if let Some((matched, consumed)) = glob_class(&pattern[px..], value.get(vx)) {
                        if matched {
                            px += consumed;
                            vx += 1;
                            continue;
                        }
                    }
                }
                _ => {
                    if vx < value.len() && pattern[px] == value[vx] {
                        px += 1;
                        vx += 1;
                        continue;
                    }
                }
            }
        }
        if let Some(star_at) = star {
            px = star_at + 1;
            mark += 1;
            if mark > value.len() {
                return false;
            }
            vx = mark;
        } else {
            return false;
        }
    }
    true
}

fn glob_class(pattern: &[u8], byte: Option<&u8>) -> Option<(bool, usize)> {
    let byte = *byte?;
    let mut index = 1;
    let mut negate = false;
    if pattern.get(index) == Some(&b'^') {
        negate = true;
        index += 1;
    }
    if pattern.get(index) == Some(&b']') {
        index += 1;
    }
    let mut matched = false;
    let mut closed = false;
    while index < pattern.len() {
        match pattern[index] {
            b']' => {
                closed = true;
                index += 1;
                break;
            }
            b'\\' if index + 1 < pattern.len() => {
                matched |= pattern[index + 1] == byte;
                index += 2;
            }
            low if pattern.get(index + 1) == Some(&b'-') && index + 2 < pattern.len() => {
                let high = pattern[index + 2];
                matched |= low <= byte && byte <= high;
                index += 3;
            }
            other => {
                matched |= other == byte;
                index += 1;
            }
        }
    }
    if !closed {
        return None;
    }
    Some((matched != negate, index))
}

fn encode_array(items: Vec<Option<Vec<u8>>>) -> Vec<u8> {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        match item {
            Some(value) => {
                out.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
                out.extend_from_slice(&value);
                out.extend_from_slice(b"\r\n");
            }
            None => out.extend_from_slice(b"$-1\r\n"),
        }
    }
    out
}

fn remote_ttl(present: bool, expires_at: Option<u64>, millis: bool) -> i64 {
    match (present, expires_at) {
        (false, _) => -2,
        (true, None) => -1,
        (true, Some(0)) => -1,
        (true, Some(ts)) => {
            let remaining =
                ryme_storage::expiry_remaining_millis(ts, ryme_txn::now_unix_millis())
                    .unwrap_or(0);
            if remaining == 0 {
                -2
            } else if millis {
                remaining.min(i64::MAX as u64) as i64
            } else {
                (remaining / 1000) as i64
            }
        }
    }
}

fn remote_expire_time(present: bool, expires_at: Option<u64>, millis: bool) -> i64 {
    if !present {
        return -2;
    }
    let Some(expires_at) = expires_at.filter(|expires_at| *expires_at != 0) else {
        return -1;
    };
    let now = ryme_txn::now_unix_millis();
    if ryme_storage::expiry_is_expired(expires_at, now) {
        return -2;
    }
    let timestamp_millis = ryme_storage::expiry_to_millis(expires_at);
    let timestamp = if millis { timestamp_millis } else { timestamp_millis / 1000 };
    timestamp.min(i64::MAX as u64) as i64
}

// Command handlers share one compact RESP2 encoder.  RESP2 scalar replies are
// valid RESP3 values too, so protocol negotiation only needs to translate the
// two incompatible null forms and preserve nested containers.  Keeping this
// conversion at the connection boundary avoids a second encoder in every
// command path.
fn resp3_replies(input: &[u8]) -> std::result::Result<Vec<u8>, ()> {
    let mut offset = 0;
    let mut output = Vec::with_capacity(input.len());
    while offset < input.len() {
        resp3_one(input, &mut offset, &mut output)?;
    }
    Ok(output)
}

fn resp3_one(
    input: &[u8],
    offset: &mut usize,
    output: &mut Vec<u8>,
) -> std::result::Result<(), ()> {
    let kind = *input.get(*offset).ok_or(())?;
    let start = *offset;
    *offset += 1;
    let line_end = find_crlf(&input[*offset..]).ok_or(())? + *offset;
    let line = &input[*offset..line_end];
    *offset = line_end + 2;
    match kind {
        b'+' | b'-' | b':' => output.extend_from_slice(&input[start..*offset]),
        b'$' => {
            let length =
                std::str::from_utf8(line).map_err(|_| ())?.parse::<i64>().map_err(|_| ())?;
            if length < 0 {
                output.extend_from_slice(b"_\r\n");
            } else {
                let length = usize::try_from(length).map_err(|_| ())?;
                let end = (*offset).checked_add(length).ok_or(())?;
                if end > input.len() || input.len() - end < 2 || &input[end..end + 2] != b"\r\n" {
                    return Err(());
                }
                output.extend_from_slice(&input[start..end + 2]);
                *offset = end + 2;
            }
        }
        b'*' | b'%' | b'>' => {
            let count =
                std::str::from_utf8(line).map_err(|_| ())?.parse::<i64>().map_err(|_| ())?;
            if count < 0 {
                output.extend_from_slice(b"_\r\n");
                return Ok(());
            }
            output.push(kind);
            output.extend_from_slice(count.to_string().as_bytes());
            output.extend_from_slice(b"\r\n");
            let items = if kind == b'%' { count.checked_mul(2).ok_or(())? } else { count };
            for _ in 0..usize::try_from(items).map_err(|_| ())? {
                resp3_one(input, offset, output)?;
            }
        }
        _ => return Err(()),
    }
    Ok(())
}

fn resp3_push(input: &[u8]) -> std::result::Result<Vec<u8>, ()> {
    let mut output = resp3_replies(input)?;
    if output.first() == Some(&b'*') {
        output[0] = b'>';
        Ok(output)
    } else {
        Err(())
    }
}

fn resp3_pubsub_replies(input: &[u8]) -> std::result::Result<Vec<u8>, ()> {
    let mut offset = 0;
    let mut output = Vec::with_capacity(input.len());
    while offset < input.len() {
        let mut reply = Vec::new();
        resp3_one(input, &mut offset, &mut reply)?;
        if reply.first() != Some(&b'*') {
            return Err(());
        }
        reply[0] = b'>';
        output.extend_from_slice(&reply);
    }
    Ok(output)
}

fn int_arg(raw: &[u8]) -> std::result::Result<i64, String> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| String::from("value is not an integer or out of range"))
}

fn float_arg(raw: &[u8]) -> std::result::Result<f64, String> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .ok_or_else(|| String::from("value is not a valid float"))
}

fn format_float(value: f64) -> String {
    if value == value.trunc() && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        let mut text = format!("{value:.17}");
        while text.contains('.') && (text.ends_with('0') || text.ends_with('.')) {
            if text.ends_with('.') {
                text.pop();
                break;
            }
            text.pop();
        }
        text
    }
}

fn slice_list(list: &[String], start: i64, stop: i64) -> Vec<Option<Vec<u8>>> {
    let length = list.len() as i64;
    if list.is_empty() {
        return Vec::new();
    }
    let mut from = if start < 0 { (length + start).max(0) } else { start.min(length) };
    let mut to = if stop < 0 { length + stop } else { stop.min(length - 1) };
    from = from.clamp(0, length);
    to = to.clamp(-1, length - 1);
    if from > to {
        return Vec::new();
    }
    list[from as usize..=to as usize].iter().map(|v| Some(v.as_bytes().to_vec())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn autocommit_get_uses_a_read_only_transaction() {
        let gateway = RespGateway::new(String::from("tenant"), String::from("database"));
        let key = RecordKey::new("tenant", "database", KV_TABLE, b"key");
        let mut seed = gateway.manager.begin();
        gateway.manager.put(&mut seed, key, b"value".to_vec());
        gateway.manager.commit(seed).unwrap();

        let command = RespCommand { name: String::from("GET"), args: vec![b"key".to_vec()] };
        let mut txn = gateway.manager.begin();
        set_command_read_only(&mut txn, &command).unwrap();

        assert_eq!(gateway.dispatch_in(&mut txn, command), encode_bulk(b"value"));
        assert!(txn.read_keys().is_empty());
        assert!(txn.writes().is_empty());
    }

    #[test]
    fn read_only_command_allowlist_excludes_mutating_and_ambiguous_commands() {
        for name in [
            "GET",
            "MGET",
            "EXISTS",
            "HGETALL",
            "LRANGE",
            "SMEMBERS",
            "ZRANGE",
            "XRANGE",
            "XREAD",
            "GEOSEARCH",
        ] {
            assert!(is_read_only_command(&RespCommand { name: name.to_string(), args: vec![] }));
        }

        for name in [
            "GETEX",
            "GETDEL",
            "SET",
            "EVAL",
            "PUBLISH",
            "XREADGROUP",
            "GEORADIUS",
            "GEORADIUSBYMEMBER",
            "GEOSEARCHSTORE",
            "ZRANGESTORE",
        ] {
            assert!(!is_read_only_command(&RespCommand { name: name.to_string(), args: vec![] }));
        }
    }

    #[test]
    fn lua_commands_enforce_publish_and_write_permissions_before_dispatch() {
        let mut gateway = RespGateway::new(String::from("tenant"), String::from("database"));
        gateway.write_allowed = false;
        gateway.publish_allowed = false;
        let mut receiver = gateway.pubsub.subscribe();
        let mut txn = gateway.manager.begin();

        let denied_publish = gateway.run_lua_script(
            &mut txn,
            b"return redis.call('PUBLISH', 'events', 'secret')",
            &[],
            &[],
        );
        assert!(denied_publish.unwrap_err().contains("NOPERM permission denied"));
        assert!(matches!(receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));

        let denied_write =
            gateway.run_lua_script(&mut txn, b"return redis.call('SET', 'key', 'value')", &[], &[]);
        assert!(denied_write.unwrap_err().contains("NOPERM permission denied"));
        assert!(txn.writes().is_empty());

        gateway.publish_allowed = true;
        assert_eq!(
            gateway
                .run_lua_script(
                    &mut txn,
                    b"return redis.call('PUBLISH', 'events', 'allowed')",
                    &[],
                    &[],
                )
                .unwrap(),
            b":0\r\n"
        );
        assert_eq!(receiver.try_recv().unwrap().payload, b"allowed");
    }

    #[test]
    fn collection_payloads_are_bounded() {
        assert!(check_collection_bytes(&vec![0; RESP_MAX_COLLECTION_BYTES]).is_ok());
        assert!(check_collection_bytes(&vec![0; RESP_MAX_COLLECTION_BYTES + 1]).is_err());
    }

    #[test]
    fn pubsub_global_subscription_limit_rolls_back_partial_requests() {
        let gateway = RespGateway::new(String::from("tenant"), String::from("database"));
        let mut client = gateway.fresh_client();
        gateway
            .pubsub
            .total_subscriptions
            .store(RESP_MAX_GLOBAL_PUBSUB_SUBSCRIPTIONS - 1, Ordering::Relaxed);

        let reply = gateway.subscribe(&[b"first".to_vec(), b"second".to_vec()], &mut client);
        assert!(reply.starts_with(b"-ERR global pubsub subscription limit exceeded"));
        assert!(client.subscriptions.is_empty());
        assert!(gateway.pubsub.counts.lock().unwrap().is_empty());
        assert_eq!(
            gateway.pubsub.total_subscriptions.load(Ordering::Relaxed),
            RESP_MAX_GLOBAL_PUBSUB_SUBSCRIPTIONS - 1
        );
    }

    #[test]
    fn pubsub_global_pattern_registry_is_bounded() {
        let bus = new_pubsub();
        for index in 0..RESP_MAX_GLOBAL_PUBSUB_PATTERNS {
            assert!(bus.add_pattern("tenant", format!("pattern-{index}").as_bytes()));
        }
        assert!(!bus.add_pattern("tenant", b"one-too-many"));
        assert!(bus.add_pattern("tenant", b"pattern-0"));
        assert_eq!(
            bus.total_subscriptions.load(Ordering::Relaxed),
            RESP_MAX_GLOBAL_PUBSUB_PATTERNS + 1
        );
    }

    #[test]
    fn resp_decoder_rejects_oversized_frames_before_copying_payload() {
        let mut input = b"*2\r\n$3\r\nGET\r\n$1048570\r\n".to_vec();
        input.extend(std::iter::repeat_n(b'x', RESP_MAX_FRAME_BYTES));
        assert!(matches!(
            decode_array(&input),
            Err(RymeError::Overload(message)) if message == "request"
        ));
    }

    #[test]
    fn resp_decoder_requires_bulk_string_terminator() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkeyxx";
        assert!(matches!(
            decode_array(input),
            Err(RymeError::InvalidArgument(message)) if message == "resp"
        ));
    }

    #[test]
    fn lua_script_replies_reject_excessive_nesting() {
        let lua = Lua::new();
        let value: LuaValue = lua
            .load("local value = 'leaf'; for _ = 1, 65 do value = { value } end; return value")
            .eval()
            .unwrap();
        assert!(matches!(
            lua_value_resp(&value),
            Err(message) if message == "script reply nesting limit exceeded"
        ));
    }

    #[test]
    fn lua_resp_parser_bounds_array_allocation_and_nesting() {
        assert!(parse_lua_resp(b"*9223372036854775807\r\n").is_err());
        let too_many_items = format!("*{}\r\n", RESP_LUA_MAX_ARRAY_ITEMS + 1);
        assert!(parse_lua_resp(too_many_items.as_bytes()).is_err());

        let mut nested = Vec::new();
        for _ in 0..=RESP_LUA_MAX_NESTING {
            nested.extend_from_slice(b"*1\r\n");
        }
        nested.extend_from_slice(b"$-1\r\n");
        assert!(matches!(
            parse_lua_resp(&nested),
            Err(message) if message == "reply nesting limit exceeded"
        ));
        assert!(matches!(
            parse_lua_resp(b"*1\r\n$1\r\nx\r\n"),
            Ok(LuaRespValue::Array(values)) if values.len() == 1
        ));
    }

    #[test]
    fn script_cache_is_scoped_and_bounded() {
        let alpha = RespGateway::new(String::from("alpha"), String::from("d"));
        let mut beta = alpha.clone();
        beta.tenant = String::from("beta");
        let script = b"return 1".to_vec();
        let sha = script_sha1(&script);

        assert!(alpha.script_command(&[b"LOAD".to_vec(), script]).starts_with(b"$40\r\n"));
        assert_eq!(
            beta.script_command(&[b"EXISTS".to_vec(), sha.as_bytes().to_vec()]),
            b"*1\r\n:0\r\n"
        );
        assert_eq!(
            alpha.script_command(&[b"EXISTS".to_vec(), sha.as_bytes().to_vec()]),
            b"*1\r\n:1\r\n"
        );
        assert_eq!(beta.script_command(&[b"FLUSH".to_vec()]), b"+OK\r\n");
        assert_eq!(alpha.script_command(&[b"EXISTS".to_vec(), sha.into_bytes()]), b"*1\r\n:1\r\n");

        for index in 0..RESP_SCRIPT_CACHE_MAX_ENTRIES.saturating_sub(1) {
            let script = format!("return {}", index + 10_000).into_bytes();
            assert!(alpha.script_command(&[b"LOAD".to_vec(), script]).starts_with(b"$"));
        }
        assert_eq!(
            alpha.script_command(&[b"LOAD".to_vec(), b"return overflow".to_vec()]),
            b"-ERR script cache limit\r\n"
        );
    }

    #[test]
    fn script_cache_rejects_excess_bytes() {
        let mut scripts = std::collections::HashMap::new();
        scripts.insert(
            (String::from("alpha"), String::from("sha")),
            vec![0; RESP_SCRIPT_CACHE_MAX_BYTES],
        );
        assert!(!script_cache_has_capacity(
            &scripts,
            &(String::from("alpha"), String::from("new")),
            1
        ));
    }

    #[test]
    fn pubsub_subscriptions_are_bounded_per_connection() {
        let gateway = RespGateway::new(String::from("tenant"), String::from("database"));
        let mut client = gateway.fresh_client();
        let channels = (0..RESP_MAX_PUBSUB_SUBSCRIPTIONS)
            .map(|index| format!("channel-{index}").into_bytes())
            .collect::<Vec<_>>();
        assert!(!gateway.subscribe(&channels, &mut client).starts_with(b"-ERR"));
        assert_eq!(client.subscriptions.len(), RESP_MAX_PUBSUB_SUBSCRIPTIONS);
        assert_eq!(
            gateway.subscribe(&[b"one-more".to_vec()], &mut client),
            b"-ERR pubsub subscription limit exceeded\r\n"
        );
        assert_eq!(
            gateway.subscribe(&[vec![b'x'; RESP_MAX_PUBSUB_NAME_BYTES + 1]], &mut client),
            b"-ERR pubsub channel or pattern is too long\r\n"
        );
    }

    #[test]
    fn cluster_bridge_registry_is_bounded() {
        let bus = new_pubsub();
        for index in 0..RESP_MAX_CLUSTER_BRIDGES {
            assert!(bus.reserve_cluster_bridge(&format!("tenant-{index}")));
        }
        assert!(!bus.reserve_cluster_bridge("overflow"));
        assert_eq!(bus.cluster_started.lock().unwrap().len(), RESP_MAX_CLUSTER_BRIDGES);
        assert!(!bus.reserve_cluster_bridge("tenant-0"));
    }
}
