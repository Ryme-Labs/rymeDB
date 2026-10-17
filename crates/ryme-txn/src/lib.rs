use ryme_error::{Result, RymeError};
use ryme_storage::{Engine, RecordKey, SegmentCacheStats, SegmentEntry, SegmentStore, StorageMode};
use ryme_wal::Wal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock, Weak};

pub const MAX_TRANSACTION_WRITE_ENTRIES: usize = 100_000;
pub const MAX_TRANSACTION_READ_KEYS: usize = 100_000;
pub const MAX_TRANSACTION_SCANNED_TABLES: usize = 10_000;
pub const MAX_TRANSACTION_CONSTRAINT_SCANS: usize = 10_000;
pub const MAX_TRANSACTION_STATE_BYTES: usize = 64 * 1024 * 1024;

const TRANSACTION_ENTRY_OVERHEAD: usize = 64;
const MVCC_GC_KEYS_PER_LOCK: usize = 1_024;
const MVCC_GC_VERSIONS_PER_LOCK: usize = 4_096;
const MIN_ORDERED_BATCH_READ_KEYS: usize = 16;

fn transaction_key_bytes(key: &RecordKey) -> usize {
    TRANSACTION_ENTRY_OVERHEAD
        .saturating_add(key.tenant.len())
        .saturating_add(key.database.len())
        .saturating_add(key.table.len())
        .saturating_add(key.pk.len())
}

fn transaction_scope_bytes(tenant: &str, database: &str, table: &str) -> usize {
    TRANSACTION_ENTRY_OVERHEAD
        .saturating_add(tenant.len())
        .saturating_add(database.len())
        .saturating_add(table.len())
}

fn transaction_write_bytes(key: &RecordKey, op: &WriteOp) -> usize {
    transaction_key_bytes(key)
        .saturating_add(16)
        .saturating_add(op.value.as_ref().map_or(0, Vec::len))
}

fn transaction_overload(kind: &str) -> RymeError {
    RymeError::Overload(format!("transaction {kind}"))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn snapshot_file_name(commit_ts: u64) -> String {
    format!("snap-{commit_ts:020}.rsnap")
}

fn snapshot_commit(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("snap-")?.strip_suffix(".rsnap")?;
    (rest.len() == 20 && rest.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| rest.parse().ok())
        .flatten()
}

fn publish_snapshot_pointer(dir: &Path, name: &str) -> Result<()> {
    if snapshot_commit(name).is_none() {
        return Err(RymeError::Internal(String::from("snapshot name")));
    }
    let temporary = dir.join("latest.tmp");
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(name.as_bytes())?;
        file.sync_data()?;
    }
    std::fs::rename(temporary, dir.join("latest"))?;
    sync_directory(dir)
}

fn write_snapshot_file(dir: &Path, commit_ts: u64, raw: &[u8]) -> Result<std::path::PathBuf> {
    if raw.len() > ryme_storage::MAX_SNAPSHOT_BYTES {
        return Err(RymeError::Overload(String::from("snapshot")));
    }
    let name = snapshot_file_name(commit_ts);
    let path = dir.join(&name);
    let temporary = dir.join(format!(".{name}.tmp"));
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(raw)?;
        file.sync_data()?;
    }
    std::fs::rename(temporary, &path)?;
    sync_directory(dir)?;
    Ok(path)
}

fn read_snapshot_file(path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;

    let file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    if length > ryme_storage::MAX_SNAPSHOT_BYTES as u64 {
        return Err(RymeError::Overload(String::from("snapshot")));
    }
    let mut raw = Vec::with_capacity(length as usize);
    file.take(ryme_storage::MAX_SNAPSHOT_BYTES as u64 + 1).read_to_end(&mut raw)?;
    if raw.len() > ryme_storage::MAX_SNAPSHOT_BYTES {
        return Err(RymeError::Overload(String::from("snapshot")));
    }
    Ok(raw)
}

#[derive(Debug, Default)]
struct ActiveRegistry {
    transactions: Mutex<ActiveTransactions>,
}

#[derive(Debug, Default)]
struct ActiveTransactions {
    by_id: BTreeMap<u64, u64>,
    by_read_ts: BTreeMap<u64, usize>,
}

impl ActiveRegistry {
    fn insert(&self, id: u64, read_ts: u64) {
        self.insert_with(id, || read_ts);
    }

    fn insert_with(&self, id: u64, current_read_ts: impl FnOnce() -> u64) -> u64 {
        let mut transactions = self.transactions.lock().unwrap_or_else(|error| error.into_inner());
        let read_ts = current_read_ts();
        if let Some(previous) = transactions.by_id.insert(id, read_ts) {
            Self::remove_read_ts(&mut transactions.by_read_ts, previous);
        }
        *transactions.by_read_ts.entry(read_ts).or_default() += 1;
        read_ts
    }

    fn remove(&self, id: u64) {
        let mut transactions = self.transactions.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(read_ts) = transactions.by_id.remove(&id) {
            Self::remove_read_ts(&mut transactions.by_read_ts, read_ts);
        }
    }

    fn len(&self) -> usize {
        self.transactions.lock().unwrap_or_else(|error| error.into_inner()).by_id.len()
    }

    fn oldest(&self) -> Option<u64> {
        let transactions = self.transactions.lock().unwrap_or_else(|error| error.into_inner());
        transactions.by_read_ts.first_key_value().map(|(ts, _)| *ts)
    }

    fn remove_read_ts(read_timestamps: &mut BTreeMap<u64, usize>, read_ts: u64) {
        let remove = if let Some(count) = read_timestamps.get_mut(&read_ts) {
            if *count == 1 {
                true
            } else {
                *count -= 1;
                false
            }
        } else {
            false
        };
        if remove {
            read_timestamps.remove(&read_ts);
        }
    }
}

/// Shared active-transaction tracking for managers that participate in one snapshot domain.
#[derive(Debug, Clone, Default)]
pub struct TxnTracker {
    active: Arc<ActiveRegistry>,
}

impl TxnTracker {
    pub fn new() -> Self {
        Self::default()
    }
}

static ACTIVE_REGISTRIES: LazyLock<Mutex<Vec<Weak<ActiveRegistry>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

fn register_active_registry(registry: &Arc<ActiveRegistry>) {
    if let Ok(mut registries) = ACTIVE_REGISTRIES.lock() {
        registries.retain(|registry| registry.strong_count() > 0);
        if !registries.iter().any(|existing| existing.as_ptr() == Arc::as_ptr(registry)) {
            registries.push(Arc::downgrade(registry));
        }
    }
}

fn active_registries() -> Vec<Arc<ActiveRegistry>> {
    let Ok(mut registries) = ACTIVE_REGISTRIES.lock() else {
        return Vec::new();
    };
    let mut live = Vec::with_capacity(registries.len());
    registries.retain(|registry| {
        if let Some(registry) = registry.upgrade() {
            live.push(registry);
            true
        } else {
            false
        }
    });
    live
}

static COMMIT_NOTIFICATIONS: LazyLock<tokio::sync::watch::Sender<u64>> = LazyLock::new(|| {
    let (sender, _) = tokio::sync::watch::channel(0);
    sender
});

pub fn subscribe_commits() -> tokio::sync::watch::Receiver<u64> {
    COMMIT_NOTIFICATIONS.subscribe()
}

fn publish_commit(commit_ts: u64) {
    COMMIT_NOTIFICATIONS.send_replace(commit_ts);
}

#[derive(Debug, Clone)]
pub struct TxnManager {
    inner: Arc<TxnInner>,
}

struct PendingCommitGuard {
    manager: TxnManager,
    commit_ts: u64,
}

impl Drop for PendingCommitGuard {
    fn drop(&mut self) {
        self.manager.clear_pending(self.commit_ts);
    }
}

#[derive(Debug)]
struct TxnInner {
    clock: AtomicU64,
    applied: AtomicU64,
    pending: Mutex<BTreeMap<u64, BTreeSet<RecordKey>>>,
    engine: RwLock<Engine>,
    // This gate owns commit ordering and its conflict-history index.
    commit: Mutex<CommitIndex>,
    active: Arc<ActiveRegistry>,
}

#[derive(Debug, Default)]
struct CommitIndex {
    by_timestamp: BTreeMap<u64, Vec<RecordKey>>,
    by_key: BTreeMap<RecordKey, Vec<u64>>,
    by_table: BTreeMap<(String, String, String), Vec<u64>>,
    pruned_through: u64,
}

impl CommitIndex {
    fn record(&mut self, commit_ts: u64, keys: Vec<RecordKey>) {
        for key in &keys {
            record_commit_timestamp(self.by_key.entry(key.clone()).or_default(), commit_ts);
            record_commit_timestamp(
                self.by_table
                    .entry((key.tenant.clone(), key.database.clone(), key.table.clone()))
                    .or_default(),
                commit_ts,
            );
        }
        if !keys.is_empty() {
            self.by_timestamp.entry(commit_ts).or_default().extend(keys);
        }
    }

    fn prune_through(&mut self, floor: u64) {
        self.pruned_through = self.pruned_through.max(floor);
        let mut affected_tables = BTreeSet::new();
        while self.by_timestamp.first_key_value().is_some_and(|(ts, _)| *ts <= floor) {
            let Some((commit_ts, keys)) = self.by_timestamp.pop_first() else {
                break;
            };
            for key in keys {
                let remove_key = self.by_key.get_mut(&key).is_some_and(|history| {
                    if let Ok(index) = history.binary_search(&commit_ts) {
                        history.remove(index);
                    }
                    history.is_empty()
                });
                if remove_key {
                    self.by_key.remove(&key);
                }
                affected_tables.insert((key.tenant, key.database, key.table));
            }
        }
        prune_commit_histories(&mut self.by_table, affected_tables, floor);
    }

    fn clear_through(&mut self, floor: u64) {
        self.by_timestamp.clear();
        self.by_key.clear();
        self.by_table.clear();
        self.pruned_through = floor;
    }
}

fn record_commit_timestamp(history: &mut Vec<u64>, commit_ts: u64) {
    if history.last().is_some_and(|latest| *latest < commit_ts) {
        history.push(commit_ts);
        return;
    }
    if let Err(index) = history.binary_search(&commit_ts) {
        history.insert(index, commit_ts);
    }
}

fn has_commit_between(history: &[u64], read_ts: u64, commit_ts: u64) -> bool {
    let index = history.partition_point(|timestamp| *timestamp <= read_ts);
    history.get(index).is_some_and(|timestamp| *timestamp < commit_ts)
}

fn prune_commit_histories<K: Ord>(
    histories: &mut BTreeMap<K, Vec<u64>>,
    keys: BTreeSet<K>,
    floor: u64,
) {
    for key in keys {
        let remove = histories.get_mut(&key).is_some_and(|history| {
            let count = history.partition_point(|timestamp| *timestamp <= floor);
            history.drain(..count);
            history.is_empty()
        });
        if remove {
            histories.remove(&key);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteOp {
    pub value: Option<Vec<u8>>,
    pub expires_at: u64,
}

impl WriteOp {
    pub fn put(value: Vec<u8>) -> Self {
        Self { value: Some(value), expires_at: 0 }
    }

    pub fn put_ttl(value: Vec<u8>, expires_at: u64) -> Self {
        Self { value: Some(value), expires_at }
    }

    pub fn delete() -> Self {
        Self { value: None, expires_at: 0 }
    }
}

static TXN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct TxnPin {
    id: u64,
    active: Arc<ActiveRegistry>,
    additional: Mutex<Vec<Arc<ActiveRegistry>>>,
}

impl TxnPin {
    fn track(&self, active: &Arc<ActiveRegistry>, read_ts: u64) {
        if Arc::ptr_eq(&self.active, active) {
            return;
        }
        let mut additional = self.additional.lock().unwrap_or_else(|error| error.into_inner());
        if additional.iter().any(|registry| Arc::ptr_eq(registry, active)) {
            return;
        }
        active.insert(self.id, read_ts);
        additional.push(Arc::clone(active));
    }

    fn restamp(&self, read_ts: u64) {
        self.active.insert(self.id, read_ts);
        let additional = self.additional.lock().unwrap_or_else(|error| error.into_inner());
        for registry in additional.iter() {
            registry.insert(self.id, read_ts);
        }
    }
}

impl Drop for TxnPin {
    fn drop(&mut self) {
        self.active.remove(self.id);
        let additional = self.additional.get_mut().unwrap_or_else(|error| error.into_inner());
        for registry in additional.iter() {
            registry.remove(self.id);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Isolation {
    Serializable,
    Snapshot,
}

#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    pub read_ts: u64,
    writes: BTreeMap<RecordKey, WriteOp>,
    read_set: BTreeSet<RecordKey>,
    observed_missing: BTreeSet<RecordKey>,
    scanned: BTreeSet<(String, String, String)>,
    constraint_scanned: BTreeSet<(String, String, String)>,
    isolation: Isolation,
    read_only: bool,
    route_generation: Option<u64>,
    state_bytes: usize,
    resource_error: Option<RymeError>,
    #[allow(dead_code)]
    pin: Arc<TxnPin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionState {
    pub read_ts: u64,
    pub writes: Vec<(RecordKey, WriteOp)>,
    pub read_set: Vec<RecordKey>,
    pub observed: Vec<(RecordKey, bool)>,
    pub scanned: Vec<(String, String, String)>,
    #[serde(default)]
    pub constraint_scanned: Vec<(String, String, String)>,
    pub isolation: Isolation,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub route_generation: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TransactionCheckpoint {
    read_ts: u64,
    writes: BTreeMap<RecordKey, WriteOp>,
    read_set: BTreeSet<RecordKey>,
    observed_missing: BTreeSet<RecordKey>,
    scanned: BTreeSet<(String, String, String)>,
    constraint_scanned: BTreeSet<(String, String, String)>,
    isolation: Isolation,
    read_only: bool,
    route_generation: Option<u64>,
    state_bytes: usize,
    resource_error: Option<RymeError>,
}

impl TransactionCheckpoint {
    pub fn state_bytes(&self) -> usize {
        self.state_bytes
    }
}

impl Transaction {
    pub fn checkpoint(&self) -> TransactionCheckpoint {
        TransactionCheckpoint {
            read_ts: self.read_ts,
            writes: self.writes.clone(),
            read_set: self.read_set.clone(),
            observed_missing: self.observed_missing.clone(),
            scanned: self.scanned.clone(),
            constraint_scanned: self.constraint_scanned.clone(),
            isolation: self.isolation,
            read_only: self.read_only,
            route_generation: self.route_generation,
            state_bytes: self.state_bytes,
            resource_error: self.resource_error.clone(),
        }
    }

    pub fn state_bytes(&self) -> usize {
        self.state_bytes
    }

    pub fn state(&self) -> TransactionState {
        let mut missing = self.observed_missing.iter().peekable();
        let observed = self
            .read_set
            .iter()
            .map(|key| {
                let existed = if missing.peek().is_some_and(|missing_key| *missing_key == key) {
                    missing.next();
                    false
                } else {
                    true
                };
                (key.clone(), existed)
            })
            .collect();
        TransactionState {
            read_ts: self.read_ts,
            writes: self.writes.iter().map(|(key, op)| (key.clone(), op.clone())).collect(),
            read_set: self.read_set.iter().cloned().collect(),
            observed,
            scanned: self.scanned.iter().cloned().collect(),
            constraint_scanned: self.constraint_scanned.iter().cloned().collect(),
            isolation: self.isolation,
            read_only: self.read_only,
            route_generation: self.route_generation,
        }
    }

    pub fn restore_checkpoint(&mut self, checkpoint: &TransactionCheckpoint) {
        self.read_ts = checkpoint.read_ts;
        self.writes.clone_from(&checkpoint.writes);
        self.read_set.clone_from(&checkpoint.read_set);
        self.observed_missing.clone_from(&checkpoint.observed_missing);
        self.scanned.clone_from(&checkpoint.scanned);
        self.constraint_scanned.clone_from(&checkpoint.constraint_scanned);
        self.isolation = checkpoint.isolation;
        self.read_only = checkpoint.read_only;
        self.route_generation = checkpoint.route_generation;
        self.state_bytes = checkpoint.state_bytes;
        self.resource_error.clone_from(&checkpoint.resource_error);
        self.pin.restamp(self.read_ts);
    }

    pub fn restamp(&mut self, read_ts: u64) {
        self.read_ts = read_ts;
        self.pin.restamp(read_ts);
    }

    pub fn set_isolation(&mut self, isolation: Isolation) {
        self.isolation = isolation;
    }

    /// Disables conflict-observation tracking for a transaction that cannot write.
    pub fn set_read_only(&mut self) -> Result<()> {
        self.ensure_healthy()?;
        if !self.writes.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("read-only transaction")));
        }
        self.read_only = true;
        Ok(())
    }

    pub fn set_route_generation(&mut self, generation: u64) {
        self.route_generation = Some(generation);
    }

    pub fn route_generation(&self) -> Option<u64> {
        self.route_generation
    }

    pub fn writes(&self) -> &BTreeMap<RecordKey, WriteOp> {
        &self.writes
    }

    pub fn read_keys(&self) -> &BTreeSet<RecordKey> {
        &self.read_set
    }

    pub fn observed_existed(&self, key: &RecordKey) -> Option<bool> {
        self.read_set.contains(key).then(|| !self.observed_missing.contains(key))
    }

    pub fn record_read(&mut self, key: RecordKey, existed: bool) -> Result<()> {
        self.record_read_ref(&key, existed)
    }

    fn record_read_ref(&mut self, key: &RecordKey, existed: bool) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        if !self.read_set.insert(key.clone()) {
            return Ok(());
        }
        let key_bytes = transaction_key_bytes(key);
        let added_bytes = key_bytes.saturating_add(key_bytes.saturating_add(1));
        if let Err(error) = self.ensure_growth(
            self.read_set.len() <= MAX_TRANSACTION_READ_KEYS,
            added_bytes,
            "read set",
        ) {
            self.read_set.remove(key);
            return Err(error);
        }
        if !existed {
            self.observed_missing.insert(key.clone());
        }
        self.state_bytes = self.state_bytes.saturating_add(added_bytes);
        Ok(())
    }

    pub fn record_scan(&mut self, tenant: &str, database: &str, table: &str) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let scope = (tenant.to_string(), database.to_string(), table.to_string());
        if self.scanned.contains(&scope) {
            return Ok(());
        }
        self.ensure_growth(
            self.scanned.len() < MAX_TRANSACTION_SCANNED_TABLES,
            transaction_scope_bytes(tenant, database, table),
            "scan set",
        )?;
        self.scanned.insert(scope);
        self.state_bytes =
            self.state_bytes.saturating_add(transaction_scope_bytes(tenant, database, table));
        Ok(())
    }

    pub fn record_constraint_scan(
        &mut self,
        tenant: &str,
        database: &str,
        table: &str,
    ) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let scope = (tenant.to_string(), database.to_string(), table.to_string());
        if self.constraint_scanned.contains(&scope) {
            return Ok(());
        }
        self.ensure_growth(
            self.constraint_scanned.len() < MAX_TRANSACTION_CONSTRAINT_SCANS,
            transaction_scope_bytes(tenant, database, table),
            "constraint scan set",
        )?;
        self.constraint_scanned.insert(scope);
        self.state_bytes =
            self.state_bytes.saturating_add(transaction_scope_bytes(tenant, database, table));
        Ok(())
    }

    fn ensure_growth(&mut self, count_ok: bool, added_bytes: usize, kind: &str) -> Result<()> {
        if let Some(error) = self.resource_error.clone() {
            return Err(error);
        }
        if !count_ok {
            let error = transaction_overload(kind);
            self.resource_error = Some(error.clone());
            return Err(error);
        }
        if self.state_bytes.saturating_add(added_bytes) > MAX_TRANSACTION_STATE_BYTES {
            let error = transaction_overload("state bytes");
            self.resource_error = Some(error.clone());
            return Err(error);
        }
        Ok(())
    }

    pub fn ensure_healthy(&self) -> Result<()> {
        self.resource_error.clone().map_or(Ok(()), Err)
    }

    fn stage_write(&mut self, key: RecordKey, op: WriteOp) -> Result<()> {
        if self.read_only {
            let error = RymeError::InvalidArgument(String::from("read-only transaction"));
            self.resource_error = Some(error.clone());
            return Err(error);
        }
        if let Some(error) = self.resource_error.clone() {
            return Err(error);
        }
        let old_bytes =
            self.writes.get(&key).map(|old| transaction_write_bytes(&key, old)).unwrap_or(0);
        let new_bytes = transaction_write_bytes(&key, &op);
        let next_bytes = self.state_bytes.saturating_sub(old_bytes).saturating_add(new_bytes);
        let next_count =
            self.writes.len().saturating_add(usize::from(!self.writes.contains_key(&key)));
        if next_count > MAX_TRANSACTION_WRITE_ENTRIES {
            let error = transaction_overload("write set");
            self.resource_error = Some(error.clone());
            return Err(error);
        }
        if next_bytes > MAX_TRANSACTION_STATE_BYTES {
            let error = transaction_overload("state bytes");
            self.resource_error = Some(error.clone());
            return Err(error);
        }
        self.writes.insert(key, op);
        self.state_bytes = next_bytes;
        Ok(())
    }

    pub fn scanned_tables(&self) -> &BTreeSet<(String, String, String)> {
        &self.scanned
    }

    pub fn scan_overlay_count(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: Option<&[u8]>,
    ) -> usize {
        let start = RecordKey::new(tenant, database, table, start_after.unwrap_or_default());
        let start_bound = start_after.map(|_| Excluded(&start)).unwrap_or(Included(&start));
        self.writes
            .range((start_bound, Unbounded))
            .take_while(|(key, _)| {
                key.tenant == tenant && key.database == database && key.table == table
            })
            .count()
    }

    pub fn overlay_scan(
        &self,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = rows.into_iter().collect();
        let now = now_unix();
        let start = RecordKey::new(tenant, database, table, start_after.unwrap_or_default());
        let start_bound = start_after.map(|_| Excluded(&start)).unwrap_or(Included(&start));
        for (key, op) in self.writes.range((start_bound, Unbounded)).take_while(|(key, _)| {
            key.tenant == tenant && key.database == database && key.table == table
        }) {
            let visible = op.expires_at == 0 || op.expires_at > now;
            match (visible, op.value.as_ref()) {
                (true, Some(value)) => {
                    merged.insert(key.pk.clone(), value.clone());
                }
                _ => {
                    merged.remove(&key.pk);
                }
            }
        }
        merged.into_iter().take(limit).collect()
    }

    pub fn isolation(&self) -> Isolation {
        self.isolation
    }

    pub fn active_count() -> usize {
        active_registries().iter().map(|registry| registry.len()).sum()
    }

    pub fn oldest_active() -> Option<u64> {
        active_registries().iter().filter_map(|registry| registry.oldest()).min()
    }
}

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

type ScanRows = Vec<(Vec<u8>, Vec<u8>)>;
type TxnFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait TxnBackend: Clone + Send + Sync + 'static {
    fn begin(&self) -> Transaction;
    fn begin_with(&self, isolation: Isolation) -> Transaction {
        let mut txn = self.begin();
        txn.set_isolation(isolation);
        txn
    }
    fn get(&self, txn: &mut Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>>;
    fn get_many(&self, txn: &mut Transaction, keys: &[RecordKey]) -> Result<Vec<Option<Vec<u8>>>> {
        keys.iter().map(|key| self.get(txn, key)).collect()
    }
    fn get_async<'a>(
        &'a self,
        txn: &'a mut Transaction,
        key: &'a RecordKey,
    ) -> TxnFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.get(txn, key) })
    }
    fn get_many_async<'a>(
        &'a self,
        txn: &'a mut Transaction,
        keys: &'a [RecordKey],
    ) -> TxnFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(self.get_async(txn, key).await?);
            }
            Ok(values)
        })
    }
    fn scan_async<'a>(
        &'a self,
        txn: &'a mut Transaction,
        tenant: &'a str,
        database: &'a str,
        table: &'a str,
        limit: usize,
    ) -> TxnFuture<'a, ScanRows> {
        Box::pin(async move { self.scan(txn, tenant, database, table, limit) })
    }
    fn scan_after_async<'a>(
        &'a self,
        txn: &'a mut Transaction,
        tenant: &'a str,
        database: &'a str,
        table: &'a str,
        start_after: &'a [u8],
        limit: usize,
    ) -> TxnFuture<'a, ScanRows> {
        Box::pin(async move { self.scan_after(txn, tenant, database, table, start_after, limit) })
    }
    fn put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>);
    fn put_with_ttl(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>, expires_at: u64);
    fn delete(&self, txn: &mut Transaction, key: RecordKey);
    fn try_put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) -> Result<()> {
        self.put(txn, key, value);
        txn.ensure_healthy()
    }
    fn try_put_with_ttl(
        &self,
        txn: &mut Transaction,
        key: RecordKey,
        value: Vec<u8>,
        expires_at: u64,
    ) -> Result<()> {
        self.put_with_ttl(txn, key, value, expires_at);
        txn.ensure_healthy()
    }
    fn try_delete(&self, txn: &mut Transaction, key: RecordKey) -> Result<()> {
        self.delete(txn, key);
        txn.ensure_healthy()
    }
    fn expires_at(&self, key: &RecordKey) -> Result<Option<u64>>;
    fn expires_at_at(&self, key: &RecordKey, _read_ts: u64) -> Result<Option<u64>> {
        self.expires_at(key)
    }
    fn commit(&self, txn: Transaction) -> impl std::future::Future<Output = Result<u64>> + Send;
    fn scan(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn scan_after(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

impl TxnBackend for TxnManager {
    fn begin(&self) -> Transaction {
        TxnManager::begin(self)
    }

    fn get(&self, txn: &mut Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        TxnManager::get(self, txn, key)
    }

    fn get_many(&self, txn: &mut Transaction, keys: &[RecordKey]) -> Result<Vec<Option<Vec<u8>>>> {
        TxnManager::get_many(self, txn, keys)
    }

    fn get_many_async<'a>(
        &'a self,
        txn: &'a mut Transaction,
        keys: &'a [RecordKey],
    ) -> TxnFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move { TxnManager::get_many(self, txn, keys) })
    }

    fn put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) {
        TxnManager::put(self, txn, key, value)
    }

    fn put_with_ttl(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>, expires_at: u64) {
        TxnManager::put_with_ttl(self, txn, key, value, expires_at)
    }

    fn delete(&self, txn: &mut Transaction, key: RecordKey) {
        TxnManager::delete(self, txn, key)
    }

    fn expires_at(&self, key: &RecordKey) -> Result<Option<u64>> {
        TxnManager::expires_at(self, key)
    }

    fn expires_at_at(&self, key: &RecordKey, read_ts: u64) -> Result<Option<u64>> {
        TxnManager::expires_at_at(self, key, read_ts)
    }

    fn commit(&self, txn: Transaction) -> impl std::future::Future<Output = Result<u64>> + Send {
        let manager = self.clone();
        async move { TxnManager::commit(&manager, txn) }
    }

    fn scan(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        TxnManager::scan(self, txn, tenant, database, table, limit)
    }

    fn scan_after(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        TxnManager::scan_after(self, txn, tenant, database, table, start_after, limit)
    }
}

impl TxnBackend for DurableManager {
    fn begin(&self) -> Transaction {
        DurableManager::begin(self)
    }

    fn get(&self, txn: &mut Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        DurableManager::get(self, txn, key)
    }

    fn get_many(&self, txn: &mut Transaction, keys: &[RecordKey]) -> Result<Vec<Option<Vec<u8>>>> {
        DurableManager::get_many(self, txn, keys)
    }

    fn get_many_async<'a>(
        &'a self,
        txn: &'a mut Transaction,
        keys: &'a [RecordKey],
    ) -> TxnFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move { DurableManager::get_many(self, txn, keys) })
    }

    fn put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) {
        DurableManager::put(self, txn, key, value)
    }

    fn put_with_ttl(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>, expires_at: u64) {
        DurableManager::put_with_ttl(self, txn, key, value, expires_at)
    }

    fn delete(&self, txn: &mut Transaction, key: RecordKey) {
        DurableManager::delete(self, txn, key)
    }

    fn expires_at(&self, key: &RecordKey) -> Result<Option<u64>> {
        DurableManager::expires_at(self, key)
    }

    fn expires_at_at(&self, key: &RecordKey, read_ts: u64) -> Result<Option<u64>> {
        DurableManager::expires_at_at(self, key, read_ts)
    }

    fn commit(&self, txn: Transaction) -> impl std::future::Future<Output = Result<u64>> + Send {
        let manager = self.clone();
        async move { DurableManager::commit(&manager, txn) }
    }

    fn scan(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        DurableManager::scan(self, txn, tenant, database, table, limit)
    }

    fn scan_after(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        DurableManager::scan_after(self, txn, tenant, database, table, start_after, limit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    Always,
    Never,
}

impl TxnManager {
    pub fn new() -> Self {
        Self::new_with_tracker(&TxnTracker::new())
    }

    pub fn new_with_tracker(tracker: &TxnTracker) -> Self {
        let active = Arc::clone(&tracker.active);
        register_active_registry(&active);
        Self {
            inner: Arc::new(TxnInner {
                clock: AtomicU64::new(1),
                applied: AtomicU64::new(1),
                pending: Mutex::new(BTreeMap::new()),
                engine: RwLock::new(Engine::new()),
                commit: Mutex::new(CommitIndex::default()),
                active,
            }),
        }
    }

    pub fn begin(&self) -> Transaction {
        let id = TXN_ID.fetch_add(1, Ordering::Relaxed);
        let pin = Arc::new(TxnPin {
            id,
            active: Arc::clone(&self.inner.active),
            additional: Mutex::new(Vec::new()),
        });
        let read_ts = pin.active.insert_with(id, || self.inner.applied.load(Ordering::SeqCst));
        Transaction {
            id,
            read_ts,
            writes: BTreeMap::new(),
            read_set: BTreeSet::new(),
            observed_missing: BTreeSet::new(),
            scanned: BTreeSet::new(),
            constraint_scanned: BTreeSet::new(),
            isolation: Isolation::Serializable,
            read_only: false,
            route_generation: None,
            state_bytes: 0,
            resource_error: None,
            pin,
        }
    }

    /// Keeps a transaction snapshot visible to this manager's garbage collector.
    /// Use when one transaction reads or writes across managers in the same timestamp domain.
    pub fn track_transaction(&self, txn: &mut Transaction) {
        txn.pin.track(&self.inner.active, txn.read_ts);
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn from_state(&self, state: TransactionState) -> Result<Transaction> {
        if state.writes.len() > MAX_TRANSACTION_WRITE_ENTRIES {
            return Err(transaction_overload("write set"));
        }
        if state.read_set.len() > MAX_TRANSACTION_READ_KEYS
            || state.observed.len() > MAX_TRANSACTION_READ_KEYS
        {
            return Err(transaction_overload("read set"));
        }
        if state.scanned.len() > MAX_TRANSACTION_SCANNED_TABLES {
            return Err(transaction_overload("scan set"));
        }
        if state.constraint_scanned.len() > MAX_TRANSACTION_CONSTRAINT_SCANS {
            return Err(transaction_overload("constraint scan set"));
        }
        if state.read_only && !state.writes.is_empty() {
            return Err(RymeError::Corrupt(String::from("read-only transaction writes")));
        }
        let read_set_entries = state.read_set.len();
        let observed_entries = state.observed.len();
        let writes: BTreeMap<_, _> = state.writes.into_iter().collect();
        let read_set: BTreeSet<_> = state.read_set.into_iter().collect();
        let observed: BTreeMap<_, _> = state.observed.into_iter().collect();
        if read_set.len() != read_set_entries
            || observed.len() != observed_entries
            || !read_set.iter().eq(observed.keys())
        {
            return Err(RymeError::Corrupt(String::from("transaction read state")));
        }
        let observed_missing = observed
            .iter()
            .filter(|entry| !*entry.1)
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>();
        let scanned: BTreeSet<_> = state.scanned.into_iter().collect();
        let constraint_scanned: BTreeSet<_> = state.constraint_scanned.into_iter().collect();
        let state_bytes =
            writes
                .iter()
                .map(|(key, op)| transaction_write_bytes(key, op))
                .chain(read_set.iter().map(transaction_key_bytes))
                .chain(observed.keys().map(|key| transaction_key_bytes(key).saturating_add(1)))
                .chain(scanned.iter().map(|(tenant, database, table)| {
                    transaction_scope_bytes(tenant, database, table)
                }))
                .chain(constraint_scanned.iter().map(|(tenant, database, table)| {
                    transaction_scope_bytes(tenant, database, table)
                }))
                .try_fold(0usize, |total, bytes| total.checked_add(bytes))
                .ok_or_else(|| transaction_overload("state bytes"))?;
        if writes.len() > MAX_TRANSACTION_WRITE_ENTRIES
            || read_set.len() > MAX_TRANSACTION_READ_KEYS
            || observed.len() > MAX_TRANSACTION_READ_KEYS
            || scanned.len() > MAX_TRANSACTION_SCANNED_TABLES
            || constraint_scanned.len() > MAX_TRANSACTION_CONSTRAINT_SCANS
            || state_bytes > MAX_TRANSACTION_STATE_BYTES
        {
            return Err(transaction_overload("state bytes"));
        }
        let id = TXN_ID.fetch_add(1, Ordering::Relaxed);
        let pin = Arc::new(TxnPin {
            id,
            active: Arc::clone(&self.inner.active),
            additional: Mutex::new(Vec::new()),
        });
        pin.active.insert(id, state.read_ts);
        Ok(Transaction {
            id,
            read_ts: state.read_ts,
            writes,
            read_set,
            observed_missing,
            scanned,
            constraint_scanned,
            isolation: state.isolation,
            read_only: state.read_only,
            route_generation: state.route_generation,
            state_bytes,
            resource_error: None,
            pin,
        })
    }

    pub fn get(&self, txn: &mut Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        txn.ensure_healthy()?;
        if let Some(staged) = txn.writes.get(key) {
            if staged.expires_at != 0 && staged.expires_at <= now_unix() {
                return Ok(None);
            }
            return Ok(staged.value.clone());
        }
        let value = {
            let engine = self
                .inner
                .engine
                .read()
                .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
            engine.read(key, txn.read_ts, now_unix())?
        };
        txn.record_read_ref(key, value.is_some())?;
        Ok(value)
    }

    pub fn get_many(
        &self,
        txn: &mut Transaction,
        keys: &[RecordKey],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        txn.ensure_healthy()?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let now = now_unix();
        if txn.writes.is_empty() {
            let values = {
                let engine = self
                    .inner
                    .engine
                    .read()
                    .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
                engine.read_many(keys, txn.read_ts, now)?
            };
            if !txn.read_only {
                for (key, value) in keys.iter().zip(&values) {
                    txn.record_read_ref(key, value.is_some())?;
                }
            }
            return Ok(values);
        }

        let mut values = vec![None; keys.len()];
        {
            let engine = self
                .inner
                .engine
                .read()
                .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
            if keys.len() < MIN_ORDERED_BATCH_READ_KEYS {
                for (index, key) in keys.iter().enumerate() {
                    values[index] = if let Some(staged) = txn.writes.get(key) {
                        if staged.expires_at != 0 && staged.expires_at <= now {
                            None
                        } else {
                            staged.value.clone()
                        }
                    } else {
                        engine.read(key, txn.read_ts, now)?
                    };
                }
            } else {
                let mut pending_indices = Vec::with_capacity(keys.len());
                let mut pending_keys = Vec::with_capacity(keys.len());
                for (index, key) in keys.iter().enumerate() {
                    if let Some(staged) = txn.writes.get(key) {
                        values[index] = if staged.expires_at != 0 && staged.expires_at <= now {
                            None
                        } else {
                            staged.value.clone()
                        };
                    } else {
                        pending_indices.push(index);
                        pending_keys.push(key);
                    }
                }
                if !pending_keys.is_empty() {
                    let pending_values =
                        engine.read_many_borrowed(&pending_keys, txn.read_ts, now)?;
                    for (index, value) in pending_indices.into_iter().zip(pending_values) {
                        values[index] = value;
                    }
                }
            }
        }
        for (key, value) in keys.iter().zip(&values) {
            if !txn.writes.contains_key(key) {
                txn.record_read_ref(key, value.is_some())?;
            }
        }
        Ok(values)
    }

    pub fn get_with(
        &self,
        txn: &mut Transaction,
        key: &RecordKey,
        fallback: impl FnOnce() -> Result<Option<Vec<u8>>>,
    ) -> Result<Option<Vec<u8>>> {
        txn.ensure_healthy()?;
        if let Some(staged) = txn.writes.get(key) {
            if staged.expires_at != 0 && staged.expires_at <= now_unix() {
                return Ok(None);
            }
            return Ok(staged.value.clone());
        }
        let now = now_unix();
        let (has_version, value) = {
            let engine = self
                .inner
                .engine
                .read()
                .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
            engine.read_with_presence(key, txn.read_ts, now)?
        };
        let value = if has_version { value } else { fallback()? };
        txn.record_read_ref(key, value.is_some())?;
        Ok(value)
    }

    pub fn try_put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) -> Result<()> {
        txn.stage_write(key, WriteOp::put(value))
    }

    pub fn put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) {
        let _ = self.try_put(txn, key, value);
    }

    pub fn try_put_with_ttl(
        &self,
        txn: &mut Transaction,
        key: RecordKey,
        value: Vec<u8>,
        expires_at: u64,
    ) -> Result<()> {
        txn.stage_write(key, WriteOp::put_ttl(value, expires_at))
    }

    pub fn put_with_ttl(
        &self,
        txn: &mut Transaction,
        key: RecordKey,
        value: Vec<u8>,
        expires_at: u64,
    ) {
        let _ = self.try_put_with_ttl(txn, key, value, expires_at);
    }

    pub fn try_delete(&self, txn: &mut Transaction, key: RecordKey) -> Result<()> {
        txn.stage_write(key, WriteOp::delete())
    }

    pub fn delete(&self, txn: &mut Transaction, key: RecordKey) {
        let _ = self.try_delete(txn, key);
    }

    pub fn reserve(&self, txn: &Transaction) -> Result<u64> {
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        self.reserve_locked(txn, &mut guard)
    }

    fn reserve_locked(&self, txn: &Transaction, committed: &mut CommitIndex) -> Result<u64> {
        txn.ensure_healthy()?;
        if txn.writes.is_empty() {
            return Ok(txn.read_ts);
        }
        let mut observed = self.inner.clock.load(Ordering::SeqCst);
        let commit_ts = loop {
            let base = observed.max(txn.read_ts);
            let next_ts = base
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("commit timestamp overflow")))?;
            match self.inner.clock.compare_exchange_weak(
                observed,
                next_ts,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break next_ts,
                Err(current) => observed = current,
            }
        };
        self.check_locked(txn, commit_ts, committed)?;
        Ok(commit_ts)
    }

    fn check_locked(
        &self,
        txn: &Transaction,
        commit_ts: u64,
        committed: &mut CommitIndex,
    ) -> Result<()> {
        if commit_ts <= txn.read_ts {
            return Err(RymeError::Conflict(String::from(
                "commit timestamp must be newer than transaction snapshot",
            )));
        }
        if txn.read_ts < committed.pruned_through {
            return Err(RymeError::Conflict(String::from("transaction snapshot too old")));
        }
        if let Some(floor) = self.inner.active.oldest() {
            committed.prune_through(floor);
        }
        for key in txn.writes.keys() {
            let conflicts = committed
                .by_key
                .get(key)
                .is_some_and(|timestamps| has_commit_between(timestamps, txn.read_ts, commit_ts));
            if conflicts {
                return Err(RymeError::Conflict(String::from("write-write conflict")));
            }
        }
        if txn.isolation == Isolation::Serializable {
            for key in txn.read_set.iter() {
                let conflicts = committed.by_key.get(key).is_some_and(|timestamps| {
                    has_commit_between(timestamps, txn.read_ts, commit_ts)
                });
                if conflicts {
                    return Err(RymeError::Conflict(String::from("read-write conflict")));
                }
            }
            for table in txn.scanned.iter() {
                let phantom = committed.by_table.get(table).is_some_and(|timestamps| {
                    has_commit_between(timestamps, txn.read_ts, commit_ts)
                });
                if phantom {
                    return Err(RymeError::Conflict(String::from("phantom conflict")));
                }
            }
        }
        for table in txn.constraint_scanned.iter() {
            let conflict = committed
                .by_table
                .get(table)
                .is_some_and(|timestamps| has_commit_between(timestamps, txn.read_ts, commit_ts));
            if conflict {
                return Err(RymeError::Conflict(String::from("constraint conflict")));
            }
        }
        let pending = self
            .inner
            .pending
            .lock()
            .map_err(|_| RymeError::Internal(String::from("pending commits")))?;
        for (_pending_ts, writes) in pending.range(..commit_ts) {
            let write_conflict = txn.writes.keys().any(|key| writes.contains(key));
            let read_conflict = txn.isolation == Isolation::Serializable
                && (txn.read_set.iter().any(|key| writes.contains(key))
                    || txn.scanned.iter().any(|table| {
                        writes.iter().any(|key| {
                            key.tenant == table.0 && key.database == table.1 && key.table == table.2
                        })
                    }));
            let constraint_conflict = txn.constraint_scanned.iter().any(|table| {
                writes.iter().any(|key| {
                    key.tenant == table.0 && key.database == table.1 && key.table == table.2
                })
            });
            if write_conflict || read_conflict || constraint_conflict {
                return Err(RymeError::Conflict(String::from("pending commit conflict")));
            }
        }
        Ok(())
    }

    fn reserve_replicated(&self, txn: &Transaction) -> Result<u64> {
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let commit_ts = self.reserve_locked(txn, &mut guard)?;
        self.inner
            .pending
            .lock()
            .map_err(|_| RymeError::Internal(String::from("pending commits")))?
            .insert(commit_ts, txn.writes.keys().cloned().collect());
        Ok(commit_ts)
    }

    fn clear_pending(&self, commit_ts: u64) {
        if let Ok(mut pending) = self.inner.pending.lock() {
            pending.remove(&commit_ts);
        }
    }

    pub fn validate_at(&self, txn: &Transaction, commit_ts: u64) -> Result<()> {
        txn.ensure_healthy()?;
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        self.check_locked(txn, commit_ts, &mut guard)
    }

    pub fn commit_filtered_at(
        &self,
        txn: &Transaction,
        commit_ts: u64,
        keep: impl Fn(&RecordKey) -> bool,
    ) -> Result<()> {
        txn.ensure_healthy()?;
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        self.check_locked(txn, commit_ts, &mut guard)?;
        let writes: BTreeMap<RecordKey, WriteOp> = txn
            .writes
            .iter()
            .filter(|(key, _)| keep(key))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.apply_owned_locked(commit_ts, writes, &mut guard)?;
        self.inner.clock.fetch_max(commit_ts.saturating_add(1), Ordering::SeqCst);
        Ok(())
    }

    /// Durably commits a filtered owned transaction without cloning its write
    /// map. Validation runs before persistence so a rejected transaction never
    /// leaves a WAL record that could be replayed after restart.
    pub fn commit_filtered_durable_at(
        &self,
        txn: Transaction,
        commit_ts: u64,
        keep: impl Fn(&RecordKey) -> bool,
        persist: impl FnOnce(u64, &[u8], &BTreeMap<RecordKey, WriteOp>) -> Result<()>,
    ) -> Result<()> {
        txn.ensure_healthy()?;
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        self.check_locked(&txn, commit_ts, &mut guard)?;
        let writes: BTreeMap<RecordKey, WriteOp> =
            txn.writes.into_iter().filter(|(key, _)| keep(key)).collect();
        if writes.is_empty() {
            return Ok(());
        }
        let payload = encode_writes(&writes)?;
        persist(commit_ts, &payload, &writes)?;
        self.apply_owned_locked(commit_ts, writes, &mut guard)?;
        self.inner.clock.fetch_max(commit_ts.saturating_add(1), Ordering::SeqCst);
        Ok(())
    }

    pub fn replay_at(&self, commit_ts: u64, writes: &BTreeMap<RecordKey, WriteOp>) -> Result<()> {
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        self.replay_locked(commit_ts, writes, None, &mut guard)
    }

    /// Replays a committed transaction while materializing only the keys owned
    /// by this replica. Conflict metadata still records every key in the
    /// transaction so a later transaction cannot miss a remote write.
    ///
    /// This is deliberately separate from `replay_at`: callers must opt into
    /// ownership-aware storage application explicitly until routing, snapshot
    /// transfer, and realtime delivery all use the same ownership decision.
    pub fn replay_at_filtered(
        &self,
        commit_ts: u64,
        writes: &BTreeMap<RecordKey, WriteOp>,
        keep: impl Fn(&RecordKey) -> bool,
    ) -> Result<()> {
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let local_writes: BTreeMap<RecordKey, WriteOp> = writes
            .iter()
            .filter(|(key, _)| keep(key))
            .map(|(key, op)| (key.clone(), op.clone()))
            .collect();
        let committed_keys: Vec<RecordKey> = writes.keys().cloned().collect();
        self.replay_locked(commit_ts, &local_writes, Some(&committed_keys), &mut guard)
    }

    fn replay_locked(
        &self,
        commit_ts: u64,
        writes: &BTreeMap<RecordKey, WriteOp>,
        committed_keys: Option<&[RecordKey]>,
        committed: &mut CommitIndex,
    ) -> Result<()> {
        let mut engine = self
            .inner
            .engine
            .write()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        let mut fresh: BTreeMap<RecordKey, WriteOp> = BTreeMap::new();
        for (key, op) in writes.iter() {
            match engine.latest_version(key) {
                None => {
                    fresh.insert(key.clone(), op.clone());
                }
                Some((ts, _, _)) if ts < commit_ts => {
                    fresh.insert(key.clone(), op.clone());
                }
                Some((ts, _, _)) if ts > commit_ts => {}
                Some((_, value, expiry)) if value == op.value && expiry == op.expires_at => {}
                Some(_) => return Err(RymeError::Corrupt(String::from("replay"))),
            }
        }
        for (key, op) in fresh.iter() {
            engine.apply_with_expiry(key.clone(), commit_ts, op.value.clone(), op.expires_at)?;
        }
        drop(engine);
        if !fresh.is_empty() {
            let keys = committed_keys
                .map(|keys| keys.to_vec())
                .unwrap_or_else(|| fresh.keys().cloned().collect());
            committed.record(commit_ts, keys);
        } else if let Some(keys) = committed_keys {
            if !keys.is_empty() {
                committed.record(commit_ts, keys.to_vec());
            }
        }
        let visible_ts = commit_ts.saturating_add(1);
        self.inner.clock.fetch_max(visible_ts, Ordering::SeqCst);
        self.inner.applied.fetch_max(visible_ts, Ordering::SeqCst);
        Ok(())
    }

    pub fn apply_at(&self, commit_ts: u64, writes: &BTreeMap<RecordKey, WriteOp>) -> Result<()> {
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        self.apply_locked(commit_ts, writes, &mut guard)
    }

    fn apply_locked(
        &self,
        commit_ts: u64,
        writes: &BTreeMap<RecordKey, WriteOp>,
        committed: &mut CommitIndex,
    ) -> Result<()> {
        self.apply_owned_locked(commit_ts, writes.clone(), committed)
    }

    fn apply_owned_locked(
        &self,
        commit_ts: u64,
        writes: BTreeMap<RecordKey, WriteOp>,
        committed: &mut CommitIndex,
    ) -> Result<()> {
        self.apply_owned_inner(commit_ts, writes, false, committed).map(|_| ())
    }

    fn apply_owned_or_confirm_locked(
        &self,
        commit_ts: u64,
        writes: BTreeMap<RecordKey, WriteOp>,
        committed: &mut CommitIndex,
    ) -> Result<()> {
        self.apply_owned_inner(commit_ts, writes, true, committed).map(|_| ())
    }

    fn apply_owned_inner(
        &self,
        commit_ts: u64,
        writes: BTreeMap<RecordKey, WriteOp>,
        accept_identical: bool,
        committed: &mut CommitIndex,
    ) -> Result<bool> {
        if writes.is_empty() {
            return Ok(false);
        }
        let committed_keys = writes.keys().cloned().collect();
        {
            let mut engine = self
                .inner
                .engine
                .write()
                .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
            if accept_identical
                && writes.iter().all(|(key, op)| {
                    engine.matches_exact_version(key, commit_ts, op.value.as_deref(), op.expires_at)
                })
            {
                return Ok(false);
            }
            for key in writes.keys() {
                if engine.latest_commit_ts(key).is_some_and(|latest_ts| latest_ts >= commit_ts) {
                    return Err(RymeError::Conflict(String::from("stale commit_ts")));
                }
            }
            for (key, op) in writes {
                engine.apply_with_expiry(key, commit_ts, op.value, op.expires_at)?;
            }
        }
        self.record_committed_locked(commit_ts, committed_keys, committed)?;
        Ok(true)
    }

    fn record_committed_locked(
        &self,
        commit_ts: u64,
        committed_keys: Vec<RecordKey>,
        committed: &mut CommitIndex,
    ) -> Result<()> {
        committed.record(commit_ts, committed_keys);
        let visible_ts = commit_ts.saturating_add(1);
        self.inner.clock.fetch_max(visible_ts, Ordering::SeqCst);
        self.inner.applied.fetch_max(visible_ts, Ordering::SeqCst);
        publish_commit(commit_ts);
        Ok(())
    }

    pub fn commit(&self, txn: Transaction) -> Result<u64> {
        txn.ensure_healthy()?;
        if txn.writes.is_empty() {
            return Ok(txn.read_ts);
        }
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let commit_ts = self.reserve_locked(&txn, &mut guard)?;
        self.apply_owned_locked(commit_ts, txn.writes, &mut guard)?;
        Ok(commit_ts)
    }

    pub async fn commit_with<Fut>(
        &self,
        txn: Transaction,
        replicate: impl FnOnce(u64, Vec<u8>) -> Fut,
    ) -> Result<u64>
    where
        Fut: std::future::Future<Output = Result<()>>,
    {
        txn.ensure_healthy()?;
        if txn.writes.is_empty() {
            return Ok(txn.read_ts);
        }
        let payload = encode_writes(&txn.writes)?;
        let commit_ts = self.reserve_replicated(&txn)?;
        let _pending = PendingCommitGuard { manager: self.clone(), commit_ts };
        replicate(commit_ts, payload).await?;
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        if !self
            .inner
            .pending
            .lock()
            .map_err(|_| RymeError::Internal(String::from("pending commits")))?
            .contains_key(&commit_ts)
        {
            return Err(RymeError::Conflict(String::from("commit invalidated")));
        }
        self.check_locked(&txn, commit_ts, &mut guard)?;
        self.apply_owned_or_confirm_locked(commit_ts, txn.writes, &mut guard)?;
        Ok(commit_ts)
    }

    pub async fn commit_at_with<Fut>(
        &self,
        txn: Transaction,
        commit_ts: u64,
        replicate: impl FnOnce(u64, Vec<u8>) -> Fut,
    ) -> Result<u64>
    where
        Fut: std::future::Future<Output = Result<()>>,
    {
        txn.ensure_healthy()?;
        if txn.writes.is_empty() {
            return Ok(txn.read_ts);
        }
        let payload = encode_writes(&txn.writes)?;
        replicate(commit_ts, payload).await?;
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        self.check_locked(&txn, commit_ts, &mut guard)?;
        self.apply_owned_or_confirm_locked(commit_ts, txn.writes, &mut guard)?;
        Ok(commit_ts)
    }

    pub fn commit_durable(
        &self,
        txn: Transaction,
        persist: impl FnOnce(u64, &[u8]) -> Result<()>,
    ) -> Result<u64> {
        self.commit_durable_with(txn, |commit_ts, payload, _| persist(commit_ts, payload))
    }

    pub fn commit_durable_with(
        &self,
        txn: Transaction,
        persist: impl FnOnce(u64, &[u8], &BTreeMap<RecordKey, WriteOp>) -> Result<()>,
    ) -> Result<u64> {
        txn.ensure_healthy()?;
        if txn.writes.is_empty() {
            return Ok(txn.read_ts);
        }
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let commit_ts = self.reserve_locked(&txn, &mut guard)?;
        let payload = encode_writes(&txn.writes)?;
        persist(commit_ts, &payload, &txn.writes)?;
        self.apply_owned_locked(commit_ts, txn.writes, &mut guard)?;
        Ok(commit_ts)
    }

    /// Persists an owned write set without applying it to the in-memory engine.
    /// Segment-backed callers use this while retaining conflict-index updates.
    fn commit_durable_consuming_with(
        &self,
        txn: Transaction,
        persist: impl FnOnce(u64, &[u8], BTreeMap<RecordKey, WriteOp>) -> Result<()>,
    ) -> Result<u64> {
        txn.ensure_healthy()?;
        if txn.writes.is_empty() {
            return Ok(txn.read_ts);
        }
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let commit_ts = self.reserve_locked(&txn, &mut guard)?;
        let payload = encode_writes(&txn.writes)?;
        let committed_keys = txn.writes.keys().cloned().collect();
        persist(commit_ts, &payload, txn.writes)?;
        self.record_committed_locked(commit_ts, committed_keys, &mut guard)?;
        Ok(commit_ts)
    }

    pub fn scan(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        txn.ensure_healthy()?;
        txn.record_scan(tenant, database, table)?;
        let overlay_count = txn.scan_overlay_count(tenant, database, table, None);
        let scan_limit = limit.saturating_add(overlay_count);
        let engine = self
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        let rows = engine.scan(tenant, database, table, txn.read_ts, now_unix(), scan_limit);
        if overlay_count == 0 {
            Ok(rows)
        } else {
            Ok(txn.overlay_scan(rows, tenant, database, table, None, limit))
        }
    }

    pub fn scan_with(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
        fallback: impl FnOnce(usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        txn.ensure_healthy()?;
        txn.record_scan(tenant, database, table)?;
        let overlay_count = txn.scan_overlay_count(tenant, database, table, None);
        let scan_limit = limit.saturating_add(overlay_count);
        let rows = fallback(scan_limit)?;
        if overlay_count == 0 {
            Ok(rows)
        } else {
            Ok(txn.overlay_scan(rows, tenant, database, table, None, limit))
        }
    }

    pub fn scan_after(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        txn.ensure_healthy()?;
        txn.record_scan(tenant, database, table)?;
        let overlay_count = txn.scan_overlay_count(tenant, database, table, Some(start_after));
        let scan_limit = limit.saturating_add(overlay_count);
        let engine = self
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        let rows = engine.scan_after(
            tenant,
            database,
            table,
            txn.read_ts,
            now_unix(),
            start_after,
            scan_limit,
        );
        if overlay_count == 0 {
            Ok(rows)
        } else {
            Ok(txn.overlay_scan(rows, tenant, database, table, Some(start_after), limit))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn scan_after_with(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
        fallback: impl FnOnce(usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        txn.ensure_healthy()?;
        txn.record_scan(tenant, database, table)?;
        let overlay_count = txn.scan_overlay_count(tenant, database, table, Some(start_after));
        let scan_limit = limit.saturating_add(overlay_count);
        let rows = fallback(scan_limit)?;
        if overlay_count == 0 {
            Ok(rows)
        } else {
            Ok(txn.overlay_scan(rows, tenant, database, table, Some(start_after), limit))
        }
    }

    pub fn advance_to(&self, commit_ts: u64) {
        let visible_ts = commit_ts.saturating_add(1);
        self.inner.clock.fetch_max(visible_ts, Ordering::SeqCst);
        self.inner.applied.fetch_max(visible_ts, Ordering::SeqCst);
    }

    pub fn clear_engine(&self) -> Result<()> {
        let _guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        self.inner
            .engine
            .write()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?
            .clear();
        Ok(())
    }

    pub fn latest_commit(&self) -> u64 {
        self.inner.applied.load(Ordering::SeqCst)
    }

    pub fn memory_bytes(&self) -> Result<u64> {
        Ok(self
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?
            .bytes_held())
    }

    pub fn gc_horizon(&self) -> u64 {
        self.inner.active.oldest().unwrap_or_else(|| self.latest_commit())
    }

    pub fn gc_oldest(&self) -> Result<u64> {
        let mut cursor: Option<ryme_storage::GcCursor> = None;
        let horizon = loop {
            let (next, complete, batch_horizon) = {
                let _commit = self
                    .inner
                    .commit
                    .lock()
                    .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
                let active =
                    self.inner.active.transactions.lock().map_err(|_| {
                        RymeError::Internal(String::from("active transaction lock"))
                    })?;
                let batch_horizon = active
                    .by_read_ts
                    .first_key_value()
                    .map(|(read_ts, _)| *read_ts)
                    .unwrap_or_else(|| self.latest_commit());
                let mut engine = self
                    .inner
                    .engine
                    .write()
                    .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
                let (next, complete) = engine.gc_batch(
                    batch_horizon,
                    cursor.as_ref(),
                    MVCC_GC_KEYS_PER_LOCK,
                    MVCC_GC_VERSIONS_PER_LOCK,
                );
                (next, complete, batch_horizon)
            };
            if complete {
                break batch_horizon;
            }
            cursor = next;
            if cursor.is_none() {
                return Err(RymeError::Internal(String::from("mvcc gc cursor")));
            }
        };
        Ok(horizon)
    }
    pub fn table_bytes(&self, tenant: &str, database: &str, table: &str) -> Result<u64> {
        let engine = self
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        Ok(engine.table_bytes(tenant, database, table))
    }

    pub fn drop_table(&self, tenant: &str, database: &str, table: &str) -> Result<usize> {
        let _guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let mut engine = self
            .inner
            .engine
            .write()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        Ok(engine.drop_table(tenant, database, table))
    }

    pub fn purge_keys(&self, keys: &[RecordKey]) -> Result<usize> {
        let _guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let mut engine = self
            .inner
            .engine
            .write()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        Ok(engine.purge_keys(keys))
    }

    pub fn replace_routing_range(
        &self,
        start: &[u8],
        end: &[u8],
        replacement: ryme_storage::Engine,
    ) -> Result<usize> {
        let _guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let mut engine = self
            .inner
            .engine
            .write()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        Ok(engine.replace_routing_range(start, end, &replacement))
    }

    pub fn replace_routing_range_for_version(
        &self,
        version: u8,
        start: &[u8],
        end: &[u8],
        replacement: ryme_storage::Engine,
    ) -> Result<usize> {
        let _guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let mut engine = self
            .inner
            .engine
            .write()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        engine.replace_routing_range_for_version(version, start, end, &replacement)
    }

    pub fn export_table(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
    ) -> Result<ryme_storage::TableRows> {
        let _guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let engine = self
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        Ok(engine.export_table(tenant, database, table))
    }

    pub fn import_table(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        rows: ryme_storage::TableRows,
    ) -> Result<u64> {
        let _guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let max = {
            let mut engine = self
                .inner
                .engine
                .write()
                .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
            engine.import_table(tenant, database, table, rows)?
        };
        if max > 0 {
            let visible_ts = max.saturating_add(1);
            self.inner.clock.fetch_max(visible_ts, Ordering::SeqCst);
            self.inner.applied.fetch_max(visible_ts, Ordering::SeqCst);
        }
        Ok(max)
    }

    pub fn spaces(&self) -> Result<Vec<(String, String, String)>> {
        let engine = self
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        Ok(engine.spaces())
    }

    pub fn expired_keys(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let read_ts = self.inner.applied.load(Ordering::SeqCst);
        let engine = self
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        Ok(engine.expired(tenant, database, table, read_ts, now_unix(), limit))
    }

    pub fn expires_at(&self, key: &RecordKey) -> Result<Option<u64>> {
        self.expires_at_at(key, self.latest_commit())
    }

    pub fn expires_at_at(&self, key: &RecordKey, read_ts: u64) -> Result<Option<u64>> {
        let engine = self
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        Ok(engine.expires_at(key, read_ts))
    }

    pub fn encode_snapshot(&self) -> Result<Vec<u8>> {
        let _guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let engine = self
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
        engine.encode_snapshot()
    }

    pub fn restore_snapshot(&self, raw: &[u8]) -> Result<u64> {
        let snapshot = Engine::decode_snapshot(raw)?;
        let max = snapshot.max_commit_ts();
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let recovery_floor =
            max.saturating_add(1).max(self.inner.applied.load(Ordering::SeqCst).saturating_add(1));
        {
            let mut engine = self
                .inner
                .engine
                .write()
                .map_err(|_| RymeError::Internal(String::from("engine lock")))?;
            engine.replace_from(snapshot);
        }
        guard.clear_through(recovery_floor);
        self.inner
            .pending
            .lock()
            .map_err(|_| RymeError::Internal(String::from("pending commits")))?
            .clear();
        self.inner.clock.store(recovery_floor, Ordering::SeqCst);
        self.inner.applied.store(recovery_floor, Ordering::SeqCst);
        Ok(max)
    }

    pub fn restore_with_replay(
        &self,
        mut snapshot: Engine,
        replay: &[(u64, BTreeMap<RecordKey, WriteOp>)],
    ) -> Result<u64> {
        for (commit_ts, writes) in replay {
            for (key, op) in writes {
                snapshot.apply_with_expiry(
                    key.clone(),
                    *commit_ts,
                    op.value.clone(),
                    op.expires_at,
                )?;
            }
        }
        let max = snapshot.max_commit_ts();
        let mut guard = self
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let recovery_floor =
            max.saturating_add(1).max(self.inner.applied.load(Ordering::SeqCst).saturating_add(1));
        self.inner
            .engine
            .write()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?
            .replace_from(snapshot);
        guard.clear_through(recovery_floor);
        self.inner
            .pending
            .lock()
            .map_err(|_| RymeError::Internal(String::from("pending commits")))?
            .clear();
        self.inner.clock.store(recovery_floor, Ordering::SeqCst);
        self.inner.applied.store(recovery_floor, Ordering::SeqCst);
        Ok(max)
    }
}

impl Default for TxnManager {
    fn default() -> Self {
        Self::new()
    }
}

pub fn encode_writes(writes: &BTreeMap<RecordKey, WriteOp>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.push(2u8);
    let count = u32::try_from(writes.len())
        .map_err(|_| RymeError::InvalidArgument(String::from("write count")))?;
    out.extend_from_slice(&count.to_be_bytes());
    for (key, op) in writes {
        push_str(&mut out, key.tenant.as_bytes())?;
        push_str(&mut out, key.database.as_bytes())?;
        push_str(&mut out, key.table.as_bytes())?;
        push_bytes(&mut out, &key.pk)?;
        match op.value.as_ref() {
            Some(bytes) => {
                out.push(0);
                push_bytes(&mut out, bytes)?;
                out.extend_from_slice(&op.expires_at.to_be_bytes());
            }
            None => {
                out.push(1);
            }
        }
    }
    Ok(out)
}

pub fn decode_writes(input: &[u8]) -> Result<BTreeMap<RecordKey, WriteOp>> {
    if input.first() == Some(&2u8) {
        return decode_writes_v2(&input[1..]);
    }
    decode_writes_v1(input)
}

fn decode_writes_v2(input: &[u8]) -> Result<BTreeMap<RecordKey, WriteOp>> {
    let mut cursor = input;
    let count = take_u32(&mut cursor)? as usize;
    if count > 100000 {
        return Err(RymeError::Corrupt(String::from("write count")));
    }
    let mut out = BTreeMap::new();
    for _ in 0..count {
        let tenant = String::from_utf8(take_str(&mut cursor)?.to_vec())
            .map_err(|_| RymeError::Corrupt(String::from("tenant")))?;
        let database = String::from_utf8(take_str(&mut cursor)?.to_vec())
            .map_err(|_| RymeError::Corrupt(String::from("database")))?;
        let table = String::from_utf8(take_str(&mut cursor)?.to_vec())
            .map_err(|_| RymeError::Corrupt(String::from("table")))?;
        let pk = take_bytes(&mut cursor)?.to_vec();
        let tombstone = take_u8(&mut cursor)?;
        let op = if tombstone == 1 {
            WriteOp::delete()
        } else if tombstone == 0 {
            let value = take_bytes(&mut cursor)?.to_vec();
            let expires_at = take_u64(&mut cursor)?;
            WriteOp::put_ttl(value, expires_at)
        } else {
            return Err(RymeError::Corrupt(String::from("tombstone")));
        };
        out.insert(RecordKey::new(&tenant, &database, &table, &pk), op);
    }
    if !cursor.is_empty() {
        return Err(RymeError::Corrupt(String::from("trailing bytes")));
    }
    Ok(out)
}

fn decode_writes_v1(input: &[u8]) -> Result<BTreeMap<RecordKey, WriteOp>> {
    let mut cursor = input;
    let count = take_u32(&mut cursor)? as usize;
    if count > 100000 {
        return Err(RymeError::Corrupt(String::from("write count")));
    }
    let mut out = BTreeMap::new();
    for _ in 0..count {
        let tenant = String::from_utf8(take_str(&mut cursor)?.to_vec())
            .map_err(|_| RymeError::Corrupt(String::from("tenant")))?;
        let database = String::from_utf8(take_str(&mut cursor)?.to_vec())
            .map_err(|_| RymeError::Corrupt(String::from("database")))?;
        let table = String::from_utf8(take_str(&mut cursor)?.to_vec())
            .map_err(|_| RymeError::Corrupt(String::from("table")))?;
        let pk = take_bytes(&mut cursor)?.to_vec();
        let tombstone = take_u8(&mut cursor)?;
        let op = if tombstone == 1 {
            WriteOp::delete()
        } else if tombstone == 0 {
            WriteOp::put(take_bytes(&mut cursor)?.to_vec())
        } else {
            return Err(RymeError::Corrupt(String::from("tombstone")));
        };
        out.insert(RecordKey::new(&tenant, &database, &table, &pk), op);
    }
    if !cursor.is_empty() {
        return Err(RymeError::Corrupt(String::from("trailing bytes")));
    }
    Ok(out)
}

fn push_str(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if bytes.len() > u16::MAX as usize {
        return Err(RymeError::InvalidArgument(String::from("field too large")));
    }
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if bytes.len() > 8 * 1024 * 1024 {
        return Err(RymeError::InvalidArgument(String::from("value too large")));
    }
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn take_u8(cursor: &mut &[u8]) -> Result<u8> {
    if cursor.is_empty() {
        return Err(RymeError::Corrupt(String::from("truncated")));
    }
    let value = cursor[0];
    *cursor = &cursor[1..];
    Ok(value)
}

fn take_u32(cursor: &mut &[u8]) -> Result<u32> {
    if cursor.len() < 4 {
        return Err(RymeError::Corrupt(String::from("truncated")));
    }
    let value = u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
    *cursor = &cursor[4..];
    Ok(value)
}

fn take_u64(cursor: &mut &[u8]) -> Result<u64> {
    if cursor.len() < 8 {
        return Err(RymeError::Corrupt(String::from("truncated")));
    }
    let value = u64::from_be_bytes([
        cursor[0], cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6], cursor[7],
    ]);
    *cursor = &cursor[8..];
    Ok(value)
}

fn take_str<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8]> {
    if cursor.len() < 2 {
        return Err(RymeError::Corrupt(String::from("truncated")));
    }
    let len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    *cursor = &cursor[2..];
    if cursor.len() < len {
        return Err(RymeError::Corrupt(String::from("truncated")));
    }
    let value = &cursor[..len];
    *cursor = &cursor[len..];
    Ok(value)
}

fn take_bytes<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = take_u32(cursor)? as usize;
    if len > 8 * 1024 * 1024 {
        return Err(RymeError::Corrupt(String::from("value too large")));
    }
    if cursor.len() < len {
        return Err(RymeError::Corrupt(String::from("truncated")));
    }
    let value = &cursor[..len];
    *cursor = &cursor[len..];
    Ok(value)
}

#[derive(Debug, Clone)]
pub struct DurableManager {
    inner: TxnManager,
    wal: Arc<Mutex<Wal>>,
    segments: Arc<SegmentStore>,
    gate: Arc<Mutex<()>>,
    policy: SyncPolicy,
    mode: StorageMode,
    dir: std::path::PathBuf,
}

const SEGMENT_COMPACTION_THRESHOLD: usize = 64;
const DEFAULT_SEGMENT_CACHE_BYTES: u64 = 64 * 1024 * 1024;

impl DurableManager {
    pub fn open(dir: &Path, segment_bytes: u64, policy: SyncPolicy) -> Result<Self> {
        Self::open_with_cache(dir, segment_bytes, policy, DEFAULT_SEGMENT_CACHE_BYTES)
    }

    pub fn open_with_cache(
        dir: &Path,
        segment_bytes: u64,
        policy: SyncPolicy,
        cache_bytes: u64,
    ) -> Result<Self> {
        Self::open_with_mode(dir, segment_bytes, policy, cache_bytes, StorageMode::Hot)
    }

    pub fn open_with_mode(
        dir: &Path,
        segment_bytes: u64,
        policy: SyncPolicy,
        cache_bytes: u64,
        mode: StorageMode,
    ) -> Result<Self> {
        Self::open_with_mode_and_tracker(
            dir,
            segment_bytes,
            policy,
            cache_bytes,
            mode,
            &TxnTracker::new(),
        )
    }

    pub fn open_with_mode_and_tracker(
        dir: &Path,
        segment_bytes: u64,
        policy: SyncPolicy,
        cache_bytes: u64,
        mode: StorageMode,
        tracker: &TxnTracker,
    ) -> Result<Self> {
        let wal = Wal::open(dir, segment_bytes)?;
        let segments = Arc::new(SegmentStore::open_with_cache(&dir.join("segments"), cache_bytes)?);
        let manager = Self {
            inner: TxnManager::new_with_tracker(tracker),
            wal: Arc::new(Mutex::new(wal)),
            segments,
            gate: Arc::new(Mutex::new(())),
            policy,
            mode,
            dir: dir.to_path_buf(),
        };
        let floor = match mode {
            StorageMode::Hot => match manager.load_latest_snapshot()? {
                Some(floor) => floor,
                None => manager.load_all_segments()?.unwrap_or(0),
            },
            StorageMode::Standard => manager.segments.max_commit_ts()?,
        };
        manager.inner.advance_to(floor);
        let recovered = manager.recover_from(floor)?;
        if mode == StorageMode::Standard && recovered > 0 {
            manager.inner.clear_engine()?;
        }
        Ok(manager)
    }

    pub fn inner(&self) -> &TxnManager {
        &self.inner
    }

    pub fn snapshot_dir(&self) -> std::path::PathBuf {
        self.dir.join("snapshots")
    }

    pub fn segment_dir(&self) -> std::path::PathBuf {
        self.segments.dir().to_path_buf()
    }

    pub fn segment_cache_stats(&self) -> SegmentCacheStats {
        self.segments.cache_stats()
    }

    pub fn storage_mode(&self) -> StorageMode {
        self.mode
    }

    pub fn resident_bytes(&self) -> Result<u64> {
        self.inner.memory_bytes()
    }

    pub fn latest_commit(&self) -> u64 {
        self.inner.latest_commit()
    }

    fn materialized_segments(&self) -> Result<Engine> {
        Ok(self.segments.load_all()?.map(|(engine, _)| engine).unwrap_or_default())
    }

    pub fn table_bytes(&self, tenant: &str, database: &str, table: &str) -> Result<u64> {
        match self.mode {
            StorageMode::Hot => self.inner.table_bytes(tenant, database, table),
            StorageMode::Standard => {
                let engine = self.materialized_segments()?;
                Ok(engine.table_bytes(tenant, database, table))
            }
        }
    }

    pub fn export_table(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
    ) -> Result<ryme_storage::TableRows> {
        match self.mode {
            StorageMode::Hot => self.inner.export_table(tenant, database, table),
            StorageMode::Standard => {
                Ok(self.materialized_segments()?.export_table(tenant, database, table))
            }
        }
    }

    pub fn import_table(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        rows: ryme_storage::TableRows,
    ) -> Result<u64> {
        if self.mode == StorageMode::Hot {
            return self.inner.import_table(tenant, database, table, rows);
        }

        let mut deltas: BTreeMap<u64, Vec<SegmentEntry>> = BTreeMap::new();
        let mut max = 0u64;
        for (pk, versions) in rows {
            let key = RecordKey {
                tenant: tenant.to_string(),
                database: database.to_string(),
                table: table.to_string(),
                pk,
            };
            for version in versions {
                max = max.max(version.commit_ts);
                deltas.entry(version.commit_ts).or_default().push(SegmentEntry {
                    key: key.clone(),
                    commit_ts: version.commit_ts,
                    value: version.value,
                    expires_at: version.expires_at,
                });
            }
        }
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        for (commit_ts, entries) in deltas {
            self.segments.write_delta_owned_with_sync(
                commit_ts,
                entries,
                self.policy == SyncPolicy::Always,
            )?;
        }
        self.inner.advance_to(max);
        Ok(max)
    }

    pub fn purge_keys(&self, keys: &[RecordKey]) -> Result<usize> {
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        if self.mode == StorageMode::Hot {
            return self.inner.purge_keys(keys);
        }
        let mut engine = self.materialized_segments()?;
        let removed = engine.purge_keys(keys);
        let max = engine.max_commit_ts();
        let (_, newest) = self.segments.write(max, &engine)?;
        self.segments.remove_except(&newest)?;
        self.inner.advance_to(max);
        Ok(removed)
    }

    pub fn spaces(&self) -> Result<Vec<(String, String, String)>> {
        match self.mode {
            StorageMode::Hot => self.inner.spaces(),
            StorageMode::Standard => Ok(self.materialized_segments()?.spaces()),
        }
    }

    pub fn expired_keys(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        match self.mode {
            StorageMode::Hot => self.inner.expired_keys(tenant, database, table, limit),
            StorageMode::Standard => Ok(self.materialized_segments()?.expired(
                tenant,
                database,
                table,
                self.latest_commit(),
                now_unix(),
                limit,
            )),
        }
    }

    pub fn expires_at(&self, key: &RecordKey) -> Result<Option<u64>> {
        self.expires_at_at(key, self.latest_commit().saturating_sub(1))
    }

    pub fn expires_at_at(&self, key: &RecordKey, read_ts: u64) -> Result<Option<u64>> {
        match self.mode {
            StorageMode::Hot => self.inner.expires_at_at(key, read_ts),
            StorageMode::Standard => Ok(self
                .segments
                .version_at(key, read_ts)?
                .and_then(|(_, value, expires_at)| value.map(|_| expires_at))),
        }
    }

    pub fn write_snapshot(&self) -> Result<(u64, std::path::PathBuf)> {
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        let snapshot = match self.mode {
            StorageMode::Hot => Engine::decode_snapshot(&self.inner.encode_snapshot()?)?,
            StorageMode::Standard => {
                self.segments.load_all()?.map(|(engine, _)| engine).unwrap_or_default()
            }
        };
        let raw = snapshot.encode_snapshot()?;
        let max = snapshot.max_commit_ts();
        self.segments.write(max, &snapshot)?;
        let dir = self.snapshot_dir();
        std::fs::create_dir_all(&dir)?;
        let path = write_snapshot_file(&dir, max, &raw)?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| RymeError::Internal(String::from("snapshot name")))?;
        publish_snapshot_pointer(&dir, name)?;
        Ok((max, path))
    }

    pub fn load_latest_snapshot(&self) -> Result<Option<u64>> {
        let latest = self.snapshot_dir().join("latest");
        let name = match std::fs::read(&latest) {
            Ok(name) => name,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(RymeError::from(e)),
        };
        let name = String::from_utf8(name)
            .map_err(|_| RymeError::Corrupt(String::from("snapshot pointer")))?;
        let name = name.trim();
        if snapshot_commit(name).is_none() {
            return Err(RymeError::Corrupt(String::from("snapshot pointer")));
        }
        let path = self.snapshot_dir().join(name);
        if !path.is_file() {
            return Err(RymeError::Corrupt(String::from("snapshot pointer")));
        }
        let raw = read_snapshot_file(&path)?;
        Ok(Some(self.inner.restore_snapshot(&raw)?))
    }

    pub fn load_all_segments(&self) -> Result<Option<u64>> {
        let Some((engine, max)) = self.segments.load_all()? else {
            return Ok(None);
        };
        self.inner.restore_snapshot(&engine.encode_snapshot()?)?;
        Ok(Some(max))
    }

    pub fn gc_oldest(&self) -> Result<u64> {
        self.inner.gc_oldest()
    }

    pub fn prune_snapshots(&self, keep: usize) -> Result<usize> {
        let dir = self.snapshot_dir();
        let mut snaps: Vec<(u64, std::path::PathBuf)> = Vec::new();
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(RymeError::from(e)),
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let Some(rest) = name.strip_prefix("snap-").and_then(|n| n.strip_suffix(".rsnap"))
            else {
                continue;
            };
            let Ok(commit) = rest.parse::<u64>() else {
                continue;
            };
            snaps.push((commit, path));
        }
        snaps.sort_by_key(|(commit, _)| *commit);
        let excess = snaps.len().saturating_sub(keep.max(1));
        let mut removed = 0;
        for (_, path) in snaps.iter().take(excess) {
            if std::fs::remove_file(path).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub fn oldest_snapshot(&self) -> Result<Option<u64>> {
        let dir = self.snapshot_dir();
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(RymeError::from(e)),
        };
        let mut oldest: Option<u64> = None;
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(rest) = name.strip_prefix("snap-").and_then(|n| n.strip_suffix(".rsnap"))
            else {
                continue;
            };
            let Ok(commit) = rest.parse::<u64>() else {
                continue;
            };
            oldest = Some(oldest.map(|v| v.min(commit)).unwrap_or(commit));
        }
        Ok(oldest)
    }

    pub fn newest_snapshot(&self) -> Result<Option<u64>> {
        let dir = self.snapshot_dir();
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(RymeError::from(e)),
        };
        let mut newest: Option<u64> = None;
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(rest) = name.strip_prefix("snap-").and_then(|n| n.strip_suffix(".rsnap"))
            else {
                continue;
            };
            let Ok(commit) = rest.parse::<u64>() else {
                continue;
            };
            newest = Some(newest.map(|v| v.max(commit)).unwrap_or(commit));
        }
        Ok(newest)
    }

    pub fn retention_sweep(&self, snapshot_keep: usize) -> Result<(usize, usize)> {
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        // WAL retention is justified by the oldest durable snapshot and does
        // not depend on segment pruning or compaction succeeding. Perform it
        // first so an oversized or damaged segment cannot stall WAL cleanup.
        let wal_removed = match self.oldest_snapshot()? {
            Some(floor) => self
                .wal
                .lock()
                .map_err(|_| RymeError::Internal(String::from("wal lock")))?
                .truncate_below(floor)?,
            None => 0,
        };
        let pruned = self.prune_snapshots(snapshot_keep)?;
        let _segments_pruned = self.segments.prune(snapshot_keep)?;
        if self.segments.segment_paths()?.len() >= SEGMENT_COMPACTION_THRESHOLD {
            if self.mode == StorageMode::Standard {
                self.segments.compact_delta_run()?;
            } else {
                self.compact_segments_locked()?;
            }
        }
        Ok((pruned, wal_removed))
    }

    /// Compacts one bounded delta run in Standard mode, or snapshots resident
    /// state in Hot mode.
    pub fn compact_segments(&self) -> Result<Option<ryme_storage::SegmentMeta>> {
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        self.compact_segments_locked()
    }

    fn compact_segments_locked(&self) -> Result<Option<ryme_storage::SegmentMeta>> {
        if self.mode == StorageMode::Standard {
            return self.segments.compact_delta_run();
        }
        let _guard = self
            .inner
            .inner
            .commit
            .lock()
            .map_err(|_| RymeError::Internal(String::from("commit lock")))?;
        let engine = self
            .inner
            .inner
            .engine
            .read()
            .map_err(|_| RymeError::Internal(String::from("engine lock")))?
            .clone();
        if engine.is_empty() {
            return Ok(None);
        }
        let max = engine.max_commit_ts();
        let (meta, newest) = self.segments.write(max, &engine)?;
        self.segments.remove_except(&newest)?;
        Ok(Some(meta))
    }

    pub fn restore_to(&self, target: u64) -> Result<u64> {
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        let path = self
            .newest_snapshot_at_or_below(target)?
            .ok_or_else(|| RymeError::NotFound(String::from("snapshot")))?;
        let raw = read_snapshot_file(&path)?;
        let snapshot = Engine::decode_snapshot(&raw)?;
        let base = snapshot.max_commit_ts();
        let records = Wal::read_all(&self.dir)?;
        let mut replay = Vec::new();
        for record in records {
            if record.commit_ts <= base || record.commit_ts > target {
                continue;
            }
            let writes = decode_writes(&record.payload)?;
            replay.push((record.commit_ts, writes));
        }
        let replayed = replay.len() as u64;
        self.inner.restore_with_replay(snapshot, &replay)?;
        let restored_raw = self.inner.encode_snapshot()?;
        let restored = Engine::decode_snapshot(&restored_raw)?;
        let restored_max = restored.max_commit_ts();
        let (_, newest) = self.segments.write(restored_max, &restored)?;
        self.segments.remove_except(&newest)?;
        self.publish_restored_snapshot(restored_max, target, &restored_raw)?;
        {
            let mut wal =
                self.wal.lock().map_err(|_| RymeError::Internal(String::from("wal lock")))?;
            wal.truncate_above(target)?;
            if self.policy == SyncPolicy::Always {
                wal.sync()?;
            }
        }
        if self.mode == StorageMode::Standard {
            self.inner.clear_engine()?;
        }
        Ok(replayed)
    }

    fn publish_restored_snapshot(&self, commit_ts: u64, target: u64, raw: &[u8]) -> Result<()> {
        let dir = self.snapshot_dir();
        std::fs::create_dir_all(&dir)?;
        let path = write_snapshot_file(&dir, commit_ts, raw)?;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let candidate = entry.path();
            let name = candidate.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let Some(rest) = name.strip_prefix("snap-").and_then(|n| n.strip_suffix(".rsnap"))
            else {
                continue;
            };
            let Ok(snapshot_ts) = rest.parse::<u64>() else {
                continue;
            };
            if snapshot_ts > target && candidate != path {
                std::fs::remove_file(candidate)?;
            }
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| RymeError::Internal(String::from("snapshot name")))?;
        publish_snapshot_pointer(&dir, name)?;
        Ok(())
    }

    fn newest_snapshot_at_or_below(&self, target: u64) -> Result<Option<std::path::PathBuf>> {
        let dir = self.snapshot_dir();
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(RymeError::from(e)),
        };
        let mut best: Option<(u64, std::path::PathBuf)> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let Some(commit) = snapshot_commit(name) else { continue };
            if commit <= target && best.as_ref().map(|(c, _)| commit > *c).unwrap_or(true) {
                best = Some((commit, path));
            }
        }
        Ok(best.map(|(_, path)| path))
    }

    pub fn recover(&self) -> Result<u64> {
        self.recover_from(0)
    }

    pub fn recover_from(&self, floor: u64) -> Result<u64> {
        let records = Wal::read_all(&self.dir)?;
        let mut applied = 0u64;
        for record in records {
            if record.commit_ts <= floor {
                continue;
            }
            let writes = decode_writes(&record.payload)?;
            if self.mode == StorageMode::Standard {
                self.segments.write_delta_owned_with_sync(
                    record.commit_ts,
                    segment_entries(record.commit_ts, &writes),
                    self.policy == SyncPolicy::Always,
                )?;
            }
            self.inner.apply_at(record.commit_ts, &writes)?;
            applied += 1;
        }
        Ok(applied)
    }

    pub fn replay_at(&self, commit_ts: u64, writes: &BTreeMap<RecordKey, WriteOp>) -> Result<()> {
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        if self.mode == StorageMode::Standard {
            self.segments.write_delta_owned_with_sync(
                commit_ts,
                segment_entries(commit_ts, writes),
                self.policy == SyncPolicy::Always,
            )?;
        }
        self.inner.replay_at(commit_ts, writes)?;
        if self.mode == StorageMode::Standard {
            self.inner.clear_engine()?;
        }
        Ok(())
    }

    pub fn begin(&self) -> Transaction {
        self.inner.begin()
    }

    pub fn get(&self, txn: &mut Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        let read_ts = txn.read_ts;
        match self.mode {
            StorageMode::Hot => self.inner.get(txn, key),
            StorageMode::Standard => {
                self.inner.get_with(txn, key, || self.segments.get(key, read_ts, now_unix()))
            }
        }
    }

    pub fn get_many(
        &self,
        txn: &mut Transaction,
        keys: &[RecordKey],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        match self.mode {
            StorageMode::Hot => self.inner.get_many(txn, keys),
            StorageMode::Standard => {
                txn.ensure_healthy()?;
                if keys.is_empty() {
                    return Ok(Vec::new());
                }
                let now = now_unix();
                if txn.writes.is_empty() {
                    let values = self.segments.get_many(keys, txn.read_ts, now)?;
                    if !txn.read_only {
                        for (key, value) in keys.iter().zip(&values) {
                            txn.record_read_ref(key, value.is_some())?;
                        }
                    }
                    return Ok(values);
                }
                let mut values = vec![None; keys.len()];
                let mut segment_indices = Vec::with_capacity(keys.len());
                let mut segment_keys = Vec::with_capacity(keys.len());
                for (index, key) in keys.iter().enumerate() {
                    if let Some(staged) = txn.writes.get(key) {
                        values[index] = if staged.expires_at != 0 && staged.expires_at <= now {
                            None
                        } else {
                            staged.value.clone()
                        };
                    } else {
                        segment_indices.push(index);
                        segment_keys.push(key);
                    }
                }
                if !segment_keys.is_empty() {
                    let segment_values =
                        self.segments.get_many_borrowed(&segment_keys, txn.read_ts, now)?;
                    for ((index, key), value) in
                        segment_indices.into_iter().zip(segment_keys).zip(segment_values)
                    {
                        txn.record_read_ref(key, value.is_some())?;
                        values[index] = value;
                    }
                }
                Ok(values)
            }
        }
    }

    pub fn try_put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) -> Result<()> {
        self.inner.try_put(txn, key, value)
    }

    pub fn put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) {
        self.inner.put(txn, key, value);
    }

    pub fn try_put_with_ttl(
        &self,
        txn: &mut Transaction,
        key: RecordKey,
        value: Vec<u8>,
        expires_at: u64,
    ) -> Result<()> {
        self.inner.try_put_with_ttl(txn, key, value, expires_at)
    }

    pub fn put_with_ttl(
        &self,
        txn: &mut Transaction,
        key: RecordKey,
        value: Vec<u8>,
        expires_at: u64,
    ) {
        self.inner.put_with_ttl(txn, key, value, expires_at);
    }

    pub fn try_delete(&self, txn: &mut Transaction, key: RecordKey) -> Result<()> {
        self.inner.try_delete(txn, key)
    }

    pub fn delete(&self, txn: &mut Transaction, key: RecordKey) {
        self.inner.delete(txn, key);
    }

    pub fn scan(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let read_ts = txn.read_ts;
        match self.mode {
            StorageMode::Hot => self.inner.scan(txn, tenant, database, table, limit),
            StorageMode::Standard => {
                self.inner.scan_with(txn, tenant, database, table, limit, |scan_limit| {
                    self.segments.scan(tenant, database, table, read_ts, now_unix(), scan_limit)
                })
            }
        }
    }

    pub fn scan_after(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let read_ts = txn.read_ts;
        match self.mode {
            StorageMode::Hot => {
                self.inner.scan_after(txn, tenant, database, table, start_after, limit)
            }
            StorageMode::Standard => self.inner.scan_after_with(
                txn,
                tenant,
                database,
                table,
                start_after,
                limit,
                |scan_limit| {
                    self.segments.scan_after(
                        tenant,
                        database,
                        table,
                        read_ts,
                        now_unix(),
                        start_after,
                        scan_limit,
                    )
                },
            ),
        }
    }

    pub fn commit(&self, txn: Transaction) -> Result<u64> {
        txn.ensure_healthy()?;
        if txn.writes.is_empty() {
            return Ok(txn.read_ts);
        }
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        let policy = self.policy;
        match self.mode {
            StorageMode::Hot => {
                self.inner.commit_durable_with(txn, |commit_ts, payload, writes| {
                    let mut wal = self
                        .wal
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("wal lock")))?;
                    wal.append_lsn(commit_ts, payload)?;
                    if policy == SyncPolicy::Always {
                        wal.sync()?;
                    }
                    self.segments.write_delta_owned_with_sync(
                        commit_ts,
                        segment_entries(commit_ts, writes),
                        policy == SyncPolicy::Always,
                    )?;
                    Ok(())
                })
            }
            StorageMode::Standard => {
                self.inner.commit_durable_consuming_with(txn, |commit_ts, payload, writes| {
                    let mut wal = self
                        .wal
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("wal lock")))?;
                    wal.append_lsn(commit_ts, payload)?;
                    if policy == SyncPolicy::Always {
                        wal.sync()?;
                    }
                    self.segments.write_delta_owned_with_sync(
                        commit_ts,
                        segment_entries_owned(commit_ts, writes),
                        policy == SyncPolicy::Always,
                    )?;
                    Ok(())
                })
            }
        }
    }

    pub fn commit_at(
        &self,
        txn: &Transaction,
        commit_ts: u64,
        keep: impl Fn(&RecordKey) -> bool,
    ) -> Result<()> {
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        let writes: BTreeMap<RecordKey, WriteOp> = txn
            .writes
            .iter()
            .filter(|(key, _)| keep(key))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if writes.is_empty() {
            return Ok(());
        }
        let payload = encode_writes(&writes)?;
        {
            let mut wal =
                self.wal.lock().map_err(|_| RymeError::Internal(String::from("wal lock")))?;
            wal.append_lsn(commit_ts, &payload)?;
            if self.policy == SyncPolicy::Always {
                wal.sync()?;
            }
        }
        self.segments.write_delta_owned_with_sync(
            commit_ts,
            segment_entries(commit_ts, &writes),
            self.policy == SyncPolicy::Always,
        )?;
        let result = self.inner.commit_filtered_at(txn, commit_ts, keep);
        if result.is_ok() && self.mode == StorageMode::Standard {
            self.inner.clear_engine()?;
        }
        result
    }

    /// Commits an owned single-shard transaction, avoiding filtered write-map
    /// copies on the durable path.
    pub fn commit_at_owned(&self, txn: Transaction, commit_ts: u64) -> Result<()> {
        let _gate =
            self.gate.lock().map_err(|_| RymeError::Internal(String::from("durable gate")))?;
        let policy = self.policy;
        let result = self.inner.commit_filtered_durable_at(
            txn,
            commit_ts,
            |_| true,
            |commit_ts, payload, writes| {
                let mut wal =
                    self.wal.lock().map_err(|_| RymeError::Internal(String::from("wal lock")))?;
                wal.append_lsn(commit_ts, payload)?;
                if policy == SyncPolicy::Always {
                    wal.sync()?;
                }
                self.segments.write_delta_owned_with_sync(
                    commit_ts,
                    segment_entries(commit_ts, writes),
                    policy == SyncPolicy::Always,
                )?;
                Ok(())
            },
        );
        if result.is_ok() && self.mode == StorageMode::Standard {
            self.inner.clear_engine()?;
        }
        result
    }
}

fn segment_entries(commit_ts: u64, writes: &BTreeMap<RecordKey, WriteOp>) -> Vec<SegmentEntry> {
    writes
        .iter()
        .map(|(key, op)| SegmentEntry {
            key: key.clone(),
            commit_ts,
            value: op.value.clone(),
            expires_at: op.expires_at,
        })
        .collect()
}

fn segment_entries_owned(
    commit_ts: u64,
    writes: BTreeMap<RecordKey, WriteOp>,
) -> Vec<SegmentEntry> {
    writes
        .into_iter()
        .map(|(key, op)| SegmentEntry {
            key,
            commit_ts,
            value: op.value,
            expires_at: op.expires_at,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filtered_commit_applies_only_owned_writes() {
        let manager = TxnManager::new();
        let mut txn = manager.begin();
        let local = RecordKey::new("t", "d", "local", b"1");
        let remote = RecordKey::new("t", "d", "remote", b"2");
        manager.put(&mut txn, local.clone(), b"local".to_vec());
        manager.put(&mut txn, remote.clone(), b"remote".to_vec());

        manager.commit_filtered_at(&txn, txn.read_ts + 1, |key| key.table == "local").unwrap();

        let mut read = manager.begin();
        assert_eq!(manager.get(&mut read, &local).unwrap(), Some(b"local".to_vec()));
        assert_eq!(manager.get(&mut read, &remote).unwrap(), None);
    }

    #[test]
    fn active_registry_tracks_oldest_timestamp_across_insert_remove_and_restamp() {
        let registry = ActiveRegistry::default();
        registry.insert(1, 5);
        registry.insert(2, 3);
        registry.insert(3, 5);
        assert_eq!(registry.oldest(), Some(3));
        assert_eq!(registry.len(), 3);

        registry.insert(2, 8);
        assert_eq!(registry.oldest(), Some(5));
        registry.remove(1);
        assert_eq!(registry.oldest(), Some(5));
        registry.remove(3);
        assert_eq!(registry.oldest(), Some(8));
        registry.remove(2);
        assert_eq!(registry.oldest(), None);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn shared_tracker_restamps_and_releases_manager_pins() {
        let tracker = TxnTracker::new();
        let creator = TxnManager::new_with_tracker(&tracker);
        let other = TxnManager::new_with_tracker(&tracker);
        let separate = TxnManager::new();
        let mut txn = creator.begin();

        assert!(Arc::ptr_eq(&creator.inner.active, &other.inner.active));
        assert!(!Arc::ptr_eq(&creator.inner.active, &separate.inner.active));
        separate.track_transaction(&mut txn);
        assert_eq!(creator.inner.active.oldest(), Some(txn.read_ts));
        assert_eq!(other.inner.active.oldest(), Some(txn.read_ts));
        assert_eq!(separate.inner.active.oldest(), Some(txn.read_ts));

        txn.restamp(txn.read_ts + 10);
        assert_eq!(creator.inner.active.oldest(), Some(txn.read_ts));
        assert_eq!(other.inner.active.oldest(), Some(txn.read_ts));
        assert_eq!(separate.inner.active.oldest(), Some(txn.read_ts));

        drop(txn);
        assert_eq!(creator.inner.active.len(), 0);
        assert_eq!(other.inner.active.len(), 0);
        assert_eq!(separate.inner.active.len(), 0);
    }

    #[test]
    fn commit_index_retains_newer_key_and_table_versions_when_pruning() {
        let mut index = CommitIndex::default();
        let key = RecordKey::new("t", "d", "messages", b"1");
        index.record(3, vec![key.clone()]);
        index.record(2, vec![key.clone()]);

        index.prune_through(2);
        assert_eq!(index.by_key.get(&key), Some(&vec![3]));
        assert_eq!(
            index.by_table.get(&(String::from("t"), String::from("d"), String::from("messages"),)),
            Some(&vec![3])
        );

        index.prune_through(3);
        assert!(!index.by_key.contains_key(&key));
        assert!(index.by_table.is_empty());
    }

    #[test]
    fn commit_histories_keep_sorted_unique_timestamps_and_exact_conflict_bounds() {
        let mut history = Vec::new();
        record_commit_timestamp(&mut history, 5);
        record_commit_timestamp(&mut history, 2);
        record_commit_timestamp(&mut history, 5);
        record_commit_timestamp(&mut history, 3);

        assert_eq!(history, vec![2, 3, 5]);
        assert!(has_commit_between(&history, 1, 3));
        assert!(!has_commit_between(&history, 3, 5));
        assert!(has_commit_between(&history, 3, 6));
        assert!(!has_commit_between(&history, 5, 6));
    }

    #[test]
    fn single_shard_read_write() {
        let manager = TxnManager::new();
        let mut first = manager.begin();
        let key = RecordKey::new("t", "d", "s", b"k");
        manager.put(&mut first, key.clone(), b"v".to_vec());
        let commit = manager.commit(first).unwrap();
        assert!(commit > 0);
        let mut second = manager.begin();
        let value = manager.get(&mut second, &key).unwrap();
        assert_eq!(value, Some(b"v".to_vec()));
    }

    #[test]
    fn batched_reads_preserve_snapshot_and_staged_write_semantics() {
        let manager = TxnManager::new();
        let first = RecordKey::new("t", "d", "s", b"first");
        let second = RecordKey::new("t", "d", "s", b"second");
        let staged = RecordKey::new("t", "d", "s", b"staged");
        let missing = RecordKey::new("t", "d", "s", b"missing");
        let mut seed = manager.begin();
        manager.put(&mut seed, first.clone(), b"v1".to_vec());
        manager.put(&mut seed, second.clone(), b"v2".to_vec());
        manager.commit(seed).unwrap();

        let mut reader = manager.begin();
        manager.put(&mut reader, staged.clone(), b"pending".to_vec());
        let mut writer = manager.begin();
        manager.put(&mut writer, first.clone(), b"new".to_vec());
        manager.commit(writer).unwrap();

        assert_eq!(
            manager
                .get_many(
                    &mut reader,
                    &[first.clone(), staged.clone(), missing.clone(), first.clone()],
                )
                .unwrap(),
            vec![Some(b"v1".to_vec()), Some(b"pending".to_vec()), None, Some(b"v1".to_vec()),]
        );
        assert_eq!(reader.read_keys(), &BTreeSet::from([first, missing]));
    }

    #[test]
    fn read_only_transactions_skip_observations_and_reject_writes() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "items", b"row");
        let stored_key = RecordKey::new("t", "d", "seed", b"row");
        let mut seed = manager.begin();
        manager.put(&mut seed, stored_key.clone(), b"value".to_vec());
        manager.commit(seed).unwrap();

        let mut txn = manager.begin();
        txn.set_read_only().unwrap();

        assert_eq!(
            manager.get_many(&mut txn, &[stored_key, key.clone()]).unwrap(),
            vec![Some(b"value".to_vec()), None]
        );
        assert_eq!(manager.get(&mut txn, &key).unwrap(), None);
        assert!(manager.scan(&mut txn, "t", "d", "items", 10).unwrap().is_empty());
        assert!(txn.read_keys().is_empty());
        assert!(txn.scanned_tables().is_empty());
        let restored = manager.from_state(txn.state()).unwrap();
        assert!(restored.state().read_only);
        assert!(manager.try_put(&mut txn, key, b"value".to_vec()).is_err());

        let mut ignored_write = manager.begin();
        ignored_write.set_read_only().unwrap();
        manager.put(
            &mut ignored_write,
            RecordKey::new("t", "d", "items", b"ignored"),
            b"value".to_vec(),
        );
        assert!(manager.commit(ignored_write).is_err());
    }

    #[test]
    fn rejected_read_tracking_rolls_back_the_inserted_key() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "items", b"over-budget");
        let mut txn = manager.begin();
        txn.state_bytes = MAX_TRANSACTION_STATE_BYTES;

        assert!(matches!(txn.record_read(key.clone(), false), Err(RymeError::Overload(_))));
        assert!(!txn.read_keys().contains(&key));
        assert!(!txn.observed_missing.contains(&key));
        assert!(matches!(manager.commit(txn), Err(RymeError::Overload(_))));
    }

    #[test]
    fn read_only_ordered_batch_reads_values_without_tracking_conflicts() {
        let manager = TxnManager::new();
        let mut seed = manager.begin();
        for id in 0u64..32 {
            manager.put(
                &mut seed,
                RecordKey::new("t", "d", "items", &id.to_be_bytes()),
                id.to_be_bytes().to_vec(),
            );
        }
        manager.commit(seed).unwrap();

        let keys = (0u64..32)
            .chain(40..56)
            .map(|id| RecordKey::new("t", "d", "items", &id.to_be_bytes()))
            .collect::<Vec<_>>();
        let mut txn = manager.begin();
        txn.set_read_only().unwrap();
        let values = manager.get_many(&mut txn, &keys).unwrap();

        assert_eq!(
            values,
            (0u64..32)
                .map(|id| Some(id.to_be_bytes().to_vec()))
                .chain((40..56).map(|_| None))
                .collect::<Vec<_>>()
        );
        assert!(txn.read_keys().is_empty());

        let mut tracked = manager.begin();
        assert_eq!(manager.get_many(&mut tracked, &keys).unwrap(), values);
        assert_eq!(tracked.read_keys(), &keys.iter().cloned().collect());
    }

    #[test]
    fn staged_write_ordered_batch_reads_preserve_values_and_conflict_tracking() {
        let manager = TxnManager::new();
        let mut seed = manager.begin();
        for id in 0u64..32 {
            manager.put(
                &mut seed,
                RecordKey::new("t", "d", "items", &id.to_be_bytes()),
                id.to_be_bytes().to_vec(),
            );
        }
        manager.commit(seed).unwrap();

        let keys = (0u64..32)
            .chain(40..48)
            .map(|id| RecordKey::new("t", "d", "items", &id.to_be_bytes()))
            .collect::<Vec<_>>();
        let staged_key = keys[8].clone();
        let mut txn = manager.begin();
        manager.put(&mut txn, staged_key.clone(), b"staged".to_vec());

        let values = manager.get_many(&mut txn, &keys).unwrap();
        assert_eq!(values[8], Some(b"staged".to_vec()));
        assert_eq!(
            values,
            (0u64..32)
                .map(|id| {
                    if id == 8 {
                        Some(b"staged".to_vec())
                    } else {
                        Some(id.to_be_bytes().to_vec())
                    }
                })
                .chain((40..48).map(|_| None))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            txn.read_keys(),
            &keys.iter().filter(|key| **key != staged_key).cloned().collect()
        );
    }

    #[test]
    fn commit_advances_clock_past_an_older_manager_snapshot() {
        let manager = TxnManager::new();
        let mut txn = manager.begin();
        txn.restamp(100);
        manager.put(&mut txn, RecordKey::new("t", "d", "s", b"k"), b"v".to_vec());

        assert_eq!(manager.commit(txn).unwrap(), 101);
        assert_eq!(manager.latest_commit(), 102);
    }

    #[test]
    fn apply_rejects_a_stale_batch_without_partial_mutation() {
        let manager = TxnManager::new();
        let stale_key = RecordKey::new("t", "d", "s", b"z");
        let fresh_key = RecordKey::new("t", "d", "s", b"a");
        let mut seed = BTreeMap::new();
        seed.insert(stale_key.clone(), WriteOp::put(b"seed".to_vec()));
        manager.apply_at(10, &seed).unwrap();

        let mut batch = BTreeMap::new();
        batch.insert(fresh_key.clone(), WriteOp::put(b"must-not-appear".to_vec()));
        batch.insert(stale_key.clone(), WriteOp::put(b"stale".to_vec()));
        assert!(matches!(manager.apply_at(5, &batch), Err(RymeError::Conflict(_))));

        let mut probe = manager.begin();
        assert_eq!(manager.get(&mut probe, &fresh_key).unwrap(), None);
        assert_eq!(manager.get(&mut probe, &stale_key).unwrap(), Some(b"seed".to_vec()));
    }

    #[test]
    fn commit_clock_accepts_last_timestamp_then_rejects_overflow() {
        let manager = TxnManager::new();
        manager.advance_to(u64::MAX - 2);

        let key = RecordKey::new("t", "d", "s", b"last");
        let mut last_commit = manager.begin();
        manager.put(&mut last_commit, key.clone(), b"at-limit".to_vec());
        assert_eq!(manager.commit(last_commit).unwrap(), u64::MAX);
        assert_eq!(manager.latest_commit(), u64::MAX);

        let mut overflow = manager.begin();
        let overflow_key = RecordKey::new("t", "d", "s", b"overflow");
        manager.put(&mut overflow, overflow_key.clone(), b"must-not-commit".to_vec());
        assert!(matches!(manager.commit(overflow), Err(RymeError::Corrupt(_))));
        assert_eq!(manager.get(&mut manager.begin(), &overflow_key).unwrap(), None);
        assert_eq!(manager.get(&mut manager.begin(), &key).unwrap(), Some(b"at-limit".to_vec()));
    }

    #[test]
    fn active_transactions_are_scoped_to_their_manager() {
        let first = TxnManager::new();
        let _reader = first.begin();

        let second = TxnManager::new();
        let mut writer = second.begin();
        second.put(&mut writer, RecordKey::new("t", "d", "s", b"k"), b"v".to_vec());
        second.commit(writer).unwrap();

        assert_eq!(second.gc_horizon(), second.latest_commit());
    }

    #[test]
    fn transaction_checkpoint_restores_staged_state() {
        let manager = TxnManager::new();
        let first = RecordKey::new("t", "d", "s", b"first");
        let second = RecordKey::new("t", "d", "s", b"second");
        let missing = RecordKey::new("t", "d", "s", b"missing");
        let after_checkpoint = RecordKey::new("t", "d", "s", b"after-checkpoint");
        let mut txn = manager.begin();
        manager.put(&mut txn, first.clone(), b"one".to_vec());
        assert_eq!(manager.get(&mut txn, &missing).unwrap(), None);
        let checkpoint = txn.checkpoint();
        manager.put(&mut txn, second.clone(), b"two".to_vec());
        assert_eq!(manager.get(&mut txn, &after_checkpoint).unwrap(), None);
        txn.restore_checkpoint(&checkpoint);

        assert_eq!(txn.observed_existed(&missing), Some(false));
        assert_eq!(txn.observed_existed(&after_checkpoint), None);
        assert_eq!(manager.get(&mut txn, &first).unwrap(), Some(b"one".to_vec()));
        assert_eq!(manager.get(&mut txn, &second).unwrap(), None);
        manager.commit(txn).unwrap();

        let mut probe = manager.begin();
        assert_eq!(manager.get(&mut probe, &first).unwrap(), Some(b"one".to_vec()));
        assert_eq!(manager.get(&mut probe, &second).unwrap(), None);
    }

    #[test]
    fn scans_include_staged_writes_without_skipping_pages() {
        let manager = TxnManager::new();
        for (pk, value) in [
            (b"1".as_slice(), b"one".as_slice()),
            (b"2".as_slice(), b"two".as_slice()),
            (b"3".as_slice(), b"three".as_slice()),
            (b"4".as_slice(), b"four".as_slice()),
        ] {
            let mut seed = manager.begin();
            manager.put(&mut seed, RecordKey::new("t", "d", "items", pk), value.to_vec());
            manager.commit(seed).unwrap();
        }

        let mut txn = manager.begin();
        manager.put(&mut txn, RecordKey::new("t", "d", "items", b"0"), b"zero".to_vec());
        manager.delete(&mut txn, RecordKey::new("t", "d", "items", b"1"));
        manager.put(&mut txn, RecordKey::new("t", "d", "items", b"9"), b"nine".to_vec());

        let first = manager.scan(&mut txn, "t", "d", "items", 2).unwrap();
        assert_eq!(
            first,
            vec![(b"0".to_vec(), b"zero".to_vec()), (b"2".to_vec(), b"two".to_vec())]
        );
        let second = manager.scan_after(&mut txn, "t", "d", "items", b"2", 2).unwrap();
        assert_eq!(
            second,
            vec![(b"3".to_vec(), b"three".to_vec()), (b"4".to_vec(), b"four".to_vec())]
        );
        let last = manager.scan_after(&mut txn, "t", "d", "items", b"4", 2).unwrap();
        assert_eq!(last, vec![(b"9".to_vec(), b"nine".to_vec())]);
    }

    #[test]
    fn staged_scan_overlay_is_bounded_to_table_and_cursor() {
        let manager = TxnManager::new();
        let mut txn = manager.begin();
        manager.put(&mut txn, RecordKey::new("t", "d", "items", b"a"), b"a".to_vec());
        manager.put(&mut txn, RecordKey::new("t", "d", "items", b"b"), b"b".to_vec());
        manager.put(&mut txn, RecordKey::new("t", "d", "other", b"z"), b"z".to_vec());
        manager.put(&mut txn, RecordKey::new("other", "d", "items", b"z"), b"z".to_vec());

        assert_eq!(txn.scan_overlay_count("t", "d", "items", None), 2);
        assert_eq!(txn.scan_overlay_count("t", "d", "items", Some(b"a")), 1);
        assert_eq!(
            txn.overlay_scan(Vec::new(), "t", "d", "items", Some(b"a"), 10),
            vec![(b"b".to_vec(), b"b".to_vec())]
        );
    }

    #[test]
    fn transaction_state_roundtrip_preserves_validation_metadata() {
        let manager = TxnManager::new();
        let watched = RecordKey::new("t", "d", "state", b"watched");
        let missing = RecordKey::new("t", "d", "state", b"missing");
        let written = RecordKey::new("t", "d", "state", b"written");
        let mut seed = manager.begin();
        manager.put(&mut seed, watched.clone(), b"v1".to_vec());
        manager.commit(seed).unwrap();

        let mut txn = manager.begin();
        assert_eq!(manager.get(&mut txn, &watched).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(manager.get(&mut txn, &missing).unwrap(), None);
        assert_eq!(txn.observed_existed(&watched), Some(true));
        assert_eq!(txn.observed_existed(&missing), Some(false));
        assert_eq!(manager.scan(&mut txn, "t", "d", "state", 10).unwrap().len(), 1);
        manager.put(&mut txn, written, b"value".to_vec());
        txn.set_isolation(Isolation::Snapshot);
        let state = txn.state();
        let restored = manager.from_state(state.clone()).unwrap();

        assert_eq!(restored.state(), state);
        assert_eq!(restored.observed_existed(&watched), Some(true));
        assert_eq!(restored.observed_existed(&missing), Some(false));
    }

    #[test]
    fn write_write_conflict() {
        let manager = TxnManager::new();
        let mut seed = manager.begin();
        let key = RecordKey::new("t", "d", "s", b"k");
        manager.put(&mut seed, key.clone(), b"v0".to_vec());
        manager.commit(seed).unwrap();
        let mut left = manager.begin();
        let mut right = manager.begin();
        manager.put(&mut left, key.clone(), b"left".to_vec());
        manager.put(&mut right, key.clone(), b"right".to_vec());
        manager.commit(left).unwrap();
        let conflicted = manager.commit(right);
        assert!(matches!(conflicted, Err(RymeError::Conflict(_))));
    }

    #[test]
    fn serializable_aborts_stale_read() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "iso", b"rw");
        let other = RecordKey::new("t", "d", "iso", b"other");
        let mut seed = manager.begin();
        manager.put(&mut seed, key.clone(), b"v1".to_vec());
        manager.commit(seed).unwrap();
        let mut reader = manager.begin();
        assert_eq!(manager.get(&mut reader, &key).unwrap(), Some(b"v1".to_vec()));
        let mut writer = manager.begin();
        manager.put(&mut writer, key.clone(), b"v2".to_vec());
        manager.commit(writer).unwrap();
        manager.put(&mut reader, other.clone(), b"w".to_vec());
        let conflicted = manager.commit(reader);
        assert!(matches!(conflicted, Err(RymeError::Conflict(_))));
    }

    #[test]
    fn snapshot_allows_stale_read() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "iso", b"srw");
        let other = RecordKey::new("t", "d", "iso", b"sother");
        let mut seed = manager.begin();
        manager.put(&mut seed, key.clone(), b"v1".to_vec());
        manager.commit(seed).unwrap();
        let mut reader = manager.begin_with(Isolation::Snapshot);
        assert_eq!(reader.isolation(), Isolation::Snapshot);
        assert_eq!(manager.get(&mut reader, &key).unwrap(), Some(b"v1".to_vec()));
        let mut writer = manager.begin();
        manager.put(&mut writer, key.clone(), b"v2".to_vec());
        manager.commit(writer).unwrap();
        manager.put(&mut reader, other.clone(), b"w".to_vec());
        assert!(manager.commit(reader).is_ok());
    }

    #[test]
    fn serializable_aborts_phantom() {
        let manager = TxnManager::new();
        let mut seed = manager.begin();
        manager.put(&mut seed, RecordKey::new("t", "d", "phant", b"a"), b"1".to_vec());
        manager.commit(seed).unwrap();
        let mut reader = manager.begin();
        assert_eq!(manager.scan(&mut reader, "t", "d", "phant", 100).unwrap().len(), 1);
        let mut writer = manager.begin();
        manager.put(&mut writer, RecordKey::new("t", "d", "phant", b"b"), b"2".to_vec());
        manager.commit(writer).unwrap();
        manager.put(&mut reader, RecordKey::new("t", "d", "iso", b"pw"), b"w".to_vec());
        let conflicted = manager.commit(reader);
        assert!(matches!(conflicted, Err(RymeError::Conflict(_))));
    }

    #[test]
    fn snapshot_keeps_write_write_conflicts() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "iso", b"ww");
        let mut left = manager.begin_with(Isolation::Snapshot);
        let mut right = manager.begin_with(Isolation::Snapshot);
        manager.put(&mut left, key.clone(), b"left".to_vec());
        manager.put(&mut right, key.clone(), b"right".to_vec());
        manager.commit(left).unwrap();
        let conflicted = manager.commit(right);
        assert!(matches!(conflicted, Err(RymeError::Conflict(_))));
    }

    #[test]
    fn write_skew_aborts_under_serializable() {
        let manager = TxnManager::new();
        let key_a = RecordKey::new("t", "d", "skew", b"a");
        let key_b = RecordKey::new("t", "d", "skew", b"b");
        let mut seed = manager.begin();
        manager.put(&mut seed, key_a.clone(), 70i64.to_le_bytes().to_vec());
        manager.put(&mut seed, key_b.clone(), 70i64.to_le_bytes().to_vec());
        manager.commit(seed).unwrap();
        let mut first = manager.begin();
        let mut second = manager.begin();
        manager.get(&mut first, &key_a).unwrap();
        manager.get(&mut first, &key_b).unwrap();
        manager.get(&mut second, &key_a).unwrap();
        manager.get(&mut second, &key_b).unwrap();
        manager.put(&mut first, key_a.clone(), 10i64.to_le_bytes().to_vec());
        manager.put(&mut second, key_b.clone(), 10i64.to_le_bytes().to_vec());
        assert!(manager.commit(first).is_ok());
        let conflicted = manager.commit(second);
        assert!(matches!(conflicted, Err(RymeError::Conflict(_))));
    }

    #[test]
    fn write_skew_commits_under_snapshot() {
        let manager = TxnManager::new();
        let key_a = RecordKey::new("t", "d", "skews", b"a");
        let key_b = RecordKey::new("t", "d", "skews", b"b");
        let mut seed = manager.begin();
        manager.put(&mut seed, key_a.clone(), 70i64.to_le_bytes().to_vec());
        manager.put(&mut seed, key_b.clone(), 70i64.to_le_bytes().to_vec());
        manager.commit(seed).unwrap();
        let mut first = manager.begin_with(Isolation::Snapshot);
        let mut second = manager.begin_with(Isolation::Snapshot);
        manager.get(&mut first, &key_a).unwrap();
        manager.get(&mut first, &key_b).unwrap();
        manager.get(&mut second, &key_a).unwrap();
        manager.get(&mut second, &key_b).unwrap();
        manager.put(&mut first, key_a.clone(), 10i64.to_le_bytes().to_vec());
        manager.put(&mut second, key_b.clone(), 10i64.to_le_bytes().to_vec());
        assert!(manager.commit(first).is_ok());
        assert!(manager.commit(second).is_ok());
    }

    #[tokio::test]
    async fn replicated_commit_fences_conflicting_local_commit() {
        let manager = TxnManager::new();
        let key_a = RecordKey::new("t", "d", "replicated", b"a");
        let key_b = RecordKey::new("t", "d", "replicated", b"b");
        let mut seed = manager.begin();
        manager.put(&mut seed, key_a.clone(), b"a".to_vec());
        manager.put(&mut seed, key_b.clone(), b"b".to_vec());
        manager.commit(seed).unwrap();

        let mut first = manager.begin();
        manager.get(&mut first, &key_b).unwrap();
        manager.put(&mut first, key_a.clone(), b"first".to_vec());
        let mut second = manager.begin();
        manager.get(&mut second, &key_a).unwrap();
        manager.put(&mut second, key_b.clone(), b"second".to_vec());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let first_task = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .commit_with(first, move |_, _| async move {
                        started_tx
                            .send(())
                            .map_err(|_| RymeError::Internal(String::from("start")))?;
                        release_rx.await.map_err(|_| RymeError::Internal(String::from("release")))
                    })
                    .await
            }
        });
        started_rx.await.unwrap();

        assert!(matches!(manager.commit(second), Err(RymeError::Conflict(_))));
        release_tx.send(()).unwrap();
        assert!(first_task.await.unwrap().is_ok());

        let mut failed = manager.begin();
        manager.put(&mut failed, key_b.clone(), b"failed".to_vec());
        assert!(manager
            .commit_with(failed, |_, _| async {
                Err(RymeError::Unavailable(String::from("replication")))
            })
            .await
            .is_err());
        let mut retry = manager.begin();
        manager.put(&mut retry, key_b, b"retry".to_vec());
        assert!(manager.commit(retry).is_ok());
    }

    #[tokio::test]
    async fn replicated_commit_at_is_idempotent_only_for_identical_writes() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "replicated", b"idempotent");
        let mut initial = manager.begin();
        let mut duplicate = manager.begin();
        let mut different = manager.begin();
        manager.put(&mut initial, key.clone(), b"same".to_vec());
        manager.put(&mut duplicate, key.clone(), b"same".to_vec());
        manager.put(&mut different, key.clone(), b"different".to_vec());

        manager.commit_at_with(initial, 100, |_, _| async { Ok(()) }).await.unwrap();
        manager.commit_at_with(duplicate, 100, |_, _| async { Ok(()) }).await.unwrap();
        assert!(matches!(
            manager.commit_at_with(different, 100, |_, _| async { Ok(()) }).await,
            Err(RymeError::Conflict(_))
        ));

        let mut read = manager.begin();
        assert_eq!(manager.get(&mut read, &key).unwrap(), Some(b"same".to_vec()));
    }

    #[tokio::test]
    async fn restore_invalidates_pending_replicated_commit() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "restore", b"k");
        let snapshot = manager.encode_snapshot().unwrap();
        let mut txn = manager.begin();
        manager.put(&mut txn, key.clone(), b"must-not-return".to_vec());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .commit_with(txn, move |_, _| async move {
                        started_tx
                            .send(())
                            .map_err(|_| RymeError::Internal(String::from("start")))?;
                        release_rx.await.map_err(|_| RymeError::Internal(String::from("release")))
                    })
                    .await
            }
        });
        started_rx.await.unwrap();
        manager.restore_snapshot(&snapshot).unwrap();
        release_tx.send(()).unwrap();
        assert!(matches!(task.await.unwrap(), Err(RymeError::Conflict(_))));

        let mut probe = manager.begin();
        assert_eq!(manager.get(&mut probe, &key).unwrap(), None);
    }

    #[test]
    fn concurrent_transfers_preserve_total() {
        let manager = TxnManager::new();
        for i in 0..4u8 {
            let mut seed = manager.begin();
            manager.put(
                &mut seed,
                RecordKey::new("t", "d", "bank", &[i]),
                100i64.to_le_bytes().to_vec(),
            );
            manager.commit(seed).unwrap();
        }
        let gate = std::sync::Arc::new(std::sync::Barrier::new(9));
        let mut handles = Vec::new();
        for worker in 0..8u8 {
            let manager = manager.clone();
            let gate = gate.clone();
            handles.push(std::thread::spawn(move || {
                gate.wait();
                for round in 0..50u64 {
                    let from = (worker + round as u8) % 4;
                    let to = (from + 1) % 4;
                    loop {
                        let mut txn = manager.begin();
                        let read = |txn: &mut Transaction, key: &RecordKey| {
                            let raw = manager.get(txn, key).unwrap().unwrap();
                            i64::from_le_bytes(raw.try_into().unwrap())
                        };
                        let key_from = RecordKey::new("t", "d", "bank", &[from]);
                        let key_to = RecordKey::new("t", "d", "bank", &[to]);
                        let balance_from = read(&mut txn, &key_from);
                        let balance_to = read(&mut txn, &key_to);
                        manager.put(&mut txn, key_from, (balance_from - 1).to_le_bytes().to_vec());
                        manager.put(&mut txn, key_to, (balance_to + 1).to_le_bytes().to_vec());
                        match manager.commit(txn) {
                            Ok(_) => break,
                            Err(RymeError::Conflict(_)) => continue,
                            Err(e) => panic!("unexpected commit error: {e:?}"),
                        }
                    }
                }
            }));
        }
        gate.wait();
        for handle in handles {
            handle.join().unwrap();
        }
        let mut probe = manager.begin();
        let mut total = 0i64;
        for i in 0..4u8 {
            let raw =
                manager.get(&mut probe, &RecordKey::new("t", "d", "bank", &[i])).unwrap().unwrap();
            total += i64::from_le_bytes(raw.try_into().unwrap());
        }
        assert_eq!(total, 400);
    }

    #[test]
    fn gc_retains_versions_visible_to_live_reader() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "s", b"gck");
        let mut seed = manager.begin();
        manager.put(&mut seed, key.clone(), b"v1".to_vec());
        manager.commit(seed).unwrap();
        let mut reader = manager.begin();
        assert!(Transaction::oldest_active().is_some_and(|ts| ts <= reader.read_ts));
        let mut writer = manager.begin();
        manager.put(&mut writer, key.clone(), b"v2".to_vec());
        manager.commit(writer).unwrap();
        let horizon = manager.gc_oldest().unwrap();
        assert!(horizon <= reader.read_ts);
        let stale = manager.get(&mut reader, &key).unwrap();
        assert_eq!(stale, Some(b"v1".to_vec()));
        drop(reader);
        manager.gc_oldest().unwrap();
        let mut fresh = manager.begin();
        let current = manager.get(&mut fresh, &key).unwrap();
        assert_eq!(current, Some(b"v2".to_vec()));
    }

    #[test]
    fn restored_snapshot_rejects_validation_after_conflict_history_pruning() {
        let manager = TxnManager::new();
        let observed_key = RecordKey::new("t", "d", "stale-snapshot", b"observed");
        let output_key = RecordKey::new("t", "d", "stale-snapshot", b"output");
        let advance_key = RecordKey::new("t", "d", "stale-snapshot", b"advance");

        let mut stale = manager.begin();
        assert_eq!(manager.get(&mut stale, &observed_key).unwrap(), None);
        manager.put(&mut stale, output_key, b"derived from old read".to_vec());
        let stale_state = stale.state();
        drop(stale);

        let mut concurrent = manager.begin();
        manager.put(&mut concurrent, observed_key, b"new value".to_vec());
        manager.commit(concurrent).unwrap();

        let mut newer = manager.begin();
        manager.put(&mut newer, advance_key, b"advance gc horizon".to_vec());
        manager.commit(newer).unwrap();

        let restored = manager.from_state(stale_state).unwrap();
        assert!(matches!(
            manager.commit(restored),
            Err(RymeError::Conflict(message)) if message == "transaction snapshot too old"
        ));
    }

    #[test]
    fn restored_snapshot_rejects_transactions_older_than_recovered_history() {
        let manager = TxnManager::new();
        let observed_key = RecordKey::new("t", "d", "restore-floor", b"observed");
        let output_key = RecordKey::new("t", "d", "restore-floor", b"output");

        let mut stale = manager.begin();
        assert_eq!(manager.get(&mut stale, &observed_key).unwrap(), None);
        manager.put(&mut stale, output_key, b"derived from old read".to_vec());
        let stale_state = stale.state();
        drop(stale);

        let mut writer = manager.begin();
        manager.put(&mut writer, observed_key, b"committed before snapshot".to_vec());
        manager.commit(writer).unwrap();

        let snapshot = manager.encode_snapshot().unwrap();
        manager.restore_snapshot(&snapshot).unwrap();

        let restored = manager.from_state(stale_state).unwrap();
        assert!(matches!(
            manager.commit(restored),
            Err(RymeError::Conflict(message)) if message == "transaction snapshot too old"
        ));
    }

    #[test]
    fn point_in_time_restore_fences_transactions_from_discarded_future() {
        let manager = TxnManager::new();
        let retained_key = RecordKey::new("t", "d", "restore-fence", b"retained");
        let discarded_key = RecordKey::new("t", "d", "restore-fence", b"discarded");
        let output_key = RecordKey::new("t", "d", "restore-fence", b"output");

        let mut first = manager.begin();
        manager.put(&mut first, retained_key, b"at restore point".to_vec());
        manager.commit(first).unwrap();
        let restore_point = manager.encode_snapshot().unwrap();

        let mut later = manager.begin();
        manager.put(&mut later, discarded_key, b"discarded by restore".to_vec());
        manager.commit(later).unwrap();

        let mut stale = manager.begin();
        manager.put(&mut stale, output_key, b"based on discarded future".to_vec());
        let stale_state = stale.state();
        drop(stale);

        manager.restore_snapshot(&restore_point).unwrap();

        let restored = manager.from_state(stale_state).unwrap();
        assert!(matches!(
            manager.commit(restored),
            Err(RymeError::Conflict(message)) if message == "transaction snapshot too old"
        ));

        let fresh = manager.begin();
        assert_eq!(fresh.read_ts, manager.latest_commit());
    }

    #[test]
    fn gc_oldest_collects_all_key_batches() {
        let manager = TxnManager::new();
        let keys: Vec<_> = (0usize..2_050)
            .map(|index| RecordKey::new("t", "d", "gc-batch", &index.to_be_bytes()))
            .collect();
        let mut initial = manager.begin();
        for key in &keys {
            manager.put(&mut initial, key.clone(), vec![1]);
        }
        manager.commit(initial).unwrap();
        let mut updates = manager.begin();
        for key in &keys {
            manager.put(&mut updates, key.clone(), vec![2]);
        }
        manager.commit(updates).unwrap();
        let mut newest = manager.begin();
        for key in &keys {
            manager.put(&mut newest, key.clone(), vec![3]);
        }
        manager.commit(newest).unwrap();

        assert_eq!(manager.memory_bytes().unwrap(), 6_150);
        manager.gc_oldest().unwrap();
        assert_eq!(manager.memory_bytes().unwrap(), 2_050);

        let mut read = manager.begin();
        for key in &keys {
            assert_eq!(manager.get(&mut read, key).unwrap(), Some(vec![3]));
        }
    }

    #[test]
    fn codec_roundtrip() {
        let mut writes = BTreeMap::new();
        writes.insert(RecordKey::new("t", "d", "users", b"1"), WriteOp::put(b"ada".to_vec()));
        writes.insert(RecordKey::new("t", "d", "users", b"2"), WriteOp::delete());
        writes.insert(
            RecordKey::new("t", "d", "users", b"3"),
            WriteOp::put_ttl(b"tmp".to_vec(), 999),
        );
        let raw = encode_writes(&writes).unwrap();
        assert_eq!(raw[0], 2u8);
        let back = decode_writes(&raw).unwrap();
        assert_eq!(back, writes);
    }

    #[test]
    fn codec_reads_v1() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1u32.to_be_bytes());
        for part in ["t", "d", "s"] {
            raw.extend_from_slice(&(part.len() as u16).to_be_bytes());
            raw.extend_from_slice(part.as_bytes());
        }
        raw.extend_from_slice(&1u32.to_be_bytes());
        raw.extend_from_slice(b"k");
        raw.push(0);
        raw.extend_from_slice(&1u32.to_be_bytes());
        raw.extend_from_slice(b"v");
        let back = decode_writes(&raw).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert(RecordKey::new("t", "d", "s", b"k"), WriteOp::put(b"v".to_vec()));
        assert_eq!(back, expected);
    }

    #[test]
    fn filtered_replay_keeps_remote_conflicts_without_local_rows() {
        let manager = TxnManager::new();
        let local = RecordKey::new("t", "d", "owned", b"local");
        let remote = RecordKey::new("t", "d", "owned", b"remote");
        let later = RecordKey::new("t", "d", "owned", b"later");
        let mut writes = BTreeMap::new();
        writes.insert(local.clone(), WriteOp::put(b"local-value".to_vec()));
        writes.insert(remote.clone(), WriteOp::put(b"remote-value".to_vec()));

        let mut stale = manager.begin();
        manager.replay_at_filtered(10, &writes, |key| key == &local).unwrap();

        let mut fresh = manager.begin();
        assert_eq!(manager.get(&mut fresh, &local).unwrap(), Some(b"local-value".to_vec()));
        assert_eq!(manager.get(&mut fresh, &remote).unwrap(), None);

        assert_eq!(manager.get(&mut stale, &remote).unwrap(), None);
        manager.put(&mut stale, later, b"later-value".to_vec());
        assert!(matches!(manager.commit(stale), Err(RymeError::Conflict(_))));
    }

    #[test]
    fn ttl_past_expiry_invisible() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "s", b"k");
        let mut txn = manager.begin();
        manager.put_with_ttl(&mut txn, key.clone(), b"v".to_vec(), 1);
        manager.commit(txn).unwrap();
        let mut probe = manager.begin();
        assert_eq!(manager.get(&mut probe, &key).unwrap(), None);
        let mut fresh = manager.begin();
        manager.put(&mut fresh, key.clone(), b"v2".to_vec());
        manager.commit(fresh).unwrap();
        let mut probe = manager.begin();
        assert_eq!(manager.get(&mut probe, &key).unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn ttl_future_visible() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "s", b"k");
        let mut txn = manager.begin();
        manager.put_with_ttl(&mut txn, key.clone(), b"v".to_vec(), u64::MAX);
        manager.commit(txn).unwrap();
        let mut probe = manager.begin();
        assert_eq!(manager.get(&mut probe, &key).unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn jepsen_transfer_conserves_total() {
        let manager = TxnManager::new();
        for index in 0u8..4 {
            let mut seed = manager.begin();
            manager.put(
                &mut seed,
                RecordKey::new("t", "d", "acct", &[index]),
                1000u64.to_be_bytes().to_vec(),
            );
            manager.commit(seed).unwrap();
        }
        let mut handles = Vec::new();
        for worker in 0..8 {
            let manager = manager.clone();
            handles.push(std::thread::spawn(move || {
                for round in 0..50u64 {
                    let from = ((worker + round) % 4) as u8;
                    let to = ((worker + round + 1) % 4) as u8;
                    let mut attempts = 0;
                    loop {
                        attempts += 1;
                        let mut txn = manager.begin();
                        let from_key = RecordKey::new("t", "d", "acct", &[from]);
                        let to_key = RecordKey::new("t", "d", "acct", &[to]);
                        let from_raw = manager.get(&mut txn, &from_key).unwrap().unwrap();
                        let to_raw = manager.get(&mut txn, &to_key).unwrap().unwrap();
                        let mut from_val = [0u8; 8];
                        from_val.copy_from_slice(&from_raw);
                        let mut to_val = [0u8; 8];
                        to_val.copy_from_slice(&to_raw);
                        let from_amt = u64::from_be_bytes(from_val);
                        let to_amt = u64::from_be_bytes(to_val);
                        manager.put(&mut txn, from_key, (from_amt - 1).to_be_bytes().to_vec());
                        manager.put(&mut txn, to_key, (to_amt + 1).to_be_bytes().to_vec());
                        match manager.commit(txn) {
                            Ok(_) => break,
                            Err(RymeError::Conflict(_)) if attempts < 50 => continue,
                            Err(e) => panic!("unexpected commit error: {e}"),
                        }
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let mut total = 0u64;
        for index in 0u8..4 {
            let mut probe = manager.begin();
            let raw = manager
                .get(&mut probe, &RecordKey::new("t", "d", "acct", &[index]))
                .unwrap()
                .unwrap();
            let mut value = [0u8; 8];
            value.copy_from_slice(&raw);
            total += u64::from_be_bytes(value);
        }
        assert_eq!(total, 4000);
    }

    #[test]
    fn durable_recover() {
        let dir = std::env::temp_dir().join(format!("ryme-durable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let mut txn = manager.begin();
        let key = RecordKey::new("t", "d", "s", b"k");
        manager.put(&mut txn, key.clone(), b"v".to_vec());
        manager.commit(txn).unwrap();
        drop(manager);
        let reopened = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let mut probe = reopened.begin();
        assert_eq!(reopened.get(&mut probe, &key).unwrap(), Some(b"v".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejected_owned_durable_commit_does_not_persist_wal_record() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-durable-owned-conflict-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let key = RecordKey::new("t", "d", "items", b"1");
        let mut stale = manager.begin();
        manager.put(&mut stale, key.clone(), b"stale".to_vec());
        let mut winner = manager.begin();
        manager.put(&mut winner, key.clone(), b"winner".to_vec());
        manager.commit(winner).unwrap();

        let wal_records_before = Wal::read_all(&dir).unwrap().len();
        let mut persist_called = false;
        let commit_ts = manager.latest_commit() + 1;
        assert!(matches!(
            manager.inner.commit_filtered_durable_at(
                stale,
                commit_ts,
                |_| true,
                |_, _, _| {
                    persist_called = true;
                    Ok(())
                },
            ),
            Err(RymeError::Conflict(_))
        ));
        assert!(!persist_called);
        assert_eq!(Wal::read_all(&dir).unwrap().len(), wal_records_before);

        drop(manager);
        let reopened = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let mut probe = reopened.begin();
        assert_eq!(reopened.get(&mut probe, &key).unwrap(), Some(b"winner".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn standard_mode_reads_segments_without_resident_engine_state() {
        let dir =
            std::env::temp_dir().join(format!("ryme-standard-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let key = RecordKey::new("t", "d", "messages", b"1");
        let mut first = manager.begin();
        manager.put(&mut first, key.clone(), b"one".to_vec());
        manager.commit(first).unwrap();
        assert_eq!(manager.resident_bytes().unwrap(), 0);

        let mut reader = manager.begin();
        let mut stale_writer = manager.begin();
        let mut second = manager.begin();
        manager.put(&mut second, key.clone(), b"two".to_vec());
        manager.commit(second).unwrap();
        manager.put(&mut stale_writer, key.clone(), b"stale".to_vec());
        assert!(matches!(manager.commit(stale_writer), Err(RymeError::Conflict(_))));
        assert_eq!(manager.get(&mut reader, &key).unwrap(), Some(b"one".to_vec()));
        assert_eq!(manager.resident_bytes().unwrap(), 0);

        let mut current = manager.begin();
        assert_eq!(manager.get(&mut current, &key).unwrap(), Some(b"two".to_vec()));
        assert_eq!(manager.resident_bytes().unwrap(), 0);
        let rows = manager.scan(&mut current, "t", "d", "messages", 10).unwrap();
        assert_eq!(rows, vec![(b"1".to_vec(), b"two".to_vec())]);
        drop(manager);

        let reopened = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let mut probe = reopened.begin();
        assert_eq!(reopened.get(&mut probe, &key).unwrap(), Some(b"two".to_vec()));
        assert_eq!(reopened.resident_bytes().unwrap(), 0);
        assert!(reopened.segment_cache_stats().misses > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn standard_mode_batch_reads_preserve_mvcc_staging_and_conflicts() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-standard-batch-read-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let first = RecordKey::new("t", "d", "items", b"first");
        let second = RecordKey::new("t", "d", "items", b"second");
        let missing = RecordKey::new("t", "d", "items", b"missing");

        let mut seed = manager.begin();
        manager.put(&mut seed, first.clone(), b"old".to_vec());
        manager.put(&mut seed, second.clone(), b"before-delete".to_vec());
        manager.commit(seed).unwrap();

        let mut stale = manager.begin();
        assert_eq!(
            manager.get_many(&mut stale, &[first.clone(), missing.clone()]).unwrap(),
            vec![Some(b"old".to_vec()), None]
        );
        let mut update = manager.begin();
        manager.put(&mut update, first.clone(), b"new".to_vec());
        manager.commit(update).unwrap();
        manager.put(&mut stale, second.clone(), b"must-not-commit".to_vec());
        assert!(matches!(manager.commit(stale), Err(RymeError::Conflict(_))));

        let mut delete = manager.begin();
        manager.delete(&mut delete, second.clone());
        manager.commit(delete).unwrap();
        let mut current = manager.begin();
        manager.put(&mut current, missing.clone(), b"staged".to_vec());
        assert_eq!(
            manager.get_many(&mut current, &[first, second, missing]).unwrap(),
            vec![Some(b"new".to_vec()), None, Some(b"staged".to_vec())]
        );

        drop(manager);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn standard_mode_batch_reads_skip_segment_lookup_for_staged_keys() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-standard-batch-staged-read-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let key = RecordKey::new("t", "d", "items", b"staged");
        let mut seed = manager.begin();
        manager.put(&mut seed, key.clone(), b"durable".to_vec());
        manager.commit(seed).unwrap();

        let mut txn = manager.begin();
        manager.put(&mut txn, key.clone(), b"staged".to_vec());
        let before = manager.segment_cache_stats();
        assert_eq!(manager.get_many(&mut txn, &[key]).unwrap(), vec![Some(b"staged".to_vec())]);
        assert_eq!(manager.segment_cache_stats(), before);

        drop(manager);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn standard_mode_batch_read_touches_each_cached_segment_once() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-standard-batch-cache-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let keys = (0..64)
            .map(|id| RecordKey::new("t", "d", "items", &(id as u64).to_be_bytes()))
            .collect::<Vec<_>>();
        for batch in keys.chunks(16) {
            let mut seed = manager.begin();
            for key in batch {
                manager.put(&mut seed, key.clone(), b"value".to_vec());
            }
            manager.commit(seed).unwrap();
        }

        let mut first_read = manager.begin();
        assert!(manager.get_many(&mut first_read, &keys).unwrap().iter().all(Option::is_some));
        let before_cached_batch = manager.segment_cache_stats();
        let mut second_read = manager.begin();
        assert!(manager.get_many(&mut second_read, &keys).unwrap().iter().all(Option::is_some));
        let after_cached_batch = manager.segment_cache_stats();
        assert_eq!(after_cached_batch.hits - before_cached_batch.hits, 4);

        drop(manager);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn standard_mode_scans_include_staged_writes() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-standard-scan-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let mut seed = manager.begin();
        manager.put(&mut seed, RecordKey::new("t", "d", "items", b"1"), b"one".to_vec());
        manager.commit(seed).unwrap();

        let mut txn = manager.begin();
        manager.put(&mut txn, RecordKey::new("t", "d", "items", b"2"), b"two".to_vec());
        assert_eq!(
            manager.scan(&mut txn, "t", "d", "items", 10).unwrap(),
            vec![(b"1".to_vec(), b"one".to_vec()), (b"2".to_vec(), b"two".to_vec())]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_then_wal_replay() {
        let dir =
            std::env::temp_dir().join(format!("ryme-snap-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let before = RecordKey::new("t", "d", "s", b"before");
        let mut first = manager.begin();
        manager.put(&mut first, before.clone(), b"1".to_vec());
        manager.commit(first).unwrap();
        let (max, _) = manager.write_snapshot().unwrap();
        assert!(max > 0);
        let after = RecordKey::new("t", "d", "s", b"after");
        let mut second = manager.begin();
        manager.put(&mut second, after.clone(), b"2".to_vec());
        manager.commit(second).unwrap();
        drop(manager);
        let reopened = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let mut probe = reopened.begin();
        assert_eq!(reopened.get(&mut probe, &before).unwrap(), Some(b"1".to_vec()));
        assert_eq!(reopened.get(&mut probe, &after).unwrap(), Some(b"2".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_snapshot_pointer_is_rejected() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-snapshot-pointer-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("snapshots")).unwrap();
        std::fs::write(dir.join("snapshots/latest"), b"../outside.rsnap").unwrap();
        assert!(matches!(
            DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always),
            Err(RymeError::Corrupt(message)) if message == "snapshot pointer"
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversized_snapshot_file_is_rejected_before_reading() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-oversized-snapshot-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let snapshots = dir.join("snapshots");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&snapshots).unwrap();
        let name = "snap-00000000000000000001.rsnap";
        let file = std::fs::File::create(snapshots.join(name)).unwrap();
        file.set_len(ryme_storage::MAX_SNAPSHOT_BYTES as u64 + 1).unwrap();
        std::fs::write(snapshots.join("latest"), name).unwrap();

        assert!(matches!(
            DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always),
            Err(RymeError::Overload(message)) if message == "snapshot"
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restore_persists_after_restart() {
        let dir =
            std::env::temp_dir().join(format!("ryme-restore-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let before = RecordKey::new("t", "d", "s", b"before");
        let mut first = manager.begin();
        manager.put(&mut first, before.clone(), b"one".to_vec());
        let checkpoint = manager.commit(first).unwrap();
        manager.write_snapshot().unwrap();
        let after = RecordKey::new("t", "d", "s", b"after");
        let mut second = manager.begin();
        manager.put(&mut second, after.clone(), b"two".to_vec());
        manager.commit(second).unwrap();
        assert_eq!(manager.restore_to(checkpoint).unwrap(), 0);
        drop(manager);

        let reopened = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let mut probe = reopened.begin();
        assert_eq!(reopened.get(&mut probe, &before).unwrap(), Some(b"one".to_vec()));
        assert_eq!(reopened.get(&mut probe, &after).unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn standard_restore_persists_after_restart() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-standard-restore-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let before = RecordKey::new("t", "d", "s", b"before");
        let mut first = manager.begin();
        manager.put(&mut first, before.clone(), b"one".to_vec());
        let checkpoint = manager.commit(first).unwrap();
        manager.write_snapshot().unwrap();
        let after = RecordKey::new("t", "d", "s", b"after");
        let mut second = manager.begin();
        manager.put(&mut second, after.clone(), b"two".to_vec());
        manager.commit(second).unwrap();
        manager.restore_to(checkpoint).unwrap();
        assert_eq!(manager.resident_bytes().unwrap(), 0);
        drop(manager);

        let reopened = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let mut probe = reopened.begin();
        assert_eq!(reopened.get(&mut probe, &before).unwrap(), Some(b"one".to_vec()));
        assert_eq!(reopened.get(&mut probe, &after).unwrap(), None);
        assert_eq!(reopened.resident_bytes().unwrap(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_fallback_recovers_without_snapshot_or_wal() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-fallback-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let key = RecordKey::new("t", "d", "s", b"segment-only");
        let mut txn = manager.begin();
        manager.put(&mut txn, key.clone(), b"durable".to_vec());
        manager.commit(txn).unwrap();
        manager.write_snapshot().unwrap();
        drop(manager);

        std::fs::remove_dir_all(dir.join("snapshots")).unwrap();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("wal") {
                std::fs::remove_file(path).unwrap();
            }
        }

        let reopened = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let mut probe = reopened.begin();
        assert_eq!(reopened.get(&mut probe, &key).unwrap(), Some(b"durable".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delta_segments_recover_multiple_commits_without_wal() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-delta-recover-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let first = RecordKey::new("t", "d", "s", b"first");
        let second = RecordKey::new("t", "d", "s", b"second");
        for (key, value) in [(&first, b"one".to_vec()), (&second, b"two".to_vec())] {
            let mut txn = manager.begin();
            manager.put(&mut txn, key.clone(), value);
            manager.commit(txn).unwrap();
        }
        drop(manager);

        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("wal") {
                std::fs::remove_file(path).unwrap();
            }
        }
        let reopened = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let mut probe = reopened.begin();
        assert_eq!(reopened.get(&mut probe, &first).unwrap(), Some(b"one".to_vec()));
        assert_eq!(reopened.get(&mut probe, &second).unwrap(), Some(b"two".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_publishes_one_recoverable_base() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-compaction-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let first = RecordKey::new("t", "d", "s", b"first");
        let second = RecordKey::new("t", "d", "s", b"second");
        for (key, value) in [(&first, b"one".to_vec()), (&second, b"two".to_vec())] {
            let mut txn = manager.begin();
            manager.put(&mut txn, key.clone(), value);
            manager.commit(txn).unwrap();
        }
        assert_eq!(
            ryme_storage::SegmentStore::open(&manager.segment_dir())
                .unwrap()
                .segment_paths()
                .unwrap()
                .len(),
            2
        );
        manager.compact_segments().unwrap().unwrap();
        assert_eq!(
            ryme_storage::SegmentStore::open(&manager.segment_dir())
                .unwrap()
                .segment_paths()
                .unwrap()
                .len(),
            1
        );
        drop(manager);

        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("wal") {
                std::fs::remove_file(path).unwrap();
            }
        }
        let reopened = DurableManager::open(&dir, 1024 * 1024, SyncPolicy::Always).unwrap();
        let mut probe = reopened.begin();
        assert_eq!(reopened.get(&mut probe, &first).unwrap(), Some(b"one".to_vec()));
        assert_eq!(reopened.get(&mut probe, &second).unwrap(), Some(b"two".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_sweep_compacts_standard_segments_at_threshold() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-standard-segment-threshold-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Never,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();

        for index in 0..(SEGMENT_COMPACTION_THRESHOLD - 1) {
            let mut txn = manager.begin();
            manager.put(
                &mut txn,
                RecordKey::new("tenant", "db", "items", &(index as u64).to_be_bytes()),
                vec![index as u8],
            );
            manager.commit(txn).unwrap();
        }
        assert_eq!(
            manager.segments.segment_paths().unwrap().len(),
            SEGMENT_COMPACTION_THRESHOLD - 1
        );
        manager.retention_sweep(1).unwrap();
        assert_eq!(
            manager.segments.segment_paths().unwrap().len(),
            SEGMENT_COMPACTION_THRESHOLD - 1
        );

        let mut txn = manager.begin();
        manager.put(
            &mut txn,
            RecordKey::new(
                "tenant",
                "db",
                "items",
                &((SEGMENT_COMPACTION_THRESHOLD - 1) as u64).to_be_bytes(),
            ),
            vec![(SEGMENT_COMPACTION_THRESHOLD - 1) as u8],
        );
        manager.commit(txn).unwrap();
        manager.retention_sweep(1).unwrap();
        assert_eq!(manager.segments.segment_paths().unwrap().len(), 1);
        drop(manager);

        let reopened = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Never,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let mut probe = reopened.begin();
        for index in 0..SEGMENT_COMPACTION_THRESHOLD {
            let key = RecordKey::new("tenant", "db", "items", &(index as u64).to_be_bytes());
            assert_eq!(reopened.get(&mut probe, &key).unwrap(), Some(vec![index as u8]));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn manual_standard_compaction_uses_bounded_delta_runs() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-standard-manual-compaction-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();

        let keys: Vec<_> =
            (0..3).map(|index| RecordKey::new("tenant", "db", "items", &[index])).collect();
        for (index, key) in keys.iter().enumerate() {
            let mut txn = manager.begin();
            manager.put(&mut txn, key.clone(), vec![index as u8]);
            manager.commit(txn).unwrap();
        }
        assert_eq!(manager.segments.segment_paths().unwrap().len(), 3);

        let compacted = manager.compact_segments().unwrap().unwrap();
        assert!(compacted.max_commit_ts > 0);
        let compacted_paths = manager.segments.segment_paths().unwrap();
        assert_eq!(compacted_paths.len(), 1);
        assert_eq!(compacted_paths[0].extension().and_then(|value| value.to_str()), Some("cmp"));
        drop(manager);

        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("wal") {
                std::fs::remove_file(path).unwrap();
            }
        }
        let reopened = DurableManager::open_with_mode(
            &dir,
            1024 * 1024,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        let mut probe = reopened.begin();
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(reopened.get(&mut probe, key).unwrap(), Some(vec![index as u8]));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_sweep_prunes_and_truncates() {
        let dir =
            std::env::temp_dir().join(format!("ryme-retain-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open(&dir, 64, SyncPolicy::Always).unwrap();
        for i in 0..6u8 {
            let mut txn = manager.begin();
            manager.put(&mut txn, RecordKey::new("t", "d", "s", &[i]), vec![i]);
            manager.commit(txn).unwrap();
            manager.write_snapshot().unwrap();
        }
        let (pruned, wal_removed) = manager.retention_sweep(2).unwrap();
        assert_eq!(pruned, 4);
        assert!(wal_removed >= 1);
        drop(manager);
        let reopened = DurableManager::open(&dir, 64, SyncPolicy::Always).unwrap();
        let mut probe = reopened.begin();
        for i in 0..6u8 {
            let key = RecordKey::new("t", "d", "s", &[i]);
            assert_eq!(reopened.get(&mut probe, &key).unwrap(), Some(vec![i]));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_truncates_wal_before_segment_pruning_can_fail() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-retain-corrupt-segment-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = DurableManager::open_with_mode(
            &dir,
            64,
            SyncPolicy::Always,
            1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        for index in 0..10u8 {
            let mut txn = manager.begin();
            manager.put(&mut txn, RecordKey::new("t", "d", "s", &[index]), vec![index]);
            manager.commit(txn).unwrap();
        }
        manager.write_snapshot().unwrap();
        for index in 10..73u8 {
            let mut txn = manager.begin();
            manager.put(&mut txn, RecordKey::new("t", "d", "s", &[index]), vec![index]);
            manager.commit(txn).unwrap();
        }

        let records_before = Wal::read_all(&dir).unwrap();
        assert!(records_before.iter().any(|record| record.commit_ts < 10));
        let latest_segment = manager.segments.segment_paths().unwrap().pop().unwrap();
        let mut bytes = std::fs::read(&latest_segment).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        std::fs::write(latest_segment, bytes).unwrap();

        assert!(manager.retention_sweep(1).is_err());
        let records_after = Wal::read_all(&dir).unwrap();
        assert!(records_after.iter().all(|record| record.commit_ts >= 10));
        assert!(records_after.len() < records_before.len());

        drop(manager);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn now_ms() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    }

    #[test]
    fn concurrent_disjoint_writes_all_commit() {
        let manager = TxnManager::new();
        let mut handles = Vec::new();
        for worker in 0..8 {
            let manager = manager.clone();
            handles.push(std::thread::spawn(move || {
                for index in 0..50 {
                    let key =
                        RecordKey::new("t", "d", "s", format!("w{worker}-{index}").as_bytes());
                    let mut txn = manager.begin();
                    manager.put(&mut txn, key, b"v".to_vec());
                    manager.commit(txn).unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let mut probe = manager.begin();
        let rows = manager.scan(&mut probe, "t", "d", "s", 1000).unwrap();
        assert_eq!(rows.len(), 400);
    }

    #[test]
    fn concurrent_counter_with_retry_is_exact() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "s", b"counter");
        let mut seed = manager.begin();
        manager.put(&mut seed, key.clone(), 0u64.to_be_bytes().to_vec());
        manager.commit(seed).unwrap();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let manager = manager.clone();
            let key = key.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..25 {
                    loop {
                        let mut txn = manager.begin();
                        let raw = manager.get(&mut txn, &key).unwrap().unwrap();
                        let mut current = [0u8; 8];
                        current.copy_from_slice(&raw);
                        let next = u64::from_be_bytes(current) + 1;
                        manager.put(&mut txn, key.clone(), next.to_be_bytes().to_vec());
                        match manager.commit(txn) {
                            Ok(_) => break,
                            Err(RymeError::Conflict(_)) => continue,
                            Err(e) => panic!("{e}"),
                        }
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let mut probe = manager.begin();
        let raw = manager.get(&mut probe, &key).unwrap().unwrap();
        let mut total = [0u8; 8];
        total.copy_from_slice(&raw);
        assert_eq!(u64::from_be_bytes(total), 200);
    }

    #[test]
    fn transaction_write_budget_rejects_without_staging_or_commit() {
        let manager = TxnManager::new();
        let mut txn = manager.begin();
        for index in 0..MAX_TRANSACTION_WRITE_ENTRIES {
            manager
                .try_put(
                    &mut txn,
                    RecordKey::new("tenant", "database", "table", index.to_be_bytes().as_slice()),
                    vec![0],
                )
                .unwrap();
        }
        let rejected = manager.try_put(
            &mut txn,
            RecordKey::new("tenant", "database", "table", b"overflow"),
            vec![0],
        );
        assert!(
            matches!(rejected, Err(RymeError::Overload(message)) if message == "transaction write set")
        );
        assert_eq!(txn.writes().len(), MAX_TRANSACTION_WRITE_ENTRIES);
        assert!(
            matches!(manager.commit(txn), Err(RymeError::Overload(message)) if message == "transaction write set")
        );
    }

    #[test]
    fn transaction_state_budget_rejects_oversized_value_before_staging() {
        let manager = TxnManager::new();
        let mut txn = manager.begin();
        let result = manager.try_put(
            &mut txn,
            RecordKey::new("tenant", "database", "table", b"oversized"),
            vec![0; MAX_TRANSACTION_STATE_BYTES],
        );
        assert!(
            matches!(result, Err(RymeError::Overload(message)) if message == "transaction state bytes")
        );
        assert!(txn.writes().is_empty());
        assert!(
            matches!(manager.commit(txn), Err(RymeError::Overload(message)) if message == "transaction state bytes")
        );
    }

    #[test]
    fn transaction_state_restore_preserves_budget_accounting() {
        let manager = TxnManager::new();
        let mut txn = manager.begin();
        manager
            .try_put(&mut txn, RecordKey::new("t", "d", "table", b"before"), b"value".to_vec())
            .unwrap();
        let checkpoint = txn.checkpoint();
        manager
            .try_put(&mut txn, RecordKey::new("t", "d", "table", b"after"), b"value".to_vec())
            .unwrap();
        txn.restore_checkpoint(&checkpoint);
        manager
            .try_put(&mut txn, RecordKey::new("t", "d", "table", b"replacement"), b"value".to_vec())
            .unwrap();
        assert_eq!(txn.writes().len(), 2);
        manager.commit(txn).unwrap();
    }

    #[test]
    fn transaction_state_reconstruction_rejects_oversized_input() {
        let manager = TxnManager::new();
        let state = TransactionState {
            read_ts: 1,
            writes: (0..=MAX_TRANSACTION_WRITE_ENTRIES)
                .map(|index| {
                    (
                        RecordKey::new("t", "d", "table", index.to_be_bytes().as_slice()),
                        WriteOp::put(vec![0]),
                    )
                })
                .collect(),
            read_set: Vec::new(),
            observed: Vec::new(),
            scanned: Vec::new(),
            constraint_scanned: Vec::new(),
            isolation: Isolation::Serializable,
            read_only: false,
            route_generation: None,
        };
        assert!(
            matches!(manager.from_state(state), Err(RymeError::Overload(message)) if message == "transaction write set")
        );
    }

    #[test]
    fn transaction_state_reconstruction_rejects_inconsistent_read_tracking() {
        let manager = TxnManager::new();
        let key = RecordKey::new("t", "d", "table", b"read");
        let mut txn = manager.begin();
        assert_eq!(manager.get(&mut txn, &key).unwrap(), None);
        let valid_state = txn.state();

        let mut missing_observation = valid_state.clone();
        missing_observation.observed.clear();
        assert!(matches!(
            manager.from_state(missing_observation),
            Err(RymeError::Corrupt(message)) if message == "transaction read state"
        ));

        let mut missing_read = valid_state.clone();
        missing_read.read_set.clear();
        assert!(matches!(
            manager.from_state(missing_read),
            Err(RymeError::Corrupt(message)) if message == "transaction read state"
        ));

        let mut duplicate_read = valid_state;
        duplicate_read.read_set.push(key);
        assert!(matches!(
            manager.from_state(duplicate_read),
            Err(RymeError::Corrupt(message)) if message == "transaction read state"
        ));
    }
}
