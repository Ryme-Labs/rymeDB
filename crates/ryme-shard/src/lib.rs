use ryme_error::{Result, RymeError};
use ryme_storage::{RecordKey, SegmentCacheStats, StorageMode};
use ryme_txn::{
    decode_writes, encode_writes, DurableManager, SyncPolicy, Transaction, TxnBackend, TxnTracker,
    MAX_GROUP_COMMIT_BYTES, MAX_GROUP_COMMIT_TRANSACTIONS,
};
use ryme_wal::Wal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableRef {
    pub tenant: String,
    pub database: String,
    pub table: String,
}

impl TableRef {
    pub fn new(tenant: &str, database: &str, table: &str) -> Self {
        Self {
            tenant: tenant.to_string(),
            database: database.to_string(),
            table: table.to_string(),
        }
    }

    pub fn of(key: &RecordKey) -> Self {
        Self::new(&key.tenant, &key.database, &key.table)
    }
}

#[derive(Debug, Clone)]
pub struct MoveReport {
    pub table: TableRef,
    pub from: usize,
    pub to: usize,
    pub rows: usize,
    pub bytes: u64,
    pub max_commit_ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangePlacement {
    pub id: String,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    #[serde(default)]
    pub routing_version: u8,
    pub shard: usize,
}

#[derive(Debug)]
struct ShardCtx {
    shard: Option<usize>,
    tables: Vec<TableRef>,
    touched: Instant,
}

#[derive(Debug)]
struct CommitRequest {
    txn: Transaction,
    reply: mpsc::SyncSender<Result<u64>>,
}

#[derive(Debug, Default)]
struct CommitQueueState {
    pending: Vec<CommitRequest>,
    pending_bytes: usize,
    processing: bool,
}

#[derive(Debug, Default)]
struct CommitQueue {
    state: Mutex<CommitQueueState>,
    ready: Condvar,
}

#[derive(Debug)]
struct ShardInner {
    base_dir: std::path::PathBuf,
    placement_path: std::path::PathBuf,
    range_placement_path: std::path::PathBuf,
    shards: Vec<DurableManager>,
    place: HashMap<TableRef, usize>,
    range_assignments: HashMap<String, usize>,
    ranges: Vec<RangePlacement>,
    paused: HashSet<TableRef>,
    side: HashMap<u64, ShardCtx>,
    coordinator: Mutex<Wal>,
}

#[derive(Debug, Clone)]
pub struct ShardSet {
    inner: Arc<Mutex<ShardInner>>,
    commit: Arc<RwLock<()>>,
    commit_queues: Arc<Vec<CommitQueue>>,
    group_commit_delay: Duration,
    commit_clock: Arc<AtomicU64>,
}

fn hash_table(table: &TableRef, buckets: usize) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in table
        .tenant
        .as_bytes()
        .iter()
        .chain(table.database.as_bytes())
        .chain(table.table.as_bytes())
    {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % buckets.max(1) as u64) as usize
}

type DecisionParts = Vec<(u32, Vec<u8>)>;

const COORDINATOR_DECISION_VERSION: u8 = 1;
const COORDINATOR_PLACEMENT_VERSION: u8 = 2;
const MAX_PENDING_GROUP_COMMITS: usize = 128;
const MAX_PENDING_GROUP_COMMIT_BYTES: usize = 64 * 1024 * 1024;

fn encode_decision(commit_ts: u64, parts: &[(u32, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(COORDINATOR_DECISION_VERSION);
    out.extend_from_slice(&commit_ts.to_le_bytes());
    out.extend_from_slice(&(parts.len() as u32).to_le_bytes());
    for (shard, payload) in parts {
        out.extend_from_slice(&shard.to_le_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
    }
    out
}

fn encode_placement(table: &TableRef, shard: usize) -> Result<Vec<u8>> {
    let shard = u32::try_from(shard)
        .map_err(|_| RymeError::InvalidArgument(String::from("placement shard")))?;
    let mut out = vec![COORDINATOR_PLACEMENT_VERSION];
    for component in [&table.tenant, &table.database, &table.table] {
        let bytes = component.as_bytes();
        let length = u32::try_from(bytes.len())
            .map_err(|_| RymeError::InvalidArgument(String::from("placement name")))?;
        out.extend_from_slice(&length.to_le_bytes());
        out.extend_from_slice(bytes);
    }
    out.extend_from_slice(&shard.to_le_bytes());
    Ok(out)
}

fn decode_placement(raw: &[u8]) -> Result<(TableRef, usize)> {
    if raw.first() != Some(&COORDINATOR_PLACEMENT_VERSION) {
        return Err(RymeError::Corrupt(String::from("placement")));
    }
    let mut offset = 1usize;
    let mut components = Vec::with_capacity(3);
    for _ in 0..3 {
        let length =
            raw.get(offset..offset + 4)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_le_bytes)
                .ok_or_else(|| RymeError::Corrupt(String::from("placement")))? as usize;
        offset = offset
            .checked_add(4)
            .and_then(|value| value.checked_add(length))
            .ok_or_else(|| RymeError::Corrupt(String::from("placement")))?;
        let start = offset - length;
        let component = std::str::from_utf8(
            raw.get(start..offset).ok_or_else(|| RymeError::Corrupt(String::from("placement")))?,
        )
        .map_err(|_| RymeError::Corrupt(String::from("placement")))?
        .to_string();
        components.push(component);
    }
    let shard = raw
        .get(offset..offset + 4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| RymeError::Corrupt(String::from("placement")))?;
    if offset + 4 != raw.len() {
        return Err(RymeError::Corrupt(String::from("placement")));
    }
    Ok((TableRef::new(&components[0], &components[1], &components[2]), shard as usize))
}

fn load_placements(path: &std::path::Path, shard_count: usize) -> Result<HashMap<TableRef, usize>> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error.into()),
    };
    let entries: Vec<(TableRef, usize)> = serde_json::from_slice(&raw)
        .map_err(|error| RymeError::Corrupt(format!("placements: {error}")))?;
    let mut placements = HashMap::with_capacity(entries.len());
    for (table, shard) in entries {
        if shard >= shard_count {
            return Err(RymeError::Corrupt(String::from("placement shard")));
        }
        placements.insert(table, shard);
    }
    Ok(placements)
}

fn load_range_assignments(
    path: &std::path::Path,
    shard_count: usize,
) -> Result<HashMap<String, usize>> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error.into()),
    };
    let entries: HashMap<String, usize> = serde_json::from_slice(&raw)
        .map_err(|error| RymeError::Corrupt(format!("range placements: {error}")))?;
    if entries.values().any(|shard| *shard >= shard_count) {
        return Err(RymeError::Corrupt(String::from("range placement shard")));
    }
    Ok(entries)
}

fn persist_range_assignments(
    path: &std::path::Path,
    assignments: &HashMap<String, usize>,
) -> Result<()> {
    let bytes = serde_json::to_vec(assignments)
        .map_err(|error| RymeError::Internal(format!("range placements: {error}")))?;
    let temporary = path.with_extension("json.tmp");
    let mut file = std::fs::File::create(&temporary)?;
    use std::io::Write;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn range_key(version: u8, key: &RecordKey) -> Option<Vec<u8>> {
    ryme_storage::record_routing_key(version, key).ok()
}

fn route_range(ranges: &[RangePlacement], key: &[u8]) -> Option<usize> {
    ranges.iter().rev().find(|range| range.start.as_slice() <= key).and_then(|range| {
        if range.end.is_empty() || key < range.end.as_slice() {
            Some(range.shard)
        } else {
            None
        }
    })
}

fn route_record(ranges: &[RangePlacement], key: &RecordKey) -> Option<usize> {
    let version = ranges.first()?.routing_version;
    route_range(ranges, range_key(version, key)?.as_slice())
}

fn decode_decision(raw: &[u8]) -> Result<(u64, DecisionParts)> {
    let corrupt = || RymeError::Corrupt(String::from("decision"));
    if raw.first() != Some(&1u8) || raw.len() < 13 {
        return Err(corrupt());
    }
    let commit_ts = u64::from_le_bytes(raw[1..9].try_into().map_err(|_| corrupt())?);
    let count = u32::from_le_bytes(raw[9..13].try_into().map_err(|_| corrupt())?) as usize;
    let remaining = raw.len() - 13;
    if count > remaining / 8 {
        return Err(corrupt());
    }
    let mut parts = Vec::with_capacity(count);
    let mut offset: usize = 13;
    for _ in 0..count {
        let header_end = offset.checked_add(8).ok_or_else(corrupt)?;
        if header_end > raw.len() {
            return Err(corrupt());
        }
        let shard = u32::from_le_bytes(raw[offset..offset + 4].try_into().map_err(|_| corrupt())?);
        let len = u32::from_le_bytes(raw[offset + 4..header_end].try_into().map_err(|_| corrupt())?)
            as usize;
        let payload_end = header_end.checked_add(len).ok_or_else(corrupt)?;
        if payload_end > raw.len() {
            return Err(corrupt());
        }
        parts.push((shard, raw[header_end..payload_end].to_vec()));
        offset = payload_end;
    }
    if offset != raw.len() {
        return Err(corrupt());
    }
    Ok((commit_ts, parts))
}

impl ShardSet {
    pub fn open(data_dir: &std::path::Path, shards: usize, policy: SyncPolicy) -> Result<Self> {
        Self::open_with_cache(data_dir, shards, policy, 64 * 1024 * 1024)
    }

    pub fn open_with_cache(
        data_dir: &std::path::Path,
        shards: usize,
        policy: SyncPolicy,
        cache_bytes: u64,
    ) -> Result<Self> {
        Self::open_with_mode(data_dir, shards, policy, cache_bytes, StorageMode::Hot)
    }

    pub fn open_with_mode(
        data_dir: &std::path::Path,
        shards: usize,
        policy: SyncPolicy,
        cache_bytes: u64,
        mode: StorageMode,
    ) -> Result<Self> {
        if shards == 0 || shards > 256 {
            return Err(RymeError::InvalidArgument(String::from("shards")));
        }
        let tracker = TxnTracker::new();
        let mut managers = Vec::new();
        for index in 0..shards {
            let dir = data_dir.join("shards").join(index.to_string()).join("wal");
            managers.push(DurableManager::open_with_mode_and_tracker(
                &dir,
                64 * 1024 * 1024,
                policy,
                cache_bytes,
                mode,
                &tracker,
            )?);
        }
        let coordinator = Wal::open(&data_dir.join("coordinator"), 1024 * 1024)?;
        let placement_path = data_dir.join("placements.json");
        let range_placement_path = data_dir.join("range-placements.json");
        let place = load_placements(&placement_path, shards)?;
        let range_assignments = load_range_assignments(&range_placement_path, shards)?;
        let latest_commit = managers.iter().map(DurableManager::latest_commit).max().unwrap_or(0);
        let set = Self {
            inner: Arc::new(Mutex::new(ShardInner {
                base_dir: data_dir.to_path_buf(),
                placement_path,
                range_placement_path,
                shards: managers,
                place,
                range_assignments,
                ranges: Vec::new(),
                paused: HashSet::new(),
                side: HashMap::new(),
                coordinator: Mutex::new(coordinator),
            })),
            commit: Arc::new(RwLock::new(())),
            commit_queues: Arc::new((0..shards).map(|_| CommitQueue::default()).collect()),
            group_commit_delay: if policy == SyncPolicy::Always {
                Duration::from_micros(100)
            } else {
                Duration::ZERO
            },
            commit_clock: Arc::new(AtomicU64::new(latest_commit)),
        };
        set.recover_coordinator()?;
        set.commit_clock.fetch_max(set.latest_commit(), Ordering::SeqCst);
        Ok(set)
    }

    fn recover_coordinator(&self) -> Result<()> {
        let dir = self
            .inner
            .lock()
            .map_err(|_| RymeError::Internal(String::from("shard lock")))?
            .base_dir
            .join("coordinator");
        let records = Wal::read_all(&dir)?;
        for record in records {
            match record.payload.first() {
                Some(&COORDINATOR_DECISION_VERSION) => {
                    let (commit_ts, parts) = decode_decision(&record.payload)?;
                    for (shard, payload) in parts {
                        let manager = self
                            .manager_for(shard as usize)
                            .ok_or_else(|| RymeError::Corrupt(String::from("cohort")))?;
                        let writes = decode_writes(&payload)?;
                        manager.replay_at(commit_ts, &writes)?;
                    }
                }
                Some(&COORDINATOR_PLACEMENT_VERSION) => {
                    let (table, shard) = decode_placement(&record.payload)?;
                    let mut inner = self
                        .inner
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("shard lock")))?;
                    if shard >= inner.shards.len() {
                        return Err(RymeError::Corrupt(String::from("placement shard")));
                    }
                    inner.place.insert(table, shard);
                }
                _ => return Err(RymeError::Corrupt(String::from("coordinator record"))),
            }
        }
        self.persist_placement_snapshot()?;
        Ok(())
    }

    fn persist_placement_snapshot(&self) -> Result<()> {
        let inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        Self::persist_placement_snapshot_locked(&inner)
    }

    fn persist_placement_snapshot_locked(inner: &ShardInner) -> Result<()> {
        let (path, entries) = {
            let mut entries: Vec<_> =
                inner.place.iter().map(|(table, shard)| (table.clone(), *shard)).collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            (inner.placement_path.clone(), entries)
        };
        let bytes = serde_json::to_vec(&entries)
            .map_err(|error| RymeError::Internal(format!("placements: {error}")))?;
        let temporary = path.with_extension("json.tmp");
        let mut file = std::fs::File::create(&temporary)?;
        use std::io::Write;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(temporary, path)?;
        Ok(())
    }

    pub fn shard_count(&self) -> usize {
        self.inner.lock().map(|inner| inner.shards.len()).unwrap_or(0)
    }

    pub fn segment_cache_stats(&self) -> Vec<SegmentCacheStats> {
        self.inner
            .lock()
            .map(|inner| inner.shards.iter().map(DurableManager::segment_cache_stats).collect())
            .unwrap_or_default()
    }

    pub fn latest_commit(&self) -> u64 {
        self.inner
            .lock()
            .map(|inner| {
                inner.shards.iter().map(|shard| shard.inner().latest_commit()).max().unwrap_or(0)
            })
            .unwrap_or(0)
    }

    pub fn expired_keys(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let _read =
            self.commit.read().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let shard = self.route_table(tenant, database, table);
        let manager =
            self.manager_for(shard).ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        manager.expired_keys(tenant, database, table, limit)
    }

    pub fn write_snapshots(&self) -> Result<Vec<(u64, std::path::PathBuf)>> {
        let managers = self
            .inner
            .lock()
            .map_err(|_| RymeError::Internal(String::from("shard lock")))?
            .shards
            .clone();
        managers.iter().map(DurableManager::write_snapshot).collect()
    }

    pub fn gc_oldest(&self) -> Result<u64> {
        let inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let mut horizon = u64::MAX;
        for shard in inner.shards.iter() {
            horizon = horizon.min(shard.gc_oldest()?);
        }
        Ok(if horizon == u64::MAX { 0 } else { horizon })
    }

    pub fn retention_sweep(&self, snapshot_keep: usize) -> Result<(usize, usize)> {
        let inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let mut pruned = 0;
        let mut wal_removed = 0;
        for shard in inner.shards.iter() {
            let (p, w) = shard.retention_sweep(snapshot_keep)?;
            pruned += p;
            wal_removed += w;
        }
        let mut floor: Option<u64> = None;
        for shard in inner.shards.iter() {
            if let Some(newest) = shard.newest_snapshot()? {
                floor = Some(floor.map(|v: u64| v.min(newest)).unwrap_or(newest));
            }
        }
        if let Some(floor) = floor {
            let mut coordinator = inner
                .coordinator
                .lock()
                .map_err(|_| RymeError::Internal(String::from("coordinator lock")))?;
            wal_removed += coordinator.truncate_below(floor)?;
        }
        Ok((pruned, wal_removed))
    }

    #[cfg(test)]
    fn shard_manager(&self, shard: usize) -> Option<DurableManager> {
        self.inner.lock().ok()?.shards.get(shard).cloned()
    }

    pub fn restore_to(&self, target: u64) -> Result<Vec<u64>> {
        let _write =
            self.commit.write().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let shards = self
            .inner
            .lock()
            .map_err(|_| RymeError::Internal(String::from("shard lock")))?
            .shards
            .clone();
        shards.into_iter().map(|shard| shard.restore_to(target)).collect()
    }

    pub fn shard_wal_dir(&self, shard: usize) -> Option<std::path::PathBuf> {
        let inner = self.inner.lock().ok()?;
        if shard >= inner.shards.len() {
            return None;
        }
        Some(inner.base_dir.join("shards").join(shard.to_string()).join("wal"))
    }

    pub fn spaces(&self) -> Vec<(String, String, String)> {
        let mut out = std::collections::BTreeSet::new();
        if let Ok(inner) = self.inner.lock() {
            for shard in inner.shards.iter() {
                if let Ok(spaces) = shard.spaces() {
                    out.extend(spaces);
                }
            }
        }
        out.into_iter().collect()
    }

    pub fn route(&self, table: &TableRef) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(shard) = inner.place.get(table) {
            return *shard;
        }
        hash_table(table, inner.shards.len())
    }

    fn route_record(&self, key: &RecordKey) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let table = TableRef::of(key);
        route_record(&inner.ranges, key).unwrap_or_else(|| {
            inner
                .place
                .get(&table)
                .copied()
                .unwrap_or_else(|| hash_table(&table, inner.shards.len()))
        })
    }

    pub fn range_placements(&self) -> Vec<RangePlacement> {
        self.inner.lock().map(|inner| inner.ranges.clone()).unwrap_or_default()
    }

    pub fn set_range_topology(&self, mut ranges: Vec<RangePlacement>) -> Result<()> {
        ranges.sort_by(|left, right| left.start.cmp(&right.start));
        if let Some(first) = ranges.first() {
            ryme_storage::validate_routing_version(first.routing_version)?;
            if ranges.iter().any(|range| range.routing_version != first.routing_version) {
                return Err(RymeError::InvalidArgument(String::from("mixed routing versions")));
            }
        }
        for (index, range) in ranges.iter().enumerate() {
            if range.shard >= self.shard_count() {
                return Err(RymeError::InvalidArgument(String::from("range shard")));
            }
            if !range.end.is_empty() && range.start >= range.end {
                return Err(RymeError::InvalidArgument(String::from("range bounds")));
            }
            if let Some(next) = ranges.get(index + 1) {
                if range.end.is_empty() || range.end != next.start {
                    return Err(RymeError::InvalidArgument(String::from("range gap")));
                }
            }
        }
        if let Some(first) = ranges.first() {
            if !first.start.is_empty() {
                return Err(RymeError::InvalidArgument(String::from("range start")));
            }
        }
        if let Some(last) = ranges.last() {
            if !last.end.is_empty() {
                return Err(RymeError::InvalidArgument(String::from("range end")));
            }
        }

        let _write =
            self.commit.write().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let old_ranges = inner.ranges.clone();
        let old_assignments = inner.range_assignments.clone();
        let mut assignments = inner.range_assignments.clone();
        for range in &ranges {
            let shard = assignments.get(&range.id).copied().or_else(|| {
                old_ranges
                    .iter()
                    .find(|old| {
                        (old.end.is_empty() || range.start < old.end)
                            && (range.end.is_empty() || old.start < range.end)
                    })
                    .map(|old| {
                        if range.start == old.start {
                            old.shard
                        } else {
                            (old.shard + 1) % inner.shards.len().max(1)
                        }
                    })
            });
            let shard = shard.unwrap_or(range.shard);
            assignments.insert(range.id.clone(), shard);
        }
        for range in &mut ranges {
            range.shard = assignments[&range.id];
        }

        let purges = Self::rebalance_ranges_locked(&inner, &ranges)?;
        let previous_ranges = inner.ranges.clone();
        let previous_assignments = inner.range_assignments.clone();
        inner.ranges = ranges;
        inner.range_assignments = assignments;
        if old_assignments != inner.range_assignments {
            if let Err(error) =
                persist_range_assignments(&inner.range_placement_path, &inner.range_assignments)
            {
                inner.ranges = previous_ranges;
                inner.range_assignments = previous_assignments;
                return Err(error);
            }
        }
        for (source_index, keys) in purges {
            let Some(source) = inner.shards.get(source_index) else {
                continue;
            };
            // Routing is durable now. Cleanup is deliberately best effort:
            // retaining a duplicate is safe, while deleting before routing is
            // durable can make the old topology lose data.
            if source.purge_keys(&keys).is_ok() {
                let _ = source.write_snapshot();
            }
        }
        Ok(())
    }

    fn rebalance_ranges_locked(
        inner: &ShardInner,
        ranges: &[RangePlacement],
    ) -> Result<Vec<(usize, Vec<RecordKey>)>> {
        let mut tables = std::collections::BTreeSet::new();
        for shard in &inner.shards {
            tables.extend(shard.spaces()?);
        }
        let mut moves: BTreeMap<(usize, usize, TableRef), ryme_storage::TableRows> =
            BTreeMap::new();
        for (tenant, database, table) in tables {
            for (source_index, source) in inner.shards.iter().enumerate() {
                let rows = source.export_table(&tenant, &database, &table)?;
                for (pk, versions) in rows {
                    let key = RecordKey::new(&tenant, &database, &table, &pk);
                    let target = route_record(ranges, &key)
                        .unwrap_or_else(|| hash_table(&TableRef::of(&key), inner.shards.len()));
                    if target != source_index {
                        moves
                            .entry((source_index, target, TableRef::of(&key)))
                            .or_default()
                            .push((key.pk, versions));
                    }
                }
            }
        }

        let mut changed = HashSet::new();
        let mut purges = Vec::new();
        for ((source_index, target, table), rows) in moves {
            let destination = inner
                .shards
                .get(target)
                .ok_or_else(|| RymeError::Unavailable(String::from("range shard")))?;
            let keys: Vec<RecordKey> = rows
                .iter()
                .map(|(pk, _)| RecordKey::new(&table.tenant, &table.database, &table.table, pk))
                .collect();
            destination.import_table(&table.tenant, &table.database, &table.table, rows)?;

            inner
                .shards
                .get(source_index)
                .ok_or_else(|| RymeError::Unavailable(String::from("range shard")))?;
            changed.insert(target);
            purges.push((source_index, keys));
        }
        for index in changed {
            inner.shards[index].write_snapshot()?;
        }
        Ok(purges)
    }

    pub fn route_table(&self, tenant: &str, database: &str, table: &str) -> usize {
        self.route(&TableRef::new(tenant, database, table))
    }

    pub fn layout(&self) -> Vec<(TableRef, usize)> {
        self.inner
            .lock()
            .map(|inner| inner.place.iter().map(|(table, shard)| (table.clone(), *shard)).collect())
            .unwrap_or_default()
    }

    pub fn tables(&self) -> Vec<(TableRef, usize, u64)> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut seen = std::collections::BTreeSet::new();
        for shard in inner.shards.iter() {
            if let Ok(spaces) = shard.spaces() {
                for (tenant, database, table) in spaces {
                    seen.insert((tenant, database, table));
                }
            }
        }
        let mut out = Vec::new();
        for (tenant, database, table) in seen {
            let table_ref = TableRef::new(&tenant, &database, &table);
            let shard = inner
                .place
                .get(&table_ref)
                .copied()
                .unwrap_or_else(|| hash_table(&table_ref, inner.shards.len()));
            let bytes = inner
                .shards
                .get(shard)
                .map(|shard| shard.table_bytes(&tenant, &database, &table).unwrap_or(0))
                .unwrap_or(0);
            out.push((table_ref, shard, bytes));
        }
        out
    }

    fn touch(&self, txn: &Transaction, key: &RecordKey) -> usize {
        let table = TableRef::of(key);
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.side.len() > 4096 {
            inner.side.retain(|_, ctx| ctx.touched.elapsed().as_secs() < 120);
        }
        let shard = route_record(&inner.ranges, key).unwrap_or_else(|| {
            inner
                .place
                .get(&table)
                .copied()
                .unwrap_or_else(|| hash_table(&table, inner.shards.len()))
        });
        let entry = inner.side.entry(txn.id).or_insert_with(|| ShardCtx {
            shard: None,
            tables: Vec::new(),
            touched: Instant::now(),
        });
        entry.touched = Instant::now();
        if !entry.tables.contains(&table) {
            entry.tables.push(table);
        }
        if entry.shard.is_none() {
            entry.shard = Some(shard);
        }
        entry.shard.unwrap_or(shard)
    }

    fn manager_for(&self, shard: usize) -> Option<DurableManager> {
        self.inner.lock().ok()?.shards.get(shard).cloned()
    }

    fn reserve_commit_ts(&self, latest_commit: u64, read_ts: u64) -> Result<u64> {
        let mut observed = self.commit_clock.load(Ordering::SeqCst);
        loop {
            let next = observed
                .max(latest_commit)
                .max(read_ts)
                .checked_add(1)
                .ok_or_else(|| RymeError::Corrupt(String::from("commit timestamp overflow")))?;
            match self.commit_clock.compare_exchange_weak(
                observed,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(next),
                Err(current) => observed = current,
            }
        }
    }

    fn commit_single(&self, txn: Transaction, ctx: ShardCtx, shard: usize) -> Result<u64> {
        if txn.writes().is_empty() {
            return Ok(txn.read_ts);
        }
        let _read =
            self.commit.read().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        // Pin topology with the read guard; release metadata lock before WAL I/O.
        let manager = {
            let inner =
                self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
            if Some(shard) != ctx.shard {
                return Err(RymeError::Unavailable(String::from("rescheduling")));
            }
            for table in ctx.tables.iter() {
                if inner.paused.contains(table) {
                    return Err(RymeError::Unavailable(String::from("rescheduling")));
                }
            }
            let manager = inner
                .shards
                .get(shard)
                .cloned()
                .ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
            manager
        };
        self.enqueue_group_commit(shard, manager, txn)
    }

    fn enqueue_group_commit(
        &self,
        shard: usize,
        manager: DurableManager,
        txn: Transaction,
    ) -> Result<u64> {
        let queue = self
            .commit_queues
            .get(shard)
            .ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        let (reply, result) = mpsc::sync_channel(1);
        let is_processor = {
            let mut state = queue
                .state
                .lock()
                .map_err(|_| RymeError::Internal(String::from("commit queue")))?;
            let txn_bytes = txn.state_bytes();
            if state.pending.len() >= MAX_PENDING_GROUP_COMMITS
                || (!state.pending.is_empty()
                    && state.pending_bytes.saturating_add(txn_bytes)
                        > MAX_PENDING_GROUP_COMMIT_BYTES)
            {
                return Err(RymeError::Overload(String::from("commit queue")));
            }
            state.pending_bytes = state.pending_bytes.saturating_add(txn_bytes);
            state.pending.push(CommitRequest { txn, reply });
            let is_processor = !state.processing;
            if is_processor {
                state.processing = true;
            }
            if state.pending.len() >= MAX_GROUP_COMMIT_TRANSACTIONS {
                queue.ready.notify_one();
            }
            is_processor
        };

        if is_processor {
            loop {
                let batch = {
                    let mut state = queue
                        .state
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("commit queue")))?;
                    if state.pending.len() < MAX_GROUP_COMMIT_TRANSACTIONS
                        && !self.group_commit_delay.is_zero()
                    {
                        state = queue
                            .ready
                            .wait_timeout(state, self.group_commit_delay)
                            .map_err(|_| RymeError::Internal(String::from("commit queue")))?
                            .0;
                    }
                    let mut count = 0usize;
                    let mut group_bytes = 0usize;
                    for request in state.pending.iter().take(MAX_GROUP_COMMIT_TRANSACTIONS) {
                        let request_bytes = request.txn.state_bytes();
                        if count > 0
                            && group_bytes.saturating_add(request_bytes) > MAX_GROUP_COMMIT_BYTES
                        {
                            break;
                        }
                        group_bytes = group_bytes.saturating_add(request_bytes);
                        count += 1;
                    }
                    let batch = state.pending.drain(..count).collect::<Vec<_>>();
                    let batch_bytes = batch.iter().fold(0usize, |total, request| {
                        total.saturating_add(request.txn.state_bytes())
                    });
                    state.pending_bytes = state.pending_bytes.saturating_sub(batch_bytes);
                    batch
                };

                let mut replies = Vec::with_capacity(batch.len());
                let mut commits = Vec::with_capacity(batch.len());
                for request in batch {
                    match self.reserve_commit_ts(0, request.txn.read_ts) {
                        Ok(commit_ts) => {
                            commits.push((request.txn, commit_ts));
                            replies.push(request.reply);
                        }
                        Err(error) => {
                            let _ = request.reply.send(Err(error));
                        }
                    }
                }
                for (reply, result) in
                    replies.into_iter().zip(manager.commit_at_batch_owned(commits))
                {
                    let _ = reply.send(result);
                }

                let mut state = queue
                    .state
                    .lock()
                    .map_err(|_| RymeError::Internal(String::from("commit queue")))?;
                if state.pending.is_empty() {
                    state.processing = false;
                    break;
                }
            }
        }

        result.recv().map_err(|_| RymeError::Unavailable(String::from("commit queue closed")))?
    }

    fn commit_two_phase(&self, txn: Transaction) -> Result<u64> {
        let _write =
            self.commit.write().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let shards = inner.shards.len();
        let mut cohort: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
        for key in txn.writes().keys().chain(txn.read_keys().iter()) {
            let table = TableRef::of(key);
            if inner.paused.contains(&table) {
                return Err(RymeError::Unavailable(String::from("rescheduling")));
            }
            let shard = route_record(&inner.ranges, key).unwrap_or_else(|| {
                inner.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards))
            });
            cohort.insert(shard);
        }
        for table in txn.scanned_tables().iter() {
            let table = TableRef::new(&table.0, &table.1, &table.2);
            if inner.paused.contains(&table) {
                return Err(RymeError::Unavailable(String::from("rescheduling")));
            }
            if inner.ranges.is_empty() {
                let shard =
                    inner.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards));
                cohort.insert(shard);
            } else {
                cohort.extend(0..shards);
            }
        }
        if cohort.len() < 2 {
            let shard = cohort.iter().next().copied().unwrap_or(0);
            let ctx = ShardCtx { shard: Some(shard), tables: Vec::new(), touched: Instant::now() };
            drop(_write);
            drop(inner);
            return self.commit_single(txn, ctx, shard);
        }
        let managers: Vec<(usize, DurableManager)> = cohort
            .iter()
            .map(|shard| {
                inner
                    .shards
                    .get(*shard)
                    .cloned()
                    .map(|manager| (*shard, manager))
                    .ok_or_else(|| RymeError::Unavailable(String::from("shard")))
            })
            .collect::<Result<Vec<_>>>()?;
        let writers: std::collections::BTreeSet<usize> = txn
            .writes()
            .keys()
            .map(|key| {
                let table = TableRef::of(key);
                route_record(&inner.ranges, key).unwrap_or_else(|| {
                    inner.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards))
                })
            })
            .collect();
        let commit_ts = self.reserve_commit_ts(0, txn.read_ts)?;
        for (_, manager) in managers.iter() {
            manager.inner().validate_at(&txn, commit_ts)?;
        }
        let mut parts: DecisionParts = Vec::with_capacity(writers.len());
        for shard in writers.iter() {
            let subset: std::collections::BTreeMap<RecordKey, ryme_txn::WriteOp> = txn
                .writes()
                .iter()
                .filter(|(key, _)| {
                    let table = TableRef::of(key);
                    route_record(&inner.ranges, key).unwrap_or_else(|| {
                        inner
                            .place
                            .get(&table)
                            .copied()
                            .unwrap_or_else(|| hash_table(&table, shards))
                    }) == *shard
                })
                .map(|(key, op)| (key.clone(), op.clone()))
                .collect();
            parts.push((*shard as u32, encode_writes(&subset)?));
        }
        {
            let mut coordinator = inner
                .coordinator
                .lock()
                .map_err(|_| RymeError::Internal(String::from("coordinator lock")))?;
            coordinator.append(commit_ts, &encode_decision(commit_ts, &parts))?;
            coordinator.sync()?;
        }
        for (shard, manager) in managers.iter() {
            let index = *shard;
            manager.commit_at(&txn, commit_ts, |key| {
                let table = TableRef::of(key);
                route_record(&inner.ranges, key).unwrap_or_else(|| {
                    inner.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards))
                }) == index
            })?;
        }
        Ok(commit_ts)
    }

    pub fn move_table(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        target: Option<usize>,
    ) -> Result<MoveReport> {
        let table_ref = TableRef::new(tenant, database, table);
        let _write =
            self.commit.write().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        if !inner.ranges.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("range placement")));
        }
        let from = inner
            .place
            .get(&table_ref)
            .copied()
            .unwrap_or_else(|| hash_table(&table_ref, inner.shards.len()));
        let to = match target {
            Some(target) if target < inner.shards.len() => target,
            Some(_) => return Err(RymeError::InvalidArgument(String::from("target"))),
            None => {
                let mut counts = vec![0usize; inner.shards.len()];
                for shard in inner.place.values() {
                    if let Some(count) = counts.get_mut(*shard) {
                        *count += 1;
                    }
                }
                counts
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index != from)
                    .min_by_key(|(_, count)| **count)
                    .map(|(index, _)| index)
                    .unwrap_or(from)
            }
        };
        if to == from {
            return Err(RymeError::InvalidArgument(String::from("target")));
        }
        let source = inner
            .shards
            .get(from)
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("shard")))?;
        let dest = inner
            .shards
            .get(to)
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("shard")))?;
        inner.paused.insert(table_ref.clone());
        let result = Self::transfer_inner(&table_ref, &source, &dest, from, to);
        let result = match result {
            Err(error) => Err(error),
            Ok(report) => (|| -> Result<MoveReport> {
                let placement = encode_placement(&table_ref, to)?;
                let commit_ts = inner
                    .shards
                    .iter()
                    .map(|shard| shard.inner().latest_commit())
                    .max()
                    .unwrap_or(0)
                    .saturating_add(1);
                let mut coordinator = inner
                    .coordinator
                    .lock()
                    .map_err(|_| RymeError::Internal(String::from("coordinator lock")))?;
                coordinator.append(commit_ts, &placement)?;
                coordinator.sync()?;
                drop(coordinator);

                let previous = inner.place.insert(table_ref.clone(), to);
                if let Err(error) = Self::persist_placement_snapshot_locked(&inner) {
                    match previous {
                        Some(shard) => {
                            inner.place.insert(table_ref.clone(), shard);
                        }
                        None => {
                            inner.place.remove(&table_ref);
                        }
                    }
                    Err(error)
                } else {
                    source.inner().drop_table(
                        &table_ref.tenant,
                        &table_ref.database,
                        &table_ref.table,
                    )?;
                    source.write_snapshot()?;
                    Ok(report)
                }
            })(),
        };
        inner.paused.remove(&table_ref);
        result
    }

    fn transfer_inner(
        table: &TableRef,
        source: &DurableManager,
        dest: &DurableManager,
        from: usize,
        to: usize,
    ) -> Result<MoveReport> {
        let rows = source.export_table(&table.tenant, &table.database, &table.table)?;
        let count = rows.len();
        let bytes: u64 = source.table_bytes(&table.tenant, &table.database, &table.table)?;
        let max = dest.import_table(&table.tenant, &table.database, &table.table, rows)?;
        dest.write_snapshot()?;
        let moved = dest.table_bytes(&table.tenant, &table.database, &table.table)?;
        if moved < bytes {
            return Err(RymeError::Corrupt(String::from("move verify")));
        }
        Ok(MoveReport { table: table.clone(), from, to, rows: count, bytes, max_commit_ts: max })
    }

    fn range_scan_managers(&self) -> Result<Option<Vec<DurableManager>>> {
        let inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        if inner.ranges.is_empty() {
            Ok(None)
        } else {
            Ok(Some(inner.shards.clone()))
        }
    }
}

impl TxnBackend for ShardSet {
    fn begin(&self) -> Transaction {
        let _write = self.commit.write().unwrap_or_else(|error| error.into_inner());
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let mut txn = inner.shards.first().expect("ShardSet must have a shard").begin();
        txn.restamp(self.commit_clock.load(Ordering::SeqCst).max(1));
        txn
    }

    fn get(&self, txn: &mut Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        let _read =
            self.commit.read().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let shard = self.route_record(key);
        let manager =
            self.manager_for(shard).ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        manager.get(txn, key)
    }

    fn put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) {
        let shard = self.touch(txn, &key);
        if let Some(manager) = self.manager_for(shard) {
            manager.put(txn, key, value);
        }
    }

    fn put_with_ttl(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>, expires_at: u64) {
        let shard = self.touch(txn, &key);
        if let Some(manager) = self.manager_for(shard) {
            manager.put_with_ttl(txn, key, value, expires_at);
        }
    }

    fn delete(&self, txn: &mut Transaction, key: RecordKey) {
        let shard = self.touch(txn, &key);
        if let Some(manager) = self.manager_for(shard) {
            manager.delete(txn, key);
        }
    }

    fn expires_at(&self, key: &RecordKey) -> Result<Option<u64>> {
        let _read =
            self.commit.read().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let shard = self.route_record(key);
        let manager =
            self.manager_for(shard).ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        manager.expires_at(key)
    }

    fn expires_at_at(&self, key: &RecordKey, read_ts: u64) -> Result<Option<u64>> {
        let _read =
            self.commit.read().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let shard = self.route_record(key);
        let manager =
            self.manager_for(shard).ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        manager.expires_at_at(key, read_ts)
    }

    fn commit(&self, txn: Transaction) -> impl std::future::Future<Output = Result<u64>> + Send {
        let backend = self.clone();
        async move {
            txn.ensure_healthy()?;
            let ctx = backend
                .inner
                .lock()
                .map_err(|_| RymeError::Internal(String::from("shard lock")))?
                .side
                .remove(&txn.id);
            let Some(ctx) = ctx else {
                return Ok(txn.read_ts);
            };
            if txn.writes().is_empty() {
                return Ok(txn.read_ts);
            }
            let guard = backend
                .inner
                .lock()
                .map_err(|_| RymeError::Internal(String::from("shard lock")))?;
            let shards = guard.shards.len();
            for table in ctx.tables.iter() {
                if guard.paused.contains(table) {
                    return Err(RymeError::Unavailable(String::from("rescheduling")));
                }
            }
            let mut cohort: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
            for key in txn.writes().keys().chain(txn.read_keys().iter()) {
                let table = TableRef::of(key);
                let shard = route_record(&guard.ranges, key).unwrap_or_else(|| {
                    guard.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards))
                });
                cohort.insert(shard);
            }
            for table in txn.scanned_tables().iter() {
                let table = TableRef::new(&table.0, &table.1, &table.2);
                if guard.ranges.is_empty() {
                    let shard = guard
                        .place
                        .get(&table)
                        .copied()
                        .unwrap_or_else(|| hash_table(&table, shards));
                    cohort.insert(shard);
                } else {
                    cohort.extend(0..shards);
                }
            }
            drop(guard);
            if cohort.len() == 1 {
                let shard = cohort.iter().next().copied().unwrap_or(0);
                return backend.commit_single(txn, ctx, shard);
            }
            backend.commit_two_phase(txn)
        }
    }

    fn scan(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let _read =
            self.commit.read().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        if let Some(managers) = self.range_scan_managers()? {
            txn.record_scan(tenant, database, table)?;
            let scan_limit =
                limit.saturating_add(txn.scan_overlay_count(tenant, database, table, None));
            let mut rows = Vec::new();
            for manager in managers {
                let mut scan_txn = manager.begin();
                scan_txn.restamp(txn.read_ts);
                rows.extend(manager.scan(&mut scan_txn, tenant, database, table, scan_limit)?);
            }
            return Ok(txn.overlay_scan(rows, tenant, database, table, None, limit));
        }
        let shard = self.route(&TableRef::new(tenant, database, table));
        let manager =
            self.manager_for(shard).ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        manager.scan(txn, tenant, database, table, limit)
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
        let _read =
            self.commit.read().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        if let Some(managers) = self.range_scan_managers()? {
            txn.record_scan(tenant, database, table)?;
            let scan_limit = limit.saturating_add(txn.scan_overlay_count(
                tenant,
                database,
                table,
                Some(start_after),
            ));
            let mut rows = Vec::new();
            for manager in managers {
                let mut scan_txn = manager.begin();
                scan_txn.restamp(txn.read_ts);
                rows.extend(manager.scan_after(
                    &mut scan_txn,
                    tenant,
                    database,
                    table,
                    start_after,
                    scan_limit,
                )?);
            }
            return Ok(txn.overlay_scan(rows, tenant, database, table, Some(start_after), limit));
        }
        let shard = self.route(&TableRef::new(tenant, database, table));
        let manager =
            self.manager_for(shard).ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        manager.scan_after(txn, tenant, database, table, start_after, limit)
    }
}

pub fn placement_key(table: &TableRef, shards: usize) -> usize {
    hash_table(table, shards)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Replicated,
    Local,
}

#[derive(Debug)]
struct HybridInner {
    raft: ryme_raft::net::ClusterBackend,
    local: ShardSet,
    replicated: HashSet<TableRef>,
}

#[derive(Debug, Clone)]
pub struct HybridBackend {
    inner: Arc<Mutex<HybridInner>>,
}

impl HybridBackend {
    pub fn new(
        raft: ryme_raft::net::ClusterBackend,
        local: ShardSet,
        replicated: HashSet<TableRef>,
    ) -> Self {
        Self { inner: Arc::new(Mutex::new(HybridInner { raft, local, replicated })) }
    }

    pub fn tier(&self, table: &TableRef) -> Tier {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.replicated.contains(table) {
            Tier::Replicated
        } else {
            Tier::Local
        }
    }

    pub fn raft(&self) -> ryme_raft::net::ClusterBackend {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).raft.clone()
    }

    pub fn local(&self) -> ShardSet {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).local.clone()
    }

    pub fn gc_oldest(&self) -> Result<u64> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let raft_horizon = inner.raft.gc_oldest()?;
        drop(inner);
        let local_horizon = self.local().gc_oldest()?;
        Ok(raft_horizon.min(local_horizon))
    }

    pub fn retention_sweep(&self, snapshot_keep: usize) -> Result<(usize, usize)> {
        self.local().retention_sweep(snapshot_keep)
    }

    pub fn tables(&self) -> Vec<(TableRef, Tier, u64)> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut seen = std::collections::BTreeSet::new();
        for space in inner.local.spaces() {
            seen.insert(TableRef::new(&space.0, &space.1, &space.2));
        }
        seen.extend(inner.replicated.iter().cloned());
        seen.into_iter()
            .map(|table| {
                let tier =
                    if inner.replicated.contains(&table) { Tier::Replicated } else { Tier::Local };
                (table, tier, 0)
            })
            .collect()
    }
}

impl TxnBackend for HybridBackend {
    fn begin(&self) -> Transaction {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let raft = inner.raft.clone();
        let local = inner.local.clone();
        drop(inner);
        let mut txn = local.begin();
        raft.node().manager().track_transaction(&mut txn);
        let read_ts = raft.node().manager().latest_commit().max(local.latest_commit()).max(1);
        txn.restamp(read_ts);
        txn
    }

    fn get(&self, txn: &mut Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        let tier = self.tier(&TableRef::of(key));
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match tier {
            Tier::Replicated => inner.raft.get(txn, key),
            Tier::Local => inner.local.get(txn, key),
        }
    }

    fn put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.replicated.contains(&TableRef::of(&key)) {
            inner.raft.put(txn, key, value);
        } else {
            inner.local.put(txn, key, value);
        }
    }

    fn put_with_ttl(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>, expires_at: u64) {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.replicated.contains(&TableRef::of(&key)) {
            inner.raft.put_with_ttl(txn, key, value, expires_at);
        } else {
            inner.local.put_with_ttl(txn, key, value, expires_at);
        }
    }

    fn delete(&self, txn: &mut Transaction, key: RecordKey) {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.replicated.contains(&TableRef::of(&key)) {
            inner.raft.delete(txn, key);
        } else {
            inner.local.delete(txn, key);
        }
    }

    fn expires_at(&self, key: &RecordKey) -> Result<Option<u64>> {
        let tier = self.tier(&TableRef::of(key));
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match tier {
            Tier::Replicated => inner.raft.expires_at(key),
            Tier::Local => inner.local.expires_at(key),
        }
    }

    fn expires_at_at(&self, key: &RecordKey, read_ts: u64) -> Result<Option<u64>> {
        let tier = self.tier(&TableRef::of(key));
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match tier {
            Tier::Replicated => inner.raft.expires_at_at(key, read_ts),
            Tier::Local => inner.local.expires_at_at(key, read_ts),
        }
    }

    fn commit(&self, txn: Transaction) -> impl std::future::Future<Output = Result<u64>> + Send {
        let backend = self.clone();
        async move {
            txn.ensure_healthy()?;
            if txn.writes().is_empty() {
                return Ok(txn.read_ts);
            }
            let (tier, raft, local) = {
                let inner = backend
                    .inner
                    .lock()
                    .map_err(|_| RymeError::Internal(String::from("shard lock")))?;
                let tables =
                    txn.writes().keys().chain(txn.read_keys().iter()).map(TableRef::of).chain(
                        txn.scanned_tables().iter().map(|(tenant, database, table)| {
                            TableRef::new(tenant, database, table)
                        }),
                    );
                let mut tier = None;
                for table in tables {
                    let current = if inner.replicated.contains(&table) {
                        Tier::Replicated
                    } else {
                        Tier::Local
                    };
                    if tier.is_some_and(|seen| seen != current) {
                        return Err(RymeError::InvalidArgument(String::from("cross-tier")));
                    }
                    tier = Some(current);
                }
                (
                    tier.ok_or_else(|| RymeError::Internal(String::from("hybrid write tier")))?,
                    inner.raft.clone(),
                    inner.local.clone(),
                )
            };
            if tier == Tier::Replicated {
                <ryme_raft::net::ClusterBackend as TxnBackend>::commit(&raft, txn).await
            } else {
                <ShardSet as TxnBackend>::commit(&local, txn).await
            }
        }
    }

    fn scan(
        &self,
        txn: &mut Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tier = self.tier(&TableRef::new(tenant, database, table));
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match tier {
            Tier::Replicated => inner.raft.scan(txn, tenant, database, table, limit),
            Tier::Local => inner.local.scan(txn, tenant, database, table, limit),
        }
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
        let tier = self.tier(&TableRef::new(tenant, database, table));
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match tier {
            Tier::Replicated => {
                inner.raft.scan_after(txn, tenant, database, table, start_after, limit)
            }
            Tier::Local => inner.local.scan_after(txn, tenant, database, table, start_after, limit),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writes(pairs: Vec<(&str, &str)>) -> Vec<(Vec<u8>, Vec<u8>)> {
        pairs.into_iter().map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec())).collect()
    }

    fn commit_table(
        backend: &ShardSet,
        tenant: &str,
        database: &str,
        table: &str,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> u64 {
        let mut txn = backend.begin();
        for (pk, value) in rows {
            backend.put(&mut txn, RecordKey::new(tenant, database, table, &pk), value);
        }
        futures_executor_block_on(backend.commit(txn)).unwrap()
    }

    fn futures_executor_block_on<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        let waker = noop_waker();
        let mut boxed = Box::pin(future);
        let mut context = std::task::Context::from_waker(&waker);
        loop {
            match boxed.as_mut().poll(&mut context) {
                std::task::Poll::Ready(value) => return value,
                std::task::Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn noop_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &TABLE)
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop(_: *const ()) {}
        static TABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &TABLE)) }
    }

    #[test]
    fn routes_tables_to_shards() {
        let dir =
            std::env::temp_dir().join(format!("ryme-shard-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        assert_eq!(backend.shard_count(), 4);
        commit_table(&backend, "t", "d", "users", writes(vec![("1", "a")]));
        commit_table(&backend, "t", "d", "orders", writes(vec![("9", "z")]));
        let mut txn = backend.begin();
        let key = RecordKey::new("t", "d", "users", b"1");
        assert_eq!(backend.get(&mut txn, &key).unwrap(), Some(b"a".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn range_topology_routes_and_migrates_rows() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-range-shard-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 2, SyncPolicy::Never).unwrap();
        commit_table(&backend, "t", "d", "users", writes(vec![("a", "one"), ("z", "last")]));
        backend
            .set_range_topology(vec![RangePlacement {
                id: String::from("users-all"),
                start: Vec::new(),
                end: Vec::new(),
                routing_version: 0,
                shard: 0,
            }])
            .unwrap();
        backend
            .set_range_topology(vec![
                RangePlacement {
                    id: String::from("users-left"),
                    start: Vec::new(),
                    end: b"users\0m".to_vec(),
                    routing_version: 0,
                    shard: 0,
                },
                RangePlacement {
                    id: String::from("users-right"),
                    start: b"users\0m".to_vec(),
                    end: Vec::new(),
                    routing_version: 0,
                    shard: 0,
                },
            ])
            .unwrap();
        let mut txn = backend.begin();
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "users", b"a")).unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "users", b"z")).unwrap(),
            Some(b"last".to_vec())
        );
        let mut scan_txn = backend.begin();
        backend.put(&mut scan_txn, RecordKey::new("t", "d", "users", b"b"), b"middle".to_vec());
        backend.delete(&mut scan_txn, RecordKey::new("t", "d", "users", b"a"));
        assert_eq!(
            backend.scan(&mut scan_txn, "t", "d", "users", 2).unwrap(),
            vec![(b"b".to_vec(), b"middle".to_vec()), (b"z".to_vec(), b"last".to_vec())]
        );
        let left = backend.shard_manager(0).unwrap();
        let right = backend.shard_manager(1).unwrap();
        let mut left_txn = left.begin();
        let mut right_txn = right.begin();
        assert_eq!(
            left.get(&mut left_txn, &RecordKey::new("t", "d", "users", b"a")).unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            right.get(&mut right_txn, &RecordKey::new("t", "d", "users", b"z")).unwrap(),
            Some(b"last".to_vec())
        );
        drop(backend);

        let backend = ShardSet::open(&dir, 2, SyncPolicy::Never).unwrap();
        backend
            .set_range_topology(vec![
                RangePlacement {
                    id: String::from("users-left"),
                    start: Vec::new(),
                    end: b"users\0m".to_vec(),
                    routing_version: 0,
                    shard: 0,
                },
                RangePlacement {
                    id: String::from("users-right"),
                    start: b"users\0m".to_vec(),
                    end: Vec::new(),
                    routing_version: 0,
                    shard: 0,
                },
            ])
            .unwrap();
        let placements = backend.range_placements();
        assert_eq!(placements[0].shard, 0);
        assert_eq!(placements[1].shard, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn range_topology_standard_mode_preserves_rows() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-range-standard-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open_with_mode(
            &dir,
            2,
            SyncPolicy::Never,
            64 * 1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        commit_table(&backend, "t", "d", "users", writes(vec![("a", "one"), ("z", "last")]));
        backend
            .set_range_topology(vec![RangePlacement {
                id: String::from("users-all"),
                start: Vec::new(),
                end: Vec::new(),
                routing_version: 0,
                shard: 0,
            }])
            .unwrap();
        backend
            .set_range_topology(vec![
                RangePlacement {
                    id: String::from("users-left"),
                    start: Vec::new(),
                    end: b"users\0m".to_vec(),
                    routing_version: 0,
                    shard: 0,
                },
                RangePlacement {
                    id: String::from("users-right"),
                    start: b"users\0m".to_vec(),
                    end: Vec::new(),
                    routing_version: 0,
                    shard: 0,
                },
            ])
            .unwrap();
        let mut txn = backend.begin();
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "users", b"a")).unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "users", b"z")).unwrap(),
            Some(b"last".to_vec())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cross_shard_commit_succeeds() {
        let dir =
            std::env::temp_dir().join(format!("ryme-shard-x-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 8, SyncPolicy::Never).unwrap();
        let mut first = None;
        for index in 0..32 {
            let table = format!("table-{index}");
            let shard = backend.route(&TableRef::new("t", "d", &table));
            if first.map(|first| first == shard).unwrap_or(true) {
                first = Some(shard);
            } else {
                let mut txn = backend.begin();
                backend.put(&mut txn, RecordKey::new("t", "d", "table-0", b"k"), b"v".to_vec());
                backend.put(&mut txn, RecordKey::new("t", "d", &table, b"k"), b"v".to_vec());
                let commit = futures_executor_block_on(backend.commit(txn)).unwrap();
                assert!(commit > 0);
                let mut probe = backend.begin();
                assert_eq!(
                    backend.get(&mut probe, &RecordKey::new("t", "d", "table-0", b"k")).unwrap(),
                    Some(b"v".to_vec())
                );
                assert_eq!(
                    backend.get(&mut probe, &RecordKey::new("t", "d", &table, b"k")).unwrap(),
                    Some(b"v".to_vec())
                );
                let _ = std::fs::remove_dir_all(&dir);
                return;
            }
        }
        panic!("expected two tables on different shards");
    }

    #[test]
    fn move_table_preserves_data() {
        let dir =
            std::env::temp_dir().join(format!("ryme-move-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        commit_table(&backend, "t", "d", "users", writes(vec![("1", "a"), ("2", "b")]));
        let before = backend.route(&TableRef::new("t", "d", "users"));
        let target = (before + 1) % 4;
        let report = backend.move_table("t", "d", "users", Some(target)).unwrap();
        assert_eq!(report.from, before);
        assert_eq!(report.to, target);
        assert_eq!(backend.route(&TableRef::new("t", "d", "users")), target);
        let mut txn = backend.begin();
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "users", b"1")).unwrap(),
            Some(b"a".to_vec())
        );
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "users", b"2")).unwrap(),
            Some(b"b".to_vec())
        );
        commit_table(&backend, "t", "d", "users", writes(vec![("3", "c")]));
        let mut txn = backend.begin();
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "users", b"3")).unwrap(),
            Some(b"c".to_vec())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn moved_placement_survives_restart() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-move-restart-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let target;
        {
            let backend = ShardSet::open_with_mode(
                &dir,
                4,
                SyncPolicy::Never,
                64 * 1024 * 1024,
                StorageMode::Standard,
            )
            .unwrap();
            commit_table(&backend, "t", "d", "users", writes(vec![("1", "a")]));
            let before = backend.route(&TableRef::new("t", "d", "users"));
            target = (before + 1) % 4;
            backend.move_table("t", "d", "users", Some(target)).unwrap();
            assert!(dir.join("placements.json").is_file());
        }

        let backend = ShardSet::open_with_mode(
            &dir,
            4,
            SyncPolicy::Never,
            64 * 1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        assert_eq!(backend.route(&TableRef::new("t", "d", "users")), target);
        let mut txn = backend.begin();
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "users", b"1")).unwrap(),
            Some(b"a".to_vec())
        );
        drop(backend);
        std::fs::remove_dir_all(dir.join("coordinator")).unwrap();

        let backend = ShardSet::open_with_mode(
            &dir,
            4,
            SyncPolicy::Never,
            64 * 1024 * 1024,
            StorageMode::Standard,
        )
        .unwrap();
        assert_eq!(backend.route(&TableRef::new("t", "d", "users")), target);
        let mut txn = backend.begin();
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "users", b"1")).unwrap(),
            Some(b"a".to_vec())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn distinct_tables(backend: &ShardSet) -> (String, String) {
        let mut first: Option<(String, usize)> = None;
        for index in 0..64 {
            let table = format!("tpc-{index}");
            let shard = backend.route(&TableRef::new("t", "d", &table));
            if let Some((other, other_shard)) = first.clone() {
                if other_shard != shard {
                    return (other, table);
                }
            } else {
                first = Some((table, shard));
            }
        }
        panic!("single shard layout");
    }

    #[test]
    fn two_phase_commit_spans_shards() {
        let dir =
            std::env::temp_dir().join(format!("ryme-2pc-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let (left, right) = distinct_tables(&backend);
        assert_ne!(
            backend.route(&TableRef::new("t", "d", &left)),
            backend.route(&TableRef::new("t", "d", &right))
        );
        let mut txn = backend.begin();
        backend.put(&mut txn, RecordKey::new("t", "d", &left, b"k"), b"v-left".to_vec());
        backend.put(&mut txn, RecordKey::new("t", "d", &right, b"k"), b"v-right".to_vec());
        let commit = futures_executor_block_on(backend.commit(txn)).unwrap();
        assert!(commit > 0);
        let mut probe = backend.begin();
        assert_eq!(
            backend.get(&mut probe, &RecordKey::new("t", "d", &left, b"k")).unwrap(),
            Some(b"v-left".to_vec())
        );
        assert_eq!(
            backend.get(&mut probe, &RecordKey::new("t", "d", &right, b"k")).unwrap(),
            Some(b"v-right".to_vec())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn two_phase_commit_uses_global_snapshot_timestamp() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-2pc-global-ts-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let mut by_shard = BTreeMap::new();
        for index in 0..128 {
            let table = format!("global-ts-{index}");
            by_shard.entry(backend.route(&TableRef::new("t", "d", &table))).or_insert(table);
            if by_shard.len() == 3 {
                break;
            }
        }
        assert_eq!(by_shard.len(), 3);
        let mut tables = by_shard.into_values();
        let high_table = tables.next().unwrap();
        let left_table = tables.next().unwrap();
        let right_table = tables.next().unwrap();

        commit_table(&backend, "t", "d", &high_table, writes(vec![("k", "high")]));
        let mut txn = backend.begin();
        backend.put(&mut txn, RecordKey::new("t", "d", &left_table, b"k"), b"left".to_vec());
        backend.put(&mut txn, RecordKey::new("t", "d", &right_table, b"k"), b"right".to_vec());
        futures_executor_block_on(backend.commit(txn)).unwrap();

        let mut probe = backend.begin();
        assert_eq!(
            backend.get(&mut probe, &RecordKey::new("t", "d", &left_table, b"k")).unwrap(),
            Some(b"left".to_vec())
        );
        assert_eq!(
            backend.get(&mut probe, &RecordKey::new("t", "d", &right_table, b"k")).unwrap(),
            Some(b"right".to_vec())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn two_phase_commit_aborts_atomically() {
        let dir =
            std::env::temp_dir().join(format!("ryme-2pca-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let (left, right) = distinct_tables(&backend);
        commit_table(&backend, "t", "d", &left, writes(vec![("k", "v1")]));
        let mut reader = backend.begin();
        assert_eq!(
            backend.get(&mut reader, &RecordKey::new("t", "d", &left, b"k")).unwrap(),
            Some(b"v1".to_vec())
        );
        let mut writer = backend.begin();
        backend.put(&mut writer, RecordKey::new("t", "d", &left, b"k"), b"v2".to_vec());
        futures_executor_block_on(backend.commit(writer)).unwrap();
        backend.put(&mut reader, RecordKey::new("t", "d", &right, b"k"), b"stale".to_vec());
        let conflicted = futures_executor_block_on(backend.commit(reader));
        assert!(matches!(conflicted, Err(ryme_error::RymeError::Conflict(_))));
        let mut probe = backend.begin();
        assert_eq!(backend.get(&mut probe, &RecordKey::new("t", "d", &right, b"k")).unwrap(), None);
        assert_eq!(
            backend.get(&mut probe, &RecordKey::new("t", "d", &left, b"k")).unwrap(),
            Some(b"v2".to_vec())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn single_shard_commit_preserves_read_snapshot_after_later_write() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-single-shard-conflict-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let key = RecordKey::new("t", "d", "users", b"k");
        commit_table(&backend, "t", "d", "users", writes(vec![("k", "v1")]));

        let mut reader = backend.begin();
        assert_eq!(backend.get(&mut reader, &key).unwrap(), Some(b"v1".to_vec()));

        let mut writer = backend.begin();
        backend.put(&mut writer, key.clone(), b"v2".to_vec());
        futures_executor_block_on(backend.commit(writer)).unwrap();

        backend.put(&mut reader, key.clone(), b"v3".to_vec());
        let result = futures_executor_block_on(backend.commit(reader));
        assert!(matches!(result, Err(RymeError::Conflict(_))));

        let mut probe = backend.begin();
        assert_eq!(backend.get(&mut probe, &key).unwrap(), Some(b"v2".to_vec()));
        drop(backend);

        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let mut recovered = backend.begin();
        assert_eq!(backend.get(&mut recovered, &key).unwrap(), Some(b"v2".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn gc_preserves_versions_for_live_sharded_snapshot() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-shard-gc-snapshot-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 1, SyncPolicy::Never).unwrap();
        let key = RecordKey::new("t", "d", "users", b"k");
        commit_table(&backend, "t", "d", "users", writes(vec![("k", "v1")]));

        let mut reader = backend.begin();
        let snapshot_ts = reader.read_ts;
        assert_eq!(backend.get(&mut reader, &key).unwrap(), Some(b"v1".to_vec()));

        commit_table(&backend, "t", "d", "users", writes(vec![("k", "v2")]));
        let gc_horizon = backend.gc_oldest().unwrap();
        assert!(gc_horizon <= snapshot_ts);
        assert_eq!(backend.get(&mut reader, &key).unwrap(), Some(b"v1".to_vec()));

        drop(reader);
        drop(backend);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn single_shard_commits_use_a_global_snapshot_timestamp() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-single-shard-global-ts-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let (first_table, second_table) = distinct_tables(&backend);
        let second_key = RecordKey::new("t", "d", &second_table, b"k");

        let mut first = backend.begin();
        let mut second = backend.begin();
        backend.put(&mut first, RecordKey::new("t", "d", &first_table, b"k"), b"first".to_vec());
        backend.put(&mut second, second_key.clone(), b"second".to_vec());
        let first_commit = futures_executor_block_on(backend.commit(first)).unwrap();

        let mut snapshot = backend.begin();
        let snapshot_ts = snapshot.read_ts;
        let second_commit = futures_executor_block_on(backend.commit(second)).unwrap();
        assert!(second_commit > first_commit);
        assert!(second_commit > snapshot_ts);
        assert_eq!(backend.get(&mut snapshot, &second_key).unwrap(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn queued_single_shard_commit_stays_after_newer_snapshot() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-queued-single-shard-ts-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let mut first = backend.begin();
        let mut queued = backend.begin();
        let first_key = RecordKey::new("t", "d", "users", b"first");
        let queued_key = RecordKey::new("t", "d", "users", b"queued");
        backend.put(&mut first, first_key, b"first".to_vec());
        backend.put(&mut queued, queued_key.clone(), b"queued".to_vec());
        let first_ts = futures_executor_block_on(backend.commit(first)).unwrap();

        let mut snapshot = backend.begin();
        let snapshot_ts = snapshot.read_ts;
        let queued_ts = futures_executor_block_on(backend.commit(queued)).unwrap();
        assert!(queued_ts > first_ts);
        assert!(queued_ts > snapshot_ts);
        assert_eq!(backend.get(&mut snapshot, &queued_key).unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn independent_shard_commits_get_distinct_global_timestamps() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-parallel-shard-commits-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let (left_table, right_table) = distinct_tables(&backend);
        let left_key = RecordKey::new("t", "d", &left_table, b"k");
        let right_key = RecordKey::new("t", "d", &right_table, b"k");
        let mut left = backend.begin();
        let mut right = backend.begin();
        backend.put(&mut left, left_key.clone(), b"left".to_vec());
        backend.put(&mut right, right_key.clone(), b"right".to_vec());

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let left_backend = backend.clone();
        let left_barrier = barrier.clone();
        let left_commit = std::thread::spawn(move || {
            left_barrier.wait();
            futures_executor_block_on(left_backend.commit(left)).unwrap()
        });
        let right_backend = backend.clone();
        let right_barrier = barrier.clone();
        let right_commit = std::thread::spawn(move || {
            right_barrier.wait();
            futures_executor_block_on(right_backend.commit(right)).unwrap()
        });
        barrier.wait();
        let left_ts = left_commit.join().unwrap();
        let right_ts = right_commit.join().unwrap();
        assert_ne!(left_ts, right_ts);

        let mut probe = backend.begin();
        assert_eq!(backend.get(&mut probe, &left_key).unwrap(), Some(b"left".to_vec()));
        assert_eq!(backend.get(&mut probe, &right_key).unwrap(), Some(b"right".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_durable_commits_share_one_segment_and_recover_without_wal() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-group-commit-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut backend = ShardSet::open(&dir, 4, SyncPolicy::Always).unwrap();
        backend.group_commit_delay = Duration::from_millis(50);
        let table = "group_commit_rows";
        let shard = backend.route(&TableRef::new("t", "d", table));
        let commits = 8;
        let barrier = Arc::new(std::sync::Barrier::new(commits + 1));
        let mut workers = Vec::with_capacity(commits);
        let mut keys = Vec::with_capacity(commits);
        for index in 0..commits {
            let key = RecordKey::new("t", "d", table, &(index as u64).to_be_bytes());
            let mut txn = backend.begin();
            backend.put(&mut txn, key.clone(), format!("value-{index}").into_bytes());
            keys.push(key);
            let worker_backend = backend.clone();
            let worker_barrier = barrier.clone();
            workers.push(std::thread::spawn(move || {
                worker_barrier.wait();
                futures_executor_block_on(worker_backend.commit(txn))
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().unwrap().unwrap();
        }
        let segment_dir = backend.shard_manager(shard).unwrap().segment_dir();
        assert_eq!(
            ryme_storage::SegmentStore::open(&segment_dir).unwrap().segment_paths().unwrap().len(),
            1
        );
        drop(backend);

        let wal_dir = dir.join("shards").join(shard.to_string()).join("wal");
        for entry in std::fs::read_dir(&wal_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("wal") {
                std::fs::remove_file(path).unwrap();
            }
        }
        let reopened = ShardSet::open(&dir, 4, SyncPolicy::Always).unwrap();
        for (index, key) in keys.iter().enumerate() {
            let mut probe = reopened.begin();
            assert_eq!(
                reopened.get(&mut probe, key).unwrap(),
                Some(format!("value-{index}").into_bytes())
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn two_phase_recovery_preserves_cohort() {
        let dir =
            std::env::temp_dir().join(format!("ryme-2pcr-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let (left, right) = {
            let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
            let tables = distinct_tables(&backend);
            let mut txn = backend.begin();
            backend.put(&mut txn, RecordKey::new("t", "d", &tables.0, b"k"), b"a".to_vec());
            backend.put(&mut txn, RecordKey::new("t", "d", &tables.1, b"k"), b"b".to_vec());
            futures_executor_block_on(backend.commit(txn)).unwrap();
            tables
        };
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let mut probe = backend.begin();
        assert_eq!(
            backend.get(&mut probe, &RecordKey::new("t", "d", &left, b"k")).unwrap(),
            Some(b"a".to_vec())
        );
        assert_eq!(
            backend.get(&mut probe, &RecordKey::new("t", "d", &right, b"k")).unwrap(),
            Some(b"b".to_vec())
        );
        for index in 0..backend.shard_count() {
            backend.shard_manager(index).unwrap().write_snapshot().unwrap();
        }
        backend.retention_sweep(1).unwrap();
        drop(backend);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let mut probe = backend.begin();
        assert_eq!(
            backend.get(&mut probe, &RecordKey::new("t", "d", &left, b"k")).unwrap(),
            Some(b"a".to_vec())
        );
        assert_eq!(
            backend.get(&mut probe, &RecordKey::new("t", "d", &right, b"k")).unwrap(),
            Some(b"b".to_vec())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restore_to_rewinds_every_local_shard() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-restore-shards-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = ShardSet::open(&dir, 4, SyncPolicy::Never).unwrap();
        let first = commit_table(&backend, "t", "d", "docs", writes(vec![("before", "one")]));
        for index in 0..backend.shard_count() {
            backend.shard_manager(index).unwrap().write_snapshot().unwrap();
        }
        commit_table(&backend, "t", "d", "docs", writes(vec![("after", "two")]));

        let replayed = backend.restore_to(first).unwrap();
        assert_eq!(replayed.len(), backend.shard_count());
        let mut txn = backend.begin();
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "docs", b"before")).unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "docs", b"after")).unwrap(),
            None
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn decision_codec_roundtrip() {
        let parts = vec![(0u32, b"one".to_vec()), (3u32, b"two".to_vec())];
        let (commit_ts, back) = decode_decision(&encode_decision(41, &parts)).unwrap();
        assert_eq!(commit_ts, 41);
        assert_eq!(back, parts);
        assert!(decode_decision(b"bogus").is_err());

        let mut impossible_count = vec![1u8];
        impossible_count.extend_from_slice(&41u64.to_le_bytes());
        impossible_count.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_decision(&impossible_count).is_err());

        let mut trailing = encode_decision(41, &[]);
        trailing.push(0);
        assert!(decode_decision(&trailing).is_err());
    }

    fn now_ms() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    }

    fn block_on<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        let waker = noop_waker();
        let mut boxed = Box::pin(future);
        let mut context = std::task::Context::from_waker(&waker);
        loop {
            match boxed.as_mut().poll(&mut context) {
                std::task::Poll::Ready(value) => return value,
                std::task::Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[tokio::test]
    async fn hybrid_routes_tiers() {
        let dir =
            std::env::temp_dir().join(format!("ryme-hybrid-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let raft_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node =
            ryme_raft::net::Node::open(0, Vec::new(), Vec::new(), &dir.join("raft")).unwrap();
        let tasks = node.spawn(raft_listener);
        wait_raft_leader(&node).await;
        let local = ShardSet::open(&dir.join("local"), 2, SyncPolicy::Never).unwrap();
        let mut replicated = HashSet::new();
        replicated.insert(TableRef::new("t", "d", "sys"));
        let backend =
            HybridBackend::new(ryme_raft::net::ClusterBackend::new(node), local, replicated);
        assert_eq!(backend.tier(&TableRef::new("t", "d", "sys")), Tier::Replicated);
        assert_eq!(backend.tier(&TableRef::new("t", "d", "cache")), Tier::Local);
        let mut txn = backend.begin();
        backend.put(&mut txn, RecordKey::new("t", "d", "sys", b"k"), b"v".to_vec());
        backend.put(&mut txn, RecordKey::new("t", "d", "cache", b"k"), b"v".to_vec());
        block_on(backend.commit(txn)).unwrap_err();
        let mut txn = backend.begin();
        backend.put(&mut txn, RecordKey::new("t", "d", "sys", b"k"), b"v".to_vec());
        block_on(backend.commit(txn)).unwrap();
        let mut txn = backend.begin();
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "sys", b"k")).unwrap(),
            Some(b"v".to_vec())
        );
        let mut txn = backend.begin();
        backend.put(&mut txn, RecordKey::new("t", "d", "cache", b"k"), b"c".to_vec());
        block_on(backend.commit(txn)).unwrap();
        let mut txn = backend.begin();
        assert_eq!(
            backend.get(&mut txn, &RecordKey::new("t", "d", "cache", b"k")).unwrap(),
            Some(b"c".to_vec())
        );
        let mut cross_tier = backend.begin();
        assert_eq!(
            backend.get(&mut cross_tier, &RecordKey::new("t", "d", "sys", b"k")).unwrap(),
            Some(b"v".to_vec())
        );
        backend.put(
            &mut cross_tier,
            RecordKey::new("t", "d", "cache", b"dependent"),
            b"value".to_vec(),
        );
        assert!(matches!(
            block_on(backend.commit(cross_tier)),
            Err(RymeError::InvalidArgument(message)) if message == "cross-tier"
        ));
        for task in tasks {
            task.abort();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn hybrid_gc_preserves_live_replicated_snapshot() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-hybrid-gc-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let raft_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node =
            ryme_raft::net::Node::open(0, Vec::new(), Vec::new(), &dir.join("raft")).unwrap();
        let tasks = node.spawn(raft_listener);
        wait_raft_leader(&node).await;
        let local = ShardSet::open(&dir.join("local"), 2, SyncPolicy::Never).unwrap();
        let table = TableRef::new("t", "d", "replicated");
        let backend = HybridBackend::new(
            ryme_raft::net::ClusterBackend::new(node.clone()),
            local,
            HashSet::from([table]),
        );
        let key = RecordKey::new("t", "d", "replicated", b"k");

        let mut first = backend.begin();
        backend.put(&mut first, key.clone(), b"v1".to_vec());
        block_on(backend.commit(first)).unwrap();

        let mut reader = backend.begin();
        assert_eq!(backend.get(&mut reader, &key).unwrap(), Some(b"v1".to_vec()));
        let mut update = backend.begin();
        backend.put(&mut update, key.clone(), b"v2".to_vec());
        block_on(backend.commit(update)).unwrap();

        backend.gc_oldest().unwrap();
        assert_eq!(backend.get(&mut reader, &key).unwrap(), Some(b"v1".to_vec()));

        drop(reader);
        for task in tasks {
            task.abort();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn hybrid_snapshot_survives_independent_tier_clock_skew() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-hybrid-clock-skew-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let raft_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node =
            ryme_raft::net::Node::open(0, Vec::new(), Vec::new(), &dir.join("raft")).unwrap();
        let tasks = node.spawn(raft_listener);
        wait_raft_leader(&node).await;
        let local = ShardSet::open(&dir.join("local"), 2, SyncPolicy::Never).unwrap();
        let backend = HybridBackend::new(
            ryme_raft::net::ClusterBackend::new(node),
            local,
            HashSet::from([TableRef::new("t", "d", "replicated")]),
        );

        for index in 0..8 {
            let key = RecordKey::new("t", "d", "replicated", index.to_string().as_bytes());
            let mut txn = backend.begin();
            backend.put(&mut txn, key, b"replicated".to_vec());
            block_on(backend.commit(txn)).unwrap();
        }

        let local_key = RecordKey::new("t", "d", "cache", b"k");
        let mut reader = backend.begin();
        let snapshot_ts = reader.read_ts;
        assert_eq!(backend.get(&mut reader, &local_key).unwrap(), None);
        let mut writer = backend.begin();
        backend.put(&mut writer, local_key.clone(), b"after-snapshot".to_vec());
        let commit_ts = block_on(backend.commit(writer)).unwrap();
        assert!(commit_ts > snapshot_ts);
        assert_eq!(backend.get(&mut reader, &local_key).unwrap(), None);

        drop(reader);
        for task in tasks {
            task.abort();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    async fn wait_raft_leader(node: &std::sync::Arc<ryme_raft::net::Node>) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if node.is_leader().await {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "no leader");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
}
