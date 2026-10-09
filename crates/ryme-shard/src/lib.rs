use ryme_error::{Result, RymeError};
use ryme_storage::{RecordKey, SegmentCacheStats, StorageMode};
use ryme_txn::{
    decode_writes, encode_writes, DurableManager, SyncPolicy, Transaction, TxnBackend, TxnManager,
};
use ryme_wal::Wal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

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

#[derive(Debug)]
struct ShardCtx {
    shard: Option<usize>,
    read_ts: u64,
    tables: Vec<TableRef>,
    touched: Instant,
}

#[derive(Debug)]
struct ShardInner {
    base_dir: std::path::PathBuf,
    placement_path: std::path::PathBuf,
    shards: Vec<DurableManager>,
    place: HashMap<TableRef, usize>,
    paused: HashSet<TableRef>,
    side: HashMap<u64, ShardCtx>,
    coordinator: Mutex<Wal>,
}

#[derive(Debug, Clone)]
pub struct ShardSet {
    inner: Arc<Mutex<ShardInner>>,
    commit: Arc<RwLock<()>>,
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

fn decode_decision(raw: &[u8]) -> Result<(u64, DecisionParts)> {
    let corrupt = || RymeError::Corrupt(String::from("decision"));
    if raw.first() != Some(&1u8) || raw.len() < 13 {
        return Err(corrupt());
    }
    let commit_ts = u64::from_le_bytes(raw[1..9].try_into().map_err(|_| corrupt())?);
    let count = u32::from_le_bytes(raw[9..13].try_into().map_err(|_| corrupt())?) as usize;
    let mut parts = Vec::with_capacity(count);
    let mut offset = 13;
    for _ in 0..count {
        if offset + 8 > raw.len() {
            return Err(corrupt());
        }
        let shard = u32::from_le_bytes(raw[offset..offset + 4].try_into().map_err(|_| corrupt())?);
        let len = u32::from_le_bytes(raw[offset + 4..offset + 8].try_into().map_err(|_| corrupt())?)
            as usize;
        offset += 8;
        if offset + len > raw.len() {
            return Err(corrupt());
        }
        parts.push((shard, raw[offset..offset + len].to_vec()));
        offset += len;
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
        let mut managers = Vec::new();
        for index in 0..shards {
            let dir = data_dir.join("shards").join(index.to_string()).join("wal");
            managers.push(DurableManager::open_with_mode(
                &dir,
                64 * 1024 * 1024,
                policy,
                cache_bytes,
                mode,
            )?);
        }
        let coordinator = Wal::open(&data_dir.join("coordinator"), 1024 * 1024)?;
        let placement_path = data_dir.join("placements.json");
        let place = load_placements(&placement_path, shards)?;
        let set = Self {
            inner: Arc::new(Mutex::new(ShardInner {
                base_dir: data_dir.to_path_buf(),
                placement_path,
                shards: managers,
                place,
                paused: HashSet::new(),
                side: HashMap::new(),
                coordinator: Mutex::new(coordinator),
            })),
            commit: Arc::new(RwLock::new(())),
        };
        set.recover_coordinator()?;
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

    pub fn shard_manager(&self, shard: usize) -> Option<DurableManager> {
        self.inner.lock().ok()?.shards.get(shard).cloned()
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

    fn touch(&self, txn: &Transaction, table: TableRef) -> usize {
        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.side.len() > 4096 {
                inner.side.retain(|_, ctx| ctx.touched.elapsed().as_secs() < 120);
            }
        }
        let (shard, stamp) = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let fallback = hash_table(&table, inner.shards.len());
            let shard = inner.place.get(&table).copied().unwrap_or(fallback);
            (shard, inner.shards[shard].inner().latest_commit())
        };
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = inner.side.entry(txn.id).or_insert_with(|| ShardCtx {
            shard: None,
            read_ts: 0,
            tables: Vec::new(),
            touched: Instant::now(),
        });
        entry.touched = Instant::now();
        if !entry.tables.contains(&table) {
            entry.tables.push(table);
        }
        if entry.shard.is_none() {
            entry.shard = Some(shard);
            entry.read_ts = stamp;
        }
        entry.shard.unwrap_or(shard)
    }

    fn manager_for(&self, shard: usize) -> Option<DurableManager> {
        self.inner.lock().ok()?.shards.get(shard).cloned()
    }

    fn commit_single(&self, txn: Transaction, ctx: ShardCtx, shard: usize) -> Result<u64> {
        let inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        if Some(shard) != ctx.shard {
            return Err(RymeError::Unavailable(String::from("rescheduling")));
        }
        for table in ctx.tables.iter() {
            if inner.paused.contains(table) {
                return Err(RymeError::Unavailable(String::from("rescheduling")));
            }
            let placed = inner
                .place
                .get(table)
                .copied()
                .unwrap_or_else(|| hash_table(table, inner.shards.len()));
            if placed != shard {
                return Err(RymeError::Unavailable(String::from("rescheduling")));
            }
        }
        let _read =
            self.commit.read().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let manager = inner
            .shards
            .get(shard)
            .cloned()
            .ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        let mut txn = txn;
        txn.restamp(ctx.read_ts);
        manager.commit(txn)
    }

    fn commit_two_phase(&self, txn: Transaction) -> Result<u64> {
        let inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let _write =
            self.commit.write().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let shards = inner.shards.len();
        let mut cohort: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
        for key in txn.writes().keys().chain(txn.read_keys().iter()) {
            let table = TableRef::of(key);
            if inner.paused.contains(&table) {
                return Err(RymeError::Unavailable(String::from("rescheduling")));
            }
            let shard =
                inner.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards));
            cohort.insert(shard);
        }
        for table in txn.scanned_tables().iter() {
            let table = TableRef::new(&table.0, &table.1, &table.2);
            if inner.paused.contains(&table) {
                return Err(RymeError::Unavailable(String::from("rescheduling")));
            }
            let shard =
                inner.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards));
            cohort.insert(shard);
        }
        if cohort.len() < 2 {
            let shard = cohort.iter().next().copied().unwrap_or(0);
            let ctx = ShardCtx {
                shard: Some(shard),
                read_ts: txn.read_ts,
                tables: Vec::new(),
                touched: Instant::now(),
            };
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
                inner.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards))
            })
            .collect();
        let commit_ts =
            managers.iter().map(|(_, manager)| manager.inner().latest_commit()).max().unwrap_or(0)
                + 1;
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
                    inner.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards))
                        == *shard
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
                inner.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards))
                    == index
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
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
        let _write =
            self.commit.write().map_err(|_| RymeError::Internal(String::from("shard lock")))?;
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
}

impl TxnBackend for ShardSet {
    fn begin(&self) -> Transaction {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let read_ts =
            inner.shards.iter().map(|shard| shard.inner().latest_commit()).max().unwrap_or(1);
        drop(inner);
        let mut txn = TxnManager::new().begin();
        txn.restamp(read_ts);
        txn
    }

    fn get(&self, txn: &mut Transaction, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        let shard = self.route(&TableRef::of(key));
        let manager =
            self.manager_for(shard).ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        manager.get(txn, key)
    }

    fn put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) {
        let shard = self.touch(txn, TableRef::of(&key));
        if let Some(manager) = self.manager_for(shard) {
            manager.put(txn, key, value);
        }
    }

    fn put_with_ttl(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>, expires_at: u64) {
        let shard = self.touch(txn, TableRef::of(&key));
        if let Some(manager) = self.manager_for(shard) {
            manager.put_with_ttl(txn, key, value, expires_at);
        }
    }

    fn delete(&self, txn: &mut Transaction, key: RecordKey) {
        let shard = self.touch(txn, TableRef::of(&key));
        if let Some(manager) = self.manager_for(shard) {
            manager.delete(txn, key);
        }
    }

    fn expires_at(&self, key: &RecordKey) -> Result<Option<u64>> {
        let shard = self.route(&TableRef::of(key));
        let manager =
            self.manager_for(shard).ok_or_else(|| RymeError::Unavailable(String::from("shard")))?;
        manager.expires_at(key)
    }

    fn commit(&self, txn: Transaction) -> impl std::future::Future<Output = Result<u64>> + Send {
        let backend = self.clone();
        async move {
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
                let shard =
                    guard.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards));
                cohort.insert(shard);
            }
            for table in txn.scanned_tables().iter() {
                let table = TableRef::new(&table.0, &table.1, &table.2);
                let shard =
                    guard.place.get(&table).copied().unwrap_or_else(|| hash_table(&table, shards));
                cohort.insert(shard);
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
struct HybridCtx {
    tables: Vec<(TableRef, Tier)>,
    stamp_raft: Option<u64>,
    stamp_local: Option<u64>,
    poisoned: bool,
    touched: Instant,
}

#[derive(Debug)]
struct HybridInner {
    raft: ryme_raft::net::ClusterBackend,
    local: ShardSet,
    replicated: HashSet<TableRef>,
    side: HashMap<u64, HybridCtx>,
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
        Self {
            inner: Arc::new(Mutex::new(HybridInner {
                raft,
                local,
                replicated,
                side: HashMap::new(),
            })),
        }
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

    fn touch(&self, txn: &Transaction, table: TableRef) -> Tier {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.side.len() > 4096 {
            inner.side.retain(|_, ctx| ctx.touched.elapsed().as_secs() < 120);
        }
        let tier = if inner.replicated.contains(&table) { Tier::Replicated } else { Tier::Local };
        let stamp = match tier {
            Tier::Replicated => inner.raft.node().manager().latest_commit(),
            Tier::Local => inner.local.latest_commit(),
        };
        let entry = inner.side.entry(txn.id).or_insert_with(|| HybridCtx {
            tables: Vec::new(),
            stamp_raft: None,
            stamp_local: None,
            poisoned: false,
            touched: Instant::now(),
        });
        entry.touched = Instant::now();
        if !entry.tables.iter().any(|(existing, _)| existing == &table) {
            entry.tables.push((table, tier));
        }
        match tier {
            Tier::Replicated => {
                entry.stamp_raft.get_or_insert(stamp);
            }
            Tier::Local => {
                entry.stamp_local.get_or_insert(stamp);
            }
        }
        if entry.tables.iter().any(|(_, other)| *other != tier) {
            entry.poisoned = true;
        }
        tier
    }
}

impl TxnBackend for HybridBackend {
    fn begin(&self) -> Transaction {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let read_ts =
            inner.raft.node().manager().latest_commit().max(inner.local.latest_commit()).max(1);
        drop(inner);
        let mut txn = TxnManager::new().begin();
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
        let tier = self.touch(txn, TableRef::of(&key));
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match tier {
            Tier::Replicated => inner.raft.put(txn, key, value),
            Tier::Local => inner.local.put(txn, key, value),
        }
    }

    fn put_with_ttl(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>, expires_at: u64) {
        let tier = self.touch(txn, TableRef::of(&key));
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match tier {
            Tier::Replicated => inner.raft.put_with_ttl(txn, key, value, expires_at),
            Tier::Local => inner.local.put_with_ttl(txn, key, value, expires_at),
        }
    }

    fn delete(&self, txn: &mut Transaction, key: RecordKey) {
        let tier = self.touch(txn, TableRef::of(&key));
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match tier {
            Tier::Replicated => inner.raft.delete(txn, key),
            Tier::Local => inner.local.delete(txn, key),
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

    fn commit(&self, txn: Transaction) -> impl std::future::Future<Output = Result<u64>> + Send {
        let backend = self.clone();
        async move {
            let ctx = backend
                .inner
                .lock()
                .map_err(|_| RymeError::Internal(String::from("shard lock")))?
                .side
                .remove(&txn.id);
            let Some(ctx) = ctx else {
                return Ok(txn.read_ts);
            };
            if ctx.poisoned {
                return Err(RymeError::InvalidArgument(String::from("cross-tier")));
            }
            let replicated = ctx.tables.iter().any(|(_, tier)| *tier == Tier::Replicated);
            let local = ctx.tables.iter().any(|(_, tier)| *tier == Tier::Local);
            if replicated && local {
                return Err(RymeError::InvalidArgument(String::from("cross-tier")));
            }
            let mut txn = txn;
            if replicated {
                if let Some(stamp) = ctx.stamp_raft {
                    txn.restamp(stamp.min(txn.read_ts));
                }
                let raft = backend.inner.lock().unwrap_or_else(|e| e.into_inner()).raft.clone();
                <ryme_raft::net::ClusterBackend as TxnBackend>::commit(&raft, txn).await
            } else {
                if let Some(stamp) = ctx.stamp_local {
                    txn.restamp(stamp.min(txn.read_ts));
                }
                let local = backend.inner.lock().unwrap_or_else(|e| e.into_inner()).local.clone();
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
    fn decision_codec_roundtrip() {
        let parts = vec![(0u32, b"one".to_vec()), (3u32, b"two".to_vec())];
        let (commit_ts, back) = decode_decision(&encode_decision(41, &parts)).unwrap();
        assert_eq!(commit_ts, 41);
        assert_eq!(back, parts);
        assert!(decode_decision(b"bogus").is_err());
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
