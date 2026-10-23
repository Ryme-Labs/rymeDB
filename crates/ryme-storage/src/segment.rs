use super::{Engine, RecordKey};
use ryme_error::{Result, RymeError};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock};

const SEGMENT_MAGIC: u32 = 0x5259_5347;
const SEGMENT_VERSION: u16 = 2;
const DELTA_COMPACTION_MAGIC: u32 = 0x5259_4443;
const DELTA_COMPACTION_VERSION: u16 = 1;
const INDEX_STRIDE: usize = 64;
const BLOOM_BITS_PER_KEY: usize = 8;
const MIN_BLOOM_BYTES: usize = 64;
const MIN_SEGMENT_DEDUP_BATCH_READ: usize = 16;
const MAX_DELTA_COMPACTION_INPUT_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_SEGMENT_CACHE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SEGMENT_BYTES: usize = 256 * 1024 * 1024;
const MAX_SEGMENT_POINTER_BYTES: usize = 64;
const DELTA_COMPACTION_MARKER_BYTES: usize = 4 + 2 + 8 + 8 + 8 + 4;
static NEXT_SEGMENT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

fn configure_private_file(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = options;
}

fn write_temp_file(dir: &Path, name: &str, bytes: &[u8], sync: bool) -> Result<PathBuf> {
    use std::io::Write;

    let (temporary, mut file) = loop {
        let id = NEXT_SEGMENT_TEMP_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let path = dir.join(format!("{name}.{}.{}.tmp", std::process::id(), id));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        configure_private_file(&mut options);
        match options.open(&path) {
            Ok(file) => break (path, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    };
    let result = file.write_all(bytes).and_then(|()| {
        if sync {
            file.sync_data()
        } else {
            Ok(())
        }
    });
    drop(file);
    if let Err(error) = result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    Ok(temporary)
}

fn install_temp_file(temporary: &Path, destination: &Path) -> Result<()> {
    if let Err(error) = std::fs::rename(temporary, destination) {
        let _ = std::fs::remove_file(temporary);
        return Err(error.into());
    }
    Ok(())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentKind {
    Snapshot = 0,
    Delta = 1,
    CompactedDelta = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeltaCompactionMarker {
    snapshot_id: u64,
    through_id: u64,
    output_id: u64,
}

#[derive(Debug, Clone)]
pub struct SegmentEntry {
    pub key: RecordKey,
    pub commit_ts: u64,
    pub value: Option<Vec<u8>>,
    pub expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub id: u64,
    pub max_commit_ts: u64,
    pub entries: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentCacheStats {
    pub limit_bytes: u64,
    pub used_bytes: u64,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
}

type VersionLookup = (u64, Option<Vec<u8>>, u64);

#[derive(Debug)]
struct CachedSegment {
    segment: Arc<ImmutableSegment>,
    bytes: u64,
    generation: u64,
}

#[derive(Debug)]
struct SegmentReadCache {
    limit_bytes: u64,
    used_bytes: u64,
    entries: HashMap<u64, CachedSegment>,
    order: VecDeque<(u64, u64)>,
    next_generation: u64,
    hits: u64,
    misses: u64,
}

impl SegmentReadCache {
    fn new(limit_bytes: u64) -> Self {
        Self {
            limit_bytes,
            used_bytes: 0,
            entries: HashMap::new(),
            order: VecDeque::new(),
            next_generation: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, id: u64) -> Option<Arc<ImmutableSegment>> {
        let segment = self.get_cached(id);
        if segment.is_none() {
            self.misses += 1;
        }
        segment
    }

    fn get_cached(&mut self, id: u64) -> Option<Arc<ImmutableSegment>> {
        let segment = self.entries.get(&id).map(|entry| Arc::clone(&entry.segment));
        if segment.is_some() {
            self.hits += 1;
            self.touch(id);
        }
        segment
    }

    fn get_many_cached(&mut self, ids: &[u64]) -> Vec<Option<Arc<ImmutableSegment>>> {
        let mut segments = Vec::with_capacity(ids.len());
        for id in ids {
            let segment = self.entries.get(id).map(|entry| Arc::clone(&entry.segment));
            if segment.is_some() {
                self.hits = self.hits.saturating_add(1);
                self.touch(*id);
            }
            segments.push(segment);
        }
        segments
    }

    fn insert(&mut self, id: u64, segment: Arc<ImmutableSegment>, bytes: u64) {
        if self.limit_bytes == 0 || bytes > self.limit_bytes {
            return;
        }
        self.remove(id);
        while self.used_bytes > self.limit_bytes.saturating_sub(bytes) {
            let Some((oldest, generation)) = self.order.pop_front() else {
                // Every live entry has one current queue entry. Refuse the
                // insertion rather than violating the byte-budget invariant
                // if that invariant is ever corrupted.
                debug_assert!(self.entries.is_empty(), "segment cache order invariant");
                return;
            };
            let current =
                self.entries.get(&oldest).is_some_and(|entry| entry.generation == generation);
            if !current {
                continue;
            }
            if let Some(entry) = self.entries.remove(&oldest) {
                self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
            }
        }
        let generation = self.allocate_generation();
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.entries.insert(id, CachedSegment { segment, bytes, generation });
        self.order.push_back((id, generation));
        self.compact_order_if_needed();
    }

    fn remove(&mut self, id: u64) {
        if let Some(entry) = self.entries.remove(&id) {
            self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
        }
        self.compact_order_if_needed();
    }

    fn touch(&mut self, id: u64) {
        let generation = self.allocate_generation();
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.generation = generation;
            self.order.push_back((id, generation));
            self.compact_order_if_needed();
        }
    }

    fn allocate_generation(&mut self) -> u64 {
        if self.next_generation == u64::MAX {
            self.compact_order();
            self.next_generation = 0;
            let live_ids: Vec<u64> = self.order.iter().map(|(id, _)| *id).collect();
            self.order.clear();
            for id in live_ids {
                self.next_generation += 1;
                let generation = self.next_generation;
                if let Some(entry) = self.entries.get_mut(&id) {
                    entry.generation = generation;
                }
                self.order.push_back((id, generation));
            }
        }
        self.next_generation += 1;
        self.next_generation
    }

    fn compact_order_if_needed(&mut self) {
        let threshold = self.entries.len().saturating_mul(2).saturating_add(32);
        if self.order.len() > threshold {
            self.compact_order();
        }
    }

    fn compact_order(&mut self) {
        let entries = &self.entries;
        self.order.retain(|(id, generation)| {
            entries.get(id).is_some_and(|entry| entry.generation == *generation)
        });
    }

    fn stats(&self) -> SegmentCacheStats {
        SegmentCacheStats {
            limit_bytes: self.limit_bytes,
            used_bytes: self.used_bytes,
            entries: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ImmutableSegment {
    meta: SegmentMeta,
    kind: SegmentKind,
    engine: Engine,
    keys: Vec<RecordKey>,
    sparse_index: Vec<(RecordKey, usize)>,
    bloom: Vec<u8>,
}

impl ImmutableSegment {
    pub fn from_engine(id: u64, engine: &Engine) -> Self {
        Self::from_engine_owned(id, engine.clone(), SegmentKind::Snapshot)
    }

    fn from_engine_owned(id: u64, engine: Engine, kind: SegmentKind) -> Self {
        let keys = engine.record_keys();
        let sparse_index = keys
            .iter()
            .enumerate()
            .filter(|(index, _)| index % INDEX_STRIDE == 0)
            .map(|(index, key)| (key.clone(), index))
            .collect();
        let bloom = build_bloom(&keys);
        let meta = SegmentMeta {
            id,
            max_commit_ts: engine.max_commit_ts(),
            entries: keys.len() as u64,
            bytes: engine.bytes_held(),
        };
        Self { meta, kind, engine, keys, sparse_index, bloom }
    }

    pub fn from_entries(id: u64, entries: &[SegmentEntry]) -> Result<Self> {
        Self::from_entries_owned(id, entries.to_vec())
    }

    pub fn from_entries_owned(id: u64, mut ordered: Vec<SegmentEntry>) -> Result<Self> {
        if !ordered.windows(2).all(|pair| {
            pair[0].key < pair[1].key
                || (pair[0].key == pair[1].key && pair[0].commit_ts <= pair[1].commit_ts)
        }) {
            ordered.sort_by(|left, right| {
                left.key.cmp(&right.key).then(left.commit_ts.cmp(&right.commit_ts))
            });
        }
        let mut engine = Engine::new();
        for entry in ordered {
            engine.apply_with_expiry(entry.key, entry.commit_ts, entry.value, entry.expires_at)?;
        }
        Ok(Self::from_engine_owned(id, engine, SegmentKind::Delta))
    }

    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    pub fn is_snapshot(&self) -> bool {
        self.kind == SegmentKind::Snapshot
    }

    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    pub fn sparse_index_len(&self) -> usize {
        self.sparse_index.len()
    }

    pub fn bloom_might_contain(&self, key: &RecordKey) -> bool {
        bloom_might_contain(&self.bloom, key)
    }

    pub fn get(&self, key: &RecordKey, read_ts: u64, now: u64) -> Result<Option<Vec<u8>>> {
        if !self.bloom_might_contain(key) {
            return Ok(None);
        }
        self.engine.read(key, read_ts, now)
    }

    pub fn version_at(&self, key: &RecordKey, read_ts: u64) -> Option<(u64, Option<Vec<u8>>, u64)> {
        if !self.bloom_might_contain(key) {
            return None;
        }
        self.engine.version_at(key, read_ts)
    }

    pub fn scan(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        read_ts: u64,
        now: u64,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.engine.scan(tenant, database, table, read_ts, now, limit)
    }

    pub fn into_engine(self) -> Engine {
        self.engine
    }

    pub fn merge_into(&self, target: &mut Engine) -> Result<()> {
        for (key, versions) in &self.engine.inner {
            for version in versions {
                target.apply_with_expiry(
                    key.clone(),
                    version.commit_ts,
                    version.value.clone(),
                    version.expires_at,
                )?;
            }
        }
        Ok(())
    }

    fn merge_owned_into(self, target: &mut Engine) -> Result<()> {
        target.merge_owned(self.engine)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let payload = self.engine.encode_snapshot()?;
        let mut out = Vec::new();
        push_segment_bytes(&mut out, &SEGMENT_MAGIC.to_be_bytes())?;
        push_segment_bytes(&mut out, &SEGMENT_VERSION.to_be_bytes())?;
        push_segment_bytes(&mut out, &(self.kind as u16).to_be_bytes())?;
        push_segment_bytes(&mut out, &self.meta.id.to_be_bytes())?;
        push_segment_bytes(&mut out, &self.meta.max_commit_ts.to_be_bytes())?;
        push_segment_bytes(
            &mut out,
            &(u32::try_from(payload.len())
                .map_err(|_| RymeError::InvalidArgument(String::from("segment size")))?)
            .to_be_bytes(),
        )?;
        push_segment_bytes(&mut out, &payload)?;
        push_segment_bytes(&mut out, &(self.bloom.len() as u32).to_be_bytes())?;
        push_segment_bytes(&mut out, &self.bloom)?;
        push_segment_bytes(&mut out, &(self.sparse_index.len() as u32).to_be_bytes())?;
        for (key, ordinal) in &self.sparse_index {
            push_key(&mut out, key)?;
            push_segment_bytes(
                &mut out,
                &u32::try_from(*ordinal)
                    .map_err(|_| RymeError::InvalidArgument(String::from("segment index")))?
                    .to_be_bytes(),
            )?;
        }
        let checksum = crc32fast::hash(&out);
        push_segment_bytes(&mut out, &checksum.to_be_bytes())?;
        Ok(out)
    }

    pub fn decode(input: &[u8]) -> Result<Self> {
        if input.len() > MAX_SEGMENT_BYTES {
            return Err(RymeError::Overload(String::from("segment")));
        }
        const V1_HEADER: usize = 4 + 2 + 2 + 8 + 4;
        if input.len() < V1_HEADER + 4 {
            return Err(RymeError::Corrupt(String::from("segment header")));
        }
        let stored = u32::from_be_bytes(
            input[input.len() - 4..]
                .try_into()
                .map_err(|_| RymeError::Corrupt(String::from("segment checksum")))?,
        );
        if crc32fast::hash(&input[..input.len() - 4]) != stored {
            return Err(RymeError::Corrupt(String::from("segment checksum")));
        }
        let mut cursor = input;
        let magic = take_u32(&mut cursor)?;
        let version = take_u16(&mut cursor)?;
        let kind = match take_u16(&mut cursor)? {
            0 => SegmentKind::Snapshot,
            1 => SegmentKind::Delta,
            2 => SegmentKind::CompactedDelta,
            _ => return Err(RymeError::Corrupt(String::from("segment kind"))),
        };
        let id = take_u64(&mut cursor)?;
        if magic != SEGMENT_MAGIC || (version != 1 && version != SEGMENT_VERSION) {
            return Err(RymeError::Corrupt(String::from("segment magic")));
        }
        let declared_max =
            if version == SEGMENT_VERSION { Some(take_u64(&mut cursor)?) } else { None };
        let payload_len = take_u32(&mut cursor)? as usize;
        if payload_len.checked_add(4).is_none_or(|required| cursor.len() < required) {
            return Err(RymeError::Corrupt(String::from("segment length")));
        }
        let payload = &cursor[..payload_len];
        cursor = &cursor[payload_len..];
        let engine = Engine::decode_snapshot(payload)?;
        let segment = Self::from_engine_owned(id, engine, kind);
        if declared_max.is_some_and(|declared| declared != segment.meta.max_commit_ts) {
            return Err(RymeError::Corrupt(String::from("segment commit")));
        }
        if segment.meta.max_commit_ts != segment.engine.max_commit_ts() {
            return Err(RymeError::Corrupt(String::from("segment commit")));
        }
        let bloom_len = take_u32(&mut cursor)? as usize;
        if bloom_len > cursor.len().saturating_sub(4) {
            return Err(RymeError::Corrupt(String::from("segment bloom")));
        }
        let bloom = &cursor[..bloom_len];
        cursor = &cursor[bloom_len..];
        let index_count = take_u32(&mut cursor)? as usize;
        if index_count != segment.sparse_index.len() {
            return Err(RymeError::Corrupt(String::from("segment index")));
        }
        for (key, expected_ordinal) in &segment.sparse_index {
            if !take_key_matches(&mut cursor, key)? {
                return Err(RymeError::Corrupt(String::from("segment metadata")));
            }
            let ordinal = take_u32(&mut cursor)? as usize;
            if ordinal != *expected_ordinal {
                return Err(RymeError::Corrupt(String::from("segment metadata")));
            }
        }
        if cursor.len() != 4 || bloom != segment.bloom || kind != segment.kind {
            return Err(RymeError::Corrupt(String::from("segment metadata")));
        }
        Ok(segment)
    }
}

#[derive(Debug, Clone)]
pub struct SegmentStore {
    dir: PathBuf,
    cache: Arc<Mutex<SegmentReadCache>>,
    paths: Arc<RwLock<Arc<Vec<PathBuf>>>>,
    segment_io: Arc<RwLock<()>>,
    compaction_gate: Arc<Mutex<()>>,
}

impl SegmentStore {
    pub fn open(dir: &Path) -> Result<Self> {
        Self::open_with_cache(dir, DEFAULT_SEGMENT_CACHE_BYTES)
    }

    pub fn open_with_cache(dir: &Path, cache_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let paths = discover_active_segment_paths(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            cache: Arc::new(Mutex::new(SegmentReadCache::new(cache_bytes))),
            paths: Arc::new(RwLock::new(Arc::new(paths))),
            segment_io: Arc::new(RwLock::new(())),
            compaction_gate: Arc::new(Mutex::new(())),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn cache_stats(&self) -> SegmentCacheStats {
        self.cache.lock().map(|cache| cache.stats()).unwrap_or(SegmentCacheStats {
            limit_bytes: 0,
            used_bytes: 0,
            entries: 0,
            hits: 0,
            misses: 0,
        })
    }

    pub fn write(&self, id: u64, engine: &Engine) -> Result<(SegmentMeta, PathBuf)> {
        let segment = ImmutableSegment::from_engine(id, engine);
        self.write_segment(segment, true, true)
    }

    pub fn write_delta(&self, id: u64, entries: &[SegmentEntry]) -> Result<SegmentMeta> {
        self.write_delta_with_sync(id, entries, true)
    }

    pub fn write_delta_with_sync(
        &self,
        id: u64,
        entries: &[SegmentEntry],
        sync: bool,
    ) -> Result<SegmentMeta> {
        self.write_delta_owned_with_sync(id, entries.to_vec(), sync)
    }

    pub fn write_delta_owned_with_sync(
        &self,
        id: u64,
        entries: Vec<SegmentEntry>,
        sync: bool,
    ) -> Result<SegmentMeta> {
        if entries.is_empty() {
            return Ok(SegmentMeta { id, max_commit_ts: 0, entries: 0, bytes: 0 });
        }
        let segment = ImmutableSegment::from_entries_owned(id, entries)?;
        let (meta, _) = self.write_segment(segment, sync, false)?;
        Ok(meta)
    }

    fn write_segment(
        &self,
        segment: ImmutableSegment,
        sync: bool,
        update_latest_pointer: bool,
    ) -> Result<(SegmentMeta, PathBuf)> {
        let id = segment.meta.id;
        let path = self.dir.join(segment_file_name(id, segment.kind));
        let name = segment_file_name(id, segment.kind);
        let encoded = segment.encode()?;
        if encoded.len() > MAX_SEGMENT_BYTES {
            return Err(RymeError::Overload(String::from("segment")));
        }
        let temporary = write_temp_file(&self.dir, &name, &encoded, sync)?;
        let _segment_io = match self.segment_io.write() {
            Ok(lock) => lock,
            Err(_) => {
                let _ = std::fs::remove_file(&temporary);
                return Err(RymeError::Internal(String::from("segment update lock")));
            }
        };
        if let Ok(mut cache) = self.cache.lock() {
            cache.remove(id);
        }
        install_temp_file(&temporary, &path)?;
        if segment.kind != SegmentKind::CompactedDelta {
            self.remember_segment(path.clone());
        }
        if update_latest_pointer {
            let latest = self.dir.join("latest");
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| RymeError::Internal(String::from("segment name")))?;
            let latest_tmp = write_temp_file(&self.dir, "latest", name.as_bytes(), sync)?;
            install_temp_file(&latest_tmp, &latest)?;
            self.refresh_paths()?;
        }
        if sync {
            sync_directory(&self.dir)?;
        }
        Ok((segment.meta.clone(), path))
    }

    pub fn latest(&self) -> Result<Option<ImmutableSegment>> {
        let Some(path) = self.latest_path()? else {
            return Ok(None);
        };
        let bytes = read_segment_file(&path)?;
        Ok(Some(ImmutableSegment::decode(&bytes)?))
    }

    fn read_segment_under_lock(&self, path: &Path) -> Result<Arc<ImmutableSegment>> {
        let id = parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or(""))
            .ok_or_else(|| RymeError::Corrupt(String::from("segment name")))?;
        if let Some(segment) = self.cache.lock().ok().and_then(|mut cache| cache.get(id)) {
            return Ok(segment);
        }

        // Do disk I/O and segment decoding without the global cache lock so
        // misses for different segments do not serialize concurrent readers.
        let bytes = read_segment_file(path)?;
        let segment = Arc::new(ImmutableSegment::decode(&bytes)?);
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(id, Arc::clone(&segment), bytes.len() as u64);
        }
        Ok(segment)
    }

    fn read_cached_segments(
        &self,
        paths: &[PathBuf],
    ) -> Option<Vec<Option<Arc<ImmutableSegment>>>> {
        let ids = paths
            .iter()
            .map(|path| parse_segment_name(path.file_name()?.to_str()?))
            .collect::<Option<Vec<_>>>()?;
        Some(self.cache.lock().ok()?.get_many_cached(&ids))
    }

    fn read_scan_segments(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: Option<&[u8]>,
    ) -> Result<Vec<(Arc<ImmutableSegment>, usize, usize)>> {
        // Hold the read lock only while resolving paths into owned segment
        // handles. Compaction cannot remove a file between path capture and
        // decode; the row merge itself runs after the lock is released.
        let _segment_io = self
            .segment_io
            .read()
            .map_err(|_| RymeError::Internal(String::from("segment read lock")))?;
        let paths = self.newest_paths()?;
        let mut cached_segments =
            self.read_cached_segments(&paths).map(|segments| segments.into_iter().rev());
        let mut segments = Vec::new();
        for path in paths.iter().rev() {
            let segment = match cached_segments.as_mut().and_then(Iterator::next).flatten() {
                Some(segment) => segment,
                None => self.read_segment_under_lock(path)?,
            };
            let scope_start = segment
                .keys
                .partition_point(|key| scope_order(key, tenant, database, table) == Ordering::Less);
            let scope_end = segment.keys[scope_start..].partition_point(|key| {
                scope_order(key, tenant, database, table) != Ordering::Greater
            }) + scope_start;
            let scan_start = start_after
                .map(|after| {
                    segment.keys[scope_start..scope_end]
                        .partition_point(|key| key.pk.as_slice() <= after)
                        + scope_start
                })
                .unwrap_or(scope_start);
            let is_snapshot = segment.is_snapshot();
            segments.push((segment, scan_start, scope_end));
            if is_snapshot {
                break;
            }
        }
        Ok(segments)
    }

    fn newest_paths(&self) -> Result<Arc<Vec<PathBuf>>> {
        self.paths
            .read()
            .map(|paths| Arc::clone(&paths))
            .map_err(|_| RymeError::Internal(String::from("segment paths lock")))
    }

    pub fn get(&self, key: &RecordKey, read_ts: u64, now: u64) -> Result<Option<Vec<u8>>> {
        let Some((_, value, expires_at)) = self.version_at(key, read_ts)? else {
            return Ok(None);
        };
        if expires_at != 0 && expires_at <= now {
            return Ok(None);
        }
        Ok(value)
    }

    /// Read several keys while resolving segment paths and loading each
    /// segment only once for the batch.
    pub fn get_many(
        &self,
        keys: &[RecordKey],
        read_ts: u64,
        now: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.get_many_with(keys.len(), |index| &keys[index], read_ts, now)
    }

    /// Read borrowed keys as a batch without allocating owned key copies.
    pub fn get_many_borrowed(
        &self,
        keys: &[&RecordKey],
        read_ts: u64,
        now: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.get_many_with(keys.len(), |index| keys[index], read_ts, now)
    }

    fn get_many_with<'a>(
        &self,
        key_count: usize,
        key_at: impl Fn(usize) -> &'a RecordKey,
        read_ts: u64,
        now: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        if key_count == 0 {
            return Ok(Vec::new());
        }

        let mut deduplicated = None;
        if key_count >= MIN_SEGMENT_DEDUP_BATCH_READ {
            let (mut strictly_ordered, mut strictly_reversed) = (true, true);
            for index in 1..key_count {
                strictly_ordered &= key_at(index - 1) < key_at(index);
                strictly_reversed &= key_at(index - 1) > key_at(index);
                if !strictly_ordered && !strictly_reversed {
                    break;
                }
            }
            if !strictly_ordered && !strictly_reversed {
                let mut unique_indices = Vec::with_capacity(key_count);
                let mut duplicate_indices = Vec::new();
                let mut first_indices = HashMap::new();
                for index in 0..key_count {
                    let key = key_at(index);
                    if let Some(first) = first_indices.get(key) {
                        duplicate_indices.push((index, *first));
                    } else {
                        first_indices.insert(key, index);
                        unique_indices.push(index);
                    }
                }
                if !duplicate_indices.is_empty() {
                    deduplicated = Some((unique_indices, duplicate_indices));
                }
            }
        }

        let _segment_io = self
            .segment_io
            .read()
            .map_err(|_| RymeError::Internal(String::from("segment read lock")))?;
        let paths = self.newest_paths()?;
        let mut values = vec![None; key_count];
        let (mut unresolved, duplicate_indices) =
            deduplicated.unwrap_or_else(|| ((0..key_count).collect(), Vec::new()));
        let mut next_unresolved = Vec::with_capacity(unresolved.len());

        for path in paths.iter().rev() {
            let segment = self.read_segment_under_lock(path)?;
            let is_snapshot = segment.is_snapshot();
            next_unresolved.clear();
            for index in unresolved.drain(..) {
                if let Some((_, value, expires_at)) = segment.version_at(key_at(index), read_ts) {
                    values[index] = if expires_at != 0 && expires_at <= now { None } else { value };
                } else if !is_snapshot {
                    next_unresolved.push(index);
                }
            }
            std::mem::swap(&mut unresolved, &mut next_unresolved);
            if unresolved.is_empty() || is_snapshot {
                break;
            }
        }

        for (duplicate, first) in duplicate_indices {
            values[duplicate] = values[first].clone();
        }

        Ok(values)
    }

    pub fn version_at(&self, key: &RecordKey, read_ts: u64) -> Result<Option<VersionLookup>> {
        let _segment_io = self
            .segment_io
            .read()
            .map_err(|_| RymeError::Internal(String::from("segment read lock")))?;
        let paths = self.newest_paths()?;
        for path in paths.iter().rev() {
            let segment = self.read_segment_under_lock(path)?;
            let Some(version) = segment.version_at(key, read_ts) else {
                if segment.is_snapshot() {
                    return Ok(None);
                }
                continue;
            };
            return Ok(Some(version));
        }
        Ok(None)
    }

    pub fn load_all(&self) -> Result<Option<(Engine, u64)>> {
        let mut paths = self.segment_paths()?;
        paths.sort_by_key(|path| {
            parse_segment_name(path.file_name().and_then(|n| n.to_str()).unwrap_or(""))
        });
        if let Some(snapshot_id) = self.latest_snapshot_id()? {
            paths.retain(|path| {
                parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or(""))
                    .is_some_and(|id| id >= snapshot_id)
            });
        }
        let mut engine = Engine::new();
        let mut found = false;
        let mut max_commit_ts = 0;
        for path in paths {
            let segment = ImmutableSegment::decode(&read_segment_file(&path)?)?;
            max_commit_ts = max_commit_ts.max(segment.meta.max_commit_ts);
            if segment.is_snapshot() {
                engine = segment.into_engine();
            } else {
                segment.merge_owned_into(&mut engine)?;
            }
            found = true;
        }
        Ok(found.then_some((engine, max_commit_ts)))
    }

    pub fn max_commit_ts(&self) -> Result<u64> {
        let mut max_commit_ts = 0;
        let snapshot_id = self.latest_snapshot_id()?;
        for path in self.segment_paths()? {
            if snapshot_id.is_some_and(|snapshot_id| {
                parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or(""))
                    .is_some_and(|id| id < snapshot_id)
            }) {
                continue;
            }
            max_commit_ts = max_commit_ts.max(read_segment_max_commit_ts(&path)?);
        }
        Ok(max_commit_ts)
    }

    fn latest_snapshot_id(&self) -> Result<Option<u64>> {
        latest_snapshot_id_in_dir(&self.dir)
    }

    pub fn scan(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        read_ts: u64,
        now: u64,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_from_segments(tenant, database, table, read_ts, now, None, limit)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn scan_after(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        read_ts: u64,
        now: u64,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_from_segments(tenant, database, table, read_ts, now, Some(start_after), limit)
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_from_segments(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        read_ts: u64,
        now: u64,
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let segments = self.read_scan_segments(tenant, database, table, start_after)?;
        if let Some((segment, _, _)) = segments.first() {
            if segment.is_snapshot() {
                return Ok(match start_after {
                    Some(after) => segment
                        .engine
                        .scan_after(tenant, database, table, read_ts, now, after, limit),
                    None => segment.engine.scan(tenant, database, table, read_ts, now, limit),
                });
            }
        }

        // Every segment is ordered by RecordKey. Merge the scoped key ranges
        // instead of materializing all visible rows before applying `limit`.
        // Segment order is newest-first, so the first version at read_ts wins.
        let mut cursors = BinaryHeap::with_capacity(segments.len());
        for (segment_index, (segment, start, end)) in segments.iter().enumerate() {
            if start < end {
                cursors.push(Reverse((segment.keys[*start].pk.as_slice(), segment_index, *start)));
            }
        }

        let mut out = Vec::with_capacity(limit.min(1024));
        while let Some(Reverse((pk, segment_index, key_index))) = cursors.pop() {
            let mut chosen: Option<Option<Vec<u8>>> = None;
            let mut current = Some((segment_index, key_index));
            while let Some((segment_index, key_index)) = current.take() {
                let (segment, _, end) = &segments[segment_index];
                let key = &segment.keys[key_index];
                if chosen.is_none() {
                    if let Some((_, value, expires_at)) = segment.engine.version_at(key, read_ts) {
                        chosen =
                            Some(if expires_at != 0 && expires_at <= now { None } else { value });
                    }
                }
                let next_index = key_index + 1;
                if next_index < *end {
                    cursors.push(Reverse((
                        segment.keys[next_index].pk.as_slice(),
                        segment_index,
                        next_index,
                    )));
                }
                current = if cursors.peek().is_some_and(|Reverse((next_pk, _, _))| next_pk == &pk) {
                    cursors
                        .pop()
                        .map(|Reverse((_, segment_index, key_index))| (segment_index, key_index))
                } else {
                    None
                };
            }
            if let Some(Some(value)) = chosen {
                out.push((pk.to_vec(), value));
                if out.len() == limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub fn latest_path(&self) -> Result<Option<PathBuf>> {
        let pointer = self.dir.join("latest");
        let pointed = if let Some(raw) =
            read_bounded_metadata_file(&pointer, MAX_SEGMENT_POINTER_BYTES, "segment pointer")?
        {
            let name = String::from_utf8(raw)
                .map_err(|_| RymeError::Corrupt(String::from("segment pointer")))?;
            let name = name.trim();
            if !name.is_empty() && parse_segment_name(name).is_some() && !name.contains('/') {
                let path = self.dir.join(name);
                if std::fs::symlink_metadata(&path)
                    .is_ok_and(|metadata| metadata.file_type().is_file())
                {
                    Some(path)
                } else {
                    return Err(RymeError::Corrupt(String::from("segment pointer")));
                }
            } else {
                return Err(RymeError::Corrupt(String::from("segment pointer")));
            }
        } else {
            None
        };
        let mut paths = self.segment_paths()?;
        paths.sort_by_key(|path| {
            parse_segment_name(path.file_name().and_then(|n| n.to_str()).unwrap_or(""))
        });
        let newest = paths.pop();
        Ok(match (pointed, newest) {
            (Some(pointed), Some(newest)) => {
                let pointed_id = parse_segment_name(
                    pointed.file_name().and_then(|name| name.to_str()).unwrap_or(""),
                );
                let newest_id = parse_segment_name(
                    newest.file_name().and_then(|name| name.to_str()).unwrap_or(""),
                );
                if newest_id > pointed_id {
                    Some(newest)
                } else {
                    Some(pointed)
                }
            }
            (Some(pointed), None) => Some(pointed),
            (None, newest) => newest,
        })
    }

    pub fn segment_paths(&self) -> Result<Vec<PathBuf>> {
        discover_active_segment_paths(&self.dir)
    }

    fn refresh_paths(&self) -> Result<()> {
        let paths = discover_active_segment_paths(&self.dir)?;
        let mut cached = self
            .paths
            .write()
            .map_err(|_| RymeError::Internal(String::from("segment paths lock")))?;
        *cached = Arc::new(paths);
        Ok(())
    }

    fn remember_segment(&self, path: PathBuf) {
        let Ok(mut paths) = self.paths.write() else { return };
        let id = parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or(""));
        let Some(id) = id else { return };
        let index = paths.binary_search_by_key(&id, |candidate| {
            parse_segment_name(candidate.file_name().and_then(|name| name.to_str()).unwrap_or(""))
                .unwrap_or(u64::MAX)
        });
        match index {
            Ok(index) if paths[index] == path => {}
            Ok(index) => Arc::make_mut(&mut paths)[index] = path,
            Err(index) => Arc::make_mut(&mut paths).insert(index, path),
        }
    }

    fn forget_segment(&self, path: &Path) {
        if let Some(id) =
            parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or(""))
        {
            if let Ok(mut cache) = self.cache.lock() {
                cache.remove(id);
            }
        }
        if let Ok(mut paths) = self.paths.write() {
            Arc::make_mut(&mut paths).retain(|candidate| candidate != path);
        }
    }

    pub fn remove_except(&self, keep: &Path) -> Result<usize> {
        let mut removed = 0;
        for path in discover_segment_paths(&self.dir)? {
            if path == keep {
                continue;
            }
            self.remove_segment_file(&path)?;
            removed += 1;
        }
        Ok(removed)
    }

    fn remove_segment_file(&self, path: &Path) -> Result<()> {
        let _segment_io = self
            .segment_io
            .write()
            .map_err(|_| RymeError::Internal(String::from("segment update lock")))?;
        std::fs::remove_file(path)?;
        self.forget_segment(path);
        Ok(())
    }

    pub fn compact_delta_run(&self) -> Result<Option<SegmentMeta>> {
        let _maintenance = self
            .compaction_gate
            .lock()
            .map_err(|_| RymeError::Internal(String::from("segment compaction lock")))?;
        let paths = self.segment_paths()?;
        let snapshot_id = self.latest_snapshot_id()?.unwrap_or(0);
        let mut selected = Vec::new();
        let mut selected_bytes = 0u64;
        for path in paths {
            let id =
                parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or(""))
                    .ok_or_else(|| RymeError::Corrupt(String::from("segment name")))?;
            if id <= snapshot_id {
                continue;
            }
            let bytes = std::fs::metadata(&path)?.len();
            let Some(total_bytes) = selected_bytes.checked_add(bytes) else {
                break;
            };
            if total_bytes > MAX_DELTA_COMPACTION_INPUT_BYTES {
                break;
            }
            selected_bytes = total_bytes;
            selected.push(path);
        }
        if selected.len() < 2 {
            return Ok(None);
        }

        let mut merged = Engine::new();
        for path in &selected {
            let segment = ImmutableSegment::decode(&read_segment_file(path)?)?;
            if segment.is_snapshot() {
                return Ok(None);
            }
            segment.merge_owned_into(&mut merged)?;
        }
        if merged.is_empty() {
            return Ok(None);
        }
        let through_id = parse_segment_name(
            selected
                .last()
                .and_then(|path| path.file_name())
                .and_then(|name| name.to_str())
                .unwrap_or(""),
        )
        .ok_or_else(|| RymeError::Corrupt(String::from("segment name")))?;
        if through_id <= snapshot_id {
            return Ok(None);
        }

        let segment =
            ImmutableSegment::from_engine_owned(through_id, merged, SegmentKind::CompactedDelta);
        let (meta, _) = self.write_segment(segment, true, false)?;
        publish_delta_compaction_marker(
            &self.dir,
            DeltaCompactionMarker { snapshot_id, through_id, output_id: through_id },
        )?;
        self.refresh_paths()?;
        for path in selected {
            self.remove_segment_file(&path)?;
        }
        Ok(Some(meta))
    }

    pub fn compact(&self) -> Result<Option<SegmentMeta>> {
        let _maintenance = self
            .compaction_gate
            .lock()
            .map_err(|_| RymeError::Internal(String::from("segment compaction lock")))?;
        let Some((engine, max_commit_ts)) = self.load_all()? else {
            return Ok(None);
        };
        let segment =
            ImmutableSegment::from_engine_owned(max_commit_ts, engine, SegmentKind::Snapshot);
        let (meta, newest) = self.write_segment(segment, true, true)?;
        self.remove_except(&newest)?;
        let marker = self.dir.join("delta-compaction");
        if marker.exists() {
            std::fs::remove_file(marker)?;
            sync_directory(&self.dir)?;
        }
        self.refresh_paths()?;
        Ok(Some(meta))
    }

    pub fn prune(&self, keep: usize) -> Result<usize> {
        let mut segments = Vec::new();
        for path in self.segment_paths()? {
            let (id, kind) = read_segment_identity(&path)?;
            let path_id =
                parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or(""))
                    .ok_or_else(|| RymeError::Corrupt(String::from("segment name")))?;
            if id != path_id {
                return Err(RymeError::Corrupt(String::from("segment id")));
            }
            // Retention only needs each segment's identity and kind. Verify
            // the complete file checksum without rebuilding its row index.
            read_segment_max_commit_ts(&path)?;
            segments.push((id, path, kind == SegmentKind::Snapshot));
        }
        segments.sort_by_key(|(id, _, _)| *id);
        let full_ids: Vec<u64> =
            segments.iter().filter_map(|(id, _, is_snapshot)| is_snapshot.then_some(*id)).collect();
        let mut keep_paths = Vec::new();
        if let Some(latest_full) = full_ids.last().copied() {
            keep_paths.extend(
                full_ids
                    .iter()
                    .rev()
                    .take(keep.max(1))
                    .filter_map(|id| segments.iter().find(|(segment_id, _, _)| segment_id == id))
                    .map(|(_, path, _)| path.clone()),
            );
            keep_paths.extend(
                segments
                    .iter()
                    .filter(|(id, _, is_snapshot)| *id > latest_full && !is_snapshot)
                    .map(|(_, path, _)| path.clone()),
            );
        } else {
            // Without a full base, every delta is part of the recovery chain.
            // Pruning a suffix here would silently make older keys unrecoverable.
            return Ok(0);
        }
        let mut removed = 0;
        for (_, path, _) in segments {
            if keep_paths.contains(&path) {
                continue;
            }
            if self.remove_segment_file(&path).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

fn read_segment_file(path: &Path) -> Result<Vec<u8>> {
    read_bounded_metadata_file(path, MAX_SEGMENT_BYTES, "segment")?.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "segment file missing").into()
    })
}

fn read_bounded_metadata_file(
    path: &Path,
    max_bytes: usize,
    label: &str,
) -> Result<Option<Vec<u8>>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_file() {
        return Err(RymeError::Corrupt(label.to_string()));
    }
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    verify_opened_file(&metadata, &file, label)?;
    let file_len = file.metadata()?.len();
    if file_len > max_bytes as u64 {
        return Err(RymeError::Overload(String::from(label)));
    }
    let capacity = usize::try_from(file_len).map_err(|_| RymeError::Overload(label.to_string()))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take((max_bytes as u64).saturating_add(1)).read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(RymeError::Overload(String::from(label)));
    }
    Ok(Some(bytes))
}

fn verify_opened_file(
    path_metadata: &std::fs::Metadata,
    file: &std::fs::File,
    label: &str,
) -> Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(RymeError::Corrupt(label.to_string()));
    }
    #[cfg(not(unix))]
    let _ = path_metadata;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        use std::os::unix::fs::PermissionsExt;
        if path_metadata.dev() != metadata.dev() || path_metadata.ino() != metadata.ino() {
            return Err(RymeError::Corrupt(label.to_string()));
        }
        if metadata.permissions().mode() & 0o7777 != 0o600 {
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
    }
    Ok(())
}

fn read_segment_identity(path: &Path) -> Result<(u64, SegmentKind)> {
    const V1_HEADER: u64 = 4 + 2 + 2 + 8 + 4;
    const V2_HEADER: u64 = V1_HEADER + 8;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(RymeError::Corrupt(String::from("segment file")));
    }
    let mut file = std::fs::File::open(path)?;
    verify_opened_file(&metadata, &file, "segment file")?;
    let file_len = file.metadata()?.len();
    let mut prefix = [0; 16];
    file.read_exact(&mut prefix)?;
    let mut cursor = &prefix[..];
    let magic = take_u32(&mut cursor)?;
    let version = take_u16(&mut cursor)?;
    let kind = match take_u16(&mut cursor)? {
        0 => SegmentKind::Snapshot,
        1 => SegmentKind::Delta,
        2 => SegmentKind::CompactedDelta,
        _ => return Err(RymeError::Corrupt(String::from("segment kind"))),
    };
    let id = take_u64(&mut cursor)?;
    let header_len = match version {
        1 => V1_HEADER,
        SEGMENT_VERSION => V2_HEADER,
        _ => return Err(RymeError::Corrupt(String::from("segment magic"))),
    };
    if magic != SEGMENT_MAGIC || file_len < header_len + 4 {
        return Err(RymeError::Corrupt(String::from("segment header")));
    }
    Ok((id, kind))
}

fn read_segment_max_commit_ts(path: &Path) -> Result<u64> {
    const V2_HEADER: usize = 4 + 2 + 2 + 8 + 8 + 4;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(RymeError::Corrupt(String::from("segment file")));
    }
    let mut file = std::fs::File::open(path)?;
    verify_opened_file(&metadata, &file, "segment file")?;
    let file_len = file.metadata()?.len();
    if file_len > MAX_SEGMENT_BYTES as u64 {
        return Err(RymeError::Overload(String::from("segment")));
    }
    if file_len < 6 {
        return Err(RymeError::Corrupt(String::from("segment header")));
    }

    let mut prefix = [0; 6];
    file.read_exact(&mut prefix)?;
    let version = u16::from_be_bytes([prefix[4], prefix[5]]);
    if version == 1 {
        let bytes = read_segment_file(path)?;
        return Ok(ImmutableSegment::decode(&bytes)?.meta.max_commit_ts);
    }
    if version != SEGMENT_VERSION || file_len < (V2_HEADER + 4) as u64 {
        return Err(RymeError::Corrupt(String::from("segment header")));
    }

    file.seek(SeekFrom::Start(0))?;
    let mut header = [0; V2_HEADER];
    file.read_exact(&mut header)?;
    let mut cursor = &header[..];
    let magic = take_u32(&mut cursor)?;
    let _version = take_u16(&mut cursor)?;
    let kind = take_u16(&mut cursor)?;
    let id = take_u64(&mut cursor)?;
    let max_commit_ts = take_u64(&mut cursor)?;
    let payload_len = take_u32(&mut cursor)? as u64;
    let expected_id =
        parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or(""));
    if magic != SEGMENT_MAGIC
        || !matches!(kind, 0..=2)
        || expected_id != Some(id)
        || payload_len > file_len.saturating_sub((V2_HEADER + 4) as u64)
    {
        return Err(RymeError::Corrupt(String::from("segment header")));
    }

    file.seek(SeekFrom::Start(0))?;
    let checksum_len = file_len - 4;
    let mut remaining = checksum_len;
    let mut buffer = [0; 64 * 1024];
    let mut hasher = crc32fast::Hasher::new();
    while remaining > 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..count])?;
        hasher.update(&buffer[..count]);
        remaining -= count as u64;
    }
    let mut stored_checksum = [0; 4];
    file.read_exact(&mut stored_checksum)?;
    if hasher.finalize() != u32::from_be_bytes(stored_checksum) {
        return Err(RymeError::Corrupt(String::from("segment checksum")));
    }

    Ok(max_commit_ts)
}

fn read_delta_compaction_marker(dir: &Path) -> Result<Option<DeltaCompactionMarker>> {
    let path = dir.join("delta-compaction");
    let Some(bytes) = read_bounded_metadata_file(
        &path,
        DELTA_COMPACTION_MARKER_BYTES,
        "delta compaction marker",
    )?
    else {
        return Ok(None);
    };
    if bytes.len() != DELTA_COMPACTION_MARKER_BYTES {
        return Err(RymeError::Corrupt(String::from("delta compaction marker")));
    }
    let checksum_offset = bytes.len() - 4;
    let stored_checksum = u32::from_be_bytes(
        bytes[checksum_offset..]
            .try_into()
            .map_err(|_| RymeError::Corrupt(String::from("delta compaction marker")))?,
    );
    if crc32fast::hash(&bytes[..checksum_offset]) != stored_checksum {
        return Err(RymeError::Corrupt(String::from("delta compaction marker")));
    }
    let mut cursor = &bytes[..checksum_offset];
    let magic = take_u32(&mut cursor)?;
    let version = take_u16(&mut cursor)?;
    let marker = DeltaCompactionMarker {
        snapshot_id: take_u64(&mut cursor)?,
        through_id: take_u64(&mut cursor)?,
        output_id: take_u64(&mut cursor)?,
    };
    if magic != DELTA_COMPACTION_MAGIC
        || version != DELTA_COMPACTION_VERSION
        || marker.through_id <= marker.snapshot_id
        || marker.output_id != marker.through_id
    {
        return Err(RymeError::Corrupt(String::from("delta compaction marker")));
    }
    Ok(Some(marker))
}

fn publish_delta_compaction_marker(dir: &Path, marker: DeltaCompactionMarker) -> Result<()> {
    let mut bytes = Vec::with_capacity(DELTA_COMPACTION_MARKER_BYTES);
    bytes.extend_from_slice(&DELTA_COMPACTION_MAGIC.to_be_bytes());
    bytes.extend_from_slice(&DELTA_COMPACTION_VERSION.to_be_bytes());
    bytes.extend_from_slice(&marker.snapshot_id.to_be_bytes());
    bytes.extend_from_slice(&marker.through_id.to_be_bytes());
    bytes.extend_from_slice(&marker.output_id.to_be_bytes());
    let checksum = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());

    let path = dir.join("delta-compaction");
    let temporary = write_temp_file(dir, "delta-compaction", &bytes, true)?;
    install_temp_file(&temporary, &path)?;
    sync_directory(dir)
}

fn latest_snapshot_id_in_dir(dir: &Path) -> Result<Option<u64>> {
    let Some(raw) = read_bounded_metadata_file(
        &dir.join("latest"),
        MAX_SEGMENT_POINTER_BYTES,
        "segment pointer",
    )?
    else {
        return Ok(None);
    };
    let name = std::str::from_utf8(&raw)
        .map_err(|_| RymeError::Corrupt(String::from("segment pointer")))?
        .trim();
    let id = parse_segment_name(name)
        .ok_or_else(|| RymeError::Corrupt(String::from("segment pointer")))?;
    let path = dir.join(name);
    let (stored_id, kind) = read_segment_identity(&path)
        .map_err(|_| RymeError::Corrupt(String::from("segment pointer")))?;
    if stored_id != id {
        return Err(RymeError::Corrupt(String::from("segment pointer")));
    }
    Ok((kind == SegmentKind::Snapshot).then_some(id))
}

fn discover_segment_paths(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(paths),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && parse_segment_name(&entry.file_name().to_string_lossy()).is_some()
        {
            paths.push(entry.path());
        }
    }
    paths.sort_by_key(|path| {
        (
            parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or("")),
            is_compacted_segment_path(path),
        )
    });
    Ok(paths)
}

fn discover_active_segment_paths(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = discover_segment_paths(dir)?;
    let Some(marker) = read_delta_compaction_marker(dir)? else {
        paths.retain(|path| !is_compacted_segment_path(path));
        return Ok(paths);
    };
    let latest_snapshot_id = latest_snapshot_id_in_dir(dir)?.unwrap_or(0);
    if latest_snapshot_id < marker.snapshot_id {
        return Err(RymeError::Corrupt(String::from("delta compaction snapshot")));
    }
    if latest_snapshot_id > marker.snapshot_id {
        paths.retain(|path| !is_compacted_segment_path(path));
        return Ok(paths);
    }

    let output = dir.join(compacted_segment_name(marker.output_id));
    if !output.is_file() {
        return Err(RymeError::Corrupt(String::from("delta compaction output")));
    }
    if read_segment_identity(&output)? != (marker.output_id, SegmentKind::CompactedDelta) {
        return Err(RymeError::Corrupt(String::from("delta compaction output")));
    }
    paths.retain(|path| {
        let id = parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap_or(""));
        if is_compacted_segment_path(path) {
            id == Some(marker.output_id)
        } else {
            !id.is_some_and(|id| id > marker.snapshot_id && id <= marker.through_id)
        }
    });
    Ok(paths)
}

fn scope_order(key: &RecordKey, tenant: &str, database: &str, table: &str) -> Ordering {
    key.tenant
        .as_str()
        .cmp(tenant)
        .then_with(|| key.database.as_str().cmp(database))
        .then_with(|| key.table.as_str().cmp(table))
}

fn segment_name(id: u64) -> String {
    format!("segment-{id:020}.sst")
}

fn compacted_segment_name(id: u64) -> String {
    format!("segment-{id:020}.cmp")
}

fn segment_file_name(id: u64, kind: SegmentKind) -> String {
    if kind == SegmentKind::CompactedDelta {
        compacted_segment_name(id)
    } else {
        segment_name(id)
    }
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let name = name.strip_prefix("segment-")?;
    let name = name.strip_suffix(".sst").or_else(|| name.strip_suffix(".cmp"))?;
    name.parse().ok()
}

fn is_compacted_segment_path(path: &Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("cmp")
}

fn build_bloom(keys: &[RecordKey]) -> Vec<u8> {
    let bytes =
        ((keys.len().max(1) * BLOOM_BITS_PER_KEY).saturating_add(7) / 8).max(MIN_BLOOM_BYTES);
    let mut bloom = vec![0u8; bytes];
    for key in keys {
        set_bloom_bits(&mut bloom, key);
    }
    bloom
}

fn bloom_might_contain(bloom: &[u8], key: &RecordKey) -> bool {
    if bloom.is_empty() {
        return false;
    }
    let (first, second) = bloom_hashes(key, bloom.len() * 8);
    [first, second].into_iter().all(|bit| bloom[bit / 8] & (1 << (bit % 8)) != 0)
}

fn set_bloom_bits(bloom: &mut [u8], key: &RecordKey) {
    let (first, second) = bloom_hashes(key, bloom.len() * 8);
    for bit in [first, second] {
        bloom[bit / 8] |= 1 << (bit % 8);
    }
}

fn bloom_hashes(key: &RecordKey, bits: usize) -> (usize, usize) {
    let first = (stable_hash(key, 0) as usize) % bits;
    let second = (stable_hash(key, 0x9e37_79b9_7f4a_7c15) as usize) % bits;
    (first, second)
}

fn stable_hash(key: &RecordKey, seed: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64 ^ seed;
    for field in [key.tenant.as_bytes(), key.database.as_bytes(), key.table.as_bytes(), &key.pk] {
        for byte in field {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

fn take_u16(cursor: &mut &[u8]) -> Result<u16> {
    if cursor.len() < 2 {
        return Err(RymeError::Corrupt(String::from("segment truncated")));
    }
    let value = u16::from_be_bytes([cursor[0], cursor[1]]);
    *cursor = &cursor[2..];
    Ok(value)
}

fn take_u32(cursor: &mut &[u8]) -> Result<u32> {
    if cursor.len() < 4 {
        return Err(RymeError::Corrupt(String::from("segment truncated")));
    }
    let value = u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
    *cursor = &cursor[4..];
    Ok(value)
}

fn take_u64(cursor: &mut &[u8]) -> Result<u64> {
    if cursor.len() < 8 {
        return Err(RymeError::Corrupt(String::from("segment truncated")));
    }
    let value = u64::from_be_bytes([
        cursor[0], cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6], cursor[7],
    ]);
    *cursor = &cursor[8..];
    Ok(value)
}

fn push_segment_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let new_len = out
        .len()
        .checked_add(bytes.len())
        .filter(|length| *length <= MAX_SEGMENT_BYTES)
        .ok_or_else(|| RymeError::Overload(String::from("segment")))?;
    out.try_reserve(new_len - out.len())
        .map_err(|_| RymeError::Overload(String::from("segment")))?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn push_key(out: &mut Vec<u8>, key: &RecordKey) -> Result<()> {
    for field in [key.tenant.as_bytes(), key.database.as_bytes(), key.table.as_bytes()] {
        if field.len() > u16::MAX as usize {
            return Err(RymeError::InvalidArgument(String::from("segment key")));
        }
        push_segment_bytes(out, &(field.len() as u16).to_be_bytes())?;
        push_segment_bytes(out, field)?;
    }
    if key.pk.len() > u32::MAX as usize {
        return Err(RymeError::InvalidArgument(String::from("segment key")));
    }
    push_segment_bytes(out, &(key.pk.len() as u32).to_be_bytes())?;
    push_segment_bytes(out, &key.pk)?;
    Ok(())
}

fn take_key_matches(cursor: &mut &[u8], expected: &RecordKey) -> Result<bool> {
    for field in
        [expected.tenant.as_bytes(), expected.database.as_bytes(), expected.table.as_bytes()]
    {
        if take_field(cursor)? != field {
            return Ok(false);
        }
    }
    let pk_len = take_u32(cursor)? as usize;
    if pk_len > cursor.len() {
        return Err(RymeError::Corrupt(String::from("segment key")));
    }
    let pk = &cursor[..pk_len];
    *cursor = &cursor[pk_len..];
    Ok(pk == expected.pk.as_slice())
}

fn take_field<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8]> {
    if cursor.len() < 2 {
        return Err(RymeError::Corrupt(String::from("segment truncated")));
    }
    let len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    *cursor = &cursor[2..];
    if cursor.len() < len {
        return Err(RymeError::Corrupt(String::from("segment truncated")));
    }
    let field = &cursor[..len];
    *cursor = &cursor[len..];
    Ok(field)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_v1_segment(segment: &ImmutableSegment) -> Vec<u8> {
        let v2 = segment.encode().unwrap();
        let mut v1 = Vec::with_capacity(v2.len() - 8);
        v1.extend_from_slice(&v2[..4]);
        v1.extend_from_slice(&1u16.to_be_bytes());
        v1.extend_from_slice(&v2[6..16]);
        v1.extend_from_slice(&v2[24..v2.len() - 4]);
        let checksum = crc32fast::hash(&v1);
        v1.extend_from_slice(&checksum.to_be_bytes());
        v1
    }

    #[test]
    fn immutable_segment_roundtrip_uses_index_and_bloom() {
        let mut engine = Engine::new();
        let first = RecordKey::new("tenant", "db", "items", b"a");
        let second = RecordKey::new("tenant", "db", "items", b"b");
        engine.apply(first.clone(), 10, Some(b"one".to_vec())).unwrap();
        engine.apply(second.clone(), 11, Some(b"two".to_vec())).unwrap();
        let segment = ImmutableSegment::from_engine(7, &engine);
        assert_eq!(segment.meta().id, 7);
        assert_eq!(segment.key_count(), 2);
        assert_eq!(segment.sparse_index_len(), 1);
        assert!(segment.bloom_might_contain(&first));
        assert!(!segment.bloom_might_contain(&RecordKey::new("tenant", "db", "items", b"missing")));
        engine.apply(first.clone(), 12, Some(b"changed".to_vec())).unwrap();
        assert_eq!(segment.get(&first, 12, 0).unwrap(), Some(b"one".to_vec()));
        let restored = ImmutableSegment::decode(&segment.encode().unwrap()).unwrap();
        assert_eq!(restored.get(&first, 10, 0).unwrap(), Some(b"one".to_vec()));
        assert_eq!(restored.get(&second, 11, 0).unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn segment_reads_confirm_bloom_false_positives_in_engine() {
        let key = RecordKey::new("tenant", "db", "items", b"present");
        let missing = RecordKey::new("tenant", "db", "items", b"missing");
        let mut engine = Engine::new();
        engine.apply(key.clone(), 1, Some(b"value".to_vec())).unwrap();
        let mut segment = ImmutableSegment::from_engine(1, &engine);
        segment.bloom.fill(u8::MAX);

        assert!(segment.bloom_might_contain(&missing));
        assert_eq!(segment.get(&key, 1, 0).unwrap(), Some(b"value".to_vec()));
        assert_eq!(segment.version_at(&key, 1), Some((1, Some(b"value".to_vec()), 0)));
        assert_eq!(segment.get(&missing, 1, 0).unwrap(), None);
        assert_eq!(segment.version_at(&missing, 1), None);
    }

    #[test]
    fn segment_v1_remains_readable_after_header_upgrade() {
        let key = RecordKey::new("tenant", "db", "items", b"legacy");
        let mut engine = Engine::new();
        engine.apply(key.clone(), 42, Some(b"value".to_vec())).unwrap();
        let segment = ImmutableSegment::from_engine(42, &engine);

        let restored = ImmutableSegment::decode(&encode_v1_segment(&segment)).unwrap();
        assert_eq!(restored.meta().max_commit_ts, 42);
        assert_eq!(restored.get(&key, 42, 0).unwrap(), Some(b"value".to_vec()));

        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-v1-max-ts-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(segment_name(42)), encode_v1_segment(&segment)).unwrap();
        assert_eq!(SegmentStore::open(&dir).unwrap().max_commit_ts().unwrap(), 42);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_decode_rejects_overflowing_payload_length() {
        let segment = ImmutableSegment::from_engine(7, &Engine::new());
        let mut encoded = segment.encode().unwrap();
        encoded[24..28].copy_from_slice(&u32::MAX.to_be_bytes());
        let checksum_offset = encoded.len() - 4;
        let checksum = crc32fast::hash(&encoded[..checksum_offset]);
        encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

        assert!(matches!(
            ImmutableSegment::decode(&encoded),
            Err(RymeError::Corrupt(message)) if message == "segment length"
        ));
    }

    #[test]
    fn segment_max_commit_reads_header_and_checks_checksum() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-max-ts-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let key = RecordKey::new("tenant", "db", "items", b"row");
        let mut engine = Engine::new();
        engine.apply(key, 42, Some(b"value".to_vec())).unwrap();
        let (_, path) = store.write(42, &engine).unwrap();
        assert_eq!(store.max_commit_ts().unwrap(), 42);

        let mut encoded = std::fs::read(&path).unwrap();
        encoded[32] ^= 1;
        std::fs::write(path, encoded).unwrap();
        assert!(
            matches!(store.max_commit_ts(), Err(RymeError::Corrupt(message)) if message == "segment checksum")
        );
        assert!(
            matches!(store.prune(1), Err(RymeError::Corrupt(message)) if message == "segment checksum")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn decode_rejects_sparse_index_mismatch_without_accepting_bad_metadata() {
        let key = RecordKey::new("tenant", "db", "items", b"a");
        let mut engine = Engine::new();
        engine.apply(key.clone(), 10, Some(b"value".to_vec())).unwrap();
        let segment = ImmutableSegment::from_engine(7, &engine);
        let mut encoded = segment.encode().unwrap();

        let payload_len = u32::from_be_bytes(encoded[24..28].try_into().unwrap()) as usize;
        let metadata_offset = 28 + payload_len;
        let bloom_len =
            u32::from_be_bytes(encoded[metadata_offset..metadata_offset + 4].try_into().unwrap())
                as usize;
        let mut index_cursor = &encoded[metadata_offset + 4 + bloom_len..];
        assert_eq!(take_u32(&mut index_cursor).unwrap(), 1);
        assert!(take_key_matches(&mut index_cursor, &key).unwrap());
        let ordinal_offset = encoded.len() - 4 - index_cursor.len();
        encoded[ordinal_offset..ordinal_offset + 4].copy_from_slice(&1u32.to_be_bytes());
        let checksum_offset = encoded.len() - 4;
        let checksum = crc32fast::hash(&encoded[..checksum_offset]);
        encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

        assert!(matches!(
            ImmutableSegment::decode(&encoded),
            Err(RymeError::Corrupt(message)) if message == "segment metadata"
        ));
    }

    #[test]
    fn delta_segments_merge_after_snapshot_base() {
        let mut base = Engine::new();
        let first = RecordKey::new("tenant", "db", "items", b"a");
        base.apply(first.clone(), 10, Some(b"one".to_vec())).unwrap();
        let snapshot = ImmutableSegment::from_engine(10, &base);
        let second = RecordKey::new("tenant", "db", "items", b"b");
        let delta = ImmutableSegment::from_entries(
            11,
            &[SegmentEntry {
                key: second.clone(),
                commit_ts: 11,
                value: Some(b"two".to_vec()),
                expires_at: 0,
            }],
        )
        .unwrap();
        let mut restored = snapshot.into_engine();
        delta.merge_into(&mut restored).unwrap();
        assert_eq!(restored.read(&first, 11, 0).unwrap(), Some(b"one".to_vec()));
        assert_eq!(restored.read(&second, 11, 0).unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn owned_delta_entries_preserve_sorted_and_unsorted_history() {
        let first = RecordKey::new("tenant", "db", "items", b"a");
        let second = RecordKey::new("tenant", "db", "items", b"b");
        let entries = vec![
            SegmentEntry {
                key: first.clone(),
                commit_ts: 1,
                value: Some(b"a1".to_vec()),
                expires_at: 0,
            },
            SegmentEntry {
                key: first.clone(),
                commit_ts: 2,
                value: Some(b"a2".to_vec()),
                expires_at: 0,
            },
            SegmentEntry {
                key: second.clone(),
                commit_ts: 2,
                value: Some(b"b2".to_vec()),
                expires_at: 0,
            },
        ];
        let sorted = ImmutableSegment::from_entries_owned(7, entries.clone()).unwrap();
        let unsorted =
            ImmutableSegment::from_entries_owned(7, entries.into_iter().rev().collect()).unwrap();

        assert_eq!(sorted.encode().unwrap(), unsorted.encode().unwrap());
        let restored = ImmutableSegment::decode(&sorted.encode().unwrap()).unwrap();
        assert!(!restored.is_snapshot());
        assert_eq!(restored.get(&first, 1, 0).unwrap(), Some(b"a1".to_vec()));
        assert_eq!(restored.get(&first, 2, 0).unwrap(), Some(b"a2".to_vec()));
        assert_eq!(restored.get(&second, 2, 0).unwrap(), Some(b"b2".to_vec()));
    }

    #[test]
    fn segment_store_reopens_latest_segment() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segments-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let mut engine = Engine::new();
        let key = RecordKey::new("tenant", "db", "items", b"k");
        engine.apply(key.clone(), 42, Some(b"value".to_vec())).unwrap();
        store.write(42, &engine).unwrap();
        drop(store);
        let reopened = SegmentStore::open(&dir).unwrap();
        let segment = reopened.latest().unwrap().unwrap();
        assert_eq!(segment.meta().max_commit_ts, 42);
        assert_eq!(segment.get(&key, 42, 0).unwrap(), Some(b"value".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delta_write_keeps_snapshot_pointer_but_latest_tracks_new_delta() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-latest-delta-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let key = RecordKey::new("tenant", "db", "items", b"k");
        let mut engine = Engine::new();
        engine.apply(key.clone(), 1, Some(b"one".to_vec())).unwrap();
        store.write(1, &engine).unwrap();
        store
            .write_delta(
                2,
                &[SegmentEntry { key, commit_ts: 2, value: Some(b"two".to_vec()), expires_at: 0 }],
            )
            .unwrap();

        assert_eq!(std::fs::read(dir.join("latest")).unwrap(), segment_name(1).as_bytes());
        assert_eq!(
            store.latest_path().unwrap().unwrap().file_name().unwrap().to_str().unwrap(),
            segment_name(2)
        );
        assert_eq!(store.latest().unwrap().unwrap().meta().max_commit_ts, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_rejects_oversized_segment_file() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-limit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let path = dir.join(segment_name(1));
        let file = std::fs::File::create(&path).unwrap();
        file.set_len((MAX_SEGMENT_BYTES + 1) as u64).unwrap();
        std::fs::write(dir.join("latest"), segment_name(1)).unwrap();

        assert!(matches!(
            store.latest(),
            Err(RymeError::Overload(message)) if message == "segment"
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_rejects_invalid_latest_pointer() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-pointer-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        std::fs::write(dir.join("latest"), b"../outside.sst").unwrap();
        assert!(
            matches!(store.latest(), Err(RymeError::Corrupt(message)) if message == "segment pointer")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_rejects_oversized_latest_pointer() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-pointer-limit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let pointer = std::fs::File::create(dir.join("latest")).unwrap();
        pointer.set_len((MAX_SEGMENT_POINTER_BYTES + 1) as u64).unwrap();

        assert!(matches!(
            store.latest(),
            Err(RymeError::Overload(message)) if message == "segment pointer"
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delta_compaction_marker_rejects_oversized_file() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-delta-marker-limit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let marker = std::fs::File::create(dir.join("delta-compaction")).unwrap();
        marker.set_len((DELTA_COMPACTION_MARKER_BYTES + 1) as u64).unwrap();

        assert!(matches!(
            read_delta_compaction_marker(&dir),
            Err(RymeError::Overload(message)) if message == "delta compaction marker"
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_prunes_superseded_deltas_but_keeps_recovery_chain() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-prune-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let mut engine = Engine::new();
        let first = RecordKey::new("tenant", "db", "items", b"a");
        engine.apply(first.clone(), 10, Some(b"one".to_vec())).unwrap();
        store.write(10, &engine).unwrap();
        let second = RecordKey::new("tenant", "db", "items", b"b");
        store
            .write_delta(
                11,
                &[SegmentEntry {
                    key: second.clone(),
                    commit_ts: 11,
                    value: Some(b"two".to_vec()),
                    expires_at: 0,
                }],
            )
            .unwrap();
        assert_eq!(store.prune(1).unwrap(), 0);
        assert_eq!(store.segment_paths().unwrap().len(), 2);
        let (recovered, max) = store.load_all().unwrap().unwrap();
        assert_eq!(max, 11);
        assert_eq!(recovered.read(&first, 11, 0).unwrap(), Some(b"one".to_vec()));
        assert_eq!(recovered.read(&second, 11, 0).unwrap(), Some(b"two".to_vec()));
        let compacted = store.compact().unwrap().unwrap();
        assert_eq!(compacted.max_commit_ts, 11);
        assert_eq!(store.segment_paths().unwrap().len(), 1);
        assert_eq!(store.newest_paths().unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compacted_delta_marker_makes_replacement_crash_safe() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-compact-marker-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let first = RecordKey::new("tenant", "db", "items", b"a");
        let second = RecordKey::new("tenant", "db", "items", b"b");
        let mut snapshot = Engine::new();
        snapshot.apply(first.clone(), 10, Some(b"old-a".to_vec())).unwrap();
        snapshot.apply(second.clone(), 10, Some(b"old-b".to_vec())).unwrap();
        store.write(10, &snapshot).unwrap();
        store
            .write_delta(
                11,
                &[SegmentEntry {
                    key: first.clone(),
                    commit_ts: 11,
                    value: Some(b"new-a".to_vec()),
                    expires_at: 0,
                }],
            )
            .unwrap();
        store
            .write_delta(
                12,
                &[SegmentEntry { key: second.clone(), commit_ts: 12, value: None, expires_at: 0 }],
            )
            .unwrap();

        let mut merged = Engine::new();
        merged.apply(first.clone(), 11, Some(b"new-a".to_vec())).unwrap();
        merged.apply(second.clone(), 12, None).unwrap();
        let compacted =
            ImmutableSegment::from_engine_owned(12, merged, SegmentKind::CompactedDelta);
        store.write_segment(compacted, true, false).unwrap();

        // Before marker publication, an orphan replacement must be ignored.
        let before_marker = SegmentStore::open(&dir).unwrap();
        assert_eq!(before_marker.segment_paths().unwrap().len(), 3);
        let (before, _) = before_marker.load_all().unwrap().unwrap();
        assert_eq!(before.read(&first, 11, 0).unwrap(), Some(b"new-a".to_vec()));
        assert_eq!(before.read(&second, 12, 0).unwrap(), None);

        publish_delta_compaction_marker(
            &dir,
            DeltaCompactionMarker { snapshot_id: 10, through_id: 12, output_id: 12 },
        )
        .unwrap();
        // Simulate a crash after publishing the marker and deleting only part
        // of the replaced inputs.
        std::fs::remove_file(dir.join(segment_name(11))).unwrap();
        let after_marker = SegmentStore::open(&dir).unwrap();
        assert_eq!(after_marker.segment_paths().unwrap().len(), 2);
        let (after, max_commit_ts) = after_marker.load_all().unwrap().unwrap();
        assert_eq!(max_commit_ts, 12);
        assert_eq!(after.read(&first, 10, 0).unwrap(), Some(b"old-a".to_vec()));
        assert_eq!(after.read(&first, 11, 0).unwrap(), Some(b"new-a".to_vec()));
        assert_eq!(after.read(&second, 11, 0).unwrap(), Some(b"old-b".to_vec()));
        assert_eq!(after.read(&second, 12, 0).unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn latest_snapshot_skips_superseded_segments_for_load_and_commit_scan() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-snapshot-floor-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let key = RecordKey::new("tenant", "db", "items", b"row");
        store
            .write_delta(
                1,
                &[SegmentEntry {
                    key: key.clone(),
                    commit_ts: 1,
                    value: Some(b"old".to_vec()),
                    expires_at: 0,
                }],
            )
            .unwrap();
        let mut snapshot = Engine::new();
        snapshot.apply(key.clone(), 1, Some(b"old".to_vec())).unwrap();
        snapshot.apply(key.clone(), 10, Some(b"snapshot".to_vec())).unwrap();
        store.write(10, &snapshot).unwrap();
        store
            .write_delta(
                11,
                &[SegmentEntry {
                    key: key.clone(),
                    commit_ts: 11,
                    value: Some(b"new".to_vec()),
                    expires_at: 0,
                }],
            )
            .unwrap();

        let obsolete = dir.join(segment_name(1));
        let mut bytes = std::fs::read(&obsolete).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        std::fs::write(obsolete, bytes).unwrap();

        assert_eq!(store.max_commit_ts().unwrap(), 11);
        let (recovered, max_commit_ts) = store.load_all().unwrap().unwrap();
        assert_eq!(max_commit_ts, 11);
        assert_eq!(recovered.read(&key, 10, 0).unwrap(), Some(b"snapshot".to_vec()));
        assert_eq!(recovered.read(&key, 11, 0).unwrap(), Some(b"new".to_vec()));

        std::fs::write(dir.join("latest"), b"../outside.sst").unwrap();
        assert!(store.max_commit_ts().is_err());
        assert!(store.load_all().is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_point_and_batch_reads_merge_newest_versions() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-read-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let mut engine = Engine::new();
        let first = RecordKey::new("tenant", "db", "items", b"a");
        let second = RecordKey::new("tenant", "db", "items", b"b");
        engine.apply(first.clone(), 10, Some(b"one".to_vec())).unwrap();
        store.write(10, &engine).unwrap();
        store
            .write_delta(
                11,
                &[SegmentEntry {
                    key: second.clone(),
                    commit_ts: 11,
                    value: Some(b"two".to_vec()),
                    expires_at: 0,
                }],
            )
            .unwrap();
        store
            .write_delta(
                12,
                &[SegmentEntry { key: first.clone(), commit_ts: 12, value: None, expires_at: 0 }],
            )
            .unwrap();
        assert_eq!(store.get(&first, 11, 0).unwrap(), Some(b"one".to_vec()));
        assert_eq!(store.get(&first, 12, 0).unwrap(), None);
        assert_eq!(store.get(&second, 11, 0).unwrap(), Some(b"two".to_vec()));
        let missing = RecordKey::new("tenant", "db", "items", b"missing");
        assert_eq!(
            store
                .get_many(
                    &[
                        second.clone(),
                        first.clone(),
                        missing.clone(),
                        second.clone(),
                        first.clone(),
                    ],
                    12,
                    0,
                )
                .unwrap(),
            vec![Some(b"two".to_vec()), None, None, Some(b"two".to_vec()), None,]
        );
        let repeated = (0..24)
            .map(|index| match index % 3 {
                0 => second.clone(),
                1 => first.clone(),
                _ => missing.clone(),
            })
            .collect::<Vec<_>>();
        let expected =
            repeated.iter().map(|key| store.get(key, 12, 0).unwrap()).collect::<Vec<_>>();
        assert_eq!(store.get_many(&repeated, 12, 0).unwrap(), expected);
        let borrowed = repeated.iter().collect::<Vec<_>>();
        assert_eq!(store.get_many_borrowed(&borrowed, 12, 0).unwrap(), expected);
        store.cache.lock().unwrap().remove(11);
        let before_partial_cache_read = store.cache_stats();
        assert_eq!(
            store.get_many(&[first.clone(), second.clone()], 12, 0).unwrap(),
            vec![None, Some(b"two".to_vec())]
        );
        let after_partial_cache_read = store.cache_stats();
        assert_eq!(after_partial_cache_read.hits - before_partial_cache_read.hits, 1);
        assert_eq!(after_partial_cache_read.misses - before_partial_cache_read.misses, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_batch_read_does_not_touch_older_cached_segments_after_resolution() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-lazy-batch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open_with_cache(&dir, 1024 * 1024).unwrap();
        let key = RecordKey::new("tenant", "db", "items", b"a");
        let mut engine = Engine::new();
        engine.apply(key.clone(), 1, Some(b"old".to_vec())).unwrap();
        store.write(1, &engine).unwrap();
        store
            .write_delta(
                2,
                &[SegmentEntry {
                    key: key.clone(),
                    commit_ts: 2,
                    value: Some(b"new".to_vec()),
                    expires_at: 0,
                }],
            )
            .unwrap();

        assert_eq!(store.get(&key, 1, 0).unwrap(), Some(b"old".to_vec()));
        let before = store.cache_stats();
        assert_eq!(store.get_many(&[key], 2, 0).unwrap(), vec![Some(b"new".to_vec())]);
        let after = store.cache_stats();
        assert_eq!(after.hits - before.hits, 1);
        assert_eq!(after.misses - before.misses, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_scans_only_the_requested_scope_and_cursor() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-scan-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let mut engine = Engine::new();
        for (key, value) in [
            (RecordKey::new("other", "db", "items", b"a"), b"other-tenant".to_vec()),
            (RecordKey::new("tenant", "db", "before", b"a"), b"other-table".to_vec()),
            (RecordKey::new("tenant", "db", "items", b"a"), b"one".to_vec()),
            (RecordKey::new("tenant", "db", "items", b"b"), b"two".to_vec()),
            (RecordKey::new("tenant", "db", "later", b"a"), b"later-table".to_vec()),
        ] {
            engine.apply(key, 10, Some(value)).unwrap();
        }
        store.write(10, &engine).unwrap();

        assert_eq!(
            store.scan("tenant", "db", "items", 10, 0, 10).unwrap(),
            vec![(b"a".to_vec(), b"one".to_vec()), (b"b".to_vec(), b"two".to_vec())]
        );
        assert_eq!(
            store.scan("tenant", "db", "items", 10, 0, 1).unwrap(),
            vec![(b"a".to_vec(), b"one".to_vec())]
        );
        assert_eq!(
            store.scan_after("tenant", "db", "items", 10, 0, b"a", 10).unwrap(),
            vec![(b"b".to_vec(), b"two".to_vec())]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_scan_merges_deltas_in_key_order_and_stops_at_limit() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-scan-merge-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let mut engine = Engine::new();
        for (pk, value) in
            [(b"a".as_slice(), b"A".as_slice()), (b"b", b"B"), (b"c", b"C"), (b"d", b"D")]
        {
            engine
                .apply(RecordKey::new("tenant", "db", "items", pk), 1, Some(value.to_vec()))
                .unwrap();
        }
        store.write(1, &engine).unwrap();
        store
            .write_delta_owned_with_sync(
                2,
                vec![
                    SegmentEntry {
                        key: RecordKey::new("tenant", "db", "items", b"a"),
                        commit_ts: 2,
                        value: None,
                        expires_at: 0,
                    },
                    SegmentEntry {
                        key: RecordKey::new("tenant", "db", "items", b"b"),
                        commit_ts: 2,
                        value: Some(b"B2".to_vec()),
                        expires_at: 0,
                    },
                    SegmentEntry {
                        key: RecordKey::new("tenant", "db", "items", b"c"),
                        commit_ts: 2,
                        value: Some(b"C2".to_vec()),
                        expires_at: 5,
                    },
                    SegmentEntry {
                        key: RecordKey::new("tenant", "db", "items", b"e"),
                        commit_ts: 2,
                        value: Some(b"E".to_vec()),
                        expires_at: 0,
                    },
                ],
                false,
            )
            .unwrap();
        store
            .write_delta_owned_with_sync(
                3,
                vec![SegmentEntry {
                    key: RecordKey::new("tenant", "db", "items", b"b"),
                    commit_ts: 3,
                    value: Some(b"B3".to_vec()),
                    expires_at: 0,
                }],
                false,
            )
            .unwrap();

        assert_eq!(
            store.scan("tenant", "db", "items", 3, 10, 2).unwrap(),
            vec![(b"b".to_vec(), b"B3".to_vec()), (b"d".to_vec(), b"D".to_vec())]
        );
        assert_eq!(
            store.scan_after("tenant", "db", "items", 3, 10, b"b", 2).unwrap(),
            vec![(b"d".to_vec(), b"D".to_vec()), (b"e".to_vec(), b"E".to_vec())]
        );
        assert_eq!(
            store.scan("tenant", "db", "items", 2, 10, 10).unwrap(),
            vec![
                (b"b".to_vec(), b"B2".to_vec()),
                (b"d".to_vec(), b"D".to_vec()),
                (b"e".to_vec(), b"E".to_vec()),
            ]
        );
        assert_eq!(
            store.scan("tenant", "db", "items", 1, 10, 10).unwrap(),
            vec![
                (b"a".to_vec(), b"A".to_vec()),
                (b"b".to_vec(), b"B".to_vec()),
                (b"c".to_vec(), b"C".to_vec()),
                (b"d".to_vec(), b"D".to_vec()),
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scans_remain_valid_while_compaction_replaces_segment_files() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-scan-compaction-race-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let key = RecordKey::new("tenant", "db", "items", b"row");
        let mut engine = Engine::new();
        engine.apply(key.clone(), 1, Some(b"base".to_vec())).unwrap();
        store.write(1, &engine).unwrap();

        let barrier = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let scan_store = store.clone();
            let scan_barrier = Arc::clone(&barrier);
            let scan_thread = scope.spawn(move || {
                scan_barrier.wait();
                for _ in 0..100 {
                    let rows = scan_store.scan("tenant", "db", "items", 100, 0, 10).unwrap();
                    assert_eq!(rows.len(), 1);
                }
            });

            barrier.wait();
            for commit_ts in 2..=32 {
                store
                    .write_delta_owned_with_sync(
                        commit_ts,
                        vec![SegmentEntry {
                            key: key.clone(),
                            commit_ts,
                            value: Some(commit_ts.to_be_bytes().to_vec()),
                            expires_at: 0,
                        }],
                        false,
                    )
                    .unwrap();
                store.compact().unwrap();
            }
            scan_thread.join().unwrap();
        });

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_paths_stay_sorted_when_delta_ids_arrive_out_of_order() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-path-order-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open(&dir).unwrap();
        let key = RecordKey::new("tenant", "db", "items", b"row");
        for commit_ts in [30, 10, 20] {
            store
                .write_delta_owned_with_sync(
                    commit_ts,
                    vec![SegmentEntry {
                        key: key.clone(),
                        commit_ts,
                        value: Some(commit_ts.to_be_bytes().to_vec()),
                        expires_at: 0,
                    }],
                    false,
                )
                .unwrap();
        }

        let paths = store.segment_paths().unwrap();
        let ids = paths
            .iter()
            .map(|path| {
                parse_segment_name(path.file_name().and_then(|name| name.to_str()).unwrap())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![10, 20, 30]);
        assert_eq!(store.get(&key, 30, 0).unwrap(), Some(30u64.to_be_bytes().to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_cache_tracks_hits_and_respects_limit() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open_with_cache(&dir, 1024 * 1024).unwrap();
        let mut engine = Engine::new();
        let key = RecordKey::new("tenant", "db", "items", b"cached");
        engine.apply(key.clone(), 10, Some(b"value".to_vec())).unwrap();
        store.write(10, &engine).unwrap();
        assert_eq!(store.get(&key, 10, 0).unwrap(), Some(b"value".to_vec()));
        assert_eq!(store.get(&key, 10, 0).unwrap(), Some(b"value".to_vec()));
        let stats = store.cache_stats();
        assert!(stats.hits >= 1);
        assert!(stats.misses >= 1);
        assert!(stats.used_bytes <= stats.limit_bytes);
        assert_eq!(stats.entries, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_cache_invalidates_rewritten_segment() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-segment-cache-rewrite-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = SegmentStore::open_with_cache(&dir, 1024 * 1024).unwrap();
        let key = RecordKey::new("tenant", "db", "items", b"cached");
        let mut first = Engine::new();
        first.apply(key.clone(), 1, Some(b"old".to_vec())).unwrap();
        store.write(7, &first).unwrap();
        assert_eq!(store.get(&key, 1, 0).unwrap(), Some(b"old".to_vec()));

        let mut replacement = Engine::new();
        replacement.apply(key.clone(), 2, Some(b"new".to_vec())).unwrap();
        store.write(7, &replacement).unwrap();
        assert_eq!(store.get(&key, 2, 0).unwrap(), Some(b"new".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_cache_touch_preserves_lru_and_bounds_lazy_queue() {
        let segment = Arc::new(ImmutableSegment::from_engine(1, &Engine::new()));
        let mut cache = SegmentReadCache::new(2);
        cache.insert(1, Arc::clone(&segment), 1);
        cache.insert(2, Arc::clone(&segment), 1);

        assert!(cache.get(1).is_some());
        cache.insert(3, Arc::clone(&segment), 1);
        assert!(cache.entries.contains_key(&1));
        assert!(!cache.entries.contains_key(&2));
        assert!(cache.entries.contains_key(&3));
        assert_eq!(cache.used_bytes, 2);

        for _ in 0..100 {
            assert!(cache.get(1).is_some());
        }
        let queue_limit = cache.entries.len().saturating_mul(2).saturating_add(32);
        assert!(cache.order.len() <= queue_limit);
    }

    #[test]
    fn segment_cache_bulk_lookup_returns_partial_hits_and_updates_lru() {
        let segment = Arc::new(ImmutableSegment::from_engine(1, &Engine::new()));
        let mut cache = SegmentReadCache::new(3);
        cache.insert(1, Arc::clone(&segment), 1);
        cache.insert(2, Arc::clone(&segment), 1);
        cache.insert(3, Arc::clone(&segment), 1);

        let partial = cache.get_many_cached(&[1, 4, 3, 1]);
        assert!(partial[0].is_some());
        assert!(partial[1].is_none());
        assert!(partial[2].is_some());
        assert!(partial[3].is_some());
        assert_eq!(cache.hits, 3);
        assert_eq!(cache.misses, 0);

        cache.insert(4, segment, 1);
        assert!(cache.entries.contains_key(&1));
        assert!(!cache.entries.contains_key(&2));
        assert!(cache.entries.contains_key(&3));
        assert!(cache.entries.contains_key(&4));
    }
}
