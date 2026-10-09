use super::{Engine, RecordKey};
use ryme_error::{Result, RymeError};
use std::path::{Path, PathBuf};

const SEGMENT_MAGIC: u32 = 0x5259_5347;
const SEGMENT_VERSION: u16 = 1;
const INDEX_STRIDE: usize = 64;
const BLOOM_BITS_PER_KEY: usize = 8;
const MIN_BLOOM_BYTES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentKind {
    Snapshot = 0,
    Delta = 1,
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
        Self {
            meta,
            kind: SegmentKind::Snapshot,
            engine: engine.clone(),
            keys,
            sparse_index,
            bloom,
        }
    }

    pub fn from_entries(id: u64, entries: &[SegmentEntry]) -> Result<Self> {
        let mut engine = Engine::new();
        let mut ordered = entries.to_vec();
        ordered.sort_by(|left, right| {
            left.key.cmp(&right.key).then(left.commit_ts.cmp(&right.commit_ts))
        });
        for entry in ordered {
            engine.apply_with_expiry(entry.key, entry.commit_ts, entry.value, entry.expires_at)?;
        }
        let mut segment = Self::from_engine(id, &engine);
        segment.kind = SegmentKind::Delta;
        Ok(segment)
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
        if !self.bloom_might_contain(key) || self.keys.binary_search(key).is_err() {
            return Ok(None);
        }
        self.engine.read(key, read_ts, now)
    }

    fn version_at(&self, key: &RecordKey, read_ts: u64) -> Option<(u64, Option<Vec<u8>>, u64)> {
        if !self.bloom_might_contain(key) || self.keys.binary_search(key).is_err() {
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

    pub fn encode(&self) -> Result<Vec<u8>> {
        let payload = self.engine.encode_snapshot()?;
        let mut out = Vec::with_capacity(4 + 2 + 2 + 8 + 4 + payload.len() + 4);
        out.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());
        out.extend_from_slice(&SEGMENT_VERSION.to_be_bytes());
        out.extend_from_slice(&(self.kind as u16).to_be_bytes());
        out.extend_from_slice(&self.meta.id.to_be_bytes());
        out.extend_from_slice(
            &(u32::try_from(payload.len())
                .map_err(|_| RymeError::InvalidArgument(String::from("segment size")))?)
            .to_be_bytes(),
        );
        out.extend_from_slice(&payload);
        out.extend_from_slice(&(self.bloom.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.bloom);
        out.extend_from_slice(&(self.sparse_index.len() as u32).to_be_bytes());
        for (key, ordinal) in &self.sparse_index {
            push_key(&mut out, key)?;
            out.extend_from_slice(
                &u32::try_from(*ordinal)
                    .map_err(|_| RymeError::InvalidArgument(String::from("segment index")))?
                    .to_be_bytes(),
            );
        }
        let checksum = crc32fast::hash(&out);
        out.extend_from_slice(&checksum.to_be_bytes());
        Ok(out)
    }

    pub fn decode(input: &[u8]) -> Result<Self> {
        const HEADER: usize = 4 + 2 + 2 + 8 + 4;
        if input.len() < HEADER + 4 {
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
        let mut cursor = &input[..];
        let magic = take_u32(&mut cursor)?;
        let version = take_u16(&mut cursor)?;
        let kind = match take_u16(&mut cursor)? {
            0 => SegmentKind::Snapshot,
            1 => SegmentKind::Delta,
            _ => return Err(RymeError::Corrupt(String::from("segment kind"))),
        };
        let id = take_u64(&mut cursor)?;
        let payload_len = take_u32(&mut cursor)? as usize;
        if magic != SEGMENT_MAGIC || version != SEGMENT_VERSION {
            return Err(RymeError::Corrupt(String::from("segment magic")));
        }
        if cursor.len() < payload_len + 4 {
            return Err(RymeError::Corrupt(String::from("segment length")));
        }
        let payload = &cursor[..payload_len];
        cursor = &cursor[payload_len..];
        let engine = Engine::decode_snapshot(payload)?;
        let mut segment = Self::from_engine(id, &engine);
        segment.kind = kind;
        if segment.meta.max_commit_ts != engine.max_commit_ts() {
            return Err(RymeError::Corrupt(String::from("segment commit")));
        }
        let bloom_len = take_u32(&mut cursor)? as usize;
        if bloom_len > cursor.len().saturating_sub(4) {
            return Err(RymeError::Corrupt(String::from("segment bloom")));
        }
        let bloom = cursor[..bloom_len].to_vec();
        cursor = &cursor[bloom_len..];
        let index_count = take_u32(&mut cursor)? as usize;
        if index_count > engine.len() {
            return Err(RymeError::Corrupt(String::from("segment index")));
        }
        let mut sparse_index = Vec::with_capacity(index_count);
        for _ in 0..index_count {
            let key = take_key(&mut cursor)?;
            let ordinal = take_u32(&mut cursor)? as usize;
            sparse_index.push((key, ordinal));
        }
        if cursor.len() != 4
            || bloom != segment.bloom
            || sparse_index != segment.sparse_index
            || kind != segment.kind
        {
            return Err(RymeError::Corrupt(String::from("segment metadata")));
        }
        Ok(segment)
    }
}

#[derive(Debug, Clone)]
pub struct SegmentStore {
    dir: PathBuf,
}

impl SegmentStore {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self { dir: dir.to_path_buf() })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn write(&self, id: u64, engine: &Engine) -> Result<(SegmentMeta, PathBuf)> {
        let segment = ImmutableSegment::from_engine(id, engine);
        self.write_segment(segment)
    }

    pub fn write_delta(&self, id: u64, entries: &[SegmentEntry]) -> Result<SegmentMeta> {
        if entries.is_empty() {
            return Ok(SegmentMeta { id, max_commit_ts: 0, entries: 0, bytes: 0 });
        }
        let segment = ImmutableSegment::from_entries(id, entries)?;
        let (meta, _) = self.write_segment(segment)?;
        Ok(meta)
    }

    fn write_segment(&self, segment: ImmutableSegment) -> Result<(SegmentMeta, PathBuf)> {
        let id = segment.meta.id;
        let path = self.dir.join(segment_name(id));
        let temporary =
            self.dir.join(format!("{segment_name}.tmp", segment_name = segment_name(id)));
        let encoded = segment.encode()?;
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(&encoded)?;
            file.sync_data()?;
        }
        std::fs::rename(&temporary, &path)?;
        let latest = self.dir.join("latest");
        let latest_tmp = self.dir.join("latest.tmp");
        std::fs::write(&latest_tmp, path.file_name().and_then(|name| name.to_str()).unwrap_or(""))?;
        std::fs::rename(latest_tmp, latest)?;
        Ok((segment.meta.clone(), path))
    }

    pub fn latest(&self) -> Result<Option<ImmutableSegment>> {
        let Some(path) = self.latest_path()? else {
            return Ok(None);
        };
        let bytes = std::fs::read(path)?;
        Ok(Some(ImmutableSegment::decode(&bytes)?))
    }

    pub fn get(&self, key: &RecordKey, read_ts: u64, now: u64) -> Result<Option<Vec<u8>>> {
        let mut paths = self.segment_paths()?;
        paths.sort_by_key(|path| {
            std::cmp::Reverse(parse_segment_name(
                path.file_name().and_then(|name| name.to_str()).unwrap_or(""),
            ))
        });
        for path in paths {
            let segment = ImmutableSegment::decode(&std::fs::read(path)?)?;
            let Some((_, value, expires_at)) = segment.version_at(key, read_ts) else {
                continue;
            };
            if expires_at != 0 && expires_at <= now {
                return Ok(None);
            }
            return Ok(value);
        }
        Ok(None)
    }

    pub fn load_all(&self) -> Result<Option<(Engine, u64)>> {
        let mut paths = self.segment_paths()?;
        paths.sort_by_key(|path| {
            parse_segment_name(path.file_name().and_then(|n| n.to_str()).unwrap_or(""))
        });
        let mut engine = Engine::new();
        let mut found = false;
        let mut max_commit_ts = 0;
        for path in paths {
            let segment = ImmutableSegment::decode(&std::fs::read(path)?)?;
            if segment.is_snapshot() {
                engine = segment.into_engine();
            } else {
                segment.merge_into(&mut engine)?;
            }
            max_commit_ts = max_commit_ts.max(engine.max_commit_ts());
            found = true;
        }
        Ok(found.then_some((engine, max_commit_ts)))
    }

    pub fn latest_path(&self) -> Result<Option<PathBuf>> {
        let pointer = self.dir.join("latest");
        if let Ok(raw) = std::fs::read(&pointer) {
            let name = String::from_utf8(raw)
                .map_err(|_| RymeError::Corrupt(String::from("segment pointer")))?;
            let name = name.trim();
            if !name.is_empty() && parse_segment_name(name).is_some() && !name.contains('/') {
                let path = self.dir.join(name);
                if path.is_file() {
                    return Ok(Some(path));
                }
            }
            return Err(RymeError::Corrupt(String::from("segment pointer")));
        }
        let mut paths = self.segment_paths()?;
        paths.sort_by_key(|path| {
            parse_segment_name(path.file_name().and_then(|n| n.to_str()).unwrap_or(""))
        });
        Ok(paths.pop())
    }

    pub fn segment_paths(&self) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        let entries = match std::fs::read_dir(&self.dir) {
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
        Ok(paths)
    }

    pub fn compact(&self) -> Result<Option<SegmentMeta>> {
        let Some((engine, max_commit_ts)) = self.load_all()? else {
            return Ok(None);
        };
        let (meta, newest) = self.write(max_commit_ts, &engine)?;
        for path in self.segment_paths()? {
            if path != newest {
                std::fs::remove_file(path)?;
            }
        }
        Ok(Some(meta))
    }

    pub fn prune(&self, keep: usize) -> Result<usize> {
        let mut segments = Vec::new();
        for path in self.segment_paths()? {
            let segment = ImmutableSegment::decode(&std::fs::read(&path)?)?;
            segments.push((segment.meta.id, path, segment.is_snapshot()));
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
            keep_paths
                .extend(segments.iter().rev().take(keep.max(1)).map(|(_, path, _)| path.clone()));
        }
        let mut removed = 0;
        for (_, path, _) in segments {
            if keep_paths.contains(&path) {
                continue;
            }
            if std::fs::remove_file(path).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

fn segment_name(id: u64) -> String {
    format!("segment-{id:020}.sst")
}

fn parse_segment_name(name: &str) -> Option<u64> {
    name.strip_prefix("segment-")?.strip_suffix(".sst")?.parse().ok()
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

fn push_key(out: &mut Vec<u8>, key: &RecordKey) -> Result<()> {
    for field in [key.tenant.as_bytes(), key.database.as_bytes(), key.table.as_bytes()] {
        if field.len() > u16::MAX as usize {
            return Err(RymeError::InvalidArgument(String::from("segment key")));
        }
        out.extend_from_slice(&(field.len() as u16).to_be_bytes());
        out.extend_from_slice(field);
    }
    if key.pk.len() > u32::MAX as usize {
        return Err(RymeError::InvalidArgument(String::from("segment key")));
    }
    out.extend_from_slice(&(key.pk.len() as u32).to_be_bytes());
    out.extend_from_slice(&key.pk);
    Ok(())
}

fn take_key(cursor: &mut &[u8]) -> Result<RecordKey> {
    let tenant = String::from_utf8(take_field(cursor)?.to_vec())
        .map_err(|_| RymeError::Corrupt(String::from("segment key")))?;
    let database = String::from_utf8(take_field(cursor)?.to_vec())
        .map_err(|_| RymeError::Corrupt(String::from("segment key")))?;
    let table = String::from_utf8(take_field(cursor)?.to_vec())
        .map_err(|_| RymeError::Corrupt(String::from("segment key")))?;
    let pk_len = take_u32(cursor)? as usize;
    if pk_len > cursor.len() {
        return Err(RymeError::Corrupt(String::from("segment key")));
    }
    let pk = cursor[..pk_len].to_vec();
    *cursor = &cursor[pk_len..];
    Ok(RecordKey::new(&tenant, &database, &table, &pk))
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
        let restored = ImmutableSegment::decode(&segment.encode().unwrap()).unwrap();
        assert_eq!(restored.get(&first, 10, 0).unwrap(), Some(b"one".to_vec()));
        assert_eq!(restored.get(&second, 11, 0).unwrap(), Some(b"two".to_vec()));
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
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_store_point_reads_merge_newest_versions() {
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
        let _ = std::fs::remove_dir_all(&dir);
    }
}
