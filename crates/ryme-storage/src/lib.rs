use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Bound;

mod segment;
pub use segment::{ImmutableSegment, SegmentCacheStats, SegmentEntry, SegmentMeta, SegmentStore};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageMode {
    #[default]
    Hot,
    Standard,
}

#[derive(Debug, Clone)]
pub struct TableVersion {
    pub commit_ts: u64,
    pub value: Option<Vec<u8>>,
    pub expires_at: u64,
}

pub type TableRows = Vec<(Vec<u8>, Vec<TableVersion>)>;

/// Routing-key formats are part of persisted range topology. Keep the legacy
/// format readable so an existing deployment can restart, while new
/// topologies include the isolation scopes that precede every record.
pub const LEGACY_ROUTING_VERSION: u8 = 0;
pub const SCOPED_ROUTING_VERSION: u8 = 1;
pub const CURRENT_ROUTING_VERSION: u8 = SCOPED_ROUTING_VERSION;
pub const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;

pub fn validate_routing_version(version: u8) -> Result<()> {
    match version {
        LEGACY_ROUTING_VERSION | SCOPED_ROUTING_VERSION => Ok(()),
        _ => Err(RymeError::Corrupt(format!("unsupported routing version {version}"))),
    }
}

pub fn routing_key(
    version: u8,
    tenant: &str,
    database: &str,
    table: &str,
    pk: &[u8],
) -> Result<Vec<u8>> {
    validate_routing_version(version)?;
    let scoped_components = if version == SCOPED_ROUTING_VERSION {
        [tenant, database, table].as_slice()
    } else {
        [table].as_slice()
    };
    if scoped_components.iter().any(|component| component.as_bytes().contains(&0)) {
        return Err(RymeError::InvalidArgument(String::from("routing component")));
    }
    let mut out = Vec::with_capacity(
        tenant.len() + database.len() + table.len() + pk.len() + if version == 0 { 1 } else { 3 },
    );
    if version == SCOPED_ROUTING_VERSION {
        out.extend_from_slice(tenant.as_bytes());
        out.push(0);
        out.extend_from_slice(database.as_bytes());
        out.push(0);
    }
    out.extend_from_slice(table.as_bytes());
    out.push(0);
    out.extend_from_slice(pk);
    Ok(out)
}

pub fn record_routing_key(version: u8, key: &RecordKey) -> Result<Vec<u8>> {
    routing_key(version, &key.tenant, &key.database, &key.table, &key.pk)
}

pub fn table_routing_bounds(
    version: u8,
    tenant: &str,
    database: &str,
    table: &str,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let start = routing_key(version, tenant, database, table, &[])?;
    let mut end = start.clone();
    let last =
        end.last_mut().ok_or_else(|| RymeError::Corrupt(String::from("empty routing prefix")))?;
    *last = last
        .checked_add(1)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("routing prefix")))?;
    Ok((start, end))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordKey {
    pub tenant: String,
    pub database: String,
    pub table: String,
    pub pk: Vec<u8>,
}

impl RecordKey {
    pub fn new(tenant: &str, database: &str, table: &str, pk: &[u8]) -> Self {
        Self {
            tenant: tenant.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            pk: pk.to_vec(),
        }
    }

    pub fn prefix(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(self.tenant.as_bytes());
        out.push(0);
        out.extend_from_slice(self.database.as_bytes());
        out.push(0);
        out.extend_from_slice(self.table.as_bytes());
        out.push(0);
        out
    }
}

#[derive(Debug, Clone)]
struct Version {
    commit_ts: u64,
    value: Option<Vec<u8>>,
    expires_at: u64,
}

#[derive(Debug, Default, Clone)]
pub struct Engine {
    inner: BTreeMap<RecordKey, Vec<Version>>,
    bytes_held: u64,
}

impl Engine {
    pub fn new() -> Self {
        Self { inner: BTreeMap::new(), bytes_held: 0 }
    }

    pub fn bytes_held(&self) -> u64 {
        self.bytes_held
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub(crate) fn record_keys(&self) -> Vec<RecordKey> {
        self.inner.keys().cloned().collect()
    }

    pub fn version_at(&self, key: &RecordKey, read_ts: u64) -> Option<(u64, Option<Vec<u8>>, u64)> {
        self.inner.get(key).and_then(|versions| {
            versions
                .get(Self::version_index_at(versions, read_ts)?)
                .map(|version| (version.commit_ts, version.value.clone(), version.expires_at))
        })
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn apply(&mut self, key: RecordKey, commit_ts: u64, value: Option<Vec<u8>>) -> Result<()> {
        self.apply_with_expiry(key, commit_ts, value, 0)
    }

    pub fn apply_with_expiry(
        &mut self,
        key: RecordKey,
        commit_ts: u64,
        value: Option<Vec<u8>>,
        expires_at: u64,
    ) -> Result<()> {
        if commit_ts == 0 {
            return Err(RymeError::InvalidArgument(String::from("commit_ts")));
        }
        let versions = self.inner.entry(key).or_default();
        if let Some(last) = versions.last() {
            if commit_ts <= last.commit_ts {
                return Err(RymeError::Conflict(String::from("stale commit_ts")));
            }
        }
        if let Some(v) = value.as_ref() {
            self.bytes_held += v.len() as u64;
        }
        versions.push(Version { commit_ts, value, expires_at });
        Ok(())
    }

    fn visible(version: &Version, now: u64) -> Option<Vec<u8>> {
        if version.expires_at != 0 && version.expires_at <= now {
            return None;
        }
        version.value.clone()
    }

    fn version_index_at(versions: &[Version], read_ts: u64) -> Option<usize> {
        versions.partition_point(|version| version.commit_ts <= read_ts).checked_sub(1)
    }

    pub fn read(&self, key: &RecordKey, read_ts: u64, now: u64) -> Result<Option<Vec<u8>>> {
        let Some(versions) = self.inner.get(key) else {
            return Ok(None);
        };
        Ok(Self::version_index_at(versions, read_ts)
            .and_then(|index| versions.get(index))
            .filter(|version| version.value.is_some())
            .and_then(|version| Self::visible(version, now)))
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
        self.scan_from(tenant, database, table, read_ts, now, None, limit)
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
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_from(tenant, database, table, read_ts, now, Some(start_after), limit)
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_from(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        read_ts: u64,
        now: u64,
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }

        let start = RecordKey::new(tenant, database, table, start_after.unwrap_or_default());
        let start_bound =
            if start_after.is_some() { Bound::Excluded(&start) } else { Bound::Included(&start) };
        let mut out = Vec::with_capacity(limit.min(1024));
        for (key, versions) in self.inner.range((start_bound, Bound::Unbounded)) {
            if key.tenant != tenant || key.database != database || key.table != table {
                break;
            }
            if let Some(value) = Self::version_index_at(versions, read_ts)
                .and_then(|index| versions.get(index))
                .filter(|version| version.value.is_some())
                .and_then(|version| Self::visible(version, now))
            {
                out.push((key.pk.clone(), value));
                if out.len() >= limit {
                    break;
                }
            }
        }
        out
    }
    pub fn table_names(&self, tenant: &str, database: &str) -> Vec<String> {
        let mut tables = std::collections::BTreeSet::new();
        for key in self.inner.keys() {
            if key.tenant == tenant && key.database == database {
                tables.insert(key.table.clone());
            }
        }
        tables.into_iter().collect()
    }

    pub fn table_bytes(&self, tenant: &str, database: &str, table: &str) -> u64 {
        let mut bytes = 0u64;
        for (key, versions) in &self.inner {
            if key.tenant == tenant && key.database == database && key.table == table {
                bytes += key.pk.len() as u64;
                for version in versions {
                    if let Some(value) = version.value.as_ref() {
                        bytes += value.len() as u64;
                    }
                }
            }
        }
        bytes
    }

    pub fn export_table(&self, tenant: &str, database: &str, table: &str) -> TableRows {
        let mut out = Vec::new();
        for (key, versions) in &self.inner {
            if key.tenant == tenant && key.database == database && key.table == table {
                out.push((
                    key.pk.clone(),
                    versions
                        .iter()
                        .map(|version| TableVersion {
                            commit_ts: version.commit_ts,
                            value: version.value.clone(),
                            expires_at: version.expires_at,
                        })
                        .collect(),
                ));
            }
        }
        out
    }

    pub fn import_table(
        &mut self,
        tenant: &str,
        database: &str,
        table: &str,
        rows: TableRows,
    ) -> Result<u64> {
        // Validate every key's incoming history before changing the engine. A
        // malformed later row must not leave an earlier part of the import
        // visible to readers.
        let mut histories: BTreeMap<RecordKey, (Option<u64>, BTreeMap<u64, (Option<&[u8]>, u64)>)> =
            BTreeMap::new();
        for (pk, versions) in &rows {
            let key = RecordKey {
                tenant: tenant.to_string(),
                database: database.to_string(),
                table: table.to_string(),
                pk: pk.clone(),
            };
            let (last, known) = histories.entry(key.clone()).or_insert_with(|| {
                let existing = self.inner.get(&key);
                (
                    existing.and_then(|versions| versions.last().map(|version| version.commit_ts)),
                    BTreeMap::new(),
                )
            });
            for version in versions {
                let incoming_value = version.value.as_deref();
                if let Some((known_value, known_expiry)) = known.get(&version.commit_ts) {
                    if *known_value != incoming_value || *known_expiry != version.expires_at {
                        return Err(RymeError::Corrupt(String::from("version conflict")));
                    }
                    continue;
                }
                if let Some(existing) = self.inner.get(&key).and_then(|history| {
                    history.iter().find(|entry| entry.commit_ts == version.commit_ts)
                }) {
                    if existing.value.as_deref() != incoming_value
                        || existing.expires_at != version.expires_at
                    {
                        return Err(RymeError::Corrupt(String::from("version conflict")));
                    }
                    known.insert(version.commit_ts, (incoming_value, version.expires_at));
                    continue;
                }
                if last.is_some_and(|last| version.commit_ts < last) {
                    return Err(RymeError::Corrupt(String::from("version order")));
                }
                *last = Some(version.commit_ts);
                known.insert(version.commit_ts, (incoming_value, version.expires_at));
            }
        }

        let mut max = 0u64;
        for (pk, versions) in rows {
            let key = RecordKey {
                tenant: tenant.to_string(),
                database: database.to_string(),
                table: table.to_string(),
                pk,
            };
            let entry = self.inner.entry(key).or_default();
            for version in versions {
                max = max.max(version.commit_ts);
                if entry.iter().any(|existing: &Version| existing.commit_ts == version.commit_ts) {
                    continue;
                }
                if let Some(last) = entry.last() {
                    if version.commit_ts < last.commit_ts {
                        return Err(RymeError::Corrupt(String::from("version order")));
                    }
                }
                if let Some(value) = version.value.as_ref() {
                    self.bytes_held += value.len() as u64;
                }
                entry.push(Version {
                    commit_ts: version.commit_ts,
                    value: version.value,
                    expires_at: version.expires_at,
                });
            }
        }
        Ok(max)
    }

    pub fn drop_table(&mut self, tenant: &str, database: &str, table: &str) -> usize {
        let before = self.inner.len();
        let mut reclaimed = 0u64;
        self.inner.retain(|key, versions| {
            let keep = key.tenant != tenant || key.database != database || key.table != table;
            if !keep {
                reclaimed = reclaimed.saturating_add(version_payload_bytes(versions));
            }
            keep
        });
        self.bytes_held = self.bytes_held.saturating_sub(reclaimed);
        before - self.inner.len()
    }

    pub fn purge_keys(&mut self, keys: &[RecordKey]) -> usize {
        let mut removed = 0;
        let mut reclaimed = 0u64;
        for key in keys {
            if let Some(versions) = self.inner.remove(key) {
                removed += 1;
                reclaimed = reclaimed.saturating_add(version_payload_bytes(&versions));
            }
        }
        self.bytes_held = self.bytes_held.saturating_sub(reclaimed);
        removed
    }

    pub fn replace_routing_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        replacement: &Engine,
    ) -> usize {
        self.replace_routing_range_for_version(LEGACY_ROUTING_VERSION, start, end, replacement)
            .unwrap_or_default()
    }

    pub fn replace_routing_range_for_version(
        &mut self,
        version: u8,
        start: &[u8],
        end: &[u8],
        replacement: &Engine,
    ) -> Result<usize> {
        validate_routing_version(version)?;
        let in_range = |key: &RecordKey| -> Result<bool> {
            let routing = record_routing_key(version, key)?;
            Ok(routing.as_slice() >= start && (end.is_empty() || routing.as_slice() < end))
        };
        let keys: Vec<RecordKey> = self
            .inner
            .keys()
            .map(|key| in_range(key).map(|matched| (key, matched)))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter_map(|(key, matched)| matched.then(|| key.clone()))
            .collect();
        let replacement_rows: Vec<_> = replacement
            .inner
            .iter()
            .map(|(key, versions)| in_range(key).map(|matched| (key, versions, matched)))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter_map(|(key, versions, matched)| matched.then_some((key, versions)))
            .collect();
        let removed = self.purge_keys(&keys);
        for (key, versions) in replacement_rows {
            self.bytes_held += versions
                .iter()
                .filter_map(|version| version.value.as_ref())
                .map(|value| value.len() as u64)
                .sum::<u64>();
            self.inner.insert(key.clone(), versions.clone());
        }
        Ok(removed)
    }

    pub fn spaces(&self) -> Vec<(String, String, String)> {
        let mut spaces = std::collections::BTreeSet::new();
        for key in self.inner.keys() {
            spaces.insert((key.tenant.clone(), key.database.clone(), key.table.clone()));
        }
        spaces.into_iter().collect()
    }

    pub fn expires_at(&self, key: &RecordKey, read_ts: u64) -> Option<u64> {
        let versions = self.inner.get(key)?;
        Self::version_index_at(versions, read_ts)
            .and_then(|index| versions.get(index))
            .filter(|version| version.value.is_some())
            .map(|version| version.expires_at)
    }

    pub fn exact(&self, key: &RecordKey, commit_ts: u64) -> Option<(Option<Vec<u8>>, u64)> {
        let versions = self.inner.get(key)?;
        versions
            .iter()
            .find(|version| version.commit_ts == commit_ts)
            .map(|version| (version.value.clone(), version.expires_at))
    }

    pub fn latest_version(&self, key: &RecordKey) -> Option<(u64, Option<Vec<u8>>, u64)> {
        self.inner
            .get(key)?
            .last()
            .map(|version| (version.commit_ts, version.value.clone(), version.expires_at))
    }

    pub fn expired(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        read_ts: u64,
        now: u64,
        limit: usize,
    ) -> Vec<Vec<u8>> {
        if limit == 0 {
            return Vec::new();
        }
        let start = RecordKey::new(tenant, database, table, &[]);
        let mut out = Vec::new();
        for (key, versions) in self.inner.range((Bound::Included(&start), Bound::Unbounded)) {
            if key.tenant != tenant || key.database != database || key.table != table {
                break;
            }
            if let Some(index) = Self::version_index_at(versions, read_ts) {
                let version = &versions[index];
                if version.value.is_some() && version.expires_at != 0 && version.expires_at <= now {
                    out.push(key.pk.clone());
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        out
    }

    pub fn range(
        &self,
        start: &RecordKey,
        end: &RecordKey,
        read_ts: u64,
        now: u64,
        limit: usize,
    ) -> Vec<(RecordKey, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }
        let mut out = Vec::new();
        let iter = self.inner.range((Bound::Included(start), Bound::Excluded(end)));
        for (key, versions) in iter {
            if let Some(value) = Self::version_index_at(versions, read_ts)
                .and_then(|index| versions.get(index))
                .filter(|version| version.value.is_some())
                .and_then(|version| Self::visible(version, now))
            {
                out.push((key.clone(), value));
                if out.len() >= limit {
                    break;
                }
            }
        }
        out
    }

    pub fn gc(&mut self, retain_ts: u64) {
        for versions in self.inner.values_mut() {
            let mut keep_from = 0;
            for (index, version) in versions.iter().enumerate() {
                if version.commit_ts <= retain_ts {
                    keep_from = index;
                } else {
                    break;
                }
            }
            if keep_from > 0 {
                let reclaimed = versions
                    .drain(0..keep_from)
                    .filter_map(|version| version.value)
                    .map(|value| value.len() as u64)
                    .sum::<u64>();
                self.bytes_held = self.bytes_held.saturating_sub(reclaimed);
            }
        }
    }

    pub fn max_commit_ts(&self) -> u64 {
        self.inner
            .values()
            .filter_map(|versions| versions.last().map(|v| v.commit_ts))
            .max()
            .unwrap_or(0)
    }

    pub fn encode_snapshot(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&0x52594D53u32.to_be_bytes());
        out.extend_from_slice(&2u16.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&self.max_commit_ts().to_be_bytes());
        let count = u32::try_from(self.inner.len())
            .map_err(|_| RymeError::InvalidArgument(String::from("snapshot size")))?;
        out.extend_from_slice(&count.to_be_bytes());
        for (key, versions) in &self.inner {
            push_field(&mut out, key.tenant.as_bytes())?;
            push_field(&mut out, key.database.as_bytes())?;
            push_field(&mut out, key.table.as_bytes())?;
            push_chunk(&mut out, &key.pk)?;
            let vcount = u32::try_from(versions.len())
                .map_err(|_| RymeError::InvalidArgument(String::from("snapshot size")))?;
            out.extend_from_slice(&vcount.to_be_bytes());
            for version in versions {
                out.extend_from_slice(&version.commit_ts.to_be_bytes());
                match version.value.as_ref() {
                    Some(value) => {
                        out.push(0);
                        push_chunk(&mut out, value)?;
                    }
                    None => {
                        out.push(1);
                    }
                }
                out.extend_from_slice(&version.expires_at.to_be_bytes());
            }
        }
        if out.len() > MAX_SNAPSHOT_BYTES.saturating_sub(4) {
            return Err(RymeError::Overload(String::from("snapshot")));
        }
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_be_bytes());
        Ok(out)
    }

    pub fn decode_snapshot(input: &[u8]) -> Result<Self> {
        if input.len() > MAX_SNAPSHOT_BYTES {
            return Err(RymeError::Overload(String::from("snapshot")));
        }
        if input.len() < 24 {
            return Err(RymeError::Corrupt(String::from("snapshot header")));
        }
        let stored = u32::from_be_bytes([
            input[input.len() - 4],
            input[input.len() - 3],
            input[input.len() - 2],
            input[input.len() - 1],
        ]);
        if crc32fast::hash(&input[..input.len() - 4]) != stored {
            return Err(RymeError::Corrupt(String::from("snapshot checksum")));
        }
        let body = &input[..input.len() - 4];
        let mut cursor = body;
        let magic = take_u32(&mut cursor)?;
        let version = take_u16(&mut cursor)?;
        let _reserved = take_u16(&mut cursor)?;
        if magic != 0x52594D53 || (version != 1 && version != 2) {
            return Err(RymeError::Corrupt(String::from("snapshot magic")));
        }
        let declared_max = take_u64(&mut cursor)?;
        let count = take_u32(&mut cursor)? as usize;
        if count > 10_000_000 {
            return Err(RymeError::Corrupt(String::from("snapshot count")));
        }
        let mut engine = Engine::new();
        let mut actual_max = 0u64;
        for _ in 0..count {
            let tenant = String::from_utf8(take_field(&mut cursor)?.to_vec())
                .map_err(|_| RymeError::Corrupt(String::from("snapshot tenant")))?;
            let database = String::from_utf8(take_field(&mut cursor)?.to_vec())
                .map_err(|_| RymeError::Corrupt(String::from("snapshot database")))?;
            let table = String::from_utf8(take_field(&mut cursor)?.to_vec())
                .map_err(|_| RymeError::Corrupt(String::from("snapshot table")))?;
            let pk = take_chunk(&mut cursor)?.to_vec();
            let vcount = take_u32(&mut cursor)? as usize;
            if vcount > 1_000_000 {
                return Err(RymeError::Corrupt(String::from("snapshot versions")));
            }
            let mut versions = Vec::with_capacity(vcount.min(64));
            let mut previous_commit_ts = None;
            for _ in 0..vcount {
                let commit_ts = take_u64(&mut cursor)?;
                if commit_ts == 0
                    || previous_commit_ts.is_some_and(|previous| commit_ts <= previous)
                {
                    return Err(RymeError::Corrupt(String::from("snapshot version order")));
                }
                previous_commit_ts = Some(commit_ts);
                actual_max = actual_max.max(commit_ts);
                let tombstone = take_u8(&mut cursor)?;
                let value = if tombstone == 1 {
                    None
                } else if tombstone == 0 {
                    Some(take_chunk(&mut cursor)?.to_vec())
                } else {
                    return Err(RymeError::Corrupt(String::from("snapshot tombstone")));
                };
                let expires_at = if version == 2 { take_u64(&mut cursor)? } else { 0 };
                versions.push(Version { commit_ts, value, expires_at });
            }
            let key = RecordKey { tenant, database, table, pk };
            if engine.inner.contains_key(&key) {
                return Err(RymeError::Corrupt(String::from("snapshot duplicate key")));
            }
            engine.bytes_held += versions
                .iter()
                .filter_map(|v| v.value.as_ref())
                .map(|v| v.len() as u64)
                .sum::<u64>();
            engine.inner.insert(key, versions);
        }
        if !cursor.is_empty() {
            return Err(RymeError::Corrupt(String::from("snapshot trailing")));
        }
        if actual_max != declared_max {
            return Err(RymeError::Corrupt(String::from("snapshot commit timestamp")));
        }
        Ok(engine)
    }

    pub fn replace_from(&mut self, other: Engine) {
        self.inner = other.inner;
        self.bytes_held = other.bytes_held;
    }

    pub fn clear(&mut self) {
        self.inner.clear();
        self.bytes_held = 0;
    }
}

fn version_payload_bytes(versions: &[Version]) -> u64 {
    versions
        .iter()
        .filter_map(|version| version.value.as_ref())
        .fold(0u64, |total, value| total.saturating_add(value.len() as u64))
}

fn push_field(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if bytes.len() > u16::MAX as usize {
        return Err(RymeError::InvalidArgument(String::from("snapshot field")));
    }
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn push_chunk(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if bytes.len() > 8 * 1024 * 1024 {
        return Err(RymeError::InvalidArgument(String::from("snapshot chunk")));
    }
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn take_u8(cursor: &mut &[u8]) -> Result<u8> {
    if cursor.is_empty() {
        return Err(RymeError::Corrupt(String::from("snapshot truncated")));
    }
    let value = cursor[0];
    *cursor = &cursor[1..];
    Ok(value)
}

fn take_u16(cursor: &mut &[u8]) -> Result<u16> {
    if cursor.len() < 2 {
        return Err(RymeError::Corrupt(String::from("snapshot truncated")));
    }
    let value = u16::from_be_bytes([cursor[0], cursor[1]]);
    *cursor = &cursor[2..];
    Ok(value)
}

fn take_u32(cursor: &mut &[u8]) -> Result<u32> {
    if cursor.len() < 4 {
        return Err(RymeError::Corrupt(String::from("snapshot truncated")));
    }
    let value = u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
    *cursor = &cursor[4..];
    Ok(value)
}

fn take_u64(cursor: &mut &[u8]) -> Result<u64> {
    if cursor.len() < 8 {
        return Err(RymeError::Corrupt(String::from("snapshot truncated")));
    }
    let value = u64::from_be_bytes([
        cursor[0], cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6], cursor[7],
    ]);
    *cursor = &cursor[8..];
    Ok(value)
}

fn take_field<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = take_u16(cursor)? as usize;
    if cursor.len() < len {
        return Err(RymeError::Corrupt(String::from("snapshot truncated")));
    }
    let value = &cursor[..len];
    *cursor = &cursor[len..];
    Ok(value)
}

fn take_chunk<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = take_u32(cursor)? as usize;
    if len > 8 * 1024 * 1024 || cursor.len() < len {
        return Err(RymeError::Corrupt(String::from("snapshot truncated")));
    }
    let value = &cursor[..len];
    *cursor = &cursor[len..];
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mvcc_snapshot_isolation() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "users", b"1");
        engine.apply(key.clone(), 10, Some(b"a".to_vec())).unwrap();
        engine.apply(key.clone(), 20, Some(b"b".to_vec())).unwrap();
        assert_eq!(engine.read(&key, 10, 1000).unwrap(), Some(b"a".to_vec()));
        assert_eq!(engine.read(&key, 19, 1000).unwrap(), Some(b"a".to_vec()));
        assert_eq!(engine.read(&key, 20, 1000).unwrap(), Some(b"b".to_vec()));
        engine.apply(key.clone(), 30, None).unwrap();
        assert_eq!(engine.read(&key, 30, 1000).unwrap(), None);
        assert_eq!(engine.read(&key, 25, 1000).unwrap(), Some(b"b".to_vec()));
    }

    #[test]
    fn version_lookup_uses_latest_version_not_after_snapshot() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "history", b"row");
        for commit_ts in 1..=1_000 {
            engine.apply(key.clone(), commit_ts, Some(commit_ts.to_be_bytes().to_vec())).unwrap();
        }

        assert_eq!(engine.read(&key, 0, 0).unwrap(), None);
        assert_eq!(engine.read(&key, 1, 0).unwrap(), Some(1u64.to_be_bytes().to_vec()));
        assert_eq!(engine.read(&key, 512, 0).unwrap(), Some(512u64.to_be_bytes().to_vec()));
        assert_eq!(engine.read(&key, 999, 0).unwrap(), Some(999u64.to_be_bytes().to_vec()));
        assert_eq!(
            engine.version_at(&key, 512),
            Some((512, Some(512u64.to_be_bytes().to_vec()), 0))
        );
    }

    #[test]
    fn gc_reclaims_bytes_for_discarded_mvcc_versions() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "history", b"row");
        engine.apply(key.clone(), 1, Some(b"old".to_vec())).unwrap();
        engine.apply(key.clone(), 2, Some(b"older".to_vec())).unwrap();
        engine.apply(key.clone(), 3, Some(b"anchor".to_vec())).unwrap();
        engine.apply(key.clone(), 4, Some(b"new".to_vec())).unwrap();

        assert_eq!(engine.bytes_held(), 3 + 5 + 6 + 3);
        engine.gc(3);

        assert_eq!(engine.bytes_held(), 6 + 3);
        assert_eq!(engine.read(&key, 3, 0).unwrap(), Some(b"anchor".to_vec()));
        assert_eq!(engine.read(&key, 4, 0).unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn removing_rows_reclaims_bytes_without_scanning_other_tables() {
        let mut engine = Engine::new();
        let dropped = RecordKey::new("t", "d", "messages", b"drop");
        let purged = RecordKey::new("t", "d", "sessions", b"purge");
        let retained = RecordKey::new("t", "d", "users", b"keep");
        engine.apply(dropped.clone(), 1, Some(b"old".to_vec())).unwrap();
        engine.apply(dropped.clone(), 2, Some(b"new".to_vec())).unwrap();
        engine.apply(purged.clone(), 3, Some(b"remove-me".to_vec())).unwrap();
        engine.apply(retained.clone(), 4, Some(b"retain".to_vec())).unwrap();

        assert_eq!(engine.drop_table("t", "d", "messages"), 1);
        assert_eq!(engine.bytes_held(), b"remove-me".len() as u64 + b"retain".len() as u64);
        assert_eq!(engine.purge_keys(&[purged.clone(), purged]), 1);
        assert_eq!(engine.bytes_held(), b"retain".len() as u64);
        assert_eq!(engine.read(&retained, 4, 0).unwrap(), Some(b"retain".to_vec()));
    }

    #[test]
    fn failed_table_import_does_not_apply_a_prefix_of_rows() {
        let mut engine = Engine::new();
        let existing = RecordKey::new("t", "d", "users", b"existing");
        let earlier_batch_row = RecordKey::new("t", "d", "users", b"new");
        engine.apply(existing.clone(), 5, Some(b"original".to_vec())).unwrap();
        let bytes_before = engine.bytes_held();

        let result = engine.import_table(
            "t",
            "d",
            "users",
            vec![
                (
                    b"new".to_vec(),
                    vec![TableVersion {
                        commit_ts: 7,
                        value: Some(b"would-have-been-imported".to_vec()),
                        expires_at: 0,
                    }],
                ),
                (
                    b"existing".to_vec(),
                    vec![TableVersion {
                        commit_ts: 4,
                        value: Some(b"stale".to_vec()),
                        expires_at: 0,
                    }],
                ),
            ],
        );

        assert!(matches!(result, Err(RymeError::Corrupt(_))));
        assert_eq!(engine.read(&earlier_batch_row, 7, 0).unwrap(), None);
        assert_eq!(engine.read(&existing, 5, 0).unwrap(), Some(b"original".to_vec()));
        assert_eq!(engine.bytes_held(), bytes_before);
    }

    #[test]
    fn table_import_rejects_conflicting_duplicate_versions_atomically() {
        let mut engine = Engine::new();
        let existing = RecordKey::new("t", "d", "users", b"existing");
        let new_row = RecordKey::new("t", "d", "users", b"new");
        engine.apply(existing.clone(), 5, Some(b"original".to_vec())).unwrap();
        let bytes_before = engine.bytes_held();

        let result = engine.import_table(
            "t",
            "d",
            "users",
            vec![
                (
                    b"new".to_vec(),
                    vec![TableVersion {
                        commit_ts: 7,
                        value: Some(b"would-have-been-imported".to_vec()),
                        expires_at: 0,
                    }],
                ),
                (
                    b"existing".to_vec(),
                    vec![TableVersion {
                        commit_ts: 5,
                        value: Some(b"conflicting".to_vec()),
                        expires_at: 0,
                    }],
                ),
            ],
        );

        assert!(
            matches!(result, Err(RymeError::Corrupt(message)) if message == "version conflict")
        );
        assert_eq!(engine.read(&new_row, 7, 0).unwrap(), None);
        assert_eq!(engine.read(&existing, 5, 0).unwrap(), Some(b"original".to_vec()));
        assert_eq!(engine.bytes_held(), bytes_before);
    }

    #[test]
    fn expiry_hides_and_later_version_returns() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "s", b"k");
        engine.apply_with_expiry(key.clone(), 10, Some(b"a".to_vec()), 100).unwrap();
        assert_eq!(engine.read(&key, 10, 50).unwrap(), Some(b"a".to_vec()));
        assert_eq!(engine.read(&key, 10, 100).unwrap(), None);
        assert_eq!(engine.read(&key, 10, 500).unwrap(), None);
        engine.apply(key.clone(), 20, Some(b"b".to_vec())).unwrap();
        assert_eq!(engine.read(&key, 20, 500).unwrap(), Some(b"b".to_vec()));
        assert_eq!(engine.scan("t", "d", "s", 20, 500, 10).len(), 1);
        assert_eq!(engine.scan("t", "d", "s", 10, 500, 10).len(), 0);
    }

    #[test]
    fn table_scans_seek_to_scope_and_resume_exclusively() {
        let mut engine = Engine::new();
        for index in 0..128u16 {
            let pk = index.to_be_bytes();
            engine.apply(RecordKey::new("a", "d", "before", &pk), 1, Some(pk.to_vec())).unwrap();
            engine.apply(RecordKey::new("a", "d", "messages", &pk), 1, Some(pk.to_vec())).unwrap();
            engine.apply(RecordKey::new("a", "d", "users", &pk), 1, Some(pk.to_vec())).unwrap();
        }

        let first_page = engine.scan("a", "d", "messages", 1, 0, 3);
        assert_eq!(
            first_page.iter().map(|(pk, _)| pk.clone()).collect::<Vec<_>>(),
            vec![
                0u16.to_be_bytes().to_vec(),
                1u16.to_be_bytes().to_vec(),
                2u16.to_be_bytes().to_vec(),
            ]
        );
        let next_page = engine.scan_after("a", "d", "messages", 1, 0, &first_page[2].0, 3);
        assert_eq!(
            next_page.iter().map(|(pk, _)| pk.clone()).collect::<Vec<_>>(),
            vec![
                3u16.to_be_bytes().to_vec(),
                4u16.to_be_bytes().to_vec(),
                5u16.to_be_bytes().to_vec(),
            ]
        );
        assert!(engine.scan("a", "d", "messages", 1, 0, 0).is_empty());
    }

    #[test]
    fn ranges_honor_zero_limit() {
        let mut engine = Engine::new();
        engine.apply(RecordKey::new("t", "d", "items", b"b"), 1, Some(b"value".to_vec())).unwrap();
        let start = RecordKey::new("t", "d", "items", b"a");
        let end = RecordKey::new("t", "d", "items", b"z");

        assert!(engine.range(&start, &end, 1, 0, 0).is_empty());
        assert_eq!(engine.range(&start, &end, 1, 0, 1).len(), 1);
    }

    #[test]
    fn snapshot_roundtrip() {
        let mut engine = Engine::new();
        let first = RecordKey::new("t", "d", "users", b"1");
        let second = RecordKey::new("t", "d", "users", b"2");
        engine.apply(first.clone(), 10, Some(b"a".to_vec())).unwrap();
        engine.apply(first.clone(), 20, Some(b"b".to_vec())).unwrap();
        engine.apply(second.clone(), 15, None).unwrap();
        let raw = engine.encode_snapshot().unwrap();
        let back = Engine::decode_snapshot(&raw).unwrap();
        assert_eq!(back.read(&first, 10, 1000).unwrap(), Some(b"a".to_vec()));
        assert_eq!(back.read(&first, 20, 1000).unwrap(), Some(b"b".to_vec()));
        assert_eq!(back.read(&second, 15, 1000).unwrap(), None);
        assert_eq!(back.max_commit_ts(), 20);
    }

    #[test]
    fn snapshot_preserves_expiry() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "s", b"k");
        engine.apply_with_expiry(key.clone(), 5, Some(b"v".to_vec()), 50).unwrap();
        let raw = engine.encode_snapshot().unwrap();
        let back = Engine::decode_snapshot(&raw).unwrap();
        assert_eq!(back.read(&key, 5, 10).unwrap(), Some(b"v".to_vec()));
        assert_eq!(back.read(&key, 5, 60).unwrap(), None);
    }

    #[test]
    fn snapshot_rejects_corruption() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "users", b"1");
        engine.apply(key, 5, Some(b"v".to_vec())).unwrap();
        let mut raw = engine.encode_snapshot().unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 0xff;
        assert!(Engine::decode_snapshot(&raw).is_err());
    }

    #[test]
    fn snapshot_rejects_out_of_order_versions_and_timestamp_mismatch() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "users", b"k");
        engine.apply(key.clone(), 10, Some(b"first".to_vec())).unwrap();
        engine.apply(key, 20, Some(b"second".to_vec())).unwrap();
        let raw_snapshot = engine.encode_snapshot().unwrap();

        let mut out_of_order = raw_snapshot.clone();
        // Header (20 bytes), three short strings (13 bytes), primary key
        // chunk (5 bytes), and version count (4 bytes) precede the first LSN.
        out_of_order[42..50].copy_from_slice(&30u64.to_be_bytes());
        refresh_snapshot_checksum(&mut out_of_order);
        assert!(matches!(Engine::decode_snapshot(&out_of_order), Err(RymeError::Corrupt(_))));

        let mut mismatched_max = raw_snapshot;
        mismatched_max[8..16].copy_from_slice(&21u64.to_be_bytes());
        refresh_snapshot_checksum(&mut mismatched_max);
        assert!(matches!(Engine::decode_snapshot(&mismatched_max), Err(RymeError::Corrupt(_))));
    }

    #[test]
    fn routing_versions_keep_tenants_and_databases_ordered() {
        let legacy = routing_key(LEGACY_ROUTING_VERSION, "tenant-a", "db-a", "docs", b"k").unwrap();
        assert_eq!(legacy, b"docs\0k");
        let scoped = routing_key(SCOPED_ROUTING_VERSION, "tenant-a", "db-a", "docs", b"k").unwrap();
        assert_eq!(scoped, b"tenant-a\0db-a\0docs\0k");
        assert_ne!(
            scoped,
            routing_key(SCOPED_ROUTING_VERSION, "tenant-b", "db-a", "docs", b"k").unwrap()
        );
        assert!(routing_key(SCOPED_ROUTING_VERSION, "tenant\0a", "db", "docs", b"k").is_err());
        assert!(routing_key(9, "tenant", "db", "docs", b"k").is_err());
    }

    #[test]
    fn scoped_table_bounds_cover_only_one_database_table() {
        let (start, end) = table_routing_bounds(SCOPED_ROUTING_VERSION, "t", "d", "docs").unwrap();
        assert_eq!(start, b"t\0d\0docs\0");
        assert_eq!(end, b"t\0d\0docs\x01");
        assert!(routing_key(SCOPED_ROUTING_VERSION, "t", "d", "docs", b"z").unwrap() < end);
        assert!(routing_key(SCOPED_ROUTING_VERSION, "t", "d2", "docs", b"a").unwrap() >= end);
    }

    #[test]
    fn scoped_range_replacement_does_not_cross_tenants() {
        let tenant_a = RecordKey::new("tenant-a", "db", "docs", b"k");
        let tenant_b = RecordKey::new("tenant-b", "db", "docs", b"k");
        let mut engine = Engine::new();
        engine.apply(tenant_a.clone(), 1, Some(b"old-a".to_vec())).unwrap();
        engine.apply(tenant_b.clone(), 2, Some(b"old-b".to_vec())).unwrap();

        let mut replacement = Engine::new();
        replacement.apply(tenant_a.clone(), 3, Some(b"new-a".to_vec())).unwrap();
        let (start, end) =
            table_routing_bounds(SCOPED_ROUTING_VERSION, "tenant-a", "db", "docs").unwrap();

        assert_eq!(
            engine
                .replace_routing_range_for_version(
                    SCOPED_ROUTING_VERSION,
                    &start,
                    &end,
                    &replacement,
                )
                .unwrap(),
            1
        );
        assert_eq!(engine.read(&tenant_a, 3, 0).unwrap(), Some(b"new-a".to_vec()));
        assert_eq!(engine.read(&tenant_b, 3, 0).unwrap(), Some(b"old-b".to_vec()));
    }

    fn refresh_snapshot_checksum(snapshot: &mut [u8]) {
        let checksum_offset = snapshot.len() - 4;
        let checksum = crc32fast::hash(&snapshot[..checksum_offset]);
        snapshot[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    }
}
