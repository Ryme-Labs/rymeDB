use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Bound;

mod segment;
pub use segment::{ImmutableSegment, SegmentCacheStats, SegmentEntry, SegmentMeta, SegmentStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageMode {
    Hot,
    Standard,
}

impl Default for StorageMode {
    fn default() -> Self {
        Self::Hot
    }
}

#[derive(Debug, Clone)]
pub struct TableVersion {
    pub commit_ts: u64,
    pub value: Option<Vec<u8>>,
    pub expires_at: u64,
}

pub type TableRows = Vec<(Vec<u8>, Vec<TableVersion>)>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
                .iter()
                .take_while(|version| version.commit_ts <= read_ts)
                .last()
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

    pub fn read(&self, key: &RecordKey, read_ts: u64, now: u64) -> Result<Option<Vec<u8>>> {
        let Some(versions) = self.inner.get(key) else {
            return Ok(None);
        };
        let mut found: Option<Vec<u8>> = None;
        for version in versions {
            if version.commit_ts > read_ts {
                break;
            }
            if version.value.is_none() {
                found = None;
            } else {
                found = Self::visible(version, now);
            }
        }
        Ok(found)
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
        let mut out = Vec::new();
        for (key, versions) in &self.inner {
            if key.tenant != tenant || key.database != database || key.table != table {
                continue;
            }
            if start_after.is_some_and(|after| key.pk.as_slice() <= after) {
                continue;
            }
            let mut current: Option<Vec<u8>> = None;
            for version in versions {
                if version.commit_ts > read_ts {
                    break;
                }
                if version.value.is_none() {
                    current = None;
                } else {
                    current = Self::visible(version, now);
                }
            }
            if let Some(value) = current {
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
        let keys: Vec<RecordKey> = self
            .inner
            .keys()
            .filter(|key| key.tenant == tenant && key.database == database && key.table == table)
            .cloned()
            .collect();
        let count = keys.len();
        for key in keys {
            self.inner.remove(&key);
        }
        count
    }

    pub fn purge_keys(&mut self, keys: &[RecordKey]) -> usize {
        let mut removed = 0;
        for key in keys {
            if self.inner.remove(key).is_some() {
                removed += 1;
            }
        }
        self.bytes_held = self
            .inner
            .values()
            .flat_map(|versions| versions.iter())
            .filter_map(|version| version.value.as_ref())
            .map(|value| value.len() as u64)
            .sum();
        removed
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
        let mut found: Option<u64> = None;
        for version in versions {
            if version.commit_ts > read_ts {
                break;
            }
            if version.value.is_none() {
                found = None;
            } else {
                found = Some(version.expires_at);
            }
        }
        found
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
        let mut out = Vec::new();
        for (key, versions) in &self.inner {
            if key.tenant != tenant || key.database != database || key.table != table {
                continue;
            }
            let mut current: Option<&Version> = None;
            for version in versions {
                if version.commit_ts > read_ts {
                    break;
                }
                current = Some(version);
            }
            if let Some(version) = current {
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
        let mut out = Vec::new();
        let iter = self.inner.range((Bound::Included(start), Bound::Excluded(end)));
        for (key, versions) in iter {
            let mut current: Option<Vec<u8>> = None;
            for version in versions {
                if version.commit_ts > read_ts {
                    break;
                }
                if version.value.is_none() {
                    current = None;
                } else {
                    current = Self::visible(version, now);
                }
            }
            if let Some(value) = current {
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
                versions.drain(0..keep_from);
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
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_be_bytes());
        Ok(out)
    }

    pub fn decode_snapshot(input: &[u8]) -> Result<Self> {
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
        let _max = take_u64(&mut cursor)?;
        let count = take_u32(&mut cursor)? as usize;
        if count > 10_000_000 {
            return Err(RymeError::Corrupt(String::from("snapshot count")));
        }
        let mut engine = Engine::new();
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
            for _ in 0..vcount {
                let commit_ts = take_u64(&mut cursor)?;
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
            versions.sort_by_key(|v| v.commit_ts);
            let key = RecordKey { tenant, database, table, pk };
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
}
