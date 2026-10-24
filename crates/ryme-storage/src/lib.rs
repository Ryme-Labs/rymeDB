use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound;

/// High bit marks expiry timestamps stored at millisecond precision. Values
/// without this bit retain the historical Unix-seconds representation.
const MILLIS_EXPIRY_TAG: u64 = 1 << 63;

/// Encodes an absolute Unix-millisecond expiry without changing the on-disk
/// version layout. Existing untagged second-based expiries remain readable.
pub fn expiry_at_millis(unix_millis: u64) -> u64 {
    MILLIS_EXPIRY_TAG | unix_millis.min(!MILLIS_EXPIRY_TAG)
}

/// Converts a stored expiry into Unix milliseconds, including legacy seconds.
pub fn expiry_to_millis(expires_at: u64) -> u64 {
    if expires_at & MILLIS_EXPIRY_TAG != 0 {
        expires_at & !MILLIS_EXPIRY_TAG
    } else {
        expires_at.saturating_mul(1000)
    }
}

/// Tests a stored expiry against a Unix-millisecond clock value.
pub fn expiry_is_expired(expires_at: u64, now_millis: u64) -> bool {
    expires_at != 0 && expiry_to_millis(expires_at) <= now_millis
}

/// Returns the remaining lifetime in milliseconds, or `None` for no expiry.
/// Returns remaining milliseconds, or `None` when the value has no expiry.
pub fn expiry_remaining_millis(expires_at: u64, now_millis: u64) -> Option<u64> {
    (expires_at != 0).then(|| expiry_to_millis(expires_at).saturating_sub(now_millis))
}

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
type ImportHistory<'a> = BTreeMap<RecordKey, (Option<u64>, BTreeMap<u64, (Option<&'a [u8]>, u64)>)>;

/// Routing-key formats are part of persisted range topology. Keep the legacy
/// format readable so an existing deployment can restart, while new
/// topologies include the isolation scopes that precede every record.
/// Scoped routing escapes NUL bytes in database names as `[0, 255]`; existing
/// names keep their original byte representation.
pub const LEGACY_ROUTING_VERSION: u8 = 0;
pub const SCOPED_ROUTING_VERSION: u8 = 1;
pub const CURRENT_ROUTING_VERSION: u8 = SCOPED_ROUTING_VERSION;
pub const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;
/// Maximum byte length of a record key or value chunk in durable formats.
pub const MAX_RECORD_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const MIN_ORDERED_BATCH_READ: usize = 16;
const MAX_ORDERED_BATCH_READ_GAP: usize = 64;

trait RecordKeyRef {
    fn record_key(&self) -> &RecordKey;
}

impl RecordKeyRef for RecordKey {
    fn record_key(&self) -> &RecordKey {
        self
    }
}

impl RecordKeyRef for &RecordKey {
    fn record_key(&self) -> &RecordKey {
        self
    }
}

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
    let has_invalid_component = if version == SCOPED_ROUTING_VERSION {
        tenant.as_bytes().contains(&0) || table.as_bytes().contains(&0)
    } else {
        table.as_bytes().contains(&0)
    };
    if has_invalid_component {
        return Err(RymeError::InvalidArgument(String::from("routing component")));
    }
    let escaped_database_bytes = if version == SCOPED_ROUTING_VERSION {
        database.as_bytes().iter().filter(|byte| **byte == 0).count()
    } else {
        0
    };
    let capacity = tenant
        .len()
        .checked_add(database.len())
        .and_then(|capacity| capacity.checked_add(table.len()))
        .and_then(|capacity| capacity.checked_add(pk.len()))
        .and_then(|capacity| capacity.checked_add(escaped_database_bytes))
        .and_then(|capacity| capacity.checked_add(if version == 0 { 1 } else { 3 }))
        .ok_or_else(|| RymeError::Overload(String::from("routing key")))?;
    let mut out = Vec::new();
    out.try_reserve_exact(capacity)
        .map_err(|_| RymeError::Overload(String::from("routing key")))?;
    if version == SCOPED_ROUTING_VERSION {
        out.extend_from_slice(tenant.as_bytes());
        out.push(0);
        for byte in database.as_bytes() {
            if *byte == 0 {
                // A literal NUL would be indistinguishable from the component
                // separator. Escape it without changing non-NUL legacy keys.
                out.extend_from_slice(&[0, u8::MAX]);
            } else {
                out.push(*byte);
            }
        }
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

fn validate_record_scope(tenant: &str, _database: &str, table: &str) -> Result<()> {
    // BranchBackend encodes its internal namespace in the database field with
    // NUL separators. Tenant and table names remain delimiter-safe identifiers.
    if tenant.as_bytes().contains(&0) || table.as_bytes().contains(&0) {
        return Err(RymeError::InvalidArgument(String::from("record scope")));
    }
    Ok(())
}

/// Validates that a key fits the storage and WAL formats and has an unambiguous scope.
pub fn validate_record_key(key: &RecordKey) -> Result<()> {
    validate_record_scope(&key.tenant, &key.database, &key.table)?;
    validate_record_key_lengths(key)
}

fn validate_record_key_lengths(key: &RecordKey) -> Result<()> {
    if key.tenant.len() > u16::MAX as usize
        || key.database.len() > u16::MAX as usize
        || key.table.len() > u16::MAX as usize
        || key.pk.len() > MAX_RECORD_CHUNK_BYTES
    {
        return Err(RymeError::InvalidArgument(String::from("record key size")));
    }
    Ok(())
}

/// Validates that a value fits the storage and WAL formats.
pub fn validate_record_value(value: &[u8]) -> Result<()> {
    if value.len() > MAX_RECORD_CHUNK_BYTES {
        return Err(RymeError::InvalidArgument(String::from("record value size")));
    }
    Ok(())
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
/// In-memory MVCC engine. Read-time arguments are Unix milliseconds.
pub struct Engine {
    inner: BTreeMap<RecordKey, VecDeque<Version>>,
    bytes_held: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcCursor {
    key: RecordKey,
    inclusive: bool,
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

    /// Return whether a version exists at the snapshot and its visible value.
    ///
    /// The existence bit is independent of TTL and tombstones so callers that
    /// merge immutable segments do not fall through to an older version when a
    /// newer version intentionally hides the value.
    pub fn read_with_presence(
        &self,
        key: &RecordKey,
        read_ts: u64,
        now: u64,
    ) -> Result<(bool, Option<Vec<u8>>)> {
        let Some(versions) = self.inner.get(key) else {
            return Ok((false, None));
        };
        let Some(version) =
            Self::version_index_at(versions, read_ts).and_then(|index| versions.get(index))
        else {
            return Ok((false, None));
        };
        let value = if version.value.is_some() { Self::visible(version, now) } else { None };
        Ok((true, value))
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
        validate_record_key(&key)?;
        if let Some(value) = value.as_deref() {
            validate_record_value(value)?;
        }
        let versions = self.inner.entry(key).or_default();
        if let Some(last) = versions.back() {
            if commit_ts <= last.commit_ts {
                return Err(RymeError::Conflict(String::from("stale commit_ts")));
            }
        }
        if let Some(v) = value.as_ref() {
            self.bytes_held += v.len() as u64;
        }
        versions.push_back(Version { commit_ts, value, expires_at });
        Ok(())
    }

    pub(crate) fn merge_owned(&mut self, other: Engine) -> Result<()> {
        // Validate the complete merge before appending any history. A conflict
        // late in key order must not leave the target partially updated.
        for (key, incoming) in &other.inner {
            if incoming.is_empty() {
                continue;
            }
            if self
                .inner
                .get(key)
                .and_then(|versions| versions.back())
                .zip(incoming.front())
                .is_some_and(|(last, first)| last.commit_ts >= first.commit_ts)
            {
                return Err(RymeError::Conflict(String::from("stale commit_ts")));
            }
        }

        let Engine { inner, bytes_held } = other;
        for (key, mut incoming) in inner {
            if incoming.is_empty() {
                continue;
            }
            let versions = self.inner.entry(key).or_default();
            versions.append(&mut incoming);
        }
        self.bytes_held += bytes_held;
        Ok(())
    }

    fn visible(version: &Version, now: u64) -> Option<Vec<u8>> {
        if expiry_is_expired(version.expires_at, now) {
            return None;
        }
        version.value.clone()
    }

    fn version_index_at(versions: &VecDeque<Version>, read_ts: u64) -> Option<usize> {
        let mut low = 0;
        let mut high = versions.len();
        while low < high {
            let middle = low + (high - low) / 2;
            if versions.get(middle)?.commit_ts <= read_ts {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low.checked_sub(1)
    }

    fn read_versions(versions: &VecDeque<Version>, read_ts: u64, now: u64) -> Option<Vec<u8>> {
        let version = versions.get(Self::version_index_at(versions, read_ts)?)?;
        if expiry_is_expired(version.expires_at, now) {
            return None;
        }
        version.value.clone()
    }

    /// Reads at an MVCC timestamp; `now` is the current Unix time in milliseconds.
    pub fn read(&self, key: &RecordKey, read_ts: u64, now: u64) -> Result<Option<Vec<u8>>> {
        self.read_with_presence(key, read_ts, now).map(|(_, value)| value)
    }

    /// Reads a same-table batch with one ordered map walk when keys are dense.
    /// Unsorted keys preserve input order; duplicate keys are read once and
    /// their values are copied to each corresponding output position.
    pub fn read_many(
        &self,
        keys: &[RecordKey],
        read_ts: u64,
        now: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.read_many_impl(keys, read_ts, now)
    }

    /// Reads a batch of borrowed keys without cloning their routing components.
    pub fn read_many_borrowed(
        &self,
        keys: &[&RecordKey],
        read_ts: u64,
        now: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.read_many_impl(keys, read_ts, now)
    }

    fn read_many_impl<K: RecordKeyRef>(
        &self,
        keys: &[K],
        read_ts: u64,
        now: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        if keys.len() < MIN_ORDERED_BATCH_READ {
            return keys.iter().map(|key| self.read(key.record_key(), read_ts, now)).collect();
        }
        let first = keys[0].record_key();
        let mut previous = first;
        let mut ordered = true;
        let mut reversed = true;
        for key in &keys[1..] {
            let key = key.record_key();
            if key.tenant != first.tenant
                || key.database != first.database
                || key.table != first.table
            {
                return keys.iter().map(|key| self.read(key.record_key(), read_ts, now)).collect();
            }
            ordered &= previous < key;
            reversed &= previous > key;
            previous = key;
        }

        if !ordered && !reversed {
            let mut seen = HashSet::with_capacity(keys.len());
            let has_duplicates =
                keys.iter().any(|key| !seen.insert(key.record_key().pk.as_slice()));
            drop(seen);
            if !has_duplicates {
                return keys.iter().map(|key| self.read(key.record_key(), read_ts, now)).collect();
            }

            let mut first_indices = HashMap::with_capacity(keys.len());
            let mut unique = Vec::with_capacity(keys.len());
            let mut duplicates = Vec::new();
            for (index, key) in keys.iter().enumerate() {
                let key = key.record_key();
                let primary_key = key.pk.as_slice();
                if let Some(first) = first_indices.get(primary_key) {
                    duplicates.push((index, *first));
                } else {
                    first_indices.insert(primary_key, index);
                    unique.push((index, key));
                }
            }
            unique.sort_unstable_by(|left, right| left.1.cmp(right.1));
            let unique_keys = unique.iter().map(|(_, key)| *key).collect::<Vec<_>>();
            let unique_values = self.read_many_impl(&unique_keys, read_ts, now)?;
            let mut values = vec![None; keys.len()];
            for ((index, _), value) in unique.into_iter().zip(unique_values) {
                values[index] = value;
            }
            for (duplicate, first) in duplicates {
                values[duplicate] = values[first].clone();
            }
            return Ok(values);
        }

        let last = keys[keys.len() - 1].record_key();
        let (range_start, range_end) = if reversed { (last, first) } else { (first, last) };

        let mut entries =
            self.inner.range((Bound::Included(range_start), Bound::Included(range_end))).peekable();
        let mut values = Vec::with_capacity(keys.len());
        let mut skipped_entries = 0usize;
        for offset in 0..keys.len() {
            let index = if reversed { keys.len() - 1 - offset } else { offset };
            let key = keys[index].record_key();
            loop {
                let Some((stored_key, versions)) = entries.peek().copied() else {
                    values.push(None);
                    break;
                };
                match stored_key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        skipped_entries += 1;
                        if skipped_entries > MAX_ORDERED_BATCH_READ_GAP {
                            return keys
                                .iter()
                                .map(|key| self.read(key.record_key(), read_ts, now))
                                .collect();
                        }
                        entries.next();
                    }
                    std::cmp::Ordering::Equal => {
                        values.push(Self::read_versions(versions, read_ts, now));
                        entries.next();
                        break;
                    }
                    std::cmp::Ordering::Greater => {
                        values.push(None);
                        break;
                    }
                }
            }
        }
        if reversed {
            values.reverse();
        }
        Ok(values)
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

    fn table_entries<'a>(
        &'a self,
        tenant: &'a str,
        database: &'a str,
        table: &'a str,
    ) -> impl Iterator<Item = (&'a RecordKey, &'a VecDeque<Version>)> + 'a {
        let start = RecordKey::new(tenant, database, table, &[]);
        self.inner.range((Bound::Included(start), Bound::Unbounded)).take_while(move |(key, _)| {
            key.tenant == tenant && key.database == database && key.table == table
        })
    }

    pub fn table_names(&self, tenant: &str, database: &str) -> Vec<String> {
        let start = RecordKey::new(tenant, database, "", &[]);
        let mut tables = Vec::new();
        let mut previous: Option<&str> = None;
        for key in self
            .inner
            .range((Bound::Included(start), Bound::Unbounded))
            .map(|(key, _)| key)
            .take_while(|key| key.tenant == tenant && key.database == database)
        {
            if previous != Some(key.table.as_str()) {
                tables.push(key.table.clone());
                previous = Some(&key.table);
            }
        }
        tables
    }

    pub fn table_bytes(&self, tenant: &str, database: &str, table: &str) -> u64 {
        let mut bytes = 0u64;
        for (key, versions) in self.table_entries(tenant, database, table) {
            bytes += key.pk.len() as u64;
            for version in versions {
                if let Some(value) = version.value.as_ref() {
                    bytes += value.len() as u64;
                }
            }
        }
        bytes
    }

    pub fn export_table(&self, tenant: &str, database: &str, table: &str) -> TableRows {
        let mut out = Vec::new();
        for (key, versions) in self.table_entries(tenant, database, table) {
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
        out
    }

    #[allow(clippy::too_many_arguments)]
    pub fn export_table_routing_range(
        &self,
        version: u8,
        tenant: &str,
        database: &str,
        table: &str,
        start: &[u8],
        end: &[u8],
        read_ts: u64,
        limit: usize,
    ) -> Result<TableRows> {
        validate_routing_version(version)?;
        if !end.is_empty() && start >= end {
            return Err(RymeError::InvalidArgument(String::from("range")));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }

        let prefix = routing_key(version, tenant, database, table, &[])?;
        if (start > prefix.as_slice() && !start.starts_with(&prefix))
            || (!end.is_empty() && end <= prefix.as_slice())
        {
            return Ok(Vec::new());
        }

        let start_pk = start.strip_prefix(prefix.as_slice()).unwrap_or_default();
        let lower = Bound::Included(RecordKey::new(tenant, database, table, start_pk));
        let upper = if end.is_empty() {
            Bound::Unbounded
        } else if let Some(end_pk) = end.strip_prefix(prefix.as_slice()) {
            Bound::Excluded(RecordKey::new(tenant, database, table, end_pk))
        } else {
            Bound::Unbounded
        };
        let mut out = Vec::with_capacity(limit.min(1024));
        for (key, versions) in self.inner.range((lower, upper)) {
            if key.tenant != tenant || key.database != database || key.table != table {
                break;
            }
            let versions = versions
                .iter()
                .take_while(|version| version.commit_ts <= read_ts)
                .map(|version| TableVersion {
                    commit_ts: version.commit_ts,
                    value: version.value.clone(),
                    expires_at: version.expires_at,
                })
                .collect::<Vec<_>>();
            if !versions.is_empty() {
                out.push((key.pk.clone(), versions));
                if out.len() == limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub fn import_table(
        &mut self,
        tenant: &str,
        database: &str,
        table: &str,
        rows: TableRows,
    ) -> Result<u64> {
        validate_record_scope(tenant, database, table)?;
        // Validate every key's incoming history before changing the engine. A
        // malformed later row must not leave an earlier part of the import
        // visible to readers.
        let mut histories: ImportHistory<'_> = BTreeMap::new();
        for (pk, versions) in &rows {
            let key = RecordKey::new(tenant, database, table, pk);
            validate_record_key_lengths(&key)?;
            if versions.iter().any(|version| version.commit_ts == 0) {
                return Err(RymeError::Corrupt(String::from("import commit timestamp")));
            }
            for version in versions {
                if let Some(value) = version.value.as_deref() {
                    validate_record_value(value)?;
                }
            }
            let (last, known) = histories.entry(key.clone()).or_insert_with(|| {
                let existing = self.inner.get(&key);
                (
                    existing.and_then(|versions| versions.back().map(|version| version.commit_ts)),
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
                if let Some(last) = entry.back() {
                    if version.commit_ts < last.commit_ts {
                        return Err(RymeError::Corrupt(String::from("version order")));
                    }
                }
                if let Some(value) = version.value.as_ref() {
                    self.bytes_held += value.len() as u64;
                }
                entry.push_back(Version {
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
            .filter(|(_, matched)| *matched)
            .map(|(key, _)| key.clone())
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

    pub fn matches_exact_version(
        &self,
        key: &RecordKey,
        commit_ts: u64,
        value: Option<&[u8]>,
        expires_at: u64,
    ) -> bool {
        self.inner.get(key).is_some_and(|versions| {
            versions
                .binary_search_by_key(&commit_ts, |version| version.commit_ts)
                .ok()
                .and_then(|index| versions.get(index))
                .is_some_and(|version| {
                    version.value.as_deref() == value && version.expires_at == expires_at
                })
        })
    }

    pub fn latest_version(&self, key: &RecordKey) -> Option<(u64, Option<Vec<u8>>, u64)> {
        self.inner
            .get(key)?
            .back()
            .map(|version| (version.commit_ts, version.value.clone(), version.expires_at))
    }

    pub fn latest_commit_ts(&self, key: &RecordKey) -> Option<u64> {
        self.inner.get(key)?.back().map(|version| version.commit_ts)
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
                if version.value.is_some() && expiry_is_expired(version.expires_at, now) {
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
        if limit == 0 || start >= end {
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
        let mut cursor = None;
        loop {
            let (next, complete) =
                self.gc_batch(retain_ts, cursor.as_ref(), usize::MAX, usize::MAX);
            if complete {
                break;
            }
            cursor = next;
        }
    }

    /// Collects bounded key/version work, returning a cursor and whether the
    /// ordered keyspace is fully visited. Keeping the cursor outside the engine
    /// lets callers release the engine lock between pages.
    pub fn gc_batch(
        &mut self,
        retain_ts: u64,
        cursor: Option<&GcCursor>,
        max_keys: usize,
        max_versions: usize,
    ) -> (Option<GcCursor>, bool) {
        if max_keys == 0 || max_versions == 0 {
            return (cursor.cloned(), false);
        }
        let mut reclaimed_bytes = 0u64;
        let (next_cursor, complete) = {
            let bounds: (Bound<&RecordKey>, Bound<&RecordKey>) = match cursor {
                Some(cursor) if cursor.inclusive => {
                    (Bound::Included(&cursor.key), Bound::Unbounded)
                }
                Some(cursor) => (Bound::Excluded(&cursor.key), Bound::Unbounded),
                None => (Bound::Unbounded, Bound::Unbounded),
            };
            let mut entries = self.inner.range_mut(bounds);
            let mut visited_keys = 0usize;
            let mut removed_versions = 0usize;
            let mut last_key = None;
            let mut resume_current = false;
            while visited_keys < max_keys && removed_versions < max_versions {
                let Some((key, versions)) = entries.next() else { break };
                let eligible = Self::version_partition_at(versions, retain_ts);
                let removable = eligible.saturating_sub(1);
                let remove_count = removable.min(max_versions - removed_versions);
                for _ in 0..remove_count {
                    if let Some(version) = versions.pop_front() {
                        if let Some(value) = version.value {
                            reclaimed_bytes = reclaimed_bytes.saturating_add(value.len() as u64);
                        }
                    }
                }
                if max_versions != usize::MAX
                    && versions.len() <= max_versions
                    && versions.capacity() > versions.len().saturating_mul(2)
                {
                    versions.shrink_to_fit();
                }
                removed_versions += remove_count;
                visited_keys += 1;
                last_key = Some(key);
                if remove_count < removable {
                    resume_current = true;
                    break;
                }
            }
            let has_more = entries.next().is_some();
            let next_cursor = if resume_current {
                last_key.cloned().map(|key| GcCursor { key, inclusive: true })
            } else if has_more {
                last_key.cloned().map(|key| GcCursor { key, inclusive: false })
            } else {
                None
            };
            (next_cursor, !resume_current && !has_more)
        };
        self.bytes_held = self.bytes_held.saturating_sub(reclaimed_bytes);
        (next_cursor, complete)
    }

    fn version_partition_at(versions: &VecDeque<Version>, read_ts: u64) -> usize {
        let mut low = 0;
        let mut high = versions.len();
        while low < high {
            let middle = low + (high - low) / 2;
            if versions[middle].commit_ts <= read_ts {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }

    pub fn max_commit_ts(&self) -> u64 {
        self.inner
            .values()
            .filter_map(|versions| versions.back().map(|v| v.commit_ts))
            .max()
            .unwrap_or(0)
    }

    pub fn encode_snapshot(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        push_snapshot_bytes(&mut out, &0x52594D53u32.to_be_bytes())?;
        push_snapshot_bytes(&mut out, &2u16.to_be_bytes())?;
        push_snapshot_bytes(&mut out, &0u16.to_be_bytes())?;
        push_snapshot_bytes(&mut out, &self.max_commit_ts().to_be_bytes())?;
        let count = u32::try_from(self.inner.len())
            .map_err(|_| RymeError::InvalidArgument(String::from("snapshot size")))?;
        push_snapshot_bytes(&mut out, &count.to_be_bytes())?;
        for (key, versions) in &self.inner {
            push_field(&mut out, key.tenant.as_bytes())?;
            push_field(&mut out, key.database.as_bytes())?;
            push_field(&mut out, key.table.as_bytes())?;
            push_chunk(&mut out, &key.pk)?;
            let vcount = u32::try_from(versions.len())
                .map_err(|_| RymeError::InvalidArgument(String::from("snapshot size")))?;
            push_snapshot_bytes(&mut out, &vcount.to_be_bytes())?;
            for version in versions {
                push_snapshot_bytes(&mut out, &version.commit_ts.to_be_bytes())?;
                match version.value.as_ref() {
                    Some(value) => {
                        push_snapshot_bytes(&mut out, &[0])?;
                        push_chunk(&mut out, value)?;
                    }
                    None => {
                        push_snapshot_bytes(&mut out, &[1])?;
                    }
                }
                push_snapshot_bytes(&mut out, &version.expires_at.to_be_bytes())?;
            }
        }
        let crc = crc32fast::hash(&out);
        out.try_reserve(4).map_err(|_| RymeError::Overload(String::from("snapshot")))?;
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
        let reserved = take_u16(&mut cursor)?;
        if magic != 0x52594D53 || (version != 1 && version != 2) || reserved != 0 {
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
            if vcount == 0 || vcount > 1_000_000 {
                return Err(RymeError::Corrupt(String::from("snapshot versions")));
            }
            let mut versions = VecDeque::with_capacity(vcount.min(4));
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
                versions.push_back(Version { commit_ts, value, expires_at });
            }
            let key = RecordKey { tenant, database, table, pk };
            validate_record_key(&key)
                .map_err(|_| RymeError::Corrupt(String::from("snapshot record key")))?;
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

fn version_payload_bytes(versions: &VecDeque<Version>) -> u64 {
    versions
        .iter()
        .filter_map(|version| version.value.as_ref())
        .fold(0u64, |total, value| total.saturating_add(value.len() as u64))
}

fn push_field(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if bytes.len() > u16::MAX as usize {
        return Err(RymeError::InvalidArgument(String::from("snapshot field")));
    }
    push_snapshot_bytes(out, &(bytes.len() as u16).to_be_bytes())?;
    push_snapshot_bytes(out, bytes)?;
    Ok(())
}

fn push_chunk(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_RECORD_CHUNK_BYTES {
        return Err(RymeError::InvalidArgument(String::from("snapshot chunk")));
    }
    push_snapshot_bytes(out, &(bytes.len() as u32).to_be_bytes())?;
    push_snapshot_bytes(out, bytes)?;
    Ok(())
}

fn push_snapshot_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    push_bounded_bytes(out, bytes, MAX_SNAPSHOT_BYTES - 4)
}

fn push_bounded_bytes(out: &mut Vec<u8>, bytes: &[u8], limit: usize) -> Result<()> {
    let new_len = out
        .len()
        .checked_add(bytes.len())
        .filter(|length| *length <= limit)
        .ok_or_else(|| RymeError::Overload(String::from("snapshot")))?;
    out.try_reserve(new_len - out.len())
        .map_err(|_| RymeError::Overload(String::from("snapshot")))?;
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
    fn expiry_formats_preserve_legacy_seconds_and_millisecond_precision() {
        let legacy_expiry = 1_700_000_000;
        assert_eq!(expiry_to_millis(legacy_expiry), legacy_expiry * 1000);
        assert!(!expiry_is_expired(legacy_expiry, legacy_expiry * 1000 - 1));
        assert!(expiry_is_expired(legacy_expiry, legacy_expiry * 1000));

        let millis_expiry = expiry_at_millis(1_700_000_000_123);
        assert_eq!(expiry_to_millis(millis_expiry), 1_700_000_000_123);
        assert_eq!(expiry_remaining_millis(millis_expiry, 1_700_000_000_000), Some(123));
        assert!(!expiry_is_expired(millis_expiry, 1_700_000_000_122));
        assert!(expiry_is_expired(millis_expiry, 1_700_000_000_123));
        assert_eq!(expiry_to_millis(expiry_at_millis(MILLIS_EXPIRY_TAG + 1)), !MILLIS_EXPIRY_TAG);
        assert_eq!(expiry_remaining_millis(0, 1_700_000_000_000), None);
    }

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
    fn ordered_batch_reads_match_point_reads_for_dense_and_sparse_keys() {
        let mut engine = Engine::new();
        for id in 0u64..100 {
            let key = RecordKey::new("t", "d", "items", &id.to_be_bytes());
            if id == 15 {
                engine.apply_with_expiry(key, 1, Some(b"expiring".to_vec()), 50).unwrap();
            } else {
                engine.apply(key, 1, Some(id.to_be_bytes().to_vec())).unwrap();
            }
        }
        engine
            .apply(
                RecordKey::new("t", "d", "items", &16u64.to_be_bytes()),
                20,
                Some(b"new-version".to_vec()),
            )
            .unwrap();
        engine.apply(RecordKey::new("t", "d", "items", &17u64.to_be_bytes()), 20, None).unwrap();

        let dense = (0u64..64)
            .chain(101..117)
            .map(|id| RecordKey::new("t", "d", "items", &id.to_be_bytes()))
            .collect::<Vec<_>>();
        let sparse = (0u64..16)
            .chain(90..106)
            .map(|id| RecordKey::new("t", "d", "items", &id.to_be_bytes()))
            .collect::<Vec<_>>();
        let mut unsorted = dense.clone();
        unsorted.swap(0, 1);
        let reversed = dense.iter().rev().cloned().collect::<Vec<_>>();
        let missing = RecordKey::new("t", "d", "items", &999u64.to_be_bytes());
        let mut duplicated =
            dense.iter().take(32).chain(dense.iter().take(32)).cloned().collect::<Vec<_>>();
        duplicated.extend([missing.clone(), missing]);
        let cross_table = (0u64..32)
            .map(|id| RecordKey::new("t", "d", "items", &id.to_be_bytes()))
            .chain((32u64..64).map(|id| RecordKey::new("t", "d", "other", &id.to_be_bytes())))
            .collect::<Vec<_>>();

        for (keys, read_ts, now) in [
            (&dense[..], 10, 0),
            (&dense[..], 20, 50),
            (&sparse[..], 20, 0),
            (&unsorted[..], 20, 0),
            (&reversed[..], 20, 0),
            (&duplicated[..], 20, 0),
            (&cross_table[..], 20, 0),
        ] {
            let expected =
                keys.iter().map(|key| engine.read(key, read_ts, now).unwrap()).collect::<Vec<_>>();
            assert_eq!(engine.read_many(keys, read_ts, now).unwrap(), expected);
            let borrowed = keys.iter().collect::<Vec<_>>();
            assert_eq!(engine.read_many_borrowed(&borrowed, read_ts, now).unwrap(), expected);
        }
    }

    #[test]
    fn owned_engine_merge_preserves_history_tombstones_and_expiry() {
        let key = RecordKey::new("t", "d", "items", b"row");
        let expiring = RecordKey::new("t", "d", "items", b"expiring");
        let mut base = Engine::new();
        base.apply(key.clone(), 1, Some(b"old".to_vec())).unwrap();

        let mut delta = Engine::new();
        delta.apply(key.clone(), 2, Some(b"new".to_vec())).unwrap();
        delta.apply(key.clone(), 3, None).unwrap();
        delta.apply_with_expiry(expiring.clone(), 4, Some(b"ttl".to_vec()), 50).unwrap();

        base.merge_owned(delta).unwrap();

        assert_eq!(base.read(&key, 1, 0).unwrap(), Some(b"old".to_vec()));
        assert_eq!(base.read(&key, 2, 0).unwrap(), Some(b"new".to_vec()));
        assert_eq!(base.read(&key, 3, 0).unwrap(), None);
        assert_eq!(base.read(&expiring, 4, 49_000).unwrap(), Some(b"ttl".to_vec()));
        assert_eq!(base.read(&expiring, 4, 50_000).unwrap(), None);
        assert_eq!(base.max_commit_ts(), 4);
        assert_eq!(base.bytes_held(), 9);
    }

    #[test]
    fn failed_owned_engine_merge_does_not_partially_mutate_target() {
        let earlier_key = RecordKey::new("t", "d", "items", b"a");
        let conflicting_key = RecordKey::new("t", "d", "items", b"z");
        let mut base = Engine::new();
        base.apply(conflicting_key.clone(), 5, Some(b"base".to_vec())).unwrap();

        let mut delta = Engine::new();
        delta.apply(earlier_key.clone(), 2, Some(b"delta".to_vec())).unwrap();
        delta.apply(conflicting_key.clone(), 4, Some(b"stale".to_vec())).unwrap();

        assert!(base.merge_owned(delta).is_err());
        assert_eq!(base.read(&earlier_key, 2, 0).unwrap(), None);
        assert_eq!(base.read(&conflicting_key, 5, 0).unwrap(), Some(b"base".to_vec()));
        assert_eq!(base.bytes_held(), 4);
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
    fn read_with_presence_keeps_hidden_versions_from_falling_back() {
        let mut engine = Engine::new();
        let value_key = RecordKey::new("t", "d", "presence", b"value");
        let tombstone_key = RecordKey::new("t", "d", "presence", b"deleted");
        let expired_key = RecordKey::new("t", "d", "presence", b"expired");
        engine.apply(value_key.clone(), 1, Some(b"value".to_vec())).unwrap();
        engine.apply(tombstone_key.clone(), 1, Some(b"old".to_vec())).unwrap();
        engine.apply(tombstone_key.clone(), 2, None).unwrap();
        engine.apply_with_expiry(expired_key.clone(), 1, Some(b"expired".to_vec()), 10).unwrap();

        assert_eq!(
            engine.read_with_presence(&value_key, 1, 0).unwrap(),
            (true, Some(b"value".to_vec()))
        );
        assert_eq!(engine.read_with_presence(&tombstone_key, 2, 0).unwrap(), (true, None));
        assert_eq!(engine.read_with_presence(&expired_key, 1, 10_000).unwrap(), (true, None));
        assert_eq!(
            engine
                .read_with_presence(&RecordKey::new("t", "d", "presence", b"missing"), 1, 0)
                .unwrap(),
            (false, None)
        );
        assert_eq!(
            engine.read_with_presence(&tombstone_key, 1, 0).unwrap(),
            (true, Some(b"old".to_vec()))
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
    fn gc_batches_resume_in_key_order_without_skipping_rows() {
        let mut engine = Engine::new();
        let keys: Vec<_> = (0..5)
            .map(|index| RecordKey::new("t", "d", "history", format!("k-{index}").as_bytes()))
            .collect();
        for key in &keys {
            for commit_ts in 1..=3 {
                engine.apply(key.clone(), commit_ts, Some(vec![commit_ts as u8])).unwrap();
            }
        }

        let (cursor, complete) = engine.gc_batch(2, None, 2, 100);
        assert!(!complete);
        assert_eq!(cursor.as_ref().map(|cursor| cursor.key.clone()), Some(keys[1].clone()));
        assert_eq!(engine.version_at(&keys[0], 1), None);
        assert!(engine.version_at(&keys[2], 1).is_some());

        let (cursor, complete) = engine.gc_batch(2, cursor.as_ref(), 2, 100);
        assert!(!complete);
        assert_eq!(cursor.as_ref().map(|cursor| cursor.key.clone()), Some(keys[3].clone()));
        let (cursor, complete) = engine.gc_batch(2, cursor.as_ref(), 2, 100);
        assert!(complete);
        assert_eq!(cursor, None);

        for key in &keys {
            assert_eq!(engine.version_at(key, 1), None);
            assert_eq!(engine.version_at(key, 2), Some((2, Some(vec![2]), 0)));
            assert_eq!(engine.version_at(key, 3), Some((3, Some(vec![3]), 0)));
        }
    }

    #[test]
    fn gc_batches_bound_work_for_a_single_long_history() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "history", b"hot");
        for commit_ts in 1..=10 {
            engine.apply(key.clone(), commit_ts, Some(vec![commit_ts as u8])).unwrap();
        }
        let initial_capacity = engine.inner.get(&key).unwrap().capacity();

        let (cursor, complete) = engine.gc_batch(9, None, 1, 3);
        assert!(!complete);
        let cursor = cursor.unwrap();
        assert_eq!(cursor.key, key);
        assert!(cursor.inclusive);
        assert_eq!(engine.version_at(&key, 3), None);

        let (cursor, complete) = engine.gc_batch(9, Some(&cursor), 1, 3);
        assert!(!complete);
        let cursor = cursor.unwrap();
        assert_eq!(cursor.key, key);
        assert!(cursor.inclusive);

        let (cursor, complete) = engine.gc_batch(9, Some(&cursor), 1, 3);
        assert!(complete);
        assert_eq!(cursor, None);
        assert_eq!(engine.bytes_held(), 2);
        assert!(engine.inner.get(&key).unwrap().capacity() < initial_capacity);
        assert_eq!(engine.read(&key, 9, 0).unwrap(), Some(vec![9]));
        assert_eq!(engine.read(&key, 10, 0).unwrap(), Some(vec![10]));
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
    fn table_operations_are_scoped_to_the_requested_ordered_key_range() {
        let mut engine = Engine::new();
        for key in [
            RecordKey::new("a", "db", "messages", b"row"),
            RecordKey::new("tenant", "a", "messages", b"row"),
            RecordKey::new("tenant", "db", "message-archive", b"row"),
            RecordKey::new("tenant", "db", "messages", b""),
            RecordKey::new("tenant", "db", "messages", b"row"),
            RecordKey::new("tenant", "db", "profiles", b"row"),
            RecordKey::new("tenant", "other", "messages", b"row"),
            RecordKey::new("z", "db", "messages", b"row"),
        ] {
            engine.apply(key, 1, Some(b"value".to_vec())).unwrap();
        }

        assert_eq!(
            engine.table_names("tenant", "db"),
            vec![
                String::from("message-archive"),
                String::from("messages"),
                String::from("profiles")
            ]
        );
        assert_eq!(engine.table_bytes("tenant", "db", "messages"), 13);
        assert_eq!(
            engine
                .export_table("tenant", "db", "messages")
                .iter()
                .map(|(pk, _)| pk.clone())
                .collect::<Vec<_>>(),
            vec![Vec::new(), b"row".to_vec()]
        );
        assert_eq!(engine.drop_table("tenant", "db", "messages"), 2);
        assert_eq!(
            engine.table_names("tenant", "db"),
            vec![String::from("message-archive"), String::from("profiles")]
        );
        assert_eq!(engine.len(), 6);
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
        assert_eq!(engine.read(&key, 10, 50_000).unwrap(), Some(b"a".to_vec()));
        assert_eq!(engine.read(&key, 10, 100_000).unwrap(), None);
        assert_eq!(engine.read(&key, 10, 500_000).unwrap(), None);
        engine.apply(key.clone(), 20, Some(b"b".to_vec())).unwrap();
        assert_eq!(engine.read(&key, 20, 500_000).unwrap(), Some(b"b".to_vec()));
        assert_eq!(engine.scan("t", "d", "s", 20, 500_000, 10).len(), 1);
        assert_eq!(engine.scan("t", "d", "s", 10, 500_000, 10).len(), 0);
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
        assert!(engine.range(&end, &start, 1, 0, 1).is_empty());
        assert!(engine.range(&start, &start, 1, 0, 1).is_empty());
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
    fn snapshot_decode_does_not_overallocate_single_version_history() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "users", b"1");
        engine.apply(key.clone(), 10, Some(b"a".to_vec())).unwrap();

        let raw = engine.encode_snapshot().unwrap();
        let decoded = Engine::decode_snapshot(&raw).unwrap();

        assert!(decoded.inner.get(&key).unwrap().capacity() <= 4);
    }

    #[test]
    fn snapshot_preserves_expiry() {
        let mut engine = Engine::new();
        let key = RecordKey::new("t", "d", "s", b"k");
        engine.apply_with_expiry(key.clone(), 5, Some(b"v".to_vec()), 50).unwrap();
        let raw = engine.encode_snapshot().unwrap();
        let back = Engine::decode_snapshot(&raw).unwrap();
        assert_eq!(back.read(&key, 5, 10_000).unwrap(), Some(b"v".to_vec()));
        assert_eq!(back.read(&key, 5, 60_000).unwrap(), None);
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
    fn bounded_snapshot_append_rejects_overflow_without_appending() {
        let mut out = vec![1, 2];
        push_bounded_bytes(&mut out, &[3], 3).unwrap();
        assert!(matches!(
            push_bounded_bytes(&mut out, &[4], 3),
            Err(RymeError::Overload(message)) if message == "snapshot"
        ));
        assert_eq!(out, [1, 2, 3]);
    }

    #[test]
    fn snapshot_rejects_empty_history_even_when_other_rows_have_versions() {
        let mut engine = Engine::new();
        engine.apply(RecordKey::new("t", "d", "s", b"a"), 1, Some(b"a".to_vec())).unwrap();
        engine.apply(RecordKey::new("t", "d", "s", b"z"), 2, Some(b"z".to_vec())).unwrap();
        let mut raw = engine.encode_snapshot().unwrap();

        // The fixed header and the first row's three 16-bit-length one-byte
        // fields and PK chunk occupy 34 bytes; then comes its version count.
        raw[34..38].copy_from_slice(&0u32.to_be_bytes());
        raw.drain(38..60); // Remove that row's encoded one-version history.
        refresh_snapshot_checksum(&mut raw);

        assert!(matches!(Engine::decode_snapshot(&raw), Err(RymeError::Corrupt(_))));
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
        assert_eq!(
            routing_key(SCOPED_ROUTING_VERSION, "tenant", "db\0branch", "docs", b"k").unwrap(),
            b"tenant\0db\0\xffbranch\0docs\0k"
        );
        assert!(routing_key(9, "tenant", "db", "docs", b"k").is_err());
    }

    #[test]
    fn record_scopes_reject_nul_before_mutation_and_on_snapshot_load() {
        let mut engine = Engine::new();
        let invalid = RecordKey::new("t\0other", "d", "items", b"k");
        assert!(matches!(
            engine.apply(invalid, 1, Some(b"value".to_vec())),
            Err(RymeError::InvalidArgument(message)) if message == "record scope"
        ));
        assert!(engine.is_empty());
        assert!(engine
            .import_table("t\0other", "d", "items", vec![(b"k".to_vec(), vec![])])
            .is_err());
        assert!(engine.is_empty());

        let mut snapshot_engine = Engine::new();
        snapshot_engine
            .apply(RecordKey::new("t", "d", "items", b"k"), 1, Some(b"value".to_vec()))
            .unwrap();
        let mut raw = snapshot_engine.encode_snapshot().unwrap();
        // The tenant begins immediately after its 16-bit length at offset 20.
        raw[22] = 0;
        refresh_snapshot_checksum(&mut raw);
        assert!(matches!(Engine::decode_snapshot(&raw), Err(RymeError::Corrupt(_))));
    }

    #[test]
    fn branch_internal_database_namespace_survives_storage_roundtrip() {
        let mut engine = Engine::new();
        let key = RecordKey::new("tenant", "app\0branch\0preview", "items", b"key");
        engine.apply(key.clone(), 1, Some(b"branch value".to_vec())).unwrap();
        let restored = Engine::decode_snapshot(&engine.encode_snapshot().unwrap()).unwrap();
        assert_eq!(restored.read(&key, 1, 0).unwrap(), Some(b"branch value".to_vec()));
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
    fn scoped_table_bounds_support_internal_branch_database_namespaces() {
        let database = "app\0branch\0preview";
        let (start, end) =
            table_routing_bounds(SCOPED_ROUTING_VERSION, "tenant", database, "docs").unwrap();
        let row = routing_key(SCOPED_ROUTING_VERSION, "tenant", database, "docs", b"key").unwrap();
        let adjacent =
            routing_key(SCOPED_ROUTING_VERSION, "tenant", "app", "branch", b"key").unwrap();

        assert!(row >= start && row < end);
        assert!(!(adjacent >= start && adjacent < end));
    }

    #[test]
    fn routing_range_export_seeks_scoped_rows_and_filters_mvcc_history() {
        let mut engine = Engine::new();
        for (tenant, database, table, pk, ts, value) in [
            ("t", "d", "docs", b"a".as_slice(), 1, b"a".as_slice()),
            ("t", "d", "docs", b"b".as_slice(), 2, b"b".as_slice()),
            ("t", "d", "docs", b"c".as_slice(), 3, b"old-c".as_slice()),
            ("t", "d", "docs", b"c".as_slice(), 5, b"new-c".as_slice()),
            ("t", "d", "docs", b"d".as_slice(), 4, b"d".as_slice()),
            ("other", "d", "docs", b"b".as_slice(), 2, b"other-b".as_slice()),
            ("t", "other-db", "docs", b"b".as_slice(), 2, b"other-db-b".as_slice()),
            ("t", "d", "other-table", b"b".as_slice(), 2, b"other-table-b".as_slice()),
        ] {
            engine
                .apply(RecordKey::new(tenant, database, table, pk), ts, Some(value.to_vec()))
                .unwrap();
        }

        let start = routing_key(SCOPED_ROUTING_VERSION, "t", "d", "docs", b"b").unwrap();
        let end = routing_key(SCOPED_ROUTING_VERSION, "t", "d", "docs", b"e").unwrap();
        let rows = engine
            .export_table_routing_range(
                SCOPED_ROUTING_VERSION,
                "t",
                "d",
                "docs",
                &start,
                &end,
                4,
                10,
            )
            .unwrap();
        assert_eq!(
            rows.iter().map(|(pk, _)| pk.as_slice()).collect::<Vec<_>>(),
            [b"b", b"c", b"d"]
        );
        let c_versions = &rows[1].1;
        assert_eq!(c_versions.len(), 1);
        assert_eq!(c_versions[0].commit_ts, 3);
        assert_eq!(c_versions[0].value.as_deref(), Some(b"old-c".as_slice()));

        let limited = engine
            .export_table_routing_range(
                SCOPED_ROUTING_VERSION,
                "t",
                "d",
                "docs",
                &start,
                &end,
                4,
                2,
            )
            .unwrap();
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[1].0, b"c");
    }

    #[test]
    fn legacy_routing_range_export_includes_matching_keys_across_scopes() {
        let mut engine = Engine::new();
        for (tenant, database, pk, value) in [
            ("t1", "d1", b"b".as_slice(), b"one".as_slice()),
            ("t2", "d2", b"c".as_slice(), b"two".as_slice()),
            ("t3", "d3", b"z".as_slice(), b"outside".as_slice()),
        ] {
            engine
                .apply(RecordKey::new(tenant, database, "docs", pk), 1, Some(value.to_vec()))
                .unwrap();
        }
        let mut rows = engine
            .export_table_routing_range(
                LEGACY_ROUTING_VERSION,
                "t1",
                "d1",
                "docs",
                b"docs\0b",
                b"docs\0d",
                1,
                10,
            )
            .unwrap();
        rows.extend(
            engine
                .export_table_routing_range(
                    LEGACY_ROUTING_VERSION,
                    "t2",
                    "d2",
                    "docs",
                    b"docs\0b",
                    b"docs\0d",
                    1,
                    10,
                )
                .unwrap(),
        );
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"b");
        assert_eq!(rows[1].0, b"c");
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
