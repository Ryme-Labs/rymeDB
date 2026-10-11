use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

type BranchChange = (Vec<u8>, Option<Vec<u8>>);

#[cfg(unix)]
fn sync_branch_directory(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_branch_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    pub id: String,
    #[serde(default)]
    pub tenant: String,
    pub parent_id: Option<String>,
    pub base_commit_ts: u64,
    #[serde(default)]
    pub storage_epoch: u64,
    pub manifest_id: String,
    pub schema_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub id: String,
    pub segments: Vec<String>,
    pub wal_start: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BranchManager {
    branches: HashMap<String, Branch>,
    manifests: HashMap<String, Manifest>,
    refs: HashMap<String, usize>,
    #[serde(default)]
    next_storage_epoch: u64,
}

impl BranchManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load(path: &Path) -> Result<Self> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Self::new()),
            Err(error) => return Err(error.into()),
        };
        let mut manager: Self = serde_json::from_slice(&bytes)
            .map_err(|error| RymeError::Corrupt(format!("branch metadata: {error}")))?;
        let mut normalized = HashMap::with_capacity(manager.branches.len());
        for (stored_key, mut branch) in manager.branches.drain() {
            let tenant = if branch.tenant.is_empty() {
                String::from("default")
            } else {
                branch.tenant.clone()
            };
            branch.tenant = tenant.clone();
            let key = if stored_key.contains('\0') {
                stored_key
            } else {
                scoped_key(&tenant, &branch.id)
            };
            normalized.insert(key, branch);
        }
        manager.branches = normalized;
        if let Some(highest_epoch) = manager.branches.values().map(|b| b.storage_epoch).max() {
            manager.next_storage_epoch =
                manager.next_storage_epoch.max(highest_epoch.saturating_add(1));
        }
        manager.rebuild_refs()?;
        Ok(manager)
    }

    pub fn persist(&self, path: &Path) -> Result<()> {
        self.validate()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|error| RymeError::Internal(format!("branch metadata: {error}")))?;
        let temporary = path.with_extension("tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_data()?;
        }
        std::fs::rename(temporary, path)?;
        if let Some(parent) = path.parent() {
            sync_branch_directory(parent)?;
        }
        Ok(())
    }

    fn rebuild_refs(&mut self) -> Result<()> {
        self.validate_branches()?;
        let mut refs = HashMap::new();
        for branch in self.branches.values() {
            *refs.entry(branch.manifest_id.clone()).or_insert(0) += 1;
        }
        self.refs = refs;
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        self.validate_branches()?;
        let mut expected = HashMap::new();
        for branch in self.branches.values() {
            *expected.entry(branch.manifest_id.clone()).or_insert(0) += 1;
        }
        if self.refs != expected {
            return Err(RymeError::Corrupt(String::from("branch manifest references")));
        }
        Ok(())
    }

    fn validate_branches(&self) -> Result<()> {
        for (key, branch) in &self.branches {
            if key != &scoped_key(&branch.tenant, &branch.id)
                || branch.id.is_empty()
                || branch.id.len() > 128
                || branch.id.contains('/')
                || branch.id.contains("..")
                || branch.id.contains('\0')
            {
                return Err(RymeError::Corrupt(String::from("branch identity")));
            }
            if !self.manifests.contains_key(&branch.manifest_id) {
                return Err(RymeError::Corrupt(String::from("branch manifest")));
            }
            if let Some(parent) = &branch.parent_id {
                if !self.branches.contains_key(&scoped_key(&branch.tenant, parent)) {
                    return Err(RymeError::Corrupt(String::from("branch parent")));
                }
            }
        }
        Ok(())
    }

    fn allocate_storage_epoch(&mut self) -> Result<u64> {
        let epoch = self.next_storage_epoch.max(1);
        if epoch == u64::MAX {
            return Err(RymeError::Conflict(String::from("branch storage epoch exhausted")));
        }
        self.next_storage_epoch = epoch + 1;
        Ok(epoch)
    }

    pub fn create_root(&mut self, id: String, manifest: Manifest) -> Result<()> {
        self.create_root_for("default", id, manifest)
    }

    pub fn create_root_for(&mut self, tenant: &str, id: String, manifest: Manifest) -> Result<()> {
        if id.is_empty()
            || id.len() > 128
            || id.contains('/')
            || id.contains("..")
            || id.contains('\0')
        {
            return Err(RymeError::InvalidArgument(String::from("branch")));
        }
        let key = scoped_key(tenant, &id);
        if self.branches.contains_key(&key) {
            return Err(RymeError::Conflict(String::from("branch")));
        }
        self.refs.insert(manifest.id.clone(), 1);
        self.manifests.insert(manifest.id.clone(), manifest.clone());
        self.branches.insert(
            key,
            Branch {
                id,
                tenant: tenant.to_string(),
                parent_id: None,
                base_commit_ts: 0,
                storage_epoch: 0,
                manifest_id: manifest.id,
                schema_version: 1,
            },
        );
        Ok(())
    }

    pub fn create_child(&mut self, id: String, parent: &str, base_commit_ts: u64) -> Result<()> {
        self.create_child_for("default", id, parent, base_commit_ts)
    }

    pub fn create_child_for(
        &mut self,
        tenant: &str,
        id: String,
        parent: &str,
        base_commit_ts: u64,
    ) -> Result<()> {
        if id.is_empty()
            || id.len() > 128
            || id.contains('/')
            || id.contains("..")
            || id.contains('\0')
        {
            return Err(RymeError::InvalidArgument(String::from("branch")));
        }
        let key = scoped_key(tenant, &id);
        if self.branches.contains_key(&key) {
            return Err(RymeError::Conflict(String::from("branch")));
        }
        let parent_branch = self
            .branches
            .get(&scoped_key(tenant, parent))
            .ok_or_else(|| RymeError::NotFound(String::from("parent")))?
            .clone();
        if parent_branch.parent_id.is_some() {
            return Err(RymeError::InvalidArgument(String::from("nested branch parent")));
        }
        let storage_epoch = self.allocate_storage_epoch()?;
        let manifest_id = parent_branch.manifest_id.clone();
        *self.refs.entry(manifest_id.clone()).or_insert(0) += 1;
        self.branches.insert(
            key,
            Branch {
                id,
                tenant: tenant.to_string(),
                parent_id: Some(parent.to_string()),
                base_commit_ts,
                storage_epoch,
                manifest_id,
                schema_version: parent_branch.schema_version,
            },
        );
        Ok(())
    }

    pub fn delete(&mut self, id: &str) -> Result<Vec<String>> {
        self.delete_for("default", id)
    }

    pub fn delete_for(&mut self, tenant: &str, id: &str) -> Result<Vec<String>> {
        if self
            .branches
            .values()
            .any(|branch| branch.tenant == tenant && branch.parent_id.as_deref() == Some(id))
        {
            return Err(RymeError::Conflict(String::from("branch has children")));
        }
        let branch = self
            .branches
            .remove(&scoped_key(tenant, id))
            .ok_or_else(|| RymeError::NotFound(String::from("branch")))?;
        let count = self.refs.entry(branch.manifest_id.clone()).or_insert(1);
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.refs.remove(&branch.manifest_id);
            if let Some(manifest) = self.manifests.remove(&branch.manifest_id) {
                return Ok(self.unreferenced_segments(&manifest));
            }
        }
        Ok(Vec::new())
    }

    pub fn diff(&self, left: &str, right: &str) -> Result<(Vec<String>, Vec<String>)> {
        self.diff_for("default", left, right)
    }

    pub fn diff_for(
        &self,
        tenant: &str,
        left: &str,
        right: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let left_branch = self
            .branches
            .get(&scoped_key(tenant, left))
            .ok_or_else(|| RymeError::NotFound(String::from("branch")))?;
        let right_branch = self
            .branches
            .get(&scoped_key(tenant, right))
            .ok_or_else(|| RymeError::NotFound(String::from("branch")))?;
        let left_manifest = self
            .manifests
            .get(&left_branch.manifest_id)
            .ok_or_else(|| RymeError::NotFound(String::from("manifest")))?;
        let right_manifest = self
            .manifests
            .get(&right_branch.manifest_id)
            .ok_or_else(|| RymeError::NotFound(String::from("manifest")))?;
        let left_set: HashSet<&str> = left_manifest.segments.iter().map(|s| s.as_str()).collect();
        let right_set: HashSet<&str> = right_manifest.segments.iter().map(|s| s.as_str()).collect();
        let only_left = left_set.difference(&right_set).map(|s| s.to_string()).collect();
        let only_right = right_set.difference(&left_set).map(|s| s.to_string()).collect();
        Ok((only_left, only_right))
    }

    pub fn get(&self, id: &str) -> Result<Branch> {
        self.get_for("default", id)
    }

    pub fn get_for(&self, tenant: &str, id: &str) -> Result<Branch> {
        self.branches
            .get(&scoped_key(tenant, id))
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("branch")))
    }

    pub fn list(&self) -> Vec<Branch> {
        self.list_for("default")
    }

    pub fn list_for(&self, tenant: &str) -> Vec<Branch> {
        let mut out: Vec<Branch> =
            self.branches.values().filter(|branch| branch.tenant == tenant).cloned().collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    pub fn reset(&mut self, id: &str, base_commit_ts: u64) -> Result<Branch> {
        self.reset_for("default", id, base_commit_ts)
    }

    pub fn reset_for(&mut self, tenant: &str, id: &str, base_commit_ts: u64) -> Result<Branch> {
        let key = scoped_key(tenant, id);
        if !self.branches.contains_key(&key) {
            return Err(RymeError::NotFound(String::from("branch")));
        }
        let storage_epoch = self.allocate_storage_epoch()?;
        let branch = self
            .branches
            .get_mut(&key)
            .ok_or_else(|| RymeError::NotFound(String::from("branch")))?;
        branch.base_commit_ts = base_commit_ts;
        branch.storage_epoch = storage_epoch;
        Ok(branch.clone())
    }

    pub fn promote(&mut self, id: &str) -> Result<Branch> {
        self.promote_for("default", id)
    }

    pub fn promote_for(&mut self, tenant: &str, id: &str) -> Result<Branch> {
        let child = self
            .branches
            .get(&scoped_key(tenant, id))
            .ok_or_else(|| RymeError::NotFound(String::from("branch")))?
            .clone();
        let parent_id = child
            .parent_id
            .clone()
            .ok_or_else(|| RymeError::InvalidArgument(String::from("root")))?;
        let parent = self
            .branches
            .get_mut(&scoped_key(tenant, &parent_id))
            .ok_or_else(|| RymeError::NotFound(String::from("parent")))?;
        let old_manifest = parent.manifest_id.clone();
        parent.manifest_id.clone_from(&child.manifest_id);
        parent.schema_version = parent.schema_version.max(child.schema_version) + 1;
        parent.base_commit_ts = child.base_commit_ts;
        *self.refs.entry(child.manifest_id.clone()).or_insert(0) += 1;
        let count = self.refs.entry(old_manifest.clone()).or_insert(1);
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.refs.remove(&old_manifest);
            self.manifests.remove(&old_manifest);
        }
        Ok(parent.clone())
    }

    fn unreferenced_segments(&self, manifest: &Manifest) -> Vec<String> {
        let mut live: HashSet<&str> = HashSet::new();
        for other in self.manifests.values() {
            for segment in &other.segments {
                live.insert(segment.as_str());
            }
        }
        manifest.segments.iter().filter(|s| !live.contains(s.as_str())).cloned().collect()
    }
}

fn scoped_key(tenant: &str, branch: &str) -> String {
    format!("{tenant}\0{branch}")
}

const BRANCH_VALUE_PREFIX: &[u8] = b"\0RYME_BRANCH\x01";

/// A copy-on-write view over a transactional backend.
///
/// Parent data is read at `base_commit_ts`. Mutations are written to a
/// branch-local database namespace, so branch creation remains metadata-only
/// and only changed rows consume additional storage. A tombstone in that
/// namespace hides a parent row without deleting the parent version.
#[derive(Debug, Clone)]
pub struct BranchBackend<B> {
    base: B,
    branch_database: String,
    base_commit_ts: u64,
    overlay: bool,
}

impl<B> BranchBackend<B>
where
    B: ryme_txn::TxnBackend,
{
    pub fn passthrough(base: B, _database: String) -> Self {
        Self { base, branch_database: String::new(), base_commit_ts: 0, overlay: false }
    }

    pub fn new(
        base: B,
        database: String,
        branch: String,
        base_commit_ts: u64,
        storage_epoch: u64,
    ) -> Self {
        let branch_database = if storage_epoch == 0 {
            format!("{database}\0branch\0{branch}")
        } else {
            format!("{database}\0branch\0{branch}\0epoch\0{storage_epoch}")
        };
        Self { branch_database, base, base_commit_ts, overlay: true }
    }

    pub fn local_database(&self) -> Option<&str> {
        self.overlay.then_some(self.branch_database.as_str())
    }

    pub fn changes(&self, tenant: &str, table: &str) -> Result<Vec<BranchChange>> {
        if !self.overlay {
            return Ok(Vec::new());
        }
        let mut txn = self.base.begin();
        Ok(self.overlay_rows(&mut txn, tenant, table)?.into_iter().collect())
    }

    fn local_key(&self, key: &ryme_storage::RecordKey) -> ryme_storage::RecordKey {
        ryme_storage::RecordKey::new(&key.tenant, &self.branch_database, &key.table, &key.pk)
    }

    fn parent_transaction(&self) -> ryme_txn::Transaction {
        let mut txn = self.base.begin();
        txn.restamp(self.base_commit_ts);
        txn
    }

    fn encode(value: Option<&[u8]>) -> Vec<u8> {
        let mut encoded =
            Vec::with_capacity(BRANCH_VALUE_PREFIX.len() + 1 + value.map_or(0, |v| v.len()));
        encoded.extend_from_slice(BRANCH_VALUE_PREFIX);
        match value {
            Some(value) => {
                encoded.push(0);
                encoded.extend_from_slice(value);
            }
            None => encoded.push(1),
        }
        encoded
    }

    fn encode_with_expiry(value: &[u8], expires_at: u64) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(BRANCH_VALUE_PREFIX.len() + 1 + 8 + value.len());
        encoded.extend_from_slice(BRANCH_VALUE_PREFIX);
        encoded.push(2);
        encoded.extend_from_slice(&expires_at.to_be_bytes());
        encoded.extend_from_slice(value);
        encoded
    }

    fn decode_with_expiry(value: &[u8]) -> Option<(Option<Vec<u8>>, u64)> {
        if !value.starts_with(BRANCH_VALUE_PREFIX) {
            return Some((Some(value.to_vec()), 0));
        }
        match value.get(BRANCH_VALUE_PREFIX.len()) {
            Some(0) => Some((Some(value[BRANCH_VALUE_PREFIX.len() + 1..].to_vec()), 0)),
            Some(1) => Some((None, 0)),
            Some(2) => {
                let expiry_start = BRANCH_VALUE_PREFIX.len() + 1;
                let expiry_end = expiry_start + 8;
                let expiry = value.get(expiry_start..expiry_end)?;
                let expires_at = u64::from_be_bytes(expiry.try_into().ok()?);
                Some((Some(value[expiry_end..].to_vec()), expires_at))
            }
            _ => None,
        }
    }

    fn decode(value: &[u8]) -> Option<Option<Vec<u8>>> {
        let (value, expires_at) = Self::decode_with_expiry(value)?;
        if expires_at != 0 && expires_at <= ryme_txn::now_unix() {
            Some(None)
        } else {
            Some(value)
        }
    }

    fn overlay_rows(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        table: &str,
    ) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
        let mut rows = BTreeMap::new();
        let local_rows = self.base.scan(txn, tenant, &self.branch_database, table, usize::MAX)?;
        for (pk, value) in local_rows {
            if let Some(decoded) = Self::decode(&value) {
                rows.insert(pk, decoded);
            }
        }
        Ok(rows)
    }

    async fn overlay_rows_async(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        table: &str,
    ) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
        let mut rows = BTreeMap::new();
        let local_rows =
            self.scan_all_async(txn, tenant, &self.branch_database, table, usize::MAX).await?;
        for (pk, value) in local_rows {
            if let Some(decoded) = Self::decode(&value) {
                rows.insert(pk, decoded);
            }
        }
        Ok(rows)
    }

    fn scan_all(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        const PAGE: usize = 1024;
        let page_limit = limit.min(PAGE);
        let mut rows = self.base.scan(txn, tenant, database, table, page_limit)?;
        while rows.len() < limit && rows.len() >= page_limit && !rows.is_empty() {
            let Some(last) = rows.last().map(|(pk, _)| pk.clone()) else { break };
            let next_limit = (limit - rows.len()).min(PAGE);
            let next = self.base.scan_after(txn, tenant, database, table, &last, next_limit)?;
            if next.is_empty() {
                break;
            }
            rows.extend(next);
        }
        Ok(rows)
    }

    fn scan_all_after(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        const PAGE: usize = 1024;
        let page_limit = limit.min(PAGE);
        let mut rows =
            self.base.scan_after(txn, tenant, database, table, start_after, page_limit)?;
        while rows.len() < limit && rows.len() >= page_limit && !rows.is_empty() {
            let Some(last) = rows.last().map(|(pk, _)| pk.clone()) else { break };
            let next_limit = (limit - rows.len()).min(PAGE);
            let next = self.base.scan_after(txn, tenant, database, table, &last, next_limit)?;
            if next.is_empty() {
                break;
            }
            rows.extend(next);
        }
        Ok(rows)
    }

    async fn scan_all_async(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        const PAGE: usize = 1024;
        let page_limit = limit.min(PAGE);
        let mut rows = self.base.scan_async(txn, tenant, database, table, page_limit).await?;
        while rows.len() < limit && rows.len() >= page_limit && !rows.is_empty() {
            let Some(last) = rows.last().map(|(pk, _)| pk.clone()) else { break };
            let next_limit = (limit - rows.len()).min(PAGE);
            let next =
                self.base.scan_after_async(txn, tenant, database, table, &last, next_limit).await?;
            if next.is_empty() {
                break;
            }
            rows.extend(next);
        }
        Ok(rows)
    }

    async fn scan_all_after_async(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        const PAGE: usize = 1024;
        let page_limit = limit.min(PAGE);
        let mut rows = self
            .base
            .scan_after_async(txn, tenant, database, table, start_after, page_limit)
            .await?;
        while rows.len() < limit && rows.len() >= page_limit && !rows.is_empty() {
            let Some(last) = rows.last().map(|(pk, _)| pk.clone()) else { break };
            let next_limit = (limit - rows.len()).min(PAGE);
            let next =
                self.base.scan_after_async(txn, tenant, database, table, &last, next_limit).await?;
            if next.is_empty() {
                break;
            }
            rows.extend(next);
        }
        Ok(rows)
    }
}

impl<B> ryme_txn::TxnBackend for BranchBackend<B>
where
    B: ryme_txn::TxnBackend,
{
    fn begin(&self) -> ryme_txn::Transaction {
        self.base.begin()
    }

    fn get(
        &self,
        txn: &mut ryme_txn::Transaction,
        key: &ryme_storage::RecordKey,
    ) -> Result<Option<Vec<u8>>> {
        if !self.overlay {
            return self.base.get(txn, key);
        }
        let local = self.local_key(key);
        if let Some(write) = txn.writes().get(&local) {
            return Ok(write.value.as_deref().and_then(Self::decode).flatten());
        }
        if let Some(value) = self.base.get(txn, &local)? {
            return Ok(Self::decode(&value).flatten());
        }
        if self.base.expires_at_at(&local, txn.read_ts)?.is_some() {
            return Ok(None);
        }
        let mut parent_txn = self.parent_transaction();
        self.base.get(&mut parent_txn, key)
    }

    fn get_async<'a>(
        &'a self,
        txn: &'a mut ryme_txn::Transaction,
        key: &'a ryme_storage::RecordKey,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Vec<u8>>>> + Send + 'a>>
    {
        if !self.overlay {
            return self.base.get_async(txn, key);
        }
        let local = self.local_key(key);
        let base = self.base.clone();
        let base_commit_ts = self.base_commit_ts;
        Box::pin(async move {
            if let Some(write) = txn.writes().get(&local) {
                return Ok(write.value.as_deref().and_then(Self::decode).flatten());
            }
            if let Some(value) = base.get_async(txn, &local).await? {
                return Ok(Self::decode(&value).flatten());
            }
            if base.expires_at_at(&local, txn.read_ts)?.is_some() {
                return Ok(None);
            }
            let mut parent_txn = base.begin();
            parent_txn.restamp(base_commit_ts);
            base.get_async(&mut parent_txn, key).await
        })
    }

    fn scan_async<'a>(
        &'a self,
        txn: &'a mut ryme_txn::Transaction,
        tenant: &'a str,
        database: &'a str,
        table: &'a str,
        limit: usize,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>>> + Send + 'a>,
    > {
        if !self.overlay {
            return self.base.scan_async(txn, tenant, database, table, limit);
        }
        let base_commit_ts = self.base_commit_ts;
        Box::pin(async move {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let overlay_rows = self.overlay_rows_async(txn, tenant, table).await?;
            let parent_limit = limit.saturating_add(overlay_rows.len());
            let mut parent_txn = self.base.begin();
            parent_txn.restamp(base_commit_ts);
            let parent =
                self.scan_all_async(&mut parent_txn, tenant, database, table, parent_limit).await?;
            let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = parent.into_iter().collect();
            for (pk, value) in overlay_rows {
                match value {
                    Some(value) => {
                        merged.insert(pk, value);
                    }
                    None => {
                        merged.remove(&pk);
                    }
                }
            }
            Ok(merged.into_iter().take(limit).collect())
        })
    }

    fn scan_after_async<'a>(
        &'a self,
        txn: &'a mut ryme_txn::Transaction,
        tenant: &'a str,
        database: &'a str,
        table: &'a str,
        start_after: &'a [u8],
        limit: usize,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>>> + Send + 'a>,
    > {
        if !self.overlay {
            return self.base.scan_after_async(txn, tenant, database, table, start_after, limit);
        }
        let base_commit_ts = self.base_commit_ts;
        Box::pin(async move {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let overlay_rows = self.overlay_rows_async(txn, tenant, table).await?;
            let parent_limit = limit.saturating_add(overlay_rows.len());
            let mut parent_txn = self.base.begin();
            parent_txn.restamp(base_commit_ts);
            let parent = self
                .scan_all_after_async(
                    &mut parent_txn,
                    tenant,
                    database,
                    table,
                    start_after,
                    parent_limit,
                )
                .await?;
            let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = parent.into_iter().collect();
            for (pk, value) in overlay_rows {
                if pk.as_slice() <= start_after {
                    continue;
                }
                match value {
                    Some(value) => {
                        merged.insert(pk, value);
                    }
                    None => {
                        merged.remove(&pk);
                    }
                }
            }
            Ok(merged.into_iter().take(limit).collect())
        })
    }

    fn put(&self, txn: &mut ryme_txn::Transaction, key: ryme_storage::RecordKey, value: Vec<u8>) {
        if self.overlay {
            self.base.put(txn, self.local_key(&key), Self::encode(Some(&value)));
        } else {
            self.base.put(txn, key, value);
        }
    }

    fn put_with_ttl(
        &self,
        txn: &mut ryme_txn::Transaction,
        key: ryme_storage::RecordKey,
        value: Vec<u8>,
        expires_at: u64,
    ) {
        if self.overlay {
            self.base.put(
                txn,
                self.local_key(&key),
                Self::encode_with_expiry(&value, expires_at),
            );
        } else {
            self.base.put_with_ttl(txn, key, value, expires_at);
        }
    }

    fn delete(&self, txn: &mut ryme_txn::Transaction, key: ryme_storage::RecordKey) {
        if self.overlay {
            self.base.put(txn, self.local_key(&key), Self::encode(None));
        } else {
            self.base.delete(txn, key);
        }
    }

    fn expires_at(&self, key: &ryme_storage::RecordKey) -> Result<Option<u64>> {
        if !self.overlay {
            return self.base.expires_at(key);
        }
        let local = self.local_key(key);
        let mut txn = self.base.begin();
        if let Some(value) = self.base.get(&mut txn, &local)? {
            let (decoded, mut expires_at) = Self::decode_with_expiry(&value)
                .ok_or_else(|| RymeError::Corrupt(String::from("branch value")))?;
            if decoded.is_some() && expires_at == 0 {
                expires_at = self.base.expires_at_at(&local, u64::MAX)?.unwrap_or(0);
            }
            return Ok(if decoded.is_some() { Some(expires_at) } else { Some(1) });
        }
        if let Some(expires_at) = self.base.expires_at_at(&local, txn.read_ts)? {
            return Ok(Some(expires_at));
        }
        self.base.expires_at_at(key, self.base_commit_ts)
    }

    fn expires_at_at(&self, key: &ryme_storage::RecordKey, read_ts: u64) -> Result<Option<u64>> {
        if !self.overlay {
            return self.base.expires_at_at(key, read_ts);
        }
        let local = self.local_key(key);
        let mut txn = self.base.begin();
        txn.restamp(read_ts);
        if let Some(value) = self.base.get(&mut txn, &local)? {
            let (decoded, mut expires_at) = Self::decode_with_expiry(&value)
                .ok_or_else(|| RymeError::Corrupt(String::from("branch value")))?;
            if decoded.is_some() && expires_at == 0 {
                expires_at = self.base.expires_at_at(&local, read_ts)?.unwrap_or(0);
            }
            return Ok(if decoded.is_some() { Some(expires_at) } else { Some(1) });
        }
        if let Some(expires_at) = self.base.expires_at_at(&local, read_ts)? {
            return Ok(Some(expires_at));
        }
        self.base.expires_at_at(key, self.base_commit_ts.min(read_ts))
    }

    fn commit(
        &self,
        txn: ryme_txn::Transaction,
    ) -> impl std::future::Future<Output = Result<u64>> + Send {
        let base = self.base.clone();
        async move { base.commit(txn).await }
    }

    fn scan(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if !self.overlay {
            return self.base.scan(txn, tenant, database, table, limit);
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let overlay = self.overlay_rows(txn, tenant, table)?;
        let parent_limit = limit.saturating_add(overlay.len());
        let mut parent_txn = self.parent_transaction();
        let parent = self.scan_all(&mut parent_txn, tenant, database, table, parent_limit)?;
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = parent.into_iter().collect();
        for (pk, value) in overlay {
            match value {
                Some(value) => {
                    merged.insert(pk, value);
                }
                None => {
                    merged.remove(&pk);
                }
            }
        }
        Ok(merged.into_iter().take(limit).collect())
    }

    fn scan_after(
        &self,
        txn: &mut ryme_txn::Transaction,
        tenant: &str,
        database: &str,
        table: &str,
        start_after: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if !self.overlay {
            return self.base.scan_after(txn, tenant, database, table, start_after, limit);
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let overlay = self.overlay_rows(txn, tenant, table)?;
        let parent_limit = limit.saturating_add(overlay.len());
        let mut parent_txn = self.parent_transaction();
        let parent = self.scan_all_after(
            &mut parent_txn,
            tenant,
            database,
            table,
            start_after,
            parent_limit,
        )?;
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = parent.into_iter().collect();
        for (pk, value) in overlay {
            if pk.as_slice() <= start_after {
                continue;
            }
            match value {
                Some(value) => {
                    merged.insert(pk, value);
                }
                None => {
                    merged.remove(&pk);
                }
            }
        }
        Ok(merged.into_iter().take(limit).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ryme_txn::TxnBackend;

    fn root_manager() -> BranchManager {
        let mut manager = BranchManager::new();
        manager
            .create_root(
                String::from("main"),
                Manifest {
                    id: String::from("genesis"),
                    segments: vec![String::from("a")],
                    wal_start: 0,
                },
            )
            .unwrap();
        manager
    }

    #[test]
    fn child_shares_manifest() {
        let mut manager = root_manager();
        manager.create_child(String::from("preview"), "main", 7).unwrap();
        let child = manager.get("preview").unwrap();
        assert_eq!(child.base_commit_ts, 7);
        let (left, right) = manager.diff("main", "preview").unwrap();
        assert!(left.is_empty() && right.is_empty());
    }

    #[test]
    fn reset_moves_base() {
        let mut manager = root_manager();
        manager.create_child(String::from("preview"), "main", 7).unwrap();
        let branch = manager.reset("preview", 42).unwrap();
        assert_eq!(branch.base_commit_ts, 42);
        assert_eq!(branch.storage_epoch, 2);
        assert!(manager.reset("ghost", 1).is_err());
    }

    #[test]
    fn promote_swaps_parent_manifest() {
        let mut manager = root_manager();
        manager.create_child(String::from("preview"), "main", 7).unwrap();
        let parent = manager.promote("preview").unwrap();
        assert_eq!(parent.id, "main");
        assert_eq!(parent.base_commit_ts, 7);
        assert!(manager.promote("main").is_err());
    }

    #[test]
    fn list_sorted() {
        let mut manager = root_manager();
        manager.create_child(String::from("zeta"), "main", 1).unwrap();
        manager.create_child(String::from("alpha"), "main", 1).unwrap();
        let ids: Vec<String> = manager.list().iter().map(|b| b.id.clone()).collect();
        assert_eq!(ids, vec![String::from("alpha"), String::from("main"), String::from("zeta")]);
    }

    #[test]
    fn child_rejects_bad_ids() {
        let mut manager = root_manager();
        assert!(manager.create_child(String::new(), "main", 1).is_err());
        assert!(manager.create_child("a".repeat(129), "main", 1).is_err());
        assert!(manager.create_child(String::from("a/b"), "main", 1).is_err());
        assert!(manager.create_child(String::from("a..b"), "main", 1).is_err());
        assert!(manager.create_child("a".repeat(128), "main", 1).is_ok());
    }

    #[test]
    fn nested_branch_parents_are_rejected() {
        let mut manager = root_manager();
        manager.create_child(String::from("preview"), "main", 1).unwrap();
        assert!(matches!(
            manager.create_child(String::from("nested"), "preview", 2),
            Err(RymeError::InvalidArgument(_))
        ));
    }

    #[test]
    fn persists_and_reloads_branch_metadata() {
        let path = std::env::temp_dir().join(format!(
            "ryme-branches-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_file(&path);
        let mut manager = root_manager();
        manager.create_child(String::from("preview"), "main", 7).unwrap();
        manager.persist(&path).unwrap();

        let restored = BranchManager::load(&path).unwrap();
        assert_eq!(restored.get("preview").unwrap().base_commit_ts, 7);
        assert!(restored.diff("main", "preview").unwrap().0.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn reload_rebuilds_manifest_refs_instead_of_trusting_disk() {
        let path = std::env::temp_dir().join(format!(
            "ryme-branches-refs-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        let mut manager = root_manager();
        manager.create_child(String::from("preview"), "main", 7).unwrap();
        manager.refs.clear();
        std::fs::write(&path, serde_json::to_vec(&manager).unwrap()).unwrap();

        let restored = BranchManager::load(&path).unwrap();
        assert_eq!(restored.refs.get("genesis"), Some(&2));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn reload_rejects_missing_manifest() {
        let path = std::env::temp_dir().join(format!(
            "ryme-branches-corrupt-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        let mut manager = root_manager();
        manager.branches.get_mut("default\0main").unwrap().manifest_id = String::from("gone");
        std::fs::write(&path, serde_json::to_vec(&manager).unwrap()).unwrap();

        assert!(BranchManager::load(&path).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn parent_cannot_be_deleted_while_children_exist() {
        let mut manager = root_manager();
        manager.create_child(String::from("preview"), "main", 7).unwrap();
        assert!(matches!(manager.delete("main"), Err(RymeError::Conflict(_))));
        assert!(manager.get("main").is_ok());
    }

    #[test]
    fn storage_epochs_are_not_reused_after_reset_or_recreate() {
        let mut manager = root_manager();
        manager.create_child(String::from("preview"), "main", 1).unwrap();
        let first = manager.get("preview").unwrap().storage_epoch;
        let reset = manager.reset("preview", 2).unwrap();
        assert!(reset.storage_epoch > first);

        manager.delete("preview").unwrap();
        manager.create_child(String::from("preview"), "main", 3).unwrap();
        let recreated = manager.get("preview").unwrap().storage_epoch;
        assert!(recreated > reset.storage_epoch);
    }

    #[test]
    fn tenant_branch_namespaces_are_isolated() {
        let mut manager = root_manager();
        manager
            .create_root_for(
                "alpha",
                String::from("main"),
                Manifest { id: String::from("alpha-genesis"), segments: Vec::new(), wal_start: 0 },
            )
            .unwrap();
        manager.create_child_for("alpha", String::from("preview"), "main", 4).unwrap();
        assert!(manager.get_for("beta", "preview").is_err());
        assert_eq!(manager.list_for("alpha").len(), 2);
        assert_eq!(manager.list_for("beta").len(), 0);
    }

    #[test]
    fn copy_on_write_overlay_reads_parent_and_masks_deletes() {
        let base = ryme_txn::TxnManager::new();
        let parent_key = ryme_storage::RecordKey::new("tenant", "default", "docs", b"parent");
        let mut parent_txn = base.begin();
        base.put(&mut parent_txn, parent_key.clone(), b"from-parent".to_vec());
        let parent_commit = base.commit(parent_txn).unwrap();

        let branch = BranchBackend::new(
            base.clone(),
            String::from("default"),
            String::from("preview"),
            parent_commit,
            0,
        );
        let mut write_txn = branch.begin();
        assert_eq!(branch.get(&mut write_txn, &parent_key).unwrap(), Some(b"from-parent".to_vec()));
        let child_key = ryme_storage::RecordKey::new("tenant", "default", "docs", b"child");
        branch.put(&mut write_txn, child_key.clone(), b"from-child".to_vec());
        branch.delete(&mut write_txn, parent_key.clone());
        base.commit(write_txn).unwrap();

        let mut read_txn = branch.begin();
        assert_eq!(branch.get(&mut read_txn, &parent_key).unwrap(), None);
        assert_eq!(branch.get(&mut read_txn, &child_key).unwrap(), Some(b"from-child".to_vec()));
        assert_eq!(
            branch.scan(&mut read_txn, "tenant", "default", "docs", 10).unwrap(),
            vec![(b"child".to_vec(), b"from-child".to_vec())]
        );
        assert_eq!(
            branch.scan_after(&mut read_txn, "tenant", "default", "docs", b"a", 10).unwrap(),
            vec![(b"child".to_vec(), b"from-child".to_vec())]
        );

        let mut base_read = base.begin();
        assert_eq!(base.get(&mut base_read, &parent_key).unwrap(), Some(b"from-parent".to_vec()));
    }

    #[tokio::test]
    async fn async_branch_reads_preserve_overlay_and_parent_snapshot() {
        let base = ryme_txn::TxnManager::new();
        let parent_key = ryme_storage::RecordKey::new("tenant", "default", "docs", b"parent");
        let mut parent_txn = base.begin();
        base.put(&mut parent_txn, parent_key.clone(), b"from-parent".to_vec());
        let parent_commit = base.commit(parent_txn).unwrap();
        let branch = BranchBackend::new(
            base.clone(),
            String::from("default"),
            String::from("preview"),
            parent_commit,
            0,
        );
        let mut branch_txn = branch.begin();
        branch.put(&mut branch_txn, parent_key.clone(), b"from-branch".to_vec());
        let child_key = ryme_storage::RecordKey::new("tenant", "default", "docs", b"child");
        branch.put(&mut branch_txn, child_key, b"from-child".to_vec());
        base.commit(branch_txn).unwrap();

        let mut reader = branch.begin();
        assert_eq!(
            branch.get_async(&mut reader, &parent_key).await.unwrap(),
            Some(b"from-branch".to_vec())
        );
        let missing_key = ryme_storage::RecordKey::new("tenant", "default", "docs", b"missing");
        assert_eq!(branch.get_async(&mut reader, &missing_key).await.unwrap(), None);
        assert_eq!(
            branch.scan_async(&mut reader, "tenant", "default", "docs", 10).await.unwrap(),
            vec![
                (b"child".to_vec(), b"from-child".to_vec()),
                (b"parent".to_vec(), b"from-branch".to_vec()),
            ]
        );
        assert!(branch
            .scan_after_async(&mut reader, "tenant", "default", "docs", b"parent", 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[test]
    fn branch_reads_keep_the_transaction_snapshot_for_overlay_rows() {
        let base = ryme_txn::TxnManager::new();
        let parent_key = ryme_storage::RecordKey::new("tenant", "default", "docs", b"row");
        let mut parent_write = base.begin();
        base.put(&mut parent_write, parent_key.clone(), b"from-parent".to_vec());
        let parent_commit = base.commit(parent_write).unwrap();
        let branch = BranchBackend::new(
            base.clone(),
            String::from("default"),
            String::from("preview"),
            parent_commit,
            0,
        );

        let mut reader = branch.begin();
        let mut branch_write = branch.begin();
        branch.put(&mut branch_write, parent_key.clone(), b"from-branch".to_vec());
        base.commit(branch_write).unwrap();

        let local_key = ryme_storage::RecordKey::new("tenant", "default", "docs", b"local");
        let local_expiry = ryme_txn::now_unix() + 10_000;
        let mut local_write = branch.begin();
        branch.put_with_ttl(&mut local_write, local_key.clone(), b"local".to_vec(), local_expiry);
        base.commit(local_write).unwrap();

        assert_eq!(branch.get(&mut reader, &parent_key).unwrap(), Some(b"from-parent".to_vec()));
        assert_eq!(
            branch.scan(&mut reader, "tenant", "default", "docs", 10).unwrap(),
            vec![(b"row".to_vec(), b"from-parent".to_vec())]
        );
        assert_eq!(branch.expires_at_at(&local_key, reader.read_ts).unwrap(), None);
        assert_eq!(branch.expires_at_at(&local_key, u64::MAX).unwrap(), Some(local_expiry));
    }

    #[test]
    fn branch_ttl_uses_parent_snapshot_and_local_expiry() {
        let base = ryme_txn::TxnManager::new();
        let parent_key = ryme_storage::RecordKey::new("tenant", "default", "docs", b"row");
        let now = ryme_txn::now_unix();
        let parent_expiry = now + 10_000;
        let mut parent_write = base.begin();
        base.put_with_ttl(&mut parent_write, parent_key.clone(), b"from-parent".to_vec(), parent_expiry);
        let parent_commit = base.commit(parent_write).unwrap();
        let branch = BranchBackend::new(
            base.clone(),
            String::from("default"),
            String::from("preview"),
            parent_commit,
            0,
        );

        let updated_expiry = now + 20_000;
        let mut parent_update = base.begin();
        base.put_with_ttl(&mut parent_update, parent_key.clone(), b"updated".to_vec(), updated_expiry);
        base.commit(parent_update).unwrap();
        assert_eq!(branch.expires_at(&parent_key).unwrap(), Some(parent_expiry));

        let mut expired_override = branch.begin();
        branch.put_with_ttl(
            &mut expired_override,
            parent_key.clone(),
            b"expired-override".to_vec(),
            now.saturating_sub(1),
        );
        base.commit(expired_override).unwrap();
        let mut expired_read = branch.begin();
        assert_eq!(branch.get(&mut expired_read, &parent_key).unwrap(), None);
        assert!(branch.scan(&mut expired_read, "tenant", "default", "docs", 10).unwrap().is_empty());

        let local_key = ryme_storage::RecordKey::new("tenant", "default", "docs", b"local");
        let local_expiry = now + 30_000;
        let mut branch_write = branch.begin();
        branch.put_with_ttl(&mut branch_write, local_key.clone(), b"from-branch".to_vec(), local_expiry);
        base.commit(branch_write).unwrap();
        assert_eq!(branch.expires_at(&local_key).unwrap(), Some(local_expiry));
    }
}
