use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    pub id: String,
    #[serde(default)]
    pub tenant: String,
    pub parent_id: Option<String>,
    pub base_commit_ts: u64,
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
        Ok(manager)
    }

    pub fn persist(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|error| RymeError::Internal(format!("branch metadata: {error}")))?;
        let temporary = path.with_extension("tmp");
        std::fs::write(&temporary, bytes)?;
        std::fs::rename(temporary, path)?;
        Ok(())
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
        let manifest_id = parent_branch.manifest_id.clone();
        *self.refs.entry(manifest_id.clone()).or_insert(0) += 1;
        self.branches.insert(
            key,
            Branch {
                id,
                tenant: tenant.to_string(),
                parent_id: Some(parent.to_string()),
                base_commit_ts,
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
        let branch = self
            .branches
            .get_mut(&scoped_key(tenant, id))
            .ok_or_else(|| RymeError::NotFound(String::from("branch")))?;
        branch.base_commit_ts = base_commit_ts;
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

    pub fn new(base: B, database: String, branch: String, base_commit_ts: u64) -> Self {
        Self {
            branch_database: format!("{database}\0branch\0{branch}"),
            base,
            base_commit_ts,
            overlay: true,
        }
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

    fn decode(value: &[u8]) -> Option<Option<Vec<u8>>> {
        if !value.starts_with(BRANCH_VALUE_PREFIX) {
            return Some(Some(value.to_vec()));
        }
        match value.get(BRANCH_VALUE_PREFIX.len()) {
            Some(0) => Some(Some(value[BRANCH_VALUE_PREFIX.len() + 1..].to_vec())),
            Some(1) => Some(None),
            _ => None,
        }
    }

    fn overlay_rows(
        &self,
        txn: &ryme_txn::Transaction,
        tenant: &str,
        table: &str,
    ) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
        let mut rows = BTreeMap::new();
        let mut local_txn = self.base.begin();
        let local_rows =
            self.scan_all(&mut local_txn, tenant, &self.branch_database, table, usize::MAX)?;
        for (pk, value) in local_rows {
            if let Some(decoded) = Self::decode(&value) {
                rows.insert(pk, decoded);
            }
        }
        for (key, write) in txn.writes() {
            if key.tenant != tenant || key.database != self.branch_database || key.table != table {
                continue;
            }
            let value = write.value.as_deref().and_then(Self::decode).flatten();
            rows.insert(key.pk.clone(), value);
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
        let mut local_txn = self.base.begin();
        if let Some(value) = self.base.get(&mut local_txn, &local)? {
            return Ok(Self::decode(&value).flatten());
        }
        let mut parent_txn = self.parent_transaction();
        self.base.get(&mut parent_txn, key)
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
            self.base.put_with_ttl(
                txn,
                self.local_key(&key),
                Self::encode(Some(&value)),
                expires_at,
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
            return Ok(if Self::decode(&value).flatten().is_some() {
                self.base.expires_at(&local)?
            } else {
                Some(1)
            });
        }
        self.base.expires_at(key)
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
        let rows = self.scan(txn, tenant, database, table, usize::MAX)?;
        Ok(rows.into_iter().filter(|(pk, _)| pk.as_slice() > start_after).take(limit).collect())
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

        let mut base_read = base.begin();
        assert_eq!(base.get(&mut base_read, &parent_key).unwrap(), Some(b"from-parent".to_vec()));
    }
}
