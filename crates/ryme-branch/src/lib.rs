use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    pub id: String,
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

#[derive(Debug, Default)]
pub struct BranchManager {
    branches: HashMap<String, Branch>,
    manifests: HashMap<String, Manifest>,
    refs: HashMap<String, usize>,
}

impl BranchManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_root(&mut self, id: String, manifest: Manifest) -> Result<()> {
        if id.is_empty() || id.len() > 128 || id.contains('/') || id.contains("..") {
            return Err(RymeError::InvalidArgument(String::from("branch")));
        }
        if self.branches.contains_key(&id) {
            return Err(RymeError::Conflict(String::from("branch")));
        }
        self.refs.insert(manifest.id.clone(), 1);
        self.manifests.insert(manifest.id.clone(), manifest.clone());
        self.branches.insert(
            id.clone(),
            Branch {
                id,
                parent_id: None,
                base_commit_ts: 0,
                manifest_id: manifest.id,
                schema_version: 1,
            },
        );
        Ok(())
    }

    pub fn create_child(&mut self, id: String, parent: &str, base_commit_ts: u64) -> Result<()> {
        if id.is_empty() || id.len() > 128 || id.contains('/') || id.contains("..") {
            return Err(RymeError::InvalidArgument(String::from("branch")));
        }
        if self.branches.contains_key(&id) {
            return Err(RymeError::Conflict(String::from("branch")));
        }
        let parent_branch = self
            .branches
            .get(parent)
            .ok_or_else(|| RymeError::NotFound(String::from("parent")))?
            .clone();
        let manifest_id = parent_branch.manifest_id.clone();
        *self.refs.entry(manifest_id.clone()).or_insert(0) += 1;
        self.branches.insert(
            id.clone(),
            Branch {
                id,
                parent_id: Some(parent.to_string()),
                base_commit_ts,
                manifest_id,
                schema_version: parent_branch.schema_version,
            },
        );
        Ok(())
    }

    pub fn delete(&mut self, id: &str) -> Result<Vec<String>> {
        let branch =
            self.branches.remove(id).ok_or_else(|| RymeError::NotFound(String::from("branch")))?;
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
        let left_branch =
            self.branches.get(left).ok_or_else(|| RymeError::NotFound(String::from("branch")))?;
        let right_branch =
            self.branches.get(right).ok_or_else(|| RymeError::NotFound(String::from("branch")))?;
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
        self.branches.get(id).cloned().ok_or_else(|| RymeError::NotFound(String::from("branch")))
    }

    pub fn list(&self) -> Vec<Branch> {
        let mut out: Vec<Branch> = self.branches.values().cloned().collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    pub fn reset(&mut self, id: &str, base_commit_ts: u64) -> Result<Branch> {
        let branch =
            self.branches.get_mut(id).ok_or_else(|| RymeError::NotFound(String::from("branch")))?;
        branch.base_commit_ts = base_commit_ts;
        Ok(branch.clone())
    }

    pub fn promote(&mut self, id: &str) -> Result<Branch> {
        let child = self
            .branches
            .get(id)
            .ok_or_else(|| RymeError::NotFound(String::from("branch")))?
            .clone();
        let parent_id = child
            .parent_id
            .clone()
            .ok_or_else(|| RymeError::InvalidArgument(String::from("root")))?;
        let parent = self
            .branches
            .get_mut(&parent_id)
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
