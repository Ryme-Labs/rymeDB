use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Component, Path};

pub mod local;
pub mod s3;

#[allow(async_fn_in_trait)]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn delete(&self, key: &str) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEntry {
    pub key: String,
    pub bytes: u64,
    pub sha256: String,
    #[serde(default)]
    pub encryption: Option<FileEncryption>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEncryption {
    pub dek_id: String,
    pub nonce_b64: String,
    pub tag_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    pub backup_id: String,
    pub commit_ts: u64,
    pub created_unix: u64,
    pub files: Vec<FileEntry>,
}

impl BackupManifest {
    pub fn manifest_key(&self) -> String {
        format!("backups/{:020}-{}/manifest.json", self.commit_ts, self.backup_id)
    }

    pub fn prefix(&self) -> String {
        format!("backups/{:020}-{}/", self.commit_ts, self.backup_id)
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(ALPHABET[(byte >> 4) as usize] as char);
        out.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn safe_relative_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('\\')
        && Path::new(name).components().all(|component| matches!(component, Component::Normal(_)))
}

pub struct Archiver<S> {
    store: S,
}

impl<S: ObjectStore> Archiver<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub async fn archive_files(
        &self,
        backup_id: &str,
        commit_ts: u64,
        files: Vec<(String, Vec<u8>)>,
    ) -> Result<BackupManifest> {
        self.archive_files_inner(backup_id, commit_ts, files, &|bytes| Ok((bytes.to_vec(), None)))
            .await
    }

    pub async fn archive_files_encrypted<F>(
        &self,
        backup_id: &str,
        commit_ts: u64,
        files: Vec<(String, Vec<u8>)>,
        seal: &F,
    ) -> Result<BackupManifest>
    where
        F: Fn(&[u8]) -> Result<(Vec<u8>, FileEncryption)> + Send + Sync,
    {
        self.archive_files_inner(backup_id, commit_ts, files, &|bytes| {
            let (sealed, encryption) = seal(bytes)?;
            Ok((sealed, Some(encryption)))
        })
        .await
    }

    async fn archive_files_inner<F>(
        &self,
        backup_id: &str,
        commit_ts: u64,
        files: Vec<(String, Vec<u8>)>,
        protect: &F,
    ) -> Result<BackupManifest>
    where
        F: Fn(&[u8]) -> Result<(Vec<u8>, Option<FileEncryption>)> + Send + Sync,
    {
        if backup_id.is_empty() || backup_id.len() > 128 {
            return Err(RymeError::InvalidArgument(String::from("backup_id")));
        }
        if backup_id.contains('/') || backup_id.contains("..") {
            return Err(RymeError::InvalidArgument(String::from("backup_id")));
        }
        let prefix = format!("backups/{commit_ts:020}-{backup_id}/");
        let mut entries = Vec::new();
        for (name, bytes) in files {
            if !safe_relative_name(&name) {
                return Err(RymeError::InvalidArgument(String::from("filename")));
            }
            let key = format!("{prefix}{name}");
            let (stored, encryption) = protect(&bytes)?;
            let digest = sha256_hex(&stored);
            let length = stored.len() as u64;
            self.store.put(&key, stored).await?;
            let roundtrip = self.store.get(&key).await?;
            if sha256_hex(&roundtrip) != digest {
                return Err(RymeError::Corrupt(String::from("archive verify")));
            }
            entries.push(FileEntry { key, bytes: length, sha256: digest, encryption });
        }
        let manifest = BackupManifest {
            backup_id: backup_id.to_string(),
            commit_ts,
            created_unix: now_unix(),
            files: entries,
        };
        let raw = serde_json::to_vec(&manifest).map_err(|e| RymeError::Internal(e.to_string()))?;
        self.store.put(&manifest.manifest_key(), raw).await?;
        Ok(manifest)
    }

    pub async fn list_backups(&self) -> Result<Vec<BackupManifest>> {
        let keys = self.store.list("backups/").await?;
        let mut out = Vec::new();
        for key in keys {
            if !key.ends_with("/manifest.json") {
                continue;
            }
            let raw = self.store.get(&key).await?;
            let manifest: BackupManifest = serde_json::from_slice(&raw)
                .map_err(|_| RymeError::Corrupt(String::from("manifest")))?;
            out.push(manifest);
        }
        out.sort_by_key(|m| m.commit_ts);
        Ok(out)
    }

    pub async fn restore_backup(
        &self,
        manifest_key: &str,
        dest_dir: &std::path::Path,
    ) -> Result<u64> {
        self.restore_backup_with(manifest_key, dest_dir, &|_, bytes| Ok(bytes.to_vec())).await
    }

    pub async fn restore_backup_with(
        &self,
        manifest_key: &str,
        dest_dir: &std::path::Path,
        open: &(impl Fn(&Option<FileEncryption>, &[u8]) -> Result<Vec<u8>> + Send + Sync),
    ) -> Result<u64> {
        let raw = self.store.get(manifest_key).await?;
        let manifest: BackupManifest = serde_json::from_slice(&raw)
            .map_err(|_| RymeError::Corrupt(String::from("manifest")))?;
        std::fs::create_dir_all(dest_dir)?;
        for file in &manifest.files {
            let bytes = self.store.get(&file.key).await?;
            if sha256_hex(&bytes) != file.sha256 {
                return Err(RymeError::Corrupt(String::from("restore verify")));
            }
            let plain = open(&file.encryption, &bytes)?;
            let name = file
                .key
                .strip_prefix(&manifest.prefix())
                .filter(|name| safe_relative_name(name))
                .ok_or_else(|| RymeError::Corrupt(String::from("filename")))?;
            let dest = dest_dir.join(name);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let tmp = dest.with_extension("tmp");
            std::fs::write(&tmp, &plain)?;
            std::fs::rename(&tmp, &dest)?;
        }
        Ok(manifest.commit_ts)
    }

    pub async fn verify_backup(
        &self,
        manifest_key: &str,
        open: &(impl Fn(&Option<FileEncryption>, &[u8]) -> Result<Vec<u8>> + Send + Sync),
    ) -> Result<u64> {
        let raw = self.store.get(manifest_key).await?;
        let manifest: BackupManifest = serde_json::from_slice(&raw)
            .map_err(|_| RymeError::Corrupt(String::from("manifest")))?;
        let mut verified = 0u64;
        for file in &manifest.files {
            let bytes = self.store.get(&file.key).await?;
            if sha256_hex(&bytes) != file.sha256 {
                return Err(RymeError::Corrupt(String::from("restore verify")));
            }
            open(&file.encryption, &bytes)?;
            verified += 1;
        }
        Ok(verified)
    }

    pub async fn prune(&self, keep: usize) -> Result<Vec<String>> {
        let mut backups = self.list_backups().await?;
        if backups.len() <= keep {
            return Ok(Vec::new());
        }
        backups.sort_by_key(|m| m.commit_ts);
        let excess = backups.len() - keep;
        let mut removed = Vec::new();
        for manifest in backups.into_iter().take(excess) {
            for key in self.store.list(&manifest.prefix()).await? {
                self.store.delete(&key).await?;
                removed.push(key);
            }
        }
        Ok(removed)
    }
}

pub async fn copy_backup<S: ObjectStore, D: ObjectStore>(
    source: &S,
    dest: &D,
    manifest: &BackupManifest,
) -> Result<u64> {
    let mut copied = 0u64;
    for file in &manifest.files {
        let bytes = source.get(&file.key).await?;
        if sha256_hex(&bytes) != file.sha256 {
            return Err(RymeError::Corrupt(String::from("copy verify")));
        }
        dest.put(&file.key, bytes).await?;
        let roundtrip = dest.get(&file.key).await?;
        if sha256_hex(&roundtrip) != file.sha256 {
            dest.delete(&file.key).await?;
            return Err(RymeError::Corrupt(String::from("copy verify")));
        }
        copied += 1;
    }
    let key = manifest.manifest_key();
    let raw = source.get(&key).await?;
    serde_json::from_slice::<BackupManifest>(&raw)
        .map_err(|_| RymeError::Corrupt(String::from("manifest")))?;
    dest.put(&key, raw).await?;
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::LocalStore;

    #[tokio::test]
    async fn archive_restore_roundtrip() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let archiver = Archiver::new(LocalStore::new(root.clone()));
        let manifest = archiver
            .archive_files(
                "b1",
                42,
                vec![
                    (String::from("snap.rsnap"), b"snapshot-bytes".to_vec()),
                    (String::from("seg.wal"), b"wal-bytes".to_vec()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(manifest.files.len(), 2);
        let listed = archiver.list_backups().await.unwrap();
        assert_eq!(listed.len(), 1);
        let dest = root.join("restored");
        let commit = archiver.restore_backup(&manifest.manifest_key(), &dest).await.unwrap();
        assert_eq!(commit, 42);
        assert_eq!(std::fs::read(dest.join("snap.rsnap")).unwrap(), b"snapshot-bytes");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn copy_backup_replicates_with_verify() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-copy-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let source = Archiver::new(LocalStore::new(root.join("primary")));
        let manifest = source
            .archive_files(
                "copy-1",
                9,
                vec![
                    (String::from("a.rsnap"), b"aaa".to_vec()),
                    (String::from("b.wal"), b"bb".to_vec()),
                ],
            )
            .await
            .unwrap();
        let replica = LocalStore::new(root.join("replica"));
        let copied = copy_backup(source.store(), &replica, &manifest).await.unwrap();
        assert_eq!(copied, 2);
        let listed = Archiver::new(replica).list_backups().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].backup_id, "copy-1");
        assert_eq!(listed[0].files.len(), 2);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn restore_detects_tamper() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-tamper-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = LocalStore::new(root.clone());
        let archiver = Archiver::new(store);
        let manifest = archiver
            .archive_files("b1", 7, vec![(String::from("f"), b"real".to_vec())])
            .await
            .unwrap();
        let backend = LocalStore::new(root.clone());
        backend.put(&manifest.files[0].key, b"fake".to_vec()).await.unwrap();
        let result = archiver.restore_backup(&manifest.manifest_key(), &root.join("out")).await;
        assert!(matches!(result, Err(RymeError::Corrupt(_))));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn prune_keeps_newest() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-prune-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let archiver = Archiver::new(LocalStore::new(root.clone()));
        for commit in [10u64, 20, 30] {
            archiver
                .archive_files(
                    &format!("b{commit}"),
                    commit,
                    vec![(String::from("f"), b"x".to_vec())],
                )
                .await
                .unwrap();
        }
        let removed = archiver.prune(2).await.unwrap();
        assert!(!removed.is_empty());
        let listed = archiver.list_backups().await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].commit_ts, 20);
        let _ = std::fs::remove_dir_all(&root);
    }

    fn now_unix_nanos() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn encrypted_archive_verify_and_restore() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-enc-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let archiver = Archiver::new(LocalStore::new(root.clone()));
        let seal = |bytes: &[u8]| {
            let mut sealed = b"ENC:".to_vec();
            sealed.extend_from_slice(bytes);
            Ok((
                sealed,
                FileEncryption {
                    dek_id: String::from("dek-1"),
                    nonce_b64: String::from("n"),
                    tag_b64: String::from("t"),
                },
            ))
        };
        let manifest = archiver
            .archive_files_encrypted("enc1", 9, vec![(String::from("f"), b"real".to_vec())], &seal)
            .await
            .unwrap();
        assert!(manifest.files[0].encryption.is_some());
        let open = |encryption: &Option<FileEncryption>, bytes: &[u8]| {
            if encryption.is_none() {
                return Err(RymeError::Corrupt(String::from("expected envelope")));
            }
            bytes
                .strip_prefix(b"ENC:")
                .map(|rest| rest.to_vec())
                .ok_or_else(|| RymeError::Corrupt(String::from("envelope")))
        };
        let verified = archiver.verify_backup(&manifest.manifest_key(), &open).await.unwrap();
        assert_eq!(verified, 1);
        let commit = archiver
            .restore_backup_with(&manifest.manifest_key(), &root.join("out"), &open)
            .await
            .unwrap();
        assert_eq!(commit, 9);
        assert_eq!(std::fs::read(root.join("out/f")).unwrap(), b"real".to_vec());
        let wrong = |_: &Option<FileEncryption>, _: &[u8]| {
            Err::<Vec<u8>, RymeError>(RymeError::Unauthorized)
        };
        assert!(archiver.verify_backup(&manifest.manifest_key(), &wrong).await.is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn nested_paths_restore_without_flattening() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-nested-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let archiver = Archiver::new(LocalStore::new(root.clone()));
        let manifest = archiver
            .archive_files(
                "nested",
                11,
                vec![(String::from("branch-schemas/tenant/preview.json"), b"schema".to_vec())],
            )
            .await
            .unwrap();
        archiver.restore_backup(&manifest.manifest_key(), &root.join("out")).await.unwrap();
        assert_eq!(
            std::fs::read(root.join("out/branch-schemas/tenant/preview.json")).unwrap(),
            b"schema"
        );
        assert!(!root.join("out/preview.json").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn archive_rejects_unsafe_relative_paths() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-paths-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let archiver = Archiver::new(LocalStore::new(root.clone()));
        for name in ["../escape", "nested/../../escape", "/absolute", "nested\\escape"] {
            let result =
                archiver.archive_files("unsafe", 1, vec![(String::from(name), Vec::new())]).await;
            assert!(matches!(result, Err(RymeError::InvalidArgument(_))), "{name}");
        }
        let _ = std::fs::remove_dir_all(&root);
    }
}
