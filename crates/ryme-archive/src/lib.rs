use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAX_ARCHIVE_OBJECT_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_ARCHIVE_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_ARCHIVE_FILES: usize = 262_144;
pub(crate) const MAX_ARCHIVE_LIST_KEYS: usize = 100_000;
pub(crate) const MAX_ARCHIVE_LIST_KEY_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_ARCHIVE_LIST_DIRECTORIES: usize = 100_000;
const MAX_ARCHIVE_LIST_DEPTH: usize = 256;
static NEXT_ARCHIVE_TEMP_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) fn create_archive_temp_file(path: &Path) -> Result<(PathBuf, File)> {
    let parent =
        path.parent().ok_or_else(|| RymeError::InvalidArgument(String::from("archive path")))?;
    for _ in 0..128 {
        let id = NEXT_ARCHIVE_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let temp = parent.join(format!(".rymedb-archive-{}-{id}.tmp", std::process::id()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&temp) {
            Ok(file) => return Ok((temp, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate unique archive temporary file",
    )
    .into())
}

pub mod local;
pub mod s3;

#[allow(async_fn_in_trait)]
pub trait ObjectStore: Send + Sync {
    /// Store one object. Implementations reject objects larger than
    /// `MAX_ARCHIVE_OBJECT_BYTES`, including direct calls that bypass `Archiver`.
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
    /// Atomically create an object only when its key is absent. Returns `false`
    /// when an object already exists. Backends without conditional creation
    /// must keep the default failure rather than emulate this with a racy read.
    async fn put_if_absent(&self, _key: &str, _bytes: Vec<u8>) -> Result<bool> {
        Err(RymeError::Unavailable(String::from("atomic archive create unsupported")))
    }
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.get_bounded(key, MAX_ARCHIVE_OBJECT_BYTES).await
    }
    /// Read an object without buffering more than `max_bytes` from the backend.
    /// Implementations must enforce this limit while receiving the object and
    /// also enforce the global `MAX_ARCHIVE_OBJECT_BYTES` ceiling.
    async fn get_bounded(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>>;
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
        && name.split('/').all(safe_path_component)
}

fn safe_path_component(component: &str) -> bool {
    if component.is_empty()
        || component.ends_with([' ', '.'])
        || component.chars().any(|character| {
            character <= '\u{1f}'
                || matches!(character, '<' | '>' | ':' | '"' | '|' | '?' | '*' | '\\' | '/')
        })
    {
        return false;
    }

    let device_base = component.split('.').next().unwrap_or(component).trim_end_matches([' ', '.']);
    if ["CON", "PRN", "AUX", "NUL", "CLOCK$", "CONIN$", "CONOUT$"]
        .iter()
        .any(|name| device_base.eq_ignore_ascii_case(name))
    {
        return false;
    }

    !["COM", "LPT"].iter().any(|prefix| {
        let Some(prefix_part) = device_base.get(..3) else {
            return false;
        };
        let Some(suffix) = device_base.get(3..) else {
            return false;
        };
        prefix_part.eq_ignore_ascii_case(prefix)
            && (matches!(suffix.as_bytes(), [b'1'..=b'9']) || matches!(suffix, "¹" | "²" | "³"))
    })
}

fn valid_backup_id(backup_id: &str) -> bool {
    !backup_id.is_empty()
        && backup_id.len() <= 128
        && !backup_id.contains('/')
        && !backup_id.contains("..")
}

fn valid_new_backup_id(backup_id: &str) -> bool {
    valid_backup_id(backup_id)
        && backup_id.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// Validates an ID before starting work to create a new archive.
///
/// New IDs are limited to portable ASCII letters, digits, underscores, and
/// hyphens. Existing manifests are validated separately to preserve legacy IDs.
pub fn validate_new_backup_id(backup_id: &str) -> Result<()> {
    if !valid_new_backup_id(backup_id) {
        return Err(RymeError::InvalidArgument(String::from("backup_id")));
    }
    Ok(())
}

fn valid_sha256(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_manifest(manifest_key: &str, manifest: &BackupManifest) -> Result<()> {
    if !valid_backup_id(&manifest.backup_id) {
        return Err(RymeError::Corrupt(String::from("manifest backup_id")));
    }
    if manifest_key != manifest.manifest_key() {
        return Err(RymeError::Corrupt(String::from("manifest key")));
    }
    if manifest.files.len() > MAX_ARCHIVE_FILES {
        return Err(RymeError::Overload(String::from("archive manifest files")));
    }
    let prefix = manifest.prefix();
    let mut names = HashSet::with_capacity(manifest.files.len());
    for file in &manifest.files {
        let name = file
            .key
            .strip_prefix(&prefix)
            .filter(|name| safe_relative_name(name))
            .ok_or_else(|| RymeError::Corrupt(String::from("manifest filename")))?;
        if !names.insert(name.to_string()) {
            return Err(RymeError::Corrupt(String::from("manifest duplicate")));
        }
        if !valid_sha256(&file.sha256) {
            return Err(RymeError::Corrupt(String::from("manifest checksum")));
        }
    }
    Ok(())
}

fn restore_stage_path(dest_dir: &Path) -> PathBuf {
    let parent = dest_dir.parent().unwrap_or_else(|| Path::new("."));
    let name = dest_dir.file_name().and_then(|name| name.to_str()).unwrap_or("restore");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    parent.join(format!(".{name}.restore-{}-{nanos}", std::process::id()))
}

fn ensure_restore_destination_absent(dest_dir: &Path) -> Result<()> {
    match std::fs::symlink_metadata(dest_dir) {
        Ok(_) => Err(RymeError::Conflict(String::from("restore destination exists"))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
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

    /// Loads one published backup manifest, returning `None` when its key is
    /// absent. The manifest schema and its paths are validated before it is
    /// returned.
    pub async fn get_backup(
        &self,
        backup_id: &str,
        commit_ts: u64,
    ) -> Result<Option<BackupManifest>> {
        if !valid_backup_id(backup_id) {
            return Err(RymeError::InvalidArgument(String::from("backup_id")));
        }
        let key = format!("backups/{commit_ts:020}-{backup_id}/manifest.json");
        let raw = match self.store.get_bounded(&key, MAX_ARCHIVE_MANIFEST_BYTES).await {
            Ok(raw) => raw,
            Err(RymeError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let manifest: BackupManifest = serde_json::from_slice(&raw)
            .map_err(|_| RymeError::Corrupt(String::from("manifest")))?;
        validate_manifest(&key, &manifest)?;
        Ok(Some(manifest))
    }

    pub async fn archive_files(
        &self,
        backup_id: &str,
        commit_ts: u64,
        files: Vec<(String, Vec<u8>)>,
    ) -> Result<BackupManifest> {
        self.archive_files_from(backup_id, commit_ts, files.into_iter().map(Ok)).await
    }

    /// Archives a fallible, lazy source so callers need not hold every file in memory.
    pub async fn archive_files_from<I>(
        &self,
        backup_id: &str,
        commit_ts: u64,
        files: I,
    ) -> Result<BackupManifest>
    where
        I: IntoIterator<Item = Result<(String, Vec<u8>)>>,
    {
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
        self.archive_files_encrypted_from(backup_id, commit_ts, files.into_iter().map(Ok), seal)
            .await
    }

    /// Archives and encrypts a fallible, lazy source one object at a time.
    pub async fn archive_files_encrypted_from<I, F>(
        &self,
        backup_id: &str,
        commit_ts: u64,
        files: I,
        seal: &F,
    ) -> Result<BackupManifest>
    where
        I: IntoIterator<Item = Result<(String, Vec<u8>)>>,
        F: Fn(&[u8]) -> Result<(Vec<u8>, FileEncryption)> + Send + Sync,
    {
        self.archive_files_inner(backup_id, commit_ts, files, &|bytes| {
            let (sealed, encryption) = seal(bytes)?;
            Ok((sealed, Some(encryption)))
        })
        .await
    }

    async fn archive_files_inner<I, F>(
        &self,
        backup_id: &str,
        commit_ts: u64,
        files: I,
        protect: &F,
    ) -> Result<BackupManifest>
    where
        I: IntoIterator<Item = Result<(String, Vec<u8>)>>,
        F: Fn(&[u8]) -> Result<(Vec<u8>, Option<FileEncryption>)> + Send + Sync,
    {
        validate_new_backup_id(backup_id)?;
        let files = files.into_iter();
        let (file_count_lower_bound, _) = files.size_hint();
        if file_count_lower_bound > MAX_ARCHIVE_FILES {
            return Err(RymeError::Overload(String::from("archive manifest files")));
        }
        let manifest_key = format!("backups/{commit_ts:020}-{backup_id}/manifest.json");
        // A zero-byte bounded read checks existence without buffering a
        // potentially large manifest; oversized means the key already exists.
        match self.store.get_bounded(&manifest_key, 0).await {
            Ok(_) => return Err(RymeError::Conflict(String::from("backup already exists"))),
            Err(RymeError::NotFound(_)) => {}
            Err(RymeError::Overload(_)) => {
                return Err(RymeError::Conflict(String::from("backup already exists")));
            }
            Err(error) => return Err(error),
        }
        let prefix = format!("backups/{commit_ts:020}-{backup_id}/");
        let mut entries = Vec::with_capacity(file_count_lower_bound);
        let mut names = HashSet::with_capacity(file_count_lower_bound);
        let mut written_keys = Vec::new();
        let mut publishing_manifest = false;
        let result = async {
            for file in files {
                if entries.len() == MAX_ARCHIVE_FILES {
                    return Err(RymeError::Overload(String::from("archive manifest files")));
                }
                let (name, bytes) = file?;
                if bytes.len() > MAX_ARCHIVE_OBJECT_BYTES {
                    return Err(RymeError::Overload(String::from("archive object")));
                }
                if !safe_relative_name(&name) {
                    return Err(RymeError::InvalidArgument(String::from("filename")));
                }
                if !names.insert(name.clone()) {
                    return Err(RymeError::InvalidArgument(String::from("duplicate filename")));
                }
                let key = format!("{prefix}{name}");
                let (stored, encryption) = protect(&bytes)?;
                if stored.len() > MAX_ARCHIVE_OBJECT_BYTES {
                    return Err(RymeError::Overload(String::from("archive object")));
                }
                let digest = sha256_hex(&stored);
                let length = stored.len() as u64;
                // Track before put: a backend can report an error after the
                // object was accepted, so cleanup must include ambiguous writes.
                written_keys.push(key.clone());
                self.store.put(&key, stored).await?;
                let roundtrip = self.store.get_bounded(&key, MAX_ARCHIVE_OBJECT_BYTES).await?;
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
            let raw =
                serde_json::to_vec(&manifest).map_err(|e| RymeError::Internal(e.to_string()))?;
            if raw.len() > MAX_ARCHIVE_MANIFEST_BYTES {
                return Err(RymeError::Overload(String::from("archive manifest")));
            }
            // A failed manifest PUT may still have reached the backend. Keep
            // every referenced object in that ambiguous case.
            publishing_manifest = true;
            self.store.put(&manifest_key, raw).await?;
            Ok(manifest)
        }
        .await;
        if result.is_err() && !publishing_manifest {
            for key in written_keys {
                let _ = self.store.delete(&key).await;
            }
        }
        result
    }

    pub async fn list_backups(&self) -> Result<Vec<BackupManifest>> {
        let keys = self.store.list("backups/").await?;
        let mut out = Vec::new();
        for key in keys {
            if !key.ends_with("/manifest.json") {
                continue;
            }
            let raw = self.store.get_bounded(&key, MAX_ARCHIVE_MANIFEST_BYTES).await?;
            let manifest: BackupManifest = serde_json::from_slice(&raw)
                .map_err(|_| RymeError::Corrupt(String::from("manifest")))?;
            validate_manifest(&key, &manifest)?;
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
        let raw = self.store.get_bounded(manifest_key, MAX_ARCHIVE_MANIFEST_BYTES).await?;
        let manifest: BackupManifest = serde_json::from_slice(&raw)
            .map_err(|_| RymeError::Corrupt(String::from("manifest")))?;
        validate_manifest(manifest_key, &manifest)?;
        ensure_restore_destination_absent(dest_dir)?;
        if let Some(parent) = dest_dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        ensure_restore_destination_absent(dest_dir)?;
        let staging = restore_stage_path(dest_dir);
        std::fs::create_dir(&staging)?;
        let restore_result: Result<()> = async {
            for file in &manifest.files {
                let bytes = self.store.get_bounded(&file.key, MAX_ARCHIVE_OBJECT_BYTES).await?;
                if bytes.len() as u64 != file.bytes || sha256_hex(&bytes) != file.sha256 {
                    return Err(RymeError::Corrupt(String::from("restore verify")));
                }
                let plain = open(&file.encryption, &bytes)?;
                let name =
                    file.key.strip_prefix(&manifest.prefix()).expect("validated archive manifest");
                let dest = staging.join(name);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let (tmp, mut output) = create_archive_temp_file(&dest)?;
                let write_result = (|| -> Result<()> {
                    output.write_all(&plain)?;
                    output.sync_all()?;
                    drop(output);
                    std::fs::rename(&tmp, &dest)?;
                    Ok(())
                })();
                if write_result.is_err() {
                    let _ = std::fs::remove_file(&tmp);
                }
                write_result?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = restore_result {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(error);
        }
        if let Err(error) = ensure_restore_destination_absent(dest_dir) {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(error);
        }
        if let Err(error) = std::fs::rename(&staging, dest_dir) {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(error.into());
        }
        Ok(manifest.commit_ts)
    }

    pub async fn verify_backup(
        &self,
        manifest_key: &str,
        open: &(impl Fn(&Option<FileEncryption>, &[u8]) -> Result<Vec<u8>> + Send + Sync),
    ) -> Result<u64> {
        let raw = self.store.get_bounded(manifest_key, MAX_ARCHIVE_MANIFEST_BYTES).await?;
        let manifest: BackupManifest = serde_json::from_slice(&raw)
            .map_err(|_| RymeError::Corrupt(String::from("manifest")))?;
        validate_manifest(manifest_key, &manifest)?;
        let mut verified = 0u64;
        for file in &manifest.files {
            let bytes = self.store.get_bounded(&file.key, MAX_ARCHIVE_OBJECT_BYTES).await?;
            if bytes.len() as u64 != file.bytes || sha256_hex(&bytes) != file.sha256 {
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
    validate_manifest(&manifest.manifest_key(), manifest)?;
    let key = manifest.manifest_key();
    let raw = source.get_bounded(&key, MAX_ARCHIVE_MANIFEST_BYTES).await?;
    let source_manifest: BackupManifest =
        serde_json::from_slice(&raw).map_err(|_| RymeError::Corrupt(String::from("manifest")))?;
    validate_manifest(&key, &source_manifest)?;
    if &source_manifest != manifest {
        return Err(RymeError::Corrupt(String::from("copy manifest")));
    }

    // A published destination manifest makes this backup identity immutable.
    // Accept an identical manifest so interrupted copies can be resumed, but
    // never replace a different backup that happens to use the same key.
    match dest.get_bounded(&key, MAX_ARCHIVE_MANIFEST_BYTES).await {
        Ok(existing) if existing == raw => {}
        Ok(_) | Err(RymeError::Overload(_)) => {
            return Err(RymeError::Conflict(String::from("copy destination manifest")));
        }
        Err(RymeError::NotFound(_)) => {}
        Err(error) => return Err(error),
    }

    let mut copied = 0u64;
    for file in &manifest.files {
        let bytes = source.get_bounded(&file.key, MAX_ARCHIVE_OBJECT_BYTES).await?;
        if bytes.len() as u64 != file.bytes || sha256_hex(&bytes) != file.sha256 {
            return Err(RymeError::Corrupt(String::from("copy verify")));
        }
        match dest.get_bounded(&file.key, MAX_ARCHIVE_OBJECT_BYTES).await {
            Ok(existing) => {
                if existing.len() as u64 != file.bytes || sha256_hex(&existing) != file.sha256 {
                    return Err(RymeError::Conflict(String::from("copy destination object")));
                }
            }
            Err(RymeError::Overload(_)) => {
                return Err(RymeError::Conflict(String::from("copy destination object")));
            }
            Err(RymeError::NotFound(_)) => {
                if dest.put_if_absent(&file.key, bytes).await? {
                    let roundtrip = dest.get_bounded(&file.key, MAX_ARCHIVE_OBJECT_BYTES).await?;
                    if roundtrip.len() as u64 != file.bytes || sha256_hex(&roundtrip) != file.sha256
                    {
                        return Err(RymeError::Corrupt(String::from("copy verify")));
                    }
                } else {
                    let existing = match dest.get_bounded(&file.key, MAX_ARCHIVE_OBJECT_BYTES).await
                    {
                        Ok(existing) => existing,
                        Err(RymeError::Overload(_)) => {
                            return Err(RymeError::Conflict(String::from(
                                "copy destination object",
                            )));
                        }
                        Err(error) => return Err(error),
                    };
                    if existing.len() as u64 != file.bytes || sha256_hex(&existing) != file.sha256 {
                        return Err(RymeError::Conflict(String::from("copy destination object")));
                    }
                }
            }
            Err(error) => return Err(error),
        }
        copied += 1;
    }
    // Recheck in case another writer published this backup while objects were
    // being copied. Do not overwrite a manifest or any object on conflict.
    match dest.get_bounded(&key, MAX_ARCHIVE_MANIFEST_BYTES).await {
        Ok(existing) if existing == raw => {}
        Ok(_) | Err(RymeError::Overload(_)) => {
            return Err(RymeError::Conflict(String::from("copy destination manifest")));
        }
        Err(RymeError::NotFound(_)) => {
            if !dest.put_if_absent(&key, raw.clone()).await? {
                let existing = match dest.get_bounded(&key, MAX_ARCHIVE_MANIFEST_BYTES).await {
                    Ok(existing) => existing,
                    Err(RymeError::Overload(_)) => {
                        return Err(RymeError::Conflict(String::from("copy destination manifest")));
                    }
                    Err(error) => return Err(error),
                };
                if existing != raw {
                    return Err(RymeError::Conflict(String::from("copy destination manifest")));
                }
            }
        }
        Err(error) => return Err(error),
    }
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::LocalStore;

    #[test]
    fn new_backup_ids_are_portable_and_legacy_ids_remain_valid() {
        assert!(validate_new_backup_id(r"legacy\id").is_err());
        assert!(validate_new_backup_id("legacy:id").is_err());
        assert!(validate_new_backup_id("trailing.").is_err());
        assert!(validate_new_backup_id("unicode-λ").is_err());
        assert!(validate_new_backup_id("backup_2026-10").is_ok());
        assert!(valid_backup_id(r"legacy\id"));

        let manifest = BackupManifest {
            backup_id: String::from(r"legacy\id"),
            commit_ts: 1,
            created_unix: 1,
            files: Vec::new(),
        };
        assert!(validate_manifest(&manifest.manifest_key(), &manifest).is_ok());
    }

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
    async fn archive_files_from_consumes_a_lazy_source() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-lazy-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let archiver = Archiver::new(LocalStore::new(root.clone()));
        let mut next = 0;
        let files = std::iter::from_fn(move || {
            let file = match next {
                0 => (String::from("first"), b"one".to_vec()),
                1 => (String::from("second"), b"two".to_vec()),
                _ => return None,
            };
            next += 1;
            Some(Ok(file))
        });

        let manifest = archiver.archive_files_from("lazy", 17, files).await.unwrap();
        assert_eq!(manifest.files.len(), 2);
        let restored = root.join("restored");
        archiver.restore_backup(&manifest.manifest_key(), &restored).await.unwrap();
        assert_eq!(std::fs::read(restored.join("first")).unwrap(), b"one");
        assert_eq!(std::fs::read(restored.join("second")).unwrap(), b"two");
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn archive_rejects_backup_id_reuse_without_replacing_existing_data() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-idempotency-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let archiver = Archiver::new(LocalStore::new(root.clone()));
        let manifest = archiver
            .archive_files("stable-id", 23, vec![(String::from("state"), b"original".to_vec())])
            .await
            .unwrap();
        assert_eq!(archiver.get_backup("stable-id", 23).await.unwrap(), Some(manifest.clone()));
        assert_eq!(archiver.get_backup("missing", 23).await.unwrap(), None);

        let duplicate = archiver
            .archive_files("stable-id", 23, vec![(String::from("state"), b"replacement".to_vec())])
            .await;
        assert!(matches!(duplicate, Err(RymeError::Conflict(_))));

        let dest = root.join("restored");
        archiver.restore_backup(&manifest.manifest_key(), &dest).await.unwrap();
        assert_eq!(std::fs::read(dest.join("state")).unwrap(), b"original");
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn archive_cleans_objects_when_source_fails_before_manifest_publish() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-partial-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let store = LocalStore::new(root.clone());
        let archiver = Archiver::new(store.clone());
        let files = vec![
            Ok((String::from("first"), b"one".to_vec())),
            Err(RymeError::Unavailable(String::from("source failed"))),
        ];

        let result = archiver.archive_files_from("partial", 29, files).await;
        assert!(matches!(result, Err(RymeError::Unavailable(_))));
        assert!(store.list("backups/").await.unwrap().is_empty());
        let _ = std::fs::remove_dir_all(root);
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
    async fn copy_backup_resumes_from_matching_destination_objects() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-copy-resume-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let source = Archiver::new(LocalStore::new(root.join("primary")));
        let manifest = source
            .archive_files(
                "resume",
                11,
                vec![
                    (String::from("first"), b"one".to_vec()),
                    (String::from("second"), b"two".to_vec()),
                ],
            )
            .await
            .unwrap();
        let replica = LocalStore::new(root.join("replica"));
        replica.put(&manifest.files[0].key, b"one".to_vec()).await.unwrap();

        assert_eq!(copy_backup(source.store(), &replica, &manifest).await.unwrap(), 2);
        assert_eq!(Archiver::new(replica).list_backups().await.unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn copy_backup_preserves_conflicting_destination_object() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-copy-conflict-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let source = Archiver::new(LocalStore::new(root.join("primary")));
        let manifest = source
            .archive_files("conflict", 13, vec![(String::from("state"), b"source".to_vec())])
            .await
            .unwrap();
        let replica = LocalStore::new(root.join("replica"));
        replica.put(&manifest.files[0].key, b"destination".to_vec()).await.unwrap();

        assert!(matches!(
            copy_backup(source.store(), &replica, &manifest).await,
            Err(RymeError::Conflict(_))
        ));
        assert_eq!(replica.get(&manifest.files[0].key).await.unwrap(), b"destination");
        assert!(matches!(replica.get(&manifest.manifest_key()).await, Err(RymeError::NotFound(_))));
        let _ = std::fs::remove_dir_all(root);
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
    async fn restore_is_atomic_on_late_failure() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-atomic-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = LocalStore::new(root.clone());
        let archiver = Archiver::new(store.clone());
        let manifest = archiver
            .archive_files(
                "atomic",
                8,
                vec![
                    (String::from("first"), b"first".to_vec()),
                    (String::from("second"), b"second".to_vec()),
                ],
            )
            .await
            .unwrap();
        store.put(&manifest.files[1].key, b"tampered".to_vec()).await.unwrap();

        let dest = root.join("out");
        let result = archiver.restore_backup(&manifest.manifest_key(), &dest).await;
        assert!(matches!(result, Err(RymeError::Corrupt(_))));
        assert!(!dest.exists());
        assert!(!root
            .read_dir()
            .unwrap()
            .filter_map(std::result::Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().starts_with(".out.restore-")));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn archive_rejects_duplicate_filenames() {
        let root = std::env::temp_dir().join(format!(
            "ryme-arch-duplicates-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let archiver = Archiver::new(LocalStore::new(root.clone()));
        let result = archiver
            .archive_files(
                "duplicates",
                1,
                vec![
                    (String::from("same"), b"one".to_vec()),
                    (String::from("same"), b"two".to_vec()),
                ],
            )
            .await;
        assert!(matches!(result, Err(RymeError::InvalidArgument(_))));
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
        for name in [
            "../escape",
            "nested/../../escape",
            "/absolute",
            "nested\\escape",
            "CON",
            "nested/NUL.txt",
            "COM1.log",
            "name.",
            "name ",
            "a:b",
            "a?b",
        ] {
            let result =
                archiver.archive_files("unsafe", 1, vec![(String::from(name), Vec::new())]).await;
            assert!(matches!(result, Err(RymeError::InvalidArgument(_))), "{name}");
        }
        assert!(safe_relative_name("nested/résumé.txt"));

        let mut manifest = BackupManifest {
            backup_id: String::from("legacy"),
            commit_ts: 2,
            created_unix: 1,
            files: Vec::new(),
        };
        manifest.files.push(FileEntry {
            key: format!("{}CON.txt", manifest.prefix()),
            bytes: 0,
            sha256: String::from("0").repeat(64),
            encryption: None,
        });
        assert!(matches!(
            validate_manifest(&manifest.manifest_key(), &manifest),
            Err(RymeError::Corrupt(_))
        ));
        let _ = std::fs::remove_dir_all(&root);
    }
}
