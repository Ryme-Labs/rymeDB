use ryme_error::{Result, RymeError};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn validate_root(&self) -> Result<()> {
        match std::fs::symlink_metadata(&self.root) {
            Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
            Ok(_) => Err(RymeError::InvalidArgument(String::from("archive root"))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RymeError::from(error)),
        }
    }

    fn create_parent_directories(&self, parent: &std::path::Path) -> Result<Vec<PathBuf>> {
        let mut missing = Vec::new();
        let mut current = Some(parent);
        while let Some(directory) = current {
            let under_root = directory.starts_with(&self.root);
            let metadata = if under_root {
                std::fs::symlink_metadata(directory)
            } else {
                std::fs::metadata(directory)
            };
            match metadata {
                Ok(metadata) if metadata.is_dir() => break,
                Ok(_) => return Err(RymeError::InvalidArgument(String::from("archive directory"))),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing.push(directory.to_path_buf());
                }
                Err(error) => return Err(RymeError::from(error)),
            }
            current = directory.parent();
        }
        std::fs::create_dir_all(parent)?;
        Ok(missing)
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        if !super::safe_relative_name(key) {
            return Err(RymeError::InvalidArgument(String::from("key")));
        }
        self.validate_root()?;
        if Self::has_symlink_component(&self.root, &self.root.join(key))? {
            return Err(RymeError::InvalidArgument(String::from("symlink key")));
        }
        Ok(self.root.join(key))
    }

    fn has_symlink_component(root: &std::path::Path, path: &std::path::Path) -> Result<bool> {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| RymeError::InvalidArgument(String::from("path")))?;
        let mut current = root.to_path_buf();
        for component in relative.components() {
            current.push(component);
            match std::fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => return Ok(true),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(RymeError::from(error)),
            }
        }
        Ok(false)
    }

    fn collect(
        prefix: &std::path::Path,
        root: &std::path::Path,
        out: &mut Vec<String>,
    ) -> Result<()> {
        let mut pending = vec![(prefix.to_path_buf(), 0usize)];
        let mut visited_directories = 0usize;
        let mut total_key_bytes = 0usize;
        while let Some((directory, depth)) = pending.pop() {
            if Self::has_symlink_component(root, &directory)? {
                continue;
            }
            visited_directories = visited_directories.saturating_add(1);
            if visited_directories > super::MAX_ARCHIVE_LIST_DIRECTORIES {
                return Err(RymeError::Overload(String::from("archive list directories")));
            }
            let entries = match std::fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(RymeError::from(e)),
            };
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                let file_type = entry.file_type()?;
                if file_type.is_symlink() {
                    continue;
                } else if file_type.is_dir() {
                    if depth >= super::MAX_ARCHIVE_LIST_DEPTH
                        || pending.len() >= super::MAX_ARCHIVE_LIST_DIRECTORIES
                    {
                        return Err(RymeError::Overload(String::from("archive list directories")));
                    }
                    pending.push((path, depth + 1));
                } else if file_type.is_file() {
                    if out.len() >= super::MAX_ARCHIVE_LIST_KEYS {
                        return Err(RymeError::Overload(String::from("archive list results")));
                    }
                    if let Ok(relative) = path.strip_prefix(root) {
                        let key = relative.to_string_lossy().replace('\\', "/");
                        total_key_bytes = total_key_bytes
                            .checked_add(key.len())
                            .filter(|bytes| *bytes <= super::MAX_ARCHIVE_LIST_KEY_BYTES)
                            .ok_or_else(|| {
                                RymeError::Overload(String::from("archive list results"))
                            })?;
                        out.push(key);
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use crate::ObjectStore;

    #[test]
    fn local_store_rejects_rooted_and_traversal_keys() {
        let store = LocalStore::new(PathBuf::from("archive"));
        for key in [
            "../escape",
            "nested/../../escape",
            "/absolute",
            "nested\\escape",
            "C:\\outside",
            "\\\\server\\share",
            "backups/CON/file",
            "backups/NUL.txt/file",
            "backups/COM1.log/file",
            "backups/a:b/file",
            "backups/trailing./file",
        ] {
            assert!(matches!(store.path(key), Err(RymeError::InvalidArgument(_))), "{key}");
        }
        assert_eq!(
            store.path("backups/backup-1/file.wal").unwrap(),
            PathBuf::from("archive/backups/backup-1/file.wal")
        );
    }

    #[tokio::test]
    async fn local_store_validates_list_prefixes_without_accepting_root() {
        let store = LocalStore::new(PathBuf::from("archive"));
        assert!(store.list("backups/").await.is_ok());
        assert!(matches!(store.list("/").await, Err(RymeError::InvalidArgument(_))));
        assert!(matches!(store.list("backups/CON/").await, Err(RymeError::InvalidArgument(_))));
    }

    #[cfg(unix)]
    #[test]
    fn local_store_rejects_and_skips_symlinks() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "ryme-local-store-symlink-{}-{}",
            std::process::id(),
            crate::now_unix()
        ));
        let outside = root.with_extension("outside");
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
        std::fs::create_dir_all(root.join("backups")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret"), b"private").unwrap();
        symlink(&outside, root.join("backups/link")).unwrap();

        let store = LocalStore::new(root.clone());
        assert!(matches!(store.path("backups/link/secret"), Err(RymeError::InvalidArgument(_))));
        let mut listed = Vec::new();
        LocalStore::collect(&root.join("backups"), &root, &mut listed).unwrap();
        assert!(listed.is_empty());

        std::fs::remove_file(root.join("backups/link")).unwrap();
        std::fs::remove_dir(root.join("backups")).unwrap();
        symlink(&outside, root.join("backups")).unwrap();
        assert!(matches!(store.path("backups/secret"), Err(RymeError::InvalidArgument(_))));
        LocalStore::collect(&root.join("backups"), &root, &mut listed).unwrap();
        assert!(listed.is_empty());

        std::fs::remove_dir_all(&root).unwrap();
        std::fs::remove_dir_all(&outside).unwrap();
    }

    #[tokio::test]
    async fn local_store_bounds_object_reads() {
        let root = std::env::temp_dir().join(format!(
            "ryme-local-store-bounds-{}-{}",
            std::process::id(),
            crate::now_unix()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = LocalStore::new(root.clone());
        store.put("small", b"123".to_vec()).await.unwrap();
        assert!(matches!(store.get_bounded("small", 2).await, Err(RymeError::Overload(_))));

        let oversized = std::fs::File::create(root.join("oversized")).unwrap();
        oversized.set_len((super::super::MAX_ARCHIVE_OBJECT_BYTES + 1) as u64).unwrap();
        assert!(matches!(store.get("oversized").await, Err(RymeError::Overload(_))));
        std::fs::remove_dir_all(root).unwrap();
    }
}

impl crate::ObjectStore for LocalStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        if bytes.len() > super::MAX_ARCHIVE_OBJECT_BYTES {
            return Err(RymeError::Overload(String::from("archive object")));
        }
        let path = self.path(key)?;
        let parent = path
            .parent()
            .ok_or_else(|| RymeError::InvalidArgument(String::from("archive path")))?;
        let missing_directories = self.create_parent_directories(parent)?;
        let path = self.path(key)?;
        let (tmp, mut file) = super::create_archive_temp_file(&path)?;
        let write_result = (|| -> Result<()> {
            std::io::Write::write_all(&mut file, &bytes)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&tmp, &path)?;
            sync_archive_directory(parent)?;
            for directory in &missing_directories {
                if let Some(parent) = directory.parent() {
                    sync_archive_directory(parent)?;
                }
            }
            Ok(())
        })();
        if write_result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        write_result?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.get_bounded(key, super::MAX_ARCHIVE_OBJECT_BYTES).await
    }

    async fn get_bounded(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>> {
        use std::io::Read;

        let max_bytes = max_bytes.min(super::MAX_ARCHIVE_OBJECT_BYTES);
        let path = self.path(key)?;
        let path_metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(RymeError::NotFound(String::from("key")));
            }
            Err(error) => return Err(RymeError::from(error)),
        };
        if !path_metadata.file_type().is_file() {
            return Err(RymeError::Corrupt(String::from("archive object file type")));
        }
        let file = match ryme_error::open_read_no_follow(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(RymeError::NotFound(String::from("key")));
            }
            Err(e) => return Err(RymeError::from(e)),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(RymeError::Corrupt(String::from("archive object file type")));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            use std::os::unix::fs::PermissionsExt;
            if path_metadata.dev() != metadata.dev() || path_metadata.ino() != metadata.ino() {
                return Err(RymeError::Corrupt(String::from("archive object file identity")));
            }
            if metadata.permissions().mode() & 0o7777 != 0o600 {
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
        }
        if metadata.len() > max_bytes as u64 {
            return Err(RymeError::Overload(String::from("archive object")));
        }
        let mut bytes = Vec::new();
        file.take((max_bytes as u64).saturating_add(1)).read_to_end(&mut bytes)?;
        if bytes.len() > max_bytes {
            return Err(RymeError::Overload(String::from("archive object")));
        }
        Ok(bytes)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let validated_prefix = prefix.strip_suffix('/').unwrap_or(prefix);
        if (!prefix.is_empty() && validated_prefix.is_empty())
            || (!validated_prefix.is_empty() && !super::safe_relative_name(validated_prefix))
        {
            return Err(RymeError::InvalidArgument(String::from("prefix")));
        }
        self.validate_root()?;
        let mut out = Vec::new();
        Self::collect(&self.root.join(prefix), &self.root, &mut out)?;
        out.retain(|key| key.starts_with(prefix));
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.path(key)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(RymeError::from(e)),
        }
    }
}

#[cfg(unix)]
fn sync_archive_directory(path: &std::path::Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_archive_directory(_path: &std::path::Path) -> Result<()> {
    Ok(())
}
