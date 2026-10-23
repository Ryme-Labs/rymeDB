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

    fn path(&self, key: &str) -> Result<PathBuf> {
        if !super::safe_relative_name(key) {
            return Err(RymeError::InvalidArgument(String::from("key")));
        }
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
        if Self::has_symlink_component(root, prefix)? {
            return Ok(());
        }
        let entries = match std::fs::read_dir(prefix) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(RymeError::from(e)),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                continue;
            } else if file_type.is_dir() {
                Self::collect(&path, root, out)?;
            } else if file_type.is_file() {
                if let Ok(relative) = path.strip_prefix(root) {
                    out.push(relative.to_string_lossy().replace('\\', "/"));
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
        ] {
            assert!(matches!(store.path(key), Err(RymeError::InvalidArgument(_))), "{key}");
        }
        assert_eq!(
            store.path("backups/backup-1/file.wal").unwrap(),
            PathBuf::from("archive/backups/backup-1/file.wal")
        );
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
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let (tmp, mut file) = super::create_archive_temp_file(&path)?;
        let write_result = (|| -> Result<()> {
            std::io::Write::write_all(&mut file, &bytes)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&tmp, &path)?;
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
        let file = match std::fs::File::open(&path) {
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
            if metadata.permissions().mode() & 0o077 != 0 {
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
