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
mod tests {
    use super::*;

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
}

impl crate::ObjectStore for LocalStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let path = self.path(key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.path(key)?;
        match std::fs::read(&path) {
            Ok(bytes) => Ok(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(RymeError::NotFound(String::from("key")))
            }
            Err(e) => Err(RymeError::from(e)),
        }
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
