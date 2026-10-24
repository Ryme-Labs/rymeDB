use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RymeError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("overload: {0}")]
    Overload(String),
    #[error("io: {0}")]
    Io(String),
    #[error("corrupt: {0}")]
    Corrupt(String),
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("read only: {0}")]
    ReadOnly(String),
    #[error("timeout")]
    Timeout,
    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, RymeError>;

/// Open a file for reading without following a Windows reparse point at the
/// final path component. Callers should still validate the expected file type.
pub fn open_read_no_follow(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    open_no_follow(path, options)
}

/// Open an existing file for reading and appending without following a
/// Windows reparse point at the final path component.
pub fn open_append_no_follow(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).append(true);
    open_no_follow(path, options)
}

/// Open an existing file for writing without following a Windows reparse point
/// at the final path component.
pub fn open_write_no_follow(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true);
    open_no_follow(path, options)
}

/// Atomically publishes `source` at `destination`, replacing an existing file.
/// On Windows, `ReplaceFileW` provides replacement semantics while allowing
/// compatible open handles; `MoveFileExW` handles a destination not yet present.
pub fn replace_file_atomically(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, ReplaceFileW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
            REPLACEFILE_WRITE_THROUGH,
        };

        let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
        let destination: Vec<u16> = destination.as_os_str().encode_wide().chain(Some(0)).collect();
        let replaced = unsafe {
            ReplaceFileW(
                destination.as_ptr(),
                source.as_ptr(),
                std::ptr::null(),
                REPLACEFILE_WRITE_THROUGH,
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        if replaced != 0 {
            return Ok(());
        }
        let replace_error = std::io::Error::last_os_error();
        if !matches!(replace_error.raw_os_error(), Some(2 | 3)) {
            return Err(replace_error);
        }
        let moved = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if moved == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(source, destination)
    }
}

fn open_no_follow(
    path: &std::path::Path,
    options: std::fs::OpenOptions,
) -> std::io::Result<std::fs::File> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        let mut options = options;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
        // Permit atomic replacement while a reader or writer still has this
        // handle open. Without delete sharing, Windows rejects WAL rewrites.
        options.share_mode(0x0000_0001 | 0x0000_0002 | 0x0000_0004);
        let file = options.open(path)?;
        if file.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "refusing to read a reparse point",
            ));
        }
        Ok(file)
    }
    #[cfg(not(windows))]
    {
        options.open(path)
    }
}

impl From<std::io::Error> for RymeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

#[cfg(test)]
mod file_publication_tests {
    use super::{open_append_no_follow, replace_file_atomically};

    #[test]
    fn replacement_succeeds_while_destination_is_open() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-error-replace-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let destination = dir.join("destination");
        let source = dir.join("source");
        std::fs::write(&destination, b"old").unwrap();
        std::fs::write(&source, b"new").unwrap();
        let open_destination = open_append_no_follow(&destination).unwrap();
        replace_file_atomically(&source, &destination).unwrap();
        drop(open_destination);
        assert_eq!(std::fs::read(&destination).unwrap(), b"new");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
