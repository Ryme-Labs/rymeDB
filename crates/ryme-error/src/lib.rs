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
    options.read(true).append(true);
    open_no_follow(path, options)
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
