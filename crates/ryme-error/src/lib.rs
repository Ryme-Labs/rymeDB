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

impl From<std::io::Error> for RymeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}
