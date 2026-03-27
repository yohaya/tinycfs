use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum TinyCfsError {
    #[error("IO error: {0}")]
    Io(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Not leader; leader hint: {leader_hint:?}")]
    NotLeader { leader_hint: Option<u64> },

    #[error("No quorum available")]
    NoQuorum,

    #[error("Entry not found: {0}")]
    NotFound(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Not a directory")]
    NotDirectory,

    #[error("Is a directory")]
    IsDirectory,

    #[error("Directory not empty")]
    NotEmpty,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Timeout waiting for consensus")]
    Timeout,

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Cluster error: {0}")]
    Cluster(String),

    #[error("Node disconnected: {0}")]
    Disconnected(String),

    /// The file lock is held by another node. Maps to EWOULDBLOCK for non-blocking
    /// setlk, and triggers a retry loop for blocking setlkw.
    #[error("Lock contended: held by {0}")]
    LockContended(String),

    /// File exceeds configured max_file_size_bytes.
    #[error("File too large (limit: {limit} bytes)")]
    FileTooLarge { limit: u64 },
}

impl From<std::io::Error> for TinyCfsError {
    fn from(e: std::io::Error) -> Self {
        TinyCfsError::Io(e.to_string())
    }
}

impl From<serde_json::Error> for TinyCfsError {
    fn from(e: serde_json::Error) -> Self {
        TinyCfsError::Serialization(e.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for TinyCfsError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        TinyCfsError::Serialization(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, TinyCfsError>;

/// Convert TinyCfsError to a POSIX errno for FUSE replies.
pub fn to_errno(e: &TinyCfsError) -> i32 {
    match e {
        TinyCfsError::NotFound(_) => libc::ENOENT,
        TinyCfsError::AlreadyExists(_) => libc::EEXIST,
        TinyCfsError::NotDirectory => libc::ENOTDIR,
        TinyCfsError::IsDirectory => libc::EISDIR,
        TinyCfsError::NotEmpty => libc::ENOTEMPTY,
        TinyCfsError::InvalidArgument(_) => libc::EINVAL,
        TinyCfsError::LockContended(_) => libc::EWOULDBLOCK,
        TinyCfsError::FileTooLarge { .. } => libc::EFBIG,
        // No quorum / no leader / timeout → filesystem is read-only until a
        // leader is elected. Return EROFS so callers see a clear "read-only"
        // signal rather than a transient resource error.
        TinyCfsError::NoQuorum | TinyCfsError::Timeout | TinyCfsError::NotLeader { .. } => {
            libc::EROFS
        }
        _ => libc::EIO,
    }
}
