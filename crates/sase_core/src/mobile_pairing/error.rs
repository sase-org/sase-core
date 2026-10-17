use std::path::PathBuf;

/// Failures from owner-local pairing issue and consume.
#[derive(Debug, thiserror::Error)]
pub enum MobilePairingError {
    #[error("unsupported pairing schema version {found}; expected {expected}")]
    SchemaVersion { found: u32, expected: u32 },
    #[error(
        "mobile pairing is not supported on this platform; owner-isolated pairing requires Linux or macOS"
    )]
    UnsupportedPlatform,
    #[error("pairing store path is unsafe: {reason}")]
    UnsafePath { reason: String },
    #[error("pairing store is corrupt")]
    Corrupt,
    #[error("pairing store is unavailable")]
    Unavailable,
    #[error("timed out waiting for the pairing store lock")]
    LockTimeout,
    #[error("no pairing store is present")]
    MissingStore,
}

impl MobilePairingError {
    pub(crate) fn unsafe_path(reason: impl Into<String>) -> Self {
        Self::UnsafePath {
            reason: reason.into(),
        }
    }

    pub(crate) fn io(
        _path: impl Into<PathBuf>,
        _source: std::io::Error,
    ) -> Self {
        Self::Unavailable
    }
}
