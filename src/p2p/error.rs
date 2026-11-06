use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("P2P artifact transport is disabled")]
    TransportDisabled,

    #[error("invalid P2P artifact descriptor: {reason}")]
    InvalidDescriptor { reason: String },

    #[error("invalid P2P artifact catalog at {path:?}: {reason}")]
    InvalidCatalog { path: PathBuf, reason: String },

    #[error("P2P operation timed out: {operation}")]
    Timeout { operation: &'static str },

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl Error {
    #[cfg(feature = "p2p-iroh")]
    pub(crate) fn internal_message(
        operation: &'static str,
        source: impl std::fmt::Display,
    ) -> Self {
        Self::Internal(anyhow::anyhow!("{operation}: {source}"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
