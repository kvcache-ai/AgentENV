use agentenv_domain::SnapshotId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type MetaResult<T> = Result<T, MetaError>;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum MetaError {
    #[error("invalid metadata request: {reason}")]
    InvalidRequest { reason: String },
    #[error("snapshot '{id}' conflicts with the existing entry")]
    SnapshotConflict { id: SnapshotId },
    #[error("alias '{alias}' already points to '{existing}', cannot bind to '{new_id}'")]
    AliasConflict {
        alias: String,
        existing: SnapshotId,
        new_id: SnapshotId,
    },
    #[error("snapshot unavailable: {reason}")]
    Unavailable { reason: String },
    #[error("metadata backend error: {message}")]
    Backend { message: String },
}

impl MetaError {
    pub(crate) fn backend(context: &str, error: impl std::fmt::Display) -> Self {
        Self::Backend {
            message: format!("{context}: {error}"),
        }
    }
}
