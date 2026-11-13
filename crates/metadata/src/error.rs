use agentenv_domain::SnapshotId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type MetaResult<T> = Result<T, MetaError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayerState {
    Missing,
    Available,
    Deleting,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayerCheck {
    pub uuid: String,
    pub state: LayerState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletableLayer {
    pub uuid: String,
    pub digest: String,
}

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
    #[error("managed layers must be re-uploaded before publication: {uuids:?}")]
    MissingLayers { uuids: Vec<String> },
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
