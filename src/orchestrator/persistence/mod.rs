mod file_backed;
#[cfg(test)]
mod mock;

use async_trait::async_trait;
use std::path::{Path, PathBuf};

use crate::orchestrator::store::SandboxMetadata;
use crate::sandbox::{PausedSandboxState, SandboxBackendFactory};
use crate::types::SandboxId;

pub use file_backed::FileBackedSandboxPersister;
#[cfg(test)]
pub(crate) use mock::{RecordingCall, RecordingPersister};

pub type PersistenceResult<T> = std::result::Result<T, SandboxPersistenceError>;

#[derive(Debug, thiserror::Error)]
pub enum SandboxPersistenceError {
    #[error("failed to {operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid sandbox record: {reason}")]
    InvalidRecord {
        reason: String,
        #[source]
        source: Option<anyhow::Error>,
    },
    #[error("invalid paused sandbox runtime state: {reason}")]
    RuntimeState { reason: &'static str },
    #[error("paused sandbox store operation failed: {operation}: {source}")]
    Store {
        operation: &'static str,
        #[source]
        source: anyhow::Error,
    },
}

impl SandboxPersistenceError {
    pub(super) fn io(
        operation: &'static str,
        path: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::Io {
            operation,
            path: path.into(),
            source,
        }
    }

    pub(super) fn store(operation: &'static str, source: anyhow::Error) -> Self {
        Self::Store { operation, source }
    }
}

#[async_trait]
/// Persistence interface for sandbox records and artifacts.
pub trait SandboxPersister: Send + Sync {
    /// Load all persisted sandbox metadata.
    async fn load_all<F>(&self, factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory;

    /// Allocate an UNIQUE directory for sandbox artifacts.
    ///
    /// `None` means persistence is disabled and the sandbox backend should manage
    /// the lifecycle of its temporary artifacts.
    async fn allocate_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>>;

    /// Persist metadata and runtime state for a paused sandbox.
    ///
    /// On failure the artifact root is left in place: the caller may resume
    /// the sandbox, and capture can leave layers its live runtime still
    /// references under that root. The sandbox's artifacts are removed with
    /// [`Self::delete_record_and_artifacts`] or by the startup orphan sweep.
    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        artifact_root: Option<&Path>,
        paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()>;

    /// Mark a paused sandbox as resuming.
    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Roll back a resuming mark after a failed resume attempt.
    async fn rollback_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete the persistence record for a sandbox.
    async fn delete_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete the persistence record and all associated artifacts.
    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;
}

#[derive(Default)]
pub struct DisabledSandboxPersister;

#[async_trait]
impl SandboxPersister for DisabledSandboxPersister {
    async fn load_all<F>(&self, _factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        Ok(Vec::new())
    }

    async fn allocate_artifact_root(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        Ok(None)
    }

    async fn persist_paused(
        &self,
        _metadata: &SandboxMetadata,
        _artifact_root: Option<&Path>,
        _paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        Ok(())
    }

    async fn mark_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn rollback_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn delete_record(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn delete_record_and_artifacts(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }
}
