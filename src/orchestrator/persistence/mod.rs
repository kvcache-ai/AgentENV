mod file_backed;
#[cfg(test)]
mod mock;

use async_trait::async_trait;
use serde_json::Value;
use std::path::{Path, PathBuf};

use crate::orchestrator::store::SandboxMetadata;
use crate::sandbox::{PausedSandboxState, SandboxBackendFactory};
use crate::types::SandboxId;

pub use file_backed::FileBackedSandboxPersister;
#[cfg(test)]
pub(crate) use mock::{RecordingCall, RecordingPersister};

pub type PersistenceResult<T> = std::result::Result<T, SandboxPersistenceError>;

/// A sandbox that was running when the previous server process stopped.
pub struct RunningSandboxRecord {
    pub metadata: SandboxMetadata,
    /// What its backend reported with `SandboxBackend::runtime_state`.
    pub runtime_state: Value,
}

/// Everything [`SandboxPersister::load_all`] found.
#[derive(Default)]
pub struct PersistedSandboxes {
    pub paused: Vec<SandboxMetadata>,
    pub running: Vec<RunningSandboxRecord>,
}

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
    /// Load all persisted sandboxes.
    async fn load_all<F>(&self, factory: &F) -> PersistenceResult<PersistedSandboxes>
    where
        F: SandboxBackendFactory;

    /// Persist metadata and runtime state for a running sandbox, replacing
    /// any earlier record of it, so that a later server process can adopt it.
    async fn persist_running(
        &self,
        metadata: &SandboxMetadata,
        runtime_state: Value,
    ) -> PersistenceResult<()>;

    /// Allocate an UNIQUE directory for sandbox artifacts.
    ///
    /// `None` means persistence is disabled and the sandbox backend should manage
    /// the lifecycle of its temporary artifacts.
    async fn allocate_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>>;

    /// Persist metadata and runtime state for a paused sandbox.
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
    async fn load_all<F>(&self, _factory: &F) -> PersistenceResult<PersistedSandboxes>
    where
        F: SandboxBackendFactory,
    {
        Ok(PersistedSandboxes::default())
    }

    async fn persist_running(
        &self,
        _metadata: &SandboxMetadata,
        _runtime_state: Value,
    ) -> PersistenceResult<()> {
        Ok(())
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
