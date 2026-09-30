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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CleanupMetrics {
    pub pending: u64,
    pub retries: u64,
    pub failures: u64,
    pub pruned_generations: u64,
    pub reclaimed_snapshot_bytes: u64,
    pub reserved_journal_bytes: u64,
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
    #[error("paused sandbox cleanup failed for both record and artifacts: record={record}; artifacts={artifacts}")]
    Cleanup {
        record: Box<SandboxPersistenceError>,
        artifacts: Box<SandboxPersistenceError>,
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

    /// Read the last durable checkpoint without replaying unrelated cleanup.
    async fn load_recovery<F>(
        &self,
        sandbox_id: &SandboxId,
        factory: &F,
    ) -> PersistenceResult<Option<SandboxMetadata>>
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

    /// Remove one allocated generation that never became the durable record.
    async fn discard_artifact_generation(
        &self,
        sandbox_id: &SandboxId,
        artifact_root: Option<&Path>,
    ) -> PersistenceResult<()>;

    /// Persist metadata and runtime state for a paused sandbox.
    ///
    /// On failure the allocated generation is left in place; the caller must
    /// discard it or, if the runtime resumed on it, retain it.
    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        artifact_root: Option<&Path>,
        paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()>;

    /// Keep an uncommitted generation that a resumed runtime still reads.
    ///
    /// It is protected from pruning until the next durable pause or final
    /// delete of the sandbox, and no longer blocks final delete.
    async fn retain_runtime_generation(
        &self,
        sandbox_id: &SandboxId,
        artifact_root: Option<&Path>,
    ) -> PersistenceResult<()>;

    /// Delete artifact generations superseded by the exact current generation.
    async fn prune_artifact_generations(
        &self,
        sandbox_id: &SandboxId,
        keep_artifact_root: Option<&Path>,
    ) -> PersistenceResult<()>;

    /// Mark a paused sandbox as resuming.
    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Roll back a resuming mark after a failed resume attempt.
    async fn rollback_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Finish a successful resume while retaining its last recovery generation.
    async fn complete_resume(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete the persistence record and all associated artifacts.
    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete a raw persisted record even when it cannot be decoded.
    async fn delete_if_persisted(&self, sandbox_id: &SandboxId) -> PersistenceResult<bool>;

    /// Replay obligations, returning successful final deletes even on partial failure.
    /// Failed obligations remain pending; only journal-read errors abort the pass.
    async fn replay_cleanup_obligations(&self) -> PersistenceResult<Vec<SandboxId>>;

    /// Whether every retained paused record can be re-pinned before image GC.
    fn image_gc_safe(&self) -> bool {
        true
    }

    fn cleanup_metrics(&self) -> CleanupMetrics;
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

    async fn load_recovery<F>(
        &self,
        _sandbox_id: &SandboxId,
        _factory: &F,
    ) -> PersistenceResult<Option<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        Ok(None)
    }

    async fn allocate_artifact_root(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        Ok(None)
    }

    async fn discard_artifact_generation(
        &self,
        _sandbox_id: &SandboxId,
        _artifact_root: Option<&Path>,
    ) -> PersistenceResult<()> {
        Ok(())
    }

    async fn persist_paused(
        &self,
        _metadata: &SandboxMetadata,
        _artifact_root: Option<&Path>,
        _paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        Ok(())
    }

    async fn retain_runtime_generation(
        &self,
        _sandbox_id: &SandboxId,
        _artifact_root: Option<&Path>,
    ) -> PersistenceResult<()> {
        Ok(())
    }

    async fn prune_artifact_generations(
        &self,
        _sandbox_id: &SandboxId,
        _keep_artifact_root: Option<&Path>,
    ) -> PersistenceResult<()> {
        Ok(())
    }

    async fn mark_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn rollback_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn complete_resume(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn delete_record_and_artifacts(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn delete_if_persisted(&self, _sandbox_id: &SandboxId) -> PersistenceResult<bool> {
        Ok(false)
    }

    async fn replay_cleanup_obligations(&self) -> PersistenceResult<Vec<SandboxId>> {
        Ok(Vec::new())
    }

    fn cleanup_metrics(&self) -> CleanupMetrics {
        CleanupMetrics::default()
    }
}
