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

    /// Prepare the stable data directory owned by this logical sandbox.
    async fn prepare_runtime_root(&self, sandbox_id: &SandboxId) -> PersistenceResult<PathBuf>;

    /// Persist metadata and runtime state for a paused sandbox.
    /// Failure leaves artifacts intact: the live backend may still own them.
    /// They are reclaimed when the sandbox is deleted or by orphan cleanup.
    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()>;

    /// Mark a paused sandbox as resuming.
    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Roll back a resuming mark after a failed resume attempt.
    async fn rollback_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete only the persisted record, retaining artifacts still referenced by
    /// the running sandbox after a successful resume.
    async fn delete_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete the record and detach its artifacts. Physical reclamation may
    /// finish after this returns, without touching a later pause generation.
    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;
}

pub struct DisabledSandboxPersister {
    root: tempfile::TempDir,
}

impl Default for DisabledSandboxPersister {
    fn default() -> Self {
        let parent = crate::cfg::ConfigManager::global_config()
            .firecracker
            .work_dir
            .as_ref()
            .expect("resolved Firecracker work directory");
        std::fs::create_dir_all(parent).expect("prepare temporary sandbox storage");
        Self {
            root: tempfile::tempdir_in(parent).expect("create temporary sandbox storage"),
        }
    }
}

#[async_trait]
impl SandboxPersister for DisabledSandboxPersister {
    async fn load_all<F>(&self, _factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        Ok(Vec::new())
    }

    async fn prepare_runtime_root(&self, sandbox_id: &SandboxId) -> PersistenceResult<PathBuf> {
        let root = self
            .root
            .path()
            .join(sandbox_id.to_string())
            .join("runtime");
        tokio::fs::create_dir_all(&root).await.map_err(|source| {
            SandboxPersistenceError::io("prepare runtime storage", &root, source)
        })?;
        tokio::fs::canonicalize(&root)
            .await
            .map_err(|source| SandboxPersistenceError::io("resolve runtime storage", &root, source))
    }

    async fn persist_paused(
        &self,
        _metadata: &SandboxMetadata,
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

    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        retire_artifacts(&self.root.path().join(sandbox_id.to_string())).await
    }
}

async fn retire_artifacts(root: &Path) -> PersistenceResult<()> {
    let retired = root.with_file_name(format!(".retired-{}", uuid::Uuid::now_v7()));
    match tokio::fs::rename(root, &retired).await {
        Ok(()) => {
            tokio::spawn(async move {
                if let Err(error) = tokio::fs::remove_dir_all(&retired).await {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(%error, path = %retired.display(), "failed to reclaim sandbox storage");
                    }
                }
            });
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(SandboxPersistenceError::io(
            "retire sandbox storage",
            root,
            source,
        )),
    }
}
