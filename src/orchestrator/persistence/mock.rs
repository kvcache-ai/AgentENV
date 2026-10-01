use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tonic::async_trait;

use super::super::store::SandboxMetadata;
use super::{CleanupMetrics, PersistenceResult, SandboxPersistenceError, SandboxPersister};
use crate::sandbox::PausedSandboxState;
use crate::types::SandboxId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum RecordingCall {
    LoadAll,
    AllocateArtifactRoot,
    DiscardArtifactGeneration,
    PersistPaused,
    RetainRuntimeGeneration,
    PruneArtifactGenerations,
    MarkResuming,
    RollbackResuming,
    CompleteResume,
    DeleteRecordAndArtifacts,
    DeleteIfPersisted,
    ReplayCleanupObligations,
}

impl RecordingCall {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LoadAll => "load_all",
            Self::AllocateArtifactRoot => "allocate_artifact_root",
            Self::DiscardArtifactGeneration => "discard_artifact_generation",
            Self::PersistPaused => "persist_paused",
            Self::RetainRuntimeGeneration => "retain_runtime_generation",
            Self::PruneArtifactGenerations => "prune_artifact_generations",
            Self::MarkResuming => "mark_resuming",
            Self::RollbackResuming => "rollback_resuming",
            Self::CompleteResume => "complete_resume",
            Self::DeleteRecordAndArtifacts => "delete_record_and_artifacts",
            Self::DeleteIfPersisted => "delete_if_persisted",
            Self::ReplayCleanupObligations => "replay_cleanup_obligations",
        }
    }
}

impl fmt::Display for RecordingCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Default)]
pub(crate) struct RecordingPersister {
    delays: Arc<Mutex<HashMap<RecordingCall, std::time::Duration>>>,
    pub(crate) calls: Arc<Mutex<Vec<RecordingCall>>>,
    loaded: Arc<Mutex<Vec<SandboxMetadata>>>,
    failures: Arc<Mutex<HashMap<RecordingCall, usize>>>,
    pending_cleanup: Arc<AtomicU64>,
    retain_delay: Arc<Mutex<Option<Duration>>>,
}

impl RecordingPersister {
    pub(crate) fn with_loaded(loaded: Vec<SandboxMetadata>) -> Self {
        Self {
            loaded: Arc::new(Mutex::new(loaded)),
            ..Default::default()
        }
    }

    pub(crate) fn calls(&self) -> Vec<RecordingCall> {
        self.calls.lock().unwrap().clone()
    }

    pub(crate) fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub(crate) fn record(&self, call: RecordingCall) {
        self.calls.lock().unwrap().push(call);
    }

    pub(crate) fn delay_retain(&self, delay: Duration) {
        *self.retain_delay.lock().unwrap() = Some(delay);
    }

    pub(crate) fn fail_next(&self, call: RecordingCall) {
        let mut failures = self.failures.lock().unwrap();
        *failures.entry(call).or_default() += 1;
    }

    pub(crate) fn delay(&self, call: RecordingCall, duration: std::time::Duration) {
        self.delays.lock().unwrap().insert(call, duration);
    }

    async fn wait_delay(&self, call: RecordingCall) {
        let delay = self.delays.lock().unwrap().get(&call).copied();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }

    fn maybe_fail(&self, call: RecordingCall) -> PersistenceResult<()> {
        let mut failures = self.failures.lock().unwrap();
        let Some(remaining) = failures.get_mut(&call) else {
            return Ok(());
        };
        if *remaining == 0 {
            return Ok(());
        }
        *remaining -= 1;
        Err(SandboxPersistenceError::InvalidRecord {
            reason: format!("forced {call} failure"),
            source: None,
        })
    }
}

#[async_trait]
impl SandboxPersister for RecordingPersister {
    async fn load_all<F>(&self, _factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: crate::sandbox::SandboxBackendFactory,
    {
        self.record(RecordingCall::LoadAll);
        self.maybe_fail(RecordingCall::LoadAll)?;
        Ok(self.loaded.lock().unwrap().clone())
    }

    async fn load_recovery<F>(
        &self,
        sandbox_id: &SandboxId,
        _factory: &F,
    ) -> PersistenceResult<Option<SandboxMetadata>>
    where
        F: crate::sandbox::SandboxBackendFactory,
    {
        Ok(self
            .loaded
            .lock()
            .unwrap()
            .iter()
            .find(|metadata| metadata.id == *sandbox_id)
            .cloned())
    }

    async fn allocate_artifact_root(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        self.record(RecordingCall::AllocateArtifactRoot);
        self.maybe_fail(RecordingCall::AllocateArtifactRoot)?;
        Ok(None)
    }

    async fn discard_artifact_generation(
        &self,
        _sandbox_id: &SandboxId,
        _artifact_root: Option<&Path>,
    ) -> PersistenceResult<()> {
        self.record(RecordingCall::DiscardArtifactGeneration);
        self.maybe_fail(RecordingCall::DiscardArtifactGeneration)
    }

    async fn persist_paused(
        &self,
        _metadata: &SandboxMetadata,
        _artifact_root: Option<&Path>,
        _paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        self.record(RecordingCall::PersistPaused);
        self.maybe_fail(RecordingCall::PersistPaused)?;
        Ok(())
    }

    async fn retain_runtime_generation(
        &self,
        _sandbox_id: &SandboxId,
        _artifact_root: Option<&Path>,
    ) -> PersistenceResult<()> {
        let delay = *self.retain_delay.lock().unwrap();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.record(RecordingCall::RetainRuntimeGeneration);
        self.maybe_fail(RecordingCall::RetainRuntimeGeneration)
    }

    async fn prune_artifact_generations(
        &self,
        _sandbox_id: &SandboxId,
        _keep_artifact_root: Option<&Path>,
    ) -> PersistenceResult<()> {
        self.record(RecordingCall::PruneArtifactGenerations);
        self.maybe_fail(RecordingCall::PruneArtifactGenerations)
    }

    async fn mark_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::MarkResuming);
        self.maybe_fail(RecordingCall::MarkResuming)?;
        Ok(())
    }

    async fn rollback_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::RollbackResuming);
        self.wait_delay(RecordingCall::RollbackResuming).await;
        self.maybe_fail(RecordingCall::RollbackResuming)?;
        Ok(())
    }

    async fn complete_resume(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::CompleteResume);
        self.wait_delay(RecordingCall::CompleteResume).await;
        self.maybe_fail(RecordingCall::CompleteResume)?;
        Ok(())
    }

    async fn delete_record_and_artifacts(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::DeleteRecordAndArtifacts);
        if let Err(error) = self.maybe_fail(RecordingCall::DeleteRecordAndArtifacts) {
            self.pending_cleanup.store(1, Ordering::Relaxed);
            return Err(error);
        }
        self.pending_cleanup.store(0, Ordering::Relaxed);
        Ok(())
    }

    async fn delete_if_persisted(&self, _sandbox_id: &SandboxId) -> PersistenceResult<bool> {
        self.record(RecordingCall::DeleteIfPersisted);
        self.maybe_fail(RecordingCall::DeleteIfPersisted)?;
        Ok(false)
    }

    async fn replay_cleanup_obligations(&self) -> PersistenceResult<Vec<SandboxId>> {
        self.record(RecordingCall::ReplayCleanupObligations);
        self.maybe_fail(RecordingCall::ReplayCleanupObligations)?;
        self.pending_cleanup.store(0, Ordering::Relaxed);
        Ok(Vec::new())
    }

    fn cleanup_metrics(&self) -> CleanupMetrics {
        CleanupMetrics {
            pending: self.pending_cleanup.load(Ordering::Relaxed),
            ..CleanupMetrics::default()
        }
    }
}
