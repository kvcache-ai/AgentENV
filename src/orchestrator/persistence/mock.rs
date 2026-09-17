use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tonic::async_trait;

use super::super::store::SandboxMetadata;
use super::{PersistenceResult, SandboxPersistenceError, SandboxPersister};
use crate::sandbox::PausedSandboxState;
use crate::types::SandboxId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum RecordingCall {
    LoadAll,
    PrepareRuntimeRoot,
    PersistPaused,
    MarkResuming,
    RollbackResuming,
    DeleteRecord,
    DeleteRecordAndArtifacts,
}

impl RecordingCall {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LoadAll => "load_all",
            Self::PrepareRuntimeRoot => "prepare_runtime_root",
            Self::PersistPaused => "persist_paused",
            Self::MarkResuming => "mark_resuming",
            Self::RollbackResuming => "rollback_resuming",
            Self::DeleteRecord => "delete_record",
            Self::DeleteRecordAndArtifacts => "delete_record_and_artifacts",
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
    pub(crate) calls: Arc<Mutex<Vec<RecordingCall>>>,
    loaded: Arc<Mutex<Vec<SandboxMetadata>>>,
    failures: Arc<Mutex<HashMap<RecordingCall, usize>>>,
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

    pub(crate) fn fail_next(&self, call: RecordingCall) {
        let mut failures = self.failures.lock().unwrap();
        *failures.entry(call).or_default() += 1;
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

    async fn prepare_runtime_root(&self, _sandbox_id: &SandboxId) -> PersistenceResult<PathBuf> {
        self.record(RecordingCall::PrepareRuntimeRoot);
        self.maybe_fail(RecordingCall::PrepareRuntimeRoot)?;
        Ok(std::env::temp_dir().join("mock-sandbox-storage"))
    }

    async fn persist_paused(
        &self,
        _metadata: &SandboxMetadata,
        _paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        self.record(RecordingCall::PersistPaused);
        self.maybe_fail(RecordingCall::PersistPaused)?;
        Ok(())
    }

    async fn mark_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::MarkResuming);
        self.maybe_fail(RecordingCall::MarkResuming)?;
        Ok(())
    }

    async fn rollback_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::RollbackResuming);
        self.maybe_fail(RecordingCall::RollbackResuming)?;
        Ok(())
    }

    async fn delete_record(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::DeleteRecord);
        self.maybe_fail(RecordingCall::DeleteRecord)?;
        Ok(())
    }

    async fn delete_record_and_artifacts(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::DeleteRecordAndArtifacts);
        self.maybe_fail(RecordingCall::DeleteRecordAndArtifacts)?;
        Ok(())
    }
}
