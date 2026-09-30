use super::gc::ImageCacheGcSummary;
use super::{RuntimeImageOwner, RuntimeImageRefs};
use crate::sandbox::RuntimeArtifactSet;
use crate::types::SandboxId;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Debug, Default)]
pub(crate) struct RecordingRuntimeImageRefs {
    pinned: std::sync::Mutex<Vec<(RuntimeImageOwner, RuntimeArtifactSet)>>,
    unpinned: std::sync::Mutex<Vec<RuntimeImageOwner>>,
    active: std::sync::Mutex<Vec<(RuntimeImageOwner, RuntimeArtifactSet)>>,
    next_pin_barrier: std::sync::Mutex<Option<(Arc<Semaphore>, Arc<Semaphore>)>>,
}

impl RecordingRuntimeImageRefs {
    pub(crate) fn pinned(&self) -> Vec<(RuntimeImageOwner, RuntimeArtifactSet)> {
        self.pinned
            .lock()
            .expect("pinned refs mutex poisoned")
            .clone()
    }

    pub(crate) fn unpinned(&self) -> Vec<RuntimeImageOwner> {
        self.unpinned
            .lock()
            .expect("unpinned refs mutex poisoned")
            .clone()
    }

    pub(crate) fn active_owners(&self) -> Vec<RuntimeImageOwner> {
        self.active
            .lock()
            .expect("active refs mutex poisoned")
            .iter()
            .map(|(owner, _)| owner.clone())
            .collect()
    }

    pub(crate) fn block_next_pin(&self, reached: Arc<Semaphore>, resume: Arc<Semaphore>) {
        *self
            .next_pin_barrier
            .lock()
            .expect("pin barrier mutex poisoned") = Some((reached, resume));
    }
}

#[async_trait]
impl RuntimeImageRefs for RecordingRuntimeImageRefs {
    async fn pin(&self, owner: RuntimeImageOwner, artifacts: RuntimeArtifactSet) -> Result<()> {
        let barrier = self
            .next_pin_barrier
            .lock()
            .expect("pin barrier mutex poisoned")
            .take();
        if let Some((reached, resume)) = barrier {
            reached.add_permits(1);
            resume
                .acquire()
                .await
                .expect("resume pin barrier open")
                .forget();
        }
        self.pinned
            .lock()
            .expect("pinned refs mutex poisoned")
            .push((owner.clone(), artifacts.clone()));
        let mut active = self.active.lock().expect("active refs mutex poisoned");
        active.retain(|(active_owner, _)| active_owner != &owner);
        active.push((owner, artifacts));
        Ok(())
    }

    async fn unpin_best_effort(&self, owner: RuntimeImageOwner) {
        self.unpinned
            .lock()
            .expect("unpinned refs mutex poisoned")
            .push(owner.clone());
        self.active
            .lock()
            .expect("active refs mutex poisoned")
            .retain(|(active_owner, _)| active_owner != &owner);
    }

    async fn reconcile_paused(&self, _live_paused: &[SandboxId]) -> Result<()> {
        Ok(())
    }

    async fn maintain_running(&self, _running: Vec<(SandboxId, RuntimeArtifactSet)>) -> Result<()> {
        Ok(())
    }

    async fn reclaim_for_disk_pressure(
        &self,
        _running: Vec<(SandboxId, RuntimeArtifactSet)>,
        _reclaim_bytes: u64,
    ) -> Result<ImageCacheGcSummary> {
        Ok(ImageCacheGcSummary::default())
    }
}
