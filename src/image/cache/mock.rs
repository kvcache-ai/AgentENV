use anyhow::Result;
use async_trait::async_trait;

use super::{RuntimeImageOwner, RuntimeImageRefs};
use crate::sandbox::RuntimeArtifactSet;
use crate::types::SandboxId;

#[derive(Debug, Default)]
pub(crate) struct RecordingRuntimeImageRefs {
    pinned: std::sync::Mutex<Vec<(RuntimeImageOwner, RuntimeArtifactSet)>>,
    unpinned: std::sync::Mutex<Vec<RuntimeImageOwner>>,
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
}

#[async_trait]
impl RuntimeImageRefs for RecordingRuntimeImageRefs {
    async fn pin(&self, owner: RuntimeImageOwner, artifacts: RuntimeArtifactSet) -> Result<()> {
        self.pinned
            .lock()
            .expect("pinned refs mutex poisoned")
            .push((owner, artifacts));
        Ok(())
    }

    async fn unpin_best_effort(&self, owner: RuntimeImageOwner) {
        self.unpinned
            .lock()
            .expect("unpinned refs mutex poisoned")
            .push(owner);
    }

    async fn reconcile_paused(&self, _live_paused: &[SandboxId]) -> Result<()> {
        Ok(())
    }

    async fn maintain_running(&self, _running: Vec<(SandboxId, RuntimeArtifactSet)>) -> Result<()> {
        Ok(())
    }
}
