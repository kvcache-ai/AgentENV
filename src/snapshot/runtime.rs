//! Node-local snapshot handles and their resource lifetimes.

use std::fmt;
use std::sync::Arc;

#[cfg(test)]
use agentenv_domain::{SnapshotManifest, SnapshotRecord};

use super::{SnapshotCatalog, SnapshotRuntimeManifest};
use crate::types::SnapshotId;

/// A complete set of node-local snapshot files and the resources keeping them usable.
pub struct SnapshotHandle {
    id: SnapshotId,
    runtime_manifest: SnapshotRuntimeManifest,
    _keepalive: Box<dyn Send + Sync>,
    /// In-flight startup-manifest recording stashed at capture time; the
    /// publish flow takes it exactly once. Interior mutability because the
    /// capture travels as `Arc<SnapshotHandle>`.
    startup_recording: std::sync::Mutex<Option<crate::snapshot::StartupRecording>>,
}

impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        // The final reference may be released by an API task or device teardown.
        // TempDir recursively deletes files, so move that work off async workers.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let keepalive = std::mem::replace(&mut self._keepalive, Box::new(()));
            runtime.spawn_blocking(move || drop(keepalive));
        }
    }
}

impl SnapshotHandle {
    pub fn new(
        id: SnapshotId,
        runtime_manifest: SnapshotRuntimeManifest,
        keepalive: impl Send + Sync + 'static,
    ) -> Self {
        Self {
            id,
            runtime_manifest,
            _keepalive: Box::new(keepalive),
            startup_recording: std::sync::Mutex::new(None),
        }
    }

    pub(crate) fn with_unpin(
        self: Arc<Self>,
        catalog: Arc<dyn SnapshotCatalog>,
        id: SnapshotId,
    ) -> Arc<Self> {
        struct UnpinOnDrop(Arc<dyn SnapshotCatalog>, SnapshotId);
        impl Drop for UnpinOnDrop {
            fn drop(&mut self) {
                self.0.unpin(&self.1);
            }
        }

        let recording = self.take_startup_recording();
        Arc::new(Self {
            id: self.id.clone(),
            runtime_manifest: self.runtime_manifest.clone(),
            _keepalive: Box::new((self, UnpinOnDrop(catalog, id))),
            startup_recording: std::sync::Mutex::new(recording),
        })
    }

    pub fn id(&self) -> &SnapshotId {
        &self.id
    }

    pub fn runtime_manifest(&self) -> &SnapshotRuntimeManifest {
        &self.runtime_manifest
    }

    /// Stash the in-flight startup-manifest recording for this capture. Only
    /// the publish flow consumes it (exactly once, via [`Self::take_startup_recording`]).
    pub(crate) fn set_startup_recording(&self, recording: crate::snapshot::StartupRecording) {
        *self
            .startup_recording
            .lock()
            .expect("startup recording lock poisoned") = Some(recording);
    }

    /// Take the stashed startup-manifest recording (publish consumes it once).
    pub(crate) fn take_startup_recording(&self) -> Option<crate::snapshot::StartupRecording> {
        self.startup_recording
            .lock()
            .expect("startup recording lock poisoned")
            .take()
    }

    #[cfg(test)]
    pub fn mock_record() -> SnapshotRecord<Arc<Self>> {
        Self::record_from_test_manifest(SnapshotRecord::mock_ready(SnapshotManifest::mock()), &[])
    }

    #[cfg(test)]
    pub(crate) fn record_from_test_manifest(
        record: SnapshotRecord,
        drives: &[crate::types::ExtraDrive],
    ) -> SnapshotRecord<Arc<Self>> {
        let handle = Arc::new(Self::new(
            record.id.clone(),
            SnapshotRuntimeManifest::for_test(0, drives),
            (),
        ));
        record.with_manifest(handle)
    }

    #[cfg(test)]
    pub(crate) fn record_from_test_legacy_manifest(
        record: SnapshotRecord,
        drives: &[crate::types::ExtraDrive],
    ) -> SnapshotRecord<Arc<Self>> {
        let mut runnable = Self::record_from_test_manifest(record, drives);
        Arc::get_mut(&mut runnable.manifest)
            .unwrap()
            .runtime_manifest
            .volume_drive_slots = 0;
        runnable
    }

    #[cfg(test)]
    pub(crate) fn record_from_test_slot_drive_manifest(
        record: SnapshotRecord,
        drives: &[crate::types::ExtraDrive],
    ) -> SnapshotRecord<Arc<Self>> {
        let mut runnable = Self::record_from_test_manifest(record, drives);
        Arc::get_mut(&mut runnable.manifest)
            .unwrap()
            .runtime_manifest
            .physical_extra_drive_count = 0;
        runnable
    }
}

impl fmt::Debug for SnapshotHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotHandle")
            .field("id", &self.id)
            .field("manifest", &self.runtime_manifest)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn final_snapshot_owner_releases_resources_off_async_worker() {
        struct DropThread(std::sync::mpsc::Sender<std::thread::ThreadId>);
        impl Drop for DropThread {
            fn drop(&mut self) {
                let _ = self.0.send(std::thread::current().id());
            }
        }
        let worker = std::thread::current().id();
        let (sent, received) = std::sync::mpsc::channel();
        let snapshot = Arc::new(SnapshotHandle::new(
            SnapshotId::generate(),
            SnapshotRuntimeManifest::for_test(4096, &[]),
            DropThread(sent),
        ));
        let reader = Arc::clone(&snapshot);
        drop(snapshot);
        assert!(received.try_recv().is_err());
        drop(reader);
        let released_on = received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_ne!(released_on, worker);
    }
}
