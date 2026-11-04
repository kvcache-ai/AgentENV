use super::drive::ResolvedAttachedDrive;
use crate::snapshot::SandboxSnapshotManifest;
#[cfg(test)]
use crate::types::{ManagedLayer, OverlaybdLayerRef};
use crate::types::{SandboxResources, SnapshotId};
use std::fmt;
use std::sync::Arc;

pub use agentenv_domain::{
    rootfs_snapshot_image_tag, CommandContext, CommittedSnapshot, PersistedDiskImagePublication,
    SnapshotPlacement, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord,
    SnapshotSource, SnapshotSourceKind, SnapshotVolume, StartupCommand, TemplateBuildErrorReason,
    TemplateBuildInfo, TemplateBuildStatus,
};

/// A complete set of node-local snapshot files and the resources keeping them usable.
pub struct Snapshot {
    id: SnapshotId,
    manifest: SandboxSnapshotManifest,
    _keepalive: Box<dyn Send + Sync>,
    /// In-flight startup-manifest recording stashed at capture time; the
    /// publish flow takes it exactly once. Interior mutability because the
    /// capture travels as `Arc<Snapshot>`.
    startup_recording: std::sync::Mutex<Option<crate::snapshot::StartupRecording>>,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        // The final reference may be released by an API task or device teardown.
        // TempDir recursively deletes files, so move that work off async workers.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let keepalive = std::mem::replace(&mut self._keepalive, Box::new(()));
            runtime.spawn_blocking(move || drop(keepalive));
        }
    }
}

impl Snapshot {
    pub fn new(
        id: SnapshotId,
        manifest: SandboxSnapshotManifest,
        keepalive: impl Send + Sync + 'static,
    ) -> Self {
        Self {
            id,
            manifest,
            _keepalive: Box::new(keepalive),
            startup_recording: std::sync::Mutex::new(None),
        }
    }

    pub fn id(&self) -> &SnapshotId {
        &self.id
    }

    pub fn manifest(&self) -> &SandboxSnapshotManifest {
        &self.manifest
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
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("id", &self.id)
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
/// A public snapshot record paired with its node-local files.
pub struct RunnableSnapshot {
    record: SnapshotRecord,
    snapshot: Arc<Snapshot>,
}

impl RunnableSnapshot {
    pub(crate) fn snapshot(&self) -> Arc<Snapshot> {
        Arc::clone(&self.snapshot)
    }

    pub(crate) fn new(record: SnapshotRecord, snapshot: Arc<Snapshot>) -> Self {
        Self { record, snapshot }
    }

    /// Returns the runtime-resolved attached drives for this snapshot.
    pub fn attached_drives(&self) -> Vec<ResolvedAttachedDrive> {
        self.manifest()
            .attached_drives
            .iter()
            .map(|drive| ResolvedAttachedDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                image_config_path: drive.image_config_path.clone(),
                read_only: drive.read_only,
                virtual_size: drive.virtual_size,
                mount_path: crate::types::normalize_mount_path_for_drive(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                )
                .unwrap_or_else(|_| crate::types::ExtraDrive::default_mount_path(&drive.drive_id)),
                sub_path: drive.sub_path.clone(),
            })
            .collect()
    }

    pub fn manifest(&self) -> &SandboxSnapshotManifest {
        self.snapshot.manifest()
    }

    /// Returns the committed snapshot record backing this runnable snapshot.
    pub fn record(&self) -> &SnapshotRecord {
        &self.record
    }

    /// Returns the committed snapshot artifact payload backing this runnable snapshot.
    pub fn committed(&self) -> &CommittedSnapshot {
        self.record
            .committed
            .as_ref()
            .expect("runnable snapshots always have committed artifact payloads")
    }

    /// Returns the CPU and memory settings for this runnable snapshot.
    pub fn resources(&self) -> &SandboxResources {
        &self.record.resources
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        Self::from_test_manifest(
            SnapshotRecord::mock_ready(CommittedSnapshot::mock()),
            Vec::new(),
        )
    }

    #[cfg(test)]
    pub(crate) fn from_test_manifest(
        record: SnapshotRecord,
        attached_drives: Vec<ResolvedAttachedDrive>,
    ) -> Self {
        let extra_drives: Vec<crate::types::ExtraDrive> = attached_drives
            .iter()
            .map(ResolvedAttachedDrive::to_extra_drive)
            .collect();

        let snapshot = Arc::new(Snapshot::new(
            record.id.clone(),
            SandboxSnapshotManifest::for_test(0, &extra_drives),
            (),
        ));
        Self::new(record, snapshot)
    }
    #[cfg(test)]
    pub(crate) fn from_test_legacy_manifest(
        record: SnapshotRecord,
        attached_drives: Vec<ResolvedAttachedDrive>,
    ) -> Self {
        let mut runnable = Self::from_test_manifest(record, attached_drives);
        Arc::get_mut(&mut runnable.snapshot)
            .unwrap()
            .manifest
            .volume_drive_slots = 0;
        runnable
    }

    /// Snapshot whose attached drives all occupy reserved slots.
    #[cfg(test)]
    pub(crate) fn from_test_slot_drive_manifest(
        record: SnapshotRecord,
        attached_drives: Vec<ResolvedAttachedDrive>,
    ) -> Self {
        let mut runnable = Self::from_test_manifest(record, attached_drives);
        Arc::get_mut(&mut runnable.snapshot)
            .unwrap()
            .manifest
            .physical_extra_drive_count = 0;
        runnable
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{SnapshotAlias, VolumeMode};

    use super::{
        rootfs_snapshot_image_tag, CommandContext, CommittedSnapshot, ManagedLayer,
        OverlaybdLayerRef, PersistedDiskImagePublication, SnapshotPlacement,
        SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
        SnapshotVolume, TemplateBuildErrorReason,
    };
    use std::collections::HashMap;

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
        let snapshot = std::sync::Arc::new(super::Snapshot::new(
            super::SnapshotId::generate(),
            crate::snapshot::SandboxSnapshotManifest::for_test(4096, &[]),
            DropThread(sent),
        ));
        let reader = std::sync::Arc::clone(&snapshot);
        drop(snapshot);
        assert!(received.try_recv().is_err());
        drop(reader);
        let released_on = received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_ne!(released_on, worker);
    }

    fn publication(
        image_ref: impl Into<String>,
        tag: impl Into<String>,
    ) -> PersistedDiskImagePublication {
        PersistedDiskImagePublication {
            image_ref: image_ref.into(),
            tag: tag.into(),
            manifest_digest: "sha256:manifest".to_string(),
            repo_blob_url: "https://registry.example/v2/ns/app/blobs".to_string(),
        }
    }

    #[test]
    fn sandbox_source_metadata_serde_is_backward_compatible() {
        let legacy = r#"{"Sandbox":{"source_sandbox_id":"sandbox-1"}}"#;
        let source: SnapshotSource = serde_json::from_str(legacy).expect("parse legacy source");
        let SnapshotSource::Sandbox {
            ref source_sandbox_id,
            ref source_sandbox_metadata,
        } = source
        else {
            panic!("expected sandbox source");
        };
        assert_eq!(source_sandbox_id, "sandbox-1");
        assert_eq!(*source_sandbox_metadata, None);
        // Serializing a metadata-less source keeps the legacy shape.
        assert_eq!(serde_json::to_string(&source).unwrap(), legacy);

        let with_metadata = r#"{"Sandbox":{"source_sandbox_id":"sandbox-1","source_sandbox_metadata":{"team":"alpha"}}}"#;
        let source: SnapshotSource =
            serde_json::from_str(with_metadata).expect("parse metadata source");
        let SnapshotSource::Sandbox {
            source_sandbox_metadata,
            ..
        } = source
        else {
            panic!("expected sandbox source");
        };
        assert_eq!(
            source_sandbox_metadata.as_ref().and_then(|m| m.get("team")),
            Some(&"alpha".to_string()),
        );
    }

    #[test]
    fn snapshot_record_without_tools_drive_version_remains_readable() {
        let record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let mut value = serde_json::to_value(record).expect("serialize snapshot record");
        value["committed"]["runtime_versions"]
            .as_object_mut()
            .expect("runtime versions must be an object")
            .remove("tools_drive_version");

        let record: SnapshotRecord =
            serde_json::from_value(value).expect("deserialize legacy snapshot record");

        assert!(record
            .committed
            .expect("snapshot must remain committed")
            .runtime_versions
            .tools_drive_version
            .is_empty());
    }

    #[test]
    fn snapshot_volume_is_logical() {
        let mut record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        record
            .committed
            .as_mut()
            .unwrap()
            .volume_snapshots
            .push(SnapshotVolume {
                mount_path: "/mnt/data".to_string(),
                mode: VolumeMode::ReadOnly,
                size_mb: 1024,
                layers: vec![OverlaybdLayerRef::Managed(ManagedLayer {
                    digest: "sha256:abc".to_string(),
                    size: 123,
                    uuid: None,
                })],
            });

        let value = serde_json::to_value(&record).expect("serialize snapshot record");
        assert_eq!(
            value["committed"]["volume_snapshots"][0]["layers"][0]["Managed"]["digest"],
            "sha256:abc"
        );
        assert_eq!(
            value["committed"]["volume_snapshots"][0]["mode"],
            "ReadOnly"
        );
        assert!(!value.to_string().contains("snapshot_dir"));
        let decoded: SnapshotRecord =
            serde_json::from_value(value.clone()).expect("deserialize record");
        assert_eq!(
            decoded.committed.as_ref().unwrap().volume_snapshots[0].mount_path,
            "/mnt/data"
        );
        assert_eq!(
            decoded.committed.unwrap().volume_snapshots[0].mode,
            VolumeMode::ReadOnly
        );

        let mut legacy = value;
        legacy["committed"]["volume_snapshots"][0]
            .as_object_mut()
            .expect("volume snapshot must be an object")
            .remove("mode");
        let decoded: SnapshotRecord =
            serde_json::from_value(legacy).expect("deserialize legacy record");
        assert_eq!(
            decoded.committed.unwrap().volume_snapshots[0].mode,
            VolumeMode::Exclusive
        );
    }

    #[test]
    fn legacy_snapshot_records_are_distributed() {
        let record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let mut value = serde_json::to_value(record).unwrap();
        value.as_object_mut().unwrap().remove("snapshot_type");
        let record: SnapshotRecord = serde_json::from_value(value).unwrap();
        assert_eq!(record.snapshot_type, SnapshotPlacement::Distributed);
    }

    #[test]
    fn local_snapshots_reject_aliases() {
        let mut metadata = SnapshotPublishMetadata {
            snapshot_type: SnapshotPlacement::Local,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sandbox-a".to_string(),
                source_sandbox_metadata: None,
            },
            ..SnapshotPublishMetadata::mock()
        };
        assert!(metadata.validate().is_ok());
        metadata.alias = Some(SnapshotAlias::parse("checkpoint").unwrap());
        assert!(metadata.validate().is_err());
    }

    #[test]
    fn managed_layer_uuid_serde_is_backward_compatible() {
        let legacy = r#"{"digest":"sha256:abc","size":123}"#;
        let layer: ManagedLayer = serde_json::from_str(legacy).expect("parse legacy layer");
        assert_eq!(layer.uuid, None);

        let with_uuid =
            r#"{"digest":"sha256:abc","size":123,"uuid":"11111111-2222-3333-4444-555555555555"}"#;
        let layer: ManagedLayer = serde_json::from_str(with_uuid).expect("parse uuid layer");
        assert_eq!(
            layer.uuid.as_deref(),
            Some("11111111-2222-3333-4444-555555555555")
        );
        let value = serde_json::to_value(&layer).expect("serialize uuid layer");
        assert_eq!(value["uuid"], "11111111-2222-3333-4444-555555555555");
    }

    #[test]
    fn returns_published_rootfs_image_ref_by_exact_tag() {
        let mut record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let rootfs_tag = rootfs_snapshot_image_tag(&record.id);
        let expected = format!("registry.example/ns/app:{rootfs_tag}");
        record.committed.as_mut().unwrap().disk_publications = vec![
            publication(
                "registry.example/ns/app:drive",
                format!("{rootfs_tag}-drive-data-0123456789ab"),
            ),
            publication(expected.clone(), rootfs_tag),
        ];

        assert_eq!(record.published_rootfs_image_ref(), Some(expected.as_str()));
    }

    #[test]
    fn effective_start_cmd_combines_entrypoint_and_cmd() {
        let ctx = CommandContext::default()
            .with_entrypoint(Some(vec!["/docker-entrypoint.sh".to_string()]))
            .with_cmd(Some(vec![
                "nginx".to_string(),
                "-g".to_string(),
                "daemon off;".to_string(),
            ]));
        assert_eq!(
            ctx.effective_start_cmd().as_deref(),
            Some("/docker-entrypoint.sh nginx -g 'daemon off;'"),
        );
    }

    #[test]
    fn effective_start_cmd_entrypoint_only() {
        let ctx = CommandContext::default().with_entrypoint(Some(vec!["node".to_string()]));
        assert_eq!(ctx.effective_start_cmd().as_deref(), Some("node"));
    }

    #[test]
    fn effective_start_cmd_cmd_only() {
        let ctx = CommandContext::default()
            .with_cmd(Some(vec!["python3".to_string(), "app.py".to_string()]));
        assert_eq!(ctx.effective_start_cmd().as_deref(), Some("python3 app.py"),);
    }

    #[test]
    fn effective_start_cmd_absent_returns_none() {
        assert_eq!(CommandContext::default().effective_start_cmd(), None);
    }

    #[test]
    fn startup_shell_is_backward_compatible() {
        let legacy = serde_json::json!({"start_cmd": "true", "ready_cmd": "true", "context": CommandContext::default()});
        let mut startup: super::StartupCommand = serde_json::from_value(legacy).unwrap();
        assert_eq!(startup.shell_command(), ("/bin/bash", "-lc"));
        assert!(serde_json::to_value(&startup)
            .unwrap()
            .get("shell")
            .is_none());
        startup.shell = Some("/bin/sh".to_owned());
        let restored: super::StartupCommand =
            serde_json::from_value(serde_json::to_value(&startup).unwrap()).unwrap();
        assert_eq!(restored.shell_command(), ("/bin/sh", "-c"));
    }

    #[test]
    fn legacy_snapshot_startup_roundtrips_without_format_changes() {
        let mut legacy =
            serde_json::to_value(SnapshotRecord::mock_ready(CommittedSnapshot::mock())).unwrap();
        legacy["committed"]["startup"] = serde_json::json!({
            "start_cmd": "[[ -n $HOME ]] && exec /app/server",
            "ready_cmd": "test -f /app/ready",
            "context": CommandContext::new(
                HashMap::from([("HOME".into(), "/app".into())]),
                "/app",
            ),
        });
        let record: SnapshotRecord = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(
            record
                .committed
                .as_ref()
                .unwrap()
                .startup
                .as_ref()
                .unwrap()
                .shell_command(),
            ("/bin/bash", "-lc"),
        );
        assert_eq!(serde_json::to_value(record).unwrap(), legacy);
    }

    #[test]
    fn effective_start_cmd_empty_vecs_return_none() {
        let ctx = CommandContext::default()
            .with_entrypoint(Some(vec![]))
            .with_cmd(Some(vec![]));
        assert_eq!(ctx.effective_start_cmd(), None);
    }

    #[test]
    fn template_build_error_reason_reads_legacy_string() {
        let reason: TemplateBuildErrorReason =
            serde_json::from_str(r#""legacy failure""#).expect("deserialize legacy reason");

        assert_eq!(reason.message, "legacy failure");
        assert_eq!(reason.step, None);
    }

    #[test]
    fn template_build_error_reason_reads_structured_reason() {
        let reason: TemplateBuildErrorReason =
            serde_json::from_str(r#"{"message":"boom","step":"resolve image"}"#)
                .expect("deserialize structured reason");

        assert_eq!(reason.message, "boom");
        assert_eq!(reason.step.as_deref(), Some("resolve image"));
    }
}
