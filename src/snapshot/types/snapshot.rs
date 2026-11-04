use super::drive::ResolvedAttachedDrive;
use crate::snapshot::SnapshotRuntimeManifest;
use crate::types::SnapshotId;
#[cfg(test)]
use crate::types::{ManagedLayer, OverlaybdLayerRef};
use std::fmt;
use std::sync::Arc;

pub use agentenv_domain::{
    rootfs_snapshot_image_tag, CommandContext, PersistedDiskImagePublication, SnapshotConfig,
    SnapshotManifest, SnapshotPlacement, SnapshotPublishMetadata, SnapshotPublishSource,
    SnapshotRecord, SnapshotSource, SnapshotSourceKind, SnapshotVolume, StartupCommand,
    TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};

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
}

impl fmt::Debug for SnapshotHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotHandle")
            .field("id", &self.id)
            .field("manifest", &self.runtime_manifest)
            .finish_non_exhaustive()
    }
}

pub type RunnableSnapshot = SnapshotRecord<Arc<SnapshotHandle>>;

impl SnapshotHandle {
    pub fn resolved_attached_drives(&self) -> Vec<ResolvedAttachedDrive> {
        self.runtime_manifest
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
    #[cfg(test)]
    pub fn mock_record() -> RunnableSnapshot {
        Self::record_from_test_manifest(
            SnapshotRecord::mock_ready(SnapshotManifest::mock()),
            Vec::new(),
        )
    }
    #[cfg(test)]
    pub(crate) fn record_from_test_manifest(
        record: SnapshotRecord,
        drives: Vec<ResolvedAttachedDrive>,
    ) -> RunnableSnapshot {
        let drives: Vec<_> = drives
            .iter()
            .map(ResolvedAttachedDrive::to_extra_drive)
            .collect();
        let handle = Arc::new(Self::new(
            record.id.clone(),
            SnapshotRuntimeManifest::for_test(0, &drives),
            (),
        ));
        record.with_manifest(handle)
    }
    #[cfg(test)]
    pub(crate) fn record_from_test_legacy_manifest(
        record: SnapshotRecord,
        drives: Vec<ResolvedAttachedDrive>,
    ) -> RunnableSnapshot {
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
        drives: Vec<ResolvedAttachedDrive>,
    ) -> RunnableSnapshot {
        let mut runnable = Self::record_from_test_manifest(record, drives);
        Arc::get_mut(&mut runnable.manifest)
            .unwrap()
            .runtime_manifest
            .physical_extra_drive_count = 0;
        runnable
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{SnapshotAlias, VolumeMode};

    use super::{
        rootfs_snapshot_image_tag, CommandContext, ManagedLayer, OverlaybdLayerRef,
        PersistedDiskImagePublication, SnapshotManifest, SnapshotPlacement,
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
        let snapshot = std::sync::Arc::new(super::SnapshotHandle::new(
            super::SnapshotId::generate(),
            crate::snapshot::SnapshotRuntimeManifest::for_test(4096, &[]),
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

    fn legacy_record_value() -> serde_json::Value {
        serde_json::json!({
            "id": super::SnapshotId::generate(), "alias": null,
            "source": {"Sandbox": {"source_sandbox_id": "legacy-source", "source_sandbox_metadata": {"team": "compat"}}},
            "resources": {"cpu_count": 2, "memory_mib": 512, "disk_size_mib": 1024},
            "created_at_unix_ms": 10, "updated_at_unix_ms": 20,
            "committed": {
                "context": {"env_vars": {"LEGACY": "1"}, "workdir": "/legacy"},
                "startup": null,
                "runtime_versions": {"kernel_version": "kernel", "firecracker_version": "firecracker", "envd_version": "envd", "tools_drive_version": "0.1.0"},
                "rootfs_layers": [], "memory_layers": [], "attached_drives": [],
                "volume_snapshots": [{"mountPath": "/data", "sizeMb": 16, "layers": []}]
            }
        })
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
        let mut value = legacy_record_value();
        value["committed"]["runtime_versions"]
            .as_object_mut()
            .expect("runtime versions must be an object")
            .remove("tools_drive_version");

        let record: SnapshotRecord =
            serde_json::from_value(value).expect("deserialize legacy snapshot record");

        assert!(record
            .config
            .runtime_versions
            .tools_drive_version
            .is_empty());
    }

    #[test]
    fn snapshot_volume_is_logical() {
        let mut record = SnapshotRecord::mock_ready(SnapshotManifest::mock());
        record.config.volume_snapshots.push(SnapshotVolume {
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
            value["config"]["volume_snapshots"][0]["layers"][0]["Managed"]["digest"],
            "sha256:abc"
        );
        assert_eq!(value["config"]["volume_snapshots"][0]["mode"], "ReadOnly");
        assert!(!value.to_string().contains("snapshot_dir"));
        let decoded: SnapshotRecord =
            serde_json::from_value(value.clone()).expect("deserialize record");
        assert_eq!(decoded.config.volume_snapshots[0].mount_path, "/mnt/data");
        assert_eq!(
            decoded.config.volume_snapshots[0].mode,
            VolumeMode::ReadOnly
        );

        let mut legacy = value;
        legacy["config"]["volume_snapshots"][0]
            .as_object_mut()
            .expect("volume snapshot must be an object")
            .remove("mode");
        let decoded: SnapshotRecord =
            serde_json::from_value(legacy).expect("deserialize legacy record");
        assert_eq!(
            decoded.config.volume_snapshots[0].mode,
            VolumeMode::Exclusive
        );
    }

    #[test]
    fn legacy_snapshot_records_are_distributed() {
        let legacy = legacy_record_value();
        let record: SnapshotRecord = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(record.snapshot_type, SnapshotPlacement::Distributed);
        assert_eq!(record.config.context.workdir, "/legacy");
        assert_eq!(record.config.volume_snapshots[0].mount_path, "/data");
        assert!(record.manifest.is_some());
        let mut local = legacy.clone();
        local["snapshot_type"] = "local".into();
        local["owner_node_id"] = "legacy-owner".into();
        let local: SnapshotRecord = serde_json::from_value(local).unwrap();
        local.validate().unwrap();
        assert!(local.manifest.is_none());
        assert_eq!(
            local.config.volume_snapshots,
            record.config.volume_snapshots
        );
        assert_eq!(local.config.context, record.config.context);
        let mut pending = legacy;
        pending["source"] = serde_json::json!({"Template": {"build": {"status": "Waiting", "started_at_unix_ms": null, "finished_at_unix_ms": null, "error_reason": null}}});
        pending["committed"] = serde_json::Value::Null;
        let pending: SnapshotRecord = serde_json::from_value(pending).unwrap();
        assert!(pending.manifest.is_none());
        assert!(
            matches!(pending.config.source, SnapshotSource::Template { build } if build.status == super::TemplateBuildStatus::Waiting)
        );
    }

    #[test]
    fn local_snapshots_reject_aliases() {
        let mut metadata = SnapshotPublishMetadata {
            snapshot_type: SnapshotPlacement::Local,
            config: crate::snapshot::SnapshotConfig {
                source: SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox-a".to_string(),
                    source_sandbox_metadata: None,
                },
                ..crate::snapshot::SnapshotConfig::mock()
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
        let mut record = SnapshotRecord::mock_ready(SnapshotManifest::mock());
        let rootfs_tag = rootfs_snapshot_image_tag(&record.id);
        let expected = format!("registry.example/ns/app:{rootfs_tag}");
        record.manifest.as_mut().unwrap().disk_publications = vec![
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
    fn legacy_snapshot_startup_preserves_captured_configuration() {
        let mut legacy = legacy_record_value();
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
            record.config.startup.as_ref().unwrap().shell_command(),
            ("/bin/bash", "-lc"),
        );
        let upgraded = serde_json::to_value(&record).unwrap();
        assert_eq!(
            upgraded["config"]["startup"],
            legacy["committed"]["startup"]
        );
        assert_eq!(upgraded["config"]["source"], legacy["source"]);
        assert_eq!(upgraded["config"]["resources"], legacy["resources"]);
        assert_eq!(upgraded["created_at_unix_ms"], legacy["created_at_unix_ms"]);
        assert!(upgraded.get("committed").is_none());
        let reread: SnapshotRecord = serde_json::from_value(upgraded.clone()).unwrap();
        assert_eq!(serde_json::to_value(reread).unwrap(), upgraded);
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
