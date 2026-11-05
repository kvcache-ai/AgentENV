use super::local::{CapturedImage, SnapshotCapture};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::SnapshotRuntimeResolver;
use super::{RunnableSnapshot, SnapshotManager, SnapshotRecord, SNAPSHOT_ARTIFACT_LAYOUT};
use crate::repository::{Repository, RepositoryError, RepositoryResult};
use crate::snapshot::SandboxSnapshotManifest;

pub fn publish_metadata_for_test(
    id: super::SnapshotId,
    alias: Option<super::SnapshotAlias>,
    source: super::SnapshotPublishSource,
) -> super::SnapshotPublishMetadata {
    super::SnapshotPublishMetadata {
        id,
        snapshot_type: super::SnapshotPlacement::Distributed,
        alias,
        source,
        context: super::CommandContext::default(),
        startup: None,
        resources: crate::types::SandboxResources::default(),
        runtime_versions: super::SnapshotRuntimeVersions {
            kernel_version: "kernel".to_string(),
            firecracker_version: "firecracker".to_string(),
            envd_version: "envd".to_string(),
            tools_drive_version: crate::cfg::ConfigManager::global_config()
                .resolved_tools_version()
                .to_string(),
        },
        virtualization_mode: crate::cfg::ConfigManager::global_config().virtualization_mode,
        image_configs: crate::types::ImageConfigs::new(),
        volume_snapshots: Vec::new(),
        custom_extension_params: None,
    }
}

/// Test double for repository interactions that should stay unreachable:
/// every operation reports itself unsupported.
#[derive(Clone, Debug, Default)]
pub struct MockRepository;

impl Repository for MockRepository {}

/// Test double for runtime resolution that should stay unreachable.
#[derive(Clone, Debug, Default)]
pub struct MockSnapshotRuntimeResolver;

impl MockSnapshotRuntimeResolver {
    fn unsupported() -> RepositoryError {
        RepositoryError::Unsupported {
            feature: "mock snapshot runtime resolver should not be called in this test".to_string(),
        }
    }
}

#[async_trait]
impl SnapshotRuntimeResolver for MockSnapshotRuntimeResolver {
    async fn resolve(&self, _snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot> {
        Err(Self::unsupported())
    }
}

/// Builds a snapshot manager backed by snapshot test doubles.
pub fn mock_snapshot_manager() -> SnapshotManager {
    SnapshotManager::from_parts(
        Arc::new(MockRepository),
        Arc::new(MockSnapshotRuntimeResolver),
        None,
    )
}

/// Writes a minimal exported snapshot artifact set for snapshot publish/resolve tests.
pub fn write_mock_built_artifacts(
    root: &Path,
) -> Result<(PathBuf, PathBuf, SandboxSnapshotManifest)> {
    let (rootfs_lower, memory_lower, capture) = write_mock_capture(root)?;
    Ok((rootfs_lower, memory_lower, capture.finish(root)?))
}

pub(super) fn write_mock_capture(root: &Path) -> Result<(PathBuf, PathBuf, SnapshotCapture)> {
    use crate::layer::{LayerChain, LayerSource};
    use overlaybd::layer::LayerFile;

    std::fs::create_dir_all(root.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_dir))?;
    let vm_state_path = root.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
    std::fs::write(&vm_state_path, b"vm state")?;
    let image = |file: &str, contents: &[u8], config: &str, virtual_size| -> Result<_> {
        let path = root.join(file);
        std::fs::write(&path, contents)?;
        let descriptor = crate::digest::FileDigest::describe_blocking(&path)?;
        let layers = LayerChain::from(vec![LayerSource::File(LayerFile::dense(
            path.clone(),
            uuid::Uuid::nil(),
            Some(overlaybd::LayerDescriptor {
                digest: descriptor.sha256,
                size: descriptor.size,
            }),
        ))]);
        Ok((
            path,
            CapturedImage {
                image_config_path: root.join(config),
                config: layers.to_runtime_config(),
                layers,
                virtual_size,
            },
        ))
    };
    let (rootfs_lower, rootfs) = image(
        "base.overlaybd.commit",
        b"base-layer",
        SNAPSHOT_ARTIFACT_LAYOUT.rootfs_image_config,
        32768,
    )?;
    let (memory_lower, memory) = image(
        "mem.overlaybd.commit",
        b"mem-layer",
        SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config,
        0,
    )?;
    Ok((
        rootfs_lower,
        memory_lower,
        SnapshotCapture {
            backend: crate::snapshot::FIRECRACKER_BACKEND,
            volume_drive_slots: 0,
            physical_extra_drive_count: 0,
            vm_state_path,
            memory,
            rootfs,
            attached_drives: Vec::new(),
        },
    ))
}
