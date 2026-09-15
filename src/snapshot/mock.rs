use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::repository::{
    RepositoryError, RepositoryResult, SnapshotListFilter, SnapshotRepository,
    SnapshotRuntimeResolver,
};
use super::{
    RunnableSnapshot, SnapshotId, SnapshotManager, SnapshotPublishMetadata, SnapshotRecord,
    SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::sandbox::SandboxSnapshotManifest;

/// Test double for snapshot repository interactions that should stay unreachable.
#[derive(Clone, Debug, Default)]
pub struct MockSnapshotRepository;

impl MockSnapshotRepository {
    fn unsupported() -> RepositoryError {
        RepositoryError::Unsupported {
            feature: "mock snapshot repository should not be called in this test".to_string(),
        }
    }
}

#[async_trait]
impl SnapshotRepository for MockSnapshotRepository {
    async fn create(&self, _record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        Err(Self::unsupported())
    }

    async fn publish(
        &self,
        _metadata: SnapshotPublishMetadata,
        _manifest: SandboxSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        Err(Self::unsupported())
    }

    async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Err(Self::unsupported())
    }

    async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        Err(Self::unsupported())
    }

    async fn delete(&self, _id_or_alias: &str) -> RepositoryResult<()> {
        Err(Self::unsupported())
    }

    async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        Err(Self::unsupported())
    }

    async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        Err(Self::unsupported())
    }

    async fn mark_build_error(
        &self,
        _id: &SnapshotId,
        _reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        Err(Self::unsupported())
    }
}

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
        Arc::new(MockSnapshotRepository),
        Arc::new(MockSnapshotRuntimeResolver),
        None,
    )
}

/// Writes a minimal exported snapshot artifact set for snapshot publish/resolve tests.
pub fn write_mock_built_artifacts(
    root: &Path,
) -> Result<(PathBuf, PathBuf, SandboxSnapshotManifest)> {
    std::fs::create_dir_all(root.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_dir))?;

    let rootfs_lower = root.join("base.overlaybd.commit");
    let memory_lower = root.join("mem.overlaybd.commit");

    std::fs::write(&rootfs_lower, b"base-layer")?;
    std::fs::write(&memory_lower, b"mem-layer")?;
    let rootfs_descriptor = crate::digest::FileDigest::describe_blocking(&rootfs_lower)?;
    let memory_descriptor = crate::digest::FileDigest::describe_blocking(&memory_lower)?;
    std::fs::write(root.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state), b"vm state")?;
    std::fs::write(
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_image_config),
        format!(
            r#"{{
  "repoBlobUrl": "",
  "lowers": [{{ "file": "{}", "digest": "{}", "size": {} }}],
  "upper": {{}},
  "resultFile": ""
}}"#,
            rootfs_lower.display(),
            rootfs_descriptor.sha256,
            rootfs_descriptor.size
        ),
    )?;
    std::fs::write(
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config),
        format!(
            r#"{{"lowers":[{{"file":"{}","digest":"{}","size":{}}}]}}"#,
            memory_lower.display(),
            memory_descriptor.sha256,
            memory_descriptor.size
        ),
    )?;

    let manifest = SandboxSnapshotManifest::new(
        crate::sandbox::FIRECRACKER_BACKEND,
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config),
        0,
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_image_config),
        32768,
        &Vec::new(),
    )?;

    Ok((rootfs_lower, memory_lower, manifest))
}
