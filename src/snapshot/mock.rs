use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::backends::build_snapshot_storage_from_config;
use super::{
    MetadataCatalog, SnapshotHandle, SnapshotManager, SnapshotRecord, SnapshotRuntimeManifest,
    SnapshotRuntimeResolver, SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::metadata::RepositoryGcStorage;
use crate::repository::{Repository, RepositoryError, RepositoryResult};

pub fn snapshot_config_for_test(source: super::SnapshotSource) -> super::SnapshotConfig {
    crate::snapshot::SnapshotConfig {
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
    async fn resolve(
        &self,
        _snapshot: Arc<SnapshotRecord>,
    ) -> RepositoryResult<Arc<SnapshotHandle>> {
        Err(Self::unsupported())
    }
}

/// Opens a SQLite-backed [`MetadataCatalog`] under `root` paired with `repository`.
pub async fn test_metadata_catalog(
    root: &Path,
    repository: Arc<dyn Repository>,
) -> (agentenv_metadata::MetaServer, Arc<MetadataCatalog>) {
    let db = agentenv_metadata::DatabaseConfig::Sqlite {
        path: root.join("metadata.sqlite3"),
    };
    db.migrate().await.expect("migrate test metadata db");
    let server = agentenv_metadata::MetaServer::open(&db)
        .await
        .expect("open test metadata server")
        .with_gc_storage(Arc::new(RepositoryGcStorage(Arc::clone(&repository))));
    let meta = agentenv_metadata::transport::embedded(server.clone());
    let catalog = Arc::new(MetadataCatalog::new(meta, repository));
    (server, catalog)
}

/// Opens a POSIX durable repository and node-local runtime resolver for tests.
pub fn test_posix_storage(
    store_root: &Path,
    local_root: PathBuf,
) -> Result<(Arc<dyn Repository>, Arc<dyn SnapshotRuntimeResolver>)> {
    let mut config = crate::cfg::ConfigManager::global_config().clone();
    config.snapshot.repository_backend = crate::cfg::SnapshotRepositoryBackendKind::PosixFs;
    config.snapshot.local_root = local_root;
    config.backend.posix_fs = Some(crate::cfg::PosixFsBackendConfig {
        snapshot_store: store_root.to_path_buf(),
    });
    build_snapshot_storage_from_config(&config, None)
}

/// Opens a POSIX + SQLite [`SnapshotManager`] under `root` for tests.
pub async fn test_snapshot_manager(
    root: &Path,
) -> (agentenv_metadata::MetaServer, SnapshotManager) {
    let (repository, resolver) =
        test_posix_storage(root, root.to_path_buf()).expect("posix storage");
    let (server, catalog) = test_metadata_catalog(root, Arc::clone(&repository)).await;
    let manager = SnapshotManager::from_parts(catalog, repository, resolver, None);
    (server, manager)
}

/// Writes a minimal exported snapshot artifact set for snapshot publish/resolve tests.
pub fn write_mock_built_artifacts(
    root: &Path,
) -> Result<(PathBuf, PathBuf, SnapshotRuntimeManifest)> {
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

    let mut manifest = SnapshotRuntimeManifest::new(
        crate::snapshot::FIRECRACKER_BACKEND,
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config),
        0,
        root.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_image_config),
        32768,
        &[],
    )?;
    manifest.memory.layers = vec![crate::layer::LayerSource::File(
        overlaybd::layer::LayerFile::dense(
            memory_lower.clone(),
            uuid::Uuid::now_v7(),
            Some(overlaybd::LayerDescriptor {
                digest: memory_descriptor.sha256,
                size: memory_descriptor.size,
            }),
        ),
    )]
    .into();
    manifest.rootfs.layers = vec![crate::layer::LayerSource::File(
        overlaybd::layer::LayerFile::dense(
            rootfs_lower.clone(),
            uuid::Uuid::now_v7(),
            Some(overlaybd::LayerDescriptor {
                digest: rootfs_descriptor.sha256,
                size: rootfs_descriptor.size,
            }),
        ),
    )]
    .into();

    Ok((rootfs_lower, memory_lower, manifest))
}
