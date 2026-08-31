use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::task;

use super::super::shared_runtime_cache_root;
use super::artifacts::{CollectedBuiltArtifacts, PosixFsArtifactStore};
use super::catalog::PosixFsCatalogStore;
use super::runtime::PosixFsRuntimeResolver;
use crate::image::cache::{local_image_services_from_global_config, OverlaybdLayerStore};
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
use crate::snapshot::repository::{RepositoryError, RepositoryResult, SnapshotListFilter};
use crate::snapshot::types::{
    CommittedSnapshot, SnapshotId, SnapshotPublishMetadata, SnapshotRecord,
};

#[derive(Clone, Debug)]
pub struct PosixFsBackendConfig {
    /// Durable repository root. This may live on a shared POSIX or distributed filesystem.
    pub root: std::path::PathBuf,
    /// Optional shared node-local cache root for downloaded/materialized runtime artifacts.
    ///
    /// When `None`, a stable node-local default under the process temp directory is used.
    pub cache_root: Option<std::path::PathBuf>,
    /// Optional node-local cache root for runtime-materialized files such as runnable image configs.
    ///
    /// When `None`, defaults to `<cache_root>/runtime`.
    pub runtime_cache_root: Option<std::path::PathBuf>,
}

pub struct PosixFsBackend {
    repository: Arc<dyn SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
}

impl PosixFsBackend {
    /// Builds the POSIX backend bundle: durable repository state plus node-local runtime resolver.
    pub fn new(config: PosixFsBackendConfig) -> Result<Self> {
        let cache_root = config
            .cache_root
            .clone()
            .unwrap_or_else(shared_runtime_cache_root);
        let cache = LocalArtifactCache::new(cache_root, None)?;
        Ok(Self::from_parts(
            config,
            local_image_services_from_global_config().overlaybd_layers,
            cache,
        ))
    }

    /// Builds the POSIX backend bundle using a shared node-local artifact cache.
    ///
    /// `runtime_image_hints` is a node-local runtime-acceleration hint (e.g. the
    /// overlaybd commit-store path) consumed only by the runtime resolver. It is
    /// passed here rather than carried in [`PosixFsBackendConfig`] so the durable
    /// backend config stays free of runtime-only concerns.
    pub(crate) fn from_parts(
        config: PosixFsBackendConfig,
        store: Arc<dyn OverlaybdLayerStore>,
        cache: Arc<LocalArtifactCache>,
    ) -> Self {
        let PosixFsBackendConfig {
            root,
            cache_root,
            runtime_cache_root,
        } = config;
        let cache_root = cache_root.unwrap_or_else(shared_runtime_cache_root);
        let runtime_cache_root = runtime_cache_root.unwrap_or_else(|| cache_root.join("runtime"));
        let catalog_store = Arc::new(PosixFsCatalogStore::new(root.clone()));
        let artifact_store = Arc::new(PosixFsArtifactStore::new(root.clone()));
        let repository: Arc<dyn SnapshotRepository> = Arc::new(PosixFsSnapshotRepository::new(
            catalog_store,
            artifact_store,
        ));
        let runtime_resolver: Arc<dyn SnapshotRuntimeResolver> = Arc::new(
            PosixFsRuntimeResolver::new(root, runtime_cache_root, store, cache),
        );

        Self {
            repository,
            runtime_resolver,
        }
    }

    /// Returns the committed-state repository for publish/get/list/delete operations.
    pub fn repository(&self) -> Arc<dyn SnapshotRepository> {
        Arc::clone(&self.repository)
    }

    /// Returns the node-local runtime resolver used to materialize runnable paths.
    pub fn runtime_resolver(&self) -> Arc<dyn SnapshotRuntimeResolver> {
        Arc::clone(&self.runtime_resolver)
    }

    /// Splits the backend into its repository and runtime-resolution components.
    pub fn into_parts(
        self,
    ) -> (
        Arc<dyn SnapshotRepository>,
        Arc<dyn SnapshotRuntimeResolver>,
    ) {
        (self.repository, self.runtime_resolver)
    }
}

#[derive(Clone)]
pub(crate) struct PosixFsSnapshotRepository {
    catalog_store: Arc<PosixFsCatalogStore>,
    artifact_store: Arc<PosixFsArtifactStore>,
}

impl PosixFsSnapshotRepository {
    pub(crate) fn new(
        catalog_store: Arc<PosixFsCatalogStore>,
        artifact_store: Arc<PosixFsArtifactStore>,
    ) -> Self {
        Self {
            catalog_store,
            artifact_store,
        }
    }

    fn committed_snapshot(
        metadata: &SnapshotPublishMetadata,
        built: CollectedBuiltArtifacts,
    ) -> CommittedSnapshot {
        CommittedSnapshot {
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            runtime_versions: metadata.runtime_versions.clone(),
            virtualization_mode: metadata.virtualization_mode,
            image_configs: metadata.image_configs.clone(),
            custom_extension_params: metadata.custom_extension_params.clone(),
            rootfs_layers: built.rootfs_layers,
            attached_drives: built.attached_drives,
            memory_layers: built.memory_layers,
            disk_publications: Vec::new(),
        }
    }

    fn create_sync(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.catalog_store.create(record)
    }

    fn publish_sync(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        let mut drive_ids = HashSet::new();
        for drive in &manifest.attached_drives {
            if !drive_ids.insert(drive.drive_id.clone()) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "duplicate attached drive id in publish request: {}",
                        drive.drive_id
                    ),
                });
            }
            if drive.virtual_size == 0 {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "attached drive '{}' virtual_size must be non-zero",
                        drive.drive_id
                    ),
                });
            }
        }

        let session = self.catalog_store.begin_publish(&metadata.id)?;
        let built = match self
            .artifact_store
            .import_built_artifacts(&metadata.id, &manifest)
        {
            Ok(built) => built,
            Err(error) => {
                let _ = self.catalog_store.abort_publish(&session);
                return Err(error);
            }
        };

        let committed = Self::committed_snapshot(&metadata, built);
        match self
            .catalog_store
            .commit_publish(&session, metadata, committed)
        {
            Ok(stored) => Ok(stored),
            Err(error) => {
                let _ = self.catalog_store.abort_publish(&session);
                Err(error)
            }
        }
    }

    fn get_sync(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.catalog_store.get(id_or_alias)
    }

    fn list_sync(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.catalog_store.list(filter)
    }

    fn delete_sync(&self, id_or_alias: &str) -> RepositoryResult<()> {
        let Some(record) = self.catalog_store.get(id_or_alias)? else {
            return Ok(());
        };
        self.catalog_store.delete_record(&record.id)
    }

    fn resolve_alias_sync(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        self.catalog_store.resolve_alias(alias)
    }

    fn try_start_sync(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        self.catalog_store.try_start(id)
    }

    fn mark_error_sync(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        self.catalog_store.mark_error(id, reason)
    }
}

#[async_trait]
impl SnapshotRepository for PosixFsSnapshotRepository {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        let repository = self.clone();
        run_repository_blocking("create snapshot record", move || {
            repository.create_sync(record)
        })
        .await
    }

    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        let repository = self.clone();
        run_repository_blocking("publish snapshot", move || {
            repository.publish_sync(metadata, manifest)
        })
        .await
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        let repository = self.clone();
        let id_or_alias = id_or_alias.to_string();
        run_repository_blocking("load snapshot", move || repository.get_sync(&id_or_alias)).await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let repository = self.clone();
        run_repository_blocking("list snapshots", move || repository.list_sync(filter)).await
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        let repository = self.clone();
        let id_or_alias = id_or_alias.to_string();
        run_repository_blocking("delete snapshot", move || {
            repository.delete_sync(&id_or_alias)
        })
        .await
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let repository = self.clone();
        let alias = alias.to_string();
        run_repository_blocking("resolve snapshot alias", move || {
            repository.resolve_alias_sync(&alias)
        })
        .await
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let repository = self.clone();
        let id = id.clone();
        run_repository_blocking("start template build", move || {
            repository.try_start_sync(&id)
        })
        .await
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let repository = self.clone();
        let id = id.clone();
        run_repository_blocking("mark template build error", move || {
            repository.mark_error_sync(&id, reason)
        })
        .await
    }
}

async fn run_repository_blocking<T, F>(operation: &'static str, work: F) -> RepositoryResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> RepositoryResult<T> + Send + 'static,
{
    task::spawn_blocking(work)
        .await
        .map_err(|error| RepositoryError::Backend {
            message: format!("repository blocking task panicked while trying to {operation}"),
            source: Some(anyhow::Error::from(error)),
        })?
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;

    use overlaybd::config::ImageConfig as OverlaybdImageConfig;
    use tempfile::TempDir;

    use super::super::runtime::PosixFsRuntimeResolver;
    use super::{PosixFsBackend, PosixFsBackendConfig, PosixFsSnapshotRepository};
    use crate::image::cache::{OverlaybdLayerLocation, OverlaybdLayerStore};
    use crate::sandbox::{ExtraDrive, FirecrackerSnapshotManifest};
    use crate::snapshot::artifact_cache::LocalArtifactCache;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::repository::{
        RepositoryError, SnapshotRepository, SnapshotRuntimeResolver,
    };
    use crate::snapshot::{
        CommittedSnapshot, ManagedLayer, OverlaybdLayerRef, SnapshotAlias, SnapshotId,
        SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
        TemplateBuildErrorReason, SNAPSHOT_ARTIFACT_LAYOUT,
    };

    use super::super::artifacts::PosixFsArtifactStore;
    use super::super::catalog::PosixFsCatalogStore;

    #[derive(Debug)]
    struct TestOverlaybdLayerStore;

    impl OverlaybdLayerStore for TestOverlaybdLayerStore {
        fn layer_location(&self, _: &str, _: u64, _: bool) -> OverlaybdLayerLocation {
            OverlaybdLayerLocation::CacheDir("test-image-cache/commits".into())
        }

        fn publishable_roots(&self) -> Vec<std::path::PathBuf> {
            Vec::new()
        }
    }

    fn test_overlaybd_layer_store() -> Arc<dyn OverlaybdLayerStore> {
        Arc::new(TestOverlaybdLayerStore)
    }

    fn sample_metadata(id: SnapshotId, alias: Option<&str>) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id,
            alias: alias.map(|value| SnapshotAlias::parse(value).expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        }
    }

    fn ready_record(
        metadata: SnapshotPublishMetadata,
        committed: CommittedSnapshot,
    ) -> SnapshotRecord {
        let mut record = SnapshotRecord::mock_ready(committed);
        record.id = metadata.id;
        record.alias = metadata.alias;
        record.resources = metadata.resources;
        let source = match metadata.source {
            SnapshotPublishSource::Template => record.source,
            SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                SnapshotSource::Sandbox { source_sandbox_id }
            }
        };
        record.source = source;
        record
    }

    fn test_backend(root: &Path) -> PosixFsBackend {
        let config = PosixFsBackendConfig {
            root: root.to_path_buf(),
            cache_root: Some(root.join("runtime-cache")),
            runtime_cache_root: Some(root.join("runtime-cache").join("runtime")),
        };
        let cache = LocalArtifactCache::new(root.join("runtime-cache"), None)
            .expect("local artifact cache");
        PosixFsBackend::from_parts(config, test_overlaybd_layer_store(), cache)
    }

    fn test_repository(root: &Path) -> PosixFsSnapshotRepository {
        PosixFsSnapshotRepository::new(
            Arc::new(PosixFsCatalogStore::new(root.to_path_buf())),
            Arc::new(PosixFsArtifactStore::new(root.to_path_buf())),
        )
    }

    fn seed_built_snapshot(root: &Path) -> FirecrackerSnapshotManifest {
        let local_root = root.join("local").join(uuid::Uuid::now_v7().to_string());
        let (_, _, manifest) =
            write_mock_built_artifacts(&local_root).expect("mock built artifacts should write");
        manifest
    }

    fn seed_committed_firecracker_manifest(
        repository_root: &Path,
        snapshot_id: &SnapshotId,
        memory_virtual_size: u64,
        rootfs_virtual_size: u64,
    ) {
        let snapshot_dir = repository_root
            .join("snapshots")
            .join(snapshot_id.to_string());
        fs::create_dir_all(&snapshot_dir).expect("snapshot dir");
        let manifest = FirecrackerSnapshotManifest::new(
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config),
            memory_virtual_size,
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_image_config),
            rootfs_virtual_size,
            &[],
        )
        .expect("seed manifest should be valid");
        fs::write(
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest),
            serde_json::to_vec_pretty(&manifest).expect("serialize firecracker manifest"),
        )
        .expect("write firecracker manifest");
    }

    #[tokio::test]
    async fn publishes_gets_and_resolves_snapshot() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = test_backend(tempdir.path());
        let repository = backend.repository();
        let resolver = backend.runtime_resolver();
        let snapshot_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        let metadata = sample_metadata(snapshot_id, Some("mvp"));
        let stored = repository
            .publish(metadata, local_artifacts)
            .await
            .expect("publish should work");

        let fetched = repository
            .get("mvp")
            .await
            .expect("get should work")
            .expect("snapshot should exist");
        assert_eq!(stored.id, fetched.id);

        let runnable = resolver
            .resolve(Arc::new(fetched))
            .await
            .expect("resolve should work");
        assert!(runnable.manifest().rootfs.image_config_path.exists());
        assert!(runnable.manifest().vm_state.path.exists());
        assert!(runnable.manifest().memory.image_config_path.exists());

        let image_config: OverlaybdImageConfig = serde_json::from_slice(
            &std::fs::read(runnable.manifest().rootfs.image_config_path.as_path())
                .expect("read image config"),
        )
        .expect("parse overlaybd image config");
        assert_eq!(image_config.repo_blob_url, "");
        assert_eq!(image_config.lowers.len(), 1);
    }

    #[tokio::test]
    async fn publish_rebinds_existing_alias_to_new_snapshot() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_backend(tempdir.path()).repository();

        let first_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        repository
            .publish(
                sample_metadata(first_id.clone(), Some("rebind")),
                local_artifacts,
            )
            .await
            .expect("first publish should work");

        let second_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        repository
            .publish(
                sample_metadata(second_id.clone(), Some("rebind")),
                local_artifacts,
            )
            .await
            .expect("second publish should rebind the alias");

        let resolved = repository
            .resolve_alias("rebind")
            .await
            .expect("resolve should work")
            .expect("alias should resolve");
        assert_eq!(resolved, second_id, "alias should move to the new snapshot");

        let previous = repository
            .get(first_id.to_string().as_str())
            .await
            .expect("get should work")
            .expect("previous snapshot should stay addressable by id");
        assert_eq!(
            previous.alias, None,
            "previous snapshot should lose the rebound alias"
        );
    }

    #[tokio::test]
    async fn failed_publish_keeps_previous_alias_and_removes_snapshot_dir() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_backend(tempdir.path()).repository();

        let first_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        repository
            .publish(
                sample_metadata(first_id.clone(), Some("rebind")),
                local_artifacts,
            )
            .await
            .expect("first publish should work");

        let second_id = SnapshotId::generate();
        let broken_artifacts = seed_built_snapshot(tempdir.path());
        // `import_built_artifacts` copies `vm_state.bin` first, so removing it
        // fails the publish before any catalog state is committed.
        fs::remove_file(&broken_artifacts.vm_state.path).expect("remove seeded vm state");
        repository
            .publish(
                sample_metadata(second_id.clone(), Some("rebind")),
                broken_artifacts,
            )
            .await
            .expect_err("publish should fail when the vm state artifact is missing");

        assert!(
            !tempdir
                .path()
                .join("snapshots")
                .join(second_id.to_string())
                .exists(),
            "failed publish should not leave a snapshot directory behind"
        );

        let resolved = repository
            .resolve_alias("rebind")
            .await
            .expect("resolve should work")
            .expect("alias should still resolve");
        assert_eq!(
            resolved, first_id,
            "alias should stay bound to the previously committed snapshot"
        );

        let previous = repository
            .get(first_id.to_string().as_str())
            .await
            .expect("get should work")
            .expect("previous snapshot should stay addressable by id");
        assert_eq!(
            previous.alias.as_ref().map(ToString::to_string),
            Some("rebind".to_string()),
            "previous snapshot should keep the alias after a failed rebind"
        );
    }

    #[tokio::test]
    async fn create_keeps_existing_alias_until_new_build_commits() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_backend(tempdir.path()).repository();

        let committed_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        repository
            .publish(
                sample_metadata(committed_id.clone(), Some("stable")),
                local_artifacts,
            )
            .await
            .expect("publish should work");

        let waiting = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(SnapshotAlias::parse("stable").expect("alias should parse")),
            crate::types::SandboxResources {
                cpu_count: 1,
                memory_mib: 256,
                disk_size_mib: 0,
            },
        );
        let waiting_id = waiting.id.clone();
        repository
            .create(waiting)
            .await
            .expect("create with an existing alias should be allowed");

        let resolved = repository
            .resolve_alias("stable")
            .await
            .expect("resolve should work")
            .expect("alias should resolve");
        assert_eq!(
            resolved, committed_id,
            "alias should keep pointing at the committed snapshot while the rebuild is pending"
        );

        let local_artifacts = seed_built_snapshot(tempdir.path());
        repository
            .publish(
                sample_metadata(waiting_id.clone(), Some("stable")),
                local_artifacts,
            )
            .await
            .expect("publishing the rebuild should rebind the alias");

        let resolved = repository
            .resolve_alias("stable")
            .await
            .expect("resolve should work")
            .expect("alias should resolve");
        assert_eq!(resolved, waiting_id, "alias should move after commit");
    }

    #[tokio::test]
    async fn delete_removes_committed_snapshot_directory() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_backend(tempdir.path()).repository();
        let snapshot_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        let metadata = sample_metadata(snapshot_id.clone(), Some("cleanup"));

        repository
            .publish(metadata, local_artifacts)
            .await
            .expect("publish should work");

        let committed_dir = tempdir
            .path()
            .join("snapshots")
            .join(snapshot_id.to_string());
        assert!(
            committed_dir.exists(),
            "committed snapshot dir should exist"
        );

        repository
            .delete("cleanup")
            .await
            .expect("delete should work");

        assert!(
            !committed_dir.exists(),
            "delete should remove the whole committed snapshot directory"
        );
        assert!(
            repository
                .get(&snapshot_id.to_string())
                .await
                .expect("get after delete should work")
                .is_none(),
            "deleted snapshot should no longer be visible"
        );
    }

    #[tokio::test]
    async fn delete_removes_failed_template_build_record() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_backend(tempdir.path()).repository();
        let snapshot_id = SnapshotId::generate();
        let record =
            SnapshotRecord::template_waiting(snapshot_id.clone(), None, Default::default());

        repository.create(record).await.expect("create should work");
        repository
            .mark_build_error(&snapshot_id, TemplateBuildErrorReason::new("boom"))
            .await
            .expect("mark error should work");

        let record_path = tempdir
            .path()
            .join("catalog")
            .join("records")
            .join(format!("{snapshot_id}.json"));
        assert!(record_path.exists(), "failed build record should exist");

        repository
            .delete(&snapshot_id.to_string())
            .await
            .expect("delete should work");

        assert!(
            !record_path.exists(),
            "delete should remove failed build record"
        );
        assert!(
            repository
                .get(&snapshot_id.to_string())
                .await
                .expect("get after delete should work")
                .is_none(),
            "deleted failed build should no longer be visible"
        );
    }

    #[tokio::test]
    async fn publish_rejects_duplicate_attached_drive_ids_before_touching_catalog() {
        let tempdir = TempDir::new().expect("tempdir");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let manifest = FirecrackerSnapshotManifest::for_test(
            32768,
            &[
                ExtraDrive::Overlaybd {
                    drive_id: "data".to_string(),
                    image_config_path: "drives/data/image.json".into(),
                    read_only: true,
                    mount_path: ExtraDrive::default_mount_path("data"),
                    virtual_size: Some(32768),
                    sub_path: None,
                },
                ExtraDrive::Overlaybd {
                    drive_id: "data".to_string(),
                    image_config_path: "drives/data/image.json".into(),
                    read_only: true,
                    mount_path: ExtraDrive::default_mount_path("data"),
                    virtual_size: Some(32768),
                    sub_path: None,
                },
            ],
        );
        let err = repository
            .publish(sample_metadata(snapshot_id, Some("dup-drive")), manifest)
            .await
            .expect_err("duplicate attached drive ids should be rejected");

        assert!(matches!(err, RepositoryError::InvalidRequest { .. }));
        assert!(!tempdir.path().join("catalog").exists());
    }

    #[tokio::test]
    async fn resolve_rejects_missing_artifact_paths() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let cache =
            LocalArtifactCache::new(tempdir.path().join("cache"), None).expect("local cache");
        let resolver = PosixFsRuntimeResolver::new(
            tempdir.path().to_path_buf(),
            tempdir.path().join("runtime-cache"),
            test_overlaybd_layer_store(),
            cache,
        );
        let metadata = sample_metadata(SnapshotId::generate(), None);
        let committed = CommittedSnapshot {
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            runtime_versions: metadata.runtime_versions.clone(),
            virtualization_mode: metadata.virtualization_mode,
            image_configs: metadata.image_configs.clone(),
            rootfs_layers: vec![OverlaybdLayerRef::Managed(ManagedLayer {
                digest: "sharedfs:missing".to_string(),
                size: 1,
                uuid: None,
            })],
            attached_drives: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            custom_extension_params: None,
        };
        let snapshot = Arc::new(ready_record(metadata, committed));

        let err = resolver
            .resolve(snapshot)
            .await
            .expect_err("resolve should fail");
        assert!(matches!(err, RepositoryError::ArtifactNotFound { .. }));
    }

    #[tokio::test]
    async fn resolve_materializes_runtime_image_config_from_rootfs_layers() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let cache =
            LocalArtifactCache::new(tempdir.path().join("cache"), None).expect("local cache");
        let resolver = PosixFsRuntimeResolver::new(
            tempdir.path().to_path_buf(),
            tempdir.path().join("runtime-cache"),
            test_overlaybd_layer_store(),
            cache,
        );
        let layer_path = tempdir.path().join("managed-layers").join(
            super::super::layout::managed_layer_file_name("sharedfs:test"),
        );
        std::fs::create_dir_all(layer_path.parent().expect("managed-layers dir"))
            .expect("managed-layers dir");
        std::fs::write(&layer_path, b"layer").expect("layer");
        let snapshot_id = SnapshotId::generate();
        let snapshot_dir = tempdir
            .path()
            .join("snapshots")
            .join(snapshot_id.to_string());
        std::fs::create_dir_all(&snapshot_dir).expect("snapshot dir");
        std::fs::write(snapshot_dir.join("vm_state.bin"), b"vm state").expect("vm state");
        seed_committed_firecracker_manifest(tempdir.path(), &snapshot_id, 0, 32 * 1024);
        std::fs::write(
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_dump),
            b"memory",
        )
        .expect("memory");

        let metadata = sample_metadata(snapshot_id, None);
        let committed = CommittedSnapshot {
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            runtime_versions: metadata.runtime_versions.clone(),
            virtualization_mode: metadata.virtualization_mode,
            image_configs: metadata.image_configs.clone(),
            rootfs_layers: vec![OverlaybdLayerRef::Managed(ManagedLayer {
                digest: "sharedfs:test".to_string(),
                size: 5,
                uuid: Some("11111111-2222-3333-4444-555555555555".to_string()),
            })],
            attached_drives: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            custom_extension_params: None,
        };
        let snapshot = Arc::new(ready_record(metadata, committed));

        let runnable = resolver
            .resolve(snapshot)
            .await
            .expect("resolve should work from rootfs layers");

        let image_config: OverlaybdImageConfig = serde_json::from_slice(
            &std::fs::read(runnable.manifest().rootfs.image_config_path.as_path())
                .expect("read image config"),
        )
        .expect("parse runtime image config");
        assert_eq!(image_config.lowers.len(), 1);
        assert_eq!(
            image_config.lowers[0].file,
            layer_path.display().to_string()
        );
        assert_eq!(
            image_config.lowers[0].uuid,
            "11111111-2222-3333-4444-555555555555"
        );
    }
}
