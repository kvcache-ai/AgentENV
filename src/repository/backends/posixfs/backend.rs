use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tempfile::NamedTempFile;
use tokio::task;

use super::super::{common::materialize_volume_image_config, shared_runtime_cache_root};
use super::artifacts::PosixFsArtifactStore;
use super::catalog::PosixFsCatalogStore;
use super::runtime::PosixFsRuntimeResolver;
use crate::image::cache::{local_image_services_from_global_config, OverlaybdLayerStore};
use crate::repository::interfaces::{Repository, SnapshotRuntimeResolver};
use crate::repository::{
    BlobBody, BlobKey, DocumentCollection, DocumentKey, DocumentUpdate, RepositoryError,
    RepositoryResult,
};
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::SnapshotId;

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
    repository: Arc<dyn Repository>,
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
        let repository: Arc<dyn Repository> =
            Arc::new(PosixFsRepository::new(catalog_store, artifact_store));
        let runtime_resolver: Arc<dyn SnapshotRuntimeResolver> = Arc::new(
            PosixFsRuntimeResolver::new(root, runtime_cache_root, store, cache),
        );

        Self {
            repository,
            runtime_resolver,
        }
    }

    /// Returns the committed-state repository for publish/get/list/delete operations.
    pub fn repository(&self) -> Arc<dyn Repository> {
        Arc::clone(&self.repository)
    }

    /// Returns the node-local runtime resolver used to materialize runnable paths.
    pub fn runtime_resolver(&self) -> Arc<dyn SnapshotRuntimeResolver> {
        Arc::clone(&self.runtime_resolver)
    }

    /// Splits the backend into its repository and runtime-resolution components.
    pub fn into_parts(self) -> (Arc<dyn Repository>, Arc<dyn SnapshotRuntimeResolver>) {
        (self.repository, self.runtime_resolver)
    }
}

#[derive(Clone)]
pub(crate) struct PosixFsRepository {
    catalog_store: Arc<PosixFsCatalogStore>,
    artifact_store: Arc<PosixFsArtifactStore>,
}

impl PosixFsRepository {
    pub(crate) fn new(
        catalog_store: Arc<PosixFsCatalogStore>,
        artifact_store: Arc<PosixFsArtifactStore>,
    ) -> Self {
        Self {
            catalog_store,
            artifact_store,
        }
    }

    async fn run_catalog<T, F>(&self, operation: &'static str, work: F) -> RepositoryResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&PosixFsCatalogStore) -> RepositoryResult<T> + Send + 'static,
    {
        let store = Arc::clone(&self.catalog_store);
        run_repository_blocking(operation, move || work(&store)).await
    }

    async fn run_artifacts<T, F>(&self, operation: &'static str, work: F) -> RepositoryResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&PosixFsArtifactStore) -> RepositoryResult<T> + Send + 'static,
    {
        let store = Arc::clone(&self.artifact_store);
        run_repository_blocking(operation, move || work(&store)).await
    }
}

#[async_trait]
impl Repository for PosixFsRepository {
    async fn put_blob(&self, key: &BlobKey, body: BlobBody<'_>) -> RepositoryResult<()> {
        let key = key.clone();
        match body {
            BlobBody::File(source) => {
                let source = source.to_path_buf();
                self.run_artifacts("store blob", move |store| {
                    store.put_blob(&key, BlobBody::File(&source))
                })
                .await
            }
            BlobBody::Bytes(bytes) => {
                self.run_artifacts("store blob", move |store| {
                    store.put_blob(&key, BlobBody::Bytes(bytes))
                })
                .await
            }
        }
    }

    async fn delete_snapshot_blobs(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let id = id.clone();
        self.run_artifacts("delete snapshot files", move |store| {
            store.delete_snapshot_files(&id)
        })
        .await
    }

    fn staging_file(&self) -> RepositoryResult<NamedTempFile> {
        self.artifact_store.staging_file()
    }

    async fn get_document(&self, key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        self.run_catalog("read document", move |store| store.get_document(&key))
            .await
    }

    async fn put_document(&self, key: DocumentKey, bytes: Vec<u8>) -> RepositoryResult<()> {
        self.run_catalog("write document", move |store| {
            store.put_document(&key, &bytes)
        })
        .await
    }

    async fn update_document(
        &self,
        key: DocumentKey,
        update: DocumentUpdate,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        self.run_catalog("update document", move |store| {
            store.update_document(&key, update)
        })
        .await
    }

    async fn delete_document(&self, key: DocumentKey) -> RepositoryResult<()> {
        self.run_catalog("delete document", move |store| store.delete_document(&key))
            .await
    }

    async fn list_documents(
        &self,
        collection: DocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        let after = after.map(str::to_owned);
        self.run_catalog("list documents", move |store| {
            store.list_documents(collection, after.as_deref(), limit)
        })
        .await
    }

    async fn read_documents(
        &self,
        collection: DocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        self.run_catalog("read documents", move |store| {
            store.read_documents(collection)
        })
        .await
    }

    async fn materialize_volume_backing(
        &self,
        _volume_id: &str,
        layers: &[crate::snapshot::OverlaybdLayerRef],
        destination: &std::path::Path,
    ) -> RepositoryResult<std::path::PathBuf> {
        materialize_volume_image_config(layers, destination, |layer| {
            overlaybd::config::LayerConfig {
                file: self
                    .artifact_store
                    .managed_layer_path(&layer.digest)
                    .to_string_lossy()
                    .into_owned(),
                digest: layer.digest.clone(),
                size: layer.size,
                uuid: layer.uuid.clone().unwrap_or_default(),
                ..Default::default()
            }
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
    use super::{PosixFsBackend, PosixFsBackendConfig, PosixFsRepository};
    use crate::image::cache::{OverlaybdLayerLocation, OverlaybdLayerStore};
    use crate::repository::{RepositoryError, SnapshotRuntimeResolver};
    use crate::sandbox::{ExtraDrive, SandboxSnapshotManifest};
    use crate::snapshot::artifact_cache::LocalArtifactCache;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::{
        CommittedSnapshot, ManagedLayer, OverlaybdLayerRef, SnapshotAlias, SnapshotCatalog,
        SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
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

    fn test_repository(root: &Path) -> PosixFsRepository {
        PosixFsRepository::new(
            Arc::new(PosixFsCatalogStore::new(root.to_path_buf())),
            Arc::new(PosixFsArtifactStore::new(root.to_path_buf())),
        )
    }

    fn seed_built_snapshot(root: &Path) -> SandboxSnapshotManifest {
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
        let manifest = SandboxSnapshotManifest::new(
            crate::sandbox::FIRECRACKER_BACKEND,
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
    async fn completed_trace_attaches_and_resolves_local_manifest_without_kvm() {
        let tempdir = TempDir::new().expect("tempdir");
        let backend = test_backend(tempdir.path());
        let repository = backend.repository();
        let snapshot_id = SnapshotId::generate();
        let mut local_artifacts = seed_built_snapshot(tempdir.path());
        let memory_size = overlaybd::startup_pack::PACK_PAGE_BYTES;
        local_artifacts.memory.virtual_size = memory_size;
        repository
            .publish(sample_metadata(snapshot_id.clone(), None), local_artifacts)
            .await
            .expect("publish");

        let trace_path = tempdir.path().join("synthetic.trace");
        let trace = overlaybd::startup_pack::encode_trace(memory_size, &[0]).expect("trace");
        tokio::fs::write(&trace_path, trace)
            .await
            .expect("write trace");
        let recording = crate::snapshot::StartupRecording {
            trace: tokio::spawn(async move { Some(trace_path) }),
            keep_alive: Box::new(()),
        };
        let guard = crate::snapshot::startup_pack::StartupManifestTaskGuard::try_register()
            .expect("register continuation");
        crate::snapshot::publish::finish_startup_manifest(
            Arc::clone(&repository),
            snapshot_id.clone(),
            recording,
            guard,
            || false,
        )
        .await;

        let committed = repository
            .get(&snapshot_id.to_string())
            .await
            .expect("get")
            .expect("published record");
        let descriptor = committed
            .committed
            .as_ref()
            .and_then(|value| value.memory_startup.as_ref())
            .expect("descriptor attached")
            .clone();
        let resolver = PosixFsRuntimeResolver::new(
            tempdir.path().to_path_buf(),
            tempdir.path().join("runtime-consume"),
            test_overlaybd_layer_store(),
            LocalArtifactCache::new(tempdir.path().join("cache-consume"), None)
                .expect("local cache"),
        )
        .with_test_consume_enabled(true);
        let runnable = resolver
            .resolve(Arc::new(committed))
            .await
            .expect("resolve");
        let pack = runnable
            .manifest()
            .memory_startup_pack
            .as_ref()
            .expect("LocalPath manifest");
        let crate::snapshot::ResolvedStartupPackSource::LocalPath(path) = &pack.source else {
            panic!("expected LocalPath");
        };
        assert_eq!(pack.index_sha256, descriptor.index_sha256);
        assert_eq!(
            std::fs::read(path).unwrap().len() as u64,
            descriptor.pack_size
        );
        let disabled = PosixFsRuntimeResolver::new(
            tempdir.path().to_path_buf(),
            tempdir.path().join("runtime-disabled"),
            test_overlaybd_layer_store(),
            LocalArtifactCache::new(tempdir.path().join("cache-disabled"), None).unwrap(),
        )
        .with_test_consume_enabled(false);
        let record = repository
            .get(&snapshot_id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert!(disabled
            .resolve(Arc::new(record.clone()))
            .await
            .unwrap()
            .manifest()
            .memory_startup_pack
            .is_none());
        let mut legacy = record;
        legacy.committed.as_mut().unwrap().memory_startup = None;
        assert!(resolver
            .resolve(Arc::new(legacy))
            .await
            .unwrap()
            .manifest()
            .memory_startup_pack
            .is_none());
    }

    #[tokio::test]
    async fn failed_commit_cleans_uncommitted_snapshot_directory() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository_root = tempdir.path().to_path_buf();
        let repository = test_backend(tempdir.path()).repository();

        let first_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        let first_metadata = sample_metadata(first_id.clone(), Some("conflict"));
        repository
            .publish(first_metadata, local_artifacts)
            .await
            .expect("first publish should work");

        let second_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        let err = repository
            .publish(
                sample_metadata(second_id.clone(), Some("conflict")),
                local_artifacts,
            )
            .await
            .expect_err("second publish should fail");

        assert!(matches!(err, RepositoryError::AliasConflict { .. }));
        assert!(
            !repository_root
                .join("snapshots")
                .join(second_id.to_string())
                .exists(),
            "failed publish should not leave a committed revision directory"
        );
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
        let manifest = SandboxSnapshotManifest::for_test(
            32768,
            &[
                ExtraDrive::Overlaybd {
                    drive_id: "data".to_string(),
                    image_config_path: "drives/data/image.json".into(),
                    read_only: true,
                    mount_path: ExtraDrive::default_mount_path("data"),
                    virtual_size: Some(32768),
                    sub_path: None,
                    snapshot_output_dir: None,
                    volume: false,
                },
                ExtraDrive::Overlaybd {
                    drive_id: "data".to_string(),
                    image_config_path: "drives/data/image.json".into(),
                    read_only: true,
                    mount_path: ExtraDrive::default_mount_path("data"),
                    virtual_size: Some(32768),
                    sub_path: None,
                    snapshot_output_dir: None,
                    volume: false,
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
            volume_snapshots: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            custom_extension_params: None,
            memory_startup: None,
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
            volume_snapshots: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            custom_extension_params: None,
            memory_startup: None,
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
