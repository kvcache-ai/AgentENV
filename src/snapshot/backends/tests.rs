use std::fs;
use std::path::Path;

use overlaybd::config::ImageConfig as OverlaybdImageConfig;
use tempfile::TempDir;

use super::*;
use crate::cfg::PosixFsBackendConfig as PosixConfig;
use crate::image::cache::OverlaybdLayerLocation;
use crate::repository::RepositoryError;
use crate::snapshot::mock::{test_metadata_catalog, write_mock_built_artifacts};
use crate::snapshot::{
    publish_snapshot, BuildErrorReason, BuildTarget, LayerRef, ManagedLayerRef, MetadataCatalog,
    SnapshotAlias, SnapshotConfig, SnapshotId, SnapshotListFilter, SnapshotManager,
    SnapshotManifest, SnapshotPlacement, SnapshotRecord, SnapshotRuntimeManifest, SnapshotSource,
    SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::types::ExtraDrive;

#[tokio::test]
async fn internal_builders_are_shared_across_nodes_and_hidden_from_public_api() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut node_a = AppConfig::default();
    node_a.snapshot.repository_backend = SnapshotRepositoryBackendKind::PosixFs;
    node_a.backend.posix_fs = Some(PosixConfig {
        snapshot_store: dir.path().join("shared"),
    });
    node_a.snapshot.local_root = dir.path().join("node-a/cache");
    node_a.image.cache.root_dir = dir.path().join("node-a/images");
    let mut node_b = node_a.clone();
    node_b.home_path = dir.path().join("node-b");
    node_b.snapshot.local_root = dir.path().join("node-b/cache");
    node_b.image.cache.root_dir = dir.path().join("node-b/images");

    let (repo_a, resolver_a) = build_snapshot_storage_from_config(&node_a, None)?;
    let (repo_b, resolver_b) = build_snapshot_storage_from_config(&node_b, None)?;
    let (server, catalog_a) = test_metadata_catalog(dir.path(), Arc::clone(&repo_a)).await;
    let catalog_b = Arc::new(MetadataCatalog::new(
        agentenv_metadata::transport::embedded(server.clone()),
        Arc::clone(&repo_b),
    ));
    let manager_a = SnapshotManager::from_parts(catalog_a, repo_a, resolver_a, None);
    let manager_b = SnapshotManager::from_parts(catalog_b, repo_b, resolver_b, None);

    let mut config = SnapshotConfig::mock();
    config.source = SnapshotSource::Builder;
    let built = manager_a
        .publish(
            SnapshotId::generate(),
            Some(SnapshotAlias::parse("builder").expect("alias should parse")),
            config,
            seed_built_snapshot(dir.path()),
            None,
        )
        .await?;

    // Both nodes resolve the internal builder snapshot.
    assert_eq!(
        manager_b.get_internal("builder").await?.unwrap().id,
        built.id
    );
    assert!(manager_b.load_runnable_internal("builder").await?.is_some());

    // Public API lookups, listings, and deletes cannot observe or retire it.
    assert!(manager_b.get("builder").await?.is_none());
    assert!(manager_b.get(built.id.to_string()).await?.is_none());
    assert!(manager_b
        .resolve_committed_alias("builder")
        .await?
        .is_none());
    assert!(manager_b
        .list(SnapshotListFilter::matches_all())
        .await?
        .is_empty());
    manager_b.delete("builder").await?;
    assert!(manager_b.get_internal("builder").await?.is_some());

    server.close().await;
    Ok(())
}

#[tokio::test]
async fn posix_backend_honors_explicit_runtime_cache_root() {
    let temp = TempDir::new().expect("tempdir");
    let custom_runtime = temp.path().join("custom-runtime");
    let backend = PosixFsBackend::from_parts(
        PosixFsBackendConfig {
            root: temp.path().join("repository"),
            cache_root: Some(temp.path().join("cache")),
            runtime_cache_root: Some(custom_runtime.clone()),
        },
        test_overlaybd_layer_store(),
        LocalArtifactCache::new(temp.path().join("cache"), None).expect("cache"),
    );
    let repository = backend.repository();
    let (server, catalog) = test_metadata_catalog(temp.path(), Arc::clone(&repository)).await;
    let manifest = seed_built_snapshot(temp.path());
    let publication = publish_snapshot(
        catalog.as_ref(),
        repository.as_ref(),
        SnapshotId::generate(),
        None,
        SnapshotConfig::mock(),
        manifest,
        None,
    )
    .await
    .expect("publish");
    let handle = backend
        .runtime_resolver()
        .resolve(Arc::new(publication.record))
        .await
        .expect("resolve");
    assert!(
        handle
            .runtime_manifest()
            .rootfs
            .image_config_path
            .starts_with(&custom_runtime),
        "runtime image config should materialize under custom runtime_cache_root"
    );
    server.close().await;
}

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

fn ready_record(
    id: SnapshotId,
    alias: Option<&str>,
    committed: SnapshotManifest,
) -> SnapshotRecord {
    SnapshotRecord::new(
        id,
        SnapshotPlacement::Distributed,
        alias.map(|value| SnapshotAlias::parse(value).expect("alias should parse")),
        SnapshotConfig::mock(),
        Some(committed),
        0,
    )
}

fn test_backend(root: &Path) -> PosixFsBackend {
    let config = PosixFsBackendConfig {
        root: root.to_path_buf(),
        cache_root: Some(root.join("cache")),
        runtime_cache_root: None,
    };
    let cache = LocalArtifactCache::new(root.join("cache"), None).expect("local artifact cache");
    PosixFsBackend::from_parts(config, test_overlaybd_layer_store(), cache)
}

fn test_resolver(
    root: &Path,
    runtime_root: std::path::PathBuf,
    cache: Arc<LocalArtifactCache>,
) -> RepositoryRuntimeResolver {
    RepositoryRuntimeResolver::new(
        Arc::new(test_repository(root)),
        cache,
        runtime_root,
        test_overlaybd_layer_store(),
        None,
    )
}

fn test_repository(root: &Path) -> PosixFsRepository {
    PosixFsRepository::new(root.to_path_buf())
}

fn seed_built_snapshot(root: &Path) -> SnapshotRuntimeManifest {
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
    let manifest = SnapshotRuntimeManifest::new(
        crate::snapshot::FIRECRACKER_BACKEND,
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
    let (server, catalog) = test_metadata_catalog(tempdir.path(), Arc::clone(&repository)).await;
    let snapshot_id = SnapshotId::generate();
    let local_artifacts = seed_built_snapshot(tempdir.path());
    let stored = publish_snapshot(
        catalog.as_ref(),
        repository.as_ref(),
        snapshot_id,
        Some(SnapshotAlias::parse("mvp").expect("alias should parse")),
        SnapshotConfig::mock(),
        local_artifacts,
        None,
    )
    .await
    .expect("publish should work")
    .record;

    let fetched = catalog
        .get("mvp")
        .await
        .expect("get should work")
        .expect("snapshot should exist");
    assert_eq!(stored.id, fetched.id);

    let handle = resolver
        .resolve(Arc::new(fetched))
        .await
        .expect("resolve should work");
    assert!(handle.runtime_manifest().rootfs.image_config_path.exists());
    assert!(handle.runtime_manifest().vm_state.path.exists());
    assert!(handle.runtime_manifest().memory.image_config_path.exists());

    let image_config: OverlaybdImageConfig = serde_json::from_slice(
        &std::fs::read(handle.runtime_manifest().rootfs.image_config_path.as_path())
            .expect("read image config"),
    )
    .expect("parse overlaybd image config");
    assert_eq!(image_config.repo_blob_url, "");
    assert_eq!(image_config.lowers.len(), 1);
    server.close().await;
}

#[tokio::test]
async fn completed_trace_attaches_and_resolves_local_manifest_without_kvm() {
    let tempdir = TempDir::new().expect("tempdir");
    let backend = test_backend(tempdir.path());
    let repository = backend.repository();
    let (server, catalog) = test_metadata_catalog(tempdir.path(), Arc::clone(&repository)).await;
    let snapshot_id = SnapshotId::generate();
    let mut local_artifacts = seed_built_snapshot(tempdir.path());
    let memory_size = overlaybd::startup_pack::PACK_PAGE_BYTES;
    local_artifacts.memory.virtual_size = memory_size;

    let trace_path = tempdir.path().join("synthetic.trace");
    let trace = overlaybd::startup_pack::encode_trace(memory_size, &[0]).expect("trace");
    tokio::fs::write(&trace_path, trace)
        .await
        .expect("write trace");
    let recording = crate::snapshot::StartupRecording {
        trace: tokio::spawn(async move { Some(trace_path) }),
        keep_alive: Box::new(()),
    };
    publish_snapshot(
        catalog.as_ref(),
        repository.as_ref(),
        snapshot_id.clone(),
        None,
        SnapshotConfig::mock(),
        local_artifacts,
        Some(recording),
    )
    .await
    .expect("publish");

    let committed = catalog
        .get(&snapshot_id.to_string())
        .await
        .expect("get")
        .expect("published record");
    let descriptor = committed
        .manifest
        .as_ref()
        .and_then(|value| value.memory_startup.as_ref())
        .expect("descriptor attached")
        .clone();
    let resolver = test_resolver(
        tempdir.path(),
        tempdir.path().join("runtime-consume"),
        LocalArtifactCache::new(tempdir.path().join("cache-consume"), None).expect("local cache"),
    )
    .with_test_consume_enabled(true);
    let handle = resolver
        .resolve(Arc::new(committed))
        .await
        .expect("resolve");
    let pack = handle
        .runtime_manifest()
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
    let disabled = test_resolver(
        tempdir.path(),
        tempdir.path().join("runtime-disabled"),
        LocalArtifactCache::new(tempdir.path().join("cache-disabled"), None).unwrap(),
    )
    .with_test_consume_enabled(false);
    let record = catalog
        .get(&snapshot_id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert!(disabled
        .resolve(Arc::new(record.clone()))
        .await
        .unwrap()
        .runtime_manifest()
        .memory_startup_pack
        .is_none());
    let mut legacy = record;
    legacy.manifest.as_mut().unwrap().memory_startup = None;
    assert!(resolver
        .resolve(Arc::new(legacy))
        .await
        .unwrap()
        .runtime_manifest()
        .memory_startup_pack
        .is_none());
    server.close().await;
}

#[tokio::test]
async fn failed_commit_cleans_uncommitted_snapshot_directory() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let repository_root = tempdir.path().to_path_buf();
    let repository = test_backend(tempdir.path()).repository();
    let (server, catalog) = test_metadata_catalog(tempdir.path(), Arc::clone(&repository)).await;

    let first_id = SnapshotId::generate();
    let local_artifacts = seed_built_snapshot(tempdir.path());
    let conflict = Some(SnapshotAlias::parse("conflict").expect("alias should parse"));
    publish_snapshot(
        catalog.as_ref(),
        repository.as_ref(),
        first_id.clone(),
        conflict.clone(),
        SnapshotConfig::mock(),
        local_artifacts,
        None,
    )
    .await
    .expect("first publish should work");

    let second_id = SnapshotId::generate();
    let local_artifacts = seed_built_snapshot(tempdir.path());
    let err = publish_snapshot(
        catalog.as_ref(),
        repository.as_ref(),
        second_id.clone(),
        conflict,
        SnapshotConfig::mock(),
        local_artifacts,
        None,
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
    server.close().await;
}

#[tokio::test]
async fn delete_and_gc_remove_committed_snapshot_directory() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let repository = test_backend(tempdir.path()).repository();
    let (server, catalog) = test_metadata_catalog(tempdir.path(), Arc::clone(&repository)).await;
    let snapshot_id = SnapshotId::generate();
    let local_artifacts = seed_built_snapshot(tempdir.path());
    publish_snapshot(
        catalog.as_ref(),
        repository.as_ref(),
        snapshot_id.clone(),
        Some(SnapshotAlias::parse("cleanup").expect("alias should parse")),
        SnapshotConfig::mock(),
        local_artifacts,
        None,
    )
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

    catalog.delete("cleanup").await.expect("delete should work");
    assert!(
        catalog
            .get(&snapshot_id.to_string())
            .await
            .expect("get after delete should work")
            .is_none(),
        "deleted snapshot should no longer be visible"
    );

    let report = server.collect_garbage(64).await.expect("gc should work");
    assert_eq!(report.reaped_snapshots, 1);
    assert!(
        !committed_dir.exists(),
        "gc should remove the whole retired snapshot directory"
    );
    server.close().await;
}

#[tokio::test]
async fn delete_build_job_removes_failed_template_build_and_reserves_id() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let repository = test_backend(tempdir.path()).repository();
    let (server, _catalog) = test_metadata_catalog(tempdir.path(), Arc::clone(&repository)).await;
    let build_id = SnapshotId::generate();

    server
        .create_build_job(
            &build_id,
            &BuildTarget::Template {
                alias: None,
                resources: Default::default(),
            },
        )
        .await
        .expect("create should work");
    server
        .fail_build_job(&build_id, &BuildErrorReason::new("boom"))
        .await
        .expect("mark error should work");
    assert!(server
        .get_build_job(&build_id)
        .await
        .expect("get")
        .is_some());

    server
        .delete_build_job(&build_id, true)
        .await
        .expect("delete should work");

    assert!(
        server
            .get_build_job(&build_id)
            .await
            .expect("get after delete")
            .is_none(),
        "deleted failed build should no longer be visible"
    );
    server.close().await;
}

#[tokio::test]
async fn publish_rejects_duplicate_attached_drive_ids_before_touching_catalog() {
    let tempdir = TempDir::new().expect("tempdir");
    let repository: Arc<dyn Repository> = Arc::new(test_repository(tempdir.path()));
    let (server, catalog) = test_metadata_catalog(tempdir.path(), Arc::clone(&repository)).await;
    let snapshot_id = SnapshotId::generate();
    let manifest = SnapshotRuntimeManifest::for_test(
        32768,
        &[
            ExtraDrive::Overlaybd {
                layers: None,

                drive_id: "data".to_string(),
                image_config_path: "drives/data/image.json".into(),
                read_only: true,
                mount_path: ExtraDrive::default_mount_path("data"),
                virtual_size: Some(32768),
                sub_path: None,
                snapshot_output_dir: None,
                kind: crate::types::ExtraDriveKind::Physical,
            },
            ExtraDrive::Overlaybd {
                layers: None,

                drive_id: "data".to_string(),
                image_config_path: "drives/data/image.json".into(),
                read_only: true,
                mount_path: ExtraDrive::default_mount_path("data"),
                virtual_size: Some(32768),
                sub_path: None,
                snapshot_output_dir: None,
                kind: crate::types::ExtraDriveKind::Physical,
            },
        ],
    );
    let err = publish_snapshot(
        catalog.as_ref(),
        repository.as_ref(),
        snapshot_id,
        Some(SnapshotAlias::parse("dup-drive").expect("alias should parse")),
        SnapshotConfig::mock(),
        manifest,
        None,
    )
    .await
    .expect_err("duplicate attached drive ids should be rejected");

    assert!(matches!(err, RepositoryError::InvalidRequest { .. }));
    assert!(catalog
        .list(SnapshotListFilter::matches_all())
        .await
        .expect("list")
        .is_empty());
    server.close().await;
}

#[tokio::test]
async fn resolve_rejects_missing_artifact_paths() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let cache = LocalArtifactCache::new(tempdir.path().join("cache"), None).expect("local cache");
    let resolver = test_resolver(tempdir.path(), tempdir.path().join("cache/runtime"), cache);
    let committed = SnapshotManifest {
        rootfs_layers: vec![LayerRef::Managed(ManagedLayerRef {
            digest: "sharedfs:missing".to_string(),
            size: 1,
            uuid: None,
        })],
        attached_drives: Vec::new(),
        memory_layers: Vec::new(),
        disk_publications: Vec::new(),
        memory_startup: None,
    };
    let snapshot = Arc::new(ready_record(SnapshotId::generate(), None, committed));

    let err = resolver
        .resolve(snapshot)
        .await
        .expect_err("resolve should fail");
    assert!(matches!(err, RepositoryError::ArtifactNotFound { .. }));
}

#[tokio::test]
async fn resolve_materializes_runtime_image_config_from_rootfs_layers() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let cache = LocalArtifactCache::new(tempdir.path().join("cache"), None).expect("local cache");
    let resolver = test_resolver(tempdir.path(), tempdir.path().join("cache/runtime"), cache);
    let layer_path = tempdir.path().join(
        crate::repository::BlobKey::Layer {
            digest: "sharedfs:test".to_owned(),
            role: crate::repository::LayerRole::Rootfs,
        }
        .posix_path(),
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

    let committed = SnapshotManifest {
        rootfs_layers: vec![LayerRef::Managed(ManagedLayerRef {
            digest: "sharedfs:test".to_string(),
            size: 5,
            uuid: Some("11111111-2222-3333-4444-555555555555".to_string()),
        })],
        attached_drives: Vec::new(),
        memory_layers: Vec::new(),
        disk_publications: Vec::new(),
        memory_startup: None,
    };
    let snapshot = Arc::new(ready_record(snapshot_id, None, committed));

    let handle = resolver
        .resolve(snapshot)
        .await
        .expect("resolve should work from rootfs layers");

    let image_config: OverlaybdImageConfig = serde_json::from_slice(
        &std::fs::read(handle.runtime_manifest().rootfs.image_config_path.as_path())
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
