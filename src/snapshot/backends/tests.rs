use std::fs;
use std::path::Path;

use overlaybd::config::ImageConfig as OverlaybdImageConfig;
use tempfile::TempDir;

use super::*;
use crate::cfg::PosixFsBackendConfig as PosixConfig;
use crate::image::cache::OverlaybdLayerLocation;
use crate::repository::RepositoryError;
use crate::sandbox::{ExtraDrive, SandboxSnapshotManifest};
use crate::snapshot::mock::write_mock_built_artifacts;
use crate::snapshot::{
    CommittedSnapshot, ManagedLayer, OverlaybdLayerRef, SnapshotAlias, SnapshotCatalog, SnapshotId,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
    TemplateBuildErrorReason, SNAPSHOT_ARTIFACT_LAYOUT,
};

#[tokio::test]
async fn builders_share_a_private_catalog_across_node_local_caches() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut node_a = AppConfig::default();
    node_a.snapshot.repository_backend = SnapshotRepositoryBackendKind::PosixFs;
    node_a.backend.posix_fs = Some(PosixConfig {
        snapshot_store: dir.path().join("shared"),
    });
    node_a.snapshot.local_cache_path = dir.path().join("node-a/cache");
    node_a.image.cache.root_dir = dir.path().join("node-a/images");
    let mut node_b = node_a.clone();
    node_b.home_path = dir.path().join("node-b");
    node_b.snapshot.local_cache_path = dir.path().join("node-b/cache");
    node_b.image.cache.root_dir = dir.path().join("node-b/images");
    let (a, _) = build_snapshot_backend_from_config(&builder_snapshot_config(&node_a), None)?;
    let (b, _) = build_snapshot_backend_from_config(&builder_snapshot_config(&node_b), None)?;
    let record = SnapshotRecord::template_waiting(
        SnapshotId::generate(),
        Some(SnapshotAlias::parse("builder")?),
        Default::default(),
    );
    a.create(record.clone()).await?;
    assert_eq!(b.get("builder").await?.unwrap().id, record.id);
    let (public, _) = build_snapshot_backend_from_config(&node_b, None)?;
    assert!(public.get("builder").await?.is_none());
    public.delete("builder").await?;
    assert!(b.get("builder").await?.is_some());
    Ok(())
}

#[test]
fn builder_namespace_preserves_oss_bucket_and_prefix() {
    for (prefix, expected) in [
        (None, "template-build/builder"),
        (
            Some(" /cluster/snapshots/ "),
            "cluster/snapshots/template-build/builder",
        ),
    ] {
        let mut config = AppConfig::default();
        config.snapshot.repository_backend = SnapshotRepositoryBackendKind::Oss;
        config.backend.oss = Some(OssBackendConfig {
            endpoint: "https://storage.example.test".to_owned(),
            bucket: "shared".to_owned(),
            prefix: prefix.map(str::to_owned),
            credential_process: None,
            access_key_id: None,
            access_key_secret: None,
            security_token: None,
            region: Some("test".to_owned()),
            addressing_style: None,
            cache_max_size_gb: None,
        });
        let builder = builder_snapshot_config(&config);
        assert_eq!(
            builder.snapshot.repository_backend,
            SnapshotRepositoryBackendKind::Oss
        );
        let oss = builder.backend.oss.unwrap();
        assert_eq!(oss.bucket, "shared");
        assert_eq!(oss.prefix.as_deref(), Some(expected));
        assert_eq!(config.backend.oss.unwrap().prefix.as_deref(), prefix);
    }
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

fn sample_metadata(id: SnapshotId, alias: Option<&str>) -> SnapshotPublishMetadata {
    SnapshotPublishMetadata {
        id,
        alias: alias.map(|value| SnapshotAlias::parse(value).expect("alias should parse")),
        ..SnapshotPublishMetadata::mock()
    }
}

fn ready_record(metadata: SnapshotPublishMetadata, committed: CommittedSnapshot) -> SnapshotRecord {
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
    let cache =
        LocalArtifactCache::new(root.join("runtime-cache"), None).expect("local artifact cache");
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
    let resolver = test_resolver(
        tempdir.path(),
        tempdir.path().join("runtime-consume"),
        LocalArtifactCache::new(tempdir.path().join("cache-consume"), None).expect("local cache"),
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
    let disabled = test_resolver(
        tempdir.path(),
        tempdir.path().join("runtime-disabled"),
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
    let record = SnapshotRecord::template_waiting(snapshot_id.clone(), None, Default::default());

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
    let cache = LocalArtifactCache::new(tempdir.path().join("cache"), None).expect("local cache");
    let resolver = test_resolver(tempdir.path(), tempdir.path().join("runtime-cache"), cache);
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
    let cache = LocalArtifactCache::new(tempdir.path().join("cache"), None).expect("local cache");
    let resolver = test_resolver(tempdir.path(), tempdir.path().join("runtime-cache"), cache);
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
