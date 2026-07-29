use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use agentenv::cfg::{ConfigManager, OssBackendConfig};
use agentenv::sandbox::FirecrackerSnapshotManifest;
use agentenv::snapshot::mock::write_mock_built_artifacts;
use agentenv::snapshot::repository::backends::OssBackend;
use agentenv::snapshot::{
    OverlaybdLayerRef, RepositoryError, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRuntimeVersions,
    SNAPSHOT_ARTIFACT_LAYOUT,
};
use agentenv::types::SandboxResources;
use agentenv_test_support::minio::{MinioFixture, MINIO_PASS, MINIO_USER};
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn test_runtime_versions() -> SnapshotRuntimeVersions {
    SnapshotRuntimeVersions {
        kernel_version: "kernel".to_string(),
        firecracker_version: "fc".to_string(),
        envd_version: "envd".to_string(),
        tools_drive_version: "0.1.0".to_string(),
    }
}

fn write_built_artifacts(root: &Path) -> Result<(String, String, FirecrackerSnapshotManifest)> {
    let (rootfs_lower, memory_lower, manifest) = write_mock_built_artifacts(root)?;
    Ok((
        digest_for_file(&rootfs_lower)?,
        digest_for_file(&memory_lower)?,
        manifest,
    ))
}

fn digest_for_file(path: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(std::fs::read(path)?);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn ensure_test_config() -> Result<()> {
    static INIT: OnceLock<()> = OnceLock::new();
    static INIT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = INIT_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
    if INIT.get().is_some() {
        return Ok(());
    }

    let config_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("snapshot-oss-e2e-config");
    let deps_path = config_root.join("env");
    let local_cache = config_root.join("snapshot-local-cache");
    std::fs::create_dir_all(&config_root)?;
    let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("config")
        .join("oss_default.toml");
    std::env::set_var("AENV_CONFIG_PATH", &config_path);
    std::env::set_var("AENV_DEPS_PATH", &deps_path);
    std::env::set_var("AENV_SNAPSHOT_LOCAL_CACHE_PATH", &local_cache);

    let manager = ConfigManager::init_global()?;
    let overlaybd_global = manager.config().ublk.overlaybd.global_config_path.clone();
    let overlaybd_dir = overlaybd_global
        .parent()
        .context("overlaybd global config must have parent")?;
    std::fs::create_dir_all(overlaybd_dir)?;
    std::fs::write(&overlaybd_global, "{}")?;

    let _ = INIT.set(());
    Ok(())
}

fn test_oss_config(fixture: &MinioFixture, prefix: &str) -> OssBackendConfig {
    OssBackendConfig {
        endpoint: fixture.endpoint.clone(),
        bucket: fixture.bucket.clone(),
        prefix: Some(prefix.to_string()),
        credential_process: None,
        access_key_id: Some(MINIO_USER.to_string()),
        access_key_secret: Some(MINIO_PASS.to_string()),
        security_token: None,
        region: Some(fixture.region.clone()),
        addressing_style: None,
        cache_max_size_gb: Some(1),
    }
}

fn prefixed_key(prefix: &str, relative: &str) -> String {
    format!("{prefix}/{relative}")
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_oss_publish_and_resolve_remote_managed_layers() -> Result<()> {
    let fixture = MinioFixture::start().await?;
    let workspace = TempDir::new()?;
    let prefix = "snapshots/e2e";
    let oss = test_oss_config(&fixture, prefix);
    ensure_test_config()?;

    let cache_root = workspace.path().join("oss-cache");
    let (repository, resolver) = OssBackend::new(&oss, cache_root)?.into_parts();
    let artifacts_root = workspace.path().join("local-artifacts");
    let (rootfs_digest, memory_digest, manifest) = write_built_artifacts(&artifacts_root)?;
    let snapshot_id = SnapshotId::generate();

    let stored = repository
        .publish(
            SnapshotPublishMetadata {
                id: snapshot_id.clone(),
                alias: Some(SnapshotAlias::parse("oss-e2e").expect("alias should parse")),
                source: SnapshotPublishSource::Template,
                context: agentenv::snapshot::CommandContext::default(),
                startup: None,
                resources: SandboxResources::default(),
                runtime_versions: test_runtime_versions(),
                image_configs: agentenv::types::ImageConfigs::new(),
                custom_extension_params: None,
            },
            manifest,
        )
        .await?;

    let committed = stored
        .committed
        .as_ref()
        .expect("published snapshot should be committed");
    assert!(matches!(
        committed.rootfs_layers.as_slice(),
        [OverlaybdLayerRef::Managed(_)]
    ));
    assert_eq!(committed.memory_layers.len(), 1);

    assert!(
        fixture
            .object_exists(&format!("{prefix}/managed-layers/{rootfs_digest}"))
            .await?
    );
    assert!(
        fixture
            .object_exists(&format!("{prefix}/managed-layers/{memory_digest}"))
            .await?
    );
    assert!(
        fixture
            .object_exists(&format!("{prefix}/artifacts/{snapshot_id}/vm_state.bin"))
            .await?
    );
    assert!(
        fixture
            .object_exists(&format!(
                "{prefix}/artifacts/{snapshot_id}/{}",
                SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest
            ))
            .await?
    );

    let runnable = resolver.resolve(Arc::new(stored)).await?;
    assert!(runnable.manifest().vm_state.path.exists());
    assert!(runnable.manifest().memory.image_config_path.exists());
    assert!(runnable.manifest().rootfs.image_config_path.exists());

    let rootfs_config: serde_json::Value = serde_json::from_slice(&std::fs::read(
        runnable.manifest().rootfs.image_config_path.as_path(),
    )?)?;
    assert_eq!(
        rootfs_config["repoBlobUrl"],
        format!("s3://{}/{prefix}/managed-layers", fixture.bucket)
    );
    assert_eq!(rootfs_config["lowers"][0]["digest"], rootfs_digest);
    assert_eq!(rootfs_config["lowers"][0]["file"], "");

    let mem_config: serde_json::Value = serde_json::from_slice(&std::fs::read(
        runnable.manifest().memory.image_config_path.as_path(),
    )?)?;
    assert_eq!(
        mem_config["repoBlobUrl"],
        format!("s3://{}/{prefix}/managed-layers", fixture.bucket)
    );
    assert_eq!(mem_config["lowers"][0]["digest"], memory_digest);
    assert_eq!(mem_config["lowers"][0]["file"], "");

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_oss_resolve_alias_cleans_up_stale_binding() -> Result<()> {
    let fixture = MinioFixture::start().await?;
    let workspace = TempDir::new()?;
    let prefix = "snapshots/stale-alias";
    let oss = test_oss_config(&fixture, prefix);
    ensure_test_config()?;

    let (repository, _) = OssBackend::new(&oss, workspace.path().join("oss-cache"))?.into_parts();
    let artifacts_root = workspace.path().join("local-artifacts");
    let (_, _, manifest) = write_built_artifacts(&artifacts_root)?;
    let alias = SnapshotAlias::parse("stale-alias").expect("alias should parse");
    let snapshot_id = SnapshotId::generate();

    let stored = repository
        .publish(
            SnapshotPublishMetadata {
                id: snapshot_id.clone(),
                alias: Some(alias.clone()),
                source: SnapshotPublishSource::Template,
                context: agentenv::snapshot::CommandContext::default(),
                startup: None,
                resources: SandboxResources::default(),
                runtime_versions: test_runtime_versions(),
                image_configs: agentenv::types::ImageConfigs::new(),
                custom_extension_params: None,
            },
            manifest,
        )
        .await?;

    let record_key = prefixed_key(prefix, &format!("catalog/records/{}.json", stored.id));
    fixture
        .client
        .delete_object()
        .bucket(&fixture.bucket)
        .key(&record_key)
        .send()
        .await?;

    assert_eq!(repository.resolve_alias(alias.as_ref()).await?, None);
    assert!(
        !fixture
            .object_exists(&prefixed_key(
                prefix,
                &format!("catalog/aliases/{}.json", alias.as_ref())
            ))
            .await?
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_oss_resolve_reports_missing_managed_layer() -> Result<()> {
    let fixture = MinioFixture::start().await?;
    let workspace = TempDir::new()?;
    let prefix = "snapshots/missing-managed-layer";
    let oss = test_oss_config(&fixture, prefix);
    ensure_test_config()?;

    let (repository, resolver) =
        OssBackend::new(&oss, workspace.path().join("oss-cache"))?.into_parts();
    let artifacts_root = workspace.path().join("local-artifacts");
    let (rootfs_digest, _, manifest) = write_built_artifacts(&artifacts_root)?;
    let snapshot_id = SnapshotId::generate();

    let stored = repository
        .publish(
            SnapshotPublishMetadata {
                id: snapshot_id,
                alias: None,
                source: SnapshotPublishSource::Template,
                context: agentenv::snapshot::CommandContext::default(),
                startup: None,
                resources: SandboxResources::default(),
                runtime_versions: test_runtime_versions(),
                image_configs: agentenv::types::ImageConfigs::new(),
                custom_extension_params: None,
            },
            manifest,
        )
        .await?;

    fixture
        .client
        .delete_object()
        .bucket(&fixture.bucket)
        .key(prefixed_key(
            prefix,
            &format!("managed-layers/{rootfs_digest}"),
        ))
        .send()
        .await?;

    let error = resolver
        .resolve(Arc::new(stored))
        .await
        .expect_err("resolve should fail when a managed layer is missing");
    match error {
        RepositoryError::ArtifactNotFound { artifact } => {
            assert!(artifact.contains("managed layer"));
            assert!(artifact.contains(&rootfs_digest));
        }
        other => panic!("expected ArtifactNotFound, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_oss_delete_by_alias_removes_manifest_and_listing() -> Result<()> {
    let fixture = MinioFixture::start().await?;
    let workspace = TempDir::new()?;
    let prefix = "snapshots/delete-alias";
    let oss = test_oss_config(&fixture, prefix);
    ensure_test_config()?;

    let (repository, _) = OssBackend::new(&oss, workspace.path().join("oss-cache"))?.into_parts();
    let artifacts_root = workspace.path().join("local-artifacts");
    let (_, _, manifest) = write_built_artifacts(&artifacts_root)?;
    let alias = SnapshotAlias::parse("delete-me").expect("alias should parse");
    let snapshot_id = SnapshotId::generate();

    let stored = repository
        .publish(
            SnapshotPublishMetadata {
                id: snapshot_id.clone(),
                alias: Some(alias.clone()),
                source: SnapshotPublishSource::Template,
                context: agentenv::snapshot::CommandContext::default(),
                startup: None,
                resources: SandboxResources::default(),
                runtime_versions: test_runtime_versions(),
                image_configs: agentenv::types::ImageConfigs::new(),
                custom_extension_params: None,
            },
            manifest,
        )
        .await?;

    repository.delete(alias.as_ref()).await?;

    assert!(repository.get(alias.as_ref()).await?.is_none());
    assert_eq!(repository.resolve_alias(alias.as_ref()).await?, None);
    assert!(repository
        .list(SnapshotListFilter::matches_all())
        .await?
        .is_empty());
    assert!(
        !fixture
            .object_exists(&prefixed_key(
                prefix,
                &format!(
                    "artifacts/{}/{}",
                    stored.id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state
                )
            ))
            .await?
    );

    Ok(())
}
