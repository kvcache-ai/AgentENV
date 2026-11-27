//! Committed-snapshot rootfs image export: load the record, resolve the
//! target, and publish the rootfs layer stack plus the snapshot runtime OCI
//! config as a plain OCI image through `regctl`. Attached drives and memory
//! snapshots never participate.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::regctl::Regctl;
use super::target::{
    resolve_snapshot_image_target, snapshot_managed_publication_for_target, SnapshotImageTarget,
};
use crate::cfg::ConfigManager;
use crate::digest;
use crate::layer::{LayerRef, ManagedLayerRef};
use crate::repository::{
    layer_chain_for_refs, open_from_config as open_repository_from_config, BlobKey, BlobLocation,
    LayerRole, Repository, RepositoryError, RepositoryResult,
};
use crate::snapshot::acr::{
    build_oci_image_manifest, host_architecture_for_oci, snapshot_oci_config_blob, OciDescriptor,
    SnapshotOciConfigInput, SourceRegistryRepository,
};
use crate::snapshot::{SnapshotId, SnapshotManifest};

/// Outcome of one snapshot image export.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SnapshotImageResult {
    pub image_ref: String,
    pub manifest_digest: String,
    /// True when the target already carried the identical image; nothing was
    /// transferred.
    pub reused: bool,
}

fn layer_digest_size(layer: &LayerRef) -> (&str, u64) {
    match layer {
        LayerRef::External(external) => (&external.digest, external.size),
        LayerRef::Managed(managed) => (&managed.digest, managed.size),
    }
}

/// Orchestrates committed-snapshot rootfs image export.
pub struct SnapshotImageService {
    meta: Option<agentenv_metadata::MetaClient>,
    repository: Arc<dyn Repository>,
    pins: Option<crate::metadata::NodePins>,
    regctl: Regctl,
}

impl SnapshotImageService {
    /// Builds the service from an explicit [`AppConfig`], intentionally skipping
    /// the node-runtime resolver, artifact cache, layer store, and P2P machinery.
    pub fn from_config(
        config: &crate::cfg::AppConfig,
        meta: agentenv_metadata::MetaClient,
    ) -> anyhow::Result<Self> {
        let repository = open_repository_from_config(config)?;
        let pins = crate::metadata::NodePins::ephemeral(meta.clone(), "aenv-snapshot");
        Ok(Self {
            meta: Some(meta),
            repository,
            pins: Some(pins),
            regctl: Regctl::new(config.resolved_regctl_binary()),
        })
    }

    /// Builds the service from the global config, intentionally skipping the
    /// node-runtime resolver, artifact cache, layer store, and P2P machinery.
    pub fn from_global_config(
        meta: agentenv_metadata::MetaClient,
        regctl_binary: impl Into<PathBuf>,
    ) -> anyhow::Result<Self> {
        let config = ConfigManager::global_config();
        let repository = open_repository_from_config(config)?;
        let pins = crate::metadata::NodePins::ephemeral(meta.clone(), "aenv-snapshot");
        Ok(Self {
            meta: Some(meta),
            repository,
            pins: Some(pins),
            regctl: Regctl::new(regctl_binary),
        })
    }

    pub fn build_image_config(
        &self,
        record: &crate::snapshot::SnapshotRecord,
        image_index: usize,
    ) -> RepositoryResult<overlaybd::config::ImageConfig> {
        use overlaybd::config::DownloadConfig;
        let manifest = record
            .manifest
            .as_ref()
            .ok_or_else(|| RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' has no committed images", record.id),
            })?;
        let layers = Self::selected_layers(manifest, image_index)?;
        let mut image = layer_chain_for_refs(self.repository.as_ref(), layers, LayerRole::Rootfs)?
            .to_runtime_config();
        image.download_override = Some(DownloadConfig {
            enable: false,
            ..Default::default()
        });
        overlaybd::config::validate_image_config(&image).map_err(|error| {
            RepositoryError::InvalidRequest {
                reason: format!("invalid snapshot image configuration: {error}"),
            }
        })?;
        Ok(image)
    }

    pub async fn record(
        &self,
        reference: &str,
    ) -> RepositoryResult<crate::snapshot::SnapshotRecord> {
        let meta = self
            .meta
            .as_ref()
            .ok_or_else(|| RepositoryError::Unsupported {
                feature: "snapshot catalog lookup".to_owned(),
            })?;
        crate::snapshot::catalog::get_snapshot(meta, self.repository.as_ref(), reference, false)
            .await?
            .ok_or_else(|| RepositoryError::SnapshotNotFound {
                lookup: reference.to_owned(),
            })
    }

    pub async fn export_image(
        &self,
        reference: &str,
        image_index: usize,
        target_repository: Option<&str>,
        tag: Option<&str>,
    ) -> RepositoryResult<SnapshotImageResult> {
        let record = self.record(reference).await?;
        let committed =
            record
                .manifest
                .as_ref()
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!(
                        "snapshot '{reference}' has no committed artifacts to export yet"
                    ),
                })?;
        let mut selected = committed.clone();
        let mut config = record.config;
        if image_index != 0 {
            selected.rootfs_layers = Self::selected_layers(committed, image_index)?.to_vec();
            config.image_configs = crate::types::ImageConfigs::new();
            config.context = crate::snapshot::CommandContext::default();
        }
        if let Some(pins) = &self.pins {
            pins.pin_snapshot(&record.id).await?;
        }
        let result = self
            .publish_committed_rootfs(&record.id, &config, &selected, target_repository, tag)
            .await;
        if let Some(pins) = &self.pins {
            pins.unpin_snapshot_and_wait(&record.id).await;
        }
        result
    }

    fn selected_layers(manifest: &SnapshotManifest, index: usize) -> RepositoryResult<&[LayerRef]> {
        if index == 0 {
            return Ok(&manifest.rootfs_layers);
        }
        let Some(crate::snapshot::CommittedAttachedDrive::Overlaybd { layers, .. }) =
            manifest.attached_drives.get(index - 1)
        else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot has no disk image {index}"),
            });
        };
        Ok(layers)
    }

    /// Publishes one committed rootfs. The deterministic manifest digest
    /// decides: identical digest at the target ref ⇒ `reused`, different ⇒
    /// conflict rejected, missing ⇒ upload only the blobs the target lacks,
    /// then `manifest put`.
    async fn publish_committed_rootfs(
        &self,
        snapshot_id: &SnapshotId,
        config: &crate::snapshot::SnapshotConfig,
        committed: &SnapshotManifest,
        target_repository: Option<&str>,
        tag: Option<&str>,
    ) -> RepositoryResult<SnapshotImageResult> {
        let target = resolve_snapshot_image_target(snapshot_id, committed, target_repository, tag)
            .map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        if committed.rootfs_layers.is_empty() {
            return Err(RepositoryError::InvalidRequest {
                reason: "committed snapshot has no rootfs layers to export".to_string(),
            });
        }

        // The snapshot config and the manifest are deterministic per snapshot
        // and tag, so the manifest digest identifies the exact image.
        let config_input =
            SnapshotOciConfigInput::new(&config.context, config.image_configs.rootfs_config());
        let (config_bytes, config_digest, config_size) =
            snapshot_oci_config_blob(host_architecture_for_oci(), config_input)?;
        let descriptors = committed
            .rootfs_layers
            .iter()
            .map(|layer| {
                let (digest, size) = layer_digest_size(layer);
                OciDescriptor::overlaybd_layer(digest.to_string(), size)
            })
            .collect();
        let manifest = build_oci_image_manifest(
            OciDescriptor::config(config_digest.clone(), config_size),
            descriptors,
            &target.tag,
        )?;
        let manifest_digest = digest::sha256_digest(&manifest);

        let image_ref = target.image_ref();
        if let Some(publication) = snapshot_managed_publication_for_target(committed, &target) {
            tracing::warn!(
                snapshot_id = %snapshot_id,
                image_ref = %image_ref,
                manifest_digest = %publication.manifest_digest,
                "target image reference is recorded as a snapshot-managed publication and may be deleted when the snapshot is deleted"
            );
        }
        let target_repository = format!("{}/{}", target.registry, target.repository);
        match self.regctl.manifest_head(&image_ref).await? {
            Some(existing) if existing == manifest_digest => {
                return Ok(SnapshotImageResult {
                    image_ref,
                    manifest_digest,
                    reused: true,
                });
            }
            Some(existing) => {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "target image ref '{image_ref}' already points to manifest {existing}, \
                         refusing to overwrite it with {manifest_digest}; choose another tag"
                    ),
                });
            }
            None => {}
        }

        // Payloads (config, manifest, managed layers) materialize under this
        // tempdir so regctl children can stream them from file descriptors.
        let tempdir = tempfile::tempdir()
            .map_err(|e| RepositoryError::backend("create snapshot image export tempdir", e))?;
        let config_path = write_payload(tempdir.path(), "config.json", &config_bytes).await?;
        let manifest_path = write_payload(tempdir.path(), "manifest.json", &manifest).await?;
        for layer in &committed.rootfs_layers {
            self.ensure_layer_blob(&target, &target_repository, layer, tempdir.path())
                .await?;
        }
        if !self
            .regctl
            .blob_head(&target_repository, &config_digest)
            .await?
        {
            self.regctl
                .blob_put(&target_repository, &config_digest, &config_path)
                .await?;
        }
        self.regctl.manifest_put(&image_ref, &manifest_path).await?;
        Ok(SnapshotImageResult {
            image_ref,
            manifest_digest,
            reused: false,
        })
    }

    /// Ensures one rootfs layer blob exists in the target repository. The
    /// `blob head` probe runs first, so blobs the target already has are
    /// neither materialized nor transferred.
    async fn ensure_layer_blob(
        &self,
        target: &SnapshotImageTarget,
        target_repository: &str,
        layer: &LayerRef,
        tempdir: &Path,
    ) -> RepositoryResult<()> {
        let (layer_digest, _) = layer_digest_size(layer);
        if self
            .regctl
            .blob_head(target_repository, layer_digest)
            .await?
        {
            return Ok(());
        }
        match layer {
            LayerRef::Managed(managed) => {
                let path = self.materialize_managed_layer(managed, tempdir).await?;
                self.regctl
                    .blob_put(target_repository, layer_digest, &path)
                    .await
            }
            LayerRef::External(external) => {
                let source = SourceRegistryRepository::parse(&external.repo_blob_url)?;
                let source_repository = format!("{}/{}", source.registry, source.repository);
                if source.registry != target.registry {
                    self.regctl
                        .blob_pipe(&source_repository, target_repository, layer_digest)
                        .await
                } else if source.repository != target.repository {
                    self.regctl
                        .blob_copy(&source_repository, target_repository, layer_digest)
                        .await
                } else {
                    // The head probe proved the blob absent; same repository
                    // leaves nothing to transfer from.
                    Err(RepositoryError::ArtifactNotFound {
                        artifact: format!(
                            "external rootfs layer blob {layer_digest} in source repository {source_repository}"
                        ),
                    })
                }
            }
        }
    }

    /// Locates one managed rootfs layer locally through [`Repository`]:
    /// in-place local blobs are validated without copying; remote blobs are
    /// downloaded to `tempdir`. Both branches verify the byte size against the
    /// committed record.
    async fn materialize_managed_layer(
        &self,
        layer: &ManagedLayerRef,
        tempdir: &Path,
    ) -> RepositoryResult<PathBuf> {
        let key = BlobKey::Layer {
            digest: layer.digest.clone(),
            role: LayerRole::Rootfs,
        };
        match self.repository.locate_blob(&key)? {
            BlobLocation::Local(path) => {
                let metadata = tokio::fs::metadata(&path).await.map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        RepositoryError::ManagedLayerNotFound {
                            digest: layer.digest.clone(),
                        }
                    } else {
                        RepositoryError::backend(
                            format!(
                                "stat managed layer '{}' at {}",
                                layer.digest,
                                path.display()
                            ),
                            error,
                        )
                    }
                })?;
                if metadata.len() != layer.size {
                    return Err(RepositoryError::IntegrityMismatch {
                        artifact: format!("managed layer '{}' at {}", layer.digest, path.display()),
                        expected: layer.size.to_string(),
                        actual: metadata.len().to_string(),
                    });
                }
                Ok(path)
            }
            BlobLocation::Remote(_) => {
                let name = layer.digest.replace([':', '/'], "_");
                let destination = tempdir.join(format!("{name}.overlaybd.commit"));
                let downloaded = self
                    .repository
                    .fetch_blob(&key, &destination)
                    .await
                    .map_err(|error| match error {
                        RepositoryError::ArtifactNotFound { .. } => {
                            RepositoryError::ManagedLayerNotFound {
                                digest: layer.digest.clone(),
                            }
                        }
                        other => other,
                    })?;
                if downloaded != layer.size {
                    return Err(RepositoryError::IntegrityMismatch {
                        artifact: format!("managed layer '{}'", layer.digest),
                        expected: layer.size.to_string(),
                        actual: downloaded.to_string(),
                    });
                }
                Ok(destination)
            }
        }
    }
}

#[cfg(test)]
fn rootfs_layer_key(digest: &str) -> BlobKey {
    BlobKey::Layer {
        digest: digest.to_owned(),
        role: LayerRole::Rootfs,
    }
}

/// Writes one regctl payload file (config or manifest JSON) into `tempdir`.
async fn write_payload(tempdir: &Path, name: &str, bytes: &[u8]) -> RepositoryResult<PathBuf> {
    let path = tempdir.join(name);
    tokio::fs::write(&path, bytes)
        .await
        .map_err(|e| RepositoryError::backend(format!("write {name} payload"), e))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use serde_json::json;
    use tempfile::TempDir;

    use super::super::regctl::fixture::{
        argv_lines, blob_path, external_ref, install_fake_regctl, managed_ref, seed_blob,
        seed_manifest, stored_manifest,
    };
    use super::super::target::SnapshotImageTargetError;
    use super::*;
    use crate::repository::backends::PosixFsRepository;
    use crate::snapshot::mock::test_metadata;
    use crate::snapshot::{
        rootfs_snapshot_image_tag, CommandContext, PersistedDiskImagePublication,
        SnapshotPlacement, SnapshotRecord, SnapshotSource,
    };

    const TARGET_REPOSITORY: &str = "reg.example/team/newapp";
    const TARGET_REF: &str = "reg.example/team/newapp:snap-1";
    const SRC_URL: &str = "https://reg.example/v2/team/app/blobs";
    const CROSS_URL: &str = "https://other.example/v2/team/app/blobs";
    const LEGACY_URL: &str = "https://reg.example/v2/legacy/app/blobs";

    fn posix_service(root: PathBuf, regctl: impl Into<PathBuf>) -> SnapshotImageService {
        let repository: Arc<dyn Repository> = Arc::new(PosixFsRepository::new(root));
        SnapshotImageService {
            meta: None,
            repository,
            pins: None,
            regctl: Regctl::new(regctl),
        }
    }

    fn seed_posix_layer(root: &Path, digest: &str, bytes: &[u8]) -> PathBuf {
        let path = root.join(rootfs_layer_key(digest).posix_path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        path
    }

    fn committed_with_layers(layers: Vec<LayerRef>) -> SnapshotManifest {
        let mut committed = SnapshotManifest::mock();
        committed.rootfs_layers = layers;
        committed
    }

    fn publication(tag: String, repo_blob_url: &str) -> PersistedDiskImagePublication {
        PersistedDiskImagePublication {
            image_ref: format!("reg.example/legacy/app:{tag}"),
            tag,
            manifest_digest: "sha256:manifest".to_string(),
            repo_blob_url: repo_blob_url.to_string(),
        }
    }

    async fn publish(
        service: &SnapshotImageService,
        committed: &SnapshotManifest,
    ) -> RepositoryResult<SnapshotImageResult> {
        service
            .publish_committed_rootfs(
                &SnapshotId::generate(),
                &crate::snapshot::SnapshotConfig::mock(),
                committed,
                Some(TARGET_REPOSITORY),
                Some("snap-1"),
            )
            .await
    }

    #[tokio::test]
    async fn unknown_or_uncommitted_snapshot_is_rejected() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("repository");
        let repository: Arc<dyn Repository> = Arc::new(PosixFsRepository::new(root));
        let (server, meta) = test_metadata(dir.path(), Arc::clone(&repository)).await;
        let mut uncommitted = SnapshotRecord::mock_ready(SnapshotManifest::default());
        uncommitted.snapshot_type = SnapshotPlacement::Local;
        uncommitted.owner_node_id = Some("node-a".into());
        uncommitted.manifest = None;
        uncommitted.config.source = SnapshotSource::Sandbox {
            source_sandbox_id: "sbx-local".into(),
            source_sandbox_metadata: None,
        };
        let lookup = uncommitted.id.to_string();
        crate::snapshot::catalog::commit_local(&meta, repository.as_ref(), uncommitted)
            .await
            .unwrap();
        let pins = crate::metadata::NodePins::ephemeral(meta.clone(), "test-export");
        let service = SnapshotImageService {
            meta: Some(meta),
            repository,
            pins: Some(pins),
            regctl: Regctl::new("/nonexistent/regctl"),
        };

        let error = service
            .export_image("missing", 0, None, None)
            .await
            .unwrap_err();
        assert!(matches!(error, RepositoryError::SnapshotNotFound { .. }));
        let error = service
            .export_image(&lookup, 0, None, None)
            .await
            .unwrap_err();
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        server.close().await;
    }

    #[tokio::test]
    async fn canonical_legacy_publication_uses_registry_truth() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("repository");
        let service = posix_service(root.clone(), install_fake_regctl(dir.path()));
        let snapshot_id = SnapshotId::generate();
        let publication = publication(rootfs_snapshot_image_tag(&snapshot_id), LEGACY_URL);
        let layer = managed_ref(b"managed bytes");
        seed_posix_layer(&root, layer_digest_size(&layer).0, b"managed bytes");
        let mut committed = committed_with_layers(vec![layer]);
        committed.disk_publications = vec![publication.clone()];

        let result = service
            .publish_committed_rootfs(
                &snapshot_id,
                &crate::snapshot::SnapshotConfig::mock(),
                &committed,
                None,
                None,
            )
            .await
            .unwrap();

        let expected_ref = format!("reg.example/legacy/app:snapshot-{snapshot_id}");
        assert_eq!(result.image_ref, expected_ref);
        assert_ne!(result.image_ref, publication.image_ref);
        let argv = argv_lines(dir.path());
        assert!(argv.contains(&format!("argv:manifest head {expected_ref}")));
        assert!(argv
            .iter()
            .any(|line| line.starts_with("argv:manifest put")));
    }

    #[tokio::test]
    async fn explicit_target_publishes_mixed_stack_end_to_end() {
        let dir = TempDir::new().unwrap();
        let service = posix_service(
            dir.path().join("repository"),
            install_fake_regctl(dir.path()),
        );
        let layers = vec![
            external_ref(b"same-registry layer", SRC_URL),
            managed_ref(b"managed local layer"),
            external_ref(b"cross-registry layer", CROSS_URL),
        ];
        let digests: Vec<String> = layers
            .iter()
            .map(|layer| layer_digest_size(layer).0.to_string())
            .collect();
        seed_blob(
            dir.path(),
            "reg.example/team/app",
            &digests[0],
            b"same-registry layer",
        );
        seed_blob(
            dir.path(),
            "other.example/team/app",
            &digests[2],
            b"cross-registry layer",
        );
        seed_posix_layer(
            &dir.path().join("repository"),
            &digests[1],
            b"managed local layer",
        );
        let committed = committed_with_layers(layers);
        let mut capture_config = crate::snapshot::SnapshotConfig::mock();
        capture_config.context = CommandContext::new(
            HashMap::from([("APP_ENV".to_string(), "snapshot".to_string())]),
            "/workspace",
        );
        capture_config.image_configs.add(
            None::<String>,
            "/",
            json!({
                "Env": ["APP_ENV=source"],
                "WorkingDir": "/source",
                "StopSignal": "SIGTERM"
            }),
        );

        let result = service
            .publish_committed_rootfs(
                &SnapshotId::generate(),
                &capture_config,
                &committed,
                Some(TARGET_REPOSITORY),
                Some("snap-1"),
            )
            .await
            .unwrap();

        assert!(!result.reused);
        assert_eq!(result.image_ref, TARGET_REF);
        let (body, stored_digest) = stored_manifest(dir.path(), TARGET_REF);
        assert_eq!(result.manifest_digest, stored_digest);
        let manifest: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let config_digest = manifest["config"]["digest"].as_str().unwrap();
        for digest in digests.iter().map(String::as_str).chain([config_digest]) {
            assert!(blob_path(dir.path(), TARGET_REPOSITORY, digest).exists());
        }
        let config_bytes =
            fs::read(blob_path(dir.path(), TARGET_REPOSITORY, config_digest)).expect("config blob");
        let config: serde_json::Value = serde_json::from_slice(&config_bytes).unwrap();
        assert_eq!(config["config"]["Env"], json!(["APP_ENV=snapshot"]));
        assert_eq!(config["config"]["WorkingDir"], "/workspace");
        assert_eq!(config["config"]["StopSignal"], "SIGTERM");
        let piped = fs::read(blob_path(dir.path(), TARGET_REPOSITORY, &digests[2])).unwrap();
        assert_eq!(piped, b"cross-registry layer");

        let argv = argv_lines(dir.path());
        for expected in [
            format!("argv:blob copy reg.example/team/app {TARGET_REPOSITORY} {}", digests[0]),
            format!("argv:blob put {TARGET_REPOSITORY} --digest {}", digests[1]),
            format!("argv:blob get other.example/team/app {}", digests[2]),
            format!("argv:manifest put --content-type application/vnd.oci.image.manifest.v1+json {TARGET_REF}"),
        ] {
            assert!(argv.contains(&expected), "missing invocation: {expected}");
        }
    }

    #[tokio::test]
    async fn republish_is_reused_and_conflicting_ref_is_rejected() {
        let dir = TempDir::new().unwrap();
        let service = posix_service(
            dir.path().join("repository"),
            install_fake_regctl(dir.path()),
        );
        let layer = managed_ref(b"managed layer bytes");
        let root = dir.path().join("repository");
        seed_posix_layer(&root, layer_digest_size(&layer).0, b"managed layer bytes");
        let committed = committed_with_layers(vec![layer]);

        let first = publish(&service, &committed).await.unwrap();
        assert!(!first.reused);
        fs::remove_file(dir.path().join("log")).unwrap();

        let second = publish(&service, &committed).await.unwrap();
        assert!(second.reused);
        assert_eq!(second.manifest_digest, first.manifest_digest);
        assert_eq!(
            argv_lines(dir.path()),
            [format!("argv:manifest head {TARGET_REF}")]
        );

        seed_manifest(
            dir.path(),
            TARGET_REF,
            &digest::sha256_digest(b"foreign manifest"),
        );
        fs::remove_file(dir.path().join("log")).unwrap();
        let error = publish(&service, &committed).await.unwrap_err();
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        assert_eq!(
            argv_lines(dir.path()),
            [format!("argv:manifest head {TARGET_REF}")]
        );
    }

    #[test]
    fn resolves_explicit_or_original_repository_target() {
        let snapshot_id = SnapshotId::generate();
        let legacy_tag = rootfs_snapshot_image_tag(&snapshot_id);
        let mut committed = SnapshotManifest::mock();
        committed.disk_publications = vec![publication(legacy_tag, LEGACY_URL)];

        let explicit = resolve_snapshot_image_target(
            &snapshot_id,
            &committed,
            Some("other.example/team/app"),
            None,
        )
        .unwrap();
        assert_eq!(explicit.image_ref(), "other.example/team/app:latest");

        let inferred = resolve_snapshot_image_target(&snapshot_id, &committed, None, None).unwrap();
        assert_eq!(
            inferred.image_ref(),
            format!("reg.example/legacy/app:snapshot-{snapshot_id}")
        );

        let legacy_target = SnapshotImageTarget::parse(
            "reg.example/legacy/app",
            Some(&rootfs_snapshot_image_tag(&snapshot_id)),
        )
        .unwrap();
        assert!(snapshot_managed_publication_for_target(&committed, &legacy_target).is_some());
        let explicit_default_port = SnapshotImageTarget::parse(
            "reg.example:443/legacy/app",
            Some(&rootfs_snapshot_image_tag(&snapshot_id)),
        )
        .unwrap();
        assert!(
            snapshot_managed_publication_for_target(&committed, &explicit_default_port).is_some()
        );
        let release_target =
            SnapshotImageTarget::parse("reg.example/legacy/app", Some("release-1")).unwrap();
        assert!(snapshot_managed_publication_for_target(&committed, &release_target).is_none());

        committed.disk_publications[0].image_ref = format!(
            "reg.example:443/legacy/app:{}",
            rootfs_snapshot_image_tag(&snapshot_id)
        );
        committed.disk_publications[0].repo_blob_url =
            "https://reg.example:443/v2/legacy/app/blobs".to_string();
        assert!(snapshot_managed_publication_for_target(&committed, &legacy_target).is_some());

        committed.disk_publications.clear();
        committed.rootfs_layers = vec![external_ref(b"a", SRC_URL), managed_ref(b"managed")];
        let source = resolve_snapshot_image_target(&snapshot_id, &committed, None, None).unwrap();
        assert_eq!(
            source.image_ref(),
            format!("reg.example/team/app:snapshot-{snapshot_id}")
        );

        for layers in [
            vec![managed_ref(b"managed")],
            vec![external_ref(b"oss", "s3://bucket/prefix/managed-layers")],
            vec![external_ref(b"a", SRC_URL), external_ref(b"b", CROSS_URL)],
        ] {
            committed.rootfs_layers = layers;
            assert!(matches!(
                resolve_snapshot_image_target(&snapshot_id, &committed, None, None),
                Err(SnapshotImageTargetError::CannotInfer { .. })
            ));
        }
    }
}
