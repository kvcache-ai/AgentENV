//! Committed-snapshot rootfs image export: load the record, resolve the
//! target, and publish the rootfs layer stack plus a minimal OCI config as a
//! plain OCI image through `regctl`. Attached drives and memory snapshots
//! never participate.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;

use super::regctl::Regctl;
use super::target::{
    resolve_snapshot_image_target, snapshot_managed_publication_for_target, SnapshotImageTarget,
};
use crate::cfg::{ConfigManager, SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind};
use crate::digest;
use crate::snapshot::repository::backends::common::acr::{
    build_oci_image_manifest, host_architecture_for_oci, minimal_oci_config_blob, OciDescriptor,
    SourceRegistryRepository,
};
use crate::snapshot::repository::backends::{oss, posixfs};
use crate::snapshot::repository::{RepositoryError, RepositoryResult, SnapshotRepository};
use crate::snapshot::{CommittedSnapshot, ManagedLayer, OverlaybdLayerRef, SnapshotId};

/// Outcome of one snapshot image export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotImageResult {
    pub image_ref: String,
    pub manifest_digest: String,
    /// True when the target already carried the identical image; nothing was
    /// transferred.
    pub reused: bool,
}

/// Where managed rootfs layer bytes live, per `snapshot.repository_backend`.
enum ManagedLayerLocator {
    PosixFs { root: PathBuf },
    Oss { client: Arc<oss::OssClient> },
}

fn layer_digest_size(layer: &OverlaybdLayerRef) -> (&str, u64) {
    match layer {
        OverlaybdLayerRef::External(external) => (&external.digest, external.size),
        OverlaybdLayerRef::Managed(managed) => (&managed.digest, managed.size),
    }
}

/// Orchestrates committed-snapshot rootfs image export.
pub struct SnapshotImageService {
    repository: Arc<dyn SnapshotRepository>,
    layers: ManagedLayerLocator,
    regctl: Regctl,
}

impl SnapshotImageService {
    /// Builds the service from the global config, intentionally skipping the
    /// node-runtime resolver, artifact cache, layer store, and P2P machinery.
    pub fn from_global_config(regctl_binary: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let config = ConfigManager::global_config();
        let (repository, layers): (Arc<dyn SnapshotRepository>, ManagedLayerLocator) =
            match config.snapshot.repository_backend {
                SnapshotRepositoryBackendKind::PosixFs => {
                    let posix = config.backend.posix_fs.as_ref().context(
                        "backend.posix_fs config is required when repository_backend = posix_fs",
                    )?;
                    let root = posix.snapshot_store.join("repository");
                    let repository = Arc::new(posixfs::PosixFsSnapshotRepository::new(
                        Arc::new(posixfs::PosixFsCatalogStore::new(root.clone())),
                        Arc::new(posixfs::PosixFsArtifactStore::new(root.clone())),
                    ));
                    (repository, ManagedLayerLocator::PosixFs { root })
                }
                SnapshotRepositoryBackendKind::Oss => {
                    let oss_config =
                        config.backend.oss.as_ref().context(
                            "backend.oss config is required when repository_backend = oss",
                        )?;
                    let policy = if config.snapshot.image_publish.enabled {
                        SnapshotImageStoragePolicy::SourceRegistry
                    } else {
                        SnapshotImageStoragePolicy::ObjectStorage
                    };
                    let config = oss::NormalizedOssConfig::new(oss_config, policy)?;
                    let client = Arc::new(oss::OssClient::new(
                        config.bucket().to_string(),
                        config.endpoint().to_string(),
                        config.region().to_string(),
                        config.prefix().to_string(),
                        config.credential_source(),
                    )?);
                    let repository = Arc::new(oss::OssSnapshotRepository::new(
                        Arc::clone(&client),
                        config.snapshot_image_storage(),
                    ));
                    (repository, ManagedLayerLocator::Oss { client })
                }
            };
        Ok(Self {
            repository,
            layers,
            regctl: Regctl::new(regctl_binary),
        })
    }

    /// Exports one snapshot rootfs by ID or alias, rejecting unknown snapshots
    /// and records without committed artifacts (unfinished template builds).
    pub async fn export_rootfs_image(
        &self,
        id_or_alias: &str,
        target_repository: Option<&str>,
        tag: Option<&str>,
    ) -> RepositoryResult<SnapshotImageResult> {
        let record = self.repository.get(id_or_alias).await?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id_or_alias.to_string(),
            }
        })?;
        let committed =
            record
                .committed
                .as_ref()
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!(
                        "snapshot '{id_or_alias}' has no committed artifacts to export yet"
                    ),
                })?;
        self.publish_committed_rootfs(&record.id, committed, target_repository, tag)
            .await
    }

    /// Publishes one committed rootfs. The deterministic manifest digest
    /// decides: identical digest at the target ref ⇒ `reused`, different ⇒
    /// conflict rejected, missing ⇒ upload only the blobs the target lacks,
    /// then `manifest put`.
    async fn publish_committed_rootfs(
        &self,
        snapshot_id: &SnapshotId,
        committed: &CommittedSnapshot,
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

        // The minimal config and the manifest are deterministic per snapshot
        // and tag, so the manifest digest identifies the exact image.
        let (config_bytes, config_digest, config_size) =
            minimal_oci_config_blob(host_architecture_for_oci())?;
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
        layer: &OverlaybdLayerRef,
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
            OverlaybdLayerRef::Managed(managed) => {
                let path = self.materialize_managed_layer(managed, tempdir).await?;
                self.regctl
                    .blob_put(target_repository, layer_digest, &path)
                    .await
            }
            OverlaybdLayerRef::External(external) => {
                // Committed snapshots record OSS-managed layers without a
                // local file as external refs carrying the synthetic
                // managed-layers s3 URL; their bytes live in the
                // managed-layer store, addressed by digest.
                if external.repo_blob_url.starts_with("s3://") {
                    let managed = ManagedLayer {
                        digest: external.digest.clone(),
                        size: external.size,
                        uuid: None,
                    };
                    let path = self.materialize_managed_layer(&managed, tempdir).await?;
                    return self
                        .regctl
                        .blob_put(target_repository, layer_digest, &path)
                        .await;
                }
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

    /// Locates one managed rootfs layer locally: POSIX layers are validated
    /// in place under the repository root; OSS objects download to `tempdir`.
    /// Both branches verify the layer size against the committed record.
    async fn materialize_managed_layer(
        &self,
        layer: &ManagedLayer,
        tempdir: &Path,
    ) -> RepositoryResult<PathBuf> {
        match &self.layers {
            ManagedLayerLocator::PosixFs { root } => {
                let path =
                    posixfs::PosixFsSnapshotArtifactLayout::managed_layer_path(root, &layer.digest);
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
            ManagedLayerLocator::Oss { client } => {
                let key = oss::OssSnapshotArtifactLayout::managed_layer_key(&layer.digest);
                let name = layer.digest.replace([':', '/'], "_");
                let destination = tempdir.join(format!("{name}.overlaybd.commit"));
                let download = client.get_to_file(&key, &destination);
                materialize_downloaded_layer(layer, &destination, download).await
            }
        }
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

/// Completes one managed layer download: maps object-store errors to
/// repository errors, verifies the downloaded size, and returns the path.
async fn materialize_downloaded_layer(
    layer: &ManagedLayer,
    destination: &Path,
    download: impl std::future::Future<Output = anyhow::Result<u64>>,
) -> RepositoryResult<PathBuf> {
    let downloaded = download.await.map_err(|error| {
        if oss::OssClient::is_not_found_error(&error) {
            RepositoryError::ManagedLayerNotFound {
                digest: layer.digest.clone(),
            }
        } else {
            RepositoryError::backend(format!("download managed layer '{}'", layer.digest), error)
        }
    })?;
    if downloaded != layer.size {
        return Err(RepositoryError::IntegrityMismatch {
            artifact: format!("managed layer '{}'", layer.digest),
            expected: layer.size.to_string(),
            actual: downloaded.to_string(),
        });
    }
    Ok(destination.to_path_buf())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::super::regctl::fixture::{
        argv_lines, blob_path, external_ref, install_fake_regctl, managed_ref, seed_blob,
        seed_manifest, stored_manifest,
    };
    use super::super::target::SnapshotImageTargetError;
    use super::*;
    use crate::snapshot::mock::MockSnapshotRepository;
    use crate::snapshot::repository::backends::posixfs::PosixFsSnapshotArtifactLayout;
    use crate::snapshot::{
        rootfs_snapshot_image_tag, PersistedDiskImagePublication, SnapshotRecord,
    };

    const TARGET_REPOSITORY: &str = "reg.example/team/newapp";
    const TARGET_REF: &str = "reg.example/team/newapp:snap-1";
    const SRC_URL: &str = "https://reg.example/v2/team/app/blobs";
    const CROSS_URL: &str = "https://other.example/v2/team/app/blobs";
    const LEGACY_URL: &str = "https://reg.example/v2/legacy/app/blobs";

    fn posix_service(root: PathBuf, regctl: impl Into<PathBuf>) -> SnapshotImageService {
        SnapshotImageService {
            repository: Arc::new(MockSnapshotRepository),
            layers: ManagedLayerLocator::PosixFs { root },
            regctl: Regctl::new(regctl),
        }
    }

    fn seed_posix_layer(root: &Path, digest: &str, bytes: &[u8]) -> PathBuf {
        let path = PosixFsSnapshotArtifactLayout::managed_layer_path(root, digest);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        path
    }

    fn committed_with_layers(layers: Vec<OverlaybdLayerRef>) -> CommittedSnapshot {
        let mut committed = CommittedSnapshot::mock();
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
        committed: &CommittedSnapshot,
    ) -> RepositoryResult<SnapshotImageResult> {
        service
            .publish_committed_rootfs(
                &SnapshotId::generate(),
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
        let repository = posixfs::PosixFsSnapshotRepository::new(
            Arc::new(posixfs::PosixFsCatalogStore::new(root.clone())),
            Arc::new(posixfs::PosixFsArtifactStore::new(root.clone())),
        );
        let uncommitted =
            SnapshotRecord::template_waiting(SnapshotId::generate(), None, Default::default());
        let lookup = uncommitted.id.to_string();
        repository.create(uncommitted).await.unwrap();
        let service = SnapshotImageService {
            repository: Arc::new(repository),
            layers: ManagedLayerLocator::PosixFs { root },
            regctl: Regctl::new("/nonexistent/regctl"),
        };

        let error = service
            .export_rootfs_image("missing", None, None)
            .await
            .unwrap_err();
        assert!(matches!(error, RepositoryError::SnapshotNotFound { .. }));
        let error = service
            .export_rootfs_image(&lookup, None, None)
            .await
            .unwrap_err();
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
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
            .publish_committed_rootfs(&snapshot_id, &committed, None, None)
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

        let result = publish(&service, &committed).await.unwrap();

        assert!(!result.reused);
        assert_eq!(result.image_ref, TARGET_REF);
        let (body, stored_digest) = stored_manifest(dir.path(), TARGET_REF);
        assert_eq!(result.manifest_digest, stored_digest);
        let manifest: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let config_digest = manifest["config"]["digest"].as_str().unwrap();
        for digest in digests.iter().map(String::as_str).chain([config_digest]) {
            assert!(blob_path(dir.path(), TARGET_REPOSITORY, digest).exists());
        }
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
    async fn s3_external_layer_exports_from_managed_layer_store() {
        let dir = TempDir::new().unwrap();
        let service = posix_service(
            dir.path().join("repository"),
            install_fake_regctl(dir.path()),
        );
        let layer = external_ref(b"oss managed bytes", "s3://bucket/prefix/managed-layers");
        let digest = layer_digest_size(&layer).0.to_string();
        seed_posix_layer(
            &dir.path().join("repository"),
            &digest,
            b"oss managed bytes",
        );
        let committed = committed_with_layers(vec![layer]);

        let result = publish(&service, &committed).await.unwrap();

        assert!(!result.reused);
        assert_eq!(
            fs::read(blob_path(dir.path(), TARGET_REPOSITORY, &digest)).unwrap(),
            b"oss managed bytes"
        );
        let argv = argv_lines(dir.path());
        assert!(argv.contains(&format!(
            "argv:blob put {TARGET_REPOSITORY} --digest {digest}"
        )));
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
        let mut committed = CommittedSnapshot::mock();
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

        // s3-marked OSS-managed layers are not registry sources.
        committed.rootfs_layers = vec![
            external_ref(b"a", SRC_URL),
            external_ref(b"oss", "s3://bucket/prefix/managed-layers"),
        ];
        let mixed = resolve_snapshot_image_target(&snapshot_id, &committed, None, None).unwrap();
        assert_eq!(
            mixed.image_ref(),
            format!("reg.example/team/app:snapshot-{snapshot_id}")
        );
        committed.rootfs_layers = vec![external_ref(b"oss", "s3://bucket/prefix/managed-layers")];
        assert!(matches!(
            resolve_snapshot_image_target(&snapshot_id, &committed, None, None),
            Err(SnapshotImageTargetError::CannotInfer { .. })
        ));

        for layers in [
            vec![managed_ref(b"managed")],
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
