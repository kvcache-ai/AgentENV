//! Resolving committed snapshots, and volume backings, into node-local
//! runnable paths. One resolver serves every repository: files a backend can
//! read in place are used where they are; everything else is fetched,
//! P2P-first, into the node-local cache. A missing in-place file is an error,
//! never a reason to fetch it from peers.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};
use overlaybd::config::{ImageConfig, LayerConfig};
use tracing::debug;
use uuid::Uuid;

use super::artifact_cache::{CacheArtifactLease, CacheHandle, LocalArtifactCache};
use super::p2p;
use super::runtime_support::{
    hydrate_runtime_manifest, load_firecracker_manifest_from_path, materialize_image_config_error,
    parse_firecracker_manifest, runtime_image_cache_key, RuntimeImageMaterializer,
};
use super::{
    CommittedAttachedDrive, CommittedSnapshot, ManagedLayer, OverlaybdLayerRef,
    ResolvedAttachedDrive, RunnableSnapshot, RuntimeArtifactLease, SnapshotId, SnapshotRecord,
};
use crate::image::cache::OverlaybdLayerStore;
use crate::p2p::P2pTransport;
use crate::repository::{
    BlobKey, BlobLocation, LayerRole, Repository, RepositoryError, RepositoryResult, SnapshotFile,
};
use crate::snapshot::SandboxSnapshotManifest;

const LAYER_EXISTS_CONCURRENCY: usize = 16;

#[async_trait]
/// Resolves committed snapshot records into node-local runnable paths.
///
/// This trait sits at the boundary between repository truth and launch-time derived state.
/// Implementations consume a committed [`SnapshotRecord`] and may materialize node-local helper files
/// such as runnable overlaybd `image.json` configs for the current node.
///
/// Contract:
///
/// - consumes a [`SnapshotRecord`] whose committed payload is present
/// - returns paths that are directly usable by sandbox / firecracker launch code on the current node
/// - may materialize node-local derived files such as runnable `image.json`
/// - must not mutate committed repository truth when generating node-local cache files
///
/// Backend guidance:
///
/// - runtime cache directories should be treated as node-local derived state
/// - shared repository storage and node-local runtime cache should be separated whenever possible
/// - concurrent resolves on one node should prefer atomic cache writes so readers never observe
///   partially written derived configs
pub trait SnapshotRuntimeResolver: Send + Sync {
    /// Resolves one committed snapshot record into a runtime-ready view for the current node.
    async fn resolve(&self, snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot>;
}

/// Resolves committed snapshots stored in one repository.
pub(crate) struct RepositoryRuntimeResolver {
    repository: Arc<dyn Repository>,
    cache: Arc<LocalArtifactCache>,
    image_materializer: RuntimeImageMaterializer,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
    #[cfg(test)]
    test_consume_enabled: Option<bool>,
}

impl RepositoryRuntimeResolver {
    pub(crate) fn new(
        repository: Arc<dyn Repository>,
        cache: Arc<LocalArtifactCache>,
        runtime_root: PathBuf,
        store: Arc<dyn OverlaybdLayerStore>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Self {
        Self {
            repository,
            cache,
            image_materializer: RuntimeImageMaterializer::new(runtime_root, store),
            p2p_transport,
            #[cfg(test)]
            test_consume_enabled: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_consume_enabled(mut self, enabled: bool) -> Self {
        self.test_consume_enabled = Some(enabled);
        self
    }
}

#[async_trait]
impl SnapshotRuntimeResolver for RepositoryRuntimeResolver {
    async fn resolve(&self, snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot> {
        let id = &snapshot.id;
        let committed =
            snapshot
                .committed
                .as_ref()
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{id}' is not ready"),
                })?;
        let mut handles: Vec<CacheHandle> = Vec::new();
        let vm_state_path = self.vm_state_path(id, &mut handles).await?;
        let committed_manifest = self.load_firecracker_manifest(id).await?;

        let memory_layers = committed
            .memory_layers
            .iter()
            .cloned()
            .map(OverlaybdLayerRef::Managed)
            .collect::<Vec<_>>();
        let mem_image_config_path = self
            .materialize_and_pin(
                &memory_layers,
                &self.image_materializer.memory_image_config_path(id),
                &format!("memory for snapshot '{id}'"),
                &runtime_image_cache_key(id, "memory/image.json"),
                LayerRole::Memory,
                &mut handles,
            )
            .await?;
        let rootfs_image_config_path = self
            .materialize_and_pin(
                &committed.rootfs_layers,
                &self.image_materializer.rootfs_image_config_path(id),
                &format!("rootfs for snapshot '{id}'"),
                &runtime_image_cache_key(id, "rootfs/image.json"),
                LayerRole::Rootfs,
                &mut handles,
            )
            .await?;
        let attached_drives = self
            .resolve_attached_drives(id, committed, &mut handles)
            .await?;

        let mut runtime_manifest = hydrate_runtime_manifest(
            committed_manifest,
            vm_state_path,
            mem_image_config_path,
            rootfs_image_config_path,
            &attached_drives,
        )?;
        runtime_manifest.memory_startup_pack = self.startup_pack(id, committed).await;

        // Runtime artifacts are protected by the sandbox start-window lease (over
        // local-only commits) + the orchestrator running set; the resolved-handle
        // needs no separate local image ref pin.
        let cache_lease: Arc<dyn RuntimeArtifactLease> =
            Arc::new(CacheArtifactLease { _handles: handles });
        debug!(snapshot_id = %id, "resolved snapshot to local runnable paths");
        Ok(RunnableSnapshot::new(
            (*snapshot).clone(),
            runtime_manifest,
            cache_lease,
        ))
    }
}

impl RepositoryRuntimeResolver {
    async fn vm_state_path(
        &self,
        id: &SnapshotId,
        handles: &mut Vec<CacheHandle>,
    ) -> RepositoryResult<PathBuf> {
        let key = BlobKey::Snapshot(id.clone(), SnapshotFile::VmState);
        let name = SnapshotFile::VmState.name();
        if let BlobLocation::Local(path) = self.repository.locate_blob(&key)? {
            if !path.exists() {
                return Err(RepositoryError::ArtifactNotFound {
                    artifact: format!("vm state at {}", path.display()),
                });
            }
            return Ok(path);
        }
        let cache_key = format!("artifacts/{id}/{name}");
        let p2p_key = p2p::fixed_artifact_key(id, name);
        let handle = self
            .cache
            .ensure_cached(&cache_key, |dest| {
                let p2p_key = p2p_key.clone();
                let key = &key;
                async move {
                    if let Some(transport) = self.p2p_transport.as_ref() {
                        match p2p::fetch_artifact(transport, &p2p_key, &dest).await {
                            Ok(size) => return Ok(size),
                            Err(error) => {
                                debug!(
                                    key = %p2p_key,
                                    error = %error,
                                    "P2P vm_state fetch failed; using repository fallback"
                                );
                            }
                        }
                    }
                    Ok(self.repository.fetch_blob(key, &dest).await?)
                }
            })
            .await
            .map_err(|error| RepositoryError::ArtifactNotFound {
                artifact: format!("vm state artifact for snapshot '{id}': {error}"),
            })?;
        let path = handle.path().to_path_buf();
        handles.push(handle);
        Ok(path)
    }

    async fn load_firecracker_manifest(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<SandboxSnapshotManifest> {
        let key = BlobKey::Snapshot(id.clone(), SnapshotFile::FirecrackerManifest);
        let name = SnapshotFile::FirecrackerManifest.name();
        if let BlobLocation::Local(path) = self.repository.locate_blob(&key)? {
            return load_firecracker_manifest_from_path(&path).await;
        }
        if let Some(transport) = self.p2p_transport.as_ref() {
            let p2p_key = p2p::fixed_artifact_key(id, name);
            match p2p::fetch_artifact_bytes(transport, &p2p_key).await {
                Ok(bytes) => match parse_firecracker_manifest(&bytes, &p2p_key) {
                    Ok(manifest) => return Ok(manifest),
                    Err(error) => {
                        debug!(
                            key = %p2p_key,
                            error = %error,
                            "P2P firecracker manifest parse failed; using repository fallback"
                        );
                    }
                },
                Err(error) => {
                    debug!(
                        key = %p2p_key,
                        error = %error,
                        "P2P firecracker manifest fetch failed; using repository fallback"
                    );
                }
            }
        }
        let bytes = self.repository.read_blob(&key).await?;
        parse_firecracker_manifest(&bytes, format!("{name} of snapshot '{id}'"))
    }

    /// The startup manifest is used only when this node consumes it and the
    /// snapshot has one: in place when readable here, otherwise by URL.
    async fn startup_pack(
        &self,
        id: &SnapshotId,
        committed: &CommittedSnapshot,
    ) -> Option<crate::snapshot::ResolvedStartupPack> {
        let consume_enabled = crate::cfg::ConfigManager::global_config()
            .snapshot
            .memory_startup_pack
            .consume_enabled;
        #[cfg(test)]
        let consume_enabled = self.test_consume_enabled.unwrap_or(consume_enabled);
        let info = committed.memory_startup.as_ref();
        let key = BlobKey::Snapshot(id.clone(), SnapshotFile::StartupManifest);
        let url = match self.repository.locate_blob(&key).ok()? {
            BlobLocation::Remote(url) => url,
            BlobLocation::Local(path) => {
                let present = tokio::fs::metadata(&path)
                    .await
                    .is_ok_and(|metadata| metadata.is_file());
                return present
                    .then(|| {
                        crate::snapshot::startup_pack::resolve_local_startup_pack_ref(
                            info,
                            consume_enabled,
                            path,
                        )
                    })
                    .flatten();
            }
        };
        crate::snapshot::startup_pack::resolve_startup_pack_ref(info, consume_enabled, || url)
    }

    async fn resolve_attached_drives(
        &self,
        id: &SnapshotId,
        committed: &CommittedSnapshot,
        handles: &mut Vec<CacheHandle>,
    ) -> RepositoryResult<Vec<ResolvedAttachedDrive>> {
        let mut drives = Vec::with_capacity(committed.attached_drives.len());
        for drive in &committed.attached_drives {
            let CommittedAttachedDrive::Overlaybd {
                drive_id,
                layers,
                read_only,
                virtual_size,
                mount_path,
                sub_path,
            } = drive;
            if *virtual_size == 0 {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("attached drive '{drive_id}' virtual_size must be non-zero"),
                });
            }
            let image_config_path = self
                .materialize_and_pin(
                    layers,
                    &self
                        .image_materializer
                        .drive_image_config_path(id, drive_id),
                    &format!("attached drive '{drive_id}' for snapshot '{id}'"),
                    &runtime_image_cache_key(id, &format!("drives/{drive_id}/image.json")),
                    LayerRole::AttachedDrive,
                    handles,
                )
                .await?;
            drives.push(ResolvedAttachedDrive::Overlaybd {
                drive_id: drive_id.clone(),
                image_config_path,
                read_only: *read_only,
                virtual_size: *virtual_size,
                mount_path: crate::types::normalize_mount_path_for_drive(
                    drive_id,
                    mount_path.clone(),
                )
                .unwrap_or_else(|_| crate::types::ExtraDrive::default_mount_path(drive_id)),
                sub_path: sub_path.clone(),
            });
        }
        Ok(drives)
    }

    /// Materializes an image config from `layers` into the node-local cache
    /// and pins it for the sandbox's lifetime.
    async fn materialize_and_pin(
        &self,
        layers: &[OverlaybdLayerRef],
        destination: &Path,
        label: &str,
        cache_key: &str,
        role: LayerRole,
        handles: &mut Vec<CacheHandle>,
    ) -> RepositoryResult<PathBuf> {
        // Snapshots without a persisted memory dump still get a stable
        // runtime config path.
        if role != LayerRole::Memory && layers.is_empty() {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("{label} has no layers"),
            });
        }
        let handle = self
            .cache
            .ensure_cached_at(cache_key, destination.to_path_buf(), |dest| async move {
                let path = self
                    .materialize_image_config(layers, &dest, label, role)
                    .await
                    .map_err(anyhow::Error::new)?;
                tokio::fs::metadata(&path)
                    .await
                    .map(|metadata| metadata.len())
                    .map_err(|error| {
                        anyhow::Error::new(RepositoryError::backend(
                            format!("stat {label} image config '{}'", path.display()),
                            error,
                        ))
                    })
            })
            .await
            .map_err(|error| materialize_image_config_error(label, error))?;
        let path = handle.path().to_path_buf();
        handles.push(handle);
        Ok(path)
    }

    /// Checks that every managed layer is stored, then writes the config.
    async fn materialize_image_config(
        &self,
        layers: &[OverlaybdLayerRef],
        destination: &Path,
        label: &str,
        role: LayerRole,
    ) -> RepositoryResult<PathBuf> {
        let managed_layers = layers
            .iter()
            .enumerate()
            .filter_map(|(index, layer)| match layer {
                OverlaybdLayerRef::Managed(layer) => Some((index, layer.digest.clone())),
                OverlaybdLayerRef::External(_) => None,
            })
            .collect::<Vec<_>>();
        let repository = Arc::clone(&self.repository);
        validate_managed_layers(&managed_layers, label, move |digest| {
            let repository = Arc::clone(&repository);
            async move { repository.has_blob(&BlobKey::Layer { digest, role }).await }
        })
        .await?;

        let managed_repo_blob_url = self.repository.managed_layers_repo_blob_url();
        self.image_materializer
            .materialize_image_config(
                layers,
                destination,
                label,
                managed_repo_blob_url.as_deref(),
                None,
                |_, layer| async move { managed_layer_config(self.repository.as_ref(), layer, role) },
            )
            .await
    }
}

/// Writes a volume's runtime image config for its stored backing layers.
pub(crate) async fn materialize_volume_backing(
    repository: &dyn Repository,
    layers: &[OverlaybdLayerRef],
    destination: &Path,
) -> RepositoryResult<PathBuf> {
    let managed_repo_blob_url = repository.managed_layers_repo_blob_url();
    let image_config = ImageConfig {
        lowers: layers
            .iter()
            .map(|layer| {
                Ok(match layer {
                    OverlaybdLayerRef::Managed(layer) => LayerConfig {
                        repo_blob_url: managed_repo_blob_url.clone().unwrap_or_default(),
                        ..managed_layer_config(repository, layer.clone(), LayerRole::Rootfs)?
                    },
                    OverlaybdLayerRef::External(layer) => LayerConfig {
                        repo_blob_url: layer.repo_blob_url.clone(),
                        digest: layer.digest.clone(),
                        size: layer.size,
                        ..LayerConfig::default()
                    },
                })
            })
            .collect::<RepositoryResult<_>>()?,
        ..ImageConfig::default()
    };
    let parent = destination
        .parent()
        .ok_or_else(|| RepositoryError::Backend {
            message: format!(
                "volume image config '{}' has no parent",
                destination.display()
            ),
            source: None,
        })?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        RepositoryError::backend(
            format!("create volume image config dir '{}'", parent.display()),
            error,
        )
    })?;
    let bytes = serde_json::to_vec_pretty(&image_config)
        .map_err(|error| RepositoryError::backend("serialize volume image config", error))?;
    let temporary = destination.with_extension(format!("tmp-{}", Uuid::now_v7().simple()));
    if let Err(error) = tokio::fs::write(&temporary, bytes).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(RepositoryError::backend(
            format!(
                "write temporary volume image config '{}'",
                temporary.display()
            ),
            error,
        ));
    }
    if let Err(error) = tokio::fs::rename(&temporary, destination).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(RepositoryError::backend(
            format!("install volume image config '{}'", destination.display()),
            error,
        ));
    }
    Ok(destination.to_path_buf())
}

/// A managed layer read in place when the repository allows it; otherwise
/// overlaybd reads it by digest from the repository's `repoBlobUrl`.
fn managed_layer_config(
    repository: &dyn Repository,
    layer: ManagedLayer,
    role: LayerRole,
) -> RepositoryResult<LayerConfig> {
    let key = BlobKey::Layer {
        digest: layer.digest.clone(),
        role,
    };
    let file = match repository.locate_blob(&key)? {
        BlobLocation::Local(path) => path.display().to_string(),
        BlobLocation::Remote(_) => String::new(),
    };
    Ok(LayerConfig {
        file,
        digest: layer.digest,
        size: layer.size,
        uuid: layer.uuid.unwrap_or_default(),
        ..LayerConfig::default()
    })
}

async fn validate_managed_layers<F, Fut>(
    layers: &[(usize, String)],
    label: &str,
    exists: F,
) -> RepositoryResult<()>
where
    F: Fn(String) -> Fut + Clone,
    Fut: Future<Output = RepositoryResult<bool>>,
{
    stream::iter(layers.iter().cloned())
        .map(|(index, digest)| {
            let exists = exists.clone();
            async move {
                if !exists(digest.clone()).await? {
                    return Err(RepositoryError::ArtifactNotFound {
                        artifact: format!("{label}: managed layer {index} '{digest}' is missing"),
                    });
                }
                Ok(())
            }
        })
        .buffer_unordered(LAYER_EXISTS_CONCURRENCY)
        .try_for_each(|()| async { Ok(()) })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use object_store_operator::CredentialSource;
    use tokio::sync::Barrier;

    use super::*;
    use crate::cfg::{SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
    use crate::repository::backends::oss::{OssClient, OssRepository};
    use crate::snapshot::ExternalLayer;

    fn digest(index: usize) -> String {
        format!("sha256:{index:064x}")
    }

    fn oss_repository() -> OssRepository {
        let client = OssClient::new(
            "bucket".to_string(),
            "https://oss.example.com".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("oss client");
        OssRepository::new(
            Arc::new(client),
            SnapshotImageStoragePolicy::ObjectStorage,
            &SnapshotPublishCompressionConfig {
                enabled: false,
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn materializes_volume_backing_with_backend_layer_urls() {
        let temp = tempfile::tempdir().expect("tempdir");
        let repository = oss_repository();
        let destination = temp.path().join("volume/image.json");
        let layers = vec![
            OverlaybdLayerRef::Managed(ManagedLayer {
                digest: "sha256:managed".to_string(),
                size: 12,
                uuid: None,
            }),
            OverlaybdLayerRef::External(ExternalLayer {
                digest: "sha256:external".to_string(),
                repo_blob_url: "https://registry.example/v2/data/blobs".to_string(),
                size: 24,
            }),
        ];

        materialize_volume_backing(&repository, &layers, &destination)
            .await
            .expect("materialize volume config");
        let config: ImageConfig =
            serde_json::from_slice(&tokio::fs::read(&destination).await.unwrap()).unwrap();
        assert_eq!(config.lowers.len(), 2);
        assert_eq!(
            config.lowers[0].repo_blob_url,
            "s3://bucket/prefix/managed-layers"
        );
        assert_eq!(
            config.lowers[1].repo_blob_url,
            "https://registry.example/v2/data/blobs"
        );
        assert!(config.lowers.iter().all(|layer| layer.file.is_empty()));
    }

    #[tokio::test]
    async fn validate_managed_layers_reports_missing_layer() {
        let indexed_layers = [(3, digest(3)), (7, digest(7)), (11, digest(11))];
        let missing = indexed_layers[2].1.clone();
        let expected_artifact = format!("rootfs: managed layer 11 '{missing}' is missing");

        let error = validate_managed_layers(&indexed_layers, "rootfs", move |digest| {
            let missing = missing.clone();
            async move { Ok(digest != missing) }
        })
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            RepositoryError::ArtifactNotFound { artifact } if artifact == expected_artifact
        ));
    }

    #[tokio::test]
    async fn validate_managed_layers_limits_concurrency_to_sixteen() {
        let indexed_layers = (0..32)
            .map(|index| (index, digest(index)))
            .collect::<Vec<_>>();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(LAYER_EXISTS_CONCURRENCY));

        validate_managed_layers(&indexed_layers, "memory", {
            let in_flight = Arc::clone(&in_flight);
            let maximum = Arc::clone(&maximum);
            let barrier = Arc::clone(&barrier);
            move |_| {
                let in_flight = Arc::clone(&in_flight);
                let maximum = Arc::clone(&maximum);
                let barrier = Arc::clone(&barrier);
                async move {
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(current, Ordering::SeqCst);
                    tokio::time::timeout(Duration::from_secs(1), barrier.wait())
                        .await
                        .map_err(|_| RepositoryError::Backend {
                            message: "concurrency barrier timed out".to_owned(),
                            source: None,
                        })?;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(true)
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(LAYER_EXISTS_CONCURRENCY, 16);
        assert_eq!(maximum.load(Ordering::SeqCst), LAYER_EXISTS_CONCURRENCY);
    }
}
