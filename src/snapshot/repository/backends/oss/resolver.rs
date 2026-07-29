use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};
use overlaybd::config::{DownloadConfig, LayerConfig};
use tracing::debug;

use super::client::OssClient;
use super::layout::OssSnapshotArtifactLayout;
use crate::image::cache::OverlaybdLayerStore;
use crate::p2p::P2pTransport;
use crate::snapshot::artifact_cache::{CacheArtifactLease, CacheHandle, LocalArtifactCache};
use crate::snapshot::p2p;
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::runtime_support::{
    hydrate_runtime_manifest, materialize_image_config_error, parse_firecracker_manifest,
    runtime_image_cache_key, RuntimeImageMaterializer,
};
use crate::snapshot::types::RuntimeArtifactLease;
use crate::snapshot::{
    CommittedAttachedDrive, OverlaybdLayerRef, RepositoryError, RepositoryResult,
    ResolvedAttachedDrive, RunnableSnapshot, SnapshotId, SnapshotRecord, SNAPSHOT_ARTIFACT_LAYOUT,
};

const MANAGED_LAYER_EXISTS_CONCURRENCY: usize = 16;

struct MaterializeSpec<'a> {
    label: &'a str,
    cache_key: &'a str,
    allow_empty_layers: bool,
    download: Option<DownloadConfig>,
}

async fn validate_managed_layers<F, Fut>(
    layers: &[(usize, String)],
    label: &str,
    exists: F,
) -> RepositoryResult<()>
where
    F: Fn(String) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<bool>> + Send,
{
    stream::iter(layers.iter().cloned())
        .map(|(index, digest)| {
            let exists = exists.clone();
            async move {
                let key = OssSnapshotArtifactLayout::managed_layer_key(&digest);
                let present = exists(key.clone()).await.map_err(|error| {
                    RepositoryError::backend(format!("check managed layer '{key}'"), error)
                })?;
                if !present {
                    return Err(RepositoryError::ArtifactNotFound {
                        artifact: format!("{label}: managed layer {index} '{digest}' is missing"),
                    });
                }
                Ok(())
            }
        })
        .buffer_unordered(MANAGED_LAYER_EXISTS_CONCURRENCY)
        .try_for_each(|()| async { Ok(()) })
        .await
}

/// Resolves committed OSS-backed snapshots into node-local runnable paths.
pub(crate) struct OssRuntimeResolver {
    client: Arc<OssClient>,
    cache: Arc<LocalArtifactCache>,
    image_materializer: RuntimeImageMaterializer,
    managed_layers_repo_blob_url: String,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
}

impl OssRuntimeResolver {
    fn layout<'a>(&self, id: &'a SnapshotId) -> OssSnapshotArtifactLayout<'a> {
        OssSnapshotArtifactLayout::new(id)
    }

    pub(crate) fn new(
        client: Arc<OssClient>,
        cache: Arc<LocalArtifactCache>,
        runtime_root: PathBuf,
        store: Arc<dyn OverlaybdLayerStore>,
        managed_layers_repo_blob_url: String,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> RepositoryResult<Self> {
        Ok(Self {
            client,
            cache,
            image_materializer: RuntimeImageMaterializer::new(runtime_root, store),
            managed_layers_repo_blob_url,
            p2p_transport,
        })
    }
}

#[async_trait]
impl SnapshotRuntimeResolver for OssRuntimeResolver {
    async fn resolve(&self, snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot> {
        let id = snapshot.id.clone();
        let committed =
            snapshot
                .committed
                .as_ref()
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{}' is not ready", snapshot.id),
                })?;
        let layout = self.layout(&id);
        let mut handles: Vec<CacheHandle> = Vec::new();

        // ── vm state snapshot ───────────────────────────────────────
        let vm_state_key = layout.artifact_key(SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        let vm_state_client = Arc::clone(&self.client);
        let p2p_transport = self.p2p_transport.clone();
        let vm_state_p2p_key = p2p::fixed_artifact_key(&id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        let vm_state_handle = self
            .cache
            .ensure_cached(&vm_state_key, |dest| {
                let client = Arc::clone(&vm_state_client);
                let key = vm_state_key.clone();
                let p2p_transport = p2p_transport.clone();
                let p2p_key = vm_state_p2p_key.clone();
                async move {
                    if let Some(transport) = p2p_transport.as_ref() {
                        match p2p::fetch_artifact(transport, &p2p_key, &dest).await {
                            Ok(size) => return Ok(size),
                            Err(error) => {
                                debug!(
                                    key = %p2p_key,
                                    error = %error,
                                    "P2P vm_state fetch failed; using backend fallback"
                                );
                            }
                        }
                    }
                    client.get_to_file(&key, &dest).await
                }
            })
            .await
            .map_err(|e| RepositoryError::ArtifactNotFound {
                artifact: format!("vm state artifact for snapshot '{id}': {e}"),
            })?;
        let vm_state_path = vm_state_handle.path().to_path_buf();
        handles.push(vm_state_handle);

        // ── firecracker manifest ───────────────────────────────────
        let committed_manifest = self
            .load_committed_firecracker_manifest(&layout, &id)
            .await?;

        // ── memory image config ────────────────────────────────────
        let memory_layers: Vec<OverlaybdLayerRef> = committed
            .memory_layers
            .iter()
            .map(|m| OverlaybdLayerRef::Managed(m.clone()))
            .collect();
        let mem_cache_key = runtime_image_cache_key(&id, "memory/image.json");
        let mem_image_config_path = self
            .materialize_layers_and_pin(
                &memory_layers,
                &self.image_materializer.memory_image_config_path(&id),
                MaterializeSpec {
                    label: "memory",
                    cache_key: &mem_cache_key,
                    allow_empty_layers: true,
                    download: None,
                },
                &mut handles,
            )
            .await?;

        // ── rootfs image config ────────────────────────────────────
        let rootfs_cache_key = runtime_image_cache_key(&id, "rootfs/image.json");
        let rootfs_image_config_path = self
            .materialize_layers_and_pin(
                &committed.rootfs_layers,
                &self.image_materializer.rootfs_image_config_path(&id),
                MaterializeSpec {
                    label: "rootfs",
                    cache_key: &rootfs_cache_key,
                    allow_empty_layers: false,
                    download: None,
                },
                &mut handles,
            )
            .await?;

        // ── attached drives ────────────────────────────────────────
        let attached_drives = self
            .resolve_attached_drives(&id, &committed.attached_drives, &mut handles)
            .await?;

        // Runtime artifacts are protected by the sandbox start-window lease (over
        // local-only commits) + the orchestrator running set; the resolved-handle
        // needs no separate local image ref pin.
        let cache_lease: Arc<dyn RuntimeArtifactLease> =
            Arc::new(CacheArtifactLease { _handles: handles });

        let runtime_manifest = hydrate_runtime_manifest(
            committed_manifest,
            vm_state_path,
            mem_image_config_path,
            rootfs_image_config_path,
            &attached_drives,
        )?;

        let runnable = RunnableSnapshot::new((*snapshot).clone(), runtime_manifest, cache_lease);
        debug!(snapshot_id = %id, "resolved oss snapshot to local runnable paths");
        Ok(runnable)
    }
}

// ── private helpers ───────────────────────────────────────────────────

impl OssRuntimeResolver {
    /// Materialize an image config from layers and pin it in the cache.
    async fn materialize_layers_and_pin(
        &self,
        layers: &[OverlaybdLayerRef],
        destination: &Path,
        spec: MaterializeSpec<'_>,
        handles: &mut Vec<CacheHandle>,
    ) -> RepositoryResult<PathBuf> {
        if !spec.allow_empty_layers && layers.is_empty() {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("{} has no layers", spec.label),
            });
        }

        let download = spec.download.clone();
        let handle = self
            .cache
            .ensure_cached_at(spec.cache_key, destination.to_path_buf(), |dest| {
                let download = download.clone();
                async move {
                    let path = self
                        .materialize_image_config(layers, &dest, spec.label, download)
                        .await
                        .map_err(anyhow::Error::new)?;
                    tokio::fs::metadata(&path)
                        .await
                        .map(|metadata| metadata.len())
                        .map_err(|error| {
                            anyhow::Error::new(RepositoryError::backend(
                                format!("stat {} image config '{}'", spec.label, path.display()),
                                error,
                            ))
                        })
                }
            })
            .await
            .map_err(|error| materialize_image_config_error(spec.label, error))?;
        let path = handle.path().to_path_buf();
        handles.push(handle);
        Ok(path)
    }

    /// Verify remote managed layers exist, build an `ImageConfig`, and write it.
    async fn materialize_image_config(
        &self,
        layers: &[OverlaybdLayerRef],
        destination: &Path,
        label: &str,
        download: Option<DownloadConfig>,
    ) -> RepositoryResult<PathBuf> {
        let managed_layers = layers
            .iter()
            .enumerate()
            .filter_map(|(index, layer)| match layer {
                OverlaybdLayerRef::Managed(layer) => Some((index, layer.digest.clone())),
                OverlaybdLayerRef::External(_) => None,
            })
            .collect::<Vec<_>>();
        let client = Arc::clone(&self.client);
        validate_managed_layers(&managed_layers, label, move |key| {
            let client = Arc::clone(&client);
            async move { client.exists(&key).await }
        })
        .await?;

        self.image_materializer
            .materialize_image_config(
                layers,
                destination,
                label,
                Some(&self.managed_layers_repo_blob_url),
                download,
                |_, managed| async move {
                    Ok(LayerConfig {
                        digest: managed.digest,
                        size: managed.size,
                        uuid: managed.uuid.unwrap_or_default(),
                        ..Default::default()
                    })
                },
            )
            .await
    }

    async fn resolve_attached_drives(
        &self,
        id: &SnapshotId,
        committed_drives: &[CommittedAttachedDrive],
        handles: &mut Vec<CacheHandle>,
    ) -> RepositoryResult<Vec<ResolvedAttachedDrive>> {
        let mut drives = Vec::new();

        for drive in committed_drives {
            match drive {
                CommittedAttachedDrive::Overlaybd {
                    drive_id,
                    layers,
                    read_only,
                    virtual_size,
                    mount_path,
                    sub_path,
                } => {
                    if *virtual_size == 0 {
                        return Err(RepositoryError::InvalidRequest {
                            reason: format!(
                                "attached drive '{}' virtual_size must be non-zero",
                                drive_id
                            ),
                        });
                    }
                    let image_config_path = self
                        .materialize_layers_and_pin(
                            layers,
                            &self
                                .image_materializer
                                .drive_image_config_path(id, drive_id),
                            MaterializeSpec {
                                label: &format!("drive '{drive_id}'"),
                                cache_key: &runtime_image_cache_key(
                                    id,
                                    &format!("drives/{drive_id}/image.json"),
                                ),
                                allow_empty_layers: false,
                                download: None,
                            },
                            handles,
                        )
                        .await?;

                    drives.push(ResolvedAttachedDrive::Overlaybd {
                        drive_id: drive_id.clone(),
                        image_config_path,
                        read_only: *read_only,
                        virtual_size: *virtual_size,
                        mount_path: crate::sandbox::normalize_mount_path_for_drive(
                            drive_id,
                            mount_path.clone(),
                        )
                        .unwrap_or_else(|_| {
                            crate::sandbox::ExtraDrive::default_mount_path(drive_id)
                        }),
                        sub_path: sub_path.clone(),
                    });
                }
            }
        }

        Ok(drives)
    }

    async fn load_committed_firecracker_manifest(
        &self,
        layout: &OssSnapshotArtifactLayout<'_>,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<crate::sandbox::FirecrackerSnapshotManifest> {
        let p2p_key =
            p2p::fixed_artifact_key(snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        if let Some(transport) = self.p2p_transport.as_ref() {
            match p2p::fetch_artifact_bytes(transport, &p2p_key).await {
                Ok(bytes) => match parse_firecracker_manifest(&bytes, &p2p_key) {
                    Ok(manifest) => return Ok(manifest),
                    Err(error) => {
                        debug!(
                            key = %p2p_key,
                            error = %error,
                            "P2P firecracker manifest parse failed; using backend fallback"
                        );
                    }
                },
                Err(error) => {
                    debug!(
                        key = %p2p_key,
                        error = %error,
                        "P2P firecracker manifest fetch failed; using backend fallback"
                    );
                }
            }
        }

        let key = layout.artifact_key(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        let bytes = self.client.get_bytes(&key).await.map_err(|error| {
            if OssClient::is_not_found_error(&error) {
                return RepositoryError::ArtifactNotFound {
                    artifact: format!(
                        "firecracker manifest artifact for snapshot '{}' at key '{}'",
                        snapshot_id, key
                    ),
                };
            }
            RepositoryError::backend(
                format!("read firecracker manifest for snapshot '{snapshot_id}'"),
                error,
            )
        })?;
        parse_firecracker_manifest(&bytes, &key)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use tokio::sync::Barrier;

    fn digest(index: usize) -> String {
        format!("sha256:{index:064x}")
    }

    #[tokio::test]
    async fn validate_managed_layers_reports_missing_layer() {
        let indexed_layers = [(3, digest(3)), (7, digest(7)), (11, digest(11))];
        let missing_key = OssSnapshotArtifactLayout::managed_layer_key(&indexed_layers[2].1);
        let expected_artifact = format!(
            "rootfs: managed layer 11 '{}' is missing",
            indexed_layers[2].1
        );

        let error = validate_managed_layers(&indexed_layers, "rootfs", move |key| {
            let missing_key = missing_key.clone();
            async move { Ok(key != missing_key) }
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
        let barrier = Arc::new(Barrier::new(MANAGED_LAYER_EXISTS_CONCURRENCY));

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
                        .map_err(|_| anyhow::anyhow!("concurrency barrier timed out"))?;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(true)
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(MANAGED_LAYER_EXISTS_CONCURRENCY, 16);
        assert_eq!(
            maximum.load(Ordering::SeqCst),
            MANAGED_LAYER_EXISTS_CONCURRENCY
        );
    }
}
