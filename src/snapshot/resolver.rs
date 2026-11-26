//! Resolving committed snapshots into node-local runnable paths. One resolver
//! serves every repository: files a backend can read in place are used where
//! they are; everything else is fetched, P2P-first, into the node-local cache.
//! A missing in-place file is an error, never a reason to fetch it from peers.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};
use tracing::debug;

use super::artifact_cache::{CacheHandle, LocalArtifactCache};
use super::p2p;
use super::{
    CommittedAttachedDrive, SnapshotAttachedDriveArtifacts, SnapshotHandle, SnapshotId,
    SnapshotManifest, SnapshotPlacement, SnapshotRecord, SnapshotRuntimeManifest,
};
use crate::image::cache::OverlaybdLayerStore;
use crate::layer::{LayerChain, LayerRef, LayerSource, ManagedLayerRef};
use crate::p2p::P2pTransport;
use crate::repository::{
    layer_chain_for_refs, BlobKey, BlobLocation, LayerRole, Repository, RepositoryError,
    RepositoryResult, SnapshotFile,
};

const MANAGED_LAYER_RESOLVE_CONCURRENCY: usize = 16;

#[async_trait]
/// Resolves committed snapshot records into node-local runnable handles.
pub trait SnapshotRuntimeResolver: Send + Sync {
    async fn resolve(&self, snapshot: Arc<SnapshotRecord>)
        -> RepositoryResult<Arc<SnapshotHandle>>;
}

/// Resolves committed snapshots stored in one repository.
pub(crate) struct RepositoryRuntimeResolver {
    repository: Arc<dyn Repository>,
    pub(super) cache: Arc<LocalArtifactCache>,
    pub(super) runtime_root: PathBuf,
    pub(super) store: Arc<dyn OverlaybdLayerStore>,
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
            runtime_root,
            store,
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

    fn layer_chain(
        &self,
        references: &[LayerRef],
        role: LayerRole,
    ) -> RepositoryResult<LayerChain> {
        layer_chain_for_refs(self.repository.as_ref(), references, role)
    }

    async fn resolve_managed_layer(
        &self,
        index: usize,
        layer: ManagedLayerRef,
        role: LayerRole,
    ) -> RepositoryResult<()> {
        let key = BlobKey::Layer {
            digest: layer.digest.clone(),
            role,
        };
        let location = self.repository.locate_blob(&key)?;
        if !self.repository.has_blob(&key).await? {
            let location = match &location {
                BlobLocation::Local(path) => path.display().to_string(),
                BlobLocation::Remote(url) => url.clone(),
            };
            return Err(RepositoryError::ArtifactNotFound {
                artifact: format!("managed layer {index} '{}' at {location}", layer.digest),
            });
        }
        Ok(())
    }

    async fn materialize_image_config(
        &self,
        snapshot_id: &SnapshotId,
        relative_path: &str,
        layers: &LayerChain,
        role: LayerRole,
    ) -> RepositoryResult<CacheHandle> {
        // Snapshots without a persisted memory dump still get a stable runtime config path.
        if role != LayerRole::Memory && layers.layers.is_empty() {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{snapshot_id}' {relative_path} has no layers"),
            });
        }
        let cache_key = format!("runtime/{snapshot_id}/{relative_path}");
        let destination = self
            .runtime_root
            .join(snapshot_id.to_string())
            .join(relative_path);
        self.cache
            .ensure_cached_at(&cache_key, destination, |destination| async move {
                let managed = layers
                    .layers
                    .iter()
                    .enumerate()
                    .filter_map(|(index, layer)| match layer {
                        LayerSource::Referenced {
                            reference: LayerRef::Managed(layer),
                            ..
                        } => Some((index, layer.clone())),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                // Existence checks stay inside the cold-cache operation.
                stream::iter(managed)
                    .map(|(index, layer)| self.resolve_managed_layer(index, layer, role))
                    .buffer_unordered(MANAGED_LAYER_RESOLVE_CONCURRENCY)
                    .try_for_each(|()| async { Ok(()) })
                    .await?;
                let mut image = layers.to_runtime_config();
                self.store.attach_to_image_config(&mut image);
                let bytes = serde_json::to_vec_pretty(&image)?;
                tokio::fs::write(destination, &bytes).await?;
                Ok(bytes.len() as u64)
            })
            .await
            .map_err(|error| materialize_image_config_error(relative_path, error))
    }
}

#[async_trait]
impl SnapshotRuntimeResolver for RepositoryRuntimeResolver {
    async fn resolve(
        &self,
        snapshot: Arc<SnapshotRecord>,
    ) -> RepositoryResult<Arc<SnapshotHandle>> {
        if snapshot.snapshot_type != SnapshotPlacement::Distributed {
            return Err(RepositoryError::Unsupported {
                feature: "resolve Local snapshots through their owner manager".to_string(),
            });
        }
        let id = &snapshot.id;
        let committed =
            snapshot
                .manifest
                .as_ref()
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{id}' is not ready"),
                })?;
        let mut handles: Vec<CacheHandle> = Vec::new();
        let vm_state_path = self.vm_state_path(id, &mut handles).await?;
        let committed_manifest = self.load_firecracker_manifest(id).await?;

        let memory_refs = committed
            .memory_layers
            .iter()
            .cloned()
            .map(LayerRef::Managed)
            .collect::<Vec<_>>();
        let memory_layers = self.layer_chain(&memory_refs, LayerRole::Memory)?;
        let memory_handle = self
            .materialize_image_config(id, "memory/image.json", &memory_layers, LayerRole::Memory)
            .await?;
        let mem_image_config_path = memory_handle.path().to_path_buf();
        handles.push(memory_handle);

        let rootfs_layers = self.layer_chain(&committed.rootfs_layers, LayerRole::Rootfs)?;
        let rootfs_handle = self
            .materialize_image_config(id, "rootfs/image.json", &rootfs_layers, LayerRole::Rootfs)
            .await?;
        let rootfs_image_config_path = rootfs_handle.path().to_path_buf();
        handles.push(rootfs_handle);

        let attached_drives = self
            .resolve_attached_drives(id, committed, &mut handles)
            .await?;

        let mut runtime_manifest = hydrate_runtime_manifest(
            committed_manifest,
            vm_state_path,
            mem_image_config_path,
            rootfs_image_config_path,
            memory_layers,
            rootfs_layers,
            attached_drives,
        )?;
        runtime_manifest.memory_startup_pack = self.startup_pack(id, committed).await;

        let handle = Arc::new(SnapshotHandle::new(id.clone(), runtime_manifest, handles));
        debug!(snapshot_id = %id, "resolved snapshot to local runnable paths");
        Ok(handle)
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
        if let BlobLocation::Local(source) = self.repository.locate_blob(&key)? {
            let handle = self
                .cache
                .ensure_cached(
                    &format!("runtime/{id}/vm_state.bin"),
                    |destination| async move {
                        match tokio::fs::hard_link(&source, &destination).await {
                            Ok(()) => Ok(tokio::fs::metadata(&destination).await?.len()),
                            Err(error)
                                if matches!(
                                    error.kind(),
                                    std::io::ErrorKind::NotFound
                                        | std::io::ErrorKind::AlreadyExists
                                ) =>
                            {
                                Err(error.into())
                            }
                            Err(_) => Ok(tokio::fs::copy(&source, &destination).await?),
                        }
                    },
                )
                .await
                .map_err(|error| {
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                    {
                        RepositoryError::ArtifactNotFound {
                            artifact: format!("vm state for snapshot '{id}'"),
                        }
                    } else {
                        RepositoryError::backend("retain resolved POSIX vm state", error)
                    }
                })?;
            let path = handle.path().to_path_buf();
            handles.push(handle);
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
    ) -> RepositoryResult<SnapshotRuntimeManifest> {
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
        committed: &SnapshotManifest,
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
        committed: &SnapshotManifest,
        handles: &mut Vec<CacheHandle>,
    ) -> RepositoryResult<Vec<SnapshotAttachedDriveArtifacts>> {
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
            let drive_layers = self.layer_chain(layers, LayerRole::AttachedDrive)?;
            let handle = self
                .materialize_image_config(
                    id,
                    &format!("drives/{drive_id}/image.json"),
                    &drive_layers,
                    LayerRole::AttachedDrive,
                )
                .await?;
            let image_config_path = handle.path().to_path_buf();
            handles.push(handle);
            drives.push(SnapshotAttachedDriveArtifacts {
                drive_id: drive_id.clone(),
                image_config_path,
                layers: drive_layers,
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
}

fn materialize_image_config_error(label: &str, error: anyhow::Error) -> RepositoryError {
    if let Some(RepositoryError::ArtifactNotFound { artifact }) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<RepositoryError>())
    {
        return RepositoryError::ArtifactNotFound {
            artifact: artifact.clone(),
        };
    }

    RepositoryError::backend(format!("materialize {label} image config"), error)
}

fn parse_firecracker_manifest(
    bytes: &[u8],
    manifest_ref: impl AsRef<str>,
) -> RepositoryResult<SnapshotRuntimeManifest> {
    serde_json::from_slice(bytes).map_err(|error| {
        RepositoryError::backend(
            format!("parse firecracker manifest '{}'", manifest_ref.as_ref()),
            error,
        )
    })
}

async fn load_firecracker_manifest_from_path(
    path: &Path,
) -> RepositoryResult<SnapshotRuntimeManifest> {
    let bytes = tokio::fs::read(path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            return RepositoryError::ArtifactNotFound {
                artifact: format!("firecracker manifest at {}", path.display()),
            };
        }
        RepositoryError::backend(
            format!("read firecracker manifest '{}'", path.display()),
            error,
        )
    })?;
    parse_firecracker_manifest(&bytes, path.display().to_string())
}

fn hydrate_runtime_manifest(
    mut manifest: SnapshotRuntimeManifest,
    vm_state_path: PathBuf,
    memory_image_config_path: PathBuf,
    rootfs_image_config_path: PathBuf,
    memory_layers: LayerChain,
    rootfs_layers: LayerChain,
    attached_drives: Vec<SnapshotAttachedDriveArtifacts>,
) -> RepositoryResult<SnapshotRuntimeManifest> {
    manifest.attached_drives = attached_drives;
    manifest
        .validate_attached_drives()
        .map_err(|error| RepositoryError::InvalidRequest {
            reason: format!("hydrate attached drives in firecracker manifest: {error:#}"),
        })?;
    manifest.vm_state.path = vm_state_path;
    manifest.memory.image_config_path = memory_image_config_path;
    manifest.memory.layers = memory_layers;
    manifest.rootfs.image_config_path = rootfs_image_config_path;
    manifest.rootfs.layers = rootfs_layers;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use overlaybd::config::ImageConfig;
    use tokio::sync::Barrier;

    use super::*;
    use crate::image::cache::{OverlaybdLayerLocation, OverlaybdLayerStore};
    use crate::layer::{ExternalLayer, ManagedLayerRef};
    use crate::repository::{BlobKey, BlobLocation, Repository};
    use crate::snapshot::artifact_cache::LocalArtifactCache;

    #[derive(Debug)]
    struct TestOverlaybdLayerStore;

    impl OverlaybdLayerStore for TestOverlaybdLayerStore {
        fn layer_location(
            &self,
            digest: &str,
            _size: u64,
            has_remote: bool,
        ) -> OverlaybdLayerLocation {
            let name = digest.replace(':', "-");
            if has_remote {
                OverlaybdLayerLocation::CacheDir(format!("cache/{name}").into())
            } else {
                OverlaybdLayerLocation::LocalFile(format!("files/{name}.commit").into())
            }
        }

        fn publishable_roots(&self) -> Vec<PathBuf> {
            Vec::new()
        }
    }

    #[derive(Default)]
    struct TestRepository {
        missing: Option<String>,
        remote: Option<String>,
        in_flight: AtomicUsize,
        maximum: AtomicUsize,
        checks: AtomicUsize,
        barrier: Option<Arc<Barrier>>,
    }

    #[async_trait]
    impl Repository for TestRepository {
        fn managed_layers_repo_blob_url(&self) -> Option<String> {
            self.remote.clone()
        }

        fn locate_blob(&self, key: &BlobKey) -> RepositoryResult<BlobLocation> {
            match &self.remote {
                Some(url) => Ok(BlobLocation::Remote(format!("{url}/{}", key.path()))),
                None => Ok(BlobLocation::Local(PathBuf::from(key.posix_path()))),
            }
        }

        async fn has_blob(&self, key: &BlobKey) -> RepositoryResult<bool> {
            self.checks.fetch_add(1, Ordering::SeqCst);
            let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(current, Ordering::SeqCst);
            if let Some(barrier) = &self.barrier {
                tokio::time::timeout(Duration::from_secs(1), barrier.wait())
                    .await
                    .map_err(|_| RepositoryError::Backend {
                        message: "concurrency barrier timed out".to_owned(),
                        source: None,
                    })?;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            let digest = match key {
                BlobKey::Layer { digest, .. } => digest,
                BlobKey::Snapshot(..) => return Ok(true),
            };
            Ok(self.missing.as_deref() != Some(digest.as_str()))
        }
    }

    fn test_resolver(root: &Path, repository: Arc<dyn Repository>) -> RepositoryRuntimeResolver {
        let cache = LocalArtifactCache::new(root.join("cache"), None).expect("local cache");
        RepositoryRuntimeResolver::new(
            repository,
            cache,
            root.join("runtime"),
            Arc::new(TestOverlaybdLayerStore),
            None,
        )
    }

    #[tokio::test]
    async fn materialize_image_config_reports_missing_managed_layer() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let missing = format!("sha256:{:064x}", 11);
        let repository = Arc::new(TestRepository {
            missing: Some(missing.clone()),
            ..Default::default()
        });
        let resolver = test_resolver(tempdir.path(), repository);
        let layers = [3usize, 7, 11]
            .into_iter()
            .map(|index| {
                LayerRef::Managed(ManagedLayerRef {
                    digest: format!("sha256:{index:064x}"),
                    size: 16,
                    uuid: None,
                })
            })
            .collect::<Vec<_>>();
        let chain = resolver.layer_chain(&layers, LayerRole::Rootfs).unwrap();
        let error = resolver
            .materialize_image_config(
                &SnapshotId::generate(),
                "rootfs/image.json",
                &chain,
                LayerRole::Rootfs,
            )
            .await
            .err()
            .expect("missing managed layer must fail");
        assert!(matches!(
            error,
            RepositoryError::ArtifactNotFound { artifact } if artifact.contains(&missing)
        ));
    }

    #[tokio::test]
    async fn materialize_image_config_limits_existence_checks_to_sixteen_and_caches() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let repository = Arc::new(TestRepository {
            barrier: Some(Arc::new(Barrier::new(MANAGED_LAYER_RESOLVE_CONCURRENCY))),
            ..Default::default()
        });
        let resolver = test_resolver(
            tempdir.path(),
            Arc::clone(&repository) as Arc<dyn Repository>,
        );
        let refs = (0..32usize)
            .map(|index| {
                LayerRef::Managed(ManagedLayerRef {
                    digest: format!("sha256:{index:064x}"),
                    size: 16,
                    uuid: None,
                })
            })
            .collect::<Vec<_>>();
        let layers = resolver.layer_chain(&refs, LayerRole::Rootfs).unwrap();
        let id = SnapshotId::generate();
        let handle = resolver
            .materialize_image_config(&id, "rootfs/image.json", &layers, LayerRole::Rootfs)
            .await
            .unwrap();
        assert_eq!(
            repository.maximum.load(Ordering::SeqCst),
            MANAGED_LAYER_RESOLVE_CONCURRENCY
        );
        let config: ImageConfig =
            serde_json::from_slice(&std::fs::read(handle.path()).unwrap()).unwrap();
        for (index, layer) in config.lowers.iter().enumerate() {
            assert_eq!(layer.digest, format!("sha256:{index:064x}"));
        }
        let second = resolver
            .materialize_image_config(&id, "rootfs/image.json", &layers, LayerRole::Rootfs)
            .await
            .unwrap();
        assert_eq!(repository.checks.load(Ordering::SeqCst), 32);
        assert_eq!(second.path(), handle.path());
        let error = resolver
            .materialize_image_config(
                &id,
                "rootfs/image.json",
                &LayerChain::default(),
                LayerRole::Rootfs,
            )
            .await
            .err()
            .expect("empty rootfs must fail even with a warm config");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn materialize_image_config_preserves_each_layers_remote_source() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let layers = vec![
            LayerRef::External(ExternalLayer {
                digest: "sha256:base".to_string(),
                repo_blob_url: "https://registry.example/v2/ns/image/blobs".to_string(),
                size: 10,
            }),
            LayerRef::External(ExternalLayer {
                digest: "sha256:next".to_string(),
                repo_blob_url: "https://registry.example/v2/ns/image/blobs/".to_string(),
                size: 20,
            }),
        ];
        let resolver = test_resolver(tempdir.path(), Arc::new(TestRepository::default()));
        let layers = resolver.layer_chain(&layers, LayerRole::Rootfs).unwrap();
        let handle = resolver
            .materialize_image_config(
                &SnapshotId::generate(),
                "rootfs/image.json",
                &layers,
                LayerRole::Rootfs,
            )
            .await
            .expect("materialize");

        let cfg: ImageConfig =
            serde_json::from_slice(&std::fs::read(handle.path()).expect("read image config"))
                .expect("parse image config");
        assert_eq!(
            cfg.repo_blob_url,
            "https://registry.example/v2/ns/image/blobs"
        );
        assert!(cfg
            .lowers
            .iter()
            .all(|layer| layer.repo_blob_url.trim_end_matches('/') == cfg.repo_blob_url));
        assert!(cfg.lowers.iter().all(|layer| layer.file.is_empty()));
        assert!(cfg.lowers.iter().all(|layer| !layer.dir.is_empty()));
    }

    #[tokio::test]
    async fn materialize_image_config_writes_layer_repo_blob_urls_for_mixed_backends() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let layers = vec![
            LayerRef::External(ExternalLayer {
                digest: "sha256:base".to_string(),
                repo_blob_url: "https://registry.example/v2/ns/image/blobs".to_string(),
                size: 10,
            }),
            LayerRef::Managed(ManagedLayerRef {
                digest: "sha256:delta".to_string(),
                size: 20,
                uuid: Some("11111111-2222-3333-4444-555555555555".to_string()),
            }),
        ];
        let resolver = test_resolver(
            tempdir.path(),
            Arc::new(TestRepository {
                remote: Some("s3://bucket/prefix/managed-layers".into()),
                ..Default::default()
            }),
        );
        let layers = resolver.layer_chain(&layers, LayerRole::Rootfs).unwrap();
        let handle = resolver
            .materialize_image_config(
                &SnapshotId::generate(),
                "rootfs/image.json",
                &layers,
                LayerRole::Rootfs,
            )
            .await
            .expect("materialize");

        let cfg: ImageConfig =
            serde_json::from_slice(&std::fs::read(handle.path()).expect("read image config"))
                .expect("parse image config");
        assert_eq!(cfg.repo_blob_url, "");
        assert_eq!(
            cfg.lowers[0].repo_blob_url,
            "https://registry.example/v2/ns/image/blobs"
        );
        assert_eq!(
            cfg.lowers[1].repo_blob_url,
            "s3://bucket/prefix/managed-layers"
        );
        assert_eq!(cfg.lowers[1].uuid, "11111111-2222-3333-4444-555555555555");
        assert!(cfg.lowers.iter().all(|layer| layer.file.is_empty()));
        assert!(cfg.lowers.iter().all(|layer| !layer.dir.is_empty()));
    }
}
