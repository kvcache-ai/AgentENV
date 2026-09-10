use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use overlaybd::config::{DownloadConfig, LayerConfig};

use super::layout::PosixFsSnapshotArtifactLayout;
use crate::image::cache::OverlaybdLayerStore;
use crate::snapshot::artifact_cache::{CacheArtifactLease, CacheHandle, LocalArtifactCache};
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::runtime_support::{
    hydrate_runtime_manifest, load_firecracker_manifest_from_path, materialize_image_config_error,
    runtime_image_cache_key, RuntimeImageMaterializer,
};
use crate::snapshot::types::RuntimeArtifactLease;
use crate::snapshot::{
    CommittedAttachedDrive, CommittedSnapshot, OverlaybdLayerRef, RepositoryError,
    RepositoryResult, ResolvedAttachedDrive, RunnableSnapshot, SnapshotId, SnapshotRecord,
    MEMORY_PREFETCH_ARTIFACT, SNAPSHOT_ARTIFACT_LAYOUT,
};

/// Resolves committed snapshot artifacts into node-local runnable paths on a POSIX filesystem.
pub struct PosixFsRuntimeResolver {
    repository_root: PathBuf,
    image_materializer: RuntimeImageMaterializer,
    cache: Arc<LocalArtifactCache>,
}

struct MaterializeSpec<'a> {
    label: &'a str,
    cache_key: &'a str,
    artifact_prefix: &'a str,
    allow_empty_layers: bool,
    download: Option<DownloadConfig>,
}

impl PosixFsRuntimeResolver {
    /// Creates a runtime resolver that turns committed POSIX-backed snapshots into node-local
    /// runnable paths.
    pub fn new(
        repository_root: PathBuf,
        runtime_cache_root: PathBuf,
        store: Arc<dyn OverlaybdLayerStore>,
        cache: Arc<LocalArtifactCache>,
    ) -> Self {
        Self {
            repository_root,
            image_materializer: RuntimeImageMaterializer::new(runtime_cache_root, store),
            cache,
        }
    }
}

#[async_trait]
impl SnapshotRuntimeResolver for PosixFsRuntimeResolver {
    /// Resolves one committed snapshot by materializing any node-local helper files needed by the
    /// current node, while leaving committed repository truth untouched.
    async fn resolve(&self, snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot> {
        let committed =
            snapshot
                .committed
                .as_ref()
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{}' is not ready", snapshot.id),
                })?;
        let snapshot_id = snapshot.id.clone();
        let vm_state_path = self.snapshot_vm_state_path(&snapshot_id)?;
        let committed_manifest = self
            .load_committed_firecracker_manifest(&snapshot_id)
            .await?;
        let mut handles: Vec<CacheHandle> = Vec::new();
        let mem_image_config_path = self
            .materialize_mem_image_config(&snapshot_id, committed, &mut handles)
            .await?;
        let rootfs_label = format!("snapshot '{}'", snapshot.id);
        let rootfs_cache_key = runtime_image_cache_key(&snapshot.id, "rootfs/image.json");
        let rootfs_image_config_path = self
            .materialize_and_pin(
                &committed.rootfs_layers,
                &self
                    .image_materializer
                    .rootfs_image_config_path(&snapshot.id),
                MaterializeSpec {
                    label: &rootfs_label,
                    cache_key: &rootfs_cache_key,
                    artifact_prefix: "rootfs_layer",
                    allow_empty_layers: false,
                    download: None,
                },
                &mut handles,
            )
            .await?;
        let attached_drives = self
            .resolve_attached_drives(&snapshot_id, committed, &mut handles)
            .await?;
        let runtime_manifest = hydrate_runtime_manifest(
            committed_manifest,
            vm_state_path.clone(),
            mem_image_config_path,
            rootfs_image_config_path,
            &attached_drives,
        )?;
        let mut runtime_manifest = runtime_manifest;
        // Best-effort: the memory prefetch manifest sits next to vm_state.bin
        // when the capture produced one (older snapshots lack it). Async
        // existence check so a slow shared repository filesystem cannot stall
        // the executor.
        runtime_manifest.memory_prefetch_path = self.memory_prefetch_path(&vm_state_path).await;
        // Runtime artifacts are protected by the sandbox start-window lease (over
        // local-only commits) + the orchestrator running set; the resolved-handle
        // needs no separate local image ref pin.
        let cache_lease: Arc<dyn RuntimeArtifactLease> =
            Arc::new(CacheArtifactLease { _handles: handles });
        let runnable = RunnableSnapshot::new((*snapshot).clone(), runtime_manifest, cache_lease);
        Ok(runnable)
    }
}

impl PosixFsRuntimeResolver {
    fn snapshot_layout(&self, snapshot_id: &SnapshotId) -> PosixFsSnapshotArtifactLayout {
        PosixFsSnapshotArtifactLayout::new(&self.repository_root, snapshot_id)
    }

    /// Returns the local path of the optional memory prefetch manifest that
    /// sits next to `vm_state.bin` (when the capture produced one).
    async fn memory_prefetch_path(&self, vm_state_path: &Path) -> Option<PathBuf> {
        let candidate = vm_state_path
            .parent()
            .map(|dir| dir.join(MEMORY_PREFETCH_ARTIFACT))?;
        match tokio::fs::try_exists(&candidate).await {
            Ok(true) => Some(candidate),
            Ok(false) | Err(_) => None,
        }
    }

    fn snapshot_vm_state_path(&self, snapshot_id: &SnapshotId) -> RepositoryResult<PathBuf> {
        let layout = self.snapshot_layout(snapshot_id);
        let vm_state_path = layout.path(SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        if vm_state_path.exists() {
            return Ok(vm_state_path);
        }
        Err(RepositoryError::ArtifactNotFound {
            artifact: format!("vm state at {}", vm_state_path.display()),
        })
    }

    async fn load_committed_firecracker_manifest(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<crate::sandbox::FirecrackerSnapshotManifest> {
        let manifest_path = self
            .snapshot_layout(snapshot_id)
            .path(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        load_firecracker_manifest_from_path(&manifest_path).await
    }

    async fn resolve_attached_drives(
        &self,
        snapshot_id: &SnapshotId,
        snapshot: &CommittedSnapshot,
        handles: &mut Vec<CacheHandle>,
    ) -> RepositoryResult<Vec<ResolvedAttachedDrive>> {
        if snapshot.attached_drives.is_empty() {
            return Ok(Vec::new());
        }
        let mut drives = Vec::with_capacity(snapshot.attached_drives.len());
        for drive in &snapshot.attached_drives {
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
                    let label = format!(
                        "attached drive '{}' for snapshot '{}'",
                        drive_id, snapshot_id
                    );
                    let cache_key = runtime_image_cache_key(
                        snapshot_id,
                        &format!("drives/{drive_id}/image.json"),
                    );
                    let image_config_path = self
                        .materialize_and_pin(
                            layers,
                            &self
                                .image_materializer
                                .drive_image_config_path(snapshot_id, drive_id),
                            MaterializeSpec {
                                label: &label,
                                cache_key: &cache_key,
                                artifact_prefix: "rootfs_layer",
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

    fn resolve_local_managed_layer(
        &self,
        index: usize,
        layer: &crate::snapshot::ManagedLayer,
        artifact_prefix: &str,
    ) -> RepositoryResult<LayerConfig> {
        let path =
            PosixFsSnapshotArtifactLayout::managed_layer_path(&self.repository_root, &layer.digest);
        if !path.exists() {
            return Err(RepositoryError::ArtifactNotFound {
                artifact: format!("{artifact_prefix}_{index} at {}", path.display()),
            });
        }
        Ok(LayerConfig {
            file: path.display().to_string(),
            digest: layer.digest.clone(),
            size: layer.size,
            uuid: layer.uuid.clone().unwrap_or_default(),
            ..Default::default()
        })
    }

    async fn materialize_and_pin(
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
                        .image_materializer
                        .materialize_image_config(
                            layers,
                            &dest,
                            spec.label,
                            None,
                            download,
                            |index, layer| async move {
                                self.resolve_local_managed_layer(
                                    index,
                                    &layer,
                                    spec.artifact_prefix,
                                )
                            },
                        )
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

    /// Materializes an overlaybd image config for memory layers.
    ///
    /// Resolves each managed memory layer to its absolute path and writes
    /// a lowers-only image config JSON that can be opened by `ImageService`.
    /// Empty memory layers are allowed so snapshots without a persisted memory
    /// snapshot still get a stable runtime config path.
    async fn materialize_mem_image_config(
        &self,
        snapshot_id: &SnapshotId,
        snapshot: &CommittedSnapshot,
        handles: &mut Vec<CacheHandle>,
    ) -> RepositoryResult<PathBuf> {
        let destination = self
            .image_materializer
            .memory_image_config_path(snapshot_id);
        let label = format!("memory for snapshot '{snapshot_id}'");
        let cache_key = runtime_image_cache_key(snapshot_id, "memory/image.json");
        let layers = snapshot
            .memory_layers
            .iter()
            .cloned()
            .map(OverlaybdLayerRef::Managed)
            .collect::<Vec<_>>();
        self.materialize_and_pin(
            &layers,
            &destination,
            MaterializeSpec {
                label: &label,
                cache_key: &cache_key,
                artifact_prefix: "memory_layer",
                allow_empty_layers: true,
                download: None,
            },
            handles,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::super::layout::managed_layer_file_name;
    use super::{MaterializeSpec, PosixFsRuntimeResolver};
    use crate::image::cache::{OverlaybdLayerLocation, OverlaybdLayerStore};
    use crate::snapshot::artifact_cache::LocalArtifactCache;
    use crate::snapshot::{OverlaybdLayerRef, RepositoryError, MEMORY_PREFETCH_ARTIFACT};
    use tempfile::TempDir;

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

    #[tokio::test]
    async fn memory_prefetch_path_detects_artifact_presence() {
        let tempdir = TempDir::new().expect("tempdir");
        let resolver = PosixFsRuntimeResolver::new(
            tempdir.path().join("repository"),
            tempdir.path().join("runtime-cache"),
            test_overlaybd_layer_store(),
            LocalArtifactCache::new(tempdir.path().join("cache"), None).unwrap(),
        );
        let vm_state = tempdir.path().join("vm_state.bin");
        std::fs::write(&vm_state, b"vm").expect("write vm_state");

        assert!(resolver.memory_prefetch_path(&vm_state).await.is_none());

        let prefetch = tempdir.path().join(MEMORY_PREFETCH_ARTIFACT);
        std::fs::write(&prefetch, b"{}").expect("write prefetch");
        assert_eq!(
            resolver.memory_prefetch_path(&vm_state).await,
            Some(prefetch)
        );
    }

    #[tokio::test]
    async fn materialize_image_config_reports_missing_managed_layer_file() {
        let tempdir = TempDir::new().expect("tempdir");
        let cache = LocalArtifactCache::new(tempdir.path().join("cache"), None).unwrap();
        let resolver = PosixFsRuntimeResolver::new(
            tempdir.path().join("repository"),
            tempdir.path().join("runtime-cache"),
            test_overlaybd_layer_store(),
            cache,
        );
        let rootfs_layers = vec![OverlaybdLayerRef::Managed(crate::snapshot::ManagedLayer {
            digest: "sha256:missing".to_string(),
            size: 1,
            uuid: None,
        })];
        let mut handles = Vec::new();
        let err = resolver
            .materialize_and_pin(
                &rootfs_layers,
                Path::new("/tmp/out/image.json"),
                MaterializeSpec {
                    label: "test rootfs",
                    cache_key: "runtime/test/rootfs/image.json",
                    artifact_prefix: "rootfs_layer",
                    allow_empty_layers: false,
                    download: None,
                },
                &mut handles,
            )
            .await
            .expect_err("missing managed layer should be rejected");

        assert!(matches!(err, RepositoryError::ArtifactNotFound { .. }));
        assert!(err
            .to_string()
            .contains(&managed_layer_file_name("sha256:missing")));
    }
}
