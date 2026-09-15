use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use overlaybd::config::{DownloadConfig, ImageConfig, LayerConfig};

use crate::image::cache::{OverlaybdLayerLocation, OverlaybdLayerStore};
use crate::sandbox::SandboxSnapshotManifest;
use crate::snapshot::{
    ManagedLayer, OverlaybdLayerRef, RepositoryError, RepositoryResult, ResolvedAttachedDrive,
    SnapshotId, SNAPSHOT_ARTIFACT_LAYOUT,
};

struct MaterializedLower {
    config: LayerConfig,
    remote_repo_blob_url: Option<String>,
}

/// Materializes a runnable snapshot's overlaybd image configs, deciding each
/// lower's node-local placement through the [`OverlaybdLayerStore`].
#[derive(Clone)]
pub(crate) struct RuntimeImageMaterializer {
    runtime_root: PathBuf,
    store: Arc<dyn OverlaybdLayerStore>,
}

impl RuntimeImageMaterializer {
    pub(crate) fn new(runtime_root: PathBuf, store: Arc<dyn OverlaybdLayerStore>) -> Self {
        Self {
            runtime_root,
            store,
        }
    }

    pub(crate) fn snapshot_dir(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.runtime_root.join(snapshot_id.to_string())
    }

    pub(crate) fn rootfs_image_config_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.snapshot_dir(snapshot_id)
            .join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_dir)
            .join(SNAPSHOT_ARTIFACT_LAYOUT.overlaybd_image_config_file)
    }

    pub(crate) fn memory_image_config_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.snapshot_dir(snapshot_id)
            .join("memory")
            .join(SNAPSHOT_ARTIFACT_LAYOUT.overlaybd_image_config_file)
    }

    pub(crate) fn drive_image_config_path(
        &self,
        snapshot_id: &SnapshotId,
        drive_id: &str,
    ) -> PathBuf {
        self.snapshot_dir(snapshot_id)
            .join(SNAPSHOT_ARTIFACT_LAYOUT.drives_dir)
            .join(drive_id)
            .join(SNAPSHOT_ARTIFACT_LAYOUT.overlaybd_image_config_file)
    }

    pub(crate) async fn materialize_image_config<F, Fut>(
        &self,
        layers: &[OverlaybdLayerRef],
        destination: &Path,
        label: &str,
        managed_repo_blob_url: Option<&str>,
        download: Option<DownloadConfig>,
        resolve_managed: F,
    ) -> RepositoryResult<PathBuf>
    where
        F: FnMut(usize, ManagedLayer) -> Fut,
        Fut: Future<Output = RepositoryResult<LayerConfig>>,
    {
        materialize_image_config(
            layers,
            destination,
            label,
            managed_repo_blob_url,
            self.store.as_ref(),
            download,
            resolve_managed,
        )
        .await
    }
}

pub(crate) fn runtime_image_cache_key(snapshot_id: &SnapshotId, relative: &str) -> String {
    format!("runtime/{snapshot_id}/{relative}")
}

pub(crate) fn materialize_image_config_error(label: &str, error: anyhow::Error) -> RepositoryError {
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

pub(crate) fn parse_firecracker_manifest(
    bytes: &[u8],
    manifest_ref: impl AsRef<str>,
) -> RepositoryResult<SandboxSnapshotManifest> {
    serde_json::from_slice(bytes).map_err(|error| {
        RepositoryError::backend(
            format!("parse firecracker manifest '{}'", manifest_ref.as_ref()),
            error,
        )
    })
}

pub(crate) async fn load_firecracker_manifest_from_path(
    path: &Path,
) -> RepositoryResult<SandboxSnapshotManifest> {
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

pub(crate) fn hydrate_runtime_manifest(
    mut manifest: SandboxSnapshotManifest,
    vm_state_path: PathBuf,
    memory_image_config_path: PathBuf,
    rootfs_image_config_path: PathBuf,
    attached_drives: &[ResolvedAttachedDrive],
) -> RepositoryResult<SandboxSnapshotManifest> {
    let extra_drives = attached_drives
        .iter()
        .map(ResolvedAttachedDrive::to_extra_drive)
        .collect::<Vec<_>>();
    manifest = manifest.with_extra_drives(&extra_drives).map_err(|error| {
        RepositoryError::InvalidRequest {
            reason: format!("hydrate attached drives in firecracker manifest: {error:#}"),
        }
    })?;
    manifest.vm_state.path = vm_state_path;
    manifest.memory.image_config_path = memory_image_config_path;
    manifest.rootfs.image_config_path = rootfs_image_config_path;
    Ok(manifest)
}

async fn materialize_image_config<F, Fut>(
    layers: &[OverlaybdLayerRef],
    destination: &Path,
    label: &str,
    managed_repo_blob_url: Option<&str>,
    store: &dyn OverlaybdLayerStore,
    download: Option<DownloadConfig>,
    mut resolve_managed: F,
) -> RepositoryResult<PathBuf>
where
    F: FnMut(usize, ManagedLayer) -> Fut,
    Fut: Future<Output = RepositoryResult<LayerConfig>>,
{
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            RepositoryError::backend(
                format!("create runtime image config dir '{}'", parent.display()),
                error,
            )
        })?;
    }

    let mut materialized = Vec::with_capacity(layers.len());

    for (index, layer) in layers.iter().enumerate() {
        match layer {
            OverlaybdLayerRef::Managed(layer) => {
                let remote_repo_blob_url = managed_repo_blob_url.map(str::to_string);
                let mut lower = resolve_managed(index, layer.clone()).await?;
                attach_local_layer_location(&mut lower, store, remote_repo_blob_url.is_some());
                materialized.push(MaterializedLower {
                    config: lower,
                    remote_repo_blob_url,
                });
            }
            OverlaybdLayerRef::External(layer) => {
                let mut lower = LayerConfig {
                    digest: layer.digest.clone(),
                    size: layer.size,
                    ..Default::default()
                };
                let remote_repo_blob_url =
                    (!layer.repo_blob_url.is_empty()).then(|| layer.repo_blob_url.clone());
                attach_local_layer_location(&mut lower, store, remote_repo_blob_url.is_some());
                materialized.push(MaterializedLower {
                    config: lower,
                    remote_repo_blob_url,
                });
            }
        }
    }

    let first_repo_blob_url = materialized
        .iter()
        .find_map(|lower| lower.remote_repo_blob_url.as_deref());
    let mixed_remote_backends = match first_repo_blob_url {
        Some(first) => materialized
            .iter()
            .filter_map(|lower| lower.remote_repo_blob_url.as_deref())
            .any(|url| first.trim_end_matches('/') != url.trim_end_matches('/')),
        None => false,
    };
    let mut image_config = ImageConfig {
        repo_blob_url: if mixed_remote_backends {
            String::new()
        } else {
            first_repo_blob_url.unwrap_or("").to_string()
        },
        lowers: Vec::with_capacity(materialized.len()),
        download_override: download,
        ..Default::default()
    };

    for mut lower in materialized {
        if mixed_remote_backends {
            if let Some(url) = lower.remote_repo_blob_url {
                lower.config.repo_blob_url = url;
            }
        }
        image_config.lowers.push(lower.config);
    }

    write_image_config(destination, label, &image_config).await
}

/// Point a lower at its node-local placement as decided by the image store.
///
/// The store owns the `file=` vs `dir=` rule: a remote-recoverable layer gets
/// `dir=` (overlaybd falls back to the registry, so the local layer stays
/// reclaimable), while a local-only layer may bind to a local `file=`
/// (which has no fallback). A lower that already has a placement, or whose
/// digest is empty, is left untouched.
fn attach_local_layer_location(
    lower: &mut LayerConfig,
    store: &dyn OverlaybdLayerStore,
    has_remote: bool,
) {
    if !lower.file.is_empty() || !lower.dir.is_empty() || lower.digest.is_empty() {
        return;
    }
    match store.layer_location(&lower.digest, lower.size, has_remote) {
        OverlaybdLayerLocation::LocalFile(file) => {
            lower.file = file.to_string_lossy().into_owned();
        }
        OverlaybdLayerLocation::CacheDir(dir) => {
            lower.dir = dir.to_string_lossy().into_owned();
        }
    }
}

async fn write_image_config(
    destination: &Path,
    label: &str,
    image_config: &ImageConfig,
) -> RepositoryResult<PathBuf> {
    let serialized = serde_json::to_vec_pretty(image_config).map_err(|error| {
        RepositoryError::backend(format!("serialize runtime image config for {label}"), error)
    })?;
    let mut tmp_path = destination.to_path_buf();
    let mut tmp_name = destination
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_else(|| "image.json".into());
    tmp_name.push(".tmp");
    tmp_path.set_file_name(tmp_name);

    tokio::fs::write(&tmp_path, serialized)
        .await
        .map_err(|error| {
            RepositoryError::backend(
                format!("write temp runtime image config '{}'", tmp_path.display()),
                error,
            )
        })?;
    tokio::fs::rename(&tmp_path, destination)
        .await
        .map_err(|error| {
            RepositoryError::backend(
                format!(
                    "move runtime image config '{}' to '{}'",
                    tmp_path.display(),
                    destination.display()
                ),
                error,
            )
        })?;
    Ok(destination.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::ExternalLayer;

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

    #[tokio::test]
    async fn materialize_image_config_keeps_single_repo_blob_url_legacy_shape() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let destination = tempdir.path().join("image.json");
        let layers = vec![
            OverlaybdLayerRef::External(ExternalLayer {
                digest: "sha256:base".to_string(),
                repo_blob_url: "https://registry.example/v2/ns/image/blobs".to_string(),
                size: 10,
            }),
            OverlaybdLayerRef::External(ExternalLayer {
                digest: "sha256:next".to_string(),
                repo_blob_url: "https://registry.example/v2/ns/image/blobs/".to_string(),
                size: 20,
            }),
        ];
        let store = TestOverlaybdLayerStore;

        materialize_image_config(
            &layers,
            &destination,
            "test",
            None,
            &store,
            None,
            |_index, _layer| async move { unreachable!("no managed layers") },
        )
        .await
        .expect("materialize");

        let cfg: ImageConfig =
            serde_json::from_slice(&std::fs::read(&destination).expect("read image config"))
                .expect("parse image config");
        assert_eq!(
            cfg.repo_blob_url,
            "https://registry.example/v2/ns/image/blobs"
        );
        assert!(cfg
            .lowers
            .iter()
            .all(|layer| layer.repo_blob_url.is_empty()));
        assert!(cfg.lowers.iter().all(|layer| layer.file.is_empty()));
        assert!(cfg.lowers.iter().all(|layer| !layer.dir.is_empty()));
    }

    #[tokio::test]
    async fn materialize_image_config_writes_layer_repo_blob_urls_for_mixed_backends() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let destination = tempdir.path().join("image.json");
        let layers = vec![
            OverlaybdLayerRef::External(ExternalLayer {
                digest: "sha256:base".to_string(),
                repo_blob_url: "https://registry.example/v2/ns/image/blobs".to_string(),
                size: 10,
            }),
            OverlaybdLayerRef::Managed(ManagedLayer {
                digest: "sha256:delta".to_string(),
                size: 20,
                uuid: Some("11111111-2222-3333-4444-555555555555".to_string()),
            }),
        ];
        let store = TestOverlaybdLayerStore;

        materialize_image_config(
            &layers,
            &destination,
            "test",
            Some("s3://bucket/prefix/managed-layers"),
            &store,
            None,
            |_index, layer| async move {
                Ok(LayerConfig {
                    digest: layer.digest,
                    size: layer.size,
                    uuid: layer.uuid.unwrap_or_default(),
                    ..Default::default()
                })
            },
        )
        .await
        .expect("materialize");

        let cfg: ImageConfig =
            serde_json::from_slice(&std::fs::read(&destination).expect("read image config"))
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
