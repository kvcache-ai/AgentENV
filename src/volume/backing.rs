use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agentenv_domain::VolumeRecord;
use overlaybd::config::{ImageConfig, LayerConfig};
use tokio::process::Command;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use super::error::VolumeError;
use crate::layer::{retain_layer_file_blocking, write_image_config, LayerRef};
use crate::repository::Repository;

const BYTES_PER_MB: u64 = 1024 * 1024;

/// Node-local materialized OverlayBD `image.json` path cached for a volume,
/// keyed by the layer stack it was materialized from so stale local configs
/// are invalidated whenever SQL `backing_layers` advances.
#[derive(Clone, Debug)]
struct LocalVolumeBacking {
    layers: Vec<LayerRef>,
    image_config: PathBuf,
}

/// Manages node-local OverlayBD backing directories, cached `image.json`
/// materializations, empty ext4 disk provisioning, and local child cloning.
#[derive(Clone)]
pub(super) struct LocalVolumeBackingStore {
    root: PathBuf,
    local_backings: Arc<RwLock<HashMap<String, LocalVolumeBacking>>>,
}

impl LocalVolumeBackingStore {
    pub(super) fn new(root: PathBuf) -> Self {
        Self {
            root,
            local_backings: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(super) fn data_dir(&self, volume_id: &str) -> PathBuf {
        self.root.join("data").join(volume_id)
    }

    pub(super) async fn local_backing(&self, record: &VolumeRecord) -> Option<PathBuf> {
        let candidate = {
            let local = self.local_backings.read().await;
            local
                .get(&record.id)
                .filter(|backing| backing.layers == record.backing_layers)
                .map(|backing| backing.image_config.clone())
        }?;
        tokio::fs::try_exists(&candidate)
            .await
            .ok()
            .filter(|exists| *exists)
            .map(|_| candidate)
    }

    pub(super) async fn cache(&self, record: &VolumeRecord, image_config: PathBuf) {
        self.local_backings.write().await.insert(
            record.id.clone(),
            LocalVolumeBacking {
                layers: record.backing_layers.clone(),
                image_config,
            },
        );
    }

    pub(super) async fn remove_and_clean(&self, volume_id: &str) {
        let backing_directory = self.data_dir(volume_id);
        self.local_backings.write().await.remove(volume_id);
        if let Err(error) = tokio::fs::remove_dir_all(&backing_directory).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    volume_id = %volume_id,
                    path = %backing_directory.display(),
                    %error,
                    "failed to clean deleted volume's node-local backing"
                );
            }
        }
    }

    pub(super) async fn materialize(
        &self,
        repository: &dyn Repository,
        record: &VolumeRecord,
    ) -> Result<Option<PathBuf>, VolumeError> {
        if let Some(path) = self.local_backing(record).await {
            return Ok(Some(path));
        }
        if record.backing_layers.is_empty() {
            return Ok(None);
        }
        let destination = self.data_dir(&record.id).join("image.json");
        debug!(
            volume_id = %record.id,
            layer_count = record.backing_layers.len(),
            "materializing volume backing"
        );
        let path = crate::repository::materialize_layer_config(
            repository,
            &record.backing_layers,
            &destination,
        )
        .await
        .map_err(|error| VolumeError::Storage(error.to_string()))?;
        debug!(
            volume_id = %record.id,
            path = %path.display(),
            "volume backing materialized"
        );
        self.cache(record, path.clone()).await;
        Ok(Some(path))
    }

    pub(super) async fn create_empty(
        &self,
        volume_id: &str,
        size_mb: u64,
    ) -> Result<PathBuf, VolumeError> {
        let directory = self.data_dir(volume_id);
        let raw = directory.join("base.ext4");
        let commit = directory.join("base.commit");
        let image_config = directory.join("image.json");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;

        let result = async {
            let file = tokio::fs::File::create(&raw)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            file.set_len(
                size_mb
                    .checked_mul(BYTES_PER_MB)
                    .ok_or(VolumeError::InvalidSize)?,
            )
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;
            drop(file);
            let status = Command::new("mkfs.ext4")
                .args(["-q", "-F"])
                .arg(&raw)
                .status()
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            if !status.success() {
                return Err(VolumeError::Storage(format!(
                    "mkfs.ext4 exited with status {status}"
                )));
            }
            overlaybd::tools::package_raw_as_overlaybd(&raw, &commit)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            let config = ImageConfig {
                lowers: vec![LayerConfig {
                    file: commit.to_string_lossy().into_owned(),
                    ..LayerConfig::default()
                }],
                ..ImageConfig::default()
            };
            write_volume_image_config(&image_config, config).await?;
            let _ = tokio::fs::remove_file(&raw).await;
            Ok(image_config)
        }
        .await;

        if result.is_err() {
            let _ = tokio::fs::remove_dir_all(&directory).await;
        }
        result
    }

    pub(super) async fn create_child(
        &self,
        volume_id: &str,
        parent_config: &Path,
    ) -> Result<PathBuf, VolumeError> {
        let directory = self.data_dir(volume_id);
        let image_config = directory.join("image.json");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;

        let parent_config_owned = parent_config.to_path_buf();
        let directory_clone = directory.clone();
        let image_config_clone = image_config.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<PathBuf, VolumeError> {
            let bytes = std::fs::read(&parent_config_owned)
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            let mut config: ImageConfig = serde_json::from_slice(&bytes)
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            let parent_dir = parent_config_owned.parent();
            for (index, layer) in config.lowers.iter_mut().enumerate() {
                if layer.file.is_empty() {
                    continue;
                }
                let raw_source = Path::new(&layer.file);
                let source = if raw_source.is_relative() {
                    parent_dir
                        .map_or_else(|| raw_source.to_path_buf(), |base| base.join(raw_source))
                } else {
                    raw_source.to_path_buf()
                };
                let extension = source
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .unwrap_or("layer");
                let destination = directory_clone.join(format!("lower-{index}.{extension}"));
                retain_layer_file_blocking(&source, &destination).map_err(|error| {
                    VolumeError::Storage(format!(
                        "clone volume layer '{}': {error}",
                        source.display()
                    ))
                })?;
                layer.file = destination.to_string_lossy().into_owned();
            }
            write_image_config(&image_config_clone, &config, "volume image config")
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            Ok(image_config_clone)
        })
        .await
        .map_err(|error| VolumeError::Storage(format!("clone volume backing task: {error}")))?;

        if result.is_err() {
            let _ = tokio::fs::remove_dir_all(&directory).await;
        }
        result
    }
}

async fn write_volume_image_config(path: &Path, config: ImageConfig) -> Result<(), VolumeError> {
    let dest = path.to_path_buf();
    tokio::task::spawn_blocking(move || write_image_config(&dest, &config, "volume image config"))
        .await
        .map_err(|error| VolumeError::Storage(format!("write volume image config task: {error}")))?
        .map_err(|error| VolumeError::Storage(error.to_string()))
}
