use std::path::{Path, PathBuf};

use crate::layer::{load_layer_chain_from_config, LayerChain};
use anyhow::Result;
use overlaybd::config::{ImageConfig, UpperMode};
use serde::{Deserialize, Serialize};

use super::device::UblkDevice;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OverlaybdConfig {
    pub image_config_path: PathBuf,
    pub read_only: bool,
    #[serde(default = "default_runtime_upper_mode")]
    pub runtime_upper_mode: UpperMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layers: Option<LayerChain>,
}

impl OverlaybdConfig {
    pub(crate) async fn resolve_layers(&self) -> Result<LayerChain> {
        resolve_layer_chain(&self.image_config_path, self.layers.as_ref()).await
    }
}

pub(crate) async fn resolve_layer_chain(
    path: &Path,
    layers: Option<&LayerChain>,
) -> Result<LayerChain> {
    if let Some(layers) = layers {
        return Ok(layers.clone());
    }
    let config = crate::cfg::ConfigManager::global_config();
    let managed_repo = config
        .backend
        .oss
        .as_ref()
        .filter(|_| {
            config.snapshot.repository_backend == crate::cfg::SnapshotRepositoryBackendKind::Oss
        })
        .map(|oss| {
            crate::repository::BlobKey::managed_layers_repo_blob_url(
                &oss.bucket,
                oss.prefix.as_deref().unwrap_or_default(),
            )
        });
    load_layer_chain_from_config(path, managed_repo.as_deref()).await
}

fn default_runtime_upper_mode() -> UpperMode {
    UpperMode::LogStructured
}

#[derive(Debug)]
pub(crate) struct OverlaybdRuntimeHandle {
    pub(crate) device: UblkDevice,
    pub(crate) image_config_path: PathBuf,
    pub(crate) actual_virtual_size: u64,
    pub(crate) layers: LayerChain,
}

pub(crate) fn load_runtime_source(path: &Path) -> Result<(ImageConfig, PathBuf)> {
    let path = std::path::absolute(path)?;
    let image = overlaybd::config::load_image_config(&path)?;
    let source_root = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    Ok((image, source_root))
}
