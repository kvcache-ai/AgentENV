use std::path::{Path, PathBuf};

use crate::layer::{load_layer_chain_from_config, LayerChain};
use crate::sandbox::overlaybd_snapshot::write_image_config;
use anyhow::{Context, Result};
use overlaybd::config::{UpperConfig, UpperMode};
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper: Option<UpperConfig>,
}

impl OverlaybdConfig {
    pub(crate) async fn resolve_layers(&self) -> Result<LayerChain> {
        resolve_layer_chain(&self.image_config_path, self.layers.as_ref()).await
    }

    pub(crate) fn restore_paused_config(&self) -> Result<()> {
        restore_paused_image_config(
            &self.image_config_path,
            self.layers.as_ref(),
            self.upper.as_ref(),
        )
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

pub(crate) fn restore_paused_image_config(
    path: &Path,
    layers: Option<&LayerChain>,
    upper: Option<&UpperConfig>,
) -> Result<()> {
    let Some(upper) = upper else {
        return Ok(());
    };
    let layers = layers.context("paused runtime has no layer chain")?;
    let mut image = match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
        Err(error) => return Err(error.into()),
    };
    // Preserve runtime extensions while restoring the checkpoint's storage state.
    let projected = layers.to_runtime_config();
    image["lowers"] = serde_json::to_value(projected.lowers)?;
    image["repoBlobUrl"] = serde_json::to_value(projected.repo_blob_url)?;
    image["upper"] = serde_json::to_value(upper)?;
    std::fs::create_dir_all(path.parent().context("paused config parent")?)?;
    write_image_config(path, &image, "paused runtime config")
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
