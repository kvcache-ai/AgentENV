use std::path::{Path, PathBuf};

use overlaybd::config::{ImageConfig, LayerConfig};
use overlaybd::layer::LayerFile;
use serde::{Deserialize, Serialize};

mod config;

pub(crate) use config::load_layer_chain_from_config;

pub use agentenv_domain::{ExternalLayer, LayerRef, ManagedLayerRef};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LayerSource {
    File(LayerFile),
    Referenced {
        reference: LayerRef,
        local_file: Option<PathBuf>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayerChain {
    pub layers: Vec<LayerSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_repo_blob_url: Option<String>,
}

impl From<Vec<LayerSource>> for LayerChain {
    fn from(layers: Vec<LayerSource>) -> Self {
        Self {
            layers,
            managed_repo_blob_url: None,
        }
    }
}

impl LayerChain {
    pub fn to_runtime_config(&self) -> ImageConfig {
        let lowers: Vec<_> = self
            .layers
            .iter()
            .map(|source| {
                let mut lower = match source {
                    LayerSource::File(file) => LayerConfig::from(file),
                    LayerSource::Referenced {
                        reference: LayerRef::Managed(layer),
                        ..
                    } => LayerConfig {
                        uuid: layer.uuid.clone().unwrap_or_default(),
                        digest: layer.digest.clone(),
                        size: layer.size,
                        repo_blob_url: self.managed_repo_blob_url.clone().unwrap_or_default(),
                        ..Default::default()
                    },
                    LayerSource::Referenced {
                        reference:
                            LayerRef::External(crate::layer::ExternalLayer {
                                digest,
                                size,
                                repo_blob_url,
                            }),
                        ..
                    } => LayerConfig {
                        digest: digest.clone(),
                        size: *size,
                        repo_blob_url: repo_blob_url.clone(),
                        ..Default::default()
                    },
                };
                if let Some(path) = source.path() {
                    lower.file = path.display().to_string();
                }
                lower
            })
            .collect();
        let mut sources = lowers
            .iter()
            .map(|layer| layer.repo_blob_url.as_str())
            .filter(|url| !url.is_empty());
        let repo_blob_url = sources
            .next()
            .filter(|first| {
                sources.all(|url| url.trim_end_matches('/') == first.trim_end_matches('/'))
            })
            .unwrap_or_default()
            .to_owned();
        ImageConfig {
            lowers,
            repo_blob_url,
            ..Default::default()
        }
    }
}

impl LayerSource {
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::File(file) => Some(&file.path),
            Self::Referenced { local_file, .. } => local_file.as_deref(),
        }
    }

    pub fn relocate_file(&mut self, path: PathBuf) {
        match self {
            Self::File(file) => file.path = path,
            Self::Referenced { local_file, .. } => *local_file = Some(path),
        }
    }
}
