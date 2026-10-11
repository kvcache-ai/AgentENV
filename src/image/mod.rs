pub(crate) mod buildkit;
pub(crate) mod cache;
pub(crate) mod commit_index;
pub(crate) mod local_layer;
mod metadata;
pub(crate) mod oci_image;
mod reference;
mod resolver;

use crate::snapshot::OverlaybdLayerRef;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub use metadata::ImageBaseContext;
pub(crate) use metadata::{env_vars_from_entries, ImageResolutionMetadata};
pub use resolver::{ImageResolver, ResolvedBlockImage};

/// The image module's single error type.
///
/// Variants exist only for the distinctions a caller actually branches on (the
/// HTTP status it returns); every other, server-side failure funnels into
/// [`ImageError::Other`], which keeps the full `anyhow` context chain for
/// diagnostics. Callers classify by matching the variant — never by downcasting
/// a type-erased error.
#[derive(Debug, Error)]
pub enum ImageError {
    /// The image reference is syntactically invalid or disallowed by config (400).
    #[error("{reason}")]
    InvalidReference { reason: String },
    /// The reference is valid but the registry has no such image/tag (404).
    #[error("{reason}")]
    NotFound { reason: String },
    /// The image exists but its format/shape is not supported by AgentENV
    /// (e.g. overlaybd turbo-OCI, tar-wrapped overlaybd, unknown layer
    /// mediaTypes). This is the publisher's/caller's image problem (400).
    #[error("{reason}")]
    UnsupportedImage { reason: String },
    /// Any other, server-side failure: network, conversion, storage, ... (500).
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// `Result` for the image module; every fallible image API returns this.
pub type ImageResult<T> = std::result::Result<T, ImageError>;

impl ImageError {
    /// `true` when the failure is the caller's fault (bad or missing image) and
    /// should be surfaced as a 4xx rather than a 5xx.
    pub fn is_user_error(&self) -> bool {
        matches!(
            self,
            Self::InvalidReference { .. } | Self::NotFound { .. } | Self::UnsupportedImage { .. }
        )
    }

    /// Prepend human-readable context while preserving the variant. This is the
    /// variant-safe counterpart to [`anyhow::Context`], which would collapse
    /// every variant into [`ImageError::Other`] and so lose the 4xx/5xx
    /// classification when context is added mid-flight.
    pub(crate) fn context(self, context: impl std::fmt::Display) -> Self {
        match self {
            Self::InvalidReference { reason } => Self::InvalidReference {
                reason: format!("{context}: {reason}"),
            },
            Self::NotFound { reason } => Self::NotFound {
                reason: format!("{context}: {reason}"),
            },
            Self::UnsupportedImage { reason } => Self::UnsupportedImage {
                reason: format!("{context}: {reason}"),
            },
            Self::Other(err) => Self::Other(err.context(context.to_string())),
        }
    }
}

/// Initialize the shared image-cache P2P transport during server startup.
pub fn initialize_image_cache_p2p_transport(
    transport: std::sync::Arc<dyn crate::p2p::P2pTransport>,
) {
    let cache = cache::ImageCacheService::shared_from_app_config(
        crate::cfg::ConfigManager::global_config(),
    );
    cache.initialize_p2p_transport(transport);
}

/// Portable description binding ordered repository layers and OCI runtime configuration.
/// Local paths and build identities are excluded from its content address.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublishedImage {
    pub(crate) schema_version: u32,
    pub(crate) os: String,
    pub(crate) architecture: String,
    pub(crate) layers: Vec<OverlaybdLayerRef>,
    pub(crate) config: Value,
}

impl PublishedImage {
    pub(crate) fn new(architecture: String, layers: Vec<OverlaybdLayerRef>, config: Value) -> Self {
        Self {
            schema_version: 1,
            os: "linux".into(),
            architecture,
            layers,
            config,
        }
    }

    pub(crate) fn encode(&self) -> Result<(String, Vec<u8>)> {
        self.validate()?;
        let mut value = serde_json::to_value(self)?;
        value.sort_all_objects();
        let bytes = serde_json::to_vec(&value)?;
        Ok((crate::digest::sha256_digest(&bytes), bytes))
    }

    pub(crate) fn decode(digest: &str, bytes: &[u8]) -> Result<Self> {
        buildkit::validate_digest(digest)?;
        ensure!(
            crate::digest::sha256_digest(bytes) == digest,
            "OverlayBD image description digest mismatch"
        );
        let image: Self =
            serde_json::from_slice(bytes).context("decode OverlayBD image description")?;
        image.validate()?;
        Ok(image)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported OverlayBD image description version"
        );
        ensure!(self.os == "linux", "unsupported image operating system");
        ensure!(
            matches!(self.architecture.as_str(), "amd64" | "arm64"),
            "unsupported image architecture"
        );
        ensure!(
            self.config.is_object(),
            "image runtime configuration must be an object"
        );
        ensure!(
            !self.layers.is_empty() && self.layers.len() <= 1024,
            "invalid image layer count"
        );
        for layer in &self.layers {
            let OverlaybdLayerRef::Managed(layer) = layer else {
                anyhow::bail!("published images must own their layers in the repository");
            };
            buildkit::validate_digest(&layer.digest)?;
            ensure!(layer.size > 0, "empty image layer");
        }
        Ok(())
    }

    pub(crate) fn key(digest: &str) -> Result<String> {
        buildkit::validate_digest(digest)?;
        Ok(format!("catalog/images/{digest}.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::ManagedLayer;
    use serde_json::json;

    fn image() -> PublishedImage {
        PublishedImage::new(
            "amd64".into(),
            [b"base".as_slice(), b"delta".as_slice()]
                .into_iter()
                .map(|data| {
                    OverlaybdLayerRef::Managed(ManagedLayer {
                        digest: crate::digest::sha256_digest(data),
                        size: data.len() as u64,
                        uuid: None,
                    })
                })
                .collect(),
            json!({"Env": ["A=1", "B=2"], "Cmd": ["serve"], "Labels": {"b": "2", "a": "1"}}),
        )
    }

    #[test]
    fn identity_binds_ordered_layers_architecture_and_runtime_config() -> Result<()> {
        let original = image();
        let (digest, bytes) = original.encode()?;
        assert_eq!(crate::digest::sha256_digest(&bytes), digest);
        assert_eq!(PublishedImage::decode(&digest, &bytes)?.encode()?.0, digest);
        for change in [0, 1, 2, 3] {
            let mut modified = original.clone();
            match change {
                0 => modified.layers.reverse(),
                1 => modified.architecture = "arm64".into(),
                2 => modified.config["Cmd"] = json!(["another-command"]),
                _ => modified.config["Env"] = json!(["B=2", "A=1"]),
            }
            assert_ne!(modified.encode()?.0, digest);
        }
        let mut reordered = original.clone();
        reordered.config = serde_json::from_str(
            r#"{"Labels":{"a":"1","b":"2"},"Cmd":["serve"],"Env":["A=1","B=2"]}"#,
        )?;
        assert_eq!(reordered.encode()?.0, digest);
        assert!(PublishedImage::decode(&digest, &serde_json::to_vec_pretty(&original)?).is_err());
        Ok(())
    }

    #[test]
    fn rejects_invalid_and_node_local_descriptions() -> Result<()> {
        let mut value = serde_json::to_value(image())?;
        value["localPath"] = json!("/node-a/image.json");
        let bytes = serde_json::to_vec(&value)?;
        assert!(PublishedImage::decode(&crate::digest::sha256_digest(&bytes), &bytes).is_err());
        let mut invalid = image();
        invalid.schema_version = 2;
        assert!(invalid.encode().is_err());
        invalid = image();
        invalid.layers.clear();
        assert!(invalid.encode().is_err());
        assert!(PublishedImage::key("sha256:../../outside").is_err());
        Ok(())
    }
}
