pub(crate) mod buildkit;
pub(crate) mod cache;
pub(crate) mod commit_index;
mod metadata;
pub(crate) mod oci_image;
mod reference;
mod resolver;
#[cfg(test)]
mod test_support;

use thiserror::Error;

pub use agentenv_domain::{ImageTagRecord, PublishedImage};
pub(crate) use cache::attach_local_layer_locations;
pub use metadata::ImageBaseContext;
pub(crate) use metadata::{env_vars_from_entries, ImageResolutionMetadata};
pub use oci_image::LayerConversionKey;
pub(crate) use resolver::PulledPublication;
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

/// Short image references in this namespace name a published image by tag, as
/// in `aenv/app:v1`. References with a registry host, such as
/// `docker.io/aenv/app:v1`, remain OCI references.
pub(crate) const IMAGE_TAG_PREFIX: &str = "aenv/";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::{LayerRef, ManagedLayerRef};
    use anyhow::Result;
    use serde_json::json;

    fn image() -> PublishedImage {
        PublishedImage::new(
            "amd64".into(),
            [b"base".as_slice(), b"delta".as_slice()]
                .into_iter()
                .map(|data| {
                    LayerRef::Managed(ManagedLayerRef {
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
        Ok(())
    }
}
