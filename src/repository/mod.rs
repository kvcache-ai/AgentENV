pub mod backends;
mod build_cache;
pub mod errors;
pub mod interfaces;
mod layer_config;
mod layout;

pub use build_cache::{BuildCacheState, BuildCacheStore};
pub use errors::{RepositoryError, RepositoryResult};
pub use interfaces::{
    BlobBody, BlobLocation, DocumentChange, DocumentUpdate, LayerRole, PublishPolicy, Repository,
};
pub(crate) use layer_config::{managed_layer_config, materialize_volume_backing};
pub(crate) use layout::is_valid_volume_component;
pub use layout::{BlobKey, DocumentCollection, DocumentKey, SnapshotFile};

/// Permits historical compaction only for the configured OSS managed source.
/// Replacing layer references does not grant ownership of the stored objects.
pub(crate) fn can_compact_repository_layer(
    layer: &overlaybd::config::LayerConfig,
    image_repo_blob_url: &str,
) -> bool {
    let config = &crate::cfg::ConfigManager::global_config().backend;
    config.oss.as_ref().is_some_and(|oss| {
        let managed = BlobKey::managed_layers_repo_blob_url(
            &oss.bucket,
            oss.prefix.as_deref().unwrap_or_default(),
        );
        layer
            .effective_repo_blob_url(image_repo_blob_url)
            .trim_end_matches('/')
            == managed
    })
}
