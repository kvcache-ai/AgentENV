//! Durable blob and content-addressed layer storage shared across nodes.

pub mod backends;
pub mod errors;
pub mod interfaces;
mod layers;
mod layout;

pub(crate) use backends::managed_layers_repo_blob_url_from_config;
pub use backends::{open_from_config, open_from_storage_config};
pub use errors::{RepositoryError, RepositoryResult};
pub use interfaces::{BlobBody, BlobLocation, LayerRole, PublishPolicy, Repository};
pub(crate) use layers::{
    layer_chain_for_refs, materialize_layer_config, restore_missing_managed_layers,
    same_repo_blob_url, store_disk_layers_with_uploads, RepositoryLayerWriter,
};
pub use layout::{BlobKey, SnapshotFile};
