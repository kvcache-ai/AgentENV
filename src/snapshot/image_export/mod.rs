//! Snapshot disk images in OCI registries: standalone rootfs export
//! (`aenv-snapshot-image`, see [`SnapshotImageService`]) and publication to
//! the source registry ([`acr`]).

pub(crate) mod acr;
mod regctl;
mod service;
mod target;

pub use service::{SnapshotImageResult, SnapshotImageService};
