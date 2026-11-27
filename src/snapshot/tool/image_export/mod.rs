//! Standalone rootfs OCI image export (`aenv-snapshot export-image`, `aenv-snapshot-image`).

mod regctl;
mod service;
mod target;

pub use service::{SnapshotImageResult, SnapshotImageService};
