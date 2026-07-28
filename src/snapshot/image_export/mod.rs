//! Standalone snapshot rootfs image export (`aenv-snapshot-image`); see
//! [`SnapshotImageService`].

mod regctl;
mod service;
mod target;

pub use service::{SnapshotImageResult, SnapshotImageService};
