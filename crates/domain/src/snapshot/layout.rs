//! Relative names of persisted snapshot artifacts in OSS and POSIX repositories.

use super::SnapshotId;

pub const LAYERS_DIR: &str = "managed-layers";

pub fn snapshot_dir(id: &SnapshotId) -> String {
    format!("artifacts/{id}")
}

pub fn posix_snapshot_dir(id: &SnapshotId) -> String {
    format!("snapshots/{id}")
}

pub fn layer_path(digest: &str) -> String {
    format!("{LAYERS_DIR}/{digest}")
}

pub fn posix_layer_path(digest: &str) -> String {
    format!(
        "{LAYERS_DIR}/{}.overlaybd.commit",
        digest.replace([':', '/'], "_")
    )
}
