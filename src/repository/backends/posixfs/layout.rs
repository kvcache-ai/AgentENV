use std::path::{Path, PathBuf};

use crate::snapshot::SnapshotId;

pub(super) fn managed_layer_file_name(digest: &str) -> String {
    format!("{}.overlaybd.commit", digest.replace([':', '/'], "_"))
}

/// Committed artifact layout for the POSIX-backed snapshot repository.
pub(crate) struct PosixFsSnapshotArtifactLayout {
    root: PathBuf,
    snapshot_id: SnapshotId,
}

impl PosixFsSnapshotArtifactLayout {
    pub(super) fn new(root: impl Into<PathBuf>, snapshot_id: &SnapshotId) -> Self {
        Self {
            root: root.into(),
            snapshot_id: snapshot_id.clone(),
        }
    }

    pub(super) fn snapshots_dir(root: &Path) -> PathBuf {
        root.join("snapshots")
    }

    pub(super) fn managed_layers_dir(root: &Path) -> PathBuf {
        root.join("managed-layers")
    }

    pub(crate) fn managed_layer_path(root: &Path, digest: &str) -> PathBuf {
        Self::managed_layers_dir(root).join(managed_layer_file_name(digest))
    }

    pub(super) fn snapshot_dir(&self) -> PathBuf {
        Self::snapshots_dir(&self.root).join(self.snapshot_id.to_string())
    }

    pub(super) fn path(&self, relative_path: &str) -> PathBuf {
        self.snapshot_dir().join(relative_path)
    }
}
