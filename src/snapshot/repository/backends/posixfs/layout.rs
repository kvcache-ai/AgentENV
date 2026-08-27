use std::path::{Path, PathBuf};

use crate::snapshot::{SnapshotAlias, SnapshotId};

pub(super) const POSIXFS_SNAPSHOT_COMMIT_MARKER: &str = "commit";
const LOCK_SUFFIX: &str = ".lock";
const VOLUME_CATALOG_DIR: &str = "volumes";

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

    pub(super) fn catalog_dir(root: &Path) -> PathBuf {
        root.join("catalog")
    }

    pub(super) fn aliases_dir(root: &Path) -> PathBuf {
        Self::catalog_dir(root).join("aliases")
    }

    pub(super) fn records_dir(root: &Path) -> PathBuf {
        Self::catalog_dir(root).join("records")
    }

    pub(super) fn volumes_dir(root: &Path) -> PathBuf {
        Self::catalog_dir(root).join(VOLUME_CATALOG_DIR)
    }

    pub(super) fn volume_record_path(root: &Path, volume_id: &str) -> PathBuf {
        Self::volumes_dir(root).join(format!("{volume_id}.json"))
    }

    pub(super) fn volume_catalog_lock_path(root: &Path) -> PathBuf {
        Self::volumes_dir(root).join(format!("catalog{LOCK_SUFFIX}"))
    }

    pub(super) fn alias_path(root: &Path, alias: &SnapshotAlias) -> PathBuf {
        Self::aliases_dir(root).join(alias.to_string())
    }

    pub(super) fn alias_lock_path(root: &Path, alias: &SnapshotAlias) -> PathBuf {
        Self::aliases_dir(root).join(format!("{alias}{LOCK_SUFFIX}"))
    }

    pub(super) fn record_path(root: &Path, id: &SnapshotId) -> PathBuf {
        Self::records_dir(root).join(format!("{id}.json"))
    }

    pub(super) fn record_lock_path(root: &Path, id: &SnapshotId) -> PathBuf {
        Self::records_dir(root).join(format!("{id}{LOCK_SUFFIX}"))
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
