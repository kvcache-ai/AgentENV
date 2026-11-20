use std::path::{Path, PathBuf};

use overlaybd::config::LayerConfig;

const SNAPSHOT_DELTA_LAYER_FILE: &str = "snapshot.commit";
/// Per-restack unique variant of [`SNAPSHOT_DELTA_LAYER_FILE`]:
/// `snapshot-<uuid>.commit`. Every restack seals its upper under a fresh
/// name so a retry can never unlink or overwrite a lower that the live
/// disk config still references.
const SNAPSHOT_DELTA_LAYER_FILE_PREFIX: &str = "snapshot-";
const SNAPSHOT_DELTA_LAYER_FILE_SUFFIX: &str = ".commit";
const SELF_CONTAINED_BASE_LAYER_FILE: &str = "managed-base.commit";
/// ZFile-recontainerized variant of [`SNAPSHOT_DELTA_LAYER_FILE`]. No longer
/// produced: capture-time compression was removed and local layers always
/// stay raw. Kept in the delta allowlist so layers in repositories published
/// while the old capture-time switches existed are still recognized.
pub(crate) const SNAPSHOT_ZFILE_DELTA_LAYER_FILE: &str = "snapshot.zfile.commit";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalLayer {
    pub(crate) path: PathBuf,
    pub(crate) digest: String,
    pub(crate) size: u64,
}

pub(crate) fn rootfs_layer_is_runtime_generated_delta(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    if matches!(
        name,
        SNAPSHOT_DELTA_LAYER_FILE
            | SELF_CONTAINED_BASE_LAYER_FILE
            | SNAPSHOT_ZFILE_DELTA_LAYER_FILE
    ) {
        return true;
    }
    // The per-restack unique naming: `snapshot-<uuid>.commit`. Accept every
    // textual UUID form so consumers recognize sealed layers regardless of
    // how the identifier was rendered.
    name.strip_prefix(SNAPSHOT_DELTA_LAYER_FILE_PREFIX)
        .and_then(|rest| rest.strip_suffix(SNAPSHOT_DELTA_LAYER_FILE_SUFFIX))
        .is_some_and(|uuid| uuid::Uuid::parse_str(uuid).is_ok())
}

impl From<LocalLayer> for LayerConfig {
    fn from(layer: LocalLayer) -> Self {
        Self {
            file: layer.path.display().to_string(),
            digest: layer.digest,
            size: layer.size,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_generated_delta_names() {
        assert!(rootfs_layer_is_runtime_generated_delta(Path::new(
            "/a/b/snapshot.commit"
        )));
        assert!(rootfs_layer_is_runtime_generated_delta(Path::new(
            "/a/b/managed-base.commit"
        )));
        assert!(rootfs_layer_is_runtime_generated_delta(Path::new(
            "/a/b/snapshot.zfile.commit"
        )));
        assert!(!rootfs_layer_is_runtime_generated_delta(Path::new(
            "/a/b/overlaybd.commit"
        )));
        assert!(!rootfs_layer_is_runtime_generated_delta(Path::new(
            "/a/b/snapshot.commit.tmp"
        )));
    }

    #[test]
    fn unique_restack_delta_names_are_runtime_generated() {
        let uuid = uuid::Uuid::now_v7().simple();
        assert!(rootfs_layer_is_runtime_generated_delta(Path::new(
            &format!("/runtime-dir/snapshot-{uuid}.commit")
        )));
        let hyphenated = uuid::Uuid::now_v7().hyphenated();
        assert!(rootfs_layer_is_runtime_generated_delta(Path::new(
            &format!("/runtime-dir/snapshot-{hyphenated}.commit")
        )));
        // Near-misses must not pass.
        assert!(!rootfs_layer_is_runtime_generated_delta(Path::new(
            "/a/b/snapshot-.commit"
        )));
        assert!(!rootfs_layer_is_runtime_generated_delta(Path::new(
            "/a/b/snapshot-not-a-uuid.commit"
        )));
        assert!(!rootfs_layer_is_runtime_generated_delta(Path::new(
            "/a/b/snapshot.commit-20240101"
        )));
        let uuid = uuid::Uuid::now_v7().simple();
        assert!(!rootfs_layer_is_runtime_generated_delta(Path::new(
            &format!("/a/b/other-snapshot-{uuid}.commit")
        )));
    }
}
