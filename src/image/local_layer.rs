use std::path::{Path, PathBuf};

use overlaybd::config::LayerConfig;

const SNAPSHOT_DELTA_LAYER_FILE: &str = "snapshot.commit";
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
    let name = path.file_name().and_then(|name| name.to_str());
    let unique_snapshot = name
        .and_then(|name| name.strip_prefix("snapshot-"))
        .and_then(|name| name.strip_suffix(".commit"))
        .is_some_and(|id| {
            id.len() == 32
                && id
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        });
    unique_snapshot
        || matches!(
            name,
            Some(
                SNAPSHOT_DELTA_LAYER_FILE
                    | SELF_CONTAINED_BASE_LAYER_FILE
                    | SNAPSHOT_ZFILE_DELTA_LAYER_FILE
            )
        )
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
    fn unique_runtime_delta_names_are_narrowly_recognized() {
        assert!(rootfs_layer_is_runtime_generated_delta(Path::new(
            "/runtime/inherited-layers/0000/snapshot-0123456789abcdef0123456789abcdef.commit"
        )));
        for name in [
            "snapshot-.commit",
            "snapshot-0123456789abcdef.commit",
            "snapshot-0123456789abcdef0123456789abcdeg.commit",
            "snapshot-0123456789abcdef0123456789abcdef.commit.tmp",
            "unrelated-0123456789abcdef0123456789abcdef.commit",
        ] {
            assert!(
                !rootfs_layer_is_runtime_generated_delta(Path::new(name)),
                "{name}"
            );
        }
    }
}
