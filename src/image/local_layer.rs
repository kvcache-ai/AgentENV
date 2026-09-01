use std::path::{Path, PathBuf};

use overlaybd::config::LayerConfig;

const SNAPSHOT_DELTA_LAYER_FILE: &str = "snapshot.commit";
const SELF_CONTAINED_BASE_LAYER_FILE: &str = "managed-base.commit";
/// ZFile-recontainerized variant of [`SNAPSHOT_DELTA_LAYER_FILE`], produced
/// when a template build requests compressed seal output.
pub(crate) const SNAPSHOT_ZFILE_DELTA_LAYER_FILE: &str = "snapshot.zfile.commit";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalLayer {
    pub(crate) path: PathBuf,
    pub(crate) digest: String,
    pub(crate) size: u64,
}

pub(crate) fn rootfs_layer_is_runtime_generated_delta(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
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
}
