use std::path::PathBuf;

/// Opaque set of local runtime artifacts a sandbox needs while it is alive.
///
/// Sandbox backends construct this from their runtime config, the orchestrator
/// carries it across lifecycle boundaries, and the image-liveness layer decides
/// how to protect the concrete local artifacts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeArtifactSet {
    overlaybd_image_config_paths: Vec<PathBuf>,
}

impl RuntimeArtifactSet {
    /// No local runtime artifacts.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from overlaybd image configs whose local-only layers must stay
    /// available while the sandbox may reopen them.
    pub(crate) fn from_overlaybd_image_configs(overlaybd_image_config_paths: Vec<PathBuf>) -> Self {
        Self {
            overlaybd_image_config_paths,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.overlaybd_image_config_paths.is_empty()
    }

    pub(crate) fn into_overlaybd_image_config_paths(self) -> Vec<PathBuf> {
        self.overlaybd_image_config_paths
    }
}
