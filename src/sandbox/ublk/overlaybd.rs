use std::path::PathBuf;

use overlaybd::config::UpperMode;
use serde::{Deserialize, Serialize};

use super::device::UblkDevice;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OverlaybdConfig {
    pub image_config_path: PathBuf,
    pub read_only: bool,
    #[serde(default = "default_runtime_upper_mode")]
    pub runtime_upper_mode: UpperMode,
}

fn default_runtime_upper_mode() -> UpperMode {
    UpperMode::LogStructured
}

#[derive(Clone, Debug)]
pub(crate) struct OverlaybdRuntimeHandle {
    pub(crate) device: UblkDevice,
    pub(crate) image_config_path: PathBuf,
    pub(crate) actual_virtual_size: u64,
}
