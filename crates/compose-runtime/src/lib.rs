//! Shared Compose launch protocol and optional Linux guest runtime.
use serde::{Deserialize, Serialize};

/// Maximum complete startup frame, including image configurations and newline.
pub const MAX_PLAN_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize)]
pub struct ComposeService {
    pub name: String,
    pub image: String,
    #[serde(rename = "localImage")]
    pub local_image: String,
    #[serde(rename = "driveID")]
    pub drive_id: String,
    #[serde(rename = "mountPath")]
    pub mount_path: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ComposePlan {
    pub compose: serde_json::Value,
    pub services: Vec<ComposeService>,
}

#[cfg(feature = "runtime")]
mod process;
#[cfg(feature = "runtime")]
pub mod start;
#[cfg(feature = "runtime")]
pub mod supervisor;
