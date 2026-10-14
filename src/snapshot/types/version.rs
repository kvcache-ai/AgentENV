use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRuntimeVersions {
    pub kernel_version: String,
    pub firecracker_version: String,
    pub envd_version: String,
    #[serde(default)]
    pub tools_drive_version: String,
}

impl SnapshotRuntimeVersions {
    pub fn new(
        kernel_version: String,
        firecracker_version: String,
        envd_version: String,
        tools_drive_version: String,
    ) -> Self {
        Self {
            kernel_version,
            firecracker_version,
            envd_version,
            tools_drive_version,
        }
    }
}
