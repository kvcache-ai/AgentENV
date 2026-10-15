use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRuntimeVersions {
    pub kernel_version: String,
    pub firecracker_version: String,
    pub envd_version: String,
    /// Legacy executable digest retained for reading existing metadata. New captures omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub firecracker_binary_sha256: Option<String>,
    /// Actual snapshot wire format, independent of the software version string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub firecracker_snapshot_version: Option<String>,
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
            firecracker_binary_sha256: None,
            firecracker_snapshot_version: None,
        }
    }
}
