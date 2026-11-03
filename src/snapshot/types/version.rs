use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRuntimeVersions {
    pub kernel_version: String,
    #[serde(rename = "firecracker_version")]
    pub vmm_version: String,
    pub envd_version: String,
    #[serde(default)]
    pub tools_drive_version: String,
}

impl SnapshotRuntimeVersions {
    pub fn new(
        kernel_version: String,
        vmm_version: String,
        envd_version: String,
        tools_drive_version: String,
    ) -> Self {
        Self {
            kernel_version,
            vmm_version,
            envd_version,
            tools_drive_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn vmm_version_preserves_legacy_wire_name() {
        let legacy = serde_json::json!({"kernel_version":"kernel", "firecracker_version":"fc",
            "envd_version":"envd", "tools_drive_version":"tools"});
        let versions: SnapshotRuntimeVersions = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(versions.vmm_version, "fc");
        assert_eq!(serde_json::to_value(versions).unwrap(), legacy);
    }
}
