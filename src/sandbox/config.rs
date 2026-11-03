//! Persisted settings shared by sandbox backends. These are not the host TOML schema.
use super::guest_metadata::GuestMetadata;
use super::ublk::UblkConfig;
use super::{EnvdAccessToken, ExtraDrive, OverlaybdConfig, SandboxNetworkPolicy};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxCommonConfig {
    pub envd_version: String,
    /// Control plane port inside the VM (default: 49983).
    pub control_plane_port: u16,
    pub env_vars: Option<HashMap<String, String>>,
    pub default_workdir: Option<String>,
    pub default_user: Option<String>,
    pub mmds_metadata: Option<GuestMetadata>,
    /// OverlayBD config for the sandbox user rootfs drive.
    pub rootfs_image_config: Option<OverlaybdConfig>,
    /// Virtual size of the sandbox user rootfs block device in bytes.
    pub rootfs_virtual_size: Option<u64>,
    #[serde(default)]
    pub rootfs_allow_shrink: bool,
    pub extra_drives: Vec<ExtraDrive>,
    pub ublk_config: Option<UblkConfig>,
    pub network_policy: Option<SandboxNetworkPolicy>,
    /// Opaque user-provided JSON passed through to the custom extension
    /// start-fresh / start-resume hooks. Persisted with snapshot configs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<crate::sandbox::CustomExtensionParams>,
    /// Effective disk I/O rate limit for the user rootfs drive. Carried in the
    /// common config so fresh boot and snapshot resume apply the same config
    /// and it persists across pause/resume, instead of each path reaching for
    /// the process-global config.
    #[serde(default)]
    pub disk_rate_limit: crate::cfg::DiskRateLimitConfig,
    /// Runtime-only envd credential. It is re-derived from sandbox metadata on
    /// resume and is intentionally excluded from persisted snapshot configs.
    #[serde(skip)]
    pub envd_access_token: Option<EnvdAccessToken>,
}
