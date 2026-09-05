mod access;
mod backend;
mod cpu_affinity;
pub(crate) mod custom_extension;
mod envd;
mod extra_drive;
mod firecracker;
#[cfg(test)]
pub(crate) mod mock;
mod network;
mod process;
mod ublk;

use std::{collections::HashMap, path::PathBuf};

pub(crate) use custom_extension::{
    custom_extension_params_is_empty, CustomExtensionClient, CustomExtensionParams,
};

use crate::types::{ImageConfigs, SandboxId};

pub use ::envd::process::Signal;
pub use access::{EnvdAccessToken, SandboxAccessTokenGenerator};
pub use backend::{
    CapturedSandboxSnapshot, PausedSandboxState, RuntimeArtifactSet, SandboxBackend,
    SandboxBackendFactory, SandboxCaptureError, SandboxCaptureResult, SandboxExecutor,
    SandboxForkResult, SandboxForkSpec, SandboxRuntimeInfo,
};
pub(crate) use cpu_affinity::{bind_process as bind_process_cpu_affinity, CpuAffinityError};
pub use cpu_affinity::{CpuAffinityOutcome, CpuAffinityRequest};
pub use extra_drive::{
    normalize_mount_path, normalize_mount_path_for_drive, validate_drive_id, validate_mount_path,
    validate_sub_path, ExtraDrive,
};
pub use firecracker::{
    FirecrackerCapturedSnapshot, FirecrackerCommonConfig, FirecrackerPausedState, FirecrackerPool,
    FirecrackerRuntimePolicy, FirecrackerSandbox, FirecrackerSandboxConfig,
    FirecrackerSandboxFactory, FirecrackerSnapshotConfig, FirecrackerSnapshotManifest,
};
pub(crate) use network::{prepare_runtime as prepare_network_runtime, NetworkManager};
pub use network::{
    BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
    ALL_INTERNET_TRAFFIC_CIDR,
};
pub use process::{Executor, ProcessHandle, ProcessOpts, ProcessOutput};
pub(crate) use ublk::OverlaybdCompactOutput;
pub use ublk::{OverlaybdConfig, UblkBackend, UblkConfig, UblkDaemonConfig, UblkDeviceManager};

#[derive(Clone, Debug)]
pub struct FreshSandboxBuildSpec {
    pub image_config_path: PathBuf,
    pub context: crate::snapshot::CommandContext,
    pub resources: crate::types::SandboxResources,
    pub extra_drives: Vec<ExtraDrive>,
    pub extra_boot_args: Option<String>,
}

/// High-level launch request consumed by sandbox backend factories.
///
/// Carries launch-time inputs from upper layers (for example orchestrator)
/// into backend construction.
#[derive(Clone, Debug, Default)]
pub struct SandboxLaunchConfig {
    /// Stable sandbox identity
    pub sandbox_id: SandboxId,
    /// Snapshot/template identity
    pub snapshot_id: String,
    /// One-off environment variable overrides to apply on top of snapshot defaults.
    pub env_vars: Option<HashMap<String, String>>,
    /// Per-sandbox egress policy.
    pub network: Option<SandboxNetworkPolicy>,
    /// Opaque extra key-value pairs merged into the MMDS metadata JSON.
    /// The sandbox layer does not interpret these; they are passed through
    /// as-is to the VM via the Firecracker MMDS interface.
    pub extra_mmds: serde_json::Map<String, serde_json::Value>,
    /// Additional drives supplied for this launch, such as persistent volume mounts.
    pub extra_drives: Vec<ExtraDrive>,
    /// Whether `extra_drives` already occupy reserved slots in the source
    /// Firecracker state and can be bound before snapshot load.
    pub extra_drives_in_snapshot: bool,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    /// Takes precedence over any value persisted in the source snapshot.
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// Runtime-only credential used by envd. The token is never serialized and
    /// its Debug representation is redacted.
    pub envd_access_token: Option<EnvdAccessToken>,
}

impl SandboxLaunchConfig {
    pub(crate) fn new(sandbox_id: SandboxId, snapshot_id: impl Into<String>) -> Self {
        Self {
            sandbox_id,
            snapshot_id: snapshot_id.into(),
            env_vars: None,
            network: None,
            extra_mmds: serde_json::Map::new(),
            extra_drives: Vec::new(),
            extra_drives_in_snapshot: false,
            custom_extension_params: None,
            envd_access_token: None,
        }
    }

    pub(crate) fn with_image_configs(mut self, image_configs: &ImageConfigs) -> Self {
        if !image_configs.is_empty() {
            self.extra_mmds
                .insert("imageConfigs".to_string(), image_configs.to_value());
        }
        self
    }
}
