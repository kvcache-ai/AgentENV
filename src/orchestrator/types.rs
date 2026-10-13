use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::sandbox::CustomExtensionParams;
use crate::snapshot::CommandContext;
use crate::types::{ImageConfigs, SandboxId, SandboxResources};

#[derive(Clone)]
pub enum SandboxLaunchSource {
    Snapshot(Box<crate::snapshot::RunnableSnapshot>),
    Image {
        image_ref: String,
        overlaybd_config_path: PathBuf,
        context: Box<CommandContext>,
        resources: Option<crate::types::SandboxResources>,
        extra_drives: Vec<crate::sandbox::ExtraDrive>,
        extra_boot_args: Option<String>,
        /// Raw source image config metadata for the sandbox's resolved images.
        image_configs: Box<ImageConfigs>,
    },
}

#[derive(Clone)]
pub struct CreateSandboxRequest {
    pub source: SandboxLaunchSource,
    /// Launch-time drives that are not part of the source snapshot.
    pub extra_drives: Vec<crate::sandbox::ExtraDrive>,
    /// Whether `extra_drives` already occupy reserved slots in the source
    /// Firecracker state. Restored volume snapshots can be bound before load;
    /// newly requested volumes must replace placeholders after load.
    pub extra_drives_in_snapshot: bool,
    pub timeout: Option<Duration>,
    pub timeout_action: super::SandboxTimeoutAction,
    pub auto_resume: bool,
    pub user_metadata: Option<HashMap<String, String>>,
    pub env_vars: Option<HashMap<String, String>>,
    pub network_policy: crate::sandbox::SandboxNetworkPolicy,
    pub secure: bool,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// Volume mounts requested for this sandbox, keyed by guest path.
    pub volume_mounts: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct SandboxForkChildSpec {
    pub sandbox_id: SandboxId,
    pub volume_mounts: HashMap<String, String>,
    pub extra_drives: Vec<crate::sandbox::ExtraDrive>,
    /// Pairs of `(source_drive_id, replacement_drive_id)`.
    pub replace_drive_ids: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SandboxMemoryResizeState {
    Converged,
    Resizing,
    RolledBack,
    Partial,
}

impl Display for SandboxMemoryResizeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Converged => "converged",
            Self::Resizing => "resizing",
            Self::RolledBack => "rolled_back",
            Self::Partial => "partial",
        };
        f.write_str(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxMemoryHotplugStatus {
    pub previous_requested_size_mib: u32,
    pub target_size_mib: u32,
    pub requested_size_mib: u32,
    pub plugged_size_mib: u32,
    pub total_size_mib: u32,
    pub slot_size_mib: u32,
    pub block_size_mib: u32,
    pub boot_memory_mib: u32,
    pub effective_memory_mib: u32,
    pub accounted_memory_mib: u32,
    pub elapsed_ms: u64,
    pub state: SandboxMemoryResizeState,
    pub rollback_target_size_mib: Option<u32>,
    pub reason: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxLifecycleEventType {
    Create,
    Delete,
    Pause,
    Resume,
    Fork,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SandboxLifecycleEvent {
    pub event_type: SandboxLifecycleEventType,
    pub sandbox_id: SandboxId,
    pub resources: SandboxResources,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SandboxState {
    Creating,
    Resuming,
    Running,
    Resizing,
    Snapshotting,
    Forking,
    Pausing,
    Paused,
    Killing,
}

impl Display for SandboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SandboxState::Creating => "creating",
            SandboxState::Resuming => "resuming",
            SandboxState::Running => "running",
            SandboxState::Resizing => "resizing",
            SandboxState::Snapshotting => "snapshotting",
            SandboxState::Forking => "forking",
            SandboxState::Pausing => "pausing",
            SandboxState::Paused => "paused",
            SandboxState::Killing => "killing",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug)]
pub struct SnapshotCaptureResult {
    pub metadata: super::store::SandboxMetadata,
    pub captured_snapshot: crate::sandbox::CapturedSandboxSnapshot,
}
