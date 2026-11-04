//! Persistent snapshot records and publication inputs.

use super::{CommandContext, SnapshotAlias, SnapshotId, SnapshotRuntimeVersions, StartupCommand};
use crate::{
    CustomExtensionParams, ImageConfigs, LayerRef as OverlaybdLayerRef,
    ManagedLayerRef as ManagedLayer, SandboxResources, VirtualizationMode, VolumeMode,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommittedAttachedDrive {
    Overlaybd {
        drive_id: String,
        layers: Vec<OverlaybdLayerRef>,
        read_only: bool,
        virtual_size: u64,
        #[serde(default)]
        mount_path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sub_path: Option<PathBuf>,
    },
}

/// Startup pack descriptor persisted in the committed record when — and only
/// when — the pack was recorded and its repository backend saved the manifest.
/// It is absent for older snapshots and best-effort recording failures.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryStartupPackInfo {
    pub pack_size: u64,
    pub mem_virtual_size: u64,
    /// Hex-encoded SHA-256 of the complete encoded startup manifest.
    /// The field name is retained for committed-record compatibility.
    pub index_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotVolume {
    pub mount_path: String,
    #[serde(default)]
    pub mode: VolumeMode,
    pub size_mb: u64,
    pub layers: Vec<OverlaybdLayerRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotPublishMetadata {
    pub id: SnapshotId,
    pub snapshot_type: SnapshotPlacement,
    pub alias: Option<SnapshotAlias>,
    pub source: SnapshotPublishSource,
    pub context: CommandContext,
    pub startup: Option<StartupCommand>,
    pub resources: SandboxResources,
    pub runtime_versions: SnapshotRuntimeVersions,
    pub virtualization_mode: VirtualizationMode,
    pub image_configs: ImageConfigs,
    pub volume_snapshots: Vec<SnapshotVolume>,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    /// Template launches inherit it unless overridden at create time.
    pub custom_extension_params: Option<CustomExtensionParams>,
}

#[cfg(any(test, feature = "test-support"))]
impl SnapshotPublishMetadata {
    pub fn mock() -> Self {
        Self {
            id: SnapshotId::generate(),
            snapshot_type: SnapshotPlacement::Distributed,
            alias: None,
            source: SnapshotPublishSource::Template,
            context: CommandContext::default(),
            startup: None,
            resources: SandboxResources::default(),
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "kernel".to_string(),
                firecracker_version: "firecracker".to_string(),
                envd_version: "envd".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: VirtualizationMode::default(),
            image_configs: ImageConfigs::new(),
            volume_snapshots: Vec::new(),
            custom_extension_params: None,
        }
    }
}

/// Storage availability requested for a reusable snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPlacement {
    Local,
    #[default]
    Distributed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SnapshotPublishSource {
    Template,
    Sandbox {
        source_sandbox_id: String,
        /// User metadata of the source sandbox at capture time. Informational
        /// provenance only; it is persisted into the snapshot record but never
        /// affects launching.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_sandbox_metadata: Option<HashMap<String, String>>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotSourceKind {
    Template,
    Sandbox,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemplateBuildStatus {
    Waiting,
    Building,
    Ready,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub struct TemplateBuildErrorReason {
    pub message: String,
    pub step: Option<String>,
}

impl<'de> Deserialize<'de> for TemplateBuildErrorReason {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Reason {
            Structured {
                message: String,
                #[serde(default)]
                step: Option<String>,
            },
            LegacyString(String),
        }

        match Reason::deserialize(deserializer)? {
            Reason::Structured { message, step } => Ok(Self { message, step }),
            Reason::LegacyString(message) => Ok(Self::new(message)),
        }
    }
}

impl TemplateBuildErrorReason {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            step: None,
        }
    }

    pub fn with_step(message: impl Into<String>, step: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            step: Some(step.into()),
        }
    }
}

impl fmt::Display for TemplateBuildErrorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemplateBuildInfo {
    pub status: TemplateBuildStatus,
    pub started_at_unix_ms: Option<i64>,
    pub finished_at_unix_ms: Option<i64>,
    pub error_reason: Option<TemplateBuildErrorReason>,
}

impl TemplateBuildInfo {
    pub fn waiting() -> Self {
        Self {
            status: TemplateBuildStatus::Waiting,
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            error_reason: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SnapshotSource {
    Template {
        build: TemplateBuildInfo,
    },
    Sandbox {
        source_sandbox_id: String,
        /// User metadata of the source sandbox at capture time. Informational
        /// provenance only; it never affects launching.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_sandbox_metadata: Option<HashMap<String, String>>,
    },
}

fn validate_snapshot_metadata(
    snapshot_type: SnapshotPlacement,
    source_is_sandbox: bool,
    alias: Option<&SnapshotAlias>,
) -> Result<(), String> {
    if snapshot_type == SnapshotPlacement::Local && !source_is_sandbox {
        return Err("Local snapshots must originate from a sandbox".to_string());
    }
    if snapshot_type == SnapshotPlacement::Local && alias.is_some() {
        return Err("Local snapshots do not support aliases".to_string());
    }
    Ok(())
}

impl SnapshotPublishMetadata {
    pub fn validate(&self) -> Result<(), String> {
        validate_snapshot_metadata(
            self.snapshot_type,
            matches!(&self.source, SnapshotPublishSource::Sandbox { .. }),
            self.alias.as_ref(),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommittedSnapshot {
    pub context: CommandContext,
    pub startup: Option<StartupCommand>,
    pub runtime_versions: SnapshotRuntimeVersions,
    /// Node virtualization ABI used to capture this snapshot.
    #[serde(default)]
    pub virtualization_mode: VirtualizationMode,
    #[serde(default, skip_serializing_if = "ImageConfigs::is_empty")]
    pub image_configs: ImageConfigs,
    pub rootfs_layers: Vec<OverlaybdLayerRef>,
    pub attached_drives: Vec<CommittedAttachedDrive>,
    /// Logical volume snapshots captured with the sandbox, without local
    /// snapshot artifact paths.
    #[serde(default)]
    pub volume_snapshots: Vec<SnapshotVolume>,
    /// Managed overlaybd layers for the memory snapshot image, ordered bottom-up.
    pub memory_layers: Vec<ManagedLayer>,
    #[serde(default)]
    pub disk_publications: Vec<PersistedDiskImagePublication>,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// Optional startup memory pack descriptor. Present only when the recording succeeded and its
    /// repository backend persisted the manifest; absent on older snapshots and best-effort
    /// recording failures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_startup: Option<MemoryStartupPackInfo>,
}

#[cfg(any(test, feature = "test-support"))]
impl CommittedSnapshot {
    pub fn mock() -> Self {
        Self {
            context: CommandContext::default(),
            startup: None,
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "kernel".to_string(),
                firecracker_version: "firecracker".to_string(),
                envd_version: "envd".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: VirtualizationMode::default(),
            image_configs: ImageConfigs::new(),
            rootfs_layers: Vec::new(),
            attached_drives: Vec::new(),
            volume_snapshots: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            custom_extension_params: None,
            memory_startup: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub id: SnapshotId,
    /// Records written before Local support describe Distributed snapshots.
    #[serde(default)]
    pub snapshot_type: SnapshotPlacement,
    /// Routing identity only; Local availability also requires this process's registry entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_node_id: Option<String>,
    pub alias: Option<SnapshotAlias>,
    pub source: SnapshotSource,
    pub resources: SandboxResources,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub committed: Option<CommittedSnapshot>,
    /// Set when a delete starts, so nothing is attached to the snapshot after.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deleting: bool,
}

impl SnapshotRecord {
    pub fn new_committed(
        id: SnapshotId,
        metadata: &SnapshotPublishMetadata,
        committed: CommittedSnapshot,
        now_unix_ms: i64,
    ) -> Self {
        let source = match &metadata.source {
            SnapshotPublishSource::Template => SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    started_at_unix_ms: None,
                    finished_at_unix_ms: Some(now_unix_ms),
                    error_reason: None,
                },
            },
            SnapshotPublishSource::Sandbox {
                source_sandbox_id,
                source_sandbox_metadata,
            } => SnapshotSource::Sandbox {
                source_sandbox_id: source_sandbox_id.clone(),
                source_sandbox_metadata: source_sandbox_metadata.clone(),
            },
        };
        Self {
            id,
            snapshot_type: metadata.snapshot_type,
            owner_node_id: None,
            alias: metadata.alias.clone(),
            source,
            resources: metadata.resources,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            committed: Some(committed),
            deleting: false,
        }
    }

    pub fn validate_committed_metadata(&self) -> Result<(), String> {
        if self.committed.is_none() {
            return Err("committed snapshot metadata requires a ready payload".to_string());
        }
        if self.snapshot_type == SnapshotPlacement::Local
            && self
                .owner_node_id
                .as_deref()
                .is_none_or(|owner| owner.trim().is_empty())
        {
            return Err("Local snapshots require owner_node_id".to_string());
        }
        if self.snapshot_type == SnapshotPlacement::Distributed && self.owner_node_id.is_some() {
            return Err("Distributed snapshots must not carry owner_node_id".to_string());
        }
        validate_snapshot_metadata(
            self.snapshot_type,
            matches!(&self.source, SnapshotSource::Sandbox { .. }),
            self.alias.as_ref(),
        )
    }

    pub fn template_waiting(
        id: SnapshotId,
        alias: Option<SnapshotAlias>,
        resources: SandboxResources,
    ) -> Self {
        let now_unix_ms = now_unix_ms();
        Self {
            id,
            snapshot_type: SnapshotPlacement::Distributed,
            owner_node_id: None,
            alias,
            source: SnapshotSource::Template {
                build: TemplateBuildInfo::waiting(),
            },
            resources,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            committed: None,
            deleting: false,
        }
    }

    pub fn mark_committed(
        &mut self,
        alias: Option<SnapshotAlias>,
        resources: SandboxResources,
        committed: CommittedSnapshot,
        source: SnapshotPublishSource,
        now_unix_ms: i64,
    ) {
        if let SnapshotPublishSource::Sandbox {
            source_sandbox_id,
            source_sandbox_metadata,
        } = source
        {
            self.source = SnapshotSource::Sandbox {
                source_sandbox_id,
                source_sandbox_metadata,
            };
        }
        if let SnapshotSource::Template { build } = &mut self.source {
            build.status = TemplateBuildStatus::Ready;
            build.finished_at_unix_ms = Some(now_unix_ms);
            build.error_reason = None;
        }
        self.alias = alias;
        self.snapshot_type = SnapshotPlacement::Distributed;
        self.owner_node_id = None;
        self.resources = resources;
        self.updated_at_unix_ms = now_unix_ms;
        self.committed = Some(committed);
    }

    /// Returns the published rootfs OCI image reference, if source-registry
    /// image publication produced one for this snapshot.
    pub fn published_rootfs_image_ref(&self) -> Option<&str> {
        let committed = self.committed.as_ref()?;
        let expected_tag = rootfs_snapshot_image_tag(&self.id);
        committed
            .disk_publications
            .iter()
            .find(|publication| publication.tag == expected_tag)
            .map(|publication| publication.image_ref.as_str())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn mock_ready(committed: CommittedSnapshot) -> Self {
        Self {
            id: SnapshotId::generate(),
            snapshot_type: SnapshotPlacement::Distributed,
            owner_node_id: None,
            alias: None,
            source: SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    started_at_unix_ms: None,
                    finished_at_unix_ms: Some(0),
                    error_reason: None,
                },
            },
            resources: SandboxResources::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
            committed: Some(committed),
            deleting: false,
        }
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedDiskImagePublication {
    pub image_ref: String,
    pub tag: String,
    pub manifest_digest: String,
    pub repo_blob_url: String,
}

const SNAPSHOT_IMAGE_TAG_PREFIX: &str = "agentenv-snapshot-";

/// OCI tag used when publishing a snapshot rootfs image to its source registry.
pub fn rootfs_snapshot_image_tag(snapshot_id: &SnapshotId) -> String {
    format!("{SNAPSHOT_IMAGE_TAG_PREFIX}{snapshot_id}")
}
