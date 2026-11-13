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
    pub config: SnapshotConfig,
}

#[cfg(any(test, feature = "test-support"))]
impl SnapshotPublishMetadata {
    pub fn mock() -> Self {
        Self {
            id: SnapshotId::generate(),
            snapshot_type: SnapshotPlacement::Distributed,
            alias: None,
            config: SnapshotConfig::mock(),
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

pub type SnapshotPublishSource = SnapshotSource;

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
            matches!(&self.config.source, SnapshotSource::Sandbox { .. }),
            self.alias.as_ref(),
        )?;
        self.config.validate()
    }
}

/// Configuration captured once and shared by local and published snapshots.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotConfig {
    pub source: SnapshotSource,
    pub resources: SandboxResources,
    pub context: CommandContext,
    pub startup: Option<StartupCommand>,
    pub runtime_versions: SnapshotRuntimeVersions,
    #[serde(default)]
    pub virtualization_mode: VirtualizationMode,
    #[serde(default, skip_serializing_if = "ImageConfigs::is_empty")]
    pub image_configs: ImageConfigs,
    /// Volumes already have durable layer references when a Local snapshot is captured.
    #[serde(default)]
    pub volume_snapshots: Vec<SnapshotVolume>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<CustomExtensionParams>,
}

impl SnapshotConfig {
    pub fn validate(&self) -> Result<(), String> {
        if let SnapshotSource::Sandbox {
            source_sandbox_id, ..
        } = &self.source
        {
            if source_sandbox_id.is_empty() || source_sandbox_id.contains('\0') {
                return Err("source sandbox ID must be nonempty text without NUL".into());
            }
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn mock() -> Self {
        Self {
            source: SnapshotSource::template_ready(),
            resources: SandboxResources::default(),
            context: CommandContext::default(),
            startup: None,
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "kernel".into(),
                firecracker_version: "firecracker".into(),
                envd_version: "envd".into(),
                tools_drive_version: "0.1.0".into(),
            },
            virtualization_mode: VirtualizationMode::default(),
            image_configs: ImageConfigs::new(),
            volume_snapshots: Vec::new(),
            custom_extension_params: None,
        }
    }
}

impl SnapshotPublishMetadata {
    pub fn config(&self, now: i64) -> SnapshotConfig {
        let mut config = self.config.clone();
        if let SnapshotSource::Template { build } = &mut config.source {
            build.finished_at_unix_ms = Some(now);
        }
        config
    }
}

impl SnapshotSource {
    pub fn template_ready() -> Self {
        Self::Template {
            build: TemplateBuildInfo {
                status: TemplateBuildStatus::Ready,
                started_at_unix_ms: None,
                finished_at_unix_ms: Some(0),
                error_reason: None,
            },
        }
    }
}

/// Published artifact inventory. Captured settings belong to SnapshotConfig.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SnapshotManifest {
    /// Managed rootfs, memory, and drive objects live in the SQL snapshot layer store.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub isolated_layers: bool,
    pub rootfs_layers: Vec<OverlaybdLayerRef>,
    pub attached_drives: Vec<CommittedAttachedDrive>,
    pub memory_layers: Vec<ManagedLayer>,
    #[serde(default)]
    pub disk_publications: Vec<PersistedDiskImagePublication>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_startup: Option<MemoryStartupPackInfo>,
}

#[cfg(any(test, feature = "test-support"))]
impl SnapshotManifest {
    pub fn mock() -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SnapshotRecord<M = Option<SnapshotManifest>> {
    pub id: SnapshotId,
    pub snapshot_type: SnapshotPlacement,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_node_id: Option<String>,
    pub alias: Option<SnapshotAlias>,
    pub config: SnapshotConfig,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub manifest: M,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deleting: bool,
}

// Read legacy repository records and records stored alongside SQL-managed artifacts.
impl<'de, M: serde::de::DeserializeOwned> Deserialize<'de> for SnapshotRecord<M> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut value = serde_json::Value::deserialize(deserializer)?;
        if let Some(record) = value
            .as_object_mut()
            .filter(|record| !record.contains_key("id"))
        {
            if let Some(serde_json::Value::Object(mut metadata)) = record.remove("metadata") {
                for field in [
                    "id",
                    "snapshot_type",
                    "owner_node_id",
                    "alias",
                    "created_at_unix_ms",
                    "updated_at_unix_ms",
                ] {
                    if let Some(value) = metadata.remove(field) {
                        record.insert(field.into(), value);
                    }
                }
                if let Some(retired) = metadata.remove("retired") {
                    record.insert("deleting".into(), retired);
                }
            }
        }
        if let Some(record) = value
            .as_object_mut()
            .filter(|record| !record.contains_key("config"))
        {
            let mut config = serde_json::Map::new();
            for key in ["source", "resources"] {
                if let Some(value) = record.remove(key) {
                    config.insert(key.into(), value);
                }
            }
            if let Some(committed) = record
                .get_mut("committed")
                .and_then(serde_json::Value::as_object_mut)
            {
                for key in [
                    "context",
                    "startup",
                    "runtime_versions",
                    "virtualization_mode",
                    "image_configs",
                    "volume_snapshots",
                    "custom_extension_params",
                ] {
                    if let Some(value) = committed.remove(key) {
                        config.insert(key.into(), value);
                    }
                }
            }
            if record
                .get("committed")
                .is_none_or(serde_json::Value::is_null)
                && config
                    .get("source")
                    .and_then(|source| source.get("Template"))
                    .is_some()
            {
                config.insert(
                    "context".into(),
                    serde_json::to_value(CommandContext::default())
                        .map_err(serde::de::Error::custom)?,
                );
                config.insert(
                    "runtime_versions".into(),
                    serde_json::to_value(SnapshotRuntimeVersions::default())
                        .map_err(serde::de::Error::custom)?,
                );
            }
            if record
                .get("snapshot_type")
                .and_then(serde_json::Value::as_str)
                == Some("local")
            {
                record.insert("committed".into(), serde_json::Value::Null);
            }
            record.insert("config".into(), serde_json::Value::Object(config));
        }
        #[derive(Deserialize)]
        struct Wire<M> {
            id: SnapshotId,
            #[serde(default)]
            snapshot_type: SnapshotPlacement,
            #[serde(default)]
            owner_node_id: Option<String>,
            alias: Option<SnapshotAlias>,
            config: SnapshotConfig,
            created_at_unix_ms: i64,
            updated_at_unix_ms: i64,
            #[serde(alias = "committed")]
            manifest: M,
            #[serde(default)]
            deleting: bool,
        }
        let record: Wire<M> = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(Self {
            id: record.id,
            snapshot_type: record.snapshot_type,
            owner_node_id: record.owner_node_id,
            alias: record.alias,
            config: record.config,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
            manifest: record.manifest,
            deleting: record.deleting,
        })
    }
}

impl<M: Serialize> SnapshotRecord<M> {
    pub fn artifact_json(&self) -> serde_json::Result<serde_json::Value> {
        #[derive(Serialize)]
        struct ArtifactRecord<'a, M> {
            metadata: super::SnapshotMetadata,
            config: &'a SnapshotConfig,
            manifest: &'a M,
        }
        let mut value = serde_json::to_value(ArtifactRecord {
            metadata: self.into(),
            config: &self.config,
            manifest: &self.manifest,
        })?;
        value.sort_all_objects();
        Ok(value)
    }
}

impl<M> SnapshotRecord<M> {
    pub fn with_manifest<N>(self, manifest: N) -> SnapshotRecord<N> {
        SnapshotRecord {
            id: self.id,
            snapshot_type: self.snapshot_type,
            owner_node_id: self.owner_node_id,
            alias: self.alias,
            config: self.config,
            created_at_unix_ms: self.created_at_unix_ms,
            updated_at_unix_ms: self.updated_at_unix_ms,
            manifest,
            deleting: self.deleting,
        }
    }
    pub fn config(&self) -> &SnapshotConfig {
        &self.config
    }
    pub fn resources(&self) -> &SandboxResources {
        &self.config.resources
    }
}

impl SnapshotRecord {
    pub fn new(
        id: SnapshotId,
        snapshot_type: SnapshotPlacement,
        alias: Option<SnapshotAlias>,
        config: SnapshotConfig,
        manifest: Option<SnapshotManifest>,
        now: i64,
    ) -> Self {
        Self {
            id,
            snapshot_type,
            owner_node_id: None,
            alias,
            config,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            manifest,
            deleting: false,
        }
    }
    pub fn new_committed(
        id: SnapshotId,
        metadata: &SnapshotPublishMetadata,
        manifest: SnapshotManifest,
        now: i64,
    ) -> Self {
        let manifest =
            (metadata.snapshot_type == SnapshotPlacement::Distributed).then_some(manifest);
        Self::new(
            id,
            metadata.snapshot_type,
            metadata.alias.clone(),
            metadata.config(now),
            manifest,
            now,
        )
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.deleting {
            return Err("cannot register a snapshot being deleted".into());
        }
        if self.snapshot_type == SnapshotPlacement::Local {
            if self.manifest.is_some() {
                return Err("Local snapshots must not carry a published manifest".into());
            }
            if self
                .owner_node_id
                .as_deref()
                .is_none_or(|owner| owner.trim().is_empty())
            {
                return Err("Local snapshots require owner_node_id".into());
            }
        } else {
            if self.manifest.is_none() {
                return Err("Distributed snapshots require a published manifest".into());
            }
            if self.owner_node_id.is_some() {
                return Err("Distributed snapshots must not carry owner_node_id".into());
            }
        }
        if let Some(alias) = &self.alias {
            SnapshotAlias::parse(alias.as_ref()).map_err(|error| error.to_string())?;
        }
        validate_snapshot_metadata(
            self.snapshot_type,
            matches!(&self.config.source, SnapshotSource::Sandbox { .. }),
            self.alias.as_ref(),
        )?;
        self.config.validate()
    }
    pub fn template_waiting(
        id: SnapshotId,
        alias: Option<SnapshotAlias>,
        resources: SandboxResources,
    ) -> Self {
        let config = SnapshotConfig {
            source: SnapshotSource::Template {
                build: TemplateBuildInfo::waiting(),
            },
            resources,
            context: CommandContext::default(),
            startup: None,
            runtime_versions: SnapshotRuntimeVersions::default(),
            virtualization_mode: VirtualizationMode::default(),
            image_configs: ImageConfigs::new(),
            volume_snapshots: Vec::new(),
            custom_extension_params: None,
        };
        Self::new(
            id,
            SnapshotPlacement::Distributed,
            alias,
            config,
            None,
            now_unix_ms(),
        )
    }
    pub fn mark_committed(
        &mut self,
        alias: Option<SnapshotAlias>,
        manifest: SnapshotManifest,
        now: i64,
    ) {
        if let SnapshotSource::Template { build } = &mut self.config.source {
            build.status = TemplateBuildStatus::Ready;
            build.finished_at_unix_ms = Some(now);
            build.error_reason = None;
        }
        self.alias = alias;
        self.snapshot_type = SnapshotPlacement::Distributed;
        self.owner_node_id = None;
        self.updated_at_unix_ms = now;
        self.manifest = Some(manifest);
    }
    pub fn published_rootfs_image_ref(&self) -> Option<&str> {
        let manifest = self.manifest.as_ref()?;
        let expected_tag = rootfs_snapshot_image_tag(&self.id);
        manifest
            .disk_publications
            .iter()
            .find(|publication| publication.tag == expected_tag)
            .map(|publication| publication.image_ref.as_str())
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn mock_ready(manifest: SnapshotManifest) -> Self {
        Self::new(
            SnapshotId::generate(),
            SnapshotPlacement::Distributed,
            None,
            SnapshotConfig::mock(),
            Some(manifest),
            0,
        )
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
