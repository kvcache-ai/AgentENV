//! Persistent snapshot records and publication inputs.

use super::{CommandContext, SnapshotAlias, SnapshotId, SnapshotRuntimeVersions, StartupCommand};
use crate::{
    CustomExtensionParams, ImageConfigs, LayerRef, ManagedLayerRef, SandboxResources,
    VirtualizationMode, VolumeMode,
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
        layers: Vec<LayerRef>,
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
    pub layers: Vec<LayerRef>,
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
    pub rootfs_layers: Vec<LayerRef>,
    pub attached_drives: Vec<CommittedAttachedDrive>,
    pub memory_layers: Vec<ManagedLayerRef>,
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

/// Immutable capture settings and, after publication, the artifact inventory.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SnapshotContent {
    pub config: SnapshotConfig,
    pub manifest: Option<SnapshotManifest>,
}

impl From<&SnapshotRecord> for SnapshotContent {
    fn from(record: &SnapshotRecord) -> Self {
        Self {
            config: record.config.clone(),
            manifest: record.manifest.clone(),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn publication(
        image_ref: impl Into<String>,
        tag: impl Into<String>,
    ) -> PersistedDiskImagePublication {
        PersistedDiskImagePublication {
            image_ref: image_ref.into(),
            tag: tag.into(),
            manifest_digest: "sha256:manifest".to_string(),
            repo_blob_url: "https://registry.example/v2/ns/app/blobs".to_string(),
        }
    }

    fn legacy_record_value() -> serde_json::Value {
        serde_json::json!({
            "id": SnapshotId::generate(), "alias": null,
            "source": {"Sandbox": {"source_sandbox_id": "legacy-source", "source_sandbox_metadata": {"team": "compat"}}},
            "resources": {"cpu_count": 2, "memory_mib": 512, "disk_size_mib": 1024},
            "created_at_unix_ms": 10, "updated_at_unix_ms": 20,
            "committed": {
                "context": {"env_vars": {"LEGACY": "1"}, "workdir": "/legacy"},
                "startup": null,
                "runtime_versions": {"kernel_version": "kernel", "firecracker_version": "firecracker", "envd_version": "envd", "tools_drive_version": "0.1.0"},
                "rootfs_layers": [], "memory_layers": [], "attached_drives": [],
                "volume_snapshots": [{"mountPath": "/data", "sizeMb": 16, "layers": []}]
            }
        })
    }

    #[test]
    fn committed_attached_drive_virtual_size_is_required_and_serialized() {
        let err = serde_json::from_value::<CommittedAttachedDrive>(serde_json::json!({
            "Overlaybd": {
                "drive_id": "data",
                "layers": [],
                "read_only": true,
                "mount_path": "/mnt/data"
            }
        }))
        .expect_err("committed attached drive should require virtual_size");
        assert!(err.to_string().contains("virtual_size"));

        let committed = CommittedAttachedDrive::Overlaybd {
            drive_id: "data".to_string(),
            layers: Vec::new(),
            read_only: true,
            virtual_size: 4096,
            mount_path: PathBuf::from("/mnt/data"),
            sub_path: None,
        };
        let committed_json = serde_json::to_value(&committed).unwrap();
        assert_eq!(committed_json["Overlaybd"]["virtual_size"], 4096);
    }

    #[test]
    fn sandbox_source_metadata_serde_is_backward_compatible() {
        let legacy = r#"{"Sandbox":{"source_sandbox_id":"sandbox-1"}}"#;
        let source: SnapshotSource = serde_json::from_str(legacy).expect("parse legacy source");
        let SnapshotSource::Sandbox {
            ref source_sandbox_id,
            ref source_sandbox_metadata,
        } = source
        else {
            panic!("expected sandbox source");
        };
        assert_eq!(source_sandbox_id, "sandbox-1");
        assert_eq!(*source_sandbox_metadata, None);
        assert_eq!(serde_json::to_string(&source).unwrap(), legacy);

        let with_metadata = r#"{"Sandbox":{"source_sandbox_id":"sandbox-1","source_sandbox_metadata":{"team":"alpha"}}}"#;
        let source: SnapshotSource =
            serde_json::from_str(with_metadata).expect("parse metadata source");
        let SnapshotSource::Sandbox {
            source_sandbox_metadata,
            ..
        } = source
        else {
            panic!("expected sandbox source");
        };
        assert_eq!(
            source_sandbox_metadata.as_ref().and_then(|m| m.get("team")),
            Some(&"alpha".to_string()),
        );
    }

    #[test]
    fn snapshot_record_without_tools_drive_version_remains_readable() {
        let mut value = legacy_record_value();
        value["committed"]["runtime_versions"]
            .as_object_mut()
            .expect("runtime versions must be an object")
            .remove("tools_drive_version");

        let record: SnapshotRecord =
            serde_json::from_value(value).expect("deserialize legacy snapshot record");

        assert!(record
            .config
            .runtime_versions
            .tools_drive_version
            .is_empty());
    }

    #[test]
    fn snapshot_volume_is_logical() {
        let mut record = SnapshotRecord::mock_ready(SnapshotManifest::mock());
        record.config.volume_snapshots.push(SnapshotVolume {
            mount_path: "/mnt/data".to_string(),
            mode: VolumeMode::ReadOnly,
            size_mb: 1024,
            layers: vec![LayerRef::Managed(ManagedLayerRef {
                digest: "sha256:abc".to_string(),
                size: 123,
                uuid: None,
            })],
        });

        let value = serde_json::to_value(&record).expect("serialize snapshot record");
        assert_eq!(
            value["config"]["volume_snapshots"][0]["layers"][0]["Managed"]["digest"],
            "sha256:abc"
        );
        assert_eq!(value["config"]["volume_snapshots"][0]["mode"], "ReadOnly");
        assert!(!value.to_string().contains("snapshot_dir"));
        let decoded: SnapshotRecord =
            serde_json::from_value(value.clone()).expect("deserialize record");
        assert_eq!(decoded.config.volume_snapshots[0].mount_path, "/mnt/data");
        assert_eq!(
            decoded.config.volume_snapshots[0].mode,
            VolumeMode::ReadOnly
        );

        let mut legacy = value;
        legacy["config"]["volume_snapshots"][0]
            .as_object_mut()
            .expect("volume snapshot must be an object")
            .remove("mode");
        let decoded: SnapshotRecord =
            serde_json::from_value(legacy).expect("deserialize legacy record");
        assert_eq!(
            decoded.config.volume_snapshots[0].mode,
            VolumeMode::Exclusive
        );
    }

    #[test]
    fn legacy_snapshot_records_are_distributed() {
        let legacy = legacy_record_value();
        let record: SnapshotRecord = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(record.snapshot_type, SnapshotPlacement::Distributed);
        assert_eq!(record.config.context.workdir, "/legacy");
        assert_eq!(record.config.volume_snapshots[0].mount_path, "/data");
        assert!(record.manifest.is_some());
        let mut local = legacy.clone();
        local["snapshot_type"] = "local".into();
        local["owner_node_id"] = "legacy-owner".into();
        let local: SnapshotRecord = serde_json::from_value(local).unwrap();
        local.validate().unwrap();
        assert!(local.manifest.is_none());
        assert_eq!(
            local.config.volume_snapshots,
            record.config.volume_snapshots
        );
        assert_eq!(local.config.context, record.config.context);
        let mut pending = legacy;
        pending["source"] = serde_json::json!({"Template": {"build": {"status": "Waiting", "started_at_unix_ms": null, "finished_at_unix_ms": null, "error_reason": null}}});
        pending["committed"] = serde_json::Value::Null;
        let pending: SnapshotRecord = serde_json::from_value(pending).unwrap();
        assert!(pending.manifest.is_none());
        assert!(
            matches!(pending.config.source, SnapshotSource::Template { build } if build.status == TemplateBuildStatus::Waiting)
        );
    }

    #[test]
    fn local_snapshots_reject_aliases() {
        let mut metadata = SnapshotPublishMetadata {
            snapshot_type: SnapshotPlacement::Local,
            config: SnapshotConfig {
                source: SnapshotSource::Sandbox {
                    source_sandbox_id: "sandbox-a".to_string(),
                    source_sandbox_metadata: None,
                },
                ..SnapshotConfig::mock()
            },
            ..SnapshotPublishMetadata::mock()
        };
        assert!(metadata.validate().is_ok());
        metadata.alias = Some(SnapshotAlias::parse("checkpoint").unwrap());
        assert!(metadata.validate().is_err());
    }

    #[test]
    fn managed_layer_uuid_serde_is_backward_compatible() {
        let legacy = r#"{"digest":"sha256:abc","size":123}"#;
        let layer: ManagedLayerRef = serde_json::from_str(legacy).expect("parse legacy layer");
        assert_eq!(layer.uuid, None);

        let with_uuid =
            r#"{"digest":"sha256:abc","size":123,"uuid":"11111111-2222-3333-4444-555555555555"}"#;
        let layer: ManagedLayerRef = serde_json::from_str(with_uuid).expect("parse uuid layer");
        assert_eq!(
            layer.uuid.as_deref(),
            Some("11111111-2222-3333-4444-555555555555")
        );
        let value = serde_json::to_value(&layer).expect("serialize uuid layer");
        assert_eq!(value["uuid"], "11111111-2222-3333-4444-555555555555");
    }

    #[test]
    fn returns_published_rootfs_image_ref_by_exact_tag() {
        let mut record = SnapshotRecord::mock_ready(SnapshotManifest::mock());
        let rootfs_tag = rootfs_snapshot_image_tag(&record.id);
        let expected = format!("registry.example/ns/app:{rootfs_tag}");
        record.manifest.as_mut().unwrap().disk_publications = vec![
            publication(
                "registry.example/ns/app:drive",
                format!("{rootfs_tag}-drive-data-0123456789ab"),
            ),
            publication(expected.clone(), rootfs_tag),
        ];

        assert_eq!(record.published_rootfs_image_ref(), Some(expected.as_str()));
    }

    #[test]
    fn effective_start_cmd_combines_entrypoint_and_cmd() {
        let ctx = CommandContext::default()
            .with_entrypoint(Some(vec!["/docker-entrypoint.sh".to_string()]))
            .with_cmd(Some(vec![
                "nginx".to_string(),
                "-g".to_string(),
                "daemon off;".to_string(),
            ]));
        assert_eq!(
            ctx.effective_start_cmd().as_deref(),
            Some("/docker-entrypoint.sh nginx -g 'daemon off;'"),
        );
    }

    #[test]
    fn effective_start_cmd_entrypoint_only() {
        let ctx = CommandContext::default().with_entrypoint(Some(vec!["node".to_string()]));
        assert_eq!(ctx.effective_start_cmd().as_deref(), Some("node"));
    }

    #[test]
    fn effective_start_cmd_cmd_only() {
        let ctx = CommandContext::default()
            .with_cmd(Some(vec!["python3".to_string(), "app.py".to_string()]));
        assert_eq!(ctx.effective_start_cmd().as_deref(), Some("python3 app.py"));
    }

    #[test]
    fn effective_start_cmd_absent_returns_none() {
        assert_eq!(CommandContext::default().effective_start_cmd(), None);
    }

    #[test]
    fn startup_shell_is_backward_compatible() {
        let legacy = serde_json::json!({"start_cmd": "true", "ready_cmd": "true", "context": CommandContext::default()});
        let mut startup: StartupCommand = serde_json::from_value(legacy).unwrap();
        assert_eq!(startup.shell_command(), ("/bin/bash", "-lc"));
        assert!(serde_json::to_value(&startup)
            .unwrap()
            .get("shell")
            .is_none());
        startup.shell = Some("/bin/sh".to_owned());
        let restored: StartupCommand =
            serde_json::from_value(serde_json::to_value(&startup).unwrap()).unwrap();
        assert_eq!(restored.shell_command(), ("/bin/sh", "-c"));
    }

    #[test]
    fn legacy_snapshot_startup_preserves_captured_configuration() {
        let mut legacy = legacy_record_value();
        legacy["committed"]["startup"] = serde_json::json!({
            "start_cmd": "[[ -n $HOME ]] && exec /app/server",
            "ready_cmd": "test -f /app/ready",
            "context": CommandContext::new(
                HashMap::from([("HOME".into(), "/app".into())]),
                "/app",
            ),
        });
        let record: SnapshotRecord = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(
            record.config.startup.as_ref().unwrap().shell_command(),
            ("/bin/bash", "-lc"),
        );
        let upgraded = serde_json::to_value(&record).unwrap();
        assert_eq!(
            upgraded["config"]["startup"],
            legacy["committed"]["startup"]
        );
        assert_eq!(upgraded["config"]["source"], legacy["source"]);
        assert_eq!(upgraded["config"]["resources"], legacy["resources"]);
        assert_eq!(upgraded["created_at_unix_ms"], legacy["created_at_unix_ms"]);
        assert!(upgraded.get("committed").is_none());
        let reread: SnapshotRecord = serde_json::from_value(upgraded.clone()).unwrap();
        assert_eq!(serde_json::to_value(reread).unwrap(), upgraded);
    }

    #[test]
    fn effective_start_cmd_empty_vecs_return_none() {
        let ctx = CommandContext::default()
            .with_entrypoint(Some(vec![]))
            .with_cmd(Some(vec![]));
        assert_eq!(ctx.effective_start_cmd(), None);
    }

    #[test]
    fn template_build_error_reason_reads_legacy_string() {
        let reason: TemplateBuildErrorReason =
            serde_json::from_str(r#""legacy failure""#).expect("deserialize legacy reason");

        assert_eq!(reason.message, "legacy failure");
        assert_eq!(reason.step, None);
    }

    #[test]
    fn template_build_error_reason_reads_structured_reason() {
        let reason: TemplateBuildErrorReason =
            serde_json::from_str(r#"{"message":"boom","step":"resolve image"}"#)
                .expect("deserialize structured reason");

        assert_eq!(reason.message, "boom");
        assert_eq!(reason.step.as_deref(), Some("resolve image"));
    }
}
