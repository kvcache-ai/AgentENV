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

use anyhow::{bail, Result};

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

/// Storage availability requested for a reusable snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPlacement {
    Local,
    #[default]
    Distributed,
}

impl SnapshotPlacement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Distributed => "distributed",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "local" => Ok(Self::Local),
            "distributed" => Ok(Self::Distributed),
            other => bail!("unknown snapshot placement '{other}'"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotSourceKind {
    Template,
    Sandbox,
    Builder,
}

impl SnapshotSourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Template => "template",
            Self::Sandbox => "sandbox",
            Self::Builder => "builder",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "template" => Ok(Self::Template),
            "sandbox" => Ok(Self::Sandbox),
            "builder" => Ok(Self::Builder),
            other => bail!("unknown snapshot source kind '{other}'"),
        }
    }
}

impl fmt::Display for SnapshotSourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotSource {
    Template,
    Builder,
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
) -> std::result::Result<(), String> {
    if snapshot_type == SnapshotPlacement::Local && !source_is_sandbox {
        return Err("Local snapshots must originate from a sandbox".to_string());
    }
    if snapshot_type == SnapshotPlacement::Local && alias.is_some() {
        return Err("Local snapshots do not support aliases".to_string());
    }
    Ok(())
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
    pub fn validate(&self) -> std::result::Result<(), String> {
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
            source: SnapshotSource::Template,
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

impl SnapshotSource {
    pub const fn source_kind(&self) -> SnapshotSourceKind {
        match self {
            Self::Template => SnapshotSourceKind::Template,
            Self::Builder => SnapshotSourceKind::Builder,
            Self::Sandbox { .. } => SnapshotSourceKind::Sandbox,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRecord<M = Option<SnapshotManifest>> {
    pub id: SnapshotId,
    #[serde(default)]
    pub snapshot_type: SnapshotPlacement,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_node_id: Option<String>,
    pub alias: Option<SnapshotAlias>,
    pub config: SnapshotConfig,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub manifest: M,
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
        }
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
        }
    }
    pub fn validate(&self) -> std::result::Result<(), String> {
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
        let mut value = serde_json::to_value(SnapshotRecord::mock_ready(SnapshotManifest::mock()))
            .expect("serialize record");
        value["config"]["runtime_versions"]
            .as_object_mut()
            .expect("runtime versions must be an object")
            .remove("tools_drive_version");

        let record: SnapshotRecord =
            serde_json::from_value(value).expect("deserialize snapshot record");

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
    fn local_snapshots_reject_aliases() {
        let mut record = SnapshotRecord::new(
            SnapshotId::generate(),
            SnapshotPlacement::Local,
            None,
            SnapshotConfig {
                source: SnapshotSource::Sandbox {
                    source_sandbox_id: "sandbox-a".to_string(),
                    source_sandbox_metadata: None,
                },
                ..SnapshotConfig::mock()
            },
            None,
            0,
        );
        record.owner_node_id = Some("node-a".to_string());
        assert!(record.validate().is_ok());
        record.alias = Some(SnapshotAlias::parse("checkpoint").unwrap());
        assert!(record.validate().is_err());
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
    fn effective_start_cmd_empty_vecs_return_none() {
        let ctx = CommandContext::default()
            .with_entrypoint(Some(vec![]))
            .with_cmd(Some(vec![]));
        assert_eq!(ctx.effective_start_cmd(), None);
    }

    #[test]
    fn snapshot_source_round_trips_builder() {
        let builder = SnapshotSource::Builder;
        assert_eq!(builder.source_kind(), SnapshotSourceKind::Builder);
        let raw = serde_json::to_string(&builder).unwrap();
        assert_eq!(raw, r#""Builder""#);
        assert_eq!(
            serde_json::from_str::<SnapshotSource>(&raw).unwrap(),
            builder
        );
    }
}
