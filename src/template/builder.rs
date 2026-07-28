use std::path::Path;
use std::sync::{Arc, RwLock};

use anyhow::Error as AnyhowError;
use tempfile::TempDir;
use tracing::info;

use super::build_spec::{TemplateBuildRootfsBase, TemplateBuildSpec};
use super::errors::{
    TemplateBuildError, TemplateBuildFailure, TemplateBuildResult, TemplatePipelineResult,
};
use super::runner::{TemplateBuildBase, TemplateBuildContext, TemplateBuildRunner};
use crate::cfg::ConfigManager;
use crate::sandbox::UblkConfig;
use crate::snapshot::{
    CommandContext, RunnableSnapshot, SnapshotId, SnapshotManager, SnapshotPublishMetadata,
    SnapshotPublishSource, SnapshotRecord, StartupCommand, TemplateBuildErrorReason,
};
use crate::types::SandboxResources;

#[derive(Clone)]
/// Coordinates template-builder flows over committed snapshots.
pub struct TemplateBuilder {
    cpu_config_arc: Option<Arc<RwLock<Option<String>>>>,
}

impl Default for TemplateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TemplateBuilder {
    /// Builds a template builder using system-managed temporary directories.
    pub fn new() -> Self {
        Self {
            cpu_config_arc: None,
        }
    }

    /// Builds a template builder that applies the cluster CPU intersection to new templates.
    pub fn with_cpu_config(arc: Arc<RwLock<Option<String>>>) -> Self {
        Self {
            cpu_config_arc: Some(arc),
        }
    }

    fn current_cpu_config(&self) -> Option<String> {
        self.cpu_config_arc.as_ref()?.read().unwrap().clone()
    }

    /// Builds a new snapshot from `config`, publishes it, and returns the committed record.
    pub async fn build_and_publish(
        &self,
        snapshot_manager: &SnapshotManager,
        spec: TemplateBuildSpec,
    ) -> TemplatePipelineResult<SnapshotRecord> {
        let snapshot_id = SnapshotId::generate();
        self.build_and_publish_with_id(snapshot_manager, snapshot_id, spec)
            .await
    }

    /// Builds and publishes a new snapshot using a caller-provided id.
    pub async fn build_and_publish_with_id(
        &self,
        snapshot_manager: &SnapshotManager,
        snapshot_id: SnapshotId,
        spec: TemplateBuildSpec,
    ) -> TemplatePipelineResult<SnapshotRecord> {
        let context = self.prepare_fresh_context(&spec, snapshot_id)?;
        self.execute_and_publish(
            snapshot_manager,
            context,
            "build snapshot artifacts from requested base",
        )
        .await
    }

    /// Builds a new snapshot from an existing committed snapshot and publishes it.
    pub async fn build_from_snapshot_and_publish(
        &self,
        snapshot_manager: &SnapshotManager,
        spec: TemplateBuildSpec,
        snapshot_id: SnapshotId,
        base_snapshot: &RunnableSnapshot,
    ) -> TemplatePipelineResult<SnapshotRecord> {
        let context = self.prepare_snapshot_base_context(&spec, snapshot_id, base_snapshot)?;
        self.execute_and_publish(
            snapshot_manager,
            context,
            "build snapshot artifacts from committed snapshot",
        )
        .await
    }

    #[tracing::instrument(
        skip(self, snapshot_manager, context, operation),
        fields(build_id = %context.build_snapshot_id)
    )]
    async fn execute_and_publish(
        &self,
        snapshot_manager: &SnapshotManager,
        context: TemplateBuildContext,
        operation: &'static str,
    ) -> TemplatePipelineResult<SnapshotRecord> {
        info!("executing template build");
        let build_execution = match TemplateBuildRunner::new().execute(&context) {
            Ok(execution) => execution,
            Err(error) => {
                let reason = Self::build_failure_reason(&error);
                return Err(TemplateBuildError::with_reason_source(
                    reason,
                    error.context(operation),
                )
                .into());
            }
        };
        let mut resources = context.resources;
        resources.disk_size_mib = build_execution
            .manifest
            .rootfs
            .virtual_size
            .div_ceil(1 << 20)
            .try_into()
            .unwrap_or(u32::MAX);

        info!("publishing template snapshot");
        let record = snapshot_manager
            .publish(
                SnapshotPublishMetadata {
                    id: context.build_snapshot_id.clone(),
                    alias: context.alias.clone(),
                    source: SnapshotPublishSource::Template,
                    context: build_execution.build_context,
                    startup: build_execution.startup,
                    resources,
                    runtime_versions: build_execution.runtime_versions,
                    virtualization_mode: context.virtualization_mode,
                    image_configs: build_execution.image_configs,
                    // Template builds intentionally do not propagate the base
                    // snapshot's extension custom config: it is a per-sandbox
                    // user setting, not part of the built image content.
                    custom_extension_params: None,
                },
                build_execution.manifest,
            )
            .await?;

        info!("template snapshot published");
        Ok(record)
    }
}

impl TemplateBuilder {
    fn build_failure_reason(error: &AnyhowError) -> TemplateBuildErrorReason {
        error
            .chain()
            .find_map(|cause| {
                cause
                    .downcast_ref::<TemplateBuildFailure>()
                    .map(|failure| failure.reason.clone())
            })
            .unwrap_or_else(|| {
                TemplateBuildErrorReason::new(
                    "template build failed while running the build sandbox",
                )
            })
    }

    fn default_sandbox_resources() -> SandboxResources {
        let config = ConfigManager::global_config();
        SandboxResources {
            cpu_count: config.machine.vcpu_count,
            memory_mib: config.machine.mem_size_mib,
            disk_size_mib: 0,
        }
    }

    fn create_local_dir(&self, snapshot_id: &SnapshotId) -> TemplateBuildResult<TempDir> {
        let prefix = format!("snapshot-{snapshot_id}-");
        let mut builder = tempfile::Builder::new();
        builder.prefix(&prefix);
        builder.tempdir().map_err(|error| {
            TemplateBuildError::with_source("create temporary snapshot dir", error)
        })
    }

    fn prepare_fresh_context(
        &self,
        spec: &TemplateBuildSpec,
        snapshot_id: SnapshotId,
    ) -> TemplateBuildResult<TemplateBuildContext> {
        let alias = spec.parsed_alias()?;
        let rootfs_base = spec.rootfs_base_ref().cloned().ok_or_else(|| {
            TemplateBuildError::invalid_input("rootfs base is required for build")
        })?;
        let resources = spec
            .resources_ref()
            .cloned()
            .unwrap_or_else(Self::default_sandbox_resources);
        let base = Self::prepare_base_rootfs(&rootfs_base)?;
        let workspace = self.create_local_dir(&snapshot_id)?;

        Ok(TemplateBuildContext {
            build_snapshot_id: snapshot_id,
            alias,
            initial_context: spec.base_context_ref().cloned().unwrap_or_default(),
            startup: Self::startup_from_spec(spec),
            override_startup: spec.overrides_startup(),
            resources,
            workspace,
            steps: spec.steps().to_vec(),
            base,
            cpu_config_json: self.current_cpu_config(),
            virtualization_mode: ConfigManager::global_config().virtualization_mode,
        })
    }

    fn prepare_snapshot_base_context(
        &self,
        spec: &TemplateBuildSpec,
        snapshot_id: SnapshotId,
        base_snapshot: &RunnableSnapshot,
    ) -> TemplateBuildResult<TemplateBuildContext> {
        if snapshot_id == base_snapshot.record().id {
            return Err(TemplateBuildError::invalid_input(
                "snapshot build target id must differ from the base snapshot id",
            ));
        }
        if spec.rootfs_base_ref().is_some() {
            return Err(TemplateBuildError::invalid_input(
                "snapshot-based build must not override the rootfs base",
            ));
        }

        let node_mode = ConfigManager::global_config().virtualization_mode;
        let base_mode = base_snapshot.committed().virtualization_mode;
        if base_mode != node_mode {
            return Err(TemplateBuildError::invalid_input(format!(
                "base snapshot '{}' uses virtualization mode '{base_mode}', but this node runs in mode '{node_mode}'",
                base_snapshot.record().id
            )));
        }

        let resources = *base_snapshot.resources();
        if let Some(new_resources) = spec.resources_ref().copied() {
            if new_resources.cpu_count != resources.cpu_count
                || new_resources.memory_mib != resources.memory_mib
            {
                return Err(TemplateBuildError::invalid_input(
                    "snapshot-based build cannot change CPU or memory because it resumes from a committed snapshot",
                ));
            }
        }

        let alias = spec.parsed_alias()?;
        let initial_context = base_snapshot.committed().context.clone();
        let override_startup = spec.overrides_startup();
        let startup = if override_startup {
            Self::startup_from_spec(spec)
        } else {
            base_snapshot.committed().startup.clone()
        };
        let workspace = self.create_local_dir(&snapshot_id)?;

        Ok(TemplateBuildContext {
            build_snapshot_id: snapshot_id,
            alias,
            initial_context,
            startup,
            override_startup,
            resources,
            workspace,
            steps: spec.steps().to_vec(),
            base: TemplateBuildBase::Snapshot {
                base_snapshot: Box::new(base_snapshot.clone()),
            },
            cpu_config_json: self.current_cpu_config(),
            virtualization_mode: base_mode,
        })
    }

    fn startup_from_spec(spec: &TemplateBuildSpec) -> Option<StartupCommand> {
        spec.overrides_startup().then(|| StartupCommand {
            start_cmd: spec.start_cmd_ref().unwrap_or_default().to_string(),
            ready_cmd: spec.ready_cmd_ref().unwrap_or_default().to_string(),
            context: CommandContext::default(),
        })
    }

    fn prepare_base_rootfs(
        rootfs_base: &TemplateBuildRootfsBase,
    ) -> TemplateBuildResult<TemplateBuildBase> {
        match rootfs_base {
            TemplateBuildRootfsBase::Ext4 { .. } => Err(TemplateBuildError::invalid_input(
                "overlaybd-only publish mode does not support ext4 rootfs bases",
            )),
            TemplateBuildRootfsBase::Overlaybd {
                image_config_path,
                image_configs,
            } => {
                Self::ensure_path_exists(image_config_path, "overlaybd image config")?;
                let ublk_config = Self::load_overlaybd_build_ublk_config(image_config_path)?;
                Ok(TemplateBuildBase::Rootfs {
                    launch_rootfs_path: image_config_path.clone(),
                    ublk_config: Some(ublk_config),
                    image_configs: image_configs.clone(),
                })
            }
        }
    }

    fn load_overlaybd_build_ublk_config(
        image_config_path: &Path,
    ) -> TemplateBuildResult<UblkConfig> {
        let ublk = &ConfigManager::global_config().ublk;
        if !ublk.enabled {
            return Err(TemplateBuildError::invalid_input(
                "overlaybd build base requires ublk to be enabled",
            ));
        }

        Ok(UblkConfig::overlaybd_with_runtime_upper_mode(
            image_config_path.to_path_buf(),
            ublk.overlaybd.read_only,
            ublk.overlaybd.runtime_upper_mode,
        ))
    }

    fn ensure_path_exists(path: &Path, kind: &str) -> TemplateBuildResult<()> {
        if !path.exists() {
            return Err(TemplateBuildError::invalid_input(format!(
                "{} not found at {}",
                kind,
                path.display()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::{CommittedSnapshot, RunnableSnapshot, SnapshotManager, SnapshotRecord};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    fn test_parts(root: &Path) -> (TemplateBuilder, SnapshotManager) {
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join("runtime-cache")),
            runtime_cache_root: Some(root.join("runtime-cache").join("runtime")),
        })
        .expect("posix backend");
        let (repository, runtime_resolver) = backend.into_parts();
        let snapshot_manager =
            SnapshotManager::from_parts(repository.clone(), runtime_resolver.clone(), None);
        (TemplateBuilder::new(), snapshot_manager)
    }

    fn sample_runnable_snapshot_with_attached_drive() -> RunnableSnapshot {
        RunnableSnapshot::from_test_manifest(
            SnapshotRecord::mock_ready(CommittedSnapshot::mock()),
            vec![crate::snapshot::ResolvedAttachedDrive::Overlaybd {
                drive_id: "data".to_string(),
                image_config_path: PathBuf::from("/tmp/drive-image.json"),
                read_only: true,
                virtual_size: 4096,
                mount_path: crate::sandbox::ExtraDrive::default_mount_path("data"),
                sub_path: None,
            }],
        )
    }

    #[tokio::test]
    async fn build_publish_requires_rootfs_base() {
        let tempdir = TempDir::new().expect("tempdir");
        let (manager, snapshot_manager) = test_parts(tempdir.path());
        let err = manager
            .build_and_publish(&snapshot_manager, TemplateBuildSpec::new())
            .await
            .expect_err("missing rootfs base should fail");

        assert!(matches!(
            err,
            crate::template::TemplatePipelineError::Build(TemplateBuildError::InvalidInput { .. })
        ));
    }

    #[tokio::test]
    async fn build_snapshot_base_rejects_overriding_rootfs_base() {
        let manager = TemplateBuilder::new();
        let runnable = RunnableSnapshot::mock();
        let config =
            crate::template::TemplateBuildSpec::new().from_existing_rootfs("/tmp/other.ext4");
        let err = manager
            .prepare_snapshot_base_context(
                &config,
                crate::snapshot::SnapshotId::generate(),
                &runnable,
            )
            .expect_err("snapshot-based build should reject overriding rootfs base");

        assert!(matches!(err, TemplateBuildError::InvalidInput { .. }));
    }

    #[tokio::test]
    async fn build_snapshot_base_rejects_resource_change() {
        let manager = TemplateBuilder::new();
        let runnable = RunnableSnapshot::mock();
        let config = crate::template::TemplateBuildSpec::new().resources(2, 256);
        let err = manager
            .prepare_snapshot_base_context(
                &config,
                crate::snapshot::SnapshotId::generate(),
                &runnable,
            )
            .expect_err("snapshot-based build should reject resource changes");

        assert!(matches!(err, TemplateBuildError::InvalidInput { .. }));
    }

    #[test]
    fn snapshot_base_context_rejects_reusing_base_snapshot_id() {
        let manager = TemplateBuilder::new();
        let runnable = RunnableSnapshot::mock();
        let err = manager
            .prepare_snapshot_base_context(
                &crate::template::TemplateBuildSpec::new(),
                runnable.record().id.clone(),
                &runnable,
            )
            .expect_err("snapshot-based build should reject target id reuse");

        assert!(matches!(err, TemplateBuildError::InvalidInput { .. }));
    }

    #[test]
    fn snapshot_base_context_rejects_other_virtualization_mode() {
        let manager = TemplateBuilder::new();
        let node_mode = ConfigManager::global_config().virtualization_mode;
        let base_mode = match node_mode {
            crate::virtualization::VirtualizationMode::Kvm => {
                crate::virtualization::VirtualizationMode::Pvm
            }
            crate::virtualization::VirtualizationMode::Pvm => {
                crate::virtualization::VirtualizationMode::Kvm
            }
        };
        let mut committed = CommittedSnapshot::mock();
        committed.virtualization_mode = base_mode;
        let runnable =
            RunnableSnapshot::from_test_manifest(SnapshotRecord::mock_ready(committed), Vec::new());

        let error = manager
            .prepare_snapshot_base_context(
                &TemplateBuildSpec::new(),
                SnapshotId::generate(),
                &runnable,
            )
            .expect_err("snapshot-based build must reject the other mode");

        assert!(
            matches!(error, TemplateBuildError::InvalidInput { ref reason }
                if reason.contains(&format!("uses virtualization mode '{base_mode}'"))
                    && reason.contains(&format!("node runs in mode '{node_mode}'")))
        );
    }

    #[test]
    fn snapshot_base_context_keeps_base_snapshot_attached_drives() {
        let manager = TemplateBuilder::new();
        let runnable = sample_runnable_snapshot_with_attached_drive();
        let context = manager
            .prepare_snapshot_base_context(
                &crate::template::TemplateBuildSpec::new(),
                crate::snapshot::SnapshotId::generate(),
                &runnable,
            )
            .expect("snapshot-base context should preserve base attached drives");

        match context.base {
            TemplateBuildBase::Snapshot { base_snapshot } => {
                let drives = base_snapshot.attached_drives();
                assert_eq!(drives.len(), 1);
                assert_eq!(drives[0].drive_id(), "data");
            }
            TemplateBuildBase::Rootfs { .. } => {
                panic!("snapshot-base context should use snapshot base")
            }
        }
    }

    #[test]
    fn snapshot_base_context_inherits_base_context_but_not_alias() {
        let manager = TemplateBuilder::new();
        let committed = CommittedSnapshot {
            context: CommandContext::new(
                std::collections::HashMap::from([
                    ("BASE_ONLY".to_string(), "1".to_string()),
                    ("SHARED".to_string(), "base".to_string()),
                ]),
                "/base",
            ),
            ..CommittedSnapshot::mock()
        };
        let record = SnapshotRecord {
            alias: Some(
                crate::snapshot::SnapshotAlias::parse("base-alias").expect("alias should parse"),
            ),
            ..SnapshotRecord::mock_ready(committed)
        };
        let runnable = RunnableSnapshot::from_test_manifest(record, Vec::new());

        let context = manager
            .prepare_snapshot_base_context(
                &crate::template::TemplateBuildSpec::new(),
                crate::snapshot::SnapshotId::generate(),
                &runnable,
            )
            .expect("snapshot-base preparation should succeed");

        assert_eq!(context.alias.as_ref().map(ToString::to_string), None);
        assert_eq!(
            context
                .initial_context
                .env_vars
                .get("BASE_ONLY")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            context
                .initial_context
                .env_vars
                .get("SHARED")
                .map(String::as_str),
            Some("base")
        );
        assert_eq!(context.initial_context.workdir.as_str(), "/base");
    }

    #[test]
    fn build_spec_stores_image_configs_on_resolved_overlaybd_base() {
        use crate::types::ImageConfigs;
        let mut image_configs = ImageConfigs::new();
        image_configs.add(
            None::<String>,
            "/",
            serde_json::json!({ "WorkingDir": "/workspace" }),
        );

        let spec = crate::template::TemplateBuildSpec::new()
            .with_resolved_overlaybd_image("/tmp/image.json", image_configs.clone());

        match spec.rootfs_base_ref().expect("rootfs base should be set") {
            TemplateBuildRootfsBase::Overlaybd {
                image_configs: stored,
                ..
            } => assert_eq!(stored, &image_configs),
            TemplateBuildRootfsBase::Ext4 { .. } => panic!("expected overlaybd base"),
        }
    }

    #[test]
    fn snapshot_base_context_inherits_base_image_configs() {
        use crate::types::ImageConfigs;
        let manager = TemplateBuilder::new();
        let mut image_configs = ImageConfigs::new();
        image_configs.add(
            None::<String>,
            "/",
            serde_json::json!({ "Env": ["BASE=1"] }),
        );

        let record = SnapshotRecord::mock_ready(CommittedSnapshot {
            image_configs: image_configs.clone(),
            ..CommittedSnapshot::mock()
        });
        let runnable = RunnableSnapshot::from_test_manifest(record, Vec::new());

        let context = manager
            .prepare_snapshot_base_context(
                &crate::template::TemplateBuildSpec::new(),
                crate::snapshot::SnapshotId::generate(),
                &runnable,
            )
            .expect("rebuild preparation should preserve image configs");

        match &context.base {
            TemplateBuildBase::Snapshot { base_snapshot } => {
                assert_eq!(&base_snapshot.committed().image_configs, &image_configs)
            }
            TemplateBuildBase::Rootfs { .. } => panic!("rebuild context should use snapshot base"),
        }
    }

    #[test]
    fn snapshot_base_context_inherits_base_startup_when_not_overridden() {
        let manager = TemplateBuilder::new();
        let startup = StartupCommand {
            start_cmd: "echo base".to_string(),
            ready_cmd: "echo ready".to_string(),
            context: CommandContext::new(
                std::collections::HashMap::from([("BASE".to_string(), "1".to_string())]),
                "/base",
            ),
        };
        let record = SnapshotRecord::mock_ready(CommittedSnapshot {
            startup: Some(startup.clone()),
            ..CommittedSnapshot::mock()
        });
        let runnable = RunnableSnapshot::from_test_manifest(record, Vec::new());

        let context = manager
            .prepare_snapshot_base_context(
                &crate::template::TemplateBuildSpec::new().env("DERIVED", "1"),
                crate::snapshot::SnapshotId::generate(),
                &runnable,
            )
            .expect("snapshot-base preparation should succeed");

        assert_eq!(context.startup, Some(startup));
        assert!(!context.override_startup);
    }

    #[test]
    fn snapshot_base_context_overrides_base_startup_when_command_is_set() {
        let manager = TemplateBuilder::new();
        let record = SnapshotRecord::mock_ready(CommittedSnapshot {
            startup: Some(StartupCommand {
                start_cmd: "echo base".to_string(),
                ready_cmd: "echo base-ready".to_string(),
                context: CommandContext::new(std::collections::HashMap::new(), "/base"),
            }),
            ..CommittedSnapshot::mock()
        });
        let runnable = RunnableSnapshot::from_test_manifest(record, Vec::new());

        let context = manager
            .prepare_snapshot_base_context(
                &crate::template::TemplateBuildSpec::new().start_cmd("echo derived"),
                crate::snapshot::SnapshotId::generate(),
                &runnable,
            )
            .expect("snapshot-base preparation should succeed");

        assert_eq!(
            context
                .startup
                .as_ref()
                .map(|startup| startup.start_cmd.as_str()),
            Some("echo derived")
        );
        assert_eq!(
            context
                .startup
                .as_ref()
                .map(|startup| startup.ready_cmd.as_str()),
            Some("")
        );
        assert!(context.override_startup);
    }
}
