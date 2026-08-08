use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{TemplateBuildError, TemplateBuildResult};
use crate::snapshot::{CommandContext, SnapshotAlias};
use crate::types::{ImageConfigs, SandboxResources};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum TemplateBuildRootfsBase {
    Ext4 {
        image_path: PathBuf,
    },
    Overlaybd {
        image_config_path: PathBuf,
        #[serde(default, skip_serializing_if = "ImageConfigs::is_empty")]
        image_configs: ImageConfigs,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct TemplateBuildStep {
    pub(crate) kind: TemplateBuildStepKind,
}

#[derive(Clone, Debug)]
pub(crate) enum TemplateBuildStepKind {
    Run {
        cmd: String,
    },
    Env {
        key: String,
        value: String,
    },
    Workdir {
        path: PathBuf,
    },
    User {
        value: String,
    },
    ExposedPort {
        port: String,
    },
    Volume {
        path: String,
    },
    Label {
        key: String,
        value: String,
    },
    /// Copies files from an uploaded build-context archive into the rootfs.
    ///
    /// `files_hash` addresses the archive in the repository's template
    /// build-file store; `user` and `mode` mirror Docker's `--chown` /
    /// `--chmod` flags.
    Copy {
        src: String,
        dest: String,
        files_hash: String,
        user: Option<String>,
        mode: Option<u32>,
    },
}

impl TemplateBuildStep {
    pub(crate) fn run(cmd: impl Into<String>) -> Self {
        Self {
            kind: TemplateBuildStepKind::Run { cmd: cmd.into() },
        }
    }

    pub(crate) fn env(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            kind: TemplateBuildStepKind::Env {
                key: key.into(),
                value: value.into(),
            },
        }
    }

    pub(crate) fn workdir(path: impl Into<PathBuf>) -> Self {
        Self {
            kind: TemplateBuildStepKind::Workdir { path: path.into() },
        }
    }

    pub(crate) fn user(value: impl Into<String>) -> Self {
        Self {
            kind: TemplateBuildStepKind::User {
                value: value.into(),
            },
        }
    }

    pub(crate) fn exposed_port(port: impl Into<String>) -> Self {
        Self {
            kind: TemplateBuildStepKind::ExposedPort { port: port.into() },
        }
    }

    pub(crate) fn volume(path: impl Into<String>) -> Self {
        Self {
            kind: TemplateBuildStepKind::Volume { path: path.into() },
        }
    }

    pub(crate) fn label(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            kind: TemplateBuildStepKind::Label {
                key: key.into(),
                value: value.into(),
            },
        }
    }

    pub(crate) fn copy(
        src: impl Into<String>,
        dest: impl Into<String>,
        files_hash: impl Into<String>,
        user: Option<String>,
        mode: Option<u32>,
    ) -> Self {
        Self {
            kind: TemplateBuildStepKind::Copy {
                src: src.into(),
                dest: dest.into(),
                files_hash: files_hash.into(),
                user,
                mode,
            },
        }
    }
}

#[derive(Clone, Debug, Default)]
/// Describes the desired template build independently from storage and runtime details.
pub struct TemplateBuildSpec {
    rootfs_base: Option<TemplateBuildRootfsBase>,
    steps: Vec<TemplateBuildStep>,
    alias: Option<String>,
    resources: Option<SandboxResources>,
    start_cmd: Option<String>,
    ready_cmd: Option<String>,
    base_context: Option<CommandContext>,
}

impl TemplateBuildSpec {
    /// Creates an empty template build description.
    pub fn new() -> Self {
        Self::default()
    }

    /// Uses an existing ext4 image as the rootfs base for a fresh build.
    pub fn from_existing_rootfs(mut self, path: impl Into<PathBuf>) -> Self {
        self.rootfs_base = Some(TemplateBuildRootfsBase::Ext4 {
            image_path: path.into(),
        });
        self
    }

    /// Uses local OverlayBD image config as the rootfs base for a fresh build.
    pub fn from_overlaybd_config(self, image_config_path: impl Into<PathBuf>) -> Self {
        self.with_resolved_overlaybd_image(image_config_path, ImageConfigs::new())
    }

    pub(crate) fn with_resolved_overlaybd_image(
        mut self,
        image_config_path: impl Into<PathBuf>,
        image_configs: ImageConfigs,
    ) -> Self {
        self.rootfs_base = Some(TemplateBuildRootfsBase::Overlaybd {
            image_config_path: image_config_path.into(),
            image_configs,
        });
        self
    }

    /// Appends a shell command build step.
    pub fn run(mut self, cmd: impl Into<String>) -> Self {
        self.steps.push(TemplateBuildStep::run(cmd));
        self
    }

    /// Appends an environment-variable build step.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.steps.push(TemplateBuildStep::env(key, value));
        self
    }

    /// Appends a working-directory build step.
    pub fn workdir(mut self, path: impl Into<PathBuf>) -> Self {
        self.steps.push(TemplateBuildStep::workdir(path));
        self
    }

    pub fn user(mut self, value: impl Into<String>) -> Self {
        self.steps.push(TemplateBuildStep::user(value));
        self
    }

    pub fn exposed_port(mut self, port: impl Into<String>) -> Self {
        self.steps.push(TemplateBuildStep::exposed_port(port));
        self
    }

    pub fn volume(mut self, path: impl Into<String>) -> Self {
        self.steps.push(TemplateBuildStep::volume(path));
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.steps.push(TemplateBuildStep::label(key, value));
        self
    }

    /// Appends a build step copying files from an uploaded build-context
    /// archive (addressed by `files_hash`) into the rootfs.
    pub fn copy(
        mut self,
        src: impl Into<String>,
        dest: impl Into<String>,
        files_hash: impl Into<String>,
        user: Option<String>,
        mode: Option<u32>,
    ) -> Self {
        self.steps
            .push(TemplateBuildStep::copy(src, dest, files_hash, user, mode));
        self
    }

    /// Returns the distinct build-context archive hashes referenced by COPY steps.
    pub(crate) fn referenced_build_file_hashes(steps: &[TemplateBuildStep]) -> Vec<String> {
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut hashes: Vec<String> = Vec::new();
        for step in steps {
            if let TemplateBuildStepKind::Copy { files_hash, .. } = &step.kind {
                if seen.insert(files_hash.as_str()) {
                    hashes.push(files_hash.clone());
                }
            }
        }
        hashes
    }

    /// Appends an `apt-get install` build step for the provided packages.
    pub fn apt<I, S>(mut self, packages: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let packages = packages
            .into_iter()
            .map(Into::into)
            .filter(|package: &String| !package.trim().is_empty())
            .collect::<Vec<_>>();

        if !packages.is_empty() {
            let cmd = format!(
                "DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y --no-install-recommends {} && rm -rf /var/lib/apt/lists/*",
                packages.join(" ")
            );
            self.steps.push(TemplateBuildStep::run(cmd));
        }

        self
    }

    /// Sets the alias to claim when this template is published.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Overrides the default CPU and memory for the built template.
    pub fn resources(mut self, cpu_count: u32, memory_mib: u32) -> Self {
        self.resources = Some(SandboxResources {
            cpu_count,
            memory_mib,
            disk_size_mib: 0, // disk size is determined after build.
        });
        self
    }

    /// Sets the command that should be running when the template snapshot is captured.
    pub fn start_cmd(mut self, cmd: impl Into<String>) -> Self {
        let cmd = cmd.into();
        self.start_cmd = (!cmd.trim().is_empty()).then_some(cmd);
        self
    }

    /// Sets the readiness command to poll before capturing the template snapshot.
    pub fn ready_cmd(mut self, cmd: impl Into<String>) -> Self {
        let cmd = cmd.into();
        self.ready_cmd = (!cmd.trim().is_empty()).then_some(cmd);
        self
    }

    pub(crate) fn parsed_alias(&self) -> TemplateBuildResult<Option<SnapshotAlias>> {
        self.alias
            .as_deref()
            .map(|value| {
                SnapshotAlias::parse(value)
                    .map_err(|error| TemplateBuildError::invalid_input(error.to_string()))
            })
            .transpose()
    }

    pub(crate) fn rootfs_base_ref(&self) -> Option<&TemplateBuildRootfsBase> {
        self.rootfs_base.as_ref()
    }

    pub(crate) fn steps(&self) -> &[TemplateBuildStep] {
        &self.steps
    }

    pub(crate) fn resources_ref(&self) -> Option<&SandboxResources> {
        self.resources.as_ref()
    }

    pub(crate) fn start_cmd_ref(&self) -> Option<&str> {
        self.start_cmd.as_deref()
    }

    pub(crate) fn ready_cmd_ref(&self) -> Option<&str> {
        self.ready_cmd.as_deref()
    }

    pub(crate) fn with_base_context(mut self, context: CommandContext) -> Self {
        self.base_context = Some(context);
        self
    }

    pub(crate) fn base_context_ref(&self) -> Option<&CommandContext> {
        self.base_context.as_ref()
    }

    pub(crate) fn overrides_startup(&self) -> bool {
        self.start_cmd.is_some() || self.ready_cmd.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::TemplateBuildSpec;

    #[test]
    fn getters_preserve_build_description() {
        let config = TemplateBuildSpec::new()
            .from_existing_rootfs("/tmp/rootfs.ext4")
            .alias("demo")
            .resources(2, 256)
            .start_cmd("echo start")
            .ready_cmd("echo ready")
            .run("echo hi");

        assert!(config.rootfs_base_ref().is_some());
        assert_eq!(
            config
                .parsed_alias()
                .expect("alias should parse")
                .as_ref()
                .map(ToString::to_string),
            Some("demo".to_string())
        );
        assert_eq!(
            config.resources_ref().map(|r| (r.cpu_count, r.memory_mib)),
            Some((2, 256))
        );
        assert_eq!(config.steps().len(), 1);
        assert_eq!(config.start_cmd_ref(), Some("echo start"));
        assert_eq!(config.ready_cmd_ref(), Some("echo ready"));
    }

    #[test]
    fn empty_startup_commands_are_ignored() {
        let config = TemplateBuildSpec::new().start_cmd("  ").ready_cmd("");

        assert_eq!(config.start_cmd_ref(), None);
        assert_eq!(config.ready_cmd_ref(), None);
        assert!(!config.overrides_startup());
    }
}
