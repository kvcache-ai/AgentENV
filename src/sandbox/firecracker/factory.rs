//! Factory for creating FirecrackerSandbox instances.
//!
//! This module implements the [`FirecrackerSandboxFactory`] which creates instances of [`FirecrackerSandbox`] by
//! reading configuration from the global [`ConfigManager`]. It also handles
//! launches from committed snapshots and resumes from snapshot configs.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use serde_json::Value;

use super::config::FirecrackerSandboxConfig;
use super::sandbox::{FirecrackerPausedState, FirecrackerSandbox};
use crate::cfg::ConfigManager;
use crate::sandbox::backend::{PausedSandboxState, SandboxBackend, SandboxBackendFactory};
use crate::sandbox::{
    EnvdAccessToken, FreshSandboxBuildSpec, OverlaybdConfig, SandboxLaunchConfig, UblkConfig,
};
use crate::snapshot::RunnableSnapshot;
use crate::types::SandboxId;

pub struct FirecrackerSandboxFactory {
    cpu_config_arc: Option<Arc<RwLock<Option<String>>>>,
}

impl FirecrackerSandboxFactory {
    pub fn new() -> Self {
        Self {
            cpu_config_arc: None,
        }
    }

    pub fn with_cpu_config(arc: Arc<RwLock<Option<String>>>) -> Self {
        Self {
            cpu_config_arc: Some(arc),
        }
    }
}

impl Default for FirecrackerSandboxFactory {
    fn default() -> Self {
        Self::new()
    }
}

fn filter_extra_boot_args(
    extra_boot_args: Option<&str>,
    allowed_prefixes: &[String],
) -> Option<String> {
    let extra_boot_args = extra_boot_args?.trim();
    if extra_boot_args.is_empty() {
        return None;
    }

    let filtered: Vec<&str> = extra_boot_args
        .split_whitespace()
        .filter(|arg| is_allowed_extra_boot_arg(arg, allowed_prefixes))
        .collect();

    (!filtered.is_empty()).then(|| filtered.join(" "))
}

fn is_allowed_extra_boot_arg(arg: &str, allowed_prefixes: &[String]) -> bool {
    let is_valid_char =
        |ch: char| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | '+' | '=');
    if arg.is_empty() || !arg.chars().all(is_valid_char) {
        return false;
    }

    allowed_prefixes
        .iter()
        .filter(|prefix| !prefix.is_empty())
        .any(|prefix| arg.starts_with(prefix))
}

impl SandboxBackendFactory for FirecrackerSandboxFactory {
    fn build(
        &self,
        build_spec: FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>> {
        let user_image_config = OverlaybdConfig {
            image_config_path: build_spec.image_config_path,
            read_only: false,
            runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
        };
        let mut config =
            FirecrackerSandboxConfig::from_global_config_with_user_image(user_image_config)?;
        let (rootfs_image_config_path, rootfs_read_only, runtime_upper_mode) = {
            let rootfs_image_config = config
                .common
                .rootfs_image_config
                .as_ref()
                .context("fresh sandbox rootfs image config is missing")?;
            (
                rootfs_image_config.image_config_path.clone(),
                rootfs_image_config.read_only,
                rootfs_image_config.runtime_upper_mode,
            )
        };
        config.common.ublk_config = Some(UblkConfig::overlaybd_with_runtime_upper_mode(
            rootfs_image_config_path,
            rootfs_read_only,
            runtime_upper_mode,
        ));
        config.vcpu_count = build_spec.resources.cpu_count;
        config.mem_size_mib = build_spec.resources.memory_mib;
        config.common.rootfs_virtual_size = if build_spec.resources.disk_size_mib == 0 {
            None
        } else {
            Some(
                u64::from(build_spec.resources.disk_size_mib)
                    .checked_mul(1024 * 1024)
                    .context("requested rootfs size overflows bytes")?,
            )
        };
        config.common.extra_drives = build_spec.extra_drives;
        let allowed_extra_boot_args_prefixes = ConfigManager::global_config()
            .firecracker
            .allowed_extra_boot_args_prefixes
            .as_deref()
            .unwrap_or_default();
        if let Some(extra_boot_args) = filter_extra_boot_args(
            build_spec.extra_boot_args.as_deref(),
            allowed_extra_boot_args_prefixes,
        ) {
            config.boot_args = Some(match config.boot_args.take() {
                Some(existing) => format!("{existing} {extra_boot_args}"),
                None => extra_boot_args,
            });
        }
        config.common.cpu_config_json = self
            .cpu_config_arc
            .as_ref()
            .and_then(|arc| arc.read().unwrap().clone());
        config.common.env_vars = (!build_spec.context.env_vars.is_empty())
            .then_some(build_spec.context.env_vars.clone());
        config.common.default_workdir = Some(build_spec.context.workdir.clone());
        config.common.default_user = build_spec.context.user.clone();
        let config = config.apply_launch_config(&launch_config);
        let sandbox = FirecrackerSandbox::new_with_id(config, launch_config.sandbox_id)?;
        Ok(Box::new(sandbox))
    }

    fn build_from_snapshot(
        &self,
        snapshot: &RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>> {
        let sandbox = FirecrackerSandbox::from_snapshot(snapshot, &launch_config)?;
        Ok(Box::new(sandbox))
    }

    fn decode_paused_state(
        &self,
        artifact_root: PathBuf,
        state: Value,
    ) -> Result<Arc<dyn PausedSandboxState>> {
        Ok(Arc::new(FirecrackerPausedState::decode(
            artifact_root,
            state,
        )?))
    }

    fn build_from_paused_state(
        &self,
        sandbox_id: SandboxId,
        state: &dyn PausedSandboxState,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Box<dyn SandboxBackend>> {
        let paused_state = state
            .downcast_ref::<FirecrackerPausedState>()
            .context("The provided PausedSandboxState is not a Firecracker paused state")?;
        let sandbox = FirecrackerSandbox::from_snapshot_config_with_override(
            paused_state.snapshot_config().clone(),
            sandbox_id,
            envd_access_token,
        )?;
        Ok(Box::new(sandbox))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::sandbox::{PausedSandboxState, RuntimeArtifactSet};

    #[derive(Debug)]
    struct WrongPausedState;

    impl PausedSandboxState for WrongPausedState {
        fn encode(&self) -> Result<Value> {
            Ok(Value::Null)
        }

        fn runtime_artifacts(&self) -> RuntimeArtifactSet {
            RuntimeArtifactSet::empty()
        }
    }

    #[test]
    fn filter_extra_boot_args_keeps_only_allowed_prefixes() {
        let prefixes = vec!["agentenv-custom.".to_string(), "agentenv.safe=".to_string()];

        assert_eq!(
            filter_extra_boot_args(
                Some("agentenv-custom.foo=bar debug agentenv.safe=1 root=/dev/vda"),
                &prefixes,
            ),
            Some("agentenv-custom.foo=bar agentenv.safe=1".to_string())
        );
    }

    #[test]
    fn filter_extra_boot_args_drops_when_no_prefix_matches() {
        let prefixes = vec!["agentenv-custom.".to_string()];

        assert_eq!(
            filter_extra_boot_args(Some("debug panic=1"), &prefixes),
            None
        );
        assert_eq!(
            filter_extra_boot_args(Some("agentenv-custom.foo=bar"), &[]),
            None
        );
    }

    #[test]
    fn filter_extra_boot_args_drops_invalid_chars() {
        let prefixes = vec!["agentenv-custom.".to_string()];

        assert_eq!(
            filter_extra_boot_args(
                Some(
                    "agentenv-custom.flag agentenv-custom.bad_char=a:b agentenv-custom.good=azAZ09_-./+= agentenv-custom.base64=YWJjZA==/+",
                ),
                &prefixes,
            ),
            Some(
                "agentenv-custom.flag agentenv-custom.good=azAZ09_-./+= agentenv-custom.base64=YWJjZA==/+"
                    .to_string()
            )
        );
    }

    #[test]
    fn build_from_paused_state_rejects_wrong_snapshot_type() {
        let factory = FirecrackerSandboxFactory::new();
        let state: Arc<dyn PausedSandboxState> = Arc::new(WrongPausedState);

        match factory.build_from_paused_state(SandboxId::new(), state.as_ref(), None) {
            Ok(_) => panic!("wrong snapshot type should fail"),
            Err(err) => assert!(err.to_string().contains("not a Firecracker paused state")),
        }
    }
}
