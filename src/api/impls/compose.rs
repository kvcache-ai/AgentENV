use std::collections::HashMap;
use std::time::Duration;

use agentenv_http_server::models;
use tokio::time::{timeout_at, Instant};

use super::{sandbox::cold_start_resources, ApiImpl};
use crate::cfg::ConfigManager;
use crate::compose::{self, ComposeBootstrap};
use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::{
    CreateSandboxRequest, SandboxLaunchSource, SandboxMetadata, SandboxTimeoutAction,
};
use crate::sandbox::ExtraDrive;
use crate::types::ImageConfigs;

impl ApiImpl {
    pub(super) async fn create_compose(
        &self,
        body: &models::NewComposeSandbox,
    ) -> Result<SandboxMetadata, models::Error> {
        if !cfg!(target_arch = "x86_64")
            || ConfigManager::global_config().virtualization_mode
                != crate::virtualization::VirtualizationMode::Kvm
        {
            return Err(Self::error(
                500,
                "Compose currently requires Linux x86-64 with KVM",
            ));
        }
        let config = &ConfigManager::global_config().compose;
        let base_image = config
            .base_image
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Self::error(500, "Compose is not configured: set compose.base_image"))?;
        let startup_timeout = body.startup_timeout.unwrap_or(300);
        let mut cold = models::NewColdSandbox::new(base_image.to_owned());
        cold.cpu_count = body.cpu_count;
        cold.memory_mb = body.memory_mb;
        cold.disk_size_mb = body.disk_size_mb;
        let resources = cold_start_resources(&cold)?;
        let deadline = Instant::now() + Duration::from_secs(startup_timeout as u64);
        let timer = SandboxStageTimer::new("create_compose");
        let plan_request = serde_json::json!({
            "compose": body.compose,
            "composeEnv": body.compose_env.clone().unwrap_or_default(),
            "profiles": body.profiles.clone().unwrap_or_default(),
        });
        let prepared = timeout_at(deadline, async {
            let mut plan = timer
                .time(
                    "plan",
                    compose::prepare(&config.planner_binary, plan_request),
                )
                .await
                .map_err(|err| {
                    Self::error(
                        if err.is::<compose::InvalidCompose>() {
                            400
                        } else {
                            500
                        },
                        err.to_string(),
                    )
                })?;
            let resolver = self.image_resolver();
            let root = timer
                .time("resolve_rootfs", resolver.resolve(base_image))
                .await
                .map_err(|err| Self::error(500, format!("resolve Compose base image: {err:#}")))?;
            let mut resolved = HashMap::new();
            let mut extra_drives = Vec::with_capacity(plan.services.len());
            let mut image_configs = ImageConfigs::new();
            if let Some(raw) = &root.raw_config {
                image_configs.add(None::<String>, "/", raw.clone());
            }
            for service in &mut plan.services {
                // Share immutable resolution, but allocate a separate writable upper
                // and placeholder image for every service, even for identical sources.
                if !resolved.contains_key(&service.image) {
                    let image = timer
                        .time("resolve_service_image", resolver.resolve(&service.image))
                        .await
                        .map_err(|err| {
                            Self::error(
                                if err.is_user_error() { 400 } else { 500 },
                                format!("resolve service {}: {err:#}", service.name),
                            )
                        })?;
                    resolved.insert(service.image.clone(), image);
                }
                let image = &resolved[&service.image];
                service.config = image.raw_config.clone().ok_or_else(|| {
                    Self::error(
                        500,
                        format!("service {} image config is missing", service.name),
                    )
                })?;
                image_configs.add(
                    Some(service.drive_id.clone()),
                    service.mount_path.clone(),
                    service.config.clone(),
                );
                extra_drives.push(
                    ExtraDrive::try_new_overlaybd_with_mount_path(
                        service.drive_id.clone(),
                        image.overlaybd_config_path.clone(),
                        false,
                        service.mount_path.clone(),
                        None::<std::path::PathBuf>,
                    )
                    .map_err(|err| Self::error(500, err.to_string()))?,
                );
            }
            // Image configs are added after planning and count toward the same
            // guest frame limit. Reject before allocating any VM resources.
            compose::encode_plan(&plan).map_err(|err| Self::error(400, err.to_string()))?;
            let request = CreateSandboxRequest {
                source: SandboxLaunchSource::Image {
                    image_ref: root.image_ref,
                    overlaybd_config_path: root.overlaybd_config_path,
                    context: Box::new(root.base_context.into()),
                    resources: Some(resources),
                    extra_drives,
                    extra_boot_args: None,
                    image_configs: Box::new(image_configs),
                },
                extra_drives: Vec::new(),
                extra_drives_in_snapshot: false,
                timeout: Some(Duration::from_secs(body.timeout.unwrap_or(300) as u64)),
                timeout_action: if body.auto_pause == Some(false) {
                    SandboxTimeoutAction::Delete
                } else {
                    SandboxTimeoutAction::Pause
                },
                auto_resume: false,
                user_metadata: body.metadata.clone(),
                env_vars: None,
                network_policy: Default::default(),
                secure: true,
                custom_extension_params: None,
                volume_mounts: HashMap::new(),
            };
            Ok::<_, models::Error>((request, ComposeBootstrap { plan, deadline }))
        })
        .await
        .map_err(|_| {
            Self::error(
                500,
                "Compose startup deadline exceeded while preparing images",
            )
        })??;
        // Lifecycle owns cancellation and rollback from this point onward.
        timer
            .time(
                "create_sandbox",
                self.orchestrator
                    .create_compose_sandbox(prepared.0, prepared.1),
            )
            .await
            .map_err(|err| Self::internal_error(&err))
    }
}
