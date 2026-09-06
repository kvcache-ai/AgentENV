use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tracing::info;

use super::ApiImpl;
use crate::{
    cfg::ConfigManager,
    snapshot::{
        repository::backends::build_builder_snapshot_backend, CommandContext, RepositoryError,
        RunnableSnapshot, SnapshotManager,
    },
    template::TemplateBuildSpec,
    types::ImageConfigs,
};

const BUILDER_READY: &str =
    "command -v buildkitd && command -v buildctl && mkdir -p /var/lib/buildkit /run/aenv-buildkit";

impl ApiImpl {
    pub(super) async fn builder_template(&self) -> Result<RunnableSnapshot> {
        let api = self.clone();
        // A cancelled request must not cancel initialization shared by other builds.
        tokio::spawn(async move {
            api.build_sessions
                .builder_template
                .get_or_try_init(|| api.initialize_builder_template())
                .await
                .cloned()
        })
        .await?
    }

    async fn initialize_builder_template(&self) -> Result<RunnableSnapshot> {
        let config = ConfigManager::global_config();
        let builder = &config.template_build;
        let inputs = serde_json::to_vec(&(
            1,
            &builder.builder_image,
            builder.builder_cpu_count,
            builder.builder_memory_mb,
            BUILDER_READY,
            config.virtualization_mode,
        ))?;
        let alias = format!("builder-{:x}", Sha256::digest(inputs));
        let (repository, resolver) = build_builder_snapshot_backend()?;
        let manager = SnapshotManager::from_parts(repository, resolver, None);
        if let Some(snapshot) = manager.load_runnable(&alias).await? {
            info!(snapshot_id = %snapshot.record().id, "reusing prepared Dockerfile builder template");
            return Ok(snapshot);
        }

        info!("preparing Dockerfile builder template for the first build");
        let resolved = self.image_resolver.resolve(&builder.builder_image).await?;
        let mut image_configs = ImageConfigs::new();
        if let Some(config) = resolved.raw_config {
            image_configs.add(None::<String>, "/", config);
        }
        let context = CommandContext::from_env_and_workdir(
            resolved.base_context.env_vars,
            Some("/".to_owned()),
        )
        .with_user(Some("root".to_owned()));
        let spec = TemplateBuildSpec::new()
            .alias(&alias)
            .resources(builder.builder_cpu_count, builder.builder_memory_mb)
            .with_startup_shell("/bin/sh")
            .with_resolved_overlaybd_image(resolved.overlaybd_config_path, image_configs)
            .with_base_context(context)
            .ready_cmd(BUILDER_READY);
        let result = self
            .template_builder
            .build_and_publish(&manager, spec)
            .await;
        // Two nodes may prepare the first builder simultaneously. If another
        // node published the alias first, reuse its committed snapshot.
        if let Err(error) = result {
            let error = anyhow::Error::new(error);
            if !error.chain().any(|cause| {
                matches!(
                    cause.downcast_ref::<RepositoryError>(),
                    Some(RepositoryError::AliasConflict { .. })
                )
            }) {
                return Err(error);
            }
        }
        manager
            .load_runnable(&alias)
            .await?
            .context("prepared builder template is missing")
    }
}
