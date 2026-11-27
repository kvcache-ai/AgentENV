mod admin;
mod attached_drives;
pub(crate) mod auth;
mod error;
pub(crate) mod image_build;
mod images;
mod pagination;
mod sandbox;
mod snapshots;
mod template;
mod template_helpers;
mod volumes;

use std::sync::Arc;

use anyhow::Error as AnyhowError;
use async_trait::async_trait;

use super::proxy::{build_proxy_client, ProxyClient};
use crate::api_key::ApiKey;
use crate::image::ImageResolver;
use crate::observability::ObservabilityService;
use crate::orchestrator::Orchestrator;
use crate::repository::RepositoryError;
use crate::snapshot::SnapshotManager;
use crate::template::{ImageBuildService, TemplateBuildService};
use crate::volume::VolumeManager;
use agentenv_http_server::apis;
use agentenv_http_server::models;

use error::ApiError;

#[derive(Clone, Debug)]
pub struct Claims;

impl From<models::LogLevel> for crate::logging::LogLevel {
    fn from(level: models::LogLevel) -> Self {
        match level {
            models::LogLevel::Debug => Self::Debug,
            models::LogLevel::Info => Self::Info,
            models::LogLevel::Warn => Self::Warn,
            models::LogLevel::Error => Self::Error,
        }
    }
}

impl From<crate::logging::LogLevel> for models::LogLevel {
    fn from(level: crate::logging::LogLevel) -> Self {
        match level {
            crate::logging::LogLevel::Debug => Self::Debug,
            crate::logging::LogLevel::Info => Self::Info,
            crate::logging::LogLevel::Warn => Self::Warn,
            crate::logging::LogLevel::Error => Self::Error,
        }
    }
}

#[derive(Clone)]
pub struct ApiImpl {
    orchestrator: Arc<Orchestrator>,
    snapshot_manager: Arc<SnapshotManager>,
    image_resolver: Arc<ImageResolver>,
    volume_manager: Arc<VolumeManager>,
    template_service: Arc<TemplateBuildService>,
    image_build_service: Arc<ImageBuildService>,
    observability: Option<Arc<ObservabilityService>>,
    proxy_client: ProxyClient,
    sandbox_proxy_domains: Vec<String>,
    api_key: ApiKey,
}

impl ApiImpl {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        orchestrator: Arc<Orchestrator>,
        snapshot_manager: Arc<SnapshotManager>,
        image_resolver: Arc<ImageResolver>,
        volume_manager: Arc<VolumeManager>,
        template_service: Arc<TemplateBuildService>,
        image_build_service: Arc<ImageBuildService>,
        observability: Option<Arc<ObservabilityService>>,
        sandbox_proxy_domains: Vec<String>,
        api_key: ApiKey,
    ) -> Self {
        Self {
            orchestrator,
            snapshot_manager,
            image_resolver,
            volume_manager,
            template_service,
            image_build_service,
            observability,
            proxy_client: build_proxy_client(),
            sandbox_proxy_domains,
            api_key,
        }
    }

    pub(crate) fn orchestrator(&self) -> Arc<Orchestrator> {
        Arc::clone(&self.orchestrator)
    }

    pub(crate) fn proxy_client(&self) -> &ProxyClient {
        &self.proxy_client
    }

    pub(crate) fn sandbox_proxy_domains(&self) -> &[String] {
        &self.sandbox_proxy_domains
    }

    pub(crate) fn image_resolver(&self) -> Arc<ImageResolver> {
        Arc::clone(&self.image_resolver)
    }

    pub(crate) fn snapshot_manager(&self) -> Arc<SnapshotManager> {
        Arc::clone(&self.snapshot_manager)
    }

    pub(crate) fn image_build_service(&self) -> &Arc<ImageBuildService> {
        &self.image_build_service
    }

    #[cfg(test)]
    pub(super) fn template_service(&self) -> &Arc<TemplateBuildService> {
        &self.template_service
    }

    #[cfg(test)]
    pub(super) fn meta(&self) -> &agentenv_metadata::MetaClient {
        self.template_service.meta()
    }

    #[cfg(test)]
    pub(super) fn set_image_build_service(&mut self, service: Arc<ImageBuildService>) {
        self.image_build_service = service;
    }

    #[cfg(test)]
    pub(super) fn set_template_service(&mut self, service: Arc<TemplateBuildService>) {
        self.template_service = service;
    }

    /// Returns the optional observability service backing node/admin
    /// observability endpoints. This is `None` when the server is configured
    /// with `observability.enabled = false`.
    pub(crate) fn observability(&self) -> Option<Arc<ObservabilityService>> {
        self.observability.as_ref().map(Arc::clone)
    }

    pub(crate) fn find_repository_error(err: &AnyhowError) -> Option<&RepositoryError> {
        ApiError::find_repository_error(err)
    }
}

#[async_trait]
impl apis::ErrorHandler<()> for ApiImpl {}

#[async_trait]
impl apis::default::Default<()> for ApiImpl {
    async fn health_get(
        &self,
        _method: &http::Method,
        _host: &headers::Host,
        _cookies: &axum_extra::extract::CookieJar,
    ) -> Result<apis::default::HealthGetResponse, ()> {
        crate::sandbox::UblkDeviceManager::check_health()
            .await
            .map_err(|error| {
                tracing::warn!(%error, "node is not healthy");
            })?;
        Ok(apis::default::HealthGetResponse::Status204_TheServiceIsHealthy)
    }
}
