//! Node-local image storage ports used outside the image cache module.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use tracing::info;

use super::gc::ImageCacheGcSummary;
use super::service::{HoldNamespace, ImageCacheService};
use crate::cfg::{AppConfig, ConfigManager};
use crate::image::oci_image::ImageConversion;
use crate::image::ImageResolutionMetadata;
use crate::sandbox::RuntimeArtifactSet;
use crate::types::SandboxId;

pub(crate) enum CachedImageConfig {
    Missing,
    Found {
        image_config_path: PathBuf,
        metadata: Option<Box<ImageResolutionMetadata>>,
    },
}

#[derive(Debug)]
pub(crate) enum OverlaybdLayerLocation {
    LocalFile(PathBuf),
    CacheDir(PathBuf),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeImageOwner {
    StartingSandbox(SandboxId),
    PausedSandbox(SandboxId),
}

#[async_trait]
pub(crate) trait SourceImageStore: Send + Sync + std::fmt::Debug {
    async fn open(
        &self,
        manifest_digest: &str,
        repository_scope: Option<&str>,
    ) -> Result<Box<dyn SourceImageEntry>>;
}

#[async_trait]
pub(crate) trait SourceImageEntry: Send {
    async fn cached_config(&self) -> Result<CachedImageConfig>;

    async fn begin_conversion(&self) -> Result<Box<dyn ImageConversion>>;

    async fn publish_config(
        &self,
        overlaybd_config: &Value,
        metadata: ImageResolutionMetadata,
        conversion: Box<dyn ImageConversion>,
    ) -> Result<PathBuf>;

    async fn write_metadata(&self, metadata: ImageResolutionMetadata) -> Result<PathBuf>;
}

pub(crate) trait OverlaybdLayerStore: Send + Sync + std::fmt::Debug {
    fn layer_location(&self, digest: &str, size: u64, has_remote: bool) -> OverlaybdLayerLocation;

    fn publishable_roots(&self) -> Vec<PathBuf>;
}

#[async_trait]
pub(crate) trait RuntimeImageRefs: Send + Sync + std::fmt::Debug {
    async fn pin(&self, owner: RuntimeImageOwner, artifacts: RuntimeArtifactSet) -> Result<()>;

    async fn unpin_best_effort(&self, owner: RuntimeImageOwner);

    async fn reconcile_paused(&self, live_paused: &[SandboxId]) -> Result<()>;

    async fn maintain_running(&self, running: Vec<(SandboxId, RuntimeArtifactSet)>) -> Result<()>;

    async fn reclaim_for_disk_pressure(
        &self,
        running: Vec<(SandboxId, RuntimeArtifactSet)>,
        reclaim_bytes: u64,
    ) -> Result<ImageCacheGcSummary>;
}

#[derive(Clone)]
pub(crate) struct LocalImageServices {
    pub(crate) source_images: Arc<dyn SourceImageStore>,
    pub(crate) overlaybd_layers: Arc<dyn OverlaybdLayerStore>,
    pub(crate) runtime_refs: Arc<dyn RuntimeImageRefs>,
}

#[derive(Debug)]
struct ImageCacheStore {
    cache: Arc<ImageCacheService>,
    gc_watermark: Option<(u64, u64)>,
    gc_min_age: Duration,
}

struct ImageCacheSourceImageEntry {
    cache: Arc<ImageCacheService>,
    manifest_digest: String,
    repository_scope: Option<String>,
}

#[async_trait]
impl SourceImageStore for ImageCacheStore {
    async fn open(
        &self,
        manifest_digest: &str,
        repository_scope: Option<&str>,
    ) -> Result<Box<dyn SourceImageEntry>> {
        Ok(Box::new(ImageCacheSourceImageEntry {
            cache: Arc::clone(&self.cache),
            manifest_digest: manifest_digest.to_string(),
            repository_scope: repository_scope.map(|s| s.to_string()),
        }))
    }
}

#[async_trait]
impl SourceImageEntry for ImageCacheSourceImageEntry {
    async fn cached_config(&self) -> Result<CachedImageConfig> {
        self.cache
            .lookup_image_config(&self.manifest_digest, self.repository_scope.as_deref())
            .await
    }

    async fn begin_conversion(&self) -> Result<Box<dyn ImageConversion>> {
        self.cache
            .begin_image_conversion(&self.manifest_digest, self.repository_scope.as_deref())
            .await
    }

    async fn publish_config(
        &self,
        overlaybd_config: &Value,
        metadata: ImageResolutionMetadata,
        conversion: Box<dyn ImageConversion>,
    ) -> Result<PathBuf> {
        self.cache
            .publish_image_config(
                &self.manifest_digest,
                self.repository_scope.as_deref(),
                overlaybd_config,
                metadata,
                conversion,
            )
            .await
    }

    async fn write_metadata(&self, metadata: ImageResolutionMetadata) -> Result<PathBuf> {
        self.cache
            .write_image_metadata(
                &self.manifest_digest,
                self.repository_scope.as_deref(),
                metadata,
            )
            .await
    }
}

impl OverlaybdLayerStore for ImageCacheStore {
    fn layer_location(&self, digest: &str, size: u64, has_remote: bool) -> OverlaybdLayerLocation {
        self.cache
            .overlaybd_layer_location(digest, size, has_remote)
    }

    fn publishable_roots(&self) -> Vec<PathBuf> {
        self.cache.allowed_overlaybd_publish_roots()
    }
}

#[async_trait]
impl RuntimeImageRefs for ImageCacheStore {
    async fn pin(&self, owner: RuntimeImageOwner, artifacts: RuntimeArtifactSet) -> Result<()> {
        let (namespace, id) = owner_namespace(owner);
        self.cache
            .protect(
                namespace,
                &id.to_string(),
                artifacts.into_overlaybd_image_config_paths(),
            )
            .await
    }

    async fn unpin_best_effort(&self, owner: RuntimeImageOwner) {
        let (namespace, id) = owner_namespace(owner);
        self.cache
            .release_protection_best_effort(namespace, &id.to_string())
            .await;
    }

    async fn reconcile_paused(&self, live_paused: &[SandboxId]) -> Result<()> {
        let ids: Vec<String> = live_paused.iter().map(|id| id.to_string()).collect();
        self.cache
            .reconcile_namespace(HoldNamespace::Paused, &ids)
            .await
    }

    async fn maintain_running(&self, running: Vec<(SandboxId, RuntimeArtifactSet)>) -> Result<()> {
        let running = running
            .into_iter()
            .filter(|(_, artifacts)| !artifacts.is_empty())
            .map(|(id, artifacts)| {
                (
                    id.to_string(),
                    artifacts.into_overlaybd_image_config_paths(),
                )
            })
            .collect();
        let summary = self
            .cache
            .run_maintenance(running, self.gc_watermark, self.gc_min_age)
            .await?;
        info!(
            reclaimed = summary.collected,
            freed_bytes = summary.freed_bytes,
            retained = summary.retained,
            "image cache maintenance pass complete"
        );
        Ok(())
    }

    async fn reclaim_for_disk_pressure(
        &self,
        running: Vec<(SandboxId, RuntimeArtifactSet)>,
        reclaim_bytes: u64,
    ) -> Result<ImageCacheGcSummary> {
        let running = running
            .into_iter()
            .filter(|(_, artifacts)| !artifacts.is_empty())
            .map(|(id, artifacts)| {
                (
                    id.to_string(),
                    artifacts.into_overlaybd_image_config_paths(),
                )
            })
            .collect();
        self.cache
            .run_pressure_maintenance(running, reclaim_bytes, self.gc_min_age)
            .await
    }
}

fn owner_namespace(owner: RuntimeImageOwner) -> (HoldNamespace, SandboxId) {
    match owner {
        RuntimeImageOwner::StartingSandbox(id) => (HoldNamespace::Runtime, id),
        RuntimeImageOwner::PausedSandbox(id) => (HoldNamespace::Paused, id),
    }
}

pub(crate) fn local_image_services_from_app_config(config: &AppConfig) -> LocalImageServices {
    let cache = ImageCacheService::shared_from_app_config(config);
    let gc = config.image.cache.gc_schedule();
    let capacity_bytes = config.image_cache_layout().capacity_bytes;
    services_from_store(Arc::new(ImageCacheStore {
        cache,
        gc_watermark: gc.watermark_bytes(capacity_bytes),
        gc_min_age: gc.min_age,
    }))
}

pub(crate) fn local_image_services_from_global_config() -> LocalImageServices {
    local_image_services_from_app_config(ConfigManager::global_config())
}

fn services_from_store(store: Arc<ImageCacheStore>) -> LocalImageServices {
    LocalImageServices {
        source_images: store.clone(),
        overlaybd_layers: store.clone(),
        runtime_refs: store,
    }
}

#[cfg(test)]
pub(crate) fn test_local_image_services_from_service(
    cache: Arc<ImageCacheService>,
    gc_watermark: Option<(u64, u64)>,
    gc_min_age: Duration,
) -> LocalImageServices {
    services_from_store(Arc::new(ImageCacheStore {
        cache,
        gc_watermark,
        gc_min_age,
    }))
}
