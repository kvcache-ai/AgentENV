//! The configured snapshot storage: a durable repository paired with the
//! node-local runtime resolver that reads snapshots from it.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::artifact_cache::LocalArtifactCache;
use super::resolver::RepositoryRuntimeResolver;
use super::SnapshotRuntimeResolver;

pub type SnapshotStorageParts = (Arc<dyn Repository>, Arc<dyn SnapshotRuntimeResolver>);

use crate::cfg::{
    AppConfig, ConfigManager, OssBackendConfig, SnapshotImageStoragePolicy,
    SnapshotPublishCompressionConfig, SnapshotRepositoryBackendKind,
};
use crate::image::cache::{
    local_image_services_from_app_config, local_image_services_from_global_config,
    OverlaybdLayerStore,
};
use crate::p2p::P2pTransport;
use crate::repository::backends::{OssRepository, PosixFsRepository};
use crate::repository::Repository;

/// Builds the configured snapshot repository and its matching runtime resolver from the global configuration.
pub fn build_snapshot_storage(
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<SnapshotStorageParts> {
    build_snapshot_storage_from_config(ConfigManager::global_config(), p2p_transport)
}

fn build_snapshot_storage_from_config(
    config: &AppConfig,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<SnapshotStorageParts> {
    let shared_cache_root = config.snapshot.local_root.join("cache");
    let runtime_root = shared_cache_root.join("runtime");
    let overlaybd_layers = local_image_services_from_app_config(config).overlaybd_layers;
    let repository = crate::repository::open_from_config(config)?;
    let (cache_max_size_gb, resolver_p2p) = match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => (None, None),
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .backend
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            std::fs::create_dir_all(&runtime_root)
                .with_context(|| format!("create oss runtime root '{}'", runtime_root.display()))?;
            (oss_config.cache_max_size_gb, p2p_transport)
        }
    };
    let cache = LocalArtifactCache::new(shared_cache_root, cache_max_size_gb)?;
    let runtime_resolver: Arc<dyn SnapshotRuntimeResolver> =
        Arc::new(RepositoryRuntimeResolver::new(
            Arc::clone(&repository),
            cache,
            runtime_root,
            overlaybd_layers,
            resolver_p2p,
        ));
    Ok((repository, runtime_resolver))
}

fn shared_cache_root() -> PathBuf {
    ConfigManager::global_config()
        .snapshot
        .local_root
        .join("cache")
}

#[derive(Clone, Debug)]
pub struct PosixFsBackendConfig {
    /// Durable repository root. This may live on a shared POSIX or distributed filesystem.
    pub root: std::path::PathBuf,
    /// Optional shared node-local cache root for downloaded/materialized runtime artifacts.
    ///
    /// When `None`, defaults to `<snapshot.local_root>/cache`.
    pub cache_root: Option<std::path::PathBuf>,
    /// Optional node-local cache root for runtime-materialized files such as runnable image configs.
    ///
    /// When `None`, defaults to `<cache_root>/runtime`.
    pub runtime_cache_root: Option<std::path::PathBuf>,
}

pub struct PosixFsBackend {
    repository: Arc<dyn Repository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
}

impl PosixFsBackend {
    /// Builds the POSIX backend bundle: durable repository state plus node-local runtime resolver.
    pub fn new(config: PosixFsBackendConfig) -> Result<Self> {
        let cache_root = config.cache_root.clone().unwrap_or_else(shared_cache_root);
        let cache = LocalArtifactCache::new(cache_root, None)?;
        Ok(Self::from_parts(
            config,
            local_image_services_from_global_config().overlaybd_layers,
            cache,
        ))
    }

    /// Builds the POSIX backend bundle using a shared node-local artifact cache.
    pub(crate) fn from_parts(
        config: PosixFsBackendConfig,
        store: Arc<dyn OverlaybdLayerStore>,
        cache: Arc<LocalArtifactCache>,
    ) -> Self {
        let PosixFsBackendConfig {
            root,
            cache_root,
            runtime_cache_root,
        } = config;
        let cache_root = cache_root.unwrap_or_else(shared_cache_root);
        let runtime_cache_root = runtime_cache_root.unwrap_or_else(|| cache_root.join("runtime"));
        let repository: Arc<dyn Repository> = Arc::new(PosixFsRepository::new(root));
        // Files are read in place, so the resolver never fetches from peers.
        let runtime_resolver: Arc<dyn SnapshotRuntimeResolver> =
            Arc::new(RepositoryRuntimeResolver::new(
                Arc::clone(&repository),
                cache,
                runtime_cache_root,
                store,
                None,
            ));

        Self {
            repository,
            runtime_resolver,
        }
    }

    /// Returns the durable artifact repository.
    pub fn repository(&self) -> Arc<dyn Repository> {
        Arc::clone(&self.repository)
    }

    /// Returns the node-local runtime resolver used to materialize runnable paths.
    pub fn runtime_resolver(&self) -> Arc<dyn SnapshotRuntimeResolver> {
        Arc::clone(&self.runtime_resolver)
    }

    /// Splits the backend into its repository and runtime-resolution components.
    pub fn into_parts(self) -> SnapshotStorageParts {
        (self.repository, self.runtime_resolver)
    }
}

/// OSS-backed snapshot backend.
///
/// Combines the durable committed-state repository (stored in OSS) with a
/// node-local runtime resolver that materializes runnable overlaybd configs.
pub struct OssBackend {
    repository: Arc<dyn Repository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
}

impl OssBackend {
    /// Build the OSS backend from config.
    ///
    /// This convenience constructor remains available for tests and direct
    /// callers. The main backend factory constructs a shared cache once and
    /// uses [`OssBackend::from_parts`] instead. Publish compression stays
    /// disabled here; use [`OssBackend::new_with_publish_compression`] to
    /// exercise `[snapshot.publish_compression]`.
    pub fn new(config: &OssBackendConfig, cache_root: PathBuf) -> Result<Self> {
        Self::new_with_publish_compression(
            config,
            cache_root,
            &SnapshotPublishCompressionConfig::default(),
        )
    }

    /// Build the OSS backend from config with explicit publish-compression
    /// settings, for tests and direct callers that cannot go through the
    /// global config.
    pub fn new_with_publish_compression(
        config: &OssBackendConfig,
        cache_root: PathBuf,
        publish_compression: &SnapshotPublishCompressionConfig,
    ) -> Result<Self> {
        let cache = LocalArtifactCache::new(cache_root.clone(), config.cache_max_size_gb)?;
        Self::from_parts(
            config,
            SnapshotImageStoragePolicy::default(),
            publish_compression,
            cache,
            cache_root.join("runtime"),
            local_image_services_from_global_config().overlaybd_layers,
            None,
        )
    }

    /// Build the OSS backend from config plus a shared node-local cache.
    pub(crate) fn from_parts(
        config: &OssBackendConfig,
        snapshot_image_storage: SnapshotImageStoragePolicy,
        publish_compression: &SnapshotPublishCompressionConfig,
        cache: Arc<LocalArtifactCache>,
        runtime_root: PathBuf,
        store: Arc<dyn OverlaybdLayerStore>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Result<Self> {
        let repository: Arc<dyn Repository> = Arc::new(OssRepository::from_config(
            config,
            snapshot_image_storage,
            publish_compression,
        )?);

        std::fs::create_dir_all(&runtime_root)
            .with_context(|| format!("create oss runtime root '{}'", runtime_root.display()))?;
        let runtime_resolver: Arc<dyn SnapshotRuntimeResolver> =
            Arc::new(RepositoryRuntimeResolver::new(
                Arc::clone(&repository),
                cache,
                runtime_root,
                store,
                p2p_transport,
            ));

        Ok(Self {
            repository,
            runtime_resolver,
        })
    }

    /// Returns the durable artifact repository.
    pub fn repository(&self) -> Arc<dyn Repository> {
        Arc::clone(&self.repository)
    }

    /// Returns the node-local runtime resolver used to materialize runnable paths.
    pub fn runtime_resolver(&self) -> Arc<dyn SnapshotRuntimeResolver> {
        Arc::clone(&self.runtime_resolver)
    }

    /// Splits the backend into its repository and runtime-resolution components.
    pub fn into_parts(self) -> SnapshotStorageParts {
        (self.repository, self.runtime_resolver)
    }
}

#[cfg(test)]
mod tests;
