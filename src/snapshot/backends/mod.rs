//! The configured snapshot storage: a durable repository paired with the
//! node-local runtime resolver that reads snapshots from it.

use std::sync::Arc;

use anyhow::{Context, Result};

use super::artifact_cache::LocalArtifactCache;
use super::resolver::RepositoryRuntimeResolver;
use crate::cfg::{AppConfig, ConfigManager, SnapshotRepositoryBackendKind};
use crate::image::cache::local_image_services_from_app_config;
use crate::p2p::P2pTransport;
use crate::repository::Repository;

type SnapshotStorageParts = (Arc<dyn Repository>, Arc<RepositoryRuntimeResolver>);

/// Builds the configured snapshot repository and its matching runtime resolver from the global configuration.
pub fn build_snapshot_storage(
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<SnapshotStorageParts> {
    build_snapshot_storage_from_config(ConfigManager::global_config(), p2p_transport)
}

/// Builds the configured snapshot repository and its matching runtime resolver from `config`.
pub fn build_snapshot_storage_from_config(
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
    let runtime_resolver = Arc::new(RepositoryRuntimeResolver::new(
        Arc::clone(&repository),
        cache,
        runtime_root,
        overlaybd_layers,
        resolver_p2p,
    ));
    Ok((repository, runtime_resolver))
}

#[cfg(test)]
mod tests;
