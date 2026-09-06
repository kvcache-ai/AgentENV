pub(crate) mod common;
pub(crate) mod oss;
pub(crate) mod posixfs;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cfg::{
    AppConfig, ConfigManager, SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind,
};
use crate::image::cache::local_image_services_from_app_config;
use crate::p2p::P2pTransport;
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
pub use oss::OssBackend;
pub use posixfs::{PosixFsBackend, PosixFsBackendConfig};

/// Builds the configured snapshot repository backend and its matching runtime resolver from the global configuration.
pub fn build_snapshot_backend(
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<(
    Arc<dyn SnapshotRepository>,
    Arc<dyn SnapshotRuntimeResolver>,
)> {
    build_snapshot_backend_from_config(ConfigManager::global_config(), p2p_transport)
}

/// Internal builders share the configured durable backend, but have a separate
/// catalog so they cannot be listed or deleted through the public template API.
pub(crate) fn build_builder_snapshot_backend() -> Result<(
    Arc<dyn SnapshotRepository>,
    Arc<dyn SnapshotRuntimeResolver>,
)> {
    let config = builder_snapshot_config(ConfigManager::global_config());
    build_snapshot_backend_from_config(&config, None)
}

fn builder_snapshot_config(config: &AppConfig) -> AppConfig {
    let mut config = config.clone();
    let namespace = "template-build/builder";
    if let Some(posix) = config.backend.posix_fs.as_mut() {
        posix.snapshot_store = posix.snapshot_store.join(namespace);
    }
    if let Some(oss) = config.backend.oss.as_mut() {
        let prefix = oss.prefix.as_deref().unwrap_or("").trim().trim_matches('/');
        oss.prefix = Some(if prefix.is_empty() {
            namespace.to_owned()
        } else {
            format!("{prefix}/{namespace}")
        });
    }
    config.snapshot.local_cache_path = config.snapshot.local_cache_path.join(namespace);
    config
}

fn build_snapshot_backend_from_config(
    config: &AppConfig,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<(
    Arc<dyn SnapshotRepository>,
    Arc<dyn SnapshotRuntimeResolver>,
)> {
    let shared_cache_root = config.snapshot.local_cache_path.clone();
    let overlaybd_layers = local_image_services_from_app_config(config).overlaybd_layers;
    match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let root = config
                .backend
                .posix_fs
                .as_ref()
                .context("backend.posix_fs config is required when repository_backend = posix_fs")?
                .snapshot_store
                .join("repository");
            let cache = LocalArtifactCache::new(shared_cache_root.clone(), None)?;
            Ok(PosixFsBackend::from_parts(
                PosixFsBackendConfig {
                    root,
                    cache_root: Some(shared_cache_root.clone()),
                    runtime_cache_root: Some(shared_cache_root.join("runtime")),
                },
                overlaybd_layers,
                cache,
            )
            .into_parts())
        }
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .backend
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            let snapshot_image_storage = if config.snapshot.image_publish.enabled {
                SnapshotImageStoragePolicy::SourceRegistry
            } else {
                SnapshotImageStoragePolicy::ObjectStorage
            };
            let cache =
                LocalArtifactCache::new(shared_cache_root.clone(), oss_config.cache_max_size_gb)?;
            Ok(OssBackend::from_parts(
                oss_config,
                snapshot_image_storage,
                cache,
                shared_cache_root.join("runtime"),
                overlaybd_layers,
                p2p_transport,
            )?
            .into_parts())
        }
    }
}

pub(crate) fn shared_runtime_cache_root() -> PathBuf {
    ConfigManager::global_config()
        .snapshot
        .local_cache_path
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{OssBackendConfig, PosixFsBackendConfig as PosixConfig};
    use crate::snapshot::{SnapshotAlias, SnapshotId, SnapshotRecord};

    #[tokio::test]
    async fn builders_share_a_private_catalog_across_node_local_caches() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut node_a = AppConfig::default();
        node_a.snapshot.repository_backend = SnapshotRepositoryBackendKind::PosixFs;
        node_a.backend.posix_fs = Some(PosixConfig {
            snapshot_store: dir.path().join("shared"),
        });
        node_a.snapshot.local_cache_path = dir.path().join("node-a/cache");
        node_a.image.cache.root_dir = dir.path().join("node-a/images");
        let mut node_b = node_a.clone();
        node_b.home_path = dir.path().join("node-b");
        node_b.snapshot.local_cache_path = dir.path().join("node-b/cache");
        node_b.image.cache.root_dir = dir.path().join("node-b/images");
        let (a, _) = build_snapshot_backend_from_config(&builder_snapshot_config(&node_a), None)?;
        let (b, _) = build_snapshot_backend_from_config(&builder_snapshot_config(&node_b), None)?;
        let record = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(SnapshotAlias::parse("builder")?),
            Default::default(),
        );
        a.create(record.clone()).await?;
        assert_eq!(b.get("builder").await?.unwrap().id, record.id);
        let (public, _) = build_snapshot_backend_from_config(&node_b, None)?;
        assert!(public.get("builder").await?.is_none());
        public.delete("builder").await?;
        assert!(b.get("builder").await?.is_some());
        Ok(())
    }

    #[test]
    fn builder_namespace_preserves_oss_bucket_and_prefix() {
        for (prefix, expected) in [
            (None, "template-build/builder"),
            (
                Some(" /cluster/snapshots/ "),
                "cluster/snapshots/template-build/builder",
            ),
        ] {
            let mut config = AppConfig::default();
            config.snapshot.repository_backend = SnapshotRepositoryBackendKind::Oss;
            config.backend.oss = Some(OssBackendConfig {
                endpoint: "https://storage.example.test".to_owned(),
                bucket: "shared".to_owned(),
                prefix: prefix.map(str::to_owned),
                credential_process: None,
                access_key_id: None,
                access_key_secret: None,
                security_token: None,
                region: Some("test".to_owned()),
                addressing_style: None,
                cache_max_size_gb: None,
            });
            let builder = builder_snapshot_config(&config);
            assert_eq!(
                builder.snapshot.repository_backend,
                SnapshotRepositoryBackendKind::Oss
            );
            let oss = builder.backend.oss.unwrap();
            assert_eq!(oss.bucket, "shared");
            assert_eq!(oss.prefix.as_deref(), Some(expected));
            assert_eq!(config.backend.oss.unwrap().prefix.as_deref(), prefix);
        }
    }
}
