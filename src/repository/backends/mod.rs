pub(crate) mod oss;
pub(crate) mod posixfs;

use std::sync::Arc;

use anyhow::{Context, Result};

use super::{BlobKey, Repository};
use crate::cfg::{AppConfig, SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind};

pub(crate) use oss::OssRepository;
pub(crate) use posixfs::PosixFsRepository;

/// Open the configured durable artifact/document repository without starting
/// node-local runtime caches, resolvers, or P2P transports.
pub fn open_from_config(config: &AppConfig) -> Result<Arc<dyn Repository>> {
    match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let posix = config.backend.posix_fs.as_ref().context(
                "backend.posix_fs config is required when repository_backend = posix_fs",
            )?;
            Ok(Arc::new(PosixFsRepository::new(
                posix.snapshot_store.join("repository"),
            )))
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
            Ok(Arc::new(OssRepository::from_config(
                oss_config,
                snapshot_image_storage,
                &config.snapshot.publish_compression,
            )?))
        }
    }
}

/// Returns the managed-layers `repoBlobUrl` implied by `config` when the
/// configured repository serves layers remotely (OSS), or `None` for local POSIX.
pub(crate) fn managed_layers_repo_blob_url_from_config(config: &AppConfig) -> Option<String> {
    config
        .backend
        .oss
        .as_ref()
        .filter(|_| config.snapshot.repository_backend == SnapshotRepositoryBackendKind::Oss)
        .map(|oss| {
            BlobKey::managed_layers_repo_blob_url(
                &oss.bucket,
                oss.prefix.as_deref().unwrap_or_default(),
            )
        })
}
