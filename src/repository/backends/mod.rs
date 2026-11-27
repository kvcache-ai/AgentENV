pub(crate) mod oss;
pub(crate) mod posixfs;

use std::sync::Arc;

use anyhow::{Context, Result};

use super::{BlobKey, Repository};
use crate::cfg::{AppConfig, RepositoryStorageConfig, SnapshotRepositoryBackendKind};

pub(crate) use oss::OssRepository;
pub(crate) use posixfs::PosixFsRepository;

/// Open the configured durable artifact/document repository without starting
/// node-local runtime caches, resolvers, or P2P transports.
pub fn open_from_config(config: &AppConfig) -> Result<Arc<dyn Repository>> {
    open_from_storage_config(&config.repository_storage_config())
}

/// Open the durable repository from a narrow [`RepositoryStorageConfig`]
/// without requiring full node [`AppConfig`] validation.
pub fn open_from_storage_config(config: &RepositoryStorageConfig) -> Result<Arc<dyn Repository>> {
    match config.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let posix = config.posix_fs.as_ref().context(
                "backend.posix_fs config is required when repository_backend = posix_fs",
            )?;
            Ok(Arc::new(PosixFsRepository::new(
                posix.snapshot_store.join("repository"),
            )))
        }
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            Ok(Arc::new(OssRepository::from_config(
                oss_config,
                config.image_storage,
                &config.publish_compression,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn open_from_storage_config_accepts_stock_and_minimal_posix_configs() -> Result<()> {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bundled =
            RepositoryStorageConfig::load_from_path(Some(&workspace.join("config/default.toml")))?;
        open_from_storage_config(&bundled)?;

        let temp = tempdir()?;
        let minimal_cfg = temp.path().join("minimal-posix.toml");
        std::fs::write(&minimal_cfg, "home_path = \"./state\"\n")?;
        let minimal = RepositoryStorageConfig::load_from_path(Some(&minimal_cfg))?;
        open_from_storage_config(&minimal)?;
        Ok(())
    }
}
