//! Storage operations used by the metadata service's collector.

use agentenv_domain::SnapshotId;
use agentenv_metadata::{GcStorage, LayerDeleteError};
use async_trait::async_trait;
use std::sync::Arc;

use crate::repository::{BlobKey, BlobLocation, LayerRole, Repository};

pub struct SnapshotGcStorage(pub Arc<dyn Repository>);

impl SnapshotGcStorage {
    pub fn from_config(config: &crate::cfg::AppConfig) -> anyhow::Result<Self> {
        let repository = match config.snapshot.repository_backend {
            crate::cfg::SnapshotRepositoryBackendKind::PosixFs => {
                let settings = config
                    .backend
                    .posix_fs
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing backend.posix_fs configuration"))?;
                Arc::new(crate::repository::backends::PosixFsRepository::new(
                    settings.snapshot_store.join("repository"),
                )) as Arc<dyn Repository>
            }
            crate::cfg::SnapshotRepositoryBackendKind::Oss => {
                let settings = config
                    .backend
                    .oss
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing backend.oss configuration"))?;
                Arc::new(crate::repository::backends::OssRepository::from_config(
                    settings,
                    crate::cfg::SnapshotImageStoragePolicy::ObjectStorage,
                    &config.snapshot.publish_compression,
                )?)
            }
        };
        Ok(Self(repository))
    }
}

#[async_trait]
impl GcStorage for SnapshotGcStorage {
    async fn delete_snapshot(&self, id: &SnapshotId) -> anyhow::Result<()> {
        let repo = self.0.as_ref();
        let publications = super::store::read_snapshot_content(repo, id)
            .await?
            .and_then(|content| content.manifest)
            .map(|manifest| manifest.disk_publications)
            .unwrap_or_default();
        super::catalog::remove_mirror(repo, id, None).await?;
        Ok(super::store::delete_snapshot(repo, id, &publications).await?)
    }

    async fn delete_layer(&self, digest: &str) -> Result<(), LayerDeleteError> {
        let repo = self.0.as_ref();
        if !digest
            .strip_prefix("sha256:")
            .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(LayerDeleteError::UnknownOutcome(anyhow::anyhow!(
                "invalid stored layer digest"
            )));
        }
        if let Err(error) = repo.delete_snapshot_layer(digest).await {
            let key = BlobKey::layer(digest.to_owned(), LayerRole::Rootfs, true);
            return Err(
                if matches!(repo.locate_blob(&key), Ok(BlobLocation::Local(_)))
                    && repo.has_blob(&key).await.unwrap_or(false)
                {
                    LayerDeleteError::StillPresent(error.into())
                } else {
                    LayerDeleteError::UnknownOutcome(error.into())
                },
            );
        }
        Ok(())
    }
}
