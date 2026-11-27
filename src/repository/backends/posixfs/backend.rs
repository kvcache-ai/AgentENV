use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::task;

use super::artifacts;
use crate::repository::interfaces::Repository;
use crate::repository::{
    BlobBody, BlobKey, BlobLocation, LayerRole, RepositoryError, RepositoryResult,
};
use crate::types::SnapshotId;

#[derive(Clone)]
pub(crate) struct PosixFsRepository {
    root: Arc<PathBuf>,
}

impl PosixFsRepository {
    /// A repository rooted at `root`, which may live on a shared POSIX or
    /// distributed filesystem.
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root: Arc::new(root),
        }
    }

    async fn run_blocking<T, F>(&self, operation: &'static str, work: F) -> RepositoryResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Path) -> RepositoryResult<T> + Send + 'static,
    {
        let root = Arc::clone(&self.root);
        task::spawn_blocking(move || work(&root))
            .await
            .map_err(|error| RepositoryError::Backend {
                message: format!("repository blocking task panicked while trying to {operation}"),
                source: Some(anyhow::Error::from(error)),
            })?
    }
}

#[async_trait]
impl Repository for PosixFsRepository {
    async fn put_blob(&self, key: &BlobKey, body: BlobBody<'_>) -> RepositoryResult<()> {
        let key = key.clone();
        match body {
            BlobBody::File(source) => {
                let source = source.to_path_buf();
                self.run_blocking("store blob", move |root| {
                    artifacts::put_blob(root, &key, BlobBody::File(&source))
                })
                .await
            }
            BlobBody::Bytes(bytes) => {
                self.run_blocking("store blob", move |root| {
                    artifacts::put_blob(root, &key, BlobBody::Bytes(bytes))
                })
                .await
            }
        }
    }

    fn locate_blob(&self, key: &BlobKey) -> RepositoryResult<BlobLocation> {
        Ok(BlobLocation::Local(artifacts::blob_path(&self.root, key)))
    }

    async fn has_blob(&self, key: &BlobKey) -> RepositoryResult<bool> {
        let path = artifacts::blob_path(&self.root, key);
        tokio::fs::try_exists(&path)
            .await
            .map_err(|error| RepositoryError::backend(format!("check '{}'", path.display()), error))
    }

    async fn stat_blob(&self, key: &BlobKey) -> RepositoryResult<Option<u64>> {
        let path = artifacts::blob_path(&self.root, key);
        match tokio::fs::metadata(&path).await {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(RepositoryError::backend(
                format!("stat '{}'", path.display()),
                error,
            )),
        }
    }

    async fn fetch_blob(&self, key: &BlobKey, destination: &Path) -> RepositoryResult<u64> {
        let path = artifacts::blob_path(&self.root, key);
        match tokio::fs::copy(&path, destination).await {
            Ok(size) => Ok(size),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(RepositoryError::ArtifactNotFound {
                    artifact: format!("'{}'", path.display()),
                })
            }
            Err(error) => Err(RepositoryError::backend(
                format!("copy '{}' to '{}'", path.display(), destination.display()),
                error,
            )),
        }
    }

    async fn read_blob(&self, key: &BlobKey) -> RepositoryResult<Vec<u8>> {
        let path = artifacts::blob_path(&self.root, key);
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(RepositoryError::ArtifactNotFound {
                    artifact: format!("'{}'", path.display()),
                })
            }
            Err(error) => Err(RepositoryError::backend(
                format!("read '{}'", path.display()),
                error,
            )),
        }
    }

    async fn delete_managed_layers(&self, digests: &[String]) -> Vec<RepositoryResult<()>> {
        let mut results = Vec::with_capacity(digests.len());
        for digest in digests {
            let path = artifacts::blob_path(
                &self.root,
                &BlobKey::Layer {
                    digest: digest.clone(),
                    role: LayerRole::Rootfs,
                },
            );
            results.push(match tokio::fs::remove_file(&path).await {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(RepositoryError::backend(
                    format!("delete '{}'", path.display()),
                    error,
                )),
            });
        }
        results
    }

    async fn delete_blob(&self, key: &BlobKey) -> RepositoryResult<()> {
        let key = key.clone();
        self.run_blocking("delete blob", move |root| {
            artifacts::remove_file_if_exists(&artifacts::blob_path(root, &key))
        })
        .await
    }

    async fn delete_snapshot_blobs(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let id = id.clone();
        self.run_blocking("delete snapshot files", move |root| {
            artifacts::delete_snapshot_files(root, &id)
        })
        .await
    }

    fn staging_dir(&self) -> Option<PathBuf> {
        Some(self.root.join(BlobKey::POSIX_STAGING_DIR))
    }
}
