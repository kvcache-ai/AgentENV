use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tempfile::NamedTempFile;
use tokio::task;

use super::{artifacts, documents};
use crate::repository::interfaces::Repository;
use crate::repository::{
    build_log_path, BlobBody, BlobKey, BlobLocation, BlobMetadata, LayerRole,
    LegacyDocumentCollection, LegacyDocumentKey, RepositoryError, RepositoryResult,
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

    async fn stat_blob(&self, key: &BlobKey) -> RepositoryResult<Option<BlobMetadata>> {
        let path = artifacts::blob_path(&self.root, key);
        match tokio::fs::metadata(&path).await {
            Ok(metadata) => Ok(Some(BlobMetadata {
                size: metadata.len(),
            })),
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

    async fn delete_snapshot_blobs(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let id = id.clone();
        self.run_blocking("delete snapshot files", move |root| {
            artifacts::delete_snapshot_files(root, &id)
        })
        .await
    }

    fn staging_file(&self) -> RepositoryResult<NamedTempFile> {
        artifacts::staging_file(&self.root)
    }

    async fn get_build_log(&self, id: &SnapshotId) -> RepositoryResult<Option<Vec<u8>>> {
        let rel = build_log_path(id);
        self.run_blocking("read build log", move |root| {
            documents::read_file(&root.join(rel))
        })
        .await
    }

    async fn put_build_log(&self, id: &SnapshotId, bytes: Vec<u8>) -> RepositoryResult<()> {
        let rel = build_log_path(id);
        self.run_blocking("write build log", move |root| {
            artifacts::write_atomic_bytes(&root.join(rel), &bytes)
        })
        .await
    }

    async fn delete_build_log(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let rel = build_log_path(id);
        self.run_blocking("delete build log", move |root| {
            documents::remove_file_if_exists(&root.join(rel))
        })
        .await
    }

    async fn get_legacy_document(
        &self,
        key: LegacyDocumentKey,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        self.run_blocking("read legacy document", move |root| {
            documents::read_file(&root.join(key.posix_path()))
        })
        .await
    }

    async fn put_legacy_document(
        &self,
        key: LegacyDocumentKey,
        bytes: Vec<u8>,
    ) -> RepositoryResult<()> {
        self.run_blocking("write legacy document", move |root| {
            artifacts::write_atomic_bytes(&root.join(key.posix_path()), &bytes)
        })
        .await
    }

    async fn list_legacy_documents(
        &self,
        collection: LegacyDocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        let after = after.map(str::to_owned);
        self.run_blocking("list legacy documents", move |root| {
            documents::list_legacy(root, collection, after.as_deref(), limit)
        })
        .await
    }

    async fn read_legacy_documents(
        &self,
        collection: LegacyDocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        self.run_blocking("read legacy documents", move |root| {
            documents::read_legacy(root, collection)
        })
        .await
    }
}
