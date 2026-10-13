use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tempfile::NamedTempFile;
use tokio::task;

use super::artifacts::PosixFsArtifactStore;
use super::catalog::PosixFsCatalogStore;
use crate::repository::interfaces::Repository;
use crate::repository::{
    BlobBody, BlobKey, BlobLocation, DocumentCollection, DocumentKey, DocumentUpdate,
    RepositoryError, RepositoryResult,
};
use crate::types::SnapshotId;

#[derive(Clone)]
pub(crate) struct PosixFsRepository {
    catalog_store: Arc<PosixFsCatalogStore>,
    artifact_store: Arc<PosixFsArtifactStore>,
}

impl PosixFsRepository {
    /// A repository rooted at `root`, which may live on a shared POSIX or
    /// distributed filesystem.
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            catalog_store: Arc::new(PosixFsCatalogStore::new(root.clone())),
            artifact_store: Arc::new(PosixFsArtifactStore::new(root)),
        }
    }

    async fn run_catalog<T, F>(&self, operation: &'static str, work: F) -> RepositoryResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&PosixFsCatalogStore) -> RepositoryResult<T> + Send + 'static,
    {
        let store = Arc::clone(&self.catalog_store);
        run_repository_blocking(operation, move || work(&store)).await
    }

    async fn run_artifacts<T, F>(&self, operation: &'static str, work: F) -> RepositoryResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&PosixFsArtifactStore) -> RepositoryResult<T> + Send + 'static,
    {
        let store = Arc::clone(&self.artifact_store);
        run_repository_blocking(operation, move || work(&store)).await
    }
}

#[async_trait]
impl Repository for PosixFsRepository {
    async fn put_blob(&self, key: &BlobKey, body: BlobBody<'_>) -> RepositoryResult<()> {
        let key = key.clone();
        match body {
            BlobBody::File(source) => {
                let source = source.to_path_buf();
                self.run_artifacts("store blob", move |store| {
                    store.put_blob(&key, BlobBody::File(&source))
                })
                .await
            }
            BlobBody::Bytes(bytes) => {
                self.run_artifacts("store blob", move |store| {
                    store.put_blob(&key, BlobBody::Bytes(bytes))
                })
                .await
            }
        }
    }

    fn locate_blob(&self, key: &BlobKey) -> RepositoryResult<BlobLocation> {
        Ok(BlobLocation::Local(self.artifact_store.blob_path(key)))
    }

    async fn has_blob(&self, key: &BlobKey) -> RepositoryResult<bool> {
        let path = self.artifact_store.blob_path(key);
        tokio::fs::try_exists(&path)
            .await
            .map_err(|error| RepositoryError::backend(format!("check '{}'", path.display()), error))
    }

    async fn fetch_blob(&self, key: &BlobKey, destination: &Path) -> RepositoryResult<u64> {
        let path = self.artifact_store.blob_path(key);
        tokio::fs::copy(&path, destination).await.map_err(|error| {
            RepositoryError::backend(
                format!("copy '{}' to '{}'", path.display(), destination.display()),
                error,
            )
        })
    }

    async fn read_blob(&self, key: &BlobKey) -> RepositoryResult<Vec<u8>> {
        let path = self.artifact_store.blob_path(key);
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

    async fn delete_snapshot_blobs(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let id = id.clone();
        self.run_artifacts("delete snapshot files", move |store| {
            store.delete_snapshot_files(&id)
        })
        .await
    }

    fn staging_file(&self) -> RepositoryResult<NamedTempFile> {
        self.artifact_store.staging_file()
    }

    async fn get_document(&self, key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        self.run_catalog("read document", move |store| store.get_document(&key))
            .await
    }

    async fn put_document(&self, key: DocumentKey, bytes: Vec<u8>) -> RepositoryResult<()> {
        self.run_catalog("write document", move |store| {
            store.put_document(&key, &bytes)
        })
        .await
    }

    async fn update_document(
        &self,
        key: DocumentKey,
        update: DocumentUpdate,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        self.run_catalog("update document", move |store| {
            store.update_document(&key, update)
        })
        .await
    }

    async fn delete_document(&self, key: DocumentKey) -> RepositoryResult<()> {
        self.run_catalog("delete document", move |store| store.delete_document(&key))
            .await
    }

    async fn list_documents(
        &self,
        collection: DocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        let after = after.map(str::to_owned);
        self.run_catalog("list documents", move |store| {
            store.list_documents(collection, after.as_deref(), limit)
        })
        .await
    }

    async fn read_documents(
        &self,
        collection: DocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        self.run_catalog("read documents", move |store| {
            store.read_documents(collection)
        })
        .await
    }
}

async fn run_repository_blocking<T, F>(operation: &'static str, work: F) -> RepositoryResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> RepositoryResult<T> + Send + 'static,
{
    task::spawn_blocking(work)
        .await
        .map_err(|error| RepositoryError::Backend {
            message: format!("repository blocking task panicked while trying to {operation}"),
            source: Some(anyhow::Error::from(error)),
        })?
}
