//! Shared documents on object storage.
//!
//! Each document is one object at [`DocumentKey::path`], which nodes on older
//! versions read and write too. Writers of one document exclude each other so
//! that no update is lost: on S3 stores, MinIO among them, an update replaces
//! a document only if its ETag is unchanged. Alibaba OSS cannot write
//! conditionally, so there every write holds the document's [`lease`].

use std::time::Duration;

use rand::RngExt;

use super::client::{OssClient, OssUploadArtifact};
use super::lease;
use crate::repository::{
    DocumentCollection, DocumentKey, DocumentUpdate, RepositoryError, RepositoryResult,
};

const MAX_UPDATE_ATTEMPTS: usize = 32;

/// Jittered, growing pauses between attempts that lost to another writer.
pub(super) struct Backoff {
    next: Duration,
}

impl Backoff {
    const FIRST: Duration = Duration::from_millis(20);
    const MAX: Duration = Duration::from_millis(500);

    pub(super) fn new() -> Self {
        Self { next: Self::FIRST }
    }

    pub(super) async fn wait(&mut self) {
        let delay = rand::rng().random_range(self.next / 2..=self.next);
        self.next = (self.next * 2).min(Self::MAX);
        tokio::time::sleep(delay).await;
    }
}

pub(super) async fn get(
    client: &OssClient,
    key: &DocumentKey,
) -> RepositoryResult<Option<Vec<u8>>> {
    read(client, &key.path()).await
}

pub(super) async fn put(
    client: &OssClient,
    key: &DocumentKey,
    bytes: Vec<u8>,
) -> RepositoryResult<()> {
    let path = key.path();
    let artifact = upload_artifact(key);
    if !client.can_append() {
        return client
            .put_bytes(&path, bytes, artifact)
            .await
            .map_err(|error| RepositoryError::backend(format!("write '{path}'"), error));
    }
    let lease = lease::acquire(client, &path).await?;
    // Any failed write may still land, so its lease runs out instead.
    client
        .put_bytes_before(&path, bytes, artifact, lease.deadline())
        .await
        .map_err(|error| RepositoryError::backend(format!("write '{path}'"), error))?;
    lease::release(client, lease).await;
    Ok(())
}

pub(super) async fn update(
    client: &OssClient,
    key: &DocumentKey,
    update: DocumentUpdate,
) -> RepositoryResult<Option<Vec<u8>>> {
    let path = key.path();
    if !client.can_append() {
        return update_if_unchanged(client, &path, update).await;
    }
    let lease = lease::acquire(client, &path).await?;
    let (current, bytes) = match read(client, &path).await {
        Ok(current) => match update(current.as_deref()) {
            Ok(Some(bytes)) => (current, bytes),
            result => {
                lease::release(client, lease).await;
                return result.map(|_| current);
            }
        },
        Err(error) => {
            lease::release(client, lease).await;
            return Err(error);
        }
    };
    client
        .put_bytes_before(&path, bytes, upload_artifact(key), lease.deadline())
        .await
        .map_err(|error| RepositoryError::backend(format!("update '{path}'"), error))?;
    lease::release(client, lease).await;
    Ok(current)
}

pub(super) async fn delete(client: &OssClient, key: &DocumentKey) -> RepositoryResult<()> {
    let path = key.path();
    if !client.can_append() {
        return client
            .delete(&path)
            .await
            .map_err(|error| RepositoryError::backend(format!("delete '{path}'"), error));
    }
    let lease = lease::acquire(client, &path).await?;
    client
        .delete_before(&path, lease.deadline())
        .await
        .map_err(|error| RepositoryError::backend(format!("delete '{path}'"), error))?;
    lease::release(client, lease).await;
    Ok(())
}

/// Lists up to `limit` document IDs after `after`, in object key order.
pub(super) async fn list(
    client: &OssClient,
    collection: DocumentCollection,
    after: Option<&str>,
    limit: usize,
) -> RepositoryResult<Vec<String>> {
    let prefix = collection.prefix();
    let start_after = after.map(|id| collection.key(id).path());
    let keys = client
        .list_keys_page(prefix, start_after.as_deref(), limit)
        .await
        .map_err(|error| RepositoryError::backend(format!("list '{prefix}'"), error))?;
    keys.iter()
        .map(|key| {
            key.strip_prefix(prefix)
                .filter(|name| !name.contains('/'))
                .and_then(|name| collection.id_from_file_name(name))
                .map(str::to_owned)
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("invalid document key '{key}'"),
                })
        })
        .collect()
}

async fn read(client: &OssClient, path: &str) -> RepositoryResult<Option<Vec<u8>>> {
    match client.get_bytes(path).await {
        Ok(bytes) => Ok(Some(bytes.to_vec())),
        Err(error) if OssClient::is_not_found_error(&error) => Ok(None),
        Err(error) => Err(RepositoryError::backend(format!("read '{path}'"), error)),
    }
}

/// Applies `update` with an ETag compare-and-swap.
async fn update_if_unchanged(
    client: &OssClient,
    path: &str,
    update: DocumentUpdate,
) -> RepositoryResult<Option<Vec<u8>>> {
    let mut backoff = Backoff::new();
    for _ in 0..MAX_UPDATE_ATTEMPTS {
        let (current, etag) = match client.get_bytes_with_etag(path).await {
            Ok((bytes, Some(etag))) => (Some(bytes.to_vec()), Some(etag)),
            Ok((_, None)) => {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("updating '{path}' requires object storage ETags"),
                })
            }
            Err(error) if OssClient::is_not_found_error(&error) => (None, None),
            Err(error) => return Err(RepositoryError::backend(format!("read '{path}'"), error)),
        };
        let Some(bytes) = update(current.as_deref())? else {
            return Ok(current);
        };
        if client
            .put_bytes_conditionally(path, bytes, etag.as_deref())
            .await
            .map_err(|error| RepositoryError::backend(format!("update '{path}'"), error))?
        {
            return Ok(current);
        }
        backoff.wait().await;
    }
    Err(RepositoryError::InvalidRequest {
        reason: format!("'{path}' changed too often during update"),
    })
}

fn upload_artifact(key: &DocumentKey) -> OssUploadArtifact {
    match key {
        DocumentKey::BuildLog(_) => OssUploadArtifact::BuildLog,
        _ => OssUploadArtifact::CatalogRecord,
    }
}
