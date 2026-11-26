//! Shared documents on object storage (build logs and read-only legacy
//! catalog documents scanned during migration).

use futures::{stream, StreamExt, TryStreamExt};

use super::client::{OssClient, OssUploadArtifact};
use crate::repository::{DocumentCollection, DocumentKey, RepositoryError, RepositoryResult};

const READ_CONCURRENCY: usize = 16;

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
    client
        .put_bytes(&path, bytes, upload_artifact(key))
        .await
        .map_err(|error| RepositoryError::backend(format!("write '{path}'"), error))
}

pub(super) async fn delete(client: &OssClient, key: &DocumentKey) -> RepositoryResult<()> {
    let path = key.path();
    client
        .delete(&path)
        .await
        .map_err(|error| RepositoryError::backend(format!("delete '{path}'"), error))
}

/// Lists up to `limit` document IDs after `after`, in object key order.
pub(super) async fn list(
    client: &OssClient,
    collection: DocumentCollection,
    after: Option<&str>,
    limit: usize,
) -> RepositoryResult<Vec<String>> {
    let prefix = collection.prefix();
    let start_after = after
        .map(|id| {
            collection.key(id).map(|key| key.path()).ok_or_else(|| {
                RepositoryError::InvalidRequest {
                    reason: format!("invalid document id '{id}'"),
                }
            })
        })
        .transpose()?;
    let keys = client
        .list_keys_page(prefix, start_after.as_deref(), limit)
        .await
        .map_err(|error| RepositoryError::backend(format!("list '{prefix}'"), error))?;
    keys.iter()
        .map(|key| document_id(collection, key))
        .collect()
}

/// Reads every document in `collection`, in object key order.
pub(super) async fn read_all(
    client: &OssClient,
    collection: DocumentCollection,
) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
    let prefix = collection.prefix();
    let keys = client
        .list_keys_recursive(prefix)
        .await
        .map_err(|error| RepositoryError::backend(format!("list '{prefix}'"), error))?;
    let read = stream::iter(keys)
        .map(|key| async move {
            let id = document_id(collection, &key)?;
            // Skips a document deleted since the listing.
            RepositoryResult::Ok(read(client, &key).await?.map(|bytes| (id, bytes)))
        })
        .buffered(READ_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(read.into_iter().flatten().collect())
}

async fn read(client: &OssClient, path: &str) -> RepositoryResult<Option<Vec<u8>>> {
    match client.get_bytes(path).await {
        Ok(bytes) => Ok(Some(bytes.to_vec())),
        Err(error) if OssClient::is_not_found_error(&error) => Ok(None),
        Err(error) => Err(RepositoryError::backend(format!("read '{path}'"), error)),
    }
}

/// The ID of the document stored at `key` in `collection`.
fn document_id(collection: DocumentCollection, key: &str) -> RepositoryResult<String> {
    key.strip_prefix(collection.prefix())
        .filter(|name| !name.contains('/'))
        .and_then(|name| collection.id_from_file_name(name))
        .map(str::to_owned)
        .ok_or_else(|| RepositoryError::InvalidRequest {
            reason: format!("invalid document key '{key}'"),
        })
}

fn upload_artifact(key: &DocumentKey) -> OssUploadArtifact {
    match key {
        DocumentKey::BuildLog(_) => OssUploadArtifact::BuildLog,
        DocumentKey::SnapshotAlias(_) | DocumentKey::VolumeAlias(_) => OssUploadArtifact::Alias,
        _ => OssUploadArtifact::CatalogRecord,
    }
}
