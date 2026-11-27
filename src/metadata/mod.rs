//! Metadata error translation and node-local GC pin tracking.

pub mod pins;

pub use agentenv_metadata::{
    DatabaseConfig, GcReport, GcStorage, MetaClient, MetaError, MetaResult, MetaServer,
    SnapshotEntry, SnapshotState,
};

pub use pins::{NodePins, SnapshotPinner};

use crate::repository::RepositoryError;

pub fn map_meta_error(error: MetaError) -> RepositoryError {
    match error {
        MetaError::InvalidRequest { reason } => RepositoryError::InvalidRequest { reason },
        conflict @ (MetaError::SnapshotConflict { .. } | MetaError::BuildConflict { .. }) => {
            RepositoryError::InvalidRequest {
                reason: conflict.to_string(),
            }
        }
        MetaError::AliasConflict {
            alias,
            existing,
            new_id,
        } => RepositoryError::AliasConflict {
            alias,
            existing,
            new_id,
        },
        error @ (MetaError::VolumeNotFound { .. }
        | MetaError::VolumeNameConflict { .. }
        | MetaError::VolumeReserved { .. }) => RepositoryError::InvalidRequest {
            reason: error.to_string(),
        },
        error @ MetaError::MissingLayers { .. } => RepositoryError::Unavailable {
            reason: error.to_string(),
        },
        MetaError::Unavailable { reason } => RepositoryError::Unavailable { reason },
        MetaError::Backend { message } => RepositoryError::Backend {
            message,
            source: None,
        },
    }
}

/// Runs a metadata commit that attaches managed layers. If GC reclaimed any
/// referenced layers before the commit transaction ran, restores them from the
/// local uploads and retries the commit once with the restored layer UUIDs.
pub(crate) async fn commit_with_layer_restore<T, E, F, Fut>(
    repository: &dyn crate::repository::Repository,
    layers: &[&agentenv_domain::ManagedLayerRef],
    uploads: &[crate::layer::PreparedLayerUpload],
    map_meta: impl Fn(MetaError) -> E,
    map_repo: impl Fn(RepositoryError) -> E,
    mut commit: F,
) -> Result<T, E>
where
    F: FnMut(Vec<String>) -> Fut,
    Fut: std::future::Future<Output = MetaResult<T>>,
{
    match commit(Vec::new()).await {
        Ok(value) => Ok(value),
        Err(MetaError::MissingLayers { uuids }) => {
            crate::repository::restore_missing_managed_layers(repository, layers, &uuids, uploads)
                .await
                .map_err(map_repo)?;
            commit(uuids).await.map_err(map_meta)
        }
        Err(error) => Err(map_meta(error)),
    }
}

#[cfg(test)]
pub(crate) async fn test_meta(root: &std::path::Path) -> (MetaServer, MetaClient) {
    let db = DatabaseConfig::Sqlite {
        path: root.join("metadata.sqlite3"),
    };
    db.migrate().await.expect("migrate test metadata db");
    let server = MetaServer::open(&db)
        .await
        .expect("open test metadata server");
    let client = agentenv_metadata::transport::embedded(server.clone());
    (server, client)
}
