//! Metadata service handle, GC storage wiring, error translation, and node-local GC pin tracking.

pub mod gc;
pub mod pins;

use std::sync::Arc;

pub use agentenv_metadata::{
    DatabaseConfig, GcReport, GcStorage, MetaClient, MetaError, MetaResult, MetaServer,
    SnapshotEntry, SnapshotState,
};
use anyhow::Context;

pub use gc::RepositoryGcStorage;
pub use pins::{NodePins, SnapshotPinner};

use crate::cfg::AppConfig;
use crate::repository::RepositoryError;

/// Owns the Node or operator tool's metadata connection.
pub struct MetadataHandle {
    client: MetaClient,
    embedded: Option<MetaServer>,
}

impl MetadataHandle {
    /// Opens the configured metadata service for a Node, automatically applying
    /// migrations when using an embedded SQLite database and wiring GC storage.
    pub async fn open_node(config: &AppConfig) -> anyhow::Result<Self> {
        if config.database.embedded_sqlite_path().is_some() {
            config.database.to_metadata_config()?.migrate().await?;
        }
        Self::open(config).await
    }

    /// Opens the configured metadata service, dialing `database.meta_address`
    /// when configured or starting an embedded [`MetaServer`] with GC storage.
    pub async fn open(config: &AppConfig) -> anyhow::Result<Self> {
        if let Some(address) = config.database.meta_address.as_deref() {
            let client = agentenv_metadata::transport::connect(address)
                .await
                .with_context(|| format!("connect to metadata server at '{address}'"))?;
            client
                .health()
                .await
                .context("check metadata server health")?;
            return Ok(Self {
                client,
                embedded: None,
            });
        }
        let storage = Arc::new(RepositoryGcStorage::from_config(config)?);
        let server = MetaServer::open(&config.database.to_metadata_config()?)
            .await?
            .with_gc_storage(storage);
        let client = agentenv_metadata::transport::embedded(server.clone());
        Ok(Self {
            client,
            embedded: Some(server),
        })
    }

    pub fn client(&self) -> MetaClient {
        self.client.clone()
    }

    pub async fn close(self) {
        drop(self.client);
        if let Some(server) = self.embedded {
            server.close().await;
        }
    }
}

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
pub async fn commit_with_layer_restore<T, E, F, Fut>(
    repository: &dyn crate::repository::Repository,
    layers: &[&agentenv_domain::ManagedLayerRef],
    local_layers: &[(&str, &std::path::Path)],
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
            crate::repository::restore_missing_managed_layers(
                repository,
                layers,
                &uuids,
                local_layers,
            )
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
