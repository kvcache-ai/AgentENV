//! Metadata service handle, GC storage wiring, error translation, and node-local GC pin tracking.

pub mod gc;
pub mod pins;

use std::sync::Arc;

pub use agentenv_metadata::{
    context::current as meta_context, DatabaseConfig, GcReport, GcStorage, MetaClient, MetaError,
    MetaResult, MetaServer, RpcError, SnapshotEntry, SnapshotState,
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
        Self::open_node_with_gc_storage(config, || {
            Ok(Arc::new(RepositoryGcStorage::from_config(config)?))
        })
        .await
    }

    /// Opens the configured metadata service for operator CLIs with GC storage.
    pub async fn open(config: &AppConfig) -> anyhow::Result<Self> {
        Self::open_with_gc_storage(config, || {
            Ok(Arc::new(RepositoryGcStorage::from_config(config)?))
        })
        .await
    }

    /// Opens the configured metadata service for a Node with a custom GC storage factory.
    async fn open_node_with_gc_storage(
        config: &AppConfig,
        storage_factory: impl FnOnce() -> anyhow::Result<Arc<dyn GcStorage>>,
    ) -> anyhow::Result<Self> {
        if config.database.embedded_sqlite_path().is_some() {
            config.database.to_metadata_config()?.migrate().await?;
        }
        Self::open_with_gc_storage(config, storage_factory).await
    }

    /// Opens the configured metadata service, dialing `database.meta_address`
    /// when configured or starting an embedded [`MetaServer`] with GC storage.
    async fn open_with_gc_storage(
        config: &AppConfig,
        storage_factory: impl FnOnce() -> anyhow::Result<Arc<dyn GcStorage>>,
    ) -> anyhow::Result<Self> {
        if let Some(address) = config.database.meta_address.as_deref() {
            let client = agentenv_metadata::transport::connect(address)
                .await
                .with_context(|| format!("connect to metadata server at '{address}'"))?;
            client
                .health(meta_context())
                .await
                .context("check metadata server health")??;
            return Ok(Self {
                client,
                embedded: None,
            });
        }
        let storage = storage_factory()?;
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
        MetaError::VolumeNotFound { lookup } => RepositoryError::VolumeNotFound { lookup },
        MetaError::VolumeNameConflict { name } => RepositoryError::VolumeNameConflict { name },
        reserved @ MetaError::VolumeReserved { .. } => RepositoryError::InvalidRequest {
            reason: reserved.to_string(),
        },
        MetaError::BuildNotFound { id } => RepositoryError::SnapshotNotFound {
            lookup: id.to_string(),
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

pub fn map_rpc_error(operation: &str, error: RpcError) -> RepositoryError {
    RepositoryError::backend(operation, error)
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
