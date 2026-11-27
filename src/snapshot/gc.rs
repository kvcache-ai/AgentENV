//! Physical GC storage adapter for snapshot artifacts and shared managed layers.

use std::sync::Arc;

use agentenv_domain::{is_sha256_digest, SnapshotId};
use agentenv_metadata::{GcStorage, MetaClient, MetaServer};
use anyhow::Context;
use async_trait::async_trait;

use super::content::{delete_snapshot_artifacts, read_snapshot_content};
use crate::cfg::AppConfig;
use crate::repository::Repository;

/// Opens the configured metadata service for a Node, applying embedded SQLite
/// migrations when needed and wiring [`SnapshotGcStorage`] for embedded servers.
pub async fn open_node_metadata(config: &AppConfig) -> anyhow::Result<MetaClient> {
    if config.database.embedded_sqlite_path().is_some() {
        config.database.to_metadata_config()?.migrate().await?;
    }
    open_metadata(config).await
}

/// Opens the configured metadata service, dialing `database.meta_address` when
/// set or starting an embedded [`MetaServer`] with [`SnapshotGcStorage`].
pub async fn open_metadata(config: &AppConfig) -> anyhow::Result<MetaClient> {
    if let Some(address) = config.database.meta_address.as_deref() {
        let client = agentenv_metadata::transport::connect(address)
            .await
            .with_context(|| format!("connect to metadata server at '{address}'"))?;
        client
            .health()
            .await
            .context("check metadata server health")?;
        return Ok(client);
    }
    let storage = SnapshotGcStorage::open(config)?;
    let server = MetaServer::open(&config.database.to_metadata_config()?)
        .await?
        .with_gc_storage(storage);
    Ok(agentenv_metadata::transport::embedded(server))
}

/// Adapts a durable [`Repository`] for physical snapshot and managed-layer GC.
pub struct SnapshotGcStorage(pub Arc<dyn Repository>);

impl SnapshotGcStorage {
    pub fn open(config: &AppConfig) -> anyhow::Result<Arc<dyn GcStorage>> {
        Ok(Arc::new(Self(crate::repository::open_from_config(config)?)))
    }
}

#[async_trait]
impl GcStorage for SnapshotGcStorage {
    async fn delete_snapshot(&self, id: &SnapshotId) -> anyhow::Result<()> {
        let repo = self.0.as_ref();
        let publications = read_snapshot_content(repo, id)
            .await?
            .and_then(|(_, manifest)| manifest)
            .map(|manifest| manifest.disk_publications)
            .unwrap_or_default();
        Ok(delete_snapshot_artifacts(repo, id, &publications).await?)
    }

    async fn delete_layers(&self, digests: &[String]) -> Vec<anyhow::Result<()>> {
        let (valid_indices, valid_digests): (Vec<_>, Vec<_>) = digests
            .iter()
            .enumerate()
            .filter(|(_, digest)| is_sha256_digest(digest))
            .map(|(idx, digest)| (idx, digest.clone()))
            .unzip();
        let mut results: Vec<anyhow::Result<()>> = (0..digests.len())
            .map(|_| Err(anyhow::anyhow!("invalid stored layer digest")))
            .collect();
        if !valid_digests.is_empty() {
            for (idx, outcome) in valid_indices
                .into_iter()
                .zip(self.0.delete_managed_layers(&valid_digests).await)
            {
                results[idx] = outcome.map_err(anyhow::Error::from);
            }
        }
        results
    }
}
