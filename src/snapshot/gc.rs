//! Storage operations used by the metadata service's collector.

use agentenv_domain::SnapshotId;
use agentenv_metadata::GcStorage;
use async_trait::async_trait;
use std::sync::Arc;

use crate::repository::Repository;

pub struct SnapshotGcStorage(pub Arc<dyn Repository>);

impl SnapshotGcStorage {
    pub fn from_config(config: &crate::cfg::AppConfig) -> anyhow::Result<Self> {
        Ok(Self(crate::repository::open_from_config(config)?))
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
        Ok(super::store::delete_snapshot(repo, id, &publications).await?)
    }

    async fn delete_layer(&self, digest: &str) -> anyhow::Result<()> {
        validate_layer_digest(digest)?;
        self.0.delete_managed_layer(digest).await?;
        Ok(())
    }

    async fn delete_layers(&self, digests: &[String]) -> Vec<anyhow::Result<()>> {
        let mut results: Vec<Option<anyhow::Result<()>>> = Vec::with_capacity(digests.len());
        let mut valid_indices = Vec::new();
        let mut valid_digests = Vec::new();
        for (idx, digest) in digests.iter().enumerate() {
            match validate_layer_digest(digest) {
                Ok(()) => {
                    results.push(None);
                    valid_indices.push(idx);
                    valid_digests.push(digest.clone());
                }
                Err(error) => results.push(Some(Err(error))),
            }
        }
        if !valid_digests.is_empty() {
            let outcomes = self.0.delete_managed_layers(&valid_digests).await;
            for (idx, outcome) in valid_indices.into_iter().zip(outcomes) {
                results[idx] = Some(outcome.map_err(anyhow::Error::from));
            }
        }
        results
            .into_iter()
            .map(|item| item.expect("every digest has an outcome"))
            .collect()
    }
}

fn validate_layer_digest(digest: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        digest
            .strip_prefix("sha256:")
            .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())),
        "invalid stored layer digest"
    );
    Ok(())
}
