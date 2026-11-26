use anyhow::{ensure, Context, Result};
use tracing::{info, warn};

use super::{ApiImpl, BuildJournal};
use crate::{
    cfg::ConfigManager,
    volume::{
        VolumeError, VolumeListFilter, VolumeMode, VolumePurpose, VolumeRecord, VolumeStatus,
    },
};

fn seed_name(build_id: &str) -> String {
    format!("aenv-buildkit-seed-{build_id}")
}

impl ApiImpl {
    async fn build_cache_seeds(&self) -> Result<(Option<String>, Vec<String>)> {
        let page = self
            .meta()
            .list_volumes(VolumeListFilter::build_cache_seeds())
            .await
            .context("read build cache state from metadata service")?;
        let mut current = None;
        let mut retired = Vec::new();
        for record in page.records {
            match record.purpose {
                VolumePurpose::BuildCacheSeed => current = Some(record.id),
                VolumePurpose::BuildCacheRetired => retired.push(record.id),
                _ => {}
            }
        }
        Ok((current, retired))
    }

    pub(super) async fn fork_build_cache(
        &self,
        id: &str,
        entry: &mut BuildJournal,
    ) -> Result<VolumeRecord> {
        let size = ConfigManager::global_config().template_build.cache_size_mb;
        let journal = self.build_journal().await?;
        let seed = match self.build_cache_seeds().await {
            Ok((current, _)) => current,
            Err(error) => {
                warn!(build_id = id, %error, "cache lookup failed; starting with an empty cache");
                None
            }
        };
        if let Some(seed) = seed {
            entry.parent = Some(seed.clone());
            entry.persist(journal, id).await?;
            let fork = async {
                let parent = self.volume_manager.get(&seed).await?;
                ensure!(
                    parent.mode == VolumeMode::ReadOnly && parent.size_mb == size,
                    "cache seed is incompatible with the configured builder"
                );
                self.volume_manager.reserve(&seed, id).await?;
                self.volume_manager
                    .create_build_cache(entry.cache.clone(), Some(seed.clone()), size, id)
                    .await
                    .map_err(anyhow::Error::from)
            }
            .await;
            match fork {
                Ok(volume) => {
                    info!(build_id = id, seed = %seed, cache = %volume.id, "forked shared build cache");
                    return Ok(volume);
                }
                Err(error) => {
                    warn!(build_id = id, %error, "cache seed unavailable; starting with an empty cache")
                }
            }
            self.release_cache_lease(id, &seed).await?;
            entry.parent = None;
            entry.persist(journal, id).await?;
        }
        Ok(self
            .volume_manager
            .create_build_cache(entry.cache.clone(), None, size, id)
            .await?)
    }

    pub(super) async fn publish_build_cache(&self, id: &str, cache: &str) -> Result<()> {
        let volume = self.volume_manager.get(cache).await?;
        ensure!(
            volume.status == VolumeStatus::Ready && volume.reserved_by_sandbox_id.is_none(),
            "builder cache did not finish publication"
        );
        let seed = self
            .volume_manager
            .create_build_cache_seed(seed_name(id), volume.id, volume.size_mb)
            .await?;
        info!(build_id = id, cache = %seed.id, "published shared build cache seed");
        Ok(())
    }

    pub(super) async fn release_cache_lease(&self, owner: &str, reference: &str) -> Result<()> {
        match self.volume_manager.get(reference).await {
            Ok(volume) => {
                self.volume_manager
                    .replace_owner_for(owner, None, &[volume.id])
                    .await?
            }
            Err(VolumeError::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub(super) async fn cleanup_build_cache(&self, id: &str, entry: &BuildJournal) -> Result<()> {
        // Children retain a seed lease until their writes are published or discarded.
        if let Some(parent) = &entry.parent {
            self.release_cache_lease(id, parent).await?;
            self.collect_build_cache(parent).await?;
        }
        self.collect_build_cache(&seed_name(id)).await?;
        self.collect_build_cache(&entry.cache).await?;
        Ok(())
    }

    pub(super) async fn collect_retired_build_caches(&self) -> Result<()> {
        let (_, retired) = self.build_cache_seeds().await?;
        for id in retired {
            if let Err(error) = self.collect_build_cache(&id).await {
                warn!(cache = %id, %error, "cache retirement failed; will retry");
            }
        }
        Ok(())
    }

    async fn collect_build_cache(&self, reference: &str) -> Result<bool> {
        #[cfg(test)]
        if self
            .build_sessions
            .fail_cache_cleanup
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("injected build cache cleanup failure");
        }
        let volume = match self.volume_manager.get(reference).await {
            Ok(volume) => volume,
            Err(VolumeError::NotFound(_)) => return Ok(true),
            Err(error) => return Err(error.into()),
        };
        if volume.purpose == VolumePurpose::BuildCacheSeed {
            return Ok(false);
        }
        match self.volume_manager.delete(&volume.id).await {
            Ok(()) | Err(VolumeError::NotFound(_)) => Ok(true),
            Err(VolumeError::Reserved(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}
