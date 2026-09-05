use anyhow::{ensure, Result};
use tracing::{info, warn};

use super::{ApiImpl, BuildJournal};
use crate::{
    cfg::ConfigManager,
    volume::{VolumeError, VolumeMode, VolumeRecord, VolumeStatus},
};

fn seed_name(build_id: &str) -> String {
    format!("aenv-buildkit-seed-{build_id}")
}

impl ApiImpl {
    pub(super) async fn fork_build_cache(
        &self,
        id: &str,
        entry: &mut BuildJournal,
    ) -> Result<VolumeRecord> {
        let size = ConfigManager::global_config().template_build.cache_size_mb;
        let journal = self.build_journal().await?;
        let seed = match self
            .snapshot_manager
            .repository()
            .get_build_cache_head()
            .await
        {
            Ok(seed) => seed,
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
            .create(
                seed_name(id),
                VolumeMode::ReadOnly,
                Some(volume.id),
                None,
                volume.size_mb,
            )
            .await?;
        let repository = self.snapshot_manager.repository();
        let previous = repository.replace_build_cache_head(&seed.id).await?;
        info!(build_id = id, cache = %seed.id, "published shared build cache seed");
        if let Some(previous) = previous {
            self.build_journal()
                .await?
                .put(format!("gc/{previous}").into_bytes(), Vec::new())
                .await?;
        }
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
        let journal = self.build_journal().await?;
        for (key, _) in journal.scan_prefix(b"gc/".to_vec()).await? {
            if self
                .collect_build_cache(std::str::from_utf8(&key[3..])?)
                .await?
            {
                journal.delete(key).await?;
            }
        }
        Ok(())
    }

    async fn collect_build_cache(&self, reference: &str) -> Result<bool> {
        let volume = match self.volume_manager.get(reference).await {
            Ok(volume) => volume,
            Err(VolumeError::NotFound(_)) => return Ok(true),
            Err(error) => return Err(error.into()),
        };
        if self
            .snapshot_manager
            .repository()
            .get_build_cache_head()
            .await?
            .as_deref()
            == Some(&volume.id)
        {
            return Ok(false);
        }
        match self.volume_manager.delete(&volume.id).await {
            Ok(()) | Err(VolumeError::NotFound(_)) => Ok(true),
            Err(VolumeError::Reserved(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}
