use anyhow::{ensure, Result};
use tracing::{info, warn};

use super::BuildJournal;
use crate::{
    cfg::ConfigManager,
    template::BuildService,
    volume::{VolumeError, VolumeMode, VolumeRecord, VolumeStatus},
};

const BUILD_CACHE_VOLUME: &str = "aenv-buildkit-cache";

impl BuildService {
    pub(super) async fn fork_build_cache(&self, id: &str, work_name: &str) -> Result<VolumeRecord> {
        let size = ConfigManager::global_config().template_build.cache_size_mb;
        let source = match self.volume_manager.get(BUILD_CACHE_VOLUME).await {
            Ok(seed) if seed.mode == VolumeMode::ReadOnly && seed.size_mb == size => Some(seed.id),
            Ok(_) => {
                warn!(
                    build_id = id,
                    "cache seed is incompatible with the configured builder; starting with an empty cache"
                );
                None
            }
            Err(VolumeError::NotFound(_)) => None,
            Err(error) => {
                warn!(build_id = id, %error, "cache lookup failed; starting with an empty cache");
                None
            }
        };
        if let Some(seed_id) = source {
            match self
                .volume_manager
                .create_reserved(work_name.to_owned(), Some(seed_id.clone()), size, id)
                .await
            {
                Ok(volume) => {
                    info!(build_id = id, seed = %seed_id, cache = %volume.id, "forked shared build cache");
                    return Ok(volume);
                }
                Err(error) => {
                    warn!(build_id = id, %error, "cache seed unavailable; starting with an empty cache");
                }
            }
        }
        Ok(self
            .volume_manager
            .create_reserved(work_name.to_owned(), None, size, id)
            .await?)
    }

    pub(super) async fn publish_build_cache(&self, id: &str, cache: &str) -> Result<()> {
        let work = self.volume_manager.get(cache).await?;
        ensure!(
            work.status == VolumeStatus::Ready && work.reserved_by_sandbox_id.is_none(),
            "builder cache did not finish publication"
        );
        for _ in 0..2 {
            match self.volume_manager.get(BUILD_CACHE_VOLUME).await {
                Ok(mut existing)
                    if existing.mode == VolumeMode::ReadOnly
                        && existing.size_mb == work.size_mb =>
                {
                    self.volume_manager
                        .replace_backing_layers(&mut existing, work.backing_layers)
                        .await?;
                    info!(build_id = id, cache = %existing.id, "published shared build cache");
                    return Ok(());
                }
                Ok(existing) => match self.volume_manager.delete(&existing.id).await {
                    Ok(()) | Err(VolumeError::NotFound(_)) => {}
                    Err(error) => return Err(error.into()),
                },
                Err(VolumeError::NotFound(_)) => {}
                Err(error) => return Err(error.into()),
            }
            match self
                .volume_manager
                .create(
                    BUILD_CACHE_VOLUME.to_owned(),
                    VolumeMode::ReadOnly,
                    Some(work.id.clone()),
                    None,
                    work.size_mb,
                )
                .await
            {
                Ok(seed) => {
                    info!(build_id = id, cache = %seed.id, "published shared build cache");
                    return Ok(());
                }
                Err(VolumeError::NameConflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        anyhow::bail!("concurrent build cache creation did not converge")
    }

    pub(super) async fn cleanup_build_cache(&self, entry: &BuildJournal) -> Result<()> {
        #[cfg(test)]
        if self
            .sessions
            .fail_cache_cleanup
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("injected build cache cleanup failure");
        }
        match self.volume_manager.delete(&entry.cache).await {
            Ok(()) | Err(VolumeError::NotFound(_)) | Err(VolumeError::Reserved(_)) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
