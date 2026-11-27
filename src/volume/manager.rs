use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use agentenv_domain::{
    is_valid_volume_component, VolumeLeaseOp, VolumeMode, VolumePurpose, VolumeRecord, VolumeStatus,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::backing::LocalVolumeBackingStore;
use super::error::VolumeError;
use crate::layer::{LayerRef, PreparedLayerUpload};
use crate::metadata::{commit_with_layer_restore, MetaClient, MetaError};
use crate::repository::{Repository, RepositoryError};

pub const DEFAULT_VOLUME_SIZE_MB: u64 = 64 * 1024;
pub const DEFAULT_VOLUME_PAGE_SIZE: usize = 100;

/// Mutually exclusive data source for creating a new volume.
enum VolumeSource<'a> {
    Empty,
    Clone {
        reference: String,
        expected_owner: Option<&'a str>,
    },
    ImageConfig(PathBuf),
}

struct CreateVolumeSpec<'a> {
    name: String,
    purpose: VolumePurpose,
    mode: VolumeMode,
    source: VolumeSource<'a>,
    size_mb: u64,
    reserved_owner: Option<&'a str>,
}

#[derive(Clone, Copy, Debug)]
pub struct VolumeLimits {
    pub max_size_mb: u64,
    pub max_mounts: usize,
}

impl Default for VolumeLimits {
    fn default() -> Self {
        let config = crate::cfg::VolumeConfig::default();
        Self {
            max_size_mb: config.max_size_mb,
            max_mounts: config.max_volume_count,
        }
    }
}

#[derive(Clone)]
pub struct VolumeManager {
    backings: LocalVolumeBackingStore,
    repository: Arc<dyn Repository>,
    meta: MetaClient,
    limits: VolumeLimits,
    #[cfg(test)]
    fail_catalog: Arc<AtomicBool>,
}

impl VolumeManager {
    pub async fn open_with_metadata(
        volumes_root: impl Into<PathBuf>,
        repository: Arc<dyn Repository>,
        meta: MetaClient,
    ) -> anyhow::Result<Self> {
        Self::open_with_metadata_and_limits(volumes_root, repository, meta, VolumeLimits::default())
            .await
    }

    pub async fn open_with_metadata_and_limits(
        volumes_root: impl Into<PathBuf>,
        repository: Arc<dyn Repository>,
        meta: MetaClient,
        mut limits: VolumeLimits,
    ) -> anyhow::Result<Self> {
        limits.max_mounts = limits.max_mounts.min(crate::cfg::MAX_VOLUME_MOUNTS);
        Ok(Self {
            backings: LocalVolumeBackingStore::new(volumes_root.into()),
            repository,
            meta,
            limits,
            #[cfg(test)]
            fail_catalog: Arc::new(AtomicBool::new(false)),
        })
    }

    #[cfg(test)]
    pub(crate) fn set_fail_catalog_for_test(&self, fail: bool) {
        self.fail_catalog.store(fail, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(super) fn track_creating_backing(
        &self,
        volume_id: &str,
    ) -> super::backing::CreatingBackingGuard<'_> {
        self.backings.track_creating(volume_id)
    }

    /// Removes node-local backing directories under `<volumes>/data/<id>` whose
    /// volume records no longer exist in shared metadata (e.g. deleted from
    /// another node). Fails closed on metadata errors and skips in-flight
    /// creations/materializations.
    pub async fn reclaim_orphaned_local_backings(&self) -> anyhow::Result<usize> {
        let data_root = self.backings.data_root();
        let mut entries = match tokio::fs::read_dir(&data_root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error.into()),
        };
        let mut reclaimed = 0usize;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = match entry.file_type().await {
                Ok(file_type) => file_type,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if !file_type.is_dir() {
                continue;
            }
            let Some(volume_id) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !volume_id.starts_with("vol_") || !is_valid_volume_component(&volume_id) {
                continue;
            }
            if self.backings.is_creating(&volume_id) {
                continue;
            }
            match self.meta.get_volume(volume_id.clone()).await {
                Ok(Some(_)) => continue,
                Err(error) => {
                    warn!(
                        %volume_id,
                        %error,
                        "skipping orphaned volume backing check due to metadata error"
                    );
                    continue;
                }
                Ok(None) => {}
            }
            if self.backings.is_creating(&volume_id) {
                continue;
            }
            self.backings.evict_cached(&volume_id).await;
            let path = entry.path();
            match tokio::fs::remove_dir_all(&path).await {
                Ok(()) => {
                    reclaimed += 1;
                    info!(
                        %volume_id,
                        path = %path.display(),
                        "reclaimed orphaned node-local volume backing"
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    warn!(
                        %volume_id,
                        path = %path.display(),
                        %error,
                        "failed to remove orphaned node-local volume backing"
                    );
                }
            }
        }
        Ok(reclaimed)
    }

    /// Periodically reclaims node-local backing directories for volumes deleted
    /// by peer nodes.
    pub fn start_local_backing_reclaimer(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if let Err(error) = manager.reclaim_orphaned_local_backings().await {
                    warn!(
                        error = %format_args!("{error:#}"),
                        "periodic orphaned volume backing cleanup failed"
                    );
                }
            }
        })
    }

    fn check_catalog_fault(&self) -> Result<(), VolumeError> {
        #[cfg(test)]
        if self.fail_catalog.load(Ordering::SeqCst) {
            return Err(VolumeError::Storage(
                "injected volume catalog failure".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn limits(&self) -> VolumeLimits {
        self.limits
    }

    pub async fn get(&self, reference: &str) -> Result<VolumeRecord, VolumeError> {
        let record = self
            .meta
            .get_volume(reference.to_owned())
            .await
            .map_err(meta_error)?
            .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))?;
        validate_volume_id(&record.id)?;
        Ok(record)
    }

    pub async fn list_page(
        &self,
        next_token: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<VolumeRecord>, Option<String>), VolumeError> {
        if limit == 0 {
            return Err(VolumeError::InvalidPageLimit);
        }
        let after_volume_id = next_token.map(decode_volume_cursor).transpose()?;
        let limit = u32::try_from(limit).unwrap_or(u32::MAX);
        let (records, next_volume_id) = self
            .meta
            .list_volumes(vec![VolumePurpose::User], after_volume_id, limit)
            .await
            .map_err(meta_error)?;
        for record in &records {
            validate_volume_id(&record.id)?;
        }
        Ok((records, next_volume_id.map(encode_volume_cursor)))
    }

    pub fn data_dir(&self, volume_id: &str) -> PathBuf {
        self.backings.data_dir(volume_id)
    }

    pub async fn local_backing(&self, record: &VolumeRecord) -> Option<PathBuf> {
        self.backings.local_backing(record).await
    }

    pub async fn materialize_backing(
        &self,
        reference: &str,
    ) -> Result<Option<PathBuf>, VolumeError> {
        let record = self.get(reference).await?;
        self.backings
            .materialize(self.repository.as_ref(), &record)
            .await
    }

    pub async fn create(
        &self,
        name: String,
        mode: VolumeMode,
        from_volume: Option<String>,
        source_config: Option<PathBuf>,
        size_mb: u64,
    ) -> Result<VolumeRecord, VolumeError> {
        let source = match (from_volume, source_config) {
            (Some(_), Some(_)) => return Err(VolumeError::MultipleSources),
            (Some(reference), None) => VolumeSource::Clone {
                reference,
                expected_owner: None,
            },
            (None, Some(path)) => VolumeSource::ImageConfig(path),
            (None, None) => VolumeSource::Empty,
        };
        self.create_volume(CreateVolumeSpec {
            name,
            purpose: VolumePurpose::User,
            mode,
            source,
            size_mb,
            reserved_owner: None,
        })
        .await
    }

    /// Creates a private writable child without publishing its unchanged backing first.
    /// The caller holds a read lease on the immutable seed until this child is released.
    pub(crate) async fn create_build_cache(
        &self,
        name: String,
        seed: Option<String>,
        size_mb: u64,
        owner: &str,
    ) -> Result<VolumeRecord, VolumeError> {
        let source = match seed {
            Some(reference) => VolumeSource::Clone {
                reference,
                expected_owner: None,
            },
            None => VolumeSource::Empty,
        };
        self.create_volume(CreateVolumeSpec {
            name,
            purpose: VolumePurpose::BuildCacheWork,
            mode: VolumeMode::Exclusive,
            source,
            size_mb,
            reserved_owner: Some(owner),
        })
        .await
    }

    /// Creates an internal read-only build cache seed from a published work
    /// cache and atomically promotes it to the active seed in metadata.
    pub(crate) async fn create_build_cache_seed(
        &self,
        name: String,
        source_volume_id: String,
        size_mb: u64,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_volume(CreateVolumeSpec {
            name,
            purpose: VolumePurpose::BuildCacheSeed,
            mode: VolumeMode::ReadOnly,
            source: VolumeSource::Clone {
                reference: source_volume_id,
                expected_owner: None,
            },
            size_mb,
            reserved_owner: None,
        })
        .await
    }

    async fn validate_new_volume(&self, name: &str, size_mb: u64) -> Result<(), VolumeError> {
        validate_name(name)?;
        if size_mb == 0 {
            return Err(VolumeError::InvalidSize);
        }
        if size_mb > self.limits.max_size_mb {
            return Err(VolumeError::SizeLimitExceeded {
                max_size_mb: self.limits.max_size_mb,
            });
        }
        match self.get(name).await {
            Ok(_) => Err(VolumeError::NameConflict(name.to_owned())),
            Err(VolumeError::NotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn create_volume(&self, spec: CreateVolumeSpec<'_>) -> Result<VolumeRecord, VolumeError> {
        let CreateVolumeSpec {
            name,
            purpose,
            mode,
            source,
            size_mb,
            reserved_owner,
        } = spec;
        self.validate_new_volume(&name, size_mb).await?;

        let id = format!("vol_{}", Uuid::now_v7().simple());
        let _creating_guard = self.backings.track_creating(&id);
        debug!(volume_id = %id, mode = ?mode, size_mb, "creating volume");
        let mut backing_layers = Vec::new();
        let mut cloned_from_id = None;
        let backing_image_config = match source {
            VolumeSource::Clone {
                reference,
                expected_owner,
            } => {
                let parent = match self.get(&reference).await {
                    Ok(parent) => parent,
                    Err(VolumeError::NotFound(_)) => {
                        return Err(VolumeError::SourceNotFound(reference));
                    }
                    Err(error) => return Err(error),
                };
                match parent.status {
                    VolumeStatus::Uploading => {
                        return Err(VolumeError::Uploading(parent.id.clone()))
                    }
                    VolumeStatus::Failed => return Err(VolumeError::Failed(parent.id.clone())),
                    VolumeStatus::Ready => {}
                }
                if let Some(owner) = parent.reserved_by_sandbox_id.as_deref() {
                    if expected_owner != Some(owner) {
                        return Err(VolumeError::Reserved(owner.to_owned()));
                    }
                }
                if parent.size_mb != size_mb {
                    return Err(VolumeError::SizeMismatch);
                }
                if !parent.backing_layers.is_empty() {
                    cloned_from_id = Some(parent.id.clone());
                    backing_layers = parent.backing_layers.clone();
                }
                if parent.reserved_by_sandbox_id.is_none() && !parent.backing_layers.is_empty() {
                    // Committed layers are immutable and repository-owned. Materializing
                    // the child on mount preserves their descriptors without copying or
                    // republishing the parent's local files. Reserved parents may have
                    // newer local writes, so they still need the local clone below.
                    None
                } else {
                    let parent_config = match self.backings.local_backing(&parent).await {
                        Some(path) => Some(path),
                        None if !parent.backing_layers.is_empty() => {
                            self.backings
                                .materialize(self.repository.as_ref(), &parent)
                                .await?
                        }
                        None => None,
                    };
                    match parent_config.as_ref() {
                        Some(path) => Some(self.backings.create_child(&id, path).await?),
                        None => None,
                    }
                }
            }
            VolumeSource::ImageConfig(source_config) => {
                Some(self.backings.create_child(&id, &source_config).await?)
            }
            VolumeSource::Empty => Some(self.backings.create_empty(&id, size_mb).await?),
        };

        let mut record = VolumeRecord {
            id,
            name,
            purpose,
            mode,
            size_mb,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: reserved_owner.map(str::to_owned),
            backing_layers,
            read_only_mounts: Vec::new(),
        };
        let create_result = async {
            let uploads = if reserved_owner.is_none() {
                self.publish_backing(&mut record, backing_image_config.as_deref())
                    .await?
            } else {
                Vec::new()
            };
            self.create_volume_in_meta(record.clone(), cloned_from_id, &uploads)
                .await?;
            if let Some(path) = backing_image_config.clone() {
                self.backings.cache(&record, path).await;
            }
            Ok(())
        }
        .await;
        if let Err(error) = create_result {
            if let Some(config) = backing_image_config.as_ref() {
                if let Some(directory) = config.parent() {
                    let _ = tokio::fs::remove_dir_all(directory).await;
                }
            }
            return Err(error);
        }
        info!(
            volume_id = %record.id,
            mode = ?record.mode,
            size_mb = record.size_mb,
            "volume created"
        );
        Ok(record)
    }

    pub async fn delete(&self, reference: &str) -> Result<(), VolumeError> {
        let record = self.get(reference).await?;
        debug!(volume_id = %record.id, "deleting volume");
        self.meta
            .delete_volume(record.id.clone())
            .await
            .map_err(meta_error)?;
        self.backings.remove_and_clean(&record.id).await;
        info!(volume_id = %record.id, "volume deleted");
        Ok(())
    }

    pub(crate) async fn create_child_for_owner(
        &self,
        reference: &str,
        name: String,
        mode: VolumeMode,
        size_mb: u64,
        source_owner: &str,
        child_owner: &str,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_volume(CreateVolumeSpec {
            name,
            purpose: VolumePurpose::User,
            mode,
            source: VolumeSource::Clone {
                reference: reference.to_owned(),
                expected_owner: Some(source_owner),
            },
            size_mb,
            reserved_owner: Some(child_owner),
        })
        .await
    }

    pub(crate) async fn create_from_snapshot(
        &self,
        name: String,
        mode: VolumeMode,
        size_mb: u64,
        backing_layers: Vec<LayerRef>,
    ) -> Result<VolumeRecord, VolumeError> {
        if backing_layers.is_empty() {
            return Err(VolumeError::EmptySnapshotLayers);
        }
        self.validate_new_volume(&name, size_mb).await?;

        let record = VolumeRecord {
            id: format!("vol_{}", Uuid::now_v7().simple()),
            name,
            purpose: VolumePurpose::User,
            mode,
            size_mb,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_layers,
            read_only_mounts: Vec::new(),
        };
        debug!(
            volume_id = %record.id,
            mode = ?record.mode,
            size_mb = record.size_mb,
            "creating volume from snapshot"
        );
        self.create_volume_in_meta(record.clone(), None, &[])
            .await?;
        info!(
            volume_id = %record.id,
            mode = ?record.mode,
            size_mb = record.size_mb,
            "volume created from snapshot"
        );
        Ok(record)
    }

    pub(crate) async fn snapshot_volume_state(
        &self,
        reference: &str,
    ) -> Result<VolumeRecord, VolumeError> {
        let mut parent = self.get(reference).await?;
        // A running sandbox restacks its volume upper into this same local
        // image config before capture. Publish that latest state before making
        // the logical volume snapshot.
        let owner = parent.reserved_by_sandbox_id.clone();
        if let Some(owner) = owner.as_deref() {
            self.recover_and_publish_backings(owner, std::slice::from_ref(&parent.id))
                .await?;
            parent = self.get(reference).await?;
        } else {
            let local_backing = self.local_backing(&parent).await;
            let uploads = self
                .publish_backing(&mut parent, local_backing.as_deref())
                .await?;
            self.persist_catalog(&parent, &uploads).await?;
            if let Some(path) = local_backing {
                self.backings.cache(&parent, path).await;
            }
        }
        Ok(parent)
    }

    pub async fn reserve(&self, reference: &str, owner: &str) -> Result<(), VolumeError> {
        let record = self.get(reference).await?;
        match record.status {
            VolumeStatus::Uploading => return Err(VolumeError::Uploading(record.id.clone())),
            VolumeStatus::Failed => return Err(VolumeError::Failed(record.id.clone())),
            VolumeStatus::Ready => {}
        }
        if let Some(existing) = self
            .meta
            .update_volume_lease(
                record.id.clone(),
                VolumeLeaseOp::Acquire {
                    owner: owner.to_owned(),
                },
            )
            .await
            .map_err(meta_error)?
        {
            return Err(VolumeError::Reserved(existing));
        }
        debug!(volume_id = %record.id, mode = ?record.mode, "volume reserved");
        Ok(())
    }

    pub async fn replace_owner_for(
        &self,
        owner: &str,
        new_owner: Option<&str>,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        self.check_catalog_fault()?;
        for volume_id in volume_ids {
            let op = match new_owner {
                Some(to) => VolumeLeaseOp::Transfer {
                    from: owner.to_owned(),
                    to: to.to_owned(),
                },
                None => VolumeLeaseOp::Release {
                    owner: owner.to_owned(),
                },
            };
            self.meta
                .update_volume_lease(volume_id.clone(), op)
                .await
                .map_err(meta_error)?;
            match new_owner {
                Some(new_owner) => debug!(
                    volume_id = %volume_id,
                    previous_owner = %owner,
                    new_owner = %new_owner,
                    "volume reservation owner replaced"
                ),
                None => debug!(volume_id = %volume_id, %owner, "volume reservation released"),
            }
        }
        Ok(())
    }

    /// Keep an incomplete capture unavailable after its sandbox is stopped.
    pub(crate) async fn fail_backings(
        &self,
        owner: &str,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        self.check_catalog_fault()?;
        for volume_id in volume_ids {
            let mut record = self.get(volume_id).await?;
            if record.reserved_by_sandbox_id.as_deref() == Some(owner) {
                record.status = VolumeStatus::Failed;
                self.persist_catalog(&record, &[]).await?;
            }
        }
        Ok(())
    }

    /// Restores deterministic node-local config paths after a server restart.
    pub async fn recover_backings(
        &self,
        owner: &str,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        for volume_id in volume_ids {
            let mut record = self.get(volume_id).await?;
            if record.reserved_by_sandbox_id.as_deref() != Some(owner) {
                continue;
            }
            if self.local_backing(&record).await.is_none() {
                let path = self.data_dir(&record.id).join("image.json");
                let exists = tokio::fs::try_exists(&path)
                    .await
                    .map_err(|error| VolumeError::Storage(error.to_string()))?;
                if !exists {
                    record.status = VolumeStatus::Failed;
                    if let Err(error) = self.persist_catalog(&record, &[]).await {
                        warn!(
                            volume_id = %record.id,
                            %error,
                            "failed to persist volume failure status"
                        );
                    }
                    warn!(
                        volume_id = %record.id,
                        path = %path.display(),
                        "reserved volume backing is missing"
                    );
                    return Err(VolumeError::Storage(format!(
                        "local backing for reserved volume '{}' is missing",
                        record.id
                    )));
                }
                debug!(volume_id = %record.id, %owner, "volume backing recovered");
                self.backings.cache(&record, path).await;
            }
        }
        Ok(())
    }

    pub async fn recover_and_publish_backings(
        &self,
        owner: &str,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        self.recover_backings(owner, volume_ids).await?;
        for volume_id in volume_ids {
            let mut record = self.get(volume_id).await?;
            if record.reserved_by_sandbox_id.as_deref() != Some(owner) {
                continue;
            }
            // Publish the state transition before uploading any writable
            // upper layer so other nodes cannot mount stale content.
            record.status = VolumeStatus::Uploading;
            if let Err(error) = self.persist_catalog(&record, &[]).await {
                self.mark_publication_failed(record, "mark_uploading", &error)
                    .await;
                return Err(error);
            }
            let local_backing = self.local_backing(&record).await;
            debug!(volume_id = %record.id, "publishing volume backing");
            let uploads = match self
                .publish_backing(&mut record, local_backing.as_deref())
                .await
            {
                Ok(uploads) => uploads,
                Err(error) => {
                    self.mark_publication_failed(record, "publish_backing", &error)
                        .await;
                    return Err(error);
                }
            };
            record.status = VolumeStatus::Ready;
            if let Err(error) = self.persist_catalog(&record, &uploads).await {
                self.mark_publication_failed(record, "mark_ready", &error)
                    .await;
                return Err(error);
            }
            info!(
                volume_id = %record.id,
                layer_count = record.backing_layers.len(),
                "volume backing published"
            );
            if let Some(path) = local_backing {
                self.backings.cache(&record, path).await;
            }
        }
        Ok(())
    }

    async fn mark_publication_failed(
        &self,
        mut record: VolumeRecord,
        stage: &'static str,
        error: &VolumeError,
    ) {
        record.status = VolumeStatus::Failed;
        if let Err(status_error) = self.persist_catalog(&record, &[]).await {
            warn!(
                volume_id = %record.id,
                error = %status_error,
                "failed to persist volume failure status"
            );
        }
        warn!(
            volume_id = %record.id,
            stage,
            %error,
            "volume backing publication failed"
        );
    }

    async fn publish_backing(
        &self,
        record: &mut VolumeRecord,
        local_backing: Option<&Path>,
    ) -> Result<Vec<PreparedLayerUpload>, VolumeError> {
        if let Some(path) = local_backing {
            let (layers, uploads) =
                crate::repository::store_disk_layers_with_uploads(self.repository.as_ref(), path)
                    .await
                    .map_err(repository_error)?;
            record.backing_layers = layers;
            return Ok(uploads);
        }
        Ok(Vec::new())
    }

    async fn create_volume_in_meta(
        &self,
        record: VolumeRecord,
        from_volume_id: Option<String>,
        uploads: &[PreparedLayerUpload],
    ) -> Result<VolumeRecord, VolumeError> {
        let managed = managed_layers(&record.backing_layers);
        let has_source = from_volume_id.is_some();
        commit_with_layer_restore(
            self.repository.as_ref(),
            &managed,
            uploads,
            |error| match error {
                MetaError::VolumeNotFound { lookup } if has_source => {
                    VolumeError::SourceNotFound(lookup)
                }
                other => meta_error(other),
            },
            repository_error,
            |restored| {
                let record = record.clone();
                let from_volume_id = from_volume_id.clone();
                async move {
                    self.meta
                        .create_volume(record, from_volume_id, restored)
                        .await
                }
            },
        )
        .await
    }

    pub(super) async fn persist_catalog(
        &self,
        record: &VolumeRecord,
        uploads: &[PreparedLayerUpload],
    ) -> Result<(), VolumeError> {
        self.check_catalog_fault()?;
        let managed = managed_layers(&record.backing_layers);
        commit_with_layer_restore(
            self.repository.as_ref(),
            &managed,
            uploads,
            meta_error,
            repository_error,
            |restored| {
                let record = record.clone();
                async move { self.meta.update_volume(record, restored).await }
            },
        )
        .await?;
        Ok(())
    }
}

fn repository_error(error: RepositoryError) -> VolumeError {
    VolumeError::Storage(error.to_string())
}

fn managed_layers(layers: &[LayerRef]) -> Vec<&agentenv_domain::ManagedLayerRef> {
    layers
        .iter()
        .filter_map(|layer| match layer {
            LayerRef::Managed(layer) => Some(layer),
            LayerRef::External(_) => None,
        })
        .collect()
}

fn meta_error(error: MetaError) -> VolumeError {
    match error {
        MetaError::VolumeNotFound { lookup } => VolumeError::NotFound(lookup),
        MetaError::VolumeNameConflict { name } => VolumeError::NameConflict(name),
        MetaError::VolumeReserved { owner, .. } => VolumeError::Reserved(owner),
        other => VolumeError::Storage(other.to_string()),
    }
}

fn encode_volume_cursor(volume_id: String) -> String {
    URL_SAFE_NO_PAD.encode(volume_id)
}

fn decode_volume_cursor(token: &str) -> Result<String, VolumeError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| VolumeError::InvalidNextToken)?;
    let volume_id = String::from_utf8(bytes).map_err(|_| VolumeError::InvalidNextToken)?;
    validate_volume_id(&volume_id).map_err(|_| VolumeError::InvalidNextToken)?;
    Ok(volume_id)
}

fn validate_name(name: &str) -> Result<(), VolumeError> {
    if is_valid_volume_component(name) {
        Ok(())
    } else {
        Err(VolumeError::InvalidName)
    }
}

fn validate_volume_id(id: &str) -> Result<(), VolumeError> {
    if is_valid_volume_component(id) {
        Ok(())
    } else {
        Err(VolumeError::Storage(format!("invalid volume id '{id}'")))
    }
}
