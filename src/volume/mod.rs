use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use overlaybd::config::{ImageConfig, LayerConfig};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::process::Command;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::layer::{LayerRef, PreparedLayerUpload};
use crate::metadata::{meta_context, MetaClient, MetaError};
use crate::repository::{Repository, RepositoryError};

pub use agentenv_domain::{
    is_valid_volume_component, VolumeMode, VolumePurpose, VolumeRecord, VolumeRecordPage,
    VolumeStatus,
};

pub const DEFAULT_VOLUME_SIZE_MB: u64 = 64 * 1024;
pub const DEFAULT_VOLUME_PAGE_SIZE: usize = 100;
const BYTES_PER_MB: u64 = 1024 * 1024;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VolumePage {
    pub records: Vec<VolumeRecord>,
    pub next_token: Option<String>,
}

fn current_local_backing(
    remote: &VolumeRecord,
    local: &HashMap<String, VolumeRecord>,
) -> Option<PathBuf> {
    local
        .get(&remote.id)
        .filter(|record| record.backing_layers == remote.backing_layers)
        .and_then(|record| record.backing_image_config.clone())
        .filter(|path| path.exists())
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum VolumeError {
    #[error("volume name must contain only letters, numbers, underscores, or hyphens")]
    InvalidName,
    #[error("volume name already exists: {0}")]
    NameConflict(String),
    #[error("volume not found: {0}")]
    NotFound(String),
    #[error("volume is reserved by sandbox {0}")]
    Reserved(String),
    #[error("volume is uploading and not usable: {0}")]
    Uploading(String),
    #[error("volume publication failed and is not usable: {0}")]
    Failed(String),
    #[error("fromVolume and image cannot be used together")]
    MultipleSources,
    #[error("volume size must be greater than zero")]
    InvalidSize,
    #[error("volume size exceeds the configured maximum of {max_size_mb} MiB")]
    SizeLimitExceeded { max_size_mb: u64 },
    #[error("volume child size must match its source size")]
    SizeMismatch,
    #[error("sandbox cannot mount more than {max_count} volumes")]
    TooManyMountedVolumes { max_count: usize },
    #[error("source volume not found: {0}")]
    SourceNotFound(String),
    #[error("invalid volume next token")]
    InvalidNextToken,
    #[error("volume page limit must be greater than zero")]
    InvalidPageLimit,
    #[error("volume catalog storage failed: {0}")]
    Storage(String),
}

#[derive(Clone)]
pub struct VolumeManager {
    records: Arc<RwLock<HashMap<String, VolumeRecord>>>,
    root: PathBuf,
    repository: Arc<dyn Repository>,
    meta: MetaClient,
    limits: VolumeLimits,
    #[cfg(test)]
    fail_catalog: Arc<AtomicBool>,
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

impl VolumeManager {
    pub async fn open_with_metadata(
        path: impl Into<std::path::PathBuf>,
        repository: Arc<dyn Repository>,
        meta: MetaClient,
    ) -> anyhow::Result<Self> {
        Self::open_with_metadata_and_limits(path, repository, meta, VolumeLimits::default()).await
    }

    pub async fn open_with_metadata_and_limits(
        path: impl Into<std::path::PathBuf>,
        repository: Arc<dyn Repository>,
        meta: MetaClient,
        mut limits: VolumeLimits,
    ) -> anyhow::Result<Self> {
        let path = path.into();
        let root = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("volume catalog path has no parent"))?
            .to_path_buf();
        limits.max_mounts = limits.max_mounts.min(crate::cfg::MAX_VOLUME_MOUNTS);
        Ok(Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            root,
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
        let mut record = self
            .meta
            .get_volume(meta_context(), reference.to_owned())
            .await
            .map_err(|error| rpc_error("get volume", error))?
            .map_err(meta_error)?
            .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))?;
        validate_volume_id(&record.id)?;
        let local = self.records.read().await;
        record.backing_image_config = current_local_backing(&record, &local);
        Ok(record)
    }

    pub async fn list_page(
        &self,
        next_token: Option<&str>,
        limit: usize,
    ) -> Result<VolumePage, VolumeError> {
        if limit == 0 {
            return Err(VolumeError::InvalidPageLimit);
        }
        let after_volume_id = next_token.map(decode_volume_cursor).transpose()?;
        let limit = u32::try_from(limit).unwrap_or(u32::MAX);
        let mut page = self
            .meta
            .list_volumes_page(meta_context(), after_volume_id, limit, false)
            .await
            .map_err(|error| rpc_error("list volumes", error))?
            .map_err(meta_error)?;
        let local = self.records.read().await;
        for record in &mut page.records {
            validate_volume_id(&record.id)?;
            record.backing_image_config = current_local_backing(record, &local);
        }
        Ok(VolumePage {
            records: page.records,
            next_token: page.next_volume_id.map(encode_volume_cursor),
        })
    }

    pub fn data_dir(&self, volume_id: &str) -> PathBuf {
        self.root.join("data").join(volume_id)
    }

    pub async fn materialize_backing(&self, reference: &str) -> Result<VolumeRecord, VolumeError> {
        let mut record = self.get(reference).await?;
        if record
            .backing_image_config
            .as_ref()
            .is_some_and(|path| path.exists())
            || record.backing_layers.is_empty()
        {
            return Ok(record);
        }
        let destination = self.data_dir(&record.id).join("image.json");
        debug!(
            volume_id = %record.id,
            layer_count = record.backing_layers.len(),
            "materializing volume backing"
        );
        let path = crate::repository::materialize_layer_config(
            self.repository.as_ref(),
            &record.backing_layers,
            &destination,
        )
        .await
        .map_err(repository_error)?;
        debug!(
            volume_id = %record.id,
            path = %path.display(),
            "volume backing materialized"
        );
        record.backing_image_config = Some(path);
        self.cache_record(record.clone()).await;
        Ok(record)
    }

    pub async fn create(
        &self,
        name: String,
        mode: VolumeMode,
        from_volume: Option<String>,
        source_config: Option<PathBuf>,
        size_mb: u64,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_with_source_owner(
            name,
            VolumePurpose::User,
            mode,
            from_volume.map(|reference| (reference, None)),
            source_config,
            size_mb,
            None,
        )
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
        self.create_with_source_owner(
            name,
            VolumePurpose::BuildCacheWork,
            VolumeMode::Exclusive,
            seed.map(|id| (id, None)),
            None,
            size_mb,
            Some(owner),
        )
        .await
    }

    /// Creates an internal read-only build cache volume from a published work
    /// cache before `replace_build_cache_head` promotes it to the active seed.
    pub(crate) async fn create_build_cache_seed(
        &self,
        name: String,
        source_volume_id: String,
        size_mb: u64,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_with_source_owner(
            name,
            VolumePurpose::BuildCacheWork,
            VolumeMode::ReadOnly,
            Some((source_volume_id, None)),
            None,
            size_mb,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_with_source_owner(
        &self,
        name: String,
        purpose: VolumePurpose,
        mode: VolumeMode,
        from_volume: Option<(String, Option<&str>)>,
        source_config: Option<PathBuf>,
        size_mb: u64,
        reserved_owner: Option<&str>,
    ) -> Result<VolumeRecord, VolumeError> {
        validate_name(&name)?;
        if size_mb == 0 {
            return Err(VolumeError::InvalidSize);
        }
        if size_mb > self.limits.max_size_mb {
            return Err(VolumeError::SizeLimitExceeded {
                max_size_mb: self.limits.max_size_mb,
            });
        }
        if from_volume.is_some() && source_config.is_some() {
            return Err(VolumeError::MultipleSources);
        }
        match self.get(&name).await {
            Ok(_) => return Err(VolumeError::NameConflict(name)),
            Err(VolumeError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }

        let id = format!("vol_{}", Uuid::now_v7().simple());
        debug!(
            volume_id = %id,
            mode = ?mode,
            size_mb,
            "creating volume"
        );
        let mut backing_layers = Vec::new();
        let mut cloned_from_id = None;
        let backing_image_config = if let Some((reference, source_owner)) = from_volume {
            let mut parent = match self.get(&reference).await {
                Ok(parent) => parent,
                Err(VolumeError::NotFound(_)) => {
                    return Err(VolumeError::SourceNotFound(reference))
                }
                Err(error) => return Err(error),
            };
            match parent.status {
                VolumeStatus::Uploading => return Err(VolumeError::Uploading(parent.id.clone())),
                VolumeStatus::Failed => return Err(VolumeError::Failed(parent.id.clone())),
                VolumeStatus::Ready => {}
            }
            if let Some(owner) = parent.reserved_by_sandbox_id.as_deref() {
                if source_owner != Some(owner) {
                    return Err(VolumeError::Reserved(owner.to_owned()));
                }
            }
            if parent.size_mb != size_mb {
                return Err(VolumeError::SizeMismatch);
            }
            if parent.reserved_by_sandbox_id.is_none() && !parent.backing_layers.is_empty() {
                // Committed layers are immutable and repository-owned. Materializing
                // the child on mount preserves their descriptors without copying or
                // republishing the parent's local files. Reserved parents may have
                // newer local writes, so they still need the local clone below.
                cloned_from_id = Some(parent.id);
                backing_layers = parent.backing_layers;
                None
            } else {
                if parent.backing_image_config.is_none() && !parent.backing_layers.is_empty() {
                    parent = self.materialize_backing(&parent.id).await?;
                }
                match parent.backing_image_config.as_ref() {
                    Some(path) => Some(self.create_child_backing(&id, path).await?),
                    None => None,
                }
            }
        } else if let Some(source_config) = source_config {
            Some(self.create_child_backing(&id, &source_config).await?)
        } else {
            Some(self.create_empty_backing(&id, size_mb).await?)
        };

        let mut record = VolumeRecord {
            id,
            name,
            purpose,
            mode,
            size_mb,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: reserved_owner.map(str::to_owned),
            backing_image_config,
            backing_layers,
            read_only_mounts: Vec::new(),
            deleting: false,
        };
        let create_result = async {
            let uploads = if reserved_owner.is_none() {
                self.publish_backing(&mut record).await?
            } else {
                Vec::new()
            };
            self.create_volume_in_meta(record.clone(), cloned_from_id, &uploads)
                .await?;
            self.cache_record(record.clone()).await;
            Ok(())
        }
        .await;
        if let Err(error) = create_result {
            if let Some(config) = record.backing_image_config.as_ref() {
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
        if let Some(owner) = record.reserved_by_sandbox_id.as_deref() {
            return Err(VolumeError::Reserved(owner.to_owned()));
        }
        if let Some(owner) = record.read_only_mounts.first() {
            return Err(VolumeError::Reserved(owner.clone()));
        }
        debug!(volume_id = %record.id, "deleting volume");
        self.meta
            .delete_volume(meta_context(), record.id.clone())
            .await
            .map_err(|error| rpc_error("delete volume", error))?
            .map_err(meta_error)?;
        let backing_directory = self.data_dir(&record.id);
        self.records.write().await.remove(&record.id);
        if let Err(error) = tokio::fs::remove_dir_all(&backing_directory).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    volume_id = %record.id,
                    path = %backing_directory.display(),
                    %error,
                    "failed to clean deleted volume's node-local backing"
                );
            }
        }
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
        self.create_with_source_owner(
            name,
            VolumePurpose::User,
            mode,
            Some((reference.to_owned(), Some(source_owner))),
            None,
            size_mb,
            Some(child_owner),
        )
        .await
    }

    pub(crate) async fn create_from_snapshot(
        &self,
        name: String,
        mode: VolumeMode,
        size_mb: u64,
        backing_layers: Vec<LayerRef>,
    ) -> Result<VolumeRecord, VolumeError> {
        validate_name(&name)?;
        if size_mb == 0 || backing_layers.is_empty() {
            return Err(VolumeError::InvalidSize);
        }
        if size_mb > self.limits.max_size_mb {
            return Err(VolumeError::SizeLimitExceeded {
                max_size_mb: self.limits.max_size_mb,
            });
        }
        match self.get(&name).await {
            Ok(_) => return Err(VolumeError::NameConflict(name)),
            Err(VolumeError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }

        let record = VolumeRecord {
            id: format!("vol_{}", Uuid::now_v7().simple()),
            name,
            purpose: VolumePurpose::User,
            mode,
            size_mb,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_image_config: None,
            backing_layers,
            read_only_mounts: Vec::new(),
            deleting: false,
        };
        debug!(
            volume_id = %record.id,
            mode = ?record.mode,
            size_mb = record.size_mb,
            "creating volume from snapshot"
        );
        self.create_volume_in_meta(record.clone(), None, &[])
            .await?;
        self.cache_record(record.clone()).await;
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
            let uploads = self.publish_backing(&mut parent).await?;
            self.persist_catalog(&parent, &uploads).await?;
            self.cache_record(parent.clone()).await;
        }
        Ok(parent)
    }

    pub async fn reserve(&self, reference: &str, owner: &str) -> Result<(), VolumeError> {
        let mut record = self.get(reference).await?;
        match record.status {
            VolumeStatus::Uploading => return Err(VolumeError::Uploading(record.id.clone())),
            VolumeStatus::Failed => return Err(VolumeError::Failed(record.id.clone())),
            VolumeStatus::Ready => {}
        }
        if record.mode == VolumeMode::ReadOnly {
            self.meta
                .reserve_read_only_volume(meta_context(), record.id.clone(), owner.to_owned())
                .await
                .map_err(|error| rpc_error("reserve read-only volume", error))?
                .map_err(meta_error)?;
            if !record.read_only_mounts.iter().any(|entry| entry == owner) {
                record.read_only_mounts.push(owner.to_owned());
            }
            debug!(volume_id = %record.id, "read-only volume reserved");
            self.cache_record(record).await;
            return Ok(());
        }
        if let Some(existing) = self
            .meta
            .reserve_volume(meta_context(), record.id.clone(), owner.to_owned())
            .await
            .map_err(|error| rpc_error("reserve volume", error))?
            .map_err(meta_error)?
        {
            return Err(VolumeError::Reserved(existing));
        }
        record.reserved_by_sandbox_id = Some(owner.to_owned());
        debug!(volume_id = %record.id, "exclusive volume reserved");
        self.cache_record(record).await;
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
            self.meta
                .replace_volume_owner(
                    meta_context(),
                    volume_id.clone(),
                    owner.to_owned(),
                    new_owner.map(str::to_owned),
                )
                .await
                .map_err(|error| rpc_error("replace volume owner", error))?
                .map_err(meta_error)?;
            if let Some(record) = self.records.write().await.get_mut(volume_id) {
                record.replace_owner(owner, new_owner);
            }
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
                self.cache_record(record).await;
            }
        }
        Ok(())
    }

    pub async fn publish_backings(
        &self,
        owner: &str,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        let mut records = Vec::with_capacity(volume_ids.len());
        for volume_id in volume_ids {
            let record = self.get(volume_id).await?;
            if record.mounted_by(owner) {
                records.push(record);
            }
        }
        self.publish_records(owner, records).await
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
            if record.backing_image_config.is_none() {
                let path = self.data_dir(&record.id).join("image.json");
                if !path.exists() {
                    record.status = VolumeStatus::Failed;
                    if let Err(error) = self.persist_catalog(&record, &[]).await {
                        warn!(
                            volume_id = %record.id,
                            %error,
                            "failed to persist volume failure status"
                        );
                    }
                    self.cache_record(record.clone()).await;
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
                record.backing_image_config = Some(path);
                debug!(volume_id = %record.id, %owner, "volume backing recovered");
                self.cache_record(record).await;
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
        self.publish_backings(owner, volume_ids).await
    }

    async fn publish_records(
        &self,
        owner: &str,
        records: Vec<VolumeRecord>,
    ) -> Result<(), VolumeError> {
        for mut record in records {
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
            self.cache_record(record.clone()).await;
            debug!(volume_id = %record.id, "publishing volume backing");
            let uploads = match self.publish_backing(&mut record).await {
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
            self.cache_record(record).await;
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
        self.cache_record(record).await;
    }

    async fn cache_record(&self, record: VolumeRecord) {
        self.records.write().await.insert(record.id.clone(), record);
    }

    async fn publish_backing(
        &self,
        record: &mut VolumeRecord,
    ) -> Result<Vec<PreparedLayerUpload>, VolumeError> {
        if let Some(path) = record.backing_image_config.as_deref() {
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
        let first = self
            .meta
            .create_volume(
                meta_context(),
                record.clone(),
                from_volume_id.clone(),
                Vec::new(),
            )
            .await
            .map_err(|error| rpc_error("create volume", error))?;
        match first {
            Ok(created) => Ok(created),
            Err(MetaError::MissingLayers { uuids }) => {
                self.restore_missing_layers(&record.backing_layers, &uuids, uploads)
                    .await?;
                self.meta
                    .create_volume(meta_context(), record, from_volume_id.clone(), uuids)
                    .await
                    .map_err(|error| rpc_error("create volume", error))?
                    .map_err(|error| match error {
                        MetaError::VolumeNotFound { lookup } if from_volume_id.is_some() => {
                            VolumeError::SourceNotFound(lookup)
                        }
                        other => meta_error(other),
                    })
            }
            Err(MetaError::VolumeNotFound { lookup }) if from_volume_id.is_some() => {
                Err(VolumeError::SourceNotFound(lookup))
            }
            Err(other) => Err(meta_error(other)),
        }
    }

    async fn persist_catalog(
        &self,
        record: &VolumeRecord,
        uploads: &[PreparedLayerUpload],
    ) -> Result<(), VolumeError> {
        self.check_catalog_fault()?;
        let first = self
            .meta
            .update_volume(meta_context(), record.clone(), Vec::new())
            .await
            .map_err(|error| rpc_error("update volume", error))?;
        match first {
            Ok(_) => Ok(()),
            Err(MetaError::MissingLayers { uuids }) => {
                self.restore_missing_layers(&record.backing_layers, &uuids, uploads)
                    .await?;
                self.meta
                    .update_volume(meta_context(), record.clone(), uuids)
                    .await
                    .map_err(|error| rpc_error("update volume", error))?
                    .map_err(meta_error)?;
                Ok(())
            }
            Err(other) => Err(meta_error(other)),
        }
    }

    async fn restore_missing_layers(
        &self,
        layers: &[LayerRef],
        missing_uuids: &[String],
        uploads: &[PreparedLayerUpload],
    ) -> Result<(), VolumeError> {
        let local_layers: Vec<(&str, &Path)> = uploads
            .iter()
            .map(|upload| (upload.digest(), upload.path()))
            .collect();
        let managed: Vec<_> = layers
            .iter()
            .filter_map(|layer| match layer {
                LayerRef::Managed(layer) => Some(layer),
                LayerRef::External(_) => None,
            })
            .collect();
        crate::repository::restore_missing_managed_layers(
            self.repository.as_ref(),
            &managed,
            missing_uuids,
            &local_layers,
        )
        .await
        .map_err(repository_error)
    }

    async fn create_empty_backing(
        &self,
        volume_id: &str,
        size_mb: u64,
    ) -> Result<PathBuf, VolumeError> {
        let directory = self.data_dir(volume_id);
        let raw = directory.join("base.ext4");
        let commit = directory.join("base.commit");
        let image_config = directory.join("image.json");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;

        let result = async {
            let file = tokio::fs::File::create(&raw)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            file.set_len(
                size_mb
                    .checked_mul(BYTES_PER_MB)
                    .ok_or(VolumeError::InvalidSize)?,
            )
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;
            drop(file);
            let status = Command::new("mkfs.ext4")
                .args(["-q", "-F"])
                .arg(&raw)
                .status()
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            if !status.success() {
                return Err(VolumeError::Storage(format!(
                    "mkfs.ext4 exited with status {status}"
                )));
            }
            overlaybd::tools::package_raw_as_overlaybd(&raw, &commit)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            let config = ImageConfig {
                lowers: vec![LayerConfig {
                    file: commit.to_string_lossy().into_owned(),
                    ..LayerConfig::default()
                }],
                ..ImageConfig::default()
            };
            let bytes = serde_json::to_vec_pretty(&config)
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            tokio::fs::write(&image_config, bytes)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            let _ = tokio::fs::remove_file(&raw).await;
            Ok(image_config)
        }
        .await;

        if result.is_err() {
            let _ = tokio::fs::remove_dir_all(&directory).await;
        }
        result
    }

    async fn create_child_backing(
        &self,
        volume_id: &str,
        parent_config: &Path,
    ) -> Result<PathBuf, VolumeError> {
        let directory = self.data_dir(volume_id);
        let image_config = directory.join("image.json");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;

        let result = async {
            let bytes = tokio::fs::read(parent_config)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            let mut config: ImageConfig = serde_json::from_slice(&bytes)
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            for (index, layer) in config.lowers.iter_mut().enumerate() {
                if layer.file.is_empty() {
                    continue;
                }
                let source = Path::new(&layer.file);
                let extension = source
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .unwrap_or("layer");
                let destination = directory.join(format!("lower-{index}.{extension}"));
                if let Err(link_error) = tokio::fs::hard_link(source, &destination).await {
                    tokio::fs::copy(source, &destination).await.map_err(|copy_error| {
                        VolumeError::Storage(format!(
                            "clone volume layer '{}' (hard link failed: {link_error}; copy failed: {copy_error})",
                            source.display()
                        ))
                    })?;
                }
                layer.file = destination.to_string_lossy().into_owned();
            }
            let bytes = serde_json::to_vec_pretty(&config)
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            tokio::fs::write(&image_config, bytes)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            Ok(image_config.clone())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_dir_all(&directory).await;
        }
        result
    }
}

fn repository_error(error: RepositoryError) -> VolumeError {
    match error {
        RepositoryError::VolumeNotFound { lookup } => VolumeError::NotFound(lookup),
        RepositoryError::VolumeNameConflict { name } => VolumeError::NameConflict(name),
        error => VolumeError::Storage(error.to_string()),
    }
}

fn meta_error(error: MetaError) -> VolumeError {
    match error {
        MetaError::VolumeNotFound { lookup } => VolumeError::NotFound(lookup),
        MetaError::VolumeNameConflict { name } => VolumeError::NameConflict(name),
        MetaError::VolumeReserved { owner, .. } => VolumeError::Reserved(owner),
        other => VolumeError::Storage(other.to_string()),
    }
}

fn rpc_error(operation: &str, error: crate::metadata::RpcError) -> VolumeError {
    VolumeError::Storage(format!("{operation}: {error}"))
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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use overlaybd::config::{ImageConfig, LayerConfig};

    use super::{VolumeError, VolumeManager, VolumeMode, VolumeRecord, VolumeStatus};
    use crate::digest::sha256_digest;
    use crate::layer::{LayerRef, ManagedLayerRef};
    use crate::metadata::{test_meta, GcStorage, MetaClient, MetaServer};
    use crate::repository::backends::PosixFsRepository;
    use crate::repository::{BlobKey, LayerRole, Repository};

    struct TestVolumeGcStorage(Arc<dyn Repository>);

    #[async_trait::async_trait]
    impl GcStorage for TestVolumeGcStorage {
        async fn delete_snapshot(&self, id: &crate::types::SnapshotId) -> anyhow::Result<()> {
            self.0.delete_snapshot_blobs(id).await.map_err(Into::into)
        }

        async fn delete_layer(&self, digest: &str) -> anyhow::Result<()> {
            self.0
                .delete_managed_layer(digest)
                .await
                .map_err(Into::into)
        }
    }

    async fn manager_with_meta(
        root: &Path,
        repository: Arc<dyn Repository>,
        meta: MetaClient,
    ) -> VolumeManager {
        VolumeManager::open_with_metadata(root.join("volumes/catalog"), repository, meta)
            .await
            .expect("volume manager")
    }

    async fn manager_at(
        root: &Path,
    ) -> (MetaServer, MetaClient, Arc<dyn Repository>, VolumeManager) {
        let repository: Arc<dyn Repository> =
            Arc::new(PosixFsRepository::new(root.join("repository")));
        let (server, _client) = test_meta(root).await;
        let server = server.with_gc_storage(Arc::new(TestVolumeGcStorage(Arc::clone(&repository))));
        let meta = agentenv_metadata::transport::embedded(server.clone());
        let manager = manager_with_meta(root, Arc::clone(&repository), meta.clone()).await;
        (server, meta, repository, manager)
    }

    async fn write_volume_layer(path: &Path, payload: &[u8]) -> ManagedLayerRef {
        use overlaybd::backend::local::LocalFile;
        use overlaybd::index_file::{CommitArgs, LSMTFile};
        use overlaybd::virtual_file::VirtualFile;
        let data = Arc::new(LocalFile::new(path.with_extension("data")).unwrap());
        let index = Arc::new(LocalFile::new(path.with_extension("index")).unwrap());
        let layer = LSMTFile::create(data, Some(index), 4096, false)
            .await
            .unwrap();
        let mut page = vec![0; 4096];
        page[..payload.len()].copy_from_slice(payload);
        layer.write_at(0, &page).await.unwrap();
        let mut args = CommitArgs::new(Arc::new(LocalFile::new(path).unwrap()));
        args.uuid = Some(uuid::Uuid::nil());
        layer.commit_with_args(args).await.unwrap();
        let bytes = std::fs::read(path).unwrap();
        ManagedLayerRef {
            digest: sha256_digest(&bytes),
            size: bytes.len() as u64,
            uuid: None,
        }
    }

    async fn published_volume(root: &Path, manager: &VolumeManager) -> VolumeRecord {
        let layer = root.join("base.commit");
        write_volume_layer(&layer, b"published volume layer").await;
        let config = ImageConfig {
            lowers: vec![
                LayerConfig {
                    file: layer.to_string_lossy().into_owned(),
                    ..LayerConfig::default()
                },
                LayerConfig {
                    repo_blob_url: "https://registry.example.test/v2/cache/blobs".to_owned(),
                    digest: sha256_digest(b"external volume layer"),
                    size: 21,
                    ..LayerConfig::default()
                },
            ],
            ..ImageConfig::default()
        };
        let path = root.join("source.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        manager
            .create(
                "parent".to_owned(),
                VolumeMode::Exclusive,
                None,
                Some(path),
                1024,
            )
            .await
            .expect("publish initial volume")
    }

    async fn append_local_layer(record: &VolumeRecord) -> ManagedLayerRef {
        let path = record.backing_image_config.as_ref().unwrap();
        let layer = path.parent().unwrap().join("unpublished.commit");
        let descriptor = write_volume_layer(&layer, b"new volume writes").await;
        let mut config = overlaybd::config::load_image_config(path).unwrap();
        config.lowers.push(LayerConfig {
            file: layer.to_string_lossy().into_owned(),
            ..LayerConfig::default()
        });
        std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
        descriptor
    }

    #[tokio::test]
    async fn published_volume_clones_survive_parent_deletion_without_local_files() {
        let temp = tempfile::tempdir().unwrap();
        let (server, meta, repository, manager) = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        // A stale local config must not override the committed backing, even
        // when the manager still has the config path in its in-memory cache.
        std::fs::write(
            parent.backing_image_config.as_ref().unwrap(),
            b"stale config",
        )
        .unwrap();
        let cold_manager =
            manager_with_meta(temp.path(), Arc::clone(&repository), meta.clone()).await;
        let mut children = Vec::new();
        for (index, (manager, mode)) in [
            (&manager, VolumeMode::ReadOnly),
            (&cold_manager, VolumeMode::Exclusive),
        ]
        .into_iter()
        .enumerate()
        {
            let child = manager
                .create(
                    format!("child-{index}"),
                    mode,
                    Some(parent.id.clone()),
                    None,
                    1024,
                )
                .await
                .expect("clone committed volume without reading local files");
            assert_eq!(child.backing_layers, parent.backing_layers);
            assert_eq!(child.mode, mode);
            assert!(child.backing_image_config.is_none());
            assert!(!manager.data_dir(&child.id).exists());
            children.push(child);
        }
        manager.delete(&parent.id).await.unwrap();
        // Deleting the parent releases its layer head, while the clones keep
        // the shared managed layer alive across GC sweeps.
        let report = server.collect_garbage(64).await.unwrap();
        assert_eq!(report.deleted_layers, 0);
        let base_digest = match &parent.backing_layers[0] {
            LayerRef::Managed(layer) => layer.digest.clone(),
            LayerRef::External(_) => panic!("expected managed base layer"),
        };
        assert!(repository
            .has_blob(&BlobKey::Layer {
                digest: base_digest.clone(),
                role: LayerRole::Rootfs,
            })
            .await
            .unwrap());

        let restarted = manager_with_meta(temp.path(), Arc::clone(&repository), meta).await;
        for child in &children {
            let child = restarted.materialize_backing(&child.id).await.unwrap();
            let config =
                overlaybd::config::load_image_config(child.backing_image_config.unwrap()).unwrap();
            assert_eq!(
                std::fs::read(&config.lowers[0].file).unwrap(),
                std::fs::read(temp.path().join("base.commit")).unwrap()
            );
            assert_eq!(
                config.lowers[0].digest,
                sha256_digest(&std::fs::read(temp.path().join("base.commit")).unwrap())
            );
            assert_eq!(
                config.lowers[0].size,
                std::fs::metadata(temp.path().join("base.commit"))
                    .unwrap()
                    .len()
            );
            assert!(config.lowers[1].file.is_empty());
            assert_eq!(
                config.lowers[1].repo_blob_url,
                "https://registry.example.test/v2/cache/blobs"
            );
            assert_eq!(
                config.lowers[1].digest,
                sha256_digest(b"external volume layer")
            );
        }
        for child in children {
            restarted.delete(&child.id).await.unwrap();
        }
        let report = server.collect_garbage(64).await.unwrap();
        assert_eq!(report.deleted_layers, 1);
        assert!(!repository
            .has_blob(&BlobKey::Layer {
                digest: base_digest,
                role: LayerRole::Rootfs,
            })
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn volume_generations_preserve_inherited_descriptors_and_new_writes() {
        let temp = tempfile::tempdir().unwrap();
        let (_server, _meta, _repo, manager) = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        let seed = manager
            .create(
                "seed".to_owned(),
                VolumeMode::ReadOnly,
                Some(parent.id.clone()),
                None,
                1024,
            )
            .await
            .unwrap();
        manager.reserve(&seed.id, "builder").await.unwrap();
        let child = manager
            .create_child_for_owner(
                &seed.id,
                "work".to_owned(),
                VolumeMode::Exclusive,
                1024,
                "builder",
                "builder",
            )
            .await
            .unwrap();
        assert_eq!(child.backing_layers, parent.backing_layers);
        let child = manager.materialize_backing(&child.id).await.unwrap();
        let config =
            overlaybd::config::load_image_config(child.backing_image_config.as_ref().unwrap())
                .unwrap();
        assert_eq!(
            config.lowers[0].digest,
            sha256_digest(&std::fs::read(temp.path().join("base.commit")).unwrap())
        );
        assert_eq!(
            config.lowers[0].size,
            std::fs::metadata(temp.path().join("base.commit"))
                .unwrap()
                .len()
        );
        let appended = append_local_layer(&child).await;
        manager
            .recover_and_publish_backings("builder", std::slice::from_ref(&child.id))
            .await
            .unwrap();
        manager
            .replace_owner_for("builder", None, &[seed.id, child.id.clone()])
            .await
            .unwrap();
        let published = manager.get(&child.id).await.unwrap();
        assert_eq!(
            &published.backing_layers[..2],
            parent.backing_layers.as_slice()
        );
        assert_eq!(published.backing_layers[2], LayerRef::Managed(appended));
        let next_seed = manager
            .create(
                "next-seed".to_owned(),
                VolumeMode::ReadOnly,
                Some(child.id),
                None,
                1024,
            )
            .await
            .unwrap();
        assert_eq!(next_seed.backing_layers, published.backing_layers);
        assert!(next_seed.backing_image_config.is_none());
    }

    #[tokio::test]
    async fn reserved_volume_clone_retains_unpublished_local_layers() {
        let temp = tempfile::tempdir().unwrap();
        let (_server, _meta, _repo, manager) = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        manager.reserve(&parent.id, "source").await.unwrap();
        let appended = append_local_layer(&parent).await;
        let child = manager
            .create_child_for_owner(
                &parent.id,
                "fork".to_owned(),
                VolumeMode::Exclusive,
                1024,
                "source",
                "child",
            )
            .await
            .unwrap();
        assert!(child.backing_layers.is_empty());
        manager
            .replace_owner_for("source", None, std::slice::from_ref(&parent.id))
            .await
            .unwrap();
        manager.delete(&parent.id).await.unwrap();
        manager
            .recover_and_publish_backings("child", std::slice::from_ref(&child.id))
            .await
            .unwrap();
        let child = manager.get(&child.id).await.unwrap();
        assert_eq!(child.backing_layers.len(), 3);
        assert_eq!(child.backing_layers[2], LayerRef::Managed(appended));
    }

    #[tokio::test]
    async fn published_volume_clone_validates_source_state_and_size() {
        let temp = tempfile::tempdir().unwrap();
        let (_server, _meta, _repo, manager) = manager_at(temp.path()).await;
        let mut parent = published_volume(temp.path(), &manager).await;
        assert_eq!(
            manager
                .create(
                    "wrong-size".to_owned(),
                    VolumeMode::ReadOnly,
                    Some(parent.id.clone()),
                    None,
                    2048
                )
                .await
                .unwrap_err(),
            VolumeError::SizeMismatch
        );
        manager.reserve(&parent.id, "owner").await.unwrap();
        assert_eq!(
            manager
                .create(
                    "reserved".to_owned(),
                    VolumeMode::ReadOnly,
                    Some(parent.id.clone()),
                    None,
                    1024
                )
                .await
                .unwrap_err(),
            VolumeError::Reserved("owner".to_owned())
        );
        for status in [VolumeStatus::Uploading, VolumeStatus::Failed] {
            parent.status = status;
            manager.persist_catalog(&parent, &[]).await.unwrap();
            let error = manager
                .create(
                    "unready".to_owned(),
                    VolumeMode::ReadOnly,
                    Some(parent.id.clone()),
                    None,
                    1024,
                )
                .await
                .unwrap_err();
            assert_eq!(
                error,
                match status {
                    VolumeStatus::Uploading => VolumeError::Uploading(parent.id.clone()),
                    VolumeStatus::Failed => VolumeError::Failed(parent.id.clone()),
                    VolumeStatus::Ready => unreachable!(),
                }
            );
        }
    }

    #[tokio::test]
    async fn create_from_snapshot_preserves_read_only_mode() {
        let temp = tempfile::tempdir().expect("temporary volume repository");
        let (_server, _meta, _repo, manager) = manager_at(temp.path()).await;

        let restored = manager
            .create_from_snapshot(
                "restored-read-only".to_owned(),
                VolumeMode::ReadOnly,
                1024,
                vec![LayerRef::Managed(ManagedLayerRef {
                    digest: sha256_digest(b"abc"),
                    size: 4096,
                    uuid: None,
                })],
            )
            .await
            .expect("restore volume snapshot");

        assert_eq!(restored.mode, VolumeMode::ReadOnly);
        assert_eq!(
            manager
                .get(&restored.id)
                .await
                .expect("persisted volume")
                .mode,
            VolumeMode::ReadOnly
        );
    }
}
