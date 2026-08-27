use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use overlaybd::config::{ImageConfig, LayerConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::process::Command;
use tokio::sync::RwLock;
use tracing::warn;
use uuid::Uuid;

use crate::snapshot::repository::{volume_catalog_shard, RepositoryError, SnapshotRepository};
use crate::snapshot::OverlaybdLayerRef;

pub const DEFAULT_VOLUME_SIZE_MB: u64 = 64 * 1024;
/// Firecracker exposes /dev/vdc through /dev/vdz for extra drives.
pub const MAX_VOLUME_MOUNTS: usize = 24;
pub const DEFAULT_VOLUME_PAGE_SIZE: usize = 100;
const BYTES_PER_MB: u64 = 1024 * 1024;

pub(crate) fn is_valid_volume_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VolumePage {
    pub records: Vec<VolumeRecord>,
    pub next_token: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum VolumeMode {
    ReadOnly,
    #[default]
    Exclusive,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeStatus {
    #[default]
    Ready,
    Uploading,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeRecord {
    pub id: String,
    pub name: String,
    pub mode: VolumeMode,
    pub source: String,
    pub parent_volume_id: Option<String>,
    pub revision: u64,
    pub size_mb: u64,
    pub status: VolumeStatus,
    pub reserved_by_sandbox_id: Option<String>,
    #[serde(skip)]
    pub backing_image_config: Option<PathBuf>,
    /// Repository-owned logical layers for the volume backing. The image config
    /// path above is only a node-local runtime cache and is never required for
    /// another node to reopen the volume.
    pub backing_layers: Vec<OverlaybdLayerRef>,
    pub read_only_mounts: Vec<String>,
}

impl VolumeRecord {
    pub(crate) fn mounted_by(&self, owner: &str) -> bool {
        self.reserved_by_sandbox_id.as_deref() == Some(owner)
            || self.read_only_mounts.iter().any(|entry| entry == owner)
    }

    pub(crate) fn replace_owner(&mut self, from: &str, to: Option<&str>) -> bool {
        let mut changed = false;
        if self.reserved_by_sandbox_id.as_deref() == Some(from) {
            self.reserved_by_sandbox_id = to.map(str::to_owned);
            changed = true;
        }
        if self.read_only_mounts.iter().any(|owner| owner == from) {
            self.read_only_mounts.retain(|owner| owner != from);
            if let Some(to) = to {
                if !self.read_only_mounts.iter().any(|owner| owner == to) {
                    self.read_only_mounts.push(to.to_owned());
                }
            }
            changed = true;
        }
        changed
    }
}

fn current_local_backing(
    remote: &VolumeRecord,
    local: &HashMap<String, VolumeRecord>,
) -> Option<PathBuf> {
    local
        .get(&remote.id)
        .filter(|record| {
            record.revision == remote.revision && record.backing_layers == remote.backing_layers
        })
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
    root: Option<PathBuf>,
    repository: Option<Arc<dyn SnapshotRepository>>,
    limits: VolumeLimits,
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
    pub fn new() -> Self {
        Self::with_limits(VolumeLimits::default())
    }

    pub fn with_limits(limits: VolumeLimits) -> Self {
        Self::with_storage(None, None, limits)
    }

    fn with_storage(
        root: Option<PathBuf>,
        repository: Option<Arc<dyn SnapshotRepository>>,
        mut limits: VolumeLimits,
    ) -> Self {
        limits.max_mounts = limits.max_mounts.min(MAX_VOLUME_MOUNTS);
        Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            root,
            repository,
            limits,
        }
    }

    pub async fn open(path: impl Into<std::path::PathBuf>) -> anyhow::Result<Self> {
        Self::open_with_repository(path, None).await
    }

    pub async fn open_with_repository(
        path: impl Into<std::path::PathBuf>,
        repository: Option<Arc<dyn SnapshotRepository>>,
    ) -> anyhow::Result<Self> {
        Self::open_with_repository_and_limits(path, repository, VolumeLimits::default()).await
    }

    pub async fn open_with_repository_and_limits(
        path: impl Into<std::path::PathBuf>,
        repository: Option<Arc<dyn SnapshotRepository>>,
        limits: VolumeLimits,
    ) -> anyhow::Result<Self> {
        let path = path.into();
        Ok(Self::with_storage(
            path.parent().map(Path::to_path_buf),
            repository,
            limits,
        ))
    }

    pub fn limits(&self) -> VolumeLimits {
        self.limits
    }

    pub async fn get(&self, reference: &str) -> Result<VolumeRecord, VolumeError> {
        if let Some(repository) = self.repository.as_ref() {
            let mut record = repository
                .get_volume(reference)
                .await
                .map_err(repository_error)?
                .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))?;
            validate_volume_id(&record.id)?;
            let local = self.records.read().await;
            record.backing_image_config = current_local_backing(&record, &local);
            return Ok(record);
        }
        self.local_get(reference).await
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
        if let Some(repository) = self.repository.as_ref() {
            let mut page = repository
                .list_volumes_page(after_volume_id.as_deref(), limit)
                .await
                .map_err(repository_error)?;
            let local = self.records.read().await;
            for record in &mut page.records {
                validate_volume_id(&record.id)?;
                record.backing_image_config = current_local_backing(record, &local);
            }
            return Ok(VolumePage {
                records: page.records,
                next_token: page.next_volume_id.map(encode_volume_cursor),
            });
        }

        let mut records: Vec<_> = self.records.read().await.values().cloned().collect();
        records.sort_by(|left, right| {
            volume_catalog_shard(&left.id)
                .cmp(&volume_catalog_shard(&right.id))
                .then_with(|| left.id.cmp(&right.id))
        });
        if let Some(after) = after_volume_id.as_deref() {
            let shard = volume_catalog_shard(after);
            records.retain(|record| {
                let record_shard = volume_catalog_shard(&record.id);
                record_shard.as_str() > shard.as_str()
                    || (record_shard == shard && record.id.as_str() > after)
            });
        }
        let has_more = records.len() > limit;
        records.truncate(limit);
        let next_token = if has_more {
            records
                .last()
                .map(|record| encode_volume_cursor(record.id.clone()))
        } else {
            None
        };
        Ok(VolumePage {
            records,
            next_token,
        })
    }

    pub fn data_dir(&self, volume_id: &str) -> Option<PathBuf> {
        self.root
            .as_ref()
            .map(|root| root.join("data").join(volume_id))
    }

    pub async fn ensure_backing_config(
        &self,
        reference: &str,
        source_config: &Path,
    ) -> Result<PathBuf, VolumeError> {
        let mut record = self.get(reference).await?;
        if let Some(path) = record
            .backing_image_config
            .clone()
            .filter(|path| path.exists())
        {
            return Ok(path);
        }
        let Some(directory) = self.data_dir(&record.id) else {
            return Ok(source_config.to_path_buf());
        };
        let target = directory.join("image.json");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;
        tokio::fs::copy(source_config, &target)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;
        record.backing_image_config = Some(target.clone());
        self.persist(&mut record).await?;
        self.cache_record(record).await;
        Ok(target)
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
        let (Some(repository), Some(root)) = (self.repository.as_ref(), self.root.as_ref()) else {
            return Ok(record);
        };
        let destination = root.join("data").join(&record.id).join("image.json");
        let path = repository
            .materialize_volume_backing(&record.id, &record.backing_layers, &destination)
            .await
            .map_err(repository_error)?;
        record.backing_image_config = Some(path);
        self.cache_record(record.clone()).await;
        Ok(record)
    }

    pub async fn create(
        &self,
        name: String,
        mode: VolumeMode,
        from_volume: Option<String>,
        image: Option<String>,
        size_mb: u64,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_with_source_owner(name, mode, from_volume, image, size_mb, None)
            .await
    }

    async fn create_with_source_owner(
        &self,
        name: String,
        mode: VolumeMode,
        from_volume: Option<String>,
        image: Option<String>,
        size_mb: u64,
        source_owner: Option<&str>,
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
        if from_volume.is_some() && image.is_some() {
            return Err(VolumeError::MultipleSources);
        }
        match self.get(&name).await {
            Ok(_) => return Err(VolumeError::NameConflict(name)),
            Err(VolumeError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }

        let id = format!("vol_{}", Uuid::now_v7().simple());
        let (source, parent_volume_id, revision, backing_image_config) =
            if let Some(reference) = from_volume {
                let mut parent = match self.get(&reference).await {
                    Ok(parent) => parent,
                    Err(VolumeError::NotFound(_)) => {
                        return Err(VolumeError::SourceNotFound(reference))
                    }
                    Err(error) => return Err(error),
                };
                if parent.status == VolumeStatus::Uploading {
                    return Err(VolumeError::Uploading(parent.id.clone()));
                }
                if parent.status == VolumeStatus::Failed {
                    return Err(VolumeError::Failed(parent.id.clone()));
                }
                if let Some(owner) = parent.reserved_by_sandbox_id.as_deref() {
                    if source_owner != Some(owner) {
                        return Err(VolumeError::Reserved(owner.to_owned()));
                    }
                }
                if parent.size_mb != size_mb {
                    return Err(VolumeError::SizeMismatch);
                }
                if parent.backing_image_config.is_none() && !parent.backing_layers.is_empty() {
                    parent = self.materialize_backing(&parent.id).await?;
                }
                (
                    "volume".to_owned(),
                    Some(parent.id.clone()),
                    parent.revision,
                    match parent.backing_image_config.as_ref() {
                        Some(path) => Some(self.create_child_backing(&id, path).await?),
                        None => None,
                    },
                )
            } else if let Some(image) = image {
                (image, None, 0, None)
            } else {
                let backing_image_config = self.create_empty_backing(&id, size_mb).await?;
                ("empty".to_owned(), None, 0, backing_image_config)
            };

        let mut record = VolumeRecord {
            id,
            name,
            mode,
            source,
            parent_volume_id,
            revision,
            size_mb,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_image_config,
            backing_layers: Vec::new(),
            read_only_mounts: Vec::new(),
        };
        let create_result = async {
            self.publish_backing(&mut record).await?;
            if let Some(repository) = self.repository.as_ref() {
                repository
                    .create_volume(record.clone())
                    .await
                    .map_err(repository_error)?;
                self.cache_record(record.clone()).await;
            } else {
                let mut records = self.records.write().await;
                if records.values().any(|entry| entry.name == record.name) {
                    return Err(VolumeError::NameConflict(record.name.clone()));
                }
                records.insert(record.id.clone(), record.clone());
            }
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
        if let Some(repository) = &self.repository {
            repository
                .delete_volume(&record.id)
                .await
                .map_err(repository_error)?;
        }
        let backing_directory = record
            .backing_image_config
            .as_deref()
            .and_then(Path::parent)
            .map(Path::to_path_buf);
        self.records.write().await.remove(&record.id);
        if let Some(directory) = backing_directory {
            if let Err(error) = tokio::fs::remove_dir_all(&directory).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    warn!(
                        volume_id = %record.id,
                        path = %directory.display(),
                        %error,
                        "failed to clean deleted volume's node-local backing"
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn create_child(
        &self,
        reference: &str,
        name: String,
        mode: VolumeMode,
        size_mb: u64,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create(name, mode, Some(reference.to_owned()), None, size_mb)
            .await
    }

    pub(crate) async fn create_child_for_owner(
        &self,
        reference: &str,
        name: String,
        mode: VolumeMode,
        size_mb: u64,
        owner: &str,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_with_source_owner(
            name,
            mode,
            Some(reference.to_owned()),
            None,
            size_mb,
            Some(owner),
        )
        .await
    }

    pub(crate) async fn snapshot_volume(
        &self,
        reference: &str,
    ) -> Result<VolumeRecord, VolumeError> {
        let mut parent = self.get(reference).await?;
        // A running sandbox restacks its volume upper into this same local
        // image config before capture. Publish that latest state before making
        // the logical volume snapshot.
        let owner = parent.reserved_by_sandbox_id.clone();
        if let Some(owner) = owner.as_deref() {
            self.publish_owner_backings(owner).await?;
            parent = self.get(reference).await?;
        } else {
            self.persist(&mut parent).await?;
        }
        let name = format!("{}-snapshot-{}", parent.name, Uuid::now_v7().simple());
        self.create_with_source_owner(
            name,
            VolumeMode::Exclusive,
            Some(parent.id),
            None,
            parent.size_mb,
            owner.as_deref(),
        )
        .await
    }

    pub async fn reserve(&self, reference: &str, owner: &str) -> Result<(), VolumeError> {
        let mut record = self.get(reference).await?;
        match record.status {
            VolumeStatus::Uploading => return Err(VolumeError::Uploading(record.id.clone())),
            VolumeStatus::Failed => return Err(VolumeError::Failed(record.id.clone())),
            VolumeStatus::Ready => {}
        }
        if record.mode == VolumeMode::ReadOnly {
            if let Some(repository) = &self.repository {
                repository
                    .reserve_read_only_volume(&record.id, owner)
                    .await
                    .map_err(repository_error)?;
                if !record.read_only_mounts.iter().any(|entry| entry == owner) {
                    record.read_only_mounts.push(owner.to_owned());
                }
                self.cache_record(record).await;
                return Ok(());
            }
            let mut records = self.records.write().await;
            let current = records
                .get_mut(&record.id)
                .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))?;
            if !current.read_only_mounts.iter().any(|entry| entry == owner) {
                current.read_only_mounts.push(owner.to_owned());
            }
            return Ok(());
        }
        if let Some(repository) = &self.repository {
            if let Some(existing) = repository
                .reserve_volume(&record.id, owner)
                .await
                .map_err(repository_error)?
            {
                return Err(VolumeError::Reserved(existing));
            }
            record.reserved_by_sandbox_id = Some(owner.to_owned());
            self.cache_record(record).await;
            return Ok(());
        }
        let mut records = self.records.write().await;
        let current = records
            .get_mut(&record.id)
            .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))?;
        if let Some(existing) = current.reserved_by_sandbox_id.as_deref() {
            if existing == owner {
                return Ok(());
            }
            return Err(VolumeError::Reserved(existing.to_owned()));
        }
        current.reserved_by_sandbox_id = Some(owner.to_owned());
        Ok(())
    }

    pub async fn release_owner(&self, owner: &str) -> Result<(), VolumeError> {
        if let Some(repository) = &self.repository {
            repository
                .replace_volume_owner(owner, None)
                .await
                .map_err(repository_error)?;
        }
        let mut records = self.records.write().await;
        for record in records.values_mut() {
            record.replace_owner(owner, None);
        }
        Ok(())
    }

    /// Publishes the latest local backing for volumes still held by a paused
    /// sandbox without releasing its exclusive reservations.
    pub async fn publish_owner_backings(&self, owner: &str) -> Result<(), VolumeError> {
        for mut record in self.records_for_owner(owner).await? {
            if record.reserved_by_sandbox_id.as_deref() != Some(owner) {
                continue;
            }
            // Publish the state transition before uploading any writable
            // upper layer so other nodes cannot mount stale content.
            record.status = VolumeStatus::Uploading;
            if let Err(error) = self.persist_catalog(&record).await {
                record.status = VolumeStatus::Failed;
                let _ = self.persist_catalog(&record).await;
                self.cache_record(record).await;
                return Err(error);
            }
            self.cache_record(record.clone()).await;
            let previous_layers = record.backing_layers.clone();
            if let Err(error) = self.publish_backing(&mut record).await {
                record.status = VolumeStatus::Failed;
                let _ = self.persist_catalog(&record).await;
                self.cache_record(record).await;
                return Err(error);
            }
            if record.backing_layers != previous_layers {
                record.revision = record.revision.saturating_add(1);
            }
            record.status = VolumeStatus::Ready;
            if let Err(error) = self.persist_catalog(&record).await {
                record.status = VolumeStatus::Failed;
                let _ = self.persist_catalog(&record).await;
                self.cache_record(record).await;
                return Err(error);
            }
            self.cache_record(record).await;
        }
        Ok(())
    }

    pub async fn rebind_owner(&self, from: &str, to: &str) -> Result<(), VolumeError> {
        if let Some(repository) = &self.repository {
            repository
                .replace_volume_owner(from, Some(to))
                .await
                .map_err(repository_error)?;
        }
        let mut records = self.records.write().await;
        for record in records.values_mut() {
            record.replace_owner(from, Some(to));
        }
        Ok(())
    }

    async fn local_get(&self, reference: &str) -> Result<VolumeRecord, VolumeError> {
        let records = self.records.read().await;
        records
            .get(reference)
            .or_else(|| records.values().find(|record| record.name == reference))
            .cloned()
            .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))
    }

    async fn cache_record(&self, record: VolumeRecord) {
        self.records.write().await.insert(record.id.clone(), record);
    }

    async fn records_for_owner(&self, owner: &str) -> Result<Vec<VolumeRecord>, VolumeError> {
        if let Some(repository) = self.repository.as_ref() {
            let mut records = repository
                .list_volumes_by_owner(owner)
                .await
                .map_err(repository_error)?;
            let local = self.records.read().await;
            for record in &mut records {
                record.backing_image_config = current_local_backing(record, &local);
            }
            return Ok(records);
        }
        Ok(self
            .records
            .read()
            .await
            .values()
            .filter(|record| record.mounted_by(owner))
            .cloned()
            .collect())
    }

    async fn persist(&self, record: &mut VolumeRecord) -> Result<(), VolumeError> {
        self.publish_backing(record).await?;
        self.persist_catalog(record).await
    }

    async fn publish_backing(&self, record: &mut VolumeRecord) -> Result<(), VolumeError> {
        if let Some(repository) = &self.repository {
            if let Some(path) = record.backing_image_config.as_deref() {
                record.backing_layers = repository
                    .publish_volume_backing(&record.id, path)
                    .await
                    .map_err(repository_error)?;
            }
        }
        Ok(())
    }

    async fn persist_catalog(&self, record: &VolumeRecord) -> Result<(), VolumeError> {
        if let Some(repository) = &self.repository {
            repository
                .put_volume(record.clone())
                .await
                .map_err(repository_error)?;
        }
        Ok(())
    }

    async fn create_empty_backing(
        &self,
        volume_id: &str,
        size_mb: u64,
    ) -> Result<Option<PathBuf>, VolumeError> {
        let Some(root) = &self.root else {
            return Ok(None);
        };
        let directory = root.join("data").join(volume_id);
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
        result.map(Some)
    }

    async fn create_child_backing(
        &self,
        volume_id: &str,
        parent_config: &Path,
    ) -> Result<PathBuf, VolumeError> {
        let Some(root) = &self.root else {
            return Ok(parent_config.to_path_buf());
        };
        let directory = root.join("data").join(volume_id);
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

impl Default for VolumeManager {
    fn default() -> Self {
        Self::new()
    }
}

fn repository_error(error: RepositoryError) -> VolumeError {
    match error {
        RepositoryError::VolumeNotFound { lookup } => VolumeError::NotFound(lookup),
        RepositoryError::VolumeNameConflict { name } => VolumeError::NameConflict(name),
        error => VolumeError::Storage(error.to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::repository::backends::posixfs::{
        PosixFsArtifactStore, PosixFsCatalogStore, PosixFsSnapshotRepository,
    };
    use crate::snapshot::repository::VolumeRecordPage;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const DEFAULT_VOLUME_SIZE_MB: u64 = 16;

    fn posix_repository(root: &Path) -> Arc<dyn SnapshotRepository> {
        Arc::new(PosixFsSnapshotRepository::new(
            Arc::new(PosixFsCatalogStore::new(root.to_path_buf())),
            Arc::new(PosixFsArtifactStore::new(root.to_path_buf())),
        ))
    }

    async fn create_empty(manager: &VolumeManager, name: &str) -> VolumeRecord {
        manager
            .create(
                name.to_owned(),
                VolumeMode::Exclusive,
                None,
                None,
                DEFAULT_VOLUME_SIZE_MB,
            )
            .await
            .unwrap()
    }

    #[derive(Default)]
    struct CountingVolumeRepository {
        get_calls: AtomicUsize,
        page_calls: AtomicUsize,
        materialize_calls: AtomicUsize,
    }

    impl CountingVolumeRepository {
        fn unsupported<T>() -> crate::snapshot::RepositoryResult<T> {
            Err(RepositoryError::Unsupported {
                feature: "snapshot operation is not used by the volume test".to_string(),
            })
        }

        fn record(index: usize) -> VolumeRecord {
            VolumeRecord {
                id: format!("vol_{index:010}"),
                name: if index == 42 {
                    "target".to_string()
                } else {
                    format!("volume-{index:010}")
                },
                mode: VolumeMode::Exclusive,
                source: "empty".to_string(),
                parent_volume_id: None,
                revision: 1,
                size_mb: DEFAULT_VOLUME_SIZE_MB,
                status: VolumeStatus::Ready,
                reserved_by_sandbox_id: None,
                backing_image_config: None,
                backing_layers: vec![OverlaybdLayerRef::Managed(crate::snapshot::ManagedLayer {
                    digest: format!("sha256:{index:064x}"),
                    size: 4096,
                    uuid: None,
                })],
                read_only_mounts: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl SnapshotRepository for CountingVolumeRepository {
        async fn create(
            &self,
            _record: crate::snapshot::SnapshotRecord,
        ) -> crate::snapshot::RepositoryResult<crate::snapshot::SnapshotRecord> {
            Self::unsupported()
        }

        async fn publish(
            &self,
            _metadata: crate::snapshot::SnapshotPublishMetadata,
            _manifest: crate::sandbox::FirecrackerSnapshotManifest,
        ) -> crate::snapshot::RepositoryResult<crate::snapshot::SnapshotRecord> {
            Self::unsupported()
        }

        async fn get(
            &self,
            _id_or_alias: &str,
        ) -> crate::snapshot::RepositoryResult<Option<crate::snapshot::SnapshotRecord>> {
            Self::unsupported()
        }

        async fn list(
            &self,
            _filter: crate::snapshot::SnapshotListFilter,
        ) -> crate::snapshot::RepositoryResult<Vec<crate::snapshot::SnapshotRecord>> {
            Self::unsupported()
        }

        async fn delete(&self, _id_or_alias: &str) -> crate::snapshot::RepositoryResult<()> {
            Self::unsupported()
        }

        async fn resolve_alias(
            &self,
            _alias: &str,
        ) -> crate::snapshot::RepositoryResult<Option<crate::snapshot::SnapshotId>> {
            Self::unsupported()
        }

        async fn try_start_build(
            &self,
            _id: &crate::snapshot::SnapshotId,
        ) -> crate::snapshot::RepositoryResult<crate::snapshot::SnapshotRecord> {
            Self::unsupported()
        }

        async fn mark_build_error(
            &self,
            _id: &crate::snapshot::SnapshotId,
            _reason: crate::snapshot::TemplateBuildErrorReason,
        ) -> crate::snapshot::RepositoryResult<()> {
            Self::unsupported()
        }

        async fn get_volume(
            &self,
            reference: &str,
        ) -> crate::snapshot::RepositoryResult<Option<VolumeRecord>> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            Ok((reference == "target" || reference == "vol_0000000042").then(|| Self::record(42)))
        }

        async fn list_volumes_page(
            &self,
            after_volume_id: Option<&str>,
            limit: usize,
        ) -> crate::snapshot::RepositoryResult<VolumeRecordPage> {
            self.page_calls.fetch_add(1, Ordering::Relaxed);
            let start = after_volume_id
                .and_then(|value| value.strip_prefix("vol_"))
                .and_then(|value| value.parse::<usize>().ok())
                .map_or(0, |index| index + 1);
            let end = start.saturating_add(limit).min(1_000_000);
            let records: Vec<_> = (start..end).map(Self::record).collect();
            let next_volume_id = (end < 1_000_000)
                .then(|| records.last().map(|record| record.id.clone()))
                .flatten();
            Ok(VolumeRecordPage {
                records,
                next_volume_id,
            })
        }

        async fn materialize_volume_backing(
            &self,
            _volume_id: &str,
            _layers: &[OverlaybdLayerRef],
            destination: &Path,
        ) -> crate::snapshot::RepositoryResult<PathBuf> {
            self.materialize_calls.fetch_add(1, Ordering::Relaxed);
            tokio::fs::create_dir_all(destination.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(destination, b"{}").await.unwrap();
            Ok(destination.to_path_buf())
        }
    }

    #[tokio::test]
    async fn repository_access_is_keyed_bounded_and_lazy() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Arc::new(CountingVolumeRepository::default());
        let manager = VolumeManager::open_with_repository(
            directory.path().join("node/catalog"),
            Some(repository.clone()),
        )
        .await
        .unwrap();

        assert_eq!(repository.get_calls.load(Ordering::Relaxed), 0);
        assert_eq!(repository.page_calls.load(Ordering::Relaxed), 0);
        assert_eq!(repository.materialize_calls.load(Ordering::Relaxed), 0);
        assert!(manager.records.read().await.is_empty());

        let target = manager.get("target").await.unwrap();
        assert_eq!(target.id, "vol_0000000042");
        assert_eq!(repository.get_calls.load(Ordering::Relaxed), 1);
        assert_eq!(repository.materialize_calls.load(Ordering::Relaxed), 0);
        assert!(manager.records.read().await.is_empty());

        let page = manager.list_page(None, 37).await.unwrap();
        assert_eq!(page.records.len(), 37);
        assert!(page.next_token.is_some());
        assert_eq!(repository.page_calls.load(Ordering::Relaxed), 1);
        assert_eq!(repository.materialize_calls.load(Ordering::Relaxed), 0);
        assert!(manager.records.read().await.is_empty());

        let materialized = manager.materialize_backing("target").await.unwrap();
        assert!(materialized.backing_image_config.unwrap().is_file());
        assert_eq!(repository.materialize_calls.load(Ordering::Relaxed), 1);
        assert_eq!(manager.records.read().await.len(), 1);
    }

    #[tokio::test]
    async fn creates_and_resolves_by_id_or_name() {
        let manager = VolumeManager::new();
        let record = create_empty(&manager, "my-data").await;

        assert_eq!(manager.get("my-data").await.unwrap(), record);
        assert_eq!(manager.get(&record.id).await.unwrap(), record);
        assert_eq!(record.source, "empty");
    }

    #[tokio::test]
    async fn local_volume_pages_have_stable_opaque_cursors() {
        let manager = VolumeManager::new();
        for index in 0..205 {
            manager
                .create(
                    format!("volume-{index:03}"),
                    VolumeMode::ReadOnly,
                    None,
                    Some("image:latest".to_string()),
                    DEFAULT_VOLUME_SIZE_MB,
                )
                .await
                .unwrap();
        }

        let mut token = None;
        let mut ids = Vec::new();
        loop {
            let page = manager.list_page(token.as_deref(), 37).await.unwrap();
            assert!(page.records.len() <= 37);
            ids.extend(page.records.into_iter().map(|record| record.id));
            let Some(next) = page.next_token else {
                break;
            };
            token = Some(next);
        }
        assert_eq!(ids.len(), 205);
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 205);
        assert_eq!(
            manager.list_page(Some("not-a-token"), 10).await,
            Err(VolumeError::InvalidNextToken)
        );
    }

    #[tokio::test]
    async fn rejects_conflicting_names_and_sources() {
        let manager = VolumeManager::new();
        create_empty(&manager, "my-data").await;

        assert_eq!(
            manager
                .create(
                    "my-data".to_owned(),
                    VolumeMode::Exclusive,
                    None,
                    None,
                    DEFAULT_VOLUME_SIZE_MB,
                )
                .await,
            Err(VolumeError::NameConflict("my-data".to_owned()))
        );
        assert_eq!(
            manager
                .create(
                    "other".to_owned(),
                    VolumeMode::Exclusive,
                    Some("my-data".to_owned()),
                    Some("image:latest".to_owned()),
                    DEFAULT_VOLUME_SIZE_MB,
                )
                .await,
            Err(VolumeError::MultipleSources)
        );
    }

    #[tokio::test]
    async fn rejects_volume_larger_than_configured_limit() {
        let manager = VolumeManager::with_limits(VolumeLimits {
            max_size_mb: 16,
            max_mounts: 4,
        });
        assert_eq!(
            manager
                .create(
                    "too-large".to_owned(),
                    VolumeMode::Exclusive,
                    None,
                    None,
                    17,
                )
                .await,
            Err(VolumeError::SizeLimitExceeded { max_size_mb: 16 })
        );
    }

    #[tokio::test]
    async fn deleting_unknown_or_reserved_volume_fails() {
        let manager = VolumeManager::new();
        assert_eq!(
            manager.delete("missing").await,
            Err(VolumeError::NotFound("missing".to_owned()))
        );

        let mut record = create_empty(&manager, "my-data").await;
        record.reserved_by_sandbox_id = Some("sbx".to_owned());
        manager
            .records
            .write()
            .await
            .insert(record.id.clone(), record);
        assert_eq!(
            manager.delete("my-data").await,
            Err(VolumeError::Reserved("sbx".to_owned()))
        );
    }

    #[tokio::test]
    async fn exclusive_reservation_rebinds_and_releases() {
        let manager = VolumeManager::new();
        let record = create_empty(&manager, "my-data").await;

        manager.reserve(&record.id, "pending").await.unwrap();
        assert_eq!(
            manager.reserve(&record.id, "other").await,
            Err(VolumeError::Reserved("pending".to_owned()))
        );
        manager.rebind_owner("pending", "sandbox").await.unwrap();
        assert_eq!(
            manager
                .get(&record.id)
                .await
                .unwrap()
                .reserved_by_sandbox_id,
            Some("sandbox".to_owned())
        );
        manager.release_owner("sandbox").await.unwrap();
        assert_eq!(
            manager
                .get(&record.id)
                .await
                .unwrap()
                .reserved_by_sandbox_id,
            None
        );
    }

    #[tokio::test]
    async fn uploading_volume_is_not_mountable_until_sync_completes() {
        let manager = VolumeManager::new();
        let record = create_empty(&manager, "my-data").await;
        manager.reserve(&record.id, "sandbox").await.unwrap();
        {
            let mut records = manager.records.write().await;
            records.get_mut(&record.id).unwrap().status = VolumeStatus::Uploading;
        }

        assert_eq!(
            manager.reserve(&record.id, "other").await,
            Err(VolumeError::Uploading(record.id.clone()))
        );
        manager.publish_owner_backings("sandbox").await.unwrap();
        assert_eq!(
            manager.get(&record.id).await.unwrap().status,
            VolumeStatus::Ready
        );
    }

    #[tokio::test]
    async fn read_only_leases_prevent_delete_until_all_owners_release() {
        let manager = VolumeManager::new();
        let record = manager
            .create(
                "shared".to_owned(),
                VolumeMode::ReadOnly,
                None,
                Some("image:latest".to_owned()),
                DEFAULT_VOLUME_SIZE_MB,
            )
            .await
            .unwrap();
        manager.reserve(&record.id, "sandbox-a").await.unwrap();
        manager.reserve(&record.id, "sandbox-b").await.unwrap();
        assert!(matches!(
            manager.delete(&record.id).await,
            Err(VolumeError::Reserved(_))
        ));
        manager.release_owner("sandbox-a").await.unwrap();
        assert!(matches!(
            manager.delete(&record.id).await,
            Err(VolumeError::Reserved(_))
        ));
        manager.release_owner("sandbox-b").await.unwrap();
        manager.delete(&record.id).await.unwrap();
    }

    #[tokio::test]
    async fn rebind_transfers_read_only_lease() {
        let manager = VolumeManager::new();
        let record = manager
            .create(
                "shared".to_owned(),
                VolumeMode::ReadOnly,
                None,
                Some("image:latest".to_owned()),
                DEFAULT_VOLUME_SIZE_MB,
            )
            .await
            .unwrap();

        manager.reserve(&record.id, "pending").await.unwrap();
        manager.rebind_owner("pending", "sandbox").await.unwrap();

        let record = manager.get(&record.id).await.unwrap();
        assert_eq!(record.read_only_mounts, vec!["sandbox"]);
        manager.release_owner("sandbox").await.unwrap();
        assert!(manager
            .get(&record.id)
            .await
            .unwrap()
            .read_only_mounts
            .is_empty());
    }

    #[tokio::test]
    async fn failed_volume_is_not_mountable() {
        let manager = VolumeManager::new();
        let record = create_empty(&manager, "failed").await;
        manager
            .records
            .write()
            .await
            .get_mut(&record.id)
            .unwrap()
            .status = VolumeStatus::Failed;
        assert_eq!(
            manager.reserve(&record.id, "sandbox").await,
            Err(VolumeError::Failed(record.id))
        );
    }

    #[tokio::test]
    async fn volume_child_operation_preserves_parent_and_size() {
        let manager = VolumeManager::new();
        let parent = create_empty(&manager, "parent").await;

        let child = manager
            .create_child(
                &parent.id,
                "child".to_owned(),
                VolumeMode::Exclusive,
                parent.size_mb,
            )
            .await
            .unwrap();

        assert_eq!(child.parent_volume_id, Some(parent.id.clone()));
        assert_eq!(child.mode, VolumeMode::Exclusive);
        assert_eq!(child.size_mb, parent.size_mb);
    }

    #[tokio::test]
    async fn direct_child_operations_reject_reserved_source() {
        let manager = VolumeManager::new();
        let parent = create_empty(&manager, "parent").await;
        manager.reserve(&parent.id, "sandbox").await.unwrap();

        assert_eq!(
            manager
                .create_child(
                    &parent.id,
                    "child".to_owned(),
                    VolumeMode::Exclusive,
                    parent.size_mb,
                )
                .await,
            Err(VolumeError::Reserved("sandbox".to_owned()))
        );
        assert_eq!(
            manager
                .create_child_for_owner(
                    &parent.id,
                    "wrong-owner-child".to_owned(),
                    VolumeMode::Exclusive,
                    parent.size_mb,
                    "other",
                )
                .await,
            Err(VolumeError::Reserved("sandbox".to_owned()))
        );
        let child = manager
            .create_child_for_owner(
                &parent.id,
                "owner-child".to_owned(),
                VolumeMode::Exclusive,
                parent.size_mb,
                "sandbox",
            )
            .await
            .unwrap();
        assert_eq!(child.parent_volume_id, Some(parent.id));
    }

    #[tokio::test]
    async fn sandbox_volume_snapshot_is_writable_child() {
        let manager = VolumeManager::new();
        let parent = create_empty(&manager, "parent").await;
        manager.reserve(&parent.id, "sandbox").await.unwrap();

        let snapshot = manager.snapshot_volume(&parent.id).await.unwrap();
        assert_eq!(snapshot.parent_volume_id, Some(parent.id));
        assert_eq!(snapshot.mode, VolumeMode::Exclusive);
        assert!(snapshot.name.starts_with("parent-snapshot-"));
    }

    #[tokio::test]
    async fn shared_posix_repository_reloads_catalog_and_enforces_owner() {
        let directory = tempfile::tempdir().unwrap();
        let repository = posix_repository(&directory.path().join("repository"));
        let first = VolumeManager::open_with_repository(
            directory.path().join("node-a/catalog"),
            Some(Arc::clone(&repository)),
        )
        .await
        .unwrap();
        let second = VolumeManager::open_with_repository(
            directory.path().join("node-b/catalog"),
            Some(Arc::clone(&repository)),
        )
        .await
        .unwrap();
        let record = first
            .create(
                "shared".to_owned(),
                VolumeMode::Exclusive,
                None,
                Some("oci://example/data".to_owned()),
                DEFAULT_VOLUME_SIZE_MB,
            )
            .await
            .unwrap();
        drop(first);
        assert_eq!(
            second
                .list_page(None, DEFAULT_VOLUME_PAGE_SIZE)
                .await
                .unwrap()
                .records[0]
                .id,
            record.id
        );
        second.reserve(&record.id, "sandbox-b").await.unwrap();

        let first_again = VolumeManager::open_with_repository(
            directory.path().join("node-a/catalog"),
            Some(repository),
        )
        .await
        .unwrap();
        assert_eq!(
            first_again.reserve(&record.id, "sandbox-a").await,
            Err(VolumeError::Reserved("sandbox-b".to_owned()))
        );
    }

    #[tokio::test]
    async fn persistent_empty_volume_has_overlaybd_backing() {
        let directory = tempfile::tempdir().unwrap();
        let manager = VolumeManager::open(directory.path().join("catalog"))
            .await
            .unwrap();
        let record = create_empty(&manager, "my-data").await;

        let config_path = record.backing_image_config.expect("empty backing config");
        let config: ImageConfig = serde_json::from_slice(
            &tokio::fs::read(&config_path)
                .await
                .expect("read image config"),
        )
        .expect("parse image config");
        assert_eq!(config.lowers.len(), 1);
        assert!(Path::new(&config.lowers[0].file).is_file());
        let backing_directory = config_path.parent().unwrap().to_path_buf();
        manager.delete(&record.id).await.unwrap();
        assert!(!backing_directory.exists());

        let record = create_empty(&manager, "cleanup-failure").await;
        let non_directory = directory.path().join("not-a-directory");
        tokio::fs::write(&non_directory, b"test").await.unwrap();
        manager
            .records
            .write()
            .await
            .get_mut(&record.id)
            .unwrap()
            .backing_image_config = Some(non_directory.join("image.json"));
        manager.delete(&record.id).await.unwrap();
        assert_eq!(
            manager.get(&record.id).await,
            Err(VolumeError::NotFound(record.id))
        );
    }

    #[tokio::test]
    async fn volume_child_gets_its_own_config_without_deleting_parent_backing() {
        let directory = tempfile::tempdir().unwrap();
        let manager = VolumeManager::open(directory.path().join("catalog"))
            .await
            .unwrap();
        let parent = create_empty(&manager, "parent").await;
        let child = manager
            .create(
                "child".to_owned(),
                VolumeMode::Exclusive,
                Some(parent.id.clone()),
                None,
                DEFAULT_VOLUME_SIZE_MB,
            )
            .await
            .unwrap();

        assert_ne!(child.backing_image_config, parent.backing_image_config);
        let parent_config = parent.backing_image_config.unwrap();
        manager.delete(&child.id).await.unwrap();
        assert!(parent_config.exists());
    }

    #[tokio::test]
    async fn volume_child_survives_parent_backing_deletion() {
        let directory = tempfile::tempdir().unwrap();
        let repository = posix_repository(&directory.path().join("repository"));
        let manager = VolumeManager::open_with_repository(
            directory.path().join("node/catalog"),
            Some(repository),
        )
        .await
        .unwrap();
        let parent = create_empty(&manager, "parent").await;
        let child = manager
            .create(
                "child".to_owned(),
                VolumeMode::Exclusive,
                Some(parent.id.clone()),
                None,
                DEFAULT_VOLUME_SIZE_MB,
            )
            .await
            .unwrap();

        manager.delete(&parent.id).await.unwrap();

        let child_config: ImageConfig = serde_json::from_slice(
            &tokio::fs::read(child.backing_image_config.unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(child_config
            .lowers
            .iter()
            .all(|layer| layer.file.is_empty() || Path::new(&layer.file).is_file()));
        manager.reserve(&child.id, "sandbox").await.unwrap();
    }
}
