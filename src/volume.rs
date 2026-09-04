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

use crate::snapshot::repository::{RepositoryError, SnapshotRepository};
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
    pub size_mb: u64,
    pub status: VolumeStatus,
    pub reserved_by_sandbox_id: Option<String>,
    #[serde(skip)]
    pub backing_image_config: Option<PathBuf>,
    /// Repository-owned layers; the image config path is only a node-local cache.
    pub backing_layers: Vec<OverlaybdLayerRef>,
    pub read_only_mounts: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) deleting: bool,
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

    pub(crate) fn validate_catalog_update(&self, next: &Self) -> Result<(), String> {
        if self.deleting {
            return Err(format!("volume '{}' is being deleted", self.id));
        }
        if self.name != next.name || self.mode != next.mode || self.size_mb != next.size_mb {
            return Err("volume identity fields cannot be changed".to_owned());
        }
        if self.backing_layers != next.backing_layers && self.status != VolumeStatus::Uploading {
            return Err(format!(
                "volume '{}' must enter uploading state before replacing its backing",
                self.id
            ));
        }
        if self.status == VolumeStatus::Failed && next.status == VolumeStatus::Ready {
            return Err(format!(
                "volume '{}' must enter uploading state before becoming ready",
                self.id
            ));
        }
        Ok(())
    }
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
    repository: Arc<dyn SnapshotRepository>,
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
    pub async fn open_with_repository(
        path: impl Into<std::path::PathBuf>,
        repository: Arc<dyn SnapshotRepository>,
    ) -> anyhow::Result<Self> {
        Self::open_with_repository_and_limits(path, repository, VolumeLimits::default()).await
    }

    pub async fn open_with_repository_and_limits(
        path: impl Into<std::path::PathBuf>,
        repository: Arc<dyn SnapshotRepository>,
        mut limits: VolumeLimits,
    ) -> anyhow::Result<Self> {
        let path = path.into();
        let root = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("volume catalog path has no parent"))?
            .to_path_buf();
        limits.max_mounts = limits.max_mounts.min(MAX_VOLUME_MOUNTS);
        Ok(Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            root,
            repository,
            limits,
        })
    }

    pub fn limits(&self) -> VolumeLimits {
        self.limits
    }

    pub async fn get(&self, reference: &str) -> Result<VolumeRecord, VolumeError> {
        let mut record = self
            .repository
            .get_volume(reference)
            .await
            .map_err(repository_error)?
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
        let mut page = self
            .repository
            .list_volumes_page(after_volume_id.as_deref(), limit)
            .await
            .map_err(repository_error)?;
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
        let path = self
            .repository
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
        source_config: Option<PathBuf>,
        size_mb: u64,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_with_source_owner(
            name,
            mode,
            from_volume.map(|reference| (reference, None)),
            source_config,
            size_mb,
            None,
        )
        .await
    }

    async fn create_with_source_owner(
        &self,
        name: String,
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
            if parent.backing_image_config.is_none() && !parent.backing_layers.is_empty() {
                parent = self.materialize_backing(&parent.id).await?;
            }
            match parent.backing_image_config.as_ref() {
                Some(path) => Some(self.create_child_backing(&id, path).await?),
                None => None,
            }
        } else if let Some(source_config) = source_config {
            Some(self.create_child_backing(&id, &source_config).await?)
        } else {
            Some(self.create_empty_backing(&id, size_mb).await?)
        };

        let mut record = VolumeRecord {
            id,
            name,
            mode,
            size_mb,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: reserved_owner.map(str::to_owned),
            backing_image_config,
            backing_layers: Vec::new(),
            read_only_mounts: Vec::new(),
            deleting: false,
        };
        let create_result = async {
            if reserved_owner.is_none() {
                self.publish_backing(&mut record).await?;
            }
            self.repository
                .create_volume(record.clone())
                .await
                .map_err(repository_error)?;
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
        self.repository
            .delete_volume(&record.id)
            .await
            .map_err(repository_error)?;
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
        backing_layers: Vec<OverlaybdLayerRef>,
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
            mode,
            size_mb,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_image_config: None,
            backing_layers,
            read_only_mounts: Vec::new(),
            deleting: false,
        };
        self.repository
            .create_volume(record.clone())
            .await
            .map_err(repository_error)?;
        self.cache_record(record.clone()).await;
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
            self.publish_backing(&mut parent).await?;
            self.persist_catalog(&parent).await?;
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
            self.repository
                .reserve_read_only_volume(&record.id, owner)
                .await
                .map_err(repository_error)?;
            if !record.read_only_mounts.iter().any(|entry| entry == owner) {
                record.read_only_mounts.push(owner.to_owned());
            }
            self.cache_record(record).await;
            return Ok(());
        }
        if let Some(existing) = self
            .repository
            .reserve_volume(&record.id, owner)
            .await
            .map_err(repository_error)?
        {
            return Err(VolumeError::Reserved(existing));
        }
        record.reserved_by_sandbox_id = Some(owner.to_owned());
        self.cache_record(record).await;
        Ok(())
    }

    pub async fn replace_owner_for(
        &self,
        owner: &str,
        new_owner: Option<&str>,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        for volume_id in volume_ids {
            self.repository
                .replace_volume_owner_for(volume_id, owner, new_owner)
                .await
                .map_err(repository_error)?;
            if let Some(record) = self.records.write().await.get_mut(volume_id) {
                record.replace_owner(owner, new_owner);
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
                    let _ = self.persist_catalog(&record).await;
                    self.cache_record(record.clone()).await;
                    return Err(VolumeError::Storage(format!(
                        "local backing for reserved volume '{}' is missing",
                        record.id
                    )));
                }
                record.backing_image_config = Some(path);
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
            if let Err(error) = self.persist_catalog(&record).await {
                record.status = VolumeStatus::Failed;
                let _ = self.persist_catalog(&record).await;
                self.cache_record(record).await;
                return Err(error);
            }
            self.cache_record(record.clone()).await;
            if let Err(error) = self.publish_backing(&mut record).await {
                record.status = VolumeStatus::Failed;
                let _ = self.persist_catalog(&record).await;
                self.cache_record(record).await;
                return Err(error);
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

    async fn cache_record(&self, record: VolumeRecord) {
        self.records.write().await.insert(record.id.clone(), record);
    }

    async fn publish_backing(&self, record: &mut VolumeRecord) -> Result<(), VolumeError> {
        if let Some(path) = record.backing_image_config.as_deref() {
            record.backing_layers = self
                .repository
                .publish_volume_backing(&record.id, path)
                .await
                .map_err(repository_error)?;
        }
        Ok(())
    }

    async fn persist_catalog(&self, record: &VolumeRecord) -> Result<(), VolumeError> {
        self.repository
            .put_volume(record.clone())
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
    use super::{VolumeManager, VolumeMode};
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::{ManagedLayer, OverlaybdLayerRef};

    #[tokio::test]
    async fn create_from_snapshot_preserves_read_only_mode() {
        let temp = tempfile::tempdir().expect("temporary volume repository");
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: temp.path().join("repository"),
            cache_root: Some(temp.path().join("cache")),
            runtime_cache_root: Some(temp.path().join("runtime")),
        })
        .expect("POSIX snapshot backend");
        let manager = VolumeManager::open_with_repository(
            temp.path().join("volumes/catalog"),
            backend.repository(),
        )
        .await
        .expect("volume manager");

        let restored = manager
            .create_from_snapshot(
                "restored-read-only".to_owned(),
                VolumeMode::ReadOnly,
                1024,
                vec![OverlaybdLayerRef::Managed(ManagedLayer {
                    digest: "sha256:abc".to_owned(),
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
