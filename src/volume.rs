use overlaybd::config::{ImageConfig, LayerConfig};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::process::Command;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::snapshot::repository::{RepositoryError, SnapshotRepository};
use crate::snapshot::OverlaybdLayerRef;

pub const DEFAULT_VOLUME_SIZE_MB: u64 = 64 * 1024;
const BYTES_PER_MB: u64 = 1024 * 1024;

fn default_volume_size_mb() -> u64 {
    // Keep catalogs written before sizeMB was introduced at their original
    // 1 GiB runtime size. New volumes use DEFAULT_VOLUME_SIZE_MB instead.
    1024
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
    #[serde(default = "default_volume_size_mb")]
    pub size_mb: u64,
    #[serde(default)]
    pub status: VolumeStatus,
    pub reserved_by_sandbox_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backing_image_config: Option<PathBuf>,
    /// Repository-owned logical layers for the volume backing. The image config
    /// path above is only a node-local runtime cache and is never required for
    /// another node to reopen the volume.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backing_layers: Vec<OverlaybdLayerRef>,
}

fn current_local_backing(remote: &VolumeRecord, local: &[VolumeRecord]) -> Option<PathBuf> {
    local
        .iter()
        .find(|record| record.id == remote.id && record.backing_layers == remote.backing_layers)
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
    #[error("fromVolume and image cannot be used together")]
    MultipleSources,
    #[error("volume size must be greater than zero")]
    InvalidSize,
    #[error("volume child size must match its source size")]
    SizeMismatch,
    #[error("source volume not found: {0}")]
    SourceNotFound(String),
    #[error("volume catalog storage failed: {0}")]
    Storage(String),
}

#[derive(Clone, Default)]
pub struct VolumeManager {
    records: Arc<RwLock<Vec<VolumeRecord>>>,
    root: Option<PathBuf>,
    repository: Option<Arc<dyn SnapshotRepository>>,
    publication_lock: Arc<Mutex<()>>,
}

impl VolumeManager {
    pub fn new() -> Self {
        Self {
            records: Arc::new(RwLock::new(Vec::new())),
            root: None,
            repository: None,
            publication_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn open(path: impl Into<std::path::PathBuf>) -> anyhow::Result<Self> {
        Self::open_with_repository(path, None).await
    }

    pub async fn open_with_repository(
        path: impl Into<std::path::PathBuf>,
        repository: Option<Arc<dyn SnapshotRepository>>,
    ) -> anyhow::Result<Self> {
        let path = path.into();
        let root = path.parent().map(Path::to_path_buf);
        let manager = Self {
            records: Arc::new(RwLock::new(Vec::new())),
            root,
            repository,
            publication_lock: Arc::new(Mutex::new(())),
        };
        manager.refresh_repository().await?;
        Ok(manager)
    }

    async fn refresh_repository(&self) -> Result<(), VolumeError> {
        let Some(repository) = self.repository.as_ref() else {
            return Ok(());
        };
        let remote_records = match repository.list_volumes().await {
            Ok(records) => records,
            Err(RepositoryError::Unsupported { .. }) => return Ok(()),
            Err(error) => return Err(repository_error(error)),
        };
        self.replace_with_remote(remote_records).await
    }

    async fn replace_with_remote(
        &self,
        mut remote_records: Vec<VolumeRecord>,
    ) -> Result<(), VolumeError> {
        let mut records = self.records.write().await;
        let local_records = records.clone();
        for remote in &mut remote_records {
            remote.backing_image_config = current_local_backing(remote, &local_records);
            if remote.backing_image_config.is_none() && !remote.backing_layers.is_empty() {
                let Some(root) = self.root.as_ref() else {
                    continue;
                };
                let destination = root.join("data").join(&remote.id).join("image.json");
                match self
                    .repository
                    .as_ref()
                    .expect("remote records require a repository")
                    .materialize_volume_backing(&remote.id, &remote.backing_layers, &destination)
                    .await
                {
                    Ok(path) => remote.backing_image_config = Some(path),
                    Err(RepositoryError::Unsupported { .. }) => {}
                    Err(error) => return Err(repository_error(error)),
                }
            }
        }
        *records = remote_records;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<VolumeRecord>, VolumeError> {
        self.refresh_repository().await?;
        let mut records = self.records.read().await.clone();
        records.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(records)
    }

    pub async fn get(&self, reference: &str) -> Result<VolumeRecord, VolumeError> {
        self.refresh_repository().await?;
        self.records
            .read()
            .await
            .iter()
            .find(|record| record.id == reference || record.name == reference)
            .cloned()
            .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))
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
        self.refresh_repository().await?;
        let mut records = self.records.write().await;
        let index = records
            .iter()
            .position(|record| record.id == reference || record.name == reference)
            .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))?;
        if let Some(path) = records[index].backing_image_config.clone() {
            return Ok(path);
        }
        let Some(directory) = self.data_dir(&records[index].id) else {
            return Ok(source_config.to_path_buf());
        };
        let target = directory.join("image.json");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;
        tokio::fs::copy(source_config, &target)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;
        records[index].backing_image_config = Some(target.clone());
        self.persist(&mut records[index]).await?;
        Ok(target)
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
        self.refresh_repository().await?;
        validate_name(&name)?;
        if size_mb == 0 {
            return Err(VolumeError::InvalidSize);
        }
        if from_volume.is_some() && image.is_some() {
            return Err(VolumeError::MultipleSources);
        }

        let mut records = self.records.write().await;
        if records.iter().any(|record| record.name == name) {
            return Err(VolumeError::NameConflict(name));
        }

        let id = format!("vol_{}", Uuid::now_v7().simple());
        let (source, parent_volume_id, revision, backing_image_config) =
            if let Some(reference) = from_volume {
                let parent = records
                    .iter()
                    .find(|record| record.id == reference || record.name == reference)
                    .ok_or_else(|| VolumeError::SourceNotFound(reference.clone()))?;
                if parent.status == VolumeStatus::Uploading {
                    return Err(VolumeError::Uploading(parent.id.clone()));
                }
                if let Some(owner) = parent.reserved_by_sandbox_id.as_deref() {
                    if source_owner != Some(owner) {
                        return Err(VolumeError::Reserved(owner.to_owned()));
                    }
                }
                if parent.size_mb != size_mb {
                    return Err(VolumeError::SizeMismatch);
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
        };
        if let Err(error) = self.persist(&mut record).await {
            if let Some(config) = record.backing_image_config.as_ref() {
                if let Some(directory) = config.parent() {
                    let _ = tokio::fs::remove_dir_all(directory).await;
                }
            }
            return Err(error);
        }
        records.push(record.clone());
        Ok(record)
    }

    pub async fn delete(&self, reference: &str) -> Result<(), VolumeError> {
        self.refresh_repository().await?;
        let mut records = self.records.write().await;
        let Some(index) = records
            .iter()
            .position(|record| record.id == reference || record.name == reference)
        else {
            return Err(VolumeError::NotFound(reference.to_owned()));
        };
        if let Some(owner) = records[index].reserved_by_sandbox_id.as_deref() {
            return Err(VolumeError::Reserved(owner.to_owned()));
        }
        if let Some(repository) = &self.repository {
            match repository.delete_volume(&records[index].id).await {
                Ok(()) | Err(RepositoryError::Unsupported { .. }) => {}
                Err(error) => return Err(repository_error(error)),
            }
        }
        if let Some(config) = records[index].backing_image_config.as_ref() {
            if let Some(directory) = config.parent() {
                if let Err(error) = tokio::fs::remove_dir_all(directory).await {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        return Err(VolumeError::Storage(error.to_string()));
                    }
                }
            }
        }
        records.remove(index);
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
        self.refresh_repository().await?;
        let mut records = self.records.write().await;
        let index = records
            .iter()
            .position(|record| record.id == reference || record.name == reference)
            .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))?;
        if records[index].status == VolumeStatus::Uploading {
            return Err(VolumeError::Uploading(records[index].id.clone()));
        }
        if records[index].mode == VolumeMode::ReadOnly {
            return Ok(());
        }
        if let Some(existing) = records[index].reserved_by_sandbox_id.as_deref() {
            if existing != owner {
                return Err(VolumeError::Reserved(existing.to_owned()));
            }
            return Ok(());
        }
        let repository_updated = if let Some(repository) = &self.repository {
            match repository.reserve_volume(&records[index].id, owner).await {
                Ok(Some(existing)) => return Err(VolumeError::Reserved(existing)),
                Ok(None) => true,
                Err(RepositoryError::Unsupported { .. }) => false,
                Err(error) => return Err(repository_error(error)),
            }
        } else {
            false
        };
        records[index].reserved_by_sandbox_id = Some(owner.to_owned());
        if !repository_updated {
            let result = self.persist_catalog(&records[index]).await;
            if result.is_ok() {
                return Ok(());
            }
            records[index].reserved_by_sandbox_id = None;
            return result;
        }
        Ok(())
    }

    pub async fn release_owner(&self, owner: &str) -> Result<(), VolumeError> {
        self.refresh_repository().await?;
        let repository_updated = if let Some(repository) = &self.repository {
            match repository.replace_volume_owner(owner, None).await {
                Ok(_) => true,
                Err(RepositoryError::Unsupported { .. }) => false,
                Err(error) => return Err(repository_error(error)),
            }
        } else {
            false
        };
        let mut records = self.records.write().await;
        for record in records.iter_mut() {
            if record.reserved_by_sandbox_id.as_deref() == Some(owner) {
                record.reserved_by_sandbox_id = None;
                if !repository_updated {
                    self.persist_catalog(record).await?;
                }
            }
        }
        Ok(())
    }

    /// Publishes the latest local backing for volumes still held by a paused
    /// sandbox without releasing its exclusive reservations.
    pub async fn publish_owner_backings(&self, owner: &str) -> Result<(), VolumeError> {
        let _publication_guard = self.publication_lock.lock().await;
        self.refresh_repository().await?;
        let mut records = self.records.write().await;
        for record in records.iter_mut() {
            if record.reserved_by_sandbox_id.as_deref() == Some(owner) {
                // Publish the state transition before uploading any writable
                // upper layer so other nodes cannot mount stale content.
                record.status = VolumeStatus::Uploading;
                self.persist_catalog(record).await?;
                let previous_layers = record.backing_layers.clone();
                self.publish_backing(record).await?;
                if record.backing_layers != previous_layers {
                    record.revision = record.revision.saturating_add(1);
                }
                record.status = VolumeStatus::Ready;
                self.persist_catalog(record).await?;
            }
        }
        Ok(())
    }

    pub async fn rebind_owner(&self, from: &str, to: &str) -> Result<(), VolumeError> {
        self.refresh_repository().await?;
        let repository_updated = if let Some(repository) = &self.repository {
            match repository.replace_volume_owner(from, Some(to)).await {
                Ok(Some(existing)) => return Err(VolumeError::Reserved(existing)),
                Ok(None) => true,
                Err(RepositoryError::Unsupported { .. }) => false,
                Err(error) => return Err(repository_error(error)),
            }
        } else {
            false
        };
        let mut records = self.records.write().await;
        for record in records.iter_mut() {
            if record.reserved_by_sandbox_id.as_deref() == Some(from) {
                record.reserved_by_sandbox_id = Some(to.to_owned());
                if !repository_updated {
                    self.persist_catalog(record).await?;
                }
            }
        }
        Ok(())
    }

    async fn persist(&self, record: &mut VolumeRecord) -> Result<(), VolumeError> {
        self.publish_backing(record).await?;
        self.persist_catalog(record).await
    }

    async fn publish_backing(&self, record: &mut VolumeRecord) -> Result<(), VolumeError> {
        if let Some(repository) = &self.repository {
            if let Some(path) = record.backing_image_config.as_deref() {
                match repository.publish_volume_backing(&record.id, path).await {
                    Ok(layers) => record.backing_layers = layers,
                    Err(RepositoryError::Unsupported { .. }) => {}
                    Err(error) => return Err(repository_error(error)),
                }
            }
        }
        Ok(())
    }

    async fn persist_catalog(&self, record: &VolumeRecord) -> Result<(), VolumeError> {
        if let Some(repository) = &self.repository {
            match repository.put_volume(record.clone()).await {
                Ok(()) | Err(RepositoryError::Unsupported { .. }) => {}
                Err(error) => return Err(repository_error(error)),
            }
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

fn repository_error(error: RepositoryError) -> VolumeError {
    VolumeError::Storage(error.to_string())
}

fn validate_name(name: &str) -> Result<(), VolumeError> {
    if !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        Ok(())
    } else {
        Err(VolumeError::InvalidName)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::repository::backends::posixfs::{
        PosixFsArtifactStore, PosixFsCatalogStore, PosixFsSnapshotRepository,
    };

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

    #[tokio::test]
    async fn creates_and_resolves_by_id_or_name() {
        let manager = VolumeManager::new();
        let record = create_empty(&manager, "my-data").await;

        assert_eq!(manager.get("my-data").await.unwrap(), record);
        assert_eq!(manager.get(&record.id).await.unwrap(), record);
        assert_eq!(record.source, "empty");
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
    async fn deleting_unknown_or_reserved_volume_fails() {
        let manager = VolumeManager::new();
        assert_eq!(
            manager.delete("missing").await,
            Err(VolumeError::NotFound("missing".to_owned()))
        );

        let mut record = create_empty(&manager, "my-data").await;
        record.reserved_by_sandbox_id = Some("sbx".to_owned());
        *manager.records.write().await = vec![record];
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
            records[0].status = VolumeStatus::Uploading;
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
        assert_eq!(second.list().await.unwrap()[0].id, record.id);
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
