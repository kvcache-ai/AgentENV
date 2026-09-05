use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::layout::PosixFsSnapshotArtifactLayout;
use crate::snapshot::repository::{SnapshotListFilter, VolumeRecordPage};
use crate::snapshot::{
    CommittedSnapshot, RepositoryError, RepositoryResult, SnapshotAlias, SnapshotId,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
    SnapshotSourceKind, TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
use crate::volume::{is_valid_volume_component, VolumeMode, VolumeRecord};
const FILE_LOCK_TIMEOUT: Option<Duration> = Some(Duration::from_secs(10));

pub struct PosixFsCatalogStore {
    root: PathBuf,
}

#[derive(Debug)]
pub(crate) struct PublishSession {
    pub(crate) snapshot_id: SnapshotId,
}

#[derive(Debug)]
struct PosixFileLockGuard {
    _file: Flock<fs::File>,
}

impl PosixFsCatalogStore {
    /// Creates a catalog store rooted at the repository's durable POSIX directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn layout(&self, snapshot_id: &SnapshotId) -> PosixFsSnapshotArtifactLayout {
        PosixFsSnapshotArtifactLayout::new(&self.root, snapshot_id)
    }

    fn commit_marker_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.layout(snapshot_id)
            .path(super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER)
    }

    fn record_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        PosixFsSnapshotArtifactLayout::record_path(&self.root, snapshot_id)
    }

    /// Starts a publish session by creating the snapshot directory under the durable catalog root.
    pub(crate) fn begin_publish(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<PublishSession> {
        self.ensure_layout()?;
        let snapshot_dir = self.layout(snapshot_id).snapshot_dir();
        fs::create_dir_all(&snapshot_dir).map_err(|error| {
            RepositoryError::backend(
                format!("create snapshot dir '{}'", snapshot_dir.display()),
                error,
            )
        })?;
        Ok(PublishSession {
            snapshot_id: snapshot_id.clone(),
        })
    }

    /// Commits one imported snapshot into the catalog and makes it visible via the commit marker.
    ///
    /// Flow:
    /// 1. acquire the alias lock when an alias is present
    /// 2. bind the alias
    /// 3. write the commit marker
    /// 4. write the committed snapshot record
    pub(crate) fn commit_publish(
        &self,
        session: &PublishSession,
        metadata: SnapshotPublishMetadata,
        committed: CommittedSnapshot,
    ) -> RepositoryResult<SnapshotRecord> {
        let now = now_unix_ms();
        let snapshot_id = metadata.id.clone();
        let write_result = if let Some(alias) = metadata.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let record = store.committed_record_unlocked(&metadata, committed.clone(), now)?;
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                if let Some(existing) = store.load_alias_target(alias)? {
                    if existing != snapshot_id {
                        if store.load_record_by_id_unlocked(&existing)?.is_some() {
                            return Err(RepositoryError::AliasConflict {
                                alias: alias.to_string(),
                                existing,
                                new_id: snapshot_id.clone(),
                            });
                        }
                        store.remove_file_if_exists(&alias_path)?;
                    }
                }
                store.write_json(&alias_path, &snapshot_id)?;
                store.write_commit_marker(&session.snapshot_id)?;
                store.write_committed_record_unlocked(&record)?;
                Ok(record)
            })
        } else {
            (|| {
                let record = self.committed_record_unlocked(&metadata, committed.clone(), now)?;
                self.write_commit_marker(&session.snapshot_id)?;
                self.write_committed_record_unlocked(&record)?;
                Ok(record)
            })()
        };

        match write_result {
            Ok(record) => Ok(record),
            Err(error) => {
                if let Some(alias) = metadata.alias.as_ref() {
                    let _ = self.with_alias_lock(alias, |store| {
                        let alias_path =
                            PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                        if store.load_alias_target(alias)?.as_ref() == Some(&snapshot_id) {
                            store.remove_file_if_exists(&alias_path)?;
                        }
                        Ok(())
                    });
                }
                let _ = self.cleanup_uncommitted_snapshot_dir(&session.snapshot_id);
                Err(error)
            }
        }
    }

    /// Cleans up an unfinished publish session that never reached the committed marker.
    pub(crate) fn abort_publish(&self, session: &PublishSession) -> RepositoryResult<()> {
        self.cleanup_uncommitted_snapshot_dir(&session.snapshot_id)
    }

    pub(crate) fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.ensure_layout()?;
        if !matches!(record.source, SnapshotSource::Template { .. }) {
            return Err(RepositoryError::InvalidRequest {
                reason: "only template snapshots can be pre-created".to_string(),
            });
        }
        if record.committed.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "pre-created template snapshots must not already be committed".to_string(),
            });
        }
        if self.load_record_by_id_unlocked(&record.id)?.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }

        if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                store.ensure_alias_available(alias, &record.id)?;
                store.write_record_unlocked(&record)?;
                store.write_json(
                    &PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias),
                    &record.id,
                )
            })?;
        } else {
            self.write_record_unlocked(&record)?;
        }
        Ok(record)
    }

    pub(crate) fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.ensure_layout()?;
        if let Ok(direct_id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.load_record_by_id_unlocked(&direct_id)? {
                return Ok(Some(record));
            }
        }

        let alias =
            SnapshotAlias::parse(id_or_alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.with_alias_lock(&alias, |store| {
            let Some(id) = store.load_alias_target(&alias)? else {
                return Ok(None);
            };
            match store.load_record_by_id_unlocked(&id)? {
                Some(record) => Ok(Some(record)),
                None => {
                    store.remove_file_if_exists(&PosixFsSnapshotArtifactLayout::alias_path(
                        &store.root,
                        &alias,
                    ))?;
                    Ok(None)
                }
            }
        })
    }

    pub(crate) fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.ensure_layout()?;
        let records_dir = self.records_dir();
        let mut records = Vec::new();
        for entry in fs::read_dir(&records_dir).map_err(|error| {
            RepositoryError::backend(
                format!("read records dir '{}'", records_dir.display()),
                error,
            )
        })? {
            let entry = entry.map_err(|error| {
                RepositoryError::backend(
                    format!("read entry in '{}'", records_dir.display()),
                    error,
                )
            })?;
            if !entry
                .file_type()
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("inspect file type '{}'", entry.path().display()),
                        error,
                    )
                })?
                .is_file()
            {
                continue;
            }
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let record: SnapshotRecord = self.read_json(&entry.path())?;
            if Self::matches_record_filter(&record, &filter) {
                records.push(record);
            }
        }
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.to_string().cmp(&right.id.to_string()))
        });
        Ok(records)
    }

    pub(crate) fn delete_record(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let Some(record) = self.load_record_by_id_unlocked(id)? else {
            // Idempotent: already doesn't exist
            return Ok(());
        };
        if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let snapshot_layout = PosixFsSnapshotArtifactLayout::new(&store.root, id);
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                store.remove_file_if_exists(
                    &snapshot_layout.path(super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER),
                )?;
                if store.load_alias_target(alias)?.as_ref() == Some(id) {
                    store.remove_file_if_exists(&alias_path)?;
                }
                if record.committed.is_some() {
                    store.remove_dir_if_exists(&snapshot_layout.snapshot_dir())?;
                }
                store.remove_file_if_exists(&store.record_path(id))
            })?;
            return Ok(());
        }
        let snapshot_layout = self.layout(id);
        self.remove_file_if_exists(&self.commit_marker_path(id))?;
        if record.committed.is_some() {
            self.remove_dir_if_exists(&snapshot_layout.snapshot_dir())?;
        }
        self.remove_file_if_exists(&self.record_path(id))?;
        Ok(())
    }

    /// Resolves one alias to a committed snapshot id and drops stale alias entries on the way.
    pub(crate) fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let alias =
            SnapshotAlias::parse(alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.with_alias_lock(&alias, |store| {
            let Some(id) = store.load_alias_target(&alias)? else {
                return Ok(None);
            };
            if store.load_record_by_id_unlocked(&id)?.is_some() {
                return Ok(Some(id));
            }
            let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, &alias);
            store.remove_file_if_exists(&alias_path)?;
            Ok(None)
        })
    }

    fn aliases_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::aliases_dir(&self.root)
    }

    fn records_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::records_dir(&self.root)
    }

    fn snapshots_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::snapshots_dir(&self.root)
    }

    fn ensure_layout(&self) -> RepositoryResult<()> {
        let catalog_dir = PosixFsSnapshotArtifactLayout::catalog_dir(&self.root);
        let aliases_dir = self.aliases_dir();
        let records_dir = self.records_dir();
        let volume_aliases_dir = PosixFsSnapshotArtifactLayout::volume_aliases_dir(&self.root);
        let volume_records_dir = PosixFsSnapshotArtifactLayout::volume_records_dir(&self.root);
        let snapshots_dir = self.snapshots_dir();
        for dir in [
            &catalog_dir,
            &aliases_dir,
            &records_dir,
            &volume_aliases_dir,
            &volume_records_dir,
            &snapshots_dir,
        ] {
            fs::create_dir_all(dir).map_err(|error| {
                RepositoryError::backend(format!("create catalog dir '{}'", dir.display()), error)
            })?;
        }
        Ok(())
    }

    pub(crate) fn get_volume(&self, reference: &str) -> RepositoryResult<Option<VolumeRecord>> {
        self.ensure_volume_component(reference, "reference")?;
        if let Some(record) = self.load_volume_by_id_unlocked(reference)? {
            return Ok(Some(record));
        }
        self.with_volume_alias_lock(reference, |store| {
            let alias_path =
                PosixFsSnapshotArtifactLayout::volume_alias_path(&store.root, reference);
            if !alias_path.exists() {
                return Ok(None);
            }
            let volume_id: String = store.read_json(&alias_path)?;
            store.ensure_volume_id(&volume_id)?;
            match store.load_volume_by_id_unlocked(&volume_id)? {
                Some(record) => Ok(Some(record)),
                None => {
                    store.remove_file_if_exists(&alias_path)?;
                    Ok(None)
                }
            }
        })
    }

    pub(crate) fn list_volumes_page(
        &self,
        after_volume_id: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<VolumeRecordPage> {
        if limit == 0 {
            return Err(RepositoryError::InvalidRequest {
                reason: "volume page limit must be greater than zero".to_string(),
            });
        }
        if let Some(volume_id) = after_volume_id {
            self.ensure_volume_id(volume_id)?;
        }
        let mut selected = self.volume_ids_unlocked()?;
        if let Some(after) = after_volume_id {
            selected.retain(|volume_id| volume_id.as_str() > after);
        }

        let has_more = selected.len() > limit;
        selected.truncate(limit);
        let mut records = Vec::with_capacity(selected.len());
        for volume_id in selected {
            if let Some(record) = self.load_volume_by_id_unlocked(&volume_id)? {
                records.push(record);
            }
        }
        let next_volume_id = has_more
            .then(|| records.last().map(|record| record.id.clone()))
            .flatten();
        Ok(VolumeRecordPage {
            records,
            next_volume_id,
        })
    }

    pub(crate) fn create_volume(&self, record: &VolumeRecord) -> RepositoryResult<()> {
        self.ensure_volume_id(&record.id)?;
        self.ensure_volume_component(&record.name, "name")?;
        self.with_volume_alias_lock(&record.name, |store| {
            let _record_guard = store.acquire_volume_record_lock(&record.id)?;
            let record_path =
                PosixFsSnapshotArtifactLayout::volume_record_path(&store.root, &record.id);
            if record_path.exists() {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("volume '{}' already exists", record.id),
                });
            }
            let alias_path =
                PosixFsSnapshotArtifactLayout::volume_alias_path(&store.root, &record.name);
            if alias_path.exists() {
                let existing_id = store.read_json::<String>(&alias_path)?;
                store.ensure_volume_id(&existing_id)?;
                if PosixFsSnapshotArtifactLayout::volume_record_path(&store.root, &existing_id)
                    .exists()
                {
                    return Err(RepositoryError::VolumeNameConflict {
                        name: record.name.clone(),
                    });
                }
                store.remove_file_if_exists(&alias_path)?;
            }
            if PosixFsSnapshotArtifactLayout::volume_record_path(&store.root, &record.name).exists()
                || PosixFsSnapshotArtifactLayout::volume_alias_path(&store.root, &record.id)
                    .exists()
            {
                return Err(RepositoryError::VolumeNameConflict {
                    name: record.name.clone(),
                });
            }

            let mut durable_record = record.clone();
            durable_record.backing_image_config = None;
            store.write_json(&record_path, &durable_record)?;
            if let Err(error) = store.write_json(&alias_path, &record.id) {
                let _ = store.remove_file_if_exists(&record_path);
                return Err(error);
            }
            Ok(())
        })
    }

    pub(crate) fn put_volume(&self, record: &VolumeRecord) -> RepositoryResult<()> {
        self.ensure_volume_id(&record.id)?;
        self.ensure_volume_component(&record.name, "name")?;
        let _guard = self.acquire_volume_record_lock(&record.id)?;
        let path = PosixFsSnapshotArtifactLayout::volume_record_path(&self.root, &record.id);
        if !path.exists() {
            return Err(RepositoryError::VolumeNotFound {
                lookup: record.id.clone(),
            });
        }
        let existing = self.read_json::<VolumeRecord>(&path)?;
        self.ensure_volume_id(&existing.id)?;
        existing
            .validate_catalog_update(record)
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        let mut durable_record = record.clone();
        durable_record.backing_image_config = None;
        // Reservation state is owned exclusively by the reservation APIs. A
        // concurrent backing/status update must not restore a released owner.
        durable_record.reserved_by_sandbox_id = existing.reserved_by_sandbox_id;
        durable_record.read_only_mounts = existing.read_only_mounts;
        self.write_json(&path, &durable_record)
    }

    pub(crate) fn get_build_cache_head(&self) -> RepositoryResult<Option<String>> {
        let path = self.root.join("template-build/cache-head.json");
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| RepositoryError::backend("read build cache head", error)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(RepositoryError::backend("read build cache head", error)),
        }
    }

    pub(crate) fn replace_build_cache_head(
        &self,
        volume_id: &str,
    ) -> RepositoryResult<Option<String>> {
        self.ensure_volume_id(volume_id)?;
        let _guard = self.acquire_volume_alias_lock("aenv-buildkit-cache-head")?;
        let previous = self.get_build_cache_head()?;
        self.write_json(
            &self.root.join("template-build/cache-head.json"),
            &volume_id,
        )?;
        Ok(previous)
    }

    pub(crate) fn delete_volume(&self, volume_id: &str) -> RepositoryResult<()> {
        self.ensure_volume_id(volume_id)?;
        let Some(existing) = self.load_volume_by_id_unlocked(volume_id)? else {
            return Ok(());
        };
        self.with_volume_alias_lock(&existing.name, |store| {
            let _record_guard = store.acquire_volume_record_lock(volume_id)?;
            let Some(record) = store.load_volume_by_id_unlocked(volume_id)? else {
                return Ok(());
            };
            if let Some(owner) = record.reserved_by_sandbox_id {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("volume '{volume_id}' is reserved by sandbox '{owner}'"),
                });
            }
            if let Some(owner) = record.read_only_mounts.first() {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "volume '{volume_id}' is mounted read-only by sandbox '{owner}'"
                    ),
                });
            }
            store.remove_file_if_exists(&PosixFsSnapshotArtifactLayout::volume_record_path(
                &store.root,
                volume_id,
            ))?;
            let alias_path =
                PosixFsSnapshotArtifactLayout::volume_alias_path(&store.root, &record.name);
            if alias_path.exists() && store.read_json::<String>(&alias_path)? == volume_id {
                store.remove_file_if_exists(&alias_path)?;
            }
            Ok(())
        })
    }

    pub(crate) fn reserve_volume(
        &self,
        volume_id: &str,
        owner: &str,
    ) -> RepositoryResult<Option<String>> {
        self.ensure_volume_id(volume_id)?;
        self.ensure_volume_component(owner, "owner")?;
        let _guard = self.acquire_volume_record_lock(volume_id)?;
        let path = PosixFsSnapshotArtifactLayout::volume_record_path(&self.root, volume_id);
        let mut record = self.load_volume_by_id_unlocked(volume_id)?.ok_or_else(|| {
            RepositoryError::VolumeNotFound {
                lookup: volume_id.to_string(),
            }
        })?;
        if record.mode == VolumeMode::ReadOnly {
            return Ok(None);
        }
        if let Some(existing) = record.reserved_by_sandbox_id.as_deref() {
            if existing != owner {
                return Ok(Some(existing.to_owned()));
            }
            return Ok(None);
        }
        record.reserved_by_sandbox_id = Some(owner.to_owned());
        self.write_json(&path, &record)?;
        Ok(None)
    }

    pub(crate) fn reserve_read_only_volume(
        &self,
        volume_id: &str,
        owner: &str,
    ) -> RepositoryResult<()> {
        self.ensure_volume_id(volume_id)?;
        self.ensure_volume_component(owner, "owner")?;
        let _guard = self.acquire_volume_record_lock(volume_id)?;
        let path = PosixFsSnapshotArtifactLayout::volume_record_path(&self.root, volume_id);
        let mut record = self.load_volume_by_id_unlocked(volume_id)?.ok_or_else(|| {
            RepositoryError::VolumeNotFound {
                lookup: volume_id.to_string(),
            }
        })?;
        if record.mode != VolumeMode::ReadOnly {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("volume '{volume_id}' is not read-only"),
            });
        }
        if record.read_only_mounts.iter().any(|entry| entry == owner) {
            return Ok(());
        }
        record.read_only_mounts.push(owner.to_owned());
        self.write_json(&path, &record)?;
        Ok(())
    }

    pub(crate) fn replace_volume_owner_for(
        &self,
        volume_id: &str,
        from: &str,
        to: Option<&str>,
    ) -> RepositoryResult<()> {
        self.ensure_volume_id(volume_id)?;
        self.ensure_volume_component(from, "owner")?;
        if let Some(to) = to {
            self.ensure_volume_component(to, "owner")?;
        }
        if to == Some(from) {
            return Ok(());
        }
        let _guard = self.acquire_volume_record_lock(volume_id)?;
        let path = PosixFsSnapshotArtifactLayout::volume_record_path(&self.root, volume_id);
        let mut record = self.load_volume_by_id_unlocked(volume_id)?.ok_or_else(|| {
            RepositoryError::VolumeNotFound {
                lookup: volume_id.to_owned(),
            }
        })?;
        if record.replace_owner(from, to) {
            self.write_json(&path, &record)?;
        }
        Ok(())
    }

    fn volume_ids_unlocked(&self) -> RepositoryResult<Vec<String>> {
        let directory = PosixFsSnapshotArtifactLayout::volume_records_dir(&self.root);
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(RepositoryError::backend(
                    format!("read volume directory '{}'", directory.display()),
                    error,
                ))
            }
        };
        let mut volume_ids = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|error| RepositoryError::backend("read volume entry", error))?;
            if !entry
                .file_type()
                .map_err(|error| RepositoryError::backend("inspect volume entry", error))?
                .is_file()
                || entry.path().extension().and_then(|value| value.to_str()) != Some("json")
            {
                continue;
            }
            let path = entry.path();
            let Some(volume_id) = path.file_stem().and_then(|value| value.to_str()) else {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("invalid volume record path '{}'", path.display()),
                });
            };
            self.ensure_volume_id(volume_id)?;
            volume_ids.push(volume_id.to_owned());
        }
        volume_ids.sort_unstable();
        Ok(volume_ids)
    }

    fn load_volume_by_id_unlocked(
        &self,
        volume_id: &str,
    ) -> RepositoryResult<Option<VolumeRecord>> {
        self.ensure_volume_id(volume_id)?;
        let path = PosixFsSnapshotArtifactLayout::volume_record_path(&self.root, volume_id);
        if !path.exists() {
            return Ok(None);
        }
        let record: VolumeRecord = self.read_json(&path)?;
        self.ensure_volume_id(&record.id)?;
        if record.id != volume_id {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "volume record id '{}' does not match path '{}'",
                    record.id,
                    path.display()
                ),
            });
        }
        Ok(Some(record))
    }

    fn ensure_volume_id(&self, volume_id: &str) -> RepositoryResult<()> {
        self.ensure_volume_component(volume_id, "id")
    }

    fn ensure_volume_component(&self, value: &str, kind: &str) -> RepositoryResult<()> {
        if !is_valid_volume_component(value) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("invalid volume {kind} '{value}'"),
            });
        }
        Ok(())
    }

    pub(crate) fn try_start(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let _guard = self.acquire_record_lock(id)?;
        let mut record = self.load_record_by_id_unlocked(id)?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
        })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        if build.status != TemplateBuildStatus::Waiting {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("template build '{id}' is not in waiting state"),
            });
        }
        build.status = TemplateBuildStatus::Building;
        build.started_at_unix_ms = Some(now);
        build.error_reason = None;
        record.updated_at_unix_ms = now;
        self.write_record_unlocked(&record)?;
        Ok(record)
    }

    pub(crate) fn mark_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let _guard = self.acquire_record_lock(id)?;
        let mut record = self.load_record_by_id_unlocked(id)?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
        })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        build.status = TemplateBuildStatus::Error;
        build.finished_at_unix_ms = Some(now);
        build.error_reason = Some(reason);
        record.updated_at_unix_ms = now;
        self.write_record_unlocked(&record)
    }

    fn read_json<T>(&self, path: &Path) -> RepositoryResult<T>
    where
        T: DeserializeOwned,
    {
        let bytes = fs::read(path).map_err(|error| {
            RepositoryError::backend(format!("read '{}'", path.display()), error)
        })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            RepositoryError::backend(format!("parse json '{}'", path.display()), error)
        })
    }

    fn write_json<T>(&self, path: &Path, value: &T) -> RepositoryResult<()>
    where
        T: Serialize,
    {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(format!("create '{}'", parent.display()), error)
            })?;
        }
        let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
            RepositoryError::backend(format!("serialize json '{}'", path.display()), error)
        })?;
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(format!("create temp file in '{}'", parent.display()), error)
        })?;
        temp.write_all(&bytes).map_err(|error| {
            RepositoryError::backend(
                format!("write temp json '{}'", temp.path().display()),
                error,
            )
        })?;
        temp.as_file().sync_all().map_err(|error| {
            RepositoryError::backend(format!("sync temp json '{}'", temp.path().display()), error)
        })?;
        let tmp_path = temp.path().to_path_buf();
        temp.persist(path).map_err(|error| {
            RepositoryError::backend(
                format!(
                    "persist json '{}' -> '{}'",
                    tmp_path.display(),
                    path.display()
                ),
                error.error,
            )
        })?;
        Ok(())
    }

    fn write_commit_marker(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let path = self.commit_marker_path(id);
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            RepositoryError::backend(format!("create '{}'", parent.display()), error)
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(
                format!("create temp commit marker in '{}'", path.display()),
                error,
            )
        })?;
        temp.write_all(b"committed").map_err(|error| {
            RepositoryError::backend(
                format!("write commit marker '{}'", temp.path().display()),
                error,
            )
        })?;
        temp.persist(&path).map_err(|error| {
            RepositoryError::backend(
                format!("persist commit marker '{}'", path.display()),
                error.error,
            )
        })?;
        Ok(())
    }

    fn remove_file_if_exists(&self, path: &Path) -> RepositoryResult<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", path.display()),
                error,
            )),
        }
    }

    fn remove_dir_if_exists(&self, path: &Path) -> RepositoryResult<()> {
        match fs::remove_dir_all(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", path.display()),
                error,
            )),
        }
    }

    fn is_committed(&self, id: &SnapshotId) -> bool {
        self.commit_marker_path(id).exists()
            && self
                .load_record_by_id_unlocked(id)
                .ok()
                .flatten()
                .is_some_and(|record| record.committed.is_some())
    }

    fn cleanup_uncommitted_snapshot_dir(&self, id: &SnapshotId) -> RepositoryResult<()> {
        if self.is_committed(id) {
            return Ok(());
        }
        let snapshot_layout = self.layout(id);
        self.remove_dir_if_exists(&snapshot_layout.snapshot_dir())
    }

    fn load_record_by_id_unlocked(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        let path = self.record_path(id);
        if !path.exists() {
            return Ok(None);
        }
        self.read_json(&path).map(Some)
    }

    fn load_alias_target(&self, alias: &SnapshotAlias) -> RepositoryResult<Option<SnapshotId>> {
        let path = PosixFsSnapshotArtifactLayout::alias_path(&self.root, alias);
        if !path.exists() {
            return Ok(None);
        }
        self.read_json(&path).map(Some)
    }

    fn acquire_file_lock(
        &self,
        lock_path: PathBuf,
        contents: String,
        label: &'static str,
        on_locked: impl Fn() -> RepositoryResult<PosixFileLockGuard>,
    ) -> RepositoryResult<PosixFileLockGuard> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(
                    format!("create {label} lock dir '{}'", parent.display()),
                    error,
                )
            })?;
        }

        let deadline = FILE_LOCK_TIMEOUT.map(|timeout| Instant::now() + timeout);
        loop {
            let file = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&lock_path)
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("open {label} lock '{}'", lock_path.display()),
                        error,
                    )
                })?;
            match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                Ok(mut file) => {
                    file.set_len(0).map_err(|error| {
                        RepositoryError::backend(
                            format!("truncate {label} lock '{}'", lock_path.display()),
                            error,
                        )
                    })?;
                    file.write_all(contents.as_bytes()).map_err(|error| {
                        RepositoryError::backend(
                            format!("write {label} lock '{}'", lock_path.display()),
                            error,
                        )
                    })?;
                    return Ok(PosixFileLockGuard { _file: file });
                }
                Err((_file, error)) if error == Errno::EAGAIN || error == Errno::EWOULDBLOCK => {
                    if let Some(deadline) = deadline {
                        if Instant::now() < deadline {
                            thread::sleep(Duration::from_millis(25));
                            continue;
                        }
                    }
                    return on_locked();
                }
                Err((_file, error)) => {
                    return Err(RepositoryError::backend(
                        format!("lock {label} lock '{}'", lock_path.display()),
                        error,
                    ));
                }
            }
        }
    }

    fn acquire_alias_lock(&self, alias: &SnapshotAlias) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_catalog_lock(
            PosixFsSnapshotArtifactLayout::alias_lock_path(&self.root, alias),
            "alias",
        )
    }

    fn acquire_record_lock(&self, id: &SnapshotId) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_catalog_lock(
            PosixFsSnapshotArtifactLayout::record_lock_path(&self.root, id),
            "record",
        )
    }

    fn acquire_volume_alias_lock(&self, alias: &str) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_catalog_lock(
            PosixFsSnapshotArtifactLayout::volume_alias_lock_path(&self.root, alias),
            "volume alias",
        )
    }

    fn acquire_volume_record_lock(&self, id: &str) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_catalog_lock(
            PosixFsSnapshotArtifactLayout::volume_record_lock_path(&self.root, id),
            "volume record",
        )
    }

    fn acquire_catalog_lock(
        &self,
        lock_path: PathBuf,
        label: &'static str,
    ) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_file_lock(
            lock_path.clone(),
            std::process::id().to_string(),
            label,
            || {
                Err(RepositoryError::Backend {
                    message: format!(
                        "timed out waiting for {label} lock '{}'",
                        lock_path.display()
                    ),
                    source: None,
                })
            },
        )
    }

    fn with_alias_lock<T>(
        &self,
        alias: &SnapshotAlias,
        action: impl FnOnce(&Self) -> RepositoryResult<T>,
    ) -> RepositoryResult<T> {
        let _guard = self.acquire_alias_lock(alias)?;
        action(self)
    }

    fn with_volume_alias_lock<T>(
        &self,
        alias: &str,
        action: impl FnOnce(&Self) -> RepositoryResult<T>,
    ) -> RepositoryResult<T> {
        let _guard = self.acquire_volume_alias_lock(alias)?;
        action(self)
    }

    fn ensure_alias_available(
        &self,
        alias: &SnapshotAlias,
        new_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&self.root, alias);
        if let Some(existing) = self.load_alias_target(alias)? {
            if &existing == new_id {
                return Ok(());
            }
            if self.load_record_by_id_unlocked(&existing)?.is_some() {
                return Err(RepositoryError::AliasConflict {
                    alias: alias.to_string(),
                    existing,
                    new_id: new_id.clone(),
                });
            }
            self.remove_file_if_exists(&alias_path)?;
        }
        Ok(())
    }

    fn write_record_unlocked(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.write_json(&self.record_path(&record.id), record)
    }

    fn committed_record_unlocked(
        &self,
        metadata: &SnapshotPublishMetadata,
        committed: CommittedSnapshot,
        now_unix_ms: i64,
    ) -> RepositoryResult<SnapshotRecord> {
        let id = metadata.id.clone();
        let alias = metadata.alias.clone();
        let resources = metadata.resources;
        let source = metadata.source.clone();
        if let Some(mut record) = self.load_record_by_id_unlocked(&id)? {
            record.mark_committed(alias, resources, committed, source, now_unix_ms);
            return Ok(record);
        }

        let source = match source {
            SnapshotPublishSource::Template => SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    started_at_unix_ms: None,
                    finished_at_unix_ms: Some(now_unix_ms),
                    error_reason: None,
                },
            },
            SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                SnapshotSource::Sandbox { source_sandbox_id }
            }
        };

        Ok(SnapshotRecord {
            id,
            alias,
            source,
            resources,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            committed: Some(committed),
        })
    }

    fn write_committed_record_unlocked(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.write_record_unlocked(record)
    }

    fn matches_record_filter(record: &SnapshotRecord, filter: &SnapshotListFilter) -> bool {
        if let Some(alias_prefix) = filter.alias_prefix.as_deref() {
            match record.alias.as_ref() {
                Some(alias) if alias.to_string().starts_with(alias_prefix) => {}
                _ => return false,
            }
        }

        if let Some(ids) = filter.snapshot_ids.as_ref() {
            if !ids.iter().any(|id| id == &record.id) {
                return false;
            }
        }

        if let Some(id_or_alias) = filter.snapshot_id_or_alias.as_deref() {
            if record.id.to_string() != id_or_alias
                && record
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias.as_ref() != id_or_alias)
            {
                return false;
            }
        }

        if let Some(source_sandbox_id) = filter.source_sandbox_id.as_deref() {
            match &record.source {
                SnapshotSource::Sandbox {
                    source_sandbox_id: record_source_sandbox_id,
                } if record_source_sandbox_id == source_sandbox_id => {}
                _ => return false,
            }
        }

        if let Some(sources) = filter.sources.as_ref() {
            let source = match &record.source {
                SnapshotSource::Template { .. } => SnapshotSourceKind::Template,
                SnapshotSource::Sandbox { .. } => SnapshotSourceKind::Sandbox,
            };
            if !sources.contains(&source) {
                return false;
            }
        }

        if let Some(statuses) = filter.template_statuses.as_ref() {
            let SnapshotSource::Template { build } = &record.source else {
                return false;
            };
            if !statuses.contains(&build.status) {
                return false;
            };
        }

        true
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::super::layout::PosixFsSnapshotArtifactLayout;
    use super::PosixFsCatalogStore;
    use crate::snapshot::RepositoryError;
    use crate::snapshot::{
        CommittedSnapshot, SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotPublishMetadata,
        SnapshotPublishSource, SnapshotRecord, SnapshotSourceKind, TemplateBuildStatus,
    };
    use crate::volume::{VolumeMode, VolumeRecord};

    #[test]
    fn build_cache_head_updates_are_atomic_across_catalog_instances() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().to_path_buf();
        let store = PosixFsCatalogStore::new(root.clone());
        assert_eq!(store.get_build_cache_head().unwrap(), None);
        let ids = (0..16)
            .map(|i| format!("vol_cache_{i}"))
            .collect::<Vec<_>>();
        let mut previous = std::thread::scope(|scope| {
            let jobs = ids
                .iter()
                .map(|id| {
                    let root = root.clone();
                    scope.spawn(move || {
                        PosixFsCatalogStore::new(root)
                            .replace_build_cache_head(id)
                            .unwrap()
                    })
                })
                .collect::<Vec<_>>();
            jobs.into_iter()
                .map(|job| job.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(previous.iter().filter(|id| id.is_none()).count(), 1);
        previous.push(
            PosixFsCatalogStore::new(root)
                .get_build_cache_head()
                .unwrap(),
        );
        let mut observed = previous.into_iter().flatten().collect::<Vec<_>>();
        observed.sort();
        let mut expected = ids;
        expected.sort();
        assert_eq!(observed, expected);
        assert!(store.replace_build_cache_head("../invalid").is_err());
    }

    #[test]
    fn begin_and_commit_make_snapshot_visible() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let session = store
            .begin_publish(&snapshot_id)
            .expect("begin should work");

        store
            .commit_publish(
                &session,
                SnapshotPublishMetadata {
                    id: snapshot_id.clone(),
                    source: SnapshotPublishSource::Template,
                    ..SnapshotPublishMetadata::mock()
                },
                CommittedSnapshot::mock(),
            )
            .expect("commit should work");

        assert!(store
            .get(&snapshot_id.to_string())
            .expect("get should work")
            .expect("snapshot should exist")
            .committed
            .is_some());
        assert!(
            PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id)
                .path(super::super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER)
                .exists()
        );
    }

    fn committed_metadata(
        id: SnapshotId,
        alias: &str,
        source: SnapshotPublishSource,
    ) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            source,
            ..SnapshotPublishMetadata::mock()
        }
    }

    fn commit_record(store: &PosixFsCatalogStore, metadata: SnapshotPublishMetadata) -> SnapshotId {
        let snapshot_id = metadata.id.clone();
        let session = store
            .begin_publish(&snapshot_id)
            .expect("begin should work");
        store
            .commit_publish(&session, metadata, CommittedSnapshot::mock())
            .expect("commit should work");
        snapshot_id
    }

    fn listed_ids(store: &PosixFsCatalogStore, filter: SnapshotListFilter) -> Vec<SnapshotId> {
        store
            .list(filter)
            .expect("list should work")
            .into_iter()
            .map(|record| record.id)
            .collect()
    }

    #[test]
    fn list_applies_record_filters() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let template_alpha = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "template-alpha",
                SnapshotPublishSource::Template,
            ),
        );
        let template_beta = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "template-beta",
                SnapshotPublishSource::Template,
            ),
        );
        let sandbox_one = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "sandbox-one",
                SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox-1".to_string(),
                },
            ),
        );
        let sandbox_two = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "sandbox-two",
                SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox-2".to_string(),
                },
            ),
        );
        let errored_template = SnapshotId::generate();
        store
            .create(SnapshotRecord::template_waiting(
                errored_template.clone(),
                Some(SnapshotAlias::parse("template-error").expect("alias should parse")),
                Default::default(),
            ))
            .expect("create template should work");
        store
            .mark_error(
                &errored_template,
                crate::snapshot::TemplateBuildErrorReason::new("boom"),
            )
            .expect("mark error should work");

        let ids = listed_ids(
            &store,
            SnapshotListFilter::by_ids([template_alpha.clone(), sandbox_one.clone()]),
        );
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&sandbox_one));

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                alias_prefix: Some("template-".to_string()),
                ..SnapshotListFilter::default()
            },
        );
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&template_beta));
        assert!(ids.contains(&errored_template));

        let ids = listed_ids(&store, SnapshotListFilter::templates());
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&template_beta));
        assert!(ids.contains(&errored_template));
        assert!(!ids.contains(&sandbox_one));

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(Some("sandbox-1".to_string()), None),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(None, Some("team/sandbox-one:v1".to_string())),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(None, Some(format!("{}:v1", sandbox_one))),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(
                Some("sandbox-2".to_string()),
                Some("sandbox-one".to_string()),
            ),
        );
        assert!(ids.is_empty());

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                template_statuses: Some(vec![TemplateBuildStatus::Error]),
                ..SnapshotListFilter::templates()
            },
        );
        assert_eq!(ids, vec![errored_template]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                alias_prefix: Some("sandbox-".to_string()),
                sources: Some(vec![SnapshotSourceKind::Sandbox]),
                snapshot_ids: Some(vec![sandbox_two.clone(), template_alpha]),
                ..SnapshotListFilter::default()
            },
        );
        assert_eq!(ids, vec![sandbox_two]);
    }

    #[test]
    fn get_rejects_path_traversal_as_alias() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        // "../../etc/passwd" is not a valid alias (nor a UUID), so alias parsing
        // validation rejects it as InvalidRequest.
        let err = store
            .get("../../etc/passwd")
            .expect_err("path traversal should be rejected");
        assert!(
            matches!(err, crate::snapshot::RepositoryError::InvalidRequest { .. }),
            "expected InvalidRequest, got: {err:?}"
        );
    }

    #[test]
    fn get_returns_none_for_unknown_valid_uuid() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let unknown = SnapshotId::generate();
        let result = store
            .get(&unknown.to_string())
            .expect("valid UUID lookup should not error");
        assert!(result.is_none(), "non-existent snapshot should return None");
    }

    fn volume_record(index: usize) -> VolumeRecord {
        VolumeRecord {
            id: format!("vol_{index:06}"),
            name: format!("data-{index:06}"),
            mode: VolumeMode::Exclusive,
            size_mb: crate::volume::DEFAULT_VOLUME_SIZE_MB,
            status: crate::volume::VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_image_config: None,
            backing_layers: Vec::new(),
            read_only_mounts: Vec::new(),
            deleting: false,
        }
    }

    #[test]
    fn volume_catalog_uses_keyed_layout_and_locks() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let mut expected_ids = Vec::new();
        for index in 0..40 {
            let record = volume_record(index);
            expected_ids.push(record.id.clone());
            store.create_volume(&record).unwrap();
        }

        assert_eq!(
            store.get_volume("data-000012").unwrap().unwrap().id,
            "vol_000012"
        );
        assert!(matches!(
            store.create_volume(&VolumeRecord {
                id: "vol_conflict".to_owned(),
                ..volume_record(12)
            }),
            Err(RepositoryError::VolumeNameConflict { .. })
        ));
        assert!(tempdir
            .path()
            .join("volumes/records/vol_000012.json")
            .is_file());
        assert!(tempdir
            .path()
            .join("volumes/records/vol_000012.lock")
            .is_file());
        assert!(tempdir.path().join("volumes/aliases/data-000012").is_file());
        assert!(tempdir
            .path()
            .join("volumes/aliases/data-000012.lock")
            .is_file());
        assert!(!tempdir.path().join("volumes.lock").exists());
        assert!(!tempdir.path().join("catalog/volumes").exists());
        assert!(!tempdir.path().join("catalog/volume-names").exists());

        expected_ids.sort_unstable();
        let mut actual_ids = Vec::new();
        let mut after = None;
        loop {
            let page = store.list_volumes_page(after.as_deref(), 7).unwrap();
            actual_ids.extend(page.records.into_iter().map(|record| record.id));
            let Some(next) = page.next_volume_id else {
                break;
            };
            after = Some(next);
        }
        assert_eq!(actual_ids, expected_ids);

        let id = &expected_ids[0];
        assert_eq!(store.reserve_volume(id, "sandbox-a").unwrap(), None);
        assert_eq!(
            store.reserve_volume(id, "sandbox-b").unwrap(),
            Some("sandbox-a".to_owned())
        );
        store
            .replace_volume_owner_for(id, "sandbox-a", Some("sandbox-b"))
            .unwrap();
        assert_eq!(
            store
                .get_volume(id)
                .unwrap()
                .unwrap()
                .reserved_by_sandbox_id
                .as_deref(),
            Some("sandbox-b")
        );
    }
}
