use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::layout::PosixFsSnapshotArtifactLayout;
use crate::repository::{DocumentCollection, DocumentKey, DocumentUpdate, SnapshotListFilter};
use crate::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::{
    CommittedSnapshot, MemoryStartupPackInfo, SnapshotAlias, SnapshotId, SnapshotPublishMetadata,
    SnapshotPublishSource, SnapshotRecord, SnapshotSource, SnapshotSourceKind,
    TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
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

    pub(crate) fn get_document(&self, key: &DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        let path = self.root.join(key.posix_path());
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(RepositoryError::backend(
                format!("read '{}'", path.display()),
                error,
            )),
        }
    }

    pub(crate) fn put_document(&self, key: &DocumentKey, bytes: &[u8]) -> RepositoryResult<()> {
        self.write_bytes(&self.root.join(key.posix_path()), bytes)
    }

    pub(crate) fn update_document(
        &self,
        key: &DocumentKey,
        update: DocumentUpdate,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        let _guard =
            self.acquire_catalog_lock(self.root.join(key.posix_lock_path()), "document")?;
        let current = self.get_document(key)?;
        if let Some(bytes) = update(current.as_deref())? {
            self.put_document(key, &bytes)?;
        }
        Ok(current)
    }

    pub(crate) fn delete_document(&self, key: &DocumentKey) -> RepositoryResult<()> {
        let _guard =
            self.acquire_catalog_lock(self.root.join(key.posix_lock_path()), "document")?;
        self.remove_file_if_exists(&self.root.join(key.posix_path()))
    }

    pub(crate) fn list_documents(
        &self,
        collection: DocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        let directory = self.root.join(collection.prefix());
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(RepositoryError::backend(
                    format!("read '{}'", directory.display()),
                    error,
                ))
            }
        };
        let mut ids = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                RepositoryError::backend(format!("read '{}'", directory.display()), error)
            })?;
            let is_file = entry
                .file_type()
                .map_err(|error| {
                    RepositoryError::backend(format!("inspect '{}'", entry.path().display()), error)
                })?
                .is_file();
            // Lock files share the directory and are not documents.
            let file_name = entry.file_name();
            let Some(id) = file_name
                .to_str()
                .and_then(|name| collection.id_from_file_name(name))
                .filter(|_| is_file)
            else {
                continue;
            };
            if after.is_none_or(|after| id > after) {
                ids.push(id.to_owned());
            }
        }
        ids.sort_unstable();
        ids.truncate(limit);
        Ok(ids)
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
        let _record_guard = self.acquire_record_lock(id)?;
        self.remove_file_if_exists(
            &self
                .root
                .join(DocumentKey::BuildLog(id.clone()).posix_path()),
        )?;
        let Some(record) = self.load_record_by_id_unlocked(id)? else {
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
        let snapshots_dir = self.snapshots_dir();
        for dir in [&catalog_dir, &aliases_dir, &records_dir, &snapshots_dir] {
            fs::create_dir_all(dir).map_err(|error| {
                RepositoryError::backend(format!("create catalog dir '{}'", dir.display()), error)
            })?;
        }
        Ok(())
    }

    /// Persist a verified manifest before attaching its descriptor under the record lock.
    /// A crash between the two renames may leave an orphan artifact or a descriptor
    /// with a missing artifact; retry with the same descriptor repairs either state.
    /// This is not a multi-file durable transaction.
    pub(crate) fn attach_memory_startup(
        &self,
        id: &SnapshotId,
        info: MemoryStartupPackInfo,
        bytes: &[u8],
    ) -> RepositoryResult<()> {
        let _guard = self.acquire_record_lock(id)?;
        let Some(mut record) = self.load_record_by_id_unlocked(id)? else {
            return Ok(());
        };
        let Some(committed) = record.committed.as_mut() else {
            return Ok(());
        };
        if info.pack_size != bytes.len() as u64
            || info.index_sha256 != crate::snapshot::startup_pack::hex_sha256(bytes)
            || overlaybd::startup_manifest::decode_manifest(bytes).map_or(true, |manifest| {
                manifest.mem_virtual_size != info.mem_virtual_size
            })
        {
            return Err(RepositoryError::InvalidRequest {
                reason: "startup manifest descriptor does not match artifact".into(),
            });
        }
        let path = self
            .layout(id)
            .path(crate::snapshot::MEMORY_STARTUP_PACK_ARTIFACT);
        if let Some(existing) = committed.memory_startup.as_ref() {
            if existing != &info {
                return Err(RepositoryError::InvalidRequest {
                    reason: "startup manifest descriptor already differs".into(),
                });
            }
            let artifact_matches = fs::File::open(&path).is_ok_and(|file| {
                let mut artifact = Vec::new();
                file.take(bytes.len() as u64 + 1)
                    .read_to_end(&mut artifact)
                    .is_ok_and(|_| artifact == bytes)
            });
            if artifact_matches {
                return Ok(());
            }
            // A matching descriptor with a missing or corrupt artifact can be
            // repaired only by supplying bytes verified against that descriptor.
        }
        let already_attached = committed.memory_startup.is_some();
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for {}", path.display()),
            source: None,
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            RepositoryError::backend(format!("create {}", parent.display()), error)
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(
                format!("create manifest temp in {}", parent.display()),
                error,
            )
        })?;
        temp.write_all(bytes)
            .and_then(|()| temp.as_file().sync_all())
            .map_err(|error| {
                RepositoryError::backend(format!("write manifest {}", path.display()), error)
            })?;
        temp.persist(&path).map_err(|error| {
            RepositoryError::backend(format!("persist manifest {}", path.display()), error.error)
        })?;
        if !already_attached {
            committed.memory_startup = Some(info);
            record.updated_at_unix_ms = now_unix_ms();
            // Keep a verified orphan artifact on record-write failure: a retry
            // can reuse it, while readers see no descriptor until commit.
            self.write_record_unlocked(&record)?;
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
        let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
            RepositoryError::backend(format!("serialize json '{}'", path.display()), error)
        })?;
        self.write_bytes(path, &bytes)
    }

    fn write_bytes(&self, path: &Path, bytes: &[u8]) -> RepositoryResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(format!("create '{}'", parent.display()), error)
            })?;
        }
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(format!("create temp file in '{}'", parent.display()), error)
        })?;
        temp.write_all(bytes).map_err(|error| {
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
    use crate::repository::SnapshotListFilter;
    use crate::snapshot::{
        CommittedSnapshot, MemoryStartupPackInfo, SnapshotAlias, SnapshotId,
        SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSourceKind,
        TemplateBuildStatus,
    };

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

    #[test]
    fn startup_manifest_attach_is_idempotent_and_cannot_revive_a_delete() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().to_path_buf();
        let store = PosixFsCatalogStore::new(root.clone());
        let id = SnapshotId::generate();
        let session = store.begin_publish(&id).unwrap();
        store
            .commit_publish(
                &session,
                SnapshotPublishMetadata {
                    id: id.clone(),
                    source: SnapshotPublishSource::Template,
                    ..SnapshotPublishMetadata::mock()
                },
                CommittedSnapshot::mock(),
            )
            .unwrap();
        let first_bytes = overlaybd::startup_manifest::encode_manifest(
            &overlaybd::startup_manifest::StartupManifest {
                mem_virtual_size: 4096,
                prefix_pages: Vec::new(),
                ranges: vec![(0, 4096)],
            },
        )
        .unwrap();
        let first = MemoryStartupPackInfo {
            pack_size: first_bytes.len() as u64,
            mem_virtual_size: 4096,
            index_sha256: crate::snapshot::startup_pack::hex_sha256(&first_bytes),
        };
        assert!(store
            .attach_memory_startup(
                &id,
                MemoryStartupPackInfo {
                    pack_size: first.pack_size + 1,
                    ..first.clone()
                },
                &first_bytes,
            )
            .is_err());
        assert!(store
            .attach_memory_startup(
                &id,
                MemoryStartupPackInfo {
                    index_sha256: "wrong hash".into(),
                    ..first.clone()
                },
                &first_bytes,
            )
            .is_err());
        assert!(store
            .attach_memory_startup(
                &id,
                MemoryStartupPackInfo {
                    mem_virtual_size: 8192,
                    ..first.clone()
                },
                &first_bytes,
            )
            .is_err());
        let artifact = PosixFsSnapshotArtifactLayout::new(&root, &id)
            .path(crate::snapshot::MEMORY_STARTUP_PACK_ARTIFACT);
        std::fs::write(&artifact, b"orphan from interrupted attach").unwrap();
        store
            .attach_memory_startup(&id, first.clone(), &first_bytes)
            .unwrap();
        assert_eq!(std::fs::read(&artifact).unwrap(), first_bytes);
        std::fs::remove_file(&artifact).unwrap();
        store
            .attach_memory_startup(&id, first.clone(), &first_bytes)
            .unwrap();
        assert_eq!(std::fs::read(&artifact).unwrap(), first_bytes);
        std::fs::write(&artifact, b"corrupt after descriptor attach").unwrap();
        store
            .attach_memory_startup(&id, first.clone(), &first_bytes)
            .unwrap();
        assert_eq!(std::fs::read(&artifact).unwrap(), first_bytes);
        let second_bytes = overlaybd::startup_manifest::encode_manifest(
            &overlaybd::startup_manifest::StartupManifest {
                mem_virtual_size: 8192,
                prefix_pages: Vec::new(),
                ranges: vec![(4096, 4096)],
            },
        )
        .unwrap();
        assert!(store
            .attach_memory_startup(
                &id,
                MemoryStartupPackInfo {
                    pack_size: second_bytes.len() as u64,
                    mem_virtual_size: 8192,
                    index_sha256: crate::snapshot::startup_pack::hex_sha256(&second_bytes),
                },
                &second_bytes,
            )
            .is_err());
        assert_eq!(std::fs::read(&artifact).unwrap(), first_bytes);
        assert_eq!(
            store
                .get(&id.to_string())
                .unwrap()
                .unwrap()
                .committed
                .unwrap()
                .memory_startup,
            Some(first.clone())
        );

        let race_id = SnapshotId::generate();
        let race_session = store.begin_publish(&race_id).unwrap();
        store
            .commit_publish(
                &race_session,
                SnapshotPublishMetadata {
                    id: race_id.clone(),
                    source: SnapshotPublishSource::Template,
                    ..SnapshotPublishMetadata::mock()
                },
                CommittedSnapshot::mock(),
            )
            .unwrap();
        let attach_root = root.clone();
        let race_bytes = first_bytes.clone();
        let race_info = first.clone();
        let delete_root = root.clone();
        let attach_id = race_id.clone();
        let delete_id = race_id.clone();
        std::thread::scope(|scope| {
            scope.spawn(move || {
                PosixFsCatalogStore::new(attach_root)
                    .attach_memory_startup(&attach_id, race_info, &race_bytes)
                    .unwrap();
            });
            scope.spawn(move || {
                PosixFsCatalogStore::new(delete_root)
                    .delete_record(&delete_id)
                    .unwrap();
            });
        });
        assert!(store.get(&race_id.to_string()).unwrap().is_none());
        assert!(!PosixFsSnapshotArtifactLayout::new(&root, &race_id)
            .path(crate::snapshot::MEMORY_STARTUP_PACK_ARTIFACT)
            .exists());
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
            matches!(
                err,
                crate::repository::RepositoryError::InvalidRequest { .. }
            ),
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
}
