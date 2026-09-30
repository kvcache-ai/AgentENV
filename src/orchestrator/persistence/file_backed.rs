use std::collections::{BTreeSet, HashMap, HashSet};
#[cfg(test)]
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::{CleanupMetrics, PersistenceResult, SandboxPersistenceError, SandboxPersister};
use crate::cfg::ConfigManager;
use crate::local_store::{LocalKvBatchOp, LocalKvStore, LocalStoreDurability};
use crate::orchestrator::{store::SandboxMetadata, SandboxState};
use crate::sandbox::{PausedSandboxState, RuntimeArtifactClosure, SandboxBackendFactory};
use crate::types::SandboxId;
use crate::virtualization::VirtualizationMode;

const RECORD_VERSION: u32 = 1;
const RECORD_DB_DIR: &str = "records.db";
const RESUME_MARKER_DIR: &str = "resume-markers";
const CLEANUP_JOURNAL_FILE: &str = "cleanup-journal.reserved";
const CLEANUP_JOURNAL_SLOT_BYTES: usize = 256;
const DEFAULT_CLEANUP_JOURNAL_RESERVE_BYTES: u64 = 64 * 1024 * 1024;
const GENERATION_REFERENCE_INDEX_VERSION: &[u8] = b"1";
const GENERATION_REFERENCE_INDEX_VERSION_KEY: &[u8] = b"index/generation-reference/version";
const GENERATION_REFERENCE_PREFIX: &str = "index/generation-reference/by-generation/";
const OWNER_GENERATION_REFERENCE_PREFIX: &str = "index/generation-reference/by-owner/";
#[cfg(not(test))]
const RESUME_MARKER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(test)]
const RESUME_MARKER_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CleanupKind {
    FinalDelete,
    PruneGenerations,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CleanupObligation {
    sandbox_id: SandboxId,
    kind: CleanupKind,
    keep_generation: Option<Uuid>,
}

impl CleanupObligation {
    fn key(&self) -> (SandboxId, CleanupKind) {
        (self.sandbox_id, self.kind)
    }
}

#[derive(Debug)]
struct CleanupJournalSlots {
    occupied: HashMap<(SandboxId, CleanupKind), usize>,
    free: BTreeSet<usize>,
}

#[derive(Debug)]
struct CleanupJournal {
    path: PathBuf,
    slots: Mutex<CleanupJournalSlots>,
}

impl CleanupJournal {
    async fn open(path: PathBuf, reserve_bytes: u64) -> PersistenceResult<Self> {
        if reserve_bytes < CLEANUP_JOURNAL_SLOT_BYTES as u64
            || !reserve_bytes.is_multiple_of(CLEANUP_JOURNAL_SLOT_BYTES as u64)
        {
            return Err(SandboxPersistenceError::RuntimeState {
                reason: "cleanup journal reserve must contain whole slots",
            });
        }
        let path_for_open = path.clone();
        let obligations = tokio::task::spawn_blocking(move || {
            if let Some(parent) = path_for_open.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path_for_open)?;
            let length = file.metadata()?.len();
            if length == 0 {
                let result = unsafe {
                    libc::posix_fallocate(file.as_raw_fd(), 0, reserve_bytes as libc::off_t)
                };
                if result != 0 {
                    return Err(std::io::Error::from_raw_os_error(result));
                }
                file.sync_all()?;
            } else if length != reserve_bytes {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "cleanup journal reserve size changed",
                ));
            }

            file.seek(SeekFrom::Start(0))?;
            let mut occupied = HashMap::new();
            let mut free = BTreeSet::new();
            let mut bytes = vec![0u8; CLEANUP_JOURNAL_SLOT_BYTES];
            for slot in 0..(reserve_bytes as usize / CLEANUP_JOURNAL_SLOT_BYTES) {
                file.read_exact(&mut bytes)?;
                let payload_len = bytes
                    .iter()
                    .position(|byte| *byte == 0)
                    .unwrap_or(bytes.len());
                if payload_len == 0 {
                    free.insert(slot);
                    continue;
                }
                let obligation: CleanupObligation = serde_json::from_slice(&bytes[..payload_len])
                    .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                })?;
                if occupied.insert(obligation.key(), slot).is_some() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "duplicate cleanup journal obligation",
                    ));
                }
            }
            Ok::<_, std::io::Error>((occupied, free))
        })
        .await
        .map_err(|source| {
            SandboxPersistenceError::store("join cleanup journal open", source.into())
        })?
        .map_err(|source| SandboxPersistenceError::io("open cleanup journal", &path, source))?;

        Ok(Self {
            path,
            slots: Mutex::new(CleanupJournalSlots {
                occupied: obligations.0,
                free: obligations.1,
            }),
        })
    }

    async fn record(&self, obligation: CleanupObligation) -> PersistenceResult<bool> {
        let mut slots = self.slots.lock().await;
        let key = obligation.key();
        let (slot, inserted) = if let Some(slot) = slots.occupied.get(&key).copied() {
            (slot, false)
        } else {
            (
                slots
                    .free
                    .pop_first()
                    .ok_or(SandboxPersistenceError::RuntimeState {
                        reason: "cleanup journal reserve is full",
                    })?,
                true,
            )
        };
        let encoded = serde_json::to_vec(&obligation).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "failed to encode cleanup obligation".to_string(),
                source: Some(source.into()),
            }
        })?;
        if encoded.len() >= CLEANUP_JOURNAL_SLOT_BYTES {
            return Err(SandboxPersistenceError::RuntimeState {
                reason: "cleanup obligation exceeds reserved slot",
            });
        }
        if let Err(error) = write_cleanup_slot(self.path.clone(), slot, encoded).await {
            if inserted {
                slots.free.insert(slot);
            }
            return Err(error);
        }
        slots.occupied.insert(key, slot);
        Ok(inserted)
    }

    async fn clear(&self, key: (SandboxId, CleanupKind)) -> PersistenceResult<bool> {
        let mut slots = self.slots.lock().await;
        let Some(slot) = slots.occupied.get(&key).copied() else {
            return Ok(false);
        };
        write_cleanup_slot(self.path.clone(), slot, Vec::new()).await?;
        slots.occupied.remove(&key);
        slots.free.insert(slot);
        Ok(true)
    }

    async fn obligations(&self) -> PersistenceResult<Vec<CleanupObligation>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<Vec<CleanupObligation>> {
            let mut file = std::fs::OpenOptions::new().read(true).open(path)?;
            let mut result = Vec::new();
            let mut bytes = vec![0u8; CLEANUP_JOURNAL_SLOT_BYTES];
            loop {
                match file.read_exact(&mut bytes) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(error) => return Err(error),
                }
                let payload_len = bytes
                    .iter()
                    .position(|byte| *byte == 0)
                    .unwrap_or(bytes.len());
                if payload_len == 0 {
                    continue;
                }
                result.push(
                    serde_json::from_slice(&bytes[..payload_len]).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                    })?,
                );
            }
            Ok(result)
        })
        .await
        .map_err(|source| {
            SandboxPersistenceError::store("join cleanup journal read", source.into())
        })?
        .map_err(|source| SandboxPersistenceError::io("read cleanup journal", &self.path, source))
    }

    async fn len(&self) -> u64 {
        self.slots.lock().await.occupied.len() as u64
    }
}

async fn write_cleanup_slot(path: PathBuf, slot: usize, encoded: Vec<u8>) -> PersistenceResult<()> {
    let journal_path = path.clone();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&journal_path)?;
        let mut bytes = vec![0u8; CLEANUP_JOURNAL_SLOT_BYTES];
        bytes[..encoded.len()].copy_from_slice(&encoded);
        let mut written = 0;
        let offset = (slot * CLEANUP_JOURNAL_SLOT_BYTES) as u64;
        while written < bytes.len() {
            written += file.write_at(&bytes[written..], offset + written as u64)?;
        }
        file.sync_data()
    })
    .await
    .map_err(|source| SandboxPersistenceError::store("join cleanup journal write", source.into()))?
    .map_err(|source| SandboxPersistenceError::io("write cleanup journal", path, source))
}

async fn directory_bytes(path: PathBuf) -> u64 {
    tokio::task::spawn_blocking(move || {
        let mut total = 0u64;
        let mut pending = vec![path];
        while let Some(path) = pending.pop() {
            let metadata = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => continue,
            };
            if metadata.is_file() {
                total = total.saturating_add(metadata.len());
                continue;
            }
            if !metadata.is_dir() {
                continue;
            }
            let entries = match std::fs::read_dir(path) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            pending.extend(entries.filter_map(|entry| entry.ok().map(|entry| entry.path())));
        }
        total
    })
    .await
    .unwrap_or(0)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PersistedPausedLifecycle {
    Paused,
    Resuming,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedPausedRecord {
    version: u32,
    lifecycle: PersistedPausedLifecycle,
    metadata: SandboxMetadata,
    artifact_root: PathBuf,
    #[serde(default)]
    artifact_closure: Option<RuntimeArtifactClosure>,
    state: Value,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GenerationReference {
    sandbox_id: SandboxId,
    generation: Uuid,
}

impl PersistedPausedRecord {
    fn into_metadata<F>(mut self, factory: &F) -> PersistenceResult<SandboxMetadata>
    where
        F: SandboxBackendFactory,
    {
        ensure_supported_version(self.version)?;
        self.validate_artifact_closure()?;

        let paused_state = factory
            .decode_paused_state(self.artifact_root, self.state)
            .map_err(|source| SandboxPersistenceError::InvalidRecord {
                reason: "failed to decode paused sandbox state".to_string(),
                source: Some(source),
            })?;
        self.metadata.state = SandboxState::Paused;
        self.metadata.paused_state = Some(paused_state);

        Ok(self.metadata)
    }

    fn validate_artifact_closure(&self) -> PersistenceResult<()> {
        let closure = self.artifact_closure.as_ref().ok_or_else(|| {
            SandboxPersistenceError::InvalidRecord {
                reason: "paused sandbox record has no durable runtime artifact closure".to_string(),
                source: None,
            }
        })?;
        closure
            .validate()
            .map(|_| ())
            .map_err(|source| SandboxPersistenceError::InvalidRecord {
                reason: "paused sandbox runtime artifact closure is incomplete".to_string(),
                source: Some(source),
            })
    }

    fn into_metadata_without_runtime_state(mut self) -> SandboxMetadata {
        self.metadata.state = SandboxState::Paused;
        self.metadata.paused_state = None;
        self.metadata
    }
}

fn decode_record(bytes: &[u8]) -> PersistenceResult<PersistedPausedRecord> {
    let record: PersistedPausedRecord =
        serde_json::from_slice(bytes).map_err(|source| SandboxPersistenceError::InvalidRecord {
            reason: "failed to deserialize record".to_string(),
            source: Some(source.into()),
        })?;
    ensure_supported_version(record.version)?;
    Ok(record)
}

fn ensure_supported_version(version: u32) -> PersistenceResult<()> {
    if version == RECORD_VERSION {
        Ok(())
    } else {
        Err(SandboxPersistenceError::InvalidRecord {
            reason: format!("unsupported record version {version}"),
            source: None,
        })
    }
}

pub struct FileBackedSandboxPersister {
    root: PathBuf,
    virtualization_mode: VirtualizationMode,
    durability: LocalStoreDurability,
    db: OnceCell<LocalKvStore>,
    generation_reference_index: OnceCell<()>,
    cleanup_journal: OnceCell<CleanupJournal>,
    cleanup_journal_reserve_bytes: u64,
    cleanup_retries: AtomicU64,
    cleanup_failures: AtomicU64,
    pruned_generations: AtomicU64,
    reclaimed_snapshot_bytes: AtomicU64,
    pending_cleanup: AtomicU64,
    /// Serializes durable closure replacement with destructive artifact GC.
    active_generations: Mutex<HashSet<(SandboxId, Uuid)>>,
    /// Uncommitted generations a resumed runtime still reads; locked after
    /// `active_generations`.
    runtime_retained_generations: Mutex<HashSet<(SandboxId, Uuid)>>,
    image_gc_safe: AtomicBool,
    #[cfg(test)]
    fail_remove_record: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fail_remove_artifacts: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    stall_next_resume_marker: std::sync::atomic::AtomicBool,
}

impl FileBackedSandboxPersister {
    pub fn new(root: PathBuf, virtualization_mode: VirtualizationMode) -> Self {
        Self {
            root,
            virtualization_mode,
            durability: LocalStoreDurability::Sync,
            db: OnceCell::new(),
            generation_reference_index: OnceCell::new(),
            cleanup_journal: OnceCell::new(),
            cleanup_journal_reserve_bytes: DEFAULT_CLEANUP_JOURNAL_RESERVE_BYTES,
            cleanup_retries: AtomicU64::new(0),
            cleanup_failures: AtomicU64::new(0),
            pruned_generations: AtomicU64::new(0),
            reclaimed_snapshot_bytes: AtomicU64::new(0),
            pending_cleanup: AtomicU64::new(0),
            active_generations: Mutex::new(HashSet::new()),
            runtime_retained_generations: Mutex::new(HashSet::new()),
            image_gc_safe: AtomicBool::new(true),
            #[cfg(test)]
            fail_remove_record: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_remove_artifacts: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            stall_next_resume_marker: std::sync::atomic::AtomicBool::new(false),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(root: PathBuf) -> Self {
        Self::new(root, VirtualizationMode::Kvm)
    }

    #[cfg(test)]
    pub(crate) fn stall_next_resume_marker(&self) {
        self.stall_next_resume_marker
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn with_durability(mut self, durability: LocalStoreDurability) -> Self {
        self.durability = durability;
        self
    }

    pub fn with_cleanup_journal_reserve_bytes(mut self, reserve_bytes: u64) -> Self {
        self.cleanup_journal_reserve_bytes = reserve_bytes;
        self
    }

    fn records_db_path(&self) -> PathBuf {
        self.root.join(RECORD_DB_DIR)
    }

    fn artifacts_root(&self) -> PathBuf {
        self.root.join("artifacts")
    }

    fn resume_marker_dir(&self) -> PathBuf {
        self.root.join(RESUME_MARKER_DIR)
    }

    fn resume_marker_path(&self, sandbox_id: &SandboxId) -> PathBuf {
        self.resume_marker_dir().join(sandbox_id.to_string())
    }

    async fn journal(&self) -> PersistenceResult<&CleanupJournal> {
        let journal = self
            .cleanup_journal
            .get_or_try_init(|| {
                CleanupJournal::open(
                    self.root.join(CLEANUP_JOURNAL_FILE),
                    self.cleanup_journal_reserve_bytes,
                )
            })
            .await?;
        self.pending_cleanup
            .store(journal.len().await, Ordering::Relaxed);
        Ok(journal)
    }

    fn sandbox_artifact_root(&self, sandbox_id: &SandboxId) -> PathBuf {
        self.artifacts_root().join(sandbox_id.to_string())
    }

    async fn db(&self) -> PersistenceResult<LocalKvStore> {
        self.db
            .get_or_try_init(|| async {
                LocalKvStore::open(self.records_db_path(), self.durability)
                    .await
                    .map_err(|source| SandboxPersistenceError::store("open RocksDB", source))
            })
            .await
            .cloned()
    }

    async fn get_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<PersistedPausedRecord> {
        let bytes = self
            .db()
            .await?
            .get(sandbox_id.to_string())
            .await
            .map_err(|source| SandboxPersistenceError::store("read paused sandbox record", source))?
            .ok_or_else(|| SandboxPersistenceError::InvalidRecord {
                reason: format!("paused sandbox record {sandbox_id} not found"),
                source: None,
            })?;
        decode_record(&bytes)
    }

    fn record_key(sandbox_id: &SandboxId) -> Vec<u8> {
        sandbox_id.to_string().into_bytes()
    }

    fn generation_reference_prefix(sandbox_id: SandboxId) -> Vec<u8> {
        format!("{GENERATION_REFERENCE_PREFIX}{sandbox_id}/").into_bytes()
    }

    fn generation_reference_key(reference: GenerationReference, owner: SandboxId) -> Vec<u8> {
        format!(
            "{GENERATION_REFERENCE_PREFIX}{}/{}/{}",
            reference.sandbox_id, reference.generation, owner
        )
        .into_bytes()
    }

    fn owner_generation_reference_prefix(owner: SandboxId) -> Vec<u8> {
        format!("{OWNER_GENERATION_REFERENCE_PREFIX}{owner}/").into_bytes()
    }

    fn owner_generation_reference_key(owner: SandboxId, reference: GenerationReference) -> Vec<u8> {
        format!(
            "{OWNER_GENERATION_REFERENCE_PREFIX}{}/{}/{}",
            owner, reference.sandbox_id, reference.generation
        )
        .into_bytes()
    }

    fn parse_generation_reference_key(
        key: &[u8],
    ) -> PersistenceResult<(GenerationReference, SandboxId)> {
        let value =
            std::str::from_utf8(key).map_err(|source| SandboxPersistenceError::InvalidRecord {
                reason: "generation-reference key is not UTF-8".to_string(),
                source: Some(source.into()),
            })?;
        let components = value
            .strip_prefix(GENERATION_REFERENCE_PREFIX)
            .ok_or_else(|| SandboxPersistenceError::InvalidRecord {
                reason: "generation-reference key has the wrong prefix".to_string(),
                source: None,
            })?
            .split('/')
            .collect::<Vec<_>>();
        if components.len() != 3 {
            return Err(SandboxPersistenceError::InvalidRecord {
                reason: "generation-reference key has the wrong shape".to_string(),
                source: None,
            });
        }
        let sandbox_id = SandboxId::parse_str(components[0]).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "generation-reference key has an invalid sandbox id".to_string(),
                source: Some(source.into()),
            }
        })?;
        let generation = Uuid::parse_str(components[1]).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "generation-reference key has an invalid generation".to_string(),
                source: Some(source.into()),
            }
        })?;
        let owner = SandboxId::parse_str(components[2]).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "generation-reference key has an invalid owner".to_string(),
                source: Some(source.into()),
            }
        })?;
        Ok((
            GenerationReference {
                sandbox_id,
                generation,
            },
            owner,
        ))
    }

    fn parse_owner_generation_reference_key(
        key: &[u8],
    ) -> PersistenceResult<(SandboxId, GenerationReference)> {
        let value =
            std::str::from_utf8(key).map_err(|source| SandboxPersistenceError::InvalidRecord {
                reason: "owner-generation-reference key is not UTF-8".to_string(),
                source: Some(source.into()),
            })?;
        let components = value
            .strip_prefix(OWNER_GENERATION_REFERENCE_PREFIX)
            .ok_or_else(|| SandboxPersistenceError::InvalidRecord {
                reason: "owner-generation-reference key has the wrong prefix".to_string(),
                source: None,
            })?
            .split('/')
            .collect::<Vec<_>>();
        if components.len() != 3 {
            return Err(SandboxPersistenceError::InvalidRecord {
                reason: "owner-generation-reference key has the wrong shape".to_string(),
                source: None,
            });
        }
        let owner = SandboxId::parse_str(components[0]).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "owner-generation-reference key has an invalid owner".to_string(),
                source: Some(source.into()),
            }
        })?;
        let sandbox_id = SandboxId::parse_str(components[1]).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "owner-generation-reference key has an invalid sandbox id".to_string(),
                source: Some(source.into()),
            }
        })?;
        let generation = Uuid::parse_str(components[2]).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "owner-generation-reference key has an invalid generation".to_string(),
                source: Some(source.into()),
            }
        })?;
        Ok((
            owner,
            GenerationReference {
                sandbox_id,
                generation,
            },
        ))
    }

    fn generation_reference_from_path(
        path: &Path,
        artifact_roots: &[PathBuf],
    ) -> PersistenceResult<Option<GenerationReference>> {
        let Some(relative) = artifact_roots
            .iter()
            .find_map(|root| path.strip_prefix(root).ok())
        else {
            return Ok(None);
        };
        let mut components = relative.components();
        let sandbox_id = components
            .next()
            .and_then(|component| component.as_os_str().to_str())
            .and_then(|value| SandboxId::parse_str(value).ok())
            .ok_or_else(|| SandboxPersistenceError::InvalidRecord {
                reason: format!(
                    "persisted runtime artifact has no sandbox id under {}: {}",
                    artifact_roots[0].display(),
                    path.display()
                ),
                source: None,
            })?;
        let generation = components
            .next()
            .and_then(|component| component.as_os_str().to_str())
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(|| SandboxPersistenceError::InvalidRecord {
                reason: format!(
                    "persisted runtime artifact has no generation under {}: {}",
                    artifact_roots[0].display(),
                    path.display()
                ),
                source: None,
            })?;
        Ok(Some(GenerationReference {
            sandbox_id,
            generation,
        }))
    }

    fn generation_references_for_record(
        &self,
        record: &PersistedPausedRecord,
    ) -> PersistenceResult<BTreeSet<GenerationReference>> {
        let closure = record.artifact_closure.as_ref().ok_or_else(|| {
            SandboxPersistenceError::InvalidRecord {
                reason: "paused sandbox record has no durable runtime artifact closure".to_string(),
                source: None,
            }
        })?;
        let artifact_root = self.artifacts_root();
        let canonical_artifact_root =
            std::fs::canonicalize(&artifact_root).unwrap_or_else(|_| artifact_root.clone());
        let artifact_roots = [artifact_root, canonical_artifact_root];
        let mut references = BTreeSet::new();
        for path in std::iter::once(record.artifact_root.as_path()).chain(closure.paths()) {
            if let Some(reference) = Self::generation_reference_from_path(path, &artifact_roots)? {
                references.insert(reference);
            }
        }
        Ok(references)
    }

    async fn record_entries(&self) -> PersistenceResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.db()
            .await?
            .entries()
            .await
            .map(|entries| {
                entries
                    .into_iter()
                    .filter(|(key, _)| {
                        std::str::from_utf8(key)
                            .ok()
                            .and_then(|value| SandboxId::parse_str(value).ok())
                            .is_some()
                    })
                    .collect()
            })
            .map_err(|source| SandboxPersistenceError::store("scan paused sandbox records", source))
    }

    async fn indexed_references_for_owner(
        &self,
        owner: SandboxId,
    ) -> PersistenceResult<BTreeSet<GenerationReference>> {
        let entries = self
            .db()
            .await?
            .scan_prefix(Self::owner_generation_reference_prefix(owner))
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("scan owner generation references", source)
            })?;
        entries
            .into_iter()
            .map(|(key, _)| {
                let (indexed_owner, reference) = Self::parse_owner_generation_reference_key(&key)?;
                if indexed_owner != owner {
                    return Err(SandboxPersistenceError::InvalidRecord {
                        reason: "owner-generation-reference prefix returned another owner"
                            .to_string(),
                        source: None,
                    });
                }
                Ok(reference)
            })
            .collect()
    }

    fn replace_reference_ops(
        owner: SandboxId,
        old: &BTreeSet<GenerationReference>,
        new: &BTreeSet<GenerationReference>,
    ) -> Vec<LocalKvBatchOp> {
        let mut ops = Vec::new();
        for reference in old.difference(new) {
            ops.push(LocalKvBatchOp::delete(Self::generation_reference_key(
                *reference, owner,
            )));
            ops.push(LocalKvBatchOp::delete(
                Self::owner_generation_reference_key(owner, *reference),
            ));
        }
        for reference in new.difference(old) {
            ops.push(LocalKvBatchOp::put(
                Self::generation_reference_key(*reference, owner),
                [],
            ));
            ops.push(LocalKvBatchOp::put(
                Self::owner_generation_reference_key(owner, *reference),
                [],
            ));
        }
        ops
    }

    async fn initialize_generation_reference_index(&self) -> PersistenceResult<()> {
        let db = self.db().await?;
        if db
            .get(GENERATION_REFERENCE_INDEX_VERSION_KEY)
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("read generation-reference index version", source)
            })?
            .as_deref()
            == Some(GENERATION_REFERENCE_INDEX_VERSION)
        {
            return Ok(());
        }

        let records = self.record_entries().await?;
        let reverse_entries = db
            .scan_prefix(GENERATION_REFERENCE_PREFIX.as_bytes())
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("scan generation-reference index", source)
            })?;
        let forward_entries = db
            .scan_prefix(OWNER_GENERATION_REFERENCE_PREFIX.as_bytes())
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("scan owner generation-reference index", source)
            })?;
        let mut ops = reverse_entries
            .into_iter()
            .chain(forward_entries)
            .map(|(key, _)| LocalKvBatchOp::delete(key))
            .collect::<Vec<_>>();
        let mut references = 0usize;
        let mut complete = true;
        for (key, bytes) in &records {
            let owner = std::str::from_utf8(key)
                .ok()
                .and_then(|value| SandboxId::parse_str(value).ok())
                .expect("record_entries returned a non-record key");
            let record = match decode_record(bytes) {
                Ok(record) => record,
                Err(error) => {
                    complete = false;
                    warn!(
                        owner = %owner,
                        error = %error,
                        "generation-reference index skipped invalid paused record"
                    );
                    continue;
                }
            };
            let record_references = match self.generation_references_for_record(&record) {
                Ok(references) => references,
                Err(error) => {
                    complete = false;
                    warn!(
                        owner = %owner,
                        error = %error,
                        "generation-reference index skipped incomplete paused record"
                    );
                    continue;
                }
            };
            references += record_references.len();
            ops.extend(Self::replace_reference_ops(
                owner,
                &BTreeSet::new(),
                &record_references,
            ));
        }
        if complete {
            ops.push(LocalKvBatchOp::put(
                GENERATION_REFERENCE_INDEX_VERSION_KEY,
                GENERATION_REFERENCE_INDEX_VERSION,
            ));
        } else {
            ops.push(LocalKvBatchOp::delete(
                GENERATION_REFERENCE_INDEX_VERSION_KEY,
            ));
            self.image_gc_safe.store(false, Ordering::Release);
        }
        db.write_batch(ops).await.map_err(|source| {
            SandboxPersistenceError::store("build generation-reference index", source)
        })?;
        info!(
            records = records.len(),
            references, complete, "built generation-reference index"
        );
        Ok(())
    }

    async fn ensure_generation_reference_index(&self) -> PersistenceResult<()> {
        self.generation_reference_index
            .get_or_try_init(|| self.initialize_generation_reference_index())
            .await
            .map(|_| ())
    }

    async fn record_cleanup(&self, obligation: CleanupObligation) -> PersistenceResult<()> {
        if self.journal().await?.record(obligation).await? {
            self.pending_cleanup.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn clear_cleanup(
        &self,
        sandbox_id: SandboxId,
        kind: CleanupKind,
    ) -> PersistenceResult<()> {
        if self.journal().await?.clear((sandbox_id, kind)).await? {
            self.pending_cleanup.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn record_prune_cleanup(
        &self,
        sandbox_id: SandboxId,
        keep_artifact_root: &Path,
    ) -> PersistenceResult<()> {
        let keep_generation = keep_artifact_root
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| Uuid::parse_str(name).ok())
            .ok_or_else(|| SandboxPersistenceError::InvalidRecord {
                reason: "paused artifact generation is not a UUID".to_string(),
                source: None,
            })?;
        self.record_cleanup(CleanupObligation {
            sandbox_id,
            kind: CleanupKind::PruneGenerations,
            keep_generation: Some(keep_generation),
        })
        .await
    }

    async fn record_final_cleanup(&self, sandbox_id: SandboxId) -> PersistenceResult<()> {
        self.record_cleanup(CleanupObligation {
            sandbox_id,
            kind: CleanupKind::FinalDelete,
            keep_generation: None,
        })
        .await
    }

    async fn put_record(&self, record: &PersistedPausedRecord) -> PersistenceResult<()> {
        let bytes = serde_json::to_vec(record).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "failed to serialize record".to_string(),
                source: Some(source.into()),
            }
        })?;

        self.ensure_generation_reference_index().await?;
        let owner = record.metadata.id;
        let old = self.indexed_references_for_owner(owner).await?;
        let new = self.generation_references_for_record(record)?;
        let mut ops = Self::replace_reference_ops(owner, &old, &new);
        ops.push(LocalKvBatchOp::put(Self::record_key(&owner), bytes));
        self.db().await?.write_batch(ops).await.map_err(|source| {
            SandboxPersistenceError::store("persist paused sandbox record", source)
        })
    }

    async fn remove_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        #[cfg(test)]
        if self
            .fail_remove_record
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(SandboxPersistenceError::store(
                "remove paused sandbox record",
                std::io::Error::from_raw_os_error(libc::ENOSPC).into(),
            ));
        }
        self.ensure_generation_reference_index().await?;
        let old = self.indexed_references_for_owner(*sandbox_id).await?;
        let mut ops = Self::replace_reference_ops(*sandbox_id, &old, &BTreeSet::new());
        ops.push(LocalKvBatchOp::delete(Self::record_key(sandbox_id)));
        self.db().await?.write_batch(ops).await.map_err(|source| {
            SandboxPersistenceError::store("remove paused sandbox record", source)
        })?;
        self.clear_resume_marker(sandbox_id).await
    }

    async fn write_resume_marker(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        let marker_dir = self.resume_marker_dir();
        fs::create_dir_all(&marker_dir).await.map_err(|source| {
            SandboxPersistenceError::io("create resume marker directory", &marker_dir, source)
        })?;
        let marker_path = self.resume_marker_path(sandbox_id);
        let staged_path = marker_dir.join(format!(".{}.{}.tmp", sandbox_id, Uuid::new_v4()));
        #[cfg(test)]
        let stall = self
            .stall_next_resume_marker
            .swap(false, std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let stall = false;
        let staged_path_for_write = staged_path.clone();
        let write_staged = async move {
            let mut marker = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&staged_path_for_write)
                .await
                .map_err(|source| {
                    SandboxPersistenceError::io(
                        "create staged resume marker",
                        &staged_path_for_write,
                        source,
                    )
                })?;
            marker.write_all(b"resuming").await.map_err(|source| {
                SandboxPersistenceError::io(
                    "write staged resume marker",
                    &staged_path_for_write,
                    source,
                )
            })?;
            if stall {
                tokio::task::spawn_blocking(|| {
                    std::thread::sleep(RESUME_MARKER_TIMEOUT * 5);
                })
                .await
                .map_err(|source| {
                    SandboxPersistenceError::store(
                        "join stalled resume marker test task",
                        source.into(),
                    )
                })?;
            }
            marker.sync_all().await.map_err(|source| {
                SandboxPersistenceError::io(
                    "sync staged resume marker",
                    &staged_path_for_write,
                    source,
                )
            })
        };
        match tokio::time::timeout(RESUME_MARKER_TIMEOUT, write_staged).await {
            Ok(result) => result?,
            Err(_) => {
                let _ = fs::remove_file(&staged_path).await;
                return Err(SandboxPersistenceError::RuntimeState {
                    reason: "persist resume marker timed out",
                });
            }
        }
        fs::rename(&staged_path, &marker_path)
            .await
            .map_err(|source| {
                SandboxPersistenceError::io("publish resume marker", &marker_path, source)
            })
    }

    async fn clear_resume_marker(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        let marker_path = self.resume_marker_path(sandbox_id);
        match fs::remove_file(&marker_path).await {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(SandboxPersistenceError::io(
                "clear resume marker",
                &marker_path,
                source,
            )),
        }
    }

    async fn resume_markers(&self) -> PersistenceResult<HashSet<SandboxId>> {
        let marker_dir = self.resume_marker_dir();
        fs::create_dir_all(&marker_dir).await.map_err(|source| {
            SandboxPersistenceError::io("create resume marker directory", &marker_dir, source)
        })?;
        let mut entries = fs::read_dir(&marker_dir).await.map_err(|source| {
            SandboxPersistenceError::io("scan resume marker directory", &marker_dir, source)
        })?;
        let mut markers = HashSet::new();
        while let Some(entry) = entries.next_entry().await.map_err(|source| {
            SandboxPersistenceError::io("read resume marker directory", &marker_dir, source)
        })? {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(sandbox_id) = SandboxId::parse_str(&name) else {
                continue;
            };
            markers.insert(sandbox_id);
        }
        Ok(markers)
    }

    async fn remove_artifact_root(&self, path: &Path) -> PersistenceResult<()> {
        #[cfg(test)]
        if self
            .fail_remove_artifacts
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
        {
            return Err(SandboxPersistenceError::io(
                "remove paused sandbox artifacts",
                path,
                std::io::Error::other("forced artifact cleanup failure"),
            ));
        }
        match fs::remove_dir_all(path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(SandboxPersistenceError::io(
                "remove paused sandbox artifacts",
                path,
                source,
            )),
        }
    }

    fn generation_id(path: &Path) -> PersistenceResult<Uuid> {
        path.file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| Uuid::parse_str(name).ok())
            .ok_or_else(|| SandboxPersistenceError::InvalidRecord {
                reason: format!(
                    "paused artifact generation is not a UUID: {}",
                    path.display()
                ),
                source: None,
            })
    }

    fn validate_closure_containment(
        artifact_root: &Path,
        closure: &RuntimeArtifactClosure,
    ) -> PersistenceResult<()> {
        let artifact_root = std::fs::canonicalize(artifact_root).map_err(|source| {
            SandboxPersistenceError::io("resolve paused artifact generation", artifact_root, source)
        })?;
        let commit_store = std::fs::canonicalize(
            ConfigManager::global_config()
                .image_cache_layout()
                .commit_store,
        )
        .ok();
        for path in closure.paths() {
            let in_generation = path.starts_with(&artifact_root);
            let in_commit_store = commit_store
                .as_ref()
                .is_some_and(|commit_store| path.starts_with(commit_store));
            if !in_generation && !in_commit_store {
                return Err(SandboxPersistenceError::InvalidRecord {
                    reason: format!(
                        "paused runtime artifact {} is outside its generation and the image commit store",
                        path.display()
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }

    async fn protected_generations(
        &self,
        target_sandbox_id: SandboxId,
        exclude_record: Option<SandboxId>,
    ) -> PersistenceResult<HashSet<Uuid>> {
        self.ensure_generation_reference_index().await?;
        if !self.image_gc_safe.load(Ordering::Acquire) {
            return self
                .protected_generations_by_record_scan(target_sandbox_id, exclude_record)
                .await;
        }
        let indexed = self
            .db()
            .await?
            .scan_prefix(Self::generation_reference_prefix(target_sandbox_id))
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("lookup generation references", source)
            })?;
        let mut protected = HashSet::new();

        for (key, _) in indexed {
            let (reference, owner) = Self::parse_generation_reference_key(&key)?;
            if exclude_record == Some(owner) {
                continue;
            }
            let record = self.get_record(&owner).await?;
            let closure = record.artifact_closure.as_ref().ok_or_else(|| {
                SandboxPersistenceError::InvalidRecord {
                    reason: "paused sandbox record has no durable runtime artifact closure"
                        .to_string(),
                    source: None,
                }
            })?;
            closure
                .protected_paths()
                .map_err(|source| SandboxPersistenceError::InvalidRecord {
                    reason: "paused sandbox runtime artifact closure is incomplete".to_string(),
                    source: Some(source),
                })?;
            let owner_references = self.generation_references_for_record(&record)?;
            if !owner_references.contains(&reference) {
                return Err(SandboxPersistenceError::InvalidRecord {
                    reason: format!(
                        "generation-reference index entry is stale: owner {owner} no longer references {}/{}",
                        reference.sandbox_id, reference.generation
                    ),
                    source: None,
                });
            }
            protected.insert(reference.generation);
        }
        Ok(protected)
    }

    async fn protected_generations_by_record_scan(
        &self,
        target_sandbox_id: SandboxId,
        exclude_record: Option<SandboxId>,
    ) -> PersistenceResult<HashSet<Uuid>> {
        let target_root = self.sandbox_artifact_root(&target_sandbox_id);
        let canonical_target_root = match fs::canonicalize(&target_root).await {
            Ok(path) => path,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(HashSet::new());
            }
            Err(source) => {
                return Err(SandboxPersistenceError::io(
                    "resolve sandbox artifact root",
                    &target_root,
                    source,
                ));
            }
        };
        // Durable closures stay inside their own generation or the commit
        // store, so an unresolvable record only affects its own sandbox.
        let mut protected = HashSet::new();
        let mut protect_all = false;
        for (key, bytes) in self.record_entries().await? {
            let owner = std::str::from_utf8(&key)
                .ok()
                .and_then(|value| SandboxId::parse_str(value).ok())
                .expect("record_entries returned a non-record key");
            if exclude_record == Some(owner) {
                continue;
            }
            let paths = match Self::record_protected_paths(&bytes) {
                Ok(paths) => paths,
                Err(error) if owner == target_sandbox_id => {
                    warn!(sandbox_id = %owner, error = %error, "paused record cannot be resolved; protecting all of its generations");
                    protect_all = true;
                    continue;
                }
                Err(error) => {
                    warn!(owner = %owner, target = %target_sandbox_id, error = %error, "unresolvable paused record cannot reference another sandbox; skipping it");
                    continue;
                }
            };

            for path in paths {
                let resolved = fs::canonicalize(&path).await;
                let relative = match &resolved {
                    Ok(canonical) => canonical.strip_prefix(&canonical_target_root).ok(),
                    Err(_) => path
                        .strip_prefix(&canonical_target_root)
                        .or_else(|_| path.strip_prefix(&target_root))
                        .ok(),
                };
                let Some(relative) = relative else {
                    continue;
                };
                let generation = relative
                    .components()
                    .next()
                    .and_then(|component| component.as_os_str().to_str())
                    .and_then(|name| Uuid::parse_str(name).ok());
                match generation {
                    Some(generation) => {
                        protected.insert(generation);
                    }
                    None => {
                        warn!(owner = %owner, artifact = %path.display(), "persisted runtime artifact is not inside a UUID generation; protecting all generations");
                        protect_all = true;
                    }
                }
            }
        }
        if protect_all {
            protected.extend(Self::generation_dirs(&canonical_target_root).await?);
        }
        Ok(protected)
    }

    fn record_protected_paths(bytes: &[u8]) -> PersistenceResult<Vec<PathBuf>> {
        let record = decode_record(bytes)?;
        let closure = record.artifact_closure.as_ref().ok_or_else(|| {
            SandboxPersistenceError::InvalidRecord {
                reason: "paused sandbox record has no durable runtime artifact closure".to_string(),
                source: None,
            }
        })?;
        let closure_paths =
            closure
                .protected_paths()
                .map_err(|source| SandboxPersistenceError::InvalidRecord {
                    reason: "paused sandbox runtime artifact closure is incomplete".to_string(),
                    source: Some(source),
                })?;
        Ok(std::iter::once(record.artifact_root.clone())
            .chain(closure_paths)
            .collect())
    }

    async fn generation_dirs(sandbox_root: &Path) -> PersistenceResult<HashSet<Uuid>> {
        let mut generations = HashSet::new();
        let mut entries = fs::read_dir(sandbox_root).await.map_err(|source| {
            SandboxPersistenceError::io("scan paused sandbox generations", sandbox_root, source)
        })?;
        while let Some(entry) = entries.next_entry().await.map_err(|source| {
            SandboxPersistenceError::io("scan paused sandbox generations", sandbox_root, source)
        })? {
            if let Some(generation) = entry
                .file_name()
                .to_str()
                .and_then(|name| Uuid::parse_str(name).ok())
            {
                generations.insert(generation);
            }
        }
        Ok(generations)
    }

    async fn prune_generations(
        &self,
        sandbox_id: &SandboxId,
        keep_artifact_root: &Path,
    ) -> PersistenceResult<()> {
        let active_generations = self.active_generations.lock().await;
        let runtime_retained = self.runtime_retained_generations.lock().await;
        let sandbox_root = self.sandbox_artifact_root(sandbox_id);
        if keep_artifact_root.parent() != Some(sandbox_root.as_path()) {
            return Err(SandboxPersistenceError::InvalidRecord {
                reason: format!(
                    "paused artifact root {} is not a direct child of {}",
                    keep_artifact_root.display(),
                    sandbox_root.display()
                ),
                source: None,
            });
        }

        let canonical_sandbox_root = fs::canonicalize(&sandbox_root).await.map_err(|source| {
            SandboxPersistenceError::io(
                "resolve paused sandbox artifact root",
                &sandbox_root,
                source,
            )
        })?;
        let canonical_keep = fs::canonicalize(keep_artifact_root)
            .await
            .map_err(|source| {
                SandboxPersistenceError::io(
                    "resolve current paused sandbox generation",
                    keep_artifact_root,
                    source,
                )
            })?;
        if canonical_keep.parent() != Some(canonical_sandbox_root.as_path()) {
            return Err(SandboxPersistenceError::InvalidRecord {
                reason: format!(
                    "paused artifact root {} escapes {}",
                    keep_artifact_root.display(),
                    sandbox_root.display()
                ),
                source: None,
            });
        }
        let keep_generation = Self::generation_id(&canonical_keep)?;
        let mut protected = self.protected_generations(*sandbox_id, None).await?;
        protected.insert(keep_generation);

        let mut entries = fs::read_dir(&canonical_sandbox_root)
            .await
            .map_err(|source| {
                SandboxPersistenceError::io(
                    "scan paused sandbox generations",
                    &canonical_sandbox_root,
                    source,
                )
            })?;
        while let Some(entry) = entries.next_entry().await.map_err(|source| {
            SandboxPersistenceError::io(
                "scan paused sandbox generations",
                &canonical_sandbox_root,
                source,
            )
        })? {
            let path = entry.path();
            if path == canonical_keep {
                continue;
            }
            let file_type = entry.file_type().await.map_err(|source| {
                SandboxPersistenceError::io("inspect paused sandbox generation", &path, source)
            })?;
            let is_uuid_generation = entry
                .file_name()
                .to_str()
                .and_then(|name| Uuid::parse_str(name).ok());
            let Some(generation) = is_uuid_generation else {
                continue;
            };
            if !file_type.is_dir()
                || protected.contains(&generation)
                || active_generations.contains(&(*sandbox_id, generation))
                || runtime_retained.contains(&(*sandbox_id, generation))
            {
                continue;
            }
            debug!(
                sandbox_id = %sandbox_id,
                artifact_root = %path.display(),
                "removing superseded paused sandbox generation"
            );
            let reclaimed = directory_bytes(path.clone()).await;
            self.remove_artifact_root(&path).await?;
            self.pruned_generations.fetch_add(1, Ordering::Relaxed);
            self.reclaimed_snapshot_bytes
                .fetch_add(reclaimed, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn finish_final_cleanup(&self, sandbox_id: SandboxId) -> PersistenceResult<()> {
        let active_generations = self.active_generations.lock().await;
        if active_generations
            .iter()
            .any(|(active_sandbox_id, _)| *active_sandbox_id == sandbox_id)
        {
            return Err(SandboxPersistenceError::InvalidRecord {
                reason: format!(
                    "sandbox {sandbox_id} has an active uncommitted artifact generation"
                ),
                source: None,
            });
        }
        let external_holds = self
            .protected_generations(sandbox_id, Some(sandbox_id))
            .await?;
        if !external_holds.is_empty() {
            return Err(SandboxPersistenceError::InvalidRecord {
                reason: format!(
                    "sandbox {sandbox_id} artifacts remain referenced by another durable paused sandbox"
                ),
                source: None,
            });
        }
        let artifact_root = self.sandbox_artifact_root(&sandbox_id);
        let reclaimed = directory_bytes(artifact_root.clone()).await;
        let artifacts_result = self.remove_artifact_root(&artifact_root).await;
        let record_result = self.remove_record(&sandbox_id).await;
        match (record_result, artifacts_result) {
            (Ok(()), Ok(())) => {
                // Final delete runs after the runtime stopped, so retained
                // generations are no longer read.
                self.runtime_retained_generations
                    .lock()
                    .await
                    .retain(|(retained_sandbox_id, _)| *retained_sandbox_id != sandbox_id);
                self.reclaimed_snapshot_bytes
                    .fetch_add(reclaimed, Ordering::Relaxed);
                self.clear_cleanup(sandbox_id, CleanupKind::FinalDelete)
                    .await
            }
            (Err(record), Ok(())) => Err(record),
            (Ok(()), Err(artifacts)) => Err(artifacts),
            (Err(record), Err(artifacts)) => Err(SandboxPersistenceError::Cleanup {
                record: Box::new(record),
                artifacts: Box::new(artifacts),
            }),
        }
    }

    async fn replay_obligation(&self, obligation: CleanupObligation) -> PersistenceResult<()> {
        self.cleanup_retries.fetch_add(1, Ordering::Relaxed);
        match obligation.kind {
            CleanupKind::FinalDelete => self.finish_final_cleanup(obligation.sandbox_id).await,
            CleanupKind::PruneGenerations => {
                let record = match self.get_record(&obligation.sandbox_id).await {
                    Ok(record) => record,
                    Err(SandboxPersistenceError::InvalidRecord { .. }) => {
                        return self
                            .clear_cleanup(obligation.sandbox_id, CleanupKind::PruneGenerations)
                            .await;
                    }
                    Err(error) => return Err(error),
                };
                if record.lifecycle != PersistedPausedLifecycle::Paused {
                    return self
                        .clear_cleanup(obligation.sandbox_id, CleanupKind::PruneGenerations)
                        .await;
                }
                let expected_generation = obligation.keep_generation.ok_or_else(|| {
                    SandboxPersistenceError::InvalidRecord {
                        reason: "prune cleanup obligation has no retained generation".to_string(),
                        source: None,
                    }
                })?;
                let record_generation = record
                    .artifact_root
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| Uuid::parse_str(name).ok());
                if record_generation != Some(expected_generation) {
                    return Err(SandboxPersistenceError::InvalidRecord {
                        reason: "prune cleanup obligation disagrees with paused record".to_string(),
                        source: None,
                    });
                }
                self.prune_generations(&obligation.sandbox_id, &record.artifact_root)
                    .await?;
                self.clear_cleanup(obligation.sandbox_id, CleanupKind::PruneGenerations)
                    .await
            }
        }
    }
}

#[async_trait]
impl SandboxPersister for FileBackedSandboxPersister {
    async fn load_all<F>(&self, factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        info!(store = %self.root.display(), "loading paused sandbox records");
        self.image_gc_safe.store(true, Ordering::Release);
        self.journal().await?;
        self.ensure_generation_reference_index().await?;
        if let Err(error) = self.replay_cleanup_obligations().await {
            warn!(error = ?error, "durable sandbox cleanup replay remains incomplete; preserving artifacts");
        }
        let pending_final_deletes: HashSet<SandboxId> = self
            .journal()
            .await?
            .obligations()
            .await?
            .into_iter()
            .filter(|obligation| obligation.kind == CleanupKind::FinalDelete)
            .map(|obligation| obligation.sandbox_id)
            .collect();
        let resuming_markers = self.resume_markers().await?;
        let records = self.record_entries().await?;
        let mut record_ids = HashSet::new();
        let mut sandboxes = Vec::new();
        let mut retained_artifacts = 0usize;

        for (key, bytes) in records {
            let sandbox_id_from_key = std::str::from_utf8(&key)
                .ok()
                .and_then(|value| SandboxId::parse_str(value).ok());
            if sandbox_id_from_key.is_some_and(|id| pending_final_deletes.contains(&id)) {
                warn!(sandbox_id = ?sandbox_id_from_key, "skipping sandbox with a durable final-delete tombstone");
                continue;
            }
            let mut record = match decode_record(&bytes) {
                Ok(record) => record,
                Err(err) => {
                    self.image_gc_safe.store(false, Ordering::Release);
                    warn!(record_key = %String::from_utf8_lossy(&key), error = %err, sandbox_id = ?sandbox_id_from_key, "invalid paused sandbox record is being preserved");
                    continue;
                }
            };
            let sandbox_id = record.metadata.id;
            record_ids.insert(sandbox_id);
            let artifact_root = record.artifact_root.clone();

            if record.lifecycle == PersistedPausedLifecycle::Resuming
                || resuming_markers.contains(&sandbox_id)
            {
                warn!(sandbox_id = %sandbox_id, "rolling interrupted resume back to the last durable paused generation");
                if record.lifecycle == PersistedPausedLifecycle::Resuming {
                    record.lifecycle = PersistedPausedLifecycle::Paused;
                    if let Err(error) = self.put_record(&record).await {
                        warn!(sandbox_id = %sandbox_id, error = ?error, "failed to persist interrupted resume rollback; preserving paused artifacts");
                    }
                }
                if let Err(error) = self.clear_resume_marker(&sandbox_id).await {
                    warn!(sandbox_id = %sandbox_id, error = ?error, "failed to clear interrupted resume marker; preserving paused artifacts");
                }
            }

            if record.metadata.virtualization_mode != self.virtualization_mode {
                self.image_gc_safe.store(false, Ordering::Release);
                warn!(
                    sandbox_id = %sandbox_id,
                    record_mode = %record.metadata.virtualization_mode,
                    node_mode = %self.virtualization_mode,
                    "loading paused sandbox metadata without resumable runtime state because its virtualization mode is incompatible"
                );
                retained_artifacts += 1;
                sandboxes.push(record.into_metadata_without_runtime_state());
                continue;
            }

            match record.clone().into_metadata(factory) {
                Ok(metadata) => {
                    if let Err(error) = self
                        .prune_artifact_generations(&sandbox_id, Some(&artifact_root))
                        .await
                    {
                        warn!(sandbox_id = %sandbox_id, error = ?error, "startup generation pruning remains incomplete; preserving artifacts");
                    }
                    retained_artifacts += 1;
                    sandboxes.push(metadata);
                }
                Err(err) => {
                    self.image_gc_safe.store(false, Ordering::Release);
                    warn!(sandbox_id = %sandbox_id, error = %err, "paused sandbox is not resumable; preserving its record and artifacts");
                    retained_artifacts += 1;
                    sandboxes.push(record.into_metadata_without_runtime_state());
                }
            }
        }

        for sandbox_id in resuming_markers.difference(&record_ids) {
            if pending_final_deletes.contains(sandbox_id) {
                continue;
            }
            self.clear_resume_marker(sandbox_id).await?;
        }

        info!(
            loaded = sandboxes.len(),
            retained = retained_artifacts,
            "loaded paused sandbox records"
        );

        Ok(sandboxes)
    }

    async fn load_recovery<F>(
        &self,
        sandbox_id: &SandboxId,
        factory: &F,
    ) -> PersistenceResult<Option<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        let bytes = self
            .db()
            .await?
            .get(Self::record_key(sandbox_id))
            .await
            .map_err(|source| SandboxPersistenceError::store("read recovery checkpoint", source))?;
        let Some(bytes) = bytes else { return Ok(None) };
        let record = decode_record(&bytes)?;
        if record.metadata.id != *sandbox_id
            || record.metadata.virtualization_mode != self.virtualization_mode
        {
            return Err(SandboxPersistenceError::RuntimeState {
                reason: "recovery checkpoint identity or virtualization mode mismatch",
            });
        }
        record.into_metadata(factory).map(Some)
    }

    async fn allocate_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        let generation = Uuid::now_v7();
        let artifact_root = self
            .sandbox_artifact_root(sandbox_id)
            .join(generation.to_string());
        let mut active_generations = self.active_generations.lock().await;
        active_generations.insert((*sandbox_id, generation));
        if let Err(source) = fs::create_dir_all(&artifact_root).await {
            active_generations.remove(&(*sandbox_id, generation));
            return Err(SandboxPersistenceError::io(
                "allocate paused sandbox artifact root",
                &artifact_root,
                source,
            ));
        }
        Ok(Some(artifact_root))
    }

    async fn discard_artifact_generation(
        &self,
        sandbox_id: &SandboxId,
        artifact_root: Option<&Path>,
    ) -> PersistenceResult<()> {
        let Some(artifact_root) = artifact_root else {
            return Ok(());
        };
        let sandbox_root = self.sandbox_artifact_root(sandbox_id);
        if artifact_root.parent() != Some(sandbox_root.as_path()) {
            return Err(SandboxPersistenceError::InvalidRecord {
                reason: format!(
                    "uncommitted artifact root {} is not a direct child of {}",
                    artifact_root.display(),
                    sandbox_root.display()
                ),
                source: None,
            });
        }
        let generation = Self::generation_id(artifact_root)?;
        let mut active_generations = self.active_generations.lock().await;
        if let Ok(record) = self.get_record(sandbox_id).await {
            if record.artifact_root == artifact_root {
                return Err(SandboxPersistenceError::InvalidRecord {
                    reason: "refusing to discard the durable paused generation".to_string(),
                    source: None,
                });
            }
        }
        let result = self.remove_artifact_root(artifact_root).await;
        active_generations.remove(&(*sandbox_id, generation));
        result
    }

    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        artifact_root: Option<&Path>,
        paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        let artifact_root = artifact_root.ok_or_else(|| SandboxPersistenceError::RuntimeState {
            reason: "file-backed persister requires an allocated artifact root",
        })?;
        debug!(
            sandbox_id = %metadata.id,
            artifact_root = %artifact_root.display(),
            "persisting paused sandbox"
        );
        let generation = Self::generation_id(artifact_root)?;
        let artifact_closure =
            paused_state
                .runtime_artifacts()
                .resolve_closure()
                .map_err(|source| SandboxPersistenceError::InvalidRecord {
                    reason: "failed to resolve paused sandbox runtime artifact closure".to_string(),
                    source: Some(source),
                });
        // On failure the generation stays allocated: the caller discards it or,
        // if the runtime resumed on top of it, retains it.
        let state =
            paused_state
                .encode()
                .map_err(|source| SandboxPersistenceError::InvalidRecord {
                    reason: "failed to encode paused sandbox state".to_string(),
                    source: Some(source),
                })?;
        let artifact_closure = artifact_closure?;
        Self::validate_closure_containment(artifact_root, &artifact_closure)?;
        let record = PersistedPausedRecord {
            version: RECORD_VERSION,
            lifecycle: PersistedPausedLifecycle::Paused,
            metadata: metadata.clone(),
            artifact_root: artifact_root.to_path_buf(),
            artifact_closure: Some(artifact_closure),
            state,
        };
        let mut active_generations = self.active_generations.lock().await;
        self.put_record(&record).await?;
        active_generations.remove(&(metadata.id, generation));
        // The new generation links every runtime-owned lower it inherited and
        // the runtime is paused, so earlier retained generations may be pruned.
        self.runtime_retained_generations
            .lock()
            .await
            .retain(|(retained_sandbox_id, _)| *retained_sandbox_id != metadata.id);
        if let Err(error) = self.clear_resume_marker(&metadata.id).await {
            warn!(sandbox_id = %metadata.id, error = ?error, "paused record is durable but its stale resume marker could not be cleared");
        }
        Ok(())
    }

    async fn retain_runtime_generation(
        &self,
        sandbox_id: &SandboxId,
        artifact_root: Option<&Path>,
    ) -> PersistenceResult<()> {
        let Some(artifact_root) = artifact_root else {
            return Ok(());
        };
        let generation = Self::generation_id(artifact_root)?;
        let mut active_generations = self.active_generations.lock().await;
        self.runtime_retained_generations
            .lock()
            .await
            .insert((*sandbox_id, generation));
        active_generations.remove(&(*sandbox_id, generation));
        Ok(())
    }

    async fn prune_artifact_generations(
        &self,
        sandbox_id: &SandboxId,
        keep_artifact_root: Option<&Path>,
    ) -> PersistenceResult<()> {
        let keep_artifact_root =
            keep_artifact_root.ok_or_else(|| SandboxPersistenceError::RuntimeState {
                reason: "file-backed persister requires a current artifact root",
            })?;
        self.record_prune_cleanup(*sandbox_id, keep_artifact_root)
            .await?;
        match self.prune_generations(sandbox_id, keep_artifact_root).await {
            Ok(()) => {
                self.clear_cleanup(*sandbox_id, CleanupKind::PruneGenerations)
                    .await
            }
            Err(error) => {
                self.cleanup_failures.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "marking paused sandbox as resuming");
        self.write_resume_marker(sandbox_id).await
    }

    async fn rollback_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "rolling back paused sandbox to paused");
        self.clear_resume_marker(sandbox_id).await
    }

    async fn complete_resume(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "retaining paused recovery record after resume");
        self.clear_resume_marker(sandbox_id).await
    }

    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "deleting paused sandbox record and artifacts");
        self.record_final_cleanup(*sandbox_id).await?;
        match self.finish_final_cleanup(*sandbox_id).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.cleanup_failures.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    async fn delete_if_persisted(&self, sandbox_id: &SandboxId) -> PersistenceResult<bool> {
        let exists = self
            .db()
            .await?
            .get(sandbox_id.to_string())
            .await
            .map_err(|source| SandboxPersistenceError::store("read raw paused record", source))?
            .is_some();
        if !exists {
            return Ok(false);
        }
        self.delete_record_and_artifacts(sandbox_id).await?;
        Ok(true)
    }

    async fn replay_cleanup_obligations(&self) -> PersistenceResult<Vec<SandboxId>> {
        let obligations = self.journal().await?.obligations().await?;
        let mut finalized = Vec::new();
        for obligation in obligations {
            match self.replay_obligation(obligation.clone()).await {
                Ok(()) => {
                    if obligation.kind == CleanupKind::FinalDelete {
                        finalized.push(obligation.sandbox_id);
                    }
                }
                Err(error) => {
                    self.cleanup_failures.fetch_add(1, Ordering::Relaxed);
                    warn!(error = ?error, "durable sandbox cleanup replay failed");
                }
            }
        }
        // Successful final deletes must release image pins even if another
        // obligation failed; its journal entry remains available for retry.
        Ok(finalized)
    }

    fn image_gc_safe(&self) -> bool {
        self.image_gc_safe.load(Ordering::Acquire)
    }

    fn cleanup_metrics(&self) -> CleanupMetrics {
        CleanupMetrics {
            pending: self.pending_cleanup.load(Ordering::Relaxed),
            retries: self.cleanup_retries.load(Ordering::Relaxed),
            failures: self.cleanup_failures.load(Ordering::Relaxed),
            pruned_generations: self.pruned_generations.load(Ordering::Relaxed),
            reclaimed_snapshot_bytes: self.reclaimed_snapshot_bytes.load(Ordering::Relaxed),
            reserved_journal_bytes: self.cleanup_journal_reserve_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::{
        mock::{MockBackendFactory, MockSnapshot},
        FreshSandboxBuildSpec, PausedSandboxState, RuntimeArtifactSet, SandboxBackend,
        SandboxLaunchConfig,
    };
    use crate::snapshot::RunnableSnapshot;
    use anyhow::Result;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    #[derive(Debug)]
    struct FailingEncodeState;

    impl PausedSandboxState for FailingEncodeState {
        fn encode(&self) -> Result<Value> {
            anyhow::bail!("forced encode failure")
        }

        fn runtime_artifacts(&self) -> RuntimeArtifactSet {
            RuntimeArtifactSet::empty()
        }
    }

    #[derive(Default)]
    struct RejectingFactory;

    impl SandboxBackendFactory for RejectingFactory {
        fn build(
            &self,
            _build_spec: FreshSandboxBuildSpec,
            _launch_config: SandboxLaunchConfig,
        ) -> Result<Box<dyn SandboxBackend>> {
            unreachable!("persister tests only decode state")
        }

        fn build_from_snapshot(
            &self,
            _snapshot: &RunnableSnapshot,
            _launch_config: SandboxLaunchConfig,
        ) -> Result<Box<dyn SandboxBackend>> {
            unreachable!("persister tests only decode state")
        }

        fn build_from_paused_state(
            &self,
            _sandbox_id: SandboxId,
            _state: &dyn PausedSandboxState,
            _envd_access_token: Option<crate::sandbox::EnvdAccessToken>,
        ) -> Result<Box<dyn SandboxBackend>> {
            unreachable!("persister tests only decode state")
        }

        fn decode_paused_state(
            &self,
            _artifact_root: PathBuf,
            _state: Value,
        ) -> Result<Arc<dyn PausedSandboxState>> {
            anyhow::bail!("forced decode failure")
        }
    }

    fn paused_state(root: &Path) -> Arc<dyn PausedSandboxState> {
        std::fs::create_dir_all(root).expect("create test artifact root");
        Arc::new(MockSnapshot)
    }

    #[derive(Debug)]
    struct ArtifactSnapshot {
        configs: Vec<PathBuf>,
    }

    impl PausedSandboxState for ArtifactSnapshot {
        fn encode(&self) -> Result<Value> {
            Ok(serde_json::json!({}))
        }

        fn runtime_artifacts(&self) -> RuntimeArtifactSet {
            RuntimeArtifactSet::from_overlaybd_image_configs(self.configs.clone())
        }
    }

    fn write_overlaybd_config(path: &Path, lower: &Path) -> Result<()> {
        std::fs::create_dir_all(path.parent().expect("config parent"))?;
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "repoBlobUrl": "",
                "lowers": [{
                    "file": lower,
                    "digest": "sha256:test-layer",
                    "size": std::fs::metadata(lower)?.len()
                }],
                "upper": {},
                "resultFile": ""
            }))?,
        )?;
        Ok(())
    }

    fn artifact_snapshot(root: &Path, lower: &Path) -> Result<Arc<dyn PausedSandboxState>> {
        let configs = [
            root.join("rootfs/image.json"),
            root.join("drives/data/image.json"),
            root.join("mem_image.json"),
        ];
        for config in &configs {
            write_overlaybd_config(config, lower)?;
        }
        Ok(Arc::new(ArtifactSnapshot {
            configs: configs.into_iter().collect(),
        }))
    }

    fn test_persister(root: &Path) -> FileBackedSandboxPersister {
        FileBackedSandboxPersister::new_for_test(root.to_path_buf())
            .with_durability(LocalStoreDurability::Memory)
            .with_cleanup_journal_reserve_bytes(1024 * 1024)
    }

    async fn persist_test_record(
        persister: &FileBackedSandboxPersister,
    ) -> anyhow::Result<(SandboxId, Arc<dyn PausedSandboxState>, PathBuf)> {
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            virtualization_mode: persister.virtualization_mode,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;
        Ok((sandbox_id, paused_state, snapshot_root))
    }

    async fn has_record(
        persister: &FileBackedSandboxPersister,
        sandbox_id: &SandboxId,
    ) -> anyhow::Result<bool> {
        Ok(persister
            .db()
            .await?
            .get(sandbox_id.to_string())
            .await?
            .is_some())
    }

    async fn has_resume_marker(
        persister: &FileBackedSandboxPersister,
        sandbox_id: &SandboxId,
    ) -> anyhow::Result<bool> {
        Ok(persister.resume_marker_path(sandbox_id).try_exists()?)
    }

    #[tokio::test]
    async fn file_persister_round_trips_paused_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join(Uuid::now_v7().to_string());
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            timeout: Some(Duration::from_secs(5)),
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };

        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, metadata.id);
        assert!(loaded[0]
            .paused_state
            .as_ref()
            .expect("paused state should be restored")
            .downcast_ref::<MockSnapshot>()
            .is_some());
        Ok(())
    }

    #[tokio::test]
    async fn paused_record_from_other_mode_is_visible_but_not_resumable() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let kvm_persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, snapshot_root) =
            persist_test_record(&kvm_persister).await?;
        drop(kvm_persister);
        let pvm_persister =
            FileBackedSandboxPersister::new(temp.path().to_path_buf(), VirtualizationMode::Pvm)
                .with_durability(LocalStoreDurability::Memory);

        let loaded = pvm_persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        assert_eq!(loaded[0].state, SandboxState::Paused);
        assert_eq!(loaded[0].virtualization_mode, VirtualizationMode::Kvm);
        assert!(loaded[0].paused_state.is_none());
        assert!(has_record(&pvm_persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn mixed_mode_records_are_both_visible_and_retained() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let kvm_persister = test_persister(temp.path());
        let (kvm_id, _kvm_state, kvm_root) = persist_test_record(&kvm_persister).await?;
        drop(kvm_persister);

        let pvm_persister =
            FileBackedSandboxPersister::new(temp.path().to_path_buf(), VirtualizationMode::Pvm)
                .with_durability(LocalStoreDurability::Memory);
        let (pvm_id, _pvm_state, pvm_root) = persist_test_record(&pvm_persister).await?;

        let mut loaded = pvm_persister.load_all(&MockBackendFactory::new()).await?;
        loaded.sort_by_key(|metadata| metadata.id);

        let kvm_metadata = loaded
            .iter()
            .find(|metadata| metadata.id == kvm_id)
            .expect("KVM metadata should remain visible");
        assert_eq!(kvm_metadata.virtualization_mode, VirtualizationMode::Kvm);
        assert!(kvm_metadata.paused_state.is_none());

        let pvm_metadata = loaded
            .iter()
            .find(|metadata| metadata.id == pvm_id)
            .expect("PVM metadata should load");
        assert_eq!(pvm_metadata.virtualization_mode, VirtualizationMode::Pvm);
        assert!(pvm_metadata.paused_state.is_some());

        assert!(has_record(&pvm_persister, &kvm_id).await?);
        assert!(has_record(&pvm_persister, &pvm_id).await?);
        assert!(kvm_root.exists());
        assert!(pvm_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn allocate_artifact_root_creates_unique_snapshot_roots() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();

        let first_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("file-backed persister should allocate artifact root");
        let second_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("file-backed persister should allocate artifact root");
        let sandbox_id_dir = sandbox_id.to_string();

        assert_ne!(first_root, second_root);
        assert!(first_root.is_dir());
        assert!(second_root.is_dir());
        assert_eq!(
            first_root.parent().and_then(Path::file_name),
            Some(std::ffi::OsStr::new(&sandbox_id_dir))
        );
        assert_eq!(
            first_root
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name),
            Some(std::ffi::OsStr::new("artifacts"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn repause_prunes_superseded_generation_after_new_record_is_durable() -> anyhow::Result<()>
    {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let old_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let old_state = paused_state(&old_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&old_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&old_root), old_state.as_ref())
            .await?;
        persister.complete_resume(&sandbox_id).await?;

        let new_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let new_state = paused_state(&new_root);
        persister
            .persist_paused(&metadata, Some(&new_root), new_state.as_ref())
            .await?;
        persister
            .prune_artifact_generations(&sandbox_id, Some(&new_root))
            .await?;

        assert!(!old_root.exists());
        assert!(new_root.exists());
        assert_eq!(
            persister.get_record(&sandbox_id).await?.artifact_root,
            new_root
        );
        Ok(())
    }

    #[tokio::test]
    async fn generation_reference_index_replaces_record_refs_atomically() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let metadata = SandboxMetadata {
            id: sandbox_id,
            ..Default::default()
        };
        let old_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("old artifact root");
        let old_state = paused_state(&old_root);
        persister
            .persist_paused(&metadata, Some(&old_root), old_state.as_ref())
            .await?;
        let old_generation = FileBackedSandboxPersister::generation_id(&old_root)?;

        let new_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("new artifact root");
        let new_state = paused_state(&new_root);
        persister
            .persist_paused(&metadata, Some(&new_root), new_state.as_ref())
            .await?;
        let new_generation = FileBackedSandboxPersister::generation_id(&new_root)?;

        assert_eq!(
            persister.indexed_references_for_owner(sandbox_id).await?,
            BTreeSet::from([GenerationReference {
                sandbox_id,
                generation: new_generation,
            }])
        );
        assert!(persister
            .db()
            .await?
            .scan_prefix(FileBackedSandboxPersister::generation_reference_prefix(
                sandbox_id
            ))
            .await?
            .iter()
            .all(|(key, _)| !String::from_utf8_lossy(key).contains(&old_generation.to_string())));
        Ok(())
    }

    #[tokio::test]
    async fn generation_reference_index_migrates_existing_records_once() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let artifact_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let state = paused_state(&artifact_root);
        let record = PersistedPausedRecord {
            version: RECORD_VERSION,
            lifecycle: PersistedPausedLifecycle::Paused,
            metadata: SandboxMetadata {
                id: sandbox_id,
                paused_state: Some(Arc::clone(&state)),
                ..Default::default()
            },
            artifact_root: artifact_root.clone(),
            artifact_closure: Some(RuntimeArtifactClosure::default()),
            state: state.encode()?,
        };
        persister
            .db()
            .await?
            .put(
                FileBackedSandboxPersister::record_key(&sandbox_id),
                serde_json::to_vec(&record)?,
            )
            .await?;
        assert!(persister
            .db()
            .await?
            .get(GENERATION_REFERENCE_INDEX_VERSION_KEY)
            .await?
            .is_none());
        drop(persister);

        let restarted = test_persister(temp.path());
        restarted.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(
            restarted
                .db()
                .await?
                .get(GENERATION_REFERENCE_INDEX_VERSION_KEY)
                .await?
                .as_deref(),
            Some(GENERATION_REFERENCE_INDEX_VERSION)
        );
        assert_eq!(
            restarted
                .indexed_references_for_owner(sandbox_id)
                .await?
                .len(),
            1
        );
        assert!(artifact_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn generation_lookup_does_not_decode_unrelated_records() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (target_id, _target_state, target_root) = persist_test_record(&persister).await?;
        let (unrelated_id, _unrelated_state, _unrelated_root) =
            persist_test_record(&persister).await?;
        persister
            .db()
            .await?
            .put(
                FileBackedSandboxPersister::record_key(&unrelated_id),
                b"invalid unrelated record",
            )
            .await?;

        let protected = persister.protected_generations(target_id, None).await?;

        assert_eq!(
            protected,
            HashSet::from([FileBackedSandboxPersister::generation_id(&target_root)?])
        );
        Ok(())
    }

    #[tokio::test]
    async fn repeated_pause_resume_with_gc_preserves_full_overlaybd_closure() -> anyhow::Result<()>
    {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let metadata = SandboxMetadata {
            id: sandbox_id,
            ..Default::default()
        };
        let mut previous_root: Option<PathBuf> = None;

        for generation_index in 0..20 {
            let root = persister
                .allocate_artifact_root(&sandbox_id)
                .await?
                .expect("artifact root");
            let own_lower = root.join("layers/snapshot.commit");
            std::fs::create_dir_all(own_lower.parent().expect("layer parent"))?;
            std::fs::write(&own_lower, format!("generation-{generation_index}"))?;
            let state = artifact_snapshot(&root, &own_lower)?;

            persister
                .persist_paused(&metadata, Some(&root), state.as_ref())
                .await?;
            persister
                .prune_artifact_generations(&sandbox_id, Some(&root))
                .await?;
            persister.replay_cleanup_obligations().await?;

            if let Some(previous_root) = previous_root.as_ref() {
                assert!(!previous_root.exists());
            }
            let generations = std::fs::read_dir(persister.sandbox_artifact_root(&sandbox_id))?
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                .count();
            assert_eq!(generations, 1, "only the current closure should remain");

            persister.mark_resuming(&sandbox_id).await?;
            persister.complete_resume(&sandbox_id).await?;
            persister.replay_cleanup_obligations().await?;
            assert!(root.exists());

            previous_root = Some(root);
        }

        persister.delete_record_and_artifacts(&sandbox_id).await?;
        assert!(!persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_closure_blocks_pruning_without_deleting_any_generation(
    ) -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let stale_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let stale_lower = stale_root.join("layers/stale.commit");
        std::fs::create_dir_all(stale_lower.parent().expect("layer parent"))?;
        std::fs::write(&stale_lower, b"stale")?;
        let current_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let current_lower = current_root.join("layers/current.commit");
        std::fs::create_dir_all(current_lower.parent().expect("layer parent"))?;
        std::fs::write(&current_lower, b"current")?;
        let state = artifact_snapshot(&current_root, &current_lower)?;
        persister
            .persist_paused(
                &SandboxMetadata {
                    id: sandbox_id,
                    ..Default::default()
                },
                Some(&current_root),
                state.as_ref(),
            )
            .await?;
        std::fs::remove_file(&current_lower)?;

        let error = persister
            .prune_artifact_generations(&sandbox_id, Some(&current_root))
            .await
            .expect_err("an incomplete closure must fail closed");

        assert!(matches!(
            error,
            SandboxPersistenceError::InvalidRecord { .. }
        ));
        assert!(stale_root.exists());
        assert!(current_root.exists());
        assert_eq!(persister.cleanup_metrics().pending, 1);
        Ok(())
    }

    #[tokio::test]
    async fn pruning_skips_a_concurrently_allocated_generation() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _state, current_root) = persist_test_record(&persister).await?;
        let in_progress = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("in-progress artifact root");

        persister
            .prune_artifact_generations(&sandbox_id, Some(&current_root))
            .await?;

        assert!(current_root.exists());
        assert!(in_progress.exists());
        persister
            .discard_artifact_generation(&sandbox_id, Some(&in_progress))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn failed_discard_unregisters_generation_so_final_delete_can_retry() -> anyhow::Result<()>
    {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let artifact_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        persister
            .fail_remove_artifacts
            .store(1, std::sync::atomic::Ordering::SeqCst);

        persister
            .discard_artifact_generation(&sandbox_id, Some(&artifact_root))
            .await
            .expect_err("forced discard failure");
        assert!(artifact_root.exists());

        persister.delete_record_and_artifacts(&sandbox_id).await?;
        assert!(!artifact_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn persist_rejects_cross_sandbox_layer_reference() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let source_id = SandboxId::new();
        let source_root = persister
            .allocate_artifact_root(&source_id)
            .await?
            .expect("source artifact root");
        let source_lower = source_root.join("layers/source.commit");
        std::fs::create_dir_all(source_lower.parent().expect("layer parent"))?;
        std::fs::write(&source_lower, b"source")?;
        let source_state = artifact_snapshot(&source_root, &source_lower)?;
        persister
            .persist_paused(
                &SandboxMetadata {
                    id: source_id,
                    ..Default::default()
                },
                Some(&source_root),
                source_state.as_ref(),
            )
            .await?;

        let dependent_id = SandboxId::new();
        let dependent_root = persister
            .allocate_artifact_root(&dependent_id)
            .await?
            .expect("dependent artifact root");
        let dependent_state = artifact_snapshot(&dependent_root, &source_lower)?;
        let error = persister
            .persist_paused(
                &SandboxMetadata {
                    id: dependent_id,
                    ..Default::default()
                },
                Some(&dependent_root),
                dependent_state.as_ref(),
            )
            .await
            .expect_err("a paused closure must be self-contained");
        assert!(matches!(
            error,
            SandboxPersistenceError::InvalidRecord { .. }
        ));
        assert!(source_lower.exists());
        assert!(
            dependent_root.exists(),
            "the caller owns failed generations"
        );
        persister
            .discard_artifact_generation(&dependent_id, Some(&dependent_root))
            .await?;
        assert!(!dependent_root.exists());

        persister.delete_record_and_artifacts(&source_id).await?;
        assert!(!persister.sandbox_artifact_root(&source_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_prunes_sibling_generations_for_valid_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let stale_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let current_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let current_state = paused_state(&current_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&current_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&current_root), current_state.as_ref())
            .await?;
        drop(persister);
        let restarted = test_persister(temp.path());

        let loaded = restarted.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert!(!stale_root.exists());
        assert!(current_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn generation_pruning_rejects_artifact_root_outside_sandbox() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let sibling = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let outside = temp.path().join("outside");
        tokio::fs::create_dir_all(&outside).await?;

        let err = persister
            .prune_artifact_generations(&sandbox_id, Some(&outside))
            .await
            .expect_err("outside artifact root must be rejected");

        assert!(matches!(err, SandboxPersistenceError::InvalidRecord { .. }));
        assert!(outside.exists());
        assert!(sibling.exists());
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_resume_restores_last_paused_generation_on_load() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, snapshot_root) = persist_test_record(&persister).await?;
        persister.mark_resuming(&sandbox_id).await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        assert!(has_record(&persister, &sandbox_id).await?);
        assert!(!has_resume_marker(&persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn persist_paused_accepts_backend_agnostic_state() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            ..Default::default()
        };

        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;
        drop(paused_state);

        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn persist_paused_failure_leaves_generation_for_caller_to_discard() -> anyhow::Result<()>
    {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let paused_state: Arc<dyn PausedSandboxState> = Arc::new(FailingEncodeState);
        let err = persister
            .persist_paused(
                &SandboxMetadata {
                    id: sandbox_id,
                    ..Default::default()
                },
                Some(&snapshot_root),
                paused_state.as_ref(),
            )
            .await
            .expect_err("encode failure should reject paused state");

        assert!(matches!(err, SandboxPersistenceError::InvalidRecord { .. }));
        assert!(
            snapshot_root.exists(),
            "a resumed runtime may still read the failed generation"
        );
        persister
            .discard_artifact_generation(&sandbox_id, Some(&snapshot_root))
            .await?;
        assert!(!snapshot_root.exists());
        assert!(persister.active_generations.lock().await.is_empty());
        Ok(())
    }

    async fn write_legacy_record(
        persister: &FileBackedSandboxPersister,
        sandbox_id: SandboxId,
    ) -> anyhow::Result<PathBuf> {
        let artifact_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join(Uuid::now_v7().to_string());
        std::fs::create_dir_all(&artifact_root)?;
        let record = PersistedPausedRecord {
            version: RECORD_VERSION,
            lifecycle: PersistedPausedLifecycle::Paused,
            metadata: SandboxMetadata {
                id: sandbox_id,
                ..Default::default()
            },
            artifact_root: artifact_root.clone(),
            artifact_closure: None,
            state: Value::Null,
        };
        persister
            .db()
            .await?
            .put(sandbox_id.to_string(), serde_json::to_vec(&record)?)
            .await?;
        Ok(artifact_root)
    }

    #[tokio::test]
    async fn legacy_and_corrupt_records_do_not_block_other_sandbox_cleanup() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let legacy_root = write_legacy_record(&persister, SandboxId::new()).await?;
        persister
            .db()
            .await?
            .put(SandboxId::new().to_string(), b"not json".to_vec())
            .await?;

        let (sandbox_id, _paused_state, durable_root) = persist_test_record(&persister).await?;
        assert!(
            !persister.image_gc_safe(),
            "legacy records force the record-scan fallback"
        );
        let stale_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join(Uuid::now_v7().to_string());
        std::fs::create_dir_all(&stale_root)?;

        persister
            .prune_artifact_generations(&sandbox_id, Some(&durable_root))
            .await?;
        assert!(!stale_root.exists());
        assert!(durable_root.exists());

        persister.delete_record_and_artifacts(&sandbox_id).await?;
        assert!(!has_record(&persister, &sandbox_id).await?);
        assert!(!durable_root.exists());
        assert!(legacy_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn unresolvable_own_record_protects_all_of_its_generations() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let legacy_root = write_legacy_record(&persister, sandbox_id).await?;
        let sibling_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join(Uuid::now_v7().to_string());
        std::fs::create_dir_all(&sibling_root)?;

        persister
            .prune_artifact_generations(&sandbox_id, Some(&legacy_root))
            .await?;

        assert!(legacy_root.exists());
        assert!(
            sibling_root.exists(),
            "a legacy runtime may still reference sibling generations"
        );
        Ok(())
    }

    #[tokio::test]
    async fn retained_runtime_generation_survives_prune_and_does_not_block_delete(
    ) -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, durable_root) = persist_test_record(&persister).await?;

        // A later pause fails durably after the runtime resumed on its generation.
        let retained_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let failing: Arc<dyn PausedSandboxState> = Arc::new(FailingEncodeState);
        persister
            .persist_paused(
                &SandboxMetadata {
                    id: sandbox_id,
                    ..Default::default()
                },
                Some(&retained_root),
                failing.as_ref(),
            )
            .await
            .expect_err("encode failure should reject paused state");
        persister
            .retain_runtime_generation(&sandbox_id, Some(&retained_root))
            .await?;

        // A pending prune for the durable record must not delete it.
        persister
            .prune_artifact_generations(&sandbox_id, Some(&durable_root))
            .await?;
        assert!(retained_root.exists());
        assert!(durable_root.exists());

        persister.delete_record_and_artifacts(&sandbox_id).await?;
        assert!(!retained_root.exists());
        assert!(!has_record(&persister, &sandbox_id).await?);
        assert!(persister
            .runtime_retained_generations
            .lock()
            .await
            .is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn durable_pause_releases_retained_runtime_generation_for_pruning() -> anyhow::Result<()>
    {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let retained_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        persister
            .retain_runtime_generation(&sandbox_id, Some(&retained_root))
            .await?;

        let next_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("artifact root");
        let next_state = paused_state(&next_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            virtualization_mode: persister.virtualization_mode,
            paused_state: Some(Arc::clone(&next_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&next_root), next_state.as_ref())
            .await?;
        persister
            .prune_artifact_generations(&sandbox_id, Some(&next_root))
            .await?;

        assert!(!retained_root.exists());
        assert!(next_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn mark_resuming_and_rollback_preserve_loadability() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, snapshot_root) = persist_test_record(&persister).await?;

        persister.mark_resuming(&sandbox_id).await?;
        assert_eq!(
            persister.get_record(&sandbox_id).await?.lifecycle,
            PersistedPausedLifecycle::Paused
        );
        assert!(has_resume_marker(&persister, &sandbox_id).await?);

        persister.rollback_resuming(&sandbox_id).await?;
        assert!(!has_resume_marker(&persister, &sandbox_id).await?);
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn blocking_resume_marker_is_bounded_and_retryable() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, snapshot_root) = persist_test_record(&persister).await?;
        persister.stall_next_resume_marker();

        let started = tokio::time::Instant::now();
        let error = persister
            .mark_resuming(&sandbox_id)
            .await
            .expect_err("blocking resume marker must time out");

        assert!(started.elapsed() < RESUME_MARKER_TIMEOUT * 2);
        assert!(matches!(
            error,
            SandboxPersistenceError::RuntimeState { .. }
        ));
        assert_eq!(
            persister.get_record(&sandbox_id).await?.lifecycle,
            PersistedPausedLifecycle::Paused
        );
        tokio::time::sleep(RESUME_MARKER_TIMEOUT * 5).await;
        assert!(!has_resume_marker(&persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());

        persister.mark_resuming(&sandbox_id).await?;
        assert!(has_resume_marker(&persister, &sandbox_id).await?);
        persister.rollback_resuming(&sandbox_id).await?;
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        Ok(())
    }

    #[tokio::test]
    async fn complete_resume_keeps_recovery_record_and_artifacts() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, snapshot_root) = persist_test_record(&persister).await?;

        persister.complete_resume(&sandbox_id).await?;

        assert!(has_record(&persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_preserves_resumed_artifacts_without_records() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let artifact_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("resumed-generation");
        tokio::fs::create_dir_all(&artifact_root).await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty());
        assert!(artifact_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_keeps_artifacts_for_valid_paused_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join(Uuid::now_v7().to_string());
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert!(persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_replay_returns_successes_when_another_delete_is_blocked() -> anyhow::Result<()>
    {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (finished, _, finished_root) = persist_test_record(&persister).await?;
        let (blocked, _, blocked_root) = persist_test_record(&persister).await?;
        let active = persister.allocate_artifact_root(&blocked).await?;
        persister.record_final_cleanup(finished).await?;
        persister.record_final_cleanup(blocked).await?;

        let finalized = persister.replay_cleanup_obligations().await?;

        assert_eq!(finalized, vec![finished]);
        assert!(!finished_root.exists());
        assert!(blocked_root.exists());
        assert_eq!(persister.cleanup_metrics().pending, 1);
        persister
            .discard_artifact_generation(&blocked, active.as_deref())
            .await?;
        assert_eq!(persister.replay_cleanup_obligations().await?, vec![blocked]);
        assert_eq!(persister.cleanup_metrics().pending, 0);
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_and_artifacts_removes_both() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, _snapshot_root) = persist_test_record(&persister).await?;

        persister.delete_record_and_artifacts(&sandbox_id).await?;

        assert!(!has_record(&persister, &sandbox_id).await?);
        assert!(!persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn final_delete_tombstone_prevents_record_resurrection_after_enospc() -> anyhow::Result<()>
    {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, snapshot_root) = persist_test_record(&persister).await?;
        persister
            .fail_remove_record
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let err = persister
            .delete_record_and_artifacts(&sandbox_id)
            .await
            .expect_err("forced record ENOSPC must be reported");

        assert!(matches!(err, SandboxPersistenceError::Store { .. }));
        assert!(!snapshot_root.exists());
        assert!(has_record(&persister, &sandbox_id).await?);

        persister
            .fail_remove_record
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;
        assert!(loaded.is_empty());
        assert!(has_record(&persister, &sandbox_id).await?);

        persister.replay_cleanup_obligations().await?;
        assert!(!has_record(&persister, &sandbox_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn artifact_cleanup_failure_is_reported_and_retryable() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, snapshot_root) = persist_test_record(&persister).await?;
        persister
            .fail_remove_artifacts
            .store(1, std::sync::atomic::Ordering::SeqCst);

        let err = persister
            .delete_record_and_artifacts(&sandbox_id)
            .await
            .expect_err("forced artifact cleanup failure must be reported");

        assert!(matches!(err, SandboxPersistenceError::Io { .. }));
        assert!(snapshot_root.exists());
        assert!(!has_record(&persister, &sandbox_id).await?);

        drop(persister);
        let restarted = test_persister(temp.path());
        restarted.replay_cleanup_obligations().await?;
        assert!(!snapshot_root.exists());
        assert_eq!(restarted.cleanup_metrics().pending, 0);
        Ok(())
    }

    #[tokio::test]
    async fn preallocated_cleanup_journal_bootstraps_delete_from_enospc() -> anyhow::Result<()> {
        let Ok(loopback_root) = std::env::var("AENV_DISK_SAFETY_TEST_ROOT") else {
            return Ok(());
        };
        let root = PathBuf::from(loopback_root).join(Uuid::now_v7().to_string());
        std::fs::create_dir_all(&root)?;
        let persister = test_persister(&root);
        let (sandbox_id, _paused_state, snapshot_root) = persist_test_record(&persister).await?;
        std::fs::write(snapshot_root.join("payload"), vec![0u8; 8 * 1024 * 1024])?;
        persister.replay_cleanup_obligations().await?;

        let filler_path = root.parent().expect("test root has parent").join("filler");
        let mut filler = std::fs::File::create(&filler_path)?;
        let chunk = vec![0u8; 1024 * 1024];
        let fill_error = loop {
            if let Err(error) = filler.write_all(&chunk) {
                break error;
            }
        };
        assert_eq!(fill_error.raw_os_error(), Some(libc::ENOSPC));

        persister.delete_record_and_artifacts(&sandbox_id).await?;
        assert!(!snapshot_root.exists());
        assert!(!has_record(&persister, &sandbox_id).await?);
        drop(filler);
        std::fs::remove_file(filler_path)?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_and_artifacts_removes_artifacts_without_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let sandbox_artifact_root = persister.sandbox_artifact_root(&sandbox_id);
        tokio::fs::create_dir_all(sandbox_artifact_root.join("stale-generation")).await?;

        persister.delete_record_and_artifacts(&sandbox_id).await?;

        assert!(!sandbox_artifact_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_and_artifacts_removes_invalid_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        persister
            .db()
            .await?
            .put(sandbox_id.to_string(), b"not-json")
            .await?;

        persister.delete_record_and_artifacts(&sandbox_id).await?;

        assert!(!has_record(&persister, &sandbox_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn load_all_preserves_invalid_record_without_deleting_artifacts() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        persister
            .db()
            .await?
            .put(sandbox_id.to_string(), b"not-json")
            .await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty());
        assert!(has_record(&persister, &sandbox_id).await?);
        assert!(!persister.image_gc_safe());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_preserves_unusable_record_and_artifacts() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let (sandbox_id, _paused_state, snapshot_root) = persist_test_record(&persister).await?;

        let loaded = persister.load_all(&RejectingFactory).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        assert!(loaded[0].paused_state.is_none());
        assert!(has_record(&persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());
        assert!(!persister.image_gc_safe());
        Ok(())
    }
}
