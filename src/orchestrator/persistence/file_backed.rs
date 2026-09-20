use std::collections::HashSet;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;
use tokio::sync::OnceCell;
use tracing::{debug, info, warn};

use super::{PersistenceResult, SandboxPersistenceError, SandboxPersister};
use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::orchestrator::{store::SandboxMetadata, SandboxState};
use crate::sandbox::{PausedSandboxState, SandboxBackendFactory};
use crate::types::SandboxId;
use crate::virtualization::VirtualizationMode;

const RECORD_VERSION: u32 = 1;
const RECORD_DB_DIR: &str = "records.db";

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
    state: Value,
}

impl PersistedPausedRecord {
    fn into_metadata<F>(mut self, factory: &F) -> PersistenceResult<SandboxMetadata>
    where
        F: SandboxBackendFactory,
    {
        let paused_state = factory.decode_paused_state(self.state).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "failed to decode paused sandbox state".to_string(),
                source: Some(source),
            }
        })?;
        self.metadata.state = SandboxState::Paused;
        self.metadata.paused_state = Some(paused_state);

        Ok(self.metadata)
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
}

impl FileBackedSandboxPersister {
    pub fn new(root: PathBuf, virtualization_mode: VirtualizationMode) -> Self {
        Self {
            root,
            virtualization_mode,
            durability: LocalStoreDurability::Sync,
            db: OnceCell::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(root: PathBuf) -> Self {
        Self::new(root, VirtualizationMode::Kvm)
    }

    pub fn with_durability(mut self, durability: LocalStoreDurability) -> Self {
        self.durability = durability;
        self
    }

    fn records_db_path(&self) -> PathBuf {
        self.root.join(RECORD_DB_DIR)
    }

    fn sandbox_root(&self, sandbox_id: &SandboxId) -> PathBuf {
        self.root.join(sandbox_id.to_string())
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

    async fn put_record(&self, record: &PersistedPausedRecord) -> PersistenceResult<()> {
        let bytes = serde_json::to_vec(record).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "failed to serialize record".to_string(),
                source: Some(source.into()),
            }
        })?;

        self.db()
            .await?
            .put(record.metadata.id.to_string(), bytes)
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("persist paused sandbox record", source)
            })
    }

    async fn remove_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.db()
            .await?
            .delete(sandbox_id.to_string())
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("remove paused sandbox record", source)
            })
    }

    async fn remove_artifact_root(path: &Path) -> PersistenceResult<()> {
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

    async fn cleanup_orphan_artifacts(
        &self,
        retained_sandbox_ids: &HashSet<SandboxId>,
    ) -> PersistenceResult<()> {
        let mut entries = match fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(SandboxPersistenceError::io(
                    "read paused sandbox artifacts",
                    &self.root,
                    source,
                ));
            }
        };

        while let Some(entry) = entries.next_entry().await.map_err(|source| {
            SandboxPersistenceError::io("scan paused sandbox artifacts", &self.root, source)
        })? {
            let file_type = entry.file_type().await.map_err(|source| {
                SandboxPersistenceError::io(
                    "inspect paused sandbox artifacts",
                    entry.path(),
                    source,
                )
            })?;
            if !file_type.is_dir() {
                continue;
            }

            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name
                .strip_prefix(".retired-")
                .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
            {
                Self::remove_artifact_root(&entry.path()).await?;
                continue;
            }
            let Some(sandbox_id) = SandboxId::parse_str(&name).ok() else {
                continue;
            };

            if !retained_sandbox_ids.contains(&sandbox_id) {
                info!(
                    sandbox_id = %sandbox_id,
                    artifacts = %entry.path().display(),
                    "removing orphaned paused sandbox artifacts"
                );
                Self::remove_artifact_root(&entry.path()).await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl SandboxPersister for FileBackedSandboxPersister {
    async fn load_all<F>(&self, factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        info!(store = %self.root.display(), "loading paused sandbox records");
        let records = self.db().await?.entries().await.map_err(|source| {
            SandboxPersistenceError::store("scan paused sandbox records", source)
        })?;
        let mut sandboxes = Vec::new();
        let mut retained_artifacts = HashSet::new();

        for (key, bytes) in records {
            let restored = decode_record(&bytes).and_then(|record| {
                if record.metadata.id.to_string().as_bytes() != key.as_slice() {
                    return Err(SandboxPersistenceError::InvalidRecord {
                        reason: "sandbox ID does not match its record key".to_string(),
                        source: None,
                    });
                }
                if record.lifecycle == PersistedPausedLifecycle::Resuming {
                    return Err(SandboxPersistenceError::RuntimeState {
                        reason: "continuation was left in resuming state",
                    });
                }
                if record.metadata.virtualization_mode != self.virtualization_mode {
                    warn!(
                        sandbox_id = %record.metadata.id,
                        record_mode = %record.metadata.virtualization_mode,
                        node_mode = %self.virtualization_mode,
                        "loading paused sandbox metadata without resumable runtime state because its virtualization mode is incompatible"
                    );
                    Ok(record.into_metadata_without_runtime_state())
                } else {
                    record.into_metadata(factory)
                }
            });
            match restored {
                Ok(metadata) => {
                    retained_artifacts.insert(metadata.id);
                    sandboxes.push(metadata);
                }
                Err(error) => {
                    warn!(record_key = %String::from_utf8_lossy(&key), %error, "discarding invalid paused sandbox record");
                    self.db().await?.delete(key).await.map_err(|source| {
                        SandboxPersistenceError::store(
                            "remove invalid paused sandbox record",
                            source,
                        )
                    })?;
                }
            }
        }

        self.cleanup_orphan_artifacts(&retained_artifacts).await?;

        info!(
            loaded = sandboxes.len(),
            retained = retained_artifacts.len(),
            "loaded paused sandbox records"
        );

        Ok(sandboxes)
    }

    async fn prepare_runtime_root(&self, sandbox_id: &SandboxId) -> PersistenceResult<PathBuf> {
        let root = self.sandbox_root(sandbox_id);
        fs::create_dir_all(&root).await.map_err(|source| {
            SandboxPersistenceError::io("prepare runtime storage", &root, source)
        })?;
        fs::canonicalize(&root)
            .await
            .map_err(|source| SandboxPersistenceError::io("resolve runtime storage", &root, source))
    }

    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        debug!(
            sandbox_id = %metadata.id,
            "persisting paused sandbox"
        );
        let state =
            paused_state
                .encode()
                .map_err(|source| SandboxPersistenceError::InvalidRecord {
                    reason: "failed to encode paused sandbox state".to_string(),
                    source: Some(source),
                })?;
        let record = PersistedPausedRecord {
            version: RECORD_VERSION,
            lifecycle: PersistedPausedLifecycle::Paused,
            metadata: metadata.clone(),
            state,
        };
        self.put_record(&record).await
    }

    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "marking paused sandbox as resuming");
        let mut record = self.get_record(sandbox_id).await?;
        record.lifecycle = PersistedPausedLifecycle::Resuming;
        self.put_record(&record).await
    }

    async fn rollback_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "rolling back paused sandbox to paused");
        let mut record = self.get_record(sandbox_id).await?;
        record.lifecycle = PersistedPausedLifecycle::Paused;
        self.put_record(&record).await
    }

    async fn delete_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "deleting paused sandbox record");
        self.remove_record(sandbox_id).await
    }

    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "deleting paused sandbox record and artifacts");
        self.remove_record(sandbox_id).await?;
        // Once the record is gone, cleanup errors must not restore a Paused sandbox.
        if let Err(error) = super::retire_artifacts(&self.sandbox_root(sandbox_id)).await {
            warn!(
                %sandbox_id,
                %error,
                "deleted sandbox record; deferring artifact retirement to orphan cleanup"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::{
        mock::{MockBackendFactory, MockSnapshot},
        FirecrackerSandboxFactory, RuntimeArtifactSet,
    };
    use anyhow::Result;
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

    fn test_persister(root: &Path) -> FileBackedSandboxPersister {
        FileBackedSandboxPersister::new_for_test(root.to_path_buf())
            .with_durability(LocalStoreDurability::Memory)
    }

    async fn persist_test_record(persister: &FileBackedSandboxPersister) -> Result<SandboxId> {
        let metadata = SandboxMetadata {
            virtualization_mode: persister.virtualization_mode,
            ..Default::default()
        };
        persister.prepare_runtime_root(&metadata.id).await?;
        persister.persist_paused(&metadata, &MockSnapshot).await?;
        Ok(metadata.id)
    }

    async fn has_record(persister: &FileBackedSandboxPersister, id: &SandboxId) -> Result<bool> {
        Ok(persister.db().await?.get(id.to_string()).await?.is_some())
    }

    #[tokio::test]
    async fn paused_record_from_other_mode_is_visible_but_not_resumable() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let kvm_persister = test_persister(temp.path());
        let sandbox_id = persist_test_record(&kvm_persister).await?;
        let snapshot_root = kvm_persister.sandbox_root(&sandbox_id);
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
        let kvm_id = persist_test_record(&kvm_persister).await?;
        let kvm_root = kvm_persister.sandbox_root(&kvm_id);
        drop(kvm_persister);

        let pvm_persister =
            FileBackedSandboxPersister::new(temp.path().to_path_buf(), VirtualizationMode::Pvm)
                .with_durability(LocalStoreDurability::Memory);
        let pvm_id = persist_test_record(&pvm_persister).await?;
        let pvm_root = pvm_persister.sandbox_root(&pvm_id);

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
    async fn startup_restores_paused_and_reclaims_only_unowned_state() -> Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let paused = persist_test_record(&persister).await?;
        let root = persister.prepare_runtime_root(&paused).await?;
        assert_eq!(root, temp.path().canonicalize()?.join(paused.to_string()));
        assert_eq!(root, persister.prepare_runtime_root(&paused).await?);
        tokio::fs::write(root.join("state"), b"retained").await?;
        let resuming = persist_test_record(&persister).await?;
        persister.mark_resuming(&resuming).await?;
        let corrupt = persist_test_record(&persister).await?;
        persister
            .db()
            .await?
            .put(corrupt.to_string(), b"not-json")
            .await?;
        let mismatched = SandboxId::new();
        persister.prepare_runtime_root(&mismatched).await?;
        let mut wrong_record = persister.get_record(&paused).await?;
        wrong_record.lifecycle = PersistedPausedLifecycle::Resuming;
        persister
            .db()
            .await?
            .put(mismatched.to_string(), serde_json::to_vec(&wrong_record)?)
            .await?;
        let orphan = SandboxId::new();
        persister.prepare_runtime_root(&orphan).await?;
        let retired = temp
            .path()
            .join(format!(".retired-{}", uuid::Uuid::now_v7()));
        tokio::fs::create_dir(&retired).await?;
        let unknown = temp.path().join("operator-files");
        tokio::fs::create_dir(&unknown).await?;
        let outside = TempDir::new()?;
        tokio::fs::write(outside.path().join("keep"), b"outside").await?;
        let link = temp.path().join(SandboxId::new().to_string());
        std::os::unix::fs::symlink(outside.path(), &link)?;

        for _ in 0..2 {
            let loaded = persister.load_all(&MockBackendFactory::new()).await?;
            assert_eq!(loaded.len(), 1);
            assert_eq!(loaded[0].id, paused);
            assert_eq!(loaded[0].state, SandboxState::Paused);
            assert!(loaded[0]
                .paused_state
                .as_ref()
                .unwrap()
                .downcast_ref::<MockSnapshot>()
                .is_some());
        }
        for id in [resuming, corrupt, mismatched, orphan] {
            assert!(!has_record(&persister, &id).await?);
            assert!(!persister.sandbox_root(&id).exists());
        }
        assert_eq!(tokio::fs::read(root.join("state")).await?, b"retained");
        assert!(!retired.exists());
        assert!(temp.path().join(RECORD_DB_DIR).is_dir());
        assert!(unknown.is_dir() && link.is_symlink());
        assert_eq!(
            tokio::fs::read(outside.path().join("keep")).await?,
            b"outside"
        );
        Ok(())
    }

    #[tokio::test]
    async fn persist_paused_keeps_live_artifacts_when_encode_fails() -> Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let metadata = SandboxMetadata::default();
        let root = persister.prepare_runtime_root(&metadata.id).await?;
        let err = persister
            .persist_paused(&metadata, &FailingEncodeState)
            .await
            .unwrap_err();
        assert!(matches!(err, SandboxPersistenceError::InvalidRecord { .. }));
        assert!(root.is_dir());
        assert!(!has_record(&persister, &metadata.id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn mark_resuming_and_rollback_preserve_loadability() -> Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let id = persist_test_record(&persister).await?;
        persister.mark_resuming(&id).await?;
        assert_eq!(
            persister.get_record(&id).await?.lifecycle,
            PersistedPausedLifecycle::Resuming
        );
        persister.rollback_resuming(&id).await?;
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, id);
        assert!(persister.sandbox_root(&id).is_dir());
        Ok(())
    }

    #[tokio::test]
    async fn retired_cleanup_preserves_a_new_pause_for_the_same_sandbox() -> Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let id = persist_test_record(&persister).await?;
        tokio::fs::write(persister.sandbox_root(&id).join("old"), b"old").await?;
        persister.delete_record_and_artifacts(&id).await?;
        let fresh = persister.prepare_runtime_root(&id).await?;
        tokio::fs::write(fresh.join("new"), b"new").await?;
        persister
            .persist_paused(
                &SandboxMetadata {
                    id,
                    ..Default::default()
                },
                &MockSnapshot,
            )
            .await?;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let mut entries = tokio::fs::read_dir(&persister.root).await?;
                let mut retired = false;
                while let Some(entry) = entries.next_entry().await? {
                    retired |= entry.file_name().to_string_lossy().starts_with(".retired-");
                }
                if !retired {
                    return Ok::<_, std::io::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        assert!(has_record(&persister, &id).await?);
        assert_eq!(tokio::fs::read(fresh.join("new")).await?, b"new");
        assert!(!fresh.join("old").exists());
        Ok(())
    }

    #[tokio::test]
    async fn deleting_a_record_preserves_storage_until_explicit_retirement() -> Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let id = persist_test_record(&persister).await?;
        persister.delete_record(&id).await?;
        assert!(!has_record(&persister, &id).await?);
        assert!(persister.sandbox_root(&id).is_dir());
        // Retirement also works when the record is absent or undecodable.
        for invalid_record in [false, true] {
            persister.prepare_runtime_root(&id).await?;
            if invalid_record {
                persister
                    .db()
                    .await?
                    .put(id.to_string(), b"not-json")
                    .await?;
            }
            persister.delete_record_and_artifacts(&id).await?;
            assert!(!has_record(&persister, &id).await?);
            assert!(!persister.sandbox_root(&id).exists());
        }
        Ok(())
    }

    #[tokio::test]
    async fn load_all_discards_unusable_backend_state_and_artifacts() -> Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let id = persist_test_record(&persister).await?;
        // A mock state cannot decode as a Firecracker continuation.
        assert!(persister
            .load_all(&FirecrackerSandboxFactory::new())
            .await?
            .is_empty());
        assert!(!has_record(&persister, &id).await?);
        assert!(!persister.sandbox_root(&id).exists());
        Ok(())
    }
}
