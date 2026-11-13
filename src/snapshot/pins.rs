//! Node-local pin sharing and durable recovery holds.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;

use super::manager::SnapshotMutationGuard;
use super::{SnapshotCatalog, SnapshotId};
use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::repository::{RepositoryError, RepositoryResult};

/// Retains a node-level shared pin on a Distributed snapshot for as long as the
/// local runtime handle or paused sandbox depends on its remote layers.
pub trait SnapshotPinLease: std::fmt::Debug + Send + Sync {
    /// Prevents `Drop` from releasing the durable hold when a paused sandbox
    /// is preserved across a node restart.
    fn disarm_for_restart(&self);
}

static LIVE_PROCESS_PINS: std::sync::OnceLock<std::sync::Mutex<HashSet<String>>> =
    std::sync::OnceLock::new();

fn live_process_pins() -> &'static std::sync::Mutex<HashSet<String>> {
    LIVE_PROCESS_PINS.get_or_init(|| std::sync::Mutex::new(HashSet::new()))
}

struct LivePinLock {
    pin_id: String,
    lock_path: Option<PathBuf>,
    _flock: Option<nix::fcntl::Flock<std::fs::File>>,
}

impl LivePinLock {
    fn acquire(holds_store: &HoldsStore, pin_id: &str) -> RepositoryResult<Option<Self>> {
        {
            let mut live = live_process_pins()
                .lock()
                .expect("live process pins mutex should not be poisoned");
            if !live.insert(pin_id.to_string()) {
                return Ok(None);
            }
        }
        let mut lock = Self {
            pin_id: pin_id.to_string(),
            lock_path: None,
            _flock: None,
        };
        let leases_dir = holds_store.path.with_extension("leases");
        std::fs::create_dir_all(&leases_dir).map_err(|error| {
            RepositoryError::backend("create snapshot pin lease directory", error)
        })?;
        let lock_path = leases_dir.join(format!("{pin_id}.lock"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| RepositoryError::backend("open snapshot pin lease lock", error))?;
        match nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock) {
            Ok(flock) => {
                lock.lock_path = Some(lock_path);
                lock._flock = Some(flock);
            }
            Err((_, nix::errno::Errno::EWOULDBLOCK)) => return Ok(None),
            Err((_, error)) => {
                return Err(RepositoryError::backend(
                    "acquire snapshot pin lease lock",
                    error,
                ));
            }
        }
        Ok(Some(lock))
    }
}

impl Drop for LivePinLock {
    fn drop(&mut self) {
        let _ = self._flock.take();
        live_process_pins()
            .lock()
            .expect("live process pins mutex should not be poisoned")
            .remove(&self.pin_id);
    }
}

struct SharedNodePin {
    pin_id: String,
    // A zero count means the last lease has scheduled the release.
    ref_count: usize,
    retain_for_restart: bool,
    closed_notify: Arc<tokio::sync::Notify>,
    live_lock: Option<LivePinLock>,
}

struct PendingRelease {
    snapshot_id: SnapshotId,
    pin_id: String,
    live_lock: Option<LivePinLock>,
}

struct OpenedHoldsDb {
    // Fields are dropped in order, so the file lock outlives RocksDB.
    store: LocalKvStore,
    _flock: nix::fcntl::Flock<std::fs::File>,
}

type SharedHoldsMap = HashMap<PathBuf, std::sync::Weak<OpenedHoldsDb>>;

static OPEN_HOLDS_DBS: std::sync::OnceLock<tokio::sync::Mutex<SharedHoldsMap>> =
    std::sync::OnceLock::new();

struct HoldsStore {
    path: PathBuf,
}

impl HoldsStore {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    async fn open_shared(&self) -> RepositoryResult<Arc<OpenedHoldsDb>> {
        let mut map = OPEN_HOLDS_DBS
            .get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
            .lock()
            .await;
        if let Some(existing) = map.get(&self.path).and_then(std::sync::Weak::upgrade) {
            return Ok(existing);
        }
        let lock_path = self.path.with_extension("lock");
        if let Some(parent) = lock_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                RepositoryError::backend("create snapshot pin holds directory", error)
            })?;
        }
        let flock = tokio::task::spawn_blocking(
            move || -> anyhow::Result<nix::fcntl::Flock<std::fs::File>> {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(false)
                    .open(&lock_path)?;
                nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusive)
                    .map_err(|(_, errno)| anyhow::Error::from(std::io::Error::from(errno)))
            },
        )
        .await
        .map_err(|error| RepositoryError::backend("join holds lock task", error))?
        .map_err(|error| RepositoryError::backend("acquire holds store lock", error))?;

        let store = LocalKvStore::open(self.path.clone(), LocalStoreDurability::Sync)
            .await
            .map_err(|error| RepositoryError::backend("open snapshot pin holds store", error))?;
        let opened = Arc::new(OpenedHoldsDb {
            store,
            _flock: flock,
        });
        map.insert(self.path.clone(), Arc::downgrade(&opened));
        Ok(opened)
    }

    async fn put_hold(&self, pin_id: &str, snapshot_id: &SnapshotId) -> RepositoryResult<()> {
        let key = format!("holds/{pin_id}").into_bytes();
        let value = snapshot_id.to_string().into_bytes();
        self.open_shared()
            .await?
            .store
            .put(key, value)
            .await
            .map_err(|error| RepositoryError::backend("operate snapshot pin holds store", error))
    }

    async fn delete_hold(&self, pin_id: &str) -> RepositoryResult<()> {
        let key = format!("holds/{pin_id}").into_bytes();
        self.open_shared()
            .await?
            .store
            .delete(key)
            .await
            .map_err(|error| RepositoryError::backend("operate snapshot pin holds store", error))
    }

    async fn list_holds(&self) -> RepositoryResult<Vec<(String, String)>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let entries = self
            .open_shared()
            .await?
            .store
            .scan_prefix(b"holds/".to_vec())
            .await
            .map_err(|error| RepositoryError::backend("operate snapshot pin holds store", error))?;
        Ok(entries
            .into_iter()
            .filter_map(|(key, value)| {
                let key = String::from_utf8(key).ok()?;
                let pin_id = key.strip_prefix("holds/")?.to_string();
                let snapshot_id = String::from_utf8(value).ok()?;
                Some((pin_id, snapshot_id))
            })
            .collect())
    }
}

pub(crate) type SnapshotLease = Arc<dyn SnapshotPinLease>;

struct PinLeaseGuard {
    snapshot_id: SnapshotId,
    pin_id: String,
    pins: Arc<SnapshotPins>,
}

impl std::fmt::Debug for PinLeaseGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotPin")
            .field("snapshot_id", &self.snapshot_id)
            .finish_non_exhaustive()
    }
}

impl SnapshotPinLease for PinLeaseGuard {
    fn disarm_for_restart(&self) {
        let mut active = self
            .pins
            .active_pins
            .lock()
            .expect("active pins mutex should not be poisoned");
        if let Some(entry) = active
            .get_mut(&self.snapshot_id)
            .filter(|entry| entry.pin_id == self.pin_id)
        {
            entry.retain_for_restart = true;
        }
    }
}

impl Drop for PinLeaseGuard {
    fn drop(&mut self) {
        let (notify, live_lock) = {
            let mut active = self
                .pins
                .active_pins
                .lock()
                .expect("active pins mutex should not be poisoned");
            let Some(entry) = active
                .get_mut(&self.snapshot_id)
                .filter(|entry| entry.pin_id == self.pin_id && entry.ref_count != 0)
            else {
                return;
            };
            entry.ref_count -= 1;
            if entry.ref_count != 0 {
                return;
            }
            if entry.retain_for_restart {
                active.remove(&self.snapshot_id);
                return;
            }
            (Arc::clone(&entry.closed_notify), entry.live_lock.take())
        };
        let snapshot_id = self.snapshot_id.clone();
        let release = PendingRelease {
            snapshot_id: snapshot_id.clone(),
            pin_id: self.pin_id.clone(),
            live_lock,
        };
        let pins = Arc::clone(&self.pins);
        let finish = move || {
            pins.active_pins
                .lock()
                .expect("active pins mutex should not be poisoned")
                .remove(&snapshot_id);
            notify.notify_waiters();
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let pins = Arc::clone(&self.pins);
            let inflight_guard = Arc::clone(&pins.inflight_pins).try_read_owned().ok();
            runtime.spawn(async move {
                let _inflight = match inflight_guard {
                    Some(guard) => guard,
                    None => Arc::clone(&pins.inflight_pins).read_owned().await,
                };
                pins.release_pin_and_clear_hold(release).await;
                finish();
            });
        } else {
            self.pins
                .pending_releases
                .lock()
                .expect("pending releases mutex should not be poisoned")
                .push(release);
            finish();
        }
    }
}

/// Node-local shared pins and the durable holds needed across restarts.
pub struct SnapshotPins {
    catalog: Arc<dyn SnapshotCatalog>,
    holds_store: HoldsStore,
    active_pins: std::sync::Mutex<HashMap<SnapshotId, SharedNodePin>>,
    pending_releases: std::sync::Mutex<Vec<PendingRelease>>,
    pin_locks: Arc<DashMap<SnapshotId, Arc<tokio::sync::Mutex<()>>>>,
    inflight_pins: Arc<tokio::sync::RwLock<()>>,
}

impl SnapshotPins {
    pub fn new(catalog: Arc<dyn SnapshotCatalog>, holds_dir: PathBuf) -> Self {
        Self::from_holds_store(catalog, HoldsStore::new(holds_dir))
    }

    fn from_holds_store(catalog: Arc<dyn SnapshotCatalog>, holds_store: HoldsStore) -> Self {
        Self {
            catalog,
            holds_store,
            active_pins: std::sync::Mutex::new(HashMap::new()),
            pending_releases: std::sync::Mutex::new(Vec::new()),
            pin_locks: Arc::new(DashMap::new()),
            inflight_pins: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    fn pin_lease(self: &Arc<Self>, snapshot_id: SnapshotId, pin_id: String) -> SnapshotLease {
        Arc::new(PinLeaseGuard {
            snapshot_id,
            pin_id,
            pins: Arc::clone(self),
        })
    }

    async fn release_pin_and_clear_hold(&self, release: PendingRelease) {
        let released = self
            .catalog
            .release_pin(&release.snapshot_id, &release.pin_id)
            .await
            .is_ok();
        if released && self.holds_store.delete_hold(&release.pin_id).await.is_ok() {
            if let Some(path) = release
                .live_lock
                .as_ref()
                .and_then(|lock| lock.lock_path.as_ref())
            {
                let _ = std::fs::remove_file(path);
            }
        } else {
            self.pending_releases
                .lock()
                .expect("pending releases mutex should not be poisoned")
                .push(release);
        }
    }

    async fn retry_pending_releases(&self) {
        let queued = {
            let mut pending = self
                .pending_releases
                .lock()
                .expect("pending releases mutex should not be poisoned");
            std::mem::take(&mut *pending)
        };
        for item in queued {
            self.release_pin_and_clear_hold(item).await;
        }
    }

    pub async fn acquire_pin(self: &Arc<Self>, id: &SnapshotId) -> RepositoryResult<SnapshotLease> {
        self.retry_pending_releases().await;
        let guard = SnapshotMutationGuard::acquire(Arc::clone(&self.pin_locks), id.clone()).await;

        let lease = loop {
            let closed = {
                let mut active = self
                    .active_pins
                    .lock()
                    .expect("active pins mutex should not be poisoned");
                let Some(pin) = active.get_mut(id) else {
                    break None;
                };
                if pin.ref_count != 0 {
                    pin.ref_count += 1;
                    break Some(self.pin_lease(id.clone(), pin.pin_id.clone()));
                }
                Arc::clone(&pin.closed_notify).notified_owned()
            };
            closed.await;
        };

        if let Some(lease) = lease {
            // Keep the shared pin alive while checking admission for this reader.
            let Some(entry) = self.catalog.get(&id.to_string()).await? else {
                return Err(RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                });
            };
            if entry.deleting {
                return Err(RepositoryError::Unavailable {
                    reason: format!("snapshot '{id}' is retired"),
                });
            }
            return Ok(lease);
        }

        let inflight = Arc::clone(&self.inflight_pins).read_owned().await;
        let pins = Arc::clone(self);
        let snapshot_id = id.clone();
        tokio::spawn(async move {
            let _guard = guard;
            let _inflight = inflight;
            let pin_id = uuid::Uuid::now_v7().to_string();
            let lock = LivePinLock::acquire(&pins.holds_store, &pin_id)?;
            let Some(live_lock) = lock else {
                return Err(RepositoryError::Unavailable {
                    reason: format!("snapshot pin '{pin_id}' is already held"),
                });
            };
            pins.holds_store.put_hold(&pin_id, &snapshot_id).await?;

            let error = match pins.catalog.pin(&snapshot_id, &pin_id).await {
                Ok(Some(_)) => None,
                Ok(None) => Some(RepositoryError::SnapshotNotFound {
                    lookup: snapshot_id.to_string(),
                }),
                Err(error) => Some(error),
            };
            if let Some(error) = error {
                pins.release_pin_and_clear_hold(PendingRelease {
                    snapshot_id,
                    pin_id,
                    live_lock: Some(live_lock),
                })
                .await;
                return Err(error);
            }

            let lease = pins.pin_lease(snapshot_id.clone(), pin_id.clone());
            pins.active_pins
                .lock()
                .expect("active pins mutex should not be poisoned")
                .insert(
                    snapshot_id,
                    SharedNodePin {
                        pin_id,
                        ref_count: 1,
                        retain_for_restart: false,
                        closed_notify: Arc::new(tokio::sync::Notify::new()),
                        live_lock: Some(live_lock),
                    },
                );
            Ok(lease)
        })
        .await
        .map_err(|error| RepositoryError::backend("finish snapshot pin task", error))?
    }

    pub async fn recover_pins(
        self: &Arc<Self>,
        retained_snapshots: &[SnapshotId],
    ) -> RepositoryResult<HashMap<SnapshotId, Vec<SnapshotLease>>> {
        let store = &self.holds_store;
        let mut needed_counts: HashMap<SnapshotId, usize> = HashMap::new();
        for id in retained_snapshots {
            *needed_counts.entry(id.clone()).or_insert(0) += 1;
        }
        let entries = store.list_holds().await?;
        let mut verified = HashMap::new();
        for (pin_id, raw_snapshot) in entries {
            let Some(live_lock) = LivePinLock::acquire(store, &pin_id)? else {
                continue;
            };
            let Ok(snapshot_id) = SnapshotId::parse(raw_snapshot.trim()) else {
                let _ = store.delete_hold(&pin_id).await;
                continue;
            };
            if let Some(&count) = needed_counts
                .get(&snapshot_id)
                .filter(|_| !verified.contains_key(&snapshot_id))
            {
                match self.catalog.pin(&snapshot_id, &pin_id).await {
                    Ok(Some(_)) => {
                        verified.insert(snapshot_id, (pin_id, count, live_lock));
                    }
                    Ok(None) | Err(RepositoryError::Unavailable { .. }) => {
                        let _ = store.delete_hold(&pin_id).await;
                    }
                    Err(error) => return Err(error),
                }
            } else {
                self.release_pin_and_clear_hold(PendingRelease {
                    snapshot_id,
                    pin_id,
                    live_lock: Some(live_lock),
                })
                .await;
            }
        }
        let mut recovered = HashMap::with_capacity(verified.len());
        let mut active = self
            .active_pins
            .lock()
            .expect("active pins mutex should not be poisoned");
        for (snapshot_id, (pin_id, count, live_lock)) in verified {
            active.insert(
                snapshot_id.clone(),
                SharedNodePin {
                    pin_id: pin_id.clone(),
                    ref_count: count,
                    retain_for_restart: false,
                    closed_notify: Arc::new(tokio::sync::Notify::new()),
                    live_lock: Some(live_lock),
                },
            );
            let leases = (0..count)
                .map(|_| self.pin_lease(snapshot_id.clone(), pin_id.clone()))
                .collect();
            recovered.insert(snapshot_id, leases);
        }
        Ok(recovered)
    }

    pub async fn shutdown(&self) {
        let _guard = self.inflight_pins.write().await;
        self.retry_pending_releases().await;
    }
}

#[cfg(test)]
mod tests {
    use agentenv_metadata::{transport, DatabaseConfig, MetaServer};
    use tempfile::TempDir;

    use super::*;
    use crate::repository::{
        backends::PosixFsRepository, BlobBody, BlobKey, LayerRole, SnapshotFile,
    };
    use crate::snapshot::{run_gc_once, MetadataCatalog, SnapshotManifest, SnapshotRecord};

    #[tokio::test]
    async fn shared_pin_and_recovery_respect_live_locks_and_restart_retention() -> anyhow::Result<()>
    {
        let root = TempDir::new()?;
        let database = DatabaseConfig::Sqlite {
            path: root.path().join("pins.sqlite3"),
        };
        database.migrate().await?;
        let server = MetaServer::open(&database).await?;
        let bytes = b"snapshot layer".to_vec();
        let digest = crate::digest::sha256_digest(&bytes);
        let record = SnapshotRecord::mock_ready(SnapshotManifest {
            isolated_layers: true,
            memory_layers: vec![agentenv_domain::ManagedLayerRef {
                digest: digest.clone(),
                size: bytes.len() as u64,
                uuid: Some(uuid::Uuid::now_v7().to_string()),
            }],
            ..SnapshotManifest::mock()
        });
        let id = record.id.clone();
        let meta = transport::embedded(server.clone());
        let repo: Arc<dyn crate::repository::Repository> =
            Arc::new(PosixFsRepository::new(root.path().join("repository")));
        let holds = root.path().join("shared-holds");
        let catalog: Arc<dyn SnapshotCatalog> =
            Arc::new(MetadataCatalog::new(meta.clone(), Arc::clone(&repo)));
        let pins = Arc::new(SnapshotPins::new(Arc::clone(&catalog), holds.clone()));
        let isolated = BlobKey::layer(digest.clone(), LayerRole::Memory, true);
        let shared = BlobKey::layer(digest, LayerRole::Memory, false);
        for key in [&shared, &isolated] {
            repo.put_blob(key, BlobBody::Bytes(bytes.clone())).await?;
        }
        repo.put_blob(
            &BlobKey::Snapshot(id.clone(), SnapshotFile::Record),
            BlobBody::Bytes(serde_json::to_vec(&record.artifact_json()?)?),
        )
        .await?;
        server
            .register_snapshot((&record).into(), record.manifest.unwrap(), &[])
            .await?;

        let paused_sandbox_lease = pins.acquire_pin(&id).await?;
        assert!(pins.pin_locks.is_empty());
        let normal_reader_lease = pins.acquire_pin(&id).await?;
        paused_sandbox_lease.disarm_for_restart();
        drop(paused_sandbox_lease);
        drop(normal_reader_lease);
        pins.shutdown().await;

        catalog.delete(&id.to_string()).await?;
        assert_eq!(
            run_gc_once(&meta, repo.as_ref(), 64)
                .await?
                .reaped_snapshots,
            0
        );
        assert!(repo.has_blob(&isolated).await?);

        let restarted = Arc::new(SnapshotPins::new(Arc::clone(&catalog), holds));
        let recovered = restarted.recover_pins(std::slice::from_ref(&id)).await?;
        assert_eq!(recovered.get(&id).map(Vec::len), Some(1));
        std::thread::spawn(move || drop(recovered)).join().unwrap();
        assert_eq!(
            restarted
                .pending_releases
                .lock()
                .expect("pending releases mutex")
                .len(),
            1
        );

        restarted.shutdown().await;
        let gc = run_gc_once(&meta, repo.as_ref(), 64).await?;
        assert_eq!(gc.reaped_snapshots, 1);
        assert_eq!(gc.deleted_layers, 1);
        assert!(!repo.has_blob(&isolated).await?);
        assert!(repo.has_blob(&shared).await?);
        server.close().await;
        Ok(())
    }
}
