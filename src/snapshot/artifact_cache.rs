use std::collections::HashMap;
use std::future::Future;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use tokio::fs;
use tokio::sync::{watch, Mutex as AsyncMutex};
use tracing::warn;

use crate::snapshot::types::RuntimeArtifactLease;

const DEFAULT_MAX_SIZE_BYTES: u64 = 10 * 1024 * 1024 * 1024; // 10 GB
const EVICTION_TARGET_RATIO: f64 = 0.8;

/// A reference-counted handle to a cached file.
///
/// While held, the underlying file is pinned and will not be evicted by LRU.
/// When all handles for a given key are dropped the ref-count reaches zero and
/// the file becomes eligible for eviction.
pub(crate) struct CacheHandle {
    cache: Arc<LocalArtifactCache>,
    key: String,
    local_path: PathBuf,
}

impl CacheHandle {
    /// Returns the local filesystem path of the cached file.
    pub(crate) fn path(&self) -> &Path {
        &self.local_path
    }
}

impl Drop for CacheHandle {
    fn drop(&mut self) {
        self.cache.release(&self.key);
    }
}

/// Lease implementation that keeps a collection of [`CacheHandle`]s pinned.
///
/// Stored as `RunnableSnapshot._lease`; as long as the sandbox keeps the
/// runnable snapshot the artifact files stay pinned in the local cache.
pub(crate) struct CacheArtifactLease {
    pub(crate) _handles: Vec<CacheHandle>,
}

impl RuntimeArtifactLease for CacheArtifactLease {}

struct CacheEntry {
    local_path: PathBuf,
    size: u64,
    last_accessed: Instant,
    ref_count: usize,
}

struct CacheIndex {
    entries: HashMap<String, CacheEntry>,
    total_size: u64,
}

/// Tracks in-flight fetches so concurrent callers that request the same key
/// wait for a single materialization rather than duplicating work.
#[derive(Clone)]
struct InflightFetch {
    receiver: watch::Receiver<Option<InflightFetchResult>>,
}

#[derive(Clone, Debug)]
enum InflightFetchResult {
    Success,
    Failed(SharedFetchError),
}

#[derive(Clone, Debug)]
struct SharedFetchError {
    message: String,
}

/// Node-local disk cache for downloaded or materialized runtime artifacts.
///
/// Design invariants:
/// - Files with `ref_count > 0` are never evicted.
/// - Fetches are deduplicated: only one fetch per key at a time.
/// - Entries are node-local derived state that can be regenerated or
///   re-fetched if the cache is missing or partially evicted.
pub(crate) struct LocalArtifactCache {
    cache_root: PathBuf,
    max_size_bytes: u64,
    index: Mutex<CacheIndex>,
    inflight: AsyncMutex<HashMap<String, InflightFetch>>,
}

impl LocalArtifactCache {
    pub(crate) fn new(cache_root: PathBuf, max_size_gb: Option<u64>) -> Result<Arc<Self>> {
        let max_size_bytes = max_size_gb
            .map(|gb| gb * 1024 * 1024 * 1024)
            .unwrap_or(DEFAULT_MAX_SIZE_BYTES);
        prepare_cache_root(&cache_root)?;

        Ok(Arc::new(Self {
            cache_root,
            max_size_bytes,
            index: Mutex::new(CacheIndex {
                entries: HashMap::new(),
                total_size: 0,
            }),
            inflight: AsyncMutex::new(HashMap::new()),
        }))
    }

    /// Ensure a cache key is materialized locally and return a handle that pins it.
    ///
    /// The provided `fetch` closure is responsible only for writing the local
    /// file and returning its size. The cache owns path mapping, deduplication,
    /// bookkeeping, pinning, and eviction.
    pub(crate) async fn ensure_cached<F, Fut>(
        self: &Arc<Self>,
        key: &str,
        fetch: F,
    ) -> Result<CacheHandle>
    where
        F: Fn(PathBuf) -> Fut,
        Fut: Future<Output = Result<u64>>,
    {
        let local_path = self.key_to_local_path(key)?;
        self.ensure_cached_at(key, local_path, fetch).await
    }

    /// Ensure a cache key is materialized at a caller-selected path and return
    /// a handle that pins it. Concurrent callers for the same key share one
    /// materialization.
    ///
    /// The caller-selected path is still part of the cache identity contract:
    /// a key must always resolve to the same path, and distinct keys must not
    /// share a path.
    pub(crate) async fn ensure_cached_at<F, Fut>(
        self: &Arc<Self>,
        key: &str,
        local_path: PathBuf,
        fetch: F,
    ) -> Result<CacheHandle>
    where
        F: Fn(PathBuf) -> Fut,
        Fut: Future<Output = Result<u64>>,
    {
        loop {
            if let Some(handle) = self.try_acquire(key) {
                return Ok(handle);
            }

            if fs::try_exists(&local_path)
                .await
                .with_context(|| format!("check local cache file '{}'", local_path.display()))?
            {
                return self.pin_local_file(key, &local_path).await;
            }

            let sender = {
                let mut inflight = self.inflight.lock().await;
                if let Some(mut existing) = inflight.get(key).cloned() {
                    drop(inflight);
                    if wait_for_inflight_result(&mut existing.receiver).await?
                        == InflightWait::Abandoned
                    {
                        self.clear_abandoned_inflight(key, &existing.receiver).await;
                    }
                    continue;
                }

                let (sender, receiver) = watch::channel(None);
                inflight.insert(key.to_string(), InflightFetch { receiver });
                sender
            };

            if let Some(parent) = local_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("create local cache dir '{}'", parent.display()))?;
            }
            let result = fetch(local_path.clone())
                .await
                .with_context(|| format!("materialize '{key}' into local cache"));

            let size = match result {
                Ok(size) => size,
                Err(error) => {
                    let shared_error = SharedFetchError::from_anyhow(&error);
                    self.finish_inflight(key, sender, InflightFetchResult::Failed(shared_error))
                        .await;
                    return Err(error);
                }
            };

            {
                let mut idx = self.lock_index();
                if let Some(previous) = idx.entries.insert(
                    key.to_string(),
                    CacheEntry {
                        local_path: local_path.clone(),
                        size,
                        last_accessed: Instant::now(),
                        ref_count: 1,
                    },
                ) {
                    idx.total_size = idx.total_size.saturating_sub(previous.size);
                }
                idx.total_size += size;
            }

            self.finish_inflight(key, sender, InflightFetchResult::Success)
                .await;

            if self.is_over_limit() {
                let cache = Arc::clone(self);
                tokio::spawn(async move {
                    if let Err(error) = cache.evict_lru() {
                        warn!(error = %error, "failed to evict snapshot artifact cache entries");
                    }
                });
            }

            return Ok(CacheHandle {
                cache: Arc::clone(self),
                key: key.to_string(),
                local_path,
            });
        }
    }

    /// Register an already-materialized local file with the cache and pin it.
    ///
    /// The same key/path identity contract as [`Self::ensure_cached_at`] applies.
    pub(crate) async fn pin_local_file(
        self: &Arc<Self>,
        key: &str,
        local_path: &Path,
    ) -> Result<CacheHandle> {
        loop {
            if let Some(handle) = self.try_acquire(key) {
                return Ok(handle);
            }

            let existing = {
                let inflight = self.inflight.lock().await;
                inflight.get(key).cloned()
            };
            if let Some(mut existing) = existing {
                if wait_for_inflight_result(&mut existing.receiver).await?
                    == InflightWait::Abandoned
                {
                    self.clear_abandoned_inflight(key, &existing.receiver).await;
                }
                continue;
            }
            break;
        }

        // A local materialization can still disappear or change between the
        // metadata read below and index insertion. The cache tolerates this:
        // later `try_acquire()` checks file existence and drops stale entries.
        let size = tokio::fs::metadata(local_path)
            .await
            .with_context(|| format!("stat local cache file '{}'", local_path.display()))?
            .len();

        let mut idx = self.lock_index();
        if let Some(previous) = idx.entries.insert(
            key.to_string(),
            CacheEntry {
                local_path: local_path.to_path_buf(),
                size,
                last_accessed: Instant::now(),
                ref_count: 1,
            },
        ) {
            idx.total_size = idx.total_size.saturating_sub(previous.size);
        }
        idx.total_size += size;

        Ok(CacheHandle {
            cache: Arc::clone(self),
            key: key.to_string(),
            local_path: local_path.to_path_buf(),
        })
    }

    fn key_to_local_path(&self, key: &str) -> Result<PathBuf> {
        let mut path = self.cache_root.clone();
        let mut saw_component = false;
        for component in Path::new(key).components() {
            match component {
                Component::Normal(part) => {
                    path.push(part);
                    saw_component = true;
                }
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(anyhow!(
                        "invalid cache key '{key}': path traversal is not allowed"
                    ));
                }
            }
        }
        if !saw_component {
            return Err(anyhow!("invalid cache key '{key}': key is empty"));
        }
        Ok(path)
    }

    fn lock_index(&self) -> std::sync::MutexGuard<'_, CacheIndex> {
        self.index.lock().unwrap_or_else(|poisoned| {
            warn!("snapshot artifact cache index mutex poisoned; recovering");
            poisoned.into_inner()
        })
    }

    async fn finish_inflight(
        &self,
        key: &str,
        sender: watch::Sender<Option<InflightFetchResult>>,
        result: InflightFetchResult,
    ) {
        let _ = sender.send(Some(result));
        let mut inflight = self.inflight.lock().await;
        inflight.remove(key);
    }

    async fn clear_abandoned_inflight(
        &self,
        key: &str,
        receiver: &watch::Receiver<Option<InflightFetchResult>>,
    ) {
        let mut inflight = self.inflight.lock().await;
        let is_same_fetch = inflight
            .get(key)
            .is_some_and(|current| current.receiver.same_channel(receiver));
        if is_same_fetch {
            inflight.remove(key);
        }
    }

    fn try_acquire(self: &Arc<Self>, key: &str) -> Option<CacheHandle> {
        let mut idx = self.lock_index();
        let entry = idx.entries.get_mut(key)?;
        if !entry.local_path.exists() {
            let size = entry.size;
            idx.entries.remove(key);
            idx.total_size = idx.total_size.saturating_sub(size);
            return None;
        }
        entry.ref_count += 1;
        entry.last_accessed = Instant::now();
        Some(CacheHandle {
            cache: Arc::clone(self),
            key: key.to_string(),
            local_path: entry.local_path.clone(),
        })
    }

    fn release(&self, key: &str) {
        let mut idx = self.lock_index();
        if let Some(entry) = idx.entries.get_mut(key) {
            entry.ref_count = entry.ref_count.saturating_sub(1);
        }
    }

    fn is_over_limit(&self) -> bool {
        let idx = self.lock_index();
        idx.total_size > self.max_size_bytes
    }

    fn evict_lru(&self) -> Result<()> {
        let target = (self.max_size_bytes as f64 * EVICTION_TARGET_RATIO) as u64;

        let (mut candidates, total_size) = {
            let idx = self.lock_index();
            if idx.total_size <= target {
                return Ok(());
            }

            let candidates = idx
                .entries
                .iter()
                .filter(|(_, entry)| entry.ref_count == 0)
                .map(|(key, entry)| (key.clone(), entry.last_accessed, entry.size))
                .collect::<Vec<_>>();
            (candidates, idx.total_size)
        };
        if total_size <= target {
            return Ok(());
        }

        candidates.sort_by_key(|(_, ts, _)| *ts);

        let mut idx = self.lock_index();
        let mut removals = Vec::new();
        for (key, _, size) in candidates {
            if idx.total_size <= target {
                break;
            }
            let Some(entry) = idx.entries.get(&key) else {
                continue;
            };
            if entry.ref_count > 0 {
                continue;
            }
            removals.push((key.clone(), entry.local_path.clone(), size));
            idx.entries.remove(&key);
            idx.total_size = idx.total_size.saturating_sub(size);
        }
        drop(idx);

        for (key, path, _) in removals {
            if let Err(error) = std::fs::remove_file(&path) {
                if error.kind() != ErrorKind::NotFound {
                    warn!(
                        cache_key = %key,
                        path = %path.display(),
                        error = %error,
                        "failed to remove evicted cache file"
                    );
                }
            }
        }

        Ok(())
    }
}

impl SharedFetchError {
    fn from_anyhow(error: &anyhow::Error) -> Self {
        Self {
            message: format!("{error:#}"),
        }
    }

    fn into_anyhow(self) -> anyhow::Error {
        anyhow!(self.message)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InflightWait {
    Finished,
    Abandoned,
}

async fn wait_for_inflight_result(
    receiver: &mut watch::Receiver<Option<InflightFetchResult>>,
) -> Result<InflightWait> {
    loop {
        if let Some(result) = receiver.borrow().clone() {
            return match result {
                InflightFetchResult::Success => Ok(InflightWait::Finished),
                InflightFetchResult::Failed(error) => Err(error.into_anyhow()),
            };
        }

        if receiver.changed().await.is_err() {
            return Ok(InflightWait::Abandoned);
        }
    }
}

fn prepare_cache_root(cache_root: &Path) -> Result<()> {
    std::fs::create_dir_all(cache_root).with_context(|| {
        format!(
            "create snapshot artifact cache root '{}'",
            cache_root.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    fn test_cache(dir: &Path) -> Arc<LocalArtifactCache> {
        LocalArtifactCache::new(dir.join("cache"), Some(1)).unwrap()
    }

    #[tokio::test]
    async fn ensure_cached_materializes_and_pins() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let cache = test_cache(tempdir.path());
        let key = "artifacts/t1/vm_state.bin";
        let handle = cache
            .ensure_cached(key, |dest| async move {
                tokio::fs::write(dest, b"snap-data").await?;
                Ok(9)
            })
            .await
            .unwrap();

        assert!(handle.path().exists());
        assert_eq!(std::fs::read(handle.path()).unwrap(), b"snap-data".to_vec());

        {
            let idx = cache.lock_index();
            let entry = idx.entries.get(key).unwrap();
            assert_eq!(entry.ref_count, 1);
        }

        drop(handle);
        {
            let idx = cache.lock_index();
            let entry = idx.entries.get(key).unwrap();
            assert_eq!(entry.ref_count, 0);
        }
    }

    #[tokio::test]
    async fn evict_lru_skips_pinned_entries() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let cache = LocalArtifactCache::new(tempdir.path().join("cache"), None).unwrap();

        let handle_a = cache
            .ensure_cached("file-a", |dest| async move {
                tokio::fs::write(dest, b"aaa").await?;
                Ok(3)
            })
            .await
            .unwrap();
        let handle_b = cache
            .ensure_cached("file-b", |dest| async move {
                tokio::fs::write(dest, b"bbb").await?;
                Ok(3)
            })
            .await
            .unwrap();

        let path_a = handle_a.path().to_path_buf();
        drop(handle_a);

        {
            let mut idx = cache.lock_index();
            idx.total_size = cache.max_size_bytes + 1;
        }

        cache.evict_lru().unwrap();

        assert!(
            !path_a.exists() || {
                let idx = cache.lock_index();
                !idx.entries.contains_key("file-a")
            }
        );
        assert!(handle_b.path().exists());
    }

    #[tokio::test]
    async fn evict_lru_removes_unpinned_entries_when_over_limit() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(LocalArtifactCache {
            cache_root: tempdir.path().join("cache"),
            max_size_bytes: 10,
            index: Mutex::new(CacheIndex {
                entries: HashMap::new(),
                total_size: 0,
            }),
            inflight: AsyncMutex::new(HashMap::new()),
        });
        std::fs::create_dir_all(&cache.cache_root).unwrap();

        let handle_a = cache
            .ensure_cached("file-a", |dest| async move {
                tokio::fs::write(dest, b"aaaaaa").await?;
                Ok(6)
            })
            .await
            .unwrap();
        drop(handle_a);

        let handle_b = cache
            .ensure_cached("file-b", |dest| async move {
                tokio::fs::write(dest, b"bbbbbb").await?;
                Ok(6)
            })
            .await
            .unwrap();
        let path_b = handle_b.path().to_path_buf();
        drop(handle_b);

        cache.evict_lru().unwrap();

        let idx = cache.lock_index();
        assert!(idx.total_size <= 8);
        assert!(idx.entries.contains_key("file-b"));
        assert_eq!(idx.entries.len(), 1);
        assert!(path_b.exists());
    }

    #[tokio::test]
    async fn pin_local_file_reuses_cached_entry_without_double_counting_size() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let cache = test_cache(tempdir.path());
        let local_path = tempdir
            .path()
            .join("runtime")
            .join(crate::snapshot::SNAPSHOT_ARTIFACT_LAYOUT.overlaybd_image_config_file);
        std::fs::create_dir_all(local_path.parent().unwrap()).unwrap();
        std::fs::write(&local_path, b"runtime-config").unwrap();
        let expected_size = std::fs::metadata(&local_path).unwrap().len();

        let first = cache
            .pin_local_file("runtime/snapshot/image.json", &local_path)
            .await
            .unwrap();
        let second = cache
            .pin_local_file("runtime/snapshot/image.json", &local_path)
            .await
            .unwrap();

        {
            let idx = cache.lock_index();
            let entry = idx.entries.get("runtime/snapshot/image.json").unwrap();
            assert_eq!(entry.ref_count, 2);
            assert_eq!(idx.total_size, expected_size);
        }

        drop(first);
        drop(second);
        let idx = cache.lock_index();
        let entry = idx.entries.get("runtime/snapshot/image.json").unwrap();
        assert_eq!(entry.ref_count, 0);
    }

    #[tokio::test]
    async fn ensure_cached_rejects_path_traversal_keys() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let cache = test_cache(tempdir.path());

        let err = match cache
            .ensure_cached("../escape", |_dest| async move { Ok(0) })
            .await
        {
            Ok(_) => panic!("path traversal key should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("path traversal"));
    }

    #[tokio::test]
    async fn ensure_cached_reuses_existing_file_after_restart() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let cache_root = tempdir.path().join("cache");
        let existing = cache_root.join("artifacts/t1/vm_state.bin");
        std::fs::create_dir_all(existing.parent().unwrap()).unwrap();
        std::fs::write(&existing, b"warm-cache").unwrap();

        let cache = LocalArtifactCache::new(cache_root, Some(1)).unwrap();
        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let fetch_counter = Arc::clone(&fetch_calls);

        let handle = cache
            .ensure_cached("artifacts/t1/vm_state.bin", move |_dest| {
                let fetch_counter = Arc::clone(&fetch_counter);
                async move {
                    fetch_counter.fetch_add(1, Ordering::SeqCst);
                    Ok(0)
                }
            })
            .await
            .unwrap();

        assert_eq!(
            std::fs::read(handle.path()).unwrap(),
            b"warm-cache".to_vec()
        );
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ensure_cached_recovers_when_materialization_leader_is_cancelled() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let cache = test_cache(tempdir.path());
        let key = "runtime/snapshot/rootfs/image.json";
        let started = Arc::new(tokio::sync::Notify::new());
        let leader_cache = Arc::clone(&cache);
        let leader_started = Arc::clone(&started);

        let leader = tokio::spawn(async move {
            leader_cache
                .ensure_cached(key, move |_dest| {
                    let leader_started = Arc::clone(&leader_started);
                    async move {
                        leader_started.notify_one();
                        std::future::pending::<Result<u64>>().await
                    }
                })
                .await
        });
        started.notified().await;
        leader.abort();
        let _ = leader.await;

        let handle = cache
            .ensure_cached(key, |dest| async move {
                tokio::fs::write(dest, b"recovered").await?;
                Ok(9)
            })
            .await
            .unwrap();

        assert_eq!(std::fs::read(handle.path()).unwrap(), b"recovered");
    }
}
