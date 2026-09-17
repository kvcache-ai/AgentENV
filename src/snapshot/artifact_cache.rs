use std::collections::HashMap;
use std::future::Future;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use tokio::fs;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tracing::warn;

const DEFAULT_MAX_SIZE_BYTES: u64 = 10 * 1024 * 1024 * 1024; // 10 GB
const EVICTION_TARGET_RATIO: f64 = 0.8;

/// A handle that keeps a cached file pinned.
///
/// While held, the underlying file will not be evicted by LRU.
pub(crate) struct CacheHandle {
    local_path: PathBuf,
    _pin: Arc<()>,
}

impl CacheHandle {
    /// Returns the local filesystem path of the cached file.
    pub(crate) fn path(&self) -> &Path {
        &self.local_path
    }
}

struct CacheEntry {
    local_path: PathBuf,
    size: u64,
    last_accessed: Instant,
    pin: Arc<()>,
}

struct CacheIndex {
    entries: HashMap<String, CacheEntry>,
    total_size: u64,
}

/// Node-local disk cache for downloaded or materialized runtime artifacts.
///
/// Design invariants:
/// - Files with a live [`CacheHandle`] are never evicted.
/// - Fetches are deduplicated: only one fetch per key at a time.
/// - Entries are node-local derived state that can be regenerated or
///   re-fetched if the cache is missing or partially evicted.
pub(crate) struct LocalArtifactCache {
    cache_root: PathBuf,
    max_size_bytes: u64,
    index: Mutex<CacheIndex>,
    key_locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
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
            key_locks: Mutex::new(HashMap::new()),
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
        F: FnOnce(PathBuf) -> Fut,
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
        F: FnOnce(PathBuf) -> Fut,
        Fut: Future<Output = Result<u64>>,
    {
        if let Some(handle) = self.try_acquire(key) {
            return Ok(handle);
        }
        let _key_lock = self.acquire_key_lock(key).await;
        if let Some(handle) = self.try_acquire(key) {
            return Ok(handle);
        }

        let size = match fs::metadata(&local_path).await {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let parent = local_path
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("create local cache dir '{}'", parent.display()))?;

                async {
                    let staging = tempfile::tempdir_in(parent)?;
                    let staged_path = staging.path().join("artifact");
                    let size = fetch(staged_path.clone()).await?;
                    std::fs::rename(&staged_path, &local_path)?;
                    Ok::<_, anyhow::Error>(size)
                }
                .await
                .with_context(|| format!("materialize '{key}' into local cache"))?
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("stat local cache file '{}'", local_path.display()));
            }
        };

        let pin = Arc::new(());
        let mut idx = self.lock_index();
        idx.entries.insert(
            key.to_string(),
            CacheEntry {
                local_path: local_path.clone(),
                size,
                last_accessed: Instant::now(),
                pin: Arc::clone(&pin),
            },
        );
        idx.total_size += size;
        drop(idx);
        let handle = CacheHandle {
            local_path,
            _pin: pin,
        };
        if self.is_over_limit() {
            let cache = Arc::clone(self);
            tokio::spawn(async move {
                cache.evict_lru().await;
            });
        }

        Ok(handle)
    }

    async fn acquire_key_lock(self: &Arc<Self>, key: &str) -> CacheKeyLock {
        let lock = {
            let mut locks = self.key_locks.lock().unwrap_or_else(|poisoned| {
                warn!("snapshot artifact cache key-lock mutex poisoned; recovering");
                poisoned.into_inner()
            });
            locks
                .entry(key.to_string())
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        let mut key_lock = CacheKeyLock {
            cache: Arc::clone(self),
            key: key.to_string(),
            lock: Some(lock),
            guard: None,
        };
        key_lock.guard = Some(
            key_lock
                .lock
                .as_ref()
                .expect("cache key lock missing")
                .clone()
                .lock_owned()
                .await,
        );
        key_lock
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

    fn try_acquire(&self, key: &str) -> Option<CacheHandle> {
        let mut idx = self.lock_index();
        let entry = idx.entries.get_mut(key)?;
        if !entry.local_path.exists() {
            let size = entry.size;
            idx.entries.remove(key);
            idx.total_size = idx.total_size.saturating_sub(size);
            return None;
        }
        entry.last_accessed = Instant::now();
        Some(CacheHandle {
            local_path: entry.local_path.clone(),
            _pin: Arc::clone(&entry.pin),
        })
    }

    fn is_over_limit(&self) -> bool {
        let idx = self.lock_index();
        idx.total_size > self.max_size_bytes
    }

    pub(crate) async fn evict_lru(self: &Arc<Self>) {
        let target = (self.max_size_bytes as f64 * EVICTION_TARGET_RATIO) as u64;

        let mut candidates = {
            let idx = self.lock_index();
            if idx.total_size <= target {
                return;
            }

            idx.entries
                .iter()
                .filter(|(_, entry)| Arc::strong_count(&entry.pin) == 1)
                .map(|(key, entry)| (key.clone(), entry.last_accessed))
                .collect::<Vec<_>>()
        };

        candidates.sort_by_key(|(_, last_accessed)| *last_accessed);
        for (key, _) in candidates {
            let _key_lock = self.acquire_key_lock(&key).await;
            let entry = {
                let mut idx = self.lock_index();
                if idx.total_size <= target {
                    return;
                }
                match idx.entries.get(&key) {
                    Some(entry) if Arc::strong_count(&entry.pin) == 1 => {}
                    _ => continue,
                }
                let entry = idx.entries.remove(&key).expect("cache entry disappeared");
                idx.total_size = idx.total_size.saturating_sub(entry.size);
                entry
            };

            if let Err(error) = std::fs::remove_file(&entry.local_path) {
                if error.kind() == ErrorKind::NotFound {
                    continue;
                }
                warn!(
                    cache_key = %key,
                    path = %entry.local_path.display(),
                    error = %error,
                    "failed to remove evicted cache file"
                );
                let mut idx = self.lock_index();
                idx.total_size += entry.size;
                idx.entries.insert(key, entry);
            }
        }
    }
}

struct CacheKeyLock {
    cache: Arc<LocalArtifactCache>,
    key: String,
    lock: Option<Arc<AsyncMutex<()>>>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for CacheKeyLock {
    fn drop(&mut self) {
        drop(self.guard.take());
        drop(self.lock.take());
        let mut locks = self.cache.key_locks.lock().unwrap_or_else(|poisoned| {
            warn!("snapshot artifact cache key-lock mutex poisoned; recovering");
            poisoned.into_inner()
        });
        if locks
            .get(&self.key)
            .is_some_and(|lock| Arc::strong_count(lock) == 1)
        {
            locks.remove(&self.key);
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

    use tokio::sync::Barrier;

    use super::*;

    fn test_cache(dir: &Path) -> Arc<LocalArtifactCache> {
        LocalArtifactCache::new(dir.join("cache"), Some(1)).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_cold_fetch_materializes_once_and_pins_all_handles() {
        const CALLERS: usize = 8;
        const KEY: &str = "artifacts/cold/vm_state.bin";

        let tempdir = tempfile::TempDir::new().unwrap();
        let cache = test_cache(tempdir.path());
        let barrier = Arc::new(Barrier::new(CALLERS));
        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();

        for _ in 0..CALLERS {
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            let fetch_calls = Arc::clone(&fetch_calls);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .ensure_cached(KEY, move |dest| {
                        let fetch_calls = Arc::clone(&fetch_calls);
                        async move {
                            fetch_calls.fetch_add(1, Ordering::SeqCst);
                            tokio::fs::write(dest, b"cold-data").await?;
                            Ok(9)
                        }
                    })
                    .await
                    .unwrap()
            }));
        }

        let mut handles = Vec::new();
        for task in tasks {
            handles.push(task.await.unwrap());
        }

        assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);
        assert_eq!(std::fs::read(handles[0].path()).unwrap(), b"cold-data");
        drop(handles);
    }

    #[tokio::test]
    async fn cancelled_fetch_does_not_publish_partial_data_and_can_be_retried() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let cache = test_cache(tempdir.path());
        let key = "artifacts/cancelled/vm_state.bin";
        let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(1);
        let fetch_cache = Arc::clone(&cache);
        let fetch = tokio::spawn(async move {
            fetch_cache
                .ensure_cached(key, |path| {
                    let started_tx = started_tx.clone();
                    async move {
                        tokio::fs::write(&path, b"partial").await?;
                        started_tx.send(()).await.unwrap();
                        std::future::pending::<Result<u64>>().await
                    }
                })
                .await
        });
        started_rx.recv().await.unwrap();
        fetch.abort();
        assert!(fetch.await.err().unwrap().is_cancelled());
        assert!(!cache.key_to_local_path(key).unwrap().exists());

        let handle = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cache.ensure_cached(key, |path| async move {
                tokio::fs::write(path, b"complete").await?;
                Ok(8)
            }),
        )
        .await
        .expect("a cancelled fetch must not block later callers")
        .unwrap();
        assert_eq!(std::fs::read(handle.path()).unwrap(), b"complete");
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
            key_locks: Mutex::new(HashMap::new()),
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

        cache.evict_lru().await;

        let idx = cache.lock_index();
        assert!(idx.total_size <= 8);
        assert!(idx.entries.contains_key("file-b"));
        assert_eq!(idx.entries.len(), 1);
        assert!(path_b.exists());
    }

    #[tokio::test]
    async fn warm_file_pins_keep_each_handle_leased() {
        const KEY: &str = "runtime/snapshot/image.json";

        let tempdir = tempfile::TempDir::new().unwrap();
        let cache = test_cache(tempdir.path());
        let local_path = tempdir.path().join("runtime").join("image.json");
        std::fs::create_dir_all(local_path.parent().unwrap()).unwrap();
        std::fs::write(&local_path, b"runtime-config").unwrap();
        let expected_size = std::fs::metadata(&local_path).unwrap().len();

        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let first = cache
            .ensure_cached_at(KEY, local_path.clone(), {
                let fetch_calls = Arc::clone(&fetch_calls);
                move |_dest| {
                    fetch_calls.fetch_add(1, Ordering::SeqCst);
                    async { anyhow::bail!("warm file should not be fetched") }
                }
            })
            .await
            .unwrap();
        let second = cache
            .ensure_cached_at(KEY, local_path.clone(), {
                let fetch_calls = Arc::clone(&fetch_calls);
                move |_dest| {
                    fetch_calls.fetch_add(1, Ordering::SeqCst);
                    async { anyhow::bail!("warm file should not be fetched") }
                }
            })
            .await
            .unwrap();
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 0);

        {
            let idx = cache.lock_index();
            assert_eq!(idx.total_size, expected_size);
            assert!(idx.entries.contains_key(KEY));
        }

        drop(first);
        {
            let mut idx = cache.lock_index();
            idx.total_size = cache.max_size_bytes + 1;
        }
        cache.evict_lru().await;
        assert!(second.path().exists());

        drop(second);
        cache.evict_lru().await;
        assert!(!local_path.exists());
        assert!(cache.key_locks.lock().unwrap().is_empty());
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

        assert_eq!(std::fs::read(handle.path()).unwrap(), b"warm-cache");
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 0);
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
}
