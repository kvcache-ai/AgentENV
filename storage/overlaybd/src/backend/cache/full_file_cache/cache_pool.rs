use super::super::meta::EntryPaths;
use super::super::{
    cache_key_digest, div_round_up, CacheFnTransFunc, DEFAULT_BLOCK_SIZE, DEFAULT_CACHE_DIR,
    DEFAULT_CAPACITY_BYTES, DEFAULT_CHECKPOINT_PERIOD, DEFAULT_DISK_AVAIL_BYTES,
    DEFAULT_EVICTION_PERIOD, EVICTION_MARK_BYTES, GIB, MAX_FREE_SPACE_BYTES, PAGE_SIZE,
    WATERMARK_RATIO,
};
use super::cache_entry::CacheEntry;
use super::cache_store::CachedFile;
use crate::config::CacheConfig;
use crate::io::virtual_file::VirtualFile;
use anyhow::{anyhow, bail, Context, Result};
use dashmap::DashMap;
use nix::errno::Errno;
use parking_lot::{Mutex, RwLock};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileCacheBackendOptions {
    pub cache_dir: PathBuf,
    pub capacity_bytes: u64,
    /// The refill unit and management unit of cache
    pub block_size: u64,
    /// Node-level cap on concurrently downloading chunks enforced by this
    /// backend's download scheduler, initialized from the global
    /// `DownloadConfig.max_inflight_blocks` when the backend is created.
    /// Bounds total scratch memory to `max_inflight_blocks` × each task's
    /// download chunk size (`download.blockSize`). Per-image overrides never
    /// resize it (see `BkDownloadScheduler`).
    pub bk_download_max_inflight_blocks: usize,
    /// Node-level cap on concurrently running background-download layer
    /// tasks, from the global `DownloadConfig.max_concurrent_files`.
    pub bk_download_max_concurrent_files: usize,
    /// Global `DownloadConfig.block_size` at backend creation: the maximum
    /// chunk size the scheduler accepts from any task, so the scratch budget
    /// `max_inflight_blocks × block_size` holds regardless of per-image
    /// overrides.
    pub bk_download_block_size: u32,
    /// Per-block read timeout for background downloads; a slow read is
    /// dropped and reissued instead of holding a block slot forever.
    pub bk_download_hedge_timeout: std::time::Duration,
}

impl Default for FileCacheBackendOptions {
    fn default() -> Self {
        let download = crate::config::DownloadConfig::default();
        Self {
            cache_dir: PathBuf::from(DEFAULT_CACHE_DIR),
            capacity_bytes: DEFAULT_CAPACITY_BYTES,
            block_size: DEFAULT_BLOCK_SIZE,
            bk_download_max_inflight_blocks: download.max_inflight_blocks,
            bk_download_max_concurrent_files: download.max_concurrent_files,
            bk_download_block_size: download.block_size,
            bk_download_hedge_timeout: super::super::bk_download::DEFAULT_HEDGE_TIMEOUT,
        }
    }
}

impl FileCacheBackendOptions {
    pub fn from_cache_config(cfg: &CacheConfig) -> Result<Self> {
        let download = crate::config::DownloadConfig::default();
        let mut opt = Self {
            cache_dir: PathBuf::from(&cfg.cache_dir),
            capacity_bytes: u64::from(cfg.cache_size_gb).saturating_mul(GIB),
            block_size: if cfg.refill_size > 0 {
                u64::from(cfg.refill_size)
            } else {
                DEFAULT_BLOCK_SIZE
            },
            // `CacheConfig` does not carry the download config; callers with
            // access to the global config (e.g. image_service) override these
            // from `DownloadConfig`.
            bk_download_max_inflight_blocks: download.max_inflight_blocks,
            bk_download_max_concurrent_files: download.max_concurrent_files,
            bk_download_block_size: download.block_size,
            bk_download_hedge_timeout: super::super::bk_download::DEFAULT_HEDGE_TIMEOUT,
        };
        opt.normalize()?;
        Ok(opt)
    }

    fn normalize(&mut self) -> Result<()> {
        if self.cache_dir.as_os_str().is_empty() {
            bail!("cache_dir cannot be empty");
        }
        if self.block_size == 0 {
            self.block_size = DEFAULT_BLOCK_SIZE;
        }
        if self.block_size < PAGE_SIZE
            || !self.block_size.is_multiple_of(PAGE_SIZE)
            || !self.block_size.is_power_of_two()
        {
            bail!(
                "block_size must be >= {PAGE_SIZE}, page-aligned, and a power of two; got {}",
                self.block_size
            );
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Public stat types (unchanged API)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes_used: u64,
    pub hits: u64,
    pub misses: u64,
    pub refills: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CachedFileStats {
    pub key: String,
    pub bytes_used: u64,
    pub hits: u64,
    pub misses: u64,
    pub refills: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheListType {
    All,
    Files,
    Dirs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachePoolStat {
    pub refill_unit: u32,
    pub total_size: u32,
    pub used_size: u32,
    pub evict_other: u64,
    pub evict_global: u64,
    pub evict_user: u64,
}

// ---------------------------------------------------------------------------
// CacheSlot — per-entry state in the DashMap
// ---------------------------------------------------------------------------

pub(crate) enum CacheSlot {
    Active(Arc<CacheEntry>),
    Evicting(Arc<Notify>),
}

impl CacheSlot {
    pub(crate) fn as_active(&self) -> Option<&Arc<CacheEntry>> {
        match self {
            CacheSlot::Active(e) => Some(e),
            CacheSlot::Evicting(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// BackendState — DashMap + pressure-state serialization
// ---------------------------------------------------------------------------

pub(crate) struct BackendState {
    pub(crate) cache_entries: DashMap<String, CacheSlot>,
    pub(crate) current_bytes: AtomicU64,
    pub(crate) is_full: AtomicBool,
    pub(crate) pressure_lock: Mutex<()>,
    pub(crate) evict_global: AtomicU64,
    pub(crate) evict_user: AtomicU64,
}

impl BackendState {
    fn new() -> Self {
        Self {
            cache_entries: DashMap::new(),
            current_bytes: AtomicU64::new(0),
            is_full: AtomicBool::new(false),
            pressure_lock: Mutex::new(()),
            evict_global: AtomicU64::new(0),
            evict_user: AtomicU64::new(0),
        }
    }

    pub(crate) async fn load_from_disk(options: &FileCacheBackendOptions) -> Result<Self> {
        let state = Self::new();
        std::fs::create_dir_all(&options.cache_dir)?;
        for item in std::fs::read_dir(&options.cache_dir)? {
            let item = item?;
            if !item.file_type()?.is_dir() {
                continue;
            }
            let cache_id = item.file_name().to_string_lossy().to_string();
            let paths = EntryPaths::new(&options.cache_dir, &cache_id);
            match CacheEntry::load_from_disk(cache_id.clone(), paths, options).await {
                Ok(entry) => {
                    let bytes = entry.total_cached_bytes();
                    state.current_bytes.fetch_add(bytes, Ordering::Relaxed);
                    state
                        .cache_entries
                        .insert(cache_id, CacheSlot::Active(entry));
                }
                Err(err) => {
                    tracing::warn!(?err, cache_id, "load cache from disk failed");
                    let dir = options.cache_dir.join(&cache_id);
                    let _ = std::fs::remove_dir_all(dir);
                }
            }
        }
        let disk = FileCacheBackend::capture_disk_pressure(options);
        let _pressure_guard = state.pressure_lock.lock();
        FileCacheBackend::publish_pressure_locked(&state, options, disk);
        drop(_pressure_guard);
        Ok(state)
    }
}

// ---------------------------------------------------------------------------
// FileCacheBackend
// ---------------------------------------------------------------------------

pub struct FileCacheBackend {
    pub(crate) options: Arc<FileCacheBackendOptions>,
    pub(crate) state: Arc<BackendState>,
    pub(crate) fn_trans_func: Arc<RwLock<Option<CacheFnTransFunc>>>,
    pub(crate) active_refills: Arc<AtomicU32>,
    pub(crate) bk_scheduler: Weak<super::super::bk_download::BkDownloadScheduler>,
    _bk_scheduler_owner: Option<Arc<super::super::bk_download::BkDownloadScheduler>>,
}

impl Clone for FileCacheBackend {
    /// Backend clones share cache state and the scheduler weak reference, but
    /// never inherit ownership of scheduler shutdown. Only the primary value
    /// returned by the constructor carries `_bk_scheduler_owner`.
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            state: self.state.clone(),
            fn_trans_func: self.fn_trans_func.clone(),
            active_refills: self.active_refills.clone(),
            bk_scheduler: self.bk_scheduler.clone(),
            _bk_scheduler_owner: None,
        }
    }
}

impl Drop for FileCacheBackend {
    /// Only the primary backend owns the scheduler; dropping it signals all
    /// scheduled download futures to wind down. Lightweight clones produced
    /// by `cached_file_backend` carry no owner and stop nothing.
    fn drop(&mut self) {
        if let Some(scheduler) = self._bk_scheduler_owner.take() {
            scheduler.stop();
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum EvictionCounter {
    Global,
    User,
}

#[derive(Debug, Clone, Copy, Default)]
struct DiskPressureSnapshot {
    evict_bytes: u64,
    suppress_cache_pressure: bool,
}

impl FileCacheBackend {
    pub async fn with_options(options: FileCacheBackendOptions) -> Result<Self> {
        Self::with_options_and_trans_func(options, None).await
    }

    pub async fn with_options_and_trans_func(
        mut options: FileCacheBackendOptions,
        fn_trans_func: Option<CacheFnTransFunc>,
    ) -> Result<Self> {
        options.normalize()?;
        std::fs::create_dir_all(&options.cache_dir)?;
        let state = BackendState::load_from_disk(&options).await?;
        let _bk_scheduler_owner = super::super::bk_download::BkDownloadScheduler::new(
            options.bk_download_max_inflight_blocks,
            options.bk_download_max_concurrent_files,
            options.bk_download_block_size,
            options.bk_download_hedge_timeout,
        );
        let backend = Self {
            options: Arc::new(options),
            state: Arc::new(state),
            fn_trans_func: Arc::new(RwLock::new(fn_trans_func)),
            active_refills: Arc::new(AtomicU32::new(0)),
            bk_scheduler: Arc::downgrade(&_bk_scheduler_owner),
            _bk_scheduler_owner: Some(_bk_scheduler_owner),
        };
        backend.start_periodic_eviction_worker();
        backend.start_checkpoint_worker();
        Ok(backend)
    }

    pub async fn from_cache_config(cfg: &CacheConfig) -> Result<Self> {
        Self::with_options(FileCacheBackendOptions::from_cache_config(cfg)?).await
    }

    // -------------------------------------------------------------------
    // Background workers
    // -------------------------------------------------------------------

    fn start_periodic_eviction_worker(&self) {
        let weak_state = Arc::downgrade(&self.state);
        let options = self.options.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                Self::periodic_eviction_loop(weak_state, options).await;
            });
            return;
        }

        let _ = std::thread::Builder::new()
            .name("overlaybd-cache-evict".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                let Ok(runtime) = runtime else {
                    return;
                };
                runtime.block_on(async move {
                    Self::periodic_eviction_loop(weak_state, options).await;
                });
            });
    }

    /// Start a background task that wake up every [DEFAULT_CHECKPOINT_PERIOD],
    /// and iterate all cache entries one by one, sync the data file and
    /// re-flush the cache metadata on local disk.
    fn start_checkpoint_worker(&self) {
        let weak_state = Arc::downgrade(&self.state);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut interval = tokio::time::interval(DEFAULT_CHECKPOINT_PERIOD);
                loop {
                    interval.tick().await;
                    let Some(state) = weak_state.upgrade() else {
                        break;
                    };
                    Self::checkpoint_dirty_entries(&state).await;
                }
            });
            return;
        }

        let _ = std::thread::Builder::new()
            .name("overlaybd-cache-checkpoint".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                let Ok(runtime) = runtime else {
                    return;
                };
                runtime.block_on(async move {
                    let mut interval = tokio::time::interval(DEFAULT_CHECKPOINT_PERIOD);
                    loop {
                        interval.tick().await;
                        let Some(state) = weak_state.upgrade() else {
                            break;
                        };
                        Self::checkpoint_dirty_entries(&state).await;
                    }
                });
            });
    }

    async fn checkpoint_dirty_entries(state: &BackendState) {
        // NOTE: do not iterator dashmap across await point
        let dirty_entries = state
            .cache_entries
            .iter()
            .filter_map(|slot_ref| {
                let entry = slot_ref.value().as_active()?;
                if entry.dirty.load(Ordering::Relaxed) {
                    Some(entry.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for entry in dirty_entries {
            let _ = entry.checkpoint().await;
        }
    }

    async fn periodic_eviction_loop(
        weak_state: std::sync::Weak<BackendState>,
        options: Arc<FileCacheBackendOptions>,
    ) {
        let mut interval = tokio::time::interval(DEFAULT_EVICTION_PERIOD);
        loop {
            interval.tick().await;
            let Some(state) = weak_state.upgrade() else {
                break;
            };
            Self::eviction_inner_for(&state, &options).await;
        }
    }

    // -------------------------------------------------------------------
    // Watermark / risk calculations
    // -------------------------------------------------------------------

    pub(crate) fn calc_water_mark(capacity_bytes: u64) -> u64 {
        let ratio_mark = capacity_bytes.saturating_mul(WATERMARK_RATIO) / 100;
        let free_space_mark = capacity_bytes.saturating_sub(MAX_FREE_SPACE_BYTES);
        ratio_mark.max(free_space_mark)
    }

    fn risk_mark_for_options(options: &FileCacheBackendOptions) -> u64 {
        let capacity = options.capacity_bytes;
        let water_mark = Self::calc_water_mark(capacity);
        capacity
            .saturating_sub(EVICTION_MARK_BYTES)
            .max((water_mark.saturating_add(capacity)) / 2)
    }

    #[cfg(test)]
    pub(crate) fn risk_mark(&self) -> u64 {
        Self::risk_mark_for_options(&self.options)
    }

    fn statvfs_for_path(path: &Path) -> Result<libc::statvfs> {
        let cpath =
            CString::new(path.as_os_str().as_bytes()).context("path contains interior NUL byte")?;
        let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        let ret = unsafe { libc::statvfs(cpath.as_ptr(), st.as_mut_ptr()) };
        if ret != 0 {
            return Err(Errno::last().into());
        }
        Ok(unsafe { st.assume_init() })
    }

    fn capture_disk_pressure(options: &FileCacheBackendOptions) -> DiskPressureSnapshot {
        let water_mark = Self::calc_water_mark(options.capacity_bytes);
        let Ok(st) = Self::statvfs_for_path(&options.cache_dir) else {
            return DiskPressureSnapshot::default();
        };
        let fs_capacity = st.f_frsize.saturating_mul(st.f_blocks);
        let disk_avail = st.f_bavail.saturating_mul(st.f_frsize);
        if disk_avail < DEFAULT_DISK_AVAIL_BYTES {
            DiskPressureSnapshot {
                evict_bytes: DEFAULT_DISK_AVAIL_BYTES.saturating_sub(disk_avail),
                suppress_cache_pressure: false,
            }
        } else {
            DiskPressureSnapshot {
                evict_bytes: 0,
                suppress_cache_pressure: fs_capacity <= water_mark,
            }
        }
    }

    fn pressure_evict_target_for_options(
        options: &FileCacheBackendOptions,
        current_bytes: u64,
        disk: DiskPressureSnapshot,
    ) -> u64 {
        let water_mark = Self::calc_water_mark(options.capacity_bytes);
        let evict_by_cache = if disk.suppress_cache_pressure {
            0
        } else if current_bytes >= water_mark {
            current_bytes.saturating_sub(water_mark)
        } else {
            0
        };
        evict_by_cache.max(disk.evict_bytes)
    }

    fn publish_pressure_locked(
        state: &BackendState,
        options: &FileCacheBackendOptions,
        disk: DiskPressureSnapshot,
    ) {
        let current_bytes = state.current_bytes.load(Ordering::Relaxed);
        let pressure = Self::pressure_evict_target_for_options(options, current_bytes, disk) > 0
            || current_bytes >= Self::risk_mark_for_options(options);
        state.is_full.store(pressure, Ordering::Relaxed);
    }

    fn add_current_bytes_for(state: &BackendState, options: &FileCacheBackendOptions, bytes: u64) {
        let disk = Self::capture_disk_pressure(options);
        let _pressure_guard = state.pressure_lock.lock();
        state.current_bytes.fetch_add(bytes, Ordering::Relaxed);
        Self::publish_pressure_locked(state, options, disk);
    }

    fn subtract_current_bytes_for(
        state: &BackendState,
        options: &FileCacheBackendOptions,
        bytes: u64,
    ) {
        let disk = Self::capture_disk_pressure(options);
        let _pressure_guard = state.pressure_lock.lock();
        state.current_bytes.fetch_sub(bytes, Ordering::Relaxed);
        Self::publish_pressure_locked(state, options, disk);
    }

    pub(crate) fn add_current_bytes(&self, bytes: u64) {
        Self::add_current_bytes_for(&self.state, &self.options, bytes);
    }

    pub(crate) fn subtract_current_bytes(&self, bytes: u64) {
        Self::subtract_current_bytes_for(&self.state, &self.options, bytes);
    }

    // -------------------------------------------------------------------
    // Eviction
    // -------------------------------------------------------------------

    fn entry_is_busy(entry: &CacheEntry) -> bool {
        entry.open_count.load(Ordering::SeqCst) > 0 || !entry.block_states.lock().is_empty()
    }

    /// Return a list of cache_id, and order by their last access time.
    /// The returned list is from old to fresh.
    fn evictable_cache_ids_by_lru(state: &BackendState) -> Vec<String> {
        let mut candidates = Vec::new();
        for slot_ref in state.cache_entries.iter() {
            let Some(entry) = slot_ref.value().as_active() else {
                continue;
            };
            let bytes = entry.total_cached_bytes();
            if bytes > 0 && !Self::entry_is_busy(entry) {
                candidates.push((entry.last_access(), entry.cache_id.clone()));
            }
        }
        candidates.sort_unstable_by_key(|(last_access, _)| *last_access);
        candidates
            .into_iter()
            .map(|(_, cache_id)| cache_id)
            .collect()
    }

    async fn evict_entry(
        state: &BackendState,
        options: &FileCacheBackendOptions,
        cache_id: &str,
        counter: EvictionCounter,
    ) -> u64 {
        let (entry, notify) = {
            let mut slot_ref = match state.cache_entries.get_mut(cache_id) {
                Some(r) => r,
                None => return 0,
            };
            let entry = match slot_ref.value().as_active() {
                Some(e) if !Self::entry_is_busy(e) => e.clone(),
                _ => return 0,
            };
            let notify = Arc::new(Notify::new());
            *slot_ref.value_mut() = CacheSlot::Evicting(notify.clone());
            (entry, notify)
        };

        let released = match entry.evict_all_blocks().await {
            Ok(bytes) => bytes,
            Err(_) => {
                if let Some(mut slot_ref) = state.cache_entries.get_mut(cache_id) {
                    *slot_ref.value_mut() = CacheSlot::Active(entry);
                }
                notify.notify_waiters();
                return 0;
            }
        };
        let _ = tokio::fs::remove_dir_all(&entry.paths.dir).await;
        state.cache_entries.remove(cache_id);
        notify.notify_waiters();

        if released > 0 {
            Self::subtract_current_bytes_for(state, options, released);
            match counter {
                EvictionCounter::Global => {
                    state.evict_global.fetch_add(released, Ordering::Relaxed);
                }
                EvictionCounter::User => {
                    state.evict_user.fetch_add(released, Ordering::Relaxed);
                }
            }
        }

        released
    }

    async fn eviction_inner_for(state: &BackendState, options: &FileCacheBackendOptions) {
        let disk = Self::capture_disk_pressure(options);
        let current_bytes = state.current_bytes.load(Ordering::Relaxed);
        let mut actual_evict =
            Self::pressure_evict_target_for_options(options, current_bytes, disk);

        if actual_evict > 0 {
            let _pressure_guard = state.pressure_lock.lock();
            state.is_full.store(true, Ordering::Relaxed);
            drop(_pressure_guard);

            for cache_id in Self::evictable_cache_ids_by_lru(state) {
                if actual_evict == 0 {
                    break;
                }
                let bytes =
                    Self::evict_entry(state, options, &cache_id, EvictionCounter::Global).await;
                actual_evict = actual_evict.saturating_sub(bytes);
            }
        }

        // Refills can be admitted before this eviction publishes is_full and
        // may update current_bytes while an entry eviction is awaiting I/O.
        // Recompute from the post-eviction state rather than publishing the
        // stale initial target (including when the initial target was zero).
        let final_disk = Self::capture_disk_pressure(options);
        let _pressure_guard = state.pressure_lock.lock();
        Self::publish_pressure_locked(state, options, final_disk);
    }

    pub(crate) async fn eviction_inner(&self) {
        Self::eviction_inner_for(&self.state, &self.options).await;
    }

    // -------------------------------------------------------------------
    // Key transform
    // -------------------------------------------------------------------

    fn transform_store_key(&self, src_name: &str) -> String {
        if let Some(func) = self.fn_trans_func.read().as_ref() {
            if let Some(store_key) = func(src_name) {
                if !store_key.is_empty() {
                    return store_key;
                }
            }
        }
        src_name.to_string()
    }

    // -------------------------------------------------------------------
    // Stats / queries
    // -------------------------------------------------------------------

    pub fn stats(&self) -> CacheStats {
        let mut stats = CacheStats {
            entries: 0,
            bytes_used: self.state.current_bytes.load(Ordering::Relaxed),
            hits: 0,
            misses: 0,
            refills: 0,
        };
        for slot_ref in self.state.cache_entries.iter() {
            let Some(entry) = slot_ref.value().as_active() else {
                continue;
            };
            stats.entries += 1;
            let (h, m, r) = entry.stats_snapshot();
            stats.hits = stats.hits.saturating_add(h);
            stats.misses = stats.misses.saturating_add(m);
            stats.refills = stats.refills.saturating_add(r);
        }
        stats
    }

    pub fn file_stats(&self, key: &str) -> Option<CachedFileStats> {
        let cache_id = cache_key_digest(key);
        let slot_ref = self.state.cache_entries.get(&cache_id)?;
        let entry = slot_ref.value().as_active()?;
        let (h, m, r) = entry.stats_snapshot();
        Some(CachedFileStats {
            key: entry.key.clone(),
            bytes_used: entry.total_cached_bytes(),
            hits: h,
            misses: m,
            refills: r,
        })
    }

    pub fn file_stats_by_src_name(&self, src_name: &str) -> Option<CachedFileStats> {
        let store_key = self.transform_store_key(src_name);
        self.file_stats(&store_key)
    }

    pub(crate) fn contains_store_key(&self, store_key: &str) -> bool {
        let cache_id = cache_key_digest(store_key);
        self.state
            .cache_entries
            .get(&cache_id)
            .is_some_and(|s| s.value().as_active().is_some())
    }

    pub(crate) fn contains_src_name(&self, src_name: &str) -> bool {
        let store_key = self.transform_store_key(src_name);
        self.contains_store_key(&store_key)
    }

    pub(crate) fn cached_size_by_store_key(&self, store_key: &str) -> Option<u64> {
        let cache_id = cache_key_digest(store_key);
        self.state
            .cache_entries
            .get(&cache_id)
            .and_then(|s| s.value().as_active().cloned())
            .map(|e| e.source_size.load(Ordering::Relaxed))
    }

    pub(crate) fn cached_size_by_src_name(&self, src_name: &str) -> Option<u64> {
        let store_key = self.transform_store_key(src_name);
        self.cached_size_by_store_key(&store_key)
    }

    pub fn stat_path(&self, pathname: Option<&str>) -> Result<CachePoolStat> {
        let refill_unit = u32::try_from(self.options.block_size).unwrap_or(u32::MAX);

        let (used_bytes, total_bytes) = match pathname {
            None | Some("/") => (
                self.state.current_bytes.load(Ordering::Relaxed),
                self.options.capacity_bytes,
            ),
            Some(path) => {
                let key = self.transform_store_key(path);
                let cache_id = cache_key_digest(&key);
                let slot_ref = self
                    .state
                    .cache_entries
                    .get(&cache_id)
                    .ok_or_else(|| anyhow!("cache entry not found for path {path}"))?;
                let entry = slot_ref
                    .value()
                    .as_active()
                    .ok_or_else(|| anyhow!("cache entry is being evicted for path {path}"))?;
                (
                    entry.total_cached_bytes(),
                    entry.source_size.load(Ordering::Relaxed),
                )
            }
        };

        let unit = u64::from(refill_unit).max(1);
        let total_units = div_round_up(total_bytes, unit).min(u64::from(u32::MAX)) as u32;
        let used_units = div_round_up(used_bytes, unit).min(u64::from(u32::MAX)) as u32;
        Ok(CachePoolStat {
            refill_unit,
            total_size: total_units,
            used_size: used_units,
            evict_other: 0,
            evict_global: self.state.evict_global.load(Ordering::Relaxed),
            evict_user: self.state.evict_user.load(Ordering::Relaxed),
        })
    }

    pub async fn set_quota(&self, _pathname: &str, _quota: u64) -> Result<()> {
        bail!("set_quota is not supported in file cache backend");
    }

    pub fn list(
        &self,
        dirname: &str,
        list_type: CacheListType,
        marker: Option<&str>,
        count: usize,
    ) -> Result<Vec<String>> {
        let marker = marker.unwrap_or_default();
        let mut keys: Vec<String> = self
            .state
            .cache_entries
            .iter()
            .filter_map(|s| s.value().as_active().map(|e| e.key.clone()))
            .collect();
        keys.sort_unstable();

        let mut out = Vec::new();
        for key in keys {
            if !dirname.is_empty() && dirname != "/" && !key.starts_with(dirname) {
                continue;
            }
            if !marker.is_empty() && key.as_str() <= marker {
                continue;
            }
            let is_dir = key.ends_with('/');
            let keep = match list_type {
                CacheListType::All => true,
                CacheListType::Files => !is_dir,
                CacheListType::Dirs => is_dir,
            };
            if keep {
                out.push(key);
            }
            if count > 0 && out.len() >= count {
                break;
            }
        }
        Ok(out)
    }

    pub async fn reset(&self, _flags: u32) -> Result<()> {
        self.evict_global().await
    }

    pub async fn resize(&self, _n: u64, _flags: u32) -> Result<()> {
        bail!("resize is not supported in file cache backend");
    }

    // -------------------------------------------------------------------
    // Eviction public APIs
    // -------------------------------------------------------------------

    pub async fn evict_by_size(&self, mut size: u64) -> Result<u64> {
        let mut evicted = 0u64;
        for cache_id in Self::evictable_cache_ids_by_lru(&self.state) {
            if size == 0 {
                break;
            }
            let bytes =
                Self::evict_entry(&self.state, &self.options, &cache_id, EvictionCounter::User)
                    .await;
            evicted = evicted.saturating_add(bytes);
            size = size.saturating_sub(bytes);
        }
        Ok(evicted)
    }

    pub async fn evict_store_key(&self, store_key: &str) -> Result<()> {
        let cache_id = cache_key_digest(store_key);
        let bytes = self.force_recycle(&cache_id).await;
        if bytes > 0 {
            self.state.evict_user.fetch_add(bytes, Ordering::Relaxed);
        }
        Ok(())
    }

    pub async fn evict_src_name(&self, src_name: &str) -> Result<()> {
        let store_key = self.transform_store_key(src_name);
        self.evict_store_key(&store_key).await
    }

    /// Evict all cache files that are not owned by an open handle or active task.
    pub async fn evict_global(&self) -> Result<()> {
        let ids: Vec<String> = self
            .state
            .cache_entries
            .iter()
            .filter_map(|s| {
                s.value().as_active()?;
                Some(s.key().clone())
            })
            .collect();
        for cache_id in ids {
            let _ = Self::evict_entry(
                &self.state,
                &self.options,
                &cache_id,
                EvictionCounter::Global,
            )
            .await;
        }
        Ok(())
    }

    pub async fn rename_store_key(&self, old_key: &str, new_key: &str) -> Result<()> {
        if old_key == new_key {
            return Ok(());
        }

        let old_id = cache_key_digest(old_key);
        let new_id = cache_key_digest(new_key);

        self.force_recycle(&new_id).await;

        let old_entry = match self.state.cache_entries.remove(&old_id) {
            Some((_, CacheSlot::Active(entry))) => entry,
            Some((key, slot)) => {
                self.state.cache_entries.insert(key, slot);
                bail!("cache entry not available for key {old_key}");
            }
            None => bail!("cache entry not found for key {old_key}"),
        };

        if old_entry.open_count.load(Ordering::Relaxed) > 0 {
            self.state
                .cache_entries
                .insert(old_id, CacheSlot::Active(old_entry));
            bail!("cache entry is busy (open_count > 0)");
        }

        let source_size = old_entry.source_size.load(Ordering::Relaxed);
        let new_paths = EntryPaths::new(&self.options.cache_dir, &new_id);

        let old_dir = &old_entry.paths.dir;
        if old_dir.try_exists()? {
            let _ = tokio::fs::remove_dir_all(&new_paths.dir).await;
            if let Some(parent) = new_paths.dir.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::rename(old_dir, &new_paths.dir).await?;
        } else {
            tokio::fs::create_dir_all(&new_paths.dir).await?;
        }

        let new_entry = CacheEntry::create(
            new_id.clone(),
            new_key.to_string(),
            source_size,
            &self.options,
            new_paths,
        )?;

        // Move bitmap from old entry.
        {
            let mut old_index = old_entry.index.write();
            let mut new_index = new_entry.index.write();
            *new_index = std::mem::take(&mut *old_index);
        }
        // Copy stats atomically.
        new_entry
            .hits
            .store(old_entry.hits.load(Ordering::Relaxed), Ordering::Relaxed);
        new_entry
            .misses
            .store(old_entry.misses.load(Ordering::Relaxed), Ordering::Relaxed);
        new_entry
            .refills
            .store(old_entry.refills.load(Ordering::Relaxed), Ordering::Relaxed);
        new_entry.last_access_nanos.store(
            old_entry.last_access_nanos.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        new_entry.dirty.store(true, Ordering::Relaxed);
        // NOTE: we do not care about block_states, since we check the open_count is zero

        let _ = new_entry.checkpoint().await;
        self.state
            .cache_entries
            .insert(new_id, CacheSlot::Active(new_entry));
        Ok(())
    }

    pub async fn rename_src_name(&self, old_src: &str, new_src: &str) -> Result<()> {
        let old_key = self.transform_store_key(old_src);
        let new_key = self.transform_store_key(new_src);
        self.rename_store_key(&old_key, &new_key).await
    }

    // -------------------------------------------------------------------
    // Open
    // -------------------------------------------------------------------

    pub async fn open_file(
        &self,
        cache_key: impl Into<String>,
        source: Arc<dyn VirtualFile>,
    ) -> Result<Arc<CachedFile>> {
        self.open_file_with_flags(cache_key, source, 0).await
    }

    pub async fn open_file_with_source_size(
        &self,
        cache_key: impl Into<String>,
        source: Arc<dyn VirtualFile>,
        source_size: u64,
    ) -> Result<Arc<CachedFile>> {
        self.open_file_with_flags_and_size(cache_key, source, 0, Some(source_size))
            .await
    }

    pub async fn open_file_with_flags(
        &self,
        cache_key: impl Into<String>,
        source: Arc<dyn VirtualFile>,
        open_flags: u32,
    ) -> Result<Arc<CachedFile>> {
        self.open_file_with_flags_and_size(cache_key, source, open_flags, None)
            .await
    }

    pub(crate) fn cached_file_backend(&self) -> Self {
        Self {
            options: self.options.clone(),
            state: self.state.clone(),
            fn_trans_func: self.fn_trans_func.clone(),
            active_refills: self.active_refills.clone(),
            bk_scheduler: self.bk_scheduler.clone(),
            _bk_scheduler_owner: None,
        }
    }

    async fn open_file_with_flags_and_size(
        &self,
        cache_key: impl Into<String>,
        source: Arc<dyn VirtualFile>,
        open_flags: u32,
        source_size: Option<u64>,
    ) -> Result<Arc<CachedFile>> {
        let cache_key = cache_key.into();
        if cache_key.is_empty() {
            bail!("cache key cannot be empty");
        }

        let cache_id = cache_key_digest(&cache_key);
        let mut initial_size = self
            .cached_size_by_store_key(&cache_key)
            .or(source_size)
            .unwrap_or(0);

        // Query the source for its size only when we would need to create a
        // new cache entry and the size is unknown. This preserves lazy-open
        // semantics when the entry already exists (the mmap region is already
        // set up for the existing entry).
        if initial_size == 0 && !self.contains_active_entry(&cache_id) {
            initial_size = source.size().await?;
        }

        let entry = self
            .ensure_open_cache_entry(&cache_id, &cache_key, initial_size)
            .await?;
        entry.touch();

        Ok(Arc::new(CachedFile {
            backend: self.cached_file_backend(),
            source: Arc::new(parking_lot::RwLock::new(Some(source))),
            cache_id,
            source_size: AtomicU64::new(initial_size),
            open_flags,
        }))
    }

    pub async fn open_cache_only(
        &self,
        cache_key: impl Into<String>,
        initial_size: u64,
    ) -> Result<Arc<CachedFile>> {
        self.open_cache_only_with_flags(cache_key, initial_size, 0)
            .await
    }

    pub async fn open_cache_only_with_flags(
        &self,
        cache_key: impl Into<String>,
        initial_size: u64,
        open_flags: u32,
    ) -> Result<Arc<CachedFile>> {
        let cache_key = cache_key.into();
        if cache_key.is_empty() {
            bail!("cache key cannot be empty");
        }
        let cache_id = cache_key_digest(&cache_key);
        let entry = self
            .ensure_open_cache_entry(&cache_id, &cache_key, initial_size)
            .await?;
        entry.touch();
        Ok(Arc::new(CachedFile {
            backend: self.cached_file_backend(),
            source: Arc::new(parking_lot::RwLock::new(None)),
            cache_id,
            source_size: AtomicU64::new(initial_size),
            open_flags,
        }))
    }

    pub(crate) async fn open_cache_only_src_name_with_flags(
        &self,
        src_name: impl AsRef<str>,
        initial_size: u64,
        open_flags: u32,
    ) -> Result<Arc<CachedFile>> {
        let store_key = self.transform_store_key(src_name.as_ref());
        self.open_cache_only_with_flags(store_key, initial_size, open_flags)
            .await
    }

    pub(crate) async fn open_src_name_with_flags(
        &self,
        src_name: impl AsRef<str>,
        source: Arc<dyn VirtualFile>,
        open_flags: u32,
    ) -> Result<Arc<CachedFile>> {
        let store_key = self.transform_store_key(src_name.as_ref());
        self.open_file_with_flags(store_key, source, open_flags)
            .await
    }

    pub async fn open(
        &self,
        cache_key: impl Into<String>,
        source: Arc<dyn VirtualFile>,
    ) -> Result<Arc<dyn VirtualFile>> {
        let file = self.open_file_with_flags(cache_key, source, 0).await?;
        Ok(file)
    }

    pub async fn open_with_flags(
        &self,
        cache_key: impl Into<String>,
        source: Arc<dyn VirtualFile>,
        open_flags: u32,
    ) -> Result<Arc<dyn VirtualFile>> {
        let file = self
            .open_file_with_flags(cache_key, source, open_flags)
            .await?;
        Ok(file)
    }

    fn contains_active_entry(&self, cache_id: &str) -> bool {
        self.state
            .cache_entries
            .get(cache_id)
            .is_some_and(|s| s.value().as_active().is_some())
    }

    async fn ensure_open_cache_entry(
        &self,
        cache_id: &str,
        cache_key: &str,
        source_size: u64,
    ) -> Result<Arc<CacheEntry>> {
        loop {
            if let Some(slot_ref) = self.state.cache_entries.get(cache_id) {
                match slot_ref.value() {
                    CacheSlot::Active(entry) => {
                        let entry = entry.clone();
                        let old_size = entry.source_size.load(Ordering::Relaxed);
                        if source_size > old_size {
                            let _ = entry.set_source_size(source_size);
                        }
                        entry.open_count.fetch_add(1, Ordering::SeqCst);
                        return Ok(entry);
                    }
                    CacheSlot::Evicting(notify) => {
                        let notify = notify.clone();
                        let notified = notify.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();
                        drop(slot_ref);
                        notified.await;
                        continue;
                    }
                }
            }

            let paths = EntryPaths::new(&self.options.cache_dir, cache_id);
            let cache_entry = CacheEntry::create(
                cache_id.to_string(),
                cache_key.to_string(),
                source_size,
                &self.options,
                paths,
            )?;
            let slot_ref = self
                .state
                .cache_entries
                .entry(cache_id.to_string())
                .or_insert(CacheSlot::Active(cache_entry));
            match slot_ref.value() {
                CacheSlot::Active(entry) => {
                    entry.open_count.fetch_add(1, Ordering::SeqCst);
                    return Ok(entry.clone());
                }
                CacheSlot::Evicting(notify) => {
                    let notify = notify.clone();
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    drop(slot_ref);
                    notified.await;
                    continue;
                }
            }
        }
    }

    // Skipping the decrement when the slot is Evicting (or absent) is
    // intentional: the entry is being destroyed and its open_count is no
    // longer meaningful.
    pub(crate) fn remove_open_file(&self, cache_id: &str) {
        if let Some(slot_ref) = self.state.cache_entries.get(cache_id) {
            if let Some(entry) = slot_ref.value().as_active() {
                entry.open_count.fetch_sub(1, Ordering::SeqCst);
                entry.touch();
            }
        }
    }

    /// Get the [CacheEntry] for a given cache_id.
    pub(crate) fn get_cache_entry(&self, cache_id: &str) -> Option<Arc<CacheEntry>> {
        self.state
            .cache_entries
            .get(cache_id)
            .and_then(|s| s.value().as_active().cloned())
    }

    pub fn submit_bk_download(
        &self,
        file: Arc<CachedFile>,
        config: crate::config::DownloadConfig,
        device_key: Option<std::path::PathBuf>,
    ) -> Result<()> {
        self.submit_bk_download_batch(vec![(file, config, device_key)])
    }

    /// Register background downloads with the backend's scheduler. Submission
    /// never fails due to execution pressure: tasks wait until the scheduler
    /// has a free file slot. Only a shut-down scheduler or invalid input is
    /// reported.
    pub fn submit_bk_download_batch(
        &self,
        requests: Vec<(
            Arc<CachedFile>,
            crate::config::DownloadConfig,
            Option<std::path::PathBuf>,
        )>,
    ) -> Result<()> {
        let scheduler = self
            .bk_scheduler
            .upgrade()
            .ok_or(super::super::BkDownloadSubmitError::Closed)?;
        scheduler.submit(
            self,
            requests
                .into_iter()
                .map(|(file, config, device_key)| {
                    (
                        file,
                        config,
                        device_key.map(|key| key.to_string_lossy().into_owned()),
                    )
                })
                .collect(),
        )
    }

    /// Stop cache-owned background downloads and await active I/O drain.
    #[allow(dead_code)]
    pub(crate) async fn shutdown_bk_downloads(&self) {
        if let Some(scheduler) = self.bk_scheduler.upgrade() {
            scheduler.shutdown().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn bk_download_registered(&self, cache_id: &str) -> bool {
        self.bk_scheduler
            .upgrade()
            .is_some_and(|scheduler| scheduler.is_registered(cache_id))
    }

    #[cfg(test)]
    pub(crate) fn bk_download_registered_count(&self) -> usize {
        self.bk_scheduler
            .upgrade()
            .map(|scheduler| scheduler.registered_count())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn bk_download_closed(&self) -> bool {
        self.bk_scheduler
            .upgrade()
            .is_some_and(|scheduler| scheduler.is_closed())
    }

    async fn force_recycle(&self, cache_id: &str) -> u64 {
        let (entry, notify) = {
            let mut slot_ref = match self.state.cache_entries.get_mut(cache_id) {
                Some(r) => r,
                None => return 0,
            };
            let entry = match slot_ref.value().as_active() {
                Some(e) if !Self::entry_is_busy(e) => e.clone(),
                _ => return 0,
            };
            let notify = Arc::new(Notify::new());
            *slot_ref.value_mut() = CacheSlot::Evicting(notify.clone());
            (entry, notify)
        };
        let released = match entry.evict_all_blocks().await {
            Ok(bytes) => bytes,
            Err(_) => {
                if let Some(mut slot_ref) = self.state.cache_entries.get_mut(cache_id) {
                    *slot_ref.value_mut() = CacheSlot::Active(entry);
                }
                notify.notify_waiters();
                return 0;
            }
        };
        let _ = tokio::fs::remove_dir_all(&entry.paths.dir).await;
        self.state.cache_entries.remove(cache_id);
        notify.notify_waiters();
        self.subtract_current_bytes(released);
        released
    }
}

impl std::fmt::Debug for FileCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let has_trans_func = self.fn_trans_func.read().is_some();
        f.debug_struct("FileCacheBackend")
            .field("options", &self.options)
            .field("has_trans_func", &has_trans_func)
            .finish_non_exhaustive()
    }
}
