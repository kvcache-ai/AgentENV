//! Cache-level background download scheduler.
//!
//! One scheduler per `FileCacheBackend`, shared by every image on the node.
//! Downloads fetch from the source in `download.blockSize` chunks (default
//! 16 MiB, the historical large-request granularity) and publish each chunk's
//! cache blocks into the shared full-file cache as soon as they land, so
//! foreground reads observe background-downloaded blocks through the same
//! bitmap and per-block loader election that foreground refills use — while
//! the cache keeps its own, smaller block size for foreground granularity.
//!
//! Submission never fails due to capacity: tasks are registered (deduplicated
//! by `cache_id`) and only *execution* is bounded — at most
//! `max_concurrent_files` layer tasks run at once, and all chunk fetches
//! share the `block_slots` semaphore (`max_inflight_blocks` concurrent
//! chunks, bounding scratch memory to `max_inflight_blocks` × the download
//! chunk size). Pending tasks hold no cache-entry reference, so entries stay
//! evictable until a task actually starts running; a task re-opens its
//! cached file when it runs.

use crate::config::DownloadConfig;
use crate::io::virtual_file::VirtualFile;
use anyhow::{anyhow, bail, Result};
use dashmap::DashMap;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use super::full_file_cache::cache_pool::FileCacheBackend;
use super::full_file_cache::cache_store::CachedFile;

const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Default per-block read timeout: a straggler registry connection must not
/// hold a block slot forever. Retried up to `HEDGE_MAX_ATTEMPTS` times per
/// block; a retry reissues the read request (connection reuse is up to the
/// source's HTTP client pool).
pub(crate) const DEFAULT_HEDGE_TIMEOUT: Duration = Duration::from_secs(6);
const HEDGE_MAX_ATTEMPTS: u32 = 3;

/// Submission failures that must not block image open/resume: the background
/// download scheduler is a best-effort accelerator and foreground
/// `CachedFile` reads refill missing blocks from the origin on demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum BkDownloadSubmitError {
    #[error("background download scheduler is shut down")]
    Closed,
}

impl BkDownloadSubmitError {
    /// Fixed log category; never carries source URLs or credentials.
    pub(crate) fn category(self) -> &'static str {
        match self {
            Self::Closed => "scheduler_closed",
        }
    }
}

/// One registered download of a remote blob into the cache. Holds no
/// `CacheEntry`/`CachedFile` reference while pending, so the entry remains
/// evictable until the task is running; the run phase re-opens the cached
/// file through `backend`.
struct BkDownloadTask {
    cache_id: String,
    cache_key: String,
    source: Arc<dyn VirtualFile>,
    source_size: u64,
    config: DownloadConfig,
    /// Background fetch granularity in cache blocks (from
    /// `config.block_size` aligned down): one source request covers this
    /// many cache blocks. The cache block size itself stays untouched, so
    /// foreground reads keep their fine-grained on-demand granularity while
    /// background downloads keep large-request throughput.
    blocks_per_chunk: u32,
    device_key: Option<String>,
    backend: FileCacheBackend,
}

struct BkThrottle {
    started: tokio::time::Instant,
    downloaded: u64,
    limit_bps: u64,
}

/// Ready tasks grouped by device key and drained round-robin, so one image
/// with many layers cannot starve later images of download turns.
struct FairReadyQueue<T> {
    groups: VecDeque<(Option<String>, VecDeque<T>)>,
}

impl<T> Default for FairReadyQueue<T> {
    fn default() -> Self {
        Self {
            groups: VecDeque::new(),
        }
    }
}

impl<T> FairReadyQueue<T> {
    fn push(&mut self, device_key: Option<String>, item: T) {
        if let Some((_, queue)) = self.groups.iter_mut().find(|(key, _)| *key == device_key) {
            queue.push_back(item);
        } else {
            self.groups.push_back((device_key, VecDeque::from([item])));
        }
    }

    fn pop(&mut self) -> Option<T> {
        let (device_key, mut queue) = self.groups.pop_front()?;
        let item = queue.pop_front()?;
        if !queue.is_empty() {
            self.groups.push_back((device_key, queue));
        }
        Some(item)
    }
}

pub(crate) struct BkDownloadScheduler {
    /// Registered tasks by cache_id; also the dedup map. A finished task only
    /// removes its own entry (pointer identity), never a newer registration.
    tasks: DashMap<String, Arc<BkDownloadTask>>,
    ready: StdMutex<FairReadyQueue<Arc<BkDownloadTask>>>,
    ready_notify: tokio::sync::Notify,
    /// Set once submission closes; checked together with `tasks` insertion so
    /// a submit racing shutdown either registers before the drain or fails.
    closed: StdMutex<bool>,
    running: Arc<AtomicBool>,
    shutdown_notify: tokio::sync::Notify,
    /// Live futures owned by the scheduler (dispatcher + timers + runs);
    /// shutdown waits for this to reach zero.
    active: AtomicUsize,
    idle_notify: tokio::sync::Notify,
    /// Cap on concurrently running layer tasks.
    file_slots: Arc<Semaphore>,
    /// Cap on in-flight download chunks across all tasks; bounds total
    /// scratch memory to `max_inflight_blocks` × the per-task download chunk
    /// size (`download.blockSize`).
    block_slots: Arc<Semaphore>,
    max_inflight_blocks: usize,
    /// Global `download.blockSize` at scheduler creation. Per-image chunk
    /// overrides larger than this are clamped to it, so the scratch budget
    /// `max_inflight_blocks × block_size` holds for every task.
    chunk_size: u32,
    hedge_timeout: Duration,
    override_mismatch_warned: AtomicBool,
    chunk_clamp_warned: AtomicBool,
}

impl BkDownloadScheduler {
    /// Create a scheduler that owns its file-task and block-I/O caps.
    ///
    /// Both caps come from the *global* download config when the cache
    /// backend is created. Per-image `maxInflightBlocks` overrides keep
    /// serialization compatibility but never resize the block cap; the first
    /// mismatch per scheduler is logged under the fixed category
    /// `max_inflight_blocks_override_ignored`.
    pub(crate) fn new(
        max_inflight_blocks: usize,
        max_concurrent_files: usize,
        chunk_size: u32,
        hedge_timeout: Duration,
    ) -> Arc<Self> {
        let scheduler = Arc::new(Self {
            tasks: DashMap::new(),
            ready: StdMutex::new(FairReadyQueue::default()),
            ready_notify: tokio::sync::Notify::new(),
            closed: StdMutex::new(false),
            running: Arc::new(AtomicBool::new(true)),
            shutdown_notify: tokio::sync::Notify::new(),
            active: AtomicUsize::new(0),
            idle_notify: tokio::sync::Notify::new(),
            file_slots: Arc::new(Semaphore::new(max_concurrent_files.max(1))),
            block_slots: Arc::new(Semaphore::new(max_inflight_blocks.max(1))),
            max_inflight_blocks: max_inflight_blocks.max(1),
            chunk_size,
            hedge_timeout,
            override_mismatch_warned: AtomicBool::new(false),
            chunk_clamp_warned: AtomicBool::new(false),
        });
        scheduler.active.fetch_add(1, Ordering::AcqRel);
        tokio::spawn(dispatch_loop(scheduler.clone()));
        scheduler
    }

    /// Register background downloads. Never fails due to execution pressure:
    /// tasks wait in the registry until a file slot is free. Only a shut-down
    /// scheduler or invalid input (missing source, unreadable cache state) is
    /// reported.
    pub(crate) fn submit(
        self: &Arc<Self>,
        backend: &FileCacheBackend,
        requests: Vec<(Arc<CachedFile>, DownloadConfig, Option<String>)>,
    ) -> Result<()> {
        let mut seen = HashSet::with_capacity(requests.len());
        for (file, mut config, device_key) in requests {
            let cache_id = file.cache_id().to_string();
            if !seen.insert(cache_id.clone()) {
                continue;
            }
            let (source, source_size) = match file.background_source_snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn_submit_skipped(&cache_id, &error);
                    continue;
                }
            };
            config.concurrency = config.concurrency.max(1);
            config.try_cnt = config.try_cnt.max(1);
            // The scheduler-owned cap always wins; a per-image override that
            // disagrees is ignored after one fixed-category warning.
            if config.max_inflight_blocks != self.max_inflight_blocks {
                self.warn_override_mismatch_once(config.max_inflight_blocks);
            }
            match file.background_is_complete(source_size) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    warn_submit_skipped(&cache_id, &error);
                    continue;
                }
            }
            let Some(cache_key) = backend
                .get_cache_entry(&cache_id)
                .map(|entry| entry.key.clone())
            else {
                tracing::warn!(
                    error_category = "cache_entry_missing",
                    cache_id,
                    "skipping background download submission"
                );
                continue;
            };
            // `download.blockSize` is the background fetch granularity; the
            // cache stores blocks in its own (smaller) size. Align down to
            // whole cache blocks, at least one, and clamp to the scheduler's
            // global chunk size so the scratch budget holds for overrides.
            let cache_block_size = file.background_block_size().max(1);
            let max_chunk_blocks = u64::from(self.chunk_size) / cache_block_size;
            let mut blocks_per_chunk = u64::from(config.block_size) / cache_block_size;
            if blocks_per_chunk > max_chunk_blocks.max(1) {
                self.warn_chunk_clamp_once(u64::from(config.block_size));
                blocks_per_chunk = max_chunk_blocks;
            }
            let blocks_per_chunk = u32::try_from(blocks_per_chunk).unwrap_or(u32::MAX).max(1);
            let task = Arc::new(BkDownloadTask {
                cache_id: cache_id.clone(),
                cache_key,
                source,
                source_size,
                config,
                blocks_per_chunk,
                device_key,
                backend: backend.cached_file_backend(),
            });

            // Closed check, registry insertion, and the active-count
            // increment must be atomic against shutdown: once `closed` is
            // set, no new task may slip past the drain.
            let closed = self.closed.lock().unwrap();
            if *closed {
                return Err(BkDownloadSubmitError::Closed.into());
            }
            let dashmap::mapref::entry::Entry::Vacant(slot) = self.tasks.entry(cache_id.clone())
            else {
                continue;
            };
            slot.insert(task.clone());
            self.active.fetch_add(1, Ordering::AcqRel);
            let scheduler = self.clone();
            tokio::spawn(async move {
                scheduler.wait_then_mark_ready(task).await;
            });
            drop(closed);
        }
        Ok(())
    }

    /// Timer phase: wait for sandbox readiness and the configured delay
    /// without holding any execution resource, then move the task to the
    /// ready queue. On shutdown the task never became ready, so its
    /// registration is removed here.
    async fn wait_then_mark_ready(self: &Arc<Self>, task: Arc<BkDownloadTask>) {
        if let Some(key) = task.device_key.as_deref() {
            crate::download_gate::wait_sandbox_ready(
                key,
                crate::download_gate::SANDBOX_READY_FALLBACK,
                &self.running,
            )
            .await;
        }
        interruptible_sleep(background_download_delay(&task.config), &self.running).await;
        if self.running.load(Ordering::Acquire) {
            let mut ready = self.ready.lock().unwrap();
            if self.running.load(Ordering::Acquire) {
                ready.push(task.device_key.clone(), task);
                drop(ready);
                self.ready_notify.notify_one();
                self.active.fetch_sub(1, Ordering::AcqRel);
                self.idle_notify.notify_waiters();
                return;
            }
        }
        remove_task(&self.tasks, &task);
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.idle_notify.notify_waiters();
    }

    /// Per-image `maxInflightBlocks` overrides never resize the
    /// scheduler-owned cap; warn once per scheduler under a fixed category
    /// (numbers only, never source URLs or credentials). Returns true when
    /// this call logged.
    fn warn_override_mismatch_once(&self, requested: usize) -> bool {
        if self.override_mismatch_warned.swap(true, Ordering::AcqRel) {
            return false;
        }
        tracing::warn!(
            error_category = "max_inflight_blocks_override_ignored",
            requested_max_inflight_blocks = requested,
            scheduler_max_inflight_blocks = self.max_inflight_blocks,
            "ignoring per-image maxInflightBlocks override; the background download block cap is fixed at backend creation"
        );
        true
    }

    /// A per-image chunk override larger than the global `blockSize` is
    /// clamped to it so the scheduler's scratch budget holds; warn once per
    /// scheduler under a fixed category (numbers only).
    fn warn_chunk_clamp_once(&self, requested: u64) {
        if self.chunk_clamp_warned.swap(true, Ordering::AcqRel) {
            return;
        }
        tracing::warn!(
            error_category = "block_size_override_clamped",
            requested_block_size = requested,
            scheduler_block_size = self.chunk_size,
            "clamping per-image blockSize override to the scheduler's global chunk size"
        );
    }

    /// Stop accepting submissions and wait for every scheduler-owned future
    /// to leave the running state. Every caller performs the same idempotent
    /// wait, so cancellation of one shutdown future cannot strand later
    /// callers behind an abandoned leader.
    pub(crate) async fn shutdown(&self) {
        self.stop();
        while self.active.load(Ordering::Acquire) != 0 {
            let notified = self.idle_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
        self.ready.lock().unwrap().groups.clear();
        self.tasks.clear();
    }

    /// Atomically close submission and signal every scheduled future to wind
    /// down. Closing both semaphores is persistent (unlike a Notify pulse), so
    /// a dispatcher or block task already waiting for a permit is guaranteed
    /// to wake. Called by `shutdown()` and by the primary backend's Drop.
    pub(crate) fn stop(&self) {
        let mut closed = self.closed.lock().unwrap();
        *closed = true;
        self.running.store(false, Ordering::Release);
        self.file_slots.close();
        self.block_slots.close();
        self.ready_notify.notify_waiters();
        self.shutdown_notify.notify_waiters();
    }

    #[cfg(test)]
    pub(crate) fn is_registered(&self, cache_id: &str) -> bool {
        self.tasks.contains_key(cache_id)
    }

    #[cfg(test)]
    pub(crate) fn registered_count(&self) -> usize {
        self.tasks.len()
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap()
    }
}

impl Drop for BkDownloadScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Remove `task` from the registry only if it is still the registered entry:
/// a later task for the same cache_id is a different `Arc` and must survive.
fn remove_task(tasks: &DashMap<String, Arc<BkDownloadTask>>, task: &Arc<BkDownloadTask>) {
    tasks.remove_if(&task.cache_id, |_, existing| Arc::ptr_eq(existing, task));
}

/// Log a skipped submission with only the fixed error category and the
/// content-addressed cache id — never the source URL or credentials.
fn warn_submit_skipped(cache_id: &str, error: &anyhow::Error) {
    tracing::warn!(
        error_category = download_error_category(error),
        cache_id,
        "skipping background download submission"
    );
}

/// RAII guard for one dispatched run: deregisters the task and balances the
/// active count even if the run future is aborted during shutdown drain
/// (dropping a JoinSet aborts live futures, skipping any trailing code).
struct RunGuard {
    scheduler: Arc<BkDownloadScheduler>,
    task: Arc<BkDownloadTask>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        remove_task(&self.scheduler.tasks, &self.task);
        self.scheduler.active.fetch_sub(1, Ordering::AcqRel);
        self.scheduler.idle_notify.notify_waiters();
    }
}

async fn dispatch_loop(scheduler: Arc<BkDownloadScheduler>) {
    let mut runs = JoinSet::new();
    loop {
        // Reap finished run futures so panics are logged and the set stays
        // bounded to live tasks.
        while let Some(result) = runs.try_join_next() {
            if let Err(error) = result {
                tracing::warn!(
                    error_category = download_error_category(&error.into()),
                    "background cache download task panicked"
                );
            }
        }
        if !scheduler.running.load(Ordering::Acquire) {
            break;
        }
        let task = {
            let notified = scheduler.ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let task = scheduler.ready.lock().unwrap().pop();
            match task {
                Some(task) => task,
                None => {
                    notified.await;
                    continue;
                }
            }
        };
        let permit = tokio::select! {
            permit = scheduler.file_slots.clone().acquire_owned() => {
                match permit {
                    Ok(permit) => permit,
                    Err(_) => break,
                }
            }
            _ = scheduler.shutdown_notify.notified() => break,
        };
        if !scheduler.running.load(Ordering::Acquire) {
            // Shutdown clears the registry itself; nothing to clean up.
            break;
        }
        scheduler.active.fetch_add(1, Ordering::AcqRel);
        // Construct the guard before spawn: dropping an unpolled future must
        // still balance `active` and remove this registration.
        let guard = RunGuard {
            scheduler: scheduler.clone(),
            task,
        };
        runs.spawn(async move {
            let _permit = permit;
            run_task(&guard.task, &guard.scheduler).await;
        });
    }
    // Dropping the JoinSet aborts any live run futures; cancellation lands at
    // a block boundary (an await point), where refill guards unwind safely.
    scheduler.active.fetch_sub(1, Ordering::AcqRel);
    scheduler.idle_notify.notify_waiters();
}

async fn run_task(task: &BkDownloadTask, scheduler: &BkDownloadScheduler) {
    let throttle = (task.config.max_mbps > 0).then(|| {
        Arc::new(Mutex::new(BkThrottle {
            started: tokio::time::Instant::now(),
            downloaded: 0,
            limit_bps: task.config.max_mbps as u64 * 1024 * 1024,
        }))
    });
    let attempts = task.config.try_cnt as u32;
    for attempt in 0..attempts {
        if !scheduler.running.load(Ordering::Acquire) {
            return;
        }
        // Re-resolve the cached file every attempt: the entry may have been
        // evicted while the task was pending, or after an ENOSPC failure.
        let file = match task
            .backend
            .open_file_with_source_size(
                task.cache_key.clone(),
                task.source.clone(),
                task.source_size,
            )
            .await
        {
            Ok(file) => file,
            Err(error) => {
                if attempt + 1 < attempts {
                    interruptible_sleep(
                        retry_backoff(attempt + 1, &task.cache_id),
                        &scheduler.running,
                    )
                    .await;
                    continue;
                }
                tracing::warn!(
                    error_category = download_error_category(&error),
                    "background cache download could not open cached file"
                );
                return;
            }
        };
        let chunks = match file.missing_background_chunks(task.source_size, task.blocks_per_chunk) {
            Ok(chunks) => chunks,
            Err(error) => {
                tracing::warn!(
                    error_category = download_error_category(&error),
                    "invalid background cache block range"
                );
                return;
            }
        };
        if chunks.is_empty() {
            return;
        }

        let first_error = download_chunks(task, &file, chunks, scheduler, throttle.as_ref()).await;
        if !scheduler.running.load(Ordering::Acquire) {
            return;
        }
        let Some(error) = first_error else {
            return;
        };
        let error_category = download_error_category(&error);
        if attempt + 1 < attempts {
            tracing::warn!(
                attempt = attempt + 1,
                error_category,
                "background cache download will retry missing blocks"
            );
            interruptible_sleep(
                retry_backoff(attempt + 1, &task.cache_id),
                &scheduler.running,
            )
            .await;
        } else {
            tracing::warn!(
                error_category,
                "background cache download terminally failed"
            );
        }
    }
}

/// Download `chunks` of one layer with per-task concurrency, returning the
/// first error seen (the task-level retry loop re-enumerates missing chunks
/// afterwards, so partial progress is kept). Each chunk future owns one
/// gate admission and one block slot for its whole fetch.
async fn download_chunks(
    task: &BkDownloadTask,
    file: &Arc<CachedFile>,
    chunks: Vec<(u64, u32)>,
    scheduler: &BkDownloadScheduler,
    throttle: Option<&Arc<Mutex<BkThrottle>>>,
) -> Option<anyhow::Error> {
    let mut pending = chunks.into_iter();
    let mut in_flight = JoinSet::new();
    let mut first_error = None;
    loop {
        while first_error.is_none()
            && scheduler.running.load(Ordering::Acquire)
            && in_flight.len() < task.config.concurrency
        {
            let Some((start_block, len_blocks)) = pending.next() else {
                break;
            };
            let file = file.clone();
            let source = task.source.clone();
            let source_size = task.source_size;
            let running = scheduler.running.clone();
            let block_slots = scheduler.block_slots.clone();
            let hedge_timeout = scheduler.hedge_timeout;
            let throttle = throttle.cloned();
            in_flight.spawn(async move {
                let read = refill_chunk_with_hedge(
                    &file,
                    &source,
                    source_size,
                    (start_block, len_blocks),
                    &running,
                    &block_slots,
                    hedge_timeout,
                )
                .await?;
                throttle_after_block(throttle.as_ref(), read, &running).await;
                Ok::<(), anyhow::Error>(())
            });
        }

        match in_flight.join_next().await {
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(error))) => {
                first_error.get_or_insert(error);
            }
            Some(Err(error)) => {
                first_error.get_or_insert(error.into());
            }
            None => break,
        }
    }
    first_error
}

/// One chunk refill: gate admission, then a block slot, then the actual read
/// with a per-attempt timeout. A timeout drops the slow read (releasing the
/// gate permit and block slot through RAII) and reissues the request on the
/// next attempt. Returns the source bytes read (for throttling).
async fn refill_chunk_with_hedge(
    file: &Arc<CachedFile>,
    source: &Arc<dyn VirtualFile>,
    source_size: u64,
    chunk: (u64, u32),
    running: &AtomicBool,
    block_slots: &Semaphore,
    hedge_timeout: Duration,
) -> Result<u64> {
    let (start_block, len_blocks) = chunk;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let result = {
            // Take the bounded I/O slot first, then perform the final
            // foreground-pressure admission. A task waiting for a slot must
            // not pre-admit itself and bypass foreground pressure later.
            let _slot = block_slots
                .acquire()
                .await
                .map_err(|_| anyhow!("background cache block slots closed"))?;
            let Some(_gate) = crate::download_gate::gate_block_read(running).await else {
                bail!("background cache download canceled before chunk dispatch");
            };
            tokio::time::timeout(
                hedge_timeout,
                file.background_refill_range(source, source_size, start_block, len_blocks),
            )
            .await
            // Gate permit and block slot are released here, before any hedge
            // backoff sleep.
        };
        match result {
            Ok(result) => return result,
            Err(_) if attempt < HEDGE_MAX_ATTEMPTS && running.load(Ordering::Acquire) => {
                tracing::warn!(
                    start_block,
                    attempt,
                    "background cache chunk read timed out; reissuing request"
                );
                interruptible_sleep(
                    Duration::from_millis(500 * u64::from(attempt) + start_block % 500),
                    running,
                )
                .await;
            }
            Err(elapsed) => return Err(elapsed.into()),
        }
    }
}

fn download_error_category(error: &anyhow::Error) -> &'static str {
    for cause in error.chain() {
        if cause.is::<tokio::task::JoinError>() {
            return "worker_join_error";
        }
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            return match io_error.kind() {
                std::io::ErrorKind::NotFound => "io_not_found",
                std::io::ErrorKind::PermissionDenied => "io_permission_denied",
                _ => "io_error",
            };
        }
        if cause.to_string().eq_ignore_ascii_case("incomplete") {
            return "incomplete";
        }
    }
    "download_error"
}

fn background_download_delay(config: &DownloadConfig) -> Duration {
    let base = config.delay.max(0) as u64;
    let extra = config.delay_extra.max(1) as u64;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    Duration::from_secs(base.saturating_add(nanos % extra))
}

fn retry_backoff(attempt: u32, seed: &str) -> Duration {
    let base = 1u64
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(u64::MAX)
        .min(30);
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in seed.as_bytes().iter().chain(attempt.to_le_bytes().iter()) {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    Duration::from_secs(base) + Duration::from_millis(hash % 1000)
}

async fn throttle_after_block(
    throttle: Option<&Arc<Mutex<BkThrottle>>>,
    bytes: u64,
    running: &AtomicBool,
) {
    let Some(throttle) = throttle else {
        return;
    };
    let sleep_for = {
        let mut throttle = throttle.lock().await;
        throttle.downloaded = throttle.downloaded.saturating_add(bytes);
        let expected = throttle.downloaded as f64 / throttle.limit_bps as f64;
        let overdue = expected - throttle.started.elapsed().as_secs_f64();
        if overdue > 0.0 {
            Duration::from_secs_f64(overdue)
        } else {
            Duration::ZERO
        }
    };
    if !sleep_for.is_zero() {
        interruptible_sleep(sleep_for, running).await;
    }
}

async fn interruptible_sleep(duration: Duration, running: &AtomicBool) {
    let deadline = tokio::time::Instant::now() + duration;
    loop {
        let now = tokio::time::Instant::now();
        if !running.load(Ordering::Acquire) || now >= deadline {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL.min(deadline - now)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fair_ready_queue_rotates_device_groups() {
        let mut queue = FairReadyQueue::default();
        queue.push(Some("a".to_string()), 1);
        queue.push(Some("a".to_string()), 2);
        queue.push(Some("b".to_string()), 3);
        queue.push(None, 4);
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), Some(4));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), None);
    }
}
