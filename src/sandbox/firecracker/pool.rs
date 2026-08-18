//! Pre-spawned Firecracker process pool.
//!
//! Each warm entry owns a network slot, a running Firecracker process, and the
//! working directory that process uses as CWD. Snapshot resume can consume an
//! entry to skip process spawn and API socket polling on the critical path.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use futures::future::join_all;
use nix::libc;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};
use tracing::{debug, info, warn};
use warm_pool::{PoolMaintenanceAction, WarmPool};

use super::config::create_firecracker_work_dir;
use super::FirecrackerInstance;
use crate::cfg::{ConfigManager, ResolvedFirecrackerPoolConfig};
use crate::sandbox::network::{NetworkManager, Slot};

const POOL_FIRECRACKER_STOP_TIMEOUT: Duration = Duration::from_secs(2);
const POOL_PRIME_POLL_INTERVAL: Duration = Duration::from_millis(20);

static POOL: OnceLock<Option<FirecrackerPool>> = OnceLock::new();

extern "C" fn firecracker_pool_exit_hook() {
    let _ = std::panic::catch_unwind(|| {
        if let Some(Some(pool)) = POOL.get() {
            if let Err(err) = pool.shutdown_for_process_exit() {
                warn!(error = %err, "firecracker pool shutdown on process exit failed");
            }
        }
    });
}

fn register_process_exit_hook(handler: extern "C" fn()) -> i32 {
    // SAFETY: `handler` uses C ABI and `atexit` accepts callbacks with signature `extern "C" fn()`.
    unsafe { libc::atexit(handler) }
}

/// One warm entry handed to a sandbox as an indivisible ownership unit.
pub(crate) struct WarmFirecracker {
    pub slot: Slot,
    pub fc_instance: FirecrackerInstance,
    pub work_dir: TempDir,
}

/// A dead warm entry waiting for network-slot teardown.
enum DeadWarmEntry {
    /// Full entry on the first teardown attempt.
    Full(WarmFirecracker),
    /// Retry after a failed first attempt. The allocation bit was already
    /// released, so retries must redo only the resource teardown and never
    /// touch the allocation bitmap: the index may have been reallocated to
    /// a live sandbox.
    SlotOnly(Slot),
}

impl DeadWarmEntry {
    fn slot_idx(&self) -> u32 {
        match self {
            DeadWarmEntry::Full(warm) => warm.slot.idx,
            DeadWarmEntry::SlotOnly(slot) => slot.idx,
        }
    }

    /// Tear down the entry. The process is known dead, so skip the
    /// graceful-stop path: dropping the instance best-effort kills any
    /// residual handle before the network slot is released. On failure the
    /// entry is returned so the caller can retain it for retry instead of
    /// losing track of stale host network state.
    fn attempt_cleanup(self) -> std::result::Result<(), DeadWarmCleanupError> {
        let slot_idx = self.slot_idx();
        let is_retry = matches!(self, DeadWarmEntry::SlotOnly(_));
        let slot = match self {
            DeadWarmEntry::Full(warm) => {
                let WarmFirecracker {
                    slot,
                    fc_instance,
                    work_dir,
                } = warm;
                drop(fc_instance);
                drop(work_dir);
                slot
            }
            DeadWarmEntry::SlotOnly(slot) => slot,
        };

        let network = NetworkManager::global();
        // The first attempt also releases the allocation bit;
        // `Slot::cleanup` re-arms itself on failure, so retries redo only
        // the teardown and never touch the bitmap again.
        let result = if is_retry {
            network.cleanup_slot_resources(&slot, false)
        } else {
            network.cleanup_allocated_slot(&slot, false)
        };
        result.map_err(|error| DeadWarmCleanupError {
            entry: DeadWarmEntry::SlotOnly(slot),
            error: error.context(format!(
                "firecracker pool: cleanup dead warm network slot {slot_idx}"
            )),
        })
    }
}

/// Error from dead-entry teardown. Keeps the entry so the caller can retain
/// it for retry.
struct DeadWarmCleanupError {
    entry: DeadWarmEntry,
    error: anyhow::Error,
}

/// Dead warm entries queued for teardown, plus the shutdown state the
/// enqueue path checks under the same lock.
#[derive(Default)]
struct DeadWarmQueue {
    /// Set once shutdown starts; late entries must be cleaned up inline by
    /// the caller because no consumer will run again.
    closed: bool,
    entries: Vec<DeadWarmEntry>,
}

pub struct FirecrackerPool {
    pool: WarmPool<WarmFirecracker>,
    /// Warm entries whose firecracker process died while parked, waiting for
    /// network-slot teardown on the maintenance thread. The acquire path runs
    /// in async snapshot-resume context and must not block on netlink/`ip`
    /// teardown, so cleanup is deferred here. The queue lock also serializes
    /// enqueue against shutdown: once `closed` is set, a late enqueue cleans
    /// up inline instead of queueing an entry no consumer will ever see.
    dead_entries: Mutex<DeadWarmQueue>,
    binary: PathBuf,
    socket_timeout: Duration,
    socket_poll_interval: Duration,
    fill_concurrency: usize,
    firecracker_work_base_dir: Option<PathBuf>,
    runtime: Runtime,
}

impl FirecrackerPool {
    /// Returns the global pool when `[pool.firecracker].enabled = true`.
    pub fn global() -> Option<&'static Self> {
        let entry = POOL.get_or_init(|| match Self::try_init() {
            Ok(pool) => pool,
            Err(err) => {
                warn!(
                    error = %err,
                    "firecracker pool init failed; falling back to cold spawn"
                );
                None
            }
        });

        let pool = entry.as_ref()?;
        pool.ensure_worker_started();
        Some(pool)
    }

    fn try_init() -> Result<Option<Self>> {
        let cfg = ConfigManager::global_config();
        let Some(pool_config) = cfg.firecracker_pool_config() else {
            return Ok(None);
        };

        // The pool is a process-wide singleton and can outlive the Tokio
        // runtime that first touched it in tests and benchmarks. Keep a small
        // owned runtime for the synchronous maintenance thread instead of
        // storing `Handle::current()`. A single worker is enough: maintenance
        // only runs occasional I/O-bound `block_on` calls (spawn Firecracker,
        // poll its API socket), while the default multi-thread runtime would
        // park one worker thread per CPU core for its entire lifetime.
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("firecracker-pool-runtime")
            .build()
            .context("firecracker pool: create runtime")?;

        let binary = cfg.resolved_firecracker_binary_path();

        let rc = register_process_exit_hook(firecracker_pool_exit_hook);
        if rc != 0 {
            warn!(
                code = rc,
                "failed to register firecracker pool process-exit hook"
            );
        }

        Ok(Some(Self::new(binary, pool_config, runtime)))
    }

    fn new(binary: PathBuf, pool_config: ResolvedFirecrackerPoolConfig, runtime: Runtime) -> Self {
        let app_config = ConfigManager::global_config();
        let socket_timeout = Duration::from_secs(app_config.firecracker.socket_timeout_secs);
        let socket_poll_interval = Duration::from_millis(app_config.firecracker.socket_poll_ms);
        let firecracker_work_base_dir = app_config.firecracker.work_dir.clone();

        Self {
            pool: WarmPool::new(pool_config.pool),
            dead_entries: Mutex::new(DeadWarmQueue::default()),
            binary,
            socket_timeout,
            socket_poll_interval,
            fill_concurrency: pool_config.fill_concurrency,
            firecracker_work_base_dir,
            runtime,
        }
    }

    fn ensure_worker_started(&'static self) {
        self.pool.start_maintenance_worker(move || {
            if let Err(err) = self.run_maintenance_cycle() {
                warn!(error = %err, "firecracker pool maintenance cycle failed");
            }
        });
    }

    pub(crate) fn try_acquire(&self) -> Option<WarmFirecracker> {
        let warm = self.acquire_live_warm()?;
        if self.pool.len() < self.pool.config().low_watermark {
            self.pool.request_maintenance();
        }
        Some(warm)
    }

    /// Pop warm entries until a live one is found. Parked warm processes run
    /// with `oom_score_adj=1000`, so they are the first OOM-kill candidates and
    /// can die while idle; handing a dead process to snapshot resume would fail
    /// the resume, so dead entries are discarded here instead.
    fn acquire_live_warm(&self) -> Option<WarmFirecracker> {
        loop {
            let mut warm = self.pool.try_acquire()?;
            match warm.fc_instance.is_process_running() {
                Ok(true) => return Some(warm),
                Ok(false) => self.enqueue_dead_warm(warm),
                Err(err) => {
                    // The probe failed, so the process state is unknown: an
                    // I/O error does not prove the child exited. Keep the
                    // entry (and its network slot) and report a pool miss
                    // instead of tearing down a process that may be alive.
                    warn!(
                        slot = warm.slot.idx,
                        error = %err,
                        "firecracker pool: warm process state probe failed; returning entry to pool"
                    );
                    if let Err(warm) = self.pool.release(warm) {
                        // Shutdown reclaimed the entry; queue it for cleanup.
                        self.enqueue_dead_warm(warm);
                    }
                    return None;
                }
            }
        }
    }

    /// Queue a warm entry whose firecracker process already exited for
    /// cleanup by the maintenance worker.
    ///
    /// The acquire path is called from the async snapshot-resume path, so it
    /// must not run network teardown inline: `cleanup_allocated_slot` does
    /// blocking netlink/`ip` work that could stall a runtime worker thread.
    /// The maintenance thread already owns all other pool teardown, so dead
    /// entries are deferred to it and the miss is returned immediately.
    fn enqueue_dead_warm(&self, warm: WarmFirecracker) {
        let slot_idx = warm.slot.idx;
        warn!(
            slot = slot_idx,
            "firecracker pool: warm process exited while parked; queueing entry for cleanup"
        );

        let entry = DeadWarmEntry::Full(warm);
        {
            let mut queue = self.dead_entries.lock().unwrap();
            if queue.closed {
                // Shutdown already drained the queue: no consumer will run
                // again, so clean up inline. This is the shutdown path's own
                // blocking teardown, consistent with the rest of shutdown
                // cleanup.
                drop(queue);
                if let Err(err) = entry.attempt_cleanup() {
                    warn!(
                        slot = slot_idx,
                        error = %err.error,
                        "firecracker pool: cleanup of dead warm entry failed during shutdown"
                    );
                }
                return;
            }
            if self.pool.config().maintenance_enabled {
                queue.entries.push(entry);
                drop(queue);
                self.pool.request_maintenance();
                return;
            }
        }

        // Maintenance is disabled: no worker consumes the queue. Run the
        // blocking teardown on a detached thread so the async acquire path
        // never stalls on netlink/`ip` work. If the attempt fails, dropping
        // the returned entry retries the teardown once more via `Slot::drop`.
        if let Err(err) = std::thread::Builder::new()
            .name("firecracker-pool-dead-cleanup".to_string())
            .spawn(move || {
                if let Err(err) = entry.attempt_cleanup() {
                    warn!(
                        error = %err.error,
                        "firecracker pool: cleanup of dead warm entry failed"
                    );
                }
            })
        {
            warn!(
                error = %err,
                "firecracker pool: failed to spawn dead-entry cleanup thread"
            );
        }
    }

    /// Clean up queued dead entries. Runs on the maintenance thread.
    ///
    /// Entries whose teardown fails are retained in the queue so a later
    /// cycle retries them; losing them would leave stale host network state
    /// behind while the slot index is already back in the allocation bitmap.
    fn cleanup_dead_warm_entries(&self) -> Result<()> {
        let dead = {
            let mut queue = self.dead_entries.lock().unwrap();
            std::mem::take(&mut queue.entries)
        };
        let mut failures = Vec::new();
        let mut failed = Vec::new();
        for entry in dead {
            if let Err(err) = entry.attempt_cleanup() {
                warn!(
                    slot = err.entry.slot_idx(),
                    error = %err.error,
                    "firecracker pool: cleanup of dead warm entry failed; retaining for retry"
                );
                failures.push(err.error.to_string());
                failed.push(err.entry);
            }
        }
        if !failed.is_empty() {
            let mut queue = self.dead_entries.lock().unwrap();
            if queue.closed {
                // Shutdown already closed the queue and no consumer remains;
                // dropping retries the teardown once more via `Slot::drop`.
                warn!(
                    count = failed.len(),
                    "firecracker pool: dropping dead warm entries that failed cleanup during shutdown"
                );
            } else {
                queue.entries.append(&mut failed);
            }
        }
        firecracker_pool_cleanup_result(failures)
    }

    /// Mark the dead-entry queue closed and take its entries. An acquire
    /// that popped its entry just before `drain_all` either enqueues before
    /// this take, or observes `closed` under the same lock and cleans up
    /// inline, so no entry is ever left queued without a consumer.
    fn close_dead_queue(&self) -> Vec<DeadWarmEntry> {
        let mut queue = self.dead_entries.lock().unwrap();
        queue.closed = true;
        std::mem::take(&mut queue.entries)
    }

    pub fn warm_len(&self) -> usize {
        self.pool.len()
    }

    pub async fn shutdown(&self) -> Result<()> {
        let drained = self.pool.drain_all();
        // Close the dead-entry queue right after drain_all so a concurrent
        // acquire that popped its entry just before shutdown either lands in
        // this take, or observes `closed` and cleans up inline.
        let dead = self.close_dead_queue();
        let mut failures = Vec::new();
        for warm in drained {
            if let Err(err) = self.cleanup_warm_async(warm).await {
                failures.push(err.to_string());
            }
        }
        for entry in dead {
            if let Err(err) = entry.attempt_cleanup() {
                warn!(
                    slot = err.entry.slot_idx(),
                    error = %err.error,
                    "firecracker pool: cleanup of dead warm entry failed during shutdown"
                );
                failures.push(err.error.to_string());
            }
        }

        firecracker_pool_cleanup_result(failures)
    }

    fn shutdown_for_process_exit(&self) -> Result<()> {
        self.shutdown_blocking(true)
    }

    fn shutdown_blocking(&self, sync_network_cleanup: bool) -> Result<()> {
        let drained = self.pool.drain_all();
        let dead = self.close_dead_queue();
        let mut failures = Vec::new();
        for warm in drained {
            if let Err(err) = self.cleanup_warm_blocking(warm, sync_network_cleanup) {
                failures.push(err.to_string());
            }
        }
        for entry in dead {
            if let Err(err) = entry.attempt_cleanup() {
                warn!(
                    slot = err.entry.slot_idx(),
                    error = %err.error,
                    "firecracker pool: cleanup of dead warm entry failed during shutdown"
                );
                failures.push(err.error.to_string());
            }
        }

        firecracker_pool_cleanup_result(failures)
    }

    /// Eagerly initialize the pool and wait until `low_watermark` warm entries
    /// exist, or until `timeout` elapses. This is best-effort; timeout is not an
    /// error because the cold path remains available.
    pub async fn prime(timeout: Duration) -> Result<()> {
        let Some(pool) = Self::global() else {
            debug!("firecracker pool disabled; skipping prime");
            return Ok(());
        };

        if !pool.pool.config().maintenance_enabled {
            debug!("firecracker pool maintenance disabled; skipping prime");
            return Ok(());
        }

        if !pool.pool.config().startup_prewarm {
            debug!("firecracker pool startup prewarm disabled; skipping prime");
            return Ok(());
        }

        let target = pool.pool.config().low_watermark;
        if target == 0 || pool.warm_len() >= target {
            return Ok(());
        }

        info!(
            low_watermark = target,
            current = pool.warm_len(),
            timeout_ms = timeout.as_millis(),
            "priming firecracker pool"
        );

        let started = Instant::now();
        loop {
            if pool.warm_len() >= target {
                info!(
                    warm = pool.warm_len(),
                    elapsed_ms = started.elapsed().as_millis(),
                    "firecracker pool primed"
                );
                return Ok(());
            }
            if started.elapsed() >= timeout {
                warn!(
                    warm = pool.warm_len(),
                    target, "firecracker pool prime timed out; continuing with partial warm-up"
                );
                return Ok(());
            }
            tokio::time::sleep(POOL_PRIME_POLL_INTERVAL).await;
        }
    }

    fn run_maintenance_cycle(&self) -> Result<()> {
        // Dead entries deferred from the acquire path are cleaned up here, on
        // the maintenance thread, where blocking network teardown is safe.
        self.cleanup_dead_warm_entries()?;

        match self.pool.compute_maintenance_action(self.pool.len()) {
            PoolMaintenanceAction::Fill(to_fill) => {
                self.runtime.block_on(self.fill_warm_entries(to_fill))?;
            }
            PoolMaintenanceAction::Drain(to_drain) => {
                for _ in 0..to_drain {
                    let Some(warm) = self.pool.try_drain_one() else {
                        break;
                    };
                    self.cleanup_warm_blocking(warm, false)?;
                }
            }
            PoolMaintenanceAction::Idle => {}
        }

        Ok(())
    }

    async fn fill_warm_entries(&self, to_fill: usize) -> Result<()> {
        let mut remaining = to_fill;
        let mut cleanup_failures = Vec::new();
        while remaining > 0 && !self.pool.is_shutting_down() {
            // `remaining` tracks refill attempts left in this maintenance action.
            // On any create failure we finish processing the current batch, then
            // stop launching additional batches to preserve the old serial
            // refill behavior.
            let batch_size = remaining.min(self.fill_concurrency);
            let results = join_all((0..batch_size).map(|_| self.create_warm_async())).await;
            let mut saw_failure = false;

            for result in results {
                match result {
                    Ok(warm) => {
                        if let Err(warm) = self.pool.try_push_bounded(warm) {
                            if let Err(err) = self.cleanup_warm_async(warm).await {
                                warn!(
                                    error = %err,
                                    "firecracker pool: cleanup of unqueued warm entry failed"
                                );
                                cleanup_failures.push(err.to_string());
                            }
                        }
                    }
                    Err(err) => {
                        saw_failure = true;
                        debug!(error = %err, "skipping firecracker pool refill attempt");
                    }
                }
            }

            if saw_failure {
                break;
            }
            remaining -= batch_size;
        }

        firecracker_pool_cleanup_result(cleanup_failures)?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn create_warm_async(&self) -> Result<WarmFirecracker> {
        let slot = NetworkManager::global()
            .allocate_any()
            .context("firecracker pool: allocate network slot")?;

        let work_dir = match create_firecracker_work_dir(self.firecracker_work_base_dir.as_deref())
        {
            Ok(work_dir) => work_dir,
            Err(err) => {
                let _ = NetworkManager::global().release(slot);
                return Err(err).context("firecracker pool: create work dir");
            }
        };

        let mut fc_instance = FirecrackerInstance::new(work_dir.path().to_path_buf());
        let stdout_path = warm_stdout_path(work_dir.path());
        let stderr_path = warm_stderr_path(work_dir.path());
        let spawn_result: Result<()> = async {
            fc_instance
                .spawn_with_netns(
                    &self.binary,
                    Some(&stdout_path),
                    Some(&stderr_path),
                    Some(&slot.namespace_path()),
                )
                .await
                .context("spawn warm firecracker process")?;
            fc_instance
                .wait_for_ready(self.socket_timeout, self.socket_poll_interval)
                .await
                .context("wait for warm firecracker api socket")
        }
        .await;

        if let Err(err) = spawn_result {
            let _ = fc_instance.stop(POOL_FIRECRACKER_STOP_TIMEOUT).await;
            let _ = NetworkManager::global().release(slot);
            return Err(err);
        }

        debug!(
            slot = slot.idx,
            work_dir = %work_dir.path().display(),
            "firecracker pool warm entry ready"
        );
        Ok(WarmFirecracker {
            slot,
            fc_instance,
            work_dir,
        })
    }

    fn cleanup_warm_blocking(
        &self,
        warm: WarmFirecracker,
        sync_network_cleanup: bool,
    ) -> Result<()> {
        let WarmFirecracker {
            slot,
            mut fc_instance,
            work_dir,
        } = warm;
        let slot_idx = slot.idx;

        if sync_network_cleanup {
            // Process-exit hooks can run after Tokio runtime thread-local state
            // has been destroyed. Avoid Runtime::block_on here; Drop kills the
            // child process without entering Tokio.
            drop(fc_instance);
        } else {
            let stop_result = self
                .runtime
                .block_on(fc_instance.stop(POOL_FIRECRACKER_STOP_TIMEOUT));
            if let Err(err) = stop_result {
                warn!(
                    slot = slot_idx,
                    error = %err,
                    "firecracker pool: stop warm process failed"
                );
            }
        }

        drop(work_dir);

        NetworkManager::global()
            .cleanup_allocated_slot(&slot, sync_network_cleanup)
            .context("firecracker pool: cleanup warm network slot")
    }

    async fn cleanup_warm_async(&self, warm: WarmFirecracker) -> Result<()> {
        let WarmFirecracker {
            slot,
            mut fc_instance,
            work_dir,
        } = warm;
        let slot_idx = slot.idx;

        if let Err(err) = fc_instance.stop(POOL_FIRECRACKER_STOP_TIMEOUT).await {
            warn!(
                slot = slot_idx,
                error = %err,
                "firecracker pool: stop warm process failed"
            );
        }

        drop(work_dir);

        NetworkManager::global()
            .cleanup_allocated_slot(&slot, false)
            .context("firecracker pool: cleanup warm network slot")
    }
}

fn firecracker_pool_cleanup_result(failures: Vec<String>) -> Result<()> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "failed to clean up firecracker pool entries: {}",
            failures.join(" | ")
        ))
    }
}

pub(crate) fn warm_stdout_path(work_dir: &Path) -> PathBuf {
    work_dir.join("firecracker-stdout.log")
}

pub(crate) fn warm_stderr_path(work_dir: &Path) -> PathBuf {
    work_dir.join("firecracker-stderr.log")
}
