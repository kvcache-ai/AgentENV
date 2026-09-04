//! Generic warm resource pool with watermark-based maintenance.
//!
//! This crate provides reusable pool mechanics for resources that are expensive
//! to create but can be reset and reused. It handles:
//! - Watermark-based refill/drain decisions
//! - Background maintenance worker with condvar signaling
//! - Shutdown coordination with safe resource cleanup
//! - Process exit hooks for static singleton pools
//!
//! Resource-specific create/reset/delete logic is provided via trait hooks.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};

/// Action computed by watermark logic for the maintenance worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolMaintenanceAction {
    /// Refill the pool by creating N new resources.
    Fill(usize),
    /// Drain N excess resources from the pool.
    Drain(usize),
    /// Pool is within watermarks; no action needed.
    Idle,
}

/// Signal state for the maintenance worker condvar.
#[derive(Debug, Default)]
struct PoolMaintenanceSignal {
    /// Work is pending (refill or drain needed).
    pending: bool,
    /// Worker should exit.
    stop: bool,
}

/// Configuration for a warm pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Target lower bound for idle resource count.
    pub low_watermark: usize,
    /// Upper bound for idle resource count.
    ///
    /// Maintenance starts by refilling toward `low_watermark`, then grows the
    /// refill target geometrically toward this bound when acquisitions drain
    /// the pool below the low watermark. It is only a strict insertion cap when
    /// maintenance is disabled.
    pub high_watermark: usize,
    /// Enable background maintenance worker.
    pub maintenance_enabled: bool,
    /// Advisory flag for callers that can prewarm once a reusable resource
    /// shape is known. `WarmPool` itself only owns generic pool mechanics.
    pub startup_prewarm: bool,
}

impl PoolConfig {
    /// Validate and normalize config values.
    pub fn validate(mut self) -> Self {
        if self.low_watermark > self.high_watermark {
            tracing::warn!(
                low = self.low_watermark,
                high = self.high_watermark,
                "low_watermark > high_watermark; clamping low to high"
            );
            self.low_watermark = self.high_watermark;
        }
        self
    }
}

/// Generic warm pool for reusable resources.
///
/// `T` is the pooled resource type. Resource-specific create/reset/delete
/// logic is provided via closures or trait objects passed to pool methods.
///
/// `T` must be `Send` because resources may be created/destroyed on the
/// maintenance worker thread.
///
/// The built-in maintenance worker requires `start_maintenance_worker` to be
/// called on a `&'static WarmPool<T>` because the worker thread owns the
/// callback for the rest of the pool lifetime.
///
/// With maintenance enabled, `high_watermark` is a drain target rather than a
/// strict insertion cap. Release paths may temporarily exceed it, and the
/// maintenance worker is expected to drain excess resources.
pub struct WarmPool<T: Send> {
    /// Idle resources ready for reuse.
    pool: Mutex<VecDeque<T>>,
    /// Watermark config.
    config: PoolConfig,
    /// Current refill target. Starts at the low watermark and grows toward the
    /// high watermark under acquisition pressure. This intentionally ratchets
    /// upward for the process lifetime: after a node observes bursty demand, it
    /// keeps extra warm capacity instead of shrinking back to cold-start
    /// behavior.
    fill_target: Mutex<usize>,
    /// Background maintenance worker state.
    maintenance_signal: Mutex<PoolMaintenanceSignal>,
    /// Wakes the maintenance worker.
    maintenance_cv: Condvar,
    /// Maintenance worker thread handle.
    maintenance_worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Ensures the maintenance worker only starts once.
    maintenance_started: AtomicBool,
    /// Rejects new allocations once shutdown cleanup starts.
    shutting_down: AtomicBool,
}

impl<T: Send> WarmPool<T> {
    /// Create a new warm pool with the given config.
    pub fn new(config: PoolConfig) -> Self {
        let config = config.validate();
        let fill_target = config.low_watermark.min(config.high_watermark);
        Self {
            pool: Mutex::new(VecDeque::new()),
            fill_target: Mutex::new(fill_target),
            config,
            maintenance_signal: Mutex::new(PoolMaintenanceSignal::default()),
            maintenance_cv: Condvar::new(),
            maintenance_worker: Mutex::new(None),
            maintenance_started: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Check if the pool is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Return the validated pool configuration.
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Return the current number of idle resources.
    pub fn len(&self) -> usize {
        self.pool.lock().unwrap().len()
    }

    /// Return whether the pool currently has no idle resources.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Compute the maintenance action based on current pool size.
    pub fn compute_maintenance_action(&self, pool_len: usize) -> PoolMaintenanceAction {
        let fill_target = self.current_fill_target();
        if pool_len < fill_target {
            let to_fill = fill_target.saturating_sub(pool_len);
            if to_fill > 0 {
                return PoolMaintenanceAction::Fill(to_fill);
            }
        }
        if pool_len > self.config.high_watermark {
            // Drain the full excess in one maintenance cycle. Resource-specific
            // cleanup happens outside the pool lock, and shutdown paths already
            // have to tolerate draining the whole pool.
            let to_drain = pool_len - self.config.high_watermark;
            if to_drain > 0 {
                return PoolMaintenanceAction::Drain(to_drain);
            }
        }
        PoolMaintenanceAction::Idle
    }

    fn current_fill_target(&self) -> usize {
        (*self.fill_target.lock().unwrap()).min(self.config.high_watermark)
    }

    fn grow_fill_target_after_pressure(&self, pool_len: usize) {
        if pool_len >= self.config.low_watermark || self.config.high_watermark == 0 {
            return;
        }

        let low = self.config.low_watermark.min(self.config.high_watermark);
        let mut target = self.fill_target.lock().unwrap();
        let next = (*target)
            .max(low)
            .max(1)
            .saturating_mul(2)
            .min(self.config.high_watermark);
        *target = next;
    }

    fn record_acquisition_pressure(&self, pool_len: usize) {
        self.grow_fill_target_after_pressure(pool_len);
        if matches!(
            self.compute_maintenance_action(pool_len),
            PoolMaintenanceAction::Fill(_)
        ) {
            self.request_maintenance();
        }
    }

    /// Request the maintenance worker to wake up and check watermarks.
    pub fn request_maintenance(&self) {
        if !self.config.maintenance_enabled || self.is_shutting_down() {
            return;
        }

        let mut signal = self.maintenance_signal.lock().unwrap();
        if signal.stop {
            return;
        }
        signal.pending = true;
        self.maintenance_cv.notify_one();
    }

    /// Try to acquire a resource from the pool (fast path).
    ///
    /// Returns `Some(resource)` if one is available, `None` if the pool is empty.
    /// Acquisition pressure grows the refill target and wakes maintenance even
    /// on a miss, allowing a burst that starts from an empty pool to influence
    /// future warm capacity.
    pub fn try_acquire(&self) -> Option<T> {
        if self.is_shutting_down() {
            return None;
        }
        let mut pool = self.pool.lock().unwrap();
        let resource = pool.pop_front();
        let next_pool_len = pool.len();
        drop(pool);
        self.record_acquisition_pressure(next_pool_len);
        resource
    }

    /// Try to acquire the first resource matching `predicate`.
    ///
    /// This is useful when only some idle resources are reusable for a request.
    pub fn try_acquire_where(&self, mut predicate: impl FnMut(&T) -> bool) -> Option<T> {
        if self.is_shutting_down() {
            return None;
        }
        let mut pool = self.pool.lock().unwrap();
        let resource = pool
            .iter()
            .position(&mut predicate)
            .and_then(|idx| pool.remove(idx));
        let next_pool_len = pool.len();
        drop(pool);
        self.record_acquisition_pressure(next_pool_len);
        resource
    }

    /// Try to enqueue an idle resource only if the pool is below the high watermark.
    ///
    /// This is intended for maintenance refill paths that have just created a
    /// resource and need a final bounded insert before publishing it as idle.
    pub fn try_push_bounded(&self, resource: T) -> Result<(), T> {
        self.try_push_bounded_to(resource, self.config.high_watermark)
    }

    /// Try to enqueue an idle resource below a caller-specific limit.
    ///
    /// The effective limit is capped at `high_watermark`. Checking the current
    /// length and inserting the resource happen under the same lock, so the
    /// caller's decision is linearizable with concurrent pool operations.
    pub fn try_push_bounded_to(&self, resource: T, limit: usize) -> Result<(), T> {
        let limit = limit.min(self.config.high_watermark);
        if !self.is_shutting_down() {
            let mut pool = self.pool.lock().unwrap();
            if !self.is_shutting_down() && pool.len() < limit {
                pool.push_back(resource);
                return Ok(());
            }
        }
        Err(resource)
    }

    /// Drain one idle resource from the back of the pool.
    pub fn try_drain_one(&self) -> Option<T> {
        let mut pool = self.pool.lock().unwrap();
        pool.pop_back()
    }

    /// Return a resource to the pool.
    ///
    /// If maintenance is enabled, enqueues the resource even when the pool is
    /// above the high watermark so the maintenance worker owns all drain
    /// decisions. If maintenance is disabled, respects the high watermark and
    /// returns `Err(resource)` when the pool is full.
    pub fn release(&self, resource: T) -> Result<(), T> {
        if !self.is_shutting_down() {
            let mut pool = self.pool.lock().unwrap();
            // Re-check after taking the lock to avoid racing shutdown.
            if !self.is_shutting_down()
                && (self.config.maintenance_enabled || pool.len() < self.config.high_watermark)
            {
                let next_pool_len = pool.len() + 1;
                pool.push_back(resource);
                drop(pool);
                if next_pool_len < self.config.low_watermark
                    || next_pool_len > self.config.high_watermark
                {
                    self.request_maintenance();
                }
                return Ok(());
            }
        }
        // Pool is full or shutting down: return the resource to the caller.
        Err(resource)
    }

    /// Drain all resources from the pool and return them.
    ///
    /// This is intended for shutdown cleanup. After calling this, the pool
    /// rejects new releases.
    pub fn drain_all(&self) -> Vec<T> {
        self.shutting_down.store(true, Ordering::Release);
        self.stop_maintenance_worker();

        let mut pool = self.pool.lock().unwrap();
        pool.drain(..).collect()
    }

    /// Start the background maintenance worker if not already started.
    ///
    /// The worker runs the provided `run_cycle` closure in a loop, which
    /// should call `compute_maintenance_action` and perform the necessary
    /// create/delete operations.
    pub fn start_maintenance_worker<F>(&'static self, run_cycle: F)
    where
        F: Fn() + Send + 'static,
    {
        if !self.config.maintenance_enabled {
            return;
        }
        if self.maintenance_started.swap(true, Ordering::AcqRel) {
            return;
        }

        match std::thread::Builder::new()
            .name("warm-pool-maintenance".to_string())
            .spawn(move || self.maintenance_worker_loop(run_cycle))
        {
            Ok(handle) => {
                *self.maintenance_worker.lock().unwrap() = Some(handle);
                self.request_maintenance();
            }
            Err(err) => {
                self.maintenance_started.store(false, Ordering::Release);
                tracing::warn!(error = %err, "failed to start warm pool maintenance worker");
            }
        }
    }

    fn maintenance_worker_loop<F>(&self, run_cycle: F)
    where
        F: Fn(),
    {
        let mut has_immediate_work = false;
        loop {
            if !has_immediate_work {
                let mut signal = self.maintenance_signal.lock().unwrap();
                while !signal.stop && !signal.pending {
                    signal = self.maintenance_cv.wait(signal).unwrap();
                }
                if signal.stop {
                    break;
                }
                signal.pending = false;
            }

            if self.is_shutting_down() {
                break;
            }

            run_cycle();

            if self.is_shutting_down() {
                break;
            }

            has_immediate_work = {
                let pool_len = self.pool.lock().unwrap().len();
                !matches!(
                    self.compute_maintenance_action(pool_len),
                    PoolMaintenanceAction::Idle
                )
            };
        }
    }

    fn stop_maintenance_worker(&self) {
        if !self.maintenance_started.load(Ordering::Acquire) {
            return;
        }

        {
            let mut signal = self.maintenance_signal.lock().unwrap();
            signal.stop = true;
            signal.pending = true;
            self.maintenance_cv.notify_all();
        }

        if let Some(handle) = self.maintenance_worker.lock().unwrap().take() {
            if let Err(err) = handle.join() {
                tracing::warn!(?err, "warm pool maintenance worker panicked during join");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validation_clamps_low_to_high() {
        let config = PoolConfig {
            low_watermark: 64,
            high_watermark: 32,
            maintenance_enabled: true,
            startup_prewarm: false,
        }
        .validate();
        assert_eq!(config.low_watermark, 32);
        assert_eq!(config.high_watermark, 32);
    }

    #[test]
    fn compute_maintenance_action_fills_to_initial_low_watermark() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 4,
            high_watermark: 10,
            maintenance_enabled: true,
            startup_prewarm: false,
        });
        assert_eq!(
            pool.compute_maintenance_action(2),
            PoolMaintenanceAction::Fill(2)
        );
        assert_eq!(
            pool.compute_maintenance_action(7),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn acquisition_pressure_grows_fill_target_geometrically() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: true,
            startup_prewarm: false,
        });

        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(2)
        );

        pool.release(1).unwrap();
        pool.release(2).unwrap();
        assert_eq!(pool.try_acquire(), Some(1));
        assert_eq!(
            pool.compute_maintenance_action(1),
            PoolMaintenanceAction::Fill(3)
        );

        assert_eq!(pool.try_acquire(), Some(2));
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(8)
        );
    }

    #[test]
    fn acquisition_misses_grow_fill_target_geometrically() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });

        assert_eq!(pool.try_acquire(), None);
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(4)
        );

        assert_eq!(pool.try_acquire(), None);
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(8)
        );

        assert_eq!(pool.try_acquire_where(|_| true), None);
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(10)
        );
    }

    #[test]
    fn acquisition_miss_requests_maintenance() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: true,
            startup_prewarm: false,
        });

        assert!(!pool.maintenance_signal.lock().unwrap().pending);
        assert_eq!(pool.try_acquire(), None);
        assert!(pool.maintenance_signal.lock().unwrap().pending);
    }

    #[test]
    fn compute_maintenance_action_drains_above_high() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 4,
            maintenance_enabled: true,
            startup_prewarm: false,
        });
        assert_eq!(
            pool.compute_maintenance_action(8),
            PoolMaintenanceAction::Drain(4)
        );
        assert_eq!(
            pool.compute_maintenance_action(4),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn try_acquire_returns_none_when_empty() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        assert!(pool.try_acquire().is_none());
    }

    #[test]
    fn try_acquire_returns_resource_when_available() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        pool.release(42).unwrap();
        assert_eq!(pool.try_acquire(), Some(42));
    }

    #[test]
    fn release_respects_high_watermark_when_maintenance_disabled() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 2,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        assert!(pool.release(1).is_ok());
        assert!(pool.release(2).is_ok());
        assert_eq!(pool.release(3), Err(3));
    }

    #[test]
    fn bounded_push_rejects_after_release_fills_stricter_limit() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 64,
            maintenance_enabled: false,
            startup_prewarm: false,
        });

        for value in 0..7 {
            pool.release(value).unwrap();
        }

        // Model the refill path observing len == 7 before a concurrent release
        // wins the race and fills the proactive limit.
        assert!(pool.len() < 8);
        pool.release(7).unwrap();

        assert_eq!(pool.try_push_bounded_to(8, 8), Err(8));
        assert_eq!(pool.len(), 8);
    }

    #[test]
    fn concurrent_bounded_pushes_atomically_respect_stricter_limit() {
        use std::sync::{Arc, Barrier};

        let pool = Arc::new(WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 64,
            maintenance_enabled: false,
            startup_prewarm: false,
        }));
        let start = Arc::new(Barrier::new(33));
        let mut workers = Vec::new();

        for value in 0..32 {
            let pool = Arc::clone(&pool);
            let start = Arc::clone(&start);
            workers.push(std::thread::spawn(move || {
                start.wait();
                pool.try_push_bounded_to(value, 8).is_ok()
            }));
        }

        start.wait();
        let inserted = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|inserted| *inserted)
            .count();

        assert_eq!(inserted, 8);
        assert_eq!(pool.len(), 8);
    }

    #[test]
    fn release_allows_maintenance_worker_to_drain_above_high_watermark() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 2,
            maintenance_enabled: true,
            startup_prewarm: false,
        });
        assert!(pool.release(1).is_ok());
        assert!(pool.release(2).is_ok());
        assert!(pool.release(3).is_ok());
        assert_eq!(pool.pool.lock().unwrap().len(), 3);
    }

    #[test]
    fn drain_all_empties_pool_and_sets_shutting_down() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        pool.release(1).unwrap();
        pool.release(2).unwrap();
        let drained = pool.drain_all();
        assert_eq!(drained, vec![1, 2]);
        assert!(pool.is_shutting_down());
        assert!(pool.pool.lock().unwrap().is_empty());
    }

    #[test]
    fn try_acquire_returns_none_after_shutdown() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        pool.release(42).unwrap();
        pool.drain_all();
        assert!(pool.try_acquire().is_none());
    }

    #[test]
    fn release_rejects_after_shutdown() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        pool.drain_all();
        assert_eq!(pool.release(42), Err(42));
    }
}
