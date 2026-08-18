//! Generic warm resource pool with watermark-based maintenance.
//!
//! This crate provides reusable pool mechanics for resources that are expensive
//! to create but can be reset and reused. It handles:
//! - Watermark-based refill/drain decisions
//! - Idle TTL decay of the geometric refill target
//! - Background maintenance worker with condvar signaling
//! - Shutdown coordination with safe resource cleanup
//! - Process exit hooks for static singleton pools
//!
//! Resource-specific create/reset/delete logic is provided via trait hooks.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

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
    /// Maximum time without acquisitions before the geometric fill target
    /// decays back to the low watermark and idle resources above it are
    /// drained. `None` keeps the fill target ratcheted for the process
    /// lifetime.
    pub idle_ttl: Option<Duration>,
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
    /// high watermark under acquisition pressure. With `idle_ttl` unset this
    /// intentionally ratchets upward for the process lifetime: after a node
    /// observes bursty demand, it keeps extra warm capacity instead of
    /// shrinking back to cold-start behavior. When `idle_ttl` is set, a
    /// sustained idle period decays the target back to the low watermark so
    /// warm capacity (and the resources it holds) is released.
    fill_target: Mutex<usize>,
    /// Last time an acquisition was attempted. Drives idle TTL decay.
    last_acquisition: Mutex<Instant>,
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
            last_acquisition: Mutex::new(Instant::now()),
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
        let idle_expired = self.decay_fill_target_if_idle();
        let fill_target = self.current_fill_target();
        if pool_len < fill_target {
            let to_fill = fill_target.saturating_sub(pool_len);
            if to_fill > 0 {
                return PoolMaintenanceAction::Fill(to_fill);
            }
        }
        // After an idle TTL decay the pool shrinks toward the decayed fill
        // target (the low watermark); otherwise only the high watermark caps
        // idle resources.
        let drain_target = if idle_expired {
            fill_target
        } else {
            self.config.high_watermark
        };
        if pool_len > drain_target {
            // Drain the full excess in one maintenance cycle. Resource-specific
            // cleanup happens outside the pool lock, and shutdown paths already
            // have to tolerate draining the whole pool.
            let to_drain = pool_len - drain_target;
            if to_drain > 0 {
                return PoolMaintenanceAction::Drain(to_drain);
            }
        }
        PoolMaintenanceAction::Idle
    }

    fn current_fill_target(&self) -> usize {
        (*self.fill_target.lock().unwrap()).min(self.config.high_watermark)
    }

    /// Collapse the geometric fill target back to the low watermark when no
    /// acquisition happened within `idle_ttl`. Returns true when the TTL had
    /// expired, so callers drain excess idle resources toward the low
    /// watermark instead of the high one. The idle clock restarts on each
    /// expiry so a fully decayed pool does not retrigger every cycle.
    fn decay_fill_target_if_idle(&self) -> bool {
        let Some(ttl) = self.config.idle_ttl else {
            return false;
        };
        let mut last = self.last_acquisition.lock().unwrap();
        if last.elapsed() < ttl {
            return false;
        }
        *last = Instant::now();
        let low = self.config.low_watermark.min(self.config.high_watermark);
        let mut target = self.fill_target.lock().unwrap();
        *target = (*target).min(low);
        true
    }

    /// Time until the idle TTL expires, if decay is configured.
    fn idle_ttl_remaining(&self) -> Option<Duration> {
        let ttl = self.config.idle_ttl?;
        let elapsed = self.last_acquisition.lock().unwrap().elapsed();
        Some(ttl.saturating_sub(elapsed))
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
        *self.last_acquisition.lock().unwrap() = Instant::now();
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
        if !self.is_shutting_down() {
            let mut pool = self.pool.lock().unwrap();
            if !self.is_shutting_down() && pool.len() < self.config.high_watermark {
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
                    match self.idle_ttl_remaining() {
                        Some(remaining) => {
                            // Wake when the idle TTL expires even if nothing
                            // requested maintenance, so the fill target can
                            // decay and excess idle resources drain.
                            let (new_signal, timeout) =
                                self.maintenance_cv.wait_timeout(signal, remaining).unwrap();
                            signal = new_signal;
                            if timeout.timed_out() && !signal.stop && !signal.pending {
                                signal.pending = true;
                            }
                        }
                        None => {
                            signal = self.maintenance_cv.wait(signal).unwrap();
                        }
                    }
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
            idle_ttl: None,
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
            idle_ttl: None,
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
            idle_ttl: None,
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
            idle_ttl: None,
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
            idle_ttl: None,
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
            idle_ttl: None,
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
            idle_ttl: None,
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
            idle_ttl: None,
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
            idle_ttl: None,
        });
        assert!(pool.release(1).is_ok());
        assert!(pool.release(2).is_ok());
        assert_eq!(pool.release(3), Err(3));
    }

    #[test]
    fn release_allows_maintenance_worker_to_drain_above_high_watermark() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 2,
            maintenance_enabled: true,
            startup_prewarm: false,
            idle_ttl: None,
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
            idle_ttl: None,
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
            idle_ttl: None,
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
            idle_ttl: None,
        });
        pool.drain_all();
        assert_eq!(pool.release(42), Err(42));
    }

    #[test]
    fn idle_ttl_decays_fill_target_and_drains_to_low_watermark() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
            idle_ttl: Some(Duration::from_millis(50)),
        });

        // Ratchet the fill target up to 8 under acquisition pressure.
        pool.release(1).unwrap();
        pool.release(2).unwrap();
        assert_eq!(pool.try_acquire(), Some(1));
        assert_eq!(pool.try_acquire(), Some(2));
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(8)
        );

        for value in 1..=6 {
            pool.release(value).unwrap();
        }

        std::thread::sleep(Duration::from_millis(80));

        // Idle past the TTL: excess drains toward the decayed fill target
        // (low watermark) instead of lingering below the high watermark.
        assert_eq!(
            pool.compute_maintenance_action(6),
            PoolMaintenanceAction::Drain(4)
        );
        assert_eq!(
            pool.compute_maintenance_action(2),
            PoolMaintenanceAction::Idle
        );
        assert_eq!(
            pool.compute_maintenance_action(1),
            PoolMaintenanceAction::Fill(1)
        );
    }

    #[test]
    fn acquisitions_reset_idle_ttl_clock() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
            idle_ttl: Some(Duration::from_millis(300)),
        });

        assert_eq!(pool.try_acquire(), None);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(pool.try_acquire(), None);
        std::thread::sleep(Duration::from_millis(150));

        // 150ms since the last acquisition: the 300ms TTL has not expired and
        // the ratcheted fill target is preserved.
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(8)
        );

        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(2)
        );
    }

    #[test]
    fn maintenance_worker_wakes_on_idle_ttl_and_drains() {
        let pool: &'static WarmPool<u32> = Box::leak(Box::new(WarmPool::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 4,
            maintenance_enabled: true,
            startup_prewarm: false,
            idle_ttl: Some(Duration::from_millis(50)),
        })));
        pool.start_maintenance_worker(move || {
            if let PoolMaintenanceAction::Drain(to_drain) =
                pool.compute_maintenance_action(pool.len())
            {
                for _ in 0..to_drain {
                    pool.try_drain_one();
                }
            }
        });

        pool.release(1).unwrap();
        pool.release(2).unwrap();
        pool.release(3).unwrap();

        // No acquisition happens, so the idle TTL wake-up drains everything
        // (low watermark is 0) without any explicit maintenance request.
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(pool.len(), 0);

        pool.drain_all();
    }
}
