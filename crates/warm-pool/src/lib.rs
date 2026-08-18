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
        // A zero idle TTL would expire on every cycle: `idle_ttl_remaining`
        // would always return zero and the maintenance worker would schedule
        // back-to-back cycles (busy loop) even after the pool reached its
        // target. The external config documents 0 as "never decay", so
        // normalize to `None` here; internal uses can then assume `Some(d)`
        // always carries d > 0.
        if self.idle_ttl == Some(Duration::ZERO) {
            self.idle_ttl = None;
        }
        self
    }
}

/// Demand-side pool state: last acquisition time, geometric fill target, and
/// the idle-decay lifecycle. Kept under one mutex so every maintenance action
/// is computed from a single atomic snapshot of demand: a concurrent
/// acquisition can never be observed halfway (new timestamp but stale decay
/// state or fill target).
#[derive(Debug)]
struct DemandState {
    /// Last time an acquisition was attempted. Drives idle TTL decay.
    last_acquisition: Instant,
    /// Current refill target. Starts at the low watermark and grows toward the
    /// high watermark under acquisition pressure. With `idle_ttl` unset this
    /// intentionally ratchets upward for the process lifetime: after a node
    /// observes bursty demand, it keeps extra warm capacity instead of
    /// shrinking back to cold-start behavior. When `idle_ttl` is set, a
    /// sustained idle period decays the target back to the low watermark so
    /// warm capacity (and the resources it holds) is released.
    fill_target: usize,
    /// Persistent "draining after idle decay" state. Set when the idle TTL
    /// expires, cleared by any acquisition, and cleared once the pool has
    /// verifiably drained to the decayed fill target (see
    /// `WarmPool::try_drain_one_for_maintenance`). Keeps the drain target
    /// pinned to the low watermark across partially-failed drain cycles, so
    /// the decay event cannot be consumed by a single action computation.
    decaying: bool,
}

impl DemandState {
    /// Compute the maintenance action from one synchronized demand snapshot.
    ///
    /// The action is a point-in-time plan: callers executing a `Drain` must
    /// re-validate each removal with `WarmPool::try_drain_one_for_maintenance`
    /// because an interleaved acquisition cancels the decay and a concurrent
    /// release/acquire changes the pool length.
    fn compute_maintenance_action(
        &mut self,
        config: &PoolConfig,
        pool_len: usize,
        now: Instant,
    ) -> PoolMaintenanceAction {
        // Apply the idle TTL decay transition, if any. The idle clock
        // restarts on each expiry so a fully decayed pool does not retrigger
        // every cycle, while `decaying` persists until the pool verifiably
        // reaches the decayed target: a partially-failed drain cycle or an
        // interleaved computation cannot consume the decay event and retain
        // the excess for another full TTL.
        if let Some(ttl) = config.idle_ttl {
            if now.saturating_duration_since(self.last_acquisition) >= ttl {
                self.last_acquisition = now;
                let low = config.low_watermark.min(config.high_watermark);
                self.fill_target = self.fill_target.min(low);
                self.decaying = true;
            }
        }

        let fill_target = self.fill_target.min(config.high_watermark);
        if pool_len < fill_target {
            let to_fill = fill_target.saturating_sub(pool_len);
            if to_fill > 0 {
                return PoolMaintenanceAction::Fill(to_fill);
            }
        }
        // After an idle TTL decay the pool shrinks toward the decayed fill
        // target (the low watermark); otherwise only the high watermark caps
        // idle resources.
        let drain_target = if self.decaying {
            fill_target
        } else {
            config.high_watermark
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

    /// Record an acquisition attempt: resets the idle clock, cancels any
    /// in-progress decay, and grows the fill target geometrically when the
    /// pool dipped below the low watermark.
    fn record_acquisition(&mut self, config: &PoolConfig, pool_len: usize, now: Instant) {
        self.last_acquisition = now;
        // New demand cancels any in-progress idle decay: the fill target may
        // grow again and the high watermark caps idle resources.
        self.decaying = false;
        if pool_len >= config.low_watermark || config.high_watermark == 0 {
            return;
        }

        let low = config.low_watermark.min(config.high_watermark);
        self.fill_target = self
            .fill_target
            .max(low)
            .max(1)
            .saturating_mul(2)
            .min(config.high_watermark);
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
    /// Demand state under a single mutex so decay decisions are atomic
    /// snapshots of the last acquisition, fill target, and decay lifecycle.
    demand_state: Mutex<DemandState>,
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
            demand_state: Mutex::new(DemandState {
                last_acquisition: Instant::now(),
                fill_target,
                decaying: false,
            }),
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
    ///
    /// The returned action is a point-in-time plan computed from `pool_len`
    /// and the demand state. A `Drain` plan can go stale before the caller
    /// finishes executing it (an interleaved acquisition cancels an
    /// in-progress decay; concurrent releases/acquisitions change the pool
    /// length), so maintenance drains must claim each resource through
    /// `try_drain_one_for_maintenance`, which re-validates the decision at
    /// execution time.
    pub fn compute_maintenance_action(&self, pool_len: usize) -> PoolMaintenanceAction {
        self.demand_state
            .lock()
            .unwrap()
            .compute_maintenance_action(&self.config, pool_len, Instant::now())
    }

    /// Time until the idle TTL expires, if decay is configured.
    fn idle_ttl_remaining(&self) -> Option<Duration> {
        let ttl = self.config.idle_ttl?;
        let elapsed = self.demand_state.lock().unwrap().last_acquisition.elapsed();
        Some(ttl.saturating_sub(elapsed))
    }

    fn record_acquisition_pressure(&self, pool_len: usize) {
        let now = Instant::now();
        self.demand_state
            .lock()
            .unwrap()
            .record_acquisition(&self.config, pool_len, now);
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

    /// Drain one idle resource as part of a maintenance drain, re-validating
    /// the drain decision against the live demand state and pool length at
    /// execution time.
    ///
    /// A previously computed `Drain` action can be stale by the time the
    /// caller executes it: an interleaved acquisition cancels an in-progress
    /// decay, and a concurrent release/acquire changes the pool length.
    /// Re-checking under the demand-state and pool locks while claiming each
    /// resource keeps the drain consistent — decay-driven drains stop as soon
    /// as an acquisition cancels the decay, and decay completion is marked
    /// only once the pool is verifiably at/below the decayed target, so a
    /// stale length snapshot cannot end the cycle early and strand resources.
    pub fn try_drain_one_for_maintenance(&self) -> Option<T> {
        let mut state = self.demand_state.lock().unwrap();
        let mut pool = self.pool.lock().unwrap();
        let drain_target = if state.decaying {
            state.fill_target.min(self.config.high_watermark)
        } else {
            self.config.high_watermark
        };
        if pool.len() <= drain_target {
            if state.decaying {
                // The pool verifiably reached the decayed fill target: the
                // decay cycle is complete and the high watermark caps idle
                // resources again.
                state.decaying = false;
            }
            return None;
        }
        let resource = pool.pop_back();
        if state.decaying && pool.len() <= drain_target {
            // This removal brought the pool to the decayed target: the decay
            // cycle is complete.
            state.decaying = false;
        }
        resource
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
    fn zero_idle_ttl_is_normalized_to_none() {
        // A zero TTL would expire every cycle and busy-loop the maintenance
        // worker; the external config documents 0 as "never decay".
        let config = PoolConfig {
            low_watermark: 0,
            high_watermark: 2,
            maintenance_enabled: true,
            startup_prewarm: false,
            idle_ttl: Some(Duration::ZERO),
        }
        .validate();
        assert_eq!(config.idle_ttl, None);
    }

    /// Rewind the idle clock so the idle TTL has expired, without sleeping.
    fn expire_idle_ttl<T: Send>(pool: &WarmPool<T>) {
        let ttl = pool.config().idle_ttl.expect("idle TTL configured");
        pool.demand_state.lock().unwrap().last_acquisition = Instant::now() - ttl;
    }

    #[test]
    fn idle_ttl_decays_fill_target_and_drains_to_low_watermark() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
            idle_ttl: Some(Duration::from_secs(60)),
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

        expire_idle_ttl(&pool);

        // Idle past the TTL: excess drains toward the decayed fill target
        // (low watermark) instead of lingering below the high watermark.
        assert_eq!(
            pool.compute_maintenance_action(6),
            PoolMaintenanceAction::Drain(4)
        );
        // The decay is not consumed by the first computation: if a drain
        // cycle only partially succeeds, the next computation keeps draining
        // toward the low watermark instead of retaining the excess for
        // another full TTL.
        assert_eq!(
            pool.compute_maintenance_action(6),
            PoolMaintenanceAction::Drain(4)
        );
        assert_eq!(
            pool.compute_maintenance_action(5),
            PoolMaintenanceAction::Drain(3)
        );

        // Execute the drain through the maintenance claim path: each removal
        // is re-validated, and the decay completes only once the pool
        // verifiably reaches the decayed target.
        for _ in 0..4 {
            assert!(pool.try_drain_one_for_maintenance().is_some());
            assert_eq!(pool.demand_state.lock().unwrap().decaying, pool.len() > 2);
        }
        assert_eq!(pool.len(), 2);
        assert!(pool.try_drain_one_for_maintenance().is_none());

        // The decay cycle ended: the high watermark caps idle resources again.
        assert_eq!(
            pool.compute_maintenance_action(6),
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
            idle_ttl: Some(Duration::from_secs(60)),
        });

        assert_eq!(pool.try_acquire(), None);
        assert_eq!(pool.try_acquire(), None);

        // Half the TTL has elapsed since the last acquisition: no decay, the
        // ratcheted fill target is preserved.
        let ttl = pool.config().idle_ttl.unwrap();
        pool.demand_state.lock().unwrap().last_acquisition = Instant::now() - ttl / 2;
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(8)
        );

        expire_idle_ttl(&pool);
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(2)
        );
    }

    #[test]
    fn demand_state_idle_ttl_uses_injected_clock() {
        let config = PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
            idle_ttl: Some(Duration::from_secs(30)),
        }
        .validate();
        let t0 = Instant::now();
        let mut state = DemandState {
            last_acquisition: t0,
            fill_target: 8,
            decaying: false,
        };

        // Just before expiry: no decay.
        assert_eq!(
            state.compute_maintenance_action(&config, 8, t0 + Duration::from_secs(29)),
            PoolMaintenanceAction::Idle
        );
        // At expiry: decay kicks in and drains toward the low watermark.
        assert_eq!(
            state.compute_maintenance_action(&config, 8, t0 + Duration::from_secs(30)),
            PoolMaintenanceAction::Drain(6)
        );

        // An acquisition resets the idle clock and cancels the decay.
        state.record_acquisition(&config, 7, t0 + Duration::from_secs(31));
        assert!(!state.decaying);
        assert_eq!(
            state.compute_maintenance_action(&config, 7, t0 + Duration::from_secs(40)),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn acquisition_cancels_stale_decay_drain_at_execution_time() {
        // Regression: a Drain action computed during decay goes stale when an
        // acquisition lands before the caller executes it; each removal must
        // be re-validated so resumed demand actually cancels the in-progress
        // decay instead of being drained toward the low watermark.
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
            idle_ttl: Some(Duration::from_secs(60)),
        });
        for value in 1..=6 {
            pool.release(value).unwrap();
        }
        expire_idle_ttl(&pool);
        assert_eq!(
            pool.compute_maintenance_action(6),
            PoolMaintenanceAction::Drain(4)
        );

        // Demand resumes after the computation but before the drain executes.
        assert!(pool.try_acquire().is_some());

        // The stale drain must not remove resources held for resumed demand.
        assert!(pool.try_drain_one_for_maintenance().is_none());
        assert_eq!(pool.len(), 5);
    }

    #[test]
    fn decay_completion_uses_verified_pool_length() {
        // Regression: decay completion must be decided from a pool-length
        // snapshot synchronized with the pool mutation, not from a length the
        // caller sampled earlier. A release landing after the sample raises
        // the pool above the decayed target; the decay must stay active until
        // the pool verifiably reaches it.
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
            idle_ttl: Some(Duration::from_secs(60)),
        });
        for value in 1..=4 {
            pool.release(value).unwrap();
        }
        expire_idle_ttl(&pool);
        assert_eq!(
            pool.compute_maintenance_action(4),
            PoolMaintenanceAction::Drain(2)
        );

        // Drain one (pool 3), then a concurrent release raises the pool back
        // to 4 — below the high watermark, so nothing re-signals maintenance.
        assert!(pool.try_drain_one_for_maintenance().is_some());
        pool.release(5).unwrap();

        // The decay is still active and drains the extra resource too.
        assert!(pool.demand_state.lock().unwrap().decaying);
        assert_eq!(
            pool.compute_maintenance_action(pool.len()),
            PoolMaintenanceAction::Drain(2)
        );
        assert!(pool.try_drain_one_for_maintenance().is_some());
        assert!(pool.try_drain_one_for_maintenance().is_some());
        assert!(!pool.demand_state.lock().unwrap().decaying);
        assert!(pool.try_drain_one_for_maintenance().is_none());
        assert_eq!(pool.len(), 2);
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
                    if pool.try_drain_one_for_maintenance().is_none() {
                        break;
                    }
                }
            }
        });

        pool.release(1).unwrap();
        pool.release(2).unwrap();
        pool.release(3).unwrap();

        // No acquisition happens, so the idle TTL wake-up drains everything
        // (low watermark is 0) without any explicit maintenance request.
        // Poll with a generous deadline instead of asserting after a fixed
        // sleep, so CI scheduling delays cannot fail the test.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !pool.is_empty() {
            assert!(
                Instant::now() < deadline,
                "maintenance worker did not drain the pool after the idle TTL"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(pool.len(), 0);

        pool.drain_all();
    }

    #[test]
    fn acquisition_clears_decay_state_before_next_computation() {
        // Regression: an acquisition landing after the idle TTL expiry must
        // cancel the drain-to-low transition atomically; a later computation
        // must not observe the new acquisition timestamp together with the
        // stale decaying state and return a low-watermark Drain for resumed
        // demand.
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
            idle_ttl: Some(Duration::from_secs(60)),
        });

        for value in 1..=6 {
            pool.release(value).unwrap();
        }
        expire_idle_ttl(&pool);

        // TTL expired: the decay starts and drains toward the low watermark.
        assert_eq!(
            pool.compute_maintenance_action(6),
            PoolMaintenanceAction::Drain(4)
        );

        // Demand resumes mid-drain: the acquisition resets the idle clock
        // and clears the decaying state in the same locked section, so the
        // high watermark caps idle resources again.
        assert!(pool.try_acquire().is_some());
        assert_eq!(
            pool.compute_maintenance_action(5),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn concurrent_acquisition_and_decay_keep_state_consistent() {
        // Regression: acquisitions and action computations race on the
        // demand state from multiple threads; the fill target must stay
        // within watermarks and no thread may observe a torn state.
        let pool = std::sync::Arc::new(WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 8,
            maintenance_enabled: false,
            startup_prewarm: false,
            idle_ttl: Some(Duration::from_millis(5)),
        }));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let pool = std::sync::Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                for value in 0..200u32 {
                    pool.release(value).unwrap();
                    let _ = pool.try_acquire();
                    let action = pool.compute_maintenance_action(pool.len());
                    if let PoolMaintenanceAction::Drain(n) | PoolMaintenanceAction::Fill(n) = action
                    {
                        assert!(n <= 8, "action size beyond high watermark: {n}");
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let state = pool.demand_state.lock().unwrap();
        assert!(state.fill_target <= 8);
    }
}
