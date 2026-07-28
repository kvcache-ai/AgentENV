use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Notify;

static GLOBAL_SCHEDULER: std::sync::OnceLock<IoWeightScheduler> = std::sync::OnceLock::new();

/// A node-wide weighted fair I/O scheduler for ublk devices.
///
/// Distributes a configurable total bandwidth budget across active devices
/// proportionally to their assigned weights. Work-conserving: unused capacity
/// from idle devices is redistributed to active ones.
pub struct IoWeightScheduler {
    inner: Arc<SchedulerInner>,
}

impl IoWeightScheduler {
    /// Initialize the global scheduler. Only the first call takes effect.
    pub fn init_global(config: SchedulerConfig) {
        GLOBAL_SCHEDULER.get_or_init(|| Self::new(config));
    }

    /// Get a reference to the global scheduler, if initialized.
    pub fn global() -> Option<&'static IoWeightScheduler> {
        GLOBAL_SCHEDULER.get()
    }
}

struct SchedulerInner {
    config: SchedulerConfig,
    state: Mutex<SchedulerState>,
    notify: Notify,
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Total bandwidth budget in bytes/sec shared across all devices.
    pub total_bandwidth_bytes_per_sec: u64,
    /// Refill interval for token replenishment.
    pub refill_interval: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            total_bandwidth_bytes_per_sec: 500 * 1024 * 1024, // 500 MB/s default
            refill_interval: Duration::from_millis(100),
        }
    }
}

struct SchedulerState {
    devices: HashMap<u32, DeviceState>,
    last_refill: Instant,
}

struct DeviceState {
    weight: u32,
    tokens: i64,
    active: bool,
}

/// Handle for a single device participating in the weighted scheduler.
pub struct IoWeightHandle {
    dev_id: u32,
    scheduler: Arc<SchedulerInner>,
    bytes_consumed: AtomicU64,
}

impl IoWeightScheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                config,
                state: Mutex::new(SchedulerState {
                    devices: HashMap::new(),
                    last_refill: Instant::now(),
                }),
                notify: Notify::new(),
            }),
        }
    }

    /// Register a device with the given weight. Higher weight = more bandwidth share.
    pub fn register_device(&self, dev_id: u32, weight: u32) -> IoWeightHandle {
        let weight = weight.max(1);
        let mut state = self.inner.state.lock();
        state.devices.insert(
            dev_id,
            DeviceState {
                weight,
                tokens: 0,
                active: false,
            },
        );
        IoWeightHandle {
            dev_id,
            scheduler: self.inner.clone(),
            bytes_consumed: AtomicU64::new(0),
        }
    }

    /// Unregister a device.
    pub fn unregister_device(&self, dev_id: u32) {
        let mut state = self.inner.state.lock();
        state.devices.remove(&dev_id);
        // Wake anyone waiting — the weight distribution changed.
        self.inner.notify.notify_waiters();
    }

    /// Update a device's weight at runtime.
    pub fn update_weight(&self, dev_id: u32, new_weight: u32) {
        let new_weight = new_weight.max(1);
        let mut state = self.inner.state.lock();
        if let Some(dev) = state.devices.get_mut(&dev_id) {
            dev.weight = new_weight;
        }
        self.inner.notify.notify_waiters();
    }

    /// Get current device weights and their effective bandwidth allocations.
    pub fn snapshot(&self) -> Vec<(u32, u32, u64)> {
        let state = self.inner.state.lock();
        let total_weight: u32 = state
            .devices
            .values()
            .filter(|d| d.active)
            .map(|d| d.weight)
            .sum();
        let total_bw = self.inner.config.total_bandwidth_bytes_per_sec;
        state
            .devices
            .iter()
            .map(|(&id, dev)| {
                let effective_bw = if total_weight > 0 && dev.active {
                    total_bw * dev.weight as u64 / total_weight as u64
                } else {
                    0
                };
                (id, dev.weight, effective_bw)
            })
            .collect()
    }
}

impl IoWeightHandle {
    /// Acquire permission to perform I/O of the given size.
    /// Will block (async) if the device has exhausted its token budget for
    /// the current refill cycle.
    pub async fn acquire(&self, bytes: u64) {
        loop {
            {
                let mut state = self.scheduler.state.lock();
                self.maybe_refill(&mut state);

                if let Some(dev) = state.devices.get_mut(&self.dev_id) {
                    dev.active = true;
                    if dev.tokens >= 0 {
                        // Consume tokens (can go negative — request is still served,
                        // but next request will wait).
                        dev.tokens -= bytes as i64;
                        self.bytes_consumed.fetch_add(bytes, Ordering::Relaxed);
                        return;
                    }
                } else {
                    // Device was unregistered — allow the I/O unthrottled.
                    return;
                }
            }
            // Sleep until next refill cycle rather than waiting on notify alone,
            // because refill is checked inline (no background task).
            tokio::time::sleep(self.scheduler.config.refill_interval).await;
        }
    }

    /// Mark that this device completed its burst of I/O (for work-conservation).
    pub fn mark_idle(&self) {
        let mut state = self.scheduler.state.lock();
        if let Some(dev) = state.devices.get_mut(&self.dev_id) {
            dev.active = false;
        }
    }

    /// Get total bytes consumed by this device.
    pub fn total_bytes_consumed(&self) -> u64 {
        self.bytes_consumed.load(Ordering::Relaxed)
    }

    fn maybe_refill(&self, state: &mut SchedulerState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill);
        if elapsed < self.scheduler.config.refill_interval {
            return;
        }

        state.last_refill = now;
        let total_bw = self.scheduler.config.total_bandwidth_bytes_per_sec;
        let refill_fraction =
            elapsed.as_secs_f64() / 1.0; // tokens per second * elapsed seconds
        let total_tokens = (total_bw as f64 * refill_fraction) as i64;

        // Count active device weights for proportional distribution.
        let active_weight: u32 = state
            .devices
            .values()
            .filter(|d| d.active)
            .map(|d| d.weight)
            .sum();

        if active_weight == 0 {
            // No active devices — give everyone tokens proportionally by weight.
            let all_weight: u32 = state.devices.values().map(|d| d.weight).sum();
            if all_weight > 0 {
                for dev in state.devices.values_mut() {
                    let share = total_tokens * dev.weight as i64 / all_weight as i64;
                    dev.tokens = share; // Reset rather than accumulate
                }
            }
        } else {
            // Work-conserving: only active devices share the budget.
            for dev in state.devices.values_mut() {
                if dev.active {
                    let share = total_tokens * dev.weight as i64 / active_weight as i64;
                    dev.tokens = share; // Reset each cycle
                } else {
                    dev.tokens = 0;
                }
            }
        }

        // Wake all waiters to re-check their token budget.
        self.scheduler.notify.notify_waiters();
    }
}

impl Drop for IoWeightHandle {
    fn drop(&mut self) {
        self.scheduler
            .state
            .lock()
            .devices
            .remove(&self.dev_id);
        self.scheduler.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_weighted_scheduling() {
        let config = SchedulerConfig {
            total_bandwidth_bytes_per_sec: 100 * 1024 * 1024, // 100 MB/s
            refill_interval: Duration::from_millis(50),
        };
        let scheduler = IoWeightScheduler::new(config);

        let h1 = scheduler.register_device(1, 2); // weight 2
        let h2 = scheduler.register_device(2, 1); // weight 1

        // First acquire should succeed (initial tokens from first refill)
        h1.acquire(1024).await;
        h2.acquire(1024).await;

        // Verify both devices got tokens
        assert!(h1.total_bytes_consumed() > 0);
        assert!(h2.total_bytes_consumed() > 0);
    }

    #[tokio::test]
    async fn test_unregister() {
        let scheduler = IoWeightScheduler::new(SchedulerConfig::default());
        let h1 = scheduler.register_device(1, 1);
        h1.acquire(512).await;
        drop(h1);
        // After drop, device is unregistered
        let snapshot = scheduler.snapshot();
        assert!(snapshot.is_empty());
    }

    #[tokio::test]
    async fn test_weight_update() {
        let scheduler = IoWeightScheduler::new(SchedulerConfig::default());
        let _h1 = scheduler.register_device(1, 1);
        scheduler.update_weight(1, 10);
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot[0].1, 10);
    }
}
