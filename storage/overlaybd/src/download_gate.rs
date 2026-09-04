//! Node-level admission gate for background downloads.
//!
//! Two process-global in-flight counters separate foreground remote reads
//! (guest page faults, boot-critical reads — always latency-sensitive) from
//! background download block reads. Before a background block read starts,
//! the gate parks it while foreground reads are in flight, but always lets a
//! small floor of background blocks through so downloads can never starve.
//!
//! Correctness relies on the single-process assumption: the ublk daemon hosts
//! every block device and every background download in one process. If devices
//! are ever split across processes, this gate must be revisited.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

/// Foreground remote reads currently in flight (any ImageFile read entry).
static FG_REMOTE_INFLIGHT: AtomicI64 = AtomicI64::new(0);

/// Background download block reads currently in flight (counted by the
/// download tasks themselves, after passing the gate).
static BK_REMOTE_INFLIGHT: AtomicI64 = AtomicI64::new(0);

/// Minimum number of background block reads always allowed in flight, even
/// when foreground traffic is saturated. Keeps downloads making progress
/// (a trickle) instead of starving them outright.
const BK_FLOOR_INFLIGHT: i64 = 1;

const GATE_BACKOFF: Duration = Duration::from_millis(200);

/// Fallback for the envd-ready wait: if no readiness notification arrives for
/// a sandbox-bound device key within this window (signal lost, failed sandbox,
/// or a non-sandbox consumer), the download starts anyway. Never a deadlock.
pub const SANDBOX_READY_FALLBACK: Duration = Duration::from_secs(20);

struct ReadyRegistry {
    ready: parking_lot::Mutex<std::collections::HashSet<String>>,
    notify: tokio::sync::Notify,
}

fn ready_registry() -> &'static ReadyRegistry {
    static READY_REGISTRY: OnceLock<ReadyRegistry> = OnceLock::new();
    READY_REGISTRY.get_or_init(|| ReadyRegistry {
        ready: parking_lot::Mutex::new(std::collections::HashSet::new()),
        notify: tokio::sync::Notify::new(),
    })
}

/// Mark a sandbox-bound device as envd-ready, releasing its held background
/// downloads. Called by the ublk daemon when the owning process reports that
/// the sandbox finished booting. Public because the daemon process invokes it
/// through its RPC handler.
pub fn notify_sandbox_ready(device_key: &str) {
    let registry = ready_registry();
    registry.ready.lock().insert(device_key.to_string());
    registry.notify.notify_waiters();
}

/// Wait until `device_key` is reported envd-ready, the fallback elapses, or
/// the download is canceled — whichever comes first. Returns in all three
/// cases; callers proceed with their (post-ready) delay afterwards.
pub(crate) async fn wait_sandbox_ready(device_key: &str, fallback: Duration, running: &AtomicBool) {
    let registry = ready_registry();
    let deadline = tokio::time::Instant::now() + fallback;
    loop {
        if registry.ready.lock().contains(device_key) {
            return;
        }
        if !running.load(Ordering::SeqCst) {
            return;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return;
        }
        let slice = GATE_BACKOFF.min(deadline - now);
        tokio::select! {
            _ = registry.notify.notified() => {}
            _ = tokio::time::sleep(slice) => {}
        }
    }
}

/// RAII guard that marks one foreground remote read as in flight. Foreground
/// reads are counted at the ImageFile read entry; background downloads bypass
/// ImageFile entirely (they read their own source file), so they never pass
/// through this guard.
pub(crate) struct FgReadGuard;

impl FgReadGuard {
    pub(crate) fn new() -> Self {
        FG_REMOTE_INFLIGHT.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for FgReadGuard {
    fn drop(&mut self) {
        FG_REMOTE_INFLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Handle returned by [`gate_block_read`] once the block read is admitted.
/// Keeps the background in-flight count elevated until dropped (i.e. for the
/// duration of the actual remote read).
pub(crate) struct BkReadPermit;

impl Drop for BkReadPermit {
    fn drop(&mut self) {
        BK_REMOTE_INFLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Test builds only: serializes gate admissions against the gate unit tests.
/// Those tests hold this mutex for their whole body, so their foreground
/// guard can never leak into a concurrently running download test and park
/// its block reads at the floor (which would flake concurrency assertions).
/// `gate_block_read` holds the mutex only for a single admission check —
/// never across the parking sleep — so a parked download cannot starve the
/// tests that serialize on it. Tests that manipulate the counters directly
/// (the gate unit tests in this module) instead hold the
/// mutex for their whole body and call `gate_block_read_inner` directly.
/// Production code never touches this.
#[cfg(test)]
static GATE_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Wait until a background block read is admitted.
///
/// Admitted immediately unless foreground reads are in flight AND the
/// background floor is already used up. While parked, re-checks every
/// GATE_BACKOFF and also watches `running`, returning `false` on cancellation
/// so a parked task never delays the drain path. On admission the background
/// in-flight count is incremented and held by the returned permit.
///
/// The check-then-increment race can briefly push the floor one block higher
/// than configured; that is benign and accepted over taking a lock.
pub(crate) async fn gate_block_read(running: &AtomicBool) -> Option<BkReadPermit> {
    loop {
        {
            // Hold the test mutex only for this admission check, never across
            // the parking sleep, so a parked download cannot starve tests
            // that serialize on GATE_TEST_MUTEX.
            #[cfg(test)]
            let _test_guard = GATE_TEST_MUTEX.lock().await;
            if let Some(permit) = try_admit_block_read(running) {
                return Some(permit);
            }
            if !running.load(Ordering::SeqCst) {
                return None;
            }
        }
        tokio::time::sleep(GATE_BACKOFF).await;
    }
}

/// One admission check: admit immediately unless foreground reads are in
/// flight and the background floor is used up.
fn try_admit_block_read(running: &AtomicBool) -> Option<BkReadPermit> {
    if !running.load(Ordering::SeqCst) {
        return None;
    }
    let foreground_busy = FG_REMOTE_INFLIGHT.load(Ordering::SeqCst) > 0;
    if !foreground_busy {
        BK_REMOTE_INFLIGHT.fetch_add(1, Ordering::SeqCst);
        return Some(BkReadPermit);
    }
    BK_REMOTE_INFLIGHT
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |background| {
            (background < BK_FLOOR_INFLIGHT).then_some(background + 1)
        })
        .ok()
        .map(|_| BkReadPermit)
}

#[cfg(test)]
async fn gate_block_read_inner(running: &AtomicBool) -> Option<BkReadPermit> {
    loop {
        if let Some(permit) = try_admit_block_read(running) {
            return Some(permit);
        }
        if !running.load(Ordering::SeqCst) {
            return None;
        }
        tokio::time::sleep(GATE_BACKOFF).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests manipulate the process-global counters, so each one holds
    // GATE_TEST_MUTEX for its whole body and calls gate_block_read_inner
    // directly. Concurrently running download tests serialize on the same
    // mutex inside gate_block_read, so the foreground guards created here can
    // never park another test's block reads at the floor. Release-phase
    // assertions still use generous timeouts so a BK=0 window is guaranteed
    // to occur.

    #[tokio::test(flavor = "current_thread")]
    async fn gate_admits_immediately_when_foreground_idle() {
        let _serialize = GATE_TEST_MUTEX.lock().await;
        let running = AtomicBool::new(true);
        let first = gate_block_read_inner(&running).await;
        assert!(first.is_some());
        let second = gate_block_read_inner(&running).await;
        assert!(second.is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gate_floor_admits_one_block_under_foreground_pressure() {
        let _serialize = GATE_TEST_MUTEX.lock().await;
        let _fg = FgReadGuard::new();
        let running = AtomicBool::new(true);
        let first = gate_block_read_inner(&running)
            .await
            .expect("floor must admit the first block");
        // Floor exhausted: a second admission parks until the first is dropped.
        let second =
            tokio::time::timeout(Duration::from_millis(300), gate_block_read_inner(&running)).await;
        assert!(second.is_err(), "second block must park behind the floor");
        drop(first);
        let third = tokio::time::timeout(Duration::from_secs(2), gate_block_read_inner(&running))
            .await
            .expect("admitted after release");
        assert!(third.is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gate_floor_is_exact_under_concurrent_admission() {
        let _serialize = GATE_TEST_MUTEX.lock().await;
        let _fg = FgReadGuard::new();
        let running = AtomicBool::new(true);
        // Holding GATE_TEST_MUTEX blocks new admissions from concurrent
        // download tests, but permits granted before we locked it may still
        // be live. Wait for them to drain so every iteration starts at BK=0.
        tokio::time::timeout(Duration::from_secs(10), async {
            while BK_REMOTE_INFLIGHT.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pre-existing background permits must drain");
        for _ in 0..50 {
            let barrier = std::sync::Barrier::new(32);
            let admitted = std::thread::scope(|scope| {
                let handles = (0..32)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            try_admit_block_read(&running)
                        })
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .filter_map(|handle| handle.join().expect("admission thread"))
                    .collect::<Vec<_>>()
            });
            assert_eq!(
                admitted.len(),
                1,
                "foreground pressure admits only the floor"
            );
            drop(admitted);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gate_returns_none_when_canceled() {
        let _serialize = GATE_TEST_MUTEX.lock().await;
        let _fg = FgReadGuard::new();
        let running = AtomicBool::new(true);
        let first = gate_block_read_inner(&running).await.expect("floor block");
        running.store(false, Ordering::SeqCst);
        let result =
            tokio::time::timeout(Duration::from_millis(500), gate_block_read_inner(&running)).await;
        assert!(matches!(result, Ok(None)));
        drop(first);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ready_wait_releases_on_notification() {
        let running = AtomicBool::new(true);
        let key = format!("test-ready-notify-{}", std::process::id());
        let waiter = wait_sandbox_ready(&key, Duration::from_secs(30), &running);
        tokio::pin!(waiter);
        tokio::time::sleep(Duration::from_millis(50)).await;
        notify_sandbox_ready(&key);
        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("waiter released on notify");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ready_wait_falls_back_after_timeout() {
        let running = AtomicBool::new(true);
        let key = format!("test-ready-fallback-{}", std::process::id());
        let started = std::time::Instant::now();
        wait_sandbox_ready(&key, Duration::from_millis(250), &running).await;
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(250));
        assert!(elapsed < Duration::from_secs(2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ready_wait_returns_early_when_canceled() {
        let running = AtomicBool::new(true);
        let key = format!("test-ready-cancel-{}", std::process::id());
        running.store(false, Ordering::SeqCst);
        tokio::time::timeout(
            Duration::from_millis(500),
            wait_sandbox_ready(&key, Duration::from_secs(30), &running),
        )
        .await
        .expect("canceled wait returns promptly");
    }
}
