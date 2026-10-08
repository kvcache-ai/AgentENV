//! Device-level I/O metrics for ublk targets.
//!
//! Recorded on the queue worker threads of the ublk daemon process, which
//! installs the global Prometheus recorder (`metrics_server.rs`), so the
//! plain `metrics` macros below export without extra plumbing. Series are
//! aggregated per target across all devices: `dev_id` is unbounded (one or
//! more devices per sandbox plus warm-pool churn) and must never become a
//! label. Fixed-label metric handles are registered once at target
//! construction, so recording a completion only pays atomic increments and
//! never rebuilds a Key or touches the recorder registry; slow-I/O tracking
//! adds one `DashMap` insert/remove per read/write request.
//!
//! Stuck-I/O visibility cannot rely on completion-time recording: a remote
//! read that hangs never completes, and in-flight `handle_io_request`
//! futures are dropped without completing when per-queue runtimes are torn
//! down. Read/write requests are therefore also registered in a
//! [`SlowIoTracker`] at I/O start (removed by an RAII guard on completion
//! or future drop), and a process-wide scanner task periodically republishes
//! the `agentenv_ublk_io_in_flight` gauge split by a `slow_io` label —
//! independent of I/O completion.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use metrics::{Counter, Histogram};

use crate::UblkDescOperation;

/// The complete `op` label value set for every `agentenv_ublk_io_*` series.
/// Flush/discard/unsupported requests are folded into `Other`; only the
/// data-plane operations get their own label.
///
/// `COUNT` is derived from `ALL` so the variants are listed exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IoOp {
    Read,
    Write,
    Other,
}

impl IoOp {
    const ALL: [IoOp; 3] = [Self::Read, Self::Write, Self::Other];
    const COUNT: usize = Self::ALL.len();

    fn as_label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Other => "other",
        }
    }

    fn idx(self) -> usize {
        match self {
            Self::Read => 0,
            Self::Write => 1,
            Self::Other => 2,
        }
    }

    /// Read/write are the data-plane operations: they get duration
    /// histograms, byte counters and slow-I/O tracking. Other requests are
    /// covered by the request counter only.
    pub(crate) fn is_data_op(self) -> bool {
        matches!(self, Self::Read | Self::Write)
    }
}

impl From<UblkDescOperation> for IoOp {
    fn from(op: UblkDescOperation) -> Self {
        match op {
            UblkDescOperation::Read => Self::Read,
            UblkDescOperation::Write => Self::Write,
            _ => Self::Other,
        }
    }
}

/// Per-target metric handles for the ublk device I/O path. Fixed-label
/// handles are registered eagerly at construction (so unused series exist
/// with value 0); error counters carry the raw errno number as a label and
/// are recorded with a direct macro call, which is affordable because
/// errors are rare.
///
/// Invariant: the global Prometheus recorder must be installed before any
/// target is constructed. Registration captures whatever recorder is
/// installed at that moment, so a target constructed before installation
/// would hold no-op handles forever, silently dropping every sample. The
/// daemon guarantees this by installing the recorder in
/// `metrics_server::spawn` before any device is created.
pub(crate) struct UblkIoMetrics {
    target: &'static str,
    requests_ok: [Counter; IoOp::COUNT],
    bytes: [Counter; 2],
    durations: [Histogram; 2],
}

impl UblkIoMetrics {
    pub(crate) fn new(target: &'static str) -> Self {
        Self {
            target,
            requests_ok: std::array::from_fn(|idx| {
                ::metrics::counter!(
                    "agentenv_ublk_io_requests_total",
                    "target" => target,
                    "op" => IoOp::ALL[idx].as_label(),
                    "errno" => "none",
                )
            }),
            // Bytes and durations only exist for the data-plane ops;
            // IoOp::ALL is ordered so indices 0 and 1 are read and write.
            bytes: std::array::from_fn(|idx| {
                ::metrics::counter!(
                    "agentenv_ublk_io_bytes_total",
                    "target" => target,
                    "op" => IoOp::ALL[idx].as_label(),
                )
            }),
            durations: std::array::from_fn(|idx| {
                // Bucket override lives in `crates/observability/src/lib.rs`
                // (`UBLK_IO_DURATION_BUCKETS`): the largest finite bucket
                // matches `SLOW_IO_THRESHOLD`.
                ::metrics::histogram!(
                    "agentenv_ublk_io_duration_seconds",
                    "target" => target,
                    "op" => IoOp::ALL[idx].as_label(),
                )
            }),
        }
    }

    /// Record one completed ublk I/O request. `errno` is the negative errno
    /// returned to the driver on failure (`None` on success); `bytes` is the
    /// completed byte count (read/write only).
    pub(crate) fn record_completion(
        &self,
        op: IoOp,
        errno: Option<i32>,
        bytes: u64,
        elapsed: Duration,
    ) {
        match errno {
            None => {
                self.requests_ok[op.idx()].increment(1);
                if op.is_data_op() && bytes > 0 {
                    self.bytes[op.idx()].increment(bytes);
                }
            }
            Some(raw) => {
                // The raw errno number becomes the label verbatim; the set
                // of errnos the target can produce is small in practice
                // (see `anyhow_to_ublk_errno`), keeping cardinality bounded.
                ::metrics::counter!(
                    "agentenv_ublk_io_requests_total",
                    "target" => self.target,
                    "op" => op.as_label(),
                    "errno" => raw.unsigned_abs().to_string(),
                )
                .increment(1);
            }
        }

        if op.is_data_op() {
            self.durations[op.idx()].record(elapsed.as_secs_f64());
        }
    }
}

// ---------------------------------------------------------------------------
// Slow-I/O tracking. Read/write requests register their start instant keyed
// by (queue id, tag) — each slot runs exactly one I/O at a time, so the key
// is unique per in-flight request. The guard removes the entry on drop,
// which also covers device teardown dropping the in-flight future. A single
// process-wide scanner task aggregates all live trackers into the
// `agentenv_ublk_io_in_flight` gauge, split by the `slow_io` label:
// I/Os pending for at least `SLOW_IO_THRESHOLD` count as `slow_io=
// "true"`, younger ones as `"false"`. Because the scanner (not I/O
// completion) maintains the gauge, I/Os stuck in a hung remote read remain
// visible.
// ---------------------------------------------------------------------------

/// An I/O pending for at least this long is reported as `slow_io="true"`.
const SLOW_IO_THRESHOLD: Duration = Duration::from_secs(30);

/// How often the scanner republishes the in-flight gauges.
const SLOW_IO_SCAN_INTERVAL: Duration = Duration::from_secs(5);

struct PendingIo {
    start: Instant,
    op: IoOp,
}

/// In-flight read/write I/Os of one target (one per ublk device).
pub(crate) struct SlowIoTracker {
    target: &'static str,
    pending: DashMap<(u16, u16), PendingIo>,
}

/// Removes the tracker entry on drop — on I/O completion, and also when the
/// in-flight `handle_io_request` future is dropped during device teardown.
pub(crate) struct SlowIoGuard {
    tracker: Arc<SlowIoTracker>,
    qid: u16,
    tag: u16,
}

impl Drop for SlowIoGuard {
    fn drop(&mut self) {
        self.tracker.pending.remove(&(self.qid, self.tag));
    }
}

impl SlowIoTracker {
    pub(crate) fn new(target: &'static str) -> Self {
        Self {
            target,
            pending: DashMap::new(),
        }
    }

    pub(crate) fn track(
        self: &Arc<Self>,
        qid: u16,
        tag: u16,
        op: IoOp,
        start: Instant,
    ) -> SlowIoGuard {
        self.pending.insert((qid, tag), PendingIo { start, op });
        SlowIoGuard {
            tracker: Arc::clone(self),
            qid,
            tag,
        }
    }

    /// Per-op pending counts as `(fresh, stuck)`: `stuck` counts I/Os
    /// pending for at least [`SLOW_IO_THRESHOLD`], `fresh` the rest. Sized
    /// by [`IoOp::COUNT`] so any op indexes safely, though only read/write
    /// are ever tracked or published.
    fn pending_counts(&self, now: Instant) -> [(u64, u64); IoOp::COUNT] {
        let mut counts = [(0u64, 0u64); IoOp::COUNT];
        for entry in self.pending.iter() {
            let slot = &mut counts[entry.op.idx()];
            if now.saturating_duration_since(entry.start) >= SLOW_IO_THRESHOLD {
                slot.1 += 1;
            } else {
                slot.0 += 1;
            }
        }
        counts
    }
}

static TRACKER_REGISTRY: Mutex<Vec<Weak<SlowIoTracker>>> = Mutex::new(Vec::new());

/// Register a target's tracker with the process-wide slow-I/O scanner.
pub(crate) fn register_slow_io_tracker(tracker: &Arc<SlowIoTracker>) {
    let mut registry = TRACKER_REGISTRY
        .lock()
        .expect("slow-io tracker registry poisoned");
    // Prune dead trackers on push too, so the registry stays bounded even
    // in processes that never spawn the scanner (tests, the standalone
    // ublk binary).
    registry.retain(|weak| weak.strong_count() > 0);
    registry.push(Arc::downgrade(tracker));
}

/// Spawn the process-wide slow-I/O scanner. Call once at daemon startup,
/// on the daemon's main runtime — never on a per-queue worker runtime,
/// because the scanner task is bound to whichever runtime spawns it.
pub fn spawn_slow_io_scanner() {
    tokio::spawn(run_slow_io_scanner());
}

async fn run_slow_io_scanner() {
    // Targets ever seen, so their series are reset to zero once the last
    // device of a target disappears instead of going stale.
    let mut seen_targets: HashSet<&'static str> = HashSet::new();
    let mut interval = tokio::time::interval(SLOW_IO_SCAN_INTERVAL);
    loop {
        interval.tick().await;
        let trackers: Vec<Arc<SlowIoTracker>> = {
            let mut registry = TRACKER_REGISTRY
                .lock()
                .expect("slow-io tracker registry poisoned");
            registry.retain(|weak| weak.strong_count() > 0);
            registry.iter().filter_map(Weak::upgrade).collect()
        };
        publish_slow_io_gauges(&trackers, &mut seen_targets, Instant::now());
    }
}

/// Aggregate the pending counts of all live trackers and republish the
/// `agentenv_ublk_io_in_flight` gauge series. Split out of the scanner loop
/// for testability.
fn publish_slow_io_gauges(
    trackers: &[Arc<SlowIoTracker>],
    seen_targets: &mut HashSet<&'static str>,
    now: Instant,
) {
    let mut totals: std::collections::HashMap<(&'static str, usize), (u64, u64)> =
        std::collections::HashMap::new();
    for tracker in trackers {
        seen_targets.insert(tracker.target);
        let counts = tracker.pending_counts(now);
        for op in [IoOp::Read, IoOp::Write] {
            let total = totals.entry((tracker.target, op.idx())).or_default();
            total.0 += counts[op.idx()].0;
            total.1 += counts[op.idx()].1;
        }
    }
    for target in seen_targets.iter() {
        for op in [IoOp::Read, IoOp::Write] {
            let (fresh, stuck) = totals
                .get(&(*target, op.idx()))
                .copied()
                .unwrap_or_default();
            for (slow_io, value) in [("false", fresh), ("true", stuck)] {
                ::metrics::gauge!(
                    "agentenv_ublk_io_in_flight",
                    "target" => *target,
                    "op" => op.as_label(),
                    "slow_io" => slow_io,
                )
                .set(value as f64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IoOp, SlowIoTracker, UblkIoMetrics};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::sync::Arc;
    use std::time::Duration;

    type CounterSeries = Vec<(String, String, u64)>;
    type HistogramSeries = Vec<(String, String, usize)>;

    fn snapshot() -> (CounterSeries, HistogramSeries) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        ::metrics::with_local_recorder(&recorder, || {
            let metrics = UblkIoMetrics::new("overlaybd");
            // One successful read, one timed-out write, one flush (folded
            // into op=other: request counter only, no duration/bytes), one
            // zero-byte read.
            metrics.record_completion(IoOp::Read, None, 4096, Duration::from_millis(3));
            metrics.record_completion(
                IoOp::Write,
                Some(-libc::ETIMEDOUT),
                0,
                Duration::from_secs(31),
            );
            metrics.record_completion(IoOp::Other, None, 0, Duration::from_millis(1));
            metrics.record_completion(IoOp::Read, None, 0, Duration::from_micros(50));
        });

        let mut counters: CounterSeries = Vec::new();
        let mut histograms: HistogramSeries = Vec::new();
        for (key, _unit, _description, value) in snapshotter.snapshot().into_vec() {
            let mut labels: Vec<String> = key
                .key()
                .labels()
                .map(|label| format!("{}={}", label.key(), label.value()))
                .collect();
            labels.sort_unstable();
            let name = key.key().name().to_owned();
            match value {
                DebugValue::Counter(recorded) => counters.push((name, labels.join(","), recorded)),
                DebugValue::Histogram(values) => {
                    histograms.push((name, labels.join(","), values.len()))
                }
                DebugValue::Gauge(_) => panic!("unexpected gauge: {name}"),
            }
        }
        counters.sort();
        histograms.sort();
        (counters, histograms)
    }

    #[test]
    fn completion_metrics_use_fixed_low_cardinality_series() {
        let (counters, histograms) = snapshot();

        let counter =
            |name: &str, labels: &str, value: u64| (name.to_owned(), labels.to_owned(), value);
        assert_eq!(
            counters,
            vec![
                counter(
                    "agentenv_ublk_io_bytes_total",
                    "op=read,target=overlaybd",
                    4096
                ),
                counter(
                    "agentenv_ublk_io_bytes_total",
                    "op=write,target=overlaybd",
                    0
                ),
                counter(
                    "agentenv_ublk_io_requests_total",
                    &format!("errno={},op=write,target=overlaybd", libc::ETIMEDOUT),
                    1
                ),
                counter(
                    "agentenv_ublk_io_requests_total",
                    "errno=none,op=other,target=overlaybd",
                    1
                ),
                counter(
                    "agentenv_ublk_io_requests_total",
                    "errno=none,op=read,target=overlaybd",
                    2
                ),
                counter(
                    "agentenv_ublk_io_requests_total",
                    "errno=none,op=write,target=overlaybd",
                    0
                ),
            ],
            "fixed-label series are registered at construction (zero when unused)",
        );

        let histogram = |name: &str, labels: &str, samples: usize| {
            (name.to_owned(), labels.to_owned(), samples)
        };
        assert_eq!(
            histograms,
            vec![
                histogram(
                    "agentenv_ublk_io_duration_seconds",
                    "op=read,target=overlaybd",
                    2
                ),
                histogram(
                    "agentenv_ublk_io_duration_seconds",
                    "op=write,target=overlaybd",
                    1
                ),
            ],
            "duration histograms only for read/write completions",
        );
    }

    #[test]
    fn slow_io_tracker_counts_fresh_and_stuck() {
        let tracker = Arc::new(SlowIoTracker::new("overlaybd"));

        // Insert entries with controlled ages by fabricating start instants.
        let now = std::time::Instant::now();
        for ((qid, tag), age, op) in [
            ((0u16, 0u16), Duration::from_secs(5), IoOp::Read),
            ((0, 1), Duration::from_secs(45), IoOp::Read),
            ((1, 0), Duration::from_secs(12), IoOp::Write),
        ] {
            tracker.pending.insert(
                (qid, tag),
                super::PendingIo {
                    start: now - age,
                    op,
                },
            );
        }

        let counts = tracker.pending_counts(now);
        // (fresh, stuck) per op against the fixed 30s threshold.
        assert_eq!(counts[IoOp::Read.idx()], (1, 1));
        assert_eq!(counts[IoOp::Write.idx()], (1, 0));
    }

    #[test]
    fn publish_slow_io_gauges_aggregates_and_zeroes() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        let gauge_snapshot = || {
            let mut gauges: Vec<(String, f64)> = Vec::new();
            for (key, _unit, _description, value) in snapshotter.snapshot().into_vec() {
                if let DebugValue::Gauge(value) = value {
                    let mut labels: Vec<String> = key
                        .key()
                        .labels()
                        .map(|label| format!("{}={}", label.key(), label.value()))
                        .collect();
                    labels.sort_unstable();
                    gauges.push((labels.join(","), *value));
                }
            }
            gauges.sort_by(|a, b| a.0.cmp(&b.0));
            gauges
        };

        ::metrics::with_local_recorder(&recorder, || {
            let now = std::time::Instant::now();
            let tracker_a = Arc::new(SlowIoTracker::new("overlaybd"));
            let tracker_b = Arc::new(SlowIoTracker::new("overlaybd"));
            for (tracker, key, age, op) in [
                (
                    &tracker_a,
                    (0u16, 0u16),
                    Duration::from_secs(15),
                    IoOp::Read,
                ),
                (&tracker_a, (0, 1), Duration::from_secs(45), IoOp::Read),
                (&tracker_b, (1, 0), Duration::from_secs(12), IoOp::Write),
            ] {
                tracker.pending.insert(
                    key,
                    super::PendingIo {
                        start: now - age,
                        op,
                    },
                );
            }

            let mut seen_targets = std::collections::HashSet::new();
            super::publish_slow_io_gauges(&[tracker_a, tracker_b], &mut seen_targets, now);
            // Counts are aggregated across trackers.
            assert_eq!(
                gauge_snapshot(),
                vec![
                    ("op=read,slow_io=false,target=overlaybd".to_owned(), 1.0),
                    ("op=read,slow_io=true,target=overlaybd".to_owned(), 1.0),
                    ("op=write,slow_io=false,target=overlaybd".to_owned(), 1.0),
                    ("op=write,slow_io=true,target=overlaybd".to_owned(), 0.0),
                ],
                "gauge series aggregated across trackers",
            );

            // Once every tracker is gone, the seen-target series are reset
            // to zero instead of going stale.
            super::publish_slow_io_gauges(&[], &mut seen_targets, now);
            let gauges = gauge_snapshot();
            assert_eq!(gauges.len(), 4, "series retained for seen targets");
            assert!(
                gauges.iter().all(|(_, value)| *value == 0.0),
                "series reset to zero once no trackers remain: {gauges:?}",
            );
        });
    }

    #[test]
    fn slow_io_guard_removes_entry_on_drop() {
        let tracker = Arc::new(SlowIoTracker::new("overlaybd"));
        {
            let _guard = tracker.track(2, 7, IoOp::Read, std::time::Instant::now());
            assert_eq!(tracker.pending.len(), 1);
            // Re-tracking the same slot replaces the stale entry.
            let _guard2 = tracker.track(2, 7, IoOp::Read, std::time::Instant::now());
            assert_eq!(tracker.pending.len(), 1);
        }
        assert!(tracker.pending.is_empty());
    }
}
