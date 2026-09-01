//! Validated completion statistics returned by Firecracker pre-faulting.

use anyhow::{ensure, Context, Result};
use firecracker_client::models::{PreFaultMemoryStats, PreFaultMemoryWorkerStats};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefaultWorkerStats {
    pub vcpu_id: u32,
    pub range_count: u64,
    pub requested_bytes: u64,
    pub completed_bytes: u64,
    pub remaining_bytes: u64,
    pub ioctl_count: u64,
    pub wall_time_us: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefaultCompletionStats {
    pub range_count: u64,
    pub requested_bytes: u64,
    pub completed_bytes: u64,
    pub remaining_bytes: u64,
    pub ioctl_count: u64,
    pub wall_time_us: u64,
    pub workers: Vec<PrefaultWorkerStats>,
}

fn nonnegative(value: i64, name: &str) -> Result<u64> {
    u64::try_from(value)
        .with_context(|| format!("Firecracker pre-fault stats has negative {name}: {value}"))
}

impl PrefaultWorkerStats {
    fn from_api(stats: PreFaultMemoryWorkerStats) -> Result<Self> {
        Ok(Self {
            vcpu_id: u32::try_from(stats.vcpu_id)
                .context("Firecracker pre-fault worker vcpu_id exceeds u32")?,
            range_count: nonnegative(stats.range_count, "worker range_count")?,
            requested_bytes: nonnegative(stats.requested_bytes, "worker requested_bytes")?,
            completed_bytes: nonnegative(stats.completed_bytes, "worker completed_bytes")?,
            remaining_bytes: nonnegative(stats.remaining_bytes, "worker remaining_bytes")?,
            ioctl_count: nonnegative(stats.ioctl_count, "worker ioctl_count")?,
            wall_time_us: nonnegative(stats.wall_time_us, "worker wall_time_us")?,
        })
    }
}

impl PrefaultCompletionStats {
    pub fn from_api(
        stats: PreFaultMemoryStats,
        expected_range_count: usize,
        expected_bytes: u64,
    ) -> Result<Self> {
        let workers = stats
            .workers
            .into_iter()
            .map(PrefaultWorkerStats::from_api)
            .collect::<Result<Vec<_>>>()?;
        let result = Self {
            range_count: nonnegative(stats.range_count, "range_count")?,
            requested_bytes: nonnegative(stats.requested_bytes, "requested_bytes")?,
            completed_bytes: nonnegative(stats.completed_bytes, "completed_bytes")?,
            remaining_bytes: nonnegative(stats.remaining_bytes, "remaining_bytes")?,
            ioctl_count: nonnegative(stats.ioctl_count, "ioctl_count")?,
            wall_time_us: nonnegative(stats.wall_time_us, "wall_time_us")?,
            workers,
        };
        ensure!(
            result.range_count
                == u64::try_from(expected_range_count).expect("range count exceeds u64"),
            "Firecracker pre-fault stats range_count {} differs from requested {}",
            result.range_count,
            expected_range_count
        );
        ensure!(
            result.requested_bytes == expected_bytes,
            "Firecracker pre-fault stats requested_bytes {} differs from requested {}",
            result.requested_bytes,
            expected_bytes
        );
        ensure!(
            result.completed_bytes == result.requested_bytes,
            "Firecracker pre-fault incomplete: requested_bytes={}, completed_bytes={}",
            result.requested_bytes,
            result.completed_bytes
        );
        ensure!(
            result.remaining_bytes == 0,
            "Firecracker pre-fault incomplete: remaining_bytes={}",
            result.remaining_bytes
        );
        ensure!(
            !result.workers.is_empty(),
            "Firecracker pre-fault stats has no workers"
        );
        let worker_requested: u64 = result
            .workers
            .iter()
            .map(|worker| worker.requested_bytes)
            .sum();
        let worker_completed: u64 = result
            .workers
            .iter()
            .map(|worker| worker.completed_bytes)
            .sum();
        let worker_remaining: u64 = result
            .workers
            .iter()
            .map(|worker| worker.remaining_bytes)
            .sum();
        ensure!(
            worker_requested == result.requested_bytes,
            "Firecracker pre-fault worker requested bytes {worker_requested} differ from aggregate {}",
            result.requested_bytes
        );
        ensure!(
            worker_completed == result.completed_bytes,
            "Firecracker pre-fault worker completed bytes {worker_completed} differ from aggregate {}",
            result.completed_bytes
        );
        ensure!(
            worker_remaining == result.remaining_bytes,
            "Firecracker pre-fault worker remaining bytes {worker_remaining} differ from aggregate {}",
            result.remaining_bytes
        );
        for worker in &result.workers {
            ensure!(
                worker.completed_bytes == worker.requested_bytes,
                "Firecracker pre-fault worker {} incomplete: requested_bytes={}, completed_bytes={}",
                worker.vcpu_id,
                worker.requested_bytes,
                worker.completed_bytes
            );
            ensure!(
                worker.remaining_bytes == 0,
                "Firecracker pre-fault worker {} has remaining_bytes={}",
                worker.vcpu_id,
                worker.remaining_bytes
            );
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(completed_bytes: i64, remaining_bytes: i64) -> PreFaultMemoryStats {
        PreFaultMemoryStats {
            range_count: 1,
            requested_bytes: 4096,
            completed_bytes,
            remaining_bytes,
            ioctl_count: 2,
            wall_time_us: 17,
            workers: vec![PreFaultMemoryWorkerStats {
                vcpu_id: 0,
                range_count: 1,
                requested_bytes: 4096,
                completed_bytes,
                remaining_bytes,
                ioctl_count: 2,
                wall_time_us: 11,
            }],
        }
    }

    #[test]
    fn accepts_fully_completed_stats() {
        let result = PrefaultCompletionStats::from_api(stats(4096, 0), 1, 4096).unwrap();
        assert_eq!(result.completed_bytes, 4096);
        assert_eq!(result.remaining_bytes, 0);
        assert_eq!(result.workers.len(), 1);
    }

    #[test]
    fn rejects_partial_or_remaining_stats() {
        assert!(PrefaultCompletionStats::from_api(stats(2048, 2048), 1, 4096).is_err());
        assert!(PrefaultCompletionStats::from_api(stats(4096, 1), 1, 4096).is_err());
    }
}

#[test]
fn rejects_unknown_stats_fields() {
    let aggregate_unknown = r#"{
            "range_count": 1,
            "requested_bytes": 4096,
            "completed_bytes": 4096,
            "remaining_bytes": 0,
            "ioctl_count": 1,
            "wall_time_us": 1,
            "workers": [{
                "vcpu_id": 0,
                "range_count": 1,
                "requested_bytes": 4096,
                "completed_bytes": 4096,
                "remaining_bytes": 0,
                "ioctl_count": 1,
                "wall_time_us": 1
            }],
            "unexpected": true
        }"#;
    assert!(serde_json::from_str::<PreFaultMemoryStats>(aggregate_unknown).is_err());

    let worker_unknown = r#"{
            "range_count": 1,
            "requested_bytes": 4096,
            "completed_bytes": 4096,
            "remaining_bytes": 0,
            "ioctl_count": 1,
            "wall_time_us": 1,
            "workers": [{
                "vcpu_id": 0,
                "range_count": 1,
                "requested_bytes": 4096,
                "completed_bytes": 4096,
                "remaining_bytes": 0,
                "ioctl_count": 1,
                "wall_time_us": 1,
                "unexpected": true
            }]
        }"#;
    assert!(serde_json::from_str::<PreFaultMemoryStats>(worker_unknown).is_err());
}
