use std::collections::HashSet;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use nix::sys::statvfs::statvfs;

use crate::cfg::AppConfig;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiskAdmissionReason {
    Ready,
    DiskCleanup,
    DiskHardLimit,
    CleanupDebt,
    DiskUsageUnavailable,
}

impl DiskAdmissionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::DiskCleanup => "disk_cleanup",
            Self::DiskHardLimit => "disk_hard_limit",
            Self::CleanupDebt => "cleanup_debt",
            Self::DiskUsageUnavailable => "disk_usage_unavailable",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiskFilesystemUsage {
    pub device_id: u64,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

impl DiskFilesystemUsage {
    pub fn used_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.available_bytes)
    }

    pub fn used_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            return 1.0;
        }
        self.used_bytes() as f64 / self.total_bytes as f64
    }
}

pub trait DiskUsageSource: Send + Sync + std::fmt::Debug {
    fn collect(&self) -> Result<Vec<DiskFilesystemUsage>>;

    fn is_fresh(&self) -> bool {
        true
    }
}

const DISK_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const DISK_SAMPLE_MAX_AGE: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct DiskUsageSample {
    started: Instant,
    usage: Vec<DiskFilesystemUsage>,
}

#[derive(Debug)]
pub(crate) struct SampledDiskUsage {
    latest: Arc<RwLock<Option<DiskUsageSample>>>,
    max_age: Duration,
}

impl SampledDiskUsage {
    pub(crate) fn for_paths(paths: Vec<PathBuf>) -> Self {
        Self::start(
            Arc::new(StatvfsDiskUsageSource { paths }),
            DISK_SAMPLE_INTERVAL,
            DISK_SAMPLE_MAX_AGE,
        )
    }

    fn start(source: Arc<dyn DiskUsageSource>, interval: Duration, max_age: Duration) -> Self {
        let latest = Arc::new(RwLock::new(None));
        let weak = Arc::downgrade(&latest);
        // A dedicated worker bounds hung filesystem probes without occupying Tokio's pool.
        let result = std::thread::Builder::new()
            .name("disk-usage".into())
            .spawn(move || {
                while weak.strong_count() > 0 {
                    let started = Instant::now();
                    let sample = match source.collect() {
                        Ok(usage) if !usage.is_empty() => Some(DiskUsageSample { started, usage }),
                        Ok(_) => None,
                        Err(error) => {
                            tracing::warn!(%error, "disk usage probe failed; closing admission");
                            None
                        }
                    };
                    let Some(latest) = weak.upgrade() else { break };
                    *latest.write().expect("disk sample lock poisoned") = sample;
                    drop(latest);
                    std::thread::sleep(interval);
                }
            });
        if let Err(error) = result {
            tracing::error!(%error, "disk usage worker unavailable; closing admission");
        }
        Self { latest, max_age }
    }
}

impl DiskUsageSource for SampledDiskUsage {
    fn is_fresh(&self) -> bool {
        self.collect().is_ok()
    }

    fn collect(&self) -> Result<Vec<DiskFilesystemUsage>> {
        let latest = self.latest.read().expect("disk sample lock poisoned");
        let sample = latest.as_ref().context("disk usage sample unavailable")?;
        anyhow::ensure!(
            sample.started.elapsed() <= self.max_age,
            "disk usage sample stale"
        );
        Ok(sample.usage.clone())
    }
}

#[derive(Debug)]
struct StatvfsDiskUsageSource {
    paths: Vec<PathBuf>,
}

impl DiskUsageSource for StatvfsDiskUsageSource {
    fn collect(&self) -> Result<Vec<DiskFilesystemUsage>> {
        let mut devices = HashSet::new();
        let mut filesystems = Vec::new();
        for path in &self.paths {
            let existing = nearest_existing_ancestor(path)?;
            let metadata = std::fs::metadata(&existing)
                .with_context(|| format!("stat disk-policy path {}", existing.display()))?;
            if !devices.insert(metadata.dev()) {
                continue;
            }
            let stats = statvfs(&existing)
                .with_context(|| format!("statvfs disk-policy path {}", existing.display()))?;
            let block_size = stats.fragment_size();
            filesystems.push(DiskFilesystemUsage {
                device_id: metadata.dev(),
                total_bytes: stats.blocks().saturating_mul(block_size),
                available_bytes: stats.blocks_available().saturating_mul(block_size),
            });
        }
        Ok(filesystems)
    }
}

fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf> {
    let mut current = path;
    loop {
        if current.exists() {
            return Ok(current.to_path_buf());
        }
        current = current.parent().with_context(|| {
            format!(
                "disk-policy path has no existing ancestor: {}",
                path.display()
            )
        })?;
    }
}

#[derive(Clone, Debug)]
pub struct DiskPolicySnapshot {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub cleanup_debt: u64,
    pub accepting_sandboxes: bool,
    pub reason: DiskAdmissionReason,
    pub cleanup_required: bool,
    reclaim_bytes: u64,
    pressure_latched: bool,
}

impl Default for DiskPolicySnapshot {
    fn default() -> Self {
        Self {
            total_bytes: 0,
            used_bytes: 0,
            available_bytes: 0,
            cleanup_debt: 0,
            accepting_sandboxes: true,
            reason: DiskAdmissionReason::Ready,
            cleanup_required: false,
            reclaim_bytes: 0,
            pressure_latched: false,
        }
    }
}

#[derive(Debug)]
pub struct DiskPolicyController {
    enabled: bool,
    cleanup_high: f64,
    cleanup_low: f64,
    admission_hard: f64,
    source: Arc<dyn DiskUsageSource>,
    snapshot: RwLock<DiskPolicySnapshot>,
}

impl DiskPolicyController {
    pub fn from_config(config: &AppConfig) -> Self {
        let paths = Self::monitored_paths(config);
        Self::new(
            config.disk_policy.enabled,
            config.disk_policy.cleanup_high_watermark_ratio,
            config.disk_policy.cleanup_low_watermark_ratio,
            config.disk_policy.admission_hard_watermark_ratio,
            Arc::new(SampledDiskUsage::for_paths(paths)),
        )
    }

    fn monitored_paths(config: &AppConfig) -> Vec<PathBuf> {
        let mut paths = vec![
            config.home_path.clone(),
            config.image.cache.root_dir.clone(),
            config.snapshot.local_cache_path.clone(),
            config.orchestrator.persisted_sandbox_store_path.clone(),
        ];
        match &config.firecracker.work_dir {
            Some(path) => {
                paths.push(path.clone());
                paths.push(path.join("managed-snapshots"));
            }
            None => {
                paths.push(std::env::temp_dir());
                paths.push(std::env::temp_dir().join("aenv/managed-snapshots"));
            }
        }
        if let Some(posix) = &config.backend.posix_fs {
            paths.push(posix.snapshot_store.clone());
        }
        paths
    }

    pub fn new(
        enabled: bool,
        cleanup_high: f64,
        cleanup_low: f64,
        admission_hard: f64,
        source: Arc<dyn DiskUsageSource>,
    ) -> Self {
        assert!(cleanup_low < cleanup_high);
        assert!(cleanup_high < admission_hard);
        Self {
            enabled,
            cleanup_high,
            cleanup_low,
            admission_hard,
            source,
            snapshot: RwLock::new(DiskPolicySnapshot::default()),
        }
    }

    pub async fn initialize(&self, cleanup_debt: u64) -> Result<()> {
        let deadline = Instant::now() + DISK_SAMPLE_INTERVAL;
        loop {
            let snapshot = self.refresh(cleanup_debt)?;
            if snapshot.reason != DiskAdmissionReason::DiskUsageUnavailable
                || Instant::now() >= deadline
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn refresh(&self, cleanup_debt: u64) -> Result<DiskPolicySnapshot> {
        if !self.enabled {
            return Ok(self.snapshot());
        }
        let filesystems = match self.source.collect() {
            Ok(filesystems) => filesystems,
            Err(_) => {
                let mut snapshot = self.snapshot.write().expect("disk policy lock poisoned");
                snapshot.accepting_sandboxes = false;
                snapshot.reason = DiskAdmissionReason::DiskUsageUnavailable;
                snapshot.cleanup_debt = cleanup_debt;
                snapshot.cleanup_required = cleanup_debt > 0;
                return Ok(snapshot.clone());
            }
        };
        let total_bytes = filesystems.iter().map(|usage| usage.total_bytes).sum();
        let used_bytes = filesystems
            .iter()
            .map(DiskFilesystemUsage::used_bytes)
            .sum();
        let available_bytes = filesystems.iter().map(|usage| usage.available_bytes).sum();
        let max_ratio = filesystems
            .iter()
            .map(DiskFilesystemUsage::used_ratio)
            .fold(0.0, f64::max);
        let mut current = self.snapshot.write().expect("disk policy lock poisoned");
        let recovering = current.pressure_latched;
        let reclaim_bytes = filesystems
            .iter()
            .map(|usage| {
                let low_bytes = (usage.total_bytes as f64 * self.cleanup_low) as u64;
                usage.used_bytes().saturating_sub(low_bytes)
            })
            .sum();

        let (accepting_sandboxes, reason, cleanup_required) = if max_ratio >= self.admission_hard {
            (false, DiskAdmissionReason::DiskHardLimit, true)
        } else if (recovering && max_ratio > self.cleanup_low) || max_ratio >= self.cleanup_high {
            (false, DiskAdmissionReason::DiskCleanup, true)
        } else if cleanup_debt > 0 {
            // Cleanup obligations are retried in the background. They remain
            // visible, but ordinary delete churn must not close admission.
            (true, DiskAdmissionReason::CleanupDebt, true)
        } else {
            (true, DiskAdmissionReason::Ready, false)
        };

        let next = DiskPolicySnapshot {
            total_bytes,
            used_bytes,
            available_bytes,
            cleanup_debt,
            accepting_sandboxes,
            reason,
            cleanup_required,
            reclaim_bytes,
            pressure_latched: matches!(
                reason,
                DiskAdmissionReason::DiskCleanup | DiskAdmissionReason::DiskHardLimit
            ),
        };
        *current = next.clone();
        Ok(next)
    }

    pub fn snapshot(&self) -> DiskPolicySnapshot {
        let mut snapshot = self
            .snapshot
            .read()
            .expect("disk policy lock poisoned")
            .clone();
        if self.enabled && !self.source.is_fresh() {
            snapshot.accepting_sandboxes = false;
            snapshot.reason = DiskAdmissionReason::DiskUsageUnavailable;
        }
        snapshot
    }

    pub fn required_reclaim_bytes(&self) -> u64 {
        self.snapshot().reclaim_bytes
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[derive(Debug)]
    struct FakeUsage {
        used: AtomicU64,
    }

    impl FakeUsage {
        fn set_percent(&self, percent: u64) {
            self.used.store(percent, Ordering::Relaxed);
        }
    }

    impl DiskUsageSource for FakeUsage {
        fn collect(&self) -> Result<Vec<DiskFilesystemUsage>> {
            Ok(vec![DiskFilesystemUsage {
                device_id: 1,
                total_bytes: 100,
                available_bytes: 100 - self.used.load(Ordering::Relaxed),
            }])
        }
    }

    #[derive(Debug)]
    struct GatedUsage {
        entered: std::sync::mpsc::Sender<()>,
        replies: std::sync::Mutex<std::sync::mpsc::Receiver<i64>>,
    }

    impl DiskUsageSource for GatedUsage {
        fn collect(&self) -> Result<Vec<DiskFilesystemUsage>> {
            self.entered.send(())?;
            let percent = self.replies.lock().unwrap().recv()?;
            anyhow::ensure!(percent >= 0, "probe failed");
            Ok(vec![DiskFilesystemUsage {
                device_id: 1,
                total_bytes: 100,
                available_bytes: 100 - percent as u64,
            }])
        }
    }

    #[tokio::test]
    async fn sampler_fails_closed_without_spawning_more_probes_and_recovers() -> Result<()> {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let sampler = Arc::new(SampledDiskUsage::start(
            Arc::new(GatedUsage {
                entered: entered_tx,
                replies: std::sync::Mutex::new(reply_rx),
            }),
            Duration::from_millis(1),
            Duration::from_millis(50),
        ));
        let controller = DiskPolicyController::new(true, 0.8, 0.7, 0.85, sampler.clone());
        entered_rx.recv_timeout(Duration::from_secs(1))?;
        for _ in 0..100 {
            assert_eq!(
                controller.refresh(0)?.reason,
                DiskAdmissionReason::DiskUsageUnavailable
            );
        }
        assert!(entered_rx.try_recv().is_err());
        reply_tx.send(86)?;
        tokio::time::timeout(Duration::from_secs(1), async {
            while controller.refresh(0).unwrap().reason != DiskAdmissionReason::DiskHardLimit {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        entered_rx.recv_timeout(Duration::from_secs(1))?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            controller.refresh(0)?.reason,
            DiskAdmissionReason::DiskUsageUnavailable
        );
        assert!(entered_rx.try_recv().is_err());
        reply_tx.send(75)?;
        // This slow response is already stale; only a subsequent fresh sample can reopen admission.
        entered_rx.recv_timeout(Duration::from_secs(1))?;
        assert_eq!(
            controller.refresh(0)?.reason,
            DiskAdmissionReason::DiskUsageUnavailable
        );
        reply_tx.send(75)?;
        tokio::time::timeout(Duration::from_secs(1), async {
            while controller.refresh(0).unwrap().reason != DiskAdmissionReason::DiskCleanup {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        entered_rx.recv_timeout(Duration::from_secs(1))?;
        reply_tx.send(60)?;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !controller.refresh(0).unwrap().accepting_sandboxes {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        entered_rx.recv_timeout(Duration::from_secs(1))?;
        reply_tx.send(-1)?;
        tokio::time::timeout(Duration::from_secs(1), async {
            while controller.refresh(0).unwrap().reason != DiskAdmissionReason::DiskUsageUnavailable
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        Ok(())
    }

    #[test]
    fn monitors_custom_pause_store_and_firecracker_work_directory() {
        let mut config = AppConfig::default();
        config.orchestrator.persisted_sandbox_store_path = "/pause-volume/sandboxes".into();
        config.firecracker.work_dir = Some("/runtime-volume/firecracker".into());

        let paths = DiskPolicyController::monitored_paths(&config);
        assert!(paths.contains(&PathBuf::from("/pause-volume/sandboxes")));
        assert!(paths.contains(&PathBuf::from("/runtime-volume/firecracker")));
        assert!(paths.contains(&PathBuf::from(
            "/runtime-volume/firecracker/managed-snapshots"
        )));
    }

    #[test]
    fn monitors_temp_runtime_and_managed_snapshot_fallbacks() {
        let mut config = AppConfig::default();
        config.firecracker.work_dir = None;

        let paths = DiskPolicyController::monitored_paths(&config);
        assert!(paths.contains(&config.orchestrator.persisted_sandbox_store_path));
        assert!(paths.contains(&std::env::temp_dir()));
        assert!(paths.contains(&std::env::temp_dir().join("aenv/managed-snapshots")));
    }

    #[test]
    fn hard_limit_recovers_only_below_low_watermark() {
        let source = Arc::new(FakeUsage {
            used: AtomicU64::new(86),
        });
        let controller = DiskPolicyController::new(true, 0.80, 0.70, 0.85, source.clone());

        let hard = controller.refresh(0).unwrap();
        assert!(!hard.accepting_sandboxes);
        assert_eq!(hard.reason, DiskAdmissionReason::DiskHardLimit);

        source.set_percent(75);
        let recovering = controller.refresh(0).unwrap();
        assert!(!recovering.accepting_sandboxes);
        assert_eq!(recovering.reason, DiskAdmissionReason::DiskCleanup);

        source.set_percent(70);
        let ready = controller.refresh(0).unwrap();
        assert!(ready.accepting_sandboxes);
        assert_eq!(ready.reason, DiskAdmissionReason::Ready);
    }

    #[test]
    fn cleanup_debt_remains_visible_without_blocking_admission() {
        let source = Arc::new(FakeUsage {
            used: AtomicU64::new(60),
        });
        let controller = DiskPolicyController::new(true, 0.80, 0.70, 0.85, source);
        let snapshot = controller.refresh(1).unwrap();
        assert!(snapshot.accepting_sandboxes);
        assert_eq!(snapshot.reason, DiskAdmissionReason::CleanupDebt);
        assert_eq!(snapshot.cleanup_debt, 1);
        assert!(snapshot.cleanup_required);
    }

    #[test]
    fn cleanup_debt_does_not_override_disk_safety_hysteresis() {
        let source = Arc::new(FakeUsage {
            used: AtomicU64::new(86),
        });
        let controller = DiskPolicyController::new(true, 0.80, 0.70, 0.85, source.clone());

        let hard = controller.refresh(1).unwrap();
        assert!(!hard.accepting_sandboxes);
        assert_eq!(hard.reason, DiskAdmissionReason::DiskHardLimit);

        source.set_percent(75);
        let recovering = controller.refresh(1).unwrap();
        assert!(!recovering.accepting_sandboxes);
        assert_eq!(recovering.reason, DiskAdmissionReason::DiskCleanup);

        source.set_percent(60);
        let ready_with_debt = controller.refresh(1).unwrap();
        assert!(ready_with_debt.accepting_sandboxes);
        assert_eq!(ready_with_debt.reason, DiskAdmissionReason::CleanupDebt);
    }

    #[test]
    fn cleanup_high_watermark_closes_admission_until_low_watermark() {
        let source = Arc::new(FakeUsage {
            used: AtomicU64::new(79),
        });
        let controller = DiskPolicyController::new(true, 0.80, 0.70, 0.85, source.clone());

        assert!(controller.refresh(0).unwrap().accepting_sandboxes);

        source.set_percent(80);
        let cleaning = controller.refresh(0).unwrap();
        assert!(!cleaning.accepting_sandboxes);
        assert_eq!(cleaning.reason, DiskAdmissionReason::DiskCleanup);

        source.set_percent(75);
        assert!(!controller.refresh(0).unwrap().accepting_sandboxes);

        source.set_percent(70);
        assert!(controller.refresh(0).unwrap().accepting_sandboxes);
    }

    #[test]
    fn reclaim_target_is_computed_per_filesystem() {
        #[derive(Debug)]
        struct MultipleFilesystems;

        impl DiskUsageSource for MultipleFilesystems {
            fn collect(&self) -> Result<Vec<DiskFilesystemUsage>> {
                Ok(vec![
                    DiskFilesystemUsage {
                        device_id: 1,
                        total_bytes: 100,
                        available_bytes: 10,
                    },
                    DiskFilesystemUsage {
                        device_id: 2,
                        total_bytes: 100,
                        available_bytes: 90,
                    },
                ])
            }
        }

        let controller =
            DiskPolicyController::new(true, 0.80, 0.70, 0.85, Arc::new(MultipleFilesystems));
        controller.refresh(0).unwrap();

        assert_eq!(controller.required_reclaim_bytes(), 20);
    }

    #[test]
    fn loopback_filesystem_crosses_policy_thresholds() -> Result<()> {
        let Ok(root) = std::env::var("AENV_DISK_SAFETY_TEST_ROOT") else {
            return Ok(());
        };
        let root = PathBuf::from(root);
        let source = Arc::new(StatvfsDiskUsageSource {
            paths: vec![root.clone(), root.join("same-filesystem/missing")],
        });
        assert_eq!(source.collect()?.len(), 1);
        let controller = DiskPolicyController::new(true, 0.80, 0.70, 0.85, source.clone());
        let filler_path = root.join("disk-policy-filler");
        let mut filler = std::fs::File::create(&filler_path)?;

        set_filesystem_usage(&source, &mut filler, 0.82)?;
        let cleaning = controller.refresh(0)?;
        assert!(!cleaning.accepting_sandboxes);
        assert_eq!(cleaning.reason, DiskAdmissionReason::DiskCleanup);

        set_filesystem_usage(&source, &mut filler, 0.86)?;
        let hard = controller.refresh(0)?;
        assert!(!hard.accepting_sandboxes);
        assert_eq!(hard.reason, DiskAdmissionReason::DiskHardLimit);

        set_filesystem_usage(&source, &mut filler, 0.75)?;
        assert!(!controller.refresh(0)?.accepting_sandboxes);
        set_filesystem_usage(&source, &mut filler, 0.68)?;
        assert!(controller.refresh(0)?.accepting_sandboxes);

        std::fs::remove_file(filler_path)?;
        Ok(())
    }

    fn set_filesystem_usage(
        source: &StatvfsDiskUsageSource,
        filler: &mut std::fs::File,
        ratio: f64,
    ) -> Result<()> {
        let usage = source.collect()?.pop().expect("one filesystem");
        let target_used = (usage.total_bytes as f64 * ratio) as u64;
        let current_used = usage.used_bytes();
        if target_used > current_used {
            filler.seek(SeekFrom::End(0))?;
            let mut remaining = target_used - current_used;
            let chunk = vec![0u8; 1024 * 1024];
            while remaining > 0 {
                let written = remaining.min(chunk.len() as u64) as usize;
                filler.write_all(&chunk[..written])?;
                remaining -= written as u64;
            }
        } else {
            let length = filler.metadata()?.len();
            filler.set_len(length.saturating_sub(current_used - target_used))?;
        }
        filler.sync_all()?;
        Ok(())
    }
}
