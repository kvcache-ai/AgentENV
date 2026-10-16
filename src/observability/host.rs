use std::cmp;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use nix::sys::statvfs::statvfs;
use tracing::warn;

const FIRST_REQUEST_CPU_SAMPLE_WINDOW: Duration = Duration::from_millis(100);
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiskMetric {
    pub mount_point: String,
    pub device: String,
    pub filesystem_type: String,
    pub used_bytes: u64,
    pub total_bytes: u64,
}

/// Latest sampled host resource snapshot used in node observability responses.
#[derive(Clone, Debug, Default)]
pub struct HostMetrics {
    pub cpu_percent: u32,
    pub cpu_count: u32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disks: Vec<DiskMetric>,
}

/// Request-time collector for host CPU, memory, and disk metrics.
///
/// Each call reads current host state and returns a fresh snapshot.
///
/// CPU utilization uses two samples; on the first call the collector waits
/// `FIRST_REQUEST_CPU_SAMPLE_WINDOW` between samples so it can return a
/// measured value immediately.
#[derive(Clone)]
pub struct HostMetricsCollector {
    previous_cpu: Arc<RwLock<Option<CpuSample>>>,
    cgroup_files: Arc<CgroupFilePaths>,
}

#[derive(Clone, Copy, Debug)]
struct CpuSample {
    total: u64,
    idle: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CgroupMemoryLimit {
    limit_bytes: u64,
    used_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CgroupFilePaths {
    cpu_max: Vec<PathBuf>,
    memory_max: Vec<PathBuf>,
    memory_current: Vec<PathBuf>,
}

impl HostMetricsCollector {
    pub fn new() -> Self {
        Self {
            previous_cpu: Arc::new(RwLock::new(None)),
            cgroup_files: Arc::new(CgroupFilePaths::detect()),
        }
    }

    pub fn collect(&self) -> HostMetrics {
        match self.collect_host_metrics() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                warn!(error = %err, "failed to collect host metrics");
                self.reset_cpu_baseline();
                HostMetrics::default()
            }
        }
    }

    fn reset_cpu_baseline(&self) {
        if let Ok(mut guard) = self.previous_cpu.write() {
            *guard = Self::read_cpu_sample().ok();
        }
    }

    fn collect_host_metrics(&self) -> Result<HostMetrics> {
        let previous_cpu = self.previous_cpu.read().ok().and_then(|guard| *guard);
        let (baseline_cpu, current_cpu) = if let Some(previous) = previous_cpu {
            (previous, Self::read_cpu_sample()?)
        } else {
            // On the first request, take two short-window samples so CPU usage
            // is computed immediately instead of returning a synthetic zero.
            let first = Self::read_cpu_sample()?;
            std::thread::sleep(FIRST_REQUEST_CPU_SAMPLE_WINDOW);
            let second = Self::read_cpu_sample()?;
            (first, second)
        };
        let cpu_percent = cpu_percent_between(baseline_cpu, current_cpu);

        if let Ok(mut guard) = self.previous_cpu.write() {
            *guard = Some(current_cpu);
        }

        let host_cpu_count = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get() as u32)
            .unwrap_or(1);
        let cpu_count = self
            .read_cgroup_cpu_limit_count()
            .map(|limit| limit.min(host_cpu_count).max(1))
            .unwrap_or(host_cpu_count);

        let (host_memory_total_bytes, host_memory_used_bytes) = Self::read_memory_bytes()?;
        let (memory_total_bytes, memory_used_bytes) = self
            .read_cgroup_memory_limit()
            .filter(|limit| limit.limit_bytes <= host_memory_total_bytes)
            .map(|limit| {
                (
                    limit.limit_bytes,
                    limit
                        .used_bytes
                        .unwrap_or(host_memory_used_bytes)
                        .min(limit.limit_bytes),
                )
            })
            .unwrap_or((host_memory_total_bytes, host_memory_used_bytes));

        Ok(HostMetrics {
            cpu_percent,
            cpu_count,
            memory_used_bytes,
            memory_total_bytes,
            disks: Self::read_disk_metrics()?,
        })
    }

    fn read_cpu_sample() -> Result<CpuSample> {
        let content = fs::read_to_string("/proc/stat").context("read /proc/stat")?;
        let line = content
            .lines()
            .find(|line| line.starts_with("cpu "))
            .context("missing aggregate cpu line in /proc/stat")?;

        let values = line
            .split_whitespace()
            .skip(1)
            .map(|raw| raw.parse::<u64>())
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("parse CPU counters from /proc/stat")?;

        let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
        Ok(CpuSample {
            total: values.into_iter().sum(),
            idle,
        })
    }

    fn read_memory_bytes() -> Result<(u64, u64)> {
        let content = fs::read_to_string("/proc/meminfo").context("read /proc/meminfo")?;
        let mut total_kib = None;
        let mut available_kib = None;

        for line in content.lines() {
            if let Some((key, value)) = line.split_once(':') {
                let kib = value
                    .split_whitespace()
                    .next()
                    .context("missing meminfo numeric value")?
                    .parse::<u64>()
                    .with_context(|| format!("parse meminfo value for key {}", key))?;
                match key {
                    "MemTotal" => total_kib = Some(kib),
                    "MemAvailable" => available_kib = Some(kib),
                    _ => {}
                }
            }
        }

        let total = total_kib.context("MemTotal missing in /proc/meminfo")? * 1024;
        let available = available_kib.context("MemAvailable missing in /proc/meminfo")? * 1024;

        Ok((total, total.saturating_sub(available)))
    }

    fn read_cgroup_cpu_limit_count(&self) -> Option<u32> {
        CgroupFilePaths::read_first(&self.cgroup_files.cpu_max)
            .as_deref()
            .and_then(Self::parse_cgroup_v2_cpu_limit)
    }

    fn parse_cgroup_v2_cpu_limit(content: &str) -> Option<u32> {
        let mut fields = content.split_whitespace();
        let quota = fields.next()?;
        if quota == "max" {
            return None;
        }
        let quota = quota.parse::<u64>().ok()?;
        let period = fields.next()?.parse::<u64>().ok()?;
        Self::cpu_quota_to_count(quota, period)
    }

    fn cpu_quota_to_count(quota: u64, period: u64) -> Option<u32> {
        if quota == 0 || period == 0 {
            return None;
        }

        // Heartbeat currently exposes CPU capacity as an integer.  Use the
        // whole-CPU part of a cgroup quota, while still reporting at least one
        // CPU for valid sub-CPU limits such as Kubernetes `500m`.
        Some(cmp::max(quota / period, 1).min(u32::MAX as u64) as u32)
    }

    fn read_cgroup_memory_limit(&self) -> Option<CgroupMemoryLimit> {
        CgroupFilePaths::read_first(&self.cgroup_files.memory_max)
            .as_deref()
            .and_then(|limit| {
                Self::parse_cgroup_memory_limit(limit).map(|limit_bytes| CgroupMemoryLimit {
                    limit_bytes,
                    used_bytes: CgroupFilePaths::read_first(&self.cgroup_files.memory_current)
                        .as_deref()
                        .and_then(Self::parse_cgroup_memory_usage),
                })
            })
    }

    fn parse_cgroup_memory_limit(content: &str) -> Option<u64> {
        let value = content.trim();
        if value == "max" {
            return None;
        }
        value.parse::<u64>().ok().filter(|limit| *limit > 0)
    }

    fn parse_cgroup_memory_usage(content: &str) -> Option<u64> {
        content.trim().parse::<u64>().ok()
    }

    fn read_disk_metrics() -> Result<Vec<DiskMetric>> {
        let mounts = parse_mounts(
            &fs::read_to_string("/proc/self/mounts").context("read /proc/self/mounts")?,
        );
        let mut seen_mount_points = HashSet::new();
        let mut disks = Vec::new();

        for mount in mounts {
            if !seen_mount_points.insert(mount.mount_point.clone()) {
                continue;
            }
            if !Self::is_real_mount(&mount) {
                continue;
            }

            let Ok(stats) = statvfs(Path::new(&mount.mount_point)) else {
                continue;
            };

            let block_size = stats.fragment_size();
            let total_bytes = stats.blocks().saturating_mul(block_size);
            let free_bytes = stats.blocks_free().saturating_mul(block_size);
            disks.push(DiskMetric {
                mount_point: mount.mount_point,
                device: mount.device,
                filesystem_type: mount.filesystem_type,
                used_bytes: total_bytes.saturating_sub(free_bytes),
                total_bytes,
            });
        }

        Ok(disks)
    }

    fn is_real_mount(mount: &MountEntry) -> bool {
        matches!(
            mount.filesystem_type.as_str(),
            "ext2" | "ext3" | "ext4" | "xfs" | "btrfs" | "overlay" | "tmpfs"
        ) && (mount.device.starts_with('/') || mount.filesystem_type == "tmpfs")
    }
}

impl Default for HostMetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CgroupFilePaths {
    fn detect() -> Self {
        Self::from_proc_self_cgroup(
            &fs::read_to_string(PROC_SELF_CGROUP).unwrap_or_default(),
            CGROUP_ROOT,
        )
    }

    fn from_proc_self_cgroup(content: &str, cgroup_root: impl AsRef<Path>) -> Self {
        Self {
            cpu_max: cgroup_file_candidates(content, cgroup_root.as_ref(), "cpu.max"),
            memory_max: cgroup_file_candidates(content, cgroup_root.as_ref(), "memory.max"),
            memory_current: cgroup_file_candidates(content, cgroup_root.as_ref(), "memory.current"),
        }
    }

    fn read_first(paths: &[PathBuf]) -> Option<String> {
        paths.iter().find_map(|path| fs::read_to_string(path).ok())
    }
}

fn cgroup_file_candidates(
    proc_self_cgroup: &str,
    cgroup_root: &Path,
    file_name: &str,
) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for line in proc_self_cgroup.lines() {
        let mut fields = line.splitn(3, ':');
        let _hierarchy_id = fields.next();
        let controllers = fields.next().unwrap_or_default();
        let cgroup_path = fields.next().unwrap_or_default();

        if controllers.is_empty() {
            candidates.push(cgroup_path_join(cgroup_root, cgroup_path, file_name));
        }
    }

    candidates
}

fn cgroup_path_join(base: &Path, cgroup_path: &str, file_name: &str) -> PathBuf {
    let relative = cgroup_path.trim_start_matches('/');
    if relative.is_empty() {
        base.join(file_name)
    } else {
        base.join(relative).join(file_name)
    }
}

fn cpu_percent_between(previous: CpuSample, current: CpuSample) -> u32 {
    let delta_total = current.total.saturating_sub(previous.total);
    if delta_total == 0 {
        return 0;
    }

    let delta_idle = current.idle.saturating_sub(previous.idle);
    let busy = delta_total.saturating_sub(delta_idle);
    ((busy as f64 / delta_total as f64) * 100.0).trunc() as u32
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MountEntry {
    device: String,
    mount_point: String,
    filesystem_type: String,
}

fn parse_mounts(content: &str) -> Vec<MountEntry> {
    content
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let device = decode_mount_field(fields.next()?);
            let mount_point = decode_mount_field(fields.next()?);
            let filesystem_type = fields.next()?.to_string();
            Some(MountEntry {
                device,
                mount_point,
                filesystem_type,
            })
        })
        .collect()
}

fn decode_mount_field(raw: &str) -> String {
    let mut buf: Vec<u8> = Vec::with_capacity(raw.len());
    let bytes = raw.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && bytes[index + 1].is_ascii_digit()
            && bytes[index + 2].is_ascii_digit()
            && bytes[index + 3].is_ascii_digit()
        {
            let octal = &raw[index + 1..index + 4];
            if let Ok(value) = u8::from_str_radix(octal, 8) {
                buf.push(value);
                index += 4;
                continue;
            }
        }

        buf.push(bytes[index]);
        index += 1;
    }

    String::from_utf8_lossy(&buf).into_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        cgroup_file_candidates, cpu_percent_between, decode_mount_field, parse_mounts,
        CgroupFilePaths, CpuSample, HostMetricsCollector, MountEntry,
    };

    #[test]
    fn cpu_percent_between_uses_busy_delta() {
        let previous = CpuSample {
            total: 100,
            idle: 20,
        };
        let current = CpuSample {
            total: 180,
            idle: 40,
        };

        assert_eq!(cpu_percent_between(previous, current), 75);
    }

    #[test]
    fn parse_cgroup_v2_cpu_limit_reads_quota() {
        assert_eq!(
            HostMetricsCollector::parse_cgroup_v2_cpu_limit("250000 100000"),
            Some(2)
        );
        assert_eq!(
            HostMetricsCollector::parse_cgroup_v2_cpu_limit("50000 100000"),
            Some(1)
        );
        assert_eq!(
            HostMetricsCollector::parse_cgroup_v2_cpu_limit("max 100000"),
            None
        );
    }

    #[test]
    fn parse_cgroup_memory_limit_ignores_unlimited() {
        assert_eq!(
            HostMetricsCollector::parse_cgroup_memory_limit("1073741824"),
            Some(1073741824)
        );
        assert_eq!(HostMetricsCollector::parse_cgroup_memory_limit("max"), None);
    }

    #[test]
    fn cgroup_file_candidates_resolves_v2_path_once() {
        let paths = cgroup_file_candidates(
            "0::/kubepods.slice/pod123/container456\n",
            std::path::Path::new("/sys/fs/cgroup"),
            "cpu.max",
        );
        assert_eq!(
            paths,
            vec![std::path::PathBuf::from(
                "/sys/fs/cgroup/kubepods.slice/pod123/container456/cpu.max"
            )]
        );
    }

    #[test]
    fn cgroup_file_paths_detects_v2_files_from_cached_proc_cgroup_content() {
        let paths = CgroupFilePaths::from_proc_self_cgroup(
            "0::/kubepods.slice/pod-a/container-a\n",
            "/sys/fs/cgroup",
        );

        assert_eq!(
            paths.cpu_max,
            vec![std::path::PathBuf::from(
                "/sys/fs/cgroup/kubepods.slice/pod-a/container-a/cpu.max"
            )]
        );
        assert_eq!(
            paths.memory_current,
            vec![std::path::PathBuf::from(
                "/sys/fs/cgroup/kubepods.slice/pod-a/container-a/memory.current"
            )]
        );
    }

    #[test]
    fn decode_mount_field_unescapes_spaces() {
        assert_eq!(decode_mount_field("/var/lib/my\\040dir"), "/var/lib/my dir");
    }

    #[test]
    fn decode_mount_field_handles_multibyte_utf8_octal_escapes() {
        // 'é' is U+00E9, encoded in UTF-8 as 0xC3 0xA9 → octal \303\251
        assert_eq!(decode_mount_field("/mnt/caf\\303\\251"), "/mnt/café");
    }

    #[test]
    fn parse_mounts_extracts_device_mount_and_fs_type() {
        let mounts = parse_mounts("/dev/root / ext4 rw 0 0\nproc /proc proc rw 0 0\n");
        assert_eq!(
            mounts,
            vec![
                MountEntry {
                    device: "/dev/root".to_string(),
                    mount_point: "/".to_string(),
                    filesystem_type: "ext4".to_string(),
                },
                MountEntry {
                    device: "proc".to_string(),
                    mount_point: "/proc".to_string(),
                    filesystem_type: "proc".to_string(),
                },
            ]
        );
    }
}
