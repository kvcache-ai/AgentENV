//! Runtime CPU-affinity control for a Firecracker process.
//!
//! This module scans procfs, validates bounded CPU lists with ranges and
//! optional strides, stops the complete Firecracker thread group while numeric
//! TIDs are used, applies affinity one thread at a time, verifies every write,
//! and restores earlier changes when a later write fails.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use nix::sched::{sched_getaffinity, sched_setaffinity, CpuSet};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

const MAX_CPU_LIST_BYTES: usize = 16 * 1024;
const MAX_CPU_LIST_VALUES: usize = 4096;
const MAX_CPU_LIST_EXPANSION: u64 = 65_536;
const THREAD_GROUP_STOP_TIMEOUT: Duration = Duration::from_secs(1);
const THREAD_GROUP_STOP_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpuAffinityRequest {
    pub vcpu: String,
    pub core: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpuAffinityOutcome {
    pub vcpu: String,
    pub cores: String,
    pub ignored_offline_cores: String,
    pub bound_thread_count: u32,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CpuAffinityError {
    #[error("{0}")]
    InvalidRequest(String),
    #[error("{0:#}")]
    Operation(#[source] anyhow::Error),
}

impl CpuAffinityError {
    fn invalid(error: anyhow::Error) -> Self {
        Self::InvalidRequest(format!("{error:#}"))
    }
}

#[derive(Debug)]
struct ThreadInfo {
    tid: i32,
    comm: String,
    state: char,
    starttime: u64,
}

impl ThreadInfo {
    /// Parse `/proc/PID/task/TID/stat`. Linux thread names may contain spaces
    /// and `)`, so the final `") "` is the only safe command delimiter.
    fn from_stat(expected_tid: i32, stat: &str) -> Result<Self> {
        let (identity, fields) = stat
            .trim_end()
            .rsplit_once(") ")
            .context("thread stat has no closing command delimiter")?;
        let (raw_tid, comm) = identity
            .split_once(" (")
            .context("thread stat has no opening command delimiter")?;
        let tid: i32 = raw_tid
            .parse()
            .with_context(|| format!("invalid thread ID {raw_tid:?} in stat"))?;
        if tid != expected_tid {
            bail!("thread stat ID changed from {expected_tid} to {tid}");
        }

        // `fields` starts at field 3 (`state`); starttime is field 22.
        let mut fields = fields.split_whitespace();
        let raw_state = fields.next().context("thread stat has no state field")?;
        let state = match raw_state.as_bytes() {
            [state] => char::from(*state),
            _ => bail!("invalid thread state {raw_state:?}"),
        };
        let raw_starttime = fields
            .nth(18)
            .context("thread stat has no starttime field")?;
        let starttime = raw_starttime
            .parse()
            .with_context(|| format!("invalid thread starttime {raw_starttime:?}"))?;

        Ok(Self {
            tid,
            comm: comm.to_owned(),
            state,
            starttime,
        })
    }

    fn is_stopped(&self) -> bool {
        matches!(self.state, 'T' | 't')
    }

    /// Re-read starttime around numeric-TID syscalls to narrow the window in
    /// which an exited TID could refer to a different thread.
    fn verify_identity(&self, pid: i32) -> Result<()> {
        let path = format!("/proc/{pid}/task/{}/stat", self.tid);
        let stat = fs::read_to_string(&path)
            .with_context(|| format!("failed to re-read {path}; thread may have exited"))?;
        let current = Self::from_stat(self.tid, &stat)
            .with_context(|| format!("failed to verify identity of tid={}", self.tid))?;
        if current.starttime != self.starttime {
            bail!(
                "tid={} identity changed during CPU-affinity operation (start time {} -> {})",
                self.tid,
                self.starttime,
                current.starttime
            );
        }
        Ok(())
    }

    /// Read this thread's affinity while checking that its numeric TID still
    /// identifies the thread discovered during the procfs scan.
    fn read_affinity(&self, pid: i32) -> Result<CpuSet> {
        self.verify_identity(pid)?;
        let affinity = sched_getaffinity(Pid::from_raw(self.tid)).with_context(|| {
            format!(
                "failed to read affinity of tid={} ({:?})",
                self.tid, self.comm
            )
        })?;
        self.verify_identity(pid)?;
        Ok(affinity)
    }
}

/// Keeps a process-wide SIGSTOP paired with SIGCONT on every normal unwind
/// path. The explicit `resume` call reports delivery failures; `Drop` is a
/// final best-effort safeguard for early returns and panics.
struct StoppedThreadGroup {
    pid: Pid,
    resumed: bool,
}

impl StoppedThreadGroup {
    fn stop(pid: i32) -> Result<Self> {
        let threads = list_threads(pid)?;
        if threads.iter().any(ThreadInfo::is_stopped) {
            bail!("Firecracker pid {pid} is already stopped");
        }

        let pid = Pid::from_raw(pid);
        kill(pid, Signal::SIGSTOP)
            .with_context(|| format!("failed to send SIGSTOP to Firecracker pid {pid}"))?;

        let guard = Self {
            pid,
            resumed: false,
        };
        let started = Instant::now();
        loop {
            let threads = list_threads(pid.as_raw())
                .with_context(|| format!("failed to confirm that Firecracker pid {pid} stopped"))?;
            if threads.iter().all(ThreadInfo::is_stopped) {
                return Ok(guard);
            }

            let elapsed = started.elapsed();
            if elapsed >= THREAD_GROUP_STOP_TIMEOUT {
                let active = threads
                    .iter()
                    .filter(|thread| !thread.is_stopped())
                    .map(|thread| format!("{}:{:?}:{}", thread.tid, thread.comm, thread.state))
                    .collect::<Vec<_>>()
                    .join(", ");
                bail!(
                    "Firecracker pid {pid} did not fully stop within {} ms; active threads: {active}",
                    THREAD_GROUP_STOP_TIMEOUT.as_millis()
                );
            }

            std::thread::sleep(THREAD_GROUP_STOP_POLL_INTERVAL);
        }
    }

    fn resume(mut self) -> Result<()> {
        kill(self.pid, Signal::SIGCONT)
            .with_context(|| format!("failed to send SIGCONT to Firecracker pid {}", self.pid))?;
        self.resumed = true;
        Ok(())
    }
}

impl Drop for StoppedThreadGroup {
    fn drop(&mut self) {
        if !self.resumed {
            let _ = kill(self.pid, Signal::SIGCONT);
        }
    }
}

pub(crate) fn bind_process(
    pid: i32,
    request: CpuAffinityRequest,
) -> std::result::Result<CpuAffinityOutcome, CpuAffinityError> {
    if pid <= 0 {
        return Err(CpuAffinityError::Operation(anyhow!(
            "invalid Firecracker pid {pid}"
        )));
    }

    if request.vcpu.len() > MAX_CPU_LIST_BYTES {
        return Err(CpuAffinityError::InvalidRequest(format!(
            "vcpu list is too long (maximum: {MAX_CPU_LIST_BYTES} bytes)"
        )));
    }
    if request.core.len() > MAX_CPU_LIST_BYTES {
        return Err(CpuAffinityError::InvalidRequest(format!(
            "core list is too long (maximum: {MAX_CPU_LIST_BYTES} bytes)"
        )));
    }

    let vcpu = request.vcpu.trim().to_owned();
    let core = request.core.trim();
    // nix::CpuSet wraps libc's fixed-size cpu_set_t. On supported Linux
    // targets, this limits CPU IDs to 0-1023.
    let max_cpu = CpuSet::count()
        .checked_sub(1)
        .context("cpu_set_t cannot represent any CPUs")
        .and_then(|value| {
            u32::try_from(value).context("cpu_set_t exceeds supported CPU identifiers")
        })
        .map_err(CpuAffinityError::Operation)?;
    let requested = parse_cpu_list(core, max_cpu)
        .with_context(|| format!("invalid core value {core:?}"))
        .map_err(CpuAffinityError::invalid)?;

    let online_raw = fs::read_to_string("/sys/devices/system/cpu/online")
        .context("failed to read /sys/devices/system/cpu/online")
        .map_err(CpuAffinityError::Operation)?;
    // The host may expose CPU IDs beyond libc's fixed-size cpu_set_t. Parse
    // the trusted sysfs value as ranges without expanding it, then consider
    // only the already-bounded CPUs requested by the caller.
    let online = parse_cpu_ranges(&online_raw)
        .context("invalid /sys/devices/system/cpu/online value")
        .map_err(CpuAffinityError::Operation)?;
    let (cores, ignored) = partition_online_cpus(&requested, &online);
    if cores.is_empty() {
        return Err(CpuAffinityError::InvalidRequest(format!(
            "requested cores {} do not intersect online cores {}",
            format_cpu_list(&requested),
            online_raw.trim()
        )));
    }

    let mut desired = CpuSet::new();
    for &cpu in &cores {
        desired
            .set(cpu as usize)
            .with_context(|| format!("core {cpu} does not fit in cpu_set_t"))
            .map_err(CpuAffinityError::Operation)?;
    }

    let cores = format_cpu_list(&cores);
    let stopped = StoppedThreadGroup::stop(pid)
        .context("failed to stop Firecracker for CPU-affinity update")
        .map_err(CpuAffinityError::Operation)?;
    let outcome = bind_stopped_process(pid, vcpu, cores, ignored, &desired);

    if let Err(error) = stopped.resume() {
        let context = match &outcome {
            Ok(_) => "CPU affinity was applied, but Firecracker could not be resumed".to_string(),
            Err(bind_error) => format!(
                "CPU-affinity operation failed ({bind_error:#}); Firecracker also could not be resumed"
            ),
        };
        return Err(CpuAffinityError::Operation(error.context(context)));
    }

    outcome
}

/// Enumerate and update tasks only after the complete thread group has entered
/// a stopped state. This prevents normal Firecracker execution from exiting a
/// worker and reusing its numeric TID during the affinity syscalls.
fn bind_stopped_process(
    pid: i32,
    vcpu: String,
    cores: String,
    ignored: Vec<u32>,
    desired: &CpuSet,
) -> std::result::Result<CpuAffinityOutcome, CpuAffinityError> {
    let threads = list_threads(pid)
        .with_context(|| format!("failed to enumerate threads of Firecracker pid {pid}"))
        .map_err(CpuAffinityError::Operation)?;
    let process = threads
        .iter()
        .find(|thread| thread.tid == pid)
        .ok_or_else(|| {
            CpuAffinityError::Operation(anyhow!(
                "Firecracker pid {pid} has no process-leader thread"
            ))
        })?;
    process
        .verify_identity(pid)
        .context("failed to verify Firecracker process identity")
        .map_err(CpuAffinityError::Operation)?;
    let targets = select_threads(pid, &vcpu, &threads)?;

    bind_threads(pid, process, &targets, desired, &cores).map_err(CpuAffinityError::Operation)?;
    let bound_thread_count = u32::try_from(targets.len())
        .context("bound thread count does not fit in u32")
        .map_err(CpuAffinityError::Operation)?;

    Ok(CpuAffinityOutcome {
        vcpu,
        cores,
        ignored_offline_cores: format_cpu_list(&ignored),
        bound_thread_count,
    })
}

fn select_threads<'a>(
    pid: i32,
    spec: &str,
    threads: &'a [ThreadInfo],
) -> std::result::Result<Vec<&'a ThreadInfo>, CpuAffinityError> {
    if spec == "*" {
        return Ok(threads.iter().collect());
    }

    let mut indexed = BTreeMap::new();
    for thread in threads {
        let Some(index) = fc_vcpu_index(&thread.comm) else {
            continue;
        };
        if indexed.insert(index, thread).is_some() {
            return Err(CpuAffinityError::Operation(anyhow!(
                "pid {pid} has duplicate fc_vcpu thread index {index}"
            )));
        }
    }
    if indexed.is_empty() {
        return Err(CpuAffinityError::Operation(anyhow!(
            "pid {pid} has no fc_vcpu threads"
        )));
    }

    let max_vcpu = indexed
        .last_key_value()
        .map(|(&index, _)| index)
        .ok_or_else(|| CpuAffinityError::Operation(anyhow!("vCPU index set unexpectedly empty")))?;
    let requested = parse_cpu_list(spec, max_vcpu)
        .with_context(|| format!("invalid vcpu value {spec:?}"))
        .map_err(CpuAffinityError::invalid)?;
    let missing: Vec<u32> = requested
        .iter()
        .copied()
        .filter(|index| !indexed.contains_key(index))
        .collect();
    if !missing.is_empty() {
        let available: Vec<u32> = indexed.keys().copied().collect();
        return Err(CpuAffinityError::InvalidRequest(format!(
            "vcpu(s) {} not found in pid {pid}; available vcpus: {}",
            format_cpu_list(&missing),
            format_cpu_list(&available)
        )));
    }

    Ok(requested
        .iter()
        .filter_map(|index| indexed.get(index).copied())
        .collect())
}

/// Parse a comma/range CPU list while bounding input length, expansion work,
/// allocation size, and the largest accepted value before any range expands.
fn parse_cpu_list(spec: &str, max_value: u32) -> Result<Vec<u32>> {
    let spec = spec.trim();
    if spec.is_empty() {
        bail!("empty cpu list");
    }
    if spec.len() > MAX_CPU_LIST_BYTES {
        bail!("cpu list is too long (maximum: {MAX_CPU_LIST_BYTES} bytes)");
    }

    let mut values = BTreeSet::new();
    let mut expansion = 0u64;
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            bail!("empty element in cpu list {spec:?}");
        }

        if let Some((lo, range_end)) = part.split_once('-') {
            let lo: u32 = lo
                .trim()
                .parse()
                .with_context(|| format!("invalid range start in {part:?}"))?;
            let (hi, stride) = match range_end.split_once(':') {
                Some((hi, stride)) => {
                    let stride: u32 = stride
                        .trim()
                        .parse()
                        .with_context(|| format!("invalid range stride in {part:?}"))?;
                    if stride == 0 {
                        bail!("range stride must be greater than zero in {part:?}");
                    }
                    (hi, stride)
                }
                None => (range_end, 1),
            };
            let hi: u32 = hi
                .trim()
                .parse()
                .with_context(|| format!("invalid range end in {part:?}"))?;
            if lo > hi {
                bail!("descending range {part:?}");
            }
            if hi > max_value {
                bail!("cpu {hi} exceeds maximum allowed value {max_value}");
            }

            let width = (u64::from(hi) - u64::from(lo)) / u64::from(stride) + 1;
            if width > MAX_CPU_LIST_VALUES as u64 {
                bail!("range {part:?} contains more than {MAX_CPU_LIST_VALUES} values");
            }
            expansion = expansion
                .checked_add(width)
                .context("cpu list expansion size overflow")?;
            if expansion > MAX_CPU_LIST_EXPANSION {
                bail!("cpu list expands to more than {MAX_CPU_LIST_EXPANSION} values");
            }
            let stride = usize::try_from(stride).context("range stride does not fit usize")?;
            values.extend((lo..=hi).step_by(stride));
        } else {
            let value: u32 = part
                .parse()
                .with_context(|| format!("invalid cpu number {part:?}"))?;
            if value > max_value {
                bail!("cpu {value} exceeds maximum allowed value {max_value}");
            }
            expansion = expansion
                .checked_add(1)
                .context("cpu list expansion size overflow")?;
            if expansion > MAX_CPU_LIST_EXPANSION {
                bail!("cpu list expands to more than {MAX_CPU_LIST_EXPANSION} values");
            }
            values.insert(value);
        }

        if values.len() > MAX_CPU_LIST_VALUES {
            bail!("cpu list contains more than {MAX_CPU_LIST_VALUES} unique values");
        }
    }
    Ok(values.into_iter().collect())
}

/// Parse a trusted kernel CPU-list as non-expanded inclusive ranges. This
/// keeps hosts with CPU IDs beyond cpu_set_t usable for lower representable
/// CPUs and avoids allocating once per online CPU.
fn parse_cpu_ranges(spec: &str) -> Result<Vec<(u32, u32)>> {
    let spec = spec.trim();
    if spec.is_empty() {
        bail!("empty cpu list");
    }
    if spec.len() > MAX_CPU_LIST_BYTES {
        bail!("cpu list is too long (maximum: {MAX_CPU_LIST_BYTES} bytes)");
    }

    let mut ranges = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            bail!("empty element in cpu list {spec:?}");
        }
        let (lo, hi) = match part.split_once('-') {
            Some((lo, hi)) => {
                let lo = lo
                    .trim()
                    .parse::<u32>()
                    .with_context(|| format!("invalid range start in {part:?}"))?;
                let hi = hi
                    .trim()
                    .parse::<u32>()
                    .with_context(|| format!("invalid range end in {part:?}"))?;
                if lo > hi {
                    bail!("descending range {part:?}");
                }
                (lo, hi)
            }
            None => {
                let value = part
                    .parse::<u32>()
                    .with_context(|| format!("invalid cpu number {part:?}"))?;
                (value, value)
            }
        };
        ranges.push((lo, hi));
        if ranges.len() > MAX_CPU_LIST_VALUES {
            bail!("cpu list contains more than {MAX_CPU_LIST_VALUES} ranges");
        }
    }
    Ok(ranges)
}

fn partition_online_cpus(requested: &[u32], online: &[(u32, u32)]) -> (Vec<u32>, Vec<u32>) {
    requested
        .iter()
        .copied()
        .partition(|cpu| online.iter().any(|&(lo, hi)| lo <= *cpu && *cpu <= hi))
}

fn format_cpu_list(cpus: &[u32]) -> String {
    let mut parts = Vec::new();
    let mut values = cpus.iter().copied().peekable();
    while let Some(start) = values.next() {
        let mut end = start;
        while end < u32::MAX && values.peek() == Some(&(end + 1)) {
            end = values.next().expect("peeked value must exist");
        }
        if start == end {
            parts.push(start.to_string());
        } else {
            parts.push(format!("{start}-{end}"));
        }
    }
    parts.join(",")
}

/// Collect a best-effort task snapshot. A task disappearing during the scan is
/// normal; all other I/O and stat-format errors fail closed.
fn list_threads(pid: i32) -> Result<Vec<ThreadInfo>> {
    let directory = format!("/proc/{pid}/task");
    let entries = fs::read_dir(&directory)
        .with_context(|| format!("failed to open {directory} (process alive and permitted?)"))?;
    let mut threads = Vec::new();

    for entry in entries {
        let entry = entry.with_context(|| format!("failed to enumerate {directory}"))?;
        let tid: i32 = match entry.file_name().to_string_lossy().parse() {
            Ok(tid) => tid,
            Err(_) => continue,
        };
        let path = format!("/proc/{pid}/task/{tid}/stat");
        let stat = match fs::read_to_string(&path) {
            Ok(stat) => stat,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error).with_context(|| format!("failed to read {path}")),
        };
        threads.push(
            ThreadInfo::from_stat(tid, &stat).with_context(|| format!("failed to parse {path}"))?,
        );
    }

    if threads.is_empty() {
        bail!("no threads found under {directory}");
    }
    threads.sort_by_key(|thread| (fc_vcpu_index(&thread.comm).unwrap_or(u32::MAX), thread.tid));
    Ok(threads)
}

fn fc_vcpu_index(comm: &str) -> Option<u32> {
    comm.strip_prefix("fc_vcpu")?.trim().parse().ok()
}

fn format_cpu_set(set: &CpuSet) -> String {
    let cpus: Vec<u32> = (0..CpuSet::count())
        .filter_map(|index| match set.is_set(index) {
            Ok(true) => u32::try_from(index).ok(),
            Ok(false) | Err(_) => None,
        })
        .collect();
    format_cpu_list(&cpus)
}

/// While the caller keeps the complete thread group stopped, apply affinity as
/// a best-effort transaction and roll back earlier updates on failure.
fn bind_threads(
    pid: i32,
    process: &ThreadInfo,
    targets: &[&ThreadInfo],
    desired: &CpuSet,
    cores: &str,
) -> Result<()> {
    let mut originals = Vec::with_capacity(targets.len());
    for &thread in targets {
        process
            .verify_identity(pid)
            .context("Firecracker process identity changed during affinity preflight")?;
        originals.push((thread, thread.read_affinity(pid)?));
        process
            .verify_identity(pid)
            .context("Firecracker process identity changed during affinity preflight")?;
    }

    for (index, (thread, _)) in originals.iter().enumerate() {
        if let Err(error) = process.verify_identity(pid) {
            return Err(error_after_rollback(
                pid,
                process,
                format!("Firecracker process identity check failed: {error:#}"),
                &originals[..index],
            ));
        }
        if let Err(error) = thread.verify_identity(pid) {
            return Err(error_after_rollback(
                pid,
                process,
                format!("thread identity check failed: {error:#}"),
                &originals[..index],
            ));
        }
        if let Err(error) = sched_setaffinity(Pid::from_raw(thread.tid), desired) {
            return Err(error_after_rollback(
                pid,
                process,
                format!(
                    "failed to bind tid={} ({:?}) to cores {cores}: {error}",
                    thread.tid, thread.comm
                ),
                &originals[..index],
            ));
        }
        let applied = index + 1;

        let actual = match thread.read_affinity(pid) {
            Ok(actual) => actual,
            Err(error) => {
                return Err(error_after_rollback(
                    pid,
                    process,
                    format!(
                        "bound tid={} ({:?}) but failed to verify its affinity: {error:#}",
                        thread.tid, thread.comm
                    ),
                    &originals[..applied],
                ));
            }
        };
        if actual != *desired {
            return Err(error_after_rollback(
                pid,
                process,
                format!(
                    "kernel restricted affinity of tid={} ({:?}): requested {cores}, actual {}",
                    thread.tid,
                    thread.comm,
                    format_cpu_set(&actual)
                ),
                &originals[..applied],
            ));
        }
        if let Err(error) = process.verify_identity(pid) {
            return Err(error_after_rollback(
                pid,
                process,
                format!("Firecracker process identity check failed: {error:#}"),
                &originals[..applied],
            ));
        }
    }
    Ok(())
}

fn error_after_rollback(
    pid: i32,
    process: &ThreadInfo,
    reason: String,
    applied: &[(&ThreadInfo, CpuSet)],
) -> anyhow::Error {
    if applied.is_empty() {
        return anyhow!("{reason}; no affinity changes were applied");
    }

    if let Err(error) = process.verify_identity(pid) {
        return anyhow!(
            "{reason}; rollback skipped because Firecracker process identity check failed: {error:#}"
        );
    }

    let mut failures = Vec::new();
    for (thread, original) in applied.iter().rev() {
        if let Err(error) = process.verify_identity(pid) {
            failures.push(format!("process identity check failed: {error:#}"));
            break;
        }
        if let Err(error) = thread.verify_identity(pid) {
            failures.push(format!(
                "tid={} (identity check failed: {error:#})",
                thread.tid
            ));
            continue;
        }
        match sched_setaffinity(Pid::from_raw(thread.tid), original) {
            Err(error) => failures.push(format!("tid={} (restore failed: {error})", thread.tid)),
            Ok(()) => match thread.read_affinity(pid) {
                Ok(actual) => {
                    if actual != *original {
                        failures.push(format!(
                            "tid={} (restore verification mismatch: {})",
                            thread.tid,
                            format_cpu_set(&actual)
                        ));
                    }
                }
                Err(error) => failures.push(format!(
                    "tid={} (restore verification failed: {error})",
                    thread.tid
                )),
            },
        }
    }

    if failures.is_empty() {
        anyhow!("{reason}; rolled back {} thread(s)", applied.len())
    } else {
        anyhow!("{reason}; rollback incomplete for {}", failures.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command, Stdio};
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    const AFFINITY_TEST_WORKERS: [&str; 2] = ["aenv-affinity-0", "aenv-affinity-1"];

    struct ChildGuard(Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn thread(tid: i32, comm: &str) -> ThreadInfo {
        ThreadInfo {
            tid,
            comm: comm.to_string(),
            state: 'S',
            starttime: 1,
        }
    }

    fn wait_until_running(pid: i32) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let threads = list_threads(pid).unwrap();
            if threads.iter().all(|thread| !thread.is_stopped()) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "test process remained stopped after SIGCONT"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn cpu_list_accepts_ranges_whitespace_and_duplicates() {
        assert_eq!(parse_cpu_list("0", 4).unwrap(), vec![0]);
        assert_eq!(parse_cpu_list("0,1-2,2,4", 4).unwrap(), vec![0, 1, 2, 4]);
        assert_eq!(parse_cpu_list(" 3 , 1 ", 4).unwrap(), vec![1, 3]);
        assert_eq!(
            parse_cpu_list("0-10:2", 10).unwrap(),
            vec![0, 2, 4, 6, 8, 10]
        );
        assert_eq!(parse_cpu_list("0-10:3", 10).unwrap(), vec![0, 3, 6, 9]);
    }

    #[test]
    fn cpu_list_rejects_invalid_and_pathological_ranges() {
        for value in [
            "", "a", "1,", "2-1", "1-x", "1:2", "0-10:", "0-10:0", "0-10:x",
        ] {
            assert!(parse_cpu_list(value, 4).is_err(), "accepted {value:?}");
        }
        assert!(parse_cpu_list("0-4294967295", u32::MAX).is_err());
        assert_eq!(
            parse_cpu_list("0-4294967295:4294967295", u32::MAX).unwrap(),
            vec![0, u32::MAX]
        );
        assert!(parse_cpu_list("1024", 1023).is_err());
    }

    #[test]
    fn bind_request_rejects_oversized_fields_before_procfs_work() {
        let oversized = "0".repeat(MAX_CPU_LIST_BYTES + 1);
        let request = CpuAffinityRequest {
            vcpu: oversized.clone(),
            core: "0".to_string(),
        };
        assert!(matches!(
            bind_process(i32::MAX, request),
            Err(CpuAffinityError::InvalidRequest(_))
        ));

        let request = CpuAffinityRequest {
            vcpu: "*".to_string(),
            core: oversized,
        };
        assert!(matches!(
            bind_process(i32::MAX, request),
            Err(CpuAffinityError::InvalidRequest(_))
        ));
    }

    #[test]
    fn cpu_list_format_is_compact_and_overflow_safe() {
        assert_eq!(format_cpu_list(&[0, 1, 2, 4]), "0-2,4");
        assert_eq!(
            format_cpu_list(&[u32::MAX - 1, u32::MAX]),
            "4294967294-4294967295"
        );
    }

    #[test]
    fn kernel_cpu_ranges_do_not_expand_and_partition_requested_values() {
        let online = parse_cpu_ranges("0-8191,10000").unwrap();
        assert_eq!(online, vec![(0, 8191), (10000, 10000)]);
        assert_eq!(
            partition_online_cpus(&[0, 1023, 9000, 10000], &online),
            (vec![0, 1023, 10000], vec![9000])
        );
        assert_eq!(
            partition_online_cpus(&[9000], &online),
            (Vec::new(), vec![9000])
        );
        assert!(parse_cpu_ranges("4-2").is_err());
    }

    #[test]
    fn stat_parser_handles_spaces_and_parentheses_in_thread_name() {
        let stat = "123 (worker ) name) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 987";
        let thread = ThreadInfo::from_stat(123, stat).unwrap();
        assert_eq!(thread.comm, "worker ) name");
        assert_eq!(thread.state, 'S');
        assert_eq!(thread.starttime, 987);
        assert!(ThreadInfo::from_stat(124, stat).is_err());
    }

    #[test]
    fn firecracker_vcpu_names_are_recognized_without_matching_helpers() {
        assert_eq!(fc_vcpu_index("fc_vcpu 0"), Some(0));
        assert_eq!(fc_vcpu_index("fc_vcpu15"), Some(15));
        assert_eq!(fc_vcpu_index("fc_vmm"), None);
        assert_eq!(fc_vcpu_index("kvm-nx-lpage-re"), None);
    }

    #[test]
    fn thread_selection_distinguishes_numeric_vcpus_from_wildcard() {
        let threads = vec![
            thread(10, "fc_vmm"),
            thread(11, "fc_vcpu 0"),
            thread(12, "fc_vcpu1"),
            thread(13, "kvm-nx-lpage-re"),
        ];

        let selected = select_threads(99, "1,0,1", &threads).unwrap();
        assert_eq!(
            selected.iter().map(|thread| thread.tid).collect::<Vec<_>>(),
            vec![11, 12]
        );
        assert_eq!(select_threads(99, "*", &threads).unwrap().len(), 4);
        assert!(matches!(
            select_threads(99, "2", &threads),
            Err(CpuAffinityError::InvalidRequest(_))
        ));

        let duplicate = vec![thread(11, "fc_vcpu0"), thread(12, "fc_vcpu 0")];
        assert!(matches!(
            select_threads(99, "0", &duplicate),
            Err(CpuAffinityError::Operation(_))
        ));
        assert!(matches!(
            select_threads(99, "0", &[thread(10, "fc_vmm")]),
            Err(CpuAffinityError::Operation(_))
        ));
    }

    #[test]
    fn current_process_threads_can_be_scanned_and_revalidated() {
        let pid = i32::try_from(std::process::id()).unwrap();
        let threads = list_threads(pid).unwrap();
        let main = threads.iter().find(|thread| thread.tid == pid).unwrap();
        main.verify_identity(pid).unwrap();
        main.read_affinity(pid).unwrap();
    }

    #[test]
    fn incomplete_rollback_reports_the_affected_tid() {
        let pid = i32::try_from(std::process::id()).unwrap();
        let threads = list_threads(pid).unwrap();
        let process = threads.iter().find(|thread| thread.tid == pid).unwrap();
        let missing = thread(i32::MAX, "exited-thread");
        let original = CpuSet::new();
        let error = error_after_rollback(
            pid,
            process,
            "apply failed".to_string(),
            &[(&missing, original)],
        );
        let message = error.to_string();
        assert!(message.contains("rollback incomplete"));
        assert!(message.contains(&format!("tid={}", missing.tid)));
    }

    #[test]
    fn wildcard_binding_and_rollback_work_on_disposable_process() {
        let executable = std::env::current_exe().unwrap();
        let child = Command::new(executable)
            .args([
                "--exact",
                "sandbox::cpu_affinity::tests::affinity_test_child",
                "--ignored",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut child = ChildGuard(child);
        let pid = i32::try_from(child.0.id()).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        let threads = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                panic!("affinity test child exited before it was ready: {status}");
            }
            if let Ok(threads) = list_threads(pid) {
                let workers_ready = AFFINITY_TEST_WORKERS
                    .iter()
                    .all(|name| threads.iter().any(|thread| thread.comm == *name));
                if workers_ready {
                    break threads;
                }
            }
            assert!(
                Instant::now() < deadline,
                "affinity test child was not ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        };

        let originals: Vec<(&ThreadInfo, CpuSet)> = threads
            .iter()
            .map(|thread| {
                (
                    thread,
                    sched_getaffinity(Pid::from_raw(thread.tid)).unwrap(),
                )
            })
            .collect();
        let process = threads.iter().find(|thread| thread.tid == pid).unwrap();
        let cpu = (0..CpuSet::count())
            .find(|&cpu| originals.iter().all(|(_, set)| set.is_set(cpu) == Ok(true)))
            .expect("child threads should share at least one allowed CPU");

        let stopped = StoppedThreadGroup::stop(pid).unwrap();
        assert!(list_threads(pid)
            .unwrap()
            .iter()
            .all(ThreadInfo::is_stopped));
        stopped.resume().unwrap();
        wait_until_running(pid);

        let error = bind_process(
            pid,
            CpuAffinityRequest {
                vcpu: "0".to_string(),
                core: cpu.to_string(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, CpuAffinityError::Operation(_)));
        wait_until_running(pid);

        let outcome = bind_process(
            pid,
            CpuAffinityRequest {
                vcpu: "*".to_string(),
                core: cpu.to_string(),
            },
        )
        .unwrap();
        assert_eq!(outcome.bound_thread_count as usize, originals.len());
        wait_until_running(pid);

        let rollback = error_after_rollback(pid, process, "test rollback".to_string(), &originals);
        assert!(rollback.to_string().contains("rolled back"));
        for (thread, original) in originals {
            assert_eq!(
                sched_getaffinity(Pid::from_raw(thread.tid)).unwrap(),
                original
            );
            thread.verify_identity(pid).unwrap();
        }
    }

    #[test]
    #[ignore = "helper process for wildcard_binding_and_rollback_work_on_disposable_process"]
    fn affinity_test_child() {
        let ready = Arc::new(Barrier::new(3));
        let workers: Vec<_> = AFFINITY_TEST_WORKERS
            .into_iter()
            .map(|name| {
                let ready = Arc::clone(&ready);
                std::thread::Builder::new()
                    .name(name.to_string())
                    .spawn(move || {
                        ready.wait();
                        std::thread::sleep(Duration::from_secs(30));
                    })
                    .unwrap()
            })
            .collect();
        ready.wait();
        for worker in workers {
            worker.join().unwrap();
        }
    }
}
