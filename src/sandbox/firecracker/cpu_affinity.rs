//! Live, non-atomic CPU-affinity updates for a Firecracker process.
//!
//! Select targets from one procfs scan, then set and immediately read back each
//! mask. Stop on the first error without rollback. Threads may exit, appear, or
//! have their TIDs reused; neither the scan nor the updates form a transaction.

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;

use anyhow::{anyhow, bail, Context, Result};
use nix::sched::{sched_getaffinity, sched_setaffinity, CpuSet};
use nix::unistd::Pid;

const MAX_CPU_LIST_BYTES: usize = 16 * 1024;
const MAX_CPU_LIST_VALUES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpuAffinityRequest {
    pub vcpu: String,
    pub core: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpuAffinityOutcome {
    pub vcpu: String,
    pub cores: String,
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

#[derive(Clone, Debug)]
struct ThreadInfo {
    tid: i32,
    comm: String,
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
    for (field, spec) in [("vcpu", &request.vcpu), ("core", &request.core)] {
        if spec.len() > MAX_CPU_LIST_BYTES {
            return Err(CpuAffinityError::InvalidRequest(format!(
                "{field} list is too long (maximum: {MAX_CPU_LIST_BYTES} bytes)"
            )));
        }
    }
    let vcpu = request.vcpu.trim().to_owned();
    if vcpu != "*" {
        parse_cpu_list(&vcpu, u32::MAX).map_err(CpuAffinityError::invalid)?;
    }
    // nix::CpuSet wraps libc's fixed-size cpu_set_t (0-1023 on supported Linux targets).
    let cores = host_cpu_list(request.core.trim(), (CpuSet::count() - 1) as u32)?;
    let mut desired = CpuSet::new();
    for &cpu in &cores {
        desired
            .set(cpu as usize)
            .with_context(|| format!("core {cpu} does not fit in cpu_set_t"))
            .map_err(CpuAffinityError::Operation)?;
    }
    let threads = list_threads(pid)
        .with_context(|| format!("failed to enumerate threads of Firecracker pid {pid}"))
        .map_err(CpuAffinityError::Operation)?;
    let targets = select_threads(pid, &vcpu, &threads)?;
    let bound_thread_count = targets.len() as u32; // Linux TIDs fit in a positive i32.
    bind_threads(
        pid,
        &targets,
        &desired,
        |tid, mask| Ok(sched_setaffinity(Pid::from_raw(tid), mask)?),
        |tid| Ok(sched_getaffinity(Pid::from_raw(tid))?),
    )
    .map_err(CpuAffinityError::Operation)?;
    Ok(CpuAffinityOutcome {
        vcpu,
        cores: format_cpu_list(&cores),
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
    let max_vcpu = *indexed.keys().next_back().unwrap();
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

fn host_cpu_list(spec: &str, max_cpu: u32) -> std::result::Result<Vec<u32>, CpuAffinityError> {
    if spec == "*" {
        let online = fs::read_to_string("/sys/devices/system/cpu/online")
            .context("failed to read host online CPUs")
            .map_err(CpuAffinityError::Operation)?;
        parse_cpu_list(&online, max_cpu)
            .context("invalid or unsupported host online CPU list")
            .map_err(CpuAffinityError::Operation)
    } else {
        parse_cpu_list(spec, max_cpu)
            .with_context(|| format!("invalid core value {spec:?}"))
            .map_err(CpuAffinityError::invalid)
    }
}

fn parse_cpu_list(spec: &str, max_value: u32) -> Result<Vec<u32>> {
    let spec = spec.trim();
    if spec.is_empty() {
        bail!("empty cpu list");
    }
    if spec.len() > MAX_CPU_LIST_BYTES {
        bail!("cpu list is too long (maximum: {MAX_CPU_LIST_BYTES} bytes)");
    }
    let values = cpulist::parse(spec).context("invalid cpu list syntax")?;
    if values.is_empty() {
        bail!("empty cpu list");
    }
    if let Some(&last) = values.last().filter(|&&last| last > max_value) {
        bail!("cpu {last} exceeds maximum allowed value {max_value}");
    }
    if values.len() > MAX_CPU_LIST_VALUES {
        bail!("cpu list contains more than {MAX_CPU_LIST_VALUES} unique values");
    }
    Ok(values)
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

fn format_cpu_set(set: &CpuSet) -> String {
    let cpus: Vec<u32> = (0..CpuSet::count())
        .filter(|&index| set.is_set(index) == Ok(true))
        .map(|index| index as u32)
        .collect();
    format_cpu_list(&cpus)
}

/// Collect one best-effort task snapshot. Tasks disappearing during the scan
/// are skipped; errors after selection are reported, not silently skipped.
fn list_threads(pid: i32) -> Result<Vec<ThreadInfo>> {
    let directory = format!("/proc/{pid}/task");
    let entries = fs::read_dir(&directory)
        .with_context(|| format!("failed to open {directory} (process alive and permitted?)"))?;
    let mut threads = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("failed to enumerate {directory}"))?;
        let tid = match entry.file_name().to_string_lossy().parse() {
            Ok(tid) => tid,
            Err(_) => continue,
        };
        let path = format!("/proc/{pid}/task/{tid}/comm");
        let comm = match fs::read_to_string(&path) {
            Ok(comm) => comm,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error).with_context(|| format!("failed to read {path}")),
        };
        threads.push(ThreadInfo {
            tid,
            comm: comm.strip_suffix('\n').unwrap_or(&comm).to_owned(),
        });
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

/// Each successful readback confirms that thread's mask at that instant only.
/// Inject the two syscalls so tests can exercise partial failures deterministically.
fn bind_threads(
    pid: i32,
    targets: &[&ThreadInfo],
    desired: &CpuSet,
    mut set: impl FnMut(i32, &CpuSet) -> Result<()>,
    mut get: impl FnMut(i32) -> Result<CpuSet>,
) -> Result<()> {
    let cores = format_cpu_set(desired);
    for (verified, thread) in targets.iter().enumerate() {
        let result = (|| -> Result<()> {
            set(thread.tid, desired).context("sched_setaffinity failed")?;
            let actual = get(thread.tid).context("sched_getaffinity readback failed")?;
            if actual != *desired {
                bail!(
                    "affinity readback mismatch: actual {}",
                    format_cpu_set(&actual)
                );
            }
            Ok(())
        })();
        if let Err(error) = result {
            bail!(
                "CPU affinity for pid {pid} failed at target {}/{} tid={} ({:?}), requested {cores}; \
                 verified {verified}/{} thread(s); current target may have changed; \
                 {} target(s) not attempted; no rollback: {error:#}",
                verified + 1,
                targets.len(),
                thread.tid,
                thread.comm,
                targets.len(),
                targets.len() - verified - 1
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn child() -> (ChildGuard, i32, Vec<ThreadInfo>) {
        let test = "sandbox::firecracker::cpu_affinity::tests::affinity_test_child";
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--ignored"])
            .env("AENV_CPU_AFFINITY_TEST_CHILD", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut child = ChildGuard(child);
        let pid = i32::try_from(child.0.id()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(child.0.try_wait().unwrap().is_none(), "helper exited");
            if let Ok(threads) = list_threads(pid) {
                if ["fc_vcpu 0", "fc_vcpu1", "worker ) name"]
                    .iter()
                    .all(|name| threads.iter().any(|t| &t.comm == name))
                {
                    return (child, pid, threads);
                }
            }
            assert!(Instant::now() < deadline, "helper did not become ready");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn request(vcpu: &str, core: &str) -> CpuAffinityRequest {
        CpuAffinityRequest {
            vcpu: vcpu.into(),
            core: core.into(),
        }
    }

    #[test]
    fn cpu_lists_parse_and_validate_limits() {
        assert_eq!(parse_cpu_list(" 4,0-6:2,2 ", 6).unwrap(), vec![0, 2, 4, 6]);
        for invalid in ["", ",", "a", "2-1", "0-2:0", " 3 , 1 "] {
            assert!(parse_cpu_list(invalid, 4).is_err(), "accepted {invalid:?}");
        }
        assert!(parse_cpu_list("1024", 1023).is_err());
        assert!(parse_cpu_list("0-4096", u32::MAX).is_err());
        assert_eq!(format_cpu_list(&[0, 1, 2, 4, u32::MAX]), "0-2,4,4294967295");
        let oversized = "0".repeat(MAX_CPU_LIST_BYTES + 1);
        for (vcpu, core) in [(oversized.as_str(), "0"), ("*", oversized.as_str())] {
            let result = bind_process(i32::MAX, request(vcpu, core));
            assert!(matches!(result, Err(CpuAffinityError::InvalidRequest(_))));
        }
    }

    #[test]
    fn host_cpu_list_distinguishes_wildcard_from_explicit_ids() {
        let online = fs::read_to_string("/sys/devices/system/cpu/online").unwrap();
        let all = host_cpu_list("*", u32::MAX).unwrap();
        assert_eq!(all, cpulist::parse(online.trim()).unwrap());
        assert_eq!(host_cpu_list("1023", 1023).unwrap(), vec![1023]);
    }

    #[test]
    fn thread_selection_distinguishes_vcpus_from_all_threads() {
        let threads =
            [(10, "fc_vmm"), (11, "fc_vcpu 0"), (12, "fc_vcpu15")].map(|(tid, comm)| ThreadInfo {
                tid,
                comm: comm.into(),
            });
        let selected = select_threads(99, "15,0,15", &threads).unwrap();
        let tids: Vec<_> = selected.iter().map(|t| t.tid).collect();
        assert_eq!(tids, vec![11, 12]);
        assert_eq!(select_threads(99, "*", &threads).unwrap().len(), 3);
        for spec in ["1", "16", "invalid"] {
            assert!(matches!(
                select_threads(99, spec, &threads),
                Err(CpuAffinityError::InvalidRequest(_))
            ));
        }
        assert!(select_threads(99, "0", &threads[..1]).is_err());
        let duplicates = [threads[1].clone(), threads[1].clone()];
        assert!(select_threads(99, "0", &duplicates).is_err());
    }

    #[test]
    fn per_thread_verification_stops_on_first_error_without_rollback() {
        let threads = [10, 11, 12].map(|tid| ThreadInfo {
            tid,
            comm: format!("thread{tid}"),
        });
        let targets: Vec<_> = threads.iter().collect();
        let mut desired = CpuSet::new();
        desired.set(1).unwrap();
        let original = CpuSet::new();
        for failed in 0..3 {
            for stage in ["ok", "set", "get", "mismatch"] {
                let masks = RefCell::new([original; 3]);
                let calls = RefCell::new(Vec::new());
                let result = bind_threads(
                    99,
                    &targets,
                    &desired,
                    |tid, mask| {
                        calls.borrow_mut().push(("set", tid));
                        let index = (tid - 10) as usize;
                        masks.borrow_mut()[index] = if index == failed && stage == "mismatch" {
                            original
                        } else {
                            *mask
                        };
                        if index == failed && stage == "set" {
                            bail!("injected set error");
                        }
                        Ok(())
                    },
                    |tid| {
                        calls.borrow_mut().push(("get", tid));
                        if (tid - 10) as usize == failed && stage == "get" {
                            bail!("injected get error");
                        }
                        Ok(masks.borrow()[(tid - 10) as usize])
                    },
                );
                let mut expected = Vec::new();
                let last = if stage == "ok" { 2 } else { failed };
                for (index, thread) in threads.iter().enumerate().take(last + 1) {
                    expected.push(("set", thread.tid));
                    if index != failed || stage != "set" {
                        expected.push(("get", thread.tid));
                    }
                }
                assert_eq!(*calls.borrow(), expected);
                if stage == "ok" {
                    result.unwrap();
                    assert!(masks.borrow().iter().all(|mask| *mask == desired));
                    continue;
                }
                let message = result.unwrap_err().to_string();
                for detail in [
                    format!("verified {failed}/3"),
                    format!("tid={}", threads[failed].tid),
                    format!("{} target(s) not attempted", 2 - failed),
                    "requested 1".into(),
                    "current target may have changed".into(),
                    "no rollback".into(),
                    stage.into(),
                ] {
                    assert!(message.contains(&detail), "{message}");
                }
                let has_cause = message.contains(&format!("injected {stage} error"));
                assert!(stage == "mismatch" || has_cause);
                for (index, mask) in masks.borrow().iter().enumerate() {
                    let changed = index < failed || (index == failed && stage != "mismatch");
                    assert_eq!(*mask, if changed { desired } else { original });
                }
            }
        }
    }

    #[test]
    fn live_process_supports_numeric_and_wildcard_selection() {
        let (_child, pid, threads) = child();
        let read = |t: &ThreadInfo| sched_getaffinity(Pid::from_raw(t.tid)).unwrap();
        let originals: Vec<_> = threads.iter().map(read).collect();
        let cpu = (0..CpuSet::count())
            .find(|&cpu| originals.iter().all(|mask| mask.is_set(cpu) == Ok(true)))
            .unwrap();
        let core = cpu.to_string();
        let mut desired = CpuSet::new();
        desired.set(cpu).unwrap();
        let outcome = bind_process(pid, request("1", &core)).unwrap();
        assert_eq!(outcome.bound_thread_count, 1);
        for (thread, original) in threads.iter().zip(&originals) {
            let expected = if thread.comm == "fc_vcpu1" {
                &desired
            } else {
                original
            };
            assert_eq!(&read(thread), expected);
        }
        let outcome = bind_process(pid, request("*", &core)).unwrap();
        assert_eq!(outcome.bound_thread_count as usize, threads.len());
        for thread in &threads {
            assert_eq!(read(thread), desired);
        }
    }

    #[test]
    #[ignore = "disposable subprocess fixture"]
    fn affinity_test_child() {
        if std::env::var_os("AENV_CPU_AFFINITY_TEST_CHILD").is_none() {
            return;
        }
        let workers: Vec<_> = ["fc_vcpu 0", "fc_vcpu1", "worker ) name"]
            .into_iter()
            .map(|name| {
                std::thread::Builder::new()
                    .name(name.into())
                    .spawn(|| std::thread::sleep(Duration::from_secs(30)))
                    .unwrap()
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
    }
}
