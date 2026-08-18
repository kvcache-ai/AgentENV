//! Reaping of orphaned runtime processes left by a previous server incarnation.
//!
//! The node agent spawns `firecracker` and `uvm-ublk-daemon` as detached child
//! processes (each in its own process group). When the agent is hard-killed —
//! SIGKILL from the container runtime, an OOM kill, a node reboot — those
//! children can outlive it while still holding sandbox resources: ublk devices,
//! network namespaces, open image files, and inherited file descriptors that
//! pin the RocksDB `LOCK` of the persisted-sandbox store. The next server
//! process then fails to spawn its daemon or open its record database and
//! crash-loops.
//!
//! [`reap`] runs early in server startup, before the ublk daemon is spawned
//! and before the persisted-sandbox store is opened, and terminates any process
//! in this PID namespace whose executable is one of the configured runtime
//! binaries. Any match necessarily belongs to a previous incarnation: the
//! current process has not launched its own children yet.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::cfg::AppConfig;

/// How long to wait for SIGTERM to take effect before escalating to SIGKILL.
const TERM_GRACE: Duration = Duration::from_secs(5);
/// Extra wait for SIGKILLed processes to disappear from `/proc`.
const KILL_GRACE: Duration = Duration::from_secs(2);
/// `readlink` on `/proc/<pid>/exe` reports the original path with this suffix
/// when the binary was replaced on disk after the process started.
const DELETED_SUFFIX: &str = " (deleted)";

/// Terminate leftover `firecracker`/`uvm-ublk-daemon` processes from a previous
/// server incarnation. Best-effort: per-process failures are logged, not
/// propagated, so an uncooperative orphan never blocks startup.
pub async fn reap(config: &AppConfig) -> Result<()> {
    let mut targets = HashSet::new();
    targets.insert(config.resolved_firecracker_binary_path());
    if let Some(daemon_binary) = config
        .ublk
        .daemon_binary_path
        .clone()
        .or_else(|| which::which("uvm-ublk-daemon").ok())
    {
        targets.insert(daemon_binary);
    }

    let mut orphans = find_processes_by_exe(Path::new("/proc"), &targets);
    if orphans.is_empty() {
        return Ok(());
    }
    orphans.sort_unstable();

    warn!(
        count = orphans.len(),
        ?orphans,
        "found orphaned runtime processes from a previous incarnation; terminating them"
    );
    for pid in &orphans {
        signal(*pid, Signal::SIGTERM);
    }
    let survivors = wait_for_exit(&orphans, TERM_GRACE).await;
    if !survivors.is_empty() {
        warn!(
            ?survivors,
            "orphaned runtime processes ignored SIGTERM; sending SIGKILL"
        );
        for pid in &survivors {
            signal(*pid, Signal::SIGKILL);
        }
        let stuck = wait_for_exit(&survivors, KILL_GRACE).await;
        if !stuck.is_empty() {
            warn!(
                ?stuck,
                "orphaned runtime processes still present after SIGKILL (likely uninterruptible sleep)"
            );
        }
    }

    info!(count = orphans.len(), "orphaned runtime processes reaped");
    Ok(())
}

fn signal(pid: u32, sig: Signal) {
    let raw = match i32::try_from(pid) {
        Ok(raw) => raw,
        Err(err) => {
            debug!(pid, error = %err, "orphaned process pid does not fit in i32");
            return;
        }
    };
    if let Err(err) = kill(Pid::from_raw(raw), sig) {
        debug!(pid, %sig, error = %err, "failed to signal orphaned process");
    }
}

/// Wait until none of `pids` is a running process anymore, or the grace period
/// elapses. Returns the pids still running.
async fn wait_for_exit(pids: &[u32], grace: Duration) -> Vec<u32> {
    let deadline = Instant::now() + grace;
    loop {
        let survivors: Vec<u32> = pids
            .iter()
            .copied()
            .filter(|pid| process_running(*pid))
            .collect();
        if survivors.is_empty() || Instant::now() >= deadline {
            return survivors;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Returns `true` if `pid` exists and is not a zombie. Zombies hold no
/// resources — their parent just has not reaped them yet.
fn process_running(pid: u32) -> bool {
    match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => !stat_is_zombie(&stat),
        Err(_) => false,
    }
}

/// Parse the state field of `/proc/<pid>/stat`. The state is the first field
/// after the closing parenthesis of `comm`, which may itself contain spaces
/// and parentheses, so split at the *last* `)`.
fn stat_is_zombie(stat: &str) -> bool {
    stat.rfind(')')
        .and_then(|close| stat[close + 1..].split_whitespace().next())
        .is_some_and(|state| state == "Z")
}

/// Scan a `/proc`-like directory for process IDs whose `exe` link resolves to
/// one of `targets`. Entries that are not numeric, have no `exe` link, or
/// cannot be read are skipped.
fn find_processes_by_exe(proc_root: &Path, targets: &HashSet<PathBuf>) -> Vec<u32> {
    if targets.is_empty() {
        return Vec::new();
    }
    let normalized_targets: HashSet<PathBuf> = targets.iter().map(|t| normalize_exe(t)).collect();

    let entries = match fs::read_dir(proc_root) {
        Ok(entries) => entries,
        Err(err) => {
            warn!(
                proc_root = %proc_root.display(),
                error = %err,
                "failed to scan for orphaned runtime processes"
            );
            return Vec::new();
        }
    };

    let mut pids = Vec::new();
    for entry in entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(exe) = fs::read_link(entry.path().join("exe")) else {
            continue;
        };
        if normalized_targets.contains(&normalize_exe(&exe)) {
            pids.push(pid);
        }
    }
    pids
}

/// Canonicalize an executable path so symlinked spawn paths and `/proc` `exe`
/// links compare equal, tolerating missing files and the `(deleted)` suffix
/// reported for replaced binaries.
fn normalize_exe(path: &Path) -> PathBuf {
    let stripped = path
        .to_str()
        .and_then(|raw| raw.strip_suffix(DELETED_SUFFIX))
        .map_or_else(|| path.to_path_buf(), PathBuf::from);
    fs::canonicalize(&stripped).unwrap_or(stripped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Build a fake `/proc` tree where `entries` maps pid -> exe link target.
    fn fake_proc(entries: &[(u32, &Path)]) -> TempDir {
        let temp = TempDir::new().expect("create fake proc root");
        for (pid, exe_target) in entries {
            let dir = temp.path().join(pid.to_string());
            fs::create_dir_all(&dir).expect("create fake pid dir");
            std::os::unix::fs::symlink(exe_target, dir.join("exe")).expect("symlink fake exe");
        }
        temp
    }

    #[test]
    fn find_processes_matches_only_configured_binaries() {
        let temp = TempDir::new().unwrap();
        let firecracker = temp.path().join("firecracker");
        let daemon = temp.path().join("uvm-ublk-daemon");
        let unrelated = temp.path().join("unrelated");
        for path in [&firecracker, &daemon, &unrelated] {
            fs::write(path, b"binary").unwrap();
        }

        let proc = fake_proc(&[(1234, &firecracker), (5678, &daemon), (4321, &unrelated)]);
        let mut targets = HashSet::new();
        targets.insert(firecracker.clone());
        targets.insert(daemon.clone());

        let mut pids = find_processes_by_exe(proc.path(), &targets);
        pids.sort_unstable();
        assert_eq!(pids, vec![1234, 5678]);
    }

    #[test]
    fn find_processes_matches_binary_replaced_on_disk() {
        let temp = TempDir::new().unwrap();
        let firecracker = temp.path().join("firecracker");

        // Simulate readlink reporting "<path> (deleted)" for a process whose
        // binary was replaced after it started: a dangling symlink carrying
        // the suffix, while the target itself no longer exists.
        let proc = fake_proc(&[(
            1234,
            Path::new(&format!("{} (deleted)", firecracker.display())),
        )]);
        let mut targets = HashSet::new();
        targets.insert(firecracker.clone());

        let pids = find_processes_by_exe(proc.path(), &targets);
        assert_eq!(pids, vec![1234]);
    }

    #[test]
    fn find_processes_skips_non_numeric_entries_and_missing_exe() {
        let temp = TempDir::new().unwrap();
        let firecracker = temp.path().join("firecracker");
        fs::write(&firecracker, b"binary").unwrap();

        let proc = fake_proc(&[(1234, &firecracker)]);
        fs::create_dir_all(proc.path().join("not-a-pid")).unwrap();
        fs::create_dir_all(proc.path().join("5678")).unwrap(); // no exe link

        let mut targets = HashSet::new();
        targets.insert(firecracker);

        let pids = find_processes_by_exe(proc.path(), &targets);
        assert_eq!(pids, vec![1234]);
    }

    #[test]
    fn stat_zombie_detection_uses_state_after_last_paren() {
        assert!(stat_is_zombie("1234 (firecracker) Z 1 2 3"));
        assert!(stat_is_zombie("1234 (weird) name) Z 1 2 3"));
        assert!(!stat_is_zombie("1234 (firecracker) S 1 2 3"));
        assert!(!stat_is_zombie("garbage"));
    }
}
