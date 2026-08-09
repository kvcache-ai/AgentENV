use std::io;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tracing::warn;

pub use linux_cap::{CAP_NET_ADMIN, CAP_NET_RAW, CAP_SYS_ADMIN};

fn capability_name(capability: i32) -> &'static str {
    match capability {
        CAP_NET_ADMIN => "CAP_NET_ADMIN",
        CAP_NET_RAW => "CAP_NET_RAW",
        CAP_SYS_ADMIN => "CAP_SYS_ADMIN",
        _ => "unknown capability",
    }
}

/// Validate the capability contract required by the non-root runtime.
///
/// The inheritable and permitted checks are intentional: privileged helper
/// processes receive only their required capability through the ambient set
/// immediately before `exec`.
pub fn require_runtime_capabilities() -> Result<()> {
    let sets = linux_cap::CapabilitySets::current().context("read process capability sets")?;
    let is_root = nix::unistd::Uid::effective().is_root();
    let missing = [CAP_NET_ADMIN, CAP_NET_RAW, CAP_SYS_ADMIN]
        .into_iter()
        .filter(|capability| {
            if is_root {
                !sets.effective(*capability).unwrap_or(false)
            } else {
                !sets.is_delegable(*capability).unwrap_or(false)
            }
        })
        .map(capability_name)
        .collect::<Vec<_>>();

    if !missing.is_empty() {
        bail!(
            "AENV runtime is missing required Linux capabilities: {}. Start it with CAP_NET_ADMIN, CAP_NET_RAW, and CAP_SYS_ADMIN in the inheritable, permitted, and effective sets (the installed systemd unit configures this automatically)",
            missing.join(", ")
        );
    }
    Ok(())
}

/// Clear ambient capabilities from the server so ordinary executables do not
/// inherit its network and namespace privileges.
pub fn clear_ambient_capabilities() -> Result<()> {
    linux_cap::clear_ambient_capabilities().context("clear ambient Linux capabilities")
}

/// Run an operation on a short-lived thread with an exact capability set.
///
/// Linux capabilities and network namespace membership are thread-scoped. The
/// new thread inherits the caller's namespace, receives only `capabilities`,
/// and exits after the operation. Commands spawned by the operation inherit the
/// requested capabilities through the ambient set without requiring a
/// `pre_exec` hook in the multi-threaded server.
pub fn run_with_scoped_capabilities<T, F>(capabilities: &[i32], operation: F) -> Result<T>
where
    T: Send,
    F: FnOnce() -> Result<T> + Send,
{
    thread::scope(|scope| -> Result<T> {
        let launcher = thread::Builder::new()
            .name("aenv-capability-command".to_string())
            .spawn_scoped(scope, move || {
                linux_cap::configure_current_process_capabilities(capabilities)
                    .context("scope command capabilities")?;
                operation()
            })
            .context("spawn capability-scoped command thread")?;

        launcher
            .join()
            .map_err(|panic| anyhow!("capability-scoped command thread panicked: {panic:?}"))?
    })
}

/// Spawn a Tokio command from a short-lived thread with an exact capability set.
///
/// `before_capability_scope` runs on the launcher thread before its capabilities
/// are replaced, allowing namespace entry that requires `CAP_SYS_ADMIN`. The
/// launcher thread then scopes its capabilities and uses the normal command
/// spawn path, so the multi-threaded server process does not need a `pre_exec`
/// hook and remains eligible for `posix_spawn`.
pub async fn spawn_tokio_command_scoped<F>(
    mut command: tokio::process::Command,
    capabilities: &'static [i32],
    before_capability_scope: F,
) -> io::Result<tokio::process::Child>
where
    F: FnOnce() -> io::Result<()> + Send + 'static,
{
    let runtime = tokio::runtime::Handle::try_current().map_err(|err| {
        io::Error::other(format!(
            "Tokio runtime is required to spawn a scoped command: {err}"
        ))
    })?;
    let (sender, receiver) = tokio::sync::oneshot::channel();

    thread::Builder::new()
        .name("aenv-process-launcher".to_string())
        .spawn(move || {
            let result = (|| {
                before_capability_scope()?;
                linux_cap::configure_current_process_capabilities(capabilities)?;

                let _runtime_guard = runtime.enter();
                command.kill_on_drop(true);
                command.spawn()
            })();

            if let Err(Ok(child)) = sender.send(result) {
                terminate_unclaimed_child(child);
            }
        })?;

    receiver
        .await
        .map_err(|_| io::Error::other("scoped command launcher exited without returning a child"))?
}

fn terminate_unclaimed_child(mut child: tokio::process::Child) {
    if let Err(err) = child.start_kill() {
        warn!(error = %err, "failed to kill unclaimed scoped child process");
    }

    for _ in 0..100 {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Err(err) => {
                warn!(error = %err, "failed to reap unclaimed scoped child process");
                return;
            }
            Ok(None) => thread::sleep(Duration::from_millis(1)),
        }
    }

    warn!("timed out reaping unclaimed scoped child process");
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::fd::{AsFd, AsRawFd};
    use std::process::{Command, Stdio};

    use nix::sched::CloneFlags;
    use tempfile::tempdir;

    use super::*;

    const CAPABILITY_STATUS_FIELDS: [&str; 4] = ["CapInh:", "CapPrm:", "CapEff:", "CapAmb:"];

    fn status_field<'a>(status: &'a str, field: &str) -> &'a str {
        status
            .lines()
            .find_map(|line| line.strip_prefix(field))
            .unwrap_or_else(|| panic!("{field} is missing from process status"))
            .trim()
    }

    fn current_capability_status() -> Result<Vec<String>> {
        let status = fs::read_to_string("/proc/thread-self/status")?;
        Ok(CAPABILITY_STATUS_FIELDS
            .iter()
            .map(|field| status_field(&status, field).to_string())
            .collect())
    }

    fn assert_child_has_no_capabilities(status: &str) {
        for field in CAPABILITY_STATUS_FIELDS {
            assert_eq!(
                status_field(status, field),
                "0000000000000000",
                "child retained {field}"
            );
        }
    }

    #[test]
    fn parses_capability_sets() {
        let status =
            "CapInh:\t0000000000203000\nCapPrm:\t0000000000203000\nCapEff:\t0000000000203000\n";
        let sets = linux_cap::CapabilitySets::from_proc_status(status).unwrap();
        assert!(sets.is_delegable(CAP_NET_ADMIN).unwrap());
        assert!(sets.is_delegable(CAP_NET_RAW).unwrap());
        assert!(sets.is_delegable(CAP_SYS_ADMIN).unwrap());
    }

    #[test]
    fn capability_must_be_present_in_all_required_sets() {
        let status =
            "CapInh:\t0000000000203000\nCapPrm:\t0000000000203000\nCapEff:\t0000000000001000\n";
        let sets = linux_cap::CapabilitySets::from_proc_status(status).unwrap();
        assert!(sets.is_delegable(CAP_NET_ADMIN).unwrap());
        assert!(!sets.is_delegable(CAP_NET_RAW).unwrap());
        assert!(!sets.is_delegable(CAP_SYS_ADMIN).unwrap());
    }

    #[tokio::test]
    async fn scoped_spawn_clears_child_capabilities_without_mutating_caller() -> Result<()> {
        let caller_capabilities = current_capability_status()?;
        let mut command = tokio::process::Command::new("sh");
        command
            .args(["-c", "cat /proc/thread-self/status"])
            .stdout(Stdio::piped());

        let child = spawn_tokio_command_scoped(command, &[], || Ok(())).await?;
        let output = child.wait_with_output().await?;

        assert!(output.status.success());
        assert_child_has_no_capabilities(&String::from_utf8(output.stdout)?);
        assert_eq!(current_capability_status()?, caller_capabilities);
        Ok(())
    }

    #[tokio::test]
    async fn terminate_unclaimed_child_kills_and_reaps_process() -> Result<()> {
        let temp = tempdir()?;
        let marker = temp.path().join("child-started");
        let mut command = tokio::process::Command::new("sh");
        command.args([
            "-c",
            &format!("touch '{}' && exec sleep 30", marker.display()),
        ]);
        let child = command.spawn()?;
        let pid = child.id().context("spawned child has no pid")?;

        for _ in 0..100 {
            if marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(marker.exists(), "child did not start");

        terminate_unclaimed_child(child);

        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
        Ok(())
    }

    #[test]
    #[ignore = "requires CAP_NET_ADMIN to delegate a capability to the child"]
    fn scoped_command_receives_only_requested_capability() -> Result<()> {
        anyhow::ensure!(
            linux_cap::has_effective_capabilities(&[CAP_NET_ADMIN])?,
            "test requires CAP_NET_ADMIN"
        );

        let caller_capabilities = current_capability_status()?;
        let output = run_with_scoped_capabilities(&[CAP_NET_ADMIN], || {
            Ok(Command::new("sh")
                .args(["-c", "cat /proc/thread-self/status"])
                .output()?)
        })?;
        let status = String::from_utf8(output.stdout)?;

        assert!(output.status.success());
        for field in CAPABILITY_STATUS_FIELDS {
            assert_eq!(
                status_field(&status, field),
                "0000000000001000",
                "child received an unexpected {field} value"
            );
        }
        assert_eq!(current_capability_status()?, caller_capabilities);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires CAP_SYS_ADMIN to create and enter a network namespace"]
    async fn scoped_spawn_enters_netns_and_drops_capabilities() -> Result<()> {
        anyhow::ensure!(
            linux_cap::has_effective_capabilities(&[CAP_SYS_ADMIN])?,
            "test requires CAP_SYS_ADMIN"
        );

        let caller_namespace = fs::read_link("/proc/thread-self/ns/net")?;
        let caller_capabilities = current_capability_status()?;
        let netns = thread::spawn(|| -> Result<fs::File> {
            nix::sched::unshare(CloneFlags::CLONE_NEWNET)?;
            Ok(fs::File::open("/proc/thread-self/ns/net")?)
        })
        .join()
        .map_err(|panic| anyhow::anyhow!("network namespace thread panicked: {panic:?}"))??;
        let target_namespace = fs::read_link(format!("/proc/self/fd/{}", netns.as_raw_fd()))?;

        let mut command = tokio::process::Command::new("sh");
        command
            .args([
                "-c",
                "readlink /proc/thread-self/ns/net; cat /proc/thread-self/status",
            ])
            .stdout(Stdio::piped());
        let child = spawn_tokio_command_scoped(command, &[], move || {
            nix::sched::setns(netns.as_fd(), CloneFlags::CLONE_NEWNET).map_err(io::Error::from)
        })
        .await?;
        let output = child.wait_with_output().await?;
        let stdout = String::from_utf8(output.stdout)?;
        let (child_namespace, child_status) = stdout
            .split_once('\n')
            .context("child output is missing process status")?;

        assert!(output.status.success());
        assert_eq!(child_namespace, target_namespace.to_string_lossy());
        assert_child_has_no_capabilities(child_status);
        assert_eq!(fs::read_link("/proc/thread-self/ns/net")?, caller_namespace);
        assert_eq!(current_capability_status()?, caller_capabilities);
        Ok(())
    }
}
