use std::fs::{self, DirBuilder, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, OpenOptionsExt};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout, Instant};

use crate::process::{self, PATH};

async fn supervise(children: &mut Vec<(&str, Child)>) -> Result<()> {
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create("/var/log/agentenv-compose")?;
    fs::create_dir_all("/sys/fs/cgroup")?;
    if !crate::start::mountpoints()?.contains(Path::new("/sys/fs/cgroup")) {
        timeout(
            Duration::from_secs(60),
            process::run(
                Command::new("/usr/bin/mount").args(["-t", "cgroup2", "none", "/sys/fs/cgroup"]),
                true,
            ),
        )
        .await
        .context("mount cgroup2 timed out")??;
    }
    for (name, program, args, socket) in [
        (
            "snapshotter",
            "plain-snapshotter",
            vec![],
            "/run/containerd-plain-snapshotter/snapshotter.sock",
        ),
        (
            "containerd",
            "containerd",
            vec!["--config", "/etc/containerd/config.toml"],
            "/run/containerd/containerd.sock",
        ),
        (
            "docker",
            "dockerd",
            vec!["--config-file", "/etc/docker/daemon.json"],
            "/var/run/docker.sock",
        ),
    ] {
        let log = OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o600)
            .open(format!("/var/log/agentenv-compose/{name}.log"))?;
        let mut command = Command::new(format!("/usr/local/bin/{program}"));
        command
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .args(args)
            .env("PATH", PATH)
            .stdout(log.try_clone()?)
            .stderr(log);
        children.push((
            name,
            command.spawn().with_context(|| format!("start {name}"))?,
        ));
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            check_children(children)?;
            if fs::symlink_metadata(socket).is_ok_and(|m| m.file_type().is_socket()) {
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "{name} did not create {socket} within 60s"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }
    loop {
        check_children(children)?;
        sleep(Duration::from_secs(1)).await;
    }
}

fn check_children(children: &mut [(&str, Child)]) -> Result<()> {
    for (name, process) in children {
        if let Some(status) = process
            .try_wait()
            .with_context(|| format!("monitor {name}"))?
        {
            bail!("{name} exited unexpectedly: {status}");
        }
    }
    Ok(())
}

/// Tini remains PID 1 and reaps orphans. Never restart runtime services: the
/// snapshotter's active metadata is in memory and survives VM snapshot restore.
pub async fn run() -> Result<()> {
    let mut children = Vec::new();
    process::until_signal(supervise(&mut children)).await
}
