//! Guest-side volume filesystem operations run through envd.

use std::path::Path;

use anyhow::{Context, Result};

use crate::sandbox::envd::EnvdInstance;
use crate::sandbox::process::Executor;
use crate::types::ExtraDrive;

pub(super) async fn mount_initial_guest_drives(
    envd: EnvdInstance,
    drives: Vec<(usize, ExtraDrive)>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Handle::current().block_on(mount_initial_guest_drives_inner(envd, drives))
    })
    .await
    .context("initial volume mount task failed")??;
    Ok(())
}

async fn mount_initial_guest_drives_inner(
    envd: EnvdInstance,
    drives: Vec<(usize, ExtraDrive)>,
) -> Result<()> {
    for (index, drive) in &drives {
        let device = format!("/dev/vd{}", char::from(b'c' + *index as u8));
        let target = drive.mount_path().to_string_lossy().into_owned();
        create_guest_directory(&envd, &target).await?;
        let mount_options = if drive.read_only() {
            // Volume snapshots are crash-consistent: even after sync, ext4
            // retains the needs-recovery flag until a clean unmount. A
            // read-only OverlayBD device cannot replay that journal, so
            // suppress replay while also enforcing a read-only guest mount.
            &["-o", "ro,noload"][..]
        } else {
            &[][..]
        };

        if let Some(sub_path) = drive.sub_path() {
            let stage = format!("/run/agentenv-drive-{}", drive.drive_id());
            create_guest_directory(&envd, &stage).await?;
            mount_guest_path_with_options(envd.clone(), mount_options, &device, &stage).await?;
            let source = format!("{stage}/{}", sub_path.display());
            mount_guest_path_with_options(envd.clone(), &["--bind"], &source, &target).await?;
            let _ = run_guest_command(
                envd.clone(),
                "/agentenv/bin/busybox".to_owned(),
                vec!["umount".to_owned(), "-l".to_owned(), stage.clone()],
            )
            .await;
        } else {
            mount_guest_path_with_options(envd.clone(), mount_options, &device, &target).await?;
        }
    }
    Ok(())
}

async fn create_guest_directory(envd: &EnvdInstance, path: &str) -> Result<()> {
    Executor::new(envd.clone())
        .with_root_user()
        .create_dir_all(path)
        .await
}

async fn run_guest_command(
    envd: EnvdInstance,
    command: String,
    args: Vec<String>,
) -> Result<crate::sandbox::process::ProcessOutput> {
    let args = args.iter().map(String::as_str).collect::<Vec<_>>();
    Executor::new(envd)
        .with_root_user()
        .run_command_with_opts(
            &command,
            &args,
            &crate::sandbox::ProcessOpts::default().with_cwd("/"),
        )
        .await
}

pub(super) async fn sync_writable_volume_filesystems(envd: EnvdInstance) -> Result<()> {
    let output = tokio::task::spawn_blocking(move || {
        tokio::runtime::Handle::current().block_on(run_guest_command(
            envd,
            "/agentenv/bin/busybox".to_owned(),
            vec!["sync".to_owned()],
        ))
    })
    .await
    .context("volume filesystem flush task failed")??;
    anyhow::ensure!(
        output.exit_code == 0,
        "failed to flush writable volume filesystems before pause (exit code {}): {}",
        output.exit_code,
        output.stderr.trim()
    );
    Ok(())
}

pub(super) async fn set_volume_freeze(
    envd: EnvdInstance,
    mount: &Path,
    frozen_device: Option<&str>,
) -> Result<crate::sandbox::process::ProcessOutput> {
    let mount = mount
        .to_str()
        .context("volume mount path is not valid UTF-8")?;
    // Keep one opened filesystem across validation and the ioctl, so an
    // unmount cannot redirect fsfreeze to the root filesystem hosting envd.
    let script = r#"
        set -eu
        exec 3< "$1"
        device=$(/agentenv/bin/busybox stat -Lc %d /proc/self/fd/3)
        if [ "$2" = --freeze ]; then
            test "$device" != "$(/agentenv/bin/busybox stat -c %d /)"
        else
            test "$device" = "$3"
        fi
        /agentenv/bin/busybox fsfreeze "$2" /proc/self/fd/3
        printf '%s\n' "$device"
    "#;
    let operation = if frozen_device.is_some() {
        "--unfreeze"
    } else {
        "--freeze"
    };
    tokio::time::timeout(std::time::Duration::from_secs(30), async move {
        Executor::new(envd)
            .with_root_user()
            .run_command_with_opts(
                "/agentenv/bin/busybox",
                &[
                    "sh",
                    "-c",
                    script,
                    "freeze-volume",
                    mount,
                    operation,
                    frozen_device.unwrap_or_default(),
                ],
                &crate::sandbox::ProcessOpts::default().with_cwd("/"),
            )
            .await
    })
    .await
    .context("volume filesystem freeze/thaw timed out")?
}

async fn mount_guest_path_with_options(
    envd: EnvdInstance,
    options: &[&str],
    source: &str,
    target: &str,
) -> Result<()> {
    let mut args = vec!["mount".to_owned(), "-n".to_owned()];
    args.extend(options.iter().map(|option| (*option).to_owned()));
    args.extend([source.to_owned(), target.to_owned()]);
    let output = run_guest_command(
        envd.clone(),
        "/agentenv/bin/busybox".to_owned(),
        args.clone(),
    )
    .await?;
    if output.exit_code == 0 {
        return Ok(());
    }

    // A stale snapshot mount must not be accepted just because its target
    // matches. Remove it and retry with the requested source/options.
    let _ = run_guest_command(
        envd.clone(),
        "/agentenv/bin/busybox".to_owned(),
        vec!["umount".to_owned(), "-l".to_owned(), target.to_owned()],
    )
    .await;
    let retry = run_guest_command(envd, "/agentenv/bin/busybox".to_owned(), args).await?;
    if retry.exit_code == 0 {
        return Ok(());
    }
    anyhow::bail!(
        "failed to mount {} at {} during snapshot launch (exit code {}): {}",
        source,
        target,
        retry.exit_code,
        retry.stderr.trim()
    )
}
