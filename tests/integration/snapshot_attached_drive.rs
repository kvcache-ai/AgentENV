use agentenv::snapshot::SnapshotPlacement;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common;

use agentenv::cfg::ConfigManager;
use agentenv::sandbox::{
    ExtraDrive, ExtraDriveKind, FirecrackerSandbox, FirecrackerSandboxConfig, OverlaybdConfig,
    SandboxBackend, SandboxExecutor, SandboxLaunchConfig,
};
use agentenv::snapshot::{
    CommittedAttachedDrive, OverlaybdLayerRef, SnapshotAlias, SnapshotId, SnapshotManager,
    SnapshotPublishMetadata, SnapshotRecord, SnapshotRuntimeVersions,
};
use agentenv::template::{TemplateBuildSpec, TemplateBuilder};
use agentenv::types::{SandboxId, SandboxResources};
use anyhow::{anyhow, Result};
use tempfile::tempdir;

fn test_parts(root: &Path) -> (TemplateBuilder, SnapshotManager) {
    let (builder, snapshot_manager, _) = common::snapshot_test_parts(root);
    (builder, snapshot_manager)
}

fn overlaybd_lower_count(image_config: &Path) -> Result<usize> {
    let value: serde_json::Value = serde_json::from_slice(&fs::read(image_config)?)?;
    Ok(value
        .get("lowers")
        .and_then(serde_json::Value::as_array)
        .map(|lowers| lowers.len())
        .unwrap_or(0))
}

fn assert_committed_attached_drive_metadata(
    snapshot: &agentenv::snapshot::CommittedSnapshot,
    image_config: &Path,
    read_only: bool,
    expected_layer_count: usize,
) {
    assert_eq!(snapshot.attached_drives.len(), 1);
    match &snapshot.attached_drives[0] {
        CommittedAttachedDrive::Overlaybd {
            layers,
            read_only: actual_read_only,
            mount_path,
            virtual_size,
            ..
        } => {
            assert_eq!(*actual_read_only, read_only);
            assert_eq!(mount_path, &PathBuf::from("/mnt/data"));
            assert!(
                *virtual_size > 0,
                "committed attached drive virtual_size should be the actual runtime size"
            );
            assert_eq!(layers.len(), expected_layer_count);
            let mut managed_layers = 0usize;
            for layer in layers {
                match layer {
                    OverlaybdLayerRef::Managed(layer) => {
                        managed_layers += 1;
                        assert!(
                            layer.digest.starts_with("sha256:"),
                            "attached drive layer should be represented by content-addressed managed-layer digest"
                        );
                        assert_ne!(
                            layer.digest,
                            image_config.display().to_string(),
                            "committed metadata must not persist the build-time image config path"
                        );
                    }
                    OverlaybdLayerRef::External(layer) => {
                        assert!(
                            !layer.digest.is_empty(),
                            "external attached drive layer should keep its source digest"
                        );
                        assert!(
                            !layer.repo_blob_url.is_empty(),
                            "external attached drive layer should keep its repoBlobUrl"
                        );
                        assert_ne!(
                            layer.repo_blob_url,
                            image_config.display().to_string(),
                            "committed metadata must not persist the build-time image config path"
                        );
                    }
                }
            }
            if !read_only {
                assert!(
                    managed_layers >= 1,
                    "writable attached drive snapshots should include at least one managed write layer"
                );
            }
        }
    }
}

async fn assert_snapshot_attached_drive_is_visible(
    sandbox: &FirecrackerSandbox,
    drive_id: &str,
    read_only: bool,
) -> Result<()> {
    // Under the current tools-drive boot contract:
    //   vda = tools drive
    //   vdb = user image
    //   vdc+ = extra drives
    // This test uses exactly one attached drive, so it must appear as /dev/vdc
    // and be mounted at /mnt/<drive_id> inside the guest.
    let command = format!(
        "test -b /dev/vdc && test -d /mnt/{drive_id} && \
         ro=$(cat /sys/block/vdc/ro) && \
         opts=$(awk '$2==\"/mnt/{drive_id}\" {{print $4}}' /proc/mounts) && \
         printf '%s|%s' \"$ro\" \"$opts\""
    );
    let output = sandbox.run_command("sh", &["-lc", &command]).await?;
    assert_eq!(output.exit_code, 0);
    let stdout = output.stdout.trim();
    let (ro, mount_opts) = stdout
        .split_once('|')
        .ok_or_else(|| anyhow!("unexpected attached drive probe output: {stdout:?}"))?;
    assert_eq!(ro, if read_only { "1" } else { "0" });
    assert!(
        mount_opts
            .split(',')
            .any(|opt| opt == if read_only { "ro" } else { "rw" }),
        "expected /mnt/{drive_id} mount options to include {}, got {:?}",
        if read_only { "ro" } else { "rw" },
        mount_opts
    );
    Ok(())
}

fn small_sandbox_config(image_config: &Path) -> Result<FirecrackerSandboxConfig> {
    let user_image = OverlaybdConfig {
        image_config_path: image_config.to_path_buf(),
        read_only: false,
        runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
    };
    let mut config = FirecrackerSandboxConfig::from_global_config_with_user_image(user_image)?;
    config.vcpu_count = 1;
    config.mem_size_mib = 128;
    Ok(config)
}

async fn publish_sandbox_snapshot_with_attached_drive(
    snapshot_manager: &SnapshotManager,
    alias: &str,
    image_config: &Path,
    read_only: bool,
    setup_cmd: &str,
) -> Result<SnapshotRecord> {
    let mut config = small_sandbox_config(image_config)?;
    config.common.extra_drives = vec![ExtraDrive::try_new_overlaybd(
        "data",
        image_config.to_path_buf(),
        read_only,
    )?];

    let mut sandbox = FirecrackerSandbox::new(config, SandboxId::new(), None)?;
    sandbox.start().await?;
    assert_snapshot_attached_drive_is_visible(&sandbox, "data", read_only).await?;

    let setup = sandbox.run_command("sh", &["-lc", setup_cmd]).await?;
    assert_eq!(setup.exit_code, 0);

    let captured = SandboxBackend::snapshot(
        &mut sandbox,
        agentenv::snapshot::SnapshotId::generate(),
        SnapshotPlacement::Distributed,
    )
    .await?;
    sandbox.stop().await?;
    drop(sandbox);
    publish_capture(snapshot_manager, alias, captured).await
}

async fn publish_capture(
    snapshot_manager: &SnapshotManager,
    alias: &str,
    captured: std::sync::Arc<agentenv::snapshot::Snapshot>,
) -> Result<SnapshotRecord> {
    let metadata = SnapshotPublishMetadata {
        snapshot_type: agentenv::snapshot::SnapshotPlacement::Distributed,
        id: captured.id().clone(),
        alias: Some(SnapshotAlias::parse(alias)?),
        source: agentenv::snapshot::SnapshotPublishSource::Sandbox {
            source_sandbox_id: "test-sandbox".to_string(),
            source_sandbox_metadata: None,
        },
        context: agentenv::snapshot::CommandContext::default(),
        startup: None,
        resources: SandboxResources {
            cpu_count: 1,
            memory_mib: 128,
            disk_size_mib: 0,
        },
        runtime_versions: SnapshotRuntimeVersions {
            kernel_version: "test-kernel".to_string(),
            firecracker_version: "test-firecracker".to_string(),
            envd_version: "test-envd".to_string(),
            tools_drive_version: ConfigManager::global_config()
                .resolved_tools_version()
                .to_string(),
        },
        virtualization_mode: agentenv::cfg::ConfigManager::global_config().virtualization_mode,
        image_configs: agentenv::types::ImageConfigs::new(),
        volume_snapshots: Vec::new(),
        custom_extension_params: None,
    };

    Ok(agentenv::sandbox::publish_captured(snapshot_manager, metadata, captured).await?)
}

#[tokio::test]
async fn frozen_volumes_survive_deletion_without_vm_snapshot() -> Result<()> {
    use agentenv::volume::{VolumeManager, VolumeMode};

    common::setup().await;
    let root = tempdir()?;
    let (_, _, repository) = common::snapshot_test_parts(root.path());
    let volumes =
        VolumeManager::open_with_repository(root.path().join("volumes"), repository).await?;
    let mut config = common::default_sandbox_config()?;
    config.vcpu_count = 2;
    config.mem_size_mib = 512;
    for name in ["data", "archive"] {
        let volume = volumes
            .create(name.to_owned(), VolumeMode::Exclusive, None, None, 1024)
            .await?;
        let image_config = volume.backing_image_config.expect("local volume config");
        config.common.extra_drives.push(ExtraDrive::Overlaybd {
            drive_id: volume.id,
            image_config_path: image_config.clone(),
            read_only: false,
            mount_path: PathBuf::from(format!("/{name}")),
            virtual_size: Some(1024 * 1024 * 1024),
            sub_path: None,
            snapshot_output_dir: Some(image_config.parent().unwrap().to_path_buf()),
            kind: ExtraDriveKind::Volume,
        });
    }
    let mut worker =
        FirecrackerSandbox::new(config.clone(), agentenv::types::SandboxId::new(), None)?;
    worker.start().await?;
    let worker_result: Result<()> = async {
        let output = worker
            .run_command(
                "/agentenv/bin/busybox",
                &[
                    "sh",
                    "-c",
                    r#"
        set -eu
        mkdir /archive/nested
        i=0
        while [ "$i" -lt 2000 ]; do
            printf 'marker-%s\n' "$i" > /data/file-$i
            printf 'marker-%s\n' "$i" > /archive/nested/file-$i
            i=$((i + 1))
        done
        /agentenv/bin/busybox umount /archive
    "#,
                ],
            )
            .await?;
        anyhow::ensure!(output.exit_code == 0, "{}", output.stderr);
        // Failure on the second mount must thaw the first mount, and must never
        // freeze the root filesystem now visible beneath the unmounted path.
        let error = worker
            .freeze_and_snapshot_volumes()
            .await
            .err()
            .ok_or_else(|| anyhow!("capture should reject the unmounted volume"))?;
        anyhow::ensure!(!error.is_terminal(), "{error}");
        let output = worker
        .executor()?
        .run_command_with_opts(
            "/agentenv/bin/busybox",
            &[
                "sh",
                "-c",
                "echo recovered > /data/recovered && /agentenv/bin/busybox mount /dev/vdd /archive",
            ],
            &agentenv::sandbox::ProcessOpts::default()
                .with_timeout(std::time::Duration::from_secs(10)),
        )
        .await?;
        anyhow::ensure!(output.exit_code == 0, "{}", output.stderr);
        worker.freeze_and_snapshot_volumes().await?;
        let output = worker
            .run_command(
                "/agentenv/bin/busybox",
                &["mount", "--bind", "/archive", "/data"],
            )
            .await?;
        anyhow::ensure!(output.exit_code == 0, "{}", output.stderr);
        anyhow::ensure!(
            worker.thaw_volumes().await.is_err(),
            "thaw must reject a replaced mount"
        );
        let output = worker
            .run_command("/agentenv/bin/busybox", &["umount", "/data"])
            .await?;
        anyhow::ensure!(output.exit_code == 0, "{}", output.stderr);
        worker.thaw_volumes().await?;
        let output = worker
            .run_command(
                "/agentenv/bin/busybox",
                &["sh", "-c", "echo thawed > /data/thawed"],
            )
            .await?;
        anyhow::ensure!(output.exit_code == 0, "{}", output.stderr);
        worker.freeze_and_snapshot_volumes().await?;
        Ok(())
    }
    .await;
    let worker_stop = worker.stop().await;
    worker_result?;
    worker_stop?;

    let ExtraDrive::Overlaybd { sub_path, .. } = config
        .common
        .extra_drives
        .iter_mut()
        .find(|drive| drive.mount_path() == Path::new("/archive"))
        .ok_or_else(|| anyhow!("archive drive missing"))?;
    *sub_path = Some(PathBuf::from("nested"));
    config.common.stdout_path = Some(root.path().join("replacement-serial"));
    config.common.default_user = Some("nobody".to_owned());
    config.common.default_workdir = Some("/".to_owned());

    let mut replacement = FirecrackerSandbox::new(config, agentenv::types::SandboxId::new(), None)?;
    replacement.start().await?;
    let replacement_result: Result<()> = async {
        let output = replacement
            .run_command(
                "/agentenv/bin/busybox",
                &[
                    "sh",
                    "-c",
                    r#"
        set -eu
        test "$(id -u)" != 0
        test "$(cat /data/recovered)" = recovered
        test "$(cat /data/thawed)" = thawed
        i=0
        while [ "$i" -lt 2000 ]; do
            read -r value < /data/file-$i
            test "$value" = "marker-$i"
            read -r value < /archive/file-$i
            test "$value" = "marker-$i"
            i=$((i + 1))
        done
    "#,
                ],
            )
            .await?;
        anyhow::ensure!(output.exit_code == 0, "{}", output.stderr);
        // Read kernel messages from the host so this check does not need a
        // root executor in a sandbox whose default user is nobody.
        let kernel_log = fs::read_to_string(replacement.firecracker_stdout_path())?;
        for device in ["vdc", "vdd"] {
            let prefix = format!("EXT4-fs ({device})");
            anyhow::ensure!(
                kernel_log.contains(&prefix),
                "missing {prefix} messages: {kernel_log}"
            );
            anyhow::ensure!(
                !kernel_log
                    .lines()
                    .any(|line| line.contains(&prefix) && line.contains("recovery complete")),
                "unexpected journal recovery: {kernel_log}"
            );
        }
        // Freeze and seal through a subPath bind mount as well.
        replacement.freeze_and_snapshot_volumes().await?;
        Ok(())
    }
    .await;
    let replacement_stop = replacement.stop().await;
    replacement_result?;
    replacement_stop?;
    Ok(())
}

#[tokio::test]
async fn capture_export_failure_keeps_restacked_runtime_usable() -> Result<()> {
    common::setup().await;
    let mut config = common::default_sandbox_config()?;
    config.vcpu_count = 1;
    config.mem_size_mib = 128;
    let image_config = config.common.rootfs_image_config.as_ref().unwrap();
    config.common.extra_drives = vec![ExtraDrive::try_new_overlaybd(
        "data",
        image_config.image_config_path.clone(),
        true,
    )?];
    let mut sandbox = FirecrackerSandbox::new(config, SandboxId::new(), None)?;
    sandbox.start().await?;
    let work_dir = sandbox.work_rootfs_path().parent().unwrap().to_path_buf();
    let rootfs_config = work_dir.join("overlaybd/image.json");
    let saved_config = fs::read(&rootfs_config)?;

    fs::remove_file(&rootfs_config)?;
    let err = SandboxBackend::fork(
        &mut sandbox,
        &[agentenv::sandbox::SandboxForkSpec {
            sandbox_id: SandboxId::new(),
            storage_root: PathBuf::from("/unused"),
            envd_access_token: None,
            extra_drives: Vec::new(),
            replace_drive_ids: Vec::new(),
        }],
    )
    .await
    .err()
    .expect("missing config must fail before restack");
    assert!(!err.is_terminal(), "{err:#}");
    fs::write(&rootfs_config, saved_config)?;
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        sandbox.run_command("echo", &["still-running"]),
    )
    .await??;
    assert_eq!(output.exit_code, 0);
    assert_eq!(output.stdout.trim(), "still-running");

    let lower_count = overlaybd_lower_count(&rootfs_config)?;
    let drive_config = work_dir.join("extra-drive-runtime-data/image.json");
    let saved_drive_config = fs::read(&drive_config)?;
    fs::remove_file(&drive_config)?;
    let err = sandbox
        .capture_snapshot(
            agentenv::snapshot::SnapshotId::generate(),
            SnapshotPlacement::Distributed,
        )
        .await
        .expect_err("missing drive config must fail after rootfs restack");
    assert!(!err.is_terminal(), "{err:#}");
    assert_eq!(overlaybd_lower_count(&rootfs_config)?, lower_count + 1);
    fs::write(drive_config, saved_drive_config)?;
    assert_eq!(sandbox.run_command("true", &[]).await?.exit_code, 0);
    drop(
        SandboxBackend::snapshot(
            &mut sandbox,
            agentenv::snapshot::SnapshotId::generate(),
            SnapshotPlacement::Distributed,
        )
        .await?,
    );
    sandbox.stop().await?;
    Ok(())
}

#[tokio::test]
async fn parallel_drive_capture_failure_preserves_completed_restack() -> Result<()> {
    common::setup().await;
    let default = common::default_sandbox_config()?;
    let mut rootfs = default.common.rootfs_image_config.unwrap();
    let image = rootfs.image_config_path.clone();
    rootfs.read_only = true;
    let mut config = FirecrackerSandboxConfig::from_global_config_with_user_image(rootfs)?;
    config.vcpu_count = 1;
    config.mem_size_mib = 128;
    config.common.extra_drives = vec![
        ExtraDrive::try_new_overlaybd("good", image.clone(), false)?,
        ExtraDrive::try_new_overlaybd("broken", image, false)?,
    ];
    let mut sandbox = FirecrackerSandbox::new(config, SandboxId::new(), None)?;
    sandbox.start_nowait().await?;
    let work_dir = sandbox.work_rootfs_path().parent().unwrap().to_path_buf();
    let good_config = work_dir.join("extra-drive-runtime-good/image.json");
    let initial_lowers = overlaybd_lower_count(&good_config)?;
    let broken_config = work_dir.join("extra-drive-runtime-broken/image.json");
    let saved_config = fs::read(&broken_config)?;
    fs::remove_file(&broken_config)?;
    let capture = SandboxBackend::snapshot(
        &mut sandbox,
        agentenv::snapshot::SnapshotId::generate(),
        SnapshotPlacement::Distributed,
    )
    .await;
    let good_lowers = overlaybd_lower_count(&good_config)?;
    let error = capture.expect_err("one attached drive must fail capture");
    assert_eq!(good_lowers, initial_lowers + 1);
    assert!(!error.is_terminal(), "{error:#}");
    fs::write(broken_config, saved_config)?;
    drop(
        SandboxBackend::snapshot(
            &mut sandbox,
            agentenv::snapshot::SnapshotId::generate(),
            SnapshotPlacement::Distributed,
        )
        .await?,
    );
    sandbox.stop().await?;
    Ok(())
}

#[tokio::test]
async fn forked_drive_snapshots_survive_parent_and_child_backend_drop() -> Result<()> {
    common::setup().await;
    let mut config = common::default_sandbox_config()?;
    config.vcpu_count = 1;
    config.mem_size_mib = 128;
    let image_path = config
        .common
        .rootfs_image_config
        .as_ref()
        .unwrap()
        .image_config_path
        .clone();
    config.common.extra_drives = vec![
        ExtraDrive::try_new_overlaybd("data", image_path.clone(), false)?,
        ExtraDrive::try_new_overlaybd("readonly", image_path, true)?,
    ];
    let mut parent = FirecrackerSandbox::new(config, SandboxId::new(), None)?;
    parent.start().await?;
    let output = parent
        .run_command(
            "sh",
            &[
                "-c",
                "echo base > /mnt/data/marker; echo base > /tmp/rootfs-marker",
            ],
        )
        .await?;
    assert_eq!(output.exit_code, 0);
    drop(
        SandboxBackend::snapshot(
            &mut parent,
            agentenv::snapshot::SnapshotId::generate(),
            SnapshotPlacement::Distributed,
        )
        .await?,
    );
    let child_storage = tempdir()?;
    let mut children = SandboxBackend::fork(
        &mut parent,
        &[agentenv::sandbox::SandboxForkSpec {
            sandbox_id: SandboxId::new(),
            storage_root: child_storage.path().to_path_buf(),
            envd_access_token: None,
            extra_drives: Vec::new(),
            replace_drive_ids: Vec::new(),
        }],
    )
    .await?;
    let mut child = children.pop().unwrap()?;

    let output = parent
        .run_command(
            "sh",
            &[
                "-c",
                "echo parent > /mnt/data/marker; echo parent > /tmp/rootfs-marker",
            ],
        )
        .await?;
    assert_eq!(output.exit_code, 0);
    drop(
        SandboxBackend::snapshot(
            &mut parent,
            agentenv::snapshot::SnapshotId::generate(),
            SnapshotPlacement::Distributed,
        )
        .await?,
    );
    drop(
        child
            .snapshot(
                agentenv::snapshot::SnapshotId::generate(),
                SnapshotPlacement::Distributed,
            )
            .await?,
    );
    parent.stop().await?;
    drop(parent);
    drop(
        child
            .snapshot(
                agentenv::snapshot::SnapshotId::generate(),
                SnapshotPlacement::Distributed,
            )
            .await?,
    );

    let paused = child.pause().await?;
    child.stop().await?;
    drop(child);
    let paused = paused
        .downcast_ref::<agentenv::sandbox::FirecrackerPausedState>()
        .unwrap();
    let snapshot = paused.snapshot_config();
    for drive in &snapshot.common.extra_drives {
        let ExtraDrive::Overlaybd {
            image_config_path, ..
        } = drive;
        let image = overlaybd::config::load_image_config(image_config_path)?;
        assert!(image
            .lowers
            .iter()
            .all(|layer| { layer.file.is_empty() || Path::new(&layer.file).is_file() }));
    }
    let mut resumed = FirecrackerSandbox::from_paused_state(
        paused,
        SandboxId::new(),
        None,
        paused.snapshot_config().vm_state_path.parent().unwrap(),
    )?;
    resumed.start().await?;
    let output = resumed
        .run_command(
            "sh",
            &[
                "-c",
                "cat /mnt/data/marker /tmp/rootfs-marker; cat /sys/block/vdd/ro",
            ],
        )
        .await?;
    assert_eq!(output.exit_code, 0);
    assert_eq!(output.stdout, "base\nbase\n1\n");
    resumed.stop().await?;
    Ok(())
}

#[tokio::test]
async fn attached_overlaybd_drive_is_visible_on_snapshot_based_build() -> Result<()> {
    common::setup().await;
    let store = tempdir()?;
    let (builder, snapshot_manager) = test_parts(store.path());
    let alias = unique_alias_for_test("snapshot_attached_overlaybd_base");
    let derived_alias = unique_alias_for_test("snapshot_attached_overlaybd_derived");
    let (_, image_config) = common::default_rootfs_template_build_spec_with_image_config();
    let source_lower_count = overlaybd_lower_count(image_config.as_path())?;

    publish_sandbox_snapshot_with_attached_drive(
        &snapshot_manager,
        &alias,
        &image_config,
        true,
        "mkdir -p /workspace && echo base > /workspace/base.txt",
    )
    .await?;

    let loaded_snapshot = snapshot_manager
        .get(&alias)
        .await?
        .ok_or_else(|| anyhow!("snapshot should exist"))?;
    assert_committed_attached_drive_metadata(
        loaded_snapshot
            .committed
            .as_ref()
            .expect("snapshot should be committed"),
        image_config.as_path(),
        true,
        source_lower_count,
    );

    let runnable = snapshot_manager
        .resolve_runnable(loaded_snapshot.clone())
        .await?;
    let mut sandbox =
        FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default(), None)?;
    sandbox.start().await?;
    assert_snapshot_attached_drive_is_visible(&sandbox, "data", true).await?;
    sandbox.stop().await?;

    let runnable = snapshot_manager
        .load_runnable(&alias)
        .await?
        .ok_or_else(|| anyhow!("base runnable snapshot should exist"))?;
    let derived_id = SnapshotId::generate();
    builder
        .build_from_snapshot_and_publish(
            &snapshot_manager,
            TemplateBuildSpec::new()
                .alias(derived_alias.clone())
                .resources(1, 128)
                .run("echo rebuilt > /workspace/rebuilt.txt"),
            derived_id,
            &runnable,
        )
        .await?;

    let derived = snapshot_manager
        .get(&derived_alias)
        .await?
        .ok_or_else(|| anyhow!("derived snapshot should exist"))?;
    assert_committed_attached_drive_metadata(
        derived
            .committed
            .as_ref()
            .expect("derived snapshot should be committed"),
        image_config.as_path(),
        true,
        source_lower_count,
    );

    let runnable = snapshot_manager.resolve_runnable(derived).await?;
    let mut sandbox =
        FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default(), None)?;
    sandbox.start().await?;
    assert_snapshot_attached_drive_is_visible(&sandbox, "data", true).await?;

    let base_out = sandbox.run_command("cat", &["/workspace/base.txt"]).await?;
    assert_eq!(base_out.exit_code, 0);
    assert_eq!(base_out.stdout.trim(), "base");

    let rebuilt_out = sandbox
        .run_command("cat", &["/workspace/rebuilt.txt"])
        .await?;
    assert_eq!(rebuilt_out.exit_code, 0);
    assert_eq!(rebuilt_out.stdout.trim(), "rebuilt");
    sandbox.stop().await?;

    Ok(())
}

#[tokio::test]
async fn writable_attached_overlaybd_drive_preserves_metadata_and_mount_mode() -> Result<()> {
    common::setup().await;
    let store = tempdir()?;
    let (_, snapshot_manager) = test_parts(store.path());
    let alias = unique_alias_for_test("snapshot_attached_overlaybd_writable");
    let (_, image_config) = common::default_rootfs_template_build_spec_with_image_config();
    let source_lower_count = overlaybd_lower_count(image_config.as_path())?;

    publish_sandbox_snapshot_with_attached_drive(
        &snapshot_manager,
        &alias,
        &image_config,
        false,
        "mkdir -p /workspace && echo writable > /workspace/base.txt",
    )
    .await?;

    let committed = snapshot_manager
        .get(&alias)
        .await?
        .ok_or_else(|| anyhow!("snapshot should exist"))?;
    assert_committed_attached_drive_metadata(
        committed
            .committed
            .as_ref()
            .expect("snapshot should be committed"),
        image_config.as_path(),
        false,
        source_lower_count + 1,
    );

    let runnable = snapshot_manager.resolve_runnable(committed).await?;
    let mut sandbox =
        FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default(), None)?;
    sandbox.start().await?;
    assert_snapshot_attached_drive_is_visible(&sandbox, "data", false).await?;
    sandbox.stop().await?;

    Ok(())
}

async fn assert_guest_files(sandbox: &FirecrackerSandbox, files: &[&str]) -> Result<()> {
    for file in files {
        let output = sandbox
            .run_command("cat", &[&format!("/mnt/data/{file}")])
            .await?;
        anyhow::ensure!(
            output.exit_code == 0 && output.stdout.trim() == *file,
            "unexpected /mnt/data/{file}: {output:?}"
        );
    }
    Ok(())
}

async fn write_guest_file(sandbox: &FirecrackerSandbox, file: &str) -> Result<()> {
    let output = sandbox
        .run_command(
            "sh",
            &["-lc", &format!("echo {file} > /mnt/data/{file} && sync")],
        )
        .await?;
    anyhow::ensure!(output.exit_code == 0, "{}", output.stderr);
    Ok(())
}

/// A drive added to a snapshot launch keeps its reserved slot through
/// capture, relaunch, fork, and a second capture.
#[tokio::test]
async fn launch_time_attached_drive_keeps_its_slot_across_captures() -> Result<()> {
    common::setup().await;
    let store = tempdir()?;
    let (_, snapshot_manager) = test_parts(store.path());
    let (_, image_config) = common::default_rootfs_template_build_spec_with_image_config();

    let mut base =
        FirecrackerSandbox::new(small_sandbox_config(&image_config)?, SandboxId::new(), None)?;
    base.start().await?;
    let captured = SandboxBackend::snapshot(
        &mut base,
        SnapshotId::generate(),
        SnapshotPlacement::Distributed,
    )
    .await?;
    base.stop().await?;
    let base_alias = unique_alias_for_test("launch_drive_base");
    publish_capture(&snapshot_manager, &base_alias, captured).await?;

    let runnable = snapshot_manager
        .load_runnable(&base_alias)
        .await?
        .ok_or_else(|| anyhow!("base snapshot should exist"))?;
    let launch_config = SandboxLaunchConfig {
        extra_drives: vec![
            ExtraDrive::try_new_overlaybd("data", image_config.clone(), false)?
                .with_kind(ExtraDriveKind::LaunchSlot),
        ],
        ..SandboxLaunchConfig::default()
    };
    let mut first = FirecrackerSandbox::from_snapshot(&runnable, &launch_config, None)?;
    first.start().await?;
    assert_snapshot_attached_drive_is_visible(&first, "data", false).await?;
    write_guest_file(&first, "first").await?;
    let captured = SandboxBackend::snapshot(
        &mut first,
        SnapshotId::generate(),
        SnapshotPlacement::Distributed,
    )
    .await?;
    first.stop().await?;
    let first_alias = unique_alias_for_test("launch_drive_first");
    publish_capture(&snapshot_manager, &first_alias, captured).await?;

    let runnable = snapshot_manager
        .load_runnable(&first_alias)
        .await?
        .ok_or_else(|| anyhow!("first snapshot should exist"))?;
    let manifest = runnable.manifest();
    assert_eq!(manifest.attached_drives.len(), 1);
    assert_eq!(manifest.effective_physical_extra_drive_count(), 0);
    assert_eq!(manifest.free_drive_slots() + 1, manifest.volume_drive_slots);

    let mut second =
        FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default(), None)?;
    second.start().await?;
    let second_result: Result<()> = async {
        assert_snapshot_attached_drive_is_visible(&second, "data", false).await?;
        assert_guest_files(&second, &["first"]).await?;
        write_guest_file(&second, "second").await?;

        // A fork child owns an immutable capture while its parent runs on.
        let captured = second
            .capture_snapshot(SnapshotId::generate(), SnapshotPlacement::Local)
            .await?;
        second.resume().await?;
        let mut child = FirecrackerSandbox::from_snapshot_files(
            captured,
            agentenv::sandbox::FirecrackerCommonConfig::from_global_config()?,
            SandboxId::new(),
            None,
        )?;
        child.start().await?;
        let child_result = assert_guest_files(&child, &["first", "second"]).await;
        child.stop().await?;
        child_result?;

        let captured = SandboxBackend::snapshot(
            &mut second,
            SnapshotId::generate(),
            SnapshotPlacement::Distributed,
        )
        .await?;
        let second_alias = unique_alias_for_test("launch_drive_second");
        publish_capture(&snapshot_manager, &second_alias, captured).await?;
        let runnable = snapshot_manager
            .load_runnable(&second_alias)
            .await?
            .ok_or_else(|| anyhow!("second snapshot should exist"))?;
        assert_eq!(
            runnable.manifest().effective_physical_extra_drive_count(),
            0
        );
        let mut third =
            FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default(), None)?;
        third.start().await?;
        let third_result = assert_guest_files(&third, &["first", "second"]).await;
        third.stop().await?;
        third_result
    }
    .await;
    let second_stop = second.stop().await;
    second_result?;
    second_stop?;
    Ok(())
}

fn unique_alias_for_test(prefix: &str) -> String {
    format!(
        "{prefix}_{:x}{:x}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}
