use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common;

use agentenv::cfg::ConfigManager;
use agentenv::sandbox::{
    ExtraDrive, FirecrackerSandbox, FirecrackerSandboxConfig, OverlaybdConfig, SandboxBackend,
    SandboxExecutor, SandboxLaunchConfig,
};
use agentenv::snapshot::{
    CommittedAttachedDrive, OverlaybdLayerRef, SnapshotAlias, SnapshotId, SnapshotManager,
    SnapshotPublishMetadata, SnapshotRecord, SnapshotRuntimeVersions,
};
use agentenv::template::{TemplateBuildSpec, TemplateBuilder};
use agentenv::types::SandboxResources;
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

async fn publish_sandbox_snapshot_with_attached_drive(
    snapshot_manager: &SnapshotManager,
    alias: &str,
    image_config: &Path,
    read_only: bool,
    setup_cmd: &str,
) -> Result<SnapshotRecord> {
    let user_image = OverlaybdConfig {
        image_config_path: image_config.to_path_buf(),
        read_only: false,
        runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
    };
    let mut config = FirecrackerSandboxConfig::from_global_config_with_user_image(user_image)?;
    config.vcpu_count = 1;
    config.mem_size_mib = 128;
    config.common.extra_drives = vec![ExtraDrive::try_new_overlaybd(
        "data",
        image_config.to_path_buf(),
        read_only,
    )?];

    let mut sandbox = FirecrackerSandbox::new(config)?;
    sandbox.start().await?;
    assert_snapshot_attached_drive_is_visible(&sandbox, "data", read_only).await?;

    let setup = sandbox.run_command("sh", &["-lc", setup_cmd]).await?;
    assert_eq!(setup.exit_code, 0);

    let captured = SandboxBackend::snapshot(&mut sandbox).await?;
    let _ = sandbox.stop().await;

    let metadata = SnapshotPublishMetadata {
        id: SnapshotId::generate(),
        alias: Some(SnapshotAlias::parse(alias)?),
        source: agentenv::snapshot::SnapshotPublishSource::Sandbox {
            source_sandbox_id: "test-sandbox".to_string(),
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
        image_configs: agentenv::types::ImageConfigs::new(),
        custom_extension_params: None,
    };

    Ok(snapshot_manager
        .publish_captured(metadata, captured)
        .await?)
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
        FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default())?;
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
        FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default())?;
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
        FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default())?;
    sandbox.start().await?;
    assert_snapshot_attached_drive_is_visible(&sandbox, "data", false).await?;
    sandbox.stop().await?;

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
