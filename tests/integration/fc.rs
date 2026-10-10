use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agentenv::cfg::ConfigManager;
use agentenv::sandbox::{
    BaseSandboxNetworkPolicy, FirecrackerSandbox, FirecrackerSnapshotConfig, ProcessOutput,
    SandboxBackend, SandboxExecutor, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
};
use anyhow::{bail, Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::ImageConfig;
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::is_zfile;

use crate::common;

const DISK_MARKER: &str = "agentenv-disk-test-ok";
const DISK_MARKER_PATH: &str = "/tmp/agentenv-test-marker";

fn lower_file_paths_from_image_config(image_config_path: &Path) -> Result<Vec<PathBuf>> {
    let image_config: ImageConfig = serde_json::from_slice(
        &fs::read(image_config_path)
            .with_context(|| format!("read image config {}", image_config_path.display()))?,
    )
    .with_context(|| format!("parse image config {}", image_config_path.display()))?;
    let image_config_dir = image_config_path
        .parent()
        .context("image config should have a parent directory")?;

    image_config
        .lowers
        .into_iter()
        .filter_map(|lower| {
            if !lower.file.is_empty() {
                Some(lower.file)
            } else if !lower.dir.is_empty() {
                Some(lower.dir)
            } else {
                None
            }
        })
        .map(|lower| {
            let lower_path = Path::new(&lower);
            let resolved = if lower_path.is_absolute() {
                lower_path.to_path_buf()
            } else {
                image_config_dir.join(lower_path)
            };
            Ok(fs::canonicalize(&resolved).unwrap_or(resolved))
        })
        .collect()
}

fn assert_no_lower_paths_under(paths: &[PathBuf], root: &Path, context: &str) {
    let canonical_root = fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    assert!(
        paths.iter().all(|path| !path.starts_with(&canonical_root)),
        "{context} should not reference managed snapshot lower paths under {}: {paths:?}",
        canonical_root.display()
    );
}

/// Write a marker file to guest disk via envd and verify it was written.
async fn write_disk_marker(sandbox: &mut FirecrackerSandbox) -> Result<()> {
    let output = sandbox
        .run_command(
            "sh",
            &["-c", &format!("echo {DISK_MARKER} > {DISK_MARKER_PATH}")],
        )
        .await?;
    if output.exit_code != 0 {
        bail!("write marker failed: {}", output.stderr);
    }
    Ok(())
}

/// Read the marker file from guest disk and verify it contains the expected value.
async fn verify_disk_marker(sandbox: &mut FirecrackerSandbox) -> Result<()> {
    let output = sandbox.run_command("cat", &[DISK_MARKER_PATH]).await?;
    if output.exit_code != 0 {
        bail!(
            "read marker failed (exit {}): {}",
            output.exit_code,
            output.stderr
        );
    }
    assert!(
        output.stdout.trim().contains(DISK_MARKER),
        "marker mismatch: expected '{}', got '{}'",
        DISK_MARKER,
        output.stdout.trim()
    );
    Ok(())
}

/// Assert that the newest local memory lower of `snapshot` is a raw LSMT
/// layer (no ZFile wrapper). Capture-time compression was removed: local
/// layers always stay raw, so local resume pays no decompression cost.
/// Compression, when enabled via `[snapshot.publish_compression]`, happens
/// once at publish time on the repository upload path.
///
/// Image config lowers are ordered bottom-to-top (oldest base layer first), so
/// the newest local lower is the last entry with a `file` path.
async fn assert_memory_layer_is_raw(snapshot: &FirecrackerSnapshotConfig) -> Result<()> {
    let image_config_path = &snapshot.mem_overlaybd_config.image_config_path;
    let image_config: ImageConfig = serde_json::from_slice(
        &fs::read(image_config_path)
            .with_context(|| format!("read image config {}", image_config_path.display()))?,
    )
    .with_context(|| format!("parse image config {}", image_config_path.display()))?;
    let image_config_dir = image_config_path
        .parent()
        .context("image config should have a parent directory")?;
    let newest_lower = image_config
        .lowers
        .iter()
        .rev()
        .find(|lower| !lower.file.is_empty())
        .context("memory snapshot should reference at least one local lower")?;
    let lower_path = Path::new(&newest_lower.file);
    let lower_path = if lower_path.is_absolute() {
        lower_path.to_path_buf()
    } else {
        image_config_dir.join(lower_path)
    };

    let file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_ro(&lower_path)
            .with_context(|| format!("open memory lower {}", lower_path.display()))?,
    );
    let zfile_flag = is_zfile(file)
        .await
        .with_context(|| format!("probe zfile header of {}", lower_path.display()))?;
    assert_eq!(
        zfile_flag,
        0,
        "memory lower {} should be a raw LSMT layer",
        lower_path.display()
    );
    Ok(())
}

#[tokio::test]
async fn microvm_lifecycle_and_snapshot_preserve_disk_state() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let snapshot = sandbox.pause().await?;
    assert!(snapshot.vm_state_path.exists());
    assert!(snapshot.mem_overlaybd_config.image_config_path.exists());

    sandbox.resume().await?;
    verify_disk_marker(&mut sandbox).await?;

    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
    verify_disk_marker(&mut resumed).await?;
    resumed.stop().await?;
    Ok(())
}

async fn serve_tools_rootfs(
    rootfs: &Path,
) -> Result<(String, tokio::task::JoinHandle<std::io::Result<()>>)> {
    use axum::{extract::Path as RoutePath, routing::get, Router};
    use bytes::Bytes;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    let mut archive = tar::Builder::new(Vec::new());
    archive.follow_symlinks(false);
    archive.append_dir_all(".", rootfs)?;
    let layer = archive.into_inner()?;
    let digest = |bytes: &[u8]| format!("sha256:{:x}", Sha256::digest(bytes));
    let layer_digest = digest(&layer);
    let config = serde_json::to_vec(&json!({
        "architecture": if std::env::consts::ARCH == "x86_64" { "amd64" } else { "arm64" },
        "os": "linux",
        "config": {"Labels": {"io.agentenv.tools-drive.format": "oci-rootfs-v1"}},
        "rootfs": {"type": "layers", "diff_ids": [layer_digest]}
    }))?;
    let config_digest = digest(&config);
    let manifest_type = "application/vnd.oci.image.manifest.v1+json";
    let manifest = Bytes::from(serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": manifest_type,
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len()},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": layer_digest, "size": layer.len()}]
    }))?);
    let manifest_digest = digest(&manifest);
    let blobs = Arc::new(std::collections::HashMap::from([
        (layer_digest, Bytes::from(layer)),
        (config_digest, Bytes::from(config)),
    ]));
    let app = Router::new()
        .route("/v2/", get(|| async { "{}" }))
        .route(
            "/v2/tools/manifests/{reference}",
            get(move || {
                let body = manifest.clone();
                let digest = manifest_digest.clone();
                async move {
                    (
                        [
                            ("content-type", manifest_type.to_string()),
                            ("docker-content-digest", digest),
                        ],
                        body,
                    )
                }
            }),
        )
        .route(
            "/v2/tools/blobs/{digest}",
            get(move |RoutePath(digest): RoutePath<String>| {
                let blobs = Arc::clone(&blobs);
                async move {
                    blobs
                        .get(&digest)
                        .cloned()
                        .ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let registry = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((registry, server))
}

#[tokio::test]
async fn oci_tools_share_a_readonly_device_across_concurrent_launches() -> Result<()> {
    use std::os::unix::fs::{symlink, FileTypeExt, PermissionsExt};

    common::setup().await;
    let global = ConfigManager::global_config();
    let tools_dir = global.deps_path.join("tools");
    fs::create_dir_all(&tools_dir)?;
    let release = tempfile::Builder::new()
        .prefix("0.0.0-oci-")
        .tempdir_in(&tools_dir)?;
    let version = release
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let rootfs = release.path().join("rootfs");
    fs::create_dir(&rootfs)?;

    // Reuse the provisioned guest binaries, regardless of the default tools format.
    let mut seed = FirecrackerSandbox::new(common::default_sandbox_config()?)?;
    seed.start().await?;
    let extracted = tokio::process::Command::new("debugfs")
        .args(["-R", &format!("rdump / {}", rootfs.display())])
        .arg(seed.work_rootfs_path().with_file_name("rootfs.ext4"))
        .output()
        .await;
    seed.stop().await?;
    let extracted = extracted?;
    assert!(
        extracted.status.success(),
        "{}",
        String::from_utf8_lossy(&extracted.stderr)
    );
    fs::write(
        rootfs.join("agentenv/tools-drive-version"),
        format!("{version}\n"),
    )?;

    let mut setup = global.clone();
    setup.home_path = release.path().join("home");
    setup.image.cache.root_dir = release.path().join("image-cache");
    setup.deps_path = release.path().join("deps");
    setup.ublk.overlaybd.global_config_path = release.path().join("config/overlaybd.json");
    setup.memory_snapshot.overlaybd_global_config_path = release.path().join("config/memory.json");
    setup.firecracker.binary_path = Some(global.resolved_firecracker_binary_path());
    setup.kernel.image_path = Some(global.resolved_kernel_image_path());
    setup.tools.drive_path = None;
    setup.tools.version = Some(version.clone());
    let regctl = setup.resolved_regctl_binary();
    fs::create_dir_all(regctl.parent().unwrap())?;
    symlink(
        global.deps_path.join("overlaybd"),
        setup.deps_path.join("overlaybd"),
    )?;
    let (registry, server) = serve_tools_rootfs(&rootfs).await?;
    setup.tools.url = Some(format!("{registry}/tools:{{version}}"));
    let prepared = async {
        fs::write(
            &regctl,
            format!(
                "#!/bin/sh\nexec {} --host reg={registry},tls=disabled \"$@\"\n",
                shell_util::shell_quote(&global.resolved_regctl_binary().to_string_lossy())
            ),
        )?;
        fs::set_permissions(&regctl, fs::Permissions::from_mode(0o755))?;
        agentenv::setup::ensure_provisioning(&setup).await
    }
    .await;
    server.abort();
    prepared?;
    for entry in fs::read_dir(setup.deps_path.join("tools").join(&version))? {
        let entry = entry?;
        fs::rename(entry.path(), release.path().join(entry.file_name()))?;
    }

    let mut config = common::default_sandbox_config()?;
    config.common.tools_drive_version = version.clone();
    let mut first = FirecrackerSandbox::new(config.clone())?;
    let mut second = FirecrackerSandbox::new(config)?;
    tokio::try_join!(first.start(), second.start())?;
    let backing = |sandbox: &FirecrackerSandbox| {
        fs::canonicalize(sandbox.work_rootfs_path().with_file_name("rootfs.ext4"))
    };
    let device = backing(&first)?;
    assert!(fs::metadata(&device)?.file_type().is_block_device());
    assert_eq!(device, backing(&second)?);
    first.stop().await?;
    let output = second
        .run_command(
            "/agentenv/bin/busybox",
            &["cat", "/agentenv/tools-drive-version", "/sys/block/vda/ro"],
        )
        .await?;
    assert_eq!(output.exit_code, 0);
    assert_eq!(output.stdout.trim(), format!("{version}\n1"));
    let paused = second.pause().await?;
    assert_eq!(paused.common.tools_drive_version, version);
    second.resume().await?;
    assert_eq!(device, backing(&second)?);
    second.stop().await?;
    Ok(())
}

/// Shared body of the memory snapshot format test: pause a marked sandbox into
/// a temp dir, assert the direct OverlayBD snapshot artifact layout, validate
/// that the newest memory lower is a raw LSMT layer, and verify that a resume
/// round-trip preserves guest disk state.
async fn run_memory_snapshot_format_and_resume_case() -> Result<()> {
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let snapshot_dir = tempfile::tempdir()?;
    let (snapshot, _) = sandbox.pause_to_dir(snapshot_dir.path()).await?;
    sandbox.stop().await?;

    assert!(snapshot.vm_state_path.exists());
    assert!(snapshot.mem_overlaybd_config.image_config_path.exists());
    assert!(snapshot_dir
        .path()
        .join("mem_overlaybd/overlaybd.commit")
        .exists());
    assert!(snapshot_dir.path().join("mem_image.json").exists());
    assert!(
        !snapshot_dir.path().join("mem.bin").exists(),
        "direct OverlayBD snapshot should not create mem.bin"
    );
    assert_memory_layer_is_raw(&snapshot).await?;

    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
    verify_disk_marker(&mut resumed).await?;
    resumed.stop().await?;
    Ok(())
}

/// Direct OverlayBD memory layers are built from Firecracker memory ranges
/// without an intermediate raw memory file, and are always written raw.
#[tokio::test]
async fn memory_snapshot_format_matches_config_and_resumes() -> Result<()> {
    common::setup().await;
    run_memory_snapshot_format_and_resume_case().await
}

#[tokio::test]
async fn backend_pause_state_round_trips_through_encoded_artifacts() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let tools_version = sandbox_config.common.tools_drive_version.clone();
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let temp = tempfile::tempdir()?;
    let artifact_root = temp.path().join("paused-artifacts");
    let paused_state = SandboxBackend::pause(&mut sandbox, Some(&artifact_root)).await?;
    sandbox.stop().await?;

    let encoded = paused_state.encode()?;
    assert_eq!(encoded["common"]["tools_drive_version"], tools_version);
    assert!(encoded.get("tools").is_none());
    assert!(!artifact_root.join("tools").exists());
    drop(paused_state);

    let decoded =
        agentenv::sandbox::FirecrackerPausedState::decode(artifact_root.clone(), encoded)?;
    let mut resumed =
        FirecrackerSandbox::resume_from_snapshot_config(decoded.snapshot_config()).await?;
    verify_disk_marker(&mut resumed).await?;
    resumed.stop().await?;
    Ok(())
}

/// Verify multi-level snapshot chains: first → second → resume from second after
/// dropping the first snapshot handle.
#[tokio::test]
async fn snapshot_chain_survives_after_parent_snapshot_handle_is_dropped() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let first_snapshot = sandbox.pause().await?;
    assert_memory_layer_is_raw(&first_snapshot).await?;
    sandbox.stop().await?;
    let first_snapshot_dir = fs::canonicalize(first_snapshot.vm_state_path.parent().unwrap())?;
    let first_persistent_generation = fs::canonicalize(
        first_snapshot
            .mem_overlaybd_config
            .image_config_path
            .parent()
            .context("first memory image should live under a persistent generation dir")?,
    )?;
    let first_rootfs_commit = fs::canonicalize(
        first_snapshot
            .common
            .rootfs_image_config
            .as_ref()
            .context("first snapshot rootfs image config should exist")?
            .image_config_path
            .parent()
            .context("first rootfs image should have a parent dir")?
            .join("snapshot.commit"),
    )?;
    assert!(
        first_snapshot_dir.exists(),
        "first snapshot tempdir should exist at {}",
        first_snapshot_dir.display()
    );

    // Resume from first, write additional data, pause again.
    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&first_snapshot).await?;
    verify_disk_marker(&mut resumed).await?;
    let second_snapshot = resumed.pause().await?;
    assert_memory_layer_is_raw(&second_snapshot).await?;
    resumed.stop().await?;
    let second_snapshot_dir = fs::canonicalize(second_snapshot.vm_state_path.parent().unwrap())?;
    let second_mem_lowers = lower_file_paths_from_image_config(
        &second_snapshot.mem_overlaybd_config.image_config_path,
    )?;
    assert!(
        second_mem_lowers
            .iter()
            .all(|path| !path.starts_with(&first_persistent_generation)),
        "second memory snapshot should adopt first generation layers instead of referencing {}: {second_mem_lowers:?}",
        first_persistent_generation.display(),
    );
    assert!(
        second_mem_lowers
            .iter()
            .any(|path| path.starts_with(second_snapshot_dir.join("inherited-layers"))),
        "second memory snapshot should adopt inherited layers under its own generation",
    );
    assert!(
        second_mem_lowers.iter().all(|path| path.exists()),
        "second memory snapshot should reference existing lower files",
    );

    let second_rootfs_lowers = lower_file_paths_from_image_config(
        &second_snapshot
            .common
            .rootfs_image_config
            .as_ref()
            .context("second snapshot rootfs image config should exist")?
            .image_config_path,
    )?;
    let second_rootfs_dir = fs::canonicalize(
        second_snapshot
            .common
            .rootfs_image_config
            .as_ref()
            .context("second snapshot rootfs image config should exist")?
            .image_config_path
            .parent()
            .context("second rootfs image should have a parent dir")?,
    )?;
    assert!(
        second_rootfs_lowers
            .iter()
            .all(|path| path != &first_rootfs_commit),
        "second rootfs snapshot should adopt first generation layers instead of referencing {}: {second_rootfs_lowers:?}",
        first_rootfs_commit.display(),
    );
    assert!(
        second_rootfs_lowers
            .iter()
            .any(|path| path.starts_with(second_rootfs_dir.join("inherited-layers"))),
        "second rootfs snapshot should adopt inherited layers under its own rootfs dir",
    );
    assert!(
        second_rootfs_lowers.iter().all(|path| path.exists()),
        "second rootfs snapshot should reference existing lower files",
    );

    // Drop first snapshot handle — second snapshot's layers should still be valid.
    drop(first_snapshot);

    let mut resumed_again =
        FirecrackerSandbox::resume_from_snapshot_config(&second_snapshot).await?;
    verify_disk_marker(&mut resumed_again).await?;

    // Also test pause_to_dir with snapshot-owned inherited layer adoption.
    let expected_snapshot_dir = tempfile::tempdir()?;
    let (dir_snapshot, _) = resumed_again
        .pause_to_dir(expected_snapshot_dir.path())
        .await?;
    resumed_again.stop().await?;
    let managed_snapshot_base_root = ConfigManager::global_config()
        .firecracker
        .work_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("agentenv"))
        .join("managed-snapshots");
    let dir_snapshot_mem_lowers =
        lower_file_paths_from_image_config(&dir_snapshot.mem_overlaybd_config.image_config_path)?;
    assert_no_lower_paths_under(
        &dir_snapshot_mem_lowers,
        &managed_snapshot_base_root,
        "pause_to_dir memory snapshot",
    );

    let dir_snapshot_rootfs_lowers = lower_file_paths_from_image_config(
        &dir_snapshot
            .common
            .rootfs_image_config
            .as_ref()
            .context("dir snapshot rootfs image config should exist")?
            .image_config_path,
    )?;
    assert_no_lower_paths_under(
        &dir_snapshot_rootfs_lowers,
        &managed_snapshot_base_root,
        "pause_to_dir rootfs snapshot",
    );
    let actual_snapshot_dir = dir_snapshot.vm_state_path.parent().unwrap();
    assert!(
        actual_snapshot_dir == expected_snapshot_dir.path(),
        "snapshot_to_dir should use the provided directory"
    );
    assert!(
        actual_snapshot_dir.exists(),
        "the provided snapshot directory should exist after snapshot_to_dir"
    );
    assert!(
        dir_snapshot_mem_lowers.iter().all(|path| path.exists()),
        "pause_to_dir memory snapshot should keep all lower files after dropping the paused-state handle"
    );
    assert!(
        dir_snapshot_rootfs_lowers.iter().all(|path| path.exists()),
        "pause_to_dir rootfs snapshot should keep all lower files after dropping the paused-state handle"
    );
    drop(second_snapshot);

    let mut resumed_from_dir =
        FirecrackerSandbox::resume_from_snapshot_config(&dir_snapshot).await?;
    verify_disk_marker(&mut resumed_from_dir).await?;
    resumed_from_dir.stop().await?;
    Ok(())
}

/// Verify multiple resumes from the same snapshot each get independent writable
/// state. Each instance reads the shared marker and writes its own file.
#[tokio::test]
async fn multiple_resumes_have_independent_disk_state() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    // Resume multiple VMs sequentially from the same snapshot.
    // Each one should see the original marker and produce its own independent snapshot.
    let vm_count = 2;
    let mut snapshots = Vec::new();
    for i in 0..vm_count {
        let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
        verify_disk_marker(&mut resumed).await?;
        // Write a per-instance marker to prove writable state is independent.
        let per_vm_marker = format!("{DISK_MARKER_PATH}.vm{i}");
        let output = resumed
            .run_command("sh", &["-c", &format!("echo ok > {per_vm_marker}")])
            .await?;
        assert_eq!(
            output.exit_code, 0,
            "per-vm write failed: {}",
            output.stderr
        );
        let snap = resumed.pause().await?;
        resumed.stop().await?;
        snapshots.push(snap);
    }

    // Verify each snapshot still has the original marker.
    for snap in &snapshots {
        let mut sandbox = FirecrackerSandbox::resume_from_snapshot_config(snap).await?;
        verify_disk_marker(&mut sandbox).await?;
        sandbox.stop().await?;
    }

    Ok(())
}

/// Runs a connectivity probe, retrying expected successes so transient public
/// DNS or upstream failures do not fail policy assertions. Expected failures
/// run once: a retry cannot turn a correctly blocked connection into a pass.
async fn run_connectivity_probe(
    sandbox: &mut FirecrackerSandbox,
    cmd: &str,
    args: &[&str],
    should_succeed: bool,
) -> Result<ProcessOutput> {
    const ATTEMPTS: usize = 3;
    let mut attempt = 1;
    loop {
        let output = sandbox.run_command(cmd, args).await?;
        if output.exit_code == 0 || !should_succeed || attempt == ATTEMPTS {
            return Ok(output);
        }
        tracing::warn!(
            attempt,
            exit_code = output.exit_code,
            stderr = %output.stderr,
            "connectivity probe failed; retrying"
        );
        attempt += 1;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

async fn assert_tcp_connect(
    sandbox: &mut FirecrackerSandbox,
    destination: &str,
    should_succeed: bool,
) -> Result<()> {
    let cmd = format!("timeout 5 bash -lc ': </dev/tcp/{destination}'");
    let output = run_connectivity_probe(sandbox, "bash", &["-lc", &cmd], should_succeed).await?;
    assert_eq!(
        output.exit_code == 0,
        should_succeed,
        "tcp connectivity expectation failed for {destination}; exit={}, stdout={}, stderr={}",
        output.exit_code,
        output.stdout,
        output.stderr
    );
    Ok(())
}

async fn assert_curl(
    sandbox: &mut FirecrackerSandbox,
    url: &str,
    should_succeed: bool,
) -> Result<()> {
    let output = run_connectivity_probe(
        sandbox,
        "curl",
        &[
            "--noproxy",
            "*",
            "-4",
            "-sS",
            "--connect-timeout",
            "5",
            "--max-time",
            "10",
            "-o",
            "/dev/null",
            "--",
            url,
        ],
        should_succeed,
    )
    .await?;
    assert_eq!(
        output.exit_code == 0,
        should_succeed,
        "curl expectation failed for {url}; exit={}, stdout={}, stderr={}",
        output.exit_code,
        output.stdout,
        output.stderr
    );
    Ok(())
}

async fn test_network(sandbox: &mut FirecrackerSandbox, after_resume: bool) -> Result<()> {
    let checks = [("tcp_ip", "8.8.8.8/53"), ("tcp_dns", "www.baidu.com/443")];

    for (name, destination) in checks {
        if let Err(err) = assert_tcp_connect(sandbox, destination, true).await {
            tracing::error!(
                name,
                after_resume,
                error = %err,
                "connectivity check failed"
            );
            return Err(err).with_context(|| format!("connectivity check failed: {name}"));
        }
    }

    Ok(())
}

#[tokio::test]
async fn microvm_can_access_internet() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;
    test_network(&mut sandbox, false).await?;
    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    let mut resumed_sandbox = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
    test_network(&mut resumed_sandbox, true).await?;
    resumed_sandbox.stop().await?;

    Ok(())
}

#[tokio::test]
async fn microvm_network_policy_controls_egress() -> Result<()> {
    common::setup().await;
    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.common.network_policy = Some(SandboxNetworkPolicy::new(
        true,
        BaseSandboxNetworkPolicy::Deny,
        SandboxNetworkEgressPolicy::new(Some(vec!["8.8.8.8".to_string()]), None)?,
    ));

    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    assert_tcp_connect(&mut sandbox, "8.8.8.8/53", true).await?;
    assert_tcp_connect(&mut sandbox, "1.1.1.1/53", false).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Deny,
            SandboxNetworkEgressPolicy::new(Some(vec!["1.1.1.1".to_string()]), None)?,
        )))
        .await?;

    assert_tcp_connect(&mut sandbox, "8.8.8.8/53", false).await?;
    assert_tcp_connect(&mut sandbox, "1.1.1.1/53", true).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Allow,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["8.8.8.8".to_string()]),
                Some(vec!["8.8.8.0/24".to_string(), "1.1.1.1".to_string()]),
            )?,
        )))
        .await?;

    assert_tcp_connect(&mut sandbox, "8.8.8.8/53", true).await?;
    assert_tcp_connect(&mut sandbox, "1.1.1.1/53", false).await?;
    assert_tcp_connect(&mut sandbox, "1.0.0.1/53", true).await?;

    sandbox.update_network_policy(None).await?;
    assert_tcp_connect(&mut sandbox, "8.8.8.8/53", true).await?;
    assert_tcp_connect(&mut sandbox, "1.1.1.1/53", true).await?;
    assert_tcp_connect(&mut sandbox, "10.255.255.254/80", false).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Allow,
            SandboxNetworkEgressPolicy::new(Some(vec!["10.0.0.0/8".to_string()]), None)?,
        )))
        .await?;
    assert_tcp_connect(&mut sandbox, "10.255.255.254/80", false).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Default,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["www.baidu.com".to_string()]),
                Some(vec!["0.0.0.0/0".to_string()]),
            )?,
        )))
        .await?;

    assert_curl(&mut sandbox, "http://www.baidu.com/", true).await?;
    assert_curl(&mut sandbox, "https://www.baidu.com/", true).await?;
    assert_curl(&mut sandbox, "https://www.qq.com/", false).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Default,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["www.qq.com".to_string()]),
                Some(vec!["0.0.0.0/0".to_string()]),
            )?,
        )))
        .await?;

    assert_curl(&mut sandbox, "https://www.baidu.com/", false).await?;
    assert_curl(&mut sandbox, "https://www.qq.com/", true).await?;

    sandbox.update_network_policy(None).await?;
    assert_curl(&mut sandbox, "https://www.baidu.com/", true).await?;
    assert_curl(&mut sandbox, "https://www.qq.com/", true).await?;

    sandbox.stop().await?;
    Ok(())
}
