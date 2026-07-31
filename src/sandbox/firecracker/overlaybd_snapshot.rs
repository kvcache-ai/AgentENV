//! Overlaybd-specific snapshot helpers for Firecracker sandboxes.
//!
//! During pause, [`restack_snapshot_overlaybd_rootfs`] either stages a
//! read-only runtime config as-is or asks the ublk daemon to
//! `close_seal + restack` the live upper before writing the persisted
//! snapshot config.
//!
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use firecracker_client::models::DirtyMemoryRanges;
use nix::unistd::Pid;
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{ImageConfig, LayerConfig};
use overlaybd::index::{Segment, SegmentMapping};
use overlaybd::index_file::compact_to;
use overlaybd::transient_io_ring::shared_transient_io_ring;
use overlaybd::virtual_file::VirtualFile;
use tracing::{debug, warn};

use super::process_vm_reader::ProcessVmReader;
use super::sandbox::managed_snapshot_base;
use crate::cfg::ConfigManager;
use crate::sandbox::ublk::UblkDevice;
use crate::sandbox::ublk::{
    compact_layers, create_commit_args, OverlaybdCompactOutput, UblkDeviceManager,
};
use crate::sandbox::SandboxCaptureError;

/// Default maximum number of overlaybd snapshot layers before compaction triggers.
/// Well below the hard limit of 255 (`MAX_STACK_LAYERS`) in overlaybd.
const DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS: usize = 32;
const INHERITED_LAYERS_DIR: &str = "inherited-layers";
const MANAGED_BASE_LAYER_FILE: &str = "managed-base.commit";
const FIRECRACKER_DIRTY_PAGE_SIZE: u64 = 4096;
const OVERLAYBD_ALIGNMENT: u64 = 512;
const DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY: usize = 32;

enum LiveOverlaybdSnapshotState {
    ReadOnly,
    Restacked(PathBuf),
}

impl LiveOverlaybdSnapshotState {
    fn snapshot_layer_path(&self) -> Option<&Path> {
        match self {
            Self::ReadOnly => None,
            Self::Restacked(path) => Some(path.as_path()),
        }
    }

    fn finish_staging(self, staged_snapshot: Result<PathBuf>) -> Result<PathBuf> {
        match self {
            Self::ReadOnly => staged_snapshot,
            Self::Restacked(_) => staged_snapshot.map_err(into_terminal_snapshot_failure),
        }
    }
}

fn into_terminal_snapshot_failure(err: anyhow::Error) -> anyhow::Error {
    SandboxCaptureError::terminal(err).into()
}

fn local_layer_config(path: &Path) -> LayerConfig {
    local_layer_config_with_descriptor(path, None)
}

fn local_layer_config_with_descriptor(
    path: &Path,
    descriptor: Option<&overlaybd::LayerDescriptor>,
) -> LayerConfig {
    LayerConfig {
        file: path.display().to_string(),
        digest: descriptor
            .map(|descriptor| descriptor.digest.clone())
            .unwrap_or_default(),
        size: descriptor.map(|descriptor| descriptor.size).unwrap_or(0),
        ..Default::default()
    }
}

fn canonicalized_runtime_owned_roots() -> &'static [PathBuf] {
    static RUNTIME_OWNED_ROOTS: std::sync::OnceLock<Vec<PathBuf>> = std::sync::OnceLock::new();
    RUNTIME_OWNED_ROOTS
        .get_or_init(|| {
            let config = ConfigManager::global_config();
            [
                managed_snapshot_base(),
                config.orchestrator.persisted_sandbox_store_path.clone(),
            ]
            .into_iter()
            .map(|root| fs::canonicalize(&root).unwrap_or(root))
            .collect::<Vec<_>>()
        })
        .as_slice()
}

/// Split a list of overlaybd lowers into two parts: the prefix of stable lowers and the suffix of runtime-owned lowers.
///
/// Runtime-created local lowers are appended during resume/fork/pause.
/// Published or cache-owned layers must be materialized before that
/// suffix; otherwise adopting only the tail would either leave a dangling
/// runtime artifact reference or change lower precedence.
fn split_runtime_suffix(
    mut lowers: Vec<LayerConfig>,
    runtime_owned_roots: &[PathBuf],
) -> (Vec<LayerConfig>, Vec<LayerConfig>) {
    let Some(first_runtime_owned_index) = lowers.iter().position(|lower| {
        let lower_path =
            fs::canonicalize(&lower.file).unwrap_or_else(|_| PathBuf::from(&lower.file));
        runtime_owned_roots
            .iter()
            .any(|root| lower_path.starts_with(root))
    }) else {
        return (lowers, Vec::new());
    };

    let runtime_owned_suffix = lowers.split_off(first_runtime_owned_index);

    debug_assert!(lowers.iter().all(|lower| {
        let lower_path =
            fs::canonicalize(&lower.file).unwrap_or_else(|_| PathBuf::from(&lower.file));
        runtime_owned_roots
            .iter()
            .all(|root| !lower_path.starts_with(root))
    }));

    (lowers, runtime_owned_suffix)
}

fn link_or_copy_file_blocking(source: &Path, destination: &Path) -> std::io::Result<()> {
    if source == destination {
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::remove_file(destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    if let Err(error) = fs::hard_link(source, destination) {
        warn!(
            source = %source.display(),
            destination = %destination.display(),
            error = %error,
            "hard-link runtime overlaybd layer failed; falling back to sparse copy"
        );
    } else {
        return Ok(());
    }

    let mut cmd = std::process::Command::new("cp");
    if cfg!(target_os = "linux") {
        cmd.arg("--reflink=auto").arg("--sparse=always");
    }
    let status = cmd.arg(source).arg(destination).status();
    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            let _ = fs::remove_file(destination);
            Err(std::io::Error::other(format!(
                "sparse copy command failed with status {status}"
            )))
        }
        Err(error) => {
            let _ = fs::remove_file(destination);
            Err(error)
        }
    }
}

async fn link_or_copy_runtime_layer(source: &Path, destination: &Path) -> Result<()> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    let description = format!(
        "link or copy runtime overlaybd layer {} -> {}",
        source.display(),
        destination.display()
    );
    tokio::task::spawn_blocking(move || link_or_copy_file_blocking(&source, &destination))
        .await
        .context("link runtime overlaybd layer task failed")?
        .with_context(|| description)
}

async fn prepare_specific_snapshot_layer_path(snapshot_layer_path: &Path) -> Result<()> {
    match tokio::fs::remove_file(&snapshot_layer_path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "remove stale overlaybd snapshot layer {}",
                    snapshot_layer_path.display()
                )
            })
        }
    }
    Ok(())
}

fn load_existing_image_config(
    image_config_path: Option<&Path>,
    description: &str,
) -> Result<ImageConfig> {
    let Some(image_config_path) = image_config_path else {
        return Ok(ImageConfig::default());
    };

    let image_config =
        overlaybd::config::load_image_config(image_config_path).with_context(|| {
            format!(
                "load {description} image config {}",
                image_config_path.display()
            )
        })?;
    Ok(image_config)
}

fn restack_target_upper_data_path(live_runtime_image_config_path: &Path) -> Result<PathBuf> {
    let image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load live overlaybd runtime config {}",
                live_runtime_image_config_path.display()
            )
        })?;
    if image_config.upper.data.is_empty() {
        anyhow::bail!(
            "restack snapshot requires writable upper.data in live runtime config {}",
            live_runtime_image_config_path.display()
        );
    }
    Ok(PathBuf::from(image_config.upper.data))
}

fn on_same_filesystem(src_path: &Path, dst_dir: &Path) -> Result<bool> {
    let src_meta = fs::metadata(src_path)
        .with_context(|| format!("stat restack source path {}", src_path.display()))?;
    let dst_meta = fs::metadata(dst_dir)
        .with_context(|| format!("stat restack destination dir {}", dst_dir.display()))?;
    Ok(src_meta.dev() == dst_meta.dev())
}

fn write_bytes_atomically(path: &Path, bytes: &[u8], description: &str) -> Result<()> {
    let parent = path.parent().with_context(|| {
        format!(
            "{description} path has no parent directory: {}",
            path.display()
        )
    })?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temp {description} in {}", parent.display()))?;
    temp.write_all(bytes)
        .with_context(|| format!("write temp {description} {}", temp.path().display()))?;
    temp.as_file()
        .sync_all()
        .with_context(|| format!("sync temp {description} {}", temp.path().display()))?;
    let temp_path = temp.path().to_path_buf();
    temp.persist(path).map_err(|error| {
        anyhow::Error::new(error.error).context(format!(
            "persist {description} {} -> {}",
            temp_path.display(),
            path.display()
        ))
    })?;
    Ok(())
}

async fn copy_file_atomically(src: &Path, dst: &Path, description: &str) -> Result<()> {
    let parent = dst.parent().with_context(|| {
        format!(
            "{description} destination has no parent directory: {}",
            dst.display()
        )
    })?;
    let temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temp {description} in {}", parent.display()))?;
    let temp_path = temp.path().to_path_buf();
    tokio::fs::copy(src, &temp_path).await.with_context(|| {
        format!(
            "copy {description} {} -> {}",
            src.display(),
            temp_path.display()
        )
    })?;
    std::fs::File::open(&temp_path)
        .with_context(|| format!("open temp {description} {}", temp_path.display()))?
        .sync_all()
        .with_context(|| format!("sync temp {description} {}", temp_path.display()))?;
    temp.persist(dst).map_err(|error| {
        anyhow::Error::new(error.error).context(format!(
            "persist {description} {} -> {}",
            temp_path.display(),
            dst.display()
        ))
    })?;
    Ok(())
}

async fn rewrite_live_runtime_config_for_restack(
    live_runtime_image_config_path: &Path,
    snapshot_layer_path: &Path,
    descriptor: Option<&overlaybd::LayerDescriptor>,
) -> Result<()> {
    let mut image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load live overlaybd runtime config {}",
                live_runtime_image_config_path.display()
            )
        })?;
    image_config.lowers.push(local_layer_config_with_descriptor(
        snapshot_layer_path,
        descriptor,
    ));
    let bytes = serde_json::to_vec_pretty(&image_config)
        .context("serialize rewritten live overlaybd runtime config")?;
    write_bytes_atomically(
        live_runtime_image_config_path,
        &bytes,
        "live overlaybd runtime config",
    )?;
    Ok(())
}

pub(super) async fn stage_overlaybd_snapshot_from_live_runtime(
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
    snapshot_layer_path: Option<&Path>,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create overlaybd snapshot dir {}", output_dir.display()))?;

    let mut image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load overlaybd rootfs image config {}",
                live_runtime_image_config_path.display()
            )
        })?;

    let appended_layer = if let Some(snapshot_layer_path) = snapshot_layer_path {
        let expected_snapshot_path = fs::canonicalize(snapshot_layer_path)
            .unwrap_or_else(|_| snapshot_layer_path.to_path_buf());
        let Some(latest_lower) = image_config.lowers.last() else {
            anyhow::bail!(
                "restack snapshot expected latest lower to be the newly claimed sealed upper layer"
            );
        };
        let actual_snapshot_path = fs::canonicalize(&latest_lower.file)
            .unwrap_or_else(|_| PathBuf::from(&latest_lower.file));
        anyhow::ensure!(
            actual_snapshot_path == expected_snapshot_path,
            "restack snapshot expected latest lower {} but found {}",
            expected_snapshot_path.display(),
            actual_snapshot_path.display()
        );
        Some(image_config.lowers.pop().expect("checked latest lower"))
    } else {
        None
    };

    let rewritten_lowers = rewrite_lowers_with_owned_runtime_suffix(
        image_config.lowers,
        output_dir,
        appended_layer,
        MANAGED_BASE_LAYER_FILE,
        // Rootfs layers must stay raw: only memory snapshots may be compressed.
        OverlaybdCompactOutput::Raw,
    )
    .await?;
    image_config.lowers = rewritten_lowers;
    image_config.upper = Default::default();
    image_config.result_file = "./result.txt".to_string();

    let output_path = output_dir.join("image.json");
    let bytes = serde_json::to_vec_pretty(&image_config)
        .context("serialize overlaybd rootfs image config with inherited runtime layers")?;
    write_bytes_atomically(&output_path, &bytes, "overlaybd rootfs image config")?;
    Ok(output_path)
}

async fn rewrite_lowers_with_owned_runtime_suffix(
    existing_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    appended_layer: Option<LayerConfig>,
    compaction_output_name: &'static str,
    compaction_output: OverlaybdCompactOutput,
) -> Result<Vec<LayerConfig>> {
    rewrite_lowers_with_runtime_roots(
        existing_lowers,
        output_dir,
        appended_layer,
        compaction_output_name,
        canonicalized_runtime_owned_roots(),
        compaction_output,
    )
    .await
}

async fn rewrite_lowers_with_runtime_roots(
    existing_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    appended_layer: Option<LayerConfig>,
    compaction_output_name: &'static str,
    runtime_owned_roots: &[PathBuf],
    compaction_output: OverlaybdCompactOutput,
) -> Result<Vec<LayerConfig>> {
    let (mut lowers, mut runtime_owned_lowers) =
        split_runtime_suffix(existing_lowers, runtime_owned_roots);

    // If the total number of lowers exceeds the default maximum, try to compact the
    // runtime-owned suffix and appended layers into a single layer.
    let compactable_count = runtime_owned_lowers.len() + usize::from(appended_layer.is_some());
    if compactable_count > 1
        && lowers.len() + compactable_count > DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS
    {
        let mut runtime_suffix = runtime_owned_lowers;
        if let Some(layer) = appended_layer {
            runtime_suffix.push(layer);
        }
        if runtime_suffix.iter().any(|lower| lower.file.is_empty()) {
            anyhow::bail!("cannot compact runtime-owned overlaybd suffix with remote lowers");
        }
        if let Some(compacted_path) = compact_layers(
            &runtime_suffix,
            &output_dir.join(compaction_output_name),
            compaction_output,
        )
        .await?
        {
            lowers.push(local_layer_config(&compacted_path));
        }
        return Ok(lowers);
    }

    // Otherwise, adopt the runtime-owned suffix into the snapshot artifact dir.
    let inherited_layers_dir = output_dir.join(INHERITED_LAYERS_DIR);
    for (index, lower) in runtime_owned_lowers.iter_mut().enumerate() {
        let source = Path::new(&lower.file);
        let destination = inherited_layers_dir.join(format!("{index:04}")).join(
            source
                .file_name()
                .unwrap_or_else(|| OsStr::new("runtime-layer.commit")),
        );
        link_or_copy_runtime_layer(source, &destination).await?;
        lower.file = destination.display().to_string();
    }
    lowers.extend(runtime_owned_lowers);

    if let Some(layer) = appended_layer {
        lowers.push(layer);
    }

    Ok(lowers)
}

async fn capture_live_overlaybd_snapshot(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
) -> Result<LiveOverlaybdSnapshotState> {
    if read_only {
        debug!(
            output_dir = %output_dir.display(),
            "staging overlaybd snapshot from read-only runtime config"
        );
        return Ok(LiveOverlaybdSnapshotState::ReadOnly);
    }

    let live_upper_data_path = restack_target_upper_data_path(live_runtime_image_config_path)
        .context("resolve restack source upper path")?;
    let snapshot_layer_path = output_dir.join("snapshot.commit");
    prepare_specific_snapshot_layer_path(&snapshot_layer_path).await?;
    let live_snapshot_layer_path = if on_same_filesystem(&live_upper_data_path, output_dir)
        .context("validate restack snapshot filesystem precondition")?
    {
        snapshot_layer_path.clone()
    } else {
        let live_snapshot_layer_path = live_upper_data_path
            .parent()
            .context("restack source upper path has no parent directory")?
            .join("snapshot.commit");
        prepare_specific_snapshot_layer_path(&live_snapshot_layer_path)
            .await
            .with_context(|| {
                format!(
                    "prepare same-filesystem restack snapshot layer {}",
                    live_snapshot_layer_path.display()
                )
            })?;
        live_snapshot_layer_path
    };

    let descriptor = UblkDeviceManager::global()
        .restack_snapshot_device(ublk_device, &live_snapshot_layer_path)
        .await
        .context("request overlaybd restack snapshot from ublk device")?;

    if live_snapshot_layer_path != snapshot_layer_path {
        // If this cross-filesystem copy fails, leave the live runtime config
        // untouched and surface a terminal pause failure. The daemon has
        // already sealed the old upper and reopened a fresh one, so callers
        // must not continue treating the live runtime as safely resumable.
        copy_file_atomically(
            &live_snapshot_layer_path,
            &snapshot_layer_path,
            "restack snapshot layer",
        )
        .await
        .context("copy restack snapshot layer into managed snapshot dir")
        .map_err(into_terminal_snapshot_failure)?;
    }

    if let Some(descriptor) = descriptor.as_ref() {
        let copied_size = tokio::fs::metadata(&snapshot_layer_path)
            .await
            .with_context(|| {
                format!(
                    "read restack snapshot layer metadata {}",
                    snapshot_layer_path.display()
                )
            })?
            .len();
        if copied_size != descriptor.size {
            let _ = fs::remove_file(&snapshot_layer_path);
            anyhow::bail!(
                "restack snapshot descriptor size mismatch for {}: descriptor says {}, file has {}",
                snapshot_layer_path.display(),
                descriptor.size,
                copied_size
            );
        }
    }

    rewrite_live_runtime_config_for_restack(
        live_runtime_image_config_path,
        &snapshot_layer_path,
        descriptor.as_ref(),
    )
    .await
    .context("rewrite live runtime config after restack snapshot")
    .map_err(into_terminal_snapshot_failure)?;

    Ok(LiveOverlaybdSnapshotState::Restacked(snapshot_layer_path))
}

pub(super) async fn build_mem_snapshot_image_config(
    resume_mem_image_config_path: Option<&Path>,
    new_layer_path: &Path,
    output_dir: &Path,
    memory_output: OverlaybdCompactOutput,
) -> Result<ImageConfig> {
    let inherited_image_config =
        load_existing_image_config(resume_mem_image_config_path, "memory snapshot")?;
    let new_layer = local_layer_config(new_layer_path);
    let lowers = rewrite_lowers_with_owned_runtime_suffix(
        inherited_image_config.lowers,
        output_dir,
        Some(new_layer),
        "mem_compacted.commit",
        memory_output,
    )
    .await?;

    Ok(ImageConfig {
        repo_blob_url: inherited_image_config.repo_blob_url,
        lowers,
        ..Default::default()
    })
}

/// Stage a persisted snapshot config from the current live overlaybd runtime.
///
/// Writable runtimes are first restacked in-place so the sealed old upper
/// becomes the newest lower. Read-only runtimes skip the restack phase and
/// only stage the persisted snapshot image config.
pub(super) async fn restack_snapshot_overlaybd_device(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create overlaybd snapshot dir {}", output_dir.display()))?;

    let live_snapshot = capture_live_overlaybd_snapshot(
        ublk_device,
        read_only,
        live_runtime_image_config_path,
        output_dir,
    )
    .await?;

    let staged_snapshot = stage_overlaybd_snapshot_from_live_runtime(
        live_runtime_image_config_path,
        output_dir,
        live_snapshot.snapshot_layer_path(),
    )
    .await;

    live_snapshot.finish_staging(staged_snapshot)
}

pub(super) async fn restack_snapshot_overlaybd_rootfs(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    snapshot_root: &Path,
) -> Result<PathBuf> {
    restack_snapshot_overlaybd_device(
        ublk_device,
        read_only,
        live_runtime_image_config_path,
        &snapshot_root.join("rootfs"),
    )
    .await
}

async fn publish_memory_overlaybd_layer(
    src_layers: &[Arc<dyn VirtualFile>],
    mappings: &[SegmentMapping],
    virtual_size: u64,
    output_path: &Path,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
) -> Result<()> {
    let lower_tmp = output_path.with_extension("commit.tmp");
    let build_result = async {
        let io_ring = shared_transient_io_ring();
        let output_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp, io_ring)
                .await
                .with_context(|| format!("create temp lower failed: {}", lower_tmp.display()))?,
        );
        let commit_args = create_commit_args(output_file, mode, concurrency).await?;
        compact_to(src_layers, mappings, virtual_size, commit_args)
            .await
            .context("compact memory layer")?;
        tokio::fs::rename(&lower_tmp, output_path)
            .await
            .with_context(|| {
                format!(
                    "move sealed memory lower into place failed: {}",
                    output_path.display()
                )
            })?;
        Ok(())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    build_result
}

/// Convert a sparse memory snapshot file into a sealed overlaybd layer.
///
/// The sparse file is produced by Firecracker's diff snapshot
/// (`SnapshotType::Diff`), where only dirty pages contain data and untouched
/// pages are holes. The parameterized raw packaging path scans the file with
/// `seek_data`/`seek_hole` and compacts only the data extents.
///
/// The input sparse file is removed only after the final layer rename succeeds.
///
/// Returns the path to the created overlaybd layer file.
pub(crate) async fn convert_sparse_mem_to_overlaybd(
    sparse_mem_path: &Path,
    output_dir: &Path,
    mode: OverlaybdCompactOutput,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create mem overlaybd dir: {}", output_dir.display()))?;

    let data_path = output_dir.join("overlaybd.commit");
    let lower_tmp = data_path.with_extension("commit.tmp");
    let build_result: Result<()> = async {
        let io_ring = shared_transient_io_ring();
        let output_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp, io_ring)
                .await
                .with_context(|| format!("create temp lower failed: {}", lower_tmp.display()))?,
        );
        let commit_args = create_commit_args(
            output_file,
            mode,
            DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY,
        )
        .await?;
        overlaybd::tools::package_raw_as_overlaybd_with_args(sparse_mem_path, commit_args)
            .await
            .context("convert sparse mem.bin to overlaybd layer")?;
        tokio::fs::rename(&lower_tmp, &data_path)
            .await
            .with_context(|| {
                format!(
                    "move sealed sparse memory lower into place failed: {}",
                    data_path.display()
                )
            })?;
        if let Err(error) = tokio::fs::remove_file(sparse_mem_path).await {
            warn!(
                mem_path = %sparse_mem_path.display(),
                error = %error,
                "failed to remove sparse mem snapshot after overlaybd commit"
            );
        }
        Ok(())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    build_result?;

    Ok(data_path)
}

fn checked_i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} must be non-negative, got {value}"))
}

fn dirty_ranges_to_segment_mappings(
    dirty_ranges: &DirtyMemoryRanges,
) -> Result<(Vec<SegmentMapping>, u64)> {
    ensure!(
        dirty_ranges.page_size == FIRECRACKER_DIRTY_PAGE_SIZE as i32,
        "dirty memory page_size must be {FIRECRACKER_DIRTY_PAGE_SIZE}, got {}",
        dirty_ranges.page_size
    );
    let memory_size = checked_i64_to_u64(dirty_ranges.memory_size, "memory_size")?;
    ensure!(
        memory_size % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
        "dirty memory_size {memory_size} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
    );

    let mut mappings = Vec::new();
    for range in &dirty_ranges.ranges {
        let mut base_host_virt_addr =
            checked_i64_to_u64(range.base_host_virt_addr, "base_host_virt_addr")?;
        let mut image_offset = checked_i64_to_u64(range.image_offset, "image_offset")?;
        let length = checked_i64_to_u64(range.length, "length")?;
        ensure!(length > 0, "dirty memory range length must be positive");
        ensure!(
            base_host_virt_addr % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range base_host_virt_addr {base_host_virt_addr:#x} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            image_offset % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range image_offset {image_offset} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            length % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range length {length} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            image_offset
                .checked_add(length)
                .is_some_and(|end| end <= memory_size),
            "dirty range [{image_offset}, {}) exceeds memory_size {memory_size}",
            image_offset.saturating_add(length)
        );

        let mut remaining_sectors = length / OVERLAYBD_ALIGNMENT;
        while remaining_sectors > 0 {
            let sector_count = remaining_sectors.min(Segment::MAX_LENGTH as u64);
            mappings.push(SegmentMapping::new(
                image_offset / OVERLAYBD_ALIGNMENT,
                sector_count as u32,
                base_host_virt_addr / OVERLAYBD_ALIGNMENT,
                false,
                0,
            ));
            let bytes = sector_count * OVERLAYBD_ALIGNMENT;
            base_host_virt_addr += bytes;
            image_offset += bytes;
            remaining_sectors -= sector_count;
        }
    }

    mappings.sort_unstable();
    // `offset` and `length` describe destination memory-image sectors, while
    // `moffset` carries the source Firecracker HVA in sector units. This check
    // intentionally rejects overlapping destination ranges, not overlapping
    // source virtual addresses.
    for pair in mappings.windows(2) {
        let previous = &pair[0];
        let next = &pair[1];
        let previous_end = previous
            .offset()
            .checked_add(u64::from(previous.length()))
            .context("dirty memory segment end overflow")?;
        ensure!(
            previous_end <= next.offset(),
            "overlapping dirty memory destination ranges detected at image sectors {} and {}",
            previous.offset(),
            next.offset()
        );
    }
    Ok((mappings, memory_size))
}

/// Convert dirty Firecracker memory ranges directly into a sealed overlaybd layer.
pub(crate) async fn convert_dirty_memory_to_overlaybd(
    firecracker_pid: Pid,
    dirty_ranges: &DirtyMemoryRanges,
    output_dir: &Path,
    mode: OverlaybdCompactOutput,
) -> Result<(PathBuf, u64)> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create mem overlaybd dir: {}", output_dir.display()))?;

    let data_path = output_dir.join("overlaybd.commit");
    let (mappings, memory_size) = dirty_ranges_to_segment_mappings(dirty_ranges)?;
    let source_file: Arc<dyn VirtualFile> = Arc::new(ProcessVmReader::new(firecracker_pid));
    let src_layers = vec![source_file];
    publish_memory_overlaybd_layer(
        &src_layers,
        &mappings,
        memory_size,
        &data_path,
        mode,
        DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY,
    )
    .await
    .context("compact dirty memory ranges as overlaybd layer")?;

    Ok((data_path, memory_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    use async_trait::async_trait;
    use bytes::Bytes;
    use firecracker_client::models::DirtyMemoryRange;
    use overlaybd::index_file::{CommitArgs, LSMTReadOnlyFile};
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn dirty_ranges_to_segment_mappings_splits_large_ranges() {
        let first_chunk = Segment::MAX_LENGTH as i64 * OVERLAYBD_ALIGNMENT as i64;
        let dirty_ranges = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: first_chunk + OVERLAYBD_ALIGNMENT as i64,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: 0x1000_0000,
                image_offset: 0,
                length: first_chunk + OVERLAYBD_ALIGNMENT as i64,
            }],
        };

        let (mappings, memory_size) =
            dirty_ranges_to_segment_mappings(&dirty_ranges).expect("convert mappings");

        assert_eq!(memory_size, dirty_ranges.memory_size as u64);
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].offset(), 0);
        assert_eq!(mappings[0].length(), Segment::MAX_LENGTH);
        assert_eq!(mappings[0].moffset, 0x1000_0000 / OVERLAYBD_ALIGNMENT);
        assert_eq!(mappings[1].offset(), Segment::MAX_LENGTH as u64);
        assert_eq!(mappings[1].length(), 1);
        assert_eq!(
            mappings[1].moffset,
            0x1000_0000 / OVERLAYBD_ALIGNMENT + Segment::MAX_LENGTH as u64
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_non_4k_page_size() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: 2048,
            memory_size: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
            ranges: Vec::new(),
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("dirty memory page_size must be 4096"),
            "{err:#}"
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_non_page_aligned_range() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: FIRECRACKER_DIRTY_PAGE_SIZE as i32,
            memory_size: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: 0x1000_0000,
                image_offset: 0,
                length: OVERLAYBD_ALIGNMENT as i64,
            }],
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("dirty range length 512 is not 4096-byte aligned"),
            "{err:#}"
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_overlaps() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: FIRECRACKER_DIRTY_PAGE_SIZE as i32,
            memory_size: (FIRECRACKER_DIRTY_PAGE_SIZE * 3) as i64,
            ranges: vec![
                DirtyMemoryRange {
                    base_host_virt_addr: 0x1000_0000,
                    image_offset: 0,
                    length: (FIRECRACKER_DIRTY_PAGE_SIZE * 2) as i64,
                },
                DirtyMemoryRange {
                    base_host_virt_addr: 0x2000_0000,
                    image_offset: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
                    length: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
                },
            ],
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("overlapping dirty memory destination ranges detected"),
            "{err:#}"
        );
    }

    #[test]
    fn split_runtime_suffix_preserves_stable_lowers() {
        let config = ConfigManager::global_config();
        let snapshot_store_root = config
            .backend
            .posix_fs
            .as_ref()
            .map(|posix_fs| posix_fs.snapshot_store.clone())
            .unwrap_or_else(|| std::env::temp_dir().join("test_snapshot_store"));
        let persisted_sandbox_store_root = config.orchestrator.persisted_sandbox_store_path.clone();
        let snapshot_store_lower = snapshot_store_root
            .join("managed-layers")
            .join("sha256_base.overlaybd.commit");
        let persisted_store_lower = persisted_sandbox_store_root
            .join("artifacts")
            .join("sandbox-parent")
            .join("1700000000000")
            .join("rootfs")
            .join("snapshot.commit");
        let managed_base_lower = managed_snapshot_base().join("sandbox/1700000000001/mem.commit");
        let lowers = vec![
            LayerConfig {
                file: String::new(),
                digest: "sha256:remote".to_string(),
                size: 42,
                ..Default::default()
            },
            local_layer_config(&snapshot_store_lower),
            local_layer_config(&persisted_store_lower),
            local_layer_config(&managed_base_lower),
        ];

        let (preserved, managed) =
            split_runtime_suffix(lowers, canonicalized_runtime_owned_roots());

        assert_eq!(preserved.len(), 2);
        assert_eq!(preserved[0].file, "");
        assert_eq!(preserved[0].digest, "sha256:remote");
        assert_eq!(PathBuf::from(&preserved[1].file), snapshot_store_lower);
        assert_eq!(managed.len(), 2);
        assert_eq!(PathBuf::from(&managed[0].file), persisted_store_lower);
        assert_eq!(PathBuf::from(&managed[1].file), managed_base_lower);
    }

    #[tokio::test]
    async fn snapshot_adopts_persisted_runtime_suffix_by_link() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifacts_root = temp.path().join("persisted-sandboxes").join("artifacts");
        let output_dir = artifacts_root.join("sandbox-c").join("gen1").join("rootfs");
        let parent_root = artifacts_root.join("sandbox-b").join("gen1").join("rootfs");
        let cache_root = temp.path().join("image-cache").join("commits");
        std::fs::create_dir_all(&parent_root).expect("create parent rootfs");
        std::fs::create_dir_all(&cache_root).expect("create cache root");

        let parent_base = parent_root.join("managed-base.commit");
        let parent_snapshot = parent_root.join("snapshot.commit");
        let cache_layer = cache_root.join("sha256-base").join("overlaybd.commit");
        std::fs::create_dir_all(cache_layer.parent().unwrap()).expect("create cache layer dir");
        std::fs::write(&parent_base, b"parent base").expect("write parent base");
        std::fs::write(&parent_snapshot, b"parent snapshot").expect("write parent snapshot");
        std::fs::write(&cache_layer, b"cache base").expect("write cache layer");

        let lowers = vec![
            LayerConfig {
                file: cache_layer.display().to_string(),
                digest: "sha256:base".to_string(),
                size: 10,
                ..Default::default()
            },
            local_layer_config(&parent_base),
            local_layer_config(&parent_snapshot),
        ];
        let runtime_owned_roots = [artifacts_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
        )
        .await
        .expect("rewrite inherited runtime layers");

        assert_eq!(rewritten.len(), 3);
        assert_eq!(PathBuf::from(&rewritten[0].file), cache_layer);
        assert_eq!(rewritten[0].digest, "sha256:base");
        assert_eq!(rewritten[0].size, 10);

        for (index, (rewritten_lower, source)) in rewritten[1..]
            .iter()
            .zip([parent_base, parent_snapshot])
            .enumerate()
        {
            let adopted = PathBuf::from(&rewritten_lower.file);
            let expected_parent = output_dir
                .join(INHERITED_LAYERS_DIR)
                .join(format!("{index:04}"));
            assert!(adopted.starts_with(expected_parent));
            assert!(!adopted.starts_with(&parent_root));
            assert_eq!(adopted.file_name(), source.file_name());
            assert!(rewritten_lower.digest.is_empty());
            assert_eq!(rewritten_lower.size, 0);

            let source_metadata = std::fs::metadata(&source).expect("source metadata");
            let adopted_metadata = std::fs::metadata(&adopted).expect("adopted metadata");
            assert_eq!(source_metadata.dev(), adopted_metadata.dev());
            assert_eq!(source_metadata.ino(), adopted_metadata.ino());
        }
    }

    #[tokio::test]
    async fn stage_live_runtime_preserves_restacked_layer_descriptor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let live_dir = temp.path().join("live");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&live_dir).expect("create live dir");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        let snapshot_lower = output_dir.join("snapshot.commit");
        std::fs::write(&snapshot_lower, b"snapshot").expect("write snapshot lower");
        let live_runtime_image_config_path = live_dir.join("image.json");
        std::fs::write(
            &live_runtime_image_config_path,
            serde_json::to_vec_pretty(&json!({
                "lowers": [
                    {
                        "file": snapshot_lower,
                        "digest": "sha256:descriptor",
                        "size": 8
                    }
                ],
                "upper": {},
                "resultFile": live_dir.join("result.txt"),
                "download": {}
            }))
            .expect("serialize live image config"),
        )
        .expect("write live image config");

        let output_path = stage_overlaybd_snapshot_from_live_runtime(
            &live_runtime_image_config_path,
            &output_dir,
            Some(&snapshot_lower),
        )
        .await
        .expect("stage snapshot");

        let staged =
            overlaybd::config::load_image_config(&output_path).expect("load staged image config");
        let latest = staged.lowers.last().expect("latest lower");
        assert_eq!(PathBuf::from(&latest.file), snapshot_lower);
        assert_eq!(latest.digest, "sha256:descriptor");
        assert_eq!(latest.size, 8);
    }

    #[tokio::test]
    async fn build_mem_snapshot_image_config_skips_compaction_for_remote_lowers() {
        let temp = tempfile::tempdir().expect("tempdir");
        let parent_config_path = temp.path().join("parent-mem-image.json");
        let repo_blob_url = "s3://agentenv-oss-validation/validation/managed-layers";
        let remote_lowers: Vec<_> = (0..DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS)
            .map(|index| {
                json!({
                    "digest": format!("sha256:parent-{index}"),
                    "size": 4096
                })
            })
            .collect();
        std::fs::write(
            &parent_config_path,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": repo_blob_url,
                "lowers": remote_lowers,
                "upper": {},
                "resultFile": "",
                "download": {}
            }))
            .expect("serialize parent image config"),
        )
        .expect("write parent image config");

        let new_layer = temp.path().join("mem_overlaybd").join("overlaybd.commit");
        std::fs::create_dir_all(new_layer.parent().unwrap()).expect("create mem layer dir");
        std::fs::write(&new_layer, b"memory delta").expect("write mem layer");
        let image_config = build_mem_snapshot_image_config(
            Some(&parent_config_path),
            &new_layer,
            temp.path(),
            OverlaybdCompactOutput::Raw,
        )
        .await
        .expect("build memory image config");

        assert_eq!(image_config.repo_blob_url, repo_blob_url);
        assert_eq!(
            image_config.lowers.len(),
            DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS + 1
        );
        assert_eq!(image_config.lowers[0].digest, "sha256:parent-0");
        assert!(image_config.lowers[..DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS]
            .iter()
            .all(|lower| lower.file.is_empty()));
        let latest = image_config.lowers.last().expect("latest mem lower");
        assert_eq!(PathBuf::from(&latest.file), new_layer);
        assert!(latest.digest.is_empty());
        assert_eq!(latest.size, 0);
        assert!(image_config.download_override.is_none());
    }

    struct FailingSource;

    #[async_trait]
    impl VirtualFile for FailingSource {
        async fn read_at(&self, _offset: u64, _len: usize) -> Result<Bytes> {
            anyhow::bail!("injected memory source read failure")
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            anyhow::bail!("injected memory source write failure")
        }

        async fn size(&self) -> Result<u64> {
            Ok(OVERLAYBD_ALIGNMENT)
        }
    }

    fn one_page_mapping() -> Vec<SegmentMapping> {
        vec![SegmentMapping::new(0, 1, 0, false, 0)]
    }

    #[tokio::test]
    async fn publish_memory_layer_failure_removes_temp_and_preserves_final() {
        let temp = tempfile::tempdir().expect("tempdir");
        let output = temp.path().join("memory.commit");
        let lower_tmp = output.with_extension("commit.tmp");
        let existing = b"existing final";
        tokio::fs::write(&output, existing)
            .await
            .expect("write existing final");
        let source: Arc<dyn VirtualFile> = Arc::new(FailingSource);
        publish_memory_overlaybd_layer(
            &[source],
            &one_page_mapping(),
            OVERLAYBD_ALIGNMENT,
            &output,
            OverlaybdCompactOutput::Raw,
            1,
        )
        .await
        .expect_err("copy failure should be returned");

        assert!(!lower_tmp.exists());
        assert_eq!(
            tokio::fs::read(&output).await.expect("read final"),
            existing
        );
    }

    #[tokio::test]
    async fn convert_sparse_mem_supports_all_outputs_and_failure_cleanup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let expected: Vec<u8> = (0..8192).map(|offset| (offset / 4096) as u8).collect();

        for (name, mode, algorithm) in [
            ("raw", OverlaybdCompactOutput::Raw, None),
            (
                "lz4",
                OverlaybdCompactOutput::ZFile {
                    algorithm: crate::cfg::MemorySnapshotCompressionAlgorithm::Lz4,
                    workers: 1,
                },
                Some(overlaybd::zfile::CompressOptions::LZ4),
            ),
            (
                "zstd",
                OverlaybdCompactOutput::ZFile {
                    algorithm: crate::cfg::MemorySnapshotCompressionAlgorithm::Zstd,
                    workers: 1,
                },
                Some(overlaybd::zfile::CompressOptions::ZSTD),
            ),
        ] {
            let source = temp.path().join(format!("{name}.mem.bin"));
            tokio::fs::write(&source, &expected)
                .await
                .expect("write memory source");
            let output_dir = temp.path().join(name);
            let output = convert_sparse_mem_to_overlaybd(&source, &output_dir, mode)
                .await
                .expect("publish memory layer");

            let file: Arc<dyn VirtualFile> = Arc::new(
                LocalFile::open_ro(&output, shared_transient_io_ring())
                    .await
                    .expect("open output"),
            );
            let file = if let Some(expected_algorithm) = algorithm {
                let zfile = overlaybd::zfile::zfile_open_ro(file, false)
                    .await
                    .expect("open zfile reader");
                assert_eq!(zfile.options().algo, expected_algorithm);
                assert_eq!(zfile.options().block_size, 4096);
                assert_eq!(zfile.options().verify, 0);
                Arc::new(zfile) as Arc<dyn VirtualFile>
            } else {
                file
            };
            let layer = LSMTReadOnlyFile::open(file)
                .await
                .expect("open logical overlaybd layer");
            assert_eq!(layer.read_at(0, expected.len()).await.unwrap(), expected);
            assert!(!source.exists());
            assert!(!output.with_extension("commit.tmp").exists());
        }

        let failing_source = temp.path().join("failing.mem.bin");
        tokio::fs::create_dir(&failing_source)
            .await
            .expect("create failing source directory");
        let output_dir = temp.path().join("failure");
        convert_sparse_mem_to_overlaybd(&failing_source, &output_dir, OverlaybdCompactOutput::Raw)
            .await
            .expect_err("directory source should not package");
        let output = output_dir.join("overlaybd.commit");
        assert!(failing_source.exists());
        assert!(!output.exists());
        assert!(!output.with_extension("commit.tmp").exists());
    }

    async fn create_tiny_raw_layer(dir: &Path, name: &str, writes: &[(u64, u8)]) -> PathBuf {
        let io_ring = shared_transient_io_ring();
        let data = Arc::new(
            LocalFile::new(dir.join(format!("{name}.data")), io_ring.clone())
                .await
                .expect("create layer data file"),
        );
        let index = Arc::new(
            LocalFile::new(dir.join(format!("{name}.index")), io_ring.clone())
                .await
                .expect("create layer index file"),
        );
        let layer = overlaybd::index_file::LSMTFile::create(data, Some(index), 2 * 4096, false)
            .await
            .expect("create layer");
        for (offset, byte) in writes {
            layer
                .write_at(*offset, &[*byte; 4096])
                .await
                .expect("write layer page");
        }
        let commit_path = dir.join(format!("{name}.commit"));
        let output: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&commit_path, io_ring)
                .await
                .expect("create layer commit output"),
        );
        layer
            .commit_with_args(CommitArgs::new(output))
            .await
            .expect("commit raw layer");
        commit_path
    }

    async fn read_sealed_layer(path: &Path, len: usize) -> (i32, Vec<u8>) {
        let local: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(path, shared_transient_io_ring())
                .await
                .expect("open sealed layer"),
        );
        let zfile_flag = overlaybd::zfile::is_zfile(local.clone())
            .await
            .expect("probe zfile header");
        let display = path.display().to_string();
        let tar_adapted = overlaybd::backend::tar::new_tar_file_adaptor(local)
            .await
            .expect("tar adaptor");
        let switched =
            overlaybd::backend::switch::new_switch_file(tar_adapted, true, Some(&display))
                .await
                .expect("switch file");
        let layer = LSMTReadOnlyFile::open(switched)
            .await
            .expect("open sealed layer as LSMT");
        let data = layer.read_at(0, len).await.expect("read sealed layer");
        (zfile_flag, data.to_vec())
    }

    fn expected_compacted_pages() -> Vec<u8> {
        let mut expected = vec![0u8; 2 * 4096];
        expected[..4096].fill(0xEE);
        expected[4096..].fill(0xAB);
        expected
    }

    #[tokio::test]
    async fn memory_runtime_suffix_compaction_uses_configured_compression() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = managed_snapshot_base().join(format!("compact-test-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        let result = async {
            let mut inherited = Vec::new();
            for index in 0..DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS {
                let path = create_tiny_raw_layer(
                    &runtime_root,
                    &format!("inherited-{index:02}"),
                    &[(0, index as u8 + 1)],
                )
                .await;
                inherited.push(json!({ "file": path, "digest": "", "size": 0 }));
            }
            let parent_config_path = temp.path().join("parent-mem-image.json");
            std::fs::write(
                &parent_config_path,
                serde_json::to_vec_pretty(&json!({
                    "repoBlobUrl": "",
                    "lowers": inherited,
                    "upper": {},
                    "resultFile": "",
                    "download": {}
                }))
                .expect("serialize parent image config"),
            )
            .expect("write parent image config");

            let new_layer =
                create_tiny_raw_layer(temp.path(), "mem-delta", &[(0, 0xEE), (4096, 0xAB)]).await;
            let image_config = build_mem_snapshot_image_config(
                Some(&parent_config_path),
                &new_layer,
                temp.path(),
                OverlaybdCompactOutput::ZFile {
                    algorithm: crate::cfg::MemorySnapshotCompressionAlgorithm::Lz4,
                    workers: 1,
                },
            )
            .await
            .expect("build memory image config");

            // Compaction must publish exactly one lower: the final sealed
            // layer, compressed with the configured algorithm.
            assert_eq!(image_config.lowers.len(), 1);
            let compacted = temp.path().join("mem_compacted.commit");
            assert_eq!(PathBuf::from(&image_config.lowers[0].file), compacted);
            assert!(!compacted.with_extension("commit.tmp").exists());

            let (zfile_flag, data) = read_sealed_layer(&compacted, 2 * 4096).await;
            assert_eq!(zfile_flag, 1);
            assert_eq!(data, expected_compacted_pages());
        }
        .await;
        let _ = std::fs::remove_dir_all(&runtime_root);
        result
    }

    #[tokio::test]
    async fn rootfs_runtime_suffix_compaction_remains_raw() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = managed_snapshot_base().join(format!("compact-test-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        let result = async {
            let mut lowers = Vec::new();
            for index in 0..=DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS {
                // The top (last) layer wins page 0 and owns page 1.
                let writes: &[(u64, u8)] = if index == DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS {
                    &[(0, 0xEE), (4096, 0xAB)]
                } else {
                    &[(0, index as u8 + 1)]
                };
                let path =
                    create_tiny_raw_layer(&runtime_root, &format!("rootfs-{index:02}"), writes)
                        .await;
                lowers.push(json!({ "file": path, "digest": "", "size": 0 }));
            }
            let live_dir = temp.path().join("live");
            std::fs::create_dir_all(&live_dir).expect("create live dir");
            let live_config_path = live_dir.join("image.json");
            std::fs::write(
                &live_config_path,
                serde_json::to_vec_pretty(&json!({
                    "lowers": lowers,
                    "upper": {},
                    "resultFile": live_dir.join("result.txt"),
                    "download": {}
                }))
                .expect("serialize live image config"),
            )
            .expect("write live image config");

            let output_dir = temp.path().join("out");
            let staged_path =
                stage_overlaybd_snapshot_from_live_runtime(&live_config_path, &output_dir, None)
                    .await
                    .expect("stage rootfs snapshot");
            let staged =
                overlaybd::config::load_image_config(&staged_path).expect("load staged config");

            // Rootfs compaction must stay raw regardless of the memory
            // snapshot compression policy.
            assert_eq!(staged.lowers.len(), 1);
            let compacted = output_dir.join(MANAGED_BASE_LAYER_FILE);
            assert_eq!(PathBuf::from(&staged.lowers[0].file), compacted);
            assert!(!compacted.with_extension("commit.tmp").exists());

            let (zfile_flag, data) = read_sealed_layer(&compacted, 2 * 4096).await;
            assert_eq!(zfile_flag, 0);
            assert_eq!(data, expected_compacted_pages());
        }
        .await;
        let _ = std::fs::remove_dir_all(&runtime_root);
        result
    }
}
