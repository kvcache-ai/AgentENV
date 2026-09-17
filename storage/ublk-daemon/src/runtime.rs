use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use overlaybd::config::{
    load_image_config, resolve_image_config_local_paths, validate_image_config,
    validate_upper_config, ImageConfig, UpperConfig, UpperMode,
};
use overlaybd::helper::{
    prepare_runtime_upper, relative_path, resolve_config_path, rewrite_overlaybd_lower_paths,
};
use overlaybd::image_file::ImageFile;
use overlaybd::index_file::{validate_rw_header_pair_paths, RwLayout};
use overlaybd::layer_metadata::{read_overlaybd_layer_virtual_size, resolve_local_layer_path};
use overlaybd::virtual_file::VirtualFile;
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;

use crate::protocol::{ResizeToolSpec, SourceStateStrategy};
use crate::server::ImageServiceCache;

const GIB: u64 = 1024 * 1024 * 1024;
const RESIZE_OUTPUT_LIMIT: usize = 64 * 1024;
const RESIZE_OUTPUT_TRUNCATED_MARKER: &[u8] = b" ... (truncated)";
const RUNTIME_IMAGE_CONFIG_FILE: &str = "image.json";
const RUNTIME_UPPER_DATA_FILE: &str = "upper.data";
const RUNTIME_UPPER_INDEX_FILE: &str = "upper.index";
const RUNTIME_SOURCE_LOWERS_DIR: &str = "source-lowers";
const RUNTIME_RESULT_FILE: &str = "result.txt";

#[derive(Debug)]
pub(crate) struct InvalidRequest(pub(crate) String);

impl std::fmt::Display for InvalidRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidRequest {}

fn cleanup_runtime_dir(path: &Path) {
    if let Err(err) = fs::remove_dir_all(path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "failed to remove daemon-owned overlaybd runtime dir during rollback"
            );
        }
    }
}

#[derive(Debug)]
pub(crate) struct MaterializedOverlaybdRuntime {
    pub runtime_image_config_path: PathBuf,
    pub actual_virtual_size: u64,
}

impl MaterializedOverlaybdRuntime {
    pub(crate) fn rollback(self) {
        cleanup_runtime_dir(
            self.runtime_image_config_path
                .parent()
                .expect("runtime config parent"),
        );
    }
}

#[derive(Clone)]
pub(crate) struct MaterializeOverlaybdRuntimeRequest<'a> {
    pub(crate) image_service_cache: &'a ImageServiceCache,
    pub(crate) source_image_config: &'a Path,
    pub(crate) global_config: &'a Path,
    pub(crate) runtime_dir: &'a Path,
    pub(crate) read_only: bool,
    pub(crate) runtime_upper_mode: UpperMode,
    pub(crate) requested_virtual_size: Option<u64>,
    pub(crate) known_source_virtual_size: Option<u64>,
    pub(crate) resize_tool: Option<&'a ResizeToolSpec>,
    /// Dedicated OverlayBD global config used only by the overlaybd-resize
    /// child process (isolated cacheDir, download disabled). The normal
    /// `global_config` above still drives size resolution and the Rust-side
    /// reopen/verification.
    pub(crate) resize_global_config: &'a Path,
    /// Daemon-wide permit serializing overlaybd-resize child processes, which
    /// all share the single isolated resize cacheDir.
    pub(crate) resize_permit: Arc<tokio::sync::Mutex<()>>,
    pub(crate) allow_shrink: bool,
    pub(crate) source_state_strategy: SourceStateStrategy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ResolvedUpperMode {
    Existing(UpperConfig),
    Create(UpperMode),
    Absent,
}

/// Complete a live restack, including its reopen configuration, before reporting success.
/// The caller holds the device's image lock throughout this operation.
pub(crate) async fn restack_runtime(
    image: &ImageFile,
    config_path: &Path,
    output_path: &Path,
    compact_from: Option<usize>,
) -> Result<(
    Option<overlaybd::LayerDescriptor>,
    Vec<overlaybd::config::LayerConfig>,
)> {
    let mut value: Value = serde_json::from_slice(&fs::read(config_path)?)?;
    let mut previous: ImageConfig = serde_json::from_value(value.clone())?;
    validate_image_config(&previous)?;
    let retained_count = compact_from.unwrap_or(previous.lowers.len());
    anyhow::ensure!(
        retained_count <= previous.lowers.len(),
        "invalid runtime compaction boundary"
    );
    if value.get("lowers").is_none() {
        value["lowers"] = json!([]);
    }
    resolve_image_config_local_paths(config_path, &mut previous);
    let runtime_root = fs::canonicalize(config_path.parent().context("runtime config parent")?)?;
    let temporary = tempfile::NamedTempFile::new_in(&runtime_root)?;

    let (descriptor, lowers) = image
        .create_snapshot_and_restack(output_path, compact_from)
        .await?;
    commit_runtime_config(temporary, config_path, value, &lowers, retained_count)?;

    if let Some(start) = compact_from {
        let retained: Vec<_> = lowers
            .iter()
            .filter_map(resolve_local_layer_path)
            .map(|path| fs::canonicalize(&path).unwrap_or(path))
            .collect();
        for lower in &previous.lowers[start..] {
            let Some(path) = resolve_local_layer_path(lower) else {
                continue;
            };
            let Ok(path) = fs::canonicalize(path) else {
                continue;
            };
            if !path.starts_with(&runtime_root) || retained.contains(&path) {
                continue;
            }
            if let Err(error) = tokio::fs::remove_file(&path).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(%error, path = %path.display(), "failed to reclaim compacted runtime layer");
                }
            }
            if let Some(parent) = path.parent().filter(|parent| *parent != runtime_root) {
                let _ = tokio::fs::remove_dir(parent).await;
            }
        }
    }
    Ok((descriptor, lowers))
}

fn commit_runtime_config(
    mut temporary: tempfile::NamedTempFile,
    path: &Path,
    mut value: Value,
    lowers: &[overlaybd::config::LayerConfig],
    retained_count: usize,
) -> Result<()> {
    use std::io::Write;
    // Every failure here follows the live mutation and must stop the caller's VM.
    (|| -> Result<()> {
        let changed = lowers
            .get(retained_count..)
            .context("restacked lower count mismatch")?;
        let raw_lowers = value["lowers"]
            .as_array_mut()
            .context("runtime lowers array")?;
        raw_lowers.truncate(retained_count);
        for lower in changed {
            raw_lowers.push(serde_json::to_value(lower)?);
        }
        temporary.write_all(&serde_json::to_vec_pretty(&value)?)?;
        temporary.persist(path).map_err(|error| error.error)?;
        Ok(())
    })()
    .map_err(|error| {
        overlaybd::RestackSnapshotTerminalFailure::new(format!(
            "commit runtime image config after restack: {error:#}"
        ))
        .into()
    })
}

pub(crate) async fn materialize_overlaybd_runtime(
    request: MaterializeOverlaybdRuntimeRequest<'_>,
) -> Result<MaterializedOverlaybdRuntime> {
    let runtime_dir = request.runtime_dir;
    fs::create_dir(runtime_dir).with_context(|| {
        format!(
            "claim overlaybd runtime dir {} (it must not already exist)",
            runtime_dir.display()
        )
    })?;
    materialize_runtime_contents(request)
        .await
        .inspect_err(|_| cleanup_runtime_dir(runtime_dir))
}

async fn materialize_runtime_contents(
    request: MaterializeOverlaybdRuntimeRequest<'_>,
) -> Result<MaterializedOverlaybdRuntime> {
    let MaterializeOverlaybdRuntimeRequest {
        image_service_cache,
        source_image_config,
        global_config,
        runtime_dir,
        read_only,
        runtime_upper_mode,
        requested_virtual_size,
        known_source_virtual_size,
        resize_tool,
        resize_global_config,
        resize_permit,
        allow_shrink,
        source_state_strategy,
    } = request;

    let source_image_config =
        std::path::absolute(source_image_config).context("resolve source image config path")?;
    let source_image_config = source_image_config.as_path();
    let raw = fs::read(source_image_config).with_context(|| {
        format!(
            "read overlaybd image config '{}'",
            source_image_config.display()
        )
    })?;
    let mut image: Value = serde_json::from_slice(&raw).with_context(|| {
        format!(
            "parse overlaybd image config '{}'",
            source_image_config.display()
        )
    })?;
    // Keep unknown fields in the output JSON while resolving local file ownership.
    let mut source: ImageConfig =
        serde_json::from_value(image.clone()).context("parse source OverlayBD image config")?;
    resolve_image_config_local_paths(source_image_config, &mut source);
    let base_virtual_size = match known_source_virtual_size {
        Some(size) => size,
        None => resolve_overlaybd_virtual_size(
            image_service_cache,
            source_image_config,
            &source,
            global_config,
        )
        .await
        .with_context(|| {
            format!(
                "resolve overlaybd base virtual size from {}",
                source_image_config.display()
            )
        })?,
    };
    anyhow::ensure!(
        base_virtual_size > 0,
        "overlaybd base virtual size must be non-zero"
    );
    let actual_virtual_size =
        validate_requested_virtual_size(requested_virtual_size, base_virtual_size, allow_shrink)
            .map_err(|err| InvalidRequest(format!("{err:#}")))?;

    let upper_mode =
        resolve_upper_mode(&image, source_image_config, read_only, runtime_upper_mode)?;
    if source_state_strategy == SourceStateStrategy::Consume
        && matches!(upper_mode, ResolvedUpperMode::Create(_))
    {
        return Err(InvalidRequest(
            "source state consume requires an existing writable OverlayBD upper".to_string(),
        )
        .into());
    }
    if actual_virtual_size != base_virtual_size
        && !matches!(upper_mode, ResolvedUpperMode::Create(_))
    {
        return Err(InvalidRequest(
            "resizing requires a fresh writable OverlayBD upper".to_string(),
        )
        .into());
    }
    let source_root = fs::canonicalize(source_image_config.parent().unwrap())?;
    adopt_source_owned_lowers(&mut image, &source, &source_root, runtime_dir).await?;
    let upper_mode = match (source_state_strategy, upper_mode) {
        (SourceStateStrategy::Consume, ResolvedUpperMode::Existing(upper)) => {
            ResolvedUpperMode::Existing(
                consume_existing_upper(upper, &source_root, runtime_dir).await?,
            )
        }
        (_, upper_mode) => upper_mode,
    };
    match &upper_mode {
        ResolvedUpperMode::Absent | ResolvedUpperMode::Existing(_) => {}
        ResolvedUpperMode::Create(mode) => {
            let upper_data_path = runtime_dir.join(RUNTIME_UPPER_DATA_FILE);
            let upper_index_path = runtime_upper_index_path(runtime_dir, *mode);
            prepare_runtime_upper(
                &upper_data_path,
                upper_index_path.as_deref(),
                base_virtual_size,
                *mode,
            )
            .with_context(|| {
                format!(
                    "prepare overlaybd runtime upper in {}",
                    runtime_dir.display()
                )
            })?;
        }
    }

    let image =
        materialize_overlaybd_image_config(image, source_image_config, runtime_dir, &upper_mode)?;
    let runtime_image_config_path = runtime_dir.join(RUNTIME_IMAGE_CONFIG_FILE);
    fs::write(
        &runtime_image_config_path,
        serde_json::to_vec_pretty(&image).context("serialize overlaybd runtime image config")?,
    )
    .with_context(|| {
        format!(
            "write overlaybd runtime image config {}",
            runtime_image_config_path.display()
        )
    })?;

    if actual_virtual_size != base_virtual_size {
        let tool = resize_tool.ok_or_else(|| {
            InvalidRequest(
                "overlaybd resize tool is required when virtual size changes".to_string(),
            )
        })?;
        // Serialize overlaybd-resize children daemon-wide: they all share the
        // single isolated resize cacheDir. The guard is held only around the
        // child process and is released on every exit path (normal, non-zero
        // exit, timeout, spawn/read errors) when it drops below or during
        // error propagation; verification reopens through the Rust
        // ImageService with the request's normal global config.
        let resize_guard = resize_permit.lock().await;
        run_resize_tool(
            tool,
            &runtime_image_config_path,
            resize_global_config,
            actual_virtual_size,
        )
        .await?;
        drop(resize_guard);
        verify_resized_runtime(
            image_service_cache,
            &runtime_image_config_path,
            global_config,
            runtime_dir,
            &upper_mode,
            actual_virtual_size,
        )
        .await?;
    }

    Ok(MaterializedOverlaybdRuntime {
        runtime_image_config_path,
        actual_virtual_size,
    })
}

async fn resolve_overlaybd_virtual_size(
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    image_cfg: &ImageConfig,
    global_config: &Path,
) -> Result<u64> {
    validate_image_config(image_cfg)?;
    for layer in image_cfg.lowers.iter().rev() {
        let Some(local_path) = resolve_local_layer_path(layer) else {
            continue;
        };
        match read_overlaybd_layer_virtual_size(&local_path) {
            Ok(size) => return Ok(size),
            Err(err) => {
                tracing::debug!(
                    layer_path = %local_path.display(),
                    error = ?err,
                    "fall back to opening overlaybd image for virtual_size"
                );
                break;
            }
        }
    }

    open_overlaybd_virtual_size(image_service_cache, image_config, image_cfg, global_config).await
}

fn validate_requested_virtual_size(
    requested_virtual_size: Option<u64>,
    base_virtual_size: u64,
    allow_shrink: bool,
) -> Result<u64> {
    let Some(requested) = requested_virtual_size else {
        return Ok(base_virtual_size);
    };
    if requested == base_virtual_size {
        return Ok(base_virtual_size);
    }
    anyhow::ensure!(
        requested > 0,
        "requested overlaybd runtime virtual size must be non-zero"
    );
    anyhow::ensure!(
        requested.is_multiple_of(GIB),
        "requested overlaybd runtime virtual size {requested} must be GiB-aligned"
    );
    anyhow::ensure!(
        requested >= base_virtual_size || allow_shrink,
        "requested overlaybd runtime virtual size {requested} is smaller than base virtual size {base_virtual_size}; shrinking is disabled"
    );
    Ok(requested)
}

async fn run_resize_tool(
    tool: &ResizeToolSpec,
    image_config: &Path,
    resize_global_config: &Path,
    target_size: u64,
) -> Result<()> {
    let runtime_dir = image_config
        .parent()
        .context("overlaybd runtime image config has no parent directory")?;
    let mut command = tokio::process::Command::new(&tool.binary);
    command
        .current_dir(runtime_dir)
        .arg("--config")
        .arg(image_config)
        .arg("--size")
        .arg((target_size / GIB).to_string())
        .arg("--service_config_path")
        .arg(resize_global_config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(lib_dir) = &tool.lib_dir {
        command.env(
            "LD_LIBRARY_PATH",
            overlaybd_library_path(lib_dir, std::env::var_os("LD_LIBRARY_PATH").as_deref())?,
        );
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("execute overlaybd-resize {}", tool.binary.display()))?;
    let stdout = child
        .stdout
        .take()
        .context("capture overlaybd-resize stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("capture overlaybd-resize stderr")?;
    let timeout = Duration::from_secs(tool.timeout_secs);
    let output = tokio::time::timeout(timeout, async {
        let (stdout, stderr, status) = tokio::join!(
            read_bounded_output(stdout),
            read_bounded_output(stderr),
            child.wait()
        );
        Ok::<_, anyhow::Error>((
            stdout.context("read overlaybd-resize stdout")?,
            stderr.context("read overlaybd-resize stderr")?,
            status.context("wait for overlaybd-resize")?,
        ))
    })
    .await;
    let (stdout, stderr, status) = match output {
        Ok(output) => output?,
        Err(_) => {
            child
                .kill()
                .await
                .context("kill timed out overlaybd-resize")?;
            let _ = child.wait().await;
            anyhow::bail!(
                "overlaybd-resize timed out after {} seconds",
                tool.timeout_secs
            );
        }
    };
    anyhow::ensure!(
        status.success(),
        "overlaybd-resize failed (status={status}): stdout={} stderr={}",
        String::from_utf8_lossy(&stdout).trim(),
        String::from_utf8_lossy(&stderr).trim()
    );
    Ok(())
}

fn overlaybd_library_path(lib_dir: &Path, inherited: Option<&OsStr>) -> Result<OsString> {
    let mut paths = vec![lib_dir.to_path_buf()];
    if let Some(inherited) = inherited.filter(|value| !value.is_empty()) {
        paths.extend(std::env::split_paths(inherited));
    }
    std::env::join_paths(paths).context("construct overlaybd-resize LD_LIBRARY_PATH")
}

async fn read_bounded_output(mut reader: impl tokio::io::AsyncRead + Unpin) -> Result<Vec<u8>> {
    let retained_limit = RESIZE_OUTPUT_LIMIT - RESIZE_OUTPUT_TRUNCATED_MARKER.len();
    let mut retained = Vec::new();
    let mut truncated = false;
    let mut chunk = [0u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        let remaining = retained_limit.saturating_sub(retained.len());
        if remaining > 0 {
            retained.extend_from_slice(&chunk[..read.min(remaining)]);
        }
        truncated |= read > remaining;
    }
    if truncated {
        retained.extend_from_slice(RESIZE_OUTPUT_TRUNCATED_MARKER);
    }
    Ok(retained)
}

fn runtime_upper_index_path(runtime_dir: &Path, mode: UpperMode) -> Option<PathBuf> {
    matches!(
        mode,
        UpperMode::LogStructured | UpperMode::HybridLogStructured
    )
    .then(|| runtime_dir.join(RUNTIME_UPPER_INDEX_FILE))
}

async fn open_overlaybd_virtual_size(
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    image_cfg: &ImageConfig,
    global_config: &Path,
) -> Result<u64> {
    let image_service = image_service_cache
        .get_or_create(global_config)
        .await
        .with_context(|| format!("load overlaybd global config '{}'", global_config.display()))?;
    let image = ImageFile::open(
        image_cfg.clone(),
        image_service,
        Some(std::fs::canonicalize(image_config).unwrap_or_else(|_| image_config.to_path_buf())),
    )
    .await
    .with_context(|| format!("open overlaybd image config '{}'", image_config.display()))?;
    image.size().await.with_context(|| {
        format!(
            "read overlaybd virtual size from '{}'",
            image_config.display()
        )
    })
}

async fn verify_resized_runtime(
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
    runtime_dir: &Path,
    upper_mode: &ResolvedUpperMode,
    expected_size: u64,
) -> Result<()> {
    if let ResolvedUpperMode::Create(mode) = upper_mode {
        let index_path = runtime_upper_index_path(runtime_dir, *mode);
        validate_rw_header_pair_paths(
            &runtime_dir.join(RUNTIME_UPPER_DATA_FILE),
            index_path.as_deref(),
            expected_size,
            RwLayout::from(*mode),
        )
        .context("validate resized OverlayBD RW header pair")?;
    }

    let image_cfg = load_image_config(image_config)
        .with_context(|| format!("load overlaybd image config '{}'", image_config.display()))?;
    let reopened =
        open_overlaybd_virtual_size(image_service_cache, image_config, &image_cfg, global_config)
            .await
            .context("reopen resized overlaybd runtime")?;
    anyhow::ensure!(
        reopened == expected_size,
        "resized overlaybd runtime size mismatch: expected {expected_size}, got {reopened}"
    );
    Ok(())
}

fn resolve_upper_mode(
    image: &Value,
    image_config_path: &Path,
    read_only: bool,
    runtime_upper_mode: UpperMode,
) -> Result<ResolvedUpperMode> {
    if read_only {
        return Ok(ResolvedUpperMode::Absent);
    }
    let mut upper: UpperConfig =
        serde_json::from_value(image.get("upper").cloned().unwrap_or_else(|| json!({})))
            .with_context(|| format!("parse overlaybd upper '{}'", image_config_path.display()))?;
    if !validate_upper_config(&upper)? {
        return Ok(ResolvedUpperMode::Create(runtime_upper_mode));
    }
    let base = image_config_path.parent().unwrap_or_else(|| Path::new("."));
    for path in [
        &mut upper.data,
        &mut upper.index,
        &mut upper.target,
        &mut upper.gzip_index,
    ] {
        if !path.is_empty() {
            *path = resolve_config_path(base, path)?
                .to_string_lossy()
                .into_owned();
        }
    }
    upper.mode = Some(upper.writable_mode());
    Ok(ResolvedUpperMode::Existing(upper))
}

async fn adopt_source_owned_lowers(
    image: &mut Value,
    source_config: &ImageConfig,
    source_root: &Path,
    runtime_dir: &Path,
) -> Result<()> {
    let runtime_dir = std::path::absolute(runtime_dir)
        .context("resolve OverlayBD runtime directory for lower adoption")?;
    for (layer_index, layer) in source_config.lowers.iter().enumerate() {
        let Some(source) = resolve_local_layer_path(layer) else {
            continue;
        };
        let Some(source) = fs::canonicalize(source).ok() else {
            continue;
        };
        let source_owned = source.starts_with(source_root);
        if !source_owned && layer.file.is_empty() {
            // Cache-owned directory layers keep their existing open/download path.
            image["lowers"][layer_index]["dir"] =
                Value::String(source.parent().unwrap().to_string_lossy().into_owned());
            continue;
        }
        let file = if source_owned {
            let destination = runtime_dir
                .join(RUNTIME_SOURCE_LOWERS_DIR)
                .join(format!("{layer_index:04}"))
                .join(source.file_name().context("local layer filename")?);
            link_immutable_layer(&source, &destination).await?;
            destination
        } else {
            source
        };
        let lower = image["lowers"][layer_index]
            .as_object_mut()
            .with_context(|| format!("overlaybd lower {layer_index} should be an object"))?;
        lower.insert(
            "file".into(),
            Value::String(file.to_string_lossy().into_owned()),
        );
        if !layer.dir.is_empty() {
            lower.insert("dir".into(), Value::String(String::new()));
        }
    }

    Ok(())
}

async fn consume_existing_upper(
    mut upper: UpperConfig,
    source_root: &Path,
    runtime_dir: &Path,
) -> Result<UpperConfig> {
    anyhow::ensure!(
        upper.target.is_empty() && upper.gzip_index.is_empty(),
        "consume does not support OverlayBD upper.target or upper.gzipIndex"
    );
    let source_data = ensure_source_owned_state(Path::new(&upper.data), source_root)?;
    let source_index = (!upper.index.is_empty())
        .then(|| ensure_source_owned_state(Path::new(&upper.index), source_root))
        .transpose()?;
    let data_path = runtime_dir.join(RUNTIME_UPPER_DATA_FILE);
    consume_state_file(
        &source_data,
        &data_path,
        upper.writable_mode() == UpperMode::Sparse,
    )
    .await?;
    upper.data = data_path.to_string_lossy().into_owned();
    if let Some(source) = source_index {
        let destination = runtime_dir.join(RUNTIME_UPPER_INDEX_FILE);
        consume_state_file(&source, &destination, false).await?;
        upper.index = destination.to_string_lossy().into_owned();
    }
    Ok(upper)
}

fn ensure_source_owned_state(path: &Path, source_root: &Path) -> Result<PathBuf> {
    let resolved = fs::canonicalize(path)
        .with_context(|| format!("resolve OverlayBD source state {}", path.display()))?;
    anyhow::ensure!(
        resolved.starts_with(source_root),
        "cannot consume OverlayBD source state {} outside source artifact root {}",
        path.display(),
        source_root.display()
    );
    Ok(resolved)
}

async fn consume_state_file(
    source: &Path,
    destination: &Path,
    preserve_sparse_layout: bool,
) -> Result<()> {
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create OverlayBD runtime directory {}", parent.display()))?;
    }

    // Keep the normal same-filesystem rename fast path. A Temporal resume may
    // place the persisted source and runtime directory on different mounts,
    // in which case rename returns EXDEV and the source must be copied before
    // it is consumed.
    match tokio::fs::rename(source, destination).await {
        Ok(()) => return Ok(()),
        Err(error) if error.raw_os_error() != Some(libc::EXDEV) => {
            return Err(error).with_context(|| {
                format!(
                    "consume OverlayBD source state {} into {}",
                    source.display(),
                    destination.display()
                )
            })
        }
        Err(_) => {}
    }

    copy_runtime_artifact(source, destination, preserve_sparse_layout).await?;
    tokio::fs::remove_file(source).await.with_context(|| {
        format!(
            "remove consumed OverlayBD source state {}",
            source.display()
        )
    })?;
    Ok(())
}

async fn copy_runtime_artifact(
    source: &Path,
    destination: &Path,
    preserve_sparse_layout: bool,
) -> Result<()> {
    if preserve_sparse_layout {
        storage_util::copy_sparse_file(source, destination).await
    } else {
        tokio::fs::copy(source, destination).await.map(|_| ())
    }
    .with_context(|| {
        format!(
            "copy OverlayBD artifact {} into {} across filesystems",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

async fn link_immutable_layer(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create OverlayBD lower directory {}", parent.display()))?;
    }
    match tokio::fs::hard_link(source, destination).await {
        Ok(()) => Ok(()),
        Err(error) if error.raw_os_error() == Some(libc::EXDEV) => {
            copy_runtime_artifact(source, destination, true).await
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "link immutable OverlayBD lower {} into {}",
                source.display(),
                destination.display()
            )
        }),
    }
}

fn materialize_overlaybd_image_config(
    mut value: Value,
    image_config_path: &Path,
    runtime_dir: &Path,
    upper_mode: &ResolvedUpperMode,
) -> Result<Value> {
    let base = image_config_path.parent().unwrap_or_else(|| Path::new("."));

    rewrite_overlaybd_lower_paths(&mut value, base, runtime_dir)?;
    let upper = match upper_mode {
        ResolvedUpperMode::Absent => json!({}),
        ResolvedUpperMode::Create(mode) => serde_json::to_value(UpperConfig {
            mode: Some(*mode),
            data: "./upper.data".into(),
            index: if *mode == UpperMode::Sparse {
                String::new()
            } else {
                "./upper.index".into()
            },
            ..Default::default()
        })?,
        ResolvedUpperMode::Existing(upper) => {
            let mut upper = upper.clone();
            for path in [
                &mut upper.data,
                &mut upper.index,
                &mut upper.target,
                &mut upper.gzip_index,
            ] {
                if !path.is_empty() {
                    *path = relative_path(runtime_dir, Path::new(path))?
                        .to_string_lossy()
                        .into_owned();
                }
            }
            serde_json::to_value(upper)?
        }
    };

    let obj = value
        .as_object_mut()
        .context("overlaybd image config should be an object")?;
    obj.insert("upper".to_string(), upper);
    obj.insert(
        "resultFile".to_string(),
        Value::String(format!("./{RUNTIME_RESULT_FILE}")),
    );

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::fs::{FileExt, MetadataExt};
    use std::sync::Arc;

    use overlaybd::backend::local::LocalFile;
    use overlaybd::config::GlobalConfig;
    use overlaybd::index_file::{create_file_rw, LayerInfo};

    async fn test_cache(dir: &Path) -> (ImageServiceCache, PathBuf) {
        let global_config = dir.join("global.json");
        fs::write(
            &global_config,
            serde_json::to_vec_pretty(&GlobalConfig::default()).unwrap(),
        )
        .unwrap();
        let cache = ImageServiceCache::new(None);
        cache.get_or_create(&global_config).await.unwrap();
        (cache, global_config)
    }

    fn write_source(dir: &Path, value: Value) -> PathBuf {
        let path = dir.join("source-image.json");
        fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        path
    }

    async fn create_sealed_lower(path: &Path, index_path: &Path, payload: &[u8]) {
        let data_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path).unwrap());
        let index_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(index_path).unwrap());
        let args = LayerInfo::new(data_file, Some(index_file), payload.len() as u64);
        let lower = create_file_rw(args).await.unwrap();
        lower.write_at(0, payload).await.unwrap();
        lower.close_seal().await.unwrap();
    }

    fn materialize_test_request<'a>(
        image_service_cache: &'a ImageServiceCache,
        source_image_config: &'a Path,
        global_config: &'a Path,
        runtime_dir: &'a Path,
    ) -> MaterializeOverlaybdRuntimeRequest<'a> {
        MaterializeOverlaybdRuntimeRequest {
            image_service_cache,
            source_image_config,
            global_config,
            runtime_dir,
            read_only: false,
            runtime_upper_mode: UpperMode::LogStructured,
            requested_virtual_size: None,
            known_source_virtual_size: None,
            resize_tool: None,
            resize_global_config: global_config,
            resize_permit: Arc::new(tokio::sync::Mutex::new(())),
            allow_shrink: false,
            source_state_strategy: SourceStateStrategy::Reuse,
        }
    }

    async fn open_restack_test_image(
        dir: &Path,
    ) -> (ImageFile, PathBuf, ImageServiceCache, PathBuf) {
        let (cache, global) = test_cache(dir).await;
        let base = dir.join("base.commit");
        create_sealed_lower(&base, &dir.join("base.index"), &[0x11; 8192]).await;
        prepare_runtime_upper(&dir.join("upper.data"), None, 8192, UpperMode::Sparse).unwrap();
        let config = write_source(
            dir,
            json!({
                "lowers": [{"file": "./base.commit", "layerExtension": {"keep": true}}],
                "upper": {"data": "./upper.data", "mode": "sparse"},
                "imageExtension": {"keep": true},
            }),
        );
        let image = cache
            .get_or_create(&global)
            .await
            .unwrap()
            .create_image_file(&config)
            .await
            .unwrap();
        (image, config, cache, global)
    }

    #[tokio::test]
    async fn restack_success_commits_reopen_config_and_reclaims_only_replaced_runtime_layers() {
        let temp = tempfile::tempdir().unwrap();
        let (image, config, cache, global) = open_restack_test_image(temp.path()).await;
        let original: Value = serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
        let first = temp.path().join("layers/first/snapshot.commit");
        let second = temp.path().join("layers/second/snapshot.commit");
        image.write_at(4096, &[0x22; 4096]).await.unwrap();
        restack_runtime(&image, &config, &first, None)
            .await
            .unwrap();
        image.write_at(4096, &[0x33; 4096]).await.unwrap();
        restack_runtime(&image, &config, &second, None)
            .await
            .unwrap();
        let exported = temp.path().join("exported.commit");
        fs::hard_link(&first, &exported).unwrap();
        let exported_bytes = fs::read(&exported).unwrap();
        image.write_at(4096, &[0x44; 4096]).await.unwrap();
        let final_layer = temp.path().join("layers/third/snapshot.commit");
        let (_, lowers) = restack_runtime(&image, &config, &final_layer, Some(1))
            .await
            .unwrap();
        assert_eq!(lowers.len(), 3);
        let committed: Value = serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
        assert_eq!(committed["imageExtension"], original["imageExtension"]);
        assert_eq!(committed["lowers"][0], original["lowers"][0]);
        assert_eq!(committed["upper"], original["upper"]);
        assert!(!first.exists());
        assert!(!second.exists());
        assert!(temp.path().join("base.commit").exists());
        assert!(final_layer.exists());
        assert_eq!(fs::read(exported).unwrap(), exported_bytes);
        drop(image);
        let reopened = cache
            .get_or_create(&global)
            .await
            .unwrap()
            .create_image_file(&config)
            .await
            .unwrap();
        assert_eq!(
            reopened.read_at(0, 4096).await.unwrap().as_ref(),
            &[0x11; 4096]
        );
        assert_eq!(
            reopened.read_at(4096, 4096).await.unwrap().as_ref(),
            &[0x44; 4096]
        );
        reopened.write_at(0, &[0x55; 4096]).await.unwrap();
        assert_eq!(
            reopened.read_at(0, 4096).await.unwrap().as_ref(),
            &[0x55; 4096]
        );
    }

    #[tokio::test]
    async fn invalid_runtime_config_rejects_restack_before_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let (image, config, _, _) = open_restack_test_image(temp.path()).await;
        fs::write(&config, "{}").unwrap();
        let output = temp.path().join("snapshot.commit");
        let error = restack_runtime(&image, &config, &output, None)
            .await
            .unwrap_err();
        assert!(!error.is::<overlaybd::RestackSnapshotTerminalFailure>());
        assert!(!output.exists());
        image.write_at(4096, &[0x22; 4096]).await.unwrap();
        assert_eq!(
            image.read_at(4096, 4096).await.unwrap().as_ref(),
            &[0x22; 4096]
        );
    }

    #[tokio::test]
    async fn config_commit_failure_after_live_restack_is_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let (image, config, _, _) = open_restack_test_image(temp.path()).await;
        let value: Value = serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
        let temporary = tempfile::NamedTempFile::new_in(temp.path()).unwrap();
        image.write_at(4096, &[0x22; 4096]).await.unwrap();
        let (_, lowers) = image
            .create_snapshot_and_restack(&temp.path().join("snapshot.commit"), None)
            .await
            .unwrap();
        // Fail the actual atomic replacement at the post-mutation commit boundary.
        fs::remove_file(&config).unwrap();
        fs::create_dir(&config).unwrap();
        let error = commit_runtime_config(temporary, &config, value, &lowers, 1).unwrap_err();
        assert!(error.is::<overlaybd::RestackSnapshotTerminalFailure>());
        assert!(temp.path().join("snapshot.commit").exists());
        assert_eq!(
            image.read_at(4096, 4096).await.unwrap().as_ref(),
            &[0x22; 4096]
        );
    }

    #[tokio::test]
    async fn materialize_writable_lower_only_runtime_resolves_base_size_when_unknown() {
        let temp = tempfile::tempdir().unwrap();
        let (cache, global_config) = test_cache(temp.path()).await;
        let lower_path = temp.path().join("lower.data");
        let lower_index_path = temp.path().join("lower.index");
        let payload = vec![0x5A; 8192];
        create_sealed_lower(&lower_path, &lower_index_path, &payload).await;
        let source = write_source(
            temp.path(),
            json!({
                "lowers": [{ "file": lower_path }],
                "upper": {},
                "resultFile": ""
            }),
        );
        let runtime_dir = temp.path().join("runtime");

        let runtime = materialize_overlaybd_runtime(materialize_test_request(
            &cache,
            &source,
            &global_config,
            &runtime_dir,
        ))
        .await
        .unwrap();

        assert_eq!(runtime.actual_virtual_size, payload.len() as u64);
        assert!(runtime_dir.join(RUNTIME_UPPER_DATA_FILE).exists());
        assert!(runtime_dir.join(RUNTIME_UPPER_INDEX_FILE).exists());
    }

    #[tokio::test]
    async fn consume_runtime_transfers_writable_upper_without_copying() {
        let temp = tempfile::tempdir_in(".").unwrap();
        let source_root = fs::canonicalize(temp.path()).unwrap();
        let (cache, global_config) = test_cache(temp.path()).await;
        let source_upper_data = source_root.join("source-upper.data");
        let source_upper_index = source_root.join("source-upper.index");
        prepare_runtime_upper(
            &source_upper_data,
            Some(&source_upper_index),
            12288,
            UpperMode::LogStructured,
        )
        .unwrap();
        let source_upper_identity = |path: &Path| {
            let metadata = fs::metadata(path).unwrap();
            (metadata.dev(), metadata.ino())
        };
        let source_upper_data_identity = source_upper_identity(&source_upper_data);
        let source_upper_index_identity = source_upper_identity(&source_upper_index);
        // Keep the config in the test's working directory so its bare filename
        // can be passed without changing the process-wide current directory.
        let source_file = tempfile::NamedTempFile::new_in(".").unwrap();
        fs::write(
            source_file.path(),
            serde_json::to_vec(&json!({
                "lowers": [],
                "upper": {
                    "mode": "logStructured",
                    "data": source_upper_data,
                    "index": source_upper_index
                },
                "resultFile": ""
            }))
            .unwrap(),
        )
        .unwrap();
        let source = Path::new(source_file.path().file_name().unwrap());
        let runtime_dir = temp.path().join("runtime");
        let mut request = materialize_test_request(&cache, source, &global_config, &runtime_dir);
        request.source_state_strategy = SourceStateStrategy::Consume;

        materialize_overlaybd_runtime(request).await.unwrap();

        for (source, name, identity) in [
            (
                source_upper_data,
                RUNTIME_UPPER_DATA_FILE,
                source_upper_data_identity,
            ),
            (
                source_upper_index,
                RUNTIME_UPPER_INDEX_FILE,
                source_upper_index_identity,
            ),
        ] {
            assert!(!source.exists());
            assert_eq!(
                source_upper_identity(&runtime_dir.join(name)),
                identity,
                "Consume must rename the {name} inode"
            );
        }
    }

    #[tokio::test]
    async fn consume_runtime_survives_source_deletion_across_filesystems() {
        let temp = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir_in("/dev/shm").unwrap();
        if fs::metadata(temp.path()).unwrap().dev()
            == fs::metadata(destination.path()).unwrap().dev()
        {
            eprintln!("cross-filesystem test requires /dev/shm on a different mount");
            return;
        }
        let (cache, global_config) = test_cache(temp.path()).await;
        let source_dir = temp.path().join("source");
        fs::create_dir(&source_dir).unwrap();
        let upper = source_dir.join("upper.data");
        prepare_runtime_upper(&upper, None, 16384, UpperMode::Sparse).unwrap();
        let upper_file = fs::OpenOptions::new().write(true).open(&upper).unwrap();
        upper_file.write_all_at(&[0; 4096], 4096).unwrap();
        upper_file.write_all_at(&[0xa7; 4096], 12288).unwrap();
        drop(upper_file);
        let lower = source_dir.join("lower.data");
        create_sealed_lower(&lower, &source_dir.join("lower.index"), &[0x5a; 16384]).await;
        let expected = fs::read(&lower).unwrap();
        let source = write_source(
            &source_dir,
            json!({
                "lowers": [{"file": lower, "layerExtension": {"version": 1}}],
                "imageExtension": {"policy": "keep"},
                "upper": {"mode": "sparse", "data": upper},
            }),
        );
        let runtime_dir = destination.path().join("runtime");
        let mut request = materialize_test_request(&cache, &source, &global_config, &runtime_dir);
        request.known_source_virtual_size = Some(16384);
        request.source_state_strategy = SourceStateStrategy::Consume;
        let runtime = materialize_overlaybd_runtime(request).await.unwrap();
        assert!(!upper.exists());
        fs::remove_dir_all(source_dir).unwrap();

        let raw: Value =
            serde_json::from_slice(&fs::read(&runtime.runtime_image_config_path).unwrap()).unwrap();
        assert_eq!(raw["imageExtension"], json!({"policy": "keep"}));
        assert_eq!(raw["lowers"][0]["layerExtension"], json!({"version": 1}));
        let image = load_image_config(&runtime.runtime_image_config_path).unwrap();
        assert_eq!(fs::read(&image.lowers[0].file).unwrap(), expected);
        assert!(Path::new(&image.upper.data).is_file());
        let image = ImageFile::open(
            image,
            cache.get_or_create(&global_config).await.unwrap(),
            None,
        )
        .await
        .unwrap();
        let restored = image.read_at(0, 16384).await.unwrap();
        assert!(restored[..4096].iter().all(|byte| *byte == 0));
        assert!(restored[4096..8192].iter().all(|byte| *byte == 0x5a));
        assert!(restored[8192..12288].iter().all(|byte| *byte == 0xa7));
        assert!(restored[12288..].iter().all(|byte| *byte == 0x5a));
    }

    #[tokio::test]
    async fn consume_runtime_rejects_upper_outside_source_artifact_root() {
        let temp = tempfile::tempdir().unwrap();
        let (cache, global_config) = test_cache(temp.path()).await;
        let source_dir = temp.path().join("source");
        fs::create_dir_all(&source_dir).unwrap();
        let external_upper = temp.path().join("external-upper.data");
        prepare_runtime_upper(&external_upper, None, 12288, UpperMode::Sparse).unwrap();
        let source = write_source(
            &source_dir,
            json!({
                "lowers": [],
                "upper": { "mode": "sparse", "data": external_upper },
                "resultFile": ""
            }),
        );
        let runtime_dir = temp.path().join("runtime");
        let mut request = materialize_test_request(&cache, &source, &global_config, &runtime_dir);
        request.known_source_virtual_size = Some(12288);
        request.source_state_strategy = SourceStateStrategy::Consume;

        let err = materialize_overlaybd_runtime(request).await.unwrap_err();

        assert!(err.to_string().contains("outside source artifact root"));
        assert!(external_upper.exists());

        fs::write(&source, br#"{"lowers":[],"upper":{}}"#).unwrap();
        let mut request = materialize_test_request(&cache, &source, &global_config, &runtime_dir);
        request.known_source_virtual_size = Some(12288);
        request.source_state_strategy = SourceStateStrategy::Consume;
        let err = materialize_overlaybd_runtime(request).await.unwrap_err();
        assert!(err.is::<InvalidRequest>(), "{err:#}");
        assert!(!runtime_dir.exists());
    }

    fn fake_resize_tool(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("fake-resize.sh");
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).unwrap();
        path
    }

    #[test]
    fn overlaybd_library_path_prepends_tool_lib_and_ignores_empty_inherited_value() {
        let lib_dir = Path::new("/overlaybd/lib");
        let inherited = std::env::join_paths(["/first", "/second"]).unwrap();
        assert_eq!(
            overlaybd_library_path(lib_dir, Some(&inherited)).unwrap(),
            std::env::join_paths(["/overlaybd/lib", "/first", "/second"]).unwrap()
        );
        assert_eq!(
            overlaybd_library_path(lib_dir, Some(OsStr::new(""))).unwrap(),
            OsString::from("/overlaybd/lib")
        );
    }

    #[tokio::test]
    async fn read_bounded_output_marks_truncation_within_limit() {
        let output = read_bounded_output(&[b'x'; RESIZE_OUTPUT_LIMIT + 1][..])
            .await
            .unwrap();
        assert_eq!(output.len(), RESIZE_OUTPUT_LIMIT);
        assert!(output.ends_with(RESIZE_OUTPUT_TRUNCATED_MARKER));
    }

    #[tokio::test]
    async fn materialize_resize_runs_fake_tool_and_reopens_target_size() {
        let temp = tempfile::tempdir().unwrap();
        let (cache, global_config) = test_cache(temp.path()).await;
        let lower_path = temp.path().join("lower.data");
        let lower_index_path = temp.path().join("lower.index");
        let data_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&lower_path).unwrap());
        let index_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&lower_index_path).unwrap());
        let lower = create_file_rw(LayerInfo::new(data_file, Some(index_file), GIB))
            .await
            .unwrap();
        lower.write_at(0, &[0x5A; 4096]).await.unwrap();
        lower.close_seal().await.unwrap();
        let source = write_source(
            temp.path(),
            json!({
                "lowers": [{ "file": lower_path }],
                "upper": {},
                "resultFile": ""
            }),
        );

        let templates = temp.path().join("templates");
        fs::create_dir(&templates).unwrap();
        let template_data = templates.join("upper.data");
        let template_index = templates.join("upper.index");
        prepare_runtime_upper(
            &template_data,
            Some(&template_index),
            2 * GIB,
            UpperMode::LogStructured,
        )
        .unwrap();
        let runtime = temp.path().join("runtime");
        let observed = temp.path().join("observed");
        let tool_lib = temp.path().join("tool-lib");
        let resize_global_config = temp.path().join("resize-overlaybd-global.json");
        fs::write(
            &resize_global_config,
            serde_json::to_vec_pretty(&GlobalConfig::default()).unwrap(),
        )
        .unwrap();
        let script = fake_resize_tool(
            temp.path(),
            &format!(
                "pwd > '{observed}'; printf '%s\\n' \"$@\" >> '{observed}'; printf '%s\\n' \"$LD_LIBRARY_PATH\" >> '{observed}'; cp '{template_data}' upper.data; cp '{template_index}' upper.index",
                observed = observed.display(),
                template_data = template_data.display(),
                template_index = template_index.display(),
            ),
        );
        let resize_tool = ResizeToolSpec {
            binary: script,
            lib_dir: Some(tool_lib.clone()),
            timeout_secs: 60,
        };
        let resize_permit = Arc::new(tokio::sync::Mutex::new(()));
        let mut request = materialize_test_request(&cache, &source, &global_config, &runtime);
        request.requested_virtual_size = Some(2 * GIB);
        request.known_source_virtual_size = None;
        request.resize_tool = Some(&resize_tool);
        request.resize_global_config = &resize_global_config;
        request.resize_permit = Arc::clone(&resize_permit);
        let materialized = materialize_overlaybd_runtime(request).await.unwrap();

        assert_eq!(materialized.actual_virtual_size, 2 * GIB);
        let image = cache
            .get_or_create(&global_config)
            .await
            .unwrap()
            .create_image_file(&materialized.runtime_image_config_path)
            .await
            .unwrap();
        assert_eq!(image.size_bytes(), 2 * GIB);
        drop(image);
        let observed = fs::read_to_string(observed).unwrap();
        let expected_prefix = format!(
            "{}\n--config\n{}\n--size\n2\n--service_config_path\n{}\n",
            runtime.display(),
            materialized.runtime_image_config_path.display(),
            resize_global_config.display(),
        );
        let library_path = observed.strip_prefix(&expected_prefix).unwrap().trim_end();
        assert_eq!(
            std::env::split_paths(OsStr::new(library_path)).next(),
            Some(tool_lib)
        );

        for (name, script, expected) in [
            (
                "failed",
                "echo failed-out; echo failed-err >&2; exit 7",
                &["failed-out", "failed-err"][..],
            ),
            ("timed-out", "sleep 5", &["timed out after 1 seconds"][..]),
        ] {
            let tool = ResizeToolSpec {
                binary: fake_resize_tool(temp.path(), script),
                lib_dir: None,
                timeout_secs: 1,
            };
            let failed_runtime = temp.path().join(name);
            let mut request =
                materialize_test_request(&cache, &source, &global_config, &failed_runtime);
            request.requested_virtual_size = Some(2 * GIB);
            request.resize_tool = Some(&tool);
            request.resize_global_config = &resize_global_config;
            request.resize_permit = Arc::clone(&resize_permit);
            let error = materialize_overlaybd_runtime(request).await.unwrap_err();
            let message = format!("{error:#}");
            assert!(
                expected.iter().all(|text| message.contains(*text)),
                "{message}"
            );
            assert!(
                !failed_runtime.exists(),
                "failed materialization must clean its runtime"
            );
            assert!(
                resize_permit.try_lock().is_ok(),
                "failed resize must release its permit"
            );
        }
    }

    #[tokio::test]
    async fn read_only_runtime_preserves_lowers_and_omits_upper() {
        let temp = tempfile::tempdir().unwrap();
        let (cache, global_config) = test_cache(temp.path()).await;
        fs::create_dir(temp.path().join("layers")).unwrap();
        fs::write(temp.path().join("layers/base.commit"), b"immutable lower").unwrap();
        let source = write_source(
            temp.path(),
            json!({
                "lowers": [{ "file": "layers/base.commit" }],
                "upper": {"mode": "sparse", "data": "unused-upper.data"},
                "resultFile": ""
            }),
        );
        let runtime_dir = temp.path().join("runtime");

        let mut request = materialize_test_request(&cache, &source, &global_config, &runtime_dir);
        request.read_only = true;
        request.known_source_virtual_size = Some(8192);
        let runtime = materialize_overlaybd_runtime(request.clone())
            .await
            .unwrap();

        assert_eq!(runtime.actual_virtual_size, 8192);
        assert!(runtime.runtime_image_config_path.exists());
        assert!(!runtime_dir.join(RUNTIME_UPPER_DATA_FILE).exists());

        let image: Value =
            serde_json::from_slice(&fs::read(&runtime.runtime_image_config_path).unwrap()).unwrap();
        assert_eq!(image["upper"], json!({}));
        assert_eq!(image["resultFile"], json!("./result.txt"));
        let layer = runtime_dir.join(image["lowers"][0]["file"].as_str().unwrap());
        assert_eq!(fs::read(&layer).unwrap(), b"immutable lower");

        let cwd = std::env::current_dir().unwrap();
        let destination = tempfile::tempdir_in(&cwd).unwrap();
        let relative_runtime = destination
            .path()
            .strip_prefix(&cwd)
            .unwrap()
            .join("runtime");
        request.runtime_dir = &relative_runtime;
        request.source_state_strategy = SourceStateStrategy::Consume;
        let consumed = materialize_overlaybd_runtime(request).await.unwrap();
        let config_path = fs::canonicalize(consumed.runtime_image_config_path).unwrap();
        let config = load_image_config(&config_path).unwrap();
        let lower = Path::new(&config.lowers[0].file);
        assert!(lower.starts_with(config_path.parent().unwrap()));
        assert_eq!(fs::read(lower).unwrap(), b"immutable lower");
        assert!(config.upper.data.is_empty());
        fs::remove_dir_all(temp.path().join("layers")).unwrap();
        assert_eq!(fs::read(layer).unwrap(), b"immutable lower");
        assert_eq!(fs::read(lower).unwrap(), b"immutable lower");
    }

    #[tokio::test]
    async fn materialize_writable_lower_only_runtime_covers_upper_layouts() {
        for (mode, expected_mode, has_index) in [
            (UpperMode::LogStructured, "logStructured", true),
            (UpperMode::Sparse, "sparse", false),
            (UpperMode::HybridLogStructured, "hybridLogStructured", true),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (cache, global_config) = test_cache(temp.path()).await;
            let source = write_source(temp.path(), json!({ "upper": {}, "resultFile": "" }));
            let runtime_dir = temp.path().join("runtime");
            let mut request =
                materialize_test_request(&cache, &source, &global_config, &runtime_dir);
            request.runtime_upper_mode = mode;
            request.requested_virtual_size = Some(4096);
            request.known_source_virtual_size = Some(4096);
            let runtime = materialize_overlaybd_runtime(request).await.unwrap();

            assert_eq!(runtime.actual_virtual_size, 4096);
            assert!(runtime_dir.join(RUNTIME_UPPER_DATA_FILE).exists());
            assert_eq!(
                runtime_dir.join(RUNTIME_UPPER_INDEX_FILE).exists(),
                has_index
            );
            let image: Value =
                serde_json::from_slice(&fs::read(&runtime.runtime_image_config_path).unwrap())
                    .unwrap();
            assert_eq!(image["upper"]["mode"], json!(expected_mode));
            assert_eq!(image["upper"]["data"], json!("./upper.data"));
            assert_eq!(
                image["upper"]["index"],
                json!(if has_index { "./upper.index" } else { "" })
            );
            let service = cache.get_or_create(&global_config).await.unwrap();
            let image = service
                .create_image_file(&runtime.runtime_image_config_path)
                .await
                .unwrap();
            image.write_at(0, &[0x42; 4096]).await.unwrap();
            restack_runtime(
                &image,
                &runtime.runtime_image_config_path,
                &runtime_dir.join("snapshot.commit"),
                None,
            )
            .await
            .unwrap();
            drop(image);
            let reopened = service
                .create_image_file(&runtime.runtime_image_config_path)
                .await
                .unwrap();
            assert_eq!(
                reopened.read_at(0, 4096).await.unwrap().as_ref(),
                &[0x42; 4096]
            );
        }
    }

    #[tokio::test]
    async fn materialize_reuses_existing_upper_and_resolves_its_size() {
        for mode in [UpperMode::LogStructured, UpperMode::Sparse] {
            let temp = tempfile::tempdir().unwrap();
            let (cache, global_config) = test_cache(temp.path()).await;
            let data = temp.path().join("existing-upper.data");
            let index =
                (mode != UpperMode::Sparse).then(|| temp.path().join("existing-upper.index"));
            prepare_runtime_upper(&data, index.as_deref(), 8192, mode).unwrap();
            let source = write_source(
                temp.path(),
                json!({
                    "lowers": [],
                    "upper": {
                        "mode": if mode == UpperMode::Sparse { Some(mode) } else { None },
                        "data": "existing-upper.data",
                        "index": if index.is_some() { "existing-upper.index" } else { "" }
                    }
                }),
            );
            let runtime_dir = temp.path().join("runtime");
            let mut request =
                materialize_test_request(&cache, &source, &global_config, &runtime_dir);
            request.runtime_upper_mode = if mode == UpperMode::Sparse {
                UpperMode::LogStructured
            } else {
                UpperMode::Sparse
            };
            let runtime = materialize_overlaybd_runtime(request).await.unwrap();
            assert_eq!(runtime.actual_virtual_size, 8192);
            assert!(!runtime_dir.join(RUNTIME_UPPER_DATA_FILE).exists());
            assert!(!runtime_dir.join(RUNTIME_UPPER_INDEX_FILE).exists());
            let image = load_image_config(&runtime.runtime_image_config_path).unwrap();
            assert_eq!(image.upper.writable_mode(), mode);
            assert_eq!(Path::new(&image.upper.data), data);
            assert_eq!(
                image.upper.index,
                index.map(|p| p.display().to_string()).unwrap_or_default()
            );
        }
    }

    #[test]
    fn requested_virtual_size_validation_matrix() {
        assert_eq!(
            validate_requested_virtual_size(None, 2 * GIB, false).unwrap(),
            2 * GIB
        );
        assert_eq!(
            validate_requested_virtual_size(Some(2 * GIB), 2 * GIB, false).unwrap(),
            2 * GIB
        );
        assert_eq!(
            validate_requested_virtual_size(Some(3 * GIB), 2 * GIB, false).unwrap(),
            3 * GIB
        );
        assert!(validate_requested_virtual_size(Some(GIB), 2 * GIB, false).is_err());
        assert_eq!(
            validate_requested_virtual_size(Some(GIB), 2 * GIB, true).unwrap(),
            GIB
        );
        assert!(validate_requested_virtual_size(Some(0), 2 * GIB, true).is_err());
        assert!(validate_requested_virtual_size(Some(GIB + 1), 2 * GIB, true).is_err());
    }
}
