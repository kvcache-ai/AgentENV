use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use overlaybd::config::{load_image_config, validate_upper_config, UpperConfig, UpperMode};
use overlaybd::helper::{
    prepare_runtime_upper, relative_path, resolve_config_path, rewrite_overlaybd_lower_paths,
};
use overlaybd::image_file::ImageFile;
use overlaybd::index_file::{validate_rw_header_pair_paths, RwLayout};
use overlaybd::layer_metadata::{read_overlaybd_layer_virtual_size, resolve_local_layer_path};
use overlaybd::virtual_file::VirtualFile;
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;

use crate::protocol::ResizeToolSpec;
use crate::server::ImageServiceCache;

const GIB: u64 = 1024 * 1024 * 1024;
const RESIZE_OUTPUT_LIMIT: usize = 64 * 1024;
const RESIZE_OUTPUT_TRUNCATED_MARKER: &[u8] = b" ... (truncated)";
const RUNTIME_IMAGE_CONFIG_FILE: &str = "image.json";
const RUNTIME_UPPER_DATA_FILE: &str = "upper.data";
const RUNTIME_UPPER_INDEX_FILE: &str = "upper.index";
const RUNTIME_RESULT_FILE: &str = "result.txt";

#[derive(Debug)]
pub(crate) struct InvalidRequest(pub(crate) String);

impl std::fmt::Display for InvalidRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidRequest {}

#[derive(Debug)]
struct ClaimedRuntimeDir {
    path: PathBuf,
}

impl ClaimedRuntimeDir {
    fn claim(path: &Path) -> Result<Self> {
        fs::create_dir(path).with_context(|| {
            format!(
                "claim overlaybd runtime dir {} (it must not already exist)",
                path.display()
            )
        })?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    fn cleanup(self) {
        if let Err(err) = fs::remove_dir_all(&self.path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %self.path.display(),
                    error = %err,
                    "failed to remove daemon-owned overlaybd runtime dir during rollback"
                );
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct MaterializedOverlaybdRuntime {
    pub runtime_image_config_path: PathBuf,
    pub actual_virtual_size: u64,
    claimed_runtime_dir: ClaimedRuntimeDir,
}

impl MaterializedOverlaybdRuntime {
    pub(crate) fn rollback(self) {
        self.claimed_runtime_dir.cleanup();
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedSourceUpper {
    data_path: PathBuf,
    index_path: Option<PathBuf>,
    target_path: Option<PathBuf>,
    gzip_index_path: Option<PathBuf>,
    writable_mode: UpperMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ResolvedUpperMode {
    Existing(ResolvedSourceUpper),
    Create(UpperMode),
    Absent,
}

pub(crate) async fn materialize_overlaybd_runtime(
    request: MaterializeOverlaybdRuntimeRequest<'_>,
) -> Result<MaterializedOverlaybdRuntime> {
    let claimed_runtime_dir = ClaimedRuntimeDir::claim(request.runtime_dir)?;
    match materialize_runtime_contents(request).await {
        Ok((runtime_image_config_path, actual_virtual_size)) => Ok(MaterializedOverlaybdRuntime {
            runtime_image_config_path,
            actual_virtual_size,
            claimed_runtime_dir,
        }),
        Err(err) => {
            claimed_runtime_dir.cleanup();
            Err(err)
        }
    }
}

async fn materialize_runtime_contents(
    request: MaterializeOverlaybdRuntimeRequest<'_>,
) -> Result<(PathBuf, u64)> {
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
    } = request;

    let base_virtual_size = match known_source_virtual_size {
        Some(size) => size,
        None => {
            resolve_overlaybd_virtual_size(image_service_cache, source_image_config, global_config)
                .await
                .with_context(|| {
                    format!(
                        "resolve overlaybd base virtual size from {}",
                        source_image_config.display()
                    )
                })?
        }
    };
    anyhow::ensure!(
        base_virtual_size > 0,
        "overlaybd base virtual size must be non-zero"
    );
    let actual_virtual_size =
        validate_requested_virtual_size(requested_virtual_size, base_virtual_size, allow_shrink)
            .map_err(|err| InvalidRequest(format!("{err:#}")))?;

    let upper_mode = resolve_upper_mode(source_image_config, read_only, runtime_upper_mode)?;
    if actual_virtual_size != base_virtual_size
        && !matches!(upper_mode, ResolvedUpperMode::Create(_))
    {
        return Err(InvalidRequest(
            "resizing requires a fresh writable OverlayBD upper".to_string(),
        )
        .into());
    }
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

    let image = materialize_overlaybd_image_config(source_image_config, runtime_dir, &upper_mode)?;
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

    Ok((runtime_image_config_path, actual_virtual_size))
}

pub(crate) async fn resolve_overlaybd_virtual_size(
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
) -> Result<u64> {
    let image_cfg = load_image_config(image_config)
        .with_context(|| format!("load overlaybd image config '{}'", image_config.display()))?;

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

    let image_service = image_service_cache
        .get_or_create(global_config)
        .await
        .with_context(|| format!("load overlaybd global config '{}'", global_config.display()))?;
    let image = ImageFile::open(
        image_cfg,
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

async fn reopen_overlaybd_virtual_size(
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
) -> Result<u64> {
    let image_cfg = load_image_config(image_config)
        .with_context(|| format!("load overlaybd image config '{}'", image_config.display()))?;
    let image_service = image_service_cache
        .get_or_create(global_config)
        .await
        .with_context(|| format!("load overlaybd global config '{}'", global_config.display()))?;
    let image = ImageFile::open(
        image_cfg,
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

    let reopened = reopen_overlaybd_virtual_size(image_service_cache, image_config, global_config)
        .await
        .context("reopen resized overlaybd runtime")?;
    anyhow::ensure!(
        reopened == expected_size,
        "resized overlaybd runtime size mismatch: expected {expected_size}, got {reopened}"
    );
    Ok(())
}

fn resolve_upper_mode(
    image_config_path: &Path,
    read_only: bool,
    runtime_upper_mode: UpperMode,
) -> Result<ResolvedUpperMode> {
    if read_only {
        return Ok(ResolvedUpperMode::Absent);
    }

    let existing_upper = resolve_source_upper(image_config_path)?;
    if let Some(existing_upper) = existing_upper {
        return Ok(ResolvedUpperMode::Existing(existing_upper));
    }

    Ok(ResolvedUpperMode::Create(runtime_upper_mode))
}

fn resolve_source_upper(image_config_path: &Path) -> Result<Option<ResolvedSourceUpper>> {
    let raw = fs::read_to_string(image_config_path).with_context(|| {
        format!(
            "read overlaybd image config '{}'",
            image_config_path.display()
        )
    })?;
    let value: Value = serde_json::from_str(&raw).with_context(|| {
        format!(
            "parse overlaybd image config '{}'",
            image_config_path.display()
        )
    })?;
    let base = image_config_path.parent().unwrap_or_else(|| Path::new("."));
    let upper_value = value
        .as_object()
        .and_then(|obj| obj.get("upper").cloned())
        .unwrap_or_else(|| serde_json::json!({}));
    let upper: UpperConfig = serde_json::from_value(upper_value)
        .with_context(|| format!("parse overlaybd upper '{}'", image_config_path.display()))?;
    if !validate_upper_config(&upper)? {
        return Ok(None);
    }

    Ok(Some(ResolvedSourceUpper {
        data_path: resolve_config_path(base, &upper.data)?,
        index_path: (!upper.index.is_empty())
            .then(|| resolve_config_path(base, &upper.index))
            .transpose()?,
        target_path: (!upper.target.is_empty())
            .then(|| resolve_config_path(base, &upper.target))
            .transpose()?,
        gzip_index_path: (!upper.gzip_index.is_empty())
            .then(|| resolve_config_path(base, &upper.gzip_index))
            .transpose()?,
        writable_mode: upper.writable_mode(),
    }))
}

fn materialize_overlaybd_image_config(
    image_config_path: &Path,
    runtime_dir: &Path,
    upper_mode: &ResolvedUpperMode,
) -> Result<Value> {
    let raw = fs::read_to_string(image_config_path).with_context(|| {
        format!(
            "read overlaybd image config failed: {}",
            image_config_path.display()
        )
    })?;
    let mut value: Value = serde_json::from_str(&raw).with_context(|| {
        format!(
            "parse overlaybd image config failed: {}",
            image_config_path.display()
        )
    })?;
    let base = image_config_path.parent().unwrap_or_else(|| Path::new("."));

    rewrite_overlaybd_lower_paths(&mut value, base, runtime_dir)?;

    let upper = match upper_mode {
        ResolvedUpperMode::Absent => json!({}),
        ResolvedUpperMode::Create(mode) => {
            json!({
                "mode": mode,
                "index": match mode {
                    UpperMode::Sparse => "",
                    UpperMode::LogStructured | UpperMode::HybridLogStructured => "./upper.index",
                },
                "data": "./upper.data",
                "target": "",
                "gzipIndex": ""
            })
        }
        ResolvedUpperMode::Existing(upper) => json!({
            "mode": upper.writable_mode,
            "index": match upper.index_path.as_ref() {
                Some(path) => relative_path(runtime_dir, path)?.to_string_lossy().into_owned(),
                None => String::new(),
            },
            "data": relative_path(runtime_dir, &upper.data_path)?
                .to_string_lossy()
                .into_owned(),
            "target": match upper.target_path.as_ref() {
                Some(path) => relative_path(runtime_dir, path)?.to_string_lossy().into_owned(),
                None => String::new(),
            },
            "gzipIndex": match upper.gzip_index_path.as_ref() {
                Some(path) => relative_path(runtime_dir, path)?.to_string_lossy().into_owned(),
                None => String::new(),
            }
        }),
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
    use std::sync::Arc;

    use overlaybd::backend::local::LocalFile;
    use overlaybd::config::GlobalConfig;
    use overlaybd::index_file::{create_file_rw, LayerInfo};
    use storage_util::io_ring::spawn_io_ring_worker;

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
        let (io_ring, _join_handle) = spawn_io_ring_worker::<io_uring::squeue::Entry>(0);
        let data_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(path, io_ring.clone()).await.unwrap());
        let index_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(index_path, io_ring).await.unwrap());
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
        }
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
    async fn resolve_overlaybd_virtual_size_falls_back_to_opening_image() {
        let temp = tempfile::tempdir().unwrap();
        let (cache, global_config) = test_cache(temp.path()).await;
        let upper_path = temp.path().join("upper.data");
        prepare_runtime_upper(&upper_path, None, 12288, UpperMode::Sparse).unwrap();
        let source = write_source(
            temp.path(),
            json!({
                "lowers": [],
                "upper": {
                    "mode": "sparse",
                    "data": upper_path
                },
                "resultFile": ""
            }),
        );

        let size = resolve_overlaybd_virtual_size(&cache, &source, &global_config)
            .await
            .unwrap();

        assert_eq!(size, 12288);
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
        let (io_ring, _join_handle) = spawn_io_ring_worker::<io_uring::squeue::Entry>(0);
        let data_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&lower_path, io_ring.clone()).await.unwrap());
        let index_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&lower_index_path, io_ring).await.unwrap());
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
        validate_rw_header_pair_paths(
            &runtime.join(RUNTIME_UPPER_DATA_FILE),
            Some(&runtime.join(RUNTIME_UPPER_INDEX_FILE)),
            2 * GIB,
            RwLayout::LogStructured,
        )
        .unwrap();
        assert_eq!(
            reopen_overlaybd_virtual_size(
                &cache,
                &materialized.runtime_image_config_path,
                &global_config,
            )
            .await
            .unwrap(),
            2 * GIB
        );
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

        let timeout_tool = ResizeToolSpec {
            binary: fake_resize_tool(temp.path(), "sleep 30"),
            lib_dir: None,
            timeout_secs: 1,
        };
        let timed_out_runtime = temp.path().join("runtime-timed-out");
        let mut timed_out_request =
            materialize_test_request(&cache, &source, &global_config, &timed_out_runtime);
        timed_out_request.requested_virtual_size = Some(2 * GIB);
        timed_out_request.resize_tool = Some(&timeout_tool);
        timed_out_request.resize_global_config = &resize_global_config;
        timed_out_request.resize_permit = Arc::clone(&resize_permit);
        let err = materialize_overlaybd_runtime(timed_out_request)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timed out after 1 seconds"));

        let second_tool = fake_resize_tool(temp.path(), "exit 0");
        let second_tool = ResizeToolSpec {
            binary: second_tool,
            lib_dir: None,
            timeout_secs: 2,
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            let _guard = resize_permit.lock().await;
            run_resize_tool(
                &second_tool,
                &materialized.runtime_image_config_path,
                &resize_global_config,
                2 * GIB,
            )
            .await
        })
        .await
        .expect("resize permit must be released after timeout")
        .unwrap();
    }

    #[tokio::test]
    async fn run_resize_tool_handles_failure_and_timeout() {
        let temp = tempfile::tempdir().unwrap();
        let image = temp.path().join("image.json");
        let global = temp.path().join("global.json");
        let failure = fake_resize_tool(temp.path(), "echo failed-out; echo failed-err >&2; exit 7");
        let err = run_resize_tool(
            &ResizeToolSpec {
                binary: failure,
                lib_dir: None,
                timeout_secs: 2,
            },
            &image,
            &global,
            GIB,
        )
        .await
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("failed-out") && message.contains("failed-err"));

        let timeout = fake_resize_tool(temp.path(), "sleep 5");
        let err = run_resize_tool(
            &ResizeToolSpec {
                binary: timeout,
                lib_dir: None,
                timeout_secs: 1,
            },
            &image,
            &global,
            GIB,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("timed out after 1 seconds"));
    }

    #[tokio::test]
    async fn materialize_read_only_runtime_writes_empty_upper() {
        let temp = tempfile::tempdir().unwrap();
        let (cache, global_config) = test_cache(temp.path()).await;
        let source = write_source(
            temp.path(),
            json!({
                "lowers": [{ "file": "layers/base.commit" }],
                "upper": {},
                "resultFile": ""
            }),
        );
        let runtime_dir = temp.path().join("runtime");

        let mut request = materialize_test_request(&cache, &source, &global_config, &runtime_dir);
        request.read_only = true;
        request.known_source_virtual_size = Some(8192);
        let runtime = materialize_overlaybd_runtime(request).await.unwrap();

        assert_eq!(runtime.actual_virtual_size, 8192);
        assert!(runtime.runtime_image_config_path.exists());
        assert!(!runtime_dir.join(RUNTIME_UPPER_DATA_FILE).exists());

        let image: Value =
            serde_json::from_slice(&fs::read(&runtime.runtime_image_config_path).unwrap()).unwrap();
        assert_eq!(image["upper"], json!({}));
        assert_eq!(image["resultFile"], json!("./result.txt"));
        assert_eq!(image["lowers"][0]["file"], json!("../layers/base.commit"));
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
            let source = write_source(
                temp.path(),
                json!({ "lowers": [], "upper": {}, "resultFile": "" }),
            );
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
        }
    }

    #[tokio::test]
    async fn materialize_writable_existing_upper_reuses_source_upper() {
        let temp = tempfile::tempdir().unwrap();
        let (cache, global_config) = test_cache(temp.path()).await;
        let source = write_source(
            temp.path(),
            json!({
                "lowers": [],
                "upper": {
                    "mode": "logStructured",
                    "data": "existing-upper.data",
                    "index": "existing-upper.index"
                },
                "resultFile": ""
            }),
        );
        let runtime_dir = temp.path().join("runtime");

        let mut request = materialize_test_request(&cache, &source, &global_config, &runtime_dir);
        request.runtime_upper_mode = UpperMode::Sparse;
        request.known_source_virtual_size = Some(16384);
        let runtime = materialize_overlaybd_runtime(request).await.unwrap();

        assert_eq!(runtime.actual_virtual_size, 16384);
        assert!(!runtime_dir.join(RUNTIME_UPPER_DATA_FILE).exists());
        let image: Value =
            serde_json::from_slice(&fs::read(&runtime.runtime_image_config_path).unwrap()).unwrap();
        assert_eq!(image["upper"]["data"], json!("../existing-upper.data"));
        assert_eq!(image["upper"]["index"], json!("../existing-upper.index"));
    }

    #[tokio::test]
    async fn materialize_writable_existing_sparse_upper_reuses_source_upper() {
        let temp = tempfile::tempdir().unwrap();
        let (cache, global_config) = test_cache(temp.path()).await;
        let source = write_source(
            temp.path(),
            json!({
                "lowers": [],
                "upper": {
                    "mode": "sparse",
                    "data": "existing-upper.data"
                },
                "resultFile": ""
            }),
        );
        let runtime_dir = temp.path().join("runtime");

        let mut request = materialize_test_request(&cache, &source, &global_config, &runtime_dir);
        request.known_source_virtual_size = Some(8192);
        let runtime = materialize_overlaybd_runtime(request).await.unwrap();

        assert_eq!(runtime.actual_virtual_size, 8192);
        assert!(!runtime_dir.join(RUNTIME_UPPER_DATA_FILE).exists());
        assert!(!runtime_dir.join(RUNTIME_UPPER_INDEX_FILE).exists());
        let image: Value =
            serde_json::from_slice(&fs::read(&runtime.runtime_image_config_path).unwrap()).unwrap();
        assert_eq!(image["upper"]["mode"], json!("sparse"));
        assert_eq!(image["upper"]["data"], json!("../existing-upper.data"));
        assert_eq!(image["upper"]["index"], json!(""));
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

    #[test]
    fn cleanup_runtime_dir_removes_all_daemon_owned_contents() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_dir = temp.path().join("runtime");
        let claimed = ClaimedRuntimeDir::claim(&runtime_dir).unwrap();
        fs::write(runtime_dir.join(RUNTIME_IMAGE_CONFIG_FILE), b"runtime").unwrap();
        let tool_temp_dir = runtime_dir.join("resize-tmp");
        fs::create_dir(&tool_temp_dir).unwrap();
        fs::write(tool_temp_dir.join("scratch"), b"temporary").unwrap();

        claimed.cleanup();

        assert!(!runtime_dir.exists());
    }
}
