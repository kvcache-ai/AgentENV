use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use criterion::{criterion_group, criterion_main, Criterion};
use overlaybd::tools::{ConvertLayerRequest, OverlaybdTools};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use tokio::process::Command as TokioCommand;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use uuid::{Builder, Uuid};

// The real-image benchmark intentionally mirrors the production background
// `regctl image copy` producer in `src/image/oci_image.rs`, with extra timing
// fields. Keep behavior changes in sync so it continues to measure the same
// pipeline shape.

const LAYERS: usize = 6;
const DOWNLOAD_DELAY: Duration = Duration::from_millis(80);
const APPLY_DELAY: Duration = Duration::from_millis(55);
const DEFAULT_DOWNLOAD_CONCURRENCY: usize = 2;
const SAMPLES: usize = 6;
const LAYER_VIRTUAL_SIZE_GIB: u64 = 64;
const DIGEST_BUFFER_SIZE: usize = 128 * 1024;
const OCI_LAYER_BLOB_POLL_INTERVAL: Duration = Duration::from_millis(50);

fn download_concurrency() -> usize {
    std::env::var("AGENTENV_BENCH_OCI_DOWNLOAD_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DOWNLOAD_CONCURRENCY)
}

async fn synthetic_reference_no_pipeline() -> Duration {
    let start = Instant::now();
    for _ in 0..LAYERS {
        tokio::time::sleep(DOWNLOAD_DELAY).await;
    }
    for _ in 0..LAYERS {
        tokio::time::sleep(APPLY_DELAY).await;
    }
    start.elapsed()
}

async fn synthetic_pipeline() -> Duration {
    let start = Instant::now();
    let semaphore = Arc::new(Semaphore::new(download_concurrency()));
    let (tx, mut rx) = mpsc::channel::<usize>(LAYERS);

    for idx in 0..LAYERS {
        let semaphore = semaphore.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let _permit = semaphore.acquire_owned().await.expect("download permit");
            tokio::time::sleep(DOWNLOAD_DELAY).await;
            tx.send(idx).await.expect("send downloaded layer");
        });
    }
    drop(tx);

    let mut ready = [false; LAYERS];
    for expected in 0..LAYERS {
        while !ready[expected] {
            let idx = rx.recv().await.expect("download result");
            ready[idx] = true;
        }
        tokio::time::sleep(APPLY_DELAY).await;
    }

    start.elapsed()
}

fn summarize(samples: &[Duration]) -> (Duration, Duration, Duration) {
    let total: Duration = samples.iter().copied().sum();
    let mean = total / samples.len() as u32;
    let min = samples.iter().copied().min().unwrap_or_default();
    let max = samples.iter().copied().max().unwrap_or_default();
    (mean, min, max)
}

fn fmt_ms(duration: Duration) -> String {
    format!("{:.2} ms", duration.as_secs_f64() * 1000.0)
}

fn print_synthetic_table(rt: &Runtime) {
    let mut reference = Vec::with_capacity(SAMPLES);
    let mut pipeline = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        reference.push(rt.block_on(synthetic_reference_no_pipeline()));
        pipeline.push(rt.block_on(synthetic_pipeline()));
    }

    let (ref_mean, ref_min, ref_max) = summarize(&reference);
    let (pipe_mean, pipe_min, pipe_max) = summarize(&pipeline);
    let speedup = ref_mean.as_secs_f64() / pipe_mean.as_secs_f64();

    println!();
    println!(
        "OCI conversion synthetic benchmark: layers={LAYERS}, download_delay={}, apply_delay={}, download_concurrency={}",
        fmt_ms(DOWNLOAD_DELAY),
        fmt_ms(APPLY_DELAY),
        download_concurrency(),
    );
    println!("{:<24} {:>12} {:>12} {:>12}", "mode", "mean", "min", "max");
    println!(
        "{:<24} {:>12} {:>12} {:>12}",
        "reference_no_pipeline",
        fmt_ms(ref_mean),
        fmt_ms(ref_min),
        fmt_ms(ref_max)
    );
    println!(
        "{:<24} {:>12} {:>12} {:>12}",
        "pipeline",
        fmt_ms(pipe_mean),
        fmt_ms(pipe_min),
        fmt_ms(pipe_max)
    );
    println!("synthetic speedup: {:.2}x", speedup);
}

#[derive(Debug, Deserialize)]
struct BenchIndex {
    manifests: Vec<BenchManifestDescriptor>,
}

#[derive(Debug, Deserialize, Clone)]
struct BenchManifestDescriptor {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
    #[serde(default)]
    platform: Option<BenchPlatform>,
}

#[derive(Debug, Deserialize, Clone)]
struct BenchPlatform {
    architecture: Option<String>,
    os: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct BenchManifest {
    layers: Vec<BenchLayer>,
}

#[derive(Debug, Deserialize, Clone)]
struct BenchLayer {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
    size: u64,
}

#[derive(Debug)]
struct ConvertedLayer {
    path: PathBuf,
    digest: String,
    convert_elapsed: Duration,
    digest_elapsed: Duration,
}

#[derive(Debug)]
struct RealRunStats {
    total: Duration,
    copy_wall: Duration,
    consumer_wait: Duration,
    convert: Duration,
    digest: Duration,
    layers: usize,
}

fn host_arch_to_oci() -> Result<&'static str> {
    match std::env::consts::ARCH {
        "x86_64" | "amd64" => Ok("amd64"),
        "aarch64" | "arm64" => Ok("arm64"),
        arch => bail!("unsupported architecture for OCI benchmark: {arch}"),
    }
}

fn run_regctl(args: &[&str]) -> Result<Vec<u8>> {
    let output = StdCommand::new("regctl")
        .args(args)
        .output()
        .with_context(|| format!("run regctl {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "regctl {} failed with {:?}: {}",
            args.join(" "),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output.stdout)
}

fn copy_image_to_layout(image_ref: &str, layout_dir: &Path) -> Result<Duration> {
    let dest = format!("ocidir://{}:latest", layout_dir.display());
    let start = Instant::now();
    let _ = run_regctl(&["image", "copy", "--platform", "local", image_ref, &dest])?;
    Ok(start.elapsed())
}

fn read_image_manifest(layout_dir: &Path, arch: &str, os: &str) -> Result<BenchManifest> {
    let index_path = layout_dir.join("index.json");
    let index_bytes =
        fs::read(&index_path).with_context(|| format!("read {}", index_path.display()))?;
    let index: BenchIndex = serde_json::from_slice(&index_bytes)
        .with_context(|| format!("parse {}", index_path.display()))?;
    let descriptor = select_manifest_for_platform(&index.manifests, arch, os)?.clone();
    let descriptor = resolve_nested_index(descriptor, layout_dir, arch, os)?;
    let manifest_path = blob_path_for_digest(layout_dir, &descriptor.digest)?;
    let manifest_bytes = fs::read(&manifest_path)
        .with_context(|| format!("read manifest {}", manifest_path.display()))?;
    serde_json::from_slice(&manifest_bytes)
        .with_context(|| format!("parse manifest {}", manifest_path.display()))
}

fn select_manifest_for_platform<'a>(
    manifests: &'a [BenchManifestDescriptor],
    arch: &str,
    os: &str,
) -> Result<&'a BenchManifestDescriptor> {
    if manifests.len() == 1 {
        return Ok(&manifests[0]);
    }
    manifests
        .iter()
        .find(|manifest| match &manifest.platform {
            Some(platform) => {
                platform.architecture.as_deref() == Some(arch) && platform.os.as_deref() == Some(os)
            }
            None => false,
        })
        .with_context(|| format!("no OCI manifest entry matches {os}/{arch}"))
}

fn resolve_nested_index(
    descriptor: BenchManifestDescriptor,
    layout_dir: &Path,
    arch: &str,
    os: &str,
) -> Result<BenchManifestDescriptor> {
    let mut current = descriptor;
    for _ in 0..4 {
        if !current.media_type.contains("image.index")
            && !current.media_type.contains("manifest.list")
        {
            return Ok(current);
        }
        let nested_path = blob_path_for_digest(layout_dir, &current.digest)?;
        let nested_bytes = fs::read(&nested_path)
            .with_context(|| format!("read nested index {}", nested_path.display()))?;
        let nested: BenchIndex = serde_json::from_slice(&nested_bytes)
            .with_context(|| format!("parse nested index {}", nested_path.display()))?;
        current = select_manifest_for_platform(&nested.manifests, arch, os)?.clone();
    }
    bail!("nested OCI index depth exceeded maximum while reading local OCI layout")
}

fn blob_path_for_digest(layout_dir: &Path, digest: &str) -> Result<PathBuf> {
    let (algo, hex) = digest
        .split_once(':')
        .with_context(|| format!("malformed OCI digest: {digest}"))?;
    Ok(layout_dir.join("blobs").join(algo).join(hex))
}

fn uuid_from_layer_digest(digest: &str) -> Uuid {
    let digest = Sha256::digest(digest.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Builder::from_custom_bytes(bytes).into_uuid()
}

fn overlaybd_root() -> Result<PathBuf> {
    let root = std::env::var("AGENTENV_BENCH_OVERLAYBD_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("env/overlaybd"));
    fs::canonicalize(&root).with_context(|| {
        format!(
            "canonicalize overlaybd root {}; run `cargo run --bin server -- --setup-only` first or set AGENTENV_BENCH_OVERLAYBD_ROOT",
            root.display()
        )
    })
}

fn render_overlaybd_global_config(root: &Path) -> Result<PathBuf> {
    let template = std::env::var("AGENTENV_BENCH_OVERLAYBD_GLOBAL_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("env/overlaybd/overlaybd-global.json"));
    let raw = fs::read_to_string(&template)
        .with_context(|| format!("read overlaybd global config {}", template.display()))?;
    let mut config: serde_json::Value = serde_json::from_str(&raw)
        .with_context(|| format!("parse overlaybd global config {}", template.display()))?;
    let log_path = root.join("overlaybd.log");
    let cache_dir = root.join("remote-blocks-cache");
    fs::create_dir_all(&cache_dir)
        .with_context(|| format!("create overlaybd cache dir {}", cache_dir.display()))?;

    config
        .get_mut("logConfig")
        .and_then(serde_json::Value::as_object_mut)
        .context("overlaybd global config has no logConfig object")?
        .insert(
            "logPath".to_string(),
            serde_json::Value::String(log_path.to_string_lossy().into_owned()),
        );
    config
        .get_mut("cacheConfig")
        .and_then(serde_json::Value::as_object_mut)
        .context("overlaybd global config has no cacheConfig object")?
        .insert(
            "cacheDir".to_string(),
            serde_json::Value::String(cache_dir.to_string_lossy().into_owned()),
        );

    let path = root.join("overlaybd-global.json");
    let mut rendered = serde_json::to_string_pretty(&config).context("render overlaybd config")?;
    rendered.push('\n');
    fs::write(&path, rendered)
        .with_context(|| format!("write overlaybd global config {}", path.display()))?;
    Ok(path)
}

async fn file_sha256_digest(path: &Path) -> Result<String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; DIGEST_BUFFER_SIZE];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

struct ConvertOneLayerRequest<'a> {
    layer: &'a BenchLayer,
    idx: usize,
    work_dir: PathBuf,
    blob_path: PathBuf,
    lower_paths: &'a [PathBuf],
    parent_uuid: Option<Uuid>,
}

async fn convert_one_layer(
    tools: &OverlaybdTools,
    global_config: &Path,
    request: ConvertOneLayerRequest<'_>,
) -> Result<ConvertedLayer> {
    let ConvertOneLayerRequest {
        layer,
        idx,
        work_dir,
        blob_path,
        lower_paths,
        parent_uuid,
    } = request;
    fs::create_dir_all(&work_dir)
        .with_context(|| format!("create layer work dir {}", work_dir.display()))?;
    let layer_uuid = uuid_from_layer_digest(&layer.digest);
    let convert_start = Instant::now();
    let result = tools
        .convert_local_oci_layer_to_overlaybd(&ConvertLayerRequest {
            work_dir,
            input_layer_path: blob_path,
            global_config_path: global_config.to_path_buf(),
            virtual_size_gib: LAYER_VIRTUAL_SIZE_GIB,
            mkfs: idx == 0,
            lower_layers: lower_paths.to_vec(),
            uuid: Some(layer_uuid),
            parent_uuid,
        })
        .await
        .with_context(|| {
            format!(
                "convert real layer {idx} ({}, {})",
                layer.digest, layer.media_type
            )
        })?;
    let convert_elapsed = convert_start.elapsed();
    let digest_start = Instant::now();
    let digest = file_sha256_digest(&result.layer_commit).await?;
    let digest_elapsed = digest_start.elapsed();
    Ok(ConvertedLayer {
        path: result.layer_commit,
        digest,
        convert_elapsed,
        digest_elapsed,
    })
}

async fn convert_layers_from_local_blobs(
    manifest: &BenchManifest,
    blob_layout_dir: &Path,
    work_root: &Path,
    tools: &OverlaybdTools,
    global_config: &Path,
) -> Result<Vec<ConvertedLayer>> {
    let mut lower_paths = Vec::with_capacity(manifest.layers.len());
    let mut converted = Vec::with_capacity(manifest.layers.len());
    let mut parent_uuid = None;

    for (idx, layer) in manifest.layers.iter().enumerate() {
        let blob_path = blob_path_for_digest(blob_layout_dir, &layer.digest)?;
        let converted_layer = convert_one_layer(
            tools,
            global_config,
            ConvertOneLayerRequest {
                layer,
                idx,
                work_dir: work_root.join(format!("layer-{idx}")),
                blob_path,
                lower_paths: &lower_paths,
                parent_uuid,
            },
        )
        .await?;
        parent_uuid = Some(uuid_from_layer_digest(&manifest.layers[idx].digest));
        lower_paths.push(converted_layer.path.clone());
        converted.push(converted_layer);
    }

    Ok(converted)
}

struct ImageCopyProducer {
    layout_dir: PathBuf,
    start: Instant,
    child: Option<tokio::process::Child>,
    stdout_task: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
    stderr_task: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
    exit_status: Option<std::process::ExitStatus>,
    stderr: Option<String>,
    image_copy_wall: Option<Duration>,
}

impl ImageCopyProducer {
    fn start(image_ref: &str, layout_dir: PathBuf) -> Result<Self> {
        let dest = format!("ocidir://{}:latest", layout_dir.display());
        let start = Instant::now();
        let mut child = TokioCommand::new("regctl")
            .args(["image", "copy", "--platform", "local", image_ref, &dest])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn regctl image copy {image_ref} {dest}"))?;
        let stdout_task = child
            .stdout
            .take()
            .map(|stdout| tokio::spawn(read_pipe_to_end(stdout)));
        let stderr_task = child
            .stderr
            .take()
            .map(|stderr| tokio::spawn(read_pipe_to_end(stderr)));
        Ok(Self {
            layout_dir,
            start,
            child: Some(child),
            stdout_task,
            stderr_task,
            exit_status: None,
            stderr: None,
            image_copy_wall: None,
        })
    }

    async fn wait_layer_blob(&mut self, idx: usize, layer: &BenchLayer) -> Result<PathBuf> {
        let blob_path = blob_path_for_digest(&self.layout_dir, &layer.digest)?;
        loop {
            if layer_blob_is_ready(&blob_path, layer.size).await? {
                return Ok(blob_path);
            }

            if let Some(status) = self.poll_exit()? {
                if !status.success() {
                    let stderr = self.stderr_text().await;
                    bail!(
                        "regctl image copy failed with {:?} before layer {idx} ({}) was ready: {stderr}",
                        status.code(),
                        layer.digest
                    );
                }
                if layer_blob_is_ready(&blob_path, layer.size).await? {
                    return Ok(blob_path);
                }
                bail!(
                    "regctl image copy completed but layer {idx} ({}) is missing or has unexpected size at {}",
                    layer.digest,
                    blob_path.display()
                );
            }

            tokio::time::sleep(OCI_LAYER_BLOB_POLL_INTERVAL).await;
        }
    }

    fn poll_exit(&mut self) -> Result<Option<std::process::ExitStatus>> {
        if let Some(status) = self.exit_status {
            return Ok(Some(status));
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        let Some(status) = child.try_wait().context("poll regctl image copy")? else {
            return Ok(None);
        };
        self.image_copy_wall = Some(self.start.elapsed());
        self.exit_status = Some(status);
        self.child = None;
        Ok(Some(status))
    }

    async fn finish(&mut self) -> Result<Duration> {
        if let Some(elapsed) = self.image_copy_wall {
            return Ok(elapsed);
        }
        let Some(mut child) = self.child.take() else {
            return Ok(self.start.elapsed());
        };
        let status = child.wait().await.context("wait regctl image copy")?;
        let elapsed = self.start.elapsed();
        self.image_copy_wall = Some(elapsed);
        self.exit_status = Some(status);
        if status.success() {
            Ok(elapsed)
        } else {
            let stderr = self.stderr_text().await;
            bail!(
                "regctl image copy failed with {:?}: {stderr}",
                status.code()
            );
        }
    }

    async fn stderr_text(&mut self) -> String {
        if let Some(stderr) = &self.stderr {
            return stderr.clone();
        }
        let stderr = match self.stderr_task.take() {
            Some(task) => match task.await {
                Ok(Ok(bytes)) => String::from_utf8_lossy(&bytes).trim().to_string(),
                Ok(Err(err)) => format!("failed to read regctl stderr: {err}"),
                Err(err) => format!("failed to join regctl stderr reader: {err}"),
            },
            None => String::new(),
        };
        self.stderr = Some(stderr.clone());
        stderr
    }
}

impl Drop for ImageCopyProducer {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
        if let Some(task) = &self.stdout_task {
            if !task.is_finished() {
                task.abort();
            }
        }
        if let Some(task) = &self.stderr_task {
            if !task.is_finished() {
                task.abort();
            }
        }
    }
}

async fn read_pipe_to_end<R>(mut reader: R) -> std::io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await?;
    Ok(buffer)
}

async fn layer_blob_is_ready(path: &Path, expected_size: u64) -> Result<bool> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.is_file() && metadata.len() == expected_size),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => {
            Err(anyhow::Error::new(err).context(format!("stat OCI layer blob {}", path.display())))
        }
    }
}

async fn convert_layers_pipeline(
    manifest: &BenchManifest,
    image_ref: &str,
    layout_dir: PathBuf,
    work_root: &Path,
    tools: &OverlaybdTools,
    global_config: &Path,
) -> Result<(Vec<ConvertedLayer>, Duration, Duration)> {
    let mut producer = ImageCopyProducer::start(image_ref, layout_dir)?;
    let mut lower_paths = Vec::with_capacity(manifest.layers.len());
    let mut converted = Vec::with_capacity(manifest.layers.len());
    let mut parent_uuid = None;
    let mut wait_total = Duration::ZERO;

    for (idx, layer) in manifest.layers.iter().enumerate() {
        let wait_start = Instant::now();
        let blob_path = producer.wait_layer_blob(idx, layer).await?;
        wait_total += wait_start.elapsed();
        let converted_layer = convert_one_layer(
            tools,
            global_config,
            ConvertOneLayerRequest {
                layer,
                idx,
                work_dir: work_root.join(format!("layer-{idx}")),
                blob_path,
                lower_paths: &lower_paths,
                parent_uuid,
            },
        )
        .await?;
        parent_uuid = Some(uuid_from_layer_digest(&manifest.layers[idx].digest));
        lower_paths.push(converted_layer.path.clone());
        converted.push(converted_layer);
    }

    let copy_wall = producer.finish().await?;
    Ok((converted, wait_total, copy_wall))
}

fn converted_stage_timings(layers: &[ConvertedLayer]) -> (Duration, Duration) {
    (
        layers.iter().map(|layer| layer.convert_elapsed).sum(),
        layers.iter().map(|layer| layer.digest_elapsed).sum(),
    )
}

fn run_real_reference(
    rt: &Runtime,
    image_ref: &str,
    tools: &OverlaybdTools,
    global_config: &Path,
    arch: &str,
) -> Result<RealRunStats> {
    let root = tempdir().context("reference tempdir")?;
    let layout_dir = root.path().join("oci");
    let work_root = root.path().join("work");
    let total_start = Instant::now();
    let copy_wall = copy_image_to_layout(image_ref, &layout_dir)?;
    let manifest = read_image_manifest(&layout_dir, arch, "linux")?;
    let converted = rt.block_on(convert_layers_from_local_blobs(
        &manifest,
        &layout_dir,
        &work_root,
        tools,
        global_config,
    ))?;
    let total = total_start.elapsed();
    let (convert, digest) = converted_stage_timings(&converted);
    let _digest_bytes: usize = converted.iter().map(|layer| layer.digest.len()).sum();
    Ok(RealRunStats {
        total,
        copy_wall,
        consumer_wait: Duration::ZERO,
        convert,
        digest,
        layers: manifest.layers.len(),
    })
}

fn run_real_pipeline(
    rt: &Runtime,
    image_ref: &str,
    tools: &OverlaybdTools,
    global_config: &Path,
    arch: &str,
) -> Result<RealRunStats> {
    let root = tempdir().context("pipeline tempdir")?;
    let manifest_layout = root.path().join("manifest-oci");
    let _manifest_copy = copy_image_to_layout(image_ref, &manifest_layout)?;
    let manifest = read_image_manifest(&manifest_layout, arch, "linux")?;
    let pipeline_layout = root.path().join("pipeline-oci");
    let work_root = root.path().join("pipeline-work");
    let total_start = Instant::now();
    let (converted, consumer_wait, copy_wall) = rt.block_on(convert_layers_pipeline(
        &manifest,
        image_ref,
        pipeline_layout,
        &work_root,
        tools,
        global_config,
    ))?;
    let total = total_start.elapsed();
    let (convert, digest) = converted_stage_timings(&converted);
    let _digest_bytes: usize = converted.iter().map(|layer| layer.digest.len()).sum();
    Ok(RealRunStats {
        total,
        copy_wall,
        consumer_wait,
        convert,
        digest,
        layers: manifest.layers.len(),
    })
}

fn print_real_table(rt: &Runtime) {
    let Some(image_ref) = std::env::var("AGENTENV_BENCH_OCI_IMAGE").ok() else {
        println!();
        println!("Skipping real OCI conversion benchmark; set AGENTENV_BENCH_OCI_IMAGE=<image>.");
        return;
    };

    let result = (|| -> Result<()> {
        let overlaybd_root = overlaybd_root()?;
        let global_root = tempdir().context("overlaybd global tempdir")?;
        let global_config = render_overlaybd_global_config(global_root.path())?;
        let tools = OverlaybdTools::from_overlaybd_install_root(&overlaybd_root);
        let arch = host_arch_to_oci()?;

        let reference = run_real_reference(rt, &image_ref, &tools, &global_config, arch)?;
        let pipeline = run_real_pipeline(rt, &image_ref, &tools, &global_config, arch)?;
        let speedup = reference.total.as_secs_f64() / pipeline.total.as_secs_f64();

        println!();
        println!(
            "OCI conversion real-image benchmark: image={image_ref}, layers={}, cache=cold",
            reference.layers
        );
        println!(
            "{:<24} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "mode", "total", "copy_wall", "wait", "convert", "digest"
        );
        println!(
            "{:<24} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "reference_no_pipeline",
            fmt_ms(reference.total),
            fmt_ms(reference.copy_wall),
            fmt_ms(reference.consumer_wait),
            fmt_ms(reference.convert),
            fmt_ms(reference.digest)
        );
        println!(
            "{:<24} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "image_copy_pipeline",
            fmt_ms(pipeline.total),
            fmt_ms(pipeline.copy_wall),
            fmt_ms(pipeline.consumer_wait),
            fmt_ms(pipeline.convert),
            fmt_ms(pipeline.digest)
        );
        println!("real-image speedup: {:.2}x", speedup);
        Ok(())
    })();

    if let Err(err) = result {
        println!("Skipping real OCI conversion benchmark: {err:#}");
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    print_synthetic_table(&rt);
    print_real_table(&rt);

    c.bench_function("oci_conversion_synthetic_reference_no_pipeline", |b| {
        b.to_async(&rt).iter(synthetic_reference_no_pipeline)
    });
    c.bench_function("oci_conversion_synthetic_pipeline", |b| {
        b.to_async(&rt).iter(synthetic_pipeline)
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
