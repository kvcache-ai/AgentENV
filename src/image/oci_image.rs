//! OCI image → overlaybd lower layer resolution.
//!
//! Given an OCI image reference, fetches just the manifest (via
//! `regctl manifest get`) and classifies it. Two shapes are supported:
//!
//! * **Standard OCI tar layers** (tar / tar+gzip / tar+zstd). After manifest
//!   inspection, layer digests are checked against the OCI→commit indexes. If
//!   every layer already has a converted `.commit` in the content-addressed
//!   commit cache, no blobs are downloaded. Missing cache entries trigger a
//!   background `regctl image copy` into a staging OCI layout; conversion waits
//!   only for the next needed layer blob to appear, and still applies layers
//!   strictly in image order.
//!
//! * **Overlaybd-native layers** (mediaType advertises the overlaybd/zfile
//!   format). The remote blob is already a sealed overlaybd lower — no blob
//!   download runs here. We parse the image reference, compute the registry
//!   blob-URL prefix, and return [`ResolvedImage::Remote`]. The generated
//!   `image.json` references layers by `{digest, size, dir}` +
//!   `repoBlobUrl`; overlaybd's `registryfs_v2` backend fetches blocks lazily
//!   over HTTP range requests while background download can populate the
//!   content-addressed commit cache.
//!
//! Tar-wrapped overlaybd layers (standard tar mediaType plus an overlaybd
//! annotation), images that mix the two shapes, and layers with unknown
//! mediaTypes are rejected with an explicit error.
//!
//! This lower-level converter expects normalized, fully-qualified image
//! references (`host/repository:tag` or `host/repository@sha256:<digest>`).
//! User-facing shortnames are normalized by `src/image/reference.rs` before
//! reaching this module.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use overlaybd::tools::{ConvertLayerRequest, OverlaybdTools};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::{Builder, Uuid};

use super::commit_index::sanitize_filename_component;
use super::local_layer::LocalLayer;
use super::{
    env_vars_from_entries, ImageBaseContext, ImageError, ImageResolutionMetadata, ImageResult,
};
use crate::digest;

/// GOMAXPROCS ceiling applied to every spawned `regctl` process; see
/// [`regctl_command`] for the rationale.
const REGCTL_GOMAXPROCS: &str = "4";
pub(crate) const REGCTL_RETRY_ATTEMPTS: u32 = 5;
pub(crate) const REGCTL_RETRY_BASE_DELAY: Duration = Duration::from_millis(500);
const OCI_LAYER_BLOB_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_INDEX_RESOLUTION_DEPTH: usize = 4;
/// Virtual block-device size baked into every converted overlaybd layer. The
/// VM sees this as the rootfs device capacity; actual storage is only what the
/// layers contain. 64 GiB comfortably covers common base images.
const LAYER_VIRTUAL_SIZE_GIB: u64 = 64;
const OCI_INDEX_MEDIA_TYPES: &[&str] = &[
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
];

/// Outcome of resolving an OCI image reference for overlaybd consumption.
///
/// `Local` carries locally-cached `.commit` files produced from the
/// standard-OCI conversion path. `Remote` carries registry descriptors — no
/// blob download runs here; the overlaybd runtime's `registryfs_v2` backend
/// fetches layer bytes on demand at sandbox startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedImage {
    Local(Vec<LocalLayer>),
    Remote {
        repo_blob_url: String,
        layers: Vec<RemoteLayer>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteLayer {
    pub(crate) digest: String,
    pub(crate) size: u64,
    pub(crate) dir: PathBuf,
}

#[derive(Debug)]
pub(crate) struct FetchedManifest {
    pub(super) manifest_digest: String,
    pub(super) selected_image_ref: String,
    pub(super) repository_scope: Option<String>,
    format: ImageFormat,
    manifest: OciManifest,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OverlaybdConversionEnv<'a> {
    pub(crate) install_root: &'a Path,
    pub(crate) global_config: &'a Path,
    pub(crate) converter_id: &'a str,
    pub(crate) regctl_binary: &'a Path,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LayerConversionKey {
    pub(crate) source_layer_digest: String,
    pub(crate) converter_id: String,
    pub(crate) virtual_size_gib: u64,
    pub(crate) mkfs: bool,
    pub(crate) parent_commit_digest: Option<String>,
    pub(crate) expected_layer_uuid: Uuid,
}

#[async_trait]
pub(crate) trait ImageConversion: Send + Sync {
    fn remote_layer_dir_hint(&self, layer_digest: &str) -> PathBuf;

    /// Create a fresh temporary staging dir for OCI-pull intermediates,
    /// placed on the same filesystem as the overlaybd commit cache so
    /// `layer.commit` can be hard-linked into the cache (avoids EXDEV).
    fn create_temp_staging_dir(&self) -> Result<tempfile::TempDir>;

    async fn lookup_converted_layer(
        &mut self,
        key: &LayerConversionKey,
    ) -> Result<Option<LocalLayer>>;

    async fn store_converted_layer(
        &mut self,
        key: &LayerConversionKey,
        temp_commit: &Path,
    ) -> Result<LocalLayer>;
}

pub(crate) async fn fetch_oci_manifest(
    regctl_binary: &Path,
    image_ref: &str,
    host_arch: &str,
) -> ImageResult<FetchedManifest> {
    ensure_regctl_binary(regctl_binary)?;
    let oci_arch = host_arch_to_oci(host_arch)?;

    fetch_manifest(regctl_binary, image_ref, oci_arch, "linux").await
}

/// Fetch the image config blob by `config_digest` and parse runtime metadata
/// out of it. The blob is verified against the digest after download.
pub(crate) async fn fetch_oci_image_config_metadata(
    regctl_binary: &Path,
    image_ref: &str,
    config_digest: &str,
) -> ImageResult<ImageResolutionMetadata> {
    let repository = image_ref_repository(image_ref)?;
    let output = run_regctl(
        regctl_binary,
        &["blob", "get", repository.as_str(), config_digest],
    )
    .await?;
    let bytes = output.stdout;
    verify_blob_digest(config_digest, &bytes)
        .with_context(|| format!("verify image config blob for {image_ref}"))?;
    let raw = std::str::from_utf8(&bytes)
        .with_context(|| format!("image config blob for {image_ref} is not UTF-8"))?;
    let metadata = parse_oci_image_config(raw)
        .with_context(|| format!("parse OCI image config for {image_ref}"))?;
    let ctx = &metadata.base_context;
    debug!(
        image = image_ref,
        config_digest,
        user = ctx.user.as_deref().unwrap_or("(none)"),
        workdir = ctx.workdir.as_deref().unwrap_or("(none)"),
        entrypoint = ?ctx.entrypoint,
        cmd = ?ctx.cmd,
        exposed_ports = ctx.exposed_ports.len(),
        volumes = ctx.volumes.len(),
        env_vars = ctx.env_vars.len(),
        labels = ctx.labels.len(),
        "resolved OCI image runtime metadata",
    );
    Ok(metadata)
}

impl FetchedManifest {
    pub(crate) fn format(&self) -> ImageFormat {
        self.format
    }

    /// Digest of the image config blob declared by the selected manifest.
    pub(crate) fn config_digest(&self) -> &str {
        &self.manifest.config.digest
    }
}

pub(crate) async fn convert_fetched_oci_image_to_overlaybd(
    image_ref: &str,
    fetched: FetchedManifest,
    conversion: OverlaybdConversionEnv<'_>,
    sink: &mut dyn ImageConversion,
    host_arch: &str,
) -> ImageResult<ResolvedImage> {
    // `fetch_oci_manifest` resolves the selected manifest for this host arch.
    // Keep the validation here too because the background copy uses
    // `--platform local`; unsupported host architecture should fail before
    // starting conversion work.
    host_arch_to_oci(host_arch)?;
    let manifest = &fetched.manifest;

    match fetched.format {
        ImageFormat::OverlaybdNative => {
            let (host, repository) = parse_image_ref(image_ref)?;
            let repo_blob_url = format!("https://{host}/v2/{repository}/blobs");
            info!(
                image = image_ref,
                layers = manifest.layers.len(),
                repo_blob_url = %repo_blob_url,
                "source image is overlaybd-native; skipping blob download — runtime registryfs_v2 will fetch on demand",
            );
            let layers = manifest
                .layers
                .iter()
                .map(|l| RemoteLayer {
                    digest: l.digest.clone(),
                    size: l.size,
                    dir: sink.remote_layer_dir_hint(&l.digest),
                })
                .collect();
            Ok(ResolvedImage::Remote {
                repo_blob_url,
                layers,
            })
        }
        ImageFormat::StandardOci => {
            // This only runs when the digest-qualified image.json cache is
            // missing or stale. Cache-hit descriptors computed here are
            // persisted into the regenerated image.json, so later sandbox
            // starts reuse that config instead of hashing these commits again.
            if let Some(lowers) =
                cached_standard_oci_lowers(manifest, conversion, &mut *sink).await?
            {
                info!(
                    image = image_ref,
                    layers = lowers.len(),
                    "source image is standard OCI and all overlaybd layer commits are cached; skipping blob download"
                );
                return Ok(ResolvedImage::Local(lowers));
            }

            info!(
                image = image_ref,
                layers = manifest.layers.len(),
                "source image is standard OCI; starting background image copy because at least one converted layer is missing"
            );
            let work = sink.create_temp_staging_dir()?;
            let selected_image_ref = fetched.selected_image_ref.as_str();
            let converter = OverlaybdLayerConverter {
                tools: OverlaybdTools::from_overlaybd_install_root(conversion.install_root),
                global_config_path: conversion.global_config.to_path_buf(),
                virtual_size_gib: LAYER_VIRTUAL_SIZE_GIB,
            };
            let lowers = convert_standard_oci_layers_pipeline(
                conversion.regctl_binary,
                manifest,
                selected_image_ref,
                work.path(),
                conversion,
                &mut *sink,
                &converter,
            )
            .await?;
            Ok(ResolvedImage::Local(lowers))
        }
    }
}

struct OverlaybdLayerConverter {
    tools: OverlaybdTools,
    global_config_path: PathBuf,
    virtual_size_gib: u64,
}

struct LayerConversionRequest<'a> {
    idx: usize,
    layer: &'a OciLayerDescriptor,
    blob_path: PathBuf,
    layer_work: PathBuf,
    mkfs: bool,
    // Each layer conversion needs the complete lower stack that existed before
    // this layer is appended, so callers pass an owned snapshot.
    lower_paths: Vec<PathBuf>,
    layer_uuid: Uuid,
    parent_uuid: Option<Uuid>,
}

impl OverlaybdLayerConverter {
    async fn convert_layer(&self, request: LayerConversionRequest<'_>) -> Result<PathBuf> {
        let LayerConversionRequest {
            idx,
            layer,
            blob_path,
            layer_work,
            mkfs,
            lower_paths,
            layer_uuid,
            parent_uuid,
        } = request;
        debug!(
            idx,
            digest = %layer.digest,
            size = layer.size,
            media_type = %layer.media_type,
            "converting OCI layer to overlaybd"
        );
        std::fs::create_dir_all(&layer_work)
            .with_context(|| format!("create layer work dir {}", layer_work.display()))?;

        let result = self
            .tools
            .convert_local_oci_layer_to_overlaybd(&ConvertLayerRequest {
                work_dir: layer_work,
                input_layer_path: blob_path,
                global_config_path: self.global_config_path.clone(),
                virtual_size_gib: self.virtual_size_gib,
                mkfs,
                lower_layers: lower_paths,
                uuid: Some(layer_uuid),
                parent_uuid,
            })
            .await
            .with_context(|| format!("convert layer {} ({})", idx, layer.digest))?;
        Ok(result.layer_commit)
    }
}

async fn convert_standard_oci_layers_pipeline(
    regctl_binary: &Path,
    manifest: &OciManifest,
    image_ref: &str,
    work_root: &Path,
    conversion: OverlaybdConversionEnv<'_>,
    sink: &mut dyn ImageConversion,
    converter: &OverlaybdLayerConverter,
) -> Result<Vec<LocalLayer>> {
    let layout_dir = work_root.join("oci");
    let mut producer = RegctlImageCopyProducer::start(regctl_binary, image_ref, layout_dir).await?;
    let result = convert_standard_oci_layers_pipeline_inner(
        manifest,
        image_ref,
        work_root,
        conversion,
        sink,
        converter,
        &mut producer,
    )
    .await;
    match result {
        Ok(lowers) => {
            if let Err(err) = producer.abort().await {
                warn!(
                    error = %err,
                    "failed to cleanly abort background regctl image copy after successful conversion"
                );
            }
            Ok(lowers)
        }
        Err(err) => {
            if let Err(abort_err) = producer.abort().await {
                warn!(
                    error = %abort_err,
                    "failed to cleanly abort background regctl image copy after conversion failure"
                );
            }
            Err(err)
        }
    }
}

async fn convert_standard_oci_layers_pipeline_inner(
    manifest: &OciManifest,
    image_ref: &str,
    work_root: &Path,
    conversion: OverlaybdConversionEnv<'_>,
    sink: &mut dyn ImageConversion,
    converter: &OverlaybdLayerConverter,
    producer: &mut RegctlImageCopyProducer,
) -> Result<Vec<LocalLayer>> {
    let mut lower_paths: Vec<PathBuf> = Vec::with_capacity(manifest.layers.len());
    let mut lowers = Vec::with_capacity(manifest.layers.len());
    let mut parent_uuid = None;
    let mut parent_commit_digest: Option<String> = None;
    for (idx, layer) in manifest.layers.iter().enumerate() {
        let layer_uuid = uuid_from_layer_digest(&layer.digest);
        let key = layer_conversion_key(
            layer,
            conversion.converter_id,
            idx == 0,
            parent_commit_digest.as_deref(),
            layer_uuid,
        );
        if let Some(cached) = sink.lookup_converted_layer(&key).await? {
            parent_commit_digest = Some(cached.digest.clone());
            parent_uuid = Some(layer_uuid);
            lower_paths.push(cached.path.clone());
            lowers.push(cached);
            continue;
        }

        let blob_path = producer
            .wait_layer_blob(idx, layer)
            .await
            .with_context(|| format!("wait for copied layer {} ({})", idx, layer.digest))?;
        let layer_work = work_root.join(format!("layer-{idx}"));

        let layer_commit = converter
            .convert_layer(LayerConversionRequest {
                idx,
                layer,
                blob_path,
                layer_work,
                mkfs: idx == 0,
                lower_paths: lower_paths.clone(),
                layer_uuid,
                parent_uuid,
            })
            .await
            .with_context(|| format!("convert layer {} ({}) for {image_ref}", idx, layer.digest))?;

        let local_layer = sink.store_converted_layer(&key, &layer_commit).await?;
        parent_commit_digest = Some(local_layer.digest.clone());
        lower_paths.push(local_layer.path.clone());
        lowers.push(local_layer);
        parent_uuid = Some(layer_uuid);
    }

    Ok(lowers)
}

struct RegctlImageCopyProducer {
    regctl_binary: PathBuf,
    image_ref: String,
    layout_dir: PathBuf,
    child: Option<tokio::process::Child>,
    stdout_task: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
    stderr_task: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
    exit_status: Option<std::process::ExitStatus>,
    stderr: Option<String>,
    attempts_started: u32,
    backoff: Duration,
}

impl RegctlImageCopyProducer {
    async fn start(
        regctl_binary: &Path,
        image_ref: &str,
        layout_dir: PathBuf,
    ) -> ImageResult<Self> {
        ensure_regctl_binary(regctl_binary)?;
        let mut producer = Self {
            regctl_binary: regctl_binary.to_path_buf(),
            image_ref: image_ref.to_string(),
            layout_dir,
            child: None,
            stdout_task: None,
            stderr_task: None,
            exit_status: None,
            stderr: None,
            attempts_started: 0,
            backoff: REGCTL_RETRY_BASE_DELAY,
        };
        producer.spawn_child()?;
        Ok(producer)
    }

    fn spawn_child(&mut self) -> ImageResult<()> {
        self.abort_pipe_tasks();
        self.exit_status = None;
        self.stderr = None;
        let dest = format!("ocidir://{}:latest", self.layout_dir.display());
        let mut command = regctl_command(&self.regctl_binary);
        command
            .args([
                "image",
                "copy",
                "--platform",
                "local",
                &self.image_ref,
                &dest,
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().context("spawn regctl image copy")?;
        let stdout_task = child
            .stdout
            .take()
            .map(|stdout| tokio::spawn(read_pipe_to_end(stdout)));
        let stderr_task = child
            .stderr
            .take()
            .map(|stderr| tokio::spawn(read_pipe_to_end(stderr)));
        self.child = Some(child);
        self.stdout_task = stdout_task;
        self.stderr_task = stderr_task;
        self.attempts_started += 1;
        Ok(())
    }

    async fn wait_layer_blob(
        &mut self,
        idx: usize,
        layer: &OciLayerDescriptor,
    ) -> ImageResult<PathBuf> {
        let blob_path = blob_path_for_digest(&self.layout_dir, &layer.digest)?;
        loop {
            if layer_blob_is_ready(&blob_path, layer.size).await? {
                debug!(
                    idx,
                    digest = %layer.digest,
                    path = %blob_path.display(),
                    "OCI layer blob is ready in background image-copy layout"
                );
                return Ok(blob_path);
            }

            if let Some(status) = self.poll_exit().await? {
                if !status.success() {
                    let stderr = self.stderr_text().await;
                    if regctl_stderr_is_not_found(&stderr) {
                        return Err(ImageError::NotFound {
                            reason: format!(
                                "regctl image copy reported the OCI resource does not exist: {stderr}",
                            ),
                        });
                    }
                    if self.attempts_started < REGCTL_RETRY_ATTEMPTS {
                        warn!(
                            attempt = self.attempts_started,
                            idx,
                            digest = %layer.digest,
                            error = %stderr,
                            "regctl image copy failed before needed layer was ready; retrying"
                        );
                        tokio::time::sleep(self.backoff).await;
                        self.backoff *= 2;
                        self.spawn_child()?;
                        continue;
                    }
                    return Err(ImageError::Other(anyhow!(
                        "regctl image copy failed after {REGCTL_RETRY_ATTEMPTS} attempts with {:?}: {stderr}",
                        status.code(),
                    )));
                }
                if layer_blob_is_ready(&blob_path, layer.size).await? {
                    return Ok(blob_path);
                }
                return Err(ImageError::Other(anyhow!(
                    "regctl image copy completed but layer {idx} ({}) is missing or has unexpected size at {}",
                    layer.digest,
                    blob_path.display()
                )));
            }

            tokio::time::sleep(OCI_LAYER_BLOB_POLL_INTERVAL).await;
        }
    }

    async fn poll_exit(&mut self) -> Result<Option<std::process::ExitStatus>> {
        if let Some(status) = self.exit_status {
            return Ok(Some(status));
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        let Some(status) = child.try_wait().context("poll regctl image copy")? else {
            return Ok(None);
        };
        self.exit_status = Some(status);
        self.child = None;
        Ok(Some(status))
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

    async fn abort(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let status = child
                .wait()
                .await
                .context("wait killed regctl image copy")?;
            self.exit_status = Some(status);
        }
        if let Some(task) = self.stdout_task.take() {
            match task.await {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    debug!(error = %err, "failed to drain regctl image copy stdout during abort");
                }
                Err(err) if err.is_cancelled() => {}
                Err(err) => return Err(anyhow::Error::new(err).context("join regctl stdout task")),
            }
        }
        if let Some(task) = self.stderr_task.take() {
            match task.await {
                Ok(Ok(bytes)) => {
                    self.stderr
                        .get_or_insert_with(|| String::from_utf8_lossy(&bytes).trim().to_string());
                }
                Ok(Err(err)) => {
                    debug!(error = %err, "failed to drain regctl image copy stderr during abort");
                }
                Err(err) if err.is_cancelled() => {}
                Err(err) => return Err(anyhow::Error::new(err).context("join regctl stderr task")),
            }
        }
        Ok(())
    }

    fn abort_pipe_tasks(&mut self) {
        if let Some(task) = self.stdout_task.take() {
            if !task.is_finished() {
                task.abort();
            }
        }
        if let Some(task) = self.stderr_task.take() {
            if !task.is_finished() {
                task.abort();
            }
        }
    }
}

impl Drop for RegctlImageCopyProducer {
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

// ---- manifest fetch ----

/// Fetch the image manifest for a given (arch, os) without downloading any
/// layer blobs. If the registry serves a manifest list/index, walks into the
/// matching sub-manifest via a digest-qualified reference and inspects it
/// again; `MAX_INDEX_RESOLUTION_DEPTH` caps pathological nesting.
async fn fetch_manifest(
    regctl_binary: &Path,
    image_ref: &str,
    arch: &str,
    os: &str,
) -> ImageResult<FetchedManifest> {
    let mut current_ref = image_ref.to_string();
    for _ in 0..MAX_INDEX_RESOLUTION_DEPTH {
        let output = run_regctl(
            regctl_binary,
            &["manifest", "get", "--format", "raw-body", &current_ref],
        )
        .await?;
        let raw = String::from_utf8(output.stdout).with_context(|| {
            format!("regctl manifest get output is not UTF-8 for {current_ref}")
        })?;
        let value: serde_json::Value = serde_json::from_str(&raw)
            .with_context(|| format!("parse manifest JSON for {current_ref}"))?;

        let media_type = value.get("mediaType").and_then(|v| v.as_str());
        let has_index_manifests = value.get("manifests").and_then(|v| v.as_array()).is_some();
        let is_index = media_type.is_some_and(|mt| OCI_INDEX_MEDIA_TYPES.contains(&mt))
            || (media_type.is_none() && has_index_manifests);

        if !is_index {
            // This hashes only the raw manifest JSON (normally KB-scale) to
            // derive the immutable OCI manifest digest, not any layer/blob data.
            let manifest_digest = digest::sha256_digest(raw.as_bytes());
            let manifest: OciManifest = serde_json::from_value(value)
                .with_context(|| format!("parse manifest JSON for {current_ref}"))?;
            let format = classify_manifest(&manifest)?;
            let selected_image_ref = image_ref_with_digest(&current_ref, &manifest_digest)?;
            let repository_scope = match format {
                ImageFormat::StandardOci => None,
                ImageFormat::OverlaybdNative => {
                    let (host, repository) = parse_image_ref(&selected_image_ref)?;
                    Some(repository_cache_key(&host, &repository))
                }
            };
            return Ok(FetchedManifest {
                manifest_digest,
                selected_image_ref,
                repository_scope,
                format,
                manifest,
            });
        }

        let index: OciIndex = serde_json::from_value(value)
            .with_context(|| format!("parse OCI index for {current_ref}"))?;
        let descriptor = select_manifest_for_platform(&index.manifests, arch, os)?;
        current_ref = image_ref_with_digest(&current_ref, &descriptor.digest)?;
    }

    Err(ImageError::Other(anyhow!(
        "OCI manifest index nesting exceeded maximum depth of {MAX_INDEX_RESOLUTION_DEPTH} \
         while resolving {image_ref}"
    )))
}

// ---- regctl ----
//
// regctl preserves manifests byte-for-byte and accepts Docker schema2
// manifests that carry OCI descriptor mediaTypes, which skopeo rejects
// (`unsupported docker v2s2 media type`).

/// Return `true` when a failed `regctl` invocation's stderr indicates the
/// registry responded with HTTP 404 (e.g. `MANIFEST_UNKNOWN` / `BLOB_UNKNOWN`).
/// regctl renders these as `... not found [http 404]: {...}`. Network/DNS
/// failures do not carry the `[http 404]` marker and must keep their 5xx
/// classification.
pub(crate) fn regctl_stderr_is_not_found(stderr: &str) -> bool {
    stderr.contains("[http 404]")
}

/// Build a [`Command`] for `regctl` with a bounded Go runtime.
///
/// `regctl` is a Go binary; the Go runtime defaults GOMAXPROCS to the number
/// of host CPUs, spawning roughly that many OS threads per process. Because
/// AgentENV forks a short-lived `regctl` per image operation and can do so at
/// high concurrency, on many-core hosts the default fans out into a large
/// number of threads and can exhaust the process/PID limit. regctl's work is
/// network/IO-bound rather than CPU-bound, so a small GOMAXPROCS caps the
/// thread footprint without affecting throughput.
///
/// All `regctl` invocations must be constructed through this so the cap is
/// applied uniformly.
pub(crate) fn regctl_command(binary: impl AsRef<std::ffi::OsStr>) -> Command {
    let mut command = Command::new(binary);
    command.env("GOMAXPROCS", REGCTL_GOMAXPROCS);
    command
}

/// Run a regctl invocation, retrying failures with exponential backoff to
/// absorb transient registry errors.
pub(crate) async fn run_regctl(
    regctl_binary: &Path,
    args: &[&str],
) -> ImageResult<std::process::Output> {
    ensure_regctl_binary(regctl_binary)?;
    let mut backoff = REGCTL_RETRY_BASE_DELAY;
    let mut last_stderr = String::new();
    for attempt in 1..=REGCTL_RETRY_ATTEMPTS {
        let output = regctl_command(regctl_binary)
            .args(args)
            .output()
            .await
            .context("spawn regctl")?;
        if output.status.success() {
            return Ok(output);
        }
        last_stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        // A 404 is not transient: the manifest/blob/image simply does not
        // exist. Fail fast with a typed error so the API layer can return a
        // 4xx instead of a 5xx, and skip the retry/backoff budget.
        if regctl_stderr_is_not_found(&last_stderr) {
            return Err(ImageError::NotFound {
                reason: format!(
                    "regctl {} reported the OCI resource does not exist: {last_stderr}",
                    args.join(" ")
                ),
            });
        }
        if attempt < REGCTL_RETRY_ATTEMPTS {
            warn!(attempt, args = ?args, error = %last_stderr, "regctl failed; retrying");
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
    }
    Err(ImageError::Other(anyhow!(
        "regctl {} failed after {REGCTL_RETRY_ATTEMPTS} attempts: {last_stderr}",
        args.join(" ")
    )))
}

pub(crate) fn ensure_regctl_binary(path: &Path) -> ImageResult<()> {
    if path
        .metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
    {
        Ok(())
    } else {
        Err(ImageError::Other(anyhow!(
            "regctl is required for OCI registry access: {}",
            path.display()
        )))
    }
}

/// Verify that `bytes` hash to `expected_digest`. Only sha256 digests are
/// supported; anything else fails closed.
fn verify_blob_digest(expected_digest: &str, bytes: &[u8]) -> Result<()> {
    let Some(expected_hex) = expected_digest.strip_prefix("sha256:") else {
        bail!("unsupported digest algorithm (expected sha256): {expected_digest}");
    };
    let actual_hex = digest::sha256_hex(bytes);
    if !actual_hex.eq_ignore_ascii_case(expected_hex) {
        bail!(
            "blob digest mismatch: manifest declares {expected_digest}, \
             fetched bytes hash to sha256:{actual_hex}"
        );
    }
    Ok(())
}

fn parse_oci_image_config(raw: &str) -> Result<ImageResolutionMetadata> {
    let document: OciImageConfigDocument =
        serde_json::from_str(raw).context("parse OCI image config JSON")?;
    let config = document.config.unwrap_or_default();

    // Extract just the "config" key from the top-level document as raw JSON,
    // excluding top-level fields like "created", "architecture", etc.
    let raw_config = serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|mut doc| doc.get_mut("config").map(serde_json::Value::take));

    Ok(ImageResolutionMetadata {
        base_context: ImageBaseContext::new(
            env_vars_from_entries(&config.env.unwrap_or_default()),
            config.working_dir,
            config.user,
            config
                .exposed_ports
                .unwrap_or_default()
                .into_keys()
                .collect(),
            config.entrypoint,
            config.cmd,
            config.volumes.unwrap_or_default().into_keys().collect(),
            config.labels.unwrap_or_default(),
        ),
        raw_config,
    })
}

// ---- OCI layout parsing ----

#[derive(Debug, Default, Deserialize)]
struct OciImageConfigDocument {
    #[serde(default)]
    config: Option<OciContainerConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct OciContainerConfig {
    #[serde(rename = "Env", default)]
    env: Option<Vec<String>>,
    #[serde(rename = "WorkingDir", default)]
    working_dir: Option<String>,
    #[serde(rename = "User", default)]
    user: Option<String>,
    #[serde(rename = "ExposedPorts", default)]
    exposed_ports: Option<HashMap<String, serde_json::Value>>,
    #[serde(rename = "Entrypoint", default)]
    entrypoint: Option<Vec<String>>,
    #[serde(rename = "Cmd", default)]
    cmd: Option<Vec<String>>,
    #[serde(rename = "Volumes", default)]
    volumes: Option<HashMap<String, serde_json::Value>>,
    #[serde(rename = "Labels", default)]
    labels: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct OciIndex {
    manifests: Vec<OciManifestDescriptor>,
}

#[derive(Debug, Deserialize, Clone)]
struct OciManifestDescriptor {
    #[serde(rename = "mediaType")]
    #[allow(dead_code)]
    media_type: String,
    digest: String,
    #[serde(default)]
    platform: Option<OciPlatform>,
}

#[derive(Debug, Deserialize, Clone)]
struct OciPlatform {
    architecture: Option<String>,
    os: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OciManifest {
    /// OCI 1.1 `artifactType`; accelerated-container-image uses it to mark
    /// overlaybd images (`...overlaybd.native.v1+json` / `...overlaybd.turbo.v1+json`).
    #[serde(rename = "artifactType", default)]
    artifact_type: Option<String>,
    config: OciManifestConfigDescriptor,
    layers: Vec<OciLayerDescriptor>,
}

#[derive(Debug, Deserialize, Clone)]
struct OciManifestConfigDescriptor {
    digest: String,
}

#[derive(Debug, Deserialize, Clone)]
struct OciLayerDescriptor {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
    size: u64,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}

fn select_manifest_for_platform<'a>(
    manifests: &'a [OciManifestDescriptor],
    arch: &str,
    os: &str,
) -> Result<&'a OciManifestDescriptor> {
    if manifests.len() == 1 {
        return Ok(&manifests[0]);
    }

    let matched = manifests.iter().find(|m| match &m.platform {
        Some(p) => p.architecture.as_deref() == Some(arch) && p.os.as_deref() == Some(os),
        None => false,
    });

    match matched {
        Some(m) => Ok(m),
        None => {
            let available: Vec<String> = manifests
                .iter()
                .map(|m| match &m.platform {
                    Some(p) => format!(
                        "{}/{}",
                        p.os.as_deref().unwrap_or("?"),
                        p.architecture.as_deref().unwrap_or("?"),
                    ),
                    None => "no-platform".into(),
                })
                .collect();
            bail!(
                "no OCI manifest entry matches {os}/{arch}; available: [{}]",
                available.join(", ")
            )
        }
    }
}

// ---- layer / manifest classification ----

/// artifactType published by accelerated-container-image for turbo-OCI
/// (`obdconv --turboOCI`) images. Turbo layer blobs are tar archives that
/// only carry filesystem/decompression *indexes* (`ext4.fs.meta`,
/// `gzip.meta`, ...); the actual data lives in the original OCI layers
/// referenced by `turbo-oci/target-digest` annotations. AgentENV's overlaybd
/// runtime does not implement the turbo-OCI read path.
const OVERLAYBD_TURBO_ARTIFACT_TYPE: &str = "application/vnd.containerd.overlaybd.turbo.v1+json";

const OVERLAYBD_TURBO_ANNOTATION_PREFIX: &str = "containerd.io/snapshot/overlaybd/turbo-oci/";
const OVERLAYBD_VERSION_ANNOTATION: &str = "containerd.io/snapshot/overlaybd/version";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayerClass {
    /// Standard OCI tar layer (tar / tar+gzip / tar+zstd).
    StandardTar,
    /// Overlaybd-native layer: the blob is already a sealed overlaybd lower.
    OverlaybdNative,
    /// Turbo-OCI layer: the blob is only an index over the original OCI
    /// layer. Not supported by AgentENV's overlaybd runtime.
    OverlaybdTurbo,
    /// Tar-wrapped overlaybd layer (tar+gzip blob with the overlaybd blob
    /// nested inside). Not yet supported.
    OverlaybdTarWrapped,
    /// Unknown mediaType — fail closed to avoid silently producing garbage.
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImageFormat {
    StandardOci,
    OverlaybdNative,
}

impl std::fmt::Display for ImageFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StandardOci => f.write_str("standard_oci"),
            Self::OverlaybdNative => f.write_str("overlaybd_native"),
        }
    }
}

fn classify_manifest(manifest: &OciManifest) -> ImageResult<ImageFormat> {
    if manifest.layers.is_empty() {
        return Err(ImageError::Other(anyhow!("OCI manifest has no layers")));
    }

    let turbo_unsupported = |detail: String| ImageError::UnsupportedImage {
        reason: format!(
            "image is an overlaybd turbo-OCI image ({detail}); turbo-OCI layers carry \
             only index metadata and are not supported by AgentENV"
        ),
    };

    // Manifest-level turbo-OCI detection: the artifactType alone is
    // authoritative even before inspecting layers.
    if manifest.artifact_type.as_deref() == Some(OVERLAYBD_TURBO_ARTIFACT_TYPE) {
        return Err(turbo_unsupported(format!(
            "artifactType={OVERLAYBD_TURBO_ARTIFACT_TYPE}"
        )));
    }

    let classes: Vec<LayerClass> = manifest.layers.iter().map(classify_layer).collect();

    for (idx, class) in classes.iter().enumerate() {
        match class {
            LayerClass::OverlaybdTurbo => {
                let layer = &manifest.layers[idx];
                return Err(turbo_unsupported(format!(
                    "layer {idx} ({}) carries turbo-OCI annotations: {:?}",
                    layer.digest, layer.annotations,
                )));
            }
            LayerClass::OverlaybdTarWrapped => {
                let layer = &manifest.layers[idx];
                return Err(ImageError::UnsupportedImage {
                    reason: format!(
                        "layer {idx} ({}) is a tar-wrapped overlaybd layer \
                         (mediaType={}, annotations={:?}); this shape is not yet \
                         supported",
                        layer.digest, layer.media_type, layer.annotations,
                    ),
                });
            }
            LayerClass::Unknown => {
                let layer = &manifest.layers[idx];
                return Err(ImageError::UnsupportedImage {
                    reason: format!(
                        "layer {idx} ({}) has an unsupported mediaType: {}",
                        layer.digest, layer.media_type,
                    ),
                });
            }
            _ => {}
        }
    }

    let all_standard = classes.iter().all(|c| *c == LayerClass::StandardTar);
    let all_overlaybd = classes.iter().all(|c| *c == LayerClass::OverlaybdNative);

    if all_standard {
        Ok(ImageFormat::StandardOci)
    } else if all_overlaybd {
        Ok(ImageFormat::OverlaybdNative)
    } else {
        Err(ImageError::UnsupportedImage {
            reason: "image has a mix of standard OCI and overlaybd-native layers; \
                     only homogeneous images are supported"
                .to_string(),
        })
    }
}

fn classify_layer(layer: &OciLayerDescriptor) -> LayerClass {
    // Turbo-OCI detection wins over everything else: turbo layers also carry
    // `blob-digest == layer digest`, so they would otherwise be misclassified
    // as mediaType-lying native and fail at runtime with confusing short-read
    // errors from the registryfs zfile probe.
    let is_turbo = layer
        .annotations
        .keys()
        .any(|key| key.starts_with(OVERLAYBD_TURBO_ANNOTATION_PREFIX))
        || layer
            .annotations
            .get(OVERLAYBD_VERSION_ANNOTATION)
            .is_some_and(|version| version.contains("turbo"));
    if is_turbo {
        return LayerClass::OverlaybdTurbo;
    }

    // Native mediaType wins: the blob is directly a sealed overlaybd lower,
    // even if the publisher also emitted a legacy overlaybd annotation.
    if is_overlaybd_native_media_type(&layer.media_type) {
        return LayerClass::OverlaybdNative;
    }

    if !is_standard_tar_media_type(&layer.media_type) {
        return LayerClass::Unknown;
    }

    // A `containerd.io/snapshot/overlaybd/blob-digest` annotation signals an
    // accelerated-container-image publication. Two conventions coexist:
    //
    //   * **mediaType-lying native**: the wire blob IS the overlaybd lower;
    //     the tar mediaType is only present for OCI-client compatibility.
    //     Publishers emit `blob-digest` equal to the layer's own digest to
    //     confirm "these bytes ARE the overlaybd content".
    //   * **tar-wrapped**: the wire blob is a tar archive containing an inner
    //     overlaybd blob. `blob-digest` then names the *inner* blob, which is
    //     not the same as the outer layer digest.
    //
    // We handle the first case as native (registryfs_v2 fetches the layer
    // digest and gets overlaybd bytes unchanged). The second still needs local
    // extraction and stays rejected.
    if let Some(annotated_digest) = layer
        .annotations
        .get("containerd.io/snapshot/overlaybd/blob-digest")
    {
        if annotated_digest == &layer.digest {
            return LayerClass::OverlaybdNative;
        }
        return LayerClass::OverlaybdTarWrapped;
    }

    LayerClass::StandardTar
}

fn is_overlaybd_native_media_type(media_type: &str) -> bool {
    // The upstream project uses a family of overlaybd-specific mediaTypes; match
    // on stable overlaybd/zfile tokens at component boundaries so unrelated
    // mediaTypes that happen to contain the substring aren't misclassified.
    const OVERLAYBD_TOKEN_PREFIXES: &[&str] = &[
        ".overlaybd",
        "/overlaybd",
        "+overlaybd",
        ".zfile",
        "/zfile",
        "+zfile",
    ];
    OVERLAYBD_TOKEN_PREFIXES
        .iter()
        .any(|token| media_type.contains(token))
}

fn is_standard_tar_media_type(media_type: &str) -> bool {
    matches!(
        media_type,
        "application/vnd.oci.image.layer.v1.tar"
            | "application/vnd.oci.image.layer.v1.tar+gzip"
            | "application/vnd.oci.image.layer.v1.tar+zstd"
            | "application/vnd.docker.image.rootfs.diff.tar"
            | "application/vnd.docker.image.rootfs.diff.tar.gzip"
    )
}

// ---- image reference parsing ----

/// Parse a fully-qualified OCI image reference into `(registry_host, repository)`.
///
/// The registry host is translated for wire-level use: `docker.io` and
/// `index.docker.io` both map to `registry-1.docker.io`. Shortnames
/// (references without a recognizable host segment) are rejected.
fn parse_image_ref(image_ref: &str) -> Result<(String, String)> {
    let (host_part, rest) = image_ref
        .split_once('/')
        .with_context(|| format!("image reference must include a registry host: {image_ref}"))?;

    // A host must look like a DNS name or carry an explicit port (:N). This
    // protects the low-level registry URL builder; user-facing shortnames are
    // normalized before conversion.
    let looks_like_host =
        host_part.contains('.') || host_part.contains(':') || host_part == "localhost";
    if !looks_like_host {
        bail!(
            "image reference must be fully qualified with a registry host \
             (e.g. `docker.io/library/debian:tag`), got: {image_ref}"
        );
    }

    // Strip the trailing tag or digest off the leaf component (not the host,
    // whose port colon must survive).
    let last_slash = rest.rfind('/').map(|i| i + 1).unwrap_or(0);
    let (repo_head, leaf) = rest.split_at(last_slash);
    let leaf_stripped = leaf
        .find('@')
        .or_else(|| leaf.find(':'))
        .map(|i| &leaf[..i])
        .unwrap_or(leaf);
    if leaf_stripped.is_empty() {
        bail!("image reference has an empty repository component: {image_ref}");
    }
    let repository = format!("{repo_head}{leaf_stripped}");

    let host = match host_part {
        "docker.io" | "index.docker.io" => "registry-1.docker.io".to_string(),
        other => other.to_string(),
    };

    Ok((host, repository))
}

fn repository_cache_key(host: &str, repository: &str) -> String {
    sanitize_filename_component(&format!("{host}/{repository}"), usize::MAX)
}

/// Build a new image reference pointing at the given manifest digest. Preserves
/// the host + repository and replaces any existing tag/digest.
pub(crate) fn image_ref_with_digest(image_ref: &str, digest: &str) -> Result<String> {
    // Find the leaf component so we can strip its tag/digest without touching
    // a possible port colon inside the host.
    let last_slash = image_ref.rfind('/').with_context(|| {
        format!("image reference is missing host/repository separator: {image_ref}")
    })?;
    let (head, leaf) = image_ref.split_at(last_slash + 1);
    let leaf_stripped = leaf
        .find('@')
        .or_else(|| leaf.find(':'))
        .map(|i| &leaf[..i])
        .unwrap_or(leaf);
    Ok(format!("{head}{leaf_stripped}@{digest}"))
}

/// Strip the tag/digest off an image reference, leaving `host/repository`.
/// Keeps the host as written — regctl does its own docker.io aliasing.
fn image_ref_repository(image_ref: &str) -> Result<String> {
    let last_slash = image_ref.rfind('/').with_context(|| {
        format!("image reference is missing host/repository separator: {image_ref}")
    })?;
    let (head, leaf) = image_ref.split_at(last_slash + 1);
    let leaf_stripped = leaf
        .find('@')
        .or_else(|| leaf.find(':'))
        .map(|i| &leaf[..i])
        .unwrap_or(leaf);
    if leaf_stripped.is_empty() {
        bail!("image reference has an empty repository component: {image_ref}");
    }
    Ok(format!("{head}{leaf_stripped}"))
}

// ---- helpers ----

fn host_arch_to_oci(host_arch: &str) -> Result<&'static str> {
    match host_arch {
        "x86_64" | "amd64" => Ok("amd64"),
        "aarch64" | "arm64" => Ok("arm64"),
        other => bail!("unsupported architecture for OCI pull: {other}"),
    }
}

fn blob_path_for_digest(layout_dir: &Path, digest: &str) -> Result<PathBuf> {
    let (algo, hex) = digest
        .split_once(':')
        .with_context(|| format!("malformed OCI digest: {digest}"))?;
    Ok(layout_dir.join("blobs").join(algo).join(hex))
}

fn uuid_from_layer_digest(digest: &str) -> Uuid {
    let digest = digest::sha256_bytes(digest.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Builder::from_custom_bytes(bytes).into_uuid()
}

/// Build the conversion-cache key for one OCI layer in its stacking context.
fn layer_conversion_key(
    layer: &OciLayerDescriptor,
    converter_id: &str,
    mkfs: bool,
    parent_commit_digest: Option<&str>,
    expected_layer_uuid: Uuid,
) -> LayerConversionKey {
    LayerConversionKey {
        source_layer_digest: layer.digest.clone(),
        converter_id: converter_id.to_string(),
        virtual_size_gib: LAYER_VIRTUAL_SIZE_GIB,
        mkfs,
        parent_commit_digest: parent_commit_digest.map(str::to_string),
        expected_layer_uuid,
    }
}

async fn cached_standard_oci_lowers(
    manifest: &OciManifest,
    conversion: OverlaybdConversionEnv<'_>,
    sink: &mut dyn ImageConversion,
) -> Result<Option<Vec<LocalLayer>>> {
    let mut lowers = Vec::with_capacity(manifest.layers.len());
    let mut parent_commit_digest: Option<String> = None;
    for (idx, layer) in manifest.layers.iter().enumerate() {
        let key = layer_conversion_key(
            layer,
            conversion.converter_id,
            idx == 0,
            parent_commit_digest.as_deref(),
            uuid_from_layer_digest(&layer.digest),
        );
        let Some(cached) = sink.lookup_converted_layer(&key).await? else {
            return Ok(None);
        };
        parent_commit_digest = Some(cached.digest.clone());
        lowers.push(cached);
    }

    Ok(Some(lowers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;
    use uuid::{Variant, Version};

    fn annotations(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn host_arch_to_oci_maps_both_naming_conventions() {
        assert_eq!(host_arch_to_oci("x86_64").unwrap(), "amd64");
        assert_eq!(host_arch_to_oci("amd64").unwrap(), "amd64");
        assert_eq!(host_arch_to_oci("aarch64").unwrap(), "arm64");
        assert_eq!(host_arch_to_oci("arm64").unwrap(), "arm64");
        assert!(host_arch_to_oci("riscv64").is_err());
    }

    #[test]
    fn regctl_stderr_is_not_found_detects_http_404() {
        assert!(regctl_stderr_is_not_found(
            "failed to get manifest reg/repo:tag: request failed: not found [http 404]: {\"errors\":[{\"code\":\"MANIFEST_UNKNOWN\"}]}"
        ));
        assert!(regctl_stderr_is_not_found(
            "failed to get blob ...: request failed: not found [http 404]: {\"errors\":[{\"code\":\"BLOB_UNKNOWN\"}]}"
        ));
    }

    #[test]
    fn regctl_stderr_is_not_found_ignores_network_errors() {
        assert!(!regctl_stderr_is_not_found(
            "failed to get manifest no-such-host/foo/bar:latest: Get \"https://no-such-host/v2/...\": dial tcp: lookup no-such-host: no such host"
        ));
        assert!(!regctl_stderr_is_not_found(
            "request failed: unauthorized [http 401]"
        ));
    }

    #[test]
    fn blob_path_for_digest_splits_on_colon() {
        let layout = Path::new("/tmp/oci");
        let got = blob_path_for_digest(layout, "sha256:abc123").unwrap();
        assert_eq!(got, PathBuf::from("/tmp/oci/blobs/sha256/abc123"));
    }

    #[test]
    fn blob_path_for_digest_rejects_malformed_digest() {
        let layout = Path::new("/tmp/oci");
        assert!(blob_path_for_digest(layout, "no-colon-here").is_err());
    }

    #[test]
    fn parse_oci_image_config_extracts_base_context() {
        let raw = r#"{
            "architecture": "amd64",
            "os": "linux",
            "config": {
                "Env": [
                    "NODE_ENV=production",
                    "PATH=/custom/bin:/usr/bin",
                    "FIRST=1",
                    "INVALID",
                    "=missing",
                    "FIRST=2",
                    "TOKEN=value=with=equals"
                ],
                "WorkingDir": "/workspace"
            }
        }"#;

        let metadata = parse_oci_image_config(raw).expect("parse image config");

        assert_eq!(
            metadata
                .base_context
                .env_vars
                .get("NODE_ENV")
                .map(String::as_str),
            Some("production")
        );
        assert_eq!(
            metadata
                .base_context
                .env_vars
                .get("PATH")
                .map(String::as_str),
            Some("/custom/bin:/usr/bin")
        );
        assert_eq!(
            metadata
                .base_context
                .env_vars
                .get("FIRST")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            metadata
                .base_context
                .env_vars
                .get("TOKEN")
                .map(String::as_str),
            Some("value=with=equals")
        );
        assert!(!metadata.base_context.env_vars.contains_key("INVALID"));
        assert!(!metadata.base_context.env_vars.contains_key(""));
        assert_eq!(metadata.base_context.workdir.as_deref(), Some("/workspace"));
    }

    #[test]
    fn parse_oci_image_config_extracts_all_runtime_fields() {
        // Uses the canonical example from https://github.com/opencontainers/image-spec/blob/main/config.md
        let raw = r#"{
            "architecture": "amd64",
            "os": "linux",
            "config": {
                "User": "alice",
                "ExposedPorts": {
                    "8080/tcp": {},
                    "443/tcp": {}
                },
                "Env": [
                    "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                    "FOO=oci_is_a",
                    "BAR=well_written_spec"
                ],
                "Entrypoint": ["/bin/my-app-binary"],
                "Cmd": ["--foreground", "--config", "/etc/my-app.d/default.cfg"],
                "Volumes": {
                    "/var/job-result-data": {},
                    "/var/log/my-app-logs": {}
                },
                "WorkingDir": "/home/alice",
                "Labels": {
                    "com.example.project.git.url": "https://example.com/project.git",
                    "com.example.project.git.commit": "45a939b2999782a3f005621a8d0f29aa387e1d6b"
                }
            }
        }"#;

        let ctx = parse_oci_image_config(raw)
            .expect("parse image config")
            .base_context;

        assert_eq!(ctx.user.as_deref(), Some("alice"));
        assert_eq!(ctx.workdir.as_deref(), Some("/home/alice"));

        assert_eq!(
            ctx.env_vars.get("FOO").map(String::as_str),
            Some("oci_is_a")
        );
        assert_eq!(
            ctx.env_vars.get("BAR").map(String::as_str),
            Some("well_written_spec")
        );

        assert_eq!(
            ctx.entrypoint.as_deref(),
            Some(["/bin/my-app-binary".to_string()].as_slice())
        );
        assert_eq!(
            ctx.cmd.as_deref(),
            Some(
                [
                    "--foreground".to_string(),
                    "--config".to_string(),
                    "/etc/my-app.d/default.cfg".to_string(),
                ]
                .as_slice()
            )
        );

        let mut ports = ctx.exposed_ports.clone();
        ports.sort();
        assert_eq!(ports, vec!["443/tcp", "8080/tcp"]);

        let mut vols = ctx.volumes.clone();
        vols.sort();
        assert_eq!(vols, vec!["/var/job-result-data", "/var/log/my-app-logs"]);

        assert_eq!(
            ctx.labels
                .get("com.example.project.git.url")
                .map(String::as_str),
            Some("https://example.com/project.git")
        );
        assert_eq!(
            ctx.labels
                .get("com.example.project.git.commit")
                .map(String::as_str),
            Some("45a939b2999782a3f005621a8d0f29aa387e1d6b")
        );
    }

    #[test]
    fn parse_oci_image_config_tolerates_missing_config() {
        let metadata = parse_oci_image_config(r#"{"architecture":"amd64"}"#)
            .expect("missing config should default");

        assert_eq!(metadata.base_context, ImageBaseContext::default());
        // No "config" key in the document, so raw_config is None.
        assert!(metadata.raw_config.is_none());
    }

    #[test]
    fn parse_oci_image_config_tolerates_null_map_fields() {
        let raw = r#"{
            "architecture": "arm64",
            "os": "linux",
            "config": {
                "Env": ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],
                "Cmd": ["/bin/bash"],
                "ExposedPorts": null,
                "Volumes": null,
                "Labels": null
            }
        }"#;

        let ctx = parse_oci_image_config(raw)
            .expect("null map fields should default")
            .base_context;

        assert_eq!(
            ctx.env_vars.get("PATH").map(String::as_str),
            Some("/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
        );
        assert_eq!(
            ctx.cmd.as_deref(),
            Some(["/bin/bash".to_string()].as_slice())
        );
        assert!(ctx.exposed_ports.is_empty());
        assert!(ctx.volumes.is_empty());
        assert!(ctx.labels.is_empty());
    }

    #[test]
    fn uuid_from_layer_digest_is_stable_and_digest_specific() {
        let digests = ["sha256:aaa", "sha256:aab", "sha256:aac", "sha256:aad"];
        let uuid = uuid_from_layer_digest(digests[0]);
        assert_eq!(uuid, uuid_from_layer_digest(digests[0]));
        assert_ne!(
            uuid_from_layer_digest("sha256:aaa"),
            uuid_from_layer_digest("sha256:bbb")
        );
        for digest in digests {
            let uuid = uuid_from_layer_digest(digest);
            assert_eq!(uuid.get_variant(), Variant::RFC4122);
            assert_eq!(uuid.get_version(), Some(Version::Custom));
        }
    }

    #[test]
    fn classify_layer_flags_overlaybd_native_media_types() {
        assert_eq!(
            classify_layer(&layer(
                "application/vnd.containerd.overlaybd.image.layer.v1.zfile",
                "sha256:aaa",
            )),
            LayerClass::OverlaybdNative,
        );
        assert_eq!(
            classify_layer(&layer(
                "application/vnd.containerd.overlaybd.v1.tar",
                "sha256:aaa"
            )),
            LayerClass::OverlaybdNative,
        );
    }

    #[test]
    fn classify_layer_recognizes_standard_tar_types() {
        for mt in [
            "application/vnd.oci.image.layer.v1.tar",
            "application/vnd.oci.image.layer.v1.tar+gzip",
            "application/vnd.oci.image.layer.v1.tar+zstd",
            "application/vnd.docker.image.rootfs.diff.tar",
            "application/vnd.docker.image.rootfs.diff.tar.gzip",
        ] {
            assert_eq!(
                classify_layer(&layer(mt, "sha256:aaa")),
                LayerClass::StandardTar,
                "expected StandardTar for mediaType {mt}"
            );
        }
    }

    #[test]
    fn classify_layer_treats_tar_with_self_referential_overlaybd_annotation_as_native() {
        // Aliyun ACR / accelerated-container-image publishes overlaybd blobs
        // with a `tar` mediaType for OCI-client compatibility and annotates
        // the layer with `blob-digest == layer.digest` to signal that the
        // wire bytes are already overlaybd. Those layers must take the
        // native passthrough — not the tar-wrapped rejection.
        let digest = "sha256:4cafc55d878a0f1f2fb497369b138272eda40a8c66b7c922f0693b89cff6b0f0";
        let mut l = layer("application/vnd.oci.image.layer.v1.tar", digest);
        l.annotations = annotations(&[
            ("containerd.io/snapshot/overlaybd/blob-digest", digest),
            ("containerd.io/snapshot/overlaybd/blob-size", "47760418"),
        ]);
        assert_eq!(classify_layer(&l), LayerClass::OverlaybdNative);
    }

    #[test]
    fn classify_layer_flags_tar_wrapped_overlaybd_when_annotation_points_at_inner_blob() {
        let mut l = layer(
            "application/vnd.oci.image.layer.v1.tar+gzip",
            "sha256:outerbeef",
        );
        l.annotations = annotations(&[(
            "containerd.io/snapshot/overlaybd/blob-digest",
            "sha256:innerbeef",
        )]);
        assert_eq!(classify_layer(&l), LayerClass::OverlaybdTarWrapped);
    }

    #[test]
    fn classify_layer_returns_unknown_for_unrecognized_media_types() {
        assert_eq!(
            classify_layer(&layer("application/vnd.something.exotic.v1", "sha256:aaa")),
            LayerClass::Unknown,
        );
    }

    #[test]
    fn select_manifest_for_platform_returns_only_entry_when_single() {
        let only = OciManifestDescriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".into(),
            digest: "sha256:aaa".into(),
            platform: None,
        };
        let picked =
            select_manifest_for_platform(std::slice::from_ref(&only), "amd64", "linux").unwrap();
        assert_eq!(picked.digest, "sha256:aaa");
    }

    #[test]
    fn select_manifest_for_platform_picks_matching_arch() {
        let entries = vec![
            OciManifestDescriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:amd".into(),
                platform: Some(OciPlatform {
                    architecture: Some("amd64".into()),
                    os: Some("linux".into()),
                }),
            },
            OciManifestDescriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:arm".into(),
                platform: Some(OciPlatform {
                    architecture: Some("arm64".into()),
                    os: Some("linux".into()),
                }),
            },
        ];
        assert_eq!(
            select_manifest_for_platform(&entries, "arm64", "linux")
                .unwrap()
                .digest,
            "sha256:arm",
        );
    }

    #[test]
    fn select_manifest_for_platform_ignores_platformless_entries_when_multi_arch() {
        let entries = vec![
            OciManifestDescriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:no-platform".into(),
                platform: None,
            },
            OciManifestDescriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:amd".into(),
                platform: Some(OciPlatform {
                    architecture: Some("amd64".into()),
                    os: Some("linux".into()),
                }),
            },
        ];
        assert_eq!(
            select_manifest_for_platform(&entries, "amd64", "linux")
                .unwrap()
                .digest,
            "sha256:amd",
        );
    }

    #[test]
    fn select_manifest_for_platform_errors_with_available_list_when_no_match() {
        let entries = vec![
            OciManifestDescriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:amd".into(),
                platform: Some(OciPlatform {
                    architecture: Some("amd64".into()),
                    os: Some("linux".into()),
                }),
            },
            OciManifestDescriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                digest: "sha256:arm".into(),
                platform: Some(OciPlatform {
                    architecture: Some("arm64".into()),
                    os: Some("linux".into()),
                }),
            },
        ];
        let err = select_manifest_for_platform(&entries, "riscv64", "linux").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("linux/amd64"),
            "error missing arch list: {msg}"
        );
        assert!(
            msg.contains("linux/arm64"),
            "error missing arch list: {msg}"
        );
    }

    fn layer(media_type: &str, digest: &str) -> OciLayerDescriptor {
        OciLayerDescriptor {
            media_type: media_type.into(),
            digest: digest.into(),
            size: 100,
            annotations: BTreeMap::new(),
        }
    }

    fn manifest_config() -> OciManifestConfigDescriptor {
        OciManifestConfigDescriptor {
            digest: "sha256:config".into(),
        }
    }

    fn read_image_manifest(layout_dir: &Path, arch: &str, os: &str) -> Result<OciManifest> {
        let index_path = layout_dir.join("index.json");
        let index_bytes =
            std::fs::read(&index_path).with_context(|| format!("read {}", index_path.display()))?;
        let index: OciIndex = serde_json::from_slice(&index_bytes)
            .with_context(|| format!("parse {}", index_path.display()))?;

        if index.manifests.is_empty() {
            bail!("OCI index at {} has no manifests", index_path.display());
        }

        let descriptor = select_manifest_for_platform(&index.manifests, arch, os)?.clone();
        let descriptor = resolve_nested_index(descriptor, layout_dir, arch, os)?;

        let manifest_path = blob_path_for_digest(layout_dir, &descriptor.digest)?;
        let manifest_bytes = std::fs::read(&manifest_path)
            .with_context(|| format!("read manifest {}", manifest_path.display()))?;
        let manifest: OciManifest = serde_json::from_slice(&manifest_bytes)
            .with_context(|| format!("parse manifest {}", manifest_path.display()))?;
        Ok(manifest)
    }

    fn resolve_nested_index(
        descriptor: OciManifestDescriptor,
        layout_dir: &Path,
        arch: &str,
        os: &str,
    ) -> Result<OciManifestDescriptor> {
        let mut current = descriptor;
        for _ in 0..MAX_INDEX_RESOLUTION_DEPTH {
            if !OCI_INDEX_MEDIA_TYPES.contains(&current.media_type.as_str()) {
                return Ok(current);
            }

            let nested_path = blob_path_for_digest(layout_dir, &current.digest)?;
            let nested_bytes = std::fs::read(&nested_path)
                .with_context(|| format!("read nested index {}", nested_path.display()))?;
            let nested: OciIndex = serde_json::from_slice(&nested_bytes)
                .with_context(|| format!("parse nested index {}", nested_path.display()))?;

            current = select_manifest_for_platform(&nested.manifests, arch, os)?.clone();
        }

        bail!(
            "nested OCI index depth exceeded maximum of {MAX_INDEX_RESOLUTION_DEPTH} while reading local OCI layout"
        )
    }

    #[test]
    fn classify_manifest_recognizes_standard_oci_image() {
        let manifest = OciManifest {
            artifact_type: None,
            config: manifest_config(),
            layers: vec![
                layer("application/vnd.oci.image.layer.v1.tar+gzip", "sha256:a"),
                layer("application/vnd.oci.image.layer.v1.tar+zstd", "sha256:b"),
                // Docker and OCI tar mediaTypes may be mixed in one manifest.
                layer(
                    "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "sha256:c",
                ),
            ],
        };
        assert_eq!(
            classify_manifest(&manifest).unwrap(),
            ImageFormat::StandardOci
        );
    }

    #[test]
    fn classify_manifest_recognizes_overlaybd_native_image() {
        let manifest = OciManifest {
            artifact_type: None,
            config: manifest_config(),
            layers: vec![
                layer(
                    "application/vnd.containerd.overlaybd.image.layer.v1.zfile",
                    "sha256:a",
                ),
                layer(
                    "application/vnd.containerd.overlaybd.image.layer.v1.zfile",
                    "sha256:b",
                ),
            ],
        };
        assert_eq!(
            classify_manifest(&manifest).unwrap(),
            ImageFormat::OverlaybdNative
        );
    }

    #[test]
    fn classify_manifest_rejects_mixed_standard_and_overlaybd_layers() {
        let manifest = OciManifest {
            artifact_type: None,
            config: manifest_config(),
            layers: vec![
                layer("application/vnd.oci.image.layer.v1.tar+gzip", "sha256:a"),
                layer(
                    "application/vnd.containerd.overlaybd.image.layer.v1.zfile",
                    "sha256:b",
                ),
            ],
        };
        let err = classify_manifest(&manifest).unwrap_err();
        assert!(
            format!("{err}").contains("mix of standard OCI and overlaybd"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn classify_manifest_rejects_tar_wrapped_overlaybd() {
        let mut l = layer("application/vnd.oci.image.layer.v1.tar+gzip", "sha256:a");
        l.annotations = annotations(&[(
            "containerd.io/snapshot/overlaybd/blob-digest",
            "sha256:deadbeef",
        )]);
        let manifest = OciManifest {
            artifact_type: None,
            config: manifest_config(),
            layers: vec![l],
        };
        let err = classify_manifest(&manifest).unwrap_err();
        assert!(
            format!("{err}").contains("tar-wrapped overlaybd"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn classify_manifest_recognizes_acr_mediatype_lying_overlaybd_image() {
        // ACR / accelerated-container-image publishes each layer with a tar
        // mediaType plus a blob-digest annotation equal to the layer's own
        // digest. The entire image should classify as OverlaybdNative and go
        // through the registryfs_v2 passthrough.
        let mut l0 = layer(
            "application/vnd.oci.image.layer.v1.tar",
            "sha256:4cafc55d878a0f1f2fb497369b138272eda40a8c66b7c922f0693b89cff6b0f0",
        );
        l0.annotations = annotations(&[
            (
                "containerd.io/snapshot/overlaybd/blob-digest",
                "sha256:4cafc55d878a0f1f2fb497369b138272eda40a8c66b7c922f0693b89cff6b0f0",
            ),
            ("containerd.io/snapshot/overlaybd/blob-size", "47760418"),
        ]);
        let mut l1 = layer("application/vnd.oci.image.layer.v1.tar", "sha256:bbbbbbbb");
        l1.annotations = annotations(&[(
            "containerd.io/snapshot/overlaybd/blob-digest",
            "sha256:bbbbbbbb",
        )]);
        let manifest = OciManifest {
            artifact_type: None,
            config: manifest_config(),
            layers: vec![l0, l1],
        };
        assert_eq!(
            classify_manifest(&manifest).unwrap(),
            ImageFormat::OverlaybdNative
        );
    }

    #[test]
    fn classify_manifest_rejects_unknown_media_type() {
        let manifest = OciManifest {
            artifact_type: None,
            config: manifest_config(),
            layers: vec![layer("application/vnd.something.odd", "sha256:a")],
        };
        let err = classify_manifest(&manifest).unwrap_err();
        assert!(
            format!("{err}").contains("unsupported mediaType"),
            "unexpected error: {err}"
        );
        assert!(err.is_user_error(), "unsupported mediaType must map to 4xx");
    }

    #[test]
    fn classify_manifest_rejects_turbo_oci_by_artifact_type() {
        // Even without inspecting layers, the OCI 1.1 artifactType marks the
        // image as turbo-OCI.
        let mut l = layer("application/vnd.oci.image.layer.v1.tar", "sha256:a");
        l.annotations =
            annotations(&[("containerd.io/snapshot/overlaybd/blob-digest", "sha256:a")]);
        let manifest = OciManifest {
            artifact_type: Some(OVERLAYBD_TURBO_ARTIFACT_TYPE.to_string()),
            config: manifest_config(),
            layers: vec![l],
        };
        let err = classify_manifest(&manifest).unwrap_err();
        assert!(
            matches!(err, ImageError::UnsupportedImage { .. }),
            "expected UnsupportedImage, got: {err:?}"
        );
        assert!(err.is_user_error(), "turbo-OCI must map to 4xx");
        assert!(
            format!("{err}").contains("turbo-OCI"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn classify_manifest_rejects_turbo_oci_by_layer_annotations() {
        // Turbo layers carry `blob-digest == layer digest` just like
        // mediaType-lying native layers; the turbo-oci/* annotations must win
        // so the image is rejected at resolve time instead of failing deep in
        // the runtime with a short-read zfile probe error.
        let mut turbo = layer("application/vnd.oci.image.layer.v1.tar", "sha256:a");
        turbo.annotations = annotations(&[
            ("containerd.io/snapshot/overlaybd/blob-digest", "sha256:a"),
            (
                "containerd.io/snapshot/overlaybd/turbo-oci/target-digest",
                "sha256:original",
            ),
            (
                "containerd.io/snapshot/overlaybd/version",
                "0.1.0-turbo.ociv1",
            ),
        ]);
        let mut native = layer("application/vnd.oci.image.layer.v1.tar", "sha256:b");
        native.annotations = annotations(&[
            ("containerd.io/snapshot/overlaybd/blob-digest", "sha256:b"),
            ("containerd.io/snapshot/overlaybd/version", "0.1.0"),
        ]);
        let manifest = OciManifest {
            artifact_type: None,
            config: manifest_config(),
            layers: vec![turbo, native],
        };
        let err = classify_manifest(&manifest).unwrap_err();
        assert!(
            matches!(err, ImageError::UnsupportedImage { .. }),
            "expected UnsupportedImage, got: {err:?}"
        );
        assert!(err.is_user_error(), "turbo-OCI must map to 4xx");
        assert!(
            format!("{err}").contains("turbo-OCI"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn classify_manifest_rejects_turbo_oci_by_version_annotation_only() {
        let mut l = layer("application/vnd.oci.image.layer.v1.tar", "sha256:a");
        l.annotations = annotations(&[
            ("containerd.io/snapshot/overlaybd/blob-digest", "sha256:a"),
            (
                "containerd.io/snapshot/overlaybd/version",
                "0.1.0-turbo.ociv1",
            ),
        ]);
        let manifest = OciManifest {
            artifact_type: None,
            config: manifest_config(),
            layers: vec![l],
        };
        let err = classify_manifest(&manifest).unwrap_err();
        assert!(matches!(err, ImageError::UnsupportedImage { .. }));
    }

    #[test]
    fn verify_blob_digest_checks_sha256() {
        let bytes = b"config-blob";
        verify_blob_digest(&digest::sha256_digest(bytes), bytes).expect("matching digest");
        assert!(verify_blob_digest(&digest::sha256_digest(b"other"), bytes).is_err());
        assert!(verify_blob_digest("sha512:abc", bytes).is_err());
    }

    #[test]
    fn image_ref_repository_strips_tag_and_digest() {
        assert_eq!(
            image_ref_repository("ghcr.io/org/repo:tag").unwrap(),
            "ghcr.io/org/repo"
        );
        assert_eq!(
            image_ref_repository("cr.example.com/team/app@sha256:abc").unwrap(),
            "cr.example.com/team/app"
        );
        assert_eq!(
            image_ref_repository("registry.internal:5000/team/app:v1").unwrap(),
            "registry.internal:5000/team/app"
        );
        assert!(image_ref_repository("no-repository-segment").is_err());
    }

    #[test]
    fn read_image_manifest_handles_single_arch_layout() {
        let tmp = tempdir().unwrap();
        let layout = tmp.path();

        let manifest_json = serde_json::json!({
            "schemaVersion": 2,
            "config": {"digest": "sha256:config"},
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:layer0",
                    "size": 123,
                }
            ]
        });
        let manifest_bytes = serde_json::to_vec(&manifest_json).unwrap();
        fs::create_dir_all(layout.join("blobs/sha256")).unwrap();
        fs::write(layout.join("blobs/sha256/manifestA"), &manifest_bytes).unwrap();

        let index_json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:manifestA",
                    "size": manifest_bytes.len() as u64,
                }
            ]
        });
        fs::write(
            layout.join("index.json"),
            serde_json::to_vec(&index_json).unwrap(),
        )
        .unwrap();

        let manifest = read_image_manifest(layout, "amd64", "linux").unwrap();
        assert_eq!(manifest.layers.len(), 1);
        assert_eq!(manifest.layers[0].digest, "sha256:layer0");
    }

    #[test]
    fn read_image_manifest_picks_matching_arch_from_multi_arch_layout() {
        let tmp = tempdir().unwrap();
        let layout = tmp.path();
        fs::create_dir_all(layout.join("blobs/sha256")).unwrap();

        let amd_manifest = serde_json::json!({
            "schemaVersion": 2,
            "config": {"digest": "sha256:amd-config"},
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": "sha256:amd-layer",
                "size": 1,
            }]
        });
        let arm_manifest = serde_json::json!({
            "schemaVersion": 2,
            "config": {"digest": "sha256:arm-config"},
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": "sha256:arm-layer",
                "size": 1,
            }]
        });
        let amd_bytes = serde_json::to_vec(&amd_manifest).unwrap();
        let arm_bytes = serde_json::to_vec(&arm_manifest).unwrap();
        fs::write(layout.join("blobs/sha256/amdManifest"), &amd_bytes).unwrap();
        fs::write(layout.join("blobs/sha256/armManifest"), &arm_bytes).unwrap();

        let index_json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:amdManifest",
                    "size": amd_bytes.len() as u64,
                    "platform": {"architecture": "amd64", "os": "linux"}
                },
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:armManifest",
                    "size": arm_bytes.len() as u64,
                    "platform": {"architecture": "arm64", "os": "linux"}
                }
            ]
        });
        fs::write(
            layout.join("index.json"),
            serde_json::to_vec(&index_json).unwrap(),
        )
        .unwrap();

        let picked_arm = read_image_manifest(layout, "arm64", "linux").unwrap();
        assert_eq!(picked_arm.layers[0].digest, "sha256:arm-layer");

        let picked_amd = read_image_manifest(layout, "amd64", "linux").unwrap();
        assert_eq!(picked_amd.layers[0].digest, "sha256:amd-layer");
    }

    #[test]
    fn read_image_manifest_rejects_excessively_nested_indexes() {
        let tmp = tempdir().unwrap();
        let layout = tmp.path();
        fs::create_dir_all(layout.join("blobs/sha256")).unwrap();

        for level in 0..=MAX_INDEX_RESOLUTION_DEPTH {
            let next_digest = if level == MAX_INDEX_RESOLUTION_DEPTH {
                "sha256:finalManifest".to_string()
            } else {
                format!("sha256:index{level}")
            };
            let media_type = if level == MAX_INDEX_RESOLUTION_DEPTH {
                "application/vnd.oci.image.manifest.v1+json"
            } else {
                "application/vnd.oci.image.index.v1+json"
            };
            let nested = serde_json::json!({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "manifests": [{
                    "mediaType": media_type,
                    "digest": next_digest,
                    "size": 1,
                    "platform": {"architecture": "amd64", "os": "linux"}
                }]
            });
            fs::write(
                layout.join(format!("blobs/sha256/index{level}")),
                serde_json::to_vec(&nested).unwrap(),
            )
            .unwrap();
        }

        fs::write(
            layout.join("blobs/sha256/finalManifest"),
            serde_json::to_vec(&serde_json::json!({
                "schemaVersion": 2,
                "config": {"digest": "sha256:config"},
                "layers": []
            }))
            .unwrap(),
        )
        .unwrap();

        fs::write(
            layout.join("index.json"),
            serde_json::to_vec(&serde_json::json!({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "manifests": [{
                    "mediaType": "application/vnd.oci.image.index.v1+json",
                    "digest": "sha256:index0",
                    "size": 1,
                    "platform": {"architecture": "amd64", "os": "linux"}
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        let err = read_image_manifest(layout, "amd64", "linux").unwrap_err();
        assert!(
            err.to_string().contains("nested OCI index depth exceeded"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_image_ref_splits_host_and_repository() {
        let (host, repo) =
            parse_image_ref("ghcr.io/org/repo:tag").expect("parse fully qualified ref");
        assert_eq!(host, "ghcr.io");
        assert_eq!(repo, "org/repo");
    }

    #[test]
    fn parse_image_ref_strips_digest_from_leaf() {
        let (host, repo) = parse_image_ref("registry.example.com/team/app@sha256:abc0123456789")
            .expect("parse ref with digest");
        assert_eq!(host, "registry.example.com");
        assert_eq!(repo, "team/app");
    }

    #[test]
    fn parse_image_ref_rewrites_docker_io_for_wire_use() {
        let (host, repo) =
            parse_image_ref("docker.io/library/debian:bookworm-slim").expect("parse docker.io ref");
        assert_eq!(host, "registry-1.docker.io");
        assert_eq!(repo, "library/debian");
    }

    #[test]
    fn parse_image_ref_preserves_registry_port() {
        let (host, repo) =
            parse_image_ref("registry.internal:5000/team/app:v1").expect("parse ref with port");
        assert_eq!(host, "registry.internal:5000");
        assert_eq!(repo, "team/app");
    }

    #[test]
    fn parse_image_ref_rejects_shortnames() {
        assert!(parse_image_ref("ubuntu:24.04").is_err());
        assert!(parse_image_ref("library/debian:tag").is_err());
    }

    #[test]
    fn image_ref_with_digest_replaces_tag_and_keeps_host() {
        assert_eq!(
            image_ref_with_digest("docker.io/library/debian:bookworm-slim", "sha256:deadbeef",)
                .unwrap(),
            "docker.io/library/debian@sha256:deadbeef",
        );
        assert_eq!(
            image_ref_with_digest("registry.internal:5000/team/app@sha256:old", "sha256:new",)
                .unwrap(),
            "registry.internal:5000/team/app@sha256:new",
        );
    }
}
