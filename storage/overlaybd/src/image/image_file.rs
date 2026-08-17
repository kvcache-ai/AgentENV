use crate::backend::local::LocalFile;
use crate::backend::switch::new_switch_file;
use crate::backend::tar::new_tar_file_adaptor;
use crate::config::{DownloadConfig, ImageConfig, LayerConfig, UpperConfig, UpperMode};
use crate::image::helper::prepare_runtime_upper;
use crate::image::image_service::CacheDownloadRequest;
use crate::image::image_service::ImageService;
use crate::io::transient_io_ring::shared_transient_io_ring;
use crate::io::virtual_file::VirtualFile;
use crate::layer::layer_metadata::{read_overlaybd_layer_uuid, COMMIT_FILE_NAME, SEALED_FILE_NAME};
use crate::lsmt::file::{
    open_file_rw, open_files_ro_with_premerged_cache, stack_files, CommitArgs, DataStat, LSMTFile,
    LSMTReadOnlyFile, LayerDescriptor, PremergedIndexCachePolicy, PARALLEL_LOAD_INDEX,
};
use crate::prefetch::{new_prefetcher, PrefetchMode, Prefetcher};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use std::fmt;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use storage_util::io_ring::IoRingHandle;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::warn;
use uuid::Uuid;

const DEFAULT_BLOCK_SIZE: u32 = 512;
const IO_ENGINE_LIBAIO: u32 = 2;

struct OpenedLowerLayer {
    file: Arc<dyn VirtualFile>,
    download: Option<CacheDownloadRequest>,
}

struct OpenedLowerFiles {
    file: Option<LSMTReadOnlyFile>,
    downloads: Vec<CacheDownloadRequest>,
}

struct InitializedImageFile {
    state: LiveImageState,
    downloads: Vec<CacheDownloadRequest>,
    prefetcher: Option<Prefetcher>,
    replay_prefetch: bool,
}

enum ImageFileBase {
    ReadOnly(LSMTReadOnlyFile),
    ReadWrite(LSMTFile),
}

impl ImageFileBase {
    fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly(_))
    }

    fn writable(&self) -> Option<&LSMTFile> {
        match self {
            Self::ReadOnly(_) => None,
            Self::ReadWrite(file) => Some(file),
        }
    }
}

#[derive(Debug)]
struct LiveImageState {
    config: ImageConfig,
    base: ImageFileBase,
    premerged_index_cache_dir: PathBuf,
    premerged_index_cache_policy: PremergedIndexCachePolicy,
}

#[derive(Debug, Error)]
#[error("restack snapshot mutated live runtime before failing: {message}")]
pub struct RestackSnapshotTerminalFailure {
    message: String,
}

impl RestackSnapshotTerminalFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

fn into_restack_terminal_failure(err: anyhow::Error) -> anyhow::Error {
    RestackSnapshotTerminalFailure::new(format!("{err:#}")).into()
}

impl fmt::Debug for ImageFileBase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly(_) => f.write_str("ImageFileBase::ReadOnly(..)"),
            Self::ReadWrite(_) => f.write_str("ImageFileBase::ReadWrite(..)"),
        }
    }
}

#[derive(Debug)]
pub struct ImageFile {
    state: RwLock<LiveImageState>,
    size_bytes: AtomicU64,
    num_lbas: AtomicU64,
    pub block_size: u32,
    prefetcher: Option<Prefetcher>,
}

impl ImageFile {
    pub async fn open(
        config: ImageConfig,
        image_service: ImageService,
        device_key: Option<PathBuf>,
    ) -> Result<Self> {
        let download_cfg = config
            .effective_download(image_service.global_config())
            .clone();
        let mut initialized = Self::init_image_file(&config, &image_service, &download_cfg).await?;
        let size = Self::base_size(&initialized.state.base).await?;
        let block_size = DEFAULT_BLOCK_SIZE;
        let num_lbas = size / u64::from(block_size);
        image_service
            .submit_bk_downloads(initialized.downloads, device_key)
            .await?;
        if initialized.replay_prefetch {
            if let Some(prefetcher) = initialized.prefetcher.as_mut() {
                prefetcher.replay()?;
            }
        }

        Ok(Self {
            state: RwLock::new(initialized.state),
            size_bytes: AtomicU64::new(size),
            num_lbas: AtomicU64::new(num_lbas),
            block_size,
            prefetcher: initialized.prefetcher,
        })
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn num_lbas(&self) -> u64 {
        self.num_lbas.load(Ordering::Relaxed)
    }

    pub async fn is_read_only(&self) -> bool {
        let state = self.state.read().await;
        state.base.is_read_only()
    }

    /// Data usage of the live image: unique bytes mapped by the merged index
    /// (`valid_data_size`) and the top layer's physical size
    /// (`total_data_size`). Cheap in-memory index scan; feeds observability
    /// of layer bloat and TRIM headroom.
    pub async fn data_stat(&self) -> Result<DataStat> {
        let state = self.state.read().await;
        match &state.base {
            ImageFileBase::ReadOnly(file) => Ok(file.data_stat()),
            ImageFileBase::ReadWrite(file) => file.data_stat().await,
        }
    }

    pub fn get_uuid(&self, layer_idx: usize) -> Result<Uuid> {
        let state = self.state.blocking_read();
        match &state.base {
            ImageFileBase::ReadOnly(file) => file.get_uuid(layer_idx),
            ImageFileBase::ReadWrite(file) => file.get_uuid(layer_idx),
        }
    }

    pub async fn compact(&self, as_file: Arc<dyn VirtualFile>) -> Result<()> {
        let state = self.state.read().await;
        match &state.base {
            ImageFileBase::ReadOnly(file) => file.flatten(as_file).await,
            ImageFileBase::ReadWrite(file) => file.flatten(as_file).await,
        }
    }

    pub async fn export_upper_as_sealed(&self, args: CommitArgs) -> Result<()> {
        let state = self.state.read().await;
        state
            .base
            .writable()
            .context("export_upper_as_sealed requires a writable upper layer")?
            .export_upper_as_sealed(args)
            .await
    }

    pub async fn create_snapshot_and_restack(
        &self,
        output_layer_path: &Path,
    ) -> Result<Option<LayerDescriptor>> {
        let mut state = self.state.write().await;
        let upper_mode = state.config.upper.writable_mode();
        let upper_data_path = PathBuf::from(&state.config.upper.data);
        let upper_index_path = (!state.config.upper.index.is_empty())
            .then(|| PathBuf::from(&state.config.upper.index));
        if upper_data_path.as_os_str().is_empty()
            || (matches!(
                upper_mode,
                UpperMode::LogStructured | UpperMode::HybridLogStructured
            ) && upper_index_path.is_none())
        {
            bail!("create_snapshot_and_restack requires a writable upper layer");
        }
        let Some(output_dir) = output_layer_path.parent() else {
            bail!(
                "snapshot output path has no parent directory: {}",
                output_layer_path.display()
            );
        };
        tokio::fs::create_dir_all(output_dir)
            .await
            .with_context(|| format!("create snapshot output dir {}", output_dir.display()))?;

        let premerged_index_cache_dir = state.premerged_index_cache_dir.clone();
        let premerged_index_cache_policy = state.premerged_index_cache_policy;
        let current = state
            .base
            .writable()
            .context("create_snapshot_and_restack requires a writable image")?;

        let virtual_size = self.size_bytes();
        let max_io_size = current.get_max_io_size();
        let group_commit_size = current.get_index_group_commit_size();
        let (restacked, descriptor) = async {
            // `open_files_ro` consumes layers in bottom-to-top order and flips
            // them into its internal top-to-bottom representation.
            let mut lower_files = current.lower_layer_files_bottom_to_top();
            let (reopened, descriptor) = current.close_seal_and_reopen().await?;
            lower_files.extend(reopened.get_lower_files());

            tokio::fs::rename(&upper_data_path, output_layer_path)
                .await
                .with_context(|| {
                    format!(
                        "move sealed upper data from {} to {}",
                        upper_data_path.display(),
                        output_layer_path.display()
                    )
                })?;

            prepare_runtime_upper(
                &upper_data_path,
                upper_index_path.as_deref(),
                virtual_size,
                upper_mode,
            )
            .context("prepare fresh upper after restack")?;

            let io_ring = shared_transient_io_ring();
            let new_upper_data: Arc<dyn VirtualFile> = Arc::new(
                LocalFile::open_rw(&upper_data_path, false, io_ring.clone())
                    .await
                    .with_context(|| {
                        format!("open fresh upper data {}", upper_data_path.display())
                    })?,
            );
            let new_upper_index = match upper_mode {
                UpperMode::Sparse => None,
                UpperMode::LogStructured | UpperMode::HybridLogStructured => {
                    let upper_index_path = upper_index_path
                        .as_ref()
                        .context("log-structured upper lost its index path during restack")?;
                    Some(Arc::new(
                        LocalFile::open_rw(upper_index_path, false, io_ring)
                            .await
                            .with_context(|| {
                                format!("open fresh upper index {}", upper_index_path.display())
                            })?,
                    ) as Arc<dyn VirtualFile>)
                }
            };
            let new_upper = open_file_rw(new_upper_data, new_upper_index)
                .await
                .context("open fresh upper after restack")?;
            new_upper.set_max_io_size(max_io_size)?;
            new_upper.set_index_group_commit(group_commit_size)?;

            let lower = open_files_ro_with_premerged_cache(
                &lower_files,
                &premerged_index_cache_dir,
                premerged_index_cache_policy,
            )
            .await
            .context("reopen lower stack after restack")?;
            let restacked = stack_files(&new_upper, &lower, false)
                .await
                .context("restack fresh upper onto sealed snapshot lower")?;
            Ok::<_, anyhow::Error>((restacked, descriptor))
        }
        .await
        .map_err(into_restack_terminal_failure)?;

        let output_metadata = tokio::fs::metadata(output_layer_path).await.ok();
        let uuid = read_overlaybd_layer_uuid(output_layer_path)
            .ok()
            .filter(|uuid| !uuid.is_nil())
            .map(|uuid| uuid.to_string())
            .unwrap_or_default();
        state.config.lowers.push(LayerConfig {
            file: output_layer_path.display().to_string(),
            digest: descriptor
                .as_ref()
                .map(|descriptor| descriptor.digest.clone())
                .unwrap_or_default(),
            size: descriptor
                .as_ref()
                .map(|descriptor| descriptor.size)
                .or_else(|| output_metadata.as_ref().map(|metadata| metadata.len()))
                .unwrap_or(0),
            uuid,
            ..LayerConfig::default()
        });
        state.base = ImageFileBase::ReadWrite(restacked);
        self.update_size_metadata(virtual_size);
        Ok(descriptor)
    }

    async fn init_image_file(
        config: &ImageConfig,
        image_service: &ImageService,
        download_cfg: &DownloadConfig,
    ) -> Result<InitializedImageFile> {
        let mut lowers_config = config.lowers.clone();
        let mut prefetcher = None;
        let mut skip_background_download = false;
        let concurrency = image_service.global_config().prefetch_config.concurrency;

        if config.acceleration_layer && !lowers_config.is_empty() {
            let accel_layer = lowers_config.pop().expect("checked non-empty lowers");
            let trace_file = Path::new(&accel_layer.dir).join("trace");
            if Prefetcher::detect_mode(&trace_file) == PrefetchMode::Replay {
                prefetcher = Some(new_prefetcher(trace_file, concurrency)?);
            }
        } else if !config.record_trace_path.is_empty()
            && tokio::fs::try_exists(&config.record_trace_path).await?
        {
            let mode = Prefetcher::detect_mode(&config.record_trace_path);
            if mode != PrefetchMode::Disabled {
                prefetcher = Some(new_prefetcher(&config.record_trace_path, concurrency)?);
                if mode == PrefetchMode::Record {
                    skip_background_download = true;
                }
            }
        }

        let lowers = Self::open_lowers(
            &lowers_config,
            &config.repo_blob_url,
            download_cfg,
            download_cfg.enable && !skip_background_download,
            image_service,
            prefetcher.as_ref(),
        )
        .await?;
        let upper_file =
            Self::open_upper(&config.upper, image_service.io_ring(&config.upper.index)).await?;
        let replay_prefetch = prefetcher
            .as_ref()
            .map(|prefetcher| prefetcher.mode() == PrefetchMode::Replay)
            .unwrap_or(false)
            && lowers.file.is_some();
        let premerged_index_cache_dir =
            PathBuf::from(&image_service.global_config().cache_config.cache_dir);
        let premerged_index_cache_policy = PremergedIndexCachePolicy::hybrid(
            image_service.global_config().cache_config.cache_size_gb,
        );

        let base = match (lowers.file, upper_file) {
            (Some(lower), Some(upper)) => InitializedImageFile {
                state: LiveImageState {
                    config: config.clone(),
                    base: ImageFileBase::ReadWrite(stack_files(&upper, &lower, false).await?),
                    premerged_index_cache_dir: premerged_index_cache_dir.clone(),
                    premerged_index_cache_policy,
                },
                downloads: lowers.downloads,
                prefetcher,
                replay_prefetch,
            },
            (Some(lower), None) => InitializedImageFile {
                state: LiveImageState {
                    config: config.clone(),
                    base: ImageFileBase::ReadOnly(lower),
                    premerged_index_cache_dir: premerged_index_cache_dir.clone(),
                    premerged_index_cache_policy,
                },
                downloads: lowers.downloads,
                prefetcher,
                replay_prefetch,
            },
            (None, Some(upper)) => InitializedImageFile {
                state: LiveImageState {
                    config: config.clone(),
                    base: ImageFileBase::ReadWrite(upper),
                    premerged_index_cache_dir,
                    premerged_index_cache_policy,
                },
                downloads: Vec::new(),
                prefetcher,
                replay_prefetch: false,
            },
            (None, None) => bail!("image config has no lower and no upper layer"),
        };

        Ok(base)
    }

    async fn open_lowers(
        lowers: &[LayerConfig],
        repo_blob_url: &str,
        download_cfg: &DownloadConfig,
        collect_download_requests: bool,
        image_service: &ImageService,
        prefetcher: Option<&Prefetcher>,
    ) -> Result<OpenedLowerFiles> {
        if lowers.is_empty() {
            return Ok(OpenedLowerFiles {
                file: None,
                downloads: Vec::new(),
            });
        }

        let results = stream::iter(lowers.iter().cloned().enumerate())
            .map(|(index, layer)| {
                let image_service = image_service.clone();
                let repo_blob_url = layer.effective_repo_blob_url(repo_blob_url).to_string();
                async move {
                    let file = Self::open_lower_layer(
                        &image_service,
                        &repo_blob_url,
                        download_cfg,
                        collect_download_requests,
                        prefetcher,
                        layer,
                        index,
                    )
                    .await;
                    (index, file)
                }
            })
            .buffer_unordered(PARALLEL_LOAD_INDEX)
            .collect::<Vec<_>>()
            .await;

        let mut ordered: Vec<Option<OpenedLowerLayer>> = (0..lowers.len()).map(|_| None).collect();
        for (index, result) in results {
            ordered[index] =
                Some(result.with_context(|| format!("failed to open lower layer {index}"))?);
        }

        let opened = ordered
            .into_iter()
            .map(|entry| {
                entry.ok_or_else(|| {
                    anyhow::anyhow!("parallel lower open returned missing layer slot")
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut files = Vec::with_capacity(opened.len());
        let mut downloads = Vec::new();
        for entry in opened {
            files.push(entry.file);
            if let Some(download) = entry.download {
                downloads.push(download);
            }
        }
        let cache_cfg = &image_service.global_config().cache_config;
        let merged = open_files_ro_with_premerged_cache(
            &files,
            &cache_cfg.cache_dir,
            PremergedIndexCachePolicy::hybrid(cache_cfg.cache_size_gb),
        )
        .await?;

        Ok(OpenedLowerFiles {
            file: Some(merged),
            downloads,
        })
    }

    async fn open_lower_layer(
        image_service: &ImageService,
        repo_blob_url: &str,
        download_cfg: &DownloadConfig,
        collect_download_requests: bool,
        prefetcher: Option<&Prefetcher>,
        layer: LayerConfig,
        index: usize,
    ) -> Result<OpenedLowerLayer> {
        let local_path = Self::open_localfile_path(&layer).await?;
        let mut opened = if let Some(local_path) = local_path {
            match Self::open_ro_file(&local_path, image_service).await {
                Ok(file) => OpenedLowerLayer {
                    file,
                    download: None,
                },
                Err(err) if is_not_found(&err) && !layer.uuid.is_empty() => {
                    Self::open_ro_p2p_uuid(image_service, &layer).await?
                }
                Err(err) => return Err(err),
            }
        } else {
            Self::open_ro_remote(
                image_service,
                repo_blob_url,
                download_cfg,
                collect_download_requests,
                &layer,
                index,
            )
            .await?
        };

        if let Some(prefetcher) = prefetcher {
            opened.file = prefetcher.new_prefetch_file(opened.file, index as u32);
        }

        if !layer.target_file.is_empty()
            || !layer.target_digest.is_empty()
            || !layer.gzip_index.is_empty()
        {
            return Err(anyhow::anyhow!(
                "warp/targetDigest/gzipIndex lower layers are not migrated in Rust image_file yet",
            ));
        }

        Ok(opened)
    }

    async fn open_upper(upper: &UpperConfig, io_ring: IoRingHandle) -> Result<Option<LSMTFile>> {
        if upper.data.is_empty() {
            return Ok(None);
        }
        if !upper.target.is_empty() || !upper.gzip_index.is_empty() {
            return Err(anyhow::anyhow!(
                "warp/gzipIndex upper layers are not migrated in Rust image_file yet",
            ));
        }

        let data_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::open_rw(&upper.data, false, io_ring.clone()).await?);
        let idx_file = match upper.writable_mode() {
            UpperMode::Sparse => None,
            UpperMode::LogStructured | UpperMode::HybridLogStructured => {
                if upper.index.is_empty() {
                    bail!("log-structured upper requires upper.index");
                }
                Some(
                    Arc::new(LocalFile::open_rw(&upper.index, false, io_ring).await?)
                        as Arc<dyn VirtualFile>,
                )
            }
        };
        Ok(Some(open_file_rw(data_file, idx_file).await?))
    }

    async fn open_localfile_path(layer: &LayerConfig) -> Result<Option<PathBuf>> {
        if !layer.file.is_empty() {
            return Ok(Some(PathBuf::from(&layer.file)));
        }
        if layer.dir.is_empty() {
            return Ok(None);
        }

        let commit = Path::new(&layer.dir).join(COMMIT_FILE_NAME);
        if tokio::fs::try_exists(&commit).await? {
            return Ok(Some(commit));
        }

        let sealed = Path::new(&layer.dir).join(SEALED_FILE_NAME);
        if tokio::fs::try_exists(&sealed).await? {
            return Ok(Some(sealed));
        }

        Ok(None)
    }

    async fn open_ro_file(
        path: &Path,
        image_service: &ImageService,
    ) -> Result<Arc<dyn VirtualFile>> {
        let path_display = path.to_string_lossy().into_owned();
        let direct_io = image_service.io_engine() == IO_ENGINE_LIBAIO;
        let file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::builder(image_service.io_ring(path))
                .write(false)
                .create(false)
                .direct_io(direct_io)
                .open(path)
                .await?,
        );
        let tar_file = new_tar_file_adaptor(file).await?;
        let switch = new_switch_file(tar_file, true, Some(path_display.as_str())).await?;
        Ok(switch)
    }

    async fn open_ro_p2p_uuid(
        image_service: &ImageService,
        layer: &LayerConfig,
    ) -> Result<OpenedLowerLayer> {
        let uuid = Uuid::parse_str(&layer.uuid)
            .with_context(|| format!("invalid overlaybd layer uuid '{}'", layer.uuid))?;
        let p2p_uuid_address = image_service
            .p2p_uuid_address()
            .context("lower layer local file is missing and p2p uuid facade is not configured")?;
        let url = format!("{}/{}", p2p_uuid_address.trim_end_matches('/'), uuid);
        let remote_file = image_service
            .open_source_blob_with_size(&url, (layer.size != 0).then_some(layer.size))
            .await?;
        let tar_file = new_tar_file_adaptor(remote_file).await?;
        let switch_file = new_switch_file(tar_file, false, Some(&url)).await?;
        Ok(OpenedLowerLayer {
            file: switch_file,
            download: None,
        })
    }

    async fn base_size(base: &ImageFileBase) -> Result<u64> {
        match base {
            ImageFileBase::ReadOnly(file) => file.size().await,
            ImageFileBase::ReadWrite(file) => file.size().await,
        }
    }

    fn update_size_metadata(&self, size: u64) {
        self.size_bytes.store(size, Ordering::Relaxed);
        self.num_lbas
            .store(size / u64::from(self.block_size), Ordering::Relaxed);
    }

    async fn refresh_size_metadata(&self) -> Result<u64> {
        let state = self.state.read().await;
        let size = Self::base_size(&state.base).await?;
        self.update_size_metadata(size);
        Ok(size)
    }

    async fn open_ro_remote(
        image_service: &ImageService,
        repo_blob_url: &str,
        download_cfg: &DownloadConfig,
        collect_download_requests: bool,
        layer: &LayerConfig,
        index: usize,
    ) -> Result<OpenedLowerLayer> {
        if layer.digest.is_empty() {
            bail!("lower layer {index} has no local file and no digest");
        }
        if repo_blob_url.is_empty() {
            bail!("repoBlobUrl is empty for remote lower layer");
        }

        if !layer.uuid.is_empty() && image_service.p2p_uuid_address().is_some() {
            match Self::open_ro_p2p_uuid(image_service, layer).await {
                Ok(opened) => return Ok(opened),
                Err(error) => {
                    warn!(
                        layer_index = index,
                        uuid = %layer.uuid,
                        error = ?error,
                        "p2p uuid lower open failed; falling back to remote digest"
                    );
                }
            }
        }

        let url = format!("{}/{}", repo_blob_url.trim_end_matches('/'), layer.digest);
        let source_size = (layer.size != 0).then_some(layer.size);
        let (remote_file, download) = if collect_download_requests {
            match image_service
                .open_remote_blob_for_bk_download_with_size(&url, source_size, download_cfg.clone())
                .await
            {
                Ok((remote_file, request)) => (remote_file, Some(request)),
                // Background request construction is an optimization; a
                // cache/source hiccup must not abort the image open. Log only
                // the fixed category (the error chain may embed the URL).
                Err(_) => {
                    tracing::warn!(
                        error_category = "bk_download_request_build_failed",
                        "falling back to foreground-only open for remote layer"
                    );
                    (
                        image_service
                            .open_remote_blob_with_size(&url, source_size)
                            .await?,
                        None,
                    )
                }
            }
        } else {
            (
                image_service
                    .open_remote_blob_with_size(&url, source_size)
                    .await?,
                None,
            )
        };
        let tar_file = new_tar_file_adaptor(remote_file).await?;
        let switch_file = new_switch_file(tar_file, false, Some(&url)).await?;
        Ok(OpenedLowerLayer {
            file: switch_file,
            download,
        })
    }
}

fn is_not_found(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == ErrorKind::NotFound)
    })
}

impl Drop for ImageFile {
    fn drop(&mut self) {
        let _ = self.prefetcher.take();
    }
}

#[async_trait]
impl VirtualFile for ImageFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        // Foreground reads all pass through ImageFile (background downloads
        // read their own source file directly and never get here), so this
        // entry is the single place to count foreground in-flight reads for
        // the background-download admission gate.
        let _fg_guard = crate::download_gate::FgReadGuard::new();
        let state = self.state.read().await;
        match &state.base {
            ImageFileBase::ReadOnly(file) => file.read_at(offset, len).await,
            ImageFileBase::ReadWrite(file) => file.read_at(offset, len).await,
        }
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let _fg_guard = crate::download_gate::FgReadGuard::new();
        let state = self.state.read().await;
        match &state.base {
            ImageFileBase::ReadOnly(file) => file.read_at_into(offset, dst).await,
            ImageFileBase::ReadWrite(file) => file.read_at_into(offset, dst).await,
        }
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let state = self.state.read().await;
        let written = state
            .base
            .writable()
            .context("writing read-only image file")?
            .write_at(offset, data)
            .await?;
        self.refresh_size_metadata().await?;
        Ok(written)
    }

    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: crate::io::virtual_file::IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> crate::io::virtual_file::LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            // Same foreground accounting as `read_at`: ublk queue threads
            // enter through the ctx variants, so the guard must live inside
            // this async body (created on first poll, dropped on completion).
            let _fg_guard = crate::download_gate::FgReadGuard::new();
            let state = self.state.read().await;
            match &state.base {
                ImageFileBase::ReadOnly(file) => file.read_at_with_ctx(ctx, offset, len).await,
                ImageFileBase::ReadWrite(file) => file.read_at_with_ctx(ctx, offset, len).await,
            }
        })
    }

    fn read_at_into_with_ctx<'a>(
        &'a self,
        ctx: crate::io::virtual_file::IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> crate::io::virtual_file::LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            // See `read_at_with_ctx`: one guard per ImageFile read request,
            // including the ublk `read_at_into_with_ctx` entry.
            let _fg_guard = crate::download_gate::FgReadGuard::new();
            let state = self.state.read().await;
            match &state.base {
                ImageFileBase::ReadOnly(file) => file.read_at_into_with_ctx(ctx, offset, dst).await,
                ImageFileBase::ReadWrite(file) => {
                    file.read_at_into_with_ctx(ctx, offset, dst).await
                }
            }
        })
    }

    fn write_at_with_ctx<'a>(
        &'a self,
        ctx: crate::io::virtual_file::IoCtx<'a>,
        offset: u64,
        data: &'a [u8],
    ) -> crate::io::virtual_file::LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let state = self.state.read().await;
            let written = state
                .base
                .writable()
                .context("writing read-only image file")?
                .write_at_with_ctx(ctx, offset, data)
                .await?;
            self.refresh_size_metadata().await?;
            Ok(written)
        })
    }

    fn write_bytes_at_with_ctx<'a>(
        &'a self,
        ctx: crate::io::virtual_file::IoCtx<'a>,
        offset: u64,
        data: Bytes,
    ) -> crate::io::virtual_file::LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let state = self.state.read().await;
            let written = state
                .base
                .writable()
                .context("writing read-only image file")?
                .write_bytes_at_with_ctx(ctx, offset, data)
                .await?;
            self.refresh_size_metadata().await?;
            Ok(written)
        })
    }

    async fn discard(&self, offset: u64, len: u64) -> Result<()> {
        let state = self.state.read().await;
        state
            .base
            .writable()
            .context("discarding read-only image file")?
            .discard(offset, len)
            .await?;
        self.refresh_size_metadata().await?;
        Ok(())
    }

    async fn size(&self) -> Result<u64> {
        self.refresh_size_metadata().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        let state = self.state.read().await;
        state
            .base
            .writable()
            .context("truncating read-only image file")?
            .truncate(size)
            .await?;
        self.refresh_size_metadata().await?;
        Ok(())
    }

    async fn sync(&self) -> Result<()> {
        let state = self.state.read().await;
        match &state.base {
            ImageFileBase::ReadOnly(file) => file.sync().await,
            ImageFileBase::ReadWrite(file) => file.sync().await,
        }
    }

    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        let state = self.state.read().await;
        match &state.base {
            ImageFileBase::ReadOnly(file) => {
                <LSMTReadOnlyFile as VirtualFile>::seek_data(file, offset).await
            }
            ImageFileBase::ReadWrite(file) => {
                <LSMTFile as VirtualFile>::seek_data(file, offset).await
            }
        }
    }

    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        let state = self.state.read().await;
        match &state.base {
            ImageFileBase::ReadOnly(file) => {
                <LSMTReadOnlyFile as VirtualFile>::seek_hole(file, offset).await
            }
            ImageFileBase::ReadWrite(file) => {
                <LSMTFile as VirtualFile>::seek_hole(file, offset).await
            }
        }
    }

    async fn evict_range(&self, offset: u64, len: u64) -> Result<()> {
        let state = self.state.read().await;
        match &state.base {
            ImageFileBase::ReadOnly(file) => file.evict_range(offset, len).await,
            ImageFileBase::ReadWrite(file) => file.evict_range(offset, len).await,
        }
    }

    async fn evict_all(&self) -> Result<()> {
        let state = self.state.read().await;
        match &state.base {
            ImageFileBase::ReadOnly(file) => file.evict_all().await,
            ImageFileBase::ReadWrite(file) => file.evict_all().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DownloadConfig;
    use crate::lsmt::file::{create_file_rw, LayerInfo, RwLayout};
    use crate::prefetch::new_prefetcher;
    use crate::test_utils::test_io_ring;
    use axum::body::Body;
    use axum::extract::{Request, State};
    use axum::http::header::CONTENT_RANGE as CONTENT_RANGE_RAW;
    use axum::http::{HeaderMap as HttpHeaderMap, Response, StatusCode as HttpStatusCode};
    use axum::routing::{any, get};
    use axum::{Json, Router};
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use tempfile::{NamedTempFile, TempDir};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex as AsyncMutex;
    use tokio::time::{sleep, Duration};

    async fn create_sealed_lower(path: &Path, index_path: &Path, payload: &[u8]) -> Result<()> {
        let ring = test_io_ring();
        let data_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path, ring.clone()).await?);
        let index_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(index_path, ring).await?);
        let args = LayerInfo::new(data_file.clone(), Some(index_file), payload.len() as u64);
        let lsmt = create_file_rw(args).await?;
        lsmt.write_at(0, payload).await?;
        lsmt.close_seal().await?;
        Ok(())
    }

    fn write_json(path: &Path, value: &serde_json::Value) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(value).expect("serialize json"),
        )
        .expect("write json");
    }

    async fn spawn_server(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
        let addr = listener.local_addr().expect("server addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("run server");
        });
        (format!("http://{addr}"), handle)
    }

    fn parse_request_range(headers: &HttpHeaderMap) -> Option<(u64, u64)> {
        let raw = headers.get(reqwest::header::RANGE)?.to_str().ok()?.trim();
        let raw = raw.strip_prefix("bytes=")?;
        let (start, end) = raw.split_once('-')?;
        Some((start.parse().ok()?, end.parse().ok()?))
    }

    fn encode_hex(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "{byte:02x}");
        }
        out
    }

    fn digest_of(data: &[u8]) -> String {
        format!("sha256:{}", encode_hex(&Sha256::digest(data)))
    }

    #[derive(Clone, Debug)]
    struct RemoteLayerState {
        blob: Arc<Vec<u8>>,
        data_bytes: Arc<AtomicUsize>,
        digest: String,
    }

    async fn handle_remote_blob(
        State(state): State<RemoteLayerState>,
        headers: HttpHeaderMap,
    ) -> Response<Body> {
        let host = headers
            .get("host")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        let location = format!("http://{host}/data/{}", state.digest);
        Response::builder()
            .status(HttpStatusCode::FOUND)
            .header(reqwest::header::LOCATION, location)
            .body(Body::empty())
            .expect("302 response")
    }

    async fn handle_remote_data(
        State(state): State<RemoteLayerState>,
        headers: HttpHeaderMap,
    ) -> Response<Body> {
        let len = state.blob.len() as u64;
        let (start, end) = parse_request_range(&headers).expect("range header");
        let start = start.min(len.saturating_sub(1));
        let end = end.min(len.saturating_sub(1));
        let body = state.blob[start as usize..=end as usize].to_vec();
        state
            .data_bytes
            .fetch_add(body.len(), AtomicOrdering::Relaxed);
        Response::builder()
            .status(HttpStatusCode::PARTIAL_CONTENT)
            .header(CONTENT_RANGE_RAW, format!("bytes {start}-{end}/{len}"))
            .header(reqwest::header::CONTENT_LENGTH, body.len().to_string())
            .body(Body::from(body))
            .expect("206 response")
    }

    async fn handle_token() -> Json<serde_json::Value> {
        Json(json!({ "token": "read-token" }))
    }

    async fn handle_remote_request(
        State(state): State<RemoteLayerState>,
        request: Request,
    ) -> Response<Body> {
        let path = request.uri().path().to_string();
        let headers = request.headers().clone();

        if path.starts_with("/v2/ns/repo/blobs/") {
            return handle_remote_blob(State(state), headers).await;
        }
        if path.starts_with("/data/") {
            return handle_remote_data(State(state), headers).await;
        }

        Response::builder()
            .status(HttpStatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("404 response")
    }

    async fn create_initialized_upper_with_mode(
        data_path: &Path,
        index_path: Option<&Path>,
        virtual_size: u64,
        mode: UpperMode,
    ) -> Result<()> {
        let ring = test_io_ring();
        let data_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(data_path, ring.clone()).await?);
        let index_file = match mode {
            UpperMode::Sparse => None,
            UpperMode::LogStructured | UpperMode::HybridLogStructured => {
                let index_path =
                    index_path.context("log-structured test upper requires an index path")?;
                Some(Arc::new(LocalFile::new(index_path, ring).await?) as Arc<dyn VirtualFile>)
            }
        };
        let mut args = LayerInfo::new(data_file, index_file, virtual_size);
        args.rw_layout = RwLayout::from(mode);
        let _upper = create_file_rw(args).await?;
        Ok(())
    }

    async fn create_initialized_upper(
        data_path: &Path,
        index_path: &Path,
        virtual_size: u64,
    ) -> Result<()> {
        create_initialized_upper_with_mode(
            data_path,
            Some(index_path),
            virtual_size,
            UpperMode::LogStructured,
        )
        .await
    }

    async fn build_service_with_io_engine(tmp: &TempDir, io_engine: u32) -> ImageService {
        let global_path = tmp.path().join("overlaybd.json");
        write_json(
            &global_path,
            &json!({
                "registryFsVersion": "v2",
                "ioEngine": io_engine,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "download": serde_json::to_value(DownloadConfig::default()).expect("download")
            }),
        );
        ImageService::from_config_path(global_path)
            .await
            .expect("service")
    }

    async fn build_service(tmp: &TempDir) -> ImageService {
        build_service_with_io_engine(tmp, 0).await
    }

    async fn build_service_with_p2p_address(tmp: &TempDir, p2p_address: &str) -> ImageService {
        let global_path = tmp.path().join("overlaybd.json");
        write_json(
            &global_path,
            &json!({
                "registryFsVersion": "v2",
                "ioEngine": 0,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "download": serde_json::to_value(DownloadConfig::default()).expect("download"),
                "p2pConfig": {
                    "enable": true,
                    "address": p2p_address
                }
            }),
        );
        ImageService::from_config_path(global_path)
            .await
            .expect("service")
    }

    async fn build_oss_service(tmp: &TempDir, endpoint: &str, region: &str) -> ImageService {
        let global_path = tmp.path().join("overlaybd.json");
        write_json(
            &global_path,
            &json!({
                "registryFsVersion": "v2",
                "ioEngine": 0,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "download": serde_json::to_value(DownloadConfig::default()).expect("download"),
                "ossConfig": {
                    "enable": true,
                    "accessKeyId": "minioadmin",
                    "secretAccessKey": "minioadmin",
                    "defaultRegion": region,
                    "defaultEndpoint": endpoint
                }
            }),
        );
        ImageService::from_config_path(global_path)
            .await
            .expect("service")
    }

    #[derive(Clone, Debug)]
    struct P2pUuidLayerState {
        blob: Arc<Vec<u8>>,
        hits: Arc<AtomicUsize>,
        miss: bool,
    }

    async fn handle_p2p_uuid_request(
        State(state): State<P2pUuidLayerState>,
        request: Request,
    ) -> Response<Body> {
        let path = request.uri().path();
        if !path.starts_with("/p2p-uuid/") {
            return Response::builder()
                .status(HttpStatusCode::NOT_FOUND)
                .body(Body::empty())
                .expect("404 response");
        }
        state.hits.fetch_add(1, AtomicOrdering::Relaxed);
        if state.miss {
            return Response::builder()
                .status(HttpStatusCode::NOT_FOUND)
                .body(Body::empty())
                .expect("404 response");
        }
        let len = state.blob.len() as u64;
        let headers = request.headers().clone();
        if *request.method() == axum::http::Method::HEAD {
            return Response::builder()
                .status(HttpStatusCode::OK)
                .header(reqwest::header::CONTENT_LENGTH, len.to_string())
                .body(Body::empty())
                .expect("head response");
        }
        let Some((start, end)) = parse_request_range(&headers) else {
            return Response::builder()
                .status(HttpStatusCode::OK)
                .header(reqwest::header::CONTENT_LENGTH, len.to_string())
                .body(Body::from(state.blob.as_ref().clone()))
                .expect("full response");
        };
        let start = start.min(len.saturating_sub(1));
        let end = end.min(len.saturating_sub(1));
        let body = state.blob[start as usize..=end as usize].to_vec();
        Response::builder()
            .status(HttpStatusCode::PARTIAL_CONTENT)
            .header(CONTENT_RANGE_RAW, format!("bytes {start}-{end}/{len}"))
            .header(reqwest::header::CONTENT_LENGTH, body.len().to_string())
            .body(Body::from(body))
            .expect("206 response")
    }

    #[derive(Clone, Debug, Default)]
    struct UploadedObjectState {
        blob: Arc<AsyncMutex<Option<Vec<u8>>>>,
    }

    async fn handle_uploaded_object(
        State(state): State<UploadedObjectState>,
        request: Request,
    ) -> Response<Body> {
        match *request.method() {
            axum::http::Method::PUT => {
                let headers = request.headers();
                let auth = headers
                    .get(reqwest::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default();
                if !auth.starts_with("AWS4-HMAC-SHA256 ") {
                    return Response::builder()
                        .status(HttpStatusCode::FORBIDDEN)
                        .body(Body::from("missing auth"))
                        .expect("403 response");
                }
                let body = axum::body::to_bytes(request.into_body(), usize::MAX)
                    .await
                    .expect("read put body");
                *state.blob.lock().await = Some(body.to_vec());
                Response::builder()
                    .status(HttpStatusCode::OK)
                    .body(Body::empty())
                    .expect("200 response")
            }
            axum::http::Method::HEAD => {
                let guard = state.blob.lock().await;
                match guard.as_ref() {
                    Some(blob) => Response::builder()
                        .status(HttpStatusCode::OK)
                        .header(reqwest::header::CONTENT_LENGTH, blob.len().to_string())
                        .body(Body::empty())
                        .expect("head response"),
                    None => Response::builder()
                        .status(HttpStatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("404 response"),
                }
            }
            axum::http::Method::GET => {
                let guard = state.blob.lock().await;
                let Some(blob) = guard.as_ref() else {
                    return Response::builder()
                        .status(HttpStatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("404 response");
                };
                let len = blob.len() as u64;
                let headers = request.headers().clone();
                if let Some((start, end)) = parse_request_range(&headers) {
                    let start = start.min(len.saturating_sub(1));
                    let end = end.min(len.saturating_sub(1));
                    let body = blob[start as usize..=end as usize].to_vec();
                    Response::builder()
                        .status(HttpStatusCode::PARTIAL_CONTENT)
                        .header(CONTENT_RANGE_RAW, format!("bytes {start}-{end}/{len}"))
                        .header(reqwest::header::CONTENT_LENGTH, body.len().to_string())
                        .body(Body::from(body))
                        .expect("206 response")
                } else {
                    Response::builder()
                        .status(HttpStatusCode::OK)
                        .header(reqwest::header::CONTENT_LENGTH, blob.len().to_string())
                        .body(Body::from(blob.clone()))
                        .expect("200 response")
                }
            }
            _ => Response::builder()
                .status(HttpStatusCode::METHOD_NOT_ALLOWED)
                .body(Body::empty())
                .expect("405 response"),
        }
    }

    #[tokio::test]
    async fn test_image_file_open_local_lower_only() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let payload = vec![0x5a; 8192];
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build sealed lower");

        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open image");
        let got = image.read_at(0, payload.len()).await.expect("read image");
        assert_eq!(got.as_ref(), payload.as_slice());
        assert!(image.is_read_only().await);
    }

    #[tokio::test]
    async fn test_image_file_stack_lower_and_existing_upper() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let lower_payload = vec![0x11; 4096];
        create_sealed_lower(&lower_path, &lower_index, &lower_payload)
            .await
            .expect("build sealed lower");

        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper(&upper_data, &upper_index, lower_payload.len() as u64)
            .await
            .expect("build initialized upper");
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig {
                mode: None,
                index: upper_index.to_string_lossy().into_owned(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open image");

        let overlay = vec![0x22; 4096];
        image.write_at(0, &overlay).await.expect("write overlay");

        let got_overlay = image.read_at(0, overlay.len()).await.expect("read overlay");
        assert_eq!(got_overlay.as_ref(), overlay.as_slice());

        let hole = image.read_at(4096, 4096).await.expect("read hole");
        assert_eq!(hole.len(), 0);
        assert!(!image.is_read_only().await);
    }

    #[tokio::test]
    async fn test_image_file_stack_lower_and_existing_sparse_upper() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let lower_payload = vec![0x11; 4096];
        create_sealed_lower(&lower_path, &lower_index, &lower_payload)
            .await
            .expect("build sealed lower");

        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper_with_mode(
            &upper_data,
            None,
            lower_payload.len() as u64,
            UpperMode::Sparse,
        )
        .await
        .expect("build initialized sparse upper");
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig {
                mode: Some(UpperMode::Sparse),
                index: String::new(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open image");

        let overlay = vec![0x22; 4096];
        image.write_at(0, &overlay).await.expect("write overlay");

        let got_overlay = image.read_at(0, overlay.len()).await.expect("read overlay");
        assert_eq!(got_overlay.as_ref(), overlay.as_slice());

        let hole = image.read_at(4096, 4096).await.expect("read hole");
        assert_eq!(hole.len(), 0);
        assert!(
            !upper_index.exists(),
            "sparse upper should not create an index file"
        );
        assert!(!image.is_read_only().await);
    }

    #[tokio::test]
    async fn test_image_file_sparse_discard_passthrough() {
        let tmp = TempDir::new().expect("tempdir");
        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper_with_mode(&upper_data, None, 8192, UpperMode::Sparse)
            .await
            .expect("build initialized sparse upper");
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: Vec::new(),
            upper: UpperConfig {
                mode: Some(UpperMode::Sparse),
                index: String::new(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg.clone(), service.clone(), None)
            .await
            .expect("open sparse image");

        let overlay = vec![0x22; 4096];
        image.write_at(0, &overlay).await.expect("write overlay");
        <ImageFile as VirtualFile>::discard(&image, 0, overlay.len() as u64)
            .await
            .expect("discard sparse overlay");

        let got = image
            .read_at(0, overlay.len())
            .await
            .expect("read discarded region");
        assert!(got.iter().all(|&b| b == 0));

        drop(image);
        let reopened = ImageFile::open(image_cfg, service, None)
            .await
            .expect("reopen sparse image");
        let reopened_got = reopened
            .read_at(0, overlay.len())
            .await
            .expect("read discarded region after reopen");
        assert!(reopened_got.iter().all(|&b| b == 0));
        assert!(
            !upper_index.exists(),
            "image-level sparse discard should not materialize an index file",
        );
    }

    #[tokio::test]
    async fn test_data_stat_reports_mapped_and_upper_bytes() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        create_sealed_lower(&lower_path, &lower_index, &vec![0x11; 8192])
            .await
            .expect("build sealed lower");
        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper(&upper_data, &upper_index, 8192)
            .await
            .expect("build initialized upper");
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig {
                mode: None,
                index: upper_index.to_string_lossy().into_owned(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open image");
        // Overlay the first half of the lower: merged view must count it once.
        image
            .write_at(0, &vec![0x22; 4096])
            .await
            .expect("write overlay");

        let stat = image.data_stat().await.expect("data stat");
        assert_eq!(stat.valid_data_size, 8192);
        assert!(stat.total_data_size >= 4096);
    }

    #[tokio::test]
    async fn test_create_snapshot_and_restack_keeps_image_writable() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let lower_payload = vec![0x11; 8192];
        create_sealed_lower(&lower_path, &lower_index, &lower_payload)
            .await
            .expect("build sealed lower");

        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper(&upper_data, &upper_index, 8192)
            .await
            .expect("build initialized upper");
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig {
                mode: None,
                index: upper_index.to_string_lossy().into_owned(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service.clone(), None)
            .await
            .expect("open image");

        let first_overlay = vec![0x22; 4096];
        image
            .write_at(0, &first_overlay)
            .await
            .expect("write first overlay");
        image.sync().await.expect("sync first overlay");

        let snapshot_path = tmp.path().join("snapshot.commit");
        let descriptor = image
            .create_snapshot_and_restack(&snapshot_path)
            .await
            .expect("restack snapshot");
        let snapshot_bytes = tokio::fs::read(&snapshot_path)
            .await
            .expect("read snapshot commit");
        assert_eq!(
            descriptor,
            Some(LayerDescriptor {
                digest: digest_of(&snapshot_bytes),
                size: snapshot_bytes.len() as u64,
            })
        );

        let got_after_snapshot = image.read_at(0, 8192).await.expect("read after snapshot");
        assert_eq!(&got_after_snapshot[..4096], first_overlay.as_slice());
        assert_eq!(&got_after_snapshot[4096..8192], &lower_payload[4096..8192]);

        let second_overlay = vec![0x33; 4096];
        image
            .write_at(4096, &second_overlay)
            .await
            .expect("write second overlay");
        image.sync().await.expect("sync second overlay");

        let got_full = image.read_at(0, 8192).await.expect("read full image");
        assert_eq!(&got_full[..4096], first_overlay.as_slice());
        assert_eq!(&got_full[4096..8192], second_overlay.as_slice());

        let snapshot_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(&snapshot_path, service.io_ring(&snapshot_path))
                .await
                .expect("open snapshot file"),
        );
        let snapshot = LSMTReadOnlyFile::open(snapshot_file)
            .await
            .expect("open snapshot lower");
        let got_snapshot = snapshot.read_at(0, 4096).await.expect("read snapshot");
        assert_eq!(got_snapshot.as_ref(), first_overlay.as_slice());
    }

    #[tokio::test]
    async fn test_create_snapshot_and_restack_keeps_sparse_image_writable() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let lower_payload = vec![0x11; 8192];
        create_sealed_lower(&lower_path, &lower_index, &lower_payload)
            .await
            .expect("build sealed lower");

        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper_with_mode(&upper_data, None, 8192, UpperMode::Sparse)
            .await
            .expect("build initialized sparse upper");
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig {
                mode: Some(UpperMode::Sparse),
                index: String::new(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service.clone(), None)
            .await
            .expect("open image");

        let first_overlay = vec![0x22; 4096];
        image
            .write_at(0, &first_overlay)
            .await
            .expect("write first overlay");
        image.sync().await.expect("sync first overlay");

        let snapshot_path = tmp.path().join("snapshot.commit");
        let descriptor = image
            .create_snapshot_and_restack(&snapshot_path)
            .await
            .expect("restack sparse snapshot");
        assert_eq!(descriptor, None);

        let got_after_snapshot = image.read_at(0, 8192).await.expect("read after snapshot");
        assert_eq!(&got_after_snapshot[..4096], first_overlay.as_slice());
        assert_eq!(&got_after_snapshot[4096..8192], &lower_payload[4096..8192]);

        let second_overlay = vec![0x33; 4096];
        image
            .write_at(4096, &second_overlay)
            .await
            .expect("write second overlay");
        image.sync().await.expect("sync second overlay");

        let got_full = image.read_at(0, 8192).await.expect("read full image");
        assert_eq!(&got_full[..4096], first_overlay.as_slice());
        assert_eq!(&got_full[4096..8192], second_overlay.as_slice());
        assert!(
            !upper_index.exists(),
            "restacked sparse upper should not materialize an index file",
        );

        let snapshot_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(&snapshot_path, service.io_ring(&snapshot_path))
                .await
                .expect("open snapshot file"),
        );
        let snapshot = LSMTReadOnlyFile::open(snapshot_file)
            .await
            .expect("open snapshot lower");
        let got_snapshot = snapshot.read_at(0, 4096).await.expect("read snapshot");
        assert_eq!(got_snapshot.as_ref(), first_overlay.as_slice());
    }

    #[tokio::test]
    async fn test_create_snapshot_and_restack_keeps_hybrid_image_writable() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let lower_payload = vec![0x11; 8192];
        create_sealed_lower(&lower_path, &lower_index, &lower_payload)
            .await
            .expect("build sealed lower");

        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper_with_mode(
            &upper_data,
            Some(&upper_index),
            8192,
            UpperMode::HybridLogStructured,
        )
        .await
        .expect("build initialized hybrid upper");
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig {
                mode: Some(UpperMode::HybridLogStructured),
                index: upper_index.to_string_lossy().into_owned(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service.clone(), None)
            .await
            .expect("open image");

        let first_overlay = vec![0x22; 4096];
        image
            .write_at(0, &first_overlay)
            .await
            .expect("write first overlay");
        image
            .write_at(0, &[0x23; 4096])
            .await
            .expect("force hybrid in-place overwrite");
        image.sync().await.expect("sync first overlay");

        let snapshot_path = tmp.path().join("snapshot.commit");
        let descriptor = image
            .create_snapshot_and_restack(&snapshot_path)
            .await
            .expect("restack hybrid snapshot");
        assert_eq!(descriptor, None);

        let got_after_snapshot = image.read_at(0, 8192).await.expect("read after snapshot");
        assert_eq!(&got_after_snapshot[..4096], &[0x23; 4096]);
        assert_eq!(&got_after_snapshot[4096..8192], &lower_payload[4096..8192]);

        let second_overlay = vec![0x33; 4096];
        image
            .write_at(4096, &second_overlay)
            .await
            .expect("write second overlay");
        image
            .write_at(4096, &[0x34; 4096])
            .await
            .expect("force restacked hybrid in-place overwrite");
        image.sync().await.expect("sync second overlay");

        let got_full = image.read_at(0, 8192).await.expect("read full image");
        assert_eq!(&got_full[..4096], &[0x23; 4096]);
        assert_eq!(&got_full[4096..8192], &[0x34; 4096]);
        assert!(
            upper_index.exists(),
            "restacked hybrid upper should keep an index file",
        );

        let snapshot_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(&snapshot_path, service.io_ring(&snapshot_path))
                .await
                .expect("open snapshot file"),
        );
        let snapshot = LSMTReadOnlyFile::open(snapshot_file)
            .await
            .expect("open snapshot lower");
        let got_snapshot = snapshot.read_at(0, 4096).await.expect("read snapshot");
        assert_eq!(got_snapshot.as_ref(), &[0x23; 4096]);
    }

    #[tokio::test]
    async fn test_create_snapshot_and_restack_rename_failure_is_terminal() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let lower_payload = vec![0x11; 4096];
        create_sealed_lower(&lower_path, &lower_index, &lower_payload)
            .await
            .expect("build sealed lower");

        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper(&upper_data, &upper_index, 4096)
            .await
            .expect("build initialized upper");
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig {
                mode: None,
                index: upper_index.to_string_lossy().into_owned(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open image");
        image
            .write_at(0, &[0x44; 4096])
            .await
            .expect("write overlay");
        image.sync().await.expect("sync overlay");

        let snapshot_path = tmp.path().join("snapshot.commit");
        std::fs::create_dir(&snapshot_path).expect("create conflicting directory");
        let err = image
            .create_snapshot_and_restack(&snapshot_path)
            .await
            .expect_err("rename conflict should fail");
        assert!(
            err.downcast_ref::<RestackSnapshotTerminalFailure>()
                .is_some(),
            "expected terminal restack failure, got: {err:#}"
        );

        let write_err = image
            .write_at(0, &[0x55; 4096])
            .await
            .expect_err("sealed runtime should reject further writes");
        assert!(
            write_err.to_string().contains("File is sealed"),
            "expected sealed write failure, got: {write_err:#}"
        );
    }

    #[tokio::test]
    async fn test_export_upper_as_oss_sealed_roundtrip() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let lower_payload = vec![0x11; 4096];
        create_sealed_lower(&lower_path, &lower_index, &lower_payload)
            .await
            .expect("build sealed lower");

        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper(&upper_data, &upper_index, lower_payload.len() as u64)
            .await
            .expect("build initialized upper");

        let app = Router::new()
            .route(
                "/export-bucket/snapshots/upper.lsmt",
                any(handle_uploaded_object),
            )
            .with_state(UploadedObjectState::default());
        let (endpoint, server_handle) = spawn_server(app).await;

        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig {
                mode: None,
                index: upper_index.to_string_lossy().into_owned(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_oss_service(&tmp, &endpoint, "us-east-1").await;
        let image = ImageFile::open(image_cfg, service.clone(), None)
            .await
            .expect("open image");
        let overlay = vec![0x22; 4096];
        image.write_at(0, &overlay).await.expect("write overlay");

        let dest_url =
            format!("s3://export-bucket/snapshots/upper.lsmt?endpoint={endpoint}&region=us-east-1");
        service
            .export_upper_as_oss_sealed(&image, &dest_url)
            .await
            .expect("export upper to oss");

        let exported = service
            .open_remote_blob(&dest_url)
            .await
            .expect("open exported object");
        let exported_ro = LSMTReadOnlyFile::open(exported)
            .await
            .expect("open exported lsmt");
        let got = exported_ro
            .read_at(0, overlay.len())
            .await
            .expect("read overlay");
        assert_eq!(got.as_ref(), overlay.as_slice());

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_image_file_requires_prepared_upper_files() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        create_sealed_lower(&lower_path, &lower_index, &[0x41; 4096])
            .await
            .expect("build sealed lower");

        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig {
                mode: None,
                index: tmp
                    .path()
                    .join("missing-upper.index")
                    .to_string_lossy()
                    .into_owned(),
                data: tmp
                    .path()
                    .join("missing-upper.data")
                    .to_string_lossy()
                    .into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let err = ImageFile::open(image_cfg, service, None)
            .await
            .expect_err("missing upper should not be auto-created");
        assert!(
            format!("{err:?}").contains("No such file")
                || format!("{err:?}").contains("not found")
                || format!("{err:?}").contains("NotFound"),
            "expected not-found error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_image_service_create_image_file_writes_result() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let payload = vec![0x7b; 4096];
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build sealed lower");

        let service = build_service(&tmp).await;
        let image_path = tmp.path().join("image.json");
        let result_path = tmp.path().join("result.txt");
        write_json(
            &image_path,
            &json!({
                "lowers": [
                    {
                        "file": lower_path
                    }
                ],
                "resultFile": result_path
            }),
        );

        let image: ImageFile = service
            .create_image_file(image_path.as_path())
            .await
            .expect("create image file");
        assert_eq!(
            image.size().await.expect("image size"),
            payload.len() as u64
        );

        let result = std::fs::read_to_string(result_path).expect("read result file");
        assert_eq!(result, "success");
    }

    #[tokio::test]
    async fn test_missing_file_lower_with_uuid_opens_from_p2p_uuid_facade() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let payload = vec![0x7c; 8192];
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build sealed lower");
        let blob = std::fs::read(&lower_path).expect("read lower blob");
        let uuid = read_overlaybd_layer_uuid(&lower_path).expect("read layer uuid");
        assert!(!uuid.is_nil());

        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/{*path}", any(handle_p2p_uuid_request))
            .with_state(P2pUuidLayerState {
                blob: Arc::new(blob),
                hits: hits.clone(),
                miss: false,
            });
        let (base, handle) = spawn_server(app).await;
        let service = build_service_with_p2p_address(&tmp, &format!("{base}/p2p-http")).await;
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: tmp
                    .path()
                    .join("missing")
                    .join("snapshot.commit")
                    .to_string_lossy()
                    .into_owned(),
                uuid: uuid.to_string(),
                size: lower_path.metadata().expect("lower metadata").len(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open image from p2p uuid facade");
        let got = image.read_at(0, payload.len()).await.expect("read layer");

        assert_eq!(got.as_ref(), payload.as_slice());
        assert!(hits.load(AtomicOrdering::Relaxed) > 0);
        handle.abort();
    }

    #[tokio::test]
    async fn test_missing_file_lower_without_uuid_errors_without_remote_fallback() {
        let tmp = TempDir::new().expect("tempdir");
        let service = build_service(&tmp).await;
        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: tmp
                    .path()
                    .join("missing")
                    .join("snapshot.commit")
                    .to_string_lossy()
                    .into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let err = ImageFile::open(image_cfg, service, None)
            .await
            .expect_err("missing file without uuid should fail");

        assert!(
            format!("{err:?}").contains("No such file")
                || format!("{err:?}").contains("not found")
                || format!("{err:?}").contains("NotFound"),
            "expected not-found error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_remote_lower_with_uuid_prefers_p2p_uuid_facade() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let payload = vec![0x7d; 8192];
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build sealed lower");
        let blob = std::fs::read(&lower_path).expect("read lower blob");
        let uuid = read_overlaybd_layer_uuid(&lower_path).expect("read layer uuid");
        assert!(!uuid.is_nil());

        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/{*path}", any(handle_p2p_uuid_request))
            .with_state(P2pUuidLayerState {
                blob: Arc::new(blob),
                hits: hits.clone(),
                miss: false,
            });
        let (base, handle) = spawn_server(app).await;
        let service = build_service_with_p2p_address(&tmp, &format!("{base}/p2p-http")).await;
        let image_cfg = ImageConfig {
            repo_blob_url: "http://127.0.0.1:9/managed-layers".to_string(),
            lowers: vec![LayerConfig {
                digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
                uuid: uuid.to_string(),
                size: lower_path.metadata().expect("lower metadata").len(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open remote lower from p2p uuid facade");
        let got = image.read_at(0, payload.len()).await.expect("read layer");

        assert_eq!(got.as_ref(), payload.as_slice());
        assert!(hits.load(AtomicOrdering::Relaxed) > 0);
        handle.abort();
    }

    #[tokio::test]
    async fn test_remote_lower_with_uuid_falls_back_to_digest_remote_on_p2p_miss() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let payload = vec![0x7e; 8192];
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build sealed lower");
        let blob = std::fs::read(&lower_path).expect("read lower blob");
        let digest = digest_of(&blob);
        let uuid = read_overlaybd_layer_uuid(&lower_path).expect("read layer uuid");
        assert!(!uuid.is_nil());

        let p2p_hits = Arc::new(AtomicUsize::new(0));
        let p2p_app = Router::new()
            .route("/{*path}", any(handle_p2p_uuid_request))
            .with_state(P2pUuidLayerState {
                blob: Arc::new(blob.clone()),
                hits: p2p_hits.clone(),
                miss: true,
            });
        let (p2p_base, p2p_handle) = spawn_server(p2p_app).await;

        let remote_state = RemoteLayerState {
            blob: Arc::new(blob),
            data_bytes: Arc::new(AtomicUsize::new(0)),
            digest: digest.clone(),
        };
        let remote_app = Router::new()
            .route("/{*path}", any(handle_remote_request))
            .route("/token", get(handle_token))
            .with_state(remote_state.clone());
        let (remote_base, remote_handle) = spawn_server(remote_app).await;

        let service = build_service_with_p2p_address(&tmp, &format!("{p2p_base}/p2p-http")).await;
        let image_cfg = ImageConfig {
            repo_blob_url: format!("{remote_base}/v2/ns/repo/blobs"),
            lowers: vec![LayerConfig {
                digest,
                uuid: uuid.to_string(),
                size: lower_path.metadata().expect("lower metadata").len(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open remote lower via digest fallback");
        let got = image.read_at(0, payload.len()).await.expect("read layer");

        assert_eq!(got.as_ref(), payload.as_slice());
        assert!(p2p_hits.load(AtomicOrdering::Relaxed) > 0);
        assert!(remote_state.data_bytes.load(AtomicOrdering::Relaxed) > 0);
        p2p_handle.abort();
        remote_handle.abort();
    }

    #[tokio::test]
    async fn test_remote_lower_prefers_layer_repo_blob_url() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let payload = vec![0x7f; 8192];
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build sealed lower");
        let blob = std::fs::read(&lower_path).expect("read lower blob");
        let digest = digest_of(&blob);

        let remote_state = RemoteLayerState {
            blob: Arc::new(blob),
            data_bytes: Arc::new(AtomicUsize::new(0)),
            digest: digest.clone(),
        };
        let remote_app = Router::new()
            .route("/{*path}", any(handle_remote_request))
            .route("/token", get(handle_token))
            .with_state(remote_state.clone());
        let (remote_base, remote_handle) = spawn_server(remote_app).await;

        let service = build_service(&tmp).await;
        let image_cfg = ImageConfig {
            repo_blob_url: "http://127.0.0.1:9/wrong/blobs".to_string(),
            lowers: vec![LayerConfig {
                digest,
                repo_blob_url: format!("{remote_base}/v2/ns/repo/blobs"),
                size: lower_path.metadata().expect("lower metadata").len(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open remote lower from layer repoBlobUrl");
        let got = image.read_at(0, payload.len()).await.expect("read layer");

        assert_eq!(got.as_ref(), payload.as_slice());
        assert!(remote_state.data_bytes.load(AtomicOrdering::Relaxed) > 0);
        remote_handle.abort();
    }

    #[tokio::test]
    async fn test_image_file_remote_lowers_batch_background_downloads_into_foreground_cache() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("remote-lower.data");
        let lower_index = tmp.path().join("remote-lower.index");
        let mut seed = 0x1234_5678_9abc_def0u64;
        let payload: Vec<u8> = (0..(1024 * 1024))
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect();
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build remote lower blob");
        let blob = std::fs::read(&lower_path).expect("read remote lower blob");
        let digest = digest_of(&blob);

        let state = RemoteLayerState {
            blob: Arc::new(blob.clone()),
            data_bytes: Arc::new(AtomicUsize::new(0)),
            digest: digest.clone(),
        };
        let app = Router::new()
            .route("/{*path}", any(handle_remote_request))
            .route("/token", get(handle_token))
            .with_state(state.clone());
        let (base, handle) = spawn_server(app).await;

        let image_cfg = ImageConfig {
            repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
            lowers: vec![
                LayerConfig {
                    dir: tmp
                        .path()
                        .join("download-layer-a")
                        .to_string_lossy()
                        .into_owned(),
                    digest: format!("{digest}-a"),
                    size: blob.len() as u64,
                    ..LayerConfig::default()
                },
                LayerConfig {
                    dir: tmp
                        .path()
                        .join("download-layer-b")
                        .to_string_lossy()
                        .into_owned(),
                    digest: format!("{digest}-b"),
                    size: blob.len() as u64,
                    ..LayerConfig::default()
                },
            ],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig {
                enable: true,
                delay: 0,
                delay_extra: 0,
                max_mbps: 0,
                try_cnt: 1,
                block_size: 4096,
                concurrency: 1,
                max_inflight_blocks: 16,
                max_concurrent_files: 8,
            }),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        service
            .cached_file_stats("initialize-runtime")
            .await
            .expect("initialize remote runtime");
        service.set_remote_mode_direct_for_test();
        let remote_urls: Vec<_> = image_cfg
            .lowers
            .iter()
            .map(|layer| format!("{}/{}", image_cfg.repo_blob_url, layer.digest))
            .collect();
        let image = ImageFile::open(image_cfg.clone(), service.clone(), None)
            .await
            .expect("open remote image");

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut complete = true;
                for remote_url in &remote_urls {
                    let stats = service
                        .cached_file_stats(remote_url)
                        .await
                        .expect("read cache stats")
                        .expect("remote layer cache entry");
                    complete &= stats.bytes_used >= blob.len() as u64;
                }
                if complete {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("background download batch did not fill both remote layer caches");

        for layer in &image_cfg.lowers {
            let commit = Path::new(&layer.dir).join(COMMIT_FILE_NAME);
            assert!(
                !commit.exists(),
                "cache background download must not create a layer commit file"
            );
        }

        let offset = 512 * 1024;
        let before_foreground_read = state.data_bytes.load(AtomicOrdering::Relaxed);
        let later = image
            .read_at(offset as u64, 4096)
            .await
            .expect("read background-completed block from foreground cache");
        assert_eq!(later.as_ref(), &payload[offset..offset + 4096]);
        assert_eq!(
            state.data_bytes.load(AtomicOrdering::Relaxed),
            before_foreground_read,
            "foreground read should reuse the background-completed cache block"
        );
        handle.abort();

        let later = image
            .read_at(offset as u64, 4096)
            .await
            .expect("read cached block after remote source shutdown");
        assert_eq!(later.as_ref(), &payload[offset..offset + 4096]);
    }

    #[tokio::test]
    async fn test_image_file_open_registers_all_layers_under_execution_pressure() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("remote-lower.data");
        let lower_index = tmp.path().join("remote-lower.index");
        let payload = vec![7u8; 1024 * 1024];
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build remote lower blob");
        let blob = std::fs::read(&lower_path).expect("read remote lower blob");
        let digest = digest_of(&blob);
        let state = RemoteLayerState {
            blob: Arc::new(blob.clone()),
            data_bytes: Arc::new(AtomicUsize::new(0)),
            digest: digest.clone(),
        };
        let app = Router::new()
            .route("/{*path}", any(handle_remote_request))
            .route("/token", get(handle_token))
            .with_state(state);
        let (base, handle) = spawn_server(app).await;

        // More layers than the scheduler's file-slot cap, all held pending by
        // a long delay: the scheduler must register every layer (the old
        // bounded queue silently skipped the whole batch when full).
        let layer_count = 12;
        let repo_blob_url = format!("{base}/v2/ns/repo/blobs");
        let lowers: Vec<LayerConfig> = (0..layer_count)
            .map(|index| LayerConfig {
                dir: tmp
                    .path()
                    .join(format!("saturated-layer-{index}"))
                    .to_string_lossy()
                    .into_owned(),
                digest: format!("{digest}-{index}"),
                size: blob.len() as u64,
                ..LayerConfig::default()
            })
            .collect();
        let image_cfg = ImageConfig {
            repo_blob_url: repo_blob_url.clone(),
            lowers,
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig {
                enable: true,
                delay: 3600,
                delay_extra: 0,
                max_mbps: 0,
                try_cnt: 1,
                block_size: 4096,
                concurrency: 1,
                max_inflight_blocks: 16,
                max_concurrent_files: 8,
            }),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg.clone(), service.clone(), None)
            .await
            .expect("open remote image must succeed under execution pressure");

        let cache = service
            .file_cache_for_test()
            .await
            .expect("remote runtime")
            .expect("file cache");
        for layer in &image_cfg.lowers {
            let remote_url = format!("{}/{}", repo_blob_url, layer.digest);
            let cache_id = encode_hex(&Sha256::digest(remote_url.as_bytes()));
            assert!(
                cache.bk_download_registered(&cache_id),
                "every layer must stay registered; submission never skips"
            );
        }
        cache.shutdown_bk_downloads().await;

        // Foreground reads still refill from the origin on demand.
        let data = image.read_at(0, 4096).await.expect("foreground read");
        assert_eq!(data.as_ref(), &payload[..4096]);
        handle.abort();
    }

    #[tokio::test]
    async fn test_image_file_record_trace_disables_background_download() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("remote-lower.data");
        let lower_index = tmp.path().join("remote-lower.index");
        let trace_path = tmp.path().join("record.trace");
        let mut seed = 0xfedc_ba98_7654_3210u64;
        let payload: Vec<u8> = (0..(1024 * 1024))
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect();
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build remote lower blob");
        std::fs::write(&trace_path, []).expect("create empty trace file");

        let blob = std::fs::read(&lower_path).expect("read remote lower blob");
        let digest = digest_of(&blob);
        let state = RemoteLayerState {
            blob: Arc::new(blob),
            data_bytes: Arc::new(AtomicUsize::new(0)),
            digest: digest.clone(),
        };
        let remote_size = state.blob.len() as u64;
        let app = Router::new()
            .route("/{*path}", any(handle_remote_request))
            .route("/token", get(handle_token))
            .with_state(state.clone());
        let (base, handle) = spawn_server(app).await;

        let image_cfg = ImageConfig {
            repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
            lowers: vec![LayerConfig {
                dir: tmp
                    .path()
                    .join("record-download-layer")
                    .to_string_lossy()
                    .into_owned(),
                digest: digest.clone(),
                size: remote_size,
                ..LayerConfig::default()
            }],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig {
                enable: true,
                delay: 0,
                delay_extra: 1,
                max_mbps: 0,
                try_cnt: 1,
                block_size: 4096,
                concurrency: 1,
                max_inflight_blocks: 16,
                max_concurrent_files: 8,
            }),
            acceleration_layer: false,
            record_trace_path: trace_path.to_string_lossy().into_owned(),
        };

        let service = build_service(&tmp).await;
        let remote_url = format!("{}/{}", image_cfg.repo_blob_url, digest);
        let image = ImageFile::open(image_cfg.clone(), service.clone(), None)
            .await
            .expect("open image with record trace");

        let first = image.read_at(0, 4096).await.expect("read first block");
        assert_eq!(first.as_ref(), &payload[..4096]);
        let cached_before_wait = service
            .cached_file_stats(&remote_url)
            .await
            .expect("read cache stats")
            .expect("remote layer cache entry")
            .bytes_used;

        sleep(Duration::from_millis(500)).await;
        let cached_after_wait = service
            .cached_file_stats(&remote_url)
            .await
            .expect("read cache stats")
            .expect("remote layer cache entry")
            .bytes_used;
        assert_eq!(cached_after_wait, cached_before_wait);
        assert!(cached_after_wait < remote_size);
        let commit = Path::new(&image_cfg.lowers[0].dir).join(COMMIT_FILE_NAME);
        assert!(
            !commit.exists(),
            "background download should be skipped while recording trace"
        );

        drop(image);
        handle.abort();

        let trace_size = std::fs::metadata(&trace_path)
            .expect("trace metadata")
            .len();
        assert!(trace_size > 0);
        assert!(PathBuf::from(format!("{}.ok", trace_path.display())).exists());
        assert!(state.data_bytes.load(AtomicOrdering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_image_file_acceleration_layer_ignores_last_lower() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let payload = vec![0x6b; 8192];
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build sealed lower");

        let accel_dir = tmp.path().join("accel");
        std::fs::create_dir_all(&accel_dir).expect("create accel dir");
        let trace_path = accel_dir.join("trace");
        std::fs::write(&trace_path, []).expect("create accel trace");
        let prefetcher = new_prefetcher(&trace_path, 1).expect("create record prefetcher");
        drop(prefetcher);

        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![
                LayerConfig {
                    file: lower_path.to_string_lossy().into_owned(),
                    ..LayerConfig::default()
                },
                LayerConfig {
                    dir: accel_dir.to_string_lossy().into_owned(),
                    ..LayerConfig::default()
                },
            ],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: true,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open image with acceleration layer");
        let got = image.read_at(0, payload.len()).await.expect("read image");
        assert_eq!(got.as_ref(), payload.as_slice());
        assert!(image.is_read_only().await);
    }

    #[tokio::test]
    async fn test_image_file_refreshes_size_metadata_after_size_probe() {
        let tmp = TempDir::new().expect("tempdir");
        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        create_initialized_upper(&upper_data, &upper_index, 4096)
            .await
            .expect("build initialized upper");

        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: Vec::new(),
            upper: UpperConfig {
                mode: None,
                index: upper_index.to_string_lossy().into_owned(),
                data: upper_data.to_string_lossy().into_owned(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open image");
        assert_eq!(image.size_bytes(), 4096);
        assert_eq!(image.num_lbas(), 8);

        image.update_size_metadata(0);
        assert_eq!(image.size_bytes(), 0);
        assert_eq!(image.num_lbas(), 0);

        assert_eq!(image.size().await.expect("refresh size"), 4096);
        assert_eq!(image.size_bytes(), 4096);
        assert_eq!(image.num_lbas(), 8);
    }

    /// Mocked remote-path test: ImageService::create_image_file -> mocked registry ->
    /// LSMT index parsing -> data read verification -> upper layer write -> overlay
    /// read-back. This is not the live registry/download-to-local E2E test.
    #[tokio::test]
    async fn test_e2e_remote_registry_lsmt_read_write() {
        let tmp = TempDir::new().expect("tempdir");

        // --- build a sealed LSMT lower layer blob ---
        let lower_path = tmp.path().join("e2e-lower.data");
        let lower_index = tmp.path().join("e2e-lower.index");
        let payload: Vec<u8> = (0..32768).map(|i| ((i * 13 + 7) % 251) as u8).collect();
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build sealed lower");
        let blob = std::fs::read(&lower_path).expect("read lower blob");
        let digest = digest_of(&blob);

        // --- mock registry server ---
        let state = RemoteLayerState {
            blob: Arc::new(blob.clone()),
            data_bytes: Arc::new(AtomicUsize::new(0)),
            digest: digest.clone(),
        };
        let app = Router::new()
            .route("/{*path}", any(handle_remote_request))
            .route("/token", get(handle_token))
            .with_state(state.clone());
        let (base, server_handle) = spawn_server(app).await;

        // --- write image config JSON consumed by create_image_file ---
        let image_path = tmp.path().join("e2e-image.json");
        let result_path = tmp.path().join("e2e-result.txt");
        let upper_data = tmp.path().join("e2e-upper.data");
        let upper_index = tmp.path().join("e2e-upper.index");
        create_initialized_upper(&upper_data, &upper_index, payload.len() as u64)
            .await
            .expect("build upper");

        write_json(
            &image_path,
            &json!({
                "repoBlobUrl": format!("{base}/v2/ns/repo/blobs"),
                "lowers": [{
                    "digest": digest,
                    "size": blob.len() as u64
                }],
                "upper": {
                    "index": upper_index,
                    "data": upper_data
                },
                "resultFile": result_path,
                "download": {
                    "enable": false,
                    "delay": 0,
                    "delayExtra": 0,
                    "maxMBps": 0,
                    "tryCnt": 1,
                    "blockSize": 4096
                }
            }),
        );

        // --- create ImageService and open image via the full path ---
        let service = build_service(&tmp).await;
        let image = service
            .create_image_file(&image_path)
            .await
            .expect("create_image_file e2e");

        // result file should say "success"
        let result = std::fs::read_to_string(&result_path).expect("read result");
        assert_eq!(result, "success");

        // remote data hits must have occurred (LSMT trailer + index + data reads)
        assert!(
            state.data_bytes.load(AtomicOrdering::Relaxed) > 0,
            "no remote data reads occurred"
        );

        // --- verify LSMT read path: data matches original payload ---
        let got = image.read_at(0, payload.len()).await.expect("read lower");
        assert_eq!(got.as_ref(), &payload[..]);

        // partial read in the middle
        let mid: u64 = 8192;
        let mid_len = 4096;
        let mid_got = image.read_at(mid, mid_len).await.expect("read mid");
        assert_eq!(
            mid_got.as_ref(),
            &payload[mid as usize..(mid as usize + mid_len)]
        );

        // --- verify write path: overlay upper layer ---
        assert!(
            !image.is_read_only().await,
            "image should be read-write with upper"
        );
        let overlay = vec![0xBB; 4096];
        image.write_at(0, &overlay).await.expect("write overlay");

        // read back overlay region — should see the written data
        let got_overlay = image.read_at(0, 4096).await.expect("read overlay");
        assert_eq!(got_overlay.as_ref(), overlay.as_slice());

        // read beyond overlay — should still see original lower data
        let got_lower = image
            .read_at(4096, 4096)
            .await
            .expect("read lower after overlay");
        assert_eq!(got_lower.as_ref(), &payload[4096..8192]);

        // size should match the original virtual size
        let size = image.size().await.expect("image size");
        assert_eq!(size, payload.len() as u64);

        drop(image);
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_image_file_rejects_unmigrated_warp_fields() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        create_sealed_lower(&lower_path, &lower_index, &[0x31; 4096])
            .await
            .expect("build sealed lower");

        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                target_file: "/tmp/target".to_string(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service(&tmp).await;
        let err = ImageFile::open(image_cfg, service, None)
            .await
            .expect_err("warp path should be unsupported");
        assert!(
            format!("{err:?}").contains("not migrated"),
            "expected unsupported/not-migrated error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_open_localfile_prefers_commit_and_sealed() {
        let tmp = TempDir::new().expect("tempdir");
        let dir = TempDir::new_in(tmp.path()).expect("layer dir");
        let commit = dir.path().join(COMMIT_FILE_NAME);
        let _ = NamedTempFile::new_in(dir.path()).expect("noise file");
        std::fs::write(&commit, b"").expect("write commit");

        let layer = LayerConfig {
            dir: dir.path().to_string_lossy().into_owned(),
            ..LayerConfig::default()
        };
        let opened = ImageFile::open_localfile_path(&layer)
            .await
            .expect("open localfile path")
            .expect("commit file");
        assert_eq!(opened, commit);
    }

    #[tokio::test]
    async fn test_image_file_open_local_lower_with_libaio_io_engine() {
        let tmp = TempDir::new().expect("tempdir");
        let lower_path = tmp.path().join("lower.data");
        let lower_index = tmp.path().join("lower.index");
        let payload: Vec<u8> = (0..8192).map(|v| (v % 251) as u8).collect();
        create_sealed_lower(&lower_path, &lower_index, &payload)
            .await
            .expect("build sealed lower");

        let image_cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                file: lower_path.to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            upper: UpperConfig::default(),
            result_file: String::new(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: String::new(),
        };

        let service = build_service_with_io_engine(&tmp, IO_ENGINE_LIBAIO).await;
        let image = ImageFile::open(image_cfg, service, None)
            .await
            .expect("open image with libaio");

        let got = image.read_at(512, 1024).await.expect("direct-io read");
        assert_eq!(got.as_ref(), &payload[512..1536]);
    }
}
