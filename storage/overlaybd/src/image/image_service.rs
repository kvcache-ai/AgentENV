use std::fmt;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::backend::cache::{
    BkDownloadSubmitError, CacheFnTransFunc, CachedFile, FileCacheBackend, FileCacheBackendOptions,
};
use crate::backend::local::LocalFile;
use crate::backend::oss::OssBackend;
use crate::backend::registryfs_v2::RegistryFsV2;
use crate::config::{
    load_global_config, resolve_image_config_local_paths, validate_image_config, DownloadConfig,
    GlobalConfig, ImageConfig,
};
use crate::image::image_file::ImageFile;
use crate::io::virtual_file::VirtualFile;
use crate::lsmt::file::CommitArgs;
use anyhow::{bail, Context, Result};
use tokio::sync::OnceCell;
use uuid::Uuid;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RemoteOpenMode {
    Direct,
    Cached,
}

struct ImageServiceInner {
    config_path: PathBuf,
    global_config: GlobalConfig,
    p2p_publish_url: Option<String>,
    remote_runtime: OnceCell<RemoteRuntime>,
    remote_mode: parking_lot::RwLock<RemoteOpenMode>,
}

struct RemoteRuntime {
    underlay_registryfs: RegistryFsV2,
    oss_backend: Option<OssBackend>,
    file_cache: Option<FileCacheBackend>,
}

pub(crate) struct CacheDownloadRequest {
    file: Arc<CachedFile>,
    config: DownloadConfig,
}

impl fmt::Debug for ImageServiceInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageServiceInner")
            .field("config_path", &self.config_path)
            .field("global_config", &self.global_config)
            .field("p2p_publish_enabled", &self.p2p_publish_url.is_some())
            .field(
                "remote_runtime_initialized",
                &self.remote_runtime.get().is_some(),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ImageService {
    inner: Arc<ImageServiceInner>,
}

impl fmt::Debug for ImageService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageService")
            .field("config_path", &self.inner.config_path)
            .field("global_config", &self.inner.global_config)
            .finish_non_exhaustive()
    }
}

impl ImageService {
    async fn with_global_config(global_config: GlobalConfig, config_path: PathBuf) -> Result<Self> {
        Self::with_global_config_and_p2p_publish_url(global_config, config_path, None).await
    }

    async fn with_global_config_and_p2p_publish_url(
        global_config: GlobalConfig,
        config_path: PathBuf,
        p2p_publish_url: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(ImageServiceInner {
                config_path,
                global_config,
                p2p_publish_url,
                remote_runtime: OnceCell::new(),
                remote_mode: parking_lot::RwLock::new(RemoteOpenMode::Cached),
            }),
        })
    }

    /// Creates an `ImageService` from an in-memory [`GlobalConfig`].
    pub async fn new(global_config: GlobalConfig) -> Result<Self> {
        Self::with_global_config(global_config, PathBuf::new()).await
    }

    pub async fn from_config_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let global_config = load_global_config(&path)?;
        Self::with_global_config(global_config, path).await
    }

    pub async fn from_config_path_with_p2p_publish_url(
        path: impl AsRef<Path>,
        p2p_publish_url: Option<String>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let global_config = load_global_config(&path)?;
        Self::with_global_config_and_p2p_publish_url(global_config, path, p2p_publish_url).await
    }

    /// Return the path from which this `ImageService` was loaded.
    ///
    /// Returns an empty path if the service was created from an in-memory
    /// [`GlobalConfig`] via [`ImageService::new`].
    pub fn config_path(&self) -> &Path {
        &self.inner.config_path
    }

    async fn build_file_cache(cfg: &GlobalConfig) -> Result<Option<FileCacheBackend>> {
        match cfg.cache_config.cache_type.as_str() {
            "" | "file" => {
                std::fs::create_dir_all(&cfg.cache_config.cache_dir)?;
                let mut options = FileCacheBackendOptions::from_cache_config(&cfg.cache_config)?;
                // The cache-owned background download scheduler takes its
                // node-level caps from the global download config; per-image
                // overrides never resize them.
                options.bk_download_max_inflight_blocks = cfg.download.max_inflight_blocks;
                options.bk_download_max_concurrent_files = cfg.download.max_concurrent_files;
                options.bk_download_block_size = cfg.download.block_size;
                let basename_transform: CacheFnTransFunc = Arc::new(|origin| {
                    let basename = Path::new(origin)
                        .file_name()
                        .and_then(|v| v.to_str())
                        .filter(|v| !v.is_empty())?;
                    Some(format!("/{basename}"))
                });
                let backend = FileCacheBackend::with_options_and_trans_func(
                    options,
                    Some(basename_transform),
                )
                .await?;
                Ok(Some(backend))
            }
            "ocf" | "download" => bail!(
                "cache type {} is not migrated in Rust image_service yet",
                cfg.cache_config.cache_type
            ),
            other => bail!("unknown cache type: {other}"),
        }
    }

    fn current_accelerate_address(&self) -> String {
        let remote_mode = *self.inner.remote_mode.read();
        if remote_mode == RemoteOpenMode::Direct {
            self.inner.global_config.p2p_config.address.clone()
        } else {
            String::new()
        }
    }

    async fn build_remote_runtime(&self) -> Result<RemoteRuntime> {
        let underlay_registryfs = RegistryFsV2::from_global_config(&self.inner.global_config)?;
        underlay_registryfs.set_accelerate_address(self.current_accelerate_address());
        let oss_backend = if self.inner.global_config.oss_config.enable {
            Some(OssBackend::new(&self.inner.global_config.oss_config)?)
        } else {
            None
        };
        let file_cache = Self::build_file_cache(&self.inner.global_config).await?;

        Ok(RemoteRuntime {
            underlay_registryfs,
            oss_backend,
            file_cache,
        })
    }

    async fn remote_runtime(&self) -> Result<&RemoteRuntime> {
        let service = self.clone();
        self.inner
            .remote_runtime
            .get_or_try_init(move || async move { service.build_remote_runtime().await })
            .await
    }

    pub fn global_config(&self) -> &GlobalConfig {
        &self.inner.global_config
    }

    pub fn io_engine(&self) -> u32 {
        self.inner.global_config.io_engine
    }

    #[cfg(test)]
    pub(crate) async fn cached_file_stats(
        &self,
        source: &str,
    ) -> Result<Option<crate::backend::cache::CachedFileStats>> {
        Ok(self
            .remote_runtime()
            .await?
            .file_cache
            .as_ref()
            .and_then(|cache| cache.file_stats(source)))
    }

    #[cfg(test)]
    pub(crate) async fn file_cache_for_test(&self) -> Result<Option<FileCacheBackend>> {
        Ok(self.remote_runtime().await?.file_cache.clone())
    }

    #[cfg(test)]
    pub(crate) fn set_remote_mode_direct_for_test(&self) {
        *self.inner.remote_mode.write() = RemoteOpenMode::Direct;
    }

    pub(crate) fn p2p_uuid_address(&self) -> Option<String> {
        let p2p = &self.inner.global_config.p2p_config;
        if !p2p.enable {
            return None;
        }
        p2p.address
            .trim_end_matches('/')
            .strip_suffix("/p2p-http")
            .map(|base| format!("{base}/p2p-uuid"))
    }

    pub fn load_image_config(&self, path: impl AsRef<Path>) -> Result<ImageConfig> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)?;
        let mut cfg: ImageConfig =
            serde_json::from_str(&raw).context("parse image config json failed")?;
        resolve_image_config_local_paths(path, &mut cfg);
        validate_image_config(&cfg)?;
        Ok(cfg)
    }

    pub async fn create_image_file(&self, path: impl AsRef<Path>) -> Result<ImageFile> {
        let device_key =
            std::fs::canonicalize(path.as_ref()).unwrap_or_else(|_| path.as_ref().to_path_buf());
        let image_config = self.load_image_config(path)?;
        let result_file = image_config.result_file.clone();

        let _ = self.enable_acceleration();

        match ImageFile::open(image_config, self.clone(), Some(device_key)).await {
            Ok(image) => {
                self.set_result_file(&result_file, "success")?;
                Ok(image)
            }
            Err(err) => {
                let _ = self.set_result_file(&result_file, &format!("failed:{err}"));
                Err(err)
            }
        }
    }

    pub fn enable_acceleration(&self) -> bool {
        let p2p = &self.inner.global_config.p2p_config;
        let accelerate_address = if p2p.enable && check_accelerate_url(&p2p.address) {
            *self.inner.remote_mode.write() = RemoteOpenMode::Direct;
            p2p.address.clone()
        } else {
            *self.inner.remote_mode.write() = RemoteOpenMode::Cached;
            String::new()
        };

        if let Some(remote_runtime) = self.inner.remote_runtime.get() {
            remote_runtime
                .underlay_registryfs
                .set_accelerate_address(accelerate_address.clone());
        }

        !accelerate_address.is_empty()
    }

    pub async fn open_remote_blob_with_size(
        &self,
        url: &str,
        source_size: Option<u64>,
    ) -> Result<Arc<dyn VirtualFile>> {
        let remote_runtime = self.remote_runtime().await?;
        let source = self.open_backend_source_with_size(url, source_size).await?;
        // OSS blobs always go through the file cache (when available) regardless
        // of RemoteOpenMode. RemoteOpenMode::Direct is only meaningful for the
        // P2P accelerator path, which acts as its own cache; OSS has no P2P
        // channel, so we always want the local file cache as a read-ahead layer.
        if Self::is_oss_url(url) {
            if let Some(cache) = remote_runtime.file_cache.as_ref() {
                let cache_file = Self::open_cached_blob(cache, url, source, source_size).await?;
                return Ok(cache_file);
            }
            return Ok(source);
        }
        let remote_mode = *self.inner.remote_mode.read();
        match remote_mode {
            RemoteOpenMode::Direct => Ok(source),
            RemoteOpenMode::Cached => {
                if let Some(cache) = remote_runtime.file_cache.as_ref() {
                    let cache_file =
                        Self::open_cached_blob(cache, url, source, source_size).await?;
                    Ok(cache_file)
                } else {
                    Ok(source)
                }
            }
        }
    }

    pub(crate) async fn open_remote_blob_for_bk_download_with_size(
        &self,
        url: &str,
        source_size: Option<u64>,
        config: DownloadConfig,
    ) -> Result<(Arc<dyn VirtualFile>, CacheDownloadRequest)> {
        let remote_runtime = self.remote_runtime().await?;
        let cache = remote_runtime
            .file_cache
            .as_ref()
            .context("background download requires a file cache backend")?;
        let source = self.open_backend_source_with_size(url, source_size).await?;
        let cache_file = Self::open_cached_blob(cache, url, source, source_size).await?;
        Ok((
            cache_file.clone(),
            CacheDownloadRequest {
                file: cache_file,
                config,
            },
        ))
    }

    /// Submit background downloads for freshly opened layers.
    ///
    /// Background download is a best-effort accelerator: submission is
    /// registered with the cache scheduler and never fails due to execution
    /// pressure, so a busy scheduler cannot make an image open skip
    /// background download — tasks simply run later. A shut-down scheduler is
    /// skipped with a warning (foreground `CachedFile` reads refill missing
    /// blocks from the origin on demand); a missing file-cache backend is a
    /// configuration error and still fails.
    pub(crate) async fn submit_bk_downloads(
        &self,
        requests: Vec<CacheDownloadRequest>,
        device_key: Option<PathBuf>,
    ) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        let remote_runtime = self.remote_runtime().await?;
        let cache = remote_runtime
            .file_cache
            .as_ref()
            .context("background download requires a file cache backend")?;
        let request_count = requests.len();
        let result = cache.submit_bk_download_batch(
            requests
                .into_iter()
                .map(|request| (request.file, request.config, device_key.clone()))
                .collect(),
        );
        match result {
            Ok(()) => Ok(()),
            Err(error) => match error.downcast_ref::<BkDownloadSubmitError>() {
                Some(submit_error) => {
                    // Log only the fixed category and the device key basename;
                    // the full path can expose tenant/host metadata.
                    let device_name = device_key
                        .as_ref()
                        .and_then(|key| key.file_name().map(|name| name.to_string_lossy()));
                    tracing::warn!(
                        error_category = submit_error.category(),
                        request_count,
                        device_key = device_name.as_deref().unwrap_or(""),
                        "skipping background download submission; foreground reads refill from origin"
                    );
                    Ok(())
                }
                None => Err(error).context("submit background downloads"),
            },
        }
    }

    async fn open_cached_blob(
        cache: &FileCacheBackend,
        url: &str,
        source: Arc<dyn VirtualFile>,
        source_size: Option<u64>,
    ) -> Result<Arc<CachedFile>> {
        let source_size = match source_size {
            Some(size) => size,
            None => source.size().await?,
        };
        cache
            .open_file_with_source_size(url.to_string(), source, source_size)
            .await
    }

    pub async fn open_remote_blob(&self, url: &str) -> Result<Arc<dyn VirtualFile>> {
        self.open_remote_blob_with_size(url, None).await
    }

    pub async fn open_source_blob(&self, url: &str) -> Result<Arc<dyn VirtualFile>> {
        self.open_source_blob_with_size(url, None).await
    }

    pub(crate) async fn open_source_blob_with_size(
        &self,
        url: &str,
        source_size: Option<u64>,
    ) -> Result<Arc<dyn VirtualFile>> {
        self.open_backend_source_with_size(url, source_size).await
    }

    fn is_oss_url(url: &str) -> bool {
        match reqwest::Url::parse(url) {
            Ok(parsed) => matches!(parsed.scheme(), "s3" | "oss"),
            Err(_) => false,
        }
    }

    async fn open_backend_source_with_size(
        &self,
        url: &str,
        source_size: Option<u64>,
    ) -> Result<Arc<dyn VirtualFile>> {
        let remote_runtime = self.remote_runtime().await?;
        if Self::is_oss_url(url) {
            let oss = remote_runtime
                .oss_backend
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("OSS backend not enabled in config"))?;
            return oss.open_with_size_hint(url, source_size);
        }

        match source_size {
            Some(size) => Ok(remote_runtime
                .underlay_registryfs
                .open_with_size_hint(url.to_string(), Some(size))),
            None => {
                remote_runtime
                    .underlay_registryfs
                    .open(url.to_string())
                    .await
            }
        }
    }

    pub async fn export_upper_as_oss_sealed(
        &self,
        image: &ImageFile,
        dest_url: &str,
    ) -> Result<()> {
        if !Self::is_oss_url(dest_url) {
            bail!("destination url must use oss:// or s3://");
        }
        let remote_runtime = self.remote_runtime().await?;
        let oss = remote_runtime
            .oss_backend
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("OSS backend not enabled in config"))?;

        let stage_dir =
            Path::new(&self.inner.global_config.cache_config.cache_dir).join("oss-stage");
        std::fs::create_dir_all(&stage_dir)
            .with_context(|| format!("create oss stage dir {}", stage_dir.display()))?;
        let stage_path = stage_dir.join(format!("{}.lsmt", Uuid::new_v4()));
        let stage_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&stage_path)?);

        let export_result = image
            .export_upper_as_sealed(CommitArgs::new(stage_file.clone()))
            .await;
        if let Err(err) = export_result {
            let _ = tokio::fs::remove_file(&stage_path).await;
            return Err(err);
        }

        stage_file.sync().await?;
        let upload_result = oss.upload_path(dest_url, &stage_path).await;
        // Always clean up the staging file regardless of upload outcome.
        // The staging file is a full copy of the sealed upper layer and can
        // be large; leaving it on disk across failures would accumulate waste.
        let _ = tokio::fs::remove_file(&stage_path).await;
        upload_result
    }

    fn set_result_file(&self, filename: &str, data: &str) -> Result<()> {
        if filename.is_empty() {
            return Ok(());
        }
        if let Some(parent) = Path::new(filename).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        Ok(std::fs::write(filename, data.as_bytes())?)
    }
}

fn check_accelerate_url(address: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(address) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    let port = url.port_or_known_default().unwrap_or(80);

    let addrs: Vec<SocketAddr> = match (host, port).to_socket_addrs() {
        Ok(v) => v.collect(),
        Err(_) => return false,
    };

    addrs
        .into_iter()
        .any(|addr| TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::{Request, State};
    use axum::http::header::CONTENT_RANGE as CONTENT_RANGE_RAW;
    use axum::http::{HeaderMap as HttpHeaderMap, Response, StatusCode as HttpStatusCode};
    use axum::routing::any;
    use axum::Router;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

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

    #[derive(Clone, Debug)]
    struct OssObjectState {
        blob: Arc<Vec<u8>>,
    }

    async fn handle_oss_object(
        State(state): State<OssObjectState>,
        request: Request,
    ) -> Response<Body> {
        let headers = request.headers().clone();
        let body = state.blob.as_slice();
        let len = body.len() as u64;

        match *request.method() {
            axum::http::Method::HEAD => Response::builder()
                .status(HttpStatusCode::OK)
                .header(reqwest::header::CONTENT_LENGTH, len.to_string())
                .body(Body::empty())
                .expect("head response"),
            axum::http::Method::GET => {
                if let Some((start, end)) = parse_request_range(&headers) {
                    let start = start.min(len.saturating_sub(1));
                    let end = end.min(len.saturating_sub(1));
                    let chunk = body[start as usize..=end as usize].to_vec();
                    Response::builder()
                        .status(HttpStatusCode::PARTIAL_CONTENT)
                        .header(CONTENT_RANGE_RAW, format!("bytes {start}-{end}/{len}"))
                        .header(reqwest::header::CONTENT_LENGTH, chunk.len().to_string())
                        .body(Body::from(chunk))
                        .expect("range response")
                } else {
                    Response::builder()
                        .status(HttpStatusCode::OK)
                        .header(reqwest::header::CONTENT_LENGTH, len.to_string())
                        .body(Body::from(body.to_vec()))
                        .expect("get response")
                }
            }
            _ => Response::builder()
                .status(HttpStatusCode::METHOD_NOT_ALLOWED)
                .body(Body::empty())
                .expect("405 response"),
        }
    }

    async fn handle_aliyun_oss_object(
        State(state): State<OssObjectState>,
        request: Request,
    ) -> Response<Body> {
        let headers = request.headers();
        let auth = headers
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let payload_header = headers
            .get("x-amz-content-sha256")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let signed_headers_ok = if request.method() == axum::http::Method::GET {
            auth.contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date")
        } else {
            auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date")
        };
        if !auth.starts_with("AWS4-HMAC-SHA256 ")
            || payload_header != "UNSIGNED-PAYLOAD"
            || !signed_headers_ok
        {
            return Response::builder()
                .status(HttpStatusCode::FORBIDDEN)
                .body(Body::from("invalid aws sigv4 headers for aliyun oss"))
                .expect("403 response");
        }
        handle_oss_object(State(state), request).await
    }

    async fn handle_oss_get_only_object(
        State(state): State<OssObjectState>,
        request: Request,
    ) -> Response<Body> {
        if request.method() == axum::http::Method::HEAD {
            return Response::builder()
                .status(HttpStatusCode::METHOD_NOT_ALLOWED)
                .body(Body::from("head should not be called"))
                .expect("405 response");
        }
        handle_oss_object(State(state), request).await
    }

    #[tokio::test]
    async fn test_load_image_config_keeps_download_override_absent() {
        let tmp = TempDir::new().expect("tempdir");
        let global_path = tmp.path().join("overlaybd.json");
        let image_path = tmp.path().join("image.json");

        write_json(
            &global_path,
            &serde_json::json!({
                "registryFsVersion": "v2",
                "ioEngine": 0,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "download": {
                    "enable": true,
                    "delay": 12,
                    "delayExtra": 7,
                    "tryCnt": 9,
                    "blockSize": 131072
                }
            }),
        );

        write_json(
            &image_path,
            &serde_json::json!({
                "repoBlobUrl": "https://registry.example/v2/ns/repo/blobs",
                "lowers": [
                    {
                        "file": "/tmp/lower.data"
                    }
                ]
            }),
        );

        let service = ImageService::from_config_path(&global_path)
            .await
            .expect("service");
        let cfg = service.load_image_config(&image_path).expect("image cfg");
        let download = cfg.effective_download(service.global_config());

        assert!(cfg.download_override.is_none());
        assert!(download.enable);
        assert_eq!(download.delay, 12);
        assert_eq!(download.delay_extra, 7);
        assert_eq!(download.try_cnt, 9);
        assert_eq!(download.block_size, 131072);
    }

    #[tokio::test]
    async fn test_set_result_file_writes_status() {
        let tmp = TempDir::new().expect("tempdir");
        let global_path = tmp.path().join("overlaybd.json");
        let result_path = tmp.path().join("result.txt");

        write_json(
            &global_path,
            &serde_json::json!({
                "registryFsVersion": "v2",
                "ioEngine": 0,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                }
            }),
        );

        let service = ImageService::from_config_path(&global_path)
            .await
            .expect("service");
        service
            .set_result_file(result_path.to_string_lossy().as_ref(), "success")
            .expect("write result");

        let raw = std::fs::read_to_string(result_path).expect("read result");
        assert_eq!(raw, "success");
    }

    #[tokio::test]
    async fn test_create_image_file_with_local_upper_skips_eager_remote_init() {
        let tmp = TempDir::new().expect("tempdir");
        let global_path = tmp.path().join("overlaybd.json");
        let image_path = tmp.path().join("image.json");
        let upper_data = tmp.path().join("upper.data");
        let upper_index = tmp.path().join("upper.index");
        let result_path = tmp.path().join("result.txt");

        crate::helper::prepare_runtime_upper(
            &upper_data,
            Some(&upper_index),
            8192,
            crate::config::UpperMode::LogStructured,
        )
        .expect("prepare runtime upper");

        write_json(
            &global_path,
            &serde_json::json!({
                "registryFsVersion": "v2",
                "ioEngine": 0,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "certConfig": {
                    "certFile": tmp.path().join("missing-cert.pem"),
                    "keyFile": tmp.path().join("missing-key.pem")
                }
            }),
        );

        write_json(
            &image_path,
            &serde_json::json!({
                "upper": {
                    "data": upper_data,
                    "index": upper_index
                },
                "resultFile": result_path
            }),
        );

        let service = ImageService::from_config_path(&global_path)
            .await
            .expect("local-only service should not initialize remote runtime eagerly");
        let image = service
            .create_image_file(&image_path)
            .await
            .expect("local-only image should open without remote runtime");

        assert_eq!(image.size().await.expect("image size"), 8192);
    }

    #[tokio::test]
    async fn test_open_remote_blob_with_s3_url_reads_object() {
        let tmp = TempDir::new().expect("tempdir");
        let global_path = tmp.path().join("overlaybd.json");
        let object = b"hello from oss".to_vec();
        let app = Router::new()
            .route("/test-bucket/layers/lower", any(handle_oss_object))
            .with_state(OssObjectState {
                blob: Arc::new(object.clone()),
            });
        let (endpoint, server_handle) = spawn_server(app).await;

        write_json(
            &global_path,
            &serde_json::json!({
                "registryFsVersion": "v2",
                "ioEngine": 0,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "ossConfig": {
                    "enable": true,
                    "accessKeyId": "minioadmin",
                    "secretAccessKey": "minioadmin",
                    "defaultRegion": "us-east-1",
                    "defaultEndpoint": endpoint
                }
            }),
        );

        let service = ImageService::from_config_path(&global_path)
            .await
            .expect("service");
        let url = format!(
            "s3://test-bucket/layers/lower?endpoint={}&region=us-east-1",
            endpoint
        );

        let file = service
            .open_remote_blob_with_size(&url, None)
            .await
            .expect("open remote blob");
        let got = file.read_at(0, object.len()).await.expect("read object");
        assert_eq!(&got[..], object.as_slice());

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_open_source_blob_with_size_hint_skips_head_for_oss() {
        let tmp = TempDir::new().expect("tempdir");
        let global_path = tmp.path().join("overlaybd.json");
        let object = b"hello hinted source blob".to_vec();
        let app = Router::new()
            .route(
                "/test-bucket/source/hinted",
                any(handle_oss_get_only_object),
            )
            .with_state(OssObjectState {
                blob: Arc::new(object.clone()),
            });
        let (endpoint, server_handle) = spawn_server(app).await;

        write_json(
            &global_path,
            &serde_json::json!({
                "registryFsVersion": "v2",
                "ioEngine": 0,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "ossConfig": {
                    "enable": true,
                    "accessKeyId": "minioadmin",
                    "secretAccessKey": "minioadmin",
                    "defaultRegion": "us-east-1",
                    "defaultEndpoint": endpoint
                }
            }),
        );

        let service = ImageService::from_config_path(&global_path)
            .await
            .expect("service");
        let url = format!(
            "s3://test-bucket/source/hinted?endpoint={}&region=us-east-1",
            endpoint
        );

        let file = service
            .open_source_blob_with_size(&url, Some(object.len() as u64))
            .await
            .expect("open source blob with size");
        let got = file.read_at(0, object.len()).await.expect("read object");
        assert_eq!(&got[..], object.as_slice());

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_p2p_uuid_address_is_derived_from_http_facade_address() {
        let tmp = TempDir::new().expect("tempdir");
        let global_path = tmp.path().join("overlaybd.json");
        write_json(
            &global_path,
            &serde_json::json!({
                "registryFsVersion": "v2",
                "ioEngine": 0,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "p2pConfig": {
                    "enable": true,
                    "address": "http://127.0.0.1:9731/p2p-http/"
                }
            }),
        );

        let service = ImageService::from_config_path(&global_path)
            .await
            .expect("service");

        assert_eq!(
            service.p2p_uuid_address().as_deref(),
            Some("http://127.0.0.1:9731/p2p-uuid")
        );
    }

    #[tokio::test]
    async fn test_open_remote_blob_with_aliyun_endpoint_uses_unsigned_payload_sigv4() {
        let tmp = TempDir::new().expect("tempdir");
        let global_path = tmp.path().join("overlaybd.json");
        let object = b"hello aliyun oss".to_vec();
        let app = Router::new()
            .route(
                "/aliyun-bucket/objects/layer",
                any(handle_aliyun_oss_object),
            )
            .with_state(OssObjectState {
                blob: Arc::new(object.clone()),
            });
        let (endpoint, server_handle) = spawn_server(app).await;

        write_json(
            &global_path,
            &serde_json::json!({
                "registryFsVersion": "v2",
                "ioEngine": 0,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "ossConfig": {
                    "enable": true,
                    "accessKeyId": "aliyun-ak",
                    "secretAccessKey": "aliyun-sk",
                    "defaultRegion": "cn-hangzhou",
                    "defaultEndpoint": endpoint
                }
            }),
        );

        let service = ImageService::from_config_path(&global_path)
            .await
            .expect("service");
        let url = "oss://aliyun-bucket/objects/layer?region=cn-hangzhou".to_string();

        let file = service
            .open_remote_blob_with_size(&url, None)
            .await
            .expect("open remote blob");
        let got = file.read_at(0, object.len()).await.expect("read object");
        assert_eq!(&got[..], object.as_slice());

        server_handle.abort();
    }
}
