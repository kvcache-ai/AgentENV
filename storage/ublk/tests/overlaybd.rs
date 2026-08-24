use anyhow::{bail, Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::backend::tar::new_tar_file_adaptor;
use overlaybd::config::{
    CredentialConfig, DownloadConfig, GlobalConfig, ImageConfig, LayerConfig, UpperConfig,
};
use overlaybd::index_file::{create_file_rw, LayerInfo};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::is_zfile;
use overlaybd::{ImageFile, ImageService};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::sleep;

const ENV_IMAGE_CONFIG: &str = "OVERLAYBD_LIVE_IMAGE_CONFIG";
const ENV_GLOBAL_CONFIG: &str = "OVERLAYBD_LIVE_GLOBAL_CONFIG";
const ENV_REPO_BLOB_URL: &str = "OVERLAYBD_LIVE_REPO_BLOB_URL";
const ENV_DIGEST: &str = "OVERLAYBD_LIVE_DIGEST";
const ENV_SIZE: &str = "OVERLAYBD_LIVE_SIZE";
const ENV_RESULT_FILE: &str = "OVERLAYBD_LIVE_RESULT_FILE";
const ENV_READ_LEN: &str = "OVERLAYBD_LIVE_READ_LEN";
const ENV_READ_OFFSETS: &str = "OVERLAYBD_LIVE_READ_OFFSETS";
const ENV_EXPECT_VSIZE: &str = "OVERLAYBD_LIVE_EXPECT_VSIZE";
const ENV_EXPECT_SHA256: &str = "OVERLAYBD_LIVE_EXPECT_SHA256";
const ENV_ENABLE_DOWNLOAD: &str = "OVERLAYBD_LIVE_ENABLE_DOWNLOAD";
const ENV_WAIT_DOWNLOAD_SECS: &str = "OVERLAYBD_LIVE_WAIT_DOWNLOAD_SECS";
const ENV_CACHE_TYPE: &str = "OVERLAYBD_LIVE_CACHE_TYPE";
const ENV_CACHE_DIR: &str = "OVERLAYBD_LIVE_CACHE_DIR";
const ENV_CREDENTIAL_FILE: &str = "OVERLAYBD_LIVE_CREDENTIAL_FILE";
const ENV_CREDENTIAL_HTTP: &str = "OVERLAYBD_LIVE_CREDENTIAL_HTTP";
const ENV_CREDENTIAL_TIMEOUT: &str = "OVERLAYBD_LIVE_CREDENTIAL_TIMEOUT";
const ENV_P2P_ENABLE: &str = "OVERLAYBD_LIVE_P2P_ENABLE";
const ENV_P2P_ADDRESS: &str = "OVERLAYBD_LIVE_P2P_ADDRESS";
const ENV_USER_AGENT: &str = "OVERLAYBD_LIVE_USER_AGENT";
const ENV_SAMPLE_STRIDE: &str = "OVERLAYBD_LIVE_SAMPLE_STRIDE";

fn env_var(name: &str) -> Option<String> {
    env::var(name).ok().map(|v| v.trim().to_string())
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    match env_var(name) {
        None => Ok(default),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => bail!("invalid boolean in {name}: {value}"),
        },
    }
}

fn env_u64(name: &str) -> Result<Option<u64>> {
    match env_var(name) {
        None => Ok(None),
        Some(value) => Ok(Some(
            value
                .parse::<u64>()
                .with_context(|| format!("parse {name} as u64 failed"))?,
        )),
    }
}

fn env_usize(name: &str) -> Result<Option<usize>> {
    match env_var(name) {
        None => Ok(None),
        Some(value) => {
            Ok(Some(value.parse::<usize>().with_context(|| {
                format!("parse {name} as usize failed")
            })?))
        }
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn sample_offsets(vsize: u64, read_len: usize) -> Result<Vec<u64>> {
    if let Some(raw) = env_var(ENV_READ_OFFSETS) {
        let mut offsets = Vec::new();
        for item in raw.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            offsets.push(
                item.parse::<u64>()
                    .with_context(|| format!("parse {ENV_READ_OFFSETS} item `{item}` failed"))?,
            );
        }
        if offsets.is_empty() {
            bail!("{ENV_READ_OFFSETS} is set but produced no offsets");
        }
        return Ok(offsets);
    }

    // Default stride: 1 MiB, configurable via env
    let stride = env_u64(ENV_SAMPLE_STRIDE)?.unwrap_or(1 << 20);
    let mut offsets = BTreeSet::new();
    offsets.insert(0);
    if vsize > read_len as u64 {
        // Head / mid / tail
        offsets.insert((vsize / 2).saturating_sub((read_len as u64) / 2));
        offsets.insert(vsize.saturating_sub(read_len as u64));
        // Stride-based sampling across the virtual range
        if stride > 0 {
            let mut off = stride;
            while off + (read_len as u64) <= vsize {
                offsets.insert(off);
                off += stride;
            }
        }
    }
    Ok(offsets.into_iter().collect())
}

/// Inline default sample image config — no external file dependency.
fn sample_image_config() -> ImageConfig {
    serde_json::from_value(serde_json::json!({
        "repoBlobUrl": "https://registry-1.docker.io/v2/overlaybd/redis/blobs",
        "lowers": [
            // Keep the sample registry-backed config bandwidth-light for local runs and CI.
            // The larger lowers remain here as documentation and can be restored if needed.
            // { "digest": "sha256:a8b5fca80efae55088290f3da8110d7742de55c2a378d5ab53226a483f390e21", "size": 4739584 },
            // { "digest": "sha256:87763befd4f3289905d709bd03c969db43e512502be7e1132b625bdef487d01f", "size": 43458048 },
            { "digest": "sha256:5bf55fa8550c47a1054c7a02138b9f79b5f574f040b1e444ad717d320d3afc67", "size": 25600 },
            // { "digest": "sha256:62a999219eb529a2403f2b5849869d3253bf1014721293333f7f66be54308b94", "size": 2610688 },
            // { "digest": "sha256:f2d33f598db59a8a4fcb490764cdfca3157ec6a742870378154cbef93acefce9", "size": 17303040 },
            { "digest": "sha256:8d77203e222f30ab4b8ba2e232fd9d71880dd80f6f24fa18e45d1d578e40eb57", "size": 8192 },
            { "digest": "sha256:8bdb50d0eb5ec766ba235c06ac8c8a6f44ab1beeed756efa532e73b79786e36a", "size": 11776 }
        ],
        "upper": {},
        "resultFile": ""
    })).expect("parse inline default sample image config")
}

fn load_image_config(work_root: &Path) -> Result<ImageConfig> {
    if let Some(path) = env_var(ENV_IMAGE_CONFIG) {
        let raw =
            fs::read_to_string(&path).with_context(|| format!("read {ENV_IMAGE_CONFIG} failed"))?;
        let cfg: ImageConfig =
            serde_json::from_str(&raw).context("parse custom image config json failed")?;
        return Ok(cfg);
    }

    if let (Some(repo_blob_url), Some(digest)) = (env_var(ENV_REPO_BLOB_URL), env_var(ENV_DIGEST)) {
        let size = env_u64(ENV_SIZE)?;
        return Ok(ImageConfig {
            repo_blob_url,
            lowers: vec![LayerConfig {
                digest,
                size: size.unwrap_or(0),
                dir: work_root.join("layer0").to_string_lossy().into_owned(),
                ..LayerConfig::default()
            }],
            result_file: work_root.join("result.txt").to_string_lossy().into_owned(),
            ..ImageConfig::default()
        });
    }

    // Use inline default sample config — no external file dependency
    let mut cfg = sample_image_config();
    for (index, lower) in cfg.lowers.iter_mut().enumerate() {
        lower.dir = work_root
            .join(format!("layer-{}", index + 1))
            .to_string_lossy()
            .into_owned();
    }
    cfg.result_file = work_root.join("result.txt").to_string_lossy().into_owned();
    Ok(cfg)
}

fn write_global_config(tmp: &TempDir) -> Result<PathBuf> {
    let mut cfg = GlobalConfig {
        enable_audit: false,
        log_path: String::new(),
        audit_path: String::new(),
        credential_file_path: String::new(),
        nr_io_rings: 1,
        ..GlobalConfig::default()
    };

    cfg.registry_fs_version = "v2".to_string();
    cfg.cache_config.cache_type = env_var(ENV_CACHE_TYPE).unwrap_or_else(|| "file".to_string());
    cfg.cache_config.cache_dir = env_var(ENV_CACHE_DIR)
        .unwrap_or_else(|| tmp.path().join("cache").to_string_lossy().into_owned());
    cfg.cache_config.cache_size_gb = 1;
    cfg.cache_config.refill_size = 262_144;

    cfg.download = download_config()?;

    if let Some(path) = env_var(ENV_CREDENTIAL_FILE) {
        cfg.credential_config = CredentialConfig {
            mode: "file".to_string(),
            path,
            timeout: env_u64(ENV_CREDENTIAL_TIMEOUT)?.unwrap_or(1) as i32,
        };
    }
    if let Some(path) = env_var(ENV_CREDENTIAL_HTTP) {
        cfg.credential_config = CredentialConfig {
            mode: "http".to_string(),
            path,
            timeout: env_u64(ENV_CREDENTIAL_TIMEOUT)?.unwrap_or(1) as i32,
        };
    }

    cfg.p2p_config.enable = env_bool(ENV_P2P_ENABLE, false)?;
    if let Some(addr) = env_var(ENV_P2P_ADDRESS) {
        cfg.p2p_config.address = addr;
    }
    if let Some(user_agent) = env_var(ENV_USER_AGENT) {
        cfg.user_agent = user_agent;
    }

    let path = tmp.path().join("overlaybd-live.json");
    fs::write(
        &path,
        serde_json::to_vec_pretty(&cfg).context("serialize generated global config failed")?,
    )
    .with_context(|| format!("write generated global config failed: {}", path.display()))?;
    Ok(path)
}

fn resolve_global_config(tmp: &TempDir) -> Result<PathBuf> {
    if let Some(path) = env_var(ENV_GLOBAL_CONFIG) {
        return Ok(PathBuf::from(path));
    }
    write_global_config(tmp)
}

fn write_image_cfg(path: &Path, cfg: &ImageConfig) -> Result<()> {
    fs::write(
        path,
        serde_json::to_vec_pretty(cfg).context("serialize image config failed")?,
    )
    .with_context(|| format!("write image config failed: {}", path.display()))
}

fn download_config() -> Result<DownloadConfig> {
    Ok(DownloadConfig {
        enable: env_bool(ENV_ENABLE_DOWNLOAD, true)?,
        delay: 0,
        delay_extra: 0,
        max_mbps: 0,
        try_cnt: 2,
        block_size: 262_144,
        concurrency: 1,
        max_inflight_blocks: 16,
        max_concurrent_files: 8,
    })
}

fn disabled_download_config() -> DownloadConfig {
    DownloadConfig {
        enable: false,
        delay: 0,
        delay_extra: 0,
        max_mbps: 0,
        try_cnt: 1,
        block_size: 262_144,
        concurrency: 1,
        max_inflight_blocks: 16,
        max_concurrent_files: 8,
    }
}

async fn create_initialized_upper(
    data_path: &Path,
    index_path: &Path,
    virtual_size: u64,
) -> Result<()> {
    let data_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(data_path)
            .with_context(|| format!("create upper data failed: {}", data_path.display()))?,
    );
    let index_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(index_path)
            .with_context(|| format!("create upper index failed: {}", index_path.display()))?,
    );
    let args = LayerInfo::new(data_file, Some(index_file), virtual_size);
    let _upper = create_file_rw(args).await.with_context(|| {
        format!(
            "initialize upper files failed: {} / {}",
            data_path.display(),
            index_path.display()
        )
    })?;
    Ok(())
}

fn pick_upper_offsets(vsize: u64, len: usize) -> Result<(u64, u64, u64)> {
    let len = len as u64;
    if vsize < len * 3 {
        bail!(
            "image is too small for upper write validation: vsize={}, read_len={}",
            vsize,
            len
        );
    }

    let head = 0;
    let mid = if (1 << 20) + len <= vsize {
        1 << 20
    } else {
        len
    };
    let preserve = if mid + len <= vsize {
        mid + len
    } else {
        len * 2
    };

    if preserve == head || preserve == mid {
        bail!(
            "failed to derive disjoint offsets for upper validation: head={head}, mid={mid}, preserve={preserve}"
        );
    }
    Ok((head, mid, preserve))
}

fn patterned_block(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|idx| seed.wrapping_add(((idx * 17 + 3) % 251) as u8))
        .collect()
}

/// Wait until every remote layer blob is fully present in the shared
/// full-file cache. Background download fills `cache_dir/<cache_id>/data`
/// (cache_id = sha256 of the blob URL) instead of producing per-layer
/// `overlaybd.commit` files; completion is measured by the data file's
/// allocated size reaching the blob size. Returns the cache data files.
async fn wait_cached_layers(
    image_cfg: &ImageConfig,
    cache_dir: &Path,
    timeout: Duration,
) -> Result<Vec<PathBuf>> {
    let layers: Vec<(PathBuf, u64)> = image_cfg
        .lowers
        .iter()
        .filter(|lower| lower.file.is_empty() && !lower.digest.is_empty())
        .map(|lower| {
            let blob_url = format!(
                "{}/{}",
                image_cfg.repo_blob_url.trim_end_matches('/'),
                lower.digest
            );
            let cache_id = format!("{:x}", Sha256::digest(blob_url.as_bytes()));
            (cache_dir.join(&cache_id).join("data"), lower.size)
        })
        .collect();
    if layers.is_empty() {
        return Ok(Vec::new());
    }

    let allocated = |path: &Path| -> u64 {
        std::fs::metadata(path)
            .map(|meta| std::os::unix::fs::MetadataExt::blocks(&meta) * 512)
            .unwrap_or(0)
    };
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if layers.iter().all(|(path, size)| allocated(path) >= *size) {
            return Ok(layers.into_iter().map(|(path, _)| path).collect());
        }
        sleep(Duration::from_millis(200)).await;
    }

    let missing: Vec<String> = layers
        .iter()
        .filter(|(path, size)| allocated(path) < *size)
        .map(|(path, size)| {
            format!(
                "{} (allocated {} of {size} bytes)",
                path.display(),
                allocated(path)
            )
        })
        .collect();
    bail!(
        "timed out waiting for cached layers: {}",
        missing.join(", ")
    );
}

async fn read_ranges(image: &ImageFile, offsets: &[u64], read_len: usize) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(offsets.len());
    for &offset in offsets {
        let data = image
            .read_at(offset, read_len)
            .await
            .with_context(|| format!("read image at offset {offset} failed"))?;
        out.push(data.to_vec());
    }
    Ok(out)
}

async fn create_image_file_with_retry(
    service: &ImageService,
    image_cfg_path: &Path,
) -> Result<ImageFile> {
    const MAX_ATTEMPTS: usize = 3;

    let mut last_error = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match service.create_image_file(image_cfg_path).await {
            Ok(image) => return Ok(image),
            Err(err) => {
                last_error = Some(err);
                if attempt < MAX_ATTEMPTS {
                    sleep(Duration::from_secs(attempt as u64)).await;
                }
            }
        }
    }

    let err = last_error.expect("create_image_file retry should capture last error");
    Err(err).with_context(|| {
        format!(
            "create image file failed after {MAX_ATTEMPTS} attempts: {}",
            image_cfg_path.display()
        )
    })
}

/// The background download stores the original registry blob in the shared
/// full-file cache. For overlaybd remote layers that blob is a tar archive
/// whose payload is the actual `overlaybd.commit` zfile. Validate that wrapper
/// rather than assuming the cached bytes are a bare LSMT commit.
async fn validate_downloaded_commit_blob(path: &Path) -> Result<()> {
    let local: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_ro(path)
            .with_context(|| format!("open downloaded commit blob failed: {}", path.display()))?,
    );
    let tar = new_tar_file_adaptor(local).await.with_context(|| {
        format!(
            "open downloaded commit blob as tar failed: {}",
            path.display()
        )
    })?;
    let detected = is_zfile(tar.clone())
        .await
        .with_context(|| format!("probe downloaded commit payload failed: {}", path.display()))?;
    if detected != 1 {
        bail!(
            "downloaded commit payload is not a zfile: {}",
            path.display()
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_registry_e2e_read_download_verify() -> Result<()> {
    let tmp = TempDir::new().context("create tempdir failed")?;
    let work_root = tmp.path().join("image-work");
    fs::create_dir_all(&work_root).context("create work root failed")?;

    let mut image_cfg = load_image_config(&work_root)?;
    image_cfg.download_override = Some(download_config()?);
    for (index, lower) in image_cfg.lowers.iter_mut().enumerate() {
        if lower.file.is_empty() && !lower.digest.is_empty() && lower.dir.is_empty() {
            lower.dir = work_root
                .join(format!("layer-{}", index + 1))
                .to_string_lossy()
                .into_owned();
        }
    }
    if let Some(result_file) = env_var(ENV_RESULT_FILE) {
        image_cfg.result_file = result_file;
    }
    if image_cfg
        .lowers
        .iter()
        .all(|lower| lower.file.is_empty() && lower.digest.is_empty())
    {
        bail!(
            "image config has no remote lower layer; this live test expects registry-backed lowers"
        );
    }

    let image_cfg_path = tmp.path().join("image-live.json");
    write_image_cfg(&image_cfg_path, &image_cfg)?;
    let global_cfg_path = resolve_global_config(&tmp)?;

    let service = ImageService::from_config_path(&global_cfg_path)
        .await
        .with_context(|| format!("open image service failed: {}", global_cfg_path.display()))?;
    let image = create_image_file_with_retry(&service, &image_cfg_path).await?;

    if !image_cfg.result_file.is_empty() {
        let result = fs::read_to_string(&image_cfg.result_file)
            .with_context(|| format!("read result file failed: {}", image_cfg.result_file))?;
        if result != "success" {
            bail!("result file is not success: {result}");
        }
    }

    let vsize = image.size().await.context("query virtual size failed")?;
    if let Some(expect_vsize) = env_u64(ENV_EXPECT_VSIZE)? {
        assert_eq!(vsize, expect_vsize, "virtual size mismatch");
    }

    let read_len = env_usize(ENV_READ_LEN)?.unwrap_or(4096);
    let offsets = sample_offsets(vsize, read_len)?;
    let remote_reads = read_ranges(&image, &offsets, read_len).await?;
    if remote_reads.iter().all(|buf| buf.is_empty()) {
        bail!("all remote reads returned empty buffers");
    }

    if let Some(expect_sha256) = env_var(ENV_EXPECT_SHA256) {
        let first_non_empty = remote_reads
            .iter()
            .find(|buf| !buf.is_empty())
            .context("no non-empty buffer available for sha256 verification")?;
        let got = format!("sha256:{}", sha256_hex(first_non_empty));
        assert_eq!(got, expect_sha256, "expected sha256 mismatch");
    }

    let download_enabled = image_cfg
        .download_override
        .as_ref()
        .map(|download| download.enable)
        .unwrap_or(false);
    if !download_enabled {
        drop(image);
        return Ok(());
    }

    let wait_secs = env_u64(ENV_WAIT_DOWNLOAD_SECS)?.unwrap_or(300);
    // Background download fills the shared full-file cache instead of
    // producing per-layer commit files: wait for every remote layer blob to
    // be fully cached, then validate each cached blob still wraps a zfile
    // commit payload.
    let global_cfg: GlobalConfig = serde_json::from_slice(
        &fs::read(&global_cfg_path).context("read generated global config failed")?,
    )
    .context("parse generated global config failed")?;
    let cache_dir = PathBuf::from(&global_cfg.cache_config.cache_dir);
    let cached_blobs =
        wait_cached_layers(&image_cfg, &cache_dir, Duration::from_secs(wait_secs)).await?;
    if cached_blobs.is_empty() {
        bail!("download is enabled but no remote lowers exist to cache");
    }
    for cached_blob in &cached_blobs {
        validate_downloaded_commit_blob(cached_blob)
            .await
            .with_context(|| format!("cached blob validation failed: {}", cached_blob.display()))?;
    }

    drop(image);

    let reopened = create_image_file_with_retry(&service, &image_cfg_path)
        .await
        .context("reopen image after background download failed")?;
    let local_reads = read_ranges(&reopened, &offsets, read_len).await?;

    for (index, (remote, local)) in remote_reads.iter().zip(local_reads.iter()).enumerate() {
        assert_eq!(
            remote, local,
            "reopened local read differs from remote read at offsets[{}]={}",
            index, offsets[index]
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_registry_e2e_upper_write_sync_reopen_persist() -> Result<()> {
    let tmp = TempDir::new().context("create tempdir failed")?;
    let work_root = tmp.path().join("image-work");
    fs::create_dir_all(&work_root).context("create work root failed")?;

    let mut rw_cfg = load_image_config(&work_root)?;
    rw_cfg.download_override = Some(disabled_download_config());
    rw_cfg.result_file = work_root
        .join("rw-result.txt")
        .to_string_lossy()
        .into_owned();
    for (index, lower) in rw_cfg.lowers.iter_mut().enumerate() {
        if lower.file.is_empty() && !lower.digest.is_empty() && lower.dir.is_empty() {
            lower.dir = work_root
                .join(format!("layer-{}", index + 1))
                .to_string_lossy()
                .into_owned();
        }
    }
    let global_cfg_path = resolve_global_config(&tmp)?;
    let service = ImageService::from_config_path(&global_cfg_path)
        .await
        .with_context(|| format!("open image service failed: {}", global_cfg_path.display()))?;

    let upper_data = work_root.join("upper.data");
    let upper_index = work_root.join("upper.index");
    create_initialized_upper(&upper_data, &upper_index, 0).await?;
    let upper_data_len_before = fs::metadata(&upper_data)
        .with_context(|| format!("stat upper data failed: {}", upper_data.display()))?
        .len();
    let upper_index_len_before = fs::metadata(&upper_index)
        .with_context(|| format!("stat upper index failed: {}", upper_index.display()))?
        .len();

    rw_cfg.upper = UpperConfig {
        mode: None,
        index: upper_index.to_string_lossy().into_owned(),
        data: upper_data.to_string_lossy().into_owned(),
        target: String::new(),
        gzip_index: String::new(),
    };

    let rw_cfg_path = tmp.path().join("image-rw.json");
    write_image_cfg(&rw_cfg_path, &rw_cfg)?;

    let image = create_image_file_with_retry(&service, &rw_cfg_path).await?;
    assert!(
        !image.is_read_only().await,
        "image should be read-write when upper is set"
    );
    let vsize = image.size().await.context("query rw image size failed")?;
    if let Some(expect_vsize) = env_u64(ENV_EXPECT_VSIZE)? {
        assert_eq!(vsize, expect_vsize, "virtual size mismatch");
    }
    let read_len = 4096usize;
    let (head_off, mid_off, preserve_off) = pick_upper_offsets(vsize, read_len)?;
    let preserved_before = image
        .read_at(preserve_off, read_len)
        .await
        .with_context(|| format!("read preserved baseline at offset {preserve_off} failed"))?;

    let head_overlay = patterned_block(0x3a, read_len);
    let mid_overlay = patterned_block(0xc1, read_len);
    image
        .write_at(head_off, &head_overlay)
        .await
        .with_context(|| format!("write head overlay at offset {head_off} failed"))?;
    image
        .write_at(mid_off, &mid_overlay)
        .await
        .with_context(|| format!("write mid overlay at offset {mid_off} failed"))?;
    image.sync().await.context("sync rw image failed")?;
    drop(image);

    let upper_data_len_after = fs::metadata(&upper_data)
        .with_context(|| format!("stat upper data failed: {}", upper_data.display()))?
        .len();
    let upper_index_len_after = fs::metadata(&upper_index)
        .with_context(|| format!("stat upper index failed: {}", upper_index.display()))?
        .len();
    assert!(
        upper_data_len_after > upper_data_len_before,
        "upper data file should grow after writes: before={}, after={}",
        upper_data_len_before,
        upper_data_len_after
    );
    assert!(
        upper_index_len_after > upper_index_len_before,
        "upper index file should grow after writes: before={}, after={}",
        upper_index_len_before,
        upper_index_len_after
    );

    let reopened = create_image_file_with_retry(&service, &rw_cfg_path)
        .await
        .context("reopen rw image after upper sync failed")?;
    assert!(
        !reopened.is_read_only().await,
        "reopened image should remain read-write with upper"
    );

    let got_head = reopened
        .read_at(head_off, read_len)
        .await
        .with_context(|| format!("read head overlay at offset {head_off} failed"))?;
    assert_eq!(got_head.as_ref(), head_overlay.as_slice());

    let got_mid = reopened
        .read_at(mid_off, read_len)
        .await
        .with_context(|| format!("read mid overlay at offset {mid_off} failed"))?;
    assert_eq!(got_mid.as_ref(), mid_overlay.as_slice());

    let preserved_after = reopened
        .read_at(preserve_off, read_len)
        .await
        .with_context(|| format!("read preserved region at offset {preserve_off} failed"))?;
    assert_eq!(
        preserved_after.as_ref(),
        preserved_before.as_ref(),
        "untouched lower-backed region changed after upper writes"
    );
    assert_eq!(
        reopened
            .size()
            .await
            .context("query reopened image size failed")?,
        vsize,
        "reopened image virtual size changed after upper writes"
    );

    let result = fs::read_to_string(&rw_cfg.result_file)
        .with_context(|| format!("read result file failed: {}", rw_cfg.result_file))?;
    assert_eq!(result, "success");

    Ok(())
}

#[tokio::test]
async fn test_registry_e2e_upper_compact_to_local_lower() -> Result<()> {
    let tmp = TempDir::new().context("create tempdir failed")?;
    let work_root = tmp.path().join("image-work");
    fs::create_dir_all(&work_root).context("create work root failed")?;

    let mut rw_cfg = load_image_config(&work_root)?;
    rw_cfg.download_override = Some(disabled_download_config());
    rw_cfg.result_file = work_root
        .join("compact-rw-result.txt")
        .to_string_lossy()
        .into_owned();
    for (index, lower) in rw_cfg.lowers.iter_mut().enumerate() {
        if lower.file.is_empty() && !lower.digest.is_empty() && lower.dir.is_empty() {
            lower.dir = work_root
                .join(format!("layer-{}", index + 1))
                .to_string_lossy()
                .into_owned();
        }
    }
    let global_cfg_path = resolve_global_config(&tmp)?;
    let service = ImageService::from_config_path(&global_cfg_path)
        .await
        .with_context(|| format!("open image service failed: {}", global_cfg_path.display()))?;

    let upper_data = work_root.join("compact-upper.data");
    let upper_index = work_root.join("compact-upper.index");
    create_initialized_upper(&upper_data, &upper_index, 0).await?;
    rw_cfg.upper = UpperConfig {
        mode: None,
        index: upper_index.to_string_lossy().into_owned(),
        data: upper_data.to_string_lossy().into_owned(),
        target: String::new(),
        gzip_index: String::new(),
    };
    let rw_cfg_path = tmp.path().join("image-compact-rw.json");
    write_image_cfg(&rw_cfg_path, &rw_cfg)?;

    let image = create_image_file_with_retry(&service, &rw_cfg_path).await?;
    assert!(
        !image.is_read_only().await,
        "image should be read-write when upper is set"
    );
    let vsize = image.size().await.context("query rw image size failed")?;
    let read_len = 4096usize;
    let (head_off, mid_off, preserve_off) = pick_upper_offsets(vsize, read_len)?;
    let preserved_before = image
        .read_at(preserve_off, read_len)
        .await
        .with_context(|| format!("read preserved baseline at offset {preserve_off} failed"))?;

    let head_overlay = patterned_block(0x5d, read_len);
    let mid_overlay = patterned_block(0xa4, read_len);
    image
        .write_at(head_off, &head_overlay)
        .await
        .with_context(|| format!("write head overlay at offset {head_off} failed"))?;
    image
        .write_at(mid_off, &mid_overlay)
        .await
        .with_context(|| format!("write mid overlay at offset {mid_off} failed"))?;
    image.sync().await.context("sync rw image failed")?;

    let compact_path = work_root.join("flattened-lower.data");
    let compact_dest: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(&compact_path)
            .with_context(|| format!("create compact dest failed: {}", compact_path.display()))?,
    );
    image
        .compact(compact_dest.clone())
        .await
        .with_context(|| format!("compact image to {} failed", compact_path.display()))?;
    compact_dest
        .sync()
        .await
        .with_context(|| format!("sync compact dest failed: {}", compact_path.display()))?;
    drop(image);

    let compact_len = fs::metadata(&compact_path)
        .with_context(|| format!("stat compact dest failed: {}", compact_path.display()))?
        .len();
    assert!(
        compact_len > 0,
        "compact output should not be empty: {}",
        compact_path.display()
    );

    let compact_cfg = ImageConfig {
        repo_blob_url: String::new(),
        lowers: vec![LayerConfig {
            file: compact_path.to_string_lossy().into_owned(),
            ..LayerConfig::default()
        }],
        upper: UpperConfig::default(),
        result_file: work_root
            .join("compact-reopen-result.txt")
            .to_string_lossy()
            .into_owned(),
        download_override: Some(disabled_download_config()),
        acceleration_layer: false,
        record_trace_path: String::new(),
    };
    let compact_cfg_path = tmp.path().join("image-compact-reopen.json");
    write_image_cfg(&compact_cfg_path, &compact_cfg)?;

    let reopened = create_image_file_with_retry(&service, &compact_cfg_path)
        .await
        .with_context(|| {
            format!(
                "reopen compacted image failed: {}",
                compact_cfg_path.display()
            )
        })?;
    assert!(
        reopened.is_read_only().await,
        "compacted image should reopen as a read-only lower"
    );
    assert_eq!(
        reopened
            .size()
            .await
            .context("query compacted image size failed")?,
        vsize,
        "compacted image virtual size mismatch"
    );

    let got_head = reopened
        .read_at(head_off, read_len)
        .await
        .with_context(|| format!("read compacted head overlay at offset {head_off} failed"))?;
    assert_eq!(got_head.as_ref(), head_overlay.as_slice());

    let got_mid = reopened
        .read_at(mid_off, read_len)
        .await
        .with_context(|| format!("read compacted mid overlay at offset {mid_off} failed"))?;
    assert_eq!(got_mid.as_ref(), mid_overlay.as_slice());

    let preserved_after = reopened
        .read_at(preserve_off, read_len)
        .await
        .with_context(|| {
            format!("read compacted preserved region at offset {preserve_off} failed")
        })?;
    assert_eq!(
        preserved_after.as_ref(),
        preserved_before.as_ref(),
        "compacted image changed an untouched lower-backed region"
    );

    let result = fs::read_to_string(&compact_cfg.result_file)
        .with_context(|| format!("read result file failed: {}", compact_cfg.result_file))?;
    assert_eq!(result, "success");

    Ok(())
}

#[test]
fn test_sample_config_valid() -> Result<()> {
    let cfg = sample_image_config();
    assert_eq!(
        cfg.repo_blob_url,
        "https://registry-1.docker.io/v2/overlaybd/redis/blobs"
    );
    assert!(cfg.lowers.len() >= 2, "sample config should be multi-layer");
    assert!(cfg
        .lowers
        .iter()
        .all(|lower| lower.digest.starts_with("sha256:")));
    assert!(cfg.lowers.iter().all(|lower| lower.size > 0));
    Ok(())
}
