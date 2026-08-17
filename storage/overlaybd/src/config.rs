use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

pub const MAX_LAYER_CNT: usize = 256;
const DEFAULT_LOG_SIZE_MB: u32 = 10;
const DEFAULT_LOG_ROTATE_NUM: i32 = 3;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct LayerConfig {
    pub gzip_index: String,
    pub file: String,
    pub target_file: String,
    pub dir: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub repo_blob_url: String,
    pub digest: String,
    pub uuid: String,
    pub target_digest: String,
    pub size: u64,
}

impl LayerConfig {
    pub fn effective_repo_blob_url<'a>(&'a self, image_repo_blob_url: &'a str) -> &'a str {
        if self.repo_blob_url.is_empty() {
            image_repo_blob_url
        } else {
            &self.repo_blob_url
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct UpperConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<UpperMode>,
    pub index: String,
    pub data: String,
    pub target: String,
    pub gzip_index: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum UpperMode {
    Sparse,
    LogStructured,
    HybridLogStructured,
}

impl UpperConfig {
    pub fn writable_mode(&self) -> UpperMode {
        self.mode.unwrap_or(UpperMode::LogStructured)
    }
}

pub fn validate_upper_config(upper: &UpperConfig) -> Result<bool> {
    let has_upper_data = !upper.data.is_empty();
    let has_upper_index = !upper.index.is_empty();
    let has_complete_upper = has_upper_data && has_upper_index;
    let has_target = !upper.target.is_empty();
    let has_gzip_index = !upper.gzip_index.is_empty();

    let has_writable_upper = match upper.mode {
        Some(UpperMode::Sparse) => {
            ensure!(
                has_upper_data,
                "upper.data is required when upper.mode is sparse"
            );
            true
        }
        Some(UpperMode::LogStructured) | Some(UpperMode::HybridLogStructured) | None => {
            let has_partial_upper = has_upper_data ^ has_upper_index;
            ensure!(
                !has_partial_upper,
                "upper.index and upper.data must be set together"
            );
            has_complete_upper
        }
    };

    if (has_target || has_gzip_index) && !has_complete_upper {
        bail!("upper.target and upper.gzipIndex require complete upper.data/index");
    }
    if has_gzip_index && !has_target {
        bail!("upper.gzipIndex requires upper.target");
    }

    Ok(has_writable_upper)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct DownloadConfig {
    pub enable: bool,
    pub delay: i32,
    pub delay_extra: i32,
    /// Rate limit shared by the concurrent chunk downloads of one layer, in
    /// MiB/s; 0 disables throttling. Backward compatible with the historical
    /// `maxMBps` JSON knob, which older configs may still carry.
    #[serde(rename = "maxMBps")]
    pub max_mbps: i32,
    pub try_cnt: i32,
    /// Background download chunk size in bytes: one source request fetches a
    /// chunk of this size (aligned down to whole cache blocks), covering
    /// `blockSize / cacheBlockSize` cache blocks. The cache keeps storing and
    /// exposing data in its own smaller block size, so foreground reads keep
    /// their fine-grained on-demand granularity while background downloads
    /// keep large-request throughput.
    pub block_size: u32,
    pub concurrency: usize,
    /// Cap on concurrently downloading chunks enforced by the cache backend's
    /// download scheduler (bounds total scratch memory to maxInflightBlocks ×
    /// the download chunk size), independent of the per-image `concurrency`
    /// knob. The global value fixes the scheduler's capacity when the
    /// file-cache backend is created. A per-image `download` override may
    /// still carry this field for serialization compatibility, but it never
    /// resizes the scheduler-owned cap; the first mismatch per scheduler is
    /// logged under the fixed category `max_inflight_blocks_override_ignored`.
    pub max_inflight_blocks: usize,
    /// Cap on concurrently running background-download layer tasks per
    /// file-cache backend. Tasks beyond the cap stay registered and run when
    /// a slot frees; submission itself is never rejected. Only the global
    /// value takes effect (per-image overrides are ignored).
    pub max_concurrent_files: usize,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            enable: false,
            delay: 300,
            delay_extra: 30,
            max_mbps: 100,
            try_cnt: 5,
            block_size: 16 * 1024 * 1024,
            concurrency: 1,
            max_inflight_blocks: 16,
            max_concurrent_files: 8,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ImageConfig {
    pub repo_blob_url: String,
    pub lowers: Vec<LayerConfig>,
    pub upper: UpperConfig,
    pub result_file: String,
    #[serde(rename = "download", skip_serializing_if = "Option::is_none")]
    pub download_override: Option<DownloadConfig>,
    pub acceleration_layer: bool,
    pub record_trace_path: String,
}

impl ImageConfig {
    pub fn effective_download<'a>(&'a self, global_config: &'a GlobalConfig) -> &'a DownloadConfig {
        self.download_override
            .as_ref()
            .unwrap_or(&global_config.download)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct P2PConfig {
    pub enable: bool,
    pub address: String,
}

impl Default for P2PConfig {
    fn default() -> Self {
        Self {
            enable: false,
            address: "http://localhost:9731/accelerator".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct GzipCacheConfig {
    pub enable: bool,
    pub cache_dir: String,
    #[serde(rename = "cacheSizeGB")]
    pub cache_size_gb: u32,
    pub refill_size: u32,
}

impl Default for GzipCacheConfig {
    fn default() -> Self {
        Self {
            enable: false,
            cache_dir: "/opt/overlaybd/gzip_cache".to_string(),
            cache_size_gb: 4,
            refill_size: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct OssConfig {
    pub enable: bool,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub security_token: String,
    pub credential_process: String,
    pub default_region: String,
    pub default_endpoint: String,
    /// Bucket addressing style: `"virtual"`, `"path"`, or empty to use
    /// endpoint-based auto-detection.
    pub default_addressing_style: String,
    /// Per-request timeout in seconds (connect + transfer). Default 30.
    pub timeout_secs: u64,
    /// Number of retries on transient failures. Default 3.
    pub retry_count: u32,
}

impl Default for OssConfig {
    fn default() -> Self {
        Self {
            enable: false,
            access_key_id: String::new(),
            secret_access_key: String::new(),
            security_token: String::new(),
            credential_process: String::new(),
            default_region: String::new(),
            default_endpoint: String::new(),
            default_addressing_style: String::new(),
            timeout_secs: 30,
            retry_count: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ExporterConfig {
    pub enable: bool,
    pub uri_prefix: String,
    pub port: i32,
    pub update_interval: u64,
}

impl Default for ExporterConfig {
    fn default() -> Self {
        Self {
            enable: false,
            uri_prefix: "/metrics".to_string(),
            port: 9863,
            update_interval: 60 * 1000 * 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct CredentialConfig {
    pub mode: String,
    pub path: String,
    pub timeout: i32,
}

impl Default for CredentialConfig {
    fn default() -> Self {
        Self {
            mode: String::new(),
            path: String::new(),
            timeout: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct CacheConfig {
    pub cache_type: String,
    pub cache_dir: String,
    #[serde(rename = "cacheSizeGB")]
    pub cache_size_gb: u32,
    /// Block size for the cache (used as both the I/O unit and bitmap
    /// granularity). Must be a power of two >= 4096. Replaces the former
    /// separate `refill_size` setting.
    pub refill_size: u32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_type: String::new(),
            cache_dir: "/opt/overlaybd/registry_cache".to_string(),
            cache_size_gb: 4,
            refill_size: 262_144,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct LogConfig {
    pub log_level: u32,
    pub log_path: String,
    #[serde(rename = "logSizeMB")]
    pub log_size_mb: u32,
    pub log_rotate_num: i32,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            log_level: 1,
            log_path: String::new(),
            log_size_mb: DEFAULT_LOG_SIZE_MB,
            log_rotate_num: DEFAULT_LOG_ROTATE_NUM,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct PrefetchConfig {
    pub concurrency: i32,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self { concurrency: 16 }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct CertConfig {
    pub cert_file: String,
    pub key_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct GlobalConfig {
    // Legacy fields
    pub registry_cache_dir: String,
    pub credential_file_path: String,
    #[serde(rename = "registryCacheSizeGB")]
    pub registry_cache_size_gb: u32,
    pub io_engine: u32,
    pub cache_type: String,
    pub log_level: u32,
    pub log_path: String,

    // Common fields
    pub download: DownloadConfig,
    pub enable_audit: bool,
    pub enable_thread: bool,
    pub p2p_config: P2PConfig,
    pub exporter_config: ExporterConfig,
    pub audit_path: String,
    pub registry_fs_version: String,
    pub cache_config: CacheConfig,
    pub gzip_cache_config: GzipCacheConfig,
    pub oss_config: OssConfig,
    pub log_config: LogConfig,
    pub prefetch_config: PrefetchConfig,
    pub cert_config: CertConfig,
    pub user_agent: String,
    pub credential_config: CredentialConfig,

    /// The number of io urings
    pub nr_io_rings: usize,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            registry_cache_dir: "/opt/overlaybd/registry_cache".to_string(),
            credential_file_path: "/opt/overlaybd/cred.json".to_string(),
            registry_cache_size_gb: 4,
            io_engine: 0,
            cache_type: "file".to_string(),
            log_level: 1,
            log_path: "/var/log/overlaybd.log".to_string(),
            download: DownloadConfig::default(),
            enable_audit: true,
            enable_thread: false,
            p2p_config: P2PConfig::default(),
            exporter_config: ExporterConfig::default(),
            audit_path: "/var/log/overlaybd-audit.log".to_string(),
            registry_fs_version: "v2".to_string(),
            cache_config: CacheConfig::default(),
            gzip_cache_config: GzipCacheConfig::default(),
            oss_config: OssConfig::default(),
            log_config: LogConfig::default(),
            prefetch_config: PrefetchConfig::default(),
            cert_config: CertConfig::default(),
            user_agent: env!("CARGO_PKG_VERSION").to_string(),
            credential_config: CredentialConfig::default(),

            nr_io_rings: 4,
        }
    }
}

impl GlobalConfig {
    pub fn normalize_compat_fields(&mut self) {
        // C++ behavior: when cacheConfig.cacheType is empty, use legacy cacheType/cacheDir/cacheSize.
        if self.cache_config.cache_type.is_empty() {
            self.cache_config.cache_type = if self.cache_type.is_empty() {
                "file".to_string()
            } else {
                self.cache_type.clone()
            };
            self.cache_config.cache_dir = self.registry_cache_dir.clone();
            self.cache_config.cache_size_gb = self.registry_cache_size_gb;
        }
        if self.cache_config.cache_type.is_empty() {
            self.cache_config.cache_type = "file".to_string();
        }

        // C++ behavior: prefer credentialConfig, fallback to legacy credentialFilePath.
        if self.credential_config.mode.is_empty() && !self.credential_file_path.is_empty() {
            self.credential_config.mode = "file".to_string();
            self.credential_config.path = self.credential_file_path.clone();
        }

        // C++ behavior: when logConfig.logPath is empty, use legacy logLevel/logPath.
        if self.log_config.log_path.is_empty() {
            self.log_config.log_level = self.log_level;
            self.log_config.log_path = self.log_path.clone();
            if self.log_config.log_size_mb == 0 {
                self.log_config.log_size_mb = DEFAULT_LOG_SIZE_MB;
            }
            if self.log_config.log_rotate_num <= 0 {
                self.log_config.log_rotate_num = DEFAULT_LOG_ROTATE_NUM;
            }
        }

        if self.user_agent.is_empty() {
            self.user_agent = env!("CARGO_PKG_VERSION").to_string();
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, rename_all = "camelCase")]
pub struct AuthConfig {
    pub auths: serde_json::Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, rename_all = "camelCase")]
pub struct ImageAuthResponse {
    pub trace_id: String,
    pub success: bool,
    pub data: AuthConfig,
}

pub fn load_global_config(path: impl AsRef<Path>) -> Result<GlobalConfig> {
    let raw = std::fs::read_to_string(path.as_ref()).context("reading global config file")?;
    let mut cfg: GlobalConfig =
        serde_json::from_str(&raw).context("parse global config json failed")?;
    cfg.normalize_compat_fields();
    validate_global_config(&cfg)?;
    Ok(cfg)
}

pub fn load_image_config(path: impl AsRef<Path>) -> Result<ImageConfig> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path).context("reading image config file")?;
    let mut cfg: ImageConfig =
        serde_json::from_str(&raw).context("parse image config json failed")?;
    resolve_image_config_local_paths(path, &mut cfg);
    validate_image_config(&cfg)?;
    Ok(cfg)
}

pub fn resolve_image_config_local_paths(image_config_path: &Path, cfg: &mut ImageConfig) {
    let base_dir = image_config_path.parent().unwrap_or_else(|| Path::new("."));

    for layer in &mut cfg.lowers {
        resolve_path_field(base_dir, &mut layer.gzip_index);
        resolve_path_field(base_dir, &mut layer.file);
        resolve_path_field(base_dir, &mut layer.target_file);
        resolve_path_field(base_dir, &mut layer.dir);
    }

    resolve_path_field(base_dir, &mut cfg.upper.index);
    resolve_path_field(base_dir, &mut cfg.upper.data);
    resolve_path_field(base_dir, &mut cfg.upper.target);
    resolve_path_field(base_dir, &mut cfg.upper.gzip_index);
    resolve_path_field(base_dir, &mut cfg.result_file);
    resolve_path_field(base_dir, &mut cfg.record_trace_path);
}

fn resolve_path_field(base_dir: &Path, value: &mut String) {
    if value.is_empty() || value.contains("://") {
        return;
    }

    let path = Path::new(value);
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    };
    *value = lexically_normalize_path(&resolved).display().to_string();
}

pub fn lexically_normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

/// Chunk knobs that are honored in every `DownloadConfig` context, including
/// per-image overrides: chunk size and per-layer concurrency.
fn validate_download_chunk_knobs(cfg: &DownloadConfig, field: &str) -> Result<()> {
    const MAX_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
    const MAX_CONCURRENCY: usize = 16;
    const MAX_LAYER_SCRATCH_BYTES: u64 = 256 * 1024 * 1024;
    ensure!(cfg.concurrency > 0, "{field} must be > 0");
    ensure!(
        cfg.concurrency <= MAX_CONCURRENCY,
        "download.concurrency must be <= {MAX_CONCURRENCY} (in {field})"
    );
    ensure!(
        cfg.block_size > 0,
        "download.blockSize must be > 0 (in {field})"
    );
    ensure!(
        u64::from(cfg.block_size) <= MAX_BLOCK_SIZE,
        "download.blockSize must be <= {MAX_BLOCK_SIZE} bytes (in {field})"
    );
    ensure!(
        u64::from(cfg.block_size) * cfg.concurrency as u64 <= MAX_LAYER_SCRATCH_BYTES,
        "download.blockSize * concurrency must be <= {MAX_LAYER_SCRATCH_BYTES} bytes (in {field})"
    );
    Ok(())
}

/// Scheduler caps, meaningful only for the *global* download config that
/// creates the backend scheduler: per-image overrides ignore these fields.
fn validate_download_scheduler_caps(cfg: &DownloadConfig, field: &str) -> Result<()> {
    const MAX_INFLIGHT_BLOCKS: usize = 128;
    const MAX_CONCURRENT_FILES: usize = 128;
    const MAX_GLOBAL_SCRATCH_BYTES: u64 = 2 * 1024 * 1024 * 1024;
    ensure!(
        cfg.max_inflight_blocks > 0,
        "download.maxInflightBlocks must be > 0 (in {field})"
    );
    ensure!(
        cfg.max_inflight_blocks <= MAX_INFLIGHT_BLOCKS,
        "download.maxInflightBlocks must be <= {MAX_INFLIGHT_BLOCKS} (in {field})"
    );
    ensure!(
        cfg.max_concurrent_files > 0,
        "download.maxConcurrentFiles must be > 0 (in {field})"
    );
    ensure!(
        cfg.max_concurrent_files <= MAX_CONCURRENT_FILES,
        "download.maxConcurrentFiles must be <= {MAX_CONCURRENT_FILES} (in {field})"
    );
    let global_scratch = u64::from(cfg.block_size)
        .checked_mul(cfg.max_inflight_blocks as u64)
        .context("download scratch budget overflow")?;
    ensure!(
        global_scratch <= MAX_GLOBAL_SCRATCH_BYTES,
        "download.blockSize * maxInflightBlocks must be <= {MAX_GLOBAL_SCRATCH_BYTES} bytes (in {field})"
    );
    Ok(())
}

pub fn validate_global_config(cfg: &GlobalConfig) -> Result<()> {
    ensure!(cfg.io_engine <= 2, "unknown ioEngine {}", cfg.io_engine);
    validate_download_chunk_knobs(&cfg.download, "download.concurrency")?;
    validate_download_scheduler_caps(&cfg.download, "download")?;

    match cfg.cache_config.cache_type.as_str() {
        "file" | "ocf" | "download" => {}
        other => {
            bail!("unknown cache type: {other}");
        }
    }

    ensure!(
        !(cfg.enable_thread && cfg.cache_config.cache_type == "file"),
        "enableThread=true is invalid when cache type is file"
    );

    match cfg.credential_config.mode.as_str() {
        "" => {}
        "file" | "http" => {
            ensure!(
                !cfg.credential_config.path.is_empty(),
                "credentialConfig.path cannot be empty when mode is set"
            );
        }
        other => {
            bail!("invalid credential mode {other}");
        }
    }

    ensure!(
        !(cfg.p2p_config.enable && cfg.p2p_config.address.is_empty()),
        "p2pConfig.address cannot be empty when p2p is enabled"
    );

    ensure!(
        matches!(cfg.registry_fs_version.as_str(), "v1" | "v2"),
        "unsupported registryFsVersion {}",
        cfg.registry_fs_version
    );

    if cfg.oss_config.enable {
        ensure!(
            !cfg.oss_config.default_endpoint.is_empty(),
            "ossConfig.defaultEndpoint cannot be empty when oss is enabled"
        );
        ensure!(
            !cfg.oss_config.default_region.trim().is_empty(),
            "ossConfig.defaultRegion cannot be empty when oss is enabled"
        );
        ensure!(
            cfg.oss_config.access_key_id.is_empty() == cfg.oss_config.secret_access_key.is_empty(),
            "ossConfig.accessKeyId and ossConfig.secretAccessKey must be set together"
        );
        ensure!(
            cfg.oss_config.credential_process.trim().is_empty()
                || (cfg.oss_config.access_key_id.is_empty()
                    && cfg.oss_config.secret_access_key.is_empty()
                    && cfg.oss_config.security_token.is_empty()),
            "ossConfig.credentialProcess cannot be combined with accessKeyId/secretAccessKey/securityToken"
        );
        ensure!(
            cfg.oss_config.security_token.is_empty()
                || (!cfg.oss_config.access_key_id.is_empty()
                    && !cfg.oss_config.secret_access_key.is_empty()),
            "ossConfig.securityToken requires accessKeyId and secretAccessKey"
        );
        ensure!(
            matches!(
                cfg.oss_config.default_addressing_style.trim(),
                "" | "virtual" | "path"
            ),
            "ossConfig.defaultAddressingStyle must be 'virtual', 'path', or empty for auto-detection, got '{}'",
            cfg.oss_config.default_addressing_style
        );
    }

    ensure!(cfg.nr_io_rings != 0, "nr_io_rings cannot be zero");

    Ok(())
}

pub fn validate_image_config(cfg: &ImageConfig) -> Result<()> {
    if let Some(download) = &cfg.download_override {
        // Per-image overrides only honor the chunk knobs; the scheduler caps
        // (maxInflightBlocks/maxConcurrentFiles) are fixed at backend
        // creation and ignored here.
        validate_download_chunk_knobs(download, "download override concurrency")?;
    }

    ensure!(
        cfg.lowers.len() <= MAX_LAYER_CNT,
        "too many lowers: {}, max {}",
        cfg.lowers.len(),
        MAX_LAYER_CNT
    );

    let has_writable_upper = validate_upper_config(&cfg.upper)?;

    ensure!(
        !cfg.lowers.is_empty() || has_writable_upper,
        "image config has no lowers and no writable upper"
    );

    for layer in &cfg.lowers {
        ensure!(
            !layer.file.is_empty() || !layer.digest.is_empty() || !layer.dir.is_empty(),
            "each lower must provide at least one of file/dir/digest"
        );
        let is_remote = layer.file.is_empty() && !layer.digest.is_empty();
        ensure!(
            !(is_remote && layer.effective_repo_blob_url(&cfg.repo_blob_url).is_empty()),
            "repoBlobUrl is required for remote lower layers"
        );
    }

    ensure!(
        !cfg.acceleration_layer || cfg.record_trace_path.is_empty(),
        "accelerationLayer and recordTracePath cannot be enabled together"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_download_config_legacy_json_defaults_concurrency() {
        let raw = r#"
        {
          "enable": true,
          "delay": 12,
          "delayExtra": 3,
          "maxMBps": 80,
          "tryCnt": 7,
          "blockSize": 1048576
        }"#;
        let cfg: DownloadConfig = serde_json::from_str(raw).expect("parse legacy download config");

        assert_eq!(cfg.concurrency, 1);
        assert_eq!(DownloadConfig::default().concurrency, 1);
        // The historical maxMBps knob remains accepted for compatibility.
        assert_eq!(cfg.max_mbps, 80);
    }

    #[test]
    fn test_download_config_external_json_schema_round_trip() {
        let raw = r#"
        {
          "enable": true,
          "delay": 12,
          "delayExtra": 3,
          "maxMBps": 80,
          "tryCnt": 7,
          "blockSize": 1048576,
          "concurrency": 4,
          "maxInflightBlocks": 9
        }"#;
        let cfg: DownloadConfig =
            serde_json::from_str(raw).expect("parse external download config");
        assert!(cfg.enable);
        assert_eq!(cfg.delay, 12);
        assert_eq!(cfg.delay_extra, 3);
        assert_eq!(cfg.max_mbps, 80);
        assert_eq!(cfg.try_cnt, 7);
        assert_eq!(cfg.block_size, 1_048_576);
        assert_eq!(cfg.concurrency, 4);
        assert_eq!(cfg.max_inflight_blocks, 9);

        let serialized = serde_json::to_value(&cfg).expect("serialize download config");
        let object = serialized
            .as_object()
            .expect("download config serializes to an object");
        for field in [
            "enable",
            "delay",
            "delayExtra",
            "maxMBps",
            "tryCnt",
            "blockSize",
            "concurrency",
            "maxInflightBlocks",
            "maxConcurrentFiles",
        ] {
            assert!(
                object.contains_key(field),
                "missing external download field {field}"
            );
        }
        assert_eq!(object.len(), 9, "unexpected external field drift");

        let reparsed: DownloadConfig =
            serde_json::from_value(serialized).expect("reparse serialized download config");
        assert_eq!(reparsed, cfg);
    }

    #[test]
    fn test_image_config_download_enabled_remote_lower_needs_no_layer_dir() {
        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "download": {
            "enable": true,
            "maxMBps": 40,
            "tryCnt": 2,
            "blockSize": 65536,
            "concurrency": 2
          }
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse image config with download");
        let download = cfg.download_override.as_ref().expect("download override");
        assert!(download.enable);
        assert_eq!(download.max_mbps, 40);
        assert_eq!(download.try_cnt, 2);
        assert_eq!(download.block_size, 65536);
        assert_eq!(download.concurrency, 2);

        // Background download fills the remote file cache, so an enabled
        // remote lower requires neither a layer directory nor a local commit
        // destination.
        assert!(cfg.lowers[0].dir.is_empty());
        assert!(cfg.lowers[0].file.is_empty());
        validate_image_config(&cfg).expect("download-enabled remote lower needs no layer dir");

        let serialized = serde_json::to_value(&cfg).expect("serialize image config");
        assert_eq!(serialized["download"]["enable"], true);
        assert_eq!(serialized["download"]["maxMBps"], 40);
    }

    #[test]
    fn test_global_download_rejects_zero_concurrency() {
        let raw = r#"
        {
          "download": {"concurrency": 0},
          "registryFsVersion": "v2",
          "ioEngine": 0
        }"#;
        let mut cfg: GlobalConfig = serde_json::from_str(raw).expect("parse global config");
        cfg.normalize_compat_fields();

        let err = validate_global_config(&cfg).expect_err("zero concurrency must fail");
        assert!(
            err.to_string().contains("download.concurrency must be > 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_global_cache_compat_fallback() {
        let raw = r#"
        {
          "cacheType": "file",
          "registryCacheDir": "/tmp/cache",
          "registryCacheSizeGB": 8,
          "credentialFilePath": "/tmp/cred.json",
          "registryFsVersion": "v2",
          "ioEngine": 0
        }"#;
        let mut cfg: GlobalConfig = serde_json::from_str(raw).expect("parse");
        cfg.normalize_compat_fields();

        assert_eq!(cfg.cache_config.cache_type, "file");
        assert_eq!(cfg.cache_config.cache_dir, "/tmp/cache");
        assert_eq!(cfg.cache_config.cache_size_gb, 8);
        assert_eq!(cfg.credential_config.mode, "file");
        assert_eq!(cfg.credential_config.path, "/tmp/cred.json");
        validate_global_config(&cfg).expect("validate");
    }

    #[test]
    fn test_global_cache_nested_precedence() {
        let raw = r#"
        {
          "cacheType": "file",
          "registryCacheDir": "/tmp/cache-legacy",
          "registryCacheSizeGB": 8,
          "cacheConfig": {
            "cacheType": "ocf",
            "cacheDir": "/tmp/cache-new",
            "cacheSizeGB": 16,
            "refillSize": 131072,
            "blockSize": 65536
          },
          "registryFsVersion": "v2",
          "ioEngine": 0
        }"#;
        let mut cfg: GlobalConfig = serde_json::from_str(raw).expect("parse");
        cfg.normalize_compat_fields();

        assert_eq!(cfg.cache_config.cache_type, "ocf");
        assert_eq!(cfg.cache_config.cache_dir, "/tmp/cache-new");
        assert_eq!(cfg.cache_config.cache_size_gb, 16);
    }

    #[test]
    fn test_log_legacy_fallback() {
        let raw = r#"
        {
          "logLevel": 2,
          "logPath": "/tmp/overlaybd.log",
          "registryFsVersion": "v2",
          "ioEngine": 0
        }"#;
        let mut cfg: GlobalConfig = serde_json::from_str(raw).expect("parse");
        cfg.normalize_compat_fields();
        assert_eq!(cfg.log_config.log_level, 2);
        assert_eq!(cfg.log_config.log_path, "/tmp/overlaybd.log");
    }

    #[test]
    fn test_oss_config_requires_region_when_enabled() {
        let raw = r#"
        {
          "registryFsVersion": "v2",
          "ioEngine": 0,
          "ossConfig": {
            "enable": true,
            "defaultEndpoint": "https://oss-cn-hangzhou.aliyuncs.com",
            "defaultRegion": ""
          }
        }"#;
        let mut cfg: GlobalConfig = serde_json::from_str(raw).expect("parse");
        cfg.normalize_compat_fields();
        let err = validate_global_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("defaultRegion"));
    }

    #[test]
    fn test_oss_config_missing_region_when_enabled_fails() {
        let raw = r#"
        {
          "registryFsVersion": "v2",
          "ioEngine": 0,
          "ossConfig": {
            "enable": true,
            "defaultEndpoint": "https://oss-cn-hangzhou.aliyuncs.com"
          }
        }"#;
        let mut cfg: GlobalConfig = serde_json::from_str(raw).expect("parse");
        cfg.normalize_compat_fields();
        let err = validate_global_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("defaultRegion"));
    }

    #[test]
    fn test_oss_config_allows_credential_process_without_static_credentials() {
        let raw = r#"
        {
          "registryFsVersion": "v2",
          "ioEngine": 0,
          "ossConfig": {
            "enable": true,
            "defaultEndpoint": "https://oss-cn-hangzhou.aliyuncs.com",
            "defaultRegion": "cn-hangzhou",
            "credentialProcess": "echo '{}'"
          }
        }"#;
        let mut cfg: GlobalConfig = serde_json::from_str(raw).expect("parse");
        cfg.normalize_compat_fields();
        validate_global_config(&cfg).expect("credential_process-only config should validate");
    }

    #[test]
    fn test_oss_config_rejects_credential_process_with_static_credentials() {
        let raw = r#"
        {
          "registryFsVersion": "v2",
          "ioEngine": 0,
          "ossConfig": {
            "enable": true,
            "defaultEndpoint": "https://oss-cn-hangzhou.aliyuncs.com",
            "defaultRegion": "cn-hangzhou",
            "credentialProcess": "echo '{}'",
            "accessKeyId": "ak",
            "secretAccessKey": "sk"
          }
        }"#;
        let mut cfg: GlobalConfig = serde_json::from_str(raw).expect("parse");
        cfg.normalize_compat_fields();
        let err = validate_global_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("credentialProcess"));
    }

    #[test]
    fn test_oss_config_rejects_security_token_without_static_credentials() {
        let raw = r#"
        {
          "registryFsVersion": "v2",
          "ioEngine": 0,
          "ossConfig": {
            "enable": true,
            "defaultEndpoint": "https://oss-cn-hangzhou.aliyuncs.com",
            "defaultRegion": "cn-hangzhou",
            "securityToken": "token"
          }
        }"#;
        let mut cfg: GlobalConfig = serde_json::from_str(raw).expect("parse");
        cfg.normalize_compat_fields();
        let err = validate_global_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("securityToken"));
    }

    #[test]
    fn test_image_config_validate_remote_require_repo() {
        let raw = r#"
        {
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{"index":"","data":""}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        let err = validate_image_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("repoBlobUrl"));
    }

    #[test]
    fn test_image_config_validate_remote_allows_layer_repo_blob_url() {
        let raw = r#"
        {
          "lowers":[{
            "digest":"sha256:abc",
            "repoBlobUrl":"https://example.io/v2/repo/blobs",
            "size":1234
          }],
          "upper":{"index":"","data":""}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        validate_image_config(&cfg).expect("layer repoBlobUrl should satisfy remote lower");
    }

    #[test]
    fn test_layer_config_uuid_serde_compat() {
        let legacy = r#"{"file":"/tmp/snapshot.commit","size":1234}"#;
        let cfg: LayerConfig = serde_json::from_str(legacy).expect("parse legacy layer");
        assert_eq!(cfg.uuid, "");

        let with_uuid = r#"{
          "file":"/tmp/snapshot.commit",
          "uuid":"11111111-2222-3333-4444-555555555555",
          "size":1234
        }"#;
        let cfg: LayerConfig = serde_json::from_str(with_uuid).expect("parse uuid layer");
        assert_eq!(cfg.uuid, "11111111-2222-3333-4444-555555555555");
        let serialized = serde_json::to_value(&cfg).expect("serialize layer");
        assert_eq!(serialized["uuid"], "11111111-2222-3333-4444-555555555555");
    }

    #[test]
    fn test_layer_config_repo_blob_url_serde_compat() {
        let legacy = r#"{"digest":"sha256:abc","size":1234}"#;
        let cfg: LayerConfig = serde_json::from_str(legacy).expect("parse legacy layer");
        assert_eq!(cfg.repo_blob_url, "");
        assert_eq!(
            cfg.effective_repo_blob_url("https://example.io/v2/repo/blobs"),
            "https://example.io/v2/repo/blobs"
        );
        let serialized = serde_json::to_value(&cfg).expect("serialize layer");
        assert!(serialized.get("repoBlobUrl").is_none());

        let with_repo = r#"{
          "digest":"sha256:abc",
          "repoBlobUrl":"https://layer.example/v2/repo/blobs",
          "size":1234
        }"#;
        let cfg: LayerConfig = serde_json::from_str(with_repo).expect("parse layer repoBlobUrl");
        assert_eq!(cfg.repo_blob_url, "https://layer.example/v2/repo/blobs");
        assert_eq!(
            cfg.effective_repo_blob_url("https://image.example/v2/repo/blobs"),
            "https://layer.example/v2/repo/blobs"
        );
        let serialized = serde_json::to_value(&cfg).expect("serialize layer");
        assert_eq!(
            serialized["repoBlobUrl"],
            "https://layer.example/v2/repo/blobs"
        );
    }

    #[test]
    fn test_image_config_validate_upper_partial() {
        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{"index":"/tmp/upper.idx","data":""}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        let err = validate_image_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("set together"));
    }

    #[test]
    fn test_upper_mode_json_values() {
        assert_eq!(
            serde_json::to_string(&UpperMode::Sparse).expect("serialize sparse mode"),
            "\"sparse\""
        );
        assert_eq!(
            serde_json::to_string(&UpperMode::LogStructured)
                .expect("serialize log-structured mode"),
            "\"logStructured\""
        );
        assert_eq!(
            serde_json::to_string(&UpperMode::HybridLogStructured)
                .expect("serialize hybrid log-structured mode"),
            "\"hybridLogStructured\""
        );
        assert_eq!(
            serde_json::from_str::<UpperMode>("\"sparse\"").expect("deserialize sparse mode"),
            UpperMode::Sparse
        );
        assert_eq!(
            serde_json::from_str::<UpperMode>("\"logStructured\"")
                .expect("deserialize log-structured mode"),
            UpperMode::LogStructured
        );
        assert_eq!(
            serde_json::from_str::<UpperMode>("\"hybridLogStructured\"")
                .expect("deserialize hybrid log-structured mode"),
            UpperMode::HybridLogStructured
        );
    }

    #[test]
    fn test_image_config_validate_legacy_log_structured_upper_without_mode() {
        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{"index":"/tmp/upper.idx","data":"/tmp/upper.data"}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        validate_image_config(&cfg).expect("legacy log upper should validate");
        assert_eq!(cfg.upper.mode, None);
        assert_eq!(cfg.upper.writable_mode(), UpperMode::LogStructured);
    }

    #[test]
    fn test_image_config_validate_legacy_data_only_upper_without_mode_fails() {
        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{"data":"/tmp/upper.data"}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        let err = validate_image_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("set together"));
    }

    #[test]
    fn test_image_config_validate_sparse_upper_without_index() {
        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{"mode":"sparse","data":"/tmp/upper.data"}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        validate_image_config(&cfg).expect("sparse upper without index should validate");
        assert_eq!(cfg.upper.writable_mode(), UpperMode::Sparse);
    }

    #[test]
    fn test_image_config_validate_log_structured_upper_requires_index() {
        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{"mode":"logStructured","data":"/tmp/upper.data"}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        let err = validate_image_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("set together"));
    }

    #[test]
    fn test_image_config_validate_hybrid_log_structured_upper_requires_index() {
        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{"mode":"hybridLogStructured","data":"/tmp/upper.data"}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        let err = validate_image_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("set together"));

        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{
            "mode":"hybridLogStructured",
            "index":"/tmp/upper.idx",
            "data":"/tmp/upper.data"
          }
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        validate_image_config(&cfg).expect("hybrid upper should validate");
        assert_eq!(cfg.upper.writable_mode(), UpperMode::HybridLogStructured);
    }

    #[test]
    fn test_image_config_validate_upper_target_requires_complete_data_index() {
        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{"mode":"sparse","data":"/tmp/upper.data","target":"/tmp/upper.target"}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        let err = validate_image_config(&cfg).expect_err("should fail");
        assert!(err
            .to_string()
            .contains("require complete upper.data/index"));
    }

    #[test]
    fn test_image_config_validate_upper_gzip_index_requires_target() {
        let raw = r#"
        {
          "repoBlobUrl":"https://example.io/v2/repo/blobs",
          "lowers":[{"digest":"sha256:abc","size":1234}],
          "upper":{"index":"/tmp/upper.idx","data":"/tmp/upper.data","gzipIndex":"/tmp/upper.gzi"}
        }"#;
        let cfg: ImageConfig = serde_json::from_str(raw).expect("parse");
        let err = validate_image_config(&cfg).expect_err("should fail");
        assert!(err.to_string().contains("requires upper.target"));
    }

    #[test]
    fn test_image_config_resolves_relative_local_paths_against_config_dir() {
        let image_path = Path::new("/tmp/agentenv/overlaybd/overlaybd-image.json");
        let mut cfg = ImageConfig {
            repo_blob_url: String::new(),
            lowers: vec![LayerConfig {
                gzip_index: "../gzip/base.index".to_string(),
                file: "../rootfs/base.commit".to_string(),
                target_file: "./target.commit".to_string(),
                dir: "./download/layer-1".to_string(),
                repo_blob_url: String::new(),
                digest: String::new(),
                uuid: String::new(),
                target_digest: String::new(),
                size: 0,
            }],
            upper: UpperConfig {
                mode: Some(UpperMode::LogStructured),
                index: "./upper.index".to_string(),
                data: "./upper.data".to_string(),
                target: String::new(),
                gzip_index: String::new(),
            },
            result_file: "./result.txt".to_string(),
            download_override: Some(DownloadConfig::default()),
            acceleration_layer: false,
            record_trace_path: "./trace.log".to_string(),
        };

        resolve_image_config_local_paths(image_path, &mut cfg);

        assert_eq!(cfg.lowers[0].gzip_index, "/tmp/agentenv/gzip/base.index");
        assert_eq!(cfg.lowers[0].file, "/tmp/agentenv/rootfs/base.commit");
        assert_eq!(
            cfg.lowers[0].target_file,
            "/tmp/agentenv/overlaybd/target.commit"
        );
        assert_eq!(
            cfg.lowers[0].dir,
            "/tmp/agentenv/overlaybd/download/layer-1"
        );
        assert_eq!(cfg.upper.index, "/tmp/agentenv/overlaybd/upper.index");
        assert_eq!(cfg.upper.data, "/tmp/agentenv/overlaybd/upper.data");
        assert_eq!(cfg.upper.mode, Some(UpperMode::LogStructured));
        assert_eq!(cfg.result_file, "/tmp/agentenv/overlaybd/result.txt");
        assert_eq!(cfg.record_trace_path, "/tmp/agentenv/overlaybd/trace.log");
    }
}
