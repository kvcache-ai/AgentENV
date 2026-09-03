use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub(crate) mod image;
pub(crate) mod network;
use anyhow::{anyhow, bail, Context, Result};
use confique::Config;
pub use image::{
    ImageCacheConfig, ImageConfig, ImageRemoteBlocksCacheConfig, ImageResolverConfig,
    ResolvedImageCacheConfig, ResolvedImageCacheGcConfig,
};
pub use network::{NetworkConfig, NetworkEgressConfig, NetworkInternalConfig};
use overlaybd::config::UpperMode;
use serde::Deserialize;

use crate::virtualization::VirtualizationMode;

const ENV_CONFIG_PATH: &str = "AENV_CONFIG_PATH";

#[cfg(test)]
const TEST_ACCESS_TOKEN_HASH_SEED: &str = "agentenv-unit-test-access-token-seed";

#[derive(Debug, Deserialize)]
struct SetupDependencyManifest {
    firecracker: ManifestVirtualizationDownloads,
    kernel: ManifestVirtualizationDownloads,
    tools: ManifestTools,
    overlaybd: ManifestDownload,
    #[serde(rename = "regclient")]
    regclient: ManifestDownload,
}

#[derive(Debug, Deserialize)]
struct ManifestDownload {
    version: String,
}

#[derive(Debug, Deserialize)]
struct ManifestVirtualizationDownloads {
    kvm: ManifestDownload,
    pvm: ManifestDownload,
}

impl ManifestVirtualizationDownloads {
    fn for_mode(&self, mode: VirtualizationMode) -> &ManifestDownload {
        match mode {
            VirtualizationMode::Kvm => &self.kvm,
            VirtualizationMode::Pvm => &self.pvm,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ManifestTools {
    version: String,
}

impl SetupDependencyManifest {
    fn get() -> &'static Self {
        use std::sync::LazyLock;

        static MANIFEST: LazyLock<SetupDependencyManifest> = LazyLock::new(|| {
            toml::from_str(include_str!("../config/deps_manifest.toml"))
                .expect("bundled setup dependency manifest is valid")
        });
        &MANIFEST
    }
}

pub(crate) fn regctl_path(deps_path: &Path) -> PathBuf {
    deps_path
        .join("regctl")
        .join(&SetupDependencyManifest::get().regclient.version)
        .join("regctl")
}

#[derive(Debug, Clone, Config)]
pub struct AppConfig {
    #[config(
        env = "AENV_HOME_PATH",
        parse_env = parse_required_path,
        default = "/var/lib/aenv"
    )]
    pub home_path: PathBuf,
    #[config(
        env = "AENV_RUNTIME_PATH",
        parse_env = parse_required_path,
        default = "/run/aenv"
    )]
    pub runtime_path: PathBuf,
    #[config(
        env = "AENV_DEPS_PATH",
        parse_env = parse_required_path,
        default = "$AENV_HOME/deps"
    )]
    pub deps_path: PathBuf,
    #[config(default = "kvm", env = "AENV_VIRTUALIZATION_MODE")]
    pub virtualization_mode: VirtualizationMode,
    #[config(nested)]
    pub firecracker: FirecrackerConfig,
    #[config(nested)]
    pub kernel: KernelConfig,
    #[config(nested)]
    pub tools: ToolsConfig,
    pub overlaybd: Option<OverlaybdDependencyConfig>,
    #[config(nested)]
    pub machine: MachineConfig,
    #[config(nested)]
    pub backend: BackendConfig,
    #[config(nested)]
    pub envd: EnvdConfig,
    #[config(nested)]
    pub sandbox: SandboxConfig,
    #[config(nested)]
    pub volume: VolumeConfig,
    #[config(nested)]
    pub orchestrator: OrchestratorConfig,
    #[config(nested)]
    pub snapshot: SnapshotConfig,
    #[config(nested)]
    pub ublk: UblkTomlConfig,
    #[config(nested)]
    pub observability: ObservabilityConfig,
    #[config(nested)]
    pub cluster: ClusterConfig,
    #[config(nested)]
    pub node_identity: NodeIdentityConfig,
    #[config(nested)]
    pub memory_snapshot: MemorySnapshotConfig,
    #[config(nested)]
    pub template_build: TemplateBuildConfig,
    #[config(nested)]
    pub pool: PoolTomlConfig,
    #[config(nested)]
    pub p2p: P2pConfig,
    #[config(nested)]
    pub image: ImageConfig,
    #[config(nested)]
    pub sandbox_proxy: SandboxProxyConfig,
    #[config(nested)]
    pub network: NetworkConfig,
    #[config(nested)]
    pub custom_extension: CustomExtensionConfig,
}

#[derive(Debug, Deserialize, Clone, Config)]
pub struct BackendConfig {
    pub posix_fs: Option<PosixFsBackendConfig>,
    pub oss: Option<OssBackendConfig>,
}

#[derive(Debug, Deserialize, Clone, Config)]
pub struct PosixFsBackendConfig {
    #[config(
        default = "$AENV_HOME/snapshot-store",
        env = "AENV_SNAPSHOT_STORE",
        parse_env = parse_required_path
    )]
    pub snapshot_store: PathBuf,
}

#[derive(Debug, Clone, Config)]
pub struct FirecrackerConfig {
    pub binary_path: Option<PathBuf>,
    pub boot_args: Option<String>,
    pub allowed_extra_boot_args_prefixes: Option<Vec<String>>,
    #[config(default = 3u64)]
    pub socket_timeout_secs: u64,
    #[config(default = 1u64)]
    pub socket_poll_ms: u64,
    pub version: Option<String>,
    pub url: Option<String>,
    /// Optional override for the per-sandbox Firecracker work root.
    /// Defaults to `$AENV_HOME/firecracker-work` after normalization.
    #[config(env = "AENV_FIRECRACKER_WORK_DIR", parse_env = parse_required_path)]
    pub work_dir: Option<PathBuf>,
    /// Optional override for the persistent Firecracker serial output directory.
    /// Defaults to `$AENV_HOME/logs/serial` after normalization.
    /// Files are grouped under `{serial_dir}/{sandbox_id}/`.
    #[config(env = "AENV_FIRECRACKER_SERIAL_DIR", parse_env = parse_required_path)]
    pub serial_dir: Option<PathBuf>,
    /// Optional Firecracker log level (e.g. "Error", "Warning", "Info", "Debug", "Trace").
    /// When set (non-empty), Firecracker logging is enabled and written to a
    /// `firecracker.log` file in the same directory as the Firecracker stdout log.
    pub log_level: Option<String>,
}

#[derive(Debug, Config, Clone)]
pub struct PoolTomlConfig {
    #[config(default = 2usize)]
    pub low_watermark: usize,
    #[config(default = 64usize)]
    pub high_watermark: usize,
    #[config(nested)]
    pub network: PoolComponentConfig,
    #[config(nested)]
    pub block: PoolComponentConfig,
    #[config(nested)]
    pub firecracker: FirecrackerProcessPoolConfig,
}

#[derive(Debug, Config, Clone)]
pub struct PoolComponentConfig {
    #[config(default = true)]
    pub enabled: bool,
    #[config(default = true)]
    pub maintenance_enabled: bool,
    #[config(default = true)]
    pub startup_prewarm: bool,
}

#[derive(Debug, Config, Clone)]
pub struct FirecrackerProcessPoolConfig {
    #[config(default = true)]
    pub enabled: bool,
    #[config(default = true)]
    pub maintenance_enabled: bool,
    #[config(default = true)]
    pub startup_prewarm: bool,
    #[config(default = 4usize)]
    pub fill_concurrency: usize,
}

#[derive(Debug, Clone)]
pub struct ResolvedFirecrackerPoolConfig {
    pub pool: warm_pool::PoolConfig,
    pub fill_concurrency: usize,
}

#[derive(Debug, Clone, Config)]
pub struct KernelConfig {
    pub image_path: Option<PathBuf>,
    pub version: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OverlaybdDependencyConfig {
    pub version: String,
    pub url: Option<String>,
    pub package_url: Option<String>,
}

#[derive(Debug, Clone, Config)]
pub struct ToolsConfig {
    /// Explicit source path for importing a local tools drive ext4 file.
    pub drive_path: Option<PathBuf>,
    /// Immutable tools drive release version (e.g. "0.1.0").
    pub version: Option<String>,
    /// Container registry URL template (e.g. "ghcr.io/org/agentenv-tools:{version}").
    pub url: Option<String>,
    /// Control plane port inside the VM (default: 49983).
    #[config(default = 49983u16)]
    pub control_plane_port: u16,
}

#[derive(Debug, Config, Clone)]
pub struct SandboxProxyConfig {
    #[config(
        default = [],
        env = "AENV_SANDBOX_PROXY_DOMAINS",
        parse_env = confique::env::parse::list_by_comma
    )]
    pub domains: Vec<String>,
}

#[derive(Debug, Config, Clone)]
pub struct EnvdConfig {
    #[config(default = "0.5.15")]
    pub version: String,
    #[config(default = 60u64)]
    pub init_timeout_secs: u64,
    #[config(default = 3u64)]
    pub poll_ms: u64,
}

#[derive(Clone, Config)]
pub struct SandboxConfig {
    #[config(
        env = "AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED",
        parse_env = parse_trimmed_string
    )]
    pub access_token_hash_seed: Option<String>,
}

impl std::fmt::Debug for SandboxConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxConfig")
            .field(
                "access_token_hash_seed",
                &self.access_token_hash_seed.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Debug, Config, Clone)]
pub struct VolumeConfig {
    /// Operator-configured maximum persistent volume size in MiB.
    #[config(default = 262144u64)]
    pub max_size_mb: u64,
    /// Maximum number of persistent volumes a sandbox may mount.
    #[config(default = 4usize)]
    pub max_volume_count: usize,
}

#[derive(Debug, Config, Clone)]
pub struct MachineConfig {
    #[config(default = 2u32)]
    pub vcpu_count: u32,
    #[config(default = 1024u32)]
    pub mem_size_mib: u32,
    #[config(nested)]
    pub disk_rate_limit: DiskRateLimitConfig,
}

#[derive(Debug, Config, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DiskRateLimitConfig {
    /// Enable per-sandbox disk I/O rate limiting via Firecracker's virtio-blk rate limiter.
    #[config(default = false)]
    pub enabled: bool,
    /// Sustained disk bandwidth limit in bytes per second (0 = unlimited).
    #[config(default = 0u64)]
    pub bandwidth_bytes_per_sec: u64,
    /// One-time bandwidth burst in bytes, granted once when the VM starts (maps
    /// to Firecracker's `one_time_burst`). It is a separate allowance consumed
    /// before the sustained bucket and is not replenished after use, so it only
    /// absorbs the initial I/O spike; it does not raise the steady-state rate.
    #[config(default = 0u64)]
    pub bandwidth_burst_bytes: u64,
    /// Sustained IOPS limit (0 = unlimited).
    #[config(default = 0u64)]
    pub iops: u64,
    /// One-time IOPS burst, granted once when the VM starts (maps to
    /// Firecracker's `one_time_burst`). Consumed before the sustained bucket and
    /// not replenished, so it only absorbs the initial spike, not steady state.
    #[config(default = 0u64)]
    pub iops_burst: u64,
}

#[derive(Debug, Config, Clone)]
pub struct SnapshotConfig {
    #[config(
        env = "AENV_SNAPSHOT_LOCAL_CACHE_PATH",
        parse_env = parse_required_path,
        default = "$AENV_HOME/snapshot-local-cache"
    )]
    pub local_cache_path: PathBuf,
    #[config(default = "posix_fs")]
    pub repository_backend: SnapshotRepositoryBackendKind,
    /// When true, snapshot artifacts are published to and fetched from the P2P
    /// transport after each commit. Has no effect unless `[p2p].enabled` is also true.
    #[config(default = true)]
    pub p2p_enabled: bool,
    #[config(nested)]
    pub image_publish: SnapshotImagePublishConfig,
}

#[derive(Debug, Config, Clone)]
pub struct SnapshotImagePublishConfig {
    #[config(default = false)]
    pub enabled: bool,
}

/// Bucket addressing style for the S3-compatible snapshot backend.
///
/// When unset, the backend auto-detects the style: Alibaba OSS and
/// bucket-in-endpoint hosts use virtual-host addressing, and everything else
/// falls back to path style. Set this explicitly when a provider's required
/// or preferred style differs from that default.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OssAddressingStyle {
    Path,
    Virtual,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OssBackendConfig {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: Option<String>,
    #[serde(alias = "credentialProcess", alias = "credential-process")]
    pub credential_process: Option<String>,
    pub access_key_id: Option<String>,
    pub access_key_secret: Option<String>,
    pub security_token: Option<String>,
    pub region: Option<String>,
    #[serde(alias = "addressingStyle", alias = "addressing-style")]
    pub addressing_style: Option<OssAddressingStyle>,
    pub cache_max_size_gb: Option<u64>,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotImageStoragePolicy {
    #[default]
    ObjectStorage,
    SourceRegistry,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotRepositoryBackendKind {
    PosixFs,
    Oss,
}

#[derive(Debug, Config, Clone)]
pub struct UblkTomlConfig {
    /// Path to the `uvm-ublk-daemon` binary.
    #[config(env = "AENV_UBLK_DAEMON_BINARY_PATH", parse_env = parse_required_path)]
    pub daemon_binary_path: Option<PathBuf>,
    /// Unix socket path for the ublk daemon.
    #[config(default = "$AENV_RUNTIME/ublk-daemon.sock")]
    pub daemon_socket_path: PathBuf,
    /// Optional override for the ublk daemon log file.
    /// Defaults to `$AENV_HOME/logs/ublk-daemon.log` after normalization.
    pub daemon_log_path: Option<PathBuf>,
    /// HTTP listen address for ublk daemon metrics. Empty string disables it.
    #[config(env = "AENV_UBLK_DAEMON_METRICS_LISTEN_ADDR", parse_env = parse_trimmed_string, default = "0.0.0.0:9103")]
    pub daemon_metrics_listen_addr: String,
    #[config(nested)]
    pub overlaybd: UblkOverlaybdTomlConfig,
}

#[derive(Debug, Config, Clone)]
pub struct UblkOverlaybdTomlConfig {
    #[config(default = "$AENV_HOME/overlaybd/overlaybd-global.json")]
    pub global_config_path: PathBuf,
    #[config(default = false)]
    pub read_only: bool,
    /// Runtime upper format for newly materialized writable OverlayBD images.
    /// Existing source uppers keep their own mode. Default: `hybridLogStructured`.
    #[config(default = "hybridLogStructured")]
    pub runtime_upper_mode: UpperMode,
    /// Permit shrinking a fresh cold-sandbox rootfs. Default: `false`.
    #[config(default = false)]
    pub allow_shrink: bool,
    /// Timeout for the OverlayBD resize tool. Default: `120`.
    #[config(default = 120u64)]
    pub resize_timeout_secs: u64,
    /// Enable overlaybd layer-level background download. Default: `false`.
    #[config(default = false)]
    pub download_enable: bool,
    /// Timeout for overlaybd P2P provider lookup requests. Default: `300`.
    #[config(default = 300u64)]
    pub p2p_lookup_timeout_ms: u64,
    /// Timeout for overlaybd P2P foreground range fetches. Default: `2000`.
    #[config(default = 2000u64)]
    pub p2p_fetch_range_timeout_ms: u64,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OverlaybdCompressionAlgorithm {
    #[default]
    Lz4,
    Zstd,
}

#[derive(Debug, Config, Clone)]
pub struct MemorySnapshotConfig {
    #[config(default = "$AENV_HOME/overlaybd/mem-overlaybd-global.json")]
    pub overlaybd_global_config_path: PathBuf,
    /// Enable Firecracker KVM dirty-page tracking for memory snapshots.
    /// Default: true; set the environment variable to false to use mincore.
    #[config(env = "AGENTENV_MEMORY_SNAPSHOT_TRACK_DIRTY_PAGES", default = true)]
    pub track_dirty_pages: bool,
    #[config(default = false)]
    pub compression_enabled: bool,
    #[config(default = "lz4")]
    pub compression_algorithm: OverlaybdCompressionAlgorithm,
    /// Number of blocking threads used to compress 4KiB blocks within a
    /// memory layer. 1 = sequential (identical output layout at any value).
    #[config(default = 1)]
    pub compression_workers: usize,
    #[config(nested)]
    pub background_download: MemorySnapshotBackgroundDownloadConfig,
}

/// Compression settings applied when a template build captures its snapshot.
/// Independent of `[memory_snapshot]`: template builds consult only this
/// section, for both memory layers and the sealed rootfs read-write layer.
#[derive(Debug, Config, Clone)]
pub struct TemplateBuildConfig {
    #[config(default = false)]
    pub compression_enabled: bool,
    #[config(default = "lz4")]
    pub compression_algorithm: OverlaybdCompressionAlgorithm,
    /// Number of blocking threads used to compress 4KiB blocks within a
    /// layer. 1 = sequential (identical output layout at any value).
    #[config(default = 1)]
    pub compression_workers: usize,
}

#[derive(Debug, Config, Clone)]
pub struct MemorySnapshotBackgroundDownloadConfig {
    #[config(default = true)]
    pub enable: bool,
    #[config(default = 0i32)]
    pub delay: i32,
    #[config(default = 1i32)]
    pub delay_extra: i32,
    #[config(default = 5i32)]
    pub try_cnt: i32,
    #[config(default = 16_777_216u32)]
    pub block_size: u32,
    #[config(default = 4usize)]
    pub concurrency: usize,
    /// Cap on in-flight background download blocks enforced by the cache
    /// backend's download queue, shared by every layer download on the node
    /// (bounds total scratch memory). Fixed when the backend is created;
    /// per-image overrides never resize it.
    #[config(default = 16usize)]
    pub max_inflight_blocks: usize,
}

impl MemorySnapshotBackgroundDownloadConfig {
    pub(crate) fn to_overlaybd_download_config(&self) -> overlaybd::config::DownloadConfig {
        overlaybd::config::DownloadConfig {
            enable: self.enable,
            delay: self.delay,
            delay_extra: self.delay_extra,
            // Memory-snapshot background download is intentionally unthrottled;
            // OSS/registry image configs may still carry maxMBps and it keeps
            // working there via the shared throttle.
            max_mbps: 0,
            try_cnt: self.try_cnt,
            block_size: self.block_size,
            concurrency: self.concurrency,
            max_inflight_blocks: self.max_inflight_blocks,
            // Not a memory-snapshot knob: the cache scheduler default applies.
            max_concurrent_files: overlaybd::config::DownloadConfig::default().max_concurrent_files,
        }
    }
}

#[derive(Debug, Config, Clone)]
pub struct ObservabilityConfig {
    /// Enables the node/admin observability service exposed by the API layer.
    #[config(default = true)]
    pub enabled: bool,
    #[config(nested)]
    pub scheduler_report: ObservabilitySchedulerReportConfig,
}

#[derive(Debug, Config, Clone)]
pub struct ObservabilitySchedulerReportConfig {
    /// Enables scheduler heartbeat reporting. The scheduler endpoint is read
    /// from `[cluster].scheduler_endpoint`.
    #[config(default = false, env = "AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED")]
    pub enabled: bool,
    #[config(default = 5u64, env = "AENV_OBSERVABILITY_REPORT_INTERVAL_SECS")]
    pub interval_secs: u64,
    /// Node HTTP endpoint advertised to schedulers using heartbeat discovery.
    #[config(env = "AENV_OBSERVABILITY_NODE_ENDPOINT", parse_env = parse_trimmed_string)]
    pub node_endpoint: Option<String>,
    /// Shared secret used to authenticate heartbeat-based registration.
    #[config(
        env = "AENV_OBSERVABILITY_REGISTRATION_TOKEN",
        parse_env = parse_trimmed_string
    )]
    pub registration_token: Option<String>,
}

#[derive(Debug, Config, Clone)]
pub struct ClusterConfig {
    /// Shared gRPC scheduler endpoint for cluster-level services.
    #[config(
        env = "AENV_OBSERVABILITY_SCHEDULER_ENDPOINT",
        parse_env = parse_trimmed_string
    )]
    pub scheduler_endpoint: Option<String>,
}

#[derive(Debug, Config, Clone)]
pub struct NodeIdentityConfig {
    #[config(env = "AENV_NODE_ID")]
    pub node_id: Option<String>,
    #[config(env = "AENV_CLUSTER_ID")]
    pub cluster_id: Option<String>,
    #[config(env = "AENV_SERVICE_INSTANCE_ID")]
    pub service_instance_id: Option<String>,
}

#[derive(Debug, Config, Clone)]
pub struct OrchestratorConfig {
    #[config(default = 1000u64)]
    pub auto_evict_interval_ms: u64,
    #[config(default = 15u64)]
    pub default_sandbox_timeout_secs: u64,
    #[config(default = 300u64)]
    pub auto_resume_min_sandbox_timeout_secs: u64,
    #[config(
        default = "$AENV_HOME/persisted-sandboxes",
        env = "AENV_PERSISTED_SANDBOX_STORE_PATH",
        parse_env = parse_required_path
    )]
    pub persisted_sandbox_store_path: PathBuf,
}

/// Custom extension service integration.
///
/// The custom extension is an external HTTP service that extends node
/// behavior. Its only current capability is sandbox lifecycle hooks
/// (`/sandbox-hook/*`), but more may be added in the future.
#[derive(Debug, Config, Clone)]
pub struct CustomExtensionConfig {
    /// Optional HTTP base URL of the custom extension service.
    /// When unset, the custom extension integration is fully disabled.
    #[config(env = "AENV_CUSTOM_EXTENSION_URL", parse_env = parse_trimmed_string)]
    pub url: Option<String>,
    /// Timeout for custom extension HTTP calls, in milliseconds.
    #[config(default = 5000u64)]
    pub timeout_ms: u64,
}

#[derive(Debug, Config, Clone)]
pub struct P2pConfig {
    #[config(default = false)]
    pub enabled: bool,
    #[config(default = "iroh")]
    pub transport: crate::p2p::P2pTransportKind,
    #[config(default = "$AENV_HOME/p2p/store")]
    pub store_dir: PathBuf,
    #[config(default = "0.0.0.0:0")]
    pub listen_addr: String,
    #[config(default = 5000u64)]
    pub lookup_timeout_ms: u64,
    #[config(default = 30000u64)]
    pub fetch_timeout_ms: u64,
    #[config(default = 5u64)]
    pub peer_discovery_refresh_interval_secs: u64,
}

macro_rules! impl_config_default {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl Default for $ty {
                fn default() -> Self {
                    <$ty as confique::Config>::builder()
                        .load()
                        .expect(concat!(stringify!($ty), " defaults are complete"))
                }
            }
        )+
    };
}
pub(crate) use impl_config_default;

impl_config_default!(
    AppConfig,
    BackendConfig,
    PosixFsBackendConfig,
    FirecrackerConfig,
    PoolTomlConfig,
    PoolComponentConfig,
    FirecrackerProcessPoolConfig,
    KernelConfig,
    ToolsConfig,
    SandboxProxyConfig,
    EnvdConfig,
    SandboxConfig,
    VolumeConfig,
    MachineConfig,
    SnapshotConfig,
    SnapshotImagePublishConfig,
    UblkTomlConfig,
    UblkOverlaybdTomlConfig,
    MemorySnapshotConfig,
    MemorySnapshotBackgroundDownloadConfig,
    ObservabilityConfig,
    ObservabilitySchedulerReportConfig,
    ClusterConfig,
    NodeIdentityConfig,
    OrchestratorConfig,
    P2pConfig,
    CustomExtensionConfig,
);

impl AppConfig {
    fn manifest_firecracker(&self) -> &ManifestDownload {
        SetupDependencyManifest::get()
            .firecracker
            .for_mode(self.virtualization_mode)
    }

    fn manifest_kernel(&self) -> &ManifestDownload {
        SetupDependencyManifest::get()
            .kernel
            .for_mode(self.virtualization_mode)
    }

    pub fn resolved_firecracker_binary_path(&self) -> PathBuf {
        self.firecracker.binary_path.clone().unwrap_or_else(|| {
            let version = self
                .firecracker
                .version
                .as_deref()
                .unwrap_or(&self.manifest_firecracker().version);
            self.deps_path
                .join("firecracker")
                .join(version)
                .join("firecracker")
        })
    }

    pub fn resolved_kernel_image_path(&self) -> PathBuf {
        self.kernel.image_path.clone().unwrap_or_else(|| {
            let version = self
                .kernel
                .version
                .as_deref()
                .unwrap_or(&self.manifest_kernel().version);
            self.deps_path
                .join("kernel")
                .join(version)
                .join("vmlinux.bin")
        })
    }

    pub fn resolved_tools_version(&self) -> &str {
        self.tools
            .version
            .as_deref()
            .unwrap_or(&SetupDependencyManifest::get().tools.version)
    }

    pub fn resolved_tools_drive_path_for_version(&self, version: &str) -> Result<PathBuf> {
        let version = semver::Version::parse(version).with_context(|| {
            format!(
                "invalid tools drive version '{version}': expected SemVer without build metadata"
            )
        })?;
        if !version.build.is_empty() {
            bail!("invalid tools drive version '{version}': build metadata is not supported");
        }
        Ok(self
            .deps_path
            .join("tools")
            .join(version.to_string())
            .join("tools.ext4"))
    }

    pub fn resolved_tools_drive_path(&self) -> Result<PathBuf> {
        self.resolved_tools_drive_path_for_version(self.resolved_tools_version())
    }

    pub(crate) fn resolved_overlaybd_oci_converter_id(&self) -> String {
        let version = self
            .overlaybd
            .as_ref()
            .map(|overlaybd| overlaybd.version.as_str())
            .unwrap_or(&SetupDependencyManifest::get().overlaybd.version);
        format!("overlaybd-oci:{version}:agentenv-cache-v1")
    }

    /// Path of the generated overlaybd global config dedicated to the offline
    /// C++ conversion tools (`overlaybd-apply`). It mirrors the runtime global
    /// config but points at an isolated cacheDir: the C++ file cache manages
    /// cacheDir as flat files and evicts (truncate+unlink) whatever it finds,
    /// which would destroy the Rust runtime cache's per-entry directories if
    /// both shared `remote-blocks`.
    pub(crate) fn resolved_overlaybd_convert_global_config_path(&self) -> PathBuf {
        self.ublk
            .overlaybd
            .global_config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("convert-overlaybd-global.json")
    }

    /// Path of the generated overlaybd global config dedicated to the offline
    /// C++ resize tool (`overlaybd-resize`). It mirrors the runtime global
    /// config but points at an isolated cacheDir: the C++ file cache manages
    /// cacheDir as flat files and evicts (truncate+unlink) whatever it finds,
    /// which would destroy the Rust runtime cache's per-entry directories if
    /// both shared `remote-blocks`.
    pub(crate) fn resolved_overlaybd_resize_global_config_path(&self) -> PathBuf {
        self.ublk
            .overlaybd
            .global_config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("resize-overlaybd-global.json")
    }

    /// Resolve the cpu-template-helper binary path derived from deps_path + version.
    /// Returns `None` if the binary does not exist on disk.
    pub fn resolved_cpu_template_helper(&self) -> Option<PathBuf> {
        let version = self
            .firecracker
            .version
            .as_deref()
            .unwrap_or(&self.manifest_firecracker().version);
        let path = self
            .deps_path
            .join("firecracker")
            .join(version)
            .join("cpu-template-helper");
        path.exists().then_some(path)
    }

    pub fn resolved_regctl_binary(&self) -> PathBuf {
        regctl_path(&self.deps_path)
    }

    pub(crate) fn image_cache_layout(&self) -> ResolvedImageCacheConfig {
        self.image.cache.layout()
    }

    pub fn network_pool_config(&self) -> warm_pool::PoolConfig {
        let pool = &self.pool.network;

        warm_pool::PoolConfig {
            low_watermark: self.pool.low_watermark,
            high_watermark: self.pool.high_watermark,
            maintenance_enabled: pool.enabled && pool.maintenance_enabled,
            startup_prewarm: pool.startup_prewarm,
        }
    }

    pub fn block_pool_config(&self) -> Option<warm_pool::PoolConfig> {
        let pool = &self.pool.block;

        if !pool.enabled {
            return None;
        }

        Some(warm_pool::PoolConfig {
            low_watermark: self.pool.low_watermark,
            high_watermark: self.pool.high_watermark,
            // The ublk daemon uses async request-time refill because the
            // reusable device shape is image/size dependent.
            maintenance_enabled: false,
            startup_prewarm: pool.startup_prewarm,
        })
    }

    pub fn firecracker_pool_config(&self) -> Option<ResolvedFirecrackerPoolConfig> {
        let pool = &self.pool.firecracker;

        if !pool.enabled {
            return None;
        }

        Some(ResolvedFirecrackerPoolConfig {
            pool: warm_pool::PoolConfig {
                low_watermark: self.pool.low_watermark,
                high_watermark: self.pool.high_watermark,
                maintenance_enabled: pool.maintenance_enabled,
                startup_prewarm: pool.startup_prewarm,
            },
            fill_concurrency: pool.fill_concurrency,
        })
    }

    fn normalize(&mut self, config_dir: &Path) -> Result<()> {
        // Resolve config-owned filesystem paths relative to the active config file.
        // This preserves existing mounted-config behavior while letting confique own
        // layered value selection.
        self.home_path = resolve_relative_to(config_dir, &self.home_path);
        self.runtime_path = resolve_path(&self.home_path, config_dir, &self.runtime_path);
        self.deps_path = resolve_path(&self.home_path, config_dir, &self.deps_path);

        self.normalize_dependency_override_paths(config_dir);
        resolve_optional_config_path_or(
            &mut self.firecracker.work_dir,
            &self.home_path,
            config_dir,
            "firecracker-work",
        );
        resolve_optional_config_path_or(
            &mut self.firecracker.serial_dir,
            &self.home_path,
            config_dir,
            "logs/serial",
        );

        resolve_optional_config_path_or(
            &mut self.ublk.daemon_binary_path,
            &self.home_path,
            config_dir,
            "ublk/uvm-ublk-daemon",
        );
        self.ublk.daemon_socket_path = resolve_runtime_path(
            &self.home_path,
            &self.runtime_path,
            config_dir,
            &self.ublk.daemon_socket_path,
        );
        resolve_optional_config_path_or(
            &mut self.ublk.daemon_log_path,
            &self.home_path,
            config_dir,
            "logs/ublk-daemon.log",
        );

        self.ublk.overlaybd.global_config_path = resolve_path(
            &self.home_path,
            config_dir,
            &self.ublk.overlaybd.global_config_path,
        );

        ImageConfig::normalize(&mut self.image, config_dir, &self.home_path);

        self.memory_snapshot.overlaybd_global_config_path = resolve_path(
            &self.home_path,
            config_dir,
            &self.memory_snapshot.overlaybd_global_config_path,
        );
        self.orchestrator.persisted_sandbox_store_path = resolve_path(
            &self.home_path,
            config_dir,
            &self.orchestrator.persisted_sandbox_store_path,
        );

        self.snapshot.local_cache_path =
            resolve_path(&self.home_path, config_dir, &self.snapshot.local_cache_path);

        if self.snapshot.repository_backend == SnapshotRepositoryBackendKind::PosixFs {
            let posix_fs = self
                .backend
                .posix_fs
                .get_or_insert_with(PosixFsBackendConfig::default);
            posix_fs.snapshot_store =
                resolve_path(&self.home_path, config_dir, &posix_fs.snapshot_store);
        }

        self.p2p.store_dir = resolve_path(&self.home_path, config_dir, &self.p2p.store_dir);

        // Dirty-page tracking is a KVM-only default. Disable it before
        // validation so an existing PVM configuration needs no new override.
        if self.virtualization_mode == VirtualizationMode::Pvm {
            self.memory_snapshot.track_dirty_pages = false;
        }

        self.cluster.normalize();
        self.sandbox_proxy.normalize()?;

        Ok(())
    }

    fn normalize_dependency_override_paths(&mut self, config_dir: &Path) {
        let home_path = self.home_path.clone();

        if let Some(binary_path) = self.firecracker.binary_path.as_mut() {
            *binary_path = resolve_path(&home_path, config_dir, binary_path);
        }

        if let Some(image_path) = self.kernel.image_path.as_mut() {
            *image_path = resolve_path(&home_path, config_dir, image_path);
        }

        if let Some(drive_path) = self.tools.drive_path.as_mut() {
            *drive_path = resolve_path(&home_path, config_dir, drive_path);
        }
    }

    fn validate(&self) -> Result<()> {
        self.validate_pool_config()?;
        self.image.cache.gc.validate()?;
        NetworkConfig::validate(&self.network)?;
        if self.ublk.overlaybd.resize_timeout_secs == 0 {
            bail!("invalid ublk.overlaybd config: resize_timeout_secs must be > 0");
        }
        self.validate_memory_snapshot_options()?;
        self.validate_memory_snapshot_background_download()?;
        self.validate_overlaybd_global_config_paths()?;
        self.validate_disk_rate_limit()?;
        self.validate_volume_limits()?;
        Ok(())
    }

    fn validate_volume_limits(&self) -> Result<()> {
        if self.volume.max_size_mb == 0 {
            bail!("volume.max_size_mb must be greater than 0");
        }
        if self.volume.max_size_mb > u64::MAX / (1024 * 1024) {
            bail!(
                "volume.max_size_mb must be at most {}",
                u64::MAX / (1024 * 1024)
            );
        }
        if self.volume.max_volume_count == 0
            || self.volume.max_volume_count > crate::volume::MAX_VOLUME_MOUNTS
        {
            bail!(
                "volume.max_volume_count must be between 1 and {}",
                crate::volume::MAX_VOLUME_MOUNTS
            );
        }
        Ok(())
    }

    /// Reject internally inconsistent or out-of-range disk rate limit configs so
    /// operator mistakes fail at load time. Disabled sections are skipped: both
    /// the fresh-boot and snapshot-resume paths ignore all configured values when
    /// disabled, so dormant/pre-staged values must not block startup.
    ///
    /// When enabled: a one-time burst is meaningless without a nonzero sustained
    /// limit (`build_disk_rate_limiter` only creates a bucket when the sustained
    /// value is > 0, so a burst paired with a zero sustained limit is silently
    /// ignored), and every value must fit Firecracker's signed `i64` token-bucket
    /// fields (the consumer converts with `i64::try_from`, so an out-of-range
    /// value would otherwise only fail later at sandbox start).
    fn validate_disk_rate_limit(&self) -> Result<()> {
        let cfg = &self.machine.disk_rate_limit;
        if !cfg.enabled {
            return Ok(());
        }
        if cfg.bandwidth_burst_bytes > 0 && cfg.bandwidth_bytes_per_sec == 0 {
            bail!(
                "machine.disk_rate_limit: bandwidth_burst_bytes is set but \
                 bandwidth_bytes_per_sec is 0; a burst requires a nonzero sustained limit"
            );
        }
        if cfg.iops_burst > 0 && cfg.iops == 0 {
            bail!(
                "machine.disk_rate_limit: iops_burst is set but iops is 0; \
                 a burst requires a nonzero sustained limit"
            );
        }
        for (name, value) in [
            ("bandwidth_bytes_per_sec", cfg.bandwidth_bytes_per_sec),
            ("bandwidth_burst_bytes", cfg.bandwidth_burst_bytes),
            ("iops", cfg.iops),
            ("iops_burst", cfg.iops_burst),
        ] {
            if value > i64::MAX as u64 {
                bail!(
                    "machine.disk_rate_limit.{name} ({value}) exceeds the maximum \
                     supported value {}",
                    i64::MAX
                );
            }
        }
        Ok(())
    }

    fn validate_memory_snapshot_options(&self) -> Result<()> {
        let memory = &self.memory_snapshot;
        if !memory.track_dirty_pages {
            return Ok(());
        }
        if self.virtualization_mode == VirtualizationMode::Pvm {
            bail!(
                "memory_snapshot.track_dirty_pages=true is disabled in PVM mode because this combination has not been tested"
            );
        }
        Ok(())
    }

    /// Sanity-bound the memory-snapshot background download knobs so a legal
    /// config cannot allocate unbounded scratch or fan out unbounded requests.
    /// Peak scratch per active layer download is `block_size × concurrency`
    /// (the download chunk is `block_size` cache blocks fetched per request),
    /// and `max_inflight_blocks × block_size` node-wide.
    fn validate_memory_snapshot_background_download(&self) -> Result<()> {
        const MAX_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
        const MAX_CONCURRENCY: usize = 16;
        const MAX_SCRATCH_BYTES: u64 = 256 * 1024 * 1024;
        let cfg = &self.memory_snapshot.background_download;
        if cfg.concurrency == 0 {
            bail!("memory_snapshot.background_download.concurrency must be > 0");
        }
        if cfg.concurrency > MAX_CONCURRENCY {
            bail!("memory_snapshot.background_download.concurrency must be <= {MAX_CONCURRENCY}");
        }
        if u64::from(cfg.block_size) > MAX_BLOCK_SIZE {
            bail!("memory_snapshot.background_download.block_size must be <= {MAX_BLOCK_SIZE}");
        }
        if u64::from(cfg.block_size) * cfg.concurrency as u64 > MAX_SCRATCH_BYTES {
            bail!(
                "memory_snapshot.background_download block_size * concurrency must be <= \
                 {MAX_SCRATCH_BYTES} bytes"
            );
        }
        const MAX_INFLIGHT_BLOCKS: usize = 128;
        if cfg.max_inflight_blocks == 0 {
            bail!("memory_snapshot.background_download.max_inflight_blocks must be > 0");
        }
        if cfg.max_inflight_blocks > MAX_INFLIGHT_BLOCKS {
            bail!(
                "memory_snapshot.background_download.max_inflight_blocks must be <= \
                 {MAX_INFLIGHT_BLOCKS}"
            );
        }
        // Backend-wide scratch budget: every in-flight chunk may allocate one
        // block_size buffer, so the global product must stay bounded too.
        const MAX_GLOBAL_SCRATCH_BYTES: u64 = 2 * 1024 * 1024 * 1024;
        let global_scratch = u64::from(cfg.block_size)
            .checked_mul(cfg.max_inflight_blocks as u64)
            .ok_or_else(|| {
                anyhow::anyhow!("memory_snapshot.background_download scratch overflow")
            })?;
        if global_scratch > MAX_GLOBAL_SCRATCH_BYTES {
            bail!(
                "memory_snapshot.background_download block_size * max_inflight_blocks must be <= \
                 {MAX_GLOBAL_SCRATCH_BYTES} bytes"
            );
        }
        if cfg.delay < 0 {
            bail!("memory_snapshot.background_download.delay must be >= 0");
        }
        if cfg.try_cnt < 1 {
            bail!("memory_snapshot.background_download.try_cnt must be >= 1");
        }
        Ok(())
    }

    pub(crate) fn validate_overlaybd_global_config_paths(&self) -> Result<()> {
        let paths = [
            (
                "ublk.overlaybd.global_config_path",
                &self.ublk.overlaybd.global_config_path,
            ),
            (
                "memory_snapshot.overlaybd_global_config_path",
                &self.memory_snapshot.overlaybd_global_config_path,
            ),
            (
                "derived convert overlaybd global config path",
                &self.resolved_overlaybd_convert_global_config_path(),
            ),
            (
                "derived resize overlaybd global config path",
                &self.resolved_overlaybd_resize_global_config_path(),
            ),
        ];
        let normalized =
            paths.map(|(name, path)| (name, overlaybd::config::lexically_normalize_path(path)));
        let canonical = normalized
            .clone()
            .map(|(name, path)| (name, std::fs::canonicalize(&path).ok()));

        for left in 0..normalized.len() {
            for right in (left + 1)..normalized.len() {
                let lexical_alias = normalized[left].1 == normalized[right].1;
                let canonical_alias =
                    canonical[left].1.is_some() && canonical[left].1 == canonical[right].1;
                if lexical_alias || canonical_alias {
                    bail!(
                        "{} ({:?}) and {} ({:?}) must be different",
                        normalized[left].0,
                        normalized[left].1,
                        normalized[right].0,
                        normalized[right].1,
                    );
                }
            }
        }
        Ok(())
    }

    fn validate_pool_config(&self) -> Result<()> {
        let network = self.network_pool_config();
        if network.maintenance_enabled {
            PoolTomlConfig::validate("network", &network)?;
        }
        if let Some(block) = self.block_pool_config() {
            PoolTomlConfig::validate("block", &block)?;
        }
        if let Some(firecracker) = self.firecracker_pool_config() {
            PoolTomlConfig::validate("firecracker", &firecracker.pool)?;
            if firecracker.fill_concurrency == 0 {
                bail!("invalid firecracker pool config: fill_concurrency must be > 0");
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ConfigManager {
    config: AppConfig,
    config_path: Option<PathBuf>,
}

static GLOBAL_CONFIG_MANAGER: OnceLock<ConfigManager> = OnceLock::new();

impl ConfigManager {
    pub fn global() -> &'static Self {
        if let Some(manager) = GLOBAL_CONFIG_MANAGER.get() {
            return manager;
        }

        #[cfg(test)]
        {
            Self::init_global().expect("test ConfigManager initialization failed")
        }

        #[cfg(not(test))]
        {
            GLOBAL_CONFIG_MANAGER
                .get()
                .expect("ConfigManager must be initialized before use")
        }
    }

    pub fn init_global() -> Result<&'static Self> {
        if let Some(manager) = GLOBAL_CONFIG_MANAGER.get() {
            return Ok(manager);
        }
        Self::set_global(Self::new()?)
    }

    pub fn init_global_from_path(path: &Path) -> Result<&'static Self> {
        if let Some(manager) = GLOBAL_CONFIG_MANAGER.get() {
            return Ok(manager);
        }
        Self::set_global(Self::new_from_path(path)?)
    }

    fn set_global(manager: Self) -> Result<&'static Self> {
        #[cfg(test)]
        let manager = {
            let mut manager = manager;
            manager
                .config
                .sandbox
                .access_token_hash_seed
                .get_or_insert_with(|| TEST_ACCESS_TOKEN_HASH_SEED.to_string());
            manager
        };
        let _ = GLOBAL_CONFIG_MANAGER.set(manager);
        GLOBAL_CONFIG_MANAGER
            .get()
            .ok_or_else(|| anyhow!("failed to initialize global sandbox config manager"))
    }

    pub fn new() -> Result<Self> {
        let config_path = Self::env_path(ENV_CONFIG_PATH).unwrap_or_else(Self::default_config_path);
        let config = Self::load_config_file(&config_path)?;
        Ok(Self {
            config,
            config_path: Some(config_path),
        })
    }

    pub fn new_from_path(path: &Path) -> Result<Self> {
        let config = Self::load_config_file(path)?;
        Ok(Self {
            config,
            config_path: Some(path.to_path_buf()),
        })
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub fn global_config() -> &'static AppConfig {
        Self::global().config()
    }

    pub fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref()
    }

    fn default_config_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("config")
            .join("default.toml")
    }

    fn load_config_file(path: &Path) -> Result<AppConfig> {
        let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let mut config = AppConfig::builder()
            .env()
            .file(path)
            .load()
            .with_context(|| format!("load config {}", path.display()))?;
        config.normalize(config_dir)?;
        config.validate()?;

        Ok(config)
    }

    fn env_path(name: &str) -> Option<PathBuf> {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    }
}

fn resolve_relative_to(base_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

const HOME_PATH_PLACEHOLDER: &str = "$AENV_HOME";
const RUNTIME_PATH_PLACEHOLDER: &str = "$AENV_RUNTIME";

fn resolve_path(home_path: &Path, config_dir: &Path, raw: &Path) -> PathBuf {
    let expanded = match raw.to_str() {
        Some(s) if s.contains(HOME_PATH_PLACEHOLDER) => {
            PathBuf::from(s.replace(HOME_PATH_PLACEHOLDER, &home_path.to_string_lossy()))
        }
        _ => raw.to_path_buf(),
    };
    resolve_relative_to(config_dir, &expanded)
}

fn resolve_runtime_path(
    home_path: &Path,
    runtime_path: &Path,
    config_dir: &Path,
    raw: &Path,
) -> PathBuf {
    let expanded = match raw.to_str() {
        Some(s) => PathBuf::from(
            s.replace(HOME_PATH_PLACEHOLDER, &home_path.to_string_lossy())
                .replace(RUNTIME_PATH_PLACEHOLDER, &runtime_path.to_string_lossy()),
        ),
        None => raw.to_path_buf(),
    };
    resolve_relative_to(config_dir, &expanded)
}

fn resolve_optional_config_path_or(
    path: &mut Option<PathBuf>,
    home_path: &Path,
    config_dir: &Path,
    default_relative_path: &str,
) {
    let resolved = match path.take() {
        Some(raw) => resolve_path(home_path, config_dir, &raw),
        None => home_path.join(default_relative_path),
    };
    *path = Some(resolved);
}

fn parse_required_path(raw: &str) -> std::result::Result<PathBuf, std::convert::Infallible> {
    Ok(PathBuf::from(raw.trim()))
}

fn parse_trimmed_string(raw: &str) -> std::result::Result<String, std::convert::Infallible> {
    Ok(raw.trim().to_string())
}

impl PoolTomlConfig {
    fn validate(name: &str, pool: &warm_pool::PoolConfig) -> Result<()> {
        if pool.low_watermark > pool.high_watermark {
            bail!(
                "invalid {name} pool config: low_watermark ({}) must be <= high_watermark ({})",
                pool.low_watermark,
                pool.high_watermark
            );
        }

        Ok(())
    }
}

impl ClusterConfig {
    fn normalize(&mut self) {
        self.scheduler_endpoint = self
            .scheduler_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .map(ToOwned::to_owned);
    }
}

impl SandboxProxyConfig {
    fn normalize(&mut self) -> Result<()> {
        let domains = &self.domains;
        let mut normalized = Vec::with_capacity(domains.len());
        for domain in domains {
            let domain = domain.trim().trim_end_matches('.');
            if domain.is_empty() {
                continue;
            }
            let Some(domain) = network::normalize_dns_name(domain) else {
                bail!("sandbox_proxy.domains contains invalid domain {domain:?}");
            };
            if !normalized.iter().any(|seen| seen == &domain) {
                normalized.push(domain);
            }
        }
        // Keep user order: domains[0] is the advertised sandbox response domain.
        self.domains = normalized;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn bundled_default_config_loads() -> Result<()> {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
        ConfigManager::new_from_path(&workspace.join("config/default.toml"))?;
        Ok(())
    }

    #[test]
    fn pvm_config_disables_dirty_page_tracking() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("pvm.toml");
        std::fs::write(&path, "virtualization_mode = \"pvm\"")?;

        let config = ConfigManager::new_from_path(&path)?;
        assert_eq!(config.config().virtualization_mode, VirtualizationMode::Pvm);
        assert!(!config.config().memory_snapshot.track_dirty_pages);
        Ok(())
    }

    #[test]
    fn sandbox_access_token_seed_is_redacted() {
        let config = SandboxConfig {
            access_token_hash_seed: Some("cluster-secret".to_string()),
        };
        assert!(!format!("{config:?}").contains("cluster-secret"));
    }

    #[test]
    fn validate_memory_snapshot_options_enforces_dirty_page_requirements() {
        let cases = [
            (VirtualizationMode::Kvm, false, None),
            (VirtualizationMode::Kvm, true, None),
            (
                VirtualizationMode::Pvm,
                true,
                Some("is disabled in PVM mode"),
            ),
        ];

        for (virtualization_mode, track_dirty_pages, expected_error) in cases {
            let config = AppConfig {
                virtualization_mode,
                memory_snapshot: MemorySnapshotConfig {
                    track_dirty_pages,
                    ..Default::default()
                },
                ..Default::default()
            };

            let result = config.validate_memory_snapshot_options();
            match expected_error {
                Some(expected_error) => {
                    let error = result.unwrap_err();
                    assert!(
                        error.to_string().contains(expected_error),
                        "expected error containing {expected_error:?}, got: {error}"
                    );
                }
                None => result.expect("supported memory snapshot options should be valid"),
            }
        }
    }

    #[test]
    fn validate_rejects_zero_memory_snapshot_download_concurrency() {
        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.concurrency = 0;

        let err = config.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("memory_snapshot.background_download.concurrency must be > 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_invalid_disk_rate_limit() {
        let cases = [
            (
                DiskRateLimitConfig {
                    enabled: true,
                    bandwidth_burst_bytes: 1024,
                    ..Default::default()
                },
                "bandwidth_burst_bytes is set but",
            ),
            (
                DiskRateLimitConfig {
                    enabled: true,
                    iops_burst: 500,
                    ..Default::default()
                },
                "iops_burst is set but",
            ),
            (
                DiskRateLimitConfig {
                    enabled: true,
                    bandwidth_bytes_per_sec: i64::MAX as u64 + 1,
                    ..Default::default()
                },
                "machine.disk_rate_limit.bandwidth_bytes_per_sec",
            ),
        ];

        for (disk_rate_limit, expected_error) in cases {
            let mut config = AppConfig::default();
            config.machine.disk_rate_limit = disk_rate_limit;

            let err = config.validate().unwrap_err();
            assert!(
                err.to_string().contains(expected_error),
                "expected error containing {expected_error:?}, got: {err}"
            );
        }
    }

    #[test]
    fn validate_skips_disabled_disk_rate_limit() {
        // A disabled section is ignored at runtime, so even internally
        // inconsistent or out-of-range values must not block startup.
        let mut config = AppConfig::default();
        config.machine.disk_rate_limit.enabled = false;
        config.machine.disk_rate_limit.bandwidth_bytes_per_sec = 0;
        config.machine.disk_rate_limit.bandwidth_burst_bytes = 1024;
        config.machine.disk_rate_limit.iops = u64::MAX;
        config
            .validate()
            .expect("disabled disk rate limit config is not validated");
    }

    #[test]
    fn validate_accepts_consistent_disk_rate_limit() {
        let mut config = AppConfig::default();
        config.machine.disk_rate_limit.enabled = true;
        config.machine.disk_rate_limit.bandwidth_bytes_per_sec = 104_857_600;
        config.machine.disk_rate_limit.bandwidth_burst_bytes = 10_485_760;
        config.machine.disk_rate_limit.iops = 3000;
        config.machine.disk_rate_limit.iops_burst = 500;
        config
            .validate()
            .expect("consistent disk rate limit config passes");
    }

    #[test]
    fn validate_rejects_invalid_volume_limits() {
        let mut config = AppConfig::default();
        config.volume.max_size_mb = 0;
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("volume.max_size_mb"));

        let mut config = AppConfig::default();
        config.volume.max_volume_count = 0;
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("volume.max_volume_count"));

        let mut config = AppConfig::default();
        config.volume.max_size_mb = u64::MAX;
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("volume.max_size_mb"));

        let mut config = AppConfig::default();
        config.volume.max_volume_count = crate::volume::MAX_VOLUME_MOUNTS + 1;
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("volume.max_volume_count"));
    }

    #[test]
    fn validate_rejects_colliding_overlaybd_global_config_paths() {
        let cases = [
            (
                "/tmp/shared-overlaybd-global.json",
                "/tmp/shared-overlaybd-global.json",
                "ublk.overlaybd.global_config_path",
                "memory_snapshot.overlaybd_global_config_path",
            ),
            (
                "/tmp/aenv/x/../global.json",
                "/tmp/aenv/global.json",
                "ublk.overlaybd.global_config_path",
                "memory_snapshot.overlaybd_global_config_path",
            ),
            (
                "/tmp/overlaybd/resize-overlaybd-global.json",
                "/var/lib/aenv/overlaybd/mem-overlaybd-global.json",
                "ublk.overlaybd.global_config_path",
                "derived resize",
            ),
            (
                "/tmp/overlaybd/overlaybd-global.json",
                "/tmp/overlaybd/convert-overlaybd-global.json",
                "memory_snapshot.overlaybd_global_config_path",
                "derived convert",
            ),
        ];

        for (runtime_path, memory_path, left_name, right_name) in cases {
            let mut config = AppConfig::default();
            config.ublk.overlaybd.global_config_path = runtime_path.into();
            config.memory_snapshot.overlaybd_global_config_path = memory_path.into();

            let err = config.validate().unwrap_err();
            let message = err.to_string();
            assert!(message.contains(left_name), "unexpected error: {err}");
            assert!(message.contains(right_name), "unexpected error: {err}");
            assert!(
                message.contains("must be different"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn validate_bounds_memory_snapshot_background_download() {
        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.concurrency = 17;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.block_size = 65 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 1;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.block_size = 64 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 8;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.block_size = 32 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 8;
        config.validate().expect("defaults-shaped config passes");

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.delay = -1;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.memory_snapshot.background_download.try_cnt = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn overlaybd_converter_cache_id_includes_configured_tool_version() {
        let config = AppConfig {
            overlaybd: Some(OverlaybdDependencyConfig {
                version: "test-version".to_string(),
                url: None,
                package_url: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            config.resolved_overlaybd_oci_converter_id(),
            "overlaybd-oci:test-version:agentenv-cache-v1"
        );
    }

    #[test]
    fn resolve_path_joins_relative_paths_against_config_dir() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path();
        let home_path = temp.path().join("home");

        assert_eq!(
            resolve_path(&home_path, config_dir, Path::new("bin/firecracker")),
            config_dir.join("bin/firecracker")
        );
        assert_eq!(
            resolve_path(&home_path, config_dir, Path::new("$AENV_HOME/image-cache")),
            home_path.join("image-cache")
        );

        Ok(())
    }

    #[test]
    fn normalize_paths_relative_to_config_dir() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path().join("configs");
        let mut config = AppConfig {
            deps_path: "./env".into(),
            ..Default::default()
        };
        config.firecracker.binary_path = Some("./bin/firecracker".into());
        config.firecracker.work_dir = Some("./firecracker-work".into());
        config.firecracker.serial_dir = Some("./firecracker-serial".into());
        config.kernel.image_path = Some("./kernel/vmlinux.bin".into());
        config.tools.drive_path = Some("./tools.ext4".into());
        config.image.cache.root_dir = "./custom-image-cache".into();
        config.snapshot.local_cache_path = "./snapshot-local-cache".into();
        config.backend.posix_fs = Some(PosixFsBackendConfig {
            snapshot_store: "./snapshot-store".into(),
        });
        config.p2p.store_dir = "./p2p-store".into();
        config.ublk.daemon_binary_path = Some("./bin/uvm-ublk-daemon".into());
        config.ublk.daemon_socket_path = "./run/uvm-ublk-daemon.sock".into();
        config.ublk.daemon_log_path = Some("./logs/uvm-ublk-daemon.log".into());
        config.ublk.overlaybd.global_config_path = "./overlaybd-global.json".into();
        config.memory_snapshot.overlaybd_global_config_path = "./mem-overlaybd-global.json".into();
        config.orchestrator.persisted_sandbox_store_path = "./persisted-sandboxes".into();
        config.normalize(&config_dir)?;

        assert_eq!(config.deps_path, config_dir.join("env"));
        assert_eq!(
            config.firecracker.binary_path,
            Some(config_dir.join("bin/firecracker"))
        );
        assert_eq!(
            config.firecracker.work_dir,
            Some(config_dir.join("firecracker-work"))
        );
        assert_eq!(
            config.firecracker.serial_dir,
            Some(config_dir.join("firecracker-serial"))
        );
        assert_eq!(
            config.kernel.image_path,
            Some(config_dir.join("kernel/vmlinux.bin"))
        );
        assert_eq!(config.tools.drive_path, Some(config_dir.join("tools.ext4")));
        assert_eq!(
            config.image.cache.root_dir,
            config_dir.join("custom-image-cache")
        );
        assert_eq!(
            config.snapshot.local_cache_path,
            config_dir.join("snapshot-local-cache")
        );
        assert_eq!(config.p2p.store_dir, config_dir.join("p2p-store"));
        assert_eq!(
            config.backend.posix_fs.as_ref().unwrap().snapshot_store,
            config_dir.join("snapshot-store")
        );
        assert_eq!(
            config.ublk.daemon_binary_path,
            Some(config_dir.join("bin/uvm-ublk-daemon"))
        );
        assert_eq!(
            config.ublk.daemon_socket_path,
            config_dir.join("run/uvm-ublk-daemon.sock")
        );
        assert_eq!(
            config.ublk.daemon_log_path,
            Some(config_dir.join("logs/uvm-ublk-daemon.log"))
        );
        assert_eq!(
            config.ublk.overlaybd.global_config_path,
            config_dir.join("overlaybd-global.json")
        );
        assert_eq!(
            config.memory_snapshot.overlaybd_global_config_path,
            config_dir.join("mem-overlaybd-global.json")
        );
        assert_eq!(
            config.orchestrator.persisted_sandbox_store_path,
            config_dir.join("persisted-sandboxes")
        );

        Ok(())
    }

    #[test]
    fn managed_dependency_paths_remain_implicit_and_resolve_from_deps_path() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path().join("configs");
        let mut config = AppConfig {
            deps_path: "./deps".into(),
            ..Default::default()
        };
        config.firecracker.version = Some("fc-test".to_string());
        config.kernel.version = Some("kernel-test".to_string());
        config.tools.version = Some("1.2.3-custom.1".to_string());

        config.normalize(&config_dir)?;

        let deps_path = config_dir.join("deps");
        assert_eq!(config.deps_path, deps_path);
        assert_eq!(config.firecracker.binary_path, None);
        assert_eq!(config.kernel.image_path, None);
        assert_eq!(config.tools.drive_path, None);
        assert_eq!(
            config.resolved_firecracker_binary_path(),
            deps_path.join("firecracker/fc-test/firecracker")
        );
        assert_eq!(
            config.resolved_kernel_image_path(),
            deps_path.join("kernel/kernel-test/vmlinux.bin")
        );
        assert_eq!(
            config.resolved_tools_drive_path()?,
            deps_path.join("tools/1.2.3-custom.1/tools.ext4")
        );
        assert!(config
            .resolved_tools_drive_path_for_version("../escape")
            .is_err());
        assert!(config
            .resolved_tools_drive_path_for_version("1.2.3+rebuilt")
            .is_err());

        Ok(())
    }

    #[test]
    fn managed_dependency_paths_select_the_active_mode_versions() {
        let manifest = SetupDependencyManifest::get();
        let mut config = AppConfig {
            deps_path: PathBuf::from("/deps"),
            virtualization_mode: VirtualizationMode::Kvm,
            ..Default::default()
        };
        assert_eq!(
            config.resolved_firecracker_binary_path(),
            PathBuf::from("/deps/firecracker")
                .join(&manifest.firecracker.kvm.version)
                .join("firecracker")
        );
        assert_eq!(
            config.resolved_kernel_image_path(),
            PathBuf::from("/deps/kernel")
                .join(&manifest.kernel.kvm.version)
                .join("vmlinux.bin")
        );

        config.virtualization_mode = VirtualizationMode::Pvm;
        assert_eq!(
            config.resolved_firecracker_binary_path(),
            PathBuf::from("/deps/firecracker")
                .join(&manifest.firecracker.pvm.version)
                .join("firecracker")
        );
        assert_eq!(
            config.resolved_kernel_image_path(),
            PathBuf::from("/deps/kernel")
                .join(&manifest.kernel.pvm.version)
                .join("vmlinux.bin")
        );
    }

    #[test]
    fn home_relative_toml_values_derive_from_home_path() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path().join("configs");
        let mut config = AppConfig {
            home_path: "./env".into(),
            runtime_path: "$AENV_HOME/run".into(),
            deps_path: "$AENV_HOME/deps".into(),
            ..Default::default()
        };
        config.ublk.daemon_socket_path = "$AENV_RUNTIME/ublk-daemon.sock".into();
        config.firecracker.serial_dir = Some("$AENV_HOME/logs/serial".into());
        config.image.cache.root_dir = "$AENV_HOME/image-cache".into();
        config.snapshot.local_cache_path = "$AENV_HOME/snapshot-local-cache".into();
        config.p2p.store_dir = "$AENV_HOME/p2p/store".into();
        config.ublk.overlaybd.global_config_path =
            "$AENV_HOME/overlaybd/overlaybd-global.json".into();
        config.memory_snapshot.overlaybd_global_config_path =
            "$AENV_HOME/overlaybd/mem-overlaybd-global.json".into();
        config.normalize(&config_dir)?;

        let home_path = config_dir.join("env");
        assert_eq!(config.runtime_path, home_path.join("run"));
        assert_eq!(
            config.ublk.daemon_socket_path,
            home_path.join("run/ublk-daemon.sock")
        );
        assert_eq!(
            config.firecracker.serial_dir,
            Some(home_path.join("logs/serial"))
        );
        assert_eq!(config.deps_path, home_path.join("deps"));
        assert_eq!(config.image.cache.root_dir, home_path.join("image-cache"));
        assert_eq!(
            config.snapshot.local_cache_path,
            home_path.join("snapshot-local-cache")
        );
        assert_eq!(config.p2p.store_dir, home_path.join("p2p").join("store"));
        assert_eq!(
            config.ublk.overlaybd.global_config_path,
            home_path.join("overlaybd").join("overlaybd-global.json")
        );
        assert_eq!(
            config.memory_snapshot.overlaybd_global_config_path,
            home_path
                .join("overlaybd")
                .join("mem-overlaybd-global.json")
        );

        Ok(())
    }

    #[test]
    fn compile_time_defaults_derive_from_home_path() -> Result<()> {
        let temp = tempdir()?;
        let config_dir = temp.path().join("configs");
        let mut config = AppConfig {
            home_path: "./env".into(),
            ..Default::default()
        };
        config.normalize(&config_dir)?;

        let home_path = config_dir.join("env");
        assert_eq!(config.runtime_path, PathBuf::from("/run/aenv"));
        assert_eq!(config.deps_path, home_path.join("deps"));
        assert_eq!(
            config.firecracker.work_dir,
            Some(home_path.join("firecracker-work"))
        );
        assert_eq!(
            config.firecracker.serial_dir,
            Some(home_path.join("logs/serial"))
        );
        assert_eq!(config.image.cache.root_dir, home_path.join("image-cache"));
        assert_eq!(
            config.snapshot.local_cache_path,
            home_path.join("snapshot-local-cache")
        );
        assert_eq!(config.p2p.store_dir, home_path.join("p2p/store"));
        assert_eq!(
            config.ublk.overlaybd.global_config_path,
            home_path.join("overlaybd/overlaybd-global.json")
        );
        assert_eq!(
            config.memory_snapshot.overlaybd_global_config_path,
            home_path.join("overlaybd/mem-overlaybd-global.json")
        );
        assert_eq!(
            config.orchestrator.persisted_sandbox_store_path,
            home_path.join("persisted-sandboxes")
        );
        assert_eq!(
            config.backend.posix_fs.as_ref().unwrap().snapshot_store,
            home_path.join("snapshot-store")
        );
        assert_eq!(
            config.ublk.daemon_binary_path,
            Some(home_path.join("ublk/uvm-ublk-daemon"))
        );
        assert_eq!(
            config.ublk.daemon_socket_path,
            PathBuf::from("/run/aenv/ublk-daemon.sock")
        );
        assert_eq!(
            config.ublk.daemon_log_path,
            Some(home_path.join("logs/ublk-daemon.log"))
        );

        Ok(())
    }

    #[test]
    fn sandbox_proxy_domains_are_normalized() -> Result<()> {
        let mut config = SandboxProxyConfig {
            domains: vec![
                " Sandbox.Example.Invalid. ".to_string(),
                "sandbox.example.invalid".to_string(),
                "".to_string(),
                "old.example.invalid".to_string(),
            ],
        };
        config.normalize()?;

        assert_eq!(
            config.domains,
            vec!["sandbox.example.invalid", "old.example.invalid"]
        );

        Ok(())
    }

    #[test]
    fn sandbox_proxy_domains_reject_invalid_domain() {
        let mut config = SandboxProxyConfig {
            domains: vec!["not a domain".to_string()],
        };
        let err = config.normalize().unwrap_err();
        assert!(
            err.to_string()
                .contains("sandbox_proxy.domains contains invalid domain"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cluster_normalize_trims_and_drops_blank_scheduler_endpoint() {
        let mut config = ClusterConfig {
            scheduler_endpoint: Some("  ".to_string()),
        };
        config.normalize();
        assert_eq!(config.scheduler_endpoint, None);

        let mut config = ClusterConfig {
            scheduler_endpoint: Some("  http://scheduler:9090  ".to_string()),
        };
        config.normalize();
        assert_eq!(
            config.scheduler_endpoint.as_deref(),
            Some("http://scheduler:9090")
        );
    }

    #[test]
    fn resolve_config_path_keeps_absolute_paths_unchanged() {
        let absolute = PathBuf::from("/tmp/agentenv-absolute");
        assert_eq!(
            resolve_path(Path::new("/ignored-home"), Path::new("/ignored"), &absolute),
            absolute
        );
    }

    #[test]
    fn validate_pool_config_rejects_invalid_values() {
        let config = AppConfig {
            pool: PoolTomlConfig {
                low_watermark: 64,
                high_watermark: 32,
                ..PoolTomlConfig::default()
            },
            ..AppConfig::default()
        };
        let err = config.validate_pool_config().unwrap_err();
        assert!(
            err.to_string().contains("low_watermark"),
            "unexpected error: {err}"
        );

        let config = AppConfig {
            pool: PoolTomlConfig {
                low_watermark: 64,
                high_watermark: 32,
                network: PoolComponentConfig {
                    maintenance_enabled: false,
                    ..PoolComponentConfig::default()
                },
                block: PoolComponentConfig {
                    enabled: false,
                    ..PoolComponentConfig::default()
                },
                firecracker: FirecrackerProcessPoolConfig {
                    enabled: false,
                    ..FirecrackerProcessPoolConfig::default()
                },
            },
            ..AppConfig::default()
        };
        assert!(config.validate_pool_config().is_ok());
    }

    #[test]
    fn firecracker_pool_fill_concurrency_rejects_zero() {
        let config = AppConfig {
            pool: PoolTomlConfig {
                firecracker: FirecrackerProcessPoolConfig {
                    enabled: true,
                    fill_concurrency: 0,
                    ..FirecrackerProcessPoolConfig::default()
                },
                ..PoolTomlConfig::default()
            },
            ..AppConfig::default()
        };

        let err = config.validate_pool_config().unwrap_err();
        assert!(
            err.to_string().contains("fill_concurrency"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_zero_overlaybd_resize_timeout() {
        let mut config = AppConfig::default();
        config.ublk.overlaybd.resize_timeout_secs = 0;
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("resize_timeout_secs must be > 0"),
            "unexpected error: {err}"
        );
    }
}
