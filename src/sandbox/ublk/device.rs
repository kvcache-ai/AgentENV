use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use overlaybd::config::UpperMode;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, OnceCell};
use tracing::{debug, info, warn};
use uvm_ublk_daemon::protocol::PackRecordingState;
use uvm_ublk_daemon::{
    CreateOverlaybdRuntimeDeviceRequest, RestackSnapshotStats, RestackSnapshotTerminalFailure,
    UblkDaemonClient, UblkDaemonSpawnConfig,
};

use super::overlaybd::OverlaybdConfig;
use crate::observability::prometheus::MetricGuard;
use crate::sandbox::SandboxCaptureError;

const UBLK_OPERATION_DURATION: &str = "agentenv_ublk_operation_duration_seconds";
const RUNTIME_DEVICE_TIMEOUT: Duration = Duration::from_secs(360);
const RESIZE_RPC_TIMEOUT_MARGIN: Duration = Duration::from_secs(120);

/// Window and guard parameters for one startup pack recording, derived from
/// `[snapshot.memory_startup_pack]` by the recording orchestration.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PackRecordingWindow {
    pub max_pages: u32,
    pub min_window_ms: u64,
    pub quiet_ms: u64,
    pub max_window_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UblkConfig {
    pub backend: UblkBackend,
}

#[derive(Clone, Debug, Serialize)]
pub enum UblkBackend {
    Overlaybd(OverlaybdConfig),
}

impl<'de> Deserialize<'de> for UblkBackend {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum PersistedUblkBackend {
            #[serde(rename = "Cow")]
            LegacyCow,
            Overlaybd(OverlaybdConfig),
        }

        match PersistedUblkBackend::deserialize(deserializer)? {
            PersistedUblkBackend::Overlaybd(config) => Ok(Self::Overlaybd(config)),
            PersistedUblkBackend::LegacyCow => Err(serde::de::Error::custom(
                "the legacy CoW ublk backend is no longer supported; this persisted sandbox cannot be resumed and must be rebuilt with OverlayBD",
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum UblkCreateSpec {
    Overlaybd {
        image_config: PathBuf,
        global_config: PathBuf,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct OverlaybdRuntimeDevice {
    pub(crate) device: UblkDevice,
    pub(crate) image_config_path: PathBuf,
    pub(crate) actual_virtual_size: u64,
}

impl UblkConfig {
    pub fn overlaybd(image_config_path: PathBuf, read_only: bool) -> Self {
        Self::overlaybd_with_runtime_upper_mode(
            image_config_path,
            read_only,
            UpperMode::LogStructured,
        )
    }

    pub fn overlaybd_with_runtime_upper_mode(
        image_config_path: PathBuf,
        read_only: bool,
        runtime_upper_mode: UpperMode,
    ) -> Self {
        Self {
            backend: UblkBackend::Overlaybd(OverlaybdConfig {
                image_config_path,
                read_only,
                runtime_upper_mode,
            }),
        }
    }
}

/// Configuration for spawning the `uvm-ublk-daemon` process.
pub struct UblkDaemonConfig {
    pub daemon_binary: PathBuf,
    pub socket_path: PathBuf,
    pub global_config: PathBuf,
    /// OverlayBD global config used by the daemon only for overlaybd-resize.
    pub resize_global_config: PathBuf,
    pub app_config: Option<PathBuf>,
    /// Log file path for the daemon. If `None`, the daemon logs to stderr.
    pub log_file: Option<PathBuf>,
    /// HTTP listen address for daemon metrics. Empty string disables it.
    pub metrics_listen_addr: String,
    /// Pool configuration. If `Some`, the daemon will enable warm pooling.
    pub pool_config: Option<uvm_ublk_daemon::PoolConfig>,
    /// Local HTTP endpoint used by the daemon to notify P2P layer publication.
    pub p2p_publish_url: Option<String>,
    /// Client-side timeout for runtime-device RPCs.
    pub runtime_device_timeout: Duration,
}

impl UblkDaemonConfig {
    /// Resolve daemon configuration from the application config.
    ///
    /// Returns `Err` if the daemon binary cannot be found (fail-fast).
    pub fn from_app_config(config: &crate::cfg::AppConfig) -> Result<Self> {
        let ublk = &config.ublk;
        let daemon_binary = ublk
            .daemon_binary_path
            .clone()
            .or_else(|| which::which("uvm-ublk-daemon").ok())
            .ok_or_else(|| {
                anyhow!(
                    "ublk daemon binary not found; \
                     set ublk.daemon_binary_path in config or ensure uvm-ublk-daemon is in PATH"
                )
            })?;

        let pool_config = config.block_pool_config();
        let runtime_device_timeout =
            runtime_device_timeout(config.ublk.overlaybd.resize_timeout_secs);

        Ok(Self {
            daemon_binary,
            socket_path: ublk.daemon_socket_path.clone(),
            global_config: ublk.overlaybd.global_config_path.clone(),
            resize_global_config: config.resolved_overlaybd_resize_global_config_path(),
            app_config: crate::cfg::ConfigManager::global()
                .config_path()
                .map(PathBuf::from),
            log_file: ublk.daemon_log_path.clone(),
            metrics_listen_addr: ublk.daemon_metrics_listen_addr.clone(),
            pool_config,
            p2p_publish_url: None,
            runtime_device_timeout,
        })
    }
}

fn runtime_device_timeout(resize_timeout_secs: u64) -> Duration {
    let resize_timeout = Duration::from_secs(resize_timeout_secs)
        .checked_add(RESIZE_RPC_TIMEOUT_MARGIN)
        .unwrap_or(Duration::MAX);
    RUNTIME_DEVICE_TIMEOUT.max(resize_timeout)
}

/// Global ublk device manager.
///
/// Wraps the daemon client for device lifecycle management. Device IDs are
/// assigned by the kernel and returned by the daemon.
///
/// Also maintains a pool of shared read-only ublk devices keyed by canonical
/// `image_config` path. Multiple sandboxes using the same memory snapshot
/// image share a single read-only ublk device (and thus the same page cache).
///
/// Initialized once via [`init_global`] (which spawns the daemon if
/// configured). Accessed everywhere else via [`global`].
pub struct UblkDeviceManager {
    client: Option<Arc<UblkDaemonClient>>,
    pool_enabled: bool,
    /// Pool of shared read-only ublk devices, keyed by canonical image config path.
    /// Values are `Weak` references: when the last `SharedReadOnlyDevice` handle is
    /// dropped, the device is deleted asynchronously and the entry becomes stale.
    shared_readonly_devices: DashMap<PathBuf, Weak<SharedReadOnlyDeviceInner>>,
    /// Per-image notifications for asynchronous shared-memory releases. A new
    /// acquire for the same image waits until the previous last-handle release
    /// has completed so the daemon does not reuse stale opened image state.
    shared_readonly_releases: DashMap<PathBuf, Arc<Notify>>,
    recent_tools_device: std::sync::Mutex<Option<SharedReadOnlyDevice>>,
}

static GLOBAL_MANAGER: OnceCell<UblkDeviceManager> = OnceCell::const_new();

impl UblkDeviceManager {
    fn new(client: Option<Arc<UblkDaemonClient>>, pool_enabled: bool) -> Self {
        Self {
            client,
            pool_enabled,
            shared_readonly_devices: DashMap::new(),
            shared_readonly_releases: DashMap::new(),
            recent_tools_device: std::sync::Mutex::new(None),
        }
    }

    /// Initialize the global device manager.
    ///
    /// If `config` is `Some`, spawns the `uvm-ublk-daemon` process and
    /// creates a client. If `config` is `None`, the manager has no daemon
    /// client (device operations will fail with a clear error).
    ///
    /// Safe to call multiple times — only the first call takes effect.
    pub async fn init_global(config: Option<UblkDaemonConfig>) {
        GLOBAL_MANAGER
            .get_or_init(|| async {
                let (client, pool_enabled) = match config {
                    Some(cfg) => {
                        let pool_enabled = cfg.pool_config.is_some();
                        let client = match UblkDaemonClient::new(UblkDaemonSpawnConfig {
                            binary_path: &cfg.daemon_binary,
                            socket_path: cfg.socket_path,
                            global_config: &cfg.global_config,
                            resize_global_config: &cfg.resize_global_config,
                            app_config: cfg.app_config.as_deref(),
                            log_file: cfg.log_file.as_deref(),
                            metrics_listen_addr: &cfg.metrics_listen_addr,
                            pool_config: cfg.pool_config.as_ref(),
                            p2p_publish_url: cfg.p2p_publish_url.as_deref(),
                            runtime_device_timeout: cfg.runtime_device_timeout,
                        })
                        .await
                        {
                            Ok(c) => Some(c),
                            Err(err) => {
                                tracing::error!(?err, "failed to spawn ublk daemon");
                                None
                            }
                        };
                        (client, pool_enabled)
                    }
                    None => (None, false),
                };
                UblkDeviceManager::new(client, pool_enabled)
            })
            .await;
    }

    /// Convenience: resolve daemon config from [`AppConfig`] and initialize.
    ///
    /// Returns an error if the daemon binary cannot be found.
    pub async fn init_global_from_config(config: &crate::cfg::AppConfig) -> Result<()> {
        Self::init_global(Some(UblkDaemonConfig::from_app_config(config)?)).await;
        Ok(())
    }

    /// Convenience: resolve daemon config and pass a P2P publish endpoint to the daemon.
    pub async fn init_global_from_config_with_p2p_publish_url(
        config: &crate::cfg::AppConfig,
        p2p_publish_url: Option<&str>,
    ) -> Result<()> {
        let mut daemon_config = UblkDaemonConfig::from_app_config(config)?;
        daemon_config.p2p_publish_url = p2p_publish_url.map(|s| s.to_string());
        Self::init_global(Some(daemon_config)).await;
        Ok(())
    }

    /// Get the global manager instance.
    ///
    /// Panics if [`init_global`] has not been called.
    pub fn global() -> &'static Self {
        GLOBAL_MANAGER
            .get()
            .expect("UblkDeviceManager::init_global() must be called before global()")
    }

    /// Returns `true` if the ublk daemon client was successfully spawned.
    pub fn is_available(&self) -> bool {
        self.client.is_some()
    }

    fn require_client(&self) -> Result<&Arc<UblkDaemonClient>> {
        self.client
            .as_ref()
            .context("ublk daemon client is unavailable")
    }

    /// Tell the daemon the sandbox owning `device_key` finished booting,
    /// releasing held background downloads. Best-effort: failures only mean
    /// the downloads start after the fallback timeout instead.
    pub(crate) async fn notify_sandbox_ready(&self, device_key: &Path) {
        let Ok(client) = self.require_client() else {
            return;
        };
        // The daemon waits on the canonicalized path (create_image_file
        // canonicalizes the image config path), so the notification must use
        // the same form — otherwise the held download silently waits out the
        // 20s fallback on symlinked or relative spellings.
        let key = std::fs::canonicalize(device_key).unwrap_or_else(|_| device_key.to_path_buf());
        if let Err(error) = client.notify_sandbox_ready(&key.to_string_lossy()).await {
            tracing::warn!(
                %error,
                device_key = %key.display(),
                "failed to notify daemon of sandbox readiness; downloads start after fallback timeout"
            );
        }
    }

    // ── Device lifecycle ────────────────────────────────────────────────

    /// Release a ublk device back to the warm pool (if enabled) or delete it.
    pub(crate) async fn release_device(&self, device: &UblkDevice) -> Result<()> {
        let client = self.require_client()?;
        let dev_id = device.dev_id;
        debug!(dev_id, "releasing ublk device");

        if !self.pool_enabled {
            return self.delete_device(device).await;
        }

        let mut metric = MetricGuard::operation(UBLK_OPERATION_DURATION, "release");
        let result = client
            .release_overlaybd(dev_id)
            .await
            .context("release overlaybd device via daemon");
        metric.finish(&result);
        result?;

        debug!(dev_id, "ublk device released");
        Ok(())
    }

    /// Create a raw overlaybd device via the daemon.
    ///
    /// This fallback is only used for shared read-only devices when the warm pool
    /// is disabled. Rootfs and extra drives must use
    /// [`create_overlaybd_runtime_device`] so the daemon owns runtime
    /// materialization and rollback.
    async fn create_raw_overlaybd_device(&self, spec: &UblkCreateSpec) -> Result<UblkDevice> {
        let client = self.require_client()?;

        debug!(?spec, "creating ublk device via daemon");

        let mut metric = MetricGuard::operation(UBLK_OPERATION_DURATION, "create_raw_overlaybd");
        let created = match spec {
            UblkCreateSpec::Overlaybd {
                image_config,
                global_config,
            } => client
                .create_overlaybd(image_config, global_config)
                .await
                .context("create overlaybd device via daemon"),
        };
        metric.finish(&created);

        let (dev_id, device_path) = created?;

        debug!(dev_id, path = %device_path.display(), "ublk device ready");

        Ok(UblkDevice {
            dev_id,
            device_path,
        })
    }

    /// Create a dedicated (non-shared, non-pooled) overlaybd memory device for
    /// a startup-pack recording VM. The device is created directly via the
    /// daemon and is NOT registered in the shared-memory maps, so no other
    /// sandbox can share it (sharing the block-device page cache would hide
    /// first-touch reads from the recorder). It must always be released with
    /// [`Self::delete_device`], never `release_device` — `release_device`
    /// would return a non-pool device to the warm pool when pooling is
    /// globally enabled.
    pub(crate) async fn create_dedicated_mem_device(
        &self,
        spec: &UblkCreateSpec,
    ) -> Result<UblkDevice> {
        self.create_raw_overlaybd_device(spec).await
    }

    /// Arm a startup-pack first-touch recorder on a memory device.
    pub(crate) async fn start_pack_recording(
        &self,
        dev_id: u32,
        output: &Path,
        window: PackRecordingWindow,
    ) -> Result<()> {
        let client = self.require_client()?;
        client
            .start_pack_recording(
                dev_id,
                output,
                window.max_pages,
                window.min_window_ms,
                window.quiet_ms,
                window.max_window_ms,
            )
            .await
    }

    /// Poll the state of the pack recording running on `dev_id`.
    pub(crate) async fn pack_recording_status(&self, dev_id: u32) -> Result<PackRecordingState> {
        let client = self.require_client()?;
        client.pack_recording_status(dev_id).await
    }

    /// Abort the pack recording on `dev_id` (idempotent).
    pub(crate) async fn abort_pack_recording(&self, dev_id: u32) -> Result<()> {
        let client = self.require_client()?;
        client.abort_pack_recording(dev_id).await
    }

    /// Best-effort: ask the daemon to prefetch a v3 startup manifest for
    /// the memory image. Never fails the resume: registration problems are
    /// logged and swallowed here (the daemon logs execution failures).
    pub(crate) async fn prefetch_startup_pack(
        &self,
        image_config: &Path,
        global_config: &Path,
        pack: &crate::snapshot::ResolvedStartupPack,
    ) {
        let timeout_secs = crate::cfg::ConfigManager::global_config()
            .snapshot
            .memory_startup_pack
            .consume_timeout_secs;
        let crate::snapshot::ResolvedStartupPackSource::OssUrl(url) = &pack.source else {
            return;
        };
        let result = async {
            let client = self.require_client()?;
            client
                .prefetch_startup_pack(
                    image_config,
                    global_config,
                    url,
                    pack.pack_size,
                    &pack.index_sha256,
                    pack.mem_virtual_size,
                    timeout_secs,
                )
                .await
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(
                %error,
                image_config = %image_config.display(),
                "startup pack prefetch registration failed (best-effort)"
            );
        }
    }

    pub(crate) async fn create_overlaybd_runtime_device(
        &self,
        request: CreateOverlaybdRuntimeDeviceRequest<'_>,
    ) -> Result<OverlaybdRuntimeDevice> {
        let client = self.require_client()?;
        let mut metric =
            MetricGuard::operation(UBLK_OPERATION_DURATION, "create_runtime_overlaybd");
        let created = client
            .create_overlaybd_runtime_device(request)
            .await
            .context("create overlaybd runtime device via daemon");
        metric.finish(&created);
        let created = created?;

        Ok(OverlaybdRuntimeDevice {
            device: UblkDevice {
                dev_id: created.dev_id,
                device_path: created.device_path,
            },
            image_config_path: created.runtime_image_config_path,
            actual_virtual_size: created.actual_virtual_size,
        })
    }

    /// Delete a ublk device via the daemon.
    ///
    /// This is also the release path for dedicated (non-pooled) memory
    /// devices created with [`Self::create_dedicated_mem_device`]: those must
    /// never go through `release_device`, which returns devices to the warm
    /// pool when pooling is globally enabled.
    pub(crate) async fn delete_device(&self, device: &UblkDevice) -> Result<()> {
        let client = self.require_client()?;
        let dev_id = device.dev_id;
        debug!(dev_id, "deleting ublk device via daemon");

        let mut metric = MetricGuard::operation(UBLK_OPERATION_DURATION, "delete");
        let result = client.delete(dev_id).await;
        metric.finish(&result);
        if let Err(e) = result {
            warn!(dev_id, error = %e, "ublk daemon delete failed");
            return Err(e);
        }

        debug!(dev_id, "ublk device deleted");
        Ok(())
    }

    /// Request a restack-style snapshot of an overlaybd device's upper layer.
    pub(crate) async fn restack_snapshot_device(
        &self,
        device: &UblkDevice,
        output_layer_path: &Path,
        kind: &'static str,
    ) -> Result<Option<overlaybd::LayerDescriptor>> {
        let client = self.require_client()?;
        debug!(
            dev_id = device.dev_id,
            output = %output_layer_path.display(),
            "requesting overlaybd restack snapshot"
        );
        let mut metric = MetricGuard::operation(UBLK_OPERATION_DURATION, "restack_snapshot");
        let result = client
            .restack_snapshot(device.dev_id, output_layer_path)
            .await;
        metric.finish(&result);
        match result {
            Ok(stats) => {
                record_restack_usage_stats(device.dev_id, kind, &stats);
                debug!(
                    dev_id = device.dev_id,
                    output = %output_layer_path.display(),
                    digest = stats.descriptor.as_ref().map(|d| d.digest.as_str()),
                    size = stats.descriptor.as_ref().map(|d| d.size),
                    "overlaybd restack snapshot completed"
                );
                Ok(stats.descriptor)
            }
            Err(err) => {
                if err
                    .downcast_ref::<RestackSnapshotTerminalFailure>()
                    .is_some()
                {
                    return Err(SandboxCaptureError::terminal(anyhow!(format!(
                        "restack snapshot overlaybd device {} to {} failed; live runtime may have been mutated: {err:#}",
                        device.dev_id,
                        output_layer_path.display()
                    )))
                    .into());
                }
                Err(err).with_context(|| {
                    format!(
                        "restack snapshot overlaybd device {} to {}",
                        device.dev_id,
                        output_layer_path.display()
                    )
                })
            }
        }
    }

    /// Gracefully shut down the daemon process.
    pub async fn shutdown_daemon(&self) -> Result<()> {
        let recent = self.recent_tools_device.lock().unwrap().take();
        if let Some(device) = recent {
            if let Err(error) = device.release().await {
                warn!(error = %error, "failed to release cached tools device during shutdown");
            }
        }
        if let Some(client) = &self.client {
            client.shutdown().await
        } else {
            Ok(())
        }
    }

    // ── Shared memory device pool ───────────────────────────────────────

    /// Get or create a shared, reference-counted memory ublk device.
    ///
    /// Shared devices are read-only overlaybd devices that can be
    /// used by multiple sandboxes simultaneously. When multiple sandboxes
    /// boot from the same snapshot template, they share a single ublk
    /// device (and thus the same Linux page cache).
    ///
    /// The device is deleted automatically when the last [`SharedReadOnlyDevice`]
    /// handle is dropped.
    pub(crate) async fn get_or_create_shared_mem(
        &self,
        spec: &UblkCreateSpec,
        virtual_size: u64,
    ) -> Result<SharedReadOnlyDevice> {
        self.get_or_create_shared_readonly(spec, Some(virtual_size))
            .await
    }

    pub(crate) async fn get_or_create_shared_tools(
        &self,
        spec: &UblkCreateSpec,
    ) -> Result<SharedReadOnlyDevice> {
        // Tools capacity comes from the published block image. The retained
        // device is created once per release and reused across launches.
        let device = self.get_or_create_shared_readonly(spec, None).await?;
        // Retain one idle tools image; active sandboxes retain any other versions.
        self.recent_tools_device
            .lock()
            .unwrap()
            .replace(device.clone());
        Ok(device)
    }

    async fn get_or_create_shared_readonly(
        &self,
        spec: &UblkCreateSpec,
        virtual_size: Option<u64>,
    ) -> Result<SharedReadOnlyDevice> {
        let UblkCreateSpec::Overlaybd {
            image_config,
            global_config,
        } = spec;

        let key = std::fs::canonicalize(image_config).unwrap_or_else(|_| image_config.clone());

        loop {
            // If the previous last handle is still releasing the daemon-side
            // shared device, wait before acquiring the same key again.
            if let Some(notify) = self
                .shared_readonly_releases
                .get(&key)
                .map(|entry| Arc::clone(entry.value()))
            {
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self
                    .shared_readonly_releases
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current.value(), &notify))
                {
                    notified.await;
                }
                continue;
            }

            // Fast path: try to upgrade an existing Weak reference.
            if let Some(weak) = self.shared_readonly_devices.get(&key) {
                if let Some(strong) = weak
                    .upgrade()
                    .filter(|inner| !inner.released.load(Ordering::Acquire))
                {
                    debug!(
                        key = %key.display(),
                        dev_id = strong.device.dev_id,
                        "reusing shared read-only ublk device"
                    );
                    return Ok(SharedReadOnlyDevice { inner: strong });
                }
            }
            break;
        }

        let device = if self.pool_enabled {
            let client = self.require_client()?;
            let mut metric =
                MetricGuard::operation(UBLK_OPERATION_DURATION, "acquire_shared_memory");
            let acquired = client
                .acquire_overlaybd(
                    image_config,
                    global_config,
                    virtual_size,
                    uvm_ublk_daemon::AccessMode::Shared,
                )
                .await
                .context("acquire shared read-only ublk device");
            metric.finish(&acquired);
            let (dev_id, device_path) = acquired?;

            UblkDevice {
                dev_id,
                device_path,
            }
        } else {
            self.create_raw_overlaybd_device(spec)
                .await
                .context("create shared read-only ublk device")?
        };

        let cache_fd = match File::open(&device.device_path) {
            Ok(file) => file,
            Err(error) => {
                let _ = self.release_device(&device).await;
                return Err(error).context("keep shared read-only device open");
            }
        };
        info!(
            key = %key.display(),
            dev_id = device.dev_id,
            path = %device.device_path.display(),
            "created or acquired shared read-only ublk device"
        );

        let inner = Arc::new(SharedReadOnlyDeviceInner {
            device,
            image_config_key: key.clone(),
            released: AtomicBool::new(false),
            cache_fd: std::sync::Mutex::new(Some(cache_fd)),
        });

        // Use the entry API to avoid overwriting a live Weak inserted by a
        // concurrent caller that won the race.
        let mut entry = self
            .shared_readonly_devices
            .entry(key)
            .or_insert_with(|| Arc::downgrade(&inner));
        if let Some(winner) = entry
            .upgrade()
            .filter(|inner| !inner.released.load(Ordering::Acquire))
        {
            if !Arc::ptr_eq(&winner, &inner) {
                // Another caller inserted a live device while we were acquiring
                // ours. Reuse theirs; `inner` will be dropped, triggering
                // automatic cleanup of our redundant device.
                debug!(
                    dev_id = inner.device.dev_id,
                    winner_dev_id = winner.device.dev_id,
                    "concurrent caller won the race, reusing their shared memory device"
                );
                return Ok(SharedReadOnlyDevice { inner: winner });
            }
        } else {
            // The existing entry was stale (Weak::upgrade failed). Replace it
            // with our freshly-created device.
            *entry = Arc::downgrade(&inner);
        }

        Ok(SharedReadOnlyDevice { inner })
    }
}

// ── Shared memory device ────────────────────────────────────────────────────

/// Inner state of a shared read-only ublk device.
///
/// When the last `Arc<SharedReadOnlyDeviceInner>` is dropped, the device is
/// deleted asynchronously and the stale entry is removed from the pool.
struct SharedReadOnlyDeviceInner {
    device: UblkDevice,
    /// Canonical key used for the shared device pool lookup.
    image_config_key: PathBuf,
    released: AtomicBool,
    // Linux clears a block device's page cache when its last opener closes
    // (blkdev_put_whole -> kill_bdev). Keep an opener between sandbox launches;
    // the daemon's /dev/ublkcN handle does not keep /dev/ublkbN open. This does
    // not prevent normal cache reclaim under memory pressure.
    cache_fd: std::sync::Mutex<Option<File>>,
}

impl Drop for SharedReadOnlyDeviceInner {
    fn drop(&mut self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }

        drop(self.cache_fd.get_mut().unwrap().take());
        let dev_id = self.device.dev_id;
        let key = self.image_config_key.clone();
        let device_path = self.device.device_path.clone();
        let device = UblkDevice {
            dev_id,
            device_path,
        };
        let notify = Arc::new(Notify::new());
        UblkDeviceManager::global()
            .shared_readonly_releases
            .insert(key.clone(), Arc::clone(&notify));
        info!(
            dev_id,
            key = %key.display(),
            "last reference to shared read-only ublk device dropped, scheduling release"
        );
        // Remove the stale Weak entry from the pool and release the device.
        // Both operations happen on a detached task to avoid blocking the
        // caller's drop path.
        //
        // Use Handle::try_current() to guard against the case where Drop runs
        // after the tokio runtime has shut down (e.g. during static destructor
        // ordering at process exit). In that scenario we skip async cleanup —
        // the ublk daemon process is exiting anyway and the kernel will reclaim
        // the devices.
        let Some(handle) = tokio::runtime::Handle::try_current().ok() else {
            warn!(
                dev_id,
                "tokio runtime unavailable during drop, skipping async device cleanup"
            );
            UblkDeviceManager::global()
                .shared_readonly_releases
                .remove_if(&key, |_, existing| Arc::ptr_eq(existing, &notify));
            notify.notify_waiters();
            return;
        };
        handle.spawn(async move {
            let _ = release_shared_readonly_device(key, device, notify).await;
        });
    }
}

async fn release_shared_readonly_device(
    key: PathBuf,
    device: UblkDevice,
    notify: Arc<Notify>,
) -> Result<()> {
    let dev_id = device.dev_id;

    // Remove stale entry — only if it's still our Weak (not replaced by a
    // fresh entry for the same key).
    UblkDeviceManager::global()
        .shared_readonly_devices
        .remove_if(&key, |_, weak| {
            weak.strong_count() == 0
                || weak
                    .upgrade()
                    .is_some_and(|inner| inner.released.load(Ordering::Acquire))
        });

    let release_result = UblkDeviceManager::global().release_device(&device).await;
    if let Err(e) = &release_result {
        warn!(dev_id, error = %e, "failed to release shared read-only ublk device");
    }
    UblkDeviceManager::global()
        .shared_readonly_releases
        .remove_if(&key, |_, existing| Arc::ptr_eq(existing, &notify));
    notify.notify_waiters();
    release_result
}

/// A shared, reference-counted handle to a read-only memory ublk device.
///
/// Cloning this handle increments the reference count. The underlying ublk
/// device is deleted only when the last handle is dropped.
#[derive(Clone)]
pub(crate) struct SharedReadOnlyDevice {
    inner: Arc<SharedReadOnlyDeviceInner>,
}

impl SharedReadOnlyDevice {
    /// Identity of this live device generation, not a recyclable kernel ID.
    /// The caller must retain a clone while using this identity.
    pub(crate) fn prefetch_identity(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }

    pub fn image_config_path(&self) -> &Path {
        &self.inner.image_config_key
    }

    /// The `/dev/ublkb<N>` path of this device.
    pub fn device_path(&self) -> &Path {
        self.inner.device.device_path()
    }

    pub async fn release(self) -> Result<()> {
        let key = self.inner.image_config_key.clone();
        let entry = UblkDeviceManager::global()
            .shared_readonly_devices
            .get_mut(&key);
        if Arc::strong_count(&self.inner) != 1 {
            return Ok(());
        }
        if self.inner.released.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let key = self.inner.image_config_key.clone();
        let device = self.inner.device.clone();
        let notify = Arc::new(Notify::new());
        UblkDeviceManager::global()
            .shared_readonly_releases
            .insert(key.clone(), Arc::clone(&notify));
        drop(entry);
        drop(self.inner.cache_fd.lock().unwrap().take());
        release_shared_readonly_device(key, device, notify).await
    }
}
/// A handle to a single `/dev/ublkb<N>` device.
///
/// This is a pure data struct. All lifecycle operations (create, delete,
/// snapshot) go through [`UblkDeviceManager`].
#[derive(Clone, Debug)]
pub(crate) struct UblkDevice {
    dev_id: u32,
    device_path: PathBuf,
}

impl UblkDevice {
    pub fn device_path(&self) -> &Path {
        &self.device_path
    }

    pub fn dev_id(&self) -> u32 {
        self.dev_id
    }
}

/// Record per-device overlaybd/ext4 usage gauges piggybacked on a restack RPC.
fn record_restack_usage_stats(dev_id: u32, kind: &'static str, stats: &RestackSnapshotStats) {
    let dev_id = dev_id.to_string();
    if let Some(stat) = &stats.data_stat {
        metrics::gauge!(
            "agentenv_overlaybd_data_bytes",
            "dev_id" => dev_id.clone(),
            "kind" => kind,
            "stat" => "valid",
        )
        .set(stat.valid_data_size as f64);
    }
    if let Some(used) = stats.ext4_used_bytes {
        metrics::gauge!(
            "agentenv_ext4_used_bytes",
            "dev_id" => dev_id.clone(),
            "kind" => kind,
        )
        .set(used as f64);
        if let Some(stat) = &stats.data_stat {
            metrics::gauge!(
                "agentenv_trim_reclaimable_bytes",
                "dev_id" => dev_id,
                "kind" => kind,
            )
            .set(stat.valid_data_size.saturating_sub(used) as f64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_persisted_cow_backend_with_migration_guidance() {
        let error = serde_json::from_str::<UblkBackend>(r#""Cow""#)
            .expect_err("persisted CoW backend must be rejected");
        let message = error.to_string();

        assert!(message.contains("cannot be resumed"), "error: {message}");
        assert!(message.contains("OverlayBD"), "error: {message}");
    }
}
