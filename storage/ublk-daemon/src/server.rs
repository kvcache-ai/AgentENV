use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use overlaybd::config::{ImageConfig, UpperConfig, UpperMode};
use overlaybd::helper::prepare_runtime_upper;
use overlaybd::image_file::ImageFile;
use overlaybd::image_service::ImageService;
use overlaybd::RestackSnapshotTerminalFailure;
use tokio::net::UnixListener;
use tokio::sync::{Mutex, Notify, RwLock};
use warm_pool::{PoolConfig, PoolMaintenanceAction, WarmPool};

use storage_util::io_ring::IoRingHandle;
use uvm_ublk::{
    delete_dev, ublk_caps, wait_for_ublk_dev, OverlaybdTarget, UVMUblkCtrlBuilder, UVMUblkDev,
    UVMUblkDevBuilder, UVMUblkTarget,
};

use crate::protocol::{
    recv_message, send_message, AccessMode, DaemonRequest, DaemonResponse, ResizeToolSpec,
};
use crate::runtime;

// ── Managed device wrapper ──────────────────────────────────────────────────

struct ManagedDevice {
    dev: UVMUblkDev<OverlaybdTarget>,
    image: Arc<ImageFile>,
}

// ── Pooled device wrapper ───────────────────────────────────────────────────

/// A warm ublk device ready for reuse.
struct PooledDevice {
    dev: UVMUblkDev<OverlaybdTarget>,
    _placeholder_image: Arc<ImageFile>,
    dev_sectors: u64,
}

// ── Active device tracking ──────────────────────────────────────────────────

/// Tracks an active exclusive-mode device (rootfs, snapshot resume).
struct ActiveExclusive {
    dev: UVMUblkDev<OverlaybdTarget>,
    image_config: PathBuf,
    image: Arc<ImageFile>,
}

/// Tracks an active shared-mode device (memory snapshot, refcounted).
struct ActiveShared {
    dev: UVMUblkDev<OverlaybdTarget>,
    image_config: PathBuf,
    image: Arc<ImageFile>,
    refcount: usize,
}

/// Canonical key for shared-mode devices: (image_config, global_config).
type SharedKey = (PathBuf, PathBuf);

/// Canonical key used to coordinate image opens with restack mutation.
type ImageLockKey = PathBuf;

// ── Lazy ImageService cache ─────────────────────────────────────────────────

/// Cache of `ImageService` instances keyed by canonical global config path.
///
/// Each unique overlaybd global config produces its own `ImageService` with
/// independent io_ring pools and cache settings. Entries are created lazily
/// on the first request that opens an overlaybd image with a given config path.
pub(crate) struct ImageServiceCache {
    services: Mutex<HashMap<PathBuf, ImageService>>,
    p2p_publish_url: Option<String>,
}

impl ImageServiceCache {
    pub(crate) fn new(p2p_publish_url: Option<String>) -> Self {
        Self {
            services: Mutex::new(HashMap::new()),
            p2p_publish_url,
        }
    }

    /// Get or lazily create an `ImageService` for the given global config path.
    pub(crate) async fn get_or_create(&self, global_config: &Path) -> Result<ImageService> {
        let canonical =
            std::fs::canonicalize(global_config).unwrap_or_else(|_| global_config.to_path_buf());

        let mut services = self.services.lock().await;
        if let Some(service) = services.get(&canonical) {
            return Ok(service.clone());
        }

        // Hold the async mutex across creation so concurrent requests for the
        // same config cannot create duplicate ImageService instances with
        // background work or open handles that are immediately discarded.
        let service = ImageService::from_config_path_with_p2p_publish_url(
            &canonical,
            self.p2p_publish_url.clone(),
        )
        .await
        .with_context(|| {
            format!(
                "create ImageService from global config: {}",
                canonical.display()
            )
        })?;
        services.insert(canonical, service.clone());
        Ok(service)
    }
}

// ── Pool state ──────────────────────────────────────────────────────────────

struct PoolState {
    /// Idle warm devices ready for reuse.
    idle: WarmPool<PooledDevice>,
    /// Active exclusive-mode devices (rootfs, snapshot resume).
    active_exclusive: DashMap<u32, ActiveExclusive>,
    /// Active shared-mode devices (memory snapshot, refcounted).
    active_shared: DashMap<SharedKey, ActiveShared>,
    /// Reverse index for shared devices so release/update/restack do not scan
    /// `active_shared` with `iter_mut`, which can deadlock under concurrent
    /// release requests.
    shared_by_dev_id: DashMap<u32, SharedKey>,
    /// Per-image locks keep restack mutation exclusive with image opens for
    /// the same image config without serializing unrelated sandbox resumes.
    image_locks: DashMap<ImageLockKey, Arc<RwLock<()>>>,
    /// Coalesces asynchronous pool refill work so concurrent acquire/release
    /// requests do not all synchronously create replacement ublk devices.
    refill_inflight: AtomicBool,
    config: PoolConfig,
    /// Maximum idle count that proactive refill may reach. Returned devices
    /// remain bounded by `config.high_watermark` instead.
    prewarm_high_watermark: usize,
    /// Ublk feature flags detected at startup.
    features: u64,
    /// Daemon-owned same-size sparse placeholder images, lazily built per device
    /// virtual size. On release the device target is swapped to one of these so
    /// the idle pool stops pinning the released business image.
    placeholders: Mutex<HashMap<u64, (PathBuf, Arc<ImageFile>)>>,
    /// Image service used to open placeholder images. Placeholders have no
    /// lowers, so opening them is local-only (no registry access).
    image_service: ImageService,
    /// Directory holding placeholder sparse files (daemon-owned, not the cache).
    placeholder_dir: PathBuf,
}

impl PoolState {
    fn new(
        config: PoolConfig,
        prewarm_high_watermark: usize,
        features: u64,
        image_service: ImageService,
        placeholder_dir: PathBuf,
    ) -> Self {
        Self {
            idle: WarmPool::new(config.clone()),
            active_exclusive: DashMap::new(),
            active_shared: DashMap::new(),
            shared_by_dev_id: DashMap::new(),
            image_locks: DashMap::new(),
            refill_inflight: AtomicBool::new(false),
            config,
            prewarm_high_watermark,
            features,
            placeholders: Mutex::new(HashMap::new()),
            image_service,
            placeholder_dir,
        }
    }

    /// Get (or lazily build) a daemon-owned sparse placeholder image whose
    /// virtual size matches `virtual_size`. Returns the placeholder's config
    /// path and opened image.
    ///
    /// The async build runs while holding the placeholder lock so two concurrent
    /// first-time releases of the same size cannot double-build and clobber each
    /// other's placeholder file.
    async fn placeholder_for(&self, virtual_size: u64) -> Result<(PathBuf, Arc<ImageFile>)> {
        let mut placeholders = self.placeholders.lock().await;
        if let Some(existing) = placeholders.get(&virtual_size) {
            return Ok(existing.clone());
        }

        std::fs::create_dir_all(&self.placeholder_dir).with_context(|| {
            format!(
                "create pool placeholder dir {}",
                self.placeholder_dir.display()
            )
        })?;
        let data_path = self.placeholder_dir.join(format!("{virtual_size}.data"));
        let config_path = self.placeholder_dir.join(format!("{virtual_size}.json"));

        prepare_runtime_upper(&data_path, None, virtual_size, UpperMode::Sparse)
            .with_context(|| format!("create placeholder upper {}", data_path.display()))?;

        let config = ImageConfig {
            lowers: Vec::new(),
            upper: UpperConfig {
                mode: Some(UpperMode::Sparse),
                data: data_path.to_string_lossy().into_owned(),
                ..Default::default()
            },
            ..Default::default()
        };
        std::fs::write(
            &config_path,
            serde_json::to_vec(&config).context("serialize placeholder image config")?,
        )
        .with_context(|| format!("write placeholder config {}", config_path.display()))?;

        let image = Arc::new(
            ImageFile::open(
                config,
                self.image_service.clone(),
                Some(config_path.clone()),
            )
            .await
            .with_context(|| format!("open placeholder image {}", config_path.display()))?,
        );
        let entry = (config_path, image);
        placeholders.insert(virtual_size, entry.clone());
        Ok(entry)
    }

    fn supports_update_size(&self) -> bool {
        self.features & ublk_caps::UBLK_F_UPDATE_SIZE != 0
    }

    fn prewarm_refill_count(&self) -> usize {
        let idle_len = self.idle.len();
        let remaining = self.prewarm_high_watermark.saturating_sub(idle_len);
        if remaining == 0 {
            return 0;
        }

        match self.idle.compute_maintenance_action(idle_len) {
            PoolMaintenanceAction::Fill(to_create) => to_create.min(remaining),
            PoolMaintenanceAction::Drain(_) | PoolMaintenanceAction::Idle => 0,
        }
    }

    fn image_lock_key(image_config: &Path) -> ImageLockKey {
        // Matches ImageServiceCache behavior: canonicalize when possible, but
        // tolerate paths that do not exist yet by falling back to the raw path.
        // Non-canonical aliases to the same missing file are therefore not
        // serialized with each other until the path exists.
        std::fs::canonicalize(image_config).unwrap_or_else(|_| image_config.to_path_buf())
    }

    fn image_lock(&self, image_config: &Path) -> Arc<RwLock<()>> {
        let key = Self::image_lock_key(image_config);
        Arc::clone(
            self.image_locks
                .entry(key)
                .or_insert_with(|| Arc::new(RwLock::new(())))
                .value(),
        )
    }
}

// ── Daemon server ───────────────────────────────────────────────────────────

pub struct UblkDaemonServer {
    socket_path: PathBuf,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: Arc<ImageServiceCache>,
    default_image_service: ImageService,
    devices: Arc<DashMap<u32, ManagedDevice>>,
    pool_state: Option<Arc<PoolState>>,
    resize_tool: Option<ResizeToolSpec>,
    resize_global_config: PathBuf,
    /// Daemon-wide permit serializing overlaybd-resize child processes: they
    /// all share the single isolated resize cacheDir, so only one may run at
    /// a time.
    resize_permit: Arc<Mutex<()>>,
    shutdown: Arc<Notify>,
}

impl UblkDaemonServer {
    pub fn new(
        socket_path: PathBuf,
        ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
        default_image_service: ImageService,
        resize_global_config: PathBuf,
    ) -> Self {
        Self::new_with_p2p_publish_url(
            socket_path,
            ctrl_ring,
            default_image_service,
            resize_global_config,
            None,
        )
    }

    pub fn new_with_p2p_publish_url(
        socket_path: PathBuf,
        ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
        default_image_service: ImageService,
        resize_global_config: PathBuf,
        p2p_publish_url: Option<String>,
    ) -> Self {
        let mut cache = ImageServiceCache::new(p2p_publish_url);
        // Pre-seed the cache with the default image service so the first
        // request that matches the daemon's CLI --global-config doesn't
        // re-create it.
        {
            let canonical = std::fs::canonicalize(default_image_service.config_path())
                .unwrap_or_else(|_| default_image_service.config_path().to_path_buf());
            cache
                .services
                .get_mut()
                .insert(canonical, default_image_service.clone());
        }
        Self {
            socket_path,
            ctrl_ring,
            image_service_cache: Arc::new(cache),
            default_image_service,
            devices: Arc::new(DashMap::new()),
            pool_state: None,
            resize_tool: None,
            resize_global_config,
            resize_permit: Arc::new(Mutex::new(())),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Configure the daemon-local OverlayBD resize tool.
    /// Must be called before `run()`.
    pub fn set_resize_tool(&mut self, resize_tool: ResizeToolSpec) {
        self.resize_tool = Some(resize_tool);
    }

    pub async fn detect_ublk_features(&self) -> Result<u64> {
        detect_ublk_features(&self.ctrl_ring).await
    }

    /// Enable warm pooling with the given configuration.
    /// Must be called before `run()`.
    pub fn enable_pool(
        &mut self,
        config: PoolConfig,
        prewarm_high_watermark: usize,
        features: u64,
    ) {
        tracing::info!(
            low_watermark = config.low_watermark,
            high_watermark = config.high_watermark,
            prewarm_high_watermark,
            startup_prewarm = config.startup_prewarm,
            "enabling overlaybd warm pool"
        );
        self.pool_state = Some(Arc::new(PoolState::new(
            config,
            prewarm_high_watermark,
            features,
            self.default_image_service.clone(),
            self.pool_placeholder_dir(),
        )));
    }

    /// Daemon-owned directory for warm-pool placeholder images, kept separate
    /// from the business image cache.
    fn pool_placeholder_dir(&self) -> PathBuf {
        self.socket_path
            .parent()
            .map(|parent| parent.join("overlaybd-pool-placeholders"))
            .unwrap_or_else(|| std::env::temp_dir().join("overlaybd-pool-placeholders"))
    }

    /// Run the daemon accept loop until shutdown is requested or a fatal error occurs.
    pub async fn run(&self) -> Result<()> {
        self.run_with_ready_signal(|| Ok(())).await
    }

    /// Run the daemon and invoke `signal_ready` after the control socket has
    /// been bound successfully.
    pub async fn run_with_ready_signal<F>(&self, signal_ready: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        // Clean up stale socket if present.
        if self.socket_path.exists() {
            match std::os::unix::net::UnixStream::connect(&self.socket_path) {
                Ok(_) => bail!(
                    "daemon socket {} is in use by another process",
                    self.socket_path.display()
                ),
                Err(_) => {
                    std::fs::remove_file(&self.socket_path).with_context(|| {
                        format!("remove stale daemon socket: {}", self.socket_path.display())
                    })?;
                }
            }
        }

        let listener = UnixListener::bind(&self.socket_path)
            .with_context(|| format!("bind daemon socket: {}", self.socket_path.display()))?;
        tracing::info!(
            path = %self.socket_path.display(),
            resize_global_config = %self.resize_global_config.display(),
            "ublk daemon listening"
        );
        signal_ready()?;

        // Note: spawned connection handlers may outlive the accept loop when
        // shutdown fires. This is acceptable — DashMap operations are individually
        // atomic, so a concurrent handle_delete and stop_all_devices on the same
        // device ID will race on `devices.remove()`, and only one will get the
        // device while the other silently skips it. No data corruption is possible.
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let (stream, _) = accept.context("accept daemon connection")?;
                    let devices = Arc::clone(&self.devices);
                    let ctrl_ring = self.ctrl_ring.clone();
                    let image_service_cache = Arc::clone(&self.image_service_cache);
                    let pool_state = self.pool_state.as_ref().map(Arc::clone);
                    let resize_tool = self.resize_tool.clone();
                    let resize_global_config = self.resize_global_config.clone();
                    let resize_permit = Arc::clone(&self.resize_permit);
                    let shutdown = Arc::clone(&self.shutdown);
                    tokio::spawn(async move {
                        if let Err(err) = handle_connection(
                            stream,
                            devices,
                            ctrl_ring,
                            image_service_cache,
                            pool_state,
                            resize_tool,
                            resize_global_config,
                            resize_permit,
                            shutdown,
                        ).await {
                            tracing::error!(?err, "daemon connection handler failed");
                        }
                    });
                }
                _ = self.shutdown.notified() => {
                    tracing::info!("ublk daemon shutdown requested");
                    break;
                }
            }
        }

        // Graceful cleanup: stop all devices.
        self.stop_all_devices().await;

        // Remove socket file.
        let _ = std::fs::remove_file(&self.socket_path);
        tracing::info!("ublk daemon stopped");
        Ok(())
    }

    /// Request the daemon to shut down.
    pub fn request_shutdown(&self) {
        self.shutdown.notify_one();
    }

    async fn stop_all_devices(&self) {
        let dev_ids: Vec<u32> = self.devices.iter().map(|r| *r.key()).collect();
        for dev_id in dev_ids {
            if let Some((_, mut device)) = self.devices.remove(&dev_id) {
                tracing::info!(dev_id, "stopping device during shutdown");
                quiesce_managed_device(&mut device).await;
                // ManagedDevice holds an open fd to the ublk char dev.
                // Must drop it before delete_dev, or the DEL_DEV ioctl will block.
                drop(device);
                if let Err(err) = delete_dev(self.ctrl_ring.clone(), dev_id).await {
                    tracing::warn!(dev_id, ?err, "failed to stop device during shutdown");
                }
            }
        }

        if let Some(pool) = &self.pool_state {
            let exclusive_ids: Vec<u32> = pool
                .active_exclusive
                .iter()
                .map(|entry| *entry.key())
                .collect();
            for dev_id in exclusive_ids {
                if let Some((_, active)) = pool.active_exclusive.remove(&dev_id) {
                    stop_overlaybd_device(self.ctrl_ring.clone(), active.dev).await;
                }
            }

            let shared_keys: Vec<SharedKey> = pool
                .active_shared
                .iter()
                .map(|entry| entry.key().clone())
                .collect();
            for key in shared_keys {
                if let Some((_, active)) = pool.active_shared.remove(&key) {
                    pool.shared_by_dev_id.remove(&active.dev.dev_id());
                    stop_overlaybd_device(self.ctrl_ring.clone(), active.dev).await;
                }
            }

            let idle_devices: Vec<PooledDevice> = pool.idle.drain_all();
            for pooled in idle_devices {
                stop_overlaybd_device(self.ctrl_ring.clone(), pooled.dev).await;
            }
        }
    }
}

// ── Per-connection handler ──────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    mut stream: tokio::net::UnixStream,
    devices: Arc<DashMap<u32, ManagedDevice>>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: Arc<ImageServiceCache>,
    pool_state: Option<Arc<PoolState>>,
    resize_tool: Option<ResizeToolSpec>,
    resize_global_config: PathBuf,
    resize_permit: Arc<Mutex<()>>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let Some(request) = recv_message::<DaemonRequest>(&mut stream).await? else {
        return Ok(());
    };

    let response = match request {
        DaemonRequest::CreateOverlaybd {
            image_config,
            global_config,
        } => {
            handle_create_raw_overlaybd(
                &devices,
                ctrl_ring,
                &image_service_cache,
                &image_config,
                &global_config,
            )
            .await
        }
        DaemonRequest::CreateOverlaybdRuntimeDevice {
            source_image_config,
            global_config,
            runtime_dir,
            read_only,
            runtime_upper_mode,
            requested_virtual_size,
            known_source_virtual_size,
            allow_shrink,
        } => {
            let request = OverlaybdRuntimeDeviceRequest {
                source_image_config: &source_image_config,
                global_config: &global_config,
                runtime_dir: &runtime_dir,
                read_only,
                runtime_upper_mode,
                requested_virtual_size,
                known_source_virtual_size,
                resize_tool: resize_tool.as_ref(),
                resize_global_config: &resize_global_config,
                resize_permit: Arc::clone(&resize_permit),
                allow_shrink,
            };
            handle_create_overlaybd_runtime_device(
                request,
                ctrl_ring,
                &devices,
                &image_service_cache,
                &pool_state,
            )
            .await
        }
        DaemonRequest::Delete { dev_id } => handle_delete(&devices, ctrl_ring, dev_id).await,
        DaemonRequest::RestackSnapshot {
            dev_id,
            output_layer_path,
        } => handle_restack_snapshot(&devices, &pool_state, dev_id, &output_layer_path).await,
        DaemonRequest::GetFeatures => handle_get_features(&pool_state),
        DaemonRequest::NotifySandboxReady { device_key } => {
            tracing::info!(
                device_key,
                "sandbox reported envd-ready; releasing held downloads"
            );
            overlaybd::download_gate::notify_sandbox_ready(&device_key);
            Ok(DaemonResponse::Ok)
        }
        DaemonRequest::AcquireOverlaybd {
            image_config,
            global_config,
            virtual_size,
            access_mode,
        } => {
            handle_acquire_overlaybd(
                &pool_state,
                ctrl_ring.clone(),
                &image_service_cache,
                &image_config,
                &global_config,
                virtual_size,
                access_mode,
            )
            .await
        }
        DaemonRequest::ReleaseOverlaybd { dev_id } => {
            handle_release_overlaybd(&pool_state, ctrl_ring.clone(), dev_id).await
        }
        DaemonRequest::UpdateSize {
            dev_id,
            new_sectors,
        } => handle_update_size(&pool_state, dev_id, new_sectors).await,
        DaemonRequest::Shutdown => {
            shutdown.notify_one();
            Ok(DaemonResponse::Ok)
        }
    };

    let response = match response {
        Ok(resp) => resp,
        Err(err) => {
            if err
                .downcast_ref::<RestackSnapshotTerminalFailure>()
                .is_some()
            {
                DaemonResponse::TerminalError {
                    message: format!("{err:#}"),
                }
            } else if err.downcast_ref::<runtime::InvalidRequest>().is_some() {
                DaemonResponse::InvalidRequest {
                    message: format!("{err:#}"),
                }
            } else {
                DaemonResponse::Error {
                    message: format!("{err:#}"),
                }
            }
        }
    };

    send_message(&mut stream, &response).await?;
    Ok(())
}

// ── Request handlers ────────────────────────────────────────────────────────

struct OverlaybdRuntimeDeviceRequest<'a> {
    source_image_config: &'a Path,
    global_config: &'a Path,
    runtime_dir: &'a Path,
    read_only: bool,
    runtime_upper_mode: overlaybd::config::UpperMode,
    requested_virtual_size: Option<u64>,
    known_source_virtual_size: Option<u64>,
    resize_tool: Option<&'a crate::protocol::ResizeToolSpec>,
    resize_global_config: &'a Path,
    resize_permit: Arc<Mutex<()>>,
    allow_shrink: bool,
}

async fn handle_create_overlaybd_runtime_device(
    request: OverlaybdRuntimeDeviceRequest<'_>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    devices: &DashMap<u32, ManagedDevice>,
    image_service_cache: &ImageServiceCache,
    pool_state: &Option<Arc<PoolState>>,
) -> Result<DaemonResponse> {
    let runtime =
        match runtime::materialize_overlaybd_runtime(runtime::MaterializeOverlaybdRuntimeRequest {
            image_service_cache,
            source_image_config: request.source_image_config,
            global_config: request.global_config,
            runtime_dir: request.runtime_dir,
            read_only: request.read_only,
            runtime_upper_mode: request.runtime_upper_mode,
            requested_virtual_size: request.requested_virtual_size,
            known_source_virtual_size: request.known_source_virtual_size,
            resize_tool: request.resize_tool,
            resize_global_config: request.resize_global_config,
            resize_permit: request.resize_permit,
            allow_shrink: request.allow_shrink,
        })
        .await
        {
            Ok(runtime) => runtime,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "materialize overlaybd runtime in {}",
                        request.runtime_dir.display()
                    )
                });
            }
        };

    let created = if pool_state.is_some() {
        match handle_acquire_overlaybd(
            pool_state,
            ctrl_ring,
            image_service_cache,
            &runtime.runtime_image_config_path,
            request.global_config,
            runtime.actual_virtual_size,
            AccessMode::Exclusive,
        )
        .await
        {
            Ok(DaemonResponse::DeviceAcquired {
                dev_id,
                device_path,
            }) => Ok((dev_id, device_path)),
            Ok(other) => bail!("unexpected acquire response for runtime device: {other:?}"),
            Err(err) => Err(err),
        }
    } else {
        create_overlaybd_device(
            devices,
            ctrl_ring,
            image_service_cache,
            &runtime.runtime_image_config_path,
            request.global_config,
        )
        .await
    };

    let (dev_id, device_path) = match created {
        Ok(created) => created,
        Err(err) => {
            // The failed create/acquire path has returned, so any transient
            // ImageFile/target locals have been dropped before we remove the
            // daemon-owned runtime directory.
            let runtime_image_config_path = runtime.runtime_image_config_path.clone();
            runtime.rollback();
            return Err(err).with_context(|| {
                format!(
                    "create overlaybd runtime device from {}",
                    runtime_image_config_path.display()
                )
            });
        }
    };

    Ok(DaemonResponse::OverlaybdRuntimeDeviceCreated {
        dev_id,
        device_path,
        actual_virtual_size: runtime.actual_virtual_size,
        runtime_image_config_path: runtime.runtime_image_config_path,
    })
}

async fn handle_create_raw_overlaybd(
    devices: &DashMap<u32, ManagedDevice>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
) -> Result<DaemonResponse> {
    tracing::info!(
        image_config = %image_config.display(),
        global_config = %global_config.display(),
        "creating raw overlaybd device"
    );

    let (dev_id, device_path) = create_overlaybd_device(
        devices,
        ctrl_ring,
        image_service_cache,
        image_config,
        global_config,
    )
    .await?;

    Ok(DaemonResponse::DeviceCreated {
        dev_id,
        device_path,
    })
}

async fn create_overlaybd_device(
    devices: &DashMap<u32, ManagedDevice>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
) -> Result<(u32, PathBuf)> {
    let image_service = image_service_cache
        .get_or_create(global_config)
        .await
        .context("resolve image service for raw overlaybd device")?;
    let image = Arc::new(
        image_service
            .create_image_file(image_config)
            .await
            .with_context(|| format!("open overlaybd image: {}", image_config.display()))?,
    );
    let image_config_path = image_config.to_path_buf();
    let discard_supported = !image.is_read_only().await;

    let target = OverlaybdTarget::from_opened_image(
        image_config_path,
        Arc::clone(&image),
        discard_supported,
    )
    .context("create overlaybd target")?;

    let ctrl = UVMUblkCtrlBuilder::new()
        .name("overlaybd-blk")
        .build(ctrl_ring.clone())
        .context("build ublk ctrl")?;

    let mut dev = UVMUblkDevBuilder::new(ctrl)
        .set_target(target)
        .build()
        .await
        .context("build ublk dev")?;

    let dev_id = dev.dev_id();
    if let Err(err) = dev.start().await.context("start ublk dev") {
        cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
        return Err(err);
    }

    if let Err(err) = wait_for_ublk_dev(dev_id).context("wait for ublk device") {
        cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
        return Err(err);
    }

    let device_path = dev.device_path().to_path_buf();
    devices.insert(
        dev_id,
        ManagedDevice {
            dev,
            image: Arc::clone(&image),
        },
    );
    tracing::info!(
        dev_id,
        path = %device_path.display(),
        "overlaybd device created"
    );
    Ok((dev_id, device_path))
}

async fn handle_delete(
    devices: &DashMap<u32, ManagedDevice>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    dev_id: u32,
) -> Result<DaemonResponse> {
    let Some((_, mut device)) = devices.remove(&dev_id) else {
        bail!("device {dev_id} not found");
    };

    quiesce_managed_device(&mut device).await;

    // ManagedDevice contains a open fd to the ublk char dev.
    // We need to drop it first, or else, the DEL_DEV command will stuck
    drop(device);
    tracing::info!(dev_id, "deleting device");
    delete_dev(ctrl_ring, dev_id)
        .await
        .with_context(|| format!("delete ublk device {dev_id}"))?;
    tracing::info!(dev_id, "device deleted");

    Ok(DaemonResponse::Deleted)
}

async fn quiesce_managed_device(device: &mut ManagedDevice) {
    quiesce_ublk_device(&mut device.dev).await;
}

async fn quiesce_ublk_device<T: UVMUblkTarget>(dev: &mut UVMUblkDev<T>) {
    let dev_id = dev.dev_id();

    if let Err(err) = dev.ctrl.stop_dev().await {
        tracing::warn!(dev_id, ?err, "failed to stop ublk device before delete");
        return;
    }

    if let Err(err) = tokio::time::timeout(Duration::from_secs(5), dev.wait_for_bg_tasks()).await {
        tracing::warn!(
            dev_id,
            ?err,
            "timed out waiting for ublk queue workers to exit after stop_dev"
        );
    }
}

async fn cleanup_failed_ublk_start<T: UVMUblkTarget>(
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    mut dev: UVMUblkDev<T>,
) {
    let dev_id = dev.dev_id();

    match dev.ctrl.stop_dev().await {
        Ok(()) => {}
        Err(err)
            if matches!(
                err.root_cause()
                    .downcast_ref::<std::io::Error>()
                    .and_then(|err| err.raw_os_error()),
                Some(libc::ENODEV)
            ) =>
        {
            tracing::info!(dev_id, "ublk device disappeared before startup cleanup");
            drop(dev);
            return;
        }
        Err(err) => {
            tracing::warn!(
                dev_id,
                ?err,
                "failed to stop ublk device after startup failure"
            );
        }
    }

    if let Err(err) = tokio::time::timeout(Duration::from_secs(5), dev.wait_for_bg_tasks()).await {
        tracing::warn!(
            dev_id,
            ?err,
            "timed out waiting for ublk queue workers after startup failure"
        );
    }

    drop(dev);

    let mut ctrl = match UVMUblkCtrlBuilder::new().dev_id(dev_id).build(ctrl_ring) {
        Ok(ctrl) => ctrl,
        Err(err) => {
            tracing::warn!(
                dev_id,
                ?err,
                "failed to build ublk ctrl for startup cleanup; kernel device may remain active"
            );
            return;
        }
    };

    match ctrl.del_dev().await {
        Ok(()) => {
            tracing::info!(dev_id, "deleted ublk device after startup failure");
        }
        Err(err)
            if matches!(
                err.root_cause()
                    .downcast_ref::<std::io::Error>()
                    .and_then(|err| err.raw_os_error()),
                Some(libc::ENODEV)
            ) =>
        {
            tracing::info!(dev_id, "ublk device already deleted after startup failure");
        }
        Err(err) => {
            tracing::warn!(
                dev_id,
                ?err,
                "failed to delete ublk device after startup failure; kernel device may remain active"
            );
        }
    }
}

async fn stop_overlaybd_device(
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    mut dev: UVMUblkDev<OverlaybdTarget>,
) {
    let dev_id = dev.dev_id();
    tracing::info!(dev_id, "stopping pooled overlaybd device during shutdown");
    quiesce_ublk_device(&mut dev).await;
    drop(dev);
    if let Err(err) = delete_dev(ctrl_ring, dev_id).await {
        tracing::warn!(
            dev_id,
            ?err,
            "failed to stop pooled overlaybd device during shutdown"
        );
    }
}

async fn handle_restack_snapshot(
    devices: &DashMap<u32, ManagedDevice>,
    pool_state: &Option<Arc<PoolState>>,
    dev_id: u32,
    output_layer_path: &Path,
) -> Result<DaemonResponse> {
    let (image, image_config) = if let Some(device_ref) = devices.get(&dev_id) {
        let image = Arc::clone(&device_ref.image);
        drop(device_ref);
        (image, None)
    } else if let Some(pool) = pool_state {
        if let Some(active) = pool.active_exclusive.get(&dev_id) {
            (Arc::clone(&active.image), Some(active.image_config.clone()))
        } else if let Some(shared_key) = pool.shared_by_dev_id.get(&dev_id) {
            let key = shared_key.clone();
            drop(shared_key);
            let active = pool
                .active_shared
                .get(&key)
                .with_context(|| format!("device {dev_id} not found for snapshot"))?;
            (Arc::clone(&active.image), Some(active.image_config.clone()))
        } else {
            bail!("device {dev_id} not found for snapshot");
        }
    } else {
        bail!("device {dev_id} not found for snapshot");
    };

    tracing::info!(
        dev_id,
        output = %output_layer_path.display(),
        "creating restack snapshot"
    );

    let descriptor =
        if let (Some(pool), Some(image_config)) = (pool_state.as_ref(), image_config.as_deref()) {
            let image_lock = pool.image_lock(image_config);
            let _image_guard = image_lock.write().await;
            image.create_snapshot_and_restack(output_layer_path).await
        } else {
            image.create_snapshot_and_restack(output_layer_path).await
        };
    let descriptor = descriptor.with_context(|| {
        format!(
            "restack overlaybd snapshot for dev_id={dev_id} to {}",
            output_layer_path.display()
        )
    })?;

    // Best-effort usage observability; never fails the restack itself.
    let data_stat = image.data_stat().await.ok();
    let image_vf: Arc<dyn overlaybd::virtual_file::VirtualFile> = image.clone();
    let ext4_used_bytes = overlaybd::ext4_stat::ext4_used_bytes(&image_vf).await.ok();

    tracing::info!(
        dev_id,
        output = %output_layer_path.display(),
        "ublk daemon restack snapshot completed"
    );
    Ok(DaemonResponse::RestackSnapshotCreated {
        descriptor,
        data_stat,
        ext4_used_bytes,
    })
}

// ── Pool handlers ───────────────────────────────────────────────────────────

fn handle_get_features(pool_state: &Option<Arc<PoolState>>) -> Result<DaemonResponse> {
    let flags = pool_state.as_ref().map(|p| p.features).unwrap_or(0);
    Ok(DaemonResponse::Features { flags })
}

async fn handle_acquire_overlaybd(
    pool_state: &Option<Arc<PoolState>>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
    virtual_size: u64,
    access_mode: AccessMode,
) -> Result<DaemonResponse> {
    let pool = pool_state
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("warm pool not enabled"))?;

    let image_lock = pool.image_lock(image_config);
    let image = {
        let _image_guard = image_lock.read().await;
        let image_service = image_service_cache
            .get_or_create(global_config)
            .await
            .context("resolve image service for acquire")?;

        Arc::new(
            image_service
                .create_image_file(image_config)
                .await
                .with_context(|| format!("open overlaybd image: {}", image_config.display()))?,
        )
    };
    let actual_virtual_size = image.size_bytes();
    anyhow::ensure!(
        virtual_size == actual_virtual_size,
        "requested overlaybd acquire virtual size {virtual_size} does not match image virtual size {actual_virtual_size}: {}",
        image_config.display()
    );

    match access_mode {
        AccessMode::Exclusive => {
            acquire_exclusive(Arc::clone(pool), ctrl_ring, image_config, image).await
        }
        AccessMode::Shared => {
            acquire_shared(
                Arc::clone(pool),
                ctrl_ring,
                image_config,
                global_config,
                image,
            )
            .await
        }
    }
}

async fn prepare_overlaybd_device(
    pool: &PoolState,
    ctrl_ring: &IoRingHandle<io_uring::squeue::Entry128>,
    image_config: &Path,
    image: &Arc<ImageFile>,
    mode: &'static str,
) -> Result<(UVMUblkDev<OverlaybdTarget>, bool)> {
    let new_sectors = image.num_lbas();
    if let Some(p) = take_idle_device(pool, new_sectors) {
        let dev_id = p.dev.dev_id();
        tracing::debug!(dev_id, mode, "reusing warm device from pool");

        let discard_supported = !image.is_read_only().await;
        p.dev
            .target()
            .swap_state(
                image_config.to_path_buf(),
                Arc::clone(image),
                discard_supported,
            )
            .with_context(|| format!("swap {mode} target state"))?;

        if pool.supports_update_size() && p.dev_sectors != new_sectors {
            update_device_size(&p.dev, new_sectors)
                .await
                .with_context(|| format!("update {mode} device size for dev_id={dev_id}"))?;
        }

        Ok((p.dev, true))
    } else {
        tracing::debug!(mode, "pool miss: creating new device");
        let dev = create_new_device(ctrl_ring.clone(), image_config, image)
            .await
            .with_context(|| format!("create new {mode} ublk device on pool miss"))?;
        Ok((dev, false))
    }
}

async fn acquire_exclusive(
    pool: Arc<PoolState>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_config: &Path,
    image: Arc<ImageFile>,
) -> Result<DaemonResponse> {
    let (dev, is_reused) =
        prepare_overlaybd_device(&pool, &ctrl_ring, image_config, &image, "exclusive").await?;

    let dev_id = dev.dev_id();
    let device_path = dev.device_path().to_path_buf();

    if is_reused {
        tracing::info!(dev_id, path = %device_path.display(), "acquired exclusive overlaybd device (reused)");
    } else {
        tracing::info!(dev_id, path = %device_path.display(), "acquired exclusive overlaybd device (new)");
    }

    // Move to active exclusive map.
    if pool.config.startup_prewarm {
        schedule_idle_pool_refill(Arc::clone(&pool), ctrl_ring, image.size_bytes());
    }

    pool.active_exclusive.insert(
        dev_id,
        ActiveExclusive {
            dev,
            image_config: image_config.to_path_buf(),
            image,
        },
    );

    Ok(DaemonResponse::DeviceAcquired {
        dev_id,
        device_path,
    })
}

async fn acquire_shared(
    pool: Arc<PoolState>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_config: &Path,
    global_config: &Path,
    image: Arc<ImageFile>,
) -> Result<DaemonResponse> {
    let key = (image_config.to_path_buf(), global_config.to_path_buf());

    // Check if already active.
    if let Some(mut entry) = pool.active_shared.get_mut(&key) {
        entry.refcount += 1;
        let dev_id = entry.dev.dev_id();
        let device_path = entry.dev.device_path().to_path_buf();
        tracing::info!(
            dev_id,
            refcount = entry.refcount,
            "reusing shared overlaybd device"
        );
        return Ok(DaemonResponse::DeviceAcquired {
            dev_id,
            device_path,
        });
    }

    let (dev, is_reused) =
        prepare_overlaybd_device(&pool, &ctrl_ring, image_config, &image, "shared").await?;

    let dev_id = dev.dev_id();
    let device_path = dev.device_path().to_path_buf();

    if is_reused {
        tracing::info!(dev_id, path = %device_path.display(), "acquired shared overlaybd device (reused)");
    } else {
        tracing::info!(dev_id, path = %device_path.display(), "acquired shared overlaybd device (new)");
    }

    // Capture the size before `image` is moved into the active map; refill only
    // needs it to build a same-size placeholder.
    let refill_virtual_size = image.size_bytes();

    match pool.active_shared.entry(key.clone()) {
        Entry::Occupied(mut entry) => {
            let active = entry.get_mut();
            active.refcount += 1;
            let existing_dev_id = active.dev.dev_id();
            let existing_path = active.dev.device_path().to_path_buf();
            let refcount = active.refcount;
            drop(entry);

            // Do not idle a redundant business-image device.
            drop(image);
            stop_overlaybd_device(ctrl_ring, dev).await;
            tracing::info!(
                dev_id = existing_dev_id,
                refcount,
                "concurrent shared overlaybd acquire reused existing device"
            );
            return Ok(DaemonResponse::DeviceAcquired {
                dev_id: existing_dev_id,
                device_path: existing_path,
            });
        }
        Entry::Vacant(entry) => {
            pool.shared_by_dev_id.insert(dev_id, key);
            entry.insert(ActiveShared {
                dev,
                image_config: image_config.to_path_buf(),
                image,
                refcount: 1,
            });
        }
    }

    if pool.config.startup_prewarm {
        schedule_idle_pool_refill(Arc::clone(&pool), ctrl_ring, refill_virtual_size);
    }

    Ok(DaemonResponse::DeviceAcquired {
        dev_id,
        device_path,
    })
}

async fn handle_release_overlaybd(
    pool_state: &Option<Arc<PoolState>>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    dev_id: u32,
) -> Result<DaemonResponse> {
    let pool = pool_state
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("warm pool not enabled"))?;

    // Try exclusive first.
    if let Some((_, active)) = pool.active_exclusive.remove(&dev_id) {
        return release_exclusive_device(pool, ctrl_ring, dev_id, active).await;
    }

    // Try shared.
    if let Some(shared_key) = pool.shared_by_dev_id.get(&dev_id) {
        let key = shared_key.clone();
        drop(shared_key);

        let mut remove_shared = false;
        if let Some(mut entry) = pool.active_shared.get_mut(&key) {
            entry.refcount = entry.refcount.saturating_sub(1);
            remove_shared = entry.refcount == 0;
            if !remove_shared {
                tracing::info!(
                    dev_id,
                    refcount = entry.refcount,
                    "decremented shared refcount"
                );
                return Ok(DaemonResponse::Released);
            }
        }

        if remove_shared {
            pool.shared_by_dev_id.remove(&dev_id);
            if let Some((_, active)) = pool.active_shared.remove(&key) {
                return release_shared_device(pool, ctrl_ring, dev_id, active).await;
            }
        }
    }

    bail!("device {dev_id} not found in active pool");
}

async fn release_exclusive_device(
    pool: &Arc<PoolState>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    dev_id: u32,
    active: ActiveExclusive,
) -> Result<DaemonResponse> {
    tracing::info!(dev_id, "releasing exclusive device");
    idle_released_device(pool, ctrl_ring, dev_id, active.dev, active.image).await;
    Ok(DaemonResponse::Released)
}

async fn release_shared_device(
    pool: &Arc<PoolState>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    dev_id: u32,
    active: ActiveShared,
) -> Result<DaemonResponse> {
    tracing::info!(dev_id, "releasing shared device (refcount reached 0)");
    idle_released_device(pool, ctrl_ring, dev_id, active.dev, active.image).await;
    Ok(DaemonResponse::Released)
}

/// Idle devices must bind daemon-owned placeholders, never business images.
async fn idle_released_device(
    pool: &Arc<PoolState>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    dev_id: u32,
    dev: UVMUblkDev<OverlaybdTarget>,
    business_image: Arc<ImageFile>,
) {
    // Clear page cache before returning to pool.
    clear_page_cache(&dev.device_path());

    let virtual_size = business_image.size_bytes();
    let (placeholder_config, placeholder_image) = match pool.placeholder_for(virtual_size).await {
        Ok(placeholder) => placeholder,
        Err(error) => {
            tracing::warn!(
                dev_id,
                error = %error,
                "failed to build pool placeholder; deleting device instead of idling business image"
            );
            drop(business_image);
            stop_overlaybd_device(ctrl_ring, dev).await;
            return;
        }
    };

    let discard = !placeholder_image.is_read_only().await;
    // Same-size swap: target dev_sectors == kernel device size, so no resize is
    // needed (works even without UBLK_F_UPDATE_SIZE).
    if let Err(error) = dev.target().swap_state(
        placeholder_config.clone(),
        Arc::clone(&placeholder_image),
        discard,
    ) {
        tracing::warn!(
            dev_id,
            error = %error,
            "failed to swap device to pool placeholder; deleting device instead of idling business image"
        );
        drop(business_image);
        stop_overlaybd_device(ctrl_ring, dev).await;
        return;
    }

    drop(business_image);
    let returned_to_pool =
        return_or_stop_idle_device(pool, ctrl_ring.clone(), dev, Arc::clone(&placeholder_image))
            .await;
    if !returned_to_pool {
        return;
    }
    if pool.config.startup_prewarm {
        schedule_idle_pool_refill(Arc::clone(pool), ctrl_ring, virtual_size);
    }
    tracing::info!(dev_id, "device returned to idle pool on placeholder");
}

async fn handle_update_size(
    pool_state: &Option<Arc<PoolState>>,
    dev_id: u32,
    new_sectors: u64,
) -> Result<DaemonResponse> {
    let pool = pool_state
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("warm pool not enabled"))?;

    if !pool.supports_update_size() {
        bail!("UBLK_F_UPDATE_SIZE not supported by kernel");
    }

    // Find the device in active exclusive or shared.
    if let Some(entry) = pool.active_exclusive.get(&dev_id) {
        let update = update_device_size(&entry.dev, new_sectors);
        drop(entry);
        update
            .await
            .with_context(|| format!("update size for exclusive dev_id={dev_id}"))?;
        tracing::info!(dev_id, new_sectors, "updated exclusive device size");
        return Ok(DaemonResponse::SizeUpdated);
    }

    if let Some(shared_key) = pool.shared_by_dev_id.get(&dev_id) {
        let key = shared_key.clone();
        drop(shared_key);
        if let Some(entry) = pool.active_shared.get(&key) {
            let update = update_device_size(&entry.dev, new_sectors);
            drop(entry);
            update
                .await
                .with_context(|| format!("update size for shared dev_id={dev_id}"))?;
            tracing::info!(dev_id, new_sectors, "updated shared device size");
            return Ok(DaemonResponse::SizeUpdated);
        }
    }

    bail!("device {dev_id} not found in active pool");
}

// ── Helper functions ────────────────────────────────────────────────────────

fn take_idle_device(pool: &PoolState, dev_sectors: u64) -> Option<PooledDevice> {
    if pool.supports_update_size() {
        return pool.idle.try_acquire();
    }

    pool.idle
        .try_acquire_where(|pooled| pooled.dev_sectors == dev_sectors)
}

async fn return_or_stop_idle_device(
    pool: &PoolState,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    dev: UVMUblkDev<OverlaybdTarget>,
    placeholder_image: Arc<ImageFile>,
) -> bool {
    let pooled = PooledDevice {
        dev,
        dev_sectors: placeholder_image.num_lbas(),
        _placeholder_image: placeholder_image,
    };

    if let Err(pooled) = pool.idle.try_push_bounded(pooled) {
        stop_excess_idle_device(pool, ctrl_ring, pooled).await;
        return false;
    }
    true
}

fn schedule_idle_pool_refill(
    pool: Arc<PoolState>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    virtual_size: u64,
) {
    if pool
        .refill_inflight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }

    tokio::spawn(async move {
        refill_idle_pool_best_effort(&pool, ctrl_ring.clone(), virtual_size).await;
        pool.refill_inflight.store(false, Ordering::Release);

        if pool.prewarm_refill_count() > 0 {
            schedule_idle_pool_refill(pool, ctrl_ring, virtual_size);
        }
    });
}

async fn refill_idle_pool(
    pool: &PoolState,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    virtual_size: u64,
) -> Result<()> {
    let to_create = pool.prewarm_refill_count();
    if to_create == 0 {
        return Ok(());
    }

    let (placeholder_config, placeholder_image) = pool
        .placeholder_for(virtual_size)
        .await
        .context("build pool placeholder for prewarm")?;

    for _ in 0..to_create {
        if pool.idle.len() >= pool.prewarm_high_watermark {
            break;
        }

        let dev = create_new_device(ctrl_ring.clone(), &placeholder_config, &placeholder_image)
            .await
            .context("prewarm overlaybd pool device")?;
        let pooled = PooledDevice {
            dev,
            dev_sectors: placeholder_image.num_lbas(),
            _placeholder_image: Arc::clone(&placeholder_image),
        };
        if pool.idle.len() >= pool.prewarm_high_watermark {
            stop_excess_idle_device(pool, ctrl_ring.clone(), pooled).await;
            break;
        }

        if let Err(pooled) = pool.idle.try_push_bounded(pooled) {
            stop_excess_idle_device(pool, ctrl_ring.clone(), pooled).await;
            break;
        }
    }

    Ok(())
}

async fn refill_idle_pool_best_effort(
    pool: &PoolState,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    virtual_size: u64,
) {
    if let Err(err) = refill_idle_pool(pool, ctrl_ring, virtual_size).await {
        tracing::warn!(
            error = %err,
            virtual_size,
            "failed to refill idle overlaybd pool"
        );
    }
}

async fn stop_excess_idle_device(
    pool: &PoolState,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    pooled: PooledDevice,
) {
    let dev_id = pooled.dev.dev_id();
    tracing::info!(
        dev_id,
        high_watermark = pool.idle.config().high_watermark,
        "idle overlaybd pool is full; stopping returned device"
    );
    stop_overlaybd_device(ctrl_ring, pooled.dev).await;
}

/// Detect ublk features by sending GET_FEATURES through the ublk control ring.
async fn detect_ublk_features(ctrl_ring: &IoRingHandle<io_uring::squeue::Entry128>) -> Result<u64> {
    // GET_FEATURES does not create a device, so this fixed dev_id is only a
    // control-command field. Create paths omit dev_id to request kernel assignment.
    let ctrl = UVMUblkCtrlBuilder::new()
        .dev_id(0)
        .build(ctrl_ring.clone())
        .context("build ctrl for feature detection")?;

    match ctrl.get_features().await {
        Ok(features) => Ok(features),
        Err(err)
            if matches!(
                err.root_cause()
                    .downcast_ref::<std::io::Error>()
                    .and_then(|err| err.raw_os_error()),
                Some(libc::ENOTTY | libc::EINVAL | libc::EOPNOTSUPP)
            ) =>
        {
            tracing::warn!(
                error = %err,
                "ublk feature query unsupported; continuing without dynamic resize support"
            );
            Ok(0)
        }
        Err(err) => Err(err).context("get ublk features"),
    }
}

/// Create a new ublk device with the given image and size.
///
/// This is called on pool miss (no warm device available). The device is
/// fully initialized (ADD_DEV + START_DEV) and ready for I/O.
async fn create_new_device(
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_config: &Path,
    image: &Arc<ImageFile>,
) -> Result<UVMUblkDev<OverlaybdTarget>> {
    let discard_supported = !image.is_read_only().await;
    let target = OverlaybdTarget::from_opened_image(
        image_config.to_path_buf(),
        Arc::clone(image),
        discard_supported,
    )
    .context("create overlaybd target")?;

    let ctrl = UVMUblkCtrlBuilder::new()
        .name("overlaybd-blk")
        .build(ctrl_ring.clone())
        .context("build ublk ctrl")?;

    let mut dev = UVMUblkDevBuilder::new(ctrl)
        .set_target(target)
        .build()
        .await
        .context("build ublk dev")?;

    let dev_id = dev.dev_id();
    if let Err(err) = dev.start().await.context("start ublk dev") {
        cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
        return Err(err);
    }
    if let Err(err) = wait_for_ublk_dev(dev_id).context("wait for ublk device") {
        cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
        return Err(err);
    }

    tracing::debug!(dev_id, path = %dev.device_path().display(), "created new ublk device");

    Ok(dev)
}

/// Update the virtual size of a ublk device.
fn update_device_size(
    dev: &UVMUblkDev<OverlaybdTarget>,
    new_sectors: u64,
) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
    dev.ctrl.update_size(new_sectors)
}

/// Best-effort page cache clear for a block device using BLKFLSBUF ioctl.
fn clear_page_cache(device_path: &Path) {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    // BLKFLSBUF ioctl number: flush buffer cache
    // Linux asm-generic/ioctl.h encodes _IO(0x12, 97) as (0x12 << 8) | 97.
    // libc models ioctl's request argument differently across Linux libc
    // implementations (c_ulong on glibc, c_int on musl). Keep the request
    // value libc-agnostic and infer the ABI-specific type at the call site.
    const BLKFLSBUF: u32 = 0x1261;

    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(device_path)
    {
        Ok(file) => file,
        Err(err) => {
            tracing::warn!(?err, path = %device_path.display(), "failed to open device for cache clear");
            return;
        }
    };

    let fd = file.as_raw_fd();
    let ret = unsafe { libc::ioctl(fd, BLKFLSBUF as _) };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(?err, path = %device_path.display(), "failed to clear page cache");
    } else {
        tracing::debug!(path = %device_path.display(), "cleared page cache");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Minimal local `ImageService`; placeholder images have no lowers, so this
    /// is never used for registry access.
    async fn test_image_service(dir: &Path) -> ImageService {
        let cache_dir = dir.join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let config_path = dir.join("global_config.json");
        let config = serde_json::json!({
            "registryFsVersion": "v2",
            "ioEngine": 0,
            "cacheConfig": {
                "cacheType": "file",
                "cacheDir": cache_dir.to_str().unwrap(),
                "cacheSizeGB": 1,
                "refillSize": 262144,
                "blockSize": 65536
            }
        });
        std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();
        ImageService::from_config_path(&config_path).await.unwrap()
    }

    fn test_pool_config() -> PoolConfig {
        PoolConfig {
            low_watermark: 0,
            high_watermark: 1,
            maintenance_enabled: false,
            startup_prewarm: false,
        }
    }

    #[tokio::test]
    async fn placeholder_for_builds_cached_same_size_images_per_virtual_size() {
        let dir = TempDir::new().unwrap();
        let image_service = test_image_service(dir.path()).await;
        let placeholder_dir = dir.path().join("placeholders");
        let pool = PoolState::new(
            test_pool_config(),
            1,
            0,
            image_service,
            placeholder_dir.clone(),
        );

        let virtual_size = 64 * 1024 * 1024;
        let (config_path, image) = pool
            .placeholder_for(virtual_size)
            .await
            .expect("build placeholder");

        // Same-size invariant: num_lbas == virtual_size / 512 (logical size, not
        // the on-disk file size which includes the header).
        assert_eq!(image.num_lbas(), virtual_size / 512);
        assert!(config_path.starts_with(&placeholder_dir));
        assert!(config_path.exists());
        assert!(placeholder_dir
            .join(format!("{virtual_size}.data"))
            .exists());

        // No lowers -> an idle placeholder device pins no hard commit, so GC is
        // not blocked by it.
        let written: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
        assert!(written["lowers"].as_array().unwrap().is_empty());

        // Second call returns the cached entry (same image instance).
        let (config_path2, image2) = pool
            .placeholder_for(virtual_size)
            .await
            .expect("cached placeholder");
        assert_eq!(config_path, config_path2);
        assert!(Arc::ptr_eq(&image, &image2));

        let (small_path, small) = pool.placeholder_for(8 * 1024 * 1024).await.expect("small");
        let (large_path, large) = pool.placeholder_for(16 * 1024 * 1024).await.expect("large");

        assert_ne!(small_path, large_path);
        assert_eq!(small.num_lbas(), 8 * 1024 * 1024 / 512);
        assert_eq!(large.num_lbas(), 16 * 1024 * 1024 / 512);
    }

    #[tokio::test]
    async fn prewarm_refill_count_is_capped_below_idle_cache_capacity() {
        let dir = TempDir::new().unwrap();
        let image_service = test_image_service(dir.path()).await;
        let pool = PoolState::new(
            PoolConfig {
                low_watermark: 16,
                high_watermark: 64,
                maintenance_enabled: false,
                startup_prewarm: true,
            },
            8,
            0,
            image_service,
            dir.path().join("placeholders"),
        );

        assert_eq!(pool.prewarm_refill_count(), 8);
        assert_eq!(pool.config.high_watermark, 64);
    }

    #[tokio::test]
    #[ignore = "requires a host with ublk control access"]
    async fn live_prewarm_refill_creates_only_the_block_specific_limit() {
        let dir = TempDir::new().unwrap();
        let image_service = test_image_service(dir.path()).await;
        let pool = PoolState::new(
            PoolConfig {
                low_watermark: 16,
                high_watermark: 64,
                maintenance_enabled: false,
                startup_prewarm: true,
            },
            8,
            0,
            image_service,
            dir.path().join("placeholders"),
        );
        let (ctrl_ring, _ctrl_ring_worker) =
            storage_util::io_ring::spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);

        let refill_result = refill_idle_pool(&pool, ctrl_ring.clone(), 64 * 1024 * 1024).await;
        let idle_len = pool.idle.len();
        for pooled in pool.idle.drain_all() {
            stop_overlaybd_device(ctrl_ring.clone(), pooled.dev).await;
        }

        refill_result.expect("refill live ublk devices");
        assert_eq!(idle_len, 8);
        assert_eq!(pool.config.high_watermark, 64);
    }
}
