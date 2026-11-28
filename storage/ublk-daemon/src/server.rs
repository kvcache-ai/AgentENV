use std::collections::{HashMap, HashSet};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use overlaybd::config::{ImageConfig, UpperConfig, UpperMode};
use overlaybd::helper::prepare_runtime_upper;
use overlaybd::image_file::ImageFile;
use overlaybd::image_service::ImageService;
use overlaybd::virtual_file::VirtualFile;
use overlaybd::LayerDescriptor;
use overlaybd::RestackSnapshotTerminalFailure;
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};
use tokio::task::JoinSet;
use warm_pool::{PoolConfig, PoolMaintenanceAction, WarmPool};

use storage_util::io_ring::IoRingHandle;
use uvm_ublk::{
    delete_dev, tmp_pack_path, ublk_caps, wait_for_ublk_dev, OverlaybdTarget, StartupPackRecorder,
    UVMUblkCtrlBuilder, UVMUblkDev, UVMUblkDevBuilder, UVMUblkTarget,
};

use crate::protocol::{
    recv_message, send_message, AccessMode, DaemonRequest, DaemonResponse, DeviceRole, DeviceSpec,
    FailedRecovery, PackRecordingState, ResizeToolSpec,
};
use crate::runtime;

// ── Managed device wrapper ──────────────────────────────────────────────────

struct ManagedDevice {
    dev: UVMUblkDev<OverlaybdTarget>,
    image: Arc<ImageFile>,
}

/// Tracks one startup pack recording attached to a device.
struct PackRecordingHandle {
    /// Final pack output path (the `memory-startup.pack` artifact).
    output: PathBuf,
    state: Arc<std::sync::Mutex<PackRecordingState>>,
    task: tokio::task::JoinHandle<()>,
    /// Set by abort/delete/shutdown; the packaging pass checks it before the
    /// final rename so a cancelled pack never lands at `output`.
    cancelled: Arc<AtomicBool>,
    /// Held by the window task while packaging runs; abort cleanup acquires
    /// it so file removal never races an in-flight finalize rename.
    finalize_gate: Arc<tokio::sync::Mutex<()>>,
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
    /// Ublk feature flags detected at startup.
    features: u64,
    /// How new devices are created.
    new_device: NewDeviceOptions,
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
            features,
            new_device: NewDeviceOptions::default(),
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

/// Maximum number of devices torn down at the same time during shutdown.
const SHUTDOWN_STOP_CONCURRENCY: usize = 16;

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
    pack_recordings: Arc<DashMap<u32, Arc<PackRecordingHandle>>>,
    /// Keep-alive list for in-flight startup pack prefetch tasks. Entries
    /// are pruned on each new prefetch; a live task is never dropped here,
    /// so a shared memory device's release never cancels a prefetch other
    /// sandboxes may still want.
    startup_pack_handles: Arc<Mutex<Vec<overlaybd::backend::cache::StartupPackHandle>>>,
    /// How new devices are created.
    new_device: NewDeviceOptions,
    /// Set by `Detach`: shutdown leaves devices for the next daemon instead
    /// of deleting them.
    detach: Arc<AtomicBool>,
    recovery: Arc<Recovery>,
    shutdown: Arc<Notify>,
}

/// Settings and state of `Recover`.
struct Recovery {
    /// How many devices are reattached at the same time.
    concurrency: usize,
    /// ublk devices that existed when this daemon started. Orphans of an
    /// earlier daemon are among them; devices this daemon creates, which are
    /// not live until their setup finishes, never are.
    sweep_candidates: Vec<u32>,
    /// The running orphan sweep. Shutdown waits for it, so it cannot leave
    /// orphans.
    sweep: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
        let owner_tag = owner_tag_for(&socket_path);
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
            pack_recordings: Arc::new(DashMap::new()),
            startup_pack_handles: Arc::new(Mutex::new(Vec::new())),
            new_device: NewDeviceOptions {
                user_recovery: false,
                owner_tag,
            },
            detach: Arc::new(AtomicBool::new(false)),
            recovery: Arc::new(Recovery {
                concurrency: default_recover_concurrency(),
                sweep_candidates: list_ublk_devices(),
                sweep: Mutex::new(None),
            }),
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

    /// Create devices with ublk user recovery, so they survive this daemon
    /// exiting and a later daemon can `Recover` them.
    /// Must be called before `enable_pool()` and `run()`.
    pub fn set_user_recovery(&mut self, enabled: bool) {
        self.new_device.user_recovery = enabled;
    }

    /// Set how many devices `Recover` reattaches at the same time.
    /// Must be called before `run()`.
    pub fn set_recover_concurrency(&mut self, concurrency: usize) {
        Arc::get_mut(&mut self.recovery)
            .expect("set_recover_concurrency() must be called before run()")
            .concurrency = concurrency.max(1);
    }

    /// Enable warm pooling with the given configuration.
    /// Must be called before `run()`.
    pub fn enable_pool(&mut self, config: PoolConfig, features: u64) {
        let mut pool = PoolState::new(
            config,
            features,
            self.default_image_service.clone(),
            self.pool_placeholder_dir(),
        );
        pool.new_device = self.new_device;
        self.pool_state = Some(Arc::new(pool));
    }

    /// Daemon-owned directory for warm-pool placeholder images, kept separate
    /// from the business image cache.
    fn pool_placeholder_dir(&self) -> PathBuf {
        self.socket_path
            .parent()
            .map(|parent| parent.join("overlaybd-pool-placeholders"))
            .unwrap_or_else(|| std::env::temp_dir().join("overlaybd-pool-placeholders"))
    }

    /// Per-device upper metrics producer for the daemon `/metrics` endpoint.
    ///
    /// Only live devices are walked, so a device that stops or is deleted
    /// disappears from the scrape output instead of leaving a stale series
    /// behind. Call after `enable_pool()`: the returned closure keeps the pool
    /// state captured at call time.
    pub fn metrics_extra(&self) -> Arc<dyn Fn() -> String + Send + Sync> {
        let devices = Arc::clone(&self.devices);
        let pool_state = self.pool_state.as_ref().map(Arc::clone);
        Arc::new(move || render_device_upper_metrics(&devices, pool_state.as_deref()))
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
                    let pack_recordings = Arc::clone(&self.pack_recordings);
                    let startup_pack_handles = Arc::clone(&self.startup_pack_handles);
                    let new_device = self.new_device;
                    let detach = Arc::clone(&self.detach);
                    let recovery = Arc::clone(&self.recovery);
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
                            pack_recordings,
                            startup_pack_handles,
                            new_device,
                            detach,
                            recovery,
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

        if self.detach.load(Ordering::Acquire) {
            // Leave devices for the next daemon to recover.
            self.detach_all_devices().await;
        } else {
            if let Some(sweep) = self.recovery.sweep.lock().await.take() {
                let _ = sweep.await;
            }
            // Graceful cleanup: stop all devices.
            self.stop_all_devices().await;
        }

        // Remove socket file.
        let _ = std::fs::remove_file(&self.socket_path);
        tracing::info!("ublk daemon stopped");
        Ok(())
    }

    /// Request the daemon to shut down.
    pub fn request_shutdown(&self) {
        self.shutdown.notify_one();
    }

    async fn abort_all_pack_recordings(&self) {
        let recording_ids: Vec<u32> = self.pack_recordings.iter().map(|r| *r.key()).collect();
        for dev_id in recording_ids {
            if let Some((_, handle)) = self.pack_recordings.remove(&dev_id) {
                abort_pack_recording_handle(&handle).await;
            }
        }
    }

    /// Prepare to exit without stopping devices. Pack recordings are aborted
    /// (their window tasks die with the process) and every served image is
    /// flushed. Exiting then closes `/dev/ublkcN`; the kernel quiesces devices
    /// created with user recovery and reissues their in-flight I/O to the
    /// daemon that recovers them. Idle pool devices are left for that
    /// daemon's sweep.
    ///
    /// Callers must not send other requests concurrently with `Detach`;
    /// `UblkDaemonClient` holds its call gate across it.
    async fn detach_all_devices(&self) {
        self.abort_all_pack_recordings().await;

        let mut served: Vec<(u32, Arc<ImageFile>, Arc<OverlaybdTarget>)> = self
            .devices
            .iter()
            .map(|entry| {
                (
                    *entry.key(),
                    Arc::clone(&entry.image),
                    Arc::clone(entry.dev.target()),
                )
            })
            .collect();
        if let Some(pool) = &self.pool_state {
            served.extend(pool.active_exclusive.iter().map(|entry| {
                (
                    *entry.key(),
                    Arc::clone(&entry.image),
                    Arc::clone(entry.dev.target()),
                )
            }));
            served.extend(pool.active_shared.iter().map(|entry| {
                (
                    entry.dev.dev_id(),
                    Arc::clone(&entry.image),
                    Arc::clone(entry.dev.target()),
                )
            }));
        }
        // The next daemon recovers these devices, so fold any failed-write
        // reservation surplus into the baseline records first.
        for (dev_id, image, target) in &served {
            flush_upper_write_baseline(*dev_id, image, target).await;
        }
        for (dev_id, image, _target) in served {
            if let Err(err) = image.sync().await {
                tracing::warn!(dev_id, error = %format!("{err:#}"), "failed to flush image before detach");
            }
        }
        tracing::info!(
            user_recovery = self.new_device.user_recovery,
            "detached ublk devices for recovery by the next daemon"
        );
    }

    async fn stop_all_devices(&self) {
        // Abort pack recordings first so a window task cannot finalize into a
        // device that is being stopped, and no tmp pack files survive.
        self.abort_all_pack_recordings().await;

        // Each STOP_DEV/DEL_DEV waits for block-layer RCU grace periods, so
        // deleting devices one by one makes shutdown time grow linearly with
        // the device count. Overlap the waits with bounded concurrency.
        let permits = Arc::new(Semaphore::new(SHUTDOWN_STOP_CONCURRENCY));
        let mut stops = JoinSet::new();

        let dev_ids: Vec<u32> = self.devices.iter().map(|r| *r.key()).collect();
        for dev_id in dev_ids {
            if let Some((_, mut device)) = self.devices.remove(&dev_id) {
                let ctrl_ring = self.ctrl_ring.clone();
                let permits = Arc::clone(&permits);
                stops.spawn(async move {
                    let _permit = permits.acquire_owned().await;
                    tracing::info!(dev_id, "stopping device during shutdown");
                    quiesce_managed_device(&mut device).await;
                    // ManagedDevice holds an open fd to the ublk char dev.
                    // Must drop it before delete_dev, or the DEL_DEV ioctl will block.
                    drop(device);
                    if let Err(err) = delete_dev(ctrl_ring, dev_id).await {
                        tracing::warn!(dev_id, ?err, "failed to stop device during shutdown");
                    }
                });
            }
        }

        if let Some(pool) = &self.pool_state {
            let mut pooled_devs = Vec::new();

            let exclusive_ids: Vec<u32> = pool
                .active_exclusive
                .iter()
                .map(|entry| *entry.key())
                .collect();
            for dev_id in exclusive_ids {
                if let Some((_, active)) = pool.active_exclusive.remove(&dev_id) {
                    pooled_devs.push(active.dev);
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
                    pooled_devs.push(active.dev);
                }
            }

            pooled_devs.extend(pool.idle.drain_all().into_iter().map(|pooled| pooled.dev));

            for dev in pooled_devs {
                let ctrl_ring = self.ctrl_ring.clone();
                let permits = Arc::clone(&permits);
                stops.spawn(async move {
                    let _permit = permits.acquire_owned().await;
                    stop_overlaybd_device(ctrl_ring, dev).await;
                });
            }
        }

        while let Some(result) = stops.join_next().await {
            if let Err(err) = result {
                tracing::warn!(?err, "device stop task failed during shutdown");
            }
        }
    }
}

/// Render the `agentenv_ublk_overlaybd_upper_*` gauges for every live writable
/// device: the logical bytes written into its upper since it was created or
/// acquired, and the configured upper write bound when one is set.
fn render_device_upper_metrics(
    devices: &DashMap<u32, ManagedDevice>,
    pool_state: Option<&PoolState>,
) -> String {
    let mut live: Vec<(u32, u64, Option<u64>)> = Vec::new();
    let mut collect = |dev_id: u32, dev: &UVMUblkDev<OverlaybdTarget>| {
        if let Some((reserved, limit)) = dev.target().upper_write_bound() {
            live.push((dev_id, reserved, limit));
        }
    };
    for entry in devices.iter() {
        collect(*entry.key(), &entry.value().dev);
    }
    if let Some(pool) = pool_state {
        for entry in pool.active_exclusive.iter() {
            collect(*entry.key(), &entry.value().dev);
        }
        // Shared devices are keyed by image, not by dev id. Copy the reverse
        // index out first: `acquire_shared` inserts into `shared_by_dev_id`
        // while holding an `active_shared` entry guard, so looking up
        // `active_shared` under a `shared_by_dev_id` shard guard would invert
        // that order and can deadlock a concurrent shared acquire.
        let shared_keys: Vec<(u32, SharedKey)> = pool
            .shared_by_dev_id
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        for (dev_id, key) in shared_keys {
            if let Some(shared) = pool.active_shared.get(&key) {
                collect(dev_id, &shared.dev);
            }
        }
    }
    if live.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    out.push_str(
        "# HELP agentenv_ublk_overlaybd_upper_bytes Logical bytes written into \
         a live device's writable overlaybd upper since it was created or acquired.\n",
    );
    out.push_str("# TYPE agentenv_ublk_overlaybd_upper_bytes gauge\n");
    for (dev_id, reserved, _) in &live {
        out.push_str(&format!(
            "agentenv_ublk_overlaybd_upper_bytes{{dev_id=\"{dev_id}\"}} {reserved}\n"
        ));
    }
    if live.iter().any(|(_, _, limit)| limit.is_some()) {
        out.push_str(
            "# HELP agentenv_ublk_overlaybd_upper_limit_bytes Configured upper \
             write bound of a live device.\n",
        );
        out.push_str("# TYPE agentenv_ublk_overlaybd_upper_limit_bytes gauge\n");
        for (dev_id, _, limit) in &live {
            let Some(limit) = limit else { continue };
            out.push_str(&format!(
                "agentenv_ublk_overlaybd_upper_limit_bytes{{dev_id=\"{dev_id}\"}} {limit}\n"
            ));
        }
    }
    out
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
    pack_recordings: Arc<DashMap<u32, Arc<PackRecordingHandle>>>,
    startup_pack_handles: Arc<Mutex<Vec<overlaybd::backend::cache::StartupPackHandle>>>,
    new_device: NewDeviceOptions,
    detach: Arc<AtomicBool>,
    recovery: Arc<Recovery>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let Some(request) = recv_message::<DaemonRequest>(&mut stream).await? else {
        return Ok(());
    };
    let rollback_ring = ctrl_ring.clone();

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
                new_device,
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
            prior_upper_write_consumed,
            upper_write_limit,
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
                prior_upper_write_consumed,
                upper_write_limit,
            };
            handle_create_overlaybd_runtime_device(
                request,
                ctrl_ring,
                &devices,
                &image_service_cache,
                &pool_state,
                new_device,
            )
            .await
        }
        DaemonRequest::Delete { dev_id } => {
            handle_delete(&devices, ctrl_ring, &pack_recordings, dev_id).await
        }
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
        DaemonRequest::StartPackRecording {
            dev_id,
            output,
            max_pages,
            min_window_ms,
            quiet_ms,
            max_window_ms,
        } => {
            handle_start_pack_recording(
                &devices,
                &pack_recordings,
                dev_id,
                PackRecordingParams {
                    output,
                    max_pages,
                    min_window: Duration::from_millis(min_window_ms),
                    quiet: Duration::from_millis(quiet_ms),
                    max_window: Duration::from_millis(max_window_ms),
                },
            )
            .await
        }
        DaemonRequest::PackRecordingStatus { dev_id } => {
            handle_pack_recording_status(&pack_recordings, dev_id)
        }
        DaemonRequest::AbortPackRecording { dev_id } => {
            handle_abort_pack_recording(&devices, &pack_recordings, dev_id).await
        }
        DaemonRequest::PrefetchStartupPack {
            image_config,
            global_config,
            url,
            pack_size,
            index_sha256,
            mem_virtual_size,
            timeout_secs,
        } => {
            handle_prefetch_startup_pack(
                &image_service_cache,
                &startup_pack_handles,
                image_config,
                global_config,
                overlaybd::image_service::StartupPackPrefetch {
                    url,
                    pack_size,
                    index_sha256,
                    mem_virtual_size,
                    timeout: Duration::from_secs(timeout_secs),
                },
            )
            .await
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
                AcquireOptions {
                    virtual_size,
                    // Plain acquires do not carry a write bound; the
                    // runtime-device path passes the per-sandbox limit.
                    upper_write: UpperWriteBudget::default(),
                },
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
        DaemonRequest::Detach => {
            detach.store(true, Ordering::Release);
            shutdown.notify_one();
            Ok(DaemonResponse::Ok)
        }
        DaemonRequest::Recover { devices: specs } => {
            handle_recover(
                Arc::clone(&devices),
                pool_state.clone(),
                ctrl_ring,
                Arc::clone(&image_service_cache),
                specs,
                new_device.owner_tag,
                &recovery,
            )
            .await
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

    let Some(dev_id) = handed_off_dev_id(&response) else {
        send_message(&mut stream, &response).await?;
        return Ok(());
    };
    if deliver_device_handoff(&mut stream, &response, dev_id).await {
        return Ok(());
    }

    // The response was the client's only record of the device, so nobody else
    // will ever release it. Undo the handoff the way the client would have:
    // raw devices are deleted, pooled devices return one acquisition (one
    // shared reference) to the pool.
    let rollback = match response {
        DaemonResponse::DeviceAcquired { .. }
        | DaemonResponse::OverlaybdRuntimeDeviceCreated { .. }
            if pool_state.is_some() =>
        {
            handle_release_overlaybd(&pool_state, rollback_ring, dev_id).await
        }
        _ => handle_delete(&devices, rollback_ring, &pack_recordings, dev_id).await,
    };
    rollback
        .map(|_| tracing::info!(dev_id, "rolled back undelivered device handoff"))
        .with_context(|| format!("roll back undelivered handoff of device {dev_id}"))
}

/// Returns the device a response hands over to the client, if any.
fn handed_off_dev_id(response: &DaemonResponse) -> Option<u32> {
    match response {
        DaemonResponse::DeviceCreated { dev_id, .. }
        | DaemonResponse::OverlaybdRuntimeDeviceCreated { dev_id, .. }
        | DaemonResponse::DeviceAcquired { dev_id, .. } => Some(*dev_id),
        _ => None,
    }
}

/// Sends a device handoff and returns `false` only when the client certainly
/// did not receive it.
///
/// A client reads one response and then closes the connection. On Linux,
/// closing a Unix stream socket with unread data makes the peer's next read
/// fail with `ECONNRESET`, so a reset means the client gave up (for example,
/// its RPC timed out) after the response was written but before reading it.
/// Any other outcome is treated as delivered: releasing a device the client
/// still uses is worse than leaking it.
async fn deliver_device_handoff(
    stream: &mut tokio::net::UnixStream,
    response: &DaemonResponse,
    dev_id: u32,
) -> bool {
    if let Err(err) = send_message(stream, response).await {
        tracing::warn!(dev_id, error = %format!("{err:#}"), "failed to send device handoff");
        return false;
    }
    match stream.read(&mut [0u8; 1]).await {
        Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {
            tracing::warn!(dev_id, "client closed without reading device handoff");
            false
        }
        _ => true,
    }
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
    prior_upper_write_consumed: Option<u64>,
    upper_write_limit: Option<u64>,
}

async fn handle_create_overlaybd_runtime_device(
    request: OverlaybdRuntimeDeviceRequest<'_>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    devices: &DashMap<u32, ManagedDevice>,
    image_service_cache: &ImageServiceCache,
    pool_state: &Option<Arc<PoolState>>,
    new_device: NewDeviceOptions,
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

    let upper_write = UpperWriteBudget {
        limit: request.upper_write_limit,
        // The snapshot's consumed budget applies to the fresh upper this
        // attach creates: the writes it covers were sealed into lowers and
        // are no longer part of the upper.
        prior_consumed: if runtime.fresh_upper {
            request.prior_upper_write_consumed
        } else {
            None
        },
        fresh_upper: runtime.fresh_upper,
    };
    let created = if pool_state.is_some() {
        match handle_acquire_overlaybd(
            pool_state,
            ctrl_ring,
            image_service_cache,
            &runtime.runtime_image_config_path,
            request.global_config,
            AcquireOptions {
                virtual_size: Some(runtime.actual_virtual_size),
                upper_write,
            },
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
            upper_write,
            new_device,
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
    new_device: NewDeviceOptions,
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
        // Raw devices serve read-only images (shared memory snapshots); no
        // upper write bound is requested for them.
        UpperWriteBudget::default(),
        new_device,
    )
    .await?;

    Ok(DaemonResponse::DeviceCreated {
        dev_id,
        device_path,
    })
}

#[allow(clippy::too_many_arguments)]
async fn create_overlaybd_device(
    devices: &DashMap<u32, ManagedDevice>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
    upper_write: UpperWriteBudget,
    new_device: NewDeviceOptions,
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
    // Resolve the restored budget before any kernel device exists: a failure
    // here must not leave an unregistered device behind.
    let consumed_upper_bytes = if discard_supported {
        let consumed = consumed_upper_write_bytes(&image, image_config, upper_write).await?;
        if upper_write.limit.is_some() {
            // Record the baseline the attached upper starts from. Without it
            // a daemon restart after a snapshot resume would restore only
            // what the upper itself holds, dropping the resumed total until
            // the next restack.
            persist_upper_write_baseline(&image, consumed)
                .await
                .with_context(|| {
                    format!(
                        "persist the upper write baseline of {}",
                        image_config.display()
                    )
                })?;
        }
        consumed
    } else {
        0
    };

    let target = OverlaybdTarget::from_opened_image(
        image_config_path,
        Arc::clone(&image),
        discard_supported,
    )
    .context("create overlaybd target")?;

    let ctrl = new_device
        .ctrl_builder()
        .build(ctrl_ring.clone())
        .context("build ublk ctrl")?;

    let mut dev = UVMUblkDevBuilder::new(ctrl)
        .set_target(target)
        .build()
        .await
        .context("build ublk dev")?;

    // Writable uppers only: a read-only image has no upper to bound.
    if discard_supported {
        dev.target()
            .arm_upper_write_bound(upper_write.limit, consumed_upper_bytes);
    }

    let dev_id = dev.dev_id();
    if let Err(err) = dev.start().await.context("start ublk dev") {
        cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
        return Err(err);
    }

    if let Err(err) = wait_for_ublk_dev_readable(dev_id).await {
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
    pack_recordings: &DashMap<u32, Arc<PackRecordingHandle>>,
    dev_id: u32,
) -> Result<DaemonResponse> {
    // Abort any pack recording first: its window task must not finalize into
    // a deleted device's image, and its pack files must not leak.
    if let Some((_, handle)) = pack_recordings.remove(&dev_id) {
        abort_pack_recording_handle(&handle).await;
    }

    let Some((_, mut device)) = devices.remove(&dev_id) else {
        bail!("device {dev_id} not found");
    };

    quiesce_managed_device(&mut device).await;
    // In-flight writes are drained now, so the reservation and the upper's
    // size are sampled atomically with one another: fold any failed-write
    // reservation surplus into the baseline record before the device goes.
    flush_upper_write_baseline(dev_id, &device.image, device.dev.target()).await;
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

// ── Startup pack recording ──────────────────────────────────────────────────

const PACK_RECORDING_TICK: Duration = Duration::from_millis(100);

/// Window/guard/output parameters for one pack recording (kept as a struct
/// so handler and window task stay within argument-count lints).
struct PackRecordingParams {
    output: PathBuf,
    max_pages: u32,
    min_window: Duration,
    quiet: Duration,
    max_window: Duration,
}

async fn handle_start_pack_recording(
    devices: &DashMap<u32, ManagedDevice>,
    pack_recordings: &DashMap<u32, Arc<PackRecordingHandle>>,
    dev_id: u32,
    params: PackRecordingParams,
) -> Result<DaemonResponse> {
    // Replace a finished handle, but never interrupt a live recording.
    if let Some(existing) = pack_recordings.get(&dev_id) {
        let finished = !matches!(
            *existing.state.lock().expect("recording state poisoned"),
            PackRecordingState::Recording
        );
        if !finished {
            return Ok(DaemonResponse::InvalidRequest {
                message: format!("pack recording already active on device {dev_id}"),
            });
        }
        drop(existing);
        pack_recordings.remove(&dev_id);
    }

    if params.max_pages == 0 {
        return Ok(DaemonResponse::InvalidRequest {
            message: "max_pages must be > 0".to_string(),
        });
    }
    if params.quiet.is_zero() {
        return Ok(DaemonResponse::InvalidRequest {
            message: "quiet_ms must be > 0".to_string(),
        });
    }
    if params.min_window > params.max_window {
        return Ok(DaemonResponse::InvalidRequest {
            message: format!(
                "min_window_ms {} exceeds max_window_ms {}",
                params.min_window.as_millis(),
                params.max_window.as_millis()
            ),
        });
    }
    let Some(parent) = params.output.parent() else {
        return Ok(DaemonResponse::InvalidRequest {
            message: format!("pack output {} has no parent dir", params.output.display()),
        });
    };
    if !parent.exists() {
        return Ok(DaemonResponse::InvalidRequest {
            message: format!("pack output dir {} does not exist", parent.display()),
        });
    }

    let Some(device) = devices.get(&dev_id) else {
        return Ok(DaemonResponse::InvalidRequest {
            message: format!("device {dev_id} not found"),
        });
    };
    let (target, image) = (Arc::clone(device.dev.target()), Arc::clone(&device.image));
    drop(device);

    let mem_virtual_size = image.size_bytes();
    let max_pages =
        (params.max_pages as usize).min(overlaybd::startup_pack::MAX_PACK_PAGES as usize);
    let recorder = Arc::new(StartupPackRecorder::new(mem_virtual_size, max_pages));
    target.set_recorder(Some(Arc::clone(&recorder)));
    let output = params.output.clone();
    tracing::info!(
        dev_id,
        output = %output.display(),
        mem_virtual_size,
        max_pages,
        "startup pack recording armed"
    );

    let state = Arc::new(std::sync::Mutex::new(PackRecordingState::Recording));
    let cancelled = Arc::new(AtomicBool::new(false));
    let finalize_gate = Arc::new(tokio::sync::Mutex::new(()));
    let task = tokio::spawn(pack_recording_window(
        Arc::clone(&recorder),
        target,
        params,
        Arc::clone(&state),
        Arc::clone(&cancelled),
        Arc::clone(&finalize_gate),
    ));
    pack_recordings.insert(
        dev_id,
        Arc::new(PackRecordingHandle {
            output,
            state,
            task,
            cancelled,
            finalize_gate,
        }),
    );
    Ok(DaemonResponse::Ok)
}

fn handle_pack_recording_status(
    pack_recordings: &DashMap<u32, Arc<PackRecordingHandle>>,
    dev_id: u32,
) -> Result<DaemonResponse> {
    let Some(handle) = pack_recordings.get(&dev_id) else {
        return Ok(DaemonResponse::InvalidRequest {
            message: format!("no pack recording on device {dev_id}"),
        });
    };
    let state = handle
        .state
        .lock()
        .expect("recording state poisoned")
        .clone();
    Ok(DaemonResponse::PackRecording { state })
}

/// Best-effort startup pack prefetch registration. Every failure mode
/// (missing image service, no bindable layers, shut-down scheduler) is
/// logged and answered with `Ok`: resume never depends on the prefetch.
async fn handle_prefetch_startup_pack(
    image_service_cache: &Arc<ImageServiceCache>,
    startup_pack_handles: &Arc<Mutex<Vec<overlaybd::backend::cache::StartupPackHandle>>>,
    image_config: PathBuf,
    global_config: PathBuf,
    pack: overlaybd::image_service::StartupPackPrefetch,
) -> Result<DaemonResponse> {
    let image_service = match image_service_cache.get_or_create(&global_config).await {
        Ok(service) => service,
        Err(error) => {
            tracing::warn!(%error, "startup pack prefetch: image service unavailable");
            return Ok(DaemonResponse::Ok);
        }
    };
    match image_service
        .prefetch_startup_pack(&image_config, pack)
        .await
    {
        Ok(Some(handle)) => {
            use overlaybd::backend::cache::StartupPackPhase;
            let mut handles = startup_pack_handles.lock().await;
            handles.retain(|existing| {
                !matches!(
                    existing.phase(),
                    StartupPackPhase::Done | StartupPackPhase::Failed | StartupPackPhase::Canceled
                )
            });
            handles.push(handle);
            tracing::info!(
                image_config = %image_config.display(),
                "startup pack prefetch registered"
            );
        }
        Ok(None) => {
            tracing::debug!(
                image_config = %image_config.display(),
                "startup pack prefetch skipped: no bindable layers or cache"
            );
        }
        Err(error) => {
            tracing::warn!(%error, "startup pack prefetch registration failed");
        }
    }
    Ok(DaemonResponse::Ok)
}

/// Cancel one recording and remove its pack files deterministically: the
/// packaging pass checks `cancelled` before the final rename, and the
/// finalize gate makes sure an in-flight rename has settled before any file
/// removal runs — so a cancelled pack never survives cleanup, while a pack
/// that completed before the cancel is preserved.
async fn abort_pack_recording_handle(handle: &PackRecordingHandle) {
    handle.cancelled.store(true, Ordering::Relaxed);
    handle.task.abort();
    let _gate = handle.finalize_gate.lock().await;
    let done = matches!(
        *handle.state.lock().expect("recording state poisoned"),
        PackRecordingState::Done { .. }
    );
    let _ = std::fs::remove_file(tmp_pack_path(&handle.output));
    if !done {
        let _ = std::fs::remove_file(&handle.output);
    }
}

async fn handle_abort_pack_recording(
    devices: &DashMap<u32, ManagedDevice>,
    pack_recordings: &DashMap<u32, Arc<PackRecordingHandle>>,
    dev_id: u32,
) -> Result<DaemonResponse> {
    let Some((_, handle)) = pack_recordings.remove(&dev_id) else {
        return Ok(DaemonResponse::Ok);
    };
    if let Some(device) = devices.get(&dev_id) {
        device.dev.target().set_recorder(None);
    }
    abort_pack_recording_handle(&handle).await;
    Ok(DaemonResponse::Ok)
}

async fn pack_recording_window(
    recorder: Arc<StartupPackRecorder>,
    target: Arc<OverlaybdTarget>,
    params: PackRecordingParams,
    state: Arc<std::sync::Mutex<PackRecordingState>>,
    cancelled: Arc<AtomicBool>,
    finalize_gate: Arc<tokio::sync::Mutex<()>>,
) {
    use uvm_ublk::RecordingVerdict;

    let mut ticker = tokio::time::interval(PACK_RECORDING_TICK);
    let verdict = loop {
        ticker.tick().await;
        let verdict = recorder.verdict(params.min_window, params.quiet, params.max_window);
        if verdict != RecordingVerdict::Continue {
            break verdict;
        }
    };

    // Recording is over: detach first so no new read is observed.
    target.set_recorder(None);
    let output = params.output.clone();

    // Finalizing writes the trace file synchronously; move it off the
    // daemon's async executor (which also serves ublk device control).
    let package = |params: &PackRecordingParams| {
        let recorder = Arc::clone(&recorder);
        let output = params.output.clone();
        let cancelled = Arc::clone(&cancelled);
        tokio::task::spawn_blocking(move || recorder.finalize(&output, &cancelled))
    };

    let state_value = match verdict {
        RecordingVerdict::Fail(reason) => {
            tracing::warn!(reason, "startup pack recording aborted by guard");
            PackRecordingState::Failed {
                reason: reason.to_string(),
            }
        }
        RecordingVerdict::Finish => {
            // Hold the gate across packaging so abort/delete/shutdown cleanup
            // never removes files from under an in-flight finalize rename.
            let _gate = finalize_gate.lock().await;
            match package(&params).await {
                Ok(Ok(outcome)) => {
                    tracing::info!(
                        output = %output.display(),
                        pages = outcome.pages,
                        bytes = outcome.bytes,
                        remote_bytes = outcome.remote_bytes,
                        skipped_high_pages = outcome.skipped_high_pages,
                        "startup pack recorded"
                    );
                    PackRecordingState::Done {
                        pages: outcome.pages,
                        bytes: outcome.bytes,
                        remote_bytes: outcome.remote_bytes,
                        path: output,
                    }
                }
                Ok(Err(error)) => {
                    tracing::warn!(?error, "startup pack packaging failed");
                    let _ = std::fs::remove_file(tmp_pack_path(&params.output));
                    PackRecordingState::Failed {
                        reason: format!("{error:#}"),
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, "startup pack packaging task failed");
                    let _ = std::fs::remove_file(tmp_pack_path(&params.output));
                    PackRecordingState::Failed {
                        reason: format!("packaging task join failed: {error:#}"),
                    }
                }
            }
        }
        RecordingVerdict::Continue => unreachable!("window loop only exits on a terminal verdict"),
    };

    *state.lock().expect("recording state poisoned") = state_value;
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
    let (image, image_config, target) = if let Some(device_ref) = devices.get(&dev_id) {
        let image = Arc::clone(&device_ref.image);
        let target = Arc::clone(device_ref.dev.target());
        drop(device_ref);
        (image, None, target)
    } else if let Some(pool) = pool_state {
        if let Some(active) = pool.active_exclusive.get(&dev_id) {
            (
                Arc::clone(&active.image),
                Some(active.image_config.clone()),
                Arc::clone(active.dev.target()),
            )
        } else if let Some(shared_key) = pool.shared_by_dev_id.get(&dev_id) {
            let key = shared_key.clone();
            drop(shared_key);
            let active = pool
                .active_shared
                .get(&key)
                .with_context(|| format!("device {dev_id} not found for snapshot"))?;
            (
                Arc::clone(&active.image),
                Some(active.image_config.clone()),
                Arc::clone(active.dev.target()),
            )
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

    // The restack seals the upper into a lower and starts a fresh, empty
    // upper, while the live target keeps its cumulative reservation. The
    // replaced upper's total is recorded as part of the seal window: once a
    // re-attach sees the upper replaced, it restores the record plus what the
    // fresh upper has appended, so the budget neither resets on resume nor
    // diverges from the live accounting. The record only applies after a
    // replacement, so a restack that fails before sealing cannot charge its
    // budget to the surviving upper, and it cannot drop the surviving upper's
    // baseline either -- that lives in its own record.
    let (descriptor, upper_write_consumed) = if let (Some(pool), Some(image_config)) =
        (pool_state.as_ref(), image_config.as_deref())
    {
        let image_lock = pool.image_lock(image_config);
        let _image_guard = image_lock.write().await;
        seal_restack_snapshot_with_upper_write_record(dev_id, &image, &target, output_layer_path)
            .await?
    } else {
        seal_restack_snapshot_with_upper_write_record(dev_id, &image, &target, output_layer_path)
            .await?
    };

    // The seal succeeded, so the image is already mutated: from here on a
    // failure is a terminal restack failure, not an error the caller may
    // retry against the replaced upper.
    if upper_write_consumed.is_some() {
        record_restacked_upper_write_baseline(dev_id, &image, devices, pool_state).await?;
    }

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
        upper_write_consumed,
    })
}

/// Seal the upper into the restack snapshot layer, recording the replaced
/// upper's write budget as part of the same reserved-to-sealed window.
///
/// The replacement record and the returned consumed total must name exactly
/// the bytes the seal captures. A write admitted between the sample and the
/// seal would land in the sealed layer while the record omitted it; after a
/// resume, recovery restores only the record plus the fresh upper, losing
/// that write from the budget and letting the device exceed its limit. The
/// write path holds the target's progress gate from before it reserves until
/// its append has landed, so taking the gate for writing here keeps every
/// write out of the upper from the sample through the seal: writes admitted
/// before the gate closed have landed and are counted, writes blocked at the
/// gate have reserved nothing and are sealed into the fresh upper that
/// follows. Without an armed limit there is no budget to record, so the gate
/// stays out of the way.
async fn seal_restack_snapshot_with_upper_write_record(
    dev_id: u32,
    image: &ImageFile,
    target: &OverlaybdTarget,
    output_layer_path: &Path,
) -> Result<(Option<LayerDescriptor>, Option<u64>)> {
    let seal = || async {
        image
            .create_snapshot_and_restack(output_layer_path)
            .await
            .with_context(|| {
                format!(
                    "restack overlaybd snapshot for dev_id={dev_id} to {}",
                    output_layer_path.display()
                )
            })
    };
    // Only a bounded device has a budget worth persisting with the snapshot.
    if !target
        .upper_write_bound()
        .is_some_and(|(_, limit)| limit.is_some())
    {
        return Ok((seal().await?, None));
    }
    target
        .with_upper_write_progress(async {
            // Re-sample with the window closed: a write that held the gate
            // when this call started has completed its append by now, and no
            // new write can reserve until the seal is done, so this total is
            // exactly what the seal captures.
            let Some((reserved, Some(_limit))) = target.upper_write_bound() else {
                // The bound was disarmed while waiting for the gate.
                return Ok((seal().await?, None));
            };
            let upper_data = image.upper_data_path().await.with_context(|| {
                format!("resolve the writable upper path of dev_id={dev_id} for restack")
            })?;
            let generation = image.upper_generation().await.with_context(|| {
                format!("resolve the writable upper generation of dev_id={dev_id} for restack")
            })?;
            persist_upper_write_record(
                &upper_data,
                UPPER_WRITE_REPLACEMENT_KIND,
                &generation,
                reserved,
            )
            .await
            .with_context(|| {
                format!("record the replaced upper write budget of dev_id={dev_id} before restack")
            })?;
            Ok((seal().await?, Some(reserved)))
        })
        .await
}

// ── Recovery ────────────────────────────────────────────────────────────────

/// How many devices `Recover` reattaches at the same time unless configured:
/// one per CPU. There is no upper bound; the daemon runs every recovered
/// device's queue workers afterwards anyway.
fn default_recover_concurrency() -> usize {
    std::thread::available_parallelism().map_or(1, usize::from)
}

/// How long a recovering device waits for the previous daemon to release it.
/// Before Linux 6.15, a queue whose every request was in flight when its
/// daemon died is only quiesced once a request times out, which takes the
/// block layer's default 30 s, so this must stay longer than that.
const RECOVER_START_TIMEOUT: Duration = Duration::from_secs(60);

/// sysfs class listing every `/dev/ublkcN` on the host.
const UBLK_CHAR_CLASS_DIR: &str = "/sys/class/ublk-char";

/// `UBLK_S_DEV_LIVE` from the kernel's `ublk_cmd.h`: a server is attached.
const UBLK_S_DEV_LIVE: u16 = 1;

async fn handle_recover(
    devices: Arc<DashMap<u32, ManagedDevice>>,
    pool_state: Option<Arc<PoolState>>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: Arc<ImageServiceCache>,
    specs: Vec<DeviceSpec>,
    owner_tag: u64,
    recovery: &Recovery,
) -> Result<DaemonResponse> {
    tracing::info!(
        devices = specs.len(),
        concurrency = recovery.concurrency,
        "recovering ublk devices"
    );
    let keep: HashSet<u32> = specs.iter().map(|spec| spec.dev_id).collect();

    let permits = Arc::new(Semaphore::new(recovery.concurrency));
    let mut recoveries = JoinSet::new();
    for spec in specs {
        let devices = Arc::clone(&devices);
        let pool_state = pool_state.clone();
        let ctrl_ring = ctrl_ring.clone();
        let image_service_cache = Arc::clone(&image_service_cache);
        let permits = Arc::clone(&permits);
        recoveries.spawn(async move {
            let _permit = permits.acquire_owned().await;
            let dev_id = spec.dev_id;
            let result = recover_device(
                &devices,
                pool_state.as_deref(),
                ctrl_ring,
                &image_service_cache,
                spec,
                owner_tag,
            )
            .await;
            (dev_id, result)
        });
    }

    let mut failed = Vec::new();
    while let Some(joined) = recoveries.join_next().await {
        let (dev_id, result) = joined.context("join device recovery task")?;
        match result {
            Ok(()) => tracing::info!(dev_id, "recovered ublk device"),
            Err(err) => {
                let message = format!("{err:#}");
                tracing::error!(dev_id, error = %message, "failed to recover ublk device");
                failed.push(FailedRecovery { dev_id, message });
            }
        }
    }

    // Sweeping only deletes devices nobody uses, so it does not hold up the
    // caller, which is waiting to resume RPCs.
    let candidates = recovery
        .sweep_candidates
        .iter()
        .copied()
        .filter(|dev_id| !keep.contains(dev_id))
        .collect();
    let mut sweep = recovery.sweep.lock().await;
    if let Some(previous) = sweep.take() {
        // Keep a single sweep, so shutdown can wait for it.
        let _ = previous.await;
    }
    *sweep = Some(tokio::spawn(sweep_orphaned_devices(
        ctrl_ring, owner_tag, candidates,
    )));

    Ok(DaemonResponse::Recovered { failed })
}

/// Reattach one device and register it where the daemon that created it
/// tracked it. On failure a device of this instance is deleted, so its
/// queued I/O fails instead of waiting for a server that never comes. The
/// deletion runs in the background: DEL_DEV waits until every opener, such
/// as a guest, closes the device, which must not hold up the recovery.
async fn recover_device(
    devices: &DashMap<u32, ManagedDevice>,
    pool_state: Option<&PoolState>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: &ImageServiceCache,
    spec: DeviceSpec,
    owner_tag: u64,
) -> Result<()> {
    let dev_id = spec.dev_id;
    // A stale spec may name an id the kernel gave to another instance's
    // device; fail it without touching that device.
    ensure_owned(&ctrl_ring, dev_id, owner_tag).await?;
    let opened = if matches!(spec.role, DeviceRole::Shared { .. }) && pool_state.is_none() {
        Err(anyhow::anyhow!(
            "shared device {dev_id} needs the warm pool, which is disabled"
        ))
    } else {
        open_recovered_device(&ctrl_ring, image_service_cache, &spec).await
    };
    let (mut dev, image) = match opened {
        Ok(opened) => opened,
        Err(err) => {
            tokio::spawn(async move {
                if let Err(delete_err) = delete_dev(ctrl_ring, dev_id).await {
                    tracing::warn!(
                        dev_id,
                        error = %format!("{delete_err:#}"),
                        "failed to delete unrecoverable ublk device"
                    );
                }
            });
            return Err(err);
        }
    };
    if let Err(err) = dev.resume().await {
        tokio::spawn(cleanup_failed_ublk_start(ctrl_ring, dev));
        return Err(err);
    }

    let DeviceSpec {
        image_config,
        global_config,
        role,
        ..
    } = spec;
    match (role, pool_state) {
        (DeviceRole::Exclusive, Some(pool)) => {
            pool.active_exclusive.insert(
                dev_id,
                ActiveExclusive {
                    dev,
                    image_config,
                    image,
                },
            );
        }
        (DeviceRole::Shared { refcount }, Some(pool)) => {
            let key = (image_config.clone(), global_config);
            pool.shared_by_dev_id.insert(dev_id, key.clone());
            pool.active_shared.insert(
                key,
                ActiveShared {
                    dev,
                    image_config,
                    image,
                    refcount,
                },
            );
        }
        // Without a pool, runtime devices live in `devices` too; shared specs
        // were rejected above.
        (DeviceRole::Raw | DeviceRole::Exclusive | DeviceRole::Shared { .. }, _) => {
            devices.insert(dev_id, ManagedDevice { dev, image });
        }
    }
    Ok(())
}

/// Wait for `/dev/ublkbN` to become readable. The wait polls with blocking
/// sleeps for up to 30 s, so keep it off the runtime's few worker threads.
async fn wait_for_ublk_dev_readable(dev_id: u32) -> Result<()> {
    tokio::task::spawn_blocking(move || wait_for_ublk_dev(dev_id))
        .await
        .context("join wait for ublk device")?
        .context("wait for ublk device")
}

/// Fail unless `dev_id` exists and a daemon of this instance created it.
async fn ensure_owned(
    ctrl_ring: &IoRingHandle<io_uring::squeue::Entry128>,
    dev_id: u32,
    owner_tag: u64,
) -> Result<()> {
    let mut ctrl = UVMUblkCtrlBuilder::new()
        .dev_id(dev_id)
        .build(ctrl_ring.clone())
        .context("build ublk ctrl")?;
    ctrl.get_dev_info().await.context("get ublk device info")?;
    if ctrl.dev_info.ublksrv_flags != owner_tag {
        bail!("ublk device {dev_id} belongs to another daemon instance");
    }
    Ok(())
}

/// Open the image a device served and attach to the device with user
/// recovery. The device does not serve I/O until `resume()`.
async fn open_recovered_device(
    ctrl_ring: &IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: &ImageServiceCache,
    spec: &DeviceSpec,
) -> Result<(UVMUblkDev<OverlaybdTarget>, Arc<ImageFile>)> {
    let image_service = image_service_cache
        .get_or_create(&spec.global_config)
        .await
        .context("resolve image service for recovered device")?;
    let image = Arc::new(
        image_service
            .create_image_file(&spec.image_config)
            .await
            .with_context(|| format!("open overlaybd image: {}", spec.image_config.display()))?,
    );
    let discard_supported = !image.is_read_only().await;
    let target = OverlaybdTarget::from_opened_image(
        spec.image_config.clone(),
        Arc::clone(&image),
        discard_supported,
    )
    .context("create overlaybd target")?;

    let ctrl = UVMUblkCtrlBuilder::new()
        .name("overlaybd-blk")
        .dev_id(spec.dev_id)
        .build(ctrl_ring.clone())
        .context("build ublk ctrl")?;
    // Re-arm the bound the device was created with: a daemon restart must not
    // silently drop an enforcement limit. The upper kept its data across the
    // restart, so the budget it already consumed has to be restored with it;
    // starting from zero would grant a fresh allowance on every restart.
    // Resolve that budget before attaching: on failure the caller deletes the
    // device, and a bound must not silently lose its cap at a re-attach.
    let consumed_upper_bytes = if discard_supported {
        let consumed = consumed_upper_write_bytes(
            &image,
            &spec.image_config,
            UpperWriteBudget {
                limit: spec.upper_write_limit,
                prior_consumed: None,
                fresh_upper: false,
            },
        )
        .await?;
        if spec.upper_write_limit.is_some() {
            // Re-publish the baseline the attached upper starts from: the
            // records next to the upper carry it across restarts, and a
            // recovered upper must keep naming it for its live generation.
            persist_upper_write_baseline(&image, consumed)
                .await
                .with_context(|| {
                    format!(
                        "persist the upper write baseline of {}",
                        spec.image_config.display()
                    )
                })?;
        }
        consumed
    } else {
        0
    };
    let dev = UVMUblkDevBuilder::new(ctrl)
        .set_target(target)
        .recover(RECOVER_START_TIMEOUT)
        .await
        .context("recover ublk dev")?;
    if discard_supported {
        dev.target()
            .arm_upper_write_bound(spec.upper_write_limit, consumed_upper_bytes);
    }
    Ok((dev, image))
}

/// Ids of the ublk devices that currently exist.
fn list_ublk_devices() -> Vec<u32> {
    let entries = match std::fs::read_dir(UBLK_CHAR_CLASS_DIR) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(err) => {
            tracing::warn!(error = %err, "failed to list ublk devices for orphan sweep");
            return Vec::new();
        }
    };
    entries
        .filter_map(|entry| {
            entry
                .ok()?
                .file_name()
                .to_str()?
                .strip_prefix("ublkc")?
                .parse::<u32>()
                .ok()
        })
        .collect()
}

/// Delete every ublk device among `candidates` that a previous daemon of
/// this instance left behind: idle pool devices, devices whose handoff never
/// reached the client, and devices created without user recovery.
///
/// Devices are matched by `owner_tag`, never by pid: the kernel resets
/// `ublksrv_pid` when a server exits. Devices some server is still serving
/// (`LIVE`) are left alone.
async fn sweep_orphaned_devices(
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    owner_tag: u64,
    candidates: Vec<u32>,
) {
    let mut ctrl = match UVMUblkCtrlBuilder::new().build(ctrl_ring.clone()) {
        Ok(ctrl) => ctrl,
        Err(err) => {
            tracing::warn!(error = %format!("{err:#}"), "failed to open ublk control for orphan sweep");
            return;
        }
    };
    for dev_id in candidates {
        ctrl.dev_info.dev_id = dev_id;
        if let Err(err) = ctrl.get_dev_info().await {
            tracing::debug!(dev_id, error = %format!("{err:#}"), "skip ublk device in orphan sweep");
            continue;
        }
        if ctrl.dev_info.ublksrv_flags != owner_tag || ctrl.dev_info.state == UBLK_S_DEV_LIVE {
            continue;
        }
        tracing::info!(dev_id, "deleting orphaned ublk device");
        if let Err(err) = delete_dev(ctrl_ring.clone(), dev_id).await {
            tracing::warn!(
                dev_id,
                error = %format!("{err:#}"),
                "failed to delete orphaned ublk device"
            );
        }
    }
}

/// Tag for devices created by daemons serving `socket_path`. It must be the
/// same for every daemon version, so it is FNV-1a over the path bytes rather
/// than std's unspecified hasher.
fn owner_tag_for(socket_path: &Path) -> u64 {
    use std::os::unix::ffi::OsStrExt;

    let hash = socket_path
        .as_os_str()
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    // Zero is what untagged devices carry.
    hash.max(1)
}

/// How the daemon creates new ublk devices.
#[derive(Debug, Clone, Copy, Default)]
struct NewDeviceOptions {
    /// Create devices with ublk user recovery so they survive daemon restarts.
    user_recovery: bool,
    /// See [`owner_tag_for`].
    owner_tag: u64,
}

impl NewDeviceOptions {
    fn ctrl_builder(self) -> UVMUblkCtrlBuilder<'static> {
        UVMUblkCtrlBuilder::new()
            .name("overlaybd-blk")
            .user_recovery(self.user_recovery)
            .owner_tag(self.owner_tag)
    }
}

// ── Pool handlers ───────────────────────────────────────────────────────────

fn handle_get_features(pool_state: &Option<Arc<PoolState>>) -> Result<DaemonResponse> {
    let flags = pool_state.as_ref().map(|p| p.features).unwrap_or(0);
    Ok(DaemonResponse::Features { flags })
}

/// Sizing and write-bound options of an overlaybd acquire.
#[derive(Clone, Copy, Default)]
struct AcquireOptions {
    /// Expected capacity; omit to use the published image capacity.
    virtual_size: Option<u64>,
    /// Write bound of the acquire, with the budget restoration inputs.
    upper_write: UpperWriteBudget,
}

/// Write-bound inputs of a device attach.
#[derive(Clone, Copy, Default)]
struct UpperWriteBudget {
    /// Cap on the logical bytes a writable upper may take; `None` leaves the
    /// upper unbounded and only accounts it for observability.
    limit: Option<u64>,
    /// Consumed budget the snapshot being resumed recorded when it was
    /// captured. It covers writes that earlier restacks sealed into lower
    /// layers, so it applies to the fresh upper an attach creates.
    prior_consumed: Option<u64>,
    /// Whether this attach created the writable upper.
    fresh_upper: bool,
}

/// Generation-tagged sidecar records, next to the upper's data file, that
/// carry the upper write budget across upper replacements and daemon
/// restarts.
///
/// `create_snapshot_and_restack` seals the upper into a lower layer and
/// starts a fresh, empty upper at the same path, so the upper's own size only
/// records what was appended since the last replacement. Two records close
/// that gap without one clobbering the other:
///
/// - [`UPPER_WRITE_BASELINE_KIND`] names a generation and holds the budget
///   consumed *before* that generation's upper existed. It is written for the
///   upper an attach created -- including the budget a resumed snapshot
///   recorded -- and for the fresh upper every successful restack produces.
/// - [`UPPER_WRITE_REPLACEMENT_KIND`] names the generation a restack is about
///   to replace and holds the cumulative budget consumed at that point. It is
///   written before the seal and only applies once the named upper has
///   actually been replaced, so a restack that fails before sealing cannot
///   charge its budget to the surviving upper -- and overwriting a previous
///   attempt's record cannot drop the budget the surviving upper's baseline
///   still carries.
const UPPER_WRITE_BASELINE_KIND: &str = "write-baseline";
const UPPER_WRITE_REPLACEMENT_KIND: &str = "write-replaced";

/// File name of a record of `kind` for the upper at `upper_data`, namespaced
/// by the upper data file name: an image config may point several uppers into
/// one directory (an existing source upper keeps its configured path), and
/// their accounting must not be shared.
fn upper_write_record_name(upper_data: &Path, kind: &str) -> String {
    match upper_data.file_name() {
        Some(name) => format!("{}.{kind}", name.to_string_lossy()),
        None => kind.to_string(),
    }
}

fn upper_write_record_path(upper_data: &Path, kind: &str) -> PathBuf {
    upper_data.with_file_name(upper_write_record_name(upper_data, kind))
}

/// Run sidecar I/O on the blocking pool: upper storage can be slow or
/// congested, and the daemon's runtime workers must stay free for unrelated
/// control requests, metrics, and shutdown.
async fn blocking_upper_write_record<T, F>(op: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(op)
        .await
        .context("join upper write record task")?
}

/// Monotonic suffix that keeps concurrent record writers from sharing a
/// temporary file. Two restacks can reach the pre-seal persistence before
/// either holds the per-image restack lock, and shutdown or release baseline
/// flushes run through the same helper, so a shared name would let one writer
/// truncate another's temporary file before either rename.
static UPPER_WRITE_RECORD_TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Atomically publish `generation`/`bytes` as `kind`'s record next to the
/// upper.
///
/// Recovery treats a missing record as nothing spent, so the record must be
/// durable once the caller acts on it: the temporary file is synced before
/// the rename and the containing directory after it, so a crash cannot drop a
/// published record while the upper it describes was already sealed or
/// charged. A crash before that loses only the unpublished temporary file.
async fn persist_upper_write_record(
    upper_data: &Path,
    kind: &str,
    upper_generation: &str,
    bytes: u64,
) -> Result<()> {
    let upper_data = upper_data.to_path_buf();
    let kind = kind.to_string();
    let upper_generation = upper_generation.to_string();
    blocking_upper_write_record(move || {
        use std::io::Write as _;

        let path = upper_write_record_path(&upper_data, &kind);
        let tmp = upper_data.with_file_name(format!(
            "{}.tmp.{}-{}",
            upper_write_record_name(&upper_data, &kind),
            std::process::id(),
            UPPER_WRITE_RECORD_TMP_SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        let publish = || -> Result<()> {
            let mut file = std::fs::File::create(&tmp)
                .with_context(|| format!("create upper write record {}", tmp.display()))?;
            file.write_all(format!("{upper_generation}\n{bytes}\n").as_bytes())
                .with_context(|| format!("write upper write record {}", tmp.display()))?;
            file.sync_all()
                .with_context(|| format!("sync upper write record {}", tmp.display()))?;
            std::fs::rename(&tmp, &path)
                .with_context(|| format!("publish upper write record {}", path.display()))?;
            let dir = upper_data.parent().unwrap_or_else(|| Path::new("."));
            std::fs::File::open(dir)
                .with_context(|| format!("open upper write record directory {}", dir.display()))?
                .sync_all()
                .with_context(|| format!("sync upper write record directory {}", dir.display()))?;
            Ok(())
        };
        match publish() {
            Ok(()) => Ok(()),
            Err(err) => {
                // The temporary file was never published; dropping it keeps a
                // failed write from littering the upper directory.
                let _ = std::fs::remove_file(&tmp);
                Err(err)
            }
        }
    })
    .await
}

/// Read a generation-tagged record, or `None` when it does not exist.
async fn read_upper_write_record(upper_data: &Path, kind: &str) -> Result<Option<(String, u64)>> {
    let upper_data = upper_data.to_path_buf();
    let kind = kind.to_string();
    blocking_upper_write_record(move || {
        let path = upper_write_record_path(&upper_data, &kind);
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("read upper write record {}", path.display()))
            }
        };
        let invalid = || anyhow::anyhow!("invalid upper write record {}", path.display());
        let mut lines = raw.lines();
        let generation = lines.next().ok_or_else(invalid)?.to_string();
        let bytes = lines
            .next()
            .ok_or_else(invalid)?
            .trim()
            .parse::<u64>()
            .with_context(|| format!("parse upper write record {}", path.display()))?;
        Ok(Some((generation, bytes)))
    })
    .await
}

/// Budget consumed before the upper generation `current_generation` existed.
///
/// The baseline record supplies it while it names the live generation. Once
/// the upper was replaced and the fresh upper's baseline has not been written
/// yet (a crash between the seal and the record write), the replacement
/// record -- naming the generation that was replaced -- supplies the
/// cumulative total instead. A replacement record naming the live generation
/// describes a restack that failed before sealing and is ignored, so its
/// budget is not charged against bytes that are still part of the upper.
async fn restored_upper_write_baseline(upper_data: &Path, current_generation: &str) -> Result<u64> {
    if let Some((generation, bytes)) =
        read_upper_write_record(upper_data, UPPER_WRITE_BASELINE_KIND).await?
    {
        if generation == current_generation {
            return Ok(bytes);
        }
    }
    if let Some((generation, bytes)) =
        read_upper_write_record(upper_data, UPPER_WRITE_REPLACEMENT_KIND).await?
    {
        if generation != current_generation {
            return Ok(bytes);
        }
    }
    Ok(0)
}

/// Persist the baseline of the image's current upper generation, so a daemon
/// restart restores `consumed` from the upper alone.
///
/// `consumed` is the total budget spent so far; subtracting what the upper
/// itself holds leaves the part that came from earlier uppers or from the
/// snapshot being resumed, which is exactly what the record must name.
/// Callers only persist when the upper records its own appends and a limit is
/// armed, so `upper_restorable_write_bytes` always resolves here.
async fn persist_upper_write_baseline(image: &ImageFile, consumed: u64) -> Result<()> {
    let upper_data = image
        .upper_data_path()
        .await
        .context("resolve the writable upper path for the write baseline")?;
    let generation = image
        .upper_generation()
        .await
        .context("resolve the writable upper generation for the write baseline")?;
    let footprint = image
        .upper_restorable_write_bytes()
        .await
        .context("read the consumed bytes of the writable upper for the write baseline")?
        .unwrap_or(0);
    persist_upper_write_record(
        &upper_data,
        UPPER_WRITE_BASELINE_KIND,
        &generation,
        consumed.saturating_sub(footprint),
    )
    .await
}

/// Target that currently serves `dev_id`, if the device still has one.
fn armed_upper_write_target(
    devices: &DashMap<u32, ManagedDevice>,
    pool_state: &Option<Arc<PoolState>>,
    dev_id: u32,
) -> Option<Arc<OverlaybdTarget>> {
    if let Some(device) = devices.get(&dev_id) {
        return Some(Arc::clone(device.dev.target()));
    }
    let pool = pool_state.as_ref()?;
    if let Some(active) = pool.active_exclusive.get(&dev_id) {
        return Some(Arc::clone(active.dev.target()));
    }
    None
}

/// Report an error raised after a restack already mutated the live image.
///
/// The client treats a plain error response as "the old runtime is still
/// safe to resume"; after the seal that is untrue, so post-mutation failures
/// must arrive as a terminal restack failure instead, like the image's own
/// post-mutation failures.
fn terminal_restack_error(context: impl std::fmt::Display, err: anyhow::Error) -> anyhow::Error {
    RestackSnapshotTerminalFailure::new(format!("{context}: {err:#}")).into()
}

/// Baseline (`reserved - footprint`) of the upper the target serves, sampled
/// with the write path's reservation-to-append window excluded.
///
/// The gate serializes with `OverlaybdTarget::handle_write`: a write holds it
/// from before it reserves its bytes until its append has landed, so a sample
/// taken here can never pair a reservation with an upper size that does not
/// cover it. Recording such a sample as surplus would charge those bytes a
/// second time once the append lands. An upper that does not record its
/// appends cannot be bounded at its next attach anyway, so there is nothing
/// to record for it.
async fn stable_upper_write_baseline(
    image: &ImageFile,
    target: &OverlaybdTarget,
) -> Result<Option<u64>> {
    target
        .with_upper_write_progress(async {
            let Some(footprint) = image.upper_restorable_write_bytes().await? else {
                return Ok(None);
            };
            let Some((reserved, Some(_limit))) = target.upper_write_bound() else {
                return Ok(None);
            };
            Ok(Some(reserved.saturating_sub(footprint)))
        })
        .await
}

/// Record the write baseline of the fresh upper a successful restack created,
/// so a restart before the next restack restores the full total instead of
/// only the bytes the fresh upper holds.
///
/// Runs after the seal replaced the upper; failures are terminal.
async fn record_restacked_upper_write_baseline(
    dev_id: u32,
    image: &ImageFile,
    devices: &DashMap<u32, ManagedDevice>,
    pool_state: &Option<Arc<PoolState>>,
) -> Result<()> {
    // A missing target means the device already went away; there is nothing
    // left to record for it.
    let Some(target) = armed_upper_write_target(devices, pool_state, dev_id) else {
        return Ok(());
    };
    let baseline = stable_upper_write_baseline(image, &target)
        .await
        .map_err(|err| {
            terminal_restack_error(
                format!("read the restacked upper of dev_id={dev_id} for its write baseline"),
                err,
            )
        })?;
    let Some(baseline) = baseline else {
        return Ok(());
    };
    let upper_data = image.upper_data_path().await.map_err(|err| {
        terminal_restack_error(
            format!("resolve the restacked upper path of dev_id={dev_id}"),
            err,
        )
    })?;
    let generation = image.upper_generation().await.map_err(|err| {
        terminal_restack_error(
            format!("resolve the restacked upper generation of dev_id={dev_id}"),
            err,
        )
    })?;
    persist_upper_write_record(
        &upper_data,
        UPPER_WRITE_BASELINE_KIND,
        &generation,
        baseline,
    )
    .await
    .map_err(|err| {
        terminal_restack_error(
            format!("record the restacked upper write baseline of dev_id={dev_id}"),
            err,
        )
    })
}

/// Fold the gap between the live reservation and what the upper itself holds
/// into the baseline record before the device stops serving the image.
///
/// A write that fails partway appends only the chunks it reached, while the
/// target keeps the whole request's reservation so a retry cannot grow the
/// upper past the bound. The upper's size alone therefore under-reports what
/// the live target charged; persisting `reserved - footprint` as the baseline
/// makes the next attach restore the exact total. Best-effort: the device is
/// going away, and a failure here only re-opens a gap bounded by the
/// reservations of failed writes.
async fn flush_upper_write_baseline(dev_id: u32, image: &ImageFile, target: &OverlaybdTarget) {
    if !target
        .upper_write_bound()
        .is_some_and(|(_, limit)| limit.is_some())
    {
        return;
    }
    let baseline = match stable_upper_write_baseline(image, target).await {
        Ok(baseline) => baseline,
        Err(err) => {
            tracing::warn!(
                dev_id,
                error = %format!("{err:#}"),
                "failed to read the writable upper before flushing its write baseline"
            );
            return;
        }
    };
    let Some(baseline) = baseline else {
        return;
    };
    let flushed = async {
        let upper_data = image.upper_data_path().await?;
        let generation = image.upper_generation().await?;
        persist_upper_write_record(
            &upper_data,
            UPPER_WRITE_BASELINE_KIND,
            &generation,
            baseline,
        )
        .await
    }
    .await;
    if let Err(err) = flushed {
        tracing::warn!(
            dev_id,
            error = %format!("{err:#}"),
            "failed to flush the upper write baseline"
        );
    }
}

/// Bytes of the upper write budget the writable upper of `image` has already
/// consumed, for arming the bound on attach.
///
/// Only an append-only (log-structured) upper records every written byte: its
/// data file's growth is exactly the consumed budget, so re-attaching the
/// upper resumes from what it already holds instead of granting a fresh
/// allowance. Uppers that write data in place (sparse, hybrid) keep no such
/// record, so a bounded device cannot honor its limit across a re-attach; it
/// is refused here, at creation as well as at re-attach, rather than silently
/// losing its cap at the next pause/resume or daemon restart.
async fn consumed_upper_write_bytes(
    image: &ImageFile,
    image_config: &Path,
    budget: UpperWriteBudget,
) -> Result<u64> {
    if image.is_read_only().await {
        // No writable upper: nothing to restore and nothing to bound.
        return Ok(0);
    }
    let restorable = image
        .upper_restorable_write_bytes()
        .await
        .with_context(|| {
            format!(
                "read the consumed bytes of the writable upper of {}",
                image_config.display()
            )
        })?;
    match restorable {
        Some(bytes) => {
            if budget.limit.is_none() {
                // Without a limit nothing is enforced; only the metrics
                // baseline is at stake, and the carried budget does not
                // matter there.
                return Ok(bytes);
            }
            // Writes sealed into lowers by earlier restacks left the upper:
            // their budget is in the snapshot record (a fresh upper) and in
            // the generation-tagged records next to the upper (an upper
            // replaced since the records were written).
            let prior = budget
                .prior_consumed
                .filter(|_| budget.fresh_upper)
                .unwrap_or(0);
            let upper_data = image.upper_data_path().await.with_context(|| {
                format!(
                    "resolve the writable upper path of {}",
                    image_config.display()
                )
            })?;
            let generation = image.upper_generation().await.with_context(|| {
                format!(
                    "resolve the writable upper generation of {}",
                    image_config.display()
                )
            })?;
            let carried = restored_upper_write_baseline(&upper_data, &generation)
                .await
                .with_context(|| {
                    format!(
                        "restore the upper write records of {}",
                        image_config.display()
                    )
                })?;
            Ok(prior.saturating_add(carried).saturating_add(bytes))
        }
        // Without a limit nothing is enforced; only the metrics baseline is
        // affected.
        None if budget.limit.is_none() => Ok(0),
        None => bail!(
            "a rootfs write limit requires the log-structured upper layout: \
             {} uses an upper that writes data in place and does not record \
             its consumed write budget, so the limit could not be enforced \
             across a re-attach; set ublk.overlaybd.runtime_upper_mode = \
             \"logStructured\" or drop the write limit",
            image_config.display()
        ),
    }
}

async fn handle_acquire_overlaybd(
    pool_state: &Option<Arc<PoolState>>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
    options: AcquireOptions,
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
    if let Some(virtual_size) = options.virtual_size {
        anyhow::ensure!(
            virtual_size == actual_virtual_size,
            "requested overlaybd acquire virtual size {virtual_size} does not match image virtual size {actual_virtual_size}: {}",
            image_config.display()
        );
    }

    match access_mode {
        AccessMode::Exclusive => {
            let consumed =
                consumed_upper_write_bytes(&image, image_config, options.upper_write).await?;
            // A read-only image has no upper to bound or to baseline; keep
            // the acquire green even when a limit rides along with it.
            if options.upper_write.limit.is_some() && !image.is_read_only().await {
                // Record the baseline the attached upper starts from, so a
                // daemon restart restores the resumed or carried budget
                // together with the upper.
                persist_upper_write_baseline(&image, consumed)
                    .await
                    .with_context(|| {
                        format!(
                            "persist the upper write baseline of {}",
                            image_config.display()
                        )
                    })?;
            }
            acquire_exclusive(
                Arc::clone(pool),
                ctrl_ring,
                image_config,
                image,
                options.upper_write.limit,
                consumed,
            )
            .await
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
    upper_write_limit: Option<u64>,
    consumed_upper_bytes: u64,
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
        // `swap_state` drops the previous image's bound; re-arm for the image
        // this device now serves.
        if discard_supported {
            p.dev
                .target()
                .arm_upper_write_bound(upper_write_limit, consumed_upper_bytes);
        }

        if pool.supports_update_size() && p.dev_sectors != new_sectors {
            update_device_size(&p.dev, new_sectors)
                .await
                .with_context(|| format!("update {mode} device size for dev_id={dev_id}"))?;
        }

        Ok((p.dev, true))
    } else {
        tracing::debug!(mode, "pool miss: creating new device");
        let dev = create_new_device(
            ctrl_ring.clone(),
            image_config,
            image,
            upper_write_limit,
            consumed_upper_bytes,
            pool.new_device,
        )
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
    upper_write_limit: Option<u64>,
    consumed_upper_bytes: u64,
) -> Result<DaemonResponse> {
    let (dev, is_reused) = prepare_overlaybd_device(
        &pool,
        &ctrl_ring,
        image_config,
        &image,
        "exclusive",
        upper_write_limit,
        consumed_upper_bytes,
    )
    .await?;

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
        // Shared devices serve read-only memory images; no upper to bound and
        // nothing to restore.
        prepare_overlaybd_device(&pool, &ctrl_ring, image_config, &image, "shared", None, 0).await?;

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
    // Fold any failed-write reservation surplus into the baseline record
    // while the device still serves the business image.
    flush_upper_write_baseline(dev_id, &active.image, active.dev.target()).await;
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

        if matches!(
            pool.idle.compute_maintenance_action(pool.idle.len()),
            PoolMaintenanceAction::Fill(_)
        ) {
            schedule_idle_pool_refill(pool, ctrl_ring, virtual_size);
        }
    });
}

async fn refill_idle_pool(
    pool: &PoolState,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    virtual_size: u64,
) -> Result<()> {
    let PoolMaintenanceAction::Fill(to_create) =
        pool.idle.compute_maintenance_action(pool.idle.len())
    else {
        return Ok(());
    };

    let (placeholder_config, placeholder_image) = pool
        .placeholder_for(virtual_size)
        .await
        .context("build pool placeholder for prewarm")?;

    for _ in 0..to_create {
        let dev = create_new_device(
            ctrl_ring.clone(),
            &placeholder_config,
            &placeholder_image,
            None,
            0,
            pool.new_device,
        )
        .await
        .context("prewarm overlaybd pool device")?;
        let pooled = PooledDevice {
            dev,
            dev_sectors: placeholder_image.num_lbas(),
            _placeholder_image: Arc::clone(&placeholder_image),
        };
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
        // Keep refill_inflight set during backoff so concurrent requests cannot
        // bypass the delay when device creation keeps failing.
        tokio::time::sleep(Duration::from_secs(1)).await;
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
    upper_write_limit: Option<u64>,
    consumed_upper_bytes: u64,
    new_device: NewDeviceOptions,
) -> Result<UVMUblkDev<OverlaybdTarget>> {
    let discard_supported = !image.is_read_only().await;
    let target = OverlaybdTarget::from_opened_image(
        image_config.to_path_buf(),
        Arc::clone(image),
        discard_supported,
    )
    .context("create overlaybd target")?;

    let ctrl = new_device
        .ctrl_builder()
        .build(ctrl_ring.clone())
        .context("build ublk ctrl")?;

    let mut dev = UVMUblkDevBuilder::new(ctrl)
        .set_target(target)
        .build()
        .await
        .context("build ublk dev")?;

    // Writable uppers only: a read-only image has no upper to bound. The
    // caller computed the upper's consumed write budget with the image open.
    if discard_supported {
        dev.target()
            .arm_upper_write_bound(upper_write_limit, consumed_upper_bytes);
    }

    let dev_id = dev.dev_id();
    if let Err(err) = dev.start().await.context("start ublk dev") {
        cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
        return Err(err);
    }
    if let Err(err) = wait_for_ublk_dev_readable(dev_id).await {
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

    /// Open a writable image whose upper is laid out in `mode`, with `primer`
    /// bytes written into it when given.
    async fn writable_upper_image(
        dir: &Path,
        mode: UpperMode,
        primer: Option<&[u8]>,
    ) -> (Arc<ImageFile>, PathBuf) {
        let upper_dir = dir.join("upper");
        std::fs::create_dir_all(&upper_dir).unwrap();
        let upper_data = upper_dir.join("upper.data");
        let upper_index = match mode {
            UpperMode::Sparse => None,
            UpperMode::LogStructured | UpperMode::HybridLogStructured => {
                Some(upper_dir.join("upper.index"))
            }
        };
        prepare_runtime_upper(&upper_data, upper_index.as_deref(), 8192, mode).unwrap();
        let mut upper = serde_json::json!({
            "mode": serde_json::to_value(mode).unwrap(),
            "data": upper_data.to_str().unwrap(),
        });
        if let Some(index) = &upper_index {
            upper["index"] = serde_json::json!(index.to_str().unwrap());
        }
        let image_config = dir.join("image.json");
        std::fs::write(
            &image_config,
            serde_json::to_vec_pretty(&serde_json::json!({
                "lowers": [],
                "upper": upper,
                "resultFile": dir.join("result.txt").to_str().unwrap()
            }))
            .unwrap(),
        )
        .unwrap();

        let service = test_image_service(dir).await;
        let image = Arc::new(
            service
                .create_image_file(&image_config)
                .await
                .expect("open image"),
        );
        if let Some(primer) = primer {
            image.write_at(0, primer).await.expect("prime upper");
        }
        (image, image_config)
    }

    /// Budget inputs of a bounded attach that restores an existing upper.
    fn bounded_attach_budget(limit: u64) -> UpperWriteBudget {
        UpperWriteBudget {
            limit: Some(limit),
            prior_consumed: None,
            fresh_upper: false,
        }
    }

    #[tokio::test]
    async fn consumed_upper_write_bytes_restores_log_structured_appends() {
        let dir = TempDir::new().unwrap();
        let (image, image_config) =
            writable_upper_image(dir.path(), UpperMode::LogStructured, Some(&[0x5Au8; 4096])).await;

        // An append-only upper records every written byte, so re-attaching it
        // resumes from the budget it already spent instead of handing out a
        // fresh allowance.
        assert_eq!(
            consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(8192))
                .await
                .unwrap(),
            4096
        );

        // A restack that replaced an earlier upper leaves a replacement
        // record naming the generation it replaced; the surviving upper only
        // holds what was appended since.
        let upper_data = image.upper_data_path().await.unwrap();
        let generation = image.upper_generation().await.unwrap();
        persist_upper_write_record(&upper_data, UPPER_WRITE_REPLACEMENT_KIND, &generation, 1024)
            .await
            .unwrap();
        // The record names the surviving upper, so an interrupted restack
        // that never replaced it must not charge its budget a second time.
        assert_eq!(
            consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(8192))
                .await
                .unwrap(),
            4096
        );
        // Once the upper is replaced (a new generation), the record applies.
        std::fs::write(
            upper_write_record_path(&upper_data, UPPER_WRITE_REPLACEMENT_KIND),
            "00000000-0000-0000-0000-000000000000\n5120\n",
        )
        .unwrap();
        assert_eq!(
            consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(8192))
                .await
                .unwrap(),
            5120 + 4096
        );
        // A baseline recorded for the live generation adds to what the upper
        // holds -- this is how a snapshot resume keeps its restored budget
        // across a daemon restart before the next restack.
        persist_upper_write_record(&upper_data, UPPER_WRITE_BASELINE_KIND, &generation, 2048)
            .await
            .unwrap();
        assert_eq!(
            consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(8192))
                .await
                .unwrap(),
            2048 + 4096
        );
        // The replacement record of an older generation must not add to a
        // baseline that already names the live one.
        std::fs::write(
            upper_write_record_path(&upper_data, UPPER_WRITE_REPLACEMENT_KIND),
            "00000000-0000-0000-0000-000000000000\n999999\n",
        )
        .unwrap();
        assert_eq!(
            consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(8192))
                .await
                .unwrap(),
            2048 + 4096
        );
        std::fs::remove_file(upper_write_record_path(
            &upper_data,
            UPPER_WRITE_BASELINE_KIND,
        ))
        .unwrap();

        // Without a limit nothing is enforced and the records are not read.
        assert_eq!(
            consumed_upper_write_bytes(
                &image,
                &image_config,
                UpperWriteBudget {
                    limit: None,
                    ..bounded_attach_budget(0)
                }
            )
            .await
            .unwrap(),
            4096
        );

        // A freshly created upper has appended nothing, but the snapshot
        // being resumed records the budget its sealed layers absorbed.
        let fresh_dir = TempDir::new().unwrap();
        let (fresh, fresh_config) =
            writable_upper_image(fresh_dir.path(), UpperMode::LogStructured, None).await;
        assert_eq!(
            consumed_upper_write_bytes(
                &fresh,
                &fresh_config,
                UpperWriteBudget {
                    limit: Some(8192),
                    prior_consumed: Some(2048),
                    fresh_upper: true,
                }
            )
            .await
            .unwrap(),
            2048
        );
        // The snapshot record only describes the fresh upper it created.
        assert_eq!(
            consumed_upper_write_bytes(
                &fresh,
                &fresh_config,
                UpperWriteBudget {
                    limit: Some(8192),
                    prior_consumed: Some(2048),
                    fresh_upper: false,
                }
            )
            .await
            .unwrap(),
            0
        );
        // Without a limit there is no enforcement to get wrong.
        assert_eq!(
            consumed_upper_write_bytes(
                &fresh,
                &fresh_config,
                UpperWriteBudget {
                    limit: None,
                    prior_consumed: Some(2048),
                    fresh_upper: true,
                }
            )
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn consumed_upper_write_bytes_counts_overwrites_on_log_structured() {
        let dir = TempDir::new().unwrap();
        let (image, image_config) =
            writable_upper_image(dir.path(), UpperMode::LogStructured, Some(&[0x5Au8; 4096])).await;
        // The append-only upper appends every write, overwrites included, so
        // both writes count toward the restored budget.
        image
            .write_at(0, &[0xA5u8; 4096])
            .await
            .expect("rewrite block");
        assert_eq!(
            consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(8192))
                .await
                .unwrap(),
            8192
        );
    }

    #[tokio::test]
    async fn consumed_upper_write_bytes_refuses_unreadable_records_for_limits() {
        let dir = TempDir::new().unwrap();
        let (image, image_config) =
            writable_upper_image(dir.path(), UpperMode::LogStructured, Some(&[0x5Au8; 4096])).await;
        let upper_data = image.upper_data_path().await.unwrap();
        std::fs::write(
            upper_write_record_path(&upper_data, UPPER_WRITE_BASELINE_KIND),
            "not-a-number",
        )
        .unwrap();

        // A bounded attach cannot restore its budget from an unreadable
        // baseline record, so it refuses instead of guessing low.
        let err = consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(8192))
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("restore the upper write records"),
            "unexpected error: {err:#}"
        );
        // The same holds for a baseline that is absent, but a replacement
        // record that cannot be parsed.
        std::fs::remove_file(upper_write_record_path(
            &upper_data,
            UPPER_WRITE_BASELINE_KIND,
        ))
        .unwrap();
        std::fs::write(
            upper_write_record_path(&upper_data, UPPER_WRITE_REPLACEMENT_KIND),
            "not-a-number",
        )
        .unwrap();
        let err = consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(8192))
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("restore the upper write records"),
            "unexpected error: {err:#}"
        );
        // Without a limit there is no enforcement to get wrong.
        assert_eq!(
            consumed_upper_write_bytes(
                &image,
                &image_config,
                UpperWriteBudget {
                    limit: None,
                    ..bounded_attach_budget(0)
                }
            )
            .await
            .unwrap(),
            4096
        );
    }

    /// A restack that fails after an earlier one succeeded must not drop the
    /// budget carried by the surviving upper's baseline.
    #[tokio::test]
    async fn consumed_upper_write_bytes_survives_failed_restack_after_success() {
        let dir = TempDir::new().unwrap();
        let (image, image_config) =
            writable_upper_image(dir.path(), UpperMode::LogStructured, None).await;
        let upper_data = image.upper_data_path().await.unwrap();
        let generation = image.upper_generation().await.unwrap();
        let replaced_generation = "11111111-1111-1111-1111-111111111111";

        // Attach baseline, then a first restack: the fresh upper's baseline
        // names the cumulative total and the replaced generation is recorded.
        persist_upper_write_baseline(&image, 0).await.unwrap();
        persist_upper_write_record(
            &upper_data,
            UPPER_WRITE_REPLACEMENT_KIND,
            replaced_generation,
            4096,
        )
        .await
        .unwrap();
        persist_upper_write_record(&upper_data, UPPER_WRITE_BASELINE_KIND, &generation, 4096)
            .await
            .unwrap();

        // Writes land in the surviving upper after that restack.
        image.write_at(0, &[0x5Au8; 4096]).await.unwrap();

        // A second restack records the pending replacement against the
        // surviving generation, and then fails before sealing it.
        persist_upper_write_record(
            &upper_data,
            UPPER_WRITE_REPLACEMENT_KIND,
            &generation,
            4096 + 4096,
        )
        .await
        .unwrap();

        // The surviving upper's baseline still carries the earlier total; an
        // attach that only consulted the pending record would drop it.
        assert_eq!(
            consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(16384))
                .await
                .unwrap(),
            4096 + 4096
        );
    }

    /// The baseline a snapshot resume restored is persisted next to the fresh
    /// upper, so a daemon restart before the next restack restores the full
    /// total instead of only the bytes the upper holds.
    #[tokio::test]
    async fn consumed_upper_write_bytes_keeps_resumed_total_across_restart() {
        let dir = TempDir::new().unwrap();
        let (image, image_config) =
            writable_upper_image(dir.path(), UpperMode::LogStructured, None).await;
        let resumed = 6 * 1024 * 1024 * 1024u64;

        // The resume attach restores the snapshot's consumed budget and
        // records the baseline of the fresh upper it created.
        let consumed = consumed_upper_write_bytes(
            &image,
            &image_config,
            UpperWriteBudget {
                limit: Some(8 * 1024 * 1024 * 1024),
                prior_consumed: Some(resumed),
                fresh_upper: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(consumed, resumed);
        persist_upper_write_baseline(&image, consumed)
            .await
            .unwrap();

        // The sandbox writes, then the daemon restarts before any restack.
        image.write_at(0, &[0x5Au8; 4096]).await.unwrap();

        // Recovery restores the resumed total plus what the upper holds.
        assert_eq!(
            consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(u64::MAX))
                .await
                .unwrap(),
            resumed + 4096
        );
    }

    /// A write that fails partway keeps its whole reservation while only part
    /// of it reached the upper. Flushing the baseline when the device stops
    /// serving the image folds the surplus in, so the next attach restores
    /// the exact total the live target charged.
    #[tokio::test]
    async fn flush_upper_write_baseline_restores_failed_write_surplus() {
        let dir = TempDir::new().unwrap();
        let (image, image_config) =
            writable_upper_image(dir.path(), UpperMode::LogStructured, Some(&[0x5Au8; 4096])).await;
        let target =
            OverlaybdTarget::from_opened_image(image_config.clone(), Arc::clone(&image), true)
                .expect("create overlaybd target");

        // The live target charged two blocks: one appended, one held as the
        // reservation of a write that failed before appending.
        target.arm_upper_write_bound(Some(16384), 8192);
        flush_upper_write_baseline(1, &image, &target).await;

        // The upper still holds a single block, and the record makes the next
        // attach restore both.
        assert_eq!(
            consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(16384))
                .await
                .unwrap(),
            8192
        );
    }

    /// The baseline pairs the bound with the upper's bytes while the write
    /// path's reservation-to-append window is held closed.
    #[tokio::test]
    async fn stable_upper_write_baseline_pairs_bound_with_upper_bytes() {
        let dir = TempDir::new().unwrap();
        let (image, image_config) =
            writable_upper_image(dir.path(), UpperMode::LogStructured, Some(&[0x5Au8; 4096])).await;
        let target = OverlaybdTarget::from_opened_image(image_config, Arc::clone(&image), true)
            .expect("create overlaybd target");

        // Two blocks charged, one appended: the surplus still counts.
        target.arm_upper_write_bound(Some(16384), 8192);
        assert_eq!(
            stable_upper_write_baseline(&image, &target).await.unwrap(),
            Some(4096)
        );

        // Without a limit there is nothing to enforce or record.
        target.arm_upper_write_bound(None, 0);
        assert_eq!(
            stable_upper_write_baseline(&image, &target).await.unwrap(),
            None
        );
    }

    /// The restack samples the replaced upper's budget and seals it inside the
    /// write path's reservation-to-append window: a total that lands while the
    /// window is still held is part of the sealed layer and must be part of
    /// the replacement record, or a resume restores a budget short by it.
    #[tokio::test]
    async fn seal_restack_records_the_budget_of_the_sealed_upper() {
        let dir = TempDir::new().unwrap();
        let (image, image_config) =
            writable_upper_image(dir.path(), UpperMode::LogStructured, Some(&[0x5Au8; 4096])).await;
        let upper_data = image.upper_data_path().await.unwrap();
        let replaced_generation = image.upper_generation().await.unwrap();
        let record_path = upper_write_record_path(&upper_data, UPPER_WRITE_REPLACEMENT_KIND);
        let layer_path = dir.path().join("snapshot").join("layer.commit");
        let target = Arc::new(
            OverlaybdTarget::from_opened_image(image_config, Arc::clone(&image), true)
                .expect("create overlaybd target"),
        );
        target.arm_upper_write_bound(Some(16384), 8192);

        // Hold the reservation-to-append window closed, as an admitted write
        // does until its append lands.
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let holder = tokio::spawn({
            let target = Arc::clone(&target);
            async move {
                target
                    .with_upper_write_progress(async {
                        let _ = held_tx.send(());
                        let _ = release_rx.await;
                    })
                    .await;
            }
        });
        held_rx.await.unwrap();

        let seal = tokio::spawn({
            let image = Arc::clone(&image);
            let target = Arc::clone(&target);
            let layer_path = layer_path.clone();
            async move {
                seal_restack_snapshot_with_upper_write_record(0, &image, &target, &layer_path).await
            }
        });
        // The seal neither samples nor records while the window is held.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!seal.is_finished(), "seal must wait for the write window");
        assert!(
            !record_path.exists(),
            "the replacement record must not be sampled while a write is in flight"
        );

        // The in-flight write's reservation and append land before the window
        // closes, so the seal observes and records them.
        target.arm_upper_write_bound(Some(16384), 12288);
        drop(release_tx);
        holder.await.unwrap();

        let (descriptor, consumed) = seal.await.unwrap().expect("seal restack");
        assert!(descriptor.is_some(), "restack must produce a layer");
        assert_eq!(consumed, Some(12288));
        let (generation, bytes) =
            read_upper_write_record(&upper_data, UPPER_WRITE_REPLACEMENT_KIND)
                .await
                .unwrap()
                .expect("replacement record");
        assert_eq!(generation, replaced_generation);
        assert_eq!(bytes, 12288);
        // The seal replaced the upper the record names.
        assert_ne!(image.upper_generation().await.unwrap(), replaced_generation);
    }

    /// Records are namespaced by the upper data file name: uppers that share a
    /// directory keep separate accounting.
    #[tokio::test]
    async fn upper_write_records_are_namespaced_by_upper_data_file() {
        let dir = TempDir::new().unwrap();
        let first = dir.path().join("first.data");
        let second = dir.path().join("second.data");
        persist_upper_write_record(&first, UPPER_WRITE_BASELINE_KIND, "gen-first", 1024)
            .await
            .unwrap();
        persist_upper_write_record(&second, UPPER_WRITE_BASELINE_KIND, "gen-second", 2048)
            .await
            .unwrap();

        assert_eq!(
            read_upper_write_record(&first, UPPER_WRITE_BASELINE_KIND)
                .await
                .unwrap()
                .unwrap(),
            ("gen-first".to_string(), 1024)
        );
        assert_eq!(
            read_upper_write_record(&second, UPPER_WRITE_BASELINE_KIND)
                .await
                .unwrap()
                .unwrap(),
            ("gen-second".to_string(), 2048)
        );
    }

    /// Concurrent writers for the same upper and record kind each publish
    /// their own generation/byte pair: they must not share a temporary file,
    /// and the last rename must leave a complete record behind.
    #[tokio::test]
    async fn concurrent_upper_write_records_do_not_share_a_temporary_file() {
        let dir = TempDir::new().unwrap();
        let upper_data = dir.path().join("upper.data");
        let mut writers = Vec::new();
        for writer in 0..16u64 {
            let upper_data = upper_data.clone();
            writers.push(tokio::spawn(async move {
                for round in 0..8u64 {
                    let generation = format!("gen-{writer}-{round}");
                    let bytes = writer * 1024 + round;
                    persist_upper_write_record(
                        &upper_data,
                        UPPER_WRITE_BASELINE_KIND,
                        &generation,
                        bytes,
                    )
                    .await
                    .unwrap();
                }
            }));
        }
        for writer in writers {
            writer.await.unwrap();
        }

        // The surviving record is one complete writer's pair, never a torn
        // mix of two.
        let (generation, bytes) = read_upper_write_record(&upper_data, UPPER_WRITE_BASELINE_KIND)
            .await
            .unwrap()
            .expect("record");
        let parsed = generation
            .strip_prefix("gen-")
            .expect("generation tag")
            .split_once('-')
            .map(|(writer, round)| {
                (
                    writer.parse::<u64>().unwrap(),
                    round.parse::<u64>().unwrap(),
                )
            })
            .expect("writer-round suffix");
        assert_eq!(bytes, parsed.0 * 1024 + parsed.1);

        // Published records leave no temporary files behind.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "temporary files left: {leftovers:?}");
    }

    #[tokio::test]
    async fn consumed_upper_write_bytes_rejects_bounded_in_place_uppers() {
        // Sparse and hybrid uppers write data in place and record no consumed
        // budget, so a bounded device is refused at creation and at re-attach
        // rather than silently losing its cap at the next pause/resume or
        // daemon restart.
        for mode in [UpperMode::Sparse, UpperMode::HybridLogStructured] {
            let dir = TempDir::new().unwrap();
            let (image, image_config) =
                writable_upper_image(dir.path(), mode, Some(&[0x5Au8; 4096])).await;
            let err =
                consumed_upper_write_bytes(&image, &image_config, bounded_attach_budget(8192))
                    .await
                    .unwrap_err();
            assert!(
                err.to_string()
                    .contains("requires the log-structured upper layout"),
                "unexpected error for {mode:?}: {err:#}"
            );
            // Without a limit there is no enforcement to get wrong.
            assert_eq!(
                consumed_upper_write_bytes(
                    &image,
                    &image_config,
                    UpperWriteBudget {
                        limit: None,
                        ..bounded_attach_budget(0)
                    }
                )
                .await
                .unwrap(),
                0
            );
        }
    }

    #[tokio::test]
    async fn failed_idle_pool_refill_waits_before_retry() {
        let dir = TempDir::new().unwrap();
        let image_service = test_image_service(dir.path()).await;
        let placeholder_dir = dir.path().join("not-a-directory");
        std::fs::write(&placeholder_dir, b"").unwrap();
        let pool = PoolState::new(
            PoolConfig {
                low_watermark: 1,
                ..test_pool_config()
            },
            0,
            image_service,
            placeholder_dir,
        );
        let (ctrl_ring, _handle) =
            storage_util::io_ring::spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);

        // Fail before allocating any ublk device, and verify repeated failures
        // leave time between attempts without permanently stopping retries.
        assert!(refill_idle_pool(&pool, ctrl_ring.clone(), 4096)
            .await
            .is_err());
        for _ in 0..2 {
            let started = tokio::time::Instant::now();
            tokio::time::timeout(
                Duration::from_secs(5),
                refill_idle_pool_best_effort(&pool, ctrl_ring.clone(), 4096),
            )
            .await
            .expect("failed refill should eventually allow another attempt");
            assert!(started.elapsed() >= Duration::from_secs(1));
        }
    }

    #[tokio::test]
    async fn placeholder_for_builds_cached_same_size_images_per_virtual_size() {
        let dir = TempDir::new().unwrap();
        let image_service = test_image_service(dir.path()).await;
        let placeholder_dir = dir.path().join("placeholders");
        let pool = PoolState::new(
            test_pool_config(),
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

    #[test]
    fn default_recover_concurrency_follows_cpu_count() {
        let cpus = std::thread::available_parallelism().map_or(1, usize::from);
        assert_eq!(default_recover_concurrency(), cpus);
    }

    #[test]
    fn owner_tag_is_stable_and_distinct_per_socket_path() {
        // Pinned: daemons of different versions must agree on the tag.
        assert_eq!(
            owner_tag_for(Path::new("/run/aenv/ublk-daemon.sock")),
            0x3327_419d_d588_c696
        );
        assert_ne!(
            owner_tag_for(Path::new("/run/aenv/ublk-daemon.sock")),
            owner_tag_for(Path::new("/run/aenv-2/ublk-daemon.sock"))
        );
        assert_ne!(owner_tag_for(Path::new("")), 0);
    }

    fn test_handoff() -> DaemonResponse {
        DaemonResponse::DeviceAcquired {
            dev_id: 7,
            device_path: PathBuf::from("/dev/ublkb7"),
        }
    }

    #[tokio::test]
    async fn device_handoff_read_by_client_is_delivered() {
        let (mut daemon, mut client) = tokio::net::UnixStream::pair().unwrap();
        let client = tokio::spawn(async move {
            let response: DaemonResponse = recv_message(&mut client).await.unwrap().unwrap();
            assert!(matches!(
                response,
                DaemonResponse::DeviceAcquired { dev_id: 7, .. }
            ));
        });

        assert!(deliver_device_handoff(&mut daemon, &test_handoff(), 7).await);
        client.await.unwrap();
    }

    #[tokio::test]
    async fn device_handoff_to_disconnected_client_is_undelivered() {
        // The client's RPC timed out and it hung up before the device was ready.
        let (mut daemon, client) = tokio::net::UnixStream::pair().unwrap();
        drop(client);

        assert!(!deliver_device_handoff(&mut daemon, &test_handoff(), 7).await);
    }

    #[tokio::test]
    async fn device_handoff_dropped_unread_is_undelivered() {
        // The client's RPC timed out after the response was written but before
        // the client read it.
        let (mut daemon, client) = tokio::net::UnixStream::pair().unwrap();
        let client = tokio::spawn(async move {
            client.readable().await.unwrap();
            drop(client);
        });

        assert!(!deliver_device_handoff(&mut daemon, &test_handoff(), 7).await);
        client.await.unwrap();
    }

    #[tokio::test]
    async fn device_handoff_dropped_mid_read_is_undelivered() {
        let (mut daemon, mut client) = tokio::net::UnixStream::pair().unwrap();
        let client = tokio::spawn(async move {
            let mut len_prefix = [0u8; 4];
            client.read_exact(&mut len_prefix).await.unwrap();
            drop(client);
        });

        assert!(!deliver_device_handoff(&mut daemon, &test_handoff(), 7).await);
        client.await.unwrap();
    }
}
