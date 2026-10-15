use std::collections::HashMap;
use std::ops::Deref;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{DownloadConfig, ImageConfig, LayerConfig, UpperConfig, UpperMode};
use overlaybd::helper::prepare_runtime_upper;
use overlaybd::image_file::ImageFile;
use overlaybd::image_service::ImageService;
use overlaybd::snapshot::OverlaybdCompactOutput;
use overlaybd::virtual_file::VirtualFile;
use overlaybd::RestackSnapshotTerminalFailure;
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};
use tokio::task::JoinSet;
use warm_pool::{PoolConfig, PoolMaintenanceAction, WarmPool};

use storage_util::fatal_exit;
use storage_util::io_ring::IoRingHandle;
use uvm_ublk::{
    delete_dev, tmp_pack_path, ublk_caps, wait_for_ublk_dev, OverlaybdTarget, StartupPackRecorder,
    UVMUblkCtrlBuilder, UVMUblkDev, UVMUblkDevBuilder, UVMUblkTarget,
};

use crate::protocol::{
    recv_message, send_message, AccessMode, DaemonRequest, DaemonResponse, PackRecordingState,
    ResizeToolSpec, SourceStateStrategy,
};
use crate::runtime::{self, MaterializeOverlaybdRuntimeRequest};

// ── Managed device wrapper ──────────────────────────────────────────────────

struct ManagedDevice {
    dev: UVMUblkDev<OverlaybdTarget>,
    image_config: PathBuf,
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

/// Canonical key for shared-mode devices: (image_config, global_config, upper access).
type SharedKey = (PathBuf, PathBuf, bool);

// ── Lazy ImageService cache ─────────────────────────────────────────────────

/// Cache of `ImageService` instances keyed by canonical global config path.
///
/// Each unique overlaybd global config produces its own `ImageService` with
/// independent io_ring pools and cache settings. Entries are created lazily
/// on the first request that opens an overlaybd image with a given config path.
pub(crate) struct ImageServiceCache {
    services: Mutex<HashMap<PathBuf, ImageService>>,
    /// Coordinate source consumption and image opens even without a warm pool.
    image_locks: DashMap<PathBuf, Arc<RwLock<()>>>,
    p2p_publish_url: Option<String>,
}

struct ImageLock<'a> {
    cache: &'a ImageServiceCache,
    key: PathBuf,
    lock: Option<Arc<RwLock<()>>>,
}

impl Deref for ImageLock<'_> {
    type Target = RwLock<()>;

    fn deref(&self) -> &Self::Target {
        self.lock.as_deref().expect("image lock released")
    }
}

impl Drop for ImageLock<'_> {
    fn drop(&mut self) {
        drop(self.lock.take());
        self.cache.image_locks.remove_if(&self.key, |_, lock| {
            // Release this caller before checking, so concurrent final drops
            // cannot both leave the last map-owned reference behind.
            Arc::strong_count(lock) == 1
        });
    }
}

impl ImageServiceCache {
    pub(crate) fn new(p2p_publish_url: Option<String>) -> Self {
        Self {
            services: Mutex::new(HashMap::new()),
            image_locks: DashMap::new(),
            p2p_publish_url,
        }
    }

    fn image_lock(&self, image_config: &Path) -> ImageLock<'_> {
        let key =
            std::fs::canonicalize(image_config).unwrap_or_else(|_| image_config.to_path_buf());
        let lock = Arc::clone(
            self.image_locks
                .entry(key.clone())
                .or_insert_with(|| Arc::new(RwLock::new(())))
                .value(),
        );
        ImageLock {
            cache: self,
            key,
            lock: Some(lock),
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
    /// Coalesces asynchronous pool refill work so concurrent acquire/release
    /// requests do not all synchronously create replacement ublk devices.
    refill_inflight: AtomicBool,
    config: PoolConfig,
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
        features: u64,
        image_service: ImageService,
        placeholder_dir: PathBuf,
    ) -> Self {
        Self {
            idle: WarmPool::new(config.clone()),
            active_exclusive: DashMap::new(),
            active_shared: DashMap::new(),
            shared_by_dev_id: DashMap::new(),
            refill_inflight: AtomicBool::new(false),
            config,
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
            pack_recordings: Arc::new(DashMap::new()),
            startup_pack_handles: Arc::new(Mutex::new(Vec::new())),
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
    pub fn enable_pool(&mut self, config: PoolConfig, features: u64) {
        self.pool_state = Some(Arc::new(PoolState::new(
            config,
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
                    let pack_recordings = Arc::clone(&self.pack_recordings);
                    let startup_pack_handles = Arc::clone(&self.startup_pack_handles);
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
        // Abort pack recordings first so a window task cannot finalize into a
        // device that is being stopped, and no tmp pack files survive.
        let recording_ids: Vec<u32> = self.pack_recordings.iter().map(|r| *r.key()).collect();
        for dev_id in recording_ids {
            if let Some((_, handle)) = self.pack_recordings.remove(&dev_id) {
                abort_pack_recording_handle(&handle).await;
            }
        }

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
            read_only_upper,
        } => {
            handle_create_raw_overlaybd(
                &devices,
                ctrl_ring,
                &image_service_cache,
                &image_config,
                &global_config,
                read_only_upper,
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
            source_state_strategy,
        } => {
            let request = MaterializeOverlaybdRuntimeRequest {
                image_service_cache: &image_service_cache,
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
                source_state_strategy,
            };
            handle_create_overlaybd_runtime_device(request, ctrl_ring, &devices, &pool_state).await
        }
        DaemonRequest::Delete { dev_id } => {
            handle_delete(&devices, ctrl_ring, &pack_recordings, dev_id).await
        }
        DaemonRequest::SnapshotDevice {
            dev_id,
            output_layer_path,
            compact_from,
        } => {
            handle_snapshot_device(
                &devices,
                &pool_state,
                &image_service_cache,
                dev_id,
                &output_layer_path,
                compact_from,
            )
            .await
        }
        DaemonRequest::CompactLayers {
            lowers,
            repo_blob_url,
            global_config,
            output_layer_path,
            output,
        } => {
            let input_layers = lowers.len();
            let started = std::time::Instant::now();
            tracing::info!(
                input_layers,
                output_path = %output_layer_path.display(),
                encoding = ?output,
                "starting overlaybd layer compaction"
            );
            let result = handle_compact_layers(
                &image_service_cache,
                lowers,
                repo_blob_url,
                &global_config,
                &output_layer_path,
                output,
            )
            .await;
            match &result {
                Ok(_) => tracing::info!(
                    input_layers,
                    output_path = %output_layer_path.display(),
                    encoding = ?output,
                    elapsed = ?started.elapsed(),
                    "compacted overlaybd layers"
                ),
                Err(error) => tracing::warn!(
                    input_layers,
                    output_path = %output_layer_path.display(),
                    encoding = ?output,
                    elapsed = ?started.elapsed(),
                    ?error,
                    "failed to compact overlaybd layers"
                ),
            }
            result
        }
        DaemonRequest::FlushForCheckpoint { dev_id } => {
            handle_flush_for_checkpoint(&devices, &pool_state, dev_id).await
        }
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
            read_only_upper,
        } => acquire_overlaybd(
            &pool_state,
            ctrl_ring.clone(),
            &image_service_cache,
            &image_config,
            &global_config,
            virtual_size,
            access_mode,
            read_only_upper,
        )
        .await
        .map(
            |(dev_id, device_path, image_config_path)| DaemonResponse::DeviceAcquired {
                dev_id,
                device_path,
                image_config_path,
            },
        ),
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

async fn handle_create_overlaybd_runtime_device(
    request: MaterializeOverlaybdRuntimeRequest<'_>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    devices: &DashMap<u32, ManagedDevice>,
    pool_state: &Option<Arc<PoolState>>,
) -> Result<DaemonResponse> {
    let image_service_cache = request.image_service_cache;
    let global_config = request.global_config;
    let runtime_dir = request.runtime_dir;
    // Reuse still needs the source files when it opens the materialized config.
    // Keep this guard through device creation; different sources and concurrent
    // Reuse requests retain their existing parallelism.
    let source_lock = image_service_cache.image_lock(request.source_image_config);
    let (_reuse_guard, _consume_guard) = match request.source_state_strategy {
        SourceStateStrategy::Reuse => (Some(source_lock.read().await), None),
        SourceStateStrategy::Consume => (None, Some(source_lock.write().await)),
    };
    let runtime = runtime::materialize_overlaybd_runtime(request)
        .await
        .with_context(|| format!("materialize overlaybd runtime in {}", runtime_dir.display()))?;

    let created = if pool_state.is_some() {
        acquire_overlaybd(
            pool_state,
            ctrl_ring,
            image_service_cache,
            &runtime.runtime_image_config_path,
            global_config,
            Some(runtime.actual_virtual_size),
            AccessMode::Exclusive,
            false,
        )
        .await
        .map(|(id, path, _)| (id, path))
    } else {
        create_overlaybd_device(
            devices,
            ctrl_ring,
            image_service_cache,
            &runtime.runtime_image_config_path,
            global_config,
            false,
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
    read_only_upper: bool,
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
        read_only_upper,
    )
    .await?;

    Ok(DaemonResponse::DeviceCreated {
        dev_id,
        device_path,
    })
}

async fn open_overlaybd_image(
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
    read_only_upper: bool,
) -> Result<Arc<ImageFile>> {
    let image_lock = image_service_cache.image_lock(image_config);
    let _image_guard = image_lock.read().await;
    let image_service = image_service_cache.get_or_create(global_config).await?;
    let image = image_service
        .create_image_file_with_upper_access(image_config, read_only_upper)
        .await?;
    Ok(Arc::new(image))
}

async fn create_overlaybd_device(
    devices: &DashMap<u32, ManagedDevice>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
    read_only_upper: bool,
) -> Result<(u32, PathBuf)> {
    let image = open_overlaybd_image(
        image_service_cache,
        image_config,
        global_config,
        read_only_upper,
    )
    .await
    .with_context(|| format!("open overlaybd image: {}", image_config.display()))?;
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
            image_config: image_config.to_path_buf(),
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
        fatal_exit(&format!("stop ublk device {dev_id}: {err:#}"));
    }

    dev.wait_for_bg_tasks().await;
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
            fatal_exit(&format!(
                "stop ublk {dev_id} after startup failure: {err:#}"
            ));
        }
    }

    dev.wait_for_bg_tasks().await;

    drop(dev);

    let mut ctrl = match UVMUblkCtrlBuilder::new().dev_id(dev_id).build(ctrl_ring) {
        Ok(ctrl) => ctrl,
        Err(err) => {
            fatal_exit(&format!(
                "open ublk {dev_id} control for startup rollback: {err:#}"
            ));
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
            fatal_exit(&format!(
                "delete ublk {dev_id} after startup failure: {err:#}"
            ));
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

async fn handle_snapshot_device(
    devices: &DashMap<u32, ManagedDevice>,
    pool_state: &Option<Arc<PoolState>>,
    image_service_cache: &ImageServiceCache,
    dev_id: u32,
    output_layer_path: &Path,
    compact_from: Option<usize>,
) -> Result<DaemonResponse> {
    let (image, image_config) = find_overlaybd_image(devices, pool_state, dev_id, "snapshot")?;

    tracing::info!(
        dev_id,
        output = %output_layer_path.display(),
        "creating overlaybd device snapshot"
    );

    let result = {
        let image_lock = image_service_cache.image_lock(&image_config);
        let _image_guard = image_lock.write().await;
        runtime::restack_runtime(&image, &image_config, output_layer_path, compact_from).await
    };
    let (descriptor, lowers) = result.with_context(|| {
        format!(
            "snapshot overlaybd device dev_id={dev_id} to {}",
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
        "ublk daemon device snapshot completed"
    );
    Ok(DaemonResponse::SnapshotDeviceCreated {
        descriptor,
        lowers,
        data_stat,
        ext4_used_bytes,
    })
}

async fn handle_compact_layers(
    image_service_cache: &ImageServiceCache,
    lowers: Vec<LayerConfig>,
    repo_blob_url: String,
    global_config: &Path,
    output_layer_path: &Path,
    output: OverlaybdCompactOutput,
) -> Result<DaemonResponse> {
    let service = image_service_cache.get_or_create(global_config).await?;
    let image = ImageFile::open(
        ImageConfig {
            repo_blob_url,
            lowers,
            download_override: Some(DownloadConfig {
                enable: false,
                ..Default::default()
            }),
            ..Default::default()
        },
        service,
        None,
    )
    .await?;
    let parent = output_layer_path
        .parent()
        .context("compacted layer output has no parent directory")?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let output_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_rw(temporary.path(), false)?);
    image
        .compact(output_file, output)
        .await
        .context("merge overlaybd layers")?;
    temporary
        .persist(output_layer_path)
        .map_err(|error| error.error)
        .with_context(|| format!("publish compacted layer to {}", output_layer_path.display()))?;
    Ok(DaemonResponse::Ok)
}

async fn handle_flush_for_checkpoint(
    devices: &DashMap<u32, ManagedDevice>,
    pool_state: &Option<Arc<PoolState>>,
    dev_id: u32,
) -> Result<DaemonResponse> {
    let (image, _) = find_overlaybd_image(devices, pool_state, dev_id, "checkpoint flush")?;
    image
        .flush_for_checkpoint()
        .await
        .with_context(|| format!("flush overlaybd image for dev_id={dev_id}"))?;
    Ok(DaemonResponse::Ok)
}

fn find_overlaybd_image(
    devices: &DashMap<u32, ManagedDevice>,
    pool_state: &Option<Arc<PoolState>>,
    dev_id: u32,
    operation: &str,
) -> Result<(Arc<ImageFile>, PathBuf)> {
    if let Some(device) = devices.get(&dev_id) {
        return Ok((Arc::clone(&device.image), device.image_config.clone()));
    }

    let Some(pool) = pool_state else {
        bail!("device {dev_id} not found for {operation}");
    };
    if let Some(active) = pool.active_exclusive.get(&dev_id) {
        return Ok((Arc::clone(&active.image), active.image_config.clone()));
    }
    if let Some(shared_key) = pool.shared_by_dev_id.get(&dev_id) {
        let key = shared_key.clone();
        drop(shared_key);
        let active = pool
            .active_shared
            .get(&key)
            .with_context(|| format!("device {dev_id} not found for {operation}"))?;
        return Ok((Arc::clone(&active.image), active.image_config.clone()));
    }

    bail!("device {dev_id} not found for {operation}")
}

// ── Pool handlers ───────────────────────────────────────────────────────────

fn handle_get_features(pool_state: &Option<Arc<PoolState>>) -> Result<DaemonResponse> {
    let flags = pool_state.as_ref().map(|p| p.features).unwrap_or(0);
    Ok(DaemonResponse::Features { flags })
}

#[allow(clippy::too_many_arguments)]
async fn acquire_overlaybd(
    pool_state: &Option<Arc<PoolState>>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_service_cache: &ImageServiceCache,
    image_config: &Path,
    global_config: &Path,
    virtual_size: Option<u64>,
    access_mode: AccessMode,
    read_only_upper: bool,
) -> Result<(u32, PathBuf, PathBuf)> {
    let pool = pool_state
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("warm pool not enabled"))?;

    let image = open_overlaybd_image(
        image_service_cache,
        image_config,
        global_config,
        read_only_upper,
    )
    .await
    .with_context(|| format!("open overlaybd image: {}", image_config.display()))?;
    let actual_virtual_size = image.size_bytes();
    if let Some(virtual_size) = virtual_size {
        anyhow::ensure!(
            virtual_size == actual_virtual_size,
            "requested overlaybd acquire virtual size {virtual_size} does not match image virtual size {actual_virtual_size}: {}",
            image_config.display()
        );
    }

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
                read_only_upper,
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

        let prepared = async {
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
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(err) = prepared {
            cleanup_failed_ublk_start(ctrl_ring.clone(), p.dev).await;
            return Err(err);
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
) -> Result<(u32, PathBuf, PathBuf)> {
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

    Ok((dev_id, device_path, image_config.to_path_buf()))
}

async fn acquire_shared(
    pool: Arc<PoolState>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    image_config: &Path,
    global_config: &Path,
    read_only_upper: bool,
    image: Arc<ImageFile>,
) -> Result<(u32, PathBuf, PathBuf)> {
    let key = (
        std::fs::canonicalize(image_config).unwrap_or_else(|_| image_config.to_path_buf()),
        std::fs::canonicalize(global_config).unwrap_or_else(|_| global_config.to_path_buf()),
        read_only_upper,
    );

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
        return Ok((dev_id, device_path, entry.image_config.clone()));
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
            let existing_config = active.image_config.clone();
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
            return Ok((existing_dev_id, existing_path, existing_config));
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

    Ok((dev_id, device_path, image_config.to_path_buf()))
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
        tracing::info!(dev_id, "releasing exclusive device");
        idle_released_device(pool, ctrl_ring, dev_id, active.dev, active.image).await;
        return Ok(DaemonResponse::Released);
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
                tracing::info!(dev_id, "releasing shared device (refcount reached 0)");
                idle_released_device(pool, ctrl_ring, dev_id, active.dev, active.image).await;
                return Ok(DaemonResponse::Released);
            }
        }
    }

    bail!("device {dev_id} not found in active pool");
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
        let dev = create_new_device(ctrl_ring.clone(), &placeholder_config, &placeholder_image)
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
    use std::time::Duration;
    use tempfile::TempDir;

    #[tokio::test]
    async fn cancelled_image_lock_waiter_keeps_active_source_protected() {
        use std::future::{poll_fn, Future};
        use std::task::Poll;

        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source.json");
        std::fs::write(&source, b"{}").unwrap();
        let alias = dir.path().join("alias.json");
        std::os::unix::fs::symlink(&source, &alias).unwrap();
        let cache = ImageServiceCache::new(None);
        let reader = cache.image_lock(&source);
        let read_guard = reader.read().await;

        let mut cancelled = Box::pin(async {
            let lock = cache.image_lock(&alias);
            let _guard = lock.write().await;
        });
        poll_fn(|cx| {
            assert!(cancelled.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(cancelled);

        let mut next = Box::pin(async {
            let lock = cache.image_lock(&alias);
            let _guard = lock.write().await;
        });
        poll_fn(|cx| {
            assert!(next.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(read_guard);
        drop(reader);
        next.as_mut().await;
        drop(next);
        assert!(cache.image_locks.is_empty());
    }

    #[tokio::test]
    async fn consume_waits_until_reuse_has_opened_the_source() {
        use std::future::{poll_fn, Future};
        use std::task::Poll;

        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source.json");
        let upper_data = dir.path().join("upper.data");
        let upper_index = dir.path().join("upper.index");
        prepare_runtime_upper(
            &upper_data,
            Some(&upper_index),
            8192,
            UpperMode::LogStructured,
        )
        .unwrap();
        std::fs::write(
            &source,
            serde_json::to_vec(&serde_json::json!({
                "upper": {"data": upper_data, "index": upper_index}
            }))
            .unwrap(),
        )
        .unwrap();
        let source_alias = dir.path().join("source-alias.json");
        std::os::unix::fs::symlink(&source, &source_alias).unwrap();
        let cache = ImageServiceCache::new(None);
        // Hold image opening at an actual await after Reuse has materialized
        // its config, while Consume could otherwise move its data/index pair.
        let _opening_blocker = cache.services.lock().await;
        let (ctrl_ring, _worker) = storage_util::io_ring::spawn_io_ring_worker(0);
        let devices = DashMap::new();
        let runtime_dirs = [
            dir.path().join("reuse"),
            dir.path().join("reuse-alias"),
            dir.path().join("consume"),
        ];
        let create = |index: usize| {
            handle_create_overlaybd_runtime_device(
                MaterializeOverlaybdRuntimeRequest {
                    image_service_cache: &cache,
                    source_image_config: if index == 0 { &source } else { &source_alias },
                    global_config: &source,
                    runtime_dir: runtime_dirs[index].as_path(),
                    read_only: false,
                    runtime_upper_mode: UpperMode::LogStructured,
                    requested_virtual_size: None,
                    known_source_virtual_size: Some(8192),
                    resize_tool: None,
                    resize_global_config: &source,
                    resize_permit: Arc::new(Mutex::new(())),
                    allow_shrink: false,
                    source_state_strategy: if index < 2 {
                        SourceStateStrategy::Reuse
                    } else {
                        SourceStateStrategy::Consume
                    },
                },
                ctrl_ring.clone(),
                &devices,
                &None,
            )
        };
        let mut reuse = std::pin::pin!(create(0));
        poll_fn(|cx| {
            assert!(reuse.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(runtime_dirs[0].join("image.json").is_file());
        let mut reuse_alias = std::pin::pin!(create(1));
        poll_fn(|cx| {
            assert!(reuse_alias.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(
            runtime_dirs[1].join("image.json").is_file(),
            "Reuse requests should open concurrently"
        );
        let mut consume = std::pin::pin!(create(2));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut consume)
                .await
                .is_err()
        );
        assert!(
            upper_data.is_file(),
            "Consume moved data before Reuse opened it"
        );
        assert!(
            upper_index.is_file(),
            "Consume moved index before Reuse opened it"
        );
        assert!(
            !runtime_dirs[2].exists(),
            "Consume must wait before materialization"
        );
    }

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
    #[ignore = "requires root and the Linux ublk driver"]
    async fn rejected_pool_acquire_waits_for_workers_before_removing_device() -> Result<()> {
        let dir = TempDir::new()?;
        let pool = PoolState::new(
            test_pool_config(),
            0,
            test_image_service(dir.path()).await,
            dir.path().join("placeholders"),
        );
        let (config, image) = pool.placeholder_for(8 * 1024 * 1024).await?;
        let (ctrl_ring, _worker) = storage_util::io_ring::spawn_io_ring_worker(0);
        let dev = create_new_device(ctrl_ring.clone(), &config, &image).await?;
        let dev_id = dev.dev_id();
        let (finished, pending) = tokio::sync::oneshot::channel();
        dev.inner.queue_threads.lock().unwrap().push(pending);
        assert!(
            return_or_stop_idle_device(&pool, ctrl_ring.clone(), dev, Arc::clone(&image)).await
        );

        let mut invalid = ImageFile::open(
            serde_json::from_slice(&std::fs::read(&config)?)?,
            pool.image_service.clone(),
            Some(config.clone()),
        )
        .await?;
        invalid.block_size = 3;
        let invalid = Arc::new(invalid);
        let slow_worker = async {
            tokio::time::sleep(Duration::from_secs(6)).await;
            let mut ctrl = UVMUblkCtrlBuilder::new()
                .dev_id(dev_id)
                .build(ctrl_ring.clone())?;
            let retained = ctrl.get_dev_info().await.is_ok();
            let _ = finished.send(Ok(()));
            Ok::<_, anyhow::Error>(retained)
        };
        let (result, retained_while_stopping) = tokio::join!(
            prepare_overlaybd_device(&pool, &ctrl_ring, &config, &invalid, "exclusive"),
            slow_worker
        );
        let rejected = result.is_err();
        drop(result);
        let mut ctrl = UVMUblkCtrlBuilder::new()
            .dev_id(dev_id)
            .build(ctrl_ring.clone())?;
        let retained = ctrl.get_dev_info().await.is_ok();
        if retained {
            // Leave no device behind when this regression check fails.
            ctrl.stop_dev().await?;
            delete_dev(ctrl_ring.clone(), dev_id).await?;
        }
        assert!(rejected);
        assert!(
            retained_while_stopping?,
            "device removed before workers stopped"
        );
        assert!(!retained, "rejected acquire left an unmanaged ublk device");
        assert_eq!(pool.idle.len(), 0);

        let (dev, _) =
            prepare_overlaybd_device(&pool, &ctrl_ring, &config, &image, "exclusive").await?;
        stop_overlaybd_device(ctrl_ring, dev).await;
        Ok(())
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

    fn test_handoff() -> DaemonResponse {
        DaemonResponse::DeviceAcquired {
            dev_id: 7,
            image_config_path: PathBuf::from("/tmp/image.json"),
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
