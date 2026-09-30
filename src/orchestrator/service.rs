use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use tokio::sync::{broadcast, oneshot, watch, Mutex, OnceCell, RwLock, Semaphore};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, info, trace, warn};

use crate::cfg::{ConfigManager, OrchestratorConfig};
use crate::disk_policy::{DiskAdmissionReason, DiskPolicyController};
use crate::image::cache::{
    local_image_services_from_global_config, RuntimeImageOwner, RuntimeImageRefs,
};
use crate::sandbox::{
    CustomExtensionClient, CustomExtensionParams, EnvdAccessToken, FirecrackerSandboxFactory,
    FreshSandboxBuildSpec, PausedSandboxState, RuntimeArtifactSet, SandboxAccessTokenGenerator,
    SandboxBackend, SandboxBackendFactory, SandboxCaptureError, SandboxForkSpec,
    SandboxLaunchConfig, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::snapshot::SnapshotRuntimeVersions;
use crate::types::{bytes_to_mib_ceil, SandboxId, SandboxResources};
use crate::volume::VolumeManager;

use super::launch_plan::{CreateLaunchSource, LaunchPlan};
use super::metrics::{
    aggregate_resource_metrics, OrchestratorCounters, OrchestratorMetrics, SandboxContribution,
};
use super::persistence::{
    DisabledSandboxPersister, FileBackedSandboxPersister, PersistenceResult, SandboxPersister,
};
use super::proxy::{ProxyLookupResult, ProxyRoute, ProxyRouteTable, ProxyTarget};
use super::store::*;
use super::types::{
    CreateSandboxRequest, SandboxForkChildSpec, SandboxLaunchSource, SandboxLifecycleEvent,
    SandboxLifecycleEventType, SandboxState, SnapshotCaptureResult,
};
use super::{OrchestratorError, Result, SandboxForkOutcome, SandboxOperation};

#[path = "sandbox_metrics.rs"]
mod sandbox_metrics;
use sandbox_metrics::SandboxMetrics;

type SandboxHandle = Arc<Mutex<Box<dyn SandboxBackend>>>;
#[derive(Default)]
enum DeleteProgress {
    #[default]
    Capture,
    Stop {
        capture_failed: bool,
    },
    Release {
        capture_failed: bool,
    },
    Done,
}

#[derive(Clone, Copy)]
struct LifecycleTimeouts {
    /// Maximum time to wait for a sandbox to leave a transitional state.
    /// Guards against indefinite blocking when a sandbox's in-progress operation
    /// never completes (e.g. the task holding the state panics without rolling back).
    transition: Duration,
    resume: Duration,
    backend_build: Duration,
    housekeeping: Duration,
}

impl From<&OrchestratorConfig> for LifecycleTimeouts {
    fn from(config: &OrchestratorConfig) -> Self {
        Self {
            transition: Duration::from_secs(config.transition_timeout_secs),
            resume: Duration::from_secs(config.resume_timeout_secs),
            backend_build: Duration::from_secs(config.resume_backend_build_timeout_secs),
            housekeeping: Duration::from_secs(config.resume_housekeeping_timeout_secs),
        }
    }
}

const SANDBOX_EVENT_CHANNEL_CAPACITY: usize = 1024;
#[cfg(test)]
const MAX_CONCURRENT_PAUSES: usize = 4;
const MAX_CONCURRENT_RESUMES: usize = 8;
const PERSISTENCE_CLEANUP_ATTEMPTS: usize = 3;

#[derive(Clone, Debug)]
enum ShutdownOutcome {
    Success,
    Failed(String),
}

impl ShutdownOutcome {
    fn from_result(result: Result<()>) -> Self {
        match result {
            Ok(()) => Self::Success,
            Err(OrchestratorError::InternalError(message)) => Self::Failed(message),
            Err(err) => Self::Failed(err.to_string()),
        }
    }

    fn as_result(&self) -> Result<()> {
        match self {
            Self::Success => Ok(()),
            Self::Failed(message) => Err(OrchestratorError::InternalError(message.clone())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailedLaunchStage {
    BackendBuilt,
    Registered,
    TransitionalPersisted,
    RunningPersisted,
}

impl FailedLaunchStage {
    fn rollback_expected_state(self, plan: &LaunchPlan) -> Option<SandboxState> {
        match self {
            Self::BackendBuilt => Some(plan.transitional_state()),
            Self::Registered => None,
            Self::TransitionalPersisted => Some(plan.transitional_state()),
            Self::RunningPersisted => Some(SandboxState::Running),
        }
    }

    fn should_detach_proxy_route(self) -> bool {
        matches!(self, Self::RunningPersisted)
    }
}

#[derive(Default)]
struct LaunchProgress {
    handle: Option<SandboxHandle>,
    stage: Option<FailedLaunchStage>,
}

pub struct Orchestrator<
    S: MetadataStore = InMemoryMetadataStore,
    F: SandboxBackendFactory = FirecrackerSandboxFactory,
    P: SandboxPersister = FileBackedSandboxPersister,
> {
    store: S,
    factory: F,
    persister: P,
    sandboxes: RwLock<HashMap<SandboxId, SandboxHandle>>,
    template_build_ids: RwLock<HashSet<SandboxId>>,
    deletions: Mutex<HashMap<SandboxId, Arc<Mutex<DeleteProgress>>>>,
    proxy_routes: RwLock<ProxyRouteTable>,
    next_proxy_route_version: AtomicU64,
    counters: OrchestratorCounters,
    sandbox_metrics: Mutex<SandboxMetrics>,
    sandbox_event_tx: broadcast::Sender<SandboxLifecycleEvent>,
    default_sandbox_timeout: Duration,
    timeouts: LifecycleTimeouts,
    is_shutting_down: std::sync::atomic::AtomicBool,
    shutdown_tx: watch::Sender<bool>,
    shutdown_outcome: OnceCell<ShutdownOutcome>,
    image_refs: Arc<dyn RuntimeImageRefs>,
    access_tokens: SandboxAccessTokenGenerator,
    volume_manager: Option<Arc<VolumeManager>>,
    image_gc_ready: AtomicBool,
    pause_permits: Semaphore,
    resume_permits: Semaphore,
    disk_policy: Arc<DiskPolicyController>,
    serial_log_dir: Option<PathBuf>,
    log_retention: Duration,
    reclaimed_log_bytes: AtomicU64,
    disk_admission_rejections: AtomicU64,
}

impl Orchestrator<InMemoryMetadataStore, FirecrackerSandboxFactory, DisabledSandboxPersister> {
    pub async fn with_in_memory_store() -> Arc<Self> {
        Self::new(
            InMemoryMetadataStore::new(),
            FirecrackerSandboxFactory::new(),
            DisabledSandboxPersister,
        )
        .await
        .expect("in-memory orchestrator should never fail to initialize")
    }
}

impl<F> Orchestrator<InMemoryMetadataStore, F>
where
    F: SandboxBackendFactory,
{
    pub async fn with_file_backed_store_and_factory(factory: F) -> Result<Arc<Self>> {
        let config = ConfigManager::global_config();
        let store = InMemoryMetadataStore::new();
        let persister = FileBackedSandboxPersister::new(
            config.orchestrator.persisted_sandbox_store_path.clone(),
            config.virtualization_mode,
        )
        .with_cleanup_journal_reserve_bytes(
            config
                .disk_policy
                .cleanup_journal_reserve_mb
                .saturating_mul(1024 * 1024),
        );
        Self::new(store, factory, persister).await
    }

    pub async fn with_file_backed_store_factory_and_volumes(
        factory: F,
        volume_manager: Arc<VolumeManager>,
    ) -> Result<Arc<Self>> {
        let config = ConfigManager::global_config();
        let store = InMemoryMetadataStore::new();
        let persister = FileBackedSandboxPersister::new(
            config.orchestrator.persisted_sandbox_store_path.clone(),
            config.virtualization_mode,
        );
        let image_refs = local_image_services_from_global_config().runtime_refs;
        Self::new_inner_with_volumes(store, factory, persister, image_refs, Some(volume_manager))
            .await
    }
}

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    pub async fn new(store: S, factory: F, persister: P) -> Result<Arc<Self>> {
        let image_refs = local_image_services_from_global_config().runtime_refs;
        Self::new_inner(
            store,
            factory,
            persister,
            image_refs,
            LifecycleTimeouts::from(&ConfigManager::global_config().orchestrator),
        )
        .await
    }

    async fn new_inner(
        store: S,
        factory: F,
        persister: P,
        image_refs: Arc<dyn RuntimeImageRefs>,
        timeouts: LifecycleTimeouts,
    ) -> Result<Arc<Self>> {
        Self::new_inner_with_volumes(store, factory, persister, image_refs, None).await
    }

    async fn new_inner_with_volumes(
        store: S,
        factory: F,
        persister: P,
        image_refs: Arc<dyn RuntimeImageRefs>,
        volume_manager: Option<Arc<VolumeManager>>,
    ) -> Result<Arc<Self>> {
        let app_config = ConfigManager::global_config();
        let config = &app_config.orchestrator;
        let disk_policy = Arc::new(DiskPolicyController::from_config(app_config));
        disk_policy
            .initialize(persister.cleanup_metrics().pending)
            .await
            .map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "initialize runtime disk policy: {error:#}"
                ))
            })?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (sandbox_event_tx, _sandbox_event_rx) =
            broadcast::channel(SANDBOX_EVENT_CHANNEL_CAPACITY);

        // Restore persisted sandboxes from the previous run, keeping the paused
        // ones (with their state) for the paused-protection reconcile below.
        let persisted = persister.load_all(&factory).await?;
        let managed_seed_must_exist = persisted_sandboxes_require_managed_seed(&persisted);
        let access_tokens = tokio::task::spawn_blocking(move || {
            SandboxAccessTokenGenerator::load_or_create(app_config, managed_seed_must_exist)
        })
        .await
        .context("join sandbox access-token seed loader")??;
        let restored_paused: Vec<(SandboxId, Arc<dyn PausedSandboxState>)> = persisted
            .iter()
            .filter(|metadata| metadata.state == SandboxState::Paused)
            .filter_map(|metadata| {
                metadata
                    .paused_state
                    .as_ref()
                    .map(|paused_state| (metadata.id, Arc::clone(paused_state)))
            })
            .collect();
        let all_paused_records_resumable = persister.image_gc_safe()
            && persisted
                .iter()
                .filter(|metadata| metadata.state == SandboxState::Paused)
                .all(|metadata| metadata.paused_state.is_some());
        for metadata in persisted {
            store.add(metadata).await?;
        }

        let orchestrator = Arc::new(Self {
            store,
            factory,
            persister,
            sandboxes: RwLock::new(HashMap::new()),
            template_build_ids: RwLock::new(HashSet::new()),
            deletions: Mutex::new(HashMap::new()),
            proxy_routes: RwLock::new(ProxyRouteTable::default()),
            next_proxy_route_version: AtomicU64::new(1),
            counters: OrchestratorCounters::default(),
            sandbox_metrics: Mutex::new(SandboxMetrics::default()),
            sandbox_event_tx,
            default_sandbox_timeout: Duration::from_secs(config.default_sandbox_timeout_secs),
            timeouts,
            is_shutting_down: std::sync::atomic::AtomicBool::new(false),
            shutdown_tx,
            shutdown_outcome: OnceCell::new(),
            image_refs,
            access_tokens,
            volume_manager,
            image_gc_ready: AtomicBool::new(false),
            pause_permits: Semaphore::new(config.max_concurrent_pauses),
            resume_permits: Semaphore::new(MAX_CONCURRENT_RESUMES),
            disk_policy,
            serial_log_dir: app_config.firecracker.serial_dir.clone(),
            log_retention: Duration::from_secs(app_config.disk_policy.log_retention_secs),
            reclaimed_log_bytes: AtomicU64::new(0),
            disk_admission_rejections: AtomicU64::new(0),
        });

        Self::start_metrics_task(&orchestrator);

        // Start the auto-evict task.
        let evict_interval = Duration::from_millis(config.auto_evict_interval_ms);
        Self::start_auto_evict_task(Arc::clone(&orchestrator), evict_interval, shutdown_rx);

        // Reconcile durable paused protection before any deleting image-cache
        // path can run. An incomplete legacy/corrupt record disables image GC.
        let gc = app_config.image.cache.gc_schedule();
        let image_gc_ready = if all_paused_records_resumable {
            match orchestrator
                .reconcile_paused_at_startup(&restored_paused)
                .await
            {
                Ok(()) => true,
                Err(error) => {
                    warn!(
                        error = %error,
                        "local image protection reconcile failed at startup; disabling deleting image GC (fail-closed)"
                    );
                    false
                }
            }
        } else {
            warn!(
                "one or more paused records have incomplete runtime closures; disabling deleting image GC (fail-closed)"
            );
            false
        };
        orchestrator
            .image_gc_ready
            .store(image_gc_ready, Ordering::Release);

        let disk_interval = Duration::from_secs(app_config.disk_policy.poll_interval_secs);
        Self::start_disk_policy_task(
            Arc::clone(&orchestrator),
            disk_interval,
            orchestrator.shutdown_tx.subscribe(),
        );

        if gc.enabled && image_gc_ready {
            Self::start_local_image_maintenance_task(
                Arc::clone(&orchestrator),
                gc.interval,
                orchestrator.shutdown_tx.subscribe(),
            );
        }

        Ok(orchestrator)
    }

    async fn run_cancellation_safe<T>(
        self: &Arc<Self>,
        operation: &'static str,
        sandbox_id: SandboxId,
        future: impl std::future::Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = future.await;
            if tx.send(result).is_err() {
                debug!(
                    sandbox_id = %sandbox_id,
                    operation,
                    "operation completed after caller stopped waiting"
                );
            }
        });

        rx.await.map_err(|err| {
            OrchestratorError::InternalError(format!(
                "operation task ended before reporting result: {err}"
            ))
        })?
    }

    async fn protect_image_refs(
        &self,
        owner: RuntimeImageOwner,
        artifacts: RuntimeArtifactSet,
        context: &'static str,
    ) -> Result<()> {
        self.image_refs
            .pin(owner, artifacts)
            .await
            .map_err(|error| {
                OrchestratorError::InternalError(format!("pin {context} image refs: {error:#}"))
            })
    }

    async fn release_image_refs(&self, owner: RuntimeImageOwner) {
        self.image_refs.unpin_best_effort(owner).await;
    }

    async fn publish_sandbox_volume_backings(
        &self,
        sandbox_id: SandboxId,
        volume_ids: &[String],
    ) -> Result<()> {
        let Some(manager) = self.volume_manager.as_ref() else {
            return Ok(());
        };
        manager
            .recover_and_publish_backings(&sandbox_id.to_string(), volume_ids)
            .await
            .map_err(|error| OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::SnapshotVolumes,
                source: error.into(),
            })
    }

    async fn finalize_terminal_volumes(&self, metadata: &SandboxMetadata) {
        let Some(manager) = self.volume_manager.as_ref() else {
            return;
        };
        let volume_ids = metadata.volume_mounts.values().cloned().collect::<Vec<_>>();
        if volume_ids.is_empty() {
            return;
        }
        let owner = metadata.id.to_string();
        if let Err(error) = manager
            .recover_and_publish_backings(&owner, &volume_ids)
            .await
        {
            warn!(sandbox_id = %metadata.id, %error, "failed to publish volumes during terminal sandbox cleanup");
        }
        if let Err(error) = manager.replace_owner_for(&owner, None, &volume_ids).await {
            warn!(sandbox_id = %metadata.id, %error, "failed to release volumes during terminal sandbox cleanup");
        }
    }

    async fn cleanup_persisted_sandbox_state(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<()> {
        let mut last_error = None;
        for attempt in 1..=PERSISTENCE_CLEANUP_ATTEMPTS {
            match self.persister.delete_record_and_artifacts(sandbox_id).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    warn!(
                        error = ?err,
                        attempt,
                        max_attempts = PERSISTENCE_CLEANUP_ATTEMPTS,
                        "persisted sandbox cleanup attempt failed"
                    );
                    last_error = Some(err);
                }
            }
        }
        Err(last_error.expect("cleanup attempts is non-zero"))
    }

    /// Snapshot the running set's local runtime artifacts for maintenance.
    async fn collect_running_artifacts(&self) -> Vec<(SandboxId, RuntimeArtifactSet)> {
        let handles = {
            self.sandboxes
                .read()
                .await
                .iter()
                .map(|(sandbox_id, handle)| (*sandbox_id, Arc::clone(handle)))
                .collect::<Vec<_>>()
        };
        let mut running = Vec::with_capacity(handles.len());
        for (sandbox_id, handle) in handles {
            let artifacts = {
                let sandbox = handle.lock().await;
                sandbox.runtime_info().runtime_artifacts
            };
            running.push((sandbox_id, artifacts));
        }
        running
    }

    /// Fail-closed startup reconcile before maintenance can run: durably protect
    /// every restored paused sandbox, then drop orphaned paused protection.
    async fn reconcile_paused_at_startup(
        &self,
        restored_paused: &[(SandboxId, Arc<dyn PausedSandboxState>)],
    ) -> Result<()> {
        let mut live_paused = Vec::with_capacity(restored_paused.len());
        for (sandbox_id, paused_state) in restored_paused {
            self.protect_image_refs(
                RuntimeImageOwner::PausedSandbox(*sandbox_id),
                paused_state.runtime_artifacts(),
                "paused sandbox",
            )
            .await?;
            live_paused.push(*sandbox_id);
        }
        self.image_refs
            .reconcile_paused(&live_paused)
            .await
            .map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "reconcile local image protection: {error:#}"
                ))
            })
    }

    /// Creates and starts a new sandbox from a resolved launch source.
    ///
    /// This call only returns after the sandbox is fully ready and persisted
    /// as `Running`, so callers can treat a successful return as immediately
    /// usable without additional polling.
    pub async fn create_sandbox(
        self: &Arc<Self>,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        let sandbox_id = SandboxId::new();
        let this = Arc::clone(self);
        self.run_cancellation_safe("create", sandbox_id, async move {
            this.create_sandbox_inner(sandbox_id, request, false).await
        })
        .await
    }

    pub(crate) async fn create_template_builder(
        self: &Arc<Self>,
        build_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("create_builder", build_id, async move {
            this.create_sandbox_inner(build_id, request, true).await
        })
        .await
    }

    #[tracing::instrument(
        name = "create_sandbox",
        skip(self, request),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn create_sandbox_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
        template_builder: bool,
    ) -> Result<SandboxMetadata> {
        if let Err(err) = self.ensure_accepting_lifecycle_operations() {
            self.counters.record_create_fail(1);
            return Err(err);
        }
        if let Err(err) = self.ensure_disk_admission("create") {
            self.counters.record_create_fail(1);
            return Err(err);
        }

        let CreateSandboxRequest {
            source,
            timeout,
            timeout_action,
            user_metadata,
            env_vars,
            auto_resume,
            network_policy,
            custom_extension_params,
            secure,
            volume_mounts,
            extra_drives: launch_extra_drives,
            extra_drives_in_snapshot,
        } = request;
        let envd_access_token = secure.then(|| self.access_tokens.generate(sandbox_id));
        info!(timeout = ?timeout, "creating sandbox");

        let result = match source {
            SandboxLaunchSource::Snapshot(snapshot) => {
                let record = snapshot.record();
                let committed = snapshot.committed();
                let configured_mode = ConfigManager::global_config().virtualization_mode;
                if committed.virtualization_mode != configured_mode {
                    self.counters.record_create_fail(1);
                    return Err(OrchestratorError::VirtualizationModeMismatch {
                        resource: format!("snapshot {}", record.id),
                        resource_mode: committed.virtualization_mode,
                        node_mode: configured_mode,
                    });
                }
                let launch_image_configs = committed.image_configs.clone();
                let mut extra_mmds = serde_json::Map::new();
                if !launch_image_configs.is_empty() {
                    extra_mmds.insert("imageConfigs".to_string(), launch_image_configs.to_value());
                };
                // Effective custom config: a launch-provided value overrides the
                // one persisted in the source snapshot; otherwise inherit it.
                // Store the effective value so publishing a snapshot from this
                // sandbox keeps the inherited config instead of dropping it.
                let effective_custom_extension_params = custom_extension_params
                    .clone()
                    .or_else(|| committed.custom_extension_params.clone());
                let launch_config = SandboxLaunchConfig {
                    sandbox_id,
                    snapshot_id: record.id.to_string(),
                    env_vars,
                    network: network_policy.runtime_policy(),
                    extra_mmds,
                    custom_extension_params: effective_custom_extension_params.clone(),
                    envd_access_token: envd_access_token.clone(),
                    extra_drives: launch_extra_drives.clone(),
                    extra_drives_in_snapshot,
                };

                let transitional_metadata = SandboxMetadata {
                    id: sandbox_id,
                    template_builder,
                    snapshot_id: record.id.to_string(),
                    snapshot_alias: record.alias.as_ref().map(ToString::to_string),
                    virtualization_mode: committed.virtualization_mode,
                    runtime_versions: committed.runtime_versions.clone(),
                    resources: *snapshot.resources(),
                    context: committed.context.clone(),
                    startup: committed.startup.clone(),
                    image_configs: launch_image_configs,
                    timeout_action,
                    auto_resume,
                    user_metadata,
                    network_policy,
                    custom_extension_params: effective_custom_extension_params,
                    volume_mounts: volume_mounts.clone(),
                    secure,
                    ..Default::default()
                };

                self.launch_sandbox(LaunchPlan::for_create_from_snapshot(
                    sandbox_id,
                    snapshot,
                    launch_config,
                    transitional_metadata,
                    NewTimeout::Set(timeout.unwrap_or(self.default_sandbox_timeout)),
                ))
                .await
            }
            SandboxLaunchSource::Image {
                image_ref,
                overlaybd_config_path,
                context,
                resources,
                mut extra_drives,
                extra_boot_args,
                image_configs,
            } => {
                let context = *context;
                extra_drives.extend(launch_extra_drives);
                let resources = resources.unwrap_or_else(default_fresh_sandbox_resources);
                let launch_image_configs = *image_configs;
                let mut extra_mmds = serde_json::Map::new();
                if !launch_image_configs.is_empty() {
                    extra_mmds.insert("imageConfigs".to_string(), launch_image_configs.to_value());
                };
                let launch_config = SandboxLaunchConfig {
                    sandbox_id,
                    snapshot_id: image_ref.clone(),
                    env_vars,
                    network: network_policy.runtime_policy(),
                    extra_mmds,
                    custom_extension_params: custom_extension_params.clone(),
                    envd_access_token,
                    extra_drives: Vec::new(),
                    extra_drives_in_snapshot: false,
                };
                let build_spec = FreshSandboxBuildSpec {
                    image_config_path: overlaybd_config_path,
                    context: context.clone(),
                    resources,
                    extra_drives,
                    extra_boot_args,
                };

                let transitional_metadata = SandboxMetadata {
                    id: sandbox_id,
                    template_builder,
                    snapshot_id: image_ref,
                    snapshot_alias: None,
                    virtualization_mode: ConfigManager::global_config().virtualization_mode,
                    runtime_versions: configured_runtime_versions(),
                    resources,
                    context,
                    image_configs: launch_image_configs,
                    timeout_action,
                    auto_resume,
                    user_metadata,
                    network_policy,
                    custom_extension_params,
                    volume_mounts,
                    secure,
                    ..Default::default()
                };

                self.launch_sandbox(LaunchPlan::for_create_fresh(
                    sandbox_id,
                    build_spec,
                    launch_config,
                    transitional_metadata,
                    NewTimeout::Set(timeout.unwrap_or(self.default_sandbox_timeout)),
                ))
                .await
            }
        };

        match result {
            Ok(metadata) => {
                self.counters.record_create_success(1);
                self.publish_sandbox_event(
                    SandboxLifecycleEventType::Create,
                    metadata.id,
                    metadata.resources,
                );
                Ok(metadata)
            }
            Err(err) => {
                self.counters.record_create_fail(1);
                Err(err)
            }
        }
    }

    /// Forks a running sandbox into multiple new sandboxes on the same node.
    pub async fn fork_sandbox(
        self: &Arc<Self>,
        source_sandbox_id: SandboxId,
        count: u32,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        if self
            .store
            .get(&source_sandbox_id)
            .await?
            .is_some_and(|metadata| !metadata.volume_mounts.is_empty())
        {
            return Err(OrchestratorError::InternalError(
                "fork with volume mounts requires volume-aware child specs".to_string(),
            ));
        }
        let child_specs = (0..count)
            .map(|_| SandboxForkChildSpec {
                sandbox_id: SandboxId::new(),
                ..Default::default()
            })
            .collect();
        self.fork_sandbox_with_specs(source_sandbox_id, child_specs, new_timeout)
            .await
    }

    pub async fn fork_sandbox_with_specs(
        self: &Arc<Self>,
        source_sandbox_id: SandboxId,
        child_specs: Vec<SandboxForkChildSpec>,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("fork", source_sandbox_id, async move {
            this.fork_sandbox_inner(source_sandbox_id, child_specs, new_timeout)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "fork_sandbox",
        skip(self, child_specs),
        fields(source_sandbox_id = %source_sandbox_id, count = child_specs.len())
    )]
    async fn fork_sandbox_inner(
        self: Arc<Self>,
        source_sandbox_id: SandboxId,
        child_specs: Vec<SandboxForkChildSpec>,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        self.ensure_accepting_lifecycle_operations()?;

        let count = u32::try_from(child_specs.len())
            .map_err(|_| OrchestratorError::InternalError("too many fork children".to_string()))?;
        let mut child_ids = HashSet::with_capacity(child_specs.len());
        for child in &child_specs {
            if child.sandbox_id == source_sandbox_id || !child_ids.insert(child.sandbox_id) {
                return Err(OrchestratorError::InternalError(
                    "fork child sandbox IDs must be unique and differ from the source".to_string(),
                ));
            }
            if self.store.get(&child.sandbox_id).await?.is_some() {
                return Err(OrchestratorError::InternalError(format!(
                    "fork child sandbox {} already exists",
                    child.sandbox_id
                )));
            }
        }
        info!("forking sandboxes");

        let source_handle = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&source_sandbox_id).cloned()
        }
        .ok_or(OrchestratorError::SandboxNotFound(source_sandbox_id))?;

        let source_metadata = self
            .store
            .update_if_state(&source_sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.state = SandboxState::Forking
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => match actual_state {
                    SandboxState::Killing => OrchestratorError::SandboxNotFound(source_sandbox_id),
                    _ => OrchestratorError::InvalidSandboxState {
                        sandbox_id: source_sandbox_id,
                        state: actual_state,
                    },
                },
                err => OrchestratorError::from(err),
            })?
            .previous;

        let backend_specs = child_specs
            .iter()
            .map(|child| SandboxForkSpec {
                sandbox_id: child.sandbox_id,
                envd_access_token: source_metadata
                    .secure
                    .then(|| self.access_tokens.generate(child.sandbox_id)),
                extra_drives: child.extra_drives.clone(),
                replace_drive_ids: child.replace_drive_ids.clone(),
            })
            .collect::<Vec<_>>();

        // Start to fork the sandbox.
        // This is a single operation that will return a list of results for each child sandbox.
        let fork_result = {
            let mut sandbox = source_handle.lock().await;
            sandbox.fork(&backend_specs).await
        };
        let forked_backends = match fork_result {
            Ok(forked_backends) => forked_backends,
            Err(err) => {
                warn!(error = ?err, "failed to fork sandbox");
                self.counters.record_create_fail(u64::from(count));
                if err.is_terminal() {
                    self.detach_sandbox_handle_and_route(&source_sandbox_id)
                        .await;
                    let _ = {
                        let mut sandbox = source_handle.lock().await;
                        sandbox.stop().await
                    };
                    self.finalize_terminal_volumes(&source_metadata).await;
                    self.store.remove(&source_sandbox_id).await?;
                } else {
                    let _ = self
                        .store
                        .update_state_if_state(
                            &source_sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Forking],
                        )
                        .await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id: source_sandbox_id,
                    operation: SandboxOperation::Fork,
                    source: err.into(),
                });
            }
        };

        // Restore the source sandbox's state to Running.
        if let Err(err) = self
            .store
            .update_state_if_state(
                &source_sandbox_id,
                SandboxState::Running,
                &[SandboxState::Forking],
            )
            .await
        {
            warn!(error = ?err, "failed to restore source sandbox metadata after fork");
        }

        // Register each forked sandbox in the store and runtime, and publish events.
        let mut outcomes = Vec::with_capacity(child_specs.len());
        let mut successes = 0u64;
        let now = SystemTime::now();
        for (child, backend) in child_specs.into_iter().zip(forked_backends) {
            let sandbox_id = child.sandbox_id;
            let backend = match backend {
                Ok(backend) => backend,
                Err(err) => {
                    warn!(%sandbox_id, error = ?err, "failed to start forked sandbox");
                    outcomes.push(Err(Self::fork_child_error(sandbox_id, err)));
                    continue;
                }
            };

            let mut metadata = source_metadata.clone();
            metadata.id = sandbox_id;
            metadata.state = SandboxState::Running;
            metadata.created_at = now;
            metadata.paused_state = None;
            metadata.volume_mounts = child.volume_mounts;
            metadata.update_timeout(new_timeout);

            let proxy_target = match Self::proxy_target_from_sandbox(backend.as_ref()) {
                Ok(proxy_target) => proxy_target,
                Err(err) => {
                    Self::stop_failed_fork(backend, sandbox_id).await;
                    outcomes.push(Err(Self::fork_child_error(
                        sandbox_id,
                        anyhow::Error::new(err),
                    )));
                    continue;
                }
            };
            if let Err(err) = self.store.add(metadata.clone()).await {
                warn!(%sandbox_id, error = ?err, "failed to register forked sandbox");
                Self::stop_failed_fork(backend, sandbox_id).await;
                outcomes.push(Err(Self::fork_child_error(
                    sandbox_id,
                    anyhow::Error::new(err),
                )));
                continue;
            }
            self.sandboxes
                .write()
                .await
                .insert(metadata.id, Arc::new(Mutex::new(backend)));
            self.upsert_proxy_route(metadata.id, proxy_target).await;
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Fork,
                metadata.id,
                metadata.resources,
            );
            successes += 1;
            outcomes.push(Ok(metadata));
        }

        self.counters.record_create_success(successes);
        self.counters
            .record_create_fail(u64::from(count) - successes);
        Ok(outcomes)
    }

    fn fork_child_error(sandbox_id: SandboxId, source: anyhow::Error) -> OrchestratorError {
        OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::Fork,
            source,
        }
    }

    async fn stop_failed_fork(mut backend: Box<dyn SandboxBackend>, sandbox_id: SandboxId) {
        if let Err(err) = backend.stop().await {
            warn!(%sandbox_id, error = ?err, "failed to stop unsuccessful fork");
        }
    }

    /// Retrieves the metadata for a sandbox by its ID.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id))]
    pub async fn get_sandbox(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        Ok(self.store.get(sandbox_id).await?)
    }

    /// Lists all sandboxes with their metadata.
    #[tracing::instrument(skip(self))]
    pub async fn list_sandboxes(&self) -> Result<Vec<SandboxMetadata>> {
        self.list_sandboxes_filtered(SandboxListFilter::matches_all())
            .await
    }

    /// Lists all sandbox IDs currently tracked by the store.
    pub async fn list_sandbox_ids(&self) -> Result<Vec<SandboxId>> {
        // Reserve builder routing while its image is resolving and while the
        // final template is publishing, even when no VM is currently running.
        let mut ids: HashSet<_> = self.store.list_ids().await?.into_iter().collect();
        ids.extend(self.template_build_ids.read().await.iter().copied());
        Ok(ids.into_iter().collect())
    }

    pub(crate) async fn register_template_build(&self, id: SandboxId) {
        self.template_build_ids.write().await.insert(id);
    }

    pub(crate) async fn unregister_template_build(&self, id: SandboxId) {
        self.template_build_ids.write().await.remove(&id);
    }

    /// Lists sandboxes that match the provided filter criteria:
    /// - If `states` is provided, only sandboxes in those states will be included.
    /// - If `user_metadata` is provided, only sandboxes whose user metadata contains
    ///   all the specified key-value pairs will be included.
    /// - If `started_after` is provided, only sandboxes created at or after that instant
    ///   will be included.
    /// - If `template` is provided, only sandboxes using the matching snapshot ID or alias
    ///   will be included.
    #[tracing::instrument(skip(self, filter))]
    pub async fn list_sandboxes_filtered(
        &self,
        filter: SandboxListFilter,
    ) -> Result<Vec<SandboxMetadata>> {
        Ok(self
            .store
            .list_filtered(filter)
            .await?
            .into_iter()
            .filter(|metadata| !metadata.template_builder)
            .collect())
    }

    pub fn get_envd_access_token(&self, metadata: &SandboxMetadata) -> Option<EnvdAccessToken> {
        metadata
            .secure
            .then(|| self.access_tokens.generate(metadata.id))
    }

    pub fn validate_envd_access_token(&self, sandbox_id: SandboxId, candidate: &str) -> bool {
        self.access_tokens.matches(sandbox_id, candidate)
    }

    pub fn traffic_access_token(&self, sandbox_id: SandboxId) -> String {
        self.access_tokens.generate_traffic(sandbox_id)
    }

    pub fn validate_traffic_access_token(&self, sandbox_id: SandboxId, candidate: &str) -> bool {
        self.access_tokens.matches_traffic(sandbox_id, candidate)
    }

    /// Resolves the current proxyability of a sandbox without touching the sandbox mutex.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id))]
    pub async fn proxy_lookup_for(&self, sandbox_id: &SandboxId) -> Result<ProxyLookupResult> {
        if let Some(route) = self.proxy_routes.read().await.route(sandbox_id).cloned() {
            trace!(
                version = route.version(),
                "resolved running proxy target from runtime table"
            );
            return Ok(ProxyLookupResult::Ready(route.target().clone()));
        }

        let metadata = self.store.get(sandbox_id).await?;
        Ok(match metadata {
            None => {
                debug!("sandbox has no runtime route or persisted metadata");
                ProxyLookupResult::NotFound
            }
            Some(metadata) if metadata.state == SandboxState::Running => {
                warn!("running sandbox is missing a runtime proxy route");
                ProxyLookupResult::RouteMissing
            }
            Some(metadata) if metadata.state == SandboxState::Paused => {
                debug!(auto_resume = metadata.auto_resume, "sandbox is paused");
                ProxyLookupResult::Paused {
                    auto_resume: metadata.auto_resume,
                }
            }
            Some(metadata) => {
                debug!(state = ?metadata.state, "sandbox exists but is not proxyable");
                ProxyLookupResult::Unavailable(metadata.state)
            }
        })
    }

    /// Updates the keep-alive timeout for a RUNNING sandbox.
    /// If `timeout` is `None`, default timeout will be applied.
    /// If `allow_shorter` is `false`, the update will be skipped if the new TTL is not longer than the existing TTL.
    ///
    /// When the sandbox is in a transitional state that may resolve to `Running`,
    /// this method waits for the transition to complete before re-evaluating the state.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id, allow_shorter = allow_shorter))]
    pub async fn keep_alive_for(
        &self,
        sandbox_id: SandboxId,
        timeout: Option<Duration>,
        allow_shorter: bool,
    ) -> Result<Option<SandboxMetadata>> {
        self.ensure_accepting_lifecycle_operations()?;

        if timeout.is_none() {
            debug!("applying default timeout for keep-alive");
        } else {
            debug!(?timeout, "updating keep-alive timeout");
        }
        let valid_timeout = timeout.unwrap_or(self.default_sandbox_timeout);

        let mut metadata = match self.store.get(&sandbox_id).await? {
            Some(metadata) => metadata,
            None => return Err(OrchestratorError::SandboxNotFound(sandbox_id)),
        };

        // If the sandbox is in a transitional state that may lead to Running,
        // wait for the transition to complete before checking whether the
        // keep-alive is applicable.
        if matches!(
            metadata.state,
            SandboxState::Creating
                | SandboxState::Resuming
                | SandboxState::Snapshotting
                | SandboxState::Forking
        ) {
            debug!(state = ?metadata.state, "sandbox in transitional state, waiting before applying keep-alive");
            metadata = self.wait_for_transition(sandbox_id, metadata.state).await?;
        }

        if metadata.state != SandboxState::Running {
            info!(state = ?metadata.state, "cannot update keep-alive timeout in non-running state");
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        let mut timeout_updated = false;
        let update_result = self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                let new_expire_time = SystemTime::now().checked_add(valid_timeout);
                if !allow_shorter {
                    if let Some(current_expire) = metadata.expires_at {
                        if let Some(new_expire) = new_expire_time {
                            if new_expire <= current_expire {
                                info!(
                                    current_expire = ?current_expire,
                                    new_expire = ?new_expire,
                                    "new timeout is not longer than current timeout, skipping update",
                                );
                                return;
                            }
                        }
                    }
                }

                metadata.set_timeout(Some(valid_timeout));
                timeout_updated = true;
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => {
                    info!(state = ?actual_state, "keep-alive update failed due to state conflict");
                    OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }
                }
                other => OrchestratorError::from(other),
            })?;
        if timeout_updated {
            info!(?valid_timeout, "sandbox keep-alive timeout updated");
        }

        Ok(Some(update_result.current))
    }

    /// Stops and deletes the sandbox with the given ID.
    ///
    /// If the sandbox is currently in a transitional state, this method waits for
    /// the in-progress operation to finish before proceeding with deletion, preventing
    /// races where an ongoing operation might overwrite the `Killing` state.
    pub async fn delete_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("delete", sandbox_id, async move {
            this.delete_sandbox_inner(sandbox_id).await
        })
        .await
    }

    async fn deletion_progress(&self, sandbox_id: SandboxId) -> Arc<Mutex<DeleteProgress>> {
        self.deletions
            .lock()
            .await
            .entry(sandbox_id)
            .or_default()
            .clone()
    }

    #[tracing::instrument(
        name = "delete_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn delete_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        info!("deleting sandbox");
        let deletion = self.deletion_progress(sandbox_id).await;
        let mut progress = deletion.lock().await;
        match *progress {
            DeleteProgress::Done => return Ok(()),
            DeleteProgress::Capture => {}
            _ => {
                return self
                    .delete_sandbox_impl(sandbox_id, SandboxState::Killing, &mut progress)
                    .await
            }
        }

        if self.store.get(&sandbox_id).await?.is_none() {
            if self.persister.delete_if_persisted(&sandbox_id).await? {
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                    .await;
                return Ok(());
            }
            if self.persister.cleanup_metrics().pending > 0 {
                self.cleanup_persisted_sandbox_state(&sandbox_id).await?;
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                    .await;
                return Ok(());
            }
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        }

        // Attempt to transition to Killing, retrying after waiting whenever we
        // find the sandbox in a transitional state.
        let previous_state = loop {
            match self
                .store
                .update_state_if_state(
                    &sandbox_id,
                    SandboxState::Killing,
                    &[SandboxState::Running, SandboxState::Paused],
                )
                .await
            {
                Ok(previous_state) => break previous_state,
                Err(StoreError::StateConflict { actual_state, .. }) => match actual_state {
                    SandboxState::Killing => {
                        debug!("sandbox already in killing state, waiting for delete to finish");
                        match self
                            .wait_for_transition(sandbox_id, SandboxState::Killing)
                            .await
                        {
                            Ok(_) => {
                                // The in-flight delete rolled back to a stable state.
                                // Retry the Killing CAS rather than letting multiple
                                // deleters run concurrently.
                                continue;
                            }
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                info!("sandbox was deleted by a concurrent delete");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
                    | SandboxState::Pausing
                    | SandboxState::Resuming => {
                        // An in-progress operation is currently holding the sandbox in this
                        // transitional state.  Wait for it to finish so our Killing transition
                        // doesn't race with the final state write from that operation.
                        debug!(
                            state = ?actual_state,
                            "sandbox in transitional state, waiting before deletion"
                        );
                        match self.wait_for_transition(sandbox_id, actual_state).await {
                            Ok(_) => {
                                // Transition finished; retry the Killing CAS.
                                continue;
                            }
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                // Sandbox was removed while we waited (e.g. by
                                // another concurrent delete).
                                info!("sandbox was deleted while waiting for transitional state");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    _ => {
                        return Err(OrchestratorError::from(StoreError::StateConflict {
                            sandbox_id,
                            expected_states: vec![SandboxState::Running, SandboxState::Paused],
                            actual_state,
                        }));
                    }
                },
                Err(err) => {
                    if matches!(err, StoreError::SandboxNotFound { .. }) {
                        self.deletions.lock().await.remove(&sandbox_id);
                    }
                    return Err(OrchestratorError::from(err));
                }
            }
        };

        self.delete_sandbox_impl(sandbox_id, previous_state, &mut progress)
            .await
    }

    async fn claim_expired_running_sandbox(
        &self,
        sandbox_id: SandboxId,
        cutoff: SystemTime,
        claimed_state: SandboxState,
    ) -> Result<bool> {
        match self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                if metadata.is_expired(cutoff) {
                    metadata.state = claimed_state;
                }
            })
            .await
        {
            Ok(update) if update.current.state == claimed_state => Ok(true),
            Ok(update) => {
                debug!(
                    expires_at = ?update.current.expires_at,
                    ?cutoff,
                    "skipping auto-eviction because sandbox expiry was updated"
                );
                Ok(false)
            }
            Err(StoreError::StateConflict { actual_state, .. }) => {
                debug!(
                    state = ?actual_state,
                    "skipping auto-eviction because sandbox state changed"
                );
                Ok(false)
            }
            Err(StoreError::SandboxNotFound { .. }) => {
                debug!("skipping auto-eviction because sandbox no longer exists");
                Ok(false)
            }
            Err(err) => Err(OrchestratorError::from(err)),
        }
    }

    async fn delete_sandbox_impl(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        previous_state: SandboxState,
        progress: &mut DeleteProgress,
    ) -> Result<()> {
        let metadata = self.store.get(&sandbox_id).await?;
        let optional_cache = metadata
            .as_ref()
            .is_some_and(|record| record.template_builder);
        let volume_ids = metadata
            .map(|metadata| metadata.volume_mounts.into_values().collect::<Vec<_>>())
            .unwrap_or_default();
        let (handle, removed_route) = self.detach_sandbox_handle_and_route(&sandbox_id).await;
        let mut capture_error = None;

        if matches!(progress, DeleteProgress::Capture) {
            let mut volumes_frozen = false;
            let capture_result: std::result::Result<(), SandboxCaptureError> = async {
                if let Some(handle) = handle.as_ref() {
                    if previous_state == SandboxState::Running && !volume_ids.is_empty() {
                        handle.lock().await.freeze_and_snapshot_volumes().await?;
                        volumes_frozen = true;
                    }
                }
                self.publish_sandbox_volume_backings(sandbox_id, &volume_ids)
                    .await
                    .map_err(|error| SandboxCaptureError::recoverable(error.into()))
            }
            .await;
            if let Err(mut error) = capture_result {
                if volumes_frozen && !error.is_terminal() && !optional_cache {
                    if let Some(handle) = handle.as_ref() {
                        if let Err(thaw_error) = handle.lock().await.thaw_volumes().await {
                            error = SandboxCaptureError::terminal(anyhow::anyhow!(
                                "delete failed: {error}; thaw failed: {thaw_error:#}"
                            ));
                        }
                    }
                }
                // Builder caches are optional; still stop and release the worker on capture failure.
                if !error.is_terminal() && !optional_cache {
                    if let Some(handle) = handle {
                        self.sandboxes.write().await.insert(sandbox_id, handle);
                    }
                    self.restore_proxy_route(sandbox_id, removed_route).await;
                    self.store
                        .update_state_if_state(
                            &sandbox_id,
                            previous_state,
                            &[SandboxState::Killing],
                        )
                        .await?;
                    return Err(OrchestratorError::SandboxOperationFailed {
                        sandbox_id,
                        operation: SandboxOperation::Stop,
                        source: error.into(),
                    });
                }
                warn!(
                    ?error,
                    "volume capture failed; stopping sandbox and failing its volumes"
                );
                capture_error = Some(error);
            }
            *progress = DeleteProgress::Stop {
                capture_failed: capture_error.is_some(),
            };
        }

        let cleanup: anyhow::Result<()> = async {
            if let DeleteProgress::Stop { capture_failed } = *progress {
                if let Some(handle) = handle.as_ref() {
                    handle.lock().await.stop().await?;
                }
                *progress = DeleteProgress::Release { capture_failed };
            }
            if let DeleteProgress::Release { capture_failed } = *progress {
                if let Some(manager) = self.volume_manager.as_ref() {
                    let owner = sandbox_id.to_string();
                    // An incomplete restack may leave image.json pointing at old
                    // layers. Never publish that backing as a successful capture.
                    if capture_failed {
                        manager.fail_backings(&owner, &volume_ids).await?;
                    }
                    manager.replace_owner_for(&owner, None, &volume_ids).await?;
                }
                self.remove_deleted_sandbox(sandbox_id).await?;
                *progress = DeleteProgress::Done;
            }
            Ok(())
        }
        .await;
        if let Err(error) = cleanup {
            if matches!(progress, DeleteProgress::Stop { .. }) {
                if let Some(handle) = handle {
                    self.sandboxes.write().await.insert(sandbox_id, handle);
                }
            }
            warn!(?error, "sandbox deletion needs a cleanup retry");
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Stop,
                source: error,
            });
        }
        if let Some(error) = capture_error.filter(|_| !optional_cache) {
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Stop,
                source: error.into(),
            });
        }
        Ok(())
    }

    async fn remove_deleted_sandbox(&self, sandbox_id: SandboxId) -> Result<()> {
        let metadata = self.store.remove(&sandbox_id).await?;
        if let Some(metadata) = metadata {
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Delete,
                metadata.id,
                metadata.resources,
            );
        }
        let persistence_cleanup = self.cleanup_persisted_sandbox_state(&sandbox_id).await;
        if let Err(err) = persistence_cleanup {
            warn!(error = ?err, "sandbox deleted with persisted cleanup pending");
            return Err(err.into());
        }
        self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
            .await;
        self.deletions.lock().await.remove(&sandbox_id);
        self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
            .await;
        info!("sandbox deleted");

        Ok(())
    }

    /// Stops every known sandbox and tears down in-memory runtime state.
    ///
    /// This is single-flight: the first caller performs cleanup and subsequent
    /// callers wait for the same outcome rather than starting duplicate work.
    ///
    /// Cleanup itself is still best-effort: the executor keeps attempting
    /// remaining sandboxes even if individual deletions fail, then returns an
    /// error if any sandbox could not be cleaned up after several passes.
    #[tracing::instrument(skip(self))]
    pub async fn shutdown(self: &Arc<Self>) -> Result<()> {
        let was_already_shutting_down = self.is_shutting_down.swap(true, Ordering::AcqRel);
        let _ = self.shutdown_tx.send_replace(true);

        if !was_already_shutting_down {
            info!("orchestrator shutdown requested; stopping all sandboxes");
        }

        let this = Arc::clone(self);
        let outcome = self
            .shutdown_outcome
            .get_or_init(|| async move {
                ShutdownOutcome::from_result(this.run_shutdown_cleanup().await)
            })
            .await;

        outcome.as_result()
    }

    /// Pauses a running sandbox by taking a snapshot and stopping its VM.
    ///
    /// If another `pause_sandbox` call is already in progress for the same
    /// sandbox (`Pausing` state), this call waits for it to complete and then
    /// returns the outcome rather than duplicating the work.
    pub async fn pause_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("pause", sandbox_id, async move {
            this.pause_sandbox_inner(sandbox_id, PauseOrigin::Api).await
        })
        .await
    }

    #[tracing::instrument(
        name = "pause_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id, origin = ?origin)
    )]
    async fn pause_sandbox_inner(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        origin: PauseOrigin,
    ) -> Result<()> {
        info!("pausing sandbox");
        self.ensure_pause_admission(origin)?;
        let queue_started = Instant::now();
        let _pause_permit = self
            .pause_permits
            .acquire()
            .await
            .expect("pause semaphore is never closed");
        info!(
            queue_ms = queue_started.elapsed().as_millis(),
            "pause permit acquired"
        );
        match self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Pausing, &[SandboxState::Running])
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    // Another task is already performing the pause.  Wait for
                    // it to finish and then report the final outcome.
                    SandboxState::Pausing => self.join_concurrent_pause(sandbox_id).await,
                    SandboxState::Paused => Ok(()),
                    SandboxState::Killing => {
                        info!("sandbox is being deleted while pausing");
                        Err(OrchestratorError::SandboxNotFound(sandbox_id))
                    }
                    _ => {
                        info!(state = ?actual_state, "cannot pause sandbox in current state");
                        Err(OrchestratorError::InvalidSandboxState {
                            sandbox_id,
                            state: actual_state,
                        })
                    }
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        self.pause_sandbox_impl(sandbox_id).await
    }

    async fn pause_sandbox_impl(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let pause_started = Instant::now();
        // Pin paused runtime artifacts before detaching from the running set.
        let runtime_artifacts = {
            let handle = self.sandboxes.read().await.get(&sandbox_id).cloned();
            match handle {
                Some(handle) => {
                    let sandbox = handle.lock().await;
                    sandbox.runtime_info().runtime_artifacts
                }
                None => RuntimeArtifactSet::empty(),
            }
        };
        if let Err(error) = self
            .protect_image_refs(
                RuntimeImageOwner::StartingSandbox(sandbox_id),
                runtime_artifacts,
                "pausing sandbox",
            )
            .await
        {
            warn!(error = %error, "failed to protect paused runtime artifacts; keeping sandbox Running");
            let _ = self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Running, &[SandboxState::Pausing])
                .await;
            return Err(error);
        }

        // Allocate persistence space while the running handle and route are
        // still attached. Allocation does not mutate the backend, so failure
        // only needs to restore metadata.
        let artifact_root = match self.persister.allocate_artifact_root(&sandbox_id).await {
            Ok(artifact_root) => artifact_root,
            Err(err) => {
                warn!(error = ?err, "failed to allocate paused sandbox artifact root");
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[SandboxState::Pausing],
                    )
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };

        let (handle, removed_proxy_route) = self.detach_sandbox_handle_and_route(&sandbox_id).await;

        let Some(handle) = handle else {
            warn!("sandbox handle not found while pausing, removing from store");
            if let Err(cleanup_err) = self
                .persister
                .discard_artifact_generation(&sandbox_id, artifact_root.as_deref())
                .await
            {
                warn!(error = ?cleanup_err, "failed to discard uncommitted paused generation");
            }
            self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                .await;
            if let Some(metadata) = self.store.get(&sandbox_id).await? {
                self.finalize_terminal_volumes(&metadata).await;
            }
            self.store.remove(&sandbox_id).await?;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        };

        // Pause the sandbox and capture the paused state for resuming later.
        let snapshot_started = Instant::now();
        let paused_state_result = {
            let mut sandbox = handle.lock().await;
            sandbox.pause(artifact_root.as_deref()).await
        };
        info!(
            snapshot_ms = snapshot_started.elapsed().as_millis(),
            success = paused_state_result.is_ok(),
            "pause snapshot phase completed"
        );

        // If pausing failed, attempt to put the sandbox back and return an error.
        let paused_state = match paused_state_result {
            Ok(s) => s,
            Err(err) => {
                warn!(error = ?err, "failed to pause sandbox");
                let terminal = err.is_terminal();
                let mut failure: anyhow::Error = err.into();
                if terminal {
                    // The handle was already detached from `self.sandboxes`
                    // before `pause()`. Do not reinsert it here: the live
                    // runtime may have been mutated and is no longer safe to
                    // keep serving as a running sandbox.
                    let stop_result = {
                        let mut sandbox = handle.lock().await;
                        sandbox.stop().await
                    };
                    if let Err(stop_err) = stop_result {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal pause failure");
                    }
                } else {
                    self.sandboxes.write().await.insert(sandbox_id, handle);
                    self.restore_proxy_route(sandbox_id, removed_proxy_route)
                        .await;
                    let _ = self
                        .store
                        .update_state_if_state(
                            &sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Pausing],
                        )
                        .await;
                }
                if let Err(cleanup_err) = self
                    .persister
                    .discard_artifact_generation(&sandbox_id, artifact_root.as_deref())
                    .await
                {
                    warn!(error = ?cleanup_err, "failed to discard uncommitted paused generation");
                }
                if terminal {
                    failure = self.recover_failed_pause(sandbox_id, failure).await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Pause,
                    source: failure,
                });
            }
        };

        let persisted_metadata = {
            let mut metadata = self
                .store
                .get(&sandbox_id)
                .await?
                .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
            metadata.state = SandboxState::Paused;
            metadata.paused_state = Some(paused_state.clone());
            metadata
        };
        let paused_artifacts = paused_state.runtime_artifacts();
        let durable_pause = async {
            paused_artifacts.resolve_closure().map_err(|source| {
                OrchestratorError::InternalError(format!(
                    "resolve paused runtime artifact closure: {source:#}"
                ))
            })?;
            self.protect_image_refs(
                RuntimeImageOwner::StartingSandbox(sandbox_id),
                paused_artifacts.clone(),
                "paused sandbox closure",
            )
            .await?;
            self.persister
                .persist_paused(
                    &persisted_metadata,
                    artifact_root.as_deref(),
                    paused_state.as_ref(),
                )
                .await
                .map_err(|error| {
                    OrchestratorError::InternalError(format!(
                        "failed to persist paused sandbox state: {error:#}"
                    ))
                })
        }
        .await;
        if let Err(err) = durable_pause {
            warn!(error = ?err, "failed to durably protect paused sandbox state");
            let resume_result = {
                let mut sandbox = handle.lock().await;
                sandbox.resume().await
            };
            let resumed = resume_result.is_ok();
            let mut failure = err;
            if let Err(resume_err) = resume_result {
                warn!(error = ?resume_err, "failed to resume sandbox after pause failure");
                let stop_result = {
                    let mut sandbox = handle.lock().await;
                    sandbox.stop().await
                };
                if let Err(stop_err) = stop_result {
                    warn!(error = ?stop_err, "failed to stop sandbox after pause failure");
                }
            } else {
                // A successful backend pause may rewrite the live runtime
                // config to reference this generation. Keep it until a later
                // successful pause prunes it.
                warn!("retaining failed paused generation referenced by resumed sandbox");
                // Register while still Pausing: the Running CAS below is what
                // lets a later pause start, and its commit releases retention.
                if let Err(retain_err) = self
                    .persister
                    .retain_runtime_generation(&sandbox_id, artifact_root.as_deref())
                    .await
                {
                    warn!(error = ?retain_err, "failed to retain paused generation for resumed sandbox");
                }
                self.sandboxes.write().await.insert(sandbox_id, handle);
                self.restore_proxy_route(sandbox_id, removed_proxy_route)
                    .await;
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[SandboxState::Pausing],
                    )
                    .await;
            }
            if !resumed {
                if let Err(cleanup_err) = self
                    .persister
                    .discard_artifact_generation(&sandbox_id, artifact_root.as_deref())
                    .await
                {
                    warn!(error = ?cleanup_err, "failed to discard uncommitted paused generation");
                }
                failure = OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Pause,
                    source: self.recover_failed_pause(sandbox_id, failure.into()).await,
                };
            }
            return Err(failure);
        }
        match self
            .protect_image_refs(
                RuntimeImageOwner::PausedSandbox(sandbox_id),
                paused_artifacts,
                "durable paused sandbox closure",
            )
            .await
        {
            Ok(()) => {
                self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                    .await;
            }
            Err(error) => {
                warn!(error = %error, "durable paused hold promotion failed; retaining transition hold");
            }
        }
        let resources = persisted_metadata.resources;
        self.store.update(persisted_metadata.clone()).await?;

        // Stop the sandbox to free up resources.
        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        match stop_result {
            Ok(()) => {
                let cleanup_started = Instant::now();
                let cleanup_result = self
                    .persister
                    .prune_artifact_generations(&sandbox_id, artifact_root.as_deref())
                    .await;
                info!(
                    cleanup_ms = cleanup_started.elapsed().as_millis(),
                    success = cleanup_result.is_ok(),
                    "pause cleanup phase completed"
                );
                if let Err(err) = cleanup_result {
                    warn!(error = ?err, "failed to prune superseded paused sandbox generations");
                }
            }
            Err(err) => {
                warn!(error = ?err, "failed to stop sandbox after pausing");
            }
        }
        self.publish_sandbox_event(SandboxLifecycleEventType::Pause, sandbox_id, resources);
        info!(
            total_ms = pause_started.elapsed().as_millis(),
            "sandbox paused"
        );

        Ok(())
    }

    /// Resumes a paused sandbox from its snapshot.
    ///
    /// If another `resume_sandbox` call is already in progress (`Resuming`
    /// state), this call waits for the ongoing resume to finish and then
    /// returns the actual outcome (either `Running` or an error) rather than
    /// duplicating the work. On success the sandbox is ready for use when this
    /// method returns.
    pub async fn resume_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("resume", sandbox_id, async move {
            this.resume_sandbox_inner(sandbox_id, timeout).await
        })
        .await
    }

    #[tracing::instrument(
        name = "resume_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id, timeout = ?timeout)
    )]
    async fn resume_sandbox_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        self.ensure_accepting_lifecycle_operations()?;
        self.ensure_disk_admission("resume")?;
        let _resume_permit = self
            .resume_permits
            .acquire()
            .await
            .expect("resume semaphore is never closed");

        info!("resuming sandbox");
        let mut metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;

        // If another resume is in progress, wait for it to complete and
        // re-evaluate the resulting stable state.
        if metadata.state == SandboxState::Resuming {
            metadata = self
                .wait_for_transition(sandbox_id, SandboxState::Resuming)
                .await?;
        }

        match metadata.state {
            SandboxState::Killing => {
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            SandboxState::Running => {
                // Already running — just update the timeout if requested and return.
                return self.maybe_update_running_timeout(sandbox_id, timeout).await;
            }
            SandboxState::Paused => {}
            state => {
                return Err(OrchestratorError::InvalidSandboxState { sandbox_id, state });
            }
        }

        let node_mode = ConfigManager::global_config().virtualization_mode;
        if metadata.virtualization_mode != node_mode {
            return Err(OrchestratorError::VirtualizationModeMismatch {
                resource: format!("paused sandbox {sandbox_id}"),
                resource_mode: metadata.virtualization_mode,
                node_mode,
            });
        }

        let paused_state = metadata.paused_state.as_ref().cloned().ok_or_else(|| {
            warn!("missing paused state while resuming");
            OrchestratorError::InternalError("missing paused state".to_string())
        })?;

        match self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Resuming, &[SandboxState::Paused])
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    SandboxState::Running => {
                        // Another task already completed the resume.
                        self.maybe_update_running_timeout(sandbox_id, timeout).await
                    }
                    SandboxState::Resuming => {
                        // A second concurrent resume snuck in between our state
                        // read and CAS.  Wait for it and return the outcome.
                        self.join_concurrent_resume(sandbox_id, timeout).await
                    }
                    SandboxState::Killing => {
                        info!("sandbox is being deleted while resuming");
                        Err(OrchestratorError::SandboxNotFound(sandbox_id))
                    }
                    _ => {
                        info!(state = ?actual_state, "cannot resume sandbox in current state");
                        Err(OrchestratorError::InvalidSandboxState {
                            sandbox_id,
                            state: actual_state,
                        })
                    }
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        let protect_paused = async {
            let paused_artifacts = paused_state.runtime_artifacts();
            paused_artifacts.resolve_closure().map_err(|source| {
                OrchestratorError::InternalError(format!(
                    "validate paused runtime artifact closure before resume: {source:#}"
                ))
            })?;
            self.protect_image_refs(
                RuntimeImageOwner::PausedSandbox(sandbox_id),
                paused_artifacts,
                "paused sandbox before resume",
            )
            .await
        }
        .await;
        if let Err(error) = protect_paused {
            let _ = self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Paused, &[SandboxState::Resuming])
                .await;
            return Err(error);
        }
        self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
            .await;

        if let Err(err) = self.persister.mark_resuming(&sandbox_id).await {
            warn!(error = ?err, "failed to mark persisted sandbox record as resuming");
            let _ = self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Paused, &[SandboxState::Resuming])
                .await;
            return Err(OrchestratorError::InternalError(format!(
                "failed to mark persisted sandbox record as resuming: {err:#}"
            )));
        }

        let resumed = self
            .launch_sandbox(LaunchPlan::for_resume(
                sandbox_id,
                paused_state,
                timeout,
                metadata.resources,
                metadata
                    .secure
                    .then(|| self.access_tokens.generate(metadata.id)),
            ))
            .await;
        if let Ok(metadata) = resumed.as_ref() {
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Resume,
                metadata.id,
                metadata.resources,
            );
        }
        resumed
    }

    /// Captures a snapshot of a running sandbox.
    pub async fn capture_snapshot(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("snapshot", sandbox_id, async move {
            this.capture_snapshot_inner(sandbox_id).await
        })
        .await
    }

    async fn begin_snapshot_operation(&self, sandbox_id: SandboxId) -> Result<SandboxHandle> {
        self.ensure_accepting_lifecycle_operations()?;
        self.store
            .update_state_if_state(
                &sandbox_id,
                SandboxState::Snapshotting,
                &[SandboxState::Running],
            )
            .await
            .map_err(|error| match error {
                StoreError::StateConflict {
                    actual_state: SandboxState::Killing,
                    ..
                } => OrchestratorError::SandboxNotFound(sandbox_id),
                StoreError::StateConflict { actual_state, .. } => {
                    OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }
                }
                error => OrchestratorError::from(error),
            })?;

        if let Some(handle) = self.sandboxes.read().await.get(&sandbox_id).cloned() {
            return Ok(handle);
        }
        warn!("sandbox handle not found while snapshotting, removing from store");
        self.detach_sandbox_handle_and_route(&sandbox_id).await;
        if let Some(metadata) = self.store.get(&sandbox_id).await? {
            self.finalize_terminal_volumes(&metadata).await;
        }
        self.store.remove(&sandbox_id).await?;
        Err(OrchestratorError::SandboxNotFound(sandbox_id))
    }

    async fn fail_snapshot_operation<T>(
        &self,
        sandbox_id: SandboxId,
        handle: &SandboxHandle,
        error: SandboxCaptureError,
        operation: SandboxOperation,
    ) -> Result<T> {
        warn!(?error, ?operation, "sandbox snapshot operation failed");
        if error.is_terminal() {
            self.detach_sandbox_handle_and_route(&sandbox_id).await;
            if let Err(stop_error) = handle.lock().await.stop().await {
                warn!(
                    ?stop_error,
                    ?operation,
                    "failed to stop sandbox after terminal snapshot failure"
                );
            }
            if let Some(metadata) = self.store.get(&sandbox_id).await? {
                self.finalize_terminal_volumes(&metadata).await;
            }
            self.store.remove(&sandbox_id).await?;
        } else {
            let _ = self
                .store
                .update_state_if_state(
                    &sandbox_id,
                    SandboxState::Running,
                    &[SandboxState::Snapshotting],
                )
                .await;
        }
        Err(OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation,
            source: error.into(),
        })
    }

    async fn finish_snapshot_operation(&self, sandbox_id: SandboxId) -> Result<()> {
        self.store
            .update_state_if_state(
                &sandbox_id,
                SandboxState::Running,
                &[SandboxState::Snapshotting],
            )
            .await?;
        Ok(())
    }

    /// Seals writable persistent-volume uppers without capturing the VM's
    /// rootfs, memory, or device state so callers can clone them locally.
    pub async fn snapshot_volume_mounts(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("snapshot_volumes", sandbox_id, async move {
            this.snapshot_volume_mounts_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "snapshot_volume_mounts",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn snapshot_volume_mounts_inner(self: Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let handle = self.begin_snapshot_operation(sandbox_id).await?;
        if let Err(error) = {
            let mut sandbox = handle.lock().await;
            sandbox.snapshot_volumes().await
        } {
            return self
                .fail_snapshot_operation(
                    sandbox_id,
                    &handle,
                    error,
                    SandboxOperation::SnapshotVolumes,
                )
                .await;
        }
        self.finish_snapshot_operation(sandbox_id).await
    }

    #[tracing::instrument(
        name = "capture_snapshot",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn capture_snapshot_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        self.ensure_accepting_lifecycle_operations()?;
        self.ensure_disk_admission("snapshot")?;

        info!("capturing sandbox snapshot");
        let handle = self.begin_snapshot_operation(sandbox_id).await?;
        let result = {
            let mut sandbox = handle.lock().await;
            sandbox.snapshot().await
        };
        let captured_snapshot = match result {
            Ok(captured_snapshot) => captured_snapshot,
            Err(error) => {
                return self
                    .fail_snapshot_operation(sandbox_id, &handle, error, SandboxOperation::Snapshot)
                    .await;
            }
        };

        self.finish_snapshot_operation(sandbox_id).await?;
        let metadata = match self.store.get(&sandbox_id).await? {
            Some(metadata) => metadata,
            None => {
                warn!("sandbox disappeared after snapshotting");
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
        };

        info!("snapshot captured");
        Ok(SnapshotCaptureResult {
            metadata,
            captured_snapshot,
        })
    }

    pub async fn replace_sandbox_network_policy(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        network_policy: SandboxNetworkPolicy,
    ) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("update_network", sandbox_id, async move {
            this.replace_sandbox_network_policy_inner(sandbox_id, network_policy)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "replace_sandbox_network_policy",
        skip(self, network_policy),
        fields(sandbox_id = %sandbox_id))
    ]
    async fn replace_sandbox_network_policy_inner(
        &self,
        sandbox_id: SandboxId,
        mut network_policy: SandboxNetworkPolicy,
    ) -> Result<()> {
        let metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        if metadata.state != SandboxState::Running {
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }
        network_policy.allow_public_traffic = metadata.network_policy.allow_public_traffic;

        let sandbox = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&sandbox_id).cloned()
        }
        .ok_or_else(|| OrchestratorError::SandboxOperationConflict {
            sandbox_id,
            operation: SandboxOperation::UpdateNetwork,
        })?;

        let runtime_policy = network_policy.runtime_policy();

        let update_result = {
            let mut sandbox = sandbox.lock().await;
            sandbox.update_network_policy(runtime_policy).await
        };
        update_result.map_err(|source| OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::UpdateNetwork,
            source,
        })?;

        self.store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.network_policy = network_policy;
            })
            .await?;

        Ok(())
    }

    /// Patch the custom extension params of a running sandbox.
    ///
    /// The patch document is passed through verbatim to the custom
    /// extension's patch-params hook, which returns the updated full params.
    /// On hook failure the sandbox keeps its previous params and the
    /// metadata store is left untouched. Returns the new full params (`None`
    /// means empty params).
    pub async fn patch_sandbox_custom_extension_params(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        patch: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<CustomExtensionParams>> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("patch_custom_extension_params", sandbox_id, async move {
            this.patch_sandbox_custom_extension_params_inner(sandbox_id, patch)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "patch_sandbox_custom_extension_params",
        skip(self, patch),
        fields(sandbox_id = %sandbox_id))
    ]
    async fn patch_sandbox_custom_extension_params_inner(
        &self,
        sandbox_id: SandboxId,
        patch: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<CustomExtensionParams>> {
        let metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        if metadata.state != SandboxState::Running {
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        let sandbox = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&sandbox_id).cloned()
        }
        .ok_or_else(|| OrchestratorError::SandboxOperationConflict {
            sandbox_id,
            operation: SandboxOperation::PatchCustomExtensionParams,
        })?;

        // Invoke the extension's patch-params hook here (the backend only
        // stores the approved value). The sandbox lock is not held during
        // the hook call so pause/stop are not blocked on extension latency.
        let client = CustomExtensionClient::global().ok_or_else(|| {
            OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::PatchCustomExtensionParams,
                source: anyhow::anyhow!(
                    "custom extension is not configured ([custom_extension].url is unset)"
                ),
            }
        })?;
        let new_params = client
            .hook_patch_params(sandbox_id, patch)
            .await
            .map_err(|source| OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::PatchCustomExtensionParams,
                source,
            })?;

        {
            let mut sandbox = sandbox.lock().await;
            sandbox.update_custom_extension_params(new_params.clone());
        }

        // NOTE: a concurrent pause may have transitioned the sandbox since the entry check,
        // so this may fail. But it's acceptable since extension state should be transient like network policy
        self.store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.custom_extension_params = new_params.clone();
            })
            .await
            .map_err(|err| match err {
                // Lost a race against a concurrent state transition (e.g.
                // pause): report it as a conflict instead of a 500.
                StoreError::StateConflict {
                    sandbox_id,
                    actual_state,
                    ..
                } => OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: actual_state,
                },
                other => OrchestratorError::from(other),
            })?;

        Ok(new_params)
    }

    /// Returns the current orchestrator metrics snapshot.
    ///
    /// Counter fields are read atomically; resource fields are aggregated by
    /// scanning the metadata store, so the returned snapshot is always
    /// consistent with the orchestrator's current set of sandboxes.
    pub async fn metrics_snapshot(&self) -> Result<OrchestratorMetrics> {
        let mut metrics = OrchestratorMetrics::default();
        self.store
            .list_with_callback(|metadata| {
                aggregate_resource_metrics(
                    &mut metrics,
                    SandboxContribution::new(metadata.state, metadata.resources),
                );
            })
            .await?;
        metrics.create_successes = self.counters.create_successes();
        metrics.create_fails = self.counters.create_fails();
        let cleanup = self.persister.cleanup_metrics();
        let disk = self.disk_policy.snapshot();
        metrics.disk_total_bytes = disk.total_bytes;
        metrics.disk_used_bytes = disk.used_bytes;
        metrics.disk_available_bytes = disk.available_bytes;
        metrics.cleanup_pending = cleanup.pending;
        metrics.cleanup_retries = cleanup.retries;
        metrics.cleanup_failures = cleanup.failures;
        metrics.pruned_generations = cleanup.pruned_generations;
        metrics.reclaimed_snapshot_bytes = cleanup.reclaimed_snapshot_bytes;
        metrics.reserved_cleanup_journal_bytes = cleanup.reserved_journal_bytes;
        metrics.reclaimed_log_bytes = self.reclaimed_log_bytes.load(Ordering::Relaxed);
        metrics.disk_admission_rejections = self.disk_admission_rejections.load(Ordering::Relaxed);
        metrics.accepting_sandboxes = disk.accepting_sandboxes;
        metrics.admission_reason = disk.reason.as_str();
        Ok(metrics)
    }

    pub fn subscribe_sandbox_events(&self) -> broadcast::Receiver<SandboxLifecycleEvent> {
        self.sandbox_event_tx.subscribe()
    }

    fn publish_sandbox_event(
        &self,
        event_type: SandboxLifecycleEventType,
        sandbox_id: SandboxId,
        resources: SandboxResources,
    ) {
        let event = SandboxLifecycleEvent {
            event_type,
            sandbox_id,
            resources,
        };
        let _ = self.sandbox_event_tx.send(event);
    }

    /// Waits for `sandbox_id` to leave `transitional_state`, then returns the
    /// resulting metadata. Returns `SandboxNotFound` if the sandbox is removed
    /// while waiting, or `InvalidSandboxState` if the sandbox is still in the
    /// transitional state after the configured transition timeout elapses.
    async fn wait_for_transition(
        &self,
        sandbox_id: SandboxId,
        transitional_state: SandboxState,
    ) -> Result<SandboxMetadata> {
        let states = [transitional_state];
        let wait = self.store.wait_while_in_states(&sandbox_id, &states);
        match tokio::time::timeout(self.timeouts.transition, wait).await {
            Ok(Ok(Some(m))) => Ok(m),
            Ok(Ok(None)) => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
            Ok(Err(e)) => Err(OrchestratorError::from(e)),
            Err(_elapsed) => {
                warn!(
                    sandbox_id = %sandbox_id,
                    state = ?transitional_state,
                    "timed out waiting for sandbox to leave transitional state"
                );
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: transitional_state,
                })
            }
        }
    }

    /// Applies `timeout` to `metadata` and persists the change if the sandbox
    /// is still `Running`. Returns the updated metadata. If `timeout` is `None`,
    /// the timeout will be cleared, which indicates no expiration.
    async fn maybe_update_running_timeout(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        let update_result = self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.update_timeout(timeout);
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => {
                    info!(state = ?actual_state, "cannot update timeout for sandbox in current state");
                    OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }
                }
                other => OrchestratorError::from(other),
            })?;
        Ok(update_result.current)
    }

    /// Joins a concurrent pause already in progress for the same sandbox.
    /// Waits for the `Pausing` state to resolve and maps the final state to
    /// the appropriate `Ok(())` / `Err(...)` result.
    async fn join_concurrent_pause(&self, sandbox_id: SandboxId) -> Result<()> {
        debug!("concurrent pause in progress, waiting for completion");
        let m = self
            .wait_for_transition(sandbox_id, SandboxState::Pausing)
            .await?;
        match m.state {
            SandboxState::Paused => {
                debug!("concurrent pause succeeded");
                Ok(())
            }
            SandboxState::Running => {
                info!("concurrent pause failed; sandbox returned to running state");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Running,
                })
            }
            SandboxState::Killing => {
                info!("sandbox is being deleted after concurrent pause attempt");
                Err(OrchestratorError::SandboxNotFound(sandbox_id))
            }
            other => {
                info!(state = ?other, "unexpected state after waiting for concurrent pause");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: other,
                })
            }
        }
    }

    /// Joins a concurrent resume already in progress for the same sandbox.
    /// Waits for the `Resuming` state to resolve, then applies `timeout` if
    /// the sandbox reached `Running`, and returns the final metadata.
    async fn join_concurrent_resume(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        debug!("concurrent resume in progress, waiting for completion");
        let m = self
            .wait_for_transition(sandbox_id, SandboxState::Resuming)
            .await?;
        match m.state {
            SandboxState::Running => self.maybe_update_running_timeout(sandbox_id, timeout).await,
            SandboxState::Paused => {
                info!("concurrent resume failed; sandbox returned to paused state");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Paused,
                })
            }
            SandboxState::Killing => {
                info!("sandbox is being deleted while resuming");
                Err(OrchestratorError::SandboxNotFound(sandbox_id))
            }
            state => {
                info!(state = ?state, "unexpected state after waiting for concurrent resume");
                Err(OrchestratorError::InvalidSandboxState { sandbox_id, state })
            }
        }
    }

    /// Automatically pauses or stops sandboxes whose timeout has expired.
    async fn evict_expired_sandboxes(self: &Arc<Self>) -> Result<Vec<SandboxId>> {
        if self.is_shutting_down() {
            debug!("skipping auto-evict because orchestrator is shutting down");
            return Ok(Vec::new());
        }

        let eviction_cutoff = SystemTime::now();
        let expired = self.store.list_expired(eviction_cutoff).await?;
        let mut evicted_ids = Vec::new();

        for metadata in expired {
            if metadata.state != SandboxState::Running {
                continue;
            }
            let _pause_permit = if metadata.timeout_action == SandboxTimeoutAction::Pause {
                if let Err(err) = self.ensure_pause_admission(PauseOrigin::AutoEvict) {
                    warn!(sandbox_id = %metadata.id, error = ?err, "auto-pause admission refused");
                    continue;
                }
                Some(
                    self.pause_permits
                        .acquire()
                        .await
                        .expect("pause semaphore is never closed"),
                )
            } else {
                None
            };
            let claimed_state = match metadata.timeout_action {
                SandboxTimeoutAction::Pause => SandboxState::Pausing,
                SandboxTimeoutAction::Delete => SandboxState::Killing,
            };
            // Use the same operation lock as explicit deletion before claiming Killing.
            let deletion = self.deletion_progress(metadata.id).await;
            let mut progress = deletion.lock().await;
            let result = match self
                .claim_expired_running_sandbox(metadata.id, eviction_cutoff, claimed_state)
                .await
            {
                Ok(true) => match metadata.timeout_action {
                    SandboxTimeoutAction::Pause => self.pause_sandbox_impl(metadata.id).await,
                    SandboxTimeoutAction::Delete => {
                        self.delete_sandbox_impl(metadata.id, SandboxState::Running, &mut progress)
                            .await
                    }
                }
                .map(|_| true),
                Ok(false) => Ok(false),
                Err(err) => Err(err),
            };
            match result {
                Ok(true) => evicted_ids.push(metadata.id),
                Ok(false) => continue,
                Err(err) => warn!(
                    sandbox_id = %metadata.id,
                    action = ?metadata.timeout_action,
                    error = ?err,
                    "failed to auto-evict expired sandbox"
                ),
            }
        }

        Ok(evicted_ids)
    }

    /// Starts a background task that periodically evicts expired sandboxes.
    /// The eviction policy is defined by the sandbox's [`timeout_action`](SandboxMetadata::timeout_action).
    fn start_auto_evict_task(
        this: Arc<Self>,
        evict_interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("auto-evict task not started: no Tokio runtime available");
            return;
        };

        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval(evict_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            debug!("auto-evict task started with interval {:?}", evict_interval);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("auto-evict task stopping because orchestrator is shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            debug!("auto-evict task stopping because orchestrator was dropped");
                            break;
                        };
                        if let Err(err) = this.evict_expired_sandboxes().await {
                            warn!("auto-evict task failed: {err}");
                        }
                    }
                }
            }
        });
    }

    /// Starts a background task that periodically runs local image maintenance
    /// (capacity eviction + fail-closed GC) over the current running set.
    fn start_local_image_maintenance_task(
        this: Arc<Self>,
        interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("local image maintenance task not started: no Tokio runtime available");
            return;
        };

        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval_at(Instant::now() + interval, interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            info!(interval = ?interval, "local image maintenance task started");

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("local image maintenance task stopping because orchestrator is shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            debug!("local image maintenance task stopping because orchestrator was dropped");
                            break;
                        };

                        let running = this.collect_running_artifacts().await;
                        if let Err(err) = this.image_refs.maintain_running(running).await {
                            warn!("local image maintenance pass failed: {err:#}");
                        }
                    }
                }
            }
        });
    }

    async fn recover_failed_pause(
        &self,
        sandbox_id: SandboxId,
        failure: anyhow::Error,
    ) -> anyhow::Error {
        let recovery = self
            .persister
            .load_recovery(&sandbox_id, &self.factory)
            .await;
        let restored = match recovery {
            Ok(Some(metadata)) => {
                let artifacts = metadata
                    .paused_state
                    .as_ref()
                    .expect("decoded recovery state")
                    .runtime_artifacts();
                if let Err(error) = self
                    .protect_image_refs(
                        RuntimeImageOwner::PausedSandbox(sandbox_id),
                        artifacts,
                        "pause recovery checkpoint",
                    )
                    .await
                {
                    self.image_gc_ready.store(false, Ordering::Release);
                    warn!(%error, %sandbox_id, "checkpoint pin failed; disabling image GC");
                }
                self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                    .await;
                self.store
                    .update_if_state(&sandbox_id, &[SandboxState::Pausing], |current| {
                        *current = metadata
                    })
                    .await
                    .map(|_| true)
            }
            Ok(None) => {
                if let Ok(Some(metadata)) = self.store.get(&sandbox_id).await {
                    self.finalize_terminal_volumes(&metadata).await;
                }
                self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                    .await;
                self.store.remove(&sandbox_id).await.map(|_| false)
            }
            Err(error) => {
                self.image_gc_ready.store(false, Ordering::Release);
                let _ = self
                    .store
                    .update_if_state(&sandbox_id, &[SandboxState::Pausing], |metadata| {
                        metadata.state = SandboxState::Paused;
                        metadata.paused_state = None;
                    })
                    .await;
                return failure.context(format!("pause failed; recovery checkpoint could not be read and has been preserved: {error}"));
            }
        };
        match restored {
            Ok(true) => {
                warn!(%sandbox_id, "pause failed; restored last durable checkpoint, losing progress since that checkpoint");
                failure.context("pause failed; sandbox is Paused at its last durable checkpoint; progress since that checkpoint was lost")
            }
            Ok(false) => failure,
            Err(error) => failure.context(format!(
                "pause failed; could not publish recovery state: {error}"
            )),
        }
    }

    async fn run_disk_policy_pass(&self) -> Result<()> {
        let cleanup = self.persister.cleanup_metrics();
        let mut snapshot = self.disk_policy.refresh(cleanup.pending).map_err(|error| {
            OrchestratorError::InternalError(format!("refresh runtime disk policy: {error:#}"))
        })?;
        if !snapshot.cleanup_required {
            return Ok(());
        }

        match self.persister.replay_cleanup_obligations().await {
            Ok(finalized) => {
                for sandbox_id in finalized {
                    self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                        .await;
                    self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                        .await;
                }
            }
            Err(error) => {
                warn!(error = ?error, "durable sandbox cleanup pass remains incomplete");
            }
        }
        snapshot = self
            .disk_policy
            .refresh(self.persister.cleanup_metrics().pending)
            .map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "refresh runtime disk policy after persisted cleanup: {error:#}"
                ))
            })?;
        if !snapshot.cleanup_required {
            return Ok(());
        }
        if self.image_gc_ready.load(Ordering::Acquire) {
            let running = self.collect_running_artifacts().await;
            let reclaim_bytes = self.disk_policy.required_reclaim_bytes();
            match self
                .image_refs
                .reclaim_for_disk_pressure(running, reclaim_bytes)
                .await
            {
                Ok(reclaimed) => info!(
                    collected = reclaimed.collected,
                    freed_bytes = reclaimed.freed_bytes,
                    retained = reclaimed.retained,
                    "disk-pressure image cache reclaim complete"
                ),
                Err(error) => warn!(error = %error, "disk-pressure image cache reclaim failed"),
            }
        } else {
            warn!("disk-pressure image cache reclaim skipped because paused closure protection is incomplete");
        }
        match self.reclaim_expired_serial_logs().await {
            Ok((removed, bytes)) => {
                self.reclaimed_log_bytes.fetch_add(bytes, Ordering::Relaxed);
                info!(
                    removed,
                    freed_bytes = bytes,
                    "expired serial logs reclaimed"
                );
            }
            Err(error) => warn!(error = ?error, "expired serial log reclaim failed"),
        }
        snapshot = self
            .disk_policy
            .refresh(self.persister.cleanup_metrics().pending)
            .map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "refresh runtime disk policy after cleanup: {error:#}"
                ))
            })?;
        info!(
            used_bytes = snapshot.used_bytes,
            total_bytes = snapshot.total_bytes,
            accepting_sandboxes = snapshot.accepting_sandboxes,
            reason = snapshot.reason.as_str(),
            "disk policy pass complete"
        );
        Ok(())
    }

    async fn reclaim_expired_serial_logs(&self) -> Result<(u64, u64)> {
        let Some(root) = &self.serial_log_dir else {
            return Ok((0, 0));
        };
        let live: HashSet<SandboxId> = self.store.list_ids().await?.into_iter().collect();
        let cutoff = SystemTime::now()
            .checked_sub(self.log_retention)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut entries = match tokio::fs::read_dir(root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
            Err(error) => {
                return Err(OrchestratorError::InternalError(format!(
                    "read serial log directory {}: {error}",
                    root.display()
                )))
            }
        };
        let mut removed = 0u64;
        let mut bytes = 0u64;
        while let Some(entry) = entries.next_entry().await.map_err(|error| {
            OrchestratorError::InternalError(format!(
                "scan serial log directory {}: {error}",
                root.display()
            ))
        })? {
            let Some(sandbox_id) = entry
                .file_name()
                .to_str()
                .and_then(|value| SandboxId::parse_str(value).ok())
            else {
                continue;
            };
            let file_type = entry.file_type().await.map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "inspect serial log entry {}: {error}",
                    entry.path().display()
                ))
            })?;
            if live.contains(&sandbox_id) || !file_type.is_dir() {
                continue;
            }
            let metadata = entry.metadata().await.map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "stat serial log entry {}: {error}",
                    entry.path().display()
                ))
            })?;
            if metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH) > cutoff {
                continue;
            }
            let path = entry.path();
            let reclaimed = path_tree_bytes(path.clone()).await;
            tokio::fs::remove_dir_all(&path).await.map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "remove expired serial log directory {}: {error}",
                    path.display()
                ))
            })?;
            removed += 1;
            bytes = bytes.saturating_add(reclaimed);
        }
        Ok((removed, bytes))
    }

    fn start_disk_policy_task(
        this: Arc<Self>,
        interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("disk policy task not started: no Tokio runtime available");
            return;
        };
        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval_at(Instant::now() + interval, interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            break;
                        };
                        if let Err(error) = this.run_disk_policy_pass().await {
                            warn!(error = ?error, "disk policy pass failed closed");
                        }
                    }
                }
            }
        });
    }

    #[tracing::instrument(skip(self, plan))]
    async fn launch_sandbox(self: &Arc<Self>, plan: LaunchPlan) -> Result<SandboxMetadata> {
        let mut progress = LaunchProgress::default();
        let result = if matches!(plan, LaunchPlan::Resume(_)) {
            match tokio::time::timeout(
                self.timeouts.resume,
                self.launch_sandbox_transaction(&plan, &mut progress),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    warn!(sandbox_id = %plan.sandbox_id(), "sandbox resume transaction timed out");
                    self.cleanup_timed_out_launch(&plan, progress).await;
                    Err(OrchestratorError::SandboxOperationFailed {
                        sandbox_id: plan.sandbox_id(),
                        operation: SandboxOperation::Start,
                        source: anyhow::anyhow!(
                            "sandbox resume transaction timed out after {:?}",
                            self.timeouts.resume
                        ),
                    })
                }
            }
        } else {
            self.launch_sandbox_transaction(&plan, &mut progress).await
        };
        let metadata = result?;
        if matches!(plan, LaunchPlan::Resume(_)) {
            // The VM is committed. Housekeeping failure must not stop it.
            let finalize = async {
                if let Err(error) = self.persister.complete_resume(&metadata.id).await {
                    warn!(%error, "failed to complete durable resume transaction");
                }
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(metadata.id))
                    .await;
            };
            if tokio::time::timeout(self.timeouts.housekeeping, finalize)
                .await
                .is_err()
            {
                warn!(sandbox_id = %metadata.id, "resume committed but housekeeping timed out");
            }
        }
        Ok(metadata)
    }

    async fn launch_sandbox_transaction(
        self: &Arc<Self>,
        plan: &LaunchPlan,
        progress: &mut LaunchProgress,
    ) -> Result<SandboxMetadata> {
        self.ensure_accepting_lifecycle_operations()?;
        // Resume already checked disk admission immediately before it changed
        // durable state to Resuming. Do not repeat synchronous filesystem probes
        // after that transition: a stuck probe would strand the sandbox there.
        if matches!(plan, LaunchPlan::Create(_)) {
            self.ensure_disk_admission("launch")?;
        }

        let sandbox_id = plan.sandbox_id();
        let transitional_state = plan.transitional_state();

        // Build and start the sandbox first, before making any state changes, so that we don't
        // have to roll back any persisted state if the build fails.
        // Meanwhile, the start process can be overlapped with the initial state persistence.
        let sandbox = match self.build_sandbox_for_launch(plan).await {
            Ok(sandbox) => sandbox,
            Err(err) => {
                self.rollback_failed_launch_metadata(plan, transitional_state)
                    .await;
                return Err(err);
            }
        };
        let handle = Arc::new(Mutex::new(sandbox));
        progress.handle = Some(handle.clone());
        progress.stage = Some(FailedLaunchStage::BackendBuilt);

        // Keep a runtime hold for the sandbox's entire running lifetime. A
        // sampled running-set snapshot is not enough: a GC pass may have
        // sampled before this handle was registered and finish afterward.
        let startup_artifacts = {
            let sandbox = handle.lock().await;
            sandbox.startup_artifacts()
        };
        if let Err(err) = self
            .protect_image_refs(
                RuntimeImageOwner::StartingSandbox(sandbox_id),
                startup_artifacts,
                "running sandbox",
            )
            .await
        {
            warn!(error = %format_args!("{err:#}"), "failed to protect runtime artifacts");
            self.cleanup_failed_launch(plan, handle, FailedLaunchStage::BackendBuilt)
                .await;
            return Err(err);
        }
        let start_result = {
            let mut sandbox = handle.lock().await;
            sandbox.start_nowait().await
        };
        if let Err(source) = start_result {
            warn!(error = %format_args!("{source:#}"), "failed to start sandbox");
            self.cleanup_failed_launch(plan, handle, FailedLaunchStage::BackendBuilt)
                .await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Start,
                source,
            });
        }
        debug!("sandbox start requested");

        // If the orchestrator started shutting down, stop here before we persist any state.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down just after starting the sandbox");
            self.cleanup_failed_launch(plan, handle, FailedLaunchStage::BackendBuilt)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let runtime_resources = {
            let sandbox = handle.lock().await;
            resources_with_runtime_info(plan.resources(), sandbox.runtime_info())
        };
        let transitional_metadata = plan.transitional_metadata().map(|metadata| {
            let mut metadata = metadata.clone();
            metadata.resources = runtime_resources;
            metadata
        });

        // Store the sandbox handle in memory.
        self.sandboxes
            .write()
            .await
            .insert(sandbox_id, handle.clone());
        progress.stage = Some(FailedLaunchStage::Registered);

        // Persist the sandbox metadata if needed (during creation).
        if let Some(metadata) = transitional_metadata.as_ref() {
            if let Err(err) = self.store.add(metadata.clone()).await {
                warn!(error = %format_args!("{err:#}"), "failed to persist sandbox metadata; cleaning up");
                self.cleanup_failed_launch(plan, handle, FailedLaunchStage::Registered)
                    .await;
                return Err(OrchestratorError::from(err));
            }
        }
        progress.stage = Some(FailedLaunchStage::TransitionalPersisted);

        // Check for shutdown again before we wait for the sandbox to become ready.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down before sandbox became ready");
            self.cleanup_failed_launch(plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        // Wait for the sandbox to be ready
        let wait_result = {
            let sandbox = handle.lock().await;
            sandbox.wait_for_ready().await
        };
        if let Err(source) = wait_result {
            warn!(error = %format_args!("{source:#}"), "sandbox failed to become ready");
            self.cleanup_failed_launch(plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::WaitReady,
                source,
            });
        }

        // Check for shutdown again before we persist the final state and publish the proxy route.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down while sandbox was becoming ready");
            self.cleanup_failed_launch(plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let proxy_target = {
            let sandbox = handle.lock().await;
            match Self::proxy_target_from_sandbox(sandbox.as_ref()) {
                Ok(proxy_target) => proxy_target,
                Err(err) => {
                    warn!(error = %format_args!("{err:#}"), "sandbox became ready without a proxy target; rolling back launch");
                    drop(sandbox);
                    self.cleanup_failed_launch(
                        plan,
                        handle,
                        FailedLaunchStage::TransitionalPersisted,
                    )
                    .await;
                    return Err(err);
                }
            }
        };
        // Acquire publication locks before Running: no cancellable wait may
        // remain between committing metadata and publishing the route.
        let sandboxes = self.sandboxes.read().await;
        let mut routes = self.proxy_routes.write().await;
        let launch_timeout = plan.timeout();
        let final_metadata = match self
            .store
            .update_if_state(
                &sandbox_id,
                std::slice::from_ref(&transitional_state),
                move |metadata| {
                    metadata.resources = runtime_resources;
                    metadata.state = SandboxState::Running;
                    metadata.update_timeout(launch_timeout);
                },
            )
            .await
        {
            Ok(update) => update.current,
            Err(err) => {
                warn!(error = %format_args!("{err:#}"), "failed to persist final sandbox metadata after launch");
                drop(routes);
                drop(sandboxes);
                self.cleanup_failed_launch(plan, handle, FailedLaunchStage::TransitionalPersisted)
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };
        progress.stage = Some(FailedLaunchStage::RunningPersisted);
        if !self.upsert_proxy_route_if_current_handle(
            &sandboxes,
            &mut routes,
            sandbox_id,
            &handle,
            proxy_target,
        ) {
            debug!("skipping runtime proxy route publication because sandbox handle is stale");
        }
        drop(routes);
        drop(sandboxes);

        info!("sandbox launch completed");
        Ok(final_metadata)
    }

    async fn cleanup_timed_out_launch(&self, plan: &LaunchPlan, progress: LaunchProgress) {
        match (progress.handle, progress.stage) {
            (Some(handle), Some(stage)) => self.cleanup_failed_launch(plan, handle, stage).await,
            _ => {
                self.rollback_failed_launch_metadata(plan, plan.transitional_state())
                    .await;
            }
        }
    }

    fn build_sandbox(&self, plan: &LaunchPlan) -> Result<Box<dyn SandboxBackend>> {
        let build_result = match plan {
            LaunchPlan::Create(plan) => match &plan.source {
                CreateLaunchSource::Snapshot { snapshot } => self
                    .factory
                    .build_from_snapshot(snapshot, plan.launch_config.clone()),
                CreateLaunchSource::Fresh { build_spec } => self
                    .factory
                    .build((**build_spec).clone(), plan.launch_config.clone()),
            },
            LaunchPlan::Resume(plan) => self.factory.build_from_paused_state(
                plan.sandbox_id,
                plan.paused_state.as_ref(),
                plan.envd_access_token.clone(),
            ),
        };
        build_result.map_err(|source| {
            warn!(error = %format_args!("{source:#}"), "failed to build sandbox");
            OrchestratorError::SandboxOperationFailed {
                sandbox_id: plan.sandbox_id(),
                operation: SandboxOperation::Build,
                source,
            }
        })
    }

    async fn build_sandbox_for_launch(
        self: &Arc<Self>,
        plan: &LaunchPlan,
    ) -> Result<Box<dyn SandboxBackend>> {
        let LaunchPlan::Resume(resume) = plan else {
            return self.build_sandbox(plan);
        };

        let sandbox_id = resume.sandbox_id;
        let paused_state = Arc::clone(&resume.paused_state);
        let this = Arc::clone(self);
        let mut build = tokio::task::spawn_blocking(move || {
            this.factory
                .build_from_paused_state(sandbox_id, paused_state.as_ref())
        });

        let build_result = match tokio::time::timeout(self.timeouts.backend_build, &mut build).await
        {
            Ok(Ok(result)) => result,
            Ok(Err(source)) => {
                return Err(OrchestratorError::InternalError(format!(
                    "resume backend build task failed: {source}"
                )))
            }
            Err(_) => {
                warn!(
                    %sandbox_id,
                    timeout = ?self.timeouts.backend_build,
                    "sandbox resume backend build timed out"
                );
                tokio::spawn(async move {
                    match build.await {
                        Ok(Ok(mut backend)) => {
                            if let Err(error) = backend.stop().await {
                                warn!(%sandbox_id, error = %format_args!("{error:#}"), "failed to stop backend that completed after resume build timeout");
                            }
                        }
                        Ok(Err(error)) => {
                            debug!(%sandbox_id, error = %format_args!("{error:#}"), "timed-out resume backend later failed to build");
                        }
                        Err(error) => {
                            warn!(%sandbox_id, error = %error, "timed-out resume backend build task failed");
                        }
                    }
                });
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Build,
                    source: anyhow::anyhow!(
                        "sandbox resume backend build timed out after {:?}",
                        self.timeouts.backend_build
                    ),
                });
            }
        };

        build_result.map_err(|source| {
            warn!(error = %format_args!("{source:#}"), "failed to build sandbox");
            OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Build,
                source,
            }
        })
    }

    async fn cleanup_failed_launch(
        &self,
        plan: &LaunchPlan,
        handle: SandboxHandle,
        stage: FailedLaunchStage,
    ) {
        let should_rollback_shared_state = if stage == FailedLaunchStage::BackendBuilt {
            true
        } else {
            self.detach_launch_runtime_if_current(
                &plan.sandbox_id(),
                &handle,
                stage.should_detach_proxy_route(),
                stage,
            )
            .await
        };

        // Stop the sandbox.
        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        if let Err(err) = stop_result {
            warn!(error = %format_args!("{err:#}"), "failed to stop sandbox while rolling back launch");
        }

        if !should_rollback_shared_state {
            return;
        }

        if let Some(expected_state) = stage.rollback_expected_state(plan) {
            self.rollback_failed_launch_metadata(plan, expected_state)
                .await;
        }
    }

    async fn rollback_failed_launch_metadata(
        &self,
        plan: &LaunchPlan,
        expected_state: SandboxState,
    ) {
        self.release_image_refs(RuntimeImageOwner::StartingSandbox(plan.sandbox_id()))
            .await;
        match plan {
            LaunchPlan::Create(_) => {
                if let Err(err) = self.store.remove(&plan.sandbox_id()).await {
                    warn!(error = %format_args!("{err:#}"), "failed to remove sandbox metadata during launch rollback");
                }
            }
            LaunchPlan::Resume(_) => {
                if let Err(err) = self
                    .store
                    .update_state_if_state(
                        &plan.sandbox_id(),
                        SandboxState::Paused,
                        std::slice::from_ref(&expected_state),
                    )
                    .await
                {
                    warn!(error = %format_args!("{err:#}"), "failed to restore sandbox metadata during launch rollback");
                }
                match tokio::time::timeout(
                    self.timeouts.housekeeping,
                    self.persister.rollback_resuming(&plan.sandbox_id()),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        warn!(error = %err, "failed to restore persisted sandbox record lifecycle during launch rollback")
                    }
                    Err(_) => {
                        warn!(sandbox_id = %plan.sandbox_id(), "resume rollback marker cleanup timed out")
                    }
                }
            }
        }
    }

    async fn detach_launch_runtime_if_current(
        &self,
        sandbox_id: &SandboxId,
        handle: &SandboxHandle,
        detach_proxy_route: bool,
        stage: FailedLaunchStage,
    ) -> bool {
        let mut sandboxes = self.sandboxes.write().await;
        let Some(current_handle) = sandboxes.get(sandbox_id) else {
            return true;
        };

        if !Arc::ptr_eq(current_handle, handle) {
            warn!(
                stage = ?stage,
                "sandbox handle was replaced during failed launch cleanup; skipping shared state rollback"
            );
            return false;
        }

        sandboxes.remove(sandbox_id);

        if detach_proxy_route {
            let removed_route = self.proxy_routes.write().await.remove(sandbox_id);
            if let Some(route) = removed_route.as_ref() {
                debug!(version = route.version(), "removed runtime proxy route");
            }
        }

        drop(sandboxes);
        true
    }

    fn proxy_target_from_sandbox(sandbox: &dyn SandboxBackend) -> Result<ProxyTarget> {
        sandbox
            .host_interaction_ip()
            .map(ProxyTarget::new)
            .ok_or_else(|| {
                warn!("sandbox started without an interaction IP");
                OrchestratorError::InternalError(
                    "sandbox missing host interaction IP after start".to_string(),
                )
            })
    }

    async fn upsert_proxy_route(&self, sandbox_id: SandboxId, target: ProxyTarget) {
        let version = self
            .next_proxy_route_version
            .fetch_add(1, Ordering::Relaxed);
        let route = self
            .proxy_routes
            .write()
            .await
            .upsert(sandbox_id, target, version);
        debug!(
            version = route.version(),
            updated_at = ?route.updated_at(),
            host_interaction_ip = %route.target().ip,
            "updated runtime proxy route"
        );
    }

    fn upsert_proxy_route_if_current_handle(
        &self,
        sandboxes: &HashMap<SandboxId, SandboxHandle>,
        routes: &mut ProxyRouteTable,
        sandbox_id: SandboxId,
        handle: &SandboxHandle,
        target: ProxyTarget,
    ) -> bool {
        // Keep the lock order aligned with detach_sandbox_handle_and_route:
        // sandboxes first, then proxy_routes.
        let Some(current_handle) = sandboxes.get(&sandbox_id) else {
            return false;
        };

        if !Arc::ptr_eq(current_handle, handle) {
            return false;
        }

        let version = self
            .next_proxy_route_version
            .fetch_add(1, Ordering::Relaxed);
        let route = routes.upsert(sandbox_id, target, version);

        debug!(
            version = route.version(),
            updated_at = ?route.updated_at(),
            host_interaction_ip = %route.target().ip,
            "updated runtime proxy route"
        );
        true
    }

    async fn restore_proxy_route(&self, sandbox_id: SandboxId, route: Option<ProxyRoute>) {
        let Some(route) = route else {
            return;
        };
        self.upsert_proxy_route(sandbox_id, route.target().clone())
            .await;
    }

    async fn detach_sandbox_handle_and_route(
        &self,
        sandbox_id: &SandboxId,
    ) -> (Option<SandboxHandle>, Option<ProxyRoute>) {
        // Keep the lock order aligned with upsert_proxy_route_if_current_handle:
        // sandboxes first, then proxy_routes.
        let mut sandboxes = self.sandboxes.write().await;
        let handle = sandboxes.remove(sandbox_id);

        let removed_route = self.proxy_routes.write().await.remove(sandbox_id);
        if let Some(route) = removed_route.as_ref() {
            debug!(version = route.version(), "removed runtime proxy route");
        }

        drop(sandboxes);
        (handle, removed_route)
    }

    async fn run_shutdown_cleanup(self: &Arc<Self>) -> Result<()> {
        const MAX_SHUTDOWN_PASSES: usize = 3;
        let mut last_failures = Vec::new();

        // Preserve recoverable sandboxes by pausing running VMs before process exit.
        for pass in 1..=MAX_SHUTDOWN_PASSES {
            let sandboxes = self
                .store
                .list_filtered(SandboxListFilter {
                    excluded_states: Some(vec![SandboxState::Paused]),
                    ..SandboxListFilter::matches_all()
                })
                .await?;
            if sandboxes.is_empty() {
                break;
            }
            last_failures.clear();

            info!(
                pass,
                remaining = sandboxes.len(),
                "preserving sandboxes during shutdown"
            );

            let results = futures::future::join_all(sandboxes.into_iter().map(|metadata| {
                let this = Arc::clone(self);
                async move {
                    let sandbox_id = metadata.id;
                    match metadata.state {
                        SandboxState::Paused => {
                            unreachable!("paused sandboxes should have been filtered out")
                        }
                        SandboxState::Running => {
                            let result = if metadata.template_builder {
                                this.delete_sandbox_inner(sandbox_id).await
                            } else {
                                this.pause_sandbox_inner(sandbox_id, PauseOrigin::Shutdown).await
                            };
                            result.err().map(|err| format!("{sandbox_id}: {err}"))
                        }
                        SandboxState::Creating
                        | SandboxState::Snapshotting
                        | SandboxState::Forking
                        | SandboxState::Pausing
                        | SandboxState::Resuming
                        | SandboxState::Killing => {
                            match this.wait_for_transition(sandbox_id, metadata.state).await {
                                Ok(_) | Err(OrchestratorError::SandboxNotFound(_)) => None,
                                Err(err) => {
                                    warn!(
                                        sandbox_id = %sandbox_id,
                                        error = ?err,
                                        pass,
                                        "failed to wait for sandbox transition during orchestrator shutdown"
                                    );
                                    Some(format!("{sandbox_id}: {err}"))
                                }
                            }
                        }
                    }
                }
            }))
            .await;
            last_failures.extend(results.into_iter().flatten());

            if last_failures.is_empty() {
                continue;
            }

            warn!(
                pass,
                failures = last_failures.len(),
                max_passes = MAX_SHUTDOWN_PASSES,
                "shutdown preservation pass completed with failures"
            );
        }

        if !last_failures.is_empty() {
            return Err(OrchestratorError::InternalError(format!(
                "failed to preserve all sandboxes during shutdown after {MAX_SHUTDOWN_PASSES} passes: {}",
                last_failures.join(", ")
            )));
        }

        // Clean up remaining network resources.
        if let Some(manager) = crate::sandbox::NetworkManager::global_if_initialized() {
            if let Err(err) = manager.shutdown() {
                warn!(error = ?err, "failed to clean up network resources during orchestrator shutdown");
            }
        }

        info!("orchestrator shutdown completed");
        Ok(())
    }

    fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Acquire)
    }

    fn ensure_accepting_lifecycle_operations(&self) -> Result<()> {
        if self.is_shutting_down() {
            info!("rejecting lifecycle operation because orchestrator is shutting down");
            return Err(OrchestratorError::ShuttingDown);
        }

        Ok(())
    }

    pub(crate) fn ensure_disk_admission(&self, operation: &'static str) -> Result<()> {
        let cleanup = self.persister.cleanup_metrics();
        let snapshot = self.disk_policy.refresh(cleanup.pending).map_err(|error| {
            OrchestratorError::InternalError(format!(
                "refresh runtime disk admission before {operation}: {error:#}"
            ))
        })?;
        if snapshot.accepting_sandboxes {
            return Ok(());
        }
        self.disk_admission_rejections
            .fetch_add(1, Ordering::Relaxed);
        Err(OrchestratorError::AdmissionBlocked {
            operation,
            reason: snapshot.reason.as_str(),
        })
    }

    fn ensure_pause_admission(&self, origin: PauseOrigin) -> Result<()> {
        match origin {
            PauseOrigin::Api => self.ensure_disk_admission("pause"),
            // Pausing an expired sandbox frees CPU/memory, so only the hard
            // limit (where the snapshot write itself risks ENOSPC) defers it.
            PauseOrigin::AutoEvict => {
                let cleanup = self.persister.cleanup_metrics();
                let snapshot = self.disk_policy.refresh(cleanup.pending).map_err(|error| {
                    OrchestratorError::InternalError(format!(
                        "refresh runtime disk admission before auto-evict pause: {error:#}"
                    ))
                })?;
                if !matches!(
                    snapshot.reason,
                    DiskAdmissionReason::DiskHardLimit | DiskAdmissionReason::DiskUsageUnavailable
                ) {
                    return Ok(());
                }
                self.disk_admission_rejections
                    .fetch_add(1, Ordering::Relaxed);
                Err(OrchestratorError::AdmissionBlocked {
                    operation: "auto-evict pause",
                    reason: snapshot.reason.as_str(),
                })
            }
            // The alternative to a shutdown pause is losing the VM, and a
            // failed persist already rolls back; never probe or refuse here.
            PauseOrigin::Shutdown => Ok(()),
        }
    }
}

/// Why a pause was requested; internal pauses bypass the admission watermarks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PauseOrigin {
    Api,
    AutoEvict,
    Shutdown,
}

async fn path_tree_bytes(path: PathBuf) -> u64 {
    tokio::task::spawn_blocking(move || {
        let mut total = 0u64;
        let mut pending = vec![path];
        while let Some(path) = pending.pop() {
            let Ok(metadata) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if metadata.is_file() {
                total = total.saturating_add(metadata.len());
                continue;
            }
            if !metadata.is_dir() {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(path) else {
                continue;
            };
            pending.extend(entries.filter_map(|entry| entry.ok().map(|entry| entry.path())));
        }
        total
    })
    .await
    .unwrap_or(0)
}

fn persisted_sandboxes_require_managed_seed(persisted: &[SandboxMetadata]) -> bool {
    persisted
        .iter()
        .any(|metadata| metadata.secure || !metadata.network_policy.allow_public_traffic)
}

#[cfg(test)]
impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    pub(crate) async fn set_proxy_target_for_test(
        &self,
        sandbox_id: SandboxId,
        target: ProxyTarget,
        state: SandboxState,
    ) {
        self.set_metadata_state_for_test(sandbox_id, state)
            .await
            .expect("seed proxy metadata state for test");

        if state == SandboxState::Running {
            self.upsert_proxy_route(sandbox_id, target).await;
        } else {
            let _ = self.proxy_routes.write().await.remove(&sandbox_id);
        }
    }

    pub(crate) async fn set_metadata_state_for_test(
        &self,
        sandbox_id: SandboxId,
        state: SandboxState,
    ) -> Result<()> {
        let existing = self.store.get(&sandbox_id).await?;
        match existing {
            Some(mut metadata) => {
                metadata.state = state;
                self.store.update(metadata).await?;
            }
            None => {
                let metadata = SandboxMetadata {
                    id: sandbox_id,
                    state,
                    ..Default::default()
                };
                self.store.add(metadata.clone()).await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn set_auto_resume_for_test(
        &self,
        sandbox_id: &SandboxId,
        auto_resume_enabled: bool,
    ) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };

        metadata.auto_resume = auto_resume_enabled;
        self.store.update(metadata).await?;

        Ok(())
    }

    pub(crate) async fn set_secure_for_test(
        &self,
        sandbox_id: &SandboxId,
        secure: bool,
    ) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };
        metadata.secure = secure;
        self.store.update(metadata).await?;
        Ok(())
    }

    pub(crate) async fn set_template_builder_for_test(&self, sandbox_id: &SandboxId) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };
        metadata.template_builder = true;
        self.store.update(metadata).await?;
        Ok(())
    }

    pub(crate) async fn set_allow_public_traffic_for_test(
        &self,
        sandbox_id: &SandboxId,
        allow_public_traffic: bool,
    ) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };
        metadata.network_policy.allow_public_traffic = allow_public_traffic;
        self.store.update(metadata).await?;
        Ok(())
    }

    pub(crate) async fn remove_proxy_route_for_test(&self, sandbox_id: &SandboxId) {
        let _ = self.proxy_routes.write().await.remove(sandbox_id);
    }
}

fn default_fresh_sandbox_resources() -> SandboxResources {
    let config = ConfigManager::global_config();
    SandboxResources {
        cpu_count: config.machine.vcpu_count,
        memory_mib: config.machine.mem_size_mib,
        // Filled from backend runtime info after the rootfs device is created.
        disk_size_mib: 0,
    }
}

fn resources_with_runtime_info(
    mut resources: SandboxResources,
    runtime_info: SandboxRuntimeInfo,
) -> SandboxResources {
    // This API resource field tracks the rootfs block device size. Attached
    // drives are separately configured storage and are not folded into it.
    if let Some(size) = runtime_info.rootfs_virtual_size {
        resources.disk_size_mib = bytes_to_mib_ceil(size);
    }
    resources
}

fn configured_runtime_versions() -> SnapshotRuntimeVersions {
    let config = ConfigManager::global_config();
    SnapshotRuntimeVersions::new(
        config
            .kernel
            .version
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        config
            .firecracker
            .version
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        config.envd.version.clone(),
        config.resolved_tools_version().to_string(),
    )
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
