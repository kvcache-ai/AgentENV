use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex as StdMutex,
};
use std::time::{Duration, SystemTime};

use tokio::sync::{broadcast, oneshot, watch, Mutex, OnceCell, RwLock};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, trace, warn};

use crate::cfg::ConfigManager;
use crate::image::cache::{
    local_image_services_from_global_config, RuntimeImageOwner, RuntimeImageRefs,
};
use crate::sandbox::{
    CustomExtensionClient, CustomExtensionParams, FirecrackerSandboxFactory, FreshSandboxBuildSpec,
    PausedSandboxState, RuntimeArtifactSet, SandboxBackend, SandboxBackendFactory,
    SandboxLaunchConfig, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::snapshot::SnapshotRuntimeVersions;
use crate::types::{bytes_to_mib_ceil, SandboxId, SandboxResources};

use super::launch_plan::{CreateLaunchSource, LaunchPlan};
use super::metrics::{
    aggregate_resource_metrics, OrchestratorCounters, OrchestratorMetrics, SandboxContribution,
};
use super::persistence::{DisabledSandboxPersister, FileBackedSandboxPersister, SandboxPersister};
use super::proxy::{ProxyLookupResult, ProxyRoute, ProxyRouteTable, ProxyTarget};
use super::store::*;
use super::types::{
    CreateSandboxRequest, SandboxLaunchSource, SandboxLifecycleEvent, SandboxLifecycleEventType,
    SandboxState, SnapshotCaptureResult,
};
use super::{OrchestratorError, Result, SandboxForkOutcome, SandboxOperation};

type SandboxHandle = Arc<Mutex<Box<dyn SandboxBackend>>>;
type LifecycleOperationLock = Arc<Mutex<LifecycleOperationState>>;
type LifecycleOperationMap = Arc<StdMutex<HashMap<SandboxId, LifecycleOperationLock>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleOperationKind {
    Delete,
    Pause,
}

#[derive(Debug)]
struct LifecycleOperationState {
    kind: LifecycleOperationKind,
    delete_succeeded: bool,
}

struct LifecycleOperationLease {
    sandbox_id: SandboxId,
    operation_lock: LifecycleOperationLock,
    operations: LifecycleOperationMap,
}

impl Drop for LifecycleOperationLease {
    fn drop(&mut self) {
        let mut operations = self
            .operations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if Arc::strong_count(&self.operation_lock) == 2
            && operations
                .get(&self.sandbox_id)
                .is_some_and(|current| Arc::ptr_eq(current, &self.operation_lock))
        {
            operations.remove(&self.sandbox_id);
        }
    }
}

/// Maximum time to wait for a sandbox to leave a transitional state.
/// Guards against indefinite blocking when a sandbox's in-progress operation
/// never completes (e.g. the task holding the state panics without rolling back).
const WAIT_TRANSITION_TIMEOUT: Duration = Duration::from_secs(60);
const SANDBOX_EVENT_CHANNEL_CAPACITY: usize = 1024;

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
    Registered,
    TransitionalPersisted,
    RunningPersisted,
}

impl FailedLaunchStage {
    fn rollback_expected_state(self, plan: &LaunchPlan) -> Option<SandboxState> {
        match self {
            Self::Registered => None,
            Self::TransitionalPersisted => Some(plan.transitional_state()),
            Self::RunningPersisted => Some(SandboxState::Running),
        }
    }

    fn should_detach_proxy_route(self) -> bool {
        matches!(self, Self::RunningPersisted)
    }
}

pub struct Orchestrator<
    S: MetadataStore = InMemoryMetadataStore,
    F: SandboxBackendFactory = FirecrackerSandboxFactory,
    P: SandboxPersister = FileBackedSandboxPersister,
> {
    store: S,
    factory: F,
    persister: P,
    lifecycle_operations: LifecycleOperationMap,
    sandboxes: RwLock<HashMap<SandboxId, SandboxHandle>>,
    proxy_routes: RwLock<ProxyRouteTable>,
    next_proxy_route_version: AtomicU64,
    counters: OrchestratorCounters,
    sandbox_event_tx: broadcast::Sender<SandboxLifecycleEvent>,
    default_sandbox_timeout: Duration,
    is_shutting_down: std::sync::atomic::AtomicBool,
    shutdown_tx: watch::Sender<bool>,
    shutdown_outcome: OnceCell<ShutdownOutcome>,
    image_refs: Arc<dyn RuntimeImageRefs>,
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
        );
        Self::new(store, factory, persister).await
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
        Self::new_inner(store, factory, persister, image_refs).await
    }

    async fn new_inner(
        store: S,
        factory: F,
        persister: P,
        image_refs: Arc<dyn RuntimeImageRefs>,
    ) -> Result<Arc<Self>> {
        let app_config = ConfigManager::global_config();
        let config = &app_config.orchestrator;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (sandbox_event_tx, _sandbox_event_rx) =
            broadcast::channel(SANDBOX_EVENT_CHANNEL_CAPACITY);

        // Restore persisted sandboxes from the previous run, keeping the paused
        // ones (with their state) for the paused-protection reconcile below.
        let persisted = persister.load_all(&factory).await?;
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
        for metadata in persisted {
            store.add(metadata).await?;
        }

        let orchestrator = Arc::new(Self {
            store,
            factory,
            persister,
            lifecycle_operations: Arc::new(StdMutex::new(HashMap::new())),
            sandboxes: RwLock::new(HashMap::new()),
            proxy_routes: RwLock::new(ProxyRouteTable::default()),
            next_proxy_route_version: AtomicU64::new(1),
            counters: OrchestratorCounters::default(),
            sandbox_event_tx,
            default_sandbox_timeout: Duration::from_secs(config.default_sandbox_timeout_secs),
            is_shutting_down: std::sync::atomic::AtomicBool::new(false),
            shutdown_tx,
            shutdown_outcome: OnceCell::new(),
            image_refs,
        });

        // Start the auto-evict task.
        let evict_interval = Duration::from_millis(config.auto_evict_interval_ms);
        Self::start_auto_evict_task(Arc::clone(&orchestrator), evict_interval, shutdown_rx);

        // Reconcile durable paused protection, then start maintenance (fail-closed).
        let gc = app_config.image.cache.gc_schedule();
        if gc.enabled {
            match orchestrator
                .reconcile_paused_at_startup(&restored_paused)
                .await
            {
                Ok(()) => {
                    Self::start_local_image_maintenance_task(
                        Arc::clone(&orchestrator),
                        gc.interval,
                        orchestrator.shutdown_tx.subscribe(),
                    );
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "local image protection reconcile failed at startup; not starting maintenance (fail-closed)"
                    );
                }
            }
        }

        Ok(orchestrator)
    }

    async fn run_lifecycle_operation<T>(
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
                    "lifecycle operation completed after caller stopped waiting"
                );
            }
        });

        rx.await.map_err(|err| {
            OrchestratorError::InternalError(format!(
                "lifecycle operation task ended before reporting result: {err}"
            ))
        })?
    }

    fn lifecycle_operation_lock(
        &self,
        sandbox_id: SandboxId,
        operation: LifecycleOperationKind,
    ) -> (LifecycleOperationLease, bool) {
        let mut operations = self
            .lifecycle_operations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let already_existed = operations.contains_key(&sandbox_id);
        let operation_lock = Arc::clone(operations.entry(sandbox_id).or_insert_with(|| {
            Arc::new(Mutex::new(LifecycleOperationState {
                kind: operation,
                delete_succeeded: false,
            }))
        }));
        drop(operations);
        (
            LifecycleOperationLease {
                sandbox_id,
                operation_lock,
                operations: Arc::clone(&self.lifecycle_operations),
            },
            already_existed,
        )
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

    async fn retain_sandbox_handle_for_cleanup(
        &self,
        sandbox_id: SandboxId,
        handle: SandboxHandle,
    ) -> std::result::Result<(), StoreError> {
        match self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Killing, &[SandboxState::Pausing])
            .await
        {
            Ok(_)
            | Err(StoreError::StateConflict {
                actual_state: SandboxState::Killing,
                ..
            }) => {
                self.sandboxes.write().await.insert(sandbox_id, handle);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn remove_stopped_sandbox_or_retain_handle(
        &self,
        sandbox_id: SandboxId,
        handle: SandboxHandle,
    ) -> Option<anyhow::Error> {
        let cleanup_error = self.store.remove(&sandbox_id).await.err()?;
        match self
            .retain_sandbox_handle_for_cleanup(sandbox_id, handle)
            .await
        {
            Ok(()) => Some(cleanup_error.into()),
            Err(state_error) => Some(anyhow::anyhow!(
                "metadata cleanup error: {cleanup_error}; cleanup state error: {state_error}"
            )),
        }
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
        self.run_lifecycle_operation("create", sandbox_id, async move {
            this.create_sandbox_inner(sandbox_id, request).await
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
    ) -> Result<SandboxMetadata> {
        if let Err(err) = self.ensure_accepting_lifecycle_operations() {
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
        } = request;
        info!(timeout = ?timeout, "creating sandbox");

        let result = match source {
            SandboxLaunchSource::Snapshot(snapshot) => {
                let record = snapshot.record();
                let committed = snapshot.committed();
                let launch_image_configs = committed.image_configs.clone();
                // Effective custom config: a launch-provided value overrides the
                // one persisted in the source snapshot; otherwise inherit it.
                // Store the effective value so publishing a snapshot from this
                // sandbox keeps the inherited config instead of dropping it.
                let effective_custom_extension_params = custom_extension_params
                    .clone()
                    .or_else(|| committed.custom_extension_params.clone());
                let mut launch_config = SandboxLaunchConfig::new(sandbox_id, record.id.to_string());
                launch_config.env_vars = env_vars;
                launch_config.network = network_policy.runtime_policy();
                launch_config.custom_extension_params = effective_custom_extension_params.clone();
                let launch_config = launch_config.with_image_configs(&launch_image_configs);

                let transitional_metadata = SandboxMetadata {
                    id: sandbox_id,
                    snapshot_id: record.id.to_string(),
                    snapshot_alias: record.alias.as_ref().map(ToString::to_string),
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
                extra_drives,
                extra_boot_args,
                image_configs,
            } => {
                let context = *context;
                let resources = resources.unwrap_or_else(default_fresh_sandbox_resources);
                let launch_image_configs = *image_configs;
                let mut launch_config = SandboxLaunchConfig::new(sandbox_id, image_ref.clone());
                launch_config.env_vars = env_vars;
                launch_config.network = network_policy.runtime_policy();
                launch_config.custom_extension_params = custom_extension_params.clone();
                let launch_config = launch_config.with_image_configs(&launch_image_configs);
                let build_spec = FreshSandboxBuildSpec {
                    image_config_path: overlaybd_config_path,
                    context: context.clone(),
                    resources,
                    extra_drives,
                    extra_boot_args,
                };

                let transitional_metadata = SandboxMetadata {
                    id: sandbox_id,
                    snapshot_id: image_ref,
                    snapshot_alias: None,
                    runtime_versions: configured_runtime_versions(),
                    resources,
                    context,
                    image_configs: launch_image_configs,
                    timeout_action,
                    auto_resume,
                    user_metadata,
                    network_policy,
                    custom_extension_params,
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
        let this = Arc::clone(self);
        self.run_lifecycle_operation("fork", source_sandbox_id, async move {
            this.fork_sandbox_inner(source_sandbox_id, count, new_timeout)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "fork_sandbox",
        skip(self),
        fields(source_sandbox_id = %source_sandbox_id, count)
    )]
    async fn fork_sandbox_inner(
        self: Arc<Self>,
        source_sandbox_id: SandboxId,
        count: u32,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        self.ensure_accepting_lifecycle_operations()?;

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

        let child_ids = (0..count).map(|_| SandboxId::new()).collect::<Vec<_>>();

        // Start to fork the sandbox.
        // This is a single operation that will return a list of results for each child sandbox.
        let fork_result = {
            let mut sandbox = source_handle.lock().await;
            sandbox.fork(&child_ids).await
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
        let mut outcomes = Vec::with_capacity(child_ids.len());
        let mut successes = 0u64;
        let now = SystemTime::now();
        for (sandbox_id, backend) in child_ids.into_iter().zip(forked_backends) {
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
        Ok(self.store.list().await?)
    }

    /// Lists all sandbox IDs currently tracked by the store.
    pub async fn list_sandbox_ids(&self) -> Result<Vec<SandboxId>> {
        Ok(self.store.list_ids().await?)
    }

    /// Lists sandboxes that match the provided filter criteria:
    /// - If `states` is provided, only sandboxes in those states will be included.
    /// - If `user_metadata` is provided, only sandboxes whose user metadata contains
    ///   all the specified key-value pairs will be included.
    #[tracing::instrument(skip(self, filter))]
    pub async fn list_sandboxes_filtered(
        &self,
        filter: SandboxListFilter,
    ) -> Result<Vec<SandboxMetadata>> {
        Ok(self.store.list_filtered(filter).await?)
    }

    /// Resolves the current proxyability of a sandbox without touching the sandbox mutex.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id))]
    pub async fn proxy_lookup_for(&self, sandbox_id: &SandboxId) -> Result<ProxyLookupResult> {
        let metadata = self.store.get(sandbox_id).await?;
        Ok(match metadata {
            None => {
                debug!("sandbox has no runtime route or persisted metadata");
                ProxyLookupResult::NotFound
            }
            Some(metadata) if metadata.state == SandboxState::Running => {
                match self.proxy_routes.read().await.route(sandbox_id).cloned() {
                    Some(route) => {
                        trace!(
                            version = route.version(),
                            "resolved running proxy target from runtime table"
                        );
                        ProxyLookupResult::Ready(route.target().clone())
                    }
                    None => {
                        warn!("running sandbox is missing a runtime proxy route");
                        ProxyLookupResult::RouteMissing
                    }
                }
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
        self.run_lifecycle_operation("delete", sandbox_id, async move {
            this.delete_sandbox_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "delete_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn delete_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        info!("deleting sandbox");

        let (operation_lease, already_existed) =
            self.lifecycle_operation_lock(sandbox_id, LifecycleOperationKind::Delete);
        let mut operation_guard = operation_lease.operation_lock.lock().await;
        let prior_delete_succeeded = already_existed && operation_guard.delete_succeeded;
        operation_guard.kind = LifecycleOperationKind::Delete;
        let result = self
            .delete_sandbox_owned_inner(sandbox_id, prior_delete_succeeded)
            .await;
        operation_guard.delete_succeeded |= result.is_ok();
        drop(operation_guard);
        drop(operation_lease);
        result
    }

    async fn delete_sandbox_owned_inner(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        prior_delete_succeeded: bool,
    ) -> Result<()> {
        // Attempt to transition to Killing, retrying after waiting whenever we
        // find the sandbox in a transitional state.
        let previous_state = loop {
            match self
                .store
                .update_state_if_state(
                    &sandbox_id,
                    SandboxState::Killing,
                    &[
                        SandboxState::Running,
                        SandboxState::Paused,
                        SandboxState::Pausing,
                    ],
                )
                .await
            {
                Ok(previous_state) => break previous_state,
                Err(StoreError::StateConflict { actual_state, .. }) => match actual_state {
                    SandboxState::Killing => {
                        // Cleanup ownership for this sandbox is serialized.
                        // Reaching Killing here means a previous cleanup attempt
                        // completed and left retryable work.
                        debug!("retrying sandbox cleanup from killing state");
                        break SandboxState::Killing;
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
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
                            expected_states: vec![
                                SandboxState::Running,
                                SandboxState::Paused,
                                SandboxState::Pausing,
                            ],
                            actual_state,
                        }));
                    }
                },
                Err(StoreError::SandboxNotFound { .. }) if prior_delete_succeeded => {
                    info!("sandbox was deleted by a concurrent cleanup");
                    return Ok(());
                }
                Err(err) => {
                    return Err(OrchestratorError::from(err));
                }
            }
        };

        let (handle, removed_route) = self.detach_sandbox_handle_and_route(&sandbox_id).await;

        // If the sandbox is still in memory, attempt to stop it.
        if let Some(handle) = handle {
            let stop_result = {
                let mut sandbox = handle.lock().await;
                sandbox.stop().await
            };

            if let Err(err) = stop_result {
                warn!(error = ?err, "failed to stop sandbox during delete");
                self.sandboxes.write().await.insert(sandbox_id, handle);
                if matches!(previous_state, SandboxState::Running | SandboxState::Paused) {
                    self.restore_proxy_route(sandbox_id, removed_route).await;
                    self.store
                        .update_state_if_state(
                            &sandbox_id,
                            previous_state,
                            &[SandboxState::Killing],
                        )
                        .await?;
                }

                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Stop,
                    source: err,
                });
            }
        }

        // Now the sandbox is successfully stopped, remove its metadata.
        let metadata = self.store.remove(&sandbox_id).await?;
        if let Some(metadata) = metadata {
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Delete,
                metadata.id,
                metadata.resources,
            );
        }
        if let Err(err) = self
            .persister
            .delete_record_and_artifacts(&sandbox_id)
            .await
        {
            warn!(error = ?err, "failed to delete persisted sandbox state");
        }
        self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
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
        self.run_lifecycle_operation("pause", sandbox_id, async move {
            this.pause_sandbox_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "pause_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn pause_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let (operation_lease, _) =
            self.lifecycle_operation_lock(sandbox_id, LifecycleOperationKind::Pause);
        let operation_guard = match operation_lease.operation_lock.try_lock() {
            Ok(mut operation_guard) => {
                operation_guard.kind = LifecycleOperationKind::Pause;
                operation_guard
            }
            Err(_) => {
                let mut operation_guard = operation_lease.operation_lock.lock().await;
                if operation_guard.kind == LifecycleOperationKind::Pause {
                    drop(operation_guard);
                    return self.join_concurrent_pause(sandbox_id).await;
                }
                operation_guard.kind = LifecycleOperationKind::Pause;
                operation_guard
            }
        };
        let result = self.pause_sandbox_owned_inner(sandbox_id).await;
        drop(operation_guard);
        drop(operation_lease);
        result
    }

    async fn pause_sandbox_owned_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        info!("pausing sandbox");
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
                RuntimeImageOwner::PausedSandbox(sandbox_id),
                runtime_artifacts,
                "paused sandbox",
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

        let (handle, removed_proxy_route) = self.detach_sandbox_handle_and_route(&sandbox_id).await;

        let Some(handle) = handle else {
            warn!("sandbox handle not found while pausing, removing from store");
            self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                .await;
            self.store.remove(&sandbox_id).await?;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        };

        // Pause the sandbox and capture the paused state for resuming later.
        let artifact_root = self.persister.allocate_artifact_root(&sandbox_id).await?;

        let paused_state_result = {
            let mut sandbox = handle.lock().await;
            sandbox.pause(artifact_root.as_deref()).await
        };

        // If pausing failed, attempt to put the sandbox back and return an error.
        let paused_state = match paused_state_result {
            Ok(s) => s,
            Err(err) => {
                warn!(error = ?err, "failed to pause sandbox");
                let (source, release_paused_refs) = if err.is_terminal() {
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
                    self.store.remove(&sandbox_id).await?;
                    (err.into(), true)
                } else {
                    let resume_result = {
                        let mut sandbox = handle.lock().await;
                        sandbox.resume().await
                    };
                    match resume_result {
                        Ok(()) => {
                            // Publish the runtime while metadata is still Pausing.
                            // Proxy lookup gates routes on Running metadata, and
                            // lifecycle operations for this sandbox are serialized,
                            // so the final CAS makes the restored runtime visible.
                            let restored_route_version = {
                                let mut sandboxes = self.sandboxes.write().await;
                                sandboxes.insert(sandbox_id, Arc::clone(&handle));
                                drop(sandboxes);

                                if let Some(route) = removed_proxy_route.as_ref() {
                                    let version = self
                                        .next_proxy_route_version
                                        .fetch_add(1, Ordering::Relaxed);
                                    self.proxy_routes.write().await.upsert(
                                        sandbox_id,
                                        route.target().clone(),
                                        version,
                                    );
                                    Some(version)
                                } else {
                                    None
                                }
                            };
                            let restore_result = self
                                .store
                                .update_state_if_state(
                                    &sandbox_id,
                                    SandboxState::Running,
                                    &[SandboxState::Pausing],
                                )
                                .await
                                .map(|_| ());
                            if restore_result.is_err() {
                                let mut sandboxes = self.sandboxes.write().await;
                                if sandboxes
                                    .get(&sandbox_id)
                                    .is_some_and(|current| Arc::ptr_eq(current, &handle))
                                {
                                    sandboxes.remove(&sandbox_id);
                                }
                                drop(sandboxes);

                                if let Some(version) = restored_route_version {
                                    let mut proxy_routes = self.proxy_routes.write().await;
                                    if proxy_routes
                                        .route(&sandbox_id)
                                        .is_some_and(|route| route.version() == version)
                                    {
                                        proxy_routes.remove(&sandbox_id);
                                    }
                                }
                            }

                            match restore_result {
                                Ok(()) => (err.into(), true),
                                Err(restore_err) => {
                                    warn!(
                                        error = ?restore_err,
                                        "failed to restore Running state after pause recovery"
                                    );
                                    let stop_result = {
                                        let mut sandbox = handle.lock().await;
                                        sandbox.stop().await
                                    };
                                    match stop_result {
                                        Ok(()) => {
                                            let cleanup_error = self
                                                .remove_stopped_sandbox_or_retain_handle(
                                                    sandbox_id, handle,
                                                )
                                                .await;
                                            let source = match cleanup_error {
                                                Some(cleanup_err) => anyhow::anyhow!(
                                                    "pause failed and Running state could not be \
                                                     restored: pause error: {err}; state error: \
                                                     {restore_err}; metadata cleanup error: \
                                                     {cleanup_err}"
                                                ),
                                                None => anyhow::anyhow!(
                                                    "pause failed and Running state could not be \
                                                     restored: pause error: {err}; state error: \
                                                     {restore_err}"
                                                ),
                                            };
                                            (source, true)
                                        }
                                        Err(stop_err) => {
                                            warn!(
                                                error = ?stop_err,
                                                "failed to stop sandbox after state recovery failure"
                                            );
                                            let cleanup_state_error = self
                                                .retain_sandbox_handle_for_cleanup(
                                                    sandbox_id, handle,
                                                )
                                                .await;
                                            let source = match cleanup_state_error {
                                                Ok(()) => anyhow::anyhow!(
                                                    "pause failed, Running state could not be \
                                                     restored, and sandbox could not be stopped: \
                                                     pause error: {err}; state error: \
                                                     {restore_err}; stop error: {stop_err:#}"
                                                ),
                                                Err(cleanup_state_err) => anyhow::anyhow!(
                                                    "pause failed, Running state could not be \
                                                     restored, sandbox could not be stopped, and \
                                                     cleanup state could not be established: pause \
                                                     error: {err}; state error: {restore_err}; stop \
                                                     error: {stop_err}; cleanup state error: \
                                                     {cleanup_state_err}"
                                                ),
                                            };
                                            (source, false)
                                        }
                                    }
                                }
                            }
                        }
                        Err(resume_err) => {
                            warn!(
                                error = ?resume_err,
                                "failed to resume sandbox after recoverable pause failure"
                            );
                            let stop_result = {
                                let mut sandbox = handle.lock().await;
                                sandbox.stop().await
                            };
                            match stop_result {
                                Ok(()) => {
                                    let cleanup_error = self
                                        .remove_stopped_sandbox_or_retain_handle(sandbox_id, handle)
                                        .await;
                                    let source = match cleanup_error {
                                        Some(cleanup_err) => anyhow::anyhow!(
                                            "pause failed and sandbox could not be resumed: pause \
                                             error: {err}; resume error: {resume_err}; metadata \
                                             cleanup error: {cleanup_err}"
                                        ),
                                        None => anyhow::anyhow!(
                                            "pause failed and sandbox could not be resumed: pause \
                                             error: {err}; resume error: {resume_err:#}"
                                        ),
                                    };
                                    (source, true)
                                }
                                Err(stop_err) => {
                                    warn!(
                                        error = ?stop_err,
                                        "failed to stop sandbox after pause recovery failure"
                                    );
                                    // The backend may still own live resources.
                                    // Keep its handle in a stable cleanup state so
                                    // a later delete can retry stopping it.
                                    let cleanup_state_error = self
                                        .retain_sandbox_handle_for_cleanup(sandbox_id, handle)
                                        .await;
                                    let source = match cleanup_state_error {
                                        Ok(()) => anyhow::anyhow!(
                                            "pause failed, sandbox could not be resumed, and \
                                             sandbox could not be stopped: pause error: {err}; \
                                             resume error: {resume_err}; stop error: {stop_err:#}"
                                        ),
                                        Err(cleanup_state_err) => anyhow::anyhow!(
                                            "pause failed, sandbox could not be resumed or stopped, \
                                             and cleanup state could not be established: pause \
                                             error: {err}; resume error: {resume_err}; stop error: \
                                             {stop_err}; cleanup state error: {cleanup_state_err}"
                                        ),
                                    };
                                    (source, false)
                                }
                            }
                        }
                    }
                };
                if release_paused_refs {
                    self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                        .await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Pause,
                    source,
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
        if let Err(err) = self
            .persister
            .persist_paused(
                &persisted_metadata,
                artifact_root.as_deref(),
                paused_state.as_ref(),
            )
            .await
        {
            warn!(error = ?err, "failed to persist paused sandbox state");
            let resume_result = {
                let mut sandbox = handle.lock().await;
                sandbox.resume().await
            };
            if let Err(resume_err) = resume_result {
                warn!(error = ?resume_err, "failed to resume sandbox after pause failure");
                let stop_result = {
                    let mut sandbox = handle.lock().await;
                    sandbox.stop().await
                };
                if let Err(stop_err) = stop_result {
                    warn!(error = ?stop_err, "failed to stop sandbox after pause failure");
                }
                if let Err(error) = self.store.remove(&sandbox_id).await {
                    warn!(error = ?error, "failed to remove sandbox after pause failure");
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
            self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                .await;
            return Err(OrchestratorError::InternalError(format!(
                "failed to persist paused sandbox state: {err:#}"
            )));
        }
        let resources = persisted_metadata.resources;
        self.store.update(persisted_metadata).await?;

        // Stop the sandbox to free up resources.
        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        if let Err(err) = stop_result {
            warn!(error = ?err, "failed to stop sandbox after pausing");
        }
        self.publish_sandbox_event(SandboxLifecycleEventType::Pause, sandbox_id, resources);
        info!("sandbox paused");

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
        self.run_lifecycle_operation("resume", sandbox_id, async move {
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

        let paused_state = metadata.paused_state.as_ref().ok_or_else(|| {
            warn!("missing paused state while resuming");
            OrchestratorError::InternalError("missing paused state".to_string())
        })?;

        let resumed = self
            .launch_sandbox(LaunchPlan::for_resume(
                sandbox_id,
                Arc::clone(paused_state),
                timeout,
                metadata.resources,
            ))
            .await;
        if let Ok(metadata) = resumed.as_ref() {
            self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                .await;
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
        self.run_lifecycle_operation("snapshot", sandbox_id, async move {
            this.capture_snapshot_inner(sandbox_id).await
        })
        .await
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

        info!("capturing sandbox snapshot");
        match self
            .store
            .update_state_if_state(
                &sandbox_id,
                SandboxState::Snapshotting,
                &[SandboxState::Running],
            )
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    SandboxState::Killing => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
                    _ => Err(OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }),
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        // Get the sandbox handle.
        let handle = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&sandbox_id).cloned()
        };
        let Some(handle) = handle else {
            warn!("sandbox handle not found while snapshotting, removing from store");
            self.detach_sandbox_handle_and_route(&sandbox_id).await;
            self.store.remove(&sandbox_id).await?;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        };

        // Call sandbox backend to capture the snapshot.
        let captured_snapshot_result = {
            let mut sandbox = handle.lock().await;
            sandbox.snapshot().await
        };

        // If snapshot capture failed, attempt to roll back to Running state and return an error.
        let captured_snapshot = match captured_snapshot_result {
            Ok(captured_snapshot) => captured_snapshot,
            Err(err) => {
                warn!(error = ?err, "failed to capture sandbox snapshot");
                if err.is_terminal() {
                    self.detach_sandbox_handle_and_route(&sandbox_id).await;
                    let stop_result = {
                        let mut sandbox = handle.lock().await;
                        sandbox.stop().await
                    };
                    if let Err(stop_err) = stop_result {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal snapshot failure");
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
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Snapshot,
                    source: err.into(),
                });
            }
        };

        // Update the sandbox state back to Running and return the captured snapshot along with the latest metadata.
        self.store
            .update_state_if_state(
                &sandbox_id,
                SandboxState::Running,
                &[SandboxState::Snapshotting],
            )
            .await?;
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

    #[tracing::instrument(skip(self, network_policy), fields(sandbox_id = %sandbox_id))]
    pub async fn replace_sandbox_network_policy(
        &self,
        sandbox_id: SandboxId,
        network_policy: SandboxNetworkPolicy,
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
    #[tracing::instrument(skip(self, patch), fields(sandbox_id = %sandbox_id))]
    pub async fn patch_sandbox_custom_extension_params(
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
    /// transitional state after the [`WAIT_TRANSITION_TIMEOUT`] elapses.
    async fn wait_for_transition(
        &self,
        sandbox_id: SandboxId,
        transitional_state: SandboxState,
    ) -> Result<SandboxMetadata> {
        let states = [transitional_state];
        let wait = self.store.wait_while_in_states(&sandbox_id, &states);
        match tokio::time::timeout(WAIT_TRANSITION_TIMEOUT, wait).await {
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

        let expired = self.store.list_expired(SystemTime::now()).await?;
        let mut evicted_ids = Vec::new();

        for metadata in expired {
            if metadata.state != SandboxState::Running {
                continue;
            }
            if let Err(err) = match metadata.timeout_action {
                SandboxTimeoutAction::Pause => self.pause_sandbox(metadata.id).await,
                SandboxTimeoutAction::Delete => self.delete_sandbox(metadata.id).await,
            } {
                warn!(
                    sandbox_id = %metadata.id,
                    action = ?metadata.timeout_action,
                    error = ?err,
                    "failed to auto-evict expired sandbox"
                );
                continue;
            }
            evicted_ids.push(metadata.id);
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
            let mut ticker = tokio::time::interval(interval);
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

    #[tracing::instrument(skip(self, plan))]
    async fn launch_sandbox(self: &Arc<Self>, plan: LaunchPlan) -> Result<SandboxMetadata> {
        self.ensure_accepting_lifecycle_operations()?;

        let sandbox_id = plan.sandbox_id();
        let transitional_state = plan.transitional_state();

        // Build and start the sandbox first, before making any state changes, so that we don't
        // have to roll back any persisted state if the build fails.
        // Meanwhile, the start process can be overlapped with the initial state persistence.
        let mut sandbox = match self.build_sandbox(&plan) {
            Ok(sandbox) => sandbox,
            Err(err) => {
                self.rollback_failed_launch_metadata(&plan, transitional_state)
                    .await;
                return Err(err);
            }
        };

        // Protect artifacts before the backend opens them.
        let startup_artifacts = sandbox.startup_artifacts();
        if let Err(err) = self
            .protect_image_refs(
                RuntimeImageOwner::StartingSandbox(sandbox_id),
                startup_artifacts,
                "starting sandbox",
            )
            .await
        {
            warn!(error = %format_args!("{err:#}"), "failed to protect starting runtime artifacts");
            self.rollback_failed_launch_metadata(&plan, transitional_state)
                .await;
            return Err(err);
        }
        if let Err(source) = sandbox.start_nowait().await {
            warn!(error = %format_args!("{source:#}"), "failed to start sandbox");
            if let Err(stop_err) = sandbox.stop().await {
                warn!(error = %format_args!("{stop_err:#}"), "failed to stop sandbox after start failure");
            }
            self.rollback_failed_launch_metadata(&plan, transitional_state)
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
            if let Err(err) = sandbox.stop().await {
                warn!(error = %format_args!("{err:#}"), "failed to stop sandbox");
            }
            self.rollback_failed_launch_metadata(&plan, transitional_state)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let runtime_resources =
            resources_with_runtime_info(plan.resources(), sandbox.runtime_info());
        let transitional_metadata = plan.transitional_metadata().map(|metadata| {
            let mut metadata = metadata.clone();
            metadata.resources = runtime_resources;
            metadata
        });

        // Store the sandbox handle in memory.
        let handle = Arc::new(Mutex::new(sandbox));
        self.sandboxes
            .write()
            .await
            .insert(sandbox_id, handle.clone());

        self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
            .await;

        // Persist the sandbox metadata if needed (during creation).
        if let Some(metadata) = transitional_metadata.as_ref() {
            if let Err(err) = self.store.add(metadata.clone()).await {
                warn!(error = %format_args!("{err:#}"), "failed to persist sandbox metadata; cleaning up");
                self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::Registered)
                    .await;
                return Err(OrchestratorError::from(err));
            }
        }

        // Check for shutdown again before we wait for the sandbox to become ready.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down before sandbox became ready");
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
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
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
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
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

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
                self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };

        let proxy_target = {
            let sandbox = handle.lock().await;
            match Self::proxy_target_from_sandbox(sandbox.as_ref()) {
                Ok(proxy_target) => proxy_target,
                Err(err) => {
                    warn!(error = %format_args!("{err:#}"), "sandbox became ready without a proxy target; rolling back launch");
                    drop(sandbox);
                    self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::RunningPersisted)
                        .await;
                    return Err(err);
                }
            }
        };
        if !self
            .upsert_proxy_route_if_current_handle(sandbox_id, &handle, proxy_target)
            .await
        {
            debug!("skipping runtime proxy route publication because sandbox handle is stale");
        }

        if matches!(plan, LaunchPlan::Resume(_)) {
            if let Err(err) = self.persister.delete_record(&sandbox_id).await {
                warn!(error = %format_args!("{err:#}"), "failed to delete persisted sandbox record after resume");
            }
        }

        info!("sandbox launch completed");
        Ok(final_metadata)
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
            LaunchPlan::Resume(plan) => self
                .factory
                .build_from_paused_state(plan.sandbox_id, plan.paused_state.as_ref()),
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

    async fn cleanup_failed_launch(
        &self,
        plan: &LaunchPlan,
        handle: SandboxHandle,
        stage: FailedLaunchStage,
    ) {
        let should_rollback_shared_state = self
            .detach_launch_runtime_if_current(
                &plan.sandbox_id(),
                &handle,
                stage.should_detach_proxy_route(),
                stage,
            )
            .await;

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
                if let Err(err) = self.persister.rollback_resuming(&plan.sandbox_id()).await {
                    warn!(error = %format_args!("{err:#}"), "failed to restore persisted sandbox record lifecycle during launch rollback");
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

    async fn upsert_proxy_route_if_current_handle(
        &self,
        sandbox_id: SandboxId,
        handle: &SandboxHandle,
        target: ProxyTarget,
    ) -> bool {
        // Keep the lock order aligned with detach_sandbox_handle_and_route:
        // sandboxes first, then proxy_routes.
        let sandboxes = self.sandboxes.write().await;
        let Some(current_handle) = sandboxes.get(&sandbox_id) else {
            return false;
        };

        if !Arc::ptr_eq(current_handle, handle) {
            return false;
        }

        let version = self
            .next_proxy_route_version
            .fetch_add(1, Ordering::Relaxed);
        let route = self
            .proxy_routes
            .write()
            .await
            .upsert(sandbox_id, target, version);
        drop(sandboxes);

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
                    states: None,
                    excluded_states: Some(vec![SandboxState::Paused]),
                    user_metadata: None,
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

            for metadata in sandboxes {
                let sandbox_id = metadata.id;
                match metadata.state {
                    SandboxState::Paused => {
                        unreachable!("paused sandboxes should have been filtered out")
                    }
                    SandboxState::Running => {
                        if let Err(err) = self.pause_sandbox(sandbox_id).await {
                            last_failures.push(format!("{sandbox_id}: {err}"));
                        }
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
                    | SandboxState::Pausing
                    | SandboxState::Resuming
                    | SandboxState::Killing => {
                        match self.wait_for_transition(sandbox_id, metadata.state).await {
                            Ok(_) | Err(OrchestratorError::SandboxNotFound(_)) => {}
                            Err(err) => {
                                warn!(
                                    sandbox_id = %sandbox_id,
                                    error = ?err,
                                    pass,
                                    "failed to wait for sandbox transition during orchestrator shutdown"
                                );
                                last_failures.push(format!("{sandbox_id}: {err}"));
                            }
                        }
                    }
                }
            }

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
