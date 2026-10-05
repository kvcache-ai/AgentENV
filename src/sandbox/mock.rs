//! In-process mock sandbox backend for unit testing.
//!
//! [`MockSandboxBackend`] immediately completes all lifecycle operations
//! without starting any real process. It is used by
//! [`MockBackendFactory`] to power Orchestrator unit tests that do not
//! need a real VM.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{sync::Mutex, thread};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::time::sleep;

use super::backend::{
    CapturedSandboxSnapshot, PausedSandboxState, RuntimeArtifactSet, SandboxBackend,
    SandboxBackendFactory, SandboxCaptureResult, SandboxForkResult, SandboxForkSpec,
    SandboxRuntimeInfo,
};
use super::{
    FreshSandboxBuildSpec, MemoryHotplugStatus, MemoryHotplugUnsupported,
    MemoryResizeConvergenceError, MemoryResizeResult, SandboxCaptureError, SandboxLaunchConfig,
};
use crate::sandbox::CustomExtensionParams;
use crate::snapshot::RunnableSnapshot;

#[derive(Debug)]
pub struct MockSnapshot;

impl PausedSandboxState for MockSnapshot {
    fn encode(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({}))
    }

    fn runtime_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::empty()
    }
}

#[derive(Debug)]
pub struct MockCapturedSnapshot;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MockOperation {
    Build,
    BuildFromSnapshot,
    Start,
    StartNowait,
    WaitForReady,
    Pause,
    Resume,
    Snapshot,
    CaptureToDir,
    SnapshotVolumes,
    ThawVolumes,
    Fork,
    ForkChild,
    Stop,
    UpdateNetwork,
    ResizeMemory,
    ReadMemoryStatus,
}

#[derive(Clone, Debug)]
pub enum MockAction {
    Succeed,
    SucceedAfter(Duration),
    Fail {
        message: String,
    },
    FailTerminal {
        message: String,
    },
    FailAfter {
        delay: Duration,
        message: String,
    },
    MemoryHotplugUnsupported {
        reason: String,
    },
    MemoryResizeRolledBack {
        elapsed_ms: u64,
    },
    MemoryResizePartial {
        observed_requested_size_mib: u32,
        observed_plugged_size_mib: u32,
        elapsed_ms: u64,
        reason: String,
    },
}

pub type MetricsSampler = Arc<
    dyn Fn() -> futures::future::BoxFuture<'static, Result<super::SandboxMetric>> + Send + Sync,
>;

pub struct MockBehavior {
    actions: Mutex<HashMap<MockOperation, VecDeque<MockAction>>>,
    on_operation: Mutex<HashMap<MockOperation, Arc<dyn Fn() + Send + Sync>>>,
    runtime_info: Mutex<SandboxRuntimeInfo>,
    metrics_sampler: Mutex<Option<MetricsSampler>>,
    source_config_paths: Mutex<Vec<std::path::PathBuf>>,
    /// Template for the virtio-mem status each backend gets its own copy of,
    /// so sandboxes built from one behavior never share resize state.
    memory_hotplug_status: Mutex<MemoryHotplugStatus>,
    boot_memory_mib: Mutex<Option<u32>>,
    stop_calls: AtomicUsize,
    update_network_calls: AtomicUsize,
    freeze_volume_calls: AtomicUsize,
    read_memory_status_calls: AtomicUsize,
}

impl Default for MockBehavior {
    fn default() -> Self {
        Self {
            actions: Mutex::new(HashMap::new()),
            on_operation: Mutex::new(HashMap::new()),
            runtime_info: Mutex::new(SandboxRuntimeInfo::default()),
            source_config_paths: Mutex::new(Vec::new()),
            memory_hotplug_status: Mutex::new(MemoryHotplugStatus {
                total_size_mib: 512,
                slot_size_mib: 128,
                block_size_mib: 2,
                requested_size_mib: 0,
                plugged_size_mib: 0,
            }),
            // Matches the default mock snapshot's memory so resize accounting
            // works out of the box; override per test when a scenario needs a
            // different boot memory.
            boot_memory_mib: Mutex::new(Some(128)),
            stop_calls: AtomicUsize::new(0),
            update_network_calls: AtomicUsize::new(0),
            freeze_volume_calls: AtomicUsize::new(0),
            read_memory_status_calls: AtomicUsize::new(0),
            metrics_sampler: Mutex::new(None),
        }
    }
}

impl MockBehavior {
    pub fn set_metrics_sampler(&self, sampler: MetricsSampler) {
        *self.metrics_sampler.lock().unwrap() = Some(sampler);
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_action(&self, operation: MockOperation, action: MockAction) {
        let mut actions = self.actions.lock().expect("mock behavior mutex poisoned");
        actions.entry(operation).or_default().push_back(action);
    }

    pub fn set_on_operation(&self, operation: MockOperation, hook: Arc<dyn Fn() + Send + Sync>) {
        self.on_operation
            .lock()
            .expect("on_operation mutex poisoned")
            .insert(operation, hook);
    }

    pub fn set_runtime_info(&self, runtime_info: SandboxRuntimeInfo) {
        *self
            .runtime_info
            .lock()
            .expect("runtime_info mutex poisoned") = runtime_info;
    }

    fn runtime_info(&self) -> SandboxRuntimeInfo {
        self.runtime_info
            .lock()
            .expect("runtime_info mutex poisoned")
            .clone()
    }

    pub fn set_source_config_paths(&self, paths: Vec<std::path::PathBuf>) {
        *self
            .source_config_paths
            .lock()
            .expect("source_config_paths mutex poisoned") = paths;
    }

    fn source_config_paths(&self) -> Vec<std::path::PathBuf> {
        self.source_config_paths
            .lock()
            .expect("source_config_paths mutex poisoned")
            .clone()
    }

    /// The status template a new backend copies; later writes only affect
    /// backends constructed afterwards.
    pub fn set_memory_hotplug_status(&self, status: MemoryHotplugStatus) {
        *self
            .memory_hotplug_status
            .lock()
            .expect("memory hotplug status mutex poisoned") = status;
    }

    fn memory_hotplug_status_template(&self) -> MemoryHotplugStatus {
        self.memory_hotplug_status
            .lock()
            .expect("memory hotplug status mutex poisoned")
            .clone()
    }

    pub fn set_boot_memory_mib(&self, boot_memory_mib: u32) {
        *self
            .boot_memory_mib
            .lock()
            .expect("boot memory mutex poisoned") = Some(boot_memory_mib);
    }

    fn boot_memory_mib(&self) -> Option<u32> {
        *self
            .boot_memory_mib
            .lock()
            .expect("boot memory mutex poisoned")
    }

    pub fn stop_calls(&self) -> usize {
        self.stop_calls.load(Ordering::Relaxed)
    }

    pub fn update_network_calls(&self) -> usize {
        self.update_network_calls.load(Ordering::Relaxed)
    }

    pub fn freeze_volume_calls(&self) -> usize {
        self.freeze_volume_calls.load(Ordering::Relaxed)
    }

    pub fn read_memory_status_calls(&self) -> usize {
        self.read_memory_status_calls.load(Ordering::Relaxed)
    }

    fn pop_action(&self, operation: MockOperation) -> MockAction {
        let mut actions = self.actions.lock().expect("mock behavior mutex poisoned");
        actions
            .get_mut(&operation)
            .and_then(VecDeque::pop_front)
            .unwrap_or(MockAction::Succeed)
    }

    fn run_operation_hook(&self, operation: MockOperation) {
        if let Some(hook) = self
            .on_operation
            .lock()
            .expect("on_operation mutex poisoned")
            .get(&operation)
            .cloned()
        {
            hook();
        }
    }

    async fn run_async_action<E, F, G>(
        action: MockAction,
        fail: F,
        fail_terminal: G,
    ) -> std::result::Result<(), E>
    where
        F: FnOnce(String) -> E,
        G: FnOnce(String) -> E,
    {
        match action {
            MockAction::Succeed => Ok(()),
            MockAction::SucceedAfter(delay) => {
                sleep(delay).await;
                Ok(())
            }
            MockAction::Fail { message } => Err(fail(message)),
            MockAction::FailTerminal { message } => Err(fail_terminal(message)),
            MockAction::FailAfter { delay, message } => {
                sleep(delay).await;
                Err(fail(message))
            }
            MockAction::MemoryHotplugUnsupported { reason } => Err(fail(reason)),
            MockAction::MemoryResizeRolledBack { .. } | MockAction::MemoryResizePartial { .. } => {
                Err(fail(
                    "memory resize action used by non-resize operation".to_string(),
                ))
            }
        }
    }

    fn run_sync_action<E, F, G>(
        action: MockAction,
        fail: F,
        fail_terminal: G,
    ) -> std::result::Result<(), E>
    where
        F: FnOnce(String) -> E,
        G: FnOnce(String) -> E,
    {
        match action {
            MockAction::Succeed => Ok(()),
            MockAction::SucceedAfter(delay) => {
                thread::sleep(delay);
                Ok(())
            }
            MockAction::Fail { message } => Err(fail(message)),
            MockAction::FailTerminal { message } => Err(fail_terminal(message)),
            MockAction::FailAfter { delay, message } => {
                thread::sleep(delay);
                Err(fail(message))
            }
            MockAction::MemoryHotplugUnsupported { reason } => Err(fail(reason)),
            MockAction::MemoryResizeRolledBack { .. } | MockAction::MemoryResizePartial { .. } => {
                Err(fail(
                    "memory resize action used by non-resize operation".to_string(),
                ))
            }
        }
    }

    async fn apply_memory_resize(
        &self,
        target_size_mib: u32,
        previous: &MemoryHotplugStatus,
    ) -> Result<()> {
        self.run_operation_hook(MockOperation::ResizeMemory);
        match self.pop_action(MockOperation::ResizeMemory) {
            MockAction::MemoryHotplugUnsupported { reason } => {
                Err(MemoryHotplugUnsupported::new(reason).into())
            }
            MockAction::MemoryResizeRolledBack { elapsed_ms } => {
                Err(MemoryResizeConvergenceError::RolledBack {
                    target_size_mib,
                    rollback_target_size_mib: previous.requested_size_mib,
                    observed: previous.clone(),
                    elapsed_ms,
                }
                .into())
            }
            MockAction::MemoryResizePartial {
                observed_requested_size_mib,
                observed_plugged_size_mib,
                elapsed_ms,
                reason,
            } => {
                let mut observed = previous.clone();
                observed.requested_size_mib = observed_requested_size_mib;
                observed.plugged_size_mib = observed_plugged_size_mib;
                Err(MemoryResizeConvergenceError::Partial {
                    target_size_mib,
                    rollback_target_size_mib: previous.requested_size_mib,
                    observed,
                    elapsed_ms,
                    reason,
                }
                .into())
            }
            action => {
                Self::run_async_action(
                    action,
                    |message| anyhow!(message),
                    |message| anyhow!(message),
                )
                .await
            }
        }
    }

    async fn apply_capture_result(&self, operation: MockOperation) -> SandboxCaptureResult<()> {
        self.run_operation_hook(operation);
        Self::run_async_action(
            self.pop_action(operation),
            |message| SandboxCaptureError::recoverable(anyhow!(message)),
            |message| SandboxCaptureError::terminal(anyhow!(message)),
        )
        .await
    }

    async fn apply_async(&self, operation: MockOperation) -> Result<()> {
        self.run_operation_hook(operation);
        match operation {
            MockOperation::Stop => {
                self.stop_calls.fetch_add(1, Ordering::Relaxed);
            }
            MockOperation::UpdateNetwork => {
                self.update_network_calls.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }

        Self::run_async_action(
            self.pop_action(operation),
            |message| anyhow!(message),
            |message| anyhow!(message),
        )
        .await
    }

    fn apply_sync(&self, operation: MockOperation) -> Result<()> {
        Self::run_sync_action(
            self.pop_action(operation),
            |message| anyhow!(message),
            |message| anyhow!(message),
        )
    }
}

// ── MockSandboxBackend ────────────────────────────────────────────────────────

/// A no-op sandbox backend for unit tests.
///
/// All lifecycle operations succeed immediately; no real processes are spawned.
pub struct MockSandboxBackend {
    behavior: Arc<MockBehavior>,
    host_ip: Option<std::net::Ipv4Addr>,
    volumes_frozen: bool,
    memory_hotplug_status: Mutex<MemoryHotplugStatus>,
}

impl MockSandboxBackend {
    pub fn new(behavior: Arc<MockBehavior>) -> Self {
        Self::new_with_host_ip(behavior, Some(std::net::Ipv4Addr::new(127, 0, 0, 1)))
    }

    pub fn new_with_host_ip(
        behavior: Arc<MockBehavior>,
        host_ip: Option<std::net::Ipv4Addr>,
    ) -> Self {
        Self {
            behavior: Arc::clone(&behavior),
            host_ip,
            volumes_frozen: false,
            memory_hotplug_status: Mutex::new(behavior.memory_hotplug_status_template()),
        }
    }
}

#[async_trait]
impl SandboxBackend for MockSandboxBackend {
    fn metrics_sample(
        &self,
    ) -> Option<futures::future::BoxFuture<'static, Result<super::SandboxMetric>>> {
        let sampler = self.behavior.metrics_sampler.lock().unwrap().clone();
        sampler.map(|sample| sample())
    }

    async fn start(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::Start).await
    }

    async fn start_nowait(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::StartNowait).await
    }

    async fn wait_for_ready(&self) -> Result<()> {
        self.behavior.apply_async(MockOperation::WaitForReady).await
    }

    async fn resize_memory_hotplug(
        &mut self,
        requested_size_mib: u32,
    ) -> Result<MemoryResizeResult> {
        let previous = self
            .memory_hotplug_status
            .lock()
            .expect("mock memory hotplug mutex poisoned")
            .clone();
        // Mirror the production preconditions so tests cannot resize past
        // the configured geometry.
        anyhow::ensure!(
            requested_size_mib <= previous.total_size_mib,
            "requested virtio-mem size {requested_size_mib} MiB exceeds total {} MiB",
            previous.total_size_mib
        );
        anyhow::ensure!(
            requested_size_mib.is_multiple_of(previous.block_size_mib),
            "requested virtio-mem size {requested_size_mib} MiB is not aligned to block size {} MiB",
            previous.block_size_mib
        );
        self.behavior
            .apply_memory_resize(requested_size_mib, &previous)
            .await?;
        let mut status = self
            .memory_hotplug_status
            .lock()
            .expect("mock memory hotplug mutex poisoned");
        let previous_requested_size_mib = status.requested_size_mib;
        status.requested_size_mib = requested_size_mib;
        status.plugged_size_mib = requested_size_mib;
        Ok(MemoryResizeResult {
            previous_requested_size_mib,
            requested_size_mib,
            plugged_size_mib: requested_size_mib,
            total_size_mib: status.total_size_mib,
            slot_size_mib: status.slot_size_mib,
            block_size_mib: status.block_size_mib,
            elapsed_ms: 0,
        })
    }

    async fn memory_hotplug_status(&mut self) -> Result<MemoryHotplugStatus> {
        self.behavior
            .read_memory_status_calls
            .fetch_add(1, Ordering::Relaxed);
        self.behavior
            .run_operation_hook(MockOperation::ReadMemoryStatus);
        match self.behavior.pop_action(MockOperation::ReadMemoryStatus) {
            MockAction::MemoryHotplugUnsupported { reason } => {
                Err(MemoryHotplugUnsupported::new(reason).into())
            }
            action => {
                MockBehavior::run_async_action(
                    action,
                    |message| anyhow!(message),
                    |message| anyhow!(message),
                )
                .await?;
                Ok(self
                    .memory_hotplug_status
                    .lock()
                    .expect("mock memory hotplug mutex poisoned")
                    .clone())
            }
        }
    }

    async fn boot_memory_mib(&mut self) -> Result<Option<u32>> {
        Ok(self.behavior.boot_memory_mib())
    }

    async fn pause(
        &mut self,
        _artifact_root: Option<&Path>,
    ) -> SandboxCaptureResult<Arc<dyn PausedSandboxState>> {
        let pause_result = self
            .behavior
            .apply_capture_result(MockOperation::Pause)
            .await;
        if let Err(pause_err) = pause_result {
            if pause_err.is_terminal() {
                return Err(pause_err);
            }
            if let Err(resume_err) = self.behavior.apply_async(MockOperation::Resume).await {
                return Err(SandboxCaptureError::terminal(anyhow!(
                    "pause failed and sandbox could not be resumed: pause error: {pause_err}; resume error: {resume_err:#}"
                )));
            }
            return Err(pause_err);
        }
        Ok(Arc::new(MockSnapshot))
    }

    async fn resume(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::Resume).await
    }

    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot> {
        self.behavior
            .apply_capture_result(MockOperation::Snapshot)
            .await?;
        Ok(CapturedSandboxSnapshot::new(
            crate::sandbox::manifest::SandboxSnapshotManifest::for_test(4096, &[]),
            MockCapturedSnapshot,
        ))
    }

    async fn capture_to_dir(
        &mut self,
        _at: &std::path::Path,
    ) -> SandboxCaptureResult<(
        crate::sandbox::SandboxSnapshotManifest,
        Option<Box<dyn std::any::Any + Send>>,
    )> {
        self.behavior
            .apply_capture_result(MockOperation::CaptureToDir)
            .await?;
        Ok((
            crate::sandbox::SandboxSnapshotManifest::for_test(4096, &[]),
            None,
        ))
    }

    async fn snapshot_volumes(&mut self) -> SandboxCaptureResult<()> {
        self.behavior
            .apply_capture_result(MockOperation::SnapshotVolumes)
            .await
    }

    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>> {
        self.behavior
            .apply_capture_result(MockOperation::Fork)
            .await?;
        Ok(spec
            .iter()
            .map(|_| {
                self.behavior
                    .apply_sync(MockOperation::ForkChild)
                    .map(|()| {
                        Box::new(Self::new_with_host_ip(
                            Arc::clone(&self.behavior),
                            self.host_ip,
                        )) as Box<dyn SandboxBackend>
                    })
            })
            .collect())
    }

    async fn stop(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::Stop).await?;
        self.volumes_frozen = false;
        Ok(())
    }

    async fn freeze_and_snapshot_volumes(&mut self) -> SandboxCaptureResult<()> {
        assert!(!self.volumes_frozen, "volumes already frozen");
        self.behavior
            .freeze_volume_calls
            .fetch_add(1, Ordering::Relaxed);
        let result = self.snapshot_volumes().await;
        self.volumes_frozen = result
            .as_ref()
            .err()
            .is_none_or(|error| error.is_terminal());
        result
    }

    async fn thaw_volumes(&mut self) -> Result<()> {
        anyhow::ensure!(self.volumes_frozen, "volumes are not frozen");
        self.behavior
            .apply_async(MockOperation::ThawVolumes)
            .await?;
        self.volumes_frozen = false;
        Ok(())
    }

    fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr> {
        self.host_ip
    }

    fn runtime_info(&self) -> SandboxRuntimeInfo {
        self.behavior.runtime_info()
    }

    fn startup_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::from_overlaybd_image_configs(self.behavior.source_config_paths())
    }

    async fn update_network_policy(
        &mut self,
        _policy: Option<super::SandboxNetworkPolicy>,
    ) -> Result<()> {
        self.behavior
            .apply_async(MockOperation::UpdateNetwork)
            .await
    }

    fn update_custom_extension_params(&mut self, _params: Option<CustomExtensionParams>) {}
}

// ── MockBackendFactory ────────────────────────────────────────────────────────

/// Factory that produces [`MockSandboxBackend`] instances.
///
/// Produced backends use deterministic placeholder values suitable for
/// asserting against in tests.
pub struct MockBackendFactory {
    behavior: Arc<MockBehavior>,
    host_ip: Option<std::net::Ipv4Addr>,
}

impl MockBackendFactory {
    pub fn new() -> Self {
        Self::with_behavior(Arc::new(MockBehavior::new()))
    }

    pub fn with_behavior(behavior: Arc<MockBehavior>) -> Self {
        Self::with_behavior_and_host_ip(behavior, Some(std::net::Ipv4Addr::new(127, 0, 0, 1)))
    }

    pub fn with_behavior_and_host_ip(
        behavior: Arc<MockBehavior>,
        host_ip: Option<std::net::Ipv4Addr>,
    ) -> Self {
        Self { behavior, host_ip }
    }
}

impl Default for MockBackendFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SandboxBackendFactory for MockBackendFactory {
    fn build(
        &self,
        _build_spec: FreshSandboxBuildSpec,
        _launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>> {
        self.behavior.apply_sync(MockOperation::Build)?;
        Ok(Box::new(MockSandboxBackend::new_with_host_ip(
            Arc::clone(&self.behavior),
            self.host_ip,
        )))
    }

    fn build_from_snapshot(
        &self,
        _snapshot: &RunnableSnapshot,
        _launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>> {
        self.behavior.apply_sync(MockOperation::Build)?;
        Ok(Box::new(MockSandboxBackend::new_with_host_ip(
            Arc::clone(&self.behavior),
            self.host_ip,
        )))
    }

    fn build_from_paused_state(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _state: &dyn PausedSandboxState,
        _envd_access_token: Option<super::EnvdAccessToken>,
    ) -> Result<Box<dyn SandboxBackend>> {
        self.behavior.apply_sync(MockOperation::BuildFromSnapshot)?;
        Ok(Box::new(MockSandboxBackend::new_with_host_ip(
            Arc::clone(&self.behavior),
            self.host_ip,
        )))
    }

    fn decode_paused_state(
        &self,
        _artifact_root: std::path::PathBuf,
        _state: serde_json::Value,
    ) -> Result<Arc<dyn PausedSandboxState>> {
        Ok(Arc::new(MockSnapshot))
    }
}
