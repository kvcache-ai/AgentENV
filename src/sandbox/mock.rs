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

use crate::snapshot::RunnableSnapshot;
use crate::types::SandboxId;

use super::backend::{
    CapturedSandboxSnapshot, PausedSandboxState, RuntimeArtifactSet, SandboxBackend,
    SandboxBackendFactory, SandboxCaptureResult, SandboxForkResult, SandboxRuntimeInfo,
};
use super::{FreshSandboxBuildSpec, SandboxCaptureError, SandboxLaunchConfig};
use crate::sandbox::{CustomExtensionParams, SandboxNetworkPolicy};

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
    Fork,
    ForkChild,
    Stop,
    UpdateNetwork,
}

#[derive(Clone, Debug)]
pub enum MockAction {
    Succeed,
    SucceedAfter(Duration),
    Fail { message: String },
    FailTerminal { message: String },
    FailAfter { delay: Duration, message: String },
}

#[derive(Default)]
pub struct MockBehavior {
    actions: Mutex<HashMap<MockOperation, VecDeque<MockAction>>>,
    on_operation: Mutex<HashMap<MockOperation, Arc<dyn Fn() + Send + Sync>>>,
    runtime_info: Mutex<SandboxRuntimeInfo>,
    source_config_paths: Mutex<Vec<std::path::PathBuf>>,
    applied_network_policies: Mutex<Vec<Option<SandboxNetworkPolicy>>>,
    stop_calls: AtomicUsize,
    update_network_calls: AtomicUsize,
}

impl MockBehavior {
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

    pub fn stop_calls(&self) -> usize {
        self.stop_calls.load(Ordering::Relaxed)
    }

    pub fn update_network_calls(&self) -> usize {
        self.update_network_calls.load(Ordering::Relaxed)
    }

    pub fn applied_network_policies(&self) -> Vec<Option<SandboxNetworkPolicy>> {
        self.applied_network_policies
            .lock()
            .expect("mock behavior mutex poisoned")
            .clone()
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
}

impl MockSandboxBackend {
    pub fn new(behavior: Arc<MockBehavior>) -> Self {
        Self::new_with_host_ip(behavior, Some(std::net::Ipv4Addr::new(127, 0, 0, 1)))
    }

    pub fn new_with_host_ip(
        behavior: Arc<MockBehavior>,
        host_ip: Option<std::net::Ipv4Addr>,
    ) -> Self {
        Self { behavior, host_ip }
    }
}

#[async_trait]
impl SandboxBackend for MockSandboxBackend {
    async fn start(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::Start).await
    }

    async fn start_nowait(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::StartNowait).await
    }

    async fn wait_for_ready(&self) -> Result<()> {
        self.behavior.apply_async(MockOperation::WaitForReady).await
    }

    async fn pause(
        &mut self,
        _artifact_root: Option<&Path>,
    ) -> SandboxCaptureResult<Arc<dyn PausedSandboxState>> {
        self.behavior
            .apply_capture_result(MockOperation::Pause)
            .await?;
        Ok(Arc::new(MockSnapshot))
    }

    async fn resume(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::Resume).await
    }

    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot> {
        self.behavior
            .apply_capture_result(MockOperation::Snapshot)
            .await?;
        Ok(CapturedSandboxSnapshot::new(MockCapturedSnapshot))
    }

    async fn fork(
        &mut self,
        child_ids: &[SandboxId],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>> {
        self.behavior
            .apply_capture_result(MockOperation::Fork)
            .await?;
        Ok(child_ids
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
        self.behavior.apply_async(MockOperation::Stop).await
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
        policy: Option<super::SandboxNetworkPolicy>,
    ) -> Result<()> {
        self.behavior
            .applied_network_policies
            .lock()
            .expect("mock behavior mutex poisoned")
            .push(policy);
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
