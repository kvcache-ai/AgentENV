use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;
use tokio::sync::broadcast;

use crate::orchestrator::{Orchestrator, SandboxLifecycleEvent};

use super::host::HostMetricsCollector;
use super::machine::detect_machine_info;
use super::{MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
use crate::identity::NodeIdentity;

/// Projects node-level observability responses from precomputed inputs.
///
/// The service merges:
/// - orchestrator runtime metrics sampled on request via `metrics_snapshot()`
/// - static node identity and machine information
/// - host metrics sampled on request
/// - the list of running sandbox IDs sampled on request
///
/// Configuration can disable this service entirely at the server wiring layer.
#[derive(Clone)]
pub struct ObservabilityService {
    orchestrator: Arc<Orchestrator>,
    identity: NodeIdentity,
    machine_info: MachineInfo,
    host_metrics: HostMetricsCollector,
    pending_cpu_config: Arc<Mutex<Option<String>>>,
    cluster_cpu_config: Arc<RwLock<Option<String>>>,
}

impl ObservabilityService {
    pub async fn new(
        identity: NodeIdentity,
        orchestrator: Arc<Orchestrator>,
        cpu_template_helper: Option<PathBuf>,
        cluster_cpu_arc: Arc<RwLock<Option<String>>>,
    ) -> Self {
        let machine_info = detect_machine_info();
        let host_metrics = HostMetricsCollector::new();
        let cpu_config_json = if let Some(p) = cpu_template_helper {
            super::machine::dump_cpu_config(p).await
        } else {
            None
        };
        Self {
            orchestrator,
            identity,
            machine_info,
            host_metrics,
            pending_cpu_config: Arc::new(Mutex::new(cpu_config_json)),
            cluster_cpu_config: cluster_cpu_arc,
        }
    }

    pub fn take_cpu_config_json(&self) -> Option<String> {
        self.pending_cpu_config.lock().unwrap().take()
    }

    pub fn store_cluster_cpu_config(&self, config: String) {
        *self.cluster_cpu_config.write().unwrap() = Some(config);
    }

    pub fn subscribe_sandbox_events(&self) -> broadcast::Receiver<SandboxLifecycleEvent> {
        self.orchestrator.subscribe_sandbox_events()
    }

    /// Returns the latest node snapshot exposed by the admin/node APIs and heartbeat reporting.
    pub async fn node_snapshot(&self) -> Result<NodeSnapshot> {
        let runtime = self.orchestrator.metrics_snapshot().await?;
        let host = self.host_metrics.collect();

        Ok(NodeSnapshot {
            version: self.identity.version.clone(),
            commit: self.identity.commit.clone(),
            node_id: self.identity.id.clone(),
            runtime_family_id: self.identity.runtime_family_id.clone(),
            service_instance_id: self.identity.service_instance_id.clone(),
            cluster_id: self.identity.cluster_id,
            machine_info: self.machine_info.clone(),
            sandbox_count: runtime.running_sandbox_count,
            sandbox_ids: self.orchestrator.list_sandbox_ids().await?,
            metrics: NodeMetricsSnapshot {
                allocated_cpu: runtime.allocated_cpu,
                allocated_memory_bytes: runtime.allocated_memory_bytes,
                cpu_percent: host.cpu_percent,
                cpu_count: host.cpu_count,
                memory_used_bytes: host.memory_used_bytes,
                memory_total_bytes: host.memory_total_bytes,
                disks: host.disks,
                paused_allocated_cpu: runtime.paused_allocated_cpu,
                paused_allocated_memory_bytes: runtime.paused_allocated_memory_bytes,
                disk_total_bytes: runtime.disk_total_bytes,
                disk_used_bytes: runtime.disk_used_bytes,
                disk_available_bytes: runtime.disk_available_bytes,
                cleanup_pending: runtime.cleanup_pending,
                cleanup_retries: runtime.cleanup_retries,
                cleanup_failures: runtime.cleanup_failures,
                pruned_generations: runtime.pruned_generations,
                reclaimed_snapshot_bytes: runtime.reclaimed_snapshot_bytes,
                reserved_cleanup_journal_bytes: runtime.reserved_cleanup_journal_bytes,
                reclaimed_log_bytes: runtime.reclaimed_log_bytes,
                disk_admission_rejections: runtime.disk_admission_rejections,
                accepting_sandboxes: runtime.accepting_sandboxes,
                admission_reason: runtime.admission_reason,
            },
            create_successes: runtime.create_successes,
            create_fails: runtime.create_fails,
            sandbox_starting_count: runtime.starting_sandbox_count,
            paused_sandbox_count: runtime.paused_sandbox_count,
        })
    }

    pub fn node_id(&self) -> &str {
        &self.identity.id
    }

    pub fn cluster_id(&self) -> uuid::Uuid {
        self.identity.cluster_id
    }

    pub fn service_instance_id(&self) -> &str {
        &self.identity.service_instance_id
    }
}
