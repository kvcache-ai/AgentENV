use super::DiskMetric;
use crate::types::SandboxId;

/// Static machine descriptors reported as part of node observability.
#[derive(Clone, Debug)]
pub struct MachineInfo {
    pub cpu_family: String,
    pub cpu_model: String,
    pub cpu_model_name: String,
    pub cpu_architecture: String,
    pub cpu_config_json: Option<String>,
}

/// Node-level metrics projected from orchestrator runtime counters and the
/// latest sampled host snapshot.
#[derive(Clone, Debug)]
pub struct NodeMetricsSnapshot {
    pub allocated_cpu: u32,
    pub allocated_memory_bytes: u64,
    pub cpu_percent: u32,
    pub cpu_count: u32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disks: Vec<DiskMetric>,
    /// CPU reservation of all paused sandboxes on the node, summed.
    pub paused_allocated_cpu: u32,
    /// Memory reservation of all paused sandboxes on the node, summed.
    pub paused_allocated_memory_bytes: u64,
    pub disk_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_available_bytes: u64,
    pub cleanup_pending: u64,
    pub cleanup_retries: u64,
    pub cleanup_failures: u64,
    pub pruned_generations: u64,
    pub reclaimed_snapshot_bytes: u64,
    pub reserved_cleanup_journal_bytes: u64,
    pub reclaimed_log_bytes: u64,
    pub disk_admission_rejections: u64,
    pub accepting_sandboxes: bool,
    pub admission_reason: &'static str,
}

/// Request-time node snapshot returned by the admin/node APIs.
///
/// This joins together identity metadata, static machine information,
/// orchestrator runtime accounting, and host resource snapshots.
#[derive(Clone, Debug)]
pub struct NodeSnapshot {
    pub version: String,
    pub commit: String,
    pub node_id: String,
    pub runtime_family_id: String,
    pub service_instance_id: String,
    pub cluster_id: uuid::Uuid,
    pub machine_info: MachineInfo,
    pub sandbox_count: u32,
    pub sandbox_ids: Vec<SandboxId>,
    pub metrics: NodeMetricsSnapshot,
    pub create_successes: u64,
    pub create_fails: u64,
    pub sandbox_starting_count: u32,
    /// Number of sandboxes currently in the Paused state on this node.
    pub paused_sandbox_count: u32,
}
