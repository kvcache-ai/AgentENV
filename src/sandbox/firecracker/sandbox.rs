use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use firecracker_client::models::drive::IoEngine;
use nix::libc;
use tempfile::TempDir;
use tracing::{debug, trace, warn};
use uuid::Uuid;
use uvm_ublk_daemon::CreateOverlaybdRuntimeDeviceRequest;

use super::config::{
    create_firecracker_work_dir, FirecrackerCommonConfig, FirecrackerRuntimePolicy,
    FirecrackerSandboxConfig, FirecrackerSnapshotConfig, PersistentSnapshotRootGuard,
    MAX_EXTRA_DRIVES,
};
use super::manifest::{
    FirecrackerSnapshotManifest, GuestMemoryWorkingSet, GuestMemoryWorkingSetLimits,
};
use super::mincore_tracking::{resident_ranges_to_working_set, ResidentMemoryRange};
use super::mmds::MmdsMetadata;
use super::overlaybd_snapshot::{
    build_mem_snapshot_image_config, convert_dirty_memory_to_overlaybd,
    restack_snapshot_overlaybd_device, restack_snapshot_overlaybd_rootfs,
};
use super::pool::{warm_stderr_path, warm_stdout_path, FirecrackerPool};
use super::prefault::{build_prefault_plan, PrefaultPlan};
use super::prefault_stats::PrefaultCompletionStats;
use super::socket::is_http_status_error;
use super::FirecrackerInstance;
use crate::sandbox::custom_extension::{
    CustomExtensionClient, CustomExtensionHookGuard, CustomExtensionParams,
};

use crate::cfg::{AppConfig, ConfigManager};
use crate::sandbox::access::EnvdAccessToken;
use crate::sandbox::backend::{
    CapturedSandboxSnapshot, PausedSandboxState, RuntimeArtifactSet, SandboxBackend,
    SandboxCaptureError, SandboxCaptureResult, SandboxExecutor, SandboxForkResult, SandboxForkSpec,
    SandboxRuntimeInfo,
};
use crate::sandbox::envd::EnvdInstance;
use crate::sandbox::extra_drive::{
    prepare_extra_drives, DriveMount, ExtraDrive, ExtraDrivePrepareMode, ROOTFS_DRIVE_ID,
    USER_ROOTFS_DRIVE_ID,
};
use crate::sandbox::network::{NetworkManager, SandboxNetworkPolicy, Slot};
use crate::sandbox::process::Executor;
use crate::sandbox::ublk::{
    OverlaybdCompactOutput, OverlaybdConfig, OverlaybdRuntimeHandle, SharedMemDevice, UblkBackend,
    UblkCreateSpec, UblkDevice, UblkDeviceManager,
};
use crate::sandbox::SandboxLaunchConfig;
use crate::snapshot::RunnableSnapshot;
use crate::types::SandboxId;

// ── Constants ────────────────────────────────────────────────────────────────

const VM_STATE_FILE_NAME: &str = "vm_state.bin";
const ROOTFS_DRIVE_PATH: &str = "rootfs.ext4";
const USER_ROOTFS_DRIVE_PATH: &str = "user-rootfs";

/// Timings for the observable stages of a snapshot restore.
///
/// `restore_setup` covers host-side resource preparation before the
/// Firecracker snapshot-load request. The remaining fields deliberately match
/// the load, KVM pre-fault, resume, and envd-ready boundaries.
#[derive(Clone, Debug, Default)]
pub struct SnapshotResumeTimings {
    pub restore_setup: Duration,
    pub snapshot_load: Duration,
    pub prefault: Duration,
    pub prefault_stats: Option<PrefaultCompletionStats>,
    pub firecracker_resume: Duration,
    pub envd_ready: Duration,
}

/// Firecracker's `TokenBucket::size` is the number of tokens replenished every
/// `refill_time`, not a per-second rate. Pinning the refill period to 1000 ms
/// makes the configured `*_per_sec` values equal the sustained per-second rate.
const RATE_LIMIT_REFILL_TIME_MS: i64 = 1000;

/// Read configured host swap for profiling observability. Swap may reduce the
/// observed mincore working set, but it must not prevent snapshot publication
/// or restore.
fn mincore_swap_total_kib(meminfo: &str) -> Result<u64> {
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix("SwapTotal:"))
        .and_then(|value| value.split_whitespace().next())
        .ok_or_else(|| anyhow::anyhow!("/proc/meminfo does not contain SwapTotal"))?
        .parse::<u64>()
        .context("parse SwapTotal from /proc/meminfo")
}

fn warn_if_mincore_host_has_swap() {
    match std::fs::read_to_string("/proc/meminfo")
        .context("read /proc/meminfo for mincore profiling swap observation")
        .and_then(|meminfo| mincore_swap_total_kib(&meminfo))
    {
        Ok(0) => {}
        Ok(swap_kib) => warn!(
            swap_kib,
            "mincore profiling continues with host swap enabled; actual swapped guest-memory pages can make the recorded working set incomplete and reduce restore pre-fault benefit"
        ),
        Err(error) => warn!(
            error = ?error,
            "cannot determine host swap state; mincore profiling continues, but its working-set completeness cannot be assessed"
        ),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ResidentRangeStats {
    range_count: usize,
    bytes: u64,
}

/// One mincore sample from the dedicated snapshot profiler.
///
/// newly_resident_* is the set difference from the preceding sample. The
/// first snapshot_loaded_paused sample therefore reports zero new bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMincoreStage {
    pub phase: &'static str,
    pub total_ranges: usize,
    pub total_bytes: u64,
    pub newly_resident_ranges: usize,
    pub newly_resident_bytes: u64,
}

/// One cumulative, baseline-excluded pre-fault candidate from the dedicated
/// mincore profiler. This is diagnostic data: normal snapshot publication
/// continues to use its existing ready-window working-set collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotPrefaultCandidate {
    pub phase: &'static str,
    pub working_set: GuestMemoryWorkingSet,
}

fn snapshot_prefault_candidate(
    phase: &'static str,
    baseline: &[ResidentMemoryRange],
    current: &[ResidentMemoryRange],
    regions: &[super::mincore_tracking::GuestMemoryImageRegion],
    limits: GuestMemoryWorkingSetLimits,
) -> Result<SnapshotPrefaultCandidate> {
    let newly_resident = newly_resident_ranges(baseline, current)?;
    let working_set = resident_ranges_to_working_set(&newly_resident, regions, limits)?;
    Ok(SnapshotPrefaultCandidate { phase, working_set })
}

fn snapshot_mincore_stage(
    phase: &'static str,
    previous: Option<&[ResidentMemoryRange]>,
    current: &[ResidentMemoryRange],
) -> Result<SnapshotMincoreStage> {
    let total = resident_range_stats(current)?;
    let newly_resident = match previous {
        Some(previous) => newly_resident_ranges(previous, current)?,
        None => Vec::new(),
    };
    let newly = resident_range_stats(&newly_resident)?;
    Ok(SnapshotMincoreStage {
        phase,
        total_ranges: total.range_count,
        total_bytes: total.bytes,
        newly_resident_ranges: newly.range_count,
        newly_resident_bytes: newly.bytes,
    })
}

fn normalized_resident_ranges(ranges: &[ResidentMemoryRange]) -> Result<Vec<ResidentMemoryRange>> {
    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|range| range.image_offset);
    let mut normalized: Vec<ResidentMemoryRange> = Vec::with_capacity(sorted.len());
    for range in sorted {
        let range_end = range
            .image_offset
            .checked_add(range.length)
            .context("resident range end overflows u64")?;
        if let Some(previous) = normalized.last_mut() {
            let previous_end = previous
                .image_offset
                .checked_add(previous.length)
                .context("resident range end overflows u64")?;
            if range.image_offset <= previous_end {
                if range_end > previous_end {
                    previous.length = range_end - previous.image_offset;
                }
                continue;
            }
        }
        normalized.push(range);
    }
    Ok(normalized)
}

fn resident_range_stats(ranges: &[ResidentMemoryRange]) -> Result<ResidentRangeStats> {
    let ranges = normalized_resident_ranges(ranges)?;
    let bytes = ranges.iter().try_fold(0_u64, |total, range| {
        total
            .checked_add(range.length)
            .context("resident range byte total overflows u64")
    })?;
    Ok(ResidentRangeStats {
        range_count: ranges.len(),
        bytes,
    })
}

fn newly_resident_ranges(
    baseline: &[ResidentMemoryRange],
    final_ranges: &[ResidentMemoryRange],
) -> Result<Vec<ResidentMemoryRange>> {
    let baseline = normalized_resident_ranges(baseline)?;
    let final_ranges = normalized_resident_ranges(final_ranges)?;
    let mut baseline_index = 0;
    let mut newly_resident = Vec::new();

    for final_range in final_ranges {
        let final_end = final_range
            .image_offset
            .checked_add(final_range.length)
            .context("final resident range end overflows u64")?;
        let mut cursor = final_range.image_offset;
        while baseline_index < baseline.len() {
            let baseline_end = baseline[baseline_index]
                .image_offset
                .checked_add(baseline[baseline_index].length)
                .context("baseline resident range end overflows u64")?;
            if baseline_end > cursor {
                break;
            }
            baseline_index += 1;
        }
        let mut index = baseline_index;
        while index < baseline.len() && baseline[index].image_offset < final_end {
            if baseline[index].image_offset > cursor {
                let end = baseline[index].image_offset.min(final_end);
                newly_resident.push(ResidentMemoryRange {
                    image_offset: cursor,
                    length: end - cursor,
                });
            }
            let baseline_end = baseline[index]
                .image_offset
                .checked_add(baseline[index].length)
                .context("baseline resident range end overflows u64")?;
            cursor = cursor.max(baseline_end);
            if cursor >= final_end {
                break;
            }
            index += 1;
        }
        if cursor < final_end {
            newly_resident.push(ResidentMemoryRange {
                image_offset: cursor,
                length: final_end - cursor,
            });
        }
    }
    Ok(newly_resident)
}

fn bandwidth_bucket(
    cfg: &crate::cfg::DiskRateLimitConfig,
) -> Result<Option<Box<firecracker_client::models::TokenBucket>>> {
    if cfg.bandwidth_bytes_per_sec == 0 {
        return Ok(None);
    }
    let size = i64::try_from(cfg.bandwidth_bytes_per_sec)
        .context("disk bandwidth_bytes_per_sec exceeds Firecracker's i64 range")?;
    let mut bw = firecracker_client::models::TokenBucket::new(RATE_LIMIT_REFILL_TIME_MS, size);
    if cfg.bandwidth_burst_bytes > 0 {
        bw.one_time_burst = Some(
            i64::try_from(cfg.bandwidth_burst_bytes)
                .context("disk bandwidth_burst_bytes exceeds Firecracker's i64 range")?,
        );
    }
    Ok(Some(Box::new(bw)))
}

fn ops_bucket(
    cfg: &crate::cfg::DiskRateLimitConfig,
) -> Result<Option<Box<firecracker_client::models::TokenBucket>>> {
    if cfg.iops == 0 {
        return Ok(None);
    }
    let size = i64::try_from(cfg.iops).context("disk iops exceeds Firecracker's i64 range")?;
    let mut ops = firecracker_client::models::TokenBucket::new(RATE_LIMIT_REFILL_TIME_MS, size);
    if cfg.iops_burst > 0 {
        ops.one_time_burst = Some(
            i64::try_from(cfg.iops_burst)
                .context("disk iops_burst exceeds Firecracker's i64 range")?,
        );
    }
    Ok(Some(Box::new(ops)))
}

/// Build the limiter attached to the user rootfs drive at fresh boot (pre-boot
/// `PUT /drives`). Returns `None` when limiting is disabled or no dimension is
/// configured, in which case the drive is added with no limiter.
fn build_disk_rate_limiter(
    cfg: &crate::cfg::DiskRateLimitConfig,
) -> Result<Option<Box<firecracker_client::models::RateLimiter>>> {
    if !cfg.enabled {
        return Ok(None);
    }
    let bandwidth = bandwidth_bucket(cfg)?;
    let ops = ops_bucket(cfg)?;
    if bandwidth.is_none() && ops.is_none() {
        return Ok(None);
    }
    let mut rl = firecracker_client::models::RateLimiter::new();
    rl.bandwidth = bandwidth;
    rl.ops = ops;
    Ok(Some(Box::new(rl)))
}

/// A token bucket Firecracker interprets as "disable this dimension".
///
/// Firecracker's `PATCH /drives` maps an *absent* token bucket to
/// `BucketUpdate::None` (leave unchanged), so a snapshot-inherited limit cannot
/// be removed by omission. The explicit disable sentinel is a bucket with both
/// `size == 0` and `refill_time == 0`; a mixed bucket (e.g. `size == 0`,
/// `refill_time == 1`) is not the sentinel and can be rejected as an invalid
/// token bucket, failing the resume PATCH. Send both fields as zero.
fn disabled_bucket() -> Box<firecracker_client::models::TokenBucket> {
    Box::new(firecracker_client::models::TokenBucket::new(0, 0))
}

/// Build the limiter to PATCH on resume, reconciling a snapshot-inherited
/// limiter against the node's current config. BOTH buckets are always present:
/// a configured dimension uses its own bucket, an unset dimension is overwritten
/// with a disabled (`size == 0`) bucket so any inherited limit on that dimension
/// is cleared (an omitted bucket would instead be left unchanged; see
/// [`disabled_bucket`]).
fn reconcile_disk_rate_limiter(
    cfg: &crate::cfg::DiskRateLimitConfig,
) -> Result<Box<firecracker_client::models::RateLimiter>> {
    let (bandwidth, ops) = if cfg.enabled {
        (bandwidth_bucket(cfg)?, ops_bucket(cfg)?)
    } else {
        (None, None)
    };
    let mut rl = firecracker_client::models::RateLimiter::new();
    rl.bandwidth = Some(bandwidth.unwrap_or_else(disabled_bucket));
    rl.ops = Some(ops.unwrap_or_else(disabled_bucket));
    Ok(Box::new(rl))
}

pub(super) fn managed_snapshot_base() -> PathBuf {
    ConfigManager::global_config()
        .firecracker
        .work_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("aenv"))
        .join("managed-snapshots")
}

// ── FirecrackerSandbox ───────────────────────────────────────────────────────

/// A Firecracker microVM-backed sandbox instance.
///
/// Internal state is managed via the high-level lifecycle methods.
/// Implements [`SandboxBackend`] for use by the Orchestrator.
pub struct FirecrackerSandbox {
    id: SandboxId,
    launch: LaunchMode,
    work_dir: TempDir,
    fc_instance: FirecrackerInstance,
    runtime_policy: FirecrackerRuntimePolicy,
    network_slot: Option<Slot>,
    current_network_policy: Option<SandboxNetworkPolicy>,
    /// Current custom extension params. Initialized from the launch config and
    /// updated via `update_custom_extension_params`; persisted into snapshots
    /// on pause.
    current_custom_extension_params: Option<CustomExtensionParams>,
    envd_instance: Option<EnvdInstance>,
    rootfs_runtime: Option<OverlaybdRuntimeHandle>,
    mem_ublk_device: Option<SharedMemDevice>,
    /// Exclusive memory device used only by a throwaway profiler.
    profiling_mem_ublk_device: Option<UblkDevice>,
    /// image.json path the memory device was opened with. Used as the device
    /// key to release held background downloads once envd is ready.
    mem_snapshot_image_config_path: Option<PathBuf>,
    /// image.json path the rootfs device was opened with. Also released at
    /// envd ready so a rootfs background download (when enabled) never
    /// waits out the fallback with no notification.
    rootfs_image_config_path: Option<PathBuf>,
    extra_drive_runtimes: Vec<OverlaybdRuntimeHandle>,
    current_rootfs_virtual_size: Option<u64>,
    live_snapshot_root: Option<Arc<PersistentSnapshotRootGuard>>,
    /// Delivers the custom extension stop hook exactly once: `stop()` calls
    /// [`CustomExtensionHookGuard::stop`], otherwise its own drop fires the
    /// best-effort notification. `None` when no start hook was delivered (or
    /// no extension is configured).
    custom_extension_hook_guard: Option<CustomExtensionHookGuard>,
    /// Optional immutable profiling metadata carried from a committed snapshot.
    /// It is consumed only while the restored VM is still paused.
    restore_working_set: Option<GuestMemoryWorkingSet>,
    profiling_mode: bool,
}

// ── SandboxBackend impl ──────────────────────────────────────────────────────

/// Firecracker-specific captured snapshot payload kept alive for publication.
#[derive(Debug)]
pub struct FirecrackerCapturedSnapshot {
    manifest: FirecrackerSnapshotManifest,
    _snapshot_root: Arc<PersistentSnapshotRootGuard>,
}

#[derive(Clone, Debug)]
pub struct FirecrackerPausedState {
    snapshot_config: FirecrackerSnapshotConfig,
}

impl FirecrackerPausedState {
    pub fn new(snapshot_config: FirecrackerSnapshotConfig) -> Self {
        Self { snapshot_config }
    }

    pub fn decode(_artifact_root: PathBuf, state: serde_json::Value) -> Result<Self> {
        let snapshot_config: FirecrackerSnapshotConfig =
            serde_json::from_value(state).context("deserialize Firecracker paused state")?;
        snapshot_config
            .validate_persisted()
            .context("validate Firecracker paused state artifacts")?;
        Ok(Self::new(snapshot_config))
    }

    pub fn snapshot_config(&self) -> &FirecrackerSnapshotConfig {
        &self.snapshot_config
    }
}

impl PausedSandboxState for FirecrackerPausedState {
    fn control_plane_port(&self) -> Option<u16> {
        let port = self.snapshot_config.common.control_plane_port;
        (port != 0).then_some(port)
    }

    fn encode(&self) -> Result<serde_json::Value> {
        serde_json::to_value(&self.snapshot_config).context("serialize Firecracker paused state")
    }

    fn runtime_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::from_overlaybd_image_configs(rootfs_and_extra_drive_image_config_paths(
            &self.snapshot_config.common,
        ))
    }
}

impl FirecrackerCapturedSnapshot {
    pub(crate) fn new(
        manifest: FirecrackerSnapshotManifest,
        snapshot_root: Arc<PersistentSnapshotRootGuard>,
    ) -> Self {
        Self {
            manifest,
            _snapshot_root: snapshot_root,
        }
    }

    pub fn manifest(&self) -> &FirecrackerSnapshotManifest {
        &self.manifest
    }
}

#[async_trait]
impl SandboxBackend for FirecrackerSandbox {
    async fn start(&mut self) -> Result<()> {
        FirecrackerSandbox::start(self).await
    }

    async fn start_nowait(&mut self) -> Result<()> {
        FirecrackerSandbox::start_nowait(self).await
    }

    async fn wait_for_ready(&self) -> Result<()> {
        FirecrackerSandbox::wait_for_ready(self).await
    }

    /// Pauses the VM and returns the paused state wrapped as a [`PausedSandboxState`].
    async fn pause(
        &mut self,
        artifact_root: Option<&Path>,
    ) -> SandboxCaptureResult<Arc<dyn PausedSandboxState>> {
        let pause_result = match artifact_root {
            Some(artifact_root) => FirecrackerSandbox::pause_to_dir(self, artifact_root)
                .await
                .map(|(snapshot_config, _)| snapshot_config),
            None => FirecrackerSandbox::pause(self).await,
        };
        let snapshot_config = match pause_result {
            Ok(snapshot_config) => snapshot_config,
            Err(err) => {
                let pause_err = SandboxCaptureError::from(err);
                if pause_err.is_terminal() {
                    return Err(pause_err);
                }
                if let Err(resume_err) = FirecrackerSandbox::resume(self).await {
                    return Err(SandboxCaptureError::terminal(anyhow::anyhow!(
                        "pause failed and sandbox could not be resumed: pause error: {pause_err}; resume error: {resume_err:#}"
                    )));
                }
                return Err(pause_err);
            }
        };
        Ok(Arc::new(FirecrackerPausedState::new(snapshot_config)))
    }

    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot> {
        let live_snapshot_root = self
            .live_snapshot_root()
            .await
            .map_err(SandboxCaptureError::from)?;
        let snapshot_dir = live_snapshot_root.path().join(Uuid::now_v7().to_string());

        let (_, manifest) = match self.pause_to_dir(&snapshot_dir).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                let snapshot_err = SandboxCaptureError::from(err);
                if snapshot_err.is_terminal() {
                    return Err(snapshot_err);
                }

                if let Err(resume_err) = FirecrackerSandbox::resume(self).await {
                    return Err(SandboxCaptureError::terminal(anyhow::anyhow!(
                        "snapshot capture failed and sandbox could not be resumed: capture error: {snapshot_err}; resume error: {resume_err:#}"
                    )));
                }

                return Err(snapshot_err);
            }
        };
        FirecrackerSandbox::resume(self)
            .await
            .map_err(SandboxCaptureError::terminal)?;

        Ok(CapturedSandboxSnapshot::new(
            FirecrackerCapturedSnapshot::new(manifest, live_snapshot_root),
        ))
    }

    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>> {
        let snapshot_config = match FirecrackerSandbox::pause(self).await {
            Ok(snapshot_config) => snapshot_config,
            Err(err) => {
                let checkpoint_err = SandboxCaptureError::from(err);
                if checkpoint_err.is_terminal() {
                    return Err(checkpoint_err);
                }
                if let Err(resume_err) = FirecrackerSandbox::resume(self).await {
                    return Err(SandboxCaptureError::terminal(anyhow::anyhow!(
                        "fork checkpoint failed and sandbox could not be resumed: checkpoint error: {checkpoint_err}; resume error: {resume_err:#}"
                    )));
                }
                return Err(checkpoint_err);
            }
        };

        FirecrackerSandbox::resume(self)
            .await
            .map_err(SandboxCaptureError::terminal)?;

        let children = spec
            .iter()
            .map(|child| {
                Self::from_snapshot_config_with_override(
                    snapshot_config.clone(),
                    child.sandbox_id,
                    child.envd_access_token.clone(),
                )
                .map(|child| Box::new(child) as Box<dyn SandboxBackend>)
                .context("build forked sandbox")
            })
            .collect::<Vec<_>>();

        let start_results = futures::future::join_all(children.into_iter().map(|child| {
            async move {
                let mut child = child?;
                match child.start().await {
                    Ok(()) => Ok(child),
                    Err(err) => {
                        if let Err(stop_err) = child.stop().await {
                            warn!(error = ?stop_err, "failed to stop fork child after start failure");
                        }
                        Err(err.context("start forked sandbox"))
                    }
                }
            }
        }))
        .await;
        Ok(start_results)
    }

    async fn resume(&mut self) -> Result<()> {
        FirecrackerSandbox::resume(self).await
    }

    async fn stop(&mut self) -> Result<()> {
        FirecrackerSandbox::stop(self).await
    }

    fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr> {
        FirecrackerSandbox::host_interaction_ip(self)
    }

    fn runtime_info(&self) -> SandboxRuntimeInfo {
        SandboxRuntimeInfo {
            rootfs_virtual_size: self.current_rootfs_virtual_size,
            runtime_artifacts: RuntimeArtifactSet::from_overlaybd_image_configs(
                self.runtime_image_config_paths(),
            ),
        }
    }

    fn startup_artifacts(&self) -> RuntimeArtifactSet {
        let common = match &self.launch {
            LaunchMode::Fresh(config) => &config.common,
            LaunchMode::Resume(config) => &config.common,
        };
        RuntimeArtifactSet::from_overlaybd_image_configs(rootfs_and_extra_drive_image_config_paths(
            common,
        ))
    }

    async fn update_network_policy(&mut self, policy: Option<SandboxNetworkPolicy>) -> Result<()> {
        if let Some(slot) = self.network_slot.as_mut() {
            slot.set_egress_policy(policy.as_ref())
                .context("configure sandbox network policy")?;
            self.current_network_policy = policy;
            Ok(())
        } else if policy.is_none() {
            self.current_network_policy = policy;
            Ok(())
        } else {
            bail!("sandbox has no active network slot")
        }
    }

    fn update_custom_extension_params(&mut self, params: Option<CustomExtensionParams>) {
        self.current_custom_extension_params = params;
    }
}

// ── SandboxExecutor impl ──────────────────────────────────────────────────────

#[async_trait(?Send)]
impl SandboxExecutor for FirecrackerSandbox {
    fn executor(&self) -> Result<Executor<'_>> {
        let envd = self
            .envd_instance
            .as_ref()
            .context("Sandbox is not running")?;
        Ok(Executor::new(envd))
    }
}

// ── FirecrackerSandbox public API ────────────────────────────────────────────

impl FirecrackerSandbox {
    fn snapshot_rootfs_virtual_size(&self) -> Result<u64> {
        self.current_rootfs_virtual_size.context(
            "rootfs virtual size cache missing; sandbox must record the user image block-device size before snapshot; ensure start() was called before pause() or snapshot",
        )
    }

    /// Create a sandbox handle for a fresh boot.
    ///
    /// This does not start Firecracker; it only prepares the object and its
    /// per-instance work directory.
    pub fn new(config: FirecrackerSandboxConfig) -> Result<Self> {
        Self::new_with_id(config, SandboxId::new())
    }

    pub(crate) fn new_with_id(config: FirecrackerSandboxConfig, id: SandboxId) -> Result<Self> {
        debug!(
            firecracker_binary = %config.common.firecracker_binary.display(),
            kernel_image = %config.kernel_image.display(),
            tools_drive_version = %config.common.tools_drive_version,
            "creating fresh firecracker sandbox"
        );
        Self::build(id, LaunchMode::Fresh(config))
    }

    /// Create a sandbox handle that resumes from the provided snapshot config.
    ///
    /// This only prepares the sandbox object and its per-instance workspace.
    /// Call [`FirecrackerSandbox::start`] or [`FirecrackerSandbox::start_nowait`] to boot it.
    #[tracing::instrument(skip(snapshot))]
    pub fn from_snapshot_config(snapshot: &FirecrackerSnapshotConfig) -> Result<Self> {
        Self::from_snapshot_config_with_override(
            snapshot.clone(),
            SandboxId::new(),
            snapshot.common.envd_access_token.clone(),
        )
    }

    /// Build a throwaway profiling sandbox. It loads the snapshot with dirty
    /// tracking disabled and an exclusive memory UBLK device.
    pub(crate) fn from_profiling_snapshot_config(
        snapshot: &FirecrackerSnapshotConfig,
    ) -> Result<Self> {
        let mut snapshot = snapshot.clone();
        snapshot.common.track_dirty_pages = false;
        let mut sandbox = Self::from_snapshot_config(&snapshot)?;
        sandbox.profiling_mode = true;
        Ok(sandbox)
    }

    pub(crate) fn from_snapshot_config_with_override(
        mut snapshot: FirecrackerSnapshotConfig,
        id: SandboxId,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Self> {
        // Runtime identity and auth override their values in the source snapshot.
        snapshot.common.envd_access_token = envd_access_token;
        let FirecrackerCommonConfig {
            mmds_metadata,
            envd_access_token,
            ..
        } = &mut snapshot.common;
        let metadata = mmds_metadata.get_or_insert_with(|| MmdsMetadata::new(id, "unknown"));
        metadata.sandbox_id = id.to_string();
        metadata.set_access_token(envd_access_token.as_ref());

        debug!(
            vm_state_path = %snapshot.vm_state_path.display(),
            mem_image_config_path = %snapshot.mem_overlaybd_config.image_config_path.display(),
            rootfs_path = ?snapshot.common.rootfs_image_config.as_ref().map(|rootfs| &rootfs.image_config_path),
            tools_drive_version = %snapshot.common.tools_drive_version,
            "creating firecracker sandbox from snapshot config"
        );
        Self::build(id, LaunchMode::Resume(snapshot))
    }

    /// Create a sandbox handle that boots from a resolved runnable committed snapshot.
    ///
    /// This only prepares the sandbox object and its per-instance workspace.
    /// Call [`FirecrackerSandbox::start`] or [`FirecrackerSandbox::start_nowait`] to boot it.
    pub fn from_snapshot(
        snapshot: &RunnableSnapshot,
        launch_config: &SandboxLaunchConfig,
    ) -> Result<Self> {
        let snapshot_config = Self::snapshot_config_for_launch(snapshot, launch_config)?;

        let sandbox = Self::build(
            launch_config.sandbox_id,
            LaunchMode::Resume(snapshot_config),
        )?;
        Ok(sandbox)
    }

    #[cfg(test)]
    fn from_snapshot_with_test_work_dir(
        snapshot: &RunnableSnapshot,
        launch_config: &SandboxLaunchConfig,
        work_dir: &Path,
    ) -> Result<Self> {
        let mut snapshot_config = Self::snapshot_config_for_launch(snapshot, launch_config)?;
        snapshot_config.common.firecracker_work_base_dir = Some(work_dir.to_path_buf());

        Self::build(
            launch_config.sandbox_id,
            LaunchMode::Resume(snapshot_config),
        )
    }

    fn snapshot_config_for_launch(
        snapshot: &RunnableSnapshot,
        launch_config: &SandboxLaunchConfig,
    ) -> Result<FirecrackerSnapshotConfig> {
        let mut snapshot_config = FirecrackerSnapshotConfig::from_runnable_snapshot(snapshot)?;
        snapshot_config.common.mmds_metadata = Some(
            MmdsMetadata::new(launch_config.sandbox_id, launch_config.snapshot_id.clone())
                .with_access_token(launch_config.envd_access_token.as_ref())
                .with_extra(launch_config.extra_mmds.clone()),
        );
        snapshot_config.common.envd_access_token = launch_config.envd_access_token.clone();
        snapshot_config.common.network_policy = launch_config.network.clone();

        // Launch-provided custom config overrides the value persisted in the
        // source snapshot; otherwise inherit the snapshot's.
        snapshot_config.common.custom_extension_params = launch_config
            .custom_extension_params
            .clone()
            .or_else(|| snapshot.committed().custom_extension_params.clone());

        if let Some(launch_env_vars) = &launch_config.env_vars {
            snapshot_config
                .common
                .env_vars
                .get_or_insert_default()
                .extend(launch_env_vars.clone());
        }

        Ok(snapshot_config)
    }

    /// Start the sandbox by launching Firecracker and waiting for readiness.
    ///
    /// This waits for the Firecracker API socket and the in-guest envd daemon.
    #[tracing::instrument(skip(self))]
    pub async fn start(&mut self) -> Result<()> {
        debug!("starting firecracker sandbox");
        self.start_nowait().await?;
        self.wait_for_ready().await
    }

    async fn start_with_prefault(&mut self, prefault_enabled: bool) -> Result<()> {
        self.start_with_prefault_and_timings(prefault_enabled)
            .await
            .map(|_| ())
    }

    async fn start_with_prefault_and_timings(
        &mut self,
        prefault_enabled: bool,
    ) -> Result<SnapshotResumeTimings> {
        self.start_with_prefault_and_timings_with_max_prefault_bytes(prefault_enabled, None)
            .await
    }

    async fn start_with_prefault_and_timings_with_max_prefault_bytes(
        &mut self,
        prefault_enabled: bool,
        max_prefault_bytes: Option<u64>,
    ) -> Result<SnapshotResumeTimings> {
        self.launch.validate()?;
        let LaunchMode::Resume(config) = &self.launch else {
            bail!("pre-fault override requires a snapshot-resume launch");
        };
        let mut timings = SnapshotResumeTimings::default();
        self.start_resume_with_prefault(
            config.clone(),
            prefault_enabled,
            max_prefault_bytes,
            Some(&mut timings),
        )
        .await?;
        let envd_ready_started = std::time::Instant::now();
        self.wait_for_ready().await?;
        timings.envd_ready = envd_ready_started.elapsed();
        Ok(timings)
    }
    /// Start the sandbox WITHOUT waiting for envd's readiness.
    ///
    /// This only waits for the Firecracker API socket to be available and
    /// returns immediately after VM start command is issued.
    pub(crate) async fn start_nowait(&mut self) -> Result<()> {
        self.launch.validate()?;
        trace!("launch config validated");
        match &self.launch {
            LaunchMode::Fresh(config) => self.start_fresh(config.clone()).await,
            LaunchMode::Resume(config) => self.start_resume(config.clone()).await,
        }
    }

    async fn start_profiling_paused(&mut self) -> Result<()> {
        self.launch.validate()?;
        let LaunchMode::Resume(config) = &self.launch else {
            bail!("profiling requires a snapshot-resume launch");
        };
        self.start_resume_with_options(config.clone(), false, None, None)
            .await
    }

    async fn wait_for_envd_ready(&self) -> Result<()> {
        let Some(envd_instance) = self.envd_instance.as_ref() else {
            return Err(anyhow::anyhow!("envd instance not initialized"));
        };
        envd_instance
            .wait_for_ready(
                self.runtime_policy.envd_timeout,
                self.runtime_policy.envd_poll_interval,
            )
            .await
    }

    async fn release_background_downloads_after_envd_ready(&self) {
        if !self.profiling_mode {
            if let Some(device_key) = &self.mem_snapshot_image_config_path {
                // envd is up: release held background downloads for this memory
                // device. Best-effort - downloads would also start after the
                // fallback timeout.
                UblkDeviceManager::global()
                    .notify_sandbox_ready(device_key)
                    .await;
            }
        }
        if let Some(device_key) = &self.rootfs_image_config_path {
            // Same release for the rootfs image's background download.
            UblkDeviceManager::global()
                .notify_sandbox_ready(device_key)
                .await;
        }
    }

    async fn initialize_envd(&self) -> Result<()> {
        let Some(envd_instance) = self.envd_instance.as_ref() else {
            return Err(anyhow::anyhow!("envd instance not initialized"));
        };
        envd_instance
            .init(
                self.launch.common().env_vars.clone(),
                self.launch.common().default_workdir.clone(),
                self.launch.common().default_user.clone(),
            )
            .await
    }

    /// Wait for the sandbox to be fully ready.
    ///
    /// This should be called after start_nowait if you want to interact with the sandbox.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn wait_for_ready(&self) -> Result<()> {
        self.wait_for_envd_ready().await?;
        self.release_background_downloads_after_envd_ready().await;
        self.initialize_envd().await
    }

    /// Pause the running sandbox and create a snapshot for later resume.
    ///
    /// This produces `vm_state.bin`, an overlaybd memory layer, and rootfs state
    /// owned by the returned [`FirecrackerSnapshotConfig`]. For overlaybd-backed
    /// sandboxes, the snapshot stores a copy of the writable upper under the
    /// snapshot dir.
    ///
    /// The snapshot artifacts are stored in a managed temporary directory that is
    /// automatically cleaned up when the reference count drops to zero.
    /// Use [`FirecrackerSandbox::pause_to_dir`] to specify a custom, caller-managed
    /// directory for the snapshot artifacts.
    ///
    /// The managed snapshot root is structured as `<managed-snapshot-base>/<sandbox_id>/<uuid>`, where:
    /// - `<managed-snapshot-base>` is `[firecracker].work_dir/managed-snapshots`, or
    ///   `<system-temp>/aenv/managed-snapshots` when `work_dir` is unset.
    /// - `<sandbox_id>` is the [`SandboxID`](crate::types::SandboxId), used to group snapshots by sandbox and improve readability.
    pub async fn pause(&mut self) -> Result<FirecrackerSnapshotConfig> {
        let snapshot_root = self.live_snapshot_root().await?;
        snapshot_root.prepare().await?;
        let snapshot_dir = snapshot_root.path().join(Uuid::now_v7().to_string());

        let (mut snapshot, _) = self.pause_to_dir(&snapshot_dir).await?;
        snapshot.managed_snapshot_root = Some(snapshot_root);

        Ok(snapshot)
    }

    /// Pause the running sandbox and persist its snapshot artifacts into a caller-managed directory.
    #[tracing::instrument(skip(self, snapshot_dir))]
    pub async fn pause_to_dir(
        &mut self,
        snapshot_dir: &Path,
    ) -> Result<(FirecrackerSnapshotConfig, FirecrackerSnapshotManifest)> {
        debug!(snapshot_dir = %snapshot_dir.display(), "pausing sandbox");
        self.fc_instance.pause().await?;

        tokio::fs::create_dir_all(snapshot_dir)
            .await
            .with_context(|| format!("create snapshot dir {}", snapshot_dir.display()))?;

        let snapshot_result = self.snapshot_to_dir(snapshot_dir).await;
        match snapshot_result {
            Ok(snapshot) => Ok(snapshot),
            Err(err) => {
                Self::cleanup_failed_snapshot_dir(snapshot_dir).await;
                Err(err)
            }
        }
    }

    async fn snapshot_to_dir(
        &self,
        snapshot_dir: &Path,
    ) -> Result<(FirecrackerSnapshotConfig, FirecrackerSnapshotManifest)> {
        let vm_state_path = snapshot_dir.join(VM_STATE_FILE_NAME);
        let memory_output = OverlaybdCompactOutput::from_memory_snapshot_config(
            &ConfigManager::global_config().memory_snapshot,
        );
        let (mem_layer_path, mem_virtual_size) = self
            .snapshot_memory_to_overlaybd(&vm_state_path, snapshot_dir, memory_output)
            .await?;

        // Build the memory image config: collect parent layers, make runtime
        // lowers local to this snapshot dir, and compact only if the layer
        // count exceeds the configured maximum.
        let resume_mem_image_config_path = match &self.launch {
            LaunchMode::Resume(config) => {
                Some(config.mem_overlaybd_config.image_config_path.as_path())
            }
            LaunchMode::Fresh(_) => None,
        };
        let mem_image_config = build_mem_snapshot_image_config(
            resume_mem_image_config_path,
            &mem_layer_path,
            snapshot_dir,
            memory_output,
        )
        .await?;
        let mem_image_config_path = snapshot_dir.join("mem_image.json");
        tokio::fs::write(
            &mem_image_config_path,
            serde_json::to_vec_pretty(&mem_image_config)
                .context("serialize mem image config for persistent dir")?,
        )
        .await
        .with_context(|| {
            format!(
                "write mem image config to {}",
                mem_image_config_path.display()
            )
        })?;

        let mem_overlaybd_config = OverlaybdConfig {
            image_config_path: mem_image_config_path,
            read_only: true,
            runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
        };

        let (base_rootfs_path, rootfs_virtual_size) = if self.uses_overlaybd_ublk() {
            let overlaybd_source = self
                .launch
                .common()
                .ublk_config
                .as_ref()
                .map(|config| match &config.backend {
                    UblkBackend::Overlaybd(source) => source,
                })
                .context("overlaybd snapshot requires overlaybd-backed ublk config")?;
            let rootfs_runtime = self
                .rootfs_runtime
                .as_ref()
                .context("overlaybd snapshot requires an active ublk device")?;
            let rootfs_image_path = restack_snapshot_overlaybd_rootfs(
                &rootfs_runtime.device,
                overlaybd_source.read_only,
                &rootfs_runtime.image_config_path,
                snapshot_dir,
            )
            .await
            .context("snapshot overlaybd runtime state to persistent dir")?;
            let size = self
                .snapshot_rootfs_virtual_size()
                .context("persist rootfs virtual size for snapshot")?;
            (rootfs_image_path, size)
        } else {
            let rootfs_path = snapshot_dir.join(ROOTFS_DRIVE_PATH);
            // Preserve the writable disk state alongside the snapshot.
            let current_rootfs = self.work_dir.path().join(ROOTFS_DRIVE_PATH);
            copy_cow(&current_rootfs, &rootfs_path).await?;
            let size = self
                .snapshot_rootfs_virtual_size()
                .context("persist rootfs virtual size for snapshot")?;
            (rootfs_path, size)
        };
        let snapshot_extra_drives = self
            .snapshot_extra_drives(snapshot_dir)
            .await
            .context("snapshot extra drives to persistent dir")?;
        let mut snapshot_common = self.launch.common().clone();
        snapshot_common.network_policy = self.current_network_policy.clone();
        snapshot_common.custom_extension_params = self.current_custom_extension_params.clone();
        snapshot_common.extra_drives = snapshot_extra_drives.clone();
        let mut rootfs_read_only = false;

        // Rewrite the overlaybd backend's image config path to point at the snapshot's rootfs.
        // So that the resumed ublk device uses the captured rootfs layers instead of the original ones.
        if let Some(ublk_config) = snapshot_common.ublk_config.as_mut() {
            let UblkBackend::Overlaybd(source) = &mut ublk_config.backend;
            rootfs_read_only = source.read_only;
            source.image_config_path = base_rootfs_path.clone();
        }
        let runtime_upper_mode = snapshot_common
            .ublk_config
            .as_ref()
            .map(|config| match &config.backend {
                UblkBackend::Overlaybd(source) => source.runtime_upper_mode,
            })
            .unwrap_or(overlaybd::config::UpperMode::LogStructured);
        snapshot_common.rootfs_image_config = Some(OverlaybdConfig {
            image_config_path: base_rootfs_path.clone(),
            read_only: rootfs_read_only,
            runtime_upper_mode,
        });
        snapshot_common.rootfs_virtual_size = Some(rootfs_virtual_size);

        let manifest = FirecrackerSnapshotManifest::new(
            vm_state_path.clone(),
            mem_overlaybd_config.image_config_path.clone(),
            mem_virtual_size,
            base_rootfs_path,
            rootfs_virtual_size,
            &snapshot_extra_drives,
        )
        .context("build firecracker snapshot manifest")?;

        let snapshot = FirecrackerSnapshotConfig {
            common: snapshot_common,
            vm_state_path,
            mem_overlaybd_config,
            mem_virtual_size,
            restore_working_set: None,
            managed_snapshot_root: None,
        };

        debug!(
            vm_state_path = %snapshot.vm_state_path.display(),
            mem_image_config_path = %snapshot.mem_overlaybd_config.image_config_path.display(),
            rootfs_path = ?snapshot.common.rootfs_image_config.as_ref().map(|rootfs| &rootfs.image_config_path),
            "persistent snapshot created"
        );
        Ok((snapshot, manifest))
    }

    async fn snapshot_memory_to_overlaybd(
        &self,
        vm_state_path: &Path,
        snapshot_dir: &Path,
        memory_output: OverlaybdCompactOutput,
    ) -> Result<(PathBuf, u64)> {
        let mem_overlaybd_dir = snapshot_dir.join("mem_overlaybd");
        let firecracker_pid = self.fc_instance.pid()?;
        self.fc_instance
            .create_state_only_snapshot(vm_state_path)
            .await?;
        // `vm_state.bin` now represents this paused VM state. Any later
        // error aborts this direct snapshot attempt and is propagated to
        // the lifecycle caller for recovery.
        let dirty_ranges = self.fc_instance.get_dirty_memory_ranges().await?;
        convert_dirty_memory_to_overlaybd(
            firecracker_pid,
            &dirty_ranges,
            &mem_overlaybd_dir,
            memory_output,
        )
        .await
        .context("convert dirty memory ranges to overlaybd layer")
    }

    /// Resume a paused sandbox in-place.
    ///
    /// Use this when you want to keep the same sandbox instance.
    pub async fn resume(&self) -> Result<()> {
        debug!("resuming paused sandbox in-place");
        self.fc_instance.resume().await?;
        Ok(())
    }

    /// Resume a new sandbox instance from snapshot config.
    ///
    /// This creates a new sandbox, starts Firecracker, and loads the snapshot config.
    #[tracing::instrument(skip(snapshot))]
    pub async fn resume_from_snapshot_config(snapshot: &FirecrackerSnapshotConfig) -> Result<Self> {
        let mut sandbox = Self::from_snapshot_config(snapshot)?;
        sandbox.start().await?;
        Ok(sandbox)
    }

    async fn collect_mincore_stage(
        &self,
        phase: &'static str,
        previous: &mut Option<Vec<ResidentMemoryRange>>,
    ) -> Result<SnapshotMincoreStage> {
        let current = self.fc_instance.get_resident_memory_ranges().await?;
        let stage = snapshot_mincore_stage(phase, previous.as_deref(), &current)?;
        *previous = Some(current);
        Ok(stage)
    }

    /// Profile mincore at each restore boundary on one dedicated profiler VM.
    ///
    /// This diagnostic records pages after snapshot load, resume, envd readiness,
    /// envd initialization, and the supplied first workload. It does not update
    /// snapshot metadata or issue pre-fault requests.
    pub async fn profile_snapshot_mincore_stages<F>(
        snapshot: &FirecrackerSnapshotConfig,
        workload: F,
    ) -> Result<Vec<SnapshotMincoreStage>>
    where
        F: for<'a> FnOnce(&'a Self) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>,
    {
        warn_if_mincore_host_has_swap();
        let mut profiler = Self::from_profiling_snapshot_config(snapshot)?;
        let result = async {
            let mut previous = None;
            let mut stages = Vec::with_capacity(5);

            profiler.start_profiling_paused().await?;
            stages.push(
                profiler
                    .collect_mincore_stage("snapshot_loaded_paused", &mut previous)
                    .await?,
            );

            profiler.fc_instance.resume().await?;
            stages.push(
                profiler
                    .collect_mincore_stage("firecracker_resumed", &mut previous)
                    .await?,
            );

            profiler.wait_for_envd_ready().await?;
            stages.push(
                profiler
                    .collect_mincore_stage("envd_ready", &mut previous)
                    .await?,
            );

            profiler
                .release_background_downloads_after_envd_ready()
                .await;
            profiler.initialize_envd().await?;
            stages.push(
                profiler
                    .collect_mincore_stage("envd_initialized", &mut previous)
                    .await?,
            );

            workload(&profiler)
                .await
                .context("run first workload during mincore profiling")?;
            stages.push(
                profiler
                    .collect_mincore_stage("first_workload_complete", &mut previous)
                    .await?,
            );

            Ok(stages)
        }
        .await;
        let stop_result = profiler.stop().await;
        match (result, stop_result) {
            (Ok(stages), Ok(())) => Ok(stages),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error.context("stop dedicated profiling sandbox")),
            (Err(run_error), Err(stop_error)) => Err(anyhow::anyhow!(
                "mincore stage profiling failed: {run_error:#}; additionally failed to stop profiler: {stop_error:#}"
            )),
        }
    }

    /// Build cumulative baseline-excluded GPA candidates at the restore
    /// boundaries used by the normal ready path. This is intentionally for
    /// controlled benchmark selection; it neither publishes metadata nor
    /// issues a pre-fault request.
    pub async fn profile_snapshot_prefault_candidates(
        snapshot: &FirecrackerSnapshotConfig,
        limits: GuestMemoryWorkingSetLimits,
    ) -> Result<Vec<SnapshotPrefaultCandidate>> {
        warn_if_mincore_host_has_swap();
        let mut profiler = Self::from_profiling_snapshot_config(snapshot)?;
        let result = async {
            profiler.start_profiling_paused().await?;
            let baseline = profiler.fc_instance.get_resident_memory_ranges().await?;
            let regions = profiler
                .fc_instance
                .get_guest_memory_image_regions()
                .await?;
            let mut candidates = Vec::with_capacity(3);

            profiler.fc_instance.resume().await?;
            let resumed = profiler.fc_instance.get_resident_memory_ranges().await?;
            candidates.push(snapshot_prefault_candidate(
                "firecracker_resumed",
                &baseline,
                &resumed,
                &regions,
                limits,
            )?);

            profiler.wait_for_envd_ready().await?;
            let ready = profiler.fc_instance.get_resident_memory_ranges().await?;
            candidates.push(snapshot_prefault_candidate(
                "envd_ready",
                &baseline,
                &ready,
                &regions,
                limits,
            )?);

            profiler
                .release_background_downloads_after_envd_ready()
                .await;
            profiler.initialize_envd().await?;
            let initialized = profiler.fc_instance.get_resident_memory_ranges().await?;
            candidates.push(snapshot_prefault_candidate(
                "envd_initialized",
                &baseline,
                &initialized,
                &regions,
                limits,
            )?);

            Ok(candidates)
        }
        .await;
        let stop_result = profiler.stop().await;
        match (result, stop_result) {
            (Ok(candidates), Ok(())) => Ok(candidates),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error.context("stop dedicated profiling sandbox")),
            (Err(run_error), Err(stop_error)) => Err(anyhow::anyhow!(
                "mincore pre-fault candidate profiling failed: {run_error:#}; additionally failed to stop profiler: {stop_error:#}"
            )),
        }
    }

    /// Resume from a snapshot while explicitly selecting whether to apply its pre-fault hint.
    ///
    /// This is intended for controlled diagnostics and benchmarks; normal restores use
    /// [`FirecrackerSandbox::resume_from_snapshot_config`] and node configuration.
    #[tracing::instrument(skip(snapshot))]
    pub async fn resume_from_snapshot_config_with_prefault(
        snapshot: &FirecrackerSnapshotConfig,
        prefault_enabled: bool,
    ) -> Result<Self> {
        let mut sandbox = Self::from_snapshot_config(snapshot)?;
        sandbox.start_with_prefault(prefault_enabled).await?;
        Ok(sandbox)
    }

    /// Resume from a snapshot with an explicit pre-fault choice and record
    /// each restore stage. This is for controlled diagnostics and benchmarks;
    /// normal restores retain their configuration-driven behavior.
    #[tracing::instrument(skip(snapshot))]
    pub async fn resume_from_snapshot_config_with_prefault_and_timings(
        snapshot: &FirecrackerSnapshotConfig,
        prefault_enabled: bool,
    ) -> Result<(Self, SnapshotResumeTimings)> {
        Self::resume_from_snapshot_config_with_prefault_and_timings_for_benchmark(
            snapshot,
            prefault_enabled,
            None,
        )
        .await
    }

    /// Resume from a snapshot with benchmark-only pre-fault limits.
    ///
    /// This deliberately requires an explicit caller-provided limit. Product
    /// restores continue to use node configuration and cannot reach this API.
    #[tracing::instrument(skip(snapshot))]
    pub async fn resume_from_snapshot_config_with_prefault_and_timings_for_benchmark(
        snapshot: &FirecrackerSnapshotConfig,
        prefault_enabled: bool,
        max_prefault_bytes: Option<u64>,
    ) -> Result<(Self, SnapshotResumeTimings)> {
        let mut sandbox = Self::from_snapshot_config(snapshot)?;
        let timings = sandbox
            .start_with_prefault_and_timings_with_max_prefault_bytes(
                prefault_enabled,
                max_prefault_bytes,
            )
            .await?;
        Ok((sandbox, timings))
    }

    /// Load a dedicated throwaway profiler paused, collect its baseline, then
    /// resume only through envd readiness and harvest the newly resident
    /// Firecracker mincore ranges.
    ///
    /// This deliberately excludes envd initialization and any guest workload:
    /// published metadata targets the restore-to-ready path.
    /// The profiler owns an exclusive memory UBLK device and never releases its
    /// memory background download notification.
    pub async fn profile_snapshot_working_set(
        snapshot: &FirecrackerSnapshotConfig,
        limits: GuestMemoryWorkingSetLimits,
    ) -> Result<GuestMemoryWorkingSet> {
        warn_if_mincore_host_has_swap();
        let mut profiler = Self::from_profiling_snapshot_config(snapshot)?;
        let result = async {
            profiler.start_profiling_paused().await?;
            let baseline = profiler.fc_instance.get_resident_memory_ranges().await?;
            profiler.fc_instance.resume().await?;
            profiler.wait_for_envd_ready().await?;
            let ready = profiler.fc_instance.get_resident_memory_ranges().await?;
            let regions = profiler
                .fc_instance
                .get_guest_memory_image_regions()
                .await?;
            snapshot_prefault_candidate("envd_ready", &baseline, &ready, &regions, limits)
                .map(|candidate| candidate.working_set)
        }
        .await;
        let stop_result = profiler.stop().await;
        match (result, stop_result) {
            (Ok(working_set), Ok(())) => Ok(working_set),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error.context("stop dedicated profiling sandbox")),
            (Err(run_error), Err(stop_error)) => Err(anyhow::anyhow!(
                "mincore ready-path profiling failed: {run_error:#}; additionally failed to stop profiler: {stop_error:#}"
            )),
        }
    }

    /// Profile the guest-memory working set after running a supplied guest
    /// workload on the dedicated profiler restore.
    ///
    /// The workload runs only after envd is ready and before the profiler is
    /// paused and sampled, so the resulting metadata represents the same
    /// ready-plus-workload path used by the caller's restore benchmark.
    pub async fn profile_snapshot_working_set_with_workload<F>(
        snapshot: &FirecrackerSnapshotConfig,
        limits: GuestMemoryWorkingSetLimits,
        workload: F,
    ) -> Result<GuestMemoryWorkingSet>
    where
        F: for<'a> FnOnce(&'a Self) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>,
    {
        warn_if_mincore_host_has_swap();
        let mut profiler = Self::from_profiling_snapshot_config(snapshot)?;
        let result = async {
            profiler.start_profiling_paused().await?;
            let baseline = profiler.fc_instance.get_resident_memory_ranges().await?;
            let baseline_stats = resident_range_stats(&baseline)?;
            profiler.fc_instance.resume().await?;
            profiler.wait_for_ready().await?;
            workload(&profiler)
                .await
                .context("run guest workload during working-set profiling")?;
            profiler.fc_instance.pause().await?;
            let resident = profiler.fc_instance.get_resident_memory_ranges().await?;
            let final_stats = resident_range_stats(&resident)?;
            let newly_resident = newly_resident_ranges(&baseline, &resident)?;
            let newly_resident_stats = resident_range_stats(&newly_resident)?;
            let regions = profiler
                .fc_instance
                .get_guest_memory_image_regions()
                .await?;
            let working_set = resident_ranges_to_working_set(&newly_resident, &regions, limits)?;
            debug!(
                baseline_ranges = baseline_stats.range_count,
                baseline_bytes = baseline_stats.bytes,
                final_ranges = final_stats.range_count,
                final_bytes = final_stats.bytes,
                newly_resident_ranges = newly_resident_stats.range_count,
                newly_resident_bytes = newly_resident_stats.bytes,
                coalesced_ranges = working_set.ranges.len(),
                working_set_bytes = working_set.total_bytes()?,
                "collected mincore profiling working set"
            );
            Ok(working_set)
        }
        .await;
        let stop_result = profiler.stop().await;
        match (result, stop_result) {
            (Ok(working_set), Ok(())) => Ok(working_set),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error.context("stop dedicated profiling sandbox")),
            (Err(run_error), Err(stop_error)) => Err(anyhow::anyhow!(
                "profiling failed: {run_error:#}; additionally failed to stop profiler: {stop_error:#}"
            )),
        }
    }

    /// Stop the Firecracker process and release network resources.
    ///
    /// Sends SIGTERM and waits for exit; if it times out, sends SIGKILL.
    #[tracing::instrument(skip(self))]
    pub async fn stop(&mut self) -> Result<()> {
        debug!("stopping firecracker sandbox");

        self.fc_instance
            .stop(self.runtime_policy.socket_timeout)
            .await?;

        // Clear envd instance
        self.envd_instance = None;

        // Cleanup ublk device (must happen after FC stop, before network cleanup)
        if let Some(runtime) = self.rootfs_runtime.take() {
            if let Err(e) = UblkDeviceManager::global()
                .release_device(&runtime.device)
                .await
            {
                warn!(error = %e, "failed to release ublk device during stop");
            }
        }

        // Shared memory device: release explicitly so a following resume for
        // the same memory image cannot race the detached Drop cleanup.
        if let Some(mem_device) = self.mem_ublk_device.take() {
            if let Err(e) = mem_device.release().await {
                warn!(error = %e, "failed to release shared memory ublk device during stop");
            }
        }
        if let Some(mem_device) = self.profiling_mem_ublk_device.take() {
            if let Err(e) = UblkDeviceManager::global()
                .release_device(&mem_device)
                .await
            {
                warn!(error = %e, "failed to release profiler memory ublk device during stop");
            }
        }

        for runtime in self.extra_drive_runtimes.drain(..) {
            if let Err(e) = UblkDeviceManager::global()
                .release_device(&runtime.device)
                .await
            {
                warn!(error = %e, "failed to release extra drive device during stop");
            }
        }

        // Invoke the stop hook before releasing network resources. Delivery
        // failures are logged inside the client and never fail stop().
        if let Some(guard) = self.custom_extension_hook_guard.take() {
            guard.stop().await;
        }

        // Cleanup network resources
        if let Some(slot) = self.network_slot.take() {
            let idx = slot.idx;
            NetworkManager::global()
                .release(slot)
                .context("Failed to release network slot")?;
            debug!(slot = idx, "network slot released");
        }

        debug!("firecracker sandbox stopped");
        Ok(())
    }

    pub(crate) fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr> {
        self.network_slot
            .as_ref()
            .map(|slot| slot.host_interaction_ip)
    }

    pub(crate) fn firecracker_binary_path(&self) -> &Path {
        &self.launch.common().firecracker_binary
    }

    pub(crate) fn tools_drive_version(&self) -> &str {
        &self.launch.common().tools_drive_version
    }

    /// Resolve the Firecracker stdout log path for this sandbox.
    pub fn firecracker_stdout_path(&self) -> PathBuf {
        self.launch
            .common()
            .stdout_path
            .clone()
            .unwrap_or_else(|| self.default_log_dir())
            .join("firecracker-stdout.log")
    }

    /// Resolve the Firecracker stderr log path for this sandbox.
    pub fn firecracker_stderr_path(&self) -> PathBuf {
        self.launch
            .common()
            .stderr_path
            .clone()
            .unwrap_or_else(|| self.default_log_dir())
            .join("firecracker-stderr.log")
    }

    /// Resolve the Firecracker logger output path for this sandbox.
    ///
    /// Lives in the default log directory and is named `firecracker.log`.
    /// Only used when `firecracker_log_level` is set.
    pub fn firecracker_log_path(&self) -> PathBuf {
        self.default_log_dir().join("firecracker.log")
    }

    fn default_log_dir(&self) -> PathBuf {
        self.launch
            .common()
            .serial_output_base_dir
            .clone()
            .map(|p| p.join(self.id.to_string()))
            .unwrap_or_else(|| self.work_dir.path().join("logs"))
    }

    fn uses_overlaybd_ublk(&self) -> bool {
        self.launch.common().ublk_config.is_some()
    }

    fn mmds_metadata(&self, common: &FirecrackerCommonConfig) -> MmdsMetadata {
        common
            .mmds_metadata
            .clone()
            .unwrap_or_else(|| MmdsMetadata::new(self.id, "unknown"))
    }

    /// Return the writable user image path inside this sandbox's Firecracker CWD.
    pub fn work_rootfs_path(&self) -> PathBuf {
        self.work_dir.path().join(USER_ROOTFS_DRIVE_PATH)
    }

    fn new_managed_persistent_snapshot_root(&self) -> Arc<PersistentSnapshotRootGuard> {
        let sandbox_dir = self.id.to_string();
        let root = managed_snapshot_base().join(sandbox_dir);
        Arc::new(PersistentSnapshotRootGuard::new(root))
    }

    async fn live_snapshot_root(&mut self) -> Result<Arc<PersistentSnapshotRootGuard>> {
        if let Some(root) = &self.live_snapshot_root {
            return Ok(Arc::clone(root));
        }
        let root = self
            .launch
            .managed_snapshot_root()
            .unwrap_or_else(|| self.new_managed_persistent_snapshot_root());
        root.prepare().await?;
        self.live_snapshot_root = Some(Arc::clone(&root));
        Ok(root)
    }

    /// Best-effort cleanup of a caller-managed snapshot directory after a failed pause.
    ///
    /// Removes the directory contents so the caller isn't left with a partially-written snapshot.
    async fn cleanup_failed_snapshot_dir(path: &Path) {
        match tokio::fs::remove_dir_all(path).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                warn!(
                    snapshot_dir = %path.display(),
                    error = %err,
                    "failed to clean up incomplete snapshot directory"
                );
            }
        }
    }
}

/// Overlaybd image config paths a sandbox opens (rootfs + extra drives).
/// For fresh launches these are source configs; for paused states they are
/// snapshot artifact configs.
/// (Memory snapshot layers are remote/repository-backed, never local-only, so
/// they are not included.)
fn rootfs_and_extra_drive_image_config_paths(common: &FirecrackerCommonConfig) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(rootfs) = &common.rootfs_image_config {
        paths.push(rootfs.image_config_path.clone());
    }
    paths.extend(
        common
            .extra_drives
            .iter()
            .map(|drive| drive.image_config_path().to_path_buf()),
    );
    paths
}

/// Build the `agentenv_drives=vdc:/mnt/data,vdd:/mnt/logs:sub/path` boot arg
/// for extra drives. Returns `None` if there are no extra drives.
/// Drive letter mapping: vda = tools drive, vdb = user image, vdc = first extra drive.
/// Each entry is `vd<letter>:<mountPath>[:<subPath>]`, with optional
/// Kubernetes-style `subPath` semantics. API validation rejects `:` in both
/// `mountPath` and `subPath`, so the `:` separators are unambiguous.
fn build_drives_boot_arg(extra_drives: &[ExtraDrive]) -> Option<String> {
    if extra_drives.is_empty() {
        return None;
    }
    assert!(
        extra_drives.len() <= MAX_EXTRA_DRIVES,
        "too many extra drives for guest naming: {} > {}",
        extra_drives.len(),
        MAX_EXTRA_DRIVES
    );
    let entries: Vec<String> = extra_drives
        .iter()
        .enumerate()
        .map(|(i, drive)| {
            let dev_letter = (b'c' + i as u8) as char;
            let mut entry = format!("vd{}:{}", dev_letter, drive.mount_path().display());
            if let Some(sub_path) = drive.sub_path() {
                entry.push(':');
                entry.push_str(&sub_path.display().to_string());
            }
            entry
        })
        .collect();
    Some(format!("agentenv_drives={}", entries.join(",")))
}

fn relocate_warm_log(src: &Path, target: &Path) -> Result<()> {
    if src == target || !src.exists() {
        return Ok(());
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create firecracker log directory {}", parent.display()))?;
    }
    if target.exists() {
        fs::remove_file(target)
            .with_context(|| format!("remove existing firecracker log {}", target.display()))?;
    }
    match fs::rename(src, target) {
        Ok(()) => Ok(()),
        Err(err) if err.raw_os_error() == Some(libc::EXDEV) => {
            fs::copy(src, target).with_context(|| {
                format!(
                    "copy warm firecracker log {} to {} after cross-device rename failed",
                    src.display(),
                    target.display()
                )
            })?;
            match fs::remove_file(src) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err).with_context(|| {
                    format!(
                        "remove warm firecracker log {} after cross-device copy",
                        src.display()
                    )
                }),
            }
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "move warm firecracker log {} to {}",
                src.display(),
                target.display()
            )
        }),
    }
}

// ── Drop ─────────────────────────────────────────────────────────────────────

/// Ensure network resources are cleaned up when the sandbox is dropped.
/// This handles cases where stop() wasn't called (panic, early return, etc.)
///
/// For ublk, devices are managed by the daemon process, so we don't need to
/// kill any server process. The device ID is NOT recycled on abnormal drop
/// since proper delete (via the daemon) was not performed.
impl Drop for FirecrackerSandbox {
    fn drop(&mut self) {
        // Fire the best-effort stop notification (fire-and-forget, never
        // blocking drop) before releasing resources.
        self.custom_extension_hook_guard.take();
        // In daemon mode, ublk devices survive sandbox drop. The daemon will
        // clean them up on its own shutdown, or they can be explicitly deleted
        // via the orchestrator's stop() path.
        self.rootfs_runtime.take();
        self.mem_ublk_device.take();
        // In daemon mode, extra-drive devices survive sandbox drop. They are
        // explicitly deleted on the stop() path and otherwise cleaned up when
        // the daemon shuts down.
        self.extra_drive_runtimes.clear();
        if let Some(slot) = self.network_slot.take() {
            if let Err(e) = NetworkManager::global().release(slot) {
                warn!(error = %e, "failed to release network slot on drop");
            }
        }
    }
}

// ── Private helpers ──────────────────────────────────────────────────────────

impl FirecrackerSandbox {
    fn build(id: SandboxId, launch: LaunchMode) -> Result<Self> {
        // Keep persisted restore metadata with the runtime object regardless of
        // which resume constructor was used. In particular, committed snapshots,
        // persisted paused sandboxes, and fork children all enter through
        // `build` rather than the benchmark-oriented `from_snapshot_config`.
        let restore_working_set = match &launch {
            LaunchMode::Fresh(_) => None,
            LaunchMode::Resume(config) => config.restore_working_set.clone(),
        };
        let work_dir =
            create_firecracker_work_dir(launch.common().firecracker_work_base_dir.as_deref())?;
        let fc_instance = FirecrackerInstance::new(work_dir.path().to_path_buf());
        let runtime_policy = launch.common().runtime_policy;
        let current_network_policy = launch.common().network_policy.clone();
        let current_custom_extension_params = launch.common().custom_extension_params.clone();
        debug!(work_dir = %work_dir.path().display(), "sandbox work directory prepared");

        Ok(Self {
            id,
            runtime_policy,
            current_rootfs_virtual_size: match &launch {
                LaunchMode::Fresh(_) => None,
                LaunchMode::Resume(config) => config.common.rootfs_virtual_size,
            },
            launch,
            work_dir,
            fc_instance,
            network_slot: None,
            current_network_policy,
            current_custom_extension_params,
            envd_instance: None,
            rootfs_runtime: None,
            mem_ublk_device: None,
            profiling_mem_ublk_device: None,
            mem_snapshot_image_config_path: None,
            rootfs_image_config_path: None,
            extra_drive_runtimes: Vec::new(),
            live_snapshot_root: None,
            custom_extension_hook_guard: None,
            restore_working_set,
            profiling_mode: false,
        })
    }
    /// Apply a KVM pre-fault performance hint. HTTP status failures are
    /// non-blocking, but a Firecracker transport failure still blocks a resume.
    async fn try_prefault_restore(&self) -> Result<Option<PrefaultCompletionStats>> {
        self.try_prefault_restore_with_config(ConfigManager::global_config())
            .await
    }

    async fn try_prefault_restore_with_config(
        &self,
        config: &AppConfig,
    ) -> Result<Option<PrefaultCompletionStats>> {
        if !config.restore_prefault.enabled {
            if std::env::var_os("AENV_BENCH_DEBUG_PREFAULT").is_some() {
                eprintln!("prefault_diagnostic skipped=disabled");
            }
            return Ok(None);
        }
        if !super::prefault::prefault_supported(
            cfg!(target_arch = "x86_64"),
            config.virtualization_mode,
        ) {
            if std::env::var_os("AENV_BENCH_DEBUG_PREFAULT").is_some() {
                eprintln!(
                    "prefault_diagnostic skipped=unsupported_mode mode={}",
                    config.virtualization_mode
                );
            }
            debug!(
                virtualization_mode = %config.virtualization_mode,
                "skip restore pre-fault: unsupported architecture or virtualization mode"
            );
            return Ok(None);
        }
        let Some(working_set) = self.restore_working_set.as_ref() else {
            if std::env::var_os("AENV_BENCH_DEBUG_PREFAULT").is_some() {
                eprintln!("prefault_diagnostic skipped=missing_working_set");
            }
            debug!("skip restore pre-fault: snapshot has no working-set metadata");
            return Ok(None);
        };
        if working_set.ranges.is_empty() {
            if std::env::var_os("AENV_BENCH_DEBUG_PREFAULT").is_some() {
                eprintln!("prefault_diagnostic skipped=empty_working_set");
            }
            debug!("skip restore pre-fault: snapshot working-set is empty");
            return Ok(None);
        }
        let limits = GuestMemoryWorkingSetLimits {
            max_bytes: config.template_profiling.max_prefault_bytes,
            max_ranges: config.template_profiling.max_range_count,
            max_guest_memory_ratio_percent: config
                .template_profiling
                .max_guest_memory_ratio_percent,
        };
        let regions = match self.fc_instance.get_guest_memory_regions().await {
            Ok(regions) => regions,
            Err(error) if is_http_status_error(&error) => {
                if std::env::var_os("AENV_BENCH_DEBUG_PREFAULT").is_some() {
                    eprintln!(
                        "prefault_diagnostic guest_memory_regions_http_status_error error={error:#}"
                    );
                }
                warn!(error = ?error, "skip restore pre-fault: guest-memory-regions unavailable");
                return Ok(None);
            }
            Err(error) => return Err(error.context("get guest-memory regions before pre-fault")),
        };
        match build_prefault_plan(true, true, Some(working_set), &regions, limits) {
            PrefaultPlan::Request { ranges, bytes } => {
                let started = std::time::Instant::now();
                match self.fc_instance.pre_fault_memory(&ranges).await {
                    Ok(Some(api_stats)) => {
                        let stats =
                            PrefaultCompletionStats::from_api(api_stats, ranges.len(), bytes)
                                .context("validate Firecracker pre-fault completion stats")?;
                        debug!(
                            range_count = stats.range_count,
                            requested_bytes = stats.requested_bytes,
                            completed_bytes = stats.completed_bytes,
                            remaining_bytes = stats.remaining_bytes,
                            ioctl_count = stats.ioctl_count,
                            worker_count = stats.workers.len(),
                            worker_stats = ?stats.workers,
                            firecracker_wall_time_us = stats.wall_time_us,
                            elapsed = ?started.elapsed(),
                            "restore pre-fault completed before resume"
                        );
                        return Ok(Some(stats));
                    }
                    Ok(None) => {
                        if std::env::var_os("AENV_BENCH_DEBUG_PREFAULT").is_some() {
                            eprintln!(
                                "prefault_diagnostic legacy_empty_response ranges={} bytes={}",
                                ranges.len(),
                                bytes
                            );
                        }
                        warn!(
                            range_count = ranges.len(),
                            bytes,
                            "Firecracker pre-fault returned legacy empty success response; completion cannot be verified"
                        );
                    }
                    Err(error) if is_http_status_error(&error) => {
                        if std::env::var_os("AENV_BENCH_DEBUG_PREFAULT").is_some() {
                            eprintln!(
                                "prefault_diagnostic http_status_error ranges={} bytes={} error={error:#}",
                                ranges.len(),
                                bytes
                            );
                        }
                        warn!(error = ?error, range_count = ranges.len(), bytes, "restore pre-fault failed; resuming normally");
                    }
                    Err(error) => return Err(error.context("pre-fault guest memory before resume")),
                }
            }
            PrefaultPlan::Skip(reason) => {
                if std::env::var_os("AENV_BENCH_DEBUG_PREFAULT").is_some() {
                    eprintln!(
                        "prefault_diagnostic plan_skip reason={reason:?} working_set={:?} guest_regions={regions:?}",
                        working_set.ranges
                    );
                }
                debug!(?reason, "skip restore pre-fault");
            }
        }
        Ok(None)
    }

    #[tracing::instrument(skip(self, config))]
    async fn start_fresh(&mut self, config: FirecrackerSandboxConfig) -> Result<()> {
        let work_dir = self.work_dir.path();
        debug!(work_dir = %work_dir.display(), "starting fresh sandbox");

        let global_config = ConfigManager::global_config();

        // ── Tools drive: plain ext4, read-only, shared across sandboxes ──
        // Symlink work_dir/rootfs.ext4 → tools drive so Firecracker can use a
        // relative path inside the work directory.
        self.link_tools_drive(&config.common, work_dir)?;

        // ── User image: overlaybd via ublk, writable, per-sandbox ──
        let user_image_symlink = work_dir.join(USER_ROOTFS_DRIVE_PATH);
        let global_cfg_path = global_config.ublk.overlaybd.global_config_path.clone();
        let rootfs_image_config = config
            .common
            .rootfs_image_config
            .as_ref()
            .context("fresh sandbox rootfs image config is missing")?;
        let runtime_dir = work_dir.join("overlaybd");
        let runtime_device = UblkDeviceManager::global()
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: &rootfs_image_config.image_config_path,
                global_config: &global_cfg_path,
                runtime_dir: &runtime_dir,
                read_only: rootfs_image_config.read_only,
                runtime_upper_mode: rootfs_image_config.runtime_upper_mode,
                requested_virtual_size: config.common.rootfs_virtual_size,
                known_source_virtual_size: None,
                allow_shrink: config.common.rootfs_allow_shrink,
            })
            .await
            .context("create user image overlaybd runtime device")?;
        self.rootfs_image_config_path = Some(rootfs_image_config.image_config_path.clone());
        let device_path = runtime_device.device.device_path().to_path_buf();
        let symlink_result = std::os::unix::fs::symlink(&device_path, &user_image_symlink)
            .context("symlink user-rootfs to ublk device");
        if let Err(err) = symlink_result {
            if let Err(release_err) = UblkDeviceManager::global()
                .release_device(&runtime_device.device)
                .await
            {
                warn!(
                    error = %release_err,
                    "failed to release user image ublk device after symlink failure"
                );
            }
            return Err(err);
        }
        self.rootfs_runtime = Some(OverlaybdRuntimeHandle {
            device: runtime_device.device,
            image_config_path: runtime_device.image_config_path,
            actual_virtual_size: runtime_device.actual_virtual_size,
        });
        self.current_rootfs_virtual_size = Some(runtime_device.actual_virtual_size);

        // ── Boot args: init=/init (tools drive has init baked in) ──
        let mut boot_args = config.boot_args.clone();
        // Tools drive contains /init; ensure boot args include init=/init.
        let missing_explicit_init_arg = match boot_args.as_deref() {
            Some(args) => !args.split_whitespace().any(|arg| arg.starts_with("init=")),
            None => true,
        };
        if missing_explicit_init_arg {
            let init_arg = "init=/init";
            boot_args = Some(match boot_args.take() {
                Some(existing) => format!("{existing} {init_arg}"),
                None => init_arg.to_string(),
            });
        }

        // ── Extra drives ──
        let (extra_drive_attachments, extra_drive_runtimes) =
            if config.common.extra_drives.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                let overlaybd_global = global_config.ublk.overlaybd.global_config_path.clone();
                let runtime_upper_mode = global_config.ublk.overlaybd.runtime_upper_mode;
                let allow_shrink = global_config.ublk.overlaybd.allow_shrink;
                prepare_extra_drives(
                    &config.common.extra_drives,
                    &overlaybd_global,
                    self.work_dir.path(),
                    runtime_upper_mode,
                    ExtraDrivePrepareMode::Fresh { allow_shrink },
                )
                .await
                .context("prepare extra drives")?
                .into_parts()
            };
        self.extra_drive_runtimes = extra_drive_runtimes;

        // ── Boot args: extra drive mount points (agentenv_drives=vdc:...) ──
        if let Some(drives_arg) = build_drives_boot_arg(&config.common.extra_drives) {
            boot_args = Some(match boot_args.take() {
                Some(existing) => format!("{existing} {drives_arg}"),
                None => drives_arg,
            });
        }

        // ── Allocate network slot and create network infrastructure ──
        let slot = NetworkManager::global()
            .allocate_any()
            .context("Failed to allocate network slot")?;
        debug!(slot = slot.idx, "allocated network slot");
        let interaction_ip = slot.host_interaction_ip;

        // Add IP configuration to boot args for the VM.
        // Uses Slot::build_ip_boot_arg() to produce the kernel ip= parameter with a
        // valid DNS server IP in the 8th field. See that method for format details.
        let ip_config = slot.build_ip_boot_arg();
        let netns = slot.namespace_path();
        self.network_slot = Some(slot);
        self.network_slot
            .as_mut()
            .expect("network slot was just assigned")
            .set_egress_policy(config.common.network_policy.as_ref())
            .context("Failed to configure sandbox egress policy")?;
        boot_args = Some(match boot_args.take() {
            Some(existing) => format!("{existing} {ip_config}"),
            None => ip_config,
        });

        // ── Custom extension hook: start-fresh (may contribute extra boot args) ──
        if let Some(client) = CustomExtensionClient::global() {
            let mut guard = CustomExtensionHookGuard::new(client, self.id);
            let extra_boot_args = guard
                .start_fresh(
                    &netns.to_string_lossy(),
                    interaction_ip,
                    config.common.custom_extension_params.as_ref(),
                )
                .await?;
            self.custom_extension_hook_guard = Some(guard);
            if let Some(extra) = extra_boot_args.filter(|args| !args.trim().is_empty()) {
                boot_args = Some(match boot_args.take() {
                    Some(existing) => format!("{existing} {extra}"),
                    None => extra,
                });
            }
        }

        // ── Spawn Firecracker inside the network namespace so it can access tap0 ──
        let firecracker_binary = config.common.firecracker_binary.clone();
        let stdout_path = self.firecracker_stdout_path();
        let stderr_path = self.firecracker_stderr_path();

        self.fc_instance
            .spawn_with_netns(
                &firecracker_binary,
                Some(&stdout_path),
                Some(&stderr_path),
                Some(&netns),
            )
            .await?;

        let envd_base_url = format!(
            "http://{}:{}",
            interaction_ip, config.common.control_plane_port
        );
        self.envd_instance = Some(EnvdInstance::new(
            envd_base_url,
            config.common.envd_access_token.clone(),
        ));

        // ── Configure microVM: tools drive as rootfs + user image + extras ──
        self.fc_instance
            .wait_for_ready(
                self.runtime_policy.socket_timeout,
                self.runtime_policy.socket_poll_interval,
            )
            .await?;
        self.configure_microvm(&config, boot_args.as_deref(), &extra_drive_attachments)
            .await?;
        self.fc_instance.start().await?;
        debug!("fresh sandbox started");
        Ok(())
    }

    #[tracing::instrument(skip(self, config))]
    async fn start_resume(&mut self, config: FirecrackerSnapshotConfig) -> Result<()> {
        self.start_resume_with_options(config, true, None, None)
            .await
    }

    async fn start_resume_with_prefault(
        &mut self,
        config: FirecrackerSnapshotConfig,
        prefault_enabled: bool,
        max_prefault_bytes: Option<u64>,
        timings: Option<&mut SnapshotResumeTimings>,
    ) -> Result<()> {
        let mut runtime_config = ConfigManager::global_config().clone();
        runtime_config.restore_prefault.enabled = prefault_enabled;
        if let Some(max_prefault_bytes) = max_prefault_bytes {
            runtime_config.template_profiling.max_prefault_bytes = max_prefault_bytes;
        }
        self.start_resume_with_options(config, true, Some(&runtime_config), timings)
            .await
    }

    #[tracing::instrument(skip(self, config, prefault_config))]
    async fn start_resume_with_options(
        &mut self,
        config: FirecrackerSnapshotConfig,
        resume_vm: bool,
        prefault_config: Option<&AppConfig>,
        mut timings: Option<&mut SnapshotResumeTimings>,
    ) -> Result<()> {
        let restore_setup_started = timings.as_deref().map(|_| std::time::Instant::now());
        // NOTE: The virtio-balloon device is NOT configured here. Balloon state
        // is part of vm_state.bin and is restored automatically by Firecracker.
        // Snapshots taken before balloon support was added will simply not have
        // the device — free_page_reporting will be absent for those VMs, which
        // is acceptable during rollout.

        // Fail fast: memory restore requires a ublk device. Check before
        // allocating any resources (Firecracker process, network namespace, …).
        anyhow::ensure!(
            UblkDeviceManager::global().is_available(),
            "snapshot resume requires an available ublk daemon client \
             because memory restore uses a shared ublk device"
        );

        let global_config = ConfigManager::global_config();

        let rootfs_virtual_size = config
            .common
            .rootfs_virtual_size
            .context("snapshot rootfs virtual size is missing")?;
        let rootfs_image_config = config
            .common
            .rootfs_image_config
            .as_ref()
            .context("snapshot rootfs image config is missing")?;
        self.current_rootfs_virtual_size = Some(rootfs_virtual_size);

        if config.common.stdout_path.is_none() && config.common.stderr_path.is_none() {
            if let Some(warm) = FirecrackerPool::global().and_then(|pool| pool.try_acquire()) {
                let warm_dir = warm.work_dir.path();
                let warm_stdout = warm_stdout_path(warm_dir);
                let warm_stderr = warm_stderr_path(warm_dir);
                debug!(
                    slot = warm.slot.idx,
                    pool_work_dir = %warm_dir.display(),
                    "using warm firecracker from pool"
                );

                self.network_slot = Some(warm.slot);
                self.work_dir = warm.work_dir; // Update self.work_dir before relocating logs since the fallback log paths are relative to the work_dir.
                let _cold = std::mem::replace(&mut self.fc_instance, warm.fc_instance);
                if let Err(err) = relocate_warm_log(&warm_stdout, &self.firecracker_stdout_path()) {
                    warn!(error = %err, "failed to relocate warm firecracker stdout log");
                }
                if let Err(err) = relocate_warm_log(&warm_stderr, &self.firecracker_stderr_path()) {
                    warn!(error = %err, "failed to relocate warm firecracker stderr log");
                }
            }
        }

        let fc_cwd = self.work_dir.path();
        let vm_state_src = fs::canonicalize(&config.vm_state_path)
            .unwrap_or_else(|_| config.vm_state_path.clone());
        debug!(
            fc_cwd = %fc_cwd.display(),
            vm_state_path = %vm_state_src.display(),
            "starting sandbox from snapshot config"
        );

        // ── Tools drive: symlink rootfs.ext4 → tools drive (read-only, from snapshot config) ──
        self.link_tools_drive(&config.common, fc_cwd)?;

        // ── User image: restore overlaybd via ublk ──
        if config.common.ublk_config.is_some() {
            let user_image_symlink = fc_cwd.join(USER_ROOTFS_DRIVE_PATH);
            let global_cfg_path = global_config.ublk.overlaybd.global_config_path.clone();
            let runtime_dir = fc_cwd.join("overlaybd");
            let runtime_device = UblkDeviceManager::global()
                .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                    source_image_config: &rootfs_image_config.image_config_path,
                    global_config: &global_cfg_path,
                    runtime_dir: &runtime_dir,
                    read_only: rootfs_image_config.read_only,
                    runtime_upper_mode: rootfs_image_config.runtime_upper_mode,
                    requested_virtual_size: Some(rootfs_virtual_size),
                    known_source_virtual_size: Some(rootfs_virtual_size),
                    allow_shrink: false,
                })
                .await
                .context("create user image overlaybd runtime device for resume")?;
            self.rootfs_image_config_path = Some(rootfs_image_config.image_config_path.clone());
            let device_path = runtime_device.device.device_path().to_path_buf();
            let symlink_result = std::os::unix::fs::symlink(&device_path, &user_image_symlink)
                .context("symlink user-rootfs to ublk device for resume");
            if let Err(err) = symlink_result {
                if let Err(release_err) = UblkDeviceManager::global()
                    .release_device(&runtime_device.device)
                    .await
                {
                    warn!(
                        error = %release_err,
                        "failed to release resumed user image ublk device after symlink failure"
                    );
                }
                return Err(err);
            }
            self.rootfs_runtime = Some(OverlaybdRuntimeHandle {
                device: runtime_device.device,
                image_config_path: runtime_device.image_config_path,
                actual_virtual_size: runtime_device.actual_virtual_size,
            });
            self.current_rootfs_virtual_size = Some(runtime_device.actual_virtual_size);
        }

        // ── Extra drives ──
        self.prepare_snapshot_backing_drives(&config.common.extra_drives)
            .await
            .context("prepare snapshot-backed extra drives for resume")?;

        // ── Network + Firecracker spawn ──
        let needs_socket_wait = self.network_slot.is_none();
        let interaction_ip = if let Some(slot) = self.network_slot.as_ref() {
            slot.host_interaction_ip
        } else {
            let slot = NetworkManager::global()
                .allocate_any()
                .context("Failed to allocate network slot for resume")?;
            debug!(slot = slot.idx, "allocated network slot for resume");
            let netns = slot.namespace_path();
            let interaction_ip = slot.host_interaction_ip;
            self.network_slot = Some(slot);

            let firecracker_binary = config.common.firecracker_binary.clone();
            let stdout_path = self.firecracker_stdout_path();
            let stderr_path = self.firecracker_stderr_path();

            self.fc_instance
                .spawn_with_netns(
                    &firecracker_binary,
                    Some(&stdout_path),
                    Some(&stderr_path),
                    Some(&netns),
                )
                .await?;

            interaction_ip
        };
        if let Some(slot) = self.network_slot.as_mut() {
            slot.set_egress_policy(config.common.network_policy.as_ref())
                .context("Failed to configure sandbox egress policy for resume")?;
        }

        // A throwaway profiler must not report a customer-visible resume or
        // create a matching stop hook: profiling is internal observation only.
        if !self.profiling_mode {
            if let Some(client) = CustomExtensionClient::global() {
                let slot = self
                    .network_slot
                    .as_ref()
                    .context("network slot must be allocated before start-resume hook")?;
                let mut guard = CustomExtensionHookGuard::new(client, self.id);
                guard
                    .start_resume(
                        &slot.namespace_path().to_string_lossy(),
                        slot.host_interaction_ip,
                        config.common.custom_extension_params.as_ref(),
                    )
                    .await?;
                self.custom_extension_hook_guard = Some(guard);
            }
        }

        let envd_base_url = format!(
            "http://{}:{}",
            interaction_ip, config.common.control_plane_port
        );
        self.envd_instance = Some(EnvdInstance::new(
            envd_base_url,
            config.common.envd_access_token.clone(),
        ));

        let mem_global_config = global_config
            .memory_snapshot
            .overlaybd_global_config_path
            .clone();
        let mem_spec = UblkCreateSpec::Overlaybd {
            image_config: config.mem_overlaybd_config.image_config_path.clone(),
            global_config: mem_global_config,
        };
        let mem_device_path = if self.profiling_mode {
            let mem_device = UblkDeviceManager::global()
                .create_unshared_mem(&mem_spec, config.mem_virtual_size)
                .await
                .context("create exclusive profiler memory ublk device")?;
            let path = mem_device.device_path().to_path_buf();
            self.profiling_mem_ublk_device = Some(mem_device);
            path
        } else {
            let mem_device = UblkDeviceManager::global()
                .get_or_create_shared_mem(&mem_spec, config.mem_virtual_size)
                .await
                .context("create or reuse shared memory ublk device for resume")?;
            let path = mem_device.device_path().to_path_buf();
            self.mem_ublk_device = Some(mem_device);
            path
        };
        self.mem_snapshot_image_config_path =
            Some(config.mem_overlaybd_config.image_config_path.clone());

        if needs_socket_wait {
            self.fc_instance
                .wait_for_ready(
                    self.runtime_policy.socket_timeout,
                    self.runtime_policy.socket_poll_interval,
                )
                .await?;
        }

        self.configure_logger(&config.common).await?;

        if let (Some(timings), Some(started)) = (timings.as_deref_mut(), restore_setup_started) {
            timings.restore_setup = started.elapsed();
        }
        let snapshot_load_started = timings.as_deref().map(|_| std::time::Instant::now());

        // Override the network interface to use the new tap0 in our namespace
        let network_overrides = [("eth0", "tap0")];
        self.fc_instance
            .load_snapshot_file(
                &vm_state_src,
                &mem_device_path,
                &network_overrides,
                false,
                config.common.track_dirty_pages,
            )
            .await?;

        let mmds_metadata = self.mmds_metadata(&config.common);
        self.fc_instance.set_mmds(&mmds_metadata).await?;

        // A restored snapshot inherits whatever limiter was active when it was
        // paused, so reconcile against the node's current config while the VM is
        // still loaded-but-paused — before resume() lets the guest issue I/O.
        // Both buckets are always overwritten (configured or unlimited) so an
        // inherited dimension the current config leaves unset is cleared rather
        // than left unchanged.
        let reconciled = reconcile_disk_rate_limiter(&config.common.disk_rate_limit)?;
        self.fc_instance
            .patch_drive_rate_limiter(USER_ROOTFS_DRIVE_ID, reconciled)
            .await
            .context("reconcile disk rate limiter on snapshot resume")?;

        if let (Some(timings), Some(started)) = (timings.as_deref_mut(), snapshot_load_started) {
            timings.snapshot_load = started.elapsed();
        }

        if resume_vm {
            let prefault_started = timings.as_deref().map(|_| std::time::Instant::now());
            let prefault_stats = match prefault_config {
                Some(config) => self.try_prefault_restore_with_config(config).await?,
                None => self.try_prefault_restore().await?,
            };
            if let (Some(timings), Some(started)) = (timings.as_deref_mut(), prefault_started) {
                timings.prefault = started.elapsed();
                timings.prefault_stats = prefault_stats;
            }
            let firecracker_resume_started = timings.as_deref().map(|_| std::time::Instant::now());
            self.fc_instance.resume().await?;
            if let (Some(timings), Some(started)) =
                (timings.as_deref_mut(), firecracker_resume_started)
            {
                timings.firecracker_resume = started.elapsed();
            }
            debug!("sandbox restored from snapshot config");
        } else {
            debug!("snapshot loaded paused for dedicated profiling");
        }
        Ok(())
    }

    /// Enable Firecracker logging when `firecracker_log_level` is configured.
    ///
    /// Must be called pre-boot (and before snapshot load). When no log level is
    /// set, this is a no-op so the default behaviour is unchanged.
    async fn configure_logger(&self, common: &FirecrackerCommonConfig) -> Result<()> {
        let Some(level) = common.firecracker_log_level.as_deref() else {
            return Ok(());
        };
        if level.trim().is_empty() {
            return Ok(());
        }
        let log_path = self.firecracker_log_path();
        if let Some(parent) = log_path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("create firecracker log directory {}", parent.display())
            })?;
        }
        self.fc_instance.set_logger(&log_path, level).await?;
        debug!(
            log_path = %log_path.display(),
            level,
            "firecracker logger enabled"
        );
        Ok(())
    }

    fn link_tools_drive(&self, common: &FirecrackerCommonConfig, work_dir: &Path) -> Result<()> {
        let tools_drive_path = common
            .resolved_tools_drive_path(ConfigManager::global_config())
            .with_context(|| {
                format!(
                    "resolve tools drive version '{}' for sandbox {}",
                    common.tools_drive_version, self.id
                )
            })?;
        let tools_drive_path = fs::canonicalize(&tools_drive_path).with_context(|| {
            format!(
                "open tools drive version '{}' at {} for sandbox {}",
                common.tools_drive_version,
                tools_drive_path.display(),
                self.id
            )
        })?;
        let firecracker_path = work_dir.join(ROOTFS_DRIVE_PATH);

        std::os::unix::fs::symlink(&tools_drive_path, &firecracker_path).with_context(|| {
            format!(
                "link tools drive version '{}' from {} to {} for sandbox {}",
                common.tools_drive_version,
                tools_drive_path.display(),
                firecracker_path.display(),
                self.id
            )
        })?;
        debug!(
            sandbox_id = %self.id,
            tools_drive_version = %common.tools_drive_version,
            source = %tools_drive_path.display(),
            destination = %firecracker_path.display(),
            "linked tools drive into Firecracker work directory"
        );
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn configure_microvm(
        &self,
        config: &FirecrackerSandboxConfig,
        boot_args: Option<&str>,
        extra_drive_attachments: &[DriveMount],
    ) -> Result<()> {
        self.configure_logger(&config.common).await?;

        self.fc_instance
            .set_machine_config(
                config.mem_size_mib,
                config.vcpu_count,
                false,
                config.common.track_dirty_pages,
            )
            .await?;

        if let Some(cpu_json) = config.common.cpu_config_json.as_deref() {
            if !cpu_json.is_empty() {
                self.fc_instance.set_cpu_config(cpu_json).await?;
            }
        }

        let kernel_image =
            fs::canonicalize(&config.kernel_image).unwrap_or_else(|_| config.kernel_image.clone());
        self.fc_instance
            .set_boot_source(&kernel_image, None, boot_args)
            .await?;

        // Drive 0 (/dev/vda): tools drive as root device, always read-only.
        self.fc_instance
            .add_drive(
                ROOTFS_DRIVE_ID,
                Path::new(ROOTFS_DRIVE_PATH),
                true,
                true,
                false,
                IoEngine::Sync,
                None,
            )
            .await
            .with_context(|| {
                format!(
                    "attach tools drive version '{}' as {} for fresh sandbox {}",
                    config.common.tools_drive_version, ROOTFS_DRIVE_PATH, self.id
                )
            })?;

        // Drive 1 (/dev/vdb): user image, writable. The disk rate limiter is
        // applied here as pre-boot drive config (rather than a post-start PATCH)
        // so throttling is in force the instant the guest starts issuing I/O.
        self.fc_instance
            .add_drive(
                USER_ROOTFS_DRIVE_ID,
                Path::new(USER_ROOTFS_DRIVE_PATH),
                false,
                false,
                true,
                IoEngine::Async,
                build_disk_rate_limiter(&config.common.disk_rate_limit)?,
            )
            .await?;

        // Drive 2+ (/dev/vdc...): user extra drives.
        self.configure_extra_drives(extra_drive_attachments).await?;

        if self.network_slot.is_some() {
            // Network interface.
            self.fc_instance
                .add_network_interface("eth0", None, "tap0".to_string(), None, None)
                .await
                .context("Failed to add network interface to microVM")?;

            // MMDS
            self.fc_instance
                .set_mmds_config("eth0")
                .await
                .context("Failed to set MMDS network configuration")?;
            let mmds_metadata = self.mmds_metadata(&config.common);
            self.fc_instance.set_mmds(&mmds_metadata).await?;
        }

        // ── Balloon: enable free page reporting ──
        // Paired with DAMON reclaim (kernel boot args): DAMON reclaims cold
        // pagecache pages inside the guest, and the balloon device reports the
        // resulting free pages to the host VMM so it can release physical memory.
        self.fc_instance.set_balloon().await?;

        Ok(())
    }

    async fn configure_extra_drives(&self, extra_drive_attachments: &[DriveMount]) -> Result<()> {
        for drive in extra_drive_attachments {
            self.fc_instance
                .add_drive(
                    &drive.drive_id,
                    &drive.attachment_path,
                    false,
                    drive.read_only,
                    true,
                    IoEngine::Async,
                    None,
                )
                .await
                .with_context(|| format!("Failed to add extra drive {}", drive.drive_id))?;
        }

        Ok(())
    }

    async fn prepare_snapshot_backing_drives(&mut self, extra_drives: &[ExtraDrive]) -> Result<()> {
        if extra_drives.is_empty() {
            self.extra_drive_runtimes.clear();
            return Ok(());
        }

        let global_config = ConfigManager::global_config();
        let ublk_config = &global_config.ublk;
        let overlaybd_global = ublk_config.overlaybd.global_config_path.clone();
        let runtime_upper_mode = ublk_config.overlaybd.runtime_upper_mode;
        let prepared_extra_drives = prepare_extra_drives(
            extra_drives,
            &overlaybd_global,
            self.work_dir.path(),
            runtime_upper_mode,
            ExtraDrivePrepareMode::Resume,
        )
        .await?;
        let (_attachments_already_in_snapshot, extra_drive_runtimes) =
            prepared_extra_drives.into_parts();
        self.extra_drive_runtimes = extra_drive_runtimes;
        Ok(())
    }

    async fn snapshot_extra_drives(&self, snapshot_dir: &Path) -> Result<Vec<ExtraDrive>> {
        let extra_drives = &self.launch.common().extra_drives;
        if extra_drives.is_empty() {
            return Ok(Vec::new());
        }
        if extra_drives.len() != self.extra_drive_runtimes.len() {
            bail!(
                "extra drive bookkeeping mismatch: {} configured drives but {} prepared devices",
                extra_drives.len(),
                self.extra_drive_runtimes.len()
            );
        }

        let mut snapped = Vec::with_capacity(extra_drives.len());
        for (drive, runtime) in extra_drives.iter().zip(self.extra_drive_runtimes.iter()) {
            let snapshot_image_config_path = restack_snapshot_overlaybd_device(
                &runtime.device,
                drive.read_only(),
                &runtime.image_config_path,
                &snapshot_dir.join("drives").join(drive.drive_id()),
                "drive",
            )
            .await
            .with_context(|| format!("snapshot extra drive '{}'", drive.drive_id()))?;
            snapped.push(
                drive
                    .with_image_config_path(snapshot_image_config_path)
                    .try_with_virtual_size(runtime.actual_virtual_size)?,
            );
        }

        Ok(snapped)
    }

    fn runtime_image_config_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        if let Some(rootfs_runtime) = &self.rootfs_runtime {
            paths.push(rootfs_runtime.image_config_path.clone());
        }
        paths.extend(
            self.extra_drive_runtimes
                .iter()
                .map(|runtime| runtime.image_config_path.clone()),
        );
        paths
    }
}

// ── LaunchMode ───────────────────────────────────────────────────────────────

enum LaunchMode {
    Fresh(FirecrackerSandboxConfig),
    Resume(FirecrackerSnapshotConfig),
}

impl LaunchMode {
    fn common(&self) -> &FirecrackerCommonConfig {
        match self {
            LaunchMode::Fresh(config) => &config.common,
            LaunchMode::Resume(config) => &config.common,
        }
    }

    fn managed_snapshot_root(&self) -> Option<Arc<PersistentSnapshotRootGuard>> {
        match self {
            LaunchMode::Fresh(_) => None,
            LaunchMode::Resume(config) => config.managed_snapshot_root.clone(),
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            LaunchMode::Fresh(config) => config.validate(),
            LaunchMode::Resume(config) => config.validate(),
        }
    }
}

/// Copy a file using reflink (CoW) if available, falling back to a full copy.
async fn copy_cow(src: &Path, dst: &Path) -> Result<()> {
    let src = src.to_path_buf();
    let dst = dst.to_path_buf();
    // File copying can be multi-GB on snapshot paths, so keep the whole
    // reflink-or-copy fallback on a blocking thread.
    tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new("cp");
        if cfg!(target_os = "linux") {
            cmd.arg("--reflink=auto");
        }
        let status = cmd.arg(&src).arg(&dst).status();
        if let Ok(s) = status {
            if s.success() {
                return Ok(());
            }
        }
        tracing::warn!(?src, ?dst, "reflink unavailable, falling back to full copy");
        fs::copy(&src, &dst)?;
        Ok(())
    })
    .await
    .context("copy_cow task failed")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::ToolsConfig;
    use crate::sandbox::{SandboxAccessTokenGenerator, SandboxExecutor};
    use crate::snapshot::{CommittedSnapshot, RunnableSnapshot, SnapshotRecord};
    use std::collections::HashMap;
    use std::convert::Infallible;

    use http_body_util::{BodyExt, Full};
    use hyper::body::{Bytes, Incoming};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use tokio::net::UnixListener;

    fn fresh_config() -> FirecrackerSandboxConfig {
        FirecrackerSandboxConfig::new(
            "firecracker".into(),
            "vmlinux.bin".into(),
            "0.1.0".to_string(),
            "user-image.json".into(),
        )
    }

    fn overlaybd_config() -> FirecrackerSandboxConfig {
        let mut config = fresh_config();
        config.common.ublk_config = Some(crate::sandbox::ublk::UblkConfig::overlaybd(
            "overlaybd-image.json".into(),
            false,
        ));
        config
    }

    fn prefault_enabled_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.restore_prefault.enabled = true;
        config
    }

    fn rate_limit_cfg() -> crate::cfg::DiskRateLimitConfig {
        crate::cfg::DiskRateLimitConfig {
            enabled: true,
            bandwidth_bytes_per_sec: 0,
            bandwidth_burst_bytes: 0,
            iops: 0,
            iops_burst: 0,
        }
    }

    #[test]
    fn rate_limiter_disabled_returns_none() {
        let mut cfg = rate_limit_cfg();
        cfg.enabled = false;
        cfg.bandwidth_bytes_per_sec = 104_857_600;
        assert!(build_disk_rate_limiter(&cfg).unwrap().is_none());
    }

    #[test]
    fn mincore_swap_total_is_observational_not_a_gate() {
        assert_eq!(
            mincore_swap_total_kib("MemTotal: 1024 kB\nSwapTotal: 0 kB\n").unwrap(),
            0
        );
        assert_eq!(
            mincore_swap_total_kib("SwapTotal: 4096 kB\n").unwrap(),
            4096
        );
        assert!(mincore_swap_total_kib("MemTotal: 1024 kB\n").is_err());
    }

    #[test]
    fn resident_range_stats_distinguish_baseline_final_and_newly_resident_bytes() -> Result<()> {
        let baseline = [
            ResidentMemoryRange {
                image_offset: 0,
                length: 100,
            },
            ResidentMemoryRange {
                image_offset: 300,
                length: 100,
            },
        ];
        let final_ranges = [
            ResidentMemoryRange {
                image_offset: 0,
                length: 200,
            },
            ResidentMemoryRange {
                image_offset: 300,
                length: 100,
            },
            ResidentMemoryRange {
                image_offset: 500,
                length: 50,
            },
        ];

        assert_eq!(
            resident_range_stats(&baseline)?,
            ResidentRangeStats {
                range_count: 2,
                bytes: 200,
            }
        );
        assert_eq!(
            resident_range_stats(&final_ranges)?,
            ResidentRangeStats {
                range_count: 3,
                bytes: 350,
            }
        );
        let newly_resident = newly_resident_ranges(&baseline, &final_ranges)?;
        assert_eq!(
            newly_resident,
            vec![
                ResidentMemoryRange {
                    image_offset: 100,
                    length: 100,
                },
                ResidentMemoryRange {
                    image_offset: 500,
                    length: 50,
                },
            ]
        );
        assert_eq!(
            resident_range_stats(&newly_resident)?,
            ResidentRangeStats {
                range_count: 2,
                bytes: 150,
            }
        );
        Ok(())
    }

    #[test]
    fn mincore_stage_reports_initial_total_and_successive_delta() -> Result<()> {
        let baseline = [ResidentMemoryRange {
            image_offset: 0,
            length: 4096,
        }];
        assert_eq!(
            snapshot_mincore_stage("snapshot_loaded_paused", None, &baseline)?,
            SnapshotMincoreStage {
                phase: "snapshot_loaded_paused",
                total_ranges: 1,
                total_bytes: 4096,
                newly_resident_ranges: 0,
                newly_resident_bytes: 0,
            }
        );

        let current = [
            ResidentMemoryRange {
                image_offset: 0,
                length: 4096,
            },
            ResidentMemoryRange {
                image_offset: 8192,
                length: 4096,
            },
        ];
        assert_eq!(
            snapshot_mincore_stage("firecracker_resumed", Some(&baseline), &current)?,
            SnapshotMincoreStage {
                phase: "firecracker_resumed",
                total_ranges: 2,
                total_bytes: 8192,
                newly_resident_ranges: 1,
                newly_resident_bytes: 4096,
            }
        );
        Ok(())
    }

    #[test]
    fn prefault_candidate_converts_baseline_delta_to_gpa() -> Result<()> {
        let baseline = [ResidentMemoryRange {
            image_offset: 0,
            length: 4096,
        }];
        let current = [ResidentMemoryRange {
            image_offset: 0,
            length: 8192,
        }];
        let candidate = snapshot_prefault_candidate(
            "firecracker_resumed",
            &baseline,
            &current,
            &[super::super::mincore_tracking::GuestMemoryImageRegion {
                image_offset: 0,
                guest_phys_addr: 0x1000,
                size: 8192,
                page_size: 4096,
            }],
            GuestMemoryWorkingSetLimits {
                max_bytes: 8192,
                max_ranges: 2,
                max_guest_memory_ratio_percent: 100,
            },
        )?;
        assert_eq!(candidate.phase, "firecracker_resumed");
        assert_eq!(candidate.working_set.ranges.len(), 1);
        assert_eq!(candidate.working_set.ranges[0].gpa, 0x2000);
        assert_eq!(candidate.working_set.ranges[0].size, 4096);
        Ok(())
    }

    #[test]
    fn rate_limiter_enabled_but_all_zero_returns_none() {
        assert!(build_disk_rate_limiter(&rate_limit_cfg())
            .unwrap()
            .is_none());
    }

    #[test]
    fn rate_limiter_bandwidth_size_equals_per_second_rate() {
        let mut cfg = rate_limit_cfg();
        cfg.bandwidth_bytes_per_sec = 104_857_600; // 100 MB/s
        cfg.bandwidth_burst_bytes = 10_485_760;
        let rl = build_disk_rate_limiter(&cfg)
            .unwrap()
            .expect("limiter present");
        let bw = rl.bandwidth.expect("bandwidth bucket");
        // With refill pinned to 1000 ms, bucket size == sustained bytes/sec.
        assert_eq!(bw.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(bw.size, 104_857_600);
        assert_eq!(bw.one_time_burst, Some(10_485_760));
        assert!(rl.ops.is_none());
    }

    #[test]
    fn rate_limiter_iops_bucket_populated() {
        let mut cfg = rate_limit_cfg();
        cfg.iops = 3000;
        cfg.iops_burst = 500;
        let rl = build_disk_rate_limiter(&cfg)
            .unwrap()
            .expect("limiter present");
        let ops = rl.ops.expect("ops bucket");
        assert_eq!(ops.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(ops.size, 3000);
        assert_eq!(ops.one_time_burst, Some(500));
        assert!(rl.bandwidth.is_none());
    }

    #[test]
    fn rate_limiter_rejects_values_beyond_i64_range() {
        let mut cfg = rate_limit_cfg();
        cfg.bandwidth_bytes_per_sec = u64::MAX;
        assert!(build_disk_rate_limiter(&cfg).is_err());
    }

    #[test]
    fn reconcile_disabled_makes_both_buckets_disabled() {
        // Firecracker treats an absent bucket in a PATCH as "leave unchanged", so
        // clearing an inherited limiter requires overwriting BOTH buckets with a
        // disabled (size == 0) bucket rather than sending an empty RateLimiter.
        let mut cfg = rate_limit_cfg();
        cfg.enabled = false;
        cfg.bandwidth_bytes_per_sec = 100 << 20;
        cfg.iops = 3000;
        let rl = reconcile_disk_rate_limiter(&cfg).unwrap();
        let bw = rl.bandwidth.expect("bandwidth bucket present");
        let ops = rl.ops.expect("ops bucket present");
        assert_eq!(bw.size, 0);
        assert_eq!(ops.size, 0);
    }

    #[test]
    fn reconcile_bandwidth_only_clears_inherited_iops() {
        // Enabled with bandwidth but no iops: bandwidth gets its configured
        // bucket, while the unset iops dimension is overwritten with a disabled
        // bucket so a snapshot-inherited IOPS limit does not survive the resume.
        let mut cfg = rate_limit_cfg();
        cfg.enabled = true;
        cfg.bandwidth_bytes_per_sec = 100 << 20;
        cfg.iops = 0;
        let rl = reconcile_disk_rate_limiter(&cfg).unwrap();
        let bw = rl.bandwidth.expect("bandwidth bucket present");
        let ops = rl.ops.expect("ops bucket present");
        assert_eq!(bw.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(bw.size, 100 << 20);
        assert_eq!(ops.size, 0);
    }

    #[test]
    fn reconcile_iops_only_clears_inherited_bandwidth() {
        let mut cfg = rate_limit_cfg();
        cfg.enabled = true;
        cfg.bandwidth_bytes_per_sec = 0;
        cfg.iops = 3000;
        let rl = reconcile_disk_rate_limiter(&cfg).unwrap();
        let bw = rl.bandwidth.expect("bandwidth bucket present");
        let ops = rl.ops.expect("ops bucket present");
        assert_eq!(bw.size, 0);
        assert_eq!(ops.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(ops.size, 3000);
    }

    #[test]
    fn reconcile_both_dimensions_use_configured_buckets() {
        let mut cfg = rate_limit_cfg();
        cfg.enabled = true;
        cfg.bandwidth_bytes_per_sec = 100 << 20;
        cfg.iops = 3000;
        let rl = reconcile_disk_rate_limiter(&cfg).unwrap();
        let bw = rl.bandwidth.expect("bandwidth bucket present");
        let ops = rl.ops.expect("ops bucket present");
        assert_eq!(bw.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(bw.size, 100 << 20);
        assert_eq!(ops.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(ops.size, 3000);
    }

    #[test]
    fn paused_state_image_cache_paths_use_snapshot_artifact_config() {
        let mut common = fresh_config().common;
        common
            .rootfs_image_config
            .as_mut()
            .expect("fresh config has a rootfs")
            .image_config_path = "snapshot/rootfs/image.json".into();
        let state = FirecrackerPausedState::new(FirecrackerSnapshotConfig {
            common,
            vm_state_path: "snapshot/vm_state.bin".into(),
            mem_overlaybd_config: OverlaybdConfig {
                image_config_path: "snapshot/mem_image.json".into(),
                read_only: true,
                runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
            },
            mem_virtual_size: 4096,
            restore_working_set: None,
            managed_snapshot_root: None,
        });

        assert_eq!(
            state.runtime_artifacts(),
            RuntimeArtifactSet::from_overlaybd_image_configs(vec![PathBuf::from(
                "snapshot/rootfs/image.json"
            )])
        );
    }

    #[test]
    fn snapshot_config_runtime_identity_replaces_source_auth() -> Result<()> {
        let source_id = SandboxId::new();
        let child_id = SandboxId::new();
        let generator = SandboxAccessTokenGenerator::new("fork-test-seed")?;
        let source_token = generator.generate(source_id);
        let child_token = generator.generate(child_id);
        let mut common = fresh_config().common;
        common.mmds_metadata =
            Some(MmdsMetadata::new(source_id, "snapshot").with_access_token(Some(&source_token)));
        common.envd_access_token = Some(source_token.clone());
        let snapshot = FirecrackerSnapshotConfig {
            common,
            vm_state_path: "snapshot/vm_state.bin".into(),
            mem_overlaybd_config: OverlaybdConfig {
                image_config_path: "snapshot/mem_image.json".into(),
                read_only: true,
                runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
            },
            mem_virtual_size: 4096,
            restore_working_set: None,
            managed_snapshot_root: None,
        };

        let child = FirecrackerSandbox::from_snapshot_config_with_override(
            snapshot,
            child_id,
            Some(child_token.clone()),
        )?;
        let common = child.launch.common();
        let metadata = common.mmds_metadata.as_ref().expect("child MMDS metadata");
        let expected =
            MmdsMetadata::new(child_id, "snapshot").with_access_token(Some(&child_token));

        assert_eq!(child.id, child_id);
        assert_eq!(common.envd_access_token.as_ref(), Some(&child_token));
        assert_eq!(metadata.sandbox_id, child_id.to_string());
        assert_eq!(metadata.access_token_hash, expected.access_token_hash);
        assert_ne!(
            metadata.access_token_hash,
            MmdsMetadata::new(source_id, "snapshot")
                .with_access_token(Some(&source_token))
                .access_token_hash
        );
        Ok(())
    }

    #[test]
    fn paused_state_without_tools_drive_version_remains_readable_but_not_resumable() -> Result<()> {
        let temp = TempDir::new()?;
        let vm_state_path = temp.path().join("vm-state.bin");
        let mem_image_path = temp.path().join("mem-image.json");
        let rootfs_image_path = temp.path().join("rootfs-image.json");
        fs::write(&vm_state_path, b"state")?;
        fs::write(&mem_image_path, b"{}")?;
        fs::write(&rootfs_image_path, b"{}")?;

        let mut common = fresh_config().common;
        common.rootfs_virtual_size = Some(4096);
        common
            .rootfs_image_config
            .as_mut()
            .expect("fresh config has a rootfs")
            .image_config_path = rootfs_image_path;
        let mut value = serde_json::to_value(FirecrackerSnapshotConfig {
            common,
            vm_state_path,
            mem_overlaybd_config: OverlaybdConfig {
                image_config_path: mem_image_path,
                read_only: true,
                runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
            },
            mem_virtual_size: 4096,
            restore_working_set: None,
            managed_snapshot_root: None,
        })?;
        let common = value["common"]
            .as_object_mut()
            .expect("common config must be an object");
        common.remove("tools_drive_version");
        common.insert(
            "tools_drive_path".to_string(),
            serde_json::Value::String("/legacy/node/tools.ext4".to_string()),
        );

        let state = FirecrackerPausedState::decode(PathBuf::new(), value)?;

        assert!(state
            .snapshot_config()
            .common
            .tools_drive_version
            .is_empty());
        let err = state
            .snapshot_config()
            .validate()
            .expect_err("legacy paused state must not resume without a tools drive version");
        assert!(err
            .to_string()
            .contains("sandbox state does not record a tools drive version"));
        Ok(())
    }

    #[test]
    fn executor_requires_running_envd_instance() -> Result<()> {
        let sandbox = FirecrackerSandbox::new(fresh_config())?;
        let err = match SandboxExecutor::executor(&sandbox) {
            Ok(_) => panic!("envd should be missing"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("Sandbox is not running"));
        Ok(())
    }

    #[tokio::test]
    async fn wait_for_ready_requires_envd_instance() -> Result<()> {
        let sandbox = FirecrackerSandbox::new(fresh_config())?;
        let err = sandbox
            .wait_for_ready()
            .await
            .expect_err("envd should be missing");
        assert!(err.to_string().contains("envd instance not initialized"));
        Ok(())
    }

    #[tokio::test]
    async fn stop_without_process_clears_envd_instance() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(fresh_config())?;
        sandbox.envd_instance = Some(EnvdInstance::new(
            format!(
                "http://127.0.0.1:{}",
                ToolsConfig::default().control_plane_port
            ),
            None,
        ));

        sandbox.stop().await?;

        assert!(sandbox.envd_instance.is_none());
        Ok(())
    }

    #[test]
    fn host_interaction_ip_reflects_network_slot_state() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(fresh_config())?;
        assert_eq!(sandbox.host_interaction_ip(), None);

        let manager = NetworkManager::new(false, 0, 0);
        let slot = manager.allocate_test_slot()?;
        let expected = slot.host_interaction_ip;
        sandbox.network_slot = Some(slot);

        assert_eq!(sandbox.host_interaction_ip(), Some(expected));
        let slot = sandbox
            .network_slot
            .take()
            .expect("network slot should still be present");
        manager.release(slot)?;
        Ok(())
    }

    #[test]
    fn work_rootfs_path_uses_overlaybd_symlink_path() -> Result<()> {
        let sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        assert_eq!(
            sandbox.work_rootfs_path(),
            sandbox.work_dir.path().join("user-rootfs")
        );
        assert!(sandbox.uses_overlaybd_ublk());
        Ok(())
    }

    #[test]
    fn build_drives_boot_arg_uses_custom_mount_path() -> Result<()> {
        let drives = vec![
            ExtraDrive::try_new_overlaybd_with_mount_path(
                "data",
                "/tmp/data-image.json",
                true,
                "/workspace/data",
                None::<std::path::PathBuf>,
            )?,
            ExtraDrive::try_new_overlaybd("logs", "/tmp/logs-image.json", true)?,
        ];

        assert_eq!(
            build_drives_boot_arg(&drives),
            Some("agentenv_drives=vdc:/workspace/data,vdd:/mnt/logs".to_string())
        );
        Ok(())
    }

    #[test]
    fn build_drives_boot_arg_includes_sub_path() -> Result<()> {
        let drives = vec![ExtraDrive::try_new_overlaybd_with_mount_path(
            "data",
            "/tmp/data-image.json",
            true,
            "/mnt/data",
            Some("sub/dir"),
        )?];

        assert_eq!(
            build_drives_boot_arg(&drives),
            Some("agentenv_drives=vdc:/mnt/data:sub/dir".to_string())
        );
        Ok(())
    }

    #[test]
    fn from_snapshot_merges_launch_env_over_snapshot_env() -> Result<()> {
        let mut snapshot_env_vars = HashMap::new();
        snapshot_env_vars.insert("FROM_SNAPSHOT".to_string(), "true".to_string());
        snapshot_env_vars.insert("SHARED_KEY".to_string(), "snapshot".to_string());
        let record = SnapshotRecord {
            id: crate::snapshot::SnapshotId::generate(),
            ..SnapshotRecord::mock_ready(CommittedSnapshot {
                context: crate::snapshot::CommandContext::new(snapshot_env_vars, "/"),
                ..CommittedSnapshot::mock()
            })
        };
        let snapshot = RunnableSnapshot::from_test_manifest(record, Vec::new());
        let launch_config = SandboxLaunchConfig {
            env_vars: Some(HashMap::from([
                ("FROM_LAUNCH".to_string(), "true".to_string()),
                ("SHARED_KEY".to_string(), "launch".to_string()),
            ])),
            sandbox_id: SandboxId::new(),
            snapshot_id: "tpl-test".to_string(),
            network: None,
            extra_mmds: serde_json::Map::new(),
            custom_extension_params: None,
            envd_access_token: None,
        };

        let snapshot_config =
            FirecrackerSandbox::snapshot_config_for_launch(&snapshot, &launch_config)
                .expect("snapshot launch config should merge env vars");
        let env_vars = snapshot_config
            .common
            .env_vars
            .as_ref()
            .expect("env vars should be present after merge");
        assert_eq!(env_vars.get("FROM_SNAPSHOT"), Some(&"true".to_string()));
        assert_eq!(env_vars.get("FROM_LAUNCH"), Some(&"true".to_string()));
        assert_eq!(env_vars.get("SHARED_KEY"), Some(&"launch".to_string()));
        Ok(())
    }

    /// Build custom extension params from a JSON object literal.
    fn params(value: serde_json::Value) -> CustomExtensionParams {
        value
            .as_object()
            .expect("params must be a JSON object")
            .clone()
    }

    #[test]
    fn from_snapshot_custom_extension_params_launch_overrides_snapshot() -> Result<()> {
        let record = SnapshotRecord {
            id: crate::snapshot::SnapshotId::generate(),
            ..SnapshotRecord::mock_ready(CommittedSnapshot {
                custom_extension_params: Some(params(serde_json::json!({"from": "snapshot"}))),
                ..CommittedSnapshot::mock()
            })
        };
        let snapshot = RunnableSnapshot::from_test_manifest(record, Vec::new());

        // Launch-provided value wins over the snapshot-persisted one.
        let launch_config = SandboxLaunchConfig {
            sandbox_id: SandboxId::new(),
            snapshot_id: "tpl-test".to_string(),
            custom_extension_params: Some(params(serde_json::json!({"from": "launch"}))),
            ..SandboxLaunchConfig::default()
        };
        let snapshot_config =
            FirecrackerSandbox::snapshot_config_for_launch(&snapshot, &launch_config)?;
        assert_eq!(
            snapshot_config.common.custom_extension_params,
            Some(params(serde_json::json!({"from": "launch"})))
        );

        // Without a launch value, the snapshot's persisted config is inherited.
        let launch_config = SandboxLaunchConfig {
            sandbox_id: SandboxId::new(),
            snapshot_id: "tpl-test".to_string(),
            ..SandboxLaunchConfig::default()
        };
        let snapshot_config =
            FirecrackerSandbox::snapshot_config_for_launch(&snapshot, &launch_config)?;
        assert_eq!(
            snapshot_config.common.custom_extension_params,
            Some(params(serde_json::json!({"from": "snapshot"})))
        );
        Ok(())
    }

    #[test]
    fn snapshot_rootfs_virtual_size_requires_cached_value() -> Result<()> {
        let sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        let err = sandbox
            .snapshot_rootfs_virtual_size()
            .expect_err("missing cached rootfs virtual size should fail");
        assert!(err
            .to_string()
            .contains("ensure start() was called before pause() or snapshot"));
        Ok(())
    }

    #[test]
    fn profiling_snapshot_config_disables_dirty_tracking_without_mutating_normal_restore(
    ) -> Result<()> {
        let mut common = fresh_config().common;
        common.track_dirty_pages = true;
        let snapshot = FirecrackerSnapshotConfig {
            common,
            vm_state_path: "vm_state.bin".into(),
            mem_overlaybd_config: OverlaybdConfig {
                image_config_path: "mem_image.json".into(),
                read_only: true,
                runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
            },
            mem_virtual_size: 4096,
            restore_working_set: None,
            managed_snapshot_root: None,
        };
        let profiler = FirecrackerSandbox::from_profiling_snapshot_config(&snapshot)?;
        assert!(profiler.profiling_mode);
        assert!(profiler.profiling_mem_ublk_device.is_none());
        assert!(snapshot.common.track_dirty_pages);
        match &profiler.launch {
            LaunchMode::Resume(config) => assert!(!config.common.track_dirty_pages),
            LaunchMode::Fresh(_) => panic!("profiler must resume a snapshot"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn restore_prefault_uses_gpa_regions_before_sending_ranges() -> Result<()> {
        let work_dir = TempDir::new()?;
        let snapshot = RunnableSnapshot::from_test_manifest_with_working_set(
            SnapshotRecord::mock_ready(CommittedSnapshot::mock()),
            GuestMemoryWorkingSet::new(vec![super::super::manifest::GuestMemoryRange {
                gpa: 4 * 1024 * 1024 * 1024,
                size: 4096,
            }]),
        );
        let launch_config = SandboxLaunchConfig {
            sandbox_id: SandboxId::new(),
            snapshot_id: "persisted-prefault-test".to_string(),
            ..SandboxLaunchConfig::default()
        };
        let sandbox = FirecrackerSandbox::from_snapshot_with_test_work_dir(
            &snapshot,
            &launch_config,
            work_dir.path(),
        )?;
        let listener = UnixListener::bind(sandbox.fc_instance.api_socket_path())?;

        let server = tokio::spawn(async move {
            for expected_path in ["/vm/guest-memory-regions", "/vm/pre-fault-memory"] {
                let (stream, _) = listener
                    .accept()
                    .await
                    .expect("accept fake Firecracker request");
                http1::Builder::new()
                    .keep_alive(false)
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request: Request<Incoming>| async move {
                            assert_eq!(request.uri().path(), expected_path);
                            let (status, body) = match expected_path {
                                "/vm/guest-memory-regions" => {
                                    assert_eq!(request.method(), Method::GET);
                                    (
                                        StatusCode::OK,
                                        r#"[{"base_host_virt_addr":0,"guest_phys_addr":0,"size":4096,"offset":0,"page_size":4096},{"base_host_virt_addr":4096,"guest_phys_addr":4294967296,"size":8192,"offset":4096,"page_size":4096}]"#,
                                    )
                                }
                                "/vm/pre-fault-memory" => {
                                    assert_eq!(request.method(), Method::PUT);
                                    let body = request
                                        .collect()
                                        .await
                                        .expect("collect pre-fault request")
                                        .to_bytes();
                                    let request: serde_json::Value =
                                        serde_json::from_slice(&body).expect("decode pre-fault request");
                                    assert_eq!(
                                        request,
                                        serde_json::json!({"ranges": [{"gpa": 4294967296_i64, "size": 4096_i64}]}),
                                    );
                                    (StatusCode::NO_CONTENT, "")
                                }
                                _ => unreachable!(),
                            };
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::from(body)))
                                    .expect("build fake Firecracker response"),
                            )
                        }),
                    )
                    .await
                    .expect("serve fake Firecracker request");
            }
        });

        sandbox
            .try_prefault_restore_with_config(&prefault_enabled_config())
            .await?;
        server.await.expect("join fake Firecracker server");
        Ok(())
    }

    #[tokio::test]
    async fn runnable_snapshot_pvm_skips_guest_memory_api() -> Result<()> {
        let work_dir = TempDir::new()?;
        let snapshot = RunnableSnapshot::from_test_manifest_with_working_set(
            SnapshotRecord::mock_ready(CommittedSnapshot::mock()),
            GuestMemoryWorkingSet::new(vec![super::super::manifest::GuestMemoryRange {
                gpa: 0,
                size: 4096,
            }]),
        );
        let launch_config = SandboxLaunchConfig {
            sandbox_id: SandboxId::new(),
            snapshot_id: "pvm-prefault-gate-test".to_string(),
            ..SandboxLaunchConfig::default()
        };
        let sandbox = FirecrackerSandbox::from_snapshot_with_test_work_dir(
            &snapshot,
            &launch_config,
            work_dir.path(),
        )?;
        let listener = UnixListener::bind(sandbox.fc_instance.api_socket_path())?;
        let mut config = prefault_enabled_config();
        config.virtualization_mode = crate::virtualization::VirtualizationMode::Pvm;

        sandbox.try_prefault_restore_with_config(&config).await?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "PVM must not query guest-memory regions or submit a pre-fault request"
        );
        Ok(())
    }

    #[tokio::test]
    async fn restore_prefault_endpoint_failure_and_empty_metadata_are_non_blocking() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(fresh_config())?;
        sandbox.restore_working_set = Some(GuestMemoryWorkingSet::new(vec![
            super::super::manifest::GuestMemoryRange { gpa: 0, size: 4096 },
        ]));
        let listener = UnixListener::bind(sandbox.fc_instance.api_socket_path())?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("accept guest-memory-regions request");
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|request: Request<Incoming>| async move {
                        assert_eq!(request.uri().path(), "/vm/guest-memory-regions");
                        assert_eq!(request.method(), Method::GET);
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Full::new(Bytes::new()))
                                .expect("build unavailable response"),
                        )
                    }),
                )
                .await
                .expect("serve unavailable response");
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                    .await
                    .is_err(),
                "endpoint failure must not send a pre-fault request"
            );
        });

        sandbox
            .try_prefault_restore_with_config(&prefault_enabled_config())
            .await?;
        server
            .await
            .expect("join unavailable fake Firecracker server");

        let mut empty_sandbox = FirecrackerSandbox::new(fresh_config())?;
        empty_sandbox.restore_working_set = Some(GuestMemoryWorkingSet::new(Vec::new()));
        let listener = UnixListener::bind(empty_sandbox.fc_instance.api_socket_path())?;
        empty_sandbox
            .try_prefault_restore_with_config(&prefault_enabled_config())
            .await?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "empty metadata must not call any Firecracker pre-fault API"
        );
        Ok(())
    }

    #[tokio::test]
    async fn restore_prefault_transport_failure_blocks_resume() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(fresh_config())?;
        sandbox.restore_working_set = Some(GuestMemoryWorkingSet::new(vec![
            super::super::manifest::GuestMemoryRange { gpa: 0, size: 4096 },
        ]));

        let error = sandbox
            .try_prefault_restore_with_config(&prefault_enabled_config())
            .await
            .expect_err("missing Firecracker socket must block resume");
        assert!(
            error
                .to_string()
                .contains("get guest-memory regions before pre-fault"),
            "unexpected transport error: {error:#}"
        );
        Ok(())
    }
}
