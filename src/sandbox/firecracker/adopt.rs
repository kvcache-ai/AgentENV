//! Carrying a running sandbox across a restart of the server process.
//!
//! A sandbox's Firecracker process, network namespace and ublk devices do not
//! depend on the server process that created them. [`FirecrackerRuntimeState`]
//! records where they are. After a restart, [`reconcile_host_runtime`] decides
//! which recorded sandboxes can still be adopted and removes everything else
//! the previous process left on the host, and [`FirecrackerSandbox::adopt`]
//! rebuilds the handle of one of them.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use firecracker_client::models::instance_info::State as InstanceState;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;
use uvm_ublk_daemon::{DeviceRole, DeviceSpec};

use super::{managed_snapshot_base, FirecrackerSandbox, LaunchMode};
use crate::cfg::AppConfig;
use crate::sandbox::access::EnvdAccessToken;
use crate::sandbox::custom_extension::{
    CustomExtensionClient, CustomExtensionHookGuard, CustomExtensionParams,
};
use crate::sandbox::firecracker::config::{PersistentSnapshotRootGuard, SandboxWorkDir};
use crate::sandbox::firecracker::instance::ProcessIdentity;
use crate::sandbox::firecracker::overlaybd_snapshot::restack_in_flight;
use crate::sandbox::firecracker::resources::SandboxResources;
use crate::sandbox::firecracker::FirecrackerInstance;
use crate::sandbox::network::{NetworkManager, SandboxNetworkPolicy};
use crate::sandbox::ublk::{
    OverlaybdRuntimeHandle, SharedReadOnlyDevice, UblkDeviceManager, UblkHandover,
};
use crate::types::SandboxId;

const WORK_DIR_PREFIX: &str = "agentenv-fc-";
const ORPHAN_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Guard of the managed snapshot root a kept sandbox reads layers from.
type KeptSnapshotRoot = Option<Arc<PersistentSnapshotRootGuard>>;

/// The sandboxes [`reconcile_host_runtime`] kept, until each is adopted.
/// A fork source and its children share a snapshot root: every guard is taken
/// before any adoption can fail, so that a failed one does not remove the
/// root of the others.
static KEPT_FOR_ADOPTION: Mutex<Option<HashMap<SandboxId, KeptSnapshotRoot>>> = Mutex::new(None);

/// Where the host resources of a running sandbox are, and the launch
/// configuration its later captures build on.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FirecrackerRuntimeState {
    launch: LaunchMode,
    work_dir: PathBuf,
    process: ProcessIdentity,
    network_slot: u32,
    network_namespace: String,
    network_policy: Option<SandboxNetworkPolicy>,
    custom_extension_params: Option<CustomExtensionParams>,
    /// Instance id of the delivered start hook; its stop hook is still owed.
    custom_extension_instance_id: Option<Uuid>,
    rootfs: Option<RuntimeDeviceState>,
    extra_drives: Vec<RuntimeDeviceState>,
    tools_device: Option<DeviceSpec>,
    mem_device: Option<DeviceSpec>,
    rootfs_virtual_size: Option<u64>,
    /// Managed snapshot root inherited at launch, or already created.
    snapshot_root: Option<PathBuf>,
}

/// A writable runtime device; its spec names the runtime image config.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeDeviceState {
    spec: DeviceSpec,
    actual_virtual_size: u64,
}

impl RuntimeDeviceState {
    fn of(runtime: &OverlaybdRuntimeHandle) -> Result<Self> {
        let spec = UblkDeviceManager::global()
            .device_spec(&runtime.device)
            .with_context(|| format!("ublk device {} is not held", runtime.device.dev_id()))?;
        Ok(Self {
            spec,
            actual_virtual_size: runtime.actual_virtual_size,
        })
    }

    fn adopt(self) -> Result<OverlaybdRuntimeHandle> {
        Ok(OverlaybdRuntimeHandle {
            device: UblkDeviceManager::global().adopt_device(&self.spec)?,
            image_config_path: self.spec.image_config,
            actual_virtual_size: self.actual_virtual_size,
        })
    }
}

impl FirecrackerRuntimeState {
    fn writable_devices(&self) -> impl Iterator<Item = &RuntimeDeviceState> {
        self.rootfs.iter().chain(&self.extra_drives)
    }

    fn devices(&self) -> impl Iterator<Item = &DeviceSpec> {
        self.writable_devices()
            .map(|device| &device.spec)
            .chain(&self.tools_device)
            .chain(&self.mem_device)
    }

    /// The managed snapshot root this sandbox's devices may read layers from.
    fn snapshot_root(&self, id: SandboxId) -> Option<PathBuf> {
        self.snapshot_root.clone().or_else(|| {
            // Created by a capture taken after the state was recorded.
            let root = managed_snapshot_base().join(id.to_string());
            root.exists().then_some(root)
        })
    }

    /// Whether the sandbox can still be adopted: its Firecracker process is
    /// alive, and no restack of its devices was cut short.
    fn is_adoptable(&self) -> bool {
        self.process.is_running_in(&self.work_dir)
            && !self
                .writable_devices()
                .any(|device| restack_in_flight(&device.spec.image_config))
    }
}

impl FirecrackerSandbox {
    /// Record what a later server process needs to [`adopt`](Self::adopt)
    /// this running sandbox.
    pub(crate) fn runtime_state(&self) -> Result<FirecrackerRuntimeState> {
        let slot = self
            .resources
            .network_slot
            .as_ref()
            .context("sandbox has no network slot")?;
        if self.resources.mem_dedicated_device.is_some() {
            bail!("a startup-pack recording sandbox cannot be adopted");
        }
        let shared_spec = |device: &SharedReadOnlyDevice| {
            device
                .recovery_spec()
                .context("shared ublk device is not held")
        };
        Ok(FirecrackerRuntimeState {
            launch: self.launch.clone(),
            work_dir: self.work_dir.path().to_path_buf(),
            process: self.fc_instance.process_identity()?,
            network_slot: slot.idx,
            network_namespace: slot.namespace_id.clone(),
            network_policy: self.current_network_policy.clone(),
            custom_extension_params: self.current_custom_extension_params.clone(),
            custom_extension_instance_id: self
                .resources
                .custom_extension_hook_guard
                .as_ref()
                .and_then(CustomExtensionHookGuard::sandbox_instance_id),
            rootfs: self
                .resources
                .rootfs_runtime
                .as_ref()
                .map(RuntimeDeviceState::of)
                .transpose()?,
            extra_drives: self
                .resources
                .extra_drive_runtimes
                .iter()
                .map(RuntimeDeviceState::of)
                .collect::<Result<_>>()?,
            tools_device: self
                .resources
                .tools_ublk_device
                .as_ref()
                .map(shared_spec)
                .transpose()?,
            mem_device: self
                .resources
                .mem_ublk_device
                .as_ref()
                .map(shared_spec)
                .transpose()?,
            rootfs_virtual_size: self.current_rootfs_virtual_size,
            snapshot_root: self
                .live_snapshot_root
                .clone()
                .or_else(|| self.launch.managed_snapshot_root())
                .map(|root| root.path().to_path_buf()),
        })
    }

    /// Rebuild the handle of a sandbox that kept running while the server
    /// process which started it went away.
    ///
    /// Nothing is started: the Firecracker process, network namespace and
    /// ublk devices recorded in `state` are taken over, and the custom
    /// extension start hook is not delivered again. A VM that a capture left
    /// paused is resumed. If any part cannot be taken over, the rest is torn
    /// down and the sandbox is gone. Only a sandbox [`reconcile_host_runtime`]
    /// kept can be adopted.
    pub(crate) async fn adopt(
        id: SandboxId,
        state: FirecrackerRuntimeState,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Self> {
        let kept = KEPT_FOR_ADOPTION
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|kept| kept.remove(&id));
        let Some(snapshot_root) = kept else {
            // Reconciliation already removed what it left on the host; only
            // the custom extension stop hook is still owed.
            if let (Some(client), Some(instance_id)) = (
                CustomExtensionClient::global(),
                state.custom_extension_instance_id,
            ) {
                drop(CustomExtensionHookGuard::adopted(client, id, instance_id));
            }
            bail!("firecracker process did not outlive the previous server process");
        };
        let FirecrackerRuntimeState {
            mut launch,
            work_dir,
            process,
            network_slot,
            network_namespace,
            network_policy,
            custom_extension_params,
            custom_extension_instance_id,
            rootfs,
            extra_drives,
            tools_device,
            mem_device,
            rootfs_virtual_size,
            snapshot_root: _,
        } = state;
        launch.common_mut().envd_access_token = envd_access_token;

        let mut sandbox = Self {
            id,
            runtime_policy: launch.common().runtime_policy,
            fc_instance: FirecrackerInstance::new(work_dir.clone()),
            launch,
            work_dir: SandboxWorkDir::Adopted(work_dir),
            resources: SandboxResources::default(),
            current_network_policy: network_policy,
            current_custom_extension_params: custom_extension_params,
            envd_instance: None,
            frozen_volume_mounts: Vec::new(),
            initial_guest_drive_mounts: Vec::new(),
            current_rootfs_virtual_size: rootfs_virtual_size,
            live_snapshot_root: snapshot_root,
        };

        // Take over everything before checking anything, so that a failed
        // adoption releases all of it.
        let mut errors = Vec::new();
        let manager = UblkDeviceManager::global();
        let resources = &mut sandbox.resources;
        if let Some(rootfs) = rootfs {
            resources.rootfs_runtime = rootfs.adopt().map_err(|e| errors.push(e)).ok();
        }
        for drive in extra_drives {
            if let Ok(runtime) = drive.adopt().map_err(|e| errors.push(e)) {
                resources.extra_drive_runtimes.push(runtime);
            }
        }
        if let Some(spec) = tools_device {
            resources.tools_ublk_device = manager
                .adopt_shared_readonly(&spec)
                .map_err(|e| errors.push(e))
                .ok();
        }
        if let Some(spec) = mem_device {
            resources.mem_ublk_device = manager
                .adopt_shared_readonly(&spec)
                .map_err(|e| errors.push(e))
                .ok();
        }
        if let (Some(client), Some(instance_id)) = (
            CustomExtensionClient::global(),
            custom_extension_instance_id,
        ) {
            resources.custom_extension_hook_guard =
                Some(CustomExtensionHookGuard::adopted(client, id, instance_id));
        }
        if let Err(error) = sandbox.fc_instance.adopt(process) {
            errors.push(error);
        }
        match NetworkManager::global()
            .adopt_configured(
                network_slot,
                network_namespace,
                sandbox.current_network_policy.clone(),
            )
            .await
        {
            Ok(lease) => {
                let interaction_ip = lease.host_interaction_ip;
                sandbox.resources.network_slot = Some(lease);
                let common = sandbox.launch.common().clone();
                sandbox.init_envd_instance(interaction_ip, &common);
            }
            Err(error) => errors.push(error),
        }

        let result = match errors.into_iter().next() {
            Some(error) => Err(error),
            None => sandbox.ensure_vm_running().await,
        };
        if let Err(error) = result {
            sandbox.abandon().await;
            return Err(error);
        }
        debug!("adopted firecracker sandbox");
        Ok(sandbox)
    }

    /// Check that Firecracker answers, and resume a VM that the previous
    /// server process paused for a capture it did not finish.
    async fn ensure_vm_running(&self) -> Result<()> {
        let timeout = self.runtime_policy.socket_timeout;
        let info = tokio::time::timeout(timeout, self.fc_instance.instance_info())
            .await
            .context("firecracker did not answer in time")??;
        match info.state {
            InstanceState::Running => Ok(()),
            InstanceState::Paused => {
                info!("resuming VM left paused by an unfinished capture");
                self.fc_instance.resume().await
            }
            InstanceState::NotStarted => bail!("firecracker VM was never started"),
        }
    }

    /// Tear down a sandbox that could not be adopted.
    async fn abandon(mut self) {
        let timeout = self.runtime_policy.socket_timeout;
        if let Err(error) = self.fc_instance.stop(timeout).await {
            warn!(%error, "failed to stop firecracker of a sandbox that could not be adopted");
        }
        if let Some(envd) = self.envd_instance.take() {
            envd.invalidate();
        }
        // Its namespace is in an unknown state: destroy it instead of
        // returning it to the warm pool.
        let lease = self.resources.network_slot.take();
        if let Err(error) = self.resources.release(timeout).await {
            warn!(%error, "failed to release resources of a sandbox that could not be adopted");
        }
        if let Some(lease) = lease {
            if let Err(error) = lease.destroy(false) {
                warn!(%error, "failed to destroy network slot of a sandbox that could not be adopted");
            }
        }
    }
}

/// Bring the host in line with the recorded runtime states before anything
/// is started, and report the ublk devices to take over.
///
/// `states` are the runtime states of the sandboxes that were running when
/// the previous server process stopped. A sandbox is kept for adoption while
/// its Firecracker process is alive. Everything else the previous process
/// left behind is removed: Firecracker processes (sandboxes that were still
/// starting, warm-pool entries), work directories, managed snapshot roots and
/// network namespaces. Processes and directories are only looked for under a
/// configured `[firecracker].work_dir`, which this server owns.
///
/// Returns the devices of the kept sandboxes. Every other device of the
/// previous process is deleted when the ublk daemon takes these over.
pub fn reconcile_host_runtime(
    config: &AppConfig,
    states: &[(SandboxId, serde_json::Value)],
) -> Result<UblkHandover> {
    let mut kept = Vec::new();
    for (sandbox_id, state) in states {
        match FirecrackerRuntimeState::deserialize(state) {
            Ok(state) if state.is_adoptable() => kept.push((*sandbox_id, state)),
            Ok(_) => {
                info!(%sandbox_id, "recorded sandbox did not outlive the previous server process")
            }
            Err(error) => warn!(%sandbox_id, %error, "ignoring unreadable sandbox runtime state"),
        }
    }
    let kept_for_adoption: HashMap<SandboxId, KeptSnapshotRoot> = kept
        .iter()
        .map(|(sandbox_id, state)| {
            let root = state.snapshot_root(*sandbox_id);
            (*sandbox_id, root.map(PersistentSnapshotRootGuard::adopt))
        })
        .collect();
    let kept_snapshot_roots: HashSet<PathBuf> = kept_for_adoption
        .values()
        .flatten()
        .map(|root| root.path().to_path_buf())
        .collect();
    *KEPT_FOR_ADOPTION.lock().unwrap() = Some(kept_for_adoption);

    if let Some(work_base) = config.firecracker.work_dir.as_deref() {
        let kept_pids: HashSet<i32> = kept.iter().map(|(_, state)| state.process.pid).collect();
        kill_orphaned_processes(work_base, &kept_pids);

        let kept_work_dirs: HashSet<&Path> = kept
            .iter()
            .map(|(_, state)| state.work_dir.as_path())
            .collect();
        remove_orphaned_dirs(work_base, &|path| {
            let is_work_dir = path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(WORK_DIR_PREFIX));
            is_work_dir && !kept_work_dirs.contains(path)
        });
        remove_orphaned_dirs(&managed_snapshot_base(), &|path| {
            !kept_snapshot_roots.contains(path)
        });
    }

    let kept_namespaces = kept
        .iter()
        .map(|(_, state)| state.network_namespace.clone())
        .collect();
    crate::sandbox::network::prepare_runtime(&config.runtime_path, &kept_namespaces)
        .context("prepare AENV network namespace runtime directory")?;

    // Sandboxes share read-only devices; each is handed over once, held once.
    let mut devices = HashMap::new();
    for spec in kept.iter().flat_map(|(_, state)| state.devices()) {
        let mut spec = spec.clone();
        if let DeviceRole::Shared { refcount } = &mut spec.role {
            *refcount = 1;
        }
        devices.insert(spec.dev_id, spec);
    }
    info!(
        recorded = states.len(),
        kept = kept.len(),
        devices = devices.len(),
        "reconciled host runtime with recorded sandboxes"
    );
    Ok(UblkHandover::new(devices.into_values().collect()))
}

/// Kill every process running in a work directory under `work_base`, except
/// `kept_pids`, and wait for them to exit so that their devices and
/// namespaces are free.
fn kill_orphaned_processes(work_base: &Path, kept_pids: &HashSet<i32>) {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return;
    };
    let orphans: Vec<Pid> = entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<i32>().ok())
        .filter(|pid| !kept_pids.contains(pid))
        .filter(|pid| {
            std::fs::read_link(format!("/proc/{pid}/cwd")).is_ok_and(|cwd| {
                cwd.parent() == Some(work_base)
                    && cwd
                        .file_name()
                        .is_some_and(|name| name.to_string_lossy().starts_with(WORK_DIR_PREFIX))
            })
        })
        .map(Pid::from_raw)
        .collect();
    for pid in &orphans {
        info!(%pid, "killing process left by the previous server process");
        if let Err(error) = kill(*pid, Signal::SIGKILL) {
            warn!(%pid, %error, "failed to kill orphaned process");
        }
    }
    // A killed process stays in /proc as a zombie until its parent reaps it;
    // it has released its resources by then.
    let exited = |pid: Pid| {
        std::fs::read_to_string(format!("/proc/{pid}/stat")).map_or(true, |stat| {
            stat.rsplit_once(')')
                .is_some_and(|(_, fields)| fields.trim_start().starts_with('Z'))
        })
    };
    let deadline = Instant::now() + ORPHAN_EXIT_TIMEOUT;
    for pid in orphans {
        while !exited(pid) {
            if Instant::now() >= deadline {
                warn!(%pid, "orphaned process did not exit in time");
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn remove_orphaned_dirs(parent: &Path, is_orphan: &dyn Fn(&Path) -> bool) {
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for path in entries.flatten().map(|entry| entry.path()) {
        if !path.is_dir() || !is_orphan(&path) {
            continue;
        }
        info!(path = %path.display(), "removing directory left by the previous server process");
        if let Err(error) = std::fs::remove_dir_all(&path) {
            warn!(path = %path.display(), %error, "failed to remove orphaned directory");
        }
    }
}
