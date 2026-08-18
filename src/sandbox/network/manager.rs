use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use anyhow::{anyhow, Result};
#[cfg(test)]
use index_set::BitSet;
use index_set::{slot_count, AtomicBitSet, SharedBitSet};
use ipnetwork::Ipv4Network;
use nix::libc;
use tracing::{debug, trace, warn};
use warm_pool::{PoolConfig, PoolMaintenanceAction, WarmPool};

use super::iptables_util::{apply_iptables_commands, IptablesRestoreCommand, OpenFailurePolicy};
use super::{NetworkAddressPlan, NetworkError, Slot, HOST_VETH_PREFIX, MAX_SLOTS};

const CONFLICT_SAMPLE_LIMIT: usize = 5;
const ERR_SHUTTING_DOWN: &str = "Network manager is shutting down";

static HOST_IPTABLES_INSTALLED: AtomicBool = AtomicBool::new(false);

static MANAGER: OnceLock<NetworkManager> = OnceLock::new();

extern "C" fn network_manager_exit_hook() {
    let _ = std::panic::catch_unwind(|| {
        if let Some(manager) = NetworkManager::global_if_initialized() {
            if let Err(err) = manager.shutdown_inner(true) {
                warn!(error = %err, "network manager shutdown on process exit failed");
            }
        }
    });
}

fn register_process_exit_hook(handler: extern "C" fn()) -> i32 {
    // SAFETY: `handler` uses C ABI and `atexit` accepts callbacks with signature `extern "C" fn()`.
    unsafe { libc::atexit(handler) }
}

struct NetworkManagerConfig {
    pool: PoolConfig,
    address_plan: NetworkAddressPlan,
    netns_dir: PathBuf,
}

fn get_network_manager_config() -> NetworkManagerConfig {
    let cfg = crate::cfg::ConfigManager::global_config();
    NetworkManagerConfig {
        pool: cfg.network_pool_config(),
        address_plan: NetworkAddressPlan::from_config(&cfg.network)
            .expect("validated network.internal config should produce an address plan"),
        netns_dir: cfg.runtime_path.join("netns"),
    }
}

pub(crate) struct NetworkManager {
    /// Bitmap tracking allocated slots.
    allocated: AtomicBitSet<{ slot_count::from_bits(MAX_SLOTS) }>,

    /// Warm slots ready for immediate reuse.
    pool: WarmPool<Slot>,

    /// Configured address plan for newly allocated network slots.
    address_plan: NetworkAddressPlan,

    /// Directory containing persistent namespace bind mounts.
    netns_dir: PathBuf,

    /// Rejects new allocations once shutdown cleanup starts.
    shutting_down: AtomicBool,
}

impl NetworkManager {
    /// Global network manager for slot allocation across all sandboxes.
    pub fn global() -> &'static Self {
        let manager = MANAGER.get_or_init(|| {
            // Register a process exit hook to clean up network resources on shutdown.
            // This is necessary because the global manager is a static singleton and
            // does not run Drop handlers at exit.
            // Without this hook, this would result in a slot resource leak during testing,
            // benchmarking, and other scenarios that do not follow the graceful shutdown path.
            let rc = register_process_exit_hook(network_manager_exit_hook);
            if rc != 0 {
                warn!(
                    code = rc,
                    "failed to register network manager process-exit hook"
                );
            }

            Self::with_config(get_network_manager_config())
        });

        manager.ensure_pool_maintenance_worker_started();
        manager
    }

    /// Returns the global network manager only if it has already been initialized.
    ///
    /// This is useful for shutdown paths that must avoid side effects (like host
    /// inspection logs) when networking was never used in the current process.
    pub fn global_if_initialized() -> Option<&'static Self> {
        MANAGER.get()
    }

    #[cfg(test)]
    pub(crate) fn new(
        maintenance_enabled: bool,
        low_watermark: usize,
        high_watermark: usize,
    ) -> Self {
        Self::with_config(NetworkManagerConfig {
            pool: PoolConfig {
                low_watermark,
                high_watermark,
                maintenance_enabled,
                startup_prewarm: true,
            },
            address_plan: NetworkAddressPlan::default(),
            netns_dir: std::env::temp_dir().join("aenv-network-tests/netns"),
        })
    }

    fn with_config(config: NetworkManagerConfig) -> Self {
        let manager = Self {
            allocated: AtomicBitSet::new(),
            pool: WarmPool::new(config.pool),
            address_plan: config.address_plan,
            netns_dir: config.netns_dir,
            shutting_down: AtomicBool::new(false),
        };

        // Reserve slot 0 (invalid for IP addresses)
        manager.allocated.insert(0);
        let existing_slots = manager.reserve_existing_host_veth_slots();
        manager.log_external_conflicts(&existing_slots);

        if let Err(e) = manager.install_global_host_iptables() {
            warn!(error = %e, "failed to install global host iptables rules during network manager init");
        }

        manager
    }

    fn shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    fn ensure_pool_maintenance_worker_started(&'static self) {
        self.pool.start_maintenance_worker(move || {
            if let Err(err) = self.run_pool_maintenance_cycle() {
                warn!(error = %err, "network pool maintenance cycle failed");
            }
        });
    }

    /// Allocates a network slot with the given index atomically.
    #[cfg(test)]
    pub fn allocate_slot(&self, idx: u32) -> Result<Slot> {
        if idx == 0 || idx as usize >= MAX_SLOTS {
            return Err(anyhow!(
                "Slot index {} out of range (max {})",
                idx,
                MAX_SLOTS - 1
            ));
        }

        match self.allocated.insert(idx as usize) {
            // insert returns true when the bit WAS already set (duplicate)
            Some(false) => Ok(Slot::new(idx, self.address_plan, self.netns_dir.clone())
                .expect("Slot index just validated")),
            Some(true) => Err(anyhow!("Slot {} already allocated", idx)),
            None => Err(anyhow!("Slot index {} out of range", idx)),
        }
    }

    #[cfg(test)]
    pub(crate) fn allocate_test_slot(&self) -> Result<Slot> {
        for idx in 1..MAX_SLOTS {
            if !self.allocated.has(idx) {
                return self.allocate_slot(idx as u32);
            }
        }
        Err(anyhow!("No available test slots"))
    }

    /// Release only the bitmap bit for a slot index.
    fn release_slot_bit(&self, idx: u32) -> Result<()> {
        if idx == 0 || idx as usize >= MAX_SLOTS {
            return Err(anyhow!("Slot index {} out of range", idx));
        }

        match self.allocated.remove(idx as usize) {
            // remove returns true when the bit WAS set (successful release)
            Some(true) => Ok(()),
            Some(false) => Err(anyhow!("Slot {} not allocated", idx)),
            None => Err(anyhow!("Slot index {} out of range", idx)),
        }
    }

    fn cleanup_slot_and_release_bit(&self, slot: Slot) -> Result<()> {
        self.cleanup_slot_and_release_bit_inner(slot, false)
    }

    fn cleanup_slot_and_release_bit_inner(&self, slot: Slot, sync_cleanup: bool) -> Result<()> {
        let idx = slot.idx;
        let cleanup_result = slot.cleanup(sync_cleanup);
        let bitset_result = self.release_slot_bit(idx);
        match (cleanup_result, bitset_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(e), Ok(())) => Err(e.into()),
            (Ok(()), Err(e)) => Err(e),
            (Err(ce), Err(be)) => {
                warn!(cleanup_error = %ce, "network slot cleanup failed alongside bitset release error");
                Err(be)
            }
        }
    }

    pub(crate) fn cleanup_allocated_slot(&self, slot: Slot, sync_cleanup: bool) -> Result<()> {
        self.cleanup_slot_and_release_bit_inner(slot, sync_cleanup)
    }

    /// Find and allocate the next available slot.
    /// Slot 0 is reserved at init, so returned indices are always >= 1.
    ///
    /// Fast path: reuse a warm slot from the pool.
    /// Slow path: allocate a new index and set up kernel network resources.
    pub fn allocate_any(&self) -> Result<Slot> {
        if self.shutting_down() {
            return Err(anyhow!(ERR_SHUTTING_DOWN));
        }

        // Fast path: reuse a warm slot from the pool.
        if let Some(slot) = self.pool.try_acquire() {
            if self.shutting_down() {
                let _ = self.cleanup_slot_and_release_bit(slot);
                return Err(anyhow!(ERR_SHUTTING_DOWN));
            }
            debug!(slot = slot.idx, "reused warm network slot from pool");
            if self.pool.len() < self.pool.config().low_watermark {
                self.pool.request_maintenance();
            }
            return Ok(slot);
        }

        // Slow path: allocate a new index and set up kernel network resources.
        self.allocate_fresh_slot()
    }

    fn allocate_fresh_slot(&self) -> Result<Slot> {
        if self.shutting_down() {
            return Err(anyhow!(ERR_SHUTTING_DOWN));
        }

        // Ensure global host iptables rules are installed before allocating slots.
        // If the initial install failed (e.g., permission denied), retry once.
        if !HOST_IPTABLES_INSTALLED.load(Ordering::Acquire) {
            self.install_global_host_iptables()
                .map_err(NetworkError::HostIptablesError)?;
        }

        match self.allocated.set_next_free_bit() {
            Some(idx) => {
                let slot = Slot::new(idx as u32, self.address_plan, self.netns_dir.clone())
                    .expect("BitSet index within valid range");
                if let Err(e) = slot.create_network() {
                    self.allocated.remove(idx);
                    return Err(anyhow!("Failed to create network: {e}"));
                }

                if self.shutting_down() {
                    if let Err(cleanup_error) = self.cleanup_slot_and_release_bit(slot) {
                        return Err(anyhow!(
                            "Network manager is shutting down and failed to cleanup newly allocated slot {}: {}",
                            idx,
                            cleanup_error
                        ));
                    }
                    return Err(anyhow!(ERR_SHUTTING_DOWN));
                }

                Ok(slot)
            }
            None => Err(anyhow!("No available slots")),
        }
    }

    #[cfg(test)]
    fn compute_maintenance_action(&self, pool_len: usize) -> PoolMaintenanceAction {
        self.pool.compute_maintenance_action(pool_len)
    }

    fn run_pool_maintenance_cycle(&self) -> Result<()> {
        let action = self.pool.compute_maintenance_action(self.pool.len());

        match action {
            PoolMaintenanceAction::Fill(to_fill) => {
                for _ in 0..to_fill {
                    if self.shutting_down() {
                        break;
                    }

                    let maybe_slot = match self.allocate_fresh_slot() {
                        Ok(slot) => Some(slot),
                        Err(err) => {
                            debug!(error = %err, "skipping pool refill attempt");
                            break;
                        }
                    };

                    if let Some(slot) = maybe_slot {
                        let slot_idx = slot.idx;
                        match self.pool.try_push_bounded(slot) {
                            Ok(()) => {
                                trace!(
                                    slot = slot_idx,
                                    pool_len = self.pool.len(),
                                    "refilled warm network slot"
                                );
                            }
                            Err(slot) => self.cleanup_slot_and_release_bit(slot)?,
                        }
                    }
                }
            }
            PoolMaintenanceAction::Drain(to_drain) => {
                for _ in 0..to_drain {
                    let maybe_slot = self.pool.try_drain_one();
                    let Some(slot) = maybe_slot else {
                        break;
                    };
                    let slot_idx = slot.idx;
                    self.cleanup_slot_and_release_bit(slot)?;
                    trace!(
                        slot = slot_idx,
                        "drained excess warm network slot from pool"
                    );
                }
            }
            PoolMaintenanceAction::Idle => {}
        }

        Ok(())
    }

    /// Release a network slot. If the pool has room the slot is cached warm for
    /// reuse by the next `allocate_any()` call.
    ///
    /// When pool maintenance is enabled, release always enqueues the slot first;
    /// slots above the high watermark are drained asynchronously by the
    /// maintenance worker.
    ///
    /// When pool maintenance is disabled, this keeps the previous bounded-pool
    /// behavior and cleans up immediately once the pool reaches high watermark.
    pub fn release(&self, slot: Slot) -> Result<()> {
        if self.shutting_down() {
            return self.cleanup_slot_and_release_bit(slot);
        }
        let slot_idx = slot.idx;
        match self.pool.release(slot) {
            Ok(()) => {
                debug!(
                    slot = slot_idx,
                    pool_len = self.pool.len(),
                    "returned network slot to pool"
                );
                Ok(())
            }
            Err(slot) => self.cleanup_slot_and_release_bit(slot),
        }
    }

    /// Cleans up all warm slots cached in the pool.
    ///
    /// This is intended for process shutdown because the global manager is a
    /// static singleton and is not guaranteed to run Drop handlers at exit.
    ///
    /// After shutdown, the manager rejects new allocations and releases without
    /// caching to prevent new resource usage and avoid silent failures from leftover slots in the pool.
    pub fn shutdown(&self) -> Result<()> {
        self.shutdown_inner(false)
    }

    fn shutdown_inner(&self, sync_cleanup: bool) -> Result<()> {
        self.shutting_down.store(true, Ordering::Release);
        let drained_slots = self.pool.drain_all();
        let had_slots = !drained_slots.is_empty();
        let mut failures = Vec::new();

        if had_slots {
            debug!(
                slot_count = drained_slots.len(),
                "draining warm network slot pool during shutdown"
            );
            for slot in drained_slots {
                let idx = slot.idx;
                if let Err(err) = self.cleanup_slot_and_release_bit_inner(slot, sync_cleanup) {
                    failures.push(format!("slot {idx} cleanup failed: {err}"));
                }
            }
        }

        self.cleanup_global_host_iptables();

        if failures.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "failed to clean up pooled network slots during shutdown: {}",
                failures.join(" | ")
            ))
        }
    }

    /// Reserve slots whose host-side `veth-<idx>` interfaces already exist.
    ///
    /// This avoids collisions with stale interfaces left by previous runs or other
    /// concurrently running processes.
    fn reserve_existing_host_veth_slots(&self) -> Vec<usize> {
        let net_dir = Path::new("/sys/class/net");
        let entries = match fs::read_dir(net_dir) {
            Ok(entries) => entries,
            Err(err) => {
                warn!(path = %net_dir.display(), error = %err, "failed to scan host interfaces");
                return Vec::new();
            }
        };

        let mut reserved_slots = Vec::new();
        for entry in entries.flatten() {
            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let Some(slot_idx) = slot_index_from_host_veth_name(&name) else {
                continue;
            };

            let _ = self.allocated.insert(slot_idx);
            reserved_slots.push(slot_idx);
        }

        reserved_slots.sort_unstable();
        reserved_slots.dedup();
        reserved_slots
    }

    fn log_external_conflicts(&self, existing_slots: &[usize]) {
        if !existing_slots.is_empty() {
            let samples = existing_slots
                .iter()
                .copied()
                .take(CONFLICT_SAMPLE_LIMIT)
                .collect::<Vec<_>>();
            warn!(
                slot_count = existing_slots.len(),
                sample_slots = ?samples,
                "detected existing host veth interfaces using AgentENV slot naming; another sandbox runtime may already be active or previous cleanup may have left conflicting devices behind"
            );
        }

        let network_range_patterns = self.address_plan.conflict_patterns();
        let firewall_conflict_patterns = std::iter::once(HOST_VETH_PREFIX.to_string())
            .chain(network_range_patterns.iter().cloned())
            .collect::<Vec<_>>();

        self.check_conflicts(
            "host interface addresses",
            "ip",
            &["-o", "addr", "show"],
            &[],
            &network_range_patterns,
            "detected host interface addresses overlapping AgentENV network ranges; another program may already be using the same address space",
        );
        self.check_conflicts(
            "host routes",
            "ip",
            &["-o", "route", "show"],
            &[],
            &network_range_patterns,
            "detected host routes overlapping AgentENV network ranges; another program may already be using the same address space",
        );
        self.check_conflicts(
            "iptables-save",
            "iptables-save",
            &[],
            &[crate::privileges::CAP_NET_ADMIN],
            &firewall_conflict_patterns,
            "detected iptables rules referencing AgentENV interfaces or network ranges; sandbox traffic may be redirected, filtered, or rewritten by another program",
        );
        self.check_conflicts(
            "nft ruleset",
            "nft",
            &["list", "ruleset"],
            &[crate::privileges::CAP_NET_ADMIN],
            &firewall_conflict_patterns,
            "detected nftables rules referencing AgentENV interfaces or network ranges; sandbox traffic may be redirected, filtered, or rewritten by another program",
        );
    }

    fn check_conflicts(
        &self,
        source: &str,
        command: &str,
        args: &[&str],
        capabilities: &'static [i32],
        patterns: &[String],
        message: &str,
    ) {
        let Some(output) = run_command(command, args, capabilities) else {
            return;
        };
        let report = collect_conflict_report(&output, patterns);
        if report.total_matches == 0 {
            return;
        }

        warn!(
            source,
            match_count = report.total_matches,
            sample_lines = ?report.samples,
            "{message}"
        );
    }

    fn install_global_host_iptables(&self) -> Result<()> {
        if HOST_IPTABLES_INSTALLED.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let commands = global_host_iptables_commands(self.address_plan.host_interaction_cidr());

        match apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr) {
            Ok(()) => {
                debug!("installed global host iptables rules for sandbox networking");
                Ok(())
            }
            Err(e) => {
                HOST_IPTABLES_INSTALLED.store(false, Ordering::Release);
                Err(e)
            }
        }
    }

    fn cleanup_global_host_iptables(&self) {
        if !HOST_IPTABLES_INSTALLED.swap(false, Ordering::AcqRel) {
            return;
        }

        let delete_commands =
            global_host_iptables_delete_commands(self.address_plan.host_interaction_cidr());

        if let Err(e) = apply_iptables_commands(
            &delete_commands,
            OpenFailurePolicy::WarnAndIgnore("failed to open iptables for cleanup"),
        ) {
            warn!(error = %e, "failed to cleanup global host iptables rules");
        }
    }
}

fn global_host_iptables_commands(
    host_interaction_cidr: Ipv4Network,
) -> [IptablesRestoreCommand; 5] {
    let cidr = host_interaction_cidr.to_string();
    [
        // Guest replies to host-initiated proxy/envd connections must remain
        // reachable, while all other guest-to-host traffic is rejected below.
        IptablesRestoreCommand::Insert {
            table: "filter",
            chain: "INPUT",
            position: 1,
            rule: format!(
                "-i {HOST_VETH_PREFIX}+ -s {cidr} -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT"
            ),
        },
        IptablesRestoreCommand::Insert {
            table: "filter",
            chain: "INPUT",
            position: 2,
            rule: format!("-i {HOST_VETH_PREFIX}+ -s {cidr} -j REJECT"),
        },
        // Packets are SNATted to the host interaction CIDR inside the sandbox namespace before
        // they enter the host FORWARD chain, so hosts with a DROP FORWARD policy
        // need to accept this post-SNAT source range.
        IptablesRestoreCommand::Append {
            table: "filter",
            chain: "FORWARD",
            rule: format!("-i {HOST_VETH_PREFIX}+ -s {cidr} -j ACCEPT"),
        },
        IptablesRestoreCommand::Append {
            table: "filter",
            chain: "FORWARD",
            rule: format!(
                "-o {HOST_VETH_PREFIX}+ -d {cidr} -m state --state RELATED,ESTABLISHED -j ACCEPT"
            ),
        },
        IptablesRestoreCommand::Append {
            table: "nat",
            chain: "POSTROUTING",
            rule: format!("-s {cidr} -j MASQUERADE"),
        },
    ]
}

fn global_host_iptables_delete_commands(
    host_interaction_cidr: Ipv4Network,
) -> [IptablesRestoreCommand; 5] {
    let cidr = host_interaction_cidr.to_string();
    [
        IptablesRestoreCommand::Delete {
            table: "filter",
            chain: "INPUT",
            rule: format!(
                "-i {HOST_VETH_PREFIX}+ -s {cidr} -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT"
            ),
        },
        IptablesRestoreCommand::Delete {
            table: "filter",
            chain: "INPUT",
            rule: format!("-i {HOST_VETH_PREFIX}+ -s {cidr} -j REJECT"),
        },
        IptablesRestoreCommand::Delete {
            table: "filter",
            chain: "FORWARD",
            rule: format!("-i {HOST_VETH_PREFIX}+ -s {cidr} -j ACCEPT"),
        },
        IptablesRestoreCommand::Delete {
            table: "filter",
            chain: "FORWARD",
            rule: format!(
                "-o {HOST_VETH_PREFIX}+ -d {cidr} -m state --state RELATED,ESTABLISHED -j ACCEPT"
            ),
        },
        IptablesRestoreCommand::Delete {
            table: "nat",
            chain: "POSTROUTING",
            rule: format!("-s {cidr} -j MASQUERADE"),
        },
    ]
}

impl Drop for NetworkManager {
    fn drop(&mut self) {
        if let Err(err) = self.shutdown() {
            warn!(error = %err, "network manager drop cleanup failed");
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ConflictReport {
    total_matches: usize,
    samples: Vec<String>,
}

fn slot_index_from_host_veth_name(name: &str) -> Option<usize> {
    let slot_str = name.strip_prefix(HOST_VETH_PREFIX)?;
    let slot_idx = slot_str.parse::<usize>().ok()?;
    if slot_idx == 0 || slot_idx >= MAX_SLOTS {
        return None;
    }
    Some(slot_idx)
}

fn collect_conflict_report(output: &str, patterns: &[String]) -> ConflictReport {
    let mut total_matches = 0;
    let mut samples = Vec::new();

    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if !patterns
            .iter()
            .any(|pattern| line.contains(pattern.as_str()))
        {
            continue;
        }

        total_matches += 1;
        if samples.len() < CONFLICT_SAMPLE_LIMIT {
            samples.push(line.to_string());
        }
    }

    ConflictReport {
        total_matches,
        samples,
    }
}

fn run_command(command: &str, args: &[&str], capabilities: &'static [i32]) -> Option<String> {
    let output = match crate::privileges::run_with_scoped_capabilities(capabilities, || {
        Command::new(command)
            .args(args)
            .output()
            .map_err(anyhow::Error::from)
    }) {
        Ok(output) => output,
        Err(err) => {
            debug!(command, args = ?args, error = %err, "failed to inspect host networking state");
            return None;
        }
    };

    if !output.status.success() {
        debug!(
            command,
            args = ?args,
            exit_code = output.status.code().unwrap_or_default(),
            stderr = %String::from_utf8_lossy(&output.stderr),
            "network conflict inspection command exited unsuccessfully"
        );
        return None;
    }

    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use index_set::BitSet;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn manager_with_capacity(capacity: usize) -> NetworkManager {
        NetworkManager::new(false, capacity, capacity)
    }

    fn test_slot(idx: u32) -> Slot {
        Slot::new(
            idx,
            NetworkAddressPlan::default(),
            std::env::temp_dir().join("aenv-network-tests/netns"),
        )
        .unwrap()
    }

    fn command_stdout(command: &str, args: &[&str]) -> Option<String> {
        let output = Command::new(command).args(args).output().ok()?;
        if !output.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn has_network_runtime_capabilities() -> bool {
        let required =
            (1u64 << crate::privileges::CAP_NET_ADMIN) | (1u64 << crate::privileges::CAP_SYS_ADMIN);
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status
                    .lines()
                    .find_map(|line| line.strip_prefix("CapEff:"))
                    .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
            })
            .is_some_and(|caps| caps & required == required)
    }

    fn parse_child_marker(output: &str, key: &str) -> Option<String> {
        let prefix = format!("{key}=");
        output
            .lines()
            .find_map(|line| line.trim().strip_prefix(&prefix).map(str::to_owned))
    }

    fn netns_exists(namespace_id: &str) -> bool {
        crate::cfg::ConfigManager::global_config()
            .runtime_path
            .join("netns")
            .join(namespace_id)
            .exists()
    }

    fn host_veth_exists(slot_idx: u32) -> bool {
        let veth_name = format!("{HOST_VETH_PREFIX}{slot_idx}");
        command_stdout("ip", &["-o", "link", "show", &veth_name])
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }

    fn host_route_exists(host_interaction_ip: &str) -> bool {
        let route = format!("{host_interaction_ip}/32");
        command_stdout("ip", &["-o", "route", "show", &route])
            .map(|s| s.lines().any(|line| line.contains(&route)))
            .unwrap_or(false)
    }

    fn wait_until(
        timeout: Duration,
        interval: Duration,
        mut predicate: impl FnMut() -> bool,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(interval);
        }
    }

    #[test]
    fn slot_zero_is_reserved_on_init() {
        let manager = manager_with_capacity(0);
        // Slot 0 is reserved internally so allocate_any will never return it.
        // Verify by checking the raw bitset directly.
        assert!(manager.allocated.has(0));
    }

    #[test]
    fn allocate_any_never_returns_slot_zero() {
        let manager = manager_with_capacity(0);
        // First allocation must skip the reserved slot 0
        let idx = manager
            .allocated
            .set_next_free_bit()
            .expect("at least one free slot");
        assert!(idx >= 1, "allocate_any returned reserved slot 0");
    }

    #[test]
    fn reject_allocation_at_boundaries() {
        let manager = manager_with_capacity(0);

        // Slot 0 is reserved
        let err = manager.allocate_slot(0).unwrap_err().to_string();
        assert!(err.contains("out of range"));

        // Slot MAX_SLOTS is out of range
        let err = manager
            .allocate_slot(MAX_SLOTS as u32)
            .unwrap_err()
            .to_string();
        assert!(err.contains("out of range"));

        // Slot MAX_SLOTS - 1 is the last valid index
        let last_slot = manager.allocate_slot((MAX_SLOTS - 1) as u32).unwrap();
        drop(last_slot);
    }

    #[test]
    fn duplicate_allocation_is_rejected() {
        let manager = manager_with_capacity(0);
        let slot = manager.allocate_test_slot().unwrap();
        let idx = slot.idx;

        let err = manager.allocate_slot(idx).unwrap_err().to_string();
        assert!(err.contains("already allocated"));
        drop(slot);
    }

    #[test]
    fn release_then_reallocate() {
        let manager = manager_with_capacity(0);
        let slot = manager.allocate_test_slot().unwrap();
        let idx = slot.idx;

        // Manually release via the local manager.
        manager.release(slot).unwrap();

        // Should be able to allocate the same index again
        let slot2 = manager.allocate_slot(idx).unwrap();
        assert_eq!(slot2.idx, idx);
        drop(slot2);
    }

    #[test]
    fn double_release_is_rejected() {
        let manager = manager_with_capacity(0);
        let slot = manager.allocate_test_slot().unwrap();
        let idx = slot.idx;
        manager.release(slot).unwrap();

        let err = manager.release_slot_bit(idx).unwrap_err().to_string();
        assert!(err.contains("not allocated"));
    }

    #[test]
    fn release_slot_zero_is_rejected() {
        let manager = manager_with_capacity(0);
        let err = manager.release_slot_bit(0).unwrap_err().to_string();
        assert!(err.contains("out of range"));
    }

    #[test]
    fn slot_index_from_host_veth_name_parses_valid_slot() {
        assert_eq!(slot_index_from_host_veth_name("veth-42"), Some(42));
    }

    #[test]
    fn slot_index_from_host_veth_name_rejects_non_matching_names() {
        assert_eq!(slot_index_from_host_veth_name("eth0"), None);
        assert_eq!(slot_index_from_host_veth_name("veth-nope"), None);
        assert_eq!(slot_index_from_host_veth_name("veth-0"), None);
    }

    #[test]
    fn collect_conflict_report_keeps_samples_and_total_count() {
        let output = "\
            10.11.0.1 via 10.12.0.3 dev veth-1\n\
            iifname \"veth-1\" tcp dport 443 redirect to :5017\n\
            unrelated line\n\
            ip saddr 10.11.0.2 oifname \"eth0\" masquerade\n\
        ";
        let patterns = vec![
            HOST_VETH_PREFIX.to_string(),
            "10.11.".to_string(),
            "10.12.".to_string(),
        ];
        let report = collect_conflict_report(output, &patterns);
        assert_eq!(report.total_matches, 3);
        assert_eq!(report.samples.len(), 3);
        assert!(report.samples[0].contains("10.11.0.1"));
        assert!(report.samples[1].contains("veth-1"));
        assert!(report.samples[2].contains("10.11.0.2"));
    }

    #[test]
    fn maintenance_action_fills_to_current_target() {
        let manager = NetworkManager::new(true, 4, 10);
        assert_eq!(
            manager.compute_maintenance_action(2),
            PoolMaintenanceAction::Fill(2)
        );
        assert_eq!(
            manager.compute_maintenance_action(7),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn maintenance_action_drains_above_high_watermark() {
        let manager = NetworkManager::new(true, 2, 4);
        assert_eq!(
            manager.compute_maintenance_action(8),
            PoolMaintenanceAction::Drain(4)
        );
        assert_eq!(
            manager.compute_maintenance_action(4),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn maintenance_cycle_drains_excess_warm_slots() {
        let manager = NetworkManager::new(true, 0, 2);
        for idx in 1..=4u32 {
            manager.allocated.insert(idx as usize);
            manager.pool.release(test_slot(idx)).unwrap();
        }

        manager.run_pool_maintenance_cycle().unwrap();

        let pool_len = manager.pool.len();
        assert_eq!(pool_len, 2);
        assert!(manager.allocated.has(1));
        assert!(manager.allocated.has(2));
        assert!(!manager.allocated.has(3));
        assert!(!manager.allocated.has(4));
    }

    #[test]
    fn concurrent_allocate_any_produces_unique_slots() {
        let n = 200;
        // Pre-populate the pool so allocate_any() always uses the fast path (no create_network).
        let manager = Arc::new(manager_with_capacity(n));
        for idx in 1..=(n as u32) {
            manager.allocated.insert(idx as usize);
            manager.pool.try_push_bounded(test_slot(idx)).unwrap();
        }

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let m = manager.clone();
                std::thread::spawn(move || {
                    let slot = m.allocate_any().unwrap();
                    let idx = slot.idx;
                    drop(slot);
                    idx
                })
            })
            .collect();

        let allocated: HashSet<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All indices must be unique
        assert_eq!(allocated.len(), n);
        // None should be slot 0
        assert!(!allocated.contains(&0));
    }

    #[test]
    fn release_returns_slot_to_pool_when_under_capacity() {
        let manager = manager_with_capacity(5);
        // Pre-insert a slot into the bitset (simulating a prior allocation)
        manager.allocated.insert(10);
        let slot = test_slot(10);
        // release() should push the slot into the pool (capacity=5, pool is empty)
        manager.release(slot).unwrap();
        let pool_len = manager.pool.len();
        assert_eq!(pool_len, 1, "slot should be in pool");
        // Clean up: drain pool.
        while let Some(s) = manager.pool.try_drain_one() {
            drop(s);
        }
        manager.allocated.remove(10);
    }

    #[test]
    fn allocate_any_prefers_pool_over_bitset() {
        let manager = manager_with_capacity(5);
        // Manually put a slot (idx=42) in the pool and mark it allocated in the bitset
        manager.allocated.insert(42);
        let pooled = test_slot(42);
        manager.pool.try_push_bounded(pooled).unwrap();

        // allocate_any() must pop from pool (idx=42) without calling create_network()
        // We verify: (1) the returned idx is 42, (2) no other slot was allocated
        let slot = manager.allocate_any().unwrap();
        assert_eq!(slot.idx, 42, "allocate_any must return the pooled slot");
        assert!(manager.pool.is_empty(), "pool must be empty after pop");
        drop(slot);
        manager.allocated.remove(42);
    }

    #[test]
    fn release_cleans_up_when_pool_is_full() {
        // high_watermark = 0: release() must NOT push to pool, must call cleanup() instead
        let manager = manager_with_capacity(0);
        manager.allocated.insert(7);
        let slot = test_slot(7);
        manager.release(slot).unwrap();
        // Pool must be empty (slot was NOT cached)
        assert!(manager.pool.is_empty(), "pool must be empty");
        // Bitset bit must be freed
        assert!(
            !manager.allocated.has(7),
            "bitset bit must be cleared after release with full pool"
        );
    }

    #[test]
    fn release_enqueues_above_high_watermark_and_drains_async() {
        let manager = NetworkManager::new(true, 0, 0);
        manager.allocated.insert(17);
        let slot = test_slot(17);

        manager.release(slot).unwrap();

        assert_eq!(
            manager.pool.len(),
            1,
            "slot should be enqueued first even when above high watermark"
        );
        assert!(
            manager.allocated.has(17),
            "bitset should remain allocated until async drain runs"
        );

        manager.run_pool_maintenance_cycle().unwrap();

        assert!(manager.pool.is_empty());
        assert!(
            !manager.allocated.has(17),
            "bitset bit must be released after maintenance drain"
        );
    }

    #[test]
    fn shutdown_cleanup_drains_pool_and_releases_bits() {
        let manager = manager_with_capacity(4);
        manager.allocated.insert(11);
        manager.allocated.insert(12);

        manager.pool.try_push_bounded(test_slot(11)).unwrap();
        manager.pool.try_push_bounded(test_slot(12)).unwrap();

        manager.shutdown().unwrap();

        assert!(manager.pool.is_empty(), "pool must be empty");
        assert!(!manager.allocated.has(11), "slot 11 bit must be released");
        assert!(!manager.allocated.has(12), "slot 12 bit must be released");
    }

    #[test]
    fn shutdown_cleanup_is_noop_for_empty_pool() {
        let manager = manager_with_capacity(4);
        manager.shutdown().unwrap();
    }

    #[test]
    fn shutdown_cleanup_is_idempotent() {
        let manager = manager_with_capacity(4);
        manager.allocated.insert(14);
        manager.pool.try_push_bounded(test_slot(14)).unwrap();

        manager.shutdown().unwrap();
        manager.shutdown().unwrap();

        assert!(manager.pool.is_empty());
        assert!(!manager.allocated.has(14));
    }

    #[test]
    fn allocate_any_is_rejected_after_shutdown_cleanup() {
        let manager = manager_with_capacity(4);
        manager.allocated.insert(9);
        manager.pool.try_push_bounded(test_slot(9)).unwrap();

        manager.shutdown().unwrap();

        let err = manager.allocate_any().unwrap_err().to_string();
        assert!(err.contains("shutting down"));
    }

    #[test]
    fn release_does_not_cache_slot_after_shutdown_cleanup() {
        let manager = manager_with_capacity(4);
        manager.shutdown().unwrap();

        manager.allocated.insert(13);
        let slot = test_slot(13);
        manager.release(slot).unwrap();

        assert!(manager.pool.is_empty(), "pool must remain empty");
        assert!(
            !manager.allocated.has(13),
            "slot bit must be released instead of kept warm"
        );
    }

    #[test]
    fn release_race_with_shutdown_does_not_recache_slot() {
        let manager = Arc::new(manager_with_capacity(4));
        manager.allocated.insert(15);
        let slot = test_slot(15);

        let release_manager = manager.clone();
        let release_thread = std::thread::spawn(move || {
            release_manager.release(slot).unwrap();
        });

        let shutdown_manager = manager.clone();
        let shutdown_thread = std::thread::spawn(move || {
            shutdown_manager.shutdown().unwrap();
        });

        release_thread.join().unwrap();
        shutdown_thread.join().unwrap();

        assert!(manager.pool.is_empty());
        assert!(!manager.allocated.has(15));
    }

    #[test]
    fn concurrent_release_with_pool() {
        use std::sync::Arc;

        // Pool capacity of 10 with 50 concurrent releases.
        let manager = Arc::new(manager_with_capacity(10));

        // Pre-populate bitset for slots 1..=50 and build Slot values.
        // We bypass allocate_any() because create_network() requires host capabilities.
        // Instead we manually mark slots as allocated in the bitset.
        for idx in 1u32..=50 {
            manager.allocated.insert(idx as usize);
        }
        let slot_values: Vec<Slot> = (1u32..=50).map(test_slot).collect();

        // Release all 50 slots concurrently — at most 10 should end up in the pool.
        let handles: Vec<_> = slot_values
            .into_iter()
            .map(|slot| {
                let m = manager.clone();
                std::thread::spawn(move || {
                    m.release(slot).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Pool must not exceed capacity.
        let pool_len = manager.pool.len();
        assert!(pool_len <= 10, "pool exceeded capacity: {pool_len}");

        // Clean up remaining pool slots.
        while let Some(slot) = manager.pool.try_drain_one() {
            drop(slot);
        }
        // Free any remaining bitset bits (for slots that were cleaned up, not pooled).
        for idx in 1u32..=50 {
            manager.allocated.remove(idx as usize);
        }
    }

    #[test]
    fn concurrent_allocate_and_release_cycle() {
        let total = 50;
        // Pre-populate the pool so allocate_any() uses the fast path (no create_network).
        let manager = Arc::new(manager_with_capacity(total));
        for idx in 1..=(total as u32) {
            manager.allocated.insert(idx as usize);
            manager.pool.try_push_bounded(test_slot(idx)).unwrap();
        }

        // Phase 1: Allocate 50 slots concurrently from the pool
        let handles: Vec<_> = (0..total)
            .map(|_| {
                let m = manager.clone();
                std::thread::spawn(move || {
                    let slot = m.allocate_any().unwrap();
                    let idx = slot.idx;
                    drop(slot);
                    idx
                })
            })
            .collect();
        let first_batch: Vec<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Phase 2: Release even-indexed results concurrently (just freeing the bitset bit)
        let to_release: Vec<u32> = first_batch.iter().copied().filter(|i| i % 2 == 0).collect();
        let release_count = to_release.len();
        let handles: Vec<_> = to_release
            .into_iter()
            .map(|idx| {
                let m = manager.clone();
                std::thread::spawn(move || m.release_slot_bit(idx).unwrap())
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Phase 3: Re-allocate the released slots (slow path — bitset only, no create_network in test)
        // Re-add them to the pool so the test remains root-free.
        for idx in first_batch.iter().copied().filter(|i| i % 2 == 0) {
            manager.allocated.insert(idx as usize); // re-mark as allocated
            manager.pool.try_push_bounded(test_slot(idx)).unwrap();
        }
        let handles: Vec<_> = (0..release_count)
            .map(|_| {
                let m = manager.clone();
                std::thread::spawn(move || {
                    let slot = m.allocate_any().unwrap();
                    let idx = slot.idx;
                    drop(slot);
                    idx
                })
            })
            .collect();
        let second_batch: HashSet<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All new allocations must be unique and non-zero
        assert_eq!(second_batch.len(), release_count);
        assert!(!second_batch.contains(&0));
    }

    #[test]
    #[ignore = "requires CAP_NET_ADMIN/CAP_SYS_ADMIN and intentionally crashes a child process"]
    fn panic_exit_with_pooled_slot_is_cleaned_by_exit_hook() {
        const CHILD_ENV: &str = "AENV_NETWORK_PANIC_CHILD";
        const TEST_NAME: &str =
            "sandbox::network::manager::tests::panic_exit_with_pooled_slot_is_cleaned_by_exit_hook";

        if std::env::var_os(CHILD_ENV).is_some() {
            let manager = NetworkManager::global();
            let slot = manager
                .allocate_any()
                .expect("child failed to allocate real network slot");

            let slot_idx = slot.idx;
            let netns_id = slot.namespace_id.clone();
            let host_veth = format!("{HOST_VETH_PREFIX}{slot_idx}");

            let host_veth_exists = command_stdout("ip", &["-o", "link", "show", &host_veth])
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            assert!(
                host_veth_exists,
                "child failed to observe allocated host veth interface: {host_veth}"
            );

            let netns_exists = netns_exists(&netns_id);
            assert!(
                netns_exists,
                "child failed to observe allocated network namespace: {netns_id}"
            );

            println!("SLOT_IDX={slot_idx}");
            println!("NETNS_ID={netns_id}");
            println!("HOST_INTERACTION_IP={}", slot.host_interaction_ip);

            manager
                .release(slot)
                .expect("child failed to return slot to warm pool");

            panic!("intentional panic for crash-cleanup validation");
        }

        if !has_network_runtime_capabilities() {
            eprintln!("skipping crash-cleanup validation: required capabilities are missing");
            return;
        }

        let exe = std::env::current_exe().expect("failed to locate current test binary");
        let output = Command::new(exe)
            .arg("--exact")
            .arg(TEST_NAME)
            .arg("--ignored")
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .output()
            .expect("failed to launch panic child process");

        let child_stdout = String::from_utf8_lossy(&output.stdout);
        let child_stderr = String::from_utf8_lossy(&output.stderr);

        assert!(
            !output.status.success(),
            "child process should fail due to intentional panic, stdout={child_stdout}, stderr={child_stderr}"
        );

        let slot_idx = parse_child_marker(&child_stdout, "SLOT_IDX")
            .and_then(|raw| raw.parse::<u32>().ok())
            .unwrap_or_else(|| {
                panic!(
                    "child did not emit slot index marker, stdout={child_stdout}, stderr={child_stderr}"
                )
            });
        let netns_id = parse_child_marker(&child_stdout, "NETNS_ID").unwrap_or_else(|| {
            panic!("child did not emit netns marker, stdout={child_stdout}, stderr={child_stderr}")
        });
        let host_interaction_ip =
            parse_child_marker(&child_stdout, "HOST_INTERACTION_IP").unwrap_or_else(
                || {
                    panic!(
                        "child did not emit host interaction IP marker, stdout={child_stdout}, stderr={child_stderr}"
                    )
                },
            );

        assert!(
            wait_until(Duration::from_secs(3), Duration::from_millis(50), || {
                !netns_exists(&netns_id)
            }),
            "network namespace leaked after panic child exit: {netns_id}"
        );
        assert!(
            wait_until(Duration::from_secs(3), Duration::from_millis(50), || {
                !host_veth_exists(slot_idx)
            }),
            "host veth leaked after panic child exit: {HOST_VETH_PREFIX}{slot_idx}"
        );
        assert!(
            wait_until(Duration::from_secs(3), Duration::from_millis(50), || {
                !host_route_exists(&host_interaction_ip)
            }),
            "host route leaked after panic child exit: {host_interaction_ip}/32"
        );
    }
}
