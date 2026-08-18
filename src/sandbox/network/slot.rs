use std::fs::{self, File};
use std::net::{IpAddr, Ipv4Addr};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, Once, OnceLock};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::{stream::TryStreamExt, StreamExt};
use netlink_packet_route::address::{AddressAttribute, AddressMessage};
use netlink_packet_route::link::{
    InfoData, InfoKind, InfoVeth, LinkAttribute, LinkFlags, LinkInfo,
};
use netlink_packet_route::{AddressFamily, RouteNetlinkMessage};
use nix::mount::{mount, MsFlags};
use nix::sched::{unshare, CloneFlags};
use rtnetlink::packet_core::{
    NetlinkMessage, NetlinkPayload, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST,
};
use rtnetlink::{new_connection, Handle};
use tracing::{debug, info, warn};

use super::iptables_util::{apply_iptables_commands, IptablesRestoreCommand, OpenFailurePolicy};
use super::policy::{
    initialize_namespace_egress_chain, set_namespace_egress_policy, SandboxNetworkPolicy,
};
use super::{NetworkAddressPlan, NetworkError, HOST_VETH_PREFIX, MAX_SLOTS, NETNS_PREFIX};

/// Process-wide baseline network namespace fd.
///
/// Captured once from the current calling thread before any `unshare(CLONE_NEWNET)`.
/// All subsequent slot creations move host-side interfaces back to this namespace.
static HOST_NS_FD: OnceLock<OwnedFd> = OnceLock::new();

/// Children that could not be reaped within the grace period after `kill`.
///
/// A single process-lifetime reaper thread polls them with `try_wait`, so a
/// persistently stuck `ip` child never accumulates one blocked thread per
/// timeout, and dropping a `Child` (which never reaps) cannot leak a zombie.
static UNREAPED_CHILDREN: Mutex<Vec<std::process::Child>> = Mutex::new(Vec::new());
static REAPER_CV: Condvar = Condvar::new();
static REAPER_ONCE: Once = Once::new();

/// Hand a killed-but-unreapable child to the centralized reaper thread.
///
/// The reaper is the only thread that ever blocks on these children; it owns
/// no pool or slot state, and there is at most one reaper for the process
/// lifetime regardless of how many teardown attempts time out.
fn hand_off_unreaped_child(child: std::process::Child) {
    REAPER_ONCE.call_once(|| {
        let spawned = std::thread::Builder::new()
            .name("network-slot-child-reaper".to_string())
            .spawn(|| {
                let mut pending: Vec<std::process::Child> = Vec::new();
                loop {
                    if pending.is_empty() {
                        let mut guard = UNREAPED_CHILDREN
                            .lock()
                            .unwrap_or_else(|err| err.into_inner());
                        while guard.is_empty() {
                            guard = REAPER_CV.wait(guard).unwrap_or_else(|err| err.into_inner());
                        }
                        pending.append(&mut guard);
                        drop(guard);
                    }
                    // `try_wait` reaps exited children; errored polls are
                    // retried on the next pass.
                    pending.retain_mut(|child| !matches!(child.try_wait(), Ok(Some(_))));
                    if pending.is_empty() {
                        continue;
                    }
                    thread::sleep(Duration::from_millis(20));
                    // Pick up children handed off while polling.
                    let mut guard = UNREAPED_CHILDREN
                        .lock()
                        .unwrap_or_else(|err| err.into_inner());
                    pending.append(&mut guard);
                    drop(guard);
                }
            });
        if let Err(err) = spawned {
            warn!(error = %err, "failed to spawn child reaper thread");
        }
    });
    UNREAPED_CHILDREN
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .push(child);
    REAPER_CV.notify_one();
}

const ARP_RETRANS_TIME_MS: &str = "100";
const NEIGH_SYSCTL_RETRIES: usize = 5;
const NEIGH_SYSCTL_RETRY_DELAY_MS: u64 = 20;

/// Get a borrowed reference to the host network namespace fd.
pub(super) fn host_ns_fd() -> BorrowedFd<'static> {
    HOST_NS_FD
        .get_or_init(|| {
            let file = File::open("/proc/thread-self/ns/net")
                .expect("Failed to open host network namespace from /proc/thread-self/ns/net");
            OwnedFd::from(file)
        })
        .as_fd()
}

#[derive(Debug)]
pub(crate) struct Slot {
    pub idx: u32,
    pub namespace_id: String,
    pub host_interaction_ip: Ipv4Addr,
    pub veth_host_ip: Ipv4Addr, // The IP on the Host side interface
    pub veth_vm_ip: Ipv4Addr,   // The IP on the VM/NS side interface (vpeer)
    address_plan: NetworkAddressPlan,
    netns_dir: PathBuf,
    cleanup_armed: AtomicBool,
}

struct NamespaceSetup {
    idx: u32,
    namespace_id: String,
    veth_vm_ip: Ipv4Addr,
    veth_host_ip: Ipv4Addr,
    host_interaction_ip: Ipv4Addr,
    address_plan: NetworkAddressPlan,
    netns_dir: PathBuf,
}

impl Slot {
    fn host_veth_name(idx: u32) -> String {
        format!("{HOST_VETH_PREFIX}{idx}")
    }

    pub(super) fn new(
        idx: u32,
        address_plan: NetworkAddressPlan,
        netns_dir: PathBuf,
    ) -> Result<Self, NetworkError> {
        // Validation for zero and overflow.
        if idx == 0 || idx >= (MAX_SLOTS as u32) {
            return Err(NetworkError::SlotOutOfRange {
                idx,
                max: (MAX_SLOTS as u32) - 1,
            });
        }

        let namespace_id = format!("{}{}", NETNS_PREFIX, uuid::Uuid::now_v7());
        let (host_interaction_ip, veth_host_ip, veth_vm_ip) = address_plan
            .slot_ips(idx)
            .map_err(NetworkError::NamespaceError)?;

        Ok(Self {
            idx,
            namespace_id,
            host_interaction_ip,
            veth_host_ip,
            veth_vm_ip,
            address_plan,
            netns_dir,
            cleanup_armed: AtomicBool::new(false),
        })
    }

    /// Creates the network infrastructure for this slot using a separate thread
    /// to isolate namespace operations.
    #[tracing::instrument(
        skip(self),
        fields(
            slot = self.idx,
            namespace_id = %self.namespace_id,
            host_veth = %Self::host_veth_name(self.idx),
            host_interaction_ip = %self.host_interaction_ip
        )
    )]
    pub(super) fn create_network(&self) -> Result<(), NetworkError> {
        // Arm drop cleanup as soon as we begin touching kernel networking state.
        // If setup fails midway, Drop can still perform best-effort cleanup.
        self.cleanup_armed.store(true, Ordering::Release);

        // Capture individual fields rather than cloning `self`. Slot is not Clone
        // intentionally — a clone would carry Drop semantics and tear down the live
        // network when the thread finishes.
        let setup = NamespaceSetup {
            idx: self.idx,
            namespace_id: self.namespace_id.clone(),
            veth_vm_ip: self.veth_vm_ip,
            veth_host_ip: self.veth_host_ip,
            host_interaction_ip: self.host_interaction_ip,
            address_plan: self.address_plan,
            netns_dir: self.netns_dir.clone(),
        };
        let idx = setup.idx;
        let veth_host_ip = setup.veth_host_ip;
        let veth_vm_ip = setup.veth_vm_ip;
        let host_interaction_ip = setup.host_interaction_ip;

        // Get the global host NS FD to move the interface back later.
        // This uses /proc/1/ns/net to ensure we always get the true host namespace,
        // even when called from threads that may have modified their namespaces.
        let host_ns_fd = host_ns_fd();

        // Spawn a thread to perform namespace operations safely.
        let handle = thread::spawn(move || Self::setup_namespace_internal(setup, host_ns_fd));

        match handle.join() {
            Ok(result) => result.map_err(NetworkError::NamespaceError),
            Err(e) => Err(NetworkError::NamespaceError(anyhow!(
                "Network setup thread panicked: {:?}",
                e
            ))),
        }?;

        // Configure the Host side now.
        Self::run_async(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("Failed to build tokio runtime")?;
            rt.block_on(Self::configure_host_interface_async(
                idx,
                veth_host_ip,
                veth_vm_ip,
                host_interaction_ip,
            ))
        })
        .map_err(NetworkError::NamespaceError)?;

        // Reduce ARP retransmit delay on host-side veth to avoid resume tail latency (issue #272).
        let veth_name = Self::host_veth_name(idx);
        Self::tune_neigh_retrans_time_ms(&veth_name);

        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(
            slot = setup.idx,
            namespace_id = %setup.namespace_id,
            veth_vm_ip = %setup.veth_vm_ip,
            veth_host_ip = %setup.veth_host_ip,
            host_interaction_ip = %setup.host_interaction_ip
        )
    )]
    fn setup_namespace_internal(
        setup: NamespaceSetup,
        host_ns_fd: BorrowedFd<'static>,
    ) -> Result<()> {
        let NamespaceSetup {
            idx,
            namespace_id,
            veth_vm_ip,
            veth_host_ip,
            host_interaction_ip,
            address_plan,
            netns_dir,
        } = setup;

        // 1. Create/Open Target Network Namespace
        if !netns_dir.exists() {
            fs::create_dir_all(&netns_dir).with_context(|| {
                format!(
                    "Failed to create AENV network namespace directory {}",
                    netns_dir.display()
                )
            })?;
        }
        let netns_path = netns_dir.join(&namespace_id);
        if !netns_path.exists() {
            File::create(&netns_path).context("Failed to create netns file")?;
        }

        // Unshare logic
        unshare(CloneFlags::CLONE_NEWNET).context("Failed to unshare(CLONE_NEWNET)")?;

        // Bind mount the new namespace to make it persistent/named
        mount(
            Some("/proc/thread-self/ns/net"),
            &netns_path,
            None::<&str>,
            MsFlags::MS_BIND,
            None::<&str>,
        )
        .context("Failed to bind mount new namespace")?;

        // Configure interfaces inside the namespace via netlink
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to build tokio runtime in thread")?;
        let tap_ip = address_plan.tap_ip();
        let vm_link_prefix = address_plan.vm_link_prefix();

        rt.block_on(Self::configure_namespace_interfaces(
            idx,
            veth_vm_ip,
            veth_host_ip,
            tap_ip,
            vm_link_prefix,
            host_ns_fd,
        ))?;

        // Enable IP forwarding inside this namespace so packets received on tap0
        // (from the VM) can be forwarded to vpeer (towards the host/internet).
        fs::write("/proc/sys/net/ipv4/ip_forward", "1")
            .context("Failed to enable IP forwarding in namespace")?;

        // Reduce ARP retransmit delay for faster resume (issue #272)
        Self::tune_neigh_retrans_time_ms("tap0");
        Self::tune_neigh_retrans_time_ms("vpeer");

        // IPTables Setup
        Self::configure_namespace_iptables_rules(
            host_interaction_ip,
            veth_host_ip,
            address_plan.vm_ip(),
            &address_plan.internal_egress_denied_cidrs(),
        )
    }

    /// Configures all network interfaces inside the namespace:
    /// creates veth pair, moves host end back, sets up loopback/vpeer/tap0, and adds default route.
    #[tracing::instrument(
        skip(host_ns_fd),
        fields(
            slot = idx,
            veth_vm_ip = %veth_vm_ip,
            veth_host_ip = %veth_host_ip
        )
    )]
    async fn configure_namespace_interfaces(
        idx: u32,
        veth_vm_ip: Ipv4Addr,
        veth_host_ip: Ipv4Addr,
        tap_ip: Ipv4Addr,
        vm_link_prefix: u8,
        host_ns_fd: BorrowedFd<'_>,
    ) -> Result<()> {
        let (connection, handle, _) = new_connection().context("Failed to connect to netlink")?;
        tokio::spawn(connection);

        // Create Veth Pair (veth-{idx} and vpeer)
        let veth_name = Self::host_veth_name(idx);
        let vpeer_name = "vpeer";

        // Create veth pair using netlink
        let mut veth_msg = netlink_packet_route::link::LinkMessage::default();
        veth_msg
            .attributes
            .push(LinkAttribute::IfName(veth_name.clone()));

        let mut peer_msg = netlink_packet_route::link::LinkMessage::default();
        peer_msg
            .attributes
            .push(LinkAttribute::IfName(vpeer_name.into()));

        let info = LinkAttribute::LinkInfo(vec![
            LinkInfo::Kind(InfoKind::Veth),
            LinkInfo::Data(InfoData::Veth(InfoVeth::Peer(peer_msg))),
        ]);
        veth_msg.attributes.push(info);

        handle
            .link()
            .add(veth_msg)
            .execute()
            .await
            .context("Failed to create veth pair")?;

        // Move veth (host end) back to Host NS
        let mut links = handle.link().get().match_name(veth_name.clone()).execute();
        if let Some(link) = links.try_next().await? {
            let mut msg = netlink_packet_route::link::LinkMessage::default();
            msg.header.index = link.header.index;
            msg.attributes
                .push(LinkAttribute::NetNsFd(host_ns_fd.as_raw_fd()));

            handle
                .link()
                .set(msg)
                .execute()
                .await
                .context("Failed to move veth to host ns")?;
        } else {
            return Err(anyhow!("Created veth interface not found"));
        }

        // Configure IPs inside NS
        // Loopback UP
        let mut lo_links = handle.link().get().match_name("lo".to_string()).execute();
        if let Some(lo) = lo_links.try_next().await? {
            let mut msg = netlink_packet_route::link::LinkMessage::default();
            msg.header.index = lo.header.index;
            msg.header.flags.insert(LinkFlags::Up);
            msg.header.change_mask.insert(LinkFlags::Up);

            handle
                .link()
                .set(msg)
                .execute()
                .await
                .context("Failed to set lo up")?;
        }

        // Vpeer setup
        // For /31 point-to-point links (RFC 3021), we should NOT set a broadcast address.
        let vpeer_name_str = "vpeer";
        let mut vpeer_links = handle
            .link()
            .get()
            .match_name(vpeer_name_str.to_string())
            .execute();
        if let Some(vpeer) = vpeer_links.try_next().await? {
            // Add IP without broadcast (RFC 3021 for /31)
            Self::add_address_no_broadcast(&handle, vpeer.header.index, veth_vm_ip, 31)
                .await
                .context("Failed to add address to vpeer")?;

            // Set vpeer UP
            let mut link_msg = netlink_packet_route::link::LinkMessage::default();
            link_msg.header.index = vpeer.header.index;
            link_msg.header.flags.insert(LinkFlags::Up);
            link_msg.header.change_mask.insert(LinkFlags::Up);
            handle
                .link()
                .set(link_msg)
                .execute()
                .await
                .context("Failed to set vpeer up")?;
        }

        // Create tap0 interface (Tun/Tap)
        let status = crate::privileges::run_with_scoped_capabilities(
            &[crate::privileges::CAP_NET_ADMIN],
            || {
                Command::new("ip")
                    .args(["tuntap", "add", "tap0", "mode", "tap"])
                    .status()
                    .context("Failed to execute ip tuntap")
            },
        )?;
        if !status.success() {
            return Err(anyhow!("ip tuntap add failed"));
        }

        // Enable tap0 and add strict IP via Netlink
        let mut tap_links = handle.link().get().match_name("tap0".to_string()).execute();
        if let Some(tap) = tap_links.try_next().await? {
            handle
                .address()
                .add(tap.header.index, IpAddr::V4(tap_ip), vm_link_prefix)
                .execute()
                .await
                .context("Failed to add address to tap0")?;

            let mut link_msg = netlink_packet_route::link::LinkMessage::default();
            link_msg.header.index = tap.header.index;
            link_msg.header.flags.insert(LinkFlags::Up);
            link_msg.header.change_mask.insert(LinkFlags::Up);
            handle
                .link()
                .set(link_msg)
                .execute()
                .await
                .context("Failed to set tap0 up")?;
        }

        // Add default route via veth_host_ip (the host side of the veth pair)
        // Using rtnetlink's RouteMessageBuilder API
        let route_msg = rtnetlink::RouteMessageBuilder::<std::net::Ipv4Addr>::new()
            .gateway(veth_host_ip)
            .build();
        handle
            .route()
            .add(route_msg)
            .execute()
            .await
            .context("Failed to add default route")?;

        Ok(())
    }

    /// Builds the kernel `ip=` boot argument for this slot's VM network configuration.
    ///
    /// Format: `ip=<vm_ip>::<tap_ip>:<netmask>:<hostname>:<iface>:<autoconf>:<dns>`
    pub(crate) fn build_ip_boot_arg(&self) -> String {
        let dns_ip = self.guest_dns_server();
        format!(
            "ip={}::{}:{}:instance:eth0:off:{}",
            self.address_plan.vm_ip(),
            self.address_plan.tap_ip(),
            self.address_plan.vm_link_mask(),
            dns_ip
        )
    }

    pub(crate) fn guest_dns_server(&self) -> Ipv4Addr {
        resolve_guest_dns_server()
    }

    pub(crate) fn namespace_path(&self) -> std::path::PathBuf {
        self.netns_dir.join(&self.namespace_id)
    }

    pub(crate) fn set_egress_policy(&self, policy: Option<&SandboxNetworkPolicy>) -> Result<()> {
        let netns_path = self.namespace_path();
        let policy = policy.cloned();
        let handle = thread::spawn(move || -> Result<()> {
            let netns = File::open(&netns_path).with_context(|| {
                format!("failed to open network namespace {}", netns_path.display())
            })?;
            nix::sched::setns(netns.as_fd(), CloneFlags::CLONE_NEWNET)
                .context("failed to enter sandbox network namespace")?;
            set_namespace_egress_policy(policy.as_ref())
        });

        match handle.join() {
            Ok(result) => result,
            Err(e) => Err(anyhow!("egress policy setup thread panicked: {:?}", e)),
        }
    }

    /// Configures iptables rules inside the namespace for VM traffic routing.
    /// This includes:
    /// - Enabling IP forwarding so the namespace can route between tap0 and vpeer.
    /// - FORWARD rules to permit traffic between the VM (tap0) and the host veth (vpeer).
    /// - SNAT/DNAT for host<->VM communication via host_interaction_ip.
    #[tracing::instrument(fields(vm_ip = %vm_ip, host_interaction_ip = %host_interaction_ip, veth_host_ip = %veth_host_ip))]
    fn configure_namespace_iptables_rules(
        host_interaction_ip: Ipv4Addr,
        veth_host_ip: Ipv4Addr,
        vm_ip: Ipv4Addr,
        internal_egress_denied_cidrs: &[String],
    ) -> Result<()> {
        let commands = [
            // FORWARD: Allow traffic from VM (tap0) to host/internet (vpeer).
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "-i tap0 -o vpeer -j ACCEPT".to_string(),
            },
            // FORWARD: Allow established/related traffic from host/internet (vpeer) back to VM (tap0).
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "-i vpeer -o tap0 -m state --state RELATED,ESTABLISHED -j ACCEPT".to_string(),
            },
            // SNAT: Rewrite source IP from the VM to the slot's host interaction IP.
            // This covers both host<->VM communication and internet-bound traffic from the VM.
            // The host then applies its own MASQUERADE to reach the internet.
            IptablesRestoreCommand::Append {
                table: "nat",
                chain: "POSTROUTING",
                rule: format!("-o vpeer -s {} -j SNAT --to {}", vm_ip, host_interaction_ip),
            },
            // DNAT: Rewrite destination IP from the host interaction IP to the VM.
            // This allows the host to reach the VM using the unique HostIP.
            IptablesRestoreCommand::Append {
                table: "nat",
                chain: "PREROUTING",
                rule: format!("-i vpeer -d {} -j DNAT --to {}", host_interaction_ip, vm_ip),
            },
        ];

        apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)?;
        initialize_namespace_egress_chain(
            veth_host_ip,
            resolve_guest_dns_server(),
            internal_egress_denied_cidrs,
        )
    }

    fn tune_neigh_retrans_time_ms(interface: &str) {
        let retrans_path = format!("/proc/sys/net/ipv4/neigh/{interface}/retrans_time_ms");

        for attempt in 0..=NEIGH_SYSCTL_RETRIES {
            match fs::write(&retrans_path, ARP_RETRANS_TIME_MS) {
                Ok(()) => {
                    return;
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::NotFound
                        && attempt < NEIGH_SYSCTL_RETRIES =>
                {
                    debug!(
                        interface,
                        path = %retrans_path,
                        attempt = attempt + 1,
                        error = %err,
                        "ARP retransmit sysctl not ready; retrying"
                    );
                    thread::sleep(Duration::from_millis(NEIGH_SYSCTL_RETRY_DELAY_MS));
                }
                Err(err) => {
                    warn!(
                        interface,
                        path = %retrans_path,
                        error = %err,
                        "failed to configure ARP retransmit delay"
                    );
                    return;
                }
            }
        }
    }

    /// Helper to run async code, handling the case where we might already be in a tokio runtime.
    fn run_async<F, T>(f: F) -> Result<T>
    where
        F: FnOnce() -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let has_current_runtime =
            std::panic::catch_unwind(|| tokio::runtime::Handle::try_current().is_ok())
                .unwrap_or(false);

        if has_current_runtime {
            // We're inside a runtime, spawn a blocking thread to avoid nested runtime
            std::thread::spawn(f)
                .join()
                .map_err(|e| anyhow!("Thread panicked: {:?}", e))?
        } else {
            // Not inside a runtime, we can run directly
            f()
        }
    }

    #[tracing::instrument(
        fields(
            slot = idx,
            host_veth = %Self::host_veth_name(idx),
            veth_host_ip = %veth_host_ip,
            veth_vm_ip = %veth_vm_ip,
            host_interaction_ip = %host_interaction_ip
        )
    )]
    async fn configure_host_interface_async(
        idx: u32,
        veth_host_ip: Ipv4Addr,
        veth_vm_ip: Ipv4Addr,
        host_interaction_ip: Ipv4Addr,
    ) -> Result<()> {
        let (connection, handle, _) = new_connection().context("Netlink connect host")?;
        tokio::spawn(connection);

        let veth_name = Self::host_veth_name(idx);

        // Wait/Check for interface
        let mut links = handle.link().get().match_name(veth_name.clone()).execute();
        if let Some(link) = links.try_next().await? {
            // Add only the veth link IP, not the host_interaction_ip.
            // host_interaction_ip is used as a routing destination, not an interface address
            //
            // For /31 point-to-point links (RFC 3021), we should NOT set a broadcast address.
            Self::add_address_no_broadcast(&handle, link.header.index, veth_host_ip, 31)
                .await
                .context("Failed to add IP to host veth")?;

            // Set UP
            let mut link_msg = netlink_packet_route::link::LinkMessage::default();
            link_msg.header.index = link.header.index;
            link_msg.header.flags.insert(LinkFlags::Up);
            link_msg.header.change_mask.insert(LinkFlags::Up);
            handle
                .link()
                .set(link_msg)
                .execute()
                .await
                .context("Failed to set host veth up")?;

            // Add route from host to namespace: packets destined for host_interaction_ip
            // are forwarded through vpeer IP (in the namespace) as the gateway.
            // This is how the host can reach the VM - the namespace's DNAT rule then
            // translates the destination to the VM's internal IP.
            //
            // Using `ip route add` command as netlink route add with gateway on /31
            // subnet has compatibility issues with some kernel configurations.
            let output = crate::privileges::run_with_scoped_capabilities(
                &[crate::privileges::CAP_NET_ADMIN],
                || {
                    std::process::Command::new("ip")
                        .args([
                            "route",
                            "add",
                            &format!("{}/32", host_interaction_ip),
                            "via",
                            &veth_vm_ip.to_string(),
                            "dev",
                            &veth_name,
                        ])
                        .output()
                        .context("Failed to execute ip route add")
                },
            )?;

            if !output.status.success() {
                return Err(anyhow!(
                    "Failed to add route to {}/32 via {} dev {}: {}",
                    host_interaction_ip,
                    veth_vm_ip,
                    veth_name,
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        } else {
            return Err(anyhow!(
                "Host veth interface {} not found after move",
                veth_name
            ));
        }
        Ok(())
    }

    /// Async helper to delete veth interface.
    /// Idempotent: succeeds even if the interface doesn't exist.
    async fn delete_veth_interface_async(idx: u32) -> Result<()> {
        let (connection, handle, _) = new_connection()?;
        tokio::spawn(connection);

        let veth_name = Self::host_veth_name(idx);
        let mut links = handle.link().get().match_name(veth_name.clone()).execute();

        // try_next returns Err if interface doesn't exist (ENODEV), treat as success
        match links.try_next().await {
            Ok(Some(link)) => {
                // Interface exists, try to delete; ignore "not found" race
                if let Err(e) = handle.link().del(link.header.index).execute().await {
                    let msg = e.to_string();
                    if !msg.contains("No such device") && !msg.contains("ENODEV") {
                        return Err(e.into());
                    }
                }
            }
            Ok(None) => {} // Interface not found
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains("No such device") && !msg.contains("ENODEV") {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    /// Deletes host veth using Tokio-assisted netlink cleanup.
    ///
    /// This is the regular (non-shutdown) cleanup path and preserves the
    /// previous runtime behavior used by normal slot release.
    fn delete_host_veth_interface_with_tokio(idx: u32) -> Result<()> {
        Self::run_async(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow!("Failed to build runtime: {}", e))?;
            rt.block_on(Self::delete_veth_interface_async(idx))
        })
    }

    /// Deletes host veth using `ip link del`.
    ///
    /// Kept synchronous as the fallback path for shutdown/exit cleanup where
    /// Tokio context may already be unavailable.
    fn delete_host_veth_interface_sync(idx: u32) -> Result<()> {
        let veth_name = Self::host_veth_name(idx);
        let (status, stderr_bytes) = crate::privileges::run_with_scoped_capabilities(
            &[crate::privileges::CAP_NET_ADMIN],
            || {
                // Hard timeout: this runs on shutdown/exit cleanup paths that
                // are joined synchronously (e.g. the firecracker pool's dead
                // entry cleanup worker), so a stuck `ip link del` must not
                // block the caller forever.
                const IP_LINK_DEL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
                // stderr goes to a temp file, not a pipe: the polling loop
                // below never drains pipes, so a child whose diagnostics fill
                // a pipe buffer would block on write and be misclassified as
                // a timeout. A file is an unbounded sink, so diagnostics can
                // never stall the child.
                let stderr_path = std::env::temp_dir().join(format!(
                    "agentenv-ip-link-del-{}-{veth_name}.stderr",
                    std::process::id()
                ));
                let stderr_file = File::create(&stderr_path).with_context(|| {
                    format!("Failed to create {}", stderr_path.display())
                })?;
                let mut child = Command::new("ip")
                    .args(["link", "del", &veth_name])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::from(stderr_file))
                    .spawn()
                    .context("Failed to spawn ip link del")?;
                let deadline = std::time::Instant::now() + IP_LINK_DEL_TIMEOUT;
                let status = loop {
                    if let Some(status) = child
                        .try_wait()
                        .context("Failed to poll ip link del")?
                    {
                        break status;
                    }
                    if std::time::Instant::now() >= deadline {
                        child.kill().context("Failed to kill timed-out ip link del")?;
                        // `kill` only sends the termination request; reap
                        // with bounded polling instead of a blocking `wait`
                        // so a process stuck in uninterruptible sleep cannot
                        // hang this shutdown path beyond the grace period.
                        let reap_deadline = std::time::Instant::now()
                            + std::time::Duration::from_secs(1);
                        let mut reaped = false;
                        while std::time::Instant::now() < reap_deadline {
                            if child
                                .try_wait()
                                .context("Failed to reap timed-out ip link del")?
                                .is_some()
                            {
                                reaped = true;
                                break;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(20));
                        }
                        if !reaped {
                            // A child stuck in uninterruptible sleep must not
                            // be dropped without `wait`: dropping `Child` does
                            // not reap it and would leak a zombie. Hand it to
                            // the centralized reaper (a single thread for the
                            // process lifetime) instead of spawning an
                            // unbounded thread per timed-out attempt.
                            hand_off_unreaped_child(child);
                        }
                        let _ = fs::remove_file(&stderr_path);
                        return Err(anyhow!(
                            "ip link del {} timed out after {:?}",
                            veth_name,
                            IP_LINK_DEL_TIMEOUT
                        ));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                };
                // Bounded read of the diagnostics captured in the temp file.
                let mut stderr_bytes = Vec::new();
                if let Ok(file) = File::open(&stderr_path) {
                    let mut limited = std::io::Read::take(file, 64 * 1024);
                    let _ = std::io::Read::read_to_end(&mut limited, &mut stderr_bytes);
                }
                let _ = fs::remove_file(&stderr_path);
                Ok((status, stderr_bytes))
            },
        )?;

        if status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&stderr_bytes);
        let stderr_lower = stderr.to_lowercase();
        if stderr_lower.contains("cannot find device")
            || stderr_lower.contains("no such device")
            || stderr_lower.contains("not found")
        {
            return Ok(());
        }

        Err(anyhow!(
            "Failed to delete veth interface {}: {}",
            veth_name,
            stderr.trim()
        ))
    }

    /// Tries Tokio-assisted veth cleanup first and falls back to synchronous
    /// cleanup on either regular error or panic.
    #[tracing::instrument(fields(slot = idx, host_veth = %Self::host_veth_name(idx)))]
    fn delete_host_veth_interface(idx: u32) -> Result<()> {
        match std::panic::catch_unwind(|| Self::delete_host_veth_interface_with_tokio(idx)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                info!(
                    slot = idx,
                    error = %err,
                    "tokio-assisted slot cleanup failed; falling back to sync cleanup"
                );
                Self::delete_host_veth_interface_sync(idx)
            }
            Err(_) => {
                info!(
                    slot = idx,
                    "tokio-assisted slot cleanup panicked; falling back to sync cleanup"
                );
                Self::delete_host_veth_interface_sync(idx)
            }
        }
    }

    /// Cleans up the network resources for this slot.
    /// This includes deleting the host-side veth interface, removing the network namespace,
    /// and removing the host-side MASQUERADE rule.
    /// Idempotent: safe to call multiple times or concurrently.
    #[tracing::instrument(
        skip(self),
        fields(
            slot = self.idx,
            namespace_id = %self.namespace_id,
            host_veth = %Self::host_veth_name(self.idx),
            host_interaction_ip = %self.host_interaction_ip,
            force_sync
        )
    )]
    pub(super) fn cleanup(&self, force_sync: bool) -> Result<(), NetworkError> {
        // Skip cleanup for slots that never attempted network setup.
        // This avoids touching host networking state for logical-only Slot values.
        if !self.cleanup_armed.swap(false, Ordering::AcqRel) {
            return Ok(());
        }

        // 1. Delete Host Veth Interface (this destroys the pair)
        let delete_result = if force_sync {
            Self::delete_host_veth_interface_sync(self.idx)
        } else {
            Self::delete_host_veth_interface(self.idx)
        };
        if let Err(e) = delete_result {
            self.cleanup_armed.store(true, Ordering::Release);
            return Err(NetworkError::NamespaceError(e));
        }

        // 2. Unmount Netns Bind Mount (may need multiple unmounts if mounted multiple times)
        let netns_path = self.namespace_path();
        let path = netns_path.as_path();
        if path.exists() {
            loop {
                match nix::mount::umount(path) {
                    Ok(_) => continue,
                    Err(nix::errno::Errno::EINVAL) => break, // Not mounted anymore
                    Err(nix::errno::Errno::ENOENT) => break, // File removed by another process
                    Err(e) => {
                        self.cleanup_armed.store(true, Ordering::Release);
                        return Err(NetworkError::NamespaceError(anyhow!(
                            "Failed to unmount netns: {}",
                            e
                        )));
                    }
                }
            }

            // 3. Delete Netns File (ignore NotFound - another process may have deleted it)
            match fs::remove_file(path) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    self.cleanup_armed.store(true, Ordering::Release);
                    return Err(NetworkError::IoError(e));
                }
            }
        }

        Ok(())
    }

    /// Add an IPv4 address to an interface without setting broadcast address.
    /// This is needed for /31 point-to-point links (RFC 3021) where there is no
    /// broadcast address. The rtnetlink crate's AddressMessageBuilder incorrectly
    /// calculates a broadcast address for /31 networks.
    async fn add_address_no_broadcast(
        handle: &Handle,
        if_index: u32,
        addr: Ipv4Addr,
        prefix_len: u8,
    ) -> Result<()> {
        let mut msg = AddressMessage::default();
        msg.header.family = AddressFamily::Inet;
        msg.header.prefix_len = prefix_len;
        msg.header.index = if_index;

        // Add Address and Local attributes (required for IPv4)
        // Do NOT add Broadcast attribute for /31 networks
        msg.attributes
            .push(AddressAttribute::Address(IpAddr::V4(addr)));
        msg.attributes
            .push(AddressAttribute::Local(IpAddr::V4(addr)));

        let mut req = NetlinkMessage::from(RouteNetlinkMessage::NewAddress(msg));
        req.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL;

        let mut response = handle.clone().request(req)?;
        while let Some(message) = response.next().await {
            if let NetlinkPayload::Error(err) = message.payload {
                return Err(anyhow!("Netlink error: {:?}", err));
            }
        }
        Ok(())
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup(true) {
            warn!(slot = self.idx, error = %e, "slot drop cleanup failed");
        }
    }
}

fn resolve_guest_dns_server() -> Ipv4Addr {
    for path in ["/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"] {
        if let Ok(contents) = fs::read_to_string(path) {
            if let Some(ip) = parse_nameserver_ipv4(&contents) {
                return ip;
            }
        }
    }

    let fallback = Ipv4Addr::new(8, 8, 8, 8);
    warn!(dns = %fallback, "falling back to public DNS for guest network");
    fallback
}

fn parse_nameserver_ipv4(contents: &str) -> Option<Ipv4Addr> {
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut parts = line.split_whitespace();
        let Some(directive) = parts.next() else {
            continue;
        };
        if directive != "nameserver" {
            continue;
        }

        let Some(candidate) = parts.next() else {
            continue;
        };
        let ip = match candidate.parse::<Ipv4Addr>() {
            Ok(ip) => ip,
            Err(_) => continue,
        };

        if ip.is_loopback() || ip.is_unspecified() {
            continue;
        }

        return Some(ip);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_slot(idx: u32, address_plan: NetworkAddressPlan) -> Result<Slot, NetworkError> {
        Slot::new(
            idx,
            address_plan,
            std::env::temp_dir().join("aenv-network-tests/netns"),
        )
    }

    fn command_stdout(command: &str, args: &[&str]) -> Option<String> {
        let output = Command::new(command).args(args).output().ok()?;
        if !output.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn host_veth_exists(slot_idx: u32) -> bool {
        let veth_name = Slot::host_veth_name(slot_idx);
        command_stdout("ip", &["-o", "link", "show", &veth_name])
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }

    fn unused_test_slot() -> Slot {
        let address_plan = NetworkAddressPlan::default();
        (30_000..MAX_SLOTS as u32)
            .find(|idx| !host_veth_exists(*idx))
            .and_then(|idx| test_slot(idx, address_plan).ok())
            .expect("failed to find an unused high-numbered network test slot")
    }

    #[test]
    fn test_slot_ip_calculation() {
        let address_plan = NetworkAddressPlan::default();

        // Test Slot 1
        let slot1 = test_slot(1, address_plan).expect("Slot 1 should be valid");
        assert_eq!(slot1.idx, 1);
        assert_eq!(slot1.host_interaction_ip.to_string(), "10.11.0.1");

        // Base 10.12.0.0. Offset 1*2 = 2.
        // Host: .2, VM: .3
        assert_eq!(slot1.veth_host_ip.to_string(), "10.12.0.2");
        assert_eq!(slot1.veth_vm_ip.to_string(), "10.12.0.3");

        // Test Slot 2
        let slot2 = test_slot(2, address_plan).expect("Slot 2 should be valid");
        assert_eq!(slot2.host_interaction_ip.to_string(), "10.11.0.2");
        // Offset 2*2 = 4. Host .4, VM .5
        assert_eq!(slot2.veth_host_ip.to_string(), "10.12.0.4");
        assert_eq!(slot2.veth_vm_ip.to_string(), "10.12.0.5");
    }

    #[test]
    fn custom_address_plan_calculates_slot_ips_and_boot_arg() {
        let config = crate::cfg::NetworkConfig {
            egress: crate::cfg::NetworkEgressConfig::default(),
            internal: crate::cfg::NetworkInternalConfig {
                host_interaction_cidr: "100.64.0.0/16".to_string(),
                veth_cidr: "100.65.0.0/16".to_string(),
            },
        };
        let address_plan = NetworkAddressPlan::from_config(&config).unwrap();
        let slot = test_slot(2, address_plan).expect("slot should be valid");

        assert_eq!(slot.host_interaction_ip.to_string(), "100.64.0.2");
        assert_eq!(slot.veth_host_ip.to_string(), "100.65.0.4");
        assert_eq!(slot.veth_vm_ip.to_string(), "100.65.0.5");
        assert!(slot
            .build_ip_boot_arg()
            .starts_with("ip=169.254.0.21::169.254.0.22:255.255.255.252:"));
    }

    #[test]
    fn test_slot_overflow() {
        let address_plan = NetworkAddressPlan::default();

        // Max valid index is 32767
        let max_valid = 32767;
        let slot = test_slot(max_valid, address_plan);
        assert!(slot.is_ok());

        // 32768 should fail
        let overflow = test_slot(32768, address_plan);
        assert!(overflow.is_err());
        match overflow {
            Err(NetworkError::SlotOutOfRange { idx, max }) => {
                assert_eq!(idx, 32768);
                assert_eq!(max, 32767);
            }
            _ => panic!("Expected SlotOutOfRange error"),
        }
    }

    #[test]
    fn parse_nameserver_ipv4_prefers_non_loopback_ipv4() {
        let conf = r#"
            # generated by systemd-resolved
            nameserver 127.0.0.53
            nameserver 10.0.0.2
            nameserver 8.8.8.8
        "#;
        assert_eq!(
            parse_nameserver_ipv4(conf),
            Some(Ipv4Addr::new(10, 0, 0, 2))
        );
    }

    #[test]
    fn parse_nameserver_ipv4_ignores_non_ipv4_entries() {
        let conf = r#"
            nameserver ::1
            nameserver not_an_ip
            search example.com
        "#;
        assert_eq!(parse_nameserver_ipv4(conf), None);
    }

    #[test]
    fn parse_nameserver_ipv4_accepts_link_local_dns() {
        let conf = "nameserver 169.254.169.253\n";
        assert_eq!(
            parse_nameserver_ipv4(conf),
            Some(Ipv4Addr::new(169, 254, 169, 253))
        );
    }

    #[test]
    fn parse_nameserver_ipv4_skips_malformed_nameserver_lines() {
        let conf = r#"
            nameserver
            nameserver 10.1.2.1
        "#;
        assert_eq!(
            parse_nameserver_ipv4(conf),
            Some(Ipv4Addr::new(10, 1, 2, 1))
        );
    }

    #[test]
    #[ignore = "requires CAP_NET_ADMIN/CAP_SYS_ADMIN and affects system configuration"]
    fn test_network_lifecycle() {
        crate::logging::init_for_tests();

        // Use a free high slot ID to avoid collisions with dev/prod and stale
        // devices from interrupted test runs.
        let slot = unused_test_slot();

        // 1. Create Network
        // This requires CAP_NET_ADMIN and CAP_SYS_ADMIN.
        match slot.create_network() {
            Ok(_) => {}
            Err(e) => {
                // If it fails due to permissions, we skip, otherwise fail
                let err_str = e.to_string();
                if err_str.contains("Operation not permitted") || err_str.contains("EPERM") {
                    println!("Skipping test due to lack of permissions");
                    return;
                }
                panic!("Failed to create network: {:?}", e);
            }
        }

        // 2. Verify Namespace File
        let netns_path = slot.namespace_path();
        assert!(
            netns_path.exists(),
            "Namespace file should exist after creation"
        );

        // 3. Verify Host Veth Interface
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let slot_idx = slot.idx;
        rt.block_on(async {
            let (connection, handle, _) = new_connection().unwrap();
            tokio::spawn(connection);
            let mut links = handle
                .link()
                .get()
                .match_name(Slot::host_veth_name(slot_idx))
                .execute();
            let link = links.try_next().await.unwrap();
            assert!(
                link.is_some(),
                "{} should exist on host",
                Slot::host_veth_name(slot_idx)
            );
        });

        // 4. Cleanup
        let clean_res = slot.cleanup(false);
        assert!(clean_res.is_ok(), "cleanup should succeed");

        // 5. Verify Removal
        assert!(!netns_path.exists(), "Namespace file should be removed");
        rt.block_on(async {
            let (connection, handle, _) = new_connection().unwrap();
            tokio::spawn(connection);
            let mut links = handle
                .link()
                .get()
                .match_name(Slot::host_veth_name(slot_idx))
                .execute();
            // ENODEV (-19) is returned when interface doesn't exist, which is expected
            let link = links.try_next().await.unwrap_or(None);
            assert!(
                link.is_none(),
                "{} should be gone",
                Slot::host_veth_name(slot_idx)
            );
        });
    }
}
