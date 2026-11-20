mod address_plan;
mod egress_proxy;
mod iptables_util;
mod manager;
mod policy;
mod resolver;
mod slot;

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;

pub(crate) use address_plan::NetworkAddressPlan;
pub(crate) use manager::{NetworkManager, SlotLease};
pub use policy::{
    BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
    ALL_INTERNET_TRAFFIC_CIDR,
};
pub(crate) use slot::Slot;

pub(crate) const MAX_SLOTS: usize = crate::cfg::network::NETWORK_MAX_SLOTS;
const NETNS_PREFIX: &str = "agentenv-ns-";
const HOST_VETH_PREFIX: &str = "veth-";

/// Whether a previous server process left network namespaces behind. Their
/// host interfaces outlive this process's startup: kept sandboxes still use
/// theirs, and the kernel tears the others down in the background.
static PREVIOUS_PROCESS_LEFT_NETWORKS: AtomicBool = AtomicBool::new(false);

/// Remove the network namespaces a previous server process left under
/// `runtime_path`, except those in `keep`, which still host sandboxes that
/// outlived it.
pub(crate) fn prepare_runtime(runtime_path: &Path, keep: &HashSet<String>) -> anyhow::Result<()> {
    let directory = runtime_path.join("netns");
    std::fs::create_dir_all(&directory)?;
    for entry in std::fs::read_dir(&directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with(NETNS_PREFIX) {
            continue;
        }
        PREVIOUS_PROCESS_LEFT_NETWORKS.store(true, Ordering::Release);
        if keep.contains(name.as_ref()) {
            continue;
        }
        let path = entry.path();
        // Remove every stacked bind mount before deleting the namespace file.
        loop {
            match nix::mount::umount2(&path, nix::mount::MntFlags::MNT_DETACH) {
                Ok(()) => continue,
                Err(nix::errno::Errno::EINVAL | nix::errno::Errno::ENOENT) => break,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("unmount stale AENV network namespace {}", path.display())
                    })
                }
            }
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum NetworkError {
    #[error("Namespace operation failed: {0}")]
    NamespaceError(anyhow::Error),
    #[error("Host iptables operation failed: {0}")]
    HostIptablesError(anyhow::Error),
    #[error("Netlink error: {0}")]
    NetlinkError(#[from] rtnetlink::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("IP network error: {0}")]
    IpNetworkError(#[from] ipnetwork::IpNetworkError),
    #[error("Nix error: {0}")]
    NixError(#[from] nix::Error),
    #[error("Slot index out of range (max {max}): {idx}")]
    SlotOutOfRange { idx: u32, max: u32 },
}
