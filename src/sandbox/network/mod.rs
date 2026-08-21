mod address_plan;
mod egress_proxy;
mod iptables_util;
mod manager;
mod policy;
mod slot;

use std::path::Path;

use anyhow::Context;

pub(crate) use address_plan::NetworkAddressPlan;
pub(crate) use manager::NetworkManager;
pub use policy::{
    BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
    ALL_INTERNET_TRAFFIC_CIDR,
};
pub(crate) use slot::Slot;

pub(crate) const MAX_SLOTS: usize = crate::cfg::network::NETWORK_MAX_SLOTS;
const NETNS_PREFIX: &str = "agentenv-ns-";
const HOST_VETH_PREFIX: &str = "veth-";

pub(crate) fn prepare_runtime(runtime_path: &Path) -> anyhow::Result<()> {
    let directory = runtime_path.join("netns");
    std::fs::create_dir_all(&directory)?;
    for entry in std::fs::read_dir(&directory)? {
        let entry = entry?;
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with(NETNS_PREFIX) {
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
