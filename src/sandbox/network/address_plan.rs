use std::net::Ipv4Addr;

use anyhow::{anyhow, bail, Result};
use ipnetwork::Ipv4Network;

use crate::cfg::NetworkConfig;

#[derive(Clone, Copy, Debug)]
pub(crate) struct NetworkAddressPlan {
    host_interaction_cidr: Ipv4Network,
    veth_cidr: Ipv4Network,
    vm_link_cidr: Ipv4Network,
    vm_ip: Ipv4Addr,
    tap_ip: Ipv4Addr,
}

impl NetworkAddressPlan {
    pub(crate) fn from_config(config: &NetworkConfig) -> Result<Self> {
        let internal = NetworkConfig::resolved_internal(config)?;
        Ok(Self {
            host_interaction_cidr: internal.host_interaction_cidr,
            veth_cidr: internal.veth_cidr,
            vm_link_cidr: internal.vm_link_cidr,
            vm_ip: network_ip_at(internal.vm_link_cidr, 1)?,
            tap_ip: network_ip_at(internal.vm_link_cidr, 2)?,
        })
    }

    #[cfg(test)]
    pub(crate) fn default() -> Self {
        Self::from_config(&NetworkConfig::default()).expect("default network address plan is valid")
    }

    pub(crate) fn slot_ips(&self, idx: u32) -> Result<(Ipv4Addr, Ipv4Addr, Ipv4Addr)> {
        let host_interaction_ip = network_ip_at(self.host_interaction_cidr, idx)?;
        let veth_offset = idx
            .checked_mul(2)
            .ok_or_else(|| anyhow!("network slot {idx} veth offset overflow"))?;
        let veth_host_ip = network_ip_at(self.veth_cidr, veth_offset)?;
        let veth_vm_ip = network_ip_at(self.veth_cidr, veth_offset + 1)?;
        Ok((host_interaction_ip, veth_host_ip, veth_vm_ip))
    }

    pub(crate) fn host_interaction_cidr(&self) -> Ipv4Network {
        self.host_interaction_cidr
    }

    pub(crate) fn vm_ip(&self) -> Ipv4Addr {
        self.vm_ip
    }

    pub(crate) fn tap_ip(&self) -> Ipv4Addr {
        self.tap_ip
    }

    pub(super) fn vm_link_prefix(&self) -> u8 {
        self.vm_link_cidr.prefix()
    }

    pub(super) fn vm_link_mask(&self) -> Ipv4Addr {
        self.vm_link_cidr.mask()
    }

    pub(crate) fn conflict_patterns(&self) -> Vec<String> {
        [
            self.host_interaction_cidr,
            self.veth_cidr,
            self.vm_link_cidr,
        ]
        .into_iter()
        .map(network_conflict_pattern)
        .collect()
    }

    pub(crate) fn internal_egress_denied_cidrs(&self) -> Vec<String> {
        // Reject the full internal pools, not just this slot's addresses, so a
        // sandbox cannot target another sandbox's namespace or VM link addresses.
        // Required gateway/DNS exceptions are inserted before these rejects.
        vec![
            self.host_interaction_cidr.to_string(),
            self.veth_cidr.to_string(),
            self.vm_link_cidr.to_string(),
        ]
    }
}

fn network_ip_at(network: Ipv4Network, offset: u32) -> Result<Ipv4Addr> {
    if offset >= network.size() {
        bail!("offset {offset} is outside network {network}");
    }
    Ok(Ipv4Addr::from(u32::from(network.network()) + offset))
}

fn network_conflict_pattern(network: Ipv4Network) -> String {
    // Best-effort warning heuristic for command text like `ip route` and
    // `iptables-save`, not an overlap proof. Non-octet-aligned CIDRs may be
    // under- or over-matched here; runtime allocation uses exact CIDR math.
    let octets = network.network().octets();
    match network.prefix() {
        0..=8 => format!("{}.", octets[0]),
        9..=16 => format!("{}.{}.", octets[0], octets[1]),
        _ => format!("{}.{}.{}.", octets[0], octets[1], octets[2]),
    }
}
