use anyhow::{bail, Context, Result};
use confique::Config;
use ipnetwork::Ipv4Network;

pub(crate) const NETWORK_MAX_SLOTS: usize = 32768;

// Part of the snapshot ABI: fresh boots pass this VM/tap link through the
// kernel `ip=` argument, and snapshot resume does not re-run boot args. Do not
// make this configurable unless vmLinkCidr is persisted in snapshot manifests
// and resume handles missing metadata for old snapshots.
const FIXED_NETWORK_VM_LINK_CIDR: &str = "169.254.0.20/30";

#[derive(Debug, Config, Clone)]
pub struct NetworkConfig {
    #[config(nested)]
    pub egress: NetworkEgressConfig,
    #[config(nested)]
    pub internal: NetworkInternalConfig,
}

#[derive(Debug, Config, Clone)]
pub struct NetworkEgressConfig {
    /// Node-level destinations that sandbox egress policy cannot override.
    #[config(default = [
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.168.0.0/16",
    ])]
    pub always_denied_cidrs: Vec<String>,
}

#[derive(Debug, Config, Clone)]
pub struct NetworkInternalConfig {
    /// CIDR used for per-slot host interaction addresses.
    #[config(default = "10.11.0.0/16")]
    pub host_interaction_cidr: String,
    /// CIDR used for per-slot namespace veth pairs.
    #[config(default = "10.12.0.0/16")]
    pub veth_cidr: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ResolvedNetworkInternalConfig {
    pub(crate) host_interaction_cidr: Ipv4Network,
    pub(crate) veth_cidr: Ipv4Network,
    pub(crate) vm_link_cidr: Ipv4Network,
}

impl NetworkConfig {
    pub(crate) fn validate(config: &Self) -> Result<()> {
        for cidr in &config.egress.always_denied_cidrs {
            cidr.parse::<Ipv4Network>().with_context(|| {
                format!("invalid network.egress.always_denied_cidrs entry {cidr:?}")
            })?;
        }

        Self::resolved_internal(config)?;
        Ok(())
    }

    pub(crate) fn resolved_internal(config: &Self) -> Result<ResolvedNetworkInternalConfig> {
        let host_interaction_cidr = config
            .internal
            .host_interaction_cidr
            .as_str()
            .parse::<Ipv4Network>()
            .context("invalid network.internal.host_interaction_cidr")?;
        let veth_cidr = config
            .internal
            .veth_cidr
            .as_str()
            .parse::<Ipv4Network>()
            .context("invalid network.internal.veth_cidr")?;
        let vm_link_cidr = FIXED_NETWORK_VM_LINK_CIDR
            .parse::<Ipv4Network>()
            .context("invalid fixed VM link CIDR")?;
        if vm_link_cidr.prefix() != 30 {
            bail!("fixed VM link CIDR must be a /30 network");
        }

        let max_slots = NETWORK_MAX_SLOTS as u32;
        let max_slot_index = max_slots - 1;
        if host_interaction_cidr.size() < max_slots {
            bail!(
                "network.internal.host_interaction_cidr ({host_interaction_cidr}) must contain at least {max_slots} addresses to cover slot indexes 1..={max_slot_index}; slot 0 is reserved"
            );
        }
        if veth_cidr.size() < max_slots * 2 {
            bail!(
                "network.internal.veth_cidr ({veth_cidr}) must contain at least {} addresses to cover two veth addresses per slot through slot {max_slot_index}",
                max_slots * 2
            );
        }

        for (left_name, left, right_name, right) in [
            (
                "network.internal.host_interaction_cidr",
                host_interaction_cidr,
                "network.internal.veth_cidr",
                veth_cidr,
            ),
            (
                "network.internal.host_interaction_cidr",
                host_interaction_cidr,
                "fixed VM link CIDR",
                vm_link_cidr,
            ),
            (
                "network.internal.veth_cidr",
                veth_cidr,
                "fixed VM link CIDR",
                vm_link_cidr,
            ),
        ] {
            if left.overlaps(right) {
                bail!("{left_name} ({left}) must not overlap {right_name} ({right})");
            }
        }

        Ok(ResolvedNetworkInternalConfig {
            host_interaction_cidr,
            veth_cidr,
            vm_link_cidr,
        })
    }
}

super::impl_config_default!(NetworkConfig, NetworkEgressConfig, NetworkInternalConfig);

pub(crate) fn normalize_dns_name(domain: &str) -> Option<String> {
    let domain = domain.to_ascii_lowercase();
    is_valid_dns_name(&domain).then_some(domain)
}

fn is_valid_dns_name(domain: &str) -> bool {
    const MAX_DNS_NAME_LEN: usize = 253;
    !domain.is_empty()
        && domain.len() <= MAX_DNS_NAME_LEN
        && domain.split('.').all(is_valid_dns_label)
}

fn is_valid_dns_label(label: &str) -> bool {
    const MAX_DNS_LABEL_LEN: usize = 63;
    let bytes = label.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_DNS_LABEL_LEN {
        return false;
    }
    if !matches!(bytes[0], b'a'..=b'z' | b'0'..=b'9')
        || !matches!(bytes[bytes.len() - 1], b'a'..=b'z' | b'0'..=b'9')
    {
        return false;
    }
    bytes
        .iter()
        .skip(1)
        .take(bytes.len().saturating_sub(2))
        .all(|byte| matches!(*byte, b'a'..=b'z' | b'0'..=b'9' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_custom_network_config() -> Result<()> {
        let config = NetworkConfig {
            egress: NetworkEgressConfig {
                always_denied_cidrs: vec!["127.0.0.0/8".to_string(), "169.254.0.0/16".to_string()],
            },
            internal: NetworkInternalConfig {
                host_interaction_cidr: "100.64.0.0/16".to_string(),
                veth_cidr: "100.65.0.0/16".to_string(),
            },
        };

        NetworkConfig::validate(&config)
    }
}
