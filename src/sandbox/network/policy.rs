use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr},
};

use super::iptables_util::{apply_iptables_commands, IptablesRestoreCommand, OpenFailurePolicy};
use crate::cfg::network::normalize_dns_name;

pub const ALL_INTERNET_TRAFFIC_CIDR: &str = "0.0.0.0/0";

const EGRESS_CHAIN: &str = "AGENTENV-EGRESS";
const USER_EGRESS_CHAIN: &str = "AGENTENV-USER-EGRESS";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum BaseSandboxNetworkPolicy {
    /// Allows outbound traffic except for static namespace egress rejects.
    #[default]
    Default,
    Allow,
    Deny,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxNetworkEgressPolicy {
    pub allowed_cidrs: Vec<String>,
    pub allowed_domains: Vec<String>,
    pub denied_cidrs: Vec<String>,
}

impl SandboxNetworkEgressPolicy {
    pub fn new(allow_out: Option<Vec<String>>, deny_out: Option<Vec<String>>) -> Result<Self> {
        let mut policy = Self::default();
        let mut allowed_cidrs = HashSet::new();
        let mut allowed_domains = HashSet::new();
        let mut denied_cidrs = HashSet::new();

        for entry in allow_out.unwrap_or_default() {
            if let Some(cidr) = try_normalize_ip_or_cidr(&entry)? {
                if allowed_cidrs.insert(cidr.clone()) {
                    policy.allowed_cidrs.push(cidr);
                }
            } else {
                let domain = normalize_domain_pattern(&entry)
                    .with_context(|| format!("invalid allowOut domain entry {entry:?}"))?;
                if allowed_domains.insert(domain.clone()) {
                    policy.allowed_domains.push(domain);
                }
            }
        }

        for entry in deny_out.unwrap_or_default() {
            let Some(cidr) = try_normalize_ip_or_cidr(&entry)? else {
                bail!("denyOut entry {entry:?} must be an IP address or CIDR block");
            };
            if denied_cidrs.insert(cidr.clone()) {
                policy.denied_cidrs.push(cidr);
            }
        }

        Ok(policy)
    }

    pub(crate) fn has_explicit_rules(&self) -> bool {
        !self.allowed_cidrs.is_empty()
            || !self.allowed_domains.is_empty()
            || !self.denied_cidrs.is_empty()
    }

    pub(crate) fn has_domain_allow_rules(&self) -> bool {
        !self.allowed_domains.is_empty()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxNetworkPolicy {
    pub base_policy: BaseSandboxNetworkPolicy,
    pub egress: SandboxNetworkEgressPolicy,
}

impl SandboxNetworkPolicy {
    pub fn new(base_policy: BaseSandboxNetworkPolicy, egress: SandboxNetworkEgressPolicy) -> Self {
        Self {
            base_policy,
            egress,
        }
    }

    pub(crate) fn runtime_policy(&self) -> Option<Self> {
        self.has_runtime_egress_rules().then(|| self.clone())
    }

    pub(crate) fn has_explicit_egress_rules(&self) -> bool {
        self.egress.has_explicit_rules()
    }

    pub(crate) fn has_runtime_egress_rules(&self) -> bool {
        self.base_policy == BaseSandboxNetworkPolicy::Deny || self.has_explicit_egress_rules()
    }

    pub(crate) fn has_domain_allow_rules(&self) -> bool {
        self.egress.has_domain_allow_rules()
    }
}

pub(super) fn set_namespace_egress_policy(policy: Option<&SandboxNetworkPolicy>) -> Result<()> {
    let default_policy = SandboxNetworkPolicy::default();
    let policy = policy.unwrap_or(&default_policy);

    if !policy.egress.allowed_domains.is_empty() {
        bail!(
            "domain entries in allowOut require the TCP egress proxy, which is not enabled yet: {:?}",
            policy.egress.allowed_domains
        );
    }

    let commands = build_user_egress_commands(policy, true);
    apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)
}

fn configured_always_denied_cidrs() -> &'static [String] {
    &crate::cfg::ConfigManager::global_config()
        .network
        .egress
        .always_denied_cidrs
}

pub(super) fn initialize_namespace_egress_chain(
    veth_host_ip: Ipv4Addr,
    guest_dns_ip: Ipv4Addr,
    internal_egress_denied_cidrs: &[String],
) -> Result<()> {
    let mut commands = vec![
        IptablesRestoreCommand::NewChain {
            table: "filter",
            chain: EGRESS_CHAIN,
        },
        IptablesRestoreCommand::NewChain {
            table: "filter",
            chain: USER_EGRESS_CHAIN,
        },
        IptablesRestoreCommand::Insert {
            table: "filter",
            chain: "FORWARD",
            position: 1,
            rule: format!("-i tap0 -o vpeer -j {EGRESS_CHAIN}"),
        },
        IptablesRestoreCommand::FlushChain {
            table: "filter",
            chain: EGRESS_CHAIN,
        },
    ];
    commands.extend(build_static_egress_commands(
        veth_host_ip,
        guest_dns_ip,
        internal_egress_denied_cidrs,
        configured_always_denied_cidrs(),
    ));

    apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)
        .context("initialize AgentENV namespace egress iptables chains")
}

fn build_static_egress_commands(
    veth_host_ip: Ipv4Addr,
    guest_dns_ip: Ipv4Addr,
    internal_egress_denied_cidrs: &[String],
    node_always_denied_cidrs: &[String],
) -> Vec<IptablesRestoreCommand> {
    let mut commands = Vec::new();

    for cidr in [format!("{veth_host_ip}/32"), format!("{guest_dns_ip}/32")] {
        commands.push(append_egress_command(format!(
            "-i tap0 -o vpeer -d {cidr} -j ACCEPT"
        )));
    }

    // Internal AgentENV networks are denied before user rules so a sandbox
    // cannot reach another sandbox's namespace or VM link addresses.
    for cidr in internal_egress_denied_cidrs
        .iter()
        .chain(node_always_denied_cidrs.iter())
    {
        commands.push(append_egress_command(format!(
            "-i tap0 -o vpeer -d {cidr} -j REJECT"
        )));
    }

    commands.push(append_egress_command(format!(
        "-i tap0 -o vpeer -j {USER_EGRESS_CHAIN}"
    )));

    commands
}

fn build_user_egress_commands(
    policy: &SandboxNetworkPolicy,
    replace: bool,
) -> Vec<IptablesRestoreCommand> {
    let mut commands = if replace {
        vec![IptablesRestoreCommand::FlushChain {
            table: "filter",
            chain: USER_EGRESS_CHAIN,
        }]
    } else {
        Vec::new()
    };

    for cidr in policy.egress.allowed_cidrs.iter().map(String::as_str) {
        commands.push(append_user_egress_command(format!(
            "-i tap0 -o vpeer -d {cidr} -j ACCEPT"
        )));
    }

    for cidr in policy.egress.denied_cidrs.iter().map(String::as_str) {
        commands.push(append_user_egress_command(format!(
            "-i tap0 -o vpeer -d {cidr} -j REJECT"
        )));
    }

    if policy.base_policy == BaseSandboxNetworkPolicy::Deny {
        commands.push(append_user_egress_command(format!(
            "-i tap0 -o vpeer -d {ALL_INTERNET_TRAFFIC_CIDR} -j REJECT"
        )));
    }

    commands
}

fn append_egress_command(rule: String) -> IptablesRestoreCommand {
    IptablesRestoreCommand::Append {
        table: "filter",
        chain: EGRESS_CHAIN,
        rule,
    }
}

fn append_user_egress_command(rule: String) -> IptablesRestoreCommand {
    IptablesRestoreCommand::Append {
        table: "filter",
        chain: USER_EGRESS_CHAIN,
        rule,
    }
}

fn try_normalize_ip_or_cidr(s: &str) -> Result<Option<String>> {
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Ok(Some(match ip {
            IpAddr::V4(ip) => format!("{ip}/32"),
            IpAddr::V6(ip) => format!("{ip}/128"),
        }));
    }

    match s.parse::<ipnetwork::IpNetwork>() {
        Ok(network) => Ok(Some(network.to_string())),
        Err(err) if s.contains('/') => {
            Err(err).with_context(|| format!("invalid IP or CIDR entry {s:?}"))
        }
        Err(_) => Ok(None),
    }
}

fn normalize_domain_pattern(pattern: &str) -> Result<String> {
    let (wildcard, domain) = pattern
        .strip_prefix("*.")
        .map(|domain| (true, domain))
        .unwrap_or((false, pattern));
    let Some(domain) = normalize_dns_name(domain) else {
        bail!("invalid DNS domain pattern");
    };
    Ok(if wildcard {
        format!("*.{domain}")
    } else {
        domain
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::NetworkConfig;

    fn append_rule(command: &IptablesRestoreCommand) -> Option<&str> {
        match command {
            IptablesRestoreCommand::Append { rule, .. } => Some(rule.as_str()),
            _ => None,
        }
    }

    #[test]
    fn new_splits_allow_cidrs_and_domains() {
        let policy = SandboxNetworkEgressPolicy::new(
            Some(vec![
                "8.8.8.8".to_string(),
                "1.1.1.0/24".to_string(),
                "*.example.com".to_string(),
            ]),
            Some(vec!["203.0.113.0/24".to_string()]),
        )
        .unwrap();

        assert_eq!(policy.allowed_cidrs, ["8.8.8.8/32", "1.1.1.0/24"]);
        assert_eq!(policy.allowed_domains, ["*.example.com"]);
        assert_eq!(policy.denied_cidrs, ["203.0.113.0/24"]);
    }

    #[test]
    fn new_deduplicates_normalized_entries_without_reordering() {
        let policy = SandboxNetworkEgressPolicy::new(
            Some(vec![
                "8.8.8.8".to_string(),
                "1.1.1.1".to_string(),
                "8.8.8.8/32".to_string(),
                "Example.com".to_string(),
                "example.com".to_string(),
            ]),
            Some(vec![
                "203.0.113.1".to_string(),
                "203.0.113.1/32".to_string(),
                "203.0.113.0/24".to_string(),
            ]),
        )
        .unwrap();

        assert_eq!(policy.allowed_cidrs, ["8.8.8.8/32", "1.1.1.1/32"]);
        assert_eq!(policy.allowed_domains, ["example.com"]);
        assert_eq!(policy.denied_cidrs, ["203.0.113.1/32", "203.0.113.0/24"]);
    }

    #[test]
    fn new_sets_base_policy() {
        let policy = SandboxNetworkPolicy::new(
            BaseSandboxNetworkPolicy::Deny,
            SandboxNetworkEgressPolicy::new(Some(vec!["8.8.8.8/32".to_string()]), None).unwrap(),
        );

        assert_eq!(policy.base_policy, BaseSandboxNetworkPolicy::Deny);
        assert!(policy.egress.denied_cidrs.is_empty());
        assert!(policy.has_runtime_egress_rules());
    }

    #[test]
    fn build_rules_keeps_allow_before_deny() {
        let policy = SandboxNetworkPolicy {
            base_policy: BaseSandboxNetworkPolicy::Deny,
            egress: SandboxNetworkEgressPolicy {
                allowed_cidrs: vec!["8.8.8.8/32".to_string()],
                denied_cidrs: Vec::new(),
                allowed_domains: Vec::new(),
            },
        };

        let commands = build_user_egress_commands(&policy, false);

        let allow_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 8.8.8.8/32 -j ACCEPT")
            })
            .unwrap();
        let deny_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 0.0.0.0/0 -j REJECT")
            })
            .unwrap();
        assert!(allow_pos < deny_pos);
    }

    #[test]
    fn build_policy_replacement_flushes_before_installing_rules() {
        let policy = SandboxNetworkPolicy {
            base_policy: BaseSandboxNetworkPolicy::Deny,
            egress: SandboxNetworkEgressPolicy {
                allowed_cidrs: vec!["8.8.8.8/32".to_string()],
                denied_cidrs: vec!["203.0.113.0/24".to_string()],
                allowed_domains: Vec::new(),
            },
        };

        let commands = build_user_egress_commands(&policy, true);

        assert!(matches!(
            commands.first(),
            Some(IptablesRestoreCommand::FlushChain {
                table: "filter",
                chain: USER_EGRESS_CHAIN,
            })
        ));
        assert_eq!(
            commands.iter().filter_map(append_rule).collect::<Vec<_>>(),
            [
                "-i tap0 -o vpeer -d 8.8.8.8/32 -j ACCEPT",
                "-i tap0 -o vpeer -d 203.0.113.0/24 -j REJECT",
                "-i tap0 -o vpeer -d 0.0.0.0/0 -j REJECT",
            ]
        );
    }

    #[test]
    fn build_default_policy_replacement_only_flushes_user_chain() {
        let commands = build_user_egress_commands(&SandboxNetworkPolicy::default(), true);

        assert!(matches!(
            commands.as_slice(),
            [IptablesRestoreCommand::FlushChain {
                table: "filter",
                chain: USER_EGRESS_CHAIN,
            }]
        ));
    }

    #[test]
    fn build_static_rules_include_baseline_in_order() {
        let denied_cidrs = NetworkConfig::default().egress.always_denied_cidrs;
        let internal_egress_denied_cidrs = Vec::new();
        let commands = build_static_egress_commands(
            Ipv4Addr::new(10, 12, 0, 2),
            Ipv4Addr::new(10, 1, 2, 1),
            &internal_egress_denied_cidrs,
            &denied_cidrs,
        );

        let host_allow_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 10.12.0.2/32 -j ACCEPT")
            })
            .unwrap();
        assert!(commands.iter().any(
            |command| append_rule(command) == Some("-i tap0 -o vpeer -d 10.1.2.1/32 -j ACCEPT")
        ));
        let hard_deny_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 10.0.0.0/8 -j REJECT")
            })
            .unwrap();
        let shared_deny_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 100.64.0.0/10 -j REJECT")
            })
            .unwrap();
        let user_chain_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -j AGENTENV-USER-EGRESS")
            })
            .unwrap();

        assert!(host_allow_pos < hard_deny_pos);
        assert!(hard_deny_pos < shared_deny_pos);
        assert!(hard_deny_pos < user_chain_pos);
        assert!(shared_deny_pos < user_chain_pos);
    }

    #[test]
    fn build_static_rules_use_configured_denied_cidrs() {
        let denied_cidrs = vec!["203.0.113.0/24".to_string()];
        let internal_egress_denied_cidrs = Vec::new();
        let commands = build_static_egress_commands(
            Ipv4Addr::new(10, 12, 0, 2),
            Ipv4Addr::new(10, 1, 2, 1),
            &internal_egress_denied_cidrs,
            &denied_cidrs,
        );

        assert!(commands.iter().any(|command| {
            append_rule(command) == Some("-i tap0 -o vpeer -d 203.0.113.0/24 -j REJECT")
        }));
        assert!(!commands.iter().any(
            |command| append_rule(command) == Some("-i tap0 -o vpeer -d 10.0.0.0/8 -j REJECT")
        ));
    }
}
