use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr},
    sync::OnceLock,
};

use super::iptables_util::{apply_iptables_commands, IptablesRestoreCommand, OpenFailurePolicy};
use crate::cfg::network::normalize_dns_name;

pub const ALL_INTERNET_TRAFFIC_CIDR: &str = "0.0.0.0/0";

const EGRESS_CHAIN: &str = "AGENTENV-EGRESS";
const USER_EGRESS_CHAIN: &str = "AGENTENV-USER-EGRESS";
const EGRESS_PROXY_CHAIN: &str = "AGENTENV-EGRESS-PROXY";
const DOMAIN_INSPECTION_TCP_PORTS: &[u16] = &[80, 443];

static PLATFORM_DENIED_CIDRS: OnceLock<Box<[String]>> = OnceLock::new();

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxNetworkPolicy {
    #[serde(default = "default_allow_public_traffic")]
    pub allow_public_traffic: bool,
    pub base_policy: BaseSandboxNetworkPolicy,
    pub egress: SandboxNetworkEgressPolicy,
}

fn default_allow_public_traffic() -> bool {
    true
}

impl Default for SandboxNetworkPolicy {
    fn default() -> Self {
        Self {
            allow_public_traffic: true,
            base_policy: BaseSandboxNetworkPolicy::default(),
            egress: SandboxNetworkEgressPolicy::default(),
        }
    }
}

impl SandboxNetworkPolicy {
    pub fn new(
        allow_public_traffic: bool,
        base_policy: BaseSandboxNetworkPolicy,
        egress: SandboxNetworkEgressPolicy,
    ) -> Self {
        Self {
            allow_public_traffic,
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

    /// Whether this policy needs traffic interception by the namespace-local
    /// egress proxy. Domain allowlists are the first proxy-backed capability;
    /// future proxy capabilities should extend this decision at this boundary.
    pub(crate) fn requires_egress_proxy(&self) -> bool {
        self.has_domain_allow_rules()
    }

    /// TCP destination ports intercepted for the proxy-backed capabilities in this policy.
    pub(crate) fn egress_proxy_tcp_ports(&self) -> &'static [u16] {
        if self.has_domain_allow_rules() {
            DOMAIN_INSPECTION_TCP_PORTS
        } else {
            &[]
        }
    }

    /// Decide whether an original IP destination may leave the sandbox.
    /// The runtime dataplane currently supplies IPv4 original destinations;
    /// retain IPv6 CIDRs for wire compatibility without pretending this
    /// method provides IPv6 iptables enforcement.
    pub(crate) fn is_ip_allowed(&self, ip: Ipv4Addr) -> bool {
        if self.is_absolutely_denied(ip) {
            return false;
        }
        if self
            .egress
            .allowed_cidrs
            .iter()
            .any(|cidr| cidr_contains(cidr, ip))
        {
            return true;
        }
        if self
            .egress
            .denied_cidrs
            .iter()
            .any(|cidr| cidr_contains(cidr, ip))
        {
            return false;
        }
        // A domain allowlist is an opt-in capability. If the inspected host is
        // absent or does not match, do not fall back to the base policy.
        if self.has_domain_allow_rules() {
            return false;
        }
        self.base_policy != BaseSandboxNetworkPolicy::Deny
    }

    /// Decide whether a host-side resolved address may leave the sandbox for
    /// an allowed domain. Passing `None` checks only the hostname branch before
    /// DNS resolution; the proxy passes `Some(resolved_ip)` before connecting.
    /// User CIDR denies do not override an explicit domain allow, matching
    /// E2B's allow precedence; absolute platform and slot denies remain
    /// effective.
    pub(crate) fn is_domain_allowed(&self, hostname: &str, resolved_ip: Option<Ipv4Addr>) -> bool {
        if hostname.is_empty()
            || !self
                .egress
                .allowed_domains
                .iter()
                .any(|pattern| domain_matches(hostname, pattern))
        {
            return false;
        }
        resolved_ip.is_none_or(|ip| !self.is_absolutely_denied(ip))
    }

    fn is_absolutely_denied(&self, ip: Ipv4Addr) -> bool {
        PLATFORM_DENIED_CIDRS
            .get_or_init(|| {
                let config = crate::cfg::ConfigManager::global_config();
                let address_plan = super::NetworkAddressPlan::from_config(&config.network)
                    .expect("validated network config should produce an address plan");
                config
                    .network
                    .egress
                    .always_denied_cidrs
                    .iter()
                    .cloned()
                    .chain(address_plan.internal_egress_denied_cidrs())
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
            })
            .iter()
            .any(|cidr| cidr_contains(cidr, ip))
    }
}

pub(super) fn set_namespace_egress_policy(
    policy: Option<&SandboxNetworkPolicy>,
    egress_proxy_port: u16,
) -> Result<()> {
    let default_policy = SandboxNetworkPolicy::default();
    let policy = policy.unwrap_or(&default_policy);

    let mut commands = build_user_egress_commands(policy, true);
    commands.extend(build_egress_proxy_commands(policy, egress_proxy_port));
    apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)
}

fn configured_always_denied_cidrs() -> &'static [String] {
    &crate::cfg::ConfigManager::global_config()
        .network
        .egress
        .always_denied_cidrs
}

fn domain_matches(hostname: &str, pattern: &str) -> bool {
    let hostname = hostname.trim_end_matches('.');
    let pattern = pattern.trim_end_matches('.');
    pattern == "*"
        || pattern.eq_ignore_ascii_case(hostname)
        || pattern.strip_prefix("*.").is_some_and(|suffix| {
            let Some(suffix_start) = hostname.len().checked_sub(suffix.len()) else {
                return false;
            };
            let Some(prefix) = hostname.get(..suffix_start) else {
                return false;
            };
            let Some(host_suffix) = hostname.get(suffix_start..) else {
                return false;
            };
            !prefix.is_empty() && prefix.ends_with('.') && host_suffix.eq_ignore_ascii_case(suffix)
        })
}

fn cidr_contains(cidr: &str, ip: Ipv4Addr) -> bool {
    cidr.parse::<ipnetwork::Ipv4Network>()
        .map(|network| network.contains(ip))
        .unwrap_or(false)
}

pub(super) fn initialize_namespace_egress_chain(
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
        guest_dns_ip,
        internal_egress_denied_cidrs,
        configured_always_denied_cidrs(),
    ));
    commands.extend([
        IptablesRestoreCommand::NewChain {
            table: "nat",
            chain: EGRESS_PROXY_CHAIN,
        },
        IptablesRestoreCommand::Insert {
            table: "nat",
            chain: "PREROUTING",
            position: 1,
            rule: format!("-i tap0 -j {EGRESS_PROXY_CHAIN}"),
        },
        IptablesRestoreCommand::FlushChain {
            table: "nat",
            chain: EGRESS_PROXY_CHAIN,
        },
    ]);

    apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)
        .context("initialize AgentENV namespace egress iptables chains")
}

fn build_static_egress_commands(
    guest_dns_ip: Ipv4Addr,
    internal_egress_denied_cidrs: &[String],
    node_always_denied_cidrs: &[String],
) -> Vec<IptablesRestoreCommand> {
    let mut commands = Vec::new();

    // Host-initiated proxy/envd connections return through this chain after the
    // namespace SNATs the guest response to its host interaction address.
    commands.push(append_egress_command(
        "-i tap0 -o vpeer -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT".to_string(),
    ));

    // Allow DNS traffic to the guest DNS server.
    for protocol in ["udp", "tcp"] {
        commands.push(append_egress_command(format!(
            "-i tap0 -o vpeer -d {guest_dns_ip}/32 -p {protocol} --dport 53 -j ACCEPT"
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

fn build_egress_proxy_commands(
    policy: &SandboxNetworkPolicy,
    egress_proxy_port: u16,
) -> Vec<IptablesRestoreCommand> {
    let mut commands = vec![IptablesRestoreCommand::FlushChain {
        table: "nat",
        chain: EGRESS_PROXY_CHAIN,
    }];
    for port in policy.egress_proxy_tcp_ports() {
        commands.push(IptablesRestoreCommand::Append {
            table: "nat",
            chain: EGRESS_PROXY_CHAIN,
            rule: format!(
                "-i tap0 -p tcp --dport {port} -j REDIRECT --to-ports {egress_proxy_port}"
            ),
        });
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
    fn new_sets_explicit_policy() {
        let policy = SandboxNetworkPolicy::new(
            false,
            BaseSandboxNetworkPolicy::Deny,
            SandboxNetworkEgressPolicy::new(Some(vec!["8.8.8.8/32".to_string()]), None).unwrap(),
        );

        assert!(!policy.allow_public_traffic);
        assert_eq!(policy.base_policy, BaseSandboxNetworkPolicy::Deny);
        assert!(policy.egress.denied_cidrs.is_empty());
        assert!(policy.has_runtime_egress_rules());
    }

    #[test]
    fn missing_ingress_policy_deserializes_as_public() {
        let mut value = serde_json::to_value(SandboxNetworkPolicy::default()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("allow_public_traffic");

        let policy: SandboxNetworkPolicy = serde_json::from_value(value).unwrap();

        assert!(policy.allow_public_traffic);
    }

    #[test]
    fn build_rules_keeps_allow_before_deny() {
        let policy = SandboxNetworkPolicy {
            allow_public_traffic: true,
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
            allow_public_traffic: true,
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
            Ipv4Addr::new(10, 1, 2, 1),
            &internal_egress_denied_cidrs,
            &denied_cidrs,
        );

        assert_eq!(
            append_rule(&commands[0]),
            Some("-i tap0 -o vpeer -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT")
        );
        let established_pos = commands
            .iter()
            .position(|command| {
                append_rule(command)
                    == Some("-i tap0 -o vpeer -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT")
            })
            .unwrap();
        let dns_udp_pos = commands
            .iter()
            .position(|command| {
                append_rule(command)
                    == Some("-i tap0 -o vpeer -d 10.1.2.1/32 -p udp --dport 53 -j ACCEPT")
            })
            .unwrap();
        let dns_tcp_pos = commands
            .iter()
            .position(|command| {
                append_rule(command)
                    == Some("-i tap0 -o vpeer -d 10.1.2.1/32 -p tcp --dport 53 -j ACCEPT")
            })
            .unwrap();
        assert!(commands.iter().any(|command| {
            append_rule(command)
                == Some("-i tap0 -o vpeer -d 10.1.2.1/32 -p udp --dport 53 -j ACCEPT")
        }));
        assert!(commands.iter().any(|command| {
            append_rule(command)
                == Some("-i tap0 -o vpeer -d 10.1.2.1/32 -p tcp --dport 53 -j ACCEPT")
        }));
        assert!(!commands.iter().any(|command| {
            append_rule(command) == Some("-i tap0 -o vpeer -d 10.12.0.2/32 -j ACCEPT")
        }));
        let hard_deny_pos = commands
            .iter()
            .position(|command| {
                append_rule(command) == Some("-i tap0 -o vpeer -d 10.0.0.0/8 -j REJECT")
            })
            .unwrap();
        assert!(established_pos < hard_deny_pos);
        assert!(dns_udp_pos < hard_deny_pos);
        assert!(dns_tcp_pos < hard_deny_pos);
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

        assert!(hard_deny_pos < shared_deny_pos);
        assert!(hard_deny_pos < user_chain_pos);
        assert!(shared_deny_pos < user_chain_pos);
    }

    #[test]
    fn build_static_rules_use_configured_denied_cidrs() {
        let denied_cidrs = vec!["203.0.113.0/24".to_string()];
        let internal_egress_denied_cidrs = Vec::new();
        let commands = build_static_egress_commands(
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

    #[test]
    fn egress_proxy_redirects_domain_inspection_ports() {
        let policy = SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Deny,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["example.com".to_string()]),
                Some(vec![ALL_INTERNET_TRAFFIC_CIDR.to_string()]),
            )
            .unwrap(),
        );
        let commands = build_egress_proxy_commands(&policy, 43210);
        let rules = commands.iter().filter_map(append_rule).collect::<Vec<_>>();
        assert_eq!(rules.len(), 2);
        assert!(rules[0].contains("--dport 80"));
        assert!(rules[1].contains("--dport 443"));
        assert!(rules.iter().all(|rule| rule.contains("--to-ports 43210")));
    }

    #[test]
    fn egress_proxy_chain_is_cleared_when_policy_needs_no_proxy() {
        let commands = build_egress_proxy_commands(&SandboxNetworkPolicy::default(), 43210);
        assert!(matches!(
            commands.as_slice(),
            [IptablesRestoreCommand::FlushChain {
                table: "nat",
                chain: EGRESS_PROXY_CHAIN,
            }]
        ));
    }

    #[test]
    fn authorization_uses_platform_denied_ranges() {
        let policy = SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Allow,
            SandboxNetworkEgressPolicy::default(),
        );

        assert!(policy.is_ip_allowed(Ipv4Addr::new(198, 51, 100, 10)));
        assert!(!policy.is_ip_allowed(Ipv4Addr::new(10, 12, 1, 1)));
    }

    #[test]
    fn domain_authorization_checks_hostname_and_resolved_ip() {
        let policy = SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Default,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["example.com".to_string()]),
                Some(Vec::new()),
            )
            .unwrap(),
        );

        assert!(policy.is_domain_allowed("example.com", None));
        assert!(policy.is_domain_allowed("example.com", Some(Ipv4Addr::new(192, 0, 2, 10))));
        assert!(!policy.is_domain_allowed("other.example", None));
        assert!(!policy.is_domain_allowed("example.com", Some(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(!policy.is_domain_allowed("example.com", Some(Ipv4Addr::new(10, 12, 1, 1))));
    }

    #[test]
    fn domain_matching_handles_non_ascii_hostnames_without_panicking() {
        assert!(std::panic::catch_unwind(|| domain_matches("é.com", "*.com")).is_ok());
        assert!(
            std::panic::catch_unwind(|| domain_matches("é.example.com", "*.example.com")).is_ok()
        );
        assert!(domain_matches("API.Example.COM", "*.example.com"));
    }
}
