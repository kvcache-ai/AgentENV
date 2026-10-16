use std::fs;

use tracing::warn;
use uuid::Uuid;

use crate::cfg::NodeIdentityConfig;

fn build_commit() -> &'static str {
    match option_env!("AENV_GIT_COMMIT") {
        Some(commit) if !commit.is_empty() => commit,
        _ => "unknown",
    }
}

#[derive(Clone, Debug)]
pub struct NodeIdentity {
    pub id: String,
    pub cluster_id: Uuid,
    pub service_instance_id: String,
    pub commit: String,
    pub version: String,
}

impl NodeIdentity {
    /// Resolves stable node identity fields used by the node/admin APIs.
    ///
    /// Values come from environment/config overrides when present, otherwise from
    /// hostname- or process-derived fallbacks. The build commit is injected at
    /// compile time rather than read from runtime configuration.
    pub fn from_config(config: &NodeIdentityConfig) -> Self {
        let hostname = read_hostname().unwrap_or_else(|| "unknown".to_string());
        let id = config
            .node_id
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| hostname.clone());

        Self {
            id,
            cluster_id: parse_uuid_with_fallback(
                "node_identity.cluster_id",
                config.cluster_id.as_deref(),
            ),
            service_instance_id: config
                .service_instance_id
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| Uuid::now_v7().to_string()),
            commit: build_commit().to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

fn parse_uuid_with_fallback(field: &str, config_value: Option<&str>) -> Uuid {
    match config_value {
        Some(raw) => match Uuid::parse_str(raw) {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!(
                    config_field = field,
                    value = %raw,
                    error = %err,
                    "invalid UUID for node identity"
                );
                Uuid::nil()
            }
        },
        None => Uuid::nil(),
    }
}

fn read_hostname() -> Option<String> {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            fs::read_to_string("/proc/sys/kernel/hostname")
                .ok()
                .map(|hostname| hostname.trim().to_string())
                .filter(|hostname| !hostname.is_empty())
        })
        .or_else(|| {
            fs::read_to_string("/etc/hostname")
                .ok()
                .map(|hostname| hostname.trim().to_string())
                .filter(|hostname| !hostname.is_empty())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observability_commit_uses_build_time_injection() {
        let runtime_override = "runtime-override-commit";
        let previous = std::env::var("AENV_GIT_COMMIT").ok();
        unsafe {
            std::env::set_var("AENV_GIT_COMMIT", runtime_override);
        }

        let identity = NodeIdentity::from_config(&NodeIdentityConfig::default());
        let expected = option_env!("AENV_GIT_COMMIT")
            .filter(|commit| !commit.is_empty())
            .unwrap_or("unknown");

        assert_ne!(identity.commit, runtime_override);
        assert_eq!(identity.commit, expected);

        match previous {
            Some(value) => unsafe {
                std::env::set_var("AENV_GIT_COMMIT", value);
            },
            None => unsafe {
                std::env::remove_var("AENV_GIT_COMMIT");
            },
        }
    }
}
