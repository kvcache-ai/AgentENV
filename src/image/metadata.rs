use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageBaseContext {
    #[serde(default)]
    pub env_vars: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    // OCI image config `User` field forwarded as-is to envd's InitPostRequest.
    // Accepted formats: "username", "uid", "user:group", "uid:gid", "" (root).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exposed_ports: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmd: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
}

impl ImageBaseContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        env_vars: HashMap<String, String>,
        workdir: Option<String>,
        user: Option<String>,
        exposed_ports: Vec<String>,
        entrypoint: Option<Vec<String>>,
        cmd: Option<Vec<String>>,
        volumes: Vec<String>,
        labels: HashMap<String, String>,
    ) -> Self {
        Self {
            env_vars,
            workdir: normalize_optional_string(workdir),
            user: normalize_optional_string(user),
            exposed_ports,
            entrypoint,
            cmd,
            volumes,
            labels,
        }
    }
}

pub(crate) fn env_vars_from_entries(entries: &[String]) -> HashMap<String, String> {
    // Docker-compatible last-wins behavior for duplicate ENV keys.
    entries
        .iter()
        .filter_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            (!key.is_empty()).then(|| (key.to_string(), value.to_string()))
        })
        .collect()
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImageResolutionMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layers: Option<crate::layer::LayerChain>,
    #[serde(default)]
    pub base_context: ImageBaseContext,
    /// Raw source image config JSON, preserved as-is for transparent pass-through
    /// to consumers (e.g. MMDS metadata) without field-level interpretation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_config: Option<serde_json::Value>,
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

impl From<ImageBaseContext> for agentenv_domain::CommandContext {
    fn from(base: ImageBaseContext) -> Self {
        Self::from_env_and_workdir(base.env_vars, base.workdir)
            .with_user(base.user)
            .with_exposed_ports(base.exposed_ports)
            .with_entrypoint(base.entrypoint)
            .with_cmd(base.cmd)
            .with_volumes(base.volumes)
            .with_labels(base.labels)
    }
}
