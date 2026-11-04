//! Launch context and runtime versions retained in snapshot records.

use serde::{Deserialize, Serialize};
use shell_util::shell_quote;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandContext {
    pub env_vars: HashMap<String, String>,
    pub workdir: String,
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

impl Default for CommandContext {
    fn default() -> Self {
        Self {
            env_vars: HashMap::new(),
            workdir: "/".to_string(),
            user: None,
            exposed_ports: Vec::new(),
            entrypoint: None,
            cmd: None,
            volumes: Vec::new(),
            labels: HashMap::new(),
        }
    }
}

impl CommandContext {
    pub fn new(env_vars: HashMap<String, String>, workdir: impl Into<String>) -> Self {
        Self {
            env_vars,
            workdir: normalize_workdir(workdir.into()),
            ..Self::default()
        }
    }

    pub fn from_env_and_workdir(
        env_vars: HashMap<String, String>,
        workdir: Option<String>,
    ) -> Self {
        Self::new(env_vars, workdir.unwrap_or_default())
    }

    pub fn with_env_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env_vars.insert(key.into(), value.into());
        self
    }

    pub fn with_env_overrides(mut self, overrides: HashMap<String, String>) -> Self {
        self.env_vars.extend(overrides);
        self
    }

    pub fn with_workdir(mut self, workdir: impl Into<String>) -> Self {
        self.workdir = normalize_workdir(workdir.into());
        self
    }

    pub fn with_user(mut self, user: Option<String>) -> Self {
        self.user = user;
        self
    }

    pub fn with_exposed_ports(mut self, ports: Vec<String>) -> Self {
        self.exposed_ports = ports;
        self
    }

    pub fn with_entrypoint(mut self, entrypoint: Option<Vec<String>>) -> Self {
        self.entrypoint = entrypoint;
        self
    }

    pub fn with_cmd(mut self, cmd: Option<Vec<String>>) -> Self {
        self.cmd = cmd;
        self
    }

    pub fn with_volumes(mut self, volumes: Vec<String>) -> Self {
        self.volumes = volumes;
        self
    }

    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    /// Returns a shell-safe command string combining entrypoint and cmd, or `None` if both are
    /// absent or empty. The result is suitable for passing to `bash -lc`.
    pub fn effective_start_cmd(&self) -> Option<String> {
        let entrypoint = self.entrypoint.as_deref().unwrap_or(&[]);
        let cmd = self.cmd.as_deref().unwrap_or(&[]);
        let parts: Vec<_> = entrypoint.iter().chain(cmd.iter()).collect();
        if parts.is_empty() {
            return None;
        }
        Some(
            parts
                .iter()
                .map(|s| shell_quote(s))
                .collect::<Vec<_>>()
                .join(" "),
        )
    }
}

fn normalize_workdir(workdir: String) -> String {
    if workdir.trim().is_empty() {
        "/".to_string()
    } else {
        workdir
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupCommand {
    pub start_cmd: String,
    pub ready_cmd: String,
    pub context: CommandContext,
    /// Absent for legacy templates, which use a Bash login shell.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell: Option<String>,
}

impl StartupCommand {
    pub fn shell_command(&self) -> (&str, &str) {
        match self.shell.as_deref() {
            Some(shell) => (shell, "-c"),
            None => ("/bin/bash", "-lc"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRuntimeVersions {
    pub kernel_version: String,
    pub firecracker_version: String,
    pub envd_version: String,
    #[serde(default)]
    pub tools_drive_version: String,
}

impl SnapshotRuntimeVersions {
    pub fn new(
        kernel_version: String,
        firecracker_version: String,
        envd_version: String,
        tools_drive_version: String,
    ) -> Self {
        Self {
            kernel_version,
            firecracker_version,
            envd_version,
            tools_drive_version,
        }
    }
}
