use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxResources {
    pub cpu_count: u32,
    pub memory_mib: u32,
    pub disk_size_mib: u32,
}

impl Default for SandboxResources {
    fn default() -> Self {
        Self {
            cpu_count: 1,
            memory_mib: 128,
            disk_size_mib: 1024,
        }
    }
}

/// Opaque JSON object passed to custom extension hooks.
pub type CustomExtensionParams = serde_json::Map<String, serde_json::Value>;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum VolumeMode {
    ReadOnly,
    #[default]
    Exclusive,
}

/// Node-wide virtualization backend and snapshot compatibility domain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VirtualizationMode {
    #[default]
    Kvm,
    Pvm,
}

impl std::fmt::Display for VirtualizationMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Kvm => "kvm",
            Self::Pvm => "pvm",
        })
    }
}

impl std::str::FromStr for VirtualizationMode {
    type Err = String;

    fn from_str(raw: &str) -> std::result::Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "kvm" => Ok(Self::Kvm),
            "pvm" => Ok(Self::Pvm),
            other => Err(format!(
                "unsupported virtualization mode {other:?}; expected \"kvm\" or \"pvm\""
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VirtualizationMode;

    #[test]
    fn defaults_to_kvm_and_parses_supported_values() {
        assert_eq!(VirtualizationMode::default(), VirtualizationMode::Kvm);
        assert_eq!(
            "KVM".parse::<VirtualizationMode>().unwrap(),
            VirtualizationMode::Kvm
        );
        assert_eq!(
            "pvm".parse::<VirtualizationMode>().unwrap(),
            VirtualizationMode::Pvm
        );
        assert!("nested".parse::<VirtualizationMode>().is_err());
    }
}
