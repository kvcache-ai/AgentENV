use serde::{Deserialize, Serialize};

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
