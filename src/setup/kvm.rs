use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use tracing::info;

use crate::virtualization::VirtualizationMode;

fn kvm_accessible() -> bool {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .is_ok()
}

pub fn add_user_to_group(user: &str, group: &str) -> Result<bool> {
    let groups = Command::new("id")
        .args(["-nG", user])
        .output()
        .with_context(|| format!("look up groups for {user}"))?;
    if !groups.status.success() {
        bail!("failed to look up groups for {user}");
    }
    if String::from_utf8_lossy(&groups.stdout)
        .split_whitespace()
        .any(|current| current == group)
    {
        return Ok(false);
    }

    let status = Command::new("usermod")
        .arg("-aG")
        .arg(group)
        .arg(user)
        .status()
        .context("failed to run usermod")?;
    if !status.success() {
        bail!("failed to add {user} to the {group} group");
    }
    Ok(true)
}

pub fn check(mode: VirtualizationMode) -> Result<()> {
    validate_mode(
        mode,
        std::env::consts::ARCH,
        Path::new("/sys/module/kvm_pvm").exists(),
    )?;

    if !Path::new("/dev/kvm").exists() {
        bail!(
            "KVM device not found (/dev/kvm). \
             Ensure the host virtualization module for {mode} is loaded and exposes /dev/kvm \
             (see the deployment documentation for more information)"
        );
    }

    if kvm_accessible() {
        info!(virtualization_mode = %mode, "/dev/kvm is accessible");
        return Ok(());
    }
    bail!(
        "/dev/kvm is not accessible for read/write; add the runtime user to the kvm group and restart its session"
    )
}

fn validate_mode(mode: VirtualizationMode, arch: &str, pvm_loaded: bool) -> Result<()> {
    if mode == VirtualizationMode::Pvm && arch != "x86_64" {
        bail!("PVM virtualization mode is only supported on x86_64 hosts");
    }

    match mode {
        VirtualizationMode::Kvm if pvm_loaded => bail!(
            "KVM mode cannot start while the kvm_pvm module is loaded; configure virtualization_mode = \"pvm\" or unload kvm_pvm"
        ),
        VirtualizationMode::Pvm if !pvm_loaded => {
            bail!("PVM mode requires the kvm_pvm host module to be loaded")
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kvm_mode_requires_pvm_module_to_be_absent() {
        validate_mode(VirtualizationMode::Kvm, "x86_64", false).unwrap();
        assert!(validate_mode(VirtualizationMode::Kvm, "x86_64", true).is_err());
    }

    #[test]
    fn pvm_mode_requires_x86_64_and_pvm_module() {
        validate_mode(VirtualizationMode::Pvm, "x86_64", true).unwrap();
        assert!(validate_mode(VirtualizationMode::Pvm, "aarch64", true).is_err());
        assert!(validate_mode(VirtualizationMode::Pvm, "x86_64", false).is_err());
    }
}
