use std::process::Command;

use anyhow::{bail, Context, Result};
use tracing::{info, warn};
use uvm_ublk::{load_ublk_module, ublk_module_loaded};

use super::udev;

fn ensure_permissions(current_group: &str) -> Result<()> {
    info!(group = current_group, "installing ublk device access rules");
    let rules = format!(
        "KERNEL==\"ublk-control\", MODE=\"0660\", GROUP=\"{current_group}\"\n\
         KERNEL==\"ublkc*\", MODE=\"0660\", GROUP=\"{current_group}\"\n\
         KERNEL==\"ublkb*\", MODE=\"0660\", GROUP=\"{current_group}\"\n\
         ACTION==\"add\", SUBSYSTEM==\"block\", KERNEL==\"ublkb*\", ATTR{{queue/scheduler}}=\"none\"\n"
    );
    udev::install_rules(
        "99-agentenv-ublk.rules",
        &rules,
        &[
            &["--subsystem-match=misc", "--sysname-match=ublk-control"],
            &["--sysname-match=ublkc*"],
            &["--sysname-match=ublkb*"],
        ],
    )
}

pub fn provision(group: &str) -> Result<()> {
    if !ublk_module_loaded() {
        install_apt_extra_kernel_modules();
    }
    load_ublk_module()?;
    udev::persist_module("ublk", "ublk_drv")?;
    udev::unpersist_module("nbd")?;
    ensure_permissions(group)?;
    Ok(())
}

pub fn check() -> Result<()> {
    if !ublk_module_loaded() {
        bail!("ublk_drv is not loaded; run `server --setup-host` as root");
    }
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/ublk-control")
        .context("open /dev/ublk-control for read/write access")?;
    Ok(())
}

fn install_apt_extra_kernel_modules() {
    if which::which("apt-get").is_err() {
        return;
    }

    let uname = Command::new("uname")
        .args(["-r"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    if uname.is_empty() {
        return;
    }

    let pkg = format!("linux-modules-extra-{uname}");
    if !Command::new("apt-get")
        .args(["install", "-y", pkg.as_str()])
        .status()
        .is_ok_and(|status| status.success())
    {
        warn!("extra kernel modules not found via apt for this kernel");
    }
}
