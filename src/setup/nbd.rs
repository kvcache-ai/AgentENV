use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use nix::unistd::{access, AccessFlags};
use tracing::info;

use super::udev;

const NBD_MODULE_PATH: &str = "/sys/module/nbd";
/// Present whenever the module was loaded with the default `nbds_max`.
const FIRST_DEVICE: &str = "/dev/nbd0";

fn nbd_module_loaded() -> bool {
    Path::new(NBD_MODULE_PATH).exists()
}

pub fn provision(group: &str) -> Result<()> {
    if !nbd_module_loaded() {
        let status = Command::new("modprobe")
            .arg("nbd")
            .status()
            .context("run modprobe nbd")?;
        if !status.success() || !nbd_module_loaded() {
            bail!(
                "failed to load the nbd kernel module ({status}); the running kernel must \
                 provide CONFIG_BLK_DEV_NBD"
            );
        }
    }
    udev::persist_module("nbd", "nbd")?;
    udev::unpersist_module("ublk")?;

    info!(group, "installing nbd device access rules");
    // Devices created while the module loaded predate the rules, so replay
    // their add events.
    udev::install_rules(
        "99-agentenv-nbd.rules",
        &format!(
            "KERNEL==\"nbd*\", SUBSYSTEM==\"block\", MODE=\"0660\", GROUP=\"{group}\"\n\
             ACTION==\"add\", SUBSYSTEM==\"block\", ENV{{DEVTYPE}}==\"disk\", KERNEL==\"nbd*\", ATTR{{queue/scheduler}}=\"none\"\n"
        ),
        &[&["--action=add", "--subsystem-match=block", "--sysname-match=nbd*"]],
    )
}

pub fn check() -> Result<()> {
    if !nbd_module_loaded() {
        bail!("nbd kernel module is not loaded; run `server --setup-host` as root");
    }
    // access(2) instead of open: opening an unconfigured device triggers a
    // failing partition scan in the kernel.
    if Path::new(FIRST_DEVICE).exists() {
        access(FIRST_DEVICE, AccessFlags::R_OK | AccessFlags::W_OK).with_context(|| {
            format!(
                "{FIRST_DEVICE} is not readable and writable; run `server --setup-host` as root"
            )
        })?;
    }
    Ok(())
}
