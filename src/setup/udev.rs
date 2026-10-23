//! udev rules and module persistence shared by the block device backends.

use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use tracing::warn;

const UDEV_RULES_DIR: &str = "/etc/udev/rules.d";
const MODULES_LOAD_DIR: &str = "/etc/modules-load.d";

/// Install `rules` as `/etc/udev/rules.d/<file_name>` and reload udev, then
/// re-trigger each `triggers` argument list so existing devices pick the
/// rules up. Skips what the host's udev setup does not support.
pub(super) fn install_rules(file_name: &str, rules: &str, triggers: &[&[&str]]) -> Result<()> {
    if Path::new(UDEV_RULES_DIR).exists() {
        let path = Path::new(UDEV_RULES_DIR).join(file_name);
        std::fs::write(
            &path,
            format!("# Managed by agentenv server setup\n{rules}"),
        )
        .with_context(|| format!("install {}", path.display()))?;
    } else {
        warn!(
            rules_dir = UDEV_RULES_DIR,
            file_name, "udev rules directory not present; skipping persistent rule install"
        );
    }

    if which::which("udevadm").is_err() {
        warn!(
            file_name,
            "udevadm not found; device permissions will apply after udev reloads rules"
        );
        return Ok(());
    }
    let status = Command::new("udevadm")
        .args(["control", "--reload-rules"])
        .status()
        .context("reload udev rules")?;
    if !status.success() {
        bail!("udevadm control --reload-rules failed with {status}");
    }
    for args in triggers {
        let _ = Command::new("udevadm").arg("trigger").args(*args).status();
    }
    let _ = Command::new("udevadm").arg("settle").status();
    Ok(())
}

/// Load `module` at boot via `/etc/modules-load.d/aenv-<name>.conf`.
pub(super) fn persist_module(name: &str, module: &str) -> Result<()> {
    std::fs::create_dir_all(MODULES_LOAD_DIR).context("create /etc/modules-load.d")?;
    let path = format!("{MODULES_LOAD_DIR}/aenv-{name}.conf");
    std::fs::write(&path, format!("{module}\n")).with_context(|| format!("install {path}"))
}

/// Stop loading the module persisted by [`persist_module`] under `name`.
pub(super) fn unpersist_module(name: &str) -> Result<()> {
    let path = format!("{MODULES_LOAD_DIR}/aenv-{name}.conf");
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("remove {path}")),
    }
}
