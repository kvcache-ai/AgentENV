use std::fs;
use std::path::Path;
use std::process::Command;
use anyhow::Context;

const HOTPLUG_SCRIPT_CANDIDATES: &[&str] = &[
    "/lib/udev/ifupdown-hotplug",
    "/usr/lib/udev/ifupdown-hotplug",
];

/// Take over /lib/udev/ifupdown-hotplug via dpkg-divert so that ifupdown
/// package upgrades cannot overwrite our patch.
///
/// Uses `--add` without `--rename`:
/// - `--rename` would immediately move the original file to .aenv-orig,
///   causing the subsequent write to fail
/// - without `--rename` only the diversion rule is registered; the original
///   path stays in place, package upgrades write their new file to .aenv-orig,
///   and the original path remains under AENV control
///
/// Non-Debian systems (no dpkg-divert) are skipped and we fall back to
/// writing the file directly.
fn ensure_dpkg_divert(script: &str) -> anyhow::Result<()> {
    let diverter = Path::new("/usr/bin/dpkg-divert");
    if !diverter.exists() {
        return Ok(());
    }

    let divert_target = format!("{}.aenv-orig", script);

    // Idempotent: skip if already diverted
    let output = Command::new(diverter)
        .args(["--quiet", "--list", script])
        .output()
        .context("run dpkg-divert --list")?;
    if !output.stdout.is_empty() {
        return Ok(());
    }

    let status = Command::new(diverter)
        .args([
            "--quiet",
            "--add",
            // Note: no --rename here
            "--divert",
            divert_target.as_str(),
            script,
        ])
        .status()
        .context("run dpkg-divert --add")?;

    if !status.success() {
        anyhow::bail!("dpkg-divert --add failed for {}", script);
    }
    Ok(())
}

/// Patch /lib/udev/ifupdown-hotplug: add `aenv-*` to the early-exit
/// whitelists of both the add and remove case statements so that NICs
/// created by AENV no longer trigger ifquery.
///
/// The execution order is strictly: read content -> write backup ->
/// dpkg-divert -> replace -> write back. The content must be read before
/// the diversion is registered, otherwise the read fails once the file
/// has been moved.
pub(super) fn patch_ifupdown_hotplug() -> anyhow::Result<()> {
    // 1) Locate the script that actually exists
    let script = match HOTPLUG_SCRIPT_CANDIDATES
        .iter()
        .find(|p| Path::new(p).exists())
    {
        Some(p) => *p,
        None => {
            eprintln!("[aenv-setup] ifupdown-hotplug not found, skip patch");
            return Ok(());
        }
    };

    // 2) Read the original content first (the script is still in place)
    let content = fs::read_to_string(script)
        .with_context(|| format!("read {}", script))?;

    // Idempotent: return immediately if already patched
    if content.contains("aenv-*") {
        eprintln!("[aenv-setup] ifupdown-hotplug already patched: {}", script);
        return Ok(());
    }

    // 3) Write the backup (from the in-memory content, no second read)
    let backup = format!("{}.aenv-backup", script);
    if !Path::new(&backup).exists() {
        fs::write(&backup, &content)
            .with_context(|| format!("write backup {}", backup))?;
    }

    // 4) Register the dpkg-divert (prevents apt upgrade from overwriting the patch)
    ensure_dpkg_divert(script)?;

    // 5) Text replacement
    let patched = content
        .replace(
            "ppp*|ippp*|isdn*|plip*|lo|irda*|ipsec*)",
            "ppp*|ippp*|isdn*|plip*|lo|irda*|ipsec*|aenv-*)",
        )
        .replace(
            "        ppp*)",
            "        ppp*|aenv-*)",
        );

    if !patched.contains("aenv-*") {
        anyhow::bail!(
            "failed to patch {}: pattern not matched, ifupdown version may differ",
            script
        );
    }

    // 6) Write back
    fs::write(script, patched)
        .with_context(|| format!("write {}", script))?;

    eprintln!("[aenv-setup] patched ifupdown-hotplug: {}", script);

    // 7) Reload udev rules (no --trigger, to avoid replaying events for all NICs)
    let _ = Command::new("udevadm")
        .args(["control", "--reload-rules"])
        .status();

    Ok(())
}
