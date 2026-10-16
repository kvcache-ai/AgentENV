use anyhow::{bail, Result};
use std::path::PathBuf;

use crate::config;
use crate::util;

/// Ensure a cargo subcommand is installed (e.g. "mutants" for `cargo mutants`).
pub fn ensure_cargo_tool(subcommand: &str, crate_name: &str) -> Result<()> {
    let binary = format!("cargo-{subcommand}");
    if which::which(&binary).is_ok() {
        return Ok(());
    }
    util::info(&format!("Installing {crate_name}..."));
    util::cmd("cargo", &["install", "--locked", crate_name])
}

/// Ensure rustup component is installed.
pub fn ensure_rustup_component(component: &str) -> Result<()> {
    let output = std::process::Command::new("rustup")
        .env("RUSTUP_SKIP_UPDATE_CHECK", "1")
        .args(["component", "list", "--installed"])
        .output();
    if let Ok(out) = output {
        let installed = String::from_utf8_lossy(&out.stdout);
        if installed.lines().any(|l| l.starts_with(component)) {
            return Ok(());
        }
    }
    util::info(&format!("Installing rustup component {component}..."));
    util::cmd("rustup", &["component", "add", component])
}

/// Ensure protoc is installed with the given version. Downloads from GitHub if missing.
pub fn ensure_protoc(version: &str, url_template: &str) -> Result<PathBuf> {
    // Check if already installed with correct version
    if let Ok(output) = std::process::Command::new("protoc")
        .arg("--version")
        .output()
    {
        if output.status.success() {
            let ver_str = String::from_utf8_lossy(&output.stdout);
            if ver_str.contains(version) {
                return Ok(which::which("protoc")?);
            }
        }
    }

    let arch = util::detect_arch()?;
    let platform = match arch.as_str() {
        "x86_64" => "linux-x86_64",
        "aarch64" => "linux-aarch_64",
        _ => bail!("unsupported arch for protoc: {arch}"),
    };

    let install_dir = dirs_install();
    let url = config::resolve_url(
        url_template,
        &[("version", version), ("platform", platform)],
    );
    let zip_path = std::env::temp_dir().join(format!("protoc-{version}.zip"));

    util::download_file(&url, &zip_path)?;

    // Extract zip
    let file = std::fs::File::open(&zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    archive.extract(&install_dir)?;

    let bin_path = install_dir.join("bin/protoc");
    if !bin_path.exists() {
        bail!(
            "protoc not found after extraction at {}",
            bin_path.display()
        );
    }

    // Add to GITHUB_PATH if in CI
    if let Ok(github_path) = std::env::var("GITHUB_PATH") {
        let bin_dir = install_dir.join("bin");
        let _ = std::fs::OpenOptions::new()
            .append(true)
            .open(&github_path)
            .and_then(|mut f| {
                use std::io::Write;
                writeln!(f, "{}", bin_dir.display())
            });
    }

    Ok(bin_path)
}

fn dirs_install() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home).join(".local")
}
