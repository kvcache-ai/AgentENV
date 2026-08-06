use std::path::Path;

use anyhow::{bail, Context, Result};
use nix::unistd::{chown, Gid};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::cfg::OverlaybdDependencyConfig;

use super::deps::{copy_file, download_file, set_executable, set_file_mode};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct OverlaybdInstalledRelease {
    tag_name: String,
    asset_name: String,
    digest: Option<String>,
}

const OVERLAYBD_TOOL_NAMES: &[&str] = &[
    "overlaybd-create",
    "overlaybd-apply",
    "overlaybd-commit",
    "overlaybd-resize",
];
#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfiguredOverlaybdRelease {
    tag_name: String,
    package_url: String,
}

pub async fn ensure_release_tools(
    overlaybd: &OverlaybdDependencyConfig,
    overlaybd_dir: &Path,
    arch: &str,
) -> Result<()> {
    let configured_release = configured_overlaybd_release(overlaybd, arch)?;
    let asset_name = configured_release
        .package_url
        .rsplit('/')
        .next()
        .context("overlaybd package URL missing asset name")?;
    let desired_release = desired_overlaybd_installed_release(&configured_release, asset_name);

    let metadata_path = overlaybd_dir.join("tools-release.json");
    let installed = read_overlaybd_installed_release(&metadata_path)?;
    let staged_default_config = overlaybd_dir.join("etc/overlaybd/overlaybd.json");
    if installed.as_ref() == Some(&desired_release)
        && overlaybd_tools_present(overlaybd_dir)
        && staged_default_config.is_file()
    {
        debug!(
            tag = %configured_release.tag_name,
            asset = %asset_name,
            "overlaybd CLI tools already installed"
        );
        return Ok(());
    }

    let downloads_dir = overlaybd_dir.join("downloads");
    std::fs::create_dir_all(&downloads_dir).with_context(|| {
        format!(
            "create overlaybd downloads dir '{}'",
            downloads_dir.display()
        )
    })?;
    let package_path = downloads_dir.join(asset_name);
    download_file(&configured_release.package_url, &package_path).await?;

    let extract_dir = tempfile::tempdir().context("create temp dir for overlaybd release")?;
    extract_overlaybd_package(&package_path, extract_dir.path())?;

    let extracted_root = extract_dir.path();
    install_overlaybd_release_tools(extracted_root, overlaybd_dir)?;
    stage_overlaybd_default_config(extracted_root, overlaybd_dir)?;

    std::fs::write(&metadata_path, serde_json::to_vec_pretty(&desired_release)?).with_context(
        || {
            format!(
                "write overlaybd release metadata '{}'",
                metadata_path.display()
            )
        },
    )?;

    // The downloaded archive is only needed during extraction; remove it (and the
    // now-empty downloads dir) so it does not bloat container image layers built
    // via `--setup-only`.
    let _ = std::fs::remove_file(&package_path);
    let _ = std::fs::remove_dir(&downloads_dir);

    Ok(())
}

fn configured_overlaybd_release(
    overlaybd: &OverlaybdDependencyConfig,
    arch: &str,
) -> Result<ConfiguredOverlaybdRelease> {
    let tag_name = overlaybd.version.trim();
    if tag_name.is_empty() {
        bail!("overlaybd.version not set in config");
    }

    let package_url_template = overlaybd
        .package_url
        .as_deref()
        .or(overlaybd.url.as_deref())
        .context("overlaybd.package_url not set in config")?
        .trim();
    if package_url_template.is_empty() {
        bail!("overlaybd.package_url not set in config");
    }

    match arch {
        "x86_64" | "aarch64" => {}
        other => bail!("unsupported overlaybd release architecture: {other}"),
    }

    Ok(ConfiguredOverlaybdRelease {
        tag_name: tag_name.to_string(),
        package_url: package_url_template
            .replace("{version}", tag_name)
            .replace("{arch}", arch),
    })
}

fn overlaybd_tools_present(overlaybd_dir: &Path) -> bool {
    OVERLAYBD_TOOL_NAMES
        .iter()
        .all(|tool| overlaybd_dir.join("bin").join(tool).is_file())
}

fn desired_overlaybd_installed_release(
    release: &ConfiguredOverlaybdRelease,
    asset_name: &str,
) -> OverlaybdInstalledRelease {
    OverlaybdInstalledRelease {
        tag_name: release.tag_name.clone(),
        asset_name: asset_name.to_string(),
        digest: None,
    }
}

fn read_overlaybd_installed_release(path: &Path) -> Result<Option<OverlaybdInstalledRelease>> {
    if !path.exists() {
        return Ok(None);
    }

    let bytes = std::fs::read(path)
        .with_context(|| format!("read overlaybd release metadata '{}'", path.display()))?;
    let metadata = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse overlaybd release metadata '{}'", path.display()))?;
    Ok(Some(metadata))
}

fn extract_overlaybd_package(package_path: &Path, destination: &Path) -> Result<()> {
    let package_name = package_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if package_name.ends_with(".tar.gz") {
        let file = std::fs::File::open(package_path)
            .with_context(|| format!("open overlaybd package {}", package_path.display()))?;
        let decoder = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(decoder);
        archive
            .unpack(destination)
            .with_context(|| format!("extract overlaybd package {}", package_path.display()))?;
        return Ok(());
    }

    bail!(
        "unsupported overlaybd package format for {} (expected .tar.gz)",
        package_path.display()
    );
}

fn install_overlaybd_release_tools(extracted_root: &Path, overlaybd_dir: &Path) -> Result<()> {
    let source_bin_dir = extracted_root.join("bin");
    if !source_bin_dir.is_dir() {
        bail!(
            "overlaybd release payload missing bin dir at {}",
            source_bin_dir.display()
        );
    }
    std::fs::create_dir_all(overlaybd_dir)
        .with_context(|| format!("create overlaybd dir '{}'", overlaybd_dir.display()))?;
    let staging = tempfile::Builder::new()
        .prefix("release-staging-")
        .tempdir_in(overlaybd_dir)
        .context("create overlaybd release staging dir")?;
    let staged_bin = staging.path().join("bin");
    copy_dir_recursive(&source_bin_dir, &staged_bin)?;

    for tool in OVERLAYBD_TOOL_NAMES {
        let staged = staged_bin.join(tool);
        if !staged.is_file() {
            bail!(
                "overlaybd release payload missing required tool '{}'",
                source_bin_dir.join(tool).display()
            );
        }
        set_executable(&staged)?;
    }

    replace_overlaybd_release_bin(overlaybd_dir, &staged_bin)?;
    remove_legacy_overlaybd_lib(overlaybd_dir)
}

fn replace_overlaybd_release_bin(overlaybd_dir: &Path, staged_bin: &Path) -> Result<()> {
    let backup = tempfile::Builder::new()
        .prefix("release-backup-")
        .tempdir_in(overlaybd_dir)
        .context("create overlaybd release backup dir")?;
    let target_bin = overlaybd_dir.join("bin");
    let backup_bin = backup.path().join("bin");

    let had_bin = target_bin.exists();
    if had_bin {
        std::fs::rename(&target_bin, &backup_bin).context("backup installed overlaybd bin dir")?;
    }

    if let Err(err) = std::fs::rename(staged_bin, &target_bin) {
        let rollback = if had_bin {
            std::fs::rename(&backup_bin, &target_bin).context("restore previous overlaybd bin dir")
        } else {
            Ok(())
        };
        return Err(swap_failure(
            anyhow::Error::new(err).context("install staged overlaybd bin dir"),
            rollback,
            backup,
        ));
    }
    Ok(())
}

fn swap_failure(
    install_error: anyhow::Error,
    rollback: Result<()>,
    backup: tempfile::TempDir,
) -> anyhow::Error {
    match rollback {
        Ok(()) => install_error,
        Err(rollback_error) => {
            let backup_path = backup.keep();
            install_error.context(format!(
                "overlaybd release rollback was incomplete: {rollback_error:#}; previous release backup preserved at '{}'",
                backup_path.display()
            ))
        }
    }
}

fn remove_legacy_overlaybd_lib(overlaybd_dir: &Path) -> Result<()> {
    let legacy_lib = overlaybd_dir.join("lib");
    let metadata = match std::fs::symlink_metadata(&legacy_lib) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect legacy overlaybd lib {}", legacy_lib.display()))
        }
    };

    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(&legacy_lib)
    } else {
        std::fs::remove_file(&legacy_lib)
    }
    .with_context(|| format!("remove legacy overlaybd lib {}", legacy_lib.display()))
}

fn stage_overlaybd_default_config(extracted_root: &Path, overlaybd_dir: &Path) -> Result<()> {
    let packaged_default_config = extracted_root.join("etc/overlaybd/overlaybd.json");
    if !packaged_default_config.is_file() {
        bail!(
            "overlaybd release payload missing default config at {}",
            packaged_default_config.display()
        );
    }

    let staged = overlaybd_dir.join("etc/overlaybd/overlaybd.json");
    if let Some(parent) = staged.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create overlaybd config dir '{}'", parent.display()))?;
    }
    std::fs::copy(&packaged_default_config, &staged).with_context(|| {
        format!(
            "stage overlaybd default config '{}' -> '{}'",
            packaged_default_config.display(),
            staged.display()
        )
    })?;
    Ok(())
}

pub fn install_system_default_config(deps_path: &Path, runtime_gid: Gid) -> Result<()> {
    let source = deps_path.join("overlaybd/etc/overlaybd/overlaybd.json");
    let destination = Path::new("/etc/overlaybd/overlaybd.json");
    install_default_config(&source, destination, runtime_gid)
}

fn install_default_config(source: &Path, destination: &Path, runtime_gid: Gid) -> Result<()> {
    if destination.exists() && !destination.is_file() {
        bail!(
            "overlaybd system config is not a regular file: {}",
            destination.display()
        );
    }

    let parent = destination
        .parent()
        .context("overlaybd system config path has no parent directory")?;
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    set_runtime_group(parent, runtime_gid)?;
    set_file_mode(parent, 0o750)?;

    if destination.is_file() {
        info!(
            path = %destination.display(),
            "retaining existing overlaybd system config content"
        );
    } else if !source.is_file() {
        bail!(
            "staged overlaybd default config is missing: {}",
            source.display()
        );
    } else {
        std::fs::copy(source, destination).with_context(|| {
            format!(
                "install overlaybd default config {} -> {}",
                source.display(),
                destination.display()
            )
        })?;
    }

    set_runtime_group(destination, runtime_gid)?;
    set_file_mode(destination, 0o640)?;
    Ok(())
}

fn set_runtime_group(path: &Path, runtime_gid: Gid) -> Result<()> {
    chown(path, None, Some(runtime_gid))
        .with_context(|| format!("set runtime group ownership on {}", path.display()))
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir_all(destination)
        .with_context(|| format!("create overlaybd lib dir '{}'", destination.display()))?;

    for entry in std::fs::read_dir(source)
        .with_context(|| format!("read overlaybd lib dir '{}'", source.display()))?
    {
        let entry = entry.with_context(|| format!("iterate '{}'", source.display()))?;
        let entry_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry_path, &destination_path)?;
        } else {
            copy_file(&entry_path, &destination_path, false)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        configured_overlaybd_release, extract_overlaybd_package, install_default_config,
        install_overlaybd_release_tools, OVERLAYBD_TOOL_NAMES,
    };
    use crate::cfg::OverlaybdDependencyConfig;
    use nix::unistd::Gid;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    #[test]
    fn configured_overlaybd_release_expands_arch_url() {
        let config = OverlaybdDependencyConfig {
            version: "v1.0.18-aenv.1".to_string(),
            url: None,
            package_url: Some(
                "https://example.invalid/static-{version}/overlaybd-tools-{version}-linux-{arch}.tar.gz"
                    .to_string(),
            ),
        };

        let release =
            configured_overlaybd_release(&config, "aarch64").expect("configured overlaybd release");

        assert_eq!(release.tag_name, "v1.0.18-aenv.1");
        assert_eq!(
            release.package_url,
            "https://example.invalid/static-v1.0.18-aenv.1/overlaybd-tools-v1.0.18-aenv.1-linux-aarch64.tar.gz"
        );
    }

    #[test]
    fn configured_overlaybd_release_rejects_unsupported_architecture() {
        let config = OverlaybdDependencyConfig {
            version: "v1.0.18-aenv.1".to_string(),
            url: None,
            package_url: Some("https://example.invalid/{arch}.tar.gz".to_string()),
        };

        let err = configured_overlaybd_release(&config, "riscv64")
            .expect_err("unsupported architecture should fail");
        assert!(err
            .to_string()
            .contains("unsupported overlaybd release architecture"));
    }

    #[test]
    fn extracts_static_overlaybd_tarball_layout() {
        let temp = tempfile::tempdir().expect("temp dir");
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        std::fs::create_dir_all(source.join("bin")).expect("create source bin");
        std::fs::write(source.join("bin/overlaybd-create"), b"tool").expect("write tool");
        let package = temp.path().join("overlaybd-tools.tar.gz");
        let file = std::fs::File::create(&package).expect("create package");
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut archive = tar::Builder::new(encoder);
        archive.append_dir_all(".", &source).expect("build package");
        archive
            .into_inner()
            .expect("finish tar")
            .finish()
            .expect("finish gzip");

        extract_overlaybd_package(&package, &destination).expect("extract package");
        assert_eq!(
            std::fs::read(destination.join("bin/overlaybd-create")).expect("read tool"),
            b"tool"
        );
    }

    #[test]
    fn installs_static_tools_without_lib_and_removes_legacy_lib() {
        let temp = tempfile::tempdir().expect("temp dir");
        let extracted = temp.path().join("extracted");
        let overlaybd_dir = temp.path().join("installed");
        std::fs::create_dir_all(extracted.join("bin")).expect("create extracted bin");
        for tool in OVERLAYBD_TOOL_NAMES {
            std::fs::write(extracted.join("bin").join(tool), b"static tool")
                .expect("write static tool");
        }
        std::fs::create_dir_all(overlaybd_dir.join("lib")).expect("create legacy lib");
        std::fs::write(overlaybd_dir.join("lib/liblegacy.so"), b"legacy")
            .expect("write legacy library");

        install_overlaybd_release_tools(&extracted, &overlaybd_dir)
            .expect("install static overlaybd tools");

        assert!(!overlaybd_dir.join("lib").exists());
        for tool in OVERLAYBD_TOOL_NAMES {
            assert!(overlaybd_dir.join("bin").join(tool).is_file());
        }
    }

    #[test]
    fn system_default_config_is_readable_by_the_runtime_group() {
        let temp = tempfile::tempdir().expect("temp dir");
        let source = temp.path().join("staged/overlaybd.json");
        let destination = temp.path().join("etc/overlaybd/overlaybd.json");
        std::fs::create_dir_all(source.parent().expect("source parent"))
            .expect("create source parent");
        std::fs::write(&source, b"{\"source\":true}\n").expect("write source config");

        let runtime_gid = Gid::current();
        install_default_config(&source, &destination, runtime_gid).expect("install default config");

        let destination_metadata = destination.metadata().expect("destination metadata");
        let parent_metadata = destination
            .parent()
            .expect("destination parent")
            .metadata()
            .expect("parent metadata");
        assert_eq!(destination_metadata.permissions().mode() & 0o777, 0o640);
        assert_eq!(destination_metadata.gid(), runtime_gid.as_raw());
        assert_eq!(parent_metadata.permissions().mode() & 0o777, 0o750);
        assert_eq!(parent_metadata.gid(), runtime_gid.as_raw());

        std::fs::write(&destination, b"{\"custom\":true}\n").expect("write custom config");
        std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o600))
            .expect("make custom config private");
        install_default_config(&source, &destination, runtime_gid).expect("retain custom config");

        assert_eq!(
            std::fs::read_to_string(&destination).expect("read retained config"),
            "{\"custom\":true}\n"
        );
        assert_eq!(
            destination
                .metadata()
                .expect("retained metadata")
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
    }
}
