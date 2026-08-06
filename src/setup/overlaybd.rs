use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use nix::unistd::{chown, Gid};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::cfg::OverlaybdDependencyConfig;

use super::deps::{copy_file, download_file, set_executable, set_file_mode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OverlaybdReleaseTarget {
    os_id: String,
    version_id: String,
    arch: String,
}

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
// Ubuntu release asset published by overlaybd upstream, used as a portable
// fallback for non-Ubuntu (e.g. RPM-family) hosts. See
// `configured_overlaybd_release`. Ubuntu 22.04 is chosen specifically because
// it's the oldest published asset linked against OpenSSL 3 (`libssl.so.3`):
// older assets (18.04/20.04) require `libssl.so.1.1`, which modern
// RPM-family distros (RHEL 9 / TencentOS 4, etc.) no longer ship, while the
// newer 24.04 asset requires `libaio.so.1t64`, which most non-Ubuntu distros
// don't package either.
const FALLBACK_UBUNTU_VERSION: &str = "22.04";

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
    let target = detect_overlaybd_release_target(arch)?;
    let configured_release = configured_overlaybd_release(overlaybd, &target)?;
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

    // The downloaded .deb is only needed during extraction; remove it (and the
    // now-empty downloads dir) so it does not bloat container image layers built
    // via `--setup-only`.
    let _ = std::fs::remove_file(&package_path);
    let _ = std::fs::remove_dir(&downloads_dir);

    Ok(())
}

fn configured_overlaybd_release(
    overlaybd: &OverlaybdDependencyConfig,
    target: &OverlaybdReleaseTarget,
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

    let target_fragment = match target.os_id.as_str() {
        "ubuntu" => format!("ubuntu1.{}.{}", target.version_id, target.arch),
        // Overlaybd upstream only publishes Ubuntu release assets. Each asset
        // bundles its own shared libraries (see `install_overlaybd_release_tools`),
        // so it runs fine on other glibc-based distros. Fall back to the
        // oldest published Ubuntu build (lowest glibc requirement) for known
        // RPM-family distros so the CLI tools install without a native asset.
        "tencentos" | "centos" | "centos-stream" | "rhel" | "redhat"
        | "redhatenterpriseserver" | "openeuler" | "openEuler" => {
            format!("ubuntu1.{}.{}", FALLBACK_UBUNTU_VERSION, target.arch)
        }
        other => bail!(
            "unsupported overlaybd release target: os={} version={} arch={}",
            other,
            target.version_id,
            target.arch
        ),
    };

    Ok(ConfiguredOverlaybdRelease {
        tag_name: tag_name.to_string(),
        package_url: package_url_template
            .replace("{version}", tag_name)
            .replace("{target}", &target_fragment),
    })
}

fn detect_overlaybd_release_target(arch: &str) -> Result<OverlaybdReleaseTarget> {
    let os_release = std::fs::read_to_string("/etc/os-release").context("read /etc/os-release")?;
    let mut os_id = None;
    let mut version_id = None;

    for line in os_release.lines() {
        if let Some(value) = line.strip_prefix("ID=") {
            os_id = Some(value.trim_matches('"').to_string());
        } else if let Some(value) = line.strip_prefix("VERSION_ID=") {
            version_id = Some(value.trim_matches('"').to_string());
        }
    }

    Ok(OverlaybdReleaseTarget {
        os_id: os_id.context("missing ID in /etc/os-release")?,
        version_id: version_id.context("missing VERSION_ID in /etc/os-release")?,
        arch: arch.to_string(),
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
    if package_name.ends_with(".deb") {
        which::which("dpkg-deb").context("dpkg-deb is required to extract overlaybd .deb")?;
        let status = Command::new("dpkg-deb")
            .arg("-x")
            .arg(package_path)
            .arg(destination)
            .status()
            .context("run dpkg-deb to extract overlaybd package")?;
        if !status.success() {
            bail!("dpkg-deb failed to extract {}", package_path.display());
        }
        return Ok(());
    }

    bail!(
        "unsupported overlaybd package format for {} (only .deb is supported currently)",
        package_path.display()
    );
}

fn install_overlaybd_release_tools(extracted_root: &Path, overlaybd_dir: &Path) -> Result<()> {
    let source_bin_dir = extracted_root.join("opt/overlaybd/bin");
    let source_lib_dir = extracted_root.join("opt/overlaybd/lib");
    if !source_bin_dir.is_dir() {
        bail!(
            "overlaybd release payload missing bin dir at {}",
            source_bin_dir.display()
        );
    }
    if !source_lib_dir.is_dir() {
        bail!(
            "overlaybd release payload missing lib dir at {}",
            source_lib_dir.display()
        );
    }

    std::fs::create_dir_all(overlaybd_dir)
        .with_context(|| format!("create overlaybd dir '{}'", overlaybd_dir.display()))?;
    let staging = tempfile::Builder::new()
        .prefix("release-staging-")
        .tempdir_in(overlaybd_dir)
        .context("create overlaybd release staging dir")?;
    let staged_bin = staging.path().join("bin");
    let staged_lib = staging.path().join("lib");
    copy_dir_recursive(&source_bin_dir, &staged_bin)?;
    copy_dir_recursive(&source_lib_dir, &staged_lib)?;

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

    replace_overlaybd_release_dirs(overlaybd_dir, &staged_bin, &staged_lib)
}

fn replace_overlaybd_release_dirs(
    overlaybd_dir: &Path,
    staged_bin: &Path,
    staged_lib: &Path,
) -> Result<()> {
    let backup = tempfile::Builder::new()
        .prefix("release-backup-")
        .tempdir_in(overlaybd_dir)
        .context("create overlaybd release backup dir")?;
    let target_bin = overlaybd_dir.join("bin");
    let target_lib = overlaybd_dir.join("lib");
    let backup_bin = backup.path().join("bin");
    let backup_lib = backup.path().join("lib");

    let had_bin = target_bin.exists();
    let had_lib = target_lib.exists();
    let mut backed_up_bin = false;
    let mut backed_up_lib = false;
    if had_bin {
        std::fs::rename(&target_bin, &backup_bin).context("backup installed overlaybd bin dir")?;
        backed_up_bin = true;
    }
    if had_lib {
        if let Err(err) = std::fs::rename(&target_lib, &backup_lib) {
            let rollback = restore_release_backup(
                &target_bin,
                &target_lib,
                &backup_bin,
                &backup_lib,
                backed_up_bin,
                backed_up_lib,
            );
            return Err(swap_failure(
                anyhow::Error::new(err).context("backup installed overlaybd lib dir"),
                rollback,
                backup,
            ));
        }
        backed_up_lib = true;
    }

    if let Err(err) = std::fs::rename(staged_bin, &target_bin) {
        let rollback = restore_release_backup(
            &target_bin,
            &target_lib,
            &backup_bin,
            &backup_lib,
            backed_up_bin,
            backed_up_lib,
        );
        return Err(swap_failure(
            anyhow::Error::new(err).context("install staged overlaybd bin dir"),
            rollback,
            backup,
        ));
    }
    if let Err(err) = std::fs::rename(staged_lib, &target_lib) {
        let mut rollback_errors = Vec::new();
        record_rename_error(
            &target_bin,
            staged_bin,
            "move newly installed overlaybd bin back to staging",
            &mut rollback_errors,
        );
        restore_release_backup_into(
            &target_bin,
            &target_lib,
            &backup_bin,
            &backup_lib,
            backed_up_bin,
            backed_up_lib,
            &mut rollback_errors,
        );
        let rollback = rollback_result(rollback_errors);
        return Err(swap_failure(
            anyhow::Error::new(err).context("install staged overlaybd lib dir"),
            rollback,
            backup,
        ));
    }

    Ok(())
}

fn restore_release_backup(
    target_bin: &Path,
    target_lib: &Path,
    backup_bin: &Path,
    backup_lib: &Path,
    backed_up_bin: bool,
    backed_up_lib: bool,
) -> Result<()> {
    let mut errors = Vec::new();
    restore_release_backup_into(
        target_bin,
        target_lib,
        backup_bin,
        backup_lib,
        backed_up_bin,
        backed_up_lib,
        &mut errors,
    );
    rollback_result(errors)
}

fn restore_release_backup_into(
    target_bin: &Path,
    target_lib: &Path,
    backup_bin: &Path,
    backup_lib: &Path,
    backed_up_bin: bool,
    backed_up_lib: bool,
    errors: &mut Vec<String>,
) {
    if backed_up_bin {
        record_rename_error(
            backup_bin,
            target_bin,
            "restore previous overlaybd bin dir",
            errors,
        );
    }
    if backed_up_lib {
        record_rename_error(
            backup_lib,
            target_lib,
            "restore previous overlaybd lib dir",
            errors,
        );
    }
}

fn record_rename_error(source: &Path, target: &Path, action: &str, errors: &mut Vec<String>) {
    if let Err(err) = std::fs::rename(source, target) {
        errors.push(format!(
            "{action} '{}' -> '{}': {err}",
            source.display(),
            target.display()
        ));
    }
}

fn rollback_result(errors: Vec<String>) -> Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        bail!(errors.join("; "))
    }
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
    use super::{configured_overlaybd_release, install_default_config, OverlaybdReleaseTarget};
    use crate::cfg::OverlaybdDependencyConfig;
    use nix::unistd::Gid;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    #[test]
    fn configured_overlaybd_release_expands_ubuntu_target_url() {
        let config = OverlaybdDependencyConfig {
            version: "v1.0.18".to_string(),
            url: None,
            package_url: Some(
                "https://example.invalid/{version}/overlaybd-foo.{target}.deb".to_string(),
            ),
        };

        let release = configured_overlaybd_release(
            &config,
            &OverlaybdReleaseTarget {
                os_id: "ubuntu".to_string(),
                version_id: "24.04".to_string(),
                arch: "x86_64".to_string(),
            },
        )
        .expect("configured overlaybd release");

        assert_eq!(release.tag_name, "v1.0.18");
        assert_eq!(
            release.package_url,
            "https://example.invalid/v1.0.18/overlaybd-foo.ubuntu1.24.04.x86_64.deb"
        );
    }

    #[test]
    fn configured_overlaybd_release_falls_back_to_ubuntu_for_rpm_family_target() {
        let config = OverlaybdDependencyConfig {
            version: "v1.0.16".to_string(),
            url: None,
            package_url: Some(
                "https://example.invalid/{version}/overlaybd-foo.{target}.deb".to_string(),
            ),
        };

        for os_id in ["centos", "centos-stream", "tencentos", "rhel", "openeuler", "openEuler"] {
            let release = configured_overlaybd_release(
                &config,
                &OverlaybdReleaseTarget {
                    os_id: os_id.to_string(),
                    version_id: "4.4".to_string(),
                    arch: "x86_64".to_string(),
                },
            )
            .unwrap_or_else(|_| panic!("configured overlaybd release for {os_id}"));

            assert_eq!(
                release.package_url,
                "https://example.invalid/v1.0.16/overlaybd-foo.ubuntu1.22.04.x86_64.deb"
            );
        }
    }

    #[test]
    fn configured_overlaybd_release_rejects_unsupported_target() {
        let config = OverlaybdDependencyConfig {
            version: "v1.0.18".to_string(),
            url: None,
            package_url: Some(
                "https://example.invalid/{version}/overlaybd-foo.{target}".to_string(),
            ),
        };

        let err = configured_overlaybd_release(
            &config,
            &OverlaybdReleaseTarget {
                os_id: "debian".to_string(),
                version_id: "12".to_string(),
                arch: "x86_64".to_string(),
            },
        )
        .expect_err("unsupported target should fail");
        assert!(err
            .to_string()
            .contains("unsupported overlaybd release target"));
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
