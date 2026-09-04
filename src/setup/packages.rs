use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fmt;
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use tracing::info;

const APT_UPDATE_ARGS: &[&str] = &[
    "apt-get",
    "-o",
    "DPkg::Lock::Timeout=120",
    "-o",
    "APT::Get::Lock-Timeout=120",
    "update",
];

const APT_INSTALL_ARGS: &[&str] = &[
    "apt-get",
    "-o",
    "DPkg::Lock::Timeout=120",
    "-o",
    "APT::Get::Lock-Timeout=120",
    "install",
    "-y",
];

#[derive(Debug, Deserialize)]
struct SetupDependencyManifest {
    packages: ManifestPackages,
}

#[derive(Debug, Deserialize)]
struct ManifestPackages {
    runtime: RuntimePackages,
    #[serde(default)]
    runtime_commands: BTreeMap<String, RuntimePackageByDistro>,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimePackageByDistro {
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    ubuntu: Option<String>,
    #[serde(default)]
    debian: Option<String>,
    #[serde(default)]
    centos: Option<String>,
    #[serde(default)]
    rhel: Option<String>,
    #[serde(default)]
    arch: Option<String>,
}

impl RuntimePackageByDistro {
    fn package_for_distro(&self, distro: Distro) -> Option<&str> {
        let specific = match distro {
            Distro::Ubuntu => self.ubuntu.as_deref(),
            Distro::Debian => self.debian.as_deref(),
            Distro::Centos => self.centos.as_deref(),
            Distro::Rhel => self.rhel.as_deref(),
            Distro::Arch => self.arch.as_deref(),
        };
        specific.or(self.default.as_deref())
    }
}

#[derive(Debug, Deserialize)]
struct RuntimePackages {
    #[serde(default)]
    common: Vec<String>,
    #[serde(default)]
    ubuntu: Vec<String>,
    #[serde(default)]
    debian: Vec<String>,
    #[serde(default)]
    centos: Vec<String>,
    #[serde(default)]
    rhel: Vec<String>,
    #[serde(default)]
    arch: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PackageChoice {
    candidates: Vec<String>,
}

impl PackageChoice {
    fn from_str(package: &str) -> Self {
        Self {
            candidates: package.split('|').map(str::to_string).collect(),
        }
    }
}

impl fmt::Display for PackageChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.candidates.as_slice() {
            [] => write!(f, "<empty package entry>"),
            [only] => write!(f, "{}", only),
            candidates => write!(f, "{}", candidates.join(" or ")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MissingRuntimeRequirement {
    Package(PackageChoice),
    Command {
        command: String,
        package: Option<PackageChoice>,
    },
}

impl MissingRuntimeRequirement {
    fn command(command: &str, package: Option<&str>) -> Self {
        Self::Command {
            command: command.to_string(),
            package: package.map(PackageChoice::from_str),
        }
    }

    fn package_choice(&self) -> Option<&PackageChoice> {
        match self {
            MissingRuntimeRequirement::Package(package) => Some(package),
            MissingRuntimeRequirement::Command { package, .. } => package.as_ref(),
        }
    }
}

impl fmt::Display for MissingRuntimeRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MissingRuntimeRequirement::Package(package) => {
                write!(f, "package {}", package)
            }
            MissingRuntimeRequirement::Command { command, package } => {
                let package_str = package
                    .as_ref()
                    .map(PackageChoice::to_string)
                    .unwrap_or_else(|| "<unknown package>".to_string());
                write!(f, "command {} (provided by {})", command, package_str)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MissingRuntimeInstallPlan {
    installable: Vec<String>,
    unavailable: Vec<MissingRuntimeRequirement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PackageManager {
    Apt,
    Dnf,
    Yum,
    Pacman,
    Yay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Distro {
    Ubuntu,
    Debian,
    Centos,
    Rhel,
    Arch,
}

fn bundled_manifest() -> &'static SetupDependencyManifest {
    use std::sync::LazyLock;

    static MANIFEST: LazyLock<SetupDependencyManifest> = LazyLock::new(|| {
        toml::from_str(include_str!("../../config/deps_manifest.toml"))
            .expect("bundled setup dependency manifest is valid")
    });
    &MANIFEST
}

fn sudo_cmd<I, S>(args: I) -> Result<bool>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let status = if nix::unistd::Uid::effective().is_root() {
        let mut args = args.into_iter();
        let Some(program) = args.next() else {
            return Ok(true);
        };
        Command::new(program)
            .args(args)
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    } else {
        Command::new("sudo")
            .args(args)
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    };
    Ok(status)
}

fn cmd_success<I, S>(program: &str, args: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn cmd_stdout<I, S>(program: &str, args: I) -> Option<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn os_release_distro(os_release: &str) -> Option<Distro> {
    let mut id = None;
    let mut id_like = Vec::new();
    for line in os_release.lines() {
        if let Some(value) = line.strip_prefix("ID=") {
            id = Some(value.trim_matches('"').to_ascii_lowercase());
        } else if let Some(value) = line.strip_prefix("ID_LIKE=") {
            id_like.extend(
                value
                    .trim_matches('"')
                    .split_whitespace()
                    .map(str::to_ascii_lowercase),
            );
        }
    }

    id.as_deref()
        .and_then(distro_from_id)
        .or_else(|| id_like.iter().find_map(|value| distro_from_id(value)))
}

fn distro_from_id(id: &str) -> Option<Distro> {
    match id {
        "ubuntu" => Some(Distro::Ubuntu),
        "debian" => Some(Distro::Debian),
        "centos" | "centos-stream" | "tencentos" | "openeuler" | "openEuler" => {
            Some(Distro::Centos)
        }
        "rhel" | "redhat" | "redhatenterpriseserver" => Some(Distro::Rhel),
        "arch" | "manjaro" => Some(Distro::Arch),
        _ => None,
    }
}

fn detect_distro() -> Result<Distro> {
    let os_release = std::fs::read_to_string("/etc/os-release").context("read /etc/os-release")?;
    os_release_distro(&os_release).context("unsupported Linux distribution")
}

fn detect_package_manager(distro: Distro) -> Result<PackageManager> {
    let candidates: &[(&str, PackageManager)] = match distro {
        Distro::Ubuntu | Distro::Debian => &[("apt-get", PackageManager::Apt)],
        Distro::Centos | Distro::Rhel => {
            &[("dnf", PackageManager::Dnf), ("yum", PackageManager::Yum)]
        }
        Distro::Arch => &[
            ("yay", PackageManager::Yay),
            ("pacman", PackageManager::Pacman),
        ],
    };

    candidates
        .iter()
        .find(|(program, _)| which::which(program).is_ok())
        .map(|(_, manager)| *manager)
        .context("no supported package manager found")
}

fn missing_runtime_requirements_for_distro(
    manager: PackageManager,
    manifest: &SetupDependencyManifest,
    distro: Distro,
) -> Result<Vec<MissingRuntimeRequirement>> {
    let distro_packages = match distro {
        Distro::Ubuntu => &manifest.packages.runtime.ubuntu,
        Distro::Debian => &manifest.packages.runtime.debian,
        Distro::Centos => &manifest.packages.runtime.centos,
        Distro::Rhel => &manifest.packages.runtime.rhel,
        Distro::Arch => &manifest.packages.runtime.arch,
    };

    let mut missing = Vec::new();
    for package in manifest
        .packages
        .runtime
        .common
        .iter()
        .chain(distro_packages)
    {
        let choice = PackageChoice::from_str(package);
        let requirement = MissingRuntimeRequirement::Package(choice.clone());
        if !choice
            .candidates
            .iter()
            .any(|candidate| package_installed(manager, candidate))
        {
            missing.push(requirement);
        }
    }

    for (command, distro_packages) in &manifest.packages.runtime_commands {
        if command_present(command) {
            continue;
        }
        let package = distro_packages.package_for_distro(distro);
        missing.push(MissingRuntimeRequirement::command(command, package));
    }

    Ok(missing)
}

fn command_present(command: &str) -> bool {
    which::which(command).is_ok()
}

fn missing_install_plan(
    manager: PackageManager,
    missing: &[MissingRuntimeRequirement],
) -> Result<MissingRuntimeInstallPlan> {
    if missing.is_empty() {
        return Ok(MissingRuntimeInstallPlan {
            installable: Vec::new(),
            unavailable: Vec::new(),
        });
    }

    refresh_package_index(manager)?;
    select_install_packages(manager, missing)
}

fn select_install_packages(
    manager: PackageManager,
    requirements: &[MissingRuntimeRequirement],
) -> Result<MissingRuntimeInstallPlan> {
    select_install_packages_with_probe(manager, requirements, package_available)
}

fn select_install_packages_with_probe<F>(
    manager: PackageManager,
    requirements: &[MissingRuntimeRequirement],
    mut is_package_available: F,
) -> Result<MissingRuntimeInstallPlan>
where
    F: FnMut(PackageManager, &str) -> bool,
{
    let mut installable = BTreeSet::new();
    let mut unavailable = Vec::new();
    for requirement in requirements {
        let Some(package) = requirement.package_choice() else {
            unavailable.push(requirement.clone());
            continue;
        };
        let selected = package.candidates.iter().find_map(|candidate| {
            is_package_available(manager, candidate).then(|| candidate.clone())
        });
        match selected {
            Some(selected) => {
                installable.insert(selected);
            }
            None => unavailable.push(requirement.clone()),
        }
    }
    Ok(MissingRuntimeInstallPlan {
        installable: installable.into_iter().collect(),
        unavailable,
    })
}

fn format_requirements(requirements: &[MissingRuntimeRequirement]) -> String {
    requirements
        .iter()
        .map(MissingRuntimeRequirement::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn missing_unavailable_requirements_message(unavailable: &[MissingRuntimeRequirement]) -> String {
    format!(
        "AENV setup could not find packages for these required runtime requirements in the configured package repositories: {}. Enable the appropriate distro repositories or install equivalent packages that provide the required commands/libraries, then rerun setup.",
        format_requirements(unavailable)
    )
}

fn package_installed(manager: PackageManager, package: &str) -> bool {
    match manager {
        PackageManager::Apt => cmd_stdout("dpkg-query", ["-W", "-f=${Status}", package])
            .map(|status| status.contains("install ok installed"))
            .unwrap_or(false),
        PackageManager::Dnf | PackageManager::Yum => cmd_success("rpm", ["-q", package]),
        PackageManager::Pacman | PackageManager::Yay => cmd_success("pacman", ["-Qi", package]),
    }
}

fn package_available(manager: PackageManager, package: &str) -> bool {
    match manager {
        PackageManager::Apt => cmd_success("apt-cache", ["show", package]),
        PackageManager::Dnf => cmd_success("dnf", ["list", "--available", package]),
        PackageManager::Yum => cmd_success("yum", ["list", "--available", package]),
        PackageManager::Pacman => cmd_success("pacman", ["-Si", package]),
        PackageManager::Yay => {
            cmd_success("yay", ["-Si", package]) || cmd_success("pacman", ["-Si", package])
        }
    }
}

fn install_packages(manager: PackageManager, missing: &MissingRuntimeInstallPlan) -> Result<()> {
    if !missing.unavailable.is_empty() {
        bail!(missing_unavailable_requirements_message(
            &missing.unavailable
        ));
    }

    let packages = &missing.installable;
    if packages.is_empty() {
        return Ok(());
    }

    info!(packages = ?packages, "installing missing runtime packages");
    match manager {
        PackageManager::Apt => install_sudo_packages("apt-get", APT_INSTALL_ARGS, packages),
        PackageManager::Dnf => install_sudo_packages("dnf", &["dnf", "install", "-y"], packages),
        PackageManager::Yum => install_sudo_packages("yum", &["yum", "install", "-y"], packages),
        PackageManager::Pacman => install_sudo_packages(
            "pacman",
            &["pacman", "-Sy", "--needed", "--noconfirm"],
            packages,
        ),
        PackageManager::Yay => {
            if !Command::new("yay")
                .args(["-S", "--needed", "--noconfirm"])
                .args(packages)
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
            {
                bail!("failed to install {} via yay", packages.join(" "));
            }
            Ok(())
        }
    }
}

fn install_sudo_packages(manager: &str, prefix: &[&str], packages: &[String]) -> Result<()> {
    let mut args = prefix
        .iter()
        .map(|arg| (*arg).to_string())
        .collect::<Vec<_>>();
    args.extend(packages.iter().cloned());
    if !sudo_cmd(args)? {
        bail!("failed to install {} via {manager}", packages.join(" "));
    }
    Ok(())
}

fn refresh_package_index(manager: PackageManager) -> Result<()> {
    match manager {
        PackageManager::Apt => {
            if !sudo_cmd(APT_UPDATE_ARGS)? {
                bail!("apt-get update failed");
            }
        }
        // Other supported package managers refresh metadata as part of the
        // install command or use repository metadata directly for probes.
        PackageManager::Dnf
        | PackageManager::Yum
        | PackageManager::Pacman
        | PackageManager::Yay => {}
    }
    Ok(())
}

pub fn ensure() -> Result<()> {
    let distro = detect_distro()?;
    let manager = detect_package_manager(distro)?;
    let manifest = bundled_manifest();
    let missing = missing_runtime_requirements_for_distro(manager, manifest, distro)?;
    let missing = missing_install_plan(manager, &missing)?;
    install_packages(manager, &missing)
}

pub fn check_runtime() -> Result<()> {
    let distro = detect_distro()?;
    let manager = detect_package_manager(distro)?;
    let missing = missing_runtime_requirements_for_distro(manager, bundled_manifest(), distro)?;
    if !missing.is_empty() {
        bail!(
            "missing AENV runtime requirements: {}; run `server --setup-only` or install the listed packages manually",
            format_requirements(&missing)
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ubuntu_os_release() {
        let os_release = "ID=ubuntu\nVERSION_ID=\"24.04\"\n";
        assert_eq!(os_release_distro(os_release), Some(Distro::Ubuntu));
    }

    #[test]
    fn parses_rhel_like_os_release() {
        let os_release = "ID=\"rocky\"\nID_LIKE=\"rhel centos fedora\"\n";
        assert_eq!(os_release_distro(os_release), Some(Distro::Rhel));
    }

    #[test]
    fn runtime_package_mapping_uses_default_with_distro_overrides() {
        let packages = RuntimePackageByDistro {
            default: Some("iproute2".to_string()),
            ubuntu: None,
            debian: None,
            centos: Some("iproute".to_string()),
            rhel: Some("iproute".to_string()),
            arch: None,
        };

        assert_eq!(
            packages.package_for_distro(Distro::Debian),
            Some("iproute2")
        );
        assert_eq!(packages.package_for_distro(Distro::Arch), Some("iproute2"));
        assert_eq!(packages.package_for_distro(Distro::Centos), Some("iproute"));
        assert_eq!(packages.package_for_distro(Distro::Rhel), Some("iproute"));
    }

    #[test]
    fn installed_command_is_not_reported_missing() {
        assert!(command_present("sh"));
    }

    #[test]
    fn unavailable_requirement_is_reported_separately() {
        let package = PackageChoice::from_str("definitely-not-a-real-package");
        let requirement = MissingRuntimeRequirement::Package(package);
        let selected =
            select_install_packages_with_probe(PackageManager::Apt, &[requirement], |_, _| false)
                .expect("select");
        assert!(selected.installable.is_empty());
        assert_eq!(selected.unavailable.len(), 1);
        let message = missing_unavailable_requirements_message(&selected.unavailable);
        assert!(message.contains("package definitely-not-a-real-package"));
    }

    #[test]
    fn parses_package_alternatives() {
        let package = PackageChoice::from_str("libaio1t64|libaio1");
        assert_eq!(
            package.candidates,
            ["libaio1t64".to_string(), "libaio1".to_string()]
        );
    }
}
