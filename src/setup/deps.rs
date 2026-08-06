use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use object_store_operator::{
    credential_source_from_fields, CredentialFields, CredentialSource, CredentialSourceOptions,
};
use overlaybd::config::DownloadConfig;
use serde::Deserialize;
use tracing::{debug, info};

use crate::cfg::{
    AppConfig, OssBackendConfig, OverlaybdDependencyConfig, SnapshotRepositoryBackendKind,
};
use crate::digest::FileDigest;
use crate::virtualization::VirtualizationMode;

#[derive(Debug, Deserialize)]
struct SetupDependencyManifest {
    firecracker: ManifestVirtualizationDownloads,
    kernel: ManifestVirtualizationDownloads,
    tools: ManifestTools,
    overlaybd: OverlaybdDependencyConfig,
    #[serde(rename = "regclient")]
    regctl: ManifestDownload,
}

#[derive(Debug, Deserialize)]
struct ManifestDownload {
    version: String,
    url: String,
    /// Per-architecture overrides. When the host architecture matches a key,
    /// the override's `version` and `url` replace the top-level defaults
    /// (unless the user has set explicit config values).
    #[serde(default)]
    arch: std::collections::HashMap<String, ArchManifestDownload>,
}

#[derive(Debug, Clone, Deserialize)]
struct ArchManifestDownload {
    version: String,
    url: String,
}

#[derive(Debug, Deserialize)]
struct ManifestVirtualizationDownloads {
    kvm: ManifestDownload,
    pvm: ManifestDownload,
}

impl ManifestVirtualizationDownloads {
    fn for_mode(&self, mode: VirtualizationMode) -> &ManifestDownload {
        match mode {
            VirtualizationMode::Kvm => &self.kvm,
            VirtualizationMode::Pvm => &self.pvm,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ManifestTools {
    url: String,
}

fn bundled_manifest() -> &'static SetupDependencyManifest {
    use std::sync::LazyLock;

    static MANIFEST: LazyLock<SetupDependencyManifest> = LazyLock::new(|| {
        toml::from_str(include_str!("../../config/deps_manifest.toml"))
            .expect("bundled setup dependency manifest is valid")
    });
    &MANIFEST
}

fn resolve_url(template: &str, vars: &[(&str, &str)]) -> String {
    let mut result = template.to_string();
    for (key, value) in vars {
        result = result.replace(&format!("{{{key}}}"), value);
    }
    result
}

pub(super) fn detect_arch() -> Result<String> {
    match std::env::consts::ARCH {
        "x86_64" => Ok("x86_64".into()),
        "aarch64" => Ok("aarch64".into()),
        other => bail!("unsupported architecture: {other}"),
    }
}

pub async fn ensure(config: &AppConfig, deps_path: &Path) -> Result<()> {
    let manifest = bundled_manifest();

    // Architecture is resolved once and reused across dependency URL/path derivation.
    let arch = detect_arch()?;
    if config.virtualization_mode == VirtualizationMode::Pvm && arch != "x86_64" {
        bail!("PVM virtualization mode is only supported on x86_64 hosts");
    }

    std::fs::create_dir_all(deps_path)?;

    ensure_firecracker(config, manifest, &arch).await?;
    ensure_kernel(config, manifest, &arch).await?;

    // regctl is a runtime dependency for all registry access, not just tools
    // drive extraction, so it remains provisioned for explicit tools drives.
    ensure_regctl(deps_path).await?;
    ensure_tools(config, deps_path, manifest)?;

    // Overlaybd runtime configs and CLI tools are required before request-time
    // image resolution, since OCI → overlaybd conversion shells out to
    // `overlaybd-create` / `overlaybd-apply` / `overlaybd-commit`.
    let overlaybd_dir = deps_path.join("overlaybd");
    std::fs::create_dir_all(&overlaybd_dir)?;
    let overlaybd_config = config.overlaybd.as_ref().unwrap_or(&manifest.overlaybd);
    super::overlaybd::ensure_release_tools(overlaybd_config, &overlaybd_dir, &arch).await?;

    info!(path = %deps_path.display(), "dependencies ready");
    Ok(())
}

async fn ensure_firecracker(
    config: &AppConfig,
    manifest: &SetupDependencyManifest,
    arch: &str,
) -> Result<()> {
    if let Some(fc_path) = config.firecracker.binary_path.as_deref() {
        return validate_explicit_file("firecracker.binary_path", fc_path, true);
    }

    let fc_path = config.resolved_firecracker_binary_path();
    if file_exists_nonempty(&fc_path) {
        debug!(path = %fc_path.display(), "firecracker binary already present");
        return Ok(());
    }

    let mode_manifest = manifest.firecracker.for_mode(config.virtualization_mode);
    let arch_override = mode_manifest.arch.get(arch);
    let fc_version = config.firecracker.version.as_deref().unwrap_or(
        arch_override
            .map(|a| a.version.as_str())
            .unwrap_or(&mode_manifest.version),
    );
    let fc_url_template = config.firecracker.url.as_deref().unwrap_or(
        arch_override
            .map(|a| a.url.as_str())
            .unwrap_or(&mode_manifest.url),
    );
    let fc_dir = fc_path
        .parent()
        .context("resolved firecracker binary path has no parent")?;
    let fc_tgz_path = fc_dir.join(format!("firecracker-{fc_version}-{arch}.tgz"));
    let cth_path = fc_dir.join("cpu-template-helper");
    let fc_url = resolve_url(fc_url_template, &[("version", fc_version), ("arch", arch)]);
    download_file(&fc_url, &fc_tgz_path).await?;
    extract_firecracker(&fc_tgz_path, &fc_path, &cth_path)?;
    // The tarball is only needed during extraction; drop it so it does not
    // bloat container image layers built via `--setup-only`.
    let _ = std::fs::remove_file(&fc_tgz_path);
    Ok(())
}

async fn ensure_kernel(
    config: &AppConfig,
    manifest: &SetupDependencyManifest,
    arch: &str,
) -> Result<()> {
    if let Some(kernel_path) = config.kernel.image_path.as_deref() {
        return validate_explicit_file("kernel.image_path", kernel_path, false);
    }

    let kernel_path = config.resolved_kernel_image_path();
    if file_exists_nonempty(&kernel_path) {
        debug!(path = %kernel_path.display(), "kernel image already present");
        return Ok(());
    }

    let mode_manifest = manifest.kernel.for_mode(config.virtualization_mode);
    let arch_override = mode_manifest.arch.get(arch);
    let kernel_version = config.kernel.version.as_deref().unwrap_or(
        arch_override
            .map(|a| a.version.as_str())
            .unwrap_or(&mode_manifest.version),
    );
    let kernel_url_template = config.kernel.url.as_deref().unwrap_or(
        arch_override
            .map(|a| a.url.as_str())
            .unwrap_or(&mode_manifest.url),
    );
    let kernel_url = resolve_url(
        kernel_url_template,
        &[("version", kernel_version), ("arch", arch)],
    );
    download_file(&kernel_url, &kernel_path).await
}

fn ensure_tools(
    config: &AppConfig,
    deps_path: &Path,
    manifest: &SetupDependencyManifest,
) -> Result<()> {
    if config.tools.version.is_none()
        && (config.tools.drive_path.is_some() || config.tools.url.is_some())
    {
        bail!("tools.version is required when tools.drive_path or tools.url is configured");
    }
    let version = config.resolved_tools_version();
    let tools_path = config.resolved_tools_drive_path()?;
    if let Some(source) = config.tools.drive_path.as_deref() {
        return install_explicit_tools_drive(source, &tools_path, version);
    }
    if file_exists_nonempty(&tools_path) {
        debug!(path = %tools_path.display(), "tools drive already present");
        return Ok(());
    }

    // The managed tools drive uses the single-ext4 wrapper convention; the
    // tools image is platform-built and has `/tools.ext4` baked in by design.
    let tools_url_template = config.tools.url.as_deref().unwrap_or(&manifest.tools.url);
    let tools_dir = tools_path
        .parent()
        .context("resolved tools drive path has no parent")?;
    std::fs::create_dir_all(tools_dir)?;
    let ghcr_image = resolve_url(tools_url_template, &[("version", version)]);
    let regctl_path = crate::cfg::regctl_path(deps_path);
    extract_ext4_from_ghcr(&regctl_path, &ghcr_image, "tools.ext4", &tools_path)?;
    Ok(())
}

fn install_explicit_tools_drive(source: &Path, destination: &Path, version: &str) -> Result<()> {
    validate_explicit_file("tools.drive_path", source, false)?;
    if destination.exists() {
        return verify_installed_tools_drive(source, destination, version);
    }

    let parent = destination
        .parent()
        .context("resolved tools drive path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary tools drive in {}", parent.display()))?;
    std::fs::copy(source, temp.path()).with_context(|| {
        format!(
            "copy tools drive source {} to {}",
            source.display(),
            temp.path().display()
        )
    })?;
    set_file_mode(temp.path(), 0o644)?;
    match temp.persist_noclobber(destination) {
        Ok(_) => {
            info!(version, path = %destination.display(), "installed local tools drive");
            Ok(())
        }
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            verify_installed_tools_drive(source, destination, version)
        }
        Err(error) => Err(error.error)
            .with_context(|| format!("install tools drive at {}", destination.display())),
    }
}

fn verify_installed_tools_drive(source: &Path, destination: &Path, version: &str) -> Result<()> {
    let source_digest = FileDigest::describe_blocking(source)
        .with_context(|| format!("hash tools drive source {}", source.display()))?;
    let installed_digest = FileDigest::describe_blocking(destination)
        .with_context(|| format!("hash installed tools drive {}", destination.display()))?;
    if source_digest != installed_digest {
        bail!(
            "tools drive version '{version}' from {} conflicts with the installed file at {}; publish a new version",
            source.display(),
            destination.display()
        );
    }
    Ok(())
}

/// True when `version` appears as a standalone token in `regctl version`
/// output, independent of the exact output format.
fn version_output_mentions_exact_token(output: &str, version: &str) -> bool {
    output
        .split(|c: char| c.is_ascii_whitespace() || matches!(c, ':' | '=' | ',' | '"' | '\''))
        .any(|token| token == version)
}

/// Ensure the version-pinned `regctl` binary is installed under `deps_path`.
async fn ensure_regctl(deps_path: &Path) -> Result<()> {
    let dest = crate::cfg::regctl_path(deps_path);
    let manifest = &bundled_manifest().regctl;
    let already_pinned = Command::new(&dest)
        .arg("version")
        .output()
        .is_ok_and(|out| {
            out.status.success()
                && version_output_mentions_exact_token(
                    &String::from_utf8_lossy(&out.stdout),
                    &manifest.version,
                )
        });
    if already_pinned {
        debug!(path = %dest.display(), version = %manifest.version, "regctl already installed, skipping");
        return Ok(());
    }

    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => bail!("unsupported architecture for regctl: {other}"),
    };
    let url = resolve_url(
        &manifest.url,
        &[("version", &manifest.version), ("arch", arch)],
    );
    info!(url = %url, dest = %dest.display(), "downloading regctl");

    if dest.exists() {
        std::fs::remove_file(&dest)
            .with_context(|| format!("remove stale regctl binary {}", dest.display()))?;
    }
    download_executable_file(&url, &dest).await?;
    info!(path = %dest.display(), "regctl installed");
    Ok(())
}

pub(crate) fn write_generated_overlaybd_global_configs(
    config: &AppConfig,
    p2p_facade_address: Option<&str>,
) -> Result<()> {
    let rootfs_download = DownloadConfig {
        enable: config.ublk.overlaybd.download_enable,
        ..Default::default()
    };
    let memory_download = config
        .memory_snapshot
        .background_download
        .to_overlaybd_download_config();

    for (path, download) in [
        (&config.ublk.overlaybd.global_config_path, &rootfs_download),
        (
            &config.memory_snapshot.overlaybd_global_config_path,
            &memory_download,
        ),
    ] {
        write_generated_overlaybd_global_config(path, config, p2p_facade_address, download)
            .with_context(|| format!("write overlaybd global config {}", path.display()))?;
    }
    Ok(())
}

pub(super) fn file_exists_nonempty(path: &Path) -> bool {
    path.metadata()
        .map(|metadata| metadata.is_file() && metadata.len() > 0)
        .unwrap_or(false)
}

fn validate_explicit_file(config_key: &str, path: &Path, executable: bool) -> Result<()> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("validate {config_key} {}", path.display()))?;
    if !metadata.is_file() {
        bail!("{config_key} is not a regular file: {}", path.display());
    }
    if metadata.len() == 0 {
        bail!("{config_key} is an empty file: {}", path.display());
    }
    std::fs::File::open(path)
        .with_context(|| format!("{config_key} is not readable: {}", path.display()))?;

    #[cfg(unix)]
    if executable {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o111 == 0 {
            bail!("{config_key} is not executable: {}", path.display());
        }
    }

    info!(config_key, path = %path.display(), "using local dependency");
    Ok(())
}

pub(super) async fn download_file(url: &str, dest: &Path) -> Result<()> {
    if file_exists_nonempty(dest) {
        debug!(path = %dest.display(), "already present, skipping download");
        return Ok(());
    }

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }

    info!(url = url, dest = %dest.display(), "downloading");

    let response = reqwest::get(url).await.context(format!("GET {url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("HTTP {} for {}", status, url);
    }

    let bytes = response.bytes().await.context("reading response body")?;

    let tmp_path = dest.with_extension("tmp");
    tokio::fs::write(&tmp_path, &bytes).await?;
    tokio::fs::rename(&tmp_path, dest)
        .await
        .context("moving downloaded file to destination")?;
    Ok(())
}

async fn download_executable_file(url: &str, dest: &Path) -> Result<()> {
    download_file(url, dest).await?;
    set_executable(dest)
}

pub(super) fn copy_file(source: &Path, destination: &Path, executable: bool) -> Result<()> {
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create target dir '{}'", parent.display()))?;
    }
    std::fs::copy(source, destination)
        .with_context(|| format!("copy '{}' to '{}'", source.display(), destination.display()))?;
    if executable {
        set_executable(destination)?;
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn set_file_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("set permissions on {}", path.display()))
}

#[cfg(not(unix))]
pub(super) fn set_file_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

pub(super) fn set_executable(path: &Path) -> Result<()> {
    set_file_mode(path, 0o755)
}
fn extract_firecracker(tgz_path: &Path, fc_path: &Path, cth_path: &Path) -> Result<()> {
    let tmp_dir = std::env::temp_dir().join(format!("agentenv-fc-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir)?;

    let file = std::fs::File::open(tgz_path)?;
    let gz = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(gz);
    archive.unpack(&tmp_dir)?;

    let fc_candidate = find_firecracker_binary(&tmp_dir)?;
    copy_file(&fc_candidate, fc_path, true)?;

    // cpu-template-helper is optional; skip silently if not in the tarball.
    if let Some(cth_candidate) = find_cpu_template_helper(&tmp_dir) {
        if copy_file(&cth_candidate, cth_path, true).is_ok() {
            info!(path = %cth_path.display(), "cpu-template-helper extracted");
        }
    }

    std::fs::remove_dir_all(&tmp_dir).ok();
    Ok(())
}

fn find_cpu_template_helper(dir: &Path) -> Option<PathBuf> {
    walkdir(dir).ok()?.into_iter().find_map(|entry| {
        let name = entry.file_name().to_string_lossy().into_owned();
        (name.contains("cpu-template-helper")
            && !name.ends_with(".debug")
            && entry.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        .then(|| entry.path())
    })
}

fn find_firecracker_binary(dir: &Path) -> Result<PathBuf> {
    let direct = dir.join("firecracker");
    if direct.is_file() {
        return Ok(direct);
    }

    for entry in walkdir(dir)? {
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if name.contains("firecracker")
            && !name.contains("jailer")
            && !name.contains("rebase-snap")
            && !name.ends_with(".debug")
            && !name.ends_with(".yaml")
            && entry.file_type().map(|ft| ft.is_file()).unwrap_or(false)
        {
            return Ok(entry.path());
        }
    }

    bail!("firecracker binary not found in {}", dir.display())
}

fn walkdir(dir: &Path) -> Result<Vec<std::fs::DirEntry>> {
    let mut results = Vec::new();
    walkdir_inner(dir, 3, &mut results)?;
    Ok(results)
}

fn walkdir_inner(dir: &Path, depth: u32, results: &mut Vec<std::fs::DirEntry>) -> Result<()> {
    if depth == 0 {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if ft.is_file() {
            results.push(entry);
        } else if ft.is_dir() {
            walkdir_inner(&entry.path(), depth - 1, results)?;
        }
    }
    Ok(())
}

fn run_cmd(program: impl AsRef<OsStr>, args: &[&str]) -> Result<std::process::Output> {
    let program = program.as_ref();
    let output = Command::new(program).args(args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{} {} failed: {}",
            program.to_string_lossy(),
            args.first().unwrap_or(&""),
            stderr.trim()
        );
    }
    Ok(output)
}

/// Pull a container image from a registry and extract a single file from it.
///
/// Uses `regctl` to copy the image into a local OCI layout and `umoci` to
/// unpack the rootfs, avoiding any dependency on a running Docker daemon.
fn extract_ext4_from_ghcr(
    regctl_path: &Path,
    image: &str,
    filename: &str,
    dest: &Path,
) -> Result<()> {
    if !file_exists_nonempty(regctl_path) {
        bail!(
            "regctl is required to download container images: {}",
            regctl_path.display()
        );
    }
    which::which("umoci").context("umoci is required to unpack container images")?;

    let tmp_dir = tempfile::tempdir().context("failed to create temporary directory")?;

    let oci_dir = tmp_dir.path().join("oci");
    let bundle_dir = tmp_dir.path().join("bundle");

    // regctl image copy --platform local image ocidir://local_dir:latest
    // (--platform local: umoci unpack needs a single-platform manifest)
    let dest_ref = format!("ocidir://{}:latest", oci_dir.display());
    info!(
        image = image,
        filename = filename,
        "pulling from container registry via regctl"
    );
    let regctl_args = [
        "image",
        "copy",
        "--platform",
        "local",
        image,
        dest_ref.as_str(),
    ];
    run_cmd(regctl_path.as_os_str(), &regctl_args)?;

    // umoci unpack --image oci_dir:latest bundle_dir
    let image_arg = format!("{}:latest", oci_dir.display());
    run_cmd(
        "umoci",
        &[
            "unpack",
            "--rootless",
            "--image",
            &image_arg,
            &bundle_dir.to_string_lossy(),
        ],
    )?;

    // Locate the file inside the unpacked rootfs.
    let rootfs_dir = bundle_dir.join("rootfs");
    let direct = rootfs_dir.join(filename);
    let src = if direct.is_file() {
        direct
    } else {
        walkdir(&rootfs_dir)?
            .into_iter()
            .find(|e| e.file_name().to_string_lossy() == filename)
            .map(|e| e.path().to_path_buf())
            .ok_or_else(|| anyhow::anyhow!("failed to find /{filename} in image {image}"))?
    };

    std::fs::rename(&src, dest).or_else(|_| std::fs::copy(&src, dest).map(|_| ()))?;
    set_file_mode(dest, 0o644)?;

    info!(path = %dest.display(), "extracted {filename}");
    Ok(())
}

fn write_generated_overlaybd_global_config(
    path: &Path,
    app_config: &AppConfig,
    p2p_facade_address: Option<&str>,
    download: &DownloadConfig,
) -> Result<()> {
    let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let image_cache = app_config.image_cache_layout();
    let log_path = config_dir.join("overlaybd.log");
    let credential_config = match detect_docker_credential_config() {
        Some(credential_path) => {
            info!("found docker credential file; wiring overlaybd runtime to reuse it for registry auth");
            serde_json::json!({
                "mode": "file",
                "path": credential_path.to_string_lossy(),
                "timeout": 5
            })
        }
        None => serde_json::json!({"mode": "", "path": "", "timeout": 1}),
    };
    let p2p_config = match p2p_facade_address {
        Some(address) => serde_json::json!({
            "enable": true,
            "address": address
        }),
        None => serde_json::json!({
            "enable": false,
            "address": "http://localhost:9731/accelerator"
        }),
    };

    let mut config = serde_json::json!({
        "logConfig": {
            "logLevel": 1,
            "logPath": log_path
        },
        "cacheConfig": {
            "cacheType": "file",
            "cacheDir": image_cache.remote_blocks_dir,
            "cacheSizeGB": image_cache.remote_blocks_size_gb,
            "refillSize": 262144,
            "blockSize": 65536
        },
        "credentialConfig": credential_config,
        "ioEngine": 0,
        "download": download,
        "p2pConfig": p2p_config,
        "enableAudit": false,
        "registryFsVersion": "v2"
    });

    if app_config.snapshot.repository_backend == SnapshotRepositoryBackendKind::Oss {
        let oss_config = app_config
            .backend
            .oss
            .as_ref()
            .context("backend.oss config is required when snapshot.repository_backend = oss")?;
        config["ossConfig"] = overlaybd_runtime_oss_config(oss_config)?;
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create config parent dir {}", parent.display()))?;
    }
    let mut raw = serde_json::to_string_pretty(&config)?;
    raw.push('\n');
    std::fs::write(path, raw)
        .with_context(|| format!("write overlaybd global config {}", path.display()))?;
    set_file_mode(path, 0o600)?;
    Ok(())
}

fn overlaybd_runtime_oss_config(oss: &OssBackendConfig) -> Result<serde_json::Value> {
    let credential_source = credential_source_from_fields(
        CredentialFields {
            access_key_id: oss.access_key_id.as_deref(),
            secret_access_key: oss.access_key_secret.as_deref(),
            security_token: oss.security_token.as_deref(),
            credential_process: oss.credential_process.as_deref(),
        },
        CredentialSourceOptions {
            scope: "backend.oss",
            allow_anonymous: false,
            required_access_key_id_label: "backend.oss.access_key_id",
            required_secret_access_key_label: "backend.oss.access_key_secret",
        },
    )?;
    let region = oss
        .region
        .as_deref()
        .map(str::trim)
        .filter(|region| !region.is_empty())
        .context("backend.oss.region must be set when generating overlaybd OSS config")?;
    let endpoint = oss.endpoint.trim();

    let mut config = serde_json::json!({
        "enable": true,
        "defaultRegion": region,
        "defaultEndpoint": endpoint,
    });

    match credential_source {
        CredentialSource::Static(credential) => {
            config["accessKeyId"] = credential.access_key_id.into();
            config["secretAccessKey"] = credential.secret_access_key.into();
            config["securityToken"] = credential.security_token.unwrap_or_default().into();
            config["credentialProcess"] = "".into();
        }
        CredentialSource::Process { command } => {
            config["accessKeyId"] = "".into();
            config["secretAccessKey"] = "".into();
            config["securityToken"] = "".into();
            config["credentialProcess"] = command.into();
        }
        CredentialSource::Anonymous => {
            bail!("backend.oss requires non-anonymous credentials");
        }
    }
    Ok(config)
}

/// Detect a docker-config-format credential file the overlaybd runtime can
/// reuse via `credentialConfig.mode=file`. Returns `None` when no obvious file
/// is present — registry auth then falls back to anonymous, which is fine for
/// public registries.
fn detect_docker_credential_config() -> Option<PathBuf> {
    let candidates = [
        std::env::var_os("DOCKER_CONFIG")
            .map(PathBuf::from)
            .map(|p| p.join("config.json")),
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|p| p.join(".docker/config.json")),
    ];
    candidates.into_iter().flatten().find(|p| p.is_file())
}

#[cfg(test)]
mod tests {
    use super::{
        bundled_manifest, ensure_firecracker, ensure_kernel, ensure_tools, file_exists_nonempty,
        overlaybd_runtime_oss_config, validate_explicit_file, version_output_mentions_exact_token,
        write_generated_overlaybd_global_configs,
    };
    use overlaybd::config::DownloadConfig;

    use crate::cfg::{
        AppConfig, MemorySnapshotConfig, OssBackendConfig, UblkOverlaybdTomlConfig, UblkTomlConfig,
    };

    fn sample_oss_config() -> OssBackendConfig {
        OssBackendConfig {
            endpoint: " https://oss-cn-hangzhou.aliyuncs.com ".to_string(),
            bucket: " demo-bucket ".to_string(),
            prefix: Some(" snapshots ".to_string()),
            credential_process: None,
            access_key_id: Some(" ak ".to_string()),
            access_key_secret: Some(" sk ".to_string()),
            security_token: Some(" token ".to_string()),
            region: Some(" cn-hangzhou ".to_string()),
            cache_max_size_gb: Some(4),
        }
    }

    fn app_config_with_overlaybd_global_configs(
        root: &std::path::Path,
        rootfs_global: std::path::PathBuf,
        mem_global: std::path::PathBuf,
    ) -> AppConfig {
        AppConfig {
            deps_path: root.to_path_buf(),
            ublk: UblkTomlConfig {
                enabled: true,
                daemon_binary_path: None,
                daemon_log_path: None,
                device_type: "overlaybd".to_string(),
                overlaybd: UblkOverlaybdTomlConfig {
                    global_config_path: rootfs_global,
                    ..Default::default()
                },
                ..UblkTomlConfig::default()
            },
            memory_snapshot: MemorySnapshotConfig {
                overlaybd_global_config_path: mem_global,
                direct_overlaybd: false,
                ..Default::default()
            },
            ..AppConfig::default()
        }
    }

    fn write_minimal_global_config(path: &std::path::Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        std::fs::write(
            path,
            r#"{"p2pConfig":{"enable":false,"address":"http://old"},"registryFsVersion":"v2"}"#,
        )
        .expect("write global config");
    }

    #[test]
    fn overlaybd_runtime_oss_config_uses_normalized_semantics() {
        let config = overlaybd_runtime_oss_config(&sample_oss_config())
            .expect("derive overlaybd oss config");

        assert_eq!(config["accessKeyId"], "ak");
        assert_eq!(config["secretAccessKey"], "sk");
        assert_eq!(config["securityToken"], "token");
        assert_eq!(config["credentialProcess"], "");
        assert_eq!(config["defaultRegion"], "cn-hangzhou");
        assert_eq!(
            config["defaultEndpoint"],
            "https://oss-cn-hangzhou.aliyuncs.com"
        );
    }

    #[test]
    fn overlaybd_runtime_oss_config_uses_credential_process_when_configured() {
        let mut oss = sample_oss_config();
        oss.access_key_id = None;
        oss.access_key_secret = None;
        oss.security_token = None;
        oss.credential_process = Some("echo creds".to_string());

        let config = overlaybd_runtime_oss_config(&oss).expect("derive overlaybd oss config");
        assert_eq!(config["accessKeyId"], "");
        assert_eq!(config["credentialProcess"], "echo creds");
    }

    #[test]
    fn overlaybd_runtime_oss_config_rejects_blank_static_credentials() {
        let mut oss = sample_oss_config();
        oss.access_key_id = Some("   ".to_string());

        let err =
            overlaybd_runtime_oss_config(&oss).expect_err("blank static credential should fail");
        assert!(err.to_string().contains("backend.oss.access_key_id"));
    }

    #[test]
    fn regctl_version_match_uses_exact_token() {
        assert!(version_output_mentions_exact_token(
            "VCSTag:     v0.11.5\n",
            "v0.11.5"
        ));
        assert!(!version_output_mentions_exact_token(
            "VCSTag:     v0.11.55\n",
            "v0.11.5"
        ));
    }

    #[tokio::test]
    async fn explicit_tools_drive_is_imported_into_its_version_directory() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let deps_path = temp.path().join("deps");
        let firecracker = temp.path().join("local-firecracker");
        let kernel = temp.path().join("local-vmlinux");
        let tools = temp.path().join("local-tools.ext4");
        std::fs::write(&firecracker, b"firecracker").expect("write firecracker");
        std::fs::set_permissions(&firecracker, std::fs::Permissions::from_mode(0o755))
            .expect("make firecracker executable");
        std::fs::write(&kernel, b"kernel").expect("write kernel");
        std::fs::write(&tools, b"tools").expect("write tools");

        let mut config = AppConfig {
            deps_path: deps_path.clone(),
            ..AppConfig::default()
        };
        config.firecracker.binary_path = Some(firecracker);
        config.kernel.image_path = Some(kernel);
        config.tools.drive_path = Some(tools);
        config.tools.version = Some("0.1.0".to_string());

        ensure_firecracker(&config, bundled_manifest(), "x86_64")
            .await
            .expect("accept explicit firecracker");
        ensure_kernel(&config, bundled_manifest(), "x86_64")
            .await
            .expect("accept explicit kernel");
        ensure_tools(&config, &deps_path, bundled_manifest()).expect("import explicit tools");

        assert!(!deps_path.join("firecracker").exists());
        assert!(!deps_path.join("kernel").exists());
        assert_eq!(
            std::fs::read(deps_path.join("tools/0.1.0/tools.ext4")).expect("read imported tools"),
            b"tools"
        );
    }

    #[test]
    fn explicit_tools_drive_version_cannot_be_reused_for_different_content() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("local-tools.ext4");
        std::fs::write(&source, b"first").expect("write tools source");
        let mut config = AppConfig {
            deps_path: temp.path().join("deps"),
            ..AppConfig::default()
        };
        config.tools.drive_path = Some(source.clone());
        config.tools.version = Some("0.1.0".to_string());

        ensure_tools(&config, &config.deps_path, bundled_manifest()).expect("install tools drive");
        std::fs::write(&source, b"second").expect("replace tools source");

        let error = ensure_tools(&config, &config.deps_path, bundled_manifest())
            .expect_err("reusing a version for different content must fail");
        assert!(error.to_string().contains("publish a new version"));
    }

    #[test]
    fn tools_url_override_requires_explicit_version_before_cache_hit() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = AppConfig {
            deps_path: temp.path().join("deps"),
            ..AppConfig::default()
        };
        let tools_path = config
            .resolved_tools_drive_path()
            .expect("resolve manifest tools version");
        std::fs::create_dir_all(tools_path.parent().expect("tools directory"))
            .expect("create tools directory");
        std::fs::write(&tools_path, b"cached tools").expect("write cached tools");
        config.tools.url = Some("registry.example.com/custom/tools:{version}".to_string());

        let error = ensure_tools(&config, &config.deps_path, bundled_manifest())
            .expect_err("custom tools URL must declare its release version");

        assert!(error.to_string().contains(
            "tools.version is required when tools.drive_path or tools.url is configured"
        ));
    }

    #[test]
    fn explicit_dependency_validation_rejects_invalid_files() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let missing = temp.path().join("missing");
        let err = validate_explicit_file("kernel.image_path", &missing, false)
            .expect_err("missing file must fail");
        assert!(err.to_string().contains("kernel.image_path"));
        assert!(err.to_string().contains(&missing.display().to_string()));

        let directory = temp.path().join("directory");
        std::fs::create_dir(&directory).expect("create directory");
        let err = validate_explicit_file("tools.drive_path", &directory, false)
            .expect_err("directory must fail");
        assert!(err.to_string().contains("not a regular file"));

        let empty = temp.path().join("empty");
        std::fs::write(&empty, []).expect("write empty file");
        let err = validate_explicit_file("kernel.image_path", &empty, false)
            .expect_err("empty file must fail");
        assert!(err.to_string().contains("is an empty file"));

        let non_executable = temp.path().join("firecracker");
        std::fs::write(&non_executable, b"firecracker").expect("write firecracker");
        std::fs::set_permissions(&non_executable, std::fs::Permissions::from_mode(0o644))
            .expect("remove executable bit");
        let err = validate_explicit_file("firecracker.binary_path", &non_executable, true)
            .expect_err("non-executable firecracker must fail");
        assert!(err.to_string().contains("is not executable"));
    }

    #[test]
    fn nonempty_file_check_rejects_directories_and_empty_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = temp.path().join("directory");
        let empty = temp.path().join("empty");
        let file = temp.path().join("file");
        std::fs::create_dir(&directory).expect("create directory");
        std::fs::write(&empty, []).expect("write empty");
        std::fs::write(&file, b"data").expect("write file");

        assert!(!file_exists_nonempty(&directory));
        assert!(!file_exists_nonempty(&empty));
        assert!(file_exists_nonempty(&file));
    }

    fn read_global_config_value(path: &std::path::Path) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(path).expect("read generated global config"))
            .expect("parse generated global config")
    }

    fn assert_download_json(value: &serde_json::Value, expected: &DownloadConfig) {
        let download = &value["download"];
        assert_eq!(download["enable"], expected.enable);
        assert_eq!(download["delay"], expected.delay);
        assert_eq!(download["delayExtra"], expected.delay_extra);
        assert_eq!(download["maxMBps"], expected.max_mbps);
        assert_eq!(download["tryCnt"], expected.try_cnt);
        assert_eq!(download["blockSize"], expected.block_size);
        assert_eq!(download["concurrency"], expected.concurrency);
        assert_eq!(download["maxInflightBlocks"], expected.max_inflight_blocks);
        assert_eq!(
            download["maxConcurrentFiles"],
            expected.max_concurrent_files
        );
    }

    #[test]
    fn generated_overlaybd_global_configs_use_distinct_download_defaults() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rootfs = temp.path().join("overlaybd-global.json");
        let mem = temp.path().join("mem-overlaybd-global.json");
        let config =
            app_config_with_overlaybd_global_configs(temp.path(), rootfs.clone(), mem.clone());

        write_generated_overlaybd_global_configs(&config, None).expect("write global configs");

        let rootfs_value = read_global_config_value(&rootfs);
        assert_eq!(rootfs_value["download"]["enable"], false);
        assert_eq!(rootfs_value["download"]["concurrency"], 1);
        let expected_memory = MemorySnapshotConfig::default()
            .background_download
            .to_overlaybd_download_config();
        let memory_value = read_global_config_value(&mem);
        assert_download_json(&memory_value, &expected_memory);
    }

    #[test]
    fn generated_memory_overlaybd_global_config_uses_explicit_download_overrides() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rootfs = temp.path().join("overlaybd-global.json");
        let mem = temp.path().join("mem-overlaybd-global.json");
        let mut config =
            app_config_with_overlaybd_global_configs(temp.path(), rootfs.clone(), mem.clone());
        config.memory_snapshot.background_download.enable = false;
        config.memory_snapshot.background_download.block_size = 2 * 1024 * 1024;
        config.memory_snapshot.background_download.concurrency = 7;

        write_generated_overlaybd_global_configs(&config, None).expect("write global configs");

        let expected_memory = DownloadConfig {
            enable: false,
            block_size: 2 * 1024 * 1024,
            concurrency: 7,
            ..MemorySnapshotConfig::default()
                .background_download
                .to_overlaybd_download_config()
        };
        assert_download_json(&read_global_config_value(&mem), &expected_memory);
    }

    #[test]
    fn generated_overlaybd_p2p_config_enables_both_global_configs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rootfs = temp.path().join("overlaybd-global.json");
        let mem = temp.path().join("mem-overlaybd-global.json");
        write_minimal_global_config(&rootfs);
        write_minimal_global_config(&mem);
        let config =
            app_config_with_overlaybd_global_configs(temp.path(), rootfs.clone(), mem.clone());

        write_generated_overlaybd_global_configs(&config, Some("http://127.0.0.1:12345/p2p-http"))
            .expect("write p2p config");

        for path in [&rootfs, &mem] {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).expect("read generated")).unwrap();
            assert_eq!(value["p2pConfig"]["enable"], true);
            assert_eq!(
                value["p2pConfig"]["address"],
                "http://127.0.0.1:12345/p2p-http"
            );
            assert_eq!(value["registryFsVersion"], "v2");
        }
    }

    #[test]
    fn generated_overlaybd_p2p_config_can_disable_facade() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rootfs = temp.path().join("overlaybd-global.json");
        let mem = temp.path().join("mem-overlaybd-global.json");
        write_minimal_global_config(&rootfs);
        write_minimal_global_config(&mem);
        let config =
            app_config_with_overlaybd_global_configs(temp.path(), rootfs.clone(), mem.clone());

        write_generated_overlaybd_global_configs(&config, None).expect("write p2p config");

        for path in [&rootfs, &mem] {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).expect("read generated")).unwrap();
            assert_eq!(value["p2pConfig"]["enable"], false);
            assert_eq!(
                value["p2pConfig"]["address"],
                "http://localhost:9731/accelerator"
            );
            assert_ne!(value["p2pConfig"]["address"], "http://old");
        }
    }
}
