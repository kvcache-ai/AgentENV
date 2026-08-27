use crate::client::{volumes::validate_volume_reference, Client};
use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const ENVD_READY_TIMEOUT: Duration = Duration::from_secs(30);
const ENVD_READY_PROBE_TIMEOUT: Duration = Duration::from_millis(500);
const ENVD_READY_PROBE_INTERVAL: Duration = Duration::from_millis(100);

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv start my-template
  aenv start --cold docker.io/library/ubuntu:24.04
  aenv start --cold docker.io/library/ubuntu:24.04 --cpu 2 --mem 2048 --disk-size-mb 8192
  aenv start --cold -d docker.io/library/ubuntu:24.04

Resource overrides are only supported with --cold.")]
pub struct Args {
    /// Template ID, snapshot alias, or external image reference when --cold is set
    target: String,
    /// Start directly from an external OCI image instead of a template/snapshot
    #[arg(long)]
    cold: bool,
    /// Require an envd access token for sandbox control communication
    #[arg(long)]
    secure: bool,
    /// Sandbox TTL in seconds
    #[arg(long, default_value_t = super::DEFAULT_TIMEOUT_SECS)]
    timeout: u32,
    #[command(flatten)]
    resources: super::CpuMemoryArgs,
    /// Root filesystem size in MiB for cold-start sandboxes (must be divisible by 1024)
    #[arg(long = "disk-size-mb", alias = "disk-mb", value_parser = parse_disk_size_mb)]
    disk_size_mb: Option<u32>,
    /// Start the sandbox and print its ID without attaching an interactive shell
    #[arg(short = 'd', long)]
    detach: bool,
    /// Mount a persistent volume using MOUNT_PATH=VOLUME_ID_OR_NAME (repeatable)
    #[arg(long = "volume", value_name = "MOUNT_PATH=VOLUME", action = clap::ArgAction::Append)]
    volumes: Vec<String>,
}

fn parse_disk_size_mb(value: &str) -> std::result::Result<u32, String> {
    let size = value
        .parse::<u32>()
        .map_err(|_| "disk size must be a positive integer in MiB".to_string())?;
    if size == 0 || !size.is_multiple_of(1024) {
        return Err("disk size must be greater than 0 and divisible by 1024 MiB".to_string());
    }
    Ok(size)
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let volume_mounts = parse_volume_mounts(&args.volumes)?;
    let sandbox = if args.cold {
        client.create_cold_sandbox(
            &args.target,
            Some(args.timeout),
            args.resources.cpu_count,
            args.resources.memory_mb,
            args.disk_size_mb,
            args.secure,
            volume_mounts,
        )?
    } else {
        if args.resources.is_set() || args.disk_size_mb.is_some() {
            anyhow::bail!("--cpu-count, --memory-mb, and --disk-size-mb require --cold");
        }
        client.create_sandbox(&args.target, Some(args.timeout), args.secure, volume_mounts)?
    };
    let sandbox_id = sandbox.sandbox_id;

    if args.detach {
        println!("{}", sandbox_id);
        return Ok(());
    }

    println!("Started sandbox {}", sandbox_id);
    let rt = super::tokio_rt()?;
    rt.block_on(wait_for_envd(
        &client,
        &sandbox_id,
        sandbox.envd_access_token.as_deref(),
    ))?;
    let code = rt.block_on(super::connect::attach(&client, &sandbox_id))?;
    std::process::exit(code);
}

fn parse_volume_mounts(values: &[String]) -> Result<Option<HashMap<String, String>>> {
    if values.is_empty() {
        return Ok(None);
    }
    let mut mounts: HashMap<String, String> = HashMap::with_capacity(values.len());
    for value in values {
        let Some((mount_path, volume)) = value.split_once('=') else {
            anyhow::bail!("volume mount must use MOUNT_PATH=VOLUME syntax: {value}");
        };
        let mount_path = normalize_mount_path(mount_path)?;
        validate_volume_reference(volume)
            .with_context(|| format!("invalid volume reference in mount: {value}"))?;
        if mounts
            .keys()
            .any(|existing| mount_paths_overlap(existing, &mount_path))
        {
            anyhow::bail!("volume mount paths overlap: {mount_path}");
        }
        if mounts
            .insert(mount_path.clone(), volume.to_owned())
            .is_some()
        {
            anyhow::bail!("volume mount path is specified more than once: {mount_path}");
        }
    }
    Ok(Some(mounts))
}

fn normalize_mount_path(value: &str) -> Result<String> {
    if !value.starts_with('/') || value == "/" {
        anyhow::bail!("volume mount path must be an absolute guest path other than /: {value}");
    }
    if value.contains('\\')
        || value.chars().any(char::is_whitespace)
        || value.contains(',')
        || value.contains(':')
    {
        anyhow::bail!("volume mount path contains invalid characters or '..': {value}");
    }
    let mut normalized = String::new();
    for component in value.split('/') {
        if component.is_empty() || component == "." {
            continue;
        }
        if component == ".." {
            anyhow::bail!("volume mount path contains invalid characters or '..': {value}");
        }
        normalized.push('/');
        normalized.push_str(component);
    }
    if normalized.is_empty() {
        anyhow::bail!("volume mount path must be an absolute guest path other than /: {value}");
    }
    for reserved in [
        "/proc",
        "/sys",
        "/dev",
        "/run",
        "/agentenv",
        "/opt/agentenv",
    ] {
        if mount_paths_overlap(&normalized, reserved) {
            anyhow::bail!("volume mount path conflicts with reserved path: {value}");
        }
    }
    Ok(normalized)
}

fn mount_paths_overlap(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

async fn wait_for_envd(
    client: &Client,
    sandbox_id: &str,
    envd_access_token: Option<&str>,
) -> Result<()> {
    let deadline = Instant::now() + ENVD_READY_TIMEOUT;
    while Instant::now() < deadline {
        if matches!(
            tokio::time::timeout(
                ENVD_READY_PROBE_TIMEOUT,
                client.transport(sandbox_id, envd_access_token)?.ready(),
            )
            .await
            .map(|result| result.is_ok()),
            Ok(true)
        ) {
            return Ok(());
        }
        tokio::time::sleep(ENVD_READY_PROBE_INTERVAL).await;
    }
    anyhow::bail!("sandbox {} envd not healthy within 30s", sandbox_id)
}

#[cfg(test)]
mod tests {
    use super::parse_volume_mounts;

    #[test]
    fn parses_volume_mounts() {
        let values = vec![
            "/mnt/data=cache".to_owned(),
            "/opt/assets=assets".to_owned(),
        ];
        let mounts = parse_volume_mounts(&values).unwrap().unwrap();
        assert_eq!(mounts.get("/mnt/data"), Some(&"cache".to_owned()));
        assert_eq!(mounts.get("/opt/assets"), Some(&"assets".to_owned()));
    }

    #[test]
    fn rejects_invalid_or_duplicate_mounts() {
        assert!(parse_volume_mounts(&["data=cache".to_owned()]).is_err());
        assert!(parse_volume_mounts(&["/mnt/data".to_owned()]).is_err());
        assert!(
            parse_volume_mounts(&["/mnt/data=cache".to_owned(), "/mnt/data=other".to_owned()])
                .is_err()
        );
    }
}
