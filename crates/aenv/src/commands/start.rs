use crate::client::Client;
use anyhow::Result;
use clap::Args as ClapArgs;
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
    let sandbox = if args.cold {
        client.create_cold_sandbox(
            &args.target,
            Some(args.timeout),
            args.resources.cpu_count,
            args.resources.memory_mb,
            args.disk_size_mb,
            args.secure,
        )?
    } else {
        if args.resources.is_set() || args.disk_size_mb.is_some() {
            anyhow::bail!("--cpu-count, --memory-mb, and --disk-size-mb require --cold");
        }
        client.create_sandbox(&args.target, Some(args.timeout), args.secure)?
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
