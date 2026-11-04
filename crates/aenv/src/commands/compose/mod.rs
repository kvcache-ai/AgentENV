//! `aenv compose up`: plans an image-only Compose project locally, cold-starts
//! the Compose guest image with one writable drive per service and starts the
//! project inside it.

mod build;
mod guest;
mod image;
mod plan;

use super::start::{parse_disk_size_mb, wait_for_envd};
use crate::client::sandboxes::{AttachedDrive, AttachedDriveSource, NewColdSandbox, Sandbox};
use crate::client::{handle_status, Client};
use anyhow::{anyhow, Context, Result};
use clap::{Args as ClapArgs, Subcommand};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Allows image preparation and VM startup.
const CREATE_TIMEOUT: Duration = Duration::from_secs(600);
/// Keeps the sandbox alive past the startup budget until `aenv` extends or
/// deletes it.
const STARTUP_TTL_MARGIN_SECS: u32 = 60;

fn parse_env(value: &str) -> std::result::Result<(String, String), String> {
    let (key, value) = value
        .split_once('=')
        .filter(|(key, _)| !key.is_empty())
        .ok_or_else(|| "expected KEY=VALUE with a non-empty key".to_owned())?;
    Ok((key.to_owned(), value.to_owned()))
}

fn read_compose(mut reader: impl Read) -> Result<String> {
    let mut compose = String::new();
    reader
        .read_to_string(&mut compose)
        .context("reading Compose source as UTF-8")?;
    Ok(compose)
}

#[derive(ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    /// Create a sandbox, wait for Compose readiness, and print its ID
    Up(UpArgs),
    /// Build service images and write an image-only Compose file
    Build(build::BuildArgs),
}

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv compose up -f compose.yaml
  aenv compose up -f compose.yaml --env TAG=stable --profile worker
  cat compose.yaml | aenv compose up -f -
  aenv compose up --base-image registry.example.com/aenv-compose:v1 -f compose.yaml

Each invocation creates a new sandbox. Manage it with aenv exec, connect, pause,
resume, snapshot, and delete. Only explicit --env values are used for interpolation;
the local environment and .env files are not loaded. Without --base-image, the
first run builds the bundled guest image with the server's builder.")]
struct UpArgs {
    /// Compose YAML or JSON file; use - to read stdin
    #[arg(short = 'f', long, default_value = "compose.yaml", value_name = "PATH")]
    file: PathBuf,
    /// Compose guest image built from images/compose/Dockerfile [default: the
    /// bundled image, built on first use]
    #[arg(long, value_name = "IMAGE")]
    base_image: Option<String>,
    /// Compose interpolation variable (repeatable; last value wins)
    #[arg(long = "env", value_name = "KEY=VALUE", value_parser = parse_env)]
    environment: Vec<(String, String)>,
    /// Enable an optional Compose profile (repeatable)
    #[arg(long = "profile", value_name = "NAME")]
    profiles: Vec<String>,
    /// Sandbox TTL in seconds, starting after Compose is ready
    #[arg(long, default_value_t = super::DEFAULT_TIMEOUT_SECS)]
    timeout: u32,
    /// Seconds allowed for Docker readiness, image registration and health checks
    #[arg(long, default_value_t = 300, value_name = "SECONDS")]
    startup_timeout: u32,
    #[command(flatten)]
    resources: super::CpuMemoryArgs,
    /// Root filesystem size in MiB (must be divisible by 1024)
    #[arg(long = "disk-size-mb", alias = "disk-mb", value_parser = parse_disk_size_mb)]
    disk_size_mb: Option<u32>,
    /// Compose planner executable [default: aenv-compose-plan next to aenv]
    #[arg(long, value_name = "PATH")]
    planner: Option<PathBuf>,
}

pub fn run(args: Args) -> Result<()> {
    match args.cmd {
        Sub::Up(args) => up(args),
        Sub::Build(args) => build::run(args),
    }
}

fn up(args: UpArgs) -> Result<()> {
    let compose = if args.file == Path::new("-") {
        read_compose(std::io::stdin().lock())?
    } else {
        let file = std::fs::File::open(&args.file)
            .with_context(|| format!("opening Compose file {}", args.file.display()))?;
        read_compose(file)?
    };
    let planner = match args.planner {
        Some(planner) => planner,
        None => std::env::current_exe()?.with_file_name(plan::PLANNER),
    };
    let rt = super::tokio_rt()?;
    let planned: plan::Plan = rt.block_on(plan::prepare(
        &planner,
        &plan::Request {
            mode: None,
            compose: &compose,
            compose_env: args.environment.into_iter().collect(),
            profiles: args.profiles,
        },
    ))?;
    let (drives, services) = service_drives(&planned.services);
    let client = Client::from_env()?;
    eprintln!("Starting Compose sandbox; waiting for services to become ready...");
    let image = args
        .base_image
        .clone()
        .unwrap_or_else(image::image_reference);
    let body = NewColdSandbox {
        image: &image,
        timeout: Some(args.startup_timeout.saturating_add(STARTUP_TTL_MARGIN_SECS)),
        cpu_count: args.resources.cpu_count,
        memory_mb: args.resources.memory_mb,
        disk_size_mb: args.disk_size_mb,
        secure: true,
        volume_mounts: None,
        attached_drives: Some(drives),
    };
    let create = || {
        handle_status(
            client
                .post("/sandboxes-cold")
                .timeout(CREATE_TIMEOUT)
                .send_json(&body),
        )
    };
    let response = match create() {
        Err(error) if args.base_image.is_none() && image::is_missing(&error) => {
            eprintln!("Building the bundled Compose guest image (first use only)...");
            image::build()?;
            create()
        }
        result => result,
    };
    let sandbox: Sandbox = response?.into_json()?;
    let budget = Duration::from_secs(args.startup_timeout.into());
    let started = rt
        .block_on(async {
            tokio::select! {
                result = start(&client, &sandbox, &planned.compose, &services, budget) => result,
                _ = tokio::signal::ctrl_c() => Err(anyhow!("interrupted")),
            }
        })
        .and_then(|()| client.set_timeout(&sandbox.sandbox_id, args.timeout));
    if let Err(error) = started {
        if let Err(cleanup) = client.delete_sandbox(&sandbox.sandbox_id) {
            eprintln!(
                "warning: failed to delete sandbox {}: {cleanup:#}",
                sandbox.sandbox_id
            );
        }
        return Err(error);
    }
    println!("{}", sandbox.sandbox_id);
    Ok(())
}

async fn start(
    client: &Client,
    sandbox: &Sandbox,
    compose: &serde_json::Value,
    services: &[guest::Service<'_>],
    budget: Duration,
) -> Result<()> {
    let token = sandbox.envd_access_token.as_deref();
    wait_for_envd(client, &sandbox.sandbox_id, token).await?;
    let transport = client.transport(&sandbox.sandbox_id, token)?;
    guest::start(&transport, compose, services, budget).await
}

/// Gives every service its own writable image drive, even when services share
/// a source image.
fn service_drives(services: &[plan::Service]) -> (Vec<AttachedDrive>, Vec<guest::Service<'_>>) {
    services
        .iter()
        .enumerate()
        .map(|(index, service)| {
            let drive_id = format!("compose_{index}");
            let mount_path = format!("/mnt/compose_{index}");
            let drive = AttachedDrive {
                drive_id: drive_id.clone(),
                read_only: false,
                mount_path: mount_path.clone(),
                source: AttachedDriveSource {
                    image: service.image.clone(),
                },
            };
            let service = guest::Service {
                name: &service.name,
                drive_id,
                mount_path,
                local_image: &service.local_image,
            };
            (drive, service)
        })
        .unzip()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[test]
    fn preserves_compose_source_and_requires_utf8() {
        let source = "services: {app: {image: '${IMAGE}', command: ['echo', '$$HOME']}}\n";
        assert_eq!(read_compose(source.as_bytes()).unwrap(), source);
        assert!(read_compose(&[0xff][..]).is_err());
    }

    fn parse(arguments: &[&str]) -> UpArgs {
        let crate::Cmd::Compose(Args { cmd: Sub::Up(args) }) =
            crate::Cli::try_parse_from(arguments).unwrap().cmd
        else {
            panic!("expected compose up");
        };
        args
    }

    #[test]
    fn accepts_profiles_explicit_environment_and_resource_aliases() {
        crate::Cli::command().debug_assert();
        let args = parse(&[
            "aenv",
            "compose",
            "up",
            "-f",
            "-",
            "--base-image",
            "registry.example.com/aenv-compose:v1",
            "--profile",
            "web",
            "--profile",
            "worker",
            "--env",
            "VALUE=a=b",
            "--env",
            "EMPTY=",
            "--cpu-count",
            "2",
            "--mem",
            "2048",
            "--disk-mb",
            "8192",
            "--timeout",
            "600",
            "--startup-timeout",
            "120",
            "--planner",
            "/opt/aenv-compose-plan",
        ]);
        assert_eq!(args.file, PathBuf::from("-"));
        assert_eq!(
            args.base_image.as_deref(),
            Some("registry.example.com/aenv-compose:v1")
        );
        assert_eq!(args.profiles, ["web", "worker"]);
        assert_eq!(
            args.environment,
            [("VALUE".into(), "a=b".into()), ("EMPTY".into(), "".into())]
        );
        assert_eq!(args.resources.cpu_count, Some(2));
        assert_eq!(args.resources.memory_mb, Some(2048));
        assert_eq!(args.disk_size_mb, Some(8192));
        assert_eq!(args.timeout, 600);
        assert_eq!(args.startup_timeout, 120);
        assert_eq!(args.planner, Some(PathBuf::from("/opt/aenv-compose-plan")));
        let defaults = parse(&["aenv", "compose", "up"]);
        assert_eq!(defaults.file, PathBuf::from("compose.yaml"));
        assert!(defaults.base_image.is_none());
        assert!(defaults.environment.is_empty());
        assert!(defaults.profiles.is_empty());
        assert!(!defaults.resources.is_set());
        assert_eq!(defaults.timeout, 300);
        assert_eq!(defaults.startup_timeout, 300);
        assert!(defaults.planner.is_none());
    }

    #[test]
    fn rejects_invalid_environment_and_disk() {
        for (flag, value) in [
            ("--env", "MISSING_VALUE"),
            ("--env", "=value"),
            ("--disk-size-mb", "0"),
            ("--disk-size-mb", "1025"),
        ] {
            assert!(crate::Cli::try_parse_from([
                "aenv",
                "compose",
                "up",
                "--base-image",
                "image",
                flag,
                value
            ])
            .is_err());
        }
    }

    #[test]
    fn gives_each_service_a_writable_drive() {
        let services: Vec<plan::Service> = serde_json::from_value(serde_json::json!([
            {"name": "a", "image": "busybox:1.37", "localImage": "aenv-compose/service-0:local"},
            {"name": "b", "image": "busybox:1.37", "localImage": "aenv-compose/service-1:local"},
        ]))
        .unwrap();
        let (drives, guest) = service_drives(&services);
        assert_eq!(
            serde_json::to_value(&drives).unwrap(),
            serde_json::json!([
                {"driveID": "compose_0", "readOnly": false, "mountPath": "/mnt/compose_0",
                 "source": {"image": "busybox:1.37"}},
                {"driveID": "compose_1", "readOnly": false, "mountPath": "/mnt/compose_1",
                 "source": {"image": "busybox:1.37"}},
            ])
        );
        let guest: Vec<_> = guest
            .iter()
            .map(|s| {
                (
                    s.name,
                    s.drive_id.as_str(),
                    s.mount_path.as_str(),
                    s.local_image,
                )
            })
            .collect();
        assert_eq!(
            guest,
            [
                (
                    "a",
                    "compose_0",
                    "/mnt/compose_0",
                    "aenv-compose/service-0:local"
                ),
                (
                    "b",
                    "compose_1",
                    "/mnt/compose_1",
                    "aenv-compose/service-1:local"
                ),
            ]
        );
    }
}
