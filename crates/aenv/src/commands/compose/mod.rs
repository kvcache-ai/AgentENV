//! `aenv compose up`: plans a Compose project locally, builds the services that
//! declare `build`, cold-starts the Compose guest image with one writable drive
//! per service and starts the project inside it.

mod build;
mod guest;
mod image;
mod plan;

use super::start::{parse_disk_size_mb, wait_for_envd};
use crate::client::sandboxes::{AttachedDrive, AttachedDriveSource, NewColdSandbox, Sandbox};
use crate::client::{handle_status, Client};
use anyhow::{anyhow, Context, Result};
use clap::{Args as ClapArgs, Subcommand};
use std::collections::BTreeMap;
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
  aenv compose up
  aenv compose up -f compose.yaml -f compose.prod.yaml --profile worker
  cat compose.yaml | aenv compose up -f -
  aenv compose up --base-image registry.example.com/aenv-compose:v1 -f compose.yaml
  aenv compose up -p web --no-build

Each invocation creates a new sandbox. Manage it with aenv exec, connect, pause,
resume, snapshot, and delete. The project is loaded as Docker Compose loads it,
including the process environment and the project's .env file. Without
--base-image, the first run builds the bundled guest image with the server's
builder.

Services that declare build: are first built like aenv compose build. With
--no-build, they use aenv/PROJECT-SERVICE:latest instead.")]
struct UpArgs {
    #[command(flatten)]
    project: plan::ProjectArgs,
    /// Compose guest image built from images/compose/Dockerfile [default: the
    /// bundled image, built on first use]
    #[arg(long, value_name = "IMAGE")]
    base_image: Option<String>,
    /// Do not build services; use their published aenv/PROJECT-SERVICE:latest images
    #[arg(long)]
    no_build: bool,
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
}

pub fn run(args: Args) -> Result<()> {
    match args.cmd {
        Sub::Up(args) => up(args),
        Sub::Build(args) => build::run(args),
    }
}

fn up(args: UpArgs) -> Result<()> {
    let rt = super::tokio_rt()?;
    let mut planned = rt.block_on(args.project.plan())?;
    let client = Client::from_env()?;
    let built = if args.no_build {
        build::latest_images(&planned)?
    } else if planned
        .services
        .iter()
        .any(|service| service.build.is_some())
    {
        let mut buildkit = super::build::BuildkitArgs::default();
        buildkit.check()?;
        rt.block_on(build::build_services(
            &client, &planned, &buildkit, None, false,
        ))?
    } else {
        BTreeMap::new()
    };
    let (drives, services) = service_drives(&mut planned, &built)?;
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
                result = start(&client, &sandbox, &planned, &services, budget) => result,
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
    planned: &plan::Plan,
    services: &[guest::Service],
    budget: Duration,
) -> Result<()> {
    let token = sandbox.envd_access_token.as_deref();
    wait_for_envd(client, &sandbox.sandbox_id, token).await?;
    let transport = client.transport(&sandbox.sandbox_id, token)?;
    guest::start(
        &transport,
        &planned.project_name,
        &planned.compose,
        services,
        budget,
    )
    .await
}

/// Gives every service its own writable image drive, even when services share
/// a source image, and points the rendered project at the image registered
/// from that drive. `built` maps services that declare build, or run another
/// service's built image, to their images.
fn service_drives(
    planned: &mut plan::Plan,
    built: &BTreeMap<String, String>,
) -> Result<(Vec<AttachedDrive>, Vec<guest::Service>)> {
    let mut drives = Vec::new();
    let mut services = Vec::new();
    for (index, service) in planned.services.iter().enumerate() {
        let source = built
            .get(&service.name)
            .or(service.image.as_ref())
            .with_context(|| format!("service {} has no image", service.name))?;
        let drive_id = format!("compose_{index}");
        let mount_path = format!("/mnt/compose_{index}");
        let local_image = format!("aenv-compose/service-{index}:local");
        planned.compose["services"][&service.name]["image"] = local_image.clone().into();
        drives.push(AttachedDrive {
            drive_id: drive_id.clone(),
            read_only: false,
            mount_path: mount_path.clone(),
            source: AttachedDriveSource {
                image: source.clone(),
            },
        });
        services.push(guest::Service {
            name: service.name.clone(),
            drive_id,
            mount_path,
            local_image,
        });
    }
    Ok((drives, services))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};
    use std::path::PathBuf;

    fn parse(arguments: &[&str]) -> UpArgs {
        let crate::Cmd::Compose(Args { cmd: Sub::Up(args) }) =
            crate::Cli::try_parse_from(arguments).unwrap().cmd
        else {
            panic!("expected compose up");
        };
        args
    }

    #[test]
    fn accepts_project_flags_and_resource_aliases() {
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
            "--env-file",
            "prod.env",
            "--project-directory",
            "app",
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
            "-p",
            "task",
            "--no-build",
        ]);
        assert_eq!(args.project.files, [PathBuf::from("-")]);
        assert_eq!(
            args.base_image.as_deref(),
            Some("registry.example.com/aenv-compose:v1")
        );
        assert_eq!(args.project.profiles, ["web", "worker"]);
        assert_eq!(
            args.project.environment,
            [("VALUE".into(), "a=b".into()), ("EMPTY".into(), "".into())]
        );
        assert_eq!(args.project.env_files, [PathBuf::from("prod.env")]);
        assert_eq!(args.project.project_directory, Some(PathBuf::from("app")));
        assert_eq!(args.resources.cpu_count, Some(2));
        assert_eq!(args.resources.memory_mb, Some(2048));
        assert_eq!(args.disk_size_mb, Some(8192));
        assert_eq!(args.timeout, 600);
        assert_eq!(args.startup_timeout, 120);
        assert_eq!(
            args.project.planner,
            Some(PathBuf::from("/opt/aenv-compose-plan"))
        );
        assert_eq!(args.project.project_name.as_deref(), Some("task"));
        assert!(args.no_build);
        let defaults = parse(&["aenv", "compose", "up"]);
        assert!(defaults.project.files.is_empty());
        assert!(defaults.base_image.is_none());
        assert!(defaults.project.environment.is_empty());
        assert!(defaults.project.profiles.is_empty());
        assert!(!defaults.resources.is_set());
        assert_eq!(defaults.timeout, 300);
        assert_eq!(defaults.startup_timeout, 300);
        assert!(defaults.project.planner.is_none());
        assert!(defaults.project.project_name.is_none() && !defaults.no_build);
        // --name remains an alias of --project-name.
        let legacy = parse(&["aenv", "compose", "up", "--name", "task"]);
        assert_eq!(legacy.project.project_name.as_deref(), Some("task"));
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
        let mut planned: plan::Plan = serde_json::from_value(serde_json::json!({
            "projectName": "app",
            "projectDirectory": "/work/app",
            "composeFiles": ["/work/app/compose.yaml"],
            "compose": {"name": "app", "services": {
                "a": {"image": "busybox:1.37"},
                "b": {"image": "busybox:1.37"},
                "c": {"command": ["true"]},
            }},
            "services": [
                {"name": "a", "image": "busybox:1.37"},
                {"name": "b", "image": "busybox:1.37"},
                {"name": "c", "build": {"context": "/work/app", "dockerfile": "Dockerfile"}},
            ],
        }))
        .unwrap();
        let built = BTreeMap::from([("c".to_owned(), "sha256:abc".to_owned())]);
        let (drives, guest) = service_drives(&mut planned, &built).unwrap();
        assert_eq!(
            serde_json::to_value(&drives).unwrap(),
            serde_json::json!([
                {"driveID": "compose_0", "readOnly": false, "mountPath": "/mnt/compose_0",
                 "source": {"image": "busybox:1.37"}},
                {"driveID": "compose_1", "readOnly": false, "mountPath": "/mnt/compose_1",
                 "source": {"image": "busybox:1.37"}},
                {"driveID": "compose_2", "readOnly": false, "mountPath": "/mnt/compose_2",
                 "source": {"image": "sha256:abc"}},
            ])
        );
        let guest: Vec<_> = guest
            .iter()
            .map(|s| {
                (
                    s.name.as_str(),
                    s.drive_id.as_str(),
                    s.mount_path.as_str(),
                    s.local_image.as_str(),
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
                (
                    "c",
                    "compose_2",
                    "/mnt/compose_2",
                    "aenv-compose/service-2:local"
                ),
            ]
        );
        assert_eq!(
            planned.compose["services"]["c"],
            serde_json::json!({"command": ["true"], "image": "aenv-compose/service-2:local"})
        );
        let mut missing: plan::Plan = serde_json::from_value(serde_json::json!({
            "projectName": "app", "projectDirectory": "/work/app", "composeFiles": [],
            "compose": {"services": {"c": {}}},
            "services": [{"name": "c", "build": {"context": "/work/app", "dockerfile": "Dockerfile"}}],
        }))
        .unwrap();
        assert!(service_drives(&mut missing, &BTreeMap::new()).is_err());
    }
}
