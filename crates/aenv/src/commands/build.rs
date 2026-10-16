use std::{path::PathBuf, process::Stdio, time::Duration};

use anyhow::{bail, ensure, Context, Result};
use clap::Args as ClapArgs;
use reqwest::Method;
use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};

use crate::client::{
    buildkit,
    templates::{CreateTemplateV3, TemplateBuildInfo, TemplateV3Response},
    Client,
};
use crate::progress::BuildProgress;

#[derive(Clone, ClapArgs)]
#[command(after_help = "\
Examples:
  aenv build --name my-ubuntu .
  aenv build --name my-python ./my-python
  aenv build --name my-app -f ./my-app/Dockerfile.custom ./my-app
  aenv build --image --name my-app ./my-app
  aenv build --image --name my-app:v1 --tag latest ./my-app
")]
pub struct Args {
    /// Local build context directory
    context: PathBuf,
    /// Dockerfile path (defaults to CONTEXT/Dockerfile)
    #[arg(short = 'f', long = "file")]
    dockerfile: Option<PathBuf>,
    /// Template name, or with --image the image name as NAME[:TAG]
    #[arg(long)]
    name: String,
    /// Build an immutable image, referenced as aenv/NAME:TAG, and print its digest
    #[arg(long, conflicts_with_all = ["start_cmd", "ready_cmd", "cpu_count", "memory_mb"])]
    image: bool,
    /// Point another tag of the image name at the built image; repeatable
    #[arg(long = "tag", requires = "image", value_name = "TAG")]
    tags: Vec<String>,
    #[command(flatten)]
    resources: super::CpuMemoryArgs,
    /// Override image ENTRYPOINT/CMD; an empty value disables startup
    #[arg(long)]
    start_cmd: Option<String>,
    /// Override the image HEALTHCHECK with a command that must succeed before capture
    #[arg(long)]
    ready_cmd: Option<String>,
    /// Build argument, KEY=VALUE; repeatable
    #[arg(long = "build-arg")]
    build_args: Vec<String>,
    /// BuildKit secret, for example id=token,src=./token; repeatable
    #[arg(long)]
    secret: Vec<String>,
    /// Rebuild without cached instructions or their cache mounts
    #[arg(long)]
    no_cache: bool,
    /// Path to the local BuildKit client executable
    #[arg(long)]
    buildctl: Option<PathBuf>,
    /// Build progress format
    #[arg(long, default_value = "auto", value_parser = ["auto", "plain", "tty"])]
    progress: String,
    /// Build deadline in seconds, plus 10 minutes for provisioning and publication
    #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u32).range(1..=86400))]
    timeout: u32,
}

/// Build the executor bundled with the Codex command using the normal builder.
#[cfg(target_os = "linux")]
pub(super) fn codex_template(context: PathBuf, name: String) -> Result<()> {
    run(Args {
        context,
        dockerfile: None,
        name,
        image: false,
        tags: vec![],
        resources: super::CpuMemoryArgs {
            cpu_count: Some(2),
            memory_mb: Some(1024),
        },
        start_cmd: Some(String::new()),
        ready_cmd: Some("true".into()),
        build_args: vec![],
        secret: vec![],
        no_cache: false,
        buildctl: None,
        progress: "auto".into(),
        timeout: 3600,
    })
}

pub fn run(mut args: Args) -> Result<()> {
    ensure!(
        cfg!(unix),
        "Dockerfile builds require a private Unix socket (Linux or macOS)"
    );
    if args.buildctl.is_none() {
        args.buildctl = Some(std::env::current_exe()?.with_file_name("aenv-buildctl"));
    }
    let context = BuildContext::prepare(&args)?;
    let buildctl = args
        .buildctl
        .as_ref()
        .context("missing BuildKit client path")?;
    let version = std::process::Command::new(buildctl)
        .arg("--version")
        .output()
        .with_context(|| {
            format!(
                "run {}: rerun the aenv installer to install buildctl, or set --buildctl",
                buildctl.display()
            )
        })?;
    ensure!(version.status.success(), "buildctl --version failed");
    ensure!(
        args.resources.cpu_count != Some(0) && args.resources.memory_mb != Some(0),
        "CPU and memory must be greater than zero"
    );
    let client = Client::from_env()?;
    super::tokio_rt()?.block_on(run_async(client, args, context))
}

async fn run_async(client: Client, args: Args, context: BuildContext) -> Result<()> {
    let request = json!({
        "timeout": args.timeout,
        "startCmd": args.start_cmd,
        "readyCmd": args.ready_cmd,
    });
    let mut session = None;
    let progress = BuildProgress::new(args.progress == "auto")?;
    let operation = async {
        let allocated: TemplateV3Response = serde_json::from_slice(
            &client
                .build_request(
                    Method::POST,
                    "/v3/templates",
                    Some(serde_json::to_value(CreateTemplateV3 {
                        name: args.name.clone(),
                        tags: args.tags.clone(),
                        cpu_count: args.resources.cpu_count,
                        memory_mb: args.resources.memory_mb,
                        image_only: args.image,
                    })?),
                )
                .await?,
        )?;
        if args.image {
            // Stdout carries only the image digest.
            eprintln!("Created image build {}", allocated.build_id);
        } else {
            println!(
                "Created template {} (build {})",
                allocated.template_id, allocated.build_id
            );
        }
        let allocated = session.insert(allocated);
        progress.stage(
            0,
            if args.image {
                "Preparing image builder"
            } else {
                "Preparing template builder"
            },
        );
        let builder: Builder = serde_json::from_slice(
            &client
                .build_request(Method::PUT, &builder_path(allocated), Some(request))
                .await?,
        )?;
        build(
            &client,
            allocated,
            &args,
            &context,
            &progress,
            &builder.image_name,
        )
        .await
    };
    let result = tokio::select! {
        biased;
        signal = interrupted() => signal.and_then(|()| Err(anyhow::anyhow!("build interrupted"))),
        result = tokio::time::timeout(Duration::from_secs(u64::from(args.timeout) + 600), operation) => result.context("build deadline exceeded").and_then(|r| r),
    };
    if result.is_ok() {
        progress.finish();
    }
    drop(progress);
    let built = match result {
        Ok(built) => built,
        Err(error) => {
            if let Some(session) = session {
                let path = builder_path(&session);
                let cleanup = tokio::time::timeout(
                    Duration::from_secs(5),
                    client.build_request(Method::DELETE, &path, None),
                )
                .await
                .context("cleanup request timed out")
                .and_then(|result| result);
                if let Err(cleanup) = cleanup {
                    eprintln!(
                        "Build cleanup: {cleanup:#}. Check build {} with `aenv template watch`.",
                        session.build_id
                    );
                }
            }
            return Err(error);
        }
    };
    let session = session.context("missing build session")?;
    if !args.image {
        println!("Template {} is ready.", session.template_id);
        return Ok(());
    }
    let digest = built
        .image_digest
        .context("server does not report the built image digest; upgrade the AgentENV server")?;
    println!("{digest}");
    for name in &session.names {
        for tag in &session.tags {
            eprintln!("Tagged aenv/{name}:{tag}");
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct Builder {
    #[serde(rename = "imageName")]
    image_name: String,
}

fn builder_path(session: &TemplateV3Response) -> String {
    format!(
        "/templates/{}/builds/{}/builder",
        session.template_id, session.build_id
    )
}

async fn wait_for_status(
    client: &Client,
    session: &TemplateV3Response,
    expected: &str,
) -> Result<TemplateBuildInfo> {
    loop {
        let path = format!(
            "/templates/{}/builds/{}/status",
            session.template_id, session.build_id
        );
        let status: TemplateBuildInfo =
            serde_json::from_slice(&client.build_request(Method::GET, &path, None).await?)?;
        ensure!(
            status.template_id == session.template_id && status.build_id == session.build_id,
            "build status response ID mismatch"
        );
        if status.status == expected {
            return Ok(status);
        }
        match status.status.as_str() {
            "waiting" | "building" => tokio::time::sleep(Duration::from_secs(1)).await,
            "error" => bail!(
                "build failed: {}",
                status
                    .reason
                    .map_or_else(|| "unknown error".into(), |r| r.message)
            ),
            other => bail!("unexpected build status: {other}"),
        }
    }
}

async fn interrupted() -> Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn build(
    client: &Client,
    session: &TemplateV3Response,
    args: &Args,
    context: &BuildContext,
    progress: &BuildProgress,
    image_name: &str,
) -> Result<TemplateBuildInfo> {
    let path = builder_path(session);
    wait_for_status(client, session, "building").await?;
    progress.stage(1, "Building image");
    let (_work, listener, address) = buildkit::bind_local().await?;
    let command = async {
        let mut command = Command::new(
            args.buildctl
                .as_ref()
                .context("missing BuildKit client path")?,
        );
        command
            .args([
                "--addr",
                &address,
                "build",
                "--frontend",
                "dockerfile.v0",
                "--progress",
                if args.progress == "auto" {
                    "plain"
                } else {
                    &args.progress
                },
            ])
            .arg("--local")
            .arg(format!("context={}", context.context.display()))
            .arg("--local")
            .arg(format!("dockerfile={}", context.dockerfile_dir.display()))
            .arg("--opt")
            .arg(format!("filename={}", context.filename))
            .arg("--output")
            .arg(format!("type=image,name={image_name},oci-mediatypes=true"))
            .stdin(Stdio::null())
            .kill_on_drop(true);
        if args.image {
            command.stdout(Stdio::null());
        }
        for arg in &args.build_args {
            command.arg("--opt").arg(format!("build-arg:{arg}"));
        }
        for secret in &args.secret {
            command.arg("--secret").arg(secret);
        }
        if args.no_cache {
            command.arg("--no-cache");
        }
        if progress.visible() {
            command.stderr(Stdio::piped());
        }
        let mut child = command.spawn().context("start buildctl")?;
        if let Some(stderr) = child.stderr.take() {
            let mut lines = BufReader::new(stderr).lines();
            while let Some(line) = lines.next_line().await? {
                progress.println(&line);
            }
        }
        let status = child.wait().await?;
        ensure!(status.success(), "BuildKit build failed ({status})");
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! {
        result = command => result?,
        result = client.buildkit_tunnel(&path, listener) => { result?; bail!("BuildKit tunnel closed"); }
    }
    progress.stage(
        2,
        if args.image {
            "Publishing image to the shared repository"
        } else {
            "Converting image and publishing template"
        },
    );
    wait_for_status(client, session, "ready").await
}

struct BuildContext {
    context: PathBuf,
    dockerfile_dir: PathBuf,
    filename: String,
}

impl BuildContext {
    fn prepare(args: &Args) -> Result<Self> {
        let context = args
            .context
            .canonicalize()
            .context("locate build context")?;
        ensure!(
            context.is_dir(),
            "build context must be a directory; use -f <Dockerfile> to select a Dockerfile"
        );
        let file = args
            .dockerfile
            .clone()
            .unwrap_or_else(|| context.join("Dockerfile"))
            .canonicalize()
            .context("locate Dockerfile")?;
        ensure!(file.is_file(), "Dockerfile must be a regular file");
        let dir = file
            .parent()
            .context("Dockerfile has no parent directory")?;
        ensure!(
            context.to_str().is_some() && dir.to_str().is_some(),
            "BuildKit requires UTF-8 context paths"
        );
        let filename = file
            .file_name()
            .and_then(|s| s.to_str())
            .context("Dockerfile filename must be UTF-8")?
            .to_owned();
        Ok(Self {
            context,
            dockerfile_dir: dir.to_owned(),
            filename,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: Args,
    }

    #[test]
    fn command_uses_docker_context_and_file_arguments() {
        Cli::command().debug_assert();
        let args = Cli::try_parse_from([
            "aenv",
            ".",
            "-f",
            "deploy/docker/Dockerfile.agentenv",
            "--name",
            "demo",
            "--cpu-count",
            "2",
            "--memory-mb",
            "512",
            "--build-arg",
            "VALUE=a b",
        ])
        .unwrap()
        .args;
        assert_eq!(args.resources.cpu_count, Some(2));
        assert_eq!(args.build_args, ["VALUE=a b"]);
        assert_eq!(args.context, PathBuf::from("."));
        assert_eq!(
            args.dockerfile,
            Some("deploy/docker/Dockerfile.agentenv".into())
        );
        for flag in [
            "--image",
            "--user-image",
            "--context",
            "--target",
            "--ssh",
            "--builder-image",
            "--builder-cpu",
            "--builder-memory",
            "--cache-size",
            "--cache-volume",
        ] {
            assert!(
                Cli::try_parse_from(["aenv", ".", "--name", "demo", flag, "value"]).is_err(),
                "{flag} should be rejected"
            );
        }
    }

    #[test]
    fn command_accepts_startup_overrides_and_explicit_empty_startup() {
        for start in ["exec /server --port 8080", ""] {
            let args = Cli::try_parse_from([
                "aenv",
                ".",
                "--name",
                "demo",
                "--start-cmd",
                start,
                "--ready-cmd",
                "test -f /ready",
            ])
            .unwrap()
            .args;
            assert_eq!(args.start_cmd.as_deref(), Some(start));
            assert_eq!(args.ready_cmd.as_deref(), Some("test -f /ready"));
        }
        let args = Cli::try_parse_from(["aenv", ".", "--name", "demo"])
            .unwrap()
            .args;
        assert!(args.start_cmd.is_none());
        assert!(args.ready_cmd.is_none());
    }

    #[test]
    fn dockerfile_location_does_not_change_context_root() -> Result<()> {
        let work = tempfile::tempdir()?;
        let context = work.path().join("context");
        let dockerfiles = work.path().join("dockerfiles");
        std::fs::create_dir(&context)?;
        std::fs::create_dir(&dockerfiles)?;
        let custom = dockerfiles.join("Custom.Dockerfile");
        std::fs::write(&custom, "FROM scratch\n")?;
        std::fs::write(context.join("Dockerfile"), "FROM scratch\n")?;
        let mut args =
            Cli::try_parse_from(["aenv", context.to_str().unwrap(), "--name", "demo"])?.args;
        let prepared = BuildContext::prepare(&args)?;
        assert_eq!(prepared.context, context.canonicalize()?);
        assert_eq!(prepared.dockerfile_dir, prepared.context);
        args.dockerfile = Some(custom.clone());
        let prepared = BuildContext::prepare(&args)?;
        assert_eq!(prepared.context, context.canonicalize()?);
        assert_eq!(prepared.dockerfile_dir, dockerfiles.canonicalize()?);
        assert_eq!(prepared.filename, "Custom.Dockerfile");
        args.context = custom;
        assert!(BuildContext::prepare(&args)
            .err()
            .unwrap()
            .to_string()
            .contains("use -f"));
        Ok(())
    }

    #[test]
    fn native_image_flags_preserve_template_builds() {
        crate::Cli::command().debug_assert();
        for (args, valid) in [
            ("aenv build --image --name web .", true),
            ("aenv build --image --name web:v1 .", true),
            (
                "aenv build --image --name web . -f Dockerfile --build-arg A=B --secret id=token,src=token",
                true,
            ),
            ("aenv build --image .", false),
            ("aenv build --image --name web", false),
            ("aenv build --image --name web . --start-cmd true", false),
            ("aenv build --image --name web . --ready-cmd true", false),
            ("aenv build --image --name web . --cpu 2", false),
            ("aenv build --image --name web . --memory 512", false),
            ("aenv build --image --name web . --tag latest --tag v1.2_rc-1", true),
            ("aenv build . --name template --tag v1", false),
        ] {
            assert_eq!(
                crate::Cli::try_parse_from(args.split_whitespace()).is_ok(),
                valid,
                "{args}"
            );
        }
        let Ok(crate::Cli {
            cmd: crate::Cmd::Build(args),
            ..
        }) = crate::Cli::try_parse_from([
            "aenv", "build", "--image", "--name", "web:v1", ".", "--tag", "latest",
        ])
        else {
            panic!("image build with a tag should parse");
        };
        assert_eq!(args.name, "web:v1");
        assert_eq!(args.tags, ["latest"]);
    }
}
