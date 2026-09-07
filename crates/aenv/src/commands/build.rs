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
pub struct Args {
    /// Local build context directory
    context: PathBuf,
    /// Dockerfile path (defaults to CONTEXT/Dockerfile)
    #[arg(short = 'f', long = "file")]
    dockerfile: Option<PathBuf>,
    /// Template name
    #[arg(long)]
    name: String,
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
    /// Disable instruction cache for this build
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
        "template": CreateTemplateV3 { name: args.name.clone(), tags: vec![], cpu_count: args.resources.cpu_count, memory_mb: args.resources.memory_mb },
        "timeout": args.timeout,
        "startCmd": args.start_cmd,
        "readyCmd": args.ready_cmd,
    });
    let mut session = None;
    let progress = BuildProgress::new(args.progress == "auto")?;
    let operation = async {
        let allocated: TemplateV3Response = serde_json::from_slice(
            &client
                .build_request(Method::POST, "/templates/builds", Some(request))
                .await?,
        )?;
        println!(
            "Created template {} (build {})",
            allocated.template_id, allocated.build_id
        );
        let allocated = session.insert(allocated);
        progress.stage(0, "Preparing template builder");
        build(&client, allocated, &args, &context, &progress).await
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
    if let Err(error) = result {
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
    println!(
        "Template {} is ready.",
        session.context("missing build session")?.template_id
    );
    Ok(())
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
) -> Result<()> {
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
            return Ok(());
        }
        match status.status.as_str() {
            "waiting" | "building" => tokio::time::sleep(Duration::from_secs(1)).await,
            "error" => bail!(
                "template build failed: {}",
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
) -> Result<()> {
    let path = builder_path(session);
    wait_for_status(client, session, "building").await?;
    progress.stage(1, "Building image");
    let (work, listener, address) = buildkit::bind_local().await?;
    let command = async {
        let metadata_path = work.path().join("result.json");
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
            .args([
                "--output",
                "type=image,name=aenv-build,oci-mediatypes=true",
                "--metadata-file",
            ])
            .arg(&metadata_path)
            .stdin(Stdio::null())
            .kill_on_drop(true);
        for arg in &args.build_args {
            command.arg("--opt").arg(format!("build-arg:{arg}"));
        }
        for secret in &args.secret {
            command.arg("--secret").arg(secret);
        }
        if args.no_cache {
            command.args(["--opt", "no-cache"]);
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
        let metadata: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(metadata_path).await?)?;
        let digest = metadata["containerimage.digest"]
            .as_str()
            .context("BuildKit did not return an image digest")?
            .to_owned();
        Ok::<_, anyhow::Error>(digest)
    };
    let digest = tokio::select! {
        result = command => result?,
        result = client.buildkit_tunnel(&path, listener) => { result?; bail!("BuildKit tunnel closed"); }
    };
    progress.stage(2, "Converting image and publishing template");
    client
        .build_request(Method::POST, &path, Some(json!({"digest": digest})))
        .await?;
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
}
