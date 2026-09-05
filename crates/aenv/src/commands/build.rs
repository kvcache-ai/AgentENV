use std::{path::PathBuf, process::Stdio, time::Duration};

use anyhow::{bail, ensure, Context, Result};
use clap::Args as ClapArgs;
use parse_dockerfile::Instruction;
use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    process::Command,
};

use crate::client::{
    handle_status,
    templates::{CreateTemplateV3, TemplateV3Response},
    Client,
};
use crate::progress::BuildProgress;

#[derive(Clone, ClapArgs)]
pub struct Args {
    /// Dockerfile path (or directory containing Dockerfile)
    dockerfile: PathBuf,
    /// Template name
    #[arg(long)]
    name: String,
    #[command(flatten)]
    resources: super::CpuMemoryArgs,
    /// Override the first FROM image
    #[arg(long = "image", alias = "user-image")]
    user_image: Option<String>,
    /// Build context directory (defaults to the Dockerfile's directory)
    #[arg(long)]
    context: Option<PathBuf>,
    /// Dockerfile stage to publish
    #[arg(long)]
    target: Option<String>,
    /// Build argument, KEY=VALUE; repeatable
    #[arg(long = "build-arg")]
    build_args: Vec<String>,
    /// BuildKit secret, for example id=token,src=./token; repeatable
    #[arg(long)]
    secret: Vec<String>,
    /// SSH agent forwarding, for example default; repeatable
    #[arg(long)]
    ssh: Vec<String>,
    /// Disable instruction cache for this build
    #[arg(long)]
    no_cache: bool,
    /// Exclusive cache volume name (defaults to a name derived from the template)
    #[arg(long)]
    cache_volume: Option<String>,
    /// Size of a newly created cache volume in MiB
    #[arg(long, default_value_t = 16384, value_parser = clap::value_parser!(u64).range(1024..))]
    cache_size: u64,
    /// OCI image containing buildkitd and buildctl
    #[arg(long, default_value = "docker.io/moby/buildkit:v0.33.0")]
    builder_image: String,
    /// Builder VM CPU cores
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u32).range(1..))]
    builder_cpu: u32,
    /// Builder VM memory in MiB
    #[arg(long, default_value_t = 2048, value_parser = clap::value_parser!(u32).range(256..))]
    builder_memory: u32,
    /// Path to the local BuildKit client executable
    #[arg(long, default_value = "buildctl")]
    buildctl: PathBuf,
    /// Build progress format
    #[arg(long, default_value = "auto", value_parser = ["auto", "plain", "tty"])]
    progress: String,
    /// Build deadline in seconds, plus 10 minutes for provisioning and publication
    #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u32).range(1..=86400))]
    timeout: u32,
    /// Override image ENTRYPOINT/CMD; an empty value disables startup
    #[arg(long)]
    start_cmd: Option<String>,
    /// Command that must succeed before the template is captured
    #[arg(long)]
    ready_cmd: Option<String>,
}

pub fn run(args: Args) -> Result<()> {
    let context = BuildContext::prepare(&args)?;
    let version = std::process::Command::new(&args.buildctl)
        .arg("--version")
        .output()
        .with_context(|| {
            format!(
                "run {}: rerun the aenv installer to install buildctl, or set --buildctl",
                args.buildctl.display()
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
    let mut interrupt = tokio::spawn(interrupted());
    let request = json!({
        "template": CreateTemplateV3 { name: args.name.clone(), tags: vec![], cpu_count: args.resources.cpu_count, memory_mb: args.resources.memory_mb },
        "builderImage": args.builder_image,
        "builderCPUCount": args.builder_cpu,
        "builderMemoryMB": args.builder_memory,
        "cacheVolume": args.cache_volume,
        "cacheSizeMB": args.cache_size,
        "timeout": args.timeout,
        "startCmd": args.start_cmd,
        "readyCmd": args.ready_cmd,
    });
    let start_client = client.clone();
    // Finish allocating the build before acting on cancellation so its ID is
    // available for the server to release the worker.
    let session: TemplateV3Response = tokio::task::spawn_blocking(move || -> Result<_> {
        Ok(
            handle_status(start_client.post("/templates/builds").send_json(request))?
                .into_json()?,
        )
    })
    .await??;
    println!(
        "Created template {} (build {})",
        session.template_id, session.build_id
    );
    let progress = BuildProgress::new(args.progress == "auto")?;
    progress.stage(0, "Preparing template builder");
    let result = tokio::select! {
        biased;
        signal = &mut interrupt => signal.context("signal handler failed").and_then(|r| r).and_then(|()| Err(anyhow::anyhow!("build interrupted"))),
        result = tokio::time::timeout(Duration::from_secs(u64::from(args.timeout) + 600), build(&client, &session, &args, &context, &progress)) => result.context("build deadline exceeded").and_then(|r| r),
    };
    interrupt.abort();
    if result.is_ok() {
        progress.finish();
    }
    drop(progress);
    if let Err(error) = result {
        let path = builder_path(&session);
        let cleanup = tokio::task::spawn_blocking(move || {
            handle_status(client.delete(&path).call()).map(|_| ())
        })
        .await?;
        if let Err(cleanup) = cleanup {
            eprintln!(
                "Build cleanup: {cleanup:#}. Check build {} with `aenv template watch`.",
                session.build_id
            );
        }
        return Err(error);
    }
    println!("Template {} is ready.", session.template_id);
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
        let client = client.clone();
        let id = session.template_id.clone();
        let build_id = session.build_id.clone();
        let status =
            tokio::task::spawn_blocking(move || client.template_build_status(&id, &build_id))
                .await??;
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
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let address = format!("tcp://{}", listener.local_addr()?);
    let command = async {
        let metadata = tempfile::tempdir()?;
        let metadata_path = metadata.path().join("result.json");
        let mut command = Command::new(&args.buildctl);
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
        if let Some(target) = &args.target {
            command.arg("--opt").arg(format!("target={target}"));
        }
        for arg in &args.build_args {
            command.arg("--opt").arg(format!("build-arg:{arg}"));
        }
        for secret in &args.secret {
            command.arg("--secret").arg(secret);
        }
        for ssh in &args.ssh {
            command.arg("--ssh").arg(ssh);
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
    let publish_client = client.clone();
    tokio::task::spawn_blocking(move || {
        handle_status(
            publish_client
                .post(&path)
                .send_json(json!({"digest": digest})),
        )
        .map(|_| ())
    })
    .await??;
    wait_for_status(client, session, "ready").await
}

struct BuildContext {
    context: PathBuf,
    dockerfile_dir: PathBuf,
    filename: String,
    _override_dir: Option<tempfile::TempDir>,
}

impl BuildContext {
    fn prepare(args: &Args) -> Result<Self> {
        let file = if args.dockerfile.is_dir() {
            args.dockerfile.join("Dockerfile")
        } else {
            args.dockerfile.clone()
        }
        .canonicalize()
        .context("locate Dockerfile")?;
        ensure!(file.is_file(), "Dockerfile must be a regular file");
        let dir = file
            .parent()
            .context("Dockerfile has no parent directory")?;
        let context = args
            .context
            .as_deref()
            .unwrap_or(dir)
            .canonicalize()
            .context("locate build context")?;
        ensure!(context.is_dir(), "build context must be a directory");
        ensure!(
            context.to_str().is_some() && dir.to_str().is_some(),
            "BuildKit requires UTF-8 context paths"
        );
        let filename = file
            .file_name()
            .and_then(|s| s.to_str())
            .context("Dockerfile filename must be UTF-8")?
            .to_owned();
        let mut result = Self {
            context,
            dockerfile_dir: dir.to_owned(),
            filename,
            _override_dir: None,
        };
        if let Some(image) = &args.user_image {
            let source = std::fs::read_to_string(&file)?;
            let overridden = override_first_image(&source, image)?;
            let work = tempfile::tempdir()?;
            std::fs::write(work.path().join(&result.filename), overridden)?;
            let ignore = format!("{}.dockerignore", result.filename);
            if dir.join(&ignore).is_file() {
                std::fs::copy(dir.join(&ignore), work.path().join(ignore))?;
            }
            result.dockerfile_dir = work.path().to_owned();
            result._override_dir = Some(work);
        }
        Ok(result)
    }
}

fn override_first_image(source: &str, image: &str) -> Result<String> {
    ensure!(
        !image.is_empty() && !image.chars().any(char::is_whitespace),
        "--image must be a single image reference"
    );
    let parsed =
        parse_dockerfile::parse(source).context("parse Dockerfile for --image override")?;
    let from = parsed
        .instructions
        .iter()
        .find_map(|i| match i {
            Instruction::From(from) => Some(from),
            _ => None,
        })
        .context("Dockerfile has no FROM instruction")?;
    let mut updated = source.to_owned();
    updated.replace_range(from.image.span.clone(), image);
    Ok(updated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_override_preserves_stages_and_parser_directives() {
        let source = "# escape=`\nARG BASE=alpine\nFROM --platform=linux/amd64 ${BASE} AS build\nRUN true\nFROM build AS release\n";
        assert_eq!(
            override_first_image(source, "ubuntu:24.04").unwrap(),
            source.replace("${BASE} AS", "ubuntu:24.04 AS")
        );
        assert!(override_first_image(source, "alpine\nRUN false").is_err());
    }

    #[test]
    fn command_accepts_legacy_flags_and_buildkit_inputs() {
        use clap::{CommandFactory, Parser};
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            args: Args,
        }
        Cli::command().debug_assert();
        let args = Cli::try_parse_from([
            "aenv",
            "Dockerfile",
            "--name",
            "demo",
            "--user-image",
            "alpine",
            "--cpu-count",
            "2",
            "--memory-mb",
            "512",
            "--build-arg",
            "VALUE=a b",
            "--target",
            "release",
        ])
        .unwrap()
        .args;
        assert_eq!(args.resources.cpu_count, Some(2));
        assert_eq!(args.build_args, ["VALUE=a b"]);
        assert_eq!(args.target.as_deref(), Some("release"));
    }
}
