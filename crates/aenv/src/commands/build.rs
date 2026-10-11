use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

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
    templates::{BuildStatusReason, CreateTemplateV3, TemplateV3Response},
    Client,
};
use crate::progress::BuildProgress;

#[derive(Clone, ClapArgs)]
#[command(after_help = "\
Examples:
  aenv build --name my-ubuntu .
  aenv build --name my-python ./my-python
  aenv build --name my-app -f ./my-app/Dockerfile.custom ./my-app
  aenv build --image ./my-app
  aenv build --compose compose.yaml
")]
pub struct Args {
    /// Local build context directory
    #[arg(required_unless_present = "compose", conflicts_with = "compose")]
    context: Option<PathBuf>,
    /// Dockerfile path (defaults to CONTEXT/Dockerfile)
    #[arg(short = 'f', long = "file", conflicts_with = "compose")]
    dockerfile: Option<PathBuf>,
    /// Template name
    #[arg(long, required_unless_present_any = ["compose", "image"], conflicts_with_all = ["compose", "image"])]
    name: Option<String>,
    /// Build an immutable image and print its digest
    #[arg(long, conflicts_with_all = ["compose", "start_cmd", "ready_cmd", "cpu_count", "memory_mb"])]
    image: bool,
    #[command(flatten)]
    compose: compose::BuildArgs,
    #[command(flatten)]
    resources: super::CpuMemoryArgs,
    /// Override image ENTRYPOINT/CMD; an empty value disables startup
    #[arg(long, conflicts_with = "compose")]
    start_cmd: Option<String>,
    /// Override the image HEALTHCHECK with a command that must succeed before capture
    #[arg(long, conflicts_with = "compose")]
    ready_cmd: Option<String>,
    /// Build argument, KEY=VALUE; repeatable
    #[arg(long = "build-arg", conflicts_with = "compose")]
    build_args: Vec<String>,
    /// BuildKit secret, for example id=token,src=./token; repeatable
    #[arg(long, conflicts_with = "compose")]
    secret: Vec<String>,
    /// Rebuild without cached instructions or their cache mounts
    #[arg(long)]
    pub(super) no_cache: bool,
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
        context: Some(context),
        dockerfile: None,
        name: Some(name),
        image: false,
        compose: compose::BuildArgs::default(),
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
    if args.compose.compose.is_some() {
        return compose::run_build(client, args);
    }
    let build = Build {
        name: args.name.clone().unwrap_or_default(),
        context: BuildContext::prepare(
            args.context.as_deref().context("missing build context")?,
            args.dockerfile.as_deref(),
        )?,
        build_args: args.build_args.clone(),
        no_cache: args.no_cache,
        image: args.image.then_some(ImageBuild {
            target: None,
            push: None,
        }),
    };
    let result = super::tokio_rt()?.block_on(run_async(&client, &args, &build))?;
    if args.image {
        println!("{}", result.published_digest()?);
    }
    Ok(())
}

pub(super) struct Build {
    pub(super) name: String,
    pub(super) context: BuildContext,
    pub(super) build_args: Vec<String>,
    pub(super) no_cache: bool,
    pub(super) image: Option<ImageBuild>,
}

/// Publish a portable OverlayBD image; `push` also exports an OCI registry copy.
pub(super) struct ImageBuild {
    pub(super) target: Option<String>,
    pub(super) push: Option<ImagePush>,
}

pub(super) struct ImagePush {
    pub(super) image: String,
    pub(super) insecure: bool,
}

pub(super) async fn run_async(client: &Client, args: &Args, input: &Build) -> Result<BuildInfo> {
    let request = json!({
        "timeout": args.timeout,
        "startCmd": args.start_cmd,
        "readyCmd": args.ready_cmd,
    });
    let mut session = None;
    let progress = BuildProgress::new(args.progress == "auto")?;
    let operation = async {
        progress.stage(0, "Preparing builder");
        let builder: Builder = if input.image.is_some() {
            let builder: Builder = serde_json::from_slice(
                &client
                    .build_request(
                        Method::POST,
                        "/images/builds",
                        Some(json!({"timeout": args.timeout})),
                    )
                    .await?,
            )?;
            let build_id = builder
                .build_id
                .clone()
                .context("image build response has no build ID")?;
            eprintln!("Allocated image build {build_id}");
            session = Some(BuildSession {
                build_id,
                template_id: None,
            });
            builder
        } else {
            let allocated: TemplateV3Response = serde_json::from_slice(
                &client
                    .build_request(
                        Method::POST,
                        "/v3/templates",
                        Some(serde_json::to_value(CreateTemplateV3 {
                            name: input.name.clone(),
                            tags: vec![],
                            cpu_count: args.resources.cpu_count,
                            memory_mb: args.resources.memory_mb,
                        })?),
                    )
                    .await?,
            )?;
            println!(
                "Created template {} (build {})",
                allocated.template_id, allocated.build_id
            );
            let allocated = session.insert(BuildSession {
                build_id: allocated.build_id,
                template_id: Some(allocated.template_id),
            });
            serde_json::from_slice(
                &client
                    .build_request(Method::PUT, &builder_path(allocated), Some(request))
                    .await?,
            )?
        };
        build(
            client,
            session.as_ref().context("missing build session")?,
            args,
            input,
            &progress,
            &builder,
        )
        .await
    };
    let result = tokio::select! {
        biased;
        signal = interrupted() => signal.and_then(|()| Err(anyhow::anyhow!("build interrupted"))),
        result = tokio::time::timeout(Duration::from_secs(u64::from(args.timeout) + 600), operation) => result.context("build deadline exceeded; if the build finished without the server reporting it ready, upgrade the AgentENV server").and_then(|r| r),
    };
    if result.is_ok() {
        progress.finish();
    }
    drop(progress);
    if result.is_err() || input.image.is_some() {
        if let Some(session) = &session {
            delete_builder(client, session).await;
        }
    }
    let built = result?;
    if let Some(image) = &input.image {
        match &image.push {
            Some(push) => eprintln!("Image {} is ready.", push.image),
            None => eprintln!("Image is ready."),
        }
    } else {
        println!(
            "Template {} is ready.",
            session
                .context("missing build session")?
                .template_id
                .context("missing template ID")?
        );
    }
    Ok(built)
}

#[derive(serde::Deserialize)]
pub(super) struct BuildInfo {
    #[serde(rename = "buildID")]
    build_id: String,
    #[serde(default, rename = "templateID")]
    template_id: Option<String>,
    status: String,
    #[serde(default)]
    reason: Option<BuildStatusReason>,
    #[serde(default, rename = "imageDigest")]
    pub(super) image_digest: Option<String>,
}

impl BuildInfo {
    fn published_digest(&self) -> Result<&str> {
        let digest = self.image_digest.as_deref().context(
            "server does not report the built image digest; upgrade the AgentENV server",
        )?;
        validate_digest(digest)?;
        Ok(digest)
    }
}

fn validate_digest(digest: &str) -> Result<()> {
    ensure!(
        digest
            .strip_prefix("sha256:")
            .is_some_and(|hex| hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))),
        "server returned an invalid image digest; upgrade the AgentENV server"
    );
    Ok(())
}

struct BuildSession {
    build_id: String,
    template_id: Option<String>,
}

#[derive(serde::Deserialize)]
struct Builder {
    #[serde(rename = "imageName")]
    image_name: String,
    #[serde(default, rename = "buildID")]
    build_id: Option<String>,
}

fn builder_path(session: &BuildSession) -> String {
    match &session.template_id {
        Some(template) => format!("/templates/{template}/builds/{}/builder", session.build_id),
        None => format!("/images/builds/{}/builder", session.build_id),
    }
}

/// Best-effort builder release; the server keeps image build records until
/// this DELETE, so failure only leaves a stale record behind.
async fn delete_builder(client: &Client, session: &BuildSession) {
    let path = if session.template_id.is_some() {
        builder_path(session)
    } else {
        format!("/images/builds/{}", session.build_id)
    };
    let cleanup = tokio::time::timeout(
        Duration::from_secs(5),
        client.build_request(Method::DELETE, &path, None),
    )
    .await
    .context("cleanup request timed out")
    .and_then(|result| result);
    if let Err(cleanup) = cleanup {
        eprintln!(
            "Build cleanup for {}: {cleanup:#}. Retry deleting the build if needed.",
            session.build_id
        );
    }
}

async fn wait_for_status(
    client: &Client,
    session: &BuildSession,
    expected: &str,
) -> Result<BuildInfo> {
    loop {
        let path = match &session.template_id {
            Some(template) => format!("/templates/{template}/builds/{}/status", session.build_id),
            None => format!("/images/builds/{}", session.build_id),
        };
        let status: BuildInfo =
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
    session: &BuildSession,
    args: &Args,
    input: &Build,
    progress: &BuildProgress,
    builder: &Builder,
) -> Result<BuildInfo> {
    let path = builder_path(session);
    let context = &input.context;
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
            .arg(format!(
                "type=image,name={},oci-mediatypes=true",
                builder.image_name
            ))
            .stdin(Stdio::null())
            .kill_on_drop(true);
        if let Some(image) = &input.image {
            command.stdout(Stdio::null());
            if let Some(push) = &image.push {
                command.arg("--output").arg(format!(
                    "type=image,name={},push=true,oci-mediatypes=true{}",
                    push.image,
                    if push.insecure {
                        ",registry.insecure=true"
                    } else {
                        ""
                    }
                ));
            }
            command.args(["--opt", "platform=linux/amd64"]);
            if let Some(target) = &image.target {
                command.arg("--opt").arg(format!("target={target}"));
            }
        }
        for arg in &input.build_args {
            command.arg("--opt").arg(format!("build-arg:{arg}"));
        }
        for secret in &args.secret {
            command.arg("--secret").arg(secret);
        }
        if input.no_cache {
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
        if input.image.is_some() {
            "Publishing image to the shared repository"
        } else {
            "Converting image and publishing template"
        },
    );
    wait_for_status(client, session, "ready").await
}

pub(super) struct BuildContext {
    context: PathBuf,
    dockerfile_dir: PathBuf,
    filename: String,
}

impl BuildContext {
    pub(super) fn prepare(context: &Path, dockerfile: Option<&Path>) -> Result<Self> {
        let context = context.canonicalize().context("locate build context")?;
        ensure!(
            context.is_dir(),
            "build context must be a directory; use -f <Dockerfile> to select a Dockerfile"
        );
        let file = dockerfile
            .map(Path::to_owned)
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

    #[tokio::test]
    async fn completed_image_status_and_cleanup_use_build_identity() -> Result<()> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = Client::new(&format!("http://{}", listener.local_addr()?), "test-key")?;
        let server = tokio::spawn(async move {
            for (method, suffix, status, body) in [
                (
                    "GET",
                    "status",
                    "200 OK",
                    r#"{"templateID":"build-1","buildID":"build-1","status":"ready"}"#,
                ),
                ("DELETE", "builder", "204 No Content", ""),
            ] {
                let (stream, _) = listener.accept().await?;
                let mut stream = tokio::io::BufReader::new(stream);
                let mut headers = String::new();
                loop {
                    let mut line = String::new();
                    ensure!(stream.read_line(&mut line).await? != 0, "unexpected EOF");
                    if line == "\r\n" {
                        break;
                    }
                    headers.push_str(&line);
                }
                assert!(headers.starts_with(&format!(
                    "{method} /templates/build-1/builds/build-1/{suffix} HTTP/1.1\r\n"
                )));
                assert!(!headers
                    .to_ascii_lowercase()
                    .contains("x-agentenv-required-node"));
                stream
                    .get_mut()
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        });
        let session = BuildSession {
            template_id: Some("build-1".into()),
            build_id: "build-1".into(),
        };
        let info = wait_for_status(&client, &session, "ready").await?;
        assert_eq!(info.build_id, "build-1");
        delete_builder(&client, &session).await;
        server.await??;
        Ok(())
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
        assert_eq!(args.context, Some(PathBuf::from(".")));
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
        let prepared =
            BuildContext::prepare(args.context.as_deref().unwrap(), args.dockerfile.as_deref())?;
        assert_eq!(prepared.context, context.canonicalize()?);
        assert_eq!(prepared.dockerfile_dir, prepared.context);
        args.dockerfile = Some(custom.clone());
        let prepared =
            BuildContext::prepare(args.context.as_deref().unwrap(), args.dockerfile.as_deref())?;
        assert_eq!(prepared.context, context.canonicalize()?);
        assert_eq!(prepared.dockerfile_dir, dockerfiles.canonicalize()?);
        assert_eq!(prepared.filename, "Custom.Dockerfile");
        args.context = Some(custom);
        assert!(BuildContext::prepare(
            args.context.as_deref().unwrap(),
            args.dockerfile.as_deref()
        )
        .err()
        .unwrap()
        .to_string()
        .contains("use -f"));
        Ok(())
    }
    #[test]
    fn image_digest_must_be_a_sha256_reference() {
        assert!(validate_digest(&format!("sha256:{}", "ab".repeat(32))).is_ok());
        for value in [
            "ab".repeat(32),
            "sha256:".to_owned(),
            format!("sha256:{}", "ab".repeat(31)),
            format!("sha256:{}", "ag".repeat(32)),
            format!("sha256:{}", "AB".repeat(32)),
            format!("sha256:{}", "ab".repeat(33)),
        ] {
            assert!(validate_digest(&value).is_err(), "{value}");
        }
    }

    #[test]
    fn native_image_flags_preserve_template_builds() {
        crate::Cli::command().debug_assert();
        for (args, valid) in [
            ("aenv build --image .", true),
            (
                "aenv build --image . -f Dockerfile --build-arg A=B --secret id=token,src=token",
                true,
            ),
            ("aenv build --image", false),
            ("aenv build --image . --name template", false),
            ("aenv build --image . --start-cmd true", false),
            ("aenv build --image . --ready-cmd true", false),
            ("aenv build --image . --cpu 2", false),
            ("aenv build --image . --memory 512", false),
        ] {
            assert_eq!(
                crate::Cli::try_parse_from(args.split_whitespace()).is_ok(),
                valid,
                "{args}"
            );
        }
    }
}

/// Compose image builds share the ordinary BuildKit transport and cleanup.
pub(super) mod compose {
    use crate::client::Client;
    use crate::commands::build::{self, Build, BuildContext, ImageBuild, ImagePush};
    use crate::commands::compose::{parse_env, read_compose};
    use anyhow::{ensure, Context, Result};
    use clap::Args as ClapArgs;
    use reqwest::Method;
    use serde::Deserialize;
    use serde_json::{json, Value};
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::path::PathBuf;
    #[derive(Clone, Default, ClapArgs)]
    #[group(id = "compose-build")]
    pub(super) struct BuildArgs {
        /// Build service images from a Compose file instead of creating a VM template
        #[arg(long, value_name = "PATH")]
        pub compose: Option<PathBuf>,
        /// Apply Harbor task defaults: build main from ./Dockerfile and keep it alive unless overridden
        #[arg(long, requires = "compose", conflicts_with_all = ["context", "name"])]
        harbor: bool,
        /// Registry repository for unique service image tags, e.g. registry.example.com/team/images;
        /// optional, only needed to distribute images through a registry
        #[arg(long, requires = "compose", conflicts_with_all = ["context", "name"])]
        image_repository: Option<String>,
        /// Write an image-only Compose file (default: compose.built.yaml beside the input); must not exist
        #[arg(long, requires = "compose", conflicts_with_all = ["context", "name"], value_name = "PATH")]
        output: Option<PathBuf>,
        /// Compose interpolation variable; repeatable, last value wins; .env is not loaded
        #[arg(long = "env", requires = "compose", conflicts_with_all = ["context", "name"], value_name = "KEY=VALUE", value_parser = parse_env)]
        environment: Vec<(String, String)>,
        /// Enable an optional Compose profile (repeatable)
        #[arg(long = "profile", requires = "compose", conflicts_with_all = ["context", "name"], value_name = "NAME")]
        profiles: Vec<String>,
        /// Allow HTTP or untrusted TLS for image pushes (development registries only)
        #[arg(long, requires = "compose", conflicts_with_all = ["context", "name"])]
        registry_insecure: bool,
    }

    #[derive(Deserialize)]
    struct Plan {
        compose: Value,
        services: Vec<Service>,
    }

    #[derive(Deserialize)]
    struct Service {
        name: String,
        context: PathBuf,
        dockerfile: PathBuf,
        args: BTreeMap<String, String>,
        target: Option<String>,
        #[serde(default, rename = "noCache")]
        no_cache: bool,
    }

    fn validate_repository(repository: &str) -> Result<()> {
        let (host, path) = repository
            .split_once('/')
            .context("--image-repository must include a registry host and repository path")?;
        ensure!(
            !host.is_empty()
                && (host.contains('.') || host.contains(':') || host == "localhost")
                && host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-:".contains(&b))
                && !path.is_empty()
                && path.len() <= 200
                && path.split('/').all(|part| !part.is_empty()
                    && part.bytes().all(|b| b.is_ascii_lowercase()
                        || b.is_ascii_digit()
                        || b"._-".contains(&b))),
            "invalid --image-repository; use REGISTRY/REPOSITORY without a scheme, tag, or digest"
        );
        Ok(())
    }

    pub(super) fn run_build(client: Client, args: build::Args) -> Result<()> {
        let options = &args.compose;
        let repository = options.image_repository.as_deref();
        if let Some(repository) = repository {
            validate_repository(repository)?;
        }
        let source = options
            .compose
            .as_ref()
            .context("missing Compose file")?
            .canonicalize()
            .context("locate Compose file")?;
        let base = source
            .parent()
            .context("Compose file has no parent directory")?;
        let output = options
            .output
            .clone()
            .unwrap_or_else(|| base.join("compose.built.yaml"));
        // Refuse an existing destination before allocating remote resources. Persist
        // without clobbering also closes the race with another writer during builds.
        ensure!(
            !output.try_exists()? && output.symlink_metadata().is_err(),
            "output {} already exists; choose another --output",
            output.display()
        );
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        let mut destination =
            tempfile::NamedTempFile::new_in(parent).context("prepare Compose output directory")?;
        let compose = read_compose(std::fs::File::open(&source)?)?;
        let request = json!({
            "compose": compose, "harbor": options.harbor,
            "composeEnv": options.environment.iter().cloned().collect::<BTreeMap<_, _>>(),
            "profiles": options.profiles,
        });
        ensure!(
            serde_json::to_vec(&request)?.len() <= 2 * 1024 * 1024,
            "Compose planner request exceeds 2 MiB"
        );
        let response = crate::commands::tokio_rt()?
            .block_on(async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(60),
                    client.build_request(Method::POST, "/sandboxes-compose/plan", Some(request)),
                )
                .await
                .context("Compose planning timed out")?
            })
            .context("plan Compose build on the server")?;
        let mut plan: Plan =
            serde_json::from_slice(&response).context("decode Compose build plan")?;
        ensure!(
            !plan.services.is_empty(),
            "Compose file selects no services with build; use aenv compose up directly"
        );

        // Validate every path before the first build, resolving context relative to
        // the Compose file and Dockerfile relative to that context, per Compose.
        let mut builds = Vec::new();
        let id = uuid::Uuid::new_v4().simple().to_string();
        for (index, service) in plan.services.into_iter().enumerate() {
            let context = base.join(service.context);
            let build = Build {
                name: format!("compose-{id}-{index}"),
                context: BuildContext::prepare(&context, Some(&context.join(service.dockerfile)))
                    .with_context(|| format!("service {}", service.name))?,
                build_args: service
                    .args
                    .into_iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect(),
                no_cache: args.no_cache || service.no_cache,
                image: Some(ImageBuild {
                    target: service.target,
                    push: repository.map(|repository| ImagePush {
                        image: format!("{repository}:aenv-{id}-{index}"),
                        insecure: options.registry_insecure,
                    }),
                }),
            };
            builds.push((service.name, build));
        }
        // Each published image is independently recoverable from the repository.
        let images = crate::commands::tokio_rt()?.block_on(async {
            let mut images = Vec::new();
            for (name, build) in builds {
                let pushed = build.image.as_ref().and_then(|image| image.push.as_ref());
                match pushed {
                    Some(push) => eprintln!("Building Compose service {name} -> {}", push.image),
                    None => eprintln!("Building Compose service {name}"),
                }
                let info = build::run_async(&client, &args, &build)
                    .await
                    .with_context(|| format!("building Compose service {name}"))?;
                let reference = match pushed {
                    Some(push) => push.image.clone(),
                    None => info.published_digest()?.to_owned(),
                };
                images.push((name, reference));
            }
            Ok::<_, anyhow::Error>(images)
        })?;
        for (name, image) in images {
            plan.compose["services"][&name]["image"] = Value::String(image);
        }
        plan.compose
            .as_object_mut()
            .context("invalid Compose plan")?
            .remove("x-aenv-build");
        let mut encoded = serde_json::to_vec_pretty(&plan.compose)?;
        encoded.push(b'\n');
        // compose up accepts YAML and JSON, both bounded to 1 MiB.
        ensure!(
            encoded.len() <= 1024 * 1024,
            "generated Compose file exceeds 1 MiB"
        );
        destination.write_all(&encoded)?;
        destination.as_file().sync_all()?;
        destination
            .persist_noclobber(&output)
            .with_context(|| format!("publish Compose output {}", output.display()))?;
        eprintln!("Built Compose file: {}", output.display());
        eprintln!("Start it with: aenv compose up -f {}", output.display());
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use clap::{CommandFactory, Parser};
        #[test]
        fn compose_build_flags_do_not_change_template_builds() {
            crate::Cli::command().debug_assert();
            for (args, valid) in [
            ("aenv build --compose compose.yaml", true),
            ("aenv build --image --compose compose.yaml", false),
            ("aenv build --compose compose.yaml --image-repository example.com/team/images --env TAG=a=b --profile worker", true),
            ("aenv build . --name demo --image-repository example.com/team/images", false),
            ("aenv build . --name demo --harbor", false),
            ("aenv build . --name demo --output out.yaml", false),
        ] {
            assert_eq!(crate::Cli::try_parse_from(args.split_whitespace()).is_ok(), valid, "{args}");
        }
            for extra in [
                "--start-cmd",
                "--ready-cmd",
                "--name",
                "--file",
                "--build-arg",
                "--secret",
            ] {
                let args = format!("aenv build --compose compose.yaml {extra} value");
                assert!(
                    crate::Cli::try_parse_from(args.split_whitespace()).is_err(),
                    "{args}"
                );
            }
        }
        #[test]
        fn repository_cannot_inject_exporter_options_or_reuse_a_tag() {
            for value in [
                "example.com/team/images",
                "localhost:5000/images",
                "192.0.2.10:5000/build",
            ] {
                assert!(validate_repository(value).is_ok(), "{value}");
            }
            for value in [
                "images",
                "https://example.com/images",
                "example.com/images:latest",
                "example.com/images,push=false",
                "example.com//images",
                "example.com/images@sha256:abc",
                "example.com/Upper",
            ] {
                assert!(validate_repository(value).is_err(), "{value}");
            }
        }
    }
}
