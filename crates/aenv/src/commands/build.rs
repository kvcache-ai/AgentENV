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
  aenv build --compose compose.yaml
")]
pub struct Args {
    /// Local build context directory
    #[arg(required_unless_present = "compose")]
    context: Option<PathBuf>,
    /// Dockerfile path (defaults to CONTEXT/Dockerfile)
    #[arg(short = 'f', long = "file")]
    dockerfile: Option<PathBuf>,
    /// Template name, with --image the image name as NAME[:TAG], or with --compose
    /// the project name that prefixes service image names
    #[arg(long, required_unless_present = "compose")]
    name: Option<String>,
    /// Build an immutable image, referenced as aenv/NAME:TAG, and print its digest
    #[arg(long, conflicts_with_all = ["start_cmd", "ready_cmd", "cpu_count", "memory_mb"])]
    image: bool,
    /// Point another tag of the image name at the built image; repeatable
    #[arg(long = "tag", requires = "image", value_name = "TAG")]
    tags: Vec<String>,
    #[command(flatten)]
    compose: compose::BuildArgs,
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
        context: Some(context),
        dockerfile: None,
        name: Some(name),
        image: false,
        tags: vec![],
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
    let context = match &args.context {
        Some(context) => Some(BuildContext::prepare(context, args.dockerfile.as_deref())?),
        None => None,
    };
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
    let Some(context) = context else {
        return compose::run_build(client, args);
    };
    super::tokio_rt()?.block_on(async {
        let (session, built) =
            run_async(&client, &args, &context, &ImageOptions::default()).await?;
        if !args.image {
            println!("Template {} is ready.", session.template_id);
            return Ok(());
        }
        println!("{}", image_digest(&built)?);
        for name in &session.names {
            for tag in &session.tags {
                eprintln!("Tagged aenv/{name}:{tag}");
            }
        }
        Ok::<_, anyhow::Error>(())
    })
}

/// Output options that only Compose service builds set.
#[derive(Default)]
struct ImageOptions {
    target: Option<String>,
    push: Option<RegistryPush>,
}

/// Also export the image to a registry, in addition to publishing it.
struct RegistryPush {
    image: String,
    insecure: bool,
}

fn image_digest(built: &TemplateBuildInfo) -> Result<&str> {
    built
        .image_digest
        .as_deref()
        .context("server does not report the built image digest; upgrade the AgentENV server")
}

async fn run_async(
    client: &Client,
    args: &Args,
    context: &BuildContext,
    options: &ImageOptions,
) -> Result<(TemplateV3Response, TemplateBuildInfo)> {
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
                        name: args.name.clone().context("missing --name")?,
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
            client,
            allocated,
            args,
            context,
            options,
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
    Ok((session.context("missing build session")?, built))
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
    options: &ImageOptions,
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
        if let Some(target) = &options.target {
            command.arg("--opt").arg(format!("target={target}"));
        }
        if let Some(push) = &options.push {
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
    fn prepare(context: &Path, dockerfile: Option<&Path>) -> Result<Self> {
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
        let prepared = BuildContext::prepare(&context, None)?;
        assert_eq!(prepared.context, context.canonicalize()?);
        assert_eq!(prepared.dockerfile_dir, prepared.context);
        let prepared = BuildContext::prepare(&context, Some(&custom))?;
        assert_eq!(prepared.context, context.canonicalize()?);
        assert_eq!(prepared.dockerfile_dir, dockerfiles.canonicalize()?);
        assert_eq!(prepared.filename, "Custom.Dockerfile");
        assert!(BuildContext::prepare(&custom, Some(&custom))
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
        assert_eq!(args.name.as_deref(), Some("web:v1"));
        assert_eq!(args.tags, ["latest"]);
    }
}

/// Compose image builds share the ordinary BuildKit transport and cleanup.
pub(super) mod compose {
    use std::collections::{BTreeMap, HashSet};
    use std::io::Write;
    use std::path::PathBuf;

    use anyhow::{ensure, Context, Result};
    use clap::Args as ClapArgs;
    use reqwest::Method;
    use serde::Deserialize;
    use serde_json::{json, Value};

    use super::{image_digest, run_async, Args, BuildContext, ImageOptions, RegistryPush};
    use crate::client::Client;
    use crate::commands::compose::{parse_env, read_compose};

    #[derive(Clone, Default, ClapArgs)]
    #[group(id = "compose-build")]
    pub(super) struct BuildArgs {
        /// Build the services that declare build: in a Compose file and write an image-only
        /// Compose file; images are named aenv/PROJECT-SERVICE
        #[arg(
            long,
            value_name = "PATH",
            conflicts_with_all = [
                "context", "dockerfile", "image", "tags", "start_cmd", "ready_cmd", "build_args",
                "secret", "cpu_count", "memory_mb",
            ]
        )]
        pub(super) compose: Option<PathBuf>,
        /// Apply Harbor task defaults: build main from ./Dockerfile and keep it alive unless overridden
        #[arg(long, requires = "compose", conflicts_with = "context")]
        harbor: bool,
        /// Registry repository for unique service image tags, e.g. registry.example.com/team/images;
        /// optional, only needed to distribute images through a registry
        #[arg(long, requires = "compose", conflicts_with = "context")]
        image_repository: Option<String>,
        /// Write an image-only Compose file (default: compose.built.yaml beside the input); must not exist
        #[arg(
            long,
            requires = "compose",
            conflicts_with = "context",
            value_name = "PATH"
        )]
        output: Option<PathBuf>,
        /// Compose interpolation variable; repeatable, last value wins; .env is not loaded
        #[arg(long = "env", requires = "compose", conflicts_with = "context", value_name = "KEY=VALUE", value_parser = parse_env)]
        environment: Vec<(String, String)>,
        /// Enable an optional Compose profile (repeatable)
        #[arg(
            long = "profile",
            requires = "compose",
            conflicts_with = "context",
            value_name = "NAME"
        )]
        profiles: Vec<String>,
        /// Allow HTTP or untrusted TLS for image pushes (development registries only)
        #[arg(long, requires = "compose", conflicts_with = "context")]
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

    /// Lowercase letters and digits joined by single `.`, `_`, or `-`, as image names require.
    fn normalize_name(value: &str) -> String {
        let mut name = String::new();
        for c in value.chars().map(|c| c.to_ascii_lowercase()) {
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                name.push(c);
            } else if !name.is_empty() && !name.ends_with(['.', '_', '-']) {
                name.push(if matches!(c, '.' | '_') { c } else { '-' });
            }
        }
        name.trim_end_matches(['.', '_', '-']).to_owned()
    }

    /// Like Docker Compose, name each service's image PROJECT-SERVICE.
    fn image_names(project: &str, services: &[String]) -> Result<Vec<String>> {
        let prefix = normalize_name(project);
        ensure!(
            !prefix.is_empty(),
            "cannot derive an image name from project {project:?}; set --name"
        );
        let mut seen = HashSet::new();
        services
            .iter()
            .map(|service| {
                let name = normalize_name(&format!("{prefix}-{service}"));
                ensure!(
                    name.len() <= 128,
                    "image name {name} exceeds 128 characters; set a shorter --name"
                );
                ensure!(
                    seen.insert(name.clone()),
                    "several services map to image name {name}; rename one of them"
                );
                Ok(name)
            })
            .collect()
    }

    pub(super) fn run_build(client: Client, args: Args) -> Result<()> {
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
        let project = match &args.name {
            Some(name) => {
                ensure!(
                    !name.contains(':'),
                    "with --compose, --name is a project name without a tag"
                );
                name.clone()
            }
            None => base
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
        };
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
        let runtime = crate::commands::tokio_rt()?;
        let response = runtime
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
        let names = image_names(
            &project,
            &plan
                .services
                .iter()
                .map(|service| service.name.clone())
                .collect::<Vec<_>>(),
        )?;

        // Validate every path before the first build, resolving context relative to
        // the Compose file and Dockerfile relative to that context, per Compose.
        let mut builds = Vec::new();
        let id = uuid::Uuid::new_v4().simple().to_string();
        for ((index, service), name) in plan.services.into_iter().enumerate().zip(names) {
            let context = base.join(&service.context);
            let prepared =
                BuildContext::prepare(&context, Some(&context.join(&service.dockerfile)))
                    .with_context(|| format!("service {}", service.name))?;
            let mut build = args.clone();
            build.name = Some(name);
            build.image = true;
            build.build_args = service
                .args
                .into_iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            build.no_cache |= service.no_cache;
            let image = ImageOptions {
                target: service.target,
                push: repository.map(|repository| RegistryPush {
                    image: format!("{repository}:aenv-{id}-{index}"),
                    insecure: options.registry_insecure,
                }),
            };
            builds.push((service.name, build, prepared, image));
        }
        // Each published image is independently recoverable from the repository.
        for (service, build, context, image) in builds {
            let name = build.name.as_deref().unwrap_or_default();
            match &image.push {
                Some(push) => {
                    eprintln!(
                        "Building Compose service {service} as aenv/{name} -> {}",
                        push.image
                    )
                }
                None => eprintln!("Building Compose service {service} as aenv/{name}"),
            }
            let (_, built) = runtime
                .block_on(run_async(&client, &build, &context, &image))
                .with_context(|| format!("building Compose service {service}"))?;
            let reference = match image.push {
                Some(push) => push.image,
                None => image_digest(&built)?.to_owned(),
            };
            plan.compose["services"][&service]["image"] = Value::String(reference);
        }
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
                ("aenv build --compose compose.yaml --name project", true),
                ("aenv build --compose compose.yaml --no-cache --timeout 600", true),
                ("aenv build --image --compose compose.yaml", false),
                ("aenv build --compose compose.yaml --image-repository example.com/team/images --env TAG=a=b --profile worker", true),
                ("aenv build . --name demo --image-repository example.com/team/images", false),
                ("aenv build . --name demo --harbor", false),
                ("aenv build . --name demo --output out.yaml", false),
                ("aenv build . --compose compose.yaml", false),
            ] {
                assert_eq!(crate::Cli::try_parse_from(args.split_whitespace()).is_ok(), valid, "{args}");
            }
            for extra in [
                "--start-cmd",
                "--ready-cmd",
                "--file",
                "--build-arg",
                "--secret",
                "--tag",
                "--cpu",
                "--memory",
            ] {
                let args = format!("aenv build --compose compose.yaml {extra} value");
                assert!(
                    crate::Cli::try_parse_from(args.split_whitespace()).is_err(),
                    "{args}"
                );
            }
        }

        #[test]
        fn service_images_are_named_like_docker_compose() {
            assert_eq!(
                image_names("My_App", &["web".into(), "Worker.1".into()]).unwrap(),
                ["my_app-web", "my_app-worker.1"]
            );
            assert_eq!(
                image_names("--odd  name--", &["db".into()]).unwrap(),
                ["odd-name-db"]
            );
            assert!(image_names("___", &["web".into()]).is_err());
            assert!(image_names("app", &["Web".into(), "web".into()]).is_err());
            assert!(image_names(&"a".repeat(126), &["web".into()]).is_err());
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
