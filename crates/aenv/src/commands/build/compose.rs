use std::{
    collections::BTreeMap,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
};

use anyhow::{ensure, Context, Result};
use clap::Args as ClapArgs;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{BuildContext, ImageExport};
use crate::{
    client::Client,
    commands::compose::{parse_env, read_compose},
};

#[derive(Clone, Default, ClapArgs)]
#[group(id = "compose-build")]
pub(super) struct Args {
    /// Build service images from a Compose file instead of creating a VM template
    #[arg(long, value_name = "PATH", requires = "image_repository")]
    pub compose: Option<PathBuf>,
    /// Apply Harbor task defaults: build main from ./Dockerfile and keep it alive unless overridden
    #[arg(long, requires = "compose", conflicts_with_all = ["context", "name"])]
    harbor: bool,
    /// Registry repository for unique service image tags, e.g. registry.example.com/team/images
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
    /// Path to the Compose planner executable bundled with aenv
    #[arg(long, requires = "compose", conflicts_with_all = ["context", "name"])]
    compose_planner: Option<PathBuf>,
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
                && part
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))),
        "invalid --image-repository; use REGISTRY/REPOSITORY without a scheme, tag, or digest"
    );
    Ok(())
}

pub(super) fn run(client: Client, args: super::Args) -> Result<()> {
    let options = &args.compose;
    let repository = options.image_repository.as_deref().context(
        "--compose requires --image-repository REGISTRY/REPOSITORY accessible to both the builder and runtime nodes",
    )?;
    validate_repository(repository)?;
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
    let planner = options
        .compose_planner
        .clone()
        .unwrap_or(std::env::current_exe()?.with_file_name("aenv-compose-plan"));
    let input = serde_json::to_vec(&json!({
        "mode": "build", "compose": compose, "harbor": options.harbor,
        "composeEnv": options.environment.iter().cloned().collect::<BTreeMap<_, _>>(),
        "profiles": options.profiles,
    }))?;
    ensure!(
        input.len() <= 2 * 1024 * 1024,
        "Compose planner request exceeds 2 MiB"
    );
    let mut child = Command::new(&planner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| {
            format!(
                "run {}; rerun the aenv installer or set --compose-planner",
                planner.display()
            )
        })?;
    child
        .stdin
        .take()
        .context("missing planner stdin")?
        .write_all(&input)?;
    let response = child
        .wait_with_output()
        .context("wait for Compose planner")?;
    ensure!(
        response.status.success(),
        "Compose validation failed ({})",
        response.status
    );
    let mut plan: Plan =
        serde_json::from_slice(&response.stdout).context("decode Compose build plan")?;
    ensure!(
        !plan.services.is_empty(),
        "Compose file selects no services with build; use aenv compose up directly"
    );

    // Validate every path before the first build, resolving context relative to
    // the Compose file and Dockerfile relative to that context, per Compose.
    let mut builds = Vec::new();
    let id = uuid::Uuid::new_v4().simple().to_string();
    for (index, service) in plan.services.into_iter().enumerate() {
        let mut build = args.clone();
        let context = base.join(service.context);
        build.dockerfile = Some(context.join(service.dockerfile));
        build.context = Some(context);
        build.no_cache |= service.no_cache;
        build.name = Some(format!("compose-{id}-{index}"));
        build.build_args = service
            .args
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        let context =
            BuildContext::prepare(&build).with_context(|| format!("service {}", service.name))?;
        let export = ImageExport {
            image: format!("{repository}:aenv-{id}-{index}"),
            target: service.target,
            insecure: options.registry_insecure,
        };
        plan.compose["services"][&service.name]["image"] = Value::String(export.image.clone());
        builds.push((service.name, build, context, export));
    }
    let mut encoded = serde_json::to_vec_pretty(&plan.compose)?;
    encoded.push(b'\n');
    // compose up accepts YAML and JSON, both bounded to 1 MiB.
    ensure!(
        encoded.len() <= 1024 * 1024,
        "generated Compose file exceeds 1 MiB"
    );
    crate::commands::tokio_rt()?.block_on(async {
        for (name, build, context, export) in builds {
            eprintln!("Building Compose service {name} -> {}", export.image);
            super::run_async(&client, &build, context, Some(&export))
                .await
                .with_context(|| format!("building Compose service {name}"))?;
        }
        Ok::<_, anyhow::Error>(())
    })?;
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
        assert!(crate::Cli::try_parse_from([
            "aenv",
            "build",
            "--compose",
            "compose.yaml",
            "--image-repository",
            "example.com/team/images",
            "--env",
            "TAG=a=b",
            "--profile",
            "worker"
        ])
        .is_ok());
        for extra in [
            "--start-cmd",
            "--ready-cmd",
            "--name",
            "--file",
            "--build-arg",
            "--secret",
        ] {
            assert!(crate::Cli::try_parse_from([
                "aenv",
                "build",
                "--compose",
                "compose.yaml",
                extra,
                "value"
            ])
            .is_err());
        }
        assert!(crate::Cli::try_parse_from([
            "aenv", "build", ".", "--name", "test", "--output", "out.yaml"
        ])
        .is_err());
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
