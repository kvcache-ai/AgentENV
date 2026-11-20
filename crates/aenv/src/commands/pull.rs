use crate::client::{
    templates::{CreateTemplateV3, StartTemplateBuildV2},
    Client,
};
use crate::progress::{format_elapsed, BuildProgress};
use anyhow::Result;
use clap::Args as ClapArgs;
use std::time::{Duration, Instant};

#[derive(ClapArgs)]
pub struct Args {
    /// Base image reference. Shortnames like `ubuntu:22.04` and `node:20` are supported; full OCI references such as `docker.io/library/ubuntu:24.04` also work.
    image: String,
    /// Override the template name. Defaults to the repo segment of the image.
    #[arg(long)]
    name: Option<String>,
    #[command(flatten)]
    resources: super::CpuMemoryArgs,
    /// Command to run before capturing the template snapshot
    #[arg(long = "start-cmd")]
    start_cmd: Option<String>,
    /// Command used to wait until the started template is ready
    #[arg(long = "ready-cmd")]
    ready_cmd: Option<String>,
    /// Set readyCmd to wait until localhost can accept TCP connections on this port
    #[arg(long, value_name = "PORT", conflicts_with = "ready_cmd")]
    probe: Option<u16>,
    /// Detach: submit the build and return immediately without waiting (previous default behavior)
    #[arg(short = 'd', long)]
    detach: bool,
    /// Timeout in seconds for waiting for the build to complete. No timeout by default; 3600 with --image.
    #[arg(long, value_name = "SECS", conflicts_with = "detach")]
    timeout: Option<u64>,
    /// Publish the image to the shared repository as aenv/<host>/<path>:<tag> instead of building a template, and print that reference
    #[arg(
        long = "image",
        conflicts_with_all = ["name", "cpu_count", "memory_mb", "start_cmd", "ready_cmd", "probe", "detach"]
    )]
    image_only: bool,
}

const IMAGE_PULL_TIMEOUT_SECS: u64 = 3600;

pub fn run(args: Args) -> Result<()> {
    if args.image_only {
        return pull_image(&args.image, args.timeout);
    }
    let client = Client::from_env()?;
    let started = Instant::now();
    let progress = if args.detach {
        None
    } else {
        let progress = BuildProgress::with_steps(true, 6)?;
        progress.stage(0, "Creating template");
        Some(progress)
    };
    let image = args.image;
    let name = args.name.unwrap_or_else(|| default_name(&image));
    let ready_cmd = args.ready_cmd.or_else(|| args.probe.map(probe_ready_cmd));

    let req = CreateTemplateV3 {
        name: name.clone(),
        tags: Vec::new(),
        cpu_count: args.resources.cpu_count,
        memory_mb: args.resources.memory_mb,
        image_only: false,
    };
    let resp = client.create_template_v3(&req)?;
    client.start_template_build_v2(
        &resp.template_id,
        &resp.build_id,
        &StartTemplateBuildV2 {
            from_image: Some(image),
            steps: Vec::new(),
            start_cmd: args.start_cmd,
            ready_cmd,
        },
    )?;
    let created = format!(
        "Created template {} (build {})",
        resp.template_id, resp.build_id
    );
    if let Some(progress) = &progress {
        progress.println(&created);
        progress.println(&format!(
            "✓ Creating template [{}]",
            format_elapsed(started.elapsed())
        ));
        progress.stage(1, "Resolving base image");
    } else {
        println!("{created}");
    }
    if args.detach {
        println!("Build started.");
        println!("Watch with: aenv template watch {}", name);
        return Ok(());
    }
    let deadline = args
        .timeout
        .and_then(|secs| Instant::now().checked_add(Duration::from_secs(secs)));
    crate::commands::template::wait_for_build(
        &client,
        &resp.template_id,
        &resp.build_id,
        &name,
        deadline,
        progress.as_ref().expect("attached pull has progress"),
        Some(started),
    )
}

fn pull_image(image: &str, timeout: Option<u64>) -> Result<()> {
    let client = Client::from_env()?;
    let started = Instant::now();
    let progress = BuildProgress::with_steps(true, 1)?;
    progress.stage(0, "Pulling and publishing image");
    let pulled = client.pull_image(
        image,
        Duration::from_secs(timeout.unwrap_or(IMAGE_PULL_TIMEOUT_SECS)),
    );
    progress.finish();
    let pulled = pulled?;
    eprintln!(
        "✓ Published {} ({}) [{}]",
        pulled.image,
        pulled.image_digest,
        format_elapsed(started.elapsed())
    );
    println!("{}", pulled.image);
    Ok(())
}

fn default_name(image: &str) -> String {
    let leaf = image.rsplit_once('/').map(|(_, n)| n).unwrap_or(image);
    leaf.find('@')
        .or_else(|| leaf.find(':'))
        .map(|idx| &leaf[..idx])
        .unwrap_or(leaf)
        .to_string()
}

fn probe_ready_cmd(port: u16) -> String {
    format!(": >/dev/tcp/127.0.0.1/{port}")
}

#[cfg(test)]
mod tests {
    use super::default_name;
    use clap::{CommandFactory, Parser};

    #[test]
    fn image_flag_excludes_template_options() {
        crate::Cli::command().debug_assert();
        for (args, valid) in [
            ("aenv pull --image python:3.12", true),
            ("aenv pull --image python:3.12 --timeout 60", true),
            ("aenv pull --image python:3.12 --name python", false),
            ("aenv pull --image python:3.12 --cpu 2", false),
            ("aenv pull --image python:3.12 --memory 512", false),
            ("aenv pull --image python:3.12 --start-cmd true", false),
            ("aenv pull --image python:3.12 --ready-cmd true", false),
            ("aenv pull --image python:3.12 --probe 80", false),
            ("aenv pull --image python:3.12 -d", false),
            ("aenv pull --image", false),
        ] {
            assert_eq!(
                crate::Cli::try_parse_from(args.split_whitespace()).is_ok(),
                valid,
                "{args}"
            );
        }
    }

    #[test]
    fn default_name_uses_leaf_segment_without_tag_or_digest() {
        assert_eq!(default_name("docker.io/library/ubuntu"), "ubuntu");
        assert_eq!(default_name("docker.io/library/ubuntu:24.04"), "ubuntu");
        assert_eq!(default_name("ubuntu@sha256:abc123"), "ubuntu");
    }
}
