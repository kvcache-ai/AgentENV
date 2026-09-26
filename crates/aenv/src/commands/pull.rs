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
    /// Timeout in seconds for waiting for the build to complete. No timeout by default.
    #[arg(long, value_name = "SECS", conflicts_with = "detach")]
    timeout: Option<u64>,
}

pub fn run(args: Args) -> Result<()> {
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

    #[test]
    fn default_name_uses_leaf_segment_without_tag_or_digest() {
        assert_eq!(default_name("docker.io/library/ubuntu"), "ubuntu");
        assert_eq!(default_name("docker.io/library/ubuntu:24.04"), "ubuntu");
        assert_eq!(default_name("ubuntu@sha256:abc123"), "ubuntu");
    }
}
