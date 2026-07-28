//! Standalone tool publishing a committed AgentENV snapshot's rootfs as a
//! plain OCI image. stdout carries exactly one line — the image reference —
//! so the tool composes in scripts; logs go to stderr and errors exit
//! non-zero with a credential-free context chain. It never starts
//! node-runtime machinery (KVM, ublk, network); registry authentication
//! stays with the Docker config that `regctl` reads itself.

use std::path::PathBuf;

use agentenv::cfg::ConfigManager;
use agentenv::snapshot::image_export::SnapshotImageService;
use anyhow::Context as _;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "aenv-snapshot-image",
    about = "Publish a committed AgentENV snapshot rootfs as a standalone OCI image \
             and print the image reference on stdout"
)]
struct Cli {
    /// Snapshot ID or alias to export.
    snapshot: String,

    /// Target OCI repository including the registry host, for example
    /// "registry.example.com/team/app". When omitted, use the snapshot's
    /// recorded rootfs publication repository or unique external source.
    #[arg(long)]
    target_repository: Option<String>,

    /// OCI image tag. Defaults to latest for an explicit target, or
    /// snapshot-<snapshot-id> when the target repository is inferred.
    #[arg(long)]
    tag: Option<String>,

    /// Path to config file (same as AENV_CONFIG_PATH).
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let cli = Cli::parse();
    let config_manager = match cli.config.as_deref() {
        Some(path) => ConfigManager::init_global_from_path(path)
            .with_context(|| format!("load AgentENV config from '{}'", path.display()))?,
        None => ConfigManager::init_global()
            .context("load AgentENV config (AENV_CONFIG_PATH or the default config path)")?,
    };
    let service =
        SnapshotImageService::from_global_config(config_manager.config().resolved_regctl_binary())
            .context("initialize the snapshot repository export backend")?;

    let lookup = cli.snapshot.clone();
    let result = service
        .export_rootfs_image(
            &cli.snapshot,
            cli.target_repository.as_deref(),
            cli.tag.as_deref(),
        )
        .await
        .with_context(|| format!("export snapshot '{lookup}' rootfs as an OCI image"))?;
    println!("{}", result.image_ref);
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use super::Cli;

    #[test]
    fn cli_parses_options_and_requires_snapshot() {
        assert_eq!(Cli::command().get_name(), "aenv-snapshot-image");
        let cli = Cli::try_parse_from([
            "aenv-snapshot-image",
            "snap-1",
            "--target-repository",
            "registry.example.com/team/app",
            "--tag",
            "release-1",
            "--config",
            "/etc/aenv/config.toml",
        ])
        .unwrap();
        assert_eq!(cli.snapshot, "snap-1");
        assert_eq!(
            cli.target_repository.as_deref(),
            Some("registry.example.com/team/app")
        );
        assert_eq!(cli.tag.as_deref(), Some("release-1"));
        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("/etc/aenv/config.toml"))
        );

        let minimal = Cli::try_parse_from(["aenv-snapshot-image", "snap-1"]).unwrap();
        assert!(
            minimal.target_repository.is_none()
                && minimal.tag.is_none()
                && minimal.config.is_none()
        );
        assert!(Cli::try_parse_from(["aenv-snapshot-image"]).is_err());
    }
}
