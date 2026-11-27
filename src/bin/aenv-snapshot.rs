use std::io::{self, IsTerminal, Read};
use std::path::PathBuf;
use std::process::ExitCode;

use agentenv::cfg::ConfigManager;
use agentenv::migration::{import_from_config, parse_snapshot_ids, ImportOptions};
use agentenv::snapshot::tool::image_export::SnapshotImageService;
use agentenv::snapshot::{open_metadata, SnapshotId};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};

#[derive(Debug, Parser)]
#[command(about = "Inspect, export, import, and collect snapshots")]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(flatten)]
    Image(ImageCommand),
    /// Import legacy POSIX or OSS catalog resources into SQL metadata.
    ///
    /// Pass `--all` to discover and import snapshots, ready templates, internal
    /// builders (including legacy `template-build/builder` sub-repositories),
    /// volumes, published images, image tags, and `imageOnly` build jobs, or
    /// pass explicit snapshot IDs (positional arguments or one per line on stdin).
    Import {
        /// Discover and import all legacy snapshots, templates, builders,
        /// volumes, published images, image tags, and image build jobs.
        #[arg(long)]
        all: bool,
        /// Validate legacy records, blobs, and SQL state without writing anything.
        #[arg(long)]
        dry_run: bool,
        /// Skip Local snapshots instead of failing when importing with `--all`.
        #[arg(long)]
        skip_local: bool,
        /// Snapshot IDs to import (bare UUID or `aenv-snapshot-<uuid>`).
        snapshots: Vec<String>,
    },
    /// Reclaim retired snapshots and unreferenced managed layers.
    Gc {
        #[arg(long, default_value_t = 64)]
        limit: u32,
    },
}

#[derive(Debug, Subcommand)]
enum ImageCommand {
    /// Publish a snapshot disk as a standalone OCI image.
    ExportImage {
        snapshot: String,
        #[arg(long, default_value_t = 0)]
        image_idx: usize,
        #[arg(long)]
        target_repository: String,
        #[arg(long)]
        tag: Option<String>,
    },
    /// Print snapshot metadata and the captured layer inventory.
    Info { snapshot: String },
    /// Print a read-only OverlayBD image configuration.
    BuildImageConfig {
        snapshot: String,
        #[arg(long, default_value_t = 0)]
        image_idx: usize,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    match run(Cli::parse()).await {
        Ok((result, code)) => {
            println!("{result:#}");
            code
        }
        Err(error) => {
            println!("{}", json!({ "error": format!("{error:#}") }));
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<(Value, ExitCode)> {
    let config = match cli.config {
        Some(path) => ConfigManager::init_global_from_path(&path)?,
        None => ConfigManager::init_global()?,
    }
    .config();
    let meta = open_metadata(config).await?;
    let result = async {
        match cli.command {
            Command::Gc { limit } => Ok((
                serde_json::to_value(meta.collect_garbage(limit).await?)?,
                ExitCode::SUCCESS,
            )),
            Command::Import {
                all,
                dry_run,
                skip_local,
                snapshots,
            } => {
                let ids = if all {
                    anyhow::ensure!(
                        snapshots.is_empty(),
                        "--all cannot be combined with explicit snapshot IDs"
                    );
                    Vec::new()
                } else {
                    anyhow::ensure!(!skip_local, "--skip-local requires --all");
                    read_snapshot_ids(&snapshots)?
                };
                let report = import_from_config(
                    config,
                    &meta,
                    &ids,
                    ImportOptions {
                        all,
                        dry_run,
                        skip_local,
                    },
                )
                .await?;
                let code = if report.has_failures() {
                    ExitCode::FAILURE
                } else {
                    ExitCode::SUCCESS
                };
                Ok((serde_json::to_value(report)?, code))
            }
            Command::Image(command) => {
                let service = SnapshotImageService::from_config(config, meta.clone())?;
                Ok((
                    run_image_command(command, &service).await?,
                    ExitCode::SUCCESS,
                ))
            }
        }
    }
    .await;
    meta.close().await;
    result
}

async fn run_image_command(
    command: ImageCommand,
    service: &SnapshotImageService,
) -> anyhow::Result<Value> {
    match command {
        ImageCommand::ExportImage {
            snapshot,
            image_idx,
            target_repository,
            tag,
        } => {
            let result = service
                .export_image(
                    &snapshot,
                    image_idx,
                    Some(&target_repository),
                    tag.as_deref(),
                )
                .await?;
            Ok(serde_json::to_value(result)?)
        }
        ImageCommand::Info { snapshot } => {
            let mut record = service.record(&snapshot).await?;
            if let agentenv::snapshot::SnapshotSource::Sandbox {
                source_sandbox_metadata,
                ..
            } = &mut record.config.source
            {
                *source_sandbox_metadata = None;
            }
            let images = record.manifest.map(|manifest| {
                json!({
                    "rootfs": manifest.rootfs_layers,
                    "attached_drives": manifest.attached_drives,
                    "memory_layers": manifest.memory_layers,
                })
            });
            Ok(json!({
                "id": record.id,
                "snapshot_type": record.snapshot_type,
                "owner_node_id": record.owner_node_id,
                "source": record.config.source,
                "created_at_unix_ms": record.created_at_unix_ms,
                "resources": record.config.resources,
                "runtime_versions": record.config.runtime_versions,
                "custom_extension_params": record.config.custom_extension_params,
                "images": images,
            }))
        }
        ImageCommand::BuildImageConfig {
            snapshot,
            image_idx,
        } => {
            let record = service.record(&snapshot).await?;
            Ok(serde_json::to_value(
                service.build_image_config(&record, image_idx)?,
            )?)
        }
    }
}

/// Parses deduplicated snapshot IDs (bare UUID or `aenv-snapshot-<uuid>`) from
/// positional arguments, or from piped stdin when no arguments are given.
fn read_snapshot_ids(args: &[String]) -> anyhow::Result<Vec<SnapshotId>> {
    use anyhow::Context as _;
    if !args.is_empty() {
        return parse_snapshot_ids(args.iter().map(String::as_str));
    }
    anyhow::ensure!(
        !io::stdin().is_terminal(),
        "provide snapshot IDs as arguments or pipe them via stdin"
    );
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .context("read snapshot IDs from stdin")?;
    parse_snapshot_ids(
        input
            .lines()
            .map(|line| line.split('#').next().unwrap_or("").trim())
            .filter(|token| !token.is_empty()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_import() {
        let cli = Cli::try_parse_from(["aenv-snapshot", "import", "aenv-snapshot-1", "2"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Import {
                all: false,
                dry_run: false,
                skip_local: false,
                snapshots
            } if snapshots.len() == 2
        ));
        let stdin_mode = Cli::try_parse_from(["aenv-snapshot", "import"]).unwrap();
        assert!(matches!(
            stdin_mode.command,
            Command::Import {
                all: false,
                dry_run: false,
                skip_local: false,
                snapshots
            } if snapshots.is_empty()
        ));
        let all_mode = Cli::try_parse_from([
            "aenv-snapshot",
            "import",
            "--all",
            "--dry-run",
            "--skip-local",
        ])
        .unwrap();
        assert!(matches!(
            all_mode.command,
            Command::Import {
                all: true,
                dry_run: true,
                skip_local: true,
                snapshots
            } if snapshots.is_empty()
        ));
    }
}
