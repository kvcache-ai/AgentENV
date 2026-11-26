use std::path::PathBuf;
use std::process::ExitCode;

use agentenv::cfg::ConfigManager;
use agentenv::metadata::MetadataHandle;
use agentenv::repository::open_from_config as open_repository_from_config;
use agentenv::snapshot::tool::image_export::SnapshotImageService;
use agentenv::snapshot::tool::import::{import_snapshots, read_snapshot_ids};
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
    /// Import selected legacy Distributed snapshots into the SQL directory
    /// and write their `snapshot.json` content files. Reads one ID per line
    /// from stdin when no positional IDs are passed.
    Import {
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
    let metadata = MetadataHandle::open(config).await?;
    let meta = metadata.client();
    let result = async {
        match cli.command {
            Command::Gc { limit } => Ok((
                serde_json::to_value(meta.collect_garbage(limit).await?)?,
                ExitCode::SUCCESS,
            )),
            Command::Import { snapshots } => {
                let ids = read_snapshot_ids(&snapshots)?;
                let repository = open_repository_from_config(config)?;
                let report = import_snapshots(repository.as_ref(), &meta, &ids).await;
                let code = if report.failed.is_empty() {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                };
                Ok((serde_json::to_value(report)?, code))
            }
            Command::Image(command) => {
                let service = SnapshotImageService::from_global_config(
                    meta,
                    config.resolved_regctl_binary(),
                )?;
                Ok((
                    run_image_command(command, &service).await?,
                    ExitCode::SUCCESS,
                ))
            }
        }
    }
    .await;
    metadata.close().await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_import() {
        let cli = Cli::try_parse_from(["aenv-snapshot", "import", "aenv-snapshot-1", "2"]).unwrap();
        assert!(matches!(cli.command, Command::Import { snapshots } if snapshots.len() == 2));
        let stdin_mode = Cli::try_parse_from(["aenv-snapshot", "import"]).unwrap();
        assert!(
            matches!(stdin_mode.command, Command::Import { snapshots } if snapshots.is_empty())
        );
    }
}
