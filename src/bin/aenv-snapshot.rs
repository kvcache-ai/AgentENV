use std::path::PathBuf;
use std::process::ExitCode;

use agentenv::cfg::ConfigManager;
use agentenv::snapshot::image_export::SnapshotImageService;
use agentenv::snapshot::{run_gc_once, MetadataHandle};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};

#[derive(Debug, Parser)]
#[command(about = "Inspect, export, and collect snapshots")]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
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
    /// Reclaim retired snapshots and unreferenced snapshot layers.
    Gc {
        #[arg(long, default_value_t = 64)]
        limit: u32,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    match run(Cli::parse()).await {
        Ok(result) => {
            println!("{result:#}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            println!("{}", json!({ "error": format!("{error:#}") }));
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<Value> {
    let config = match cli.config {
        Some(path) => ConfigManager::init_global_from_path(&path)?,
        None => ConfigManager::init_global()?,
    }
    .config();
    let metadata = MetadataHandle::open(&config.database).await?;
    let meta = metadata.client();
    let service =
        SnapshotImageService::from_global_config(meta.clone(), config.resolved_regctl_binary())?;
    let result = run_command(cli.command, &service, &meta).await;
    service.shutdown().await;
    metadata.close().await;
    result
}

async fn run_command(
    command: Command,
    service: &SnapshotImageService,
    meta: &agentenv_metadata::MetaClient,
) -> anyhow::Result<Value> {
    match command {
        Command::ExportImage {
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
        Command::Info { snapshot } => {
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
        Command::BuildImageConfig {
            snapshot,
            image_idx,
        } => {
            let record = service.record(&snapshot).await?;
            Ok(serde_json::to_value(
                service.build_image_config(&record, image_idx)?,
            )?)
        }
        Command::Gc { limit } => Ok(serde_json::to_value(
            run_gc_once(meta, service.repository(), limit).await?,
        )?),
    }
}
