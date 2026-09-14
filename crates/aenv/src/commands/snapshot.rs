use crate::client::{snapshots::SnapshotInfo, Client};
use crate::output::{self, Format};
use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};
use tabled::Tabled;

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv snapshot create <sandbox-id> --name my-base
  aenv snapshot ls
  aenv snapshot ls --sandbox-id <sandbox-id>
  aenv start my-base

Snapshots are persistent and reusable. Use `aenv start <snapshot>` to create one or more new sandboxes from a snapshot.")]
pub struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    /// Create a persistent snapshot from a running sandbox
    Create {
        #[arg(add = crate::commands::completion::add_running_sandbox_candidates())]
        sandbox_id: String,
        /// Snapshot name or alias. If omitted, the server returns the generated snapshot ID.
        #[arg(long)]
        name: Option<String>,
    },
    /// List persistent snapshots
    #[command(visible_alias = "ls")]
    List {
        /// Filter snapshots by source sandbox ID
        #[arg(long = "sandbox-id")]
        sandbox_id: Option<String>,
        #[arg(long, value_enum)]
        output: Option<Format>,
    },
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    match args.cmd {
        Sub::Create { sandbox_id, name } => create(&client, &sandbox_id, name.as_deref()),
        Sub::List { sandbox_id, output } => {
            list(&client, sandbox_id.as_deref(), output::resolve(output))
        }
    }
}

#[derive(Tabled)]
struct Row {
    #[tabled(rename = "SNAPSHOT ID")]
    snapshot_id: String,
    #[tabled(rename = "NAMES")]
    names: String,
    #[tabled(rename = "IMAGE REF")]
    image_ref: String,
}

fn create(client: &Client, sandbox_id: &str, name: Option<&str>) -> Result<()> {
    let snapshot = client.create_snapshot(sandbox_id, name)?;
    println!("Created snapshot {}", snapshot.snapshot_id);
    if let Some(image_ref) = &snapshot.image_ref {
        println!("Image: {image_ref}");
    }
    Ok(())
}

fn list(client: &Client, sandbox_id: Option<&str>, format: Format) -> Result<()> {
    let snapshots = client.list_snapshots(sandbox_id)?;
    output::render(format, &snapshots, |snapshot: &SnapshotInfo| Row {
        snapshot_id: snapshot.snapshot_id.clone(),
        names: if snapshot.names.is_empty() {
            "-".to_string()
        } else {
            snapshot.names.join(",")
        },
        image_ref: snapshot
            .image_ref
            .clone()
            .unwrap_or_else(|| "-".to_string()),
    })
}
