mod legacy;
mod store;
mod target;

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use agentenv_domain::SnapshotId;
use agentenv_metadata::{DatabaseConfig, MetaServer};
use anyhow::{ensure, Result};
use clap::{Args, Parser};

use store::{Source, Store};

#[derive(Parser)]
#[command(about = "Import selected legacy OSS snapshots into an AgentENV database")]
struct Cli {
    #[command(flatten)]
    source: Source,
    #[command(flatten)]
    database: Database,
    /// Snapshot UUID to import. Repeat for a batch.
    #[arg(long, required = true, value_parser = SnapshotId::parse)]
    snapshot_id: Vec<SnapshotId>,
}

#[derive(Args)]
#[group(required = true, multiple = false)]
struct Database {
    #[arg(long, env = "AENV_DATABASE_POSTGRES_URL", hide_env_values = true)]
    postgres_url: Option<String>,
    #[arg(long)]
    sqlite_path: Option<PathBuf>,
}

impl From<Database> for DatabaseConfig {
    fn from(database: Database) -> Self {
        match (database.postgres_url, database.sqlite_path) {
            (Some(url), None) => Self::Postgres { url },
            (None, Some(path)) => Self::Sqlite { path },
            _ => unreachable!("clap requires exactly one database"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let metadata = MetaServer::open(&cli.database.into()).await?;
    let mut store = Store::new(cli.source)?;
    let mut layers = HashMap::new();
    let mut imported = 0;
    let mut reused = 0;
    for id in cli.snapshot_id.into_iter().collect::<BTreeSet<_>>() {
        // A UUID-shaped alias is not the selected snapshot identity.
        let existing = metadata
            .get_snapshot(&id.to_string())
            .await?
            .filter(|entry| entry.id == id);
        if let Some(entry) = &existing {
            ensure!(entry.is_visible(), "snapshot {id} is retired");
        }
        let record = legacy::read_snapshot(&mut store, &id, &mut layers).await?;
        if target::import_snapshot(&metadata, &mut store, record, existing).await? {
            reused += 1;
        } else {
            imported += 1;
        }
    }
    println!("Imported {imported}, reused {reused}");
    Ok(())
}
