mod legacy;
mod shadow;
mod store;
mod target;

use std::path::PathBuf;

use agentenv_metadata::{DatabaseConfig, MetaServer};
use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};

use legacy::{sha256, Plan};
use shadow::Shadow;
use store::{Source, Store};

#[derive(Parser)]
#[command(about = "Hand off a legacy OSS snapshot directory to an isolated AgentENV database")]
struct Cli {
    #[command(flatten)]
    source: Source,
    /// Local cache for audited catalog objects and artifacts.
    #[arg(long)]
    plan: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Audit the legacy directory without writing to OSS or SQL.
    Prepare,
    /// Preload a disposable shadow database while legacy traffic continues.
    Stage(Database),
    /// Reconcile the shadow database after all legacy writers have drained.
    Apply(FrozenTarget),
    /// Verify and seal the shadow database before it serves traffic.
    Verify(FrozenTarget),
}

#[derive(Args)]
struct FrozenTarget {
    /// Confirm all legacy catalog writers have drained and remain fenced.
    #[arg(long, required = true)]
    source_frozen: bool,
    #[command(flatten)]
    database: Database,
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
    let mut store = Store::new(cli.source)?;
    let verify = matches!(cli.command, Command::Verify(_));
    let stage = matches!(cli.command, Command::Stage(_));
    let frozen = matches!(cli.command, Command::Apply(_) | Command::Verify(_));
    let database = match cli.command {
        Command::Prepare => None,
        Command::Stage(database) => Some(database),
        Command::Apply(target) | Command::Verify(target) => Some(target.database),
    };

    let mut destination = match database {
        Some(database) => {
            let config = database.into();
            let metadata = MetaServer::open(&config).await?;
            let source = sha256(&serde_json::to_vec(&store.source)?);
            let shadow = Shadow::open(&config, &source, stage).await?;
            if !verify {
                shadow.ensure_writable()?;
            }
            Some((metadata, shadow))
        }
        None => None,
    };
    let previous = Plan::read(&cli.plan, &store.source)?;
    let (mut plan, catalog, artifacts) =
        Plan::refresh(&mut store, previous.as_ref(), frozen).await?;
    if let Some((metadata, shadow)) = &mut destination {
        if verify {
            previous
                .as_ref()
                .context("run apply before verify")?
                .require_same_catalog(&plan)?;
            target::verify(
                shadow, metadata, &mut store, &catalog, &mut plan, &artifacts,
            )
            .await?;
            println!("Verified and sealed the frozen catalog");
        } else {
            target::sync(
                shadow, metadata, &mut store, &catalog, &mut plan, &artifacts,
            )
            .await?;
        }
    }
    if !verify {
        plan.save(&cli.plan)?;
    }
    println!(
        "{} Distributed snapshots, {} aliases, {} Local snapshots awaiting owner action, {} deferred inventory items",
        catalog.records.len(),
        catalog.records.values().filter(|record| record.alias.is_some()).count(),
        catalog.local_ids.len(),
        catalog.deferred
    );
    Ok(())
}
