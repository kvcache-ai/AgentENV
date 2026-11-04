use agentenv_metadata::DatabaseConfig;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "aenv-metadata",
    about = "AgentENV metadata database administration"
)]
struct Options {
    #[arg(
        long,
        env = "AENV_DATABASE_BACKEND",
        value_enum,
        default_value = "sqlite",
        global = true
    )]
    backend: Backend,
    #[arg(
        long,
        env = "AENV_DATABASE_SQLITE_PATH",
        default_value = "metadata.sqlite3",
        global = true
    )]
    sqlite_path: PathBuf,
    #[arg(
        long,
        env = "AENV_DATABASE_POSTGRES_URL",
        hide_env_values = true,
        global = true
    )]
    postgres_url: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum Backend {
    Sqlite,
    Postgres,
}

#[derive(Subcommand)]
enum Command {
    /// Inspect or migrate the metadata database without starting other services.
    Db {
        #[command(subcommand)]
        command: DatabaseCommand,
    },
}

#[derive(Clone, Copy, Subcommand)]
enum DatabaseCommand {
    /// Apply bundled forward migrations.
    Migrate,
    /// Print migration history as JSON without creating a database or history table.
    Status,
    /// Check that no migrations are pending and history matches this binary.
    Check,
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::parse();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    match options.command {
        Command::Db { command } => run_database(&options, command).await,
    }
}

fn database_config(options: &Options) -> Result<DatabaseConfig> {
    match options.backend {
        Backend::Sqlite => Ok(DatabaseConfig::Sqlite {
            path: options.sqlite_path.clone(),
        }),
        Backend::Postgres => Ok(DatabaseConfig::Postgres {
            url: options
                .postgres_url
                .clone()
                .context("PostgreSQL requires --postgres-url or AENV_DATABASE_POSTGRES_URL")?,
        }),
    }
}

async fn run_database(options: &Options, command: DatabaseCommand) -> Result<()> {
    let database = database_config(options)?;
    if matches!(command, DatabaseCommand::Migrate) {
        database.migrate().await?;
        println!("database migrations complete");
        return Ok(());
    }
    let status = database.status().await?;
    if matches!(command, DatabaseCommand::Check) {
        status.ensure_ready()?;
    }
    println!("{}", serde_json::to_string(&status)?);
    Ok(())
}
