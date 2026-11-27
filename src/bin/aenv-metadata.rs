use agentenv::cfg::validate_postgres_url;
use agentenv_metadata::{transport, DatabaseConfig, MetaServer};
use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "aenv-metadata",
    about = "AgentENV metadata service and database administration"
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
    #[arg(long, env = "AENV_DATABASE_SQLITE_PATH", global = true)]
    sqlite_path: Option<PathBuf>,
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
    /// Serve the metadata service without applying database migrations.
    Serve {
        #[arg(long, default_value = "127.0.0.1:9230")]
        listen: String,
        #[arg(long, env = "AENV_CONFIG_PATH")]
        config: Option<PathBuf>,
    },
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
    match &options.command {
        Command::Db { command } => run_database(&options, *command).await,
        Command::Serve { listen, config } => serve(&options, listen, config.as_deref()).await,
    }
}

fn database_config(options: &Options) -> Result<DatabaseConfig> {
    match options.backend {
        Backend::Sqlite => {
            ensure!(
                options.postgres_url.is_none(),
                "--postgres-url / AENV_DATABASE_POSTGRES_URL cannot be set when --backend=sqlite"
            );
            let path = options
                .sqlite_path
                .clone()
                .unwrap_or_else(|| PathBuf::from("metadata.sqlite3"));
            ensure!(
                !path.as_os_str().is_empty(),
                "--sqlite-path / AENV_DATABASE_SQLITE_PATH must not be empty"
            );
            Ok(DatabaseConfig::Sqlite { path })
        }
        Backend::Postgres => {
            ensure!(
                options.sqlite_path.is_none(),
                "--sqlite-path / AENV_DATABASE_SQLITE_PATH cannot be set when --backend=postgres"
            );
            let url = validate_postgres_url(
                options.postgres_url.as_deref(),
                "PostgreSQL requires --postgres-url or AENV_DATABASE_POSTGRES_URL",
            )?;
            Ok(DatabaseConfig::Postgres { url })
        }
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

async fn serve(options: &Options, listen: &str, config: Option<&std::path::Path>) -> Result<()> {
    let shutdown = shutdown_signal().context("register metadata service shutdown signals")?;
    let storage_config = agentenv::cfg::RepositoryStorageConfig::load_from_path(config)?;
    let storage = agentenv::snapshot::SnapshotGcStorage::open_from_storage_config(&storage_config)?;
    let server = MetaServer::open(&database_config(options)?)
        .await?
        .with_gc_storage(storage);
    let result = async {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .context("bind metadata service listener")?;
        tracing::info!(address = %listener.local_addr()?, "metadata service listening");
        transport::serve_tcp(listener, server.clone(), shutdown)
            .await
            .context("serve metadata service")
    }
    .await;
    server.close().await;
    result
}

fn shutdown_signal() -> std::io::Result<impl std::future::Future<Output = std::io::Result<()>>> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        Ok(async move {
            tokio::select! {
                _ = interrupt.recv() => Ok(()),
                _ = terminate.recv() => Ok(()),
            }
        })
    }
    #[cfg(not(unix))]
    Ok(tokio::signal::ctrl_c())
}
