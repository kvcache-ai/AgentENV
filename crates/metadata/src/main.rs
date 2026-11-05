use agentenv_metadata::{transport, DatabaseConfig, MetaServer};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "aenv-metadata",
    about = "AgentENV snapshot directory and database administration"
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
    /// Serve the snapshot directory without applying database migrations.
    Serve {
        #[arg(long, env = "AENV_META_ADDRESS", default_value = "127.0.0.1:9230")]
        listen: String,
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
        Command::Serve { listen } => serve(&options, listen).await,
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

async fn serve(options: &Options, listen: &str) -> Result<()> {
    let shutdown = shutdown_signal().context("register snapshot directory shutdown signals")?;
    let server = MetaServer::open(&database_config(options)?).await?;
    let result = async {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .context("bind snapshot directory listener")?;
        tracing::info!(address = %listener.local_addr()?, "snapshot directory listening");
        transport::serve_tcp(listener, server.clone(), shutdown)
            .await
            .context("serve snapshot directory")
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
