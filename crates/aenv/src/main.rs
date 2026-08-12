use anyhow::Result;
use clap::{Parser, Subcommand};

mod auth;
mod client;
mod commands;
mod grpc;
mod output;
mod progress;
mod pty;

#[derive(Parser)]
#[command(name = "aenv", version, about = "AENV CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Save server URL and API key
    Auth,
    /// Build a template from a base image.
    /// Waits for the build to complete by default; exits non-zero on failure. Use -d to return immediately.
    Pull(commands::pull::Args),
    /// Build a template from a local Dockerfile
    Build(commands::build::Args),
    /// Start a sandbox and attach an interactive shell
    Start(commands::start::Args),
    /// Run a command in a sandbox
    Exec(commands::exec::Args),
    /// Upload a file to a sandbox
    Upload(commands::upload::Args),
    /// Download a file from a sandbox
    Download(commands::download::Args),
    /// Generate shell completion scripts
    Completion(commands::completion::Args),
    /// Internal dynamic completion protocol
    #[command(name = "__complete", hide = true)]
    Complete(commands::completion::CompleteArgs),
    /// Attach an interactive shell to a running sandbox
    #[command(visible_alias = "cn")]
    Connect(commands::connect::Args),
    /// Pause a running sandbox
    Pause(commands::pause::Args),
    /// Resume a paused sandbox
    Resume(commands::resume::Args),
    /// List sandboxes
    #[command(visible_alias = "ls")]
    List(commands::list::Args),
    /// Kill a sandbox
    #[command(visible_alias = "rm")]
    Delete(commands::delete::Args),
    /// Set the sandbox expiration (seconds from now)
    Timeout(commands::timeout::Args),
    /// Snapshot operations
    #[command(visible_alias = "snap")]
    Snapshot(commands::snapshot::Args),
    /// Template operations
    #[command(visible_alias = "templates")]
    Template(commands::template::Args),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Auth => commands::auth::run(),
        Cmd::Pull(a) => commands::pull::run(a),
        Cmd::Build(a) => commands::build::run(a),
        Cmd::Start(a) => commands::start::run(a),
        Cmd::Exec(a) => commands::exec::run(a),
        Cmd::Upload(a) => commands::upload::run(a),
        Cmd::Download(a) => commands::download::run(a),
        Cmd::Completion(a) => commands::completion::run(a),
        Cmd::Complete(a) => commands::completion::run_dynamic(a),
        Cmd::Connect(a) => commands::connect::run(a),
        Cmd::Pause(a) => commands::pause::run(a),
        Cmd::Resume(a) => commands::resume::run(a),
        Cmd::List(a) => commands::list::run(a),
        Cmd::Delete(a) => commands::delete::run(a),
        Cmd::Timeout(a) => commands::timeout::run(a),
        Cmd::Snapshot(a) => commands::snapshot::run(a),
        Cmd::Template(a) => commands::template::run(a),
    }
}
