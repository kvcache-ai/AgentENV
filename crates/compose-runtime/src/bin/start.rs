use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result};

fn run() -> Result<()> {
    let seconds: f64 = std::env::args()
        .nth(1)
        .context("usage: aenv-compose-start TIMEOUT_SECONDS")?
        .parse()
        .context("invalid startup timeout")?;
    let budget = Duration::try_from_secs_f64(seconds).context("invalid startup timeout")?;
    anyhow::ensure!(!budget.is_zero(), "startup timeout must be positive");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(aenv_compose_runtime::start::run(tokio::io::stdin(), budget));
    // Tokio's blocking stdin reader cannot be cancelled. Do not wait for EOF on
    // runtime shutdown after a deadline/signal; exiting also ends that thread.
    runtime.shutdown_background();
    result
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error:#}");
            ExitCode::FAILURE
        }
    }
}
