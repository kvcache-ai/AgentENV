use crate::client::Client;
use anyhow::Result;
use clap::Args as ClapArgs;

#[derive(ClapArgs)]
pub struct Args {
    #[arg(add = crate::commands::completion::add_paused_sandbox_candidates())]
    sandbox_id: String,
    /// TTL in seconds from now. Must be longer than the sandbox's current TTL.
    #[arg(long, default_value_t = super::DEFAULT_TIMEOUT_SECS)]
    timeout: u32,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    client.connect_sandbox(&args.sandbox_id, args.timeout)?;
    println!("Resumed {}", args.sandbox_id);
    Ok(())
}
