use crate::client::Client;
use anyhow::Result;
use clap::Args as ClapArgs;

#[derive(ClapArgs)]
pub struct Args {
    #[arg(add = crate::commands::completion::add_running_sandbox_candidates())]
    sandbox_id: String,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    client.pause_sandbox(&args.sandbox_id)?;
    println!("Paused {}", args.sandbox_id);
    Ok(())
}
