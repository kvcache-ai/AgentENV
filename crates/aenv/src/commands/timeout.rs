use crate::client::Client;
use anyhow::Result;
use clap::Args as ClapArgs;

#[derive(ClapArgs)]
pub struct Args {
    #[arg(add = crate::commands::completion::add_running_sandbox_candidates())]
    sandbox_id: String,
    /// Seconds from now until the sandbox should expire
    seconds: u32,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    client.set_timeout(&args.sandbox_id, args.seconds)?;
    println!("Timeout for {} set to {}s", args.sandbox_id, args.seconds);
    Ok(())
}
