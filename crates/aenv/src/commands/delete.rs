use crate::client::Client;
use anyhow::Result;
use clap::Args as ClapArgs;

#[derive(ClapArgs)]
pub struct Args {
    #[arg(add = crate::commands::completion::add_active_sandbox_candidates())]
    sandbox_id: String,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    client.delete_sandbox(&args.sandbox_id)?;
    println!("Deleted sandbox {}", args.sandbox_id);
    Ok(())
}
