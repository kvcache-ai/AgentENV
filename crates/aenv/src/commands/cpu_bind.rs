use crate::client::Client;
use anyhow::Result;
use clap::Args as ClapArgs;

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv cpu-bind <sandbox-id> --vcpu '*' --core 2-3")]
pub struct Args {
    #[arg(add = crate::commands::completion::add_running_sandbox_candidates())]
    sandbox_id: String,
    /// Firecracker vCPU list, or '*' for every current Firecracker thread
    #[arg(long)]
    vcpu: String,
    /// Host CPU list (0-1023)
    #[arg(long)]
    core: String,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let result = client.bind_cpu_affinity(&args.sandbox_id, &args.vcpu, &args.core)?;
    println!(
        "Sandbox {}: bound {} thread(s) (vCPU {}) to cores {}",
        result.sandbox_id, result.bound_thread_count, result.vcpu, result.cores
    );
    if !result.ignored_offline_cores.is_empty() {
        println!("Ignored offline cores: {}", result.ignored_offline_cores);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: Args,
    }

    #[test]
    fn parses_arguments() {
        let args = Cli::parse_from(["test", "sandbox-1", "--vcpu", "*", "--core", "2-3"]).args;
        assert_eq!(args.sandbox_id, "sandbox-1");
        assert_eq!(args.vcpu, "*");
        assert_eq!(args.core, "2-3");
    }
}
