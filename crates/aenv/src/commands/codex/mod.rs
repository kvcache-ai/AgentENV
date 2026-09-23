//! Native Codex sessions with execution isolated in AgentENV.

use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};
use std::path::PathBuf;

#[cfg(target_os = "linux")]
mod guest;
#[cfg(target_os = "linux")]
mod project;
#[cfg(target_os = "linux")]
mod runtime;
#[cfg(target_os = "linux")]
mod socket;

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv codex setup
  aenv codex start ./my-project
  aenv codex resume /path/to/session
  aenv codex finish /path/to/session")]
pub struct Args {
    #[command(subcommand)]
    command: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Prepare the bundled executor template
    Setup(Setup),
    /// Open native Codex on a sandbox copy of your project
    Start(Start),
    /// Resume a paused sandbox and its Codex conversation
    Resume(Resume),
    /// Save project files and delete the session's sandbox
    #[command(visible_alias = "recover")]
    Finish { session: PathBuf },
}

#[derive(ClapArgs)]
struct Setup {
    #[arg(long)]
    template: Option<String>,
}

#[derive(ClapArgs)]
struct TurnOptions {
    /// Override the configured model
    #[arg(long)]
    model: Option<String>,
}

#[derive(ClapArgs)]
struct Start {
    project: PathBuf,
    /// Parent directory for sessions and saved project copies
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    template: Option<String>,
    #[command(flatten)]
    turn: TurnOptions,
}

#[derive(ClapArgs)]
struct Resume {
    session: PathBuf,
    #[command(flatten)]
    turn: TurnOptions,
}

pub fn run(args: Args) -> Result<()> {
    #[cfg(target_os = "linux")]
    return runtime::run(args.command);
    #[cfg(not(target_os = "linux"))]
    {
        let _ = args;
        anyhow::bail!("aenv codex currently requires Linux")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        args: Args,
    }

    #[test]
    fn command_interface_accepts_project_paths() {
        let cli = TestCli::try_parse_from(["codex", "start", "project with spaces"]).unwrap();
        let Action::Start(args) = cli.args.command else {
            panic!("expected start")
        };
        assert_eq!(args.project, PathBuf::from("project with spaces"));
        assert!(TestCli::try_parse_from(["codex", "setup", "--template", "executor"]).is_ok());
        for option in ["--model", "--base-url"] {
            assert!(TestCli::try_parse_from(["codex", "setup", option, "value"]).is_err());
        }
        for command in ["start", "resume"] {
            assert!(TestCli::try_parse_from(["codex", command, ".", "--prompt", "Task"]).is_err());
        }
        assert!(matches!(
            TestCli::try_parse_from(["codex", "recover", "session"])
                .unwrap()
                .args
                .command,
            Action::Finish { .. }
        ));
    }
}
