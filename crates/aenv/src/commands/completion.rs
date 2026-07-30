use anyhow::Result;
use clap::Args as ClapArgs;
use clap::CommandFactory;
use clap::ValueEnum;
use clap_complete::Shell as ClapShell;
use std::io::Write;

/// Shell to generate completion for.
///
/// Limited to bash/zsh/fish per issue #37; elvish and powershell are explicit
/// non-goals.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum Shell {
    Bash,
    Zsh,
    Fish,
}

impl From<Shell> for ClapShell {
    fn from(shell: Shell) -> Self {
        match shell {
            Shell::Bash => ClapShell::Bash,
            Shell::Zsh => ClapShell::Zsh,
            Shell::Fish => ClapShell::Fish,
        }
    }
}

#[derive(ClapArgs)]
pub struct Args {
    /// Shell to generate completion for.
    #[arg(value_enum)]
    pub shell: Shell,
}

/// Generate a static completion script for the requested shell and write it to
/// stdout. The command tree is rebuilt from the live `Cli` derive spec via
/// `crate::Cli::command()` so completion can never drift from the real CLI.
pub fn run(args: Args) -> Result<()> {
    let mut cmd = crate::Cli::command();
    // Generate into memory first: clap_complete's generators panic on write
    // errors (`Generator::generate` calls `.expect`), so writing straight to
    // stdout would turn a closed downstream pipe into a panic. A `Vec` cannot
    // fail, so generation is infallible here; only the explicit stdout write
    // below can, and it propagates cleanly via `?`.
    let mut script = Vec::new();
    clap_complete::generate(ClapShell::from(args.shell), &mut cmd, "aenv", &mut script);

    let mut out = std::io::stdout().lock();
    if let Err(err) = out.write_all(&script).and_then(|_| out.flush()) {
        if err.kind() != std::io::ErrorKind::BrokenPipe {
            return Err(err.into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_for(shell: Shell) -> String {
        let mut cmd = crate::Cli::command();
        let mut buf = Vec::new();
        clap_complete::generate(ClapShell::from(shell), &mut cmd, "aenv", &mut buf);
        String::from_utf8(buf).expect("completion output is valid UTF-8")
    }

    #[test]
    fn bash_has_compdef_or_complete_f() {
        let s = generate_for(Shell::Bash);
        assert!(
            s.contains("complete -F") || s.contains("compdef"),
            "bash output should register the binary; got:\n{s}"
        );
    }

    #[test]
    fn zsh_has_compdef_header() {
        let s = generate_for(Shell::Zsh);
        assert!(
            s.starts_with("#compdef"),
            "zsh output should start with a #compdef header; got:\n{s}"
        );
    }

    #[test]
    fn fish_has_complete_calls() {
        let s = generate_for(Shell::Fish);
        assert!(
            s.contains("complete "),
            "fish output should contain `complete` invocations; got:\n{s}"
        );
    }

    #[test]
    fn includes_alias_cn() {
        // Assert a bash-specific dispatch fragment rather than a bare substring:
        // the `aenv,cn)` arm exists only because `cn` is a registered alias for
        // the top-level `connect` command.
        let s = generate_for(Shell::Bash);
        assert!(
            s.contains("aenv,cn)"),
            "completion should dispatch the `cn` alias; got:\n{s}"
        );
    }

    #[test]
    fn includes_subcommands() {
        // This hierarchical dispatch key exists only when `create` is emitted as
        // a child of `snapshot`.
        let s = generate_for(Shell::Bash);
        assert!(
            s.contains("aenv__subcmd__snapshot__subcmd__create"),
            "completion should register `snapshot create` as a subcommand; got:\n{s}"
        );
    }

    #[test]
    fn includes_output_enum_table_json() {
        // The `--output` enum is emitted as a bash `compgen -W` word list; this
        // fragment exists only for that option's possible values.
        let s = generate_for(Shell::Bash);
        assert!(
            s.contains("compgen -W \"table json\""),
            "completion should offer the --output values (table, json); got:\n{s}"
        );
    }
}
