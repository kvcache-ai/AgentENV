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
    let mut out = std::io::stdout();
    clap_complete::generate(ClapShell::from(args.shell), &mut cmd, "aenv", &mut out);
    out.flush()?;
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
        // `cn` is a visible alias for `connect`; clap_complete emits visible
        // aliases, so it must appear in the generated script. (None of the
        // canonical command names contain the substring "cn", so its presence is
        // a clean signal that aliases flow through.)
        let s = generate_for(Shell::Bash);
        assert!(
            s.contains("cn"),
            "completion should reference the `cn` alias for connect; got:\n{s}"
        );
    }

    #[test]
    fn includes_subcommands() {
        // Subcommand groups flow through from the derive spec: `snapshot`
        // exposes a `create` child, so both must appear in the script. (`create`
        // is unique to `snapshot create` — no top-level command uses it.)
        let s = generate_for(Shell::Bash);
        assert!(
            s.contains("snapshot") && s.contains("create"),
            "completion should reference the `snapshot` command and its `create` \
             subcommand; got:\n{s}"
        );
    }

    #[test]
    fn includes_output_enum_table_json() {
        // Proves the `--output` Format ValueEnum (Table/Json) flows through.
        let s = generate_for(Shell::Bash);
        assert!(
            s.contains("table") && s.contains("json"),
            "completion should enumerate the --output values (table, json); got:\n{s}"
        );
    }
}
