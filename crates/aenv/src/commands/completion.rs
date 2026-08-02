use anyhow::Context;
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
    write_completion(args.shell, &mut std::io::stdout().lock())
}

/// Generate the completion script for `shell` and write it to `out`.
///
/// Generation goes through an in-memory buffer first: clap_complete's
/// generators panic on write errors (`Generator::generate` calls `.expect`),
/// so writing straight to `out` would turn a closed downstream pipe into a
/// panic. The buffer cannot fail, so generation is infallible; only the
/// explicit write below can. A `BrokenPipe` there (e.g. `aenv completion bash
/// | head`) is normal and treated as success; any other error propagates with
/// context. Split out from `run` so the write branches are unit-testable.
fn write_completion<W: Write>(shell: Shell, out: &mut W) -> Result<()> {
    let mut cmd = crate::Cli::command();
    let mut script = Vec::new();
    clap_complete::generate(ClapShell::from(shell), &mut cmd, "aenv", &mut script);
    match out.write_all(&script).and_then(|_| out.flush()) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(err) => Err(err).context("writing completion script to stdout"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_for(shell: Shell) -> String {
        let mut buf = Vec::new();
        write_completion(shell, &mut buf).expect("writing to a Vec cannot fail");
        String::from_utf8(buf).expect("completion output is valid UTF-8")
    }

    /// Writer that always fails with a configured error kind, for exercising
    /// `write_completion`'s error branches.
    struct FailingWriter {
        kind: std::io::ErrorKind,
        fail_on_flush: bool,
    }

    impl std::io::Write for FailingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.fail_on_flush {
                Ok(buf.len())
            } else {
                Err(std::io::Error::from(self.kind))
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::from(self.kind))
        }
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
    fn connect_exposes_cn_alias() {
        // Assert on the Command tree, not on clap_complete's generated bash
        // dispatch format: the internal `aenv,cn)` / `__subcmd__` naming is an
        // implementation detail a compatible clap_complete upgrade could rename
        // even though completion still works. If `connect` declares `cn` as a
        // visible alias, clap_complete emits it — that contract is ours.
        let cmd = crate::Cli::command();
        let connect = cmd
            .find_subcommand("connect")
            .expect("`connect` command exists");
        assert!(
            connect.get_visible_aliases().any(|a| a == "cn"),
            "`connect` should declare `cn` as a visible alias"
        );
    }

    #[test]
    fn snapshot_exposes_create_subcommand() {
        // See `connect_exposes_cn_alias`: assert on the Command tree, not on
        // clap_complete's internal bash helper naming.
        let cmd = crate::Cli::command();
        let snapshot = cmd
            .find_subcommand("snapshot")
            .expect("`snapshot` command exists");
        assert!(
            snapshot.find_subcommand("create").is_some(),
            "`snapshot` should expose a `create` subcommand"
        );
    }

    #[test]
    fn output_arg_offers_table_and_json() {
        // Assert on the argument metadata: the exact `compgen -W "table json"`
        // string is clap_complete's bash formatting, which a compatible upgrade
        // could change. The possible values are defined on the `--output` arg.
        let cmd = crate::Cli::command();
        let list = cmd.find_subcommand("list").expect("`list` command exists");
        let output = list
            .get_arguments()
            .find(|a| a.get_long() == Some("output"))
            .expect("`list` should declare an --output argument");
        let possible = output.get_possible_values();
        let names: Vec<&str> = possible.iter().map(|v| v.get_name()).collect();
        assert!(
            names.contains(&"table") && names.contains(&"json"),
            "--output should offer table and json; got {names:?}"
        );
    }

    #[test]
    fn write_completion_succeeds_into_buffer() {
        let mut buf: Vec<u8> = Vec::new();
        write_completion(Shell::Bash, &mut buf).expect("Vec write succeeds");
        assert!(
            !buf.is_empty(),
            "a completion script should have been written"
        );
    }

    #[test]
    fn broken_pipe_is_treated_as_success() {
        // `aenv completion bash | head` closes the pipe early; that must exit
        // cleanly rather than error or panic.
        let mut out = FailingWriter(std::io::ErrorKind::BrokenPipe);
        write_completion(Shell::Bash, &mut out)
            .expect("BrokenPipe during completion output should not error");
    }

    #[test]
    fn other_io_error_propagates() {
        let mut out = FailingWriter(std::io::ErrorKind::Other);
        let err = write_completion(Shell::Bash, &mut out)
            .expect_err("non-BrokenPipe I/O errors should propagate");
        assert!(
            err.to_string()
                .contains("writing completion script to stdout"),
            "error should carry completion context; got: {err}"
        );
    }
}
