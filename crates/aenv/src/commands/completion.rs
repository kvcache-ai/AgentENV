use anyhow::Context;
use anyhow::Result;
use clap::Args as ClapArgs;
use clap::ValueEnum;
use clap_complete::engine::{ArgValueCandidates, CompletionCandidate};
use clap_complete::env::{Bash, EnvCompleter, Fish, Zsh};
use std::io::Write;
use std::time::Duration;

use crate::client::sandboxes::ListedSandbox;

const DYNAMIC_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const DYNAMIC_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

/// Shell to generate completion for.
///
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum Shell {
    Bash,
    Zsh,
    Fish,
}

impl Shell {
    /// The `EnvCompleter` used to emit this shell's registration script.
    fn completer(self) -> &'static dyn EnvCompleter {
        match self {
            Shell::Bash => &Bash,
            Shell::Zsh => &Zsh,
            Shell::Fish => &Fish,
        }
    }
}

#[derive(ClapArgs)]
pub struct Args {
    /// Shell to generate completion for.
    #[arg(value_enum)]
    pub shell: Shell,
}

/// Generate a completion script for the requested shell and write it to stdout.
pub fn run(args: Args) -> Result<()> {
    write_completion(args.shell, &mut std::io::stdout().lock())
}

pub fn running_sandbox_candidates() -> Vec<CompletionCandidate> {
    sandbox_candidates(|state| state == Some("running"))
}

pub fn paused_sandbox_candidates() -> Vec<CompletionCandidate> {
    sandbox_candidates(|state| state == Some("paused"))
}

pub fn active_sandbox_candidates() -> Vec<CompletionCandidate> {
    sandbox_candidates(|_| true)
}

fn sandbox_candidates<F>(state_matches: F) -> Vec<CompletionCandidate>
where
    F: Fn(Option<&str>) -> bool,
{
    let Ok(credentials) = crate::auth::load() else {
        return Vec::new();
    };
    let Ok(client) = crate::client::Client::new_with_timeouts(
        &credentials.url,
        &credentials.api_key,
        DYNAMIC_CONNECT_TIMEOUT,
        DYNAMIC_REQUEST_TIMEOUT,
    ) else {
        return Vec::new();
    };
    let Ok(sandboxes) = client.list_sandboxes() else {
        return Vec::new();
    };

    let mut candidates = filter_sandboxes(sandboxes, state_matches);
    candidates.sort_by(|left, right| left.sandbox_id.cmp(&right.sandbox_id));
    candidates
        .into_iter()
        .map(|sandbox| CompletionCandidate::new(sandbox.sandbox_id))
        .collect()
}

fn filter_sandboxes<I, F>(sandboxes: I, state_matches: F) -> Vec<ListedSandbox>
where
    I: IntoIterator<Item = ListedSandbox>,
    F: Fn(Option<&str>) -> bool,
{
    sandboxes
        .into_iter()
        .filter(|sandbox| state_matches(sandbox.state.as_deref()))
        .collect()
}

pub fn add_running_sandbox_candidates() -> ArgValueCandidates {
    ArgValueCandidates::new(running_sandbox_candidates)
}

pub fn add_paused_sandbox_candidates() -> ArgValueCandidates {
    ArgValueCandidates::new(paused_sandbox_candidates)
}

pub fn add_active_sandbox_candidates() -> ArgValueCandidates {
    ArgValueCandidates::new(active_sandbox_candidates)
}

/// Generate the completion registration script for `shell` and write it to
/// `out`.
///
/// The emitted script is the dynamic engine's registration: it hooks the
/// shell so that each completion request calls back into the current `aenv`
/// binary (`COMPLETE=<shell> aenv -- ...`), which is what evaluates the
/// dynamic `ArgValueCandidates` providers (e.g. live sandbox IDs). Emitting
/// the static `clap_complete::generate` script instead would silently disable
/// those providers, so the two must not be mixed up here.
///
/// Generation goes through an in-memory buffer first: the buffer cannot fail,
/// so registration is infallible; only the explicit write below can. A
/// `BrokenPipe` there (e.g. `aenv completion bash | head`) is normal and
/// treated as success; any other error propagates with context. Split out
/// from `run` so the write branches are unit-testable.
fn write_completion<W: Write>(shell: Shell, out: &mut W) -> Result<()> {
    let mut script = Vec::new();
    shell
        .completer()
        .write_registration("COMPLETE", "aenv", "aenv", "aenv", &mut script)
        .expect("writing to an in-memory buffer cannot fail");
    match out.write_all(&script).and_then(|_| out.flush()) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(err) => Err(err).context("writing completion script to stdout"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory as _;

    fn sandbox(id: &str, state: &str) -> ListedSandbox {
        ListedSandbox {
            sandbox_id: id.to_string(),
            template_id: "template".to_string(),
            alias: None,
            state: Some(state.to_string()),
            cpu_count: None,
            memory_mib: None,
            disk_size_mib: None,
            started_at: None,
            end_at: None,
        }
    }

    #[test]
    fn state_filter_keeps_only_matching_sandboxes() {
        let sandboxes = [sandbox("paused", "paused"), sandbox("running", "running")];
        let running = filter_sandboxes(sandboxes, |state| state == Some("running"));
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].sandbox_id, "running");
    }

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

    // The registration scripts below must route completion requests back into
    // the `aenv` binary via the `COMPLETE=<shell>` environment variable: that
    // callback is what makes the dynamic `ArgValueCandidates` providers (live
    // sandbox IDs) reachable. A static script would contain the same command
    // tree but never invoke the binary at completion time.

    #[test]
    fn bash_registers_dynamic_callback() {
        let s = generate_for(Shell::Bash);
        assert!(
            s.contains("_clap_complete_aenv")
                && s.contains(r#"COMPLETE="bash""#)
                && s.contains(r#""aenv" --"#),
            "bash output should register a callback into the aenv binary; got:\n{s}"
        );
    }

    #[test]
    fn zsh_registers_dynamic_callback() {
        let s = generate_for(Shell::Zsh);
        assert!(
            s.starts_with("#compdef aenv")
                && s.contains("_clap_dynamic_completer_aenv")
                && s.contains(r#"COMPLETE="zsh""#),
            "zsh output should register a callback into the aenv binary; got:\n{s}"
        );
    }

    #[test]
    fn fish_registers_dynamic_callback() {
        let s = generate_for(Shell::Fish);
        assert!(
            s.contains("complete --keep-order --exclusive --command aenv")
                && s.contains("COMPLETE=fish aenv"),
            "fish output should register a callback into the aenv binary; got:\n{s}"
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
        let mut out = FailingWriter {
            kind: std::io::ErrorKind::BrokenPipe,
            fail_on_flush: false,
        };
        write_completion(Shell::Bash, &mut out)
            .expect("BrokenPipe during completion output should not error");
    }

    #[test]
    fn broken_pipe_on_flush_is_treated_as_success() {
        let mut out = FailingWriter {
            kind: std::io::ErrorKind::BrokenPipe,
            fail_on_flush: true,
        };
        write_completion(Shell::Bash, &mut out)
            .expect("BrokenPipe during completion flush should not error");
    }

    #[test]
    fn other_io_error_propagates() {
        let mut out = FailingWriter {
            kind: std::io::ErrorKind::Other,
            fail_on_flush: false,
        };
        let err = write_completion(Shell::Bash, &mut out)
            .expect_err("non-BrokenPipe I/O errors should propagate");
        assert!(
            err.to_string()
                .contains("writing completion script to stdout"),
            "error should carry completion context; got: {err}"
        );
    }

    #[test]
    fn other_io_error_on_flush_propagates() {
        let mut out = FailingWriter {
            kind: std::io::ErrorKind::Other,
            fail_on_flush: true,
        };
        let err =
            write_completion(Shell::Bash, &mut out).expect_err("flush errors should propagate");
        assert!(
            err.to_string()
                .contains("writing completion script to stdout"),
            "error should carry completion context; got: {err}"
        );
    }
}
