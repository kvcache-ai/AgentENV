use anyhow::Context;
use anyhow::Result;
use clap::Args as ClapArgs;
use clap::ValueEnum;
use clap_complete::engine::{ArgValueCandidates, CompletionCandidate};
use clap_complete::env::{Bash, EnvCompleter, Fish, Zsh};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::io::Write;
use std::time::Duration;

use crate::client::sandboxes::ListedSandbox;
use crate::client::snapshots::SnapshotInfo;
use crate::client::templates::Template;
use crate::client::Client;

const DYNAMIC_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const DYNAMIC_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

/// Upper bound on the candidates one resource kind contributes.
///
/// Completion is interactive: a deployment with thousands of templates or
/// snapshots should not dump all of them into the shell's candidate list. The
/// budget is per resource kind so that many templates cannot crowd snapshots
/// out of `aenv start`'s candidates entirely.
const MAX_DYNAMIC_CANDIDATES: usize = 100;

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

/// Build a client for a completion request, or `None` when completion cannot
/// reach the API.
///
/// Completion is best-effort: missing credentials or an unusable URL yield no
/// candidates rather than an error, and the short timeouts keep a slow or
/// unreachable server from blocking the shell.
fn dynamic_client() -> Option<Client> {
    let credentials = crate::auth::load().ok()?;
    Client::new_with_timeouts(
        &credentials.url,
        &credentials.api_key,
        DYNAMIC_CONNECT_TIMEOUT,
        DYNAMIC_REQUEST_TIMEOUT,
    )
    .ok()
}

fn sandbox_candidates<F>(state_matches: F) -> Vec<CompletionCandidate>
where
    F: Fn(Option<&str>) -> bool,
{
    let Some(client) = dynamic_client() else {
        return Vec::new();
    };
    let Ok(sandboxes) = client.list_sandboxes() else {
        return Vec::new();
    };

    let mut candidates = filter_sandboxes(sandboxes, state_matches);
    candidates.sort_by(|left, right| left.sandbox_id.cmp(&right.sandbox_id));
    candidates
        .into_iter()
        .map(|sandbox| {
            CompletionCandidate::new(&sandbox.sandbox_id).help(Some(sandbox_help(&sandbox).into()))
        })
        .collect()
}

/// Describe a sandbox candidate with its template and state, which is what
/// tells otherwise indistinguishable sandbox UUIDs apart in the shell.
fn sandbox_help(sandbox: &ListedSandbox) -> String {
    let state = sandbox
        .state
        .as_deref()
        .map(str::trim)
        .filter(|state| !state.is_empty())
        .map(|state| format!(" ({state})"))
        .unwrap_or_default();
    format!("template {}{}", sandbox.template_id, state)
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

/// Candidates for arguments that accept a template ID or name, such as
/// `aenv template watch` and `aenv template delete`.
pub fn template_candidates() -> Vec<CompletionCandidate> {
    let Some(client) = dynamic_client() else {
        return Vec::new();
    };
    let Ok(templates) = client.list_templates() else {
        return Vec::new();
    };
    let mut set = CandidateSet::default();
    set.extend_templates(templates);
    set.into_vec()
}

/// Candidates for `aenv start <target>`, which accepts either a template or a
/// snapshot.
///
/// `--cold` takes an external OCI image reference instead, so no local
/// resources are offered — and no API call is made — in that case.
pub fn start_target_candidates() -> Vec<CompletionCandidate> {
    start_target_candidates_for(std::env::args_os())
}

fn start_target_candidates_for<I, T>(args: I) -> Vec<CompletionCandidate>
where
    I: IntoIterator<Item = T>,
    T: AsRef<OsStr>,
{
    if contains_cold_flag(args) {
        return Vec::new();
    }
    let Some(client) = dynamic_client() else {
        return Vec::new();
    };

    // Either lookup failing only costs us that resource kind's candidates.
    let mut set = CandidateSet::default();
    if let Ok(templates) = client.list_templates() {
        set.extend_templates(templates);
    }
    if let Ok(snapshots) = client.list_snapshots(None) {
        set.extend_snapshots(snapshots);
    }
    set.into_vec()
}

/// Whether `--cold` appears among the words being completed.
///
/// A candidate provider gets no context about the rest of the command line,
/// but the completion callback runs as `COMPLETE=<shell> aenv -- <words...>`,
/// so the words the user has typed are in our own argv. `--cold` is a boolean
/// flag, so it can never be some other argument's value here; `--cold=true` is
/// also accepted by clap, hence the prefix match.
fn contains_cold_flag<I, T>(args: I) -> bool
where
    I: IntoIterator<Item = T>,
    T: AsRef<OsStr>,
{
    args.into_iter().any(|arg| {
        let arg = arg.as_ref();
        arg == OsStr::new("--cold") || arg.to_str().is_some_and(|arg| arg.starts_with("--cold="))
    })
}

/// Ordered, de-duplicated candidate accumulator.
///
/// Human-readable names are offered ahead of IDs (see `extend_*`), a value is
/// never offered twice — the same name can be both a template name and an
/// alias — and each `extend_*` call contributes at most
/// `MAX_DYNAMIC_CANDIDATES` candidates.
#[derive(Default)]
struct CandidateSet {
    seen: HashSet<String>,
    candidates: Vec<CompletionCandidate>,
}

impl CandidateSet {
    /// Offer `value`, returning whether it was added (blank and already-offered
    /// values are skipped).
    fn push(&mut self, value: String, help: String) -> bool {
        if value.trim().is_empty() || !self.seen.insert(value.clone()) {
            return false;
        }
        self.candidates
            .push(CompletionCandidate::new(value).help(Some(help.into())));
        true
    }

    /// Add every template's names and aliases, then their IDs.
    ///
    /// Names come first because that is what users type; IDs stay available as
    /// a fallback for resources without a name. Both groups are sorted so the
    /// candidate order is stable across completion requests.
    fn extend_templates(&mut self, templates: Vec<Template>) {
        let mut names = Vec::new();
        let mut ids = Vec::new();
        for template in templates {
            let status = template
                .build_status
                .as_deref()
                .map(str::trim)
                .filter(|status| !status.is_empty())
                .map(|status| format!(" ({status})"))
                .unwrap_or_default();
            for name in template.names.iter().chain(template.aliases.iter()) {
                names.push((
                    name.clone(),
                    format!("template {}{}", template.template_id, status),
                ));
            }
            ids.push((template.template_id, format!("template{status}")));
        }
        self.extend_sorted(names, ids);
    }

    /// Add every snapshot's names, then their IDs. See `extend_templates`.
    fn extend_snapshots(&mut self, snapshots: Vec<SnapshotInfo>) {
        let mut names = Vec::new();
        let mut ids = Vec::new();
        for snapshot in snapshots {
            for name in &snapshot.names {
                names.push((name.clone(), format!("snapshot {}", snapshot.snapshot_id)));
            }
            ids.push((snapshot.snapshot_id, "snapshot".to_string()));
        }
        self.extend_sorted(names, ids);
    }

    /// Offer `names` first, then `ids`, each group sorted for a stable
    /// candidate order, up to this resource kind's budget.
    fn extend_sorted(&mut self, mut names: Vec<(String, String)>, mut ids: Vec<(String, String)>) {
        names.sort();
        ids.sort();
        let mut budget = MAX_DYNAMIC_CANDIDATES;
        for (value, help) in names.into_iter().chain(ids) {
            if budget == 0 {
                break;
            }
            if self.push(value, help) {
                budget -= 1;
            }
        }
    }

    fn into_vec(self) -> Vec<CompletionCandidate> {
        self.candidates
    }
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

pub fn add_template_candidates() -> ArgValueCandidates {
    ArgValueCandidates::new(template_candidates)
}

pub fn add_start_target_candidates() -> ArgValueCandidates {
    ArgValueCandidates::new(start_target_candidates)
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

    #[test]
    fn sandbox_help_carries_template_and_state() {
        // Sandbox IDs are UUIDs, so the description is the only thing that
        // tells two candidates apart in the shell.
        assert_eq!(
            sandbox_help(&sandbox("id", "running")),
            "template template (running)"
        );
        let mut stateless = sandbox("id", "running");
        stateless.state = None;
        assert_eq!(sandbox_help(&stateless), "template template");
    }

    fn template(id: &str, names: &[&str], aliases: &[&str], status: Option<&str>) -> Template {
        Template {
            template_id: id.to_string(),
            build_id: "build".to_string(),
            build_status: status.map(str::to_string),
            aliases: aliases.iter().map(|a| a.to_string()).collect(),
            names: names.iter().map(|n| n.to_string()).collect(),
            cpu_count: None,
            memory_mib: None,
            disk_size_mib: None,
            public: None,
            spawn_count: None,
            created_at: None,
            updated_at: None,
        }
    }

    fn snapshot_info(id: &str, names: &[&str]) -> SnapshotInfo {
        SnapshotInfo {
            snapshot_id: id.to_string(),
            names: names.iter().map(|n| n.to_string()).collect(),
            image_ref: None,
        }
    }

    /// `(value, help)` pairs, so assertions read like what the shell shows.
    fn described(candidates: Vec<CompletionCandidate>) -> Vec<(String, String)> {
        candidates
            .into_iter()
            .map(|candidate| {
                (
                    candidate.get_value().to_string_lossy().into_owned(),
                    candidate
                        .get_help()
                        .map(|help| help.to_string())
                        .unwrap_or_default(),
                )
            })
            .collect()
    }

    fn values(candidates: Vec<CompletionCandidate>) -> Vec<String> {
        described(candidates)
            .into_iter()
            .map(|(value, _)| value)
            .collect()
    }

    fn template_candidates_for(templates: Vec<Template>) -> Vec<CompletionCandidate> {
        let mut set = CandidateSet::default();
        set.extend_templates(templates);
        set.into_vec()
    }

    fn snapshot_candidates_for(snapshots: Vec<SnapshotInfo>) -> Vec<CompletionCandidate> {
        let mut set = CandidateSet::default();
        set.extend_snapshots(snapshots);
        set.into_vec()
    }

    #[test]
    fn template_names_and_aliases_precede_ids() {
        let candidates = template_candidates_for(vec![
            template("id-b", &["beta"], &[], None),
            template("id-a", &["alpha"], &["alpha-alias"], None),
        ]);
        assert_eq!(
            values(candidates),
            vec!["alpha", "alpha-alias", "beta", "id-a", "id-b"],
            "names and aliases should be offered first, each group sorted"
        );
    }

    #[test]
    fn template_help_distinguishes_kind_and_carries_id_and_status() {
        let candidates = described(template_candidates_for(vec![template(
            "id-a",
            &["alpha"],
            &[],
            Some("ready"),
        )]));
        assert_eq!(
            candidates,
            vec![
                ("alpha".to_string(), "template id-a (ready)".to_string()),
                ("id-a".to_string(), "template (ready)".to_string()),
            ]
        );
    }

    #[test]
    fn template_without_name_or_status_falls_back_to_bare_id() {
        let candidates = described(template_candidates_for(vec![template(
            "id-a",
            &[],
            &[],
            None,
        )]));
        assert_eq!(
            candidates,
            vec![("id-a".to_string(), "template".to_string())]
        );
    }

    #[test]
    fn duplicate_and_blank_values_are_dropped() {
        // A template can list the same string as both a name and an alias, and
        // two templates can share an alias; the shell should see it once.
        let candidates = template_candidates_for(vec![
            template("id-a", &["shared"], &["shared", "  "], None),
            template("id-b", &["shared"], &[], None),
        ]);
        assert_eq!(values(candidates), vec!["shared", "id-a", "id-b"]);
    }

    #[test]
    fn snapshot_names_precede_ids_and_are_labelled() {
        let candidates = described(snapshot_candidates_for(vec![
            snapshot_info("snap-b", &[]),
            snapshot_info("snap-a", &["base"]),
        ]));
        assert_eq!(
            candidates,
            vec![
                ("base".to_string(), "snapshot snap-a".to_string()),
                ("snap-a".to_string(), "snapshot".to_string()),
                ("snap-b".to_string(), "snapshot".to_string()),
            ]
        );
    }

    #[test]
    fn start_target_offers_templates_then_snapshots() {
        let mut set = CandidateSet::default();
        set.extend_templates(vec![template("id-a", &["alpha"], &[], None)]);
        set.extend_snapshots(vec![snapshot_info("snap-a", &["base"])]);
        assert_eq!(
            values(set.into_vec()),
            vec!["alpha", "id-a", "base", "snap-a"]
        );
    }

    #[test]
    fn candidate_count_is_capped_per_resource_kind() {
        let templates: Vec<Template> = (0..MAX_DYNAMIC_CANDIDATES + 10)
            .map(|i| template(&format!("id-{i:04}"), &[], &[], None))
            .collect();
        assert_eq!(
            template_candidates_for(templates).len(),
            MAX_DYNAMIC_CANDIDATES
        );
    }

    #[test]
    fn many_templates_do_not_crowd_out_snapshots() {
        let mut set = CandidateSet::default();
        set.extend_templates(
            (0..MAX_DYNAMIC_CANDIDATES + 10)
                .map(|i| template(&format!("id-{i:04}"), &[], &[], None))
                .collect(),
        );
        set.extend_snapshots(vec![snapshot_info("snap-a", &["base"])]);
        let values = values(set.into_vec());
        assert_eq!(values.len(), MAX_DYNAMIC_CANDIDATES + 2);
        assert!(
            values.contains(&"base".to_string()),
            "snapshots should still be offered after a full template budget"
        );
    }

    #[test]
    fn cold_flag_is_detected_in_completion_argv() {
        // Shape of a real callback invocation: `aenv -- <words...>`.
        assert!(contains_cold_flag([
            "aenv", "--", "aenv", "start", "--cold", ""
        ]));
        assert!(contains_cold_flag([
            "aenv",
            "--",
            "aenv",
            "start",
            "--cold=true",
            ""
        ]));
        assert!(!contains_cold_flag(["aenv", "--", "aenv", "start", ""]));
        assert!(!contains_cold_flag([
            "aenv",
            "--",
            "aenv",
            "start",
            "--coldish"
        ]));
        assert!(!contains_cold_flag(Vec::<&str>::new()));
    }

    /// The `--cold` target is an external OCI image reference, so completion
    /// must not offer local templates or snapshots there. Going through
    /// `start_target_candidates_for` also proves no API call happens: the
    /// early return runs before credentials or the network are touched, which
    /// is why this test passes with no server reachable.
    #[test]
    fn start_target_offers_nothing_for_cold_start() {
        assert!(
            start_target_candidates_for(["aenv", "--", "aenv", "start", "--cold", ""]).is_empty()
        );
    }

    #[test]
    fn start_target_arg_has_dynamic_candidates() {
        // Assert the provider is attached to the argument, not on generated
        // script text: script formatting is clap_complete's, the wiring is ours.
        let cmd = crate::Cli::command();
        let start = cmd
            .find_subcommand("start")
            .expect("`start` command exists");
        let target = start
            .get_arguments()
            .find(|arg| arg.get_id() == "target")
            .expect("`start` should declare a target argument");
        assert!(
            target.get::<ArgValueCandidates>().is_some(),
            "`start <target>` should offer dynamic template/snapshot candidates"
        );
    }

    #[test]
    fn template_watch_and_delete_args_have_dynamic_candidates() {
        let cmd = crate::Cli::command();
        let template = cmd
            .find_subcommand("template")
            .expect("`template` command exists");
        for sub in ["watch", "delete"] {
            let arg = template
                .find_subcommand(sub)
                .unwrap_or_else(|| panic!("`template {sub}` exists"))
                .get_arguments()
                .find(|arg| arg.get_id() == "template")
                .unwrap_or_else(|| panic!("`template {sub}` declares a template argument"))
                .clone();
            assert!(
                arg.get::<ArgValueCandidates>().is_some(),
                "`template {sub}` should offer dynamic template candidates"
            );
        }
    }

    #[test]
    fn snapshot_list_sandbox_filter_has_dynamic_candidates() {
        let cmd = crate::Cli::command();
        let list = cmd
            .find_subcommand("snapshot")
            .expect("`snapshot` command exists")
            .find_subcommand("list")
            .expect("`snapshot list` exists");
        let sandbox_id = list
            .get_arguments()
            .find(|arg| arg.get_long() == Some("sandbox-id"))
            .expect("`snapshot list` declares --sandbox-id");
        assert!(
            sandbox_id.get::<ArgValueCandidates>().is_some(),
            "`snapshot list --sandbox-id` should offer dynamic sandbox candidates"
        );
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
