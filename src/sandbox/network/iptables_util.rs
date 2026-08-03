use std::fmt::Write as _;
use std::io::Write as IoWrite;
use std::process::{Command, Stdio};

use anyhow::{anyhow, Context, Result};
use tracing::warn;

pub(super) enum IptablesRestoreCommand {
    NewChain {
        table: &'static str,
        chain: &'static str,
    },
    FlushChain {
        table: &'static str,
        chain: &'static str,
    },
    Insert {
        table: &'static str,
        chain: &'static str,
        position: i32,
        rule: String,
    },
    Append {
        table: &'static str,
        chain: &'static str,
        rule: String,
    },
    Delete {
        table: &'static str,
        chain: &'static str,
        rule: String,
    },
}

impl IptablesRestoreCommand {
    fn table(&self) -> &'static str {
        match self {
            Self::NewChain { table, .. }
            | Self::FlushChain { table, .. }
            | Self::Insert { table, .. }
            | Self::Append { table, .. }
            | Self::Delete { table, .. } => table,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum OpenFailurePolicy {
    ReturnErr,
    WarnAndIgnore(&'static str),
}

/// Builds an `iptables-restore` script from the given rules.
///
/// Rules **must** be grouped by table (all rules for one table appear consecutively).
/// A `debug_assert` fires in dev/test builds if this invariant is violated.
fn build_restore_script(commands: &[IptablesRestoreCommand]) -> String {
    debug_assert!(
        commands.windows(2).all(|w| w[0].table() <= w[1].table()),
        "iptables restore commands must be grouped by table; got interleaved tables"
    );

    let mut script = String::new();
    let mut current_table: Option<&str> = None;

    for command in commands {
        let table = command.table();
        if current_table != Some(table) {
            if current_table.is_some() {
                script.push_str("COMMIT\n");
            }
            writeln!(&mut script, "*{table}").expect("writing to String cannot fail");
            current_table = Some(table);
        }
        match command {
            IptablesRestoreCommand::NewChain { chain, .. } => {
                writeln!(&mut script, "-N {chain}").expect("writing to String cannot fail");
            }
            IptablesRestoreCommand::FlushChain { chain, .. } => {
                writeln!(&mut script, "-F {chain}").expect("writing to String cannot fail");
            }
            IptablesRestoreCommand::Insert {
                chain,
                position,
                rule,
                ..
            } => {
                writeln!(&mut script, "-I {chain} {position} {rule}")
                    .expect("writing to String cannot fail");
            }
            IptablesRestoreCommand::Append { chain, rule, .. } => {
                writeln!(&mut script, "-A {chain} {rule}").expect("writing to String cannot fail");
            }
            IptablesRestoreCommand::Delete { chain, rule, .. } => {
                writeln!(&mut script, "-D {chain} {rule}").expect("writing to String cannot fail");
            }
        }
    }

    if current_table.is_some() {
        script.push_str("COMMIT\n");
    }

    script
}

fn apply_restore_script(script: &str) -> Result<()> {
    crate::privileges::run_with_scoped_capabilities(&[crate::privileges::CAP_NET_ADMIN], || {
        let mut child = Command::new("iptables-restore")
            .arg("--noflush")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn iptables-restore")?;

        {
            let stdin = child
                .stdin
                .as_mut()
                .context("Failed to open stdin for iptables-restore")?;
            stdin
                .write_all(script.as_bytes())
                .context("Failed to write iptables-restore script")?;
        }

        let output = child
            .wait_with_output()
            .context("Failed to wait for iptables-restore")?;
        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(anyhow!("iptables-restore failed: {}", stderr.trim()))
    })
}

fn handle_restore_failure(err: anyhow::Error, policy: OpenFailurePolicy) -> Result<()> {
    match policy {
        OpenFailurePolicy::ReturnErr => Err(err),
        OpenFailurePolicy::WarnAndIgnore(message) => {
            warn!(error = %err, "{message}");
            Ok(())
        }
    }
}

/// Modify iptables rules through `iptables-restore`.
///
/// Normal rule sets are applied in one atomic batch. Cleanup consists only of
/// delete commands, which are applied individually so rules that are already
/// absent remain idempotent without parsing rule strings back into argv.
pub(super) fn apply_iptables_commands(
    commands: &[IptablesRestoreCommand],
    open_failure_policy: OpenFailurePolicy,
) -> Result<()> {
    if commands.is_empty() {
        return Ok(());
    }

    if commands
        .iter()
        .all(|command| matches!(command, IptablesRestoreCommand::Delete { .. }))
    {
        for command in commands {
            let script = build_restore_script(std::slice::from_ref(command));
            if let Err(err) = apply_restore_script(&script) {
                if is_missing_iptables_rule_error(&err.to_string()) {
                    continue;
                }
                match open_failure_policy {
                    OpenFailurePolicy::ReturnErr => return Err(err),
                    OpenFailurePolicy::WarnAndIgnore(message) => {
                        // Best-effort teardown must continue so one stale or
                        // externally removed rule does not leak later rules.
                        warn!(error = %err, "{message}");
                    }
                }
            }
        }
        return Ok(());
    }

    let script = build_restore_script(commands);
    apply_restore_script(&script).or_else(|err| handle_restore_failure(err, open_failure_policy))
}

fn is_missing_iptables_rule_error(error: &str) -> bool {
    let error = error.trim();
    let contains_ci = |needle: &str| {
        error
            .as_bytes()
            .windows(needle.len())
            .any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
    };
    contains_ci("bad rule")
        || contains_ci("does a matching rule exist")
        || contains_ci("no chain/target/match by that name")
        || contains_ci("rule does not exist")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_restore_script_renders_commands_in_order() {
        let script = build_restore_script(&[
            IptablesRestoreCommand::NewChain {
                table: "filter",
                chain: "AGENTENV-EGRESS",
            },
            IptablesRestoreCommand::NewChain {
                table: "filter",
                chain: "AGENTENV-USER-EGRESS",
            },
            IptablesRestoreCommand::Insert {
                table: "filter",
                chain: "FORWARD",
                position: 1,
                rule: "-i tap0 -o vpeer -j AGENTENV-EGRESS".to_string(),
            },
            IptablesRestoreCommand::FlushChain {
                table: "filter",
                chain: "AGENTENV-EGRESS",
            },
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "AGENTENV-EGRESS",
                rule: "-i tap0 -o vpeer -j AGENTENV-USER-EGRESS".to_string(),
            },
        ]);

        assert_eq!(
            script,
            "\
*filter
-N AGENTENV-EGRESS
-N AGENTENV-USER-EGRESS
-I FORWARD 1 -i tap0 -o vpeer -j AGENTENV-EGRESS
-F AGENTENV-EGRESS
-A AGENTENV-EGRESS -i tap0 -o vpeer -j AGENTENV-USER-EGRESS
COMMIT
"
        );
    }

    #[test]
    fn build_restore_script_groups_commands_by_table() {
        let script = build_restore_script(&[
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "-i tap0 -j ACCEPT".to_string(),
            },
            IptablesRestoreCommand::Delete {
                table: "nat",
                chain: "POSTROUTING",
                rule: "-o vpeer -j MASQUERADE".to_string(),
            },
        ]);

        assert_eq!(
            script,
            "\
*filter
-A FORWARD -i tap0 -j ACCEPT
COMMIT
*nat
-D POSTROUTING -o vpeer -j MASQUERADE
COMMIT
"
        );
    }

    #[test]
    fn build_restore_script_preserves_quoted_rule_arguments() {
        let script = build_restore_script(&[IptablesRestoreCommand::Append {
            table: "filter",
            chain: "FORWARD",
            rule: r#"-m comment --comment "sandbox traffic" -j ACCEPT"#.to_string(),
        }]);

        assert_eq!(
            script,
            "*filter\n-A FORWARD -m comment --comment \"sandbox traffic\" -j ACCEPT\nCOMMIT\n"
        );
    }
}
