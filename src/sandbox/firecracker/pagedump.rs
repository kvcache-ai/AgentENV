//! Best-effort export of a guest process's resident GPA ranges.
//!
//! Used at snapshot capture time to record envd's working set for the
//! memory prefetch path. Everything is best-effort: any failure yields
//! `None`, and the caller simply proceeds without a prefetch manifest.
//!
//! The export runs the static `aenv-pagedump` binary shipped in the tools
//! drive (`/agentenv/bin/aenv-pagedump`) directly (no shell or pgrep
//! dependency): one process that resolves the target by name, reads
//! `/proc/<pid>/maps` and `/proc/<pid>/pagemap`, and prints the GPA ranges
//! as JSON. When the binary is unavailable (older tools drives), the export
//! fails and the snapshot silently gets no prefetch manifest.

use std::time::Duration;

use tracing::{debug, warn};

use crate::sandbox::envd::EnvdInstance;
use crate::sandbox::process::{Executor, ProcessOpts};
use crate::snapshot::{MAX_PREFETCH_BYTES, MAX_PREFETCH_RANGES};

/// Export the present GPA ranges of `process_name` inside the sandbox.
/// Returns `None` on any failure (process missing, pagemap unreadable,
/// malformed output) — the prefetch path is strictly optional.
///
/// Takes the envd instance by value so callers can construct the executor
/// inside whichever runtime drives the future (run_command's future is not
/// `Send`; see `write_memory_prefetch_file` for the spawn_blocking pattern).
pub(crate) async fn export_process_gpa_ranges(
    envd_instance: EnvdInstance,
    process_name: String,
) -> Option<Vec<(u64, u64)>> {
    let exec = Executor::new(envd_instance);
    // Run the tools-drive binary directly (no shell/pgrep dependency): it
    // resolves the process name itself. The timeout kills a stalled dump so
    // it cannot linger into the snapshot.
    let opts = ProcessOpts::new().with_timeout(Duration::from_secs(5));
    let out = exec
        .run_command_with_opts("/agentenv/bin/aenv-pagedump", &[&process_name], &opts)
        .await
        .ok()?;
    if out.exit_code != 0 {
        debug!(
            process_name,
            "prefetch export: aenv-pagedump unavailable or failed"
        );
        return None;
    }
    let ranges = parse_pagedump_output(&out.stdout).ok()?;
    if ranges.is_empty() {
        return None;
    }
    debug!(
        process_name,
        "prefetch export: exported via aenv-pagedump binary"
    );

    let total_bytes = ranges
        .iter()
        .try_fold(0u64, |acc, (_, len)| acc.checked_add(*len))?;
    if ranges.len() > MAX_PREFETCH_RANGES || total_bytes > MAX_PREFETCH_BYTES {
        warn!(
            ranges = ranges.len(),
            total_bytes, "prefetch manifest exceeds caps; disabling prefetch for this snapshot"
        );
        return None;
    }
    Some(ranges)
}

fn parse_pagedump_output(text: &str) -> Result<Vec<(u64, u64)>, serde_json::Error> {
    #[derive(serde::Deserialize)]
    struct Dump {
        ranges: Vec<(u64, u64)>,
    }
    let dump: Dump = serde_json::from_str(text)?;
    Ok(dump.ranges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pagedump_binary_json_output() {
        let out = r#"{"ranges":[[4096,4096],[12288,8192]]}"#;
        let ranges = parse_pagedump_output(out).unwrap();
        assert_eq!(ranges, vec![(4096, 4096), (12288, 8192)]);

        assert!(parse_pagedump_output("not json").is_err());
        assert_eq!(parse_pagedump_output(r#"{"ranges":[]}"#).unwrap(), vec![]);
    }
}
