use crate::client::{
    templates::{build_status, Template},
    Client,
};
use crate::output::{self, Format};
use crate::progress::{format_elapsed, BuildProgress};
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use clap::{Args as ClapArgs, Subcommand};
use std::{
    thread,
    time::{Duration, Instant},
};
use tabled::Tabled;

const BUILD_STATUS_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    /// List all templates
    #[command(visible_alias = "ls")]
    List {
        #[arg(long, value_enum)]
        output: Option<Format>,
    },
    /// Delete a template by ID or name
    #[command(visible_alias = "rm")]
    Delete {
        #[arg(add = crate::commands::completion::add_any_template_candidates())]
        template: String,
    },
    /// Watch a template build until it succeeds or fails
    Watch {
        #[arg(add = crate::commands::completion::add_pending_template_candidates())]
        template: String,
    },
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    match args.cmd {
        Sub::List { output } => list(&client, output::resolve(output)),
        Sub::Delete { template } => delete(&client, &template),
        Sub::Watch { template } => watch(&client, &template),
    }
}

#[derive(Tabled)]
struct Row {
    name: String,
    #[tabled(rename = "TEMPLATE ID")]
    template_id: String,
    #[tabled(rename = "STATUS")]
    status: String,
    #[tabled(rename = "CPU")]
    cpu: String,
    #[tabled(rename = "MEM (MiB)")]
    mem: String,
    #[tabled(rename = "DISK (MiB)")]
    disk: String,
    #[tabled(rename = "UPDATED")]
    updated: String,
}

fn list(client: &Client, format: Format) -> Result<()> {
    let templates = client.list_templates()?;
    output::render(format, &templates, |t: &Template| Row {
        name: output::dash(
            t.names
                .first()
                .cloned()
                .or_else(|| t.aliases.first().cloned()),
        ),
        template_id: t.template_id.clone(),
        status: output::dash(t.build_status.clone()),
        cpu: output::dash(t.cpu_count),
        mem: output::dash(t.memory_mib),
        disk: output::dash(t.disk_size_mib),
        updated: output::dash(t.updated_at.clone()),
    })
}

fn delete(client: &Client, arg: &str) -> Result<()> {
    let id = crate::commands::resolve_template(client, arg)?;
    client.delete_template(&id)?;
    println!("Deleted template {}", id);
    Ok(())
}

fn watch(client: &Client, arg: &str) -> Result<()> {
    let id = crate::commands::resolve_template(client, arg)?;
    let progress = BuildProgress::with_steps(true, 6)?;
    progress.stage(1, "Resolving base image");
    wait_for_build(client, &id, &id, arg, None, &progress, None)
}

pub(crate) fn wait_for_build(
    client: &Client,
    template_id: &str,
    build_id: &str,
    name: &str,
    deadline: Option<Instant>,
    progress: &BuildProgress,
    overall_started: Option<Instant>,
) -> Result<()> {
    let mut logs_offset = 0usize;
    let mut phase = Some(BuildPhase::new(1, "Resolving base image", None));
    let mut build_started = None;
    let mut build_finished = None;

    loop {
        let info = client.template_build_status(template_id, build_id, logs_offset)?;
        if info.template_id != template_id || info.build_id != build_id {
            bail!(
                "Build status response mismatch: expected template {template_id} build {build_id}, got template {} build {}",
                info.template_id,
                info.build_id
            );
        }
        logs_offset += info.log_entries.len();
        for entry in &info.log_entries {
            if entry.message.starts_with("template build started") {
                build_started.get_or_insert(entry.timestamp);
            }
            if let Some((stage, message)) = build_phase(&entry.message) {
                advance_build_phase(progress, &mut phase, stage, message, entry.timestamp);
                continue;
            }
            if entry.message.starts_with("template build completed") {
                build_finished = Some(entry.timestamp);
                finish_build_phase(progress, &mut phase, Some(entry.timestamp));
                continue;
            }
            if is_build_lifecycle_log(&entry.message) {
                continue;
            }
            print_build_log(entry, progress);
        }

        // The status endpoint returns at most 100 entries. Drain an existing
        // backlog before sleeping or handling a terminal status.
        if info.log_entries.len() == 100 {
            continue;
        }

        match info.status.as_str() {
            build_status::READY => {
                finish_build_phase(progress, &mut phase, build_finished);
                progress.finish();
                let elapsed = overall_started
                    .map(|started| started.elapsed())
                    .or_else(|| {
                        timestamp_elapsed(build_started, build_finished.unwrap_or_else(Utc::now))
                    });
                if let Some(elapsed) = elapsed {
                    println!(
                        "Template {template_id} is ready in {}.",
                        format_elapsed(elapsed)
                    );
                } else {
                    println!("Template {template_id} is ready.");
                }
                return Ok(());
            }
            build_status::ERROR => {
                progress.finish();
                let reason = info
                    .reason
                    .map(format_build_failure_reason)
                    .unwrap_or_else(|| "unknown error".to_string());
                bail!("Build failed: {reason}");
            }
            build_status::WAITING | build_status::BUILDING => {}
            other => progress.println(&format!("Build returned unknown status: {other}")),
        }

        if deadline.is_some_and(|dl| Instant::now() >= dl) {
            progress.finish();
            bail!(
                "Timed out waiting for build. Resume with: aenv template watch {}",
                name
            );
        }
        thread::sleep(BUILD_STATUS_POLL_INTERVAL);
    }
}

struct BuildPhase {
    stage: u64,
    message: &'static str,
    timestamp: Option<DateTime<Utc>>,
    observed_at: Instant,
}

impl BuildPhase {
    fn new(stage: u64, message: &'static str, timestamp: Option<DateTime<Utc>>) -> Self {
        Self {
            stage,
            message,
            timestamp,
            observed_at: Instant::now(),
        }
    }
}

fn build_phase(message: &str) -> Option<(u64, &'static str)> {
    if message.starts_with("template build started") {
        Some((1, "Resolving base image"))
    } else if message.starts_with("template build base image resolved")
        || message.starts_with("template build base template resolved")
        || message.starts_with("executing template build")
    {
        Some((2, "Starting template build sandbox"))
    } else if message.starts_with("template build sandbox started") {
        Some((3, "Running build and startup checks"))
    } else if message.starts_with("capturing template snapshot") {
        Some((4, "Capturing template snapshot"))
    } else if message.starts_with("publishing template snapshot") {
        Some((5, "Publishing template snapshot"))
    } else {
        None
    }
}

fn is_build_lifecycle_log(message: &str) -> bool {
    message.starts_with("template snapshot published")
}

fn advance_build_phase(
    progress: &BuildProgress,
    phase: &mut Option<BuildPhase>,
    stage: u64,
    message: &'static str,
    timestamp: DateTime<Utc>,
) {
    if let Some(current) = phase.as_mut() {
        if current.stage == stage {
            current.timestamp.get_or_insert(timestamp);
            return;
        }
        if current.stage > stage {
            return;
        }
    }
    finish_build_phase(progress, phase, Some(timestamp));
    progress.stage(stage, message);
    *phase = Some(BuildPhase::new(stage, message, Some(timestamp)));
}

fn finish_build_phase(
    progress: &BuildProgress,
    phase: &mut Option<BuildPhase>,
    finished_at: Option<DateTime<Utc>>,
) {
    if let Some(phase) = phase.take() {
        let elapsed = finished_at
            .and_then(|finished| timestamp_elapsed(phase.timestamp, finished))
            .unwrap_or_else(|| phase.observed_at.elapsed());
        progress.println(&format!(
            "✓ {} [{}]",
            phase.message,
            format_elapsed(elapsed)
        ));
    }
}

fn timestamp_elapsed(started: Option<DateTime<Utc>>, finished: DateTime<Utc>) -> Option<Duration> {
    (finished - started?).to_std().ok()
}

fn print_build_log(entry: &crate::client::templates::BuildLogEntry, progress: &BuildProgress) {
    let line = match entry.level.as_str() {
        "debug" => return,
        "info" => entry.message.clone(),
        level => format!("[{level}] {}", entry.message),
    };
    progress.println(&line);
}

fn format_build_failure_reason(reason: crate::client::templates::BuildStatusReason) -> String {
    let message = if reason.message.trim().is_empty() {
        "unknown error".to_string()
    } else {
        reason.message
    };

    match reason.step.filter(|step| !step.trim().is_empty()) {
        Some(step) => format!("{message} (step: {step})"),
        None => message,
    }
}
