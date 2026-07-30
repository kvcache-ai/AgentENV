use crate::client::{templates::Template, Client};
use crate::output::{self, Format};
use anyhow::{bail, Result};
use clap::{Args as ClapArgs, Subcommand};
use std::{
    io::{self, Write},
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
    Delete { template: String },
    /// Watch a template build until it succeeds or fails
    Watch { template: String },
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
    wait_for_build(client, &id, &id, arg, None)
}

pub(crate) fn wait_for_build(
    client: &Client,
    template_id: &str,
    build_id: &str,
    name: &str,
    deadline: Option<Instant>,
) -> Result<()> {
    let mut last_status = None::<String>;
    let mut last_line_len = 0usize;

    loop {
        let info = client.template_build_status(template_id, build_id)?;
        if info.template_id != template_id || info.build_id != build_id {
            bail!(
                "Build status response mismatch: expected template {template_id} build {build_id}, got template {} build {}",
                info.template_id,
                info.build_id
            );
        }
        let status_changed = last_status.as_deref() != Some(info.status.as_str());
        if status_changed {
            last_line_len =
                print_status_line(&format!("Build status: {}", info.status), last_line_len)?;
            last_status = Some(info.status.clone());
        }

        match info.status.as_str() {
            "ready" => {
                finish_status_line(
                    &format!("Build succeeded: template {template_id} is ready."),
                    last_line_len,
                )?;
                return Ok(());
            }
            "error" => {
                let reason = info
                    .reason
                    .map(format_build_failure_reason)
                    .unwrap_or_else(|| "unknown error".to_string());
                clear_status_line(last_line_len)?;
                bail!("Build failed: {reason}");
            }
            "waiting" | "building" => {}
            other => {
                if status_changed {
                    finish_status_line(
                        &format!("Build returned unknown status: {other}"),
                        last_line_len,
                    )?;
                    last_line_len = 0;
                }
            }
        }

        if deadline.is_some_and(|dl| Instant::now() >= dl) {
            clear_status_line(last_line_len)?;
            bail!(
                "Timed out waiting for build. Resume with: aenv template watch {}",
                name
            );
        }
        thread::sleep(BUILD_STATUS_POLL_INTERVAL);
    }
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

fn print_status_line(line: &str, previous_len: usize) -> Result<usize> {
    let clear_padding = " ".repeat(previous_len.saturating_sub(line.len()));
    print!("\r{line}{clear_padding}");
    io::stdout().flush()?;
    Ok(line.len())
}

fn finish_status_line(line: &str, previous_len: usize) -> Result<()> {
    let clear_padding = " ".repeat(previous_len.saturating_sub(line.len()));
    println!("\r{line}{clear_padding}");
    Ok(())
}

fn clear_status_line(previous_len: usize) -> Result<()> {
    if previous_len > 0 {
        print!("\r{}\r", " ".repeat(previous_len));
        io::stdout().flush()?;
    }
    Ok(())
}
