use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;

/// Overall build phases; Dockerfile output is printed above the live bar.
pub struct BuildProgress {
    bar: ProgressBar,
}

impl BuildProgress {
    pub fn new(enabled: bool) -> Result<Self> {
        let bar = if enabled && std::env::var_os("TERM").is_none_or(|term| term != "dumb") {
            ProgressBar::with_draw_target(Some(3), ProgressDrawTarget::stderr_with_hz(10))
        } else {
            ProgressBar::hidden()
        };
        bar.set_style(
            ProgressStyle::with_template(
                "{spinner:.cyan} [{wide_bar:.cyan/blue}] {pos}/{len} {msg} [{elapsed_precise}]",
            )?
            .progress_chars("=>-"),
        );
        if !bar.is_hidden() {
            bar.enable_steady_tick(Duration::from_millis(100));
        }
        Ok(Self { bar })
    }

    pub fn visible(&self) -> bool {
        !self.bar.is_hidden()
    }

    pub fn stage(&self, completed: u64, message: &str) {
        self.bar.set_position(completed);
        self.bar.set_message(message.to_owned());
        if !self.visible() {
            eprintln!("{message}...");
        }
    }

    pub fn println(&self, message: &str) {
        if self.visible() {
            self.bar.println(message);
        } else {
            eprintln!("{message}");
        }
    }

    pub fn finish(&self) {
        self.bar.set_position(3);
        self.bar.finish_and_clear();
    }
}

impl Drop for BuildProgress {
    fn drop(&mut self) {
        self.bar.finish_and_clear();
    }
}

const TEMPLATE: &str =
    "{prefix:.bold} [{bar:36.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} ETA {eta} {msg}";

#[derive(Clone)]
pub struct TransferProgress {
    state: Arc<ProgressState>,
}

struct ProgressState {
    bar: ProgressBar,
    visible: bool,
}

impl Drop for ProgressState {
    fn drop(&mut self) {
        if self.visible && !self.bar.is_finished() {
            self.bar.abandon_with_message("transfer cancelled");
        }
    }
}

impl TransferProgress {
    pub fn new(prefix: &str, total_bytes: u64) -> Result<Self> {
        let visible = std::io::stderr().is_terminal() && total_bytes > 0;
        let bar = if visible {
            ProgressBar::with_draw_target(Some(total_bytes), ProgressDrawTarget::stderr_with_hz(10))
        } else {
            ProgressBar::hidden()
        };
        bar.set_style(
            ProgressStyle::with_template(TEMPLATE)
                .context("building transfer progress style")?
                .progress_chars("=>-"),
        );
        bar.set_prefix(prefix.to_string());
        Ok(Self {
            state: Arc::new(ProgressState { bar, visible }),
        })
    }

    pub fn set_message(&self, message: impl Into<String>) {
        if self.state.visible {
            self.state.bar.set_message(message.into());
        }
    }

    pub fn inc(&self, bytes: u64) {
        self.state.bar.inc(bytes);
    }

    pub fn finish(&self) {
        if self.state.visible {
            self.state.bar.finish_and_clear();
        }
    }

    pub fn abandon(&self) {
        if self.state.visible {
            self.state.bar.abandon_with_message("transfer failed");
        }
    }
}
