use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::io::IsTerminal;
use std::sync::Arc;

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
