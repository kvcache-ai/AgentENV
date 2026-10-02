use std::process::{Output, Stdio};

use anyhow::{bail, Context, Result};
use tokio::process::Command;

pub(crate) const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

pub(crate) async fn run(command: &mut Command, check: bool) -> Result<Output> {
    let program = command
        .as_std()
        .get_program()
        .to_string_lossy()
        .into_owned();
    let mut output = command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("run {program}"))?;
    output
        .stderr
        .drain(..output.stderr.len().saturating_sub(8192));
    if check && !output.status.success() {
        bail!(
            "{program} failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output)
}

pub(crate) async fn until_signal<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    tokio::select! {
        result = future => result,
        _ = term.recv() => bail!("received SIGTERM"),
        _ = interrupt.recv() => bail!("received SIGINT"),
    }
}
