use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use tokio::time::Duration;
use tonic::Request;

use envd::filesystem::MakeDirRequest;
use envd::process::{
    process_event, process_input, process_selector, ProcessClient, ProcessConfig, ProcessInput,
    ProcessSelector, SendInputRequest, SendSignalRequest, Signal, StartRequest, StartResponse,
};

use super::envd::EnvdInstance;

const MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024; // 10 MiB

/// Options for starting a process inside the sandbox.
#[derive(Clone, Debug, Default)]
pub struct ProcessOpts {
    /// Environment variables to set for the process.
    pub envs: HashMap<String, String>,
    /// Working directory. If `None`, the process inherits the envd default.
    pub cwd: Option<String>,
    /// Maximum time to wait for the process to complete.
    pub timeout: Option<Duration>,
}

impl ProcessOpts {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set environment variables (builder-style).
    pub fn with_envs(mut self, envs: HashMap<String, String>) -> Self {
        self.envs = envs;
        self
    }

    /// Set working directory (builder-style).
    pub fn with_cwd(mut self, cwd: impl Into<String>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Set execution timeout (builder-style).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// The result of a completed process execution.
#[derive(Clone, Debug)]
pub struct ProcessOutput {
    /// Combined standard output.
    pub stdout: String,
    /// Combined standard error.
    pub stderr: String,
    /// Process exit code.
    pub exit_code: i32,
}

/// Handle to a running process inside the sandbox.
///
/// Created by [`SandboxExecutor::start_process`][crate::sandbox::SandboxExecutor::start_process].
/// Can be used to stream output, send stdin, or kill the process.
pub struct ProcessHandle {
    pid: u32,
    client: ProcessClient,
    stream: tonic::Streaming<StartResponse>,
    timeout: Option<Duration>,
}

impl ProcessHandle {
    /// The PID of the running process inside the guest VM.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Wait for the process to exit and collect all remaining output.
    pub async fn wait(&mut self) -> Result<ProcessOutput> {
        let mut stdout = String::new();
        let mut stderr = String::new();

        let collect = async {
            while let Some(response) = self.stream.next().await {
                let resp = response.context("gRPC stream error")?;
                let Some(event_wrapper) = resp.event else {
                    continue;
                };
                match event_wrapper.event {
                    Some(process_event::Event::Data(data_event)) => {
                        Self::collect_output(&data_event, &mut stdout, &mut stderr);
                        if stdout.len() + stderr.len() > MAX_OUTPUT_BYTES {
                            bail!("process output exceeded maximum allowed size ({MAX_OUTPUT_BYTES} bytes)");
                        }
                    }
                    Some(process_event::Event::End(end_event)) => {
                        return Ok(ProcessOutput {
                            stdout,
                            stderr,
                            exit_code: end_event.exit_code,
                        });
                    }
                    _ => {}
                }
            }
            bail!("process stream ended without an exit event");
        };

        match self.timeout {
            Some(timeout) => match tokio::time::timeout(timeout, collect).await {
                Ok(res) => res,
                Err(_) => {
                    // Best-effort cleanup to avoid leaving an orphaned process running.
                    let _ = self.kill().await;
                    bail!("process timed out after {timeout:?}");
                }
            },
            None => collect.await,
        }
    }

    /// Send data to the process's standard input.
    pub async fn send_stdin(&mut self, data: &[u8]) -> Result<()> {
        let selector = ProcessSelector {
            selector: Some(process_selector::Selector::Pid(self.pid)),
        };
        self.client
            .send_input(Request::new(SendInputRequest {
                process: Some(selector),
                input: Some(ProcessInput {
                    input: Some(process_input::Input::Stdin(data.to_vec())),
                }),
            }))
            .await
            .context("failed to send stdin")?;
        Ok(())
    }

    /// Send a signal to the process (e.g. SIGTERM, SIGKILL).
    pub async fn send_signal(&mut self, signal: Signal) -> Result<()> {
        let selector = ProcessSelector {
            selector: Some(process_selector::Selector::Pid(self.pid)),
        };
        self.client
            .send_signal(Request::new(SendSignalRequest {
                process: Some(selector),
                signal: signal as i32,
            }))
            .await
            .context("failed to send signal")?;
        Ok(())
    }

    /// Kill the process with SIGKILL.
    pub async fn kill(&mut self) -> Result<()> {
        self.send_signal(Signal::Sigkill).await
    }

    /// Extract stdout/stderr from a DataEvent into the provided buffers.
    fn collect_output(
        data_event: &process_event::DataEvent,
        stdout: &mut String,
        stderr: &mut String,
    ) {
        match &data_event.output {
            Some(process_event::data_event::Output::Stdout(bytes)) => {
                stdout.push_str(&String::from_utf8_lossy(bytes));
            }
            Some(process_event::data_event::Output::Stderr(bytes)) => {
                stderr.push_str(&String::from_utf8_lossy(bytes));
            }
            _ => {}
        }
    }
}

// ── Executor ──────────────────────────────────────────────────────────────────

/// A handle for executing processes inside a running sandbox.
///
/// Obtained via [`SandboxExecutor::executor`][crate::sandbox::SandboxExecutor::executor].
/// Owns a lightweight clone of the sandbox's envd connection settings.
pub struct Executor {
    envd_instance: EnvdInstance,
}

impl Executor {
    pub(super) fn new(envd_instance: EnvdInstance) -> Self {
        Self { envd_instance }
    }

    /// Run a command and wait for it to complete.
    pub async fn run_command(&self, cmd: &str, args: &[&str]) -> Result<ProcessOutput> {
        self.run_command_with_opts(cmd, args, &ProcessOpts::default())
            .await
    }

    /// Run a command with custom options and wait for it to complete.
    pub async fn run_command_with_opts(
        &self,
        cmd: &str,
        args: &[&str],
        opts: &ProcessOpts,
    ) -> Result<ProcessOutput> {
        let mut handle = self.start_process_inner(cmd, args, opts, false).await?;
        handle.wait().await
    }

    /// Start a long-running process and return a [`ProcessHandle`].
    pub async fn start_process(
        &self,
        cmd: &str,
        args: &[&str],
        opts: &ProcessOpts,
    ) -> Result<ProcessHandle> {
        self.start_process_inner(cmd, args, opts, true).await
    }

    /// Create a directory (and any missing parents) inside the sandbox via
    /// envd's filesystem service, so no binary from the guest image is
    /// involved. An already-existing directory is not an error, matching
    /// `mkdir -p` semantics.
    pub async fn create_dir_all(&self, path: &str) -> Result<()> {
        let mut client = self.envd_instance.filesystem_client().await?;
        match client
            .make_dir(Request::new(MakeDirRequest {
                path: path.to_string(),
            }))
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::AlreadyExists => Ok(()),
            Err(status) => {
                Err(status).with_context(|| format!("failed to create directory {path} via envd"))
            }
        }
    }

    /// Start a process via the envd gRPC client and return a [`ProcessHandle`].
    async fn start_process_inner(
        &self,
        cmd: &str,
        args: &[&str],
        opts: &ProcessOpts,
        stdin_enabled: bool,
    ) -> Result<ProcessHandle> {
        let request = Request::new(StartRequest {
            process: Some(ProcessConfig {
                cmd: cmd.to_string(),
                args: args.iter().map(|s| s.to_string()).collect(),
                envs: opts.envs.clone(),
                cwd: opts.cwd.clone(),
            }),
            pty: None,
            tag: None,
            stdin: Some(stdin_enabled),
        });

        let mut client = self.envd_instance.process_client().await?;
        let mut stream = client
            .start(request)
            .await
            .context("failed to start process via envd")?
            .into_inner();

        // Wait for the StartEvent to learn the PID.
        let pid = Self::wait_for_start_event(&mut stream).await?;

        Ok(ProcessHandle {
            pid,
            client,
            stream,
            timeout: opts.timeout,
        })
    }

    /// Consume stream messages until we get the `Start` event with the PID.
    async fn wait_for_start_event(stream: &mut tonic::Streaming<StartResponse>) -> Result<u32> {
        while let Some(response) = stream.next().await {
            let resp = response.context("gRPC stream error while waiting for start event")?;
            let Some(event_wrapper) = resp.event else {
                continue;
            };
            if let Some(process_event::Event::Start(start_event)) = event_wrapper.event {
                return Ok(start_event.pid);
            }
        }
        bail!("process stream closed before receiving start event");
    }
}
