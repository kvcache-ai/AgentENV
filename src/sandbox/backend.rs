//! Abstractions for sandbox backends.
//!
//! [`SandboxBackend`] represents the lifecycle of a single sandbox instance.
//! [`SandboxBackendFactory`] is responsible for constructing new sandbox
//! instances (from scratch, from a committed snapshot, or from paused state).

use super::manifest::SandboxSnapshotManifest;
use std::any::Any;
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    EnvdAccessToken, Executor, FreshSandboxBuildSpec, ProcessHandle, ProcessOpts, ProcessOutput,
    SandboxLaunchConfig, SandboxNetworkPolicy,
};
use crate::sandbox::CustomExtensionParams;
use crate::snapshot::RunnableSnapshot;
use crate::types::SandboxId;

/// A concrete sandbox backend's paused state.
///
/// The Orchestrator treats this value as completely opaque: it stores it in
/// [`SandboxMetadata`][crate::orchestrator::SandboxMetadata] after a
/// `pause` call and passes it back to
/// [`SandboxBackendFactory::build_from_paused_state`] when a resume is requested.
/// Concrete implementations own their serialized form.
pub trait PausedSandboxState: Any + fmt::Debug + Send + Sync + 'static {
    fn encode(&self) -> Result<Value>;

    /// Local artifacts this paused sandbox will reopen on resume.
    /// The orchestrator only carries this value to the image-liveness layer; it
    /// does not interpret the backend-specific artifact identities inside it.
    fn runtime_artifacts(&self) -> RuntimeArtifactSet;
    /// Effective envd control-plane port persisted with the paused runtime, when available.
    fn control_plane_port(&self) -> Option<u16> {
        None
    }
}

impl dyn PausedSandboxState {
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: PausedSandboxState,
    {
        (self as &dyn Any).downcast_ref::<T>()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SandboxCaptureError {
    #[error("{0}")]
    Recoverable(#[source] anyhow::Error),
    #[error("{0}")]
    Terminal(#[source] anyhow::Error),
}

impl SandboxCaptureError {
    pub fn recoverable(err: anyhow::Error) -> Self {
        Self::Recoverable(err)
    }

    pub fn terminal(err: anyhow::Error) -> Self {
        Self::Terminal(err)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal(_))
    }
}

impl From<anyhow::Error> for SandboxCaptureError {
    fn from(err: anyhow::Error) -> Self {
        match err.downcast::<Self>() {
            Ok(snapshot_err) => snapshot_err,
            Err(err) => Self::Recoverable(err),
        }
    }
}

pub type SandboxCaptureResult<T> = std::result::Result<T, SandboxCaptureError>;
pub type SandboxForkResult = anyhow::Result<Box<dyn SandboxBackend>>;

#[derive(Clone, Debug)]
pub struct SandboxForkSpec {
    pub sandbox_id: SandboxId,
    pub envd_access_token: Option<EnvdAccessToken>,
    pub extra_drives: Vec<super::ExtraDrive>,
    /// Pairs of `(source_drive_id, replacement_drive_id)`.
    pub replace_drive_ids: Vec<(String, String)>,
}

/// Opaque set of local runtime artifacts a sandbox needs while it is alive.
///
/// Sandbox backends construct this from their runtime config, the orchestrator
/// carries it across lifecycle boundaries, and the image-liveness layer decides
/// how to protect the concrete local artifacts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeArtifactSet {
    overlaybd_image_config_paths: Vec<PathBuf>,
}

/// Exact local files opened through a set of persisted overlaybd image configs.
///
/// This closure is stored with paused metadata. Generation pruning must use the
/// stored closure and re-resolve it before removing any sibling generation.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeArtifactFile {
    path: PathBuf,
    size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    digest: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeArtifactClosure {
    image_configs: Vec<RuntimeArtifactFile>,
    local_layers: Vec<RuntimeArtifactFile>,
}

impl RuntimeArtifactClosure {
    pub(crate) fn resolve(artifacts: &RuntimeArtifactSet) -> Result<Self> {
        let mut image_configs = BTreeSet::new();
        let mut local_layers = BTreeSet::new();

        for image_config_path in &artifacts.overlaybd_image_config_paths {
            let image_config_file = runtime_artifact_file(image_config_path, None, "image config")?;
            let image_config_path = &image_config_file.path;
            let image_config = overlaybd::config::load_image_config(image_config_path)
                .with_context(|| {
                    format!(
                        "load paused overlaybd image config {}",
                        image_config_path.display()
                    )
                })?;
            overlaybd::config::validate_image_config(&image_config).with_context(|| {
                format!(
                    "validate paused overlaybd image config {}",
                    image_config_path.display()
                )
            })?;

            for (index, lower) in image_config.lowers.iter().enumerate() {
                match overlaybd::layer_metadata::resolve_local_layer_path(lower) {
                    Some(path) => {
                        local_layers.insert(runtime_artifact_file(
                            &path,
                            (!lower.digest.is_empty()).then(|| lower.digest.clone()),
                            "overlaybd lower layer",
                        )?);
                    }
                    None
                        if lower.file.contains("://")
                            || (!lower.digest.is_empty()
                                && !lower
                                    .effective_repo_blob_url(&image_config.repo_blob_url)
                                    .is_empty()) => {}
                    None => bail!(
                        "paused overlaybd image config {} lower {index} has no resolvable local layer or remote source",
                        image_config_path.display()
                    ),
                }
            }
            image_configs.insert(image_config_file);
        }

        Ok(Self {
            image_configs: image_configs.into_iter().collect(),
            local_layers: local_layers.into_iter().collect(),
        })
    }

    pub(crate) fn validate(&self) -> Result<Self> {
        for artifact in self.image_configs.iter().chain(self.local_layers.iter()) {
            let actual = runtime_artifact_file(
                &artifact.path,
                artifact.digest.clone(),
                "persisted runtime artifact",
            )?;
            anyhow::ensure!(
                actual.size == artifact.size,
                "persisted runtime artifact size changed at {}: expected {}, got {}",
                artifact.path.display(),
                artifact.size,
                actual.size
            );
        }
        Self::resolve(&RuntimeArtifactSet::from_overlaybd_image_configs(
            self.image_configs
                .iter()
                .map(|artifact| artifact.path.clone())
                .collect(),
        ))
    }

    pub(crate) fn paths(&self) -> impl Iterator<Item = &Path> {
        self.image_configs
            .iter()
            .chain(self.local_layers.iter())
            .map(|artifact| artifact.path.as_path())
    }

    pub(crate) fn protected_paths(&self) -> Result<BTreeSet<PathBuf>> {
        let current = self.validate()?;
        Ok(self
            .paths()
            .chain(current.paths())
            .map(Path::to_path_buf)
            .collect())
    }
}

fn runtime_artifact_file(
    path: &Path,
    digest: Option<String>,
    description: &str,
) -> Result<RuntimeArtifactFile> {
    let canonical = std::fs::canonicalize(path)
        .with_context(|| format!("resolve {description} {}", path.display()))?;
    let metadata = std::fs::metadata(&canonical)
        .with_context(|| format!("stat {description} {}", canonical.display()))?;
    anyhow::ensure!(
        metadata.is_file(),
        "{description} is not a regular file: {}",
        canonical.display()
    );
    Ok(RuntimeArtifactFile {
        path: canonical,
        size: metadata.len(),
        digest,
    })
}

impl RuntimeArtifactSet {
    /// No local runtime artifacts.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from overlaybd image configs whose local-only layers must stay
    /// available while the sandbox may reopen them.
    pub(crate) fn from_overlaybd_image_configs(overlaybd_image_config_paths: Vec<PathBuf>) -> Self {
        Self {
            overlaybd_image_config_paths,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.overlaybd_image_config_paths.is_empty()
    }

    pub(crate) fn into_overlaybd_image_config_paths(self) -> Vec<PathBuf> {
        self.overlaybd_image_config_paths
    }

    pub(crate) fn resolve_closure(&self) -> Result<RuntimeArtifactClosure> {
        RuntimeArtifactClosure::resolve(self)
    }
}

#[cfg(test)]
mod runtime_artifact_tests {
    use super::*;

    #[test]
    fn closure_resolves_file_dir_and_remote_lowers() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let direct = temp.path().join("direct.commit");
        let dir = temp.path().join("dir-layer");
        let dir_commit = dir.join("overlaybd.commit");
        let config = temp.path().join("image.json");
        std::fs::create_dir_all(&dir)?;
        std::fs::write(&direct, b"direct")?;
        std::fs::write(&dir_commit, b"directory")?;
        std::fs::write(
            &config,
            serde_json::to_vec_pretty(&serde_json::json!({
                "repoBlobUrl": "https://registry.example/v2/repo/blobs",
                "lowers": [
                    {"file": direct, "digest": "sha256:direct", "size": 6},
                    {"dir": dir, "digest": "sha256:dir", "size": 9},
                    {"file": "https://registry.example/native-layer", "digest": "sha256:url", "size": 10},
                    {"digest": "sha256:remote", "size": 11}
                ],
                "upper": {},
                "resultFile": ""
            }))?,
        )?;

        let closure =
            RuntimeArtifactSet::from_overlaybd_image_configs(vec![config]).resolve_closure()?;

        assert_eq!(closure.image_configs.len(), 1);
        assert_eq!(closure.local_layers.len(), 2);
        assert!(closure
            .paths()
            .any(|path| path == direct.canonicalize().unwrap()));
        assert!(closure
            .paths()
            .any(|path| path == dir_commit.canonicalize().unwrap()));
        closure.validate()?;
        Ok(())
    }

    #[test]
    fn closure_validation_fails_closed_when_a_local_lower_disappears() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let lower = temp.path().join("lower.commit");
        let config = temp.path().join("image.json");
        std::fs::write(&lower, b"lower")?;
        std::fs::write(
            &config,
            serde_json::to_vec_pretty(&serde_json::json!({
                "lowers": [{"file": lower, "digest": "sha256:lower", "size": 5}],
                "upper": {},
                "resultFile": ""
            }))?,
        )?;
        let closure =
            RuntimeArtifactSet::from_overlaybd_image_configs(vec![config]).resolve_closure()?;
        std::fs::remove_file(lower)?;

        assert!(closure.validate().is_err());
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SandboxRuntimeInfo {
    pub rootfs_virtual_size: Option<u64>,
    pub runtime_artifacts: RuntimeArtifactSet,
}

/// Opaque captured snapshot artifacts produced from a running sandbox.
///
/// Unlike [`PausedSandboxState`], this value is intended for one-shot
/// consumption by snapshot publication code. Concrete backends may use it to
/// keep temporary artifact directories alive until publication finishes.
pub struct CapturedSandboxSnapshot {
    manifest: SandboxSnapshotManifest,
    artifacts: Box<dyn Any + Send>,
}

impl CapturedSandboxSnapshot {
    /// Take a capture, with whatever the backend has to hold on to until
    /// publication is over. A backend which writes its artifacts under a
    /// temporary root passes the guard of that root as `artifacts`, and one
    /// which writes them somewhere durable passes `()`.
    pub fn new<T>(manifest: SandboxSnapshotManifest, artifacts: T) -> Self
    where
        T: Send + 'static,
    {
        Self {
            manifest,
            artifacts: Box::new(artifacts),
        }
    }

    /// What was captured, in the form the snapshot layer publishes.
    pub fn manifest(&self) -> &SandboxSnapshotManifest {
        &self.manifest
    }

    /// The backend which took the capture.
    pub fn backend(&self) -> &str {
        &self.manifest.backend
    }

    pub fn downcast_artifacts_ref<T>(&self) -> Option<&T>
    where
        T: Send + 'static,
    {
        self.artifacts.downcast_ref::<T>()
    }
}

impl fmt::Debug for CapturedSandboxSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapturedSandboxSnapshot")
            .field("backend", &self.manifest.backend)
            .finish_non_exhaustive()
    }
}

/// Lifecycle interface for a single sandbox instance.
///
/// Implementors must be `Send + 'static` so that they can be stored inside
/// `Arc<Mutex<Box<dyn SandboxBackend>>>` handles managed by the Orchestrator.
#[async_trait]
pub trait SandboxBackend: Send + 'static {
    /// Capture an owned sampling future under the runtime lock, then poll it
    /// after releasing the lock. Unsupported backends return None.
    fn metrics_sample(
        &self,
    ) -> Option<futures::future::BoxFuture<'static, Result<super::SandboxMetric>>> {
        None
    }

    /// Start the sandbox and block until readiness.
    async fn start(&mut self) -> Result<()>;

    /// Start the sandbox without waiting for the sandbox to become ready.
    async fn start_nowait(&mut self) -> Result<()>;

    /// Block until the sandbox signals readiness.
    ///
    /// Should be called after [`start_nowait`][Self::start_nowait] before any
    /// workload is submitted.
    async fn wait_for_ready(&self) -> Result<()>;

    /// Pause the sandbox and capture its state for later resume.
    ///
    /// After this call the caller is expected to invoke [`stop`][Self::stop]
    /// to release system resources; the paused state encapsulates everything
    /// needed to resume the sandbox later via
    /// [`SandboxBackendFactory::build_from_paused_state`].
    ///
    /// [`SandboxCaptureError::Terminal`] indicates snapshot capture mutated the live
    /// runtime before failing, so callers must not keep treating the sandbox
    /// as safely runnable.
    ///
    /// For simplicity, [`SandboxCaptureError::Recoverable`] must guarantee the sandbox
    /// has already been restored to a running state before the error is returned.
    async fn pause(
        &mut self,
        artifact_root: Option<&Path>,
    ) -> SandboxCaptureResult<Arc<dyn PausedSandboxState>>;

    /// Resume a paused but not-yet-stopped sandbox from its snapshot.
    ///
    /// Idempotent: calling `resume` more than once must not return an error.
    async fn resume(&mut self) -> Result<()>;

    /// Capture a persistent snapshot from a running sandbox.
    ///
    /// After this call the sandbox is expected to continue running.
    ///
    /// [`SandboxCaptureError::Terminal`] indicates snapshot capture mutated the live
    /// runtime before failing, so callers must not keep treating the sandbox
    /// as safely runnable.
    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot>;

    /// Flush and seal writable persistent-volume upper layers while keeping
    /// the sandbox running.
    ///
    /// The sealed layers are node-local until the volume catalog publishes
    /// them. This operation provides the runtime half of that publication
    /// barrier without capturing rootfs, memory, or VM state.
    async fn snapshot_volumes(&mut self) -> SandboxCaptureResult<()>;

    /// Fork this running sandbox into ready child backends.
    ///
    /// The outer error is reserved for failures before child startup begins.
    /// After the source has been restored, implementations must attempt every
    /// child concurrently and return one result per `spec` entry in the
    /// same order. Successful children stay running when a sibling fails.
    ///
    /// [`SandboxCaptureError::Terminal`] indicates the fork attempt mutated the
    /// source runtime past safe resume, so callers must stop treating the
    /// source as runnable. Child construction/start failures after source
    /// recovery belong in the corresponding [`SandboxForkResult`].
    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>>;

    /// Capture the guest into `at` and describe what was written.
    ///
    /// The guest is left held. A template build takes its snapshot this way
    /// and stops the sandbox afterwards, so unlike [`snapshot`][Self::snapshot]
    /// nothing is resumed and the directory is the caller's to keep. The
    /// optional second tuple element carries the backend's opaque capture
    /// payload for consumers that need backend-specific capture state (e.g.
    /// startup-manifest recording re-booting from the capture); `None` for
    /// backends without one.
    async fn capture_to_dir(
        &mut self,
        at: &Path,
    ) -> SandboxCaptureResult<(SandboxSnapshotManifest, Option<Box<dyn Any + Send>>)>;

    /// Stop the sandbox and release all associated system resources.
    ///
    /// Idempotent: calling `stop` more than once must not return an error.
    async fn stop(&mut self) -> Result<()>;

    /// Freeze writable persistent filesystems and seal their volume layers,
    /// without capturing memory, rootfs, or VM state. On success, writes remain
    /// frozen until `stop` or `thaw_volumes`. Recoverable errors guarantee that
    /// writes have resumed; terminal errors require runtime teardown.
    /// Callers must keep ownership through completion and thaw/stop; dropping
    /// this future does not cancel guest I/O. The orchestrator shields deletion
    /// from request cancellation with an owned task.
    async fn freeze_and_snapshot_volumes(&mut self) -> SandboxCaptureResult<()>;

    /// Resume writes after abandoning a deletion that froze the volumes.
    async fn thaw_volumes(&mut self) -> Result<()>;

    /// Obtain the IP address that the sandbox can use to interact with the host.
    fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr>;

    /// Return runtime facts that are only known after the backend has started.
    fn runtime_info(&self) -> SandboxRuntimeInfo;

    /// Local runtime artifacts this sandbox opens on start.
    fn startup_artifacts(&self) -> RuntimeArtifactSet;

    /// Update the sandbox network policy at runtime.
    async fn update_network_policy(&mut self, policy: Option<SandboxNetworkPolicy>) -> Result<()>;

    /// Update the custom extension params held by the sandbox runtime.
    ///
    /// Plain assignment of an already-approved value: the custom extension
    /// patch-params hook is invoked by the caller (orchestrator layer), not
    /// by the backend. Cannot fail.
    fn update_custom_extension_params(&mut self, params: Option<CustomExtensionParams>);
}

/// Factory interface for creating and restoring sandbox backend instances.
///
/// A single factory instance is stored inside the
/// [`Orchestrator`][crate::orchestrator::Orchestrator] and is used for every
/// `create_sandbox` and `resume_sandbox` request.
pub trait SandboxBackendFactory: Send + Sync + 'static {
    /// Build a brand-new sandbox backend from a high-level launch request.
    fn build(
        &self,
        build_spec: FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>>;

    /// Build a sandbox backend from a runnable committed snapshot plus launch request.
    fn build_from_snapshot(
        &self,
        snapshot: &RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>>;

    /// Decode backend-specific paused state loaded from persistence.
    fn decode_paused_state(
        &self,
        artifact_root: PathBuf,
        state: Value,
    ) -> Result<Arc<dyn PausedSandboxState>>;

    /// Build a sandbox backend from backend-specific paused state captured by `pause`.
    fn build_from_paused_state(
        &self,
        sandbox_id: crate::types::SandboxId,
        state: &dyn PausedSandboxState,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Box<dyn SandboxBackend>>;
}

/// Process execution capability of a running sandbox.
///
/// Implement [`executor`][Self::executor] to provide a [`ProcessClient`][envd::process::ProcessClient]-backed
/// [`Executor`]. The three convenience methods (`run_command`,
/// `run_command_with_opts`, `start_process`) have default implementations that
/// simply call `self.executor()?` and delegate, so callers can continue using
/// the familiar `sandbox.run_command(...)` pattern without boilerplate.
///
/// # Note on `Send`
/// `&Self` may be `!Send` (e.g. `FirecrackerSandbox` holds tonic clients that
/// are `!Sync`), so the generated futures are not required to be `Send`.
#[async_trait(?Send)]
pub trait SandboxExecutor: Send {
    /// Obtain a process executor backed by this sandbox's envd connection.
    ///
    /// Returns an error if the sandbox is not running.
    fn executor(&self) -> Result<Executor>;

    /// Run a command inside the sandbox and wait for it to complete.
    ///
    /// Returns the captured stdout, stderr, and exit code.
    ///
    /// # Example
    /// ```no_run
    /// use agentenv::sandbox::SandboxExecutor;
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// let output = sandbox.run_command("echo", &["hello", "world"]).await?;
    /// assert_eq!(output.exit_code, 0);
    /// println!("{}", output.stdout);
    /// # Ok(())
    /// # }
    /// ```
    async fn run_command(&self, cmd: &str, args: &[&str]) -> Result<ProcessOutput> {
        self.executor()?.run_command(cmd, args).await
    }

    /// Run a command with custom options and wait for it to complete.
    ///
    /// # Example
    /// ```no_run
    /// use agentenv::sandbox::{ProcessOpts, SandboxExecutor};
    /// use std::collections::HashMap;
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// let opts = ProcessOpts::new().with_cwd("/tmp");
    /// let output = sandbox.run_command_with_opts("ls", &["-la"], &opts).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn run_command_with_opts(
        &self,
        cmd: &str,
        args: &[&str],
        opts: &ProcessOpts,
    ) -> Result<ProcessOutput> {
        self.executor()?
            .run_command_with_opts(cmd, args, opts)
            .await
    }

    /// Create a directory (and any missing parents) inside the sandbox.
    ///
    /// Goes through envd's filesystem service rather than exec'ing a binary,
    /// so it works in images that ship no userland (scratch, distroless).
    /// An already-existing directory is not an error.
    ///
    /// # Example
    /// ```no_run
    /// use agentenv::sandbox::SandboxExecutor;
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// sandbox.create_dir_all("/home/user/work").await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn create_dir_all(&self, path: &str) -> Result<()> {
        self.executor()?.create_dir_all(path).await
    }

    /// Start a long-running process and return a handle.
    ///
    /// # Example
    /// ```no_run
    /// use agentenv::sandbox::{ProcessOpts, SandboxExecutor};
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// let mut handle = sandbox.start_process("cat", &[], &ProcessOpts::default()).await?;
    /// handle.send_stdin(b"hello\n").await?;
    /// handle.kill().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn start_process(
        &self,
        cmd: &str,
        args: &[&str],
        opts: &ProcessOpts,
    ) -> Result<ProcessHandle> {
        self.executor()?.start_process(cmd, args, opts).await
    }
}
