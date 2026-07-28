//! Thin `regctl` shell-out wrapper for snapshot image export: uploads stream
//! from file descriptors, same-registry mounts use `blob copy`, cross-registry
//! blobs stream through a `blob get | blob put` pipe. Authentication stays
//! with the Docker config `regctl` reads itself; retries, the GOMAXPROCS
//! ceiling, and 404 classification follow `src/image/oci_image.rs`.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::io::AsyncWriteExt;
use tracing::warn;

use crate::image::oci_image::{
    regctl_command, regctl_stderr_is_not_found, run_regctl, REGCTL_RETRY_ATTEMPTS,
    REGCTL_RETRY_BASE_DELAY,
};
use crate::image::ImageError;
use crate::snapshot::repository::backends::common::acr::OCI_IMAGE_MANIFEST_MEDIA_TYPE;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};

/// Stateless `regctl` CLI wrapper: no registry state of its own.
pub(crate) struct Regctl {
    binary: PathBuf,
}

impl Regctl {
    pub(crate) fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
        }
    }

    /// Digest published at `image_ref`, or `None` when the ref does not exist.
    pub(crate) async fn manifest_head(&self, image_ref: &str) -> RepositoryResult<Option<String>> {
        match run_regctl(&self.binary, &["manifest", "head", image_ref]).await {
            Ok(output) => Ok(Some(
                String::from_utf8_lossy(&output.stdout).trim().to_string(),
            )),
            Err(ImageError::NotFound { .. }) => Ok(None),
            Err(error) => Err(RepositoryError::backend(
                format!("regctl manifest head {image_ref}"),
                error,
            )),
        }
    }

    pub(crate) async fn manifest_put(
        &self,
        image_ref: &str,
        manifest: &Path,
    ) -> RepositoryResult<()> {
        self.put_file(
            &[
                "manifest",
                "put",
                "--content-type",
                OCI_IMAGE_MANIFEST_MEDIA_TYPE,
                image_ref,
            ],
            manifest,
            &format!("manifest put {image_ref}"),
        )
        .await
    }

    /// Probes whether `digest` exists as a blob in `repository`.
    pub(crate) async fn blob_head(&self, repository: &str, digest: &str) -> RepositoryResult<bool> {
        match run_regctl(&self.binary, &["blob", "head", repository, digest]).await {
            Ok(_) => Ok(true),
            Err(ImageError::NotFound { .. }) => Ok(false),
            Err(error) => Err(RepositoryError::backend(
                format!("regctl blob head {repository} {digest}"),
                error,
            )),
        }
    }

    /// Uploads a blob; the child reads the payload file descriptor directly,
    /// so bytes never enter this process's memory.
    pub(crate) async fn blob_put(
        &self,
        repository: &str,
        digest: &str,
        payload: &Path,
    ) -> RepositoryResult<()> {
        self.put_file(
            &["blob", "put", repository, "--digest", digest],
            payload,
            &format!("blob put {repository} --digest {digest}"),
        )
        .await
    }

    /// Mounts a blob between two repositories of one registry; no bytes move
    /// through this process.
    pub(crate) async fn blob_copy(
        &self,
        source: &str,
        target: &str,
        digest: &str,
    ) -> RepositoryResult<()> {
        run_regctl(&self.binary, &["blob", "copy", source, target, digest])
            .await
            .map_err(|error| match error {
                ImageError::NotFound { reason } => RepositoryError::ArtifactNotFound {
                    artifact: format!(
                        "external rootfs layer blob {digest} in source repository {source}: {reason}"
                    ),
                },
                other => RepositoryError::backend(
                    format!("regctl blob copy {source} {target} {digest}"),
                    other,
                ),
            })?;
        Ok(())
    }

    /// Cross-registry transfer through a `blob get | blob put` child-process
    /// pipe; bytes never land in this process's memory or on local disk.
    pub(crate) async fn blob_pipe(
        &self,
        source: &str,
        target: &str,
        digest: &str,
    ) -> RepositoryResult<()> {
        let description =
            format!("blob get {source} {digest} | blob put {target} --digest {digest}");
        let mut backoff = REGCTL_RETRY_BASE_DELAY;
        let mut last_failure = String::new();
        for attempt in 1..=REGCTL_RETRY_ATTEMPTS {
            match self.spawn_blob_pipe(source, target, digest).await? {
                Ok(()) => return Ok(()),
                Err(summary) => last_failure = summary,
            }
            if attempt < REGCTL_RETRY_ATTEMPTS {
                warn!(attempt, error = %last_failure, "regctl {description} failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }
        Err(RepositoryError::Backend {
            message: format!(
                "regctl {description} failed after {REGCTL_RETRY_ATTEMPTS} attempts: {last_failure}"
            ),
            source: None,
        })
    }

    /// One regctl invocation reading its payload from a file: transient
    /// failures retry with backoff, a registry 404 fails fast.
    async fn put_file(
        &self,
        args: &[&str],
        payload: &Path,
        description: &str,
    ) -> RepositoryResult<()> {
        let mut backoff = REGCTL_RETRY_BASE_DELAY;
        let mut last_stderr = String::new();
        for attempt in 1..=REGCTL_RETRY_ATTEMPTS {
            let stdin = std::fs::File::open(payload).map_err(|e| {
                RepositoryError::backend(format!("open regctl {description} payload"), e)
            })?;
            let output = regctl_command(&self.binary)
                .args(args)
                .stdin(Stdio::from(stdin))
                .output()
                .await
                .map_err(|e| RepositoryError::backend(format!("run regctl {description}"), e))?;
            if output.status.success() {
                return Ok(());
            }
            last_stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if regctl_stderr_is_not_found(&last_stderr) {
                return Err(RepositoryError::Backend {
                    message: format!("regctl {description} hit a registry 404: {last_stderr}"),
                    source: None,
                });
            }
            if attempt < REGCTL_RETRY_ATTEMPTS {
                warn!(attempt, error = %last_stderr, "regctl {description} failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }
        Err(RepositoryError::Backend {
            message: format!(
                "regctl {description} failed after {REGCTL_RETRY_ATTEMPTS} attempts: {last_stderr}"
            ),
            source: None,
        })
    }

    /// One pipe attempt: `Ok(Ok(()))` transferred, `Ok(Err(summary))` is
    /// retryable, `Err` is fatal (spawn/wait failure or registry 404).
    async fn spawn_blob_pipe(
        &self,
        source: &str,
        target: &str,
        digest: &str,
    ) -> RepositoryResult<Result<(), String>> {
        let mut get_child = regctl_command(&self.binary)
            .args(["blob", "get", source, digest])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                RepositoryError::backend(format!("spawn regctl blob get {source} {digest}"), e)
            })?;
        let mut put_child = match regctl_command(&self.binary)
            .args(["blob", "put", target, "--digest", digest])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                let _ = get_child.kill().await;
                return Err(RepositoryError::backend(
                    format!("spawn regctl blob put {target} --digest {digest}"),
                    error,
                ));
            }
        };
        let mut get_stdout = get_child.stdout.take().expect("stdout was piped");
        let mut put_stdin = put_child.stdin.take().expect("stdin was piped");
        let pump = tokio::spawn(async move {
            let result = tokio::io::copy(&mut get_stdout, &mut put_stdin).await;
            let _ = put_stdin.shutdown().await;
            result
        });
        let (get_output, put_output, pump) = tokio::join!(
            get_child.wait_with_output(),
            put_child.wait_with_output(),
            pump
        );
        let get_output =
            get_output.map_err(|e| RepositoryError::backend("wait for regctl blob get", e))?;
        let put_output =
            put_output.map_err(|e| RepositoryError::backend("wait for regctl blob put", e))?;
        let get_stderr = String::from_utf8_lossy(&get_output.stderr)
            .trim()
            .to_string();
        let put_stderr = String::from_utf8_lossy(&put_output.stderr)
            .trim()
            .to_string();
        if get_output.status.success() && put_output.status.success() {
            pump.map_err(|e| RepositoryError::backend("join regctl blob pipe pump", e))?
                .map_err(|e| {
                    RepositoryError::backend("stream blob bytes between regctl blob get/put", e)
                })?;
            return Ok(Ok(()));
        }
        if !get_output.status.success() && regctl_stderr_is_not_found(&get_stderr) {
            return Err(RepositoryError::ArtifactNotFound {
                artifact: format!(
                    "external rootfs layer blob {digest} in source repository {source}: {get_stderr}"
                ),
            });
        }
        if !put_output.status.success() && regctl_stderr_is_not_found(&put_stderr) {
            return Err(RepositoryError::Backend {
                message: format!(
                    "regctl blob put {target} --digest {digest} hit a registry 404: {put_stderr}"
                ),
                source: None,
            });
        }
        Ok(Err(format!(
            "blob get: {get_stderr}; blob put: {put_stderr}"
        )))
    }
}

#[cfg(test)]
pub(crate) mod fixture {
    //! Shared fake `regctl` (bash): logs every invocation's argv, answers
    //! `head` probes, and stores blobs/manifests by digest under
    //! `{dir}/state`. No registry HTTP server runs.

    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    use crate::digest;
    use crate::snapshot::{ExternalLayer, ManagedLayer, OverlaybdLayerRef};

    const FAKE_REGCTL_SCRIPT: &str = r#"#!/usr/bin/env bash
dir="$(cd "$(dirname "$0")" && pwd)"; state="$dir/state"
printf 'argv:%s\n' "$*" >>"$dir/log"
nf() { echo "regctl: $* [http 404]: not found" >&2; exit 1; }
mkey() { printf '%s' "$1" | tr '/:@' '___'; }
case "$1 $2" in
"blob head") [ -f "$state/blobs/$3/$4" ] || nf "blob $3 $4" ;;
"blob get") [ -f "$state/blobs/$3/$4" ] || nf "blob $3 $4"; cat "$state/blobs/$3/$4" ;;
"blob put") mkdir -p "$state/blobs/$3"; cat >"$state/blobs/$3/$5" ;;
"blob copy") [ -f "$state/blobs/$3/$5" ] || nf "blob $3 $5"; mkdir -p "$state/blobs/$4"; cp "$state/blobs/$3/$5" "$state/blobs/$4/$5" ;;
"manifest head") m="$state/manifests/$(mkey "$3")"; [ -f "$m/digest" ] || nf "manifest $3"; cat "$m/digest" ;;
"manifest put") m="$state/manifests/$(mkey "$5")"; mkdir -p "$m"; cat >"$m/body"; printf 'sha256:%s\n' "$(sha256sum "$m/body" | cut -d' ' -f1)" >"$m/digest" ;;
*) echo "unexpected regctl invocation: $*" >&2; exit 2 ;;
esac
"#;

    pub(crate) fn install_fake_regctl(dir: &Path) -> PathBuf {
        let binary = dir.join("regctl");
        fs::write(&binary, FAKE_REGCTL_SCRIPT).expect("write fake regctl");
        fs::set_permissions(&binary, PermissionsExt::from_mode(0o755)).expect("chmod fake regctl");
        binary
    }

    pub(crate) fn external_ref(bytes: &[u8], repo_blob_url: &str) -> OverlaybdLayerRef {
        OverlaybdLayerRef::External(ExternalLayer {
            digest: digest::sha256_digest(bytes),
            size: bytes.len() as u64,
            repo_blob_url: repo_blob_url.to_string(),
        })
    }

    pub(crate) fn managed_ref(bytes: &[u8]) -> OverlaybdLayerRef {
        OverlaybdLayerRef::Managed(ManagedLayer {
            digest: digest::sha256_digest(bytes),
            size: bytes.len() as u64,
            uuid: None,
        })
    }

    pub(crate) fn blob_path(dir: &Path, repository: &str, digest: &str) -> PathBuf {
        dir.join("state")
            .join("blobs")
            .join(repository)
            .join(digest)
    }

    pub(crate) fn seed_blob(dir: &Path, repository: &str, digest: &str, bytes: &[u8]) {
        let path = blob_path(dir, repository, digest);
        fs::create_dir_all(path.parent().expect("blob parent")).expect("blob dir");
        fs::write(path, bytes).expect("seed blob");
    }

    fn manifest_dir(dir: &Path, image_ref: &str) -> PathBuf {
        dir.join("state")
            .join("manifests")
            .join(image_ref.replace(['/', ':', '@'], "_"))
    }

    pub(crate) fn seed_manifest(dir: &Path, image_ref: &str, manifest_digest: &str) {
        let store = manifest_dir(dir, image_ref);
        fs::create_dir_all(&store).expect("manifest store dir");
        fs::write(store.join("digest"), format!("{manifest_digest}\n")).expect("seed manifest");
    }

    /// Stored manifest body plus the digest the fake computed for it.
    pub(crate) fn stored_manifest(dir: &Path, image_ref: &str) -> (Vec<u8>, String) {
        let store = manifest_dir(dir, image_ref);
        let body = fs::read(store.join("body")).expect("stored manifest body");
        let digest = fs::read_to_string(store.join("digest"))
            .expect("stored manifest digest")
            .trim()
            .to_string();
        (body, digest)
    }

    pub(crate) fn argv_lines(dir: &Path) -> Vec<String> {
        fs::read_to_string(dir.join("log"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }
}
