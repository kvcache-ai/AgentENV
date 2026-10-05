//! Best-effort registry writeback of immutable, locally converted base images.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use nix::sys::statvfs::statvfs;
use overlaybd::backend::local::LocalFile;
use overlaybd::zfile::{zfile_compress, CompressArgs, CompressOptions};
use serde_json::{json, Value};
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tracing::{info, warn};

use super::cache::{stable_path_identity, ImageCacheService};
use super::local_layer::LocalLayer;
use super::oci_image::{regctl_command, regctl_stderr_is_not_found};
use super::resolver::parse_overlaybd_referrer_for_converter;
use crate::cfg::AppConfig;
use crate::digest::{sha256_digest, FileDigest};

const MANIFEST_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
const CONFIG_TYPE: &str = "application/vnd.oci.image.config.v1+json";
const LAYER_TYPE: &str = "application/vnd.containerd.overlaybd.image.layer.v1.zfile";
const ARTIFACT_TYPE: &str = "application/vnd.containerd.overlaybd.native.v1+json";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(1800);
const ATTEMPTS: usize = 3;
const FAILURE_COOLDOWN: Duration = Duration::from_secs(300);
const ACCESS_DENIED_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);
const HISTORY_LIMIT: usize = 4096;

#[derive(Debug, Clone, Copy)]
enum PublicationState {
    InFlight,
    Done(Instant),
    Failed(Instant),
    AccessDenied(Instant),
}

impl PublicationState {
    fn completed_at(self) -> Option<Instant> {
        match self {
            Self::InFlight => None,
            Self::Done(at) | Self::Failed(at) | Self::AccessDenied(at) => Some(at),
        }
    }
}

struct PublicationJob {
    publisher: Arc<Publisher>,
    source: String,
    succeeded: bool,
    access_denied: bool,
}

impl Drop for PublicationJob {
    fn drop(&mut self) {
        let mut states = self.publisher.states.lock().unwrap();
        // Bound completed history without evicting queued or running work.
        if states
            .values()
            .filter(|state| state.completed_at().is_some())
            .count()
            >= HISTORY_LIMIT
        {
            let oldest = states
                .iter()
                .filter_map(|(key, state)| state.completed_at().map(|at| (key.clone(), at)))
                .min_by_key(|(_, at)| *at)
                .unwrap()
                .0;
            states.remove(&oldest);
        }
        let now = Instant::now();
        states.insert(
            self.source.clone(),
            if self.succeeded {
                PublicationState::Done(now)
            } else if self.access_denied {
                PublicationState::AccessDenied(now)
            } else {
                PublicationState::Failed(now)
            },
        );
    }
}

#[derive(Debug)]
pub(super) struct Publisher {
    cache: Arc<ImageCacheService>,
    prefixes: Vec<String>,
    binary: PathBuf,
    staging: PathBuf,
    reserve_bytes: u64,
    capacity: Arc<Semaphore>,
    active: Semaphore,
    states: Mutex<BTreeMap<String, PublicationState>>,
    converter_id: String,
}

impl Publisher {
    pub(super) fn from_config(config: &AppConfig) -> Option<Arc<Self>> {
        let resolver = &config.image.resolver;
        if resolver.publish_overlaybd_prefixes.is_empty() {
            return None;
        }
        // Resolvers are also constructed per request; share the node's upload budget.
        static SHARED: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Publisher>>>> = OnceLock::new();
        let mut shared = SHARED.get_or_init(Mutex::default).lock().unwrap();
        Some(Arc::clone(
            shared
                .entry(stable_path_identity(&config.image.cache.root_dir))
                .or_insert_with(|| {
                    Arc::new(Self {
                        cache: ImageCacheService::shared_from_app_config(config),
                        prefixes: resolver.publish_overlaybd_prefixes.clone(),
                        binary: config.resolved_regctl_binary(),
                        staging: config.image.cache.root_dir.join("publication"),
                        reserve_bytes: resolver.min_free_disk_gb.saturating_mul(1024 * 1024 * 1024),
                        capacity: Arc::new(Semaphore::new(resolver.publication_capacity)),
                        active: Semaphore::new(resolver.publication_concurrency),
                        states: Mutex::default(),
                        converter_id: config.resolved_overlaybd_oci_converter_id(),
                    })
                }),
        ))
    }

    pub(super) fn enqueue(
        self: &Arc<Self>,
        source: &str,
        config_path: PathBuf,
        converter_id: &str,
    ) {
        if converter_id != self.converter_id {
            return;
        }
        if !self
            .prefixes
            .iter()
            .any(|prefix| source.starts_with(prefix))
        {
            return;
        }
        // Include the repository: referrers and write permissions are repository-scoped.
        let mut states = self.states.lock().unwrap();
        match states.get(source) {
            Some(PublicationState::InFlight | PublicationState::Done(_)) => return,
            Some(PublicationState::Failed(at)) if at.elapsed() < FAILURE_COOLDOWN => return,
            Some(PublicationState::AccessDenied(at)) if at.elapsed() < ACCESS_DENIED_COOLDOWN => {
                return
            }
            _ => {}
        }
        let Ok(slot) = self.capacity.clone().try_acquire_owned() else {
            metrics::counter!("agentenv_image_publication_total", "result" => "queue_full")
                .increment(1);
            return;
        };
        states.insert(source.to_string(), PublicationState::InFlight);
        drop(states);
        let mut job = PublicationJob {
            publisher: Arc::clone(self),
            source: source.to_string(),
            succeeded: false,
            access_denied: false,
        };
        let publisher = Arc::clone(self);
        let source = source.to_string();
        metrics::counter!("agentenv_image_publication_total", "result" => "queued").increment(1);
        tokio::spawn(async move {
            let _slot = slot;
            let _active = publisher
                .active
                .acquire()
                .await
                .expect("publisher remains open");
            let started = Instant::now();
            let result = publisher.publish(&source, &config_path).await;
            job.succeeded = result.is_ok();
            job.access_denied = result
                .as_ref()
                .err()
                .is_some_and(|error| error.is::<RegistryAccessDenied>());
            drop(job);
            metrics::histogram!("agentenv_image_publication_duration_seconds")
                .record(started.elapsed().as_secs_f64());
            let outcome = if result.is_ok() { "done" } else { "failed" };
            metrics::counter!("agentenv_image_publication_total", "result" => outcome).increment(1);
            match result {
                Ok(()) => info!(image = %source, "published converted base image"),
                Err(error) => {
                    warn!(image = %source, error = %format!("{error:#}"), "base image publication failed; local image remains usable")
                }
            }
        });
    }

    async fn publish(&self, source: &str, config_path: &Path) -> Result<()> {
        let referrers =
            checked_output(self.command(&["artifact", "list", "--format", "body", source])).await?;
        if parse_overlaybd_referrer_for_converter(
            std::str::from_utf8(&referrers.stdout)?,
            Some(&self.converter_id),
        )?
        .is_some()
        {
            return Ok(());
        }
        let (repository, subject_digest) = source
            .rsplit_once('@')
            .context("source must be digest-pinned")?;
        let source_output =
            checked_output(self.command(&["manifest", "get", source, "--format", "raw-body"]))
                .await?;
        ensure!(
            sha256_digest(&source_output.stdout) == subject_digest,
            "source manifest digest mismatch"
        );
        let source_manifest: Value = serde_json::from_slice(&source_output.stdout)?;
        let subject = json!({
            "mediaType": source_manifest["mediaType"].as_str().unwrap_or(MANIFEST_TYPE),
            "digest": subject_digest, "size": source_output.stdout.len(),
        });
        let config_digest = source_manifest["config"]["digest"]
            .as_str()
            .context("source config digest")?;
        let config_output =
            checked_output(self.command(&["blob", "get", repository, config_digest])).await?;
        ensure!(
            sha256_digest(&config_output.stdout) == config_digest,
            "source config digest mismatch"
        );
        let mut config: Value = serde_json::from_slice(&config_output.stdout)?;
        ensure!(
            config["os"] == "linux" && config["architecture"] == "amd64",
            "publication currently supports linux/amd64"
        );
        // Queue entries do not pin cache data indefinitely; evicted images are skipped.
        let (hold, layers) = self.cache.hold_publication_layers(config_path).await?;
        tokio::fs::create_dir_all(&self.staging).await?;
        let work = tempfile::tempdir_in(&self.staging)?;
        let blobs = work.path().join("blobs/sha256");
        tokio::fs::create_dir_all(&blobs).await?;
        tokio::fs::write(
            work.path().join("oci-layout"),
            b"{\"imageLayoutVersion\":\"1.0.0\"}",
        )
        .await?;
        tokio::fs::write(
            work.path().join("index.json"),
            b"{\"schemaVersion\":2,\"manifests\":[]}",
        )
        .await?;
        let mut descriptors = Vec::with_capacity(layers.len());
        for layer in &layers {
            self.check_space(layer)?;
            let output = work.path().join("compressed.layer");
            compress_layer(&layer.path, &output).await?;
            let descriptor = FileDigest::describe(&output).await?;
            let payload = blobs.join(descriptor.sha256.strip_prefix("sha256:").unwrap());
            tokio::fs::rename(&output, &payload).await?;
            self.put_blob(repository, work.path(), &descriptor).await?;
            descriptors.push(json!({"mediaType": LAYER_TYPE, "digest": descriptor.sha256, "size": descriptor.size}));
            tokio::fs::remove_file(payload).await?;
        }
        config["rootfs"] = json!({"type": "layers", "diff_ids": descriptors.iter().map(|d| &d["digest"]).collect::<Vec<_>>()});
        let config_bytes = serde_json::to_vec(&config)?;
        let config_digest = sha256_digest(&config_bytes);
        let config_descriptor = FileDigest {
            size: config_bytes.len() as u64,
            sha256: config_digest.clone(),
        };
        tokio::fs::write(
            blobs.join(config_digest.strip_prefix("sha256:").unwrap()),
            &config_bytes,
        )
        .await?;
        self.put_blob(repository, work.path(), &config_descriptor)
            .await?;
        let manifest = attachment(subject, &config_descriptor, descriptors, &self.converter_id);
        let bytes = serde_json::to_vec(&manifest)?;
        let manifest_ref = format!("{repository}@{}", sha256_digest(&bytes));
        let path = work.path().join("manifest.json");
        tokio::fs::write(&path, bytes).await?;
        self.put_manifest(&manifest_ref, &path).await?;
        hold.release_best_effort("registry_publication_complete")
            .await;
        Ok(())
    }

    fn check_space(&self, layer: &LocalLayer) -> Result<()> {
        let stat = statvfs(&self.staging)?;
        let free = stat.blocks_available().saturating_mul(stat.fragment_size());
        // Allow for incompressible ZFile overhead as well as foreground reserve.
        let needed = layer
            .size
            .saturating_add(layer.size / 16)
            .saturating_add(1024 * 1024)
            .saturating_add(self.reserve_bytes);
        ensure!(
            free >= needed,
            "insufficient cache disk space for background compression"
        );
        Ok(())
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = regctl_command(&self.binary);
        command.args(args).kill_on_drop(true);
        command
    }

    async fn blob_exists(&self, repository: &str, digest: &str) -> Result<bool> {
        let output = bounded_output(self.command(&["blob", "head", repository, digest])).await?;
        if output.status.success() {
            return Ok(true);
        }
        let error = String::from_utf8_lossy(&output.stderr);
        if regctl_stderr_is_not_found(&error) {
            return Ok(false);
        }
        bail!("blob lookup failed: {error}")
    }

    async fn put_blob(&self, repository: &str, layout: &Path, blob: &FileDigest) -> Result<()> {
        let source = format!("ocidir://{}", layout.display());
        let mut failure = None;
        for attempt in 0..ATTEMPTS {
            // A failed HEAD is inconclusive; let the idempotent copy use its retry budget.
            if self
                .blob_exists(repository, &blob.sha256)
                .await
                .unwrap_or(false)
            {
                return Ok(());
            }
            // OCI-directory copy supplies the payload length and supports chunked retries.
            match checked_output(self.command(&["blob", "copy", &source, repository, &blob.sha256]))
                .await
            {
                Ok(_) => {
                    metrics::counter!("agentenv_image_publication_bytes_total")
                        .increment(blob.size);
                    return Ok(());
                }
                Err(error) if error.is::<RegistryAccessDenied>() => return Err(error),
                Err(error) => failure = Some(error),
            }
            if attempt + 1 < ATTEMPTS {
                sleep(Duration::from_secs(1 << attempt)).await;
            }
        }
        // A timed-out client can still have committed its last request.
        if self
            .blob_exists(repository, &blob.sha256)
            .await
            .unwrap_or(false)
        {
            return Ok(());
        }
        Err(failure.expect("an upload was attempted"))
    }

    async fn put_manifest(&self, reference: &str, path: &Path) -> Result<()> {
        let mut failure = None;
        for attempt in 0..ATTEMPTS {
            let mut command = self.command(&[
                "manifest",
                "put",
                "--content-type",
                MANIFEST_TYPE,
                reference,
            ]);
            command.stdin(Stdio::from(std::fs::File::open(path)?));
            match checked_output(command).await {
                Ok(_) => return Ok(()),
                Err(error) if error.is::<RegistryAccessDenied>() => return Err(error),
                Err(error) => failure = Some(error),
            }
            if attempt + 1 < ATTEMPTS {
                sleep(Duration::from_secs(1 << attempt)).await;
            }
        }
        Err(failure.expect("manifest publication was attempted"))
    }
}

async fn bounded_output(mut command: Command) -> Result<Output> {
    timeout(COMMAND_TIMEOUT, command.output())
        .await
        .context("registry command timed out")?
        .context("run registry command")
}

#[derive(Debug, thiserror::Error)]
#[error("registry access denied: {0}")]
struct RegistryAccessDenied(String);

async fn checked_output(command: Command) -> Result<Output> {
    let output = bounded_output(command).await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() && stderr.contains("[http 403]") {
        return Err(RegistryAccessDenied(stderr.into_owned()).into());
    }
    ensure!(output.status.success(), "registry command failed: {stderr}");
    Ok(output)
}

async fn compress_layer(source: &Path, destination: &Path) -> Result<()> {
    let source = source.to_owned();
    let destination = destination.to_owned();
    let runtime = tokio::runtime::Handle::current();
    // ZFile compression includes synchronous CPU work and positional writes.
    tokio::task::spawn_blocking(move || {
        runtime.block_on(async move {
            let source = Arc::new(
                LocalFile::builder()
                    .read(true)
                    .write(false)
                    .create(false)
                    .open(source)?,
            );
            let destination = Arc::new(LocalFile::builder().truncate(true).open(destination)?);
            let args = CompressArgs::new(CompressOptions::new(CompressOptions::ZSTD, 32 * 1024, 1));
            zfile_compress(source, destination, &args).await
        })
    })
    .await
    .context("background compression worker")?
}

fn attachment(
    subject: Value,
    config: &FileDigest,
    layers: Vec<Value>,
    converter_id: &str,
) -> Value {
    json!({
        "schemaVersion": 2, "mediaType": MANIFEST_TYPE,
        "artifactType": ARTIFACT_TYPE, "subject": subject,
        "config": {"mediaType": CONFIG_TYPE, "digest": config.sha256, "size": config.size},
        "layers": layers,
        "annotations": {"co.prometheus.overlaybd.producer": "agentenv-writeback-zstd32-v1",
            "co.prometheus.overlaybd.converter": converter_id},
    })
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use overlaybd::zfile::zfile_open_ro;

    use super::*;
    use crate::cfg::ResolvedImageCacheConfig;
    use crate::image::oci_image::{classify_manifest, ImageFormat};

    #[tokio::test]
    async fn compression_preserves_bytes_for_partial_reads() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = temp.path().join("source");
        let output = temp.path().join("compressed");
        let bytes: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&source, &bytes).await?;
        compress_layer(&source, &output).await?;
        let file = Arc::new(
            LocalFile::builder()
                .read(true)
                .write(false)
                .create(false)
                .open(&output)?,
        );
        let reader = zfile_open_ro(file, true).await?;
        assert_eq!(reader.options().block_size, 32 * 1024);
        assert_eq!(reader.options().algo, CompressOptions::ZSTD);
        assert_eq!(reader.original_size(), bytes.len() as u64);
        for (offset, length) in [(0, 512), (32_000, 4096), (65_500, 8192), (99_488, 512)] {
            let mut actual = vec![0; length];
            assert_eq!(reader.pread(&mut actual, offset as u64).await?, length);
            assert_eq!(actual, bytes[offset..offset + length]);
        }
        assert_eq!(tokio::fs::read(source).await?, bytes);
        Ok(())
    }

    async fn fixture(
        temp: &Path,
        fail_upload: bool,
    ) -> Result<(Publisher, String, PathBuf, Value)> {
        let root = temp.join("cache");
        let commits = root.join("commits");
        tokio::fs::create_dir_all(&commits).await?;
        let layer = commits.join("test-layer");
        let bytes = vec![42u8; 65536];
        tokio::fs::write(&layer, &bytes).await?;
        let local_config = temp.join("local.json");
        tokio::fs::write(&local_config, serde_json::to_vec(&json!({"lowers": [{"file": layer, "digest": sha256_digest(&bytes), "size": bytes.len()}]}))?).await?;
        let config = json!({"architecture":"amd64", "os":"linux", "config":{"Env":["KEEP=1"], "Entrypoint":["/bin/example"], "Labels":{"keep":"yes"}}, "rootfs":{"type":"layers", "diff_ids":["old"]}, "history":[{"created_by":"example"}]});
        let config_bytes = serde_json::to_vec(&config)?;
        let manifest = serde_json::to_vec(
            &json!({"mediaType": MANIFEST_TYPE, "schemaVersion": 2, "config":{"digest": sha256_digest(&config_bytes)}}),
        )?;
        tokio::fs::write(temp.join("source-manifest"), &manifest).await?;
        tokio::fs::write(temp.join("source-config"), config_bytes).await?;
        let binary = temp.join("regctl");
        tokio::fs::write(
            &binary,
            r#"#!/bin/sh
set -eu
cd "$(dirname "$0")"
printf '%s\n' "$*" >> calls
case "$1 $2" in
  'artifact list') if test -f referrers; then cat referrers; else echo '{"manifests":[]}'; fi ;;
  'manifest get') cat source-manifest ;;
  'blob get') cat source-config ;;
  'blob head')
    if test -f fail-head; then echo '503 head unavailable' >&2; exit 1; fi
    echo 'request failed: not found [http 404]' >&2; exit 1 ;;
  'blob copy')
    if test -f deny-upload; then echo 'request failed: forbidden [http 403]' >&2; exit 1; fi
    if test -f fail-upload; then echo '503 unavailable' >&2; exit 1; fi
    cp "${3#ocidir://}/blobs/sha256/${5#sha256:}" "uploaded-${5#sha256:}"
    ;;
  'manifest put') cat > published ;;
  *) exit 2 ;;
esac
"#,
        )
        .await?;
        tokio::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).await?;
        if fail_upload {
            tokio::fs::write(temp.join("fail-upload"), b"").await?;
        }
        let publisher = Publisher {
            cache: Arc::new(ImageCacheService::from_resolved_config(
                ResolvedImageCacheConfig {
                    root_dir: root.clone(),
                    commit_store: commits,
                    remote_blocks_dir: root.join("remote"),
                    remote_blocks_size_gb: 1,
                    capacity_bytes: None,
                },
            )),
            prefixes: vec!["registry.example/".to_string()],
            binary,
            staging: root.join("publication"),
            reserve_bytes: 0,
            capacity: Arc::new(Semaphore::new(2)),
            active: Semaphore::new(1),
            states: Mutex::default(),
            converter_id: "test-converter-v1".into(),
        };
        Ok((
            publisher,
            format!("registry.example/image@{}", sha256_digest(&manifest)),
            local_config,
            config,
        ))
    }

    #[tokio::test]
    async fn only_explicit_http_forbidden_gets_long_cooldown() -> Result<()> {
        for (message, denied) in [
            ("request failed: forbidden [http 403]", true),
            ("token refresh: unauthorized [http 401]", false),
            ("open cached layer: permission denied", false),
            ("token refresh temporarily forbidden", false),
        ] {
            let mut command = Command::new("sh");
            command.args(["-c", "printf '%s' \"$1\" >&2; exit 1", "test", message]);
            let error = checked_output(command).await.unwrap_err();
            assert_eq!(error.is::<RegistryAccessDenied>(), denied, "{message}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn only_matching_native_converter_skips_publication() -> Result<()> {
        for (artifact, converter, skip) in [
            (ARTIFACT_TYPE, Some("test-converter-v1"), true),
            (ARTIFACT_TYPE, Some("older-converter"), false),
            (ARTIFACT_TYPE, None, false),
            (
                "application/vnd.azure.artifact.streaming.v1",
                Some("test-converter-v1"),
                false,
            ),
        ] {
            let temp = tempfile::tempdir()?;
            let (publisher, source, local, _) = fixture(temp.path(), false).await?;
            let mut descriptor = json!({"digest": "sha256:existing", "artifactType": artifact});
            if let Some(converter) = converter {
                descriptor["annotations"] = json!({"co.prometheus.overlaybd.converter": converter});
            }
            tokio::fs::write(
                temp.path().join("referrers"),
                serde_json::to_vec(&json!({"manifests": [descriptor]}))?,
            )
            .await?;
            publisher.publish(&source, &local).await?;
            assert_eq!(temp.path().join("published").exists(), !skip);
            if skip {
                let calls = tokio::fs::read_to_string(temp.path().join("calls")).await?;
                assert_eq!(calls.lines().count(), 1);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn denied_upload_stops_retries_and_uses_long_cooldown() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (publisher, source, local, _) = fixture(temp.path(), false).await?;
        tokio::fs::write(temp.path().join("deny-upload"), b"").await?;
        let publisher = Arc::new(publisher);
        publisher.enqueue(&source, local.clone(), "test-converter-v1");
        timeout(Duration::from_secs(30), async {
            while publisher.capacity.available_permits() != 2 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert!(matches!(
            publisher.states.lock().unwrap()[&source],
            PublicationState::AccessDenied(_)
        ));
        let calls = tokio::fs::read_to_string(temp.path().join("calls")).await?;
        assert_eq!(
            calls
                .lines()
                .filter(|line| line.starts_with("blob copy"))
                .count(),
            1
        );
        assert!(!temp.path().join("published").exists());
        publisher.states.lock().unwrap().insert(
            source.clone(),
            PublicationState::AccessDenied(Instant::now() - FAILURE_COOLDOWN),
        );
        publisher.enqueue(&source, local.clone(), "test-converter-v1");
        assert_eq!(publisher.capacity.available_permits(), 2);
        assert_eq!(
            tokio::fs::read_to_string(temp.path().join("calls")).await?,
            calls
        );
        publisher.states.lock().unwrap().insert(
            source.clone(),
            PublicationState::AccessDenied(Instant::now() - ACCESS_DENIED_COOLDOWN),
        );
        tokio::fs::remove_file(temp.path().join("deny-upload")).await?;
        publisher.enqueue(&source, local, "test-converter-v1");
        timeout(Duration::from_secs(30), async {
            while publisher.capacity.available_permits() != 2 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert!(temp.path().join("published").exists());
        Ok(())
    }

    #[tokio::test]
    async fn publishes_source_manifest_without_media_type() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (publisher, _, local, _) = fixture(temp.path(), false).await?;
        let path = temp.path().join("source-manifest");
        let mut source: Value = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
        source.as_object_mut().unwrap().remove("mediaType");
        let bytes = serde_json::to_vec(&source)?;
        tokio::fs::write(path, &bytes).await?;
        publisher
            .publish(
                &format!("registry.example/image@{}", sha256_digest(&bytes)),
                &local,
            )
            .await?;
        let manifest: Value =
            serde_json::from_slice(&tokio::fs::read(temp.path().join("published")).await?)?;
        assert_eq!(manifest["subject"]["mediaType"], MANIFEST_TYPE);
        assert_eq!(manifest["subject"]["digest"], sha256_digest(&bytes));
        assert_eq!(manifest["subject"]["size"], bytes.len());
        Ok(())
    }

    #[tokio::test]
    async fn mismatched_converter_never_queues_publication() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (publisher, source, local, _) = fixture(temp.path(), false).await?;
        let publisher = Arc::new(publisher);
        for converter in [
            "tools-oci-rootfs-v1:overlaybd-v1.0.18-aenv.1",
            "another-user-converter",
        ] {
            publisher.enqueue(&source, local.clone(), converter);
        }
        assert_eq!(publisher.capacity.available_permits(), 2);
        assert!(publisher.states.lock().unwrap().is_empty());
        tokio::task::yield_now().await;
        assert!(!temp.path().join("calls").exists());
        Ok(())
    }

    #[tokio::test]
    async fn publishes_attachment_last_and_preserves_source_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (publisher, source, local, config) = fixture(temp.path(), false).await?;
        publisher.publish(&source, &local).await?;
        let manifest: Value =
            serde_json::from_slice(&tokio::fs::read(temp.path().join("published")).await?)?;
        assert_eq!(
            manifest["subject"]["digest"],
            source.rsplit_once('@').unwrap().1
        );
        assert_eq!(manifest["artifactType"], ARTIFACT_TYPE);
        assert_eq!(
            manifest["annotations"]["co.prometheus.overlaybd.converter"],
            "test-converter-v1"
        );
        let consumer_manifest = serde_json::from_value(manifest.clone())?;
        assert_eq!(
            classify_manifest(&consumer_manifest)?,
            ImageFormat::OverlaybdNative
        );
        let digest = manifest["config"]["digest"].as_str().unwrap();
        let uploaded: Value = serde_json::from_slice(
            &tokio::fs::read(temp.path().join(format!(
                "uploaded-{}",
                digest.strip_prefix("sha256:").unwrap()
            )))
            .await?,
        )?;
        assert_eq!(uploaded["config"], config["config"]);
        assert_eq!(uploaded["history"], config["history"]);
        assert_eq!(uploaded["architecture"], "amd64");
        assert_eq!(
            uploaded["rootfs"]["diff_ids"][0],
            manifest["layers"][0]["digest"]
        );
        let calls = tokio::fs::read_to_string(temp.path().join("calls")).await?;
        assert!(calls.lines().last().unwrap().starts_with("manifest put"));
        assert_eq!(
            calls
                .lines()
                .filter(|line| line.starts_with("blob copy"))
                .count(),
            2
        );
        assert_eq!(std::fs::read_dir(&publisher.staging)?.count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn queue_is_bounded_and_does_not_wait_for_a_worker() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (publisher, source, local, _) = fixture(temp.path(), false).await?;
        let publisher = Arc::new(publisher);
        let active = publisher.active.acquire().await?;
        publisher.enqueue(
            "another.example/image@sha256:unused",
            local.clone(),
            "test-converter-v1",
        );
        assert_eq!(publisher.capacity.available_permits(), 2);
        for _ in 0..3 {
            publisher.enqueue(&source, local.clone(), "test-converter-v1");
        }
        assert_eq!(publisher.capacity.available_permits(), 1);
        publisher.enqueue(
            &source.replace("/image@", "/second@"),
            local.clone(),
            "test-converter-v1",
        );
        publisher.enqueue(
            &source.replace("/image@", "/third@"),
            local.clone(),
            "test-converter-v1",
        );
        assert_eq!(publisher.capacity.available_permits(), 0);
        assert_eq!(publisher.states.lock().unwrap().len(), 2);
        tokio::task::yield_now().await;
        assert!(!temp.path().join("calls").exists());
        drop(active);
        timeout(Duration::from_secs(30), async {
            while publisher.capacity.available_permits() != 2 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert!(temp.path().join("published").exists());
        publisher.enqueue(&source, local, "test-converter-v1");
        assert_eq!(publisher.capacity.available_permits(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn head_errors_do_not_prevent_upload_or_hide_upload_failure() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (publisher, source, local, _) = fixture(temp.path(), false).await?;
        tokio::fs::write(temp.path().join("fail-head"), b"").await?;
        publisher.publish(&source, &local).await?;
        assert!(temp.path().join("published").exists());
        tokio::fs::write(temp.path().join("fail-upload"), b"").await?;
        tokio::fs::write(temp.path().join("calls"), b"").await?;
        let error = publisher.publish(&source, &local).await.unwrap_err();
        assert!(format!("{error:#}").contains("503 unavailable"));
        let calls = tokio::fs::read_to_string(temp.path().join("calls")).await?;
        assert_eq!(
            calls
                .lines()
                .filter(|line| line.starts_with("blob copy"))
                .count(),
            ATTEMPTS
        );
        Ok(())
    }

    #[tokio::test]
    async fn failure_cooldown_allows_later_retry() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (publisher, source, local, _) = fixture(temp.path(), true).await?;
        let publisher = Arc::new(publisher);
        publisher.enqueue(&source, local.clone(), "test-converter-v1");
        timeout(Duration::from_secs(30), async {
            while publisher.capacity.available_permits() != 2 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        publisher.enqueue(&source, local.clone(), "test-converter-v1");
        assert_eq!(publisher.capacity.available_permits(), 2);
        assert!(matches!(
            publisher.states.lock().unwrap()[&source],
            PublicationState::Failed(_)
        ));
        publisher.states.lock().unwrap().insert(
            source.clone(),
            PublicationState::Failed(Instant::now() - FAILURE_COOLDOWN),
        );
        tokio::fs::remove_file(temp.path().join("fail-upload")).await?;
        publisher.enqueue(&source, local, "test-converter-v1");
        assert_eq!(publisher.capacity.available_permits(), 1);
        timeout(Duration::from_secs(30), async {
            while publisher.capacity.available_permits() != 2 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert!(temp.path().join("published").exists());
        Ok(())
    }

    #[tokio::test]
    async fn completion_history_is_bounded_and_abandoned_jobs_can_retry() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (publisher, source, _, _) = fixture(temp.path(), false).await?;
        let publisher = Arc::new(publisher);
        let now = Instant::now();
        {
            let mut states = publisher.states.lock().unwrap();
            for i in 0..HISTORY_LIMIT {
                states.insert(format!("done-{i}"), PublicationState::Done(now));
            }
            states.insert(source.clone(), PublicationState::InFlight);
            states.insert("other-running".into(), PublicationState::InFlight);
        }
        drop(PublicationJob {
            publisher: publisher.clone(),
            source: source.clone(),
            succeeded: false,
            access_denied: false,
        });
        let states = publisher.states.lock().unwrap();
        assert_eq!(states.len(), HISTORY_LIMIT + 1);
        assert!(matches!(states[&source], PublicationState::Failed(_)));
        assert!(matches!(
            states["other-running"],
            PublicationState::InFlight
        ));
        Ok(())
    }

    #[tokio::test]
    async fn failed_upload_is_bounded_and_never_publishes_attachment() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (publisher, source, local, _) = fixture(temp.path(), true).await?;
        assert!(publisher.publish(&source, &local).await.is_err());
        assert!(!temp.path().join("published").exists());
        let calls = tokio::fs::read_to_string(temp.path().join("calls")).await?;
        assert_eq!(
            calls
                .lines()
                .filter(|line| line.starts_with("blob copy"))
                .count(),
            ATTEMPTS
        );
        assert_eq!(std::fs::read_dir(&publisher.staging)?.count(), 0);
        Ok(())
    }
}
