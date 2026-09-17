//! Overlaybd-specific snapshot helpers for Firecracker sandboxes.
//!
//! Organizes independent snapshot layers and preserves their local files.
//! The caller supplies the directories whose artifacts need to be retained.
//!
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use overlaybd::config::{load_image_config, ImageConfig, LayerConfig};
use overlaybd::layer_metadata::resolve_local_layer_path;
use tempfile::NamedTempFile;
use tracing::warn;
use uuid::Uuid;

use crate::overlaybd::{compact_layers, OverlaybdCompactOutput};
use crate::sandbox::ublk::OverlaybdRuntimeHandle;
use crate::sandbox::ublk::UblkDeviceManager;

/// Base budget for runtime-owned lowers, reduced by one per four stable lowers.
/// This is a compaction threshold, not a limit on the total lower count.
const RUNTIME_LAYER_BUDGET_BASE: usize = 32;
const INHERITED_LAYERS_DIR: &str = "inherited-layers";
const MANAGED_BASE_LAYER_FILE: &str = "managed-base.commit";

pub(super) fn local_layer_config(path: &Path) -> LayerConfig {
    local_layer_config_with_descriptor(path, None)
}

pub(super) fn local_layer_config_with_descriptor(
    path: &Path,
    descriptor: Option<&overlaybd::LayerDescriptor>,
) -> LayerConfig {
    LayerConfig {
        file: path.display().to_string(),
        digest: descriptor
            .map(|descriptor| descriptor.digest.clone())
            .unwrap_or_default(),
        size: descriptor.map(|descriptor| descriptor.size).unwrap_or(0),
        ..Default::default()
    }
}

pub(super) fn runtime_owned_roots_for_live_config(
    roots: &[PathBuf],
    image_config_path: &Path,
) -> Vec<PathBuf> {
    let mut roots = roots.to_vec();
    if let Some(runtime_dir) = image_config_path.parent() {
        let runtime_dir =
            fs::canonicalize(runtime_dir).unwrap_or_else(|_| runtime_dir.to_path_buf());
        if !roots.contains(&runtime_dir) {
            roots.push(runtime_dir);
        }
    }
    roots
}

/// Split a list of overlaybd lowers into two parts: the prefix of stable lowers and the suffix of runtime-owned lowers.
///
/// Runtime-created local lowers are appended during resume/fork/pause.
/// Published or cache-owned layers must be materialized before that
/// suffix; otherwise adopting only the tail would either leave a dangling
/// runtime artifact reference or change lower precedence.
fn split_runtime_suffix(
    mut lowers: Vec<LayerConfig>,
    runtime_owned_roots: &[PathBuf],
) -> (Vec<LayerConfig>, Vec<LayerConfig>) {
    let Some(first_runtime_owned_index) = lowers
        .iter()
        .position(|lower| layer_references_runtime_owned_artifact(lower, runtime_owned_roots))
    else {
        return (lowers, Vec::new());
    };

    let runtime_owned_suffix = lowers.split_off(first_runtime_owned_index);

    (lowers, runtime_owned_suffix)
}

fn path_is_runtime_owned(path: &Path, runtime_owned_roots: &[PathBuf]) -> bool {
    runtime_owned_roots
        .iter()
        .any(|root| path.starts_with(root))
}

fn layer_references_runtime_owned_artifact(
    lower: &LayerConfig,
    runtime_owned_roots: &[PathBuf],
) -> bool {
    let source = resolve_local_layer_path(lower).or_else(|| {
        (!lower.dir.is_empty() && lower.file.is_empty()).then(|| PathBuf::from(&lower.dir))
    });
    source.is_some_and(|path| {
        let resolved = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        path_is_runtime_owned(&path, runtime_owned_roots)
            || path_is_runtime_owned(&resolved, runtime_owned_roots)
    })
}

fn link_or_copy_file_blocking(source: &Path, destination: &Path) -> std::io::Result<()> {
    if source == destination {
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::remove_file(destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    if let Err(error) = fs::hard_link(source, destination) {
        warn!(
            source = %source.display(),
            destination = %destination.display(),
            error = %error,
            "hard-link runtime artifact failed; falling back to sparse copy"
        );
    } else {
        return Ok(());
    }

    if let Err(error) = storage_util::copy_sparse_file_blocking(source, destination) {
        let _ = fs::remove_file(destination);
        return Err(error);
    }
    Ok(())
}

async fn link_or_copy_runtime_layer(source: &Path, destination: &Path) -> Result<()> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    let description = format!(
        "link or copy runtime artifact {} -> {}",
        source.display(),
        destination.display()
    );
    tokio::task::spawn_blocking(move || link_or_copy_file_blocking(&source, &destination))
        .await
        .context("link runtime artifact task failed")?
        .with_context(|| description)
}

pub(super) fn write_image_config(
    path: &Path,
    image: &ImageConfig,
    description: &str,
) -> Result<()> {
    let bytes =
        serde_json::to_vec_pretty(image).with_context(|| format!("serialize {description}"))?;
    let parent = path.parent().context("image config path has no parent")?;
    let mut temp = NamedTempFile::new_in(parent)
        .with_context(|| format!("create temp {description} in {}", parent.display()))?;
    temp.write_all(&bytes)
        .with_context(|| format!("write temp {description} {}", temp.path().display()))?;
    temp.persist(path)
        .map(|_| ())
        .map_err(|error| error.error)
        .with_context(|| format!("persist {description} {}", path.display()))
}

/// Record the size of a freshly sealed upper layer produced by a snapshot,
/// labeled by layer kind and whether capture compacted the runtime-owned
/// lowers. Best-effort: an unreadable layer file is only logged.
pub(super) async fn record_sealed_upper_size(
    kind: &'static str,
    layer_path: &Path,
    compacted: bool,
) {
    match tokio::fs::metadata(layer_path).await {
        Ok(metadata) => {
            metrics::histogram!(
                "agentenv_snapshot_sealed_upper_size_bytes",
                "kind" => kind,
                "compacted" => if compacted { "true" } else { "false" },
            )
            .record(metadata.len() as f64);
        }
        Err(err) => {
            warn!(
                layer_path = %layer_path.display(),
                error = %err,
                "failed to stat sealed upper layer for size metric"
            );
        }
    }
}

async fn stage_overlaybd_snapshot(
    mut image: ImageConfig,
    output_dir: &Path,
    runtime_owned_roots: &[PathBuf],
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir).await?;
    image.lowers = rewrite_lowers_with_runtime_roots(
        image.lowers,
        output_dir,
        None,
        MANAGED_BASE_LAYER_FILE,
        runtime_owned_roots,
        // Rootfs layers stay raw; only memory snapshots may be compressed.
        OverlaybdCompactOutput::Raw,
    )
    .await?
    .0;
    image.upper = Default::default();
    image.result_file = "./result.txt".to_string();
    let output_path = output_dir.join("image.json");
    write_image_config(&output_path, &image, "overlaybd snapshot config")?;
    Ok(output_path)
}

fn runtime_layer_budget(base_layers: usize) -> usize {
    RUNTIME_LAYER_BUDGET_BASE
        .saturating_sub(base_layers / 4)
        .max(1)
}

pub(super) async fn rewrite_lowers_with_runtime_roots(
    existing_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    appended_layer: Option<LayerConfig>,
    compaction_output_name: &'static str,
    runtime_owned_roots: &[PathBuf],
    compaction_output: OverlaybdCompactOutput,
) -> Result<(Vec<LayerConfig>, bool)> {
    let (mut lowers, runtime_owned_lowers) =
        split_runtime_suffix(existing_lowers, runtime_owned_roots);

    let max_runtime_owned_layers = runtime_layer_budget(lowers.len());
    let compactable_count = runtime_owned_lowers.len() + usize::from(appended_layer.is_some());
    if compactable_count > max_runtime_owned_layers {
        let mut runtime_suffix = runtime_owned_lowers;
        if let Some(layer) = appended_layer {
            runtime_suffix.push(layer);
        }
        if runtime_suffix.iter().any(|lower| lower.file.is_empty()) {
            anyhow::bail!("cannot compact runtime-owned overlaybd suffix with remote lowers");
        }
        if let Some(compacted_path) = compact_layers(
            &runtime_suffix,
            &output_dir.join(compaction_output_name),
            compaction_output,
        )
        .await?
        {
            lowers.push(local_layer_config(&compacted_path));
        }
        return Ok((lowers, true));
    }

    let mut lowers = adopt_runtime_owned_lowers(
        lowers,
        runtime_owned_lowers,
        output_dir,
        runtime_owned_roots,
    )
    .await?;
    if let Some(layer) = appended_layer {
        lowers.push(layer);
    }
    Ok((lowers, false))
}

async fn adopt_runtime_owned_lowers(
    mut lowers: Vec<LayerConfig>,
    mut runtime_owned_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    runtime_owned_roots: &[PathBuf],
) -> Result<Vec<LayerConfig>> {
    let inherited_layers_dir = output_dir.join(INHERITED_LAYERS_DIR);
    for (index, lower) in runtime_owned_lowers.iter_mut().enumerate() {
        let layer_dir = inherited_layers_dir.join(format!("{index:04}"));
        if let Some(source) = resolve_local_layer_path(lower) {
            let resolved = fs::canonicalize(&source).unwrap_or_else(|_| source.clone());
            if path_is_runtime_owned(&resolved, runtime_owned_roots) {
                let destination = layer_dir.join(
                    resolved
                        .file_name()
                        .unwrap_or_else(|| OsStr::new("runtime-layer.commit")),
                );
                link_or_copy_runtime_layer(&resolved, &destination).await?;
                lower.file = destination.display().to_string();
                lower.dir.clear();
            } else if path_is_runtime_owned(&source, runtime_owned_roots) {
                lower.file = resolved.display().to_string();
                lower.dir.clear();
            }
        } else if !lower.dir.is_empty() {
            let path = Path::new(&lower.dir);
            let resolved = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
            if path_is_runtime_owned(path, runtime_owned_roots)
                || path_is_runtime_owned(&resolved, runtime_owned_roots)
            {
                anyhow::bail!(
                    "runtime-owned overlaybd lower dir has no commit or sealed artifact: {}",
                    lower.dir
                );
            }
        }
    }
    lowers.extend(runtime_owned_lowers);
    Ok(lowers)
}

/// Stage a persisted snapshot config from the current live overlaybd runtime.
///
/// Writable runtimes are first restacked in-place so the sealed old upper
/// becomes the newest lower. Read-only runtimes skip the restack phase and
/// only stage the persisted snapshot image config.
pub(super) async fn restack_snapshot_overlaybd_device(
    runtime: &OverlaybdRuntimeHandle,
    read_only: bool,
    output_dir: &Path,
    kind: &'static str,
    owned_roots: &[PathBuf],
) -> Result<PathBuf> {
    let live_runtime_image_config_path = &runtime.image_config_path;
    let mut image = load_image_config(live_runtime_image_config_path)?;
    if !read_only {
        ensure!(
            !image.upper.data.is_empty(),
            "restack requires a writable upper"
        );
        let upper = Path::new(&image.upper.data);
        let layer_dir = upper
            .parent()
            .context("runtime upper has no parent directory")?
            .join("runtime-layers")
            .join(Uuid::now_v7().to_string());
        tokio::fs::create_dir_all(&layer_dir).await?;
        let layer = layer_dir.join("snapshot.commit");
        let roots =
            runtime_owned_roots_for_live_config(owned_roots, live_runtime_image_config_path);
        let base_layers = image
            .lowers
            .iter()
            .position(|lower| layer_references_runtime_owned_artifact(lower, &roots))
            .unwrap_or(image.lowers.len());
        let runtime_layers = image.lowers.len() - base_layers;
        let compact_from = (runtime_layers >= 2
            && (runtime_layers + 1 > runtime_layer_budget(base_layers)
                || image.lowers.len() + 1 > overlaybd::index_file::MAX_STACK_LAYERS))
            .then_some(base_layers);
        let lowers = match UblkDeviceManager::global()
            .restack_snapshot_device(&runtime.device, &layer, compact_from, kind)
            .await
        {
            Ok(lowers) => lowers,
            Err(error) => {
                // Only remove an empty directory; a terminal restack may own files here.
                let _ = tokio::fs::remove_dir(&layer_dir).await;
                return Err(error);
            }
        };
        record_sealed_upper_size(kind, &layer, compact_from.is_some()).await;
        image.lowers = lowers;
    }

    stage_overlaybd_snapshot(
        image,
        output_dir,
        &runtime_owned_roots_for_live_config(owned_roots, live_runtime_image_config_path),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlaybd::backend::local::LocalFile;
    use overlaybd::virtual_file::VirtualFile;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;

    use serde_json::json;

    #[test]
    fn split_runtime_suffix_preserves_stable_lowers() {
        let temp = tempfile::tempdir().unwrap();
        let snapshot_store_root = temp.path().join("snapshots");
        let persisted_sandbox_store_root = temp.path().join("persisted-sandboxes");
        let managed_base = temp.path().join("managed-snapshots");
        let snapshot_store_lower = snapshot_store_root
            .join("managed-layers")
            .join("sha256_base.overlaybd.commit");
        let persisted_store_lower = persisted_sandbox_store_root
            .join("artifacts")
            .join("sandbox-parent")
            .join("1700000000000")
            .join("rootfs")
            .join("snapshot.commit");
        let managed_base_lower = managed_base.join("sandbox/1700000000001/mem.commit");
        let lowers = vec![
            LayerConfig {
                file: String::new(),
                digest: "sha256:remote".to_string(),
                size: 42,
                ..Default::default()
            },
            local_layer_config(&snapshot_store_lower),
            local_layer_config(&persisted_store_lower),
            local_layer_config(&managed_base_lower),
        ];

        let (preserved, managed) =
            split_runtime_suffix(lowers, &[persisted_sandbox_store_root, managed_base]);

        assert_eq!(preserved.len(), 2);
        assert_eq!(preserved[0].file, "");
        assert_eq!(preserved[0].digest, "sha256:remote");
        assert_eq!(PathBuf::from(&preserved[1].file), snapshot_store_lower);
        assert_eq!(managed.len(), 2);
        assert_eq!(PathBuf::from(&managed[0].file), persisted_store_lower);
        assert_eq!(PathBuf::from(&managed[1].file), managed_base_lower);
    }

    #[tokio::test]
    async fn snapshot_adopts_persisted_runtime_suffix_by_link() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifacts_root = temp.path().join("persisted-sandboxes").join("artifacts");
        let output_dir = artifacts_root.join("sandbox-c").join("gen1").join("rootfs");
        let parent_root = artifacts_root.join("sandbox-b").join("gen1").join("rootfs");
        let cache_root = temp.path().join("image-cache").join("commits");
        std::fs::create_dir_all(&parent_root).expect("create parent rootfs");
        std::fs::create_dir_all(&cache_root).expect("create cache root");

        let parent_base = parent_root.join("managed-base.commit");
        let parent_snapshot = parent_root.join("snapshot.commit");
        let cache_layer = cache_root.join("sha256-base").join("overlaybd.commit");
        std::fs::create_dir_all(cache_layer.parent().unwrap()).expect("create cache layer dir");
        std::fs::write(&parent_base, b"parent base").expect("write parent base");
        std::fs::write(&parent_snapshot, b"parent snapshot").expect("write parent snapshot");
        std::fs::write(&cache_layer, b"cache base").expect("write cache layer");

        let lowers = vec![
            LayerConfig {
                file: cache_layer.display().to_string(),
                digest: "sha256:base".to_string(),
                size: 10,
                ..Default::default()
            },
            local_layer_config(&parent_base),
            local_layer_config(&parent_snapshot),
        ];
        let runtime_owned_roots = [artifacts_root.canonicalize().unwrap()];

        let (rewritten, _) = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
        )
        .await
        .expect("rewrite inherited runtime layers");

        assert_eq!(rewritten.len(), 3);
        assert_eq!(PathBuf::from(&rewritten[0].file), cache_layer);
        assert_eq!(rewritten[0].digest, "sha256:base");
        assert_eq!(rewritten[0].size, 10);

        for (index, (rewritten_lower, source)) in rewritten[1..]
            .iter()
            .zip([parent_base, parent_snapshot])
            .enumerate()
        {
            let adopted = PathBuf::from(&rewritten_lower.file);
            let expected_parent = output_dir
                .join(INHERITED_LAYERS_DIR)
                .join(format!("{index:04}"));
            assert!(adopted.starts_with(expected_parent));
            assert!(!adopted.starts_with(&parent_root));
            assert_eq!(adopted.file_name(), source.file_name());
            assert!(rewritten_lower.digest.is_empty());
            assert_eq!(rewritten_lower.size, 0);

            let source_metadata = std::fs::metadata(&source).expect("source metadata");
            let adopted_metadata = std::fs::metadata(&adopted).expect("adopted metadata");
            assert_eq!(source_metadata.dev(), adopted_metadata.dev());
            assert_eq!(source_metadata.ino(), adopted_metadata.ino());
        }
    }

    #[tokio::test]
    async fn staged_runtime_lowers_survive_removal_of_a_symlinked_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_dir = temp.path().join("runtime");
        let runtime_alias = temp.path().join("runtime-alias");
        let output_dir = temp.path().join("generation");
        let stable_lower = temp.path().join("base.commit");
        fs::write(&stable_lower, b"base").unwrap();
        fs::create_dir_all(runtime_dir.join("runtime-layers")).unwrap();
        std::os::unix::fs::symlink(&runtime_dir, &runtime_alias).unwrap();
        let layer = runtime_alias.join("runtime-layers/layer.commit");
        fs::write(&layer, b"snapshot data").unwrap();
        let config_path = runtime_alias.join("image.json");
        fs::write(
            &config_path,
            serde_json::to_vec(&ImageConfig {
                lowers: vec![
                    local_layer_config(&stable_lower),
                    LayerConfig {
                        digest: "sha256:continued".into(),
                        size: 13,
                        ..local_layer_config(&layer)
                    },
                ],
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();

        let staged = stage_overlaybd_snapshot(
            load_image_config(&config_path).unwrap(),
            &output_dir,
            &runtime_owned_roots_for_live_config(&[], &config_path),
        )
        .await
        .unwrap();
        let image = overlaybd::config::load_image_config(staged).unwrap();
        assert_eq!(Path::new(&image.lowers[0].file), stable_lower);
        assert_eq!(image.lowers[1].digest, "sha256:continued");
        assert_eq!(image.lowers[1].size, 13);
        let owned_layer = Path::new(&image.lowers[1].file);
        assert!(owned_layer.starts_with(&output_dir));
        fs::remove_dir_all(runtime_dir).unwrap();
        assert_eq!(fs::read(owned_layer).unwrap(), b"snapshot data");
    }

    #[tokio::test]
    async fn stage_live_runtime_preserves_restacked_layer_descriptor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let live_dir = temp.path().join("live");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&live_dir).expect("create live dir");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        let snapshot_lower = live_dir.join("snapshot.commit");
        std::fs::write(&snapshot_lower, b"snapshot").expect("write snapshot lower");
        let live_runtime_image_config_path = live_dir.join("image.json");
        std::fs::write(
            &live_runtime_image_config_path,
            serde_json::to_vec_pretty(&json!({
                "lowers": [
                    {
                        "file": snapshot_lower,
                        "digest": "sha256:descriptor",
                        "size": 8
                    }
                ],
                "upper": {},
                "resultFile": live_dir.join("result.txt"),
                "download": {}
            }))
            .expect("serialize live image config"),
        )
        .expect("write live image config");

        let output_path = stage_overlaybd_snapshot(
            load_image_config(&live_runtime_image_config_path).unwrap(),
            &output_dir,
            &runtime_owned_roots_for_live_config(&[], &live_runtime_image_config_path),
        )
        .await
        .expect("stage snapshot");

        let staged =
            overlaybd::config::load_image_config(&output_path).expect("load staged image config");
        let latest = staged.lowers.last().expect("latest lower");
        assert!(Path::new(&latest.file).starts_with(&output_dir));
        assert_eq!(
            fs::read(&latest.file).unwrap(),
            fs::read(snapshot_lower).unwrap()
        );
        assert_eq!(latest.digest, "sha256:descriptor");
        assert_eq!(latest.size, 8);
    }

    #[tokio::test]
    async fn layer_rewrite_keeps_remote_lowers_as_references() {
        let temp = tempfile::tempdir().expect("tempdir");
        let parent_config_path = temp.path().join("parent-mem-image.json");
        let repo_blob_url = "s3://agentenv-oss-validation/validation/managed-layers";
        let remote_lowers: Vec<_> = (0..RUNTIME_LAYER_BUDGET_BASE)
            .map(|index| {
                json!({
                    "digest": format!("sha256:parent-{index}"),
                    "size": 4096
                })
            })
            .collect();
        std::fs::write(
            &parent_config_path,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": repo_blob_url,
                "lowers": remote_lowers,
                "upper": {},
                "resultFile": "",
                "download": {}
            }))
            .expect("serialize parent image config"),
        )
        .expect("write parent image config");

        let new_layer = temp.path().join("mem_overlaybd").join("overlaybd.commit");
        std::fs::create_dir_all(new_layer.parent().unwrap()).expect("create mem layer dir");
        std::fs::write(&new_layer, b"memory delta").expect("write mem layer");
        let mut image_config = load_image_config(&parent_config_path).unwrap();
        image_config.lowers = rewrite_lowers_with_runtime_roots(
            image_config.lowers,
            temp.path(),
            Some(local_layer_config(&new_layer)),
            "mem_compacted.commit",
            &[],
            OverlaybdCompactOutput::Raw,
        )
        .await
        .expect("rewrite memory layers")
        .0;

        assert_eq!(image_config.repo_blob_url, repo_blob_url);
        assert_eq!(image_config.lowers.len(), RUNTIME_LAYER_BUDGET_BASE + 1);
        assert_eq!(image_config.lowers[0].digest, "sha256:parent-0");
        assert!(image_config.lowers[..RUNTIME_LAYER_BUDGET_BASE]
            .iter()
            .all(|lower| lower.file.is_empty()));
        let latest = image_config.lowers.last().expect("latest mem lower");
        assert_eq!(PathBuf::from(&latest.file), new_layer);
        assert!(latest.digest.is_empty());
        assert_eq!(latest.size, 0);
    }
    #[tokio::test]
    async fn rewrite_deep_stable_prefix_adopts_without_compaction() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        // A stable prefix deeper than DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS must
        // not trigger compaction on its own: only the runtime-owned suffix counts
        // toward the budget (32 - 33/4 = 24 here, and the suffix is just 2).
        let mut lowers: Vec<LayerConfig> = (0..33)
            .map(|index| LayerConfig {
                file: String::new(),
                digest: format!("sha256:base-{index}"),
                size: 4096,
                ..Default::default()
            })
            .collect();
        let runtime_a = runtime_root.join("a.commit");
        let runtime_b = runtime_root.join("b.commit");
        std::fs::write(&runtime_a, b"runtime a").expect("write runtime layer a");
        std::fs::write(&runtime_b, b"runtime b").expect("write runtime layer b");
        lowers.push(local_layer_config(&runtime_a));
        lowers.push(local_layer_config(&runtime_b));
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let (rewritten, _) = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
        )
        .await
        .expect("rewrite with deep stable prefix");

        assert_eq!(rewritten.len(), 35);
        assert!(rewritten[..33].iter().all(|lower| lower.file.is_empty()));
        assert_eq!(rewritten[0].digest, "sha256:base-0");
        for (index, rewritten_lower) in rewritten[33..].iter().enumerate() {
            let adopted = PathBuf::from(&rewritten_lower.file);
            let expected_parent = output_dir
                .join(INHERITED_LAYERS_DIR)
                .join(format!("{index:04}"));
            assert!(adopted.starts_with(expected_parent));
        }
        assert!(!output_dir.join(MANAGED_BASE_LAYER_FILE).exists());
    }

    /// Seal a tiny raw overlaybd commit for compaction tests.
    async fn seal_raw_test_layer(
        dir: &Path,
        name: &str,
        vsize: u64,
        writes: &[(u64, u8)],
    ) -> PathBuf {
        let data = Arc::new(
            LocalFile::new(dir.join(format!("{name}.data"))).expect("create layer data file"),
        );
        let index = Arc::new(
            LocalFile::new(dir.join(format!("{name}.index"))).expect("create layer index file"),
        );
        let layer = overlaybd::index_file::LSMTFile::create(data, Some(index), vsize, false)
            .await
            .expect("create layer");
        for (offset, byte) in writes {
            layer
                .write_at(*offset, &[*byte; 4096])
                .await
                .expect("write layer page");
        }
        let commit_path = dir.join(format!("{name}.commit"));
        let output: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&commit_path).expect("create layer commit output"));
        layer
            .commit_with_args(overlaybd::index_file::CommitArgs::new(output))
            .await
            .expect("commit raw layer");
        commit_path
    }

    #[tokio::test]
    async fn rewrite_shrinking_budget_compacts_runtime_suffix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        // A 124-layer stable prefix shrinks the budget to max(32 - 124/4, 1) = 1,
        // so a two-layer runtime-owned suffix already triggers compaction.
        let mut lowers: Vec<LayerConfig> = (0..124)
            .map(|index| LayerConfig {
                file: String::new(),
                digest: format!("sha256:base-{index}"),
                size: 4096,
                ..Default::default()
            })
            .collect();
        let layer_a = seal_raw_test_layer(&runtime_root, "a", 4096, &[(0, 0xAA)]).await;
        let layer_b = seal_raw_test_layer(&runtime_root, "b", 4096, &[(0, 0xBB)]).await;
        lowers.push(local_layer_config(&layer_a));
        lowers.push(local_layer_config(&layer_b));
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let (rewritten, _) = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
        )
        .await
        .expect("rewrite with shrinking budget");

        assert_eq!(rewritten.len(), 125);
        assert!(rewritten[..124].iter().all(|lower| lower.file.is_empty()));
        let compacted = PathBuf::from(&rewritten[124].file);
        assert_eq!(compacted, output_dir.join(MANAGED_BASE_LAYER_FILE));
        assert!(compacted.exists());
        assert!(!compacted.with_extension("commit.tmp").exists());
    }
}
