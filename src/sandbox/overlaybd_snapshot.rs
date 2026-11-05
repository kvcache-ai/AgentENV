//! Backend-neutral OverlayBD snapshot helpers.
//!
//! Organizes independent snapshot layers and preserves their local files.
//! The caller supplies the directories whose artifacts need to be retained.
//!
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use overlaybd::config::OverlaybdCompactOutput;
use overlaybd::layer::LayerFile;
use tempfile::NamedTempFile;
use tracing::warn;

use crate::cfg::ConfigManager;
use crate::layer::{LayerChain, LayerRef, LayerSource};
use crate::sandbox::ublk::{OverlaybdRuntimeHandle, UblkDeviceManager};
use crate::sandbox::SandboxCaptureError;
use crate::snapshot::SnapshotRootfsArtifacts;

/// Base budget for runtime-owned lowers, reduced by one per four stable lowers.
/// This is a compaction threshold, not a limit on the total lower count.
const RUNTIME_LAYER_BUDGET_BASE: usize = 32;
const INHERITED_LAYERS_DIR: &str = "inherited-layers";
const MANAGED_BASE_LAYER_FILE: &str = "managed-base.commit";

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

fn path_is_runtime_owned(path: &Path, runtime_owned_roots: &[PathBuf]) -> bool {
    runtime_owned_roots
        .iter()
        .any(|root| path.starts_with(root))
}

fn layer_references_runtime_owned_artifact(lower: &LayerSource, roots: &[PathBuf]) -> bool {
    lower.path().is_some_and(|path| {
        let resolved = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        path_is_runtime_owned(path, roots) || path_is_runtime_owned(&resolved, roots)
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
    image: &impl serde::Serialize,
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

/// Only the suffix after the last ineligible layer may be compacted; preserve the prefix.
fn compactable_suffix_start(chain: &LayerChain, roots: &[PathBuf]) -> usize {
    chain
        .layers
        .iter()
        .rposition(|layer| {
            let managed = chain.managed_repo_blob_url.is_some()
                && matches!(
                    layer,
                    LayerSource::Referenced {
                        reference: LayerRef::Managed(_),
                        ..
                    }
                );
            !layer_references_runtime_owned_artifact(layer, roots) && !managed
        })
        .map_or(0, |index| index + 1)
}

/// Select an old lower suffix, optionally including the planned `appended_layer` at its end.
/// The appended file is only needed when the selected range includes it.
/// A live upper only reserves an output slot; restack seals it after merging old lowers.
/// `None` leaves capacity validation to capture, or to restack after checking for an empty upper.
pub(super) fn snapshot_compaction_range(
    chain: &LayerChain,
    runtime_owned_roots: &[PathBuf],
    appended_layer: Option<&LayerFile>,
    reserve_upper: bool,
) -> Option<Range<usize>> {
    let limit = overlaybd::index_file::MAX_STACK_LAYERS;
    let appended_count = usize::from(appended_layer.is_some());
    let reserved_count = usize::from(reserve_upper);
    let runtime_start = chain
        .layers
        .iter()
        .position(|layer| layer_references_runtime_owned_artifact(layer, runtime_owned_roots))
        .unwrap_or(chain.layers.len());
    let runtime_end = chain.layers.len() + appended_count;
    let runtime_count = runtime_end - runtime_start;
    let exceeds_limit = runtime_end + reserved_count > limit;
    let runtime_budget = RUNTIME_LAYER_BUDGET_BASE
        .saturating_sub(runtime_start / 4)
        .max(1);
    if runtime_count + reserved_count <= runtime_budget && !exceeds_limit {
        return None;
    }
    if (2..=limit).contains(&runtime_count) && runtime_start + 1 + reserved_count <= limit {
        return Some(runtime_start..runtime_end);
    }
    if !exceeds_limit {
        return None;
    }

    let start = compactable_suffix_start(chain, runtime_owned_roots);
    // Keep the new delta separate: merging it with a full old stack would open 256 inputs.
    ((2..=limit).contains(&(chain.layers.len() - start))
        && start + 1 + appended_count + reserved_count <= limit)
        .then_some(start..chain.layers.len())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn rewrite_lowers_with_runtime_roots(
    chain: &LayerChain,
    overlaybd_global_config_path: &Path,
    output_dir: &Path,
    appended_layer: Option<LayerFile>,
    compaction_output_name: &'static str,
    runtime_owned_roots: &[PathBuf],
    compaction_output: OverlaybdCompactOutput,
    compute_digest: bool,
) -> Result<(LayerChain, bool)> {
    let range =
        snapshot_compaction_range(chain, runtime_owned_roots, appended_layer.as_ref(), false);
    let compacted = range.is_some();
    let mut retained = chain.clone();
    let mut new_layers: Vec<_> = appended_layer.into_iter().map(LayerSource::File).collect();
    if let Some(range) = range {
        let mut selected = retained.layers.split_off(range.start);
        if range.end > chain.layers.len() {
            selected.append(&mut new_layers);
        }
        let image = LayerChain {
            layers: selected,
            managed_repo_blob_url: chain.managed_repo_blob_url.clone(),
        }
        .to_runtime_config();
        let layer = UblkDeviceManager::global()
            .compact_layers(
                &image,
                overlaybd_global_config_path,
                &output_dir.join(compaction_output_name),
                compaction_output,
                compute_digest,
            )
            .await?;
        new_layers.insert(0, LayerSource::File(layer));
    }
    ensure!(retained.layers.len() + new_layers.len() <= overlaybd::index_file::MAX_STACK_LAYERS,
        "snapshot would exceed lower layer limit: no mergeable history suffix (layer_count={}, appended={}, limit={})",
        chain.layers.len(), new_layers.len(), overlaybd::index_file::MAX_STACK_LAYERS);
    adopt_runtime_owned_layers(&mut retained, output_dir, runtime_owned_roots).await?;
    retained.layers.extend(new_layers);
    Ok((retained, compacted))
}

pub(super) async fn adopt_runtime_owned_layers(
    chain: &mut LayerChain,
    output_dir: &Path,
    roots: &[PathBuf],
) -> Result<()> {
    let output_root = fs::canonicalize(output_dir).unwrap_or_else(|_| output_dir.to_path_buf());
    for (index, layer) in chain.layers.iter_mut().enumerate() {
        let Some(source) = layer.path() else {
            continue;
        };
        let resolved = fs::canonicalize(source).unwrap_or_else(|_| source.to_path_buf());
        if resolved.starts_with(&output_root) {
            continue;
        }
        if path_is_runtime_owned(&resolved, roots) {
            let destination = output_dir
                .join(INHERITED_LAYERS_DIR)
                .join(format!("{index:04}"))
                .join(
                    resolved
                        .file_name()
                        .unwrap_or_else(|| OsStr::new("runtime-layer.commit")),
                );
            link_or_copy_runtime_layer(&resolved, &destination).await?;
            layer.relocate_file(destination);
        } else if path_is_runtime_owned(source, roots) {
            layer.relocate_file(resolved);
        }
    }
    Ok(())
}

/// Restack the live device and assemble the independent snapshot's layer chain.
/// Completion retains files before deriving the snapshot's reopen configuration.
pub(super) async fn capture_disk_snapshot(
    runtime: &mut OverlaybdRuntimeHandle,
    output_dir: &Path,
    kind: &'static str,
    owned_roots: &[PathBuf],
    compute_digest: bool,
) -> Result<SnapshotRootfsArtifacts> {
    let layers = &mut runtime.layers;
    let live_runtime_image_config_path = &runtime.image_config_path;
    let roots = runtime_owned_roots_for_live_config(owned_roots, live_runtime_image_config_path);
    let compact_from =
        snapshot_compaction_range(layers, &roots, None, true).map(|range| range.start);
    let result = UblkDeviceManager::global()
        .snapshot_device(&runtime.device, compact_from, kind, compute_digest)
        .await?;
    if let Some(restack) = result.restack {
        let compacted = restack.compacted_lowers.is_some();
        if let Some((range, layer)) = restack.compacted_lowers {
            if range.start >= range.end || range.end > layers.layers.len() {
                return Err(SandboxCaptureError::terminal(anyhow::anyhow!(
                    "invalid runtime layer replacement range"
                ))
                .into());
            }
            layers.layers.splice(range, [LayerSource::File(layer)]);
        }
        let sealed_path = restack.sealed_upper.path.clone();
        layers.layers.push(LayerSource::File(restack.sealed_upper));
        record_sealed_upper_size(kind, &sealed_path, compacted).await;
    }
    tokio::fs::create_dir_all(output_dir).await?;
    let (layers, _) = rewrite_lowers_with_runtime_roots(
        layers,
        &ConfigManager::global_config()
            .ublk
            .overlaybd
            .global_config_path,
        output_dir,
        None,
        MANAGED_BASE_LAYER_FILE,
        &roots,
        OverlaybdCompactOutput::Raw,
        compute_digest,
    )
    .await?;
    Ok(SnapshotRootfsArtifacts {
        image_config_path: output_dir.join("image.json"),
        layers,
        virtual_size: runtime.actual_virtual_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::ManagedLayerRef;
    use overlaybd::backend::local::LocalFile;
    use overlaybd::layer::LayerFile;
    use overlaybd::virtual_file::VirtualFile;
    use overlaybd::LayerDescriptor;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;
    use uuid::Uuid;

    fn local(path: &Path, descriptor: Option<LayerDescriptor>) -> LayerSource {
        LayerSource::File(LayerFile::dense(
            path.to_path_buf(),
            Uuid::nil(),
            descriptor,
        ))
    }

    #[test]
    fn compactable_suffix_preserves_stable_lowers() {
        let root = tempfile::tempdir().unwrap();
        let runtime = root.path().join("runtime");
        let mut chain = LayerChain {
            layers: vec![
                LayerSource::Referenced {
                    reference: LayerRef::External(crate::layer::ExternalLayer {
                        digest: "sha256:remote".into(),
                        size: 42,
                        repo_blob_url: "https://registry/blobs".into(),
                    }),
                    local_file: None,
                },
                LayerSource::Referenced {
                    reference: LayerRef::Managed(ManagedLayerRef {
                        digest: "sha256:stored".into(),
                        size: 42,
                        uuid: None,
                    }),
                    local_file: Some(root.path().join("stored.commit")),
                },
                local(&runtime.join("first.commit"), None),
                local(&runtime.join("second.commit"), None),
            ],
            managed_repo_blob_url: Some("s3://bucket/layers".into()),
        };
        assert_eq!(
            compactable_suffix_start(&chain, std::slice::from_ref(&runtime)),
            1
        );
        chain.managed_repo_blob_url = None;
        assert_eq!(compactable_suffix_start(&chain, &[runtime]), 2);
    }

    #[tokio::test]
    async fn snapshot_adopts_persisted_runtime_suffix_by_link() {
        let temp = tempfile::tempdir().unwrap();
        let sandbox = temp.path().join("sandboxes");
        let parent = sandbox.join("parent");
        fs::create_dir_all(&parent).unwrap();
        let cached = temp.path().join("cache.commit");
        let first = parent.join("base.commit");
        let second = parent.join("snapshot.commit");
        for path in [&cached, &first, &second] {
            fs::write(path, b"layer data").unwrap();
        }
        let chain = LayerChain::from(vec![
            local(
                &cached,
                Some(LayerDescriptor {
                    digest: "sha256:base".into(),
                    size: 10,
                }),
            ),
            local(&first, None),
            local(&second, None),
        ]);
        let output = temp.path().join("capture");
        let (rewritten, _) = rewrite_lowers_with_runtime_roots(
            &chain,
            Path::new("unused"),
            &output,
            None,
            MANAGED_BASE_LAYER_FILE,
            &[sandbox],
            OverlaybdCompactOutput::Raw,
            false,
        )
        .await
        .unwrap();
        assert_eq!(rewritten.layers.len(), 3);
        assert_eq!(rewritten.layers[0], chain.layers[0]);
        for (retained, source) in rewritten.layers[1..].iter().zip([first, second]) {
            let path = retained.path().unwrap();
            assert!(path.starts_with(&output));
            assert_eq!(path.file_name(), source.file_name());
            let before = fs::metadata(source).unwrap();
            let after = fs::metadata(path).unwrap();
            assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
        }
    }

    #[tokio::test]
    async fn staged_runtime_lowers_survive_removal_of_a_symlinked_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let runtime = temp.path().join("runtime");
        let alias = temp.path().join("runtime-alias");
        fs::create_dir(&runtime).unwrap();
        std::os::unix::fs::symlink(&runtime, &alias).unwrap();
        let stable = temp.path().join("stable.commit");
        let source = alias.join("layer.commit");
        fs::write(&stable, b"base").unwrap();
        fs::write(&source, b"snapshot data").unwrap();
        let mut chain = LayerChain::from(vec![
            local(&stable, None),
            local(
                &source,
                Some(LayerDescriptor {
                    digest: "sha256:continued".into(),
                    size: 13,
                }),
            ),
        ]);
        let output = temp.path().join("capture");
        adopt_runtime_owned_layers(
            &mut chain,
            &output,
            &runtime_owned_roots_for_live_config(&[], &alias.join("image.json")),
        )
        .await
        .unwrap();
        let image = chain.to_runtime_config();
        assert_eq!(Path::new(&image.lowers[0].file), stable);
        assert_eq!(image.lowers[1].digest, "sha256:continued");
        assert_eq!(image.lowers[1].size, 13);
        let retained = Path::new(&image.lowers[1].file);
        assert!(retained.starts_with(output));
        fs::remove_dir_all(runtime).unwrap();
        assert_eq!(fs::read(retained).unwrap(), b"snapshot data");
    }

    #[tokio::test]
    async fn stage_live_runtime_preserves_restacked_layer_descriptor() {
        let temp = tempfile::tempdir().unwrap();
        let runtime = temp.path().join("runtime");
        fs::create_dir(&runtime).unwrap();
        let source = runtime.join("snapshot.commit");
        fs::write(&source, b"snapshot").unwrap();
        let chain = LayerChain::from(vec![local(
            &source,
            Some(LayerDescriptor {
                digest: "sha256:descriptor".into(),
                size: 8,
            }),
        )]);
        let output = temp.path().join("out");
        let (staged, _) = rewrite_lowers_with_runtime_roots(
            &chain,
            Path::new("unused"),
            &output,
            None,
            MANAGED_BASE_LAYER_FILE,
            &[runtime],
            OverlaybdCompactOutput::Raw,
            false,
        )
        .await
        .unwrap();
        let image = staged.to_runtime_config();
        let latest = image.lowers.last().unwrap();
        assert!(Path::new(&latest.file).starts_with(output));
        assert_eq!(fs::read(&latest.file).unwrap(), fs::read(source).unwrap());
        assert_eq!(latest.digest, "sha256:descriptor");
        assert_eq!(latest.size, 8);
    }

    #[tokio::test]
    async fn layer_rewrite_keeps_remote_lowers_as_references() {
        let temp = tempfile::tempdir().unwrap();
        let repo = "s3://agentenv-oss-validation/validation/managed-layers";
        let chain = LayerChain::from(
            (0..RUNTIME_LAYER_BUDGET_BASE)
                .map(|index| LayerSource::Referenced {
                    reference: LayerRef::External(crate::layer::ExternalLayer {
                        digest: format!("sha256:parent-{index}"),
                        size: 4096,
                        repo_blob_url: repo.into(),
                    }),
                    local_file: None,
                })
                .collect::<Vec<_>>(),
        );
        let path = temp.path().join("delta.commit");
        fs::write(&path, b"memory delta").unwrap();
        let (result, _) = rewrite_lowers_with_runtime_roots(
            &chain,
            Path::new("unused"),
            temp.path(),
            Some(LayerFile::dense(path.clone(), Uuid::nil(), None)),
            "compacted.commit",
            &[],
            OverlaybdCompactOutput::Raw,
            false,
        )
        .await
        .unwrap();
        assert_eq!(result.layers[..RUNTIME_LAYER_BUDGET_BASE], chain.layers);
        assert_eq!(result.layers.len(), RUNTIME_LAYER_BUDGET_BASE + 1);
        assert_eq!(result.layers.last().unwrap().path(), Some(path.as_path()));
        let image = result.to_runtime_config();
        assert_eq!(image.repo_blob_url, repo);
        assert!(image.lowers[..RUNTIME_LAYER_BUDGET_BASE]
            .iter()
            .all(|layer| layer.file.is_empty()));
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
        let mut lowers: Vec<LayerSource> = (0..33)
            .map(|index| LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    repo_blob_url: "https://registry.example/v2/image/blobs".into(),
                    digest: format!("sha256:base-{index}"),
                    size: 4096,
                }),
                local_file: None,
            })
            .collect();
        let runtime_a = runtime_root.join("a.commit");
        let runtime_b = runtime_root.join("b.commit");
        std::fs::write(&runtime_a, b"runtime a").expect("write runtime layer a");
        std::fs::write(&runtime_b, b"runtime b").expect("write runtime layer b");
        lowers.push(local(&runtime_a, None));
        lowers.push(local(&runtime_b, None));
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let chain = LayerChain::from(lowers);
        let (rewritten, _) = rewrite_lowers_with_runtime_roots(
            &chain,
            &ConfigManager::global_config()
                .ublk
                .overlaybd
                .global_config_path,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            false,
        )
        .await
        .expect("rewrite with deep stable prefix");

        let rewritten = rewritten.to_runtime_config().lowers;
        assert_eq!(rewritten.len(), 35);
        assert!(rewritten[..33].iter().all(|lower| lower.file.is_empty()));
        assert_eq!(rewritten[0].digest, "sha256:base-0");
        for rewritten_lower in &rewritten[33..] {
            let adopted = PathBuf::from(&rewritten_lower.file);
            assert!(adopted.starts_with(&output_dir));
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
    #[ignore = "requires the ublk daemon and CAP_SYS_ADMIN; run by make test-unit capability group"]
    async fn rewrite_shrinking_budget_compacts_runtime_suffix() {
        UblkDeviceManager::init_global_from_config(ConfigManager::global_config())
            .await
            .expect("initialize compaction daemon");
        assert!(UblkDeviceManager::global().is_available());
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        // A 124-layer stable prefix shrinks the budget to max(32 - 124/4, 1) = 1,
        // so a two-layer runtime-owned suffix already triggers compaction.
        let mut lowers: Vec<LayerSource> = (0..124)
            .map(|index| LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    repo_blob_url: "https://registry.example/v2/image/blobs".into(),
                    digest: format!("sha256:base-{index}"),
                    size: 4096,
                }),
                local_file: None,
            })
            .collect();
        let layer_a = seal_raw_test_layer(&runtime_root, "a", 4096, &[(0, 0xAA)]).await;
        let layer_b = seal_raw_test_layer(&runtime_root, "b", 4096, &[(0, 0xBB)]).await;
        lowers.push(local(&layer_a, None));
        lowers.push(local(&layer_b, None));
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let chain = LayerChain::from(lowers);
        let (rewritten, _) = rewrite_lowers_with_runtime_roots(
            &chain,
            &ConfigManager::global_config()
                .ublk
                .overlaybd
                .global_config_path,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            false,
        )
        .await
        .expect("rewrite with shrinking budget");

        let rewritten = rewritten.to_runtime_config().lowers;
        assert_eq!(rewritten.len(), 125);
        assert!(rewritten[..124].iter().all(|lower| lower.file.is_empty()));
        let compacted = PathBuf::from(&rewritten[124].file);
        assert_eq!(compacted, output_dir.join(MANAGED_BASE_LAYER_FILE));
        assert!(compacted.exists());
        assert!(!compacted.with_extension("commit.tmp").exists());
        UblkDeviceManager::global()
            .shutdown_daemon()
            .await
            .expect("stop compaction daemon");
    }
}
