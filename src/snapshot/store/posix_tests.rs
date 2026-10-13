//! `store_snapshot` against a POSIX repository, which links local files in
//! place where it can.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;

use overlaybd::backend::local::LocalFile;
use overlaybd::index_file::{CommitArgs, LSMTFile};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::{is_zfile, CompressArgs, CompressOptions, ZFileCompactWriter};
use tempfile::TempDir;

use crate::digest::FileDigest;
use crate::repository::backends::PosixFsRepository;
use crate::repository::{BlobKey, LayerRole};
use crate::snapshot::mock::write_mock_built_artifacts;
use crate::snapshot::store::store_snapshot;
use crate::snapshot::{
    CommittedAttachedDrive, SnapshotId, SnapshotPublishMetadata, SNAPSHOT_ARTIFACT_LAYOUT,
};

fn test_repository(root: &std::path::Path) -> PosixFsRepository {
    PosixFsRepository::new(root.to_path_buf())
}

fn metadata(id: &SnapshotId) -> SnapshotPublishMetadata {
    SnapshotPublishMetadata {
        id: id.clone(),
        ..SnapshotPublishMetadata::mock()
    }
}

/// Where a POSIX repository stores a layer, relative to its root.
fn layer_file(digest: &str) -> PathBuf {
    PathBuf::from(
        BlobKey::Layer {
            digest: digest.to_owned(),
            role: LayerRole::Rootfs,
        }
        .posix_path(),
    )
}

fn assert_same_inode(left: &std::path::Path, right: &std::path::Path) {
    let left_metadata = fs::metadata(left).expect("left metadata");
    let right_metadata = fs::metadata(right).expect("right metadata");
    assert_eq!(left_metadata.dev(), right_metadata.dev());
    assert_eq!(left_metadata.ino(), right_metadata.ino());
}

#[tokio::test]
async fn attached_drive_rootfs_layers_are_derived_from_resolved_lower_paths() {
    let tempdir = TempDir::new().expect("tempdir");
    let repository = test_repository(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    let local_dir = tempdir.path().join("local");
    let (_, _, mut manifest) =
        write_mock_built_artifacts(local_dir.as_path()).expect("mock built artifacts should write");

    // Create drive directory structure: local_dir/drives/data/
    let drive_dir = local_dir.join("drives").join("data");
    fs::create_dir_all(&drive_dir).expect("create drive dir");

    // Create layers dir through the layout API used by drive image configs.
    let layers_dir = local_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.drive_layers_dir);
    fs::create_dir_all(&layers_dir).expect("create layers dir");
    let drive_layer = layers_dir.join("drive.commit");
    fs::write(&drive_layer, b"drive-layer").expect("drive lower");
    let drive_descriptor =
        FileDigest::describe_blocking(&drive_layer).expect("describe drive lower");

    // Write drive image.json with relative path to layers
    fs::write(
        drive_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.overlaybd_image_config_file),
        format!(
            r#"{{
  "repoBlobUrl": "",
  "lowers": [{{ "file": "../layers/drive.commit", "digest": "{}", "size": {} }}],
  "upper": {{}},
  "resultFile": ""
}}"#,
            drive_descriptor.sha256, drive_descriptor.size
        ),
    )
    .expect("write drive image config");

    manifest = manifest
        .with_extra_drives(&[crate::sandbox::ExtraDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: local_dir
                .join(SNAPSHOT_ARTIFACT_LAYOUT.drives_dir)
                .join("data")
                .join(SNAPSHOT_ARTIFACT_LAYOUT.overlaybd_image_config_file),
            read_only: true,
            mount_path: crate::sandbox::ExtraDrive::default_mount_path("data"),
            virtual_size: Some(4096),
            sub_path: None,
            snapshot_output_dir: None,
            volume: false,
        }])
        .expect("attached drive manifest should include virtual size");

    let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
        .await
        .expect("import built artifacts");

    let path = match &built.attached_drives[0] {
        CommittedAttachedDrive::Overlaybd { layers, .. } => {
            assert_eq!(layers.len(), 1);
            match &layers[0] {
                crate::snapshot::OverlaybdLayerRef::Managed(layer) => {
                    layer_file(&layer.digest).display().to_string()
                }
                crate::snapshot::OverlaybdLayerRef::External(_) => {
                    panic!("expected managed rootfs layer")
                }
            }
        }
    };
    assert!(path.contains("managed-layers"));
    assert!(tempdir.path().join(&path).exists());
}

#[tokio::test]
async fn store_snapshot_extracts_rootfs_layers_from_image_config() {
    let tempdir = TempDir::new().expect("tempdir");
    let repository = test_repository(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    let local_dir = tempdir.path().join("local");
    let (_, _, manifest) =
        write_mock_built_artifacts(local_dir.as_path()).expect("mock built artifacts should write");
    let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
        .await
        .expect("collect built artifacts");

    assert_eq!(built.rootfs_layers.len(), 1);
    match &built.rootfs_layers[0] {
        crate::snapshot::OverlaybdLayerRef::Managed(layer) => {
            let path = layer_file(&layer.digest);
            assert!(path.display().to_string().contains("managed-layers"));
            assert!(tempdir.path().join(path).exists());
        }
        crate::snapshot::OverlaybdLayerRef::External(_) => {
            panic!("expected managed rootfs layer")
        }
    }
}

#[tokio::test]
async fn store_snapshot_hard_links_local_publish_artifacts_when_possible() {
    let tempdir = TempDir::new().expect("tempdir");
    let repository = test_repository(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    let local_dir = tempdir.path().join("local");
    let (rootfs_lower, memory_lower, manifest) =
        write_mock_built_artifacts(local_dir.as_path()).expect("mock built artifacts should write");

    let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
        .await
        .expect("collect built artifacts");

    let snapshot_dir = tempdir
        .path()
        .join(BlobKey::posix_snapshot_dir(&snapshot_id));
    assert_same_inode(
        &manifest.vm_state.path,
        &snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
    );

    match &built.rootfs_layers[0] {
        crate::snapshot::OverlaybdLayerRef::Managed(layer) => {
            assert_same_inode(
                &rootfs_lower,
                &tempdir.path().join(layer_file(&layer.digest)),
            );
        }
        crate::snapshot::OverlaybdLayerRef::External(_) => {
            panic!("expected managed rootfs layer")
        }
    }
    assert_same_inode(
        &memory_lower,
        &tempdir
            .path()
            .join(layer_file(&built.memory_layers[0].digest)),
    );
}

#[tokio::test]
async fn store_snapshot_uses_declared_local_layer_descriptor() {
    let tempdir = TempDir::new().expect("tempdir");
    let repository = test_repository(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    let local_dir = tempdir.path().join("local");
    let (rootfs_lower, _, manifest) =
        write_mock_built_artifacts(local_dir.as_path()).expect("mock built artifacts should write");
    // Deliberately not the content digest: this verifies that descriptor
    // backed import trusts the descriptor instead of re-hashing the file.
    let declared_digest = "sha256:declared-rootfs";
    fs::write(
        &manifest.rootfs.image_config_path,
        format!(
            r#"{{
  "repoBlobUrl": "",
  "lowers": [{{ "file": "{}", "digest": "{}", "size": 10 }}],
  "upper": {{}},
  "resultFile": ""
}}"#,
            rootfs_lower.display(),
            declared_digest
        ),
    )
    .expect("write descriptor rootfs image config");

    let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
        .await
        .expect("collect built artifacts");

    match &built.rootfs_layers[0] {
        crate::snapshot::OverlaybdLayerRef::Managed(layer) => {
            assert_eq!(layer.digest, declared_digest);
            assert_eq!(layer.size, 10);
            let path = layer_file(&layer.digest);
            assert!(tempdir.path().join(path).exists());
        }
        crate::snapshot::OverlaybdLayerRef::External(_) => {
            panic!("expected managed rootfs layer")
        }
    }
}

#[tokio::test]
async fn store_snapshot_keeps_mem_image_config_local_only() {
    let tempdir = TempDir::new().expect("tempdir");
    let repository = test_repository(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    let local_dir = tempdir.path().join("local");
    let (_, _, manifest) =
        write_mock_built_artifacts(local_dir.as_path()).expect("mock built artifacts should write");
    let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
        .await
        .expect("collect built artifacts");

    assert_eq!(built.memory_layers.len(), 1);
    let snapshot_dir = tempdir
        .path()
        .join(BlobKey::posix_snapshot_dir(&snapshot_id));
    let persisted_manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest))
            .expect("read committed firecracker manifest"),
    )
    .expect("parse committed firecracker manifest");
    assert!(persisted_manifest["vmState"].get("path").is_none());
    assert!(persisted_manifest["memory"]
        .get("imageConfigPath")
        .is_none());
    assert!(persisted_manifest["rootfs"]
        .get("imageConfigPath")
        .is_none());
    assert!(
        !snapshot_dir
            .join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config)
            .exists(),
        "committed snapshot dir should not persist mem_image.json"
    );
}

#[tokio::test]
async fn store_snapshot_links_dense_exports_in_from_staging() {
    let tempdir = TempDir::new().expect("tempdir");
    let repository = test_repository(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    let local_dir = tempdir.path().join("local");
    let (_, _, manifest) =
        write_mock_built_artifacts(local_dir.as_path()).expect("mock built artifacts should write");
    // A captured sparse rootfs delta, which is dense-exported to be stored.
    let delta_path = local_dir.join("snapshot.commit");
    let data: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&delta_path).expect("create delta"));
    let delta = LSMTFile::create(data, None, 2 * 4096, true)
        .await
        .expect("create sparse delta");
    delta.write_at(0, &[0xBC; 4096]).await.expect("write delta");
    delta.close_seal().await.expect("seal delta");
    drop(delta);
    fs::write(
        &manifest.rootfs.image_config_path,
        format!(r#"{{"lowers":[{{"file":"{}"}}]}}"#, delta_path.display()),
    )
    .expect("write rootfs image config");

    let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
        .await
        .expect("store snapshot");

    let crate::snapshot::OverlaybdLayerRef::Managed(layer) = &built.rootfs_layers[0] else {
        panic!("expected managed rootfs layer");
    };
    let staged = built
        .local_layers
        .iter()
        .find(|upload| upload.digest() == layer.digest)
        .expect("dense export kept for publication");
    let staging_dir = tempdir.path().join("staging");
    assert!(staged.path().starts_with(&staging_dir));
    assert_same_inode(
        staged.path(),
        &tempdir.path().join(layer_file(&layer.digest)),
    );
    drop(built);
    assert_eq!(fs::read_dir(&staging_dir).expect("staging dir").count(), 0);
}

/// Write a real ZFile-compressed sealed LSMT layer, mirroring a compressed
/// memory lower as found in repositories published while capture-time
/// compression existed (or produced by publish-time compression).
async fn write_zfile_lsmt_layer(path: &std::path::Path) {
    let data =
        Arc::new(LocalFile::new(path.with_extension("data")).expect("create layer data file"));
    let index =
        Arc::new(LocalFile::new(path.with_extension("index")).expect("create layer index file"));
    let layer = LSMTFile::create(data, Some(index), 2 * 4096, false)
        .await
        .expect("create layer");
    layer
        .write_at(0, &[0x5A; 4096])
        .await
        .expect("write layer page 0");
    layer
        .write_at(4096, &[0xA5; 4096])
        .await
        .expect("write layer page 1");
    let output = Arc::new(LocalFile::new(path).expect("create zfile layer output"));
    let compress_args = CompressArgs::new(CompressOptions::new(
        CompressOptions::LZ4,
        CompressOptions::DEFAULT_BLOCK_SIZE,
        0,
    ));
    let writer = Arc::new(
        ZFileCompactWriter::new(output, &compress_args)
            .await
            .expect("create zfile compact writer"),
    );
    layer
        .commit_with_args(CommitArgs::from_writer(writer))
        .await
        .expect("commit zfile layer");
}

#[tokio::test]
async fn store_snapshot_preserves_zfile_memory_lower_bytes() {
    let tempdir = TempDir::new().expect("tempdir");
    let repository = test_repository(tempdir.path());
    let snapshot_id = SnapshotId::generate();
    let local_dir = tempdir.path().join("local");
    let (_, _, manifest) =
        write_mock_built_artifacts(local_dir.as_path()).expect("mock built artifacts should write");

    // Replace the mock memory lower with a real ZFile layer. Production
    // mem image configs carry no digest/size for the freshly written
    // lower, so the import hashes the physical file.
    let zfile_lower = local_dir.join("mem.zfile.commit");
    write_zfile_lsmt_layer(&zfile_lower).await;
    let zfile_descriptor =
        FileDigest::describe_blocking(&zfile_lower).expect("describe zfile lower");
    fs::write(
        &manifest.memory.image_config_path,
        format!(r#"{{"lowers":[{{"file":"{}"}}]}}"#, zfile_lower.display()),
    )
    .expect("write zfile memory image config");

    let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
        .await
        .expect("import built artifacts");

    assert_eq!(built.memory_layers.len(), 1);
    let managed = &built.memory_layers[0];
    assert_eq!(managed.digest, zfile_descriptor.sha256);
    assert_eq!(managed.size, zfile_descriptor.size);
    // ZFile wraps the raw LSMT header, so no layer UUID is exposed.
    assert!(managed.uuid.is_none());

    let managed_path = tempdir.path().join(layer_file(&managed.digest));
    assert_eq!(
        fs::read(&managed_path).expect("read managed layer"),
        fs::read(&zfile_lower).expect("read source zfile lower"),
        "managed layer must preserve the physical ZFile bytes"
    );
    assert_eq!(
        fs::metadata(&managed_path)
            .expect("managed layer metadata")
            .len(),
        fs::metadata(&zfile_lower)
            .expect("source zfile metadata")
            .len()
    );
    // The stored bytes still probe as a ZFile with the source algorithm.
    let managed_file = Arc::new(LocalFile::open_ro(&managed_path).expect("open managed layer"));
    assert_eq!(
        is_zfile(managed_file).await.expect("probe managed layer"),
        1
    );
}
