use std::path::Path;

use agentenv::cfg::ConfigManager;
use agentenv::volume::{VolumeManager, VolumeMode, VolumeStatus, DEFAULT_VOLUME_SIZE_MB};
use overlaybd::config::ImageConfig;
use tempfile::tempdir;

use crate::common;

#[tokio::test]
async fn published_volume_upper_is_mountable_with_latest_data_on_another_node() {
    let directory = tempdir().unwrap();
    ConfigManager::init_global().unwrap();
    let (_, _, repository) = common::snapshot_test_parts(directory.path());

    let node_a = VolumeManager::open_with_repository(
        directory.path().join("node-a/catalog"),
        Some(repository.clone()),
    )
    .await
    .unwrap();
    let record = node_a
        .create(
            "shared-data".to_owned(),
            VolumeMode::Exclusive,
            None,
            None,
            DEFAULT_VOLUME_SIZE_MB,
        )
        .await
        .unwrap();
    node_a.reserve(&record.id, "sandbox-a").await.unwrap();

    // A paused/deleted sandbox restacks its writable upper into this local
    // config before publish_owner_backings publishes the complete backing.
    let config_path = record.backing_image_config.clone().unwrap();
    let mut config: ImageConfig =
        serde_json::from_slice(&tokio::fs::read(&config_path).await.unwrap()).unwrap();
    config.lowers.push(config.lowers[0].clone());
    tokio::fs::write(&config_path, serde_json::to_vec(&config).unwrap())
        .await
        .unwrap();

    node_a.publish_owner_backings("sandbox-a").await.unwrap();
    assert_eq!(
        repository
            .get_volume(&record.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        VolumeStatus::Ready
    );

    // The API delete path syncs first and releases the reservation second.
    node_a.release_owner("sandbox-a").await.unwrap();

    let node_b = VolumeManager::open_with_repository(
        directory.path().join("node-b/catalog"),
        Some(repository),
    )
    .await
    .unwrap();
    let mut reopened = node_b.get(&record.id).await.unwrap();
    assert_eq!(reopened.status, VolumeStatus::Ready);
    assert_eq!(reopened.reserved_by_sandbox_id, None);
    assert_eq!(reopened.backing_layers.len(), 2);
    assert!(reopened.backing_image_config.is_none());

    // Backing materialization is lazy and happens only when the second node
    // prepares the volume for a mount.
    reopened = node_b.materialize_backing(&record.id).await.unwrap();
    let reopened_config = reopened.backing_image_config.unwrap();
    let reopened_image: ImageConfig =
        serde_json::from_slice(&tokio::fs::read(reopened_config).await.unwrap()).unwrap();
    assert_eq!(reopened_image.lowers.len(), 2);
    assert!(reopened_image
        .lowers
        .iter()
        .all(|lower| Path::new(&lower.file).is_file()));
    node_b.reserve(&record.id, "sandbox-b").await.unwrap();
    node_b.release_owner("sandbox-b").await.unwrap();
    let child = node_b
        .create(
            "shared-data-child".to_owned(),
            VolumeMode::Exclusive,
            Some(record.id),
            None,
            DEFAULT_VOLUME_SIZE_MB,
        )
        .await
        .unwrap();
    let child_config: ImageConfig = serde_json::from_slice(
        &tokio::fs::read(child.backing_image_config.unwrap())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(child_config.lowers.len(), 2);
}
