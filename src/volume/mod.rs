//! Persistent volume catalog management, leasing, and node-local backing cache.

mod backing;
mod error;
mod manager;

pub use agentenv_domain::{
    is_valid_volume_component, VolumeLeaseOp, VolumeMode, VolumePurpose, VolumeRecord, VolumeStatus,
};
pub use error::VolumeError;
pub use manager::{VolumeLimits, VolumeManager, DEFAULT_VOLUME_PAGE_SIZE, DEFAULT_VOLUME_SIZE_MB};

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use overlaybd::config::{ImageConfig, LayerConfig};

    use super::{VolumeError, VolumeManager, VolumeMode, VolumeRecord, VolumeStatus};
    use crate::digest::sha256_digest;
    use crate::layer::{LayerRef, ManagedLayerRef};
    use crate::metadata::{test_meta, GcStorage, MetaClient, MetaServer};
    use crate::repository::backends::PosixFsRepository;
    use crate::repository::{BlobKey, LayerRole, Repository};

    struct TestLayerGcStorage(Arc<dyn Repository>);

    #[async_trait::async_trait]
    impl GcStorage for TestLayerGcStorage {
        async fn delete_snapshot(&self, _id: &agentenv_domain::SnapshotId) -> anyhow::Result<()> {
            Ok(())
        }

        async fn delete_layers(&self, digests: &[String]) -> Vec<anyhow::Result<()>> {
            self.0
                .delete_managed_layers(digests)
                .await
                .into_iter()
                .map(|result| result.map_err(anyhow::Error::from))
                .collect()
        }
    }

    async fn manager_with_meta(
        root: &Path,
        repository: Arc<dyn Repository>,
        meta: MetaClient,
    ) -> VolumeManager {
        VolumeManager::open_with_metadata(root.join("volumes"), repository, meta)
            .await
            .expect("volume manager")
    }

    async fn manager_at(
        root: &Path,
    ) -> (MetaServer, MetaClient, Arc<dyn Repository>, VolumeManager) {
        let repository: Arc<dyn Repository> =
            Arc::new(PosixFsRepository::new(root.join("repository")));
        let (server, _client) = test_meta(root).await;
        let server = server.with_gc_storage(Arc::new(TestLayerGcStorage(Arc::clone(&repository))));
        let meta = agentenv_metadata::transport::embedded(server.clone());
        let manager = manager_with_meta(root, Arc::clone(&repository), meta.clone()).await;
        (server, meta, repository, manager)
    }

    async fn write_volume_layer(path: &Path, payload: &[u8]) -> ManagedLayerRef {
        use overlaybd::backend::local::LocalFile;
        use overlaybd::index_file::{CommitArgs, LSMTFile};
        use overlaybd::virtual_file::VirtualFile;
        let data = Arc::new(LocalFile::new(path.with_extension("data")).unwrap());
        let index = Arc::new(LocalFile::new(path.with_extension("index")).unwrap());
        let layer = LSMTFile::create(data, Some(index), 4096, false)
            .await
            .unwrap();
        let mut page = vec![0; 4096];
        page[..payload.len()].copy_from_slice(payload);
        layer.write_at(0, &page).await.unwrap();
        let mut args = CommitArgs::new(Arc::new(LocalFile::new(path).unwrap()));
        args.uuid = Some(uuid::Uuid::nil());
        layer.commit_with_args(args).await.unwrap();
        let bytes = std::fs::read(path).unwrap();
        ManagedLayerRef {
            digest: sha256_digest(&bytes),
            size: bytes.len() as u64,
            uuid: None,
        }
    }

    async fn published_volume(root: &Path, manager: &VolumeManager) -> VolumeRecord {
        let layer = root.join("base.commit");
        write_volume_layer(&layer, b"published volume layer").await;
        let config = ImageConfig {
            lowers: vec![
                LayerConfig {
                    file: layer.to_string_lossy().into_owned(),
                    ..LayerConfig::default()
                },
                LayerConfig {
                    repo_blob_url: "https://registry.example.test/v2/cache/blobs".to_owned(),
                    digest: sha256_digest(b"external volume layer"),
                    size: 21,
                    ..LayerConfig::default()
                },
            ],
            ..ImageConfig::default()
        };
        let path = root.join("source.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        manager
            .create(
                "parent".to_owned(),
                VolumeMode::Exclusive,
                None,
                Some(path),
                1024,
            )
            .await
            .expect("publish initial volume")
    }

    async fn append_local_layer(manager: &VolumeManager, record: &VolumeRecord) -> ManagedLayerRef {
        let path = manager.local_backing(record).await.unwrap();
        let layer = path.parent().unwrap().join("unpublished.commit");
        let descriptor = write_volume_layer(&layer, b"new volume writes").await;
        let mut config = overlaybd::config::load_image_config(&path).unwrap();
        config.lowers.push(LayerConfig {
            file: layer.to_string_lossy().into_owned(),
            ..LayerConfig::default()
        });
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        descriptor
    }

    #[tokio::test]
    async fn published_volume_clones_survive_parent_deletion_without_local_files() {
        let temp = tempfile::tempdir().unwrap();
        let (server, meta, repository, manager) = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        // A stale local config must not override the committed backing, even
        // when the manager still has the config path in its in-memory cache.
        std::fs::write(
            manager.local_backing(&parent).await.unwrap(),
            b"stale config",
        )
        .unwrap();
        let cold_manager =
            manager_with_meta(temp.path(), Arc::clone(&repository), meta.clone()).await;
        let mut children = Vec::new();
        for (index, (manager, mode)) in [
            (&manager, VolumeMode::ReadOnly),
            (&cold_manager, VolumeMode::Exclusive),
        ]
        .into_iter()
        .enumerate()
        {
            let child = manager
                .create(
                    format!("child-{index}"),
                    mode,
                    Some(parent.id.clone()),
                    None,
                    1024,
                )
                .await
                .expect("clone committed volume without reading local files");
            assert_eq!(child.backing_layers, parent.backing_layers);
            assert_eq!(child.mode, mode);
            assert!(manager.local_backing(&child).await.is_none());
            assert!(!manager.data_dir(&child.id).exists());
            children.push(child);
        }
        manager.delete(&parent.id).await.unwrap();
        // Deleting the parent releases its layer head, while the clones keep
        // the shared managed layer alive across GC sweeps.
        let report = server.collect_garbage(64).await.unwrap();
        assert_eq!(report.deleted_layers, 0);
        let base_digest = match &parent.backing_layers[0] {
            LayerRef::Managed(layer) => layer.digest.clone(),
            LayerRef::External(_) => panic!("expected managed base layer"),
        };
        assert!(repository
            .has_blob(&BlobKey::Layer {
                digest: base_digest.clone(),
                role: LayerRole::Rootfs,
            })
            .await
            .unwrap());

        let restarted = manager_with_meta(temp.path(), Arc::clone(&repository), meta).await;
        for child in &children {
            let backing = restarted
                .materialize_backing(&child.id)
                .await
                .unwrap()
                .unwrap();
            let config = overlaybd::config::load_image_config(backing).unwrap();
            assert_eq!(
                std::fs::read(&config.lowers[0].file).unwrap(),
                std::fs::read(temp.path().join("base.commit")).unwrap()
            );
            assert_eq!(
                config.lowers[0].digest,
                sha256_digest(&std::fs::read(temp.path().join("base.commit")).unwrap())
            );
            assert_eq!(
                config.lowers[0].size,
                std::fs::metadata(temp.path().join("base.commit"))
                    .unwrap()
                    .len()
            );
            assert!(config.lowers[1].file.is_empty());
            assert_eq!(
                config.lowers[1].repo_blob_url,
                "https://registry.example.test/v2/cache/blobs"
            );
            assert_eq!(
                config.lowers[1].digest,
                sha256_digest(b"external volume layer")
            );
        }
        for child in children {
            restarted.delete(&child.id).await.unwrap();
        }
        let report = server.collect_garbage(64).await.unwrap();
        assert_eq!(report.deleted_layers, 1);
        assert!(!repository
            .has_blob(&BlobKey::Layer {
                digest: base_digest,
                role: LayerRole::Rootfs,
            })
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn volume_generations_preserve_inherited_descriptors_and_new_writes() {
        let temp = tempfile::tempdir().unwrap();
        let (_server, _meta, _repo, manager) = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        let seed = manager
            .create(
                "seed".to_owned(),
                VolumeMode::ReadOnly,
                Some(parent.id.clone()),
                None,
                1024,
            )
            .await
            .unwrap();
        manager.reserve(&seed.id, "builder").await.unwrap();
        let child = manager
            .create_child_for_owner(
                &seed.id,
                "work".to_owned(),
                VolumeMode::Exclusive,
                1024,
                "builder",
                "builder",
            )
            .await
            .unwrap();
        assert_eq!(child.backing_layers, parent.backing_layers);
        let backing = manager
            .materialize_backing(&child.id)
            .await
            .unwrap()
            .unwrap();
        let config = overlaybd::config::load_image_config(&backing).unwrap();
        assert_eq!(
            config.lowers[0].digest,
            sha256_digest(&std::fs::read(temp.path().join("base.commit")).unwrap())
        );
        assert_eq!(
            config.lowers[0].size,
            std::fs::metadata(temp.path().join("base.commit"))
                .unwrap()
                .len()
        );
        let appended = append_local_layer(&manager, &child).await;
        manager
            .recover_and_publish_backings("builder", std::slice::from_ref(&child.id))
            .await
            .unwrap();
        manager
            .replace_owner_for("builder", None, &[seed.id, child.id.clone()])
            .await
            .unwrap();
        let published = manager.get(&child.id).await.unwrap();
        assert_eq!(
            &published.backing_layers[..2],
            parent.backing_layers.as_slice()
        );
        assert_eq!(published.backing_layers[2], LayerRef::Managed(appended));
        let next_seed = manager
            .create(
                "next-seed".to_owned(),
                VolumeMode::ReadOnly,
                Some(child.id),
                None,
                1024,
            )
            .await
            .unwrap();
        assert_eq!(next_seed.backing_layers, published.backing_layers);
        assert!(manager.local_backing(&next_seed).await.is_none());
    }

    #[tokio::test]
    async fn reserved_volume_clone_retains_unpublished_local_layers() {
        let temp = tempfile::tempdir().unwrap();
        let (server, _meta, _repo, manager) = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        manager.reserve(&parent.id, "source").await.unwrap();
        let appended = append_local_layer(&manager, &parent).await;
        let child = manager
            .create_child_for_owner(
                &parent.id,
                "fork".to_owned(),
                VolumeMode::Exclusive,
                1024,
                "source",
                "child",
            )
            .await
            .unwrap();
        assert_eq!(child.backing_layers, parent.backing_layers);
        manager
            .replace_owner_for("source", None, std::slice::from_ref(&parent.id))
            .await
            .unwrap();
        manager.delete(&parent.id).await.unwrap();
        assert_eq!(server.collect_garbage(64).await.unwrap().deleted_layers, 0);
        manager
            .recover_and_publish_backings("child", std::slice::from_ref(&child.id))
            .await
            .unwrap();
        let child = manager.get(&child.id).await.unwrap();
        assert_eq!(child.backing_layers.len(), 3);
        assert_eq!(child.backing_layers[2], LayerRef::Managed(appended));
    }

    #[tokio::test]
    async fn published_volume_clone_validates_source_state_and_size() {
        let temp = tempfile::tempdir().unwrap();
        let (_server, _meta, _repo, manager) = manager_at(temp.path()).await;
        let mut parent = published_volume(temp.path(), &manager).await;
        assert_eq!(
            manager
                .create(
                    "wrong-size".to_owned(),
                    VolumeMode::ReadOnly,
                    Some(parent.id.clone()),
                    None,
                    2048
                )
                .await
                .unwrap_err(),
            VolumeError::SizeMismatch
        );
        manager.reserve(&parent.id, "owner").await.unwrap();
        assert_eq!(
            manager
                .create(
                    "reserved".to_owned(),
                    VolumeMode::ReadOnly,
                    Some(parent.id.clone()),
                    None,
                    1024
                )
                .await
                .unwrap_err(),
            VolumeError::Reserved("owner".to_owned())
        );
        for status in [VolumeStatus::Uploading, VolumeStatus::Failed] {
            parent.status = status;
            manager.persist_catalog(&parent, &[]).await.unwrap();
            let error = manager
                .create(
                    "unready".to_owned(),
                    VolumeMode::ReadOnly,
                    Some(parent.id.clone()),
                    None,
                    1024,
                )
                .await
                .unwrap_err();
            assert_eq!(
                error,
                match status {
                    VolumeStatus::Uploading => VolumeError::Uploading(parent.id.clone()),
                    VolumeStatus::Failed => VolumeError::Failed(parent.id.clone()),
                    VolumeStatus::Ready => unreachable!(),
                }
            );
        }
    }

    #[tokio::test]
    async fn create_from_snapshot_preserves_read_only_mode() {
        let temp = tempfile::tempdir().expect("temporary volume repository");
        let (_server, _meta, _repo, manager) = manager_at(temp.path()).await;

        assert_eq!(
            manager
                .create_from_snapshot(
                    "empty-snapshot".to_owned(),
                    VolumeMode::ReadOnly,
                    1024,
                    Vec::new(),
                )
                .await
                .unwrap_err(),
            VolumeError::EmptySnapshotLayers
        );
        let restored = manager
            .create_from_snapshot(
                "restored-read-only".to_owned(),
                VolumeMode::ReadOnly,
                1024,
                vec![LayerRef::Managed(ManagedLayerRef {
                    digest: sha256_digest(b"abc"),
                    size: 4096,
                    uuid: None,
                })],
            )
            .await
            .expect("restore volume snapshot");

        assert_eq!(restored.mode, VolumeMode::ReadOnly);
        assert_eq!(
            manager
                .get(&restored.id)
                .await
                .expect("persisted volume")
                .mode,
            VolumeMode::ReadOnly
        );
    }

    #[tokio::test]
    async fn reclaim_orphaned_local_backings_cleans_cross_node_deleted_volume() {
        let temp = tempfile::tempdir().expect("temporary volume repository");
        let (_server, meta, repo, node_a) = manager_at(temp.path()).await;
        let node_b = VolumeManager::open_with_metadata(
            temp.path().join("node-b-volumes"),
            Arc::clone(&repo),
            meta.clone(),
        )
        .await
        .expect("open node-b manager");

        let layer = vec![LayerRef::Managed(ManagedLayerRef {
            digest: sha256_digest(b"cross-node-delete"),
            size: 4096,
            uuid: None,
        })];
        let deleted = node_a
            .create_from_snapshot(
                "cross-node-deleted".into(),
                VolumeMode::Exclusive,
                1024,
                layer.clone(),
            )
            .await
            .unwrap();
        let live = node_a
            .create_from_snapshot("cross-node-live".into(), VolumeMode::Exclusive, 1024, layer)
            .await
            .unwrap();

        for id in [&deleted.id, &live.id] {
            let dir = node_a.data_dir(id);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("base.commit"), b"backing-bytes").unwrap();
        }
        let inflight_id = format!("vol_{}", uuid::Uuid::now_v7().simple());
        let inflight_dir = node_a.data_dir(&inflight_id);
        let _inflight_guard = node_a.track_creating_backing(&inflight_id);
        std::fs::create_dir_all(&inflight_dir).unwrap();
        std::fs::write(inflight_dir.join("base.commit"), b"inflight-bytes").unwrap();

        node_b.delete(&deleted.id).await.unwrap();
        assert!(node_a.data_dir(&deleted.id).join("base.commit").exists());

        assert_eq!(node_a.reclaim_orphaned_local_backings().await.unwrap(), 1);
        assert!(!node_a.data_dir(&deleted.id).exists());
        assert!(node_a.data_dir(&live.id).join("base.commit").exists());
        assert!(inflight_dir.join("base.commit").exists());
    }
}
