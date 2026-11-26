//! Unified SQL snapshot directory backed by `agentenv-metadata` (`MetaClient`)
//! and an immutable artifact [`Repository`].

use std::{path::Path, sync::Arc};

use agentenv_domain::{SnapshotConfig, SnapshotSource, SnapshotVolume};
use agentenv_metadata::{MetaClient, MetaError, SnapshotEntry};
use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};

use super::now_unix_ms;
use super::{SnapshotAlias, SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotRecord};
use crate::metadata::{commit_with_layer_restore, map_meta_error, NodePins, SnapshotPinner};
use crate::repository::{
    encode_snapshot_content, read_snapshot_content, BlobBody, BlobKey, Repository, RepositoryError,
    RepositoryResult, SnapshotFile,
};

pub use agentenv_domain::SnapshotListFilter;

const LIST_RECORD_CONCURRENCY: usize = 8;

/// SQL snapshot directory covering sandbox snapshots, ready templates, and
/// internal builder snapshots, with optional node-local pin tracking (`pins.db`).
#[derive(Clone)]
pub struct MetadataCatalog {
    meta: MetaClient,
    repository: Arc<dyn Repository>,
    pins: Option<Arc<NodePins>>,
}

impl MetadataCatalog {
    pub fn new(meta: MetaClient, repository: Arc<dyn Repository>) -> Self {
        Self {
            meta,
            repository,
            pins: None,
        }
    }

    pub fn with_pins(mut self, pins: Arc<NodePins>) -> Self {
        self.pins = Some(pins);
        self
    }

    pub fn meta_client(&self) -> &MetaClient {
        &self.meta
    }

    async fn check_sql_alias(
        &self,
        alias: &SnapshotAlias,
        id: &SnapshotId,
        internal: bool,
    ) -> RepositoryResult<()> {
        if let Some(entry) = self
            .meta
            .get_snapshot(alias.to_string(), internal)
            .await
            .map_err(map_meta_error)?
        {
            if &entry.id != id && entry.alias.as_ref() == Some(alias) {
                return Err(RepositoryError::AliasConflict {
                    alias: alias.to_string(),
                    existing: entry.id,
                    new_id: id.clone(),
                });
            }
        }
        Ok(())
    }

    async fn write_content(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let bytes = encode_snapshot_content(record)
            .map_err(|error| RepositoryError::backend("encode snapshot content", error))?;
        self.repository
            .put_blob(
                &BlobKey::Snapshot(record.id.clone(), SnapshotFile::Record),
                BlobBody::Bytes(bytes),
            )
            .await
    }

    async fn assemble_record(&self, entry: SnapshotEntry) -> RepositoryResult<SnapshotRecord> {
        let (config, manifest) = read_snapshot_content(self.repository.as_ref(), &entry.id)
            .await?
            .ok_or_else(|| RepositoryError::Unavailable {
                reason: format!("snapshot content '{}' is missing", entry.id),
            })?;
        entry.into_record(config, manifest).map_err(map_meta_error)
    }

    async fn entry_for_id(
        &self,
        id: &SnapshotId,
        internal: bool,
    ) -> RepositoryResult<Option<SnapshotEntry>> {
        Ok(self
            .meta
            .get_snapshot(id.to_string(), internal)
            .await
            .map_err(map_meta_error)?
            .filter(|entry| &entry.id == id))
    }

    pub fn release_orphan_pins(&self) {
        if let Some(pins) = &self.pins {
            pins.release_orphans(&self.meta);
        }
    }

    pub async fn prepare_distributed(
        &self,
        id: &SnapshotId,
        alias: Option<&SnapshotAlias>,
        internal: bool,
    ) -> RepositoryResult<()> {
        if let Some(entry) = self.entry_for_id(id, internal).await? {
            if !entry.is_visible() {
                return Err(RepositoryError::Unavailable {
                    reason: format!("snapshot '{id}' is {}", entry.state),
                });
            }
            if entry.placement == Some(SnapshotPlacement::Distributed) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{id}' is already committed"),
                });
            }
        }
        if let Some(alias) = alias {
            self.check_sql_alias(alias, id, internal).await?;
        }
        Ok(())
    }

    pub async fn commit_local(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        if record.snapshot_type != SnapshotPlacement::Local {
            return Err(RepositoryError::Unsupported {
                feature: "metadata-only commit of Distributed snapshots".to_string(),
            });
        }
        record
            .validate()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        self.write_content(&record).await?;
        let entry = self
            .meta
            .register_snapshot(record, Vec::new())
            .await
            .map_err(map_meta_error)?;
        self.assemble_record(entry).await
    }

    pub async fn commit_distributed(
        &self,
        id: SnapshotId,
        alias: Option<SnapshotAlias>,
        config: SnapshotConfig,
        manifest: SnapshotManifest,
        local_layers: &[(&str, &Path)],
    ) -> RepositoryResult<SnapshotRecord> {
        let internal = matches!(config.source, SnapshotSource::Builder);
        let previous = self.entry_for_id(&id, internal).await?;
        if let Some(entry) = &previous {
            if !entry.is_visible() {
                return Err(RepositoryError::Unavailable {
                    reason: format!("snapshot '{id}' is {}", entry.state),
                });
            }
            if entry.placement == Some(SnapshotPlacement::Distributed) {
                if entry.alias != alias {
                    return Err(map_meta_error(MetaError::SnapshotConflict { id }));
                }
                return self.assemble_record(entry.clone()).await;
            }
        }
        let promoting = previous.is_some();
        let now = now_unix_ms();
        let config = if promoting {
            read_snapshot_content(self.repository.as_ref(), &id)
                .await?
                .map(|(stored_config, _)| stored_config)
                .unwrap_or(config)
        } else {
            config
        };
        let mut record = SnapshotRecord::new(
            id,
            SnapshotPlacement::Distributed,
            alias,
            config,
            Some(manifest),
            now,
        );
        if let Some(previous) = previous {
            record.created_at_unix_ms = previous.created_at_unix_ms;
        }
        record
            .validate()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        self.write_content(&record).await?;
        let manifest = record
            .manifest
            .as_ref()
            .expect("published Distributed manifest");
        let layers: Vec<_> = managed_layers(manifest, &record.config.volume_snapshots).collect();
        let entry = commit_with_layer_restore(
            self.repository.as_ref(),
            &layers,
            local_layers,
            map_meta_error,
            std::convert::identity,
            |restored| {
                let record = record.clone();
                async move {
                    if promoting {
                        let id = record.id.clone();
                        self.meta
                            .promote_snapshot(record, restored)
                            .await?
                            .ok_or_else(|| MetaError::Unavailable {
                                reason: format!("snapshot '{id}' is not found for promotion"),
                            })
                    } else {
                        self.meta.register_snapshot(record, restored).await
                    }
                }
            },
        )
        .await?;
        self.assemble_record(entry).await
    }

    pub async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.get_in_namespace(id_or_alias, false).await
    }

    pub async fn get_in_namespace(
        &self,
        id_or_alias: &str,
        internal: bool,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        if let Some(entry) = self
            .meta
            .get_snapshot(id_or_alias.to_string(), internal)
            .await
            .map_err(map_meta_error)?
        {
            return if entry.is_visible() {
                self.assemble_record(entry).await.map(Some)
            } else {
                Ok(None)
            };
        }
        Ok(None)
    }

    pub async fn get_metadata(&self, reference: &str) -> RepositoryResult<Option<SnapshotEntry>> {
        Ok(self
            .meta
            .get_snapshot(reference.to_string(), false)
            .await
            .map_err(map_meta_error)?
            .filter(SnapshotEntry::is_visible))
    }

    pub async fn list_metadata(
        &self,
        filter: SnapshotListFilter,
    ) -> RepositoryResult<Vec<SnapshotEntry>> {
        self.meta
            .list_snapshots(filter)
            .await
            .map_err(map_meta_error)
    }

    pub async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let sql = self.list_metadata(filter).await?;
        stream::iter(sql)
            .map(|entry| async move { self.assemble_record(entry).await })
            .buffered(LIST_RECORD_CONCURRENCY)
            .try_collect()
            .await
    }

    pub async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        if let Some(entry) = self
            .meta
            .get_snapshot(id_or_alias.to_string(), false)
            .await
            .map_err(map_meta_error)?
        {
            return self
                .meta
                .retire_snapshot(entry.id)
                .await
                .map_err(map_meta_error);
        }
        if let Ok(id) = SnapshotId::parse(id_or_alias) {
            return self.meta.retire_snapshot(id).await.map_err(map_meta_error);
        }
        Ok(())
    }
}

#[async_trait]
impl SnapshotPinner for MetadataCatalog {
    async fn pin(&self, id: &SnapshotId) -> RepositoryResult<()> {
        if let Some(pins) = &self.pins {
            pins.pin_snapshot(&self.meta, id).await?;
        }
        Ok(())
    }

    fn unpin(&self, id: &SnapshotId) {
        if let Some(pins) = &self.pins {
            pins.unpin_snapshot(&self.meta, id);
        }
    }
}

fn managed_layers<'a>(
    manifest: &'a SnapshotManifest,
    volume_snapshots: &'a [SnapshotVolume],
) -> impl Iterator<Item = &'a super::ManagedLayerRef> {
    manifest.memory_layers.iter().chain(
        manifest
            .rootfs_layers
            .iter()
            .chain(manifest.attached_drives.iter().flat_map(|drive| {
                let super::CommittedAttachedDrive::Overlaybd { layers, .. } = drive;
                layers
            }))
            .chain(volume_snapshots.iter().flat_map(|volume| &volume.layers))
            .filter_map(|layer| match layer {
                super::LayerRef::Managed(layer) => Some(layer),
                super::LayerRef::External(_) => None,
            }),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use agentenv_metadata::{transport, DatabaseConfig, MetaServer};
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::layer::LayerSource;
    use crate::repository::{LayerRole, LegacyDocumentKey, SnapshotFile};
    use crate::snapshot::mock::{test_posix_storage, write_mock_built_artifacts};
    use crate::snapshot::{
        ManagedLayerRef, SnapshotAlias, SnapshotConfig, SnapshotHandle, SnapshotId,
        SnapshotListFilter, SnapshotManager, SnapshotPlacement, SnapshotSource,
        SNAPSHOT_ARTIFACT_LAYOUT,
    };

    fn built_capture(root: &Path, id: SnapshotId) -> Arc<SnapshotHandle> {
        let dir = tempfile::Builder::new()
            .prefix("capture-")
            .tempdir_in(root)
            .unwrap();
        let mut manifest = write_mock_built_artifacts(dir.path()).unwrap().2;
        manifest.memory.virtual_size = overlaybd::startup_pack::PACK_PAGE_BYTES;
        for layer in &mut manifest.rootfs.layers.layers {
            if let LayerSource::File(file) = layer {
                file.uuid = uuid::Uuid::now_v7();
            }
        }
        for layer in &mut manifest.memory.layers.layers {
            if let LayerSource::File(file) = layer {
                file.uuid = uuid::Uuid::now_v7();
            }
        }
        crate::snapshot::finish_snapshot(&mut manifest, dir.path()).unwrap();
        Arc::new(SnapshotHandle::new(id, manifest, dir))
    }

    fn node_manager(root: &Path, node_id: &str, meta: MetaClient) -> SnapshotManager {
        let (repository, resolver) =
            test_posix_storage(root, root.join(format!("node-{node_id}"))).unwrap();
        let catalog = MetadataCatalog::new(meta, Arc::clone(&repository));
        let mut manager =
            SnapshotManager::from_parts(Arc::new(catalog), repository, resolver, None);
        manager.node_id = node_id.to_string();
        manager
    }

    async fn spawn_drop_response_proxy(
        server_addr: std::net::SocketAddr,
        drop_next_promote: Arc<AtomicBool>,
    ) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((client_stream, _)) = listener.accept().await {
                let Ok(server_stream) = TcpStream::connect(server_addr).await else {
                    break;
                };
                let drop_flag = Arc::clone(&drop_next_promote);
                tokio::spawn(async move {
                    let (mut c_read, mut c_write) = client_stream.into_split();
                    let (mut s_read, mut s_write) = server_stream.into_split();
                    let to_server = tokio::io::copy(&mut c_read, &mut s_write);
                    let to_client = async {
                        let mut buf = vec![0_u8; 64 * 1024];
                        loop {
                            let n = tokio::io::AsyncReadExt::read(&mut s_read, &mut buf).await?;
                            if n == 0 {
                                return Ok::<_, std::io::Error>(());
                            }
                            if drop_flag.load(Ordering::SeqCst)
                                && String::from_utf8_lossy(&buf[..n]).contains("PromoteSnapshot")
                            {
                                drop_flag.store(false, Ordering::SeqCst);
                                let _ = c_write.shutdown().await;
                                return Ok(());
                            }
                            c_write.write_all(&buf[..n]).await?;
                            c_write.flush().await?;
                        }
                    };
                    let _ = tokio::select! {
                        res = to_server => res.map(|_| ()),
                        res = to_client => res,
                    };
                });
            }
        });
        proxy_addr
    }

    #[tokio::test]
    async fn metadata_catalog_drives_snapshot_lifecycle_and_retains_committed_artifacts_on_lost_response(
    ) -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let sqlite = DatabaseConfig::Sqlite {
            path: root.path().join("metadata.sqlite3"),
        };
        exercise_metadata_snapshot_flow(&sqlite).await?;
        if let Ok(url) = std::env::var("AENV_TEST_DATABASE_URL") {
            exercise_metadata_snapshot_flow(&DatabaseConfig::Postgres { url }).await?;
        }
        Ok(())
    }

    async fn exercise_metadata_snapshot_flow(database: &DatabaseConfig) -> anyhow::Result<()> {
        let root = TempDir::new()?;
        database.migrate().await?;
        let server = MetaServer::open(database).await?;

        let node_a = node_manager(root.path(), "node-a", transport::embedded(server.clone()));
        let node_b = node_manager(root.path(), "node-b", transport::embedded(server.clone()));

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let server_addr = listener.local_addr()?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let serving = tokio::spawn(transport::serve_tcp(listener, server.clone(), async move {
            let _ = stopped.await;
            Ok(())
        }));

        let drop_promote = Arc::new(AtomicBool::new(false));
        let proxy_addr = spawn_drop_response_proxy(server_addr, Arc::clone(&drop_promote)).await;
        let client = transport::connect(&proxy_addr.to_string()).await?;
        let tcp_node_a = node_manager(root.path(), "node-a", client);

        // Local registration -> promote with lost RPC response -> artifacts retained -> retry succeeds.
        let local_id = SnapshotId::generate();
        let local_snap = built_capture(root.path(), local_id.clone());
        let local_config = crate::snapshot::SnapshotConfig {
            source: SnapshotSource::Sandbox {
                source_sandbox_id: "sbx-local".into(),
                source_sandbox_metadata: None,
            },
            ..crate::snapshot::SnapshotConfig::mock()
        };
        let local_record = tcp_node_a
            .publish_snapshot(local_snap, SnapshotPlacement::Local, None, local_config)
            .await?;
        assert_eq!(local_record.snapshot_type, SnapshotPlacement::Local);
        assert!(tcp_node_a
            .load_runnable(local_id.to_string())
            .await?
            .is_some());
        assert!(node_b.load_runnable(local_id.to_string()).await.is_err());

        drop_promote.store(true, Ordering::SeqCst);
        assert!(tcp_node_a.promote(&local_id.to_string()).await.is_err());

        let snapshot_dir = root
            .path()
            .join("repository/snapshots")
            .join(local_id.to_string());
        assert!(snapshot_dir
            .join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state)
            .is_file());
        assert!(snapshot_dir.join(SnapshotFile::Record.name()).is_file());

        let promoted = node_a
            .promote(&local_id.to_string())
            .await?
            .expect("retry promote returns committed Distributed record");
        assert_eq!(promoted.snapshot_type, SnapshotPlacement::Distributed);

        let resolved_on_b = node_b
            .load_runnable(local_id.to_string())
            .await?
            .expect("node B resolves promoted snapshot");
        assert!(resolved_on_b
            .manifest
            .runtime_manifest()
            .vm_state
            .path
            .is_file());

        // Direct Distributed publish, alias conflict rollback, list, and retirement.
        let dist_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse(&format!("snap-{dist_id}"))?;
        let dist_capture = built_capture(root.path(), dist_id.clone());
        let trace_path = root.path().join("startup.trace");
        std::fs::write(
            &trace_path,
            overlaybd::startup_pack::encode_trace(
                dist_capture.runtime_manifest().memory.virtual_size,
                &[0],
            )?,
        )?;
        dist_capture.set_startup_recording(crate::snapshot::StartupRecording {
            trace: tokio::spawn(async move { Some(trace_path) }),
            keep_alive: Box::new(Arc::clone(&dist_capture)),
        });
        let dist_config = crate::snapshot::SnapshotConfig {
            source: SnapshotSource::Sandbox {
                source_sandbox_id: "sbx-dist".into(),
                source_sandbox_metadata: None,
            },
            ..crate::snapshot::SnapshotConfig::mock()
        };
        node_a
            .publish_snapshot(
                dist_capture,
                SnapshotPlacement::Distributed,
                Some(alias.clone()),
                dist_config.clone(),
            )
            .await?;

        let distributed = node_b.get(dist_id.to_string()).await?.unwrap();
        assert!(distributed
            .manifest
            .as_ref()
            .unwrap()
            .memory_startup
            .is_some());

        let conflict_id = SnapshotId::generate();
        let conflict_capture = built_capture(root.path(), conflict_id.clone());
        assert!(matches!(
            node_a
                .publish_snapshot(
                    conflict_capture,
                    SnapshotPlacement::Distributed,
                    Some(alias.clone()),
                    dist_config,
                )
                .await,
            Err(RepositoryError::AliasConflict { .. })
        ));
        assert!(!root
            .path()
            .join("repository/snapshots")
            .join(conflict_id.to_string())
            .exists());

        let repository = node_a.repository();
        let key = BlobKey::Snapshot(dist_id.clone(), SnapshotFile::Record);
        let stored: serde_json::Value = serde_json::from_slice(&repository.read_blob(&key).await?)?;
        assert!(stored.get("metadata").is_none());
        assert!(stored.get("config").is_some());
        assert!(stored.get("manifest").is_some());
        assert!(stored["manifest"]["memory_startup"].is_object());
        // Commits are SQL-only and never mirror to legacy catalog/records or catalog/aliases.
        assert!(repository
            .get_legacy_document(LegacyDocumentKey::SnapshotRecord(dist_id.clone()))
            .await?
            .is_none());
        assert!(repository
            .get_legacy_document(LegacyDocumentKey::SnapshotAlias(alias.clone()))
            .await?
            .is_none());
        assert!(repository
            .get_legacy_document(LegacyDocumentKey::SnapshotRecord(local_id.clone()))
            .await?
            .is_none());

        let listed = node_b
            .list(SnapshotListFilter::sandbox_snapshots(
                Some("sbx-dist".into()),
                None,
            ))
            .await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, dist_id);

        node_b.delete(alias.as_ref()).await?;
        assert!(node_a.get(alias.as_ref()).await?.is_none());
        assert!(resolved_on_b
            .manifest
            .runtime_manifest()
            .vm_state
            .path
            .is_file());

        let _ = stop.send(());
        serving.await??;
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn failed_sql_registration_stays_invisible_and_succeeds_on_retry() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let database = DatabaseConfig::Sqlite {
            path: root.path().join("metadata.sqlite3"),
        };
        database.migrate().await?;
        let server = MetaServer::open(&database).await?;
        let repo: Arc<dyn Repository> = Arc::new(
            crate::repository::backends::PosixFsRepository::new(root.path().join("repository")),
        );
        let catalog = MetadataCatalog::new(transport::embedded(server.clone()), Arc::clone(&repo))
            .with_pins(Arc::new(
                NodePins::open(root.path(), "test-node".into()).await?,
            ));
        let alias = SnapshotAlias::parse("retry")?;
        let mut owner = SnapshotRecord::mock_ready(SnapshotManifest::default());
        owner.config.source = SnapshotSource::Sandbox {
            source_sandbox_id: "owner".into(),
            source_sandbox_metadata: None,
        };
        owner.alias = Some(alias.clone());
        // Another node claims the SQL alias after this publication's preflight.
        server.register_snapshot(owner.clone(), &[]).await?;
        let candidate_id = SnapshotId::generate();
        let manifest = SnapshotManifest::default();
        assert!(matches!(
            catalog
                .commit_distributed(
                    candidate_id.clone(),
                    Some(alias.clone()),
                    owner.config.clone(),
                    manifest.clone(),
                    &[],
                )
                .await,
            Err(RepositoryError::AliasConflict { .. })
        ));
        assert!(catalog.get(&candidate_id.to_string()).await?.is_none());
        assert!(server
            .get_snapshot(&candidate_id.to_string(), false)
            .await?
            .is_none());

        server.retire_snapshot(owner.id).await?;
        catalog
            .prepare_distributed(&candidate_id, Some(&alias), false)
            .await?;
        let committed = catalog
            .commit_distributed(
                candidate_id.clone(),
                Some(alias.clone()),
                owner.config,
                manifest,
                &[],
            )
            .await?;
        assert_eq!(committed.id, candidate_id);
        assert_eq!(
            server
                .get_snapshot(alias.as_ref(), false)
                .await?
                .unwrap()
                .id,
            candidate_id
        );
        assert_eq!(catalog.get(alias.as_ref()).await?.unwrap().id, candidate_id);
        catalog.pin(&committed.id).await?;
        catalog.unpin(&committed.id);
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn template_and_internal_builder_snapshots_are_isolated_in_sql() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let database = DatabaseConfig::Sqlite {
            path: root.path().join("metadata.sqlite3"),
        };
        database.migrate().await?;
        let server = MetaServer::open(&database).await?;
        let meta = transport::embedded(server.clone());
        let repository: Arc<dyn Repository> = Arc::new(
            crate::repository::backends::PosixFsRepository::new(root.path().join("repository")),
        );
        let catalog = MetadataCatalog::new(meta.clone(), Arc::clone(&repository)).with_pins(
            Arc::new(NodePins::open(root.path(), "test-node".into()).await?),
        );

        let template_alias = SnapshotAlias::parse("template-snap")?;
        let template_record = catalog
            .commit_distributed(
                SnapshotId::generate(),
                Some(template_alias.clone()),
                SnapshotConfig {
                    source: SnapshotSource::Template,
                    ..SnapshotConfig::mock()
                },
                SnapshotManifest::mock(),
                &[],
            )
            .await?;

        let builder_alias = SnapshotAlias::parse("builder-abc123")?;
        let builder_record = catalog
            .commit_distributed(
                SnapshotId::generate(),
                Some(builder_alias.clone()),
                SnapshotConfig {
                    source: SnapshotSource::Builder,
                    ..SnapshotConfig::mock()
                },
                SnapshotManifest::mock(),
                &[],
            )
            .await?;

        // Public queries see template, never internal builder.
        assert_eq!(
            catalog.get(template_alias.as_ref()).await?.unwrap().id,
            template_record.id
        );
        assert!(catalog.get(builder_alias.as_ref()).await?.is_none());
        assert!(catalog.get(&builder_record.id.to_string()).await?.is_none());
        assert_eq!(
            catalog
                .get_in_namespace(builder_alias.as_ref(), true)
                .await?
                .unwrap()
                .id,
            builder_record.id
        );

        let public_ids: HashSet<_> = catalog
            .list_metadata(SnapshotListFilter::default())
            .await?
            .into_iter()
            .map(|entry| entry.id)
            .collect();
        assert_eq!(public_ids, HashSet::from([template_record.id.clone()]));
        let builder_ids: HashSet<_> = catalog
            .list_metadata(SnapshotListFilter {
                sources: Some(vec![agentenv_domain::SnapshotSourceKind::Builder]),
                ..Default::default()
            })
            .await?
            .into_iter()
            .map(|entry| entry.id)
            .collect();
        assert_eq!(builder_ids, HashSet::from([builder_record.id.clone()]));

        // Public delete by builder alias must NOT delete the internal builder.
        catalog.delete(builder_alias.as_ref()).await?;
        assert!(catalog
            .get_in_namespace(builder_alias.as_ref(), true)
            .await?
            .is_some());

        catalog.delete(template_alias.as_ref()).await?;
        assert!(catalog.get(template_alias.as_ref()).await?.is_none());
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn publication_reuploads_a_layer_collected_before_registration() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let database = DatabaseConfig::Sqlite {
            path: root.path().join("metadata.sqlite3"),
        };
        database.migrate().await?;
        let server = MetaServer::open(&database).await?;
        let repo: Arc<dyn Repository> = Arc::new(
            crate::repository::backends::PosixFsRepository::new(root.path().join("repository")),
        );
        let server = server.with_gc_storage(Arc::new(crate::metadata::RepositoryGcStorage(
            Arc::clone(&repo),
        )));
        let meta = transport::embedded(server.clone());
        let catalog = MetadataCatalog::new(meta.clone(), Arc::clone(&repo));
        let source = root.path().join("captured.commit");
        let bytes = b"captured layer";
        std::fs::write(&source, bytes)?;
        let digest = crate::digest::sha256_digest(bytes);
        let manifest = SnapshotManifest {
            memory_layers: vec![ManagedLayerRef {
                digest: digest.clone(),
                size: bytes.len() as u64,
                uuid: None,
            }],
            ..SnapshotManifest::default()
        };
        let make_config = || SnapshotConfig {
            source: SnapshotSource::Sandbox {
                source_sandbox_id: "source-sandbox".into(),
                source_sandbox_metadata: None,
            },
            ..SnapshotConfig::mock()
        };
        let key = BlobKey::Layer {
            digest: digest.clone(),
            role: LayerRole::Memory,
        };
        repo.put_blob(&key, BlobBody::File(&source)).await?;
        let first = catalog
            .commit_distributed(
                SnapshotId::generate(),
                None,
                make_config(),
                manifest.clone(),
                &[],
            )
            .await?;
        catalog.delete(&first.id.to_string()).await?;
        assert_eq!(meta.collect_garbage(64).await?.deleted_layers, 1);
        assert!(!repo.has_blob(&key).await?);

        let next_id = SnapshotId::generate();
        catalog
            .commit_distributed(
                next_id.clone(),
                None,
                make_config(),
                manifest,
                &[(digest.as_str(), source.as_path())],
            )
            .await?;
        assert_eq!(repo.read_blob(&key).await?, bytes);
        assert!(catalog.get(&next_id.to_string()).await?.is_some());
        Ok(())
    }
}
