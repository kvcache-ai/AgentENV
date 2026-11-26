//! Unified SQL snapshot directory backed by `agentenv-metadata` (`MetaClient`)
//! and an immutable artifact [`Repository`].

use std::{path::Path, sync::Arc};

use agentenv_domain::{
    SnapshotContent, SnapshotMetadata, SnapshotState, SnapshotVisibility, SnapshotVolume,
};
use agentenv_metadata::{context::current as meta_context, MetaClient, MetaError};
use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};

use super::now_unix_ms;
use super::{
    SnapshotAlias, SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotPublishMetadata,
    SnapshotRecord,
};
use crate::metadata::{map_meta_error, map_rpc_error, NodePins};
use crate::repository::{
    BlobBody, BlobKey, Repository, RepositoryError, RepositoryResult, SnapshotFile,
};

pub use agentenv_domain::SnapshotListFilter;

const LIST_RECORD_CONCURRENCY: usize = 8;

/// Snapshot directory, independent of artifact storage and runtime resolution.
#[async_trait]
pub trait SnapshotCatalog: Send + Sync {
    /// Returns the underlying SQL metadata client when backed by `agentenv-metadata`.
    fn meta(&self) -> Option<&MetaClient> {
        None
    }

    async fn prepare_distributed(
        &self,
        id: &SnapshotId,
        alias: Option<&SnapshotAlias>,
        visibility: SnapshotVisibility,
    ) -> RepositoryResult<Option<SnapshotRecord>>;
    async fn commit_local(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord>;
    async fn commit_distributed(
        &self,
        metadata: &SnapshotPublishMetadata,
        committed: SnapshotManifest,
        local_layers: &[(&str, &Path)],
    ) -> RepositoryResult<SnapshotRecord>;
    async fn get_for_promotion(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        self.get(&id.to_string()).await
    }
    async fn get(&self, reference: &str) -> RepositoryResult<Option<SnapshotRecord>>;
    async fn get_with_visibility(
        &self,
        reference: &str,
        visibility: SnapshotVisibility,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        match visibility {
            SnapshotVisibility::Public => self.get(reference).await,
            SnapshotVisibility::Internal => Ok(None),
        }
    }
    async fn get_metadata(&self, reference: &str) -> RepositoryResult<Option<SnapshotMetadata>> {
        Ok(self
            .get(reference)
            .await?
            .as_ref()
            .map(SnapshotMetadata::from))
    }
    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>>;
    async fn list_metadata(
        &self,
        filter: SnapshotListFilter,
    ) -> RepositoryResult<Vec<SnapshotMetadata>> {
        Ok(self
            .list(filter)
            .await?
            .iter()
            .map(SnapshotMetadata::from)
            .collect())
    }
    async fn delete(&self, reference: &str) -> RepositoryResult<()>;
    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>>;
    async fn pin(&self, _id: &SnapshotId) -> RepositoryResult<()> {
        Ok(())
    }
    fn unpin(&self, _id: &SnapshotId) {}
    fn release_orphan_pins(&self) {}
}

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

    fn repository(&self) -> &dyn Repository {
        self.repository.as_ref()
    }

    async fn check_sql_alias(
        &self,
        alias: &SnapshotAlias,
        id: &SnapshotId,
        visibility: SnapshotVisibility,
    ) -> RepositoryResult<()> {
        if let Some(entry) = self.fetch_entry(alias.as_ref(), visibility).await? {
            if &entry.id != id && entry.alias.as_ref() == Some(alias) {
                if entry.is_visible() {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing: entry.id,
                        new_id: id.clone(),
                    });
                }
                self.retire(&entry.id).await?;
            }
        }
        Ok(())
    }

    async fn write_content(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let bytes = serde_json::to_vec_pretty(&SnapshotContent::from(record))
            .map_err(|error| RepositoryError::backend("encode snapshot content", error))?;
        self.repository
            .put_blob(
                &BlobKey::Snapshot(record.id.clone(), SnapshotFile::Record),
                BlobBody::Bytes(bytes),
            )
            .await
    }

    async fn assemble_record(&self, entry: SnapshotMetadata) -> RepositoryResult<SnapshotRecord> {
        let content = super::store::read_snapshot_content(self.repository(), entry.id())
            .await?
            .ok_or_else(|| RepositoryError::Unavailable {
                reason: format!("snapshot content '{}' is missing", entry.id),
            })?;
        let snapshot_type =
            entry
                .snapshot_placement()
                .map_err(|error| RepositoryError::Unavailable {
                    reason: error.to_string(),
                })?;
        // A promotion can prepare its published inventory before the SQL commit.
        let manifest = match snapshot_type {
            SnapshotPlacement::Local => None,
            SnapshotPlacement::Distributed => {
                Some(
                    content
                        .manifest
                        .ok_or_else(|| RepositoryError::Unavailable {
                            reason: format!("published snapshot '{}' has no manifest", entry.id),
                        })?,
                )
            }
        };
        Ok(SnapshotRecord {
            id: entry.id,
            snapshot_type,
            owner_node_id: entry.owner_node_id,
            alias: entry.alias,
            config: content.config,
            created_at_unix_ms: entry.created_at_unix_ms,
            updated_at_unix_ms: entry.updated_at_unix_ms,
            manifest,
            deleting: entry.state != SnapshotState::Active,
        })
    }

    async fn commit_entry(
        &self,
        record: &SnapshotRecord,
        manifest: &SnapshotManifest,
        promoting: bool,
        restored: Vec<String>,
    ) -> Result<SnapshotMetadata, MetaError> {
        let volume_snapshots = record.config.volume_snapshots.clone();
        if promoting {
            self.meta
                .promote_snapshot(
                    meta_context(),
                    record.into(),
                    volume_snapshots,
                    manifest.clone(),
                    restored,
                )
                .await
                .map_err(|error| MetaError::Backend {
                    message: format!("promote snapshot: {error}"),
                })?
                .and_then(|entry| {
                    entry.ok_or_else(|| MetaError::Unavailable {
                        reason: format!("snapshot '{}' is not found for promotion", record.id),
                    })
                })
        } else {
            self.meta
                .register_snapshot(
                    meta_context(),
                    record.into(),
                    volume_snapshots,
                    manifest.clone(),
                    restored,
                )
                .await
                .map_err(|error| MetaError::Backend {
                    message: format!("register snapshot: {error}"),
                })?
        }
    }

    async fn fetch_entry(
        &self,
        reference: &str,
        visibility: SnapshotVisibility,
    ) -> RepositoryResult<Option<SnapshotMetadata>> {
        self.meta
            .get_snapshot_with_visibility(meta_context(), reference.to_string(), visibility)
            .await
            .map_err(|error| map_rpc_error("get snapshot from metadata directory", error))?
            .map_err(map_meta_error)
    }

    async fn entry_for_id(
        &self,
        id: &SnapshotId,
        visibility: SnapshotVisibility,
    ) -> RepositoryResult<Option<SnapshotMetadata>> {
        Ok(self
            .fetch_entry(&id.to_string(), visibility)
            .await?
            .filter(|entry| &entry.id == id))
    }

    async fn list_sql_entries(
        &self,
        filter: SnapshotListFilter,
    ) -> RepositoryResult<Vec<SnapshotMetadata>> {
        self.meta
            .list_snapshots(meta_context(), filter)
            .await
            .map_err(|error| map_rpc_error("list snapshots from metadata directory", error))?
            .map_err(map_meta_error)
    }

    async fn retire(&self, id: &SnapshotId) -> RepositoryResult<()> {
        self.meta
            .retire_snapshot(meta_context(), id.clone())
            .await
            .map_err(|error| map_rpc_error("retire snapshot in metadata directory", error))?
            .map_err(map_meta_error)
    }

    async fn register_distributed(
        &self,
        record: &SnapshotRecord,
        promoting: bool,
        local_layers: &[(&str, &Path)],
    ) -> RepositoryResult<SnapshotMetadata> {
        let manifest = record
            .manifest
            .as_ref()
            .expect("published Distributed manifest");
        match self
            .commit_entry(record, manifest, promoting, Vec::new())
            .await
        {
            Err(MetaError::MissingLayers { uuids }) => {
                let layers: Vec<_> =
                    managed_layers(manifest, &record.config.volume_snapshots).collect();
                crate::repository::restore_missing_managed_layers(
                    self.repository(),
                    &layers,
                    &uuids,
                    local_layers,
                )
                .await?;
                self.commit_entry(record, manifest, promoting, uuids).await
            }
            result => result,
        }
        .map_err(map_meta_error)
    }
}

#[async_trait]
impl SnapshotCatalog for MetadataCatalog {
    fn meta(&self) -> Option<&MetaClient> {
        Some(&self.meta)
    }

    async fn get_for_promotion(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        let Some(entry) = self.entry_for_id(id, SnapshotVisibility::Public).await? else {
            return Ok(None);
        };
        if !entry.is_visible() {
            return Ok(None);
        }
        self.assemble_record(entry).await.map(Some)
    }

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

    fn release_orphan_pins(&self) {
        if let Some(pins) = &self.pins {
            pins.release_orphans(&self.meta);
        }
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        SnapshotAlias::parse(alias).map_err(|error| RepositoryError::InvalidRequest {
            reason: error.to_string(),
        })?;
        Ok(self.get_metadata(alias).await?.map(|entry| entry.id))
    }

    async fn prepare_distributed(
        &self,
        id: &SnapshotId,
        alias: Option<&SnapshotAlias>,
        visibility: SnapshotVisibility,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        if let Some(entry) = self.entry_for_id(id, visibility).await? {
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
            self.check_sql_alias(alias, id, visibility).await?;
        }
        Ok(None)
    }

    async fn commit_local(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
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
            .register_snapshot(
                meta_context(),
                (&record).into(),
                record.config.volume_snapshots.clone(),
                SnapshotManifest::default(),
                Vec::new(),
            )
            .await
            .map_err(|error| map_rpc_error("register Local snapshot", error))?
            .map_err(map_meta_error)?;
        self.assemble_record(entry).await
    }

    async fn commit_distributed(
        &self,
        metadata: &SnapshotPublishMetadata,
        manifest: SnapshotManifest,
        local_layers: &[(&str, &Path)],
    ) -> RepositoryResult<SnapshotRecord> {
        let id = metadata.id.clone();
        let alias = metadata.alias.clone();
        let visibility = metadata.config.source.visibility();
        let previous = self.entry_for_id(&id, visibility).await?;
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
            super::store::read_snapshot_content(self.repository(), &id)
                .await?
                .map(|content| content.config)
                .unwrap_or_else(|| metadata.config())
        } else {
            metadata.config()
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
        let entry = self
            .register_distributed(&record, promoting, local_layers)
            .await?;
        self.assemble_record(entry).await
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.get_with_visibility(id_or_alias, SnapshotVisibility::Public)
            .await
    }

    async fn get_with_visibility(
        &self,
        id_or_alias: &str,
        visibility: SnapshotVisibility,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        if let Some(entry) = self.fetch_entry(id_or_alias, visibility).await? {
            return if entry.is_visible() {
                self.assemble_record(entry).await.map(Some)
            } else {
                Ok(None)
            };
        }
        Ok(None)
    }

    async fn get_metadata(&self, reference: &str) -> RepositoryResult<Option<SnapshotMetadata>> {
        Ok(self
            .fetch_entry(reference, SnapshotVisibility::Public)
            .await?
            .filter(SnapshotMetadata::is_visible))
    }

    async fn list_metadata(
        &self,
        filter: SnapshotListFilter,
    ) -> RepositoryResult<Vec<SnapshotMetadata>> {
        self.list_sql_entries(filter).await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let sql = self.list_sql_entries(filter.clone()).await?;
        let mut records: Vec<_> = stream::iter(sql)
            .map(|entry| async move { self.assemble_record(entry).await })
            .buffered(LIST_RECORD_CONCURRENCY)
            .try_collect()
            .await?;
        records.retain(|record| filter.matches(record));
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(records)
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        if let Some(entry) = self
            .fetch_entry(id_or_alias, SnapshotVisibility::Public)
            .await?
        {
            return self.retire(&entry.id).await;
        }
        if let Ok(id) = SnapshotId::parse(id_or_alias) {
            return self.retire(&id).await;
        }
        Ok(())
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
    use crate::repository::{DocumentKey, LayerRole, SnapshotFile};
    use crate::snapshot::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::{
        ManagedLayerRef, SnapshotAlias, SnapshotConfig, SnapshotHandle, SnapshotId,
        SnapshotListFilter, SnapshotManager, SnapshotPlacement, SnapshotPublishMetadata,
        SnapshotSource, SNAPSHOT_ARTIFACT_LAYOUT,
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
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join(format!("cache-{node_id}"))),
            runtime_cache_root: None,
        })
        .unwrap();
        let (repository, resolver) = backend.into_parts();
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
        let local_meta = SnapshotPublishMetadata {
            id: local_id.clone(),
            snapshot_type: SnapshotPlacement::Local,
            config: crate::snapshot::SnapshotConfig {
                source: SnapshotSource::Sandbox {
                    source_sandbox_id: "sbx-local".into(),
                    source_sandbox_metadata: None,
                },
                ..crate::snapshot::SnapshotConfig::mock()
            },
            ..SnapshotPublishMetadata::mock()
        };
        let local_record = tcp_node_a.publish_snapshot(local_snap, local_meta).await?;
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
        let dist_meta = SnapshotPublishMetadata {
            id: dist_id.clone(),
            snapshot_type: SnapshotPlacement::Distributed,
            alias: Some(alias.clone()),
            config: crate::snapshot::SnapshotConfig {
                source: SnapshotSource::Sandbox {
                    source_sandbox_id: "sbx-dist".into(),
                    source_sandbox_metadata: None,
                },
                ..crate::snapshot::SnapshotConfig::mock()
            },
        };
        node_a
            .publish_snapshot(dist_capture, dist_meta.clone())
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
                    SnapshotPublishMetadata {
                        id: conflict_id.clone(),
                        ..dist_meta
                    }
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
            .get_document(DocumentKey::SnapshotRecord(dist_id.clone()))
            .await?
            .is_none());
        assert!(repository
            .get_document(DocumentKey::SnapshotAlias(alias.clone()))
            .await?
            .is_none());
        assert!(repository
            .get_document(DocumentKey::SnapshotRecord(local_id.clone()))
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
        server
            .register_snapshot((&owner).into(), &[], SnapshotManifest::default(), &[])
            .await?;
        let metadata = SnapshotPublishMetadata {
            id: SnapshotId::generate(),
            alias: Some(alias.clone()),
            snapshot_type: SnapshotPlacement::Distributed,
            config: owner.config.clone(),
        };
        let manifest = SnapshotManifest::default();
        assert!(matches!(
            catalog
                .commit_distributed(&metadata, manifest.clone(), &[])
                .await,
            Err(RepositoryError::AliasConflict { .. })
        ));
        assert!(catalog.get(&metadata.id.to_string()).await?.is_none());
        assert!(server
            .get_snapshot(&metadata.id.to_string())
            .await?
            .is_none());

        server.retire_snapshot(owner.id).await?;
        assert!(catalog
            .prepare_distributed(&metadata.id, Some(&alias), SnapshotVisibility::Public)
            .await?
            .is_none());
        let committed = catalog.commit_distributed(&metadata, manifest, &[]).await?;
        assert_eq!(committed.id, metadata.id);
        assert_eq!(
            server.get_snapshot(alias.as_ref()).await?.unwrap().id,
            metadata.id
        );
        assert_eq!(catalog.get(alias.as_ref()).await?.unwrap().id, metadata.id);
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
        let template_meta = SnapshotPublishMetadata {
            id: SnapshotId::generate(),
            alias: Some(template_alias.clone()),
            snapshot_type: SnapshotPlacement::Distributed,
            config: SnapshotConfig {
                source: SnapshotSource::Template,
                ..SnapshotConfig::mock()
            },
        };
        let template_record = catalog
            .commit_distributed(&template_meta, SnapshotManifest::mock(), &[])
            .await?;

        let builder_alias = SnapshotAlias::parse("builder-abc123")?;
        let builder_meta = SnapshotPublishMetadata {
            id: SnapshotId::generate(),
            alias: Some(builder_alias.clone()),
            snapshot_type: SnapshotPlacement::Distributed,
            config: SnapshotConfig {
                source: SnapshotSource::Builder,
                ..SnapshotConfig::mock()
            },
        };
        let builder_record = catalog
            .commit_distributed(&builder_meta, SnapshotManifest::mock(), &[])
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
                .get_with_visibility(builder_alias.as_ref(), SnapshotVisibility::Internal)
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
            .list_metadata(SnapshotListFilter::builders())
            .await?
            .into_iter()
            .map(|entry| entry.id)
            .collect();
        assert_eq!(builder_ids, HashSet::from([builder_record.id.clone()]));

        // Public delete by builder alias must NOT delete the internal builder.
        catalog.delete(builder_alias.as_ref()).await?;
        assert!(catalog
            .get_with_visibility(builder_alias.as_ref(), SnapshotVisibility::Internal)
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
        let server = server.with_gc_storage(Arc::new(crate::snapshot::SnapshotGcStorage(
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
        let make_metadata = || SnapshotPublishMetadata {
            config: SnapshotConfig {
                source: SnapshotSource::Sandbox {
                    source_sandbox_id: "source-sandbox".into(),
                    source_sandbox_metadata: None,
                },
                ..SnapshotConfig::mock()
            },
            ..SnapshotPublishMetadata::mock()
        };
        let key = BlobKey::Layer {
            digest: digest.clone(),
            role: LayerRole::Memory,
        };
        repo.put_blob(&key, BlobBody::File(&source)).await?;
        let first = catalog
            .commit_distributed(&make_metadata(), manifest.clone(), &[])
            .await?;
        catalog.delete(&first.id.to_string()).await?;
        assert_eq!(
            meta.collect_garbage(meta_context(), 64)
                .await??
                .deleted_layers,
            1
        );
        assert!(!repo.has_blob(&key).await?);

        let next = make_metadata();
        catalog
            .commit_distributed(&next, manifest, &[(digest.as_str(), source.as_path())])
            .await?;
        assert_eq!(repo.read_blob(&key).await?, bytes);
        assert!(catalog.get(&next.id.to_string()).await?.is_some());
        Ok(())
    }
}
