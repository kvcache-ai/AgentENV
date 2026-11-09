//! Snapshot directory backed by `agentenv-metadata` (`MetaClient`) and an artifact [`Repository`].

use std::sync::Arc;

use agentenv_metadata::{
    context::current as meta_context, MetaClient, MetaError, RpcError, SnapshotEntry,
};
use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};

use super::catalog::{SnapshotCatalog, SnapshotListFilter};
use super::types::now_unix_ms;
use super::{
    CommittedSnapshot, SnapshotAlias, SnapshotId, SnapshotPlacement, SnapshotPublishMetadata,
    SnapshotRecord,
};
use crate::repository::{
    BlobBody, BlobKey, Repository, RepositoryError, RepositoryResult, SnapshotFile,
};

const LIST_MANIFEST_CONCURRENCY: usize = 8;

/// Authoritative snapshot directory over `agentenv-metadata`, paired with a [`Repository`]
/// that stores each Distributed snapshot's immutable [`CommittedSnapshot`] manifest.
#[derive(Clone)]
pub struct MetadataCatalog {
    meta: MetaClient,
    repository: Arc<dyn Repository>,
}

impl MetadataCatalog {
    pub fn new(meta: MetaClient, repository: Arc<dyn Repository>) -> Self {
        Self { meta, repository }
    }

    async fn write_committed_manifest(
        &self,
        id: &SnapshotId,
        committed: &CommittedSnapshot,
    ) -> RepositoryResult<()> {
        let value = serde_json::to_value(committed).map_err(|error| {
            RepositoryError::backend("serialize committed snapshot manifest", error)
        })?;
        let bytes = serde_json::to_vec_pretty(&sort_json(value)).map_err(|error| {
            RepositoryError::backend("encode committed snapshot manifest", error)
        })?;
        self.repository
            .put_blob(
                &BlobKey::Snapshot(id.clone(), SnapshotFile::CommittedManifest),
                BlobBody::Bytes(bytes),
            )
            .await
    }

    async fn read_committed_manifest(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<CommittedSnapshot> {
        let bytes = self
            .repository
            .read_blob(&BlobKey::Snapshot(
                id.clone(),
                SnapshotFile::CommittedManifest,
            ))
            .await?;
        serde_json::from_slice(&bytes).map_err(|error| {
            RepositoryError::backend(format!("parse committed snapshot manifest '{id}'"), error)
        })
    }

    async fn assemble_record(&self, entry: SnapshotEntry) -> RepositoryResult<SnapshotRecord> {
        match entry.snapshot_placement().map_err(map_meta_error)? {
            SnapshotPlacement::Local => entry.into_local_record().map_err(map_meta_error),
            SnapshotPlacement::Distributed => {
                let committed = self.read_committed_manifest(entry.id()).await?;
                entry.into_record(committed).map_err(map_meta_error)
            }
        }
    }

    async fn fetch_entry(&self, reference: &str) -> RepositoryResult<Option<SnapshotEntry>> {
        self.meta
            .get_snapshot(meta_context(), reference.to_string())
            .await
            .map_err(|error| map_rpc_error("get snapshot from metadata directory", error))?
            .map_err(map_meta_error)
    }
}

#[async_trait]
impl SnapshotCatalog for MetadataCatalog {
    fn committed_content_is_immutable(&self) -> bool {
        true
    }

    async fn create(&self, _: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        Err(RepositoryError::Unsupported {
            feature: "pre-created template builds in the metadata directory".into(),
        })
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        SnapshotAlias::parse(alias).map_err(|error| RepositoryError::InvalidRequest {
            reason: error.to_string(),
        })?;
        Ok(self.get(alias).await?.map(|record| record.id))
    }

    async fn try_start_build(&self, _: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        Err(RepositoryError::Unsupported {
            feature: "template build transitions in the metadata directory".into(),
        })
    }

    async fn mark_build_error(
        &self,
        _: &SnapshotId,
        _: super::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        Err(RepositoryError::Unsupported {
            feature: "template build transitions in the metadata directory".into(),
        })
    }

    async fn prepare_distributed(
        &self,
        id: &SnapshotId,
        alias: Option<&SnapshotAlias>,
    ) -> RepositoryResult<()> {
        if let Some(entry) = self.fetch_entry(&id.to_string()).await? {
            if !entry.is_visible() {
                return Err(RepositoryError::Unavailable {
                    reason: format!("snapshot '{id}' is retired"),
                });
            }
            if entry.snapshot_placement().map_err(map_meta_error)? == SnapshotPlacement::Distributed
            {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{id}' is already committed"),
                });
            }
        }
        if let Some(alias) = alias.filter(|alias| SnapshotId::parse(alias.as_ref()).is_err()) {
            if let Some(entry) = self.fetch_entry(alias.as_ref()).await? {
                if entry.id() != id {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing: entry.id().clone(),
                        new_id: id.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    async fn commit_local(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        if record.snapshot_type != SnapshotPlacement::Local {
            return Err(RepositoryError::Unsupported {
                feature: "metadata-only commit of Distributed snapshots".to_string(),
            });
        }
        record
            .validate_committed_metadata()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        let entry = self
            .meta
            .register_snapshot(meta_context(), record)
            .await
            .map_err(|error| map_rpc_error("register Local snapshot", error))?
            .map_err(map_meta_error)?;
        entry.into_local_record().map_err(map_meta_error)
    }

    async fn commit_distributed(
        &self,
        metadata: &SnapshotPublishMetadata,
        committed: CommittedSnapshot,
    ) -> RepositoryResult<SnapshotRecord> {
        let id = metadata.id.clone();
        let previous = self.fetch_entry(&id.to_string()).await?;
        let now = now_unix_ms();
        let (is_promote, record) = match previous {
            Some(entry) if !entry.is_visible() => {
                return Err(RepositoryError::Unavailable {
                    reason: format!("snapshot '{id}' is retired"),
                });
            }
            Some(entry)
                if entry.snapshot_placement().map_err(map_meta_error)?
                    == SnapshotPlacement::Distributed =>
            {
                self.write_committed_manifest(&id, &committed).await?;
                return entry.into_record(committed).map_err(map_meta_error);
            }
            Some(entry) => {
                let mut record = entry.into_local_record().map_err(map_meta_error)?;
                record.mark_committed(
                    metadata.alias.clone(),
                    metadata.resources,
                    committed.clone(),
                    metadata.source.clone(),
                    now,
                );
                (true, record)
            }
            None => (
                false,
                SnapshotRecord::new_committed(id.clone(), metadata, committed.clone(), now),
            ),
        };

        self.write_committed_manifest(&id, &committed).await?;
        let result = if is_promote {
            self.meta
                .promote_snapshot(meta_context(), record)
                .await
                .map_err(|error| map_rpc_error("promote snapshot in metadata directory", error))?
                .and_then(|entry| {
                    entry.ok_or_else(|| MetaError::Unavailable {
                        reason: format!("snapshot '{id}' is not found for promotion"),
                    })
                })
        } else {
            self.meta
                .register_snapshot(meta_context(), record)
                .await
                .map_err(|error| {
                    map_rpc_error("register Distributed snapshot in metadata directory", error)
                })?
        };

        match result {
            Ok(entry) => entry.into_record(committed).map_err(map_meta_error),
            Err(MetaError::SnapshotConflict { .. }) if is_promote => {
                if let Some(current) = self.fetch_entry(&id.to_string()).await? {
                    if current.is_visible()
                        && current.snapshot_placement().ok() == Some(SnapshotPlacement::Distributed)
                    {
                        return current.into_record(committed).map_err(map_meta_error);
                    }
                }
                Err(map_meta_error(MetaError::SnapshotConflict { id }))
            }
            Err(error) => Err(map_meta_error(error)),
        }
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        let Some(entry) = self.fetch_entry(id_or_alias).await? else {
            return Ok(None);
        };
        if !entry.is_visible() {
            return Ok(None);
        }
        self.assemble_record(entry).await.map(Some)
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let entries = self
            .meta
            .list_snapshots(meta_context(), filter)
            .await
            .map_err(|error| map_rpc_error("list snapshots from metadata directory", error))?
            .map_err(map_meta_error)?;
        stream::iter(entries)
            .map(|entry| self.assemble_record(entry))
            .buffered(LIST_MANIFEST_CONCURRENCY)
            .try_collect()
            .await
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        let id = if let Ok(id) = SnapshotId::parse(id_or_alias) {
            id
        } else {
            let alias = SnapshotAlias::parse(id_or_alias).map_err(|error| {
                RepositoryError::InvalidRequest {
                    reason: error.to_string(),
                }
            })?;
            let Some(entry) = self.fetch_entry(alias.as_ref()).await? else {
                return Ok(());
            };
            entry.id().clone()
        };
        self.meta
            .retire_snapshot(meta_context(), id)
            .await
            .map_err(|error| map_rpc_error("retire snapshot in metadata directory", error))?
            .map_err(map_meta_error)
    }
}

fn sort_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(sort_json).collect())
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().map(|(k, v)| (k, sort_json(v))).collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            serde_json::Value::Object(entries.into_iter().collect())
        }
        other => other,
    }
}

fn map_meta_error(error: MetaError) -> RepositoryError {
    match error {
        MetaError::InvalidRequest { reason } => RepositoryError::InvalidRequest { reason },
        MetaError::SnapshotConflict { id } => RepositoryError::InvalidRequest {
            reason: format!("snapshot '{id}' is already registered with different content"),
        },
        MetaError::AliasConflict {
            alias,
            existing,
            new_id,
        } => RepositoryError::AliasConflict {
            alias,
            existing,
            new_id,
        },
        MetaError::Unavailable { reason } => RepositoryError::Unavailable { reason },
        MetaError::Backend { message } => RepositoryError::Backend {
            message,
            source: None,
        },
    }
}

fn map_rpc_error(operation: &str, error: RpcError) -> RepositoryError {
    RepositoryError::backend(operation, error)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use agentenv_metadata::{transport, DatabaseConfig, MetaServer};
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::layer::LayerSource;
    use crate::repository::SnapshotFile;
    use crate::snapshot::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::{
        Snapshot, SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotManager,
        SnapshotPlacement, SnapshotPublishMetadata, SnapshotPublishSource,
        SNAPSHOT_ARTIFACT_LAYOUT,
    };

    fn built_capture(root: &Path, id: SnapshotId) -> Arc<Snapshot> {
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
        crate::snapshot::local::finish_snapshot(&mut manifest, dir.path()).unwrap();
        Arc::new(Snapshot::new(id, manifest, dir))
    }

    fn node_manager(root: &Path, node_id: &str, meta: MetaClient) -> SnapshotManager {
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join(format!("cache-{node_id}"))),
            runtime_cache_root: Some(root.join(format!("cache-{node_id}/runtime"))),
        })
        .unwrap();
        let (_, repository, resolver) = backend.into_parts();
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
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sbx-local".into(),
                source_sandbox_metadata: None,
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
        assert!(snapshot_dir
            .join(SnapshotFile::CommittedManifest.name())
            .is_file());

        let promoted = node_a
            .promote(&local_id.to_string())
            .await?
            .expect("retry promote returns committed Distributed record");
        assert_eq!(promoted.snapshot_type, SnapshotPlacement::Distributed);

        let resolved_on_b = node_b
            .load_runnable(local_id.to_string())
            .await?
            .expect("node B resolves promoted snapshot");
        assert!(resolved_on_b.manifest().vm_state.path.is_file());

        // Direct Distributed publish, alias conflict rollback, list, and retirement.
        let dist_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse(&format!("snap-{dist_id}"))?;
        let dist_capture = built_capture(root.path(), dist_id.clone());
        let trace_path = root.path().join("startup.trace");
        std::fs::write(
            &trace_path,
            overlaybd::startup_pack::encode_trace(
                dist_capture.manifest().memory.virtual_size,
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
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sbx-dist".into(),
                source_sandbox_metadata: None,
            },
            ..SnapshotPublishMetadata::mock()
        };
        node_a
            .publish_snapshot(dist_capture, dist_meta.clone())
            .await?;

        let distributed = node_b.get(dist_id.to_string()).await?.unwrap();
        assert!(distributed
            .committed
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
        let key = BlobKey::Snapshot(dist_id.clone(), SnapshotFile::CommittedManifest);
        let manifest = repository.read_blob(&key).await?;
        repository
            .put_blob(&key, BlobBody::Bytes(manifest.clone()))
            .await?;
        assert!(matches!(
            repository
                .put_blob(&key, BlobBody::Bytes(b"different".to_vec()))
                .await,
            Err(RepositoryError::InvalidRequest { .. })
        ));
        assert_eq!(repository.read_blob(&key).await?, manifest);

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
        assert!(resolved_on_b.manifest().vm_state.path.is_file());

        let _ = stop.send(());
        serving.await??;
        server.close().await;
        Ok(())
    }
}
