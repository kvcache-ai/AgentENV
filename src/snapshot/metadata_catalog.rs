//! Snapshot directory backed by `agentenv-metadata` (`MetaClient`) and an artifact [`Repository`].

use std::{collections::HashSet, path::Path, sync::Arc};

use agentenv_domain::SnapshotMetadata;
use agentenv_metadata::{
    context::current as meta_context, MetaClient, MetaError, MetaServer, RpcError,
};
use anyhow::Context;
use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};

use super::catalog::{self, RepositorySnapshotExt, SnapshotCatalog, SnapshotListFilter};
use super::types::now_unix_ms;
use super::{
    SnapshotAlias, SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotPublishMetadata,
    SnapshotRecord, SnapshotSource,
};
use crate::repository::{
    BlobBody, BlobKey, Repository, RepositoryError, RepositoryResult, SnapshotFile,
};

const LIST_RECORD_CONCURRENCY: usize = 8;

/// Owns the Node or operator tool's metadata connection.
pub struct MetadataHandle {
    client: MetaClient,
    embedded: Option<MetaServer>,
}

impl MetadataHandle {
    pub async fn open_node(config: &crate::cfg::DatabaseConfig) -> anyhow::Result<Self> {
        if config.embedded_sqlite_path().is_some() {
            config.to_metadata_config()?.migrate().await?;
        }
        Self::open(config).await
    }

    pub async fn open(config: &crate::cfg::DatabaseConfig) -> anyhow::Result<Self> {
        if let Some(address) = config.meta_address.as_deref() {
            let client = agentenv_metadata::transport::connect(address)
                .await
                .with_context(|| format!("connect to metadata server at '{address}'"))?;
            client
                .health(meta_context())
                .await
                .context("check metadata server health")??;
            return Ok(Self {
                client,
                embedded: None,
            });
        }
        let server = MetaServer::open(&config.to_metadata_config()?).await?;
        let client = agentenv_metadata::transport::embedded(server.clone());
        Ok(Self {
            client,
            embedded: Some(server),
        })
    }

    pub fn client(&self) -> MetaClient {
        self.client.clone()
    }

    pub async fn close(self) {
        drop(self.client);
        if let Some(server) = self.embedded {
            server.close().await;
        }
    }
}

/// SQL directory for sandbox snapshots, with repository-backed template
/// builds and compatibility reads for earlier snapshots.
#[derive(Clone)]
pub struct MetadataCatalog {
    meta: MetaClient,
    repository: Arc<dyn Repository>,
}

impl MetadataCatalog {
    pub fn new(meta: MetaClient, repository: Arc<dyn Repository>) -> Self {
        Self { meta, repository }
    }

    fn repository(&self) -> &dyn Repository {
        self.repository.as_ref()
    }

    async fn check_sql_alias(
        &self,
        alias: &SnapshotAlias,
        id: &SnapshotId,
    ) -> RepositoryResult<()> {
        if let Some(entry) = self.fetch_entry(alias.as_ref()).await? {
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

    async fn write_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let value = record
            .artifact_json()
            .map_err(|error| RepositoryError::backend("serialize snapshot record", error))?;
        let bytes = serde_json::to_vec_pretty(&value)
            .map_err(|error| RepositoryError::backend("encode snapshot record", error))?;
        self.repository
            .put_blob(
                &BlobKey::Snapshot(record.id.clone(), SnapshotFile::Record),
                BlobBody::Bytes(bytes),
            )
            .await
    }

    async fn assemble_record(&self, entry: SnapshotMetadata) -> RepositoryResult<SnapshotRecord> {
        let mut record = super::store::read_snapshot_record(self.repository(), entry.id())
            .await?
            .ok_or_else(|| RepositoryError::Unavailable {
                reason: format!("snapshot record '{}' is missing", entry.id),
            })?;
        if record.id != entry.id {
            return Err(RepositoryError::Unavailable {
                reason: format!("snapshot record '{}' has the wrong ID", entry.id),
            });
        }
        record.snapshot_type =
            entry
                .snapshot_placement()
                .map_err(|error| RepositoryError::Unavailable {
                    reason: error.to_string(),
                })?;
        // A promotion can prepare its published inventory before the SQL commit.
        if record.snapshot_type == SnapshotPlacement::Local {
            record.manifest = None;
        }
        record.owner_node_id = entry.owner_node_id;
        record.alias = entry.alias;
        record.created_at_unix_ms = entry.created_at_unix_ms;
        record.updated_at_unix_ms = entry.updated_at_unix_ms;
        record.deleting = entry.retired;
        Ok(record)
    }

    async fn commit_entry(
        &self,
        record: &SnapshotRecord,
        manifest: &SnapshotManifest,
        promoting: bool,
        restored: Vec<String>,
    ) -> Result<SnapshotMetadata, MetaError> {
        if promoting {
            self.meta
                .promote_snapshot(meta_context(), record.into(), manifest.clone(), restored)
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
                .register_snapshot(meta_context(), record.into(), manifest.clone(), restored)
                .await
                .map_err(|error| MetaError::Backend {
                    message: format!("register snapshot: {error}"),
                })?
        }
    }

    async fn fetch_entry(&self, reference: &str) -> RepositoryResult<Option<SnapshotMetadata>> {
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

    fn owns_snapshot_layers(&self) -> bool {
        true
    }

    async fn tracks_snapshot(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        Ok(self
            .fetch_entry(&id.to_string())
            .await?
            .is_some_and(|entry| &entry.id == id))
    }

    async fn pin(
        &self,
        id: &SnapshotId,
        pin_id: &str,
    ) -> RepositoryResult<Option<SnapshotMetadata>> {
        self.meta
            .pin_snapshot(meta_context(), id.clone(), pin_id.to_owned())
            .await
            .map_err(|error| map_rpc_error("pin snapshot", error))?
            .map_err(map_meta_error)
    }

    async fn release_pin(&self, id: &SnapshotId, pin_id: &str) -> RepositoryResult<()> {
        self.meta
            .release_pin(meta_context(), id.clone(), pin_id.to_owned())
            .await
            .map_err(|error| map_rpc_error("release snapshot pin", error))?
            .map_err(map_meta_error)
    }

    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        if let Some(alias) = &record.alias {
            self.check_sql_alias(alias, &record.id).await?;
        }
        self.repository.create(record).await
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        SnapshotAlias::parse(alias).map_err(|error| RepositoryError::InvalidRequest {
            reason: error.to_string(),
        })?;
        Ok(self.get(alias).await?.map(|record| record.id))
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        self.repository.try_start_build(id).await
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: super::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        self.repository.mark_build_error(id, reason).await
    }

    async fn prepare_distributed(
        &self,
        id: &SnapshotId,
        alias: Option<&SnapshotAlias>,
    ) -> RepositoryResult<()> {
        if let Some(entry) = self
            .fetch_entry(&id.to_string())
            .await?
            .filter(|entry| entry.id() == id)
        {
            if !entry.is_visible() {
                return Err(RepositoryError::Unavailable {
                    reason: format!("snapshot '{id}' is retired"),
                });
            }
            if entry.snapshot_type == Some(SnapshotPlacement::Distributed) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{id}' is already committed"),
                });
            }
        }
        if let Some(alias) = alias {
            self.check_sql_alias(alias, id).await?;
            catalog::ensure_alias_available(self.repository(), alias, id).await?;
        }
        super::publish::ensure_not_committed(self.repository(), id).await?;
        Ok(())
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
        self.write_record(&record).await?;
        let entry = self
            .meta
            .register_snapshot(
                meta_context(),
                (&record).into(),
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
        if matches!(metadata.config.source, SnapshotSource::Template { .. }) {
            return catalog::commit(self.repository(), metadata, manifest).await;
        }
        let id = metadata.id.clone();
        let alias = metadata.alias.clone();
        let previous = self
            .fetch_entry(&id.to_string())
            .await?
            .filter(|entry| entry.id == id);
        if let Some(entry) = &previous {
            if !entry.is_visible() {
                return Err(RepositoryError::Unavailable {
                    reason: format!("snapshot '{id}' is retired"),
                });
            }
            if entry.snapshot_type == Some(SnapshotPlacement::Distributed) {
                if entry.alias != alias {
                    return Err(map_meta_error(MetaError::SnapshotConflict { id }));
                }
                return self.assemble_record(entry.clone()).await;
            }
        }
        let promoting = previous.is_some();
        let now = now_unix_ms();
        let prepared = super::store::read_snapshot_record(self.repository(), &id).await?;
        let record = if let Some(mut record) = prepared {
            if record.snapshot_type == SnapshotPlacement::Distributed {
                if record.alias != alias {
                    return Err(map_meta_error(MetaError::SnapshotConflict { id }));
                }
                // The artifact bytes and their timestamps are reused after an uncertain promotion.
            } else {
                record.mark_committed(alias, manifest, now);
            }
            record
        } else {
            SnapshotRecord::new(
                id.clone(),
                SnapshotPlacement::Distributed,
                alias,
                metadata.config(now),
                Some(manifest),
                now,
            )
        };
        record
            .validate()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        let manifest = record
            .manifest
            .as_ref()
            .expect("Distributed publication was validated")
            .clone();
        self.write_record(&record).await?;
        if let Some(alias) = &record.alias {
            catalog::claim_alias(self.repository(), alias, &record.id).await?;
        }

        let entry = match self
            .commit_entry(&record, &manifest, promoting, Vec::new())
            .await
        {
            Err(MetaError::MissingLayers { uuids }) if manifest.isolated_layers => {
                for layer in managed_layers(&manifest) {
                    if layer.uuid.as_ref().is_some_and(|uuid| uuids.contains(uuid)) {
                        let key = BlobKey::layer(
                            layer.digest.clone(),
                            crate::repository::LayerRole::Rootfs,
                            true,
                        );
                        if !self.repository.has_blob(&key).await? {
                            let (_, source) = local_layers
                                .iter()
                                .find(|(digest, _)| *digest == layer.digest)
                                .ok_or_else(|| RepositoryError::ManagedLayerNotFound {
                                    digest: layer.digest.clone(),
                                })?;
                            self.repository
                                .put_blob(&key, BlobBody::File(source))
                                .await?;
                        }
                    }
                }
                self.commit_entry(&record, &manifest, promoting, uuids)
                    .await
            }
            result => result,
        }
        .map_err(map_meta_error)?;
        self.assemble_record(entry).await
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        let Some(entry) = self.fetch_entry(id_or_alias).await? else {
            return self.repository.get(id_or_alias).await;
        };
        if !entry.is_visible() {
            return Ok(None);
        }
        self.assemble_record(entry).await.map(Some)
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let entries = self
            .meta
            .list_snapshots(meta_context(), filter.clone())
            .await
            .map_err(|error| map_rpc_error("list snapshots from metadata directory", error))?
            .map_err(map_meta_error)?;
        let mut records: Vec<_> = stream::iter(entries)
            .map(|entry| self.assemble_record(entry))
            .buffered(LIST_RECORD_CONCURRENCY)
            .try_collect()
            .await?;
        records.retain(|record| filter.matches(record));
        let sql_ids: HashSet<_> = records.iter().map(|record| record.id.clone()).collect();
        let legacy = self.repository.list(filter).await?;
        let legacy = stream::iter(
            legacy
                .into_iter()
                .filter(|record| !sql_ids.contains(&record.id)),
        )
        .map(|record| async move {
            Ok::<_, RepositoryError>(
                self.fetch_entry(&record.id.to_string())
                    .await?
                    .is_none()
                    .then_some(record),
            )
        })
        .buffered(LIST_RECORD_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
        records.extend(legacy.into_iter().flatten());
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(records)
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        let id = match self.fetch_entry(id_or_alias).await? {
            Some(entry) => entry.id,
            None => {
                self.repository.delete(id_or_alias).await?;
                let Ok(id) = SnapshotId::parse(id_or_alias) else {
                    return Ok(());
                };
                id
            }
        };
        self.meta
            .retire_snapshot(meta_context(), id)
            .await
            .map_err(|error| map_rpc_error("retire snapshot in metadata directory", error))?
            .map_err(map_meta_error)
    }
}

fn managed_layers(manifest: &SnapshotManifest) -> impl Iterator<Item = &super::ManagedLayer> {
    manifest.memory_layers.iter().chain(
        manifest
            .rootfs_layers
            .iter()
            .chain(manifest.attached_drives.iter().flat_map(|drive| {
                let super::CommittedAttachedDrive::Overlaybd { layers, .. } = drive;
                layers
            }))
            .filter_map(|layer| match layer {
                super::OverlaybdLayerRef::Managed(layer) => Some(layer),
                super::OverlaybdLayerRef::External(_) => None,
            }),
    )
}

pub(super) fn map_meta_error(error: MetaError) -> RepositoryError {
    match error {
        MetaError::InvalidRequest { reason } => RepositoryError::InvalidRequest { reason },
        conflict @ MetaError::SnapshotConflict { .. } => RepositoryError::InvalidRequest {
            reason: conflict.to_string(),
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
        error @ MetaError::MissingLayers { .. } => RepositoryError::Unavailable {
            reason: error.to_string(),
        },
        MetaError::Unavailable { reason } => RepositoryError::Unavailable { reason },
        MetaError::Backend { message } => RepositoryError::Backend {
            message,
            source: None,
        },
    }
}

pub(super) fn map_rpc_error(operation: &str, error: RpcError) -> RepositoryError {
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
    use crate::repository::{LayerRole, SnapshotFile};
    use crate::snapshot::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::{
        run_gc_once, ManagedLayer, SnapshotAlias, SnapshotConfig, SnapshotHandle, SnapshotId,
        SnapshotListFilter, SnapshotManager, SnapshotPlacement, SnapshotPublishMetadata,
        SnapshotPublishSource, SNAPSHOT_ARTIFACT_LAYOUT,
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
        crate::snapshot::local::finish_snapshot(&mut manifest, dir.path()).unwrap();
        Arc::new(SnapshotHandle::new(id, manifest, dir))
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
            config: crate::snapshot::SnapshotConfig {
                source: SnapshotPublishSource::Sandbox {
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
                source: SnapshotPublishSource::Sandbox {
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
        let stored: SnapshotRecord = serde_json::from_slice(&repository.read_blob(&key).await?)?;
        assert_eq!(stored.id, dist_id);
        assert!(stored.manifest.is_some());

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
    async fn publication_reuploads_a_layer_collected_before_registration() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let database = DatabaseConfig::Sqlite {
            path: root.path().join("metadata.sqlite3"),
        };
        database.migrate().await?;
        let server = MetaServer::open(&database).await?;
        let meta = transport::embedded(server.clone());
        let repo: Arc<dyn Repository> = Arc::new(
            crate::repository::backends::PosixFsRepository::new(root.path().join("repository")),
        );
        let catalog = MetadataCatalog::new(meta.clone(), Arc::clone(&repo));
        let source = root.path().join("captured.commit");
        let bytes = b"captured layer";
        std::fs::write(&source, bytes)?;
        let digest = crate::digest::sha256_digest(bytes);
        let manifest = SnapshotManifest {
            isolated_layers: true,
            memory_layers: vec![ManagedLayer {
                digest: digest.clone(),
                size: bytes.len() as u64,
                uuid: Some(uuid::Uuid::now_v7().to_string()),
            }],
            ..SnapshotManifest::default()
        };
        let make_metadata = || SnapshotPublishMetadata {
            config: SnapshotConfig {
                source: SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "source-sandbox".into(),
                    source_sandbox_metadata: None,
                },
                ..SnapshotConfig::mock()
            },
            ..SnapshotPublishMetadata::mock()
        };
        let key = BlobKey::layer(digest.clone(), LayerRole::Memory, true);
        repo.put_blob(&key, BlobBody::File(&source)).await?;
        let first = catalog
            .commit_distributed(&make_metadata(), manifest.clone(), &[])
            .await?;
        catalog.delete(&first.id.to_string()).await?;
        assert_eq!(
            run_gc_once(&meta, repo.as_ref(), 64).await?.deleted_layers,
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
