//! Snapshot directory backed by `agentenv-metadata` (`MetaClient`) and an artifact [`Repository`].

use std::{collections::HashSet, path::Path, sync::Arc};

use agentenv_domain::{SnapshotContent, SnapshotMetadata, SnapshotSourceKind};
use agentenv_metadata::{
    context::current as meta_context, MetaClient, MetaError, MetaServer, RpcError,
};
use anyhow::Context;
use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};

use super::file_catalog::{self, RepositorySnapshotExt};
use super::now_unix_ms;
use super::{
    SnapshotAlias, SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotPublishMetadata,
    SnapshotRecord, SnapshotSource, TemplateBuildErrorReason,
};
use crate::repository::{
    BlobBody, BlobKey, DocumentKey, Repository, RepositoryError, RepositoryResult, SnapshotFile,
};

pub use agentenv_domain::SnapshotListFilter;

const LIST_RECORD_CONCURRENCY: usize = 8;

/// Snapshot directory, independent of artifact storage and runtime resolution.
#[async_trait]
pub trait SnapshotCatalog: Send + Sync {
    /// Immutable manifests must include startup metadata before the catalog commit.
    fn committed_content_is_immutable(&self) -> bool {
        false
    }

    async fn prepare_distributed(
        &self,
        id: &SnapshotId,
        alias: Option<&SnapshotAlias>,
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
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord>;
    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>>;
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord>;
    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()>;
    async fn pin(&self, _id: &SnapshotId) -> RepositoryResult<()> {
        Ok(())
    }
    fn unpin(&self, _id: &SnapshotId) {}
    fn release_orphan_pins(&self) {}
}

/// Owns the Node or operator tool's metadata connection.
pub struct MetadataHandle {
    client: MetaClient,
    embedded: Option<MetaServer>,
}

impl MetadataHandle {
    pub async fn open_node(config: &crate::cfg::AppConfig) -> anyhow::Result<Self> {
        if config.database.embedded_sqlite_path().is_some() {
            config.database.to_metadata_config()?.migrate().await?;
        }
        Self::open(config).await
    }

    pub async fn open(config: &crate::cfg::AppConfig) -> anyhow::Result<Self> {
        if let Some(address) = config.database.meta_address.as_deref() {
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
        let storage = super::SnapshotGcStorage::from_config(config)?;
        let server = MetaServer::open(&config.database.to_metadata_config()?)
            .await?
            .with_gc_storage(Arc::new(storage));
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

/// SQL directory for sandbox snapshots, with repository-backed template builds
/// and optional node-local pin tracking (`pins.db`).
#[derive(Clone)]
pub struct MetadataCatalog {
    meta: MetaClient,
    repository: Arc<dyn Repository>,
    pins: Option<Arc<super::pins::SnapshotPins>>,
}

fn is_template(record: &SnapshotRecord) -> bool {
    matches!(record.config.source, SnapshotSource::Template { .. })
}

fn includes_templates(filter: &SnapshotListFilter) -> bool {
    filter
        .sources
        .as_ref()
        .is_none_or(|sources| sources.contains(&SnapshotSourceKind::Template))
}

impl MetadataCatalog {
    pub fn new(meta: MetaClient, repository: Arc<dyn Repository>) -> Self {
        Self {
            meta,
            repository,
            pins: None,
        }
    }

    pub(crate) fn with_pins(mut self, pins: Arc<super::pins::SnapshotPins>) -> Self {
        self.pins = Some(pins);
        self
    }

    fn repository(&self) -> &dyn Repository {
        self.repository.as_ref()
    }

    async fn read_template(&self, reference: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(self.repository.get(reference).await?.filter(is_template))
    }

    async fn check_sql_alias(
        &self,
        alias: &SnapshotAlias,
        id: &SnapshotId,
    ) -> RepositoryResult<()> {
        if let Some(entry) = self.fetch_entry(alias.as_ref()).await? {
            if &entry.id != id && entry.alias.as_ref() == Some(alias) {
                if entry.is_visible() {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing: entry.id,
                        new_id: id.clone(),
                    });
                }
                // A prior release reserved aliases even after retirement.
                self.retire(&entry.id).await?;
            }
        }
        Ok(())
    }

    async fn check_template_alias(
        &self,
        alias: &SnapshotAlias,
        id: &SnapshotId,
    ) -> RepositoryResult<()> {
        if let Some(template) = self.read_template(alias.as_ref()).await? {
            if &template.id != id {
                return Err(RepositoryError::AliasConflict {
                    alias: alias.to_string(),
                    existing: template.id,
                    new_id: id.clone(),
                });
            }
        }
        Ok(())
    }

    async fn ensure_template_not_committed(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let Some(bytes) = self
            .repository
            .get_document(DocumentKey::SnapshotRecord(id.clone()))
            .await?
        else {
            return Ok(());
        };
        let record: SnapshotRecord = serde_json::from_slice(&bytes).map_err(|error| {
            RepositoryError::backend(format!("parse snapshot record '{id}'"), error)
        })?;
        if is_template(&record)
            && (record.deleting
                || (record.manifest.is_some() && record.snapshot_type != SnapshotPlacement::Local))
        {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is already committed"),
            });
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
            deleting: entry.retired,
        })
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

    async fn entry_for_id(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotMetadata>> {
        Ok(self
            .fetch_entry(&id.to_string())
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
                for layer in managed_layers(manifest) {
                    let uuid = layer
                        .directory_uuid()
                        .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
                    if uuids.contains(&uuid.to_string()) {
                        let key = BlobKey::Layer {
                            digest: layer.digest.clone(),
                            role: crate::repository::LayerRole::Rootfs,
                        };
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
                self.commit_entry(record, manifest, promoting, uuids).await
            }
            result => result,
        }
        .map_err(map_meta_error)
    }
}

#[async_trait]
impl SnapshotCatalog for MetadataCatalog {
    fn committed_content_is_immutable(&self) -> bool {
        true
    }

    async fn get_for_promotion(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        let Some(entry) = self.entry_for_id(id).await? else {
            return Ok(None);
        };
        if !entry.is_visible() {
            return Ok(None);
        }
        self.assemble_record(entry).await.map(Some)
    }

    async fn pin(&self, id: &SnapshotId) -> RepositoryResult<()> {
        if let Some(pins) = &self.pins {
            pins.pin(&self.meta, id).await?;
        }
        Ok(())
    }

    fn unpin(&self, id: &SnapshotId) {
        if let Some(pins) = &self.pins {
            pins.unpin(&self.meta, id);
        }
    }

    fn release_orphan_pins(&self) {
        if let Some(pins) = &self.pins {
            pins.release_orphans(&self.meta);
        }
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
        Ok(self.get_metadata(alias).await?.map(|entry| entry.id))
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
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        if let Some(entry) = self.entry_for_id(id).await? {
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
            self.check_template_alias(alias, id).await?;
        }
        self.ensure_template_not_committed(id).await?;
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
            if let Some(alias) = &metadata.alias {
                self.check_sql_alias(alias, &metadata.id).await?;
            }
            return file_catalog::commit(self.repository(), metadata, manifest).await;
        }
        let id = metadata.id.clone();
        let alias = metadata.alias.clone();
        let previous = self.entry_for_id(&id).await?;
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
        if let Some(alias) = &alias {
            self.check_template_alias(alias, &id).await?;
        }
        let promoting = previous.is_some();
        let now = now_unix_ms();
        let config = if promoting {
            super::store::read_snapshot_content(self.repository(), &id)
                .await?
                .map(|content| content.config)
                .unwrap_or_else(|| metadata.config(now))
        } else {
            metadata.config(now)
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
        if let Some(entry) = self.fetch_entry(id_or_alias).await? {
            return if entry.is_visible() {
                self.assemble_record(entry).await.map(Some)
            } else {
                Ok(None)
            };
        }
        self.read_template(id_or_alias).await
    }

    async fn get_metadata(&self, reference: &str) -> RepositoryResult<Option<SnapshotMetadata>> {
        if let Some(entry) = self.fetch_entry(reference).await? {
            return Ok(entry.is_visible().then_some(entry));
        }
        Ok(self
            .read_template(reference)
            .await?
            .as_ref()
            .map(SnapshotMetadata::from))
    }

    async fn list_metadata(
        &self,
        filter: SnapshotListFilter,
    ) -> RepositoryResult<Vec<SnapshotMetadata>> {
        let mut metadata = self.list_sql_entries(filter.clone()).await?;
        if includes_templates(&filter) {
            let sql_ids: HashSet<_> = metadata.iter().map(|entry| entry.id.clone()).collect();
            for record in self.repository.list(filter).await? {
                if is_template(&record) && !sql_ids.contains(&record.id) {
                    metadata.push(SnapshotMetadata::from(&record));
                }
            }
        }
        metadata.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(metadata)
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let sql = self.list_sql_entries(filter.clone()).await?;
        let sql_ids: HashSet<_> = sql.iter().map(|entry| entry.id.clone()).collect();
        let mut records: Vec<_> = stream::iter(sql)
            .map(|entry| async move { self.assemble_record(entry).await })
            .buffered(LIST_RECORD_CONCURRENCY)
            .try_collect()
            .await?;
        if includes_templates(&filter) {
            for record in self.repository.list(filter.clone()).await? {
                if is_template(&record) && !sql_ids.contains(&record.id) {
                    records.push(record);
                }
            }
        }
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
        if let Some(entry) = self.fetch_entry(id_or_alias).await? {
            return self.retire(&entry.id).await;
        }
        if let Some(template) = self.read_template(id_or_alias).await? {
            return self.repository.delete(&template.id.to_string()).await;
        }
        if let Ok(id) = SnapshotId::parse(id_or_alias) {
            return self.retire(&id).await;
        }
        Ok(())
    }
}

fn managed_layers(manifest: &SnapshotManifest) -> impl Iterator<Item = &super::ManagedLayerRef> {
    manifest.memory_layers.iter().chain(
        manifest
            .rootfs_layers
            .iter()
            .chain(manifest.attached_drives.iter().flat_map(|drive| {
                let super::CommittedAttachedDrive::Overlaybd { layers, .. } = drive;
                layers
            }))
            .filter_map(|layer| match layer {
                super::LayerRef::Managed(layer) => Some(layer),
                super::LayerRef::External(_) => None,
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
        // Sandbox commits are SQL-only and never mirror to catalog/records or catalog/aliases.
        assert!(repository.get(alias.as_ref()).await?.is_none());
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
                crate::snapshot::pins::SnapshotPins::open(root.path(), "test-node".into()).await?,
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
            .register_snapshot((&owner).into(), SnapshotManifest::default(), &[])
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
        // Failed SQL registration must not write legacy file-catalog entries or become visible.
        assert!(repo
            .get_document(DocumentKey::SnapshotRecord(metadata.id.clone()))
            .await?
            .is_none());
        assert!(repo
            .get_document(DocumentKey::SnapshotAlias(alias.clone()))
            .await?
            .is_none());
        assert!(catalog.get(&metadata.id.to_string()).await?.is_none());
        assert!(server
            .get_snapshot(&metadata.id.to_string())
            .await?
            .is_none());

        server.retire_snapshot(owner.id).await?;
        assert!(catalog
            .prepare_distributed(&metadata.id, Some(&alias))
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
    async fn unimported_legacy_sandbox_files_are_ignored_while_templates_stay_visible(
    ) -> anyhow::Result<()> {
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
        let catalog =
            MetadataCatalog::new(meta.clone(), Arc::clone(&repository)).with_pins(Arc::new(
                crate::snapshot::pins::SnapshotPins::open(root.path(), "test-node".into()).await?,
            ));

        // 1. SQL-only sandbox snapshot with content blob + second SQL entry without content blob.
        let mut sql_record = SnapshotRecord::mock_ready(SnapshotManifest::default());
        sql_record.config.source = SnapshotSource::Sandbox {
            source_sandbox_id: "sql-sandbox".to_owned(),
            source_sandbox_metadata: None,
        };
        let sql_alias = SnapshotAlias::parse("sql-snap")?;
        sql_record.alias = Some(sql_alias.clone());
        let sql_id = sql_record.id.clone();
        let key = BlobKey::Snapshot(sql_id.clone(), SnapshotFile::Record);
        let original = serde_json::to_vec(&SnapshotContent::from(&sql_record))?;
        repository
            .put_blob(&key, BlobBody::Bytes(original.clone()))
            .await?;
        server
            .register_snapshot(
                (&sql_record).into(),
                sql_record.manifest.clone().unwrap(),
                &[],
            )
            .await?;

        let other_sql = SnapshotRecord {
            id: SnapshotId::generate(),
            alias: None,
            ..sql_record.clone()
        };
        server
            .register_snapshot(
                (&other_sql).into(),
                other_sql.manifest.clone().unwrap(),
                &[],
            )
            .await?;

        // 2. Un-imported legacy sandbox snapshot written directly to the repository file catalog.
        let legacy_alias = SnapshotAlias::parse("legacy-sandbox")?;
        let legacy_meta = SnapshotPublishMetadata {
            id: SnapshotId::generate(),
            alias: Some(legacy_alias.clone()),
            snapshot_type: SnapshotPlacement::Distributed,
            config: SnapshotConfig {
                source: SnapshotSource::Sandbox {
                    source_sandbox_id: "legacy-sandbox-src".into(),
                    source_sandbox_metadata: None,
                },
                ..SnapshotConfig::mock()
            },
        };
        let legacy_sandbox =
            file_catalog::commit(repository.as_ref(), &legacy_meta, SnapshotManifest::mock())
                .await?;

        // 3. Template build stored in the repository file catalog.
        let template_alias = SnapshotAlias::parse("template-snap")?;
        let template_id = SnapshotId::generate();
        let template_waiting = SnapshotRecord::template_waiting(
            template_id.clone(),
            Some(template_alias.clone()),
            Default::default(),
        );
        catalog.create(template_waiting).await?;
        let template_meta = SnapshotPublishMetadata {
            id: template_id.clone(),
            alias: Some(template_alias.clone()),
            snapshot_type: SnapshotPlacement::Distributed,
            config: SnapshotConfig {
                source: SnapshotSource::template_ready(),
                ..SnapshotConfig::mock()
            },
        };
        let template_record = catalog
            .commit_distributed(&template_meta, SnapshotManifest::mock(), &[])
            .await?;

        // SQL sandbox and Template are visible; un-imported legacy sandbox is invisible.
        assert_eq!(catalog.get(sql_alias.as_ref()).await?.unwrap().id, sql_id);
        assert_eq!(catalog.get(&sql_id.to_string()).await?.unwrap().id, sql_id);
        catalog.pin(&sql_id).await?;
        catalog.unpin(&sql_id);

        assert!(catalog.get(legacy_alias.as_ref()).await?.is_none());
        assert!(catalog.get(&legacy_sandbox.id.to_string()).await?.is_none());
        assert!(catalog.get_metadata(legacy_alias.as_ref()).await?.is_none());
        assert!(catalog
            .resolve_alias(legacy_alias.as_ref())
            .await?
            .is_none());
        // Legacy sandbox alias does not block a new SQL snapshot.
        assert!(catalog
            .prepare_distributed(&SnapshotId::generate(), Some(&legacy_alias))
            .await?
            .is_none());
        // Deleting by legacy alias does not touch un-imported legacy files.
        catalog.delete(legacy_alias.as_ref()).await?;
        assert!(repository.get(legacy_alias.as_ref()).await?.is_some());

        assert_eq!(
            catalog.get(template_alias.as_ref()).await?.unwrap().id,
            template_id
        );
        assert_eq!(
            catalog.resolve_alias(template_alias.as_ref()).await?,
            Some(template_id.clone())
        );
        // Pinning a non-SQL template record is a safe no-op.
        catalog.pin(&template_record.id).await?;
        catalog.unpin(&template_record.id);
        assert!(matches!(
            catalog
                .prepare_distributed(&SnapshotId::generate(), Some(&template_alias))
                .await,
            Err(RepositoryError::AliasConflict { existing, .. }) if existing == template_id
        ));

        // list_metadata returns SQL sandboxes + templates (without needing content blobs) and excludes legacy sandboxes.
        let candidates = catalog.list_metadata(SnapshotListFilter::default()).await?;
        let ids: HashSet<_> = candidates.iter().map(|entry| entry.id.clone()).collect();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&sql_id));
        assert!(ids.contains(&other_sql.id));
        assert!(ids.contains(&template_id));
        assert!(!ids.contains(&legacy_sandbox.id));

        assert_eq!(
            catalog
                .list(SnapshotListFilter::by_ids([
                    sql_id.clone(),
                    legacy_sandbox.id.clone(),
                    template_id.clone(),
                ]))
                .await?
                .into_iter()
                .map(|record| record.id)
                .collect::<HashSet<_>>(),
            HashSet::from([sql_id.clone(), template_id.clone()])
        );

        // Deleting the template removes it from the file catalog.
        catalog.delete(template_alias.as_ref()).await?;
        assert!(catalog.get(template_alias.as_ref()).await?.is_none());
        assert_eq!(repository.read_blob(&key).await?, original);
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
