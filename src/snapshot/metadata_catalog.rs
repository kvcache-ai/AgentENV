//! Snapshot directory backed by `agentenv-metadata` (`MetaClient`) and an artifact [`Repository`].

use std::{collections::HashMap, path::Path, sync::Arc};

use agentenv_domain::{SnapshotContent, SnapshotMetadata};
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
        let server = MetaServer::open(&config.database.to_metadata_config()?).await?;
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
    read_from_sql: bool,
}

enum DirectoryRecord {
    Sql(SnapshotMetadata),
    Legacy(Box<SnapshotRecord>),
}

impl DirectoryRecord {
    fn into_metadata(self) -> SnapshotMetadata {
        match self {
            Self::Sql(metadata) => metadata,
            Self::Legacy(record) => SnapshotMetadata::from(record.as_ref()),
        }
    }
}

impl MetadataCatalog {
    pub fn new(meta: MetaClient, repository: Arc<dyn Repository>) -> Self {
        Self {
            meta,
            repository,
            read_from_sql: true,
        }
    }

    pub(super) fn with_read_from_sql(mut self, read_from_sql: bool) -> Self {
        self.read_from_sql = read_from_sql;
        self
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
        if let Some(owner) = catalog::read_alias(self.repository(), alias).await? {
            if &owner != id
                && self
                    .entry_for_id(&owner)
                    .await?
                    .is_some_and(|entry| entry.retired)
            {
                catalog::remove_mirror(self.repository(), &owner, Some(alias)).await?;
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
        let content =
            match super::store::read_snapshot_content(self.repository(), entry.id()).await? {
                Some(content) => content,
                None => {
                    let legacy = catalog::read_record(self.repository(), entry.id())
                        .await?
                        .ok_or_else(|| RepositoryError::Unavailable {
                            reason: format!("snapshot content '{}' is missing", entry.id),
                        })?;
                    SnapshotContent::from(&legacy)
                }
            };
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

    async fn retire(&self, id: &SnapshotId) -> RepositoryResult<()> {
        self.meta
            .retire_snapshot(meta_context(), id.clone())
            .await
            .map_err(|error| map_rpc_error("retire snapshot in metadata directory", error))?
            .map_err(map_meta_error)
    }

    fn prefer_sql(&self, entry: &SnapshotMetadata, file: &SnapshotRecord) -> bool {
        // A file-first promotion may still have a Local SQL row.
        self.read_from_sql
            || (entry != &SnapshotMetadata::from(file)
                && !(entry.snapshot_type == Some(SnapshotPlacement::Local)
                    && file.snapshot_type == SnapshotPlacement::Distributed))
    }

    async fn read_entry(&self, reference: &str) -> RepositoryResult<Option<DirectoryRecord>> {
        let sql = self.fetch_entry(reference).await?;
        if let Some(entry) = &sql {
            if !entry.is_visible() {
                return Ok(None);
            }
            if self.read_from_sql {
                return Ok(sql.map(DirectoryRecord::Sql));
            }
        }
        if let Some(record) = self.repository.get(reference).await? {
            if sql.as_ref().is_some_and(|entry| entry.id != record.id) {
                // A file-first claim can still lose to an existing SQL-only alias.
                return Ok(sql.map(DirectoryRecord::Sql));
            }
            let entry = match &sql {
                Some(entry) if entry.id == record.id => Some(entry.clone()),
                _ => self.entry_for_id(&record.id).await?,
            };
            match entry {
                Some(entry) if !entry.is_visible() => {}
                Some(entry) if self.prefer_sql(&entry, &record) => {
                    return Ok(Some(DirectoryRecord::Sql(entry)));
                }
                _ => return Ok(Some(DirectoryRecord::Legacy(Box::new(record)))),
            }
        }
        // Older releases wrote SQL snapshots without a matching legacy document.
        Ok(sql.map(DirectoryRecord::Sql))
    }

    async fn list_entries(
        &self,
        filter: SnapshotListFilter,
    ) -> RepositoryResult<Vec<DirectoryRecord>> {
        let sql = self
            .meta
            .list_snapshots(meta_context(), filter.clone())
            .await
            .map_err(|error| map_rpc_error("list snapshots from metadata directory", error))?
            .map_err(map_meta_error)?;
        let mut sql: HashMap<_, _> = sql
            .into_iter()
            .map(|entry| (entry.id.clone(), entry))
            .collect();
        let legacy = self.repository.list(filter).await?;
        let mut entries: Vec<_> = stream::iter(legacy)
            .map(|record| {
                let entry = sql.remove(&record.id);
                async move {
                    let result = match entry {
                        Some(entry) if self.prefer_sql(&entry, &record) => {
                            Some(DirectoryRecord::Sql(entry))
                        }
                        Some(_) => Some(DirectoryRecord::Legacy(Box::new(record))),
                        None if self.entry_for_id(&record.id).await?.is_none() => {
                            Some(DirectoryRecord::Legacy(Box::new(record)))
                        }
                        None => None,
                    };
                    Ok::<_, RepositoryError>(result)
                }
            })
            .buffered(LIST_RECORD_CONCURRENCY)
            .try_filter_map(|entry| async { Ok(entry) })
            .try_collect()
            .await?;
        entries.extend(sql.into_values().map(DirectoryRecord::Sql));
        Ok(entries)
    }

    async fn mirror_distributed(
        &self,
        record: &SnapshotRecord,
        promoting: bool,
        local_layers: &[(&str, &Path)],
    ) -> RepositoryResult<SnapshotMetadata> {
        let manifest = record
            .manifest
            .as_ref()
            .expect("published Distributed manifest");
        let result = match self
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
        .map_err(map_meta_error);
        if result.is_err() {
            self.remove_file_if_retired(record).await?;
        }
        result
    }

    async fn remove_file_if_retired(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        // Retirement can win between the file commit and its SQL mirror.
        if matches!(self.entry_for_id(&record.id).await, Ok(Some(entry)) if entry.retired) {
            catalog::remove_mirror(self.repository(), &record.id, record.alias.as_ref()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SnapshotCatalog for MetadataCatalog {
    fn committed_content_is_immutable(&self) -> bool {
        true
    }

    async fn get_for_promotion(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        let entry = self.entry_for_id(id).await?;
        if entry.as_ref().is_some_and(|entry| !entry.is_visible()) {
            return Ok(None);
        }
        if let Some(record) = self.repository.get(&id.to_string()).await? {
            // A retry can arrive after the file commit and before the SQL promotion.
            if record.manifest.is_some()
                && entry.as_ref().and_then(|entry| entry.snapshot_type)
                    != Some(SnapshotPlacement::Distributed)
            {
                self.mirror_distributed(&record, entry.is_some(), &[])
                    .await?;
                return Ok(Some(record));
            }
        }
        self.get(&id.to_string()).await
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
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        let entry = self.entry_for_id(id).await?;
        if let Some(entry) = &entry {
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
        if let Some(record) = self.repository.get(&id.to_string()).await? {
            if record.alias.as_ref() == alias && record.manifest.is_some() {
                self.mirror_distributed(&record, entry.is_some(), &[])
                    .await?;
                return Ok(Some(record));
            }
        }
        super::publish::ensure_not_committed(self.repository(), id).await?;
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
        self.repository.commit_record(record.clone()).await?;
        let result = self
            .meta
            .register_snapshot(
                meta_context(),
                (&record).into(),
                SnapshotManifest::default(),
                Vec::new(),
            )
            .await
            .map_err(|error| map_rpc_error("register Local snapshot", error))
            .and_then(|entry| entry.map_err(map_meta_error));
        if result.is_err() {
            self.remove_file_if_retired(&record).await?;
        }
        self.assemble_record(result?).await
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
                let record = self.assemble_record(entry.clone()).await?;
                return Ok(record);
            }
        }
        let promoting = previous.is_some();
        let now = now_unix_ms();
        let prepared = super::store::read_snapshot_content(self.repository(), &id).await?;
        let (config, manifest) = match prepared {
            Some(content) => (content.config, content.manifest.unwrap_or(manifest)),
            None => (metadata.config(now), manifest),
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
        let id = record.id.clone();
        let record = catalog::commit_with_alias(
            self.repository(),
            &id,
            metadata.alias.as_ref(),
            move |previous| {
                if previous.as_ref().is_some_and(|previous| {
                    previous.deleting || previous.snapshot_type == SnapshotPlacement::Distributed
                }) {
                    return Err(RepositoryError::InvalidRequest {
                        reason: format!(
                            "snapshot '{}' is already committed or retiring",
                            record.id
                        ),
                    });
                }
                let mut next = record.clone();
                if let Some(previous) = previous {
                    next.created_at_unix_ms = previous.created_at_unix_ms;
                }
                Ok(next)
            },
        )
        .await?;
        let entry = self
            .mirror_distributed(&record, promoting, local_layers)
            .await?;
        self.assemble_record(entry).await
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        match self.read_entry(id_or_alias).await? {
            Some(DirectoryRecord::Sql(entry)) => self.assemble_record(entry).await.map(Some),
            Some(DirectoryRecord::Legacy(record)) => Ok(Some(*record)),
            None => Ok(None),
        }
    }

    async fn get_metadata(&self, reference: &str) -> RepositoryResult<Option<SnapshotMetadata>> {
        Ok(self
            .read_entry(reference)
            .await?
            .map(DirectoryRecord::into_metadata))
    }

    async fn list_metadata(
        &self,
        filter: SnapshotListFilter,
    ) -> RepositoryResult<Vec<SnapshotMetadata>> {
        let mut metadata: Vec<_> = self
            .list_entries(filter)
            .await?
            .into_iter()
            .map(DirectoryRecord::into_metadata)
            .collect();
        metadata.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(metadata)
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let entries = self.list_entries(filter.clone()).await?;
        let mut records: Vec<_> = stream::iter(entries)
            .map(|entry| async move {
                match entry {
                    DirectoryRecord::Sql(entry) => self.assemble_record(entry).await,
                    DirectoryRecord::Legacy(record) => Ok(*record),
                }
            })
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
        let entry = self.fetch_entry(id_or_alias).await?;
        let file = self.repository.get(id_or_alias).await?;
        let Some(id) = entry
            .as_ref()
            .map(|entry| entry.id.clone())
            .or_else(|| file.as_ref().map(|record| record.id.clone()))
            .or_else(|| SnapshotId::parse(id_or_alias).ok())
        else {
            return Ok(());
        };
        if entry.is_some() {
            // Retire the file directory first so SQL failures can be retried.
            catalog::remove_mirror(
                self.repository(),
                &id,
                entry.as_ref().and_then(|entry| entry.alias.as_ref()),
            )
            .await?;
        } else {
            self.repository.delete(&id.to_string()).await?;
        }
        self.retire(&id).await
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
    use crate::repository::SnapshotFile;
    use crate::snapshot::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::{
        SnapshotAlias, SnapshotHandle, SnapshotId, SnapshotListFilter, SnapshotManager,
        SnapshotPlacement, SnapshotPublishMetadata, SnapshotPublishSource,
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
        let stored: serde_json::Value = serde_json::from_slice(&repository.read_blob(&key).await?)?;
        assert!(stored.get("metadata").is_none());
        assert!(stored.get("config").is_some());
        assert!(stored.get("manifest").is_some());
        let mirror = repository
            .get(alias.as_ref())
            .await?
            .expect("legacy mirror");
        assert_eq!(mirror.id, dist_id);
        let document = repository
            .get_document(crate::repository::DocumentKey::SnapshotRecord(
                dist_id.clone(),
            ))
            .await?
            .expect("file catalog record");
        let legacy: serde_json::Value = serde_json::from_slice(&document)?;
        assert_eq!(
            legacy["source"],
            serde_json::to_value(&mirror.config.source)?
        );
        assert_eq!(
            legacy["resources"],
            serde_json::to_value(mirror.config.resources)?
        );
        assert!(legacy["committed"]["memory_startup"].is_object());

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
    async fn failed_sql_registration_keeps_the_file_commit_for_retry() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let database = DatabaseConfig::Sqlite {
            path: root.path().join("metadata.sqlite3"),
        };
        database.migrate().await?;
        let server = MetaServer::open(&database).await?;
        let repo: Arc<dyn Repository> = Arc::new(
            crate::repository::backends::PosixFsRepository::new(root.path().join("repository")),
        );
        let catalog = MetadataCatalog::new(transport::embedded(server.clone()), Arc::clone(&repo));
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
            catalog.commit_distributed(&metadata, manifest, &[]).await,
            Err(RepositoryError::AliasConflict { .. })
        ));
        let file = repo
            .get(&metadata.id.to_string())
            .await?
            .expect("file already published");
        assert_eq!(file.alias.as_ref(), Some(&alias));
        assert!(server
            .get_snapshot(&metadata.id.to_string())
            .await?
            .is_none());
        server.retire_snapshot(owner.id).await?;
        let repaired = catalog
            .prepare_distributed(&metadata.id, Some(&alias))
            .await?
            .expect("retry reuses committed files");
        assert_eq!(repaired.created_at_unix_ms, file.created_at_unix_ms);
        assert_eq!(
            server.get_snapshot(alias.as_ref()).await?.unwrap().id,
            file.id
        );
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn older_sql_only_records_are_visible_in_both_read_modes() -> anyhow::Result<()> {
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
        let mut old_record = SnapshotRecord::mock_ready(SnapshotManifest::default());
        old_record.config.source = SnapshotSource::Sandbox {
            source_sandbox_id: "legacy-rc-sandbox".to_owned(),
            source_sandbox_metadata: None,
        };
        let alias = SnapshotAlias::parse("old-rc")?;
        old_record.alias = Some(alias.clone());
        let id = old_record.id.clone();
        let key = BlobKey::Snapshot(id.clone(), SnapshotFile::Record);
        let original = serde_json::to_vec(&old_record.artifact_json()?)?;
        repository
            .put_blob(&key, BlobBody::Bytes(original.clone()))
            .await?;
        server
            .register_snapshot(
                (&old_record).into(),
                old_record.manifest.clone().unwrap(),
                &[],
            )
            .await?;
        catalog::claim_alias(repository.as_ref(), &alias, &id).await?;

        let other = SnapshotRecord {
            id: SnapshotId::generate(),
            alias: None,
            ..old_record.clone()
        };
        server
            .register_snapshot((&other).into(), other.manifest.clone().unwrap(), &[])
            .await?;
        // A missing content file outside the selected page must only affect that page.
        for read_from_sql in [false, true] {
            let catalog = MetadataCatalog::new(meta.clone(), Arc::clone(&repository))
                .with_read_from_sql(read_from_sql);
            assert_eq!(catalog.get(alias.as_ref()).await?.unwrap().id, id);
            assert_eq!(catalog.get(&id.to_string()).await?.unwrap().id, id);
            let candidates = catalog.list_metadata(SnapshotListFilter::default()).await?;
            assert_eq!(candidates.len(), 2);
            assert!(candidates.iter().any(|entry| entry.id == other.id));
            assert_eq!(
                catalog
                    .list(SnapshotListFilter::by_ids([id.clone()]))
                    .await?[0]
                    .id,
                id
            );
        }
        assert!(repository.get(alias.as_ref()).await?.is_none());
        assert_eq!(repository.read_blob(&key).await?, original);
        Ok(())
    }
}
