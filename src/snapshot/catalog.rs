//! The shared snapshot catalog: one record per snapshot plus an alias binding,
//! stored as repository documents so every backend follows the same rules.
//!
//! Records are written before their aliases claim them and outlive them on
//! delete, so an alias whose record is missing is always stale.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tracing::warn;

use super::publish;
use super::publish::SnapshotPublication;
use super::{
    CommittedSnapshot, MemoryStartupPackInfo, SnapshotAlias, SnapshotId, SnapshotPublishMetadata,
    SnapshotPublishSource, SnapshotRecord, SnapshotSource, SnapshotSourceKind,
    TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
use crate::repository::{
    DocumentChange, DocumentCollection, DocumentKey, Repository, RepositoryError, RepositoryResult,
};
use crate::snapshot::SandboxSnapshotManifest;

const MAX_ALIAS_CLAIM_ATTEMPTS: usize = 5;

/// Snapshot record list filter.
///
/// When multiple fields are present they combine with AND semantics.
/// When all fields are `None`, the filter matches all snapshot records,
/// including pending template builds and committed snapshots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotListFilter {
    /// Match aliases that start with this prefix.
    pub alias_prefix: Option<String>,
    /// Restrict results to this exact set of snapshot ids.
    pub snapshot_ids: Option<Vec<SnapshotId>>,
    /// Restrict results to a single snapshot id or exact alias.
    pub snapshot_id_or_alias: Option<String>,
    /// Restrict results to snapshots captured from this source sandbox id.
    ///
    /// This only matches records whose source is [`SnapshotSourceKind::Sandbox`].
    pub source_sandbox_id: Option<String>,
    /// Restrict results to records with these source kinds.
    pub sources: Option<Vec<SnapshotSourceKind>>,
    /// Restrict results to template records whose build status is in this set.
    ///
    /// Sandbox records never match this field.
    pub template_statuses: Option<Vec<TemplateBuildStatus>>,
}

impl SnapshotListFilter {
    pub fn matches_all() -> Self {
        Self::default()
    }

    pub fn by_ids<I>(snapshot_ids: I) -> Self
    where
        I: IntoIterator<Item = SnapshotId>,
    {
        Self {
            snapshot_ids: Some(snapshot_ids.into_iter().collect()),
            ..Self::default()
        }
    }

    pub fn templates() -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Template]),
            ..Self::default()
        }
    }

    pub fn sandbox_snapshots(
        source_sandbox_id: Option<String>,
        snapshot_id_or_alias: Option<String>,
    ) -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Sandbox]),
            source_sandbox_id,
            snapshot_id_or_alias: snapshot_id_or_alias.map(|value| {
                let unqualified = value
                    .rsplit_once('/')
                    .map_or(value.as_str(), |(_, name)| name);
                unqualified
                    .split_once(':')
                    .map_or(unqualified, |(name, _)| name)
                    .to_string()
            }),
            ..Self::default()
        }
    }
}

/// Snapshot records and alias bindings shared by every node.
#[async_trait]
pub trait SnapshotCatalog {
    /// Creates a durable template snapshot record before build artifacts exist.
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord>;

    /// Stores a captured snapshot and commits its record.
    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord>;

    /// Like [`Self::publish`], retaining any transformed local layer files
    /// until the caller finishes best-effort P2P publication after the commit.
    async fn publish_with_local_layers(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
    ) -> RepositoryResult<SnapshotPublication>;

    /// Loads one snapshot record by ID or alias.
    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>>;

    /// Lists snapshot records matching `filter`, newest first.
    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>>;

    /// Deletes one snapshot by ID or alias, with its alias binding, build log,
    /// and per-snapshot files. Missing snapshots are considered success.
    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()>;

    /// Resolves an alias to the snapshot that currently owns it.
    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>>;

    /// Atomically transitions one template build from waiting to building.
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord>;

    /// Marks one template build as failed.
    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()>;
}

#[async_trait]
impl<R: Repository + ?Sized> SnapshotCatalog for R {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        if !matches!(record.source, SnapshotSource::Template { .. }) {
            return Err(RepositoryError::InvalidRequest {
                reason: "only template snapshots can be pre-created".to_string(),
            });
        }
        if record.committed.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "pre-created template snapshots must not already be committed".to_string(),
            });
        }
        if let Some(alias) = record.alias.as_ref() {
            ensure_alias_available(self, alias, &record.id).await?;
        }
        let bytes = encode_record(&record)?;
        let existing = self
            .update_document(
                DocumentKey::SnapshotRecord(record.id.clone()),
                Box::new(move |current| {
                    Ok(match current {
                        None => DocumentChange::Put(bytes.clone()),
                        Some(_) => DocumentChange::Keep,
                    })
                }),
            )
            .await?;
        if existing.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }
        if let Some(alias) = record.alias.as_ref() {
            if let Err(error) = claim_alias(self, alias, &record.id).await {
                if let Err(rollback) = self
                    .delete_document(DocumentKey::SnapshotRecord(record.id.clone()))
                    .await
                {
                    warn!(snapshot_id = %record.id, error = %rollback, "failed to roll back snapshot record");
                }
                return Err(error);
            }
        }
        Ok(record)
    }

    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        Ok(self
            .publish_with_local_layers(metadata, manifest)
            .await?
            .record)
    }

    async fn publish_with_local_layers(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
    ) -> RepositoryResult<SnapshotPublication> {
        publish::publish(self, metadata, manifest).await
    }

    /// Snapshots being deleted are hidden, since their files may be gone.
    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(find_record(self, id_or_alias)
            .await?
            .filter(|record| !record.deleting))
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let mut records = Vec::new();
        for (id, bytes) in self
            .read_documents(DocumentCollection::SnapshotRecords)
            .await?
        {
            let record = decode_record(&id, &bytes)?;
            if !record.deleting && matches_filter(&record, &filter) {
                records.push(record);
            }
        }
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.to_string().cmp(&right.id.to_string()))
        });
        Ok(records)
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        // A delete that failed partway leaves the record marked; retrying
        // finishes it.
        let Some(record) = find_record(self, id_or_alias).await? else {
            if let Ok(id) = SnapshotId::parse(id_or_alias) {
                self.delete_document(DocumentKey::BuildLog(id)).await?;
            }
            return Ok(());
        };
        let id = record.id;
        // A startup manifest stored from now on is removed rather than
        // attached, even if it lands after the files are deleted, and the
        // snapshot can no longer be committed. Cleanup uses the record as
        // marked, which includes any commit that landed before.
        let marked = update_record(self, &id, |_, record| {
            Ok(match record {
                Some(mut record) if !record.deleting => {
                    record.deleting = true;
                    (Some(record.clone()), Some(record))
                }
                record => (None, record),
            })
        })
        .await?;
        let Some(record) = marked else {
            // Another delete finished first.
            return Ok(());
        };
        let publications = record
            .committed
            .map(|committed| committed.disk_publications)
            .unwrap_or_default();
        // Files go before the record, and the alias after it, so a failed
        // delete can be retried by ID or alias.
        super::store::delete_snapshot(self, &id, &publications).await?;
        self.delete_document(DocumentKey::BuildLog(id.clone()))
            .await?;
        self.delete_document(DocumentKey::SnapshotRecord(id.clone()))
            .await?;
        if let Some(alias) = record.alias {
            release_alias(self, alias, &id).await?;
        }
        Ok(())
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let alias = parse_alias(alias)?;
        Ok(alias_owner(self, &alias)
            .await?
            .and_then(|(id, record)| (!record.deleting).then_some(id)))
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let now = now_unix_ms();
        update_record(self, id, move |id, record| {
            let mut record = record.filter(|record| !record.deleting).ok_or_else(|| {
                RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                }
            })?;
            let build = template_build(&mut record)?;
            if build.status != TemplateBuildStatus::Waiting {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("template build '{id}' is not in waiting state"),
                });
            }
            build.status = TemplateBuildStatus::Building;
            build.started_at_unix_ms = Some(now);
            build.error_reason = None;
            record.updated_at_unix_ms = now;
            Ok((Some(record.clone()), record))
        })
        .await
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let now = now_unix_ms();
        update_record(self, id, move |id, record| {
            let mut record = record.filter(|record| !record.deleting).ok_or_else(|| {
                RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                }
            })?;
            let build = template_build(&mut record)?;
            build.status = TemplateBuildStatus::Error;
            build.finished_at_unix_ms = Some(now);
            build.error_reason = Some(reason.clone());
            record.updated_at_unix_ms = now;
            Ok((Some(record), ()))
        })
        .await
    }
}

/// Commits a stored snapshot's record and then claims its alias. A committed
/// record cannot be committed again.
pub(crate) async fn commit<R>(
    repository: &R,
    metadata: &SnapshotPublishMetadata,
    committed: CommittedSnapshot,
) -> RepositoryResult<SnapshotRecord>
where
    R: Repository + ?Sized,
{
    let id = metadata.id.clone();
    let now = now_unix_ms();
    let next = {
        let metadata = metadata.clone();
        move |current: Option<SnapshotRecord>| -> RepositoryResult<SnapshotRecord> {
            let Some(mut record) = current else {
                return Ok(new_committed_record(&metadata, committed.clone(), now));
            };
            if record.committed.is_some() {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{}' is already committed", record.id),
                });
            }
            if record.deleting {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{}' is being deleted", record.id),
                });
            }
            record.mark_committed(
                metadata.alias.clone(),
                metadata.resources,
                committed.clone(),
                metadata.source.clone(),
                now,
            );
            Ok(record)
        }
    };
    let (previous, record) = update_record(repository, &id, move |_, record| {
        let next = next(record.clone())?;
        Ok((Some(next.clone()), (record, next)))
    })
    .await?;
    let Some(alias) = metadata.alias.as_ref() else {
        return Ok(record);
    };
    if let Err(error) = claim_alias(repository, alias, &id).await {
        // Restore the record this commit replaced, unless it changed since.
        let written = encode_record(&record)?;
        let restored = match previous.as_ref() {
            Some(previous) => DocumentChange::Put(encode_record(previous)?),
            None => DocumentChange::Delete,
        };
        let rolled_back = repository
            .update_document(
                DocumentKey::SnapshotRecord(id.clone()),
                Box::new(move |current| {
                    Ok(if current == Some(written.as_slice()) {
                        restored.clone()
                    } else {
                        DocumentChange::Keep
                    })
                }),
            )
            .await;
        if let Err(rollback) = rolled_back {
            warn!(snapshot_id = %id, error = %rollback, "failed to roll back snapshot record");
        }
        return Err(error);
    }
    Ok(record)
}

/// Fails with the alias owner when a live snapshot other than `id` owns `alias`.
pub(crate) async fn ensure_alias_available<R>(
    repository: &R,
    alias: &SnapshotAlias,
    id: &SnapshotId,
) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    match alias_owner(repository, alias).await? {
        Some((existing, _)) if &existing != id => Err(alias_conflict(alias, existing, id)),
        _ => Ok(()),
    }
}

/// Records a stored startup manifest on a committed snapshot. Returns false
/// when the snapshot is gone or being deleted, so the caller can remove its
/// files.
pub(crate) async fn attach_startup_manifest<R>(
    repository: &R,
    id: &SnapshotId,
    info: MemoryStartupPackInfo,
) -> RepositoryResult<bool>
where
    R: Repository + ?Sized,
{
    let now = now_unix_ms();
    update_record(repository, id, move |_, record| {
        let Some(mut record) = record.filter(|record| !record.deleting) else {
            return Ok((None, false));
        };
        let Some(committed) = record.committed.as_mut() else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' is not committed", record.id),
            });
        };
        if committed.memory_startup.is_some() {
            return Ok((None, true));
        }
        committed.memory_startup = Some(info.clone());
        record.updated_at_unix_ms = now;
        Ok((Some(record), true))
    })
    .await
}

fn new_committed_record(
    metadata: &SnapshotPublishMetadata,
    committed: CommittedSnapshot,
    now: i64,
) -> SnapshotRecord {
    let source = match metadata.source.clone() {
        SnapshotPublishSource::Template => SnapshotSource::Template {
            build: TemplateBuildInfo {
                status: TemplateBuildStatus::Ready,
                started_at_unix_ms: None,
                finished_at_unix_ms: Some(now),
                error_reason: None,
            },
        },
        SnapshotPublishSource::Sandbox { source_sandbox_id } => {
            SnapshotSource::Sandbox { source_sandbox_id }
        }
    };
    SnapshotRecord {
        id: metadata.id.clone(),
        alias: metadata.alias.clone(),
        source,
        resources: metadata.resources,
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
        committed: Some(committed),
        deleting: false,
    }
}

fn template_build(record: &mut SnapshotRecord) -> RepositoryResult<&mut TemplateBuildInfo> {
    match &mut record.source {
        SnapshotSource::Template { build } => Ok(build),
        SnapshotSource::Sandbox { .. } => Err(RepositoryError::InvalidRequest {
            reason: format!("snapshot '{}' is not a template build", record.id),
        }),
    }
}

/// Atomically applies `decide` to one snapshot record and writes the record it
/// returns, if any. `decide` must be pure: it runs again on the state the
/// update was applied to, to report the outcome.
async fn update_record<R, T>(
    repository: &R,
    id: &SnapshotId,
    decide: impl Fn(&SnapshotId, Option<SnapshotRecord>) -> RepositoryResult<(Option<SnapshotRecord>, T)>
        + Clone
        + Send
        + Sync
        + 'static,
) -> RepositoryResult<T>
where
    R: Repository + ?Sized,
{
    let update = decide.clone();
    let update_id = id.clone();
    let applied_to = repository
        .update_document(
            DocumentKey::SnapshotRecord(id.clone()),
            Box::new(move |bytes| {
                let record = bytes
                    .map(|bytes| decode_record(&update_id.to_string(), bytes))
                    .transpose()?;
                Ok(match update(&update_id, record)?.0 {
                    Some(next) => DocumentChange::Put(encode_record(&next)?),
                    None => DocumentChange::Keep,
                })
            }),
        )
        .await?;
    let record = applied_to
        .map(|bytes| decode_record(&id.to_string(), &bytes))
        .transpose()?;
    Ok(decide(id, record)?.1)
}

/// Points `alias` at `id` unless a live snapshot already owns it.
async fn claim_alias<R>(
    repository: &R,
    alias: &SnapshotAlias,
    id: &SnapshotId,
) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    let key = DocumentKey::SnapshotAlias(alias.clone());
    let claimed = encode_alias(id)?;
    for _ in 0..MAX_ALIAS_CLAIM_ATTEMPTS {
        let current = repository.get_document(key.clone()).await?;
        if let Some(bytes) = current.as_deref() {
            let existing = decode_alias(alias, bytes)?;
            if &existing == id {
                return Ok(());
            }
            if read_record(repository, &existing).await?.is_some() {
                return Err(alias_conflict(alias, existing, id));
            }
        }
        let (expected, claimed) = (current.clone(), claimed.clone());
        let applied_to = repository
            .update_document(
                key.clone(),
                Box::new(move |now| {
                    Ok(if now == expected.as_deref() {
                        DocumentChange::Put(claimed.clone())
                    } else {
                        DocumentChange::Keep
                    })
                }),
            )
            .await?;
        if applied_to == current {
            return Ok(());
        }
    }
    Err(RepositoryError::Backend {
        message: format!("snapshot alias '{alias}' changed too often while claiming it"),
        source: None,
    })
}

/// Removes `alias` if it still points at `id`.
async fn release_alias<R>(
    repository: &R,
    alias: SnapshotAlias,
    id: &SnapshotId,
) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    let owned = encode_alias(id)?;
    let target = id.clone();
    let alias_for_decode = alias.clone();
    repository
        .update_document(
            DocumentKey::SnapshotAlias(alias),
            Box::new(move |current| {
                let Some(bytes) = current else {
                    return Ok(DocumentChange::Keep);
                };
                // Accept any encoding of the same ID, such as a legacy one.
                Ok(
                    if bytes == owned.as_slice()
                        || decode_alias(&alias_for_decode, bytes)? == target
                    {
                        DocumentChange::Delete
                    } else {
                        DocumentChange::Keep
                    },
                )
            }),
        )
        .await?;
    Ok(())
}

/// The live snapshot that owns `alias`, removing the alias when its snapshot
/// is gone.
async fn alias_owner<R>(
    repository: &R,
    alias: &SnapshotAlias,
) -> RepositoryResult<Option<(SnapshotId, SnapshotRecord)>>
where
    R: Repository + ?Sized,
{
    let Some(bytes) = repository
        .get_document(DocumentKey::SnapshotAlias(alias.clone()))
        .await?
    else {
        return Ok(None);
    };
    let id = decode_alias(alias, &bytes)?;
    if let Some(record) = read_record(repository, &id).await? {
        return Ok(Some((id, record)));
    }
    warn!(alias = %alias, snapshot_id = %id, "cleaning up stale alias pointing to missing snapshot");
    release_alias(repository, alias.clone(), &id).await?;
    Ok(None)
}

/// Finds a snapshot by ID or alias, including one being deleted.
async fn find_record<R>(
    repository: &R,
    id_or_alias: &str,
) -> RepositoryResult<Option<SnapshotRecord>>
where
    R: Repository + ?Sized,
{
    if let Ok(id) = SnapshotId::parse(id_or_alias) {
        if let Some(record) = read_record(repository, &id).await? {
            return Ok(Some(record));
        }
    }
    let alias = parse_alias(id_or_alias)?;
    Ok(alias_owner(repository, &alias)
        .await?
        .map(|(_, record)| record))
}

async fn read_record<R>(repository: &R, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>>
where
    R: Repository + ?Sized,
{
    repository
        .get_document(DocumentKey::SnapshotRecord(id.clone()))
        .await?
        .map(|bytes| decode_record(&id.to_string(), &bytes))
        .transpose()
}

fn parse_alias(alias: &str) -> RepositoryResult<SnapshotAlias> {
    SnapshotAlias::parse(alias).map_err(|error| RepositoryError::InvalidRequest {
        reason: format!("invalid alias '{alias}': {error}"),
    })
}

fn alias_conflict(alias: &SnapshotAlias, existing: SnapshotId, id: &SnapshotId) -> RepositoryError {
    RepositoryError::AliasConflict {
        alias: alias.to_string(),
        existing,
        new_id: id.clone(),
    }
}

fn decode_record(id: &str, bytes: &[u8]) -> RepositoryResult<SnapshotRecord> {
    serde_json::from_slice(bytes)
        .map_err(|error| RepositoryError::backend(format!("parse snapshot record '{id}'"), error))
}

fn encode_record(record: &SnapshotRecord) -> RepositoryResult<Vec<u8>> {
    serde_json::to_vec_pretty(record)
        .map_err(|error| RepositoryError::backend("serialize snapshot record", error))
}

fn decode_alias(alias: &SnapshotAlias, bytes: &[u8]) -> RepositoryResult<SnapshotId> {
    serde_json::from_slice(bytes)
        .map_err(|error| RepositoryError::backend(format!("parse alias '{alias}'"), error))
}

fn encode_alias(id: &SnapshotId) -> RepositoryResult<Vec<u8>> {
    serde_json::to_vec(id).map_err(|error| RepositoryError::backend("serialize alias", error))
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn matches_filter(record: &SnapshotRecord, filter: &SnapshotListFilter) -> bool {
    if let Some(alias_prefix) = filter.alias_prefix.as_deref() {
        match record.alias.as_ref() {
            Some(alias) if alias.to_string().starts_with(alias_prefix) => {}
            _ => return false,
        }
    }

    if let Some(ids) = filter.snapshot_ids.as_ref() {
        if !ids.iter().any(|id| id == &record.id) {
            return false;
        }
    }

    if let Some(id_or_alias) = filter.snapshot_id_or_alias.as_deref() {
        if record.id.to_string() != id_or_alias
            && record
                .alias
                .as_ref()
                .is_none_or(|alias| alias.as_ref() != id_or_alias)
        {
            return false;
        }
    }

    if let Some(source_sandbox_id) = filter.source_sandbox_id.as_deref() {
        match &record.source {
            SnapshotSource::Sandbox {
                source_sandbox_id: record_source_sandbox_id,
            } if record_source_sandbox_id == source_sandbox_id => {}
            _ => return false,
        }
    }

    if let Some(sources) = filter.sources.as_ref() {
        let source = match &record.source {
            SnapshotSource::Template { .. } => SnapshotSourceKind::Template,
            SnapshotSource::Sandbox { .. } => SnapshotSourceKind::Sandbox,
        };
        if !sources.contains(&source) {
            return false;
        }
    }

    if let Some(statuses) = filter.template_statuses.as_ref() {
        let SnapshotSource::Template { build } = &record.source else {
            return false;
        };
        if !statuses.contains(&build.status) {
            return false;
        };
    }

    true
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::snapshot::backends::{PosixFsBackend, PosixFsBackendConfig};

    fn repository(root: &Path) -> Arc<dyn Repository> {
        PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join("cache")),
            runtime_cache_root: None,
        })
        .expect("posix backend")
        .repository()
    }

    fn metadata(alias: Option<&str>, source: SnapshotPublishSource) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id: SnapshotId::generate(),
            alias: alias.map(|alias| SnapshotAlias::parse(alias).expect("alias")),
            source,
            ..SnapshotPublishMetadata::mock()
        }
    }

    async fn committed(
        repository: &dyn Repository,
        alias: Option<&str>,
        source: SnapshotPublishSource,
    ) -> RepositoryResult<SnapshotId> {
        let metadata = metadata(alias, source);
        commit(repository, &metadata, CommittedSnapshot::mock()).await?;
        Ok(metadata.id)
    }

    async fn listed(repository: &dyn Repository, filter: SnapshotListFilter) -> Vec<SnapshotId> {
        repository
            .list(filter)
            .await
            .expect("list")
            .into_iter()
            .map(|record| record.id)
            .collect()
    }

    #[tokio::test]
    async fn list_applies_record_filters() {
        let tempdir = TempDir::new().unwrap();
        let repository = repository(tempdir.path());
        let repository = repository.as_ref();
        let template = SnapshotPublishSource::Template;
        let sandbox = |id: &str| SnapshotPublishSource::Sandbox {
            source_sandbox_id: id.to_owned(),
        };
        let template_alpha = committed(repository, Some("template-alpha"), template.clone())
            .await
            .unwrap();
        let template_beta = committed(repository, Some("template-beta"), template.clone())
            .await
            .unwrap();
        let sandbox_one = committed(repository, Some("sandbox-one"), sandbox("sandbox-1"))
            .await
            .unwrap();
        let sandbox_two = committed(repository, Some("sandbox-two"), sandbox("sandbox-2"))
            .await
            .unwrap();
        let errored_template = SnapshotId::generate();
        repository
            .create(SnapshotRecord::template_waiting(
                errored_template.clone(),
                Some(SnapshotAlias::parse("template-error").unwrap()),
                Default::default(),
            ))
            .await
            .unwrap();
        repository
            .mark_build_error(&errored_template, TemplateBuildErrorReason::new("boom"))
            .await
            .unwrap();

        let ids = listed(
            repository,
            SnapshotListFilter::by_ids([template_alpha.clone(), sandbox_one.clone()]),
        )
        .await;
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&template_alpha) && ids.contains(&sandbox_one));

        let ids = listed(
            repository,
            SnapshotListFilter {
                alias_prefix: Some("template-".to_string()),
                ..SnapshotListFilter::default()
            },
        )
        .await;
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&template_beta) && ids.contains(&errored_template));

        let ids = listed(repository, SnapshotListFilter::templates()).await;
        assert_eq!(ids.len(), 3);
        assert!(!ids.contains(&sandbox_one));

        let ids = listed(
            repository,
            SnapshotListFilter::sandbox_snapshots(Some("sandbox-1".to_string()), None),
        )
        .await;
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed(
            repository,
            SnapshotListFilter::sandbox_snapshots(None, Some("team/sandbox-one:v1".to_string())),
        )
        .await;
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed(
            repository,
            SnapshotListFilter::sandbox_snapshots(None, Some(format!("{sandbox_one}:v1"))),
        )
        .await;
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed(
            repository,
            SnapshotListFilter::sandbox_snapshots(
                Some("sandbox-2".to_string()),
                Some("sandbox-one".to_string()),
            ),
        )
        .await;
        assert!(ids.is_empty());

        let ids = listed(
            repository,
            SnapshotListFilter {
                template_statuses: Some(vec![TemplateBuildStatus::Error]),
                ..SnapshotListFilter::templates()
            },
        )
        .await;
        assert_eq!(ids, vec![errored_template]);

        let ids = listed(
            repository,
            SnapshotListFilter {
                alias_prefix: Some("sandbox-".to_string()),
                sources: Some(vec![SnapshotSourceKind::Sandbox]),
                snapshot_ids: Some(vec![sandbox_two.clone(), template_alpha]),
                ..SnapshotListFilter::default()
            },
        )
        .await;
        assert_eq!(ids, vec![sandbox_two]);
    }

    #[tokio::test]
    async fn get_validates_aliases_and_misses_unknown_ids() {
        let tempdir = TempDir::new().unwrap();
        let repository = repository(tempdir.path());
        // Neither a UUID nor a valid alias.
        let error = repository.get("../../etc/passwd").await.unwrap_err();
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        let unknown = SnapshotId::generate().to_string();
        assert!(repository.get(&unknown).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn aliases_belong_to_one_live_snapshot() {
        let tempdir = TempDir::new().unwrap();
        let repository = repository(tempdir.path());
        let repository = repository.as_ref();
        let template = SnapshotPublishSource::Template;
        let first = committed(repository, Some("shared"), template.clone())
            .await
            .unwrap();
        let error = committed(repository, Some("shared"), template.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(error, RepositoryError::AliasConflict { existing, .. } if existing == first)
        );
        // The losing commit leaves no record behind.
        assert_eq!(
            listed(repository, SnapshotListFilter::matches_all()).await,
            std::slice::from_ref(&first)
        );
        assert_eq!(
            repository.resolve_alias("shared").await.unwrap(),
            Some(first.clone())
        );

        // A name whose record is gone is stale and can be claimed again.
        repository
            .delete_document(DocumentKey::SnapshotRecord(first))
            .await
            .unwrap();
        assert_eq!(repository.resolve_alias("shared").await.unwrap(), None);
        let second = committed(repository, Some("shared"), template)
            .await
            .unwrap();
        assert_eq!(repository.get("shared").await.unwrap().unwrap().id, second);

        // Deleting by alias removes the binding with the record.
        repository.delete("shared").await.unwrap();
        assert!(repository.get(&second.to_string()).await.unwrap().is_none());
        assert!(repository
            .get_document(DocumentKey::SnapshotAlias(
                SnapshotAlias::parse("shared").unwrap()
            ))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn committed_snapshots_cannot_be_committed_again() {
        let tempdir = TempDir::new().unwrap();
        let repository = repository(tempdir.path());
        let metadata = metadata(None, SnapshotPublishSource::Template);
        commit(repository.as_ref(), &metadata, CommittedSnapshot::mock())
            .await
            .unwrap();
        let error = commit(repository.as_ref(), &metadata, CommittedSnapshot::mock())
            .await
            .unwrap_err();
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn template_builds_being_deleted_cannot_be_committed() {
        let tempdir = TempDir::new().unwrap();
        let repository = repository(tempdir.path());
        let metadata = metadata(None, SnapshotPublishSource::Template);
        let mut record =
            SnapshotRecord::template_waiting(metadata.id.clone(), None, Default::default());
        // A delete that stopped right after marking the record.
        record.deleting = true;
        repository
            .put_document(
                DocumentKey::SnapshotRecord(metadata.id.clone()),
                encode_record(&record).unwrap(),
            )
            .await
            .unwrap();
        let error = commit(repository.as_ref(), &metadata, CommittedSnapshot::mock())
            .await
            .unwrap_err();
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        let record = read_record(repository.as_ref(), &metadata.id)
            .await
            .unwrap()
            .unwrap();
        assert!(record.deleting && record.committed.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_builder_starts_each_template_build() {
        let tempdir = TempDir::new().unwrap();
        let id = SnapshotId::generate();
        repository(tempdir.path())
            .create(SnapshotRecord::template_waiting(
                id.clone(),
                None,
                Default::default(),
            ))
            .await
            .unwrap();
        let mut starts = Vec::new();
        for _ in 0..16 {
            // Separate repositories stand in for separate nodes.
            let repository = repository(tempdir.path());
            let id = id.clone();
            starts.push(tokio::spawn(async move {
                repository.try_start_build(&id).await
            }));
        }
        let mut started = 0;
        for start in starts {
            match start.await.unwrap() {
                Ok(_) => started += 1,
                Err(RepositoryError::InvalidRequest { .. }) => {}
                Err(error) => panic!("unexpected error: {error}"),
            }
        }
        assert_eq!(started, 1);
    }

    #[tokio::test]
    async fn startup_manifests_never_revive_deleted_snapshots() {
        let tempdir = TempDir::new().unwrap();
        let repository = repository(tempdir.path());
        let repository = repository.as_ref();
        let info = MemoryStartupPackInfo {
            pack_size: 1,
            mem_virtual_size: 4096,
            index_sha256: "digest".to_owned(),
        };
        let id = committed(repository, None, SnapshotPublishSource::Template)
            .await
            .unwrap();
        assert!(attach_startup_manifest(repository, &id, info.clone())
            .await
            .unwrap());
        // Attaching again keeps the first descriptor.
        let other = MemoryStartupPackInfo {
            pack_size: 2,
            ..info.clone()
        };
        assert!(attach_startup_manifest(repository, &id, other)
            .await
            .unwrap());
        let record = repository.get(&id.to_string()).await.unwrap().unwrap();
        assert_eq!(record.committed.unwrap().memory_startup, Some(info.clone()));

        repository.delete(&id.to_string()).await.unwrap();
        assert!(!attach_startup_manifest(repository, &id, info)
            .await
            .unwrap());
        assert!(repository.get(&id.to_string()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn startup_manifests_are_not_attached_once_a_delete_starts() {
        let tempdir = TempDir::new().unwrap();
        let repository = repository(tempdir.path());
        let repository = repository.as_ref();
        let id = committed(repository, None, SnapshotPublishSource::Template)
            .await
            .unwrap();
        // A delete that stopped right after marking the record.
        update_record(repository, &id, |_, record| {
            let mut record = record.expect("committed record");
            record.deleting = true;
            Ok((Some(record), ()))
        })
        .await
        .unwrap();

        let info = MemoryStartupPackInfo {
            pack_size: 1,
            mem_virtual_size: 4096,
            index_sha256: "digest".to_owned(),
        };
        assert!(!attach_startup_manifest(repository, &id, info)
            .await
            .unwrap());
        repository.delete(&id.to_string()).await.unwrap();
        assert!(repository.get(&id.to_string()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn snapshots_being_deleted_are_hidden_until_the_delete_finishes() {
        let tempdir = TempDir::new().unwrap();
        let repository = repository(tempdir.path());
        let repository = repository.as_ref();
        let id = committed(repository, Some("hidden"), SnapshotPublishSource::Template)
            .await
            .unwrap();
        // A delete that failed after marking the record.
        update_record(repository, &id, |_, record| {
            let mut record = record.expect("committed record");
            record.deleting = true;
            Ok((Some(record), ()))
        })
        .await
        .unwrap();

        assert!(repository.get(&id.to_string()).await.unwrap().is_none());
        assert!(repository.get("hidden").await.unwrap().is_none());
        assert_eq!(repository.resolve_alias("hidden").await.unwrap(), None);
        assert!(repository
            .list(SnapshotListFilter::default())
            .await
            .unwrap()
            .is_empty());

        assert!(matches!(
            repository.try_start_build(&id).await.unwrap_err(),
            RepositoryError::SnapshotNotFound { .. }
        ));
        assert!(matches!(
            repository
                .mark_build_error(&id, TemplateBuildErrorReason::new("late"))
                .await
                .unwrap_err(),
            RepositoryError::SnapshotNotFound { .. }
        ));

        // Retrying the delete by alias finishes it.
        repository.delete("hidden").await.unwrap();
        assert!(read_record(repository, &id).await.unwrap().is_none());
        assert!(repository
            .get_document(DocumentKey::SnapshotAlias(
                SnapshotAlias::parse("hidden").unwrap()
            ))
            .await
            .unwrap()
            .is_none());
    }
}
