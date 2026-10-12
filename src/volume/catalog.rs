//! The shared volume catalog: one record per volume plus a name alias, stored
//! as repository documents so every backend follows the same rules.

use async_trait::async_trait;
use tracing::warn;

use super::{is_valid_volume_component, VolumeMode, VolumeRecord, VolumeStatus};
use crate::repository::{
    DocumentChange, DocumentCollection, DocumentKey, Repository, RepositoryError, RepositoryResult,
};

const MAX_ALIAS_CLAIM_ATTEMPTS: usize = 5;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VolumeRecordPage {
    pub records: Vec<VolumeRecord>,
    /// The last returned volume ID. Supplying it to the next request resumes
    /// after that record in the repository's stable ID order.
    pub next_volume_id: Option<String>,
}

/// Volume records and name bindings shared by every node.
#[async_trait]
pub trait VolumeCatalog {
    /// Resolves a volume by ID first, then by its unique name.
    async fn get_volume(&self, reference: &str) -> RepositoryResult<Option<VolumeRecord>>;

    /// Lists one bounded page ordered by the repository's stable volume cursor.
    async fn list_volumes_page(
        &self,
        after_volume_id: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<VolumeRecordPage>;

    /// Creates one durable volume record and claims its unique name.
    async fn create_volume(&self, record: VolumeRecord) -> RepositoryResult<()>;

    /// Updates one existing durable volume record, keeping its reservations.
    async fn put_volume(&self, record: VolumeRecord) -> RepositoryResult<()>;

    /// Removes one unmounted volume. Missing volumes are considered success.
    async fn delete_volume(&self, volume_id: &str) -> RepositoryResult<()>;

    /// Acquires an exclusive reservation. `Ok(Some(owner))` reports a
    /// conflicting owner.
    async fn reserve_volume(
        &self,
        volume_id: &str,
        owner: &str,
    ) -> RepositoryResult<Option<String>>;

    /// Adds a shared lease for a read-only volume.
    async fn reserve_read_only_volume(&self, volume_id: &str, owner: &str) -> RepositoryResult<()>;

    /// Releases or rebinds one volume, changing it only when `from` mounts it.
    async fn replace_volume_owner_for(
        &self,
        volume_id: &str,
        from: &str,
        to: Option<&str>,
    ) -> RepositoryResult<()>;
}

#[async_trait]
impl<R: Repository + ?Sized> VolumeCatalog for R {
    async fn get_volume(&self, reference: &str) -> RepositoryResult<Option<VolumeRecord>> {
        validate(reference, "reference")?;
        if let Some(record) = read_record(self, reference).await? {
            return Ok(Some(record));
        }
        match read_alias(self, reference).await? {
            Some(volume_id) => read_record(self, &volume_id).await,
            None => Ok(None),
        }
    }

    async fn list_volumes_page(
        &self,
        after_volume_id: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<VolumeRecordPage> {
        if limit == 0 {
            return Err(RepositoryError::InvalidRequest {
                reason: "volume page limit must be greater than zero".to_string(),
            });
        }
        if let Some(volume_id) = after_volume_id {
            validate(volume_id, "id")?;
        }
        let mut ids = self
            .list_documents(
                DocumentCollection::VolumeRecords,
                after_volume_id,
                limit.saturating_add(1),
            )
            .await?;
        let has_more = ids.len() > limit;
        ids.truncate(limit);
        let next_volume_id = has_more.then(|| ids.last().cloned()).flatten();
        let mut records = Vec::with_capacity(ids.len());
        for volume_id in ids {
            // Skips a volume deleted since the listing.
            if let Some(record) = read_record(self, &volume_id).await? {
                records.push(record);
            }
        }
        Ok(VolumeRecordPage {
            records,
            next_volume_id,
        })
    }

    async fn create_volume(&self, record: VolumeRecord) -> RepositoryResult<()> {
        validate(&record.id, "id")?;
        validate(&record.name, "name")?;
        let mut record = record;
        record.backing_image_config = None;
        // IDs and names share one lookup namespace.
        if read_record(self, &record.name).await?.is_some()
            || read_alias(self, &record.id).await?.is_some()
        {
            return Err(RepositoryError::VolumeNameConflict { name: record.name });
        }
        // Write the record before claiming its name, so a name whose record is
        // missing is always stale rather than mid-creation.
        let bytes = encode_record(&record)?;
        let existing = self
            .update_document(
                DocumentKey::VolumeRecord(record.id.clone()),
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
                reason: format!("volume '{}' already exists", record.id),
            });
        }
        let claimed = claim_alias(self, &record.name, &record.id).await;
        if !matches!(claimed, Ok(true)) {
            if let Err(error) = self
                .delete_document(DocumentKey::VolumeRecord(record.id.clone()))
                .await
            {
                warn!(volume_id = %record.id, %error, "failed to roll back unnamed volume record");
            }
        }
        match claimed {
            Ok(true) => Ok(()),
            Ok(false) => Err(RepositoryError::VolumeNameConflict { name: record.name }),
            Err(error) => Err(error),
        }
    }

    async fn put_volume(&self, record: VolumeRecord) -> RepositoryResult<()> {
        validate(&record.id, "id")?;
        let mut record = record;
        record.backing_image_config = None;
        let volume_id = record.id.clone();
        update_record(self, &volume_id, move |existing| {
            let existing = existing.ok_or_else(|| RepositoryError::VolumeNotFound {
                lookup: record.id.clone(),
            })?;
            existing
                .validate_catalog_update(&record)
                .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
            let mut next = record.clone();
            // Reservation state is owned exclusively by the reservation APIs. A
            // concurrent backing/status update must not restore a released owner.
            next.reserved_by_sandbox_id = existing.reserved_by_sandbox_id;
            next.read_only_mounts = existing.read_only_mounts;
            Ok((Some(next), ()))
        })
        .await
    }

    async fn delete_volume(&self, volume_id: &str) -> RepositoryResult<()> {
        validate(volume_id, "id")?;
        let id = volume_id.to_owned();
        let deleted = update_record(self, volume_id, move |record| {
            let Some(mut record) = record else {
                return Ok((None, None));
            };
            if let Some(owner) = record.reserved_by_sandbox_id.as_deref() {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("volume '{id}' is reserved by sandbox '{owner}'"),
                });
            }
            if let Some(owner) = record.read_only_mounts.first() {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("volume '{id}' is mounted read-only by sandbox '{owner}'"),
                });
            }
            if record.deleting {
                return Ok((None, Some(record)));
            }
            // Mark the record first so reservations and name claims stop
            // treating it as live before it disappears.
            record.deleting = true;
            record.status = VolumeStatus::Failed;
            Ok((Some(record.clone()), Some(record)))
        })
        .await?;
        let Some(record) = deleted else {
            return Ok(());
        };
        self.delete_document(DocumentKey::VolumeRecord(volume_id.to_owned()))
            .await?;
        // A create may already have reclaimed the name from the deleting record.
        let owned = encode_alias(volume_id)?;
        self.update_document(
            DocumentKey::VolumeAlias(record.name),
            Box::new(move |current| {
                Ok(if current == Some(owned.as_slice()) {
                    DocumentChange::Delete
                } else {
                    DocumentChange::Keep
                })
            }),
        )
        .await?;
        Ok(())
    }

    async fn reserve_volume(
        &self,
        volume_id: &str,
        owner: &str,
    ) -> RepositoryResult<Option<String>> {
        validate(volume_id, "id")?;
        validate(owner, "owner")?;
        let (id, owner) = (volume_id.to_owned(), owner.to_owned());
        update_record(self, volume_id, move |record| {
            let mut record = usable_record(record, &id)?;
            if record.mode == VolumeMode::ReadOnly {
                return Ok((None, None));
            }
            if let Some(existing) = record.reserved_by_sandbox_id.clone() {
                return Ok((None, (existing != owner).then_some(existing)));
            }
            record.reserved_by_sandbox_id = Some(owner.clone());
            Ok((Some(record), None))
        })
        .await
    }

    async fn reserve_read_only_volume(&self, volume_id: &str, owner: &str) -> RepositoryResult<()> {
        validate(volume_id, "id")?;
        validate(owner, "owner")?;
        let (id, owner) = (volume_id.to_owned(), owner.to_owned());
        update_record(self, volume_id, move |record| {
            let mut record = usable_record(record, &id)?;
            if record.mode != VolumeMode::ReadOnly {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("volume '{}' is not read-only", record.id),
                });
            }
            if record.read_only_mounts.contains(&owner) {
                return Ok((None, ()));
            }
            record.read_only_mounts.push(owner.clone());
            Ok((Some(record), ()))
        })
        .await
    }

    async fn replace_volume_owner_for(
        &self,
        volume_id: &str,
        from: &str,
        to: Option<&str>,
    ) -> RepositoryResult<()> {
        validate(volume_id, "id")?;
        validate(from, "owner")?;
        if let Some(to) = to {
            validate(to, "owner")?;
        }
        if to == Some(from) {
            return Ok(());
        }
        let (from, to) = (from.to_owned(), to.map(str::to_owned));
        update_record(self, volume_id, move |record| {
            // Missing and deleting volumes have no owners left to change.
            let Some(mut record) = record.filter(|record| !record.deleting) else {
                return Ok((None, ()));
            };
            let changed = record.replace_owner(&from, to.as_deref());
            Ok((changed.then_some(record), ()))
        })
        .await
    }
}

/// Atomically applies `decide` to one volume record and writes the record it
/// returns, if any. `decide` must be pure: it runs again on the state the
/// update was applied to, to report the outcome.
async fn update_record<R, T>(
    repository: &R,
    volume_id: &str,
    decide: impl Fn(Option<VolumeRecord>) -> RepositoryResult<(Option<VolumeRecord>, T)>
        + Clone
        + Send
        + Sync
        + 'static,
) -> RepositoryResult<T>
where
    R: Repository + ?Sized,
{
    let id = volume_id.to_owned();
    let update = decide.clone();
    let applied_to = repository
        .update_document(
            DocumentKey::VolumeRecord(volume_id.to_owned()),
            Box::new(move |bytes| {
                let record = bytes.map(|bytes| decode_record(&id, bytes)).transpose()?;
                Ok(match update(record)?.0 {
                    Some(next) => DocumentChange::Put(encode_record(&next)?),
                    None => DocumentChange::Keep,
                })
            }),
        )
        .await?;
    let record = applied_to
        .map(|bytes| decode_record(volume_id, &bytes))
        .transpose()?;
    Ok(decide(record)?.1)
}

/// Returns a record that can take a new reservation.
fn usable_record(record: Option<VolumeRecord>, volume_id: &str) -> RepositoryResult<VolumeRecord> {
    let record = record.ok_or_else(|| RepositoryError::InvalidRequest {
        reason: format!("volume '{volume_id}' does not exist in the repository"),
    })?;
    if record.deleting || record.status != VolumeStatus::Ready {
        return Err(RepositoryError::InvalidRequest {
            reason: format!("volume '{}' is not usable", record.id),
        });
    }
    Ok(record)
}

/// Points `name` at `volume_id` unless a live volume already owns it.
async fn claim_alias<R>(repository: &R, name: &str, volume_id: &str) -> RepositoryResult<bool>
where
    R: Repository + ?Sized,
{
    let key = DocumentKey::VolumeAlias(name.to_owned());
    let claimed = encode_alias(volume_id)?;
    for _ in 0..MAX_ALIAS_CLAIM_ATTEMPTS {
        let current = repository.get_document(key.clone()).await?;
        if let Some(bytes) = current.as_deref() {
            let existing = decode_alias(bytes)?;
            if existing == volume_id {
                return Ok(true);
            }
            // Records are written before their names, so a name whose record
            // is missing or being deleted is stale.
            if read_record(repository, &existing)
                .await?
                .is_some_and(|record| !record.deleting)
            {
                return Ok(false);
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
            return Ok(true);
        }
    }
    Err(RepositoryError::Backend {
        message: format!("volume alias '{name}' changed too often while claiming it"),
        source: None,
    })
}

async fn read_record<R>(repository: &R, volume_id: &str) -> RepositoryResult<Option<VolumeRecord>>
where
    R: Repository + ?Sized,
{
    validate(volume_id, "id")?;
    repository
        .get_document(DocumentKey::VolumeRecord(volume_id.to_owned()))
        .await?
        .map(|bytes| decode_record(volume_id, &bytes))
        .transpose()
}

async fn read_alias<R>(repository: &R, name: &str) -> RepositoryResult<Option<String>>
where
    R: Repository + ?Sized,
{
    repository
        .get_document(DocumentKey::VolumeAlias(name.to_owned()))
        .await?
        .map(|bytes| decode_alias(&bytes))
        .transpose()
}

fn decode_record(volume_id: &str, bytes: &[u8]) -> RepositoryResult<VolumeRecord> {
    let record: VolumeRecord = serde_json::from_slice(bytes)
        .map_err(|error| RepositoryError::backend("parse volume record", error))?;
    validate(&record.id, "id")?;
    if record.id != volume_id {
        return Err(RepositoryError::InvalidRequest {
            reason: format!(
                "volume record id '{}' does not match key '{volume_id}'",
                record.id
            ),
        });
    }
    Ok(record)
}

fn encode_record(record: &VolumeRecord) -> RepositoryResult<Vec<u8>> {
    serde_json::to_vec_pretty(record)
        .map_err(|error| RepositoryError::backend("serialize volume record", error))
}

fn encode_alias(volume_id: &str) -> RepositoryResult<Vec<u8>> {
    serde_json::to_vec(volume_id)
        .map_err(|error| RepositoryError::backend("serialize volume alias", error))
}

fn decode_alias(bytes: &[u8]) -> RepositoryResult<String> {
    let volume_id: String = serde_json::from_slice(bytes)
        .map_err(|error| RepositoryError::backend("parse volume alias", error))?;
    validate(&volume_id, "id")?;
    Ok(volume_id)
}

fn validate(value: &str, kind: &str) -> RepositoryResult<()> {
    if !is_valid_volume_component(value) {
        return Err(RepositoryError::InvalidRequest {
            reason: format!("invalid volume {kind} '{value}'"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::repository::backends::{PosixFsBackend, PosixFsBackendConfig};

    fn repository(root: &Path) -> Arc<dyn Repository> {
        PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join("cache")),
            runtime_cache_root: None,
        })
        .unwrap()
        .repository()
    }

    fn volume_record(index: usize) -> VolumeRecord {
        VolumeRecord {
            id: format!("vol_{index:06}"),
            name: format!("data-{index:06}"),
            mode: VolumeMode::Exclusive,
            size_mb: crate::volume::DEFAULT_VOLUME_SIZE_MB,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_image_config: None,
            backing_layers: Vec::new(),
            read_only_mounts: Vec::new(),
            deleting: false,
        }
    }

    #[tokio::test]
    async fn posix_catalog_keeps_existing_layout_and_locks() {
        let temp = tempfile::tempdir().unwrap();
        let repository = repository(temp.path());
        let mut expected_ids = Vec::new();
        for index in 0..40 {
            let record = volume_record(index);
            expected_ids.push(record.id.clone());
            repository.create_volume(record).await.unwrap();
        }
        assert_eq!(
            repository
                .get_volume("data-000012")
                .await
                .unwrap()
                .unwrap()
                .id,
            "vol_000012"
        );
        assert!(matches!(
            repository
                .create_volume(VolumeRecord {
                    id: "vol_conflict".to_owned(),
                    ..volume_record(12)
                })
                .await,
            Err(RepositoryError::VolumeNameConflict { .. })
        ));
        assert!(repository
            .get_volume("vol_conflict")
            .await
            .unwrap()
            .is_none());
        let root = temp.path().join("repository");
        for path in [
            "volumes/records/vol_000012.json",
            "volumes/records/vol_000012.lock",
            "volumes/aliases/data-000012",
            "volumes/aliases/data-000012.lock",
        ] {
            assert!(root.join(path).is_file(), "{path}");
        }

        let mut actual_ids = Vec::new();
        let mut after = None;
        loop {
            let page = repository
                .list_volumes_page(after.as_deref(), 7)
                .await
                .unwrap();
            actual_ids.extend(page.records.into_iter().map(|record| record.id));
            let Some(next) = page.next_volume_id else {
                break;
            };
            after = Some(next);
        }
        assert_eq!(actual_ids, expected_ids);

        let id = &expected_ids[0];
        assert_eq!(
            repository.reserve_volume(id, "sandbox-a").await.unwrap(),
            None
        );
        assert_eq!(
            repository.reserve_volume(id, "sandbox-b").await.unwrap(),
            Some("sandbox-a".to_owned())
        );
        repository
            .replace_volume_owner_for(id, "sandbox-a", Some("sandbox-b"))
            .await
            .unwrap();
        let record = repository.get_volume(id).await.unwrap().unwrap();
        assert_eq!(record.reserved_by_sandbox_id.as_deref(), Some("sandbox-b"));
        assert!(repository.delete_volume(id).await.is_err());
    }

    /// Hides one volume record from reads but not from listings, like a
    /// delete that lands between the two.
    struct DeletedAfterListing {
        inner: Arc<dyn Repository>,
        hidden: &'static str,
    }

    #[async_trait]
    impl Repository for DeletedAfterListing {
        async fn get_document(&self, key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
            match &key {
                DocumentKey::VolumeRecord(id) if id == self.hidden => Ok(None),
                _ => self.inner.get_document(key).await,
            }
        }

        async fn list_documents(
            &self,
            collection: DocumentCollection,
            after: Option<&str>,
            limit: usize,
        ) -> RepositoryResult<Vec<String>> {
            self.inner.list_documents(collection, after, limit).await
        }
    }

    #[tokio::test]
    async fn listing_skips_volumes_deleted_since_the_listing() {
        let temp = tempfile::tempdir().unwrap();
        let inner = repository(temp.path());
        for index in 0..3 {
            inner.create_volume(volume_record(index)).await.unwrap();
        }
        let repository = DeletedAfterListing {
            inner,
            hidden: "vol_000001",
        };
        let page = repository.list_volumes_page(None, 2).await.unwrap();
        assert_eq!(page.records, vec![volume_record(0)]);
        assert_eq!(page.next_volume_id.as_deref(), Some("vol_000001"));
        let page = repository
            .list_volumes_page(page.next_volume_id.as_deref(), 2)
            .await
            .unwrap();
        assert_eq!(page.records, vec![volume_record(2)]);
        assert_eq!(page.next_volume_id, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_creates_of_one_name_leave_one_volume() {
        let temp = tempfile::tempdir().unwrap();
        let creates = (0..16)
            .map(|index| {
                let repository = repository(temp.path());
                let record = VolumeRecord {
                    name: "shared".to_owned(),
                    ..volume_record(index)
                };
                tokio::spawn(async move {
                    let id = record.id.clone();
                    repository.create_volume(record).await.map(|()| id)
                })
            })
            .collect::<Vec<_>>();
        let mut created = Vec::new();
        for create in creates {
            match create.await.unwrap() {
                Ok(id) => created.push(id),
                Err(RepositoryError::VolumeNameConflict { .. }) => {}
                Err(error) => panic!("unexpected create error: {error}"),
            }
        }
        assert_eq!(created.len(), 1);
        let repository = repository(temp.path());
        assert_eq!(
            repository.get_volume("shared").await.unwrap().unwrap().id,
            created[0]
        );
        let page = repository.list_volumes_page(None, 100).await.unwrap();
        assert_eq!(
            page.records
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            created
        );
    }

    #[tokio::test]
    async fn reservations_require_a_live_ready_volume() {
        let temp = tempfile::tempdir().unwrap();
        let repository = repository(temp.path());
        let mut record = volume_record(0);
        repository.create_volume(record.clone()).await.unwrap();
        for status in [VolumeStatus::Uploading, VolumeStatus::Failed] {
            record.status = status;
            repository.put_volume(record.clone()).await.unwrap();
            assert!(repository
                .reserve_volume(&record.id, "sandbox")
                .await
                .is_err());
        }
        assert!(repository
            .reserve_volume("vol_missing", "sandbox")
            .await
            .is_err());
        repository
            .replace_volume_owner_for("vol_missing", "sandbox", None)
            .await
            .unwrap();

        // A volume that is being deleted takes no new owners, and its name
        // can be claimed by a new volume.
        let mut deleting = VolumeRecord {
            reserved_by_sandbox_id: Some("old".to_owned()),
            ..volume_record(1)
        };
        repository.create_volume(deleting.clone()).await.unwrap();
        deleting.deleting = true;
        repository
            .put_document(
                DocumentKey::VolumeRecord(deleting.id.clone()),
                encode_record(&deleting).unwrap(),
            )
            .await
            .unwrap();
        assert!(repository
            .reserve_volume(&deleting.id, "new")
            .await
            .is_err());
        repository
            .replace_volume_owner_for(&deleting.id, "old", Some("new"))
            .await
            .unwrap();
        let unchanged = repository.get_volume(&deleting.id).await.unwrap().unwrap();
        assert_eq!(unchanged.reserved_by_sandbox_id.as_deref(), Some("old"));
        repository
            .create_volume(VolumeRecord {
                name: deleting.name.clone(),
                ..volume_record(2)
            })
            .await
            .unwrap();
        assert_eq!(
            repository
                .get_volume(&deleting.name)
                .await
                .unwrap()
                .unwrap()
                .id,
            volume_record(2).id
        );
    }

    #[tokio::test]
    async fn names_pointing_at_missing_records_are_reclaimed() {
        let temp = tempfile::tempdir().unwrap();
        let repository = repository(temp.path());
        repository
            .put_document(
                DocumentKey::VolumeAlias("data".to_owned()),
                serde_json::to_vec("vol_gone").unwrap(),
            )
            .await
            .unwrap();
        assert!(repository.get_volume("data").await.unwrap().is_none());
        let record = VolumeRecord {
            name: "data".to_owned(),
            ..volume_record(0)
        };
        repository.create_volume(record.clone()).await.unwrap();
        assert_eq!(
            repository.get_volume("data").await.unwrap().unwrap().id,
            record.id
        );
        repository.delete_volume(&record.id).await.unwrap();
        assert!(repository.get_volume("data").await.unwrap().is_none());
        assert!(!temp.path().join("repository/volumes/aliases/data").exists());
    }
}
