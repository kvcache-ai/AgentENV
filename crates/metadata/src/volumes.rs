//! Persistent volume records, reservations, and BuildKit cache-seed coordination.

use std::collections::BTreeSet;

use agentenv_domain::{
    is_valid_volume_component, BuildCacheState, LayerRef, ResourceKind, VolumeMode, VolumePurpose,
    VolumeRecord, VolumeRecordPage, VolumeStatus,
};
use sqlx::{
    AssertSqlSafe, ColumnIndex, Decode, Encode, Executor, IntoArguments, Pool, QueryBuilder,
    Transaction, Type,
};

use crate::database::{begin_tx, commit_tx, dispatch_db, QueryRows, SqlDialect};
use crate::layer_graph::{
    attach_layers, collect_disk_relations, now_unix_ms, release_owner_heads, sync_owner_heads,
};
use crate::{MetaError, MetaResult, MetaServer};

const VOLUME_COLUMNS: &str = "volume_id, name, purpose, mode, size_mb, status, backing_layers_json, reserved_by, ro_mounts_json";

#[derive(sqlx::FromRow)]
struct VolumeRow {
    volume_id: String,
    name: String,
    purpose: String,
    mode: String,
    size_mb: i64,
    status: String,
    backing_layers_json: String,
    reserved_by: Option<String>,
    ro_mounts_json: String,
}

impl VolumeRow {
    fn into_record(self) -> MetaResult<VolumeRecord> {
        let purpose = VolumePurpose::parse(&self.purpose)
            .map_err(|error| MetaError::backend("decode volume purpose", error))?;
        let mode = match self.mode.as_str() {
            "exclusive" => VolumeMode::Exclusive,
            "read_only" => VolumeMode::ReadOnly,
            other => {
                return Err(MetaError::backend(
                    "decode volume mode",
                    format!("unknown volume mode '{other}'"),
                ))
            }
        };
        let status = VolumeStatus::parse(&self.status)
            .map_err(|error| MetaError::backend("decode volume status", error))?;
        let size_mb = u64::try_from(self.size_mb)
            .map_err(|error| MetaError::backend("decode volume size", error))?;
        let backing_layers: Vec<LayerRef> = serde_json::from_str(&self.backing_layers_json)
            .map_err(|error| MetaError::backend("decode volume backing layers", error))?;
        let read_only_mounts: Vec<String> = serde_json::from_str(&self.ro_mounts_json)
            .map_err(|error| MetaError::backend("decode volume read-only mounts", error))?;
        Ok(VolumeRecord {
            id: self.volume_id,
            name: self.name,
            purpose,
            mode,
            size_mb,
            status,
            reserved_by_sandbox_id: self.reserved_by,
            backing_layers,
            read_only_mounts,
        })
    }
}

fn mode_as_str(mode: VolumeMode) -> &'static str {
    match mode {
        VolumeMode::Exclusive => "exclusive",
        VolumeMode::ReadOnly => "read_only",
    }
}

fn validate_component(value: &str, kind: &str) -> MetaResult<()> {
    if !is_valid_volume_component(value) {
        return Err(MetaError::InvalidRequest {
            reason: format!("invalid volume {kind} '{value}'"),
        });
    }
    Ok(())
}

fn validate_new_record(record: &VolumeRecord) -> MetaResult<i64> {
    validate_component(&record.id, "id")?;
    validate_component(&record.name, "name")?;
    if record.size_mb == 0 {
        return Err(MetaError::InvalidRequest {
            reason: "volume size must be greater than zero".into(),
        });
    }
    let size_mb = i64::try_from(record.size_mb).map_err(|_| MetaError::InvalidRequest {
        reason: "volume size exceeds supported range".into(),
    })?;
    if record.mode == VolumeMode::ReadOnly && record.reserved_by_sandbox_id.is_some() {
        return Err(MetaError::InvalidRequest {
            reason: "read-only volumes cannot carry an exclusive reservation".into(),
        });
    }
    if record.purpose == VolumePurpose::BuildCacheSeed && record.mode != VolumeMode::ReadOnly {
        return Err(MetaError::InvalidRequest {
            reason: "build cache seeds must use read-only mode".into(),
        });
    }
    if let Some(owner) = record.reserved_by_sandbox_id.as_deref() {
        validate_component(owner, "owner")?;
    }
    for owner in &record.read_only_mounts {
        validate_component(owner, "owner")?;
    }
    Ok(size_mb)
}

fn state_from_rows(rows: Vec<(String, String)>) -> BuildCacheState {
    let mut current = None;
    let mut retired = BTreeSet::new();
    for (id, purpose) in rows {
        if purpose == VolumePurpose::BuildCacheSeed.as_str() {
            current = Some(id);
        } else if purpose == VolumePurpose::BuildCacheRetired.as_str() {
            retired.insert(id);
        }
    }
    BuildCacheState { current, retired }
}

impl MetaServer {
    pub async fn get_volume(&self, reference: &str) -> MetaResult<Option<VolumeRecord>> {
        validate_component(reference, "reference")?;
        dispatch_db!(&self.database, |pool| VolumeStore(pool)
            .get(reference)
            .await)
    }

    pub async fn list_volumes_page(
        &self,
        after_volume_id: Option<&str>,
        limit: u32,
        include_internal: bool,
    ) -> MetaResult<VolumeRecordPage> {
        if limit == 0 {
            return Err(MetaError::InvalidRequest {
                reason: "volume page limit must be greater than zero".into(),
            });
        }
        if let Some(after) = after_volume_id {
            validate_component(after, "id")?;
        }
        dispatch_db!(&self.database, |pool| {
            VolumeStore(pool)
                .list_page(after_volume_id, limit, include_internal)
                .await
        })
    }

    pub async fn create_volume(
        &self,
        record: VolumeRecord,
        from_volume_id: Option<&str>,
        restored_layers: &[String],
    ) -> MetaResult<VolumeRecord> {
        let size_mb = validate_new_record(&record)?;
        if let Some(source_id) = from_volume_id {
            validate_component(source_id, "id")?;
        }
        dispatch_db!(&self.database, |pool| {
            VolumeStore(pool)
                .create(record, size_mb, from_volume_id, restored_layers)
                .await
        })
    }

    pub async fn update_volume(
        &self,
        record: VolumeRecord,
        restored_layers: &[String],
    ) -> MetaResult<VolumeRecord> {
        validate_component(&record.id, "id")?;
        dispatch_db!(&self.database, |pool| {
            VolumeStore(pool).update(record, restored_layers).await
        })
    }

    pub async fn delete_volume(&self, volume_id: &str) -> MetaResult<()> {
        validate_component(volume_id, "id")?;
        dispatch_db!(&self.database, |pool| VolumeStore(pool)
            .delete(volume_id)
            .await)
    }

    pub async fn reserve_volume(&self, volume_id: &str, owner: &str) -> MetaResult<Option<String>> {
        validate_component(volume_id, "id")?;
        validate_component(owner, "owner")?;
        dispatch_db!(&self.database, |pool| {
            VolumeStore(pool).reserve(volume_id, owner).await
        })
    }

    pub async fn reserve_read_only_volume(&self, volume_id: &str, owner: &str) -> MetaResult<()> {
        validate_component(volume_id, "id")?;
        validate_component(owner, "owner")?;
        dispatch_db!(&self.database, |pool| {
            VolumeStore(pool).reserve_read_only(volume_id, owner).await
        })
    }

    pub async fn replace_volume_owner(
        &self,
        volume_id: &str,
        from: &str,
        to: Option<&str>,
    ) -> MetaResult<()> {
        validate_component(volume_id, "id")?;
        validate_component(from, "owner")?;
        if let Some(to) = to {
            validate_component(to, "owner")?;
        }
        if to == Some(from) {
            return Ok(());
        }
        dispatch_db!(&self.database, |pool| {
            VolumeStore(pool).replace_owner(volume_id, from, to).await
        })
    }

    pub async fn get_build_cache_state(&self) -> MetaResult<BuildCacheState> {
        let rows = dispatch_db!(&self.database, |pool| {
            sqlx::query_as::<_, (String, String)>(
                "SELECT volume_id, purpose FROM volumes
                 WHERE purpose IN ('build_cache_seed', 'build_cache_retired')
                 ORDER BY volume_id ASC",
            )
            .fetch_all(pool)
            .await
        })
        .map_err(|error| MetaError::backend("read build cache state", error))?;
        Ok(state_from_rows(rows))
    }

    pub async fn replace_build_cache_head(&self, volume_id: &str) -> MetaResult<BuildCacheState> {
        validate_component(volume_id, "id")?;
        dispatch_db!(&self.database, |pool| {
            VolumeStore(pool).replace_build_cache_head(volume_id).await
        })
    }
}

struct VolumeStore<'a, D: SqlDialect>(&'a Pool<D>);

impl<D> VolumeStore<'_, D>
where
    D: SqlDialect<QueryResult: QueryRows, Arguments: IntoArguments<D>>,
    for<'p> &'p Pool<D>: Executor<'p, Database = D>,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> Option<&'b str>: Encode<'b, D> + Type<D>,
    for<'b> String: Encode<'b, D> + Decode<'b, D> + Type<D>,
    for<'b> Option<String>: Decode<'b, D> + Type<D>,
    for<'b> i64: Encode<'b, D> + Type<D>,
    for<'r> VolumeRow: sqlx::FromRow<'r, D::Row>,
    usize: ColumnIndex<D::Row>,
{
    async fn lock_for_update(
        tx: &mut Transaction<'_, D>,
        volume_id: &str,
        context: &str,
    ) -> MetaResult<Option<VolumeRecord>> {
        let row = sqlx::query_as::<D, VolumeRow>(AssertSqlSafe(format!(
            "SELECT {VOLUME_COLUMNS} FROM volumes WHERE volume_id = $1{}",
            D::FOR_UPDATE,
        )))
        .bind(volume_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| MetaError::backend(context, error))?;
        row.map(VolumeRow::into_record).transpose()
    }

    async fn get(&self, reference: &str) -> MetaResult<Option<VolumeRecord>> {
        let by_id = AssertSqlSafe(format!(
            "SELECT {VOLUME_COLUMNS} FROM volumes WHERE volume_id = $1"
        ));
        let row = match sqlx::query_as::<D, VolumeRow>(by_id)
            .bind(reference)
            .fetch_optional(self.0)
            .await
            .map_err(|error| MetaError::backend("get volume by id", error))?
        {
            Some(row) => Some(row),
            None => sqlx::query_as::<D, VolumeRow>(AssertSqlSafe(format!(
                "SELECT {VOLUME_COLUMNS} FROM volumes WHERE name = $1"
            )))
            .bind(reference)
            .fetch_optional(self.0)
            .await
            .map_err(|error| MetaError::backend("get volume by name", error))?,
        };
        row.map(VolumeRow::into_record).transpose()
    }

    async fn list_page(
        &self,
        after_volume_id: Option<&str>,
        limit: u32,
        include_internal: bool,
    ) -> MetaResult<VolumeRecordPage> {
        let mut query =
            QueryBuilder::<D>::new(format!("SELECT {VOLUME_COLUMNS} FROM volumes WHERE "));
        query.push(if include_internal {
            "1 = 1"
        } else {
            "purpose = 'user'"
        });
        if let Some(after) = after_volume_id {
            query.push(" AND volume_id > ").push_bind(after);
        }
        let rows = query
            .push(" ORDER BY volume_id ASC LIMIT ")
            .push_bind(i64::from(limit).saturating_add(1))
            .build_query_as::<VolumeRow>()
            .fetch_all(self.0)
            .await
            .map_err(|error| MetaError::backend("list volumes", error))?;
        let limit_usize = limit as usize;
        let has_more = rows.len() > limit_usize;
        let mut records = Vec::with_capacity(rows.len().min(limit_usize));
        for row in rows.into_iter().take(limit_usize) {
            records.push(row.into_record()?);
        }
        let next_volume_id = has_more
            .then(|| records.last().map(|record| record.id.clone()))
            .flatten();
        Ok(VolumeRecordPage {
            records,
            next_volume_id,
        })
    }

    async fn create(
        &self,
        mut record: VolumeRecord,
        size_mb: i64,
        from_volume_id: Option<&str>,
        restored_layers: &[String],
    ) -> MetaResult<VolumeRecord> {
        let ro_mounts_json = serde_json::to_string(&record.read_only_mounts)
            .map_err(|error| MetaError::backend("encode read-only mounts", error))?;
        loop {
            let mut tx = begin_tx(self.0, "begin volume creation").await?;
            let mut backing_layers = record.backing_layers.clone();
            if let Some(source_id) = from_volume_id {
                let source = sqlx::query_as::<D, (String, String)>(AssertSqlSafe(format!(
                    "SELECT status, backing_layers_json FROM volumes WHERE volume_id = $1{}",
                    D::FOR_SHARE,
                )))
                .bind(source_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("lock source volume for clone", error))?;
                let Some((source_status, source_layers_json)) = source else {
                    return Err(MetaError::VolumeNotFound {
                        lookup: source_id.to_owned(),
                    });
                };
                if source_status != VolumeStatus::Ready.as_str() {
                    return Err(MetaError::Unavailable {
                        reason: format!("source volume '{source_id}' is not ready"),
                    });
                }
                if backing_layers.is_empty() {
                    backing_layers =
                        serde_json::from_str(&source_layers_json).map_err(|error| {
                            MetaError::backend("decode source volume layers", error)
                        })?;
                }
            }

            // IDs and names share one lookup namespace.
            let conflicts = sqlx::query_as::<D, (String, String)>(
                "SELECT volume_id, name FROM volumes
                 WHERE volume_id = $1 OR name = $1 OR volume_id = $2 OR name = $2",
            )
            .bind(record.id.as_str())
            .bind(record.name.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("check volume conflicts", error))?;
            if conflicts.iter().any(|(id, _)| id == &record.id) {
                return Err(MetaError::InvalidRequest {
                    reason: format!("volume '{}' already exists", record.id),
                });
            }
            if !conflicts.is_empty() {
                return Err(MetaError::VolumeNameConflict {
                    name: record.name.clone(),
                });
            }

            let backing_layers_json = serde_json::to_string(&backing_layers)
                .map_err(|error| MetaError::backend("encode volume layers", error))?;
            let relations = collect_disk_relations(&backing_layers)?;
            let inserted = sqlx::query::<D>(
                "INSERT INTO volumes (volume_id, name, purpose, mode, size_mb, status, backing_layers_json, reserved_by, ro_mounts_json, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)",
            )
            .bind(record.id.as_str())
            .bind(record.name.as_str())
            .bind(record.purpose.as_str())
            .bind(mode_as_str(record.mode))
            .bind(size_mb)
            .bind(record.status.as_str())
            .bind(backing_layers_json.as_str())
            .bind(record.reserved_by_sandbox_id.as_deref())
            .bind(ro_mounts_json.as_str())
            .bind(now_unix_ms())
            .execute(&mut *tx)
            .await;
            if let Err(error) = inserted {
                let _ = tx.rollback().await;
                if error
                    .as_database_error()
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
                {
                    let exists: Option<String> =
                        sqlx::query_scalar("SELECT volume_id FROM volumes WHERE volume_id = $1")
                            .bind(record.id.as_str())
                            .fetch_optional(self.0)
                            .await
                            .map_err(|lookup| {
                                MetaError::backend("read conflicting volume", lookup)
                            })?;
                    return Err(if exists.is_some() {
                        MetaError::InvalidRequest {
                            reason: format!("volume '{}' already exists", record.id),
                        }
                    } else {
                        MetaError::VolumeNameConflict {
                            name: record.name.clone(),
                        }
                    });
                }
                return Err(MetaError::backend("create volume", error));
            }

            if !attach_layers(
                &mut tx,
                ResourceKind::Volume,
                &record.id,
                &relations,
                restored_layers,
            )
            .await?
            {
                tx.rollback()
                    .await
                    .map_err(|error| MetaError::backend("restart volume layer checks", error))?;
                continue;
            }
            commit_tx(tx, "commit volume creation").await?;
            record.backing_layers = backing_layers;
            return Ok(record);
        }
    }

    async fn update(
        &self,
        record: VolumeRecord,
        restored_layers: &[String],
    ) -> MetaResult<VolumeRecord> {
        let backing_layers_json = serde_json::to_string(&record.backing_layers)
            .map_err(|error| MetaError::backend("encode volume layers", error))?;
        let relations = collect_disk_relations(&record.backing_layers)?;
        loop {
            let mut tx = begin_tx(self.0, "begin volume update").await?;
            let Some(existing) =
                Self::lock_for_update(&mut tx, &record.id, "lock volume for update").await?
            else {
                return Err(MetaError::VolumeNotFound {
                    lookup: record.id.clone(),
                });
            };
            existing
                .validate_update(&record)
                .map_err(|reason| MetaError::InvalidRequest { reason })?;
            let layers_changed = existing.backing_layers != record.backing_layers;
            let mut next = record.clone();
            next.purpose = existing.purpose;
            next.reserved_by_sandbox_id = existing.reserved_by_sandbox_id;
            next.read_only_mounts = existing.read_only_mounts;

            sqlx::query::<D>(
                "UPDATE volumes
                 SET status = $2, backing_layers_json = $3, updated_at = $4
                 WHERE volume_id = $1",
            )
            .bind(next.id.as_str())
            .bind(next.status.as_str())
            .bind(backing_layers_json.as_str())
            .bind(now_unix_ms())
            .execute(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("update volume", error))?;

            if layers_changed
                && !sync_owner_heads(
                    &mut tx,
                    ResourceKind::Volume,
                    &next.id,
                    &relations,
                    restored_layers,
                )
                .await?
            {
                tx.rollback().await.map_err(|error| {
                    MetaError::backend("restart volume update layer checks", error)
                })?;
                continue;
            }
            commit_tx(tx, "commit volume update").await?;
            return Ok(next);
        }
    }

    async fn delete(&self, volume_id: &str) -> MetaResult<()> {
        let mut tx = begin_tx(self.0, "begin volume deletion").await?;
        let Some(existing) =
            Self::lock_for_update(&mut tx, volume_id, "lock volume for deletion").await?
        else {
            return Ok(());
        };
        if let Some(owner) = existing
            .reserved_by_sandbox_id
            .or_else(|| existing.read_only_mounts.into_iter().next())
        {
            return Err(MetaError::VolumeReserved {
                id: existing.id,
                owner,
            });
        }
        if existing.purpose == VolumePurpose::BuildCacheSeed {
            return Err(MetaError::Unavailable {
                reason: format!("volume '{}' is the active build cache seed", existing.id),
            });
        }
        sqlx::query::<D>("DELETE FROM volumes WHERE volume_id = $1")
            .bind(volume_id)
            .execute(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("delete volume row", error))?;
        release_owner_heads(&mut tx, ResourceKind::Volume, volume_id).await?;
        commit_tx(tx, "commit volume deletion").await
    }

    async fn reserve(&self, volume_id: &str, owner: &str) -> MetaResult<Option<String>> {
        let mut tx = begin_tx(self.0, "begin volume reservation").await?;
        let Some(existing) =
            Self::lock_for_update(&mut tx, volume_id, "lock volume for reservation").await?
        else {
            return Err(MetaError::VolumeNotFound {
                lookup: volume_id.to_owned(),
            });
        };
        if existing.status != VolumeStatus::Ready {
            return Err(MetaError::Unavailable {
                reason: format!("volume '{}' is not usable", existing.id),
            });
        }
        if existing.mode == VolumeMode::ReadOnly {
            return Ok(None);
        }
        if let Some(current) = existing.reserved_by_sandbox_id {
            return Ok((current != owner).then_some(current));
        }
        sqlx::query::<D>(
            "UPDATE volumes SET reserved_by = $2, updated_at = $3 WHERE volume_id = $1",
        )
        .bind(volume_id)
        .bind(owner)
        .bind(now_unix_ms())
        .execute(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("reserve volume", error))?;
        commit_tx(tx, "commit volume reservation").await?;
        Ok(None)
    }

    async fn reserve_read_only(&self, volume_id: &str, owner: &str) -> MetaResult<()> {
        let mut tx = begin_tx(self.0, "begin read-only reservation").await?;
        let Some(mut existing) =
            Self::lock_for_update(&mut tx, volume_id, "lock volume for read-only mount").await?
        else {
            return Err(MetaError::VolumeNotFound {
                lookup: volume_id.to_owned(),
            });
        };
        if existing.status != VolumeStatus::Ready {
            return Err(MetaError::Unavailable {
                reason: format!("volume '{}' is not usable", existing.id),
            });
        }
        if existing.mode != VolumeMode::ReadOnly {
            return Err(MetaError::InvalidRequest {
                reason: format!("volume '{}' is not read-only", existing.id),
            });
        }
        if !existing.read_only_mounts.iter().any(|entry| entry == owner) {
            existing.read_only_mounts.push(owner.to_owned());
            let ro_mounts_json = serde_json::to_string(&existing.read_only_mounts)
                .map_err(|error| MetaError::backend("encode read-only mounts", error))?;
            sqlx::query::<D>(
                "UPDATE volumes SET ro_mounts_json = $2, updated_at = $3 WHERE volume_id = $1",
            )
            .bind(volume_id)
            .bind(ro_mounts_json.as_str())
            .bind(now_unix_ms())
            .execute(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("record read-only mount", error))?;
        }
        commit_tx(tx, "commit read-only reservation").await
    }

    async fn replace_owner(&self, volume_id: &str, from: &str, to: Option<&str>) -> MetaResult<()> {
        let mut tx = begin_tx(self.0, "begin owner replacement").await?;
        let Some(mut existing) =
            Self::lock_for_update(&mut tx, volume_id, "lock volume for owner change").await?
        else {
            return Ok(());
        };
        if existing.replace_owner(from, to) {
            let ro_mounts_json = serde_json::to_string(&existing.read_only_mounts)
                .map_err(|error| MetaError::backend("encode read-only mounts", error))?;
            sqlx::query::<D>(
                "UPDATE volumes
                 SET reserved_by = $2, ro_mounts_json = $3, updated_at = $4
                 WHERE volume_id = $1",
            )
            .bind(volume_id)
            .bind(existing.reserved_by_sandbox_id.as_deref())
            .bind(ro_mounts_json.as_str())
            .bind(now_unix_ms())
            .execute(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("replace volume owner", error))?;
        }
        commit_tx(tx, "commit owner replacement").await
    }

    async fn replace_build_cache_head(&self, volume_id: &str) -> MetaResult<BuildCacheState> {
        let mut tx = begin_tx(self.0, "begin cache seed update").await?;
        let row = sqlx::query_as::<D, (String, String)>(AssertSqlSafe(format!(
            "SELECT mode, status FROM volumes WHERE volume_id = $1{}",
            D::FOR_UPDATE,
        )))
        .bind(volume_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("lock build cache seed volume", error))?;
        let Some((mode, status)) = row else {
            return Err(MetaError::VolumeNotFound {
                lookup: volume_id.to_owned(),
            });
        };
        if mode != mode_as_str(VolumeMode::ReadOnly) {
            return Err(MetaError::InvalidRequest {
                reason: format!("build cache seed '{volume_id}' must be read-only"),
            });
        }
        if status != VolumeStatus::Ready.as_str() {
            return Err(MetaError::Unavailable {
                reason: format!("build cache seed '{volume_id}' is not ready"),
            });
        }
        let now = now_unix_ms();
        sqlx::query::<D>(
            "UPDATE volumes
             SET purpose = 'build_cache_retired', updated_at = $2
             WHERE purpose = 'build_cache_seed' AND volume_id <> $1",
        )
        .bind(volume_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("retire previous build cache seed", error))?;
        sqlx::query::<D>(
            "UPDATE volumes
             SET purpose = 'build_cache_seed', updated_at = $2
             WHERE volume_id = $1",
        )
        .bind(volume_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("promote build cache seed", error))?;
        let rows = sqlx::query_as::<D, (String, String)>(
            "SELECT volume_id, purpose FROM volumes
             WHERE purpose IN ('build_cache_seed', 'build_cache_retired')
             ORDER BY volume_id ASC",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("read build cache state", error))?;
        commit_tx(tx, "commit build cache seed update").await?;
        Ok(state_from_rows(rows))
    }
}
