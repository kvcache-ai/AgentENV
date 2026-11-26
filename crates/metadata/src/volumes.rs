//! Persistent volume records, leases, and BuildKit cache-seed coordination.

use agentenv_domain::{
    is_valid_volume_component, LayerRef, ResourceKind, VolumeLeaseOp, VolumeListFilter, VolumeMode,
    VolumePurpose, VolumeRecord, VolumeRecordPage, VolumeStatus,
};
use sqlx::{AssertSqlSafe, Executor, QueryBuilder, Transaction};

use crate::database::{begin_tx, commit_tx, dispatch_db, impl_sql_backend, Db};
use crate::layer_graph::{collect_disk_relations, now_unix_ms};
use crate::{MetaError, MetaResult, MetaServer};

const VOLUME_COLUMNS: &str = "volume_id, name, purpose, mode, size_mb, status, backing_layers_json, reserved_by, ro_mounts_json";

#[derive(sqlx::FromRow)]
pub(crate) struct VolumeRow {
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
    if record.purpose == VolumePurpose::BuildCacheSeed {
        if record.mode != VolumeMode::ReadOnly {
            return Err(MetaError::InvalidRequest {
                reason: "build cache seeds must use read-only mode".into(),
            });
        }
        if record.status != VolumeStatus::Ready {
            return Err(MetaError::Unavailable {
                reason: format!("build cache seed '{}' is not ready", record.id),
            });
        }
    }
    if let Some(owner) = record.reserved_by_sandbox_id.as_deref() {
        validate_component(owner, "owner")?;
    }
    for owner in &record.read_only_mounts {
        validate_component(owner, "owner")?;
    }
    Ok(size_mb)
}

impl MetaServer {
    pub async fn get_volume(&self, reference: &str) -> MetaResult<Option<VolumeRecord>> {
        validate_component(reference, "reference")?;
        dispatch_db!(&self.database, |pool| Db(pool).get_volume(reference).await)
    }

    pub async fn list_volumes(&self, filter: VolumeListFilter) -> MetaResult<VolumeRecordPage> {
        if filter.limit == 0 {
            return Err(MetaError::InvalidRequest {
                reason: "volume page limit must be greater than zero".into(),
            });
        }
        if let Some(after) = filter.after_volume_id.as_deref() {
            validate_component(after, "id")?;
        }
        dispatch_db!(&self.database, |pool| Db(pool).list_volumes(&filter).await)
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
            Db(pool)
                .create_volume(record, size_mb, from_volume_id, restored_layers)
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
            Db(pool).update_volume(record, restored_layers).await
        })
    }

    pub async fn delete_volume(&self, volume_id: &str) -> MetaResult<()> {
        validate_component(volume_id, "id")?;
        dispatch_db!(&self.database, |pool| Db(pool)
            .delete_volume(volume_id)
            .await)
    }

    pub async fn update_volume_lease(
        &self,
        volume_id: &str,
        op: VolumeLeaseOp,
    ) -> MetaResult<Option<String>> {
        validate_component(volume_id, "id")?;
        match &op {
            VolumeLeaseOp::Acquire { owner } | VolumeLeaseOp::Release { owner } => {
                validate_component(owner, "owner")?;
            }
            VolumeLeaseOp::Transfer { from, to } => {
                validate_component(from, "owner")?;
                validate_component(to, "owner")?;
                if from == to {
                    return Ok(None);
                }
            }
        }
        dispatch_db!(&self.database, |pool| {
            Db(pool).update_volume_lease(volume_id, &op).await
        })
    }
}

impl_sql_backend! {
impl<D> Db<'_, D>
where
    for<'r> VolumeRow: sqlx::FromRow<'r, D::Row>,
{
    async fn check_volume_conflicts<'e, E>(
        executor: E,
        id: &str,
        name: &str,
        context: &str,
    ) -> MetaResult<()>
    where
        E: Executor<'e, Database = D>,
    {
        // IDs and names share one lookup namespace.
        let conflicts = sqlx::query_as::<D, (String, String)>(
            "SELECT volume_id, name FROM volumes
             WHERE volume_id = $1 OR name = $1 OR volume_id = $2 OR name = $2",
        )
        .bind(id)
        .bind(name)
        .fetch_all(executor)
        .await
        .map_err(|error| MetaError::backend(context, error))?;
        if conflicts.iter().any(|(existing_id, _)| existing_id == id) {
            return Err(MetaError::InvalidRequest {
                reason: format!("volume '{id}' already exists"),
            });
        }
        if !conflicts.is_empty() {
            return Err(MetaError::VolumeNameConflict {
                name: name.to_owned(),
            });
        }
        Ok(())
    }
    async fn lock_volume_for_update(
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

    async fn get_volume(&self, reference: &str) -> MetaResult<Option<VolumeRecord>> {
        let row = match sqlx::query_as::<D, VolumeRow>(AssertSqlSafe(format!(
            "SELECT {VOLUME_COLUMNS} FROM volumes WHERE volume_id = $1"
        )))
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

    async fn list_volumes(&self, filter: &VolumeListFilter) -> MetaResult<VolumeRecordPage> {
        let mut query =
            QueryBuilder::<D>::new(format!("SELECT {VOLUME_COLUMNS} FROM volumes WHERE 1 = 1"));
        if !filter.purposes.is_empty() {
            query.push(" AND purpose IN (");
            let mut sep = query.separated(", ");
            for purpose in &filter.purposes {
                sep.push_bind(purpose.as_str());
            }
            query.push(")");
        }
        if let Some(after) = filter.after_volume_id.as_deref() {
            query.push(" AND volume_id > ").push_bind(after);
        }
        let rows = query
            .push(" ORDER BY volume_id ASC LIMIT ")
            .push_bind(i64::from(filter.limit).saturating_add(1))
            .build_query_as::<VolumeRow>()
            .fetch_all(self.0)
            .await
            .map_err(|error| MetaError::backend("list volumes", error))?;
        let limit_usize = filter.limit as usize;
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

    async fn create_volume(
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

            Self::check_volume_conflicts(
                &mut *tx,
                &record.id,
                &record.name,
                "check volume conflicts",
            )
            .await?;

            let now = now_unix_ms();
            if record.purpose == VolumePurpose::BuildCacheSeed {
                // Atomically retire any prior active seed before inserting its successor.
                sqlx::query::<D>(
                    "UPDATE volumes
                     SET purpose = 'build_cache_retired', updated_at = $1
                     WHERE purpose = 'build_cache_seed'",
                )
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("retire previous build cache seed", error))?;
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
            .bind(now)
            .execute(&mut *tx)
            .await;
            if let Err(error) = inserted {
                let _ = tx.rollback().await;
                if error
                    .as_database_error()
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
                {
                    Self::check_volume_conflicts(
                        self.0,
                        &record.id,
                        &record.name,
                        "read conflicting volume",
                    )
                    .await?;
                    if record.purpose == VolumePurpose::BuildCacheSeed {
                        // Another writer concurrently promoted a build cache seed; retry to supersede it.
                        continue;
                    }
                }
                return Err(MetaError::backend("create volume", error));
            }

            if !Self::attach_layers(
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

    async fn update_volume(
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
                Self::lock_volume_for_update(&mut tx, &record.id, "lock volume for update").await?
            else {
                return Err(MetaError::VolumeNotFound {
                    lookup: record.id.clone(),
                });
            };
            validate_volume_update(&existing, &record)
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
                && !Self::sync_owner_heads(
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

    async fn delete_volume(&self, volume_id: &str) -> MetaResult<()> {
        let mut tx = begin_tx(self.0, "begin volume deletion").await?;
        let Some(existing) =
            Self::lock_volume_for_update(&mut tx, volume_id, "lock volume for deletion").await?
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
        Self::release_owner_heads(&mut tx, ResourceKind::Volume, volume_id).await?;
        commit_tx(tx, "commit volume deletion").await
    }

    async fn update_volume_lease(
        &self,
        volume_id: &str,
        op: &VolumeLeaseOp,
    ) -> MetaResult<Option<String>> {
        let mut tx = begin_tx(self.0, "begin volume lease update").await?;
        let Some(mut existing) =
            Self::lock_volume_for_update(&mut tx, volume_id, "lock volume for lease update")
                .await?
        else {
            return match op {
                VolumeLeaseOp::Acquire { .. } => Err(MetaError::VolumeNotFound {
                    lookup: volume_id.to_owned(),
                }),
                VolumeLeaseOp::Transfer { .. } | VolumeLeaseOp::Release { .. } => Ok(None),
            };
        };
        match op {
            VolumeLeaseOp::Acquire { owner } => {
                if existing.status != VolumeStatus::Ready {
                    return Err(MetaError::Unavailable {
                        reason: format!("volume '{}' is not usable", existing.id),
                    });
                }
                match existing.mode {
                    VolumeMode::Exclusive => {
                        if let Some(current) = existing.reserved_by_sandbox_id {
                            return Ok((current != *owner).then_some(current));
                        }
                        existing.reserved_by_sandbox_id = Some(owner.clone());
                        Self::persist_volume_owners(&mut tx, volume_id, &existing).await?;
                    }
                    VolumeMode::ReadOnly => {
                        if !existing.read_only_mounts.iter().any(|entry| entry == owner) {
                            existing.read_only_mounts.push(owner.clone());
                            Self::persist_volume_owners(&mut tx, volume_id, &existing).await?;
                        }
                    }
                }
            }
            VolumeLeaseOp::Transfer { from, to } => {
                if replace_volume_owner(&mut existing, from, Some(to)) {
                    Self::persist_volume_owners(&mut tx, volume_id, &existing).await?;
                }
            }
            VolumeLeaseOp::Release { owner } => {
                if replace_volume_owner(&mut existing, owner, None) {
                    Self::persist_volume_owners(&mut tx, volume_id, &existing).await?;
                }
            }
        }
        commit_tx(tx, "commit volume lease update").await?;
        Ok(None)
    }

    async fn persist_volume_owners(
        tx: &mut Transaction<'_, D>,
        volume_id: &str,
        record: &VolumeRecord,
    ) -> MetaResult<()> {
        let ro_mounts_json = serde_json::to_string(&record.read_only_mounts)
            .map_err(|error| MetaError::backend("encode read-only mounts", error))?;
        sqlx::query::<D>(
            "UPDATE volumes
             SET reserved_by = $2, ro_mounts_json = $3, updated_at = $4
             WHERE volume_id = $1",
        )
        .bind(volume_id)
        .bind(record.reserved_by_sandbox_id.as_deref())
        .bind(ro_mounts_json.as_str())
        .bind(now_unix_ms())
        .execute(&mut **tx)
        .await
        .map_err(|error| MetaError::backend("replace volume owner", error))?;
        Ok(())
    }
}
}

fn validate_volume_update(existing: &VolumeRecord, next: &VolumeRecord) -> Result<(), String> {
    if existing.name != next.name || existing.mode != next.mode || existing.size_mb != next.size_mb
    {
        return Err("volume identity fields cannot be changed".to_owned());
    }
    if existing.backing_layers != next.backing_layers && existing.status != VolumeStatus::Uploading
    {
        return Err(format!(
            "volume '{}' must enter uploading state before replacing its backing",
            existing.id
        ));
    }
    if existing.status == VolumeStatus::Failed && next.status == VolumeStatus::Ready {
        return Err(format!(
            "volume '{}' must enter uploading state before becoming ready",
            existing.id
        ));
    }
    Ok(())
}

fn replace_volume_owner(record: &mut VolumeRecord, from: &str, to: Option<&str>) -> bool {
    let mut changed = false;
    if record.reserved_by_sandbox_id.as_deref() == Some(from) {
        record.reserved_by_sandbox_id = to.map(str::to_owned);
        changed = true;
    }
    if record.read_only_mounts.iter().any(|owner| owner == from) {
        record.read_only_mounts.retain(|owner| owner != from);
        if let Some(to) = to {
            if !record.read_only_mounts.iter().any(|owner| owner == to) {
                record.read_only_mounts.push(to.to_owned());
            }
        }
        changed = true;
    }
    changed
}
