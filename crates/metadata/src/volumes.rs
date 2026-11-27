//! Persistent volume records, leases, and BuildKit cache-seed coordination.

use agentenv_domain::{
    is_valid_volume_component, LayerRef, ResourceKind, VolumeLeaseOp, VolumeMode, VolumePurpose,
    VolumeRecord, VolumeStatus,
};

use crate::db::{is_unique_violation, DbRow, DbTx, MetaSqlExt, Sql};
use crate::layers::{
    attach_layers, collect_disk_relations, now_unix_ms, release_owner_heads, sync_owner_heads,
};
use crate::{MetaError, MetaResult, MetaServer};

const COLS: &str = "volume_id, name, purpose, mode, size_mb, status, backing_layers_json, reserved_by, ro_mounts_json";

fn decode_volume(row: DbRow) -> MetaResult<VolumeRecord> {
    Ok(VolumeRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        purpose: VolumePurpose::parse(&row.get::<String>(2)?)
            .map_err(|e| MetaError::backend("decode volume purpose", e))?,
        mode: VolumeMode::parse(&row.get::<String>(3)?)
            .map_err(|e| MetaError::backend("decode volume mode", e))?,
        size_mb: u64::try_from(row.get::<i64>(4)?)
            .map_err(|e| MetaError::backend("decode volume size", e))?,
        status: VolumeStatus::parse(&row.get::<String>(5)?)
            .map_err(|e| MetaError::backend("decode volume status", e))?,
        backing_layers: serde_json::from_str::<Vec<LayerRef>>(&row.get::<String>(6)?)
            .map_err(|e| MetaError::backend("decode volume backing layers", e))?,
        reserved_by_sandbox_id: row.get(7)?,
        read_only_mounts: serde_json::from_str::<Vec<String>>(&row.get::<String>(8)?)
            .map_err(|e| MetaError::backend("decode volume read-only mounts", e))?,
    })
}

fn require_component(value: &str, kind: &str) -> MetaResult<()> {
    if !is_valid_volume_component(value) {
        return Err(MetaError::invalid(format!(
            "invalid volume {kind} '{value}'"
        )));
    }
    Ok(())
}

fn conflict_query(id: &str, name: &str) -> Sql {
    // IDs and names share one lookup namespace.
    Sql::new(
        "SELECT volume_id, name FROM volumes
         WHERE volume_id = $1 OR name = $1 OR volume_id = $2 OR name = $2",
    )
    .bind(id)
    .bind(name)
}

fn check_conflicts(rows: Vec<DbRow>, id: &str, name: &str) -> MetaResult<()> {
    for row in &rows {
        if row.get::<String>(0)? == id {
            return Err(MetaError::invalid(format!("volume '{id}' already exists")));
        }
    }
    if !rows.is_empty() {
        return Err(MetaError::VolumeNameConflict {
            name: name.to_owned(),
        });
    }
    Ok(())
}

async fn lock_volume(
    tx: &mut DbTx<'_>,
    volume_id: &str,
    context: &str,
) -> MetaResult<Option<VolumeRecord>> {
    tx.fetch_optional(
        Sql::new(format!("SELECT {COLS} FROM volumes WHERE volume_id = $1"))
            .for_update()
            .bind(volume_id),
    )
    .await
    .meta_err(context)?
    .map(decode_volume)
    .transpose()
}

impl MetaServer {
    pub async fn get_volume(&self, reference: &str) -> MetaResult<Option<VolumeRecord>> {
        require_component(reference, "reference")?;
        self.database
            .fetch_optional(
                Sql::new(format!(
                    "SELECT {COLS} FROM volumes
                     WHERE volume_id = $1 OR name = $1
                     ORDER BY CASE WHEN volume_id = $1 THEN 0 ELSE 1 END
                     LIMIT 1"
                ))
                .bind(reference),
            )
            .await
            .meta_err("get volume")?
            .map(decode_volume)
            .transpose()
    }

    pub async fn list_volumes(
        &self,
        purposes: &[VolumePurpose],
        after_volume_id: Option<&str>,
        limit: u32,
    ) -> MetaResult<(Vec<VolumeRecord>, Option<String>)> {
        if limit == 0 {
            return Err(MetaError::invalid(
                "volume page limit must be greater than zero",
            ));
        }
        if let Some(after) = after_volume_id {
            require_component(after, "id")?;
        }
        let mut q = Sql::new(format!("SELECT {COLS} FROM volumes WHERE 1 = 1"));
        if !purposes.is_empty() {
            q.push(" AND purpose IN (")
                .push_in(purposes.iter().map(|p| p.as_str()))
                .push(")");
        }
        if let Some(after) = after_volume_id {
            q.push(" AND volume_id > ").push_bind(after);
        }
        q.push(" ORDER BY volume_id ASC LIMIT ")
            .push_bind(i64::from(limit).saturating_add(1));
        let mut rows = self.database.fetch_all(q).await.meta_err("list volumes")?;
        let has_more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let records = rows
            .into_iter()
            .map(decode_volume)
            .collect::<MetaResult<Vec<_>>>()?;
        let next_volume_id = has_more
            .then(|| records.last().map(|r| r.id.clone()))
            .flatten();
        Ok((records, next_volume_id))
    }

    pub async fn create_volume(
        &self,
        mut record: VolumeRecord,
        from_volume_id: Option<&str>,
        restored_layers: &[String],
    ) -> MetaResult<VolumeRecord> {
        require_component(&record.id, "id")?;
        require_component(&record.name, "name")?;
        if record.size_mb == 0 {
            return Err(MetaError::invalid("volume size must be greater than zero"));
        }
        let size_mb = i64::try_from(record.size_mb)
            .map_err(|_| MetaError::invalid("volume size exceeds supported range"))?;
        if record.mode == VolumeMode::ReadOnly && record.reserved_by_sandbox_id.is_some() {
            return Err(MetaError::invalid(
                "read-only volumes cannot carry an exclusive reservation",
            ));
        }
        if record.purpose == VolumePurpose::BuildCacheSeed {
            if record.mode != VolumeMode::ReadOnly {
                return Err(MetaError::invalid(
                    "build cache seeds must use read-only mode",
                ));
            }
            if record.status != VolumeStatus::Ready {
                return Err(MetaError::unavailable(format!(
                    "build cache seed '{}' is not ready",
                    record.id
                )));
            }
        }
        if let Some(owner) = record.reserved_by_sandbox_id.as_deref() {
            require_component(owner, "owner")?;
        }
        for owner in &record.read_only_mounts {
            require_component(owner, "owner")?;
        }
        if let Some(source_id) = from_volume_id {
            require_component(source_id, "id")?;
        }
        let ro_mounts_json = serde_json::to_string(&record.read_only_mounts)
            .map_err(|e| MetaError::backend("encode read-only mounts", e))?;
        loop {
            let mut tx = self.database.begin("begin volume creation").await?;
            let mut backing_layers = record.backing_layers.clone();
            if let Some(source_id) = from_volume_id {
                let Some(row) = tx
                    .fetch_optional(
                        Sql::new(
                            "SELECT status, backing_layers_json FROM volumes WHERE volume_id = $1",
                        )
                        .for_share()
                        .bind(source_id),
                    )
                    .await
                    .meta_err("lock source volume for clone")?
                else {
                    return Err(MetaError::VolumeNotFound {
                        lookup: source_id.to_owned(),
                    });
                };
                if row.get::<String>(0)? != VolumeStatus::Ready.as_str() {
                    return Err(MetaError::unavailable(format!(
                        "source volume '{source_id}' is not ready"
                    )));
                }
                if backing_layers.is_empty() {
                    backing_layers = serde_json::from_str(&row.get::<String>(1)?)
                        .map_err(|e| MetaError::backend("decode source volume layers", e))?;
                }
            }

            let conflicts = tx
                .fetch_all(conflict_query(&record.id, &record.name))
                .await
                .meta_err("check volume conflicts")?;
            check_conflicts(conflicts, &record.id, &record.name)?;

            let now = now_unix_ms();
            if record.purpose == VolumePurpose::BuildCacheSeed {
                // Atomically retire any prior active seed before inserting its successor.
                tx.execute(
                    Sql::new(
                        "UPDATE volumes
                         SET purpose = 'build_cache_retired', updated_at = $1
                         WHERE purpose = 'build_cache_seed'",
                    )
                    .bind(now),
                )
                .await
                .meta_err("retire previous build cache seed")?;
            }

            let backing_layers_json = serde_json::to_string(&backing_layers)
                .map_err(|e| MetaError::backend("encode volume layers", e))?;
            let relations = collect_disk_relations(&backing_layers)?;
            let inserted = tx
                .execute(
                    Sql::new(
                        "INSERT INTO volumes (volume_id, name, purpose, mode, size_mb, status, backing_layers_json, reserved_by, ro_mounts_json, created_at, updated_at)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)",
                    )
                    .bind(&record.id)
                    .bind(&record.name)
                    .bind(record.purpose.as_str())
                    .bind(record.mode.as_str())
                    .bind(size_mb)
                    .bind(record.status.as_str())
                    .bind(&backing_layers_json)
                    .bind(record.reserved_by_sandbox_id.as_deref())
                    .bind(&ro_mounts_json)
                    .bind(now),
                )
                .await;
            if let Err(error) = inserted {
                let _ = tx.rollback("abort volume creation").await;
                if is_unique_violation(&error) {
                    let conflicts = self
                        .database
                        .fetch_all(conflict_query(&record.id, &record.name))
                        .await
                        .meta_err("read conflicting volume")?;
                    check_conflicts(conflicts, &record.id, &record.name)?;
                    if record.purpose == VolumePurpose::BuildCacheSeed {
                        // Another writer concurrently promoted a build cache seed; retry to supersede it.
                        continue;
                    }
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
                tx.rollback("restart volume layer checks").await?;
                continue;
            }
            tx.commit("commit volume creation").await?;
            record.backing_layers = backing_layers;
            return Ok(record);
        }
    }

    pub async fn update_volume(
        &self,
        record: VolumeRecord,
        restored_layers: &[String],
    ) -> MetaResult<VolumeRecord> {
        require_component(&record.id, "id")?;
        let backing_layers_json = serde_json::to_string(&record.backing_layers)
            .map_err(|e| MetaError::backend("encode volume layers", e))?;
        let relations = collect_disk_relations(&record.backing_layers)?;
        loop {
            let mut tx = self.database.begin("begin volume update").await?;
            let Some(existing) = lock_volume(&mut tx, &record.id, "lock volume for update").await?
            else {
                return Err(MetaError::VolumeNotFound {
                    lookup: record.id.clone(),
                });
            };
            if existing.name != record.name
                || existing.mode != record.mode
                || existing.size_mb != record.size_mb
            {
                return Err(MetaError::invalid(
                    "volume identity fields cannot be changed",
                ));
            }
            let layers_changed = existing.backing_layers != record.backing_layers;
            if layers_changed && existing.status != VolumeStatus::Uploading {
                return Err(MetaError::invalid(format!(
                    "volume '{}' must enter uploading state before replacing its backing",
                    existing.id
                )));
            }
            if existing.status == VolumeStatus::Failed && record.status == VolumeStatus::Ready {
                return Err(MetaError::invalid(format!(
                    "volume '{}' must enter uploading state before becoming ready",
                    existing.id
                )));
            }
            let mut next = record.clone();
            next.purpose = existing.purpose;
            next.reserved_by_sandbox_id = existing.reserved_by_sandbox_id;
            next.read_only_mounts = existing.read_only_mounts;

            tx.execute(
                Sql::new(
                    "UPDATE volumes
                     SET status = $2, backing_layers_json = $3, updated_at = $4
                     WHERE volume_id = $1",
                )
                .bind(&next.id)
                .bind(next.status.as_str())
                .bind(&backing_layers_json)
                .bind(now_unix_ms()),
            )
            .await
            .meta_err("update volume")?;

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
                tx.rollback("restart volume update layer checks").await?;
                continue;
            }
            tx.commit("commit volume update").await?;
            return Ok(next);
        }
    }

    pub async fn delete_volume(&self, volume_id: &str) -> MetaResult<()> {
        require_component(volume_id, "id")?;
        let mut tx = self.database.begin("begin volume deletion").await?;
        let Some(existing) = lock_volume(&mut tx, volume_id, "lock volume for deletion").await?
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
            return Err(MetaError::unavailable(format!(
                "volume '{}' is the active build cache seed",
                existing.id
            )));
        }
        tx.execute(Sql::new("DELETE FROM volumes WHERE volume_id = $1").bind(volume_id))
            .await
            .meta_err("delete volume row")?;
        release_owner_heads(&mut tx, ResourceKind::Volume, volume_id).await?;
        tx.commit("commit volume deletion").await
    }

    pub async fn update_volume_lease(
        &self,
        volume_id: &str,
        op: VolumeLeaseOp,
    ) -> MetaResult<Option<String>> {
        require_component(volume_id, "id")?;
        match &op {
            VolumeLeaseOp::Acquire { owner } | VolumeLeaseOp::Release { owner } => {
                require_component(owner, "owner")?;
            }
            VolumeLeaseOp::Transfer { from, to } => {
                require_component(from, "owner")?;
                require_component(to, "owner")?;
                if from == to {
                    return Ok(None);
                }
            }
        }
        let mut tx = self.database.begin("begin volume lease update").await?;
        let Some(mut existing) =
            lock_volume(&mut tx, volume_id, "lock volume for lease update").await?
        else {
            return match op {
                VolumeLeaseOp::Acquire { .. } => Err(MetaError::VolumeNotFound {
                    lookup: volume_id.to_owned(),
                }),
                VolumeLeaseOp::Transfer { .. } | VolumeLeaseOp::Release { .. } => Ok(None),
            };
        };
        let mut changed = false;
        match &op {
            VolumeLeaseOp::Acquire { owner } => {
                if existing.status != VolumeStatus::Ready {
                    return Err(MetaError::unavailable(format!(
                        "volume '{}' is not usable",
                        existing.id
                    )));
                }
                match existing.mode {
                    VolumeMode::Exclusive => {
                        if let Some(current) = existing.reserved_by_sandbox_id {
                            return Ok((current != *owner).then_some(current));
                        }
                        existing.reserved_by_sandbox_id = Some(owner.clone());
                        changed = true;
                    }
                    VolumeMode::ReadOnly => {
                        if !existing.read_only_mounts.contains(owner) {
                            existing.read_only_mounts.push(owner.clone());
                            changed = true;
                        }
                    }
                }
            }
            VolumeLeaseOp::Transfer { from, to } => {
                if existing.reserved_by_sandbox_id.as_deref() == Some(from.as_str()) {
                    existing.reserved_by_sandbox_id = Some(to.clone());
                    changed = true;
                }
                let before = existing.read_only_mounts.len();
                existing.read_only_mounts.retain(|entry| entry != from);
                if existing.read_only_mounts.len() != before {
                    if !existing.read_only_mounts.contains(to) {
                        existing.read_only_mounts.push(to.clone());
                    }
                    changed = true;
                }
            }
            VolumeLeaseOp::Release { owner } => {
                if existing.reserved_by_sandbox_id.as_deref() == Some(owner.as_str()) {
                    existing.reserved_by_sandbox_id = None;
                    changed = true;
                }
                let before = existing.read_only_mounts.len();
                existing.read_only_mounts.retain(|entry| entry != owner);
                changed |= existing.read_only_mounts.len() != before;
            }
        }
        if changed {
            let ro_mounts_json = serde_json::to_string(&existing.read_only_mounts)
                .map_err(|e| MetaError::backend("encode read-only mounts", e))?;
            tx.execute(
                Sql::new(
                    "UPDATE volumes
                     SET reserved_by = $2, ro_mounts_json = $3, updated_at = $4
                     WHERE volume_id = $1",
                )
                .bind(volume_id)
                .bind(existing.reserved_by_sandbox_id.as_deref())
                .bind(&ro_mounts_json)
                .bind(now_unix_ms()),
            )
            .await
            .meta_err("replace volume owner")?;
        }
        tx.commit("commit volume lease update").await?;
        Ok(None)
    }
}
