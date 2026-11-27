//! Cross-resource GC pin coordination (`gc_pins`) over live snapshots, volumes, and images.

use agentenv_domain::{is_sha256_digest, is_valid_volume_component, ResourceKind, SnapshotId};
use sqlx::AssertSqlSafe;
use uuid::Uuid;

use crate::database::{begin_tx, commit_tx, dispatch_db, impl_sql_backend, Db, QueryRows};
use crate::{MetaError, MetaResult, MetaServer};

fn require_pin_id(value: Uuid) -> MetaResult<String> {
    if value.is_nil() {
        return Err(MetaError::InvalidRequest {
            reason: "pin ID requires a non-nil UUID".to_owned(),
        });
    }
    Ok(value.to_string())
}

fn validate_target_id(kind: ResourceKind, target_id: &str) -> MetaResult<String> {
    match kind {
        ResourceKind::Snapshot => SnapshotId::parse(target_id)
            .map(|id| id.to_string())
            .map_err(|error| MetaError::InvalidRequest {
                reason: error.to_string(),
            }),
        ResourceKind::Volume => {
            if !is_valid_volume_component(target_id) {
                return Err(MetaError::InvalidRequest {
                    reason: format!("invalid volume id '{target_id}'"),
                });
            }
            Ok(target_id.to_owned())
        }
        ResourceKind::Image => {
            if !is_sha256_digest(target_id) {
                return Err(MetaError::InvalidRequest {
                    reason: format!("invalid image digest '{target_id}'"),
                });
            }
            Ok(target_id.to_owned())
        }
    }
}

impl MetaServer {
    /// Pin any live resource (`snapshot`, `volume`, or `image`) against GC.
    pub async fn pin_resource(
        &self,
        kind: ResourceKind,
        target_id: &str,
        pin_id: Uuid,
        owner_node_id: Option<String>,
    ) -> MetaResult<bool> {
        let target_id = validate_target_id(kind, target_id)?;
        let pin_id = require_pin_id(pin_id)?;
        dispatch_db!(&self.database, |pool| {
            Db(pool)
                .pin_resource(kind, &target_id, &pin_id, owner_node_id.as_deref())
                .await
        })
    }

    /// Release a pin or record a tombstone so a late pin cannot reopen it.
    pub async fn release_pin(
        &self,
        kind: ResourceKind,
        target_id: &str,
        pin_id: Uuid,
    ) -> MetaResult<()> {
        let target_id = validate_target_id(kind, target_id)?;
        let pin_id = require_pin_id(pin_id)?;
        dispatch_db!(&self.database, |pool| {
            Db(pool).release_pin(kind, &target_id, &pin_id).await
        })
    }
}

impl_sql_backend! {
impl<D> Db<'_, D> {
    async fn pin_resource(
        &self,
        kind: ResourceKind,
        target_id: &str,
        pin_id: &str,
        owner_node_id: Option<&str>,
    ) -> MetaResult<bool> {
        let mut tx = begin_tx(self.0, "begin resource pin").await?;
        let (target_active, inactive_reason) = match kind {
            ResourceKind::Snapshot => {
                let state: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT state FROM snapshots WHERE snapshot_id = {}{}",
                    D::uuid(1),
                    D::FOR_SHARE,
                )))
                .bind(target_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("lock snapshot for pin", error))?;
                let Some(state) = state else {
                    return Ok(false);
                };
                if state == "reserved" || state == "reaped" {
                    return Err(MetaError::Unavailable {
                        reason: format!("snapshot '{target_id}' is {state}"),
                    });
                }
                (
                    state == "active",
                    format!("snapshot '{target_id}' is retired"),
                )
            }
            ResourceKind::Volume => {
                let status: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT status FROM volumes WHERE volume_id = $1{}",
                    D::FOR_SHARE,
                )))
                .bind(target_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("lock volume for pin", error))?;
                if status.is_none() {
                    return Ok(false);
                }
                (true, format!("volume '{target_id}' is unavailable"))
            }
            ResourceKind::Image => {
                let retired: Option<bool> = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT retired FROM images WHERE digest = $1{}",
                    D::FOR_SHARE,
                )))
                .bind(target_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("lock image for pin", error))?;
                let Some(retired) = retired else {
                    return Ok(false);
                };
                (!retired, format!("image '{target_id}' is retired"))
            }
        };
        let kind_str = kind.as_str();
        if target_active {
            sqlx::query::<D>(AssertSqlSafe(format!(
                "INSERT INTO gc_pins (pin_id, target_kind, target_id, owner_node_id, released)
                 VALUES ({}, $2, $3, $4, FALSE)
                 ON CONFLICT (pin_id) DO NOTHING",
                D::uuid(1),
            )))
            .bind(pin_id)
            .bind(kind_str)
            .bind(target_id)
            .bind(owner_node_id)
            .execute(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("insert resource pin", error))?;
        }
        let stored_pin = sqlx::query_as::<D, (String, String, bool)>(AssertSqlSafe(format!(
            "SELECT target_kind, target_id, released FROM gc_pins WHERE pin_id = {}{}",
            D::uuid(1),
            D::FOR_UPDATE,
        )))
        .bind(pin_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("read resource pin", error))?;
        let Some((bound_kind, bound_target, released)) = stored_pin else {
            return Err(MetaError::Unavailable {
                reason: inactive_reason,
            });
        };
        if bound_kind != kind_str || bound_target != target_id {
            return Err(MetaError::InvalidRequest {
                reason: format!("pin '{pin_id}' is already bound to {bound_kind} '{bound_target}'"),
            });
        }
        if released {
            return Err(MetaError::Unavailable {
                reason: format!("pin '{pin_id}' has already been released"),
            });
        }
        commit_tx(tx, "commit resource pin").await?;
        Ok(true)
    }

    async fn release_pin(
        &self,
        kind: ResourceKind,
        target_id: &str,
        pin_id: &str,
    ) -> MetaResult<()> {
        let updated = sqlx::query::<D>(AssertSqlSafe(format!(
            "INSERT INTO gc_pins (pin_id, target_kind, target_id, released)
             VALUES ({}, $2, $3, TRUE)
             ON CONFLICT (pin_id) DO UPDATE SET released = TRUE
             WHERE gc_pins.target_kind = EXCLUDED.target_kind
               AND gc_pins.target_id = EXCLUDED.target_id",
            D::uuid(1),
        )))
        .bind(pin_id)
        .bind(kind.as_str())
        .bind(target_id)
        .execute(self.0)
        .await
        .map_err(|error| MetaError::backend("release resource pin", error))?;
        if updated.rows_affected() == 0 {
            let (bound_kind, bound_target): (String, String) =
                sqlx::query_as(AssertSqlSafe(format!(
                    "SELECT target_kind, target_id FROM gc_pins WHERE pin_id = {}",
                    D::uuid(1),
                )))
                .bind(pin_id)
                .fetch_one(self.0)
                .await
                .map_err(|error| MetaError::backend("read conflicting resource pin", error))?;
            return Err(MetaError::InvalidRequest {
                reason: format!("pin '{pin_id}' is already bound to {bound_kind} '{bound_target}'"),
            });
        }
        Ok(())
    }
}
}
