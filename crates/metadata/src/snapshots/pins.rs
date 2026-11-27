//! Snapshot GC pin coordination (`snapshot_pins`) protecting live and retired snapshots.

use agentenv_domain::SnapshotId;
use uuid::Uuid;

use crate::db::{MetaSqlExt, Sql};
use crate::{MetaError, MetaResult, MetaServer};

fn require_pin(pin_id: Uuid) -> MetaResult<()> {
    if pin_id.is_nil() {
        return Err(MetaError::invalid("pin ID requires a non-nil UUID"));
    }
    Ok(())
}

impl MetaServer {
    /// Pin a live snapshot against GC. Returns `Ok(false)` if the snapshot does not exist yet.
    pub async fn pin_snapshot(
        &self,
        id: &SnapshotId,
        pin_id: Uuid,
        owner_node_id: Option<String>,
    ) -> MetaResult<bool> {
        require_pin(pin_id)?;
        let mut tx = self.database.begin("begin snapshot pin").await?;
        let Some(row) = tx
            .fetch_optional(
                Sql::new("SELECT state FROM snapshots WHERE snapshot_id = $1")
                    .for_share()
                    .bind(id),
            )
            .await
            .meta_err("lock snapshot for pin")?
        else {
            return Ok(false);
        };
        let state: String = row.get(0)?;
        if state == "reserved" || state == "reaped" {
            return Err(MetaError::unavailable(format!(
                "snapshot '{id}' is {state}"
            )));
        }
        if state == "active" {
            tx.execute(
                Sql::new(
                    "INSERT INTO snapshot_pins (pin_id, snapshot_id, owner_node_id, released)
                     VALUES ($1, $2, $3, FALSE) ON CONFLICT (pin_id) DO NOTHING",
                )
                .bind(pin_id)
                .bind(id)
                .bind(owner_node_id),
            )
            .await
            .meta_err("insert snapshot pin")?;
        }
        let Some(pin_row) = tx
            .fetch_optional(
                Sql::new("SELECT snapshot_id, released FROM snapshot_pins WHERE pin_id = $1")
                    .for_update()
                    .bind(pin_id),
            )
            .await
            .meta_err("read snapshot pin")?
        else {
            return Err(MetaError::unavailable(format!(
                "snapshot '{id}' is retired"
            )));
        };
        let bound = pin_row.snapshot_id(0)?;
        if &bound != id {
            return Err(MetaError::invalid(format!(
                "pin '{pin_id}' is already bound to snapshot '{bound}'"
            )));
        }
        if pin_row.get::<bool>(1)? {
            return Err(MetaError::unavailable(format!(
                "pin '{pin_id}' has already been released"
            )));
        }
        tx.commit("commit snapshot pin").await?;
        Ok(true)
    }

    /// Release a pin or record a tombstone so a late pin cannot reopen it.
    pub async fn release_snapshot_pin(&self, id: &SnapshotId, pin_id: Uuid) -> MetaResult<()> {
        require_pin(pin_id)?;
        let updated = self
            .database
            .execute(
                Sql::new(
                    "INSERT INTO snapshot_pins (pin_id, snapshot_id, released)
                     VALUES ($1, $2, TRUE)
                     ON CONFLICT (pin_id) DO UPDATE SET released = TRUE
                     WHERE snapshot_pins.snapshot_id = EXCLUDED.snapshot_id",
                )
                .bind(pin_id)
                .bind(id),
            )
            .await
            .meta_err("release snapshot pin")?;
        if updated == 0 {
            let bound = self
                .database
                .fetch_one(
                    Sql::new("SELECT snapshot_id FROM snapshot_pins WHERE pin_id = $1")
                        .bind(pin_id),
                )
                .await
                .meta_err("read conflicting snapshot pin")?
                .snapshot_id(0)?;
            return Err(MetaError::invalid(format!(
                "pin '{pin_id}' is already bound to snapshot '{bound}'"
            )));
        }
        Ok(())
    }
}
