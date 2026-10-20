//! Typed ownership of a sandbox's transitional lifecycle state.
//!
//! Create, resume, pause, fork and snapshot each hold the sandbox in a
//! transitional state that blocks every other lifecycle operation until it is
//! resolved. A [`Claim`] is the only proof that the current task owns that
//! state, and it is resolved exactly once: `commit` moves the sandbox to the
//! operation's target state, `rollback` restores the stable state it came
//! from, and `teardown` removes a sandbox whose runtime can no longer be
//! trusted. Each of these consumes the claim. A claim dropped without being
//! resolved rolls back in the background, so an early return or panic cannot
//! strand the sandbox in its transitional state.

use std::marker::PhantomData;
use std::result::Result as StdResult;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use super::persistence::PersistenceResult;
use super::service::SandboxHandle;
use super::store::{MetadataUpdateResult, SandboxMetadata, StoreError};
use super::{OrchestratorError, Result, SandboxState};
use crate::types::SandboxId;

/// How a failed operation restores a stable state.
#[derive(Clone, Copy, Debug)]
pub(super) enum Rollback {
    /// Return the sandbox to the state it was claimed from.
    Restore(SandboxState),
    /// Return a resuming sandbox to `Paused`, including its persisted record.
    RestorePausedRecord,
    /// Remove the sandbox; it did not exist before the claim.
    Remove,
}

pub(super) trait ClaimKind: Send + 'static {
    const TRANSITIONAL: SandboxState;
    const COMMITTED: SandboxState;
    const ROLLBACK: Rollback;
}

/// A claim acquired by moving an existing sandbox out of a stable state.
pub(super) trait StableClaimKind: ClaimKind {
    const FROM: SandboxState;
}

/// A new sandbox registered as `Creating`.
pub(super) enum Create {}
/// A paused sandbox moved to `Resuming` before its persisted record is marked.
pub(super) enum PendingResume {}
/// A resuming sandbox whose persisted record is marked as resuming.
pub(super) enum Resume {}
pub(super) enum Pause {}
pub(super) enum Fork {}
pub(super) enum Snapshot {}

impl ClaimKind for Create {
    const TRANSITIONAL: SandboxState = SandboxState::Creating;
    const COMMITTED: SandboxState = SandboxState::Running;
    const ROLLBACK: Rollback = Rollback::Remove;
}

impl ClaimKind for PendingResume {
    const TRANSITIONAL: SandboxState = SandboxState::Resuming;
    const COMMITTED: SandboxState = SandboxState::Running;
    const ROLLBACK: Rollback = Rollback::Restore(Self::FROM);
}

impl StableClaimKind for PendingResume {
    const FROM: SandboxState = SandboxState::Paused;
}

impl ClaimKind for Resume {
    const TRANSITIONAL: SandboxState = SandboxState::Resuming;
    const COMMITTED: SandboxState = SandboxState::Running;
    const ROLLBACK: Rollback = Rollback::RestorePausedRecord;
}

impl ClaimKind for Pause {
    const TRANSITIONAL: SandboxState = SandboxState::Pausing;
    const COMMITTED: SandboxState = SandboxState::Paused;
    const ROLLBACK: Rollback = Rollback::Restore(Self::FROM);
}

impl StableClaimKind for Pause {
    const FROM: SandboxState = SandboxState::Running;
}

impl ClaimKind for Fork {
    const TRANSITIONAL: SandboxState = SandboxState::Forking;
    const COMMITTED: SandboxState = SandboxState::Running;
    const ROLLBACK: Rollback = Rollback::Restore(Self::FROM);
}

impl StableClaimKind for Fork {
    const FROM: SandboxState = SandboxState::Running;
}

impl ClaimKind for Snapshot {
    const TRANSITIONAL: SandboxState = SandboxState::Snapshotting;
    const COMMITTED: SandboxState = SandboxState::Running;
    const ROLLBACK: Rollback = Rollback::Restore(Self::FROM);
}

impl StableClaimKind for Snapshot {
    const FROM: SandboxState = SandboxState::Running;
}

pub(super) type MetadataUpdate = Box<dyn FnOnce(&mut SandboxMetadata) + Send + 'static>;

/// The orchestrator primitives a claim needs to acquire and resolve itself.
#[async_trait]
pub(super) trait ClaimHost: Send + Sync + 'static {
    async fn update_metadata_if_state(
        &self,
        sandbox_id: SandboxId,
        expected_state: SandboxState,
        update: MetadataUpdate,
    ) -> StdResult<MetadataUpdateResult, StoreError>;
    async fn add_metadata(&self, metadata: SandboxMetadata) -> StdResult<(), StoreError>;
    async fn remove_metadata(&self, sandbox_id: SandboxId) -> StdResult<(), StoreError>;
    async fn mark_resuming(&self, sandbox_id: SandboxId) -> PersistenceResult<()>;
    async fn rollback_resuming(&self, sandbox_id: SandboxId) -> PersistenceResult<()>;
    /// Detaches and stops the runtime, finalizes volumes, and removes the sandbox.
    async fn teardown(&self, sandbox_id: SandboxId, handle: Option<SandboxHandle>) -> Result<()>;
}

#[must_use = "a lifecycle claim must be committed, rolled back, or torn down"]
pub(super) struct Claim<K: ClaimKind> {
    host: Arc<dyn ClaimHost>,
    metadata: Box<SandboxMetadata>,
    resolved: bool,
    _kind: PhantomData<fn() -> K>,
}

impl<K: StableClaimKind> Claim<K> {
    /// Moves the sandbox from `K::FROM` into `K::TRANSITIONAL`.
    pub(super) async fn acquire(
        host: Arc<dyn ClaimHost>,
        sandbox_id: SandboxId,
    ) -> StdResult<Self, StoreError> {
        let update = host
            .update_metadata_if_state(
                sandbox_id,
                K::FROM,
                Box::new(|metadata| metadata.state = K::TRANSITIONAL),
            )
            .await?;
        Ok(Self::new(host, update.previous))
    }

    /// Like [`Claim::acquire`], but leaves the sandbox untouched and returns
    /// `None` unless `admit` accepts its current metadata.
    pub(super) async fn acquire_if(
        host: Arc<dyn ClaimHost>,
        sandbox_id: SandboxId,
        admit: impl FnOnce(&SandboxMetadata) -> bool + Send + 'static,
    ) -> StdResult<Option<Self>, StoreError> {
        let update = host
            .update_metadata_if_state(
                sandbox_id,
                K::FROM,
                Box::new(|metadata| {
                    if admit(metadata) {
                        metadata.state = K::TRANSITIONAL;
                    }
                }),
            )
            .await?;
        Ok((update.current.state == K::TRANSITIONAL).then(|| Self::new(host, update.previous)))
    }
}

impl Claim<Create> {
    /// Registers a new sandbox as `Creating`.
    pub(super) async fn acquire(
        host: Arc<dyn ClaimHost>,
        mut metadata: SandboxMetadata,
    ) -> StdResult<Self, StoreError> {
        metadata.state = Create::TRANSITIONAL;
        host.add_metadata(metadata.clone()).await?;
        Ok(Self::new(host, metadata))
    }
}

impl Claim<PendingResume> {
    /// Marks the persisted record as resuming. On failure the sandbox is
    /// returned to `Paused`.
    pub(super) async fn mark_resuming(mut self) -> Result<Claim<Resume>> {
        if let Err(err) = self.host.mark_resuming(self.sandbox_id()).await {
            warn!(error = ?err, "failed to mark persisted sandbox record as resuming");
            self.rollback().await;
            return Err(OrchestratorError::InternalError(format!(
                "failed to mark persisted sandbox record as resuming: {err:#}"
            )));
        }
        self.resolved = true;
        Ok(Claim::new(Arc::clone(&self.host), (*self.metadata).clone()))
    }
}

impl<K: ClaimKind> Claim<K> {
    fn new(host: Arc<dyn ClaimHost>, metadata: SandboxMetadata) -> Self {
        Self {
            host,
            metadata: Box::new(metadata),
            resolved: false,
            _kind: PhantomData,
        }
    }

    pub(super) fn sandbox_id(&self) -> SandboxId {
        self.metadata.id
    }

    /// The sandbox metadata as it was when the claim was acquired.
    pub(super) fn metadata(&self) -> &SandboxMetadata {
        &self.metadata
    }

    /// Moves the sandbox to `K::COMMITTED`, applying `update` atomically.
    ///
    /// On failure the still-unresolved claim is handed back so the caller can
    /// release whatever the operation acquired before resolving it.
    pub(super) async fn commit(
        mut self,
        update: impl FnOnce(&mut SandboxMetadata) + Send + 'static,
    ) -> StdResult<SandboxMetadata, (Self, StoreError)> {
        let result = self
            .host
            .update_metadata_if_state(
                self.sandbox_id(),
                K::TRANSITIONAL,
                Box::new(move |metadata| {
                    update(metadata);
                    metadata.state = K::COMMITTED;
                }),
            )
            .await;
        match result {
            Ok(update) => {
                self.resolved = true;
                Ok(update.current)
            }
            Err(err) => Err((self, err)),
        }
    }

    /// Restores the stable state the sandbox had before the claim.
    pub(super) async fn rollback(mut self) {
        self.resolved = true;
        rollback::<K>(self.host.as_ref(), self.sandbox_id()).await;
    }

    /// Removes a sandbox whose runtime failed terminally.
    pub(super) async fn teardown(mut self, handle: Option<SandboxHandle>) -> Result<()> {
        self.resolved = true;
        self.host.teardown(self.sandbox_id(), handle).await
    }
}

/// Sandboxes whose claims were dropped unresolved, so tests can tell an
/// explicit resolution from the background fallback.
#[cfg(test)]
static DROPPED_UNRESOLVED: std::sync::Mutex<Vec<SandboxId>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(super) fn was_dropped_unresolved(sandbox_id: SandboxId) -> bool {
    DROPPED_UNRESOLVED
        .lock()
        .expect("dropped claims mutex poisoned")
        .contains(&sandbox_id)
}

impl<K: ClaimKind> Drop for Claim<K> {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let sandbox_id = self.sandbox_id();
        #[cfg(test)]
        DROPPED_UNRESOLVED
            .lock()
            .expect("dropped claims mutex poisoned")
            .push(sandbox_id);
        warn!(
            %sandbox_id,
            state = ?K::TRANSITIONAL,
            "lifecycle claim dropped without being resolved; rolling back"
        );
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            warn!(%sandbox_id, "no runtime available to roll back dropped claim");
            return;
        };
        let host = Arc::clone(&self.host);
        runtime.spawn(async move { rollback::<K>(host.as_ref(), sandbox_id).await });
    }
}

async fn rollback<K: ClaimKind>(host: &dyn ClaimHost, sandbox_id: SandboxId) {
    let state = match K::ROLLBACK {
        Rollback::Restore(state) => state,
        Rollback::RestorePausedRecord => SandboxState::Paused,
        Rollback::Remove => {
            if let Err(err) = host.remove_metadata(sandbox_id).await {
                warn!(%sandbox_id, error = %err, "failed to remove sandbox metadata during rollback");
            }
            return;
        }
    };
    if let Err(err) = host
        .update_metadata_if_state(
            sandbox_id,
            K::TRANSITIONAL,
            Box::new(move |metadata| metadata.state = state),
        )
        .await
    {
        warn!(%sandbox_id, error = %err, ?state, "failed to restore sandbox state during rollback");
    }
    if matches!(K::ROLLBACK, Rollback::RestorePausedRecord) {
        if let Err(err) = host.rollback_resuming(sandbox_id).await {
            warn!(%sandbox_id, error = ?err, "failed to restore persisted sandbox record lifecycle during rollback");
        }
    }
}
