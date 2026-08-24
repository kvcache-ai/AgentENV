mod in_memory;
mod metadata;

use std::collections::HashMap;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::orchestrator::SandboxState;
use crate::types::SandboxId;

pub use in_memory::InMemoryMetadataStore;
pub use metadata::{NewTimeout, SandboxMetadata, SandboxTimeoutAction};

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug)]
pub struct MetadataUpdateResult {
    pub previous: SandboxMetadata,
    pub current: SandboxMetadata,
}

#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("store backend error: {source}")]
    Backend {
        #[source]
        source: anyhow::Error,
    },
    #[error("sandbox {sandbox_id} not found")]
    SandboxNotFound { sandbox_id: SandboxId },
    #[error("sandbox {sandbox_id} already exists")]
    SandboxAlreadyExists { sandbox_id: SandboxId },
    #[error(
        "sandbox {sandbox_id} state conflict: expected one of {expected_states:?}, got {actual_state:?}"
    )]
    StateConflict {
        sandbox_id: SandboxId,
        expected_states: Vec<SandboxState>,
        actual_state: SandboxState,
    },
}

/// The filter criteria for listing sandboxes in the metadata store.
///
/// The returned sandboxes must match `states` (if specified) and include `user_metadata` (if specified),
/// but must not match `excluded_states` (if specified).
#[derive(Clone, Debug, Default)]
pub struct SandboxListFilter {
    pub states: Option<Vec<SandboxState>>,
    pub excluded_states: Option<Vec<SandboxState>>,
    pub user_metadata: Option<HashMap<String, String>>,
    /// Include only sandboxes created at or after this instant.
    pub started_after: Option<SystemTime>,
    /// Include only sandboxes whose snapshot ID or alias matches this value.
    pub template: Option<String>,
}

impl SandboxListFilter {
    pub fn matches_all() -> Self {
        Self::default()
    }
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn add(&self, metadata: SandboxMetadata) -> Result<()>;
    async fn update(&self, metadata: SandboxMetadata) -> Result<()>;
    /// Updates the sandbox state to `new_state` if the current state is one of `expected_states`.
    /// Returns the PREVIOUS sandbox state on success.
    async fn update_state_if_state(
        &self,
        sandbox_id: &SandboxId,
        new_state: SandboxState,
        expected_states: &[SandboxState],
    ) -> Result<SandboxState>;
    /// Atomically updates sandbox metadata using the latest stored value if the
    /// current state is one of `expected_states`.
    ///
    /// The update callback is synchronous by design: store implementations may
    /// run it while holding their metadata lock, so callers must not perform
    /// async work inside the callback.
    async fn update_if_state<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[SandboxState],
        update: F,
    ) -> Result<MetadataUpdateResult>
    where
        F: FnOnce(&mut SandboxMetadata) + Send;
    async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>>;
    async fn remove(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>>;
    async fn list(&self) -> Result<Vec<SandboxMetadata>>;
    async fn list_with_callback<F>(&self, callback: F) -> Result<()>
    where
        F: FnMut(&SandboxMetadata) + Send;
    async fn list_filtered(&self, filter: SandboxListFilter) -> Result<Vec<SandboxMetadata>>;
    async fn list_expired(&self, now: SystemTime) -> Result<Vec<SandboxMetadata>>;
    async fn list_ids(&self) -> Result<Vec<SandboxId>>;

    /// Waits until the sandbox state is no longer any of the given `transitional_states`,
    /// then returns the latest metadata.
    ///
    /// Returns `Ok(None)` if the sandbox was removed while waiting.
    /// Returns immediately (without blocking) if the sandbox is already in a non-transitional state.
    async fn wait_while_in_states(
        &self,
        sandbox_id: &SandboxId,
        transitional_states: &[SandboxState],
    ) -> Result<Option<SandboxMetadata>>;
}
