use std::collections::{BTreeSet, HashMap};
use std::time::SystemTime;

use async_trait::async_trait;
use tokio::sync::{watch, RwLock};

use crate::orchestrator::SandboxState;

use super::{
    MetadataStore, MetadataUpdateResult, Result, SandboxId, SandboxListFilter, SandboxMetadata,
    StoreError,
};

/// In-memory metadata store backed by a `RwLock<HashMap>`.
///
/// Each sandbox has an associated `watch` channel that broadcasts its current
/// `Option<SandboxState>` (where `None` means the sandbox has been removed).
/// This enables efficient, lock-free waiting for state transitions via
/// `wait_while_in_states`.
pub struct InMemoryMetadataStore {
    inner: RwLock<StoreInner>,
}

#[derive(Clone)]
struct SandboxRecord {
    metadata: SandboxMetadata,
    state_tx: watch::Sender<Option<SandboxState>>,
}

#[derive(Default)]
struct StoreInner {
    records: HashMap<SandboxId, SandboxRecord>,
    by_expiry: BTreeSet<(SystemTime, SandboxId)>,
}

impl Default for InMemoryMetadataStore {
    fn default() -> Self {
        Self {
            inner: RwLock::new(StoreInner::default()),
        }
    }
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sends `new_state` through the watch channel for `sandbox_id`, waking any
    /// tasks blocked in `wait_while_in_states`. Silently ignores missing entries
    /// (e.g. called after the entry was already removed).
    ///
    /// **Important**: uses `send_replace` rather than `send` so that the channel
    /// value is updated unconditionally, even when there are currently no active
    /// receivers. If `send` were used and a notification were fired with zero
    /// receivers, the channel value would remain stale; a receiver that subscribed
    /// afterwards would see the old (already-stable) state, skip the `wait_for`
    /// predicate check, and immediately read a transitional state from `inner`.
    fn notify_state(tx: &watch::Sender<Option<SandboxState>>, new_state: Option<SandboxState>) {
        tx.send_replace(new_state);
    }

    fn user_metadata_matches(
        metadata: &SandboxMetadata,
        required_pairs: Option<&HashMap<String, String>>,
    ) -> bool {
        required_pairs.is_none_or(|required_pairs| {
            metadata
                .user_metadata
                .as_ref()
                .is_some_and(|actual_metadata| {
                    required_pairs
                        .iter()
                        .all(|(k, v)| actual_metadata.get(k) == Some(v))
                })
        })
    }
}

impl StoreInner {
    fn index_expiry(&mut self, sandbox_id: &SandboxId, expires_at: Option<SystemTime>) {
        if let Some(expires_at) = expires_at {
            self.by_expiry.insert((expires_at, *sandbox_id));
        }
    }

    fn deindex_expiry(&mut self, sandbox_id: &SandboxId, expires_at: Option<SystemTime>) {
        if let Some(expires_at) = expires_at {
            self.by_expiry.remove(&(expires_at, *sandbox_id));
        }
    }
}

#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
    async fn add(&self, metadata: SandboxMetadata) -> Result<()> {
        let mut inner = self.inner.write().await;
        if inner.records.contains_key(&metadata.id) {
            return Err(StoreError::SandboxAlreadyExists {
                sandbox_id: metadata.id,
            });
        }

        let (tx, _) = watch::channel(Some(metadata.state));
        inner.index_expiry(&metadata.id, metadata.expires_at);
        inner.records.insert(
            metadata.id,
            SandboxRecord {
                metadata,
                state_tx: tx,
            },
        );
        Ok(())
    }

    async fn update(&self, metadata: SandboxMetadata) -> Result<()> {
        let new_state = metadata.state;
        let sandbox_id = metadata.id;
        let new_expires_at = metadata.expires_at;
        let tx = {
            let mut inner = self.inner.write().await;
            let (previous, tx) = {
                let record = inner
                    .records
                    .get_mut(&sandbox_id)
                    .ok_or_else(|| StoreError::SandboxNotFound { sandbox_id })?;
                let previous = std::mem::replace(&mut record.metadata, metadata);
                (previous, record.state_tx.clone())
            };
            inner.deindex_expiry(&sandbox_id, previous.expires_at);
            inner.index_expiry(&sandbox_id, new_expires_at);
            tx
        };
        // Notify *after* releasing the write lock so watchers that immediately
        // attempt a read won't be blocked by our own write lock.
        Self::notify_state(&tx, Some(new_state));
        Ok(())
    }

    async fn update_state_if_state(
        &self,
        sandbox_id: &SandboxId,
        new_state: SandboxState,
        expected_states: &[SandboxState],
    ) -> Result<SandboxState> {
        let (previous, tx) = {
            let mut inner = self.inner.write().await;
            let Some(record) = inner.records.get_mut(sandbox_id) else {
                return Err(StoreError::SandboxNotFound {
                    sandbox_id: *sandbox_id,
                });
            };

            let previous_state = record.metadata.state;
            if !expected_states.contains(&previous_state) {
                return Err(StoreError::StateConflict {
                    sandbox_id: *sandbox_id,
                    expected_states: expected_states.to_vec(),
                    actual_state: previous_state,
                });
            }

            record.metadata.state = new_state;
            let tx = record.state_tx.clone();
            (previous_state, tx)
        };

        Self::notify_state(&tx, Some(new_state));
        Ok(previous)
    }

    async fn update_if_state<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[SandboxState],
        update: F,
    ) -> Result<MetadataUpdateResult>
    where
        F: FnOnce(&mut SandboxMetadata) + Send,
    {
        let (result, notification) = {
            let mut inner = self.inner.write().await;
            let (previous, current, tx) = {
                let record = inner.records.get_mut(sandbox_id).ok_or_else(|| {
                    StoreError::SandboxNotFound {
                        sandbox_id: *sandbox_id,
                    }
                })?;
                let actual_state = record.metadata.state;
                if !expected_states.contains(&actual_state) {
                    return Err(StoreError::StateConflict {
                        sandbox_id: *sandbox_id,
                        expected_states: expected_states.to_vec(),
                        actual_state,
                    });
                }

                let previous = record.metadata.clone();
                update(&mut record.metadata);
                let current = record.metadata.clone();
                (previous, current, record.state_tx.clone())
            };

            if previous.expires_at != current.expires_at {
                inner.deindex_expiry(sandbox_id, previous.expires_at);
                inner.index_expiry(sandbox_id, current.expires_at);
            }

            let notification = (previous.state != current.state).then_some((tx, current.state));
            (MetadataUpdateResult { previous, current }, notification)
        };

        if let Some((tx, new_state)) = notification {
            Self::notify_state(&tx, Some(new_state));
        }

        Ok(result)
    }

    async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        Ok(self
            .inner
            .read()
            .await
            .records
            .get(sandbox_id)
            .map(|record| record.metadata.clone()))
    }

    async fn remove(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        let removed = {
            let mut inner = self.inner.write().await;
            let removed = inner.records.remove(sandbox_id);
            if let Some(record) = &removed {
                inner.deindex_expiry(&record.metadata.id, record.metadata.expires_at);
            }
            removed
        };

        if let Some(record) = &removed {
            // Notify waiters that this sandbox is gone, then drop sender.
            Self::notify_state(&record.state_tx, None);
        }

        Ok(removed.map(|record| record.metadata))
    }

    async fn list_ids(&self) -> Result<Vec<SandboxId>> {
        Ok(self.inner.read().await.records.keys().cloned().collect())
    }

    async fn list(&self) -> Result<Vec<SandboxMetadata>> {
        Ok(self
            .inner
            .read()
            .await
            .records
            .values()
            .map(|record| record.metadata.clone())
            .collect())
    }

    async fn list_with_callback<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(&SandboxMetadata) + Send,
    {
        for record in self.inner.read().await.records.values() {
            callback(&record.metadata);
        }
        Ok(())
    }

    async fn list_filtered(&self, filter: SandboxListFilter) -> Result<Vec<SandboxMetadata>> {
        let states_filter = filter.states;
        let excluded_states_filter = filter.excluded_states;
        let user_metadata_filter = filter.user_metadata;
        let started_after_filter = filter.started_after;
        let template_filter = filter.template;
        let inner = self.inner.read().await;

        let matched = inner
            .records
            .values()
            .filter(|record| {
                let metadata = &record.metadata;
                let state_matches = states_filter
                    .as_ref()
                    .is_none_or(|states| states.contains(&metadata.state));
                let excluded_state_matches = excluded_states_filter
                    .as_ref()
                    .is_some_and(|states| states.contains(&metadata.state));
                let user_metadata_matches =
                    Self::user_metadata_matches(metadata, user_metadata_filter.as_ref());
                let started_after_matches = started_after_filter
                    .is_none_or(|started_after| metadata.created_at >= started_after);
                let template_matches = template_filter.as_ref().is_none_or(|template| {
                    metadata.snapshot_id == *template
                        || metadata
                            .snapshot_alias
                            .as_deref()
                            .is_some_and(|alias| alias == template)
                });
                state_matches
                    && !excluded_state_matches
                    && user_metadata_matches
                    && started_after_matches
                    && template_matches
            })
            .map(|record| record.metadata.clone())
            .collect();

        Ok(matched)
    }

    async fn list_expired(&self, now: SystemTime) -> Result<Vec<SandboxMetadata>> {
        let inner = self.inner.read().await;
        let expired = inner
            .by_expiry
            .range(..=(now, SandboxId::max()))
            .filter_map(|(_, sandbox_id)| inner.records.get(sandbox_id))
            .map(|record| record.metadata.clone())
            .collect();
        Ok(expired)
    }

    /// Waits asynchronously until the sandbox's state is no longer one of the
    /// provided `transitional_states`, then returns the latest metadata snapshot.
    ///
    /// ## Concurrency contract
    /// - The metadata and watch sender are stored in the same map entry,
    ///   guarded by one `RwLock`, so there is no cross-lock ordering risk.
    /// - Notifications are sent *after* `inner` is updated and its write lock is
    ///   released, so a woken task reading from `inner` will always see the state
    ///   that triggered the notification (or a newer one).
    /// - If the sandbox was already in a non-transitional state when the call is
    ///   made, `watch::Receiver::wait_for` returns immediately without blocking.
    async fn wait_while_in_states(
        &self,
        sandbox_id: &SandboxId,
        transitional_states: &[SandboxState],
    ) -> Result<Option<SandboxMetadata>> {
        // Subscribe to the watch channel while holding the read lock briefly.
        let mut rx = {
            let inner = self.inner.read().await;
            match inner.records.get(sandbox_id) {
                Some(record) => record.state_tx.subscribe(),
                // No channel → sandbox was already removed (or never existed).
                None => return Ok(None),
            }
        };

        // `wait_for` checks the current value first; if it already satisfies
        // the predicate (i.e. the state is NOT in `transitional_states`), it
        // returns immediately without suspending the task.
        let is_deleted = match rx
            .wait_for(|state| {
                // Predicate satisfied when: deleted (None) OR state is stable.
                state
                    .as_ref()
                    .is_none_or(|s| !transitional_states.contains(s))
            })
            .await
        {
            // Sender was dropped without sending None (shouldn't happen in
            // practice, but treat it the same as deletion).
            Err(_) => true,
            Ok(state_ref) => state_ref.is_none(),
        };

        if is_deleted {
            return Ok(None);
        }

        // Read the authoritative metadata from `inner` after the notification.
        // There is a brief window between the notification and this read during
        // which the state could advance again; the caller must handle the
        // returned state defensively (re-check it before acting).
        Ok(self
            .inner
            .read()
            .await
            .records
            .get(sandbox_id)
            .map(|record| record.metadata.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::SandboxState;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    #[tokio::test]
    async fn in_memory_store_roundtrip() {
        let store = InMemoryMetadataStore::new();
        let sandbox_id = SandboxId::new();
        let metadata = SandboxMetadata {
            id: sandbox_id,
            timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        };

        store.add(metadata.clone()).await.unwrap();

        let got = store.get(&sandbox_id).await.unwrap().unwrap();
        assert_eq!(got.id, sandbox_id);
        assert_eq!(got.timeout, Some(Duration::from_secs(30)));

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 1);
        let ids = store.list_ids().await.unwrap();
        assert_eq!(ids, vec![sandbox_id]);

        let removed = store.remove(&sandbox_id).await.unwrap();
        assert!(removed.is_some());
        assert!(store.get(&sandbox_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_expired_only_returns_expired_records() {
        let store = InMemoryMetadataStore::new();
        let base = UNIX_EPOCH + Duration::from_secs(1_000);

        let expired_id = SandboxId::new();
        let expired = SandboxMetadata {
            id: expired_id,
            timeout: Some(Duration::from_secs(1)),
            created_at: base,
            expires_at: Some(base + Duration::from_secs(1)),
            ..Default::default()
        };
        let active = SandboxMetadata {
            id: SandboxId::new(),
            timeout: Some(Duration::from_secs(100)),
            created_at: base,
            expires_at: Some(base + Duration::from_secs(100)),
            ..Default::default()
        };

        store.add(expired).await.unwrap();
        store.add(active).await.unwrap();

        let got = store
            .list_expired(base + Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, expired_id);
    }

    #[tokio::test]
    async fn list_expired_reflects_updated_expiry_index() {
        let store = InMemoryMetadataStore::new();
        let base = UNIX_EPOCH + Duration::from_secs(1_000);
        let sandbox_id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id: sandbox_id,
                state: SandboxState::Running,
                created_at: base,
                expires_at: Some(base + Duration::from_secs(100)),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut updated = store.get(&sandbox_id).await.unwrap().unwrap();
        updated.expires_at = Some(base + Duration::from_secs(1));
        store.update(updated).await.unwrap();

        let expired = store
            .list_expired(base + Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].id, sandbox_id);
    }

    #[tokio::test]
    async fn list_filtered_matches_state_and_user_metadata() {
        let store = InMemoryMetadataStore::new();

        let mut user_metadata_match = HashMap::new();
        user_metadata_match.insert("team".to_string(), "alpha".to_string());
        user_metadata_match.insert("region".to_string(), "us".to_string());

        let mut user_metadata_other = HashMap::new();
        user_metadata_other.insert("team".to_string(), "beta".to_string());

        let match_id = SandboxId::new();
        store
            .add(SandboxMetadata {
                id: match_id,
                state: SandboxState::Running,
                user_metadata: Some(user_metadata_match.clone()),
                ..Default::default()
            })
            .await
            .unwrap();

        store
            .add(SandboxMetadata {
                id: SandboxId::new(),
                state: SandboxState::Running,
                user_metadata: Some(user_metadata_other),
                ..Default::default()
            })
            .await
            .unwrap();

        store
            .add(SandboxMetadata {
                id: SandboxId::new(),
                state: SandboxState::Paused,
                user_metadata: Some(user_metadata_match.clone()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut required = HashMap::new();
        required.insert("team".to_string(), "alpha".to_string());

        let filtered = store
            .list_filtered(SandboxListFilter {
                states: Some(vec![SandboxState::Running]),
                user_metadata: Some(required),
                ..SandboxListFilter::matches_all()
            })
            .await
            .unwrap();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, match_id);
    }

    #[tokio::test]
    async fn list_filtered_matches_started_after_and_template_id_or_alias() {
        let store = InMemoryMetadataStore::new();
        let base = UNIX_EPOCH + Duration::from_secs(1_000);
        let matching_after_id = SandboxId::new();
        let matching_at_id = SandboxId::new();
        let before_id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id: matching_after_id,
                snapshot_id: "template-id".to_string(),
                snapshot_alias: Some("template-alias".to_string()),
                created_at: base + Duration::from_secs(1),
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .add(SandboxMetadata {
                id: matching_at_id,
                snapshot_id: "template-id".to_string(),
                snapshot_alias: Some("template-alias".to_string()),
                created_at: base,
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .add(SandboxMetadata {
                id: before_id,
                snapshot_id: "template-id".to_string(),
                snapshot_alias: Some("template-alias".to_string()),
                created_at: base - Duration::from_secs(1),
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .add(SandboxMetadata {
                snapshot_id: "other-template".to_string(),
                created_at: base,
                ..Default::default()
            })
            .await
            .unwrap();

        for template in ["template-id", "template-alias"] {
            let filtered = store
                .list_filtered(SandboxListFilter {
                    started_after: Some(base),
                    template: Some(template.to_string()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(filtered.len(), 2);
            assert!(filtered
                .iter()
                .any(|metadata| metadata.id == matching_after_id));
            assert!(filtered
                .iter()
                .any(|metadata| metadata.id == matching_at_id));
            assert!(!filtered.iter().any(|metadata| metadata.id == before_id));
        }
    }

    #[tokio::test]
    async fn list_filtered_supports_multiple_states() {
        let store = InMemoryMetadataStore::new();
        let running_id = SandboxId::new();
        let paused_id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id: running_id,
                state: SandboxState::Running,
                ..Default::default()
            })
            .await
            .unwrap();

        store
            .add(SandboxMetadata {
                id: paused_id,
                state: SandboxState::Paused,
                ..Default::default()
            })
            .await
            .unwrap();

        store
            .add(SandboxMetadata {
                id: SandboxId::new(),
                state: SandboxState::Creating,
                ..Default::default()
            })
            .await
            .unwrap();

        let filtered = store
            .list_filtered(SandboxListFilter {
                states: Some(vec![SandboxState::Running, SandboxState::Paused]),
                ..SandboxListFilter::matches_all()
            })
            .await
            .unwrap();

        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|m| m.id == running_id));
        assert!(filtered.iter().any(|m| m.id == paused_id));
    }

    #[tokio::test]
    async fn list_filtered_excludes_states() {
        let store = InMemoryMetadataStore::new();
        let running_id = SandboxId::new();
        let creating_id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id: running_id,
                state: SandboxState::Running,
                ..Default::default()
            })
            .await
            .unwrap();

        store
            .add(SandboxMetadata {
                id: SandboxId::new(),
                state: SandboxState::Paused,
                ..Default::default()
            })
            .await
            .unwrap();

        store
            .add(SandboxMetadata {
                id: creating_id,
                state: SandboxState::Creating,
                ..Default::default()
            })
            .await
            .unwrap();

        let filtered = store
            .list_filtered(SandboxListFilter {
                excluded_states: Some(vec![SandboxState::Paused]),
                ..SandboxListFilter::matches_all()
            })
            .await
            .unwrap();

        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|m| m.id == running_id));
        assert!(filtered.iter().any(|m| m.id == creating_id));
    }

    #[tokio::test]
    async fn list_filtered_reflects_state_transition() {
        let store = InMemoryMetadataStore::new();
        let sandbox_id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id: sandbox_id,
                state: SandboxState::Running,
                ..Default::default()
            })
            .await
            .unwrap();

        store
            .update_state_if_state(&sandbox_id, SandboxState::Paused, &[SandboxState::Running])
            .await
            .unwrap();

        let running = store
            .list_filtered(SandboxListFilter {
                states: Some(vec![SandboxState::Running]),
                ..SandboxListFilter::matches_all()
            })
            .await
            .unwrap();
        assert!(running.is_empty());

        let paused = store
            .list_filtered(SandboxListFilter {
                states: Some(vec![SandboxState::Paused]),
                ..SandboxListFilter::matches_all()
            })
            .await
            .unwrap();
        assert_eq!(paused.len(), 1);
        assert_eq!(paused[0].id, sandbox_id);
    }

    #[tokio::test]
    async fn update_if_state_applies_only_on_expected_state() {
        let store = InMemoryMetadataStore::new();
        let id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id,
                state: SandboxState::Running,
                ..Default::default()
            })
            .await
            .unwrap();

        let previous = store
            .update_if_state(&id, &[SandboxState::Running], |metadata| {
                metadata.state = SandboxState::Pausing;
            })
            .await
            .unwrap()
            .previous;
        assert_eq!(previous.state, SandboxState::Running);
        assert_eq!(previous.resources, SandboxMetadata::default().resources);

        let conflict = store
            .update_if_state(&id, &[SandboxState::Running], |metadata| {
                metadata.state = SandboxState::Paused;
            })
            .await
            .unwrap_err();

        match conflict {
            StoreError::StateConflict {
                sandbox_id,
                expected_states,
                actual_state,
            } => {
                assert_eq!(sandbox_id, id);
                assert_eq!(expected_states, vec![SandboxState::Running]);
                assert_eq!(actual_state, SandboxState::Pausing);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_if_state_preserves_concurrent_field_changes() {
        let store = Arc::new(InMemoryMetadataStore::new());
        let sandbox_id = SandboxId::new();
        let mut metadata = SandboxMetadata {
            id: sandbox_id,
            state: SandboxState::Running,
            user_metadata: Some(HashMap::from([(
                "owner".to_string(),
                "initial".to_string(),
            )])),
            ..Default::default()
        };
        metadata.set_timeout(Some(Duration::from_secs(30)));
        store.add(metadata).await.unwrap();

        let (timeout_entered_tx, timeout_entered_rx) = tokio::sync::oneshot::channel();
        let (timeout_release_tx, timeout_release_rx) = std::sync::mpsc::channel();
        let store_for_timeout = Arc::clone(&store);
        let runtime_handle = tokio::runtime::Handle::current();
        let timeout_update = tokio::task::spawn_blocking(move || {
            runtime_handle.block_on(async move {
                store_for_timeout
                    .update_if_state(&sandbox_id, &[SandboxState::Running], move |metadata| {
                        let _ = timeout_entered_tx.send(());
                        timeout_release_rx
                            .recv()
                            .expect("timeout update should be released");
                        metadata.set_timeout(Some(Duration::from_secs(120)));
                    })
                    .await
            })
        });
        timeout_entered_rx
            .await
            .expect("timeout update should enter the update closure");

        let store_for_user_metadata = Arc::clone(&store);
        let user_metadata_update = tokio::spawn(async move {
            store_for_user_metadata
                .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                    metadata
                        .user_metadata
                        .get_or_insert_with(HashMap::new)
                        .insert("trace".to_string(), "kept".to_string());
                })
                .await
        });

        tokio::task::yield_now().await;
        timeout_release_tx
            .send(())
            .expect("timeout update task should still be waiting");
        timeout_update
            .await
            .expect("timeout update task should not panic")
            .unwrap();
        user_metadata_update
            .await
            .expect("user metadata update task should not panic")
            .unwrap();

        let fetched = store
            .get(&sandbox_id)
            .await
            .unwrap()
            .expect("sandbox metadata should exist after atomic updates");
        assert_eq!(fetched.timeout, Some(Duration::from_secs(120)));
        assert_eq!(
            fetched.user_metadata.as_ref().and_then(|m| m.get("owner")),
            Some(&"initial".to_string())
        );
        assert_eq!(
            fetched.user_metadata.as_ref().and_then(|m| m.get("trace")),
            Some(&"kept".to_string())
        );
    }

    #[tokio::test]
    async fn add_fails_if_sandbox_exists() {
        let store = InMemoryMetadataStore::new();
        let id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id,
                ..Default::default()
            })
            .await
            .unwrap();

        let err = store
            .add(SandboxMetadata {
                id,
                ..Default::default()
            })
            .await
            .unwrap_err();

        assert!(matches!(err, StoreError::SandboxAlreadyExists { .. }));
    }

    #[tokio::test]
    async fn update_state_if_state_updates_only_state() {
        let store = InMemoryMetadataStore::new();

        let id = SandboxId::new();
        let mut metadata = SandboxMetadata {
            id,
            state: SandboxState::Running,
            ..Default::default()
        };
        metadata.timeout = Some(Duration::from_secs(9));

        store.add(metadata).await.unwrap();

        let previous_state = store
            .update_state_if_state(&id, SandboxState::Pausing, &[SandboxState::Running])
            .await
            .unwrap();

        assert_eq!(previous_state, SandboxState::Running);
        let stored = store.get(&id).await.unwrap().unwrap();
        assert_eq!(stored.state, SandboxState::Pausing);
        assert_eq!(stored.timeout, Some(Duration::from_secs(9)));
    }

    /// `wait_while_in_states` must return immediately when the sandbox is
    /// already in a stable (non-transitional) state.
    #[tokio::test]
    async fn wait_while_in_states_returns_immediately_if_already_stable() {
        let store = InMemoryMetadataStore::new();
        let id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id,
                state: SandboxState::Running,
                ..Default::default()
            })
            .await
            .unwrap();

        let result = store
            .wait_while_in_states(&id, &[SandboxState::Creating])
            .await
            .unwrap();

        assert!(result.is_some());
        assert_eq!(result.unwrap().state, SandboxState::Running);
    }

    /// `wait_while_in_states` must wake up and return the new metadata once the
    /// state transitions out of the transitional set.
    #[tokio::test]
    async fn wait_while_in_states_wakes_on_transition() {
        use tokio::time::{timeout, Duration};

        let store = std::sync::Arc::new(InMemoryMetadataStore::new());
        let id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id,
                state: SandboxState::Creating,
                ..Default::default()
            })
            .await
            .unwrap();

        let store_clone = store.clone();

        // Spawn a task that waits for the Creating state to end.
        let wait_handle = tokio::spawn(async move {
            store_clone
                .wait_while_in_states(&id, &[SandboxState::Creating])
                .await
        });

        // Give the waiter task a moment to start waiting.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Advance state to Running.
        store
            .update_state_if_state(&id, SandboxState::Running, &[SandboxState::Creating])
            .await
            .unwrap();

        // The waiter should resolve promptly.
        let result = timeout(Duration::from_secs(1), wait_handle)
            .await
            .expect("wait_while_in_states should resolve within timeout")
            .expect("task should not panic")
            .unwrap();

        assert!(result.is_some());
        assert_eq!(result.unwrap().state, SandboxState::Running);
    }

    /// `wait_while_in_states` returns `Ok(None)` when the sandbox is removed.
    #[tokio::test]
    async fn wait_while_in_states_returns_none_on_remove() {
        use tokio::time::{timeout, Duration};

        let store = std::sync::Arc::new(InMemoryMetadataStore::new());
        let id = SandboxId::new();

        store
            .add(SandboxMetadata {
                id,
                state: SandboxState::Creating,
                ..Default::default()
            })
            .await
            .unwrap();

        let store_clone = store.clone();

        let wait_handle = tokio::spawn(async move {
            store_clone
                .wait_while_in_states(&id, &[SandboxState::Creating])
                .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        store.remove(&id).await.unwrap();

        let result = timeout(Duration::from_secs(1), wait_handle)
            .await
            .expect("wait should resolve after remove")
            .expect("task should not panic")
            .unwrap();

        assert!(
            result.is_none(),
            "should return None when sandbox is removed"
        );
    }

    /// `wait_while_in_states` returns `Ok(None)` when the sandbox never existed.
    #[tokio::test]
    async fn wait_while_in_states_returns_none_for_unknown_sandbox() {
        let store = InMemoryMetadataStore::new();
        let id = SandboxId::new();

        let result = store
            .wait_while_in_states(&id, &[SandboxState::Creating])
            .await
            .unwrap();

        assert!(result.is_none());
    }
}
