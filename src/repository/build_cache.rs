use std::collections::BTreeSet;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{DocumentKey, Repository, RepositoryError, RepositoryResult};

/// The head and its pending retirements are committed in one repository write.
#[derive(Default, Deserialize, Serialize)]
pub struct BuildCacheState {
    pub current: Option<String>,
    pub retired: BTreeSet<String>,
}

/// Typed access to the shared build-cache state document.
#[async_trait]
pub trait BuildCacheStore {
    /// Shared immutable cache seed for Dockerfile builds, independent of node and template.
    async fn get_build_cache_head(&self) -> RepositoryResult<Option<String>>;

    async fn get_build_cache_state(&self) -> RepositoryResult<BuildCacheState>;

    /// Atomically publishes a fresh seed and records the previous seed for retirement.
    async fn replace_build_cache_head(&self, volume_id: &str) -> RepositoryResult<Option<String>>;

    /// Acknowledges retirement after the volume has been deleted.
    async fn forget_retired_build_cache(&self, volume_id: &str) -> RepositoryResult<()>;
}

#[async_trait]
impl<R: Repository + ?Sized> BuildCacheStore for R {
    async fn get_build_cache_head(&self) -> RepositoryResult<Option<String>> {
        Ok(self.get_build_cache_state().await?.current)
    }

    async fn get_build_cache_state(&self) -> RepositoryResult<BuildCacheState> {
        BuildCacheState::load(
            self.get_document(DocumentKey::BuildCacheState)
                .await?
                .as_deref(),
        )
    }

    async fn replace_build_cache_head(&self, volume_id: &str) -> RepositoryResult<Option<String>> {
        let volume_id = volume_id.to_owned();
        let replaced = self
            .update_document(
                DocumentKey::BuildCacheState,
                Box::new(move |bytes| {
                    let mut state = BuildCacheState::load(bytes)?;
                    state.replace(&volume_id)?;
                    state.encode().map(Some)
                }),
            )
            .await?;
        Ok(BuildCacheState::load(replaced.as_deref())?.current)
    }

    async fn forget_retired_build_cache(&self, volume_id: &str) -> RepositoryResult<()> {
        let volume_id = volume_id.to_owned();
        self.update_document(
            DocumentKey::BuildCacheState,
            Box::new(move |bytes| {
                let mut state = BuildCacheState::load(bytes)?;
                state.retired.remove(&volume_id);
                state.encode().map(Some)
            }),
        )
        .await?;
        Ok(())
    }
}

impl BuildCacheState {
    fn load(bytes: Option<&[u8]>) -> RepositoryResult<Self> {
        bytes.map_or_else(|| Ok(Self::default()), Self::decode)
    }

    fn encode(&self) -> RepositoryResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|error| RepositoryError::backend("encode build cache state", error))
    }

    fn decode(bytes: &[u8]) -> RepositoryResult<Self> {
        let state: Self = serde_json::from_slice(bytes)
            .map_err(|error| RepositoryError::backend("read build cache state", error))?;
        for id in state.current.iter().chain(&state.retired) {
            Self::validate_id(id)?;
        }
        if state
            .current
            .as_ref()
            .is_some_and(|id| state.retired.contains(id))
        {
            return Err(RepositoryError::InvalidRequest {
                reason: "current build cache seed is also retired".to_owned(),
            });
        }
        Ok(state)
    }

    fn validate_id(id: &str) -> RepositoryResult<()> {
        if !crate::volume::is_valid_volume_component(id) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("invalid build cache volume ID '{id}'"),
            });
        }
        Ok(())
    }

    fn replace(&mut self, id: &str) -> RepositoryResult<Option<String>> {
        Self::validate_id(id)?;
        if self.retired.contains(id) {
            return Err(RepositoryError::InvalidRequest {
                reason: "cannot publish a retired build cache seed".to_owned(),
            });
        }
        let previous = self.current.replace(id.to_owned());
        if let Some(previous) = &previous {
            if previous != id {
                self.retired.insert(previous.clone());
            }
        }
        Ok(previous)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::backends::{PosixFsBackend, PosixFsBackendConfig};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn build_cache_head_updates_are_atomic_across_repository_instances() {
        let temp = tempfile::tempdir().unwrap();
        let config = PosixFsBackendConfig {
            root: temp.path().join("repository"),
            cache_root: Some(temp.path().join("cache")),
            runtime_cache_root: None,
        };
        let repository = || PosixFsBackend::new(config.clone()).unwrap().repository();
        let store = repository();
        assert_eq!(store.get_build_cache_head().await.unwrap(), None);
        let ids = (0..16)
            .map(|i| format!("vol_cache_{i}"))
            .collect::<Vec<_>>();
        let jobs = ids
            .iter()
            .map(|id| {
                let (repository, id) = (repository(), id.clone());
                tokio::spawn(async move { repository.replace_build_cache_head(&id).await })
            })
            .collect::<Vec<_>>();
        let mut previous = Vec::new();
        for job in jobs {
            previous.push(job.await.unwrap().unwrap());
        }
        assert_eq!(previous.iter().filter(|id| id.is_none()).count(), 1);
        let state = store.get_build_cache_state().await.unwrap();
        previous.push(state.current.clone());
        let mut observed = previous.into_iter().flatten().collect::<Vec<_>>();
        observed.sort();
        let mut expected = ids;
        expected.sort();
        assert_eq!(observed, expected);
        assert_eq!(state.retired.len(), 15);
        assert!(!state.retired.contains(state.current.as_ref().unwrap()));
        for id in &state.retired {
            assert!(store.replace_build_cache_head(id).await.is_err());
            store.forget_retired_build_cache(id).await.unwrap();
        }
        let after = store.get_build_cache_state().await.unwrap();
        assert!(after.retired.is_empty());
        assert_eq!(after.current, state.current);
        assert!(store.replace_build_cache_head("../invalid").await.is_err());
    }

    #[test]
    fn validates_cache_volume_ids() {
        assert_eq!(
            BuildCacheState::decode(br#"{"current":"vol_seed","retired":[]}"#)
                .unwrap()
                .current
                .as_deref(),
            Some("vol_seed")
        );
        for bytes in [
            br#"{"current":"../invalid","retired":[]}"#.as_slice(),
            br#"{"current":"vol_a","retired":["../invalid"]}"#,
            br#"{"current":"vol_a","retired":["vol_a"]}"#,
        ] {
            assert!(BuildCacheState::decode(bytes).is_err());
        }
    }
}
