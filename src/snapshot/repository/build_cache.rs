use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{RepositoryError, RepositoryResult};

/// The head and its pending retirements are committed in one repository write.
#[derive(Default, Deserialize, Serialize)]
pub struct BuildCacheState {
    pub current: Option<String>,
    pub retired: BTreeSet<String>,
}

impl BuildCacheState {
    pub(crate) fn decode(bytes: &[u8]) -> RepositoryResult<Self> {
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

    pub(crate) fn replace(&mut self, id: &str) -> RepositoryResult<Option<String>> {
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
