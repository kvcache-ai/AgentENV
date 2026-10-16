use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::p2p::{P2pArtifactDescriptor, P2pArtifactKey};

#[derive(Clone)]
pub(super) struct DescriptorCache {
    inner: Arc<DashMap<P2pArtifactKey, CachedDescriptor>>,
    hit_ttl: Duration,
    miss_ttl: Duration,
    max_entries: usize,
}

#[derive(Clone)]
struct CachedDescriptor {
    inserted_at: Instant,
    descriptor: Option<P2pArtifactDescriptor>,
}

impl DescriptorCache {
    pub(super) fn new(hit_ttl: Duration, miss_ttl: Duration, max_entries: usize) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            hit_ttl,
            miss_ttl,
            max_entries,
        }
    }

    #[allow(clippy::option_option)] // None: not cached; Some(None): cached miss.
    pub(super) fn get(&self, key: &P2pArtifactKey) -> Option<Option<P2pArtifactDescriptor>> {
        let cached = self.inner.get(key)?;
        let ttl = if cached.descriptor.is_some() {
            self.hit_ttl
        } else {
            self.miss_ttl
        };
        if cached.inserted_at.elapsed() <= ttl {
            return Some(cached.descriptor.clone());
        }
        drop(cached);
        self.inner.remove(key);
        None
    }

    pub(super) fn insert(&self, key: P2pArtifactKey, descriptor: Option<P2pArtifactDescriptor>) {
        self.inner.insert(
            key,
            CachedDescriptor {
                inserted_at: Instant::now(),
                descriptor,
            },
        );
        self.prune_if_needed();
    }

    pub(super) fn remove(&self, key: &P2pArtifactKey) {
        self.inner.remove(key);
    }

    fn prune_if_needed(&self) {
        if self.max_entries == 0 {
            self.inner.clear();
            return;
        }
        if self.inner.len() <= self.max_entries {
            return;
        }

        let now = Instant::now();
        let expired: Vec<_> = self
            .inner
            .iter()
            .filter_map(|entry| {
                let ttl = if entry.descriptor.is_some() {
                    self.hit_ttl
                } else {
                    self.miss_ttl
                };
                (now.duration_since(entry.inserted_at) > ttl).then(|| entry.key().clone())
            })
            .collect();
        for key in expired {
            self.inner.remove(&key);
        }
        if self.inner.len() <= self.max_entries {
            return;
        }

        let mut entries: Vec<_> = self
            .inner
            .iter()
            .map(|entry| (entry.inserted_at, entry.key().clone()))
            .collect();
        entries.sort_by_key(|(inserted_at, _)| *inserted_at);
        let remove_count = self.inner.len().saturating_sub(self.max_entries);
        for (_, key) in entries.into_iter().take(remove_count) {
            self.inner.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_cache_prunes_old_entries_at_max_size() {
        let cache = DescriptorCache::new(Duration::from_secs(60), Duration::from_secs(60), 2);
        cache.insert("old-a".to_string(), None);
        cache.insert("old-b".to_string(), None);
        cache.insert("new-c".to_string(), None);

        assert!(cache.get(&"old-a".to_string()).is_none());
        assert!(cache.get(&"old-b".to_string()).is_some());
        assert!(cache.get(&"new-c".to_string()).is_some());
    }
}
