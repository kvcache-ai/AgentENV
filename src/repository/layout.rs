use crate::snapshot::SnapshotId;

/// A small shared document that every backend stores at the same path relative
/// to its repository root. This is the only place that names those paths;
/// changing one strands the data already in existing repositories.
#[derive(Clone, Debug)]
pub enum DocumentKey {
    /// The complete log of one template build.
    BuildLog(SnapshotId),
    /// The shared Dockerfile build-cache seed and its pending retirements.
    BuildCacheState,
}

impl DocumentKey {
    pub(crate) fn path(&self) -> String {
        match self {
            Self::BuildLog(id) => format!("snapshots/build-logs/{id}.json"),
            Self::BuildCacheState => "template-build/cache-head.json".to_owned(),
        }
    }

    /// The lock file that serializes POSIX document updates across nodes.
    pub(crate) fn posix_lock_path(&self) -> String {
        match self {
            // Kept at its original path so nodes on older versions still
            // exclude each other.
            Self::BuildCacheState => "volumes/aliases/aenv-buildkit-cache-head.lock".to_owned(),
            _ => format!("{}.lock", self.path()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_paths_match_existing_repositories() {
        let id = SnapshotId::generate();
        assert_eq!(
            DocumentKey::BuildLog(id.clone()).path(),
            format!("snapshots/build-logs/{id}.json")
        );
        assert_eq!(
            DocumentKey::BuildCacheState.path(),
            "template-build/cache-head.json"
        );
        assert_eq!(
            DocumentKey::BuildCacheState.posix_lock_path(),
            "volumes/aliases/aenv-buildkit-cache-head.lock"
        );
    }
}
