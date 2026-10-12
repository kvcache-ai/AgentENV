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
    /// One volume record, keyed by volume ID.
    VolumeRecord(String),
    /// The volume ID that currently owns a volume name.
    VolumeAlias(String),
}

impl DocumentKey {
    pub(crate) fn path(&self) -> String {
        match self {
            Self::BuildLog(id) => format!("snapshots/build-logs/{id}.json"),
            Self::BuildCacheState => "template-build/cache-head.json".to_owned(),
            Self::VolumeRecord(id) => format!("{VOLUME_RECORDS_PREFIX}{id}.json"),
            Self::VolumeAlias(name) => format!("volumes/aliases/{name}.json"),
        }
    }

    /// The path in a POSIX repository, which predates the shared layout for
    /// volume aliases.
    pub(crate) fn posix_path(&self) -> String {
        match self {
            Self::VolumeAlias(name) => format!("volumes/aliases/{name}"),
            _ => self.path(),
        }
    }

    /// The lock file that serializes POSIX document updates across nodes.
    pub(crate) fn posix_lock_path(&self) -> String {
        // Kept at their original paths so nodes on older versions still
        // exclude each other.
        match self {
            Self::BuildCacheState => "volumes/aliases/aenv-buildkit-cache-head.lock".to_owned(),
            Self::VolumeRecord(id) => format!("{VOLUME_RECORDS_PREFIX}{id}.lock"),
            Self::VolumeAlias(name) => format!("volumes/aliases/{name}.lock"),
            Self::BuildLog(_) => format!("{}.lock", self.path()),
        }
    }
}

const VOLUME_RECORDS_PREFIX: &str = "volumes/records/";

/// Documents that are listed together, in ID order.
#[derive(Clone, Copy, Debug)]
pub enum DocumentCollection {
    VolumeRecords,
}

impl DocumentCollection {
    /// The directory or object prefix that holds every document in the collection.
    pub(crate) fn prefix(self) -> &'static str {
        match self {
            Self::VolumeRecords => VOLUME_RECORDS_PREFIX,
        }
    }

    pub(crate) fn key(self, id: &str) -> DocumentKey {
        match self {
            Self::VolumeRecords => DocumentKey::VolumeRecord(id.to_owned()),
        }
    }

    /// Extracts the ID from a file name under [`Self::prefix`].
    pub(crate) fn id_from_file_name(self, file_name: &str) -> Option<&str> {
        match self {
            Self::VolumeRecords => file_name.strip_suffix(".json"),
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
        let record = DocumentKey::VolumeRecord("vol_test".to_owned());
        assert_eq!(record.path(), "volumes/records/vol_test.json");
        assert_eq!(record.posix_path(), "volumes/records/vol_test.json");
        assert_eq!(record.posix_lock_path(), "volumes/records/vol_test.lock");
        let alias = DocumentKey::VolumeAlias("test-data".to_owned());
        assert_eq!(alias.path(), "volumes/aliases/test-data.json");
        assert_eq!(alias.posix_path(), "volumes/aliases/test-data");
        assert_eq!(alias.posix_lock_path(), "volumes/aliases/test-data.lock");
        let records = DocumentCollection::VolumeRecords;
        assert!(record.path().starts_with(records.prefix()));
        assert_eq!(records.id_from_file_name("vol_test.json"), Some("vol_test"));
        assert_eq!(records.id_from_file_name("vol_test.lock"), None);
    }
}
