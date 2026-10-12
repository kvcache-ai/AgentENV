use crate::snapshot::{SnapshotAlias, SnapshotId};

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
    /// One snapshot or template record.
    SnapshotRecord(SnapshotId),
    /// The snapshot ID that currently owns an alias.
    SnapshotAlias(SnapshotAlias),
}

impl DocumentKey {
    pub(crate) fn path(&self) -> String {
        match self {
            Self::BuildLog(id) => format!("snapshots/build-logs/{id}.json"),
            Self::BuildCacheState => "template-build/cache-head.json".to_owned(),
            Self::VolumeRecord(id) => format!("{VOLUME_RECORDS_PREFIX}{id}.json"),
            Self::VolumeAlias(name) => format!("volumes/aliases/{name}.json"),
            Self::SnapshotRecord(id) => format!("{SNAPSHOT_RECORDS_PREFIX}{id}.json"),
            Self::SnapshotAlias(alias) => format!("catalog/aliases/{alias}.json"),
        }
    }

    /// The path in a POSIX repository, which predates the shared layout for
    /// aliases.
    pub(crate) fn posix_path(&self) -> String {
        match self {
            Self::VolumeAlias(name) => format!("volumes/aliases/{name}"),
            Self::SnapshotAlias(alias) => format!("catalog/aliases/{alias}"),
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
            Self::SnapshotRecord(id) => format!("{SNAPSHOT_RECORDS_PREFIX}{id}.lock"),
            Self::SnapshotAlias(alias) => format!("catalog/aliases/{alias}.lock"),
            Self::BuildLog(_) => format!("{}.lock", self.path()),
        }
    }
}

const VOLUME_RECORDS_PREFIX: &str = "volumes/records/";
const SNAPSHOT_RECORDS_PREFIX: &str = "catalog/records/";

/// Documents that are listed together, in ID order.
#[derive(Clone, Copy, Debug)]
pub enum DocumentCollection {
    VolumeRecords,
    SnapshotRecords,
}

impl DocumentCollection {
    /// The directory or object prefix that holds every document in the collection.
    pub(crate) fn prefix(self) -> &'static str {
        match self {
            Self::VolumeRecords => VOLUME_RECORDS_PREFIX,
            Self::SnapshotRecords => SNAPSHOT_RECORDS_PREFIX,
        }
    }

    /// The key of the document with `id`; `None` when `id` is not valid for
    /// the collection.
    pub(crate) fn key(self, id: &str) -> Option<DocumentKey> {
        match self {
            Self::VolumeRecords => Some(DocumentKey::VolumeRecord(id.to_owned())),
            Self::SnapshotRecords => SnapshotId::parse(id).ok().map(DocumentKey::SnapshotRecord),
        }
    }

    /// Extracts the ID from a file name under [`Self::prefix`].
    pub(crate) fn id_from_file_name(self, file_name: &str) -> Option<&str> {
        match self {
            Self::VolumeRecords | Self::SnapshotRecords => file_name.strip_suffix(".json"),
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
        let snapshot_id = SnapshotId::generate();
        let snapshot = DocumentKey::SnapshotRecord(snapshot_id.clone());
        assert_eq!(
            snapshot.path(),
            format!("catalog/records/{snapshot_id}.json")
        );
        assert_eq!(snapshot.posix_path(), snapshot.path());
        assert_eq!(
            snapshot.posix_lock_path(),
            format!("catalog/records/{snapshot_id}.lock")
        );
        let snapshot_alias =
            DocumentKey::SnapshotAlias(SnapshotAlias::parse("base-image").unwrap());
        assert_eq!(snapshot_alias.path(), "catalog/aliases/base-image.json");
        assert_eq!(snapshot_alias.posix_path(), "catalog/aliases/base-image");
        assert_eq!(
            snapshot_alias.posix_lock_path(),
            "catalog/aliases/base-image.lock"
        );
        let snapshots = DocumentCollection::SnapshotRecords;
        assert!(snapshot.path().starts_with(snapshots.prefix()));
        assert!(snapshots.key(&snapshot_id.to_string()).is_some());
        assert!(snapshots.key("not-an-id").is_none());

        let records = DocumentCollection::VolumeRecords;
        assert!(record.path().starts_with(records.prefix()));
        assert_eq!(records.id_from_file_name("vol_test.json"), Some("vol_test"));
        assert_eq!(records.id_from_file_name("vol_test.lock"), None);
    }
}
