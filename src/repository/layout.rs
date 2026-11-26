use super::{LayerRole, RepositoryError, RepositoryResult};
use crate::types::{ImageName, ImageTag, SnapshotAlias, SnapshotId};

/// A small shared document that every backend stores at the same path relative
/// to its repository root. Runtime builds write [`DocumentKey::BuildLog`]; the
/// remaining keys exist to read pre-SQL legacy documents during migration.
#[derive(Clone, Debug)]
pub enum DocumentKey {
    /// The complete log of one template or image build.
    BuildLog(SnapshotId),
    /// Legacy volume record, keyed by volume ID (read-only migration).
    VolumeRecord(String),
    /// Legacy volume name pointer (read-only migration).
    VolumeAlias(String),
    /// Legacy snapshot or template record (read-only migration).
    SnapshotRecord(SnapshotId),
    /// Legacy snapshot alias pointer (read-only migration).
    SnapshotAlias(SnapshotAlias),
    /// Legacy published image description, named by its digest (read-only migration).
    /// Construct it with [`DocumentKey::image`], which validates the digest.
    Image(ImageDigest),
    /// Legacy image tag pointer (read-only migration).
    ImageTag(ImageTag),
}

/// A validated `sha256:<hex>` image digest. Only [`DocumentKey::image`]
/// creates one, so an image key never leaves the repository.
#[derive(Clone, Debug)]
pub struct ImageDigest(String);

impl DocumentKey {
    pub fn image(digest: &str) -> RepositoryResult<Self> {
        if !crate::digest::is_sha256_digest(digest) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("invalid image digest '{digest}'"),
            });
        }
        Ok(Self::Image(ImageDigest(digest.to_owned())))
    }

    pub(crate) fn path(&self) -> String {
        match self {
            Self::BuildLog(id) => format!("snapshots/build-logs/{id}.json"),
            Self::VolumeRecord(id) => format!("{VOLUME_RECORDS_PREFIX}{id}.json"),
            Self::VolumeAlias(name) => format!("volumes/aliases/{name}.json"),
            Self::SnapshotRecord(id) => format!("{SNAPSHOT_RECORDS_PREFIX}{id}.json"),
            Self::SnapshotAlias(alias) => format!("catalog/aliases/{alias}.json"),
            Self::Image(digest) => format!("{IMAGES_PREFIX}{}.json", digest.0),
            Self::ImageTag(tag) => format!("{}{}.json", image_tags_prefix(tag.name()), tag.tag()),
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
}

fn image_tags_prefix(name: &ImageName) -> String {
    format!("{IMAGE_TAGS_PREFIX}{name}/")
}

const VOLUME_RECORDS_PREFIX: &str = "volumes/records/";
const IMAGES_PREFIX: &str = "images/descriptions/";
const IMAGE_TAGS_PREFIX: &str = "images/tags/";
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

/// One blob a repository stores. Like [`DocumentKey`], this is the only place
/// that names blob paths; changing one strands the blobs already stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobKey {
    /// A content-addressed layer, stored once for every snapshot and volume
    /// that uses it.
    Layer { digest: String, role: LayerRole },
    /// One file of one snapshot, removed with the snapshot.
    Snapshot(SnapshotId, SnapshotFile),
}

/// The files a snapshot stores besides its layers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotFile {
    VmState,
    FirecrackerManifest,
    CommittedManifest,
    Record,
    StartupManifest,
}

impl SnapshotFile {
    /// The file's name among the snapshot's files.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::VmState => "vm_state.bin",
            Self::FirecrackerManifest => "firecracker-manifest.json",
            Self::CommittedManifest => "committed-manifest.json",
            Self::Record => "snapshot.json",
            Self::StartupManifest => "memory-startup.pack",
        }
    }
}

impl BlobKey {
    /// The directory or object prefix that holds every managed layer.
    pub(crate) const LAYERS_DIR: &'static str = agentenv_domain::LAYERS_DIR;

    pub(crate) fn managed_layers_repo_blob_url(bucket: &str, prefix: &str) -> String {
        let bucket = bucket.trim();
        let prefix = prefix.trim().trim_matches('/');
        let key = Self::LAYERS_DIR;
        if prefix.is_empty() {
            format!("s3://{bucket}/{key}")
        } else {
            format!("s3://{bucket}/{prefix}/{key}")
        }
    }

    /// Where a POSIX repository prepares blobs before storing them, so they
    /// are on the repository's filesystem.
    pub(crate) const POSIX_STAGING_DIR: &'static str = "staging";

    /// The object key in an object-store repository.
    pub(crate) fn path(&self) -> String {
        match self {
            Self::Layer { digest, .. } => agentenv_domain::layer_path(digest),
            Self::Snapshot(id, file) => format!("{}/{}", Self::snapshot_dir(id), file.name()),
        }
    }

    /// The path in a POSIX repository, which predates the shared layout.
    pub(crate) fn posix_path(&self) -> String {
        match self {
            Self::Layer { digest, .. } => agentenv_domain::posix_layer_path(digest),
            Self::Snapshot(id, file) => {
                format!("{}/{}", Self::posix_snapshot_dir(id), file.name())
            }
        }
    }

    /// The object prefix that holds every blob of one snapshot.
    pub(crate) fn snapshot_dir(id: &SnapshotId) -> String {
        agentenv_domain::snapshot_dir(id)
    }

    /// The directory that holds every file of one snapshot in a POSIX
    /// repository.
    pub(crate) fn posix_snapshot_dir(id: &SnapshotId) -> String {
        agentenv_domain::posix_snapshot_dir(id)
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
        let record = DocumentKey::VolumeRecord("vol_test".to_owned());
        assert_eq!(record.path(), "volumes/records/vol_test.json");
        assert_eq!(record.posix_path(), "volumes/records/vol_test.json");
        let alias = DocumentKey::VolumeAlias("test-data".to_owned());
        assert_eq!(alias.path(), "volumes/aliases/test-data.json");
        assert_eq!(alias.posix_path(), "volumes/aliases/test-data");
        let snapshot_id = SnapshotId::generate();
        let snapshot = DocumentKey::SnapshotRecord(snapshot_id.clone());
        assert_eq!(
            snapshot.path(),
            format!("catalog/records/{snapshot_id}.json")
        );
        assert_eq!(snapshot.posix_path(), snapshot.path());
        let snapshot_alias =
            DocumentKey::SnapshotAlias(SnapshotAlias::parse("base-image").unwrap());
        assert_eq!(snapshot_alias.path(), "catalog/aliases/base-image.json");
        assert_eq!(snapshot_alias.posix_path(), "catalog/aliases/base-image");
        let snapshots = DocumentCollection::SnapshotRecords;
        assert!(snapshot.path().starts_with(snapshots.prefix()));
        assert!(snapshots.key(&snapshot_id.to_string()).is_some());
        assert!(snapshots.key("not-an-id").is_none());

        let records = DocumentCollection::VolumeRecords;
        assert!(record.path().starts_with(records.prefix()));
        assert_eq!(records.id_from_file_name("vol_test.json"), Some("vol_test"));
        assert_eq!(records.id_from_file_name("vol_test.lock"), None);

        let digest = crate::digest::sha256_digest(b"image");
        let image = DocumentKey::image(&digest).unwrap();
        assert_eq!(image.path(), format!("images/descriptions/{digest}.json"));
        assert_eq!(image.posix_path(), image.path());
        assert!(DocumentKey::image("sha256:../../outside").is_err());
        let tag = DocumentKey::ImageTag(ImageTag::parse("web").unwrap());
        assert_eq!(tag.path(), "images/tags/web/latest.json");
        assert_eq!(tag.posix_path(), tag.path());
    }

    #[test]
    fn blob_paths_match_existing_repositories() {
        let layer = BlobKey::Layer {
            digest: "sha256:abc".to_owned(),
            role: LayerRole::Rootfs,
        };
        assert_eq!(layer.path(), "managed-layers/sha256:abc");
        assert_eq!(
            layer.posix_path(),
            "managed-layers/sha256_abc.overlaybd.commit"
        );
        let id = SnapshotId::generate();
        let vm_state = BlobKey::Snapshot(id.clone(), SnapshotFile::VmState);
        assert_eq!(vm_state.path(), format!("artifacts/{id}/vm_state.bin"));
        assert_eq!(
            vm_state.posix_path(),
            format!("snapshots/{id}/vm_state.bin")
        );
        let manifest = BlobKey::Snapshot(id.clone(), SnapshotFile::FirecrackerManifest);
        assert_eq!(
            manifest.path(),
            format!("artifacts/{id}/firecracker-manifest.json")
        );
        let startup = BlobKey::Snapshot(id.clone(), SnapshotFile::StartupManifest);
        assert_eq!(
            startup.posix_path(),
            format!("snapshots/{id}/memory-startup.pack")
        );
        assert!(vm_state
            .path()
            .starts_with(&format!("{}/", BlobKey::snapshot_dir(&id))));
    }
}
