use super::LayerRole;
use crate::types::SnapshotId;

/// One blob a repository stores. This is the only place that names blob paths;
/// changing one strands the blobs already stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobKey {
    /// A content-addressed layer, stored once for every snapshot, volume, and
    /// published image that uses it.
    Layer { digest: String, role: LayerRole },
    /// One file of one snapshot, removed with the snapshot.
    Snapshot(SnapshotId, SnapshotFile),
    /// A build's persisted JSON log file.
    BuildLog(SnapshotId),
}

/// The files a snapshot stores besides its layers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotFile {
    VmState,
    FirecrackerManifest,
    Record,
    StartupManifest,
}

impl SnapshotFile {
    /// The file's name among the snapshot's files.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::VmState => "vm_state.bin",
            Self::FirecrackerManifest => "firecracker-manifest.json",
            Self::Record => "snapshot.json",
            Self::StartupManifest => "memory-startup.pack",
        }
    }
}

impl BlobKey {
    pub(crate) fn managed_layers_repo_blob_url(bucket: &str, prefix: &str) -> String {
        let bucket = bucket.trim();
        let prefix = prefix.trim().trim_matches('/');
        let key = agentenv_domain::LAYERS_DIR;
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
            Self::Snapshot(id, file) => {
                format!("{}/{}", agentenv_domain::snapshot_dir(id), file.name())
            }
            Self::BuildLog(id) => format!("snapshots/build-logs/{id}.json"),
        }
    }

    /// The path in a POSIX repository, which predates the shared layout.
    pub(crate) fn posix_path(&self) -> String {
        match self {
            Self::Layer { digest, .. } => agentenv_domain::posix_layer_path(digest),
            Self::Snapshot(id, file) => {
                format!(
                    "{}/{}",
                    agentenv_domain::posix_snapshot_dir(id),
                    file.name()
                )
            }
            Self::BuildLog(id) => format!("snapshots/build-logs/{id}.json"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let build_log = BlobKey::BuildLog(id.clone());
        assert_eq!(build_log.path(), format!("snapshots/build-logs/{id}.json"));
        assert_eq!(build_log.posix_path(), build_log.path());
        assert!(vm_state
            .path()
            .starts_with(&format!("{}/", agentenv_domain::snapshot_dir(&id))));
    }
}
