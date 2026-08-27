use crate::snapshot::SnapshotId;

/// Committed object layout for the OSS snapshot backend.
pub(crate) struct OssSnapshotArtifactLayout<'a> {
    snapshot_id: &'a SnapshotId,
}

impl<'a> OssSnapshotArtifactLayout<'a> {
    pub(super) fn new(snapshot_id: &'a SnapshotId) -> Self {
        Self { snapshot_id }
    }

    pub(super) fn alias_key(alias: &str) -> String {
        format!("catalog/aliases/{alias}.json")
    }

    pub(super) fn record_key(id: &SnapshotId) -> String {
        format!("catalog/records/{id}.json")
    }

    pub(super) fn volume_record_key(volume_id: &str) -> String {
        format!("catalog/volumes/{volume_id}.json")
    }

    pub(super) fn volume_records_prefix() -> &'static str {
        "catalog/volumes/"
    }

    pub(crate) fn managed_layer_key(digest: &str) -> String {
        format!("managed-layers/{digest}")
    }

    pub(super) fn artifact_prefix(&self) -> String {
        format!("artifacts/{}/", self.snapshot_id)
    }

    pub(super) fn artifact_key(&self, relative_path: &str) -> String {
        format!("{}{}", self.artifact_prefix(), relative_path)
    }
}
