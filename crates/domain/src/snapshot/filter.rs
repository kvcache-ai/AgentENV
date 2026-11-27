use super::{SnapshotId, SnapshotPlacement, SnapshotSourceKind};

/// Snapshot record list filter.
///
/// When multiple fields are present they combine with AND semantics.
/// When `sources` is `None`, queries restrict to public snapshots (excluding
/// internal [`SnapshotSourceKind::Builder`] entries).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotListFilter {
    /// Match an exact snapshot alias (never falls back to snapshot ID lookup).
    #[serde(default)]
    pub alias: Option<String>,
    /// Match aliases that start with this prefix.
    pub alias_prefix: Option<String>,
    /// Restrict results to this exact set of snapshot ids.
    pub snapshot_ids: Option<Vec<SnapshotId>>,
    /// Restrict results to a single snapshot id or exact alias.
    pub snapshot_id_or_alias: Option<String>,
    /// Restrict results to snapshots captured from this source sandbox id.
    ///
    /// This only matches records whose source is [`SnapshotSourceKind::Sandbox`].
    pub source_sandbox_id: Option<String>,
    /// Restrict results to records with these source kinds.
    pub sources: Option<Vec<SnapshotSourceKind>>,
    /// Restrict results to a specific placement (`local` or `distributed`).
    #[serde(default)]
    pub placement: Option<SnapshotPlacement>,
    /// Restrict results to records owned by this node ID.
    #[serde(default)]
    pub owner_node_id: Option<String>,
    /// Optional maximum number of rows returned.
    #[serde(default)]
    pub limit: Option<u32>,
}

impl SnapshotListFilter {
    pub fn by_ids<I>(snapshot_ids: I) -> Self
    where
        I: IntoIterator<Item = SnapshotId>,
    {
        Self {
            snapshot_ids: Some(snapshot_ids.into_iter().collect()),
            ..Self::default()
        }
    }

    pub fn templates() -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Template]),
            ..Self::default()
        }
    }

    pub fn sandbox_snapshots(
        source_sandbox_id: Option<String>,
        snapshot_id_or_alias: Option<String>,
    ) -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Sandbox]),
            source_sandbox_id,
            snapshot_id_or_alias: snapshot_id_or_alias.map(|value| {
                let unqualified = value
                    .rsplit_once('/')
                    .map_or(value.as_str(), |(_, name)| name);
                unqualified
                    .split_once(':')
                    .map_or(unqualified, |(name, _)| name)
                    .to_string()
            }),
            ..Self::default()
        }
    }
}
