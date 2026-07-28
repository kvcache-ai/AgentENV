//! Publish target resolution for snapshot image export.

use std::collections::BTreeSet;

use thiserror::Error;

use crate::snapshot::repository::backends::common::acr::SourceRegistryRepository;
use crate::snapshot::{
    rootfs_snapshot_image_tag, CommittedSnapshot, OverlaybdLayerRef, PersistedDiskImagePublication,
    SnapshotId,
};

const DEFAULT_TAG: &str = "latest";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotImageTarget {
    pub registry: String,
    pub repository: String,
    pub tag: String,
}

impl SnapshotImageTarget {
    pub fn parse(
        target_repository: &str,
        tag: Option<&str>,
    ) -> Result<Self, SnapshotImageTargetError> {
        if target_repository.contains("://") || target_repository.contains('@') {
            return Err(invalid_target(
                "target repository must not include a URL scheme or digest",
            ));
        }
        let (registry, repository) = target_repository.split_once('/').ok_or_else(|| {
            invalid_target("target repository must include a registry and repository path")
        })?;
        if registry.is_empty()
            || registry.chars().any(char::is_whitespace)
            || !valid_repository(repository)
        {
            return Err(invalid_target(
                "target repository contains an invalid registry or repository path",
            ));
        }
        let registry = registry.to_ascii_lowercase();
        let registry = registry
            .strip_suffix(":443")
            .filter(|host| !host.is_empty())
            .unwrap_or(&registry)
            .to_string();
        Ok(Self {
            registry,
            repository: repository.to_string(),
            tag: validated_tag(tag)?.to_string(),
        })
    }

    pub fn image_ref(&self) -> String {
        format!("{}/{}:{}", self.registry, self.repository, self.tag)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SnapshotImageTargetError {
    #[error("invalid snapshot image target: {reason}")]
    InvalidTarget { reason: String },
    #[error("cannot infer snapshot image target: {reason}; pass --target-repository")]
    CannotInfer { reason: String },
}

fn invalid_target(reason: impl Into<String>) -> SnapshotImageTargetError {
    SnapshotImageTargetError::InvalidTarget {
        reason: reason.into(),
    }
}

/// The canonical legacy rootfs publication: the `disk_publications` entry
/// carrying the snapshot's rootfs tag.
fn canonical_rootfs_publication<'a>(
    snapshot_id: &SnapshotId,
    committed: &'a CommittedSnapshot,
) -> Option<&'a PersistedDiskImagePublication> {
    let rootfs_tag = rootfs_snapshot_image_tag(snapshot_id);
    committed
        .disk_publications
        .iter()
        .find(|publication| publication.tag == rootfs_tag)
}

/// Resolves an explicit target, or defaults to the snapshot's original
/// repository under a standalone snapshot tag.
pub(crate) fn resolve_snapshot_image_target(
    snapshot_id: &SnapshotId,
    committed: &CommittedSnapshot,
    target_repository: Option<&str>,
    tag: Option<&str>,
) -> Result<SnapshotImageTarget, SnapshotImageTargetError> {
    if let Some(target_repository) = target_repository {
        return SnapshotImageTarget::parse(target_repository, tag);
    }

    let (registry, repository) =
        if let Some(publication) = canonical_rootfs_publication(snapshot_id, committed) {
            infer_repository_from_blob_urls([publication.repo_blob_url.as_str()])?
        } else {
            infer_repository_from_blob_urls(committed.rootfs_layers.iter().filter_map(|layer| {
                match layer {
                    OverlaybdLayerRef::External(layer) => Some(layer.repo_blob_url.as_str()),
                    OverlaybdLayerRef::Managed(_) => None,
                }
            }))?
        };
    let tag = match tag {
        Some(tag) => validated_tag(Some(tag))?.to_string(),
        None => format!("snapshot-{snapshot_id}"),
    };
    Ok(SnapshotImageTarget {
        registry,
        repository,
        tag,
    })
}

/// Returns the persisted snapshot-managed publication for an equivalent
/// normalized registry/repository/tag target.
pub(crate) fn snapshot_managed_publication_for_target<'a>(
    committed: &'a CommittedSnapshot,
    target: &SnapshotImageTarget,
) -> Option<&'a PersistedDiskImagePublication> {
    let image_ref = target.image_ref();
    committed.disk_publications.iter().find(|publication| {
        if publication.tag != target.tag {
            return false;
        }
        SourceRegistryRepository::parse(&publication.repo_blob_url)
            .map(|source| {
                source.registry == target.registry && source.repository == target.repository
            })
            .unwrap_or_else(|_| publication.image_ref == image_ref)
    })
}

fn infer_repository_from_blob_urls<'a>(
    repo_blob_urls: impl IntoIterator<Item = &'a str>,
) -> Result<(String, String), SnapshotImageTargetError> {
    let locations = repo_blob_urls
        .into_iter()
        .map(|url| {
            SourceRegistryRepository::parse(url)
                .map(|source| (source.registry, source.repository))
                .map_err(|error| SnapshotImageTargetError::CannotInfer {
                    reason: error.to_string(),
                })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if locations.len() != 1 {
        let reason = if locations.is_empty() {
            "snapshot rootfs has no external registry source"
        } else {
            "snapshot rootfs has multiple external registry sources"
        };
        return Err(SnapshotImageTargetError::CannotInfer {
            reason: reason.to_string(),
        });
    }
    Ok(locations.into_iter().next().expect("checked one source"))
}

/// Non-empty `/`-separated lowercase components; the registry stays the final
/// authority on repository names.
fn valid_repository(repository: &str) -> bool {
    !repository.is_empty() && repository.split('/').all(valid_component)
}

fn valid_component(component: &str) -> bool {
    let mut bytes = component.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_lowercase() || first.is_ascii_digit())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn validated_tag(tag: Option<&str>) -> Result<&str, SnapshotImageTargetError> {
    let tag = tag.unwrap_or(DEFAULT_TAG);
    let mut chars = tag.chars();
    let valid = !tag.is_empty()
        && tag.len() <= 128
        && matches!(chars.next(), Some(first) if first.is_ascii_alphanumeric() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-'));
    if valid {
        Ok(tag)
    } else {
        Err(invalid_target(format!("invalid OCI image tag '{tag}'")))
    }
}
