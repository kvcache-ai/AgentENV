//! Read-only pre-SQL repository catalog access and legacy record decoders used
//! exclusively by offline migration (`aenv-snapshot import`).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agentenv_domain::{BuildErrorReason, ImageTag, SnapshotAlias, SnapshotId, SnapshotRecord};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tokio::task;

use crate::cfg::{AppConfig, SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind};
use crate::repository::backends::{oss::OssClient, OssRepository, PosixFsRepository};
use crate::repository::{Repository, RepositoryError, RepositoryResult};

const VOLUME_RECORDS_PREFIX: &str = "volumes/records/";
const IMAGES_PREFIX: &str = "images/descriptions/";
const IMAGE_TAGS_PREFIX: &str = "images/tags/";
const IMAGE_BUILDS_PREFIX: &str = "images/builds/";
const SNAPSHOT_RECORDS_PREFIX: &str = "catalog/records/";

/// Pre-SQL document keys scanned strictly during offline migration.
#[derive(Clone, Debug)]
pub(crate) enum LegacyDocumentKey {
    /// Legacy shared build-cache head and retirement state.
    BuildCacheState,
    /// Legacy volume record, keyed by volume ID.
    VolumeRecord(String),
    /// Legacy volume name pointer.
    VolumeAlias(String),
    /// Legacy snapshot or template record.
    SnapshotRecord(SnapshotId),
    /// Legacy snapshot alias pointer.
    SnapshotAlias(SnapshotAlias),
    /// Legacy published image description, named by its validated digest.
    Image(ImageDigest),
    /// Legacy image tag pointer.
    ImageTag(ImageTag),
    /// Legacy imageOnly build record.
    ImageBuild(SnapshotId),
}

#[derive(Clone, Debug)]
pub(crate) struct ImageDigest(String);

impl LegacyDocumentKey {
    pub(crate) fn image(digest: &str) -> RepositoryResult<Self> {
        if !crate::digest::is_sha256_digest(digest) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("invalid image digest '{digest}'"),
            });
        }
        Ok(Self::Image(ImageDigest(digest.to_owned())))
    }

    pub(crate) fn path(&self) -> String {
        match self {
            Self::BuildCacheState => "template-build/cache-head.json".to_owned(),
            Self::VolumeRecord(id) => format!("{VOLUME_RECORDS_PREFIX}{id}.json"),
            Self::VolumeAlias(name) => format!("volumes/aliases/{name}.json"),
            Self::SnapshotRecord(id) => format!("{SNAPSHOT_RECORDS_PREFIX}{id}.json"),
            Self::SnapshotAlias(alias) => format!("catalog/aliases/{alias}.json"),
            Self::Image(digest) => format!("{IMAGES_PREFIX}{}.json", digest.0),
            Self::ImageTag(tag) => format!("{IMAGE_TAGS_PREFIX}{}/{}.json", tag.name(), tag.tag()),
            Self::ImageBuild(id) => format!("{IMAGE_BUILDS_PREFIX}{id}.json"),
        }
    }

    /// The path in a pre-SQL POSIX repository, where aliases omit `.json`.
    pub(crate) fn posix_path(&self) -> String {
        match self {
            Self::VolumeAlias(name) => format!("volumes/aliases/{name}"),
            Self::SnapshotAlias(alias) => format!("catalog/aliases/{alias}"),
            _ => self.path(),
        }
    }
}

/// Legacy pre-SQL document collections scanned during offline migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LegacyDocumentCollection {
    VolumeRecords,
    SnapshotRecords,
    Images,
    ImageTags,
    ImageBuilds,
}

impl LegacyDocumentCollection {
    pub(crate) fn prefix(self) -> &'static str {
        match self {
            Self::VolumeRecords => VOLUME_RECORDS_PREFIX,
            Self::SnapshotRecords => SNAPSHOT_RECORDS_PREFIX,
            Self::Images => IMAGES_PREFIX,
            Self::ImageTags => IMAGE_TAGS_PREFIX,
            Self::ImageBuilds => IMAGE_BUILDS_PREFIX,
        }
    }

    pub(crate) fn key(self, id: &str) -> Option<LegacyDocumentKey> {
        match self {
            Self::VolumeRecords => Some(LegacyDocumentKey::VolumeRecord(id.to_owned())),
            Self::SnapshotRecords => SnapshotId::parse(id)
                .ok()
                .map(LegacyDocumentKey::SnapshotRecord),
            Self::Images => LegacyDocumentKey::image(id).ok(),
            Self::ImageTags => ImageTag::parse(id).ok().map(LegacyDocumentKey::ImageTag),
            Self::ImageBuilds => SnapshotId::parse(id)
                .ok()
                .map(LegacyDocumentKey::ImageBuild),
        }
    }

    pub(crate) fn id_from_relative_path(self, relative: &str) -> Option<String> {
        match self {
            Self::VolumeRecords | Self::SnapshotRecords | Self::Images | Self::ImageBuilds => {
                (!relative.contains('/'))
                    .then(|| relative.strip_suffix(".json"))
                    .flatten()
                    .filter(|id| self.key(id).is_some())
                    .map(str::to_owned)
            }
            Self::ImageTags => {
                let trimmed = relative.strip_suffix(".json")?;
                let (name, tag) = trimmed.rsplit_once('/')?;
                let parsed = ImageTag::parse(&format!("{name}:{tag}")).ok()?;
                Some(parsed.to_string())
            }
        }
    }
}

/// Read-only access to legacy pre-SQL catalog documents. Kept strictly inside
/// the offline migration module so runtime `Repository` stays blob-only.
#[async_trait]
pub(crate) trait LegacyCatalog: Send + Sync {
    async fn get_document(&self, key: LegacyDocumentKey) -> RepositoryResult<Option<Vec<u8>>>;

    async fn list_documents(
        &self,
        collection: LegacyDocumentCollection,
    ) -> RepositoryResult<Vec<String>>;
}

pub(crate) struct PosixLegacyCatalog {
    root: Arc<PathBuf>,
}

impl PosixLegacyCatalog {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root: Arc::new(root),
        }
    }

    async fn run_blocking<T, F>(&self, operation: &'static str, work: F) -> RepositoryResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Path) -> RepositoryResult<T> + Send + 'static,
    {
        let root = Arc::clone(&self.root);
        task::spawn_blocking(move || work(&root))
            .await
            .map_err(|error| RepositoryError::Backend {
                message: format!("legacy catalog task panicked while trying to {operation}"),
                source: Some(anyhow::Error::from(error)),
            })?
    }
}

#[async_trait]
impl LegacyCatalog for PosixLegacyCatalog {
    async fn get_document(&self, key: LegacyDocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        self.run_blocking("read legacy document", move |root| {
            let path = root.join(key.posix_path());
            match fs::read(&path) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(RepositoryError::backend(
                    format!("read '{}'", path.display()),
                    error,
                )),
            }
        })
        .await
    }

    async fn list_documents(
        &self,
        collection: LegacyDocumentCollection,
    ) -> RepositoryResult<Vec<String>> {
        self.run_blocking("list legacy documents", move |root| {
            list_posix_legacy(root, collection)
        })
        .await
    }
}

fn list_posix_legacy(
    root: &Path,
    collection: LegacyDocumentCollection,
) -> RepositoryResult<Vec<String>> {
    let mut entries = Vec::new();
    collect_posix_legacy_files(
        &root.join(collection.prefix()),
        "",
        collection,
        &mut entries,
    )?;
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    Ok(entries.into_iter().map(|(_, id)| id).collect())
}

fn collect_posix_legacy_files(
    directory: &Path,
    prefix_rel: &str,
    collection: LegacyDocumentCollection,
    out: &mut Vec<(String, String)>,
) -> RepositoryResult<()> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(RepositoryError::backend(
                format!("read '{}'", directory.display()),
                error,
            ))
        }
    };
    let nested = matches!(collection, LegacyDocumentCollection::ImageTags);
    for entry in entries {
        let entry = entry.map_err(|error| {
            RepositoryError::backend(format!("read '{}'", directory.display()), error)
        })?;
        let file_type = entry.file_type().map_err(|error| {
            RepositoryError::backend(format!("inspect '{}'", entry.path().display()), error)
        })?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if file_type.is_dir() && nested {
            collect_posix_legacy_files(
                &entry.path(),
                &format!("{prefix_rel}{name}/"),
                collection,
                out,
            )?;
        } else if file_type.is_file() {
            let relative = format!("{prefix_rel}{name}");
            if let Some(id) = collection.id_from_relative_path(&relative) {
                out.push((relative, id));
            }
        }
    }
    Ok(())
}

const OSS_LIST_PAGE_SIZE: usize = 256;

pub(crate) struct OssLegacyCatalog {
    client: Arc<OssClient>,
}

impl OssLegacyCatalog {
    pub(crate) fn new(client: Arc<OssClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl LegacyCatalog for OssLegacyCatalog {
    async fn get_document(&self, key: LegacyDocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        let path = key.path();
        match self.client.get_bytes(&path).await {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(error) if OssClient::is_not_found_error(&error) => Ok(None),
            Err(error) => Err(RepositoryError::backend(format!("read '{path}'"), error)),
        }
    }

    async fn list_documents(
        &self,
        collection: LegacyDocumentCollection,
    ) -> RepositoryResult<Vec<String>> {
        let prefix = collection.prefix();
        let mut ids = Vec::new();
        let mut start_after: Option<String> = None;
        loop {
            let keys = self
                .client
                .list_keys_page(prefix, start_after.as_deref(), OSS_LIST_PAGE_SIZE)
                .await
                .map_err(|error| RepositoryError::backend(format!("list '{prefix}'"), error))?;
            let full = keys.len() == OSS_LIST_PAGE_SIZE;
            start_after = keys.last().cloned();
            ids.extend(keys.iter().filter_map(|key| {
                key.strip_prefix(prefix)
                    .and_then(|relative| collection.id_from_relative_path(relative))
            }));
            if !full {
                break;
            }
        }
        Ok(ids)
    }
}

pub(crate) struct LegacyImportSources {
    pub(crate) repo: Arc<dyn Repository>,
    pub(crate) catalog: Box<dyn LegacyCatalog>,
}

/// Opens the configured repository plus its read-only pre-SQL legacy catalog.
pub(crate) fn open_legacy_sources_from_config(config: &AppConfig) -> Result<LegacyImportSources> {
    match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let posix = config.backend.posix_fs.as_ref().context(
                "backend.posix_fs config is required when repository_backend = posix_fs",
            )?;
            let main_root = posix.snapshot_store.join("repository");
            Ok(LegacyImportSources {
                repo: Arc::new(PosixFsRepository::new(main_root.clone())),
                catalog: Box::new(PosixLegacyCatalog::new(main_root)),
            })
        }
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .backend
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            let snapshot_image_storage = if config.snapshot.image_publish.enabled {
                SnapshotImageStoragePolicy::SourceRegistry
            } else {
                SnapshotImageStoragePolicy::ObjectStorage
            };
            let main_client = Arc::new(OssClient::from_config(oss_config)?);
            let main_repo = Arc::new(OssRepository::new(
                Arc::clone(&main_client),
                snapshot_image_storage,
                &config.snapshot.publish_compression,
            ));
            Ok(LegacyImportSources {
                repo: main_repo,
                catalog: Box::new(OssLegacyCatalog::new(main_client)),
            })
        }
    }
}

/// Decodes a pre-SQL snapshot record JSON (legacy flat `source`/`resources`/`committed`,
/// nested `metadata`, or `{"Template": {"build": ...}}`) into [`SnapshotRecord`].
pub(crate) fn decode_legacy_snapshot_record(
    mut value: Value,
) -> serde_json::Result<SnapshotRecord> {
    if let Some(record) = value
        .as_object_mut()
        .filter(|record| !record.contains_key("id"))
    {
        if let Some(Value::Object(mut metadata)) = record.remove("metadata") {
            for field in [
                "id",
                "snapshot_type",
                "owner_node_id",
                "alias",
                "created_at_unix_ms",
                "updated_at_unix_ms",
            ] {
                if let Some(v) = metadata.remove(field) {
                    record.insert(field.into(), v);
                }
            }
            if !record.contains_key("snapshot_type") {
                if let Some(placement) = metadata.remove("placement").filter(|v| !v.is_null()) {
                    record.insert("snapshot_type".into(), placement);
                }
            }
        }
    }
    if let Some(record) = value
        .as_object_mut()
        .filter(|record| !record.contains_key("config"))
    {
        let mut config = serde_json::Map::new();
        for key in ["source", "resources"] {
            if let Some(v) = record.remove(key) {
                config.insert(key.into(), v);
            }
        }
        if let Some(committed) = record.get_mut("committed").and_then(Value::as_object_mut) {
            for key in [
                "context",
                "startup",
                "runtime_versions",
                "virtualization_mode",
                "image_configs",
                "volume_snapshots",
                "custom_extension_params",
            ] {
                if let Some(v) = committed.remove(key) {
                    config.insert(key.into(), v);
                }
            }
        }
        if record.get("committed").is_none_or(Value::is_null)
            && config
                .get("source")
                .and_then(|source| source.get("Template"))
                .is_some()
        {
            config.insert(
                "context".into(),
                serde_json::to_value(agentenv_domain::CommandContext::default())?,
            );
            config.insert(
                "runtime_versions".into(),
                serde_json::to_value(agentenv_domain::SnapshotRuntimeVersions::default())?,
            );
        }
        if record.get("snapshot_type").and_then(Value::as_str) == Some("local") {
            record.insert("committed".into(), Value::Null);
        }
        record.insert("config".into(), Value::Object(config));
    }
    if let Some(record) = value.as_object_mut() {
        if let Some(config) = record.get_mut("config").and_then(Value::as_object_mut) {
            if config
                .get("source")
                .and_then(Value::as_object)
                .is_some_and(|map| map.len() == 1 && map.contains_key("Template"))
            {
                config.insert("source".into(), Value::String("Template".into()));
            }
        }
        if !record.contains_key("manifest") {
            let committed = record.remove("committed").unwrap_or(Value::Null);
            record.insert("manifest".into(), committed);
        }
    }
    serde_json::from_value(value)
}

/// Pre-SQL `images/builds/<id>.json` record format for `imageOnly` builds.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LegacyImageBuildRecord {
    pub(crate) name: String,
    pub(crate) tags: Vec<String>,
    pub(crate) build: LegacyBuildInfo,
    #[serde(default)]
    pub(crate) image_digest: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct LegacyBuildInfo {
    pub(crate) status: LegacyBuildStatus,
    #[serde(default)]
    pub(crate) started_at_unix_ms: Option<i64>,
    #[serde(default)]
    pub(crate) error_reason: Option<LegacyBuildErrorReason>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
pub(crate) enum LegacyBuildStatus {
    #[serde(alias = "waiting")]
    Waiting,
    #[serde(alias = "building")]
    Building,
    #[serde(alias = "ready")]
    Ready,
    #[serde(alias = "error")]
    Error,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum LegacyBuildErrorReason {
    Structured {
        message: String,
        #[serde(default)]
        step: Option<String>,
    },
    Message(String),
}

impl From<LegacyBuildErrorReason> for BuildErrorReason {
    fn from(value: LegacyBuildErrorReason) -> Self {
        match value {
            LegacyBuildErrorReason::Structured { message, step } => Self { message, step },
            LegacyBuildErrorReason::Message(message) => Self::new(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_document_paths_match_pre_sql_repositories() {
        let record = LegacyDocumentKey::VolumeRecord("vol_test".to_owned());
        assert_eq!(record.path(), "volumes/records/vol_test.json");
        assert_eq!(record.posix_path(), "volumes/records/vol_test.json");

        let alias = LegacyDocumentKey::VolumeAlias("test-data".to_owned());
        assert_eq!(alias.path(), "volumes/aliases/test-data.json");
        assert_eq!(alias.posix_path(), "volumes/aliases/test-data");

        let snapshot_id = SnapshotId::generate();
        let snapshot = LegacyDocumentKey::SnapshotRecord(snapshot_id.clone());
        assert_eq!(
            snapshot.path(),
            format!("catalog/records/{snapshot_id}.json")
        );
        assert_eq!(snapshot.posix_path(), snapshot.path());

        let snapshot_alias =
            LegacyDocumentKey::SnapshotAlias(SnapshotAlias::parse("base-image").unwrap());
        assert_eq!(snapshot_alias.path(), "catalog/aliases/base-image.json");
        assert_eq!(snapshot_alias.posix_path(), "catalog/aliases/base-image");

        let cache_state = LegacyDocumentKey::BuildCacheState;
        assert_eq!(cache_state.path(), "template-build/cache-head.json");

        let build_id = SnapshotId::generate();
        let image_build = LegacyDocumentKey::ImageBuild(build_id.clone());
        assert_eq!(image_build.path(), format!("images/builds/{build_id}.json"));
        assert_eq!(
            LegacyDocumentCollection::ImageBuilds
                .id_from_relative_path(&format!("{build_id}.json")),
            Some(build_id.to_string())
        );
        assert_eq!(
            LegacyDocumentCollection::ImageBuilds
                .id_from_relative_path(&format!("{build_id}.lock")),
            None
        );

        let digest = crate::digest::sha256_digest(b"image");
        let image = LegacyDocumentKey::image(&digest).unwrap();
        assert_eq!(image.path(), format!("images/descriptions/{digest}.json"));
        assert!(LegacyDocumentKey::image("sha256:../../outside").is_err());

        let tag = LegacyDocumentKey::ImageTag(ImageTag::parse("web").unwrap());
        assert_eq!(tag.path(), "images/tags/web/latest.json");
        assert_eq!(
            LegacyDocumentCollection::ImageTags
                .id_from_relative_path("docker.io/library/python/3.12.json"),
            Some("docker.io/library/python:3.12".to_owned())
        );
        assert_eq!(
            LegacyDocumentCollection::ImageTags.id_from_relative_path("stray.json"),
            None
        );
    }
}
