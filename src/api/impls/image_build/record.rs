//! Shared-repository records of imageOnly builds. Like template records,
//! they let any node report a build after its worker has gone.

use serde::{Deserialize, Serialize};

use crate::repository::{
    DocumentChange, DocumentKey, Repository, RepositoryError, RepositoryResult,
};
use crate::snapshot::{
    SnapshotId, TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
use crate::types::{now_unix_ms, ImageName, ImageTag};

/// One imageOnly build: the image name and tags it publishes, and its status.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImageBuildRecord {
    pub(crate) name: String,
    pub(crate) tags: Vec<String>,
    pub(crate) build: TemplateBuildInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) image_digest: Option<String>,
    pub(crate) created_at_unix_ms: i64,
}

impl ImageBuildRecord {
    pub(crate) fn waiting(name: &ImageName, tags: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            tags,
            build: TemplateBuildInfo::waiting(),
            image_digest: None,
            created_at_unix_ms: now_unix_ms(),
        }
    }

    /// The tags that point at the image once the build publishes it.
    pub(crate) fn image_tags(&self) -> anyhow::Result<Vec<ImageTag>> {
        let name = ImageName::parse(&self.name)?;
        self.tags
            .iter()
            .map(|tag| ImageTag::new(&name, tag))
            .collect()
    }
}

pub(crate) async fn create<R>(
    repository: &R,
    id: &SnapshotId,
    record: &ImageBuildRecord,
) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    repository
        .put_document(DocumentKey::ImageBuild(id.clone()), encode(record)?)
        .await
}

pub(crate) async fn get<R>(
    repository: &R,
    id: &SnapshotId,
) -> RepositoryResult<Option<ImageBuildRecord>>
where
    R: Repository + ?Sized,
{
    repository
        .get_document(DocumentKey::ImageBuild(id.clone()))
        .await?
        .map(|bytes| decode(id, &bytes))
        .transpose()
}

/// Atomically moves a waiting build to building, which also excludes starts
/// on other nodes.
pub(crate) async fn start<R>(repository: &R, id: &SnapshotId) -> RepositoryResult<ImageBuildRecord>
where
    R: Repository + ?Sized,
{
    let now = now_unix_ms();
    update(repository, id, move |id, record| {
        let mut record = record.ok_or_else(|| RepositoryError::SnapshotNotFound {
            lookup: id.to_string(),
        })?;
        if record.build.status != TemplateBuildStatus::Waiting {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("image build '{id}' has already started"),
            });
        }
        record.build.status = TemplateBuildStatus::Building;
        record.build.started_at_unix_ms = Some(now);
        Ok((Some(record.clone()), record))
    })
    .await
}

/// Marks a running build ready with the image it published.
pub(crate) async fn finish<R>(repository: &R, id: &SnapshotId, digest: &str) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    let now = now_unix_ms();
    let digest = digest.to_owned();
    update(repository, id, move |id, record| {
        let mut record = record.ok_or_else(|| RepositoryError::SnapshotNotFound {
            lookup: id.to_string(),
        })?;
        if !is_running(&record) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("image build '{id}' is no longer running"),
            });
        }
        record.build.status = TemplateBuildStatus::Ready;
        record.build.finished_at_unix_ms = Some(now);
        record.image_digest = Some(digest.clone());
        Ok((Some(record), ()))
    })
    .await
}

/// Marks a waiting or running build failed. Finished and deleted builds stay
/// as they are.
pub(crate) async fn fail<R>(
    repository: &R,
    id: &SnapshotId,
    reason: TemplateBuildErrorReason,
) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    let now = now_unix_ms();
    update(repository, id, move |_, record| {
        let Some(mut record) = record.filter(is_running) else {
            return Ok((None, ()));
        };
        record.build.status = TemplateBuildStatus::Error;
        record.build.finished_at_unix_ms = Some(now);
        record.build.error_reason = Some(reason.clone());
        Ok((Some(record), ()))
    })
    .await
}

/// Removes the record and the build logs. The published image and its tags
/// stay.
pub(crate) async fn delete<R>(repository: &R, id: &SnapshotId) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    repository
        .delete_document(DocumentKey::ImageBuild(id.clone()))
        .await?;
    repository
        .delete_document(DocumentKey::BuildLog(id.clone()))
        .await
}

fn is_running(record: &ImageBuildRecord) -> bool {
    matches!(
        record.build.status,
        TemplateBuildStatus::Waiting | TemplateBuildStatus::Building
    )
}

/// Atomically applies `decide` to the record and writes the record it
/// returns, if any. `decide` must be pure: it runs again on the state the
/// update was applied to, to report the outcome.
async fn update<R, T>(
    repository: &R,
    id: &SnapshotId,
    decide: impl Fn(&SnapshotId, Option<ImageBuildRecord>) -> RepositoryResult<(Option<ImageBuildRecord>, T)>
        + Clone
        + Send
        + Sync
        + 'static,
) -> RepositoryResult<T>
where
    R: Repository + ?Sized,
{
    let update = decide.clone();
    let update_id = id.clone();
    let applied_to = repository
        .update_document(
            DocumentKey::ImageBuild(id.clone()),
            Box::new(move |bytes| {
                let record = bytes.map(|bytes| decode(&update_id, bytes)).transpose()?;
                Ok(match update(&update_id, record)?.0 {
                    Some(next) => DocumentChange::Put(encode(&next)?),
                    None => DocumentChange::Keep,
                })
            }),
        )
        .await?;
    let record = applied_to.map(|bytes| decode(id, &bytes)).transpose()?;
    Ok(decide(id, record)?.1)
}

fn encode(record: &ImageBuildRecord) -> RepositoryResult<Vec<u8>> {
    serde_json::to_vec(record)
        .map_err(|error| RepositoryError::backend("encode image build", error))
}

fn decode(id: &SnapshotId, bytes: &[u8]) -> RepositoryResult<ImageBuildRecord> {
    serde_json::from_slice(bytes)
        .map_err(|error| RepositoryError::backend(format!("decode image build '{id}'"), error))
}
