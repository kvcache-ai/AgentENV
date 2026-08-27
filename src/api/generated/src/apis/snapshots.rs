use async_trait::async_trait;
use axum::extract::*;
use axum_extra::extract::CookieJar;
use bytes::Bytes;
use headers::Host;
use http::Method;
use serde::{Deserialize, Serialize};

use crate::{models, types::*};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SnapshotsGetResponse {
    /// Successfully returned snapshots
    Status200_SuccessfullyReturnedSnapshots {
        body: Vec<models::SnapshotInfo>,
        x_next_token: Option<String>,
    },
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SnapshotsSnapshotIdDeleteResponse {
    /// The snapshot was deleted or was already absent
    Status204_TheSnapshotWasDeletedOrWasAlreadyAbsent,
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SnapshotsSnapshotIdGetResponse {
    /// Successfully returned the snapshot
    Status200_SuccessfullyReturnedTheSnapshot(models::SnapshotInfo),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

/// Snapshots
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait Snapshots<E: std::fmt::Debug + Send + Sync + 'static = ()>:
    super::ErrorHandler<E>
{
    type Claims;

    /// List snapshots.
    ///
    /// SnapshotsGet - GET /snapshots
    async fn snapshots_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::SnapshotsGetQueryParams,
    ) -> Result<SnapshotsGetResponse, E>;

    /// Delete snapshot.
    ///
    /// SnapshotsSnapshotIdDelete - DELETE /snapshots/{snapshotID}
    async fn snapshots_snapshot_id_delete(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SnapshotsSnapshotIdDeletePathParams,
    ) -> Result<SnapshotsSnapshotIdDeleteResponse, E>;

    /// SnapshotsSnapshotIdGet - GET /snapshots/{snapshotID}
    async fn snapshots_snapshot_id_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SnapshotsSnapshotIdGetPathParams,
    ) -> Result<SnapshotsSnapshotIdGetResponse, E>;
}
