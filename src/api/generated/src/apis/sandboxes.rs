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
pub enum SandboxesColdPostResponse {
    /// The sandbox was created successfully
    Status201_TheSandboxWasCreatedSuccessfully {
        body: models::Sandbox,
        x_agentenv_sandbox_id: Option<String>,
    },
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesGetResponse {
    /// Successfully returned all running sandboxes
    Status200_SuccessfullyReturnedAllRunningSandboxes(Vec<models::ListedSandbox>),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesPostResponse {
    /// The sandbox was created successfully
    Status201_TheSandboxWasCreatedSuccessfully {
        body: models::Sandbox,
        x_agentenv_sandbox_id: Option<String>,
    },
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdConnectPostResponse {
    /// The sandbox was already running
    Status200_TheSandboxWasAlreadyRunning(models::Sandbox),
    /// The sandbox was resumed successfully
    Status201_TheSandboxWasResumedSuccessfully(models::Sandbox),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdCustomExtensionParamsGetResponse {
    /// The current custom extension params
    Status200_TheCurrentCustomExtensionParams(
        std::collections::HashMap<String, crate::types::Object>,
    ),
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
pub enum SandboxesSandboxIdCustomExtensionParamsPatchResponse {
    /// The updated full custom extension params
    Status200_TheUpdatedFullCustomExtensionParams(
        std::collections::HashMap<String, crate::types::Object>,
    ),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdDeleteResponse {
    /// The sandbox was killed successfully
    Status204_TheSandboxWasKilledSuccessfully,
    /// Not found
    Status404_NotFound(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdForkPostResponse {
    /// The sandbox was snapshotted and the forks were attempted; each entry reports one fork's outcome
    Status201_TheSandboxWasSnapshottedAndTheForksWereAttempted(Vec<models::SandboxForkResult>),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdGetResponse {
    /// Successfully returned the sandbox
    Status200_SuccessfullyReturnedTheSandbox(models::SandboxDetail),
    /// Not found
    Status404_NotFound(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdMemoryGetResponse {
    /// Memory hotplug status
    Status200_MemoryHotplugStatus(models::SandboxMemoryHotplugStatus),
    /// Not found
    Status404_NotFound(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Unprocessable entity
    Status422_UnprocessableEntity(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdMemoryPatchResponse {
    /// Memory resize converged
    Status200_MemoryResizeConverged(models::SandboxMemoryHotplugStatus),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Unprocessable entity
    Status422_UnprocessableEntity(models::Error),
    /// Gateway timeout
    Status504_GatewayTimeout(models::SandboxMemoryHotplugStatus),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdNetworkPutResponse {
    /// Successfully updated the sandbox network configuration
    Status204_SuccessfullyUpdatedTheSandboxNetworkConfiguration,
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdPausePostResponse {
    /// The sandbox was paused successfully and can be resumed
    Status204_TheSandboxWasPausedSuccessfullyAndCanBeResumed,
    /// Conflict
    Status409_Conflict(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SandboxesSandboxIdRefreshesPostResponse {
    /// Successfully refreshed the sandbox
    Status204_SuccessfullyRefreshedTheSandbox,
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
pub enum SandboxesSandboxIdResumePostResponse {
    /// The sandbox was resumed successfully
    Status201_TheSandboxWasResumedSuccessfully(models::Sandbox),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
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
pub enum SandboxesSandboxIdSnapshotsPostResponse {
    /// Snapshot created successfully
    Status201_SnapshotCreatedSuccessfully(models::SnapshotInfo),
    /// Bad request
    Status400_BadRequest(models::Error),
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
pub enum SandboxesSandboxIdTimeoutPostResponse {
    /// Successfully set the sandbox timeout
    Status204_SuccessfullySetTheSandboxTimeout,
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
pub enum V2SandboxesGetResponse {
    /// Successfully returned all running sandboxes
    Status200_SuccessfullyReturnedAllRunningSandboxes {
        body: Vec<models::ListedSandbox>,
        x_next_token: Option<String>,
        x_total_running: Option<i32>,
    },
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

/// Sandboxes
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait Sandboxes<E: std::fmt::Debug + Send + Sync + 'static = ()>:
    super::ErrorHandler<E>
{
    type Claims;

    /// SandboxesColdPost - POST /sandboxes-cold
    async fn sandboxes_cold_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        body: &models::NewColdSandbox,
    ) -> Result<SandboxesColdPostResponse, E>;

    /// List running sandboxes.
    ///
    /// SandboxesGet - GET /sandboxes
    async fn sandboxes_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::SandboxesGetQueryParams,
    ) -> Result<SandboxesGetResponse, E>;

    /// Create sandbox.
    ///
    /// SandboxesPost - POST /sandboxes
    async fn sandboxes_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        body: &models::NewSandbox,
    ) -> Result<SandboxesPostResponse, E>;

    /// Connect sandbox.
    ///
    /// SandboxesSandboxIdConnectPost - POST /sandboxes/{sandboxID}/connect
    async fn sandboxes_sandbox_id_connect_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdConnectPostPathParams,
        body: &models::ConnectSandbox,
    ) -> Result<SandboxesSandboxIdConnectPostResponse, E>;

    /// SandboxesSandboxIdCustomExtensionParamsGet - GET /sandboxes/{sandboxID}/custom-extension-params
    async fn sandboxes_sandbox_id_custom_extension_params_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdCustomExtensionParamsGetPathParams,
    ) -> Result<SandboxesSandboxIdCustomExtensionParamsGetResponse, E>;

    /// SandboxesSandboxIdCustomExtensionParamsPatch - PATCH /sandboxes/{sandboxID}/custom-extension-params
    async fn sandboxes_sandbox_id_custom_extension_params_patch(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdCustomExtensionParamsPatchPathParams,
        body: &std::collections::HashMap<String, crate::types::Object>,
    ) -> Result<SandboxesSandboxIdCustomExtensionParamsPatchResponse, E>;

    /// Kill sandbox.
    ///
    /// SandboxesSandboxIdDelete - DELETE /sandboxes/{sandboxID}
    async fn sandboxes_sandbox_id_delete(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdDeletePathParams,
    ) -> Result<SandboxesSandboxIdDeleteResponse, E>;

    /// Fork sandbox.
    ///
    /// SandboxesSandboxIdForkPost - POST /sandboxes/{sandboxID}/fork
    async fn sandboxes_sandbox_id_fork_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdForkPostPathParams,
        body: &Option<models::SandboxForkRequest>,
    ) -> Result<SandboxesSandboxIdForkPostResponse, E>;

    /// Sandbox.
    ///
    /// SandboxesSandboxIdGet - GET /sandboxes/{sandboxID}
    async fn sandboxes_sandbox_id_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdGetPathParams,
    ) -> Result<SandboxesSandboxIdGetResponse, E>;

    /// Get sandbox virtio-mem status.
    ///
    /// SandboxesSandboxIdMemoryGet - GET /sandboxes/{sandboxID}/memory
    async fn sandboxes_sandbox_id_memory_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdMemoryGetPathParams,
    ) -> Result<SandboxesSandboxIdMemoryGetResponse, E>;

    /// Resize sandbox virtio-mem memory.
    ///
    /// SandboxesSandboxIdMemoryPatch - PATCH /sandboxes/{sandboxID}/memory
    async fn sandboxes_sandbox_id_memory_patch(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdMemoryPatchPathParams,
        body: &models::SandboxMemoryHotplugResize,
    ) -> Result<SandboxesSandboxIdMemoryPatchResponse, E>;

    /// Update sandbox network.
    ///
    /// SandboxesSandboxIdNetworkPut - PUT /sandboxes/{sandboxID}/network
    async fn sandboxes_sandbox_id_network_put(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdNetworkPutPathParams,
        body: &models::SandboxNetworkUpdateConfig,
    ) -> Result<SandboxesSandboxIdNetworkPutResponse, E>;

    /// Pause sandbox.
    ///
    /// SandboxesSandboxIdPausePost - POST /sandboxes/{sandboxID}/pause
    async fn sandboxes_sandbox_id_pause_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdPausePostPathParams,
    ) -> Result<SandboxesSandboxIdPausePostResponse, E>;

    /// Refresh sandbox.
    ///
    /// SandboxesSandboxIdRefreshesPost - POST /sandboxes/{sandboxID}/refreshes
    async fn sandboxes_sandbox_id_refreshes_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdRefreshesPostPathParams,
        body: &Option<models::SandboxRefreshRequest>,
    ) -> Result<SandboxesSandboxIdRefreshesPostResponse, E>;

    /// Resume sandbox.
    ///
    /// SandboxesSandboxIdResumePost - POST /sandboxes/{sandboxID}/resume
    async fn sandboxes_sandbox_id_resume_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdResumePostPathParams,
        body: &models::ResumedSandbox,
    ) -> Result<SandboxesSandboxIdResumePostResponse, E>;

    /// Create snapshot.
    ///
    /// SandboxesSandboxIdSnapshotsPost - POST /sandboxes/{sandboxID}/snapshots
    async fn sandboxes_sandbox_id_snapshots_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdSnapshotsPostPathParams,
        body: &models::SandboxSnapshotRequest,
    ) -> Result<SandboxesSandboxIdSnapshotsPostResponse, E>;

    /// Set sandbox timeout.
    ///
    /// SandboxesSandboxIdTimeoutPost - POST /sandboxes/{sandboxID}/timeout
    async fn sandboxes_sandbox_id_timeout_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdTimeoutPostPathParams,
        body: &Option<models::SandboxTimeoutRequest>,
    ) -> Result<SandboxesSandboxIdTimeoutPostResponse, E>;

    /// List sandboxes (v2).
    ///
    /// V2SandboxesGet - GET /v2/sandboxes
    async fn v2_sandboxes_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::V2SandboxesGetQueryParams,
    ) -> Result<V2SandboxesGetResponse, E>;
}
