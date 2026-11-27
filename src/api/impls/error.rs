use agentenv_http_server::models;
use agentenv_metadata::MetaError;
use anyhow::Error as AnyhowError;
use axum::{
    response::{IntoResponse, Response},
    Json,
};
use http::StatusCode;

use crate::orchestrator::OrchestratorError;
use crate::repository::RepositoryError;
use crate::sandbox::custom_extension::HookStatusError;
use crate::template::BuildServiceError;
use crate::volume::VolumeError;

/// Unified HTTP-layer API error carrying a typed status class so every handler
/// returns consistent HTTP status codes and JSON `models::Error` bodies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ApiError {
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    TooManyRequests(String),
    BadGateway(String),
    ServiceUnavailable(String),
    GatewayTimeout(String),
    Internal(String),
}

impl ApiError {
    pub(super) fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub(super) fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub(super) fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict(message.into())
    }

    pub(super) fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    pub(super) fn internal_from(err: &dyn std::error::Error) -> Self {
        let mut message = err.to_string();
        let mut current = err.source();
        while let Some(source) = current {
            let cause = source.to_string();
            if cause != message {
                message.push_str(": ");
                message.push_str(&cause);
            }
            current = source.source();
        }
        Self::Internal(message)
    }

    pub(super) fn status_code(&self) -> i32 {
        match self {
            Self::BadRequest(_) => 400,
            Self::NotFound(_) => 404,
            Self::Conflict(_) => 409,
            Self::TooManyRequests(_) => 429,
            Self::BadGateway(_) => 502,
            Self::ServiceUnavailable(_) => 503,
            Self::GatewayTimeout(_) => 504,
            Self::Internal(_) => 500,
        }
    }

    pub(super) fn http_status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::TooManyRequests(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::BadGateway(_) => StatusCode::BAD_GATEWAY,
            Self::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::GatewayTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub(super) fn message(&self) -> &str {
        match self {
            Self::BadRequest(msg)
            | Self::NotFound(msg)
            | Self::Conflict(msg)
            | Self::TooManyRequests(msg)
            | Self::BadGateway(msg)
            | Self::ServiceUnavailable(msg)
            | Self::GatewayTimeout(msg)
            | Self::Internal(msg) => msg,
        }
    }

    pub(super) fn into_message(self) -> String {
        match self {
            Self::BadRequest(msg)
            | Self::NotFound(msg)
            | Self::Conflict(msg)
            | Self::TooManyRequests(msg)
            | Self::BadGateway(msg)
            | Self::ServiceUnavailable(msg)
            | Self::GatewayTimeout(msg)
            | Self::Internal(msg) => msg,
        }
    }

    pub(super) fn into_model(self) -> models::Error {
        let code = self.status_code();
        models::Error::new(code, self.into_message())
    }

    /// Converts into a `models::Error` whose JSON `code` matches the exact HTTP
    /// status variant chosen by an OpenAPI response enum.
    pub(super) fn into_model_with_code(self, code: i32) -> models::Error {
        models::Error::new(code, self.into_message())
    }

    pub(super) fn into_server_error_model(self) -> models::Error {
        self.into_model_with_code(500)
    }

    pub(super) fn into_bad_request_model(self) -> models::Error {
        self.into_model_with_code(400)
    }

    pub(super) fn into_response(self) -> Response {
        let status = self.http_status();
        (status, Json(self.into_model())).into_response()
    }

    pub(crate) fn find_repository_error(err: &AnyhowError) -> Option<&RepositoryError> {
        err.chain()
            .find_map(|cause| cause.downcast_ref::<RepositoryError>())
    }

    pub(super) fn from_reusable_repository(err: &RepositoryError) -> Self {
        match err {
            RepositoryError::Backend { .. } | RepositoryError::Unavailable { .. } => {
                Self::ServiceUnavailable(err.to_string())
            }
            _ => Self::from(err),
        }
    }

    pub(super) fn from_reusable_snapshot_manager(err: &AnyhowError) -> Self {
        match Self::find_repository_error(err) {
            Some(error) => Self::from_reusable_repository(error),
            None => Self::internal_from(err.as_ref()),
        }
    }

    pub(super) fn from_snapshot_manager(err: &AnyhowError) -> Self {
        match Self::find_repository_error(err) {
            Some(error) => Self::from(error),
            None => Self::internal_from(err.as_ref()),
        }
    }

    pub(super) fn from_repository_build(err: &RepositoryError) -> Option<Self> {
        match err {
            RepositoryError::InvalidRequest { .. }
            | RepositoryError::AliasConflict { .. }
            | RepositoryError::IntegrityMismatch { .. } => Some(Self::BadRequest(err.to_string())),
            _ => None,
        }
    }

    pub(super) fn client_or_server_response<R>(
        self,
        bad_request: impl FnOnce(models::Error) -> R,
        server_error: impl FnOnce(models::Error) -> R,
    ) -> R {
        if matches!(self, Self::BadRequest(_)) {
            bad_request(self.into_bad_request_model())
        } else {
            server_error(self.into_server_error_model())
        }
    }
}

impl From<ApiError> for models::Error {
    fn from(error: ApiError) -> Self {
        error.into_model()
    }
}

impl From<BuildServiceError> for ApiError {
    fn from(error: BuildServiceError) -> Self {
        match error {
            BuildServiceError::InvalidInput(msg) => Self::BadRequest(msg),
            BuildServiceError::NotFound(msg) => Self::NotFound(msg),
            BuildServiceError::Conflict(msg) => Self::Conflict(msg),
            BuildServiceError::TooManyRequests(msg) => Self::TooManyRequests(msg),
            BuildServiceError::UpstreamFailed(msg) => Self::BadGateway(msg),
            BuildServiceError::Unavailable(msg) => Self::ServiceUnavailable(msg),
            BuildServiceError::UpstreamTimeout(msg) => Self::GatewayTimeout(msg),
            BuildServiceError::Internal(msg) => Self::Internal(msg),
        }
    }
}

impl From<MetaError> for ApiError {
    fn from(err: MetaError) -> Self {
        Self::from(BuildServiceError::from_meta(err))
    }
}

impl From<&RepositoryError> for ApiError {
    fn from(err: &RepositoryError) -> Self {
        Self::from(BuildServiceError::from_repository(err))
    }
}

impl From<VolumeError> for ApiError {
    fn from(error: VolumeError) -> Self {
        match error {
            VolumeError::InvalidName
            | VolumeError::MultipleSources
            | VolumeError::SourceNotFound(_)
            | VolumeError::InvalidSize
            | VolumeError::EmptySnapshotLayers
            | VolumeError::InvalidNextToken
            | VolumeError::InvalidPageLimit
            | VolumeError::SizeLimitExceeded { .. }
            | VolumeError::SizeMismatch
            | VolumeError::TooManyMountedVolumes { .. } => Self::BadRequest(error.to_string()),
            VolumeError::NotFound(_) => Self::NotFound(error.to_string()),
            VolumeError::NameConflict(_)
            | VolumeError::Reserved(_)
            | VolumeError::Uploading(_)
            | VolumeError::Failed(_) => Self::Conflict(error.to_string()),
            VolumeError::Storage(_) => Self::Internal(error.to_string()),
        }
    }
}

/// Find the [`HookStatusError`] in an orchestrator error chain, present when
/// the failure was a non-2xx response from the custom extension.
pub(super) fn hook_status_error(err: &OrchestratorError) -> Option<&HookStatusError> {
    let OrchestratorError::SandboxOperationFailed { source, .. } = err else {
        return None;
    };
    source
        .chain()
        .find_map(|cause| cause.downcast_ref::<HookStatusError>())
}

/// Error message to surface to the caller as a client error (4xx) when the
/// custom extension rejected the operation with a 4xx hook response.
pub(super) fn hook_client_error_message(err: &OrchestratorError) -> Option<String> {
    hook_status_error(err)
        .filter(|hook_err| hook_err.is_client_error())
        .map(ToString::to_string)
}

impl From<OrchestratorError> for ApiError {
    fn from(err: OrchestratorError) -> Self {
        if let Some(message) = hook_client_error_message(&err) {
            return Self::BadRequest(message);
        }
        match err {
            OrchestratorError::ShuttingDown => {
                Self::ServiceUnavailable("orchestrator is shutting down".to_owned())
            }
            OrchestratorError::SandboxNotFound(id) => {
                Self::NotFound(format!("sandbox {id} not found"))
            }
            OrchestratorError::InvalidSandboxState { .. } => Self::BadRequest(err.to_string()),
            OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation,
                source,
            } => Self::Internal(format!(
                "sandbox {} operation {:?} failed: {}",
                sandbox_id,
                operation,
                Self::internal_from(source.as_ref()).into_message()
            )),
            OrchestratorError::SandboxOperationConflict { .. } => Self::Conflict(err.to_string()),
            other => Self::internal_from(&other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_error_fallback_normalizes_json_code_to_500() {
        let model = ApiError::from(OrchestratorError::ShuttingDown).into_server_error_model();
        assert_eq!(model.code, 500);
        assert!(model.message.contains("shutting down"));
    }

    #[test]
    fn bad_request_fallback_normalizes_json_code_to_400() {
        let model = ApiError::from(VolumeError::NotFound("vol_1".into())).into_bad_request_model();
        assert_eq!(model.code, 400);
        assert!(model.message.contains("vol_1"));
    }
}
