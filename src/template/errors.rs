use anyhow::Error as AnyhowError;
use thiserror::Error;

use crate::repository::RepositoryError;
use crate::snapshot::TemplateBuildErrorReason;

pub type TemplateBuildResult<T> = Result<T, TemplateBuildError>;
pub type TemplatePipelineResult<T> = Result<T, TemplatePipelineError>;

#[derive(Debug, Error)]
#[error("{reason}")]
pub(crate) struct TemplateBuildFailure {
    pub reason: TemplateBuildErrorReason,
}

impl TemplateBuildFailure {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            reason: TemplateBuildErrorReason::new(message),
        }
    }

    pub(crate) fn with_step(message: impl Into<String>, step: impl Into<String>) -> Self {
        Self {
            reason: TemplateBuildErrorReason::with_step(message, step),
        }
    }
}

pub(crate) fn command_output_suffix(stdout: &str, stderr: &str) -> String {
    let stderr = stderr.trim();
    if !stderr.is_empty() {
        return format!("; stderr: {stderr}");
    }

    let stdout = stdout.trim();
    if !stdout.is_empty() {
        return format!("; stdout: {stdout}");
    }

    String::new()
}

#[derive(Debug, Error)]
pub enum TemplateBuildError {
    #[error("invalid template build input: {reason}")]
    InvalidInput { reason: String },

    #[error("template build failed: {reason}")]
    System {
        reason: TemplateBuildErrorReason,
        #[source]
        source: Option<AnyhowError>,
    },
}

impl TemplateBuildError {
    pub fn invalid_input(reason: impl Into<String>) -> Self {
        Self::InvalidInput {
            reason: reason.into(),
        }
    }

    pub fn system(message: impl Into<String>) -> Self {
        Self::System {
            reason: TemplateBuildErrorReason::new(message),
            source: None,
        }
    }

    pub fn with_source(message: impl Into<String>, source: impl Into<AnyhowError>) -> Self {
        Self::System {
            reason: TemplateBuildErrorReason::new(message),
            source: Some(source.into()),
        }
    }

    pub(crate) fn with_reason_source(
        reason: TemplateBuildErrorReason,
        source: impl Into<AnyhowError>,
    ) -> Self {
        Self::System {
            reason,
            source: Some(source.into()),
        }
    }
}

#[derive(Debug, Error)]
pub enum TemplatePipelineError {
    #[error(transparent)]
    Build(#[from] TemplateBuildError),

    #[error(transparent)]
    Repository(#[from] RepositoryError),
}
