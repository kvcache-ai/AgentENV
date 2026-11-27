//! Unified asynchronous build jobs for template and imageOnly builds.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{impl_str_enum, SandboxResources, SnapshotAlias, SnapshotId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildStatus {
    Waiting,
    Building,
    Ready,
    Error,
}

impl_str_enum!(BuildStatus, "build status" {
    Waiting => "waiting",
    Building => "building",
    Ready => "ready",
    Error => "error",
});

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildErrorReason {
    pub message: String,
    #[serde(default)]
    pub step: Option<String>,
}

impl BuildErrorReason {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            step: None,
        }
    }

    pub fn with_step(message: impl Into<String>, step: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            step: Some(step.into()),
        }
    }
}

impl fmt::Display for BuildErrorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Target specification captured when a build job is requested.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BuildTarget {
    Template {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alias: Option<SnapshotAlias>,
        resources: SandboxResources,
    },
    Image {
        name: String,
        tags: Vec<String>,
    },
}

/// State transition applied to an existing build job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildJobTransition {
    Start,
    Finish { image_digest: Option<String> },
    Fail { reason: BuildErrorReason },
}

/// Durable metadata record of an asynchronous template or imageOnly build.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildJobRecord {
    pub build_id: SnapshotId,
    pub status: BuildStatus,
    pub target: BuildTarget,
    pub result_image_digest: Option<String>,
    pub error_reason: Option<BuildErrorReason>,
    pub started_at_unix_ms: Option<i64>,
    pub finished_at_unix_ms: Option<i64>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

impl BuildJobRecord {
    pub fn waiting(build_id: SnapshotId, target: BuildTarget, now_unix_ms: i64) -> Self {
        Self {
            build_id,
            status: BuildStatus::Waiting,
            target,
            result_image_digest: None,
            error_reason: None,
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_error_reason_round_trips() {
        let reason: BuildErrorReason =
            serde_json::from_str(r#"{"message":"boom","step":"resolve image"}"#)
                .expect("deserialize structured reason");
        assert_eq!(reason.message, "boom");
        assert_eq!(reason.step.as_deref(), Some("resolve image"));

        let without_step: BuildErrorReason =
            serde_json::from_str(r#"{"message":"boom"}"#).expect("deserialize message only");
        assert_eq!(without_step.step, None);
    }
}
