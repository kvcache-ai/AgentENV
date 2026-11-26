//! Unified asynchronous build jobs for template and imageOnly builds.

use std::fmt;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::{ImageName, ImageTag, SandboxResources, SnapshotAlias, SnapshotId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildKind {
    Template,
    Image,
}

impl BuildKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Template => "template",
            Self::Image => "image",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "template" => Ok(Self::Template),
            "image" => Ok(Self::Image),
            other => bail!("unknown build kind '{other}'"),
        }
    }
}

impl fmt::Display for BuildKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildStatus {
    Waiting,
    Building,
    Ready,
    Error,
}

impl BuildStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Building => "building",
            Self::Ready => "ready",
            Self::Error => "error",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "waiting" => Ok(Self::Waiting),
            "building" => Ok(Self::Building),
            "ready" => Ok(Self::Ready),
            "error" => Ok(Self::Error),
            other => bail!("unknown build status '{other}'"),
        }
    }
}

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

impl BuildTarget {
    pub const fn kind(&self) -> BuildKind {
        match self {
            Self::Template { .. } => BuildKind::Template,
            Self::Image { .. } => BuildKind::Image,
        }
    }
}

/// Durable metadata record of an asynchronous template or imageOnly build.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildJobRecord {
    pub build_id: SnapshotId,
    pub kind: BuildKind,
    pub status: BuildStatus,
    pub target: BuildTarget,
    pub result_snapshot_id: Option<SnapshotId>,
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
            kind: target.kind(),
            status: BuildStatus::Waiting,
            target,
            result_snapshot_id: None,
            result_image_digest: None,
            error_reason: None,
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
        }
    }

    pub fn template_alias(&self) -> Option<&SnapshotAlias> {
        match &self.target {
            BuildTarget::Template { alias, .. } => alias.as_ref(),
            BuildTarget::Image { .. } => None,
        }
    }

    pub fn image_name(&self) -> Option<&str> {
        match &self.target {
            BuildTarget::Image { name, .. } => Some(name.as_str()),
            BuildTarget::Template { .. } => None,
        }
    }

    pub fn image_tags(&self) -> Result<Vec<ImageTag>> {
        let BuildTarget::Image { name, tags } = &self.target else {
            bail!("build '{}' is not an image build", self.build_id);
        };
        let name = ImageName::parse(name)?;
        tags.iter().map(|tag| ImageTag::new(&name, tag)).collect()
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
