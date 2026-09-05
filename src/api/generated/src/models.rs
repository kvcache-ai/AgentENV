#![allow(unused_qualifications)]

use http::HeaderValue;
use validator::Validate;

#[cfg(feature = "server")]
use crate::header;
use crate::{models, types::*};

#[allow(dead_code)]
fn from_validation_error(e: validator::ValidationError) -> validator::ValidationErrors {
    let mut errs = validator::ValidationErrors::new();
    errs.add("na", e);
    errs
}

#[allow(dead_code)]
pub fn check_xss_string(v: &str) -> std::result::Result<(), validator::ValidationError> {
    if ammonia::is_html(v) {
        std::result::Result::Err(validator::ValidationError::new("xss detected"))
    } else {
        std::result::Result::Ok(())
    }
}

#[allow(dead_code)]
pub fn check_xss_vec_string(v: &[String]) -> std::result::Result<(), validator::ValidationError> {
    if v.iter().any(|i| ammonia::is_html(i)) {
        std::result::Result::Err(validator::ValidationError::new("xss detected"))
    } else {
        std::result::Result::Ok(())
    }
}

#[allow(dead_code)]
pub fn check_xss_map_string(
    v: &std::collections::HashMap<String, String>,
) -> std::result::Result<(), validator::ValidationError> {
    if v.keys().any(|k| ammonia::is_html(k)) || v.values().any(|v| ammonia::is_html(v)) {
        std::result::Result::Err(validator::ValidationError::new("xss detected"))
    } else {
        std::result::Result::Ok(())
    }
}

#[allow(dead_code)]
pub fn check_xss_map_nested<T>(
    v: &std::collections::HashMap<String, T>,
) -> std::result::Result<(), validator::ValidationError>
where
    T: validator::Validate,
{
    if v.keys().any(|k| ammonia::is_html(k)) || v.values().any(|v| v.validate().is_err()) {
        std::result::Result::Err(validator::ValidationError::new("xss detected"))
    } else {
        std::result::Result::Ok(())
    }
}

#[allow(dead_code)]
pub fn check_xss_map<T>(
    v: &std::collections::HashMap<String, T>,
) -> std::result::Result<(), validator::ValidationError> {
    if v.keys().any(|k| ammonia::is_html(k)) {
        std::result::Result::Err(validator::ValidationError::new("xss detected"))
    } else {
        std::result::Result::Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct NodesGetQueryParams {
    /// Identifier of the cluster
    #[serde(rename = "clusterID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<uuid::Uuid>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct NodesNodeIdGetPathParams {
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct NodesNodeIdGetQueryParams {
    /// Identifier of the cluster
    #[serde(rename = "clusterID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<uuid::Uuid>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdCpuAffinityPostPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesGetQueryParams {
    /// Metadata query used to filter the sandboxes (e.g. \"user=abc&app=prod\"). Each key and values must be URL encoded.
    #[serde(rename = "metadata")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdConnectPostPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdCustomExtensionParamsGetPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdCustomExtensionParamsPatchPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdDeletePathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdForkPostPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdGetPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdNetworkPutPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdPausePostPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdRefreshesPostPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdResumePostPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdSnapshotsPostPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxesSandboxIdTimeoutPostPathParams {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct V2SandboxesGetQueryParams {
    /// Metadata query used to filter the sandboxes (e.g. \"user=abc&app=prod\"). Each key and values must be URL encoded.
    #[serde(rename = "metadata")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    /// Filter sandboxes by one or more states
    #[serde(rename = "state")]
    #[serde(default)]
    pub state: Vec<models::SandboxState>,
    /// Sort direction by sandbox start time. Defaults to desc (newest first).
    #[serde(rename = "order")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<models::OrderDirection>,
    /// Return sandboxes started at or after this timestamp.
    #[serde(rename = "startedAfter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_after: Option<chrono::DateTime<chrono::Utc>>,
    /// Filter sandboxes by a template ID or alias.
    #[serde(rename = "template")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// Cursor to start the list from
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    /// Maximum number of items to return per page
    #[serde(rename = "limit")]
    #[validate(range(min = 1u32, max = 100u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SnapshotsGetQueryParams {
    #[serde(rename = "sandboxID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_id: Option<String>,
    /// Filter snapshots by name or ID, optionally tag-qualified (e.g. \"my-snapshot\", \"my-team/my-snapshot\" or \"my-snapshot:v1\").
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Maximum number of items to return per page
    #[serde(rename = "limit")]
    #[validate(range(min = 1u32, max = 100u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Cursor to start the list from
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SnapshotsSnapshotIdGetPathParams {
    pub snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplatesAliasesAliasGetPathParams {
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplatesGetQueryParams {
    #[serde(rename = "teamID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplatesTemplateIdBuildsBuildIdStatusGetPathParams {
    pub template_id: String,
    pub build_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplatesTemplateIdDeletePathParams {
    pub template_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplatesTemplateIdGetPathParams {
    pub template_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplatesTemplateIdGetQueryParams {
    /// Cursor to start the list from
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    /// Maximum number of items to return per page
    #[serde(rename = "limit")]
    #[validate(range(min = 1u32, max = 100u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct V2TemplatesGetQueryParams {
    #[serde(rename = "teamID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    /// Cursor to start the list from
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    /// Maximum number of items to return per page
    #[serde(rename = "limit")]
    #[validate(range(min = 1u32, max = 100u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct V2TemplatesTemplateIdBuildsBuildIdPostPathParams {
    pub template_id: String,
    pub build_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct VolumesGetQueryParams {
    /// Cursor to start the list from
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    /// Maximum number of items to return per page
    #[serde(rename = "limit")]
    #[validate(range(min = 1u32, max = 100u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct VolumesVolumeIdDeletePathParams {
    pub volume_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct VolumesVolumeIdGetPathParams {
    pub volume_id: String,
}

/// Block drive to attach when starting a sandbox. Attached drives are sandbox launch inputs; if the sandbox is later snapshotted, the current drive state is captured into the resulting snapshot.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AttachedDrive {
    /// Identifier used for the attached Firecracker drive. Must be non-empty, unique per sandbox, and must not contain \"/\".
    #[serde(rename = "driveID")]
    #[validate(custom(function = "check_xss_string"))]
    pub drive_id: String,

    /// Whether the attached drive should be mounted read-only. Defaults to true.
    #[serde(rename = "readOnly")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,

    /// Absolute guest mount path. Defaults to `/mnt/{driveID}`. Must not be `/`, contain `..`, spaces, commas, or colons, and must not be under reserved paths such as `/proc`, `/sys`, `/dev`, `/run`, `/agentenv`, or `/opt/agentenv`.
    #[serde(rename = "mountPath")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mount_path: Option<String>,

    /// Optional sub-directory inside the drive to expose at `mountPath`, matching Kubernetes `volumeMounts.subPath`. When set, only the named sub-directory of the drive is visible at `mountPath` inside the guest. Must be a relative path, must not be empty, must not contain `..`, spaces, commas, or colons, and must not start with `/`.
    #[serde(rename = "subPath")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,

    /// Disk size for the sandbox in MiB
    #[serde(rename = "diskSizeMB")]
    #[validate(range(min = 0u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_size_mb: Option<u32>,

    #[serde(rename = "source")]
    #[validate(nested)]
    pub source: models::AttachedDriveSource,
}

impl AttachedDrive {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(drive_id: String, source: models::AttachedDriveSource) -> AttachedDrive {
        AttachedDrive {
            drive_id,
            read_only: Some(true),
            mount_path: None,
            sub_path: None,
            disk_size_mb: None,
            source,
        }
    }
}

/// Converts the AttachedDrive value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for AttachedDrive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("driveID".to_string()),
            Some(self.drive_id.to_string()),
            self.read_only
                .as_ref()
                .map(|read_only| ["readOnly".to_string(), read_only.to_string()].join(",")),
            self.mount_path
                .as_ref()
                .map(|mount_path| ["mountPath".to_string(), mount_path.to_string()].join(",")),
            self.sub_path
                .as_ref()
                .map(|sub_path| ["subPath".to_string(), sub_path.to_string()].join(",")),
            self.disk_size_mb
                .as_ref()
                .map(|disk_size_mb| ["diskSizeMB".to_string(), disk_size_mb.to_string()].join(",")),
            // Skipping source in query parameter serialization
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AttachedDrive value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AttachedDrive {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub drive_id: Vec<String>,
            pub read_only: Vec<bool>,
            pub mount_path: Vec<String>,
            pub sub_path: Vec<String>,
            pub disk_size_mb: Vec<u32>,
            pub source: Vec<models::AttachedDriveSource>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AttachedDrive".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "driveID" => intermediate_rep.drive_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "readOnly" => intermediate_rep.read_only.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "mountPath" => intermediate_rep.mount_path.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "subPath" => intermediate_rep.sub_path.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "diskSizeMB" => intermediate_rep.disk_size_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "source" => intermediate_rep.source.push(
                        <models::AttachedDriveSource as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AttachedDrive".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AttachedDrive {
            drive_id: intermediate_rep
                .drive_id
                .into_iter()
                .next()
                .ok_or_else(|| "driveID missing in AttachedDrive".to_string())?,
            read_only: intermediate_rep.read_only.into_iter().next(),
            mount_path: intermediate_rep.mount_path.into_iter().next(),
            sub_path: intermediate_rep.sub_path.into_iter().next(),
            disk_size_mb: intermediate_rep.disk_size_mb.into_iter().next(),
            source: intermediate_rep
                .source
                .into_iter()
                .next()
                .ok_or_else(|| "source missing in AttachedDrive".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AttachedDrive> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<AttachedDrive>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AttachedDrive>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for AttachedDrive - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<AttachedDrive> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AttachedDrive as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into AttachedDrive - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Source for a sandbox attached drive. `image` must be provided.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct AttachedDriveSource {
    /// OCI image reference to resolve into an OverlayBD drive.
    #[serde(rename = "image")]
    #[validate(custom(function = "check_xss_string"))]
    pub image: String,
}

impl AttachedDriveSource {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(image: String) -> AttachedDriveSource {
        AttachedDriveSource { image }
    }
}

/// Converts the AttachedDriveSource value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for AttachedDriveSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> =
            vec![Some("image".to_string()), Some(self.image.to_string())];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a AttachedDriveSource value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for AttachedDriveSource {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub image: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing AttachedDriveSource".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "image" => intermediate_rep.image.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing AttachedDriveSource".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(AttachedDriveSource {
            image: intermediate_rep
                .image
                .into_iter()
                .next()
                .ok_or_else(|| "image missing in AttachedDriveSource".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<AttachedDriveSource> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<AttachedDriveSource>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<AttachedDriveSource>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for AttachedDriveSource - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<AttachedDriveSource> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <AttachedDriveSource as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into AttachedDriveSource - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct BuildLogEntry {
    /// Timestamp of the log entry
    #[serde(rename = "timestamp")]
    pub timestamp: chrono::DateTime<chrono::Utc>,

    /// Log message content
    #[serde(rename = "message")]
    #[validate(custom(function = "check_xss_string"))]
    pub message: String,

    #[serde(rename = "level")]
    #[validate(nested)]
    pub level: models::LogLevel,

    /// Step in the build process related to the log entry
    #[serde(rename = "step")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

impl BuildLogEntry {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        timestamp: chrono::DateTime<chrono::Utc>,
        message: String,
        level: models::LogLevel,
    ) -> BuildLogEntry {
        BuildLogEntry {
            timestamp,
            message,
            level,
            step: None,
        }
    }
}

/// Converts the BuildLogEntry value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for BuildLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            // Skipping timestamp in query parameter serialization
            Some("message".to_string()),
            Some(self.message.to_string()),
            // Skipping level in query parameter serialization
            self.step
                .as_ref()
                .map(|step| ["step".to_string(), step.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a BuildLogEntry value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for BuildLogEntry {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub timestamp: Vec<chrono::DateTime<chrono::Utc>>,
            pub message: Vec<String>,
            pub level: Vec<models::LogLevel>,
            pub step: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing BuildLogEntry".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "timestamp" => intermediate_rep.timestamp.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "message" => intermediate_rep.message.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "level" => intermediate_rep.level.push(
                        <models::LogLevel as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "step" => intermediate_rep.step.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing BuildLogEntry".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(BuildLogEntry {
            timestamp: intermediate_rep
                .timestamp
                .into_iter()
                .next()
                .ok_or_else(|| "timestamp missing in BuildLogEntry".to_string())?,
            message: intermediate_rep
                .message
                .into_iter()
                .next()
                .ok_or_else(|| "message missing in BuildLogEntry".to_string())?,
            level: intermediate_rep
                .level
                .into_iter()
                .next()
                .ok_or_else(|| "level missing in BuildLogEntry".to_string())?,
            step: intermediate_rep.step.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<BuildLogEntry> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<BuildLogEntry>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<BuildLogEntry>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for BuildLogEntry - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<BuildLogEntry> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <BuildLogEntry as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into BuildLogEntry - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct BuildStatusReason {
    /// Message with the status reason, currently reporting only for error status
    #[serde(rename = "message")]
    #[validate(custom(function = "check_xss_string"))]
    pub message: String,

    /// Step that failed
    #[serde(rename = "step")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,

    /// Log entries related to the status reason
    #[serde(rename = "logEntries")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_entries: Option<Vec<models::BuildLogEntry>>,
}

impl BuildStatusReason {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(message: String) -> BuildStatusReason {
        BuildStatusReason {
            message,
            step: None,
            log_entries: None,
        }
    }
}

/// Converts the BuildStatusReason value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for BuildStatusReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("message".to_string()),
            Some(self.message.to_string()),
            self.step
                .as_ref()
                .map(|step| ["step".to_string(), step.to_string()].join(",")),
            // Skipping logEntries in query parameter serialization
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a BuildStatusReason value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for BuildStatusReason {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub message: Vec<String>,
            pub step: Vec<String>,
            pub log_entries: Vec<Vec<models::BuildLogEntry>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing BuildStatusReason".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "message" => intermediate_rep.message.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "step" => intermediate_rep.step.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "logEntries" => return std::result::Result::Err(
                        "Parsing a container in this style is not supported in BuildStatusReason"
                            .to_string(),
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing BuildStatusReason".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(BuildStatusReason {
            message: intermediate_rep
                .message
                .into_iter()
                .next()
                .ok_or_else(|| "message missing in BuildStatusReason".to_string())?,
            step: intermediate_rep.step.into_iter().next(),
            log_entries: intermediate_rep.log_entries.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<BuildStatusReason> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<BuildStatusReason>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<BuildStatusReason>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for BuildStatusReason - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<BuildStatusReason> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <BuildStatusReason as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into BuildStatusReason - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ConnectSandbox {
    /// Timeout in seconds from the current time after which the sandbox should expire
    #[serde(rename = "timeout")]
    #[validate(range(min = 0u32))]
    pub timeout: u32,
}

impl ConnectSandbox {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(timeout: u32) -> ConnectSandbox {
        ConnectSandbox { timeout }
    }
}

/// Converts the ConnectSandbox value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for ConnectSandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> =
            vec![Some("timeout".to_string()), Some(self.timeout.to_string())];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a ConnectSandbox value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for ConnectSandbox {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub timeout: Vec<u32>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing ConnectSandbox".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "timeout" => intermediate_rep.timeout.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing ConnectSandbox".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(ConnectSandbox {
            timeout: intermediate_rep
                .timeout
                .into_iter()
                .next()
                .ok_or_else(|| "timeout missing in ConnectSandbox".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<ConnectSandbox> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<ConnectSandbox>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<ConnectSandbox>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for ConnectSandbox - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<ConnectSandbox> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <ConnectSandbox as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into ConnectSandbox - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// CPU cores for the sandbox
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct CpuCount {}

impl CpuCount {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> CpuCount {
        CpuCount {}
    }
}

/// Converts the CpuCount value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for CpuCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a CpuCount value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for CpuCount {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {}

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing CpuCount".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing CpuCount".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(CpuCount {})
    }
}

// Methods for converting between header::IntoHeaderValue<CpuCount> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<CpuCount>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<CpuCount>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for CpuCount - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<CpuCount> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <CpuCount as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into CpuCount - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct DiskMetrics {
    /// Mount point of the disk
    #[serde(rename = "mountPoint")]
    #[validate(custom(function = "check_xss_string"))]
    pub mount_point: String,

    /// Device name
    #[serde(rename = "device")]
    #[validate(custom(function = "check_xss_string"))]
    pub device: String,

    /// Filesystem type (e.g., ext4, xfs)
    #[serde(rename = "filesystemType")]
    #[validate(custom(function = "check_xss_string"))]
    pub filesystem_type: String,

    /// Used space in bytes
    #[serde(rename = "usedBytes")]
    pub used_bytes: u64,

    /// Total space in bytes
    #[serde(rename = "totalBytes")]
    pub total_bytes: u64,
}

impl DiskMetrics {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        mount_point: String,
        device: String,
        filesystem_type: String,
        used_bytes: u64,
        total_bytes: u64,
    ) -> DiskMetrics {
        DiskMetrics {
            mount_point,
            device,
            filesystem_type,
            used_bytes,
            total_bytes,
        }
    }
}

/// Converts the DiskMetrics value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for DiskMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("mountPoint".to_string()),
            Some(self.mount_point.to_string()),
            Some("device".to_string()),
            Some(self.device.to_string()),
            Some("filesystemType".to_string()),
            Some(self.filesystem_type.to_string()),
            Some("usedBytes".to_string()),
            Some(self.used_bytes.to_string()),
            Some("totalBytes".to_string()),
            Some(self.total_bytes.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a DiskMetrics value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for DiskMetrics {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub mount_point: Vec<String>,
            pub device: Vec<String>,
            pub filesystem_type: Vec<String>,
            pub used_bytes: Vec<u64>,
            pub total_bytes: Vec<u64>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing DiskMetrics".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "mountPoint" => intermediate_rep.mount_point.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "device" => intermediate_rep.device.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "filesystemType" => intermediate_rep.filesystem_type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "usedBytes" => intermediate_rep.used_bytes.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "totalBytes" => intermediate_rep.total_bytes.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing DiskMetrics".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(DiskMetrics {
            mount_point: intermediate_rep
                .mount_point
                .into_iter()
                .next()
                .ok_or_else(|| "mountPoint missing in DiskMetrics".to_string())?,
            device: intermediate_rep
                .device
                .into_iter()
                .next()
                .ok_or_else(|| "device missing in DiskMetrics".to_string())?,
            filesystem_type: intermediate_rep
                .filesystem_type
                .into_iter()
                .next()
                .ok_or_else(|| "filesystemType missing in DiskMetrics".to_string())?,
            used_bytes: intermediate_rep
                .used_bytes
                .into_iter()
                .next()
                .ok_or_else(|| "usedBytes missing in DiskMetrics".to_string())?,
            total_bytes: intermediate_rep
                .total_bytes
                .into_iter()
                .next()
                .ok_or_else(|| "totalBytes missing in DiskMetrics".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<DiskMetrics> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<DiskMetrics>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<DiskMetrics>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for DiskMetrics - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<DiskMetrics> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <DiskMetrics as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into DiskMetrics - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Disk size for the sandbox in MiB
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct DiskSizeMb {}

impl DiskSizeMb {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> DiskSizeMb {
        DiskSizeMb {}
    }
}

/// Converts the DiskSizeMb value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for DiskSizeMb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a DiskSizeMb value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for DiskSizeMb {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {}

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing DiskSizeMb".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing DiskSizeMb".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(DiskSizeMb {})
    }
}

// Methods for converting between header::IntoHeaderValue<DiskSizeMb> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<DiskSizeMb>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<DiskSizeMb>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for DiskSizeMb - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<DiskSizeMb> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <DiskSizeMb as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into DiskSizeMb - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Version of the envd running in the sandbox
#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct EnvdVersion(pub String);

impl validator::Validate for EnvdVersion {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

impl std::convert::From<String> for EnvdVersion {
    fn from(x: String) -> Self {
        EnvdVersion(x)
    }
}

impl std::fmt::Display for EnvdVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for EnvdVersion {
    type Err = std::string::ParseError;
    fn from_str(x: &str) -> std::result::Result<Self, Self::Err> {
        std::result::Result::Ok(EnvdVersion(x.to_string()))
    }
}

impl std::convert::From<EnvdVersion> for String {
    fn from(x: EnvdVersion) -> Self {
        x.0
    }
}

impl std::ops::Deref for EnvdVersion {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::ops::DerefMut for EnvdVersion {
    fn deref_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Error {
    /// Error code
    #[serde(rename = "code")]
    pub code: i32,

    /// Error
    #[serde(rename = "message")]
    #[validate(custom(function = "check_xss_string"))]
    pub message: String,
}

impl Error {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(code: i32, message: String) -> Error {
        Error { code, message }
    }
}

/// Converts the Error value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("code".to_string()),
            Some(self.code.to_string()),
            Some("message".to_string()),
            Some(self.message.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Error value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Error {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub code: Vec<i32>,
            pub message: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Error".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "code" => intermediate_rep.code.push(
                        <i32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "message" => intermediate_rep.message.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Error".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Error {
            code: intermediate_rep
                .code
                .into_iter()
                .next()
                .ok_or_else(|| "code missing in Error".to_string())?,
            message: intermediate_rep
                .message
                .into_iter()
                .next()
                .ok_or_else(|| "message missing in Error".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Error> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<Error>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Error>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for Error - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<Error> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => match <Error as std::str::FromStr>::from_str(value) {
                std::result::Result::Ok(value) => {
                    std::result::Result::Ok(header::IntoHeaderValue(value))
                }
                std::result::Result::Err(err) => std::result::Result::Err(format!(
                    r#"Unable to convert header value '{value}' into Error - {err}"#
                )),
            },
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ListedSandbox {
    /// Identifier of the template from which is the sandbox created
    #[serde(rename = "templateID")]
    #[validate(custom(function = "check_xss_string"))]
    pub template_id: String,

    /// Alias of the template
    #[serde(rename = "alias")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,

    /// Identifier of the sandbox
    #[serde(rename = "sandboxID")]
    #[validate(custom(function = "check_xss_string"))]
    pub sandbox_id: String,

    /// Identifier of the client
    #[serde(rename = "clientID")]
    #[validate(custom(function = "check_xss_string"))]
    pub client_id: String,

    /// Time when the sandbox was started
    #[serde(rename = "startedAt")]
    pub started_at: chrono::DateTime<chrono::Utc>,

    /// Time when the sandbox will expire
    #[serde(rename = "endAt")]
    pub end_at: chrono::DateTime<chrono::Utc>,

    /// CPU cores for the sandbox
    #[serde(rename = "cpuCount")]
    #[validate(range(min = 1u32))]
    pub cpu_count: u32,

    /// Memory for the sandbox in MiB
    #[serde(rename = "memoryMB")]
    #[validate(range(min = 128u32))]
    pub memory_mb: u32,

    /// Disk size for the sandbox in MiB
    #[serde(rename = "diskSizeMB")]
    #[validate(range(min = 0u32))]
    pub disk_size_mb: u32,

    #[serde(rename = "metadata")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,

    #[serde(rename = "state")]
    #[validate(nested)]
    pub state: models::SandboxState,

    /// Version of the envd running in the sandbox
    #[serde(rename = "envdVersion")]
    #[validate(custom(function = "check_xss_string"))]
    pub envd_version: String,

    /// Map of absolute guest mount paths to volume IDs or names.
    #[serde(rename = "volumeMounts")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<std::collections::HashMap<String, String>>,
}

impl ListedSandbox {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        template_id: String,
        sandbox_id: String,
        client_id: String,
        started_at: chrono::DateTime<chrono::Utc>,
        end_at: chrono::DateTime<chrono::Utc>,
        cpu_count: u32,
        memory_mb: u32,
        disk_size_mb: u32,
        state: models::SandboxState,
        envd_version: String,
    ) -> ListedSandbox {
        ListedSandbox {
            template_id,
            alias: None,
            sandbox_id,
            client_id,
            started_at,
            end_at,
            cpu_count,
            memory_mb,
            disk_size_mb,
            metadata: None,
            state,
            envd_version,
            volume_mounts: None,
        }
    }
}

/// Converts the ListedSandbox value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for ListedSandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("templateID".to_string()),
            Some(self.template_id.to_string()),
            self.alias
                .as_ref()
                .map(|alias| ["alias".to_string(), alias.to_string()].join(",")),
            Some("sandboxID".to_string()),
            Some(self.sandbox_id.to_string()),
            Some("clientID".to_string()),
            Some(self.client_id.to_string()),
            // Skipping startedAt in query parameter serialization

            // Skipping endAt in query parameter serialization
            Some("cpuCount".to_string()),
            Some(self.cpu_count.to_string()),
            Some("memoryMB".to_string()),
            Some(self.memory_mb.to_string()),
            Some("diskSizeMB".to_string()),
            Some(self.disk_size_mb.to_string()),
            // Skipping metadata in query parameter serialization

            // Skipping state in query parameter serialization
            Some("envdVersion".to_string()),
            Some(self.envd_version.to_string()),
            // Skipping volumeMounts in query parameter serialization
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a ListedSandbox value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for ListedSandbox {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub template_id: Vec<String>,
            pub alias: Vec<String>,
            pub sandbox_id: Vec<String>,
            pub client_id: Vec<String>,
            pub started_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub end_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub cpu_count: Vec<u32>,
            pub memory_mb: Vec<u32>,
            pub disk_size_mb: Vec<u32>,
            pub metadata: Vec<std::collections::HashMap<String, String>>,
            pub state: Vec<models::SandboxState>,
            pub envd_version: Vec<String>,
            pub volume_mounts: Vec<std::collections::HashMap<String, String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing ListedSandbox".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "templateID" => intermediate_rep.template_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "alias" => intermediate_rep.alias.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sandboxID" => intermediate_rep.sandbox_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "clientID" => intermediate_rep.client_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "startedAt" => intermediate_rep.started_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "endAt" => intermediate_rep.end_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cpuCount" => intermediate_rep.cpu_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "memoryMB" => intermediate_rep.memory_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "diskSizeMB" => intermediate_rep.disk_size_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "metadata" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in ListedSandbox"
                                .to_string(),
                        );
                    }
                    #[allow(clippy::redundant_clone)]
                    "state" => intermediate_rep.state.push(
                        <models::SandboxState as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "envdVersion" => intermediate_rep.envd_version.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "volumeMounts" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in ListedSandbox"
                                .to_string(),
                        );
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing ListedSandbox".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(ListedSandbox {
            template_id: intermediate_rep
                .template_id
                .into_iter()
                .next()
                .ok_or_else(|| "templateID missing in ListedSandbox".to_string())?,
            alias: intermediate_rep.alias.into_iter().next(),
            sandbox_id: intermediate_rep
                .sandbox_id
                .into_iter()
                .next()
                .ok_or_else(|| "sandboxID missing in ListedSandbox".to_string())?,
            client_id: intermediate_rep
                .client_id
                .into_iter()
                .next()
                .ok_or_else(|| "clientID missing in ListedSandbox".to_string())?,
            started_at: intermediate_rep
                .started_at
                .into_iter()
                .next()
                .ok_or_else(|| "startedAt missing in ListedSandbox".to_string())?,
            end_at: intermediate_rep
                .end_at
                .into_iter()
                .next()
                .ok_or_else(|| "endAt missing in ListedSandbox".to_string())?,
            cpu_count: intermediate_rep
                .cpu_count
                .into_iter()
                .next()
                .ok_or_else(|| "cpuCount missing in ListedSandbox".to_string())?,
            memory_mb: intermediate_rep
                .memory_mb
                .into_iter()
                .next()
                .ok_or_else(|| "memoryMB missing in ListedSandbox".to_string())?,
            disk_size_mb: intermediate_rep
                .disk_size_mb
                .into_iter()
                .next()
                .ok_or_else(|| "diskSizeMB missing in ListedSandbox".to_string())?,
            metadata: intermediate_rep.metadata.into_iter().next(),
            state: intermediate_rep
                .state
                .into_iter()
                .next()
                .ok_or_else(|| "state missing in ListedSandbox".to_string())?,
            envd_version: intermediate_rep
                .envd_version
                .into_iter()
                .next()
                .ok_or_else(|| "envdVersion missing in ListedSandbox".to_string())?,
            volume_mounts: intermediate_rep.volume_mounts.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<ListedSandbox> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<ListedSandbox>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<ListedSandbox>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for ListedSandbox - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<ListedSandbox> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <ListedSandbox as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into ListedSandbox - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// State of the sandbox
/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types, clippy::large_enum_variant)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum LogLevel {
    #[serde(rename = "debug")]
    Debug,
    #[serde(rename = "info")]
    Info,
    #[serde(rename = "warn")]
    Warn,
    #[serde(rename = "error")]
    Error,
}

impl validator::Validate for LogLevel {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            LogLevel::Debug => write!(f, "debug"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Warn => write!(f, "warn"),
            LogLevel::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "debug" => std::result::Result::Ok(LogLevel::Debug),
            "info" => std::result::Result::Ok(LogLevel::Info),
            "warn" => std::result::Result::Ok(LogLevel::Warn),
            "error" => std::result::Result::Ok(LogLevel::Error),
            _ => std::result::Result::Err(format!(r#"Value not valid: {s}"#)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct MachineInfo {
    /// CPU family of the node
    #[serde(rename = "cpuFamily")]
    #[validate(custom(function = "check_xss_string"))]
    pub cpu_family: String,

    /// CPU model of the node
    #[serde(rename = "cpuModel")]
    #[validate(custom(function = "check_xss_string"))]
    pub cpu_model: String,

    /// CPU model name of the node
    #[serde(rename = "cpuModelName")]
    #[validate(custom(function = "check_xss_string"))]
    pub cpu_model_name: String,

    /// CPU architecture of the node
    #[serde(rename = "cpuArchitecture")]
    #[validate(custom(function = "check_xss_string"))]
    pub cpu_architecture: String,
}

impl MachineInfo {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        cpu_family: String,
        cpu_model: String,
        cpu_model_name: String,
        cpu_architecture: String,
    ) -> MachineInfo {
        MachineInfo {
            cpu_family,
            cpu_model,
            cpu_model_name,
            cpu_architecture,
        }
    }
}

/// Converts the MachineInfo value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for MachineInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("cpuFamily".to_string()),
            Some(self.cpu_family.to_string()),
            Some("cpuModel".to_string()),
            Some(self.cpu_model.to_string()),
            Some("cpuModelName".to_string()),
            Some(self.cpu_model_name.to_string()),
            Some("cpuArchitecture".to_string()),
            Some(self.cpu_architecture.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a MachineInfo value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for MachineInfo {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub cpu_family: Vec<String>,
            pub cpu_model: Vec<String>,
            pub cpu_model_name: Vec<String>,
            pub cpu_architecture: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing MachineInfo".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "cpuFamily" => intermediate_rep.cpu_family.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cpuModel" => intermediate_rep.cpu_model.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cpuModelName" => intermediate_rep.cpu_model_name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cpuArchitecture" => intermediate_rep.cpu_architecture.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing MachineInfo".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(MachineInfo {
            cpu_family: intermediate_rep
                .cpu_family
                .into_iter()
                .next()
                .ok_or_else(|| "cpuFamily missing in MachineInfo".to_string())?,
            cpu_model: intermediate_rep
                .cpu_model
                .into_iter()
                .next()
                .ok_or_else(|| "cpuModel missing in MachineInfo".to_string())?,
            cpu_model_name: intermediate_rep
                .cpu_model_name
                .into_iter()
                .next()
                .ok_or_else(|| "cpuModelName missing in MachineInfo".to_string())?,
            cpu_architecture: intermediate_rep
                .cpu_architecture
                .into_iter()
                .next()
                .ok_or_else(|| "cpuArchitecture missing in MachineInfo".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<MachineInfo> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<MachineInfo>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<MachineInfo>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for MachineInfo - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<MachineInfo> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <MachineInfo as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into MachineInfo - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Memory for the sandbox in MiB
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct MemoryMb {}

impl MemoryMb {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> MemoryMb {
        MemoryMb {}
    }
}

/// Converts the MemoryMb value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for MemoryMb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a MemoryMb value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for MemoryMb {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {}

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing MemoryMb".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing MemoryMb".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(MemoryMb {})
    }
}

// Methods for converting between header::IntoHeaderValue<MemoryMb> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<MemoryMb>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<MemoryMb>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for MemoryMb - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<MemoryMb> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <MemoryMb as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into MemoryMb - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct NewColdSandbox {
    /// Explicit external OCI image reference to use as the sandbox rootfs. Cold starts may pull and convert OCI layers on a cache miss and can take tens of seconds.
    #[serde(rename = "image")]
    #[validate(custom(function = "check_xss_string"))]
    pub image: String,

    /// Time to live for the sandbox in seconds.
    #[serde(rename = "timeout")]
    #[validate(range(min = 0u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,

    /// Automatically pauses the sandbox after the timeout
    #[serde(rename = "autoPause")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_pause: Option<bool>,

    #[serde(rename = "autoResume")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_resume: Option<models::SandboxAutoResumeConfig>,

    /// Secure all system communication with sandbox
    #[serde(rename = "secure")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secure: Option<bool>,

    /// Allow sandbox to access the internet. When set to false, it behaves the same as specifying denyOut to 0.0.0.0/0 in the network config.
    #[serde(rename = "allowInternetAccess")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_internet_access: Option<bool>,

    #[serde(rename = "network")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<models::SandboxNetworkConfig>,

    #[serde(rename = "metadata")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,

    #[serde(rename = "envVars")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_vars: Option<std::collections::HashMap<String, String>>,

    /// Opaque JSON object interpreted only by the custom extension. An absent value and an empty object are equivalent: both mean empty params.
    #[serde(rename = "customExtensionParams")]
    #[validate(custom(function = "check_xss_map"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<std::collections::HashMap<String, crate::types::Object>>,

    /// Map of absolute guest mount paths to volume IDs or names.
    #[serde(rename = "volumeMounts")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<std::collections::HashMap<String, String>>,

    /// CPU cores for the cold-start sandbox.
    #[serde(rename = "cpuCount")]
    #[validate(range(min = 1u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_count: Option<u32>,

    /// Memory for the cold-start sandbox in MiB.
    #[serde(rename = "memoryMB")]
    #[validate(range(min = 128u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u32>,

    /// Disk size for the sandbox in MiB
    #[serde(rename = "diskSizeMB")]
    #[validate(range(min = 0u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_size_mb: Option<u32>,

    /// Attached drives for the cold-start sandbox. If the sandbox is later snapshotted, current drive state is captured into the snapshot.
    #[serde(rename = "attachedDrives")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attached_drives: Option<Vec<models::AttachedDrive>>,

    /// Additional kernel command-line arguments appended to the Firecracker boot args for this cold-start sandbox. Arguments are whitespace-separated and may contain only 0-9, a-z, A-Z, underscore, hyphen, dot, slash, plus, or equals sign, so base64 values are allowed. The server also applies the firecracker.allowed_extra_boot_args_prefixes allowlist; non-matching or invalid arguments are silently ignored.
    #[serde(rename = "extraBootArgs")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_boot_args: Option<String>,
}

impl NewColdSandbox {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(image: String) -> NewColdSandbox {
        NewColdSandbox {
            image,
            timeout: Some(15),
            auto_pause: Some(true),
            auto_resume: None,
            secure: None,
            allow_internet_access: None,
            network: None,
            metadata: None,
            env_vars: None,
            custom_extension_params: None,
            volume_mounts: None,
            cpu_count: None,
            memory_mb: None,
            disk_size_mb: None,
            attached_drives: None,
            extra_boot_args: None,
        }
    }
}

/// Converts the NewColdSandbox value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for NewColdSandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("image".to_string()),
            Some(self.image.to_string()),
            self.timeout
                .as_ref()
                .map(|timeout| ["timeout".to_string(), timeout.to_string()].join(",")),
            self.auto_pause
                .as_ref()
                .map(|auto_pause| ["autoPause".to_string(), auto_pause.to_string()].join(",")),
            // Skipping autoResume in query parameter serialization
            self.secure
                .as_ref()
                .map(|secure| ["secure".to_string(), secure.to_string()].join(",")),
            self.allow_internet_access
                .as_ref()
                .map(|allow_internet_access| {
                    [
                        "allowInternetAccess".to_string(),
                        allow_internet_access.to_string(),
                    ]
                    .join(",")
                }),
            // Skipping network in query parameter serialization

            // Skipping metadata in query parameter serialization

            // Skipping envVars in query parameter serialization

            // Skipping customExtensionParams in query parameter serialization
            // Skipping customExtensionParams in query parameter serialization

            // Skipping volumeMounts in query parameter serialization
            self.cpu_count
                .as_ref()
                .map(|cpu_count| ["cpuCount".to_string(), cpu_count.to_string()].join(",")),
            self.memory_mb
                .as_ref()
                .map(|memory_mb| ["memoryMB".to_string(), memory_mb.to_string()].join(",")),
            self.disk_size_mb
                .as_ref()
                .map(|disk_size_mb| ["diskSizeMB".to_string(), disk_size_mb.to_string()].join(",")),
            // Skipping attachedDrives in query parameter serialization
            self.extra_boot_args.as_ref().map(|extra_boot_args| {
                ["extraBootArgs".to_string(), extra_boot_args.to_string()].join(",")
            }),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a NewColdSandbox value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for NewColdSandbox {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub image: Vec<String>,
            pub timeout: Vec<u32>,
            pub auto_pause: Vec<bool>,
            pub auto_resume: Vec<models::SandboxAutoResumeConfig>,
            pub secure: Vec<bool>,
            pub allow_internet_access: Vec<bool>,
            pub network: Vec<models::SandboxNetworkConfig>,
            pub metadata: Vec<std::collections::HashMap<String, String>>,
            pub env_vars: Vec<std::collections::HashMap<String, String>>,
            pub custom_extension_params:
                Vec<std::collections::HashMap<String, crate::types::Object>>,
            pub volume_mounts: Vec<std::collections::HashMap<String, String>>,
            pub cpu_count: Vec<u32>,
            pub memory_mb: Vec<u32>,
            pub disk_size_mb: Vec<u32>,
            pub attached_drives: Vec<Vec<models::AttachedDrive>>,
            pub extra_boot_args: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing NewColdSandbox".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "image" => intermediate_rep.image.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "timeout" => intermediate_rep.timeout.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "autoPause" => intermediate_rep.auto_pause.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "autoResume" => intermediate_rep.auto_resume.push(
                        <models::SandboxAutoResumeConfig as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "secure" => intermediate_rep.secure.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "allowInternetAccess" => intermediate_rep.allow_internet_access.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "network" => intermediate_rep.network.push(
                        <models::SandboxNetworkConfig as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    "metadata" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewColdSandbox"
                                .to_string(),
                        );
                    }
                    "envVars" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewColdSandbox"
                                .to_string(),
                        );
                    }
                    "customExtensionParams" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewColdSandbox"
                                .to_string(),
                        );
                    }
                    "volumeMounts" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewColdSandbox"
                                .to_string(),
                        );
                    }
                    #[allow(clippy::redundant_clone)]
                    "cpuCount" => intermediate_rep.cpu_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "memoryMB" => intermediate_rep.memory_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "diskSizeMB" => intermediate_rep.disk_size_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "attachedDrives" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewColdSandbox"
                                .to_string(),
                        );
                    }
                    #[allow(clippy::redundant_clone)]
                    "extraBootArgs" => intermediate_rep.extra_boot_args.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing NewColdSandbox".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(NewColdSandbox {
            image: intermediate_rep
                .image
                .into_iter()
                .next()
                .ok_or_else(|| "image missing in NewColdSandbox".to_string())?,
            timeout: intermediate_rep.timeout.into_iter().next(),
            auto_pause: intermediate_rep.auto_pause.into_iter().next(),
            auto_resume: intermediate_rep.auto_resume.into_iter().next(),
            secure: intermediate_rep.secure.into_iter().next(),
            allow_internet_access: intermediate_rep.allow_internet_access.into_iter().next(),
            network: intermediate_rep.network.into_iter().next(),
            metadata: intermediate_rep.metadata.into_iter().next(),
            env_vars: intermediate_rep.env_vars.into_iter().next(),
            custom_extension_params: intermediate_rep.custom_extension_params.into_iter().next(),
            volume_mounts: intermediate_rep.volume_mounts.into_iter().next(),
            cpu_count: intermediate_rep.cpu_count.into_iter().next(),
            memory_mb: intermediate_rep.memory_mb.into_iter().next(),
            disk_size_mb: intermediate_rep.disk_size_mb.into_iter().next(),
            attached_drives: intermediate_rep.attached_drives.into_iter().next(),
            extra_boot_args: intermediate_rep.extra_boot_args.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<NewColdSandbox> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<NewColdSandbox>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<NewColdSandbox>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for NewColdSandbox - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<NewColdSandbox> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <NewColdSandbox as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into NewColdSandbox - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct NewSandbox {
    /// Identifier of the required template/snapshot. Use POST /sandboxes-cold to create a sandbox directly from an external OCI image.
    #[serde(rename = "templateID")]
    #[validate(custom(function = "check_xss_string"))]
    pub template_id: String,

    /// Time to live for the sandbox in seconds.
    #[serde(rename = "timeout")]
    #[validate(range(min = 0u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,

    /// Automatically pauses the sandbox after the timeout
    #[serde(rename = "autoPause")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_pause: Option<bool>,

    #[serde(rename = "autoResume")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_resume: Option<models::SandboxAutoResumeConfig>,

    /// Secure all system communication with sandbox
    #[serde(rename = "secure")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secure: Option<bool>,

    /// Allow sandbox to access the internet. When set to false, it behaves the same as specifying denyOut to 0.0.0.0/0 in the network config.
    #[serde(rename = "allow_internet_access")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_internet_access: Option<bool>,

    #[serde(rename = "network")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<models::SandboxNetworkConfig>,

    #[serde(rename = "metadata")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,

    #[serde(rename = "envVars")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_vars: Option<std::collections::HashMap<String, String>>,

    /// Opaque JSON object interpreted only by the custom extension. An absent value and an empty object are equivalent: both mean empty params.
    #[serde(rename = "customExtensionParams")]
    #[validate(custom(function = "check_xss_map"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<std::collections::HashMap<String, crate::types::Object>>,

    /// MCP configuration for the sandbox
    #[serde(rename = "mcp")]
    #[serde(deserialize_with = "deserialize_optional_nullable")]
    #[serde(default = "default_optional_nullable")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp: Option<Nullable<std::collections::HashMap<String, crate::types::Object>>>,

    /// Map of absolute guest mount paths to volume IDs or names.
    #[serde(rename = "volumeMounts")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<std::collections::HashMap<String, String>>,
}

impl NewSandbox {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(template_id: String) -> NewSandbox {
        NewSandbox {
            template_id,
            timeout: Some(15),
            auto_pause: Some(true),
            auto_resume: None,
            secure: None,
            allow_internet_access: None,
            network: None,
            metadata: None,
            env_vars: None,
            custom_extension_params: None,
            mcp: None,
            volume_mounts: None,
        }
    }
}

/// Converts the NewSandbox value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for NewSandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("templateID".to_string()),
            Some(self.template_id.to_string()),
            self.timeout
                .as_ref()
                .map(|timeout| ["timeout".to_string(), timeout.to_string()].join(",")),
            self.auto_pause
                .as_ref()
                .map(|auto_pause| ["autoPause".to_string(), auto_pause.to_string()].join(",")),
            // Skipping autoResume in query parameter serialization
            self.secure
                .as_ref()
                .map(|secure| ["secure".to_string(), secure.to_string()].join(",")),
            self.allow_internet_access
                .as_ref()
                .map(|allow_internet_access| {
                    [
                        "allow_internet_access".to_string(),
                        allow_internet_access.to_string(),
                    ]
                    .join(",")
                }),
            // Skipping network in query parameter serialization

            // Skipping metadata in query parameter serialization

            // Skipping envVars in query parameter serialization

            // Skipping customExtensionParams in query parameter serialization
            // Skipping customExtensionParams in query parameter serialization

            // Skipping mcp in query parameter serialization
            // Skipping mcp in query parameter serialization

            // Skipping volumeMounts in query parameter serialization
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a NewSandbox value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for NewSandbox {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub template_id: Vec<String>,
            pub timeout: Vec<u32>,
            pub auto_pause: Vec<bool>,
            pub auto_resume: Vec<models::SandboxAutoResumeConfig>,
            pub secure: Vec<bool>,
            pub allow_internet_access: Vec<bool>,
            pub network: Vec<models::SandboxNetworkConfig>,
            pub metadata: Vec<std::collections::HashMap<String, String>>,
            pub env_vars: Vec<std::collections::HashMap<String, String>>,
            pub custom_extension_params:
                Vec<std::collections::HashMap<String, crate::types::Object>>,
            pub mcp: Vec<std::collections::HashMap<String, crate::types::Object>>,
            pub volume_mounts: Vec<std::collections::HashMap<String, String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing NewSandbox".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "templateID" => intermediate_rep.template_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "timeout" => intermediate_rep.timeout.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "autoPause" => intermediate_rep.auto_pause.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "autoResume" => intermediate_rep.auto_resume.push(
                        <models::SandboxAutoResumeConfig as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "secure" => intermediate_rep.secure.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "allow_internet_access" => intermediate_rep.allow_internet_access.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "network" => intermediate_rep.network.push(
                        <models::SandboxNetworkConfig as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    "metadata" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewSandbox"
                                .to_string(),
                        );
                    }
                    "envVars" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewSandbox"
                                .to_string(),
                        );
                    }
                    "customExtensionParams" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewSandbox"
                                .to_string(),
                        );
                    }
                    "mcp" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewSandbox"
                                .to_string(),
                        );
                    }
                    "volumeMounts" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NewSandbox"
                                .to_string(),
                        );
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing NewSandbox".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(NewSandbox {
            template_id: intermediate_rep
                .template_id
                .into_iter()
                .next()
                .ok_or_else(|| "templateID missing in NewSandbox".to_string())?,
            timeout: intermediate_rep.timeout.into_iter().next(),
            auto_pause: intermediate_rep.auto_pause.into_iter().next(),
            auto_resume: intermediate_rep.auto_resume.into_iter().next(),
            secure: intermediate_rep.secure.into_iter().next(),
            allow_internet_access: intermediate_rep.allow_internet_access.into_iter().next(),
            network: intermediate_rep.network.into_iter().next(),
            metadata: intermediate_rep.metadata.into_iter().next(),
            env_vars: intermediate_rep.env_vars.into_iter().next(),
            custom_extension_params: intermediate_rep.custom_extension_params.into_iter().next(),
            mcp: std::result::Result::Err(
                "Nullable types not supported in NewSandbox".to_string(),
            )?,
            volume_mounts: intermediate_rep.volume_mounts.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<NewSandbox> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<NewSandbox>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<NewSandbox>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for NewSandbox - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<NewSandbox> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <NewSandbox as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into NewSandbox - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct NewVolume {
    /// Unique volume name.
    #[serde(rename = "name")]
    #[validate(
            regex(path = *RE_NEWVOLUME_NAME),
          custom(function = "check_xss_string"),
    )]
    pub name: String,

    /// Volume size in MiB. Defaults to 65536 MiB (64 GiB).
    #[serde(rename = "sizeMB")]
    #[validate(range(min = 1u64))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_mb: Option<u64>,

    /// Access mode (`ro` or `exclusive`).
    #[serde(rename = "mode")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    /// Existing volume ID or name to use as a COW source. An exclusive source must not be mounted by a sandbox; sandbox fork snapshots and publishes its owned volumes internally before creating child volumes.
    #[serde(rename = "fromVolume")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_volume: Option<String>,

    /// OCI or OverlayBD image reference for the initial content.
    #[serde(rename = "image")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
}

lazy_static::lazy_static! {
    static ref RE_NEWVOLUME_NAME: regex::Regex = regex::Regex::new("^[a-zA-Z0-9_-]+$").unwrap();
}

impl NewVolume {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(name: String) -> NewVolume {
        NewVolume {
            name,
            size_mb: Some(65536),
            mode: None,
            from_volume: None,
            image: None,
        }
    }
}

/// Converts the NewVolume value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for NewVolume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("name".to_string()),
            Some(self.name.to_string()),
            self.size_mb
                .as_ref()
                .map(|size_mb| ["sizeMB".to_string(), size_mb.to_string()].join(",")),
            self.mode
                .as_ref()
                .map(|mode| ["mode".to_string(), mode.to_string()].join(",")),
            self.from_volume
                .as_ref()
                .map(|from_volume| ["fromVolume".to_string(), from_volume.to_string()].join(",")),
            self.image
                .as_ref()
                .map(|image| ["image".to_string(), image.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a NewVolume value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for NewVolume {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub name: Vec<String>,
            pub size_mb: Vec<u64>,
            pub mode: Vec<String>,
            pub from_volume: Vec<String>,
            pub image: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing NewVolume".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "name" => intermediate_rep.name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sizeMB" => intermediate_rep.size_mb.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "mode" => intermediate_rep.mode.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "fromVolume" => intermediate_rep.from_volume.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "image" => intermediate_rep.image.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing NewVolume".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(NewVolume {
            name: intermediate_rep
                .name
                .into_iter()
                .next()
                .ok_or_else(|| "name missing in NewVolume".to_string())?,
            size_mb: intermediate_rep.size_mb.into_iter().next(),
            mode: intermediate_rep.mode.into_iter().next(),
            from_volume: intermediate_rep.from_volume.into_iter().next(),
            image: intermediate_rep.image.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<NewVolume> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<NewVolume>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<NewVolume>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for NewVolume - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<NewVolume> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <NewVolume as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into NewVolume - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Node {
    /// Version of the orchestrator
    #[serde(rename = "version")]
    #[validate(custom(function = "check_xss_string"))]
    pub version: String,

    /// Commit of the orchestrator
    #[serde(rename = "commit")]
    #[validate(custom(function = "check_xss_string"))]
    pub commit: String,

    /// Identifier of the node
    #[serde(rename = "id")]
    #[validate(custom(function = "check_xss_string"))]
    pub id: String,

    /// Service instance identifier of the node
    #[serde(rename = "serviceInstanceID")]
    #[validate(custom(function = "check_xss_string"))]
    pub service_instance_id: String,

    /// Identifier of the cluster
    #[serde(rename = "clusterID")]
    #[validate(custom(function = "check_xss_string"))]
    pub cluster_id: String,

    #[serde(rename = "machineInfo")]
    #[validate(nested)]
    pub machine_info: models::MachineInfo,

    #[serde(rename = "status")]
    #[validate(nested)]
    pub status: models::NodeStatus,

    /// Number of sandboxes running on the node
    #[serde(rename = "sandboxCount")]
    pub sandbox_count: u32,

    #[serde(rename = "metrics")]
    #[validate(nested)]
    pub metrics: models::NodeMetrics,

    /// Number of sandbox create successes
    #[serde(rename = "createSuccesses")]
    pub create_successes: u64,

    /// Number of sandbox create fails
    #[serde(rename = "createFails")]
    pub create_fails: u64,

    /// Number of starting Sandboxes
    #[serde(rename = "sandboxStartingCount")]
    pub sandbox_starting_count: u32,

    /// Number of sandboxes currently in the Paused state
    #[serde(rename = "sandboxPausedCount")]
    pub sandbox_paused_count: u32,
}

impl Node {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        version: String,
        commit: String,
        id: String,
        service_instance_id: String,
        cluster_id: String,
        machine_info: models::MachineInfo,
        status: models::NodeStatus,
        sandbox_count: u32,
        metrics: models::NodeMetrics,
        create_successes: u64,
        create_fails: u64,
        sandbox_starting_count: u32,
        sandbox_paused_count: u32,
    ) -> Node {
        Node {
            version,
            commit,
            id,
            service_instance_id,
            cluster_id,
            machine_info,
            status,
            sandbox_count,
            metrics,
            create_successes,
            create_fails,
            sandbox_starting_count,
            sandbox_paused_count,
        }
    }
}

/// Converts the Node value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("version".to_string()),
            Some(self.version.to_string()),
            Some("commit".to_string()),
            Some(self.commit.to_string()),
            Some("id".to_string()),
            Some(self.id.to_string()),
            Some("serviceInstanceID".to_string()),
            Some(self.service_instance_id.to_string()),
            Some("clusterID".to_string()),
            Some(self.cluster_id.to_string()),
            // Skipping machineInfo in query parameter serialization

            // Skipping status in query parameter serialization
            Some("sandboxCount".to_string()),
            Some(self.sandbox_count.to_string()),
            // Skipping metrics in query parameter serialization
            Some("createSuccesses".to_string()),
            Some(self.create_successes.to_string()),
            Some("createFails".to_string()),
            Some(self.create_fails.to_string()),
            Some("sandboxStartingCount".to_string()),
            Some(self.sandbox_starting_count.to_string()),
            Some("sandboxPausedCount".to_string()),
            Some(self.sandbox_paused_count.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Node value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Node {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub version: Vec<String>,
            pub commit: Vec<String>,
            pub id: Vec<String>,
            pub service_instance_id: Vec<String>,
            pub cluster_id: Vec<String>,
            pub machine_info: Vec<models::MachineInfo>,
            pub status: Vec<models::NodeStatus>,
            pub sandbox_count: Vec<u32>,
            pub metrics: Vec<models::NodeMetrics>,
            pub create_successes: Vec<u64>,
            pub create_fails: Vec<u64>,
            pub sandbox_starting_count: Vec<u32>,
            pub sandbox_paused_count: Vec<u32>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Node".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "version" => intermediate_rep.version.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "commit" => intermediate_rep.commit.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "id" => intermediate_rep.id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "serviceInstanceID" => intermediate_rep.service_instance_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "clusterID" => intermediate_rep.cluster_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "machineInfo" => intermediate_rep.machine_info.push(
                        <models::MachineInfo as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "status" => intermediate_rep.status.push(
                        <models::NodeStatus as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sandboxCount" => intermediate_rep.sandbox_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "metrics" => intermediate_rep.metrics.push(
                        <models::NodeMetrics as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "createSuccesses" => intermediate_rep.create_successes.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "createFails" => intermediate_rep.create_fails.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sandboxStartingCount" => intermediate_rep.sandbox_starting_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sandboxPausedCount" => intermediate_rep.sandbox_paused_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Node".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Node {
            version: intermediate_rep
                .version
                .into_iter()
                .next()
                .ok_or_else(|| "version missing in Node".to_string())?,
            commit: intermediate_rep
                .commit
                .into_iter()
                .next()
                .ok_or_else(|| "commit missing in Node".to_string())?,
            id: intermediate_rep
                .id
                .into_iter()
                .next()
                .ok_or_else(|| "id missing in Node".to_string())?,
            service_instance_id: intermediate_rep
                .service_instance_id
                .into_iter()
                .next()
                .ok_or_else(|| "serviceInstanceID missing in Node".to_string())?,
            cluster_id: intermediate_rep
                .cluster_id
                .into_iter()
                .next()
                .ok_or_else(|| "clusterID missing in Node".to_string())?,
            machine_info: intermediate_rep
                .machine_info
                .into_iter()
                .next()
                .ok_or_else(|| "machineInfo missing in Node".to_string())?,
            status: intermediate_rep
                .status
                .into_iter()
                .next()
                .ok_or_else(|| "status missing in Node".to_string())?,
            sandbox_count: intermediate_rep
                .sandbox_count
                .into_iter()
                .next()
                .ok_or_else(|| "sandboxCount missing in Node".to_string())?,
            metrics: intermediate_rep
                .metrics
                .into_iter()
                .next()
                .ok_or_else(|| "metrics missing in Node".to_string())?,
            create_successes: intermediate_rep
                .create_successes
                .into_iter()
                .next()
                .ok_or_else(|| "createSuccesses missing in Node".to_string())?,
            create_fails: intermediate_rep
                .create_fails
                .into_iter()
                .next()
                .ok_or_else(|| "createFails missing in Node".to_string())?,
            sandbox_starting_count: intermediate_rep
                .sandbox_starting_count
                .into_iter()
                .next()
                .ok_or_else(|| "sandboxStartingCount missing in Node".to_string())?,
            sandbox_paused_count: intermediate_rep
                .sandbox_paused_count
                .into_iter()
                .next()
                .ok_or_else(|| "sandboxPausedCount missing in Node".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Node> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<Node>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Node>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for Node - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<Node> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => match <Node as std::str::FromStr>::from_str(value) {
                std::result::Result::Ok(value) => {
                    std::result::Result::Ok(header::IntoHeaderValue(value))
                }
                std::result::Result::Err(err) => std::result::Result::Err(format!(
                    r#"Unable to convert header value '{value}' into Node - {err}"#
                )),
            },
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct NodeDetail {
    /// Identifier of the cluster
    #[serde(rename = "clusterID")]
    #[validate(custom(function = "check_xss_string"))]
    pub cluster_id: String,

    /// Version of the orchestrator
    #[serde(rename = "version")]
    #[validate(custom(function = "check_xss_string"))]
    pub version: String,

    /// Commit of the orchestrator
    #[serde(rename = "commit")]
    #[validate(custom(function = "check_xss_string"))]
    pub commit: String,

    /// Identifier of the node
    #[serde(rename = "id")]
    #[validate(custom(function = "check_xss_string"))]
    pub id: String,

    /// Service instance identifier of the node
    #[serde(rename = "serviceInstanceID")]
    #[validate(custom(function = "check_xss_string"))]
    pub service_instance_id: String,

    #[serde(rename = "machineInfo")]
    #[validate(nested)]
    pub machine_info: models::MachineInfo,

    #[serde(rename = "status")]
    #[validate(nested)]
    pub status: models::NodeStatus,

    /// Number of sandboxes running on the node
    #[serde(rename = "sandboxCount")]
    pub sandbox_count: u32,

    #[serde(rename = "metrics")]
    #[validate(nested)]
    pub metrics: models::NodeMetrics,

    /// Number of sandbox create successes
    #[serde(rename = "createSuccesses")]
    pub create_successes: u64,

    /// Number of sandbox create fails
    #[serde(rename = "createFails")]
    pub create_fails: u64,

    /// Number of sandboxes currently in the Paused state
    #[serde(rename = "sandboxPausedCount")]
    pub sandbox_paused_count: u32,
}

impl NodeDetail {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        cluster_id: String,
        version: String,
        commit: String,
        id: String,
        service_instance_id: String,
        machine_info: models::MachineInfo,
        status: models::NodeStatus,
        sandbox_count: u32,
        metrics: models::NodeMetrics,
        create_successes: u64,
        create_fails: u64,
        sandbox_paused_count: u32,
    ) -> NodeDetail {
        NodeDetail {
            cluster_id,
            version,
            commit,
            id,
            service_instance_id,
            machine_info,
            status,
            sandbox_count,
            metrics,
            create_successes,
            create_fails,
            sandbox_paused_count,
        }
    }
}

/// Converts the NodeDetail value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for NodeDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("clusterID".to_string()),
            Some(self.cluster_id.to_string()),
            Some("version".to_string()),
            Some(self.version.to_string()),
            Some("commit".to_string()),
            Some(self.commit.to_string()),
            Some("id".to_string()),
            Some(self.id.to_string()),
            Some("serviceInstanceID".to_string()),
            Some(self.service_instance_id.to_string()),
            // Skipping machineInfo in query parameter serialization

            // Skipping status in query parameter serialization
            Some("sandboxCount".to_string()),
            Some(self.sandbox_count.to_string()),
            // Skipping metrics in query parameter serialization
            Some("createSuccesses".to_string()),
            Some(self.create_successes.to_string()),
            Some("createFails".to_string()),
            Some(self.create_fails.to_string()),
            Some("sandboxPausedCount".to_string()),
            Some(self.sandbox_paused_count.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a NodeDetail value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for NodeDetail {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub cluster_id: Vec<String>,
            pub version: Vec<String>,
            pub commit: Vec<String>,
            pub id: Vec<String>,
            pub service_instance_id: Vec<String>,
            pub machine_info: Vec<models::MachineInfo>,
            pub status: Vec<models::NodeStatus>,
            pub sandbox_count: Vec<u32>,
            pub metrics: Vec<models::NodeMetrics>,
            pub create_successes: Vec<u64>,
            pub create_fails: Vec<u64>,
            pub sandbox_paused_count: Vec<u32>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing NodeDetail".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "clusterID" => intermediate_rep.cluster_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "version" => intermediate_rep.version.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "commit" => intermediate_rep.commit.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "id" => intermediate_rep.id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "serviceInstanceID" => intermediate_rep.service_instance_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "machineInfo" => intermediate_rep.machine_info.push(
                        <models::MachineInfo as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "status" => intermediate_rep.status.push(
                        <models::NodeStatus as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sandboxCount" => intermediate_rep.sandbox_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "metrics" => intermediate_rep.metrics.push(
                        <models::NodeMetrics as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "createSuccesses" => intermediate_rep.create_successes.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "createFails" => intermediate_rep.create_fails.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sandboxPausedCount" => intermediate_rep.sandbox_paused_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing NodeDetail".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(NodeDetail {
            cluster_id: intermediate_rep
                .cluster_id
                .into_iter()
                .next()
                .ok_or_else(|| "clusterID missing in NodeDetail".to_string())?,
            version: intermediate_rep
                .version
                .into_iter()
                .next()
                .ok_or_else(|| "version missing in NodeDetail".to_string())?,
            commit: intermediate_rep
                .commit
                .into_iter()
                .next()
                .ok_or_else(|| "commit missing in NodeDetail".to_string())?,
            id: intermediate_rep
                .id
                .into_iter()
                .next()
                .ok_or_else(|| "id missing in NodeDetail".to_string())?,
            service_instance_id: intermediate_rep
                .service_instance_id
                .into_iter()
                .next()
                .ok_or_else(|| "serviceInstanceID missing in NodeDetail".to_string())?,
            machine_info: intermediate_rep
                .machine_info
                .into_iter()
                .next()
                .ok_or_else(|| "machineInfo missing in NodeDetail".to_string())?,
            status: intermediate_rep
                .status
                .into_iter()
                .next()
                .ok_or_else(|| "status missing in NodeDetail".to_string())?,
            sandbox_count: intermediate_rep
                .sandbox_count
                .into_iter()
                .next()
                .ok_or_else(|| "sandboxCount missing in NodeDetail".to_string())?,
            metrics: intermediate_rep
                .metrics
                .into_iter()
                .next()
                .ok_or_else(|| "metrics missing in NodeDetail".to_string())?,
            create_successes: intermediate_rep
                .create_successes
                .into_iter()
                .next()
                .ok_or_else(|| "createSuccesses missing in NodeDetail".to_string())?,
            create_fails: intermediate_rep
                .create_fails
                .into_iter()
                .next()
                .ok_or_else(|| "createFails missing in NodeDetail".to_string())?,
            sandbox_paused_count: intermediate_rep
                .sandbox_paused_count
                .into_iter()
                .next()
                .ok_or_else(|| "sandboxPausedCount missing in NodeDetail".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<NodeDetail> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<NodeDetail>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<NodeDetail>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for NodeDetail - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<NodeDetail> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <NodeDetail as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into NodeDetail - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Node metrics
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct NodeMetrics {
    /// Number of allocated CPU cores for the active running sandboxes (excludes paused)
    #[serde(rename = "allocatedCPU")]
    pub allocated_cpu: u32,

    /// Node CPU usage percentage
    #[serde(rename = "cpuPercent")]
    pub cpu_percent: u32,

    /// Total number of CPU cores on the node
    #[serde(rename = "cpuCount")]
    pub cpu_count: u32,

    /// Amount of allocated memory in bytes for the active running sandboxes (excludes paused)
    #[serde(rename = "allocatedMemoryBytes")]
    pub allocated_memory_bytes: u64,

    /// Node memory used in bytes
    #[serde(rename = "memoryUsedBytes")]
    pub memory_used_bytes: u64,

    /// Total node memory in bytes
    #[serde(rename = "memoryTotalBytes")]
    pub memory_total_bytes: u64,

    /// Detailed metrics for each disk/mount point
    #[serde(rename = "disks")]
    #[validate(nested)]
    pub disks: Vec<models::DiskMetrics>,

    /// Sum of CPU reservations across all sandboxes currently in the Paused state
    #[serde(rename = "pausedAllocatedCPU")]
    pub paused_allocated_cpu: u32,

    /// Sum of memory reservations (bytes) across all sandboxes currently in the Paused state
    #[serde(rename = "pausedAllocatedMemoryBytes")]
    pub paused_allocated_memory_bytes: u64,
}

impl NodeMetrics {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        allocated_cpu: u32,
        cpu_percent: u32,
        cpu_count: u32,
        allocated_memory_bytes: u64,
        memory_used_bytes: u64,
        memory_total_bytes: u64,
        disks: Vec<models::DiskMetrics>,
        paused_allocated_cpu: u32,
        paused_allocated_memory_bytes: u64,
    ) -> NodeMetrics {
        NodeMetrics {
            allocated_cpu,
            cpu_percent,
            cpu_count,
            allocated_memory_bytes,
            memory_used_bytes,
            memory_total_bytes,
            disks,
            paused_allocated_cpu,
            paused_allocated_memory_bytes,
        }
    }
}

/// Converts the NodeMetrics value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for NodeMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("allocatedCPU".to_string()),
            Some(self.allocated_cpu.to_string()),
            Some("cpuPercent".to_string()),
            Some(self.cpu_percent.to_string()),
            Some("cpuCount".to_string()),
            Some(self.cpu_count.to_string()),
            Some("allocatedMemoryBytes".to_string()),
            Some(self.allocated_memory_bytes.to_string()),
            Some("memoryUsedBytes".to_string()),
            Some(self.memory_used_bytes.to_string()),
            Some("memoryTotalBytes".to_string()),
            Some(self.memory_total_bytes.to_string()),
            // Skipping disks in query parameter serialization
            Some("pausedAllocatedCPU".to_string()),
            Some(self.paused_allocated_cpu.to_string()),
            Some("pausedAllocatedMemoryBytes".to_string()),
            Some(self.paused_allocated_memory_bytes.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a NodeMetrics value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for NodeMetrics {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub allocated_cpu: Vec<u32>,
            pub cpu_percent: Vec<u32>,
            pub cpu_count: Vec<u32>,
            pub allocated_memory_bytes: Vec<u64>,
            pub memory_used_bytes: Vec<u64>,
            pub memory_total_bytes: Vec<u64>,
            pub disks: Vec<Vec<models::DiskMetrics>>,
            pub paused_allocated_cpu: Vec<u32>,
            pub paused_allocated_memory_bytes: Vec<u64>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing NodeMetrics".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "allocatedCPU" => intermediate_rep.allocated_cpu.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cpuPercent" => intermediate_rep.cpu_percent.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cpuCount" => intermediate_rep.cpu_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "allocatedMemoryBytes" => intermediate_rep.allocated_memory_bytes.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "memoryUsedBytes" => intermediate_rep.memory_used_bytes.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "memoryTotalBytes" => intermediate_rep.memory_total_bytes.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "disks" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in NodeMetrics"
                                .to_string(),
                        );
                    }
                    #[allow(clippy::redundant_clone)]
                    "pausedAllocatedCPU" => intermediate_rep.paused_allocated_cpu.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "pausedAllocatedMemoryBytes" => {
                        intermediate_rep.paused_allocated_memory_bytes.push(
                            <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                        )
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing NodeMetrics".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(NodeMetrics {
            allocated_cpu: intermediate_rep
                .allocated_cpu
                .into_iter()
                .next()
                .ok_or_else(|| "allocatedCPU missing in NodeMetrics".to_string())?,
            cpu_percent: intermediate_rep
                .cpu_percent
                .into_iter()
                .next()
                .ok_or_else(|| "cpuPercent missing in NodeMetrics".to_string())?,
            cpu_count: intermediate_rep
                .cpu_count
                .into_iter()
                .next()
                .ok_or_else(|| "cpuCount missing in NodeMetrics".to_string())?,
            allocated_memory_bytes: intermediate_rep
                .allocated_memory_bytes
                .into_iter()
                .next()
                .ok_or_else(|| "allocatedMemoryBytes missing in NodeMetrics".to_string())?,
            memory_used_bytes: intermediate_rep
                .memory_used_bytes
                .into_iter()
                .next()
                .ok_or_else(|| "memoryUsedBytes missing in NodeMetrics".to_string())?,
            memory_total_bytes: intermediate_rep
                .memory_total_bytes
                .into_iter()
                .next()
                .ok_or_else(|| "memoryTotalBytes missing in NodeMetrics".to_string())?,
            disks: intermediate_rep
                .disks
                .into_iter()
                .next()
                .ok_or_else(|| "disks missing in NodeMetrics".to_string())?,
            paused_allocated_cpu: intermediate_rep
                .paused_allocated_cpu
                .into_iter()
                .next()
                .ok_or_else(|| "pausedAllocatedCPU missing in NodeMetrics".to_string())?,
            paused_allocated_memory_bytes: intermediate_rep
                .paused_allocated_memory_bytes
                .into_iter()
                .next()
                .ok_or_else(|| "pausedAllocatedMemoryBytes missing in NodeMetrics".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<NodeMetrics> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<NodeMetrics>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<NodeMetrics>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for NodeMetrics - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<NodeMetrics> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <NodeMetrics as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into NodeMetrics - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Status of the node. - draining: the node is bound to be shut down. It will not accept new sandboxes and will stop once all existing sandboxes are done.
/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types, clippy::large_enum_variant)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum NodeStatus {
    #[serde(rename = "ready")]
    NodeStatusReady,
    #[serde(rename = "draining")]
    NodeStatusDraining,
    #[serde(rename = "connecting")]
    NodeStatusConnecting,
    #[serde(rename = "unhealthy")]
    NodeStatusUnhealthy,
}

impl validator::Validate for NodeStatus {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            NodeStatus::NodeStatusReady => write!(f, "ready"),
            NodeStatus::NodeStatusDraining => write!(f, "draining"),
            NodeStatus::NodeStatusConnecting => write!(f, "connecting"),
            NodeStatus::NodeStatusUnhealthy => write!(f, "unhealthy"),
        }
    }
}

impl std::str::FromStr for NodeStatus {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "ready" => std::result::Result::Ok(NodeStatus::NodeStatusReady),
            "draining" => std::result::Result::Ok(NodeStatus::NodeStatusDraining),
            "connecting" => std::result::Result::Ok(NodeStatus::NodeStatusConnecting),
            "unhealthy" => std::result::Result::Ok(NodeStatus::NodeStatusUnhealthy),
            _ => std::result::Result::Err(format!(r#"Value not valid: {s}"#)),
        }
    }
}

/// Sort direction
/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types, clippy::large_enum_variant)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum OrderDirection {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

impl validator::Validate for OrderDirection {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

impl std::fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            OrderDirection::Asc => write!(f, "asc"),
            OrderDirection::Desc => write!(f, "desc"),
        }
    }
}

impl std::str::FromStr for OrderDirection {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "asc" => std::result::Result::Ok(OrderDirection::Asc),
            "desc" => std::result::Result::Ok(OrderDirection::Desc),
            _ => std::result::Result::Err(format!(r#"Value not valid: {s}"#)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ResumedSandbox {
    /// Time to live for the sandbox in seconds.
    #[serde(rename = "timeout")]
    #[validate(range(min = 0u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,
}

impl ResumedSandbox {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> ResumedSandbox {
        ResumedSandbox { timeout: Some(15) }
    }
}

/// Converts the ResumedSandbox value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for ResumedSandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            self.timeout
                .as_ref()
                .map(|timeout| ["timeout".to_string(), timeout.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a ResumedSandbox value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for ResumedSandbox {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub timeout: Vec<u32>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing ResumedSandbox".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "timeout" => intermediate_rep.timeout.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing ResumedSandbox".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(ResumedSandbox {
            timeout: intermediate_rep.timeout.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<ResumedSandbox> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<ResumedSandbox>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<ResumedSandbox>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for ResumedSandbox - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<ResumedSandbox> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <ResumedSandbox as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into ResumedSandbox - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Sandbox {
    /// Identifier of the template from which is the sandbox created
    #[serde(rename = "templateID")]
    #[validate(custom(function = "check_xss_string"))]
    pub template_id: String,

    /// Identifier of the sandbox
    #[serde(rename = "sandboxID")]
    #[validate(custom(function = "check_xss_string"))]
    pub sandbox_id: String,

    /// Alias of the template
    #[serde(rename = "alias")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,

    /// Identifier of the client
    #[serde(rename = "clientID")]
    #[validate(custom(function = "check_xss_string"))]
    pub client_id: String,

    /// Version of the envd running in the sandbox
    #[serde(rename = "envdVersion")]
    #[validate(custom(function = "check_xss_string"))]
    pub envd_version: String,

    /// Access token used for envd communication
    #[serde(rename = "envdAccessToken")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envd_access_token: Option<String>,

    /// Token required for accessing sandbox via proxy.
    #[serde(rename = "trafficAccessToken")]
    #[serde(deserialize_with = "deserialize_optional_nullable")]
    #[serde(default = "default_optional_nullable")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traffic_access_token: Option<Nullable<String>>,

    /// Base domain where the sandbox traffic is accessible
    #[serde(rename = "domain")]
    #[serde(deserialize_with = "deserialize_optional_nullable")]
    #[serde(default = "default_optional_nullable")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<Nullable<String>>,
}

impl Sandbox {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        template_id: String,
        sandbox_id: String,
        client_id: String,
        envd_version: String,
    ) -> Sandbox {
        Sandbox {
            template_id,
            sandbox_id,
            alias: None,
            client_id,
            envd_version,
            envd_access_token: None,
            traffic_access_token: None,
            domain: None,
        }
    }
}

/// Converts the Sandbox value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for Sandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("templateID".to_string()),
            Some(self.template_id.to_string()),
            Some("sandboxID".to_string()),
            Some(self.sandbox_id.to_string()),
            self.alias
                .as_ref()
                .map(|alias| ["alias".to_string(), alias.to_string()].join(",")),
            Some("clientID".to_string()),
            Some(self.client_id.to_string()),
            Some("envdVersion".to_string()),
            Some(self.envd_version.to_string()),
            self.envd_access_token.as_ref().map(|envd_access_token| {
                ["envdAccessToken".to_string(), envd_access_token.to_string()].join(",")
            }),
            self.traffic_access_token
                .as_ref()
                .map(|traffic_access_token| {
                    [
                        "trafficAccessToken".to_string(),
                        traffic_access_token
                            .as_ref()
                            .map_or("null".to_string(), |x| x.to_string()),
                    ]
                    .join(",")
                }),
            self.domain.as_ref().map(|domain| {
                [
                    "domain".to_string(),
                    domain
                        .as_ref()
                        .map_or("null".to_string(), |x| x.to_string()),
                ]
                .join(",")
            }),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Sandbox value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Sandbox {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub template_id: Vec<String>,
            pub sandbox_id: Vec<String>,
            pub alias: Vec<String>,
            pub client_id: Vec<String>,
            pub envd_version: Vec<String>,
            pub envd_access_token: Vec<String>,
            pub traffic_access_token: Vec<String>,
            pub domain: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Sandbox".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "templateID" => intermediate_rep.template_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sandboxID" => intermediate_rep.sandbox_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "alias" => intermediate_rep.alias.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "clientID" => intermediate_rep.client_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "envdVersion" => intermediate_rep.envd_version.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "envdAccessToken" => intermediate_rep.envd_access_token.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "trafficAccessToken" => {
                        return std::result::Result::Err(
                            "Parsing a nullable type in this style is not supported in Sandbox"
                                .to_string(),
                        );
                    }
                    "domain" => {
                        return std::result::Result::Err(
                            "Parsing a nullable type in this style is not supported in Sandbox"
                                .to_string(),
                        );
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Sandbox".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Sandbox {
            template_id: intermediate_rep
                .template_id
                .into_iter()
                .next()
                .ok_or_else(|| "templateID missing in Sandbox".to_string())?,
            sandbox_id: intermediate_rep
                .sandbox_id
                .into_iter()
                .next()
                .ok_or_else(|| "sandboxID missing in Sandbox".to_string())?,
            alias: intermediate_rep.alias.into_iter().next(),
            client_id: intermediate_rep
                .client_id
                .into_iter()
                .next()
                .ok_or_else(|| "clientID missing in Sandbox".to_string())?,
            envd_version: intermediate_rep
                .envd_version
                .into_iter()
                .next()
                .ok_or_else(|| "envdVersion missing in Sandbox".to_string())?,
            envd_access_token: intermediate_rep.envd_access_token.into_iter().next(),
            traffic_access_token: std::result::Result::Err(
                "Nullable types not supported in Sandbox".to_string(),
            )?,
            domain: std::result::Result::Err(
                "Nullable types not supported in Sandbox".to_string(),
            )?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Sandbox> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<Sandbox>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Sandbox>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for Sandbox - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<Sandbox> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <Sandbox as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into Sandbox - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Auto-resume configuration for paused sandboxes.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxAutoResumeConfig {
    /// Auto-resume enabled flag for paused sandboxes. Default false.
    #[serde(rename = "enabled")]
    pub enabled: bool,
}

impl SandboxAutoResumeConfig {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> SandboxAutoResumeConfig {
        SandboxAutoResumeConfig { enabled: false }
    }
}

/// Converts the SandboxAutoResumeConfig value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxAutoResumeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> =
            vec![Some("enabled".to_string()), Some(self.enabled.to_string())];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxAutoResumeConfig value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxAutoResumeConfig {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub enabled: Vec<bool>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxAutoResumeConfig".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "enabled" => intermediate_rep.enabled.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxAutoResumeConfig".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxAutoResumeConfig {
            enabled: intermediate_rep
                .enabled
                .into_iter()
                .next()
                .ok_or_else(|| "enabled missing in SandboxAutoResumeConfig".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxAutoResumeConfig> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxAutoResumeConfig>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxAutoResumeConfig>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxAutoResumeConfig - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxAutoResumeConfig> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxAutoResumeConfig as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxAutoResumeConfig - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Auto-resume enabled flag for paused sandboxes. Default false.
#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxAutoResumeEnabled(pub bool);

impl validator::Validate for SandboxAutoResumeEnabled {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

impl std::convert::From<bool> for SandboxAutoResumeEnabled {
    fn from(x: bool) -> Self {
        SandboxAutoResumeEnabled(x)
    }
}

impl std::convert::From<SandboxAutoResumeEnabled> for bool {
    fn from(x: SandboxAutoResumeEnabled) -> Self {
        x.0
    }
}

impl std::ops::Deref for SandboxAutoResumeEnabled {
    type Target = bool;
    fn deref(&self) -> &bool {
        &self.0
    }
}

impl std::ops::DerefMut for SandboxAutoResumeEnabled {
    fn deref_mut(&mut self) -> &mut bool {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxCpuAffinity {
    #[serde(rename = "sandboxID")]
    #[validate(custom(function = "check_xss_string"))]
    pub sandbox_id: String,

    #[serde(rename = "vcpu")]
    #[validate(custom(function = "check_xss_string"))]
    pub vcpu: String,

    /// Normalized online host CPUs that were applied.
    #[serde(rename = "cores")]
    #[validate(custom(function = "check_xss_string"))]
    pub cores: String,

    /// Normalized requested CPUs that were offline, or an empty string.
    #[serde(rename = "ignoredOfflineCores")]
    #[validate(custom(function = "check_xss_string"))]
    pub ignored_offline_cores: String,

    #[serde(rename = "boundThreadCount")]
    #[validate(range(min = 1u32))]
    pub bound_thread_count: u32,
}

impl SandboxCpuAffinity {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        sandbox_id: String,
        vcpu: String,
        cores: String,
        ignored_offline_cores: String,
        bound_thread_count: u32,
    ) -> SandboxCpuAffinity {
        SandboxCpuAffinity {
            sandbox_id,
            vcpu,
            cores,
            ignored_offline_cores,
            bound_thread_count,
        }
    }
}

/// Converts the SandboxCpuAffinity value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxCpuAffinity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("sandboxID".to_string()),
            Some(self.sandbox_id.to_string()),
            Some("vcpu".to_string()),
            Some(self.vcpu.to_string()),
            Some("cores".to_string()),
            Some(self.cores.to_string()),
            Some("ignoredOfflineCores".to_string()),
            Some(self.ignored_offline_cores.to_string()),
            Some("boundThreadCount".to_string()),
            Some(self.bound_thread_count.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxCpuAffinity value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxCpuAffinity {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub sandbox_id: Vec<String>,
            pub vcpu: Vec<String>,
            pub cores: Vec<String>,
            pub ignored_offline_cores: Vec<String>,
            pub bound_thread_count: Vec<u32>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxCpuAffinity".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "sandboxID" => intermediate_rep.sandbox_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "vcpu" => intermediate_rep.vcpu.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cores" => intermediate_rep.cores.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "ignoredOfflineCores" => intermediate_rep.ignored_offline_cores.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "boundThreadCount" => intermediate_rep.bound_thread_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxCpuAffinity".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxCpuAffinity {
            sandbox_id: intermediate_rep
                .sandbox_id
                .into_iter()
                .next()
                .ok_or_else(|| "sandboxID missing in SandboxCpuAffinity".to_string())?,
            vcpu: intermediate_rep
                .vcpu
                .into_iter()
                .next()
                .ok_or_else(|| "vcpu missing in SandboxCpuAffinity".to_string())?,
            cores: intermediate_rep
                .cores
                .into_iter()
                .next()
                .ok_or_else(|| "cores missing in SandboxCpuAffinity".to_string())?,
            ignored_offline_cores: intermediate_rep
                .ignored_offline_cores
                .into_iter()
                .next()
                .ok_or_else(|| "ignoredOfflineCores missing in SandboxCpuAffinity".to_string())?,
            bound_thread_count: intermediate_rep
                .bound_thread_count
                .into_iter()
                .next()
                .ok_or_else(|| "boundThreadCount missing in SandboxCpuAffinity".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxCpuAffinity> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxCpuAffinity>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxCpuAffinity>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxCpuAffinity - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxCpuAffinity> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxCpuAffinity as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxCpuAffinity - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxCpuAffinityRequest {
    /// Firecracker vCPU list, or '*' to select every current Firecracker thread.
    #[serde(rename = "vcpu")]
    #[validate(length(max = 16384), custom(function = "check_xss_string"))]
    pub vcpu: String,

    /// Host CPU list with optional range strides such as 0-10:2. Supported CPU IDs are 0-1023. Offline CPUs are ignored unless the online intersection is empty.
    #[serde(rename = "core")]
    #[validate(length(max = 16384), custom(function = "check_xss_string"))]
    pub core: String,
}

impl SandboxCpuAffinityRequest {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(vcpu: String, core: String) -> SandboxCpuAffinityRequest {
        SandboxCpuAffinityRequest { vcpu, core }
    }
}

/// Converts the SandboxCpuAffinityRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxCpuAffinityRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("vcpu".to_string()),
            Some(self.vcpu.to_string()),
            Some("core".to_string()),
            Some(self.core.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxCpuAffinityRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxCpuAffinityRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub vcpu: Vec<String>,
            pub core: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxCpuAffinityRequest".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "vcpu" => intermediate_rep.vcpu.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "core" => intermediate_rep.core.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxCpuAffinityRequest".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxCpuAffinityRequest {
            vcpu: intermediate_rep
                .vcpu
                .into_iter()
                .next()
                .ok_or_else(|| "vcpu missing in SandboxCpuAffinityRequest".to_string())?,
            core: intermediate_rep
                .core
                .into_iter()
                .next()
                .ok_or_else(|| "core missing in SandboxCpuAffinityRequest".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxCpuAffinityRequest> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxCpuAffinityRequest>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxCpuAffinityRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxCpuAffinityRequest - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxCpuAffinityRequest> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxCpuAffinityRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxCpuAffinityRequest - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxDetail {
    /// Identifier of the template from which is the sandbox created
    #[serde(rename = "templateID")]
    #[validate(custom(function = "check_xss_string"))]
    pub template_id: String,

    /// Alias of the template
    #[serde(rename = "alias")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,

    /// Identifier of the sandbox
    #[serde(rename = "sandboxID")]
    #[validate(custom(function = "check_xss_string"))]
    pub sandbox_id: String,

    /// Identifier of the client
    #[serde(rename = "clientID")]
    #[validate(custom(function = "check_xss_string"))]
    pub client_id: String,

    /// Time when the sandbox was started
    #[serde(rename = "startedAt")]
    pub started_at: chrono::DateTime<chrono::Utc>,

    /// Time when the sandbox will expire
    #[serde(rename = "endAt")]
    pub end_at: chrono::DateTime<chrono::Utc>,

    /// Version of the envd running in the sandbox
    #[serde(rename = "envdVersion")]
    #[validate(custom(function = "check_xss_string"))]
    pub envd_version: String,

    /// Access token used for envd communication
    #[serde(rename = "envdAccessToken")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envd_access_token: Option<String>,

    /// Whether internet access was explicitly enabled or disabled for the sandbox. Null means it was not explicitly set.
    #[serde(rename = "allowInternetAccess")]
    #[serde(deserialize_with = "deserialize_optional_nullable")]
    #[serde(default = "default_optional_nullable")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_internet_access: Option<Nullable<bool>>,

    /// Base domain where the sandbox traffic is accessible
    #[serde(rename = "domain")]
    #[serde(deserialize_with = "deserialize_optional_nullable")]
    #[serde(default = "default_optional_nullable")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<Nullable<String>>,

    /// CPU cores for the sandbox
    #[serde(rename = "cpuCount")]
    #[validate(range(min = 1u32))]
    pub cpu_count: u32,

    /// Memory for the sandbox in MiB
    #[serde(rename = "memoryMB")]
    #[validate(range(min = 128u32))]
    pub memory_mb: u32,

    /// Disk size for the sandbox in MiB
    #[serde(rename = "diskSizeMB")]
    #[validate(range(min = 0u32))]
    pub disk_size_mb: u32,

    #[serde(rename = "metadata")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,

    #[serde(rename = "state")]
    #[validate(nested)]
    pub state: models::SandboxState,

    #[serde(rename = "network")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<models::SandboxNetworkConfig>,

    #[serde(rename = "lifecycle")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<models::SandboxLifecycle>,

    /// Map of absolute guest mount paths to volume IDs or names.
    #[serde(rename = "volumeMounts")]
    #[validate(custom(function = "check_xss_map_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<std::collections::HashMap<String, String>>,
}

impl SandboxDetail {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        template_id: String,
        sandbox_id: String,
        client_id: String,
        started_at: chrono::DateTime<chrono::Utc>,
        end_at: chrono::DateTime<chrono::Utc>,
        envd_version: String,
        cpu_count: u32,
        memory_mb: u32,
        disk_size_mb: u32,
        state: models::SandboxState,
    ) -> SandboxDetail {
        SandboxDetail {
            template_id,
            alias: None,
            sandbox_id,
            client_id,
            started_at,
            end_at,
            envd_version,
            envd_access_token: None,
            allow_internet_access: None,
            domain: None,
            cpu_count,
            memory_mb,
            disk_size_mb,
            metadata: None,
            state,
            network: None,
            lifecycle: None,
            volume_mounts: None,
        }
    }
}

/// Converts the SandboxDetail value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("templateID".to_string()),
            Some(self.template_id.to_string()),
            self.alias
                .as_ref()
                .map(|alias| ["alias".to_string(), alias.to_string()].join(",")),
            Some("sandboxID".to_string()),
            Some(self.sandbox_id.to_string()),
            Some("clientID".to_string()),
            Some(self.client_id.to_string()),
            // Skipping startedAt in query parameter serialization

            // Skipping endAt in query parameter serialization
            Some("envdVersion".to_string()),
            Some(self.envd_version.to_string()),
            self.envd_access_token.as_ref().map(|envd_access_token| {
                ["envdAccessToken".to_string(), envd_access_token.to_string()].join(",")
            }),
            self.allow_internet_access
                .as_ref()
                .map(|allow_internet_access| {
                    [
                        "allowInternetAccess".to_string(),
                        allow_internet_access
                            .as_ref()
                            .map_or("null".to_string(), |x| x.to_string()),
                    ]
                    .join(",")
                }),
            self.domain.as_ref().map(|domain| {
                [
                    "domain".to_string(),
                    domain
                        .as_ref()
                        .map_or("null".to_string(), |x| x.to_string()),
                ]
                .join(",")
            }),
            Some("cpuCount".to_string()),
            Some(self.cpu_count.to_string()),
            Some("memoryMB".to_string()),
            Some(self.memory_mb.to_string()),
            Some("diskSizeMB".to_string()),
            Some(self.disk_size_mb.to_string()),
            // Skipping metadata in query parameter serialization

            // Skipping state in query parameter serialization

            // Skipping network in query parameter serialization

            // Skipping lifecycle in query parameter serialization

            // Skipping volumeMounts in query parameter serialization
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxDetail value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxDetail {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub template_id: Vec<String>,
            pub alias: Vec<String>,
            pub sandbox_id: Vec<String>,
            pub client_id: Vec<String>,
            pub started_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub end_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub envd_version: Vec<String>,
            pub envd_access_token: Vec<String>,
            pub allow_internet_access: Vec<bool>,
            pub domain: Vec<String>,
            pub cpu_count: Vec<u32>,
            pub memory_mb: Vec<u32>,
            pub disk_size_mb: Vec<u32>,
            pub metadata: Vec<std::collections::HashMap<String, String>>,
            pub state: Vec<models::SandboxState>,
            pub network: Vec<models::SandboxNetworkConfig>,
            pub lifecycle: Vec<models::SandboxLifecycle>,
            pub volume_mounts: Vec<std::collections::HashMap<String, String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxDetail".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "templateID" => intermediate_rep.template_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "alias" => intermediate_rep.alias.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sandboxID" => intermediate_rep.sandbox_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "clientID" => intermediate_rep.client_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "startedAt" => intermediate_rep.started_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "endAt" => intermediate_rep.end_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "envdVersion" => intermediate_rep.envd_version.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "envdAccessToken" => intermediate_rep.envd_access_token.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "allowInternetAccess" => return std::result::Result::Err(
                        "Parsing a nullable type in this style is not supported in SandboxDetail"
                            .to_string(),
                    ),
                    "domain" => return std::result::Result::Err(
                        "Parsing a nullable type in this style is not supported in SandboxDetail"
                            .to_string(),
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cpuCount" => intermediate_rep.cpu_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "memoryMB" => intermediate_rep.memory_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "diskSizeMB" => intermediate_rep.disk_size_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "metadata" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in SandboxDetail"
                                .to_string(),
                        );
                    }
                    #[allow(clippy::redundant_clone)]
                    "state" => intermediate_rep.state.push(
                        <models::SandboxState as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "network" => intermediate_rep.network.push(
                        <models::SandboxNetworkConfig as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "lifecycle" => intermediate_rep.lifecycle.push(
                        <models::SandboxLifecycle as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    "volumeMounts" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in SandboxDetail"
                                .to_string(),
                        );
                    }
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxDetail".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxDetail {
            template_id: intermediate_rep
                .template_id
                .into_iter()
                .next()
                .ok_or_else(|| "templateID missing in SandboxDetail".to_string())?,
            alias: intermediate_rep.alias.into_iter().next(),
            sandbox_id: intermediate_rep
                .sandbox_id
                .into_iter()
                .next()
                .ok_or_else(|| "sandboxID missing in SandboxDetail".to_string())?,
            client_id: intermediate_rep
                .client_id
                .into_iter()
                .next()
                .ok_or_else(|| "clientID missing in SandboxDetail".to_string())?,
            started_at: intermediate_rep
                .started_at
                .into_iter()
                .next()
                .ok_or_else(|| "startedAt missing in SandboxDetail".to_string())?,
            end_at: intermediate_rep
                .end_at
                .into_iter()
                .next()
                .ok_or_else(|| "endAt missing in SandboxDetail".to_string())?,
            envd_version: intermediate_rep
                .envd_version
                .into_iter()
                .next()
                .ok_or_else(|| "envdVersion missing in SandboxDetail".to_string())?,
            envd_access_token: intermediate_rep.envd_access_token.into_iter().next(),
            allow_internet_access: std::result::Result::Err(
                "Nullable types not supported in SandboxDetail".to_string(),
            )?,
            domain: std::result::Result::Err(
                "Nullable types not supported in SandboxDetail".to_string(),
            )?,
            cpu_count: intermediate_rep
                .cpu_count
                .into_iter()
                .next()
                .ok_or_else(|| "cpuCount missing in SandboxDetail".to_string())?,
            memory_mb: intermediate_rep
                .memory_mb
                .into_iter()
                .next()
                .ok_or_else(|| "memoryMB missing in SandboxDetail".to_string())?,
            disk_size_mb: intermediate_rep
                .disk_size_mb
                .into_iter()
                .next()
                .ok_or_else(|| "diskSizeMB missing in SandboxDetail".to_string())?,
            metadata: intermediate_rep.metadata.into_iter().next(),
            state: intermediate_rep
                .state
                .into_iter()
                .next()
                .ok_or_else(|| "state missing in SandboxDetail".to_string())?,
            network: intermediate_rep.network.into_iter().next(),
            lifecycle: intermediate_rep.lifecycle.into_iter().next(),
            volume_mounts: intermediate_rep.volume_mounts.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxDetail> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxDetail>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxDetail>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxDetail - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxDetail> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxDetail as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxDetail - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxForkRequest {
    /// Time to live for the new forked sandboxes in seconds. When omitted, each fork inherits the source sandbox timeout.
    #[serde(rename = "timeout")]
    #[validate(range(min = 0u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,

    /// Number of forked sandboxes to create. All forks boot from the same snapshot, so the snapshot is captured once regardless of count. Each fork succeeds or fails independently; the outcome of each is reported in its entry of the response list.
    #[serde(rename = "count")]
    #[validate(range(min = 1u32, max = 100u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
}

impl SandboxForkRequest {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> SandboxForkRequest {
        SandboxForkRequest {
            timeout: None,
            count: Some(1),
        }
    }
}

/// Converts the SandboxForkRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxForkRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            self.timeout
                .as_ref()
                .map(|timeout| ["timeout".to_string(), timeout.to_string()].join(",")),
            self.count
                .as_ref()
                .map(|count| ["count".to_string(), count.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxForkRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxForkRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub timeout: Vec<u32>,
            pub count: Vec<u32>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxForkRequest".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "timeout" => intermediate_rep.timeout.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "count" => intermediate_rep.count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxForkRequest".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxForkRequest {
            timeout: intermediate_rep.timeout.into_iter().next(),
            count: intermediate_rep.count.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxForkRequest> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxForkRequest>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxForkRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxForkRequest - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxForkRequest> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxForkRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxForkRequest - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Result of one requested fork. Exactly one of sandbox or error is set: sandbox when the fork started successfully, error when it failed to start.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxForkResult {
    #[serde(rename = "sandbox")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<models::Sandbox>,

    #[serde(rename = "error")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<models::Error>,
}

impl SandboxForkResult {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> SandboxForkResult {
        SandboxForkResult {
            sandbox: None,
            error: None,
        }
    }
}

/// Converts the SandboxForkResult value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxForkResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            // Skipping sandbox in query parameter serialization

            // Skipping error in query parameter serialization

        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxForkResult value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxForkResult {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub sandbox: Vec<models::Sandbox>,
            pub error: Vec<models::Error>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxForkResult".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "sandbox" => intermediate_rep.sandbox.push(
                        <models::Sandbox as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "error" => intermediate_rep.error.push(
                        <models::Error as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxForkResult".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxForkResult {
            sandbox: intermediate_rep.sandbox.into_iter().next(),
            error: intermediate_rep.error.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxForkResult> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxForkResult>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxForkResult>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxForkResult - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxForkResult> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxForkResult as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxForkResult - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Sandbox lifecycle policy returned by sandbox info.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxLifecycle {
    /// Whether the sandbox can auto-resume.
    #[serde(rename = "autoResume")]
    pub auto_resume: bool,

    #[serde(rename = "onTimeout")]
    #[validate(nested)]
    pub on_timeout: models::SandboxOnTimeout,
}

impl SandboxLifecycle {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(auto_resume: bool, on_timeout: models::SandboxOnTimeout) -> SandboxLifecycle {
        SandboxLifecycle {
            auto_resume,
            on_timeout,
        }
    }
}

/// Converts the SandboxLifecycle value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("autoResume".to_string()),
            Some(self.auto_resume.to_string()),
            // Skipping onTimeout in query parameter serialization
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxLifecycle value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxLifecycle {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub auto_resume: Vec<bool>,
            pub on_timeout: Vec<models::SandboxOnTimeout>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxLifecycle".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "autoResume" => intermediate_rep.auto_resume.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "onTimeout" => intermediate_rep.on_timeout.push(
                        <models::SandboxOnTimeout as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxLifecycle".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxLifecycle {
            auto_resume: intermediate_rep
                .auto_resume
                .into_iter()
                .next()
                .ok_or_else(|| "autoResume missing in SandboxLifecycle".to_string())?,
            on_timeout: intermediate_rep
                .on_timeout
                .into_iter()
                .next()
                .ok_or_else(|| "onTimeout missing in SandboxLifecycle".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxLifecycle> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxLifecycle>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxLifecycle>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxLifecycle - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxLifecycle> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxLifecycle as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxLifecycle - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxNetworkConfig {
    /// Specify if the sandbox URLs should be accessible only with authentication.
    #[serde(rename = "allowPublicTraffic")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_public_traffic: Option<bool>,

    /// List of allowed destinations for egress traffic. Each entry can be a CIDR block (e.g. \"8.8.8.8/32\"), a bare IP address (e.g. \"8.8.8.8\"), or a domain name (e.g. \"example.com\", \"*.example.com\"). Allowed entries always take precedence over denied entries.
    #[serde(rename = "allowOut")]
    #[validate(custom(function = "check_xss_vec_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_out: Option<Vec<String>>,

    /// List of denied CIDR blocks or IP addresses for egress traffic. Domain names are not supported for deny rules.
    #[serde(rename = "denyOut")]
    #[validate(custom(function = "check_xss_vec_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deny_out: Option<Vec<String>>,

    /// Specify host mask which will be used for all sandbox requests
    #[serde(rename = "maskRequestHost")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mask_request_host: Option<String>,
}

impl SandboxNetworkConfig {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> SandboxNetworkConfig {
        SandboxNetworkConfig {
            allow_public_traffic: Some(true),
            allow_out: None,
            deny_out: None,
            mask_request_host: None,
        }
    }
}

/// Converts the SandboxNetworkConfig value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxNetworkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            self.allow_public_traffic
                .as_ref()
                .map(|allow_public_traffic| {
                    [
                        "allowPublicTraffic".to_string(),
                        allow_public_traffic.to_string(),
                    ]
                    .join(",")
                }),
            self.allow_out.as_ref().map(|allow_out| {
                [
                    "allowOut".to_string(),
                    allow_out
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ]
                .join(",")
            }),
            self.deny_out.as_ref().map(|deny_out| {
                [
                    "denyOut".to_string(),
                    deny_out
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ]
                .join(",")
            }),
            self.mask_request_host.as_ref().map(|mask_request_host| {
                ["maskRequestHost".to_string(), mask_request_host.to_string()].join(",")
            }),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxNetworkConfig value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxNetworkConfig {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub allow_public_traffic: Vec<bool>,
            pub allow_out: Vec<Vec<String>>,
            pub deny_out: Vec<Vec<String>>,
            pub mask_request_host: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxNetworkConfig".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "allowPublicTraffic" => intermediate_rep.allow_public_traffic.push(<bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "allowOut" => return std::result::Result::Err("Parsing a container in this style is not supported in SandboxNetworkConfig".to_string()),
                    "denyOut" => return std::result::Result::Err("Parsing a container in this style is not supported in SandboxNetworkConfig".to_string()),
                    #[allow(clippy::redundant_clone)]
                    "maskRequestHost" => intermediate_rep.mask_request_host.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    _ => return std::result::Result::Err("Unexpected key while parsing SandboxNetworkConfig".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxNetworkConfig {
            allow_public_traffic: intermediate_rep.allow_public_traffic.into_iter().next(),
            allow_out: intermediate_rep.allow_out.into_iter().next(),
            deny_out: intermediate_rep.deny_out.into_iter().next(),
            mask_request_host: intermediate_rep.mask_request_host.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxNetworkConfig> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxNetworkConfig>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxNetworkConfig>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxNetworkConfig - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxNetworkConfig> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxNetworkConfig as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxNetworkConfig - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Network configuration update for a running sandbox. Replaces the current egress rules with the provided configuration. Omitting a field clears it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxNetworkUpdateConfig {
    /// List of allowed destinations for egress traffic. Each entry can be a CIDR block (e.g. \"8.8.8.8/32\"), a bare IP address (e.g. \"8.8.8.8\"), or a domain name (e.g. \"example.com\", \"*.example.com\"). Allowed entries always take precedence over denied entries.
    #[serde(rename = "allowOut")]
    #[validate(custom(function = "check_xss_vec_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_out: Option<Vec<String>>,

    /// List of denied CIDR blocks or IP addresses for egress traffic. Domain names are not supported for deny rules.
    #[serde(rename = "denyOut")]
    #[validate(custom(function = "check_xss_vec_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deny_out: Option<Vec<String>>,

    /// Allow sandbox to access the internet. When set to false, it behaves the same as specifying denyOut to 0.0.0.0/0 in the network config.
    #[serde(rename = "allow_internet_access")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_internet_access: Option<bool>,
}

impl SandboxNetworkUpdateConfig {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> SandboxNetworkUpdateConfig {
        SandboxNetworkUpdateConfig {
            allow_out: None,
            deny_out: None,
            allow_internet_access: None,
        }
    }
}

/// Converts the SandboxNetworkUpdateConfig value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxNetworkUpdateConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            self.allow_out.as_ref().map(|allow_out| {
                [
                    "allowOut".to_string(),
                    allow_out
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ]
                .join(",")
            }),
            self.deny_out.as_ref().map(|deny_out| {
                [
                    "denyOut".to_string(),
                    deny_out
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ]
                .join(",")
            }),
            self.allow_internet_access
                .as_ref()
                .map(|allow_internet_access| {
                    [
                        "allow_internet_access".to_string(),
                        allow_internet_access.to_string(),
                    ]
                    .join(",")
                }),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxNetworkUpdateConfig value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxNetworkUpdateConfig {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub allow_out: Vec<Vec<String>>,
            pub deny_out: Vec<Vec<String>>,
            pub allow_internet_access: Vec<bool>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxNetworkUpdateConfig".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "allowOut" => return std::result::Result::Err("Parsing a container in this style is not supported in SandboxNetworkUpdateConfig".to_string()),
                    "denyOut" => return std::result::Result::Err("Parsing a container in this style is not supported in SandboxNetworkUpdateConfig".to_string()),
                    #[allow(clippy::redundant_clone)]
                    "allow_internet_access" => intermediate_rep.allow_internet_access.push(<bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    _ => return std::result::Result::Err("Unexpected key while parsing SandboxNetworkUpdateConfig".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxNetworkUpdateConfig {
            allow_out: intermediate_rep.allow_out.into_iter().next(),
            deny_out: intermediate_rep.deny_out.into_iter().next(),
            allow_internet_access: intermediate_rep.allow_internet_access.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxNetworkUpdateConfig> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxNetworkUpdateConfig>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxNetworkUpdateConfig>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxNetworkUpdateConfig - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxNetworkUpdateConfig> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxNetworkUpdateConfig as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxNetworkUpdateConfig - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Action taken when the sandbox times out.
/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types, clippy::large_enum_variant)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum SandboxOnTimeout {
    #[serde(rename = "kill")]
    Kill,
    #[serde(rename = "pause")]
    Pause,
}

impl validator::Validate for SandboxOnTimeout {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

impl std::fmt::Display for SandboxOnTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            SandboxOnTimeout::Kill => write!(f, "kill"),
            SandboxOnTimeout::Pause => write!(f, "pause"),
        }
    }
}

impl std::str::FromStr for SandboxOnTimeout {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kill" => std::result::Result::Ok(SandboxOnTimeout::Kill),
            "pause" => std::result::Result::Ok(SandboxOnTimeout::Pause),
            _ => std::result::Result::Err(format!(r#"Value not valid: {s}"#)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxRefreshRequest {
    /// Duration for which the sandbox should be kept alive in seconds
    #[serde(rename = "duration")]
    #[validate(range(min = 0u16, max = 3600u16))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<u16>,
}

impl SandboxRefreshRequest {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> SandboxRefreshRequest {
        SandboxRefreshRequest { duration: None }
    }
}

/// Converts the SandboxRefreshRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxRefreshRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            self.duration
                .as_ref()
                .map(|duration| ["duration".to_string(), duration.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxRefreshRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxRefreshRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub duration: Vec<u16>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxRefreshRequest".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "duration" => intermediate_rep.duration.push(
                        <u16 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxRefreshRequest".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxRefreshRequest {
            duration: intermediate_rep.duration.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxRefreshRequest> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxRefreshRequest>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxRefreshRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxRefreshRequest - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxRefreshRequest> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxRefreshRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxRefreshRequest - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxSnapshotRequest {
    /// Optional name for the snapshot template. If a snapshot template with this name already exists, a new build will be assigned to the existing template instead of creating a new one.
    #[serde(rename = "name")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl SandboxSnapshotRequest {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> SandboxSnapshotRequest {
        SandboxSnapshotRequest { name: None }
    }
}

/// Converts the SandboxSnapshotRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxSnapshotRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            self.name
                .as_ref()
                .map(|name| ["name".to_string(), name.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxSnapshotRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxSnapshotRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub name: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxSnapshotRequest".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "name" => intermediate_rep.name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxSnapshotRequest".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxSnapshotRequest {
            name: intermediate_rep.name.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxSnapshotRequest> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxSnapshotRequest>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxSnapshotRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxSnapshotRequest - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxSnapshotRequest> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxSnapshotRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxSnapshotRequest - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// State of the sandbox
/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types, clippy::large_enum_variant)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum SandboxState {
    #[serde(rename = "running")]
    Running,
    #[serde(rename = "paused")]
    Paused,
}

impl validator::Validate for SandboxState {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

impl std::fmt::Display for SandboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            SandboxState::Running => write!(f, "running"),
            SandboxState::Paused => write!(f, "paused"),
        }
    }
}

impl std::str::FromStr for SandboxState {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "running" => std::result::Result::Ok(SandboxState::Running),
            "paused" => std::result::Result::Ok(SandboxState::Paused),
            _ => std::result::Result::Err(format!(r#"Value not valid: {s}"#)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SandboxTimeoutRequest {
    /// Timeout in seconds from the current time after which the sandbox should expire
    #[serde(rename = "timeout")]
    #[validate(range(min = 0u32))]
    pub timeout: u32,
}

impl SandboxTimeoutRequest {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(timeout: u32) -> SandboxTimeoutRequest {
        SandboxTimeoutRequest { timeout }
    }
}

/// Converts the SandboxTimeoutRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SandboxTimeoutRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> =
            vec![Some("timeout".to_string()), Some(self.timeout.to_string())];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SandboxTimeoutRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SandboxTimeoutRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub timeout: Vec<u32>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SandboxTimeoutRequest".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "timeout" => intermediate_rep.timeout.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SandboxTimeoutRequest".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SandboxTimeoutRequest {
            timeout: intermediate_rep
                .timeout
                .into_iter()
                .next()
                .ok_or_else(|| "timeout missing in SandboxTimeoutRequest".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SandboxTimeoutRequest> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SandboxTimeoutRequest>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SandboxTimeoutRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SandboxTimeoutRequest - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SandboxTimeoutRequest> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SandboxTimeoutRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SandboxTimeoutRequest - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SnapshotInfo {
    /// The actual snapshot ID (stable identifier). Always contains the raw snapshot UUID, never an alias.
    #[serde(rename = "snapshotID")]
    #[validate(custom(function = "check_xss_string"))]
    pub snapshot_id: String,

    /// User-provided aliases for the snapshot. Empty if no alias was assigned during snapshot creation.
    #[serde(rename = "names")]
    #[validate(custom(function = "check_xss_vec_string"))]
    pub names: Vec<String>,

    /// CPU cores for the sandbox
    #[serde(rename = "cpuCount")]
    #[validate(range(min = 1u32))]
    pub cpu_count: u32,

    /// Memory for the sandbox in MiB
    #[serde(rename = "memoryMB")]
    #[validate(range(min = 128u32))]
    pub memory_mb: u32,

    /// Disk size for the sandbox in MiB
    #[serde(rename = "diskSizeMB")]
    #[validate(range(min = 0u32))]
    pub disk_size_mb: u32,

    /// Time when the snapshot was created
    #[serde(rename = "createdAt")]
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// Time when the snapshot was last updated
    #[serde(rename = "updatedAt")]
    pub updated_at: chrono::DateTime<chrono::Utc>,

    /// OverlayBD-native OCI image reference for the snapshot rootfs when source-registry image publication was enabled and succeeded. Omitted when no rootfs image was published.
    #[serde(rename = "imageRef")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_ref: Option<String>,
}

impl SnapshotInfo {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        snapshot_id: String,
        names: Vec<String>,
        cpu_count: u32,
        memory_mb: u32,
        disk_size_mb: u32,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> SnapshotInfo {
        SnapshotInfo {
            snapshot_id,
            names,
            cpu_count,
            memory_mb,
            disk_size_mb,
            created_at,
            updated_at,
            image_ref: None,
        }
    }
}

/// Converts the SnapshotInfo value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for SnapshotInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("snapshotID".to_string()),
            Some(self.snapshot_id.to_string()),
            Some("names".to_string()),
            Some(
                self.names
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            Some("cpuCount".to_string()),
            Some(self.cpu_count.to_string()),
            Some("memoryMB".to_string()),
            Some(self.memory_mb.to_string()),
            Some("diskSizeMB".to_string()),
            Some(self.disk_size_mb.to_string()),
            // Skipping createdAt in query parameter serialization

            // Skipping updatedAt in query parameter serialization
            self.image_ref
                .as_ref()
                .map(|image_ref| ["imageRef".to_string(), image_ref.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a SnapshotInfo value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for SnapshotInfo {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub snapshot_id: Vec<String>,
            pub names: Vec<Vec<String>>,
            pub cpu_count: Vec<u32>,
            pub memory_mb: Vec<u32>,
            pub disk_size_mb: Vec<u32>,
            pub created_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub updated_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub image_ref: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing SnapshotInfo".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "snapshotID" => intermediate_rep.snapshot_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "names" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in SnapshotInfo"
                                .to_string(),
                        );
                    }
                    #[allow(clippy::redundant_clone)]
                    "cpuCount" => intermediate_rep.cpu_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "memoryMB" => intermediate_rep.memory_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "diskSizeMB" => intermediate_rep.disk_size_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "createdAt" => intermediate_rep.created_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "updatedAt" => intermediate_rep.updated_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "imageRef" => intermediate_rep.image_ref.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing SnapshotInfo".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(SnapshotInfo {
            snapshot_id: intermediate_rep
                .snapshot_id
                .into_iter()
                .next()
                .ok_or_else(|| "snapshotID missing in SnapshotInfo".to_string())?,
            names: intermediate_rep
                .names
                .into_iter()
                .next()
                .ok_or_else(|| "names missing in SnapshotInfo".to_string())?,
            cpu_count: intermediate_rep
                .cpu_count
                .into_iter()
                .next()
                .ok_or_else(|| "cpuCount missing in SnapshotInfo".to_string())?,
            memory_mb: intermediate_rep
                .memory_mb
                .into_iter()
                .next()
                .ok_or_else(|| "memoryMB missing in SnapshotInfo".to_string())?,
            disk_size_mb: intermediate_rep
                .disk_size_mb
                .into_iter()
                .next()
                .ok_or_else(|| "diskSizeMB missing in SnapshotInfo".to_string())?,
            created_at: intermediate_rep
                .created_at
                .into_iter()
                .next()
                .ok_or_else(|| "createdAt missing in SnapshotInfo".to_string())?,
            updated_at: intermediate_rep
                .updated_at
                .into_iter()
                .next()
                .ok_or_else(|| "updatedAt missing in SnapshotInfo".to_string())?,
            image_ref: intermediate_rep.image_ref.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<SnapshotInfo> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<SnapshotInfo>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<SnapshotInfo>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for SnapshotInfo - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<SnapshotInfo> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <SnapshotInfo as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into SnapshotInfo - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Template {
    /// Identifier of the template
    #[serde(rename = "templateID")]
    #[validate(custom(function = "check_xss_string"))]
    pub template_id: String,

    /// Identifier of the last successful build for given template
    #[serde(rename = "buildID")]
    #[validate(custom(function = "check_xss_string"))]
    pub build_id: String,

    /// CPU cores for the sandbox
    #[serde(rename = "cpuCount")]
    #[validate(range(min = 1u32))]
    pub cpu_count: u32,

    /// Memory for the sandbox in MiB
    #[serde(rename = "memoryMB")]
    #[validate(range(min = 128u32))]
    pub memory_mb: u32,

    /// Disk size for the sandbox in MiB
    #[serde(rename = "diskSizeMB")]
    #[validate(range(min = 0u32))]
    pub disk_size_mb: u32,

    /// Whether the template is public or only accessible by the team
    #[serde(rename = "public")]
    pub public: bool,

    /// Names of the template (namespace/alias format when namespaced)
    #[serde(rename = "names")]
    #[validate(custom(function = "check_xss_vec_string"))]
    pub names: Vec<String>,

    /// Time when the template was created
    #[serde(rename = "createdAt")]
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// Time when the template was last updated
    #[serde(rename = "updatedAt")]
    pub updated_at: chrono::DateTime<chrono::Utc>,

    /// Time when the template was last used
    #[serde(rename = "lastSpawnedAt")]
    pub last_spawned_at: Nullable<chrono::DateTime<chrono::Utc>>,

    /// Number of times the template was used
    #[serde(rename = "spawnCount")]
    pub spawn_count: i64,

    /// Number of times the template was built
    #[serde(rename = "buildCount")]
    pub build_count: i32,

    /// Version of the envd running in the sandbox
    #[serde(rename = "envdVersion")]
    #[validate(custom(function = "check_xss_string"))]
    pub envd_version: String,

    #[serde(rename = "buildStatus")]
    #[validate(nested)]
    pub build_status: models::TemplateBuildStatus,
}

impl Template {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        template_id: String,
        build_id: String,
        cpu_count: u32,
        memory_mb: u32,
        disk_size_mb: u32,
        public: bool,
        names: Vec<String>,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
        last_spawned_at: Nullable<chrono::DateTime<chrono::Utc>>,
        spawn_count: i64,
        build_count: i32,
        envd_version: String,
        build_status: models::TemplateBuildStatus,
    ) -> Template {
        Template {
            template_id,
            build_id,
            cpu_count,
            memory_mb,
            disk_size_mb,
            public,
            names,
            created_at,
            updated_at,
            last_spawned_at,
            spawn_count,
            build_count,
            envd_version,
            build_status,
        }
    }
}

/// Converts the Template value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for Template {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("templateID".to_string()),
            Some(self.template_id.to_string()),
            Some("buildID".to_string()),
            Some(self.build_id.to_string()),
            Some("cpuCount".to_string()),
            Some(self.cpu_count.to_string()),
            Some("memoryMB".to_string()),
            Some(self.memory_mb.to_string()),
            Some("diskSizeMB".to_string()),
            Some(self.disk_size_mb.to_string()),
            Some("public".to_string()),
            Some(self.public.to_string()),
            Some("names".to_string()),
            Some(
                self.names
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            // Skipping createdAt in query parameter serialization

            // Skipping updatedAt in query parameter serialization

            // Skipping lastSpawnedAt in query parameter serialization
            Some("spawnCount".to_string()),
            Some(self.spawn_count.to_string()),
            Some("buildCount".to_string()),
            Some(self.build_count.to_string()),
            Some("envdVersion".to_string()),
            Some(self.envd_version.to_string()),
            // Skipping buildStatus in query parameter serialization
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Template value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Template {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub template_id: Vec<String>,
            pub build_id: Vec<String>,
            pub cpu_count: Vec<u32>,
            pub memory_mb: Vec<u32>,
            pub disk_size_mb: Vec<u32>,
            pub public: Vec<bool>,
            pub names: Vec<Vec<String>>,
            pub created_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub updated_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub last_spawned_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub spawn_count: Vec<i64>,
            pub build_count: Vec<i32>,
            pub envd_version: Vec<String>,
            pub build_status: Vec<models::TemplateBuildStatus>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Template".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "templateID" => intermediate_rep.template_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "buildID" => intermediate_rep.build_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cpuCount" => intermediate_rep.cpu_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "memoryMB" => intermediate_rep.memory_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "diskSizeMB" => intermediate_rep.disk_size_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "public" => intermediate_rep.public.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "names" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in Template"
                                .to_string(),
                        );
                    }
                    #[allow(clippy::redundant_clone)]
                    "createdAt" => intermediate_rep.created_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "updatedAt" => intermediate_rep.updated_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    "lastSpawnedAt" => {
                        return std::result::Result::Err(
                            "Parsing a nullable type in this style is not supported in Template"
                                .to_string(),
                        );
                    }
                    #[allow(clippy::redundant_clone)]
                    "spawnCount" => intermediate_rep.spawn_count.push(
                        <i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "buildCount" => intermediate_rep.build_count.push(
                        <i32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "envdVersion" => intermediate_rep.envd_version.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "buildStatus" => intermediate_rep.build_status.push(
                        <models::TemplateBuildStatus as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Template".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Template {
            template_id: intermediate_rep
                .template_id
                .into_iter()
                .next()
                .ok_or_else(|| "templateID missing in Template".to_string())?,
            build_id: intermediate_rep
                .build_id
                .into_iter()
                .next()
                .ok_or_else(|| "buildID missing in Template".to_string())?,
            cpu_count: intermediate_rep
                .cpu_count
                .into_iter()
                .next()
                .ok_or_else(|| "cpuCount missing in Template".to_string())?,
            memory_mb: intermediate_rep
                .memory_mb
                .into_iter()
                .next()
                .ok_or_else(|| "memoryMB missing in Template".to_string())?,
            disk_size_mb: intermediate_rep
                .disk_size_mb
                .into_iter()
                .next()
                .ok_or_else(|| "diskSizeMB missing in Template".to_string())?,
            public: intermediate_rep
                .public
                .into_iter()
                .next()
                .ok_or_else(|| "public missing in Template".to_string())?,
            names: intermediate_rep
                .names
                .into_iter()
                .next()
                .ok_or_else(|| "names missing in Template".to_string())?,
            created_at: intermediate_rep
                .created_at
                .into_iter()
                .next()
                .ok_or_else(|| "createdAt missing in Template".to_string())?,
            updated_at: intermediate_rep
                .updated_at
                .into_iter()
                .next()
                .ok_or_else(|| "updatedAt missing in Template".to_string())?,
            last_spawned_at: std::result::Result::Err(
                "Nullable types not supported in Template".to_string(),
            )?,
            spawn_count: intermediate_rep
                .spawn_count
                .into_iter()
                .next()
                .ok_or_else(|| "spawnCount missing in Template".to_string())?,
            build_count: intermediate_rep
                .build_count
                .into_iter()
                .next()
                .ok_or_else(|| "buildCount missing in Template".to_string())?,
            envd_version: intermediate_rep
                .envd_version
                .into_iter()
                .next()
                .ok_or_else(|| "envdVersion missing in Template".to_string())?,
            build_status: intermediate_rep
                .build_status
                .into_iter()
                .next()
                .ok_or_else(|| "buildStatus missing in Template".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Template> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<Template>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Template>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for Template - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<Template> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <Template as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into Template - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplateAliasResponse {
    /// Identifier of the template
    #[serde(rename = "templateID")]
    #[validate(custom(function = "check_xss_string"))]
    pub template_id: String,

    /// Whether the template is public or only accessible by the team
    #[serde(rename = "public")]
    pub public: bool,
}

impl TemplateAliasResponse {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(template_id: String, public: bool) -> TemplateAliasResponse {
        TemplateAliasResponse {
            template_id,
            public,
        }
    }
}

/// Converts the TemplateAliasResponse value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for TemplateAliasResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("templateID".to_string()),
            Some(self.template_id.to_string()),
            Some("public".to_string()),
            Some(self.public.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TemplateAliasResponse value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TemplateAliasResponse {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub template_id: Vec<String>,
            pub public: Vec<bool>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TemplateAliasResponse".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "templateID" => intermediate_rep.template_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "public" => intermediate_rep.public.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TemplateAliasResponse".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TemplateAliasResponse {
            template_id: intermediate_rep
                .template_id
                .into_iter()
                .next()
                .ok_or_else(|| "templateID missing in TemplateAliasResponse".to_string())?,
            public: intermediate_rep
                .public
                .into_iter()
                .next()
                .ok_or_else(|| "public missing in TemplateAliasResponse".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TemplateAliasResponse> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<TemplateAliasResponse>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TemplateAliasResponse>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for TemplateAliasResponse - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<TemplateAliasResponse> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TemplateAliasResponse as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into TemplateAliasResponse - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplateBuild {
    /// Identifier of the build
    #[serde(rename = "buildID")]
    pub build_id: uuid::Uuid,

    #[serde(rename = "status")]
    #[validate(nested)]
    pub status: models::TemplateBuildStatus,

    /// Time when the build was created
    #[serde(rename = "createdAt")]
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// Time when the build was last updated
    #[serde(rename = "updatedAt")]
    pub updated_at: chrono::DateTime<chrono::Utc>,

    /// Time when the build was finished
    #[serde(rename = "finishedAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,

    /// CPU cores for the sandbox
    #[serde(rename = "cpuCount")]
    #[validate(range(min = 1u32))]
    pub cpu_count: u32,

    /// Memory for the sandbox in MiB
    #[serde(rename = "memoryMB")]
    #[validate(range(min = 128u32))]
    pub memory_mb: u32,

    /// Disk size for the sandbox in MiB
    #[serde(rename = "diskSizeMB")]
    #[validate(range(min = 0u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_size_mb: Option<u32>,

    /// Version of the envd running in the sandbox
    #[serde(rename = "envdVersion")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envd_version: Option<String>,
}

impl TemplateBuild {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        build_id: uuid::Uuid,
        status: models::TemplateBuildStatus,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
        cpu_count: u32,
        memory_mb: u32,
    ) -> TemplateBuild {
        TemplateBuild {
            build_id,
            status,
            created_at,
            updated_at,
            finished_at: None,
            cpu_count,
            memory_mb,
            disk_size_mb: None,
            envd_version: None,
        }
    }
}

/// Converts the TemplateBuild value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for TemplateBuild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            // Skipping buildID in query parameter serialization

            // Skipping status in query parameter serialization

            // Skipping createdAt in query parameter serialization

            // Skipping updatedAt in query parameter serialization

            // Skipping finishedAt in query parameter serialization
            Some("cpuCount".to_string()),
            Some(self.cpu_count.to_string()),
            Some("memoryMB".to_string()),
            Some(self.memory_mb.to_string()),
            self.disk_size_mb
                .as_ref()
                .map(|disk_size_mb| ["diskSizeMB".to_string(), disk_size_mb.to_string()].join(",")),
            self.envd_version.as_ref().map(|envd_version| {
                ["envdVersion".to_string(), envd_version.to_string()].join(",")
            }),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TemplateBuild value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TemplateBuild {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub build_id: Vec<uuid::Uuid>,
            pub status: Vec<models::TemplateBuildStatus>,
            pub created_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub updated_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub finished_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub cpu_count: Vec<u32>,
            pub memory_mb: Vec<u32>,
            pub disk_size_mb: Vec<u32>,
            pub envd_version: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TemplateBuild".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "buildID" => intermediate_rep.build_id.push(
                        <uuid::Uuid as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "status" => intermediate_rep.status.push(
                        <models::TemplateBuildStatus as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "createdAt" => intermediate_rep.created_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "updatedAt" => intermediate_rep.updated_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "finishedAt" => intermediate_rep.finished_at.push(
                        <chrono::DateTime<chrono::Utc> as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "cpuCount" => intermediate_rep.cpu_count.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "memoryMB" => intermediate_rep.memory_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "diskSizeMB" => intermediate_rep.disk_size_mb.push(
                        <u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "envdVersion" => intermediate_rep.envd_version.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TemplateBuild".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TemplateBuild {
            build_id: intermediate_rep
                .build_id
                .into_iter()
                .next()
                .ok_or_else(|| "buildID missing in TemplateBuild".to_string())?,
            status: intermediate_rep
                .status
                .into_iter()
                .next()
                .ok_or_else(|| "status missing in TemplateBuild".to_string())?,
            created_at: intermediate_rep
                .created_at
                .into_iter()
                .next()
                .ok_or_else(|| "createdAt missing in TemplateBuild".to_string())?,
            updated_at: intermediate_rep
                .updated_at
                .into_iter()
                .next()
                .ok_or_else(|| "updatedAt missing in TemplateBuild".to_string())?,
            finished_at: intermediate_rep.finished_at.into_iter().next(),
            cpu_count: intermediate_rep
                .cpu_count
                .into_iter()
                .next()
                .ok_or_else(|| "cpuCount missing in TemplateBuild".to_string())?,
            memory_mb: intermediate_rep
                .memory_mb
                .into_iter()
                .next()
                .ok_or_else(|| "memoryMB missing in TemplateBuild".to_string())?,
            disk_size_mb: intermediate_rep.disk_size_mb.into_iter().next(),
            envd_version: intermediate_rep.envd_version.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TemplateBuild> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<TemplateBuild>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TemplateBuild>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for TemplateBuild - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<TemplateBuild> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TemplateBuild as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into TemplateBuild - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplateBuildInfo {
    /// Build logs
    #[serde(rename = "logs")]
    #[validate(custom(function = "check_xss_vec_string"))]
    pub logs: Vec<String>,

    /// Build logs structured
    #[serde(rename = "logEntries")]
    #[validate(nested)]
    pub log_entries: Vec<models::BuildLogEntry>,

    /// Identifier of the template
    #[serde(rename = "templateID")]
    #[validate(custom(function = "check_xss_string"))]
    pub template_id: String,

    /// Identifier of the build
    #[serde(rename = "buildID")]
    #[validate(custom(function = "check_xss_string"))]
    pub build_id: String,

    #[serde(rename = "status")]
    #[validate(nested)]
    pub status: models::TemplateBuildStatus,

    #[serde(rename = "reason")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<models::BuildStatusReason>,
}

impl TemplateBuildInfo {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        logs: Vec<String>,
        log_entries: Vec<models::BuildLogEntry>,
        template_id: String,
        build_id: String,
        status: models::TemplateBuildStatus,
    ) -> TemplateBuildInfo {
        TemplateBuildInfo {
            logs,
            log_entries,
            template_id,
            build_id,
            status,
            reason: None,
        }
    }
}

/// Converts the TemplateBuildInfo value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for TemplateBuildInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("logs".to_string()),
            Some(
                self.logs
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            // Skipping logEntries in query parameter serialization
            Some("templateID".to_string()),
            Some(self.template_id.to_string()),
            Some("buildID".to_string()),
            Some(self.build_id.to_string()),
            // Skipping status in query parameter serialization

            // Skipping reason in query parameter serialization
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TemplateBuildInfo value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TemplateBuildInfo {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub logs: Vec<Vec<String>>,
            pub log_entries: Vec<Vec<models::BuildLogEntry>>,
            pub template_id: Vec<String>,
            pub build_id: Vec<String>,
            pub status: Vec<models::TemplateBuildStatus>,
            pub reason: Vec<models::BuildStatusReason>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TemplateBuildInfo".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "logs" => return std::result::Result::Err(
                        "Parsing a container in this style is not supported in TemplateBuildInfo"
                            .to_string(),
                    ),
                    "logEntries" => return std::result::Result::Err(
                        "Parsing a container in this style is not supported in TemplateBuildInfo"
                            .to_string(),
                    ),
                    #[allow(clippy::redundant_clone)]
                    "templateID" => intermediate_rep.template_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "buildID" => intermediate_rep.build_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "status" => intermediate_rep.status.push(
                        <models::TemplateBuildStatus as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "reason" => intermediate_rep.reason.push(
                        <models::BuildStatusReason as std::str::FromStr>::from_str(val)
                            .map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TemplateBuildInfo".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TemplateBuildInfo {
            logs: intermediate_rep
                .logs
                .into_iter()
                .next()
                .ok_or_else(|| "logs missing in TemplateBuildInfo".to_string())?,
            log_entries: intermediate_rep
                .log_entries
                .into_iter()
                .next()
                .ok_or_else(|| "logEntries missing in TemplateBuildInfo".to_string())?,
            template_id: intermediate_rep
                .template_id
                .into_iter()
                .next()
                .ok_or_else(|| "templateID missing in TemplateBuildInfo".to_string())?,
            build_id: intermediate_rep
                .build_id
                .into_iter()
                .next()
                .ok_or_else(|| "buildID missing in TemplateBuildInfo".to_string())?,
            status: intermediate_rep
                .status
                .into_iter()
                .next()
                .ok_or_else(|| "status missing in TemplateBuildInfo".to_string())?,
            reason: intermediate_rep.reason.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TemplateBuildInfo> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<TemplateBuildInfo>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TemplateBuildInfo>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for TemplateBuildInfo - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<TemplateBuildInfo> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TemplateBuildInfo as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into TemplateBuildInfo - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplateBuildRequestV3 {
    /// Name of the template. Can include a tag with colon separator (e.g. \"my-template\" or \"my-template:v1\"). If tag is included, it will be treated as if the tag was provided in the tags array.
    #[serde(rename = "name")]
    #[validate(length(max = 128), custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Tags to assign to the template build
    #[serde(rename = "tags")]
    #[validate(custom(function = "check_xss_vec_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,

    /// CPU cores for the sandbox
    #[serde(rename = "cpuCount")]
    #[validate(range(min = 1u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_count: Option<u32>,

    /// Memory for the sandbox in MiB
    #[serde(rename = "memoryMB")]
    #[validate(range(min = 128u32))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u32>,
}

impl TemplateBuildRequestV3 {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> TemplateBuildRequestV3 {
        TemplateBuildRequestV3 {
            name: None,
            tags: None,
            cpu_count: None,
            memory_mb: None,
        }
    }
}

/// Converts the TemplateBuildRequestV3 value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for TemplateBuildRequestV3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            self.name
                .as_ref()
                .map(|name| ["name".to_string(), name.to_string()].join(",")),
            self.tags.as_ref().map(|tags| {
                [
                    "tags".to_string(),
                    tags.iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ]
                .join(",")
            }),
            self.cpu_count
                .as_ref()
                .map(|cpu_count| ["cpuCount".to_string(), cpu_count.to_string()].join(",")),
            self.memory_mb
                .as_ref()
                .map(|memory_mb| ["memoryMB".to_string(), memory_mb.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TemplateBuildRequestV3 value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TemplateBuildRequestV3 {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub name: Vec<String>,
            pub tags: Vec<Vec<String>>,
            pub cpu_count: Vec<u32>,
            pub memory_mb: Vec<u32>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TemplateBuildRequestV3".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "name" => intermediate_rep.name.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "tags" => return std::result::Result::Err("Parsing a container in this style is not supported in TemplateBuildRequestV3".to_string()),
                    #[allow(clippy::redundant_clone)]
                    "cpuCount" => intermediate_rep.cpu_count.push(<u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "memoryMB" => intermediate_rep.memory_mb.push(<u32 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    _ => return std::result::Result::Err("Unexpected key while parsing TemplateBuildRequestV3".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TemplateBuildRequestV3 {
            name: intermediate_rep.name.into_iter().next(),
            tags: intermediate_rep.tags.into_iter().next(),
            cpu_count: intermediate_rep.cpu_count.into_iter().next(),
            memory_mb: intermediate_rep.memory_mb.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TemplateBuildRequestV3> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<TemplateBuildRequestV3>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TemplateBuildRequestV3>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for TemplateBuildRequestV3 - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<TemplateBuildRequestV3> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TemplateBuildRequestV3 as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into TemplateBuildRequestV3 - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplateBuildStartV2 {
    /// Image to use as a base for the template build
    #[serde(rename = "fromImage")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_image: Option<String>,

    /// Template to use as a base for the template build
    #[serde(rename = "fromTemplate")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_template: Option<String>,

    /// Whether the whole build should be forced to run regardless of the cache
    #[serde(rename = "force")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,

    /// List of steps to execute in the template build
    #[serde(rename = "steps")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub steps: Option<Vec<models::TemplateStep>>,

    /// Start command to execute in the template after the build
    #[serde(rename = "startCmd")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_cmd: Option<String>,

    /// Ready check command to execute in the template after the build
    #[serde(rename = "readyCmd")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_cmd: Option<String>,
}

impl TemplateBuildStartV2 {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new() -> TemplateBuildStartV2 {
        TemplateBuildStartV2 {
            from_image: None,
            from_template: None,
            force: Some(false),
            steps: None,
            start_cmd: None,
            ready_cmd: None,
        }
    }
}

/// Converts the TemplateBuildStartV2 value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for TemplateBuildStartV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            self.from_image
                .as_ref()
                .map(|from_image| ["fromImage".to_string(), from_image.to_string()].join(",")),
            self.from_template.as_ref().map(|from_template| {
                ["fromTemplate".to_string(), from_template.to_string()].join(",")
            }),
            self.force
                .as_ref()
                .map(|force| ["force".to_string(), force.to_string()].join(",")),
            // Skipping steps in query parameter serialization
            self.start_cmd
                .as_ref()
                .map(|start_cmd| ["startCmd".to_string(), start_cmd.to_string()].join(",")),
            self.ready_cmd
                .as_ref()
                .map(|ready_cmd| ["readyCmd".to_string(), ready_cmd.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TemplateBuildStartV2 value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TemplateBuildStartV2 {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub from_image: Vec<String>,
            pub from_template: Vec<String>,
            pub force: Vec<bool>,
            pub steps: Vec<Vec<models::TemplateStep>>,
            pub start_cmd: Vec<String>,
            pub ready_cmd: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TemplateBuildStartV2".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "fromImage" => intermediate_rep.from_image.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "fromTemplate" => intermediate_rep.from_template.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "force" => intermediate_rep.force.push(<bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "steps" => return std::result::Result::Err("Parsing a container in this style is not supported in TemplateBuildStartV2".to_string()),
                    #[allow(clippy::redundant_clone)]
                    "startCmd" => intermediate_rep.start_cmd.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "readyCmd" => intermediate_rep.ready_cmd.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    _ => return std::result::Result::Err("Unexpected key while parsing TemplateBuildStartV2".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TemplateBuildStartV2 {
            from_image: intermediate_rep.from_image.into_iter().next(),
            from_template: intermediate_rep.from_template.into_iter().next(),
            force: intermediate_rep.force.into_iter().next(),
            steps: intermediate_rep.steps.into_iter().next(),
            start_cmd: intermediate_rep.start_cmd.into_iter().next(),
            ready_cmd: intermediate_rep.ready_cmd.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TemplateBuildStartV2> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<TemplateBuildStartV2>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TemplateBuildStartV2>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for TemplateBuildStartV2 - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<TemplateBuildStartV2> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TemplateBuildStartV2 as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into TemplateBuildStartV2 - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Status of the template build
/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types, clippy::large_enum_variant)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum TemplateBuildStatus {
    #[serde(rename = "building")]
    Building,
    #[serde(rename = "waiting")]
    Waiting,
    #[serde(rename = "ready")]
    Ready,
    #[serde(rename = "error")]
    Error,
}

impl validator::Validate for TemplateBuildStatus {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

impl std::fmt::Display for TemplateBuildStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            TemplateBuildStatus::Building => write!(f, "building"),
            TemplateBuildStatus::Waiting => write!(f, "waiting"),
            TemplateBuildStatus::Ready => write!(f, "ready"),
            TemplateBuildStatus::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for TemplateBuildStatus {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "building" => std::result::Result::Ok(TemplateBuildStatus::Building),
            "waiting" => std::result::Result::Ok(TemplateBuildStatus::Waiting),
            "ready" => std::result::Result::Ok(TemplateBuildStatus::Ready),
            "error" => std::result::Result::Ok(TemplateBuildStatus::Error),
            _ => std::result::Result::Err(format!(r#"Value not valid: {s}"#)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplateRequestResponseV3 {
    /// Identifier of the template
    #[serde(rename = "templateID")]
    #[validate(custom(function = "check_xss_string"))]
    pub template_id: String,

    /// Identifier of the last successful build for given template
    #[serde(rename = "buildID")]
    #[validate(custom(function = "check_xss_string"))]
    pub build_id: String,

    /// Whether the template is public or only accessible by the team
    #[serde(rename = "public")]
    pub public: bool,

    /// Names of the template
    #[serde(rename = "names")]
    #[validate(custom(function = "check_xss_vec_string"))]
    pub names: Vec<String>,

    /// Tags assigned to the template build
    #[serde(rename = "tags")]
    #[validate(custom(function = "check_xss_vec_string"))]
    pub tags: Vec<String>,

    /// Aliases of the template
    #[serde(rename = "aliases")]
    #[validate(custom(function = "check_xss_vec_string"))]
    pub aliases: Vec<String>,
}

impl TemplateRequestResponseV3 {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        template_id: String,
        build_id: String,
        public: bool,
        names: Vec<String>,
        tags: Vec<String>,
        aliases: Vec<String>,
    ) -> TemplateRequestResponseV3 {
        TemplateRequestResponseV3 {
            template_id,
            build_id,
            public,
            names,
            tags,
            aliases,
        }
    }
}

/// Converts the TemplateRequestResponseV3 value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for TemplateRequestResponseV3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("templateID".to_string()),
            Some(self.template_id.to_string()),
            Some("buildID".to_string()),
            Some(self.build_id.to_string()),
            Some("public".to_string()),
            Some(self.public.to_string()),
            Some("names".to_string()),
            Some(
                self.names
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            Some("tags".to_string()),
            Some(
                self.tags
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            Some("aliases".to_string()),
            Some(
                self.aliases
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TemplateRequestResponseV3 value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TemplateRequestResponseV3 {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub template_id: Vec<String>,
            pub build_id: Vec<String>,
            pub public: Vec<bool>,
            pub names: Vec<Vec<String>>,
            pub tags: Vec<Vec<String>>,
            pub aliases: Vec<Vec<String>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TemplateRequestResponseV3".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "templateID" => intermediate_rep.template_id.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "buildID" => intermediate_rep.build_id.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "public" => intermediate_rep.public.push(<bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "names" => return std::result::Result::Err("Parsing a container in this style is not supported in TemplateRequestResponseV3".to_string()),
                    "tags" => return std::result::Result::Err("Parsing a container in this style is not supported in TemplateRequestResponseV3".to_string()),
                    "aliases" => return std::result::Result::Err("Parsing a container in this style is not supported in TemplateRequestResponseV3".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing TemplateRequestResponseV3".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TemplateRequestResponseV3 {
            template_id: intermediate_rep
                .template_id
                .into_iter()
                .next()
                .ok_or_else(|| "templateID missing in TemplateRequestResponseV3".to_string())?,
            build_id: intermediate_rep
                .build_id
                .into_iter()
                .next()
                .ok_or_else(|| "buildID missing in TemplateRequestResponseV3".to_string())?,
            public: intermediate_rep
                .public
                .into_iter()
                .next()
                .ok_or_else(|| "public missing in TemplateRequestResponseV3".to_string())?,
            names: intermediate_rep
                .names
                .into_iter()
                .next()
                .ok_or_else(|| "names missing in TemplateRequestResponseV3".to_string())?,
            tags: intermediate_rep
                .tags
                .into_iter()
                .next()
                .ok_or_else(|| "tags missing in TemplateRequestResponseV3".to_string())?,
            aliases: intermediate_rep
                .aliases
                .into_iter()
                .next()
                .ok_or_else(|| "aliases missing in TemplateRequestResponseV3".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TemplateRequestResponseV3> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<TemplateRequestResponseV3>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TemplateRequestResponseV3>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for TemplateRequestResponseV3 - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<TemplateRequestResponseV3> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TemplateRequestResponseV3 as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into TemplateRequestResponseV3 - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

/// Step in the template build process
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplateStep {
    /// Type of the step
    #[serde(rename = "type")]
    #[validate(custom(function = "check_xss_string"))]
    pub r_type: String,

    /// Arguments for the step
    #[serde(rename = "args")]
    #[validate(custom(function = "check_xss_vec_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    /// Hash of the files used in the step
    #[serde(rename = "filesHash")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_hash: Option<String>,

    /// Whether the step should be forced to run regardless of the cache
    #[serde(rename = "force")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,
}

impl TemplateStep {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(r_type: String) -> TemplateStep {
        TemplateStep {
            r_type,
            args: None,
            files_hash: None,
            force: Some(false),
        }
    }
}

/// Converts the TemplateStep value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for TemplateStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("type".to_string()),
            Some(self.r_type.to_string()),
            self.args.as_ref().map(|args| {
                [
                    "args".to_string(),
                    args.iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ]
                .join(",")
            }),
            self.files_hash
                .as_ref()
                .map(|files_hash| ["filesHash".to_string(), files_hash.to_string()].join(",")),
            self.force
                .as_ref()
                .map(|force| ["force".to_string(), force.to_string()].join(",")),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TemplateStep value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TemplateStep {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub r_type: Vec<String>,
            pub args: Vec<Vec<String>>,
            pub files_hash: Vec<String>,
            pub force: Vec<bool>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TemplateStep".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "type" => intermediate_rep.r_type.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    "args" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in TemplateStep"
                                .to_string(),
                        );
                    }
                    #[allow(clippy::redundant_clone)]
                    "filesHash" => intermediate_rep.files_hash.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "force" => intermediate_rep.force.push(
                        <bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing TemplateStep".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TemplateStep {
            r_type: intermediate_rep
                .r_type
                .into_iter()
                .next()
                .ok_or_else(|| "type missing in TemplateStep".to_string())?,
            args: intermediate_rep.args.into_iter().next(),
            files_hash: intermediate_rep.files_hash.into_iter().next(),
            force: intermediate_rep.force.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TemplateStep> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<TemplateStep>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TemplateStep>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for TemplateStep - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<TemplateStep> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TemplateStep as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into TemplateStep - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TemplateWithBuilds {
    /// Identifier of the template
    #[serde(rename = "templateID")]
    #[validate(custom(function = "check_xss_string"))]
    pub template_id: String,

    /// Whether the template is public or only accessible by the team
    #[serde(rename = "public")]
    pub public: bool,

    /// Names of the template (namespace/alias format when namespaced)
    #[serde(rename = "names")]
    #[validate(custom(function = "check_xss_vec_string"))]
    pub names: Vec<String>,

    /// Time when the template was created
    #[serde(rename = "createdAt")]
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// Time when the template was last updated
    #[serde(rename = "updatedAt")]
    pub updated_at: chrono::DateTime<chrono::Utc>,

    /// Time when the template was last used
    #[serde(rename = "lastSpawnedAt")]
    pub last_spawned_at: Nullable<chrono::DateTime<chrono::Utc>>,

    /// Number of times the template was used
    #[serde(rename = "spawnCount")]
    pub spawn_count: i64,

    /// List of builds for the template
    #[serde(rename = "builds")]
    #[validate(nested)]
    pub builds: Vec<models::TemplateBuild>,
}

impl TemplateWithBuilds {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(
        template_id: String,
        public: bool,
        names: Vec<String>,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
        last_spawned_at: Nullable<chrono::DateTime<chrono::Utc>>,
        spawn_count: i64,
        builds: Vec<models::TemplateBuild>,
    ) -> TemplateWithBuilds {
        TemplateWithBuilds {
            template_id,
            public,
            names,
            created_at,
            updated_at,
            last_spawned_at,
            spawn_count,
            builds,
        }
    }
}

/// Converts the TemplateWithBuilds value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for TemplateWithBuilds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("templateID".to_string()),
            Some(self.template_id.to_string()),
            Some("public".to_string()),
            Some(self.public.to_string()),
            Some("names".to_string()),
            Some(
                self.names
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            // Skipping createdAt in query parameter serialization

            // Skipping updatedAt in query parameter serialization

            // Skipping lastSpawnedAt in query parameter serialization
            Some("spawnCount".to_string()),
            Some(self.spawn_count.to_string()),
            // Skipping builds in query parameter serialization
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a TemplateWithBuilds value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for TemplateWithBuilds {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub template_id: Vec<String>,
            pub public: Vec<bool>,
            pub names: Vec<Vec<String>>,
            pub created_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub updated_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub last_spawned_at: Vec<chrono::DateTime<chrono::Utc>>,
            pub spawn_count: Vec<i64>,
            pub builds: Vec<Vec<models::TemplateBuild>>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing TemplateWithBuilds".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "templateID" => intermediate_rep.template_id.push(<String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "public" => intermediate_rep.public.push(<bool as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "names" => return std::result::Result::Err("Parsing a container in this style is not supported in TemplateWithBuilds".to_string()),
                    #[allow(clippy::redundant_clone)]
                    "createdAt" => intermediate_rep.created_at.push(<chrono::DateTime::<chrono::Utc> as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    #[allow(clippy::redundant_clone)]
                    "updatedAt" => intermediate_rep.updated_at.push(<chrono::DateTime::<chrono::Utc> as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "lastSpawnedAt" => return std::result::Result::Err("Parsing a nullable type in this style is not supported in TemplateWithBuilds".to_string()),
                    #[allow(clippy::redundant_clone)]
                    "spawnCount" => intermediate_rep.spawn_count.push(<i64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?),
                    "builds" => return std::result::Result::Err("Parsing a container in this style is not supported in TemplateWithBuilds".to_string()),
                    _ => return std::result::Result::Err("Unexpected key while parsing TemplateWithBuilds".to_string())
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(TemplateWithBuilds {
            template_id: intermediate_rep
                .template_id
                .into_iter()
                .next()
                .ok_or_else(|| "templateID missing in TemplateWithBuilds".to_string())?,
            public: intermediate_rep
                .public
                .into_iter()
                .next()
                .ok_or_else(|| "public missing in TemplateWithBuilds".to_string())?,
            names: intermediate_rep
                .names
                .into_iter()
                .next()
                .ok_or_else(|| "names missing in TemplateWithBuilds".to_string())?,
            created_at: intermediate_rep
                .created_at
                .into_iter()
                .next()
                .ok_or_else(|| "createdAt missing in TemplateWithBuilds".to_string())?,
            updated_at: intermediate_rep
                .updated_at
                .into_iter()
                .next()
                .ok_or_else(|| "updatedAt missing in TemplateWithBuilds".to_string())?,
            last_spawned_at: std::result::Result::Err(
                "Nullable types not supported in TemplateWithBuilds".to_string(),
            )?,
            spawn_count: intermediate_rep
                .spawn_count
                .into_iter()
                .next()
                .ok_or_else(|| "spawnCount missing in TemplateWithBuilds".to_string())?,
            builds: intermediate_rep
                .builds
                .into_iter()
                .next()
                .ok_or_else(|| "builds missing in TemplateWithBuilds".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<TemplateWithBuilds> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<TemplateWithBuilds>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<TemplateWithBuilds>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for TemplateWithBuilds - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<TemplateWithBuilds> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <TemplateWithBuilds as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into TemplateWithBuilds - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Volume {
    /// Stable identifier of the volume.
    #[serde(rename = "volumeID")]
    #[validate(custom(function = "check_xss_string"))]
    pub volume_id: String,

    /// Unique human-readable volume name.
    #[serde(rename = "name")]
    #[validate(
            regex(path = *RE_VOLUME_NAME),
          custom(function = "check_xss_string"),
    )]
    pub name: String,

    /// Access mode (`ro` or `exclusive`).
    #[serde(rename = "mode")]
    #[validate(custom(function = "check_xss_string"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    /// Effective volume size in MiB.
    #[serde(rename = "sizeMB")]
    #[validate(range(min = 1u64))]
    pub size_mb: u64,

    /// Availability state (`ready`, `uploading`, or `failed`).
    #[serde(rename = "status")]
    #[validate(custom(function = "check_xss_string"))]
    pub status: String,
}

lazy_static::lazy_static! {
    static ref RE_VOLUME_NAME: regex::Regex = regex::Regex::new("^[a-zA-Z0-9_-]+$").unwrap();
}

impl Volume {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    pub fn new(volume_id: String, name: String, size_mb: u64, status: String) -> Volume {
        Volume {
            volume_id,
            name,
            mode: None,
            size_mb,
            status,
        }
    }
}

/// Converts the Volume value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::fmt::Display for Volume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<Option<String>> = vec![
            Some("volumeID".to_string()),
            Some(self.volume_id.to_string()),
            Some("name".to_string()),
            Some(self.name.to_string()),
            self.mode
                .as_ref()
                .map(|mode| ["mode".to_string(), mode.to_string()].join(",")),
            Some("sizeMB".to_string()),
            Some(self.size_mb.to_string()),
            Some("status".to_string()),
            Some(self.status.to_string()),
        ];

        write!(
            f,
            "{}",
            params.into_iter().flatten().collect::<Vec<_>>().join(",")
        )
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Volume value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Volume {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub volume_id: Vec<String>,
            pub name: Vec<String>,
            pub mode: Vec<String>,
            pub size_mb: Vec<u64>,
            pub status: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Volume".to_string(),
                    );
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "volumeID" => intermediate_rep.volume_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "name" => intermediate_rep.name.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "mode" => intermediate_rep.mode.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "sizeMB" => intermediate_rep.size_mb.push(
                        <u64 as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "status" => intermediate_rep.status.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Volume".to_string(),
                        );
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Volume {
            volume_id: intermediate_rep
                .volume_id
                .into_iter()
                .next()
                .ok_or_else(|| "volumeID missing in Volume".to_string())?,
            name: intermediate_rep
                .name
                .into_iter()
                .next()
                .ok_or_else(|| "name missing in Volume".to_string())?,
            mode: intermediate_rep.mode.into_iter().next(),
            size_mb: intermediate_rep
                .size_mb
                .into_iter()
                .next()
                .ok_or_else(|| "sizeMB missing in Volume".to_string())?,
            status: intermediate_rep
                .status
                .into_iter()
                .next()
                .ok_or_else(|| "status missing in Volume".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Volume> and HeaderValue

#[cfg(feature = "server")]
impl std::convert::TryFrom<header::IntoHeaderValue<Volume>> for HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Volume>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Invalid header value for Volume - value: {hdr_value} is invalid {e}"#
            )),
        }
    }
}

#[cfg(feature = "server")]
impl std::convert::TryFrom<HeaderValue> for header::IntoHeaderValue<Volume> {
    type Error = String;

    fn try_from(hdr_value: HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <Volume as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        r#"Unable to convert header value '{value}' into Volume - {err}"#
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                r#"Unable to convert header: {hdr_value:?} to string: {e}"#
            )),
        }
    }
}
