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
pub enum TemplatesAliasesAliasGetResponse {
    /// Successfully queried template by alias
    Status200_SuccessfullyQueriedTemplateByAlias(models::TemplateAliasResponse),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Forbidden
    Status403_Forbidden(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum TemplatesGetResponse {
    /// Successfully returned all templates
    Status200_SuccessfullyReturnedAllTemplates(Vec<models::Template>),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum TemplatesTemplateIdBuildsBuildIdStatusGetResponse {
    /// Successfully returned the template
    Status200_SuccessfullyReturnedTheTemplate(models::TemplateBuildInfo),
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
pub enum TemplatesTemplateIdDeleteResponse {
    /// The template was deleted successfully
    Status204_TheTemplateWasDeletedSuccessfully,
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum TemplatesTemplateIdFilesHashGetResponse {
    /// Successfully returned the upload link
    Status201_SuccessfullyReturnedTheUploadLink(models::TemplateBuildFileUpload),
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
pub enum TemplatesTemplateIdGetResponse {
    /// Successfully returned the template with its builds
    Status200_SuccessfullyReturnedTheTemplateWithItsBuilds(models::TemplateWithBuilds),
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
pub enum V2TemplatesGetResponse {
    /// Successfully returned all templates
    Status200_SuccessfullyReturnedAllTemplates {
        body: Vec<models::Template>,
        x_next_token: Option<String>,
    },
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Forbidden
    Status403_Forbidden(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum V2TemplatesTemplateIdBuildsBuildIdPostResponse {
    /// The build has started
    Status202_TheBuildHasStarted,
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
pub enum V3TemplatesPostResponse {
    /// The build was requested successfully
    Status202_TheBuildWasRequestedSuccessfully(models::TemplateRequestResponseV3),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

/// Templates
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait Templates<E: std::fmt::Debug + Send + Sync + 'static = ()>:
    super::ErrorHandler<E>
{
    type Claims;

    /// Check template alias.
    ///
    /// TemplatesAliasesAliasGet - GET /templates/aliases/{alias}
    async fn templates_aliases_alias_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::TemplatesAliasesAliasGetPathParams,
    ) -> Result<TemplatesAliasesAliasGetResponse, E>;

    /// List templates.
    ///
    /// TemplatesGet - GET /templates
    async fn templates_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::TemplatesGetQueryParams,
    ) -> Result<TemplatesGetResponse, E>;

    /// Template build status.
    ///
    /// TemplatesTemplateIdBuildsBuildIdStatusGet - GET /templates/{templateID}/builds/{buildID}/status
    async fn templates_template_id_builds_build_id_status_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdBuildsBuildIdStatusGetPathParams,
    ) -> Result<TemplatesTemplateIdBuildsBuildIdStatusGetResponse, E>;

    /// Delete template.
    ///
    /// TemplatesTemplateIdDelete - DELETE /templates/{templateID}
    async fn templates_template_id_delete(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdDeletePathParams,
    ) -> Result<TemplatesTemplateIdDeleteResponse, E>;

    /// Template build file upload link.
    ///
    /// TemplatesTemplateIdFilesHashGet - GET /templates/{templateID}/files/{hash}
    async fn templates_template_id_files_hash_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdFilesHashGetPathParams,
    ) -> Result<TemplatesTemplateIdFilesHashGetResponse, E>;

    /// List template builds.
    ///
    /// TemplatesTemplateIdGet - GET /templates/{templateID}
    async fn templates_template_id_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdGetPathParams,
        query_params: &models::TemplatesTemplateIdGetQueryParams,
    ) -> Result<TemplatesTemplateIdGetResponse, E>;

    /// List templates (v2).
    ///
    /// V2TemplatesGet - GET /v2/templates
    async fn v2_templates_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::V2TemplatesGetQueryParams,
    ) -> Result<V2TemplatesGetResponse, E>;

    /// Start template build (v2).
    ///
    /// V2TemplatesTemplateIdBuildsBuildIdPost - POST /v2/templates/{templateID}/builds/{buildID}
    async fn v2_templates_template_id_builds_build_id_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::V2TemplatesTemplateIdBuildsBuildIdPostPathParams,
        body: &models::TemplateBuildStartV2,
    ) -> Result<V2TemplatesTemplateIdBuildsBuildIdPostResponse, E>;

    /// Create template (v3).
    ///
    /// V3TemplatesPost - POST /v3/templates
    async fn v3_templates_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        body: &models::TemplateBuildRequestV3,
    ) -> Result<V3TemplatesPostResponse, E>;
}
