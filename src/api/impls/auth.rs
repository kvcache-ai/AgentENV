use super::{ApiImpl, Claims};
use crate::{api::proxy, types::SandboxId};
use agentenv_http_server::apis;
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{rejection::RawPathParamsRejection, RawPathParams, Request, State},
    http::{header::HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

pub(crate) const API_KEY_HEADER: &str = "x-api-key";
pub(crate) const TRAFFIC_ACCESS_TOKEN_HEADER: &str = "e2b-traffic-access-token";
pub(crate) const ENVD_ACCESS_TOKEN_HEADER: &str = "x-access-token";

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    values.next().is_none().then_some(value)
}

impl ApiImpl {
    pub(crate) fn has_valid_api_key(&self, headers: &HeaderMap) -> bool {
        single_header(headers, API_KEY_HEADER)
            .is_some_and(|value| self.api_key.matches(value.as_bytes()))
    }

    pub(crate) fn traffic_access_token(&self, sandbox_id: SandboxId) -> String {
        self.orchestrator.traffic_access_token(sandbox_id)
    }

    fn has_valid_traffic_access_token(&self, headers: &HeaderMap, sandbox_id: SandboxId) -> bool {
        single_header(headers, TRAFFIC_ACCESS_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|candidate| {
                self.orchestrator
                    .validate_traffic_access_token(sandbox_id, candidate)
            })
    }
}

pub(crate) async fn require_auth<I>(
    State(api_impl): State<I>,
    params: Result<RawPathParams, RawPathParamsRejection>,
    mut request: Request,
    next: Next,
) -> Response<Body>
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    let proxy_request =
        proxy::is_sandbox_proxy_request(&request, api_impl.as_ref().sandbox_proxy_domains());
    if matches!(request.uri().path(), "/health" | "/metrics") && !proxy_request {
        return next.run(request).await;
    }

    let api_impl = api_impl.as_ref();
    if !proxy_request {
        return if api_impl.has_valid_api_key(request.headers()) {
            if let Some(id) = params
                .as_ref()
                .ok()
                .and_then(|params| params.iter().find(|(key, _)| *key == "sandbox_id"))
                .map(|(_, value)| value)
                .and_then(|id| SandboxId::parse_str(id).ok())
            {
                match api_impl.orchestrator().get_sandbox(&id).await {
                    Ok(Some(metadata)) if metadata.template_builder => {
                        return StatusCode::NOT_FOUND.into_response();
                    }
                    Ok(_) => {}
                    Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                }
            }
            next.run(request).await
        } else {
            StatusCode::UNAUTHORIZED.into_response()
        };
    }

    let has_api_key = api_impl.has_valid_api_key(request.headers());

    let Some((sandbox_id, target_port)) =
        proxy::route_for_auth(&request, api_impl.sandbox_proxy_domains())
    else {
        request.headers_mut().remove(ENVD_ACCESS_TOKEN_HEADER);
        return if proxy::has_proxy_prefix(request.uri().path()) || has_api_key {
            next.run(request).await
        } else {
            StatusCode::UNAUTHORIZED.into_response()
        };
    };
    if has_api_key {
        request.headers_mut().remove(API_KEY_HEADER);
    }
    let metadata = match api_impl.orchestrator().get_sandbox(&sandbox_id).await {
        Ok(Some(metadata)) => metadata,
        Ok(None) => {
            request.headers_mut().remove(ENVD_ACCESS_TOKEN_HEADER);
            return proxy::sandbox_not_found_response(sandbox_id);
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    if metadata.template_builder {
        return proxy::sandbox_not_found_response(sandbox_id);
    }

    let envd_request = target_port == proxy::effective_envd_port(&metadata);
    let envd_authorized = envd_request
        && metadata.secure
        && single_header(request.headers(), ENVD_ACCESS_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|candidate| {
                api_impl
                    .orchestrator()
                    .validate_envd_access_token(sandbox_id, candidate)
            });
    let authorized = if envd_request {
        !metadata.secure || envd_authorized
    } else {
        metadata.network_policy.allow_public_traffic
            || api_impl.has_valid_traffic_access_token(request.headers(), sandbox_id)
    };

    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if !envd_authorized {
        request.headers_mut().remove(ENVD_ACCESS_TOKEN_HEADER);
    }

    next.run(request).await
}

#[async_trait]
impl apis::ApiKeyAuthHeader for ApiImpl {
    type Claims = Claims;

    async fn extract_claims_from_header(
        &self,
        headers: &HeaderMap,
        _key: &str,
    ) -> Option<Self::Claims> {
        self.has_valid_api_key(headers).then_some(Claims)
    }
}

#[async_trait]
impl apis::ApiAuthBasic for ApiImpl {
    type Claims = Claims;

    async fn extract_claims_from_auth_header(
        &self,
        _kind: apis::BasicAuthKind,
        headers: &HeaderMap,
        _key: &str,
    ) -> Option<Self::Claims> {
        // The outer middleware is authoritative. This adapter keeps the
        // E2B-compatible generated router from rejecting its API-key request.
        self.has_valid_api_key(headers).then_some(Claims)
    }
}
