use agentenv_http_server::apis;
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header::HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

use super::{ApiImpl, Claims};
use crate::{api::proxy, types::SandboxId};

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
            .is_some_and(|value| value.as_bytes() == self.api_key.as_bytes())
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
    request: Request,
    next: Next,
) -> Response<Body>
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    let proxy_request =
        proxy::is_sandbox_proxy_request(&request, api_impl.as_ref().sandbox_proxy_domains());
    if request.uri().path() == "/health" && !proxy_request {
        return next.run(request).await;
    }

    let api_impl = api_impl.as_ref();
    let mut authorized = api_impl.has_valid_api_key(request.headers());
    if !authorized && proxy_request {
        if let Some((sandbox_id, target_port)) =
            proxy::route_for_auth(&request, api_impl.sandbox_proxy_domains())
        {
            authorized = api_impl.has_valid_traffic_access_token(request.headers(), sandbox_id);
            let envd_candidate = single_header(request.headers(), ENVD_ACCESS_TOKEN_HEADER)
                .and_then(|value| value.to_str().ok());
            if !authorized {
                if let Some(candidate) = envd_candidate {
                    authorized = proxy::has_valid_envd_access_token(
                        api_impl,
                        sandbox_id,
                        target_port,
                        candidate,
                    )
                    .await;
                }
            }
        }
    }

    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
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
