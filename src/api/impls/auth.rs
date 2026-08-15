use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header::HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use agentenv_http_server::apis;

use super::{ApiImpl, Claims};
use crate::api::proxy;

pub(crate) const API_KEY_HEADER: &str = "x-api-key";
pub(crate) const TRAFFIC_ACCESS_TOKEN_HEADER: &str = "e2b-traffic-access-token";
pub(crate) const ENVD_ACCESS_TOKEN_HEADER: &str = "x-access-token";
const TRAFFIC_TOKEN_PREFIX: &str = "aenv_trf_";
const TRAFFIC_TOKEN_CONTEXT: &[u8] = b"agentenv-sandbox-traffic-v1\0";

fn single_header_matches(headers: &HeaderMap, name: &str, expected: &str) -> bool {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }

    value.as_bytes() == expected.as_bytes()
}

fn derive_traffic_access_token(api_key: &[u8], sandbox_id: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(api_key).expect("HMAC accepts API keys of any length");
    mac.update(TRAFFIC_TOKEN_CONTEXT);
    mac.update(sandbox_id.as_bytes());
    format!(
        "{TRAFFIC_TOKEN_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

impl ApiImpl {
    pub(crate) fn has_valid_api_key(&self, headers: &HeaderMap) -> bool {
        single_header_matches(headers, API_KEY_HEADER, &self.api_key)
    }

    pub(crate) fn traffic_access_token(&self, sandbox_id: &str) -> String {
        derive_traffic_access_token(self.api_key.as_bytes(), sandbox_id)
    }

    fn has_valid_traffic_access_token(&self, headers: &HeaderMap, sandbox_id: &str) -> bool {
        let expected = self.traffic_access_token(sandbox_id);
        single_header_matches(headers, TRAFFIC_ACCESS_TOKEN_HEADER, &expected)
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

    let mut authorized = api_impl.as_ref().has_valid_api_key(request.headers());
    if !authorized && proxy_request {
        authorized =
            proxy::sandbox_id_for_proxy_auth(&request, api_impl.as_ref().sandbox_proxy_domains())
                .is_some_and(|sandbox_id| {
                    api_impl
                        .as_ref()
                        .has_valid_traffic_access_token(request.headers(), &sandbox_id)
                });
    }
    if !authorized && proxy_request {
        if let Some((sandbox_id, target_port, candidate)) = proxy::envd_access_token_for_proxy_auth(
            &request,
            api_impl.as_ref().sandbox_proxy_domains(),
        ) {
            authorized = proxy::has_valid_envd_access_token(
                api_impl.as_ref(),
                sandbox_id,
                target_port,
                candidate,
            )
            .await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_match_requires_one_exact_value() {
        let mut headers = HeaderMap::new();
        assert!(!single_header_matches(
            &headers,
            API_KEY_HEADER,
            "correct-key"
        ));
        headers.insert(API_KEY_HEADER, "correct-key".parse().unwrap());
        assert!(single_header_matches(
            &headers,
            API_KEY_HEADER,
            "correct-key"
        ));
        assert!(!single_header_matches(
            &headers,
            API_KEY_HEADER,
            "wrong-key"
        ));

        headers.append(API_KEY_HEADER, "correct-key".parse().unwrap());
        assert!(!single_header_matches(
            &headers,
            API_KEY_HEADER,
            "correct-key"
        ));
    }

    #[test]
    fn traffic_access_token_matches_gateway_contract() {
        assert_eq!(
            derive_traffic_access_token(b"test-key", "0191f4d0-7b2a-7c11-9c2d-0123456789ab"),
            "aenv_trf_PwHqhTxLa_mzUCNIGx03uiTHxZ3k995pKDOS50PaGWo"
        );
    }
}
