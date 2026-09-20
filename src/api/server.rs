use axum::{
    body::{Body, Bytes},
    extract::{FromRequest, MatchedPath, Request},
    http::{header, HeaderValue, Method},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};

use super::{impls::auth, proxy, ApiImpl};
use crate::observability::prometheus;
use agentenv_http_server::apis;
use agentenv_observability::metrics_handler;

pub fn new<I, A, E, C>(api_impl: I) -> Router
where
    I: AsRef<A> + AsRef<ApiImpl> + Clone + Send + Sync + 'static,
    A: apis::admin::Admin<E, Claims = C>
        + apis::default::Default<E>
        + apis::sandboxes::Sandboxes<E, Claims = C>
        + apis::snapshots::Snapshots<E, Claims = C>
        + apis::templates::Templates<E, Claims = C>
        + apis::volumes::Volumes<E, Claims = C>
        + apis::ApiKeyAuthHeader<Claims = C>
        + apis::ApiAuthBasic<Claims = C>
        + Send
        + Sync
        + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    // Keep the generated control-plane API as the primary router, then merge in
    // the hand-written `/proxy/*` entrypoints needed for the temporary reverse
    // proxy contract.
    agentenv_http_server::server::new::<I, A, E, C>(api_impl.clone())
        .route_layer(middleware::from_fn(optional_connect_body))
        .merge(proxy::router(api_impl.clone()))
        .merge(super::impls::image_build::router(api_impl.clone()))
        .route("/metrics", get(metrics_handler))
        .layer(middleware::from_fn_with_state(
            api_impl.clone(),
            proxy::sandbox_proxy_classifier::<I>,
        ))
        .layer(middleware::from_fn_with_state(
            api_impl,
            auth::require_auth::<I>,
        ))
        .layer(middleware::from_fn(prometheus::http_metrics_middleware))
}

// The generated Json<Option<ConnectSandboxV2>> extractor accepts JSON null,
// but rejects an absent body even though the v2 contract permits it.
async fn optional_connect_body(request: Request, next: Next) -> Response {
    let is_connect = request.method() == Method::POST
        && request
            .extensions()
            .get::<MatchedPath>()
            .is_some_and(|path| path.as_str() == "/v2/sandboxes/{sandbox_id}/connect");
    if !is_connect {
        return next.run(request).await;
    }

    let (mut parts, body) = request.into_parts();
    let bytes = match Bytes::from_request(Request::from_parts(parts.clone(), body), &()).await {
        Ok(bytes) => bytes,
        Err(error) => return error.into_response(),
    };
    let body = if bytes.is_empty() {
        parts.headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        parts
            .headers
            .insert(header::CONTENT_LENGTH, HeaderValue::from_static("4"));
        parts.headers.remove(header::TRANSFER_ENCODING);
        Body::from("null")
    } else {
        Body::from(bytes)
    };
    next.run(Request::from_parts(parts, body)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentenv_http_server::models::ConnectSandboxV2;
    use axum::{extract::DefaultBodyLimit, routing::post, Json};
    use http::StatusCode;
    use tower::ServiceExt;

    #[tokio::test]
    async fn optional_connect_body_preserves_json_and_body_limits() {
        let router = Router::new()
            .route(
                "/v2/sandboxes/{sandbox_id}/connect",
                post(|Json(body): Json<Option<ConnectSandboxV2>>| async move {
                    body.and_then(|body| body.timeout)
                        .unwrap_or(300)
                        .to_string()
                }),
            )
            .route_layer(middleware::from_fn(optional_connect_body))
            .layer(DefaultBodyLimit::max(64));

        for (body, content_type, status, expected) in [
            ("", None, StatusCode::OK, "300"),
            ("", Some("application/json"), StatusCode::OK, "300"),
            ("{}", Some("application/json"), StatusCode::OK, "300"),
            ("null", Some("application/json"), StatusCode::OK, "300"),
            (
                r#"{"timeout":60}"#,
                Some("application/json"),
                StatusCode::OK,
                "60",
            ),
            ("{", Some("application/json"), StatusCode::BAD_REQUEST, ""),
            (
                "{}",
                Some("text/plain"),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "",
            ),
            ("{}", None, StatusCode::UNSUPPORTED_MEDIA_TYPE, ""),
        ] {
            let mut request = Request::builder()
                .method(Method::POST)
                .uri("/v2/sandboxes/test/connect");
            if let Some(content_type) = content_type {
                request = request.header(header::CONTENT_TYPE, content_type);
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::from(body)).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                status,
                "body={body:?}, content_type={content_type:?}"
            );
            if status == StatusCode::OK {
                assert_eq!(
                    axum::body::to_bytes(response.into_body(), 64)
                        .await
                        .unwrap(),
                    expected
                );
            }
        }

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v2/sandboxes/test/connect")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(" ".repeat(65)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn optional_connect_body_does_not_change_v1() {
        let router = Router::new()
            .route(
                "/sandboxes/{sandbox_id}/connect",
                post(|Json(_): Json<ConnectSandboxV2>| async {}),
            )
            .route_layer(middleware::from_fn(optional_connect_body));
        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/sandboxes/test/connect")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
