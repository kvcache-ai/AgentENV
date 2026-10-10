//! E2B-compatible `/events/webhooks*` endpoints backed by the custom
//! extension. Only list, get, and update are exposed; see [`crate::webhook`].

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;
use tracing::warn;

use agentenv_http_server::apis::webhooks::*;
use agentenv_http_server::models;

use super::ApiImpl;
use crate::webhook::{WebhookError, WebhookPatch, WebhookView, TEAM_ID};

fn error_response(error: WebhookError) -> models::Error {
    match error {
        WebhookError::InvalidRequest(message) => ApiImpl::error(400, message),
        WebhookError::NotFound => ApiImpl::error(404, "webhook not found"),
        WebhookError::Repository(error) => {
            warn!(error = %error, "webhook repository operation failed");
            ApiImpl::internal_error(&error)
        }
    }
}

fn detail(view: WebhookView) -> models::WebhookDetail {
    models::WebhookDetail {
        id: view.id.to_string(),
        team_id: TEAM_ID.to_string(),
        name: view.config.name,
        created_at: view.config.created_at,
        url: view.url,
        enabled: view.config.enabled,
        events: view.config.events,
    }
}

#[async_trait]
impl Webhooks<()> for ApiImpl {
    type Claims = super::Claims;

    async fn events_webhooks_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
    ) -> Result<EventsWebhooksGetResponse, ()> {
        Ok(match self.webhooks.list().await {
            Ok(views) => EventsWebhooksGetResponse::Status200_ListOfRegisteredWebhooks(
                views.into_iter().map(detail).collect(),
            ),
            Err(error) => EventsWebhooksGetResponse::Status500_ServerError(error_response(error)),
        })
    }

    async fn events_webhooks_webhook_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::EventsWebhooksWebhookIdGetPathParams,
    ) -> Result<EventsWebhooksWebhookIdGetResponse, ()> {
        Ok(match self.webhooks.get(path_params.webhook_id).await {
            Ok(view) => {
                EventsWebhooksWebhookIdGetResponse::Status200_SuccessfullyReturnedTheWebhookConfiguration(
                    detail(view),
                )
            }
            Err(error @ WebhookError::NotFound) => {
                EventsWebhooksWebhookIdGetResponse::Status404_NotFound(error_response(error))
            }
            Err(error) => {
                EventsWebhooksWebhookIdGetResponse::Status500_ServerError(error_response(error))
            }
        })
    }

    async fn events_webhooks_webhook_id_patch(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::EventsWebhooksWebhookIdPatchPathParams,
        body: &models::WebhookConfiguration,
    ) -> Result<EventsWebhooksWebhookIdPatchResponse, ()> {
        let patch = WebhookPatch {
            name: body.name.clone(),
            url: body.url.clone(),
            events: body.events.clone(),
            enabled: body.enabled,
            signature_secret: body.signature_secret.clone(),
        };
        Ok(
            match self.webhooks.update(path_params.webhook_id, patch).await {
                Ok(view) => {
                    EventsWebhooksWebhookIdPatchResponse::Status200_SuccessfullyUpdatedWebhook(
                        detail(view),
                    )
                }
                Err(error @ WebhookError::InvalidRequest(_)) => {
                    EventsWebhooksWebhookIdPatchResponse::Status400_BadRequest(error_response(
                        error,
                    ))
                }
                Err(error @ WebhookError::NotFound) => {
                    EventsWebhooksWebhookIdPatchResponse::Status404_NotFound(error_response(error))
                }
                Err(error) => EventsWebhooksWebhookIdPatchResponse::Status500_ServerError(
                    error_response(error),
                ),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use http::{header, Request, StatusCode};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use super::*;
    use crate::{
        api::server,
        api_key::ApiKey,
        cfg::AppConfig,
        image::ImageResolver,
        orchestrator::{FileBackedSandboxPersister, Orchestrator},
        snapshot::mock::mock_snapshot_manager,
        template::TemplateBuilder,
        webhook::EXTENSION_WEBHOOK_ID,
    };

    const API_KEY: &str = "e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// Service behavior with an extension is covered in `crate::webhook`;
    /// the test config has no `[custom_extension].url`, so this checks the
    /// routes are wired and report no webhook.
    #[tokio::test]
    async fn routes_without_extension() {
        let root = tempfile::tempdir().unwrap();
        let snapshot_manager = Arc::new(mock_snapshot_manager());
        let volume_manager = Arc::new(
            crate::volume::VolumeManager::open_with_repository(
                root.path().join("volumes/catalog.json"),
                snapshot_manager.repository(),
            )
            .await
            .unwrap(),
        );
        let orchestrator = Orchestrator::new(
            crate::orchestrator::InMemoryMetadataStore::new(),
            crate::sandbox::FirecrackerSandboxFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().join("sandboxes")),
        )
        .await
        .unwrap();
        let app = server::new(Arc::new(ApiImpl::new(
            orchestrator,
            snapshot_manager,
            Arc::new(TemplateBuilder::new()),
            Arc::new(ImageResolver::new(&AppConfig::default())),
            volume_manager,
            None,
            Vec::new(),
            ApiKey::new(API_KEY).unwrap(),
        )));

        let call = |method: &'static str, uri: String, body: Option<Value>| {
            let app = app.clone();
            async move {
                let mut request = Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(header::HOST, "localhost")
                    .header("x-api-key", API_KEY);
                if body.is_some() {
                    request = request.header(header::CONTENT_TYPE, "application/json");
                }
                let body = body.map_or_else(Body::empty, |body| Body::from(body.to_string()));
                let response = app.oneshot(request.body(body).unwrap()).await.unwrap();
                let status = response.status();
                let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
                    .await
                    .unwrap();
                (
                    status,
                    serde_json::from_slice(&bytes).unwrap_or(Value::Null),
                )
            }
        };
        let path = format!("/events/webhooks/{EXTENSION_WEBHOOK_ID}");

        assert_eq!(
            call("GET", "/events/webhooks".into(), None).await,
            (StatusCode::OK, json!([]))
        );
        assert_eq!(
            call("GET", path.clone(), None).await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            call("PATCH", path.clone(), Some(json!({"enabled": true})))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        // Create, delete, deliveries, and stats are not part of the API.
        assert_eq!(
            call("POST", "/events/webhooks".into(), Some(json!({})))
                .await
                .0,
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            call("DELETE", path.clone(), None).await.0,
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            call("GET", format!("{path}/deliveries"), None).await.0,
            StatusCode::NOT_FOUND
        );
    }
}
