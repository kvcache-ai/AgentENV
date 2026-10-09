//! E2B-compatible `/events/webhooks*` endpoints backed by [`WebhookService`].

use std::sync::Arc;

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;
use tracing::warn;

use agentenv_http_server::apis::webhooks::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::ApiImpl;
use crate::webhook::{
    DeliveryQuery, DeliveryRecord, DurationStats, NewWebhook, WebhookError, WebhookPatch,
    WebhookRecord, WebhookService, TEAM_ID,
};

const DEFAULT_DELIVERY_PAGE_SIZE: usize = 25;

impl ApiImpl {
    fn webhook_service(&self) -> Result<Arc<WebhookService>, models::Error> {
        self.webhooks
            .clone()
            .ok_or_else(|| Self::error(404, "event webhooks are not enabled on this server"))
    }
}

fn error_response(error: WebhookError) -> models::Error {
    match error {
        WebhookError::InvalidRequest(message) => ApiImpl::error(400, message),
        WebhookError::NotFound => ApiImpl::error(404, "webhook not found"),
        WebhookError::Repository(error) => {
            warn!(error = %error, "webhook repository operation failed");
            ApiImpl::internal_error(&error)
        }
        WebhookError::Internal(error) => {
            warn!(error = %format_args!("{error:#}"), "webhook operation failed");
            ApiImpl::internal_error(error.as_ref())
        }
    }
}

/// Runs `call` against the service, mapping every failure to an API error.
async fn with_service<T, F, Fut>(api: &ApiImpl, call: F) -> Result<T, models::Error>
where
    F: FnOnce(Arc<WebhookService>) -> Fut,
    Fut: std::future::Future<Output = Result<T, WebhookError>>,
{
    call(api.webhook_service()?).await.map_err(error_response)
}

fn detail(record: WebhookRecord) -> models::WebhookDetail {
    models::WebhookDetail {
        id: record.id.to_string(),
        team_id: TEAM_ID.to_string(),
        name: record.name,
        created_at: record.created_at,
        url: record.url,
        enabled: record.enabled,
        events: record.events,
    }
}

fn creation(record: WebhookRecord) -> models::WebhookCreation {
    let detail = detail(record);
    models::WebhookCreation {
        id: detail.id,
        name: detail.name,
        created_at: detail.created_at,
        team_id: detail.team_id,
        url: detail.url,
        enabled: detail.enabled,
        events: detail.events,
    }
}

fn nullable<T>(value: Option<T>) -> Option<Nullable<T>> {
    Some(value.map_or(Nullable::Null, Nullable::Present))
}

fn delivery(record: DeliveryRecord) -> models::WebhookDelivery {
    models::WebhookDelivery {
        id: record.id,
        team_id: TEAM_ID,
        webhook_id: record.webhook_id,
        event_id: record.event_id,
        sandbox_id: record.sandbox_id,
        event_type: record.event_type,
        status: if record.success { "success" } else { "failed" }.to_string(),
        duration_ms: i32::try_from(record.duration_ms).unwrap_or(i32::MAX),
        request_body: record.request_body,
        request_headers: record.request_headers,
        request_url: record.request_url,
        response_body: nullable(record.response_body),
        response_headers: nullable(record.response_headers),
        response_http_status_code: nullable(record.response_status.map(i32::from)),
        error_class: record.error_class.map_or(Nullable::Null, Nullable::Present),
        error_message: nullable(record.error_message),
        timestamp: record.timestamp,
    }
}

fn duration(stats: DurationStats) -> models::WebhookDeliveryDurationStats {
    models::WebhookDeliveryDurationStats {
        minimum: stats.minimum,
        average: stats.average,
        maximum: stats.maximum,
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
        Ok(
            match with_service(self, |service| async move { service.list().await }).await {
                Ok(records) => EventsWebhooksGetResponse::Status200_ListOfRegisteredWebhooks(
                    records.into_iter().map(detail).collect(),
                ),
                Err(err) if err.code == 404 => EventsWebhooksGetResponse::Status404_NotFound(err),
                Err(err) => EventsWebhooksGetResponse::Status500_ServerError(err),
            },
        )
    }

    async fn events_webhooks_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::WebhookCreate,
    ) -> Result<EventsWebhooksPostResponse, ()> {
        let input = NewWebhook {
            name: body.name.clone(),
            url: body.url.clone(),
            events: body.events.clone(),
            enabled: body.enabled,
            signature_secret: body.signature_secret.clone(),
        };
        Ok(
            match with_service(self, |service| async move { service.create(input).await }).await {
                Ok(record) => EventsWebhooksPostResponse::Status201_SuccessfullyCreatedWebhook(
                    creation(record),
                ),
                Err(err) if err.code == 400 => {
                    EventsWebhooksPostResponse::Status400_BadRequest(err)
                }
                Err(err) if err.code == 404 => EventsWebhooksPostResponse::Status404_NotFound(err),
                Err(err) => EventsWebhooksPostResponse::Status500_ServerError(err),
            },
        )
    }

    async fn events_webhooks_webhook_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::EventsWebhooksWebhookIdDeletePathParams,
    ) -> Result<EventsWebhooksWebhookIdDeleteResponse, ()> {
        let id = path_params.webhook_id;
        Ok(
            match with_service(self, |service| async move { service.delete(id).await }).await {
                Ok(()) => {
                    EventsWebhooksWebhookIdDeleteResponse::Status200_SuccessfullyDeletedWebhook
                }
                Err(err) if err.code == 404 => {
                    EventsWebhooksWebhookIdDeleteResponse::Status404_NotFound(err)
                }
                Err(err) => EventsWebhooksWebhookIdDeleteResponse::Status500_ServerError(err),
            },
        )
    }

    async fn events_webhooks_webhook_id_deliveries_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::EventsWebhooksWebhookIdDeliveriesGetPathParams,
        query_params: &models::EventsWebhooksWebhookIdDeliveriesGetQueryParams,
    ) -> Result<EventsWebhooksWebhookIdDeliveriesGetResponse, ()> {
        let id = path_params.webhook_id;
        let query = DeliveryQuery {
            cursor: query_params.cursor.clone(),
            limit: query_params
                .limit
                .map_or(DEFAULT_DELIVERY_PAGE_SIZE, |limit| limit as usize),
            order_asc: query_params.order_asc.unwrap_or(false),
            start: query_params.start,
            end: query_params.end,
            statuses: query_params.delivery_status.clone(),
            event_types: query_params.event_type.clone(),
        };
        let result = with_service(self, |service| async move {
            service.deliveries(id, query).await
        })
        .await;
        Ok(match result {
            Ok(page) => EventsWebhooksWebhookIdDeliveriesGetResponse::Status200_ListOfWebhookDeliveryAttemptsGroupedByEvent(
                models::WebhookDeliveriesListPayload {
                    data: page
                        .groups
                        .into_iter()
                        .map(|group| models::WebhookDeliveryGroup {
                            event_id: group.event_id,
                            event_type: group.event_type,
                            sandbox_id: group.sandbox_id,
                            attempts: group.attempts.into_iter().map(delivery).collect(),
                        })
                        .collect(),
                    next_cursor: page.next_cursor.map_or(Nullable::Null, Nullable::Present),
                },
            ),
            Err(err) if err.code == 400 => {
                EventsWebhooksWebhookIdDeliveriesGetResponse::Status400_BadRequest(err)
            }
            Err(err) if err.code == 404 => {
                EventsWebhooksWebhookIdDeliveriesGetResponse::Status404_NotFound(err)
            }
            Err(err) => EventsWebhooksWebhookIdDeliveriesGetResponse::Status500_ServerError(err),
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
        let id = path_params.webhook_id;
        Ok(
            match with_service(self, |service| async move { service.get(id).await }).await {
                Ok(record) => {
                    EventsWebhooksWebhookIdGetResponse::Status200_SuccessfullyReturnedTheWebhookConfiguration(
                        detail(record),
                    )
                }
                Err(err) if err.code == 404 => {
                    EventsWebhooksWebhookIdGetResponse::Status404_NotFound(err)
                }
                Err(err) => EventsWebhooksWebhookIdGetResponse::Status500_ServerError(err),
            },
        )
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
        let id = path_params.webhook_id;
        let patch = WebhookPatch {
            name: body.name.clone(),
            url: body.url.clone(),
            events: body.events.clone(),
            enabled: body.enabled,
            signature_secret: body.signature_secret.clone(),
        };
        Ok(
            match with_service(
                self,
                |service| async move { service.update(id, patch).await },
            )
            .await
            {
                Ok(record) => {
                    EventsWebhooksWebhookIdPatchResponse::Status200_SuccessfullyUpdatedWebhook(
                        detail(record),
                    )
                }
                Err(err) if err.code == 400 => {
                    EventsWebhooksWebhookIdPatchResponse::Status400_BadRequest(err)
                }
                Err(err) if err.code == 404 => {
                    EventsWebhooksWebhookIdPatchResponse::Status404_NotFound(err)
                }
                Err(err) => EventsWebhooksWebhookIdPatchResponse::Status500_ServerError(err),
            },
        )
    }

    async fn events_webhooks_webhook_id_stats_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::EventsWebhooksWebhookIdStatsGetPathParams,
        query_params: &models::EventsWebhooksWebhookIdStatsGetQueryParams,
    ) -> Result<EventsWebhooksWebhookIdStatsGetResponse, ()> {
        let id = path_params.webhook_id;
        let (start, end) = (query_params.start, query_params.end);
        let result = with_service(self, |service| async move {
            service.stats(id, start, end).await
        })
        .await;
        Ok(match result {
            Ok(stats) => EventsWebhooksWebhookIdStatsGetResponse::Status200_WebhookDeliveryStats(
                models::WebhookDeliveryStats {
                    buckets: stats
                        .buckets
                        .into_iter()
                        .map(|bucket| models::WebhookDeliveryStatsBucket {
                            timestamp: bucket.timestamp,
                            total: bucket.total,
                            failed: bucket.failed,
                            duration_ms: duration(bucket.duration_ms),
                        })
                        .collect(),
                    total: stats.total,
                    failed: stats.failed,
                    duration_ms: duration(stats.duration_ms),
                },
            ),
            Err(err) if err.code == 400 => {
                EventsWebhooksWebhookIdStatsGetResponse::Status400_BadRequest(err)
            }
            Err(err) if err.code == 404 => {
                EventsWebhooksWebhookIdStatsGetResponse::Status404_NotFound(err)
            }
            Err(err) => EventsWebhooksWebhookIdStatsGetResponse::Status500_ServerError(err),
        })
    }
}

#[cfg(test)]
mod tests {
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
        snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig},
        snapshot::SnapshotManager,
        template::TemplateBuilder,
    };

    const API_KEY: &str = "e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    async fn app(webhooks: bool) -> (tempfile::TempDir, axum::Router) {
        let root = tempfile::tempdir().unwrap();
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.path().join("repository"),
            cache_root: Some(root.path().join("cache")),
            runtime_cache_root: None,
        })
        .unwrap();
        let snapshot_manager = Arc::new(SnapshotManager::from_parts(
            backend.repository(),
            backend.runtime_resolver(),
            None,
        ));
        let orchestrator = Orchestrator::new(
            crate::orchestrator::InMemoryMetadataStore::new(),
            crate::sandbox::FirecrackerSandboxFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().join("sandboxes")),
        )
        .await
        .unwrap();
        let volume_manager = Arc::new(
            crate::volume::VolumeManager::open_with_repository(
                root.path().join("volumes/catalog.json"),
                backend.repository(),
            )
            .await
            .unwrap(),
        );
        let mut api = ApiImpl::new(
            orchestrator,
            snapshot_manager,
            Arc::new(TemplateBuilder::new()),
            Arc::new(ImageResolver::new(&AppConfig::default())),
            volume_manager,
            None,
            Vec::new(),
            ApiKey::new(API_KEY).unwrap(),
        );
        if webhooks {
            let service =
                WebhookService::open(backend.repository(), root.path().join("deliveries"))
                    .await
                    .unwrap();
            api = api.with_webhooks(service);
        }
        (root, server::new(Arc::new(api)))
    }

    async fn call(
        app: &axum::Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "localhost")
            .header("x-api-key", API_KEY);
        if body.is_some() {
            request = request.header(header::CONTENT_TYPE, "application/json");
        }
        let body = body.map_or_else(Body::empty, |body| Body::from(body.to_string()));
        let response = app
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn webhook_endpoints_round_trip() {
        let (_root, app) = app(true).await;
        let (status, created) = call(
            &app,
            "POST",
            "/events/webhooks",
            Some(json!({
                "name": "hook",
                "url": "https://example.com/hook",
                "events": ["sandbox.lifecycle.created"],
                "signatureSecret": "secret",
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        assert_eq!(created["enabled"], true);
        assert_eq!(created["teamId"], TEAM_ID.to_string());
        assert!(created.get("signatureSecret").is_none());
        let id = created["id"].as_str().unwrap().to_string();

        let (status, list) = call(&app, "GET", "/events/webhooks", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(list.as_array().unwrap().len(), 1);

        let (status, patched) = call(
            &app,
            "PATCH",
            &format!("/events/webhooks/{id}"),
            Some(json!({"enabled": false, "events": ["sandbox.lifecycle.killed"]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{patched}");
        assert_eq!(patched["enabled"], false);
        assert_eq!(patched["events"], json!(["sandbox.lifecycle.killed"]));

        let (status, deliveries) = call(
            &app,
            "GET",
            &format!("/events/webhooks/{id}/deliveries?limit=10"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(deliveries, json!({"data": [], "nextCursor": null}));

        let (status, stats) =
            call(&app, "GET", &format!("/events/webhooks/{id}/stats"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stats["total"], 0);
        assert_eq!(stats["buckets"].as_array().unwrap().len(), 25);

        let (status, _) = call(
            &app,
            "GET",
            &format!(
                "/events/webhooks/{id}/stats?start=2026-01-02T00:00:00Z&end=2026-01-01T00:00:00Z"
            ),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _) = call(&app, "DELETE", &format!("/events/webhooks/{id}"), None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&app, "GET", &format!("/events/webhooks/{id}"), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn webhook_endpoints_validate_input_and_auth() {
        let (_root, app) = app(true).await;
        let (status, body) = call(
            &app,
            "POST",
            "/events/webhooks",
            Some(json!({
                "name": "hook",
                "url": "https://example.com/hook",
                "events": ["sandbox.lifecycle.unknown"],
                "signatureSecret": "secret",
            })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("unknown event type"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/events/webhooks")
                    .header(header::HOST, "localhost")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn webhook_endpoints_return_404_when_disabled() {
        let (_root, app) = app(false).await;
        let (status, _) = call(&app, "GET", "/events/webhooks", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
