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
pub enum EventsWebhooksGetResponse {
    /// List of registered webhooks.
    Status200_ListOfRegisteredWebhooks(Vec<models::WebhookDetail>),
    /// Not found
    Status404_NotFound(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum EventsWebhooksWebhookIdGetResponse {
    /// Successfully returned the webhook configuration.
    Status200_SuccessfullyReturnedTheWebhookConfiguration(models::WebhookDetail),
    /// Not found
    Status404_NotFound(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum EventsWebhooksWebhookIdPatchResponse {
    /// Successfully updated webhook.
    Status200_SuccessfullyUpdatedWebhook(models::WebhookDetail),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

/// Webhooks
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait Webhooks<E: std::fmt::Debug + Send + Sync + 'static = ()>:
    super::ErrorHandler<E>
{
    type Claims;

    /// EventsWebhooksGet - GET /events/webhooks
    async fn events_webhooks_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
    ) -> Result<EventsWebhooksGetResponse, E>;

    /// EventsWebhooksWebhookIdGet - GET /events/webhooks/{webhookID}
    async fn events_webhooks_webhook_id_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::EventsWebhooksWebhookIdGetPathParams,
    ) -> Result<EventsWebhooksWebhookIdGetResponse, E>;

    /// EventsWebhooksWebhookIdPatch - PATCH /events/webhooks/{webhookID}
    async fn events_webhooks_webhook_id_patch(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::EventsWebhooksWebhookIdPatchPathParams,
        body: &models::WebhookConfiguration,
    ) -> Result<EventsWebhooksWebhookIdPatchResponse, E>;
}
