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
pub enum AdmissionGetResponse {
    /// Runtime admission is ready
    Status204_RuntimeAdmissionIsReady,
    /// Runtime admission is blocked
    Status503_RuntimeAdmissionIsBlocked(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum HealthGetResponse {
    /// The service is healthy
    Status204_TheServiceIsHealthy,
    /// Authentication error
    Status401_AuthenticationError(models::Error),
}

/// Default
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait Default<E: std::fmt::Debug + Send + Sync + 'static = ()>: super::ErrorHandler<E> {
    /// Check whether the runtime accepts write-heavy sandbox operations.
    ///
    /// AdmissionGet - GET /admission
    async fn admission_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
    ) -> Result<AdmissionGetResponse, E>;

    /// Health check.
    ///
    /// HealthGet - GET /health
    async fn health_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
    ) -> Result<HealthGetResponse, E>;
}
