use std::future::Future;
use std::time::Duration;

use anyhow::Context;
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::services::{Oss, S3};
use thiserror::Error;

use crate::auth::{CachedCredentialSource, ResolvedCredential};
use crate::{OpenDalError, OpenDalErrorKind, OpenDalResult, Operator};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_RETRIES: usize = 3;

pub type ObjectStoreOperatorResult<T> = std::result::Result<T, ObjectStoreOperatorError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AddressingStyle {
    Virtual,
    Path,
}

impl AddressingStyle {
    pub fn uses_virtual_host_style(&self) -> bool {
        matches!(self, Self::Virtual)
    }
}

/// The API an operator speaks. Every supported store speaks S3; Alibaba OSS's
/// native API additionally appends to objects, which its S3 API cannot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ObjectStoreService {
    #[default]
    S3,
    AliyunOss,
}

#[derive(Clone, Debug)]
pub struct ObjectStoreOperatorConfig {
    pub service: ObjectStoreService,
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub addressing_style: AddressingStyle,
    pub timeout: Option<Duration>,
    pub max_retries: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct OperatorWithCredential {
    operator: Operator,
    credential: Option<ResolvedCredential>,
}

impl OperatorWithCredential {
    pub fn new(operator: Operator, credential: Option<ResolvedCredential>) -> Self {
        Self {
            operator,
            credential,
        }
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    pub fn credential(&self) -> Option<&ResolvedCredential> {
        self.credential.as_ref()
    }
}

#[derive(Debug, Error)]
pub enum ObjectStoreOperatorError {
    #[error(transparent)]
    OpenDal(#[from] OpenDalError),
    #[error("credential refresh failed")]
    CredentialRefresh(#[source] anyhow::Error),
    #[error("operator build failed")]
    OperatorBuild(#[source] anyhow::Error),
}

pub fn build_object_store_operator(
    config: &ObjectStoreOperatorConfig,
    credential: Option<&ResolvedCredential>,
) -> ObjectStoreOperatorResult<Operator> {
    let operator = match config.service {
        ObjectStoreService::S3 => Operator::new(s3_builder(config, credential)).map(|b| b.finish()),
        ObjectStoreService::AliyunOss => {
            Operator::new(aliyun_oss_builder(config, credential)).map(|b| b.finish())
        }
    }
    .context("build OpenDAL object-store operator")
    .map_err(ObjectStoreOperatorError::OperatorBuild)?;

    let timeout = config.timeout.unwrap_or(DEFAULT_TIMEOUT);
    let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
    Ok(operator
        .layer(
            TimeoutLayer::new()
                .with_timeout(timeout)
                .with_io_timeout(timeout),
        )
        .layer(RetryLayer::new().with_max_times(max_retries)))
}

fn aliyun_oss_builder(
    config: &ObjectStoreOperatorConfig,
    credential: Option<&ResolvedCredential>,
) -> Oss {
    let mut builder = Oss::default()
        .bucket(&config.bucket)
        .endpoint(&config.endpoint);
    if let Some(credential) = credential {
        builder = builder.access_key_id(&credential.access_key_id);
        builder = builder.access_key_secret(&credential.secret_access_key);
        if let Some(token) = credential.security_token.as_deref() {
            builder = builder.security_token(token);
        }
    } else {
        builder = builder.allow_anonymous();
    }
    builder
}

fn s3_builder(config: &ObjectStoreOperatorConfig, credential: Option<&ResolvedCredential>) -> S3 {
    let mut builder = S3::default()
        .bucket(&config.bucket)
        .endpoint(&config.endpoint)
        .region(&config.region);

    if config.addressing_style.uses_virtual_host_style() {
        builder = builder.enable_virtual_host_style();
    }

    if let Some(credential) = credential {
        builder = builder.access_key_id(&credential.access_key_id);
        builder = builder.secret_access_key(&credential.secret_access_key);
        if let Some(token) = credential.security_token.as_deref() {
            builder = builder.session_token(token);
        }
    } else {
        builder = builder.allow_anonymous();
    }
    builder
}

pub async fn run_with_refresh<T, F, Fut>(
    current: &OperatorWithCredential,
    credentials: Option<&CachedCredentialSource>,
    config: &ObjectStoreOperatorConfig,
    op: F,
) -> ObjectStoreOperatorResult<(T, Option<OperatorWithCredential>)>
where
    F: Fn(Operator) -> Fut,
    Fut: Future<Output = OpenDalResult<T>>,
{
    match op(current.operator().clone()).await {
        Ok(value) => Ok((value, None)),
        Err(err) if err.kind() == OpenDalErrorKind::PermissionDenied => {
            // Only permission-denied responses trigger credential refresh.
            // This matches the backends we currently target, including Aliyun
            // OSS, where expired credentials surface as 403-style failures.
            // Other authentication errors are treated as terminal until we
            // explicitly broaden the retry contract.
            let Some(credentials) = credentials else {
                return Err(ObjectStoreOperatorError::OpenDal(err));
            };
            let Some(previous) = current.credential() else {
                return Err(ObjectStoreOperatorError::OpenDal(err));
            };
            let Some(refreshed) = credentials
                .force_refresh(previous)
                .await
                .map_err(ObjectStoreOperatorError::CredentialRefresh)?
            else {
                return Err(ObjectStoreOperatorError::OpenDal(err));
            };
            let operator = build_object_store_operator(config, Some(&refreshed))?;
            let value = op(operator.clone()).await?;
            Ok((
                value,
                Some(OperatorWithCredential::new(operator, Some(refreshed))),
            ))
        }
        Err(err) => Err(ObjectStoreOperatorError::OpenDal(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::AddressingStyle;

    #[test]
    fn addressing_style_maps_to_expected_s3_modes() {
        assert!(AddressingStyle::Virtual.uses_virtual_host_style());
        assert!(!AddressingStyle::Path.uses_virtual_host_style());
    }
}
