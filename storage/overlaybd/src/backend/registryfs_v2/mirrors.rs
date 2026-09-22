use super::*;
use crate::config::{registry_authority as authority, validate_registry_mirror_hosts};
use std::sync::Mutex as StdMutex;

const MIRROR_COOLDOWN: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(super) struct MirrorEndpoint {
    origin: Url,
    reader: Arc<RegistryFSImplV2>,
    retry_after: StdMutex<Option<Instant>>,
}

pub(super) struct RangeRead {
    pub body: Bytes,
    pub total: u64,
}

pub(super) fn build_endpoints(
    options: &RegistryFsV2Options,
    credential: &CredentialMode,
) -> Result<HashMap<String, Vec<MirrorEndpoint>>> {
    let config = &options.registry_mirrors;
    validate_registry_mirror_hosts(config)?;
    if config.is_empty() || !options.accelerate_address.is_empty() {
        return Ok(HashMap::new());
    }
    anyhow::ensure!(
        !matches!(credential, CredentialMode::Static { .. }),
        "native registry mirrors require endpoint-specific credentials"
    );
    let mut result = HashMap::new();
    for (host, mirrors) in config {
        let mut endpoints = Vec::new();
        for mirror in mirrors {
            let origin = Url::parse(mirror)?;
            let mut endpoint_options = options.clone();
            endpoint_options.registry_mirrors.clear();
            // Separate endpoint clients also partition bearer-token and redirect caches.
            let reader = RegistryFsV2::with_credential_mode(endpoint_options, credential.clone())?;
            endpoints.push(MirrorEndpoint {
                origin,
                reader: reader.inner,
                retry_after: StdMutex::new(None),
            });
        }
        result.insert(host.clone(), endpoints);
    }
    Ok(result)
}

impl RegistryFSImplV2 {
    pub(super) async fn try_mirrors(
        &self,
        url: &str,
        offset: u64,
        count: usize,
        deadline: Instant,
    ) -> Result<Option<RangeRead>> {
        if self.mirrors.is_empty() || !self.accelerate_address().is_empty() {
            return Ok(None);
        }
        let original = Url::parse(url)?;
        let Some(endpoints) = self.mirrors.get(authority(&original)) else {
            return Ok(None);
        };
        if count == 0 {
            return Ok(Some(RangeRead {
                body: Bytes::new(),
                total: 0,
            }));
        }
        for endpoint in endpoints {
            if endpoint
                .retry_after
                .lock()
                .unwrap()
                .is_some_and(|until| until > Instant::now())
            {
                continue;
            }
            let mut target = endpoint.origin.clone();
            target.set_path(original.path());
            target.set_query(original.query());
            match tokio::time::timeout(
                remaining_timeout(deadline)?,
                endpoint
                    .reader
                    .read_checked_range(target.as_str(), offset, count),
            )
            .await
            {
                Ok(Ok(read)) => return Ok(Some(read)),
                _ => {
                    *endpoint.retry_after.lock().unwrap() = Some(Instant::now() + MIRROR_COOLDOWN);
                    endpoint.reader.invalidate_url_info(target.as_str());
                    tracing::debug!(mirror = %endpoint.origin, "native registry mirror failed; trying fallback");
                }
            }
        }
        Ok(None)
    }

    async fn read_checked_range(&self, url: &str, offset: u64, count: usize) -> Result<RangeRead> {
        let mut response = self.fetch_range_response(url, offset, count).await?;
        let (length, total) = if response.status() == StatusCode::PARTIAL_CONTENT {
            let value = response
                .headers()
                .get(CONTENT_RANGE)
                .context("missing Content-Range")?
                .to_str()?;
            let (range, total) = value
                .strip_prefix("bytes ")
                .context("invalid range unit")?
                .split_once('/')
                .context("invalid Content-Range")?;
            let (start, end) = range.split_once('-').context("invalid range bounds")?;
            let (start, end, total) = (
                start.parse::<u64>()?,
                end.parse::<u64>()?,
                total.parse::<u64>()?,
            );
            let requested_end = offset
                .checked_add(count as u64 - 1)
                .context("range overflow")?;
            anyhow::ensure!(
                total > offset && start == offset && end == requested_end.min(total - 1),
                "registry returned a different byte range"
            );
            (end - start + 1, total)
        } else {
            let total = response
                .headers()
                .get(CONTENT_LENGTH)
                .context("missing Content-Length")?
                .to_str()?
                .parse::<u64>()?;
            anyhow::ensure!(
                offset == 0 && total <= count as u64,
                "registry ignored byte range"
            );
            (total, total)
        };
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            anyhow::ensure!(
                body.len() as u64 + chunk.len() as u64 <= length,
                "oversized registry range body"
            );
            body.extend_from_slice(&chunk);
        }
        anyhow::ensure!(body.len() as u64 == length, "truncated registry range body");
        Ok(RangeRead {
            body: Bytes::from(body),
            total,
        })
    }
}
