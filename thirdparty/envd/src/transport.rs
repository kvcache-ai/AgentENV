use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use http::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::sync::Mutex;
use tower::Service;

// tonic::body::Body is the type used by generated clients.
pub(crate) type TonicBoxBody = tonic::body::Body;

pub(crate) type Channel = tower::util::BoxCloneService<
    http::Request<TonicBoxBody>,
    http::Response<hyper::body::Incoming>,
    Box<dyn std::error::Error + Send + Sync>, // Use boxed error to support anyhow and others
>;

#[derive(Clone)]
struct DualClient {
    h1: Client<HttpConnector, TonicBoxBody>,
    h2: Client<HttpConnector, TonicBoxBody>,
    protocol: Arc<Mutex<Option<Protocol>>>,
    uri: Uri,
    access_token: Option<http::HeaderValue>,
}

/// Bounds the HTTP/2 probe, after which the call falls back to HTTP/1.1.
///
/// A server without h2c fails the probe's connection while the probe is
/// still being queued on it. hyper can then lose the queued probe without
/// failing it, and the pooled connection keeps it from being dropped, so
/// the probe never completes.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
enum Protocol {
    H1,
    H2,
}

impl Service<http::Request<TonicBoxBody>> for DualClient {
    type Response = http::Response<hyper::body::Incoming>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: http::Request<TonicBoxBody>) -> Self::Future {
        let h1 = self.h1.clone();
        let h2 = self.h2.clone();
        let protocol = self.protocol.clone();
        let uri = self.uri.clone();
        let access_token = self.access_token.clone();

        Box::pin(async move {
            // Determine protocol if unknown
            let mut proto = { *protocol.lock().await };

            if proto.is_none() {
                // Probe with H2 (OPTIONS *) to check if the server supports HTTP/2 Prior Knowledge.
                // We construct a harmless probing request.
                let mut parts = uri.clone().into_parts();
                parts.path_and_query = Some(http::uri::PathAndQuery::from_static("/"));

                let probe_req = http::Request::builder()
                    .method(http::Method::OPTIONS)
                    .uri(Uri::from_parts(parts).map_err(|e| anyhow!("Invalid URI parts: {}", e))?)
                    .body(TonicBoxBody::default())
                    .map_err(|e| anyhow!("Failed to build probe request: {}", e))?;

                // Try H2
                match tokio::time::timeout(PROBE_TIMEOUT, h2.request(probe_req)).await {
                    Ok(Ok(_)) => {
                        proto = Some(Protocol::H2);
                    }
                    Ok(Err(_)) | Err(_) => {
                        proto = Some(Protocol::H1);
                    }
                }
                *protocol.lock().await = proto;
            }

            if let Some(access_token) = access_token {
                req.headers_mut().insert("x-access-token", access_token);
            }

            // Prepare the actual request
            match proto.unwrap() {
                Protocol::H2 => {
                    let mut parts = uri.into_parts();
                    parts.path_and_query = req.uri().path_and_query().cloned();
                    *req.uri_mut() =
                        Uri::from_parts(parts).map_err(|e| anyhow!("Invalid URI parts: {}", e))?;

                    h2.request(req).await.map_err(|e| e.into())
                }
                Protocol::H1 => {
                    let mut parts = uri.into_parts();
                    parts.path_and_query = req.uri().path_and_query().cloned();
                    *req.uri_mut() =
                        Uri::from_parts(parts).map_err(|e| anyhow!("Invalid URI parts: {}", e))?;

                    // Coerce version to HTTP/1.1
                    *req.version_mut() = http::Version::HTTP_11;

                    h1.request(req).await.map_err(|e| e.into())
                }
            }
        })
    }
}

/// envd exchanges small RPC frames, so Nagle only ever trades latency for a
/// coalescing win that never materializes here.
fn nodelay_connector() -> HttpConnector {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    connector
}

/// Creates a channel compatible with both HTTP/1.1 and HTTP/2.
///
/// The channel automatically probes the server to determine supported protocol (H2 or H1).
pub fn new_channel(addr: &str, access_token: Option<&str>) -> anyhow::Result<Channel> {
    let uri: Uri = addr.parse().context("Invalid URI")?;
    let access_token = access_token
        .map(|token| {
            let mut token = http::HeaderValue::from_str(token)?;
            token.set_sensitive(true);
            Ok::<_, http::header::InvalidHeaderValue>(token)
        })
        .transpose()
        .context("Invalid envd access token")?;

    let h1 = Client::builder(TokioExecutor::new())
        .http2_only(false)
        .build(nodelay_connector());

    // H2 with Prior Knowledge for cleartext
    let h2 = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build(nodelay_connector());

    let service = DualClient {
        h1,
        h2,
        protocol: Arc::new(Mutex::new(None)),
        uri,
        access_token,
    };

    Ok(tower::util::BoxCloneService::new(service))
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use super::*;

    /// A probe that never completes must fall back to HTTP/1.1 instead of
    /// blocking the call.
    #[tokio::test]
    async fn stalled_h2_probe_falls_back_to_http1() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        let n = stream.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buf[..n]);
                    }
                    if request.starts_with(b"PRI ") {
                        // Hold the HTTP/2 probe without answering it.
                        std::future::pending::<()>().await;
                    }
                    stream
                        .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
                        .await
                        .unwrap();
                });
            }
        });

        let channel = new_channel(&format!("http://{addr}"), None).unwrap();
        let request = http::Request::builder()
            .uri("/ping")
            .body(TonicBoxBody::default())
            .unwrap();
        let response = tokio::time::timeout(PROBE_TIMEOUT * 4, channel.oneshot(request))
            .await
            .expect("call blocked on the HTTP/2 probe")
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::NO_CONTENT);
        assert_eq!(response.version(), http::Version::HTTP_11);
    }
}
