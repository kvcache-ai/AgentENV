use std::fmt;
use std::path::PathBuf;

use anyhow::{Context, Result};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::Uri;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::connector::UnixConnector;

#[derive(Debug)]
pub(super) struct HttpStatusError {
    status: hyper::StatusCode,
    body: String,
}

impl fmt::Display for HttpStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Request failed: {} - {}", self.status, self.body)
    }
}

impl std::error::Error for HttpStatusError {}

pub(super) fn is_http_status_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<HttpStatusError>().is_some()
}

#[derive(Clone)]
pub(super) struct UnixSocketClient {
    client: HyperClient<UnixConnector, Full<Bytes>>,
}

impl UnixSocketClient {
    pub fn new(socket_path: PathBuf) -> Self {
        let connector = UnixConnector { path: socket_path };
        let client = HyperClient::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }

    pub async fn request<B, R>(
        &self,
        method: hyper::Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let uri: Uri = format!("http://localhost{}", path)
            .parse()
            .context("Failed to parse URI")?;

        let body_bytes = if let Some(b) = body {
            serde_json::to_vec(b).context("Failed to serialize body")?
        } else {
            vec![]
        };

        let req = hyper::Request::builder()
            .method(method)
            .uri(uri)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::ACCEPT, "application/json")
            .body(Full::new(Bytes::from(body_bytes)))
            .context("Failed to build request")?;

        let res = self.client.request(req).await.context("Request failed")?;
        let status = res.status();

        if !status.is_success() {
            let bytes = res
                .collect()
                .await
                .context("Failed to read error body")?
                .to_bytes();
            let error_msg = String::from_utf8_lossy(&bytes);
            return Err(HttpStatusError {
                status,
                body: error_msg.into_owned(),
            }
            .into());
        }

        let bytes = res
            .collect()
            .await
            .context("Failed to read response body")?
            .to_bytes();

        // Handle empty response
        if bytes.is_empty() {
            return serde_json::from_str("null")
                .context("Failed to deserialize null for empty response");
        }

        let response_body: R =
            serde_json::from_slice(&bytes).context("Failed to deserialize response body")?;
        Ok(response_body)
    }

    // Helper for requests without response body
    pub async fn request_no_content<B>(
        &self,
        method: hyper::Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<()>
    where
        B: Serialize + ?Sized,
    {
        self.request::<B, serde::de::IgnoredAny>(method, path, body)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct JsonReply {
        answer: u32,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct JsonRequest {
        hello: String,
    }

    #[tokio::test]
    async fn request_serializes_body_and_deserializes_response() -> Result<()> {
        let temp = tempdir()?;
        let socket_path = temp.path().join("firecracker.sock");
        let listener = UnixListener::bind(&socket_path)?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|req: Request<Incoming>| async move {
                        assert_eq!(req.method(), Method::PUT);
                        assert_eq!(req.uri().path(), "/machine-config");
                        let bytes = req
                            .collect()
                            .await
                            .expect("collect request body")
                            .to_bytes();
                        let parsed: JsonRequest =
                            serde_json::from_slice(&bytes).expect("decode request body");
                        assert_eq!(
                            parsed,
                            JsonRequest {
                                hello: "world".to_string(),
                            }
                        );
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(hyper::header::CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from_static(br#"{"answer":42}"#)))
                                .expect("build response"),
                        )
                    }),
                )
                .await
                .expect("serve connection");
        });

        let client = UnixSocketClient::new(socket_path);
        let reply: JsonReply = client
            .request(
                Method::PUT,
                "/machine-config",
                Some(&JsonRequest {
                    hello: "world".to_string(),
                }),
            )
            .await?;

        assert_eq!(reply, JsonReply { answer: 42 });
        server.await.expect("server task");
        Ok(())
    }

    #[tokio::test]
    async fn request_no_content_accepts_empty_success_body() -> Result<()> {
        let temp = tempdir()?;
        let socket_path = temp.path().join("firecracker.sock");
        let listener = UnixListener::bind(&socket_path)?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::NO_CONTENT)
                                .body(Full::new(Bytes::new()))
                                .expect("build response"),
                        )
                    }),
                )
                .await
                .expect("serve connection");
        });

        let client = UnixSocketClient::new(socket_path);
        client
            .request_no_content::<serde_json::Value>(Method::PATCH, "/vm", None)
            .await?;

        server.await.expect("server task");
        Ok(())
    }

    #[tokio::test]
    async fn request_surfaces_http_error_status_and_body() {
        let temp = tempdir().expect("tempdir");
        let socket_path = temp.path().join("firecracker.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind unix socket");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header(hyper::header::CONTENT_TYPE, "text/plain")
                                .body(Full::new(Bytes::from_static(b"bad request body")))
                                .expect("build response"),
                        )
                    }),
                )
                .await
                .expect("serve connection");
        });

        let client = UnixSocketClient::new(socket_path);
        let err = client
            .request::<serde_json::Value, JsonReply>(Method::GET, "/bad", None)
            .await
            .expect_err("non-success response should fail");

        assert!(err.to_string().contains("400 Bad Request"));
        assert!(err.to_string().contains("bad request body"));
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn request_reports_invalid_json_responses() {
        let temp = tempdir().expect("tempdir");
        let socket_path = temp.path().join("firecracker.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind unix socket");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(hyper::header::CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from_static(b"not-json")))
                                .expect("build response"),
                        )
                    }),
                )
                .await
                .expect("serve connection");
        });

        let client = UnixSocketClient::new(socket_path);
        let err = client
            .request::<serde_json::Value, JsonReply>(Method::GET, "/vm", None)
            .await
            .expect_err("invalid json should fail");

        assert!(err.to_string().contains("deserialize response body"));
        server.await.expect("server task");
    }
}
