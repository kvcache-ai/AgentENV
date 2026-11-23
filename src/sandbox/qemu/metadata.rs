//! Per-sandbox MMDS-compatible endpoint, served inside the sandbox netns.
use anyhow::{Context, Result};
use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Router,
};
use serde_json::Value;
use std::{
    net::TcpListener,
    os::fd::AsRawFd,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

#[derive(Clone)]
struct Metadata {
    value: Arc<Value>,
    tokens: Arc<std::sync::Mutex<std::collections::HashMap<String, Instant>>>,
}

pub(super) struct MetadataServer {
    task: JoinHandle<()>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Drop for MetadataServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl MetadataServer {
    pub async fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if tokio::time::timeout(Duration::from_secs(1), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
            let _ = (&mut self.task).await;
        }
    }
    pub fn start(netns: &Path, value: Value) -> Result<Self> {
        let ns = std::fs::File::open(netns)?;
        // Namespace changes are confined to this short-lived thread. Only the
        // socket is returned to Tokio; the server's worker threads never setns.
        let listener = crate::privileges::run_with_scoped_capabilities(
            &[
                crate::privileges::CAP_SYS_ADMIN,
                crate::privileges::CAP_NET_ADMIN,
            ],
            move || {
                if unsafe { libc::setns(ns.as_raw_fd(), libc::CLONE_NEWNET) } != 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                let status = std::process::Command::new("ip")
                    .args(["address", "replace", "169.254.169.254/32", "dev", "lo"])
                    .status()?;
                anyhow::ensure!(status.success(), "configure QEMU metadata address");
                // This sysctl is local to the sandbox network namespace. Avoid
                // adding CAP_NET_BIND_SERVICE to AgentENV's capability contract.
                std::fs::write("/proc/sys/net/ipv4/ip_unprivileged_port_start", "0")?;
                let listener = TcpListener::bind("169.254.169.254:80")?;
                listener.set_nonblocking(true)?;
                Ok(listener)
            },
        )?;
        let listener = tokio::net::TcpListener::from_std(listener).context("metadata listener")?;
        let state = Metadata {
            value: Arc::new(value),
            tokens: Default::default(),
        };
        let router = Router::new().fallback(handler).with_state(state);
        let (shutdown, signal) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = signal.await;
                })
                .await
            {
                tracing::warn!(%error, "QEMU metadata service exited");
            }
        });
        Ok(Self {
            task,
            shutdown: Some(shutdown),
        })
    }
}

async fn handler(State(state): State<Metadata>, request: Request) -> Response {
    let path = request.uri().path();
    if request.method() == axum::http::Method::PUT && path == "/latest/api/token" {
        let ttl = request
            .headers()
            .get("X-metadata-token-ttl-seconds")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        let Some(ttl @ 1..=21600) = ttl else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let token = uuid::Uuid::now_v7().to_string();
        let mut tokens = state.tokens.lock().unwrap();
        tokens.retain(|_, expires| *expires > Instant::now());
        if tokens.len() >= 1024 {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
        tokens.insert(token.clone(), Instant::now() + Duration::from_secs(ttl));
        return token.into_response();
    }
    let authorized = request
        .headers()
        .get("X-metadata-token")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|token| {
            state
                .tokens
                .lock()
                .unwrap()
                .get(token)
                .is_some_and(|end| *end > Instant::now())
        });
    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if request.method() != axum::http::Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let mut value = state.value.as_ref();
    for key in path.trim_matches('/').split('/').filter(|s| !s.is_empty()) {
        let Some(next) = value.get(key) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        value = next;
    }
    if request
        .headers()
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("application/json"))
    {
        return axum::Json(value.clone()).into_response();
    }
    match value {
        Value::String(text) => text.clone().into_response(),
        Value::Object(map) => map
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
            .into_response(),
        _ => axum::Json(value.clone()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };

    fn state() -> Metadata {
        Metadata {
            value: Arc::new(serde_json::json!({"instanceID":"test-id", "accessTokenHash":"hash"})),
            tokens: Default::default(),
        }
    }
    #[tokio::test]
    async fn metadata_requires_a_valid_token() {
        let state = state();
        assert_eq!(
            handler(State(state.clone()), Request::new(Body::empty()))
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let response = handler(
            State(state.clone()),
            Request::builder()
                .method("PUT")
                .uri("/latest/api/token")
                .header("X-metadata-token-ttl-seconds", "60")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let token = String::from_utf8(to_bytes(response.into_body(), 1024).await.unwrap().to_vec())
            .unwrap();
        let response = handler(
            State(state.clone()),
            Request::builder()
                .uri("/")
                .header("X-metadata-token", &token)
                .header("Accept", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        let value: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
        assert_eq!(value["instanceID"], "test-id");
        state
            .tokens
            .lock()
            .unwrap()
            .insert(token.clone(), Instant::now() - Duration::from_secs(1));
        assert_eq!(
            handler(
                State(state),
                Request::builder()
                    .header("X-metadata-token", token)
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    #[tokio::test]
    async fn metadata_rejects_invalid_ttl() {
        for ttl in ["0", "21601", "invalid"] {
            assert_eq!(
                handler(
                    State(state()),
                    Request::builder()
                        .method("PUT")
                        .uri("/latest/api/token")
                        .header("X-metadata-token-ttl-seconds", ttl)
                        .body(Body::empty())
                        .unwrap()
                )
                .await
                .status(),
                StatusCode::BAD_REQUEST
            );
        }
    }
}
