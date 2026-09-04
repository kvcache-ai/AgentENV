use std::future::Future;
use std::time::Instant;

use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

pub async fn http_metrics_middleware(request: Request, next: Next) -> Response {
    if request.uri().path() == "/metrics" {
        return next.run(request).await;
    }

    let method = http_method_label(request.method());
    let route = http_route_label(request.uri().path());
    let start = Instant::now();
    let response = next.run(request).await;
    let status = http_status_label(response.status());
    let route_source = response
        .extensions()
        .get::<HttpRouteSource>()
        .copied()
        .unwrap_or(HttpRouteSource::ControlPlane)
        .as_str();
    let elapsed = start.elapsed().as_secs_f64();

    metrics::histogram!(
        "agentenv_http_request_duration_seconds",
        "method" => method,
        "route" => route,
        "route_source" => route_source,
        "status" => status,
    )
    .record(elapsed);

    response
}

#[derive(Clone, Copy)]
pub(crate) enum HttpRouteSource {
    ControlPlane,
    ProxyHost,
    ProxyHeader,
    ProxyPrefix,
}

impl HttpRouteSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::ControlPlane => "control_plane",
            Self::ProxyHost => "proxy_host",
            Self::ProxyHeader => "proxy_header",
            Self::ProxyPrefix => "proxy_prefix",
        }
    }
}

pub struct SandboxStageTimer {
    operation: &'static str,
}

impl SandboxStageTimer {
    pub fn new(operation: &'static str) -> Self {
        Self { operation }
    }

    pub async fn time<F, T, E>(&self, stage: &'static str, future: F) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        let _inflight = SandboxStageInFlight::new(self.operation, stage);
        let start = Instant::now();
        let result = future.await;
        let status = result_status(result.is_ok());
        let elapsed = start.elapsed().as_secs_f64();
        metrics::histogram!(
            "agentenv_sandbox_stage_duration_seconds",
            "operation" => self.operation,
            "stage" => stage,
            "status" => status,
        )
        .record(elapsed);
        result
    }
}

struct SandboxStageInFlight {
    operation: &'static str,
    stage: &'static str,
}

impl SandboxStageInFlight {
    fn new(operation: &'static str, stage: &'static str) -> Self {
        metrics::gauge!(
            "agentenv_sandbox_stage_inflight",
            "operation" => operation,
            "stage" => stage,
        )
        .increment(1.0);
        Self { operation, stage }
    }
}

impl Drop for SandboxStageInFlight {
    fn drop(&mut self) {
        metrics::gauge!(
            "agentenv_sandbox_stage_inflight",
            "operation" => self.operation,
            "stage" => self.stage,
        )
        .decrement(1.0);
    }
}

pub struct MetricGuard {
    metric: &'static str,
    label: MetricGuardLabel,
    start: Instant,
    status: &'static str,
    recorded: bool,
}

#[derive(Clone, Copy)]
enum MetricGuardLabel {
    Operation(&'static str),
    OperationArtifact {
        operation: &'static str,
        artifact: &'static str,
    },
    Stage(&'static str),
}

impl MetricGuard {
    pub fn operation(metric: &'static str, operation: &'static str) -> Self {
        // A guard dropped before finish() is treated as cancellation. That
        // includes futures dropped by caller-side timeouts, which is useful
        // signal distinct from an operation returning an error.
        Self {
            metric,
            label: MetricGuardLabel::Operation(operation),
            start: Instant::now(),
            status: "canceled",
            recorded: false,
        }
    }

    /// Operation metric with an additional artifact dimension, used by OSS
    /// upload operations so per-artifact latency and cancellation stay
    /// visible (a dropped guard still records with status "canceled").
    pub fn operation_artifact(
        metric: &'static str,
        operation: &'static str,
        artifact: &'static str,
    ) -> Self {
        Self {
            metric,
            label: MetricGuardLabel::OperationArtifact {
                operation,
                artifact,
            },
            start: Instant::now(),
            status: "canceled",
            recorded: false,
        }
    }

    pub fn stage(metric: &'static str, stage: &'static str) -> Self {
        Self {
            metric,
            label: MetricGuardLabel::Stage(stage),
            start: Instant::now(),
            status: "canceled",
            recorded: false,
        }
    }

    pub fn finish<T, E>(&mut self, result: &Result<T, E>) {
        self.status = result_status(result.is_ok());
        self.record();
    }

    fn record(&mut self) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        let elapsed = self.start.elapsed().as_secs_f64();
        match self.label {
            MetricGuardLabel::Operation(operation) => {
                metrics::histogram!(
                    self.metric,
                    "operation" => operation,
                    "status" => self.status,
                )
                .record(elapsed);
            }
            MetricGuardLabel::OperationArtifact {
                operation,
                artifact,
            } => {
                metrics::histogram!(
                    self.metric,
                    "operation" => operation,
                    "artifact" => artifact,
                    "status" => self.status,
                )
                .record(elapsed);
            }
            MetricGuardLabel::Stage(stage) => {
                metrics::histogram!(
                    self.metric,
                    "stage" => stage,
                    "status" => self.status,
                )
                .record(elapsed);
            }
        }
    }
}

impl Drop for MetricGuard {
    fn drop(&mut self) {
        self.record();
    }
}

pub fn result_status(ok: bool) -> &'static str {
    if ok {
        "ok"
    } else {
        "error"
    }
}

pub fn http_method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::PATCH => "PATCH",
        Method::DELETE => "DELETE",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        _ => "OTHER",
    }
}

pub fn http_status_label(status: StatusCode) -> &'static str {
    match status.as_u16() {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

pub fn http_route_label(path: &str) -> &'static str {
    let path = path.trim_end_matches('/');
    let path = if path.is_empty() { "/" } else { path };
    match path {
        "/sandboxes" => "/sandboxes",
        "/sandboxes-cold" => "/sandboxes-cold",
        "/v2/sandboxes" => "/v2/sandboxes",
        "/snapshots" => "/snapshots",
        "/templates" => "/templates",
        "/v3/templates" => "/v3/templates",
        "/nodes" => "/nodes",
        "/health" => "/health",
        _ => dynamic_route_label(path),
    }
}

fn dynamic_route_label(path: &str) -> &'static str {
    let mut parts = path.trim_matches('/').split('/');
    let first = parts.next();
    let second = parts.next();
    let third = parts.next();
    let fourth = parts.next();
    let fifth = parts.next();

    match (first, second, third, fourth, fifth) {
        (Some("sandboxes"), Some(_), None, None, None) => "/sandboxes/{sandbox_id}",
        (Some("sandboxes"), Some(_), Some("snapshots"), None, None) => {
            "/sandboxes/{sandbox_id}/snapshots"
        }
        (Some("sandboxes"), Some(_), Some("network"), None, None) => {
            "/sandboxes/{sandbox_id}/network"
        }
        (Some("sandboxes"), Some(_), Some("pause"), None, None) => "/sandboxes/{sandbox_id}/pause",
        (Some("sandboxes"), Some(_), Some("resume"), None, None) => {
            "/sandboxes/{sandbox_id}/resume"
        }
        (Some("sandboxes"), Some(_), Some("fork"), None, None) => "/sandboxes/{sandbox_id}/fork",
        (Some("nodes"), Some(_), None, None, None) => "/nodes/{node_id}",
        (Some("proxy"), _, _, _, _) => "/proxy/*",
        _ => "unmatched",
    }
}

#[cfg(test)]
mod tests {
    use super::http_route_label;

    #[test]
    fn route_labels_hide_ids() {
        assert_eq!(
            http_route_label("/sandboxes/sb-1/snapshots"),
            "/sandboxes/{sandbox_id}/snapshots"
        );
        assert_eq!(http_route_label("/nodes/node-a"), "/nodes/{node_id}");
        assert_eq!(http_route_label("/templates"), "/templates");
        assert_eq!(http_route_label("/snapshots"), "/snapshots");
        assert_eq!(http_route_label("/v3/templates"), "/v3/templates");
        assert_eq!(http_route_label("/health"), "/health");
        assert_eq!(
            http_route_label("/sandboxes/sb-1/network"),
            "/sandboxes/{sandbox_id}/network"
        );
        assert_eq!(
            http_route_label("/sandboxes/sb-1/pause"),
            "/sandboxes/{sandbox_id}/pause"
        );
        assert_eq!(
            http_route_label("/sandboxes/sb-1/resume"),
            "/sandboxes/{sandbox_id}/resume"
        );
        assert_eq!(
            http_route_label("/sandboxes/sb-1/fork"),
            "/sandboxes/{sandbox_id}/fork"
        );
        assert_eq!(
            http_route_label("/templates/tpl/builds/build/status"),
            "unmatched"
        );
    }
}
