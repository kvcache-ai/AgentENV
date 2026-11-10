//! Per-scrape guest gauges never enter the process-wide recorder, so stopped
//! sandboxes do not leave retained metric handles or obsolete resource series.
use std::time::Duration;

use axum::{
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use metrics_exporter_prometheus::PrometheusBuilder;

use super::ApiImpl;
use crate::{
    cfg::{ConfigManager, OrchestratorConfig},
    sandbox::SandboxMetric,
    types::SandboxId,
};

pub(super) fn router<I>(api_impl: I) -> Router
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    router_with_config(
        api_impl,
        ConfigManager::global_config().orchestrator.clone(),
    )
}

pub(super) fn router_with_config<I>(api_impl: I, config: OrchestratorConfig) -> Router
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    Router::new().route(
        "/metrics",
        get(move || {
            let api = api_impl.clone();
            let config = config.clone();
            async move {
                if !config.metrics_prometheus_enabled || config.metrics_interval_secs == 0 {
                    return agentenv_observability::metrics_handler().await;
                }
                let Some(mut body) = agentenv_observability::render_metrics() else {
                    return agentenv_observability::metrics_handler().await;
                };
                match api
                    .as_ref()
                    .orchestrator()
                    .sandbox_metrics_snapshot(Duration::from_secs(
                        config.metrics_prometheus_max_age_secs,
                    ))
                    .await
                {
                    Ok(samples) => {
                        body.push_str(&encode_samples(&samples));
                        (
                            [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
                            body,
                        )
                            .into_response()
                    }
                    Err(error) => {
                        tracing::warn!(%error, "cannot snapshot sandbox metrics for Prometheus");
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            "sandbox metrics unavailable",
                        )
                            .into_response()
                    }
                }
            }
        }),
    )
}

fn encode_samples(samples: &[(SandboxId, Option<SandboxMetric>)]) -> String {
    // A local recorder reuses the standard encoder/label escaping while keeping
    // high-churn sandbox IDs out of the long-lived global registry.
    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, || {
        metrics::describe_gauge!(
            "agentenv_sandbox_guest_sample_available",
            "Whether a running sandbox has a fresh sample from its current runtime (0 or 1)."
        );
        metrics::describe_gauge!(
            "agentenv_sandbox_guest_sample_timestamp_seconds",
            "Guest-reported Unix timestamp of the exported sample."
        );
        metrics::describe_gauge!(
            "agentenv_sandbox_guest_cpu_usage_percent",
            "Guest CPU percentage, passed through from envd cpu_used_pct."
        );
        metrics::describe_gauge!("agentenv_sandbox_guest_cpu_count", "Guest CPU count.");
        metrics::describe_gauge!(
            "agentenv_sandbox_guest_memory_used_bytes",
            "Guest memory used in bytes, as reported by envd."
        );
        metrics::describe_gauge!(
            "agentenv_sandbox_guest_memory_total_bytes",
            "Guest total memory in bytes, as reported by envd."
        );
        metrics::describe_gauge!(
            "agentenv_sandbox_guest_memory_cache_bytes",
            "Guest cached memory in bytes, as reported by envd."
        );
        for (id, sample) in samples {
            let id = id.to_string();
            metrics::gauge!("agentenv_sandbox_guest_sample_available", "sandbox_id" => id.clone())
                .set(if sample.is_some() { 1.0 } else { 0.0 });
            let Some(sample) = sample else { continue };
            metrics::gauge!("agentenv_sandbox_guest_sample_timestamp_seconds", "sandbox_id" => id.clone())
                .set(sample.timestamp.timestamp() as f64);
            metrics::gauge!("agentenv_sandbox_guest_cpu_usage_percent", "sandbox_id" => id.clone())
                .set(f64::from(sample.cpu_used_pct));
            metrics::gauge!("agentenv_sandbox_guest_cpu_count", "sandbox_id" => id.clone())
                .set(f64::from(sample.cpu_count));
            metrics::gauge!("agentenv_sandbox_guest_memory_used_bytes", "sandbox_id" => id.clone())
                .set(sample.mem_used as f64);
            metrics::gauge!("agentenv_sandbox_guest_memory_total_bytes", "sandbox_id" => id.clone())
                .set(sample.mem_total as f64);
            metrics::gauge!("agentenv_sandbox_guest_memory_cache_bytes", "sandbox_id" => id)
                .set(sample.mem_cache as f64);
        }
    });
    recorder.handle().render()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guest_gauges_preserve_values_and_retire_series_between_scrapes() {
        let id = SandboxId::new();
        let sample = SandboxMetric {
            timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            cpu_count: 4,
            cpu_used_pct: 25.5,
            mem_used: 4_294_967_296,
            mem_total: 8_589_934_592,
            mem_cache: 3_221_225_472,
            disk_used: 0,
            disk_total: 0,
        };
        let text = encode_samples(&[(id, Some(sample))]);
        for (name, value) in [
            ("sample_available", "1"),
            ("sample_timestamp_seconds", "1700000000"),
            ("cpu_count", "4"),
            ("cpu_usage_percent", "25.5"),
            ("memory_used_bytes", "4294967296"),
            ("memory_total_bytes", "8589934592"),
            ("memory_cache_bytes", "3221225472"),
        ] {
            assert!(
                text.contains(&format!(
                    "agentenv_sandbox_guest_{name}{{sandbox_id=\"{id}\"}} {value}"
                )),
                "missing {name}: {text}"
            );
            assert!(text.contains(&format!("# TYPE agentenv_sandbox_guest_{name} gauge")));
            assert!(text.contains(&format!("# HELP agentenv_sandbox_guest_{name} ")));
        }

        let missing = encode_samples(&[(id, None)]);
        assert!(missing.contains(&format!(
            "agentenv_sandbox_guest_sample_available{{sandbox_id=\"{id}\"}} 0"
        )));
        assert!(!missing.contains("agentenv_sandbox_guest_memory_used_bytes{"));
        assert!(!missing.contains("agentenv_sandbox_guest_cpu_usage_percent{"));
        assert!(!missing.contains("agentenv_sandbox_guest_sample_timestamp_seconds{"));
        assert!(!encode_samples(&[]).contains(&id.to_string()));
    }

    #[test]
    fn guest_encoding_does_not_register_series_in_the_callers_recorder() {
        let caller = PrometheusBuilder::new().build_recorder();
        metrics::with_local_recorder(&caller, || {
            let id = SandboxId::new();
            assert!(encode_samples(&[(id, None)]).contains(&id.to_string()));
            assert!(!caller.handle().render().contains("agentenv_sandbox_guest_"));
        });
    }
}
