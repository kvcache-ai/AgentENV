use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use axum::http::{header::CONTENT_TYPE, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{routing::get, Router};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

const TEXT_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

// Keep in sync with services/shared/observability/buckets.go.
const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
    120.0, 300.0, 600.0, 900.0, 1200.0, 1800.0,
];

/// Fine-grained buckets for the two sampled ZFile read-path duration
/// histograms: 1µs..500µs in 1/2/5 steps, then the standard
/// `DURATION_BUCKETS` ladder from 1ms to 30s. Values are seconds, strictly
/// increasing; the exporter appends the implicit `+Inf` bucket itself.
/// Attached only to the two exact metric names in
/// [`init_prometheus_recorder`] via [`Matcher::Full`], so every other (and
/// any future) histogram keeps `DURATION_BUCKETS`.
const ZFILE_FINE_DURATION_BUCKETS: &[f64] = &[
    0.000_001, 0.000_002, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250,
    0.000_500, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0,
    30.0,
];

/// Byte-size buckets for `agentenv_snapshot_sealed_upper_size_bytes`, the
/// only non-duration histogram today: 128 KiB..64 GiB in 2x steps. The
/// global `DURATION_BUCKETS` default is seconds-based and would land every
/// size observation in `+Inf`.
const SIZE_BYTES_BUCKETS: &[f64] = &[
    131_072.0,        // 128 KiB
    262_144.0,        // 256 KiB
    524_288.0,        // 512 KiB
    1_048_576.0,      // 1 MiB
    2_097_152.0,      // 2 MiB
    4_194_304.0,      // 4 MiB
    8_388_608.0,      // 8 MiB
    16_777_216.0,     // 16 MiB
    33_554_432.0,     // 32 MiB
    67_108_864.0,     // 64 MiB
    134_217_728.0,    // 128 MiB
    268_435_456.0,    // 256 MiB
    536_870_912.0,    // 512 MiB
    1_073_741_824.0,  // 1 GiB
    2_147_483_648.0,  // 2 GiB
    4_294_967_296.0,  // 4 GiB
    8_589_934_592.0,  // 8 GiB
    17_179_869_184.0, // 16 GiB
    34_359_738_368.0, // 32 GiB
    68_719_476_736.0, // 64 GiB
];

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn init_prometheus_recorder() -> anyhow::Result<()> {
    // Startup code can call init more than once in tests; only a true
    // concurrent first initialization is treated as an error.
    if PROMETHEUS_HANDLE.get().is_some() {
        return Ok(());
    }

    let handle = PrometheusBuilder::new()
        .set_buckets(DURATION_BUCKETS)?
        // Per-metric overrides take precedence over the global buckets
        // above (full match beats the default); both ZFile histograms share
        // the same microsecond-resolution bucket set.
        .set_buckets_for_metric(
            Matcher::Full("agentenv_overlaybd_zfile_pread_duration_seconds".to_owned()),
            ZFILE_FINE_DURATION_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("agentenv_overlaybd_zfile_decompress_duration_seconds".to_owned()),
            ZFILE_FINE_DURATION_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("agentenv_snapshot_sealed_upper_size_bytes".to_owned()),
            SIZE_BYTES_BUCKETS,
        )?
        .install_recorder()?;
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let upkeep = handle.clone();
        runtime.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                upkeep.run_upkeep();
            }
        });
    }
    PROMETHEUS_HANDLE
        .set(handle)
        .map_err(|_| anyhow::anyhow!("prometheus recorder was initialized concurrently"))?;
    Ok(())
}

pub async fn metrics_handler() -> Response {
    match PROMETHEUS_HANDLE.get() {
        Some(handle) => ([(CONTENT_TYPE, TEXT_CONTENT_TYPE)], handle.render()).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "prometheus recorder is not initialized",
        )
            .into_response(),
    }
}

pub async fn serve_metrics(
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(
        listener,
        Router::new().route("/metrics", get(metrics_handler)),
    )
    .with_graceful_shutdown(shutdown)
    .await
}
