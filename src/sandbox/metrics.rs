//! Guest resource samples. These are not host process RSS or allocation counters.
use anyhow::{ensure, Context, Result};
use envd::http_client::models::Metrics;

#[derive(Clone, Debug)]
pub struct SandboxMetric {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub cpu_count: i32,
    pub cpu_used_pct: f32,
    pub mem_used: i64,
    pub mem_total: i64,
    pub mem_cache: i64,
    pub disk_used: i64,
    pub disk_total: i64,
}

impl TryFrom<Metrics> for SandboxMetric {
    type Error = anyhow::Error;

    fn try_from(raw: Metrics) -> Result<Self> {
        let ts = raw.ts.context("missing envd timestamp")?;
        ensure!(ts >= 0, "negative envd timestamp");
        let sample = Self {
            timestamp: chrono::DateTime::from_timestamp(ts, 0).context("invalid envd timestamp")?,
            cpu_count: raw.cpu_count.context("missing cpu_count")?,
            cpu_used_pct: raw.cpu_used_pct.context("missing cpu_used_pct")?,
            mem_used: raw.mem_used.context("missing mem_used")?,
            mem_total: raw.mem_total.context("missing mem_total")?,
            mem_cache: raw.mem_cache.context("missing mem_cache")?,
            disk_used: raw.disk_used.context("missing disk_used")?,
            disk_total: raw.disk_total.context("missing disk_total")?,
        };
        ensure!(
            sample.cpu_count > 0 && sample.cpu_used_pct.is_finite() && sample.cpu_used_pct >= 0.0,
            "invalid guest CPU metrics"
        );
        ensure!(
            [
                sample.mem_used,
                sample.mem_total,
                sample.mem_cache,
                sample.disk_used,
                sample.disk_total
            ]
            .iter()
            .all(|n| *n >= 0),
            "negative guest byte count"
        );
        Ok(sample)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_metrics_decode_large_byte_counts_and_reject_missing_values() {
        let raw: Metrics = serde_json::from_value(serde_json::json!({
            "ts": 1700000000, "cpu_count": 2, "cpu_used_pct": 50.0,
            "mem_used": 4294967296i64, "mem_total": 8589934592i64,
            "mem_cache": 3221225472i64, "disk_used": 17179869184i64,
            "disk_total": 34359738368i64
        }))
        .unwrap();
        let sample = SandboxMetric::try_from(raw.clone()).unwrap();
        assert_eq!(sample.mem_total, 8589934592);
        assert_eq!(sample.disk_total, 34359738368);
        let response: agentenv_http_server::models::SandboxMetric = sample.into();
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 9);
        assert_eq!(json["timestampUnix"], 1700000000i64);
        assert_eq!(json["timestamp"], "2023-11-14T22:13:20Z");
        assert_eq!(json["memTotal"], 8589934592i64);
        assert_eq!(json["diskTotal"], 34359738368i64);
        let mut missing = raw;
        missing.mem_cache = None;
        assert!(SandboxMetric::try_from(missing).is_err());
    }
}
