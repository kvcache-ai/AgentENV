use std::{error::Error, sync::OnceLock, time::Duration};

use ::metrics::{Counter, Histogram};
use anyhow::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RemoteSource {
    RegistryDirect { host: String },
    RegistryP2p,
    OssObject,
}

impl RemoteSource {
    fn as_label(&self) -> &'static str {
        match self {
            Self::RegistryDirect { .. } => "registry_direct",
            Self::RegistryP2p => "registry_p2p",
            Self::OssObject => "oss_object",
        }
    }

    /// Origin registry host, only meaningful for direct registry reads. P2P
    /// reads are served via the local facade and OSS reads target an object
    /// store, so both report `"none"`.
    fn registry_label(&self) -> &str {
        match self {
            Self::RegistryDirect { host } => host,
            Self::RegistryP2p | Self::OssObject => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteReadOperation {
    ReadRange,
    ReadRangeInto,
}

impl RemoteReadOperation {
    fn as_label(self) -> &'static str {
        match self {
            Self::ReadRange => "read_range",
            Self::ReadRangeInto => "read_range_into",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteReadStatus {
    Ok,
    RateLimited,
    Timeout,
    Error,
}

impl RemoteReadStatus {
    fn from_result<T>(result: &Result<T>) -> Self {
        match result {
            Ok(_) => Self::Ok,
            Err(err) => err
                .chain()
                .find_map(classify_remote_read_error)
                .unwrap_or(Self::Error),
        }
    }

    fn from_http_status(status: u16) -> Self {
        match status {
            429 => Self::RateLimited,
            408 | 504 => Self::Timeout,
            _ => Self::Error,
        }
    }

    fn as_label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::RateLimited => "rate_limited",
            Self::Timeout => "timeout",
            Self::Error => "error",
        }
    }
}

pub(crate) fn registry_source(accelerate_address: &str, url: &str) -> RemoteSource {
    if accelerate_address.trim().is_empty() {
        RemoteSource::RegistryDirect {
            host: normalize_host(url),
        }
    } else {
        RemoteSource::RegistryP2p
    }
}

/// Extract a low-cardinality host/domain label from a URL. Strips any scheme,
/// path, query, fragment and userinfo, keeping `host[:port]`. Returns
/// `"unknown"` when nothing usable remains.
fn normalize_host(raw: &str) -> String {
    let without_scheme = raw.split_once("://").map(|(_, rest)| rest).unwrap_or(raw);
    let authority = without_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(without_scheme);
    let host = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority)
        .trim();
    if host.is_empty() {
        "unknown".to_string()
    } else {
        host.to_string()
    }
}

pub(crate) fn record_remote_read<T>(
    source: &RemoteSource,
    operation: RemoteReadOperation,
    result: &Result<T>,
    bytes: impl FnOnce(&T) -> u64,
    elapsed: Duration,
) {
    let status = RemoteReadStatus::from_result(result);
    ::metrics::histogram!(
        "agentenv_overlaybd_remote_read_duration_seconds",
        "source" => source.as_label(),
        "registry" => source.registry_label().to_string(),
        "operation" => operation.as_label(),
        "status" => status.as_label(),
    )
    .record(elapsed.as_secs_f64());
    if let Ok(value) = result {
        ::metrics::counter!(
            "agentenv_overlaybd_remote_read_bytes_total",
            "source" => source.as_label(),
            "registry" => source.registry_label().to_string(),
            "operation" => operation.as_label(),
        )
        .increment(bytes(value));
    }
}

pub(crate) fn record_remote_metadata<T>(
    source: &RemoteSource,
    result: &Result<T>,
    elapsed: Duration,
) {
    let status = RemoteReadStatus::from_result(result);
    ::metrics::histogram!(
        "agentenv_overlaybd_remote_metadata_duration_seconds",
        "source" => source.as_label(),
        "registry" => source.registry_label().to_string(),
        "status" => status.as_label(),
    )
    .record(elapsed.as_secs_f64());
}

fn classify_remote_read_error(cause: &(dyn Error + 'static)) -> Option<RemoteReadStatus> {
    if cause.is::<tokio::time::error::Elapsed>() {
        return Some(RemoteReadStatus::Timeout);
    }

    classify_remote_read_error_from_dependencies(cause)
}

#[cfg(feature = "full")]
fn classify_remote_read_error_from_dependencies(
    cause: &(dyn Error + 'static),
) -> Option<RemoteReadStatus> {
    if let Some(reqwest_error) = cause.downcast_ref::<reqwest::Error>() {
        if reqwest_error.is_timeout() {
            return Some(RemoteReadStatus::Timeout);
        }
        return reqwest_error
            .status()
            .map(|status| RemoteReadStatus::from_http_status(status.as_u16()));
    }

    if let Some(opendal_error) = cause.downcast_ref::<object_store_operator::OpenDalError>() {
        return classify_opendal_error(opendal_error);
    }

    if let Some(object_store_error) =
        cause.downcast_ref::<object_store_operator::ObjectStoreOperatorError>()
    {
        match object_store_error {
            object_store_operator::ObjectStoreOperatorError::OpenDal(opendal_error) => {
                return classify_opendal_error(opendal_error);
            }
            object_store_operator::ObjectStoreOperatorError::CredentialRefresh(_)
            | object_store_operator::ObjectStoreOperatorError::OperatorBuild(_) => {}
        }
    }

    None
}

#[cfg(feature = "full")]
fn classify_opendal_error(error: &object_store_operator::OpenDalError) -> Option<RemoteReadStatus> {
    match error.kind() {
        object_store_operator::OpenDalErrorKind::RateLimited => Some(RemoteReadStatus::RateLimited),
        _ => None,
    }
}

#[cfg(not(feature = "full"))]
fn classify_remote_read_error_from_dependencies(
    _cause: &(dyn Error + 'static),
) -> Option<RemoteReadStatus> {
    None
}

// ---------------------------------------------------------------------------
// ZFile read-path metrics. Exact counters are aggregated by the caller
// (`compression::zfile`) across one logical `pread` and recorded here once
// per pread (never per block or per retry). Duration histograms cover only
// the preads picked by the 1-in-1024 per-thread sampler, so their `_count`
// is a SAMPLE COUNT, not an operation count — compute rates from the exact
// counters instead. Metric handles are registered lazily on first use and
// cached in `ZFileReadMetrics`, so the hot path never rebuilds a Key or
// touches the recorder registry.
// ---------------------------------------------------------------------------

/// ZFile compression codec. This is the complete `codec` label value set
/// for every ZFile metric: keep cardinality at exactly these two values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ZFileCodec {
    Lz4,
    Zstd,
}

impl ZFileCodec {
    fn as_label(self) -> &'static str {
        match self {
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

/// Outcome of one logical ZFile `pread`; the complete `status` label value
/// set of `agentenv_overlaybd_zfile_pread_duration_seconds`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ZFileReadStatus {
    Ok,
    Error,
}

impl ZFileReadStatus {
    fn as_label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

/// Exact counters accumulated in plain stack variables across one logical
/// ZFile `pread` by the read loop, then handed to [`ZFileReadMetrics::record`]
/// once. A field left at zero records nothing, so error exits and EOF
/// reads never create empty series.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ZFileReadStats {
    /// Bytes returned to the caller; set only on success.
    pub output_bytes: u64,
    /// Encoded bytes submitted to exact reads, including retry re-reads.
    pub encoded_bytes: u64,
    /// Logical zfile blocks touched by the initial request; retries do not
    /// re-increment this.
    pub blocks: u64,
    /// Initial merged-range reads plus single-block retry exact reads. This
    /// is a zfile-level read count, NOT a syscall/HTTP attempt count: one
    /// merged read may be filled by several backing calls.
    pub read_batches: u64,
    /// Block re-reads after a CRC32C mismatch.
    pub checksum_retries: u64,
    /// Block re-reads after a decompress failure.
    pub decompress_retries: u64,
}

/// Lazily register a per-codec counter slot and increment it: registration
/// (Key construction plus recorder lookup) happens exactly once per reader,
/// after which a logical `pread` only pays an atomic increment.
macro_rules! zcounter {
    ($slot:expr, $name:literal, $codec:expr, $value:expr) => {
        $slot
            .get_or_init(|| ::metrics::counter!($name, "codec" => $codec))
            .increment($value)
    };
    ($slot:expr, $name:literal, $codec:expr, $value:expr, $lk:literal => $lv:literal) => {
        $slot
            .get_or_init(|| ::metrics::counter!($name, "codec" => $codec, $lk => $lv))
            .increment($value)
    };
}

/// Metric handles for one codec's ZFile read path. Registration is lazy
/// (unused series stay absent), so constructing a reader is free; the pread
/// hot path never rebuilds a Key or touches the recorder registry.
pub(crate) struct ZFileReadMetrics {
    codec: ZFileCodec,
    output_bytes: OnceLock<Counter>,
    encoded_bytes: OnceLock<Counter>,
    blocks: OnceLock<Counter>,
    read_batches: OnceLock<Counter>,
    checksum_retries: OnceLock<Counter>,
    decompress_retries: OnceLock<Counter>,
    pread_duration_ok: OnceLock<Histogram>,
    pread_duration_error: OnceLock<Histogram>,
    decompress_duration: OnceLock<Histogram>,
}

impl ZFileReadMetrics {
    pub(crate) fn new(codec: ZFileCodec) -> Self {
        Self {
            codec,
            output_bytes: OnceLock::new(),
            encoded_bytes: OnceLock::new(),
            blocks: OnceLock::new(),
            read_batches: OnceLock::new(),
            checksum_retries: OnceLock::new(),
            decompress_retries: OnceLock::new(),
            pread_duration_ok: OnceLock::new(),
            pread_duration_error: OnceLock::new(),
            decompress_duration: OnceLock::new(),
        }
    }

    /// Record the exact counters of one logical ZFile `pread`. Called
    /// exactly once per pread by the read path's observation wrapper, on
    /// success and on every error exit alike.
    pub(crate) fn record(&self, stats: &ZFileReadStats) {
        let codec = self.codec.as_label();
        if stats.output_bytes > 0 {
            zcounter!(
                self.output_bytes,
                "agentenv_overlaybd_zfile_output_bytes_total",
                codec,
                stats.output_bytes
            );
        }
        if stats.encoded_bytes > 0 {
            zcounter!(
                self.encoded_bytes,
                "agentenv_overlaybd_zfile_encoded_bytes_total",
                codec,
                stats.encoded_bytes
            );
        }
        if stats.blocks > 0 {
            zcounter!(
                self.blocks,
                "agentenv_overlaybd_zfile_blocks_total",
                codec,
                stats.blocks
            );
        }
        if stats.read_batches > 0 {
            zcounter!(
                self.read_batches,
                "agentenv_overlaybd_zfile_read_batches_total",
                codec,
                stats.read_batches
            );
        }
        if stats.checksum_retries > 0 {
            zcounter!(self.checksum_retries, "agentenv_overlaybd_zfile_retries_total", codec, stats.checksum_retries, "reason" => "checksum");
        }
        if stats.decompress_retries > 0 {
            zcounter!(self.decompress_retries, "agentenv_overlaybd_zfile_retries_total", codec, stats.decompress_retries, "reason" => "decompress");
        }
    }

    /// Record the sampled duration histograms of one logical ZFile `pread`.
    /// Called only for preads selected by the 1-in-1024 per-thread sampler,
    /// so both histograms' exported `_count` is a SAMPLE COUNT, not an
    /// operation count. `decompress_elapsed` is the SUM of all per-block
    /// decode times inside this single pread (retry decodes included),
    /// never a per-block record.
    pub(crate) fn record_durations(
        &self,
        status: ZFileReadStatus,
        pread_elapsed: Duration,
        decompress_elapsed: Duration,
    ) {
        let codec = self.codec.as_label();
        let pread_duration = match status {
            ZFileReadStatus::Ok => &self.pread_duration_ok,
            ZFileReadStatus::Error => &self.pread_duration_error,
        };
        pread_duration
            .get_or_init(|| {
                ::metrics::histogram!(
                    "agentenv_overlaybd_zfile_pread_duration_seconds",
                    "codec" => codec,
                    "status" => status.as_label(),
                )
            })
            .record(pread_elapsed.as_secs_f64());
        self.decompress_duration
            .get_or_init(|| {
                ::metrics::histogram!(
                    "agentenv_overlaybd_zfile_decompress_duration_seconds",
                    "codec" => codec,
                )
            })
            .record(decompress_elapsed.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_host, registry_source, RemoteReadStatus, RemoteSource};
    use super::{ZFileCodec, ZFileReadMetrics, ZFileReadStats, ZFileReadStatus};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::time::Duration;

    #[test]
    fn remote_read_status_labels_http_statuses() {
        assert_eq!(
            RemoteReadStatus::from_http_status(429).as_label(),
            "rate_limited"
        );
        assert_eq!(
            RemoteReadStatus::from_http_status(408).as_label(),
            "timeout"
        );
        assert_eq!(RemoteReadStatus::from_http_status(500).as_label(), "error");
    }

    #[test]
    fn normalize_host_extracts_authority_from_url() {
        assert_eq!(
            normalize_host("https://registry-1.docker.io/v2/library/ubuntu/blobs/sha256:abc"),
            "registry-1.docker.io"
        );
        assert_eq!(
            normalize_host("http://localhost:5000/v2/foo"),
            "localhost:5000"
        );
        assert_eq!(
            normalize_host("oss-cn-beijing.aliyuncs.com"),
            "oss-cn-beijing.aliyuncs.com"
        );
        assert_eq!(
            normalize_host("https://user:pass@reg.example.com/path"),
            "reg.example.com"
        );
        assert_eq!(normalize_host("   "), "unknown");
        assert_eq!(normalize_host(""), "unknown");
    }

    #[test]
    fn registry_source_selects_variant_and_carries_host() {
        let direct = registry_source("", "https://reg.example.com/v2/blob");
        assert_eq!(direct.as_label(), "registry_direct");
        assert_eq!(direct.registry_label(), "reg.example.com");

        // P2P and OSS reads do not carry a meaningful registry host.
        let p2p = registry_source("http://127.0.0.1:9000", "https://reg.example.com/v2/blob");
        assert_eq!(p2p.as_label(), "registry_p2p");
        assert_eq!(p2p.registry_label(), "none");

        let oss = RemoteSource::OssObject;
        assert_eq!(oss.as_label(), "oss_object");
        assert_eq!(oss.registry_label(), "none");
    }

    /// Drive the ZFile metrics handle set through a thread-local
    /// [`DebuggingRecorder`] — synchronously, never installing a global
    /// recorder, so this stays safe alongside parallel async tests — and
    /// assert the exact metric names, the fixed label sets, the recorded
    /// counter values, and that all-zero stats record no series at all.
    #[test]
    fn zfile_metrics_use_fixed_low_cardinality_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        ::metrics::with_local_recorder(&recorder, || {
            // Full success+retry stats for lz4, a blocks-only record for
            // zstd, and an all-zero record that must create no series (error
            // exits and EOF reads rely on this zero-skip).
            let lz4 = ZFileReadMetrics::new(ZFileCodec::Lz4);
            let zstd = ZFileReadMetrics::new(ZFileCodec::Zstd);
            lz4.record(&ZFileReadStats {
                output_bytes: 8192,
                encoded_bytes: 9000,
                blocks: 2,
                read_batches: 2,
                checksum_retries: 1,
                decompress_retries: 3,
            });
            zstd.record(&ZFileReadStats {
                blocks: 1,
                ..ZFileReadStats::default()
            });
            zstd.record(&ZFileReadStats::default());

            lz4.record_durations(
                ZFileReadStatus::Ok,
                Duration::from_micros(42),
                Duration::from_micros(30),
            );
            lz4.record_durations(
                ZFileReadStatus::Error,
                Duration::from_micros(7),
                Duration::ZERO,
            );
        });

        let mut counters: Vec<(String, String, u64)> = Vec::new();
        let mut histograms: Vec<(String, String, usize)> = Vec::new();
        for (key, _unit, _description, value) in snapshotter.snapshot().into_vec() {
            let mut labels: Vec<String> = key
                .key()
                .labels()
                .map(|label| format!("{}={}", label.key(), label.value()))
                .collect();
            labels.sort_unstable();
            let name = key.key().name().to_owned();
            match value {
                DebugValue::Counter(recorded) => counters.push((name, labels.join(","), recorded)),
                DebugValue::Histogram(values) => {
                    histograms.push((name, labels.join(","), values.len()))
                }
                DebugValue::Gauge(_) => panic!("zfile metrics must not use gauges: {name}"),
            }
        }
        counters.sort();
        histograms.sort();

        // Aliases for the longest names, so each expected series fits on
        // one line.
        const RETRIES: &str = "agentenv_overlaybd_zfile_retries_total";
        const PREAD_DUR: &str = "agentenv_overlaybd_zfile_pread_duration_seconds";
        const DECOMP_DUR: &str = "agentenv_overlaybd_zfile_decompress_duration_seconds";

        let counter =
            |name: &str, labels: &str, value: u64| (name.to_owned(), labels.to_owned(), value);
        assert_eq!(
            counters,
            vec![
                counter("agentenv_overlaybd_zfile_blocks_total", "codec=lz4", 2),
                counter("agentenv_overlaybd_zfile_blocks_total", "codec=zstd", 1),
                counter(
                    "agentenv_overlaybd_zfile_encoded_bytes_total",
                    "codec=lz4",
                    9000
                ),
                counter(
                    "agentenv_overlaybd_zfile_output_bytes_total",
                    "codec=lz4",
                    8192
                ),
                counter(
                    "agentenv_overlaybd_zfile_read_batches_total",
                    "codec=lz4",
                    2
                ),
                counter(RETRIES, "codec=lz4,reason=checksum", 1),
                counter(RETRIES, "codec=lz4,reason=decompress", 3),
            ],
            "exact counter series with fixed names/labels; zero fields record nothing",
        );

        let histogram = |name: &str, labels: &str, samples: usize| {
            (name.to_owned(), labels.to_owned(), samples)
        };
        assert_eq!(
            histograms,
            vec![
                histogram(DECOMP_DUR, "codec=lz4", 2),
                histogram(PREAD_DUR, "codec=lz4,status=error", 1),
                histogram(PREAD_DUR, "codec=lz4,status=ok", 1),
            ],
            "sampled histogram series with fixed names/labels and sample counts",
        );
    }
}
