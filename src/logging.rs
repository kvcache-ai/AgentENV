use std::env;
use std::sync::Once;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter::filter_fn, fmt, EnvFilter, Layer};

const DEFAULT_FILTER: &str = "agentenv=info,envd=info,uvm_ublk=info";
const LOG_FORMAT_ENV: &str = "AENV_LOG_FORMAT";
const LOG_SPAN_EVENTS_ENV: &str = "AENV_LOG_SPAN_EVENTS";

static INIT_LOGGING: Once = Once::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Compact,
    Pretty,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpanEvents {
    Off,
    New,
    Enter,
    Exit,
    Close,
    Active,
    Full,
}

impl LogFormat {
    fn from_env() -> Self {
        let raw = env::var(LOG_FORMAT_ENV).unwrap_or_default();
        Self::parse(&raw)
    }

    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "json" => Self::Json,
            "pretty" => Self::Pretty,
            "compact" | "" => Self::Compact,
            _ => Self::Compact,
        }
    }
}

impl SpanEvents {
    fn from_env() -> Self {
        let raw = env::var(LOG_SPAN_EVENTS_ENV).unwrap_or_default();
        Self::parse(&raw)
    }

    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "none" | "" => Self::Off,
            "new" => Self::New,
            "enter" => Self::Enter,
            "exit" => Self::Exit,
            "close" => Self::Close,
            "active" => Self::Active,
            "full" => Self::Full,
            _ => Self::Off,
        }
    }

    fn to_fmt_span(self) -> FmtSpan {
        match self {
            Self::Off => FmtSpan::NONE,
            Self::New => FmtSpan::NEW,
            Self::Enter => FmtSpan::ENTER,
            Self::Exit => FmtSpan::EXIT,
            Self::Close => FmtSpan::CLOSE,
            Self::Active => FmtSpan::ACTIVE,
            Self::Full => FmtSpan::FULL,
        }
    }
}

/// Initialize process-wide logging once.
///
/// - Log level filter comes from `RUST_LOG`, or defaults to `agentenv=info,envd=info,uvm_ublk=info`.
/// - Output format comes from `AENV_LOG_FORMAT`: `compact` (default), `pretty`, or `json`.
/// - Span lifecycle events come from `AENV_LOG_SPAN_EVENTS`: `off` (default), `new`, `enter`,
///   `exit`, `close`, `active`, or `full`.
///
/// Repeated calls are no-ops. If another global subscriber has already been installed,
/// the initialization error is ignored.
pub fn init() {
    INIT_LOGGING.call_once(|| {
        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

        let format = LogFormat::from_env();
        let span_events = SpanEvents::from_env().to_fmt_span();
        let base = fmt::layer().with_span_events(span_events);

        let fmt_layer = match format {
            LogFormat::Compact => base.compact().boxed(),
            LogFormat::Pretty => base.pretty().boxed(),
            LogFormat::Json => base.json().boxed(),
        };

        let build_filter = filter_fn(|metadata| {
            metadata.target().starts_with("agentenv::")
                && *metadata.level() <= tracing::Level::DEBUG
        });
        let _ = tracing_subscriber::registry()
            .with(fmt_layer.with_filter(filter))
            .with(crate::template::logs::BuildLogLayer.with_filter(build_filter))
            .try_init();
    });
}

/// Initialize process-wide logging once for tests.
///
/// Same behavior as [`init`], but writes through the test writer so output is
/// captured by Rust test harness and only shown on failures (unless nocapture).
pub fn init_for_tests() {
    INIT_LOGGING.call_once(|| {
        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

        let format = LogFormat::from_env();
        let span_events = SpanEvents::from_env().to_fmt_span();
        let base = fmt::layer()
            .with_test_writer()
            .with_span_events(span_events);

        let fmt_layer = match format {
            LogFormat::Compact => base.compact().boxed(),
            LogFormat::Pretty => base.pretty().boxed(),
            LogFormat::Json => base.json().boxed(),
        };

        let build_filter = filter_fn(|metadata| {
            metadata.target().starts_with("agentenv::")
                && *metadata.level() <= tracing::Level::DEBUG
        });
        let _ = tracing_subscriber::registry()
            .with(fmt_layer.with_filter(filter))
            .with(crate::template::logs::BuildLogLayer.with_filter(build_filter))
            .try_init();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_format_from_env() {
        assert_eq!(LogFormat::parse(""), LogFormat::Compact);
        assert_eq!(LogFormat::parse("compact"), LogFormat::Compact);
        assert_eq!(LogFormat::parse("pretty"), LogFormat::Pretty);
        assert_eq!(LogFormat::parse("json"), LogFormat::Json);
        assert_eq!(LogFormat::parse("unknown-value"), LogFormat::Compact);
    }

    #[test]
    fn parse_span_events_from_env() {
        assert_eq!(SpanEvents::parse(""), SpanEvents::Off);
        assert_eq!(SpanEvents::parse("off"), SpanEvents::Off);
        assert_eq!(SpanEvents::parse("none"), SpanEvents::Off);
        assert_eq!(SpanEvents::parse("new"), SpanEvents::New);
        assert_eq!(SpanEvents::parse("enter"), SpanEvents::Enter);
        assert_eq!(SpanEvents::parse("exit"), SpanEvents::Exit);
        assert_eq!(SpanEvents::parse("close"), SpanEvents::Close);
        assert_eq!(SpanEvents::parse("active"), SpanEvents::Active);
        assert_eq!(SpanEvents::parse("full"), SpanEvents::Full);
        assert_eq!(SpanEvents::parse("unknown-value"), SpanEvents::Off);
    }
}
