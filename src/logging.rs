use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex, Once, OnceLock, Weak};

use serde::{Deserialize, Serialize};

use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id},
    Event, Subscriber,
};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{
    filter::filter_fn, fmt, layer::Context as LayerContext, registry::LookupSpan, EnvFilter, Layer,
};

const DEFAULT_FILTER: &str = "agentenv=info,envd=info,uvm_ublk=info";
const LOG_FORMAT_ENV: &str = "AENV_LOG_FORMAT";
const LOG_SPAN_EVENTS_ENV: &str = "AENV_LOG_SPAN_EVENTS";

static INIT_LOGGING: Once = Once::new();

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

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
            .with(BuildLogLayer.with_filter(build_filter))
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
            .with(BuildLogLayer.with_filter(build_filter))
            .try_init();
    });
}

/// Receives a build's log lines: those written through a [`BuildLogger`] and
/// the `agentenv` events emitted inside a span whose `build_id` field names
/// the build it was registered for.
pub(crate) trait BuildLogSink: Send + Sync {
    fn log(&self, level: LogLevel, step: Option<&str>, message: String);
}

/// Writes log lines to a build's sink, or nowhere when it has none.
#[derive(Clone, Default)]
pub struct BuildLogger(Option<Arc<dyn BuildLogSink>>);

impl BuildLogger {
    pub(crate) fn new(sink: Arc<dyn BuildLogSink>) -> Self {
        Self(Some(sink))
    }

    pub fn log(&self, level: LogLevel, step: Option<&str>, message: impl Into<String>) {
        let Some(sink) = &self.0 else { return };
        sink.log(level, step, message.into());
    }

    pub(crate) fn output(&self, step: Option<&str>, stderr: bool, bytes: &[u8]) {
        for line in String::from_utf8_lossy(bytes).lines() {
            self.log(
                if stderr {
                    LogLevel::Warn
                } else {
                    LogLevel::Info
                },
                step,
                line,
            );
        }
    }
}

impl std::fmt::Debug for BuildLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BuildLogger")
            .field(&self.0.is_some())
            .finish()
    }
}

type BuildLogSinks = Mutex<HashMap<String, Weak<dyn BuildLogSink>>>;

static BUILD_LOG_SINKS: OnceLock<BuildLogSinks> = OnceLock::new();

fn build_log_sinks() -> &'static BuildLogSinks {
    BUILD_LOG_SINKS.get_or_init(Default::default)
}

/// Sends the events of `build_id`'s spans to `sink` while it is alive.
/// Also drops the sinks that died without being unregistered.
pub(crate) fn route_build_logs(build_id: String, sink: Weak<dyn BuildLogSink>) {
    let mut sinks = build_log_sinks().lock().unwrap();
    sinks.retain(|_, sink| sink.strong_count() > 0);
    sinks.insert(build_id, sink);
}

/// Stops routing `build_id`, unless a newer sink has replaced `sink`.
pub(crate) fn stop_build_log_routing(build_id: &str, sink: &Weak<dyn BuildLogSink>) {
    let mut sinks = build_log_sinks().lock().unwrap();
    if sinks
        .get(build_id)
        .is_some_and(|current| current.ptr_eq(sink))
    {
        sinks.remove(build_id);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BuildLogLayer;

#[derive(Clone, Debug)]
struct BuildSpan {
    build_id: String,
}

#[derive(Default)]
struct FieldVisitor {
    build_id: Option<String>,
    message: Option<String>,
    fields: Vec<String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}");
        match field.name() {
            "build_id" => self.build_id = Some(value.trim_matches('"').to_owned()),
            "message" => self.message = Some(value),
            name => self.fields.push(format!("{name}={value}")),
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "build_id" => self.build_id = Some(value.to_owned()),
            "message" => self.message = Some(value.to_owned()),
            name => self.fields.push(format!("{name}={value:?}")),
        }
    }
}

impl<S> Layer<S> for BuildLogLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        let inherited = if attrs.is_contextual() {
            ctx.lookup_current()
        } else {
            attrs.parent().and_then(|parent| ctx.span(parent))
        }
        .and_then(|span| {
            span.extensions()
                .get::<BuildSpan>()
                .map(|build| build.build_id.clone())
        });
        let Some(build_id) = visitor.build_id.or(inherited) else {
            return;
        };
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(BuildSpan { build_id });
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        if !event.metadata().target().starts_with("agentenv::") {
            return;
        }
        let Some(build_id) = ctx.event_scope(event).and_then(|scope| {
            scope.from_root().find_map(|span| {
                span.extensions()
                    .get::<BuildSpan>()
                    .map(|build| build.build_id.clone())
            })
        }) else {
            return;
        };
        let sink = build_log_sinks()
            .lock()
            .unwrap()
            .get(&build_id)
            .and_then(Weak::upgrade);
        let Some(sink) = sink else { return };

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let mut message = visitor
            .message
            .unwrap_or_else(|| event.metadata().name().to_owned());
        if !visitor.fields.is_empty() {
            message.push(' ');
            message.push_str(&visitor.fields.join(" "));
        }
        let level = match *event.metadata().level() {
            tracing::Level::DEBUG | tracing::Level::TRACE => LogLevel::Debug,
            tracing::Level::INFO => LogLevel::Info,
            tracing::Level::WARN => LogLevel::Warn,
            tracing::Level::ERROR => LogLevel::Error,
        };
        sink.log(level, None, message);
    }
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
