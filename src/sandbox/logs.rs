//! Sandbox log decoding and bounded query-page helpers.
use std::collections::BTreeMap;

use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::logging::LogLevel;

pub(crate) const MAX_LOG_BYTES: usize = 32 * 1024 * 1024;
const MAX_LINE_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxLogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub message: String,
    pub fields: BTreeMap<String, String>,
}

/// Same seven-day query window as E2B's LogQueryWindow.
pub(crate) fn log_query_window(
    cursor: Option<i64>,
    backward: bool,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let now = Utc::now();
    let oldest = now - Duration::days(7);
    let (mut start, end) = match cursor {
        Some(ms) => {
            // i64 milliseconds can exceed chrono's representable range.
            let cursor = DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::<Utc>::MAX_UTC);
            if backward {
                (
                    cursor
                        .checked_sub_signed(Duration::days(7))
                        .unwrap_or(oldest),
                    cursor
                        .checked_sub_signed(Duration::nanoseconds(1))
                        .unwrap_or(cursor),
                )
            } else {
                (
                    cursor
                        .checked_add_signed(Duration::milliseconds(1))
                        .unwrap_or(cursor),
                    cursor
                        .checked_add_signed(Duration::days(7))
                        .unwrap_or(DateTime::<Utc>::MAX_UTC),
                )
            }
        }
        None => (oldest, now),
    };
    start = start.max(oldest);
    (start, end)
}

pub(crate) struct LogPage {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    limit: usize,
    backward: bool,
    level: Option<LogLevel>,
    search: Option<String>,
    entries: Vec<SandboxLogEntry>,
}

impl LogPage {
    pub(crate) fn new(
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: usize,
        backward: bool,
        level: Option<LogLevel>,
        search: Option<String>,
    ) -> Self {
        Self {
            start,
            end,
            limit,
            backward,
            level,
            search,
            entries: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, entry: SandboxLogEntry) {
        if self.limit == 0
            || entry.timestamp < self.start
            || entry.timestamp > self.end
            || self.level.is_some_and(|level| entry.level < level)
            || self
                .search
                .as_ref()
                .is_some_and(|search| !entry.message.contains(search))
        {
            return;
        }
        self.entries.push(entry);
    }

    pub(crate) fn finish(mut self) -> Vec<SandboxLogEntry> {
        self.entries.sort_by(|left, right| {
            if self.backward {
                right.timestamp.cmp(&left.timestamp)
            } else {
                left.timestamp.cmp(&right.timestamp)
            }
        });
        // Equal timestamps remain separate records; sorting is stable and does not deduplicate.
        self.entries.truncate(self.limit);
        self.entries
    }
}

/// Handles arbitrary HTTP/file chunk boundaries, with a hard bound on a single line.
#[derive(Default)]
pub(crate) struct LogDecoder {
    pending: Vec<u8>,
    fallback: Option<(DateTime<Utc>, LogLevel, BTreeMap<String, String>)>,
}

impl LogDecoder {
    pub(crate) fn with_fallback(
        timestamp: DateTime<Utc>,
        level: LogLevel,
        fields: BTreeMap<String, String>,
    ) -> Self {
        Self {
            pending: Vec::new(),
            fallback: Some((timestamp, level, fields)),
        }
    }

    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Result<Vec<SandboxLogEntry>> {
        let mut result = Vec::new();
        for part in bytes.split_inclusive(|b| *b == b'\n') {
            if self.pending.len() + part.len() > MAX_LINE_BYTES {
                bail!("sandbox log line exceeds size limit");
            }
            self.pending.extend_from_slice(part);
            if part.ends_with(b"\n") {
                if let Some(entry) = parse_log_line(&self.pending).or_else(|| {
                    let (timestamp, level, fields) = self.fallback.as_ref()?;
                    let message = std::str::from_utf8(&self.pending).ok()?.trim_end();
                    (!message.is_empty()).then(|| SandboxLogEntry {
                        timestamp: *timestamp,
                        level: *level,
                        message: message.to_owned(),
                        fields: fields.clone(),
                    })
                }) {
                    result.push(entry);
                }
                self.pending.clear();
            }
        }
        Ok(result)
    }
}

fn parse_log_line(bytes: &[u8]) -> Option<SandboxLogEntry> {
    let line = std::str::from_utf8(bytes).ok()?.trim_end();
    if let Ok(entry) = serde_json::from_str::<SandboxLogEntry>(line) {
        return Some(entry);
    }
    let (prefix, content) = line.split_once(' ').unwrap_or(("", line));
    let stamp = DateTime::parse_from_rfc3339(&format!("{}Z", prefix.replace('_', "T")))
        .ok()
        .map(|t| t.with_timezone(&Utc));
    let content = if stamp.is_some() {
        content.trim_start()
    } else {
        line
    };
    if let Ok(mut object) =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(content)
    {
        let timestamp = object
            .remove("timestamp")
            .and_then(|v| {
                v.as_str()
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            })
            .map(|t| t.with_timezone(&Utc))
            .or(stamp)?;
        let message = object.remove("message")?.as_str()?.to_owned();
        let level = object
            .remove("level")
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or(LogLevel::Info);
        let fields = object
            .into_iter()
            .filter_map(|(k, v)| match v {
                serde_json::Value::String(s) => Some((k, s)),
                serde_json::Value::Number(n) => Some((k, n.to_string())),
                serde_json::Value::Bool(b) => Some((k, b.to_string())),
                _ => None,
            })
            .collect();
        Some(SandboxLogEntry {
            timestamp,
            message,
            level,
            fields,
        })
    } else {
        Some(SandboxLogEntry {
            timestamp: stamp?,
            level: LogLevel::Info,
            message: content.to_owned(),
            fields: BTreeMap::from([("logger".into(), "envd".into())]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ms: i64, level: LogLevel, message: &str) -> SandboxLogEntry {
        SandboxLogEntry {
            timestamp: DateTime::from_timestamp_millis(ms).unwrap(),
            level,
            message: message.into(),
            fields: BTreeMap::new(),
        }
    }

    fn test_page(limit: usize, backward: bool) -> LogPage {
        LogPage::new(
            DateTime::from_timestamp_millis(0).unwrap(),
            DateTime::from_timestamp_millis(10000).unwrap(),
            limit,
            backward,
            None,
            None,
        )
    }

    #[test]
    fn sandbox_logs_merge_filters_before_limit_and_preserves_equal_timestamps() {
        let mut page = LogPage::new(
            DateTime::from_timestamp_millis(0).unwrap(),
            DateTime::from_timestamp_millis(10000).unwrap(),
            3,
            true,
            Some(LogLevel::Warn),
            Some("Error".into()),
        );
        for (ms, level, text) in [
            (10, LogLevel::Error, "Error old"),
            (20, LogLevel::Warn, "Error same"),
            (20, LogLevel::Warn, "Error same"),
            (30, LogLevel::Debug, "Error debug"),
            (40, LogLevel::Error, "error lowercase"),
            (50, LogLevel::Error, "Error new"),
        ] {
            page.push(entry(ms, level, text));
        }
        let result = page.finish();
        assert_eq!(
            result
                .iter()
                .map(|e| e.timestamp.timestamp_millis())
                .collect::<Vec<_>>(),
            vec![50, 20, 20]
        );
        let mut page = test_page(2, false);
        for ms in [30, 10, 20] {
            page.push(entry(ms, LogLevel::Info, "message"));
        }
        assert_eq!(
            page.finish()
                .iter()
                .map(|e| e.timestamp.timestamp_millis())
                .collect::<Vec<_>>(),
            vec![10, 20]
        );
        let mut page = test_page(0, true);
        page.push(entry(1, LogLevel::Error, "message"));
        assert!(page.finish().is_empty());
    }

    #[test]
    fn sandbox_logs_decoder_handles_svlogd_chunking_and_flat_fields() {
        let line = b"2026-09-29_01:02:03.12345 {\"timestamp\":\"2026-09-29T01:02:03.123456Z\",\"message\":\"started\",\"level\":\"debug\",\"logger\":\"process\",\"pid\":42,\"nested\":{}}\n";
        let mut decoder = LogDecoder::default();
        let mut logs = Vec::new();
        for chunk in line.chunks(7) {
            logs.extend(decoder.feed(chunk).unwrap());
        }
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].level, LogLevel::Debug);
        assert_eq!(logs[0].fields["pid"], "42");
        assert!(!logs[0].fields.contains_key("nested"));
        assert_eq!(logs[0].timestamp.timestamp_subsec_micros(), 123456);
        assert!(decoder.feed(b"partial").unwrap().is_empty());
        let mut decoder = LogDecoder::default();
        assert!(decoder.feed(&vec![b'x'; MAX_LINE_BYTES + 1]).is_err());
    }

    #[test]
    fn sandbox_logs_pagination_excludes_cursor_and_terminates() {
        let base = Utc::now().timestamp_millis() - 1000;
        let entries: Vec<_> = (0..10)
            .map(|offset| entry(base + offset * 10, LogLevel::Info, "same"))
            .collect();
        for backward in [false, true] {
            for limit in [1, 4, 20] {
                let mut cursor = None;
                let mut collected = Vec::new();
                let mut terminated = false;
                for _ in 0..=entries.len() {
                    let (start, end) = log_query_window(cursor, backward);
                    let mut page = LogPage::new(start, end, limit, backward, None, None);
                    for entry in &entries {
                        page.push(entry.clone());
                    }
                    let result = page.finish();
                    assert!(result.len() <= limit);
                    if result.is_empty() {
                        terminated = true;
                        break;
                    }
                    let next = result.last().unwrap().timestamp.timestamp_millis();
                    if let Some(previous) = cursor {
                        assert!(if backward {
                            next < previous
                        } else {
                            next > previous
                        });
                    }
                    cursor = Some(next);
                    collected.extend(result);
                }
                assert!(terminated, "pagination must reach an empty page");
                let mut expected = entries.clone();
                expected.sort_by(|a, b| {
                    if backward {
                        b.timestamp.cmp(&a.timestamp)
                    } else {
                        a.timestamp.cmp(&b.timestamp)
                    }
                });
                assert_eq!(collected, expected);
            }
        }
    }

    #[test]
    fn sandbox_logs_hard_limit_can_skip_remaining_boundary_millisecond() {
        let base = Utc::now().timestamp_millis() - 1000;
        let mut entries = vec![entry(base - 1, LogLevel::Info, "before")];
        for micros in [0, 0, 100, 500, 999] {
            let mut log = entry(base, LogLevel::Info, "same");
            log.timestamp += Duration::microseconds(micros);
            entries.push(log);
        }
        entries.push(entry(base + 1, LogLevel::Info, "after"));
        for backward in [false, true] {
            let (start, end) = log_query_window(None, backward);
            let mut page = LogPage::new(start, end, 3, backward, None, None);
            for log in &entries {
                page.push(log.clone());
            }
            let first = page.finish();
            assert_eq!(first.len(), 3);
            let cursor = first.last().unwrap().timestamp.timestamp_millis();
            assert_eq!(cursor, base);
            let (start, end) = log_query_window(Some(cursor), backward);
            let mut page = LogPage::new(start, end, 3, backward, None, None);
            for log in &entries {
                page.push(log.clone());
            }
            let second = page.finish();
            assert_eq!(second.len(), 1);
            assert_eq!(second[0].message, if backward { "before" } else { "after" });
            // The three unreturned logs in the cursor millisecond are skipped.
            assert_eq!(first.len() + second.len(), entries.len() - 3);
        }
    }

    #[test]
    fn sandbox_logs_cursor_handles_int64_max_and_expired_window() {
        let future = log_query_window(Some(i64::MAX), false);
        assert!(future.0 <= future.1);
        let old = log_query_window(Some(0), true);
        assert!(old.0 > old.1);
    }
}
