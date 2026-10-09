use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const SLOW_THRESHOLD_MICROS: u64 = 5000;

#[derive(Debug, Clone)]
pub struct LatencyWindow {
    inner: Arc<LatencyInner>,
}

#[derive(Debug)]
struct LatencyInner {
    count: AtomicU64,
    sum_micros: AtomicU64,
    max_micros: AtomicU64,
}

impl LatencyWindow {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(LatencyInner {
                count: AtomicU64::new(0),
                sum_micros: AtomicU64::new(0),
                max_micros: AtomicU64::new(0),
            }),
        }
    }

    pub fn observe_micros(&self, value: u64) {
        self.inner.count.fetch_add(1, Ordering::Relaxed);
        self.inner.sum_micros.fetch_add(value, Ordering::Relaxed);
        let mut current = self.inner.max_micros.load(Ordering::Relaxed);
        while value > current {
            match self.inner.max_micros.compare_exchange(
                current,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let count = self.inner.count.load(Ordering::Relaxed);
        let sum = self.inner.sum_micros.load(Ordering::Relaxed);
        let max = self.inner.max_micros.load(Ordering::Relaxed);
        LatencySnapshot { count, mean_micros: sum.checked_div(count).unwrap_or(0), max_micros: max }
    }
}

impl Default for LatencyWindow {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LatencySnapshot {
    pub count: u64,
    pub mean_micros: u64,
    pub max_micros: u64,
}

#[derive(Debug, Clone)]
pub struct Histogram {
    inner: Arc<Mutex<HistogramInner>>,
}

#[derive(Debug)]
struct HistogramInner {
    samples: Vec<u64>,
    next: usize,
    filled: usize,
    count: u64,
    sum_micros: u64,
    max_micros: u64,
}

impl Histogram {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.clamp(64, 8192);
        Self {
            inner: Arc::new(Mutex::new(HistogramInner {
                samples: vec![0; capacity],
                next: 0,
                filled: 0,
                count: 0,
                sum_micros: 0,
                max_micros: 0,
            })),
        }
    }

    pub fn record(&self, value_micros: u64) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.count = guard.count.saturating_add(1);
            guard.sum_micros = guard.sum_micros.saturating_add(value_micros);
            guard.max_micros = guard.max_micros.max(value_micros);
            let len = guard.samples.len();
            if len > 0 {
                let slot = guard.next % len;
                guard.samples[slot] = value_micros;
                guard.next = guard.next.wrapping_add(1);
                guard.filled = guard.filled.saturating_add(1).min(len);
            }
        }
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut values = Vec::new();
        let (count, sum, max) = match self.inner.lock() {
            Ok(guard) => {
                let take = guard.filled.min(guard.samples.len());
                values.extend_from_slice(&guard.samples[..take]);
                (guard.count, guard.sum_micros, guard.max_micros)
            }
            Err(_) => (0, 0, 0),
        };
        values.sort_unstable();
        let quantile = |q: f64| -> u64 {
            if values.is_empty() {
                return 0;
            }
            let rank = ((q * values.len() as f64).ceil() as usize).clamp(1, values.len());
            values[rank - 1]
        };
        HistogramSnapshot {
            count,
            mean_micros: sum.checked_div(count).unwrap_or(0),
            max_micros: max,
            p50_micros: quantile(0.50),
            p90_micros: quantile(0.90),
            p95_micros: quantile(0.95),
            p99_micros: quantile(0.99),
            p999_micros: quantile(0.999),
        }
    }

    pub fn render_prometheus(&self, name: &str, help: &str) -> String {
        let snap = self.snapshot();
        let mut out = String::new();
        out.push_str(&format!("# HELP {name} {help}\n"));
        out.push_str(&format!("# TYPE {name} summary\n"));
        out.push_str(&format!(
            "{name}{{quantile=\"0.5\"}} {}\n{name}{{quantile=\"0.9\"}} {}\n{name}{{quantile=\"0.95\"}} {}\n{name}{{quantile=\"0.99\"}} {}\n{name}{{quantile=\"0.999\"}} {}\n{name}_count {}\n{name}_max_microseconds {}\n",
            snap.p50_micros,
            snap.p90_micros,
            snap.p95_micros,
            snap.p99_micros,
            snap.p999_micros,
            snap.count,
            snap.max_micros
        ));
        out
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub mean_micros: u64,
    pub max_micros: u64,
    pub p50_micros: u64,
    pub p90_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub p999_micros: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent: Option<String>,
    pub name: String,
    pub started_unix: u64,
    pub duration_micros: u64,
    pub attributes: Vec<(String, String)>,
}

impl TraceSpan {
    pub fn root(name: String, started_unix: u64) -> Self {
        let trace_id = hex_id(&started_unix.to_le_bytes(), b"trace");
        let span_id = hex_id(&started_unix.to_le_bytes(), b"span");
        Self {
            trace_id,
            span_id,
            parent: None,
            name,
            started_unix,
            duration_micros: 0,
            attributes: Vec::new(),
        }
    }

    pub fn linked(trace_id: String, parent: String, name: String, started_unix: u64) -> Self {
        let mut span = Self::root(name, started_unix);
        span.trace_id = trace_id;
        span.parent = Some(parent);
        span
    }

    pub fn child(&self, name: String, started_unix: u64) -> Self {
        let span_id = hex_id(&started_unix.to_le_bytes(), self.span_id.as_bytes());
        Self {
            trace_id: self.trace_id.clone(),
            span_id,
            parent: Some(self.span_id.clone()),
            name,
            started_unix,
            duration_micros: 0,
            attributes: Vec::new(),
        }
    }

    pub fn finish(&mut self, duration_micros: u64) {
        self.duration_micros = duration_micros;
    }

    pub fn attr(&mut self, key: String, value: String) {
        if self.attributes.len() < 32 {
            self.attributes.push((key, value));
        }
    }
}

fn hex_id(seed: &[u8], salt: &[u8]) -> String {
    let mut out = String::with_capacity(32);
    let mut counter = 0u8;
    while out.len() < 32 {
        let mut state = 0xcbf29ce484222325u64;
        for byte in seed.iter().chain(salt.iter()).chain([counter].iter()) {
            state ^= *byte as u64;
            state = state.wrapping_mul(0x100000001b3);
        }
        for byte in state.to_le_bytes() {
            if out.len() >= 32 {
                break;
            }
            out.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap_or('0'));
        }
        counter = counter.wrapping_add(1);
    }
    out
}

#[derive(Debug, Default)]
pub struct TraceCollector {
    spans: std::collections::VecDeque<TraceSpan>,
    capacity: usize,
}
impl TraceCollector {
    pub fn new(capacity: usize) -> Self {
        Self { spans: std::collections::VecDeque::new(), capacity: capacity.clamp(1, 10000) }
    }

    pub fn push(&mut self, span: TraceSpan) {
        if self.capacity == 0 {
            return;
        }
        while self.spans.len() >= self.capacity {
            self.spans.pop_front();
        }
        self.spans.push_back(span);
    }

    pub fn recent(&self, limit: usize) -> Vec<TraceSpan> {
        self.recent_filtered(limit, None, None)
    }

    pub fn recent_filtered(
        &self,
        limit: usize,
        name: Option<&str>,
        table: Option<&str>,
    ) -> Vec<TraceSpan> {
        let limit = limit.clamp(1, 1000);
        self.spans
            .iter()
            .rev()
            .filter(|span| name.is_none_or(|want| span.name == want))
            .filter(|span| {
                table.is_none_or(|want| {
                    span.attributes.iter().any(|(key, value)| key == "table" && value == want)
                })
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn drain(&mut self, limit: usize) -> Vec<TraceSpan> {
        let limit = limit.clamp(1, 1000).min(self.spans.len());
        self.spans.drain(..limit).collect()
    }

    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowEntry {
    pub kind: String,
    pub fingerprint: String,
    pub table: String,
    pub micros: u64,
    pub at_unix: u64,
}

#[derive(Debug, Clone)]
pub struct SlowLog {
    inner: Arc<Mutex<SlowLogInner>>,
}

#[derive(Debug)]
struct SlowLogInner {
    entries: std::collections::VecDeque<SlowEntry>,
    capacity: usize,
}

impl SlowLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SlowLogInner {
                entries: std::collections::VecDeque::new(),
                capacity: capacity.clamp(16, 4096),
            })),
        }
    }

    pub fn record(&self, entry: SlowEntry) {
        if let Ok(mut guard) = self.inner.lock() {
            while guard.entries.len() >= guard.capacity {
                guard.entries.pop_front();
            }
            guard.entries.push_back(entry);
        }
    }

    pub fn recent(&self, limit: usize) -> Vec<SlowEntry> {
        self.recent_filtered(limit, None)
    }

    pub fn recent_filtered(&self, limit: usize, table: Option<&str>) -> Vec<SlowEntry> {
        let limit = limit.clamp(1, 500);
        match self.inner.lock() {
            Ok(guard) => guard
                .entries
                .iter()
                .rev()
                .filter(|entry| table.is_none_or(|name| entry.table == name))
                .take(limit)
                .cloned()
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().map(|guard| guard.entries.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for SlowLog {
    fn default() -> Self {
        Self::new(256)
    }
}

fn otlp_id(value: &str, len: usize) -> String {
    let clean: String =
        value.chars().filter(|c| c.is_ascii_hexdigit()).collect::<String>().to_lowercase();
    if clean.len() >= len {
        clean[clean.len() - len..].to_string()
    } else {
        format!("{:0>width$}", clean, width = len)
    }
}

fn otlp_attributes(attributes: &[(String, String)]) -> Vec<serde_json::Value> {
    attributes
        .iter()
        .map(|(key, value)| serde_json::json!({"key": key, "value": {"stringValue": value}}))
        .collect()
}

pub fn otlp_resource_spans(service: &str, spans: &[TraceSpan]) -> serde_json::Value {
    let encoded: Vec<serde_json::Value> = spans
        .iter()
        .map(|span| {
            let start_nanos = span.started_unix.saturating_mul(1_000_000_000);
            let end_nanos = start_nanos.saturating_add(span.duration_micros.saturating_mul(1_000));
            let mut encoded = serde_json::json!({
                "traceId": otlp_id(&span.trace_id, 32),
                "spanId": otlp_id(&span.span_id, 16),
                "name": span.name,
                "kind": 1,
                "startTimeUnixNano": start_nanos.to_string(),
                "endTimeUnixNano": end_nanos.to_string(),
                "attributes": otlp_attributes(&span.attributes),
                "status": {},
            });
            if let Some(parent) = span.parent.as_ref() {
                encoded["parentSpanId"] = serde_json::Value::String(otlp_id(parent, 16));
            }
            encoded
        })
        .collect();
    serde_json::json!({
        "resourceSpans": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
            "scopeSpans": [{"scope": {"name": "rymedb"}, "spans": encoded}],
        }],
    })
}

pub fn parse_traceparent(header: &str) -> Option<(String, String)> {
    let mut parts = header.split('-');
    let version = parts.next()?;
    if version.len() != 2 || !version.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    if version == "ff" {
        return None;
    }
    let trace_id = parts.next()?;
    let parent_id = parts.next()?;
    let flags = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if trace_id.len() != 32
        || parent_id.len() != 16
        || flags.len() != 2
        || !trace_id.bytes().all(|b| b.is_ascii_hexdigit())
        || !parent_id.bytes().all(|b| b.is_ascii_hexdigit())
        || !flags.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return None;
    }
    if trace_id.bytes().all(|b| b == b'0') || parent_id.bytes().all(|b| b == b'0') {
        return None;
    }
    Some((trace_id.to_lowercase(), parent_id.to_lowercase()))
}

pub fn query_fingerprint(sql: &str) -> String {
    let mut out = String::new();
    let mut current = String::new();
    let mut quoted: Option<char> = None;
    let flush = |current: &mut String, out: &mut String| {
        if current.is_empty() {
            return;
        }
        if current.chars().all(|c| c.is_ascii_digit()) {
            out.push('?');
        } else {
            out.push_str(current);
        }
        current.clear();
    };
    for ch in sql.chars() {
        if let Some(q) = quoted {
            if ch == q {
                quoted = None;
                out.push('?');
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            flush(&mut current, &mut out);
            quoted = Some(ch);
            continue;
        }
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            current.push(ch.to_ascii_uppercase());
        } else {
            flush(&mut current, &mut out);
            if ch.is_whitespace() {
                if !out.ends_with(' ') && !out.is_empty() {
                    out.push(' ');
                }
            } else if ch == ',' || ch == ';' || ch == '(' || ch == ')' || ch == '=' {
                if !out.ends_with(' ') && !out.is_empty() {
                    out.push(' ');
                }
                out.push(ch);
                out.push(' ');
            } else {
                out.push(ch);
            }
        }
    }
    flush(&mut current, &mut out);
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_track_order() {
        let histogram = Histogram::new(128);
        for value in 1..=100u64 {
            histogram.record(value);
        }
        let snap = histogram.snapshot();
        assert_eq!(snap.count, 100);
        assert!(snap.p50_micros >= 45 && snap.p50_micros <= 55);
        assert!(snap.p99_micros >= 95);
        assert_eq!(snap.p999_micros, 100);
        assert_eq!(snap.max_micros, 100);
    }

    #[test]
    fn window_still_works() {
        let window = LatencyWindow::new();
        window.observe_micros(10);
        window.observe_micros(30);
        let snap = window.snapshot();
        assert_eq!(snap.count, 2);
        assert_eq!(snap.max_micros, 30);
    }

    #[test]
    fn trace_parent_links() {
        let mut root = TraceSpan::root(String::from("gateway"), 1000);
        root.attr(String::from("table"), String::from("docs"));
        root.finish(250);
        let child = root.child(String::from("storage"), 1001);
        assert_eq!(child.trace_id, root.trace_id);
        assert_eq!(child.parent, Some(root.span_id.clone()));
        assert_ne!(child.span_id, root.span_id);
        assert_eq!(root.attributes.len(), 1);
    }

    #[test]
    fn collector_bounds_memory() {
        let mut collector = TraceCollector::new(4);
        for index in 0..10 {
            collector.push(TraceSpan::root(format!("op-{index}"), 1000 + index as u64));
        }
        assert_eq!(collector.len(), 4);
        assert_eq!(collector.recent(2).len(), 2);
    }

    #[test]
    fn collector_filters_by_name_and_table() {
        let mut collector = TraceCollector::new(16);
        for (index, (name, table)) in
            [("kv_put", "a"), ("sql_exec", "b"), ("kv_put", "b")].iter().enumerate()
        {
            let mut span = TraceSpan::root(String::from(*name), 1000 + index as u64);
            span.attr(String::from("table"), String::from(*table));
            collector.push(span);
        }
        let puts = collector.recent_filtered(10, Some("kv_put"), None);
        assert_eq!(puts.len(), 2);
        assert!(puts.iter().all(|span| span.name == "kv_put"));
        assert_eq!(puts.first().map(|span| span.started_unix), Some(1002));
        let b = collector.recent_filtered(10, None, Some("b"));
        assert_eq!(b.len(), 2);
        let both = collector.recent_filtered(10, Some("kv_put"), Some("b"));
        assert_eq!(both.len(), 1);
        assert_eq!(both.first().map(|span| span.started_unix), Some(1002));
        assert!(collector.recent_filtered(10, Some("missing"), None).is_empty());
        assert!(collector.recent_filtered(10, None, Some("missing")).is_empty());
        assert_eq!(collector.recent_filtered(1, None, Some("b")).len(), 1);
    }

    #[test]
    fn fingerprint_normalizes_literals() {
        let first = query_fingerprint("SELECT * FROM docs KEY 'hello'");
        let second = query_fingerprint("select * from docs KEY 'world'");
        assert_eq!(first, second);
        assert!(first.contains('?'));
        assert!(!first.contains("hello"));
        let numbered = query_fingerprint("SELECT * FROM docs LIMIT 100");
        assert!(numbered.contains('?'));
    }

    #[test]
    fn slow_log_bounds_and_orders() {
        let log = SlowLog::new(16);
        for index in 0..20u64 {
            log.record(SlowEntry {
                kind: String::from("sql"),
                fingerprint: format!("SELECT {index}"),
                table: String::from("docs"),
                micros: 6000 + index,
                at_unix: 1000 + index,
            });
        }
        assert_eq!(log.len(), 16);
        let recent = log.recent(500);
        assert_eq!(recent.len(), 16);
        assert_eq!(recent.first().map(|e| e.micros), Some(6019));
        assert_eq!(recent.last().map(|e| e.micros), Some(6004));
        assert_eq!(log.recent(2).len(), 2);
    }

    #[test]
    fn slow_log_filters_by_table_newest_first() {
        let log = SlowLog::new(16);
        for (index, table) in ["a", "b", "a", "b", "a"].iter().enumerate() {
            log.record(SlowEntry {
                kind: String::from("sql"),
                fingerprint: format!("SELECT {index}"),
                table: String::from(*table),
                micros: 6000 + index as u64,
                at_unix: 1000 + index as u64,
            });
        }
        let filtered = log.recent_filtered(500, Some("a"));
        assert_eq!(filtered.len(), 3);
        assert!(filtered.iter().all(|entry| entry.table == "a"));
        assert_eq!(filtered.first().map(|e| e.micros), Some(6004));
        assert_eq!(log.recent_filtered(2, Some("b")).len(), 2);
        assert!(log.recent_filtered(500, Some("missing")).is_empty());
        assert_eq!(log.recent_filtered(500, None).len(), 5);
    }
}

#[cfg(test)]
mod otlp_tests {
    use super::*;

    #[test]
    fn drain_removes_oldest_first() {
        let mut collector = TraceCollector::new(8);
        for index in 0..5 {
            collector.push(TraceSpan::root(format!("op-{index}"), 1000 + index as u64));
        }
        let taken = collector.drain(3);
        assert_eq!(taken.len(), 3);
        assert_eq!(taken[0].name, "op-0");
        assert_eq!(taken[2].name, "op-2");
        assert_eq!(collector.len(), 2);
        let rest = collector.drain(100);
        assert_eq!(rest.len(), 2);
        assert!(collector.is_empty());
    }

    #[test]
    fn otlp_shape_matches_protocol() {
        let mut root = TraceSpan::root(String::from("kv_get"), 1_700_000_000);
        root.attr(String::from("table"), String::from("docs"));
        root.finish(250);
        let child = root.child(String::from("storage"), 1_700_000_001);
        let payload = otlp_resource_spans("rymedb", &[root, child]);
        let spans = &payload["resourceSpans"][0]["scopeSpans"][0]["spans"];
        assert_eq!(spans.as_array().map(Vec::len), Some(2));
        let first = &spans[0];
        assert_eq!(first["name"], "kv_get");
        assert_eq!(first["kind"], 1);
        assert_eq!(first["startTimeUnixNano"], "1700000000000000000");
        assert_eq!(first["endTimeUnixNano"], "1700000000000250000");
        assert_eq!(first["traceId"].as_str().map(str::len), Some(32));
        assert_eq!(first["spanId"].as_str().map(str::len), Some(16));
        assert!(first.get("parentSpanId").is_none());
        assert_eq!(
            first["attributes"],
            serde_json::json!([{"key": "table", "value": {"stringValue": "docs"}}])
        );
        let second = &spans[1];
        assert_eq!(second["parentSpanId"].as_str().map(str::len), Some(16));
        assert_eq!(
            payload["resourceSpans"][0]["resource"]["attributes"],
            serde_json::json!([{"key": "service.name", "value": {"stringValue": "rymedb"}}])
        );
        assert_eq!(payload["resourceSpans"][0]["scopeSpans"][0]["scope"]["name"], "rymedb");
    }

    #[test]
    fn otlp_empty_spans_still_valid() {
        let payload = otlp_resource_spans("rymedb", &[]);
        assert_eq!(
            payload["resourceSpans"][0]["scopeSpans"][0]["spans"].as_array().map(Vec::len),
            Some(0)
        );
    }
}

#[cfg(test)]
mod traceparent_tests {
    use super::*;

    #[test]
    fn valid_header_links() {
        let linked = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4730-00f067aa0ba902b7-01");
        assert_eq!(
            linked,
            Some((
                String::from("4bf92f3577b34da6a3ce929d0e0e4730"),
                String::from("00f067aa0ba902b7")
            ))
        );
    }

    #[test]
    fn malformed_headers_rejected() {
        assert_eq!(parse_traceparent(""), None);
        assert_eq!(parse_traceparent("00-abc-00f067aa0ba902b7-01"), None);
        assert_eq!(
            parse_traceparent("ff-4bf92f3577b34da6a3ce929d0e0e4730-00f067aa0ba902b7-01"),
            None
        );
        assert_eq!(
            parse_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01"),
            None
        );
        assert_eq!(
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4730-0000000000000000-01"),
            None
        );
        assert_eq!(
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4730-00f067aa0ba902b7-01-extra"),
            None
        );
        assert_eq!(
            parse_traceparent("00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-00f067aa0ba902b7-01"),
            None
        );
    }
}
