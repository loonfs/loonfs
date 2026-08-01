//! The server's own metrics: the instruments it reports as LoonFS's first
//! embedder, and the Prometheus text its `/metrics` route serves.
//!
//! The runtime defines instruments and reports values; exporting them is the
//! embedder's job (`loonfs::metrics`). This is that job, done once: the
//! server installs a [`DefaultMetricsRecorder`] on its handles, adds the
//! request-level instruments only an HTTP server can report, and renders the
//! whole snapshot on demand.

use axum::http::{Method, StatusCode};
use loonfs::metrics::{
    CounterHandle, DefaultMetricsRecorder, HistogramHandle, MetricEntry, MetricValue,
    MetricsRecorder, MetricsSnapshot, LATENCY_SECONDS_BOUNDARIES,
};
use loonfs::RuntimeCacheStats;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, MutexGuard};

/// The `route` label for a request that matched no route, and for one whose
/// template arrived after the intern table filled.
const UNMATCHED_ROUTE: &str = "unmatched";

/// Most distinct route templates the intern table will hold.
///
/// The router's route list is fixed at build time and well under this, so
/// the cap never binds in practice. It exists so the leak below is bounded
/// by a mechanism rather than by a promise about what axum hands us.
const MAX_ROUTE_LABELS: usize = 128;

/// Everything the server reports and everything it renders.
pub(super) struct ServerMetrics {
    recorder: Arc<DefaultMetricsRecorder>,
    routes: Mutex<RouteLabels>,
    requests: Mutex<HashMap<&'static str, RequestInstruments>>,
    busy_uploads: Arc<dyn CounterHandle>,
    busy_downloads: Arc<dyn CounterHandle>,
}

/// One route's request instruments, and the per-method-and-status counters
/// that route has served.
struct RequestInstruments {
    seconds: Arc<dyn HistogramHandle>,
    served: HashMap<(&'static str, &'static str), Arc<dyn CounterHandle>>,
}

impl ServerMetrics {
    /// Builds the server's recorder and registers what it can register up
    /// front: the two admission-rejection counters, whose labels are the
    /// closed pair of things this server refuses when it is full.
    pub(super) fn new() -> Arc<Self> {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let busy = |kind: &'static str| {
            recorder.register_counter(
                "loonfs.server.busy_rejections",
                "Requests refused at a concurrency limit",
                &[("kind", kind)],
            )
        };
        Arc::new(Self {
            busy_uploads: busy("upload"),
            busy_downloads: busy("download"),
            routes: Mutex::new(RouteLabels::default()),
            requests: Mutex::new(HashMap::new()),
            recorder,
        })
    }

    /// The recorder the runtime handles report through.
    pub(super) fn recorder(&self) -> Arc<dyn MetricsRecorder> {
        Arc::clone(&self.recorder) as Arc<dyn MetricsRecorder>
    }

    /// Reports one served request.
    ///
    /// `matched_route` is the template axum matched, not the request's path:
    /// the path carries namespace and upload ids, and one unbounded label is
    /// how a metrics backend dies.
    pub(super) fn request_served(
        &self,
        matched_route: Option<&str>,
        method: &Method,
        status: StatusCode,
        elapsed_seconds: f64,
    ) {
        let route = self.route_label(matched_route);
        let (seconds, served) = {
            let mut requests = lock(&self.requests);
            let instruments = requests
                .entry(route)
                .or_insert_with(|| RequestInstruments::register(self.recorder.as_ref(), route));
            instruments.served(
                self.recorder.as_ref(),
                route,
                method_label(method),
                status_class_label(status),
            )
        };
        served.increment(1);
        seconds.record(elapsed_seconds);
    }

    /// Reports one proxied upload refused for want of a transfer slot.
    pub(super) fn upload_rejected_as_busy(&self) {
        self.busy_uploads.increment(1);
    }

    /// Reports one proxied content read refused for want of a transfer slot.
    pub(super) fn download_rejected_as_busy(&self) {
        self.busy_downloads.increment(1);
    }

    /// Renders everything this process has reported, plus the gauges that
    /// are read at scrape time rather than accumulated.
    pub(super) fn render(
        &self,
        cache: &RuntimeCacheStats,
        upload_permits: usize,
        download_permits: usize,
    ) -> String {
        let mut rendered = render_snapshot(&self.recorder.snapshot());
        render_scrape_gauges(&mut rendered, cache, upload_permits, download_permits);
        rendered
    }

    fn route_label(&self, matched_route: Option<&str>) -> &'static str {
        match matched_route {
            Some(route) => lock(&self.routes).intern(route),
            None => UNMATCHED_ROUTE,
        }
    }
}

impl RequestInstruments {
    fn register(recorder: &dyn MetricsRecorder, route: &'static str) -> Self {
        Self {
            seconds: recorder.register_histogram(
                "loonfs.server.request_seconds",
                "Time to serve one request, by matched route",
                &[("route", route)],
                LATENCY_SECONDS_BOUNDARIES,
            ),
            served: HashMap::new(),
        }
    }

    fn served(
        &mut self,
        recorder: &dyn MetricsRecorder,
        route: &'static str,
        method: &'static str,
        status_class: &'static str,
    ) -> (Arc<dyn HistogramHandle>, Arc<dyn CounterHandle>) {
        let counter = self
            .served
            .entry((method, status_class))
            .or_insert_with(|| {
                recorder.register_counter(
                    "loonfs.server.requests",
                    "Requests served, by matched route, method, and status class",
                    &[
                        ("route", route),
                        ("method", method),
                        ("status_class", status_class),
                    ],
                )
            })
            .clone();
        (Arc::clone(&self.seconds), counter)
    }
}

/// The `&'static str` route labels this process has seen.
///
/// Route templates arrive borrowed from the router, and a label has to be
/// `'static` — that is the type-level cardinality rule the runtime's
/// recorder enforces. Interning leaks each distinct template exactly once,
/// which is bounded because the router's route list is fixed at build time
/// and capped besides.
#[derive(Default)]
struct RouteLabels {
    interned: HashMap<String, &'static str>,
}

impl RouteLabels {
    fn intern(&mut self, route: &str) -> &'static str {
        if let Some(label) = self.interned.get(route) {
            return label;
        }
        if self.interned.len() >= MAX_ROUTE_LABELS {
            return UNMATCHED_ROUTE;
        }
        let label: &'static str = Box::leak(route.to_owned().into_boxed_str());
        self.interned.insert(route.to_owned(), label);
        label
    }
}

/// The `method` label for a served request.
///
/// `Method::as_str` borrows from the method, and a label must be `'static`,
/// so the standard methods are mapped through this closed match instead —
/// which also keeps an extension method from becoming a label of its own.
fn method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::HEAD => "HEAD",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::OPTIONS => "OPTIONS",
        Method::PATCH => "PATCH",
        _ => "other",
    }
}

/// The `status_class` label for a served request: the status code's leading
/// digit, which is the part an operator alerts on.
fn status_class_label(status: StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

/// Renders a snapshot as Prometheus text exposition format 0.0.4.
///
/// Dotted names become underscored ones, counters take the `_total` suffix
/// the format expects, and histograms emit cumulative buckets with the
/// `+Inf` bucket the format requires. Entries arrive ordered by name and
/// then labels, so one `# HELP`/`# TYPE` pair per name is enough and the
/// output is byte-identical for identical readings.
fn render_snapshot(snapshot: &MetricsSnapshot) -> String {
    let mut rendered = String::new();
    let mut current_name = None;
    for entry in snapshot.all() {
        let name = prometheus_name(entry);
        if current_name.as_deref() != Some(name.as_str()) {
            write_metric_header(&mut rendered, &name, entry);
            current_name = Some(name.clone());
        }
        write_entry(&mut rendered, &name, entry);
    }
    rendered
}

fn write_metric_header(rendered: &mut String, name: &str, entry: &MetricEntry) {
    let kind = match entry.value {
        MetricValue::Counter(_) => "counter",
        MetricValue::Gauge(_) => "gauge",
        MetricValue::Histogram { .. } => "histogram",
    };
    let _ = writeln!(rendered, "# HELP {name} {}", escape_help(entry.description));
    let _ = writeln!(rendered, "# TYPE {name} {kind}");
}

fn write_entry(rendered: &mut String, name: &str, entry: &MetricEntry) {
    match &entry.value {
        MetricValue::Counter(value) => {
            let _ = writeln!(rendered, "{name}{} {value}", labels(&entry.labels, None));
        }
        MetricValue::Gauge(value) => {
            let _ = writeln!(rendered, "{name}{} {value}", labels(&entry.labels, None));
        }
        MetricValue::Histogram {
            boundaries,
            bucket_counts,
            count,
            sum,
        } => {
            let mut cumulative = 0u64;
            for (boundary, filed) in boundaries.iter().zip(bucket_counts.iter()) {
                cumulative += filed;
                let _ = writeln!(
                    rendered,
                    "{name}_bucket{} {cumulative}",
                    labels(&entry.labels, Some(&format_float(*boundary)))
                );
            }
            let _ = writeln!(
                rendered,
                "{name}_bucket{} {count}",
                labels(&entry.labels, Some("+Inf"))
            );
            let _ = writeln!(
                rendered,
                "{name}_sum{} {}",
                labels(&entry.labels, None),
                format_float(*sum)
            );
            let _ = writeln!(
                rendered,
                "{name}_count{} {count}",
                labels(&entry.labels, None)
            );
        }
    }
}

/// Appends the values that are read when a scrape asks rather than
/// accumulated as work happens: the runtime's cache counters and the
/// transfer slots this server has free.
fn render_scrape_gauges(
    rendered: &mut String,
    cache: &RuntimeCacheStats,
    upload_permits: usize,
    download_permits: usize,
) {
    for (field, value) in cache_gauges(cache) {
        let name = format!("loonfs_cache_{field}");
        let _ = writeln!(rendered, "# HELP {name} Runtime cache counter `{field}`");
        let _ = writeln!(rendered, "# TYPE {name} gauge");
        let _ = writeln!(rendered, "{name} {value}");
    }
    for (name, description, value) in [
        (
            "loonfs_server_upload_permits_available",
            "Proxied-upload slots free right now",
            upload_permits,
        ),
        (
            "loonfs_server_download_permits_available",
            "Proxied-content-read slots free right now",
            download_permits,
        ),
    ] {
        let _ = writeln!(rendered, "# HELP {name} {description}");
        let _ = writeln!(rendered, "# TYPE {name} gauge");
        let _ = writeln!(rendered, "{name} {value}");
    }
}

/// The runtime cache counters, named as their fields are.
///
/// Destructured without a rest pattern on purpose: a counter added to
/// [`RuntimeCacheStats`] and not exported here fails to compile.
fn cache_gauges(stats: &RuntimeCacheStats) -> [(&'static str, usize); 18] {
    let RuntimeCacheStats {
        latest_metadata_view_reads,
        wal_tail_projection_cache_hits,
        wal_tail_projection_cache_misses,
        wal_tail_projection_cache_inserts,
        wal_tail_projection_cache_evictions,
        wal_tail_projection_cache_evicted_rows,
        wal_tail_projection_cache_evicted_decoded_bytes,
        wal_tail_projection_cache_uncacheable_count,
        wal_tail_projection_cache_uncacheable_rows,
        wal_tail_projection_cache_uncacheable_decoded_bytes,
        wal_tail_projection_cache_cached_rows,
        wal_tail_projection_cache_cached_decoded_bytes,
        metadata_table_cache_hits,
        metadata_table_cache_misses,
        metadata_table_cache_inserts,
        metadata_table_cache_evictions,
        metadata_table_cache_filter_skips,
        metadata_table_cache_filter_false_positives,
    } = *stats;
    [
        ("latest_metadata_view_reads", latest_metadata_view_reads),
        (
            "wal_tail_projection_cache_hits",
            wal_tail_projection_cache_hits,
        ),
        (
            "wal_tail_projection_cache_misses",
            wal_tail_projection_cache_misses,
        ),
        (
            "wal_tail_projection_cache_inserts",
            wal_tail_projection_cache_inserts,
        ),
        (
            "wal_tail_projection_cache_evictions",
            wal_tail_projection_cache_evictions,
        ),
        (
            "wal_tail_projection_cache_evicted_rows",
            wal_tail_projection_cache_evicted_rows,
        ),
        (
            "wal_tail_projection_cache_evicted_decoded_bytes",
            wal_tail_projection_cache_evicted_decoded_bytes,
        ),
        (
            "wal_tail_projection_cache_uncacheable_count",
            wal_tail_projection_cache_uncacheable_count,
        ),
        (
            "wal_tail_projection_cache_uncacheable_rows",
            wal_tail_projection_cache_uncacheable_rows,
        ),
        (
            "wal_tail_projection_cache_uncacheable_decoded_bytes",
            wal_tail_projection_cache_uncacheable_decoded_bytes,
        ),
        (
            "wal_tail_projection_cache_cached_rows",
            wal_tail_projection_cache_cached_rows,
        ),
        (
            "wal_tail_projection_cache_cached_decoded_bytes",
            wal_tail_projection_cache_cached_decoded_bytes,
        ),
        ("metadata_table_cache_hits", metadata_table_cache_hits),
        ("metadata_table_cache_misses", metadata_table_cache_misses),
        ("metadata_table_cache_inserts", metadata_table_cache_inserts),
        (
            "metadata_table_cache_evictions",
            metadata_table_cache_evictions,
        ),
        (
            "metadata_table_cache_filter_skips",
            metadata_table_cache_filter_skips,
        ),
        (
            "metadata_table_cache_filter_false_positives",
            metadata_table_cache_filter_false_positives,
        ),
    ]
}

/// The exported name: dots become underscores, and a counter takes the
/// `_total` suffix Prometheus expects of one.
fn prometheus_name(entry: &MetricEntry) -> String {
    let mut name = entry.name.replace('.', "_");
    if matches!(entry.value, MetricValue::Counter(_)) {
        name.push_str("_total");
    }
    name
}

fn labels(labels: &[(&'static str, &'static str)], le: Option<&str>) -> String {
    if labels.is_empty() && le.is_none() {
        return String::new();
    }
    let mut rendered = String::from("{");
    for (index, (key, value)) in labels.iter().enumerate() {
        if index > 0 {
            rendered.push(',');
        }
        let _ = write!(rendered, "{key}=\"{}\"", escape_label(value));
    }
    if let Some(le) = le {
        if !labels.is_empty() {
            rendered.push(',');
        }
        let _ = write!(rendered, "le=\"{le}\"");
    }
    rendered.push('}');
    rendered
}

/// Renders a float the way the exposition format wants it: an integral
/// value keeps a trailing `.0` so a boundary and a bucket label never
/// disagree between scrapes.
fn format_float(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.1}")
    } else {
        format!("{value}")
    }
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn escape_help(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\n', "\\n")
}

// A poisoned instrument table is recovered rather than propagated:
// reporting a metric must never be the thing that takes the server down.
fn lock<T>(table: &Mutex<T>) -> MutexGuard<'_, T> {
    table
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
