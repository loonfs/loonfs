//! Shared runtime and HTTP request metrics recorder.

use super::router::find_route_path;
use axum::http::{Method, StatusCode};
use loonfs::metrics::{
    CounterHandle, DefaultMetricsRecorder, HistogramHandle, MetricsRecorder, MetricsSnapshot,
    LATENCY_SECONDS_BOUNDARIES,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The `route` label for a request that matched no route.
pub(super) const UNMATCHED_ROUTE: &str = "unmatched";

/// The `route` label for a request to a route that the host added beside
/// the contract routes, such as `/health`.
const OTHER_ROUTE: &str = "other";

/// Returns the `route` label of a request whose matched route template is
/// `matched_route`.
///
/// A label is the template, not the request's path: the path includes
/// namespace and upload IDs, and an ID in a label creates a series for
/// each ID. A label also has to be `'static`, so it comes from the contract
/// route table.
pub(super) fn route_label(matched_route: Option<&str>) -> &'static str {
    match matched_route {
        Some(route) => find_route_path(route).unwrap_or(OTHER_ROUTE),
        None => UNMATCHED_ROUTE,
    }
}

pub struct HttpMetrics {
    recorder: Arc<DefaultMetricsRecorder>,
    requests: Mutex<HashMap<&'static str, RequestInstruments>>,
    busy_requests: Arc<dyn CounterHandle>,
    busy_uploads: Arc<dyn CounterHandle>,
    busy_downloads: Arc<dyn CounterHandle>,
}

/// One route's request instruments, and the per-method-and-status counters
/// that route has served.
struct RequestInstruments {
    seconds: Arc<dyn HistogramHandle>,
    served: HashMap<(&'static str, &'static str), Arc<dyn CounterHandle>>,
}

impl HttpMetrics {
    pub fn new() -> Arc<Self> {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let busy = |kind: &'static str| {
            recorder.register_counter(
                "loonfs.server.busy_rejections",
                "Requests refused at a concurrency limit",
                &[("kind", kind)],
            )
        };
        Arc::new(Self {
            busy_requests: busy("request"),
            busy_uploads: busy("upload"),
            busy_downloads: busy("download"),
            requests: Mutex::new(HashMap::new()),
            recorder,
        })
    }

    /// The recorder the runtime reports through.
    pub fn recorder(&self) -> Arc<dyn MetricsRecorder> {
        Arc::clone(&self.recorder) as Arc<dyn MetricsRecorder>
    }

    /// Reports one served request, under its [`route_label`].
    pub(super) fn request_served(
        &self,
        route: &'static str,
        method: &Method,
        status: StatusCode,
        elapsed_seconds: f64,
    ) {
        let (seconds, served) =
            self.request_handles(route, method_label(method), status_class_label(status));
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

    pub(super) fn request_rejected_as_busy(&self) {
        self.busy_requests.increment(1);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        self.recorder.snapshot()
    }

    /// Returns the handles one served request reports through. A missing
    /// handle is registered before the request-metrics lock is taken, so the
    /// recorder never runs under it. When two first requests race, the
    /// handle stored first is kept.
    fn request_handles(
        &self,
        route: &'static str,
        method: &'static str,
        status_class: &'static str,
    ) -> (Arc<dyn HistogramHandle>, Arc<dyn CounterHandle>) {
        let route_registered = match self.lock_requests().get(route) {
            Some(instruments) => match instruments.served(method, status_class) {
                Some(handles) => return handles,
                None => true,
            },
            None => false,
        };
        let recorder = self.recorder.as_ref();
        let registered = (!route_registered).then(|| RequestInstruments::register(recorder, route));
        let counter = RequestInstruments::register_served(recorder, route, method, status_class);
        let mut requests = self.lock_requests();
        let instruments = requests.entry(route).or_insert_with(|| {
            registered.expect("a route found unregistered should be registered here")
        });
        instruments
            .served
            .entry((method, status_class))
            .or_insert(counter);
        instruments
            .served(method, status_class)
            .expect("the served counter should be stored by now")
    }

    fn lock_requests(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<&'static str, RequestInstruments>> {
        self.requests
            .lock()
            .expect("the request metrics lock should not be poisoned")
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

    fn register_served(
        recorder: &dyn MetricsRecorder,
        route: &'static str,
        method: &'static str,
        status_class: &'static str,
    ) -> Arc<dyn CounterHandle> {
        recorder.register_counter(
            "loonfs.server.requests",
            "Requests served, by matched route, method, and status class",
            &[
                ("route", route),
                ("method", method),
                ("status_class", status_class),
            ],
        )
    }

    /// The handles for one method and status class, or `None` before that
    /// pair's counter is stored.
    fn served(
        &self,
        method: &'static str,
        status_class: &'static str,
    ) -> Option<(Arc<dyn HistogramHandle>, Arc<dyn CounterHandle>)> {
        let counter = self.served.get(&(method, status_class))?;
        Some((Arc::clone(&self.seconds), Arc::clone(counter)))
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

#[cfg(test)]
mod tests;
