//! Shared runtime and HTTP request metrics recorder.

use axum::http::{Method, StatusCode};
use loonfs::metrics::{
    CounterHandle, DefaultMetricsRecorder, HistogramHandle, MetricsRecorder, MetricsSnapshot,
    LATENCY_SECONDS_BOUNDARIES,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The `route` label for a request that matched no route, and for one whose
/// template arrived after the intern table filled.
const UNMATCHED_ROUTE: &str = "unmatched";

/// Most distinct route templates the intern table will hold.
///
/// The router's route list is fixed at build time and well under this, so
/// the cap never binds in practice. It exists so the leak below is bounded
/// by a mechanism rather than by a promise about what axum hands us.
const MAX_ROUTE_LABELS: usize = 128;

pub struct HttpMetrics {
    recorder: Arc<DefaultMetricsRecorder>,
    routes: Mutex<RouteLabels>,
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
            routes: Mutex::new(RouteLabels::default()),
            requests: Mutex::new(HashMap::new()),
            recorder,
        })
    }

    /// The recorder the runtime reports through.
    pub fn recorder(&self) -> Arc<dyn MetricsRecorder> {
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
    ) -> &'static str {
        let route = self.route_label(matched_route);
        let (seconds, served) =
            self.request_handles(route, method_label(method), status_class_label(status));
        served.increment(1);
        seconds.record(elapsed_seconds);
        route
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

    fn route_label(&self, matched_route: Option<&str>) -> &'static str {
        match matched_route {
            Some(route) => self
                .routes
                .lock()
                .expect("the route metrics lock should not be poisoned")
                .intern(route),
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

#[cfg(test)]
mod tests;
