//! What the exposition format promises, held to the byte: one header pair
//! per name, cumulative buckets, and an output that does not move between
//! scrapes of the same readings.

#![allow(clippy::panic)]
// A wrong reading kind in a fixture is a bug in the test.

use super::*;
use loonfs::metrics::LATENCY_SECONDS_BOUNDARIES;

#[test]
fn a_counter_renders_with_the_total_suffix_and_its_labels() {
    let recorder = DefaultMetricsRecorder::new();
    recorder
        .register_counter(
            "loonfs.object_store.operations",
            "Object-store calls by operation and outcome",
            &[("operation", "put"), ("result", "ok")],
        )
        .increment(3);

    assert_eq!(
        render_snapshot(&recorder.snapshot()),
        "# HELP loonfs_object_store_operations_total Object-store calls by operation and outcome\n\
         # TYPE loonfs_object_store_operations_total counter\n\
         loonfs_object_store_operations_total{operation=\"put\",result=\"ok\"} 3\n"
    );
}

#[test]
fn a_gauge_renders_without_a_suffix() {
    let recorder = DefaultMetricsRecorder::new();
    recorder
        .register_gauge("loonfs.publisher.queue_depth", "Queued candidates", &[])
        .set(7);

    assert_eq!(
        render_snapshot(&recorder.snapshot()),
        "# HELP loonfs_publisher_queue_depth Queued candidates\n\
         # TYPE loonfs_publisher_queue_depth gauge\n\
         loonfs_publisher_queue_depth 7\n"
    );
}

/// Prometheus buckets are cumulative and must end at `+Inf`; the runtime's
/// are per-bucket, so this is where the running sum happens.
#[test]
fn a_histogram_renders_cumulative_buckets_ending_at_infinity() {
    const BOUNDARIES: &[f64] = &[1.0, 2.0];
    let recorder = DefaultMetricsRecorder::new();
    let histogram = recorder.register_histogram(
        "loonfs.server.request_seconds",
        "Time to serve one request",
        &[("route", "/health")],
        BOUNDARIES,
    );
    histogram.record(0.5);
    histogram.record(1.5);
    histogram.record(9.0);

    assert_eq!(
        render_snapshot(&recorder.snapshot()),
        "# HELP loonfs_server_request_seconds Time to serve one request\n\
         # TYPE loonfs_server_request_seconds histogram\n\
         loonfs_server_request_seconds_bucket{route=\"/health\",le=\"1.0\"} 1\n\
         loonfs_server_request_seconds_bucket{route=\"/health\",le=\"2.0\"} 2\n\
         loonfs_server_request_seconds_bucket{route=\"/health\",le=\"+Inf\"} 3\n\
         loonfs_server_request_seconds_sum{route=\"/health\"} 11.0\n\
         loonfs_server_request_seconds_count{route=\"/health\"} 3\n"
    );
}

/// One name, one header pair, however many label sets it carries.
#[test]
fn label_sets_of_one_name_share_a_single_header_pair() {
    let recorder = DefaultMetricsRecorder::new();
    for kind in ["upload", "download"] {
        recorder
            .register_counter(
                "loonfs.server.busy_rejections",
                "Requests refused at a concurrency limit",
                &[("kind", kind)],
            )
            .increment(1);
    }

    let rendered = render_snapshot(&recorder.snapshot());
    assert_eq!(rendered.matches("# HELP").count(), 1);
    assert_eq!(rendered.matches("# TYPE").count(), 1);
    assert_eq!(
        rendered
            .lines()
            .filter(|line| !line.starts_with('#'))
            .count(),
        2
    );
}

/// A scrape that reads the same numbers twice must produce the same bytes,
/// or a diffing operator sees changes that did not happen.
#[test]
fn rendering_the_same_readings_twice_produces_the_same_bytes() {
    let recorder = DefaultMetricsRecorder::new();
    for (name, labels) in [
        ("loonfs.maintenance.steps", [("job", "gc")]),
        ("loonfs.maintenance.steps", [("job", "metadata")]),
        ("loonfs.gc.retained", [("category", "all")]),
    ] {
        recorder
            .register_counter(name, "Steps", &labels)
            .increment(1);
    }
    recorder
        .register_histogram(
            "loonfs.maintenance.step_seconds",
            "Step duration",
            &[("job", "gc")],
            LATENCY_SECONDS_BOUNDARIES,
        )
        .record(0.02);

    let first = render_snapshot(&recorder.snapshot());
    assert_eq!(first, render_snapshot(&recorder.snapshot()));
    // Names ascend, so a reader scanning the output finds a metric where it
    // expects to.
    let names: Vec<&str> = first
        .lines()
        .filter(|line| line.starts_with("# TYPE"))
        .map(|line| line.split(' ').nth(2).expect("type line names its metric"))
        .collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted);
}

#[test]
fn scrape_time_gauges_carry_permit_levels() {
    let mut rendered = String::new();
    render_scrape_gauges(&mut rendered, None, 4, 2);

    assert!(rendered.contains("loonfs_server_upload_permits_available 4\n"));
    assert!(rendered.contains("loonfs_server_download_permits_available 2\n"));
    assert_eq!(
        rendered
            .lines()
            .filter(|line| line.starts_with("# TYPE"))
            .count(),
        if cfg!(target_os = "linux") { 3 } else { 2 },
        "the two permit pools and Linux RSS where available"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_scrape_reports_positive_process_resident_bytes() {
    let mut rendered = String::new();
    render_scrape_gauges(&mut rendered, None, 0, 0);

    let resident_bytes = rendered
        .lines()
        .find_map(|line| {
            line.strip_prefix("loonfs_process_resident_bytes ")
                .and_then(|value| value.parse::<u64>().ok())
        })
        .expect("a Linux scrape should render process RSS");
    assert!(resident_bytes > 0);
}

/// The whole reason the label is the matched template: a route seen twice is
/// one label, and a path that matched nothing never becomes one.
#[test]
fn route_labels_intern_once_and_refuse_to_grow_without_bound() {
    let mut routes = RouteLabels::default();
    let first = routes.intern("/v0/namespaces/{namespace}/commits");
    let again = routes.intern("/v0/namespaces/{namespace}/commits");
    assert_eq!(first, again);
    assert_eq!(first.as_ptr(), again.as_ptr());

    for index in 0..MAX_ROUTE_LABELS {
        routes.intern(&format!("/synthetic/{index}"));
    }
    assert_eq!(routes.intern("/one/too/many"), UNMATCHED_ROUTE);
}

#[test]
fn status_classes_collapse_to_their_leading_digit() {
    assert_eq!(status_class_label(StatusCode::OK), "2xx");
    assert_eq!(status_class_label(StatusCode::UNAUTHORIZED), "4xx");
    assert_eq!(status_class_label(StatusCode::SERVICE_UNAVAILABLE), "5xx");
}
