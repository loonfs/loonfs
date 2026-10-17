//! What the exposition format promises, held to the byte: one header pair
//! per name, cumulative buckets, and an output that does not move between
//! scrapes of the same readings.

use super::*;
use loonfs::metrics::{DefaultMetricsRecorder, MetricsRecorder, LATENCY_SECONDS_BOUNDARIES};

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

#[test]
fn rendering_the_same_readings_twice_produces_the_same_bytes() {
    let recorder = DefaultMetricsRecorder::new();
    for (name, labels) in [
        ("loonfs.maintenance.sweep_visit_failures", [("call", "gc")]),
        (
            "loonfs.maintenance.sweep_visit_failures",
            [("call", "metadata")],
        ),
        ("loonfs.gc.retained", [("category", "all")]),
    ] {
        recorder
            .register_counter(name, "Failures", &labels)
            .increment(1);
    }
    recorder
        .register_histogram(
            "loonfs.maintenance.compaction_seconds",
            "Compaction duration",
            &[("outcome", "published")],
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
    render_scrape_gauges(
        &mut rendered,
        None,
        Some(GrepStepBudgetStats {
            running: 1,
            waiting: 3,
        }),
        4,
        2,
        7,
    );

    assert!(rendered.contains("loonfs_grep_steps_running 1\n"));
    assert!(rendered.contains("loonfs_grep_steps_waiting 3\n"));
    assert!(rendered.contains("loonfs_server_upload_permits_available 4\n"));
    assert!(rendered.contains("loonfs_server_download_permits_available 2\n"));
    assert!(rendered.contains("loonfs_server_in_flight_requests 7\n"));
    assert_eq!(
        rendered
            .lines()
            .filter(|line| line.starts_with("# TYPE"))
            .count(),
        if cfg!(target_os = "linux") { 6 } else { 5 },
        "the grep steps, transfer slots, request count, and Linux RSS where available"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_scrape_reports_positive_process_resident_bytes() {
    let mut rendered = String::new();
    render_scrape_gauges(&mut rendered, None, None, 0, 0, 0);

    let resident_bytes = rendered
        .lines()
        .find_map(|line| {
            line.strip_prefix("loonfs_process_resident_bytes ")
                .and_then(|value| value.parse::<u64>().ok())
        })
        .expect("a Linux scrape should render process RSS");
    assert!(resident_bytes > 0);
}

/// Checks that each field of the cache stats renders into its own series,
/// under the exported name and labels that operators scrape.
#[test]
fn each_cache_stats_field_renders_into_its_own_series() {
    let mut rendered = String::new();
    render_metadata_cache(
        &mut rendered,
        &MetadataCacheStats {
            segment_hits: 1,
            segment_misses: 2,
            segment_inserts: 3,
            segment_evictions: 4,
            segment_filter_skips: 5,
            segment_filter_false_positives: 6,
            segment_bytes: 7,
            head_anchor_hits: 8,
            head_anchor_misses: 9,
            wal_tail_hits: 10,
            wal_tail_misses: 11,
            wal_tail_inserts: 12,
            head_state_evictions: 13,
            head_state_evicted_bytes: 14,
            head_state_rejections: 15,
            head_state_rejected_bytes: 16,
            head_state_bytes: 17,
            wal_object_hits: 18,
            wal_object_misses: 19,
            wal_object_inserts: 20,
            content_object_hits: 21,
            content_object_misses: 22,
            content_object_inserts: 23,
            object_evictions: 24,
            object_bytes: 25,
            ..MetadataCacheStats::default()
        },
    );
    render_grep_block_cache(
        &mut rendered,
        &DecodedBlockCacheStats {
            hits: 26,
            misses: 27,
            inserts: 28,
            evictions: 29,
            ..DecodedBlockCacheStats::default()
        },
    );

    let expected = [
        "loonfs_metadata_segment_cache_gets_total{result=\"hit\"} 1",
        "loonfs_metadata_segment_cache_gets_total{result=\"miss\"} 2",
        "loonfs_metadata_segment_cache_inserts_total 3",
        "loonfs_metadata_segment_cache_evictions_total 4",
        "loonfs_metadata_segment_cache_filter_skips_total 5",
        "loonfs_metadata_segment_cache_filter_false_positives_total 6",
        "loonfs_metadata_segment_cache_retained_decoded_bytes 7",
        "loonfs_namespace_head_cache_gets_total{result=\"hit\"} 8",
        "loonfs_namespace_head_cache_gets_total{result=\"miss\"} 9",
        "loonfs_wal_tail_projection_cache_gets_total{result=\"hit\"} 10",
        "loonfs_wal_tail_projection_cache_gets_total{result=\"miss\"} 11",
        "loonfs_wal_tail_projection_cache_inserts_total 12",
        "loonfs_head_state_cache_evictions_total 13",
        "loonfs_head_state_cache_evicted_decoded_bytes_total 14",
        "loonfs_head_state_cache_rejections_total 15",
        "loonfs_head_state_cache_rejected_decoded_bytes_total 16",
        "loonfs_head_state_cache_retained_decoded_bytes 17",
        "loonfs_object_cache_gets_total{key_class=\"wal_object\",result=\"hit\"} 18",
        "loonfs_object_cache_gets_total{key_class=\"wal_object\",result=\"miss\"} 19",
        "loonfs_object_cache_inserts_total{key_class=\"wal_object\"} 20",
        "loonfs_object_cache_gets_total{key_class=\"content\",result=\"hit\"} 21",
        "loonfs_object_cache_gets_total{key_class=\"content\",result=\"miss\"} 22",
        "loonfs_object_cache_inserts_total{key_class=\"content\"} 23",
        "loonfs_object_cache_evictions_total 24",
        "loonfs_object_cache_retained_decoded_bytes 25",
        "loonfs_grep_block_cache_gets_total{result=\"hit\"} 26",
        "loonfs_grep_block_cache_gets_total{result=\"miss\"} 27",
        "loonfs_grep_block_cache_inserts_total 28",
        "loonfs_grep_block_cache_evictions_total 29",
    ];
    for series in expected {
        assert!(
            rendered.lines().any(|line| line == series),
            "missing `{series}` in:\n{rendered}"
        );
    }
    assert_eq!(
        rendered
            .lines()
            .filter(|line| !line.starts_with('#'))
            .count(),
        expected.len(),
        "one series for each exported field"
    );
}
