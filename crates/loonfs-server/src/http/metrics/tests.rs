//! What the exposition format promises, held to the byte: one header pair
//! per name, cumulative buckets, and an output that does not move between
//! scrapes of the same readings.

use super::*;
use loonfs::engine::DecodedBlockCacheStats;
use loonfs::metrics::{DefaultMetricsRecorder, MetricsRecorder, LATENCY_SECONDS_BOUNDARIES};
use loonfs::{MetadataCacheStats, SegmentFilterStats, SsdSegmentBlocksStats};

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
        Some(GrepPassBudgetStats {
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
    render_scrape_gauges(&mut rendered, None, 0, 0, 0);

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
/// under the exported name and labels that operators scrape, and that each
/// tier of the segment cache reports under its `tier` label.
#[test]
fn each_cache_stats_field_renders_into_its_own_series() {
    // Each cache's counters start at a different hundred, so a series that
    // renders another cache's field shows up as a mismatch.
    let cache = |base: usize| DecodedBlockCacheStats {
        hits: base + 1,
        misses: base + 2,
        inserts: base + 3,
        evictions: base + 4,
        evicted_decoded_bytes: base + 5,
        rejections: base + 6,
        rejected_decoded_bytes: base + 7,
        cached_decoded_bytes: base + 8,
    };
    let mut rendered = String::new();
    render_metadata_cache(
        &mut rendered,
        &MetadataCacheStats {
            segment_blocks: cache(0),
            segment_filter: SegmentFilterStats {
                skips: 9,
                false_positives: 10,
            },
            segment_ssd: Some(SsdSegmentBlocksStats {
                hits: 11,
                misses: 12,
                inserts: 13,
                evictions: 14,
                rejections: 15,
                stored_bytes: 16,
            }),
            manifests: cache(100),
            heads: cache(200),
            wal_tails: cache(300),
            content_objects: cache(400),
            completed_upload_sessions: cache(500),
        },
    );
    render_grep_block_cache(&mut rendered, &cache(600));

    let mut expected = vec![
        "loonfs_segment_cache_gets_total{tier=\"memory\",result=\"hit\"} 1".to_owned(),
        "loonfs_segment_cache_gets_total{tier=\"memory\",result=\"miss\"} 2".to_owned(),
        "loonfs_segment_cache_inserts_total{tier=\"memory\"} 3".to_owned(),
        "loonfs_segment_cache_evictions_total{tier=\"memory\"} 4".to_owned(),
        "loonfs_segment_cache_rejections_total{tier=\"memory\"} 6".to_owned(),
        "loonfs_segment_cache_retained_decoded_bytes 8".to_owned(),
        "loonfs_segment_cache_gets_total{tier=\"ssd\",result=\"hit\"} 11".to_owned(),
        "loonfs_segment_cache_gets_total{tier=\"ssd\",result=\"miss\"} 12".to_owned(),
        "loonfs_segment_cache_inserts_total{tier=\"ssd\"} 13".to_owned(),
        "loonfs_segment_cache_evictions_total{tier=\"ssd\"} 14".to_owned(),
        "loonfs_segment_cache_rejections_total{tier=\"ssd\"} 15".to_owned(),
        "loonfs_segment_cache_retained_stored_bytes 16".to_owned(),
        "loonfs_segment_cache_filter_skips_total 9".to_owned(),
        "loonfs_segment_cache_filter_false_positives_total 10".to_owned(),
    ];
    for (name, base) in [
        ("manifest", 100),
        ("head", 200),
        ("wal_tail", 300),
        ("content_object", 400),
        ("grep", 600),
    ] {
        expected.extend([
            format!(
                "loonfs_{name}_cache_gets_total{{result=\"hit\"}} {}",
                base + 1
            ),
            format!(
                "loonfs_{name}_cache_gets_total{{result=\"miss\"}} {}",
                base + 2
            ),
            format!("loonfs_{name}_cache_inserts_total {}", base + 3),
            format!("loonfs_{name}_cache_evictions_total {}", base + 4),
            format!(
                "loonfs_{name}_cache_evicted_decoded_bytes_total {}",
                base + 5
            ),
            format!("loonfs_{name}_cache_rejections_total {}", base + 6),
            format!(
                "loonfs_{name}_cache_rejected_decoded_bytes_total {}",
                base + 7
            ),
            format!("loonfs_{name}_cache_retained_decoded_bytes {}", base + 8),
        ]);
    }
    for series in &expected {
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

/// Checks that a cache without an SSD tier renders no `tier="ssd"` series
/// and no stored-bytes gauge, so a dashboard doesn't read an idle tier.
#[test]
fn a_cache_without_an_ssd_tier_renders_only_the_memory_tier() {
    let mut rendered = String::new();
    render_metadata_cache(&mut rendered, &MetadataCacheStats::default());
    assert!(rendered.contains("loonfs_segment_cache_gets_total{tier=\"memory\",result=\"hit\"} 0"));
    assert!(!rendered.contains("tier=\"ssd\""), "{rendered}");
    assert!(!rendered.contains("loonfs_segment_cache_retained_stored_bytes"));
}
