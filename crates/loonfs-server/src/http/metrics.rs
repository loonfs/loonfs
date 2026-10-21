//! Prometheus rendering for the server's operational metrics route.

use super::AppState;
use crate::local_cache::FoyerCacheStats;
use loonfs::engine::DecodedBlockCacheStats;
use loonfs::metrics::{
    KeyClass, MetricEntry, MetricValue, MetricsSnapshot, RESULT_HIT, RESULT_MISS,
};
use loonfs::MetadataCacheStats;
use loonfs_grep::GrepStepBudgetStats;
use loonfs_http::RequestLimit;
use std::fmt::Write as _;

/// Renders the recorder's snapshot, then the values that a scrape reads from
/// the server's state, such as the caches' stats and the free transfer slots.
pub(super) fn render(state: &AppState) -> String {
    let binding = &state.binding;
    let mut rendered = render_snapshot(&binding.metrics.snapshot());
    render_metadata_cache(&mut rendered, &state.runtime.metadata_cache().stats());
    // A server that doesn't serve grep has no grep block cache. Reporting
    // zeros would incorrectly suggest that the cache is idle.
    if let Some(grep_block_cache) = &state.grep_block_cache {
        render_grep_block_cache(&mut rendered, &grep_block_cache.stats());
    }
    render_scrape_gauges(
        &mut rendered,
        state.local_cache.as_ref().map(|cache| cache.foyer_stats()),
        binding
            .grep
            .worker()
            .map(|worker| worker.step_budget().stats()),
        binding.upload_permits.available_permits(),
        binding.download_permits.available_permits(),
        binding
            .request_limit
            .as_ref()
            .map_or(0, RequestLimit::in_flight),
    );
    rendered
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

/// Appends current gauge values sampled during a scrape: local cache state,
/// grep steps, Linux process RSS, and available transfer slots.
fn render_scrape_gauges(
    rendered: &mut String,
    local_cache: Option<FoyerCacheStats>,
    grep_steps: Option<GrepStepBudgetStats>,
    upload_permits: usize,
    download_permits: usize,
    in_flight_requests: usize,
) {
    // Omit local-cache metrics when this server has no local cache. Reporting
    // zeros would incorrectly suggest that an enabled cache is idle.
    if let Some(local_cache) = local_cache {
        for (name, description, value) in local_cache_gauges(&local_cache) {
            write_gauge(rendered, name, description, value);
        }
    }
    if let Some(GrepStepBudgetStats { running, waiting }) = grep_steps {
        for (name, description, value) in [
            (
                "loonfs_grep_steps_running",
                "Grep build and reorganize steps holding a step permit right now",
                running,
            ),
            (
                "loonfs_grep_steps_waiting",
                "Grep build and reorganize steps waiting for a step permit right now",
                waiting,
            ),
        ] {
            write_gauge(rendered, name, description, value);
        }
    }
    if let Some(resident_bytes) = process_resident_bytes() {
        write_gauge(
            rendered,
            "loonfs_process_resident_bytes",
            "Resident set size of the server process, sampled at scrape",
            resident_bytes,
        );
    }
    for (name, description, value) in [
        (
            "loonfs_server_in_flight_requests",
            "Requests holding a request permit through response completion",
            in_flight_requests,
        ),
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
        write_gauge(rendered, name, description, value);
    }
}

fn write_gauge(
    rendered: &mut String,
    name: &str,
    description: &str,
    value: impl std::fmt::Display,
) {
    let _ = writeln!(rendered, "# HELP {name} {description}");
    let _ = writeln!(rendered, "# TYPE {name} gauge");
    let _ = writeln!(rendered, "{name} {value}");
}

/// The label pairs of one series of a counter.
type SeriesLabels<'a> = &'a [(&'static str, &'static str)];

/// The labels of a series that has none.
const NO_LABELS: SeriesLabels<'static> = &[];

/// Writes a counter with one series for each label set. The written name
/// ends in the `_total` suffix that Prometheus expects of a counter.
fn write_counter(
    rendered: &mut String,
    name: &str,
    description: &str,
    series: &[(SeriesLabels<'_>, usize)],
) {
    let _ = writeln!(rendered, "# HELP {name}_total {description}");
    let _ = writeln!(rendered, "# TYPE {name}_total counter");
    for (series_labels, value) in series {
        let _ = writeln!(
            rendered,
            "{name}_total{} {value}",
            labels(series_labels, None)
        );
    }
}

/// Returns the `hit` and `miss` series of a lookup counter.
fn lookups(hits: usize, misses: usize) -> [(SeriesLabels<'static>, usize); 2] {
    [
        (&[("result", RESULT_HIT)], hits),
        (&[("result", RESULT_MISS)], misses),
    ]
}

/// Writes the metadata cache's stats as counters and gauges.
///
/// The stats are the one source of these values, and the runtime's recorder
/// doesn't register them. Listing every field makes this code fail to
/// compile when `MetadataCacheStats` gains a field, which ensures new
/// statistics are considered for export. The fields bound to `_` aren't
/// exported.
fn render_metadata_cache(rendered: &mut String, stats: &MetadataCacheStats) {
    let MetadataCacheStats {
        segment_hits,
        segment_misses,
        segment_inserts,
        segment_evictions,
        segment_filter_skips,
        segment_filter_false_positives,
        segment_bytes,
        head_anchor_hits,
        head_anchor_misses,
        head_evictions,
        head_evicted_bytes,
        head_rejections,
        head_rejected_bytes,
        head_bytes,
        wal_tail_hits,
        wal_tail_misses,
        wal_tail_inserts,
        head_state_evictions,
        head_state_evicted_bytes,
        head_state_rejections,
        head_state_rejected_bytes,
        head_state_bytes,
        wal_object_hits,
        wal_object_misses,
        wal_object_inserts,
        content_object_hits,
        content_object_misses,
        content_object_inserts,
        object_evictions,
        object_bytes,
        completed_session_hits: _,
        completed_session_misses: _,
        completed_session_evictions: _,
        completed_session_bytes: _,
    } = *stats;
    write_counter(
        rendered,
        "loonfs_namespace_head_cache_gets",
        "Namespace head cache lookups, by outcome",
        &lookups(head_anchor_hits, head_anchor_misses),
    );
    for (name, description, value) in [
        (
            "loonfs_head_cache_evictions",
            "Namespace heads evicted to stay within the head budget",
            head_evictions,
        ),
        (
            "loonfs_head_cache_evicted_decoded_bytes",
            "Bytes charged to evicted namespace heads, including keys and per-entry bookkeeping",
            head_evicted_bytes,
        ),
        (
            "loonfs_head_cache_rejections",
            "Namespace heads heavier than the whole head budget",
            head_rejections,
        ),
        (
            "loonfs_head_cache_rejected_decoded_bytes",
            "Bytes charged to rejected namespace heads, including keys and per-entry bookkeeping",
            head_rejected_bytes,
        ),
    ] {
        write_counter(rendered, name, description, &[(NO_LABELS, value)]);
    }
    write_gauge(
        rendered,
        "loonfs_head_cache_retained_decoded_bytes",
        "Bytes currently charged to namespace heads, including keys and per-entry bookkeeping",
        head_bytes,
    );

    write_counter(
        rendered,
        "loonfs_metadata_segment_cache_gets",
        "Decoded metadata-segment cache lookups, by outcome",
        &lookups(segment_hits, segment_misses),
    );
    for (name, description, value) in [
        (
            "loonfs_metadata_segment_cache_inserts",
            "Blocks inserted into the decoded metadata-segment cache",
            segment_inserts,
        ),
        (
            "loonfs_metadata_segment_cache_evictions",
            "Blocks evicted from the decoded metadata-segment cache",
            segment_evictions,
        ),
        (
            "loonfs_metadata_segment_cache_filter_skips",
            "Segments skipped after their filter ruled out a lookup",
            segment_filter_skips,
        ),
        (
            "loonfs_metadata_segment_cache_filter_false_positives",
            "Filter admissions that matched no metadata rows",
            segment_filter_false_positives,
        ),
    ] {
        write_counter(rendered, name, description, &[(NO_LABELS, value)]);
    }
    write_gauge(
        rendered,
        "loonfs_metadata_segment_cache_retained_decoded_bytes",
        "Bytes currently charged to metadata-segment cache entries, including keys and per-entry bookkeeping",
        segment_bytes,
    );

    write_counter(
        rendered,
        "loonfs_wal_tail_projection_cache_gets",
        "WAL-tail projection cache lookups, by outcome",
        &lookups(wal_tail_hits, wal_tail_misses),
    );
    write_counter(
        rendered,
        "loonfs_wal_tail_projection_cache_inserts",
        "WAL-tail projections inserted into the cache",
        &[(NO_LABELS, wal_tail_inserts)],
    );

    for (name, description, value) in [
        (
            "loonfs_head_state_cache_evictions",
            "WAL-tail projections evicted to stay within the head-state budget",
            head_state_evictions,
        ),
        (
            "loonfs_head_state_cache_evicted_decoded_bytes",
            "Bytes charged to evicted WAL-tail projections, including keys and per-entry bookkeeping",
            head_state_evicted_bytes,
        ),
        (
            "loonfs_head_state_cache_rejections",
            "WAL-tail projections heavier than the whole head-state budget",
            head_state_rejections,
        ),
        (
            "loonfs_head_state_cache_rejected_decoded_bytes",
            "Bytes charged to rejected WAL-tail projections, including keys and per-entry bookkeeping",
            head_state_rejected_bytes,
        ),
    ] {
        write_counter(rendered, name, description, &[(NO_LABELS, value)]);
    }
    write_gauge(
        rendered,
        "loonfs_head_state_cache_retained_decoded_bytes",
        "Bytes currently charged to WAL-tail projections, including keys and per-entry bookkeeping",
        head_state_bytes,
    );

    // The `key_class` labels match the ones that the object-store metrics
    // use for the same objects.
    let (wal_object, content) = (KeyClass::WalObject.as_str(), KeyClass::Content.as_str());
    write_counter(
        rendered,
        "loonfs_object_cache_gets",
        "Object cache lookups, by key class and outcome",
        &[
            (
                &[("key_class", wal_object), ("result", RESULT_HIT)],
                wal_object_hits,
            ),
            (
                &[("key_class", wal_object), ("result", RESULT_MISS)],
                wal_object_misses,
            ),
            (
                &[("key_class", content), ("result", RESULT_HIT)],
                content_object_hits,
            ),
            (
                &[("key_class", content), ("result", RESULT_MISS)],
                content_object_misses,
            ),
        ],
    );
    write_counter(
        rendered,
        "loonfs_object_cache_inserts",
        "Objects inserted into the object cache, by key class",
        &[
            (&[("key_class", wal_object)], wal_object_inserts),
            (&[("key_class", content)], content_object_inserts),
        ],
    );
    write_counter(
        rendered,
        "loonfs_object_cache_evictions",
        "WAL objects and content objects evicted to stay within their budgets",
        &[(NO_LABELS, object_evictions)],
    );
    write_gauge(
        rendered,
        "loonfs_object_cache_retained_decoded_bytes",
        "Bytes currently charged to WAL objects and content objects, including keys and per-entry bookkeeping",
        object_bytes,
    );
}

/// Writes the grep block cache's stats as counters.
///
/// Listing every field makes this code fail to compile when
/// `DecodedBlockCacheStats` gains a field, which ensures new statistics are
/// considered for export. The fields bound to `_` aren't exported.
fn render_grep_block_cache(rendered: &mut String, stats: &DecodedBlockCacheStats) {
    let DecodedBlockCacheStats {
        hits,
        misses,
        inserts,
        evictions,
        evicted_decoded_bytes: _,
        rejections: _,
        rejected_decoded_bytes: _,
        cached_decoded_bytes: _,
    } = *stats;
    write_counter(
        rendered,
        "loonfs_grep_block_cache_gets",
        "Decoded grep block cache lookups, by outcome",
        &lookups(hits, misses),
    );
    write_counter(
        rendered,
        "loonfs_grep_block_cache_inserts",
        "Blocks inserted into the decoded grep block cache",
        &[(NO_LABELS, inserts)],
    );
    write_counter(
        rendered,
        "loonfs_grep_block_cache_evictions",
        "Blocks evicted from the decoded grep block cache",
        &[(NO_LABELS, evictions)],
    );
}

/// Reads the Linux-only resident-page count for this process.
#[cfg(target_os = "linux")]
fn process_resident_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    // Neither rustix nor libc is a direct dependency. Every deployment
    // target shipped by this project uses 4 KiB pages.
    Some(resident_pages.saturating_mul(4096))
}

#[cfg(not(target_os = "linux"))]
fn process_resident_bytes() -> Option<u64> {
    None
}

/// Converts the current Foyer local-cache statistics into gauges.
///
/// These are foyer's own numbers, read at scrape time; the cache's hits,
/// misses, and inserts are reported through the runtime's recorder like
/// every other instrument and are already in the snapshot above.
///
/// Listing every field makes this code fail to compile when `FoyerCacheStats`
/// gains a field, which ensures new statistics are considered for export.
fn local_cache_gauges(stats: &FoyerCacheStats) -> [(&'static str, &'static str, u64); 6] {
    let FoyerCacheStats {
        memory_bytes,
        memory_capacity_bytes,
        disk_bytes,
        disk_capacity_bytes,
        queue_buffer_overflows,
        queue_channel_overflows,
    } = *stats;
    [
        (
            "loonfs_local_cache_memory_bytes",
            "Bytes the local block cache's memory tier is holding",
            memory_bytes as u64,
        ),
        (
            "loonfs_local_cache_memory_capacity_bytes",
            "Bytes the local block cache's memory tier may hold",
            memory_capacity_bytes as u64,
        ),
        (
            "loonfs_local_cache_disk_bytes",
            "Bytes of the local block cache's disk device claimed as blocks",
            disk_bytes as u64,
        ),
        (
            "loonfs_local_cache_disk_capacity_bytes",
            "Bytes of disk the local block cache was opened with",
            disk_capacity_bytes as u64,
        ),
        (
            "loonfs_local_cache_queue_buffer_overflows",
            "Inserts the local block cache dropped with a full flush buffer",
            queue_buffer_overflows,
        ),
        (
            "loonfs_local_cache_queue_channel_overflows",
            "Inserts the local block cache dropped with a full submit queue",
            queue_channel_overflows,
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

#[cfg(test)]
mod tests;
