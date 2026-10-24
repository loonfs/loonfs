//! Prometheus rendering for the server's operational metrics route.

use super::AppState;
use loonfs::engine::DecodedBlockCacheStats;
use loonfs::metrics::{MetricEntry, MetricValue, MetricsSnapshot, RESULT_HIT, RESULT_MISS};
use loonfs::{MetadataCacheStats, SsdSegmentBlocksStats};
use loonfs_grep::GrepStepBudgetStats;
use loonfs_http::RequestLimit;
use std::fmt::Write as _;

/// Renders the recorder's snapshot, then the values that a scrape reads from
/// the server's state, such as the caches' stats and the free transfer slots.
pub(super) fn render(state: &AppState) -> String {
    let binding = &state.binding;
    let mut rendered = render_snapshot(&binding.metrics.snapshot());
    render_metadata_cache(&mut rendered, &state.runtime.metadata_cache().stats());
    // A server that doesn't serve grep has no grep cache. Reporting zeros
    // would incorrectly suggest that the cache is idle.
    if let Some(grep_block_cache) = &state.grep_block_cache {
        render_grep_block_cache(&mut rendered, &grep_block_cache.stats());
    }
    render_scrape_gauges(
        &mut rendered,
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

/// Appends current gauge values sampled during a scrape: grep steps, Linux
/// process RSS, and available transfer slots.
fn render_scrape_gauges(
    rendered: &mut String,
    grep_steps: Option<GrepStepBudgetStats>,
    upload_permits: usize,
    download_permits: usize,
    in_flight_requests: usize,
) {
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

/// The counters of one tier of the segment cache.
struct SegmentTierCounts {
    hits: usize,
    misses: usize,
    inserts: usize,
    evictions: usize,
    rejections: usize,
}

/// The label sets of one tier's series: the tier alone, and the tier with
/// each lookup result.
type TierLabels = [SeriesLabels<'static>; 3];

const MEMORY_TIER: TierLabels = [
    &[("tier", "memory")],
    &[("tier", "memory"), ("result", RESULT_HIT)],
    &[("tier", "memory"), ("result", RESULT_MISS)],
];

const SSD_TIER: TierLabels = [
    &[("tier", "ssd")],
    &[("tier", "ssd"), ("result", RESULT_HIT)],
    &[("tier", "ssd"), ("result", RESULT_MISS)],
];

/// Writes the segment cache's counters, with one series for each tier
/// under a `tier` label, and the segment cache's gauges.
///
/// The SSD tier's series appear only on a server with an SSD tier, because
/// zeros would suggest that the server has an idle SSD tier.
fn render_segment_cache(
    rendered: &mut String,
    memory: SegmentTierCounts,
    ssd: Option<SsdSegmentBlocksStats>,
    retained_decoded_bytes: usize,
) {
    let mut tiers = vec![(MEMORY_TIER, memory)];
    if let Some(ssd) = ssd {
        let counts = SegmentTierCounts {
            hits: ssd.hits,
            misses: ssd.misses,
            inserts: ssd.inserts,
            evictions: ssd.evictions,
            rejections: ssd.rejections,
        };
        tiers.push((SSD_TIER, counts));
    }
    let gets: Vec<_> = tiers
        .iter()
        .flat_map(|([_, hit, miss], counts)| [(*hit, counts.hits), (*miss, counts.misses)])
        .collect();
    write_counter(
        rendered,
        "loonfs_segment_cache_gets",
        "Metadata segment cache lookups, by tier and outcome",
        &gets,
    );
    let per_tier = |count: fn(&SegmentTierCounts) -> usize| -> Vec<_> {
        tiers
            .iter()
            .map(|([tier, ..], counts)| (*tier, count(counts)))
            .collect()
    };
    write_counter(
        rendered,
        "loonfs_segment_cache_inserts",
        "Blocks inserted into a tier of the metadata segment cache",
        &per_tier(|counts| counts.inserts),
    );
    write_counter(
        rendered,
        "loonfs_segment_cache_evictions",
        "Blocks evicted from a tier of the metadata segment cache",
        &per_tier(|counts| counts.evictions),
    );
    write_counter(
        rendered,
        "loonfs_segment_cache_rejections",
        "Blocks that a tier of the metadata segment cache didn't keep",
        &per_tier(|counts| counts.rejections),
    );
    write_gauge(
        rendered,
        "loonfs_segment_cache_retained_decoded_bytes",
        "Bytes currently charged to metadata segment blocks in memory, including keys and per-entry bookkeeping",
        retained_decoded_bytes,
    );
    if let Some(ssd) = ssd {
        write_gauge(
            rendered,
            "loonfs_segment_cache_retained_stored_bytes",
            "Bytes currently charged to metadata segment blocks on the SSD tier, at least 4 KiB for each block, including per-entry bookkeeping",
            ssd.stored_bytes,
        );
    }
}

/// Writes the metadata cache's stats as counters and gauges, in one family
/// for each cache.
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
        segment_rejections,
        segment_filter_skips,
        segment_filter_false_positives,
        segment_bytes,
        segment_ssd,
        manifest_hits,
        manifest_misses,
        manifest_inserts,
        manifest_evictions,
        manifest_rejections,
        manifest_bytes,
        head_hits,
        head_misses,
        head_evictions,
        head_evicted_bytes,
        head_rejections,
        head_rejected_bytes,
        head_bytes,
        wal_tail_hits,
        wal_tail_misses,
        wal_tail_inserts,
        wal_tail_evictions,
        wal_tail_evicted_bytes,
        wal_tail_rejections,
        wal_tail_rejected_bytes,
        wal_tail_bytes,
        content_object_hits,
        content_object_misses,
        content_object_inserts,
        content_object_evictions,
        content_object_bytes,
        completed_upload_session_hits: _,
        completed_upload_session_misses: _,
        completed_upload_session_evictions: _,
        completed_upload_session_bytes: _,
    } = *stats;
    render_segment_cache(
        rendered,
        SegmentTierCounts {
            hits: segment_hits,
            misses: segment_misses,
            inserts: segment_inserts,
            evictions: segment_evictions,
            rejections: segment_rejections,
        },
        segment_ssd,
        segment_bytes,
    );
    for (name, description, hits, misses) in [
        (
            "loonfs_manifest_cache_gets",
            "Manifest cache lookups, by outcome",
            manifest_hits,
            manifest_misses,
        ),
        (
            "loonfs_head_cache_gets",
            "Namespace head cache lookups, by outcome",
            head_hits,
            head_misses,
        ),
        (
            "loonfs_wal_tail_cache_gets",
            "WAL-tail cache lookups, by outcome",
            wal_tail_hits,
            wal_tail_misses,
        ),
        (
            "loonfs_content_object_cache_gets",
            "Content object cache lookups, by outcome",
            content_object_hits,
            content_object_misses,
        ),
    ] {
        write_counter(rendered, name, description, &lookups(hits, misses));
    }
    for (name, description, value) in [
        (
            "loonfs_segment_cache_filter_skips",
            "Segments skipped after their filter ruled out a lookup",
            segment_filter_skips,
        ),
        (
            "loonfs_segment_cache_filter_false_positives",
            "Filter admissions that matched no metadata rows",
            segment_filter_false_positives,
        ),
        (
            "loonfs_manifest_cache_inserts",
            "Manifests inserted into the manifest cache",
            manifest_inserts,
        ),
        (
            "loonfs_manifest_cache_evictions",
            "Manifests evicted to stay within the manifest budget",
            manifest_evictions,
        ),
        (
            "loonfs_manifest_cache_rejections",
            "Manifests heavier than the whole manifest budget",
            manifest_rejections,
        ),
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
        (
            "loonfs_wal_tail_cache_inserts",
            "WAL-tail projections inserted into the WAL-tail cache",
            wal_tail_inserts,
        ),
        (
            "loonfs_wal_tail_cache_evictions",
            "WAL-tail projections evicted to stay within the WAL-tail budget",
            wal_tail_evictions,
        ),
        (
            "loonfs_wal_tail_cache_evicted_decoded_bytes",
            "Bytes charged to evicted WAL-tail projections, including keys and per-entry bookkeeping",
            wal_tail_evicted_bytes,
        ),
        (
            "loonfs_wal_tail_cache_rejections",
            "WAL-tail projections heavier than the whole WAL-tail budget",
            wal_tail_rejections,
        ),
        (
            "loonfs_wal_tail_cache_rejected_decoded_bytes",
            "Bytes charged to rejected WAL-tail projections, including keys and per-entry bookkeeping",
            wal_tail_rejected_bytes,
        ),
        (
            "loonfs_content_object_cache_inserts",
            "Content objects inserted into the content object cache",
            content_object_inserts,
        ),
        (
            "loonfs_content_object_cache_evictions",
            "Content objects evicted to stay within the content object budget",
            content_object_evictions,
        ),
    ] {
        write_counter(rendered, name, description, &[(NO_LABELS, value)]);
    }
    for (name, description, value) in [
        (
            "loonfs_manifest_cache_retained_decoded_bytes",
            "Bytes currently charged to manifests, including keys and per-entry bookkeeping",
            manifest_bytes,
        ),
        (
            "loonfs_head_cache_retained_decoded_bytes",
            "Bytes currently charged to namespace heads, including keys and per-entry bookkeeping",
            head_bytes,
        ),
        (
            "loonfs_wal_tail_cache_retained_decoded_bytes",
            "Bytes currently charged to WAL-tail projections, including keys and per-entry bookkeeping",
            wal_tail_bytes,
        ),
        (
            "loonfs_content_object_cache_retained_decoded_bytes",
            "Bytes currently charged to content objects, including keys and per-entry bookkeeping",
            content_object_bytes,
        ),
    ] {
        write_gauge(rendered, name, description, value);
    }
}

/// Writes the grep cache's stats as counters and a gauge.
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
        cached_decoded_bytes,
    } = *stats;
    write_counter(
        rendered,
        "loonfs_grep_cache_gets",
        "Grep cache lookups, by outcome",
        &lookups(hits, misses),
    );
    write_counter(
        rendered,
        "loonfs_grep_cache_inserts",
        "Decoded grep manifests and blocks inserted into the grep cache",
        &[(NO_LABELS, inserts)],
    );
    write_counter(
        rendered,
        "loonfs_grep_cache_evictions",
        "Decoded grep manifests and blocks evicted to stay within the grep cache budget",
        &[(NO_LABELS, evictions)],
    );
    write_gauge(
        rendered,
        "loonfs_grep_cache_retained_decoded_bytes",
        "Bytes currently charged to decoded grep manifests and blocks, including keys and per-entry bookkeeping",
        cached_decoded_bytes,
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
