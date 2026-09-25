//! Prometheus rendering for the server's operational metrics route.

use crate::local_cache::FoyerCacheStats;
use loonfs::metrics::{MetricEntry, MetricValue, MetricsSnapshot};
use loonfs_http::HttpMetrics;
use std::fmt::Write as _;

pub(super) fn render(
    metrics: &HttpMetrics,
    local_cache: Option<FoyerCacheStats>,
    upload_permits: usize,
    download_permits: usize,
) -> String {
    let mut rendered = render_snapshot(&metrics.snapshot());
    render_scrape_gauges(&mut rendered, local_cache, upload_permits, download_permits);
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
/// Linux process RSS, and available transfer slots.
fn render_scrape_gauges(
    rendered: &mut String,
    local_cache: Option<FoyerCacheStats>,
    upload_permits: usize,
    download_permits: usize,
) {
    // Omit local-cache metrics when this server has no local cache. Reporting
    // zeros would incorrectly suggest that an enabled cache is idle.
    if let Some(local_cache) = local_cache {
        for (name, description, value) in local_cache_gauges(&local_cache) {
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
