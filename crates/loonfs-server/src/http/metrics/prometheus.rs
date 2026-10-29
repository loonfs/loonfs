//! The Prometheus text exposition format 0.0.4: metric names, label sets,
//! headers, and the series of counters, gauges, and histograms.

use loonfs::metrics::{MetricEntry, MetricValue, MetricsSnapshot};
use std::fmt::Write as _;

/// Renders a snapshot as Prometheus text exposition format 0.0.4.
///
/// Dotted names become underscored ones, counters take the `_total` suffix
/// the format expects, and histograms emit cumulative buckets with the
/// `+Inf` bucket the format requires. Entries arrive ordered by name and
/// then labels, so one `# HELP`/`# TYPE` pair per name is enough and the
/// output is byte-identical for identical readings.
pub(super) fn render_snapshot(snapshot: &MetricsSnapshot) -> String {
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

pub(super) fn write_gauge(
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
pub(super) type SeriesLabels<'a> = &'a [(&'static str, &'static str)];

/// The labels of a series that has none.
pub(super) const NO_LABELS: SeriesLabels<'static> = &[];

/// Writes a counter with one series for each label set. The written name
/// ends in the `_total` suffix that Prometheus expects of a counter.
pub(super) fn write_counter(
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
