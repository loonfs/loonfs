//! Prometheus rendering for the server's operational metrics route.

mod caches;
mod prometheus;

use self::caches::{
    render_grep_block_cache, render_grep_worker_block_cache, render_metadata_cache,
};
use self::prometheus::{render_snapshot, write_counter, write_gauge};
use super::AppState;
use loonfs::engine::PoolCounts;
use loonfs_grep::GrepPublicationStats;
use loonfs_http::RequestLimit;

/// Renders the recorder's snapshot, then the values that a scrape reads from
/// the server's state, such as the caches' stats and the free transfer slots.
pub(super) fn render(state: &AppState) -> String {
    let http = &state.http;
    let mut rendered = render_snapshot(&http.metrics.snapshot());
    render_metadata_cache(&mut rendered, &http.runtime.metadata_cache().stats());
    // A server that doesn't serve grep has no grep cache. Reporting zeros
    // would incorrectly suggest that the cache is idle.
    if let Some(service) = http.grep.service() {
        render_grep_block_cache(&mut rendered, &service.block_cache_stats());
    }
    let grep_worker = http.grep.worker();
    if let Some(worker) = grep_worker {
        render_grep_worker_block_cache(&mut rendered, &worker.block_cache_stats());
    }
    render_scrape_gauges(
        &mut rendered,
        grep_worker.map(|worker| worker.step_budget().stats()),
        http.upload_permits().available_permits(),
        http.download_permits().available_permits(),
        http.request_limit().map_or(0, RequestLimit::in_flight),
    );
    // A server that doesn't maintain grep publishes no grep entries, and
    // zeros would suggest that its index steps publish nothing.
    if let Some(worker) = grep_worker {
        render_grep_publications(&mut rendered, worker.publication_stats());
    }
    rendered
}

/// Writes how the grep worker's publications of grep's entry ended, as one
/// counter with an `outcome` label.
///
/// Listing every field makes this code fail to compile when
/// `GrepPublicationStats` gains a field, which ensures a new outcome is
/// considered for export.
fn render_grep_publications(rendered: &mut String, stats: GrepPublicationStats) {
    let GrepPublicationStats {
        published,
        rebuilt,
        superseded,
    } = stats;
    write_counter(
        rendered,
        "loonfs_grep_publications",
        "Publications of grep's entry by this server's index steps, by outcome",
        &[
            (&[("outcome", "published")], published),
            (&[("outcome", "rebuilt")], rebuilt),
            (&[("outcome", "superseded")], superseded),
        ],
    );
}

/// Appends current gauge values sampled during a scrape: grep steps, Linux
/// process RSS, and available transfer slots.
fn render_scrape_gauges(
    rendered: &mut String,
    grep_steps: Option<PoolCounts>,
    upload_permits: usize,
    download_permits: usize,
    in_flight_requests: usize,
) {
    if let Some(PoolCounts { running, waiting }) = grep_steps {
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

#[cfg(test)]
mod tests;
