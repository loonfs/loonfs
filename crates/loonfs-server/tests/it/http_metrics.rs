//! What a scrape of a running server actually returns.

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs_client::NamespacePath;
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use std::collections::BTreeMap;
use std::io::Read as _;
use tempfile::tempdir;

/// The exposition lines of one scrape, keyed by everything left of the
/// value. Comment lines are dropped; a value that does not parse as a number
/// is a rendering bug and fails here.
fn scrape(server_url: &str, token: Option<&str>) -> Result<BTreeMap<String, f64>, u16> {
    let request = raw_agent().get(&format!("{server_url}/metrics"));
    let request = match token {
        Some(token) => request.set("authorization", &format!("Bearer {token}")),
        None => request,
    };
    let response = match request.call() {
        Ok(response) => response,
        Err(ureq::Error::Status(status, _)) => return Err(status),
        Err(error) => unreachable!("metrics scrape failed: {error}"),
    };
    assert_eq!(
        response.header("content-type"),
        Some("text/plain; version=0.0.4")
    );
    let mut body = String::new();
    response
        .into_reader()
        .read_to_string(&mut body)
        .expect("read scrape body");
    Ok(body
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| {
            let (series, value) = line
                .rsplit_once(' ')
                .unwrap_or_else(|| unreachable!("exposition line without a value: {line}"));
            let value: f64 = value
                .parse()
                .unwrap_or_else(|_| unreachable!("unparsable value in `{line}`"));
            (series.to_owned(), value)
        })
        .collect())
}

fn series(scrape: &BTreeMap<String, f64>, name: &str) -> f64 {
    *scrape
        .get(name)
        .unwrap_or_else(|| unreachable!("no series `{name}` in the scrape"))
}

/// Object-store calls of every operation and outcome, summed.
fn object_store_calls(scrape: &BTreeMap<String, f64>) -> f64 {
    scrape
        .iter()
        .filter(|(series, _)| series.starts_with("loonfs_object_store_operations_total{"))
        .map(|(_, calls)| calls)
        .sum()
}

/// The route reports what the process is doing, not whether it is up, so it
/// authorizes like every other route — unlike `/health` and `/readiness`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scraping_metrics_without_a_token_is_unauthorized() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-metrics-auth",
    ))
    .await;

    assert_eq!(scrape(&harness.server_url, None).expect_err("401"), 401);
    assert!(raw_agent()
        .get(&format!("{}/health", harness.server_url))
        .call()
        .is_ok());

    harness.server.abort();
}

/// One scrape has to show all three layers moving: the requests this server
/// served, the object-store calls they made, and the runtime caches those
/// reads warmed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scrape_reports_requests_object_store_calls_and_cache_levels() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-metrics",
    ))
    .await;

    let namespace = namespace_id("metered");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("metered", "/note.txt").expect("parse path");
    harness
        .client
        .put_file_bytes(&target, b"body", &replace_file_options())
        .await
        .expect("write file");
    harness
        .client
        .namespace_status(&namespace)
        .await
        .expect("read status");

    let first = scrape(&harness.server_url, Some("test-token")).expect("scrape");

    // Requests are labeled by the route template axum matched, never by the
    // path — the namespace id in that path would be an unbounded label.
    // Labels render in sorted order, so a scrape of the same readings is
    // byte-stable however the instrument was registered.
    let status_route = "loonfs_server_requests_total{method=\"GET\",\
                        route=\"/v0/namespaces/{namespace}\",status_class=\"2xx\"}";
    assert_eq!(series(&first, status_route), 1.0);
    assert_eq!(
        series(
            &first,
            "loonfs_server_requests_total{method=\"POST\",route=\"/v0/namespaces\",\
             status_class=\"2xx\"}"
        ),
        1.0
    );
    assert!(first
        .keys()
        .all(|series| !series.contains("/v0/namespaces/metered")));
    assert!(
        series(
            &first,
            "loonfs_server_request_seconds_count{route=\"/v0/namespaces/{namespace}\"}"
        ) >= 1.0
    );

    // The object-store bridge and the scrape-time cache gauges.
    assert!(
        series(
            &first,
            "loonfs_object_store_operations_total{operation=\"put\",result=\"ok\"}"
        ) > 0.0
    );
    assert!(
        series(
            &first,
            "loonfs_object_store_bytes_in_total{operation=\"put\"}"
        ) > 0.0
    );
    assert!(first.contains_key("loonfs_cache_metadata_table_cache_hits"));
    assert!(first.contains_key("loonfs_cache_latest_metadata_view_reads"));
    assert_eq!(
        series(&first, "loonfs_server_upload_permits_available"),
        8.0,
        "no transfer is in flight, so every configured slot is free"
    );

    // Counters are counters: a second status read moves that series and
    // only that series.
    harness
        .client
        .namespace_status(&namespace)
        .await
        .expect("read status again");
    let second = scrape(&harness.server_url, Some("test-token")).expect("second scrape");
    assert_eq!(series(&second, status_route), 2.0);
    assert!(
        object_store_calls(&second) > object_store_calls(&first),
        "the second read made object-store calls of its own"
    );

    harness.server.abort();
}

/// A path outside the served surface must not become a label of its own, or
/// one scanner turns the request metric into a cardinality bomb.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmatched_paths_share_one_route_label() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-metrics-unmatched",
    ))
    .await;

    for path in ["/wp-login.php", "/admin", "/v0/nope"] {
        let _ = raw_agent()
            .get(&format!("{}{path}", harness.server_url))
            .call();
    }

    let scraped = scrape(&harness.server_url, Some("test-token")).expect("scrape");
    assert_eq!(
        series(
            &scraped,
            "loonfs_server_requests_total{method=\"GET\",route=\"unmatched\",\
             status_class=\"4xx\"}"
        ),
        3.0
    );
    assert!(scraped.keys().all(|series| !series.contains("wp-login")));

    harness.server.abort();
}
