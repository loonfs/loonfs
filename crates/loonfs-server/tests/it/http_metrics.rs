//! What a scrape of a running server actually returns.

use crate::common::http_split_support::*;
use crate::common::{scrape, series, start_server};
use loonfs_client::NamespacePath;
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use std::collections::BTreeMap;
use tempfile::tempdir;

/// Object-store calls of every operation and outcome, summed.
fn object_store_calls(scrape: &BTreeMap<String, f64>) -> f64 {
    scrape
        .iter()
        .filter(|(series, _)| series.starts_with("loonfs_object_store_operations_total{"))
        .map(|(_, calls)| calls)
        .sum()
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scrape_reports_requests_object_store_calls_and_cache_metrics() {
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
        .get_namespace(&namespace)
        .await
        .expect("read status");

    let first = scrape(&harness.server_url, Some("test-token")).expect("scrape");

    // Requests are labeled by the route template axum matched, never by the
    // path — the namespace id in that path would be an unbounded label.
    // Labels render in sorted order, so a scrape of the same readings is
    // byte-stable however the instrument was registered.
    let status_route = "loonfs_server_requests_total{method=\"GET\",\
                        route=\"/v0/namespaces/{namespace_id}\",status_class=\"2xx\"}";
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
            "loonfs_server_request_seconds_count{route=\"/v0/namespaces/{namespace_id}\"}"
        ) >= 1.0
    );

    // The recorder includes both object-store and cache activity.
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
    assert!(first.contains_key("loonfs_metadata_segment_cache_gets_total{result=\"hit\"}"));
    assert!(first.contains_key("loonfs_runtime_cache_latest_metadata_view_reads_total"));
    assert!(first.contains_key("loonfs_grep_block_cache_gets_total{result=\"hit\"}"));
    assert!(first.contains_key("loonfs_wal_tail_projection_cache_retained_rows"));
    assert_eq!(
        series(&first, "loonfs_server_upload_permits_available"),
        8.0,
        "no transfer is in flight, so every configured slot is free"
    );

    // Counters are counters: a second status read moves that series and
    // only that series.
    harness
        .client
        .get_namespace(&namespace)
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
