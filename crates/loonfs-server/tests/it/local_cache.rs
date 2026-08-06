//! What the node-local block cache carries from one server to the next.

use crate::common::http_split_support::*;
use crate::common::{scrape, series, start_graceful_server};
use loonfs_api::CreateCheckpointRequest;
use loonfs_client::NamespacePath;
use loonfs_server::{LocalCacheConfig, MaintenanceMode, ServerConfig};
use loonfs_test_support::ids::namespace_id;
use std::path::Path;
use tempfile::tempdir;

/// Every block kind a segment read can ask this cache for.
const BLOCK_KINDS: [&str; 3] = ["index", "filter", "data"];

/// The `loonfs.local_cache.gets` series for one block kind and outcome.
fn gets(kind: &str, result: &str) -> String {
    format!("loonfs_local_cache_gets_total{{kind=\"{kind}\",result=\"{result}\"}}")
}

/// The `loonfs.local_cache.inserts` series for one block kind.
fn inserts(kind: &str) -> String {
    format!("loonfs_local_cache_inserts_total{{kind=\"{kind}\"}}")
}

/// A server holding its cache in `cache_root` and its objects in
/// `store_root`.
///
/// Maintenance is manual so the two servers read the same manifest: an
/// automatic flush between them would publish segments the first server
/// never saw, and a miss on those would say nothing about the cache.
fn test_config_with_local_cache(
    store_root: &Path,
    cache_root: &Path,
    writer_id: &str,
) -> ServerConfig {
    ServerConfig {
        maintenance: MaintenanceMode::Manual,
        local_cache: Some(LocalCacheConfig {
            path: cache_root.display().to_string(),
            memory_bytes: 4 * 1024 * 1024,
            disk_bytes: 128 * 1024 * 1024,
        }),
        ..test_config(store_root.to_path_buf(), writer_id, "local-cache")
    }
}

/// A restarted server reads every metadata segment section — index, filter,
/// and data — out of the local cache instead of the store.
///
/// The second server starts with empty in-memory caches, so the only thing
/// carried over is the cache directory the first one closed. Every section it
/// wants is one the first server fetched and filled, and a section it had to
/// fetch would show up here twice over: as a miss on the way in and as an
/// insert on the way back. Zero of both is what makes this a full read with
/// no segment bytes read at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_server_reads_every_segment_section_from_its_local_cache() {
    let store_dir = tempdir().expect("store tempdir");
    let cache_dir = tempdir().expect("cache tempdir");
    let namespace = namespace_id("warm");
    let listing = NamespacePath::parse("warm", "/docs").expect("parse path");

    let first = start_graceful_server(test_config_with_local_cache(
        &store_dir.path().join("store"),
        cache_dir.path(),
        "local-cache-first",
    ))
    .await;
    first
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    for index in 0..4 {
        let path = format!("/docs/file-{index}.txt");
        let target = NamespacePath::parse("warm", &path).expect("parse path");
        first
            .client
            .put_file_bytes(&target, b"file", &replace_file_options())
            .await
            .expect("put file");
    }
    first
        .client
        .create_checkpoint(
            &namespace,
            &CreateCheckpointRequest {
                name: "warm".to_owned(),
                ttl_ms: None,
            },
        )
        .await
        .expect("create checkpoint");
    // One write after the checkpoint moves the head, so reads resolve
    // against the published manifest and touch its segments.
    let extra = NamespacePath::parse("warm", "/docs/after.txt").expect("parse path");
    first
        .client
        .put_file_bytes(&extra, b"after", &replace_file_options())
        .await
        .expect("put file after the checkpoint");

    let warmed = first
        .client
        .list_path_entries_all(&listing)
        .await
        .expect("warm the cache");
    assert_eq!(warmed.entries.len(), 5);
    let filled = scrape(&first.server_url, Some("test-token")).expect("scrape the first server");
    assert!(
        series(&filled, &gets("index", "miss")) >= 1.0,
        "a cold server misses on the sections it then fills"
    );
    for kind in BLOCK_KINDS {
        assert!(
            series(&filled, &inserts(kind)) >= 1.0,
            "a cold server should have filled its {kind} sections"
        );
    }
    first.shut_down().await;

    let second = start_graceful_server(test_config_with_local_cache(
        &store_dir.path().join("store"),
        cache_dir.path(),
        "local-cache-second",
    ))
    .await;
    let served = second
        .client
        .list_path_entries_all(&listing)
        .await
        .expect("list from the restarted server");
    assert_eq!(served.entries, warmed.entries);

    let restarted = scrape(&second.server_url, Some("test-token")).expect("scrape the second");
    for kind in ["index", "data"] {
        assert!(
            series(&restarted, &gets(kind, "hit")) >= 1.0,
            "the restarted server should read {kind} sections from the cache"
        );
    }
    for kind in BLOCK_KINDS {
        assert_eq!(
            series(&restarted, &gets(kind, "miss")),
            0.0,
            "every {kind} section the restarted server wanted should have survived"
        );
        assert_eq!(
            series(&restarted, &inserts(kind)),
            0.0,
            "a {kind} section served from the cache is not fetched, so it is not reinserted"
        );
    }
    second.shut_down().await;
}
