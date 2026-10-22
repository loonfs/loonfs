//! What the SSD tier of the metadata segment cache keeps from one server to
//! the next.

use crate::common::http_split_support::*;
use crate::common::{collect_path_entries, scrape, series, start_graceful_server};
use loonfs_client::NamespacePath;
use loonfs_server::{JobMode, MetadataCacheConfig, MetadataCacheSsdConfig, ServerConfig};
use loonfs_test_support::ids::namespace_id;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;
use tempfile::tempdir;

/// The segment cache's lookups of `tier` with `result`.
fn gets(scrape: &BTreeMap<String, f64>, tier: &str, result: &str) -> f64 {
    series(
        scrape,
        &format!("loonfs_segment_cache_gets_total{{tier=\"{tier}\",result=\"{result}\"}}"),
    )
}

/// The ranged GETs of metadata segments that the server sent to the store.
fn segment_gets(scrape: &BTreeMap<String, f64>) -> f64 {
    scrape
        .get(
            "loonfs_object_store_operations_total{key_class=\"metadata_segment\",\
             operation=\"get\",result=\"ok\"}",
        )
        .copied()
        .unwrap_or(0.0)
}

/// A server that keeps its objects in `store_root` and its SSD tier in
/// `ssd_root`.
///
/// Maintenance is serve-only, so the two servers read the same manifest. A
/// fold between them would publish segments that the first server never
/// read, and a miss on those would say nothing about the tier.
fn config_with_ssd_tier(store_root: &Path, ssd_root: &Path, writer_id: &str) -> ServerConfig {
    ServerConfig {
        maintenance: JobMode::ServeOnly,
        metadata_cache: MetadataCacheConfig {
            ssd: Some(MetadataCacheSsdConfig {
                directory: ssd_root.display().to_string(),
                max_segment_bytes: NonZeroUsize::new(64 * 1024 * 1024).expect("nonzero"),
            }),
            ..MetadataCacheConfig::default()
        },
        ..test_config(store_root.to_path_buf(), writer_id, "ssd-tier")
    }
}

/// Checks that a server with an SSD tier writes the blocks it reads to
/// disk, and that a server restarted over the same directory reads them
/// from disk: its first reads hit the SSD tier and send no GET for a
/// metadata segment, while a server without the tier sends GETs for the
/// same reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_server_reads_its_metadata_blocks_from_the_ssd_tier() {
    let store_dir = tempdir().expect("store tempdir");
    let ssd_dir = tempdir().expect("ssd tempdir");
    let store_root = store_dir.path().join("store");
    let namespace = namespace_id("warm");
    let listing = NamespacePath::parse("warm", "/docs").expect("parse path");

    let first = start_graceful_server(config_with_ssd_tier(
        &store_root,
        ssd_dir.path(),
        "ssd-tier-first",
    ))
    .await;
    first
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    for index in 0..4 {
        let target =
            NamespacePath::parse("warm", &format!("/docs/file-{index}.txt")).expect("parse path");
        first
            .client
            .put_file_with_options(
                &target,
                b"file",
                &loonfs_test_support::test_actor(),
                &replace_file_options(),
            )
            .await
            .expect("put file");
    }
    // The snapshot folds the WAL tail into segments, and the write after it
    // moves the head, so reads resolve against the folded manifest.
    first
        .client
        .create_snapshot(&namespace, "warm", None)
        .await
        .expect("create snapshot");
    let extra = NamespacePath::parse("warm", "/docs/after.txt").expect("parse path");
    first
        .client
        .put_file_with_options(
            &extra,
            b"after",
            &loonfs_test_support::test_actor(),
            &replace_file_options(),
        )
        .await
        .expect("put a file after the snapshot");
    let listed = collect_path_entries(&first.client, &listing, &Default::default())
        .await
        .expect("list from the first server");
    assert_eq!(listed.entries.len(), 5);
    let filled = scrape(&first.server_url, Some("test-token")).expect("scrape the first server");
    assert!(
        series(&filled, "loonfs_segment_cache_inserts_total{tier=\"ssd\"}") >= 1.0,
        "the first server writes the blocks it inserts to disk"
    );
    // Shutting down closes the SSD tier, which writes the blocks it queued.
    first.shut_down().await;

    let second = start_graceful_server(config_with_ssd_tier(
        &store_root,
        ssd_dir.path(),
        "ssd-tier-second",
    ))
    .await;
    let found = scrape(&second.server_url, Some("test-token")).expect("scrape at startup");
    assert!(series(&found, "loonfs_segment_cache_retained_stored_bytes") > 0.0);
    let served = collect_path_entries(&second.client, &listing, &Default::default())
        .await
        .expect("list from the restarted server");
    assert_eq!(served.entries, listed.entries);

    let restarted = scrape(&second.server_url, Some("test-token")).expect("scrape the second");
    assert!(
        gets(&restarted, "ssd", "hit") >= 1.0,
        "the restarted server reads blocks from disk"
    );
    assert_eq!(
        gets(&restarted, "ssd", "miss"),
        0.0,
        "every block the restarted server wanted was on disk"
    );
    assert_eq!(
        segment_gets(&restarted),
        0.0,
        "the restarted server sends no GET for a metadata segment"
    );
    second.shut_down().await;

    let without_tier = start_graceful_server(ServerConfig {
        maintenance: JobMode::ServeOnly,
        ..test_config(store_root.clone(), "ssd-tier-without", "ssd-tier")
    })
    .await;
    let cold = collect_path_entries(&without_tier.client, &listing, &Default::default())
        .await
        .expect("list from a server without the tier");
    assert_eq!(cold.entries, listed.entries);
    let cold = scrape(&without_tier.server_url, Some("test-token")).expect("scrape");
    assert!(
        segment_gets(&cold) >= 1.0,
        "a server without the tier reads the segments from the store"
    );
    without_tier.shut_down().await;
}
