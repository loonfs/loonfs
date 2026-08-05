#![allow(clippy::panic)]
// Cache tests panic in unexpected match arms for precise diagnostics.

use super::{
    FoyerOverflowCounters, FoyerStoredMetadataBlockCache, OverflowRegistry, CACHE_DIRECTORY,
    DISK_BLOCK_BYTES, FOYER_BUFFER_OVERFLOW_LABEL, FOYER_CHANNEL_OVERFLOW_LABEL,
    FOYER_INNER_OP_COUNTER_VEC,
};
use crate::config::{LocalCacheConfig, ServerConfigError};
use bytes::Bytes;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue, MetricsSnapshot, NoopMetricsRecorder};
use loonfs::{StoredMetadataBlockCache, StoredMetadataBlockKey, StoredMetadataBlockKind};
use mixtrics::metrics::RegistryOps;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::tempdir;

/// The smallest disk tier these tests can ask for.
///
/// foyer claims the device as whole blocks and warns when the block count is
/// not comfortably above the flusher count, so the floor is set by
/// [`DISK_BLOCK_BYTES`] rather than by anything the tests want. Eight blocks
/// clears it. The block files are sparse, so a test pays for the bytes it
/// writes and not for the capacity it asked for.
const TEST_DISK_BYTES: u64 = 8 * DISK_BLOCK_BYTES as u64;

/// A memory tier larger than everything these tests insert.
const TEST_MEMORY_BYTES: u64 = 4 * 1024 * 1024;

fn test_config(root: &Path) -> LocalCacheConfig {
    LocalCacheConfig {
        path: root.display().to_string(),
        memory_bytes: TEST_MEMORY_BYTES,
        disk_bytes: TEST_DISK_BYTES,
    }
}

async fn open(root: &Path) -> FoyerStoredMetadataBlockCache {
    FoyerStoredMetadataBlockCache::open(&test_config(root), &NoopMetricsRecorder)
        .await
        .expect("open local cache")
}

fn key(offset: u64) -> StoredMetadataBlockKey {
    StoredMetadataBlockKey {
        payload_checksum: "sha256:00112233445566778899aabbccddeeff".to_owned(),
        kind: StoredMetadataBlockKind::Data,
        offset,
    }
}

/// The cache keeps what it is given and finds it again after a restart.
///
/// The reopen is the point. An insert reaches disk, closing flushes what the
/// memory tier still held, and a fresh cache over the same directory serves
/// both; without that this would be an in-memory cache with a directory
/// beside it.
#[tokio::test]
async fn a_kept_block_survives_a_reopen() {
    let temp_dir = tempdir().expect("tempdir");
    let bytes = Bytes::from_static(b"encoded stored block");

    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let cache =
        FoyerStoredMetadataBlockCache::open(&test_config(temp_dir.path()), recorder.as_ref())
            .await
            .expect("open local cache");
    assert_eq!(cache.get(&key(0)).await, None, "an empty cache misses");
    cache.insert(key(0), bytes.clone());
    assert_eq!(cache.get(&key(0)).await, Some(bytes.clone()));
    cache.close().await.expect("close local cache");
    drop(cache);

    // The instruments are registered at construction and carry a closed
    // label set, so one hit and one miss are already two named series.
    let snapshot = recorder.snapshot();
    assert_eq!(counter(&snapshot, "loonfs.local_cache.gets", "hit"), 1);
    assert_eq!(counter(&snapshot, "loonfs.local_cache.gets", "miss"), 1);
    assert_eq!(counter(&snapshot, "loonfs.local_cache.closes", "clean"), 1);

    let reopened = open(temp_dir.path()).await;
    assert_eq!(
        reopened.get(&key(0)).await,
        Some(bytes),
        "a reopened cache serves what the closed one wrote"
    );
    reopened.close().await.expect("close reopened cache");
}

/// One directory belongs to one server.
#[tokio::test]
async fn a_second_open_of_one_directory_is_refused() {
    let temp_dir = tempdir().expect("tempdir");
    let held = open(temp_dir.path()).await;

    match FoyerStoredMetadataBlockCache::open(&test_config(temp_dir.path()), &NoopMetricsRecorder)
        .await
    {
        Err(ServerConfigError::InvalidField { field, reason }) => {
            assert_eq!(field, "local_cache.path");
            assert!(reason.contains("locked by another process"), "{reason}");
        }
        Err(other) => panic!("expected a locked-directory error, got {other:?}"),
        Ok(_) => panic!("a second server must not open a directory another one owns"),
    }

    held.close().await.expect("close local cache");
}

/// A closed cache is inert, and closing it again is not an error.
#[tokio::test]
async fn a_closed_cache_answers_nothing_and_keeps_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let cache = open(temp_dir.path()).await;
    let bytes = Bytes::from_static(b"encoded stored block");
    cache.insert(key(0), bytes.clone());
    assert_eq!(cache.get(&key(0)).await, Some(bytes.clone()));

    cache.close().await.expect("close local cache");
    assert!(cache.is_closed());

    assert_eq!(
        cache.get(&key(0)).await,
        None,
        "a lookup after the close is a miss, whatever the cache still holds"
    );
    cache.insert(key(1), bytes);
    cache.invalidate(&key(0));
    assert_eq!(cache.get(&key(1)).await, None, "the insert did nothing");
    cache
        .close()
        .await
        .expect("closing a closed cache does nothing further and succeeds");
}

/// A root the process cannot make a directory of fails startup.
#[tokio::test]
async fn an_unusable_path_fails_startup() {
    let temp_dir = tempdir().expect("tempdir");
    let file_path = temp_dir.path().join("not-a-directory");
    std::fs::write(&file_path, b"").expect("write file");

    match FoyerStoredMetadataBlockCache::open(&test_config(&file_path), &NoopMetricsRecorder).await
    {
        Err(ServerConfigError::InvalidField { field, .. }) => {
            assert_eq!(field, "local_cache.path");
        }
        Err(other) => panic!("expected an invalid path error, got {other:?}"),
        Ok(_) => panic!("a root that is a file must not open"),
    }
}

/// The disk tier writes beneath the versioned directory, in one open file
/// per block.
///
/// The file count is what bounds [`DISK_BLOCK_BYTES`] from below: the tier
/// claims the whole capacity as blocks at startup and holds every block file
/// open, so a smaller block means proportionally more descriptors for the
/// same `disk_bytes`. The assertion is here so that relationship cannot
/// change without someone noticing.
#[tokio::test]
async fn the_disk_tier_claims_the_capacity_as_block_files() {
    let temp_dir = tempdir().expect("tempdir");
    let cache = open(temp_dir.path()).await;
    cache.insert(key(0), Bytes::from_static(b"encoded stored block"));
    cache.close().await.expect("close local cache");

    let directory = temp_dir.path().join(CACHE_DIRECTORY);
    assert!(
        directory.is_dir(),
        "the versioned directory is where the tier's files go"
    );
    let files = std::fs::read_dir(&directory)
        .expect("read the versioned directory")
        .count();
    assert_eq!(
        files,
        (TEST_DISK_BYTES / DISK_BLOCK_BYTES as u64) as usize,
        "the tier holds one file per block of the capacity it was given"
    );
}

/// foyer's overflow counters reach this process's numbers.
///
/// The bridge is one metric name and two label values agreed with foyer, and
/// nothing in the type system holds the agreement together, so it is
/// asserted here: the registry hands back the counters foyer increments, and
/// what foyer increments is what a scrape reads.
#[test]
fn the_registry_keeps_foyers_two_overflow_counters() {
    let counters = FoyerOverflowCounters::default();
    let registry = OverflowRegistry::new(counters.clone());

    let inner_ops = registry.register_counter_vec(
        FOYER_INNER_OP_COUNTER_VEC.into(),
        "foyer disk cache inner operations".into(),
        &["name", "op"],
    );
    inner_ops
        .counter(&["cache".into(), FOYER_BUFFER_OVERFLOW_LABEL.into()])
        .increase(3);
    inner_ops
        .counter(&["cache".into(), FOYER_CHANNEL_OVERFLOW_LABEL.into()])
        .increase(5);
    // An operation this process does not keep is discarded rather than
    // misfiled onto one of the two.
    inner_ops
        .counter(&["cache".into(), "queue_rotate".into()])
        .increase(7);

    assert_eq!(counters.buffer.load(Ordering::Relaxed), 3);
    assert_eq!(counters.channel.load(Ordering::Relaxed), 5);

    // Every other vector foyer registers is discarded whole.
    registry
        .register_counter_vec("foyer_storage_op_total".into(), "other".into(), &["name"])
        .counter(&["cache".into()])
        .increase(11);
    assert_eq!(counters.buffer.load(Ordering::Relaxed), 3);
    assert_eq!(counters.channel.load(Ordering::Relaxed), 5);
}

/// The value of one counter series, found by name and `result` label.
fn counter(snapshot: &MetricsSnapshot, name: &str, result: &str) -> u64 {
    snapshot
        .all()
        .iter()
        .filter(|entry| {
            entry.name == name
                && entry
                    .labels
                    .iter()
                    .any(|(key, value)| *key == "result" && *value == result)
        })
        .map(|entry| match entry.value {
            MetricValue::Counter(value) => value,
            _ => panic!("`{name}` should be a counter"),
        })
        .sum()
}
