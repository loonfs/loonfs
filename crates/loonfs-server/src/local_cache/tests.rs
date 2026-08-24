#![allow(clippy::panic)]
// Cache tests panic in unexpected match arms for precise diagnostics.

use super::{
    DiskGeometry, FoyerOverflowCounters, FoyerStoredMetadataBlockCache, OverflowRegistry,
    CACHE_DIRECTORY, DISK_BLOCK_BYTES, FOYER_BUFFER_OVERFLOW_LABEL, FOYER_CHANNEL_OVERFLOW_LABEL,
    FOYER_INNER_OP_COUNTER_VEC, GEOMETRY_MARKER, MIN_DISK_BYTES,
};
use crate::config::{LocalCacheConfig, ServerConfigError};
use bytes::Bytes;
use loonfs::metrics::{
    DefaultMetricsRecorder, MetricValue, MetricsSnapshot, NoopMetricsRecorder, RESULT_HIT,
    RESULT_MISS, RESULT_OK,
};
use loonfs::{StoredMetadataBlockCache, StoredMetadataBlockKey, StoredMetadataBlockKind};
use mixtrics::metrics::RegistryOps;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::tempdir;

/// The disk tier these tests ask for by default.
///
/// foyer claims the device as whole blocks and warns when the block count is
/// not comfortably above the flusher count, so the floor is set by
/// [`DISK_BLOCK_BYTES`] rather than by anything the tests want. Eight blocks
/// clears it with room to shrink toward [`MIN_DISK_BYTES`]. The block files
/// are sparse, so a test pays for the bytes it writes and not for the
/// capacity it asked for.
const TEST_DISK_BYTES: u64 = 8 * DISK_BLOCK_BYTES as u64;

/// A memory tier larger than everything these tests insert.
const TEST_MEMORY_BYTES: u64 = 4 * 1024 * 1024;

/// The prefix foyer names one block file by, followed by the block's index.
///
/// Nothing in foyer's api reports this, so the geometry marker's whole job
/// rests on a naming convention read out of foyer's source. The workspace
/// resolves one foyer version, and a release that renames these files
/// should break the tests below rather than start quietly orphaning them.
const BLOCK_FILE_PREFIX: &str = "foyer-storage-direct-fs-";

fn test_config(root: &Path) -> LocalCacheConfig {
    sized_config(root, TEST_DISK_BYTES)
}

fn sized_config(root: &Path, disk_bytes: u64) -> LocalCacheConfig {
    LocalCacheConfig {
        path: root.display().to_string(),
        memory_bytes: TEST_MEMORY_BYTES,
        disk_bytes,
    }
}

async fn open(root: &Path) -> FoyerStoredMetadataBlockCache {
    open_sized(root, TEST_DISK_BYTES).await
}

async fn open_sized(root: &Path, disk_bytes: u64) -> FoyerStoredMetadataBlockCache {
    FoyerStoredMetadataBlockCache::open(&sized_config(root, disk_bytes), &NoopMetricsRecorder)
        .await
        .expect("open local cache")
}

/// The block files under the versioned directory, which is every file in it
/// but the geometry marker.
fn block_files(root: &Path) -> usize {
    std::fs::read_dir(root.join(CACHE_DIRECTORY))
        .expect("read the versioned directory")
        .filter(|entry| {
            entry
                .as_ref()
                .expect("read a directory entry")
                .file_name()
                .to_string_lossy()
                .starts_with(BLOCK_FILE_PREFIX)
        })
        .count()
}

/// The geometry the versioned directory records.
fn geometry(root: &Path) -> DiskGeometry {
    let bytes = std::fs::read(root.join(CACHE_DIRECTORY).join(GEOMETRY_MARKER))
        .expect("read the geometry marker");
    serde_json::from_slice(&bytes).expect("parse the geometry marker")
}

fn key(offset: u64) -> StoredMetadataBlockKey {
    StoredMetadataBlockKey {
        object_checksum: "sha256:00112233445566778899aabbccddeeff".to_owned(),
        kind: StoredMetadataBlockKind::Data,
        offset,
    }
}

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
    assert_eq!(counter(&snapshot, "loonfs.local_cache.gets", RESULT_HIT), 1);
    assert_eq!(
        counter(&snapshot, "loonfs.local_cache.gets", RESULT_MISS),
        1
    );
    assert_eq!(
        counter(&snapshot, "loonfs.local_cache.closes", RESULT_OK),
        1
    );

    let reopened = open(temp_dir.path()).await;
    assert_eq!(
        reopened.get(&key(0)).await,
        Some(bytes),
        "a reopened cache serves what the closed one wrote"
    );
    reopened.close().await.expect("close reopened cache");
}

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
    assert_eq!(
        block_files(temp_dir.path()),
        (TEST_DISK_BYTES / DISK_BLOCK_BYTES as u64) as usize,
        "the tier holds one file per block of the capacity it was given"
    );
    assert_eq!(
        geometry(temp_dir.path()),
        DiskGeometry {
            block_bytes: DISK_BLOCK_BYTES,
            blocks: (TEST_DISK_BYTES / DISK_BLOCK_BYTES as u64) as usize,
        },
        "the directory records the geometry it was built for"
    );
}

#[tokio::test]
async fn a_shrunk_disk_tier_starts_over() {
    let temp_dir = tempdir().expect("tempdir");
    let bytes = Bytes::from_static(b"encoded stored block");

    let cache = open_sized(temp_dir.path(), TEST_DISK_BYTES).await;
    cache.insert(key(0), bytes.clone());
    assert_eq!(cache.get(&key(0)).await, Some(bytes));
    cache.close().await.expect("close local cache");
    drop(cache);
    assert_eq!(block_files(temp_dir.path()), 8);

    let shrunk_blocks = (MIN_DISK_BYTES / DISK_BLOCK_BYTES as u64) as usize;
    let reopened = open_sized(temp_dir.path(), MIN_DISK_BYTES).await;
    assert_eq!(
        block_files(temp_dir.path()),
        shrunk_blocks,
        "the directory holds the blocks this start claims and no others"
    );
    assert_eq!(
        geometry(temp_dir.path()),
        DiskGeometry {
            block_bytes: DISK_BLOCK_BYTES,
            blocks: shrunk_blocks,
        },
        "the marker records the smaller geometry"
    );
    assert_eq!(
        reopened.get(&key(0)).await,
        None,
        "the discarded directory took what it held with it"
    );
    reopened.close().await.expect("close reopened cache");
}

#[tokio::test]
async fn a_grown_disk_tier_keeps_what_it_had() {
    let temp_dir = tempdir().expect("tempdir");
    let bytes = Bytes::from_static(b"encoded stored block");

    let cache = open_sized(temp_dir.path(), MIN_DISK_BYTES).await;
    cache.insert(key(0), bytes.clone());
    cache.close().await.expect("close local cache");
    drop(cache);

    let grown_blocks = (TEST_DISK_BYTES / DISK_BLOCK_BYTES as u64) as usize;
    let reopened = open_sized(temp_dir.path(), TEST_DISK_BYTES).await;
    assert_eq!(
        reopened.get(&key(0)).await,
        Some(bytes),
        "a grown tier still serves what the smaller one wrote"
    );
    assert_eq!(
        block_files(temp_dir.path()),
        grown_blocks,
        "the directory holds one file per block of the larger capacity"
    );
    assert_eq!(
        geometry(temp_dir.path()),
        DiskGeometry {
            block_bytes: DISK_BLOCK_BYTES,
            blocks: grown_blocks,
        },
        "the marker records the geometry this start claimed"
    );
    reopened.close().await.expect("close reopened cache");
}

#[tokio::test]
async fn an_unreadable_geometry_marker_starts_over() {
    let temp_dir = tempdir().expect("tempdir");
    let bytes = Bytes::from_static(b"encoded stored block");

    let cache = open(temp_dir.path()).await;
    cache.insert(key(0), bytes);
    cache.close().await.expect("close local cache");
    drop(cache);

    std::fs::write(
        temp_dir.path().join(CACHE_DIRECTORY).join(GEOMETRY_MARKER),
        b"not json",
    )
    .expect("overwrite the geometry marker");

    let blocks = (TEST_DISK_BYTES / DISK_BLOCK_BYTES as u64) as usize;
    let reopened = open(temp_dir.path()).await;
    assert_eq!(
        reopened.get(&key(0)).await,
        None,
        "the discarded directory took what it held with it"
    );
    assert_eq!(
        block_files(temp_dir.path()),
        blocks,
        "the directory was rebuilt from nothing"
    );
    assert_eq!(
        geometry(temp_dir.path()),
        DiskGeometry {
            block_bytes: DISK_BLOCK_BYTES,
            blocks,
        },
        "the rebuilt directory records a marker again"
    );
    reopened.close().await.expect("close reopened cache");
}

#[tokio::test]
async fn an_unreadable_geometry_marker_fails_startup() {
    let temp_dir = tempdir().expect("tempdir");
    let cache = open(temp_dir.path()).await;
    cache.insert(key(0), Bytes::from_static(b"encoded stored block"));
    cache.close().await.expect("close local cache");
    drop(cache);

    let marker = temp_dir.path().join(CACHE_DIRECTORY).join(GEOMETRY_MARKER);
    std::fs::remove_file(&marker).expect("remove the geometry marker");
    std::fs::create_dir(&marker).expect("put something unreadable in its place");

    match FoyerStoredMetadataBlockCache::open(&test_config(temp_dir.path()), &NoopMetricsRecorder)
        .await
    {
        Err(ServerConfigError::InvalidField { field, reason }) => {
            assert_eq!(field, "local_cache.path");
            assert!(reason.contains("failed to read"), "{reason}");
        }
        Err(other) => panic!("expected an unreadable-marker error, got {other:?}"),
        Ok(_) => panic!("a marker that cannot be read must not be taken for a fresh directory"),
    }
    assert!(
        temp_dir.path().join(CACHE_DIRECTORY).is_dir(),
        "a start that could not read the marker leaves the directory alone"
    );
}

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
