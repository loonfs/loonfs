//! Live reads with a freshness setting. A `latest` read confirms the
//! namespace head before it reads, and a `bounded` read confirms it at most
//! once per interval.

use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::{
    ChangeSeq, Error, Freshness, ListChangesOptions, ListOptions, LoonFs, ManifestNo, Namespace,
    PinId, ReadFileOptions, ReadFileStreamOptions, ReadOnly, SharedObjectStore, StatOptions,
    Writable,
};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{KeyPredicate, MemoryStore, RecordingStore};
use loonfs_types::{ChecksumAlgorithm, ROOT_INODE_ID};
use std::sync::Arc;
use std::time::Duration;

/// The manifest check interval of the fixture's reader runtime.
const INTERVAL_MS: u64 = 1_000;

struct Fixture {
    /// Records every request that either runtime sends.
    recording: Arc<RecordingStore<MemoryStore>>,
    /// The namespace in a separate read-only runtime, which reads from its
    /// read cache's anchor.
    reader: Namespace<ReadOnly>,
    writer: LoonFs<Writable>,
}

/// Creates a writer runtime with a namespace that has one file, and a
/// separate read-only runtime over the same store with a manual clock and a
/// one-second manifest check interval.
async fn fixture(name: &str) -> Fixture {
    let recording = Arc::new(RecordingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = namespace_id(name);
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id(name))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .put_file("/file.txt", b"file", &actor)
        .await
        .expect("commit file");
    writer.drain().await.expect("finish hints");
    let clock = Arc::new(ManualClock::new(1_000_000));
    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .monotonic_timer(clock.clone())
        .manifest_revalidation_interval_ms(INTERVAL_MS)
        .build()
        .await
        .expect("reader")
        .namespace(&namespace_id);
    Fixture {
        recording,
        reader,
        writer,
    }
}

impl Fixture {
    async fn shutdown(self) {
        self.writer.shutdown().await.expect("writer shutdown");
    }
}

fn bounded(interval_ms: u64) -> Freshness {
    Freshness::Bounded {
        interval: Some(Duration::from_millis(interval_ms)),
    }
}

/// Asserts that a read failed with `invalid_request` for `expected`.
fn assert_invalid_request(error: Option<Error>, expected: &str) {
    match error {
        Some(Error::InvalidRequest { param, .. }) => assert_eq!(param, expected),
        other => panic!("expected `invalid_request` for `{expected}`, got {other:?}"),
    }
}

/// Checks that a bounded interval above the revalidation bound fails every
/// live read with `invalid_request` for `freshness_interval_ms`, before the
/// read sends a request.
#[tokio::test]
async fn an_interval_above_the_revalidation_bound_is_rejected_before_any_request() {
    let fixture = fixture("interval-above-bound").await;
    let reader = &fixture.reader;
    let freshness = bounded(READ_REVALIDATION_BOUND_MS + 1);
    let stat = StatOptions {
        freshness,
        ..StatOptions::default()
    };
    let list = ListOptions {
        freshness,
        ..ListOptions::default()
    };
    let file = ReadFileOptions { freshness };
    let stream = ReadFileStreamOptions {
        freshness,
        ..ReadFileStreamOptions::default()
    };
    fixture.recording.reset();
    let errors = [
        reader.stat_with_options("/file.txt", &stat).await.err(),
        reader
            .stat_by_inode_with_options(ROOT_INODE_ID, &stat)
            .await
            .err(),
        reader
            .list_with_options("/", &list)
            .next()
            .await
            .and_then(Result::err),
        reader
            .list_by_inode_with_options(ROOT_INODE_ID, &list)
            .next()
            .await
            .and_then(Result::err),
        reader
            .read_file_with_options("/file.txt", &file)
            .await
            .err(),
        reader
            .read_file_by_inode_with_options(ROOT_INODE_ID, &file)
            .await
            .err(),
        reader
            .read_file_stream_with_options("/file.txt", &stream)
            .await
            .err(),
        reader
            .read_file_stream_by_inode_with_options(ROOT_INODE_ID, &stream)
            .await
            .err(),
        reader
            .list_changes_with_options(ChangeSeq(0), &ListChangesOptions { freshness })
            .next()
            .await
            .and_then(Result::err),
    ];
    for error in errors {
        assert_invalid_request(error, "freshness_interval_ms");
    }
    assert_eq!(fixture.recording.take(), []);
    fixture.shutdown().await;
}

/// Checks that a bounded read with a `snapshot_id` fails with
/// `invalid_request` for `freshness` before the read sends a request, and
/// that a read view refuses a bounded read too, because a pinned read has no
/// freshness setting.
#[tokio::test]
async fn a_bounded_read_of_a_pinned_view_is_rejected_before_any_request() {
    let fixture = fixture("bounded-snapshot").await;
    let reader = &fixture.reader;
    let freshness = bounded(INTERVAL_MS);
    let snapshot_id = Some(PinId::generate(ManifestNo(1)));
    let stat = StatOptions {
        snapshot_id: snapshot_id.clone(),
        freshness,
        ..StatOptions::default()
    };
    let list = ListOptions {
        snapshot_id,
        freshness,
        ..ListOptions::default()
    };
    fixture.recording.reset();
    let errors = [
        reader.stat_with_options("/file.txt", &stat).await.err(),
        reader
            .stat_by_inode_with_options(ROOT_INODE_ID, &stat)
            .await
            .err(),
        reader
            .list_with_options("/", &list)
            .next()
            .await
            .and_then(Result::err),
        reader
            .list_by_inode_with_options(ROOT_INODE_ID, &list)
            .next()
            .await
            .and_then(Result::err),
    ];
    for error in errors {
        assert_invalid_request(error, "freshness");
    }
    assert_eq!(fixture.recording.take(), []);

    let view = reader.read_view().await.expect("read view");
    let stat = StatOptions {
        freshness,
        ..StatOptions::default()
    };
    let stream = ReadFileStreamOptions {
        freshness,
        ..ReadFileStreamOptions::default()
    };
    fixture.recording.reset();
    assert_invalid_request(
        view.stat_with_options("/file.txt", &stat).await.err(),
        "freshness",
    );
    assert_invalid_request(
        view.read_file_stream_with_options("/file.txt", &stream)
            .await
            .err(),
        "freshness",
    );
    assert_eq!(fixture.recording.take(), []);
    fixture.shutdown().await;
}
