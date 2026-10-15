//! Live reads with a freshness setting. A `latest` read confirms the
//! namespace head before it reads, and a `bounded` read confirms it at most
//! once per interval.

use crate::writer_position_reads::{self, Parked};
use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    ChangeSeq, Error, Freshness, ListChangesOptions, ListOptions, LoonFs, ManifestNo, Namespace,
    NamespaceId, PinId, ReadFileOptions, ReadFileStreamOptions, ReadOnly, SharedObjectStore,
    StatOptions, Writable,
};
use loonfs_objectstore::keys::{metadata_manifest_prefix, wal_prefix};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{KeyPredicate, MemoryStore, RecordedOperation, RecordingStore};
use loonfs_types::{ChecksumAlgorithm, ROOT_INODE_ID};
use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;

/// The manifest check interval of the fixture's reader runtime.
const INTERVAL_MS: u64 = 1_000;

struct Fixture {
    /// Records every request that either runtime sends.
    recording: Arc<RecordingStore<MemoryStore>>,
    /// The reader runtime's monotonic clock.
    clock: Arc<ManualClock>,
    /// The reader runtime's metrics.
    recorder: Arc<DefaultMetricsRecorder>,
    namespace_id: NamespaceId,
    /// The namespace in the writer runtime.
    namespace: Namespace<Writable>,
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
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .monotonic_timer(clock.clone())
        .manifest_revalidation_interval_ms(INTERVAL_MS)
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("reader")
        .namespace(&namespace_id);
    Fixture {
        recording,
        clock,
        recorder,
        namespace_id,
        namespace,
        reader,
        writer,
    }
}

impl Fixture {
    /// Returns how many reader runtime reads took the anchor after
    /// confirming its head, and how many took it as it was.
    fn anchor_sources(&self) -> (u64, u64) {
        let snapshot = self.recorder.snapshot();
        let source = |label| {
            let entry = snapshot
                .by_name("loonfs.read_head.source")
                .find(|entry| entry.labels == [("source", label)])
                .unwrap_or_else(|| panic!("no `{label}` read head source"));
            match entry.value {
                MetricValue::Counter(value) => value,
                ref other => panic!("expected a counter, found {other:?}"),
            }
        };
        (source("anchor"), source("bounded"))
    }

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

/// Checks that bounded reads in a separate reader runtime send no request
/// while the reader's head was confirmed less than the interval ago, and
/// that the first bounded read after the interval sends the WAL probe, plus
/// the manifest check because the manifest check interval has passed too.
#[tokio::test]
async fn a_bounded_read_sends_no_request_inside_its_interval() {
    let fixture = fixture("bounded-interval").await;
    let reader = &fixture.reader;
    let freshness = Freshness::Bounded { interval: None };
    reader.stat("/file.txt").await.expect("latest stat");
    reader
        .list("/")
        .next()
        .await
        .expect("a page")
        .expect("latest list");
    reader.read_file("/file.txt").await.expect("latest read");
    let (anchors, bounded_reads) = fixture.anchor_sources();

    fixture.recording.reset();
    fixture.clock.advance_ms(INTERVAL_MS - 1);
    let entry = reader
        .stat_with_options(
            "/file.txt",
            &StatOptions {
                freshness,
                ..StatOptions::default()
            },
        )
        .await
        .expect("bounded stat");
    reader
        .list_with_options(
            "/",
            &ListOptions {
                freshness,
                ..ListOptions::default()
            },
        )
        .next()
        .await
        .expect("a page")
        .expect("bounded list");
    let file = reader
        .read_file_with_options("/file.txt", &ReadFileOptions { freshness })
        .await
        .expect("bounded read");
    assert_eq!(file.bytes, b"file");
    assert_eq!(fixture.recording.take(), []);
    assert_eq!(fixture.anchor_sources(), (anchors, bounded_reads + 3));

    fixture.clock.advance_ms(1);
    let confirmed = reader
        .stat_with_options(
            "/file.txt",
            &StatOptions {
                freshness,
                ..StatOptions::default()
            },
        )
        .await
        .expect("bounded stat after the interval");
    assert_eq!(confirmed.head_seq, entry.head_seq);
    assert_matches!(
        fixture.recording.take().as_slice(),
        [
            RecordedOperation::Head { key: manifest_key },
            RecordedOperation::Get {
                key: wal_key,
                range: None,
                result_bytes: 0,
            },
        ] if manifest_key.starts_with(&metadata_manifest_prefix(&fixture.namespace_id))
            && wal_key.starts_with(&wal_prefix(&fixture.namespace_id))
    );
    assert_eq!(fixture.anchor_sources(), (anchors + 1, bounded_reads + 3));
    fixture.shutdown().await;
}

/// Checks that the head that reads start from never moves backwards on a
/// runtime. A bounded read inside its interval can miss another writer's
/// commit, but after a `latest` read sees the commit, a bounded read with a
/// long interval reads at that commit's head or a newer one, not at the head
/// the reader confirmed before it.
#[tokio::test]
async fn a_bounded_read_after_a_latest_read_never_reads_an_older_head() {
    let fixture = fixture("bounded-monotonic").await;
    let reader = &fixture.reader;
    let long = StatOptions {
        freshness: bounded(READ_REVALIDATION_BOUND_MS),
        ..StatOptions::default()
    };
    let before = reader
        .stat_with_options("/file.txt", &long)
        .await
        .expect("first bounded read");
    let commit = fixture
        .namespace
        .put_file("/next.txt", b"next", &loonfs_test_support::test_actor())
        .await
        .expect("commit");
    let stale = reader
        .stat_with_options("/file.txt", &long)
        .await
        .expect("bounded read inside the interval");
    assert_eq!(stale.head_seq, before.head_seq);

    let latest = reader.stat("/next.txt").await.expect("latest read");
    assert_eq!(latest.head_seq, commit.committed_seq);
    let after = reader
        .stat_with_options("/next.txt", &long)
        .await
        .expect("bounded read after the latest read");
    assert!(
        after.head_seq >= commit.committed_seq,
        "a bounded read at {} after a latest read at {}",
        after.head_seq,
        commit.committed_seq
    );
    fixture.shutdown().await;
}

/// Checks that a bounded read after another writer's takeover drops the
/// writer's position reads at the current head, not at an anchor from before
/// the position.
///
/// A read that loads the namespace while the writer has no position can
/// store its anchor after the writer's next commit starts a new position.
/// The test parks that load's manifest check until the commit returns, so
/// the anchor's head is older than the reads from the new position. When
/// the takeover drops the position, the anchor goes with it, so a bounded
/// read with a long interval loads the namespace again.
#[tokio::test]
async fn a_bounded_read_after_a_dropped_position_reads_the_current_head() {
    let fixture = writer_position_reads::fixture("dropped-position", Parked::ManifestChecks).await;
    let gate = fixture.recording.inner();
    // Past the revalidation bound, the next read drops the position and
    // loads the namespace from the store.
    fixture.clock.advance_ms(READ_REVALIDATION_BOUND_MS);
    gate.block_next();
    let (loaded, commit) = futures::join!(fixture.reader.stat("/small.txt"), async {
        gate.wait_until_blocked().await;
        let commit = fixture
            .put("/during.txt")
            .await
            .expect("a commit while the load waits");
        gate.release();
        commit
    });
    let loaded = loaded.expect("the load");
    assert!(
        loaded.head_seq < commit.committed_seq,
        "the load's head {} is from before the commit at {}",
        loaded.head_seq,
        commit.committed_seq
    );
    let positioned = fixture
        .reader
        .stat("/during.txt")
        .await
        .expect("a read from the new position");
    assert_eq!(positioned.head_seq, commit.committed_seq);

    let peer = writer_position_reads::peer(&fixture, "dropped-position-peer").await;
    let peer_commit = peer
        .open_namespace(&fixture.namespace_id)
        .expect("open namespace")
        .put_file("/peer.txt", b"peer", &loonfs_test_support::test_actor())
        .await
        .expect("the peer takes over");
    fixture.clock.advance_ms(writer_position_reads::INTERVAL_MS);
    let entry = fixture
        .reader
        .stat_with_options(
            "/during.txt",
            &StatOptions {
                freshness: bounded(READ_REVALIDATION_BOUND_MS),
                ..StatOptions::default()
            },
        )
        .await
        .expect("a bounded read after the takeover");
    assert_eq!(entry.head_seq, peer_commit.committed_seq);
}
