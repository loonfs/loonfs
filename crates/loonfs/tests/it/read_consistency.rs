//! Live reads with a consistency setting. A `strong` read confirms the
//! namespace head before it reads, and an `eventual` read confirms it only
//! when the last confirmation is older than the read's maximum staleness.

use crate::writer_position_reads::{self, Parked};
use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    ChangeSeq, Consistency, Error, ListChangesOptions, ListOptions, LoonFs, ManifestNo, Namespace,
    NamespaceId, PinId, ReadFileOptions, ReadFileStreamOptions, ReadOnly, SharedObjectStore,
    StatOptions, Writable,
};
use loonfs_objectstore::keys::{metadata_manifest_prefix, wal_prefix};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::{ChecksumAlgorithm, ROOT_INODE_ID};
use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;

/// The manifest check interval of the fixture's reader runtime.
const INTERVAL_MS: u64 = 1_000;

struct Fixture {
    /// Records every request that either runtime sends, and can park the
    /// namespace's manifest checks.
    recording: Arc<RecordingStore<BlockingStore<MemoryStore>>>,
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
/// one-second manifest check interval. The store parks the namespace's
/// manifest checks (HEAD requests) once a test calls `block_next`.
async fn fixture(name: &str) -> Fixture {
    let namespace_id = namespace_id(name);
    let recording = Arc::new(RecordingStore::new(
        BlockingStore::new(
            MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
            KeyPredicate::manifest(&namespace_id),
            OperationSelector::Head,
        ),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
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
        (source("anchor"), source("eventual"))
    }

    async fn shutdown(self) {
        self.writer.shutdown().await.expect("writer shutdown");
    }
}

fn eventual(max_staleness_ms: u64) -> Consistency {
    Consistency::Eventual {
        max_staleness: Some(Duration::from_millis(max_staleness_ms)),
    }
}

/// Asserts that a read failed with `invalid_request` for `expected`.
fn assert_invalid_request(error: Option<Error>, expected: &str) {
    match error {
        Some(Error::InvalidRequest { param, .. }) => assert_eq!(param, expected),
        other => panic!("expected `invalid_request` for `{expected}`, got {other:?}"),
    }
}

/// Checks that a maximum staleness above the revalidation bound fails every
/// live read with `invalid_request` for `max_staleness_ms`, before the
/// read sends a request.
#[tokio::test]
async fn a_maximum_staleness_above_the_revalidation_bound_is_rejected_before_any_request() {
    let fixture = fixture("staleness-above-bound").await;
    let reader = &fixture.reader;
    let consistency = eventual(READ_REVALIDATION_BOUND_MS + 1);
    let stat = StatOptions {
        consistency,
        ..StatOptions::default()
    };
    let list = ListOptions {
        consistency,
        ..ListOptions::default()
    };
    let file = ReadFileOptions { consistency };
    let stream = ReadFileStreamOptions {
        consistency,
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
            .list_changes_with_options(ChangeSeq(0), &ListChangesOptions { consistency })
            .next()
            .await
            .and_then(Result::err),
    ];
    for error in errors {
        assert_invalid_request(error, "max_staleness_ms");
    }
    assert_eq!(fixture.recording.take(), []);
    fixture.shutdown().await;
}

/// Checks that an eventual read with a `snapshot_id` fails with
/// `invalid_request` for `consistency` before the read sends a request, and
/// that a read view refuses an eventual read too, because a pinned read has no
/// consistency setting.
#[tokio::test]
async fn an_eventual_read_of_a_pinned_view_is_rejected_before_any_request() {
    let fixture = fixture("eventual-snapshot").await;
    let reader = &fixture.reader;
    let consistency = eventual(INTERVAL_MS);
    let snapshot_id = Some(PinId::generate(ManifestNo(1)));
    let stat = StatOptions {
        snapshot_id: snapshot_id.clone(),
        consistency,
        ..StatOptions::default()
    };
    let list = ListOptions {
        snapshot_id,
        consistency,
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
        assert_invalid_request(error, "consistency");
    }
    assert_eq!(fixture.recording.take(), []);

    let view = reader.read_view().await.expect("read view");
    let stat = StatOptions {
        consistency,
        ..StatOptions::default()
    };
    let stream = ReadFileStreamOptions {
        consistency,
        ..ReadFileStreamOptions::default()
    };
    fixture.recording.reset();
    assert_invalid_request(
        view.stat_with_options("/file.txt", &stat).await.err(),
        "consistency",
    );
    assert_invalid_request(
        view.read_file_stream_with_options("/file.txt", &stream)
            .await
            .err(),
        "consistency",
    );
    assert_eq!(fixture.recording.take(), []);
    fixture.shutdown().await;
}

/// Checks that eventual reads in a separate reader runtime send no request
/// while the reader's head was confirmed less than the maximum staleness ago, and
/// that the first eventual read after the maximum staleness passes sends the WAL probe, plus
/// the manifest check because the manifest check interval has passed too.
#[tokio::test]
async fn an_eventual_read_sends_no_request_within_its_maximum_staleness() {
    let fixture = fixture("eventual-staleness").await;
    let reader = &fixture.reader;
    let consistency = Consistency::Eventual {
        max_staleness: None,
    };
    reader.stat("/file.txt").await.expect("strong stat");
    reader
        .list("/")
        .next()
        .await
        .expect("a page")
        .expect("strong list");
    reader.read_file("/file.txt").await.expect("strong read");
    let (anchors, eventual_reads) = fixture.anchor_sources();

    fixture.recording.reset();
    fixture.clock.advance_ms(INTERVAL_MS - 1);
    let entry = reader
        .stat_with_options(
            "/file.txt",
            &StatOptions {
                consistency,
                ..StatOptions::default()
            },
        )
        .await
        .expect("eventual stat");
    reader
        .list_with_options(
            "/",
            &ListOptions {
                consistency,
                ..ListOptions::default()
            },
        )
        .next()
        .await
        .expect("a page")
        .expect("eventual list");
    let file = reader
        .read_file_with_options("/file.txt", &ReadFileOptions { consistency })
        .await
        .expect("eventual read");
    assert_eq!(file.bytes, b"file");
    assert_eq!(fixture.recording.take(), []);
    assert_eq!(fixture.anchor_sources(), (anchors, eventual_reads + 3));

    fixture.clock.advance_ms(1);
    let confirmed = reader
        .stat_with_options(
            "/file.txt",
            &StatOptions {
                consistency,
                ..StatOptions::default()
            },
        )
        .await
        .expect("eventual stat after the maximum staleness");
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
    assert_eq!(fixture.anchor_sources(), (anchors + 1, eventual_reads + 3));
    fixture.shutdown().await;
}

/// Checks that the head that reads start from never moves backwards on a
/// runtime. An eventual read within its maximum staleness can miss another writer's
/// commit, but after a `strong` read sees the commit, an eventual read with a
/// long maximum staleness reads at that commit's head or a newer one, not at the head
/// the reader confirmed before it.
#[tokio::test]
async fn an_eventual_read_after_a_strong_read_never_reads_an_older_head() {
    let fixture = fixture("eventual-monotonic").await;
    let reader = &fixture.reader;
    let long = StatOptions {
        consistency: eventual(READ_REVALIDATION_BOUND_MS),
        ..StatOptions::default()
    };
    let before = reader
        .stat_with_options("/file.txt", &long)
        .await
        .expect("first eventual read");
    let commit = fixture
        .namespace
        .put_file("/next.txt", b"next", &loonfs_test_support::test_actor())
        .await
        .expect("commit");
    let stale = reader
        .stat_with_options("/file.txt", &long)
        .await
        .expect("eventual read within the maximum staleness");
    assert_eq!(stale.head_seq, before.head_seq);

    let strong = reader.stat("/next.txt").await.expect("strong read");
    assert_eq!(strong.head_seq, commit.committed_seq);
    let after = reader
        .stat_with_options("/next.txt", &long)
        .await
        .expect("eventual read after the strong read");
    assert!(
        after.head_seq >= commit.committed_seq,
        "an eventual read at {} after a strong read at {}",
        after.head_seq,
        commit.committed_seq
    );
    fixture.shutdown().await;
}

/// Checks that an eventual read after another writer's takeover drops the
/// writer's position reads at the current head, not at an anchor from before
/// the position.
///
/// A read that loads the namespace while the writer has no position can
/// finish after the writer's next commit starts a new position, with a head
/// older than the reads from that position. The test parks that load's
/// manifest check until the commit returns. The position's start fences the
/// load's install, and the takeover's drop of the position leaves no anchor
/// either, so an eventual read with a long maximum staleness loads the namespace again.
#[tokio::test]
async fn an_eventual_read_after_a_dropped_position_reads_the_current_head() {
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
                consistency: eventual(READ_REVALIDATION_BOUND_MS),
                ..StatOptions::default()
            },
        )
        .await
        .expect("an eventual read after the takeover");
    assert_eq!(entry.head_seq, peer_commit.committed_seq);
}

/// Checks that a load that started before the writer's session took a
/// position, and finished after the session closed, doesn't put its older
/// head back into the read cache.
///
/// The steps follow the race. The load reads the WAL at the first commit's
/// head and waits on its manifest check. The writer commits again, a read
/// from the writer's position returns that newer head, the session closes,
/// and then the load finishes. The position's start and the session's close
/// each drop the namespace's anchor while the slot is empty, and each drop
/// advances the slot's absence generation, which fences the load's install.
/// Without that advance, the load would install its older anchor, and the
/// eventual read at the end would return the first commit's head.
#[tokio::test]
async fn a_load_that_spans_a_writer_session_does_not_restore_an_older_head() {
    let fixture = writer_position_reads::fixture("spanning-load", Parked::ManifestChecks).await;
    let gate = fixture.recording.inner();
    // Past the revalidation bound, the next read drops the position and
    // loads the namespace from the store.
    fixture.clock.advance_ms(READ_REVALIDATION_BOUND_MS);
    gate.block_next();
    let (loaded, positioned) = futures::join!(fixture.reader.stat("/small.txt"), async {
        gate.wait_until_blocked().await;
        let commit = fixture
            .put("/during.txt")
            .await
            .expect("a commit while the load waits");
        let positioned = fixture
            .reader
            .stat("/during.txt")
            .await
            .expect("a read from the writer's position");
        assert_eq!(positioned.head_seq, commit.committed_seq);
        fixture
            .writer
            .open_namespace(&fixture.namespace_id)
            .expect("a handle on the session")
            .close()
            .await
            .expect("close the session");
        gate.release();
        positioned
    });
    let loaded = loaded.expect("the load");
    assert!(
        loaded.head_seq < positioned.head_seq,
        "the load's head {} is older than the position's {}",
        loaded.head_seq,
        positioned.head_seq
    );
    let entry = fixture
        .reader
        .stat_with_options(
            "/small.txt",
            &StatOptions {
                consistency: eventual(READ_REVALIDATION_BOUND_MS),
                ..StatOptions::default()
            },
        )
        .await
        .expect("an eventual read after the close");
    assert_eq!(entry.head_seq, positioned.head_seq);
}

/// Checks that of two cold loads of one namespace in a reader runtime, the
/// older one finishing last doesn't replace the newer head in the read
/// cache.
///
/// The first load reads the WAL and waits on its manifest check. The writer
/// commits, and a second load reads the newer head and installs it. Both
/// loads found the slot empty, so the first load's install is fenced when it
/// finishes, and that read starts from the newer anchor instead.
#[tokio::test]
async fn an_older_cold_load_finishing_last_leaves_the_newer_head_cached() {
    let fixture = fixture("concurrent-cold-loads").await;
    let gate = fixture.recording.inner();
    gate.block_next();
    let (older, newer) = futures::join!(fixture.reader.stat("/file.txt"), async {
        gate.wait_until_blocked().await;
        let commit = fixture
            .namespace
            .put_file("/next.txt", b"next", &loonfs_test_support::test_actor())
            .await
            .expect("a commit while the first load waits");
        let newer = fixture
            .reader
            .stat("/next.txt")
            .await
            .expect("a second cold load");
        assert_eq!(newer.head_seq, commit.committed_seq);
        gate.release();
        newer
    });
    let older = older.expect("the first cold load");
    assert_eq!(
        older.head_seq, newer.head_seq,
        "the fenced load starts from the newer anchor"
    );
    let entry = fixture
        .reader
        .stat_with_options(
            "/next.txt",
            &StatOptions {
                consistency: eventual(READ_REVALIDATION_BOUND_MS),
                ..StatOptions::default()
            },
        )
        .await
        .expect("an eventual read after both loads");
    assert_eq!(entry.head_seq, newer.head_seq);
    fixture.shutdown().await;
}
