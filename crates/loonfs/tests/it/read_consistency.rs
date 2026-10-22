//! Live reads with a consistency setting. A `strong` read confirms the
//! namespace head before it reads, and an `eventual` read (the default)
//! confirms it only when the last confirmation is older than the read's
//! maximum staleness.

use crate::writer_position_reads::{self, Parked};
use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    ChangeSeq, Consistency, DownloadOptions, Error, ListChangesOptions, ListFileRevisionsOptions,
    ListOptions, ListTrashOptions, LoonFs, MetadataCache, Namespace, NamespaceId, ReadAt,
    ReadConsistency, ReadFileOptions, ReadFileStreamOptions, ReadOnly, SharedObjectStore,
    StatOptions, Writable,
};
use loonfs_objectstore::keys::{metadata_manifest_prefix, wal_prefix};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, page_limit, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::{ChecksumAlgorithm, RevisionNo, ROOT_INODE_ID};
use std::assert_matches;
use std::sync::Arc;

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

fn eventual(max_staleness_ms: u64) -> Option<Consistency> {
    Some(Consistency::Eventual {
        max_staleness_ms: Some(max_staleness_ms),
    })
}

/// Returns the options of a strong stat.
fn strong() -> StatOptions {
    StatOptions {
        at: ReadAt::Live(Some(Consistency::Strong)),
        ..StatOptions::default()
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
        at: ReadAt::Live(consistency),
        ..StatOptions::default()
    };
    let list = ListOptions {
        at: ReadAt::Live(consistency),
        ..ListOptions::default()
    };
    let file = ReadFileOptions {
        at: ReadAt::Live(consistency),
    };
    let stream = ReadFileStreamOptions {
        consistency,
        ..ReadFileStreamOptions::default()
    };
    let revisions = ListFileRevisionsOptions { consistency };
    let download = DownloadOptions {
        at: ReadAt::Live(consistency),
        ..DownloadOptions::default()
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
            .list_changes_with_options(
                ChangeSeq(0),
                &ListChangesOptions {
                    at: ReadAt::Live(consistency),
                },
            )
            .next()
            .await
            .and_then(Result::err),
        reader
            .list_trash_with_options(&ListTrashOptions { consistency })
            .next()
            .await
            .and_then(Result::err),
        reader
            .list_file_revisions_with_options("/file.txt", &revisions)
            .next()
            .await
            .and_then(Result::err),
        reader
            .list_file_revisions_by_inode_with_options(ROOT_INODE_ID, &revisions)
            .next()
            .await
            .and_then(Result::err),
        reader
            .create_download_with_options("/file.txt", &download)
            .await
            .err(),
        reader
            .create_revision_download_with_options("/file.txt", RevisionNo(1), &download)
            .await
            .err(),
        reader
            .create_download_by_inode_with_options(ROOT_INODE_ID, &download)
            .await
            .err(),
        reader
            .create_revision_download_by_inode_with_options(ROOT_INODE_ID, RevisionNo(1), &download)
            .await
            .err(),
    ];
    for error in errors {
        assert_invalid_request(error, "max_staleness_ms");
    }
    assert_eq!(fixture.recording.take(), []);
    fixture.shutdown().await;
}

/// Checks that a read view refuses an eventual read with `invalid_request`
/// for `consistency` before the read sends a request, because a pinned read
/// has no consistency setting.
#[tokio::test]
async fn an_eventual_read_of_a_pinned_view_is_rejected_before_any_request() {
    let fixture = fixture("eventual-view").await;
    let reader = &fixture.reader;
    let consistency = eventual(INTERVAL_MS);
    let view = reader.read_view().await.expect("read view");
    let stat = StatOptions {
        at: ReadAt::Live(consistency),
        ..StatOptions::default()
    };
    let list = ListOptions {
        at: ReadAt::Live(consistency),
        ..ListOptions::default()
    };
    let stream = ReadFileStreamOptions {
        consistency,
        ..ReadFileStreamOptions::default()
    };
    let download = DownloadOptions {
        at: ReadAt::Live(consistency),
        ..DownloadOptions::default()
    };
    fixture.recording.reset();
    let errors = [
        view.stat_with_options("/file.txt", &stat).await.err(),
        view.list_with_options("/", &list)
            .next()
            .await
            .and_then(Result::err),
        view.read_file_stream_with_options("/file.txt", &stream)
            .await
            .err(),
        view.create_download_with_options("/file.txt", &download)
            .await
            .err(),
        view.create_download_by_inode_with_options(ROOT_INODE_ID, &download)
            .await
            .err(),
    ];
    for error in errors {
        assert_invalid_request(error, "consistency");
    }
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
    let consistency = None;
    reader
        .stat_with_options("/file.txt", &strong())
        .await
        .expect("strong stat");
    reader
        .list_with_options(
            "/",
            &ListOptions {
                at: ReadAt::Live(Some(Consistency::Strong)),
                ..ListOptions::default()
            },
        )
        .next()
        .await
        .expect("a page")
        .expect("strong list");
    reader
        .read_file_with_options(
            "/file.txt",
            &ReadFileOptions {
                at: ReadAt::Live(Some(Consistency::Strong)),
            },
        )
        .await
        .expect("strong read");
    let (anchors, eventual_reads) = fixture.anchor_sources();

    fixture.recording.reset();
    fixture.clock.advance_ms(INTERVAL_MS - 1);
    let entry = reader
        .stat_with_options(
            "/file.txt",
            &StatOptions {
                at: ReadAt::Live(consistency),
                ..StatOptions::default()
            },
        )
        .await
        .expect("eventual stat");
    reader
        .list_with_options(
            "/",
            &ListOptions {
                at: ReadAt::Live(consistency),
                ..ListOptions::default()
            },
        )
        .next()
        .await
        .expect("a page")
        .expect("eventual list");
    let file = reader
        .read_file_with_options(
            "/file.txt",
            &ReadFileOptions {
                at: ReadAt::Live(consistency),
            },
        )
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
                at: ReadAt::Live(consistency),
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

/// Checks that a runtime whose default consistency is `strong` confirms the
/// head on a read that sets none, such as a revision listing without
/// options, and that a read that sets `eventual` there sends no request
/// within its maximum staleness.
#[tokio::test]
async fn a_strong_default_confirms_reads_that_set_no_consistency() {
    let fixture = fixture("strong-default").await;
    let store: SharedObjectStore = fixture.recording.clone();
    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .monotonic_timer(fixture.clock.clone())
        .consistency(ReadConsistency::Strong)
        .build()
        .await
        .expect("reader")
        .namespace(&fixture.namespace_id);
    reader.stat("/file.txt").await.expect("warm read");
    let actor = loonfs_test_support::test_actor();
    fixture
        .namespace
        .put_file("/next.txt", b"next", &actor)
        .await
        .expect("commit");
    fixture.recording.reset();
    let revisions = reader
        .list_file_revisions("/next.txt")
        .page_limit(page_limit(10))
        .page()
        .await
        .expect("the listing confirms the head and finds the new file");
    assert_eq!(revisions.revisions.len(), 1);
    assert!(!fixture.recording.take().is_empty());

    fixture
        .namespace
        .put_file("/later.txt", b"later", &actor)
        .await
        .expect("commit");
    fixture.recording.reset();
    let options = StatOptions {
        at: ReadAt::Live(eventual(READ_REVALIDATION_BOUND_MS)),
        ..StatOptions::default()
    };
    let missing = reader.stat_with_options("/later.txt", &options).await;
    assert_eq!(
        missing.expect_err("within the maximum staleness").code(),
        loonfs::ErrorCode::PathNotFound
    );
    assert_eq!(fixture.recording.take(), []);
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
        at: ReadAt::Live(eventual(READ_REVALIDATION_BOUND_MS)),
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

    let strong = reader
        .stat_with_options("/next.txt", &strong())
        .await
        .expect("strong read");
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

/// Creates a read-only runtime over `store` whose head cache has
/// `max_head_bytes`, with a manual clock and a one-second manifest check
/// interval.
async fn budgeted_reader(
    store: &SharedObjectStore,
    clock: Arc<ManualClock>,
    max_head_bytes: usize,
) -> LoonFs<ReadOnly> {
    LoonFs::builder_with_store(store.clone())
        .read_only()
        .monotonic_timer(clock)
        .manifest_revalidation_interval_ms(INTERVAL_MS)
        .metadata_cache(
            MetadataCache::builder()
                .max_head_bytes(max_head_bytes)
                .build(),
        )
        .build()
        .await
        .expect("reader")
}

/// Returns whether a runtime whose head cache has `max_head_bytes` keeps
/// the namespace's head after a strong read, so an eventual read takes it
/// without loading the namespace.
async fn keeps_the_head(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    max_head_bytes: usize,
) -> bool {
    let reader =
        budgeted_reader(store, Arc::new(ManualClock::new(1_000_000)), max_head_bytes).await;
    let namespace = reader.namespace(namespace_id);
    namespace
        .stat_with_options("/file.txt", &strong())
        .await
        .expect("strong read");
    let misses = reader.metadata_cache().stats().head_misses;
    namespace
        .stat_with_options(
            "/file.txt",
            &StatOptions {
                at: ReadAt::Live(eventual(READ_REVALIDATION_BOUND_MS)),
                ..StatOptions::default()
            },
        )
        .await
        .expect("eventual read");
    reader.metadata_cache().stats().head_misses == misses
}

/// Checks that an eventual read after a strong read never reads an older
/// head when the newer head is too large for the head cache.
///
/// A commit can't change a namespace's root grants, so the head grows here
/// the one way it can: a writer with a 128-byte writer ID takes the
/// namespace over, and the head includes its writer's ID. The reader's
/// head budget is the smallest that keeps the head from before the
/// takeover, so the newer head is too large to cache. The takeover's
/// WAL-tail projection goes to the tail cache, which has a budget of its
/// own, so it doesn't evict the older head. A cache that kept the older
/// head after refusing the newer one would return the sequence from before
/// the takeover to the eventual read at the end.
#[tokio::test]
async fn an_eventual_read_after_a_strong_read_of_a_head_too_large_to_cache_loads_it() {
    let namespace_id = namespace_id("eventual-oversized");
    let store: SharedObjectStore = Arc::new(MemoryStore::new(ChecksumAlgorithm::Crc64nvme));
    let actor = loonfs_test_support::test_actor();
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("a"))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .put_file("/file.txt", b"file", &actor)
        .await
        .expect("commit file");
    writer.shutdown().await.expect("writer shutdown");

    let (mut lower, mut upper) = (0, 64 * 1024);
    assert!(keeps_the_head(&store, &namespace_id, upper).await);
    while upper - lower > 1 {
        let middle = lower + (upper - lower) / 2;
        if keeps_the_head(&store, &namespace_id, middle).await {
            upper = middle;
        } else {
            lower = middle;
        }
    }
    let clock = Arc::new(ManualClock::new(1_000_000));
    let reader = budgeted_reader(&store, clock.clone(), upper).await;
    let namespace = reader.namespace(&namespace_id);
    let before = namespace
        .stat_with_options("/file.txt", &strong())
        .await
        .expect("strong read");

    let peer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id(&format!("b{}", "x".repeat(127))))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("peer");
    let commit = peer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .put_file("/peer.txt", &[b'p'; 8 * 1024], &actor)
        .await
        .expect("the peer takes over");
    assert!(commit.committed_seq > before.head_seq);
    clock.advance_ms(INTERVAL_MS);
    let strong = namespace
        .stat_with_options("/peer.txt", &strong())
        .await
        .expect("strong read");
    assert_eq!(strong.head_seq, commit.committed_seq);

    let misses = reader.metadata_cache().stats().head_misses;
    let after = namespace
        .stat_with_options(
            "/file.txt",
            &StatOptions {
                at: ReadAt::Live(eventual(READ_REVALIDATION_BOUND_MS)),
                ..StatOptions::default()
            },
        )
        .await
        .expect("eventual read");
    assert_eq!(after.head_seq, commit.committed_seq);
    assert_eq!(
        reader.metadata_cache().stats().head_misses,
        misses + 1,
        "the eventual read loads the namespace"
    );
    peer.shutdown().await.expect("peer shutdown");
}

/// Checks that an eventual read after another writer's takeover drops the
/// writer's position reads at the current head, not at a head from before
/// the position.
///
/// A read that loads the namespace while the writer has no position can
/// finish after the writer's next commit starts a new position, with a head
/// older than the reads from that position. The test parks that load's
/// manifest check until the commit returns. After the takeover, the first
/// eventual read past its maximum staleness runs the session's check, which
/// finds the new writer's claim, so the runtime drops the position and the
/// cached head it became, and the read loads the namespace again.
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
    fixture
        .clock
        .advance_ms(writer_position_reads::MAX_STALENESS_MS);
    let entry = fixture
        .reader
        .stat_with_options(
            "/during.txt",
            &StatOptions {
                at: ReadAt::Live(eventual(writer_position_reads::MAX_STALENESS_MS)),
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
/// and then the load finishes. The session's close makes its position the
/// namespace's cached head, and the load's older result doesn't replace a
/// newer head, so the load starts from the position's head, and so does the
/// eventual read at the end.
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
    assert_eq!(
        loaded.head_seq, positioned.head_seq,
        "the load starts from the head that the session's close installed"
    );
    let entry = fixture
        .reader
        .stat_with_options(
            "/small.txt",
            &StatOptions {
                at: ReadAt::Live(eventual(READ_REVALIDATION_BOUND_MS)),
                ..StatOptions::default()
            },
        )
        .await
        .expect("an eventual read after the close");
    assert_eq!(entry.head_seq, positioned.head_seq);
}

/// Checks that the strong reads of one namespace in a reader runtime share
/// the runtime's refreshes of the namespace's head, and that the refresh
/// confirms the head for the eventual reads after it.
///
/// A strong read starts a refresh, whose successor probe the store parks.
/// Strong reads that arrive during that refresh wait for it, because it
/// might have sent its probes before they started, and then share the next
/// one. So the wave sends two WAL probes, one for each refresh, and the
/// successor probe of the first, because the second refresh is within the
/// manifest check interval of the first.
#[tokio::test]
async fn a_wave_of_strong_reads_shares_at_most_two_refreshes() {
    let fixture = fixture("strong-wave").await;
    fixture
        .reader
        .stat("/file.txt")
        .await
        .expect("cache a view");
    fixture.clock.advance_ms(INTERVAL_MS);
    let gate = fixture.recording.inner();
    gate.block_next();
    fixture.recording.reset();
    let options = strong();
    let (first, ()) = futures::join!(
        fixture.reader.stat_with_options("/file.txt", &options),
        async {
            gate.wait_until_blocked().await;
            let mut wave = std::pin::pin!(futures::future::join_all(
                (0..4).map(|_| fixture.reader.stat_with_options("/file.txt", &options))
            ));
            assert!(
                futures::poll!(wave.as_mut()).is_pending(),
                "the reads wait for the parked refresh"
            );
            gate.release();
            for entry in wave.await {
                entry.expect("a read during the refresh");
            }
        }
    );
    first.expect("the read that started the first refresh");
    let operations = fixture.recording.take();
    let is_wal = |key: &str| key.starts_with(&wal_prefix(&fixture.namespace_id));
    let probes = |manifests: bool| {
        operations
            .iter()
            .filter(|operation| match operation {
                RecordedOperation::Head { key } => manifests && !is_wal(key),
                RecordedOperation::Get { key, .. } => !manifests && is_wal(key),
                _ => false,
            })
            .count()
    };
    assert_eq!(
        (probes(true), probes(false)),
        (1, 2),
        "successor probes and WAL probes: {operations:?}"
    );
    fixture
        .reader
        .stat("/file.txt")
        .await
        .expect("an eventual read");
    assert_eq!(
        fixture.recording.take(),
        [],
        "the refresh confirmed the head"
    );
    fixture.shutdown().await;
}

/// Checks that a writer session's position becomes the namespace's cached
/// head when the session closes. A read with the default consistency
/// (eventual) within its maximum staleness then sends no request, and the
/// next strong read sends only the WAL probe, without loading the namespace
/// from the store.
#[tokio::test]
async fn a_closed_sessions_position_becomes_the_cached_head() {
    let fixture = writer_position_reads::fixture("closed-session", Parked::WalPuts).await;
    let commit = fixture.put("/next.txt").await.expect("commit");
    fixture
        .writer
        .open_namespace(&fixture.namespace_id)
        .expect("a handle on the session")
        .close()
        .await
        .expect("close the session");
    fixture.recording.reset();
    let entry = fixture
        .reader
        .stat("/next.txt")
        .await
        .expect("an eventual read after the close");
    assert_eq!(entry.head_seq, commit.committed_seq);
    assert_eq!(fixture.recording.take(), []);

    let entry = fixture
        .reader
        .stat_with_options("/next.txt", &strong())
        .await
        .expect("a strong read after the close");
    assert_eq!(entry.head_seq, commit.committed_seq);
    assert_matches!(
        fixture.recording.take().as_slice(),
        [RecordedOperation::Get {
            key,
            range: None,
            result_bytes: 0,
        }] if key.starts_with(&wal_prefix(&fixture.namespace_id))
    );
}
