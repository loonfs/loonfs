//! Reads in a runtime with the namespace's writer session, which start from
//! the session's position instead of the read cache.

use crate::common::is_manifest_head;
use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    Commit, Consistency, ErrorCode, FoldWalOutcome, LoonFs, Namespace, ReadFileOptions, ReadOnly,
    SharedObjectStore, StatOptions, Writable,
};
use loonfs_core::limits::MAX_POSITION_MOVES;
use loonfs_objectstore::keys::{hint, metadata_manifest_object};
use loonfs_test_support::clock::{bounded, ManualClock};
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::{ChecksumAlgorithm, ManifestNo, NamespaceId};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

pub(crate) const MAX_STALENESS_MS: u64 = 1_000;

/// How long a read that must not wait for the store gets before the test
/// fails.
const NO_WAIT: Duration = Duration::from_secs(10);

/// Whether `operation` is a GET of the namespace's manifest `manifest_no`.
fn is_manifest_get(
    operation: &RecordedOperation,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
) -> bool {
    matches!(
        operation,
        RecordedOperation::Get { key, .. } if *key == metadata_manifest_object(namespace_id, &manifest_no)
    )
}

/// Returns the options of a strong stat.
fn strong() -> StatOptions {
    StatOptions {
        consistency: Some(Consistency::Strong),
        ..StatOptions::default()
    }
}

pub(crate) struct Fixture {
    /// Records every request, and can park the operations the fixture
    /// selected.
    pub(crate) recording: Arc<RecordingStore<BlockingStore<MemoryStore>>>,
    recorder: Arc<DefaultMetricsRecorder>,
    pub(crate) clock: Arc<ManualClock>,
    pub(crate) namespace_id: NamespaceId,
    namespace: Namespace<Writable>,
    pub(crate) reader: Namespace<ReadOnly>,
    pub(crate) writer: LoonFs<Writable>,
}

/// The requests that a fixture's store parks once a test calls
/// `block_next`.
pub(crate) enum Parked {
    /// The namespace's WAL puts.
    WalPuts,
    /// HEAD requests for the namespace's manifests.
    ManifestChecks,
}

/// Creates a writer runtime with a one-second maximum staleness, a namespace
/// with a 1 KiB file, and a reader on the writer's runtime.
pub(crate) async fn fixture(name: &str, parked: Parked) -> Fixture {
    let namespace_id = namespace_id(name);
    let (keys, operation) = match parked {
        Parked::WalPuts => (
            KeyPredicate::prefix(loonfs_objectstore::keys::wal_prefix(&namespace_id)),
            OperationSelector::PutCreateIfAbsent,
        ),
        Parked::ManifestChecks => (
            KeyPredicate::manifest(&namespace_id),
            OperationSelector::Head,
        ),
    };
    let recording = Arc::new(RecordingStore::new(
        BlockingStore::new(
            MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
            keys,
            operation,
        ),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let clock = Arc::new(ManualClock::new(1_000_000));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = LoonFs::builder_with_store(store)
        .writer_id(writer_id(name))
        .min_publish_interval_ms(0)
        .max_staleness_ms(MAX_STALENESS_MS)
        .monotonic_timer(clock.clone())
        .metrics_recorder(recorder.clone())
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
        .put_file("/small.txt", &[b's'; 1024], &actor)
        .await
        .expect("small file");
    Fixture {
        reader: writer.read_only().namespace(&namespace_id),
        recording,
        recorder,
        clock,
        namespace_id,
        namespace,
        writer,
    }
}

/// Opens another writer runtime over the fixture's store.
pub(crate) async fn peer(fixture: &Fixture, name: &str) -> LoonFs<Writable> {
    let store: SharedObjectStore = fixture.recording.clone();
    LoonFs::builder_with_store(store)
        .writer_id(writer_id(name))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("peer")
}

impl Fixture {
    pub(crate) async fn put(&self, path: &str) -> loonfs::Result<Commit> {
        self.namespace
            .put_file(path, b"file", &loonfs_test_support::test_actor())
            .await
    }

    fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let snapshot = self.recorder.snapshot();
        let entry = snapshot
            .by_name(name)
            .find(|entry| entry.labels == labels)
            .unwrap_or_else(|| panic!("no `{name}` registered with labels {labels:?}"));
        match entry.value {
            MetricValue::Counter(value) => value,
            ref other => panic!("expected a counter, found {other:?}"),
        }
    }

    /// Returns how many reads started from the writer's position and how
    /// many from the read cache.
    fn head_sources(&self) -> (u64, u64) {
        let source = |label| self.counter("loonfs.read_head.source", &[("source", label)]);
        (source("position"), source("anchor"))
    }

    /// Returns how many read cache lookups of a namespace head hit and how
    /// many missed.
    fn head_cache_lookups(&self) -> (usize, usize) {
        let stats = self.writer.metadata_cache().stats();
        (stats.head_anchor_hits, stats.head_anchor_misses)
    }

    fn tail_replays(&self) -> u64 {
        self.counter("loonfs.publisher.tail_replays", &[])
    }

    /// Asserts that the requests since the last reset start with a successor
    /// check of a manifest, that the walk after it loads that manifest and
    /// no later one, and that the read then reloads the namespace from its
    /// hint.
    fn assert_check_stopped_at_the_claim(&self) {
        let operations = self.recording.take();
        let Some(reload) = operations
            .iter()
            .position(|operation| operation.key() == hint(&self.namespace_id))
        else {
            panic!("no reload from the hint: {operations:?}");
        };
        let walked: Vec<&str> = operations[..reload]
            .iter()
            .filter(|operation| matches!(operation, RecordedOperation::Get { .. }))
            .map(RecordedOperation::key)
            .collect();
        assert!(
            matches!(
                operations.first(),
                Some(RecordedOperation::Head { key }) if walked == [key.as_str()]
            ),
            "a successor check, a walk that stops at the claim, and a reload: {operations:?}"
        );
    }
}

/// Checks that a strong stat and a strong 1 KiB read after a commit in the
/// writer's runtime start from the writer's position, and that each sends
/// one request, the session's check of the position's manifest.
#[tokio::test]
async fn strong_reads_after_a_commit_send_one_manifest_check_each() {
    let fixture = fixture("position-reads", Parked::WalPuts).await;
    let commit = fixture.put("/next.txt").await.expect("commit");
    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    let entry = fixture
        .reader
        .stat_with_options("/next.txt", &strong())
        .await
        .expect("stat");
    assert_eq!(entry.head_seq, commit.committed_seq);
    let file = fixture
        .reader
        .read_file_with_options(
            "/small.txt",
            &ReadFileOptions {
                consistency: Some(Consistency::Strong),
            },
        )
        .await
        .expect("read");
    assert_eq!(file.bytes, [b's'; 1024]);
    let operations = fixture.recording.take();
    assert!(
        operations.len() == 2
            && operations
                .iter()
                .all(|operation| is_manifest_head(operation, &fixture.namespace_id)),
        "{operations:?}"
    );
    assert_eq!(fixture.head_sources(), (positions + 2, anchors));
}

/// Checks that a read with the default consistency (eventual) in the
/// writer's runtime within its maximum staleness of the position's last
/// check starts from the writer's position and sends no store request.
#[tokio::test]
async fn an_eventual_read_starts_from_the_position_with_no_store_request() {
    let fixture = fixture("eventual-position-reads", Parked::WalPuts).await;
    let commit = fixture.put("/next.txt").await.expect("commit");
    let (positions, anchors) = fixture.head_sources();
    let cache_lookups = fixture.head_cache_lookups();
    fixture.recording.reset();
    let entry = fixture.reader.stat("/next.txt").await.expect("stat");
    assert_eq!(entry.head_seq, commit.committed_seq);
    assert_eq!(fixture.recording.take(), []);
    assert_eq!(fixture.head_sources(), (positions + 1, anchors));
    assert_eq!(
        fixture.head_cache_lookups(),
        cache_lookups,
        "a read from the position doesn't look up the read cache"
    );
}

/// Checks that writes in the writer's runtime send no check of the
/// position's manifest. The catalog load and the content preparation before
/// a write read the position as it is, because the position includes every
/// commit of the session, and another writer's takeover fences the session
/// at the write's own publish.
#[tokio::test]
async fn writes_send_no_manifest_check() {
    let fixture = fixture("write-checks", Parked::WalPuts).await;
    let actor = loonfs_test_support::test_actor();
    let large = vec![b'l'; 128 * 1024];
    fixture.recording.reset();
    fixture.put("/inline.txt").await.expect("an inline put");
    fixture
        .namespace
        .put_file("/large.bin", &large, &actor)
        .await
        .expect("a put above the inline threshold");
    fixture
        .namespace
        .append_file("/large.bin", b"more", &actor)
        .await
        .expect("an append");
    let operations = fixture.recording.take();
    assert!(
        !operations
            .iter()
            .any(|operation| is_manifest_head(operation, &fixture.namespace_id)),
        "{operations:?}"
    );
}

/// Checks that a strong read while a publish waits for its WAL put starts
/// from the head before that publish, without waiting for it, and sends
/// nothing but the session's check, and that a read after the publish
/// returns sees its commit.
#[tokio::test]
async fn a_read_during_a_publish_starts_from_the_head_before_it() {
    let fixture = fixture("read-during-publish", Parked::WalPuts).await;
    let before = fixture.put("/before.txt").await.expect("commit before");
    let gate = fixture.recording.inner();
    gate.block_next();
    let (during, ()) = futures::join!(fixture.put("/during.txt"), async {
        gate.wait_until_blocked().await;
        fixture.recording.reset();
        let entry = bounded(
            NO_WAIT,
            fixture.reader.stat_with_options("/before.txt", &strong()),
        )
        .await
        .expect("the read doesn't wait for the publish")
        .expect("stat");
        assert_eq!(entry.head_seq, before.committed_seq);
        let missing = bounded(
            NO_WAIT,
            fixture.reader.stat_with_options("/during.txt", &strong()),
        )
        .await
        .expect("the read doesn't wait for the publish");
        assert_eq!(
            missing.expect_err("the publish hasn't returned").code(),
            ErrorCode::PathNotFound
        );
        let operations = fixture.recording.take();
        assert!(
            operations
                .iter()
                .all(|operation| is_manifest_head(operation, &fixture.namespace_id)),
            "{operations:?}"
        );
        gate.release();
    });
    let during = during.expect("the publish succeeds");
    let entry = fixture.reader.stat("/during.txt").await.expect("stat");
    assert_eq!(entry.head_seq, during.committed_seq);
}

/// Checks that another writer's takeover is visible in the old writer's
/// runtime to a strong read at once, and to an eventual read once its
/// maximum staleness passes, and not before.
///
/// Within the staleness, an eventual read starts from the old session's
/// position and sends nothing. A strong read, and the first eventual read
/// after the staleness, waits for the session's check of the position's
/// manifest. The check finds the new writer's claim, and its walk stops
/// there instead of loading the fold that the new writer published above
/// the claim, so the read reloads the namespace.
#[tokio::test]
async fn another_writers_takeover_is_seen_by_a_strong_read_and_past_the_staleness() {
    for (name, advance_ms, options) in [
        ("takeover-strong", 0, strong()),
        (
            "takeover-eventual",
            MAX_STALENESS_MS,
            StatOptions::default(),
        ),
    ] {
        let fixture = fixture(name, Parked::WalPuts).await;
        let peer_id = format!("{name}-peer");
        let peer = peer(&fixture, &peer_id).await;
        let peer_commit = peer
            .open_namespace(&fixture.namespace_id)
            .expect("open namespace")
            .put_file("/peer.txt", b"peer", &loonfs_test_support::test_actor())
            .await
            .expect("the peer takes over");
        let folded = peer
            .maintenance(writer_id(&peer_id))
            .fold_wal(&fixture.namespace_id)
            .await
            .expect("fold");
        assert_eq!(folded.outcome, FoldWalOutcome::Published, "{name}");

        fixture.recording.reset();
        let inside = fixture.reader.stat("/peer.txt").await;
        assert_eq!(
            inside.expect_err("within the staleness").code(),
            ErrorCode::PathNotFound,
            "{name}"
        );
        assert_eq!(fixture.recording.take(), [], "{name}");

        fixture.clock.advance_ms(advance_ms);
        let (positions, anchors) = fixture.head_sources();
        let entry = fixture
            .reader
            .stat_with_options("/peer.txt", &options)
            .await
            .expect("the check finds the claim");
        assert_eq!(entry.head_seq, peer_commit.committed_seq, "{name}");
        fixture.assert_check_stopped_at_the_claim();
        assert_eq!(fixture.head_sources(), (positions, anchors + 1), "{name}");
        let fenced = fixture.put("/after.txt").await;
        assert_eq!(
            fenced.expect_err("the old session is fenced").code(),
            ErrorCode::WriterFenced,
            "{name}"
        );
    }
}

/// Checks that a check whose walk finds more manifests above the position
/// than one check moves the position onto drops the position, and that the
/// read reloads the namespace from its hint.
///
/// Another runtime folds each of the session's commits, one fold more than
/// the cap, under the session's writer epoch, so the walk loads one manifest
/// past the cap and stops there.
#[tokio::test]
async fn a_walk_past_its_cap_drops_the_position() {
    let fixture = fixture("walk-cap", Parked::WalPuts).await;
    let peer = peer(&fixture, "walk-cap-peer").await;
    let maintenance = peer.maintenance(writer_id("walk-cap-peer"));
    for index in 0..=MAX_POSITION_MOVES {
        fixture
            .put(&format!("/fill-{index}.txt"))
            .await
            .expect("commit");
        let folded = maintenance
            .fold_wal(&fixture.namespace_id)
            .await
            .expect("fold");
        assert_eq!(folded.outcome, FoldWalOutcome::Published);
    }
    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    fixture
        .reader
        .stat_with_options("/small.txt", &strong())
        .await
        .expect("stat");
    let operations = fixture.recording.take();
    let reload = operations
        .iter()
        .position(|operation| operation.key() == hint(&fixture.namespace_id))
        .expect("a reload from the hint");
    let walked = operations[..reload]
        .iter()
        .filter(|operation| matches!(operation, RecordedOperation::Get { .. }))
        .count();
    assert_eq!(walked, MAX_POSITION_MOVES + 1, "{operations:?}");
    assert_eq!(fixture.head_sources(), (positions, anchors + 1));
}

/// Checks that a fold that another runtime publishes is found by the first
/// eventual read past its maximum staleness, that the position moves onto
/// the fold's manifest, and that the session's next publish writes above it
/// without loading the namespace from the store.
#[tokio::test]
async fn another_runtimes_fold_moves_the_position_past_the_staleness() {
    let fixture = fixture("fold-interval", Parked::WalPuts).await;
    let peer = peer(&fixture, "fold-peer").await;
    let folded = peer
        .maintenance(writer_id("fold-peer"))
        .fold_wal(&fixture.namespace_id)
        .await
        .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);

    fixture.recording.reset();
    fixture.reader.stat("/small.txt").await.expect("stat");
    assert_eq!(fixture.recording.take(), [], "within the staleness");

    fixture.clock.advance_ms(MAX_STALENESS_MS);
    let (positions, anchors) = fixture.head_sources();
    fixture
        .reader
        .stat("/small.txt")
        .await
        .expect("the check finds the fold");
    let operations = fixture.recording.take();
    let after_fold = folded
        .manifest_no
        .successor()
        .expect("next manifest number");
    assert!(
        operations.len() >= 3
            && operations[0]
                == RecordedOperation::Head {
                    key: metadata_manifest_object(&fixture.namespace_id, &folded.manifest_no),
                }
            && is_manifest_get(&operations[1], &fixture.namespace_id, folded.manifest_no)
            && is_manifest_get(&operations[2], &fixture.namespace_id, after_fold)
            && operations
                .iter()
                .all(|operation| operation.key() != hint(&fixture.namespace_id)),
        "the check finds the fold's manifest, loads it, finds nothing after it, and runs no \
         discovery: {operations:?}"
    );
    assert_eq!(fixture.head_sources(), (positions + 1, anchors));

    let replays = fixture.tail_replays();
    let commit = fixture
        .put("/after.txt")
        .await
        .expect("commit after the fold");
    assert_eq!(
        fixture.tail_replays(),
        replays,
        "the position moved onto the fold's manifest, so the publish replays nothing"
    );
    fixture.recording.reset();
    let entry = fixture.reader.stat("/after.txt").await.expect("stat");
    assert_eq!(entry.head_seq, commit.committed_seq);
    assert_eq!(
        fixture.recording.take(),
        [],
        "the check confirmed the position"
    );
}

/// Checks that a manifest check that finds a fold this runtime published,
/// while a publish keeps the engine locked so the position can't move yet,
/// loads the fold's manifest, finds nothing after it, and keeps the
/// position without reloading, and that once the publish releases the
/// engine, reads start from the fold's manifest.
///
/// The fold runs through maintenance while the publish waits for its WAL
/// put, so the fold's move of the position waits for the engine.
#[tokio::test]
async fn a_check_that_finds_this_runtimes_own_fold_keeps_the_position() {
    let fixture = fixture("own-fold-check", Parked::WalPuts).await;
    let maintenance = fixture.writer.maintenance(writer_id("own-fold-check"));
    let replays = fixture.tail_replays();
    let manifest = |manifest_no| RecordedOperation::Head {
        key: metadata_manifest_object(&fixture.namespace_id, &manifest_no),
    };
    let gate = fixture.recording.inner();
    gate.block_next();
    let (during, folded) = futures::join!(fixture.put("/during.txt"), async {
        gate.wait_until_blocked().await;
        let folded = maintenance
            .fold_wal(&fixture.namespace_id)
            .await
            .expect("fold");
        assert_eq!(folded.outcome, FoldWalOutcome::Published);
        fixture.clock.advance_ms(MAX_STALENESS_MS);
        let (positions, anchors) = fixture.head_sources();
        fixture.recording.reset();
        bounded(NO_WAIT, fixture.reader.stat("/small.txt"))
            .await
            .expect("the read doesn't wait for the publish")
            .expect("stat");
        let after_fold = folded
            .manifest_no
            .successor()
            .expect("next manifest number");
        let operations = fixture.recording.take();
        assert!(
            operations.len() == 3
                && operations[0] == manifest(folded.manifest_no)
                && is_manifest_get(&operations[1], &fixture.namespace_id, folded.manifest_no)
                && is_manifest_get(&operations[2], &fixture.namespace_id, after_fold),
            "one HEAD finds the fold's manifest, the read loads it, finds nothing after it, \
             and reloads nothing: {operations:?}"
        );
        assert_eq!(fixture.head_sources(), (positions + 1, anchors));
        gate.release();
        folded
    });
    let during = during.expect("the publish succeeds");
    assert_eq!(fixture.tail_replays(), replays);

    fixture.recording.reset();
    let entry = fixture.reader.stat("/during.txt").await.expect("stat");
    assert_eq!(entry.head_seq, during.committed_seq);
    // The read checks the manifest after the fold's, and it reads the fold's
    // manifest the first time it starts from that manifest.
    let next = manifest(
        folded
            .manifest_no
            .successor()
            .expect("next manifest number"),
    );
    let fold_manifest = metadata_manifest_object(&fixture.namespace_id, &folded.manifest_no);
    let operations = fixture.recording.take();
    assert!(
        operations.contains(&next)
            && operations.iter().all(|operation| {
                *operation == next
                    || matches!(
                        operation,
                        RecordedOperation::Get { key, .. } if *key == fold_manifest
                    )
            }),
        "the read starts from the fold's manifest: {operations:?}"
    );
    fixture
        .put("/after.txt")
        .await
        .expect("commit after the fold");
    assert_eq!(fixture.tail_replays(), replays);
}

/// Checks that once a session finds that it was fenced, the next read in its
/// runtime starts from the read cache and reads the store, so it sees the
/// new writer's commits.
#[tokio::test]
async fn a_fenced_sessions_next_read_reads_the_store() {
    let fixture = fixture("fenced-read", Parked::WalPuts).await;
    let peer = peer(&fixture, "fenced-peer").await;
    let peer_commit = peer
        .open_namespace(&fixture.namespace_id)
        .expect("open namespace")
        .put_file("/peer.txt", b"peer", &loonfs_test_support::test_actor())
        .await
        .expect("the peer takes over");
    let fenced = fixture.put("/fenced.txt").await;
    assert_eq!(
        fenced.expect_err("the old session is fenced").code(),
        ErrorCode::WriterFenced
    );

    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    let entry = fixture.reader.stat("/peer.txt").await.expect("stat");
    assert_eq!(entry.head_seq, peer_commit.committed_seq);
    assert!(!fixture.recording.take().is_empty());
    assert_eq!(fixture.head_sources(), (positions, anchors + 1));
}

/// Checks that a read doesn't start from a position whose last manifest
/// check is past the revalidation bound. A check's answer can't confirm the
/// position then, so the read drops the position and reloads the namespace,
/// and the session's next publish loads the namespace too.
#[tokio::test]
async fn a_position_past_the_revalidation_bound_is_not_read_from() {
    let fixture = fixture("position-bound", Parked::WalPuts).await;
    fixture.clock.advance_ms(READ_REVALIDATION_BOUND_MS);
    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    fixture.reader.stat("/small.txt").await.expect("stat");
    let operations = fixture.recording.take();
    assert!(
        matches!(
            operations.first(),
            Some(RecordedOperation::GetWithMetadata { key, .. })
                if *key == hint(&fixture.namespace_id)
        ),
        "the read reloads from the hint without a successor check: {operations:?}"
    );
    assert_eq!(fixture.head_sources(), (positions, anchors + 1));

    let replays = fixture.tail_replays();
    fixture.put("/after.txt").await.expect("commit");
    assert_eq!(fixture.tail_replays(), replays + 1);
    fixture.recording.reset();
    fixture.reader.stat("/after.txt").await.expect("stat");
    assert_eq!(fixture.recording.take(), []);
}

/// Checks that the reads that need a check of the position share the
/// session's checks, and that the check confirms the position for the
/// eventual reads after it.
///
/// A strong read starts a check, which the store parks. Strong reads and an
/// eventual read past its maximum staleness that arrive during that check
/// wait for it, because it started before they did, and then share the next
/// one. So the wave sends two HEADs, and an eventual read after it sends
/// nothing.
#[tokio::test]
async fn a_wave_of_reads_shares_at_most_two_checks() {
    let fixture = fixture("shared-check", Parked::ManifestChecks).await;
    fixture.clock.advance_ms(MAX_STALENESS_MS);
    let gate = fixture.recording.inner();
    gate.block_next();
    fixture.recording.reset();
    let first_options = strong();
    let first = fixture
        .reader
        .stat_with_options("/small.txt", &first_options);
    let (first, ()) = futures::join!(first, async {
        gate.wait_until_blocked().await;
        let reads = [strong(), strong(), strong(), StatOptions::default()];
        let mut wave =
            pin!(futures::future::join_all(reads.iter().map(|options| {
                fixture.reader.stat_with_options("/small.txt", options)
            }),));
        assert!(
            futures::poll!(wave.as_mut()).is_pending(),
            "the reads wait for the parked check"
        );
        gate.release();
        for entry in wave.await {
            entry.expect("a read during the check");
        }
    });
    first.expect("the read that started the first check");
    let operations = fixture.recording.take();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| is_manifest_head(operation, &fixture.namespace_id))
            .count(),
        2,
        "{operations:?}"
    );
    fixture.recording.reset();
    fixture.reader.stat("/small.txt").await.expect("stat");
    assert_eq!(
        fixture.recording.take(),
        [],
        "the check confirmed the position"
    );
}
